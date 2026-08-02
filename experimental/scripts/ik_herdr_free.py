#!/usr/bin/env python3
"""
IK ↔ Herdr free-agent bridge (v1.8).

Master: IK (fork binary). Workers: white-list slots (or-free + ollama-cloud + NIM)
with LIVE health-ranked models (not sticky preferred ids).

Multi-agent (v1.8):
  - --to mesh|all  → fan-out free/full workers (parallel oneshot)
  - --to auto      → primary by route + automatic failover
  - --failover / --no-failover  (default: on for auto/mesh)
  - --parallel     force parallel even for single primary
  - mesh winner = best quality score (not primary-first thin RESULT)
  - oneshot default = full brief (use --simple only for doctor/ping)
  - ok requires RESULT + VERIFY markers (VERIFY may be N/A with reason)
  - parallel: max-concurrent semaphore + stagger + 429 retry
  - interactive mesh: parallel panes + quality winner + thin skip
  - interrupt stuck free panes (C-c) + ensure recovery
  - file queue (add|list|run|clear|worker) + Author≠Reviewer
  - review retry-on-FAIL + queue worker daemon (--bg)
  - metrics JSONL (delegate/queue/worker/doctor)
  - LIVE roster: roster/health/probe/health-worker + ik_model_roster.py
  - day runner: long parallel jobs (day go|status|stop)

Herdr facts (probed 2026-08-02):
  - agent list JSON uses utf-8 BOM
  - kind in field `agent` (pi|grok), custom name in field `name` (pi-ling)
  - `herdr agent read` returns PLAIN TEXT (not JSON)
  - `herdr agent list` / `pane list` return JSON

Commands:
  status [--json]
  ensure [--lane fast|long|all|full]
  route <hint...>
  roster show|refresh|pool
  health | probe | health-worker start|status|stop|once
  day go|status|stop
  delegate --to auto|mesh|all|ling|nemotron|NAME --goal "..."
  collect | interrupt | queue | doctor | metrics
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path
from typing import Any

HERDR_BIN = os.environ.get(
    "HERDR_BIN",
    r"C:\Users\anton\AppData\Local\Programs\Herdr\bin\herdr.exe"
    if os.name == "nt"
    else "herdr",
)
PI_CMD = os.environ.get(
    "PI_CMD",
    r"C:\Users\anton\AppData\Roaming\npm\pi.cmd" if os.name == "nt" else "pi",
)
# Direct node entry (avoids cmd quoting issues with pi.cmd + shell=True)
PI_CLI = os.environ.get(
    "PI_CLI",
    r"C:\Users\anton\AppData\Roaming\npm\node_modules\@earendil-works\pi-coding-agent\dist\cli.js",
)
WHITE_LIST = Path(
    os.environ.get(
        "IK_PI_WHITELIST",
        r"C:\Users\anton\Sync\Configs\pi\white-list.json",
    )
)
TOOLS = "read,bash,edit,write,grep,find,ls"
AGENT_DIR = Path.home() / ".pi" / "agent"
QUEUE_PATH = Path(
    os.environ.get(
        "IK_FREE_QUEUE",
        str(Path.home() / "Sync" / "Data" / "ik_free_queue.jsonl"),
    )
)

# Dynamic workers: white-list slots + models.json candidates + health scores
# (NOT hardcoded model ids — see ik_model_roster.py)
try:
    import ik_model_roster as _roster  # type: ignore
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import ik_model_roster as _roster  # type: ignore

FREE_WORKERS: dict[str, dict[str, Any]] = {}


def refresh_workers(*, force: bool = False) -> dict[str, dict[str, Any]]:
    """Load/rebuild active roster from health (full white-list slots)."""
    global FREE_WORKERS
    FREE_WORKERS = _roster.load_workers(refresh=force)
    if not FREE_WORKERS:
        # last-resort seed if files missing
        FREE_WORKERS = _roster.rebuild_roster(persist=True)
    return FREE_WORKERS


def record_run_health(r: dict[str, Any]) -> None:
    """Push delegate/oneshot outcome into health store + re-rank roster."""
    try:
        key = str(r.get("key") or "")
        meta = FREE_WORKERS.get(key) or {}
        provider = str(r.get("provider") or meta.get("provider") or "")
        model = str(r.get("model") or meta.get("model") or "")
        if not provider or not model:
            return
        _roster.record_outcome(
            provider,
            model,
            ok=bool(r.get("ok")),
            quality=float(r.get("quality") or score_result(r)),
            rate_limited=bool(r.get("rate_limited")),
            latency_ms=r.get("latency_ms"),
            error=str(r.get("error") or "")[:200] or None,
            key=key,
            lane=str(meta.get("lane") or ""),
        )
        # keep in-process map in sync with new best models
        refresh_workers(force=False)
    except Exception:
        pass


# initial load (rebuild active-roster if missing)
refresh_workers(force=not Path(
    os.environ.get(
        "IK_ACTIVE_ROSTER",
        str(Path.home() / "Sync" / "Configs" / "pi" / "active-roster.json"),
    )
).is_file())

# v1.4 rate-limit / concurrency (free OpenRouter friendly)
MAX_CONCURRENT = max(1, int(os.environ.get("IK_FREE_MAX_CONCURRENT", "2")))
STAGGER_SEC = float(os.environ.get("IK_FREE_STAGGER_SEC", "1.5"))
RATE_BACKOFF_SEC = float(os.environ.get("IK_FREE_RATE_BACKOFF_SEC", "8"))
MAX_RETRIES_429 = max(0, int(os.environ.get("IK_FREE_MAX_RETRIES_429", "1")))
MAX_REVIEW_RETRIES = max(0, int(os.environ.get("IK_FREE_MAX_REVIEW_RETRIES", "1")))
QUEUE_WORKER_PID = Path(
    os.environ.get(
        "IK_FREE_QUEUE_WORKER_PID",
        str(Path.home() / "Sync" / "Data" / "ik_free_queue_worker.pid"),
    )
)
QUEUE_WORKER_LOG = Path(
    os.environ.get(
        "IK_FREE_QUEUE_WORKER_LOG",
        str(Path.home() / "Sync" / "Data" / "ik_free_queue_worker.log"),
    )
)
METRICS_PATH = Path(
    os.environ.get(
        "IK_FREE_METRICS",
        str(Path.home() / "Sync" / "Data" / "ik_free_metrics.jsonl"),
    )
)

_free_sem = threading.Semaphore(MAX_CONCURRENT)
_stagger_lock = threading.Lock()
_last_launch_ts = 0.0


def _stagger_launch() -> float:
    """Serialize starts so free providers are not hammered at t=0."""
    global _last_launch_ts
    with _stagger_lock:
        now = time.time()
        wait = STAGGER_SEC - (now - _last_launch_ts)
        if wait > 0:
            time.sleep(wait)
        _last_launch_ts = time.time()
        return _last_launch_ts


def looks_rate_limited(text: str) -> bool:
    if not text:
        return False
    return bool(
        re.search(
            r"\b429\b|rate.?limit|too many requests|quota.?exceed|capacity|temporarily unavailable",
            text,
            re.I,
        )
    )


def pick_quality_winner(
    chain: list[str], results_by_key: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    ok_keys = [k for k in chain if results_by_key.get(k, {}).get("ok")]
    if not ok_keys:
        return None
    ranked = sorted(
        ok_keys,
        key=lambda k: (-score_result(results_by_key[k]), chain.index(k)),
    )
    return results_by_key[ranked[0]]


def mesh_report_from_results(
    chain: list[str],
    results_by_key: dict[str, dict[str, Any]],
    attempts: list[dict[str, Any]],
    *,
    mode: str,
) -> dict[str, Any]:
    winner = pick_quality_winner(chain, results_by_key)
    ok_keys = [k for k in chain if results_by_key.get(k, {}).get("ok")]
    if winner is None:
        vals = list(results_by_key.values())
        last = max(vals, key=lambda r: score_result(r)) if vals else {}
        return {
            "ok": False,
            "mode": mode,
            "strategy": "parallel-quality",
            "chain": chain,
            "attempts": attempts,
            "failover_used": True,
            "workers_ok": [],
            "results": {
                k: {
                    "ok": results_by_key[k].get("ok"),
                    "result": results_by_key[k].get("result"),
                    "verify": results_by_key[k].get("verify"),
                    "quality": score_result(results_by_key[k]),
                    "error": results_by_key[k].get("error"),
                }
                for k in chain
                if k in results_by_key
            },
            "error": "all free workers failed",
            "result": last.get("result", ""),
            "verify": last.get("verify", ""),
            "raw_tail": last.get("raw_tail", ""),
            "worker": last.get("worker"),
            "model": last.get("model"),
            "quality": score_result(last),
            "rate": {
                "max_concurrent": MAX_CONCURRENT,
                "stagger_sec": STAGGER_SEC,
            },
        }
    peers = {
        k: {
            "ok": results_by_key[k].get("ok"),
            "result": results_by_key[k].get("result"),
            "verify": results_by_key[k].get("verify"),
            "quality": score_result(results_by_key[k]),
            "worker": results_by_key[k].get("worker"),
            "model": results_by_key[k].get("model"),
        }
        for k in chain
        if k != winner.get("key") and k in results_by_key
    }
    return {
        "ok": True,
        "mode": mode,
        "strategy": "parallel-quality",
        "chain": chain,
        "attempts": attempts,
        "failover_used": len(ok_keys) < len(chain) or chain[0] != winner.get("key"),
        "workers_ok": ok_keys,
        "winner_key": winner.get("key"),
        "worker": winner.get("worker"),
        "model": winner.get("model"),
        "result": winner.get("result"),
        "verify": winner.get("verify"),
        "raw_tail": winner.get("raw_tail"),
        "has_result_marker": winner.get("has_result_marker"),
        "quality": score_result(winner),
        "rc": winner.get("rc"),
        "peers": peers,
        "results": {
            k: {
                "ok": results_by_key[k].get("ok"),
                "result": results_by_key[k].get("result"),
                "verify": results_by_key[k].get("verify"),
                "quality": score_result(results_by_key[k]),
                "error": results_by_key[k].get("error"),
            }
            for k in chain
            if k in results_by_key
        },
        "rate": {
            "max_concurrent": MAX_CONCURRENT,
            "stagger_sec": STAGGER_SEC,
        },
    }


def die(msg: str, code: int = 1) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(code)


def run_herdr_raw(args: list[str]) -> subprocess.CompletedProcess[str]:
    if not Path(HERDR_BIN).exists() and os.name == "nt":
        die(f"herdr binary not found: {HERDR_BIN}")
    try:
        return subprocess.run(
            [HERDR_BIN] + args,
            capture_output=True,
            text=True,
            encoding="utf-8-sig",
            errors="replace",
            check=False,
        )
    except FileNotFoundError:
        die(f"herdr binary not found: {HERDR_BIN}")


def _first_json_object(raw: str) -> Any:
    raw = (raw or "").strip().lstrip("\ufeff")
    if not raw:
        raise json.JSONDecodeError("empty", raw, 0)
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        if start < 0:
            raise
        depth = 0
        in_str = False
        esc = False
        for i, ch in enumerate(raw[start:], start):
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    return json.loads(raw[start : i + 1])
        raise


def herdr_json(args: list[str]) -> Any:
    p = run_herdr_raw(args)
    raw = p.stdout or ""
    if p.returncode != 0 and not raw.strip():
        die(f"herdr {' '.join(args)} rc={p.returncode} stderr={(p.stderr or '')[:400]}")
    if not raw.strip():
        die(f"empty herdr output for: {args}")
    try:
        return _first_json_object(raw)
    except json.JSONDecodeError as e:
        die(f"JSON parse error for {args}: {e}\n{raw[:500]}")


def get_agents() -> list[dict[str, Any]]:
    data = herdr_json(["agent", "list"])
    return list(data.get("result", {}).get("agents") or [])


def agent_kind(a: dict[str, Any]) -> str:
    return str(a.get("agent") or "unknown")


def agent_name(a: dict[str, Any]) -> str:
    # custom rename in `name`, kind in `agent`
    return str(a.get("name") or a.get("agent") or "unknown")


def is_generic_pi(a: dict[str, Any]) -> bool:
    return agent_kind(a) == "pi" and not a.get("name")


def find_agent(target: str) -> dict[str, Any] | None:
    agents = get_agents()
    # pane id
    for a in agents:
        if a.get("pane_id") == target:
            return a
    # custom name exact
    for a in agents:
        if a.get("name") == target:
            return a
    # free key
    if target in FREE_WORKERS:
        want = FREE_WORKERS[target]["name"]
        for a in agents:
            if a.get("name") == want:
                return a
    # display name / kind
    for a in agents:
        if agent_name(a) == target or agent_kind(a) == target:
            return a
    if target in ("pi", "any-free", "auto"):
        # prefer named free idle, then generic pi idle
        free_names = {w["name"] for w in FREE_WORKERS.values()}
        for a in agents:
            if a.get("name") in free_names and a.get("agent_status") in (
                "idle",
                "done",
                "unknown",
            ):
                return a
        for a in agents:
            if is_generic_pi(a) and a.get("agent_status") in (
                "idle",
                "done",
                "unknown",
            ):
                return a
    return None


def pane_text(pane_or_name: str, lines: int = 60) -> str:
    a = find_agent(pane_or_name)
    target = a["pane_id"] if a else pane_or_name
    # agent read → PLAIN TEXT (probed)
    p = run_herdr_raw(["agent", "read", target, "--lines", str(lines)])
    if p.returncode == 0 and (p.stdout or "").strip():
        raw = p.stdout or ""
        # sometimes still JSON
        if raw.lstrip().startswith("{"):
            try:
                data = _first_json_object(raw)
                res = data.get("result") or {}
                if isinstance(res, dict):
                    rd = res.get("read")
                    if isinstance(rd, dict) and rd.get("text"):
                        return str(rd["text"])
                    if res.get("text"):
                        return str(res["text"])
            except Exception:
                pass
        return raw
    # pane read fallback
    p2 = run_herdr_raw(["pane", "read", target, "--lines", str(lines)])
    raw2 = p2.stdout or ""
    if raw2.lstrip().startswith("{"):
        try:
            data = _first_json_object(raw2)
            res = data.get("result") or {}
            if isinstance(res, dict):
                rd = res.get("read")
                if isinstance(rd, dict) and rd.get("text"):
                    return str(rd["text"])
                if res.get("text"):
                    return str(res["text"])
        except Exception:
            return raw2
    return raw2


def extract_result(text: str) -> dict[str, str]:
    out = {"result": "", "verify": "", "raw_tail": (text or "")[-2000:]}
    if not text:
        return out
    cleaned_lines = []
    for ln in text.splitlines():
        s = ln.strip()
        if "No models match pattern" in ln:
            continue
        if s in ("(Empty line)",):
            continue
        # strip TUI chrome
        if s.startswith("─") or s.startswith("━"):
            continue
        if re.match(r"^~ \(main\)$", s):
            continue
        if re.match(r"^↑|^↓|^0\.0%|CH\d", s):
            continue
        cleaned_lines.append(ln)
    cleaned = "\n".join(cleaned_lines)

    # Prefer LAST RESULT/VERIFY (pane scrollback has older reports)
    matches = list(
        re.finditer(r"RESULT:\s*(.+?)(?=\n\s*VERIFY:|\n\s*RESULT:|\Z)", cleaned, re.S | re.I)
    )
    if matches:
        out["result"] = matches[-1].group(1).strip()
        # VERIFY after last RESULT if present
        tail = cleaned[matches[-1].start() :]
        m2 = re.search(
            r"VERIFY:\s*(.+?)(?:\n\s*(?:RISK|NEXT|DONE|TASK|RESULT):|\Z)",
            tail,
            re.S | re.I,
        )
        if m2:
            out["verify"] = m2.group(1).strip()
    else:
        m2 = re.search(
            r"VERIFY:\s*(.+?)(?:\n\s*(?:RISK|NEXT|DONE|TASK):|\Z)",
            cleaned,
            re.S | re.I,
        )
        if m2:
            out["verify"] = m2.group(1).strip()
    if not out["result"]:
        lines = [ln for ln in cleaned.strip().splitlines() if ln.strip()]
        out["result"] = "\n".join(lines[-8:]) if lines else ""
    return out


# Minimum quality for sequential failover to accept a "success" without trying peers.
# Thin one-liners with VERIFY:N/A score ~25–35; real digests score 50+.
MIN_ACCEPT_SCORE = 40


def score_result(r: dict[str, Any]) -> int:
    """
    Quality score for mesh winner / sequential accept.
    Higher = better for orchestrator synthesis.
    """
    if not r or r.get("error"):
        return 0
    result = str(r.get("result") or "").strip()
    verify = str(r.get("verify") or "").strip()
    score = 0
    if r.get("rc") == 0:
        score += 5
    if r.get("has_result_marker"):
        score += 10
    if result:
        score += min(len(result) // 15, 30)  # up to +30 for substance
        if len(result) < 24:
            score -= 12  # thin one-liner penalty
        if re.search(r"(?m)^\s*[-*•]|\d+\.\s", result):
            score += 8  # structured bullets
        if re.search(r"https?://|\.com|\.io|\.net|source", result, re.I):
            score += 6
    if verify:
        score += 15
        vlow = verify.lower()
        # bare N/A without reason is weak; N/A with reason is ok
        if re.match(r"^n/?a\s*$", vlow):
            score -= 10
        elif "n/a" in vlow and len(verify) < 12:
            score -= 8
        else:
            score += 10  # meaningful VERIFY
        if re.search(r"exit\s*\d|rc\s*[=:]|research-only|command|lint|test", vlow):
            score += 8
    else:
        score -= 15  # missing VERIFY
    if r.get("ok") is False and not result:
        return 0
    return max(0, score)


def is_thin_success(r: dict[str, Any]) -> bool:
    """ok marker present but too thin for auto-accept in sequential failover."""
    return bool(r.get("ok")) and score_result(r) < MIN_ACCEPT_SCORE


def pi_launch_cmd(provider: str, model: str) -> str:
    return (
        f'"{PI_CMD}" -ne --provider {provider} --model {model} '
        f"--tools {TOOLS}"
    )


def ensure_worker(key: str) -> dict[str, Any]:
    if key not in FREE_WORKERS:
        die(f"unknown free key: {key}")
    meta = FREE_WORKERS[key]
    name = meta["name"]

    existing = find_agent(name)
    if existing:
        return {
            "action": "already",
            "name": name,
            "pane_id": existing.get("pane_id"),
            "status": existing.get("agent_status"),
            "model": meta["model"],
        }

    # Prefer rename of generic pi (no custom name)
    for a in get_agents():
        if not is_generic_pi(a):
            continue
        if a.get("agent_status") not in ("idle", "done", "unknown"):
            continue
        pane = a["pane_id"]
        ren = run_herdr_raw(["agent", "rename", pane, name])
        time.sleep(0.5)
        a2 = find_agent(name)
        if a2:
            return {
                "action": "renamed",
                "name": name,
                "pane_id": a2.get("pane_id"),
                "status": a2.get("agent_status"),
                "model": meta["model"],
                "rename_rc": ren.returncode,
            }
        # name taken → someone else has it
        a3 = find_agent(name)
        if a3:
            return {
                "action": "already",
                "name": name,
                "pane_id": a3.get("pane_id"),
                "status": a3.get("agent_status"),
                "model": meta["model"],
            }

    # Split new pane
    agents = get_agents()
    base_pane = next((a.get("pane_id") for a in agents if a.get("focused")), None)
    if not base_pane and agents:
        base_pane = agents[0].get("pane_id")
    if not base_pane:
        die("no panes to split")

    before = {a.get("pane_id") for a in get_agents()}
    # also track all panes
    try:
        pl0 = herdr_json(["pane", "list"])
        before_panes = {
            p.get("pane_id")
            for p in (pl0.get("result", {}).get("panes") or [])
            if p.get("pane_id")
        }
    except Exception:
        before_panes = set(before)

    split = run_herdr_raw(
        [
            "pane",
            "split",
            base_pane,
            "--direction",
            "right",
            "--ratio",
            "0.4",
            "--no-focus",
        ]
    )
    new_pane = None
    try:
        sdata = _first_json_object(split.stdout or "{}")
        res = sdata.get("result") or sdata
        new_pane = (
            res.get("pane_id")
            or res.get("id")
            or (res.get("pane") or {}).get("pane_id")
            or (res.get("pane") or {}).get("id")
        )
    except Exception:
        new_pane = None

    if not new_pane:
        time.sleep(0.8)
        pl = herdr_json(["pane", "list"])
        for p in pl.get("result", {}).get("panes") or []:
            pid = p.get("pane_id")
            if pid and pid not in before_panes:
                new_pane = pid
                break

    if not new_pane:
        die(f"split failed for {name}: {(split.stdout or '')[:300]} {(split.stderr or '')[:200]}")

    launch = pi_launch_cmd(meta["provider"], meta["model"])
    if os.name == "nt":
        run_herdr_raw(["pane", "run", new_pane, "cmd", "/c", launch])
    else:
        run_herdr_raw(["pane", "run", new_pane, "bash", "-lc", launch])

    # wait for agent detection
    for _ in range(15):
        time.sleep(1)
        for a in get_agents():
            if a.get("pane_id") == new_pane:
                run_herdr_raw(["agent", "rename", new_pane, name])
                time.sleep(0.3)
                a2 = find_agent(name) or find_agent(new_pane)
                return {
                    "action": "started",
                    "name": name,
                    "pane_id": new_pane,
                    "status": (a2 or a).get("agent_status"),
                    "model": meta["model"],
                    "launch": launch,
                }

    return {
        "action": "started_pending",
        "name": name,
        "pane_id": new_pane,
        "status": "unknown",
        "model": meta["model"],
        "launch": launch,
        "note": "agent not detected yet; pane created",
    }



def resolve_pane_target(target: str) -> dict[str, Any] | None:
    """Resolve free key / name / pane_id to agent dict."""
    if not target:
        return None
    if target in FREE_WORKERS:
        return find_agent(FREE_WORKERS[target]["name"])
    return find_agent(target)


def interrupt_agent(
    target: str,
    *,
    wait_sec: float = 8.0,
    pulses: int = 2,
    ensure_after: bool = True,
) -> dict[str, Any]:
    """
    Soft-stop a stuck free pane via herdr send-keys C-c (1–2 pulses).
    Avoids Escape spam that can kill agent detection.
    """
    key = target if target in FREE_WORKERS else None
    a = resolve_pane_target(target)
    if not a:
        out = {
            "ok": False,
            "target": target,
            "error": "agent not found",
            "action": "missing",
        }
        if ensure_after and key:
            try:
                e = ensure_worker(key)
                out["ensure"] = e
                out["action"] = "ensured_missing"
                out["ok"] = True
            except Exception as ex:
                out["ensure_error"] = str(ex)
        return out

    pane = a["pane_id"]
    name = agent_name(a)
    status0 = str(a.get("agent_status") or "unknown")
    rcs = []
    for i in range(max(1, pulses)):
        p = run_herdr_raw(["agent", "send-keys", pane, "C-c"])
        rcs.append(p.returncode)
        time.sleep(0.6 if i == 0 else 1.0)

    # wait for non-working
    deadline = time.time() + max(2.0, wait_sec)
    status1 = status0
    while time.time() < deadline:
        a2 = find_agent(pane) or find_agent(name)
        if not a2:
            status1 = "missing"
            break
        status1 = str(a2.get("agent_status") or "unknown")
        if status1 not in ("working", "busy", "running"):
            break
        run_herdr_raw(
            [
                "agent",
                "wait",
                pane,
                "--until",
                "idle",
                "--until",
                "done",
                "--until",
                "blocked",
                "--timeout",
                "2000",
            ]
        )
        time.sleep(0.3)

    ensure_info = None
    if status1 == "missing" and ensure_after and key:
        try:
            ensure_info = ensure_worker(key)
            status1 = "ensured"
        except Exception as ex:
            ensure_info = {"error": str(ex)}

    ok = status1 not in ("working", "busy", "running")
    return {
        "ok": ok,
        "target": target,
        "name": name,
        "pane_id": pane,
        "status_before": status0,
        "status_after": status1,
        "send_keys_rc": rcs,
        "pulses": pulses,
        "ensure": ensure_info,
        "action": "interrupt",
    }


def interrupt_free_stuck(*, force: bool = False, wait_sec: float = 8.0) -> dict[str, Any]:
    """Interrupt free workers that are working (or all free if force)."""
    results = []
    for key, meta in FREE_WORKERS.items():
        a = find_agent(meta["name"])
        if not a:
            results.append(interrupt_agent(key, wait_sec=wait_sec, ensure_after=True))
            continue
        st = str(a.get("agent_status") or "")
        if force or st in ("working", "busy", "running"):
            results.append(interrupt_agent(key, wait_sec=wait_sec, ensure_after=True))
        else:
            results.append(
                {
                    "ok": True,
                    "target": key,
                    "name": meta["name"],
                    "pane_id": a.get("pane_id"),
                    "status_before": st,
                    "status_after": st,
                    "action": "skip_not_stuck",
                }
            )
    return {
        "ok": all(r.get("ok") for r in results),
        "results": results,
    }


def cmd_interrupt(target: str, force: bool, wait: float) -> None:
    if target in ("all", "free", "*"):
        rep = interrupt_free_stuck(force=force, wait_sec=wait)
    else:
        # single: interrupt if stuck or force
        a = resolve_pane_target(target)
        st = str((a or {}).get("agent_status") or "missing")
        if force or st in ("working", "busy", "running", "missing"):
            rep = interrupt_agent(target, wait_sec=wait, ensure_after=True)
        else:
            rep = {
                "ok": True,
                "target": target,
                "action": "skip_not_stuck",
                "status_before": st,
                "status_after": st,
            }
    print(json.dumps(rep, indent=2, ensure_ascii=False))
    if not rep.get("ok"):
        sys.exit(2)


# --- file queue ---
def _queue_load() -> list[dict[str, Any]]:
    path = QUEUE_PATH
    if not path.is_file():
        return []
    items = []
    for ln in path.read_text(encoding="utf-8").splitlines():
        ln = ln.strip()
        if not ln:
            continue
        try:
            items.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    return items


def _queue_save(items: list[dict[str, Any]]) -> None:
    path = QUEUE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    body = "\n".join(json.dumps(it, ensure_ascii=False) for it in items)
    if body:
        body += "\n"
    tmp.write_text(body, encoding="utf-8")
    tmp.replace(path)


def queue_add(
    goal: str,
    *,
    scope: str = "",
    to: str = "auto",
    mode: str = "oneshot",
    wait: int = 120,
    review: bool = False,
    priority: int = 50,
    max_retries: int | None = None,
    max_attempts: int | None = None,
) -> dict[str, Any]:
    items = _queue_load()
    job = {
        "id": f"job-{int(time.time())}-{len(items)+1}",
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "status": "pending",
        "priority": int(priority),
        "goal": goal,
        "scope": scope or "research-only",
        "to": to,
        "mode": mode,
        "wait": int(wait),
        "review": bool(review),
        "max_retries": int(MAX_REVIEW_RETRIES if max_retries is None else max_retries),
        "max_attempts": int(
            max_attempts
            if max_attempts is not None
            else int(os.environ.get("IK_DAY_MAX_ATTEMPTS", "3"))
        ),
        "attempts": 0,
        "progress": [],
        "partial_result": "",
    }
    items.append(job)
    _queue_save(items)
    return {"ok": True, "job": job, "queue_path": str(QUEUE_PATH), "pending": sum(1 for i in items if i.get("status") == "pending")}


def queue_list() -> dict[str, Any]:
    items = _queue_load()
    pending = [i for i in items if i.get("status") == "pending"]
    done = [i for i in items if i.get("status") in ("done", "failed")]
    return {
        "ok": True,
        "path": str(QUEUE_PATH),
        "total": len(items),
        "pending": len(pending),
        "done": len(done),
        "items": items[-30:],
    }


def queue_clear(*, done_only: bool = True) -> dict[str, Any]:
    items = _queue_load()
    if done_only:
        keep = [i for i in items if i.get("status") == "pending"]
    else:
        keep = []
    _queue_save(keep)
    return {"ok": True, "kept": len(keep), "cleared": len(items) - len(keep)}



def parse_review_verdict(review: dict[str, Any]) -> bool:
    """True only if reviewer RESULT explicitly indicates PASS (not FAIL)."""
    r0 = str(review.get("result") or "").strip().upper()
    if not r0:
        return False
    # strip common markdown ticks / quotes
    r0 = r0.lstrip("`*'\" ")
    if r0.startswith("FAIL") or re.search(r"\bFAIL\b", r0[:120]):
        return False
    if r0.startswith("PASS") or re.search(r"\bPASS\b", r0[:120]):
        return True
    # no explicit verdict → not passed
    return False


def _run_author_once(
    author_key: str,
    brief: str,
    wait: int,
    mode: str,
) -> dict[str, Any]:
    if mode == "interactive":
        author = interactive_delegate(author_key, brief, wait)
    else:
        author = oneshot_run(author_key, brief, wait)
    author["key"] = author_key
    author["role"] = "author"
    return author


def _run_reviewer_once(
    reviewer_key: str,
    author: dict[str, Any],
    scope: str,
    wait: int,
    mode: str,
) -> dict[str, Any]:
    review_goal = (
        "You are REVIEWER (not author). Read AUTHOR_OUTPUT below. "
        "Check: concrete facts? VERIFY present and honest? no hallucinations? "
        "Reply first word PASS or FAIL, then 1-3 issues if FAIL. "
        "VERIFY: review-only.\n\n"
        f"AUTHOR_OUTPUT:\nRESULT: {author.get('result','')}\nVERIFY: {author.get('verify','')}\n"
    )
    review_brief = build_brief(
        review_goal,
        scope or "review-only",
        "PASS/FAIL + issues",
        "review-only; no code change",
        simple=False,
    )
    w = max(45, wait // 2)
    if mode == "interactive":
        review = interactive_delegate(reviewer_key, review_brief, w)
    else:
        review = oneshot_run(reviewer_key, review_brief, w)
    review["key"] = reviewer_key
    review["role"] = "reviewer"
    review["passed"] = parse_review_verdict(review)
    return review


def author_reviewer_pass(
    goal: str,
    scope: str,
    wait: int,
    *,
    mode: str = "oneshot",
    max_retries: int | None = None,
) -> dict[str, Any]:
    """
    Author≠Reviewer with v1.6 retry-on-FAIL:
      round 0: author → reviewer
      on FAIL: author revise using reviewer issues → reviewer again
      max_retries = extra revise rounds (default MAX_REVIEW_RETRIES)
    """
    if max_retries is None:
        max_retries = MAX_REVIEW_RETRIES
    max_retries = max(0, int(max_retries))

    primary = resolve_to_key("auto", goal)
    chain = failover_chain(primary)
    author_key = chain[0]
    reviewer_key = chain[1] if len(chain) > 1 else chain[0]

    author_brief = build_brief(
        goal,
        scope,
        "Author pass: complete goal with concrete facts; RESULT+VERIFY required",
        "state checks or research-only + sources",
        simple=False,
    )
    author = _run_author_once(author_key, author_brief, wait, mode)

    if not author.get("ok"):
        if reviewer_key != author_key:
            author2 = _run_author_once(reviewer_key, author_brief, wait, mode)
            if author2.get("ok") or score_result(author2) > score_result(author):
                # author2 ran on old reviewer_key; keep roles distinct
                old_author = author_key
                author = author2
                author_key = author2.get("key") or reviewer_key
                reviewer_key = old_author if old_author != author_key else reviewer_key
            else:
                return {
                    "ok": False,
                    "mode": f"review-{mode}",
                    "strategy": "author-reviewer",
                    "author": author,
                    "error": "author failed",
                    "quality": score_result(author),
                    "rounds": 0,
                }
        else:
            return {
                "ok": False,
                "mode": f"review-{mode}",
                "strategy": "author-reviewer",
                "author": author,
                "error": "author failed",
                "quality": score_result(author),
                "rounds": 0,
            }

    if not author.get("ok"):
        return {
            "ok": False,
            "mode": f"review-{mode}",
            "strategy": "author-reviewer",
            "author": {
                "ok": author.get("ok"),
                "result": author.get("result"),
                "verify": author.get("verify"),
                "quality": score_result(author),
                "worker": author.get("worker"),
            },
            "error": "author failed (no RESULT+VERIFY)",
            "quality": score_result(author),
            "rounds": 0,
            "retries_used": 0,
            "max_retries": max_retries,
        }

    history: list[dict[str, Any]] = []
    review = _run_reviewer_once(reviewer_key, author, scope, wait, mode)
    history.append(
        {
            "round": 0,
            "author_quality": score_result(author),
            "review_passed": review.get("passed"),
            "review_result": (review.get("result") or "")[:400],
        }
    )

    retries_used = 0
    while (not review.get("passed")) and retries_used < max_retries:
        retries_used += 1
        issues = (review.get("result") or "")[:600]
        revise_goal = (
            f"REVISE your previous answer. Reviewer said FAIL with issues:\n{issues}\n\n"
            f"Original GOAL: {goal}\n"
            "Produce improved RESULT with concrete facts and honest VERIFY. "
            "Do not mention the reviewer; just answer the goal better."
        )
        revise_brief = build_brief(
            revise_goal,
            scope,
            "Revised author output; RESULT+VERIFY required",
            "state checks or research-only",
            simple=False,
        )
        author = _run_author_once(author_key, revise_brief, wait, mode)
        author["role"] = "author"
        author["revise_round"] = retries_used
        if not author.get("ok") and reviewer_key != author_key:
            # try other free as author once; keep Author≠Reviewer keys distinct
            alt = _run_author_once(reviewer_key, revise_brief, wait, mode)
            if alt.get("ok") or score_result(alt) > score_result(author):
                old_author = author_key
                author = alt
                author["key"] = reviewer_key
                author_key = reviewer_key
                reviewer_key = old_author

        if not author.get("ok"):
            history.append(
                {
                    "round": retries_used,
                    "author_quality": score_result(author),
                    "review_passed": False,
                    "review_result": "author revise failed",
                }
            )
            break
        review = _run_reviewer_once(reviewer_key, author, scope, wait, mode)
        history.append(
            {
                "round": retries_used,
                "author_quality": score_result(author),
                "review_passed": review.get("passed"),
                "review_result": (review.get("result") or "")[:400],
            }
        )

    passed = bool(review.get("passed"))
    return {
        "ok": bool(author.get("ok")) and bool(review.get("ok")) and passed,
        "mode": f"review-{mode}",
        "strategy": "author-reviewer-retry" if retries_used else "author-reviewer",
        "author_key": author.get("key"),
        "reviewer_key": reviewer_key,
        "author": {
            "ok": author.get("ok"),
            "result": author.get("result"),
            "verify": author.get("verify"),
            "quality": score_result(author),
            "worker": author.get("worker"),
            "revise_round": author.get("revise_round", 0),
        },
        "review": {
            "ok": review.get("ok"),
            "result": review.get("result"),
            "verify": review.get("verify"),
            "quality": score_result(review),
            "worker": review.get("worker"),
            "passed": passed,
        },
        "result": author.get("result"),
        "verify": f"author: {author.get('verify')}; review: {review.get('result')}",
        "quality": score_result(author),
        "review_passed": passed,
        "retries_used": retries_used,
        "max_retries": max_retries,
        "history": history,
        "winner_key": author.get("key"),
        "worker": author.get("worker"),
        "model": author.get("model"),
    }


def queue_run(limit: int = 1) -> dict[str, Any]:
    items = _queue_load()
    # Recover jobs left in "running" after crash/kill
    changed = False
    for i in items:
        if i.get("status") == "running":
            i["status"] = "failed"
            i["error"] = "stale_running_recovered_on_next_run"
            i["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            changed = True
    if changed:
        _queue_save(items)
        items = _queue_load()
    pending = sorted(
        [i for i in items if i.get("status") == "pending"],
        key=lambda x: (-int(x.get("priority") or 50), x.get("ts") or ""),
    )
    ran = []
    for job in pending[: max(1, limit)]:
        job["status"] = "running"
        job["started"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _queue_save(items)
        try:
            if job.get("review"):
                rep = author_reviewer_pass(
                    job["goal"],
                    job.get("scope") or "",
                    int(job.get("wait") or 120),
                    mode=job.get("mode") or "oneshot",
                    max_retries=int(job.get("max_retries") if job.get("max_retries") is not None else MAX_REVIEW_RETRIES),
                )
            else:
                # reuse delegate internals via oneshot/interactive mesh/auto
                to = job.get("to") or "auto"
                mode = job.get("mode") or "oneshot"
                wait = int(job.get("wait") or 120)
                goal = job["goal"]
                scope = job.get("scope") or ""
                brief = build_brief(goal, scope, "", "", simple=False)
                primary = resolve_to_key(to, goal)
                if is_mesh_target(to):
                    chain = failover_chain(primary)
                    if mode == "interactive":
                        rep = run_interactive_with_failover(
                            chain, brief, wait, parallel=True
                        )
                    else:
                        rep = run_oneshot_with_failover(
                            chain, brief, wait, parallel=True
                        )
                else:
                    chain = failover_chain(primary)
                    if mode == "interactive":
                        rep = run_interactive_with_failover(
                            chain, brief, wait, parallel=False
                        )
                    else:
                        rep = run_oneshot_with_failover(
                            chain, brief, wait, parallel=False
                        )
            job["status"] = "done" if rep.get("ok") else "failed"
            job["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            job["report"] = {
                "ok": rep.get("ok"),
                "winner_key": rep.get("winner_key") or rep.get("author_key"),
                "quality": rep.get("quality"),
                "result": (rep.get("result") or "")[:800],
                "verify": (rep.get("verify") or "")[:400],
                "error": rep.get("error"),
                "strategy": rep.get("strategy"),
            }
            ran.append({"id": job["id"], "status": job["status"], "report": job["report"]})
            metrics_emit(
                "queue_job",
                {
                    "ok": job["status"] == "done",
                    "job_id": job["id"],
                    "quality": (job.get("report") or {}).get("quality"),
                    "winner_key": (job.get("report") or {}).get("winner_key"),
                    "strategy": (job.get("report") or {}).get("strategy"),
                    "error": (job.get("report") or {}).get("error"),
                    "to": job.get("to"),
                },
            )
        except Exception as e:
            job["status"] = "failed"
            job["error"] = str(e)
            job["finished"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            ran.append({"id": job["id"], "status": "failed", "error": str(e)})
            metrics_emit(
                "queue_job",
                {"ok": False, "job_id": job.get("id"), "error": str(e)},
            )
        _queue_save(items)
    return {
        "ok": all(r.get("status") == "done" for r in ran) if ran else True,
        "ran": ran,
        "remaining_pending": sum(1 for i in items if i.get("status") == "pending"),
        "path": str(QUEUE_PATH),
    }




def metrics_emit(event: str, payload: dict[str, Any] | None = None) -> None:
    """Append one metrics line (best-effort, never raises to caller)."""
    try:
        rec = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "event": event,
            "pid": os.getpid(),
        }
        if payload:
            # keep metrics compact
            for k in (
                "ok",
                "mode",
                "strategy",
                "winner_key",
                "author_key",
                "reviewer_key",
                "quality",
                "workers_ok",
                "failover_used",
                "retries_used",
                "rate_limited",
                "latency_ms",
                "to",
                "mesh",
                "review_passed",
                "error",
                "pending",
                "ran",
                "cycle",
                "job_id",
                "worker",
                "model",
            ):
                if k in payload and payload[k] is not None:
                    rec[k] = payload[k]
        path = METRICS_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def metrics_load(last: int = 50) -> list[dict[str, Any]]:
    path = METRICS_PATH
    if not path.is_file():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    out: list[dict[str, Any]] = []
    for ln in lines[-(max(1, last)) :]:
        ln = ln.strip()
        if not ln:
            continue
        try:
            out.append(json.loads(ln))
        except json.JSONDecodeError:
            continue
    return out


def metrics_summary(last: int = 100) -> dict[str, Any]:
    rows = metrics_load(last=last)
    by_event: dict[str, int] = {}
    by_winner: dict[str, int] = {}
    ok_n = 0
    fail_n = 0
    qualities: list[float] = []
    latencies: list[float] = []
    rate_hits = 0
    for r in rows:
        ev = str(r.get("event") or "unknown")
        by_event[ev] = by_event.get(ev, 0) + 1
        if r.get("ok") is True:
            ok_n += 1
        elif r.get("ok") is False:
            fail_n += 1
        w = r.get("winner_key") or r.get("author_key") or r.get("worker")
        if w:
            by_winner[str(w)] = by_winner.get(str(w), 0) + 1
        if isinstance(r.get("quality"), (int, float)):
            qualities.append(float(r["quality"]))
        if isinstance(r.get("latency_ms"), (int, float)):
            latencies.append(float(r["latency_ms"]))
        if r.get("rate_limited"):
            rate_hits += 1
    avg_q = round(sum(qualities) / len(qualities), 1) if qualities else None
    avg_lat = round(sum(latencies) / len(latencies), 1) if latencies else None
    return {
        "ok": True,
        "path": str(METRICS_PATH),
        "n": len(rows),
        "ok_n": ok_n,
        "fail_n": fail_n,
        "success_rate": round(ok_n / max(1, ok_n + fail_n), 3),
        "by_event": by_event,
        "by_winner": by_winner,
        "avg_quality": avg_q,
        "avg_latency_ms": avg_lat,
        "rate_limited_hits": rate_hits,
        "last": rows[-5:] if rows else [],
    }


def cmd_metrics(args: Any) -> None:
    action = getattr(args, "metrics_cmd", "summary") or "summary"
    last = int(getattr(args, "last", 50) or 50)
    if action == "show":
        rows = metrics_load(last=last)
        rep = {"ok": True, "path": str(METRICS_PATH), "n": len(rows), "items": rows}
    elif action == "summary":
        rep = metrics_summary(last=last)
    elif action == "clear":
        if METRICS_PATH.is_file():
            METRICS_PATH.unlink()
        rep = {"ok": True, "cleared": True, "path": str(METRICS_PATH)}
    else:
        die(f"unknown metrics cmd: {action}")
    print(json.dumps(rep, indent=2, ensure_ascii=False))


def queue_worker_start_bg(
    *,
    interval: float = 15.0,
    limit: int = 1,
    max_cycles: int = 0,
) -> dict[str, Any]:
    """Spawn detached queue worker process; logs to QUEUE_WORKER_LOG."""
    st = queue_worker_status()
    if st.get("running"):
        return {
            "ok": False,
            "error": "worker already running",
            "status": st,
            "hint": "use: queue worker stop",
        }

    script = Path(__file__).resolve()
    log_path = QUEUE_WORKER_LOG
    log_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(script),
        "queue",
        "worker",
        "start",
        "--interval",
        str(interval),
        "--limit",
        str(limit),
    ]
    if max_cycles and max_cycles > 0:
        cmd += ["--max-cycles", str(max_cycles)]

    creationflags = 0
    if os.name == "nt":
        # DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW
        creationflags = 0x00000008 | 0x00000200 | 0x08000000

    with log_path.open("a", encoding="utf-8") as lf:
        lf.write(
            f"\n--- bg start {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} "
            f"cmd={' '.join(cmd)} ---\n"
        )
        lf.flush()
        popen_kw: dict[str, Any] = {
            "args": cmd,
            "stdout": lf,
            "stderr": subprocess.STDOUT,
            "cwd": str(Path.home()),
            "stdin": subprocess.DEVNULL,
        }
        if os.name == "nt":
            popen_kw["creationflags"] = creationflags
        else:
            popen_kw["start_new_session"] = True
        proc = subprocess.Popen(**popen_kw)

    # wait for pid file from child loop
    deadline = time.time() + 8.0
    st2: dict[str, Any] = {"running": False}
    while time.time() < deadline:
        time.sleep(0.4)
        st2 = queue_worker_status()
        if st2.get("running"):
            break

    metrics_emit(
        "worker_bg_start",
        {
            "ok": bool(st2.get("running")),
            "worker": "queue-worker",
            "latency_ms": None,
        },
    )
    return {
        "ok": True,
        "bg": True,
        "spawn_pid": proc.pid,
        "log": str(log_path),
        "cmd": cmd,
        "status": st2,
        "pid_path": str(QUEUE_WORKER_PID),
    }


def queue_worker_loop(
    *,
    interval: float = 15.0,
    limit: int = 1,
    max_cycles: int = 0,
    once: bool = False,
) -> dict[str, Any]:
    """
    Poll queue and run pending jobs.
    max_cycles=0 means forever until Ctrl+C (or once=True → single poll).
    Writes PID to QUEUE_WORKER_PID.
    """
    cycles = 0
    total_ran = 0
    pid_path = QUEUE_WORKER_PID
    try:
        pid_path.parent.mkdir(parents=True, exist_ok=True)
        pid_path.write_text(str(os.getpid()), encoding="utf-8")
    except Exception:
        pass

    report_cycles: list[dict[str, Any]] = []
    try:
        while True:
            cycles += 1
            # ensure free lanes best-effort
            for k in FREE_WORKERS:
                try:
                    ensure_worker(k)
                except Exception:
                    pass
            # interrupt stuck free before taking new work
            try:
                interrupt_free_stuck(force=False, wait_sec=3.0)
            except Exception:
                pass

            pending_n = sum(1 for i in _queue_load() if i.get("status") == "pending")
            if pending_n > 0:
                rep = queue_run(limit=limit)
                n = len(rep.get("ran") or [])
                total_ran += n
                report_cycles.append(
                    {
                        "cycle": cycles,
                        "pending_before": pending_n,
                        "ran": n,
                        "ok": rep.get("ok"),
                        "remaining": rep.get("remaining_pending"),
                    }
                )
                print(
                    json.dumps(
                        {"worker_cycle": cycles, "queue_run": rep},
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                metrics_emit(
                    "worker_cycle",
                    {
                        "ok": rep.get("ok"),
                        "cycle": cycles,
                        "pending": pending_n,
                        "ran": n,
                    },
                )
            else:
                report_cycles.append(
                    {"cycle": cycles, "pending_before": 0, "ran": 0, "idle": True}
                )
                print(
                    json.dumps(
                        {"worker_cycle": cycles, "idle": True, "pending": 0},
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                metrics_emit(
                    "worker_cycle",
                    {"ok": True, "cycle": cycles, "pending": 0, "ran": 0},
                )

            if once:
                break
            if max_cycles and cycles >= max_cycles:
                break
            time.sleep(max(3.0, float(interval)))
    except KeyboardInterrupt:
        report_cycles.append({"stopped": "KeyboardInterrupt", "cycle": cycles})
    finally:
        try:
            if pid_path.is_file() and pid_path.read_text(encoding="utf-8").strip() == str(
                os.getpid()
            ):
                pid_path.unlink(missing_ok=True)
        except Exception:
            pass

    return {
        "ok": True,
        "cycles": cycles,
        "total_ran": total_ran,
        "history": report_cycles[-20:],
        "pid_path": str(pid_path),
    }


def queue_worker_status() -> dict[str, Any]:
    pid_path = QUEUE_WORKER_PID
    if not pid_path.is_file():
        return {"ok": True, "running": False, "pid_path": str(pid_path)}
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except Exception:
        return {
            "ok": False,
            "running": False,
            "error": "bad pid file",
            "pid_path": str(pid_path),
        }
    alive = False
    try:
        if os.name == "nt":
            # OpenProcess check via tasklist is heavy; use os.kill(pid, 0) not on Windows
            import ctypes

            kernel32 = ctypes.windll.kernel32  # type: ignore[attr-defined]
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid)
            if handle:
                alive = True
                kernel32.CloseHandle(handle)
        else:
            os.kill(pid, 0)
            alive = True
    except Exception:
        alive = False
    if not alive:
        try:
            pid_path.unlink(missing_ok=True)
        except Exception:
            pass
    return {
        "ok": True,
        "running": alive,
        "pid": pid,
        "pid_path": str(pid_path),
        "log_path": str(QUEUE_WORKER_LOG),
        "metrics_path": str(METRICS_PATH),
    }


def queue_worker_stop() -> dict[str, Any]:
    st = queue_worker_status()
    if not st.get("running"):
        return {"ok": True, "stopped": False, "reason": "not running", **st}
    pid = st.get("pid")
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/F"],
                capture_output=True,
                text=True,
                check=False,
            )
        else:
            os.kill(int(pid), 15)
        time.sleep(0.5)
    except Exception as e:
        return {"ok": False, "error": str(e), **st}
    try:
        QUEUE_WORKER_PID.unlink(missing_ok=True)
    except Exception:
        pass
    return {"ok": True, "stopped": True, "pid": pid}


def cmd_queue(args: Any) -> None:
    sub = args.queue_cmd
    if sub == "add":
        mr = getattr(args, "max_retries", None)
        ma = getattr(args, "max_attempts", None)
        rep = queue_add(
            args.goal,
            scope=args.scope or "",
            to=args.to,
            mode=args.mode,
            wait=args.wait,
            review=args.review,
            priority=args.priority,
            max_retries=mr,
            max_attempts=ma,
        )
    elif sub == "list":
        rep = queue_list()
    elif sub == "clear":
        rep = queue_clear(done_only=not args.all)
    elif sub == "run":
        for k in FREE_WORKERS:
            try:
                ensure_worker(k)
            except Exception:
                pass
        rep = queue_run(limit=args.limit)
    elif sub == "worker":
        action = getattr(args, "worker_action", "status") or "status"
        if action == "status":
            rep = queue_worker_status()
        elif action == "stop":
            rep = queue_worker_stop()
        elif action == "start":
            interval = float(getattr(args, "interval", 15) or 15)
            limit = int(getattr(args, "limit", 1) or 1)
            max_cycles = int(getattr(args, "max_cycles", 0) or 0)
            once = bool(getattr(args, "once", False))
            bg = bool(getattr(args, "bg", False))
            if once and not max_cycles:
                max_cycles = 1
            if bg:
                rep = queue_worker_start_bg(
                    interval=interval, limit=limit, max_cycles=max_cycles
                )
            else:
                rep = queue_worker_loop(
                    interval=interval,
                    limit=limit,
                    max_cycles=max_cycles,
                    once=once,
                )
        else:
            die(f"unknown worker action: {action}")
    else:
        die(f"unknown queue subcommand: {sub}")
    print(json.dumps(rep, indent=2, ensure_ascii=False))
    if not rep.get("ok"):
        sys.exit(2)


def cmd_status(as_json: bool) -> None:
    refresh_workers(force=False)
    agents = get_agents()
    free_names = {w["name"] for w in FREE_WORKERS.values()}
    rows = []
    for a in agents:
        n = agent_name(a)
        kind = agent_kind(a)
        rows.append(
            {
                "name": n,
                "kind": kind,
                "custom_name": a.get("name"),
                "status": a.get("agent_status"),
                "pane_id": a.get("pane_id"),
                "focused": bool(a.get("focused")),
                "is_free": n in free_names,
                "is_generic_pi": is_generic_pi(a),
            }
        )
    present = {r["name"] for r in rows if r["is_free"]}
    report = {
        "herdr": HERDR_BIN,
        "whitelist": str(WHITE_LIST),
        "roster": str(_roster.ACTIVE_ROSTER),
        "health": str(_roster.HEALTH_PATH),
        "agent_dir": str(AGENT_DIR),
        "agents": rows,
        "missing_free": sorted(free_names - present),
        "free_workers": FREE_WORKERS,
        "failover_order": _roster.failover_order(),
    }
    if as_json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return
    print("IK herd status (health-ranked roster)")
    print(f"  herdr: {HERDR_BIN}")
    print(f"  roster: {_roster.ACTIVE_ROSTER}")
    print(f"  agents: {len(rows)}")
    for r in rows:
        flag = "SLOT" if r["is_free"] else ("pi?" if r["is_generic_pi"] else "other")
        label = r["name"] if r["kind"] == r["name"] else f"{r['name']}({r['kind']})"
        print(f"  - {label:<22} {r['status']:<10} {r['pane_id']:<10} [{flag}]")
    miss = report["missing_free"]
    print(f"  missing slots: {', '.join(miss) if miss else 'none'}")
    print("  active models (by health):")
    for k, w in FREE_WORKERS.items():
        print(
            f"    {k}: {w.get('provider')}/{w.get('model')} "
            f"score={w.get('score', '?')} status={w.get('status', '?')}"
        )


def cmd_ensure(lane: str) -> None:
    refresh_workers(force=False)
    keys: list[str] = []
    if lane == "all":
        keys = list(FREE_WORKERS.keys())
    elif lane == "fast":
        keys = [
            k
            for k, w in FREE_WORKERS.items()
            if w.get("lane") in ("fast", "free") or k == "ling"
        ] or list(FREE_WORKERS.keys())[:1]
    elif lane == "long":
        keys = [
            k
            for k, w in FREE_WORKERS.items()
            if w.get("lane") in ("long", "free", "cloud", "nim") or k == "nemotron"
        ] or list(FREE_WORKERS.keys())
    results = []
    for k in keys:
        if k not in FREE_WORKERS:
            continue
        try:
            results.append(ensure_worker(k))
        except SystemExit as e:
            results.append({"key": k, "error": f"exit {e.code}"})
        except Exception as ex:
            results.append({"key": k, "error": str(ex)[:200]})
    print(json.dumps({"ensure": results}, indent=2, ensure_ascii=False))


def cmd_route(hint: str) -> None:
    h = hint.lower()
    long_kw = (
        "long",
        "context",
        "review",
        "architecture",
        "multi-file",
        "refactor",
        "big",
        "digest",
        "synthesis",
        "research",
    )
    key = "nemotron" if any(k in h for k in long_kw) else "ling"
    w = FREE_WORKERS[key]
    chain = failover_chain(key)
    print(
        json.dumps(
            {
                "key": key,
                "name": w["name"],
                "model": w["model"],
                "provider": w["provider"],
                "reason": "long-ctx keywords"
                if key == "nemotron"
                else "default fast free tools",
                "failover_chain": chain,
                "mesh_workers": list(FREE_WORKERS.keys()),
                "note": "auto=primary+failover; mesh=parallel all free",
            },
            indent=2,
        )
    )


def build_brief(goal: str, scope: str, dod: str, verify: str, simple: bool) -> str:
    if simple:
        # Minimal ping (doctor only). Prefer full brief for real work.
        return (
            f"{goal}\n\n"
            "Reply with EXACTLY this format (nothing else before RESULT):\n"
            "RESULT: <one short line>\n"
            "VERIFY: N/A doctor-ping\n"
        )
    return f"""GOAL: {goal}
SCOPE_PATH: {scope or "C:/Users/anton"}
OUT_OF_SCOPE: secrets, force-push, shutdown, external send without confirm
DoD: {dod or "complete the goal; print RESULT + VERIFY"}
VERIFY_EXPECT: {verify or "state what you checked (command+RC or research-only + sources)"}
REPORT_TO: ik

Rules:
- Use tools when needed; no fake tool_call XML.
- Prefer concrete facts over vague summaries.
- Do NOT invent dollar figures, product launches, or incidents without evidence.
- End EXACTLY with both markers (required):
RESULT: <3-12 concrete facts or bullets>
VERIFY: <checks → RC OR research-only + sources OR N/A <reason>>
"""



def oneshot_run(key: str, brief: str, timeout_sec: int) -> dict[str, Any]:
    refresh_workers(force=False)
    if key not in FREE_WORKERS:
        # best available by health
        order = _roster.failover_order()
        key = order[0] if order else next(iter(FREE_WORKERS), "ling")
    meta = FREE_WORKERS.get(key) or next(iter(FREE_WORKERS.values()))
    env = os.environ.copy()
    if AGENT_DIR.is_dir():
        env["PI_CODING_AGENT_DIR"] = str(AGENT_DIR)

    # Prefer node cli.js (reliable argv) over pi.cmd shell
    if Path(PI_CLI).is_file():
        args = [
            "node",
            PI_CLI,
            "-ne",
            "-p",
            brief,
            "--no-session",
            "--provider",
            meta["provider"],
            "--model",
            meta["model"],
            "--tools",
            TOOLS,
        ]
        shell = False
    else:
        if not Path(PI_CMD).exists() and os.name == "nt":
            die(f"pi not found: {PI_CMD} / {PI_CLI}")
        args = [
            PI_CMD,
            "-ne",
            "-p",
            brief,
            "--no-session",
            "--provider",
            meta["provider"],
            "--model",
            meta["model"],
            "--tools",
            TOOLS,
        ]
        shell = os.name == "nt"

    def _invoke_once() -> dict[str, Any]:
        t0 = time.time()
        try:
            p = subprocess.run(
                args,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=timeout_sec,
                shell=shell,
                env=env,
            )
        except subprocess.TimeoutExpired:
            out = {
                "ok": False,
                "mode": "oneshot",
                "error": f"timeout after {timeout_sec}s",
                "model": meta["model"],
                "provider": meta["provider"],
                "worker": meta["name"],
                "key": key,
                "rate_limited": False,
                "latency_ms": (time.time() - t0) * 1000.0,
            }
            out["quality"] = 0
            record_run_health(out)
            return out

        text = (p.stdout or "") + "\n" + (p.stderr or "")
        rate_hit = looks_rate_limited(text)
        extracted = extract_result(text)
        has_marker = bool(re.search(r"RESULT:\s*\S", text, re.I))
        has_verify = bool(re.search(r"VERIFY:\s*\S", text, re.I)) or bool(
            (extracted.get("verify") or "").strip()
        )
        result_ok = bool((extracted.get("result") or "").strip())
        ok = p.returncode == 0 and has_marker and has_verify and result_ok and not rate_hit
        out = {
            "ok": ok,
            "mode": "oneshot",
            "rc": p.returncode,
            "worker": meta["name"],
            "model": meta["model"],
            "provider": meta["provider"],
            "key": key,
            "result": extracted["result"],
            "verify": extracted["verify"],
            "raw_tail": extracted["raw_tail"],
            "has_result_marker": has_marker,
            "has_verify_marker": has_verify,
            "invoke": "node-cli" if Path(PI_CLI).is_file() else "pi.cmd",
            "rate_limited": rate_hit,
            "latency_ms": (time.time() - t0) * 1000.0,
        }
        if rate_hit and not out.get("error"):
            out["error"] = "rate_limited"
            out["ok"] = False
        out["quality"] = score_result(out)
        record_run_health(out)
        return out

    # v1.4: stagger starts + concurrency cap for free providers
    _stagger_launch()
    acquired = _free_sem.acquire(timeout=max(30, timeout_sec + 30))
    if not acquired:
        return {
            "ok": False,
            "mode": "oneshot",
            "error": "concurrency slot timeout",
            "model": meta["model"],
            "worker": meta["name"],
            "key": key,
            "quality": 0,
        }
    try:
        out = _invoke_once()
        retries = 0
        while (
            (not out.get("ok"))
            and out.get("rate_limited")
            and retries < MAX_RETRIES_429
        ):
            retries += 1
            time.sleep(RATE_BACKOFF_SEC * retries)
            out = _invoke_once()
            out["retries_429"] = retries
        return out
    finally:
        _free_sem.release()



def wait_agent_ready(pane: str, timeout_sec: int = 60) -> str:
    """Wait until agent is idle/done/unknown; return last status."""
    deadline = time.time() + max(5, timeout_sec)
    last = "unknown"
    while time.time() < deadline:
        agents = get_agents()
        a = next((x for x in agents if x.get("pane_id") == pane), None)
        if not a:
            return "missing"
        last = str(a.get("agent_status") or "unknown")
        if last in ("idle", "done", "unknown", "blocked"):
            return last
        # working / busy
        run_herdr_raw(
            [
                "agent",
                "wait",
                pane,
                "--until",
                "idle",
                "--until",
                "done",
                "--timeout",
                str(min(15000, int((deadline - time.time()) * 1000))),
            ]
        )
        time.sleep(0.3)
    return last


def interactive_delegate(
    target_key: str, brief: str, wait_sec: int
) -> dict[str, Any]:
    if target_key in FREE_WORKERS:
        try:
            ensure_worker(target_key)
        except SystemExit as e:
            return {
                "ok": False,
                "mode": "interactive",
                "error": f"ensure failed: {e}",
                "worker": FREE_WORKERS[target_key]["name"],
                "model": FREE_WORKERS[target_key]["model"],
                "key": target_key,
                "quality": 0,
            }
        name = FREE_WORKERS[target_key]["name"]
        model = FREE_WORKERS[target_key]["model"]
    else:
        name = target_key
        model = "?"

    a = find_agent(name)
    if not a:
        return {
            "ok": False,
            "mode": "interactive",
            "error": f"no herdr agent {name}; run ensure first",
            "worker": name,
            "model": model,
            "key": target_key if target_key in FREE_WORKERS else name,
            "quality": 0,
        }

    pane = a["pane_id"]
    status_before = wait_agent_ready(pane, timeout_sec=min(90, max(20, wait_sec // 2)))
    if status_before == "missing":
        return {
            "ok": False,
            "mode": "interactive",
            "error": f"pane vanished: {pane}",
            "worker": name,
            "model": model,
            "key": target_key if target_key in FREE_WORKERS else name,
            "quality": 0,
        }

    # marker so extract prefers post-prompt RESULT (scrollback may be long)
    stamp = f"IK-TASK-{int(time.time())}"
    stamped_brief = f"{brief}\n\nTASK_ID: {stamp}\n"

    timeout_ms = max(10000, wait_sec * 1000)
    p = run_herdr_raw(
        [
            "agent",
            "prompt",
            pane,
            stamped_brief,
            "--wait",
            "--until",
            "idle",
            "--until",
            "done",
            "--until",
            "blocked",
            "--timeout",
            str(timeout_ms),
        ]
    )
    # post-wait status: if still working, do not accept partial scrollback
    status_after = "unknown"
    try:
        agents = get_agents()
        a2 = next((x for x in agents if x.get("pane_id") == pane), None)
        if a2:
            status_after = str(a2.get("agent_status") or "unknown")
    except Exception:
        status_after = "unknown"

    text = pane_text(pane, lines=120)
    # prefer content after stamp if present
    if stamp in text:
        text = text.split(stamp, 1)[-1]
    extracted = extract_result(text)
    has_marker = bool(re.search(r"RESULT:\s*\S", text, re.I))
    has_verify = bool(re.search(r"VERIFY:\s*\S", text, re.I)) or bool(
        (extracted.get("verify") or "").strip()
    )
    rate_hit = looks_rate_limited(text)
    still_working = status_after in ("working", "busy", "running")
    ok = (
        has_marker
        and has_verify
        and bool(extracted.get("result"))
        and not rate_hit
        and not still_working
    )
    out = {
        "ok": ok,
        "mode": "interactive",
        "worker": agent_name(a),
        "pane_id": pane,
        "model": model,
        "key": target_key if target_key in FREE_WORKERS else name,
        "herdr_rc": p.returncode,
        "herdr_out": ((p.stdout or "") + (p.stderr or ""))[-500:],
        "result": extracted["result"],
        "verify": extracted["verify"],
        "raw_tail": extracted["raw_tail"],
        "has_result_marker": has_marker,
        "has_verify_marker": has_verify,
        "status_before": status_before,
        "status_after": status_after,
        "task_id": stamp,
        "rate_limited": rate_hit,
    }
    if rate_hit:
        out["error"] = "rate_limited"
        out["ok"] = False
    elif still_working:
        # v1.5: one soft interrupt pulse, re-read if settled
        key_for_int = target_key if target_key in FREE_WORKERS else name
        try:
            ir = interrupt_agent(key_for_int, wait_sec=6.0, pulses=1, ensure_after=True)
            out["interrupt"] = {
                "ok": ir.get("ok"),
                "status_after": ir.get("status_after"),
                "action": ir.get("action"),
            }
            # re-check status and extract if settled
            a3 = find_agent(pane) or find_agent(name)
            if a3:
                status_after = str(a3.get("agent_status") or status_after)
                out["status_after"] = status_after
            if status_after not in ("working", "busy", "running"):
                text2 = pane_text(pane, lines=120)
                if stamp in text2:
                    text2 = text2.split(stamp, 1)[-1]
                extracted2 = extract_result(text2)
                has_marker2 = bool(re.search(r"RESULT:\s*\S", text2, re.I))
                has_verify2 = bool(re.search(r"VERIFY:\s*\S", text2, re.I)) or bool(
                    (extracted2.get("verify") or "").strip()
                )
                if has_marker2 and has_verify2 and extracted2.get("result"):
                    out["result"] = extracted2["result"]
                    out["verify"] = extracted2["verify"]
                    out["has_result_marker"] = True
                    out["has_verify_marker"] = True
                    out["ok"] = True
                    out["error"] = None
                    out["recovered_after_interrupt"] = True
                else:
                    out["error"] = f"timeout_still_working status={status_after}"
                    out["ok"] = False
            else:
                out["error"] = f"timeout_still_working status={status_after}"
                out["ok"] = False
        except Exception as ex:
            out["error"] = f"timeout_still_working; interrupt_failed: {ex}"
            out["ok"] = False
    out["quality"] = score_result(out)
    return out


def resolve_to_key(to: str, goal: str) -> str:
    if to in ("auto", "mesh", "all", "free", "any-free"):
        h = goal.lower()
        long_kw = (
            "long",
            "context",
            "review",
            "architecture",
            "multi-file",
            "refactor",
            "digest",
            "synthesis",
            "research",
        )
        return "nemotron" if any(k in h for k in long_kw) else "ling"
    if to in FREE_WORKERS:
        return to
    if to in ("pi-ling", "ling-flash"):
        return "ling"
    if to in ("pi-nemotron",):
        return "nemotron"
    return to


def failover_chain(primary: str) -> list[str]:
    """Health-ranked keys: primary first, then best scores (not hardcoded order)."""
    refresh_workers(force=False)
    keys = _roster.failover_order(primary if primary in FREE_WORKERS else None)
    if not keys:
        keys = list(FREE_WORKERS.keys())
    if primary not in FREE_WORKERS:
        return keys
    return [primary] + [k for k in keys if k != primary]


def is_mesh_target(to: str) -> bool:
    return to in ("mesh", "all", "free")



def run_oneshot_with_failover(
    chain: list[str],
    brief: str,
    wait: int,
    *,
    parallel: bool = False,
) -> dict[str, Any]:
    """
    Multi free worker:
      - parallel=True  → staggered concurrent oneshots; quality winner
      - parallel=False → try in order until good-quality ok (auto-substitute)
    """
    attempts: list[dict[str, Any]] = []
    wait_each = wait
    if parallel and len(chain) > 1:
        wait_each = max(60, wait)

    def _one(k: str) -> dict[str, Any]:
        r = oneshot_run(k, brief, wait_each)
        r["key"] = k
        return r

    if parallel and len(chain) > 1:
        results_by_key: dict[str, dict[str, Any]] = {}
        # max_workers still len(chain); semaphore caps real concurrency
        with ThreadPoolExecutor(max_workers=len(chain)) as pool:
            futs = {pool.submit(_one, k): k for k in chain}
            for fut in as_completed(futs):
                k = futs[fut]
                try:
                    r = fut.result()
                except Exception as e:
                    r = {
                        "ok": False,
                        "key": k,
                        "worker": FREE_WORKERS.get(k, {}).get("name", k),
                        "model": FREE_WORKERS.get(k, {}).get("model", "?"),
                        "error": str(e),
                        "mode": "oneshot",
                    }
                results_by_key[k] = r
                attempts.append(
                    {
                        "key": k,
                        "ok": bool(r.get("ok")),
                        "worker": r.get("worker"),
                        "model": r.get("model"),
                        "error": r.get("error"),
                        "rc": r.get("rc"),
                        "has_result_marker": r.get("has_result_marker"),
                        "quality": score_result(r),
                        "rate_limited": r.get("rate_limited"),
                        "retries_429": r.get("retries_429"),
                    }
                )
        rep = mesh_report_from_results(
            chain, results_by_key, attempts, mode="oneshot-mesh"
        )
        return rep

    # Sequential failover: first good-quality success wins; thin ok → try next
    last: dict[str, Any] = {}
    best_ok: dict[str, Any] | None = None
    for i, k in enumerate(chain):
        r = _one(k)
        last = r
        q = score_result(r)
        attempts.append(
            {
                "key": k,
                "ok": bool(r.get("ok")),
                "worker": r.get("worker"),
                "model": r.get("model"),
                "error": r.get("error"),
                "rc": r.get("rc"),
                "has_result_marker": r.get("has_result_marker"),
                "quality": q,
                "rate_limited": r.get("rate_limited"),
                "attempt": i + 1,
            }
        )
        if r.get("ok"):
            if best_ok is None or q > score_result(best_ok):
                best_ok = r
            if not is_thin_success(r) or i == len(chain) - 1:
                return {
                    **r,
                    "mode": "oneshot-failover" if i > 0 else r.get("mode", "oneshot"),
                    "strategy": "sequential-failover",
                    "chain": chain,
                    "attempts": attempts,
                    "failover_used": i > 0,
                    "winner_key": k,
                    "workers_ok": [k],
                    "substituted_from": chain[0] if i > 0 else None,
                    "quality": q,
                    "thin_skipped": False,
                    "rate": {
                        "max_concurrent": MAX_CONCURRENT,
                        "stagger_sec": STAGGER_SEC,
                    },
                }
            continue

        # rate-limited: brief pause before next worker
        if r.get("rate_limited"):
            time.sleep(RATE_BACKOFF_SEC)

    if best_ok is not None:
        bk = best_ok.get("key")
        return {
            **best_ok,
            "mode": "oneshot-failover",
            "strategy": "sequential-failover-thin",
            "chain": chain,
            "attempts": attempts,
            "failover_used": True,
            "winner_key": bk,
            "workers_ok": [a["key"] for a in attempts if a.get("ok")],
            "substituted_from": chain[0] if bk != chain[0] else None,
            "quality": score_result(best_ok),
            "thin_skipped": True,
            "note": "only thin RESULT/VERIFY available; accepted best quality",
            "rate": {
                "max_concurrent": MAX_CONCURRENT,
                "stagger_sec": STAGGER_SEC,
            },
        }

    return {
        "ok": False,
        "mode": "oneshot-failover",
        "strategy": "sequential-failover",
        "chain": chain,
        "attempts": attempts,
        "failover_used": len(chain) > 1,
        "workers_ok": [],
        "error": "all free workers failed",
        "result": last.get("result", ""),
        "verify": last.get("verify", ""),
        "raw_tail": last.get("raw_tail", ""),
        "worker": last.get("worker"),
        "model": last.get("model"),
        "rc": last.get("rc"),
        "quality": score_result(last),
        "rate": {
            "max_concurrent": MAX_CONCURRENT,
            "stagger_sec": STAGGER_SEC,
        },
    }


def run_interactive_with_failover(
    chain: list[str],
    brief: str,
    wait: int,
    *,
    parallel: bool = False,
) -> dict[str, Any]:
    """
    Interactive Herdr panes:
      - parallel=True → prompt all free panes concurrently (mesh)
      - parallel=False → sequential with quality/thin skip + failover
    """
    attempts: list[dict[str, Any]] = []

    def _one(k: str) -> dict[str, Any]:
        try:
            r = interactive_delegate(k, brief, wait)
        except SystemExit as e:
            r = {
                "ok": False,
                "key": k,
                "error": f"agent missing / ensure failed: {e}",
                "mode": "interactive",
                "worker": FREE_WORKERS.get(k, {}).get("name", k),
                "model": FREE_WORKERS.get(k, {}).get("model", "?"),
            }
        except Exception as e:
            r = {
                "ok": False,
                "key": k,
                "error": str(e),
                "mode": "interactive",
                "worker": FREE_WORKERS.get(k, {}).get("name", k),
                "model": FREE_WORKERS.get(k, {}).get("model", "?"),
            }
        r["key"] = k
        r.setdefault("quality", score_result(r))
        return r

    if parallel and len(chain) > 1:
        results_by_key: dict[str, dict[str, Any]] = {}
        with ThreadPoolExecutor(max_workers=len(chain)) as pool:
            futs = {pool.submit(_one, k): k for k in chain}
            for fut in as_completed(futs):
                k = futs[fut]
                try:
                    r = fut.result()
                except Exception as e:
                    r = {
                        "ok": False,
                        "key": k,
                        "error": str(e),
                        "mode": "interactive",
                        "worker": FREE_WORKERS.get(k, {}).get("name", k),
                        "model": FREE_WORKERS.get(k, {}).get("model", "?"),
                    }
                results_by_key[k] = r
                attempts.append(
                    {
                        "key": k,
                        "ok": bool(r.get("ok")),
                        "worker": r.get("worker"),
                        "model": r.get("model"),
                        "error": r.get("error"),
                        "quality": score_result(r),
                        "rate_limited": r.get("rate_limited"),
                        "pane_id": r.get("pane_id"),
                    }
                )
        return mesh_report_from_results(
            chain, results_by_key, attempts, mode="interactive-mesh"
        )

    last: dict[str, Any] = {}
    best_ok: dict[str, Any] | None = None
    for i, k in enumerate(chain):
        r = _one(k)
        last = r
        q = score_result(r)
        attempts.append(
            {
                "key": k,
                "ok": bool(r.get("ok")),
                "worker": r.get("worker"),
                "model": r.get("model"),
                "error": r.get("error"),
                "quality": q,
                "attempt": i + 1,
                "pane_id": r.get("pane_id"),
            }
        )
        if r.get("ok"):
            if best_ok is None or q > score_result(best_ok):
                best_ok = r
            if not is_thin_success(r) or i == len(chain) - 1:
                return {
                    **r,
                    "strategy": "sequential-failover",
                    "chain": chain,
                    "attempts": attempts,
                    "failover_used": i > 0,
                    "winner_key": k,
                    "workers_ok": [k],
                    "substituted_from": chain[0] if i > 0 else None,
                    "quality": q,
                    "thin_skipped": False,
                }
            continue
        if r.get("rate_limited"):
            time.sleep(RATE_BACKOFF_SEC)

    if best_ok is not None:
        bk = best_ok.get("key")
        return {
            **best_ok,
            "mode": "interactive-failover",
            "strategy": "sequential-failover-thin",
            "chain": chain,
            "attempts": attempts,
            "failover_used": True,
            "winner_key": bk,
            "workers_ok": [a["key"] for a in attempts if a.get("ok")],
            "substituted_from": chain[0] if bk != chain[0] else None,
            "quality": score_result(best_ok),
            "thin_skipped": True,
            "note": "only thin interactive RESULT; accepted best quality",
        }

    return {
        "ok": False,
        "mode": "interactive-failover",
        "strategy": "sequential-failover",
        "chain": chain,
        "attempts": attempts,
        "failover_used": len(chain) > 1,
        "workers_ok": [],
        "error": "all free workers failed",
        "result": last.get("result", ""),
        "verify": last.get("verify", ""),
        "raw_tail": last.get("raw_tail", ""),
        "worker": last.get("worker"),
        "model": last.get("model"),
        "quality": score_result(last),
    }


def cmd_delegate(
    to: str,
    goal: str,
    scope: str,
    dod: str,
    verify: str,
    wait: int,
    mode: str,
    simple: bool,
    failover: bool = True,
    parallel: bool = False,
    review: bool = False,
    max_retries: int | None = None,
) -> None:
    t0 = time.time()
    refresh_workers(force=False)
    primary = resolve_to_key(to, goal)
    mesh = is_mesh_target(to)
    # mesh/all always multi; auto uses failover chain by default
    if mesh:
        chain = list(FREE_WORKERS.keys())
        # still order by route primary first
        chain = failover_chain(primary)
        # v1.4: mesh parallel for both oneshot and interactive
        use_parallel = True
        use_failover = True
    elif to == "auto" or failover:
        chain = failover_chain(primary if primary in FREE_WORKERS else "ling")
        use_parallel = parallel
        use_failover = failover
    else:
        # explicit single worker, no substitute unless --failover
        key = primary if primary in FREE_WORKERS else to
        chain = [key] if key in FREE_WORKERS else failover_chain("ling")
        if not failover:
            chain = chain[:1]
        use_parallel = parallel
        use_failover = failover

    # ensure free panes present (best-effort, non-fatal for oneshot)
    for k in chain:
        if k in FREE_WORKERS:
            try:
                ensure_worker(k)
            except SystemExit:
                pass
            except Exception:
                pass

    brief = build_brief(goal, scope, dod, verify, simple=simple)  # v1.3: full brief default

    if review:
        report = author_reviewer_pass(
            goal, scope, wait, mode=mode, max_retries=max_retries
        )
        report["goal"] = goal
        report["scope"] = scope
        report["to"] = to
        report["resolved_primary"] = primary
        report["mesh"] = mesh
        report["review_mode"] = True
        print(json.dumps(report, indent=2, ensure_ascii=False))
        if not report.get("ok"):
            sys.exit(2)
        return

    if mode == "oneshot":
        if use_failover or mesh or use_parallel or len(chain) > 1:
            report = run_oneshot_with_failover(
                chain, brief, wait, parallel=use_parallel or mesh
            )
        else:
            k = chain[0] if chain[0] in FREE_WORKERS else "ling"
            report = oneshot_run(k, brief, wait)
            report["chain"] = [k]
            report["strategy"] = "single"
            report["failover_used"] = False
            report["workers_ok"] = [k] if report.get("ok") else []
            report["quality"] = score_result(report)
    else:
        # interactive: mesh/parallel or sequential failover
        if use_parallel or mesh:
            report = run_interactive_with_failover(
                chain, brief, wait, parallel=True
            )
        elif use_failover or len(chain) > 1:
            report = run_interactive_with_failover(
                chain, brief, wait, parallel=False
            )
        else:
            report = interactive_delegate(
                chain[0] if chain[0] in FREE_WORKERS else to, brief, wait
            )
            report["chain"] = chain[:1]
            report["strategy"] = "single"
            report["failover_used"] = False
            report["quality"] = score_result(report)

    report["goal"] = goal
    report["scope"] = scope
    report["to"] = to
    report["resolved_primary"] = primary
    report["mesh"] = mesh
    report.setdefault("quality", score_result(report))
    report["latency_ms"] = int((time.time() - t0) * 1000)
    metrics_emit(
        "delegate",
        {
            "ok": report.get("ok"),
            "mode": report.get("mode"),
            "strategy": report.get("strategy"),
            "winner_key": report.get("winner_key") or report.get("author_key"),
            "quality": report.get("quality"),
            "workers_ok": report.get("workers_ok"),
            "failover_used": report.get("failover_used"),
            "retries_used": report.get("retries_used"),
            "review_passed": report.get("review_passed"),
            "to": report.get("to"),
            "mesh": report.get("mesh"),
            "worker": report.get("worker"),
            "model": report.get("model"),
            "error": report.get("error"),
            "latency_ms": report.get("latency_ms"),
        },
    )
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if not report.get("ok"):
        sys.exit(2)


def cmd_collect(target: str, lines: int) -> None:
    text = pane_text(target, lines=lines)
    extracted = extract_result(text)
    print(
        json.dumps(
            {"target": target, "chars": len(text), **extracted},
            indent=2,
            ensure_ascii=False,
        )
    )


def cmd_doctor() -> None:
    issues: list[str] = []
    info: dict[str, Any] = {}
    if not Path(HERDR_BIN).exists() and os.name == "nt":
        issues.append(f"herdr missing: {HERDR_BIN}")
    else:
        try:
            ags = get_agents()
            info["agents"] = len(ags)
            info["free"] = [
                agent_name(a)
                for a in ags
                if a.get("name") in {w["name"] for w in FREE_WORKERS.values()}
            ]
        except SystemExit as e:
            issues.append(f"herdr agent list failed: {e}")
        except Exception as e:
            issues.append(f"herdr error: {e}")

    info["pi_cli"] = Path(PI_CLI).is_file()
    info["pi_cmd"] = Path(PI_CMD).exists() if os.name == "nt" else True
    info["agent_dir"] = AGENT_DIR.is_dir()
    models = AGENT_DIR / "models.json"
    if models.is_file():
        try:
            mj = json.loads(models.read_text(encoding="utf-8"))
            info["or_free_in_models"] = "or-free" in (mj.get("providers") or {})
        except Exception as e:
            issues.append(f"models.json: {e}")
    else:
        issues.append("missing ~/.pi/agent/models.json")

    # quick oneshot (simple brief)
    r = oneshot_run(
        "ling",
        "Print exactly:\nRESULT: doctor-ok\nVERIFY: N/A doctor-ping\n",
        90,
    )
    info["oneshot"] = {
        "ok": r.get("ok"),
        "result": r.get("result"),
        "verify": r.get("verify"),
        "quality": score_result(r),
        "rc": r.get("rc"),
        "invoke": r.get("invoke"),
    }
    if not r.get("ok"):
        issues.append(f"oneshot failed: {r}")

    # mesh parallel smoke: both free, quality pick
    try:
        mesh_brief = build_brief(
            "List 3 stable practices for parallel free AI agents (bullets). No web.",
            "research-only",
            "3 bullets + VERIFY",
            "research-only; no code change",
            simple=False,
        )
        mesh = run_oneshot_with_failover(
            list(FREE_WORKERS.keys()), mesh_brief, 100, parallel=True
        )
        info["mesh_parallel"] = {
            "ok": mesh.get("ok"),
            "strategy": mesh.get("strategy"),
            "winner_key": mesh.get("winner_key"),
            "workers_ok": mesh.get("workers_ok"),
            "quality": mesh.get("quality"),
            "rate": mesh.get("rate"),
            "qualities": {
                k: (mesh.get("results") or {}).get(k, {}).get("quality")
                for k in (mesh.get("chain") or [])
            },
        }
        info["rate_config"] = {
            "max_concurrent": MAX_CONCURRENT,
            "stagger_sec": STAGGER_SEC,
            "backoff_sec": RATE_BACKOFF_SEC,
            "max_retries_429": MAX_RETRIES_429,
        }
        if not mesh.get("ok"):
            issues.append(f"mesh parallel smoke failed: {mesh.get('error')}")
        elif score_result(mesh) < MIN_ACCEPT_SCORE:
            issues.append(
                f"mesh winner quality low: {mesh.get('quality')} < {MIN_ACCEPT_SCORE}"
            )
    except Exception as e:
        issues.append(f"mesh smoke error: {e}")

    # unit self-check scoring (no network)
    thin = {
        "ok": True,
        "rc": 0,
        "has_result_marker": True,
        "result": "Digest of AI agents news",
        "verify": "N/A",
    }
    rich = {
        "ok": True,
        "rc": 0,
        "has_result_marker": True,
        "result": "- a\n- b\n- c\n- d with source example.com",
        "verify": "research-only; sources: example.com",
    }
    info["score_selfcheck"] = {
        "thin": score_result(thin),
        "rich": score_result(rich),
        "pass": score_result(rich) > score_result(thin),
    }
    if not info["score_selfcheck"]["pass"]:
        issues.append("score_result selfcheck failed (rich should beat thin)")

    info["rate_selfcheck"] = {
        "detect_429": looks_rate_limited("Error 429 rate limit exceeded"),
        "detect_clean": not looks_rate_limited("RESULT: ok\nVERIFY: N/A"),
    }
    if not info["rate_selfcheck"]["detect_429"] or not info["rate_selfcheck"]["detect_clean"]:
        issues.append("looks_rate_limited selfcheck failed")

    info["queue_path"] = str(QUEUE_PATH)
    info["review_retries"] = MAX_REVIEW_RETRIES
    info["queue_worker"] = queue_worker_status()
    info["interrupt_api"] = True
    info["review_verdict_selfcheck"] = {
        "pass": parse_review_verdict({"result": "PASS ok", "ok": True}),
        "fail": not parse_review_verdict({"result": "FAIL bad", "ok": True}),
    }
    if not info["review_verdict_selfcheck"]["pass"] or not info["review_verdict_selfcheck"]["fail"]:
        issues.append("parse_review_verdict selfcheck failed")
    # soft check: interrupt skip on idle free (should not harm)
    try:
        ir = interrupt_free_stuck(force=False, wait_sec=2.0)
        info["interrupt_smoke"] = {
            "ok": ir.get("ok"),
            "actions": [r.get("action") for r in ir.get("results") or []],
        }
        if not ir.get("ok"):
            issues.append(f"interrupt smoke failed: {ir}")
    except Exception as e:
        issues.append(f"interrupt smoke error: {e}")

    out = {"ok": len(issues) == 0, "issues": issues, "info": info}
    print(json.dumps(out, indent=2, ensure_ascii=False))
    if issues:
        sys.exit(2)


def main() -> None:
    ap = argparse.ArgumentParser(description="IK free agents inside Herdr")
    sub = ap.add_subparsers(dest="cmd", required=True)

    st = sub.add_parser("status")
    st.add_argument("--json", action="store_true")

    en = sub.add_parser("ensure")
    en.add_argument("--lane", choices=["fast", "long", "all"], default="fast")

    rt = sub.add_parser("route")
    rt.add_argument("hint", nargs="+")

    dg = sub.add_parser("delegate")
    dg.add_argument(
        "--to",
        default="auto",
        help="auto|mesh|all|ling|nemotron|NAME  (auto=failover; mesh=parallel all free)",
    )
    dg.add_argument("--goal", required=True)
    dg.add_argument("--scope", default="")
    dg.add_argument("--dod", default="")
    dg.add_argument("--verify", default="")
    dg.add_argument("--wait", type=int, default=120)
    dg.add_argument(
        "--mode", choices=["interactive", "oneshot"], default="oneshot"
    )
    dg.add_argument(
        "--simple",
        action="store_true",
        help="Minimal RESULT/VERIFY brief (best for free models)",
    )
    dg.add_argument(
        "--failover",
        dest="failover",
        action="store_true",
        default=True,
        help="Auto-substitute next free worker on failure (default: on)",
    )
    dg.add_argument(
        "--no-failover",
        dest="failover",
        action="store_false",
        help="Disable substitute chain (single worker only)",
    )
    dg.add_argument(
        "--parallel",
        action="store_true",
        help="Run free chain in parallel (mesh-style); implies multi-agent",
    )
    dg.add_argument(
        "--review",
        action="store_true",
        help="Author≠Reviewer dual-pass (primary authors; other free reviews)",
    )
    dg.add_argument(
        "--retries",
        type=int,
        default=None,
        help="Review FAIL revise rounds (default IK_FREE_MAX_REVIEW_RETRIES or 1)",
    )

    cl = sub.add_parser("collect")
    cl.add_argument("target")
    cl.add_argument("-n", "--lines", type=int, default=60)

    sub.add_parser("doctor", help="Self-check herdr + oneshot free path")

    mt = sub.add_parser("metrics", help="Free-herd metrics JSONL")
    mtsub = mt.add_subparsers(dest="metrics_cmd", required=True)
    ms = mtsub.add_parser("show")
    ms.add_argument("--last", type=int, default=30)
    msum = mtsub.add_parser("summary")
    msum.add_argument("--last", type=int, default=100)
    mtsub.add_parser("clear")

    ro = sub.add_parser(
        "roster",
        help="Show/rebuild LIVE health-ranked active roster (models not frozen)",
    )
    ro.add_argument(
        "action",
        nargs="?",
        default="show",
        choices=["show", "refresh", "pool"],
        help="show active-roster, force rebuild, or full candidate pools",
    )
    hh = sub.add_parser("health", help="Model health scores (live)")
    pb = sub.add_parser(
        "probe",
        help="LIVE probe: stale-first batch over models.json pool (or --all)",
    )
    pb.add_argument("--limit", type=int, default=6)
    pb.add_argument("--timeout", type=int, default=30)
    pb.add_argument(
        "--method",
        choices=["auto", "api", "pi"],
        default="auto",
        help="api=HTTP ping, pi=node cli, auto=api then pi",
    )
    pb.add_argument(
        "--all",
        action="store_true",
        help="Probe entire candidate pool (slow)",
    )
    pb.add_argument(
        "--provider",
        default=None,
        help="Limit to provider: nvidia|or-free|ollama-cloud",
    )
    hw = sub.add_parser(
        "health-worker",
        help="Background worker: probe ALL models over time, rebuild roster",
    )
    hw.add_argument(
        "action",
        nargs="?",
        default="status",
        choices=["start", "stop", "status", "once"],
    )
    hw.add_argument("--interval", type=float, default=300.0)
    hw.add_argument("--batch", type=int, default=6)
    hw.add_argument("--timeout", type=int, default=30)
    hw.add_argument(
        "--method", choices=["auto", "api", "pi"], default="auto"
    )
    hw.add_argument(
        "--bg",
        action="store_true",
        help="Detached background service",
    )

    day = sub.add_parser(
        "day",
        help="Long-running parallel day runner (leave for hours; failover + observe)",
    )
    daysub = day.add_subparsers(dest="day_cmd", required=True)
    daysub.add_parser("status")
    daysub.add_parser("observe")
    daysub.add_parser("stop")
    day_start = daysub.add_parser("start")
    day_start.add_argument("--foreground", action="store_true")
    day_start.add_argument("--parallel", type=int, default=2)
    day_start.add_argument("--interval", type=float, default=20.0)
    day_start.add_argument("--max-cycles", type=int, default=0)
    day_go = daysub.add_parser(
        "go",
        help="Natural language: enqueue goal(s) + start day loop forever",
    )
    day_go.add_argument("--goal", required=True)
    day_go.add_argument("--parallel", type=int, default=2)
    day_go.add_argument("--interval", type=float, default=20.0)
    day_go.add_argument("--priority", type=int, default=80)
    day_go.add_argument("--to", default="auto")
    day_go.add_argument("--wait", type=int, default=180)
    day_go.add_argument("--max-attempts", type=int, default=3)
    day_go.add_argument("--review", action="store_true")
    day_once = daysub.add_parser("once")
    day_once.add_argument("--parallel", type=int, default=2)

    ir = sub.add_parser("interrupt", help="Soft-stop stuck free panes (C-c)")
    ir.add_argument(
        "--target",
        default="all",
        help="free key | agent name | pane | all",
    )
    ir.add_argument(
        "--force",
        action="store_true",
        help="Interrupt even if not working",
    )
    ir.add_argument(
        "--wait",
        type=float,
        default=8.0,
        help="Seconds to wait after C-c",
    )

    q = sub.add_parser("queue", help="File queue for free-herd jobs")
    qsub = q.add_subparsers(dest="queue_cmd", required=True)
    qa = qsub.add_parser("add")
    qa.add_argument("--goal", required=True)
    qa.add_argument("--scope", default="research-only")
    qa.add_argument("--to", default="auto")
    qa.add_argument("--mode", choices=["oneshot", "interactive"], default="oneshot")
    qa.add_argument("--wait", type=int, default=120)
    qa.add_argument("--review", action="store_true")
    qa.add_argument("--priority", type=int, default=50)
    qa.add_argument("--retries", dest="max_retries", type=int, default=None)
    qa.add_argument(
        "--max-attempts",
        type=int,
        default=None,
        help="Job-level retries for day runner (default 3)",
    )
    ql = qsub.add_parser("list")
    qr = qsub.add_parser("run")
    qr.add_argument("--limit", type=int, default=1)
    qw = qsub.add_parser("worker", help="Poll queue daemon (start|status|stop)")
    qw.add_argument(
        "worker_action",
        nargs="?",
        default="status",
        choices=["start", "status", "stop"],
    )
    qw.add_argument("--interval", type=float, default=15.0, help="Seconds between polls")
    qw.add_argument("--limit", type=int, default=1, help="Jobs per cycle")
    qw.add_argument(
        "--max-cycles",
        type=int,
        default=0,
        help="Stop after N cycles (0=forever)",
    )
    qw.add_argument(
        "--once",
        action="store_true",
        help="Single poll cycle then exit",
    )
    qw.add_argument(
        "--bg",
        action="store_true",
        help="Start detached background worker (service mode)",
    )
    qc = qsub.add_parser("clear")
    qc.add_argument(
        "--all",
        action="store_true",
        help="Clear pending too (default: done/failed only)",
    )


    args = ap.parse_args()
    if args.cmd == "status":
        cmd_status(args.json)
    elif args.cmd == "ensure":
        cmd_ensure(args.lane)
    elif args.cmd == "route":
        cmd_route(" ".join(args.hint))
    elif args.cmd == "interrupt":
        cmd_interrupt(args.target, args.force, args.wait)
    elif args.cmd == "metrics":
        cmd_metrics(args)
    elif args.cmd == "queue":
        cmd_queue(args)
    elif args.cmd == "delegate":
        cmd_delegate(
            args.to,
            args.goal,
            args.scope,
            args.dod,
            args.verify,
            args.wait,
            args.mode,
            args.simple,
            failover=args.failover,
            parallel=args.parallel,
            review=args.review,
            max_retries=args.retries,
        )
    elif args.cmd == "collect":
        cmd_collect(args.target, args.lines)
    elif args.cmd == "doctor":
        cmd_doctor()
    elif args.cmd == "roster":
        if args.action == "pool":
            # full live candidate pools
            import subprocess as _sp

            print(
                _sp.check_output(
                    [
                        sys.executable,
                        str(Path(__file__).resolve().parent / "ik_model_roster.py"),
                        "pool",
                    ],
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )
            )
        else:
            if args.action == "refresh":
                workers = refresh_workers(force=True)
            else:
                workers = refresh_workers(force=False)
            print(
                json.dumps(
                    {
                        "source": "ik_model_roster",
                        "path": str(_roster.ACTIVE_ROSTER),
                        "workers": workers,
                        "failover_order": _roster.failover_order(),
                        "live": getattr(_roster, "ROSTER_LIVE", True),
                    },
                    indent=2,
                    ensure_ascii=False,
                )
            )
    elif args.cmd == "health":
        print(json.dumps(_roster.health_summary(), indent=2, ensure_ascii=False))
    elif args.cmd == "probe":
        out = _roster.probe_cycle(
            batch=args.limit,
            timeout_sec=args.timeout,
            method=getattr(args, "method", "auto") or "auto",
            pi_cli=PI_CLI if Path(PI_CLI).is_file() else "",
            all_models=bool(getattr(args, "all", False)),
            provider=getattr(args, "provider", None),
        )
        refresh_workers(force=True)
        print(json.dumps(out, indent=2, ensure_ascii=False))
    elif args.cmd == "health-worker":
        act = getattr(args, "action", "status") or "status"
        if act == "status":
            print(
                json.dumps(
                    _roster.health_worker_status(), indent=2, ensure_ascii=False
                )
            )
        elif act == "stop":
            print(
                json.dumps(
                    _roster.health_worker_stop(), indent=2, ensure_ascii=False
                )
            )
        elif act == "once":
            print(
                json.dumps(
                    _roster.health_worker_loop(
                        interval=args.interval,
                        batch=args.batch,
                        timeout_sec=args.timeout,
                        method=args.method,
                        once=True,
                    ),
                    indent=2,
                    ensure_ascii=False,
                )
            )
        elif act == "start":
            if getattr(args, "bg", False):
                print(
                    json.dumps(
                        _roster.health_worker_start_bg(
                            interval=args.interval,
                            batch=args.batch,
                            timeout_sec=args.timeout,
                            method=args.method,
                        ),
                        indent=2,
                        ensure_ascii=False,
                    )
                )
            else:
                print(
                    json.dumps(
                        _roster.health_worker_loop(
                            interval=args.interval,
                            batch=args.batch,
                            timeout_sec=args.timeout,
                            method=args.method,
                            once=False,
                        ),
                        indent=2,
                        ensure_ascii=False,
                    )
                )
    elif args.cmd == "day":
        try:
            import ik_day_runner as dayr
        except ImportError:
            sys.path.insert(0, str(Path(__file__).resolve().parent))
            import ik_day_runner as dayr
        dcmd = args.day_cmd
        if dcmd == "status":
            print(json.dumps(dayr.day_status(), indent=2, ensure_ascii=False))
        elif dcmd == "observe":
            print(json.dumps(dayr.observe(), indent=2, ensure_ascii=False))
        elif dcmd == "stop":
            print(json.dumps(dayr.day_stop(), indent=2, ensure_ascii=False))
        elif dcmd == "once":
            rep = dayr.run_cycle(parallel=int(args.parallel))
            dayr.write_dashboard(cycle=1, extra={"once": rep})
            print(json.dumps(rep, indent=2, ensure_ascii=False))
        elif dcmd == "start":
            if getattr(args, "foreground", False):
                rep = dayr.day_loop(
                    parallel=int(args.parallel),
                    interval=float(args.interval),
                    max_cycles=int(args.max_cycles or 0),
                )
                print(json.dumps(rep, indent=2, ensure_ascii=False))
            else:
                print(
                    json.dumps(
                        dayr.day_start_bg(
                            parallel=int(args.parallel),
                            interval=float(args.interval),
                            max_cycles=int(args.max_cycles or 0),
                        ),
                        indent=2,
                        ensure_ascii=False,
                    )
                )
        elif dcmd == "go":
            print(
                json.dumps(
                    dayr.day_go(
                        args.goal,
                        parallel=int(getattr(args, "parallel", 2) or 2),
                        interval=float(getattr(args, "interval", 20) or 20),
                        priority=int(getattr(args, "priority", 80) or 80),
                        to=str(getattr(args, "to", "auto") or "auto"),
                        wait=int(getattr(args, "wait", 180) or 180),
                        max_attempts=int(getattr(args, "max_attempts", 3) or 3),
                        review=bool(getattr(args, "review", False)),
                    ),
                    indent=2,
                    ensure_ascii=False,
                )
            )
        else:
            die(f"unknown day cmd: {dcmd}")


if __name__ == "__main__":
    main()
