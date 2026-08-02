#!/usr/bin/env python3
"""
IK Day Runner — long-running parallel mesh without human babysitting.

Goals:
  - Leave for the day; free/slow workers keep chewing the queue
  - Parallel jobs on *different* tasks
  - Fault tolerance: lease reclaim + model failover + job retry with continuation
  - Observe later via status board (JSON + MD)

Does NOT replace ik_herdr_free; uses its queue, oneshot failover, health roster.

Commands:
  python ik_day_runner.py status|observe|start|stop|once
  python ik_herdr_free.py day status|observe|start|stop|once
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

# Import bridge (same directory)
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import ik_herdr_free as ik  # noqa: E402

SYNC_DATA = Path(os.environ.get("IK_SYNC_DATA", str(Path.home() / "Sync" / "Data")))
DAY_PID = Path(os.environ.get("IK_DAY_PID", str(SYNC_DATA / "ik_day_runner.pid")))
DAY_LOG = Path(os.environ.get("IK_DAY_LOG", str(SYNC_DATA / "ik_day_runner.log")))
DAY_STATUS = Path(
    os.environ.get("IK_DAY_STATUS", str(SYNC_DATA / "ik_day_status.json"))
)
DAY_STATUS_MD = Path(
    os.environ.get("IK_DAY_STATUS_MD", str(SYNC_DATA / "ik_day_status.md"))
)

# Defaults tuned for free/slow models
DEFAULT_PARALLEL = max(1, int(os.environ.get("IK_DAY_PARALLEL", "2")))
DEFAULT_INTERVAL = float(os.environ.get("IK_DAY_INTERVAL", "20"))
DEFAULT_LEASE_SEC = int(os.environ.get("IK_DAY_LEASE_SEC", "900"))  # 15 min
DEFAULT_MAX_ATTEMPTS = int(os.environ.get("IK_DAY_MAX_ATTEMPTS", "3"))
DEFAULT_JOB_WAIT = int(os.environ.get("IK_DAY_JOB_WAIT", "180"))

_lock = threading.Lock()


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _now_ts() -> float:
    return time.time()


def _log(msg: str) -> None:
    line = f"[{_now()}] {msg}"
    print(line, flush=True)
    try:
        DAY_LOG.parent.mkdir(parents=True, exist_ok=True)
        with DAY_LOG.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        if os.name == "nt":
            import ctypes

            k = ctypes.windll.kernel32
            h = k.OpenProcess(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
            if not h:
                return False
            k.CloseHandle(h)
            return True
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def load_queue() -> list[dict[str, Any]]:
    return ik._queue_load()


def save_queue(items: list[dict[str, Any]]) -> None:
    ik._queue_save(items)


def reclaim_stale(*, lease_sec: int = DEFAULT_LEASE_SEC) -> list[str]:
    """
    running/claimed jobs past lease or dead owner → pending again (retry),
    not hard-fail (unless attempts exhausted).
    """
    items = load_queue()
    reclaimed: list[str] = []
    now = _now_ts()
    changed = False
    for j in items:
        st = j.get("status")
        if st not in ("running", "claimed"):
            continue
        lease = float(j.get("lease_until") or 0)
        owner = int(j.get("owner_pid") or 0)
        stale = False
        reason = ""
        if lease and lease < now:
            stale = True
            reason = "lease_expired"
        elif owner and not _pid_alive(owner):
            stale = True
            reason = "owner_dead"
        if not stale:
            continue
        attempts = int(j.get("attempts") or 0)
        max_a = int(j.get("max_attempts") or DEFAULT_MAX_ATTEMPTS)
        j["last_error"] = reason
        j["reclaims"] = int(j.get("reclaims") or 0) + 1
        j["progress"] = list(j.get("progress") or []) + [
            {"ts": _now(), "event": "reclaim", "reason": reason, "attempts": attempts}
        ]
        if attempts >= max_a:
            j["status"] = "failed"
            j["error"] = f"max_attempts after {reason}"
            j["finished"] = _now()
        else:
            j["status"] = "pending"
            j.pop("lease_until", None)
            j.pop("owner_pid", None)
            j.pop("started", None)
            # keep partial_result for continuation
        reclaimed.append(str(j.get("id")))
        changed = True
    if changed:
        save_queue(items)
    return reclaimed


def claim_jobs(
    n: int,
    *,
    lease_sec: int = DEFAULT_LEASE_SEC,
    owner_pid: int | None = None,
) -> list[dict[str, Any]]:
    """Atomically claim up to n pending jobs (file lock via single-thread claim)."""
    with _lock:
        items = load_queue()
        pending = sorted(
            [i for i in items if i.get("status") == "pending"],
            key=lambda x: (-int(x.get("priority") or 50), x.get("ts") or ""),
        )
        claimed: list[dict[str, Any]] = []
        pid = owner_pid or os.getpid()
        for j in pending[: max(0, n)]:
            j["status"] = "claimed"
            j["owner_pid"] = pid
            j["lease_until"] = _now_ts() + lease_sec
            j["claimed_at"] = _now()
            j["attempts"] = int(j.get("attempts") or 0) + 1
            j["progress"] = list(j.get("progress") or []) + [
                {
                    "ts": _now(),
                    "event": "claim",
                    "attempt": j["attempts"],
                    "owner": pid,
                }
            ]
            claimed.append(j)
        if claimed:
            save_queue(items)
        return [dict(c) for c in claimed]


def _update_job(job_id: str, mutator) -> dict[str, Any] | None:
    with _lock:
        items = load_queue()
        for j in items:
            if j.get("id") == job_id:
                mutator(j)
                save_queue(items)
                return dict(j)
        return None


def renew_lease(job_id: str, lease_sec: int = DEFAULT_LEASE_SEC) -> None:
    def m(j: dict[str, Any]) -> None:
        j["lease_until"] = _now_ts() + lease_sec
        j["heartbeat"] = _now()

    _update_job(job_id, m)


def build_job_brief(job: dict[str, Any]) -> str:
    goal = str(job.get("goal") or "")
    scope = str(job.get("scope") or "research-only")
    partial = (job.get("partial_result") or "").strip()
    attempt = int(job.get("attempts") or 1)
    if partial and attempt > 1:
        goal = (
            f"CONTINUATION (attempt {attempt}). Previous partial RESULT below — "
            f"complete or improve, do not discard useful facts.\n\n"
            f"PREVIOUS_PARTIAL:\n{partial[:1500]}\n\n"
            f"ORIGINAL_GOAL:\n{goal}"
        )
    return ik.build_brief(
        goal,
        scope,
        job.get("dod") or "complete with RESULT+VERIFY",
        job.get("verify") or "checks or research-only + sources",
        simple=False,
    )


def execute_job(job: dict[str, Any], *, lease_sec: int = DEFAULT_LEASE_SEC) -> dict[str, Any]:
    """
    Run one job with health-ranked failover chain.
    On fail → pending retry (if attempts left) or failed.
    """
    job_id = str(job.get("id"))
    wait = int(job.get("wait") or DEFAULT_JOB_WAIT)
    to = str(job.get("to") or "auto")
    mode = str(job.get("mode") or "oneshot")
    review = bool(job.get("review"))

    def mark_running(j: dict[str, Any]) -> None:
        j["status"] = "running"
        j["started"] = _now()
        j["lease_until"] = _now_ts() + lease_sec
        j["owner_pid"] = os.getpid()

    _update_job(job_id, mark_running)
    renew_lease(job_id, lease_sec)
    _log(f"RUN {job_id} attempt={job.get('attempts')} to={to} review={review}")

    try:
        ik.refresh_workers(force=False)
    except Exception:
        pass

    try:
        if review:
            rep = ik.author_reviewer_pass(
                job.get("goal") or "",
                job.get("scope") or "",
                wait,
                mode=mode,
                max_retries=int(
                    job.get("max_retries")
                    if job.get("max_retries") is not None
                    else ik.MAX_REVIEW_RETRIES
                ),
            )
        else:
            brief = build_job_brief(job)
            primary = ik.resolve_to_key(to, job.get("goal") or "")
            # Prefer health failover order; avoid pinning to one dead model
            if ik.is_mesh_target(to) or to == "auto":
                chain = ik.failover_chain(primary)
            elif to in ik.FREE_WORKERS:
                # still allow failover unless job says no_failover
                if job.get("no_failover"):
                    chain = [to]
                else:
                    chain = ik.failover_chain(to)
            else:
                chain = ik.failover_chain(primary)

            # Skip workers currently in health cooldown when possible
            try:
                healthy = [
                    k
                    for k in chain
                    if (ik.FREE_WORKERS.get(k) or {}).get("status")
                    not in ("cooldown", "banned", "poor")
                ]
                if healthy:
                    chain = healthy + [k for k in chain if k not in healthy]
            except Exception:
                pass

            if mode == "interactive":
                rep = ik.run_interactive_with_failover(
                    chain, brief, wait, parallel=False
                )
            else:
                # sequential failover within job (another model picks up)
                rep = ik.run_oneshot_with_failover(
                    chain, brief, wait, parallel=False
                )

        ok = bool(rep.get("ok"))
        result_text = (rep.get("result") or "")[:2000]
        verify_text = (rep.get("verify") or "")[:800]

        def finish(j: dict[str, Any]) -> None:
            j["finished"] = _now()
            j["partial_result"] = result_text
            j["report"] = {
                "ok": ok,
                "winner_key": rep.get("winner_key") or rep.get("author_key") or rep.get("key"),
                "quality": rep.get("quality"),
                "result": result_text[:800],
                "verify": verify_text[:400],
                "error": rep.get("error"),
                "strategy": rep.get("strategy"),
                "failover_used": rep.get("failover_used"),
                "workers_ok": rep.get("workers_ok"),
                "attempts": j.get("attempts"),
            }
            j["progress"] = list(j.get("progress") or []) + [
                {
                    "ts": _now(),
                    "event": "done" if ok else "fail_attempt",
                    "ok": ok,
                    "winner": j["report"].get("winner_key"),
                    "quality": j["report"].get("quality"),
                }
            ]
            max_a = int(j.get("max_attempts") or DEFAULT_MAX_ATTEMPTS)
            if ok:
                j["status"] = "done"
            elif int(j.get("attempts") or 0) >= max_a:
                j["status"] = "failed"
                j["error"] = rep.get("error") or "attempts_exhausted"
            else:
                # requeue for another agent/model later
                j["status"] = "pending"
                j.pop("lease_until", None)
                j.pop("owner_pid", None)
                j["last_error"] = rep.get("error") or "no RESULT+VERIFY"
                # rotate preferred worker away from failed winner if any
                failed_key = rep.get("key") or rep.get("winner_key")
                if failed_key and j.get("to") == failed_key:
                    j["to"] = "auto"

        _update_job(job_id, finish)
        ik.metrics_emit(
            "day_job",
            {
                "ok": ok,
                "job_id": job_id,
                "quality": rep.get("quality"),
                "winner_key": rep.get("winner_key") or rep.get("key"),
                "strategy": rep.get("strategy"),
                "error": rep.get("error"),
            },
        )
        _log(
            f"{'OK' if ok else 'RETRY/FAIL'} {job_id} "
            f"winner={rep.get('winner_key') or rep.get('key')} q={rep.get('quality')}"
        )
        return {"id": job_id, "ok": ok, "report": rep.get("result", "")[:200]}
    except Exception as e:
        def fail(j: dict[str, Any]) -> None:
            j["last_error"] = str(e)[:300]
            j["progress"] = list(j.get("progress") or []) + [
                {"ts": _now(), "event": "exception", "error": str(e)[:200]}
            ]
            max_a = int(j.get("max_attempts") or DEFAULT_MAX_ATTEMPTS)
            if int(j.get("attempts") or 0) >= max_a:
                j["status"] = "failed"
                j["error"] = str(e)[:300]
                j["finished"] = _now()
            else:
                j["status"] = "pending"
                j.pop("lease_until", None)
                j.pop("owner_pid", None)

        _update_job(job_id, fail)
        _log(f"EXC {job_id}: {e}")
        ik.metrics_emit("day_job", {"ok": False, "job_id": job_id, "error": str(e)[:200]})
        return {"id": job_id, "ok": False, "error": str(e)}


def write_dashboard(
    *,
    cycle: int = 0,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    items = load_queue()
    by_st: dict[str, int] = {}
    for i in items:
        st = str(i.get("status") or "?")
        by_st[st] = by_st.get(st, 0) + 1

    running = [i for i in items if i.get("status") in ("running", "claimed")]
    pending = [i for i in items if i.get("status") == "pending"]
    recent_done = [
        i
        for i in items
        if i.get("status") in ("done", "failed")
    ][-8:]

    try:
        ik.refresh_workers(force=False)
        workers = {
            k: {
                "model": v.get("model"),
                "provider": v.get("provider"),
                "score": v.get("score"),
                "status": v.get("status"),
            }
            for k, v in ik.FREE_WORKERS.items()
        }
        failover = ik._roster.failover_order()
    except Exception:
        workers = {}
        failover = []

    dash = {
        "updated_at": _now(),
        "cycle": cycle,
        "runner_pid": os.getpid() if DAY_PID.is_file() else None,
        "queue_path": str(ik.QUEUE_PATH),
        "counts": by_st,
        "pending": len(pending),
        "running": [
            {
                "id": r.get("id"),
                "goal": (r.get("goal") or "")[:120],
                "attempts": r.get("attempts"),
                "to": r.get("to"),
                "lease_until": r.get("lease_until"),
                "owner_pid": r.get("owner_pid"),
            }
            for r in running
        ],
        "pending_head": [
            {
                "id": p.get("id"),
                "priority": p.get("priority"),
                "goal": (p.get("goal") or "")[:120],
                "attempts": p.get("attempts") or 0,
                "to": p.get("to"),
            }
            for p in sorted(
                pending,
                key=lambda x: (-int(x.get("priority") or 50), x.get("ts") or ""),
            )[:10]
        ],
        "recent": [
            {
                "id": d.get("id"),
                "status": d.get("status"),
                "goal": (d.get("goal") or "")[:100],
                "winner": (d.get("report") or {}).get("winner_key"),
                "quality": (d.get("report") or {}).get("quality"),
                "error": d.get("error") or (d.get("report") or {}).get("error"),
            }
            for d in recent_done
        ],
        "workers": workers,
        "failover_order": failover,
        "extra": extra or {},
    }

    # PID status
    st = day_status()
    dash["daemon"] = st

    DAY_STATUS.parent.mkdir(parents=True, exist_ok=True)
    DAY_STATUS.write_text(
        json.dumps(dash, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    lines = [
        f"# IK Day Runner — status",
        f"",
        f"Updated: **{dash['updated_at']}**  ·  cycle `{cycle}`",
        f"",
        f"## Daemon",
        f"- running: `{st.get('running')}` pid=`{st.get('pid')}`",
        f"- log: `{DAY_LOG}`",
        f"",
        f"## Queue counts",
    ]
    for k, v in sorted(by_st.items()):
        lines.append(f"- **{k}**: {v}")
    lines += ["", "## Active models (health)", ""]
    for k, w in workers.items():
        lines.append(
            f"- `{k}`: {w.get('provider')}/{w.get('model')} "
            f"score={w.get('score')} ({w.get('status')})"
        )
    lines += ["", f"Failover: `{' → '.join(failover)}`", "", "## Running", ""]
    if not running:
        lines.append("_none_")
    for r in dash["running"]:
        lines.append(
            f"- `{r['id']}` attempt={r.get('attempts')} to={r.get('to')} — {r.get('goal')}"
        )
    lines += ["", "## Pending (top)", ""]
    if not dash["pending_head"]:
        lines.append("_empty — add jobs with queue add_")
    for p in dash["pending_head"]:
        lines.append(
            f"- p{p.get('priority')} `{p['id']}` att={p.get('attempts')} — {p.get('goal')}"
        )
    lines += ["", "## Recent finished", ""]
    for d in dash["recent"]:
        lines.append(
            f"- **{d['status']}** `{d['id']}` q={d.get('quality')} "
            f"winner={d.get('winner')} — {d.get('goal')}"
        )
    lines += [
        "",
        "## Observe",
        "```powershell",
        "python C:/Users/anton/agent_tools/ik_day_runner.py observe",
        "python C:/Users/anton/agent_tools/ik_herdr_free.py day status",
        "```",
        "",
    ]
    DAY_STATUS_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return dash


def idle_maintenance(*, do_probe: bool = False) -> dict[str, Any]:
    """When queue empty: optional health probe / soft ensure — no destructive work."""
    out: dict[str, Any] = {"idle": True}
    try:
        ik.refresh_workers(force=False)
        out["workers"] = list(ik.FREE_WORKERS.keys())
    except Exception as e:
        out["refresh_err"] = str(e)[:120]
    if do_probe:
        try:
            # light: rebuild roster only (full probe is expensive)
            ik.refresh_workers(force=True)
            out["roster_refresh"] = True
        except Exception as e:
            out["probe_err"] = str(e)[:120]
    return out


def run_cycle(
    *,
    parallel: int = DEFAULT_PARALLEL,
    lease_sec: int = DEFAULT_LEASE_SEC,
    idle_probe: bool = False,
) -> dict[str, Any]:
    reclaimed = reclaim_stale(lease_sec=lease_sec)
    if reclaimed:
        _log(f"reclaimed: {reclaimed}")

    # Best-effort ensure + interrupt stuck free panes
    try:
        ik.refresh_workers(force=False)
        for k in list(ik.FREE_WORKERS.keys())[:4]:
            try:
                ik.ensure_worker(k)
            except Exception:
                pass
        ik.interrupt_free_stuck(force=False, wait_sec=2.0)
    except Exception:
        pass

    claimed = claim_jobs(parallel, lease_sec=lease_sec)
    if not claimed:
        idle = idle_maintenance(do_probe=idle_probe)
        return {
            "ok": True,
            "ran": 0,
            "reclaimed": reclaimed,
            "idle": True,
            "idle_info": idle,
        }

    results: list[dict[str, Any]] = []
    # Parallel different jobs; each job does sequential model failover inside
    with ThreadPoolExecutor(max_workers=max(1, parallel)) as ex:
        futs = {
            ex.submit(execute_job, j, lease_sec=lease_sec): j.get("id")
            for j in claimed
        }
        for fut in as_completed(futs):
            try:
                results.append(fut.result())
            except Exception as e:
                results.append({"id": futs[fut], "ok": False, "error": str(e)})

    return {
        "ok": all(r.get("ok") for r in results) if results else True,
        "ran": len(results),
        "results": results,
        "reclaimed": reclaimed,
        "idle": False,
    }


def day_loop(
    *,
    parallel: int = DEFAULT_PARALLEL,
    interval: float = DEFAULT_INTERVAL,
    max_cycles: int = 0,
    once: bool = False,
    idle_probe_every: int = 10,
) -> dict[str, Any]:
    DAY_PID.parent.mkdir(parents=True, exist_ok=True)
    DAY_PID.write_text(str(os.getpid()), encoding="utf-8")
    _log(
        f"day_loop start pid={os.getpid()} parallel={parallel} "
        f"interval={interval} max_cycles={max_cycles or '∞'}"
    )
    ik.metrics_emit(
        "day_start",
        {"ok": True, "parallel": parallel, "interval": interval},
    )
    cycles = 0
    history: list[dict[str, Any]] = []
    try:
        while True:
            cycles += 1
            idle_probe = idle_probe_every > 0 and (cycles % idle_probe_every == 0)
            rep = run_cycle(
                parallel=parallel,
                idle_probe=idle_probe,
            )
            history.append(
                {
                    "cycle": cycles,
                    "ts": _now(),
                    "ran": rep.get("ran"),
                    "idle": rep.get("idle"),
                    "ok": rep.get("ok"),
                    "reclaimed": rep.get("reclaimed"),
                }
            )
            write_dashboard(cycle=cycles, extra={"last_cycle": rep})
            _log(
                f"cycle {cycles} ran={rep.get('ran')} idle={rep.get('idle')} "
                f"reclaimed={len(rep.get('reclaimed') or [])}"
            )
            ik.metrics_emit(
                "day_cycle",
                {
                    "ok": rep.get("ok"),
                    "cycle": cycles,
                    "ran": rep.get("ran"),
                    "pending": sum(
                        1 for i in load_queue() if i.get("status") == "pending"
                    ),
                },
            )
            if once:
                break
            if max_cycles and cycles >= max_cycles:
                break
            time.sleep(max(5.0, float(interval)))
    except KeyboardInterrupt:
        _log("KeyboardInterrupt — stopping")
    finally:
        try:
            if DAY_PID.is_file() and DAY_PID.read_text(encoding="utf-8").strip() == str(
                os.getpid()
            ):
                DAY_PID.unlink(missing_ok=True)
        except Exception:
            pass
        write_dashboard(cycle=cycles, extra={"stopped": True})
        ik.metrics_emit("day_stop", {"ok": True, "cycle": cycles})
        _log(f"day_loop stop cycles={cycles}")

    return {
        "ok": True,
        "cycles": cycles,
        "history": history[-30:],
        "status_path": str(DAY_STATUS),
        "status_md": str(DAY_STATUS_MD),
        "log_path": str(DAY_LOG),
    }


def day_status() -> dict[str, Any]:
    if not DAY_PID.is_file():
        return {
            "ok": True,
            "running": False,
            "pid_path": str(DAY_PID),
            "status_path": str(DAY_STATUS),
            "status_md": str(DAY_STATUS_MD),
            "log_path": str(DAY_LOG),
        }
    try:
        pid = int(DAY_PID.read_text(encoding="utf-8").strip())
    except Exception:
        return {
            "ok": False,
            "running": False,
            "error": "bad pid file",
            "pid_path": str(DAY_PID),
        }
    alive = _pid_alive(pid)
    dash = {}
    if DAY_STATUS.is_file():
        try:
            dash = json.loads(DAY_STATUS.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "ok": True,
        "running": alive,
        "pid": pid,
        "pid_path": str(DAY_PID),
        "status_path": str(DAY_STATUS),
        "status_md": str(DAY_STATUS_MD),
        "log_path": str(DAY_LOG),
        "dashboard_updated": dash.get("updated_at"),
        "counts": dash.get("counts"),
        "pending": dash.get("pending"),
        "running_jobs": dash.get("running"),
    }


def day_stop() -> dict[str, Any]:
    st = day_status()
    if not st.get("running"):
        DAY_PID.unlink(missing_ok=True)
        return {"ok": True, "stopped": False, "reason": "not running", **st}
    pid = int(st["pid"])
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
                check=False,
            )
        else:
            os.kill(pid, 15)
    except Exception as e:
        return {"ok": False, "error": str(e), "pid": pid}
    time.sleep(0.8)
    DAY_PID.unlink(missing_ok=True)
    # force requeue any jobs still marked running/claimed
    items = load_queue()
    reclaimed: list[str] = []
    for j in items:
        if j.get("status") in ("running", "claimed"):
            j["status"] = "pending"
            j.pop("lease_until", None)
            j.pop("owner_pid", None)
            j["progress"] = list(j.get("progress") or []) + [
                {"ts": _now(), "event": "reclaim", "reason": "day_stop"}
            ]
            reclaimed.append(str(j.get("id")))
    if reclaimed:
        save_queue(items)
    write_dashboard(cycle=0, extra={"stopped_by_user": True})
    return {
        "ok": True,
        "stopped": True,
        "pid": pid,
        "reclaimed": reclaimed,
        "status_md": str(DAY_STATUS_MD),
    }


def day_go(
    goals: list[str] | str,
    *,
    parallel: int = DEFAULT_PARALLEL,
    interval: float = DEFAULT_INTERVAL,
    priority: int = 80,
    to: str = "auto",
    wait: int = DEFAULT_JOB_WAIT,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    review: bool = False,
) -> dict[str, Any]:
    """
    Natural-language day start for IK chat:
      - enqueue one or more goals (split by blank lines / numbered list)
      - start background day loop (forever until stop)
    User talks to IK; IK calls this — no web UI.
    """
    if isinstance(goals, str):
        text = goals.strip()
        parts: list[str] = []
        # numbered list 1. / 1) / -
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        if len(lines) >= 2 and all(
            re.match(r"^(\d+[\.\)]\s+|[-*•]\s+)", ln) for ln in lines
        ):
            for ln in lines:
                parts.append(re.sub(r"^(\d+[\.\)]\s+|[-*•]\s+)", "", ln).strip())
        elif "\n\n" in text:
            parts = [p.strip() for p in text.split("\n\n") if p.strip()]
        else:
            parts = [text]
    else:
        parts = [str(g).strip() for g in goals if str(g).strip()]

    if not parts:
        return {"ok": False, "error": "empty goals"}

    jobs = []
    for i, g in enumerate(parts):
        rep = ik.queue_add(
            g,
            scope="day-go",
            to=to,
            mode="oneshot",
            wait=wait,
            review=review,
            priority=max(1, int(priority) - i),
            max_attempts=max_attempts,
        )
        jobs.append(rep.get("job"))

    started = day_start_bg(parallel=parallel, interval=interval, max_cycles=0)
    write_dashboard(
        cycle=0,
        extra={"day_go": True, "jobs": [j.get("id") for j in jobs if j]},
    )
    return {
        "ok": True,
        "enqueued": len(jobs),
        "jobs": jobs,
        "day": started,
        "observe": str(DAY_STATUS_MD),
        "hint": "Уходи. По возвращении: python ik_herdr_free.py day observe | stop",
    }


def day_start_bg(
    *,
    parallel: int = DEFAULT_PARALLEL,
    interval: float = DEFAULT_INTERVAL,
    max_cycles: int = 0,
) -> dict[str, Any]:
    st = day_status()
    if st.get("running"):
        return {
            "ok": False,
            "error": "day runner already running",
            "status": st,
            "hint": "python ik_day_runner.py stop",
        }
    DAY_LOG.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(Path(__file__).resolve()),
        "start",
        "--foreground",
        "--parallel",
        str(parallel),
        "--interval",
        str(interval),
    ]
    if max_cycles > 0:
        cmd += ["--max-cycles", str(max_cycles)]

    creationflags = 0
    if os.name == "nt":
        creationflags = 0x00000008 | 0x00000200 | 0x08000000  # DETACHED|NEWGROUP|NOWINDOW

    with DAY_LOG.open("a", encoding="utf-8") as lf:
        lf.write(f"\n--- bg day start {_now()} {' '.join(cmd)} ---\n")
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
        subprocess.Popen(**popen_kw)

    time.sleep(1.2)
    st2 = day_status()
    return {
        "ok": bool(st2.get("running")),
        "started": True,
        "status": st2,
        "log_path": str(DAY_LOG),
        "observe": str(DAY_STATUS_MD),
        "hint": "Add jobs: python ik_herdr_free.py queue add --goal '...' --to auto --priority 50",
    }


def observe() -> dict[str, Any]:
    dash = write_dashboard(cycle=0)
    md = DAY_STATUS_MD.read_text(encoding="utf-8") if DAY_STATUS_MD.is_file() else ""
    return {
        "ok": True,
        "status": day_status(),
        "dashboard": dash,
        "status_md_path": str(DAY_STATUS_MD),
        "status_md_preview": md[:2500],
    }


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser(description="IK long-running parallel day runner")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status")
    sub.add_parser("observe")
    sub.add_parser("stop")

    st = sub.add_parser("start", help="Start day runner (background by default)")
    st.add_argument("--foreground", action="store_true")
    st.add_argument("--parallel", type=int, default=DEFAULT_PARALLEL)
    st.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    st.add_argument("--max-cycles", type=int, default=0)

    go = sub.add_parser(
        "go",
        help="Enqueue natural-language goal(s) and start day loop forever",
    )
    go.add_argument("--goal", required=True, help="Task text (or multi-line list)")
    go.add_argument("--parallel", type=int, default=DEFAULT_PARALLEL)
    go.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    go.add_argument("--priority", type=int, default=80)
    go.add_argument("--to", default="auto")
    go.add_argument("--wait", type=int, default=DEFAULT_JOB_WAIT)
    go.add_argument("--max-attempts", type=int, default=DEFAULT_MAX_ATTEMPTS)
    go.add_argument("--review", action="store_true")

    onc = sub.add_parser("once", help="Single cycle then exit")
    onc.add_argument("--parallel", type=int, default=DEFAULT_PARALLEL)

    rec = sub.add_parser("reclaim", help="Reclaim stale leases only")
    rec.add_argument("--lease-sec", type=int, default=DEFAULT_LEASE_SEC)

    args = ap.parse_args()
    if args.cmd == "status":
        print(json.dumps(day_status(), indent=2, ensure_ascii=False))
    elif args.cmd == "observe":
        print(json.dumps(observe(), indent=2, ensure_ascii=False))
    elif args.cmd == "stop":
        print(json.dumps(day_stop(), indent=2, ensure_ascii=False))
    elif args.cmd == "reclaim":
        r = reclaim_stale(lease_sec=args.lease_sec)
        print(json.dumps({"ok": True, "reclaimed": r}, indent=2))
    elif args.cmd == "once":
        rep = run_cycle(parallel=args.parallel)
        write_dashboard(cycle=1, extra={"once": rep})
        print(json.dumps(rep, indent=2, ensure_ascii=False))
    elif args.cmd == "start":
        if args.foreground:
            rep = day_loop(
                parallel=args.parallel,
                interval=args.interval,
                max_cycles=args.max_cycles,
            )
            print(json.dumps(rep, indent=2, ensure_ascii=False))
        else:
            print(
                json.dumps(
                    day_start_bg(
                        parallel=args.parallel,
                        interval=args.interval,
                        max_cycles=args.max_cycles,
                    ),
                    indent=2,
                    ensure_ascii=False,
                )
            )
    elif args.cmd == "go":
        print(
            json.dumps(
                day_go(
                    args.goal,
                    parallel=args.parallel,
                    interval=args.interval,
                    priority=args.priority,
                    to=args.to,
                    wait=args.wait,
                    max_attempts=args.max_attempts,
                    review=args.review,
                ),
                indent=2,
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
