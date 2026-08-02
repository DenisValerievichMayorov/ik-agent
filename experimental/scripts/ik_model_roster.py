#!/usr/bin/env python3
"""
Dynamic model roster for IK herd — LIVE health-ranked (not sticky preferred).

Sources:
  - white-list.json  → slots (name, provider, lane); model = seed only
  - models.json      → LIVE candidate pool per provider
  - black-list.json  → banned models/providers
  - ik_model_health.json → live success/quality/latency/cooldown

Outputs:
  - Sync/Configs/pi/active-roster.json  → current workers (what bridge uses)
  - Sync/Data/ik_model_health.json      → scores

Live policy (default IK_ROSTER_LIVE=1):
  - white-list model is a *seed prior* (tiny bonus), NOT a freeze
  - highest health score among provider candidates wins the slot
  - anti-collapse: slots sharing a provider get different models when possible
  - background health-worker probes ALL candidates (API ping, round-robin)

Score (0–100-ish):
  0.40 * success_rate*100 + 0.30 * avg_quality + 0.15 * recency
  + 0.15 * latency_score − rate-limit/fail-streak penalties
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SYNC = Path(os.environ.get("IK_SYNC", str(Path.home() / "Sync")))
CONFIG_PI = Path(
    os.environ.get("IK_PI_CONFIG", str(SYNC / "Configs" / "pi"))
)
WHITE_LIST = Path(
    os.environ.get("IK_PI_WHITELIST", str(CONFIG_PI / "white-list.json"))
)
BLACK_LIST = Path(
    os.environ.get("IK_PI_BLACKLIST", str(CONFIG_PI / "black-list.json"))
)
MODELS_JSON = Path(
    os.environ.get("IK_PI_MODELS", str(CONFIG_PI / "models.json"))
)
ACTIVE_ROSTER = Path(
    os.environ.get(
        "IK_ACTIVE_ROSTER", str(CONFIG_PI / "active-roster.json")
    )
)
HEALTH_PATH = Path(
    os.environ.get(
        "IK_MODEL_HEALTH",
        str(SYNC / "Data" / "ik_model_health.json"),
    )
)
HEALTH_WORKER_PID = Path(
    os.environ.get(
        "IK_MODEL_HEALTH_WORKER_PID",
        str(SYNC / "Data" / "ik_model_health_worker.pid"),
    )
)
HEALTH_WORKER_LOG = Path(
    os.environ.get(
        "IK_MODEL_HEALTH_WORKER_LOG",
        str(SYNC / "Data" / "ik_model_health_worker.log"),
    )
)
HEALTH_CURSOR_PATH = Path(
    os.environ.get(
        "IK_MODEL_HEALTH_CURSOR",
        str(SYNC / "Data" / "ik_model_health_cursor.json"),
    )
)

# Live ranking knobs (env-overridable)
def _env_bool(name: str, default: bool) -> bool:
    v = os.environ.get(name)
    if v is None:
        return default
    return str(v).strip().lower() not in ("0", "false", "no", "off")


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except (TypeError, ValueError):
        return default


ROSTER_LIVE = _env_bool("IK_ROSTER_LIVE", True)
# sticky margin: 0 = pure live; legacy freeze was 15
SWITCH_MARGIN = _env_float("IK_SWITCH_MARGIN", 0.0 if ROSTER_LIVE else 15.0)
# seed prior for white-list model (not a freeze)
PREFERRED_BONUS = _env_float("IK_PREFERRED_BONUS", 3.0 if ROSTER_LIVE else 15.0)
# re-probe if last_probe older than this many hours
STALE_HOURS = _env_float("IK_PROBE_STALE_HOURS", 6.0)
DEFAULT_PROBE_BATCH = _env_int("IK_PROBE_BATCH", 6)
DEFAULT_WATCH_INTERVAL = _env_float("IK_HEALTH_WATCH_INTERVAL", 300.0)

# Seed slots if white-list missing (bootstrap only; prefer file)
_SEED_SLOTS: list[dict[str, str]] = [
    {
        "key": "ling",
        "name": "pi-ling",
        "provider": "or-free",
        "model": "inclusionai/ling-3.0-flash:free",
        "lane": "fast",
    },
    {
        "key": "nemotron",
        "name": "pi-nemotron",
        "provider": "or-free",
        "model": "nvidia/nemotron-3-super-120b-a12b:free",
        "lane": "long",
    },
    {
        "key": "ollama-cloud",
        "name": "pi-ollama-cloud",
        "provider": "ollama-cloud",
        "model": "minimax-m3",
        "lane": "cloud",
    },
    {
        "key": "nvidia",
        "name": "pi-nvidia",
        "provider": "nvidia",
        "model": "openai/gpt-oss-20b",
        "lane": "nim",
    },
]

COOLDOWN_FAILS = 3
COOLDOWN_SEC = 30 * 60
RATE_LIMIT_COOLDOWN_SEC = 10 * 60
DEFAULT_PI_CLI = (
    r"C:\Users\anton\AppData\Roaming\npm\node_modules"
    r"\@earendil-works\pi-coding-agent\dist\cli.js"
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_ts() -> float:
    return time.time()


def _load_json(path: Path, default: Any) -> Any:
    if not path.is_file():
        return default
    try:
        raw = path.read_text(encoding="utf-8-sig")
        return json.loads(raw)
    except Exception:
        return default


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def _slot_key_from_name(name: str) -> str:
    n = name.lower().strip()
    if n.startswith("pi-"):
        n = n[3:]
    return re.sub(r"[^a-z0-9_-]+", "-", n) or "worker"


def _lane_from_type(t: str, provider: str) -> str:
    t = (t or "").lower()
    if "fast" in t:
        return "fast"
    if "long" in t:
        return "long"
    if provider == "or-free":
        return "free"
    if provider == "ollama-cloud":
        return "cloud"
    if provider == "nvidia":
        return "nim"
    return t or "general"


def load_blacklist() -> tuple[set[str], set[str]]:
    """Return (banned_providers, banned_model_ids)."""
    data = _load_json(BLACK_LIST, {})
    providers = {
        str(p.get("provider") or p).lower()
        for p in (data.get("providers_removed") or [])
        if p
    }
    # also bare strings
    for p in data.get("providers") or []:
        providers.add(str(p).lower())
    models = set()
    for m in data.get("models") or []:
        if isinstance(m, dict):
            mid = m.get("model") or m.get("id")
            if mid:
                models.add(str(mid).lower())
        else:
            models.add(str(m).lower())
    # hard policy
    providers |= {"cerebras", "mistral", "groq", "ollama"}  # local ollama out
    return providers, models


def load_slots() -> list[dict[str, str]]:
    """Slots from white-list (structural). Models may be overridden by health."""
    data = _load_json(WHITE_LIST, {})
    agents = data.get("agents") or []
    if not agents:
        return [dict(s) for s in _SEED_SLOTS]
    banned_p, banned_m = load_blacklist()
    slots: list[dict[str, str]] = []
    for a in agents:
        provider = str(a.get("provider") or "").strip()
        model = str(a.get("model") or "").strip()
        name = str(a.get("name") or "").strip()
        if not provider or not name:
            continue
        if provider.lower() in banned_p:
            continue
        if model.lower() in banned_m:
            # keep slot but model will be re-picked from candidates
            model = ""
        key = _slot_key_from_name(name)
        lane = _lane_from_type(str(a.get("type") or ""), provider)
        slots.append(
            {
                "key": key,
                "name": name,
                "provider": provider,
                "model": model,
                "lane": lane,
                "notes": str(a.get("notes") or ""),
            }
        )
    return slots or [dict(s) for s in _SEED_SLOTS]


def load_candidates() -> dict[str, list[str]]:
    """provider -> list of model ids from models.json (+ whitelist seeds)."""
    banned_p, banned_m = load_blacklist()
    out: dict[str, list[str]] = {}
    data = _load_json(MODELS_JSON, {})
    providers = data.get("providers") or {}
    for prov, meta in providers.items():
        pl = str(prov).lower()
        if pl in banned_p:
            continue
        ids: list[str] = []
        for m in meta.get("models") or []:
            mid = str(m.get("id") or m.get("model") or "").strip()
            if not mid:
                continue
            if mid.lower() in banned_m:
                continue
            ids.append(mid)
        if ids:
            out[str(prov)] = ids
    # ensure white-list models appear even if not in models.json
    for s in load_slots():
        p, m = s["provider"], s.get("model") or ""
        if not m:
            continue
        if m.lower() in banned_m or p.lower() in banned_p:
            continue
        out.setdefault(p, [])
        if m not in out[p]:
            out[p].insert(0, m)
    return out


def _mid(provider: str, model: str) -> str:
    return f"{provider}::{model}"


def empty_health() -> dict[str, Any]:
    return {
        "version": 1,
        "updated_at": _now_iso(),
        "entries": {},
        "history": [],  # last N events (trimmed)
    }


def load_health() -> dict[str, Any]:
    h = _load_json(HEALTH_PATH, None)
    if not isinstance(h, dict) or "entries" not in h:
        return empty_health()
    h.setdefault("version", 1)
    h.setdefault("entries", {})
    h.setdefault("history", [])
    return h


def save_health(h: dict[str, Any]) -> None:
    h["updated_at"] = _now_iso()
    # cap history
    hist = h.get("history") or []
    if len(hist) > 200:
        h["history"] = hist[-200:]
    _save_json(HEALTH_PATH, h)


def _entry(
    provider: str, model: str, *, key: str = "", lane: str = ""
) -> dict[str, Any]:
    return {
        "provider": provider,
        "model": model,
        "key": key,
        "lane": lane,
        "success": 0,
        "fail": 0,
        "rate_limited": 0,
        "quality_sum": 0.0,
        "quality_n": 0,
        "latency_sum_ms": 0.0,
        "latency_n": 0,
        "fail_streak": 0,
        "last_ok": None,
        "last_fail": None,
        "last_probe": None,
        "last_rate_limit": None,
        "cooldown_until": 0.0,
        "status": "unknown",
        "probe_method": None,
    }


def ensure_entry(
    h: dict[str, Any],
    provider: str,
    model: str,
    *,
    key: str = "",
    lane: str = "",
) -> dict[str, Any]:
    mid = _mid(provider, model)
    e = h["entries"].get(mid)
    if not e:
        e = _entry(provider, model, key=key, lane=lane)
        h["entries"][mid] = e
    if key:
        e["key"] = key
    if lane:
        e["lane"] = lane
    return e


def _iso_age_hours(iso: str | None) -> float | None:
    if not iso:
        return None
    try:
        t = datetime.fromisoformat(str(iso).replace("Z", "+00:00")).timestamp()
        return max(0.0, (_now_ts() - t) / 3600.0)
    except Exception:
        return None


def compute_score(e: dict[str, Any]) -> float:
    """Higher is better. Cooldown / banned → negative. Includes latency."""
    banned_p, banned_m = load_blacklist()
    if e.get("provider", "").lower() in banned_p:
        return -1000.0
    if str(e.get("model", "")).lower() in banned_m:
        return -1000.0
    cd = float(e.get("cooldown_until") or 0)
    if cd > _now_ts():
        return -100.0 - (cd - _now_ts()) / 60.0

    total = int(e.get("success", 0)) + int(e.get("fail", 0))
    if total == 0:
        # untested: neutral prior (must be probed to compete)
        return 35.0

    success_rate = int(e.get("success", 0)) / total
    qn = int(e.get("quality_n", 0)) or 0
    avg_q = (float(e.get("quality_sum", 0)) / qn) if qn else 50.0

    # recency: last_ok within ~25h → bonus; also last_probe keeps "freshness"
    recency = 20.0
    age_ok = _iso_age_hours(e.get("last_ok"))
    if age_ok is not None:
        recency = max(0.0, 100.0 - age_ok * 4.0)
    age_probe = _iso_age_hours(e.get("last_probe"))
    if age_probe is not None and age_probe > STALE_HOURS:
        recency = min(recency, max(0.0, 100.0 - age_probe * 5.0))

    # latency score: lower avg ms → higher (caps at ~10s for 0)
    latency_score = 50.0
    ln = int(e.get("latency_n", 0) or 0)
    if ln > 0:
        avg_ms = float(e.get("latency_sum_ms", 0)) / ln
        # 500ms → ~95, 3s → ~70, 10s → 0
        latency_score = max(0.0, min(100.0, 100.0 - (avg_ms / 100.0)))

    score = (
        0.40 * success_rate * 100.0
        + 0.30 * float(avg_q)
        + 0.15 * recency
        + 0.15 * latency_score
    )
    rl = int(e.get("rate_limited", 0))
    score -= min(45.0, rl * 8.0)
    streak = int(e.get("fail_streak", 0))
    if streak >= 2:
        score -= streak * 10.0
    return round(score, 2)


def refresh_status(e: dict[str, Any]) -> str:
    banned_p, banned_m = load_blacklist()
    if e.get("provider", "").lower() in banned_p or str(
        e.get("model", "")
    ).lower() in banned_m:
        e["status"] = "banned"
        return "banned"
    if float(e.get("cooldown_until") or 0) > _now_ts():
        e["status"] = "cooldown"
        return "cooldown"
    total = int(e.get("success", 0)) + int(e.get("fail", 0))
    sc = compute_score(e)
    if total == 0:
        e["status"] = "untested"
    elif sc >= 60:
        e["status"] = "active"
    elif sc >= 35:
        e["status"] = "degraded"
    else:
        e["status"] = "poor"
    e["score"] = sc
    return e["status"]


def record_outcome(
    provider: str,
    model: str,
    *,
    ok: bool,
    quality: float = 0.0,
    rate_limited: bool = False,
    latency_ms: float | None = None,
    error: str | None = None,
    key: str = "",
    lane: str = "",
) -> dict[str, Any]:
    """Update health after a run. Returns updated entry + score."""
    h = load_health()
    e = ensure_entry(h, provider, model, key=key, lane=lane)
    if rate_limited:
        e["rate_limited"] = int(e.get("rate_limited", 0)) + 1
        e["fail"] = int(e.get("fail", 0)) + 1
        e["fail_streak"] = int(e.get("fail_streak", 0)) + 1
        e["last_rate_limit"] = _now_iso()
        e["last_fail"] = _now_iso()
        e["cooldown_until"] = max(
            float(e.get("cooldown_until") or 0),
            _now_ts() + RATE_LIMIT_COOLDOWN_SEC,
        )
        ok = False
    elif ok:
        e["success"] = int(e.get("success", 0)) + 1
        e["fail_streak"] = 0
        e["last_ok"] = _now_iso()
        if quality is not None:
            e["quality_sum"] = float(e.get("quality_sum", 0)) + float(quality)
            e["quality_n"] = int(e.get("quality_n", 0)) + 1
    else:
        e["fail"] = int(e.get("fail", 0)) + 1
        e["fail_streak"] = int(e.get("fail_streak", 0)) + 1
        e["last_fail"] = _now_iso()
        if int(e.get("fail_streak", 0)) >= COOLDOWN_FAILS:
            e["cooldown_until"] = max(
                float(e.get("cooldown_until") or 0),
                _now_ts() + COOLDOWN_SEC,
            )

    if latency_ms is not None:
        e["latency_sum_ms"] = float(e.get("latency_sum_ms", 0)) + float(
            latency_ms
        )
        e["latency_n"] = int(e.get("latency_n", 0)) + 1

    e["last_probe"] = _now_iso()
    if error is not None:
        e["last_error"] = str(error)[:200]
    refresh_status(e)
    h["history"].append(
        {
            "ts": _now_iso(),
            "provider": provider,
            "model": model,
            "key": key,
            "ok": ok,
            "quality": quality,
            "rate_limited": rate_limited,
            "error": (error or "")[:200] or None,
            "score": e.get("score"),
        }
    )
    save_health(h)
    # re-rank roster after every outcome
    roster = rebuild_roster(persist=True)
    return {"entry": e, "score": e.get("score"), "roster_keys": list(roster.keys())}


def pick_best_model(
    provider: str,
    candidates: list[str],
    *,
    preferred: str = "",
    key: str = "",
    lane: str = "",
    switch_margin: float | None = None,
    exclude: set[str] | None = None,
) -> tuple[str, float]:
    """
    Pick model for a *slot* from LIVE candidate pool.

    Live policy (default):
      - highest health score wins
      - preferred = tiny seed bonus only (PREFERRED_BONUS), not a freeze
      - switch_margin=0 → no sticky home model
      - exclude → anti-collapse (models already taken by sibling slots)

    Legacy sticky: set IK_ROSTER_LIVE=0 or IK_SWITCH_MARGIN=15.
    """
    margin = SWITCH_MARGIN if switch_margin is None else float(switch_margin)
    excl = {x.lower() for x in (exclude or set()) if x}
    h = load_health()
    banned_p, banned_m = load_blacklist()
    if provider.lower() in banned_p:
        return preferred or (candidates[0] if candidates else ""), -1000.0

    ranked: list[tuple[float, str, str]] = []  # score, model, status
    seen: set[str] = set()
    pool = list(candidates)
    if preferred and preferred not in pool:
        pool.insert(0, preferred)

    for idx, m in enumerate(pool):
        if not m or m in seen:
            continue
        seen.add(m)
        if m.lower() in banned_m:
            continue
        if m.lower() in excl:
            continue
        e = ensure_entry(h, provider, m, key=key, lane=lane)
        # always recompute live (ignore stale cached score)
        st = refresh_status(e)
        sc = float(compute_score(e))
        e["score"] = sc
        if preferred and m == preferred:
            sc += PREFERRED_BONUS
        # list-order soft tie-break (models.json order = preference among equals)
        sc -= idx * 0.05
        # hard-skip unusable if we still have alternatives later
        if st == "banned":
            sc = -1000.0
        elif st == "cooldown":
            sc = min(sc, -100.0)  # never beat a healthy model
        elif st == "poor":
            sc -= 25.0
        ranked.append((sc, m, st))

    # if everything excluded, retry without exclude
    if not ranked and excl:
        return pick_best_model(
            provider,
            candidates,
            preferred=preferred,
            key=key,
            lane=lane,
            switch_margin=margin,
            exclude=None,
        )

    if not ranked:
        return preferred or "", 0.0
    ranked.sort(key=lambda x: (-x[0], x[1]))
    best_sc, best_m, _best_st = ranked[0]

    # Legacy sticky mode only when margin > 0
    if margin > 0 and preferred and preferred.lower() not in banned_m:
        if preferred.lower() not in excl:
            e_home = ensure_entry(h, provider, preferred, key=key, lane=lane)
            sc_home = compute_score(e_home) + PREFERRED_BONUS
            st_home = refresh_status(e_home)
            if st_home not in ("banned", "cooldown", "poor") and sc_home >= 0:
                if best_m == preferred or best_sc < sc_home + margin:
                    return preferred, sc_home - PREFERRED_BONUS

    if best_sc < -50 and preferred and preferred.lower() not in excl:
        return preferred, best_sc
    # return raw score without seed bonus for reporting
    e_best = ensure_entry(h, provider, best_m, key=key, lane=lane)
    return best_m, float(e_best.get("score") or compute_score(e_best))


def rebuild_roster(*, persist: bool = True) -> dict[str, dict[str, Any]]:
    """
    Build workers dict: key -> {name, provider, model, lane, score, status, notes}
    LIVE: model = best health among models.json candidates for provider.
    Anti-collapse when multiple slots share a provider.
    """
    slots = load_slots()
    cands = load_candidates()
    h = load_health()
    workers: dict[str, dict[str, Any]] = {}
    used_by_provider: dict[str, set[str]] = {}

    # Prefer assigning higher-score slots first within same provider pool
    # so anti-collapse leaves weaker slots with next-best models.
    def _slot_seed_score(s: dict[str, str]) -> float:
        pool = list(cands.get(s["provider"]) or [])
        pref = s.get("model") or ""
        if not pool and not pref:
            return -999.0
        m, sc = pick_best_model(
            s["provider"],
            pool,
            preferred=pref,
            key=s["key"],
            lane=s.get("lane", ""),
            exclude=None,
        )
        return sc

    ordered_slots = sorted(slots, key=_slot_seed_score, reverse=True)

    for s in ordered_slots:
        key = s["key"]
        provider = s["provider"]
        pool = list(cands.get(provider) or [])
        preferred = s.get("model") or ""
        exclude = used_by_provider.get(provider.lower(), set())
        model, sc = pick_best_model(
            provider,
            pool,
            preferred=preferred,
            key=key,
            lane=s.get("lane", ""),
            exclude=exclude,
        )
        if not model:
            model = preferred or (pool[0] if pool else "unknown")
        used_by_provider.setdefault(provider.lower(), set()).add(model.lower())
        e = ensure_entry(
            h, provider, model, key=key, lane=s.get("lane", "")
        )
        st = refresh_status(e)
        seed_note = f"seed={preferred}" if preferred and preferred != model else "seed=active"
        workers[key] = {
            "name": s["name"],
            "provider": provider,
            "model": model,
            "lane": s.get("lane") or "general",
            "notes": (
                f"live-rank score={sc} {seed_note}; "
                f"{s.get('notes') or 'health-ranked'}"
            ),
            "score": float(e.get("score") if e.get("score") is not None else sc),
            "status": st,
            "health_id": _mid(provider, model),
            "seed_model": preferred,
            "live": ROSTER_LIVE,
        }

    # order keys: active first by score
    ordered = sorted(
        workers.items(),
        key=lambda kv: (
            0 if kv[1].get("status") in ("active", "untested", "degraded") else 1,
            -float(kv[1].get("score") or 0),
            kv[0],
        ),
    )
    workers = {k: v for k, v in ordered}

    save_health(h)

    if persist:
        # top candidates snapshot per provider (for transparency)
        pool_snapshot: dict[str, list[dict[str, Any]]] = {}
        for prov, models in cands.items():
            rows = []
            for m in models:
                e = ensure_entry(h, prov, m)
                refresh_status(e)
                rows.append(
                    {
                        "model": m,
                        "score": e.get("score"),
                        "status": e.get("status"),
                        "last_probe": e.get("last_probe"),
                        "last_ok": e.get("last_ok"),
                        "success": e.get("success"),
                        "fail": e.get("fail"),
                    }
                )
            rows.sort(key=lambda r: -(r.get("score") or -999))
            pool_snapshot[prov] = rows
        doc = {
            "version": 2,
            "updated_at": _now_iso(),
            "source": "ik_model_roster.live_health_rank",
            "policy": {
                "live": ROSTER_LIVE,
                "switch_margin": SWITCH_MARGIN,
                "preferred_bonus": PREFERRED_BONUS,
                "stale_hours": STALE_HOURS,
            },
            "default_mode": _load_json(WHITE_LIST, {}).get(
                "default_mode", "full-4"
            ),
            "workers": workers,
            "mesh_keys": list(workers.keys()),
            "failover_order": [
                k
                for k, v in ordered
                if v.get("status") not in ("banned",)
            ],
            "candidate_pools": pool_snapshot,
            "notes": (
                "LIVE roster: models.json is the full candidate pool; "
                "white-list model is seed prior only; "
                "health-worker probes all candidates in background; "
                "black-list always excluded."
            ),
        }
        _save_json(ACTIVE_ROSTER, doc)

    return workers


def load_workers(*, refresh: bool = False) -> dict[str, dict[str, Any]]:
    """Workers for bridge. refresh=True rebuilds from health."""
    if refresh or not ACTIVE_ROSTER.is_file():
        return rebuild_roster(persist=True)
    doc = _load_json(ACTIVE_ROSTER, {})
    workers = doc.get("workers") or {}
    if not workers:
        return rebuild_roster(persist=True)
    # normalize strings
    out: dict[str, dict[str, Any]] = {}
    for k, v in workers.items():
        if not isinstance(v, dict):
            continue
        out[str(k)] = {
            "name": str(v.get("name") or f"pi-{k}"),
            "provider": str(v.get("provider") or "or-free"),
            "model": str(v.get("model") or ""),
            "lane": str(v.get("lane") or "general"),
            "notes": str(v.get("notes") or ""),
            "score": float(v.get("score") or 0),
            "status": str(v.get("status") or "unknown"),
        }
    return out


def failover_order(primary: str | None = None) -> list[str]:
    workers = load_workers()
    keys = [
        k
        for k, v in sorted(
            workers.items(),
            key=lambda kv: -float(kv[1].get("score") or 0),
        )
        if v.get("status") not in ("banned", "cooldown")
    ]
    # include cooldown at end as last resort
    for k, v in workers.items():
        if k not in keys:
            keys.append(k)
    if primary and primary in keys:
        keys = [primary] + [k for k in keys if k != primary]
    elif primary and primary in workers:
        keys = [primary] + keys
    return keys


def health_summary() -> dict[str, Any]:
    h = load_health()
    rows = []
    for mid, e in h.get("entries", {}).items():
        refresh_status(e)
        rows.append(
            {
                "id": mid,
                "provider": e.get("provider"),
                "model": e.get("model"),
                "key": e.get("key"),
                "score": e.get("score"),
                "status": e.get("status"),
                "success": e.get("success"),
                "fail": e.get("fail"),
                "rate_limited": e.get("rate_limited"),
                "avg_quality": (
                    round(
                        float(e.get("quality_sum", 0))
                        / max(1, int(e.get("quality_n", 0))),
                        1,
                    )
                    if int(e.get("quality_n", 0))
                    else None
                ),
                "last_ok": e.get("last_ok"),
                "fail_streak": e.get("fail_streak"),
            }
        )
    rows.sort(key=lambda r: -(r.get("score") or -999))
    workers = load_workers()
    return {
        "updated_at": h.get("updated_at"),
        "health_path": str(HEALTH_PATH),
        "roster_path": str(ACTIVE_ROSTER),
        "entries": rows,
        "active_roster": {
            k: {
                "model": v.get("model"),
                "provider": v.get("provider"),
                "score": v.get("score"),
                "status": v.get("status"),
            }
            for k, v in workers.items()
        },
        "history_tail": (h.get("history") or [])[-10:],
    }


def load_provider_api(provider: str) -> dict[str, str] | None:
    """baseUrl + apiKey from models.json for OpenAI-compatible providers."""
    data = _load_json(MODELS_JSON, {})
    meta = (data.get("providers") or {}).get(provider) or {}
    base = str(meta.get("baseUrl") or "").rstrip("/")
    key = str(meta.get("apiKey") or "")
    if not base or not key:
        return None
    return {"baseUrl": base, "apiKey": key, "api": str(meta.get("api") or "")}


def api_ping(
    provider: str,
    model: str,
    *,
    timeout_sec: int = 30,
) -> dict[str, Any]:
    """
    Fast OpenAI-compatible health ping (no pi, no tools).
    Marks model live if HTTP 200 and non-empty content or reasoning.
    """
    cfg = load_provider_api(provider)
    if not cfg:
        return {
            "ok": False,
            "provider": provider,
            "model": model,
            "error": "no_api_config",
            "method": "api",
        }
    body = json.dumps(
        {
            "model": model,
            "messages": [
                {
                    "role": "user",
                    "content": "Reply with exactly: PONG",
                }
            ],
            "max_tokens": 64,
            "temperature": 0,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"{cfg['baseUrl']}/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {cfg['apiKey']}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://local.ik-health",
            "X-Title": "ik-model-health",
        },
        method="POST",
    )
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            code = getattr(resp, "status", 200) or 200
        latency = (time.time() - t0) * 1000.0
        j = json.loads(raw)
        ch = (j.get("choices") or [{}])[0]
        msg = ch.get("message") or {}
        content = str(msg.get("content") or "").strip()
        reasoning = str(
            msg.get("reasoning_content") or msg.get("reasoning") or ""
        ).strip()
        text_blob = (content + " " + reasoning).strip()
        rate = False
        ok = bool(text_blob) and code < 400
        quality = 0.0
        if ok:
            quality = 85.0 if re.search(r"\bPONG\b", content, re.I) else 72.0
        rec = record_outcome(
            provider,
            model,
            ok=ok,
            quality=quality,
            rate_limited=rate,
            latency_ms=latency,
            error=None if ok else f"empty_response code={code}",
        )
        h = load_health()
        e = ensure_entry(h, provider, model)
        e["probe_method"] = "api"
        save_health(h)
        return {
            "ok": ok,
            "provider": provider,
            "model": model,
            "method": "api",
            "http": code,
            "latency_ms": round(latency, 1),
            "score": rec.get("score"),
            "status": rec.get("entry", {}).get("status"),
            "snippet": (content or reasoning)[:80],
        }
    except urllib.error.HTTPError as ex:
        latency = (time.time() - t0) * 1000.0
        body_err = ""
        try:
            body_err = ex.read().decode("utf-8", errors="replace")[:300]
        except Exception:
            pass
        rate = ex.code == 429 or bool(
            re.search(r"rate.?limit|quota|capacity", body_err, re.I)
        )
        rec = record_outcome(
            provider,
            model,
            ok=False,
            quality=0,
            rate_limited=rate,
            latency_ms=latency,
            error=f"HTTP {ex.code}: {body_err[:160]}",
        )
        return {
            "ok": False,
            "provider": provider,
            "model": model,
            "method": "api",
            "http": ex.code,
            "rate_limited": rate,
            "latency_ms": round(latency, 1),
            "error": f"HTTP {ex.code}",
            "score": rec.get("score"),
        }
    except Exception as ex:
        latency = (time.time() - t0) * 1000.0
        rec = record_outcome(
            provider,
            model,
            ok=False,
            quality=0,
            latency_ms=latency,
            error=str(ex)[:200],
        )
        return {
            "ok": False,
            "provider": provider,
            "model": model,
            "method": "api",
            "error": str(ex)[:200],
            "latency_ms": round(latency, 1),
            "score": rec.get("score"),
        }


def probe_ping(
    provider: str,
    model: str,
    *,
    pi_cli: str,
    timeout_sec: int = 45,
) -> dict[str, Any]:
    """Minimal RESULT/VERIFY ping via node pi cli (tools none)."""
    brief = (
        "Reply with exactly two lines:\n"
        "RESULT: probe-ok\n"
        "VERIFY: health-ping"
    )
    args = [
        "node",
        pi_cli,
        "-ne",
        "-p",
        brief,
        "--no-session",
        "--provider",
        provider,
        "--model",
        model,
        "--tools",
        "none",
    ]
    t0 = time.time()
    try:
        p = subprocess.run(
            args,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_sec,
        )
        text = (p.stdout or "") + "\n" + (p.stderr or "")
        rate = bool(
            re.search(
                r"\b429\b|rate.?limit|quota|capacity",
                text,
                re.I,
            )
        )
        has_r = bool(re.search(r"RESULT:\s*\S", text, re.I))
        has_v = bool(re.search(r"VERIFY:\s*\S", text, re.I))
        ok = p.returncode == 0 and has_r and has_v and not rate
        quality = 70.0 if ok else 0.0
        if ok and "probe-ok" in text.lower():
            quality = 85.0
        latency = (time.time() - t0) * 1000.0
        rec = record_outcome(
            provider,
            model,
            ok=ok,
            quality=quality,
            rate_limited=rate,
            latency_ms=latency,
            error=None if ok else text[-300:],
        )
        h = load_health()
        e = ensure_entry(h, provider, model)
        e["probe_method"] = "pi"
        save_health(h)
        return {
            "ok": ok,
            "provider": provider,
            "model": model,
            "method": "pi",
            "rc": p.returncode,
            "rate_limited": rate,
            "latency_ms": round(latency, 1),
            "score": rec.get("score"),
            "status": rec.get("entry", {}).get("status"),
        }
    except subprocess.TimeoutExpired:
        rec = record_outcome(
            provider,
            model,
            ok=False,
            quality=0,
            error=f"timeout {timeout_sec}s",
        )
        return {
            "ok": False,
            "provider": provider,
            "model": model,
            "method": "pi",
            "error": "timeout",
            "score": rec.get("score"),
        }
    except Exception as ex:
        rec = record_outcome(
            provider, model, ok=False, quality=0, error=str(ex)[:200]
        )
        return {
            "ok": False,
            "provider": provider,
            "model": model,
            "method": "pi",
            "error": str(ex)[:200],
            "score": rec.get("score"),
        }


def list_all_candidates() -> list[tuple[str, str]]:
    """Flat list of (provider, model) from live pool."""
    out: list[tuple[str, str]] = []
    for prov, models in load_candidates().items():
        for m in models:
            out.append((prov, m))
    return out


def _probe_priority(provider: str, model: str, h: dict[str, Any]) -> tuple:
    """Lower tuple = probe sooner (stale / untested first)."""
    mid = _mid(provider, model)
    e = (h.get("entries") or {}).get(mid) or {}
    age = _iso_age_hours(e.get("last_probe"))
    if age is None:
        age_key = 1e9  # never probed
    else:
        age_key = age
    total = int(e.get("success", 0)) + int(e.get("fail", 0))
    untested = 0 if total == 0 else 1
    # active roster models also get priority when stale
    return (-age_key if age_key == 1e9 else -age_key, untested, mid)


def probe_one(
    provider: str,
    model: str,
    *,
    pi_cli: str = "",
    timeout_sec: int = 30,
    method: str = "auto",
) -> dict[str, Any]:
    """Probe single model. method: auto|api|pi."""
    method = (method or "auto").lower()
    if method == "api":
        return api_ping(provider, model, timeout_sec=timeout_sec)
    if method == "pi":
        cli = pi_cli or os.environ.get("PI_CLI", DEFAULT_PI_CLI)
        return probe_ping(
            provider, model, pi_cli=cli, timeout_sec=timeout_sec
        )
    # auto: API first, fall back to pi if no config / soft fail on empty
    r = api_ping(provider, model, timeout_sec=timeout_sec)
    if r.get("ok"):
        return r
    if r.get("error") == "no_api_config" or r.get("http") in (404, 400):
        cli = pi_cli or os.environ.get("PI_CLI", DEFAULT_PI_CLI)
        if Path(cli).is_file():
            r2 = probe_ping(
                provider, model, pi_cli=cli, timeout_sec=timeout_sec
            )
            r2["fallback_from"] = r.get("error") or r.get("http")
            return r2
    return r


def probe_cycle(
    *,
    batch: int | None = None,
    timeout_sec: int = 30,
    method: str = "auto",
    pi_cli: str = "",
    all_models: bool = False,
    provider: str | None = None,
) -> dict[str, Any]:
    """
    Probe a batch of candidates (stale-first). Rebuild roster after.
    all_models=True → probe entire pool (can take minutes).
    """
    batch_n = len(list_all_candidates()) if all_models else (
        batch if batch is not None else DEFAULT_PROBE_BATCH
    )
    batch_n = max(1, int(batch_n))
    h = load_health()
    targets = list_all_candidates()
    if provider:
        targets = [(p, m) for p, m in targets if p == provider]

    # active roster always considered "important" when stale
    workers = load_workers(refresh=False)
    active_set = {
        _mid(w["provider"], w["model"]) for w in workers.values()
    }

    def sort_key(pm: tuple[str, str]) -> tuple:
        p, m = pm
        mid = _mid(p, m)
        e = (h.get("entries") or {}).get(mid) or {}
        age = _iso_age_hours(e.get("last_probe"))
        never = age is None
        stale = never or (age is not None and age >= STALE_HOURS)
        total = int(e.get("success", 0)) + int(e.get("fail", 0))
        # priority: never > stale > active-roster > rest; then oldest first
        tier = 0 if never else (1 if stale else (2 if mid in active_set else 3))
        age_sort = -(age if age is not None else 1e9)
        return (tier, age_sort, total, mid)

    targets_sorted = sorted(targets, key=sort_key)

    # round-robin cursor so we eventually cover all even if batch small
    cursor = _load_json(HEALTH_CURSOR_PATH, {"i": 0})
    start_i = int(cursor.get("i") or 0) % max(1, len(targets_sorted))
    rotated = targets_sorted[start_i:] + targets_sorted[:start_i]
    # but still prefer never/stale at front of this cycle
    rotated = sorted(rotated, key=sort_key)[:batch_n]

    results: list[dict[str, Any]] = []
    for p, m in rotated:
        r = probe_one(
            p,
            m,
            pi_cli=pi_cli,
            timeout_sec=timeout_sec,
            method=method,
        )
        r["key"] = f"cand:{p}"
        results.append(r)
        # brief stagger to reduce rate-limit bursts
        time.sleep(0.35)

    new_i = (start_i + len(rotated)) % max(1, len(targets_sorted))
    _save_json(
        HEALTH_CURSOR_PATH,
        {"i": new_i, "updated_at": _now_iso(), "last_batch": len(rotated)},
    )

    roster = rebuild_roster(persist=True)
    ok_n = sum(1 for r in results if r.get("ok"))
    return {
        "ok": True,
        "probed": len(results),
        "ok_n": ok_n,
        "fail_n": len(results) - ok_n,
        "pool_size": len(targets),
        "batch": batch_n,
        "method": method,
        "live": ROSTER_LIVE,
        "results": results,
        "active_roster": {
            k: {
                "model": v["model"],
                "score": v.get("score"),
                "status": v.get("status"),
                "seed_model": v.get("seed_model"),
            }
            for k, v in roster.items()
        },
        "cursor": new_i,
    }


def probe_active(
    *, pi_cli: str, timeout_sec: int = 45, limit: int = 8
) -> dict[str, Any]:
    """Backward-compatible: probe batch (active + stale candidates)."""
    return probe_cycle(
        batch=limit,
        timeout_sec=timeout_sec,
        method="auto",
        pi_cli=pi_cli,
        all_models=False,
    )


def health_worker_status() -> dict[str, Any]:
    if not HEALTH_WORKER_PID.is_file():
        return {
            "ok": True,
            "running": False,
            "pid_path": str(HEALTH_WORKER_PID),
            "log": str(HEALTH_WORKER_LOG),
        }
    try:
        pid = int(HEALTH_WORKER_PID.read_text(encoding="utf-8").strip())
    except Exception:
        return {
            "ok": False,
            "running": False,
            "error": "bad pid file",
            "pid_path": str(HEALTH_WORKER_PID),
        }
    running = False
    if os.name == "nt":
        try:
            out = subprocess.run(
                ["tasklist", "/FI", f"PID eq {pid}"],
                capture_output=True,
                text=True,
                timeout=10,
            )
            running = str(pid) in (out.stdout or "")
        except Exception:
            running = False
    else:
        try:
            os.kill(pid, 0)
            running = True
        except OSError:
            running = False
    if not running:
        try:
            HEALTH_WORKER_PID.unlink(missing_ok=True)
        except Exception:
            pass
    return {
        "ok": True,
        "running": running,
        "pid": pid if running else None,
        "pid_path": str(HEALTH_WORKER_PID),
        "log": str(HEALTH_WORKER_LOG),
    }


def health_worker_stop() -> dict[str, Any]:
    st = health_worker_status()
    if not st.get("running"):
        return {"ok": True, "stopped": False, "reason": "not_running", **st}
    pid = st.get("pid")
    try:
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/F"],
                capture_output=True,
                text=True,
                timeout=15,
            )
        else:
            os.kill(int(pid), 15)
    except Exception as ex:
        return {"ok": False, "error": str(ex), **st}
    try:
        HEALTH_WORKER_PID.unlink(missing_ok=True)
    except Exception:
        pass
    return {"ok": True, "stopped": True, "pid": pid}


def health_worker_loop(
    *,
    interval: float | None = None,
    batch: int | None = None,
    timeout_sec: int = 30,
    method: str = "auto",
    max_cycles: int = 0,
    once: bool = False,
) -> dict[str, Any]:
    """Background loop: probe candidate batches forever (or once)."""
    interval = (
        DEFAULT_WATCH_INTERVAL if interval is None else float(interval)
    )
    batch = DEFAULT_PROBE_BATCH if batch is None else int(batch)
    cycles = 0
    reports: list[dict[str, Any]] = []
    try:
        HEALTH_WORKER_PID.parent.mkdir(parents=True, exist_ok=True)
        HEALTH_WORKER_PID.write_text(str(os.getpid()), encoding="utf-8")
    except Exception:
        pass
    try:
        while True:
            cycles += 1
            rep = probe_cycle(
                batch=batch,
                timeout_sec=timeout_sec,
                method=method,
            )
            summary = {
                "cycle": cycles,
                "probed": rep.get("probed"),
                "ok_n": rep.get("ok_n"),
                "fail_n": rep.get("fail_n"),
                "cursor": rep.get("cursor"),
                "roster": rep.get("active_roster"),
                "ts": _now_iso(),
            }
            reports.append(summary)
            print(json.dumps(summary, ensure_ascii=False), flush=True)
            if once:
                break
            if max_cycles and cycles >= max_cycles:
                break
            time.sleep(max(30.0, float(interval)))
    except KeyboardInterrupt:
        reports.append({"stopped": "KeyboardInterrupt", "cycle": cycles})
    finally:
        try:
            if (
                HEALTH_WORKER_PID.is_file()
                and HEALTH_WORKER_PID.read_text(encoding="utf-8").strip()
                == str(os.getpid())
            ):
                HEALTH_WORKER_PID.unlink(missing_ok=True)
        except Exception:
            pass
    return {
        "ok": True,
        "cycles": cycles,
        "reports": reports[-20:],
        "running": False,
    }


def health_worker_start_bg(
    *,
    interval: float | None = None,
    batch: int | None = None,
    timeout_sec: int = 30,
    method: str = "auto",
) -> dict[str, Any]:
    st = health_worker_status()
    if st.get("running"):
        return {
            "ok": False,
            "error": "health worker already running",
            "status": st,
            "hint": "python ik_model_roster.py watch stop",
        }
    interval = (
        DEFAULT_WATCH_INTERVAL if interval is None else float(interval)
    )
    batch = DEFAULT_PROBE_BATCH if batch is None else int(batch)
    script = Path(__file__).resolve()
    HEALTH_WORKER_LOG.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(script),
        "watch",
        "start",
        "--interval",
        str(interval),
        "--batch",
        str(batch),
        "--timeout",
        str(timeout_sec),
        "--method",
        method,
    ]
    creationflags = 0
    if os.name == "nt":
        creationflags = 0x00000008 | 0x00000200 | 0x08000000
    with HEALTH_WORKER_LOG.open("a", encoding="utf-8") as lf:
        lf.write(
            f"\n--- bg start {_now_iso()} cmd={' '.join(cmd)} ---\n"
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

    deadline = time.time() + 8.0
    st2 = {"running": False}
    while time.time() < deadline:
        time.sleep(0.4)
        st2 = health_worker_status()
        if st2.get("running"):
            break
    return {
        "ok": bool(st2.get("running")),
        "bg": True,
        "spawn_pid": proc.pid,
        "log": str(HEALTH_WORKER_LOG),
        "cmd": cmd,
        "status": st2,
    }


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="IK LIVE dynamic model roster + health worker"
    )
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("show")
    sub.add_parser("refresh")
    sub.add_parser("health")
    sub.add_parser("pool", help="Show full candidate pools with scores")
    pb = sub.add_parser(
        "probe",
        help="Probe candidates (stale-first batch; --all = full pool)",
    )
    pb.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_PROBE_BATCH,
        help="Batch size (default from IK_PROBE_BATCH)",
    )
    pb.add_argument("--timeout", type=int, default=30)
    pb.add_argument(
        "--method",
        choices=["auto", "api", "pi"],
        default="auto",
        help="api=direct HTTP, pi=node cli, auto=api then pi",
    )
    pb.add_argument(
        "--all",
        action="store_true",
        help="Probe entire candidate pool (slow)",
    )
    pb.add_argument(
        "--provider",
        default=None,
        help="Limit to one provider (nvidia|or-free|ollama-cloud)",
    )
    w = sub.add_parser(
        "watch",
        help="Background health worker (probe all models over time)",
    )
    w.add_argument(
        "action",
        nargs="?",
        default="status",
        choices=["start", "stop", "status", "once"],
    )
    w.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_WATCH_INTERVAL,
        help="Seconds between probe cycles (default 300)",
    )
    w.add_argument(
        "--batch",
        type=int,
        default=DEFAULT_PROBE_BATCH,
        help="Models per cycle",
    )
    w.add_argument("--timeout", type=int, default=30)
    w.add_argument(
        "--method", choices=["auto", "api", "pi"], default="auto"
    )
    w.add_argument(
        "--bg",
        action="store_true",
        help="Detached background service",
    )
    w.add_argument("--max-cycles", type=int, default=0)
    args = ap.parse_args()
    pi_cli = os.environ.get("PI_CLI", DEFAULT_PI_CLI)

    if args.cmd == "show":
        print(
            json.dumps(
                load_workers(refresh=False), indent=2, ensure_ascii=False
            )
        )
    elif args.cmd == "refresh":
        print(
            json.dumps(
                rebuild_roster(persist=True), indent=2, ensure_ascii=False
            )
        )
    elif args.cmd == "health":
        print(
            json.dumps(health_summary(), indent=2, ensure_ascii=False)
        )
    elif args.cmd == "pool":
        h = load_health()
        cands = load_candidates()
        out = {}
        for prov, models in cands.items():
            rows = []
            for m in models:
                e = ensure_entry(h, prov, m)
                refresh_status(e)
                rows.append(
                    {
                        "model": m,
                        "score": e.get("score"),
                        "status": e.get("status"),
                        "success": e.get("success"),
                        "fail": e.get("fail"),
                        "last_probe": e.get("last_probe"),
                        "last_ok": e.get("last_ok"),
                        "avg_latency_ms": (
                            round(
                                float(e.get("latency_sum_ms", 0))
                                / max(1, int(e.get("latency_n", 0))),
                                1,
                            )
                            if int(e.get("latency_n", 0))
                            else None
                        ),
                    }
                )
            rows.sort(key=lambda r: -(r.get("score") or -999))
            out[prov] = rows
        print(
            json.dumps(
                {
                    "live": ROSTER_LIVE,
                    "switch_margin": SWITCH_MARGIN,
                    "preferred_bonus": PREFERRED_BONUS,
                    "stale_hours": STALE_HOURS,
                    "pools": out,
                },
                indent=2,
                ensure_ascii=False,
            )
        )
    elif args.cmd == "probe":
        print(
            json.dumps(
                probe_cycle(
                    batch=args.limit,
                    timeout_sec=args.timeout,
                    method=args.method,
                    pi_cli=pi_cli,
                    all_models=bool(args.all),
                    provider=args.provider,
                ),
                indent=2,
                ensure_ascii=False,
            )
        )
    elif args.cmd == "watch":
        act = args.action or "status"
        if act == "status":
            print(
                json.dumps(
                    health_worker_status(), indent=2, ensure_ascii=False
                )
            )
        elif act == "stop":
            print(
                json.dumps(
                    health_worker_stop(), indent=2, ensure_ascii=False
                )
            )
        elif act == "once":
            print(
                json.dumps(
                    health_worker_loop(
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
            if args.bg:
                print(
                    json.dumps(
                        health_worker_start_bg(
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
                # foreground forever (or max-cycles)
                print(
                    json.dumps(
                        health_worker_loop(
                            interval=args.interval,
                            batch=args.batch,
                            timeout_sec=args.timeout,
                            method=args.method,
                            max_cycles=args.max_cycles,
                            once=False,
                        ),
                        indent=2,
                        ensure_ascii=False,
                    )
                )
