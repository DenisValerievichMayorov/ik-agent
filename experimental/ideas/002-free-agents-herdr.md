# 002 — Free agents inside Herdr, orchestrated by IK

## Status (2026-08-02): **v1.8 LIVE roster + bg/metrics**

Bridge: `agent_tools/ik_herdr_free.py` (+ `ik_model_roster.py`, `ik_day_runner.py`).
Synced copy: `Sync/Tools/agent_tools/`.

| Piece | Path |
|-------|------|
| Master binary | `~/.ik/bin/ik.exe` (this fork) |
| Agent profile | `~/.grok/agents/ik.md` / `Sync/Configs/grok-agents/ik.md` |
| Bridge | `agent_tools/ik_herdr_free.py` |
| Live roster | `agent_tools/ik_model_roster.py` + `Configs/pi/active-roster.json` |
| Day runner | `agent_tools/ik_day_runner.py` |
| Skill | `Sync/Skills/ik-herdr-free` |
| White-list | `Sync/Configs/pi/white-list.json` |

## v1.3–v1.8 features

- quality mesh winner, full oneshot brief, VERIFY required
- rate-limit semaphore/stagger/429 retry
- interactive mesh + interrupt C-c
- queue + Author≠Reviewer + retry-on-FAIL
- bg queue worker + metrics JSONL
- LIVE health-ranked models (not sticky preferred)
- day runner for multi-hour parallel work

## Flow

1. User runs `ik` → agent profile `ik`.
2. IK runs `ik_herdr_free.py status|ensure|delegate|day|roster`.
3. Workers: white-list slots; models from health rank over models.json pool.
4. Default oneshot (`pi -p`) for reliable tools.

## v2 (later)

Native tools in fork: `herdr_status`, `herdr_delegate` in `xai-grok-tools`.
