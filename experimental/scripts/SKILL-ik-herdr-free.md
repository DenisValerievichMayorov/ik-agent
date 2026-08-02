---
name: ik-herdr-free
description: >
  IK free-herd bridge v1.8: LIVE model roster, health-worker probes all
  candidates, mesh quality, rate-limit, interrupt, queue, Author≠Reviewer,
  bg workers, metrics JSONL.
  Triggers: ik free, free-herd, mesh, queue, interrupt, review, metrics,
  health-worker, live roster.
---

# IK Free-Herd (v1.8 LIVE roster)

## Commands

```powershell
python C:\Users\anton\agent_tools\ik_herdr_free.py status|ensure|doctor|interrupt|roster

# LIVE model roster (white-list model = seed only, not freeze)
python ...\ik_herdr_free.py roster show|refresh|pool
python ...\ik_herdr_free.py probe --limit 6 --method auto
python ...\ik_herdr_free.py probe --all --provider nvidia
python ...\ik_herdr_free.py health-worker start --bg --interval 300 --batch 6
python ...\ik_herdr_free.py health-worker status|stop|once
# equivalent: python C:\Users\anton\agent_tools\ik_model_roster.py watch start --bg

# Delegate
python ...\ik_herdr_free.py delegate --to mesh|auto --goal "..."
python ...\ik_herdr_free.py delegate --to auto --review --retries 1 --goal "..."

# Queue + background service
python ...\ik_herdr_free.py queue add --goal "..." --to auto --max-attempts 3
python ...\ik_herdr_free.py queue run --limit 1
python ...\ik_herdr_free.py queue worker start --bg --interval 20
python ...\ik_herdr_free.py queue worker status|stop

# One text field (preferred for user)
python ...\ik_herdr_free.py ui
# or Desktop shortcut «IK-Задание» → http://127.0.0.1:8765

# Day runner — leave for hours: parallel jobs + failover + observe
python ...\ik_herdr_free.py day start --parallel 2 --interval 20
python ...\ik_herdr_free.py day observe
python ...\ik_herdr_free.py day stop

# Metrics
python ...\ik_herdr_free.py metrics show --last 30
python ...\ik_herdr_free.py metrics summary --last 100
```

## Data files (`Sync/Data/`)

| File | Purpose |
|------|---------|
| `ik_free_queue.jsonl` | job queue |
| `ik_free_queue_worker.pid` | simple bg worker PID |
| `ik_free_queue_worker.log` | simple bg worker log |
| `ik_free_metrics.jsonl` | events |
| `ik_model_health.json` | live model scores |
| `ik_model_health_worker.pid` | health watch PID |
| `ik_model_health_worker.log` | health watch log |
| `ik_model_health_cursor.json` | round-robin probe cursor |
| `ik_day_runner.pid` | day runner PID |
| `ik_day_runner.log` | day runner log |
| `ik_day_status.json` / `.md` | board when you return |

Env: `IK_ROSTER_LIVE=1`, `IK_SWITCH_MARGIN=0`, `IK_PREFERRED_BONUS=3`, `IK_PROBE_STALE_HOURS=6`, `IK_PROBE_BATCH=6`, `IK_HEALTH_WATCH_INTERVAL=300`.

## Metrics fields (compact)

`ts, event, ok, mode, strategy, winner_key, quality, latency_ms, workers_ok, failover_used, retries_used, to, mesh, worker, model, job_id, cycle…`

## Stability (cumulative)

1. Full oneshot brief; RESULT+VERIFY  
2. Mesh winner by quality; thin skip  
3. Semaphore + stagger + 429 retry  
4. Interrupt C-c (no Escape spam)  
5. Review PASS strict + retry-on-FAIL  
6. Queue stale recovery  
7. BG queue worker + metrics  
8. **LIVE roster** + **health-worker** probes all `models.json` candidates  

## Loop

```text
status → ensure → interrupt if stuck → doctor
→ health-worker --bg (keep models live)
→ queue worker --bg | delegate
→ metrics summary → mesh-report
```
