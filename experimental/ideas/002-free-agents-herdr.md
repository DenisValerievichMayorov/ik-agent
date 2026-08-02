# 002 — Free agents inside Herdr, orchestrated by IK

## Status (2026-08-02): **v1.1 verified (scripts + agent profile)**

`doctor` + oneshot + interactive PASS. Bridge: `agent_tools/ik_herdr_free.py`.

Not in binary tools yet (Windows rebuild cost). Orchestration via:

| Piece | Path |
|-------|------|
| Master binary | `~/.ik/bin/ik.exe` |
| Agent profile | `~/.grok/agents/ik.md` |
| Bridge | `agent_tools/ik_herdr_free.py` |
| Skill | `Sync/skills/ik-herdr-free` |
| White-list | `Sync/Configs/pi/white-list.json` |

## Flow

1. User runs `ik` → loads agent `ik` (free-herd master).
2. IK runs `ik_herdr_free.py status|ensure|delegate`.
3. Free workers: ling-flash / nemotron free (or-free).
4. Default delegate mode: **oneshot** (`pi -p`) for reliable tools.
5. Interactive mode keeps work in Herdr panes.

## v2 (later)

Native tools in fork: `herdr_status`, `herdr_delegate` registered in `xai-grok-tools` so the model calls them without shelling out.
