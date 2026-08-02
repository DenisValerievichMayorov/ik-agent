# 001 — Custom tools + mesh report surface

## Problem

Stock Grok Build is excellent as a coding harness, but personal mesh workflows
need stable “functions” (tools) that always emit VERIFY-style reports and can
talk to local automation (Herdr, wiki raw logs) without re-prompting every time.

## Direction

1. Prefer **config-layer** first when possible (`~/.grok/agents`, skills, hooks).
2. Use this fork when the change must live in the **tool registry / runtime**
   (new tool kinds, fixed system mandate, binary defaults).

## Candidate tools (draft)

| Name | Purpose | VERIFY |
|------|---------|--------|
| `mesh_report` | Write structured DONE/VERIFY/RISK/NEXT to a path | file exists + schema |
| `wiki_raw_append` | Append operational note under wiki raw | path + lint optional |
| `system_snapshot` | Collect service/process facts for maintenance | command RC |

## Out of scope for v0

- Local Ollama routing
- Replacing xAI cloud models
- Full Herdr protocol inside the binary (start with files + scripts)

## DoD for first real patch

- [ ] One new tool registered and callable in a headless session
- [ ] Documented in `experimental/tools/`
- [ ] Entry in `PATCHES.md`
- [ ] `cargo check -p xai-grok-tools` (or relevant crate) succeeds
