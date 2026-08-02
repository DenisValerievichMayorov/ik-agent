# experimental/

Scaffold for **IK harness experiments**. Nothing here is loaded by the binary
automatically. Use this tree to design features before (or while) you patch
Rust crates.

## Layout

```text
experimental/
  README.md           # this file
  PATCHES.md          # log of intentional code patches vs upstream
  ideas/              # design notes before coding
  tools/              # tool specs (name, schema, DoD) before impl
```

## Workflow (recommended)

1. Write a short note under `ideas/` (problem, DoD, VERIFY).
2. If it needs a new tool: draft JSON/schema + behaviour in `tools/`.
3. Implement in the matching crate (see `FORK.md` table).
4. Record the patch in `PATCHES.md` (files + intent).
5. Build: `cargo build -p xai-grok-pager-bin --release`.
6. Smoke: launch the binary, run one task that hits the new path.

## First experiment ideas (backlog)

- Custom default agent profile baked into the binary (beyond `~/.grok/agents/`).
- Mesh report tool (DONE/VERIFY block → file or Herdr).
- Extra slash command for “system maintenance” entrypoints.
- Harder always-approve / sandbox profiles for headless workers.

Add new ideas as markdown files; do not bloat the Rust tree with unused stubs.
