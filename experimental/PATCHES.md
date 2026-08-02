# Patches vs upstream

Baseline: `upstream/main` @ `a5727c5` (“Synced from monorepo”), `SOURCE_REV`
`30192d2eef5d91a8fff0e53957de5bd05b43398c`.

| Date | Patch | Files | Status |
|------|--------|-------|--------|
| 2026-08-02 | Fork bootstrap (docs + experimental scaffold only) | `FORK.md`, `experimental/**`, `README.md` | active |
| 2026-08-02 | Windows protoc: avoid `/dev/stdout` + `/dev/null` in dep scan | `crates/build/xai-proto-build/src/lib.rs` | active |
| 2026-08-02 | Windows link: final binary needs `/DEBUG:NONE` (LNK4319 PDB limit) | build flags only (see FORK.md) | active |
| 2026-08-02 | Free-herd orchestration (scripts, not binary tools yet) | `agent_tools/ik_herdr_free.py`, `~/.grok/agents/ik.md`, skill `ik-herdr-free` | active |

Former Ollama tool-call fallbacks were intentionally **not** carried over.
Free agents in Herdr: see `experimental/ideas/002-free-agents-herdr.md`.
