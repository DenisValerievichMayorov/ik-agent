# Patches vs upstream

Baseline: `upstream/main` @ `a5727c5` (“Synced from monorepo”), `SOURCE_REV`
`30192d2eef5d91a8fff0e53957de5bd05b43398c`.

| Date | Patch | Files | Status |
|------|--------|-------|--------|
| 2026-08-02 | Fork bootstrap (docs + experimental scaffold only) | `FORK.md`, `experimental/**`, `README.md` | active |

No runtime/code patches in the initial public fork. Former Ollama tool-call
fallbacks were intentionally **not** carried over.
