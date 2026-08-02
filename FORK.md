# IK Agent — experimental fork of Grok Build

This repository is a **personal experimental fork** of
[xai-org/grok-build](https://github.com/xai-org/grok-build) (Apache-2.0).

Upstream is the official SpaceXAI coding-agent harness and TUI. We keep it as
`upstream` and use this fork to prototype custom agent behaviour, tools, and
mesh integrations without waiting on the official release channel.

## Goals

- Experiment with **harness-level** features (tools, prompts, session lifecycle,
  hooks wiring, custom agent defaults).
- Keep a clear split: **upstream-compatible core** vs **`experimental/`** notes
  and local patches.
- Stay buildable from the same Rust workspace as Grok Build.

## Non-goals

- Replacing the official `grok` binary for daily cloud work (keep stock
  installs under `~/.grok` for that).
- Upstream PR stream (xAI does not accept external contributions to
  `grok-build`; this fork is for local / personal use).
- Local LLM / Ollama focus (retired). Cloud models + custom harness only.

## Remotes

```text
upstream  https://github.com/xai-org/grok-build.git
origin    https://github.com/DenisValerievichMayorov/ik-agent.git
```

## Sync from upstream

```powershell
git fetch upstream
git merge upstream/main
# or: git rebase upstream/main
```

After a large sync, re-apply only the patches you still want under
`experimental/PATCHES.md`.

## Build (same as upstream)

Requirements: Rust (see `rust-toolchain.toml`), DotSlash, protoc.

```powershell
cargo build -p xai-grok-pager-bin --release
# artifact: target/release/xai-grok-pager  (install/rename as ik or grok-ik)
```

Windows builds are **best-effort** (upstream note). Prefer WSL/Linux for
reliable release builds if MSVC/gnu fails.

## Where to add experiments

| Area | Crate / path | Typical change |
|------|----------------|----------------|
| Tools (run terminal, edit, …) | `crates/codegen/xai-grok-tools` | New tool impl + registration |
| Agent runtime / sessions | `crates/codegen/xai-grok-shell` | Lifecycle, headless, ACP |
| Sampling / API client | `crates/codegen/xai-grok-sampler` | Request/response shaping |
| System / request assembly | `crates/codegen/xai-chat-state` | System mandates, message build |
| Config surface | `crates/codegen/xai-grok-config` | New config keys |
| TUI | `crates/codegen/xai-grok-pager` | UI, slash commands |
| Notes & patch log (this fork) | `experimental/` | Docs only; no runtime load |

Prefer **small, reviewable patches** + an entry in `experimental/PATCHES.md`.

## License

Upstream first-party code: **Apache License 2.0** (see `LICENSE`,
`THIRD-PARTY-NOTICES`). This fork retains that license. Add your own notices
when you introduce substantial original code.
