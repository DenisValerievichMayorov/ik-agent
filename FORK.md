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

Requirements: Rust (see `rust-toolchain.toml`), protoc (or DotSlash + `bin/protoc`).

### Linux / macOS

```sh
cargo build -p xai-grok-pager-bin --release
# artifact: target/release/xai-grok-pager
```

### Windows (MSVC) — verified 2026-08-02

Prereqs:

1. Visual Studio 2022/2026 with MSVC
2. **Windows 11 SDK** (need `kernel32.lib` under `Windows Kits\10\Lib\...`)
3. `protoc` on PATH or `PROTOC=...\protoc.exe` (e.g. protobuf 29.3 win64 zip)
4. Toolchain: `rustup toolchain install 1.92.0-x86_64-pc-windows-msvc`

```bat
call "C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat"
set PROTOC=C:\path\to\protoc.exe
cd Sync\projects\ik-agent
REM Final link: MSVC hits LNK4319 (PDB public-symbol limit) without this:
cargo +1.92.0-x86_64-pc-windows-msvc rustc -p xai-grok-pager-bin --release -- -C link-arg=/DEBUG:NONE
```

Install:

```powershell
Copy-Item target\release\xai-grok-pager.exe $env:USERPROFILE\.ik\bin\ik.exe -Force
Copy-Item target\release\xai-grok-pager.exe $env:USERPROFILE\.local\bin\ik.exe -Force
```

Smoke: `ik --version` → should report fork commit (e.g. `0.2.110 (810d1fd…)`), not stock xAI 0.2.118.

Windows builds remain **best-effort** upstream; this fork carries the protoc
Windows patch so codegen works.

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
