<p align="center">
  <img src="assets/logo.png" alt="Forager" width="128">
  <h1 align="center">Forager</h1>
  <p align="center">
    <a href="https://kimyoungjin06.github.io/forager-cli/"><img src="https://img.shields.io/badge/docs-forager-blue" alt="Documentation"></a>
    <a href="https://github.com/kimyoungjin06/forager-cli/actions/workflows/ci.yml"><img src="https://github.com/kimyoungjin06/forager-cli/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
    <a href="https://github.com/kimyoungjin06/forager-cli/releases"><img src="https://img.shields.io/github/v/release/kimyoungjin06/forager-cli" alt="GitHub release"></a>
    <a href="https://blog.rust-lang.org/2025/06/26/Rust-1.88.0/"><img src="https://img.shields.io/badge/MSRV-1.88-blue?logo=rust" alt="MSRV"></a>
    <a href="https://github.com/kimyoungjin06/forager-cli/stargazers"><img src="https://img.shields.io/github/stars/kimyoungjin06/forager-cli?style=social" alt="GitHub stars"></a>
    <a href="https://kimyoungjin06.github.io/forager-cli/credits.html"><img src="https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fraw.githubusercontent.com%2Fkimyoungjin06%2Fforager-cli%2Fcredit%2Fcredits.json&query=%24.contributors.length&label=contributors&color=blue&logo=github" alt="Contributors"></a>
  </p>
</p>

Local meta-harness for harness-backed AI agents. Built on tmux, written in Rust.

> Forager lets people entrust meaningful work to agents and return to evidence,
> choices, and continuity instead of mystery.

Forager does not try to replace Claude Code, Codex, OpenCode, Gemini CLI,
Mistral Vibe, OpenHands, or local LLM scripts. It supervises harness-backed
agents with local approvals, durable runtime evidence, recovery state,
reviewable handoffs, and deliberate knowledge promotion.

Run multiple harness-backed agents in parallel across different branches of your
codebase, each in its own isolated session. Send bounded work Offdesk, return to
evidence, and hand the next step to a fresh harness without reconstructing state
from chat scrollback. `aoe` remains available as a legacy compatibility alias
while the project moves to `forager`.

> If you find this project useful, please consider giving it a star on GitHub: it helps others discover the project!

![Forager TUI preview](docs/assets/tui.png)

## Why Forager Exists

AI coding agents are getting stronger, but long-running agent work is still hard
to trust after the fact. Forager keeps the useful autonomy while making the
operational boundary explicit:

- **Entrust** meaningful work to a chosen harness-backed agent.
- **Observe** what actually ran through local state, tmux, heartbeat, progress,
  logs, and result artifacts.
- **Return** to evidence, risks, next choices, and reusable handoff packages.
- **Promote** only reviewed lessons into durable project knowledge.

## What Forager Owns

Hosted harnesses own their agent loops, prompts, model calls, tools, and
interaction style. Forager owns the local control plane around them:

- **Harness-backed agent support** -- Claude Code, OpenCode, Mistral Vibe,
  Codex CLI, Gemini CLI, local scripts, and future hosted harness agents
- **TUI dashboard** -- create, monitor, attach, recover, and review sessions
- **Agent + terminal views** -- toggle between AI agents and paired shell terminals with `t`
- **Status detection** -- see which agents are running, waiting for input, idle, stopped, or failed
- **Git worktrees** -- run parallel agents on different branches of the same repo
- **Diff view** -- review git changes without leaving the TUI
- **Per-repo config** -- `.forager/config.toml` for project-specific settings and hooks, with `.aoe/config.toml` fallback
- **Profiles** -- separate workspaces for different projects or clients
- **Offdesk recovery** -- durable task queueing, approval retry, lifecycle recovery, and audit trails
- **Ondesk/Offdesk handoff** -- project initialization, prompt packages,
  launch dry runs, runtime approvals, closeout packets, and wiki review
  surfaces for longer autonomous work
- **Reviewed knowledge** -- adaptive wiki candidates become reusable guidance
  only after explicit review and promotion
- **CLI and TUI** -- full functionality from both interfaces

## The Operating Loop

```text
observe current state
  -> prepare bounded work
  -> request approval
  -> run under durable local supervision
  -> collect evidence
  -> close out and review
  -> return to a fresh Ondesk harness
  -> promote durable knowledge deliberately
```

Forager wraps [tmux](https://github.com/tmux/tmux/wiki). Each session is a tmux session, so agents keep running when you close the TUI. Reopen `forager` and everything is still there.

The key tmux shortcut to know: **`Ctrl+b d`** detaches from a session and returns to the TUI.

## Installation

**Prerequisites:** [tmux](https://github.com/tmux/tmux/wiki) (required)

```bash
# Quick install (Linux & macOS)
curl -fsSL \
  https://raw.githubusercontent.com/kimyoungjin06/forager-cli/main/scripts/install.sh \
  | bash

# Build from source
git clone https://github.com/kimyoungjin06/forager-cli
cd forager-cli && cargo build --release
```

The install script and release artifacts now use `forager` as the primary
command and keep `aoe` as a legacy alias during the transition.

## Quick Start

```bash
# Launch the TUI
forager

# Add a session from CLI
forager add /path/to/project

# Add a session on a new git branch
forager add . -w feat/my-feature -b

```

In the TUI: `n` to create a session, `Enter` to attach, `t` to toggle terminal view, `D` for diff view, `d` to delete, `?` for help.

## Documentation

- **[Project Direction](https://kimyoungjin06.github.io/forager-cli/docs/project-direction.html)** -- product direction and operating principles
- **[Hosted Harness Agents](https://kimyoungjin06.github.io/forager-cli/docs/hosted-harness-agents.html)** -- contract for supervising agents built by other harnesses
- **[Installation](https://kimyoungjin06.github.io/forager-cli/docs/installation.html)** -- prerequisites and install methods
- **[Quick Start](https://kimyoungjin06.github.io/forager-cli/docs/quick-start.html)** -- first steps and basic usage
- **[Workflow Guide](https://kimyoungjin06.github.io/forager-cli/docs/guides/workflow.html)** -- recommended setup with bare repos and worktrees
- **[Operation Cycle](https://kimyoungjin06.github.io/forager-cli/docs/guides/operation-cycle.html)** -- Ondesk to Offdesk to Ondesk lifecycle, approvals, evidence, and wiki boundaries
- **[Repo Config & Hooks](https://kimyoungjin06.github.io/forager-cli/docs/guides/repo-config.html)** -- per-project settings and automation
- **[Configuration Reference](https://kimyoungjin06.github.io/forager-cli/docs/guides/configuration.html)** -- all config options
- **[CLI Reference](https://kimyoungjin06.github.io/forager-cli/docs/cli/reference.html)** -- complete command documentation

## FAQ

### What happens when I close Forager?

Nothing. Sessions are tmux sessions running in the background. Open and close `forager` as often as you like. Sessions only get removed when you explicitly delete them.

### Which harness-backed agents are supported?

Claude Code, OpenCode, Mistral Vibe, Codex CLI, and Gemini CLI. Forager
auto-detects which are installed on your system.

## Troubleshooting

### Using Forager with mobile SSH clients (Termius, Blink, etc.)

Run `forager` inside a tmux session when connecting from mobile:

```bash
tmux new-session -s main
forager
```

Use `Ctrl+b L` to toggle back to Forager after attaching to an agent session.

### Claude Code is flickering

This is a known Claude Code issue, not a Forager problem: https://github.com/anthropics/claude-code/issues/1913

## Development

```bash
cargo check          # Type-check
cargo test           # Run tests
cargo fmt            # Format
cargo clippy         # Lint
cargo build --release  # Release build

# Debug logging
FORAGER_DEBUG=1 cargo run --bin forager
```

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=kimyoungjin06/forager-cli&type=date&legend=top-left)](https://www.star-history.com/#kimyoungjin06/forager-cli&type=date&legend=top-left)

## Acknowledgments

Inspired by [agent-deck](https://github.com/asheshgoplani/agent-deck) (Go + Bubble Tea).

## License

MIT License -- see [LICENSE](LICENSE) for details.
