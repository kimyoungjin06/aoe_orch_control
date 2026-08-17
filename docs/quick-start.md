# Quick Start

## Launch the TUI

```bash
forager
```

This opens the dashboard. You'll see an empty session list on first run.

## Create Your First Session

**From the TUI:** Press `n` to open the new session dialog. Fill in the path to your project (or leave it as `.` for the current directory) and press `Enter`.

**From the CLI:**

```bash
forager add /path/to/project
```

The session appears in the dashboard with status **Idle**.

## Attach to a Session

Select a session and press `Enter` to attach. You're now inside a tmux session running your AI agent (Claude Code by default).

To return to the TUI, press **`Ctrl+b d`** (the standard tmux detach shortcut).

## Use the Terminal View

Press `t` to toggle between Agent View and Terminal View. Each agent session has a paired shell terminal where you can run builds, tests, and git commands without interrupting the agent.

## Review Changes with Diff View

Press `D` to open the diff view. This shows changes between your working directory and the base branch. Navigate files with `j`/`k`, press `e` to edit, and `Esc` to close.

## Create a Worktree Session

To work on a new branch with its own directory:

```bash
# CLI
forager add . -w feat/my-feature -b

# TUI: press n, fill in the worktree branch field
```

This creates a new git branch, a worktree directory, and a session pointing at it. When you delete the session, Forager offers to clean up the worktree too.

## Choose a Different Agent

By default, Forager uses Claude Code. To use a different tool:

```bash
forager add -c opencode .
forager add -c vibe .
forager add -c codex .
forager add -c gemini .
```

In the TUI, select the tool from the dropdown in the new session dialog.

## TUI Keyboard Reference

| Key | Action |
|-----|--------|
| `n` | New session |
| `a` | Open the read-only operator attention queue |
| `Enter` | Attach to session |
| `d` | Delete session |
| `t` | Toggle Agent/Terminal view |
| `D` | Open diff view |
| `/` | Search sessions |
| `?` | Show help |
| `q` | Quit |
| `Ctrl+b d` | Detach from tmux session |

## Next Steps

- [Workflow Guide](guides/workflow.md) -- recommended setup with bare repos and parallel agents
- [Repo Config & Hooks](guides/repo-config.md) -- per-project settings
- [CLI Reference](cli/reference.md) -- every command and flag
