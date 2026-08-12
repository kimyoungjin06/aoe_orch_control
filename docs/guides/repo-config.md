# Repository Configuration & Hooks

Forager supports per-repo configuration via a `.forager/config.toml` file in your project root. Existing `.aoe/config.toml` files are still honored as a compatibility fallback. This lets you define project-specific defaults and hooks that apply to every team member using Forager on that repo.

## Getting Started

Generate a template config:

```bash
forager init
```

This creates `.forager/config.toml` with commented-out examples. Edit the file to enable the settings you need.

To inspect which repo config path is active, run:

```bash
forager doctor
```

To copy an existing `.aoe/config.toml` into `.forager/config.toml`, run:

```bash
forager migrate aoe
```

The migration preserves `.aoe/config.toml` and stops without copying anything if
`.forager/config.toml` already exists.

## Configuration Sections

### Hooks

Hooks run shell commands at specific points in the session lifecycle.

```toml
[hooks]
# Run once when a session is first created (failures abort creation)
on_create = ["npm install", "cp .env.example .env"]

# Run every time a session starts (failures are logged but non-fatal)
on_launch = ["npm install"]
```

**`on_create`** runs only once, when the session is first created. If any command fails, session creation is aborted. Use this for one-time setup like installing dependencies or generating config files.

**`on_launch`** runs every time a session starts (including the first time, and every restart). Failures are logged as warnings but don't prevent the session from starting. Use this for things like ensuring dependencies are up to date.

### Session

```toml
[session]
default_tool = "opencode"   # Override the default agent for this repo
auto_orchestrator = true
```

Available tools: `claude`, `opencode`, `vibe`, `codex`, `gemini`.
The executable `session.orchestrator_command` setting remains global/profile
only and is not applied from checked-in repo config.

### Worktree

Override worktree settings for this repo:

```toml
[worktree]
path_template = "../{repo-name}-worktrees/{branch}"
bare_repo_path_template = "./{branch}"
auto_cleanup = true
show_branch_in_tui = true
delete_branch_on_cleanup = false
```

Repo config can also override `[sandbox]`, `[tmux]`, and `[diff]`. The settings
TUI exposes exactly the active repo-supported categories when the Repo tab is
available. Updates, sound, Claude config directory, and the default profile are
personal settings and stay out of active repo config.

## Hook Trust System

When Forager encounters hooks in a repo for the first time, it prompts you to review and approve them before execution. This prevents untrusted repos from running arbitrary commands.

- Trust decisions are stored globally (shared across all profiles)
- If hook commands change (e.g., someone updates `.forager/config.toml`), Forager prompts for re-approval
- Use `--trust-hooks` with `forager add` to skip the trust prompt (useful for CI or repos you control)

```bash
# Trust hooks automatically
forager add --trust-hooks .
```

## Config Precedence

Settings are resolved in this order (later overrides earlier):

1. **Global config** (`~/.forager/config.toml`)
2. **Profile config** (`~/.forager/profiles/<name>/config.toml`)
3. **Repo config** (`.forager/config.toml`)

Legacy `~/.agent-of-empires`, `~/.config/agent-of-empires`, and `.aoe/config.toml`
paths are used when they already exist and the new Forager paths do not.

Only settings that are explicitly set in the repo config override the global/profile values. Unset fields inherit from the higher-level config.

## Example: Full Repo Config

```toml
[hooks]
on_create = ["npm install", "npx prisma generate"]
on_launch = ["npm install"]

[session]
default_tool = "claude"

[worktree]
show_branch_in_tui = true
```

## Checking Into Version Control

The `.forager/config.toml` file is meant to be committed to your repo so the entire team shares the same configuration. The hook trust system ensures that each developer explicitly approves hook commands before they run.
