# Configuration Reference

Forager uses a layered configuration system. Settings are resolved in this order:

1. **Global config** -- `~/.forager/config.toml` (or `~/.config/forager/config.toml` on Linux)
2. **Profile config** -- `~/.forager/profiles/<name>/config.toml`
3. **Repo config** -- `.forager/config.toml` in the project root

Forager still reads and writes the existing legacy paths when they
already exist: `~/.agent-of-empires`, `~/.config/agent-of-empires`, and
`.aoe/config.toml`.

Run `forager doctor` to see which global data path, active profile directory,
repo config path, and profile environment source are active on the current
machine. `forager status --json` also reports `profile_dir`,
`profile_dir_source`, `app_dir`, and `app_dir_source` so automation can tell
whether compatibility storage is currently active.

Run `forager migrate aoe` to copy existing legacy global data and the
current repo's `.aoe/config.toml` into the new Forager paths. The migration keeps
legacy paths as backups and refuses to overwrite existing Forager targets.

Later layers override earlier ones. Only explicitly set fields override; unset fields inherit from the previous layer.

All active settings below can also be edited from the TUI settings screen (press `s` or access via the menu). Compatibility-only keys are called out explicitly and are not shown as active controls.

## File Locations

| Platform | Global Config |
|----------|--------------|
| Linux | `$XDG_CONFIG_HOME/forager/config.toml` (defaults to `~/.config/forager/`) |
| macOS | `~/.forager/config.toml` |

```
~/.forager/
  config.toml              # Global configuration
  trusted_repos.toml       # Hook trust decisions (auto-managed)
  .schema_version          # Migration tracking (auto-managed)
  profiles/
    default/
      sessions.json        # Session data
      groups.json          # Group hierarchy
      config.toml          # Profile-specific overrides
  logs/                    # Session execution logs
```

## Environment Variables

| Variable | Description |
|----------|-------------|
| `FORAGER_PROFILE` | Default profile to use |
| `FORAGER_DEBUG` | Enable debug logging (`1` to enable) |
| `AGENT_OF_EMPIRES_PROFILE` | Legacy fallback for `FORAGER_PROFILE` |
| `AGENT_OF_EMPIRES_DEBUG` | Legacy fallback for `FORAGER_DEBUG` |

## Project Registry And Telegram Operator

Multi-project Telegram routing uses
`$XDG_CONFIG_HOME/forager/projects.toml`, defaulting to
`~/.config/forager/projects.toml` on Linux. Keys, display names, workspace
folder names, and `aliases` can all select a project:

```toml
schema = "forager_project_registry.v1"

[projects.twinpaper]
display_name = "TwinPaper"
aliases = ["트윈페이퍼"]
workspace_patterns = ["1.2.8.TwinPaper"]
session_group = "Twin"
wiki_profile = "twinpaper-review"
```

Keep aliases unique across projects. Exact selectors with a duplicated alias
are rejected as ambiguous, and Telegram asks for a canonical project key.
ASCII project mentions use token boundaries, so a key embedded in a longer
ordinary word does not silently change chat or Wiki focus.

The Telegram service installer should receive the workspace root explicitly
when it cannot infer the nearest directory named `Workspace`:

The default Telegram credential file is
`$XDG_CONFIG_HOME/forager/telegram.env` on Linux, falling back to
`~/.config/forager/telegram.env`. On macOS it is
`~/.forager/telegram.env`. `OFFDESK_TELEGRAM_ENV` and `--env-file` override
that default.

```bash
python3 scripts/install_offdesk_telegram_operator_service.py \
  --workspace-root /path/to/Workspace \
  --env-file /path/to/telegram.env \
  --dry-run
```

The env file contains a bot token and must be private. Set mode `0600` before
starting or installing the service:

```bash
chmod 600 /path/to/telegram.env
```

Listener state, feedback, conversation JSONL, update journal, reply outbox, and
loop-status files are written with mode `0600`. Their parent cache directory is
created with mode `0700`. The service installer passes explicit
`--update-journal-file` and `--reply-outbox-file` paths. The journal prevents a
completed Telegram effect from being executed again, while the outbox retries
an undelivered reply independently of the Telegram offset.

## General

The Global tab's General category also edits `default_profile`, which is the
profile used when `forager` starts without `-p`. The same value can be changed
with `forager profile default <name>`.

## Session

```toml
[session]
default_tool = "claude"   # claude, opencode, vibe, codex, gemini
yolo_mode_default = false
auto_orchestrator = false
orchestrator_title = "Orchestrator"
orchestrator_command = "forager-orch start"
```

| Option | Default | Description |
|--------|---------|-------------|
| `default_tool` | (auto-detect) | Default agent for new sessions. Falls back to the first available tool if unset or unavailable. |
| `yolo_mode_default` | `false` | Enable YOLO mode by default for new sessions (skip permission prompts). |
| `auto_orchestrator` | `false` | Create an orchestrator session when a project session is created. |
| `orchestrator_title` | (built-in title) | Optional title for automatically created orchestrator sessions. |
| `orchestrator_command` | (selected agent command) | Optional command override for automatically created orchestrator sessions. |

## Worktree

```toml
[worktree]
path_template = "../{repo-name}-worktrees/{branch}"
bare_repo_path_template = "./{branch}"
auto_cleanup = true
show_branch_in_tui = true
delete_branch_on_cleanup = false
```

| Option | Default | Description |
|--------|---------|-------------|
| `path_template` | `../{repo-name}-worktrees/{branch}` | Path template for worktrees in regular repos |
| `bare_repo_path_template` | `./{branch}` | Path template for worktrees in bare repos |
| `auto_cleanup` | `true` | Prompt to remove worktree when deleting a session |
| `show_branch_in_tui` | `true` | Display branch name in the TUI session list |
| `delete_branch_on_cleanup` | `false` | Also delete the git branch when removing a worktree |

The old `worktree.enabled` key is still accepted when reading existing files,
but it does not control current session creation. A worktree is requested
explicitly by entering a branch in the new-session dialog or CLI command.

## Cleanup

```toml
[sandbox]
auto_cleanup = true
```

`sandbox.auto_cleanup` only controls whether stored legacy sandbox containers
are selected for deletion by default. New sandbox sessions are not created.

**Template variables:**

| Variable | Description |
|----------|-------------|
| `{repo-name}` | Repository folder name |
| `{branch}` | Branch name (slashes converted to hyphens) |
| `{session-id}` | First 8 characters of session UUID |

## tmux

```toml
[tmux]
status_bar = "auto"
mouse = "auto"
```

| Option | Default | Description |
|--------|---------|-------------|
| `status_bar` | `"auto"` | `"auto"`: apply if no `~/.tmux.conf`; `"enabled"`: always apply; `"disabled"`: never apply |
| `mouse` | `"auto"` | Same modes as `status_bar`. Controls mouse support in Forager tmux sessions. |

Forager applies only the session-local `mouse` option. It does not replace
server-global `MouseDrag1Pane`, `MouseDragEnd1Pane`, or copy-mode bindings from
the user's tmux configuration. This preserves custom clipboard commands and
selection behavior such as `copy-pipe-no-clear`. Hold Shift while dragging when
terminal-native selection is preferred over tmux selection.

New Claude sessions started by Forager set
`CLAUDE_CODE_DISABLE_ALTERNATE_SCREEN=1`. Claude then renders into tmux's normal
scrollback buffer, so dragging to select text and scrolling while a tmux
selection is active behave consistently with Codex sessions. This policy does
not affect standalone Claude processes, Codex sessions, or Forager terminal
sessions. A Claude process that was already running must be restarted before
the environment change takes effect.

## Diff

```toml
[diff]
default_branch = "main"
context_lines = 3
```

| Option | Default | Description |
|--------|---------|-------------|
| `default_branch` | (auto-detect) | Base branch for diffs |
| `context_lines` | `3` | Lines of context around changes |

## Updates

```toml
[updates]
check_enabled = true
check_interval_hours = 24
notify_in_cli = true
```

| Option | Default | Description |
|--------|---------|-------------|
| `check_enabled` | `true` | Check for new versions |
| `check_interval_hours` | `24` | Hours between update checks |
| `notify_in_cli` | `true` | Show update notifications in CLI output |

The old `updates.auto_update` key is still accepted when reading existing
files, but Forager does not install updates automatically.

## Claude

```toml
[claude]
config_dir = "~/.claude"
```

| Option | Default | Description |
|--------|---------|-------------|
| `config_dir` | (none) | `CLAUDE_CONFIG_DIR` for newly started Claude sessions. Supports `~/` prefix. |

## Profiles

Profiles provide separate workspaces with their own sessions and groups. Each profile can override any of the settings above.

```bash
forager                 # Uses "default" profile
forager -p work         # Uses "work" profile
forager profile create client-xyz
forager profile list
forager profile default work   # Set "work" as default
```

Profile overrides go in `~/.forager/profiles/<name>/config.toml` and use the same format as the global config.

## Repo Config

Per-repo settings go in `.forager/config.toml` at your project root. Run `forager init` to generate a template. Existing `.aoe/config.toml` files are still honored.

Repo config supports `[hooks]`, `[session]`, `[worktree]`, `[sandbox]`,
`[tmux]`, and `[diff]`. Updates, sound, Claude config directory, and the default
profile remain global/profile settings. Older repo files containing `[updates]`
or `[sound]` are still read for compatibility, but those sections are not shown
as active Repo-tab controls.

See [Repo Config & Hooks](repo-config.md) for details.
