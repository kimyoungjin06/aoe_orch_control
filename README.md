# Agent of Empires

A terminal session manager for AI coding agents, written in Rust.

## How It Works

Agent of Empires is a wrapper around [tmux](https://github.com/tmux/tmux/wiki), the terminal multiplexer. Each AI coding session you create is actually a tmux session under the hood.

Once you attach to a session, you're working directly in tmux. Basic tmux knowledge helps:

| tmux Command | What It Does |
|--------------|--------------|
| `Ctrl+b d` | Detach from session (return to Agent of Empires) |
| `Ctrl+b [` | Enter scroll/copy mode |
| `Ctrl+b c` | Create new window within session |
| `Ctrl+b n` / `Ctrl+b p` | Next/previous window |

If you're new to tmux, the key thing to remember is `Ctrl+b d` to detach and return to the TUI.

## Installation

### Homebrew (macOS and Linux)

```bash
brew tap njbrake/aoe
brew install aoe
```

### From GitHub Releases

Download the latest release for your platform from the [releases page](https://github.com/njbrake/agent-of-empires/releases).

```bash
# Example for macOS ARM64
curl -LO https://github.com/njbrake/agent-of-empires/releases/latest/download/aoe-darwin-arm64.tar.gz
tar -xzf aoe-darwin-arm64.tar.gz
sudo mv aoe-darwin-arm64 /usr/local/bin/aoe
```

### From Source

```bash
cargo install --git https://github.com/njbrake/agent-of-empires
```

Or clone and build:

```bash
git clone https://github.com/njbrake/agent-of-empires
cd agent-of-empires
cargo build --release
# Binary at target/release/aoe
```

## Features

- **TUI Dashboard** - Visual interface to manage all your AI coding sessions
- **Session Management** - Create, attach, detach, and delete sessions
- **Group Organization** - Organize sessions into hierarchical folders
- **Status Detection** - Automatic status detection for Claude Code and OpenCode
- **tmux Integration** - Sessions persist in tmux for reliability
- **MCP Server Management** - Configure and manage Model Context Protocol servers
- **Multi-profile Support** - Separate workspaces for different projects

## Requirements

- **tmux** - Required for session management (installed automatically via Homebrew)
  - macOS: `brew install tmux`
  - Ubuntu/Debian: `sudo apt install tmux`
  - Familiarity with basic tmux operations is recommended (see [How It Works](#how-it-works) above)

## Quick Start

```bash
# Launch the TUI
aoe

# Or add a session directly from CLI
aoe add /path/to/project
```

## Using the TUI

### Launching

```bash
aoe           # Launch TUI with default profile
aoe -p work   # Launch with a specific profile
```

### Keyboard Shortcuts

| Key | Action |
|-----|--------|
| **Navigation** | |
| `j` / `↓` | Move down |
| `k` / `↑` | Move up |
| `h` / `←` | Collapse group |
| `l` / `→` | Expand group |
| `g` | Go to top |
| `G` | Go to bottom |
| `PageUp` / `PageDown` | Page navigation |
| **Actions** | |
| `Enter` | Attach to selected session |
| `n` | Create new session |
| `d` | Delete selected session |
| `r` / `F5` | Refresh session list |
| **Other** | |
| `/` | Search sessions |
| `?` | Toggle help overlay |
| `q` / `Ctrl+c` | Quit TUI |

### Attaching and Detaching from Sessions

1. **Attach to a session**: Select a session and press `Enter`
   - The TUI will temporarily exit and you'll be connected to the tmux session

2. **Detach from a session**: Press `Ctrl+b` then `d`
   - This is tmux's standard detach sequence
   - You'll return to the Agent of Empires TUI

3. **Alternative detach** (if already in tmux): The session will be switched, use `Ctrl+b d` to return

### Session Status Indicators

- 🟢 **Running** - Agent is actively processing
- 🟡 **Waiting** - Agent is waiting for input
- ⚪ **Idle** - Session is inactive
- 🔴 **Error** - An error was detected

## CLI Commands

```bash
# Session management
aoe add <path>              # Add a new session
aoe add . --title "my-proj" # Add with custom title
aoe list                    # List all sessions
aoe list --json             # List as JSON
aoe remove <id|title>       # Remove a session
aoe status                  # Show status summary

# Session lifecycle
aoe session start <id>      # Start a session
aoe session stop <id>       # Stop a session
aoe session restart <id>    # Restart a session
aoe session attach <id>     # Attach to a session
aoe session show <id>       # Show session details

# Groups
aoe group create <name>     # Create a group
aoe group list              # List groups
aoe group delete <name>     # Delete a group

# Profiles
aoe profile list            # List profiles
aoe profile create <name>   # Create a profile
aoe profile delete <name>   # Delete a profile

# MCP servers
aoe mcp list                # List configured MCP servers
aoe mcp attach <name>       # Attach MCP to current session
aoe mcp detach <name>       # Detach MCP from current session

# Maintenance
aoe update                  # Check for updates
aoe uninstall               # Uninstall Agent of Empires
```

## Configuration

### Profiles

Profiles let you maintain separate workspaces with their own sessions and groups. This is useful when you want to keep different contexts isolated—for example, work projects vs personal projects, or different client engagements.

```bash
aoe                 # Uses "default" profile
aoe -p work         # Uses "work" profile
aoe -p client-xyz   # Uses "client-xyz" profile
```

Each profile stores its own `sessions.json` and `groups.json`, so switching profiles gives you a completely different set of sessions.

### File Locations

Configuration is stored in `~/.agent-of-empires/`:

```
~/.agent-of-empires/
├── config.toml           # Global configuration
├── profiles/
│   └── default/
│       ├── sessions.json # Session data
│       └── groups.json   # Group structure
└── logs/                 # Session logs
```

### Environment Variables

| Variable | Description |
|----------|-------------|
| `AGENT_OF_EMPIRES_PROFILE` | Default profile to use |
| `AGENT_OF_EMPIRES_DEBUG` | Enable debug logging |

## Development

```bash
# Check code
cargo check

# Run tests
cargo test

# Format code
cargo fmt

# Lint
cargo clippy

# Run in debug mode
AGENT_OF_EMPIRES_DEBUG=1 cargo run

# Build release binary
cargo build --release
```

## Dependencies

Key dependencies:
- `ratatui` + `crossterm` - TUI framework
- `clap` - CLI argument parsing
- `serde` + `serde_json` + `toml` - Serialization
- `tokio` - Async runtime
- `notify` - File system watching
- `reqwest` - HTTP client for updates

## License

MIT License - see [LICENSE](LICENSE) for details.
