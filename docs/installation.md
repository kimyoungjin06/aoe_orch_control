# Installation

## Prerequisites

- [tmux](https://github.com/tmux/tmux/wiki) (required)

## Install Forager

### Quick Install (Recommended)

Run the install script:

```bash
curl -fsSL \
  https://raw.githubusercontent.com/kimyoungjin06/forager-cli/main/scripts/install.sh \
  | bash
```

### Build from Source

```bash
git clone https://github.com/kimyoungjin06/forager-cli
cd forager-cli
cargo build --release
```

The primary binary will be at `target/release/forager`; `target/release/aoe`
is also built as a legacy compatibility alias.

Release installations verify the published SHA-256 checksum before extracting
or replacing the local binary. A missing or malformed checksum stops the
installation.

### Telegram Operator Bundle

Each release also publishes a platform-independent
`forager-operator-<version>.tar.gz` bundle. It contains the Telegram operator,
watchdog, service installer, Wiki helpers, and the operational documentation
needed to run them with an installed `forager` binary. Verify the adjacent
`.sha256` file, extract the bundle, and follow `docs/remote-operator.md` inside
the extracted directory.

The default Telegram credential file is
`$XDG_CONFIG_HOME/forager/telegram.env` on Linux, falling back to
`~/.config/forager/telegram.env`. On macOS it is
`~/.forager/telegram.env`. Set `OFFDESK_TELEGRAM_ENV` or pass `--env-file` to
use a different private file.

## Verify Installation

```bash
forager --version
```

## Uninstall

To remove Forager:

```bash
forager uninstall
```

This will guide you through removing the binary, configuration, and tmux settings.
