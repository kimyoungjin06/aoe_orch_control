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
installation. Releases beginning with v0.14.0 also publish
`release-provenance.json`, which binds every archive checksum to the exact Git
tag and source commit.

After installation, the script writes a private `forager_install_receipt.v1`
record containing the version, source commit when provenance is available,
archive checksum, installed binary checksum, and install path. The default
receipt is `$XDG_STATE_HOME/forager/install-receipt.txt` on Linux, falling back
to `~/.local/state/forager/install-receipt.txt`; on macOS it is
`~/.forager/install-receipt.txt`. Set `FORAGER_INSTALL_RECEIPT` to use
another path.

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
cat "${XDG_STATE_HOME:-$HOME/.local/state}/forager/install-receipt.txt"
```

On macOS, inspect `~/.forager/install-receipt.txt` instead.

## Uninstall

To remove Forager:

```bash
forager uninstall
```

This will guide you through removing the binary, configuration, and tmux settings.
