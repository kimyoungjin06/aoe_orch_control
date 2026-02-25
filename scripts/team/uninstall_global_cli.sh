#!/usr/bin/env bash
set -euo pipefail

BIN_DIR="${HOME}/.local/bin"

for name in aoe-team-stack aoe-team-tmux; do
  path="$BIN_DIR/$name"
  if [[ -L "$path" || -f "$path" ]]; then
    rm -f "$path"
    echo "[OK] removed $path"
  else
    echo "[SKIP] not found: $path"
  fi
done
