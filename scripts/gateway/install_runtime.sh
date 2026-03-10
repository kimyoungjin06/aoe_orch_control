#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN_DIR="${HOME}/.local/bin"
GATEWAY_DIR="$PROJECT_ROOT/scripts/gateway"
MAIN_SRC="$GATEWAY_DIR/aoe-telegram-gateway.py"
MAIN_DST="$BIN_DIR/aoe-telegram-gateway"

if [[ ! -f "$MAIN_SRC" ]]; then
  echo "[ERROR] missing gateway source: $MAIN_SRC"
  exit 1
fi

mkdir -p "$BIN_DIR"

linked=()
while IFS= read -r -d '' src; do
  chmod 755 "$src"
  dst="$BIN_DIR/$(basename "$src")"
  ln -sfn "$src" "$dst"
  linked+=("$dst -> $src")
done < <(find "$GATEWAY_DIR" -maxdepth 1 -type f -name '*.py' -print0 | sort -z)

ln -sfn "$MAIN_SRC" "$MAIN_DST"

echo "[OK] linked gateway runtime"
echo " - $MAIN_DST -> $MAIN_SRC"
for row in "${linked[@]}"; do
  echo " - $row"
done
