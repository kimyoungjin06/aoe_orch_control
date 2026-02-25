#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN_DIR="${HOME}/.local/bin"

SRC_STACK="$PROJECT_ROOT/scripts/team/aoe-team-stack.sh"
DST_STACK="$BIN_DIR/aoe-team-stack"
DST_TMUX="$BIN_DIR/aoe-team-tmux"

if [[ ! -x "$SRC_STACK" ]]; then
  echo "[ERROR] missing launcher script: $SRC_STACK" >&2
  exit 1
fi

mkdir -p "$BIN_DIR"
ln -sfn "$SRC_STACK" "$DST_STACK"
ln -sfn "$SRC_STACK" "$DST_TMUX"

echo "[OK] installed global launchers"
echo " - $DST_STACK -> $SRC_STACK"
echo " - $DST_TMUX -> $SRC_STACK"
echo
echo "examples:"
echo "  aoe-team-stack start"
echo "  aoe-team-stack ui"
echo "  aoe-team-stack panel"
echo "  aoe-team-stack switch 2"
