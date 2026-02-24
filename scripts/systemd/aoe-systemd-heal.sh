#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
STACK_SCRIPT="$PROJECT_ROOT/.aoe-team/telegram_tmux.sh"
STACK_UNIT="aoe-telegram-stack.service"

if "$STACK_SCRIPT" health --wait=2 >/dev/null 2>&1; then
  echo "[OK] stack healthy"
  exit 0
fi

echo "[WARN] stack unhealthy, restarting"
systemctl --user restart "$STACK_UNIT"

if "$STACK_SCRIPT" health --wait=12 >/dev/null 2>&1; then
  echo "[OK] stack recovered"
  exit 0
fi

echo "[FAIL] stack recovery failed after retries"
exit 1
