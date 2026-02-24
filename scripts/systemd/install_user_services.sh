#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
UNIT_SRC_DIR="$PROJECT_ROOT/systemd/user"
UNIT_DST_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"

STACK_UNIT="aoe-telegram-stack.service"
HEAL_UNIT="aoe-telegram-heal.service"
HEAL_TIMER="aoe-telegram-heal.timer"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[ERROR] systemctl not found"
  exit 1
fi

if ! command -v tmux >/dev/null 2>&1; then
  echo "[ERROR] tmux not found"
  exit 1
fi

mkdir -p "$UNIT_DST_DIR"

render_unit() {
  local src="$1"
  local dst="$2"
  if [[ ! -f "$src" ]]; then
    echo "[ERROR] missing template: $src"
    exit 1
  fi
  sed "s|__PROJECT_ROOT__|$PROJECT_ROOT|g" "$src" > "$dst"
}

render_unit "$UNIT_SRC_DIR/$STACK_UNIT.template" "$UNIT_DST_DIR/$STACK_UNIT"
render_unit "$UNIT_SRC_DIR/$HEAL_UNIT.template" "$UNIT_DST_DIR/$HEAL_UNIT"
render_unit "$UNIT_SRC_DIR/$HEAL_TIMER.template" "$UNIT_DST_DIR/$HEAL_TIMER"

chmod +x "$PROJECT_ROOT/.aoe-team/telegram_tmux.sh"
chmod +x "$PROJECT_ROOT/scripts/systemd/aoe-systemd-heal.sh"

echo "[STEP] daemon-reload"
systemctl --user daemon-reload

echo "[STEP] enable + start $STACK_UNIT"
systemctl --user enable --now "$STACK_UNIT"

echo "[STEP] enable + start $HEAL_TIMER"
systemctl --user enable --now "$HEAL_TIMER"

echo "[STEP] stack restart"
systemctl --user restart "$STACK_UNIT"

echo "[STEP] status"
systemctl --user --no-pager --full status "$STACK_UNIT" || true
systemctl --user --no-pager --full status "$HEAL_TIMER" || true

LINGER_STATE="$(loginctl show-user "$USER" -p Linger 2>/dev/null | awk -F= '{print $2}' || true)"
if [[ "$LINGER_STATE" != "yes" ]]; then
  echo "[HINT] To keep user services alive after logout/reboot, run:"
  echo "       sudo loginctl enable-linger $USER"
fi

echo "[OK] installed user services"
