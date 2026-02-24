#!/usr/bin/env bash
set -euo pipefail

UNIT_DST_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"

STACK_UNIT="aoe-telegram-stack.service"
HEAL_UNIT="aoe-telegram-heal.service"
HEAL_TIMER="aoe-telegram-heal.timer"

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[ERROR] systemctl not found"
  exit 1
fi

echo "[STEP] disable + stop"
systemctl --user disable --now "$HEAL_TIMER" 2>/dev/null || true
systemctl --user disable --now "$STACK_UNIT" 2>/dev/null || true

echo "[STEP] remove unit files"
rm -f "$UNIT_DST_DIR/$STACK_UNIT" "$UNIT_DST_DIR/$HEAL_UNIT" "$UNIT_DST_DIR/$HEAL_TIMER"

echo "[STEP] daemon-reload"
systemctl --user daemon-reload

echo "[OK] uninstalled user services"
