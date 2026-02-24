#!/usr/bin/env bash
set -euo pipefail

USER_NAME="kimyoungjin06"
RULE_FILE="/etc/sudoers.d/99-kimyoungjin06-nopasswd"
RULE_LINE="${USER_NAME} ALL=(ALL:ALL) NOPASSWD: ALL"

install -d -m 755 /etc/sudoers.d
printf '%s\n' "$RULE_LINE" > "$RULE_FILE"
chmod 440 "$RULE_FILE"
visudo -cf "$RULE_FILE" >/dev/null

echo "[OK] sudoers rule installed: $RULE_FILE"
sudo -k
if sudo -n true >/dev/null 2>&1; then
  echo "ROOT_OK"
else
  echo "ROOT_FAIL"
  exit 1
fi
