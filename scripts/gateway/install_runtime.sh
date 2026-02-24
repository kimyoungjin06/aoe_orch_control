#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BIN_DIR="${HOME}/.local/bin"

SRC_GW="$PROJECT_ROOT/scripts/gateway/aoe-telegram-gateway.py"
SRC_ACL="$PROJECT_ROOT/scripts/gateway/aoe_tg_acl.py"
SRC_PARSE="$PROJECT_ROOT/scripts/gateway/aoe_tg_parse.py"
SRC_RESOLVER="$PROJECT_ROOT/scripts/gateway/aoe_tg_command_resolver.py"
SRC_HANDLERS="$PROJECT_ROOT/scripts/gateway/aoe_tg_command_handlers.py"
SRC_MGMT_HANDLERS="$PROJECT_ROOT/scripts/gateway/aoe_tg_management_handlers.py"
SRC_ORCH_OVERVIEW_HANDLERS="$PROJECT_ROOT/scripts/gateway/aoe_tg_orch_overview_handlers.py"
SRC_ORCH_TASK_HANDLERS="$PROJECT_ROOT/scripts/gateway/aoe_tg_orch_task_handlers.py"
SRC_RETRY_HANDLERS="$PROJECT_ROOT/scripts/gateway/aoe_tg_retry_handlers.py"
SRC_ROLE_HANDLERS="$PROJECT_ROOT/scripts/gateway/aoe_tg_role_handlers.py"
SRC_MESSAGE_FLOW="$PROJECT_ROOT/scripts/gateway/aoe_tg_message_flow.py"
SRC_RUN_HANDLERS="$PROJECT_ROOT/scripts/gateway/aoe_tg_run_handlers.py"
DST_GW="$BIN_DIR/aoe-telegram-gateway"
DST_ACL="$BIN_DIR/aoe_tg_acl.py"
DST_PARSE="$BIN_DIR/aoe_tg_parse.py"
DST_RESOLVER="$BIN_DIR/aoe_tg_command_resolver.py"
DST_HANDLERS="$BIN_DIR/aoe_tg_command_handlers.py"
DST_MGMT_HANDLERS="$BIN_DIR/aoe_tg_management_handlers.py"
DST_ORCH_OVERVIEW_HANDLERS="$BIN_DIR/aoe_tg_orch_overview_handlers.py"
DST_ORCH_TASK_HANDLERS="$BIN_DIR/aoe_tg_orch_task_handlers.py"
DST_RETRY_HANDLERS="$BIN_DIR/aoe_tg_retry_handlers.py"
DST_ROLE_HANDLERS="$BIN_DIR/aoe_tg_role_handlers.py"
DST_MESSAGE_FLOW="$BIN_DIR/aoe_tg_message_flow.py"
DST_RUN_HANDLERS="$BIN_DIR/aoe_tg_run_handlers.py"

if [[ ! -f "$SRC_GW" ]]; then
  echo "[ERROR] missing gateway source: $SRC_GW"
  exit 1
fi

if [[ ! -f "$SRC_ACL" ]]; then
  echo "[ERROR] missing ACL module source: $SRC_ACL"
  exit 1
fi

if [[ ! -f "$SRC_PARSE" ]]; then
  echo "[ERROR] missing parse module source: $SRC_PARSE"
  exit 1
fi

if [[ ! -f "$SRC_RESOLVER" ]]; then
  echo "[ERROR] missing command resolver source: $SRC_RESOLVER"
  exit 1
fi

if [[ ! -f "$SRC_HANDLERS" ]]; then
  echo "[ERROR] missing command handlers source: $SRC_HANDLERS"
  exit 1
fi

if [[ ! -f "$SRC_MGMT_HANDLERS" ]]; then
  echo "[ERROR] missing management handlers source: $SRC_MGMT_HANDLERS"
  exit 1
fi

if [[ ! -f "$SRC_ORCH_OVERVIEW_HANDLERS" ]]; then
  echo "[ERROR] missing orch overview handlers source: $SRC_ORCH_OVERVIEW_HANDLERS"
  exit 1
fi

if [[ ! -f "$SRC_ORCH_TASK_HANDLERS" ]]; then
  echo "[ERROR] missing orch task handlers source: $SRC_ORCH_TASK_HANDLERS"
  exit 1
fi

if [[ ! -f "$SRC_RETRY_HANDLERS" ]]; then
  echo "[ERROR] missing retry handlers source: $SRC_RETRY_HANDLERS"
  exit 1
fi

if [[ ! -f "$SRC_ROLE_HANDLERS" ]]; then
  echo "[ERROR] missing role handlers source: $SRC_ROLE_HANDLERS"
  exit 1
fi

if [[ ! -f "$SRC_MESSAGE_FLOW" ]]; then
  echo "[ERROR] missing message flow source: $SRC_MESSAGE_FLOW"
  exit 1
fi

if [[ ! -f "$SRC_RUN_HANDLERS" ]]; then
  echo "[ERROR] missing run handlers source: $SRC_RUN_HANDLERS"
  exit 1
fi

mkdir -p "$BIN_DIR"
chmod 755 "$SRC_GW" "$SRC_ACL" "$SRC_PARSE" "$SRC_RESOLVER" "$SRC_HANDLERS" \
  "$SRC_MGMT_HANDLERS" "$SRC_ORCH_OVERVIEW_HANDLERS" "$SRC_ORCH_TASK_HANDLERS" \
  "$SRC_RETRY_HANDLERS" "$SRC_ROLE_HANDLERS" "$SRC_MESSAGE_FLOW" "$SRC_RUN_HANDLERS"
ln -sfn "$SRC_GW" "$DST_GW"
ln -sfn "$SRC_ACL" "$DST_ACL"
ln -sfn "$SRC_PARSE" "$DST_PARSE"
ln -sfn "$SRC_RESOLVER" "$DST_RESOLVER"
ln -sfn "$SRC_HANDLERS" "$DST_HANDLERS"
ln -sfn "$SRC_MGMT_HANDLERS" "$DST_MGMT_HANDLERS"
ln -sfn "$SRC_ORCH_OVERVIEW_HANDLERS" "$DST_ORCH_OVERVIEW_HANDLERS"
ln -sfn "$SRC_ORCH_TASK_HANDLERS" "$DST_ORCH_TASK_HANDLERS"
ln -sfn "$SRC_RETRY_HANDLERS" "$DST_RETRY_HANDLERS"
ln -sfn "$SRC_ROLE_HANDLERS" "$DST_ROLE_HANDLERS"
ln -sfn "$SRC_MESSAGE_FLOW" "$DST_MESSAGE_FLOW"
ln -sfn "$SRC_RUN_HANDLERS" "$DST_RUN_HANDLERS"

echo "[OK] linked runtime"
echo " - $DST_GW -> $SRC_GW"
echo " - $DST_ACL -> $SRC_ACL"
echo " - $DST_PARSE -> $SRC_PARSE"
echo " - $DST_RESOLVER -> $SRC_RESOLVER"
echo " - $DST_HANDLERS -> $SRC_HANDLERS"
echo " - $DST_MGMT_HANDLERS -> $SRC_MGMT_HANDLERS"
echo " - $DST_ORCH_OVERVIEW_HANDLERS -> $SRC_ORCH_OVERVIEW_HANDLERS"
echo " - $DST_ORCH_TASK_HANDLERS -> $SRC_ORCH_TASK_HANDLERS"
echo " - $DST_RETRY_HANDLERS -> $SRC_RETRY_HANDLERS"
echo " - $DST_ROLE_HANDLERS -> $SRC_ROLE_HANDLERS"
echo " - $DST_MESSAGE_FLOW -> $SRC_MESSAGE_FLOW"
echo " - $DST_RUN_HANDLERS -> $SRC_RUN_HANDLERS"
