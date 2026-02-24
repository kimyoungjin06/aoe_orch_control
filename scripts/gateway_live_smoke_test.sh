#!/usr/bin/env bash
set -euo pipefail

ROOT="/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control"
TEAM_DIR="$ROOT/.aoe-team"
EVENT_LOG="$TEAM_DIR/logs/gateway_events.jsonl"
TEST_ID="live-smoke-$(date +%s)-$$"

BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-}"
CHAT_ID="${TG_TEST_CHAT_ID:-}"
API="https://api.telegram.org/bot${BOT_TOKEN}"

if [[ -z "$BOT_TOKEN" ]]; then
  echo "[FAIL] TELEGRAM_BOT_TOKEN is required"
  exit 1
fi

if [[ -z "$CHAT_ID" ]]; then
  echo "[FAIL] TG_TEST_CHAT_ID is required"
  exit 1
fi

if [[ ! -f "$EVENT_LOG" ]]; then
  echo "[FAIL] event log not found: $EVENT_LOG"
  echo "Start gateway first: $TEAM_DIR/telegram_tmux.sh start"
  exit 1
fi

send_msg() {
  local text="$1"
  python3 - "$API" "$CHAT_ID" "$text" <<'PY'
import json
import sys
import urllib.request

api = sys.argv[1]
chat_id = sys.argv[2]
text = sys.argv[3]
payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
req = urllib.request.Request(
    api + "/sendMessage",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(req, timeout=20) as resp:
    resp.read()
PY
}

find_trace_id() {
  local marker="$1"
  python3 - "$EVENT_LOG" "$CHAT_ID" "$marker" <<'PY'
import json
import sys

path = sys.argv[1]
chat_id = sys.argv[2]
marker = sys.argv[3]
actor = f"telegram:{chat_id}"

rows = []
with open(path, "r", encoding="utf-8") as f:
    rows = f.readlines()

for raw in reversed(rows):
    raw = raw.strip()
    if not raw:
        continue
    try:
        row = json.loads(raw)
    except Exception:
        continue
    if not isinstance(row, dict):
        continue
    if str(row.get("actor", "")) != actor:
        continue
    if str(row.get("event", "")) != "incoming_message":
        continue
    detail = str(row.get("detail", ""))
    if marker not in detail:
        continue
    trace_id = str(row.get("trace_id", "")).strip()
    if trace_id:
        print(trace_id)
        break
PY
}

wait_for_trace() {
  local marker="$1"
  local timeout="${2:-20}"
  local waited=0
  while (( waited < timeout )); do
    local trace
    trace="$(find_trace_id "$marker" || true)"
    if [[ -n "$trace" ]]; then
      printf '%s\n' "$trace"
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

has_trace_event() {
  local trace_id="$1"
  local event_csv="$2"
  python3 - "$EVENT_LOG" "$trace_id" "$event_csv" <<'PY'
import json
import sys

path = sys.argv[1]
trace_id = sys.argv[2]
event_set = set(x.strip() for x in sys.argv[3].split(",") if x.strip())

with open(path, "r", encoding="utf-8") as f:
    for raw in f:
        raw = raw.strip()
        if not raw:
            continue
        try:
            row = json.loads(raw)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        if str(row.get("trace_id", "")) != trace_id:
            continue
        if str(row.get("event", "")) in event_set:
            print("hit")
            sys.exit(0)
sys.exit(1)
PY
}

wait_for_trace_event() {
  local trace_id="$1"
  local event_csv="$2"
  local timeout="${3:-30}"
  local waited=0
  while (( waited < timeout )); do
    if has_trace_event "$trace_id" "$event_csv" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
    waited=$((waited + 1))
  done
  return 1
}

echo "[STEP] /help"
HELP_MARKER="${TEST_ID}-help"
send_msg "/help $HELP_MARKER"
help_trace="$(wait_for_trace "$HELP_MARKER" 20 || true)"
if [[ -z "$help_trace" ]]; then
  echo "[FAIL] /help trace not observed"
  exit 1
fi
wait_for_trace_event "$help_trace" "command_resolved,send_message" 20 || { echo "[FAIL] /help command flow not observed"; exit 1; }

echo "[STEP] /monitor 1"
MON_MARKER="${TEST_ID}-monitor"
send_msg "/monitor 1 $MON_MARKER"
mon_trace="$(wait_for_trace "$MON_MARKER" 20 || true)"
if [[ -z "$mon_trace" ]]; then
  echo "[FAIL] /monitor trace not observed"
  exit 1
fi
wait_for_trace_event "$mon_trace" "command_resolved,send_message" 20 || { echo "[FAIL] /monitor command flow not observed"; exit 1; }

echo "[STEP] /dispatch"
DISPATCH_MARKER="${TEST_ID}-dispatch"
send_msg "/dispatch live-smoke dispatch test $DISPATCH_MARKER"
dispatch_trace="$(wait_for_trace "$DISPATCH_MARKER" 20 || true)"
if [[ -z "$dispatch_trace" ]]; then
  echo "[FAIL] /dispatch trace not observed"
  exit 1
fi
wait_for_trace_event "$dispatch_trace" "command_resolved" 20 || { echo "[FAIL] /dispatch run command not observed"; exit 1; }
if ! wait_for_trace_event "$dispatch_trace" "dispatch_result,dispatch_completed,handler_error" 90; then
  echo "[FAIL] dispatch result not observed in 90s"
  exit 1
fi

echo "[PASS] gateway_live_smoke_test"
