#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEAM_DIR="$PROJECT_ROOT/.aoe-team"
ENV_FILE="$TEAM_DIR/telegram.env"
HANDLER="$TEAM_DIR/worker_codex_handler.sh"
GATEWAY_BIN="$PROJECT_ROOT/scripts/gateway/aoe-telegram-gateway.py"

SESS_GW="aoe_tg_gateway"
WORKER_PREFIX="aoe_tg_worker_"

worker_roles() {
  PROJECT_ROOT="$PROJECT_ROOT" python3 - <<'PY'
import json
import os
from pathlib import Path

project_root = Path(os.environ['PROJECT_ROOT'])
cfg = project_root / '.aoe-team' / 'orchestrator.json'
roles = []
try:
    data = json.loads(cfg.read_text(encoding='utf-8'))
    for a in data.get('agents', []):
        role = str(a.get('role', '')).strip()
        if role:
            roles.append(role)
except Exception:
    roles = ['DataEngineer', 'Reviewer']

if not roles:
    roles = ['DataEngineer', 'Reviewer']

seen = set()
for r in roles:
    if r in seen:
        continue
    seen.add(r)
    print(r)
PY
}


worker_sess_name() {
  local role="$1"
  local key
  key="$(printf '%s' "$role" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/_/g')"
  printf '%s%s\n' "$WORKER_PREFIX" "$key"
}

stop_workers() {
  tmux list-sessions -F '#{session_name}' 2>/dev/null | grep "^${WORKER_PREFIX}" | while read -r s; do
    tmux kill-session -t "$s" 2>/dev/null || true
  done || true
}

start() {
  tmux kill-session -t "$SESS_GW" 2>/dev/null || true
  stop_workers

  while read -r role; do
    [[ -n "$role" ]] || continue
    sess="$(worker_sess_name "$role")"
    tmux new-session -d -s "$sess" -c "$PROJECT_ROOT" \
      "bash -lc 'set -a; . \"$ENV_FILE\"; set +a; exec /home/kimyoungjin06/.local/bin/aoe-orch worker --project-root \"$PROJECT_ROOT\" --for \"$role\" --handler-cmd \"$HANDLER\" --interval-sec 1 --quiet'"
  done < <(worker_roles)

  tmux new-session -d -s "$SESS_GW" -c "$PROJECT_ROOT" \
    "bash -lc 'set -a; . \"$ENV_FILE\"; set +a; exec \"$GATEWAY_BIN\" --project-root \"$PROJECT_ROOT\" --verbose'"

  echo "started"
}

stop() {
  tmux kill-session -t "$SESS_GW" 2>/dev/null || true
  stop_workers
  echo "stopped"
}

status() {
  tmux list-sessions -F '#{session_name}' 2>/dev/null | grep -E "^${SESS_GW}$|^${WORKER_PREFIX}" || true
}

health() {
  local wait_sec=0
  local default_wait_sec=3

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --wait)
        shift
        if [[ $# -gt 0 && "$1" =~ ^[0-9]+$ ]]; then
          wait_sec="$1"
          shift
        else
          wait_sec="$default_wait_sec"
        fi
        ;;
      --wait=*)
        wait_sec="${1#*=}"
        if [[ ! "$wait_sec" =~ ^[0-9]+$ ]]; then
          echo "usage: $0 health [--wait[=seconds]]" >&2
          return 2
        fi
        shift
        ;;
      *)
        echo "usage: $0 health [--wait[=seconds]]" >&2
        return 2
        ;;
    esac
  done

  local attempts=1
  if (( wait_sec > 0 )); then
    attempts=$((wait_sec + 1))
  fi

  local i=1
  while (( i <= attempts )); do
    if health_once; then
      echo "healthy"
      return 0
    fi
    if (( i < attempts )); then
      sleep 1
    fi
    i=$((i + 1))
  done

  echo "unhealthy"
  return 1
}

health_once() {
  local ok=1
  tmux has-session -t "$SESS_GW" 2>/dev/null || ok=0

  while read -r role; do
    [[ -n "$role" ]] || continue
    sess="$(worker_sess_name "$role")"
    tmux has-session -t "$sess" 2>/dev/null || ok=0
  done < <(worker_roles)

  [[ "$ok" -eq 1 ]]
}

logs() {
  echo "--- $SESS_GW ---"
  tmux capture-pane -pt "$SESS_GW" | tail -n 80 || true
  tmux list-sessions -F '#{session_name}' 2>/dev/null | grep "^${WORKER_PREFIX}" | while read -r s; do
    echo "--- $s ---"
    tmux capture-pane -pt "$s" | tail -n 40 || true
  done || true
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  status) status ;;
  health) health ;;
  logs) logs ;;
  *) echo "usage: $0 {start|stop|restart|status|health [--wait[=seconds]]|logs}"; exit 1 ;;
esac
