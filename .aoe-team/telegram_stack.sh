#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control"
TEAM_DIR="$PROJECT_ROOT/.aoe-team"
ENV_FILE="$TEAM_DIR/telegram.env"

GW_PID_FILE="$TEAM_DIR/telegram_gateway.pid"
DE_PID_FILE="$TEAM_DIR/worker_dataengineer.pid"
RV_PID_FILE="$TEAM_DIR/worker_reviewer.pid"

start() {
  set -a
  . "$ENV_FILE"
  set +a

  nohup /home/kimyoungjin06/.local/bin/aoe-orch worker --project-root "$PROJECT_ROOT" --for DataEngineer --handler-cmd 'printf "[%s] %s" "$AOE_WORKER_ACTOR" "$AOE_MSG_TITLE"' --interval-sec 1 --quiet > "$TEAM_DIR/worker_dataengineer.log" 2>&1 &
  echo $! > "$DE_PID_FILE"

  nohup /home/kimyoungjin06/.local/bin/aoe-orch worker --project-root "$PROJECT_ROOT" --for Reviewer --handler-cmd 'printf "[%s] %s" "$AOE_WORKER_ACTOR" "$AOE_MSG_TITLE"' --interval-sec 1 --quiet > "$TEAM_DIR/worker_reviewer.log" 2>&1 &
  echo $! > "$RV_PID_FILE"

  nohup /home/kimyoungjin06/.local/bin/aoe-telegram-gateway --project-root "$PROJECT_ROOT" --verbose > "$TEAM_DIR/telegram_gateway.log" 2>&1 &
  echo $! > "$GW_PID_FILE"

  echo "started"
}

stop_one() {
  local f="$1"
  if [[ -f "$f" ]]; then
    local pid
    pid="$(cat "$f" || true)"
    if [[ -n "${pid:-}" ]] && ps -p "$pid" >/dev/null 2>&1; then
      kill "$pid" || true
    fi
    rm -f "$f"
  fi
}

stop() {
  stop_one "$GW_PID_FILE"
  stop_one "$DE_PID_FILE"
  stop_one "$RV_PID_FILE"
  echo "stopped"
}

status() {
  for f in "$GW_PID_FILE" "$DE_PID_FILE" "$RV_PID_FILE"; do
    if [[ -f "$f" ]]; then
      pid="$(cat "$f" || true)"
      if [[ -n "${pid:-}" ]] && ps -p "$pid" >/dev/null 2>&1; then
        echo "$f: running pid=$pid"
      else
        echo "$f: stale"
      fi
    else
      echo "$f: missing"
    fi
  done
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  restart) stop; start ;;
  *) echo "usage: $0 {start|stop|status|restart}"; exit 1 ;;
esac
