#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control"
TEAM_DIR="$PROJECT_ROOT/.aoe-team"
ENV_FILE="$TEAM_DIR/telegram.env"

GW_PID_FILE="$TEAM_DIR/telegram_gateway.pid"
DE_PID_FILE="$TEAM_DIR/worker_dataengineer.pid"
RV_PID_FILE="$TEAM_DIR/worker_reviewer.pid"
LOCK_FILE="$TEAM_DIR/.gateway.instance.lock"

find_pids() {
  local pattern="$1"
  pgrep -f "$pattern" 2>/dev/null || true
}

kill_pid_list() {
  local pids=("$@")
  local pid
  for pid in "${pids[@]}"; do
    [[ -n "${pid:-}" ]] || continue
    if ps -p "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
}

stop_by_pattern() {
  local pattern="$1"
  mapfile -t pids < <(find_pids "$pattern")
  if [[ "${#pids[@]}" -gt 0 ]]; then
    kill_pid_list "${pids[@]}"
  fi
}

start() {
  set -a
  . "$ENV_FILE"
  set +a

  # Avoid duplicate processes if pid files got out-of-sync (ex: stale pid files + live lock).
  if [[ -n "$(find_pids "aoe-telegram-gateway.*--project-root $PROJECT_ROOT")" ]]; then
    echo "already running (gateway)"
    return 0
  fi

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
  # Best-effort cleanup for cases where pid files got stale.
  stop_by_pattern "aoe-telegram-gateway.*--project-root $PROJECT_ROOT"
  stop_by_pattern "aoe-orch worker.*--project-root $PROJECT_ROOT.*--for DataEngineer"
  stop_by_pattern "aoe-orch worker.*--project-root $PROJECT_ROOT.*--for Reviewer"
  rm -f "$LOCK_FILE" >/dev/null 2>&1 || true
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
