#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
TEAM_DIR="$PROJECT_ROOT/.aoe-team"
SELF_SCRIPT="$SCRIPT_DIR/telegram_tmux.sh"
ENV_FILE="$TEAM_DIR/telegram.env"
HANDLER="$TEAM_DIR/worker_codex_handler.sh"
GATEWAY_BIN="$PROJECT_ROOT/scripts/gateway/aoe-telegram-gateway.py"
BOOTSTRAP_RUNTIME="$PROJECT_ROOT/scripts/team/bootstrap_runtime_templates.sh"

SESS_GW="aoe_tg_gateway"
WORKER_PREFIX="aoe_tg_worker_"
PANEL_TITLE="AOE_PANEL"

prepare_runtime() {
  if [[ -x "$BOOTSTRAP_RUNTIME" ]]; then
    bash "$BOOTSTRAP_RUNTIME" --project-root "$PROJECT_ROOT" --team-dir "$TEAM_DIR" >/dev/null
  fi
}

preflight() {
  if ! command -v tmux >/dev/null 2>&1; then
    echo "[ERROR] tmux not found in PATH" >&2
    return 1
  fi
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "[ERROR] missing env file: $ENV_FILE" >&2
    echo "[HINT] create it from your secure runtime settings, then retry start." >&2
    return 1
  fi
  if [[ ! -x "$HANDLER" ]]; then
    echo "[ERROR] missing worker handler: $HANDLER" >&2
    return 1
  fi
  if [[ ! -f "$GATEWAY_BIN" ]]; then
    echo "[ERROR] missing gateway bin: $GATEWAY_BIN" >&2
    return 1
  fi
}

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

role_abbr() {
  local role="$1"
  local out
  out="$(printf '%s' "$role" | sed -E 's/[^A-Za-z0-9]+/ /g; s/([A-Z])[a-z]*/\1/g' | tr -d ' ')"
  out="$(printf '%s' "$out" | tr '[:lower:]' '[:upper:]')"
  if (( ${#out} < 2 )); then
    out="$(printf '%s' "$role" | tr -cd '[:alnum:]' | cut -c1-2 | tr '[:lower:]' '[:upper:]')"
  fi
  if [[ -z "$out" ]]; then
    out="$(printf '%s' "$role" | tr -cd '[:alnum:]' | cut -c1-2 | tr '[:lower:]' '[:upper:]')"
  fi
  if [[ -z "$out" ]]; then
    out="WK"
  fi
  printf '%s\n' "$out"
}

session_rows() {
  local idx=1
  local state="down"
  if tmux has-session -t "$SESS_GW" 2>/dev/null; then
    state="up"
  fi
  printf '%s|%s|%s|%s\n' "$idx" "$SESS_GW" "GW" "$state"
  idx=$((idx + 1))

  while read -r role; do
    [[ -n "$role" ]] || continue
    local sess
    local label
    local st="down"
    sess="$(worker_sess_name "$role")"
    label="$(role_abbr "$role")"
    if tmux has-session -t "$sess" 2>/dev/null; then
      st="up"
    fi
    printf '%s|%s|%s|%s\n' "$idx" "$sess" "$label" "$st"
    idx=$((idx + 1))
  done < <(worker_roles)
}

build_hint_map() {
  local rows="$1"
  local out=""
  local idx sess label state
  while IFS='|' read -r idx sess label state; do
    [[ -n "$idx" && -n "$sess" ]] || continue
    if (( idx > 9 )); then
      continue
    fi
    out+="${out:+ }[M-${idx}:${label}]"
  done <<< "$rows"
  printf '%s\n' "${out:-[M-1:GW]}"
}

apply_alt_number_bindings() {
  local rows="$1"
  local idx sess label state

  for idx in 1 2 3 4 5 6 7 8 9; do
    tmux unbind-key -n "M-$idx" 2>/dev/null || true
  done

  while IFS='|' read -r idx sess label state; do
    [[ -n "$idx" && -n "$sess" ]] || continue
    if (( idx > 9 )); then
      continue
    fi
    tmux bind-key -n "M-$idx" if-shell \
      "tmux has-session -t '$sess' 2>/dev/null" \
      "switch-client -t '$sess'" \
      "display-message 'missing session: $sess'"
  done <<< "$rows"
}

apply_visuals() {
  local rows hints
  rows="$(session_rows)"
  hints="$(build_hint_map "$rows")"

  apply_alt_number_bindings "$rows"

  local idx sess label state
  while IFS='|' read -r idx sess label state; do
    [[ -n "$sess" ]] || continue
    if [[ "$state" != "up" ]]; then
      continue
    fi
    tmux set-option -t "$sess" status-left "#[fg=colour40,bold]#{session_name}#[default] "
    tmux set-option -t "$sess" status-right "#[fg=colour45]AOE#[default] $hints #[fg=colour244]| %Y-%m-%d %H:%M"
  done <<< "$rows"
}

overview_once() {
  local rows idx sess label state
  rows="$(session_rows)"
  printf 'AOE tmux overview\n'
  printf 'project: %s\n' "$PROJECT_ROOT"
  printf '\n'
  printf '%-4s %-6s %-6s %-30s\n' "idx" "key" "state" "session"
  printf '%-4s %-6s %-6s %-30s\n' "---" "---" "-----" "------------------------------"
  while IFS='|' read -r idx sess label state; do
    [[ -n "$sess" ]] || continue
    printf '%-4s %-6s %-6s %-30s  (%s)\n' "$idx" "M-$idx" "$state" "$sess" "$label"
  done <<< "$rows"
  printf '\n'
  printf 'switch: %s switch <idx|session>\n' "$SELF_SCRIPT"
  printf 'panel : %s panel   (toggle right-side live panel)\n' "$SELF_SCRIPT"
}

overview() {
  if [[ "${1:-}" == "--watch" ]]; then
    while true; do
      clear
      overview_once
      sleep 1
    done
  fi
  overview_once
}

resolve_session_target() {
  local token="$1"
  local rows idx sess label state
  rows="$(session_rows)"

  if [[ "$token" =~ ^[0-9]+$ ]]; then
    while IFS='|' read -r idx sess label state; do
      if [[ "$idx" == "$token" ]]; then
        printf '%s\n' "$sess"
        return 0
      fi
    done <<< "$rows"
    return 1
  fi

  token="$(printf '%s' "$token" | tr '[:upper:]' '[:lower:]')"
  while IFS='|' read -r idx sess label state; do
    local sess_l label_l
    sess_l="$(printf '%s' "$sess" | tr '[:upper:]' '[:lower:]')"
    label_l="$(printf '%s' "$label" | tr '[:upper:]' '[:lower:]')"
    if [[ "$token" == "$sess_l" || "$token" == "$label_l" ]]; then
      printf '%s\n' "$sess"
      return 0
    fi
  done <<< "$rows"

  return 1
}

switch_session() {
  local token="${1:-}"
  if [[ -z "$token" ]]; then
    echo "usage: $SELF_SCRIPT switch <idx|session>" >&2
    return 2
  fi

  local target
  if ! target="$(resolve_session_target "$token")"; then
    echo "[ERROR] unknown target: $token" >&2
    overview_once >&2
    return 1
  fi

  if ! tmux has-session -t "$target" 2>/dev/null; then
    echo "[ERROR] session not running: $target" >&2
    return 1
  fi

  if [[ -n "${TMUX:-}" ]]; then
    tmux switch-client -t "$target"
  else
    tmux attach-session -t "$target"
  fi
}

panel() {
  if [[ -z "${TMUX:-}" ]]; then
    echo "[ERROR] panel toggle works only inside tmux client." >&2
    echo "[HINT] attach first, then run: $SELF_SCRIPT panel" >&2
    return 1
  fi

  local existing
  existing="$(tmux list-panes -F '#{pane_id} #{pane_title}' | awk -v t="$PANEL_TITLE" '$2==t {print $1; exit}')"
  if [[ -n "$existing" ]]; then
    tmux kill-pane -t "$existing"
    echo "panel closed"
    return 0
  fi

  local new_pane cmd
  cmd="bash -lc '$SELF_SCRIPT overview --watch'"
  new_pane="$(tmux split-window -dPF '#{pane_id}' -h -l 42 -c "$PROJECT_ROOT" "$cmd")"
  tmux select-pane -t "$new_pane" -T "$PANEL_TITLE" >/dev/null 2>&1 || true
  echo "panel opened"
}

ui() {
  apply_visuals
  overview_once
}

stop_workers() {
  tmux list-sessions -F '#{session_name}' 2>/dev/null | grep "^${WORKER_PREFIX}" | while read -r s; do
    tmux kill-session -t "$s" 2>/dev/null || true
  done || true
}

start() {
  prepare_runtime
  preflight || return 1

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

  apply_visuals
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
  init) prepare_runtime; echo "runtime initialized" ;;
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  ui) ui ;;
  overview) shift; overview "${1:-}" ;;
  switch) shift; switch_session "${1:-}" ;;
  panel) panel ;;
  status) status ;;
  health) health ;;
  logs) logs ;;
  *) echo "usage: $SELF_SCRIPT {init|start|stop|restart|ui|overview [--watch]|switch <idx|session>|panel|status|health [--wait[=seconds]]|logs}"; exit 1 ;;
esac
