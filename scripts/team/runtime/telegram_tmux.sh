#!/usr/bin/env bash
set -euo pipefail

# Ensure tmux hooks can find our installed CLIs.
export PATH="$HOME/.local/bin:$PATH"

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
PACKAGE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DEFAULT_PROJECT_ROOT="$PACKAGE_ROOT"
PROJECT_ROOT="${AOE_PROJECT_ROOT:-$DEFAULT_PROJECT_ROOT}"
TEAM_DIR="${AOE_TEAM_DIR:-$PROJECT_ROOT/.aoe-team}"
SELF_SCRIPT="$SCRIPT_PATH"
ENV_FILE="$TEAM_DIR/telegram.env"
HANDLER="${AOE_WORKER_HANDLER:-$PACKAGE_ROOT/scripts/team/runtime/worker_codex_handler.sh}"
GATEWAY_BIN="$PACKAGE_ROOT/scripts/gateway/aoe-telegram-gateway.py"
SCHEDULER_BIN="$PACKAGE_ROOT/scripts/gateway/aoe-auto-scheduler.py"
BOOTSTRAP_RUNTIME="$PACKAGE_ROOT/scripts/team/bootstrap_runtime_templates.sh"

SESS_GW_PRIMARY="${AOE_TMUX_GATEWAY_SESSION:-aoe_mo_gateway}"
SESS_GW_LEGACY="${AOE_TMUX_GATEWAY_SESSION_LEGACY:-aoe_tg_gateway}"
SESS_SCHEDULER="${AOE_TMUX_SCHEDULER_SESSION:-aoe_mo_scheduler}"
WORKER_PREFIX_PRIMARY="${AOE_TMUX_WORKER_PREFIX:-aoe_tf_worker_}"
WORKER_PREFIX_LEGACY="${AOE_TMUX_WORKER_PREFIX_LEGACY:-aoe_tg_worker_}"
EPHEMERAL_WORKER_PREFIX="${AOE_TF_WORKER_SESSION_PREFIX:-tfw_}"
STATIC_WORKERS_RAW="${AOE_TMUX_STATIC_WORKERS:-0}"
case "$(printf '%s' "$STATIC_WORKERS_RAW" | tr '[:upper:]' '[:lower:]')" in
  1|true|yes|on) STATIC_WORKERS=1 ;;
  *) STATIC_WORKERS=0 ;;
esac
PAGE_SIZE_RAW="${AOE_TMUX_PAGE_SIZE:-9}"
if [[ "$PAGE_SIZE_RAW" =~ ^[0-9]+$ ]] && (( PAGE_SIZE_RAW > 0 )); then
  PAGE_SIZE="$PAGE_SIZE_RAW"
else
  PAGE_SIZE=9
fi
if (( PAGE_SIZE > 9 )); then
  PAGE_SIZE=9
fi
HINT_NAME_MAX_RAW="${AOE_TMUX_HINT_NAME_MAX:-7}"
if [[ "$HINT_NAME_MAX_RAW" =~ ^[0-9]+$ ]] && (( HINT_NAME_MAX_RAW > 1 )); then
  HINT_NAME_MAX="$HINT_NAME_MAX_RAW"
else
  HINT_NAME_MAX=7
fi
COMPACT_NAME_MAX_RAW="${AOE_TMUX_COMPACT_NAME_MAX:-20}"
if [[ "$COMPACT_NAME_MAX_RAW" =~ ^[0-9]+$ ]] && (( COMPACT_NAME_MAX_RAW > 5 )); then
  COMPACT_NAME_MAX="$COMPACT_NAME_MAX_RAW"
else
  COMPACT_NAME_MAX=20
fi
PAGE_OPTION_KEY="@aoe_team_page"
FOCUS_OPTION_KEY="@aoe_team_focus"
HINT_OPTION_KEY="@aoe_team_hints"
ORDER_MODE_RAW="${AOE_TMUX_ORDER:-project}"
case "$(printf '%s' "$ORDER_MODE_RAW" | tr '[:upper:]' '[:lower:]')" in
  type|legacy) ORDER_MODE="type" ;;
  project|group|focused|focus) ORDER_MODE="project" ;;
  *) ORDER_MODE="project" ;;
esac

SCOPE_MODE_RAW="${AOE_TMUX_SCOPE:-focus}"
case "$(printf '%s' "$SCOPE_MODE_RAW" | tr '[:upper:]' '[:lower:]')" in
  all|global) SCOPE_MODE="all" ;;
  focus|group|project|scoped) SCOPE_MODE="focus" ;;
  *) SCOPE_MODE="focus" ;;
esac

in_tmux_context() {
  [[ -n "${TMUX:-}" || -n "${TMUX_SWITCH:-}" ]]
}

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
  if [[ ! -x "$SCHEDULER_BIN" ]]; then
    echo "[ERROR] missing scheduler bin: $SCHEDULER_BIN" >&2
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
    roles = ['DataEngineer', 'Codex-Reviewer', 'Codex-Dev', 'Codex-Writer', 'Codex-Analyst']

if not roles:
    roles = ['DataEngineer', 'Codex-Reviewer', 'Codex-Dev', 'Codex-Writer', 'Codex-Analyst']

seen = set()
for r in roles:
    if r in seen:
        continue
    seen.add(r)
    print(r)
PY
}


gateway_session_names() {
  printf '%s\n' "$SESS_GW_PRIMARY"
  if [[ "$SESS_GW_LEGACY" != "$SESS_GW_PRIMARY" ]]; then
    printf '%s\n' "$SESS_GW_LEGACY"
  fi
}

worker_prefixes() {
  printf '%s\n' "$WORKER_PREFIX_PRIMARY"
  if [[ "$WORKER_PREFIX_LEGACY" != "$WORKER_PREFIX_PRIMARY" ]]; then
    printf '%s\n' "$WORKER_PREFIX_LEGACY"
  fi
}

is_gateway_session() {
  local sess="$1"
  local name
  while read -r name; do
    [[ -n "$name" ]] || continue
    if [[ "$sess" == "$name" ]]; then
      return 0
    fi
  done < <(gateway_session_names)
  return 1
}

worker_prefix_for_session() {
  local sess="$1"
  local pref
  while read -r pref; do
    [[ -n "$pref" ]] || continue
    if [[ "$sess" == "${pref}"* ]]; then
      printf '%s\n' "$pref"
      return 0
    fi
  done < <(worker_prefixes)
  return 1
}

is_worker_session() {
  worker_prefix_for_session "$1" >/dev/null 2>&1
}

is_ephemeral_worker_session() {
  local sess="$1"
  [[ -n "$EPHEMERAL_WORKER_PREFIX" && "$sess" == "${EPHEMERAL_WORKER_PREFIX}"* ]]
}

worker_sess_name_for_prefix() {
  local role="$1"
  local prefix="$2"
  local key
  key="$(printf '%s' "$role" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9]/_/g')"
  printf '%s%s\n' "$prefix" "$key"
}

worker_sess_name() {
  local role="$1"
  worker_sess_name_for_prefix "$role" "$WORKER_PREFIX_PRIMARY"
}

worker_session_names_for_role() {
  local role="$1"
  local pref
  while read -r pref; do
    [[ -n "$pref" ]] || continue
    worker_sess_name_for_prefix "$role" "$pref"
  done < <(worker_prefixes)
}

current_gateway_session() {
  local sess
  while read -r sess; do
    [[ -n "$sess" ]] || continue
    if tmux has-session -t "$sess" 2>/dev/null; then
      printf '%s\n' "$sess"
      return 0
    fi
  done < <(gateway_session_names)
  printf '%s\n' "$SESS_GW_PRIMARY"
}

gateway_session_state() {
  local sess
  while read -r sess; do
    [[ -n "$sess" ]] || continue
    if tmux has-session -t "$sess" 2>/dev/null; then
      printf 'up\n'
      return 0
    fi
  done < <(gateway_session_names)
  printf 'down\n'
}

current_worker_session_for_role() {
  local role="$1"
  local sess
  while read -r sess; do
    [[ -n "$sess" ]] || continue
    if tmux has-session -t "$sess" 2>/dev/null; then
      printf '%s\n' "$sess"
      return 0
    fi
  done < <(worker_session_names_for_role "$role")
  worker_sess_name "$role"
}

worker_session_state_for_role() {
  local role="$1"
  local sess
  while read -r sess; do
    [[ -n "$sess" ]] || continue
    if tmux has-session -t "$sess" 2>/dev/null; then
      printf 'up\n'
      return 0
    fi
  done < <(worker_session_names_for_role "$role")
  printf 'down\n'
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

session_display_name() {
  local sess="$1"
  if is_gateway_session "$sess"; then
    printf 'gateway\n'
    return 0
  fi
  if [[ "$sess" == "$SESS_SCHEDULER" ]]; then
    printf 'scheduler\n'
    return 0
  fi
  local pref
  if pref="$(worker_prefix_for_session "$sess")"; then
    printf '%s\n' "${sess#${pref}}"
    return 0
  fi
  local id8
  if id8="$(aoe_session_id8 "$sess" 2>/dev/null)"; then
    if [[ "$sess" == aoe_Orchestrator* ]]; then
      printf 'orch\n'
      return 0
    fi
    if [[ "$sess" == aoe_* ]]; then
      local name
      name="${sess#aoe_}"
      name="${name%_${id8}}"
      if [[ -n "$name" ]]; then
        printf '%s\n' "$name"
        return 0
      fi
    fi
    printf '%s\n' "$id8"
    return 0
  fi
  if [[ "$sess" == aoe_Orchestrator* ]]; then
    local name
    name="${sess#aoe_Orchestrator}"
    name="${name#_}"
    name="${name#-}"
    if [[ -n "$name" ]]; then
      printf '%s\n' "$name"
    else
      printf 'orchestrator\n'
    fi
    return 0
  fi
  if [[ "$sess" == aoe_* ]]; then
    printf '%s\n' "${sess#aoe_}"
    return 0
  fi
  printf '%s\n' "$sess"
}

trim_text() {
  local text="$1"
  local max="${2:-20}"
  if [[ ! "$max" =~ ^[0-9]+$ ]] || (( max < 2 )); then
    printf '%s\n' "$text"
    return 0
  fi
  if (( ${#text} <= max )); then
    printf '%s\n' "$text"
    return 0
  fi
  local keep=$((max - 1))
  printf '%s~\n' "${text:0:keep}"
}

trim_middle_text() {
  local text="$1"
  local max="${2:-7}"
  if [[ ! "$max" =~ ^[0-9]+$ ]] || (( max < 4 )); then
    trim_text "$text" "$max"
    return 0
  fi
  if (( ${#text} <= max )); then
    printf '%s\n' "$text"
    return 0
  fi
  local head tail
  head=$(((max - 1) / 2))
  tail=$((max - head - 1))
  if (( tail < 1 )); then
    tail=1
  fi
  printf '%s~%s\n' "${text:0:head}" "${text: -tail}"
}

hint_name_for_session() {
  local sess="$1"
  local name
  name="$(session_display_name "$sess")"
  name="$(printf '%s' "$name" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9._-]+/-/g; s/^-+//; s/-+$//; s/--+/-/g')"
  if [[ -z "$name" ]]; then
    name="na"
  fi
  trim_middle_text "$name" "$HINT_NAME_MAX"
}

strip_aoe_hints_from_status_right() {
  local fmt="${1:-}"
  if [[ -z "$fmt" ]]; then
    printf '\n'
    return 0
  fi
  # AoE adds a global "aoe-tmux-hints" prefix on status-right which conflicts
  # with our group-scoped hint bar. Keep the rest (pane title, time, etc).
  printf '%s\n' "$fmt" | sed -E 's/^#\([^)]*aoe-tmux-hints[^)]*\)//'
}

aoe_instance_meta_lines() {
  if ! command -v aoe >/dev/null 2>&1; then
    return 0
  fi

  # id8 \t project_path \t group_path
  aoe list --json --all 2>/dev/null | python3 -c '
import json
import sys

raw = sys.stdin.read().strip()
if not raw:
    raise SystemExit(0)

try:
    data = json.loads(raw)
except Exception:
    raise SystemExit(0)

if not isinstance(data, list):
    raise SystemExit(0)

for row in data:
    if not isinstance(row, dict):
        continue
    sid = str(row.get("id", "") or "").strip()
    if len(sid) < 8:
        continue
    id8 = sid[:8].lower()
    path = str(row.get("path", "") or "").strip()
    group = str(row.get("group", "") or "").strip()
    sys.stdout.write(f"{id8}\t{path}\t{group}\n")
'
}

aoe_session_id8() {
  local sess="$1"
  local token
  if [[ "$sess" =~ _([0-9a-fA-F]{8})$ ]]; then
    token="${BASH_REMATCH[1]}"
    printf '%s\n' "$(printf '%s' "$token" | tr '[:upper:]' '[:lower:]')"
    return 0
  fi
  return 1
}

session_rows() {
  local idx=1
  local seen=""
  local base=""
  local sess label state role st

  # Optional AoE meta: id8 -> project path / group path
  declare -A id8_path=()
  declare -A id8_group=()
  local id8 path group
  while IFS=$'\t' read -r id8 path group; do
    [[ -n "$id8" ]] || continue
    id8_path["$id8"]="$path"
    id8_group["$id8"]="$group"
  done < <(aoe_instance_meta_lines || true)

  local focus_key=""
  local active_sess active_id8
  active_sess="$(tmux display-message -p '#S' 2>/dev/null || true)"
  if [[ -z "$active_sess" ]]; then
    active_sess="$(tmux list-clients -F '#{client_activity} #{client_session}' 2>/dev/null | sort -nr | head -n 1 | awk '{print $2}' || true)"
  fi
  if active_id8="$(aoe_session_id8 "$active_sess" 2>/dev/null)"; then
    if [[ -n "${id8_group[$active_id8]:-}" ]]; then
      focus_key="${id8_group[$active_id8]}"
    elif [[ -n "${id8_path[$active_id8]:-}" ]]; then
      focus_key="${id8_path[$active_id8]}"
    fi
  elif [[ -n "$active_sess" ]]; then
    if is_gateway_session "$active_sess" || is_worker_session "$active_sess" || is_ephemeral_worker_session "$active_sess" || [[ "$active_sess" == "$SESS_SCHEDULER" ]]; then
      focus_key="__infra__"
    fi
  fi
  local stored_focus
  stored_focus="$(tmux show-option -gqv "$FOCUS_OPTION_KEY" 2>/dev/null || true)"
  if [[ -n "$focus_key" ]]; then
    if [[ "$focus_key" != "$stored_focus" ]]; then
      tmux set-option -gq "$FOCUS_OPTION_KEY" "$focus_key" >/dev/null 2>&1 || true
      set_current_page 1 "$focus_key"
      stored_focus="$focus_key"
    fi
  elif [[ -n "$stored_focus" ]]; then
    focus_key="$stored_focus"
  fi

  while read -r sess; do
    [[ -n "$sess" ]] || continue
    if printf '%s\n' "$seen" | grep -Fxq "$sess"; then
      continue
    fi
    base+="${sess}|OR|up"$'\n'
    seen+="${sess}"$'\n'
    idx=$((idx + 1))
  done < <(tmux list-sessions -F '#{session_name}' 2>/dev/null | grep '^aoe_Orchestrator' || true)

  state="$(gateway_session_state)"
  sess="$(current_gateway_session)"
  if ! printf '%s\n' "$seen" | grep -Fxq "$sess"; then
    base+="${sess}|GW|${state}"$'\n'
    seen+="${sess}"$'\n'
    idx=$((idx + 1))
  fi

  if [[ "$STATIC_WORKERS" == "1" ]]; then
    while read -r role; do
      [[ -n "$role" ]] || continue
      st="$(worker_session_state_for_role "$role")"
      sess="$(current_worker_session_for_role "$role")"
      label="$(role_abbr "$role")"
      if printf '%s\n' "$seen" | grep -Fxq "$sess"; then
        continue
      fi
      base+="${sess}|${label}|${st}"$'\n'
      seen+="${sess}"$'\n'
      idx=$((idx + 1))
    done < <(worker_roles)
  fi

  while read -r sess; do
    [[ -n "$sess" ]] || continue
    if printf '%s\n' "$seen" | grep -Fxq "$sess"; then
      continue
    fi
    # Hide "orphan" aoe_* sessions that are not tracked by Agent of Empires.
    # They tend to be legacy/leftover and show up as "__unknown__" buckets.
    local sid8=""
    if sid8="$(aoe_session_id8 "$sess" 2>/dev/null)"; then
      if [[ -z "${id8_path[$sid8]:-}" && -z "${id8_group[$sid8]:-}" ]]; then
        continue
      fi
    fi
    label="$(role_abbr "${sess#aoe_}")"
    base+="${sess}|${label}|up"$'\n'
    seen+="${sess}"$'\n'
    idx=$((idx + 1))
  done < <(tmux list-sessions -F '#{session_name}' 2>/dev/null | grep '^aoe_' | grep -v '^aoe_term_' || true)

  base="$(printf '%s' "$base" | sed '/^$/d')"
  if [[ -z "$base" ]]; then
    return 0
  fi

  if [[ "$ORDER_MODE" == "type" ]]; then
    idx=1
    while IFS='|' read -r sess label state; do
      [[ -n "$sess" ]] || continue
      printf '%s|%s|%s|%s\n' "$idx" "$sess" "$label" "$state"
      idx=$((idx + 1))
    done <<< "$base"
    return 0
  fi

  declare -A buckets=()
  declare -A bucket_seen=()
  local bucket_keys=()
  local key line_id8

  while IFS='|' read -r sess label state; do
    [[ -n "$sess" ]] || continue
    key=""
    if line_id8="$(aoe_session_id8 "$sess" 2>/dev/null)"; then
      if [[ -n "${id8_group[$line_id8]:-}" ]]; then
        key="${id8_group[$line_id8]}"
      elif [[ -n "${id8_path[$line_id8]:-}" ]]; then
        key="${id8_path[$line_id8]}"
      else
        key="__unknown__"
      fi
    else
      key="__infra__"
    fi

    if [[ -z "${bucket_seen[$key]+x}" ]]; then
      bucket_keys+=("$key")
      bucket_seen["$key"]=1
    fi
    buckets["$key"]+="${sess}|${label}|${state}"$'\n'
  done <<< "$base"

  if [[ "$SCOPE_MODE" == "focus" && -n "$focus_key" && -n "${buckets[$focus_key]+x}" ]]; then
    idx=1
    while IFS='|' read -r sess label state; do
      [[ -n "$sess" ]] || continue
      printf '%s|%s|%s|%s\n' "$idx" "$sess" "$label" "$state"
      idx=$((idx + 1))
    done <<< "$(printf '%s' "${buckets[$focus_key]}" | sed '/^$/d')"
    return 0
  fi

  local ordered_keys=()
  if [[ -n "$focus_key" && -n "${buckets[$focus_key]+x}" ]]; then
    ordered_keys+=("$focus_key")
  fi
  if [[ -n "${buckets[__infra__]+x}" ]]; then
    if [[ "${ordered_keys[*]:-}" != *"__infra__"* ]]; then
      ordered_keys+=("__infra__")
    fi
  fi

  local sorted_keys k
  sorted_keys="$(printf '%s\n' "${bucket_keys[@]}" | sort -u)"
  while IFS= read -r k; do
    [[ -n "$k" ]] || continue
    if [[ "$k" == "$focus_key" || "$k" == "__infra__" || "$k" == "__unknown__" ]]; then
      continue
    fi
    ordered_keys+=("$k")
  done <<< "$sorted_keys"
  if [[ -n "${buckets[__unknown__]+x}" ]]; then
    ordered_keys+=("__unknown__")
  fi

  idx=1
  for k in "${ordered_keys[@]}"; do
    while IFS='|' read -r sess label state; do
      [[ -n "$sess" ]] || continue
      printf '%s|%s|%s|%s\n' "$idx" "$sess" "$label" "$state"
      idx=$((idx + 1))
    done <<< "$(printf '%s' "${buckets[$k]}" | sed '/^$/d')"
  done
}

rows_count() {
  local rows="$1"
  local c=0
  local _line
  while IFS= read -r _line; do
    [[ -n "$_line" ]] || continue
    c=$((c + 1))
  done <<< "$rows"
  printf '%s\n' "$c"
}

total_pages_for_rows() {
  local rows="$1"
  local count total
  count="$(rows_count "$rows")"
  total=$(((count + PAGE_SIZE - 1) / PAGE_SIZE))
  if (( total < 1 )); then
    total=1
  fi
  printf '%s\n' "$total"
}

normalize_page_index() {
  local page="$1"
  local total="$2"
  local out
  if [[ "$page" =~ ^[0-9]+$ ]]; then
    out="$page"
  else
    out=1
  fi
  if (( out < 1 )); then
    out=1
  fi
  if (( out > total )); then
    out="$total"
  fi
  printf '%s\n' "$out"
}

page_option_key_for_group() {
  local key="$1"
  local norm hash
  norm="$(printf '%s' "$key" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"
  if [[ -z "$norm" ]]; then
    norm="default"
  fi
  if (( ${#norm} > 24 )); then
    hash="$(printf '%s' "$key" | sha1sum | awk '{print $1}' | cut -c1-10)"
    norm="${norm:0:12}_${hash}"
  fi
  printf '@aoe_team_page_%s\n' "$norm"
}

current_page_raw() {
  local group_key="${1:-}"
  local raw opt
  if [[ -z "$group_key" ]]; then
    group_key="$(tmux show-option -gqv "$FOCUS_OPTION_KEY" 2>/dev/null || true)"
  fi
  if [[ -z "$group_key" ]]; then
    printf '1\n'
    return 0
  fi
  opt="$(page_option_key_for_group "$group_key")"
  raw="$(tmux show-option -gqv "$opt" 2>/dev/null || true)"
  if [[ "$raw" =~ ^[0-9]+$ ]] && (( raw > 0 )); then
    printf '%s\n' "$raw"
  else
    printf '1\n'
  fi
}

set_current_page() {
  local page="$1"
  local group_key="${2:-}"
  local opt
  if [[ -z "$group_key" ]]; then
    group_key="$(tmux show-option -gqv "$FOCUS_OPTION_KEY" 2>/dev/null || true)"
  fi
  [[ -n "$group_key" ]] || return 0
  opt="$(page_option_key_for_group "$group_key")"
  tmux set-option -gq "$opt" "$page" >/dev/null 2>&1 || true
}

current_page_for_rows() {
  local rows="$1"
  local group_key="${2:-}"
  local total raw page
  total="$(total_pages_for_rows "$rows")"
  raw="$(current_page_raw "$group_key")"
  page="$(normalize_page_index "$raw" "$total")"
  if [[ "$page" != "$raw" ]]; then
    set_current_page "$page" "$group_key"
  fi
  printf '%s|%s\n' "$page" "$total"
}

page_rows_for() {
  local rows="$1"
  local page="$2"
  local start end local_idx
  local abs_idx sess label state
  start=$(((page - 1) * PAGE_SIZE + 1))
  end=$((page * PAGE_SIZE))
  while IFS='|' read -r abs_idx sess label state; do
    [[ -n "$abs_idx" && -n "$sess" ]] || continue
    if (( abs_idx < start || abs_idx > end )); then
      continue
    fi
    local_idx=$((abs_idx - start + 1))
    printf '%s|%s|%s|%s|%s\n' "$local_idx" "$abs_idx" "$sess" "$label" "$state"
  done <<< "$rows"
}

focus_display_name() {
  local key="$1"
  local out="$key"
  case "$out" in
    __infra__) out="Stack" ;;
    __unknown__) out="Unknown" ;;
  esac
  if [[ "$out" == */* ]]; then
    out="$(basename "$out")"
  fi
  out="$(trim_text "$out" 14)"
  printf '%s\n' "$out"
}

build_hint_map() {
  local page_rows="$1"
  local page="$2"
  local total="$3"
  local focus_key="${4:-}"
  local out=""
  local lidx abs_idx sess label state token state_mark
  while IFS='|' read -r lidx abs_idx sess label state; do
    [[ -n "$lidx" && -n "$sess" ]] || continue
    token="$(hint_name_for_session "$sess")"
    state_mark=""
    if [[ "$state" != "up" ]]; then
      state_mark="!"
    fi
    out+="${out:+ }[${lidx}:${token}${state_mark}]"
  done <<< "$page_rows"
  if [[ -z "$out" ]]; then
    out='[1:-]'
  fi
  local prefix=""
  if [[ -n "$focus_key" ]]; then
    prefix="[$(focus_display_name "$focus_key")] "
  fi
  printf '%sP%s/%s %s\n' "$prefix" "$page" "$total" "$out"
}

apply_alt_number_bindings() {
  local lidx
  for lidx in 1 2 3 4 5 6 7 8 9; do
    tmux unbind-key -n "M-$lidx" 2>/dev/null || true
    tmux unbind-key "$lidx" 2>/dev/null || true

    # Resolve the target dynamically at keypress time based on current page/order.
    # Use `env` to ensure the script treats this as an in-tmux switch even if
    # future tmux versions change run-shell environment behavior.
    tmux bind-key -n "M-$lidx" run-shell "env TMUX_SWITCH=1 $SELF_SCRIPT switch $lidx; true" || true
    tmux bind-key "$lidx" run-shell "env TMUX_SWITCH=1 $SELF_SCRIPT switch $lidx; true" || true
  done
}

apply_page_nav_bindings() {
  tmux unbind-key -n "M-," 2>/dev/null || true
  tmux unbind-key -n "M-." 2>/dev/null || true
  tmux bind-key -n "M-," run-shell "$SELF_SCRIPT page prev --quiet; true" || true
  tmux bind-key -n "M-." run-shell "$SELF_SCRIPT page next --quiet; true" || true
}

ensure_tmux_hooks() {
  # Auto-refresh hint bar when sessions/clients change, so new AoE sessions get
  # the same UX immediately (no manual `ui` required).
  local hook_cmd
  # Make hook refresh best-effort: never spam tmux with "returned N".
  # Keep it shell-simple (avoid redirections/||) so it behaves the same across shells.
  hook_cmd="run-shell -b \"$SELF_SCRIPT refresh --quiet; true\""
  local needle
  needle="$SELF_SCRIPT"

  local hook out line cmd
  for hook in client-attached client-session-changed session-created session-closed session-renamed; do
    out="$(tmux show-hook -g "$hook" 2>/dev/null || true)"
    # Always normalize: remove any older entries that call our refresh, then
    # append a single safe refresh hook. This prevents duplicate hooks and
    # gets rid of old ones that might still exit non-zero.
    if [[ -z "$out" || "$out" == "$hook" ]]; then
      tmux set-hook -g "$hook" "$hook_cmd" >/dev/null 2>&1 || true
      continue
    fi

    tmux set-hook -gu "$hook" >/dev/null 2>&1 || true

    local first=1
    while IFS= read -r line; do
      [[ -n "$line" ]] || continue
      # Extract the command part: "hook[idx] <cmd...>"
      cmd="$(printf '%s\n' "$line" | sed -E "s/^${hook}\\[[0-9]+\\] //")"
      [[ -n "$cmd" ]] || continue
      if printf '%s\n' "$cmd" | grep -Fq "$needle"; then
        continue
      fi
      if (( first )); then
        tmux set-hook -g "$hook" "$cmd" >/dev/null 2>&1 || true
        first=0
      else
        tmux set-hook -ag "$hook" "$cmd" >/dev/null 2>&1 || true
      fi
    done <<< "$out"

    if (( first )); then
      tmux set-hook -g "$hook" "$hook_cmd" >/dev/null 2>&1 || true
    else
      tmux set-hook -ag "$hook" "$hook_cmd" >/dev/null 2>&1 || true
    fi
  done
}

apply_visuals() {
  ensure_tmux_hooks
  apply_alt_number_bindings
  apply_page_nav_bindings

  # 2-line status: keep the original AoE(tmux) status line on the bottom row,
  # and show session hints on the top row. This avoids side panels (copy/paste).
  local top_fmt
  top_fmt="#[fg=colour45,bold]#{@aoe_team_hints}#[default] #[fg=colour244]| Alt+1-9 | Alt+,/Alt+.#[default]"

  local bottom_fmt
  bottom_fmt="$(tmux show-option -gqv "status-format[0]" 2>/dev/null || true)"
  local orig_right clean_right
  orig_right="$(tmux show-option -gqv "status-right" 2>/dev/null || true)"
  clean_right="$(strip_aoe_hints_from_status_right "$orig_right")"

  # Optional AoE meta: id8 -> project path / group name
  declare -A id8_path=()
  declare -A id8_group=()
  local id8 path group
  while IFS=$'\t' read -r id8 path group; do
    [[ -n "$id8" ]] || continue
    id8_path["$id8"]="$path"
    id8_group["$id8"]="$group"
  done < <(aoe_instance_meta_lines || true)

  # Build a stable base set of sessions we manage and their group keys.
  local base=""
  local seen=""
  local sess label state role st

  while read -r sess; do
    [[ -n "$sess" ]] || continue
    if printf '%s\n' "$seen" | grep -Fxq "$sess"; then
      continue
    fi
    base+="${sess}|OR|up"$'\n'
    seen+="${sess}"$'\n'
  done < <(tmux list-sessions -F '#{session_name}' 2>/dev/null | grep '^aoe_Orchestrator' || true)

  state="$(gateway_session_state)"
  sess="$(current_gateway_session)"
  if ! printf '%s\n' "$seen" | grep -Fxq "$sess"; then
    base+="${sess}|GW|${state}"$'\n'
    seen+="${sess}"$'\n'
  fi

  if [[ "$STATIC_WORKERS" == "1" ]]; then
    while read -r role; do
      [[ -n "$role" ]] || continue
      st="$(worker_session_state_for_role "$role")"
      sess="$(current_worker_session_for_role "$role")"
      label="$(role_abbr "$role")"
      if printf '%s\n' "$seen" | grep -Fxq "$sess"; then
        continue
      fi
      base+="${sess}|${label}|${st}"$'\n'
      seen+="${sess}"$'\n'
    done < <(worker_roles)
  fi

  while read -r sess; do
    [[ -n "$sess" ]] || continue
    if printf '%s\n' "$seen" | grep -Fxq "$sess"; then
      continue
    fi
    local sid8=""
    if sid8="$(aoe_session_id8 "$sess" 2>/dev/null)"; then
      if [[ -z "${id8_path[$sid8]:-}" && -z "${id8_group[$sid8]:-}" ]]; then
        continue
      fi
    fi
    label="$(role_abbr "${sess#aoe_}")"
    base+="${sess}|${label}|up"$'\n'
    seen+="${sess}"$'\n'
  done < <(tmux list-sessions -F '#{session_name}' 2>/dev/null | grep '^aoe_' | grep -v '^aoe_term_' || true)

  base="$(printf '%s' "$base" | sed '/^$/d')"
  if [[ -z "$base" ]]; then
    return 0
  fi

  declare -A buckets=()
  declare -A bucket_seen=()
  local bucket_keys=()
  local key line_id8
  while IFS='|' read -r sess label state; do
    [[ -n "$sess" ]] || continue
    if line_id8="$(aoe_session_id8 "$sess" 2>/dev/null)"; then
      if [[ -n "${id8_group[$line_id8]:-}" ]]; then
        key="${id8_group[$line_id8]}"
      elif [[ -n "${id8_path[$line_id8]:-}" ]]; then
        key="${id8_path[$line_id8]}"
      else
        key="__unknown__"
      fi
    else
      key="__infra__"
    fi
    if [[ -z "${bucket_seen[$key]+x}" ]]; then
      bucket_keys+=("$key")
      bucket_seen["$key"]=1
    fi
    buckets["$key"]+="${sess}|${label}|${state}"$'\n'
  done <<< "$base"

  local k rows page_info page total page_rows hints idx
  for k in "${bucket_keys[@]}"; do
    rows=""
    idx=1
    while IFS='|' read -r sess label state; do
      [[ -n "$sess" ]] || continue
      rows+="${idx}|${sess}|${label}|${state}"$'\n'
      idx=$((idx + 1))
    done <<< "$(printf '%s' "${buckets[$k]}" | sed '/^$/d')"
    rows="$(printf '%s' "$rows" | sed '/^$/d')"
    [[ -n "$rows" ]] || continue

    page_info="$(current_page_for_rows "$rows" "$k")"
    page="${page_info%%|*}"
    total="${page_info##*|}"
    page_rows="$(page_rows_for "$rows" "$page")"
    hints="$(build_hint_map "$page_rows" "$page" "$total" "$k")"

    while IFS='|' read -r idx sess label state; do
      [[ -n "$sess" ]] || continue
      if [[ "$state" != "up" ]]; then
        continue
      fi
      # Sessions can disappear while a hook-driven refresh is running.
      tmux has-session -t "$sess" 2>/dev/null || continue
      tmux set-option -t "$sess" -q "$HINT_OPTION_KEY" "$hints" >/dev/null 2>&1 || true

      if [[ -n "$clean_right" ]]; then
        tmux set-option -t "$sess" -q status-right "$clean_right" >/dev/null 2>&1 || true
      fi

      tmux set-option -t "$sess" status 2 >/dev/null 2>&1 || true
      tmux set-option -t "$sess" "status-format[0]" "$top_fmt" >/dev/null 2>&1 || true
      if [[ -n "$bottom_fmt" ]]; then
        tmux set-option -t "$sess" "status-format[1]" "$bottom_fmt" >/dev/null 2>&1 || true
      fi

      # Cleanup legacy state from older versions (no longer used).
      tmux set-option -t "$sess" -u @aoe_team_orig_status_format0 >/dev/null 2>&1 || true
      tmux set-option -t "$sess" -u @aoe_team_orig_status_right >/dev/null 2>&1 || true
    done <<< "$rows"
  done
}

overview_once() {
  local compact="${1:-0}"
  local rows page_info page total page_rows active_sess count
  local lidx abs_idx sess label state
  rows="$(session_rows)"
  page_info="$(current_page_for_rows "$rows")"
  page="${page_info%%|*}"
  total="${page_info##*|}"
  page_rows="$(page_rows_for "$rows" "$page")"
  count="$(rows_count "$rows")"
  if [[ "$compact" == "1" ]]; then
    local mark st_short disp
    active_sess="$(tmux display-message -p '#S' 2>/dev/null || true)"
    printf 'TEAM P%s/%s\n' "$page" "$total"
    printf '\n'
    while IFS='|' read -r lidx abs_idx sess label state; do
      [[ -n "$sess" ]] || continue
      mark=" "
      if [[ "$sess" == "$active_sess" ]]; then
        mark=">"
      fi
      st_short="-"
      if [[ "$state" == "up" ]]; then
        st_short="+"
      fi
      disp="$(trim_text "$(session_display_name "$sess")" "$COMPACT_NAME_MAX")"
      printf '%s%s(%s) %-2s %-3s %s\n' "$mark" "$lidx" "$abs_idx" "$st_short" "$label" "$disp"
    done <<< "$page_rows"
    if [[ -z "$page_rows" ]]; then
      printf '(no sessions)\n'
    fi
    printf '\n'
    printf 'Alt+<idx>(page) / C-b <idx>\n'
    printf 'Alt+, prev | Alt+. next\n'
    return 0
  fi

  if [[ "$compact" != "1" ]]; then
    printf 'AOE tmux overview\n'
    printf 'project: %s\n' "$PROJECT_ROOT"
    printf 'page: %s/%s (total sessions: %s)\n' "$page" "$total" "$count"
    printf '\n'
  fi
  printf '%-4s %-5s %-10s %-6s %-30s\n' "idx" "abs" "key" "state" "session"
  printf '%-4s %-5s %-10s %-6s %-30s\n' "---" "-----" "---------" "-----" "------------------------------"
  while IFS='|' read -r lidx abs_idx sess label state; do
    [[ -n "$sess" ]] || continue
    printf '%-4s %-5s %-10s %-6s %-30s  (%s)\n' "$lidx" "$abs_idx" "M-$lidx/$lidx" "$state" "$sess" "$label"
  done <<< "$page_rows"
  if [[ -z "$page_rows" ]]; then
    printf '(no sessions)\n'
  fi
  if [[ "$compact" != "1" ]]; then
    printf '\n'
    printf 'shortcut: Alt+<idx> or Prefix(C-b)+<idx>\n'
    printf 'page: Alt+, (prev), Alt+. (next)\n'
    printf 'page cmd: %s page <next|prev|set N|status|reset>\n' "$SELF_SCRIPT"
    printf 'switch: %s switch <idx|session>\n' "$SELF_SCRIPT"
  fi
}

overview() {
  local watch=0
  local compact=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --watch) watch=1 ;;
      --compact) compact=1 ;;
      *)
        echo "usage: $SELF_SCRIPT overview [--watch] [--compact]" >&2
        return 2
        ;;
    esac
    shift
  done

  if [[ "$watch" == "1" ]]; then
    while true; do
      clear
      overview_once "$compact"
      sleep 1
    done
  fi
  overview_once "$compact"
}

resolve_session_target() {
  local token="$1"
  local rows idx sess label state page_info page total page_rows lidx abs_idx
  rows="$(session_rows)"

  if [[ "$token" =~ ^[0-9]+$ ]]; then
    if (( token >= 1 && token <= PAGE_SIZE )); then
      page_info="$(current_page_for_rows "$rows")"
      page="${page_info%%|*}"
      total="${page_info##*|}"
      page_rows="$(page_rows_for "$rows" "$page")"
      while IFS='|' read -r lidx abs_idx sess label state; do
        if [[ "$lidx" == "$token" ]]; then
          printf '%s\n' "$sess"
          return 0
        fi
      done <<< "$page_rows"
    fi
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
    if in_tmux_context; then
      tmux display-message "unknown target: $token"
      return 0
    else
      echo "[ERROR] unknown target: $token" >&2
      overview_once >&2
    fi
    return 1
  fi

  if ! tmux has-session -t "$target" 2>/dev/null; then
    if in_tmux_context; then
      tmux display-message "missing session: $target"
      return 0
    else
      echo "[ERROR] session not running: $target" >&2
    fi
    return 1
  fi

  if in_tmux_context; then
    # Switching is invoked by tmux keybindings; never let internal failures
    # spam "returned N" messages in the status line.
    set +e
    tmux switch-client -t "$target" 2>/dev/null
    if [[ $? -ne 0 ]]; then
      tmux display-message "switch failed: $target"
    fi
    apply_visuals >/dev/null 2>&1
    set -e
    return 0
  fi

  tmux attach-session -t "$target"
}

page_control() {
  local action="${1:-status}"
  shift || true
  local target=""
  local quiet=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --quiet) quiet=1 ;;
      *)
        if [[ -z "$target" ]]; then
          target="$1"
        else
          if in_tmux_context; then
            tmux display-message "usage: page next|prev|set N|status|reset"
            return 0
          fi
          echo "usage: $SELF_SCRIPT page [next|prev|set <N>|status|reset] [--quiet]" >&2
          return 2
        fi
        ;;
    esac
    shift
  done

  if [[ "$action" =~ ^[0-9]+$ ]]; then
    target="$action"
    action="set"
  fi

  local rows total current new_page
  rows="$(session_rows)"
  total="$(total_pages_for_rows "$rows")"
  current="$(normalize_page_index "$(current_page_raw)" "$total")"

  case "$action" in
    status) new_page="$current" ;;
    next)
      new_page=$((current + 1))
      if (( new_page > total )); then
        new_page=1
      fi
      ;;
    prev)
      new_page=$((current - 1))
      if (( new_page < 1 )); then
        new_page="$total"
      fi
      ;;
    reset) new_page=1 ;;
    set)
      if [[ -z "$target" || ! "$target" =~ ^[0-9]+$ ]]; then
        if in_tmux_context; then
          tmux display-message "usage: page set <N>"
          return 0
        fi
        echo "usage: $SELF_SCRIPT page set <N>" >&2
        return 2
      fi
      new_page="$(normalize_page_index "$target" "$total")"
      ;;
    *)
      if in_tmux_context; then
        tmux display-message "usage: page next|prev|set N|status|reset"
        return 0
      fi
      echo "usage: $SELF_SCRIPT page [next|prev|set <N>|status|reset] [--quiet]" >&2
      return 2
      ;;
  esac

  if in_tmux_context; then
    set +e
    set_current_page "$new_page"
    apply_visuals >/dev/null 2>&1
    set -e
    if [[ "$quiet" != "1" ]]; then
      tmux display-message "page: $new_page/$total"
    fi
    return 0
  fi

  set_current_page "$new_page"
  apply_visuals

  if [[ "$quiet" != "1" ]]; then
    echo "page: $new_page/$total"
  fi
}

ui() {
  apply_visuals
  overview_once
}

stop_workers() {
  local s
  while read -r s; do
    [[ -n "$s" ]] || continue
    if is_worker_session "$s" || is_ephemeral_worker_session "$s"; then
      tmux kill-session -t "$s" 2>/dev/null || true
    fi
  done < <(tmux list-sessions -F '#{session_name}' 2>/dev/null || true)
}

stop_gateways() {
  local s
  while read -r s; do
    [[ -n "$s" ]] || continue
    tmux kill-session -t "$s" 2>/dev/null || true
  done < <(gateway_session_names)
}

stop_scheduler() {
  tmux kill-session -t "$SESS_SCHEDULER" 2>/dev/null || true
}

start_scheduler() {
  if tmux has-session -t "$SESS_SCHEDULER" 2>/dev/null; then
    echo "scheduler already running: $SESS_SCHEDULER"
    return 0
  fi
  tmux new-session -d -s "$SESS_SCHEDULER" -c "$PROJECT_ROOT" \
    "bash -lc 'set -a; . \"$ENV_FILE\"; set +a; exec \"$SCHEDULER_BIN\" --project-root \"$PROJECT_ROOT\" --team-dir \"$TEAM_DIR\"'"
  echo "scheduler started: $SESS_SCHEDULER"
}

scheduler_status() {
  if tmux has-session -t "$SESS_SCHEDULER" 2>/dev/null; then
    echo "scheduler: up ($SESS_SCHEDULER)"
  else
    echo "scheduler: down ($SESS_SCHEDULER)"
  fi
}

start() {
  local role sess
  prepare_runtime
  preflight || return 1

  stop_gateways
  stop_scheduler
  stop_workers

  if [[ "$STATIC_WORKERS" == "1" ]]; then
    while read -r role; do
      [[ -n "$role" ]] || continue
      sess="$(worker_sess_name "$role")"
      tmux new-session -d -s "$sess" -c "$PROJECT_ROOT" \
        "bash -lc 'set -a; . \"$ENV_FILE\"; set +a; exec /home/kimyoungjin06/.local/bin/aoe-orch worker --project-root \"$PROJECT_ROOT\" --for \"$role\" --handler-cmd \"$HANDLER\" --interval-sec 1 --quiet'"
    done < <(worker_roles)
  fi

  tmux new-session -d -s "$SESS_GW_PRIMARY" -c "$PROJECT_ROOT" \
    "bash -lc 'set -a; . \"$ENV_FILE\"; set +a; exec \"$GATEWAY_BIN\" --project-root \"$PROJECT_ROOT\" --verbose'"

  apply_visuals
  echo "started"
}

stop() {
  stop_gateways
  stop_scheduler
  stop_workers
  echo "stopped"
}

status() {
  local s
  while read -r s; do
    [[ -n "$s" ]] || continue
    if is_gateway_session "$s" || is_worker_session "$s"; then
      echo "$s"
    fi
  done < <(tmux list-sessions -F '#{session_name}' 2>/dev/null || true)
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
  local last_reasons=""
  while (( i <= attempts )); do
    if last_reasons="$(health_once)"; then
      echo "healthy"
      return 0
    fi
    if (( i < attempts )); then
      sleep 1
    fi
    i=$((i + 1))
  done

  echo "unhealthy"
  if [[ -n "$last_reasons" ]]; then
    printf '%s\n' "$last_reasons"
  fi
  return 1
}

health_once() {
  local -a reasons=()

  if ! command -v tmux >/dev/null 2>&1; then
    reasons+=("E_HEALTH_NO_TMUX")
    printf '%s\n' "${reasons[@]}"
    return 1
  fi

  if ! tmux list-sessions >/dev/null 2>&1; then
    # Covers "no server running" and any inability to query tmux.
    reasons+=("E_HEALTH_TMUX_SERVER_DOWN")
  fi

  if [[ ! -f "$ENV_FILE" ]]; then
    reasons+=("E_HEALTH_ENV_MISSING path=$ENV_FILE")
  fi
  if [[ ! -x "$HANDLER" ]]; then
    reasons+=("E_HEALTH_HANDLER_MISSING path=$HANDLER")
  fi
  if [[ ! -f "$GATEWAY_BIN" ]]; then
    reasons+=("E_HEALTH_GATEWAY_BIN_MISSING path=$GATEWAY_BIN")
  fi

  if [[ "$(gateway_session_state)" != "up" ]]; then
    local expected=""
    local sess=""
    while read -r sess; do
      [[ -n "$sess" ]] || continue
      if [[ -n "$expected" ]]; then
        expected+=","
      fi
      expected+="$sess"
    done < <(gateway_session_names)
    reasons+=("E_HEALTH_GATEWAY_DOWN expected=$expected")
  fi

  if [[ "$STATIC_WORKERS" == "1" ]]; then
    local role=""
    while read -r role; do
      [[ -n "$role" ]] || continue
      if [[ "$(worker_session_state_for_role "$role")" != "up" ]]; then
        local expected=""
        local sess=""
        while read -r sess; do
          [[ -n "$sess" ]] || continue
          if [[ -n "$expected" ]]; then
            expected+=","
          fi
          expected+="$sess"
        done < <(worker_session_names_for_role "$role")
        reasons+=("E_HEALTH_WORKER_DOWN role=$role expected=$expected")
      fi
    done < <(worker_roles)
  fi

  if (( ${#reasons[@]} > 0 )); then
    printf '%s\n' "${reasons[@]}"
    return 1
  fi
  return 0
}

logs() {
  local gw
  gw="$(current_gateway_session)"
  if tmux has-session -t "$gw" 2>/dev/null; then
    echo "--- $gw ---"
    tmux capture-pane -pt "$gw" | tail -n 80 || true
  fi

  local s
  while read -r s; do
    [[ -n "$s" ]] || continue
    if is_worker_session "$s" || is_ephemeral_worker_session "$s"; then
      echo "--- $s ---"
      tmux capture-pane -pt "$s" | tail -n 40 || true
    fi
  done < <(tmux list-sessions -F '#{session_name}' 2>/dev/null || true)
}

case "${1:-}" in
  init) prepare_runtime; echo "runtime initialized" ;;
  start) start ;;
  stop) stop ;;
  restart) stop; start ;;
  ui) ui ;;
  refresh)
    shift || true
    # refresh is often invoked from tmux hooks; treat as best-effort and never
    # fail the tmux hook command (to avoid "returned 1" messages).
    set +e
    set +u
    set +o pipefail
    apply_visuals >/dev/null 2>&1 || true
    exit 0
    ;;
  overview) shift; overview "$@" ;;
  switch) shift; switch_session "${1:-}" ;;
  page) shift; page_control "${1:-status}" "$@" ;;
  auto)
    shift
    case "${1:-status}" in
      on|start) preflight || exit 1; start_scheduler; apply_visuals ;;
      off|stop) stop_scheduler; echo "scheduler stopped: $SESS_SCHEDULER" ;;
      status) scheduler_status ;;
      *) echo "usage: $SELF_SCRIPT auto {on|off|status}"; exit 2 ;;
    esac
    ;;
  status) status ;;
  health) health ;;
  logs) logs ;;
  *) echo "usage: $SELF_SCRIPT {init|start|stop|restart|ui|refresh|overview [--watch] [--compact]|switch <idx|session>|page [next|prev|set <N>|status|reset] [--quiet]|auto {on|off|status}|status|health [--wait[=seconds]]|logs}"; exit 1 ;;
esac
