#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="$(readlink -f "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "$SCRIPT_PATH")" && pwd)"
ROLE="${AOE_WORKER_ACTOR:-Worker}"
ROLE_SAFE="$(printf '%s' "$ROLE" | tr -c 'A-Za-z0-9._-' '_')"
PACKAGE_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
DEFAULT_PROJECT_ROOT="$PACKAGE_ROOT"
PROJECT_ROOT="${AOE_PROJECT_ROOT:-$DEFAULT_PROJECT_ROOT}"
TEAM_DIR="${AOE_TEAM_DIR:-$PROJECT_ROOT/.aoe-team}"
LOG_DIR="$TEAM_DIR/logs"
mkdir -p "$LOG_DIR"

worker_runtime_meta() {
  TEAM_DIR="$TEAM_DIR" ROLE="$ROLE" AOE_WORKER_PROVIDER="${AOE_WORKER_PROVIDER:-}" AOE_WORKER_LAUNCH="${AOE_WORKER_LAUNCH:-}" python3 - <<'PY'
import json
import os
from pathlib import Path

team_dir = Path(os.environ.get("TEAM_DIR", ".")).expanduser().resolve()
role = str(os.environ.get("ROLE", "")).strip()
provider_override = str(os.environ.get("AOE_WORKER_PROVIDER", "")).strip()
launch_override = str(os.environ.get("AOE_WORKER_LAUNCH", "")).strip()
provider = provider_override
launch = launch_override

cfg = team_dir / "orchestrator.json"
provider_commands = {}
row = {}
try:
    data = json.loads(cfg.read_text(encoding="utf-8"))
    if isinstance(data, dict):
        raw_cmds = data.get("provider_commands")
        if isinstance(raw_cmds, dict):
            provider_commands = {
                str(k).strip().casefold(): str(v).strip()
                for k, v in raw_cmds.items()
                if str(k).strip() and str(v).strip()
            }
        candidates = []
        coordinator = data.get("coordinator")
        if isinstance(coordinator, dict):
            candidates.append(coordinator)
        agents = data.get("agents")
        if isinstance(agents, list):
            candidates.extend(item for item in agents if isinstance(item, dict))
        role_key = role.casefold()
        for item in candidates:
            item_role = str(item.get("role", "")).strip()
            if item_role and item_role.casefold() == role_key:
                row = item
                break
except Exception:
    row = {}

if not provider:
    provider = str(row.get("provider", "")).strip()
if not provider:
    provider = "codex"

if not launch:
    launch = str(row.get("launch", "")).strip()
if not launch:
    launch = provider_commands.get(provider.casefold(), "")
if not launch:
    launch = provider

print(provider)
print(launch)
PY
}

shell_join() {
  local out="" arg
  for arg in "$@"; do
    out+="${out:+ }$(printf '%q' "$arg")"
  done
  printf '%s\n' "$out"
}

build_launch_parts() {
  local launch="$1"
  local -n target_ref="$2"
  target_ref=()
  if [[ -n "$launch" ]]; then
    # launch is a simple CLI command string from orchestrator.json (for example "claude" or "codex").
    read -r -a target_ref <<<"$launch"
  fi
}

append_env_exports() {
  local -n target_ref="$1"
  shift
  local key value
  for key in "$@"; do
    value="${!key-}"
    if [[ -n "$value" ]]; then
      target_ref+=("$key=$value")
    fi
  done
}

is_rate_limited_text() {
  local text="${1:-}"
  printf '%s' "$text" | grep -Eiq 'rate[[:space:]_-]*limit|429|too[[:space:]]+many[[:space:]]+requests|retry[[:space:]_-]*after|quota|overloaded|capacity'
}

run_codex_fallback_for_claude_limit() {
  local allow="${AOE_CLAUDE_FALLBACK_TO_CODEX:-1}"
  local normalized
  normalized="$(printf '%s' "$allow" | tr '[:upper:]' '[:lower:]')"
  if [[ "$normalized" == "0" || "$normalized" == "false" || "$normalized" == "no" || "$normalized" == "off" ]]; then
    return 1
  fi

  local fb_tool_path
  if ! fb_tool_path="$(command -v codex)"; then
    return 1
  fi

  local fb_perm_mode fb_run_as_root
  fb_perm_mode="$(printf '%s' "${AOE_CODEX_PERMISSION_MODE:-full}" | tr '[:upper:]' '[:lower:]')"
  fb_run_as_root="$(printf '%s' "${AOE_CODEX_RUN_AS_ROOT:-0}" | tr '[:upper:]' '[:lower:]')"

  local -a fb_cmd fb_args
  local fb_root_output_mode=0
  fb_cmd=("$fb_tool_path")
  fb_args=(exec --skip-git-repo-check --disable multi_agent -C "$WORKDIR" -o "$OUT_FILE")
  case "$fb_perm_mode" in
    full|unsafe|bypass|dangerous)
      fb_args+=(--dangerously-bypass-approvals-and-sandbox)
      ;;
    danger|danger-full-access)
      fb_args+=(--sandbox danger-full-access)
      ;;
    workspace|workspace-write|safe|"")
      fb_args+=(--sandbox workspace-write)
      ;;
    read-only|readonly)
      fb_args+=(--sandbox read-only)
      ;;
    *)
      fb_args+=(--sandbox workspace-write)
      ;;
  esac

  if [[ "$fb_run_as_root" == "1" || "$fb_run_as_root" == "true" || "$fb_run_as_root" == "yes" ]]; then
    if sudo -n true >/dev/null 2>&1; then
      fb_cmd=(sudo -n env)
      append_env_exports fb_cmd HOME PATH OPENAI_API_KEY OPENAI_BASE_URL OPENAI_ORG_ID OPENAI_PROJECT_ID HTTP_PROXY HTTPS_PROXY NO_PROXY ALL_PROXY
      fb_cmd+=("$fb_tool_path")
      fb_root_output_mode=1
    fi
  fi

  echo "[WARN] provider_rate_limit provider=claude fallback=codex role=${ROLE}" >>"$LOG_FILE"
  if [[ "$fb_root_output_mode" == "1" ]]; then
    sudo -n rm -f "$OUT_FILE" >/dev/null 2>&1 || true
  fi
  rm -f "$OUT_FILE" >/dev/null 2>&1 || true
  : >"$OUT_FILE"
  if ! "${fb_cmd[@]}" "${fb_args[@]}" "$(cat "$PROMPT_FILE")" >>"$LOG_FILE" 2>&1; then
    return 1
  fi

  if [[ "$fb_root_output_mode" == "1" ]]; then
    sudo -n test -s "$OUT_FILE" >/dev/null 2>&1 || [[ -s "$OUT_FILE" ]]
  else
    [[ -s "$OUT_FILE" ]]
  fi
}

run_claude_fallback_for_codex_limit() {
  local allow="${AOE_CODEX_FALLBACK_TO_CLAUDE:-1}"
  local normalized
  normalized="$(printf '%s' "$allow" | tr '[:upper:]' '[:lower:]')"
  if [[ "$normalized" == "0" || "$normalized" == "false" || "$normalized" == "no" || "$normalized" == "off" ]]; then
    return 1
  fi

  local fb_tool_path
  if ! fb_tool_path="$(command -v claude)"; then
    return 1
  fi

  local fb_perm_mode fb_run_as_root
  fb_perm_mode="$(printf '%s' "${AOE_CLAUDE_PERMISSION_MODE:-${AOE_CODEX_PERMISSION_MODE:-full}}" | tr '[:upper:]' '[:lower:]')"
  fb_run_as_root="$(printf '%s' "${AOE_CLAUDE_RUN_AS_ROOT:-${AOE_CODEX_RUN_AS_ROOT:-0}}" | tr '[:upper:]' '[:lower:]')"

  local -a fb_cmd fb_args
  local fb_root_output_mode=0
  fb_cmd=("$fb_tool_path")
  fb_args=(-p --output-format text --add-dir "$WORKDIR")
  case "$fb_perm_mode" in
    full|unsafe|bypass|dangerous|danger|danger-full-access)
      fb_args+=(--dangerously-skip-permissions --permission-mode bypassPermissions)
      ;;
    workspace|workspace-write|safe|"")
      fb_args+=(--permission-mode acceptEdits)
      ;;
    read-only|readonly)
      fb_args+=(--permission-mode plan)
      ;;
    auto|default|dontask|dont-ask|acceptedits|bypasspermissions|plan)
      case "$fb_perm_mode" in
        dontask|dont-ask) fb_perm_mode="dontAsk" ;;
        acceptedits) fb_perm_mode="acceptEdits" ;;
        bypasspermissions) fb_perm_mode="bypassPermissions" ;;
      esac
      fb_args+=(--permission-mode "$fb_perm_mode")
      ;;
    *)
      fb_args+=(--dangerously-skip-permissions --permission-mode bypassPermissions)
      ;;
  esac

  if [[ "$fb_run_as_root" == "1" || "$fb_run_as_root" == "true" || "$fb_run_as_root" == "yes" ]]; then
    if sudo -n true >/dev/null 2>&1; then
      fb_cmd=(sudo -n env)
      append_env_exports fb_cmd \
        HOME PATH ANTHROPIC_API_KEY ANTHROPIC_BASE_URL ANTHROPIC_AUTH_TOKEN \
        CLAUDE_CODE_USE_BEDROCK CLAUDE_CODE_OAUTH_TOKEN CLAUDE_CONFIG_DIR \
        AWS_REGION AWS_DEFAULT_REGION AWS_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN \
        HTTP_PROXY HTTPS_PROXY NO_PROXY ALL_PROXY
      fb_cmd+=("$fb_tool_path")
      fb_root_output_mode=1
    fi
  fi

  echo "[WARN] provider_rate_limit provider=codex fallback=claude role=${ROLE}" >>"$LOG_FILE"
  if [[ "$fb_root_output_mode" == "1" ]]; then
    sudo -n rm -f "$OUT_FILE" >/dev/null 2>&1 || true
  fi
  rm -f "$OUT_FILE" >/dev/null 2>&1 || true
  : >"$OUT_FILE"
  if ! "${fb_cmd[@]}" "${fb_args[@]}" "$(cat "$PROMPT_FILE")" >"$OUT_FILE" 2>>"$LOG_FILE"; then
    return 1
  fi

  if [[ "$fb_root_output_mode" == "1" ]]; then
    sudo -n test -s "$OUT_FILE" >/dev/null 2>&1 || [[ -s "$OUT_FILE" ]]
  else
    [[ -s "$OUT_FILE" ]]
  fi
}

provider_cooldown_fallback() {
  local provider="${1:-}"
  TEAM_DIR="$TEAM_DIR" PROVIDER="$provider" python3 - <<'PY'
import json
import os
from datetime import datetime, timezone
from pathlib import Path

provider = str(os.environ.get("PROVIDER", "")).strip().lower()
fallbacks = {"claude": "codex", "codex": "claude"}
fallback = fallbacks.get(provider, "")
if not fallback:
    print("\t\t")
    raise SystemExit(0)

path = Path(os.environ.get("TEAM_DIR", ".")).expanduser().resolve() / "provider_capacity.json"
try:
    data = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    data = {}
providers = data.get("providers") if isinstance(data, dict) else {}
if not isinstance(providers, dict):
    print("\t\t")
    raise SystemExit(0)

def parse_iso(raw):
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

now = datetime.now(timezone.utc)
prow = providers.get(provider) if isinstance(providers.get(provider), dict) else {}
frow = providers.get(fallback) if isinstance(providers.get(fallback), dict) else {}
pretry = parse_iso(prow.get("next_retry_at") or prow.get("last_retry_at"))
fretry = parse_iso(frow.get("next_retry_at") or frow.get("last_retry_at"))
if pretry is None or pretry <= now:
    print("\t\t")
    raise SystemExit(0)
if fretry is not None and fretry > now:
    print("\t\t")
    raise SystemExit(0)
print(f"{fallback}\t{str(prow.get('next_retry_at', '')).strip()}\t{str(prow.get('cooldown_level', '')).strip()}")
PY
}

extract_request_id() {
  local env_rid="${AOE_REQUEST_ID:-}"
  local body="${AOE_MSG_BODY:-}"
  local title="${AOE_MSG_TITLE:-}"
  local rid=""

  if [[ -n "$env_rid" ]]; then
    printf '%s\n' "$env_rid"
    return
  fi

  rid="$(awk -F':' '/^Request ID[[:space:]]*:/ {gsub(/^[[:space:]]+/, \"\", $2); print $2; exit}' <<<"$body" | tr -d '\r' | awk '{print $1}')"
  if [[ -n "$rid" ]]; then
    printf '%s\n' "$rid"
    return
  fi

  # Fallback: title like "[r_YYYY...] Role: ..."
  rid="$(sed -n 's/^\\[\\(r_[^]]\\+\\)\\].*/\\1/p' <<<"$title" | head -n 1 | tr -d '\r' | awk '{print $1}')"
  if [[ -n "$rid" ]]; then
    printf '%s\n' "$rid"
    return
  fi

  printf '\n'
}

lookup_tf_exec_meta() {
  local rid="$1"
  TEAM_DIR="$TEAM_DIR" RID="$rid" python3 - <<'PY'
import json
import os
from pathlib import Path

team_dir = Path(os.environ.get("TEAM_DIR", ".")).expanduser().resolve()
rid = (os.environ.get("RID") or "").strip()
path = team_dir / "tf_exec_map.json"
data = {}
try:
    data = json.loads(path.read_text(encoding="utf-8"))
except Exception:
    data = {}
row = data.get(rid) if isinstance(data, dict) else None
if not isinstance(row, dict):
    row = {}
print(str(row.get("workdir", "")).strip())
print(str(row.get("run_dir", "")).strip())
print(str(row.get("project_key", "")).strip())
print(str(row.get("project_alias", "")).strip())
print(str(row.get("project_root", "")).strip())
print(str(row.get("team_dir", "")).strip())
print(str(row.get("tf_id", "")).strip())
print(str(row.get("task_short_id", "")).strip())
print(str(row.get("task_alias", "")).strip())
print(str(row.get("branch", "")).strip())
print(str(row.get("mode", "")).strip())
print(str(row.get("source_request_id", "")).strip())
print(str(row.get("control_mode", "")).strip())
print(str(row.get("gateway_request_id", "")).strip())
PY
}

extract_user_request() {
  local body="${AOE_MSG_BODY:-}"
  local extracted
  extracted="$(awk 'BEGIN{flag=0}/^User Request:/{flag=1;next}/^Role Focus:/{flag=0}flag{print}' <<<"$body" | sed '/^[[:space:]]*$/d')"
  if [[ -n "$extracted" ]]; then
    printf '%s\n' "$extracted"
    return
  fi

  if [[ -n "${AOE_MSG_TITLE:-}" ]]; then
    printf '%s\n' "${AOE_MSG_TITLE}"
    return
  fi

  printf '%s\n' "$body"
}

role_guide() {
  local role_key
  role_key="$(printf '%s' "$ROLE" | tr '[:upper:]' '[:lower:]')"
  case "$role_key" in
    *review*|*qa*|*verif*|*critic*)
      cat <<'R'
- 리스크/회귀/누락 테스트를 우선 본다.
- 문제가 있으면 원인과 영향 범위를 먼저 말한다.
- 해결책은 바로 실행 가능한 형태로 제시한다.
R
      ;;
    *data*engineer*|*dataengineer*|*schema*|*etl*)
      cat <<'R'
- 데이터 품질, 스키마 정합성, 재현 가능성을 우선 본다.
- 검증 쿼리/체크포인트를 짧게 제시한다.
- 결과는 운영 적용 순서로 정리한다.
R
      ;;
    *codex-dev*|*codex_dev*|*codexdev*|*local-dev*|*local_dev*|*localdev*|*developer*|*builder*|*implement*)
      cat <<'R'
- 코드 수정, 버그 재현, 검증 순서를 우선 본다.
- 변경 파일과 검증 결과를 분명히 적는다.
- 애매하면 추측하지 말고 가장 작은 안전한 수정안을 제시한다.
R
      ;;
    *writer*|*doc*|*scribe*)
      cat <<'R'
- 문서 구조를 먼저 잡고, 사람이 바로 쓸 수 있는 형태로 정리한다.
- 근거가 된 파일/명령/결정사항을 짧게 연결한다.
- 장황한 설명보다 전달력과 재사용성을 우선한다.
R
      ;;
    *codex-analyst*|*codex_analyst*|*codexanalyst*|*analyst*|*analysis*|*research*|*tuner*)
      cat <<'R'
- 현재 상태를 조사하고, 선택지와 트레이드오프를 분리해서 정리한다.
- 사실과 해석을 섞지 말고, 결론에는 이유를 붙인다.
- 추가 조사가 필요한 부분은 불확실성으로 명시한다.
R
      ;;
    *)
      cat <<'R'
- 사용자 질문에 직접 답하고 실행 가능한 다음 단계를 제시한다.
- 불필요한 메타 설명은 생략한다.
R
      ;;
  esac
}

if ! command -v codex >/dev/null 2>&1; then
  echo "codex CLI not found" >&2
  exit 1
fi

USER_REQ="$(extract_user_request)"
GUIDE="$(role_guide)"
REQ_ID="$(extract_request_id)"

TF_WORKDIR=""
TF_RUN_DIR=""
TF_PROJECT_KEY=""
TF_PROJECT_ALIAS=""
TF_PROJECT_ROOT=""
TF_TEAM_DIR_META=""
TF_ID=""
TF_TASK_SHORT_ID=""
TF_TASK_ALIAS=""
TF_BRANCH=""
TF_EXEC_MODE=""
TF_SOURCE_REQUEST_ID=""
TF_CONTROL_MODE=""
TF_GATEWAY_REQUEST_ID=""
if [[ -n "$REQ_ID" ]]; then
  meta_out="$(lookup_tf_exec_meta "$REQ_ID" || true)"
  TF_WORKDIR="$(sed -n '1p' <<<"$meta_out" | tr -d '\r')"
  TF_RUN_DIR="$(sed -n '2p' <<<"$meta_out" | tr -d '\r')"
  TF_PROJECT_KEY="$(sed -n '3p' <<<"$meta_out" | tr -d '\r')"
  TF_PROJECT_ALIAS="$(sed -n '4p' <<<"$meta_out" | tr -d '\r')"
  TF_PROJECT_ROOT="$(sed -n '5p' <<<"$meta_out" | tr -d '\r')"
  TF_TEAM_DIR_META="$(sed -n '6p' <<<"$meta_out" | tr -d '\r')"
  TF_ID="$(sed -n '7p' <<<"$meta_out" | tr -d '\r')"
  TF_TASK_SHORT_ID="$(sed -n '8p' <<<"$meta_out" | tr -d '\r')"
  TF_TASK_ALIAS="$(sed -n '9p' <<<"$meta_out" | tr -d '\r')"
  TF_BRANCH="$(sed -n '10p' <<<"$meta_out" | tr -d '\r')"
  TF_EXEC_MODE="$(sed -n '11p' <<<"$meta_out" | tr -d '\r')"
  TF_SOURCE_REQUEST_ID="$(sed -n '12p' <<<"$meta_out" | tr -d '\r')"
  TF_CONTROL_MODE="$(sed -n '13p' <<<"$meta_out" | tr -d '\r')"
  TF_GATEWAY_REQUEST_ID="$(sed -n '14p' <<<"$meta_out" | tr -d '\r')"
fi

WORKDIR="${AOE_TF_WORKDIR:-}"
if [[ -z "$WORKDIR" ]]; then
  WORKDIR="$TF_WORKDIR"
fi
if [[ -z "$WORKDIR" ]]; then
  WORKDIR="$TF_PROJECT_ROOT"
fi
if [[ -z "$WORKDIR" ]]; then
  WORKDIR="$PROJECT_ROOT"
fi
if [[ ! -d "$WORKDIR" ]]; then
  if [[ -n "$TF_PROJECT_ROOT" && -d "$TF_PROJECT_ROOT" ]]; then
    WORKDIR="$TF_PROJECT_ROOT"
  else
    WORKDIR="$PROJECT_ROOT"
  fi
fi

if [[ -n "$TF_RUN_DIR" ]]; then
  LOG_DIR="$TF_RUN_DIR/logs"
  mkdir -p "$LOG_DIR"
fi

PROMPT_FILE="$(mktemp "$LOG_DIR/worker_prompt_${ROLE_SAFE}.XXXXXX")"
OUT_FILE="$(mktemp "$LOG_DIR/worker_out_${ROLE_SAFE}.XXXXXX")"
if [[ -n "$REQ_ID" ]]; then
  LOG_FILE="$LOG_DIR/worker_${ROLE_SAFE}_${REQ_ID}.log"
else
  LOG_FILE="$LOG_DIR/worker_${ROLE_SAFE}.log"
fi

runtime_meta="$(worker_runtime_meta || true)"
WORKER_PROVIDER="$(sed -n '1p' <<<"$runtime_meta" | tr -d '\r')"
WORKER_LAUNCH="$(sed -n '2p' <<<"$runtime_meta" | tr -d '\r')"
WORKER_PROVIDER_KEY="$(printf '%s' "${WORKER_PROVIDER:-codex}" | tr '[:upper:]' '[:lower:]')"
COOLDOWN_FALLBACK_PROVIDER=""
COOLDOWN_FALLBACK_RETRY_AT=""
COOLDOWN_FALLBACK_LEVEL=""
if [[ "$WORKER_PROVIDER_KEY" == "claude" || "$WORKER_PROVIDER_KEY" == "codex" ]]; then
  cooldown_meta="$(provider_cooldown_fallback "$WORKER_PROVIDER_KEY" || true)"
  COOLDOWN_FALLBACK_PROVIDER="$(sed -n '1p' <<<"$cooldown_meta" | awk -F'\t' '{print $1}' | tr -d '\r')"
  COOLDOWN_FALLBACK_RETRY_AT="$(sed -n '1p' <<<"$cooldown_meta" | awk -F'\t' '{print $2}' | tr -d '\r')"
  COOLDOWN_FALLBACK_LEVEL="$(sed -n '1p' <<<"$cooldown_meta" | awk -F'\t' '{print $3}' | tr -d '\r')"
  if [[ -n "$COOLDOWN_FALLBACK_PROVIDER" ]]; then
    echo "[WARN] provider_cooldown provider=${WORKER_PROVIDER_KEY} fallback=${COOLDOWN_FALLBACK_PROVIDER} retry_at=${COOLDOWN_FALLBACK_RETRY_AT:-"-"} level=${COOLDOWN_FALLBACK_LEVEL:-"-"} role=${ROLE}" >>"$LOG_FILE"
    WORKER_PROVIDER_KEY="$COOLDOWN_FALLBACK_PROVIDER"
    WORKER_LAUNCH="$COOLDOWN_FALLBACK_PROVIDER"
  fi
fi

# Never use an unquoted heredoc here: USER_REQ can contain backticks/$(...) from Telegram,
# which would trigger command substitution during prompt generation.
{
  printf '너는 AOE 팀의 %s 역할 에이전트다.\n\n' "$ROLE"
  printf '역할 지침:\n%s\n\n' "$GUIDE"
  printf '실행 컨텍스트:\n'
  printf -- '- project_alias: %s\n' "${TF_PROJECT_ALIAS:-"-"}"
  printf -- '- project_key: %s\n' "${TF_PROJECT_KEY:-"-"}"
  printf -- '- tf_id: %s\n' "${TF_ID:-"-"}"
  printf -- '- task_label: %s\n' "${TF_TASK_SHORT_ID:-"-"}"
  printf -- '- task_alias: %s\n' "${TF_TASK_ALIAS:-"-"}"
  printf -- '- control_mode: %s\n' "${TF_CONTROL_MODE:-dispatch}"
  printf -- '- source_request_id: %s\n' "${TF_SOURCE_REQUEST_ID:-"-"}"
  printf -- '- project_root: %s\n' "${TF_PROJECT_ROOT:-"$PROJECT_ROOT"}"
  printf -- '- workdir: %s\n' "$WORKDIR"
  printf -- '- run_dir: %s\n' "${TF_RUN_DIR:-"-"}"
  printf -- '- exec_mode: %s\n' "${TF_EXEC_MODE:-"-"}"
  printf -- '- branch: %s\n' "${TF_BRANCH:-"-"}"
  printf -- '- team_dir: %s\n\n' "${TF_TEAM_DIR_META:-"$TEAM_DIR"}"
  printf '사용자 요청:\n%s\n\n' "$USER_REQ"
  cat <<'RULES'
응답 규칙:
- 한국어로, 사용자에게 직접 말하듯 답한다.
- 요청ID/상태표/서버로그 같은 메타 표현은 쓰지 않는다.
- 결론 먼저, 필요한 근거는 짧게.
- 길이는 과도하게 길지 않게(대략 10~25줄 권장). 근거가 필요하면 근거가 우선이다.
- 마지막 줄에 "다음 행동:" 1줄을 반드시 쓴다.
- 필요한 경우, 읽기 전용 확인을 위해 안전한 명령을 실행해도 된다(예: ls, find, rg, sed, cat, head, tail, stat, git status/diff/log).
- 파일 수정은 요청에서 명시적으로 요구되거나 명확히 안전하고 필요한 경우에만 수행하고, 그 외에는 read-only로 유지한다.
- aoe-team 명령을 직접 호출하지 말고, 텍스트 답변만 작성한다.

최종 답변 본문만 출력해라.
RULES
} >"$PROMPT_FILE"

LAUNCH_PARTS=()
build_launch_parts "$WORKER_LAUNCH" LAUNCH_PARTS
if [[ "${#LAUNCH_PARTS[@]}" -eq 0 ]]; then
  LAUNCH_PARTS=("$WORKER_PROVIDER_KEY")
fi

TOOL_BIN="${LAUNCH_PARTS[0]}"
if ! command -v "$TOOL_BIN" >/dev/null 2>&1; then
  echo "${WORKER_PROVIDER_KEY} CLI not found: $TOOL_BIN" >&2
  exit 1
fi
TOOL_PATH="$(command -v "$TOOL_BIN")"
BASE_CMD=("$TOOL_PATH")
if (( ${#LAUNCH_PARTS[@]} > 1 )); then
  BASE_CMD+=("${LAUNCH_PARTS[@]:1}")
fi

ROOT_OUTPUT_MODE=0
ACTIVE_PERM_MODE=""
ACTIVE_RUN_AS_ROOT=""
RUN_CMD=()
RUN_ARGS=()

case "$WORKER_PROVIDER_KEY" in
  codex|"")
    ACTIVE_PERM_MODE="$(printf '%s' "${AOE_CODEX_PERMISSION_MODE:-full}" | tr '[:upper:]' '[:lower:]')"
    ACTIVE_RUN_AS_ROOT="$(printf '%s' "${AOE_CODEX_RUN_AS_ROOT:-0}" | tr '[:upper:]' '[:lower:]')"
    RUN_ARGS=(exec --skip-git-repo-check --disable multi_agent -C "$WORKDIR" -o "$OUT_FILE")
    case "$ACTIVE_PERM_MODE" in
      full|unsafe|bypass|dangerous)
        RUN_ARGS+=(--dangerously-bypass-approvals-and-sandbox)
        ;;
      danger|danger-full-access)
        RUN_ARGS+=(--sandbox danger-full-access)
        ;;
      workspace|workspace-write|safe|"")
        ACTIVE_PERM_MODE="workspace-write"
        RUN_ARGS+=(--sandbox workspace-write)
        ;;
      read-only|readonly)
        ACTIVE_PERM_MODE="read-only"
        RUN_ARGS+=(--sandbox read-only)
        ;;
      *)
        ACTIVE_PERM_MODE="workspace-write"
        RUN_ARGS+=(--sandbox workspace-write)
        ;;
    esac
    RUN_CMD=("${BASE_CMD[@]}")
    if [[ "$ACTIVE_RUN_AS_ROOT" == "1" || "$ACTIVE_RUN_AS_ROOT" == "true" || "$ACTIVE_RUN_AS_ROOT" == "yes" ]]; then
      if sudo -n true >/dev/null 2>&1; then
        RUN_CMD=(sudo -n env)
        append_env_exports RUN_CMD HOME PATH OPENAI_API_KEY OPENAI_BASE_URL OPENAI_ORG_ID OPENAI_PROJECT_ID HTTP_PROXY HTTPS_PROXY NO_PROXY ALL_PROXY
        RUN_CMD+=("${BASE_CMD[@]}")
        ROOT_OUTPUT_MODE=1
      else
        echo "[WARN] AOE_CODEX_RUN_AS_ROOT=1 but sudo -n unavailable; fallback user mode" >>"$LOG_FILE"
      fi
    fi
    ;;
  claude)
    ACTIVE_PERM_MODE="$(printf '%s' "${AOE_CLAUDE_PERMISSION_MODE:-${AOE_CODEX_PERMISSION_MODE:-full}}" | tr '[:upper:]' '[:lower:]')"
    ACTIVE_RUN_AS_ROOT="$(printf '%s' "${AOE_CLAUDE_RUN_AS_ROOT:-${AOE_CODEX_RUN_AS_ROOT:-0}}" | tr '[:upper:]' '[:lower:]')"
    RUN_ARGS=(-p --output-format text --add-dir "$WORKDIR")
    case "$ACTIVE_PERM_MODE" in
      full|unsafe|bypass|dangerous|danger|danger-full-access)
        ACTIVE_PERM_MODE="bypassPermissions"
        RUN_ARGS+=(--dangerously-skip-permissions --permission-mode bypassPermissions)
        ;;
      workspace|workspace-write|safe|"")
        ACTIVE_PERM_MODE="acceptEdits"
        RUN_ARGS+=(--permission-mode acceptEdits)
        ;;
      read-only|readonly)
        ACTIVE_PERM_MODE="plan"
        RUN_ARGS+=(--permission-mode plan)
        ;;
      auto|default|dontask|dont-ask|acceptedits|bypasspermissions|plan)
        case "$ACTIVE_PERM_MODE" in
          dontask|dont-ask) ACTIVE_PERM_MODE="dontAsk" ;;
          acceptedits) ACTIVE_PERM_MODE="acceptEdits" ;;
          bypasspermissions) ACTIVE_PERM_MODE="bypassPermissions" ;;
        esac
        RUN_ARGS+=(--permission-mode "$ACTIVE_PERM_MODE")
        ;;
      *)
        ACTIVE_PERM_MODE="bypassPermissions"
        RUN_ARGS+=(--dangerously-skip-permissions --permission-mode bypassPermissions)
        ;;
    esac
    RUN_CMD=("${BASE_CMD[@]}")
    if [[ "$ACTIVE_RUN_AS_ROOT" == "1" || "$ACTIVE_RUN_AS_ROOT" == "true" || "$ACTIVE_RUN_AS_ROOT" == "yes" ]]; then
      if sudo -n true >/dev/null 2>&1; then
        RUN_CMD=(sudo -n env)
        append_env_exports RUN_CMD \
          HOME PATH ANTHROPIC_API_KEY ANTHROPIC_BASE_URL ANTHROPIC_AUTH_TOKEN \
          CLAUDE_CODE_USE_BEDROCK CLAUDE_CODE_OAUTH_TOKEN CLAUDE_CONFIG_DIR \
          AWS_REGION AWS_DEFAULT_REGION AWS_PROFILE AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN \
          HTTP_PROXY HTTPS_PROXY NO_PROXY ALL_PROXY
        RUN_CMD+=("${BASE_CMD[@]}")
        ROOT_OUTPUT_MODE=1
      else
        echo "[WARN] AOE_CLAUDE_RUN_AS_ROOT=1 but sudo -n unavailable; fallback user mode" >>"$LOG_FILE"
      fi
    fi
    ;;
  *)
    echo "unsupported worker provider: ${WORKER_PROVIDER_KEY} (launch=${WORKER_LAUNCH:-$TOOL_BIN})" >&2
    exit 1
    ;;
esac

cleanup_temp_files() {
  if [[ "$ROOT_OUTPUT_MODE" == "1" ]] && sudo -n true >/dev/null 2>&1; then
    sudo -n rm -f "$PROMPT_FILE" "$OUT_FILE" >/dev/null 2>&1 || true
  fi
  rm -f "$PROMPT_FILE" "$OUT_FILE" >/dev/null 2>&1 || true
}

if [[ "${AOE_WORKER_DRY_RUN:-0}" == "1" || "${AOE_WORKER_DRY_RUN:-}" == "true" || "${AOE_WORKER_DRY_RUN:-}" == "yes" ]]; then
  printf 'provider=%s\n' "$WORKER_PROVIDER_KEY"
  printf 'launch=%s\n' "${WORKER_LAUNCH:-$TOOL_BIN}"
  printf 'permission_mode=%s\n' "$ACTIVE_PERM_MODE"
  printf 'run_as_root=%s\n' "${ACTIVE_RUN_AS_ROOT:-0}"
  printf 'workdir=%s\n' "$WORKDIR"
  printf 'command=%s\n' "$(shell_join "${RUN_CMD[@]}" "${RUN_ARGS[@]}" "<PROMPT>")"
  cleanup_temp_files
  exit 0
fi

echo "[INFO] role=${ROLE} request_id=${REQ_ID:-} tf_id=${TF_ID:-} project=${TF_PROJECT_KEY:-} workdir=${WORKDIR} provider=${WORKER_PROVIDER_KEY} launch=${WORKER_LAUNCH:-$TOOL_BIN} permission_mode=${ACTIVE_PERM_MODE} run_as_root=${ACTIVE_RUN_AS_ROOT:-0}" >>"$LOG_FILE"
if [[ "$WORKER_PROVIDER_KEY" == "claude" ]]; then
  if ! "${RUN_CMD[@]}" "${RUN_ARGS[@]}" "$(cat "$PROMPT_FILE")" >"$OUT_FILE" 2>>"$LOG_FILE"; then
    err_tail="$(tail -n 80 "$LOG_FILE" 2>/dev/null || true)"
    if is_rate_limited_text "$err_tail" && run_codex_fallback_for_claude_limit; then
      echo "[INFO] claude rate limit fallback succeeded (role=${ROLE})" >>"$LOG_FILE"
    else
      echo "claude exec failed (role=${ROLE})" >&2
      tail -n 40 "$LOG_FILE" >&2 || true
      cleanup_temp_files
      exit 1
    fi
  fi
else
  if ! "${RUN_CMD[@]}" "${RUN_ARGS[@]}" "$(cat "$PROMPT_FILE")" >>"$LOG_FILE" 2>&1; then
    err_tail="$(tail -n 80 "$LOG_FILE" 2>/dev/null || true)"
    if [[ "$WORKER_PROVIDER_KEY" == "codex" ]] && is_rate_limited_text "$err_tail" && run_claude_fallback_for_codex_limit; then
      echo "[INFO] codex rate limit fallback succeeded (role=${ROLE})" >>"$LOG_FILE"
    else
      echo "${WORKER_PROVIDER_KEY} exec failed (role=${ROLE})" >&2
      tail -n 40 "$LOG_FILE" >&2 || true
      cleanup_temp_files
      exit 1
    fi
  fi
fi

if [[ "$WORKER_PROVIDER_KEY" != "claude" ]]; then
  :
elif [[ "$ROOT_OUTPUT_MODE" == "1" ]]; then
  if ! sudo -n test -s "$OUT_FILE" >/dev/null 2>&1 && [[ ! -s "$OUT_FILE" ]]; then
    echo "empty claude output (role=${ROLE})" >&2
    cleanup_temp_files
    exit 1
  fi
elif [[ ! -s "$OUT_FILE" ]]; then
  echo "empty claude output (role=${ROLE})" >&2
  tail -n 40 "$LOG_FILE" >&2 || true
  cleanup_temp_files
  exit 1
fi

if [[ "$ROOT_OUTPUT_MODE" == "1" ]]; then
  if ! sudo -n test -s "$OUT_FILE" >/dev/null 2>&1 && [[ ! -s "$OUT_FILE" ]]; then
    echo "empty codex output (role=${ROLE})" >&2
    cleanup_temp_files
    exit 1
  fi
elif [[ ! -s "$OUT_FILE" ]]; then
  echo "empty codex output (role=${ROLE})" >&2
  cleanup_temp_files
  exit 1
fi

if [[ "$ROOT_OUTPUT_MODE" == "1" ]] && sudo -n true >/dev/null 2>&1; then
  if ! sudo -n cat "$OUT_FILE"; then
    cat "$OUT_FILE"
  fi
else
  cat "$OUT_FILE"
fi
cleanup_temp_files
