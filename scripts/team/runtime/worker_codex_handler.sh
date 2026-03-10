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
  case "$ROLE" in
    Reviewer)
      cat <<'R'
- 리스크/회귀/누락 테스트를 우선 본다.
- 문제가 있으면 원인과 영향 범위를 먼저 말한다.
- 해결책은 바로 실행 가능한 형태로 제시한다.
R
      ;;
    DataEngineer)
      cat <<'R'
- 데이터 품질, 스키마 정합성, 재현 가능성을 우선 본다.
- 검증 쿼리/체크포인트를 짧게 제시한다.
- 결과는 운영 적용 순서로 정리한다.
R
      ;;
    Local-Dev)
      cat <<'R'
- 코드 수정, 버그 재현, 검증 순서를 우선 본다.
- 변경 파일과 검증 결과를 분명히 적는다.
- 애매하면 추측하지 말고 가장 작은 안전한 수정안을 제시한다.
R
      ;;
    Local-Writer)
      cat <<'R'
- 문서 구조를 먼저 잡고, 사람이 바로 쓸 수 있는 형태로 정리한다.
- 근거가 된 파일/명령/결정사항을 짧게 연결한다.
- 장황한 설명보다 전달력과 재사용성을 우선한다.
R
      ;;
    Local-Analyst)
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

PERM_MODE_RAW="${AOE_CODEX_PERMISSION_MODE:-full}"
PERM_MODE="$(printf '%s' "$PERM_MODE_RAW" | tr '[:upper:]' '[:lower:]')"
RUN_AS_ROOT_RAW="${AOE_CODEX_RUN_AS_ROOT:-0}"
RUN_AS_ROOT="$(printf '%s' "$RUN_AS_ROOT_RAW" | tr '[:upper:]' '[:lower:]')"
CODEX_ARGS=(exec --skip-git-repo-check --disable multi_agent -C "$WORKDIR" -o "$OUT_FILE")
CODEX_CMD=(codex)
ROOT_OUTPUT_MODE=0

case "$PERM_MODE" in
  full|unsafe|bypass|dangerous)
    CODEX_ARGS+=(--dangerously-bypass-approvals-and-sandbox)
    ;;
  danger|danger-full-access)
    CODEX_ARGS+=(--sandbox danger-full-access)
    ;;
  workspace|workspace-write|safe|"")
    CODEX_ARGS+=(--sandbox workspace-write)
    ;;
  read-only|readonly)
    CODEX_ARGS+=(--sandbox read-only)
    ;;
  *)
    PERM_MODE="workspace-write"
    CODEX_ARGS+=(--sandbox workspace-write)
    ;;
esac

if [[ "$RUN_AS_ROOT" == "1" || "$RUN_AS_ROOT" == "true" || "$RUN_AS_ROOT" == "yes" ]]; then
  if sudo -n true >/dev/null 2>&1; then
    CODEX_CMD=(sudo -n env)
    for k in HOME OPENAI_API_KEY OPENAI_BASE_URL OPENAI_ORG_ID OPENAI_PROJECT_ID HTTP_PROXY HTTPS_PROXY NO_PROXY ALL_PROXY; do
      v="${!k-}"
      if [[ -n "$v" ]]; then
        CODEX_CMD+=("$k=$v")
      fi
    done
    CODEX_CMD+=(codex)
    ROOT_OUTPUT_MODE=1
  else
    echo "[WARN] AOE_CODEX_RUN_AS_ROOT=1 but sudo -n unavailable; fallback user mode" >>"$LOG_FILE"
  fi
fi

cleanup_temp_files() {
  if [[ "$ROOT_OUTPUT_MODE" == "1" ]] && sudo -n true >/dev/null 2>&1; then
    sudo -n rm -f "$PROMPT_FILE" "$OUT_FILE" >/dev/null 2>&1 || true
  fi
  rm -f "$PROMPT_FILE" "$OUT_FILE" >/dev/null 2>&1 || true
}

echo "[INFO] role=${ROLE} request_id=${REQ_ID:-} tf_id=${TF_ID:-} project=${TF_PROJECT_KEY:-} workdir=${WORKDIR} permission_mode=${PERM_MODE} run_as_root=${RUN_AS_ROOT}" >>"$LOG_FILE"
if ! "${CODEX_CMD[@]}" "${CODEX_ARGS[@]}" "$(cat "$PROMPT_FILE")" >>"$LOG_FILE" 2>&1; then
  echo "codex exec failed (role=${ROLE})" >&2
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
