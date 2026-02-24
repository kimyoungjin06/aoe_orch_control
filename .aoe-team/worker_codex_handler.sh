#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROLE="${AOE_WORKER_ACTOR:-Worker}"
ROLE_SAFE="$(printf '%s' "$ROLE" | tr -c 'A-Za-z0-9._-' '_')"
TEAM_DIR="${AOE_TEAM_DIR:-$SCRIPT_DIR}"
PROJECT_ROOT="$(dirname "$TEAM_DIR")"
LOG_DIR="$TEAM_DIR/logs"
mkdir -p "$LOG_DIR"

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

PROMPT_FILE="$(mktemp "$LOG_DIR/worker_prompt_${ROLE_SAFE}.XXXXXX")"
OUT_FILE="$(mktemp "$LOG_DIR/worker_out_${ROLE_SAFE}.XXXXXX")"
LOG_FILE="$LOG_DIR/worker_${ROLE_SAFE}.log"

cat > "$PROMPT_FILE" <<P
너는 AOE 팀의 ${ROLE} 역할 에이전트다.

역할 지침:
${GUIDE}

사용자 요청:
${USER_REQ}

응답 규칙:
- 한국어로, 사용자에게 직접 말하듯 답한다.
- 요청ID/상태표/서버로그 같은 메타 표현은 쓰지 않는다.
- 결론 먼저, 필요한 근거는 짧게.
- 길이 6~12줄 이내.
- 마지막 줄에 "다음 행동:" 1줄을 반드시 쓴다.
- 쉘 명령 실행/파일 수정/aoe-team 명령 호출은 하지 말고 텍스트 답변만 작성한다.

최종 답변 본문만 출력해라.
P

PERM_MODE_RAW="${AOE_CODEX_PERMISSION_MODE:-full}"
PERM_MODE="$(printf '%s' "$PERM_MODE_RAW" | tr '[:upper:]' '[:lower:]')"
RUN_AS_ROOT_RAW="${AOE_CODEX_RUN_AS_ROOT:-0}"
RUN_AS_ROOT="$(printf '%s' "$RUN_AS_ROOT_RAW" | tr '[:upper:]' '[:lower:]')"
CODEX_ARGS=(exec --skip-git-repo-check --disable multi_agent -C "$PROJECT_ROOT" -o "$OUT_FILE")
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

echo "[INFO] role=${ROLE} permission_mode=${PERM_MODE} run_as_root=${RUN_AS_ROOT}" >>"$LOG_FILE"
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
