# RUNBOOK

## 1. Service Topology
- Gateway session: `aoe_tg_gateway`
- Worker sessions: `aoe_tg_worker_*`
- Project root: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control`
- Team dir: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team`
- Gateway source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe-telegram-gateway.py`
- Parse/Resolver/Handlers/Flow/ACL modules: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_parse.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_command_resolver.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_command_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_management_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_orch_overview_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_orch_task_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_retry_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_role_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_message_flow.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_run_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_acl.py`
- Runtime boundary: `.aoe-team` is mutable runtime state. `team.json`, `orchestrator.json`, `workers/*.json`, `agents/*/AGENTS.md` are local-only and not versioned.

## 2. Standard Operations
0. First-time runtime init (if `.aoe-team/orchestrator.json` is missing):
`aoe-orch init --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control --overview "aoe_orch_control project orchestration"`
0. Bootstrap missing runtime files from templates (optional, non-destructive):
`bash /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/team/bootstrap_runtime_templates.sh --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control`
0. Equivalent shortcut:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh init`
1. Start:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh start`
0. (Optional) refresh runtime symlinks:
`bash /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/install_runtime.sh`
2. Status:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh status`
3. Logs (tmux pane capture):
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh logs`
4. Structured gateway events:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/logs/gateway_events.jsonl`
5. Health:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh health --wait=3`
6. Stop:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh stop`
7. Gateway regression tests (pytest via uv):
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway_pytest.sh`
8. Smoke/Error subset wrappers:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway_smoke_test.sh`
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway_error_test.sh`
9. CI workflow definition:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.github/workflows/gateway-tests.yml`
- job shape: `gateway-smoke`, `gateway-error` (matrix parallel run)

## 2.1 Systemd User Mode (recommended)
1. Install:
`bash /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/systemd/install_user_services.sh`
2. Stack status:
`systemctl --user status aoe-telegram-stack.service`
3. Heal timer status:
`systemctl --user status aoe-telegram-heal.timer`
4. Force health check now:
`systemctl --user start aoe-telegram-heal.service`
5. Auto-run after logout/reboot:
`sudo loginctl enable-linger kimyoungjin06`

## 3. Health Checks
1. Process check:
`ps -ef | rg 'aoe-telegram-gateway|aoe-orch worker'`
2. Session check:
`tmux list-sessions -F '#{session_name}' | rg '^aoe_tg_'`
3. Gateway command check (dry):
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe-telegram-gateway.py --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control --allow-chat-ids 1 --once --dry-run --simulate-chat-id 1 --simulate-text '/monitor 3'`
4. KPI check:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe-telegram-gateway.py --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control --allow-chat-ids 1 --once --dry-run --simulate-chat-id 1 --simulate-text '/kpi 24'`

## 4. Incident Response
### 4.1 No Telegram response
1. Check `TELEGRAM_BOT_TOKEN` in `.aoe-team/telegram.env`.
2. Verify gateway session exists and is running.
3. Ensure preflight passes: `telegram_tmux.sh init` then `telegram_tmux.sh start`.
4. Restart stack with `telegram_tmux.sh restart`.
4. Re-run `/whoami`, `/help`, `/monitor` from Telegram.

### 4.5 Access denied / unauthorized
1. Check `.aoe-team/telegram.env` values:
- `AOE_DENY_BY_DEFAULT`
- `TELEGRAM_OWNER_CHAT_ID`
- `TELEGRAM_ALLOW_CHAT_IDS`
- `TELEGRAM_ADMIN_CHAT_IDS`
- `TELEGRAM_READONLY_CHAT_IDS`
2. If allowlist is empty and deny mode is on, send `/lockme` from owner chat.
3. `/lockme`는 allowlist를 현재 chat으로 재설정하고 admin/readonly ACL을 비웁니다.
4. Validate with `/whoami` and `/acl` (role + ACL 확인).
5. Update ACL if needed:
- `/grant admin <chat_id|alias>`
- `/grant readonly <chat_id|alias>`
- `/revoke <allow|admin|readonly|all> <chat_id|alias>`
6. Prefer alias workflow:
- `/acl`로 alias table 확인 (`1:12345...` 형태)
- 이후 `/grant admin 1`, `/revoke readonly 2`처럼 단축 가능
7. Restart stack if env was edited manually.

### 4.2 Token rotation (mandatory on exposure)
1. BotFather에서 기존 토큰 revoke 후 새 토큰 발급.
2. `.aoe-team/telegram.env`의 `TELEGRAM_BOT_TOKEN` 갱신.
3. `telegram_tmux.sh restart` 실행.
4. Telegram에서 `/help`, `/kpi 24`, `/monitor 3` 순으로 정상 응답 확인.
5. 운영 메모에 회전 시점/사유 기록.

### 4.3 Worker replies missing
1. Check worker tmux sessions (`aoe_tg_worker_*`).
2. Inspect worker logs under `.aoe-team/logs/`.
3. Verify role config in `.aoe-team/orchestrator.json`.
4. Re-dispatch with explicit role scope.

### 4.4 State inconsistency
1. Validate `.aoe-team/orch_manager_state.json` is readable JSON.
2. Keep backup copy before edits.
3. Restart gateway; it now sanitizes invalid task/stage fields on load.
4. Re-check with `/task <T-xxx|alias|request_id>`.

## 5. Error Code Guide
- `E_COMMAND`: invalid command usage/arguments.
- `E_TIMEOUT`: orchestration command timeout.
- `E_GATE`: verifier/planning gate blocked.
- `E_ORCH`: aoe-orch execution issue.
- `E_REQUEST`: request query issue.
- `E_TELEGRAM`: Telegram API send/poll issue.
- `E_AUTH`: unauthorized / permission denied.
- `E_INTERNAL`: uncategorized handler error.

## 6. Recovery Objective
- Target restart time: within 10 minutes.
- Minimum recovery verification:
- `/status`
- `/monitor 3`
- one `/dispatch` test request completion

## 7. Live Smoke Test
- Prerequisites:
- `TELEGRAM_BOT_TOKEN`, `TG_TEST_CHAT_ID` 환경변수 설정
- Gateway stack 실행 중
- Command:
- `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway_live_smoke_test.sh`

## 8. Runtime Tunables
- `AOE_TG_SEND_RETRIES`: Telegram send retry count (default 2)
- `AOE_TG_SEND_RETRY_DELAY_MS`: retry base delay in ms (default 300)
- `AOE_GATEWAY_LOG_MAX_BYTES`: event log rotation threshold (default 5MB)
- `AOE_GATEWAY_LOG_KEEP_FILES`: rotated event log count (default 5)
- `AOE_GATEWAY_INSTANCE_LOCK`: single-instance lock file path (default `.aoe-team/.gateway.instance.lock`)
- `AOE_DENY_BY_DEFAULT`: deny when allowlist empty (default 1)
- `TELEGRAM_ALLOW_CHAT_IDS`: comma-separated allowed chat IDs
- `TELEGRAM_ADMIN_CHAT_IDS`: optional admin-only chat IDs
- `TELEGRAM_READONLY_CHAT_IDS`: optional read-only chat IDs
- `TELEGRAM_OWNER_CHAT_ID`: owner chat ID (when set, `/lockme`, `/grant`, `/revoke` are owner-only)
- `AOE_CHAT_ALIASES_FILE`: alias mapping file path (default `.aoe-team/telegram_chat_aliases.json`)
- `AOE_CONFIRM_TTL_SEC`: high-risk auto-run confirmation TTL seconds (default 300)
- `AOE_CHAT_MAX_RUNNING`: per-chat concurrent running task limit (default 2, 0 disables)
- `AOE_CHAT_DAILY_CAP`: per-chat daily task creation limit (default 40, 0 disables)

## 9. Command Delta
- `/cancel` : pending mode 해제
- `/pick <번호|label>` : 현재 task 포커스 지정
- `/cancel <task>` : 실행 중 요청 cancel 시도
- `/ok` : 고위험 자동실행 확인 후 진행
- `/retry <task>` : 동일 prompt/roles로 재실행
- `/replan <task>` : planner/critic를 다시 생성해 재실행
- `/whoami` : 현재 chat 권한/allowlist 확인
- `/mode [on|off|direct]` : 기본 평문 라우팅 모드 설정/조회 (`/on`, `/off` 단축 지원, `/off`는 one-shot pending도 해제)
- `/lockme` : 현재 chat으로 allowlist 잠금
- `/acl` : ACL(allow/admin/readonly) 요약 확인
- `/grant <allow|admin|readonly> <chat_id|alias>` : ACL 권한 부여
- `/revoke <allow|admin|readonly|all> <chat_id|alias>` : ACL 권한 제거
- owner mode: `TELEGRAM_OWNER_CHAT_ID` 설정 시 `/lockme`, `/grant`, `/revoke`는 owner-only
- systemd install/uninstall:
- `scripts/systemd/install_user_services.sh`
- `scripts/systemd/uninstall_user_services.sh`
- Safe plain-text shortcuts in slash-only mode:
- `모니터 5` -> `/monitor 5`
- `확인 1` -> `/check 1`
- `상태 1` -> `/task 1`
- `재시도 1` -> `/retry 1`
- `재계획 1` -> `/replan 1`
- `취소 1` -> `/cancel 1`
