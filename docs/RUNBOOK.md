# RUNBOOK

## 1. Service Topology
- Gateway session (default): `aoe_mo_gateway`
- Worker sessions (default): `aoe_tf_worker_*`
- Legacy session compatibility: `aoe_tg_gateway`, `aoe_tg_worker_*`
- Project root: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control`
- Team dir: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team`
- TF exec map (auto): `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/tf_exec_map.json` (request_id -> workdir/run_dir)
- TF run dirs (auto): `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/tf_runs/<request_id>/` (per-request logs/meta; failure is pruned)
- TF worktrees (auto, default): `/home/kimyoungjin06/Desktop/Workspace/.aoe-tf/<project>/<request_id>/` (set `AOE_TF_WORK_ROOT` to override)
- Gateway poll state: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_gateway_state.json` (`offset`, `acked_updates`, `handled_messages`, `duplicate_skipped`, `unauthorized_skipped`, `empty_skipped`, `handler_errors`, `failed_queue` 등)
- Ephemeral room logs (auto): `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/logs/rooms/<room>/<YYYY-MM-DD(.N)?>.jsonl` (default retention: 14 days)
- Investigations registry (auto-sync target): `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/docs/investigations_mo/registry` (`project_lock.yaml` with `tf_report`, `tf_registry.md`, `handoff_index.csv`, `tf_close_index.csv`)
- Gateway source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe-telegram-gateway.py`
- Parse/Resolver/Handlers/Flow/ACL modules: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_parse.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_command_resolver.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_command_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_management_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_orch_overview_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_orch_task_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_retry_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_role_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_message_flow.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_run_handlers.py`, `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_acl.py`
- Runtime boundary: `.aoe-team` is mutable runtime state. `team.json`, `orchestrator.json`, `workers/*.json`, `agents/*/AGENTS.md` are local-only and not versioned.

## 2. Standard Operations
0. Install global launcher (one-time):
`bash /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/team/install_global_cli.sh`
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
2.1 Session overview with numeric map:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh overview`
2.1.1 Telegram project alias map:
`/map` (또는 `aoe map`, `aoe orch map`)
2.2 Apply visual/key UI (status bar hints + Alt+1..9 and Prefix+1..9 binding refresh, page nav Alt+,/Alt+.):
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh ui`
2.2.1 Page control (9개 초과 세션 페이징):
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh page status`
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh page next`
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh page set 2`
2.3 Optional session naming override:
`AOE_TMUX_GATEWAY_SESSION=aoe_mo_gateway AOE_TMUX_WORKER_PREFIX=aoe_tf_worker_ aoe-team-stack restart`
2.3.1 Optional page size override (1..9):
`AOE_TMUX_PAGE_SIZE=9 aoe-team-stack ui`
2.3.2 Optional hint width override:
`AOE_TMUX_HINT_NAME_MAX=7 aoe-team-stack ui`
`AOE_TMUX_COMPACT_NAME_MAX=20 aoe-team-stack ui`
2.4 Quick switch by index:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh switch 2`
2.5 Same operations via global command (from anywhere):
`aoe-team-stack start`
`aoe-team-stack ui`
`aoe-team-stack page status`
`aoe-team-stack switch 2`
2.6 Remove global launcher:
`bash /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/team/uninstall_global_cli.sh`
2.7 Off-desk(퇴근 후) 권장 실행(Telegram):
`/offdesk on`
`/auto on fanout recent`  # (manual equivalent)
`/auto status`
긴급 정지:
`/panic`
3. Logs (tmux pane capture):
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram_tmux.sh logs`
4. Structured gateway events:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/logs/gateway_events.jsonl`
5. Health (unhealthy 시 `E_HEALTH_*` 원인 코드 출력):
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
`tmux list-sessions -F '#{session_name}' | rg '^aoe_(mo_gateway|tf_worker_|tg_gateway|tg_worker_)'`
2.1 Stack health (reason code 포함):
`aoe-team-stack health --wait=3`
3. Gateway command check (dry):
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe-telegram-gateway.py --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control --allow-chat-ids 1 --once --dry-run --simulate-chat-id 1 --simulate-text '/monitor 3'`
4. KPI check:
`/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe-telegram-gateway.py --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control --allow-chat-ids 1 --once --dry-run --simulate-chat-id 1 --simulate-text '/kpi 24'`
5. Investigations registry freshness check:
`ls -l /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/docs/investigations_mo/registry/project_lock.yaml /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/docs/investigations_mo/registry/tf_registry.md /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/docs/investigations_mo/registry/handoff_index.csv`
6. Investigations registry content sanity check:
`rg -n "active_project:|active_tf:|\\| tf_id \\||handoff_id,project_alias" /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/docs/investigations_mo/registry/project_lock.yaml /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/docs/investigations_mo/registry/tf_registry.md /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/docs/investigations_mo/registry/handoff_index.csv`

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
2. If allowlist is empty and deny mode is on, send `/onlyme` (권장) or `/lockme` from your chat.
3. `/onlyme`는 allowlist를 현재 chat으로 재설정하고 `owner_only`(1:1 DM gate)를 켭니다. `/lockme`는 allowlist만 재설정합니다.
4. Validate with `/whoami` and `/acl` (role + ACL 확인).
5. Update ACL if needed:
- `/grant admin <chat_id|alias>`
- `/grant readonly <chat_id|alias>`
- `/revoke <allow|admin|readonly|all> <chat_id|alias>`
6. Prefer alias workflow:
- `/acl`로 alias table 확인 (`1:12345...` 형태)
- 이후 `/grant admin 1`, `/revoke readonly 2`처럼 단축 가능
7. Restart stack if env was edited manually.

### 4.6 Owner-only 모드 + 평문 입력(운영 메모)
- 목적: root/no-prompt 운영에서 텔레그램 봇을 **오너 1인**에게만 고정하고, 그룹/타인 입력을 원천 차단.
- 설정(권장): `.aoe-team/telegram.env`
  - `TELEGRAM_OWNER_CHAT_ID=<내 chat_id>`
  - `AOE_OWNER_ONLY=1` (오너 DM(private) + from.id 일치만 허용)
  - `AOE_OWNER_BOOTSTRAP_MODE=dispatch` (처음부터 "그냥 평문" 입력을 dispatch로 라우팅)
- 빠른 설정(bootstrap):
  - allowlist가 비어있고 `AOE_DENY_BY_DEFAULT=1`인 상태에서, Telegram DM으로 `/onlyme` 1회 실행.
- 사용 흐름(가장 단순):
  - 1: 오너 계정으로 봇에게 DM
  - 2: `/on`(선택) 또는 평문 1회 전송(bootstrap 동작 확인)
  - 3: 이후 평문은 자동 실행(질문은 intent 감지로 direct로 라우팅)
- 주의:
  - 이 구성은 "원격 root 콘솔"에 가깝습니다. 텔레그램 계정 보안(2FA)과 토큰 회전 정책을 반드시 유지하세요.

### 4.7 tmux "returned N" 스팸(UX 깨짐)
- 증상:
  - tmux 하단/상단에 `'/.../telegram_tmux.sh refresh --quiet' returned 1` 같은 메시지가 반복 출력
  - 또는 `TMUX_SWITCH=1 ... switch N returned 2` 등 전환 단축키 실행 시 에러 팝업
- 원인:
  - 구버전 hook/키바인딩이 tmux 서버에 남아있거나, copy-mode 진입 상태(`[0/xxxx]`)에서 메시지가 겹쳐 보이는 경우
- 조치(권장 순서):
  - copy-mode 표시(`[0/xxxx]`)가 있으면 `q`로 먼저 빠져나오기
  - UI 재적용: `aoe-team-stack ui` (또는 `.aoe-team/telegram_tmux.sh ui`)
  - 확인: `tmux show-hooks -g | rg 'refresh --quiet; true'` 가 보이면 정상

### 4.8 `status`가 `orchestrator.json` 누락으로 실패
- 증상:
  - (구버전) `!status` 입력 시 gateway tmux 로그에 Python Traceback이 찍히고 응답이 없거나,
  - (신버전) `!status`가 `[WARN] orch config missing (orchestrator.json)` 또는 `config not found` 류 메시지를 표시
- 원인:
  - orch는 registry(`orch_manager_state.json`)에는 등록되어 있지만, 해당 프로젝트의 `.aoe-team/orchestrator.json`이 생성되지 않음
  - 흔한 케이스: 예전 버전에서 프로젝트만 등록됨, `--no-init`로 추가됨, 프로젝트 폴더 이동/정리 중 `.aoe-team` 일부만 남음
- 조치:
  - Telegram에서 초기화(권장):
    - `!orch add <name> --path <project_root>` (기본값으로 init/spawn 수행)
    - 세션 spawn 없이 config만 만들려면: `!orch add <name> --path <project_root> --no-spawn`
  - 로컬에서 직접 초기화:
    - `aoe-orch init --project-root <project_root> --team-dir <project_root>/.aoe-team --overview "<overview>"`
    - (필요 시) `aoe-orch spawn --project-root <project_root> --team-dir <project_root>/.aoe-team`

### 4.2 Token rotation (mandatory on exposure)
1. BotFather에서 기존 토큰 revoke 후 새 토큰 발급.
2. `.aoe-team/telegram.env`의 `TELEGRAM_BOT_TOKEN` 갱신.
3. `telegram_tmux.sh restart` 실행.
4. Telegram에서 `/help`, `/kpi 24`, `/monitor 3` 순으로 정상 응답 확인.
5. 운영 메모에 회전 시점/사유 기록.

### 4.3 Worker replies missing
1. Check worker tmux sessions (`aoe_tf_worker_*`; legacy `aoe_tg_worker_*`).
2. Inspect worker logs under `.aoe-team/logs/`.
3. Verify role config in `.aoe-team/orchestrator.json`.
4. Re-dispatch with explicit role scope.

### 4.4 State inconsistency
1. Validate `.aoe-team/orch_manager_state.json` is readable JSON.
2. Keep backup copy before edits.
3. Restart gateway; it now sanitizes invalid task/stage fields on load.
4. Re-check with `/task <T-xxx|alias|request_id>`.
5. Re-sync investigations registry from manager state (manual recovery):
`python3 -c "from pathlib import Path; import json, sys; root=Path('/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control'); sys.path.insert(0, str((root / 'scripts/gateway').resolve())); from aoe_tg_investigations_sync import sync_investigations_docs; sp=(root / '.aoe-team/orch_manager_state.json').resolve(); sync_investigations_docs(sp, json.loads(sp.read_text(encoding='utf-8'))); print('synced investigations_mo registry')"`
6. Verify `docs/investigations_mo/registry/project_lock.yaml`, `docs/investigations_mo/registry/tf_registry.md`, `docs/investigations_mo/registry/tf_close_index.csv` and that `docs/investigations_mo/projects/<alias>/tfs/<tf_id>/report.md` exists.

## 5. Error Code Guide
- `E_COMMAND`: invalid command usage/arguments.
- `E_TIMEOUT`: orchestration command timeout.
- `E_GATE`: verifier/planning gate blocked.
- `E_ORCH`: aoe-orch execution issue.
- `E_REQUEST`: request query issue.
- `E_TELEGRAM`: Telegram API send/poll issue.
- `E_AUTH`: unauthorized / permission denied.
- `E_HEALTH_NO_TMUX`: `tmux` not found in PATH.
- `E_HEALTH_TMUX_SERVER_DOWN`: tmux server is down/unreachable.
- `E_HEALTH_ENV_MISSING`: missing `.aoe-team/telegram.env`.
- `E_HEALTH_HANDLER_MISSING`: missing `.aoe-team/worker_codex_handler.sh`.
- `E_HEALTH_GATEWAY_BIN_MISSING`: missing `scripts/gateway/aoe-telegram-gateway.py`.
- `E_HEALTH_GATEWAY_DOWN`: gateway tmux session missing.
- `E_HEALTH_WORKER_DOWN`: worker tmux session missing (per role).
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
- `AOE_GATEWAY_DEDUP_KEEP`: 최근 처리 update/message dedup 캐시 크기 (default 2000, range 100..20000)
- `AOE_GATEWAY_FAILED_KEEP`: 실패 입력 replay queue 보관 수 (default 200, range 10..5000)
- `AOE_GATEWAY_FAILED_TTL_HOURS`: replay queue 항목 TTL 시간 (default 168, 0 disables, range 0..8760)
- `AOE_GATEWAY_INSTANCE_LOCK`: single-instance lock file path (default `.aoe-team/.gateway.instance.lock`)
- `AOE_DENY_BY_DEFAULT`: deny when allowlist empty (default 1)
- `TELEGRAM_ALLOW_CHAT_IDS`: comma-separated allowed chat IDs
- `TELEGRAM_ADMIN_CHAT_IDS`: optional admin-only chat IDs
- `TELEGRAM_READONLY_CHAT_IDS`: optional read-only chat IDs
- `TELEGRAM_OWNER_CHAT_ID`: owner chat ID (when set, `/lockme`, `/grant`, `/revoke` are owner-only)
- `AOE_OWNER_ONLY`: owner-only enforcement (when set, only private-DM messages from owner are accepted)
- `AOE_OWNER_BOOTSTRAP_MODE`: owner UX convenience; when chat default_mode is unset, auto set to `dispatch` or `direct` on first owner message
- `AOE_EXEC_CRITIC`: post-execution critic verdict + auto-retry loop (default 1)
- `AOE_EXEC_RETRY_MAX`: max total attempts (including first) when exec critic returns `retry` (default 3, range 1..9)
- `AOE_CHAT_ALIASES_FILE`: alias mapping file path (default `.aoe-team/telegram_chat_aliases.json`)
- `AOE_CONFIRM_TTL_SEC`: high-risk auto-run confirmation TTL seconds (default 300)
- `AOE_ROOM_RETENTION_DAYS`: room logs retention in days (default 14, 0 disables)
- `AOE_ROOM_AUTOPUBLISH`: auto-post key TF events into current room (default 1)
- `AOE_ROOM_AUTOPUBLISH_ROUTE`: autopublish routing when room is `global` (room|project|project-tf|tf, default project)
- `AOE_CHAT_MAX_RUNNING`: per-chat concurrent running task limit (default 2, 0 disables)
- `AOE_CHAT_DAILY_CAP`: per-chat daily task creation limit (default 40, 0 disables)
- `AOE_DEFAULT_REPORT_LEVEL`: report verbosity default (short|normal|long), overridden by chat `/report` setting
- `AOE_TF_EXEC_MODE`: TF 실행 워크스페이스 모드 (`worktree`|`inplace`|`none`, default `worktree`)
- `AOE_TF_WORK_ROOT`: worktree 모드에서 TF workdir 생성 루트 (default: `<project_root_parent>/.aoe-tf/`)
- `AOE_TF_ARTIFACT_POLICY`: TF 실행 아티팩트/번들 보존 정책 (`success-only`|`all`|`none`, default `success-only`)
- `AOE_TF_EXEC_CACHE_TTL_HOURS`: success TF exec cache TTL in hours (default 72, 0 disables; ignored when `AOE_TF_ARTIFACT_POLICY=all`)
- `AOE_TF_DOC_MODE`: TF 문서 스캐폴드 모드 (`single`|`legacy`, default `single`)  
  `single`은 TF당 `report.md` 1장만 유지하고, `legacy`는 `ongoing/note/handoff` 스캐폴드를 유지

## 9. Command Delta
- `/cancel` : pending mode 해제
- `/pick [번호|label]` : 현재 task 포커스 지정 (빈칸이면 최근 목록 + 버튼 제공)
- `/cancel <task>` : 실행 중 요청 cancel 시도
- `/ok` : 고위험 자동실행 확인 후 진행
- `/retry <task>` : 동일 prompt/roles로 재실행
- `/replan <task>` : planner/critic를 다시 생성해 재실행
- `/queue` : (Mother-Orch) 전체 프로젝트 todo 큐 요약 보기
- `/sync [all|O#|name]` : (Mother-Orch) 각 프로젝트의 `.aoe-team/AOE_TODO.md`를 todo 큐에 반영(추가/업데이트/완료)
- `/sync recent [O#|name|all] [N]` : (Mother-Orch) 프로젝트 루트의 **최근 문서 N개(기본 3)** 를 스캔해 todo 후보를 추출 후 큐에 반영
- `/next` : (Mother-Orch) 전체 프로젝트에서 다음 실행 가능한 Todo를 선택해 dispatch(TF)로 실행
- `/orch pause <O#|name> [reason]` : 프로젝트 일시정지(글로벌 스케줄러에서 기본 제외)
- `/orch resume <O#|name>` : 프로젝트 일시정지 해제
- `/fanout [N] [force]` : (Mother-Orch) **프로젝트별로 1개씩** `/todo next`를 실행(순차 wave)
- `/drain [N] [force]` : (Mother-Orch) `/next`를 N회 반복 실행(기본 10회, 최대 50회)
- `/auto [on|off|status]` : (Mother-Orch) tmux 백그라운드 스케줄러로 `/next`(또는 `fanout`)를 주기적으로 실행(게이트웨이 polling을 블록하지 않음)
- `/auto on fanout recent` : (off-desk 권장) idle 상태에서 `/sync recent all quiet`를 1회 실행해 큐를 시드(seed)한 뒤, `/fanout` 스케줄링을 계속
- `/offdesk [on|off|status]` : off-desk 프리셋(입력 최소화). on은 `report=short` + `routing=off` + `room=global` + `auto=fanout recent`를 한 번에 적용
- `/panic [status]` : (긴급) auto/offdesk 즉시 중지 + pending/confirm 정리 + routing off
- `/todo` : 프로젝트 Todo(backlog) 조회
- `/todo add [P1|P2|P3] <summary>` : Todo 추가
- `/todo done <TODO-xxx|number>` : Todo 완료 처리
- `/todo next` : 다음 open Todo를 선택해 dispatch(TF)로 실행
- `/whoami` : 현재 chat 권한/allowlist 확인
- `/mode [on|off|direct]` : 기본 평문 라우팅 모드 설정/조회 (`/on`, `/off` 단축 지원, `/off`는 one-shot pending도 해제)
- `/report [short|normal|long|off]` : 결과/보고 응답 길이(요약/기본/상세) 설정/조회
- `/replay [list|latest|<idx>|<id>|show <idx|id|latest>|purge]` : 핸들러 오류 입력 큐 조회/상세/재실행/정리
- readonly chat: `/replay list|show`만 허용, `/replay <id|idx|latest>`와 `/replay purge`는 거부
- readonly chat: `/todo`는 조회만 가능(`/todo add|done` 거부)
- `/lockme` : 현재 chat으로 allowlist 잠금
- `/onlyme` : `/lockme` + owner-only(1:1 private DM gate) 활성화
- `/acl` : ACL(allow/admin/readonly) 요약 확인
- `/status`, `/kpi` : run-loop poll 카운터(`acked/handled/duplicates/unauthorized/empty/handler_errors/failed_queue_total/last_failed_at`) 포함 (`/status`는 `active_tf_count`도 포함)

pause/resume 규칙:

- pause된 프로젝트는 `/next`, `/fanout`, `/auto`, `/offdesk`에서 기본적으로 제외된다.
- `force`를 주면(예: `/fanout force`) pause를 무시하고 포함한다.
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
- tmux visualization/switch:
- `telegram_tmux.sh overview` : numeric session map
- `telegram_tmux.sh ui` : status bar map + `Alt+1..9`, `Alt+,`/`Alt+.` and `Prefix(C-b)+1..9` keybinding refresh
- `telegram_tmux.sh page <next|prev|set N|status|reset>` : page 제어
- `telegram_tmux.sh switch <idx|session>` : direct attach/switch
- global aliases:
- `aoe-team-stack ...`
- `aoe-team-tmux ...`
- Telegram orch alias:
- `/map` 으로 `O1..` 프로젝트 별칭 확인
- `/monitor O1`, `/kpi O1`, `/use O1` (또는 `aoe orch use O1`) 형태로 대상 지정 가능

## 10. Troubleshooting: tmux `returned N` 스팸
- 증상: tmux 상태바에 아래 같은 메시지가 계속 뜸.
- `'/path/to/telegram_tmux.sh refresh --quiet' returned 1`
- `'TMUX_SWITCH=1 ... telegram_tmux.sh switch 2' returned 2`
- 원인: tmux `run-shell`(훅/키바인딩)이 실행한 커맨드가 **0이 아닌 종료코드(1/2)** 로 끝나면 tmux가 그대로 `returned N`을 상태바에 출력함.
- 단기 조치:
- tmux UI/훅/키바인딩을 재적용: `aoe-team-stack ui` (또는 `.aoe-team/telegram_tmux.sh ui`)
- 화면만 꼬였으면 리드로우: `Ctrl-b r`
- 상태바에 `[0/xxxx]`가 보이면 copy-mode일 수 있으니 `q`로 탈출
- 확인/디버깅:
- 현재 훅 확인: `tmux show-hooks -g | rg telegram_tmux.sh`
- 현재 키바인딩 확인: `tmux list-keys | rg telegram_tmux.sh`
- 최근 tmux 메시지 확인: `tmux show-messages | tail -n 30`
- 예방(구현 원칙):
- tmux 훅에서 호출되는 `refresh --quiet`는 **항상 exit 0**(best-effort)로 끝나야 함.
- tmux 키바인딩(`switch`, `page`)도 tmux 컨텍스트에서는 실패하더라도 **exit 0**으로 마무리하고, 필요한 경우 `tmux display-message`로만 사용자에게 알림.
