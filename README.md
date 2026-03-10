# aoe_orch_control

Telegram-controlled orchestration workspace for multi-session AOE operations.

## Source Attribution
- Base project (fork/upstream): `njbrake/agent-of-empires`
- Upstream repository: `https://github.com/njbrake/agent-of-empires`
- Local extensions include Telegram bot control flow, task alias/monitor UX, and orchestrator-worker automation.

## Project Intent
- Why: 자동 오케스트레이션으로 야간/비근무 시간에도 태스크를 안정적으로 수행한다.
- How: Orchestrator가 계획/배정을 관리하고, 역할별 Sub-session이 실행/검증을 분리 수행한다.
- Control Plane: 사용자는 Telegram 자연어 명령으로 프로젝트별 Orch를 원격 제어한다.

## Governance Docs
- Constitution (highest level): `docs/CONSTITUTION.md`
- Constitution trace roadmap: `docs/ROADMAP.md`
- Command reference: `docs/COMMANDS.md`
- Multi-agent investigations workspace: `docs/investigations_mo/README.md`
- Multi-agent investigations template: `docs/templates/multi_agent_tf_ops_template/README.md`
- Project charter: `docs/PROJECT_CHARTER.md`
- Fork policy: `docs/FORK_POLICY.md`
- Upstream baseline: `docs/UPSTREAM_BASELINE.md`
- Runbook: `docs/RUNBOOK.md`
- Daily checklist: `docs/DAILY_CHECKLIST.md`
- Systemd user setup: `docs/SYSTEMD_USER_SETUP.md`

## Local Runtime
- Project root: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control`
- Team directory: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team`
- Gateway source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe-telegram-gateway.py`
- ACL module source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_acl.py`
- Parse module source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_parse.py`
- Command resolver source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_command_resolver.py`
- Command handlers source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_command_handlers.py`
- Management handlers source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_management_handlers.py`
- Orch overview handlers source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_orch_overview_handlers.py`
- Orch task handlers source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_orch_task_handlers.py`
- Retry handlers source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_retry_handlers.py`
- Role handlers source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_role_handlers.py`
- Message flow source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_message_flow.py`
- Run handlers source: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/aoe_tg_run_handlers.py`
- Runtime link install: `bash /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/gateway/install_runtime.sh`
- Global launcher install: `bash /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/team/install_global_cli.sh`
- Runtime init (template bootstrap): `aoe-team-stack --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control init`
- Start stack: `aoe-team-stack --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control start`
- Stop stack: `aoe-team-stack --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control stop`
- Session overview (number map): `aoe-team-stack --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control overview`
- Apply tmux visual/key UI: `aoe-team-stack --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control ui`
- tmux page size env: `AOE_TMUX_PAGE_SIZE=<N>` (range: `1..9`, default: `9`)
- tmux hint name width env: `AOE_TMUX_HINT_NAME_MAX=<N>` (default: `7`)
- tmux compact display name width env (overview): `AOE_TMUX_COMPACT_NAME_MAX=<N>` (default: `20`)
- Session naming envs (default + legacy compatibility):
- `AOE_TMUX_GATEWAY_SESSION=aoe_mo_gateway` (legacy auto-detect: `aoe_tg_gateway`)
- `AOE_TMUX_WORKER_PREFIX=aoe_tf_worker_` (legacy auto-detect: `aoe_tg_worker_`)
- Fast switch by index/session: `aoe-team-stack --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control switch 2`
- tmux page control: `aoe-team-stack --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control page next|prev|set <N>|status|reset`
- Systemd install: `/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/scripts/systemd/install_user_services.sh`
- Telegram input policy: slash-first (`/dispatch`, `/direct`, `/mode`, `/monitor`, `/check`, `/task`, `/pick`, `/kpi`, `/map`, `/cancel`, `/retry`, `/replan`, `/replay`, `/help`, `/whoami`, `/acl`, `/grant`, `/revoke`)
- Access policy: deny-by-default + ACL envs (`TELEGRAM_ALLOW_CHAT_IDS`, `TELEGRAM_ADMIN_CHAT_IDS`, `TELEGRAM_READONLY_CHAT_IDS`)
- Owner policy: set `TELEGRAM_OWNER_CHAT_ID` to enforce owner-only control for `/lockme`, `/grant`, `/revoke`
- Chat alias mapping: short numeric aliases (`1..999`) persisted at `.aoe-team/telegram_chat_aliases.json` and usable in `/grant`/`/revoke`
- Orch alias mapping: project aliases (`O1..O999`) auto-assigned in `.aoe-team/orch_manager_state.json`, visible via `/map`, usable as orch target (`/monitor O1`)
- Persistent routing mode: `/mode on|off|direct` + shortcuts `/on`, `/off` (`on/direct`면 slash-only에서도 평문 자동 라우팅)
- Auto-run safety: high-risk plain-text auto-run requires `/ok` confirmation (`/cancel` to discard)
- Chat guardrails: per-chat running limit + daily cap (`AOE_CHAT_MAX_RUNNING`, `AOE_CHAT_DAILY_CAP`)
- Handler error replay queue: `/replay [list|latest|<idx>|<id>|show <idx|id|latest>|purge]` and keep-limit env `AOE_GATEWAY_FAILED_KEEP`
- Replay queue TTL env: `AOE_GATEWAY_FAILED_TTL_HOURS` (default `168`, `0` disables expiry)
- First-time lock: send `/lockme` (resets allowlist to current chat and clears admin/readonly), verify with `/whoami`
- Safe natural shortcuts (slash-only mode): `모니터 5`, `확인 1`, `상태 1`, `재시도 1`, `재계획 1`, `취소 1`
- tmux quick switch: `Alt+1..9` or `Ctrl+b` then `1..9`, page move `Alt+,`/`Alt+.` (status bar and `overview` show `Pn/N`)

## Global Commands
- After one-time install, use from any directory:
- `aoe-team-stack init|start|stop|restart|status`
- `aoe-team-stack overview|ui|page`
- `aoe-team-stack switch <idx|session>`
- `aoe-team-stack page next|prev|set <N>|status|reset`
- alias: `aoe-team-tmux ...` (same launcher)
- multi-project mode: `aoe-team-stack --project-root /path/to/project start`
- uninstall: `bash scripts/team/uninstall_global_cli.sh`
- note: `aoe` core behavior remains upstream. `aoe-team` is upstream team-protocol CLI; stack UI control is `aoe-team-stack`.

## Runtime Boundary
- `.aoe-team` is an active runtime directory.
- versioned launcher/worker assets live under `scripts/team/`; `.aoe-team/*.sh` are generated compatibility shims only.
- The following mutable runtime files are intentionally untracked:
- `.aoe-team/team.json`
- `.aoe-team/orchestrator.json`
- `.aoe-team/workers/*.json`
- `.aoe-team/agents/*/AGENTS.md`
- Versioned defaults are stored in `templates/aoe-team/`.
- Bootstrap missing runtime files from templates:
- `bash scripts/team/bootstrap_runtime_templates.sh --project-root /home/kimyoungjin06/Desktop/Workspace/aoe_orch_control`
- Keep service code/docs in repository paths (`scripts/`, `docs/`, `.github/`) and treat `.aoe-team` state as environment-local.

## Tests
- Pytest gateway regression: `scripts/gateway_pytest.sh`
- Direct invoke: `uv run --with pytest pytest -q tests/gateway/test_gateway_cli.py`
- Smoke subset wrapper: `scripts/gateway_smoke_test.sh` (runs `-m smoke`)
- Error subset wrapper: `scripts/gateway_error_test.sh` (runs `-m error`)
- CI workflow: `.github/workflows/gateway-tests.yml` (`smoke`/`error` 병렬 matrix job)
