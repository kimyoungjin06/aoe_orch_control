# ARCHITECTURE

## 1. Purpose

`aoe_orch_control` is a stateful orchestration gateway for multi-project AOE operations.

The system has two planes:

- control plane: Telegram + `aoe-team-stack` + tmux operator surface
- execution plane: Orchestrator/worker sessions, TF workdirs, runtime queue/task state

The core design goal is:

- package assets stay versioned and deployable
- runtime state stays local, mutable, and disposable

## 2. Deployment Boundary

### 2.1 Versioned package assets

These paths are part of the deployable package and should be tracked in git:

- `scripts/`
- `templates/`
- `docs/`
- `tests/`
- `systemd/`

Important entrypoints:

- `scripts/team/aoe-team-stack.sh`
- `scripts/team/runtime/telegram_tmux.sh`
- `scripts/team/runtime/worker_codex_handler.sh`
- `scripts/gateway/aoe-telegram-gateway.py`

Package-relative path resolution is centralized in:

- `scripts/gateway/aoe_tg_package_paths.py`

### 2.2 Generated runtime state

`.aoe-team/` is an active runtime directory, not a package source directory.

Typical local-only runtime artifacts:

- `.aoe-team/orch_manager_state.json`
- `.aoe-team/auto_scheduler.json`
- `.aoe-team/tf_exec_map.json`
- `.aoe-team/telegram_gateway_state.json`
- `.aoe-team/logs/`
- `.aoe-team/messages/`
- `.aoe-team/tf_runs/`
- `.aoe-team/team.json`
- `.aoe-team/orchestrator.json`
- `.aoe-team/workers/*.json`
- `.aoe-team/agents/*/AGENTS.md`

Generated compatibility shims may exist in `.aoe-team/`, but they are runtime conveniences only:

- `.aoe-team/telegram_tmux.sh`
- `.aoe-team/telegram_stack.sh`
- `.aoe-team/worker_codex_handler.sh`

These shims forward to package-managed scripts under `scripts/team/`.

### 2.3 Bootstrap path

Runtime initialization is done by:

- `scripts/team/bootstrap_runtime_templates.sh`
- `aoe-team-stack --project-root <path> init`

Versioned defaults come from:

- `templates/aoe-team/`

## 3. Public Entrypoints

### 3.1 Stack control

Primary stack launcher:

- `scripts/team/aoe-team-stack.sh`

Responsibilities:

- resolve target project root
- derive `AOE_TEAM_DIR`
- delegate to `scripts/team/runtime/telegram_tmux.sh`

### 3.2 Gateway

Primary gateway process:

- `scripts/gateway/aoe-telegram-gateway.py`

Responsibilities:

- Telegram polling and send loop
- request intake
- state load/save
- command dispatch
- TF runtime coordination

### 3.3 Systemd

User service entrypoint:

- `systemd/user/aoe-telegram-stack.service.template`

Operational wrappers:

- `scripts/systemd/install_user_services.sh`
- `scripts/systemd/aoe-systemd-heal.sh`

## 4. Runtime State Model

The system is state-machine driven. The primary source of runtime truth is:

- `.aoe-team/orch_manager_state.json`

This registry stores:

- project registry
- active/focus state
- todo queue
- todo proposals
- task lifecycle state
- chat routing/session metadata
- pending confirmation and locks

Secondary runtime state:

- `.aoe-team/auto_scheduler.json`
  - auto/offdesk scheduler mode and counters
- `.aoe-team/tf_exec_map.json`
  - request-to-workdir/run-dir mapping
- `.aoe-team/telegram_gateway_state.json`
  - Telegram polling offsets, dedupe, failed queue
- `.aoe-team/logs/gateway_events.jsonl`
  - structured operator/event log

## 5. Gateway Layering

The gateway is evolving from a large monolith into a modular monolith with explicit policy/state/view modules.

### 5.1 Intake and command resolution

Input parsing and normalization:

- `scripts/gateway/aoe_tg_parse.py`
- `scripts/gateway/aoe_tg_command_resolver.py`

Command routing:

- `scripts/gateway/aoe_tg_command_handlers.py`

### 5.2 Run pipeline

Natural-language execution requests are handled by:

- `scripts/gateway/aoe_tg_run_handlers.py`

This layer covers:

- planner / critic / repair progression
- dispatch to TF workers
- verifier/critic result handling
- proposal capture from execution results

### 5.3 Backlog and scheduling pipeline

Todo discovery, sync, queue operations, and offdesk scheduling are handled by:

- `scripts/gateway/aoe_tg_scheduler_handlers.py`

This layer covers:

- `/sync`
- salvage/bootstrap
- replace/prune
- `/queue`
- `/next`
- `/fanout`

### 5.4 Management and orchestration UX

Operator-facing management flows are handled by:

- `scripts/gateway/aoe_tg_management_handlers.py`
- `scripts/gateway/aoe_tg_orch_overview_handlers.py`
- `scripts/gateway/aoe_tg_orch_task_handlers.py`
- `scripts/gateway/aoe_tg_todo_handlers.py`

This layer covers:

- `/map`
- `/offdesk`
- `/auto`
- `/orch ...`
- `/todo ...`

## 6. Policy / State / View Modules

Recent refactoring introduced explicit domain modules. These are the intended stable seams.

### 6.1 Policy

- `scripts/gateway/aoe_tg_ops_policy.py`
  - visible/schedulable project scope
  - queue selection helpers
- `scripts/gateway/aoe_tg_todo_policy.py`
  - proposal acceptance and canonical syncback rules
- `scripts/gateway/aoe_tg_schema.py`
  - planner/critic schema normalization

### 6.2 State

- `scripts/gateway/aoe_tg_task_state.py`
  - task store, aliasing, lifecycle sync
- `scripts/gateway/aoe_tg_todo_state.py`
  - todo proposals, syncback plan/apply, todo mutation helpers
- `scripts/gateway/aoe_tg_blocked_state.py`
  - blocked/manual_followup state transitions

### 6.3 View

- `scripts/gateway/aoe_tg_task_view.py`
  - task display and lifecycle summary
- `scripts/gateway/aoe_tg_ops_view.py`
  - compact ops/offdesk rendering

## 7. Execution Model

### 7.1 Mother-Orch

The tmux stack is the operator runtime for the control plane.

Core sessions:

- gateway session
- request-scoped TF worker sessions
- optional operator/overview surfaces

### 7.2 Request-scoped TF execution

TF execution is request-scoped, not permanent-role-scoped.

Key runtime artifacts:

- `.aoe-team/tf_exec_map.json`
- `.aoe-team/tf_runs/<request_id>/`

The intended lifecycle is:

1. intake request
2. plan/critic/repair
3. create request-scoped worker session
4. execute in project-aware workdir
5. collect result
6. update todo/task state
7. keep only the minimal report/state needed for operations

## 8. Sync Model

There are four backlog-related layers:

1. canonical document backlog
   - project `TODO.md`
   - optional `AOE_TODO.md` include routing
2. runtime todo queue
   - active execution queue in `orch_manager_state.json`
3. proposal inbox
   - follow-up candidates created by TF or salvage
4. syncback plan
   - controlled projection of runtime state back into canonical `TODO.md`

Design rule:

- TF does not directly rewrite canonical todo documents
- TF proposes, runtime accepts/rejects, syncback applies later

## 9. Current Structural Assessment

The architecture is no longer a flat script pile, but it is still a modular monolith.

Stable improvements already in place:

- package/runtime separation
- policy/state/view extraction
- offdesk preflight/review flow
- proposal inbox and syncback path
- blocked/manual_followup state normalization

Remaining large cores:

- `scripts/gateway/aoe-telegram-gateway.py`
- `scripts/gateway/aoe_tg_scheduler_handlers.py`
- `scripts/gateway/aoe_tg_run_handlers.py`

The current engineering rule is:

- new behavior should be added to policy/state/view/helper modules first
- large handler files should keep orchestration glue, not domain rules

## 10. Operational Rules

For deployment and maintenance:

- treat `scripts/`, `templates/`, `docs/`, `tests/`, `systemd/` as package-owned
- treat `.aoe-team/` as environment-local runtime
- never commit runtime secrets or mutable state
- prefer `aoe-team-stack` as the public control surface
- regenerate runtime files from templates/bootstrap instead of versioning local mutations

## 11. Related Docs

- `README.md`
- `docs/COMMANDS.md`
- `docs/RUNBOOK.md`
- `docs/CONSTITUTION.md`
- `docs/PROJECT_CHARTER.md`
