# AUTOGEN CORE ADOPTION

## 1. Decision

Decision:

- adopt `AutoGen Core` only as an optional `TF execution backend`
- do not replace the current Telegram/control-plane/runtime-state architecture
- do not move canonical todo/proposal/syncback ownership into AutoGen

Current verdict:

- `recommended for controlled backend experimentation`
- `not recommended as a top-level orchestration rewrite`

## 2. Why This Decision

This repository already owns the higher-level orchestration model:

- Telegram control plane
- project registry / focus / lock
- runtime queue and todo proposals
- sync / salvage / syncback
- offdesk / auto scheduling
- tmux/request-scoped worker runtime

These responsibilities are already explicit in:

- `docs/ARCHITECTURE.md`
- `scripts/gateway/aoe_tg_scheduler_handlers.py`
- `scripts/gateway/aoe_tg_todo_state.py`
- `scripts/gateway/aoe_tg_scheduler_control_handlers.py`

Replacing that layer with another framework would create duplicated ownership.

The missing piece is narrower:

- richer multi-agent coordination inside a single TF run
- message-driven specialist collaboration inside one execution attempt

That is the seam where `AutoGen Core` fits.

## 3. What AutoGen Core Is Good At

Based on the official AutoGen Core documentation, the framework is strongest at:

- routed agents
- runtime-managed messaging
- direct message and publish/subscribe patterns
- intervention middleware
- event-driven multi-agent coordination

Relevant official references:

- https://github.com/microsoft/autogen
- https://microsoft.github.io/autogen/dev/user-guide/core-user-guide/index.html
- https://microsoft.github.io/autogen/dev/user-guide/core-user-guide/framework/agent-and-agent-runtime.html
- https://microsoft.github.io/autogen/dev/user-guide/core-user-guide/framework/message-and-communication.html
- https://microsoft.github.io/autogen/dev/user-guide/core-user-guide/framework/topic-and-subscription.html
- https://microsoft.github.io/autogen/dev/user-guide/core-user-guide/framework/intervention-handler.html

This matches the desired `TF team` behavior better than it matches the current
project-level `Mother-Orch` control plane.

## 4. What Must Stay Local

The following responsibilities must remain in `aoe_orch_control`:

- Telegram command parsing and operator UX
- project registry and alias mapping
- project lock / owner / ACL rules
- canonical `TODO.md` sync and salvage policy
- runtime queue state
- todo proposal inbox
- syncback decision and file mutation
- offdesk / auto scheduling and reporting

In short:

- AutoGen can decide `how TF agents talk`
- this repo must continue to decide `what project runs, when it runs, and how backlog state changes`

## 5. Target Integration Boundary

Recommended integration point:

- current local TF backend remains the default
- add a second backend: `backend=autogen_core`

Input to the backend adapter:

- project key
- task/request id
- task summary
- role set
- workspace path
- retry budget
- approval/verifier flags

Output from the backend adapter:

- terminal verdict
- role-level evidence/artifacts
- normalized follow-up proposals
- structured execution event log

This adapter boundary should sit between:

- `scripts/gateway/aoe_tg_run_handlers.py`
- `scripts/gateway/aoe_tg_exec_pipeline.py`
- `scripts/gateway/aoe_tg_tf_exec.py`

## 6. Mapping Current Concepts To AutoGen

Current repo concept -> AutoGen Core concept:

- project orch -> external supervisor outside AutoGen
- TF run -> one AutoGen runtime session
- worker role (`Reviewer`, `Local-Dev`, `DataEngineer`) -> RoutedAgent type
- Orch request context -> runtime/session bootstrap payload
- critic/verifier gate -> intervention handler or dedicated reviewer agent
- follow-up proposal extraction -> adapter post-processing, not AutoGen-owned backlog mutation

Important boundary:

- `Mother-Orch` and `Project-Orch` remain outside AutoGen
- only the `TF execution cell` becomes AutoGen-managed

## 7. Recommended Phase Plan

### Phase 0. No-runtime design spike

Goal:

- prove that a single TF task can be expressed as an AutoGen runtime without
  touching Telegram or canonical todo state

Deliverable:

- `scripts/experiments/autogen_core_tf_spike.py`
- dry-run JSON output only

Success:

- can simulate `planner -> worker -> reviewer -> verdict`
- can emit normalized follow-up proposals in the repo's existing schema

### Phase 1. Backend adapter

Goal:

- add a backend interface for TF execution

Recommended new seam:

- `scripts/gateway/aoe_tg_tf_backend.py`
- `scripts/gateway/aoe_tg_tf_backend_local.py`
- `scripts/gateway/aoe_tg_tf_backend_autogen.py`

Rules:

- default remains `local`
- backend selection must be explicit and per-run or per-project
- failure in `autogen_core` must fall back cleanly to operator-visible error, not silent state mutation

### Phase 2. Controlled project pilot

Goal:

- enable `autogen_core` only for one sandbox TF profile

Not for:

- offdesk global fanout
- all projects
- high-risk write tasks by default

Good pilot targets:

- research synthesis
- reviewer-only TF
- read-only structured analysis

### Phase 3. Restricted write-capable TF

Goal:

- allow write tasks only after evidence/reporting quality is acceptable

Required before enabling:

- stable role transcript capture
- deterministic artifact path layout
- normalized verdict schema
- proposal extraction parity with the local backend

## 8. Risks

### 8.1 Runtime duplication

AutoGen has its own runtime model.

This repo also has:

- request lifecycle state
- tmux worker/session lifecycle
- TF workspace lifecycle

If AutoGen is allowed to own too much, the system will have two orchestrators.

Mitigation:

- keep AutoGen runtime scoped to one TF execution only

### 8.2 State ownership confusion

If AutoGen writes backlog state directly, the following will diverge:

- `orch_manager_state.json`
- canonical `TODO.md`
- proposal inbox

Mitigation:

- AutoGen may emit proposals
- only repo-owned state/policy modules may mutate backlog state

### 8.3 Observability mismatch

Current operator workflows depend on:

- `/task`
- `/queue`
- `/monitor`
- gateway event logs

Mitigation:

- adapter must mirror AutoGen runtime events into existing gateway logs

### 8.4 Over-automation too early

AutoGen is attractive for team behavior, but the repo is still stabilizing:

- `scripts/gateway/aoe_tg_sync_sources.py`
- `scripts/gateway/aoe_tg_run_handlers.py`

Mitigation:

- use AutoGen only after adapter seam exists
- do not mix framework adoption with backlog-engine rewrites

## 9. Go / No-Go Criteria

Go if:

- we need richer intra-TF agent messaging than the local backend can support
- current backend adapter seam is explicit
- verdict/proposal schema is stable enough to compare outputs

No-Go if:

- the intent is to replace Telegram/offdesk/todo ownership
- the intent is to remove current project/runtime state modules
- the team expects AutoGen to solve scheduler or sync quality problems

## 10. Concrete Recommendation

Recommendation:

1. do not adopt AutoGen Core as a repo-wide control plane
2. build a backend adapter first
3. run one read-only TF spike
4. compare local backend vs AutoGen backend on:
   - verdict quality
   - follow-up proposal quality
   - event trace readability
   - operator recovery behavior

If Phase 0 and Phase 1 succeed, then AutoGen Core becomes a good candidate for
specialized TFs.

If not, keep the current local backend and continue improving:

- `scripts/gateway/aoe_tg_sync_sources.py`
- `scripts/gateway/aoe_tg_run_handlers.py`

## 11. Immediate TODO

- [x] define `TF backend adapter` interface
- [ ] add `backend=local` explicit default to live execution wiring
- [x] add `autogen_core` spike script under `scripts/experiments/`
- [x] add benchmark set and dry-run compare harness for local vs AutoGen contract review
- [ ] define normalized runtime event schema for adapter output
- [ ] define proposal extraction parity test between local and AutoGen backend
