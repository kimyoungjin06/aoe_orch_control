# ORCH CORE CONTRACT

## 1. Purpose

This document fixes the minimum contract between:

- project-level Orch
- task-scoped TF planner
- TF workers / critic / verifier
- backlog proposal capture
- backend runtime event mirroring

The goal is to prevent hidden contract drift across:

- manual operator runs
- `/next` and off-desk scheduling
- backend experiments such as `autogen_core`

The contract is implemented in:

- `scripts/gateway/aoe_tg_orch_contract.py`

The runtime event envelope remains shared with:

- `scripts/gateway/aoe_tg_tf_event_schema.py`

Mother-Orch control-plane actions are defined separately in:

- `scripts/gateway/aoe_tg_orch_actions.py`
- `docs/MOTHER_ORCH_ACTION_API.md`

## 2. Contract Objects

### 2.1 OrchTaskSpec

This is the only task input Orch should hand to a TF planner.

Required fields:

- `task_id`
- `project_key`
- `title`
- `objective`
- `priority`
- `source`
- `readonly`
- `approval_mode`
- `requested_roles`
- `acceptance_criteria`
- `retry_budget`
- `status`

Operational meaning:

- describes *what* the TF is supposed to achieve
- references backlog lineage, but does not mutate backlog directly
- keeps backend/profile hints advisory, not authoritative

Important policy:

- TF may read `source_ref`
- TF must not rewrite queue state directly
- TF should emit proposals instead of creating backlog rows

### 2.2 TFPlan

This is the planner-owned execution contract.

Required fields:

- `status`
- `summary`
- `strategy`
- `assignments`
- `execution_order`
- `critic`
- `evidence_required`
- `blocking_issues`

States:

- `draft`
- `ready`
- `blocked`

Important policy:

- `blocked` must always explain why it cannot proceed
- `ready` must name a critic role and minimum evidence requirements
- role assignments are role-scoped, not backend-scoped
- `meta.phase2_team_spec` must make Phase2 execution lanes and critic lanes explicit

### 2.2.1 Phase2TeamSpec

This is the execution-facing team contract derived from the plan after Phase1 stabilizes.

Required fields:

- `execution_mode`
- `execution_groups`
- `review_mode`
- `review_groups`
- `team_roles`
- `critic_role`
- `integration_role`

Operational meaning:

- `execution_groups` define parallel execution lanes for Phase2
- `review_groups` define parallel critic/verifier lanes
- `team_roles` is the total staffing footprint for the TF
- `critic_role` and `integration_role` define who closes the loop after execution

Important policy:

- execution lanes should default to parallel when more than one role owns subtasks
- review lanes should only be populated after verifier policy is resolved
- planners must not leave Phase2 staffing implicit

### 2.3 TFRoleAssignment

Each plan expands into one or more role-scoped assignments.

Required fields:

- `role`
- `kind`
- `goal`
- `deliverable`
- `acceptance`

Kinds:

- `planner`
- `worker`
- `critic`
- `verifier`
- `writer`
- `analyst`
- `reviewer`
- `engineer`

Important policy:

- backend routing happens after assignment
- a single backend may fulfill multiple assignments

### 2.4 TFVerdict

This is the terminal outcome of a TF run.

Required fields:

- `status`
- `action`
- `summary`
- `reason`
- `attempt`
- `max_attempts`
- `manual_followup`
- `retry_hint`
- `evidence`
- `artifacts`

Statuses:

- `success`
- `retry`
- `fail`
- `intervention`

Actions:

- `none`
- `retry`
- `replan`
- `escalate`
- `abort`

Important policy:

- verdict describes execution outcome
- verdict does not mutate canonical backlog
- follow-up work must be emitted as proposal output

### 2.5 FollowupProposal

This is the only safe way for TF to suggest new backlog work.

Required fields:

- `summary`
- `priority`
- `kind`
- `reason`
- `source_request_id`
- `source_todo_id`
- `confidence`
- `source_tf_id`
- `owner_role`
- `acceptance`

Kinds:

- `followup`
- `handoff`
- `risk`
- `debt`

Important policy:

- high-confidence proposals may later be accepted into queue
- medium-confidence proposals stay in inbox until operator review
- TF never writes canonical `TODO.md` directly

### 2.6 RuntimeEvent

This uses the shared normalized runtime event envelope from:

- `scripts/gateway/aoe_tg_tf_event_schema.py`

Orch-level rule:

- all TF backends, local or experimental, must emit the same envelope
- backend-specific payload stays inside `payload`
- operator logs mirror this envelope into project/root gateway logs

## 3. TF Lifecycle

The lifecycle is split into two major phases.

### 3.1 Phase1: planning

Default policy:

- TF is the default execution path for real work
- planning is mandatory before execution
- Phase1 runs an ensemble planner loop
- target loop count is at least `3`
- Codex and Claude receive the same planning mission each round
- each round shares criticism and improved plan candidates back into the next round

The output of Phase1 is:

- one stable `TFPlan`
- critic issues reduced to a dispatchable level
- explicit execution team shape for Phase2

### 3.2 Phase2: execution

Only after Phase1 is stable does execution begin.

Default policy:

- execution team is assembled from the Phase1 plan
- independent work proceeds in parallel
- critic/verifier review should also be parallel where possible
- terminal outcome is captured as `TFVerdict`
- any new work enters via `FollowupProposal`, not direct backlog mutation

The intended lifecycle states are:

1. `queued`
2. `planning`
3. `running`
4. `critic_review`
5. `needs_retry`
6. `manual_intervention`
7. `completed`
8. `archived`

Current implementation is not yet a single explicit state machine, but Phase1/Phase2 is now the required conceptual boundary.

Current runtime field:

- `tf_phase`
- `lane_states`

Current mapping rule:

- `plan_gate blocked` -> `blocked`
- `planning in progress` -> `planning`
- `execution/staffing active` -> `running`
- `verification/integration active` -> `critic_review`
- `exec_critic retry` -> `needs_retry`
- `exec_critic fail/intervention` -> `manual_intervention`
- terminal success -> `completed`

Current lane-state rule:

- `execution lane` status is derived from role execution status
- `review lane` status is derived from dependency completion first, then reviewer status
- unresolved review dependencies surface as `waiting_on_dependencies`
- `review lane` may also carry `verdict/action/reason` from `exec_critic`
- `review_verdicts` are summarized separately from lane completion status
- `exec_critic` may also carry lane-target metadata:
  - `rerun_execution_lane_ids`
  - `rerun_review_lane_ids`
  - `manual_followup_execution_lane_ids`
  - `manual_followup_review_lane_ids`

Current local backend staging rule:

- if `execution_lanes` and `review_lanes` both exist and the request is not `no_wait`,
  local TF execution may run `execution -> review` as two linked requests
- linked request ids are preserved for reply-finalization and operator audit
- staged execution also records:
  - `phase2_request_ids`
  - `phase2_review_triggered`
  - `phase2_review_skipped_reason`

This contract exists so the next phase can move `run_handlers` toward that state machine
without re-inventing payload shapes.

## 4. Backlog Ownership Rules

Backlog ownership is intentionally split.

### Owned by Orch / repo state

- runtime queue
- todo proposal inbox
- syncback plan/apply
- manual follow-up escalation

### Owned by TF

- plan
- verdict
- runtime events
- follow-up proposals

### Forbidden for TF

- direct canonical `TODO.md` mutation
- direct queue mutation
- direct syncback execution

## 5. Provider Boundary

Provider/backend choice is intentionally outside the core contract.

The contract should survive:

- local Codex worker execution
- local Claude worker execution
- sandbox `autogen_core`

That is why role assignments are role-scoped and runtime events are backend-neutral.

## 6. Immediate Next Step

This contract is Phase 1 only.

Next implementation phase:

- build a real TF lifecycle engine on top of these schemas
- move `run_handlers` orchestration glue toward explicit lifecycle transitions
