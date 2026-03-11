# AUTOGEN CORE SANDBOX PILOT

## 1. Purpose

This document defines the first allowed `autogen_core` pilot scope for `aoe_orch_control`.

The pilot is intentionally narrow.
It exists to validate:

- runtime event mirroring
- proposal output parity
- operator visibility and rollback behavior

It does not authorize a repo-wide backend switch.

## 2. Non-Goals

The sandbox pilot is not for:

- replacing the Telegram control plane
- replacing runtime queue / proposal / syncback ownership
- enabling global `offdesk` fanout on `autogen_core`
- enabling write-capable TF by default
- enabling all projects or all roles

## 3. Allowed Scope

The first pilot may run only when all of the following are true.

### Allowed project scope

- exactly one sandbox project or one sandbox TF profile
- no default/global backend flip
- no hidden automatic rollout across all orch projects

### Allowed workload types

- `review`
- `research synthesis`
- `read-only structured analysis`
- `writer/report handoff`

### Disallowed workload types

- canonical `TODO.md` mutation
- `syncback` mutation
- repository write tasks
- branch/worktree mutation that changes project files
- destructive shell operations
- unattended `offdesk on fanout` using `autogen_core`

## 4. Ownership Boundary

The following always remain repo-owned.

- Telegram input handling
- project registry / alias / focus / lock
- runtime todo queue
- proposal inbox
- canonical `TODO.md` syncback
- offdesk / auto scheduling
- final backlog mutation policy

The following may be backend-owned inside the pilot.

- intra-TF message passing
- role-to-role handoff
- runtime-local reviewer / critic coordination
- backend-local artifact packaging

## 5. Entry Criteria

Do not start the sandbox pilot until all of the following are true.

1. `local` and `autogen_core` experiment contracts match for:
- request shape
- output contract
- runtime event contract
- proposal contract

2. `autogen_core_compare.py --format markdown` shows:
- `request_shape_match_cases == benchmark_count`
- `output_contract_match_cases == benchmark_count`
- `runtime_event_contract_cases == benchmark_count`
- `proposal_contract_cases == benchmark_count`

3. A backend runtime event envelope exists and is stable.
- current module: `scripts/gateway/aoe_tg_tf_event_schema.py`

4. Failure behavior is explicit.
- backend failure is operator-visible
- no silent queue mutation
- no silent todo/proposal mutation

5. Rollback is immediate.
- pilot backend can be disabled without touching canonical runtime state

## 6. Exit Criteria

A pilot run is considered acceptable only if all of the following hold.

1. Operator can see the full TF path.
- request accepted
- roles resolved
- runtime started/completed
- verdict emitted
- proposals emitted

2. Follow-up proposals are normalized into the repo schema.
- required proposal fields present
- confidence within expected range
- no backend-owned backlog mutation

3. Result handling remains operator-safe.
- failure surfaces in the same control plane
- no orphan state in runtime queue
- no hidden side effects in project files

4. The produced verdict is usable in current operator workflows.
- `/task`
- `/queue`
- `/todo proposals`
- `/monitor`

## 7. Stop Conditions

Stop the pilot immediately if any of the following occur.

- backend emits non-normalized event rows
- backend emits non-normalized proposals
- backend mutates backlog state directly
- backend requires changes to top-level Telegram or offdesk logic
- operator cannot explain the runtime outcome from mirrored events
- sandbox run produces unexpected file mutations

## 8. Required Evidence

Each pilot run should capture:

- benchmark/task id
- backend name
- runtime event trace
- verdict
- artifacts summary
- follow-up proposals
- operator assessment
  - usable / noisy / unsafe

Store this evidence outside the live control plane until the pilot is accepted.

## 9. Recommended Pilot Sequence

1. Run `local` and `autogen_core` through the experiment compare harness.
2. Verify contract parity remains green.
3. Enable `autogen_core` only for one sandbox TF profile.
4. Run one read-only benchmark task.
5. Review event trace and proposals manually.
6. If stable, expand to another read-only task type.
7. Do not allow write-capable pilot until event mirroring and artifact review are proven.

## 10. Next Technical Gate

Completed technical gates:

- sandbox wiring for backend runtime event mirroring into existing gateway/operator logs
- controlled backend selection for one sandbox TF profile only
- read-only sandbox runner over canonical `TODO.md`
- one TwinPaper sandbox pilot with mirrored project/root event traces

Next technical milestone:

- run a second read-only pilot on another task shape before any broader pilot
