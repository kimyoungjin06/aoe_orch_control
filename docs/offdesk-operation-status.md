# Offdesk Operation Status

This page records the current Forager Offdesk operating baseline and the work
that remains before treating the system as a dependable overnight operator.

Snapshot date: 2026-06-17.

## Current Baseline

Forager now has a generic Offdesk operating loop, a read-only Remote Operator
projection surface, a Telegram transport adapter, project initialization
packets, plan registration/review records, runtime approval gates, background
runner probes, closeout packets, Ondesk return packages, and adaptive wiki
review surfaces.

At this point:

- the default Forager profile is loaded from primary Forager storage;
- the default profile has no pending approvals, queued tasks, active tasks,
  failed tasks, resume-pending tasks, stale background runs, or
  closeout-required tasks;
- the Telegram listener is installed as a user service and reports healthy
  polling state through `scripts/offdesk_remote_operator_telegram.py --health`;
- the Telegram listener can survive known Telegram transport failures and
  record them as loop state instead of exiting;
- `scripts/offdesk_remote_operator_watchdog.py` runs outside the listener,
  reads the loop-status file and systemd service state, and sends rate-limited
  emergency Telegram alerts when remote operation is not currently reliable;
- the local model endpoint configured for the Remote Operator is available;
- Telegram can start and monitor only a reviewed, bound task through
  task-scoped `offdesk tick`, and can create a closeout packet only for that
  completed task; closeout review, accepted truth, arbitrary launch, and shell
  actions remain blocked from Telegram by design;
- `cargo test`, `cargo fmt -- --check`, `cargo clippy -- -D warnings`,
  `mdbook build`, and the website build pass locally.

This baseline proves that the approval-gated runtime path and the remote
operator status path can be inspected consistently. It still does not prove
that every long autonomous run will produce useful project output.

## Verified Generic Flow

The intended flow is:

```text
project init
  -> implementation or plan packet
  -> plan dry run
  -> plan registration
  -> plan review
  -> launch-preparation packet
  -> enqueue dispatch.runtime task
  -> tick creates operator approval
  -> offdesk ok <approval-id>
  -> tick launches supervised runner
  -> poll reconciles heartbeat, log, and result artifacts
  -> closeout packet
  -> closeout review
  -> Ondesk return package
```

The Telegram Remote Operator currently covers the planning, narrowly scoped
runtime, and receipt-bound closeout side of this flow:

- read-only `/status`, `/pending`, `/plans`, and `/show`;
- freeform planning request capture;
- project candidate selection;
- project initialization preview;
- project initialization packet creation;
- bounded plan draft creation;
- plan registration;
- explicit plan-review approval;
- read-only launch-preparation packet creation;
- pending gate request creation for local approval;
- exact pending gate approval resolution;
- bounded execution-brief generation;
- reviewed workload binding;
- bound enqueue run;
- task-scoped runtime start;
- task-scoped runtime monitor/readout;
- completed-task closeout packet creation;
- closeout review handoff creation;
- handoff-bound `approved`, `revise`, or `blocked` closeout verdicts.

It intentionally does not provide broad runtime launch or general process
control. Telegram can start only the reviewed task that was already bound and
queued, then monitor and close out that same task. A closeout verdict is valid
only through the prepared handoff, and accepted truth exists only when the CLI
closeout receipt reports `accepted`.

Remote Operator phase status:

| Phase | Status | Boundary |
| --- | --- | --- |
| Read-only status and pending/plans/show | Implemented | No approvals are resolved. |
| Freeform planning request and project selection | Implemented | Creates planning/session evidence only. |
| Project init preview/run | Implemented | Creates local project operation packets; no runtime work. |
| Plan draft and registration | Implemented | Writes/dry-runs local plan artifacts only. |
| Explicit plan-review approval | Implemented | Records plan review; does not authorize launch. |
| Launch-preparation packet | Implemented | Creates `offdesk_plan_launch_prep.v1`; no gate approval. |
| Gate request | Implemented | Creates pending `dispatch.runtime` approval; does not resolve it. |
| Gate approval resolution | Implemented | Resolves exact matching approval; does not enqueue or launch. |
| Execution brief | Implemented | Writes bounded brief for local enqueue; does not enqueue or launch. |
| Enqueue handoff | Implemented | Writes a local-review command template only; does not enqueue or launch. |
| Workload binding | Implemented | Verifies and binds reviewed `prepared_task.json`; does not enqueue or launch. |
| Enqueue run | Implemented | Runs only bound `offdesk enqueue`; does not launch or tick. |
| Runtime start | Implemented | Runs only task-scoped `offdesk tick`; does not monitor or close out. |
| Runtime monitor/readout | Implemented | Polls only the same task with `offdesk tick --limit 0`; does not dispatch or close out. |
| Closeout packet | Implemented | Generates closeout artifacts only for the completed monitored task; does not review or accept truth. |
| Closeout review handoff | Implemented | Writes local `closeout-review` verdict templates; does not execute verdicts or accept truth. |
| Closeout verdict/accepted-truth bridge | Implemented | Runs only handoff-bound `closeout-review` verdicts; accepted truth follows the CLI receipt. |
| Listener health and external watchdog | Implemented | Reports or alerts; does not mutate Offdesk state. |

## Completed Work

### Runtime Safety

- Provider fallback is approval-gated and does not consume
  `dispatch.runtime` approvals.
- Runtime tasks require scoped `dispatch.runtime` approval before launch.
- Long Python workloads should use `local-tmux` when they need live inspection.
- System-critical operations remain forbidden unless a separate reviewed
  capability and approval path exists.
- Pre-mutation snapshots and dry-run restore plans keep rollback reasoning
  separate from execution.

### Project And Module Scope

- `forager project init` creates read-only project operation packets.
- Project initialization can identify module candidates and selected operation
  targets without mutating the target project.
- `MODULE_OPERATION_PREFLIGHT.json` is a reviewed preflight reference, not a
  hidden runtime permission grant.
- Ondesk prompt packages include concise project/module summaries when matching
  initialization artifacts exist.

### Remote Operator

- Remote projections are read-only and operator-safe.
- Telegram messages are compact enough for mobile scanning and keep detailed
  diagnostics in local health output.
- Telegram freeform messages can become reviewable decision-inbox items or
  planning sessions.
- Telegram can guide project selection, initialization preview/run, plan draft,
  plan registration, explicit plan-review approval, and read-only
  launch-preparation packet creation without launching work.
- Telegram can create a pending `dispatch.runtime` gate approval request from
  a launch-preparation packet without enqueueing or launching work.
- Telegram can approve or deny the exact pending gate approval it created, but
  cannot enqueue, launch, tick, or mark runtime work accepted.
- Telegram can write an `ExecutionBrief` for the exact approved gate context,
  but cannot enqueue, launch, tick, or mark runtime work accepted.
- Telegram can write an enqueue handoff receipt with a local-review command
  template, but cannot enqueue, launch, tick, or mark runtime work accepted.
- Telegram can bind a reviewed `prepared_task.json` to the approved execution
  brief after checking exact project/request/task and brief hash, but cannot
  enqueue, launch, tick, or mark runtime work accepted.
- Telegram can enqueue only the bound `dispatch.runtime` task after rechecking
  prepared workload and execution brief hashes, but cannot launch, tick, or mark
  runtime work accepted.
- Telegram can start only the bound queued task through task-scoped tick after
  rechecking prepared workload and execution brief hashes, but cannot close
  out or mark runtime work accepted.
- Telegram can monitor only the started task through task-scoped tick with
  `--limit 0`, but cannot dispatch, close out, or mark runtime work accepted.
- Telegram can create closeout artifacts only after the task-scoped monitor
  shows `completed`, but cannot review closeout, mutate files, or mark runtime
  work accepted.
- Telegram transport errors, send failures, and unexpected loop exceptions are
  recorded as health state with backoff.
- The user service uses restart-friendly systemd settings.
- The external watchdog reads loop-status and systemd state from outside the
  listener process and sends rate-limited emergency alerts when remote
  operation is not reliable.

### Adaptive Wiki And Return Boundary

- Offdesk may create wiki candidates or run-local trial entries.
- Canonical wiki promotion remains review-gated.
- Closeout and Ondesk return are required lifecycle stages for completed
  runtime work.
- `forager offdesk wiki export-markdown` defaults to the active profile's
  `wiki-vault/` directory and reports projection freshness.

### Documentation And Artifact Governance

- `PROJECT_STATE.md`, `DECISIONS.md`, and `DELIVERABLES.md` define the shallow
  current surfaces for the Forager checkout.
- `forager project audit-docs` audits current-state freshness, decision and
  deliverable surfaces, human-facing output candidates, large logs, latest
  aliases, and adaptive wiki markdown projection freshness.
- `forager project apply-governance-hints` is dry-run by default, requires
  `--reviewed` to write, creates only missing governance surfaces, and never
  overwrites existing project documents.
- `forager offdesk closeout` carries focused documentation-governance
  recommendations into the Ondesk return package.
- Domain-specific validation history is retained under `archive/domain-history/`
  instead of being presented as product documentation.

## Current Completion Boundaries

### 1. Closeout Review And Accepted-Truth Bridge - Implemented

Verified contract: generated closeout packets connect to closeout review without
collapsing plan approval, gate request creation, approval resolution, brief
generation, enqueue, runtime start, runtime monitoring, closeout packet
generation, and accepted-truth review.

Acceptance checks:

- `forager offdesk enqueue`, `launch`, and `tick` remain separate runtime
  surfaces;
- Telegram cannot convert a plan-review approval, gate request, or gate
  approval into execution by itself;
- Telegram enqueue handoff stays a review receipt until a reviewed workload
  packet is bound;
- Telegram workload binding only produces `bound_enqueue_args` for local
  review; it must not run `forager offdesk enqueue` itself;
- Telegram enqueue run can queue only the bound task and must not call launch or
  tick;
- Telegram runtime start can call only task-scoped
  `offdesk tick --project-key --task-id --limit 1` and must not close out or
  accept runtime work;
- Telegram runtime monitor can call only task-scoped
  `offdesk tick --project-key --task-id --limit 0` and read the same task; it
  must not dispatch, close out, or accept runtime work;
- Telegram closeout packet can call only
  `offdesk closeout --project-key --task-id --dry-run --json`; it must not run
  closeout review, mutate files, or accept runtime work;
- Telegram closeout review handoff can write local `closeout-review` command
  templates only; it must not execute `closeout-review`, mutate files, or
  accept runtime work;
- Telegram closeout verdict recording can run only handoff-bound
  `closeout-review --verdict approved|revise|blocked`; accepted truth is
  recorded only when the resulting closeout receipt status is `accepted`;
- stale plan, stale review, changed launch packet, or mismatched approval
  blocks progression.

### 2. Accepted-Truth Review Bridge - Implemented

Verified contract: closeout readiness and accepted-truth decisions are exposed
through compact operator surfaces.

Acceptance checks:

- mobile surfaces show heartbeat, blocker, and next-safe-action summaries;
- closeout artifacts are linked through CLI-inspectable receipts;
- completed execution remains separate from accepted truth;
- review-required states do not disappear from status.

### 3. Documentation And Dependency Hygiene - Ongoing

Goal: keep product docs current and release-clean.

Acceptance checks:

- `docs/**` contains no project-specific validation names in product surfaces;
- committed screenshots are profile-neutral;
- CLI reference is regenerated when command surfaces change;
- website dependencies are refreshed and `npm audit` is reviewed.

### 4. Module Decomposition - Remaining

Goal: reduce regression risk before adding more remote actions.

Acceptance checks:

- Offdesk CLI plan/closeout/remote-operator code is split into smaller modules;
- Telegram adapter session, rendering, provider, and loop code are separated;
- existing integration tests continue to cover the public contracts.
