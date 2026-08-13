# Remote Operator

The Remote Operator layer lets an operator supervise Offdesk work from a
remote surface such as Telegram, Slack, a mobile app, or a small web console.
It is not a remote shell. It is a transport-agnostic control surface for
planning, approval, observation, interruption, and closeout.

The first likely transport is Telegram, but Telegram must remain an adapter.
The durable contract belongs to Forager's Offdesk registry, approval gate, and
runtime evidence model.

## Purpose

Remote operation should make long-running work easier to entrust without
weakening Forager's safety boundaries.

It should support:

- selecting which project should receive autonomous time;
- preparing read-only plans and council reviews;
- approving plan review separately from launch approval;
- observing progress, heartbeat, blockers, and final closeout;
- interrupting only for decisions that materially change risk or scope;
- preserving append-only receipts for every remote action.

The operator should be able to start a night or lunch-break planning cycle from
a phone, make a small number of high-leverage decisions, and return later to a
reviewable closeout packet instead of raw chat logs.

## Non-Goals

Remote Operator must not provide:

- arbitrary shell execution;
- direct `git push`, deletion, file movement, or package installation commands;
- direct task launch that bypasses `offdesk gate`;
- direct provider or model retargeting without the existing approval boundary;
- access to secrets, environment variables, raw token values, or sensitive
  absolute paths;
- a transport-specific state model that only Telegram can understand.

Any future transport must use the same remote command envelope, approval
receipt, staleness checks, and run ownership model.

## Architecture

```text
Remote transport
  -> Remote Operator adapter
  -> Remote command envelope
  -> Project inventory and Plan Mode
  -> Offdesk plan registry
  -> Operator review receipt
  -> Launch-preparation packet
  -> Execution brief candidate
  -> Offdesk gate approval
  -> Background runner
  -> Monitor, decision interrupts, and closeout
```

Transport adapters receive messages and callbacks. They do not decide whether a
mutation is allowed. The Remote Operator core normalizes input, validates
identity and staleness, and then calls existing Forager surfaces.

Telegram freeform text is routed through a local intent agent when one is
available. The agent is a classifier only: it may label a message as feedback,
a Plan Mode request, an execution request, an approval attempt, or an unsafe
mutation attempt, but it cannot authorize execution, approval resolution, shell
access, git mutation, provider retargeting, or background dispatch. If no local
agent is available in `auto` mode, the adapter falls back to the deterministic
keyword classifier and still records the message as review input only.

The current default provider is local Ollama, preferring configured models and
then local Qwen Coder candidates. Product deployments should use a generic LLM
provider section when the same runtime is shared across transports and scripts:

```toml
[llm.provider]
provider = "ollama"
base_urls = [
  "http://127.0.0.1:11434",
  "http://<gpu-server>:11434"
]
models = [
  "qwen3-coder-next:latest",
  "qwen3-coder:30b"
]
timeout_sec = 20
num_ctx = 8192
num_predict = 768
```

Telegram can still override the shared provider when the remote operator needs
a distinct intent classifier:

```toml
[remote_operator.telegram.agent]
intent_mode = "auto"
provider = "ollama"
base_urls = [
  "http://127.0.0.1:11434",
  "http://<gpu-server>:11434"
]
models = [
  "qwen3-coder-next:latest",
  "qwen3-coder:30b"
]
timeout_sec = 20
num_ctx = 8192
num_predict = 768
```

The same values can be overridden by CLI arguments or environment variables
such as `OFFDESK_REMOTE_OPERATOR_AGENT_INTENT_MODE`,
`OFFDESK_REMOTE_OPERATOR_AGENT_BASE_URL`, `OFFDESK_REMOTE_OPERATOR_AGENT_MODELS`,
`OLLAMA_BASE_URL`, and `OFFDESK_OLLAMA_MODEL`.

The durable path is:

```text
forager offdesk plan
  -> forager offdesk plan-review
  -> forager offdesk plan-launch-prep
  -> forager offdesk gate
  -> forager offdesk ok|cancel
  -> forager offdesk launch|enqueue|tick
```

Plan approval is not launch approval. Launch-preparation packets are not
execution permission. Runtime mutation remains behind `offdesk gate`.

## State Machine

Remote Operator tracks user-visible workflow state while Offdesk keeps durable
task and approval state.

```text
idle
  -> portfolio_scan_requested
  -> project_candidates_ready
  -> plan_draft_ready
  -> council_review_ready
  -> operator_review_pending
  -> plan_approved
  -> launch_prep_ready
  -> gate_pending
  -> running
  -> needs_decision
  -> running
  -> closeout_ready
  -> completed
```

Terminal or exceptional states:

```text
rejected
revision_required
stale_action_rejected
approval_expired
blocked
aborted
transport_unavailable
```

State transitions must be idempotent. Replaying a Telegram callback or mobile
button must not duplicate a review, consume the wrong approval, or launch work
twice.

## Current Implementation Status

The current Telegram implementation covers read-only inspection, remote Plan
Mode preparation, explicit plan-review approval, launch-preparation packet
creation, gate-request creation for pending local approval, exact gate approval
resolution, execution-brief generation, reviewed workload binding, bound enqueue
run, task-scoped runtime start, task-scoped runtime monitor/readout, health
checks, closeout packet creation, handoff-bound closeout verdicts, and an
external watchdog. It does not provide unbound runtime launch or general
process control. Accepted truth can appear only as the result of the existing
CLI closeout receipt contract, never as a generic Telegram approval.

| Surface | Current status | Authority boundary | Durable/local artifact |
| --- | --- | --- | --- |
| `/status`, `/pending`, `/plans`, `/show` | Implemented | Read-only projection only | `remote_operator_readonly_projection.v1` |
| Freeform Telegram text | Implemented | Classifier and inbox input only; no approval or execution authority | `remote_operator_telegram_feedback.v1` and decision inbox rows |
| Project selection | Implemented | Selects a Plan Mode target; does not start work | `telegram_remote_plan_session.v1` |
| Project init preview | Implemented | Dry-run review of project markers and init command | `telegram_remote_project_init_preview.v1` |
| Project init run | Implemented | Runs the local project initialization packet command only; no Offdesk runtime work | `telegram_remote_project_init_run.v1` |
| Plan draft | Implemented | Builds a bounded draft and validates `forager offdesk plan --dry-run` | `telegram_remote_plan_draft.v1` |
| Plan registration | Implemented | Writes only the local Offdesk plan registry | `telegram_remote_plan_registration.v1` |
| Plan-review approval | Implemented | Records plan review only; not launch approval | `telegram_remote_plan_review.v1` and `forager offdesk plan-review` record |
| Launch-preparation packet | Implemented | Creates a read-only packet from an approved review; not gate approval | `telegram_remote_plan_launch_prep.v1` and `offdesk_plan_launch_prep.v1` |
| Gate request | Implemented | Runs `offdesk gate` to create pending approval only; does not approve it | `telegram_remote_plan_gate_request.v1` and pending approval row |
| Gate approval resolution | Implemented | Resolves only the exact matching pending approval; does not enqueue or launch | `telegram_remote_plan_gate_resolution.v1` and approval transition row |
| Execution brief | Implemented | Creates a bounded `ExecutionBrief`; does not enqueue or launch | `telegram_remote_plan_execution_brief.v1` and `EXECUTION_BRIEF.json` |
| Enqueue handoff | Implemented | Writes a local-review command template only; does not enqueue or launch | `telegram_remote_plan_enqueue_handoff.v1` and `PLAN_ENQUEUE_HANDOFF.json` |
| Workload binding | Implemented | Binds a reviewed `prepared_task.json` to the execution brief; does not enqueue or launch | `telegram_remote_plan_workload_binding.v1` and `PLAN_WORKLOAD_BINDING.json` |
| Enqueue run | Implemented | Runs only the bound `offdesk enqueue` argv; does not launch or tick | `telegram_remote_plan_enqueue_run.v1` and `PLAN_ENQUEUE_RUN.json` |
| Listener health | Implemented | Reports local transport/model readiness; no mutation authority | `remote_operator_telegram_health.v1` |
| External watchdog | Implemented | Runs outside the listener; sends rate-limited emergency alerts only | `remote_operator_telegram_watchdog.v1` |
| Runtime start | Implemented | Runs task-scoped `offdesk tick --project-key --task-id --limit 1`; does not monitor, close out, or accept truth | `telegram_remote_plan_runtime_start.v1` and `PLAN_RUNTIME_START.json` |
| Runtime monitor/readout | Implemented | Runs task-scoped `offdesk tick --project-key --task-id --limit 0` and reads the same task; does not dispatch, close out, or accept truth | `telegram_remote_plan_runtime_monitor.v1` and `PLAN_RUNTIME_MONITOR.json` |
| Closeout packet | Implemented | Runs `offdesk closeout --project-key --task-id --dry-run --json`; does not review, mutate files, or accept truth | `telegram_remote_plan_closeout_packet.v1` and `PLAN_CLOSEOUT_PACKET.json` |
| Closeout review handoff | Implemented | Writes bounded `closeout-review` verdict templates; does not run verdicts yet | `telegram_remote_plan_closeout_review_handoff.v1` and `PLAN_CLOSEOUT_REVIEW_HANDOFF.json` |
| Closeout verdict/accepted-truth bridge | Implemented | Runs only handoff-bound `closeout-review --verdict approved|revise|blocked`; accepted truth is recorded only from the closeout receipt | `telegram_remote_plan_closeout_verdict.v1` and `PLAN_CLOSEOUT_VERDICT.json` |

The practical rule is: Telegram can prepare and review a plan and create the
read-only launch-preparation packet and pending gate request without turning
plan approval into runtime authority. After `실행 브리프 생성`, Telegram can
write a local enqueue handoff template, bind a reviewed `prepared_task.json`,
enqueue the bound task, and start only that queued task through task-scoped
tick. Telegram can then poll/read only that same task and create the closeout
packet for a completed task. A later explicit verdict may run only the
handoff-bound `closeout-review` command. Accepted truth is recorded only when
that command produces an accepted closeout receipt.

## Remote Command Envelope

Every remote message or button callback is normalized before it can affect
Forager state.

```json
{
  "schema": "remote_command_envelope.v1",
  "source_transport": "telegram",
  "source_surface": "remote_operator.telegram",
  "operator_id": "telegram:<user-id>",
  "chat_id": "telegram:<chat-id>",
  "intent": "approve_plan",
  "target_type": "offdesk_plan",
  "target_id": "plan_...",
  "observed_hash": "sha256:...",
  "observed_version": "offdesk_plan_summary.v1",
  "message_id": "telegram:<message-id>",
  "callback_id": "telegram:<callback-query-id>",
  "nonce": "uuid",
  "created_at": "2026-06-06T00:00:00Z",
  "expires_at": "2026-06-06T00:30:00Z"
}
```

The envelope is operator-safe. It must not store bot tokens, environment
values, raw prompts containing secrets, or full command output.

The `observed_hash` is mandatory for approvals. It binds the approval to the
exact plan, launch-prep packet, execution brief, or pending approval summary
that the operator saw.

## Project Selection Contract

When multiple projects are available, Plan Mode starts with a comparable
project inventory. Each candidate must use the same fields so the recommendation
is inspectable.

```json
{
  "schema": "remote_project_candidate.v1",
  "project_key": "98-harness",
  "workspace_path": "/operator-safe/path",
  "current_branch": "offdesk-generic-planning-harness",
  "head_ref": "sha",
  "dirty_tree": false,
  "remote_sync": "up_to_date|ahead|behind|diverged|unknown",
  "recent_failures": [],
  "available_tests": ["cargo test", "cargo clippy"],
  "estimated_runtime_minutes": 180,
  "risk_level": "low|medium|high",
  "autonomy_fit": "low|medium|high",
  "requires_network": false,
  "requires_secrets": false,
  "requires_human_context": false,
  "recommended_next_action": "build execution brief bridge",
  "blocked_reason": null
}
```

The recommendation should explain why a project is selected and why plausible
alternatives were not selected. A project with a dirty tree, missing tests,
unknown branch state, or human-context dependency can still be recommended, but
the risk must be explicit.

## Plan Mode Portfolio Flow

A request such as `/plan tonight --until 09:00` should perform:

1. Read-only workspace discovery.
2. Project candidate inventory.
3. Candidate ranking by value, readiness, risk, and autonomy fit.
4. Plan generation for the selected project or top candidates.
5. Council-style review of the plan.
6. Registration through `forager offdesk plan`.
7. Remote summary with a stable observed hash.

The remote summary should be compact:

```text
Recommended plan

Project: 98.Harness
Goal: Build execution brief bridge from launch-prep to offdesk gate
Risk: medium
Autonomy fit: high
Deadline: 09:00
Requires launch approval: yes
Plan hash: sha256:...

Actions:
[Approve Plan] [Request Revision] [Reject]
```

The full plan stays in the registry. The transport only displays a bounded
summary and links or IDs needed for inspection.

## Approval Model

Remote operation has at least two human approval boundaries.

### Plan Review Approval

Plan approval means:

- the operator accepts the direction;
- `forager offdesk plan-review --decision approved` may be recorded;
- a launch-preparation packet may be built;
- work still must not be enqueued, launched, or dispatched.

### Launch Approval

Launch approval means:

- an execution brief candidate has been prepared;
- `forager offdesk gate` has created or evaluated a scoped approval;
- the operator approves the exact pending approval ID or capability transition;
- the normal Offdesk runtime path may continue.

The two approvals must not be collapsed into one Telegram button. A remote
operator can approve a plan while still refusing to launch it after seeing the
execution brief.

## Staleness Guards

Before accepting any remote approval, Forager must validate:

- the approval or review target still exists;
- the target is still pending or reviewable;
- the observed hash matches the current target summary;
- the source artifact hash has not changed;
- the remote command envelope has not expired;
- the nonce has not already been consumed;
- the project branch and head ref still match the displayed summary, unless the
  execution brief explicitly allows drift;
- the dirty-tree state has not changed in a way that invalidates the plan;
- the capability remains allowed under current policy;
- provider capacity, model availability, and auth state still satisfy the
  approval metadata when relevant.

If any check fails, the remote action is rejected as stale and an append-only
receipt records the reason. The operator receives a fresh summary rather than a
best-effort mutation.

## Autonomy Budget

Every autonomous run needs an explicit budget. Time alone is not enough.

```json
{
  "schema": "remote_autonomy_budget.v1",
  "deadline": "2026-06-06T09:00:00+09:00",
  "max_turns": 12,
  "max_model_calls": 30,
  "max_cost_usd": null,
  "max_commits": 3,
  "max_pushes": 1,
  "max_project_switches": 1,
  "max_retries_per_failure": 2,
  "decision_interrupt_policy": "conservative"
}
```

Budget exhaustion should stop or pause the run. It should not silently expand
scope. A budget update is a new remote decision with its own envelope and
receipt.

## Decision Interrupts

Remote Operator should interrupt only for decisions that change risk, scope, or
durable state.

Mandatory interrupts:

- project switch;
- scope expansion;
- destructive file operation;
- commit or push;
- provider or model retargeting;
- network or secret requirement not declared in the approved plan;
- test failure bypass;
- approval retargeting;
- budget exceeded;
- repeated blocker;
- source hash, branch, or dirty-tree mismatch;
- closeout verdict requiring human judgement.

No interrupt is required for:

- read-only inspection;
- formatting;
- focused test reruns;
- bounded documentation updates;
- non-destructive artifact creation;
- implementation choices inside the approved plan and budget;
- status polling or heartbeat updates.

This keeps remote operation from becoming one-message-at-a-time chat while
still preserving important decisions.

## Run Ownership

When a run starts, ownership is recorded separately from transport state.

```json
{
  "schema": "remote_run_ownership.v1",
  "run_id": "run_...",
  "project_key": "98-harness",
  "plan_id": "plan_...",
  "plan_review_id": "review_...",
  "launch_prep_id": "launch_prep_...",
  "approval_id": "approval_...",
  "operator_id": "telegram:<user-id>",
  "transport": "telegram",
  "started_at": "2026-06-06T00:00:00Z",
  "deadline": "2026-06-06T09:00:00+09:00",
  "abort_policy": "operator_or_safety_gate",
  "closeout_required": true
}
```

Only the owner, an explicitly configured emergency operator, or a local CLI
operator should be able to abort or revise a run. Observer accounts can read
status but cannot approve mutations.

## Telegram Transport Policy

The current Telegram adapter exposes a small safe command surface:

```text
/status
/projects [project]
/plans
/show <plan_id>
/pending
freeform planning text
project-selection buttons
plan-session buttons through 계획 승인
```

### Grounded freeform chat

Plain text routes to the local chat agent (Ollama). The chat prompt is
grounded with two payload sections so answers come from real state instead of
guesses:

- `operator_snapshot`: live read-only state gathered per message in
  `build_chat_operator_snapshot()`: workstation surface summary
  (attention counts, capacity, health, up to 5 open decision titles),
  `autonomy_armed`, and workspace project folder hints discovered under the
  operator's workspace roots. Every section degrades gracefully; chat still
  works if a probe fails.
- `supported_commands`: the complete slash-command surface from
  `telegram_operator/routing.py::COMMAND_SURFACE` (keep in sync with
  `parse_remote_command`).

A deterministic guard (`telegram_operator/agent.py::scrub_unknown_commands`)
rewrites any slash command outside the real surface to `/help` in model
replies, so the agent can never teach the operator a command that bounces
with `unsupported_remote_operator_command`. Filesystem paths and URLs are
left untouched.

The following command classes are future design targets, not current Telegram
runtime authority:

```text
/approve <approval_id>
/deny <approval_id>
/abort <run_id>
```

Current buttons carry the same bounded command text as the visible command
surface. Conceptual action names exposed by read-only projections are not
accepted as raw Telegram callback authority. New callbacks must remain
unavailable until the corresponding bridge has explicit staleness checks,
observed-hash binding, nonce and TTL validation, and local CLI recovery.
Revision and rejection of a registered Plan Mode artifact are not yet exposed
through Telegram.

Examples of unavailable direct callbacks include:

```text
approve_launch
deny_launch
acknowledge_status
abort_run
request_closeout
```

Forbidden command classes:

```text
/run <shell>
/exec <shell>
/git push
/delete
/move
/install
/set-env
/retarget-provider
```

If an operator needs one of these actions, Remote Operator should create a
bounded approval or decision request rather than execute the text directly.

## Security And Audit

Remote Operator requires:

- configured allowlist of transport user IDs and chat IDs;
- one owner for mutation approvals during the initial release;
- read-only mode for observers;
- nonce-based idempotency for every callback;
- short TTL for approval callbacks;
- append-only receipts for accepted and rejected remote actions;
- secret redaction before persistence and before transport replies;
- a Telegram env file with mode `0600`, rejected at startup if group or other
  users can read it;
- private `0600` listener state, feedback, conversation, and loop-status files;
- rate limits for plan requests, status polling, and callback retries;
- local CLI recovery for every remote-visible state.

Remote transport outages must never leave work in an unknowable state. The CLI
must remain the source of truth for plans, approvals, tasks, and closeout.

## Monitoring And Closeout

Running work should emit:

- heartbeat status;
- current phase;
- latest meaningful progress;
- current blocker, if any;
- budget remaining;
- pending decisions;
- last verification result;
- final closeout artifact refs.

The morning summary should not be a raw transcript. It should include:

- what changed;
- files changed;
- commits and pushes;
- tests run and failures;
- open blockers;
- decisions needed;
- next safe action;
- whether the tree is dirty;
- whether closeout was accepted, needs revision, or is blocked.

The closeout packet remains the durable handoff surface. Telegram only displays
a compact summary and artifact references.

## Failure Recovery

Remote Operator must handle:

- duplicate callback delivery;
- stale button presses;
- revoked or expired approvals;
- transport outage during a run;
- background runner failure;
- local profile lock contention;
- project branch drift;
- dirty tree drift;
- plan source file deletion;
- model/provider cooldown;
- operator ownership handoff.

Recovery should prefer read-only status and fresh approval requests over
implicit mutation.

### Action Readiness

Remote health is not a single global truth. A listener can be healthy while
freeform planning, launch preparation, or execution is blocked. Every remote
surface should therefore report readiness per requested action.

Status vocabulary:

- `healthy`: all dependencies required for the action are available;
- `degraded`: the transport works, but quality or judgment is reduced;
- `blocked`: the requested action cannot proceed;
- `unsafe`: the request would cross an authority, freshness, or approval
  boundary;
- `unknown`: the surface cannot prove the state, so mutations stay blocked.

Initial action gates:

| Action | Required proof | Failure response |
| --- | --- | --- |
| `status` | Telegram config, listener loop status | Keep read-only status if possible; otherwise report transport outage. |
| `project_scan` | Workspace roots and readable project markers | Allow deterministic search; ask for a path if unresolved. |
| `build_plan` | Resolved project and available local agent/model for freeform judgment | Block new plan/night-run starts; allow status, project scan, and existing plan review. |
| `start_offdesk` | Approved plan, launch prep, gate approval, execution brief, reviewed workload binding, queued task | Allow only bound task start/readout; fail closed for arbitrary launch, shell, closeout, and accepted truth. |

When local agent/model resolution fails, Telegram should not imply that a new
night run is ready. It may still show deterministic project candidates, but the
mobile message must say that new plan/night-run start is blocked and include a
short recovery hint.

### Common Degraded States

| Class | Examples | User-visible response |
| --- | --- | --- |
| Transport outage | missing token, chat allowlist, stale poll loop | Block remote commands; point to local CLI recovery. |
| Agent outage | stale Ollama/GPU URL, missing model, provider error | Mark `build_plan` blocked; keep status and project scan available. |
| Runtime/provider outage | no provider capacity, model cooldown, runner unavailable | Permit read-only plan review; hide launch/start actions. |
| Project resolution failure | project hint not in top candidates, path missing | Search the full workspace first, then ask for an exact path. |
| Project state risk | dirty tree, branch drift, missing project markers | Produce a preview or blocker list; do not start autonomy. |
| Stale artifact | changed plan draft, old approval button, source hash mismatch | Mark unsafe and require fresh review. |
| Runner uncertainty | stale heartbeat, missing progress, unreadable closeout | Stop claiming active execution; surface recovery commands. |
| Config drift | service uses old script/config or stale provider URL | Report the concrete stale dependency and require restart/recheck. |

## Implementation Phases

### Phase 1: Read-Only Remote Surface - Implemented

- Implement transport allowlist.
- Implement `/status`, `/pending`, `/plans`, and `/show`.
- Return operator-safe summaries only.
- No remote launch, enqueue, shell execution, provider retargeting, or file
  mutation.
- Remote plan review receipts may be recorded only against registered plan
  artifacts. They do not authorize launch preparation, enqueue, runtime
  execution, or project mutation.

Current CLI projection surface:

```bash
forager offdesk remote-operator status --json
forager offdesk remote-operator pending --json
forager offdesk remote-operator plans --json
forager offdesk remote-operator show <plan-id> --json
```

These commands emit `remote_operator_readonly_projection.v1` with
`read_only=true`, `mutation_authorized=false`, and execution authorization set
to false. They are intended for Telegram or another transport adapter to
render. The `pending` projection reads approval rows without resolving or
expiring them.

The Telegram adapter starts from these read-only commands and adds bounded Plan
Mode and task-bound action receipts. Mutation-capable actions remain behind the
confirmation, observed-state, approval, and task-binding gates described later:

```bash
scripts/offdesk_remote_operator_telegram.py \
  --dry-run \
  --command-text "/status" \
  --forager-bin target/debug/forager

scripts/offdesk_remote_operator_telegram.py \
  --once \
  --env-file /path/to/telegram.env \
  --forager-bin target/debug/forager

scripts/offdesk_remote_operator_telegram.py \
  --env-file /path/to/telegram.env \
  --forager-bin target/debug/forager

scripts/offdesk_remote_operator_telegram.py \
  --health \
  --env-file /path/to/telegram.env

scripts/offdesk_remote_operator_watchdog.py \
  --dry-run \
  --env-file /path/to/telegram.env \
  --loop-status-file ~/.cache/forager/remote_operator_telegram_loop.json

scripts/offdesk_remote_operator_telegram.py \
  --send-command-text "/status" \
  --env-file /path/to/telegram.env \
  --forager-bin target/debug/forager
```

The default credential file is
`$XDG_CONFIG_HOME/forager/telegram.env` on Linux, falling back to
`~/.config/forager/telegram.env`. On macOS it is
`~/.forager/telegram.env`. Set `OFFDESK_TELEGRAM_ENV` or pass `--env-file` to
override it. The service installer requires the file to exist with mode
`0600` or stricter.

The adapter accepts plain Telegram text as read-only chat. Structured surfaces
are explicit slash commands: `/status`, `/projects [project]`, `/pending`,
`/plans`, `/show <plan-id>`, `/feedback`, `/remember`, `/wiki [project]`,
`/plan`, `/decisions`, `/decision`, `/confirm`,
`/cancel`, `/help`, and `/guide` (aliases `/qna`, `/usage`, `/commands`), plus
the bounded plan-session buttons described below. `/guide` renders the full
grouped command reference sheet from `routing.py::COMMAND_SURFACE` under a
relaxed reference-sheet budget (40 lines / 2000 chars) instead of the 5-line
mobile card contract; `/help` stays a compact card and points to `/guide`.
Unsupported commands such as `/approve`, `/launch`, `/exec`, or `/git push`
return an unsupported result and do not call mutation-capable local surfaces.
Without `--once`, live polling stays attached and keeps reading Telegram
updates. `--once` is for one-shot probes, and `--max-polls` is available for
bounded smoke tests. `--send-command-text` sends one read-only projection to
the configured owner chat without consuming updates.

The continuous poller takes an exclusive local listener lock so two service
instances cannot both issue Telegram `getUpdates`. The systemd unit uses
`Restart=on-failure`, a bounded start limit, and `UMask=0077`. The loop status
records per-status counts, consecutive errors, and last success/error times in
addition to the latest result. Health also verifies that registered projects
resolve under the pinned workspace roots. A zero-project scan is degraded or
unhealthy instead of being reported as a healthy project surface.

## Waiting-Session Notifications

With `--session-notify` (the systemd installer enables it by default) each
poll runs `forager status --json` and pushes one card per waiting-episode of
a supervised agent session: a `forager go` session that hits a permission
prompt or question while the operator is away produces
`세션 입력 대기 / ⏸ <project> · <title> (<tool>)` with prompt-bound approve,
deny, or numbered-option buttons. The project tag comes from the session's
registry match. Before sending a key, the listener rechecks both the waiting
status and the pane hash shown on the card. It reports success only when the
pane repaints; an unchanged pane returns `no_effect` and that exact prompt is
not offered again.

Plain Telegram text can also become a real local-agent message. The local
intent model receives the current `running`, `idle`, and `waiting` supervised
session list and returns a `send_agent` action with one exact session id and
the text to deliver. A message that replies to a waiting-session card is bound
to that card's session and pane hash. After one successful delivery, follow-up
instructions may continue using the same session as chat context. If several
sessions could match, a deterministic validator asks which one instead of
trusting the model's first choice. An explicitly mentioned project, agent
title, reply-card binding, full current session id, and minimum confidence all
have to agree before any tmux input is sent.

Text delivery uses a private tmux buffer with bracketed paste and never invokes
a shell. This preserves multi-line instructions and indentation. The target
must still be live when delivery starts. For a card-bound reply, the pane hash
must still match. A successful tmux paste is reported as `input_queued`; a
subsequent repaint upgrades the evidence to `pane_reacted`. Neither state is
presented as proof that the agent accepted or completed the work.
`/session_send <id> [hash] -- <text>` provides the same path explicitly for
local recovery and testing. Direct `--dry-run --command-text` previews never
send tmux input. Ordinary operators can write natural language such as
`forager Codex에게 전체 테스트를 다시 돌리고 결과를 알려달라고 전해줘`.

Reminders are bounded per prompt. `--session-notify-backoff-sec` defaults to
1800 seconds and doubles on each repeat up to
`--session-notify-max-backoff-sec` (default 14400). A prompt stops after
`--session-notify-max-cards` cards (default 4; 0 disables the cap). A changed
pane hash starts a fresh prompt budget even if the session remained in the
waiting state between polls. Session cards are quiet from
`--session-notify-quiet-from-hour` (default 23) until
`--session-notify-quiet-to-hour` (default 8); an item still waiting after the
window is eligible immediately. Equal quiet-hour bounds disable the window.
`--session-status-file` overrides the probe for replay tests.

## Idle-Proposed Overnight Autonomy

With `--autonomy-propose` the listener watches local activity (newest mtimes
under the Claude/Codex session trees by default). When the workstation has
been idle past the threshold (default 30 minutes, only after
`--autonomy-propose-from-hour`, default 17:00), it sends ONE confirmation
card offering to arm the bounded overnight window; the operator's 확인 tap is
the only thing that arms it. Unanswered or declined proposals back off
(default 2h); an already-armed window, an active operator pause, or another
FRESH pending confirmation suppresses proposals. An expired pending
confirmation (for example last night's unanswered proposal card) is cleared
by the scan instead of blocking proposals forever.

Approval runs `offdesk_autonomy_run.py --arm` (window until 09:00 local by
default). While armed, three systemd user timers do the night work: an
`offdesk tick` heartbeat every 10 minutes (launching only already-approved
tasks), the operator-owned nightly distillation playbook at 02:00
(candidate-only), and the 08:50 morning brief (/attention card + wiki
candidate-queue summary), which then disarms for the day. `/pause` still
halts dispatch instantly regardless of the armed state. Install the timers
with `scripts/install_offdesk_autonomy_timers.py --install --enable`; they
are inert unless armed.

## One-look triage

`/attention` is the fast "what needs me right now" summary. It reads the current
operator-safe workstation surface and the registered wiki planes, then returns
a single card aggregating open decisions, accepted-truth recovery follow-ups,
tasks flagged for review, and the wiki candidate backlog. The card keeps
runtime decisions ahead of wiki curation, names the single most urgent action,
and points at `/decisions`, `/recovery`, `/tasks`, or `/wiki`. It is
read-only and never mutates state.

`/wiki` ranks registered projects by unpromoted candidate count and offers
the busiest project as a one-tap next action. `/wiki <project-key>` shows one
plane's candidate/promoted counts and newest candidate claims, then sets that
project as the chat's sticky focus. This is a review surface only: promotion
and rejection remain local governed mutations.

`/projects` returns a compact portfolio with live session counts and workspace
resolution. `/projects <project>` accepts the project key, display name,
workspace folder, or configured alias, then sets the same chat focus used by
plain text and `/remember`. The service installer resolves and pins the real
workspace root before scripts are copied into `~/.local/share`; project lookup
also supports one nested organization directory such as `0.1.NIMS/EpiMS`.

## Proactive attention notifications

The live poller can also push, unprompted, when something is waiting so an
urgent item can be handled without checking first. With `--attention-notify`
(the systemd installer enables it by default; set `OFFDESK_REMOTE_OPERATOR_
ATTENTION_NOTIFY=1` or pass the flag manually), each poll scans the current
operator-safe `workstation_surface.v1` for open decisions and accepted-truth
recovery follow-ups and sends the owner chat a short card naming the top item
and the exact command to run (for example `/decision <id> revise`).

It is deduplicated: an item is pushed once, and a still-waiting item is
re-notified after `--attention-reminder-sec` (default 21600 seconds). Set the
value to 0 to notify only once. A resolved item is pruned so it notifies
afresh if it reopens. The scan is read-only, never mutates state, and never
crashes the poll loop: a surface-export or send failure is recorded and the
loop continues.

## Emergency stop

`/tasks` lists tasks that can still be cancelled (drawn from the operator-safe
workstation surface: anything not already completed or cancelled), with each
task id and status. `/cancel-task <task-id> [reason]` returns a single-use
confirmation card; `/confirm <token>` runs `forager offdesk cancel-task`, which
marks the durable task cancelled so the harness stops continuing or resuming
it.

This is the fail-safe direction (it stops work, it does not start anything), so
it is available without the `--enable-runtime-dispatch` opt-in. It does not,
however, kill an already-running background process: the result card says so
plainly ("the background runner may still be running"). An unknown or
already-finished task fails cleanly with a clear card and never wedges the poll
loop.

`/pause [reason]` is the global emergency stop. It is immediate (no
confirmation step, because stopping is fail-safe and speed matters): one message
engages a persistent operator pause, and `forager offdesk tick` then holds all
new dispatch (dispatch-ready tasks stay queued and are reported as held) while
still polling and reconciling existing background runs. `/resume` re-enables
autonomy and is confirm-gated, since restarting dispatch is a deliberate act:
`/resume` returns a confirmation card and `/confirm <token>` clears the pause.
The pause state persists in `offdesk_operator_pause.json`, so it survives across
ticks and processes until explicitly resumed. Locally the same switch is
`forager offdesk pause` / `offdesk unpause` / `offdesk pause-status`.

## Guarded remote decision execution

`/decisions` lists open decision records and the bounded action kinds
available for each (for example `revise` or `block`). `/decision <decision-id>
<action> [note]` does not execute anything: it exports a fresh operator-safe
`workstation_surface.v1`, rebuilds the executable action envelope from that
surface, and returns a confirmation card carrying a single-use token bound to
the decision id, action kind, and observed hash, with a TTL
(`--dispatch-confirm-ttl-sec`, default 300s).

Every confirmation card carries one-tap `확인` / `취소` buttons so the operator
does not have to type the token. `확인` sends a bare `/confirm` (no token),
which confirms the single pending confirmation for that chat; a typed
`/confirm <token>` still works and must match. `/attention` and the notification
cards additionally offer the single most urgent action as a one-tap button
(e.g. `/decision <id> revise`), so an urgent item can be handled by tapping the
action and then `확인`.

The `/decisions` card goes one step further: it renders the most urgent open
decision's action kinds (e.g. `승인`/`보류`/`차단`) as full `/decision <id>
<action>` buttons. Tapping one dispatches that action straight into the confirm
step, so a decision is handled in two taps (action button, then `확인`) with no
typing. Only the top open decision is buttoned to keep the mobile card tight;
the remaining decisions stay reachable via the `/decision <id> <action> [note]`
text hint or by re-running `/decisions`.

`/confirm <token>` is the only step that applies a decision. It re-exports the
surface, rejects the request if the decision's observed hash changed since the
token was issued, then runs the existing receipt-gated executor chain
(`action-envelope` -> `action-preflight` -> `action-decision` ->
`action-closeout`). The CLI independently re-validates the observed hash,
nonce, and expiry, so a stale envelope is rejected with a receipt rather than a
mutation. `/cancel` clears a pending confirmation.

This surface never executes arbitrary shell text, never records accepted truth,
and only orchestrates commands the CLI already exposes. It drives the same
receipt-gated executors as the local Web decision action center, so the two
surfaces share one safety model.

`/recovery` and `/recover <closeout-id> <action> [note]` mirror this flow for
accepted-truth recovery follow-ups (`resolve_followup`, `retire_closeout`),
sharing the same confirmation token. `/confirm` on a recovery token runs
`accepted-truth-recovery-envelope`, which validates the recovery envelope and
records an `accepted_truth_recovery_action_receipt.v1`. This stops at
validation: recording accepted truth or running the fallback command remains a
separate explicit local step that this surface does not perform. A successful
validation card shows the exact receipt-bound local fallback command so the
operator does not need to rediscover it from `/recovery`.

### Runtime dispatch (opt-in)

`/runtime` lists post-closeout handoffs that are ready for runtime dispatch.
`/dispatch <closeout-id> <runner> -- <command>` queues an operator-supplied
command for a receipted closeout. Unlike the decision and recovery surfaces,
this runs a command the operator types, so it is **off by default**. It is only
available when the listener is started with `--enable-runtime-dispatch` (or
`OFFDESK_REMOTE_OPERATOR_ENABLE_RUNTIME_DISPATCH=1`); without that flag
`/dispatch` is refused and no confirmation is stored.

Treat `--enable-runtime-dispatch` as remote command execution: a compromised
Telegram account can queue arbitrary commands. Enable it only on trusted
setups with a locked-down chat allowlist. When enabled, `/dispatch` still
requires a `/confirm <token>` step. On confirm, `runtime-preflight`
re-verifies the closeout against the latest canonical decision receipt, then
`runtime-dispatch` queues a durable `OffdeskTask`. It does not launch a
process: the queued command runs later only through `forager offdesk tick` and
the scheduler gate.

### Curated dispatch allowlist (safer alternative)

`/run` is the safe alternative to free-form `/dispatch`. Instead of typing a
command, the operator names a pre-vetted template from a local allowlist. The
runner and command come from a JSON file on the trusted machine, never from the
Telegram message, so a compromised chat can only trigger commands that were
already vetted locally.

Point the listener at the file with `--dispatch-allowlist-file <path>` (or
`OFFDESK_REMOTE_OPERATOR_DISPATCH_ALLOWLIST`). It is off by default and is a
**separate** opt-in from `--enable-runtime-dispatch`: a setup can allow named
commands without allowing free-form ones. The file shape is:

```json
{ "templates": [
  { "name": "smoke", "runner": "local-background", "command": "cargo test", "description": "run tests" }
] }
```

A copy-ready starter file lives at
`docs/examples/offdesk_dispatch_allowlist.example.json`; copy it, edit the
templates, and point `--dispatch-allowlist-file` at your copy. The systemd
installer accepts the same flag:
`install_offdesk_telegram_operator_service.py --dispatch-allowlist-file <path>`
appends it to the unit's `ExecStart`.

Templates missing a name, runner, or command are dropped, and a malformed or
missing file degrades to "not configured" rather than crashing the poll loop.
`/run` (or `/run --list`) shows the available templates; `/run <closeout-id>
<name>` builds a confirm card. On `/confirm` the command is **re-resolved from
the current allowlist by name** and run through the same `runtime-preflight` ->
`runtime-dispatch` chain, so removing a template from the file revokes it even
for an outstanding confirmation, and the executed command is always a currently
vetted template. Curated `/run` works without `--enable-runtime-dispatch`
because the command is bounded by the local allowlist rather than operator
input.

Telegram messages should stay short enough for mobile scanning: a compact
title, the current state, and the next local-safe action. Longer listener
diagnostics belong in local health output, not in the chat message.

Plain Telegram text starts as chat. It can become a validated message to one
live local agent, but it is not feedback capture and not a planning request.
Use `/feedback <text>` to record an operator note in the decision inbox, and
use `/plan <request>` to create a planning-request decision with an explicit
note that no autonomous work has started. `/remember <text>` records an
adaptive wiki candidate. With an explicit or sticky project focus it uses the
registry's `wiki_profile` and project scope; without a focus it uses the
listener profile and user-global scope. It is not promoted knowledge and
cannot affect runtime behavior until local wiki review promotes it.

Chat keeps a small rolling history of recent turns per chat in local listener
state so follow-up questions can be answered in context. Assistant history uses
the exact semantic text sent to Telegram, not an internal pre-render response.
When the agent asks a clarification, that question is retained as clarification
context for the next chat turn without refreshing an unrelated decision or wiki
card. The history is bounded, expires with `--context-max-age-sec`, never leaves
the local state file. Conversation history grants no additional authority;
only the separately validated single-session delivery path can send tmux input.
Project focus has its own 24-hour default expiry via
`--project-focus-max-age-sec`.

Project aliases are fail-closed. ASCII aliases match token boundaries instead
of arbitrary substrings, and an alias shared by multiple registry entries does
not select whichever entry happens to load first. Chat asks for the canonical
project key, `/wiki` and `/projects` list the conflicting keys, and `/remember`
records nothing until the target is unambiguous.

Every handled inbound update also appends
`remote_operator_telegram_conversation.v1` to the private conversation JSONL.
It stores hashed chat/user IDs, inbound text, parsed command, selected project,
read-only tools used, the exact semantic text sent, send receipt, and a result
effect envelope. The effect envelope reports `read_only=false` only when a
guarded write actually succeeded. It names the concrete authority domain, such
as plan artifact, plan registry, Offdesk gate, task queue, runtime launch, or
accepted-truth review, and includes the nested receipt schema and status when
available. Conversation-ledger failure is reported in the result but does not
replay an already handled Telegram mutation.

Before handling an allowed inbound update, the listener fsyncs a `started` row
to `remote_operator_telegram_updates.jsonl`. After the handler returns, it
persists the handler-owned state and queues the exact Telegram reply in
`remote_operator_telegram_reply_outbox.jsonl` before marking the update
`effect_committed`. Only a delivered reply advances that row to `completed`.
The Telegram offset can advance while delivery is pending because the next
poll retries the private outbox without executing the effect handler again.

If the process stops after a reply is queued, the outbox proves that handler
processing reached the delivery boundary and repairs the journal on restart.
A process stop while the journal is only `started` remains ambiguous and fails
closed. Inspect it locally with:

```bash
scripts/offdesk_remote_operator_telegram.py --journal-inspect \
  --journal-update-id <UPDATE_ID>
```

After checking downstream receipts, stop the listener and explicitly choose
one resolution. `retry` asserts that no effect was applied and permits one
fresh handler attempt. `complete` suppresses handler retry because the update
was already handled or intentionally abandoned:

```bash
scripts/offdesk_remote_operator_telegram.py --journal-reconcile retry \
  --journal-update-id <UPDATE_ID> \
  --journal-reason "downstream receipts show no applied effect"
```

The reconcile command takes the normal listener lock, so it refuses to run
while the service owns the poller. A malformed listener state, journal, or
reply outbox is a health failure. The listener never resets a corrupt state
file to offset zero. Telegram delivery itself cannot be exactly-once: a process
stop after Telegram accepts `sendMessage` but before the local delivered row is
fsynced can produce a duplicate reply, but it still cannot replay the guarded
effect.

Journal and outbox files are append-only during normal polling. During a
maintenance stop, compact them against the trusted listener offset. Completed
updates below that offset are removed, while every unresolved update and
pending reply is preserved:

```bash
scripts/offdesk_remote_operator_telegram.py --journal-compact
```

Freeform service claims are treated as operator reports, not facts. A message
that names a local port automatically seeds the agent with a `service_probe`
observation, and the agent can request the same read-only TCP listener tool in
its bounded tool loop. It must label a service report unverified when no live
observation supports it.

Planning requests open a short project-selection session. The operator can tap
a candidate button or type a project number/name directly. If the typed project
is not in the candidate list, the listener stores it as a manual project hint
for later path confirmation. This only selects the Plan Mode target; launch,
approval, shell, wiki promotion, and git mutation remain unavailable from
Telegram.

After a project is selected, the listener keeps the session active and offers
`초기화 검토`, `다시 선택`, and `보류`. `초기화 검토` writes a
`telegram_remote_project_init_preview.v1` receipt under the local plan-session
cache. The receipt records the selected project markers, documentation sources,
entrypoints, and the local `forager project init ... --include-git --json`
command to review. It does not run `project init`, does not register an
Offdesk plan, and does not start work. Manual project hints must first be
resolved to a real project path before this preview receipt can be created.

After that preview receipt exists, the listener offers `초기화 생성`.
This runs only the local `forager project init ... --include-git --json`
initialization packet command and stores a
`telegram_remote_project_init_run.v1` receipt in the same plan-session cache.
The Telegram message stays path-free and short, while the local receipt records
the generated packet output for later Plan Mode review. This still does not
register an Offdesk plan, approve anything, start runtime work, run shell
commands outside the initialization command, or mutate git state.

After the initialization packet exists, the listener offers `계획 초안 생성`.
This writes a bounded `offdesk_multiturn_plan.v1` draft to the local
plan-session cache, then validates it with
`forager offdesk plan ... --dry-run --json`. The resulting
`telegram_remote_plan_draft.v1` receipt records the dry-run validation output.
This still does not register the plan, approve launch preparation, enqueue
work, or start runtime execution.

After the draft validates, the listener offers `계획 등록`. The listener first
checks that the draft hash still matches the dry-run receipt, then runs
`forager offdesk plan ... --json` without `--dry-run` and stores a
`telegram_remote_plan_registration.v1` receipt. Registration writes the local
Offdesk plan registry only. It still does not record a plan-review approval,
build launch-preparation packets, enqueue work, or start runtime execution.

After registration succeeds, the listener keeps the same Telegram session active
and offers `계획 승인`. This maps only to
`forager offdesk plan-review <plan-id> --decision approved --json` and stores a
`telegram_remote_plan_review.v1` receipt. The receipt records the reviewed plan,
source hash, and Offdesk review record path for local audit. It does not build
launch-preparation packets, enqueue work, or start runtime execution.

Project candidate discovery is read-only and generic. Configure scan roots with
repeated `--workspace-root` flags or `OFFDESK_REMOTE_OPERATOR_WORKSPACE_ROOTS`.
If neither is set, the adapter scans the nearest `Workspace` directory when one
is available.

For a user-level service:

```bash
scripts/install_offdesk_telegram_operator_service.py \
  --install \
  --enable \
  --restart \
  --include-watchdog \
  --env-file /path/to/telegram.env \
  --forager-bin "$PWD/target/debug/forager"

systemctl --user status forager-telegram-operator.service
systemctl --user status forager-telegram-operator-watchdog.timer
```

Run the same installer with `--dry-run` first to inspect the generated units.
Dry-run does not copy scripts, write units, or call `systemctl`. A real install
copies both the listener and watchdog into
`~/.local/share/forager/scripts/`; both units execute from that stable copy so
moving or switching the source checkout does not break the running service.
The installer refuses to deploy from a dirty Git checkout by default. Use a
clean checkout or verified release bundle for normal upgrades.
`--allow-dirty-source` exists only for an intentional development deployment.

The watchdog is separate from the listener. It reads the listener loop-status
file, checks the user service state, sends at most one compact emergency
Telegram alert per alert window, and reports concrete local recovery commands.
If the listener is stale or the service is failed, the alert says plainly that
remote/offdesk operation is currently not reliable instead of pretending the
night run can continue.

### Phase 2: Remote Envelope And Receipts - Partially Implemented

- Implemented: plan-session receipts for project init preview/run, plan draft,
  plan registration, and plan review.
- Implemented: unsupported command attempts are rejected instead of routed to
  shell or launch surfaces.
- Remaining: full `remote_command_envelope.v1`, nonce and TTL validation for
  every callback, and a uniform receipt inspector for all remote receipts.

### Phase 3: Plan Mode Bridge - Partially Implemented

- Implemented: freeform planning text becomes a reviewable decision-inbox item
  or a Plan Mode session.
- Implemented: project candidates can be selected by button or direct typing.
- Implemented: project initialization preview/run receipts are stored locally.
- Implemented: bounded plan draft generation and `forager offdesk plan
  --dry-run --json` validation.
- Implemented: plan registration through `forager offdesk plan --json`.
- Remaining: broader portfolio ranking beyond the current candidate session,
  richer plan comparison, and uniform observed-hash display for all summaries.

### Phase 4: Plan Review Bridge - Partially Implemented

- Implemented: `계획 승인` records
  `forager offdesk plan-review <plan-id> --decision approved --json`.
- Implemented: approval re-hashes the registered plan source and rejects a
  stale source before recording the review.
- Implemented: plan review does not build launch-preparation packets, enqueue
  work, or start runtime execution.
- Remaining: revision-required and rejected decisions, and complete callback
  nonce/TTL checks outside the guarded dispatch confirmation surface.

### Phase 5: Launch-Prep And Gate Bridge - Partially Implemented

- Implemented: Telegram `실행 준비 검토` runs
  `forager offdesk plan-launch-prep <plan-id> --review-id <review-id> --json`
  after an approved plan review.
- Implemented: `telegram_remote_plan_launch_prep.v1` receipts keep
  `approval_authorized`, `gate_approval_authorized`, `execution_authorized`,
  `enqueue_authorized`, and `runtime_authorized` false.
- Implemented: Telegram `게이트 요청` runs
  `forager offdesk gate dispatch.runtime ... --json` to create a pending local
  approval row.
- Implemented: `telegram_remote_plan_gate_request.v1` receipts keep
  `approval_authorized`, `gate_approval_authorized`, `execution_authorized`,
  `launch_authorized`, `enqueue_authorized`, and `runtime_authorized` false.
- Implemented: Telegram `게이트 승인` and `게이트 거절` resolve only the exact
  matching pending approval after checking `approval_id`, action, project,
  request, task, source surface, and launch-prep hash.
- Implemented: `telegram_remote_plan_gate_resolution.v1` receipts keep
  `execution_authorized`, `launch_authorized`, `enqueue_authorized`, and
  `runtime_authorized` false.
- Implemented: Telegram `실행 브리프 생성` writes a bounded
  `EXECUTION_BRIEF.json` with `approved=true` and
  `allowed_runtime_mutations=["dispatch.runtime"]` for the exact approved
  gate context.
- Implemented: `telegram_remote_plan_execution_brief.v1` receipts keep
  `execution_authorized`, `launch_authorized`, `enqueue_authorized`, and
  `runtime_authorized` false.
- Implemented: Telegram `큐 등록 검토` writes
  `PLAN_ENQUEUE_HANDOFF.json` with a local-review enqueue command template
  that still requires a reviewed workload command.
- Implemented: `telegram_remote_plan_enqueue_handoff.v1` receipts keep
  `execution_authorized`, `launch_authorized`, `enqueue_authorized`, and
  `runtime_authorized` false.
- Implemented: Telegram accepts a `prepared_task.json` path after handoff and
  verifies the prepared workload kind, preflight readiness, exact
  project/request/task match, `dispatch.runtime` capability, wrapper existence,
  and execution-brief hash.
- Implemented: `telegram_remote_plan_workload_binding.v1` receipts write
  `bound_enqueue_args` for local review while keeping `execution_authorized`,
  `launch_authorized`, `enqueue_authorized`, and `runtime_authorized` false.
- Implemented: Telegram `큐 등록 실행` runs only the exact bound
  `offdesk enqueue dispatch.runtime` argv after rechecking prepared workload
  and execution brief hashes.
- Implemented: `telegram_remote_plan_enqueue_run.v1` receipts keep
  `execution_authorized`, `launch_authorized`, and `runtime_authorized` false.
- Implemented: CLI `forager offdesk tick` accepts `--project-key` and
  `--task-id` filters so remote runtime start can target one queued task rather
  than sweeping the full queue.
- Implemented: Telegram `실행 시작` runs only task-scoped
  `offdesk tick --project-key <project> --task-id <task> --limit 1 --json`
  after rechecking prepared workload and execution-brief hashes.
- Implemented: `telegram_remote_plan_runtime_start.v1` receipts keep
  `closeout_authorized` and `accepted_truth_authorized` false.
- Implemented: Telegram `실행 상태 확인` runs only task-scoped
  `offdesk tick --project-key <project> --task-id <task> --limit 0 --json`
  and then reads the same task with `offdesk tasks --project-key <project>
  --task-id <task> --json`.
- Implemented: `telegram_remote_plan_runtime_monitor.v1` receipts keep
  `dispatch_authorized`, `closeout_authorized`, and
  `accepted_truth_authorized` false.
- Implemented: Telegram `마무리 패킷 생성` runs only
  `offdesk closeout --project-key <project> --task-id <task> --dry-run --json`
  after a completed task-scoped monitor receipt.
- Implemented: `telegram_remote_plan_closeout_packet.v1` receipts keep
  `closeout_review_authorized`, `file_mutation_authorized`, and
  `accepted_truth_authorized` false.
- Implemented: Telegram `마무리 검토 준비` writes only a bounded
  `closeout-review` handoff with verdict command templates.
- Implemented: `telegram_remote_plan_closeout_review_handoff.v1` receipts keep
  `remote_closeout_review_authorized`, `file_mutation_authorized`, and
  `accepted_truth_authorized` false.
- Implemented: Telegram `승인 기록`, `수정 요청 기록`, and `차단 기록` run only
  `closeout-review --verdict approved|revise|blocked` from the reviewed
  handoff.
- Implemented: `telegram_remote_plan_closeout_verdict.v1` receipts keep
  `project_file_mutation_authorized` and `file_mutation_authorized` false, and
  record `accepted_truth_recorded` only when the CLI receipt status is
  `accepted`.

### Phase 6: Runtime Monitor Readout - Implemented

- Poll only the already-started task by `project_key` and `task_id`.
- Use `--limit 0` so the monitor path cannot dispatch queued work.
- Show compact status, blocker, and next-safe-action summaries.
- Keep closeout and accepted-truth authority out of the monitor action. Those
  transitions require the later closeout handoff and verdict phases.

### Phase 7: Closeout Packet Bridge - Implemented

- Generate closeout artifacts only for the completed monitored task.
- Keep generated closeout artifacts read-only and review-required.
- Do not run `closeout-review`, file operations, or accepted-truth decisions.

### Phase 8: Closeout Review Handoff - Implemented

- Show local `closeout-review` command templates for `revise`, `blocked`, and
  `approved`.
- Warn that `approved` can create accepted truth when the closeout has no
  follow-ups.
- Do not run `closeout-review` until a verdict button is selected.

### Phase 9: Closeout Verdict And Accepted Truth Bridge - Implemented

- Allow Telegram to record `approved`, `revise`, or `blocked` closeout-review
  verdicts from the prepared handoff only.
- Treat accepted truth as the CLI closeout receipt result, not as a generic
  Telegram approval.
- Keep project-file mutation authorization false.
- Keep CLI as recovery path for all state.

## Operator Runbooks

### Start A Planning Cycle From Telegram

Use this when away from the desk and the goal is to prepare a reviewed plan,
not launch runtime work.

1. Send a freeform planning request, for example "tonight, inspect which
   project should get Offdesk time".
2. Review the compact project candidates.
3. Select a project by button, or type a project number/name directly.
4. Tap `초기화 검토` to create a local project-init preview receipt.
5. Tap `초기화 생성` only after the preview is acceptable.
6. Tap `계획 초안 생성` to create and dry-run validate the plan draft.
7. Tap `계획 등록` to write the local Offdesk plan registry entry.
8. Tap `계획 승인` only if the plan direction is acceptable.
9. Tap `실행 준비 검토` to create the read-only launch-preparation packet.
10. Tap `게이트 요청` to create a pending local `dispatch.runtime` approval.
11. Tap `게이트 승인` or `게이트 거절` to resolve that exact approval.
12. Tap `실행 브리프 생성` to write the bounded `ExecutionBrief` file.
13. Tap `큐 등록 검토` to write a local enqueue handoff template.
14. Type the reviewed `prepared_task.json` path to bind the workload packet.
15. Tap `큐 등록 실행` to enqueue the bound task.
16. Tap `실행 시작` to start only that queued task.
17. Tap `실행 상태 확인` to poll/read only that same task.
18. If the task is completed, tap `마무리 패킷 생성` to create closeout
    artifacts only.
19. Tap `마무리 검토 준비` to write local `closeout-review` verdict
    templates without running a verdict.
20. Tap `승인 기록`, `수정 요청 기록`, or `차단 기록` only if that closeout
    verdict is intended.

After step 17, the task may be running, completed, failed, or waiting for
recovery. After step 18, closeout artifacts exist. After step 19, local review
commands exist. After step 20, the selected closeout verdict is recorded, and
accepted truth is present only if the resulting `closeout_receipt.v1`
acceptance status is `accepted`.

### Continue After Plan Approval

Plan approval can be followed by Telegram `실행 준비 검토` or by the equivalent
local command:

```bash
forager offdesk plan-launch-prep <plan-id> --json
```

After the launch-preparation packet exists, the handoff point is local gate
request and approval review. Telegram can create the request, or the same
request can be made locally:

```bash
forager offdesk gate ... --json
```

After the pending approval exists, approval and runtime progression remain
separate:

```bash
forager offdesk ok <approval-id>
forager offdesk tick --project-key <project-key> --task-id <task-id> --limit 1 --json
```

Telegram can now replace the local `ok/cancel` step only for the exact matching
approval it created. It can also enqueue only the reviewed bound task. Launch
uses task-scoped tick only; monitor uses task-scoped tick with `--limit 0`.
Closeout packet creation can run only after a completed task-scoped monitor.
Closeout review remains local until the next bridge is implemented.

### Respond To A Watchdog Alert

The watchdog alert is deliberately blunt. If it says remote operation is
unreliable, assume Telegram cannot safely drive the night run.

1. Check the listener service:

   ```bash
   systemctl --user status forager-telegram-operator.service
   ```

2. Restart the listener if the service is failed or inactive:

   ```bash
   systemctl --user restart forager-telegram-operator.service
   ```

3. Recheck the listener health:

   ```bash
   scripts/offdesk_remote_operator_telegram.py \
     --health \
     --env-file /path/to/telegram.env \
     --loop-status-file ~/.cache/forager/remote_operator_telegram_loop.json
   ```

4. Recheck the watchdog:

   ```bash
   scripts/offdesk_remote_operator_watchdog.py \
     --dry-run \
     --env-file /path/to/telegram.env \
     --loop-status-file ~/.cache/forager/remote_operator_telegram_loop.json
   ```

Resume remote planning only after health is `healthy` or the remaining
degradation is explicitly acceptable for read-only inspection.

## Acceptance Criteria

Remote Operator is ready for practical planning use when:

- no remote message can execute arbitrary shell text;
- plan approval and launch approval are separate receipts;
- project candidates are comparable across workspaces;
- every remote-visible state can be inspected through CLI;
- Telegram outage does not prevent local recovery;
- planning requests can produce registered, reviewable plans without launching
  runtime work;
- approved plans can produce launch-preparation packets without approving a
  gate, enqueue, launch, or runtime dispatch;
- launch-preparation packets can produce and resolve exact pending gate
  approvals without launching work;
- pending gate approvals can be approved or denied without enqueueing,
  launching, or ticking runtime work;
- approved gate context can produce an execution brief without enqueueing,
  launching, or ticking runtime work;
- execution briefs can produce local enqueue handoff templates without
  enqueueing, launching, or ticking runtime work;
- enqueue handoff receipts can bind reviewed prepared workload packets without
  enqueueing, launching, or ticking runtime work;
- bound workload packets can enqueue a queued task without launching or ticking
  runtime work;
- queued bound tasks can start through task-scoped tick without monitoring,
  closeout, or accepted-truth authority;
- watchdog alerts make stale or failed listener state visible outside the
  listener process.

Remote Operator is ready for runtime/overnight use only after:

- stale callbacks are rejected and recorded through a full remote command
  envelope;
- every mutation-capable callback binds to an observed hash;
- launch-preparation packets, gate requests, approval resolution, execution
  brief generation, enqueue, and runtime launch remain distinct from
  plan-review approval;
- Telegram can observe heartbeat, blockers, and closeout readiness without
  claiming accepted truth;
- closeout produces a reviewable morning package;
- tests cover duplicate callbacks, expired approvals, hash mismatch, observer
  denial, and owner-only abort.
