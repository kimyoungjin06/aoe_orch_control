# Web Dashboard Control Plane TODO

Updated: 2026-06-19

This is the long-term implementation backlog for turning the current WebUI
review route into a workstation-level Forager operations dashboard. The goal is
not to embed a terminal in the browser. The goal is to make project state,
decisions, provenance, action boundaries, and accepted truth visible and
operable without forcing the operator to decode terminal scrollback.

## Product Position

The Web UI should be a control room for one Forager workstation and, later, a
team-scale manager surface across multiple workstations. It competes with a
terminal only where the terminal is weak:

- seeing all active projects at once;
- finding the next decision that needs a human;
- understanding why a task is blocked;
- tracing plan -> task -> artifact -> closeout -> receipt -> accepted truth;
- executing only structured, receipt-backed Forager actions;
- asking a context-aware assistant about evidence and next safe actions.

The terminal and VSCode remain the raw-power fallback for debugging, unusual
commands, git work, and feature development.

## Design Principles

- Keep the first viewport calm: show health, attention, and the top next safe
  action, not every possible button.
- Treat actions as contextual: each card or drawer should expose only the
  actions that make sense for that state.
- Preserve progressive disclosure: summary first, evidence second, raw JSON and
  CLI fallback last.
- Never make the chatbot an action bypass. It may explain, search, summarize,
  and propose an action card; execution still goes through the same action
  envelope and receipt checks.
- Distinguish facts from inferences:
  - live state;
  - last observed state;
  - stale state;
  - receipt-backed fact;
  - inferred summary.
- Prefer scoped provenance graphs over full hairball graphs. The default graph
  should show the selected project, decision, task, artifact, or closeout and
  its immediate support chain.
- Keep project-file mutation, runtime mutation, closeout verdicts, and accepted
  truth visibly separate.

## Information Architecture

Use a small top-level navigation set:

- `Overview`: workstation health and top attention.
- `Work`: project portfolio, active runs, blocked runs, and closeout state.
- `Decisions`: the human decision inbox and action center.
- `Graph`: scoped work/knowledge provenance graph.
- `Settings`: profiles, workspace roots, providers, Telegram, watchdog, local
  model, and capacity.

Do not create a separate top-level page for every entity type. Runs, projects,
artifacts, receipts, and closeouts should open in detail drawers or stable
detail routes from these top-level areas.

## Core Read Models

### P0 - `workstation_surface.v1`

Goal: one read model that answers "is this workstation healthy, and what needs
attention now?"

Fields:

- `schema`
- `generated_at`
- `workstation_id`
- `profile`
- `workspace_roots`
- `health`
  - Telegram listener
  - watchdog
  - local LLM
  - configured provider
  - GPU endpoint if present
  - queue/runtime store freshness
- `capacity`
  - active background runners
  - queued tasks
  - provider-deferred tasks
  - token or budget signals when available
- `attention_counts`
  - pending approvals
  - decision inbox items
  - blocked tasks
  - failed tasks
  - closeout required
  - accepted truth missing
  - follow-up decisions required
- `top_attention`
- `next_safe_actions`
- `stale_state`
- `source_refs`

Done when:

- `/dashboard` can render from this model without reading unrelated local files.
- The top attention item matches `forager status --json` or a documented
  stronger source.
- The model explicitly says when data is stale or unavailable.

### P0 - `decision_inbox_surface.v1`

Goal: make human decisions the primary action center.

Decision categories:

- plan review;
- gate request;
- workload binding;
- runtime recovery;
- closeout packet review;
- closeout verdict;
- closeout follow-up decision;
- accepted truth recovery;
- adaptive wiki or knowledge promotion;
- provider/model outage triage.

Each decision item should include:

- `decision_id`
- `kind`
- `severity`
- `project_key`
- `what_changed`
- `why_now`
- `risk`
- `evidence_refs`
- `allowed_actions`
- `authorization_boundary`
- `stale_guard`
- `receipt_ref`
- `cli_fallback`

Done when:

- Every dashboard action starts from a decision item or a clearly scoped entity
  detail.
- The default view can be sorted by urgency without hiding blocked work.

### P1 - `project_portfolio_surface.v1`

Goal: show all projects as a manager-level portfolio rather than as a single
phase label.

Each project should expose parallel state chips:

- plan;
- runtime;
- decisions;
- closeout;
- accepted truth;
- wiki or knowledge state;
- last artifact;
- last activity.

Done when:

- A project can simultaneously show `plan=approved`, `runtime=running`,
  `closeout=not_ready`, `truth=missing`, and `decisions=1` without collapsing
  into a misleading single phase.

### P1 - `action_envelope.v1`

Goal: define the Web UI execution boundary before adding mutation buttons.

Fields:

- `action_id`
- `action_kind`
- `profile`
- `project_key`
- `target_ref`
- `observed_hash`
- `nonce`
- `ttl`
- `issued_at`
- `expires_at`
- `idempotency_key`
- `preview`
- `allowed_command`
- `forbidden_effects`
- `expected_receipt_schema`
- `requires_confirmation`
- `confirmation_phrase`
- `stale_rejection_reason`

Rules:

- No arbitrary shell input.
- No action without an observed target and receipt expectation.
- No accepted-truth action unless the closeout receipt or verdict path is
  explicit.
- Repeated clicks must be idempotent or produce a clear duplicate receipt.

Done when:

- Web UI, Telegram, and TUI can describe the same action boundary.
- A failed or stale action produces a durable receipt instead of silently doing
  nothing.

### P1 - `work_graph_surface.v1`

Goal: provide provenance and knowledge graph visibility without overwhelming
the operator.

Default graph scope:

`Plan -> Task -> Artifact -> Closeout -> Receipt -> Accepted truth`

Node types:

- workstation;
- project;
- plan;
- approval;
- gate;
- task;
- run;
- artifact;
- closeout packet;
- closeout receipt;
- decision record;
- accepted truth;
- wiki or knowledge entry;
- agent;
- provider;
- model.

Edge types:

- created;
- approved;
- queued;
- executed;
- produced;
- supports;
- blocks;
- supersedes;
- accepted;
- needs decision;
- generated by.

Done when:

- The graph can answer "what evidence supports this accepted truth?"
- The default view is scoped to the selected item, not the whole repository.

### P2 - `chat_context_surface.v1`

Goal: make the assistant useful inside the dashboard without bypassing the
control plane.

The chatbot can:

- summarize current state;
- explain a decision;
- trace provenance;
- compare projects by risk or attention;
- find artifacts;
- propose a next action card;
- draft a plan or closeout review note.

The chatbot cannot:

- run shell commands directly;
- mutate files directly;
- approve, enqueue, launch, close out, or accept truth outside
  `action_envelope.v1`;
- hide whether an answer is receipt-backed or inferred.

Done when:

- Every assistant answer cites state refs, receipt refs, or explicitly marks
  itself as an inference.
- Suggested actions appear as reviewable action cards, not as direct execution.

## Screen-Level TODO

### P0 - `/dashboard`

Purpose: calm workstation overview.

First viewport:

- workstation health;
- active work count;
- blocked and failed count;
- decision needed count;
- closeout and accepted-truth count;
- top attention item;
- two primary actions at most:
  - review top item;
  - open decisions;
- assistant entry with current dashboard context.

Avoid:

- raw command lists;
- more than three primary buttons;
- unscoped logs;
- full graph visualization.

### P0 - `/decisions`

Purpose: the primary action center.

Layout:

- decision queue;
- selected decision detail;
- evidence and receipt refs;
- allowed actions;
- authorization boundary;
- CLI fallback in a collapsed detail section.

Action rule:

- show one to three actions per selected decision;
- if there are more actions, group them under a secondary menu;
- destructive or accepted-truth actions require a stronger confirmation
  phrase.

### P1 - `/work`

Purpose: project portfolio and active work monitor.

Layout:

- project table or lane list;
- state chips per project rather than a single project phase;
- active task drawer;
- closeout and accepted truth status;
- filters for blocked, running, attention, accepted, and stale.

Status: implemented as a first selectable portfolio route. `/work/` now shows
parallel project state chips, selected-project provenance, read-only assistant
prompts, and an attention path that consolidates decision/runtime/truth blockers
with state refs, fallback commands, and action boundaries. It also includes
read-only project filters for all, attention, blocked, running, review,
recovery, accepted, stale, and truth-gap views plus a read-only active-task drawer that prefers exact
`workstation_surface.v1` task-store rows from `projects[].task_items`, falling
back to decision/runtime/truth/project-row state only when task-store rows are
absent. Task-store rows now include compact inspection details for runner,
ticket, gate, attempts, artifacts, mode, provider, and errors so operators can
judge whether to monitor, review, or recover without opening raw logs. The same
drawer now surfaces task-linked action records from decision envelope receipts,
decision executions, decision closeouts, and runtime dispatch receipts when
those append-only records can be matched to the task.

### P1 - `/graph`

Purpose: scoped provenance and knowledge graph.

Default:

- show selected project or decision support graph;
- one to two hops by default;
- highlight blocker and accepted-truth paths;
- offer "expand" only as an advanced action.

Status: implemented as a first read-only route. `/graph/` now renders
project-scoped support paths, blocker lists, and evidence refs from
`workstation_surface.v1`, with fixture fallback and live source-status handling.
Expandable multi-hop graph exploration remains intentionally deferred.

### P1 - `/settings`

Purpose: read-only workstation readiness and source-contract surface.

Default:

- show Telegram, local LLM, runtime store, and other health checks;
- show capacity signals such as active runners, queued tasks, provider-deferred
  work, and budget warnings;
- list known workspace roots and source refs from `workstation_surface.v1`;
- keep edits and runtime changes out of the Web UI settings page.

Status: implemented as a first read-only route. `/settings/` now hydrates from
`workstation_surface.v1`, shows source-status warnings, exposes workspace roots
and source refs, and documents guardrails without adding config mutation
controls.

### P2 - Assistant Panel

Purpose: context-aware explanation and action proposal.

Entry points:

- overview context;
- selected project;
- selected decision;
- selected graph node;
- selected receipt or artifact.

Suggested prompts:

- "What needs my attention?"
- "Can this be approved?"
- "What evidence supports this?"
- "What blocks this project?"
- "Show the provenance path."
- "Draft a safe action note."

Status: first deterministic read-only answer surface implemented.
`workstation_surface.v1` now carries `chat_context_surface.v1` with overview
project, and selected-decision scopes, cited answers, inference notes, prompt
seeds, and review-only suggested action cards. `/dashboard/`, `/work/`, and
`/decisions/` render those answer/action surfaces while keeping copyable prompts
as a fallback. Retrieval-backed answers, graph-node/receipt scopes, and
executable action-card proposal preflight loops remain deferred.

## Risk Register

- Button overload: mitigated by contextual actions and detail drawers.
- Stale dashboard state: mitigated by generated timestamps, stale banners, and
  observed-hash rejection.
- Chatbot action bypass: mitigated by action cards and envelopes.
- Misleading single project phase: mitigated by parallel state chips.
- Graph hairball: mitigated by scoped default graph.
- Duplicate clicks: mitigated by idempotency keys and receipt identity.
- Hidden terminal dependency: mitigated by collapsed CLI fallback on every
  action.
- Over-broad backend authority: mitigated by a local-only Forager control API
  or action packet processor with explicit schemas.

## Current Completeness Review - 2026-08-12

Current baseline checked with `npm run test:visual`: 32 Playwright tests pass
across desktop and mobile. Visual tests now build and run under a website build
lock, use an isolated Playwright preview port, keep Astro 7 in foreground mode
inside detected agent environments, and bound worker concurrency. This reduces
stale-preview, concurrent-`dist`, and resource-contention failures. This is now
a useful route-level smoke baseline, but it still misses deeper product gaps
such as backend action API wiring, a real assistant answer surface, and expanded
graph exploration.

### P0 - Make `/decisions/` a selectable action center

Current gap:

- The queue renders decision and runtime handoff cards as static `<article>`
  elements. Only the first open decision drives the detail panel.
- Runtime handoff detail is only reachable when there are no open decisions.
- Tests assert the first detail and details disclosure only; they do not click
  queue items or verify selection state.

Todo:

- Render decision queue rows and runtime handoff rows as accessible buttons or
  links with `aria-pressed` or equivalent selected-state semantics.
- Track a selected item id in client state and render the matching decision or
  runtime handoff detail.
- Preserve the strict boundary: selection changes detail context only; it does
  not run an action, preflight, dispatch, or closeout.
- Add Playwright coverage for:
  - selecting the second decision;
  - selecting a runtime handoff while decisions still exist;
  - keyboard focus and activation;
  - mobile selection without horizontal overflow.

Done when:

- Every visible item in the decision queue can be inspected from the same page.
- Runtime handoffs are inspectable without clearing the decision inbox first.
- Tests would fail if `/decisions/` regressed to a first-item-only detail view.

Status: implemented for the current `/decisions/` route. Queue items now select
detail context only, runtime handoffs are inspectable alongside open decisions,
and Playwright covers second-decision and runtime-handoff selection.

### P0 - Surface live, fallback, stale, and failed-load states explicitly

Current gap:

- `/review/`, `/dashboard/`, and `/decisions/` silently render fixture fallback
  when the live JSON fetch fails.
- The source label is visible, but a failed live fetch does not produce a clear
  operator-facing warning.
- This is risky because the dashboard may look usable while it is no longer
  reading current workstation state.

Todo:

- Add a shared source-status banner component or projection helper with states:
  `live`, `fixture_fallback`, `live_fetch_failed`, `stale_live`, and
  `missing_surface_url`.
- On fetch failure, preserve the fallback UI but show a compact warning near the
  page header with the attempted URL, failure class, and safe CLI refresh path.
- Keep raw error text out of the primary card unless it is short and actionable.
- Add tests that route a 404/500/network failure and assert that fallback is
  visibly marked as fallback or failed-live state.

Done when:

- An operator can tell at a glance whether the page is live, stale, fixture-only,
  or degraded.
- A failed live fetch can no longer be mistaken for a fresh workstation state.

Status: implemented for `/review/`, `/dashboard/`, `/work/`, `/graph/`,
`/settings/`, and `/decisions/` with a shared source-status banner and
failed-live fallback tests.

### P0 - Repair dashboard project portfolio density

Current gap:

- Desktop screenshots show the selected project detail card becoming too narrow;
  the `Plan / Runtime / Closeout / Truth` chips overlap or become unreadable.
- Mobile screenshots do not overflow horizontally, but status lines and project
  details are too dense to function as a mobile manager surface.
- The current tests catch page-level overflow, not internal compression,
  truncation, or chip readability.

Todo:

- Keep the project list/detail layout single-column until a wider breakpoint, or
  give the detail card more width before switching to two columns.
- Use two state-chip columns at normal desktop widths and reserve four columns
  for wide desktop only.
- Replace long status strings in the project list with semantic chips or short
  labels, moving full detail into the selected panel.
- Add visual/DOM checks for minimum chip widths, non-overlapping chip text, and
  readable selected project detail on desktop and mobile.

Done when:

- The selected project detail can show plan, runtime, closeout, truth, decisions,
  runtime handoffs, and truth items without overlapping or relying on tiny text.
- Mobile users can identify the selected project, blocker, and next action before
  opening details or scrolling through raw identifiers.

Status: implemented for the current dashboard. Project list rows now use compact
state chips, selected-project state chips no longer force four columns at normal
desktop widths, and Playwright checks chip overflow on desktop/mobile.

### P1 - Make assistant prompts honest controls

Current gap:

- Assistant prompt chips look like buttons but have no click behavior.
- The panel correctly says read-only, but the controls still imply a missing
  action.

Todo:

- Choose one behavior for the read-only phase:
  - copy prompt to clipboard and show a short status message;
  - open a local prompt composer drawer that still cannot execute actions; or
  - render prompts as non-button chips until the assistant panel is wired.
- Keep suggested actions as action-card proposals only; no direct execution path
  from chat or prompt chips.
- Add a small browser test for the chosen behavior.

Done when:

- Prompt controls either do something visible and bounded, or they no longer look
  like interactive action buttons.

Status: implemented as bounded copy controls on `/dashboard/` and `/work/`. If
clipboard access is unavailable, the UI shows the prompt text as a ready-to-copy
fallback instead of silently doing nothing.

### P1 - Improve visual review artifacts

Current gap:

- Full-page screenshots capture the fixed top nav over the middle of long pages,
  which makes the artifact harder to review.
- `/decisions/` has no saved screenshot artifact even though it is now the main
  action-center route.

Todo:

- Capture viewport screenshots for fixed-header layout checks and full-page
  screenshots for document-length checks, or hide fixed nav only for full-page
  artifact capture.
- Add desktop and mobile `/decisions/` screenshots to the visual smoke suite.
- Add one narrow-width dashboard screenshot after the project portfolio layout is
  repaired.

Done when:

- Screenshot artifacts are useful for human review, not just byte-size smoke
  assertions.

Status: partially implemented. `/decisions/`, `/work/`, `/graph/`, and
`/settings/` desktop/mobile screenshots are now captured, and visual smoke
screenshots stabilize the fixed nav during full-page artifact capture. A future
visual pass can still add more targeted narrow-width portfolio artifacts.

### P1 - Normalize Web UI information hierarchy

Current gap:

- `/dashboard/` still mixes overview, project portfolio, decision inbox, truth
  recovery, graph, assistant, health, and capacity in one long page.
- This is acceptable for a read-only prototype, but it will become exhausting as
  soon as more projects and actions are present.

Todo:

- Keep `/dashboard/` focused on workstation health, top attention, project
  portfolio summary, and two primary navigation actions.
- Move deeper action details to `/decisions/` and scoped evidence detail to
  `/review/` or a future selected-entity detail route.
- Decide whether `/work/` should become a real project portfolio route before
  adding more project cards to `/dashboard/`.

Done when:

- The first viewport answers "what needs attention now?" without requiring the
  operator to parse every subsystem at once.
- Detail-heavy sections are reachable but not all permanently expanded on the
  dashboard.

Status: implemented for the first route split. `/dashboard/` now stays focused
on workstation health, top attention, decision/truth summaries, and project
attention counts. `/work/` now owns the selectable project portfolio, scoped
provenance graph, and read-only assistant context, with desktop/mobile visual
coverage.

## Suggested Implementation Order

1. Define `workstation_surface.v1` and export it from existing local status,
   tasks, plans, closeout, Telegram loop, and review-surface sources.
2. Create `/dashboard` as a read-only static/hydrated route backed by
   `workstation_surface.v1`.
3. Define `decision_inbox_surface.v1` and render `/decisions` as read-only.
4. Define `action_envelope.v1` and implement preview-only action cards.
5. Wire low-risk actions through the envelope with nonce, TTL, observed hash,
   idempotency key, and receipt inspection.
6. Add project portfolio state chips through `project_portfolio_surface.v1`.
7. Add scoped provenance graph through `work_graph_surface.v1`.
8. Add dashboard-aware assistant in read-only mode.
9. Let the assistant propose action cards, still executed only through
   `action_envelope.v1`.

## First Slice

Implement `workstation_surface.v1` without adding any new mutation button.

Current status:

- Done: documented the long-term dashboard/control-plane backlog.
- Done: added an `attention` fixture for `workstation_surface.v1`.
- Done: added a typed dashboard projection helper for
  `workstation_surface.v1`.
- Done: added a read-only `/dashboard/` route with fixture fallback and
  client-side hydration from `/workstation-surface.json`.
- Done: added desktop/mobile Playwright coverage for dashboard rendering, no
  horizontal page overflow, wrapped command text, and live surface hydration.
- Done: added nav and landing-page entry points to `/dashboard/`.
- Done: added `forager ondesk workstation-surface --json`.
- Done: added `npm run export:workstation-surface`, which writes
  `website/public/workstation-surface.json`.
- Done: connected top attention to `forager status --json`, Offdesk status
  summary, task queue, decision ledger, and Telegram loop-status projection.
- Done: added fixture variants for healthy idle, agent outage, active run,
  accepted closeout, and accepted-truth recovery states.
- Done: added Playwright hydration coverage for all workstation fixture
  variants, including empty-state rendering for idle/accepted decision and
  project gaps.
- Done: upgraded the `/dashboard/` project portfolio from a passive table into
  a selectable project work queue with a focused detail panel showing each
  project's plan/runtime/closeout/truth chips, decision count, runtime handoff
  count, accepted-truth recovery count, and project-specific next action.
- Done: split that deeper project work queue into `/work/`, leaving
  `/dashboard/` as a calmer overview and moving scoped graph plus assistant
  context out of the always-expanded dashboard surface.
- Done: moved the selected-project Scoped Graph to `/work/`, rendering that
  project's plan -> runtime -> closeout -> accepted-truth path plus local
  decision, runtime-handoff, and truth-recovery blockers.
- Done: moved the selected-project Assistant panel to `/work/` in read-only
  mode, with scoped context, up to three suggested prompts, and explicit state
  or receipt refs so it cannot act as an execution bypass.
- Done: added `/graph/` as a read-only scoped provenance route with project
  selector, support-path rail, blockers, evidence refs, source-status handling,
  and desktop/mobile visual coverage.
- Done: added `/settings/` as a read-only workstation readiness route with
  service health, capacity signals, workspace roots, source refs, operational
  guardrails, and desktop/mobile visual coverage.
- Done: defined `decision_inbox_surface.v1` inside
  `workstation_surface.v1`, including open/visible counts, urgency sort order,
  read-only action model, stale guard, authorization boundary, receipt policy,
  and CLI fallback.
- Done: added a read-only `/decisions/` action-center route backed by
  `/workstation-surface.json`, with desktop/mobile Playwright coverage.
- Done: defined `action_envelope.v1` preview-only cards for decision actions,
  including target ref, observed hash, nonce, TTL, issued/expiry timestamps,
  idempotency key, allowed inspection command, forbidden effects, expected
  receipt schema, confirmation phrase, and stale rejection reason.
- Done: added `forager ondesk action-envelope --envelope <PATH> --json`, which
  validates an `action_envelope.v1` against the current decision ledger and
  writes an idempotent `action_envelope_receipt.v1` acceptance or stale
  rejection receipt.
- Done: `workstation_surface.v1` reads `action_envelope_receipts.jsonl` and
  attaches the latest receipt verdict, stale reason, failed checks, and receipt
  history count to matching action cards.
- Done: `/decisions/` renders the receipt verdict block on desktop/mobile
  without exposing full logs or arbitrary command input.
- Done: added `forager ondesk action-preflight --receipt-id <ID> --json`,
  which starts from an `action_envelope_receipt.v1`, rejects stale/non-latest
  receipts, rechecks the current decision hash, and writes idempotent
  `action_execution_preflight.v1` records without mutating project/runtime
  state.
- Done: added `forager ondesk action-decision --preflight-id <ID> --note
  <TEXT> --json` as the first action-specific executor. It requires a ready
  `action_execution_preflight.v1`, supports only bounded decision choices,
  records blocked attempts idempotently, applies supported choices as
  canonical decision handoffs, and never dispatches runtime work.
- Done: `workstation_surface.v1` reads `decision_action_executions.jsonl` and
  attaches the latest `decision_action_execution.v1` result plus execution
  history count to matching action cards by project, decision, and action kind.
- Done: `/decisions/` renders applied/blocked decision action execution
  results without exposing full execution logs.
- Done: added `forager ondesk action-closeout --execution-id <ID> --json`.
  It starts from an applied `decision_action_execution.v1`, verifies the
  matching handoff-ready decision record, appends a canonical `DecisionReceipt`,
  writes `decision_action_closeout.v1`, and remains explicitly non-runtime.
- Done: `workstation_surface.v1` now projects append-only decisions through the
  latest record per decision id, so a newly `receipted` decision no longer
  remains open because of older ledger entries.
- Done: Web/mobile decision action cards expose a short closeout fallback
  command for applied handoff executions instead of full execution logs.
- Done: added `forager ondesk runtime-preflight --closeout-id <ID> --json`.
  It starts from a `decision_action_closeout.v1`, verifies the latest canonical
  decision record is `receipted` with the matching `DecisionReceipt`, and
  writes `runtime_dispatch_preflight.v1` without queueing or launching runtime
  work.
- Done: added `forager ondesk runtime-dispatch --preflight-id <ID> --runner
  <RUNNER> --cmd <CMD> --json`. It requires a ready runtime preflight, writes
  an idempotent `runtime_dispatch_receipt.v1`, and queues an `OffdeskTask`; it
  does not launch a process. Launch remains under `forager offdesk tick` and
  the existing scheduler gate.
- Done: `workstation_surface.v1` now includes a separate
  `runtime_dispatch_surface.v1` built from receipted
  `decision_action_closeout.v1` records plus latest runtime preflight/dispatch
  receipts. `/decisions/` renders this as a post-closeout handoff area with
  preflight, queue-dispatch, and scheduler-gated tick CLI fallbacks, without
  re-opening receipted decisions or turning the decision inbox into a terminal.
- Done: `workstation_surface.v1` now includes
  `accepted_truth_recovery_surface.v1` for latest closeout receipts that are
  not accepted truth. `/dashboard/` renders follow-up, blocked/revision,
  receipt-missing, and retired-incomplete states separately from the decision
  inbox, with local CLI fallbacks for `closeout-decision` and
  `closeout-retire`.
- Done: accepted-truth recovery fallbacks now carry
  `accepted_truth_recovery_action_envelope.v1` previews, and `forager ondesk
  accepted-truth-recovery-envelope --envelope <PATH> --json` validates them
  against the current recovery surface into idempotent
  `accepted_truth_recovery_action_receipt.v1` records. The dashboard still
  does not resolve follow-ups, retire closeouts, move files, promote wiki
  state, or record accepted truth.
- Done: added the first local Web interaction bridge. `npm run serve:actions`
  serves the built static Web UI from `127.0.0.1` and exposes
  `GET /api/ondesk/bridge-status` plus `POST /api/ondesk/action-envelope`;
  `/decisions/` can validate a visible `action_envelope.v1` through existing
  `forager ondesk action-envelope` logic and record an
  `action_envelope_receipt.v1`. This bridge refreshes the exported workstation
  surface after validation, but it still does not run action preflight, apply
  decisions, close out records, queue runtime work, launch processes, or
  record accepted truth.
- Done: the bridge accepts only a compact action request (`action_id`,
  `decision_id`, `observed_hash`) and reconstructs the executable envelope
  from a freshly exported operator-safe `workstation_surface.v1`; full
  envelope payloads from the browser are rejected.
- Done: same-origin and Host-header allowlist checks guard the bridge
  (including against DNS rebinding), and refreshed surface exports in
  `website/public/` are served with precedence over the stale `dist/` copy so
  a page reload recovers from `409 observed_hash_changed`. `/decisions/` now
  distinguishes bridge refusals (including 409 stale guidance) from an
  unavailable bridge.
- Done: `/decisions/` probes `GET /api/ondesk/bridge-status` on load and shows a
  readiness indicator (`data-bridge-status`), so the operator knows whether
  actions will record receipts before the first click.
- Done: `/decisions/` rehydrates from the bridge-refreshed surface after a
  successful action, so the latest receipt block updates without a manual
  reload; the action status is restored on the freshly rendered card.
- Done: Playwright coverage for bridge readiness (ready and offline states) and
  post-action rehydration in `website/tests/visual/review.spec.ts`.

Immediate hardening backlog before broader Web execution:

- Done: a real temp-profile bridge smoke drives `serve-local-actions.mjs`
  against a seeded profile and the built forager binary
  (`website/tests/bridge/`, `npm run test:bridge`). It caught a latent bug:
  `exportWorkstationSurface` did not return the parsed surface, so every real
  action POST failed; now fixed.
- Done: operator-facing decision action copy ("Validate & record receipt", a
  "Read-only preview" mode label via `actionModeLabel`) with the verbose
  observed-hash and idempotency fields behind progressive disclosure to keep
  mobile density low.
- Done: the bridge exposes `POST /api/ondesk/accepted-truth-recovery-envelope`
  (compact `action_id`, `closeout_id`, `observed_hash` request), rebuilding the
  recovery envelope from a fresh surface and running
  `forager ondesk accepted-truth-recovery-envelope`. It validates and records a
  recovery receipt but never records accepted truth; covered by the bridge
  smoke.
- P2: Wire the dashboard truth-recovery cards to the new recovery endpoint
  (a clickable "Validate & record recovery receipt" mirroring the decisions
  action controls, with bridge-status probe and rehydration), then action
  preflight, before considering any decision application or runtime dispatch
  controls.

Scope:

- Add a CLI export command or script that writes
  `website/public/workstation-surface.json`.
- Include workstation health, attention counts, top attention, and
  `next_safe_actions`.
- Add fixtures for:
  - healthy idle;
  - pending decision;
  - agent outage;
  - active run;
  - closeout accepted;
  - accepted truth missing.
- Add `/dashboard` read-only route with desktop/mobile Playwright smoke tests.
- Keep `/review/` as the selected-item detail surface.

Non-goals:

- no backend server;
- no arbitrary command execution;
- no chatbot execution;
- no full graph;
- no mutation actions.
