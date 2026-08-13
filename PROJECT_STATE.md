# Project State

Updated: 2026-08-13

This is the small current surface for Forager development work. It is separate
from the public product README and from the mdBook user guides.

## Current Focus

Forager is a local autonomy meta-harness around tmux-supervised,
harness-backed agent work. The current development focus is the generic
Offdesk operating loop, mobile Remote Operator surfaces, runtime recovery,
documentation/artifact hygiene, and keeping domain-specific validation history
out of product-facing docs. The product direction is defined in
`docs/project-direction.md`.

## First Reads For Development

- repository rules: `AGENTS.md`
- project direction: `docs/project-direction.md`
- hosted harness contract: `docs/hosted-harness-agents.md`
- product overview: `README.md`
- remote operator contract: `docs/remote-operator.md`
- operation cycle: `docs/guides/operation-cycle.md`
- current offdesk status: `docs/offdesk-operation-status.md`
- UI surface TODO: `docs/ui-surface-todo.md`
- Web dashboard control-plane TODO:
  `docs/web-dashboard-control-plane-todo.md`
- runtime smoke runbook: `docs/guides/offdesk-runtime-smoke.md`
- long-run validation runbook: `docs/guides/offdesk-long-run-validation.md`
- adaptive wiki contract: `docs/adaptive-wiki.md`

## Active Documentation Work

- Public README, mdBook introduction, brand system, and Astro landing page lead
  with the north star and local meta-harness positioning.
- The committed TUI preview must stay profile-neutral. It should not show a
  project-specific profile, domain module, or private workspace name.
- `forager project audit-docs` provides the v1 audit for governance surfaces,
  deliverables, decision sources, current freshness, latest aliases, logs,
  adaptive wiki projection freshness, and focused recommendations.
- `forager offdesk closeout` carries documentation governance recommendations
  into `RETURN_PACKAGE.md`, so `ondesk prompt-package` can surface them without
  embedding the full audit summary.
- `forager project apply-governance-hints` is the reviewed bridge from packet
  hints into target project files. It dry-runs by default, creates only missing
  governance surfaces with `--reviewed`, and never overwrites existing docs.
- `forager offdesk wiki export-markdown` defaults to the active profile's
  `wiki-vault/` projection and reports whether the markdown vault is missing,
  stale, fresh, or empty relative to canonical adaptive wiki JSON state.
- `forager doctor` and `forager status --json` expose active profile/app
  directory source, making legacy AoE storage fallback visible before
  migration.
- Telegram Remote Operator health is live for the default profile. It reports a
  polling listener, configured chat allowlist, local model availability, and a
  guarded action surface. Runtime mutation still requires the documented
  confirmation and task-binding gates.
- The external Telegram Remote Operator watchdog can run outside the listener
  process, read loop-status and systemd state, send rate-limited emergency
  alerts, and render user-service/timer units through the installer.
- Domain-specific validation notes have been moved out of mdBook product docs
  into `archive/domain-history/`. They remain available for historical
  inspection but should not shape public product language.

## Current Gaps

- The Remote Operator can guide project selection, initialization preview,
  plan draft creation, plan registration, plan-review approval, and read-only
  launch-preparation packet creation. It can also create a pending
  `dispatch.runtime` gate request, resolve that exact approval, and write a
  bounded `ExecutionBrief`, a local-review enqueue handoff receipt, and a
  prepared-workload binding receipt. It can enqueue only the bound
  `dispatch.runtime` task, start it only through task-scoped tick, and monitor
  only that same task through task-scoped tick with `--limit 0`. It can create
  a closeout packet only for that completed monitored task, then write a local
  `closeout-review` handoff with verdict command templates. It can record only
  handoff-bound `approved`, `revise`, or `blocked` closeout-review verdicts
  from Telegram. Accepted truth is recorded only when the resulting
  `closeout_receipt.v1` status is `accepted`.
- Closeout follow-up decision resolution and evidence-incomplete retirement are
  available through receipt-backed CLI workflows. Decision resolution now
  fails closed on stale tasks, missing evidence, required reads, unsafe
  operations, and unknown or pending wiki promotion state. A receipt exposes
  `accepted_scope` only when its final status is `accepted`.
- The Telegram listener now survives known transport failures, and the external
  watchdog covers stale listener, failed service, and emergency alert paths.
- Website dependencies were refreshed to close `npm audit` findings; Astro 7,
  Tailwind 4, and an esbuild security override are now part of the verified
  site build path. CI installs the lockfile with `npm ci`, runs the action
  bridge tests, rejects high-severity audit findings, builds the static site,
  and runs 32 desktop/mobile Playwright scenarios before release.
- UI surfaces now have a first shared operator-state contract, compact Telegram
  decision relay cards, semantic TUI Offdesk summaries, a fixture-backed WebUI
  review route, live `review_surface.v1` export/hydration for the WebUI route,
  Playwright desktop/mobile screenshot coverage, and a TUI preview summary.
- The long-term Web dashboard/control-plane direction is captured in
  `docs/web-dashboard-control-plane-todo.md`: workstation overview, project
  portfolio, decision inbox, scoped provenance graph, action envelope, and
  dashboard-aware assistant.
- The first Web dashboard slice is live: `forager ondesk workstation-surface
  --json`, `npm run export:workstation-surface`, and the static `/dashboard/`
  route now share `workstation_surface.v1`.
- The workstation dashboard now has fixture coverage for healthy idle, agent
  outage, active run, accepted closeout, and accepted-truth recovery states.
- The `/dashboard/` project portfolio now behaves as a selectable work queue:
  selecting a project shows a focused detail panel with plan/runtime/closeout/
  truth chips, decision/runtime/truth-recovery counts, and a project-specific
  next action derived from the existing `workstation_surface.v1`. The
  dashboard's Scoped Graph now follows the selected project and shows that
  project's plan/runtime/closeout/accepted-truth path plus local blockers. The
  read-only Assistant panel now follows the same selected project and exposes
  scoped prompts plus state or receipt refs before any future action-card work.
- The read-only Web decision action center is live: `workstation_surface.v1`
  now embeds `decision_inbox_surface.v1`, and `/decisions/` renders open
  decision records with allowed action previews, stale guards, authorization
  boundary, and CLI fallback.
- Decision action previews now carry `action_envelope.v1` cards with observed
  hashes, nonce/TTL, issued/expiry timestamps, idempotency keys, forbidden
  effects, confirmation phrases, expected receipt schema, and safe CLI
  inspection fallback. `forager ondesk action-envelope --envelope <PATH>
  --json` now validates those envelopes against the current decision ledger and
  writes idempotent `action_envelope_receipt.v1` acceptance or stale-rejection
  receipts. `workstation_surface.v1` now attaches matching latest receipt
  verdicts back onto action cards, and `/decisions/` renders those verdicts
  without exposing full logs. `forager ondesk action-preflight --receipt-id
  <ID> --json` now starts only from a validated action-envelope receipt,
  rejects stale or non-latest receipts, rechecks the current decision hash, and
  writes an idempotent `action_execution_preflight.v1` without mutating runtime
  or project state. `forager ondesk action-decision --preflight-id <ID>
  --note <TEXT> --json` is the first action-specific executor: it requires a
  ready preflight, supports only bounded decision choices, appends canonical
  decision handoff records, records idempotent `decision_action_execution.v1`
  receipts, and still does not dispatch runtime work or update accepted truth.
  `workstation_surface.v1` now attaches the latest matching
  `decision_action_execution.v1` result back onto decision action cards, and
  `/decisions/` renders applied/blocked execution summaries without exposing
  full execution logs. `forager ondesk action-closeout --execution-id <ID>
  --json` now closes an applied decision action handoff into a canonical
  `DecisionReceipt`, writes `decision_action_closeout.v1`, and keeps runtime
  dispatch/project-file/accepted-truth mutation out of this stage. The
  workstation surface projects the append-only decision ledger through the
  latest record per decision id, so receipted decisions no longer remain open
  through stale ledger entries. `forager ondesk runtime-preflight
  --closeout-id <ID> --json` now verifies a receipted closeout against the
  latest canonical decision receipt and writes `runtime_dispatch_preflight.v1`.
  `forager ondesk runtime-dispatch --preflight-id <ID> --runner <RUNNER>
  --cmd <CMD> --json` queues a durable `OffdeskTask` and records
  `runtime_dispatch_receipt.v1`; it does not launch a process, so actual
  runtime execution still passes through `forager offdesk tick` and the
  scheduler gate. `workstation_surface.v1` now exposes a separate
  `runtime_dispatch_surface.v1` for receipted post-closeout handoffs, and
  `/decisions/` renders preflight, queue-dispatch, and tick CLI fallbacks
  without re-opening the decision inbox. The workstation surface also exposes
  `accepted_truth_recovery_surface.v1` for latest closeout receipts that are
  not accepted truth, and `/dashboard/` renders those follow-up, blocked, or
  retired-incomplete states with short local CLI fallback commands plus
  receipt-backed `accepted_truth_recovery_action_envelope.v1` previews. The
  matching `forager ondesk accepted-truth-recovery-envelope --envelope <PATH>
  --json` command writes only
  `accepted_truth_recovery_action_receipt.v1` validation/stale receipts and
  never executes the fallback or records accepted truth.
- The Rust Offdesk CLI decomposition now keeps clap value parsing in
  `src/cli/offdesk/parsing.rs`; canonical decision resolution and handoff/
  receipt transitions, adaptive-wiki proposal receipt construction, and
  closeout acceptance/follow-up receipt calculation now live in
  `src/offdesk/workflow/`. Stored closeout receipt parsing also shares that
  fail-closed workflow boundary: missing or unknown evidence/wiki states map
  to `missing`/`audit_unavailable`, never review-ready accepted truth. Typed
  ordinary review, decision-resolution, and retirement record builders now own
  input normalization, receipt construction, authorization boundaries, and
  accepted scope calculation. Review records and receipts use one normalized
  evidence list, so placeholders such as `none` and `n/a` cannot appear in one
  while disappearing from the other. The CLI's `wiki_proposal_receipts.rs`,
  `closeout_records.rs`, `closeout_report.rs`, and `closeout_render.rs` modules
  now own their bounded responsibilities. The closeout record adapter covers artifact discovery, task
  freshness checks, append-only path allocation, review/receipt persistence,
  and idempotent RETURN_PACKAGE section rendering. Its accepted-truth scan fails
  closed on corrupt review JSON rather than silently allowing
  evidence-incomplete retirement. The closeout report adapter owns report
  assembly, output-directory allocation, and the five initial artifact writes.
  It allocates the default output directory only after source reads succeed and
  preflights every target before writing, so an explicit-output collision cannot
  leave a partial closeout package. The pure closeout renderer owns the plan,
  RETURN_PACKAGE, and commercial-review Markdown contracts plus their bounded
  list limits and ordering helpers. The main Offdesk CLI module retains command
  routing, human output, and evidence computation. The next staged extraction
  is implementation-packet evidence coverage calculation.
- The 2026-06-26 refactor baseline is captured in
  `docs/refactor-baseline-20260626.md`. The mutation-freeze has been lifted by
  product decision to widen the Telegram operator surface, but new remote
  execution must reuse the existing receipt-gated CLI executors rather than
  add new mutation logic, and must land in the new `scripts/telegram_operator/`
  modules instead of the monolith.
- Telegram now has a guarded remote execution surface in
  `scripts/telegram_operator/dispatch.py`: `/decisions` lists open decisions,
  `/decision <id> <action> [note]` returns a single-use confirmation token
  bound to the target's observed hash with a TTL, and `/confirm <token>`
  runs the existing `action-envelope` -> `action-preflight` ->
  `action-decision` -> `action-closeout` chain after re-checking the hash.
  `/recovery` and `/recover <closeout-id> <action> [note]` mirror this for
  accepted-truth recovery follow-ups; `/confirm` on a recovery token runs
  `accepted-truth-recovery-envelope`, which validates and records a receipt but
  stops short of recording accepted truth. `/cancel` clears a pending
  confirmation. It never records accepted truth.
- The Telegram adapter has extracted health, redaction, plan-message
  renderers, schemas, stage receipt builders, shared result plumbing
  (`base.py`), and the plan-session state machine (`plan_workflow.py`) into
  acyclic modules under `scripts/telegram_operator/`. The main adapter is now
  about 4,372 lines after later journaling, session-delivery, and plain-text
  agent work expanded it again, so decomposition is not considered complete.
- Runtime dispatch is available but opt-in: `/runtime` lists post-closeout
  handoffs and `/dispatch <closeout-id> <runner> -- <command>` queues an
  operator-supplied command, gated behind `--enable-runtime-dispatch`
  (default off; this is remote command execution). On `/confirm`,
  `runtime-preflight` re-verifies the closeout and `runtime-dispatch` queues a
  durable task that runs only through `forager offdesk tick`.
- Telegram has one-look triage: `/attention` reads the workstation surface and
  returns a single card aggregating open decisions, recovery follow-ups, and
  review-flagged tasks, with per-category counts and the single most urgent
  action first. Read-only; logic in `attention_summary`
  (`scripts/telegram_operator/notifier.py`).
- Telegram confirmation cards carry one-tap `확인`/`취소` buttons: `확인` sends a
  bare `/confirm` that confirms the single pending confirmation for the chat, so
  the operator never types the token. `/attention` also offers the top action as
  a one-tap button. The `/decisions` card renders the top open decision's action
  kinds as full `/decision <id> <action>` buttons, so a decision is handled in
  two taps (action, then `확인`). This completes the urgent-handling roadmap
  (notify -> triage -> act -> stop), now fully tap-driven.
- The Telegram poller can proactively notify: with `--attention-notify` (the
  systemd installer enables it by default) each poll scans the workstation
  surface and pushes the owner chat a deduplicated card for newly waiting
  decisions and recovery follow-ups, naming the exact command to run. The scan
  is read-only and never crashes the loop. This closes the biggest gap for
  urgent handling: the operator is told when to act instead of having to poll
  `/decisions` manually. Logic lives in
  `scripts/telegram_operator/notifier.py`.
- Telegram has an emergency stop: `/tasks` lists cancellable tasks and
  `/cancel-task <task-id> [reason]` -> `/confirm` marks a durable task
  cancelled (fail-safe; available without the runtime-dispatch opt-in). It does
  not kill an already-running background process, which the result card states
  plainly.
- Telegram has a global emergency pause: `/pause [reason]` is immediate and
  engages a persistent operator pause (`offdesk_operator_pause.json`) so
  `forager offdesk tick` holds all new dispatch (queued tasks stay queued,
  reported as `held`) while still polling existing runs. `/resume` is
  confirm-gated and clears the pause. Backed by `src/offdesk/operator_pause.rs`
  and the `forager offdesk pause`/`unpause`/`pause-status` CLI. This completes
  the emergency-stop slice of the urgent-handling roadmap (notify -> triage ->
  act -> stop).

- Telegram has a curated dispatch allowlist: `/run` lets the operator dispatch a
  named, pre-vetted command template without the free-form
  `--enable-runtime-dispatch`. Templates live in a local JSON file
  (`--dispatch-allowlist-file`); the operator only names one, and the command is
  re-resolved from the current allowlist at confirm time (removing a template
  revokes it even for an outstanding confirmation). This is the safer alternative
  to free-form `/dispatch`. Logic in `scripts/telegram_operator/allowlist.py`.

- Offdesk has event-driven learning signals (Hermes pattern #9): each denied
  approval, failed runtime task, and resume-pending recovery row emits an
  adaptive-wiki *candidate* (recommendation-only, redacted, `runtime_observed`).
  A durable cursor (`learning_signals_state.json`) emits each event once while
  the candidate store merges by claim so repeated patterns accrue
  `occurrence_count`. `run_offdesk_tick` runs the scan automatically (reports
  `learning_signals_emitted`, never fails the tick) and
  `forager offdesk learning-scan [--json]` runs it on demand. No auto-promotion;
  candidates still require the existing reviewed promotion path. Logic in
  `src/offdesk/learning_signals.rs`.

- The adaptive-wiki knowledge graph now has a servable web visualization: the
  `/knowledge` route (`website/src/pages/knowledge.astro`) renders the tag graph
  with Plotly (`plotly.js-basic-dist-min`) as a clustered network -- records
  (promoted/candidate/deprecated) grouped into regions by tag prefix, derived
  structural edges hidden by default, hover detail, read-only/advisory. Data is
  plugged in via `npm run export:wiki-graph` (`website/scripts/export-wiki-graph.mjs`
  runs `forager offdesk wiki graph --json`, enriches nodes with
  status/kind/scope/occurrence, writes the gitignored `public/wiki-graph.json`);
  the route builds from the committed `src/data/wiki-graph.sample.json` fixture and
  hydrates from the live export at runtime, mirroring the workstation-surface
  pattern. The basic Plotly bundle still adds a large (~1.1 MB) JS chunk scoped
  to the `/knowledge` page only.
- `/knowledge` is multi-view along two axes: a **profile** selector (tenant; from
  a `public/wiki-graph/index.json` manifest written by `--profiles`) and a
  **facet** filter (research vs ops) sliced within a profile. Facet is derived
  per record in the export (`facet/*` tag wins; else research modes ->research,
  development/maintenance ->ops, governance tags ->ops). `?profile=&facet=`
  deep-link the view. This realizes the "knowledge planes" model: a project's
  research knowledge and its operational (harness-use) knowledge separate by
  facet, while Forager's own operating knowledge is intended to live in a
  dedicated `forager-ops` profile. Astro dev/preview bind to all interfaces
  (`server.host`/`preview.host`) for remote access.
- Operators can author knowledge directly (e.g. from a doc review) with
  `forager offdesk wiki record-candidate --kind --scope --scope-ref --claim ...`
  which records a governed candidate (origin `operator_explicit`, signal
  `imported_doc`) via the existing `record_candidate` path; promotion stays a
  separate reviewed step. This filled the missing capture primitive (previously
  candidates came only from `/remember`, overnight ingest, or learning-signals).
  Used to seed the TwinPaper wiki from `AGENTS.md`/`README.md` (5 -> 13 entries).
- Continuous doc distillation is live: `scripts/offdesk_wiki_distiller.py`
  sends a project document plus the distillation rubric to a local
  Ollama-compatible model, verifies every candidate's evidence quote verbatim
  against the source (fabricated provenance is rejected mechanically), and with
  `--record` writes survivors as unpromoted candidates
  (`origin=background_review`, `confidence=inferred`) via `record-candidate
  --origin`. Dry-run by default; never promotes. Chat-log and session-transcript
  distillation are planned follow-ups behind the same evidence/redaction
  boundary.
- Session retrospective distillation closes the mistake-prevention loop:
  `scripts/offdesk_wiki_session_distiller.py` extracts operator corrections,
  boundaries, and preferences from a local session transcript with a local LLM,
  verifies each lesson's operator quote verbatim against the real messages,
  filters injected prompts, and records survivors as unpromoted candidates
  (failure patterns carry `signal_kind=operator_correction`, feeding correction
  records and `evaluate-recurrence`). Benchmarked against Hermes source: Hermes
  has no automatic correction extraction (model-in-the-loop memory tool only),
  so this is a deliberate divergence, not an adoption. `record-candidate` gains
  `--signal-kind`.
- Entries are editable in place: `forager offdesk wiki edit <id> [--kind]
  [--agent-mode]... [--clear-agent-modes] [--claim] [--ai-instruction]
  [--human-summary] [--evidence-ref]...` and
  `forager offdesk wiki add-tag <id> [--core-tag]... [--proposed-tag]...`. This
  lets a reviewer's reclassification, mode-scope, compress, evidence-fix, and
  retag verdicts apply without reject + re-record; each appends an audit record
  (`edit` / `retag`). The
  doc->distillation contract and review rubric live in
  `docs/adaptive-wiki-distillation.md`, validated by an A/B test (18-40% less
  projected context) and an independent reviewing-agent pass.

- Commute autonomy (퇴근→출근) is wired end-to-end: idle-watch in the Telegram
  listener proposes arming a bounded overnight window (one confirm card;
  operator tap is the only approval; backoff + quiet-hours + pause/pending
  suppression), `offdesk_autonomy_run.py` manages the armed state
  (auto-expiring, /pause-independent), and three armed-gated systemd timers
  run the night: tick heartbeat every 10 min, nightly distillation playbook
  at 02:00, morning brief at 08:50 which then disarms. Timers are installed
  by `install_offdesk_autonomy_timers.py` and are inert unless armed. Expired
  pending confirmations are cleared by the proposal scan instead of blocking
  future proposals.

- Telegram freeform chat is grounded: every plain-text message hands the local
  chat agent an `operator_snapshot` (workstation surface summary, health,
  open decisions, workspace project folder hints, autonomy state) plus the
  complete `supported_commands` surface from `routing.py::COMMAND_SURFACE`,
  and `scrub_unknown_commands()` rewrites hallucinated slash commands to
  `/help`. Before this the agent answered state questions with "cannot check"
  and invented commands like `/list` that bounced as unsupported. `/projects`
  now renders the registered portfolio, and `/projects <project>` selects one
  project through the same registry resolver. `/guide` (aliases `/qna`,
  `/usage`, `/commands`) renders the
  full grouped command reference sheet from the same `COMMAND_SURFACE` truth
  table under a relaxed reference-sheet budget; `/help` stays a 5-line card
  and points to it.

- Multi-project orchestration now has a single project registry:
  `~/.config/forager/projects.toml` (`forager_project_registry.v1`) maps each
  project key to workspace path patterns, human-language aliases, forager
  session group, and wiki profile. Consumers: the Telegram chat snapshot
  (`registered_projects`), the
  session miner (`--use-project-registry`, replacing hardcoded
  `--project-map` lines in the nightly playbook), and the web portfolio
  surface (`website/scripts/export-portfolio.mjs` ->
  `public/portfolio-surface.json` -> `/portfolio` page: one row per project
  with live session counts by tool, wiki plane entry/candidate counts, and a
  link into `/knowledge?profile=...`). Loader:
  `scripts/telegram_operator/projects.py`. Telegram `/wiki` aggregates
  candidate pressure across these planes, `/wiki <key>` opens one plane and
  sets chat focus, and focused `/remember` writes a project-scoped candidate
  into that plane. A general `/p <key>` prefix remains a later convenience.

- Telegram chat now has project focus grounding: a plain-text message that
  names a registered project (key, display name, or folder alias,
  case-insensitive) resolves to that project, and the chat agent receives a
  `project_focus` block with the project's live sessions (from
  `forager status --json` rows), session counts, and its wiki plane's entry
  candidate/promoted counts, recent candidates, and recent promoted claims.
  The focus is sticky per chat for 24 hours by default
  (`chat_focus_by_chat` in listener state): follow-up messages that drop the
  project name keep answering about the same project, but an old focus cannot
  silently contaminate a much later conversation. Resolution and probes live in
  `scripts/telegram_operator/projects.py` (`resolve_chat_focus`,
  `build_project_focus`); every probe degrades without breaking chat.

- Telegram plain text runs an agentic tool loop, not intent enumeration: per
  message the chat agent may call up to 3 read-only tools
  (`workspace_overview`, `list_dir`, `read_file`, confined to registered
  projects with path-traversal guards, plus `service_probe` for an observed
  local TCP listener check, in
  `scripts/telegram_operator/projects.py`) and must finish with one terminal
  action: `answer` (chat reply) or `propose_plan` (delegation_goal -> the
  existing plan-capture confirm card; tap stays the only approval). The loop
  lives in `agent.py::chat_with_agent` with the executor in the adapter
  (`run_chat_tool`); tool use is logged as `chat_tools_used`. This handles
  compound requests like "진단하고 새로 계획 세워봐" (inspect files, fold
  findings into the proposed goal) without per-request-type code. Legacy
  intent-shaped model output still maps (delegate_work, inspect_project ->
  overview tool). Keyword markers apply only when the local agent is off or
  unreachable. Per operator decision, new chat capabilities should be new
  tools or terminal actions in this loop, never keyword lists.

- Telegram runtime state is now auditable and fail-closed at the local file
  boundary. The env file must be mode `0600`; generated state, feedback,
  conversation, and loop-status files are forced to `0600` under private
  directories. The service installer pins the actual workspace root before
  deploying stable scripts, limits restart bursts, and passes a single-listener
  lock file. Listener health reports registered versus resolvable project
  counts, status counts, and consecutive errors. Each handled inbound message
  also appends a hashed-ID conversation row containing the exact semantic text
  sent to Telegram and a receipt-linked effect envelope with its concrete
  authority domain. A durable started/effect-committed/completed update journal
  and private reply outbox prevent effect replay while allowing a failed reply
  to be retried after the Telegram offset advances. Ambiguous `started` rows
  require explicit local inspect/reconcile with the listener stopped. Corrupt
  listener state, journals, and outboxes fail closed and surface through
  listener health; chat clarifications preserve their own follow-up context
  instead of refreshing unrelated card context.
  Telegram plain text can now select a current supervised Codex or Claude
  session through the local intent model and deliver a bracketed-paste message
  plus Enter to its tmux pane. A deterministic validator requires one exact
  live target after applying explicit project, agent-title, and reply-card
  constraints, so the model cannot select the first Codex or Claude from an
  ambiguous list. Multiple waiting cards do not create a sticky session;
  direct replies remain card-bound. Delivery receipts distinguish tmux input
  queued and pane reacted from application-level work acceptance, and direct
  dry-run previews never send tmux input. Health exposes separate
  `session_message` readiness for the local model and tmux.
  Live verification on 2026-08-13 confirmed the explicit-target path end to
  end: a user-originated Telegram message mentioning `forager` selected the
  one live Forager Codex session, committed an authorized `session_message`
  effect in the `agent_session_input` authority domain, produced a reacting
  pane, completed the update journal row, and delivered the Telegram receipt
  with no pending outbox entry. The production adapter also passed the
  complementary no-project dry-run against five live Codex candidates: the
  model's proposed target was replaced by `session_target_ambiguous`, the
  mobile response named the candidates and asked for clarification, and the
  resulting effect remained unauthorized `none` with no tmux dispatch. A
  user-originated live replay then confirmed the same network inbound boundary
  while the chat still held a sticky `forager` focus: sticky context did not
  narrow the target, the bot asked the operator to choose among five live Codex
  sessions, the completed journal row retained an unauthorized `none` effect,
  the reply was delivered, and the outbox had no pending entry.

- `forager go [tool] [-- args]` is the zero-friction wrapper for direct agent
  work (`src/cli/go.rs`, Rust registry loader in
  `src/session/project_registry.rs`): resolves cwd against the registry,
  refreshes the project's `.wiki-brief.md` from its wiki plane (best-effort,
  never blocks launch), then finds-or-creates the (path, tool) session and
  attaches. Reuse is idempotent; extra args update a stopped session's
  command but are ignored (with a notice) on a running one. `--no-attach`
  and `--no-brief` exist for scripts. Recommended: `alias cc='forager go
  claude'`. Waiting-state Telegram notification and prompt-bound remote input
  are now part of this wrapper path.

- Harness services resolve the forager binary through
  `scripts/forager_bin.py` (`$FORAGER_BIN` -> `~/.local/bin/forager` ->
  newest target build -> PATH) instead of hardcoding `target/debug`, which a
  rebuild can rewrite mid-run. Refresh the stable binary deliberately with
  `cargo build --release && cp target/release/forager ~/.local/bin/`.

- The wrapper value loop is closed: `forager status` now emits per-session
  rows (id/title/tool/path/status/registry project) in `--json`, a
  per-project rollup line and a `harness: autonomy ... · wiki candidates ...`
  line in human output (signals shared with the TUI via
  `offdesk::orchestration`). The Telegram listener's `--session-notify`
  (installer default on) pushes one card per waiting-episode of a supervised
  session, project-tagged from the registry, with per-prompt backoff;
  episodes that end clear immediately. A `forager go` session that hits a
  permission prompt while the operator is away now reaches the phone.
- Waiting-session notification is hardened against false Codex approval
  matches. Prompt detection uses prompt-shaped bottom-of-pane evidence, a
  changed prompt hash resets its own reminder budget, quiet hours default to
  23:00 through 08:00, and repeated reminders use bounded exponential
  backoff. Telegram session input reports success only after the pane reacts;
  an unchanged pane is recorded as `no_effect` and is not offered again for
  that exact prompt.
- Telegram wiki triage is project-aware. `/attention` includes the registered
  wiki candidate total without turning the existing backlog into proactive
  alerts. `/wiki` ranks projects by candidate count and offers the busiest
  plane as a one-tap next action; `/wiki <key>` shows its newest candidate
  claims. Project mention or sticky focus routes `/remember` to the registry's
  `wiki_profile` with project scope instead of the listener's default profile.
  Rust and Telegram Wiki writers share `adaptive_wiki.lock`, use atomic JSON
  replacement, and preserve corrupt state instead of resetting it. Duplicate
  project aliases now block chat focus and `/remember` writes until the
  operator supplies a canonical project key.
- TUI navigation now renders the home session list with stateful scrolling, so
  repeated Down input keeps the selected row visible on the first screen. TUI
  startup explicitly clears the alternate-screen buffer. Forager applies the
  configured session-local tmux mouse option but never rewrites server-global
  drag or copy-mode bindings. This preserves explicit clipboard and selection
  behavior such as `copy-pipe-no-clear`; terminal-native selection remains
  available with Shift-drag. New Forager-managed Claude sessions set
  `CLAUDE_CODE_DISABLE_ALTERNATE_SCREEN=1`, keeping their conversation in tmux
  scrollback so selection and wheel behavior matches Codex sessions without
  changing standalone Claude or terminal sessions. Home preview capture now
  runs outside the input/render thread. Status polling refreshes the selected
  session every second and background sessions every three seconds, and status
  detection no longer performs an unused pane PID lookup. In the 2026-08-12
  live A/B check, `forager status --json` median latency fell from about 1.39s
  to 0.76s and system CPU time fell by about 44% across six runs per build.

- New-project onboarding is a one-liner or one tap: `forager go --register
  [--key <key>]` appends the directory to the project registry (folder name
  as pattern, key doubling as a fresh wiki plane) and continues straight into
  brief + session; the TUI equivalent is a "Register Project" confirm shown
  after creating a session at an unregistered path (only when a registry
  exists), attaching either way. The TUI status bar now carries harness-wide
  orchestration signals: `Autonomy ARMED` while the overnight window is
  armed, and the wiki candidate queue total across every registered plane
  (`load_orchestration_summary`, refreshed on reload). The TUI now has a
  full-screen project registry view (`p` from home,
  `src/tui/home/projects.rs`): every registered project with live
  running/waiting/total session counts and wiki plane, active projects
  first; Enter jumps to the project's first session or shows the
  `forager go` onboarding hint for session-less projects. Remaining TUI
  improvement slice deferred: an actionable attention panel.

## Next Work Candidates

0. Apply the deferred council verdicts with the new `wiki edit --kind
   --agent-mode/--clear-agent-modes` primitive: reclassify
   telegram-guarded-execution and offdesk-tick entries (forager-ops),
   direction-review entry (twinpaper-review, procedure->policy_rule), and fix
   agent-mode projection on migrations (drop maintenance), tick (universal),
   and figure-anchors (+analysis).

1. Curate the live 124-candidate portfolio through the project-ranked queue.
   Spot-check prereview-supported items for quote-claim mismatch before any
   promotion. The measured pointer-quote, quote-truncation, and per-quote
   fan-out defects are already fixed in the harvest pipeline.
2. Extend learning signals to the remaining lifecycle events (pre-compression
   extraction, wiki projection usage) and add a curator-style staleness report
   (Hermes patterns #9 follow-up and #10).
3. Continue splitting the large Offdesk CLI (`src/cli/offdesk.rs`, ~15.7k
   lines). Value parsing and the first typed decision, adaptive-wiki, and
   closeout receipt workflows are separated. Move closeout decision-resolution
   and retirement record construction through the same typed boundary next;
   keep artifact I/O and RETURN_PACKAGE rendering in CLI adapters.
4. Optionally split `scripts/telegram_operator/receipts.py` (~1,960 lines) by
   stage family if it keeps growing; it is cohesive today.
5. Optionally add parameterized `/run` templates (constrained argument
   substitution) if fixed commands prove too rigid; keep injection surface in
   mind.

## Refresh Rule

Refresh this file when the active development focus changes, a new governance
surface is added, or the next contributor would otherwise need to infer current
state from multiple long-form status documents.
