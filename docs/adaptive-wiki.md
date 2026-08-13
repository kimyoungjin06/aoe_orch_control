# Adaptive Wiki

Forager's adaptive wiki is the active Offdesk learning layer inspired by
Hermes' memory and operator-preference patterns. It is not a raw transcript
store and it is not a prompt-injection channel. The canonical record is shared,
while the AI and human surfaces are generated as separate projections.

## Goals

- Capture repeated corrections, stable project knowledge, failure patterns, and
  operator decisions as durable wiki records.
- Keep one canonical source of truth while avoiding prompt-sized human wiki
  pages in the model context.
- Require promotion before a learned candidate can affect future execution.
- Preserve evidence refs so operators can audit why an entry exists.
- Benchmark adaptive behavior, not just memory recall.

Historical implementation notes are archived under `archive/domain-history/`.
This page is the active product contract.

## Overnight Candidate Learning

Overnight Offdesk runs can improve the adaptive wiki, but only by adding
reviewable candidates. They must not promote, reject, rescope, deprecate, or
rewrite governed wiki entries while the operator is away.

Workload producers may run a deterministic post-run reviewer and, when
candidate capture is enabled, convert review output into
`adaptive_wiki_candidates.json`. The ingest step should record a
`wiki_candidate_ingest.json` sidecar and leave `promotion_allowed` set to
`false` in the run summary.

The morning review loop remains explicit:

```bash
forager -p <profile> offdesk wiki candidates --json
forager -p <profile> offdesk wiki lint --json
forager -p <profile> offdesk wiki review --json
```

Operators or the Council then decide whether to promote, merge, rescope, or
reject candidates. Candidate ingestion is a memory capture step, not an
authority grant.

Telegram `/remember <text>` uses the same candidate boundary. It records an
operator-explicit `preference` candidate in the active profile, but it does not
promote the entry and does not change runtime projection. Plain Telegram chat
does not become a wiki candidate automatically.

## Provisional Overnight Trials

Council can also recommend a temporary `trial_promote` decision during an
overnight run. This is not canonical promotion. The decision creates a
run-local `adaptive_wiki_trial_entries.json` overlay under the workload output
directory, not under the profile's promoted wiki entry store.

Trial entries are deliberately constrained:

- `activation_mode` is always `context_only`.
- The overlay expires at the scheduled campaign end, such as the next 09:00
  KST target, or at a short fallback expiry.
- Trial context is injected in a separate
  `<provisional-adaptive-wiki-context>` fenced block.
- Trial context cannot change commands, files, workdirs, providers, models,
  approvals, or canonical adaptive wiki entries.
- Morning Ondesk review is the only final promotion authority.

Morning review should compare the original candidate, Council trial decision,
trial usage, follow-up evidence, counterexamples, and lint/review reports
before deciding `promote`, `merge`, `rescope`, `reject`, or
`needs_more_evidence`.

A morning review report can unify candidate and provisional rows into one
lifecycle view. It may recommend low-confidence `context_only` promotion
without effect evaluation, but it does not mutate canonical wiki files; final
promotion still runs through Ondesk review and the normal
`forager offdesk wiki promote` surface.

## Storage Shape

The v0 Offdesk artifacts are profile-local JSON files:

- `adaptive_wiki_candidates.json` stores observed but unpromoted learning
  candidates.
- `adaptive_wiki_entries.json` stores canonical wiki entries, including
  promoted and deprecated records.
- `adaptive_wiki_audit.jsonl` records operator review mutations such as
  promote, reject, rescope, deprecate, and add-counterexample.
- `adaptive_wiki_usage.jsonl` records which promoted entries were attached to a
  launched runtime probe or handoff.
- `adaptive_wiki_corrections.jsonl` records first-class correction evidence
  derived from operator corrections and later recurrence/counterexample
  signals.
- `adaptive_wiki_review_events.jsonl` records operator decisions on curator
  review proposals, such as accepted, rejected, or superseded.

All canonical Wiki writers, including the Rust CLI and Telegram `/remember`,
serialize updates through the profile-local `adaptive_wiki.lock`. JSON state
files are written to a same-directory temporary file, synced, and atomically
renamed while that lock is held; JSONL rows are synced before the writer lock
is released. Telegram does not quarantine and replace malformed candidate
state. It reports the candidate write as failed and preserves the original
bytes for local recovery.

Canonical entries use this conceptual schema:

```json
{
  "id": "wiki_entry_...",
  "kind": "preference | procedure | failure_pattern | policy_rule | fact",
  "scope": "session | artifact_kind | project | user_global",
  "scope_ref": "report | project-a | *",
  "status": "candidate | promoted | deprecated",
  "activation_mode": "context_only | confirm | auto_apply",
  "agent_modes": ["planning | development | analysis | writing | critique | review | maintenance"],
  "claim": "Reports keep evidence and recommendations separate.",
  "ai_instruction": "When editing report artifacts, keep evidence and recommendations in separate sections.",
  "human_summary": "The operator repeatedly corrected report rewrites that merged evidence with recommendations.",
  "evidence_refs": ["task:...", "audit:...", "diff:..."],
  "counterexamples": [],
  "core_tags": ["project/project-a", "agent/critique"],
  "proposed_tags": ["method/baseline-first"],
  "confidence": "explicit | repeated | inferred",
  "created_at": "...",
  "updated_at": "...",
  "review_after": "..."
}
```

`auto_apply` is representable in the schema but is not wired into Offdesk
runtime dispatch in v0. Runtime integration starts with `context_only` and
`confirm` projection exposure so learned knowledge cannot silently mutate
behavior.

## Projection Boundary

The AI projection is compact, redacted, and execution-oriented:

```json
{
  "id": "wiki_entry_...",
  "kind": "failure_pattern",
  "scope": "artifact_kind",
  "scope_ref": "report",
  "activation_mode": "confirm",
  "agent_modes": ["writing"],
  "instruction": "Confirm before merging evidence and recommendations.",
  "confidence": "repeated",
  "evidence_count": 3
}
```

It includes only promoted entries that match the current session, project,
artifact, and agent-mode scope. It does not include human summaries, raw
transcripts, raw evidence payloads, candidates, deprecated entries, or secrets.
Projection selection is budgeted by default so runtime context stays bounded: selected
entries are ordered by scope specificity, confidence, evidence count, recency,
activation mode, and stable id. The default budget is 8 entries, roughly 4000
estimated context characters, and 500 characters per instruction. Operators
can inspect the selection and rejection reasons with `forager offdesk wiki
projection --report --json`.

Entries past `review_after` are not excluded by the default projection policy.
When they are still selected, the report adds `review_expired` warning records
and increments `summary.review_expired_projected`. This keeps useful knowledge
visible while making stale trust explicit for humans and agents. Strict
exclusion is opt-in for projection reports with `--exclude-review-expired`.
That mode rejects expired entries with `review_expired_excluded` and leaves the
default runtime projection path unchanged. Operators can inspect the impact
before choosing strict behavior with `--compare-review-expired-policy`, which
returns both the default warn report and the strict exclusion report plus a
summary of selected-entry differences.

Projection reports also flag promoted entries that share the same kind and
scope but project opposite instruction polarity for the same normalized target.
That conflict report is advisory: it creates curator review pressure, but it
does not auto-rescope, deprecate, reorder, or change runtime behavior.

The human projection is governance-oriented. It includes sanitized claims,
human summaries, evidence refs, counterexamples, status, activation mode,
agent modes, confidence, and candidate hit counts. Operators can use this
surface to promote, deprecate, rescope, or review entries.

`forager offdesk wiki export-markdown` writes this human projection as a
one-way markdown vault. If `--output` is omitted, the command uses the active
profile's `wiki-vault/` directory:

```bash
forager -p <profile> offdesk wiki export-markdown
```

Use `--dry-run --json` to check whether the vault is `missing`, `stale`,
`fresh`, or `empty_canonical` without writing files. The export report includes
`projection_status.reexport_recommended` so Ondesk and project audits can turn
stale projection state into an operator action.

The vault is not canonical. If `adaptive_wiki_entries.json` or
`adaptive_wiki_candidates.json` changes after export, re-export the vault
instead of editing markdown pages by hand.

## Tag Graph Boundary

Adaptive wiki entries and candidates can carry two tag classes:

- `core_tags`: controlled tags that may be used by graph export, retrieval,
  review queues, and future routing experiments.
- `proposed_tags`: open suggestions that remain reviewable until a reviewer
  normalizes, promotes, or rejects them.

The graph builder also derives core tags from canonical fields such as
`kind`, `scope`, `status`, `confidence`, `agent_modes`, `capability_ids`, and
`required_artifact_kinds`. This keeps stable structure available even when an
entry has no explicit tags.

Use `forager offdesk wiki graph --json` to inspect the read-only graph, or
`forager offdesk wiki graph --output <dir>` to write `graph.json` and
`graph.md`. The command does not mutate wiki state and does not affect runtime
projection. The detailed registry and review criteria live in
[`adaptive-wiki-tag-graph.md`](adaptive-wiki-tag-graph.md).

## Agent Mode Boundary

The target Offdesk mode contract is defined in
[`offdesk-agent-modes.md`](offdesk-agent-modes.md). It separates agent intent
from execution authority, approval requirements, provider/model routing,
workdir safety, and wiki mutation safety.

Adaptive wiki entries can be shared or mode-specific. `agent_modes` is optional:
an empty or missing list means the entry is universal and can appear for any
Offdesk mode. A non-empty list restricts
projection to matching `--agent-mode` queries.

The implemented mode vocabulary is:

- `planning`: scoped plans, evidence gates, risks, and review handoffs.
- `development`: implementation, debugging, refactoring, and verification
  work.
- `analysis`: log, metric, experiment, and system-state interpretation.
- `writing`: research synthesis, drafting, rewriting, and editorial
  improvement.
- `critique`: adversarial analysis, risk finding, and quality judgement.
- `review`: read-only checkpoint review before execution, approval, or handoff.
- `maintenance`: read-only wiki, task, repo, model, and machine health
  inspection.

Legacy persisted values remain loadable: `code_development` loads as
`development`, and `research_writing` loads as `writing`.

`offdesk gate`, `offdesk launch`, `offdesk enqueue`, `offdesk wiki entries`,
`offdesk wiki candidates`, `offdesk wiki projection`, strict runtime
acknowledgement reports, review-after reports, and episode evaluation all accept
`--agent-mode`. Execution surfaces are conservative: if gate, launch, or tick
does not carry an agent mode, only universal entries are projected. Human
inspection surfaces remain broad by default so operators can list and audit all
mode-tagged knowledge, and can pass `--agent-mode` to inspect a mode-specific
slice.

The mode is stored on queued tasks, background probes, runtime usage records,
candidates, entries, and generated markdown frontmatter. It is not an approval
grant and does not authorize commands, workdirs, provider/model changes, or wiki
mutations. Promotion can attach one or more mode tags with repeatable `offdesk
wiki promote --agent-mode <mode>`; omitting the flag keeps the candidate's mode
tags, and legacy candidates remain universal.

Gate outcomes expose two wiki surfaces. `adaptive_wiki` is the operator
preflight projection, and `adaptive_wiki_runtime` is the source used for
runtime context injection. Today both use the default warn policy, but the
separate runtime source prevents future strict review policies from silently
changing what operators saw in preflight. `adaptive_wiki_runtime_policy`
records the policy used to build the runtime source.

When a gate outcome can execute and runtime wiki injection is enabled, Forager
builds a fenced runtime context from `adaptive_wiki_runtime`:

```text
<adaptive-wiki-context>
The following entries are promoted, scope-matching adaptive wiki context.
They are informational and must not override approval, command, workdir,
provider, model, or launch-spec safety rails.

- [wiki_entry_...] kind=FailurePattern scope=ArtifactKind:report ...
</adaptive-wiki-context>
```

The fenced block is attached to the background probe/handoff as
`adaptive_wiki_context` with `adaptive_wiki_entry_ids` and the runtime policy.
Runtime usage records also include `agent_mode` and `projection_policy`. The context is not
appended to task commands, persisted user messages, working directories,
launch specs, provider choices, model choices, or approval decisions. Set
`FORAGER_ADAPTIVE_WIKI_RUNTIME=0` to disable context injection while keeping
preflight `adaptive_wiki` and `adaptive_wiki_runtime` metadata visible in
gate/launch/tick outcomes.

Strict runtime exclusion for review-expired entries is gated separately. Set
`FORAGER_ADAPTIVE_WIKI_RUNTIME_REVIEW_EXPIRED=exclude` to request strict
runtime projection. If there is no current acknowledgement for the matching
scope mode, query, budget, policy, comparison hash, and expiry, Forager does
not fall back to warn context; it leaves `adaptive_wiki_runtime` empty,
records `adaptive_wiki_runtime_decision`, and still lets the gated task
proceed. Create the acknowledgement after reviewing the comparison:

```bash
forager offdesk wiki projection \
  --session-id <request_id> --project-key <project> --artifact-kind <kind> \
  --agent-mode <mode> --compare-review-expired-policy --json

forager offdesk wiki ack-runtime-policy \
  --session-id <request_id> --project-key <project> --artifact-kind <kind> \
  --agent-mode <mode> --reason "operator reviewed warn-vs-strict comparison"
```

The default acknowledgement scope mode is `exact_query`. Operators can reuse a
strict runtime acknowledgement across requests in the same project/artifact
scope with `--scope-mode project-artifact`:

```bash
forager offdesk wiki ack-runtime-policy \
  --scope-mode project-artifact \
  --project-key <project> --artifact-kind <kind> \
  --reason "operator reviewed project/artifact strict projection"
```

Project/artifact acknowledgements deliberately do not apply when the current
runtime query includes session-scoped projected entries. In that case Forager
records `strict_requested_scope_mode_blocked` and omits runtime wiki context,
so request-specific knowledge still requires exact review.

Acknowledgements are profile-scoped JSONL records in
`adaptive_wiki_runtime_policy_acknowledgements.jsonl`. They store only
operator-safe ids, scope mode, policy, query, budget, comparison hash, reason,
and expiry; they do not store runtime instructions or raw evidence payloads.

Use `runtime-policy-ack-report` to inspect the acknowledgement surface before
or during strict runtime rollout:

```bash
forager offdesk wiki runtime-policy-ack-report \
  --session-id <request_id> --project-key <project> --artifact-kind <kind> \
  --agent-mode <mode> --near-expiry-hours 6 --json
```

The report is read-only. It lists expired and near-expiry acknowledgements, and
when a query is supplied it includes the strict runtime decision. This makes
`strict_requested_scope_mode_blocked` visible before a task launch consumes the
same query. Rows that need operator follow-up include `suggested_action` with a
comparison command and an `ack-runtime-policy` command template. The old
acknowledgement is not modified; operators re-run the comparison and append a
new acknowledgement when strict runtime should continue.

Use `review-after-report` to inspect promoted wiki entries whose review window
is expired or close to expiring:

```bash
forager offdesk wiki review-after-report \
  --project-key <project> --artifact-kind <kind> --agent-mode <mode> \
  --near-expiry-hours 168 --json
```

This report is also read-only. It uses the same scope matching as the human
projection, lists only promoted entries that need attention, and exposes a
`renew-review-after` command template without including adaptive instructions
or raw evidence payloads.

`forager offdesk debug-bundle` includes read-only summary counters for both
maintenance reports as
`adaptive_wiki_runtime_policy_ack_attention_summary` and
`adaptive_wiki_review_after_attention_summary`. These are summaries only; the
bundle does not add runtime instructions or raw evidence payloads for the
maintenance surface.

`forager offdesk maintenance-report` is the compact read-only operator surface
for the same maintenance pass. It aggregates durable task status, background
probe phase, pending approvals, resume records, provider capacity attention,
runtime policy acknowledgement attention, and `review_after` attention without
polling runners, mutating task state, or approving actions.

When a maintenance finding needs a real mutation, restart, cleanup, recovery,
or wiki change, use `forager offdesk maintenance-request` to create a scoped
approval row such as `maintenance.artifact_cleanup` or
`maintenance.wiki_review_after`. The request command does not execute the
maintenance operation and does not consume an existing approved one-time grant;
after approval, the reviewed maintenance command must still be run explicitly.

Offdesk task, poll, background, and debug-bundle JSON also include derived
`mode_verdict`, `mode_risk`, `mode_risk_detail`, and
`review_stage_required` fields. These fields are computed from the current
task/probe lifecycle and selected `agent_mode`; they do not mutate task state,
approve actions, or replace artifact review. A missing `agent_mode` is reported
as `mode_verdict=unscoped` and `mode_risk=missing_agent_mode`.

How documents, session transcripts, and reviews turn into candidates is
governed by [`adaptive-wiki-distillation.md`](adaptive-wiki-distillation.md):
the distillation rules, the local-LLM distillers with verbatim evidence
verification, and the tiered review (operator packet, local pre-filter, agent
Council).

## Operator Review Commands

The current CLI exposes both inspection and governed review mutation commands:

- `forager offdesk wiki candidates --json`
- `forager offdesk wiki corrections --json`
- `forager offdesk wiki proposal-events --json`
- `forager offdesk wiki proposal-events --proposal-id <proposal-id> --json`
- `forager offdesk wiki record-proposal-event <proposal-id> --decision <accepted|rejected|superseded> --reason <text>`
- `forager offdesk wiki accept-proposal <proposal-id> --reason <text>`
- `forager offdesk wiki reject-proposal <proposal-id> --reason <text>`
- `forager offdesk wiki supersede-proposal <proposal-id> --reason <text>`
- `forager offdesk wiki entries --json`
- `forager offdesk wiki show <id> --json`
- `forager offdesk wiki projection --project-key <key> --artifact-kind <kind> --agent-mode <mode> --json`
- `forager offdesk wiki brief --project-key <key> [--agent-mode <mode>] [--out <path>]`
  renders the projection as a session-start markdown brief with per-entry
  trust signals (confidence, evidence count, age, STALE past `review_after`)
  and an explicit skepticism contract: conflicting entries are verified and
  recorded (counterexample/deprecate), never silently obeyed or ignored.
  Promotion now sets `review_after` (+90d default, `--review-after-days`).
- `forager offdesk wiki projection --project-key <key> --report --max-entries <n> --json`
- `forager offdesk wiki projection --project-key <key> --compare-review-expired-policy --json`
- `forager offdesk wiki runtime-policy-acks --json`
- `forager offdesk wiki runtime-policy-ack-report --session-id <request> --project-key <key> --artifact-kind <kind> --agent-mode <mode> --json`
- `forager offdesk wiki review-after-report --project-key <key> --artifact-kind <kind> --agent-mode <mode> --json`
- `forager offdesk wiki ack-runtime-policy --session-id <request> --project-key <key> --artifact-kind <kind> --agent-mode <mode> --json`
- `forager offdesk wiki lint --json`
- `forager offdesk wiki export-markdown`
- `forager offdesk wiki export-markdown --dry-run --json`
- `forager offdesk wiki export-markdown --output <dir>`
- `forager offdesk wiki review --json`
- `forager offdesk wiki review --dry-run --json`
- `forager offdesk wiki review --active-only --json`
- `forager offdesk wiki review --decided-only --json`
- `forager offdesk wiki review --stale-only --json`
- `forager offdesk wiki proposal-handoff <proposal-id> --json`
- `forager offdesk wiki proposal-receipt <proposal-id> --audit-id <id> --event-id <id> --command <cmd> --json`
- `forager offdesk wiki proposal-receipt <proposal-id> --audit-id <id> --event-id <id> --command <cmd> --export`
- `forager offdesk wiki proposal-receipt <proposal-id> --audit-id <id> --event-id <id> --command <cmd> --output <path> --json`
- `forager offdesk wiki evaluate-episode <entry-id> --project-key <key> --agent-mode <mode> --out-project-key <other> --json`
- `forager offdesk wiki evaluate-episode <entry-id> --artifact-kind <kind> --dry-run --json`
- `forager offdesk wiki episode-trace --request-id <id> --project-key <key> --json`
- `forager offdesk wiki episode-trace --entry-id <entry-id> --dry-run --json`
- `forager offdesk wiki evaluate-recurrence <entry-id> --json`
- `forager offdesk wiki evaluate-recurrence <entry-id> --dry-run --json`
- `forager offdesk wiki promotion-chain <entry-id> --json`
- `forager offdesk wiki promotion-chain <entry-id> --dry-run --json`
- `forager offdesk wiki record-candidate --kind <kind> --scope <scope> --scope-ref <ref> --claim <text> [--ai-instruction <text>] [--evidence-ref <ref>]... [--core-tag <tag>]... [--origin <who>] [--signal-kind <what>]`
- `forager offdesk wiki promote <candidate-id> --scope <scope> --scope-ref <ref> --activation-mode <mode> --agent-mode <mode>`
- `forager offdesk wiki reject <candidate-id> --reason <text>`
- `forager offdesk wiki edit <entry-id> [--kind <kind>] [--agent-mode <mode>]... [--clear-agent-modes] [--claim <text>] [--ai-instruction <text>] [--human-summary <text>] [--evidence-ref <ref>]... --reason <text>`
- `forager offdesk wiki add-tag <entry-id> [--core-tag <tag>]... [--proposed-tag <tag>]... --reason <text>`
- `forager offdesk wiki rescope <entry-id> --scope <scope> --scope-ref <ref>`
- `forager offdesk wiki deprecate <entry-id> --reason <text>`
- `forager offdesk wiki renew-review-after <entry-id> --review-after <rfc3339> --reason <text>`
- `forager offdesk wiki add-counterexample <entry-id> --evidence-ref <ref> --reason <text>`
- `forager offdesk wiki update-runbook <entry-id> --support-ref <ref> --capability-id <id> --required-artifact-kind <kind> --reason <text>`

Mutation commands change only the adaptive wiki store and append audit records.
They do not rewrite commands, workdirs, launch specs, provider/model choices, or
approval state.

`wiki edit --agent-mode` replaces the complete mode list with the repeated
values supplied in that command. Use `--clear-agent-modes` to make an entry
universal. The two forms are mutually exclusive, so an omitted mode option
continues to mean "leave the current mode scope unchanged."

## Markdown Human Vault

`forager offdesk wiki export-markdown` generates a one-way markdown vault for
operator review. By default it writes to the active profile's `wiki-vault/`.
Pass `--output <dir>` to write a separate review copy:

```text
adaptive-wiki/
  SCHEMA.md
  index.md
  log.md
  entries/
    preference/
    procedure/
    failure-pattern/
    policy-rule/
    fact/
  candidates/
  raw/
    audits/
    diffs/
    docs/
```

The export is deterministic and sanitized. `SCHEMA.md` records the taxonomy and
safety invariants, `index.md` links entries and candidates, `log.md` records the
export snapshot, entry pages include frontmatter for scope/status/confidence,
agent modes, and counterexamples, and candidate pages include source refs,
agent modes, and review reason.
The markdown vault is not a writable source of truth in v0. Operators should
continue to mutate wiki state through the CLI review commands.

Use `--dry-run --json` to inspect planned files, content hashes, and
`projection_status` without writing the vault. `projection_status.state`
reports `missing`, `stale`, `fresh`, or `empty_canonical`.

## Curator Review Reports

`forager offdesk wiki review --json` generates a recommendation-only curator
report under `adaptive_wiki_review_reports/<timestamp>/` with `report.json` and
`REPORT.md`. The report reads entries, candidates, lint, usage records, and
audit records, correction records, and promotion evidence-chain completeness,
then proposes actions such as promote, reject, deprecate, renew-review,
rescope, split, merge, or add-counterexample.

The curator uses the evidence graph conservatively:

- post-promotion correction records tied to an entry produce a rescope/review
  proposal rather than changing the entry automatically;
- missing promotion audits or missing promotion snapshots produce a renew-review
  proposal so operators can decide whether to preserve, recreate, or deprecate
  the evidence chain;
- legacy profiles still get the older lint, candidate, usage, and audit-derived
  proposals.

Reports are proposals only. They do not mutate candidates, entries, approvals,
commands, workdirs, provider/model choices, or launch specs. Use
`--dry-run --json` to inspect the same proposals without writing report files.
Every proposal includes a subject id and evidence refs, and report content is
sanitized before output.

`forager offdesk wiki record-proposal-event <proposal-id> --decision accepted
--reason <text>` records an operator lifecycle decision in
`adaptive_wiki_review_events.jsonl`. The event log is sanitized before storage
and can be listed with `forager offdesk wiki proposal-events --json`. Recording
a proposal event does not apply the proposed mutation; it only makes the
operator decision auditable.

Review reports join current proposals to the latest lifecycle event with the
same proposal id. JSON and markdown reports expose the latest
accepted/rejected/superseded decision, the deciding actor, reason, evidence
refs, and open/decided proposal counts. This annotation is still report-only:
it does not suppress, accept, reject, or mutate any proposal by itself.

Lifecycle decisions become stale when the proposal subject or timestamped
evidence records change after the decision. Stale decisions are counted as
open again in review summaries and include `stale_evidence_refs` so an operator
can see why the accepted/rejected/superseded decision needs renewed review.

Review filtering supports three queue views. `--active-only` shows proposals
with no decision plus proposals with stale decisions. `--decided-only` shows
non-stale accepted/rejected/superseded proposals. `--stale-only` shows only
proposals whose latest lifecycle decision needs renewed review. Filtered review
reports include `filtered_out_proposals` so JSON, markdown, and human output
stay explicit about how many current proposals were hidden by the selected
view.

Proposal closure helpers record lifecycle events for current review proposals
without requiring the operator to copy action, subject, or evidence metadata
manually. `accept-proposal`, `reject-proposal`, and `supersede-proposal` load
the current proposal, copy its action/subject/evidence refs into a sanitized
event, and append that event to `adaptive_wiki_review_events.jsonl`. They do
not apply the proposed wiki mutation. To avoid accidental duplicate closure,
the helpers reject a proposal that already has a non-stale accepted/rejected or
superseded decision unless `--allow-decided` is passed.

`forager offdesk wiki proposal-handoff <proposal-id> --json` previews the exact
governed mutation command for a current proposal when one is safe to derive. It
is read-only and never executes the command. The preview status is `ready` when
an exact command is available, `manual_required` when the proposal still needs
operator choices such as scope or evidence selection, and
`blocked_by_decision` when the proposal already has a non-stale lifecycle
decision. Manual previews also include `required_inputs` and
`mutation_options`, which describe the operator choices and command templates
that would make the handoff exact. These contracts are advisory only; they do
not apply, accept, reject, or supersede a proposal.

When the operator supplies the missing inputs, the same preview command can
promote a manual proposal to `ready` without mutating state. Supported
parameterized previews are intentionally narrow:

- `--mutation rescope --scope <scope> --scope-ref <ref>` for entry rescope,
  renew-review, or split proposals;
- `--mutation add-counterexample --evidence-ref <ref> --reason <text>` for
  entry counterexample, renew-review, or split proposals;
- `--mutation deprecate --reason <text>` for entry deprecate or renew-review
  proposals;
- `--mutation deprecate-duplicate --deprecated-entry-id <entry> --reason
  <text>` for entry merge cleanup proposals.

Projection-conflict split proposals get a richer handoff contract. Their
manual preview lists `rescope`, `deprecate`, `split`, and `add_counterexample`
paths from the same proposal. `--mutation deprecate --reason <text>` retires
the proposal subject by default, and `--deprecated-entry-id <entry>` may target
the conflicting entry referenced in proposal evidence. `--mutation split`
remains manual because resolving a real conflict can require multiple governed
wiki mutations before a receipt can link the work.

These flags still only preview the governed command. The operator must run the
returned mutation command separately and record any lifecycle decision
separately.

`forager offdesk wiki promote <candidate-id>` writes an
`adaptive_wiki_promotion_receipt.v1` artifact under
`adaptive_wiki_promotion_receipts/` while it performs the canonical promotion.
The receipt records the candidate id, entry id, audit id, reviewed snapshots,
activation mode, scope transition, and authority boundary. It proves one
promotion was recorded; it does not authorize future runtime projection,
cleanup, provider retargeting, or accepted truth for task outputs.

`forager offdesk wiki proposal-receipt <proposal-id> --audit-id <id>
--event-id <id> --command <cmd> --json` links those separated steps after the
fact. The default output is transient. `--export` writes a sanitized receipt
under `adaptive_wiki_proposal_receipts/`, and `--output <path>` writes the same
receipt to a caller-selected path without overwriting an existing file. Receipt
creation is otherwise read-only: it does not append lifecycle events, audit
records, wiki rows, reports, approvals, or task mutations. The receipt hashes
the sanitized previewed command, loads the mutation audit record, loads the
lifecycle event, and checks that all three point at the same proposal subject.
If the proposal is no longer current because the mutation already removed or
changed the subject, the receipt uses the action, subject, and evidence
metadata copied into the lifecycle event as the proposal snapshot. Incomplete
receipts keep `status: "incomplete"` and include check details that show which
link did not match.

The intended proposal workflow keeps recommendation, operator decision,
mutation, and verification separate:

1. Inspect recommendations with `forager offdesk wiki review --dry-run --json`.
2. Preview the governed mutation with `forager offdesk wiki proposal-handoff
   <proposal-id> --json`.
3. Record the operator decision with `accept-proposal`, `reject-proposal`, or
   `supersede-proposal`.
4. Run the returned governed mutation command when the decision requires a
   wiki state change. Promotion mutations write a durable promotion receipt
   automatically.
5. Link the preview, lifecycle event, and mutation audit with
   `proposal-receipt`. Use `--export` or `--output` when the operator needs a
   durable receipt artifact for later audit.

## Episode Evaluation Reports

`forager offdesk wiki evaluate-episode <entry-id> --json` generates a
report-only projection check under `adaptive_wiki_episode_reports/<timestamp>/`
with `episode.json` and `EPISODE.md`. The command compares an in-scope query
against an out-of-scope query for one target entry.

The report checks whether:

- the target entry appears in the in-scope AI projection;
- the target entry does not appear in the out-of-scope AI projection;
- deprecated entries are absent from both projections;
- review-expired entries are surfaced as failures instead of silently guiding
  execution;
- projected entries have evidence refs.

Use `--project-key`, `--artifact-kind`, and `--session-id` for the in-scope
query. Use `--out-project-key`, `--out-artifact-kind`, and `--out-session-id`
to set the comparison query explicitly; when omitted, the command derives a
non-matching out-of-scope value from the in-scope inputs. `--dry-run --json`
returns the same checks without writing report files.

Episode reports do not mutate candidates, entries, audits, usage records,
approvals, commands, workdirs, provider/model choices, or launch specs. They
are a deterministic benchmark surface for scope leakage and stale activation,
not an autonomous task runner.

## Live Episode Trace Reports

`forager offdesk wiki episode-trace --json` connects existing durable Offdesk
artifacts into one replayable trace. It reads:

- `offdesk_tasks.json`;
- `background_runs.json`;
- `task_resume_state.json`;
- `adaptive_wiki_usage.jsonl`;
- `adaptive_wiki_candidates.json`;
- `adaptive_wiki_corrections.jsonl`;
- `adaptive_wiki_audit.jsonl`.

When not run with `--dry-run`, it writes
`adaptive_wiki_episode_traces/<timestamp>/report.json`,
`adaptive_wiki_episode_traces/<timestamp>/trace.jsonl`, and
`adaptive_wiki_episode_traces/<timestamp>/REPORT.md`.

Trace events include task enqueue/completion/failure, projection attachment,
runtime wiki usage, first-class correction records, legacy
operator-correction candidates, promotion audits, counterexamples,
resume-pending states, and rollback-derived candidates. When a correction
record references a candidate, the candidate-derived correction fallback is not
double-counted. Use `--request-id`, `--task-id`, `--project-key`,
`--artifact-kind`, or `--entry-id` to restrict the trace.

This is still evidence only. The trace command does not enqueue, launch, retry,
promote, deprecate, alter approvals, rewrite commands, change workdirs, or
retarget provider/model choices. It exists so operators and future curator
reports can evaluate whether wiki knowledge affected real behavior.

## Correction Recurrence Reports

`forager offdesk wiki evaluate-recurrence <entry-id> --json` evaluates whether
correction signals continue after a promoted entry becomes available. It uses
first-class `adaptive_wiki_corrections.jsonl` rows when present, keeps legacy
candidate-derived correction fallback for older profiles, and avoids
double-counting a candidate that already has a correction record. It uses the
target entry's scope as the trace filter, finds the promotion boundary from the
promotion audit record or entry `created_at`, then compares:

- operator-correction events before and after promotion;
- task failure or resume-pending events tied to the target entry;
- counterexample events tied to the target entry;
- post-promotion runtime usage of the target entry.

When not run with `--dry-run`, it writes
`adaptive_wiki_recurrence_reports/<timestamp>/report.json`,
`adaptive_wiki_recurrence_reports/<timestamp>/recurrence.jsonl`, and
`adaptive_wiki_recurrence_reports/<timestamp>/REPORT.md`.

The report emits one of three assessments:

- `insufficient_evidence`: no target entry or no post-promotion usage;
- `no_recurrence_observed`: usage exists and no post-promotion correction or
  failure recurrence was observed;
- `recurrence_observed`: at least one post-promotion correction, counterexample,
  failure, or resume-pending signal was observed.

This evaluator is intentionally conservative. It does not prove the wiki entry
caused an improvement; it only quantifies whether relevant corrections recur
after promotion using available durable evidence.

## Promotion Evidence Chain Reports

`forager offdesk wiki promotion-chain <entry-id> --json` reconstructs the
promotion-time evidence chain for a promoted entry. New promotion audit records
store redacted human candidate and entry snapshots, so the report can compare:

- the promotion audit record;
- the candidate snapshot that was reviewed;
- the entry snapshot created by promotion;
- the current entry projection;
- runtime usage records for the entry;
- later audit records tied to the same entry.

When not run with `--dry-run`, it writes
`adaptive_wiki_promotion_chains/<timestamp>/report.json`,
`adaptive_wiki_promotion_chains/<timestamp>/chain.jsonl`, and
`adaptive_wiki_promotion_chains/<timestamp>/REPORT.md`.

This command is report-only. It does not promote, deprecate, rescope, retry,
launch, alter approvals, rewrite commands, change workdirs, or retarget
provider/model choices. Older promotion audits that predate snapshots are
reported with explicit missing-snapshot failures instead of being silently
backfilled from current state.

`forager offdesk wiki review --json` also reports promotion receipt coverage in
its summary:

- `promotion_receipts_checked`;
- `promotion_receipt_files_invalid`;
- `promoted_entries_with_promotion_receipt`;
- `promoted_entries_missing_promotion_receipt`.

These fields are review signals, not automatic blockers. Older promoted entries
may lack receipts even when they have promotion audit records, so treat missing
receipts or invalid receipt files as cues to inspect `promotion-chain` before
relying on the entry in a new high-stakes context.

## Procedure Runbooks

Procedure entries can now carry governed runbook metadata:

- `support_refs`: human/export references such as `references/foo.md`,
  `templates/foo.md`, or `scripts/foo.sh`;
- `capability_ids`: capability registry ids the procedure is relevant to;
- `required_artifact_kinds`: artifact classes the procedure depends on.

Use `forager offdesk wiki update-runbook` to attach this metadata. The command
only works on `kind=procedure` entries, appends an audit record, and does not
change command strings, workdirs, provider/model routing, launch specs, or
approval state.

Runbook support refs appear in human projection and markdown export under
`Runbook Support`. They remain excluded from compact AI projection and runtime
wiki context unless a future allowed flow explicitly loads support material
after scheduler approval.

## Adaptive Loop

```text
Observe correction/failure/preference
  -> record candidate with evidence refs
  -> merge repeated candidate occurrences
  -> recommend promotion once evidence is strong enough
  -> operator promotes, rejects, rescopes, or changes activation mode
  -> AI projection supplies only matching promoted entries
  -> runtime probe receives fenced context when launch is allowed
  -> review audit records how entries changed
  -> future usage audit records which entries influenced a task
  -> benchmark checks whether future behavior improved without scope leakage
```

The critical safety property is that observation and execution remain separate:
learning candidates are durable evidence, not automatic routing or mutation
policy.

## Offdesk Preflight Integration

`forager offdesk gate --json` and launch/tick gate outcomes can include an
`adaptive_wiki` array. The entries are generated from the AI projection only:

- `status=promoted` entries are eligible;
- `status=candidate` and `status=deprecated` entries are excluded;
- project entries match the current `--project-key`;
- artifact entries match `--artifact-kind`;
- request-scoped entries use the current `request_id` as the bounded Offdesk
  session-like scope until a real Hermes session id is available;
- entries with no `agent_modes` are universal;
- tagged entries require the current `--agent-mode` to match, and are excluded
  from gate/launch/tick projection when no mode is present;
- instructions are redacted before they are serialized.

This projection is preflight context only. It does not rewrite commands,
provider/model selection, workdirs, launch specs, or approval decisions.

## Runtime Handoff Integration

When launch or tick dispatch is approved and a background probe is created, the
same matching AI projection is converted into a fenced runtime context block.
The launch path stores the block and entry ids on the background probe so
runtime consumers can inspect the exact context that was made available.

For each injected entry, Forager appends an `adaptive_wiki_usage.jsonl` record
with the entry id, task id, request id, project key, artifact kind, projection
kind (`runtime_probe`), agent mode, activation mode, and timestamp. No usage record is
written when the task is blocked before launch, when no promoted scoped entry
matches, or when runtime injection is disabled with
`FORAGER_ADAPTIVE_WIKI_RUNTIME=0`.

This handoff is still context-only. `confirm` entries may guide planning or
review text, but they do not authorize execution. `auto_apply` remains a stored
schema value only and is not wired to mutation policy.

## Benchmark Contract

The adaptive wiki benchmark should exercise episodes rather than static QA:

1. The agent produces or risks a known mistake.
2. The operator corrects it and an evidence-backed candidate is recorded.
3. Repeated evidence promotes the candidate to an operator-visible
   recommendation.
4. The operator promotes the candidate with a bounded scope.
5. A similar task in scope receives the AI projection and avoids the mistake.
6. A task outside scope does not receive or apply the entry.
7. A deprecated entry no longer appears in AI projection.

Primary metrics:

- `candidate_capture_rate`: corrections that become candidates.
- `promotion_precision`: promoted entries that help later tasks.
- `post_promotion_correction_reduction`: repeated correction drop after
  promotion.
- `scope_leakage_rate`: entries applied outside their scope.
- `stale_entry_activation_rate`: deprecated or review-expired entries that
  still affect execution.
- `evidence_trace_completeness`: promoted entries with enough source refs to
  audit their origin.
- `operator_review_load`: candidate volume per useful promotion.

## Live LLM Prompt Harness

`scripts/offdesk_wiki_llm_harness.py` is a live, opt-in harness for checking
whether a model follows adaptive wiki projection and evidence-state contracts.
It is deliberately outside Cargo tests because it depends on a reachable model
endpoint, such as an Ollama-compatible Gemma server.

The harness loads the real `offdesk wiki projection` for the selected profile,
project, artifact kind, and projection mode, then runs contract-based cases for
the canonical Offdesk mode vocabulary. Target modes and projection modes remain
separate so legacy profiles can still be tested explicitly, but canonical
`planning`, `development`, `analysis`, `writing`, `critique`, `review`, and
`maintenance` modes now use matching projection scopes.

- planning with evidence gates and stop conditions;
- planning an Offdesk workload with a mandatory separate review stage;
- planning development, analysis, and writing work with different target
  lenses;
- planning a tiny toy task before a longer autonomous workload;
- review-mode checking of a draft Offdesk workload plan;
- development planning without false completed-action claims;
- analysis of evidence windows with observation/inference separation;
- writing reportability with missing or supplied evidence;
- critique of open-explore-only direction changes;
- maintenance reports that keep cleanup and system changes approval-gated;
- repo-root module entrypoint commands from supplied repository facts;
- JSON-only reportability verdicts;
- JSON-only target-mode classification.

Critique fixtures should treat exploratory results as exploratory signals
unless they are paired with promotion-comparable gate evidence. A model must
not report that evidence is absent when the bundle contains weaker exploratory
signals; it should instead explain which signals are not yet comparable to the
required promotion evidence.

It grades anchors, forbidden completion claims, strict JSON parseability, and
case-specific JSON schemas. It does not grade exact prose. Domain anchors are
canonicalized through a small alias table before grading, so terms such as
`no-option`, `no-op`, `no option`, and `single-nooption` are treated as the
same baseline family while still being recorded as canonicalization warnings.
Empty model responses are tracked separately and can be retried with
`--retry-empty` so prompt-contract failures are not confused with endpoint
instability.

Use `--why-depth <0..6>` to request a compact Six-Why causal ladder for
non-JSON cases. Use `--why-depth-sweep 0,3,6` to compare no ladder, a shallow
ladder, and the full six-question probe in one run. JSON-only cases
automatically skip this section so structured contracts remain parseable. The
grader records `why_depth_requested`, `why_depth_effective`,
`why_ladder_observed_depth`, `why_ladder_score`, and
`why_ladder_failures`; it also records `why_ladder_row_pattern` so operators
can distinguish reasoning failures from row-format mismatches. The summary
groups pass rate and average ladder scores by requested depth. The ladder is
considered useful only if it improves evidence discipline without increasing
false claims, invented evidence, or format failures. Pass
`--store-response-text` during prompt-quality debugging when the full model
answer is needed; the default artifact stores only an 800-character preview.

For `planning_toy_task_design`, the harness also records a deterministic
semantic quality rubric separate from pass/fail. It scores whether the toy task
is small, bounded, read-only, and planner-only; avoids making the toy task
itself a depth sweep; names input evidence; specifies expected planner output;
includes an evaluation rubric; lists stop conditions; keeps safety boundaries
explicit; names the next agent mode; shows evidence restraint; and is
actionable as a planner test artifact. Depth is useful only when it raises or
preserves this semantic score enough to justify the added latency and response
length.

For Offdesk autonomous planning cases, the harness should check that the model
routes to a separate review mode rather than performing review inside planning.
Planning cases may mark the review decision as `pending_review` because the
review has not run yet. Review-mode cases then produce the read-only review
artifact with the reviewed artifact, blocking issues, missing evidence,
counterarguments, safety and approval gates, and a decision such as `proceed`,
`revise`, `needs_approval`, or `blocked`. Harness summaries include
`review_stage_summary` when a case requires this contract.

Planning cases are target-specific. Development plans are evaluated for change
scope, test strategy, regression risk, rollback, and review handoff. Analysis
plans are evaluated for evidence sources, observation/inference separation,
competing causes, missing diagnostics, decision thresholds, and review handoff.
Writing plans are evaluated for audience, claim status, evidence mapping,
citation/source gaps, overclaim risks, revision steps, and review handoff.

For cases that declare a JSON contract, the harness sends Ollama
`format=json` by default; pass `--no-json-format` only when intentionally
testing prompt-only JSON compliance.
For Ollama-compatible thinking models, the harness sends `think:false` by
default because these cases test final-answer contract compliance, not hidden
reasoning budget. Pass `--think` when intentionally stress-testing reasoning
mode.
Use `--max-budget <tokens>` to cap per-case `num_predict` on memory-constrained
models, and `--num-ctx <tokens>` when an Ollama model supports a larger context
window than its current loaded default.
The prompt guide assumes commands run from the repository root unless the task
explicitly changes directories, so model outputs should use repo-relative paths
instead of basename-only or module-cwd-relative commands.

Example:

```bash
OFFDESK_LLM_BASE_URL=http://<gpu-server>:11434 \
OFFDESK_LLM_MODEL=gemma4:26b \
scripts/offdesk_wiki_llm_harness.py \
  --profile adaptive-debug \
  --prompt-profile contract_v3 \
  --why-depth-sweep 0,3,6 \
  --temperature 0.2 \
  --iterations 2
```

For `qwen3-coder-next:latest`, the harness has been validated with
`--max-budget 12288 --num-ctx 16384` while keeping `think:false`. Live workload
probes also showed that JSON-contract outputs should use API-level JSON mode
rather than relying on prompt wording alone.

Results are preserved by default under the selected profile in
`wiki_llm_harness_runs/<timestamp>/results.json`. The summary includes
`mode_coverage`, `mode_failures`, and `why_depth_summary` so operators can
tell whether a run exercised all target modes and whether deeper causal
questioning improved semantic quality rather than only increasing answer
length. The depth summary includes average response length, latency, ladder
score, observed row count, and semantic quality score when available.

## Prepared Offdesk Autonomy Workload

A prepared Offdesk autonomy workload is a read-only, bounded workload for
testing autonomous execution on a real project task. It reads project guidance,
module facts, and a deterministic evidence bundle, then repeats
operator-command, evidence-state, writing, code-planning, and critique cases
against the configured model. The default configuration should be paced and
bounded by either iteration count or wall-clock stop time. It writes only to
the selected workload output directory:

- `manifest.json`
- `evidence/evidence_bundle.json`
- `evidence/evidence_review.json`
- `responses/*.txt`
- `progress.jsonl`
- `episodes/*.json`
- `council/*/council.json`
- `council_progress.jsonl`
- `heartbeat.json`
- `result.json`
- `REPORT.md`

The evidence bundle contract is defined in
[`offdesk-evidence-bundles.md`](offdesk-evidence-bundles.md). The workload uses
the bundle instead of a fixed log prefix so current project evidence, recent
run summaries, and targeted excerpts stay in the model's context.

The bundle may also embed a `module_operation_profiles.<module_key>` entry.
That profile turns the module into an explicit operating unit with canonical
commands, approval requirements, forbidden actions, reportability vocabulary,
and Ondesk return requirements. See
[`Module Operation Profiles`](guides/module-operation-profile.md) for the
operator-facing contract.

Preparation must create the workload directory and produce a guarded runtime
dispatch packet only after recording preflight evidence:

- a clean live role-gate result from
  `scripts/offdesk_role_llm_episode_harness.py`;
- a separate review artifact for the exact prepared manifest;
- the latest matching `MODULE_OPERATION_PREFLIGHT.json`, or an explicit
  module preflight artifact, proving that module profile and evidence builders
  are recognized before runtime preparation;
- a review decision that allows enqueue: `proceed` or `needs_approval`.

For generic bounded commands, `scripts/prepare_offdesk_workload.py` produces
the prepared manifest, workload review, launch packet, validation packet, and
approval-gated enqueue script. Domain-specific producers should reuse the same
manifest shape and add stricter `review_contract` requirements when they need
custom evidence bundles or post-run reviewers.

The final runtime dispatch should preserve the same scope and artifacts:

```bash
forager offdesk launch \
  --runner local-tmux \
  --project-key <project-key> \
  --request-id <request-id> \
  --task-id <task-id> \
  --cmd "<bounded-workload-command>" \
  --workdir /path/to/project \
  --result-artifact <workload-output>/result.json \
  --artifact manifest=<workload-output>/manifest.json \
  --artifact preflight=<workload-output>/preflight.json \
  dispatch.runtime
```

For overnight runs, prefer a wall-clock stop time over a fixed duration:

```bash
OFFDESK_RUN_UNTIL_KST=09:00 \
OFFDESK_LLM_BASE_URL=http://<gpu-server>:11434 \
OFFDESK_LLM_MODEL=qwen3-coder-next:latest \
<bounded-workload-command>
```

The prepared manifest records the estimated duration and target timestamp, but
the runtime workload recomputes the next `09:00 Asia/Seoul` when it actually
starts. That keeps approval or tick delays from accidentally pushing the run
past the morning review window.

Every prepare run writes `LAUNCH_DRY_RUN.md`, an operator-facing launch review
packet with the preflight verdict, blockers, safety boundary, key artifacts, and
the exact enqueue/approval/poll commands. If the role gate is missing, failed,
the module operation preflight is missing or unrecognized, or the review
artifact returns `blocked` or `revise`, `--enqueue` stops before adding the task
to the Offdesk queue and writes `preflight.json` plus `enqueue_blocked.json` in
the workload directory. The generated `offdesk_enqueue_command.sh` also checks
for `preflight_ready` unless `FORAGER_ALLOW_PREFLIGHT_BLOCKERS=1` is set
deliberately.

A generated review artifact should inspect the exact `prepared_task.json` in
the workload directory. A clean deterministic review returns `needs_approval`,
not `proceed`, because `dispatch.runtime` still requires separate operator
approval before `offdesk tick` can launch the workload.

The prepared task uses `dispatch.runtime` and `local-tmux` by default, so
`offdesk tick` should stop at an operator-required approval before the workload
starts. Approve the pending `dispatch.runtime` action and run `offdesk tick
--limit 1` only when the operator is ready to begin the bounded run. Use
`--runner local-background` only for short smoke workloads; long Python
workloads should use tmux so the process remains inspectable after the tick
command exits.

The current launch path is covered by a short smoke runbook:
[`Offdesk Runtime Smoke`](guides/offdesk-runtime-smoke.md).
That smoke validates prepare, enqueue, approval, local-tmux launch, polling,
result artifacts, and deterministic post-run review without starting an
overnight campaign.

The workload report separates raw pass/fail from operator judgement. `REPORT.md`
and `result.json` include an `assessment` block with `overall_verdict`,
`operator_risk`, `next_action`, failure-category counts, baseline policy
coverage, and `false_negative_prevented_count`. The latter increments when a
response satisfies a required domain anchor only through an accepted alias.
Treat `pass_with_canonicalization` as usable evidence that still deserves
prompt or checker review before comparing strict pass rates across models.

### Episode Council Gate

`scripts/offdesk_episode_council_harness.py` adds an optional GPT/Claude-style
Council between workload episodes. The Council is read-only and reviews the
completed episode record plus recent campaign state. It returns a deterministic
consensus decision: `continue`, `revise`, `pivot`, `handoff`, `block`,
`needs_approval`, or `needs_council_execution`.

The Council has three modes:

- `prompt-package`: write `gpt_prompt.md` and `claude_prompt.md` for manual or
  external execution;
- `mock`: produce deterministic reviewer JSON for smoke tests;
- `command`: send each prompt on stdin to configured reviewer commands from
  `--gpt-council-command` and `--claude-council-command`, or the
  `OFFDESK_GPT_COUNCIL_CMD` and `OFFDESK_CLAUDE_COUNCIL_CMD` environment
  variables.

Council integration is opt-in:

```bash
OFFDESK_GPT_COUNCIL_CMD="gpt-reviewer-command" \
OFFDESK_CLAUDE_COUNCIL_CMD="claude-reviewer-command" \
<bounded-workload-command-with-council-enabled>
```

When enabled, the workload writes one episode JSON artifact, runs the Council,
and stops on any non-`continue` decision by default. This makes a long campaign
an episode chain with explicit direction-change gates rather than a single
blind long-running job. The Council decision does not authorize mutation,
approval, cleanup, provider retargeting, or wiki promotion.

## Runtime Episode Harness

`scripts/offdesk_runtime_episode_harness.py` is a deterministic CLI harness for
the runtime wiring that sits below model behavior. It creates an isolated
profile under `target/offdesk-runtime-episode-harness/<timestamp>/`, writes a
small promoted wiki fixture, then drives real `forager offdesk` commands.

The harness checks that:

- gate without `--agent-mode` projects only shared runtime entries;
- gate with `--agent-mode development` projects shared plus matching
  mode-specific entries;
- `offdesk enqueue` plus `offdesk tick` writes a background probe with a fenced
  `adaptive_wiki_context`;
- runtime context excludes out-of-scope and deprecated entries;
- secret-like text in wiki instructions is redacted before runtime and debug
  surfaces;
- wiki context does not rewrite the task command, workdir, launch spec, or
  provider/model routing;
- `adaptive_wiki_usage.jsonl`, `offdesk debug-bundle`, and
  `offdesk wiki episode-trace` link the runtime projection back to the task;
- `FORAGER_ADAPTIVE_WIKI_RUNTIME=0` keeps preflight projection visible while
  omitting probe context and usage records.

Example:

```bash
scripts/offdesk_runtime_episode_harness.py --runtime-disabled-check
```

Results are preserved in the harness work root as `results.json` alongside the
isolated profile and runtime artifacts.

## Role Episode Harness

`scripts/offdesk_role_episode_harness.py` is a deterministic CLI harness for
role-specific projection behavior. It does not call a model. It creates an
isolated profile under `target/offdesk-role-episode-harness/<timestamp>/`,
writes shared entries plus planning, development, analysis, writing, critique,
review, maintenance, and deprecated wiki entries, then drives real
`forager offdesk gate inspect.status` commands.

The harness checks that:

- gate without `--agent-mode` projects shared entries only;
- gate with `--agent-mode planning` projects shared plus planning entries only;
- gate with `--agent-mode development` projects shared plus
  development entries only;
- gate with `--agent-mode analysis` projects shared plus analysis entries only;
- gate with `--agent-mode writing` projects shared plus
  writing entries only;
- gate with `--agent-mode critique` projects shared plus critique entries only;
- gate with `--agent-mode review` projects shared plus review entries only,
  not critique entries;
- gate with `--agent-mode maintenance` projects shared plus maintenance entries
  only;
- legacy `code_development` and `research_writing` entries still project into
  canonical `development` and `writing` modes;
- deprecated entries never appear in any AI projection;
- the episode exercises every role-specific entry at least once.

Example:

```bash
scripts/offdesk_role_episode_harness.py
```

Results are preserved in the harness work root as `results.json` alongside the
isolated profile fixture.

## Live Role LLM Episode Harness

`scripts/offdesk_role_llm_episode_harness.py` is the live model companion to
the deterministic role episode harness. It creates an isolated profile under
`target/offdesk-role-llm-episode-harness/<timestamp>/`, writes shared,
development, writing, critique, and deprecated wiki entries with
role markers, loads the execution-facing projection through real `forager
offdesk gate inspect.status` calls, then sends the projected context to an
Ollama-compatible model.

The harness checks that:

- the gate projection contains shared plus matching role guidance and no
  out-of-scope or deprecated marker;
- the model response uses the matching visible role marker;
- the model does not emit out-of-scope role markers;
- the model keeps adaptive wiki guidance as context only, not execution
  authority;
- the model does not claim completed edits, tests, approvals, or execution
  without evidence;
- writing remains pending when RunLog and validation evidence are
  missing;
- critique asks for no-option and singlex evidence before strategy changes.

For repeated preflight runs, the harness records `failure_category_counts`,
per-case `case_summary`, and a `quality_gate` block. Failure categories include
`projection_leakage`, `response_role_leakage`, `authority_boundary_failure`,
`false_completion_claim`, `research_overclaim`, `critique_baseline_skip`,
`json_format_failure`, and `empty_response`. The `quality_gate` verdict remains
`blocked` until every selected episode passes.

Example:

```bash
scripts/offdesk_role_llm_episode_harness.py \
  --model qwen3-coder-next:latest \
  --base-url http://<gpu-server>:11434 \
  --temperature 0.0 \
  --iterations 5 \
  --max-budget 2048 \
  --num-ctx 8192
```

Results are preserved in the harness work root as `results.json`. Full model
responses are omitted by default; pass `--store-response-text` when debugging a
role-leakage or authority-boundary failure.

## Current v0

The current implementation includes the canonical store, projection builders,
preflight projection exposure, fenced runtime probe/handoff context, usage audit
records, approval-denial candidate capture, read-only wiki inspection commands,
governed review mutation commands, basic governance lint, one-way markdown
vault export, append-only review audit records, curator review reports,
procedure runbook metadata, deterministic episode evaluation reports, live
episode trace reports over existing durable artifacts, correction recurrence
evaluation, first-class correction evidence records, promotion evidence chain
reports, evidence-graph-aware curator proposals, proposal lifecycle event
logging, projection quality reports, review-expired projection warnings,
promoted-entry conflict proposals, the live LLM prompt harness, the runtime
episode harness, the role-specific projection episode harness, and the live
role-specific LLM episode harness. It intentionally does not yet:

- make wiki context an authority for commands, launch specs, approvals,
  provider/model routing, or workdir changes;
- create dashboard actions;
- auto-apply wiki-driven mutations;
- migrate existing `operator_preferences.json` artifacts;
- run live benchmark episodes automatically from a new task harness.

Those should be separate integration passes after the projection contract is
stable.
