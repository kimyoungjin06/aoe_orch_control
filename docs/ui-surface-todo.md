# UI Surface TODO

Updated: 2026-06-18

This backlog turns the current UI review into implementation-sized work. It is
not a visual redesign brief. The main gap is information architecture: Telegram,
TUI, WebUI, and CLI/JSON should expose the same operator state and decision
model while using different levels of detail.

Long-term Web dashboard and control-plane work is tracked separately in
`docs/web-dashboard-control-plane-todo.md`. This document stays focused on the
first shared UI surface and renderer slices.

## Review Summary

The original UI surface review found these gaps:

- The website explained the product direction, but did not yet expose rich
  WebUI review or workstation-dashboard routes.
- The Remote Operator Telegram adapter has a strict mobile contract
  (`5 lines / 360 chars`), but the decision relay card still allows much denser
  primary messages.
- The TUI has an Offdesk morning-review card, but its primary counters are
  compressed as `p/q/a/r/f/c` or `p/q/a/r/f/x` and depend on help text for
  interpretation.
- The preview panel still centers terminal output before semantic review state.

## Implementation Status

Completed in the first UI pass:

- Added `operator_state_card.v1` fixtures for the core operator states and a
  pure renderer that projects them into Telegram, TUI rows, and WebUI card JSON.
- Applied a strict mobile primary-card contract to the Telegram decision relay.
- Replaced the TUI Offdesk morning-review abbreviations with semantic labels
  and width-sensitive render tests.
- Added a fixture-backed read-only WebUI review route at `/review/`.
- Added a semantic TUI preview summary for error, decision, Offdesk, closeout,
  and next-action signals before raw output.
- Added a full-screen read-only TUI attention queue on `a`, backed by the
  existing prioritized next-safe-action contract. It keeps approval, recovery,
  review, monitoring, and dispatch semantics explicit and shows commands as
  not executed with their authorization boundary.
- Fixed preview path shortening so paths that only share the home-directory
  string prefix no longer render as home-relative.
- Added a landing-page path to the new `/review/` surface so public product copy
  does not leave the operator UI hidden behind docs only.
- Installed Playwright visual smoke coverage for desktop and mobile review
  route screenshots.
- Added a live `review_surface.v1` export script and client-side hydration path
  so `/review/` can stay static while reading current local operator state.
- Added Playwright coverage for the exported live review-surface contract.
- Refreshed website dependencies to Astro 7 and Tailwind 4, closed current
  `npm audit` findings, and kept desktop/mobile Playwright coverage passing.
- Added a long-term Web dashboard/control-plane backlog and a fixture-backed
  read-only `/dashboard/` route for `workstation_surface.v1`.
- Added landing-page and nav entry points to `/dashboard/`, plus desktop/mobile
  Playwright coverage for dashboard rendering and live surface hydration.
- Added `forager ondesk workstation-surface --json` and
  `npm run export:workstation-surface` so the static dashboard can hydrate from
  current local Forager state.

Remaining after this pass:

- Extend browser coverage to more live review-state variants after the first
  exported-state path is used in daily operation.
- Polish the public website route/copy balance after the live review data path
  is proven.

## P0 - Shared Operator State Contract

Goal: define the common compact state card that every human-facing UI can
project.

Todo:

- Define `operator_state_card.v1` as a small read model over existing
  `review_surface.v1`, `next_safe_actions`, remote-operator projections, and
  decision records.
- Required fields:
  - `title`
  - `severity`
  - `state_summary`
  - `primary_blocker_or_decision`
  - `next_safe_action`
  - `detail_ref`
  - `authorization_boundary`
- Add fixture examples for:
  - no work pending
  - approval pending
  - local agent/model outage
  - failed Offdesk task
  - closeout required
  - plan ready for review
- Add tests that all compact renderers can consume the same fixture without
  inventing surface-specific state.

Done when:

- One fixture can render into Telegram text, TUI rows, and a WebUI card without
  querying unrelated state.
- The first next-safe action matches `forager status --json`.

Status: implemented for fixture-backed renderer tests. Live
`forager status --json` wiring remains a follow-up.

## P0 - Telegram Decision Relay Mobile Contract

Goal: make all Telegram primary messages mobile-scannable, not only the Remote
Operator adapter.

Todo:

- Port or share the mobile-card contract from
  `scripts/offdesk_remote_operator_telegram.py`.
- Apply a hard primary-card budget to
  `scripts/offdesk_telegram_decision_relay.py`.
- Move dense material to the detail card:
  - evidence
  - Council details
  - failure diagnostics
  - reply examples
  - long scope text
- Stop requiring every primary card to contain both `질문` and `범위` when the
  inline keyboard and input placeholder already carry the interaction model.
- Keep direct typing available through placeholders and free-form reply handling.
- Add regression tests for line count, character count, forbidden terms, and
  button labels.

Done when:

- Decision relay primary cards follow a comparable budget to Remote Operator
  cards.
- Buttons answer the common case, and direct typing remains discoverable.
- Raw ids, local paths, request files, and trace-only fields stay out of the
  mobile message.

Status: implemented for decision relay primary cards and regression-tested.

## P0 - TUI Offdesk Cockpit

Goal: make the TUI answer "what needs my attention now?" without decoding
abbreviations.

Todo:

- Replace or supplement `p/q/a/r/f/x` with labeled rows:
  - approval pending
  - queued
  - active
  - resume pending
  - failed
  - closeout required
- Keep a narrow-terminal fallback, but make the default view semantic.
- Move the next-safe action into a stable row with clear command/action wording.
- Add width-sensitive render tests for common terminal sizes.
- Keep the help dialog as secondary explanation, not the only place where the
  counters are understandable.

Done when:

- At 80 columns, the operator can identify the top Offdesk state and next safe
  action without opening help.
- At wider terminals, the card shows enough counts to avoid opening raw status
  JSON first.

Status: implemented and covered by 80-column render tests.

The follow-up actionable attention panel is also implemented with keyboard
navigation, a scrollable detail state, and an explicit empty state. It remains
read-only and does not approve or execute the displayed commands.

## P1 - WebUI Review Route

Goal: create the first real rich review surface instead of only describing
WebUI in product copy.

Todo:

- Start with a read-only renderer for `review_surface.v1`.
- Show:
  - status and severity
  - first next-safe action
  - accepted-truth state
  - open decisions
  - runtime summary
  - artifact meaning before artifact location
  - closeout and wiki review state
- Keep the first implementation local/static if needed. Do not build a heavy
  server before the read model is proven.
- Add fixture-backed screenshots or HTML snapshots for the main review states.

Done when:

- A morning review can be inspected from one page without opening terminal
  scrollback first.
- Telegram handoff/detail links have a concrete rich review target.

Status: implemented as a static route with fixture fallback, optional
`review_surface.v1` hydration from `public/review-surface.json`, an export
script, and Playwright desktop/mobile plus live-contract smoke coverage.

## P1 - Preview Panel Semantic State

Goal: reduce dependence on raw terminal scrollback in the TUI preview.

Todo:

- Add a semantic summary block above terminal output when Offdesk or decision
  state exists.
- Surface last error, current decision, latest next-safe action, and artifact
  summary before raw output.
- Keep raw output available, but treat it as detail.
- Fix path shortening ambiguity where paths that merely share a prefix with
  home can render like `~extra/...`.

Done when:

- A completed, failed, or blocked session can be understood from the preview
  summary before reading terminal output.
- Path display never makes a non-home path look like a home-relative path.

Status: implemented for session error, Offdesk, decision, closeout, failure, and
next-action signals, with path-shortening regression coverage.

## P2 - Website Visual And Copy Polish

Goal: make the public site reflect the product without becoming the operator UI.

Todo:

- Separate product-marketing copy from operator-workflow entry points.
- Add a visible path from landing page to the actual review surface once it
  exists.
- Rebalance the dominant dark slate/cyan palette with the brand system rather
  than another single-hue variant.
- Remove unused decorative CSS if it is not used by the current pages.
- Keep the Astro/Tailwind 4 build path covered by homepage and review-route
  smoke tests after future dependency refreshes.

Done when:

- The public site explains Forager, while the WebUI/review route handles
  current operational state.

Status: partially implemented. The landing page and nav now link to `/review/`.
Playwright covers the homepage and review route on desktop and mobile. Broader
visual polish remains a separate follow-up.

## Suggested Implementation Order

1. Add `operator_state_card.v1` fixtures and a pure renderer contract test.
   Keep this slice small: no new WebUI route, no TUI layout change, and no
   Telegram behavior change until the shared read model is stable.
2. Apply the mobile-card contract to the Telegram decision relay using the new
   shared card fixture.
3. Upgrade the TUI Offdesk morning-review card to semantic labels using the same
   fixture.
4. Add the first fixture-backed WebUI review route.
5. Improve the TUI preview semantic block.
6. Add live `review_surface.v1` export/hydration for `/review/`.
7. Polish the website after the real review route is exercised with daily state.

## First Implementation Slice

The first slice should be deliberately narrow:

- Add fixture JSON under `tests/fixtures/ui/operator_state_cards/`.
- Add a renderer helper for compact cards, preferably in a small module rather
  than inside the large Telegram adapter.
- Prove that the helper can render at least:
  - a Telegram primary-card string within budget;
  - TUI-friendly labeled rows;
  - a WebUI-card JSON projection.
- Do not change live Telegram sending or TUI layout in the same patch unless the
  contract tests are already passing.

This keeps the next patch reversible and avoids coupling the contract decision
to a specific visual surface too early.

## Review Gates

Before promoting any UI slice:

- The same state fixture must render consistently across all targeted surfaces.
- Compact Telegram messages must satisfy the mobile contract.
- TUI render tests must include at least one narrow viewport.
- Rich WebUI/HTML output must be screenshot- or snapshot-checked.
- User-facing summaries must avoid raw paths, request ids, secret-like values,
  and trace-only JSON keys unless explicitly expanded in a detail view.
