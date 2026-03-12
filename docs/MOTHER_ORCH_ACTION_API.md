# MOTHER ORCH ACTION API

## 1. Purpose

This document fixes the stable control-plane seam Mother-Orch should use before
any MCP adapter is added.

The Action API is the layer that sits between:

- plaintext intent routing
- Telegram / CLI / future MCP adapters
- project/backlog/task execution internals

The goal is to ensure Mother-Orch always decides through one normalized
contract instead of letting each adapter invent its own shortcuts.

Implementation seam:

- `scripts/gateway/aoe_tg_orch_actions.py`

## 2. Why This Exists

The system already has working commands such as:

- `/map`
- `/queue`
- `/todo`
- `/sync`
- `/offdesk`
- `/dispatch`

But Mother-Orch should not reason in raw command strings.

It should reason in action categories:

- `status`
- `inspect`
- `work`
- `control`

This keeps three responsibilities separate:

1. plaintext interpretation
2. control-plane action selection
3. downstream execution backend choice

## 3. Call Envelope

Every Mother-Orch action call should normalize to the same envelope:

- `action`
- `family`
- `intent_class`
- `risk_level`
- `project_key`
- `readonly`
- `mutates_runtime`
- `mutates_canonical`
- `operator_surface`
- `args`

Operational meaning:

- `intent_class` says what kind of user intent this is
- `risk_level` says what kind of mutation boundary the action crosses
- `project_key` scopes the action when required
- `args` contains action-specific parameters only after validation

## 4. Intent Classes

- `status`
  - short operator status lookups
  - examples: project state, queue state, current task, offdesk status
- `inspect`
  - read-heavy investigation or preview
  - examples: sync preview, salvage preview, syncback preview
- `work`
  - real TF-producing work
  - examples: dispatch task, retry task, replan task, sync apply
- `control`
  - control-plane mutation
  - examples: focus project, accept proposal, offdesk start

## 5. Risk Levels

- `safe`
  - read-only from Mother-Orch point of view
  - may still read project files or runtime state
- `runtime_mutation`
  - mutates runtime queue/task/session state
  - does not mutate canonical project backlog
- `canonical_mutation`
  - mutates canonical project backlog such as `TODO.md`

This distinction matters because:

- Mother-Orch should be conservative about escalating from `safe` to mutation
- future MCP adapters must preserve these boundaries
- offdesk policy should treat canonical mutation as a stricter class than queue mutation

## 6. Action Families

### 6.1 Project Registry

- `list_projects`
- `focus_project`
- `clear_focus`

These actions manage operator scope and project targeting.

### 6.2 Project Status

- `get_project_status`
- `monitor_project`

These actions answer “what is happening now?” without changing work.

### 6.3 Backlog

- `get_queue`
- `get_task`
- `list_followups`

These actions expose runtime backlog and TF state to Mother-Orch.

### 6.4 Backlog Sync

- `sync_preview`
- `sync_apply`
- `sync_salvage`

These actions are the bridge from recent documents/canonical backlog into the
runtime queue.

### 6.5 TF Execution

- `dispatch_task`
- `retry_task`
- `replan_task`

These actions are the only entrypoints that should create or re-enter TF work.

### 6.6 Proposal Inbox

- `accept_proposal`
- `reject_proposal`

These actions keep TF-generated follow-up work from writing backlog directly.

### 6.7 Canonical Backlog

- `syncback_preview`
- `syncback_apply`

These actions make canonical TODO drift explicit and auditable.

### 6.8 Offdesk

- `offdesk_prepare`
- `offdesk_review`
- `offdesk_start`
- `offdesk_status`

These actions are the operating surface for Mother-Orch nightly control.

## 7. Current Default Mapping

Mother-Orch should prefer these mappings:

- plain-language “what is happening?” -> `status`
- plain-language “check / inspect / summarize current state” -> `inspect`
- plain-language “do / fix / write / analyze / push / rerun” -> `work`
- explicit scope/approval/backlog decisions -> `control`

Important rule:

- plain text should not drop into raw direct-chat by default
- it should first normalize into one of these Action API calls

## 8. Non-Goals

The Action API is not:

- a replacement for the runtime queue schema
- a TF backend API
- an MCP protocol by itself

Those are downstream concerns.

The Action API is the stable seam *before* those adapters/backend choices.

## 9. Relationship To MCP

MCP is a good future adapter surface, but not the first design layer.

The correct order is:

1. define Mother-Orch Action API
2. keep Telegram/CLI aligned to it
3. only then expose it through MCP if needed

If MCP is added later, it should map almost 1:1 to these actions, for example:

- `list_projects`
- `get_project_status`
- `get_queue`
- `sync_preview`
- `dispatch_task`
- `retry_task`
- `accept_proposal`
- `offdesk_prepare`
- `offdesk_start`

## 10. Immediate Follow-Up

The next implementation step after this document is:

1. use Action API in the plaintext router
2. classify natural-language input into `status / inspect / work / control`
3. keep `/direct` as an expert-only escape hatch, not the default path
