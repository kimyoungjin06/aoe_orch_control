# PR Draft: Scheduler Decomposition 1

## Proposed Title

Extract scheduler domain helpers and thin gateway management layers

## Summary

This branch separates the scheduler domain into explicit modules and reduces
the size of the remaining gateway/management handler files without changing the
operator command surface.

Main changes:

- split scheduler source discovery into `scripts/gateway/aoe_tg_sync_sources.py`
- split sync merge/prune logic into `scripts/gateway/aoe_tg_sync_merge.py`
- split queue selection helpers into `scripts/gateway/aoe_tg_queue_engine.py`
- split offdesk/auto/focus/panic control handling into
  `scripts/gateway/aoe_tg_scheduler_control_handlers.py`
- split management chat/ACL flows into:
  - `scripts/gateway/aoe_tg_management_chat.py`
  - `scripts/gateway/aoe_tg_management_acl.py`
- keep `scripts/gateway/aoe_tg_scheduler_handlers.py` focused on command UX,
  diagnostics, and transition wiring

## Why

Before this branch, scheduler behavior lived mostly inside
`scripts/gateway/aoe_tg_scheduler_handlers.py`, which made sync heuristics,
merge rules, queue selection, and operator UX hard to change safely.

This branch moves domain logic behind clearer seams:

- source discovery and classification
- merge/prune and sync metadata
- queue selection and status counting
- operator control plane

The goal is not a rewrite. The goal is to lower change risk for future work on
sync quality, offdesk behavior, and queue semantics.

## Current Module Boundaries

Scheduler-related modules after this branch:

- `scripts/gateway/aoe_tg_scheduler_handlers.py`
  - command UX orchestration
  - `/sync`, `/queue`, `/next`, `/fanout` reply flow
  - sync diagnostics and replay handling
- `scripts/gateway/aoe_tg_sync_sources.py`
  - scenario include parsing
  - source classification
  - recent/todo/salvage discovery
  - provenance tagging
- `scripts/gateway/aoe_tg_sync_merge.py`
  - replace/prune
  - scenario item application
  - sync metadata stamping
- `scripts/gateway/aoe_tg_queue_engine.py`
  - todo sorting
  - status counts
  - next candidate selection
  - drain peek helpers

Management control modules touched by the same refactor:

- `scripts/gateway/aoe_tg_scheduler_control_handlers.py`
- `scripts/gateway/aoe_tg_offdesk_flow.py`
- `scripts/gateway/aoe_tg_management_chat.py`
- `scripts/gateway/aoe_tg_management_acl.py`

## Size Snapshot

Current key file sizes on this branch:

- `scripts/gateway/aoe-telegram-gateway.py` - `2713`
- `scripts/gateway/aoe_tg_scheduler_handlers.py` - `1889`
- `scripts/gateway/aoe_tg_sync_sources.py` - `1680`
- `scripts/gateway/aoe_tg_run_handlers.py` - `1611`
- `scripts/gateway/aoe_tg_scheduler_control_handlers.py` - `1139`
- `scripts/gateway/aoe_tg_management_handlers.py` - `705`

## Validation

Executed on this branch:

- `scripts/gateway_pytest.sh tests/gateway/test_scheduler_sync_parse.py`
- `bash scripts/gateway_smoke_test.sh`
- `bash scripts/gateway_error_test.sh`

Recommended reviewer focus:

- `/sync preview` and fallback diagnostics
- replace/prune semantics
- `/next` and `/fanout` candidate selection
- offdesk/auto control commands after handler split

## Risks

- `scripts/gateway/aoe_tg_sync_sources.py` is still large and contains multiple
  heuristics in one file.
- `scripts/gateway/aoe_tg_scheduler_handlers.py` still owns substantial UX and
  replay behavior.
- queue semantics are now clearer, but they still depend on consistent use of
  shared helpers from `aoe_tg_ops_policy.py` and `aoe_tg_queue_engine.py`.

## Follow-Up After Merge

1. Split `scripts/gateway/aoe_tg_sync_sources.py` again if parser/discovery
   logic grows further.
2. Keep new scheduler changes out of `scripts/gateway/aoe_tg_scheduler_handlers.py`
   unless they are pure command UX.
3. Continue the next decomposition pass in `scripts/gateway/aoe_tg_run_handlers.py`.
