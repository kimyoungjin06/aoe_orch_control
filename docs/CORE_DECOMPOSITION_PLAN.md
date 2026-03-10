# CORE DECOMPOSITION PLAN

## 1. Goal

This plan defines the next structural refactor for the remaining gateway cores.

The objective is not a rewrite.

The objective is:

- reduce change risk in the largest files
- move domain rules out of handler monoliths
- preserve current operator behavior while changing internals

## 2. Current Baseline

Current large cores:

- `scripts/gateway/aoe-telegram-gateway.py` - `7685` lines
- `scripts/gateway/aoe_tg_scheduler_handlers.py` - `3797` lines
- `scripts/gateway/aoe_tg_run_handlers.py` - `2391` lines
- `scripts/gateway/aoe_tg_management_handlers.py` - `2367` lines

Existing extracted seams already in place:

- `scripts/gateway/aoe_tg_ops_policy.py`
- `scripts/gateway/aoe_tg_ops_view.py`
- `scripts/gateway/aoe_tg_schema.py`
- `scripts/gateway/aoe_tg_blocked_state.py`
- `scripts/gateway/aoe_tg_task_state.py`
- `scripts/gateway/aoe_tg_task_view.py`
- `scripts/gateway/aoe_tg_todo_policy.py`
- `scripts/gateway/aoe_tg_todo_state.py`
- `scripts/gateway/aoe_tg_package_paths.py`

This means the next cuts should target orchestration glue that still owns too
many responsibilities.

## 3. Non-Goals

Do not do these as part of the decomposition effort:

- rewrite the Telegram command surface
- redesign offdesk semantics
- change canonical `TODO.md` policy
- replace tmux runtime model
- convert everything into classes/framework abstractions without need

## 4. Refactor Rules

Every decomposition change should follow these rules:

1. preserve external command behavior first
2. keep state shape backward-compatible unless migration is explicit
3. add parity tests before or during extraction
4. move policy/state/view logic first, keep handlers thin
5. avoid introducing a new dependency injection framework

## 5. Phase Order

### Phase 1. Gateway composition root reduction

Target file:

- `scripts/gateway/aoe-telegram-gateway.py`

#### 1A. Transport split

Create:

- `scripts/gateway/aoe_tg_transport.py`

Move:

- Telegram send/retry helpers
- polling helpers
- quick keyboard primitives that are transport-local
- failed queue serialization helpers that belong to poll/send lifecycle

Expected source candidates:

- `tg_api`
- `tg_send_text`
- `safe_tg_send_text`
- `tg_get_updates`
- `split_text`
- `build_quick_reply_keyboard`

Keep in gateway root:

- CLI argument parsing
- top-level bootstrapping
- composition of transport + handlers

#### 1B. Runtime/core state split

Create:

- `scripts/gateway/aoe_tg_runtime_core.py`

Move:

- project root/team dir/state path resolution
- manager state load/save glue
- registry bootstrap
- gateway event append/mirror helpers
- process lock helpers

Expected source candidates:

- `resolve_project_root`
- `resolve_team_dir`
- `resolve_state_file`
- `load_manager_state`
- `save_manager_state`
- `ensure_default_project_registered`
- `log_gateway_event`
- `append_gateway_event_targets`
- `acquire_process_lock`

Acceptance criteria:

- `aoe-telegram-gateway.py` becomes a real assembly layer
- transport/state helpers are not edited in place anymore

### Phase 2. Scheduler domain split

Target file:

- `scripts/gateway/aoe_tg_scheduler_handlers.py`

#### 2A. Sync source extraction

Create:

- `scripts/gateway/aoe_tg_sync_sources.py`

Move:

- source classification
- scenario include parsing
- todo/recent/salvage discovery
- source policy loading/application
- provenance tagging

Expected source candidates:

- `_scenario_include_tokens`
- `_load_project_sync_policy`
- `_apply_sync_policy`
- `_classify_sync_source`
- `_recent_doc_candidates`
- `_todo_file_candidates`
- `_discover_todo_file_todos`
- `_discover_recent_doc_todos`
- `_discover_salvage_doc_todos`
- `_discover_salvage_doc_proposals`
- `_discover_sync_fallback_todos`

#### 2B. Sync merge engine extraction

Create:

- `scripts/gateway/aoe_tg_sync_merge.py`

Move:

- duplicate choice
- replace/prune behavior
- sync metadata stamping
- scenario item application

Expected source candidates:

- `_choose_sync_row`
- `_apply_scenario_items_to_entry`
- `_stamp_sync_meta`
- merge/prune internals inside `handle_scheduler_command`

#### 2C. Queue engine extraction

Create:

- `scripts/gateway/aoe_tg_queue_engine.py`

Move:

- next candidate selection
- global candidate selection
- status counting
- queue summary assembly

Expected source candidates:

- `_pick_global_next_candidate`
- `_count_todo_statuses`
- `_queue_reply_markup`
- `_find_pending_todo_for_chat`

Acceptance criteria:

- scheduler handler becomes command UX orchestration
- sync heuristics no longer live primarily in one file

### Phase 3. Run pipeline split

Target file:

- `scripts/gateway/aoe_tg_run_handlers.py`

#### 3A. Planning pipeline extraction

Create:

- `scripts/gateway/aoe_tg_plan_pipeline.py`

Move:

- success-first prompt fallback
- planning prompt construction
- planner/critic/repair sequencing
- plan application and lineage glue
- planning progress emission helpers

Expected source candidates:

- `_apply_success_first_prompt_fallbacks`
- `_compute_dispatch_plan`
- `_emit_planning_progress`
- `_apply_plan_and_lineage`

#### 3B. Execution pipeline extraction

Create:

- `scripts/gateway/aoe_tg_exec_pipeline.py`

Move:

- dispatch execution
- dispatch exception recovery
- result synthesis
- proposal capture
- terminal todo cleanup/finalization

Expected source candidates:

- `_dispatch_and_sync_task`
- `_send_dispatch_result`
- `_send_dispatch_exception`
- `_maybe_capture_todo_proposals`
- `_finalize_todo_after_run`
- `_cleanup_terminal_todo_gate`

Acceptance criteria:

- run handlers keep request UX routing
- planning and execution policy stop living in the same file

### Phase 4. Management handler cleanup

Target file:

- `scripts/gateway/aoe_tg_management_handlers.py`

This file is smaller priority than the previous three, but still large.

Create only after phases 1-3 stabilize:

- `scripts/gateway/aoe_tg_offdesk_flow.py`
- `scripts/gateway/aoe_tg_report_view.py`

Move:

- offdesk prepare/review/report assembly
- auto/offdesk short/long report builders
- project preflight aggregation

Acceptance criteria:

- management handlers mostly map command -> domain function -> reply

## 6. Test Strategy Per Phase

For each phase:

1. identify current behavior via parity tests
2. extract helpers/modules
3. rerun targeted parity tests
4. rerun smoke for affected command family

Minimum regression coverage:

- transport:
  - polling/send helpers
  - failed queue replay behavior
- scheduler:
  - `/sync preview`
  - `/sync replace`
  - `/queue`
  - `/next`
  - `/fanout`
- run:
  - confirm flow
  - planning gate
  - dispatch exception
  - proposal capture
- management:
  - `/offdesk prepare`
  - `/offdesk review`
  - `/auto status`

## 7. Sequencing Policy

Do not attempt all phases in one branch unless changes are trivial.

Preferred sequence:

1. one phase per branch
2. one command family per branch when possible
3. merge after tests are green and behavior diff is minimal

Recommended next execution order:

1. Phase 2A + 2B first
2. Phase 1A + 1B second
3. Phase 3A + 3B third
4. Phase 4 last

Reason:

- current operational risk is highest in scheduler sync/backlog code
- gateway composition root is important but lower immediate incident frequency
- run pipeline is large, but now has clearer seams after schema/state extraction

## 8. Success Metrics

Short-term success:

- no new runtime/state regression from refactor work
- fewer duplicate helper implementations
- smaller diff surface for scheduler/run changes

Mid-term success:

- `aoe-telegram-gateway.py` under `5k` lines
- `aoe_tg_scheduler_handlers.py` under `2.5k` lines
- `aoe_tg_run_handlers.py` under `1.6k` lines

Long-term success:

- large handler files mostly contain orchestration glue
- new behavior is implemented in policy/state/view/pipeline modules by default

## 9. Immediate Next Action

Start with:

- Phase 2A: `aoe_tg_sync_sources.py`

Why:

- it addresses the highest operational risk
- it isolates the most failure-prone heuristics
- it improves both offdesk quality and future scheduler changes

## 10. Related Docs

- `docs/ARCHITECTURE.md`
- `docs/DEPLOYMENT.md`
- `docs/ROADMAP.md`
- `docs/RUNBOOK.md`
