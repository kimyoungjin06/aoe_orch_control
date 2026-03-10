# Rulebook v1 (Multi-Agent TF)

status: active
effective_date: __TAG__
mode: rule-first

## Core Rules
1. Runtime truth is `.aoe-team/orch_manager_state.json`; document truth is `project_lock.yaml`.
2. Active project is exactly one per operator view; active TF is exactly one per project focus.
3. Project-level execution context goes to `projects/<project>/ongoing.md`.
4. TF-level execution context goes to `projects/<project>/tfs/<tf>/ongoing.md`.
5. Accepted decisions go to TF `note.md` and are indexed in `decision_index.csv`.
6. TF-to-TF context transfer must be written to `handoff.md` and indexed in `handoff_index.csv`.
7. Failed or deferred items move to TF `archive/` and `archive_index.csv`.
8. Switch order: lock update -> registry sync -> project ongoing update -> TF ongoing update.
9. Evidence reference is mandatory before marking a task as done.
10. Path/schema changes require constitution/charter/roadmap update in the same change set.

## TF Close Gate
- `done`: objective met + evidence attached + critic accepted
- `retry`: objective unmet but recoverable
- `escalate`: external dependency, policy conflict, or unresolved critic verdict
