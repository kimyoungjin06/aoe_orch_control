# Operating Model (Low-Context First, Multi-Agent)

## Source-of-Truth Hierarchy
1. `.aoe-team/orch_manager_state.json` (runtime)
2. `docs/investigations_mo/registry/project_lock.yaml` (document lock)
3. `docs/investigations_mo/registry/project_flow_branching.md`
4. `docs/investigations_mo/projects/<project_alias>/ongoing.md`
5. `docs/investigations_mo/projects/<project_alias>/tfs/<tf_id>/report.md`
6. `docs/investigations_mo/registry/tf_registry.md`
7. `docs/investigations_mo/registry/runlog_recent.md`

## Protocol
- project/TF switch: lock -> registry -> project ongoing -> TF report
- lifecycle: TF report (single doc) + success-only bundle in `projects/<alias>/archive/<tf_id>.tar.gz`
- decisions: only mark done after critic and evidence
