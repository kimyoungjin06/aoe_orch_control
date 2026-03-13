#!/usr/bin/env python3
"""Gateway state and workflow regression tests."""

from _gateway_test_support import *  # noqa: F401,F403

def test_sync_salvage_creates_proposals_when_only_loose_followups_exist(tmp_path: Path) -> None:
    project_root = tmp_path / "DemoProject"
    team_dir = project_root / ".aoe-team"
    docs_dir = project_root / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "handoff.md").write_text(
        "# Research Handoff\n\n"
        "## Next steps\n"
        "- validate the overnight bootstrap path\n"
        "- review the queue status before morning standup\n",
        encoding="utf-8",
    )

    manager_state = gw.default_manager_state(project_root, team_dir)
    entry = manager_state["projects"]["default"]
    entry["project_root"] = str(project_root)
    entry["team_dir"] = str(team_dir)
    entry["display_name"] = "DemoProject"
    entry["project_alias"] = "O1"

    sent: list[tuple[str, dict]] = []

    def _send(body: str, **kwargs) -> bool:
        sent.append((body, kwargs))
        return True

    def _get_context(target: str | None):
        token = str(target or "default").strip().lower()
        if token in {"default", "o1", "demoproject"}:
            return "default", entry, argparse.Namespace(team_dir=team_dir, project_root=project_root)
        raise RuntimeError(f"unknown orch project: {target}")

    saved = {"n": 0}

    def _save_manager_state(*_args, **_kwargs) -> None:
        saved["n"] += 1

    result = sched.handle_scheduler_command(
        cmd="sync",
        args=argparse.Namespace(dry_run=False, manager_state_file=str(team_dir / "orch_manager_state.json")),
        manager_state=manager_state,
        chat_id="owner",
        chat_role="owner",
        orch_target=None,
        rest="salvage default since 5h",
        send=_send,
        get_context=_get_context,
        save_manager_state=_save_manager_state,
        now_iso=lambda: "2026-03-10T00:10:00+0900",
    )

    assert result == {"terminal": True}
    assert entry.get("todos") == []
    proposals = entry.get("todo_proposals") or []
    assert [row.get("summary") for row in proposals] == [
        "validate the overnight bootstrap path",
        "review the queue status before morning standup",
    ]
    assert saved["n"] >= 1
    assert sent
    body = sent[-1][0]
    assert "mode: salvage_docs" in body
    assert "- proposed: 2" in body


def test_ops_policy_summarizes_visible_and_hidden_projects() -> None:
    projects = {
        "default": {
            "project_alias": "O1",
            "display_name": "default",
            "system_project": True,
            "ops_hidden": True,
            "ops_hidden_reason": "internal fallback project",
        },
        "twinpaper": {
            "project_alias": "O2",
            "display_name": "TwinPaper",
            "ops_hidden": True,
            "ops_hidden_reason": "project on hold",
        },
        "nano": {
            "project_alias": "O3",
            "display_name": "Nano",
        },
    }

    scope = ops_policy.summarize_ops_scope(projects)

    assert scope["included"] == ["O3 Nano"]
    assert scope["excluded"] == [
        "O1 default (internal fallback project)",
        "O2 TwinPaper (project on hold)",
    ]


def test_ops_policy_list_projects_can_skip_paused_and_require_ready(tmp_path: Path) -> None:
    ready_root = tmp_path / "Ready"
    ready_team = ready_root / ".aoe-team"
    ready_team.mkdir(parents=True, exist_ok=True)
    (ready_team / "orchestrator.json").write_text("{}", encoding="utf-8")

    paused_root = tmp_path / "Paused"
    paused_team = paused_root / ".aoe-team"
    paused_team.mkdir(parents=True, exist_ok=True)
    (paused_team / "orchestrator.json").write_text("{}", encoding="utf-8")

    projects = {
        "ready": {
            "project_alias": "O3",
            "display_name": "Ready",
            "project_root": str(ready_root),
            "team_dir": str(ready_team),
            "paused": False,
        },
        "paused": {
            "project_alias": "O4",
            "display_name": "Paused",
            "project_root": str(paused_root),
            "team_dir": str(paused_team),
            "paused": True,
        },
        "broken": {
            "project_alias": "O5",
            "display_name": "Broken",
            "project_root": str(tmp_path / "Broken"),
            "team_dir": str(tmp_path / "Broken" / ".aoe-team"),
            "paused": False,
        },
    }

    visible_keys = [key for key, _entry in ops_policy.list_ops_projects(projects)]
    schedulable_keys = [
        key for key, _entry in ops_policy.list_ops_projects(projects, skip_paused=True, require_ready=True)
    ]

    assert visible_keys == ["ready", "paused", "broken"]
    assert schedulable_keys == ["ready"]


def test_ops_policy_builders_render_next_none_and_batch_finish() -> None:
    no_next = ops_policy.build_no_runnable_todo_message(
        focus_label="O4 Local_Map_Analysis",
        unready_rows=["- O3 (nano): missing orchestrator.json"],
    )
    batch = ops_policy.build_batch_finish_message(
        title="fanout finished",
        executed=2,
        reason="done",
        counters={"paused": 1, "unready": 0, "empty": 3, "busy": 0, "pending": 1, "missing_alias": 0},
        next_lines=["- /queue", "- /fanout"],
    )

    assert "locked project O4 Local_Map_Analysis has no runnable todo" in no_next
    assert "unready:" in no_next
    assert "- /next force" in no_next
    assert "fanout finished" in batch
    assert "- executed: 2" in batch
    assert "- skipped_paused: 1" in batch
    assert "- skipped_empty: 3" in batch
    assert "- reason: done" in batch


def test_ops_view_renders_snapshot_and_compact_scope_lines() -> None:
    entry = {
        "project_alias": "O4",
        "display_name": "Local_Map",
        "todos": [
            {"id": "TODO-1", "summary": "blocked item", "priority": "P1", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
            {"id": "TODO-2", "summary": "open item", "priority": "P2", "status": "open"},
        ],
        "pending_todo": {"todo_id": "TODO-2"},
        "last_sync_at": "2026-03-06T11:00:00+0900",
        "last_sync_mode": "scenario",
        "tasks": {
            "REQ-1": {
                "short_id": "T-101",
                "prompt": "Review Local Map backlog and summarize",
                "status": "running",
                "updated_at": "2026-03-06T12:00:00+0900",
            }
        },
    }
    projects = {"local_map": entry}

    snapshot = ops_view.render_project_snapshot_lines(key="local_map", entry=entry, locked=True)
    compact = ops_view.render_ops_scope_compact_lines(projects, detail_level="long")

    assert snapshot[0] == "project snapshot"
    assert "- project: O4 Local_Map [locked]" in snapshot
    assert "- todo: open=1 running=0 blocked=1 followup=1 pending=yes" in snapshot
    assert any("blocked_head: TODO-1 x2 [manual_followup]" in line for line in snapshot)
    assert compact
    assert compact[0].startswith("- O4 Local_Map: open=1 running=0 blocked=1 followup=1")
    assert any("next: P2 TODO-2 | open item" in line for line in compact)


def test_emit_planning_progress_logs_and_sends_chat_message() -> None:
    sent: list[tuple[str, dict]] = []
    logged: list[dict] = []

    def _send(body: str, **kwargs) -> bool:
        sent.append((body, kwargs))
        return True

    def _log_event(**kwargs) -> None:
        logged.append(kwargs)

    run_handlers._emit_planning_progress(
        phase="repair",
        key="local_map_analysis",
        send=_send,
        log_event=_log_event,
        emit_chat=True,
        detail="critic issues found; auto-replanning",
        attempt=1,
        total=2,
    )

    assert logged[-1]["event"] == "planning_repair"
    assert logged[-1]["status"] == "running"
    assert "attempt=1/2" in logged[-1]["detail"]
    assert sent
    assert "planning: auto-replan" in sent[-1][0]
    assert "- orch: local_map_analysis" in sent[-1][0]
    assert "- progress: 1/2" in sent[-1][0]
    assert sent[-1][1]["context"] == "planning-progress"


def test_compute_dispatch_plan_reports_progress_sequence() -> None:
    args = argparse.Namespace(
        task_planning=True,
        dry_run=False,
        plan_max_subtasks=4,
        plan_auto_replan=True,
        plan_replan_attempts=1,
        plan_block_on_critic=True,
    )
    phases: list[dict] = []
    critic_call_count = {"n": 0}

    def _build(*_args, **_kwargs):
        return {
            "summary": "plan",
            "subtasks": [{"id": "S1", "title": "build", "goal": "build", "owner_role": "DataEngineer", "acceptance": ["ok"]}],
        }

    def _critic(*_args, **_kwargs):
        critic_call_count["n"] += 1
        if critic_call_count["n"] == 1:
            return {"approved": False, "issues": ["fix this"], "recommendations": ["repair"]}
        return {"approved": True, "issues": [], "recommendations": []}

    def _repair(*_args, **_kwargs):
        return {
            "summary": "plan-fixed",
            "subtasks": [{"id": "S1", "title": "fixed", "goal": "fixed", "owner_role": "DataEngineer", "acceptance": ["ok"]}],
        }

    meta = run_handlers._compute_dispatch_plan(
        args=args,
        p_args=argparse.Namespace(),
        prompt="build something",
        dispatch_mode=True,
        run_control_mode="normal",
        run_source_task=None,
        selected_roles=[],
        available_roles=["DataEngineer", "Reviewer"],
        available_worker_roles=lambda roles: roles,
        normalize_task_plan_payload=lambda parsed, **_kwargs: parsed,
        build_task_execution_plan=_build,
        critique_task_execution_plan=_critic,
        critic_has_blockers=lambda critic: (not bool(critic.get("approved", True))) or bool(critic.get("issues") or []),
        repair_task_execution_plan=_repair,
        plan_roles_from_subtasks=lambda plan: ["DataEngineer"] if isinstance(plan, dict) else [],
        report_progress=lambda **kwargs: phases.append(kwargs),
    )

    assert meta.plan_gate_blocked is False
    assert [row["phase"] for row in phases] == ["planner", "critic", "repair", "critic", "ready"]
    assert phases[2]["attempt"] == 1
    assert phases[2]["total"] == 1


def test_plan_pipeline_module_matches_run_planning_exports() -> None:
    prompt = "각 프로젝트별로 5시간 내로 가장 늦게 생성된 10개 md를 살펴보고 업데이트 부탁해"
    assert run_handlers._apply_success_first_prompt_fallbacks(prompt) == plan_pipeline.apply_success_first_prompt_fallbacks(prompt)

    def _choose_roles(user_prompt: str, **_kwargs):
        if "analyze" in user_prompt.lower():
            return ["Analyst", "Reviewer"]
        return ["Reviewer"]

    run_mode = run_handlers._resolve_dispatch_mode_and_roles(
        run_force_mode=None,
        run_roles_override="",
        project_roles_csv="",
        auto_dispatch_enabled=True,
        prompt="analyze this change",
        choose_auto_dispatch_roles=_choose_roles,
        available_roles=["Analyst", "Reviewer"],
        team_dir=ROOT,
    )
    module_mode = plan_pipeline.resolve_dispatch_mode_and_roles(
        run_force_mode=None,
        run_roles_override="",
        project_roles_csv="",
        auto_dispatch_enabled=True,
        prompt="analyze this change",
        choose_auto_dispatch_roles=_choose_roles,
        available_roles=["Analyst", "Reviewer"],
        team_dir=ROOT,
    )
    assert run_mode == module_mode

    run_sent: list[tuple[str, dict]] = []
    module_sent: list[tuple[str, dict]] = []
    run_logged: list[dict] = []
    module_logged: list[dict] = []

    run_handlers._emit_planning_progress(
        phase="repair",
        key="local_map_analysis",
        send=lambda body, **kwargs: run_sent.append((body, kwargs)) or True,
        log_event=lambda **kwargs: run_logged.append(kwargs),
        emit_chat=True,
        detail="critic issues found; auto-replanning",
        attempt=1,
        total=2,
    )
    plan_pipeline.emit_planning_progress(
        phase="repair",
        key="local_map_analysis",
        send=lambda body, **kwargs: module_sent.append((body, kwargs)) or True,
        log_event=lambda **kwargs: module_logged.append(kwargs),
        emit_chat=True,
        detail="critic issues found; auto-replanning",
        attempt=1,
        total=2,
    )

    assert run_logged == module_logged
    assert run_sent == module_sent


def test_plan_pipeline_module_matches_run_compute_and_lineage_helpers() -> None:
    args = argparse.Namespace(
        task_planning=True,
        dry_run=False,
        plan_max_subtasks=4,
        plan_auto_replan=True,
        plan_replan_attempts=1,
        plan_block_on_critic=True,
    )
    critic_call_count = {"n": 0}

    def _build(*_args, **_kwargs):
        return {
            "summary": "plan",
            "subtasks": [{"id": "S1", "title": "build", "goal": "build", "owner_role": "DataEngineer", "acceptance": ["ok"]}],
        }

    def _critic(*_args, **_kwargs):
        critic_call_count["n"] += 1
        if critic_call_count["n"] == 1:
            return {"approved": False, "issues": ["fix this"], "recommendations": ["repair"]}
        return {"approved": True, "issues": [], "recommendations": []}

    def _repair(*_args, **_kwargs):
        return {
            "summary": "plan-fixed",
            "subtasks": [{"id": "S1", "title": "fixed", "goal": "fixed", "owner_role": "DataEngineer", "acceptance": ["ok"]}],
        }

    run_phases: list[dict] = []
    module_phases: list[dict] = []
    critic_has_blockers = lambda critic: (not bool(critic.get("approved", True))) or bool(critic.get("issues") or [])

    run_meta = run_handlers._compute_dispatch_plan(
        args=args,
        p_args=argparse.Namespace(),
        prompt="build something",
        dispatch_mode=True,
        run_control_mode="normal",
        run_source_task=None,
        selected_roles=[],
        available_roles=["DataEngineer", "Reviewer"],
        available_worker_roles=lambda roles: roles,
        normalize_task_plan_payload=lambda parsed, **_kwargs: parsed,
        build_task_execution_plan=_build,
        critique_task_execution_plan=_critic,
        critic_has_blockers=critic_has_blockers,
        repair_task_execution_plan=_repair,
        plan_roles_from_subtasks=lambda plan: ["DataEngineer"] if isinstance(plan, dict) else [],
        report_progress=lambda **kwargs: run_phases.append(kwargs),
    )

    critic_call_count["n"] = 0
    module_meta = plan_pipeline.compute_dispatch_plan(
        args=args,
        p_args=argparse.Namespace(),
        prompt="build something",
        dispatch_mode=True,
        run_control_mode="normal",
        run_source_task=None,
        selected_roles=[],
        available_roles=["DataEngineer", "Reviewer"],
        available_worker_roles=lambda roles: roles,
        normalize_task_plan_payload=lambda parsed, **_kwargs: parsed,
        build_task_execution_plan=_build,
        critique_task_execution_plan=_critic,
        critic_has_blockers=critic_has_blockers,
        repair_task_execution_plan=_repair,
        plan_roles_from_subtasks=lambda plan: ["DataEngineer"] if isinstance(plan, dict) else [],
        report_progress=lambda **kwargs: module_phases.append(kwargs),
    )

    assert run_meta == module_meta
    assert run_phases == module_phases

    task_a = {"request_id": "REQ-001", "context": {}}
    task_b = {"request_id": "REQ-001", "context": {}}
    source_a = {"request_id": "REQ-000", "context": {}}
    source_b = {"request_id": "REQ-000", "context": {}}
    notes_a: list[tuple[tuple, dict]] = []
    notes_b: list[tuple[tuple, dict]] = []

    kwargs = dict(
        task=task_a,
        plan_data={"subtasks": [{"id": "S1"}]},
        plan_critic={"approved": True, "issues": [], "recommendations": []},
        plan_roles=["DataEngineer"],
        plan_replans=[{"attempt": 1, "critic": "approved", "subtasks": 1}],
        plan_error="",
        critic_has_blockers=critic_has_blockers,
        lifecycle_set_stage=lambda *args, **kwargs: notes_a.append((args, kwargs)),
        run_control_mode="retry",
        run_source_request_id="REQ-000",
        run_source_task=source_a,
        req_id="REQ-001",
        now_iso=lambda: "2026-03-11T10:00:00+09:00",
    )
    run_handlers._apply_plan_and_lineage(**kwargs)

    kwargs["task"] = task_b
    kwargs["run_source_task"] = source_b
    kwargs["lifecycle_set_stage"] = lambda *args, **kwargs: notes_b.append((args, kwargs))
    plan_pipeline.apply_plan_and_lineage(**kwargs)

    assert task_a == task_b
    assert source_a == source_b
    assert notes_a == notes_b


def test_save_manager_state_syncs_investigations_registry_files(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project = state["projects"]["default"]
    project["project_alias"] = "O7"
    project["last_request_id"] = "REQ-B"
    project["tasks"] = {
        "REQ-A": {
            "short_id": "T-ALPHA",
            "prompt": "Build baseline",
            "status": "completed",
            "created_at": "2026-02-26T10:00:00+00:00",
            "updated_at": "2026-02-26T10:10:00+00:00",
            "control_mode": "project-orch",
        },
        "REQ-B": {
            "short_id": "T-BETA",
            "prompt": "Validate result",
            "status": "running",
            "source_request_id": "REQ-A",
            "created_at": "2026-02-26T11:00:00+00:00",
            "updated_at": "2026-02-26T11:05:00+00:00",
            "control_mode": "mother-orch",
        },
    }
    state_path = tmp_path / ".aoe-team" / "orch_manager_state.json"
    gw.save_manager_state(state_path, state)

    registry_root = tmp_path / "docs" / "investigations_mo" / "registry"
    project_lock = (registry_root / "project_lock.yaml").read_text(encoding="utf-8")
    tf_registry = (registry_root / "tf_registry.md").read_text(encoding="utf-8")
    handoff_index = (registry_root / "handoff_index.csv").read_text(encoding="utf-8")

    assert "active_project: O7" in project_lock
    assert "active_tf: TF-BETA" in project_lock
    assert "| TF-BETA | O7 | Validate result | running | - | mother-orch |" in tf_registry
    assert "H-O7-TF-BETA-REQB,O7,TF-ALPHA,TF-BETA,REQ-B" in handoff_index


def test_save_manager_state_syncs_registry_for_empty_tasks(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_path = tmp_path / ".aoe-team" / "orch_manager_state.json"
    gw.save_manager_state(state_path, state)

    registry_root = tmp_path / "docs" / "investigations_mo" / "registry"
    project_lock = (registry_root / "project_lock.yaml").read_text(encoding="utf-8")
    tf_registry = (registry_root / "tf_registry.md").read_text(encoding="utf-8")
    handoff_index = (registry_root / "handoff_index.csv").read_text(encoding="utf-8")

    assert "active_project: O1" in project_lock
    assert "active_tf: TF-ACTIVE" in project_lock
    # Default doc mode is "single": global TF registry uses a single report doc column.
    assert "| - | - | - | - | - | - | - | - | - |" in tf_registry
    assert handoff_index.strip() == "handoff_id,project_alias,from_tf,to_tf,task_id,created_at,doc,status"


def test_load_manager_state_preserves_todo_proposals_and_lineage_fields(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    state_path = team_dir / "orch_manager_state.json"
    team_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "active": "default",
        "projects": {
            "default": {
                "name": "default",
                "display_name": "default",
                "project_alias": "O1",
                "project_root": str(tmp_path),
                "team_dir": str(team_dir),
                "tasks": {},
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "follow up export",
                        "priority": "P2",
                        "status": "open",
                        "proposal_id": "PROP-001",
                        "proposal_kind": "followup",
                        "created_from_request_id": "REQ-123",
                        "created_from_todo_id": "TODO-000",
                    }
                ],
                "todo_seq": 1,
                "todo_proposals": [
                    {
                        "id": "PROP-001",
                        "summary": "follow up export",
                        "priority": "P2",
                        "kind": "followup",
                        "status": "open",
                        "reason": "result left one manual export step",
                        "confidence": 0.8,
                        "source_request_id": "REQ-123",
                        "source_todo_id": "TODO-000",
                        "source_task_label": "T-123",
                    }
                ],
                "todo_proposal_seq": 1,
            }
        },
    }
    state_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    loaded = gw.load_manager_state(state_path, tmp_path, team_dir)
    loaded_runtime = runtime_core.load_manager_state(
        state_path,
        tmp_path,
        team_dir,
        default_manager_state=gw.default_manager_state,
        now_iso=gw.now_iso,
        normalize_project_name=gw.normalize_project_name,
        sanitize_task_record=gw.sanitize_task_record,
        trim_project_tasks=gw.trim_project_tasks,
        normalize_task_alias_key=gw.normalize_task_alias_key,
        bool_from_json=gw.bool_from_json,
        normalize_project_alias=gw.normalize_project_alias,
        backfill_task_aliases=gw.backfill_task_aliases,
        ensure_project_aliases=gw.ensure_project_aliases,
        sanitize_project_lock_row=gw.sanitize_project_lock_row,
        sanitize_chat_session_row=gw.sanitize_chat_session_row,
    )
    project = loaded["projects"]["default"]

    assert loaded_runtime == loaded
    assert project["todo_proposal_seq"] == 1
    assert project["todo_proposals"][0]["source_request_id"] == "REQ-123"
    assert project["todo_proposals"][0]["confidence"] == 0.8
    assert project["todos"][0]["proposal_id"] == "PROP-001"
    assert project["todos"][0]["created_from_request_id"] == "REQ-123"


def test_extract_followup_todo_proposals_normalizes_json_payload() -> None:
    def _fake_run_codex_exec(args, prompt, timeout_sec=0):
        return json.dumps(
            {
                "proposals": [
                    {
                        "summary": "prepare deployment checklist",
                        "priority": "P1",
                        "kind": "handoff",
                        "reason": "release notes mention it is missing",
                        "confidence": 0.88,
                    },
                    {
                        "summary": "prepare deployment checklist",
                        "priority": "P3",
                        "kind": "note",
                        "reason": "duplicate",
                        "confidence": 2.0,
                    },
                ]
            },
            ensure_ascii=False,
        )

    original = gw.run_codex_exec
    gw.run_codex_exec = _fake_run_codex_exec
    try:
        rows = gw.extract_followup_todo_proposals(
            argparse.Namespace(orch_command_timeout_sec=120),
            "run release prep",
            {"replies": [{"role": "Codex-Writer", "body": "release note draft is done; deployment checklist is still missing"}]},
            task={"todo_id": "TODO-001", "plan": {"summary": "release prep"}},
            reply_lang="en",
        )
    finally:
        gw.run_codex_exec = original

    assert len(rows) == 1
    assert rows[0]["summary"] == "prepare deployment checklist"
    assert rows[0]["priority"] == "P1"
    assert rows[0]["kind"] == "handoff"
    assert rows[0]["confidence"] == 0.88


def test_orch_responses_module_matches_gateway_wrappers() -> None:
    def _fake_run_codex_exec(args, prompt, timeout_sec=0):
        if "proposals" in prompt:
            return json.dumps(
                {
                    "proposals": [
                        {
                            "summary": "prepare deployment checklist",
                            "priority": "P1",
                            "kind": "handoff",
                            "reason": "release notes mention it is missing",
                            "confidence": 0.88,
                        }
                    ]
                },
                ensure_ascii=False,
            )
        if "\"verdict\"" in prompt or "execution critic" in prompt or "execution critic이다" in prompt:
            return json.dumps(
                {
                    "verdict": "retry",
                    "action": "replan",
                    "reason": "missing validation",
                    "fix": "add verifier pass",
                },
                ensure_ascii=False,
            )
        return "ok"

    args = argparse.Namespace(orch_command_timeout_sec=120)
    state = {"replies": [{"role": "Reviewer", "body": "need one more validation step"}]}
    task = {"todo_id": "TODO-001", "plan": {"summary": "release prep", "subtasks": [{"title": "draft"}]}}

    original = gw.run_codex_exec
    gw.run_codex_exec = _fake_run_codex_exec
    try:
        assert gw.run_orchestrator_direct(args, "hello", reply_lang="ko") == orch_responses.run_orchestrator_direct(
            args,
            "hello",
            reply_lang="ko",
            default_reply_lang=gw.DEFAULT_REPLY_LANG,
            normalize_chat_lang_token=gw.normalize_chat_lang_token,
            run_codex_exec=_fake_run_codex_exec,
        )
        assert gw.synthesize_orchestrator_response(args, "hello", state, reply_lang="ko") == orch_responses.synthesize_orchestrator_response(
            args,
            "hello",
            state,
            reply_lang="ko",
            default_reply_lang=gw.DEFAULT_REPLY_LANG,
            normalize_chat_lang_token=gw.normalize_chat_lang_token,
            run_codex_exec=_fake_run_codex_exec,
        )
        assert gw.critique_task_execution_result(
            args,
            "hello",
            state,
            task=task,
            attempt_no=1,
            max_attempts=3,
            reply_lang="ko",
        ) == orch_responses.critique_task_execution_result(
            args,
            "hello",
            state,
            task=task,
            attempt_no=1,
            max_attempts=3,
            reply_lang="ko",
            default_reply_lang=gw.DEFAULT_REPLY_LANG,
            normalize_chat_lang_token=gw.normalize_chat_lang_token,
            mask_sensitive_text=gw.mask_sensitive_text,
            run_codex_exec=_fake_run_codex_exec,
            parse_json_object_from_text=gw.parse_json_object_from_text,
            normalize_exec_critic_payload=gw.normalize_exec_critic_payload,
            now_iso=gw.now_iso,
        )
        assert gw.extract_followup_todo_proposals(
            args,
            "run release prep",
            state,
            task=task,
            reply_lang="ko",
        ) == orch_responses.extract_followup_todo_proposals(
            args,
            "run release prep",
            state,
            task=task,
            reply_lang="ko",
            default_reply_lang=gw.DEFAULT_REPLY_LANG,
            default_orch_command_timeout_sec=gw.DEFAULT_ORCH_COMMAND_TIMEOUT_SEC,
            normalize_chat_lang_token=gw.normalize_chat_lang_token,
            mask_sensitive_text=gw.mask_sensitive_text,
            run_codex_exec=_fake_run_codex_exec,
            parse_json_object_from_text=gw.parse_json_object_from_text,
        )
    finally:
        gw.run_codex_exec = original


def test_ensure_tf_exec_workspace_records_project_envelope(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_TF_EXEC_MODE", "inplace")

    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)

    args = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        _aoe_project_key="demo_proj",
        _aoe_project_alias="O9",
        _aoe_control_mode="retry",
        _aoe_source_request_id="REQ-000",
    )

    meta = gw.ensure_tf_exec_workspace(args, "REQ-001")
    tf_map = gw.load_tf_exec_map(team_dir)

    assert meta["project_key"] == "demo_proj"
    assert meta["project_alias"] == "O9"
    assert meta["project_root"] == str(project_root)
    assert meta["team_dir"] == str(team_dir)
    assert meta["control_mode"] == "retry"
    assert meta["source_request_id"] == "REQ-000"
    assert meta["tf_id"].startswith("TF-REQ-")
    assert tf_map["REQ-001"]["project_key"] == "demo_proj"
    assert tf_map["REQ-001"]["project_alias"] == "O9"


def test_sync_task_lifecycle_attaches_exec_context_and_updates_tf_exec_map(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)

    req_id = "REQ-CTX"
    run_dir = team_dir / "tf_runs" / req_id
    workdir = tmp_path / "work_ctx"
    run_dir.mkdir(parents=True, exist_ok=True)
    workdir.mkdir(parents=True, exist_ok=True)
    _write_tf_exec_map(team_dir, req_id, mode="inplace", workdir=workdir, run_dir=run_dir)

    entry = {
        "name": "demo_proj",
        "project_alias": "O9",
        "project_root": str(tmp_path),
        "team_dir": str(team_dir),
        "tasks": {},
        "task_alias_index": {},
        "task_seq": 0,
    }
    request_data = {
        "request_id": req_id,
        "role_states": [{"role": "Reviewer", "status": "done"}],
        "counts": {"assignments": 1, "replies": 1},
        "complete": True,
    }

    task = gw.sync_task_lifecycle(
        entry=entry,
        request_data=request_data,
        prompt="Validate output",
        mode="dispatch",
        selected_roles=["Reviewer"],
        verifier_roles=[],
        require_verifier=False,
        verifier_candidates=["Reviewer"],
    )
    assert isinstance(task, dict)

    context = task.get("context") or {}
    assert context["project_key"] == "demo_proj"
    assert context["project_alias"] == "O9"
    assert context["project_root"] == str(tmp_path)
    assert context["team_dir"] == str(team_dir)
    assert context["workdir"] == str(workdir)
    assert context["run_dir"] == str(run_dir)
    assert context["task_short_id"] == task["short_id"]
    assert context["tf_id"] == gw.task_short_to_tf_id(task["short_id"])

    tf_map = gw.load_tf_exec_map(team_dir)
    row = tf_map[req_id]
    assert row["project_key"] == "demo_proj"
    assert row["project_alias"] == "O9"
    assert row["task_short_id"] == task["short_id"]
    assert row["task_alias"] == task["alias"]
    assert row["tf_id"] == gw.task_short_to_tf_id(task["short_id"])


def test_sanitize_task_record_preserves_context_and_lineage_fields() -> None:
    task = gw.sanitize_task_record(
        {
            "short_id": "t-007",
            "alias": "demo-task",
            "control_mode": "retry",
            "source_request_id": "REQ-000",
            "plan": {"summary": "do work"},
            "exec_critic": {"verdict": "retry"},
            "context": {
                "project_key": "demo_proj",
                "project_alias": "o9",
                "project_root": "/tmp/project",
                "team_dir": "/tmp/project/.aoe-team",
                "workdir": "/tmp/project/work",
                "run_dir": "/tmp/project/.aoe-team/tf_runs/REQ-007",
            },
        },
        "REQ-007",
    )

    assert task["control_mode"] == "retry"
    assert task["source_request_id"] == "REQ-000"
    assert task["plan"]["summary"] == "do work"
    assert task["exec_critic"]["verdict"] == "retry"
    assert task["context"]["project_key"] == "demo_proj"
    assert task["context"]["project_alias"] == "O9"
    assert task["context"]["task_short_id"] == "T-007"
    assert task["context"]["tf_id"] == "TF-007"
    assert task["tf_phase"] == "needs_retry"
    assert task["tf_phase_reason"] == "critic_parse_error"


def test_schema_normalizes_plan_and_exec_critic_payloads() -> None:
    plan = schema.normalize_task_plan_payload(
        {
            "summary": "demo",
            "subtasks": [
                {"title": "collect data", "role": "Codex-Analyst", "acceptance": ["done"]},
                {"id": "S2", "goal": "write memo", "owner_role": "UnknownRole"},
            ],
        },
        user_prompt="analyze and summarize",
        workers=["Codex-Analyst", "Codex-Writer"],
        max_subtasks=2,
    )
    critic = schema.normalize_plan_critic_payload(
        {"approved": False, "issues": ["missing acceptance"], "recommendations": ["tighten output contract"]},
        max_items=5,
    )
    exec_critic = schema.normalize_exec_critic_payload(
        {"verdict": "재시도", "action": "", "reason": "증거 부족", "fix": "evidence 추가"},
        attempt_no=2,
        max_attempts=3,
        at="2026-03-10T10:00:00+0900",
    )

    assert plan["summary"] == "demo"
    assert plan["subtasks"][0]["owner_role"] == "Codex-Analyst"
    assert plan["subtasks"][1]["owner_role"] == "UnknownRole"
    assert critic["approved"] is False
    assert critic["issues"] == ["missing acceptance"]
    assert exec_critic["verdict"] == "retry"
    assert exec_critic["action"] == "retry"
    assert exec_critic["reason"] == "증거 부족"


def test_sanitize_task_record_normalizes_nested_schema_fields() -> None:
    task = gw.sanitize_task_record(
        {
            "prompt": "do work",
            "plan": {
                "summary": " messy ",
                "subtasks": [{"goal": "collect data", "role": "Reviewer"}],
            },
            "plan_critic": {"approved": False, "issues": ["  missing acceptance  "], "recommendations": [" add checks "]},
            "plan_replans": [{"attempt": "2", "critic": "bad", "subtasks": "3"}],
            "plan_gate_passed": False,
            "exec_critic": {"verdict": "ok", "action": "retry", "reason": " all good ", "attempt": "2", "max_attempts": "4"},
        },
        "REQ-009",
    )

    assert task["plan"]["summary"] == "messy"
    assert task["plan"]["subtasks"][0]["owner_role"] == "Reviewer"
    assert task["plan_critic"]["issues"] == ["missing acceptance"]
    assert task["plan_replans"] == [{"attempt": 2, "critic": "unknown", "subtasks": 3}]
    assert task["plan_gate_reason"] == "missing acceptance"
    assert task["exec_critic"]["verdict"] == "success"
    assert task["exec_critic"]["action"] == "none"
    assert task["tf_phase"] == "blocked"
    assert task["tf_phase_reason"] == "missing acceptance"


def test_plan_critic_primary_issue_and_lifecycle_summary_use_schema_reason() -> None:
    issue = schema.plan_critic_primary_issue({"approved": False, "issues": ["  missing acceptance criteria  "]})
    assert issue == "missing acceptance criteria"

    summary = gw.summarize_task_lifecycle(
        "Demo",
        {
            "request_id": "REQ-101",
            "status": "failed",
            "mode": "dispatch",
            "roles": ["Codex-Dev"],
            "verifier_roles": ["Reviewer"],
            "stages": {"planning": "failed"},
            "plan": {
                "summary": "demo plan",
                "subtasks": [{"id": "S1", "title": "collect data", "owner_role": "Codex-Dev"}],
                "meta": {
                    "phase2_team_spec": {
                        "execution_mode": "single",
                        "execution_groups": [
                            {"group_id": "E1", "role": "Codex-Dev", "subtask_ids": ["S1"], "subtask_titles": ["collect data"], "goals": ["collect data"]}
                        ],
                        "review_mode": "single",
                        "review_groups": [
                            {"group_id": "R1", "role": "Reviewer", "kind": "verifier", "scope": "phase2_outputs", "depends_on": ["E1"]}
                        ],
                        "team_roles": ["Codex-Dev", "Reviewer"],
                        "critic_role": "Reviewer",
                        "integration_role": "Reviewer",
                    },
                    "phase2_execution_plan": {
                        "execution_mode": "single",
                        "execution_lanes": [
                            {"lane_id": "L1", "role": "Codex-Dev", "subtask_ids": ["S1"], "parallel": False}
                        ],
                        "review_mode": "single",
                        "review_lanes": [
                            {"lane_id": "R1", "role": "Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": False}
                        ],
                        "parallel_workers": False,
                        "parallel_reviews": False,
                        "readonly": True,
                    },
                },
            },
            "plan_critic": {"approved": False, "issues": ["missing acceptance criteria"]},
            "plan_gate_passed": False,
            "plan_gate_reason": issue,
            "exec_critic": {
                "verdict": "retry",
                "action": "replan",
                "reason": "need a stricter acceptance contract",
                "attempt": 2,
                "max_attempts": 3,
                "at": "2026-03-10T10:00:00+0900",
            },
        },
    )

    assert "plan_gate: blocked" in summary
    assert "plan_gate_reason: missing acceptance criteria" in summary
    assert "tf_phase: blocked" in summary
    assert "phase2_execution: single lanes=1" in summary
    assert "phase2_review: single lanes=1" in summary
    assert "phase2_exec_plan: single workers_parallel=no reviews_parallel=no readonly=yes" in summary
    assert "- exec L1 [Codex-Dev/serial]" in summary
    assert "-> S1" in summary
    assert "- critic R1 [Reviewer/verifier/serial]" in summary
    assert "after L1" in summary
    assert "exec_critic: retry (action=replan)" in summary
    assert "exec_attempts: 2/3" in summary
    assert "exec_reason: need a stricter acceptance contract" in summary


def test_blocked_state_helpers_clear_and_promote_manual_followup() -> None:
    item = {
        "status": "running",
        "blocked_count": 1,
        "blocked_bucket": "manual_followup",
        "blocked_reason": "needs operator decision",
        "blocked_alerted_at": "2026-03-10T09:00:00+0900",
        "current_request_id": "REQ-9",
    }

    outcome = blocked_state.apply_todo_execution_outcome(
        item,
        task_status="failed",
        exec_verdict="retry",
        exec_reason="missing evidence",
        req_id="REQ-10",
        now="2026-03-10T10:00:00+0900",
        task_label="T-010 demo",
        manual_followup_threshold=2,
    )

    assert outcome == "blocked"
    assert item["status"] == "blocked"
    assert item["blocked_count"] == 2
    assert item["blocked_bucket"] == "manual_followup"
    assert item["blocked_reason"] == "missing evidence"

    had_followup = blocked_state.clear_blocked_meta(item, clear_current_request=True)
    assert had_followup is True
    assert "blocked_bucket" not in item
    assert "blocked_reason" not in item
    assert "current_request_id" not in item


def test_apply_exec_critic_lifecycle_marks_retry_replan_as_planning_and_needs_retry() -> None:
    task = {
        "status": "running",
        "stages": {
            "intake": "done",
            "planning": "done",
            "staffing": "done",
            "execution": "done",
            "verification": "done",
            "integration": "running",
            "close": "running",
        },
    }

    task_state.apply_exec_critic_lifecycle(
        task,
        {"verdict": "retry", "action": "replan", "reason": "split the scope first"},
        lifecycle_set_stage=gw.lifecycle_set_stage,
    )

    assert task["stages"]["planning"] == "running"
    assert task["stages"]["integration"] == "running"
    assert task["tf_phase"] == "needs_retry"
    assert task["tf_phase_reason"] == "split the scope first"


def test_apply_exec_critic_lifecycle_marks_intervention_as_manual_intervention() -> None:
    task = {
        "status": "running",
        "stages": {
            "intake": "done",
            "planning": "done",
            "staffing": "done",
            "execution": "done",
            "verification": "done",
            "integration": "running",
            "close": "running",
        },
    }

    task_state.apply_exec_critic_lifecycle(
        task,
        {"verdict": "intervention", "action": "escalate", "reason": "operator decision required"},
        lifecycle_set_stage=gw.lifecycle_set_stage,
    )

    assert task["stages"]["integration"] == "failed"
    assert task["stages"]["close"] == "failed"
    assert task["tf_phase"] == "manual_intervention"
    assert task["tf_phase_reason"] == "operator decision required"


def test_apply_exec_critic_lifecycle_overlays_review_lane_verdicts() -> None:
    task = {
        "status": "running",
        "stages": {
            "intake": "done",
            "planning": "done",
            "staffing": "done",
            "execution": "done",
            "verification": "done",
            "integration": "running",
            "close": "running",
        },
        "plan": {"meta": {}},
        "lane_states": {
            "execution": [{"lane_id": "L1", "role": "Codex-Dev", "status": "done"}],
            "review": [{"lane_id": "R1", "role": "Reviewer", "kind": "verifier", "status": "done", "depends_on": ["L1"]}],
            "summary": {
                "execution": {"done": 1},
                "review": {"done": 1},
            },
        },
    }

    task_state.apply_exec_critic_lifecycle(
        task,
        {"verdict": "retry", "action": "replan", "reason": "review lane found missing acceptance"},
        lifecycle_set_stage=gw.lifecycle_set_stage,
    )

    review_row = task["lane_states"]["review"][0]
    assert review_row["status"] == "done"
    assert review_row["verdict"] == "retry"
    assert review_row["action"] == "replan"
    assert review_row["reason"] == "review lane found missing acceptance"
    assert task["lane_states"]["summary"]["review_verdicts"] == {"retry": 1}
    assert task["exec_critic"]["rerun_execution_lane_ids"] == ["L1"]
    assert task["exec_critic"]["rerun_review_lane_ids"] == ["R1"]
    summary = gw.summarize_task_lifecycle("Demo", task)
    assert "phase2_lane_state: exec done=1 | review done=1 | review_verdict retry=1" in summary
    assert "- critic R1 [Reviewer/verifier/serial] [done] -> retry/replan after L1" in summary
    assert "exec_rerun_targets: execution=L1 review=R1" in summary


def test_apply_exec_critic_lifecycle_marks_manual_followup_lane_targets() -> None:
    task = {
        "status": "running",
        "stages": {
            "intake": "done",
            "planning": "done",
            "staffing": "done",
            "execution": "done",
            "verification": "done",
            "integration": "running",
            "close": "running",
        },
        "plan": {"meta": {}},
        "lane_states": {
            "execution": [
                {"lane_id": "L1", "role": "Codex-Dev", "status": "done"},
                {"lane_id": "L2", "role": "Codex-Writer", "status": "done"},
            ],
            "review": [{"lane_id": "R1", "role": "Reviewer", "kind": "verifier", "status": "done", "depends_on": ["L1", "L2"]}],
            "summary": {
                "execution": {"done": 2},
                "review": {"done": 1},
            },
        },
    }

    task_state.apply_exec_critic_lifecycle(
        task,
        {"verdict": "intervention", "action": "escalate", "reason": "operator decision required"},
        lifecycle_set_stage=gw.lifecycle_set_stage,
    )

    assert task["exec_critic"]["manual_followup_execution_lane_ids"] == ["L1", "L2"]
    assert task["exec_critic"]["manual_followup_review_lane_ids"] == ["R1"]
    summary = gw.summarize_task_lifecycle("Demo", task)
    assert "exec_manual_followup_targets: execution=L1, L2 review=R1" in summary


def test_blocked_state_helpers_render_manual_followup_summary() -> None:
    rows = [
        {"id": "TODO-1", "status": "blocked", "blocked_bucket": "manual_followup", "blocked_reason": "need review", "blocked_count": 2, "updated_at": "2026-03-10T10:00:00+0900"},
        {"id": "TODO-2", "status": "open"},
        {"id": "TODO-3", "status": "blocked", "blocked_reason": "later", "blocked_count": 1, "updated_at": "2026-03-10T11:00:00+0900"},
    ]

    assert blocked_state.manual_followup_indices(rows, limit=3) == [1]
    assert blocked_state.blocked_bucket_count(rows, "manual_followup") == 1
    head = blocked_state.blocked_head_summary(rows)
    assert head["id"] == "TODO-1"
    assert head["bucket"] == "manual_followup"
    assert head["reason"] == "need review"


def test_task_view_module_matches_gateway_lifecycle_summary() -> None:
    task = {
        "request_id": "REQ-202",
        "short_id": "T-202",
        "alias": "demo-task",
        "status": "running",
        "mode": "dispatch",
        "roles": ["Codex-Dev", "Reviewer"],
        "verifier_roles": ["Reviewer"],
        "stages": {"planning": "done", "execution": "running"},
        "context": {
            "project_key": "demo_proj",
            "project_alias": "O9",
            "task_short_id": "T-202",
            "source_request_id": "REQ-101",
            "control_mode": "retry",
        },
        "plan": {"summary": "demo", "subtasks": [{"id": "S1", "title": "collect", "owner_role": "Codex-Dev"}]},
        "plan_critic": {"approved": False, "issues": ["missing acceptance"]},
        "plan_gate_passed": False,
        "plan_gate_reason": "missing acceptance",
        "exec_critic": {"verdict": "retry", "action": "replan", "reason": "need evidence", "attempt": 1, "max_attempts": 3, "at": "2026-03-10T10:00:00+0900"},
        "result": {"assignments": 1, "replies": 0, "complete": False, "pending_roles": ["Codex-Dev"]},
        "history": [{"at": "2026-03-10T10:00:00+0900", "stage": "planning", "status": "done", "note": "critic issues"}],
    }

    expected = gw.summarize_task_lifecycle("Demo", task)
    actual = task_view.summarize_task_lifecycle("Demo", task)
    assert actual == expected


def test_task_state_module_matches_gateway_alias_and_monitor_helpers() -> None:
    entry = {
        "tasks": {
            "REQ-1": {
                "request_id": "REQ-1",
                "prompt": "collect data and write memo",
                "status": "running",
                "stage": "execution",
                "roles": ["Codex-Dev", "Reviewer"],
                "plan": {
                    "meta": {
                        "phase2_execution_plan": {
                            "execution_mode": "single",
                            "execution_lanes": [{"lane_id": "L1", "role": "Codex-Dev"}],
                            "review_mode": "single",
                            "review_lanes": [{"lane_id": "R1", "role": "Reviewer"}],
                        }
                    }
                },
                "updated_at": "2026-03-10T10:00:00+0900",
                "created_at": "2026-03-10T09:00:00+0900",
            }
        },
        "task_alias_index": {},
        "task_seq": 0,
    }

    task_state.backfill_task_aliases(entry)
    assert gw.resolve_task_request_id(entry, "T-001") == task_state.resolve_task_request_id(entry, "T-001")
    assert gw.resolve_task_request_id(entry, "collect-data-write-memo") == task_state.resolve_task_request_id(
        entry, "collect-data-write-memo"
    )

    gw_summary = gw.summarize_task_monitor("Demo", entry, limit=5)
    state_summary = task_state.summarize_task_monitor(
        "Demo",
        entry,
        limit=5,
        normalize_task_status=gw.normalize_task_status,
        dedupe_roles=gw.dedupe_roles,
        task_display_label=gw.task_display_label,
        lifecycle_stages=gw.LIFECYCLE_STAGES,
    )
    assert state_summary == gw_summary
    assert "lanes E1/R1" in gw_summary


def test_task_state_snapshot_and_sync_match_gateway() -> None:
    request_data = {
        "request_id": "REQ-301",
        "role_states": [
            {"role": "Codex-Dev", "status": "done"},
            {"role": "Reviewer", "status": "pending"},
        ],
        "counts": {"assignments": 2, "replies": 1},
        "complete": False,
    }
    assert task_state.extract_request_snapshot(request_data, dedupe_roles=gw.dedupe_roles) == gw.extract_request_snapshot(
        request_data
    )

    entry_a = {"name": "demo_proj", "project_alias": "O9", "project_root": "/tmp/demo", "team_dir": "/tmp/demo/.aoe-team", "tasks": {}, "task_alias_index": {}, "task_seq": 0}
    entry_b = copy.deepcopy(entry_a)

    task_a = gw.sync_task_lifecycle(
        entry=entry_a,
        request_data=request_data,
        prompt="Validate output",
        mode="dispatch",
        selected_roles=["Codex-Dev", "Reviewer"],
        verifier_roles=["Reviewer"],
        require_verifier=True,
        verifier_candidates=["Reviewer"],
    )
    task_b = task_state.sync_task_lifecycle(
        entry_b,
        request_data,
        prompt="Validate output",
        mode="dispatch",
        selected_roles=["Codex-Dev", "Reviewer"],
        verifier_roles=["Reviewer"],
        require_verifier=True,
        verifier_candidates=["Reviewer"],
        dedupe_roles=gw.dedupe_roles,
        ensure_task_record=gw.ensure_task_record,
        lifecycle_set_stage=gw.lifecycle_set_stage,
        normalize_task_status=gw.normalize_task_status,
        sync_task_exec_context=lambda entry, task: task.get("context", {}) if isinstance(task, dict) else {},
    )

    assert task_a is not None
    assert task_b is not None
    assert task_b["status"] == task_a["status"]
    assert task_b["roles"] == task_a["roles"]
    assert task_b["verifier_roles"] == task_a["verifier_roles"]
    assert task_b["result"] == task_a["result"]
    assert task_b["stages"] == task_a["stages"]


def test_task_state_sync_records_role_mismatch_and_task_summary_surfaces_it() -> None:
    request_data = {
        "request_id": "REQ-ROLE-1",
        "requested_roles": ["Codex-Writer", "Reviewer"],
        "executed_roles": ["Codex-Analyst", "Reviewer"],
        "role_states": [
            {"role": "Codex-Analyst", "status": "done"},
            {"role": "Reviewer", "status": "done"},
        ],
        "counts": {"assignments": 2, "replies": 2},
        "complete": True,
    }
    entry = {"name": "demo_proj", "project_alias": "O9", "project_root": "/tmp/demo", "team_dir": "/tmp/demo/.aoe-team", "tasks": {}, "task_alias_index": {}, "task_seq": 0}

    task = task_state.sync_task_lifecycle(
        entry,
        request_data,
        prompt="Write the handoff note",
        mode="dispatch",
        selected_roles=["Codex-Writer", "Reviewer"],
        verifier_roles=["Reviewer"],
        require_verifier=True,
        verifier_candidates=["Reviewer"],
        dedupe_roles=gw.dedupe_roles,
        ensure_task_record=gw.ensure_task_record,
        lifecycle_set_stage=gw.lifecycle_set_stage,
        normalize_task_status=gw.normalize_task_status,
        sync_task_exec_context=lambda entry, task: task.get("context", {}) if isinstance(task, dict) else {},
    )

    assert task is not None
    assert task["result"]["role_mismatch"] is True
    assert task["result"]["dropped_roles"] == ["Codex-Writer"]
    assert task["result"]["added_roles"] == ["Codex-Analyst"]

    summary = gw.summarize_task_lifecycle("Demo", task)
    assert "requested_roles: Codex-Writer, Reviewer" in summary
    assert "executed_roles: Codex-Analyst, Reviewer" in summary
    assert "role_mismatch: dropped=Codex-Writer added=Codex-Analyst" in summary


def test_task_monitor_surfaces_role_mismatch_targets() -> None:
    entry = {
        "tasks": {
            "REQ-ROLE-2": {
                "request_id": "REQ-ROLE-2",
                "prompt": "Write summary",
                "status": "running",
                "stage": "execution",
                "roles": ["Codex-Writer", "Reviewer"],
                "result": {
                    "requested_roles": ["Codex-Writer", "Reviewer"],
                    "executed_roles": ["Codex-Analyst", "Reviewer"],
                    "dropped_roles": ["Codex-Writer"],
                    "added_roles": ["Codex-Analyst"],
                    "role_mismatch": True,
                },
                "updated_at": "2026-03-13T10:00:00+0900",
                "created_at": "2026-03-13T09:00:00+0900",
            }
        },
        "task_alias_index": {},
        "task_seq": 0,
    }

    task_state.backfill_task_aliases(entry)
    summary = task_state.summarize_task_monitor(
        "Demo",
        entry,
        limit=5,
        normalize_task_status=gw.normalize_task_status,
        dedupe_roles=gw.dedupe_roles,
        task_display_label=gw.task_display_label,
        lifecycle_stages=gw.LIFECYCLE_STAGES,
    )

    assert "roles drop:Codex-Writer add:Codex-Analyst" in summary


def test_task_state_sync_derives_lane_states_and_review_waits_on_dependencies() -> None:
    request_data = {
        "request_id": "REQ-302",
        "role_states": [
            {"role": "Codex-Dev", "status": "done"},
            {"role": "Codex-Writer", "status": "running"},
            {"role": "Reviewer", "status": "pending"},
        ],
        "counts": {"assignments": 3, "replies": 1},
        "complete": False,
    }
    plan = {
        "summary": "parallel execution",
        "subtasks": [
            {"id": "S1", "title": "implement", "owner_role": "Codex-Dev"},
            {"id": "S2", "title": "write report", "owner_role": "Codex-Writer"},
        ],
        "meta": {
            "phase2_execution_plan": {
                "execution_mode": "parallel",
                "execution_lanes": [
                    {"lane_id": "L1", "role": "Codex-Dev", "subtask_ids": ["S1"], "parallel": True},
                    {"lane_id": "L2", "role": "Codex-Writer", "subtask_ids": ["S2"], "parallel": True},
                ],
                "review_mode": "single",
                "review_lanes": [
                    {"lane_id": "R1", "role": "Reviewer", "kind": "verifier", "depends_on": ["L1", "L2"], "parallel": False}
                ],
                "parallel_workers": True,
                "parallel_reviews": False,
                "readonly": True,
            }
        },
    }

    entry = {"name": "demo_proj", "project_alias": "O9", "project_root": "/tmp/demo", "team_dir": "/tmp/demo/.aoe-team", "tasks": {}, "task_alias_index": {}, "task_seq": 0}
    task = gw.ensure_task_record(
        entry=entry,
        request_id="REQ-302",
        prompt="Parallelize implementation and reporting",
        mode="dispatch",
        roles=["Codex-Dev", "Codex-Writer", "Reviewer"],
        verifier_roles=["Reviewer"],
        require_verifier=True,
    )
    task["plan"] = copy.deepcopy(plan)

    synced = task_state.sync_task_lifecycle(
        entry,
        request_data,
        prompt="Parallelize implementation and reporting",
        mode="dispatch",
        selected_roles=["Codex-Dev", "Codex-Writer", "Reviewer"],
        verifier_roles=["Reviewer"],
        require_verifier=True,
        verifier_candidates=["Reviewer"],
        dedupe_roles=gw.dedupe_roles,
        ensure_task_record=gw.ensure_task_record,
        lifecycle_set_stage=gw.lifecycle_set_stage,
        normalize_task_status=gw.normalize_task_status,
        sync_task_exec_context=lambda current_entry, current_task: current_task.get("context", {}) if isinstance(current_task, dict) else {},
    )

    lane_states = synced["lane_states"]
    assert lane_states["summary"]["execution"] == {"done": 1, "running": 1}
    assert lane_states["summary"]["review"] == {"waiting_on_dependencies": 1}
    assert lane_states["execution"][0]["lane_id"] == "L1"
    assert lane_states["execution"][0]["status"] == "done"
    assert lane_states["execution"][1]["lane_id"] == "L2"
    assert lane_states["execution"][1]["status"] == "running"
    assert lane_states["review"][0]["lane_id"] == "R1"
    assert lane_states["review"][0]["status"] == "waiting_on_dependencies"
    assert lane_states["review"][0]["waiting_on"] == ["L2"]

    summary = gw.summarize_task_lifecycle("Demo", synced)
    assert "phase2_lane_state: exec done=1, running=1 | review waiting_on_dependencies=1" in summary
    assert "- exec L1 [Codex-Dev/parallel] [done] -> S1" in summary
    assert "- exec L2 [Codex-Writer/parallel] [running] -> S2" in summary
    assert "- critic R1 [Reviewer/verifier/serial] [waiting_on_dependencies] after L1, L2" in summary


def test_task_state_sync_records_phase2_review_trigger_metadata() -> None:
    request_data = {
        "request_id": "REQ-303",
        "role_states": [
            {"role": "Codex-Dev", "status": "done"},
            {"role": "Reviewer", "status": "done"},
        ],
        "counts": {"assignments": 2, "replies": 2},
        "complete": True,
        "phase2_request_ids": {"execution": "REQ-EXEC", "review": "REQ-REVIEW"},
        "phase2_review_triggered": True,
    }
    plan = {
        "summary": "execution then review",
        "subtasks": [{"id": "S1", "title": "implement", "owner_role": "Codex-Dev"}],
        "meta": {
            "phase2_execution_plan": {
                "execution_mode": "single",
                "execution_lanes": [{"lane_id": "L1", "role": "Codex-Dev", "subtask_ids": ["S1"], "parallel": False}],
                "review_mode": "single",
                "review_lanes": [{"lane_id": "R1", "role": "Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": False}],
                "parallel_workers": False,
                "parallel_reviews": False,
                "readonly": True,
            }
        },
    }
    entry = {"name": "demo_proj", "project_alias": "O9", "project_root": "/tmp/demo", "team_dir": "/tmp/demo/.aoe-team", "tasks": {}, "task_alias_index": {}, "task_seq": 0}
    task = gw.ensure_task_record(
        entry=entry,
        request_id="REQ-303",
        prompt="Implement and verify",
        mode="dispatch",
        roles=["Codex-Dev", "Reviewer"],
        verifier_roles=["Reviewer"],
        require_verifier=True,
    )
    task["plan"] = copy.deepcopy(plan)

    synced = task_state.sync_task_lifecycle(
        entry,
        request_data,
        prompt="Implement and verify",
        mode="dispatch",
        selected_roles=["Codex-Dev", "Reviewer"],
        verifier_roles=["Reviewer"],
        require_verifier=True,
        verifier_candidates=["Reviewer"],
        dedupe_roles=gw.dedupe_roles,
        ensure_task_record=gw.ensure_task_record,
        lifecycle_set_stage=gw.lifecycle_set_stage,
        normalize_task_status=gw.normalize_task_status,
        sync_task_exec_context=lambda current_entry, current_task: current_task.get("context", {}) if isinstance(current_task, dict) else {},
    )

    assert synced["result"]["phase2_request_ids"] == {"execution": "REQ-EXEC", "review": "REQ-REVIEW"}
    assert synced["result"]["phase2_review_triggered"] is True
    summary = gw.summarize_task_lifecycle("Demo", synced)
    assert "phase2_requests: execution=REQ-EXEC review=REQ-REVIEW" in summary
    assert "phase2_review_triggered: yes" in summary


def test_task_state_sync_prefers_gateway_request_id_and_keeps_parallel_phase2_requests() -> None:
    request_data = {
        "request_id": "REQ-L1",
        "gateway_request_id": "REQ-TOP",
        "linked_request_ids": ["REQ-L1", "REQ-L2", "REQ-R1", "REQ-R2"],
        "role_states": [
            {"role": "Codex-Dev", "status": "done"},
            {"role": "Codex-Writer", "status": "done"},
            {"role": "Reviewer", "status": "done"},
            {"role": "Claude-Reviewer", "status": "done"},
        ],
        "counts": {"assignments": 4, "replies": 4},
        "complete": True,
        "phase2_request_ids": {
            "execution": ["REQ-L1", "REQ-L2"],
            "review": ["REQ-R1", "REQ-R2"],
        },
        "phase2_review_triggered": True,
    }
    entry = {"name": "demo_proj", "project_alias": "O9", "project_root": "/tmp/demo", "team_dir": "/tmp/demo/.aoe-team", "tasks": {}, "task_alias_index": {}, "task_seq": 0}
    synced = task_state.sync_task_lifecycle(
        entry,
        request_data,
        prompt="Implement and review in parallel",
        mode="dispatch",
        selected_roles=["Codex-Dev", "Codex-Writer", "Reviewer", "Claude-Reviewer"],
        verifier_roles=["Reviewer", "Claude-Reviewer"],
        require_verifier=True,
        verifier_candidates=["Reviewer", "Claude-Reviewer"],
        dedupe_roles=gw.dedupe_roles,
        ensure_task_record=gw.ensure_task_record,
        lifecycle_set_stage=gw.lifecycle_set_stage,
        normalize_task_status=gw.normalize_task_status,
        sync_task_exec_context=lambda current_entry, current_task: current_task.get("context", {}) if isinstance(current_task, dict) else {},
    )

    assert synced["request_id"] == "REQ-TOP"
    assert synced["result"]["linked_request_ids"] == ["REQ-L1", "REQ-L2", "REQ-R1", "REQ-R2"]
    assert synced["result"]["phase2_request_ids"] == {
        "execution": ["REQ-L1", "REQ-L2"],
        "review": ["REQ-R1", "REQ-R2"],
    }
    summary = gw.summarize_task_lifecycle("Demo", synced)
    assert "phase2_requests: execution=REQ-L1, REQ-L2 review=REQ-R1, REQ-R2" in summary


def test_derive_lane_states_prefers_lane_specific_status_for_same_role_rows() -> None:
    task = {
        "plan": {
            "meta": {
                "phase2_execution_plan": {
                    "execution_lanes": [
                        {"lane_id": "L1", "role": "Codex-Analyst"},
                        {"lane_id": "L2", "role": "Codex-Analyst"},
                    ],
                    "review_lanes": [
                        {"lane_id": "R1", "role": "Reviewer", "depends_on": ["L1"]},
                        {"lane_id": "R2", "role": "Reviewer", "depends_on": ["L2"]},
                    ],
                }
            }
        }
    }
    snapshot = {
        "rows": [
            {"role": "Codex-Analyst", "status": "done", "lane_id": "L1", "phase2_stage": "execution"},
            {"role": "Codex-Analyst", "status": "running", "lane_id": "L2", "phase2_stage": "execution"},
            {"role": "Reviewer", "status": "pending", "lane_id": "R1", "phase2_stage": "review"},
            {"role": "Reviewer", "status": "pending", "lane_id": "R2", "phase2_stage": "review"},
        ],
        "complete": False,
        "pending_roles": ["Codex-Analyst", "Reviewer"],
        "done_roles": [],
        "failed_roles": [],
    }

    lane_states = task_state.derive_lane_states(task, snapshot)
    assert lane_states["execution"] == [
        {"lane_id": "L1", "role": "Codex-Analyst", "status": "done", "parallel": True},
        {"lane_id": "L2", "role": "Codex-Analyst", "status": "running", "parallel": True},
    ]
    assert lane_states["review"][0]["status"] == "pending"
    assert lane_states["review"][1]["status"] == "waiting_on_dependencies"


def test_task_monitor_includes_lane_state_summary() -> None:
    entry = {
        "tasks": {
            "REQ-1": {
                "request_id": "REQ-1",
                "prompt": "collect data and write memo",
                "status": "running",
                "stage": "execution",
                "roles": ["Codex-Dev", "Reviewer"],
                "lane_states": {
                    "execution": [{"lane_id": "L1", "role": "Codex-Dev", "status": "running"}],
                    "review": [{"lane_id": "R1", "role": "Reviewer", "status": "waiting_on_dependencies", "depends_on": ["L1"]}],
                    "summary": {
                        "execution": {"running": 1},
                        "review": {"waiting_on_dependencies": 1},
                    },
                },
                "plan": {
                    "meta": {
                        "phase2_execution_plan": {
                            "execution_mode": "single",
                            "execution_lanes": [{"lane_id": "L1", "role": "Codex-Dev"}],
                            "review_mode": "single",
                            "review_lanes": [{"lane_id": "R1", "role": "Reviewer"}],
                        }
                    }
                },
                "updated_at": "2026-03-10T10:00:00+0900",
                "created_at": "2026-03-10T09:00:00+0900",
            }
        },
        "task_alias_index": {},
        "task_seq": 0,
    }

    summary = task_state.summarize_task_monitor(
        "Demo",
        entry,
        limit=5,
        normalize_task_status=gw.normalize_task_status,
        dedupe_roles=gw.dedupe_roles,
        task_display_label=gw.task_display_label,
        lifecycle_stages=gw.LIFECYCLE_STAGES,
    )
    assert "lanes E1/R1 [exec running=1 | review waiting_on_dependencies=1]" in summary


def test_task_monitor_includes_review_verdict_summary() -> None:
    entry = {
        "tasks": {
            "REQ-1": {
                "request_id": "REQ-1",
                "prompt": "collect data and write memo",
                "status": "running",
                "stage": "integration",
                "roles": ["Codex-Dev", "Reviewer"],
                "lane_states": {
                    "execution": [{"lane_id": "L1", "role": "Codex-Dev", "status": "done"}],
                    "review": [{"lane_id": "R1", "role": "Reviewer", "status": "done", "verdict": "retry", "action": "replan", "depends_on": ["L1"]}],
                    "summary": {
                        "execution": {"done": 1},
                        "review": {"done": 1},
                        "review_verdicts": {"retry": 1},
                    },
                },
                "plan": {
                    "meta": {
                        "phase2_execution_plan": {
                            "execution_mode": "single",
                            "execution_lanes": [{"lane_id": "L1", "role": "Codex-Dev"}],
                            "review_mode": "single",
                            "review_lanes": [{"lane_id": "R1", "role": "Reviewer"}],
                        }
                    }
                },
                "updated_at": "2026-03-10T10:00:00+0900",
                "created_at": "2026-03-10T09:00:00+0900",
            }
        },
        "task_alias_index": {},
        "task_seq": 0,
    }

    summary = task_state.summarize_task_monitor(
        "Demo",
        entry,
        limit=5,
        normalize_task_status=gw.normalize_task_status,
        dedupe_roles=gw.dedupe_roles,
        task_display_label=gw.task_display_label,
        lifecycle_stages=gw.LIFECYCLE_STAGES,
    )
    assert "lanes E1/R1 [exec done=1 | review done=1 | review_verdict retry=1]" in summary


def test_task_monitor_includes_lane_rerun_and_followup_targets() -> None:
    entry = {
        "tasks": {
            "REQ-1": {
                "request_id": "REQ-1",
                "prompt": "collect data and write memo",
                "status": "running",
                "stage": "integration",
                "roles": ["Codex-Dev", "Reviewer"],
                "exec_critic": {
                    "verdict": "retry",
                    "action": "retry",
                    "rerun_execution_lane_ids": ["L2"],
                    "rerun_review_lane_ids": ["R1"],
                    "manual_followup_execution_lane_ids": ["L3"],
                },
                "result": {
                    "phase2_request_ids": {
                        "execution": ["REQ-L1", "REQ-L2"],
                        "review": ["REQ-R1"],
                    },
                    "linked_request_ids": ["REQ-L1", "REQ-L2", "REQ-R1"],
                    "phase2_parallelized": True,
                },
                "lane_states": {
                    "execution": [{"lane_id": "L1", "role": "Codex-Dev", "status": "done"}],
                    "review": [{"lane_id": "R1", "role": "Reviewer", "status": "done", "verdict": "retry", "action": "replan", "depends_on": ["L2"]}],
                    "summary": {
                        "execution": {"done": 1},
                        "review": {"done": 1},
                        "review_verdicts": {"retry": 1},
                    },
                },
                "plan": {
                    "meta": {
                        "phase2_execution_plan": {
                            "execution_mode": "parallel",
                            "execution_lanes": [{"lane_id": "L1", "role": "Codex-Dev"}, {"lane_id": "L2", "role": "Claude-Analyst"}],
                            "review_mode": "single",
                            "review_lanes": [{"lane_id": "R1", "role": "Reviewer"}],
                        }
                    }
                },
                "updated_at": "2026-03-10T10:00:00+0900",
                "created_at": "2026-03-10T09:00:00+0900",
            }
        },
        "task_alias_index": {},
        "task_seq": 0,
    }

    summary = task_state.summarize_task_monitor(
        "Demo",
        entry,
        limit=5,
        normalize_task_status=gw.normalize_task_status,
        dedupe_roles=gw.dedupe_roles,
        task_display_label=gw.task_display_label,
        lifecycle_stages=gw.LIFECYCLE_STAGES,
    )
    assert "reqs E2/R1 linked=3 parallel=yes" in summary
    assert "{rerun E:L2 R:R1 | followup E:L3 R:-}" in summary
    assert (
        "first: /retry T-001 | collect-data-write-memo lane L2,R1 | active task requires retry (needs_retry) "
        "target execution=L2; review=R1"
    ) in summary


def test_priority_actions_module_matches_task_and_offdesk_policies() -> None:
    lane_targets = priority_actions.task_lane_target_snapshot(
        {
            "exec_critic": {
                "rerun_execution_lane_ids": ["L2"],
                "rerun_review_lane_ids": ["R1"],
                "manual_followup_execution_lane_ids": ["L3"],
            }
        }
    )
    assert lane_targets == {
        "rerun_execution_lane_ids": ["L2"],
        "rerun_review_lane_ids": ["R1"],
        "manual_followup_execution_lane_ids": ["L3"],
        "manual_followup_review_lane_ids": [],
    }
    task_priority = priority_actions.task_priority_action_snapshot(
        label="T-001 | collect-data-write-memo",
        tf_phase="needs_retry",
        rerun_execution_lane_ids=["L2"],
        rerun_review_lane_ids=["R1"],
        manual_followup_execution_lane_ids=[],
        manual_followup_review_lane_ids=[],
    )
    assert task_priority == {
        "action": "/retry T-001 | collect-data-write-memo lane L2,R1",
        "reason": "active task requires retry (needs_retry) target execution=L2; review=R1",
    }
    offdesk_priority = priority_actions.offdesk_priority_action_snapshot(
        alias="O4",
        active_task_label="",
        active_task_tf_phase="queued",
        active_task_targets=None,
        syncback_pending=False,
        followup_count=0,
        proposal_count=0,
        bootstrap_recommended=True,
        blocked_count=0,
        open_count=0,
        sync_quality="never",
        sync_quality_warn=False,
        sync_stale=False,
        canonical_exists=False,
        include_ok=False,
        last_sync_mode="never",
    )
    assert offdesk_priority == {
        "action": "/sync bootstrap O4 24h",
        "reason": "bootstrap backlog because canonical TODO.md is missing",
    }


def test_sync_catalog_module_classifies_sources_and_policy_consistently(tmp_path: Path) -> None:
    root = tmp_path / "Project"
    docs = root / "docs" / "handoff"
    docs.mkdir(parents=True, exist_ok=True)
    path = docs / "latest_handoff.md"
    path.write_text("# Handoff\n- [ ] package release notes\n", encoding="utf-8")

    info = sync_catalog._classify_sync_source(path, root, mode="recent_docs")
    assert info["source_class"] == "recent_doc"
    assert info["sync_group"] == "recent_docs"
    assert info["doc_type"] == "handoff"
    assert float(info["confidence"]) >= 0.8

    policy = {
        "doc_type_confidence": {"handoff": 0.91},
        "group_overrides": {"recent_doc": "recent_handoff_docs"},
        "min_confidence": 0.75,
    }
    patched = sync_catalog._apply_sync_policy(info, rel="docs/handoff/latest_handoff.md", policy=policy)
    assert patched["sync_group"] == "recent_handoff_docs"
    assert float(patched["confidence"]) == pytest.approx(0.91, rel=0, abs=1e-6)
    assert sync_catalog._sync_candidate_allowed(patched) is True


def test_sync_extract_module_matches_scheduler_doc_extraction_exports() -> None:
    text = """
# Notes

## Todo
- Purpose:
- P1: implement the actual follow-up

## Next steps
- P2 review the summary before off-desk handoff
"""

    assert sync_extract._extract_todo_items_from_doc(text, allow_any_checkbox=False) == sched._extract_todo_items_from_doc(
        text, allow_any_checkbox=False
    )
    assert sync_extract._extract_salvage_proposal_items_from_doc(text) == sched._extract_salvage_proposal_items_from_doc(text)


def test_sync_discovery_module_matches_scheduler_discovery_exports(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    docs_dir = project_root / "docs"
    team_dir.mkdir(parents=True, exist_ok=True)
    docs_dir.mkdir(parents=True, exist_ok=True)

    (team_dir / "AOE_TODO.md").write_text("# AOE_TODO.md\n\n## Tasks\n\n", encoding="utf-8")
    (project_root / "TODO.md").write_text("- [ ] P1: file fallback todo\n", encoding="utf-8")
    (docs_dir / "meeting-notes.md").write_text("# Todo\n- P1: recent fallback todo\n", encoding="utf-8")

    mode_a, items_a, meta_a, sources_a = sync_discovery._discover_sync_fallback_todos(
        project_root=project_root,
        docs_limit=3,
        files_limit=20,
        max_bytes=512 * 1024,
        min_mtime=0.0,
    )
    mode_b, items_b, meta_b, sources_b = sched._discover_sync_fallback_todos(
        project_root=project_root,
        docs_limit=3,
        files_limit=20,
        max_bytes=512 * 1024,
        min_mtime=0.0,
    )

    assert (mode_a, items_a, sources_a) == (mode_b, items_b, sources_b)
    assert meta_a["items_found"] == meta_b["items_found"]
    assert meta_a["active_modes"] == meta_b["active_modes"]


def test_task_state_sanitize_task_record_matches_gateway(monkeypatch) -> None:
    monkeypatch.setattr(gw, "now_iso", lambda: "2026-03-11T10:00:00+0900")
    raw_task = {
        "mode": "weird",
        "prompt": "  Review the output  ",
        "roles": ["Codex-Dev", "Reviewer", "Codex-Dev"],
        "verifier_roles": ["Reviewer", "Reviewer"],
        "require_verifier": 1,
        "stages": {"planning": "complete", "execution": "active", "garbage": "bad"},
        "stage": "unknown",
        "history": [
            {"at": "", "stage": "planning", "status": "success", "note": "ready"},
            {"at": "", "stage": "bad", "status": "oops"},
        ],
        "status": "done",
        "short_id": "t-008",
        "alias": " review-output ",
        "control_mode": "DISPATCH",
        "source_request_id": "REQ-00123456789",
        "retry_of": "REQ-0001",
        "replan_of": "REQ-0002",
        "retry_children": ["REQ-010", "REQ-010", ""],
        "replan_children": ["REQ-020", "REQ-021", "REQ-020"],
        "initiator_chat_id": "939062873",
        "todo_id": "TODO-004",
        "todo_priority": "p2",
        "todo_status": "RUNNING",
        "plan": {
            "summary": "demo",
            "meta": {"worker_roles": ["Reviewer", "Codex-Dev", "Reviewer"]},
            "subtasks": [{"id": "S1", "title": "check", "owner_role": "Reviewer"}],
        },
        "plan_critic": {"approved": False, "issues": ["missing acceptance"]},
        "plan_roles": ["Reviewer", "Codex-Dev", "Reviewer"],
        "plan_replans": [{"attempt": "2", "critic": "retry", "subtasks": "3"}],
        "plan_gate_passed": False,
        "exec_critic": {
            "verdict": "success",
            "action": "none",
            "attempt": 1,
            "max_attempts": 3,
            "at": "",
        },
        "context": {"project_key": "demo"},
    }

    expected = gw.sanitize_task_record(copy.deepcopy(raw_task), "REQ-777")
    actual = task_state.sanitize_task_record(
        copy.deepcopy(raw_task),
        "REQ-777",
        dedupe_roles=gw.dedupe_roles,
        lifecycle_stages=gw.LIFECYCLE_STAGES,
        normalize_stage_status=gw.normalize_stage_status,
        normalize_task_status=gw.normalize_task_status,
        now_iso=gw.now_iso,
        history_limit=gw.DEFAULT_TASK_HISTORY_LIMIT,
        normalize_task_plan_schema=gw.normalize_task_plan_schema,
        normalize_plan_critic_payload=gw.normalize_plan_critic_payload,
        normalize_plan_replans_payload=gw.normalize_plan_replans_payload,
        plan_critic_primary_issue=gw.plan_critic_primary_issue,
        normalize_exec_critic_payload=gw.normalize_exec_critic_payload,
        build_task_context=gw.build_task_context,
    )

    assert actual == expected


def test_tf_worker_specs_use_request_scoped_session_and_logs(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)

    args = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        orch_command_timeout_sec=900,
        aoe_orch_bin="/usr/bin/aoe-orch",
    )

    specs = gw.tf_worker_specs(args, "REQ-123", ["Reviewer"], startup_timeout_sec=120)

    assert len(specs) == 1
    spec = specs[0]
    assert spec["session"].startswith("tfw_req-123_reviewer")
    assert "aoe-tf-worker-session.py" in spec["shell"]
    assert "scripts/team/runtime/worker_codex_handler.sh" in spec["shell"]
    assert str(team_dir / "telegram.env") not in spec["shell"] or ". " in spec["shell"]
    assert str(team_dir / "tf_runs" / "REQ-123" / "logs" / "worker_reviewer.console.log") in spec["log_file"]


def test_resolve_dispatch_roles_from_preview_reads_dispatch_plan(monkeypatch) -> None:
    args = argparse.Namespace(
        aoe_orch_bin="/usr/bin/aoe-orch",
        project_root=Path("/tmp/project"),
        team_dir=Path("/tmp/project/.aoe-team"),
        orch_poll_sec=2.0,
        orch_command_timeout_sec=900,
    )

    class Proc:
        returncode = 0
        stdout = json.dumps(
            {
                "request_id": "REQ-1",
                "dispatch_plan": [
                    {"role": "DataEngineer", "title": "A"},
                    {"role": "Reviewer", "title": "B"},
                ],
            }
        )
        stderr = ""

    monkeypatch.setattr(gw, "run_command", lambda cmd, env, timeout_sec: Proc())
    roles = gw.resolve_dispatch_roles_from_preview(
        args,
        "Check quality",
        request_id="REQ-1",
        roles_override="",
        priority="P2",
        timeout_sec=120,
    )
    assert roles == ["DataEngineer", "Reviewer"]


def test_choose_auto_dispatch_roles_prefers_reviewer_for_simple_check(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Reviewer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "DataEngineer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Reviewer" / "AGENTS.md").write_text(
        "# AGENTS.md - Reviewer\n\n## Mission\nFind risks, regressions, and missing tests before merge.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "DataEngineer" / "AGENTS.md").write_text(
        "# AGENTS.md - DataEngineer\n\n## Mission\nOwn data ingestion, ETL quality, and schema consistency.\n",
        encoding="utf-8",
    )

    roles = gw.choose_auto_dispatch_roles(
        "현재 프로젝트 루트에서 .github가 있는지만 확인하고 한 문장으로 답해줘.",
        available_roles=["DataEngineer", "Reviewer"],
        team_dir=team_dir,
    )

    assert roles == ["Reviewer"]


def test_choose_auto_dispatch_roles_adds_claude_companion_for_multi_review_request(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    for role, mission in (
        ("Reviewer", "Find risks, regressions, and missing tests before merge."),
        ("Claude-Reviewer", "Find risks, regressions, and missing tests before merge."),
    ):
        (team_dir / "agents" / role).mkdir(parents=True, exist_ok=True)
        (team_dir / "agents" / role / "AGENTS.md").write_text(
            f"# AGENTS.md - {role}\n\n## Mission\n{mission}\n",
            encoding="utf-8",
        )

    roles = gw.choose_auto_dispatch_roles(
        "현재 변경사항을 검토하고 각각 교차검증해서 리스크를 짚어줘.",
        available_roles=["Reviewer", "Claude-Reviewer"],
        team_dir=team_dir,
    )

    assert roles == ["Reviewer", "Claude-Reviewer"]


def test_choose_auto_dispatch_roles_adds_claude_companion_for_explicit_review_role(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    for role, mission in (
        ("Reviewer", "Find risks, regressions, and missing tests before merge."),
        ("Claude-Reviewer", "Find risks, regressions, and missing tests before merge."),
    ):
        (team_dir / "agents" / role).mkdir(parents=True, exist_ok=True)
        (team_dir / "agents" / role / "AGENTS.md").write_text(
            f"# AGENTS.md - {role}\n\n## Mission\n{mission}\n",
            encoding="utf-8",
        )

    roles = gw.choose_auto_dispatch_roles(
        "Reviewer가 현재 변경사항을 검토하고 리스크를 짚어줘.",
        available_roles=["Reviewer", "Claude-Reviewer"],
        team_dir=team_dir,
    )

    assert roles == ["Reviewer", "Claude-Reviewer"]


def test_choose_auto_dispatch_roles_builds_multi_role_tf_from_prompt_mix(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Codex-Dev").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Reviewer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Codex-Writer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Codex-Dev" / "AGENTS.md").write_text(
        "# AGENTS.md - Codex-Dev\n\n## Mission\nImplement code changes and fix application bugs.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Reviewer" / "AGENTS.md").write_text(
        "# AGENTS.md - Reviewer\n\n## Mission\nFind risks, regressions, and missing tests before merge.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Codex-Writer" / "AGENTS.md").write_text(
        "# AGENTS.md - Codex-Writer\n\n## Mission\nWrite concise documents and reports.\n",
        encoding="utf-8",
    )

    roles = gw.choose_auto_dispatch_roles(
        "로그인 버그를 수정하고 회귀 리스크도 같이 검토해줘.",
        available_roles=["Codex-Dev", "Reviewer", "Codex-Writer"],
        team_dir=team_dir,
    )

    assert roles == ["Codex-Dev", "Reviewer"]


def test_choose_auto_dispatch_roles_picks_local_analyst_for_analysis_prompt(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Codex-Analyst").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Reviewer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Codex-Analyst" / "AGENTS.md").write_text(
        "# AGENTS.md - Codex-Analyst\n\n## Mission\nInvestigate project state, compare options, and surface defensible recommendations.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Reviewer" / "AGENTS.md").write_text(
        "# AGENTS.md - Reviewer\n\n## Mission\nFind risks, regressions, and missing tests before merge.\n",
        encoding="utf-8",
    )

    roles = gw.choose_auto_dispatch_roles(
        "현재 구조를 조사하고 두 방식의 트레이드오프를 비교해서 추천안을 정리해줘.",
        available_roles=["Codex-Analyst", "Reviewer"],
        team_dir=team_dir,
    )

    assert roles == ["Codex-Analyst"]


def test_choose_auto_dispatch_roles_prefers_local_writer_for_doc_request(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Codex-Writer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Codex-Dev").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Codex-Writer" / "AGENTS.md").write_text(
        "# AGENTS.md - Codex-Writer\n\n## Mission\nWrite concise project documents, summaries, and handoff notes that people can use immediately.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Codex-Dev" / "AGENTS.md").write_text(
        "# AGENTS.md - Codex-Dev\n\n## Mission\nImplement code changes, debug failures, and return verifiable fixes.\n",
        encoding="utf-8",
    )

    roles = gw.choose_auto_dispatch_roles(
        "배포 전에 문서를 정리하고 요약 보고서를 작성해줘.",
        available_roles=["Codex-Writer", "Codex-Dev"],
        team_dir=team_dir,
    )

    assert roles == ["Codex-Writer"]


def test_choose_auto_dispatch_roles_adds_claude_writer_and_analyst_companions_when_available(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    for role, mission in (
        ("Codex-Writer", "Write concise project documents and reports."),
        ("Claude-Writer", "Write concise project documents and reports."),
        ("Codex-Analyst", "Investigate project state, compare options, and recommend next steps."),
        ("Claude-Analyst", "Investigate project state, compare options, and recommend next steps."),
    ):
        (team_dir / "agents" / role).mkdir(parents=True, exist_ok=True)
        (team_dir / "agents" / role / "AGENTS.md").write_text(
            f"# AGENTS.md - {role}\n\n## Mission\n{mission}\n",
            encoding="utf-8",
        )

    writer_roles = gw.choose_auto_dispatch_roles(
        "문서를 정리하고 각각 교차검증해서 handoff 초안을 만들어줘.",
        available_roles=["Codex-Writer", "Claude-Writer", "Codex-Analyst", "Claude-Analyst"],
        team_dir=team_dir,
    )
    analyst_roles = gw.choose_auto_dispatch_roles(
        "현재 구조를 조사하고 각각 비교해서 추천안을 정리해줘.",
        available_roles=["Codex-Writer", "Claude-Writer", "Codex-Analyst", "Claude-Analyst"],
        team_dir=team_dir,
    )

    assert writer_roles == ["Codex-Writer", "Claude-Writer"]
    assert analyst_roles == ["Codex-Analyst", "Claude-Analyst"]


def test_choose_auto_dispatch_roles_orders_build_before_review_companions(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    for role, mission in (
        ("Codex-Dev", "Implement code changes and fix application bugs."),
        ("Reviewer", "Find risks, regressions, and missing tests before merge."),
        ("Claude-Reviewer", "Find risks, regressions, and missing tests before merge."),
    ):
        (team_dir / "agents" / role).mkdir(parents=True, exist_ok=True)
        (team_dir / "agents" / role / "AGENTS.md").write_text(
            f"# AGENTS.md - {role}\n\n## Mission\n{mission}\n",
            encoding="utf-8",
        )

    roles = gw.choose_auto_dispatch_roles(
        "로그인 버그를 수정하고 회귀 리스크도 같이 검토해줘.",
        available_roles=["Codex-Dev", "Reviewer", "Claude-Reviewer"],
        team_dir=team_dir,
    )

    assert roles == ["Codex-Dev", "Reviewer", "Claude-Reviewer"]


def test_available_worker_roles_uses_expanded_default_pool() -> None:
    assert gw.available_worker_roles([]) == [
        "DataEngineer",
        "Reviewer",
        "Claude-Reviewer",
        "Codex-Dev",
        "Codex-Writer",
        "Claude-Writer",
        "Codex-Analyst",
        "Claude-Analyst",
    ]


def test_runtime_seed_default_repair_agents_include_claude_companions() -> None:
    assert runtime_seed.DEFAULT_REPAIR_AGENTS == [
        "DataEngineer:codex",
        "Reviewer:codex",
        "Claude-Reviewer:claude",
        "Codex-Dev:codex",
        "Codex-Writer:codex",
        "Claude-Writer:claude",
        "Codex-Analyst:codex",
        "Claude-Analyst:claude",
    ]


def test_seed_runtime_from_spec_copies_claude_companion_templates(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    template_root = ROOT / "templates" / "aoe-team"
    project_root.mkdir(parents=True, exist_ok=True)

    spec = {
        "version": 1,
        "created_at": "2026-03-12T00:00:00+09:00",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "overview": "test",
        "coordinator": {"role": "Orchestrator"},
        "agents": [
            {"role": "Reviewer"},
            {"role": "Claude-Reviewer"},
            {"role": "Codex-Writer"},
            {"role": "Claude-Writer"},
            {"role": "Codex-Analyst"},
            {"role": "Claude-Analyst"},
        ],
    }

    logs = runtime_seed.seed_runtime_from_spec(
        template_root=template_root,
        project_root=project_root,
        team_dir=team_dir,
        overview="test",
        spec=spec,
        force=False,
    )

    assert any("Claude-Reviewer" in row for row in logs)
    assert any("Claude-Writer" in row for row in logs)
    assert any("Claude-Analyst" in row for row in logs)
    assert (team_dir / "agents" / "Claude-Reviewer" / "AGENTS.md").exists()
    assert (team_dir / "agents" / "Claude-Writer" / "AGENTS.md").exists()
    assert (team_dir / "agents" / "Claude-Analyst" / "AGENTS.md").exists()
    assert (team_dir / "workers" / "Claude-Reviewer.json").exists()
    assert (team_dir / "workers" / "Claude-Writer.json").exists()
    assert (team_dir / "workers" / "Claude-Analyst.json").exists()


def test_finalize_tf_exec_meta_marks_failed_roles_and_syncs_run_meta(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    req_id = "REQ-FAIL"
    run_dir = team_dir / "tf_runs" / req_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "meta.json").write_text(json.dumps({"request_id": req_id, "status": "running"}) + "\n", encoding="utf-8")

    tf_map = {
        req_id: {
            "request_id": req_id,
            "run_dir": str(run_dir),
            "status": "running",
        }
    }
    gw.save_tf_exec_map(team_dir, tf_map)

    gw.finalize_tf_exec_meta(
        team_dir,
        req_id,
        {
            "request_id": req_id,
            "complete": True,
            "roles": [{"role": "Reviewer", "status": "failed"}],
            "reply_messages": [],
        },
    )

    tf_row = gw.load_tf_exec_map(team_dir)[req_id]
    run_meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))

    assert tf_row["status"] == "failed"
    assert tf_row["failed_role_count"] == 1
    assert run_meta["status"] == "failed"
    assert run_meta["failed_role_count"] == 1


def test_finalize_request_reply_messages_marks_only_unresolved(monkeypatch, tmp_path: Path) -> None:
    args = argparse.Namespace(
        aoe_team_bin="aoe-team",
        team_dir=tmp_path / ".aoe-team",
    )
    args.team_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        gw,
        "run_request_query",
        lambda _args, _rid: {
            "request_id": "REQ-1",
            "reply_messages": [
                {"id": "m_sent", "from": "Reviewer", "status": "sent"},
                {"id": "m_done", "from": "DataEngineer", "status": "done"},
            ],
        },
    )

    calls: list[tuple[str, str, str]] = []

    def _fake_done(_args, message_id: str, actor: str, note: str) -> tuple[bool, str]:
        calls.append((message_id, actor, note))
        return True, f"done {message_id}"

    monkeypatch.setattr(gw, "run_message_done", _fake_done)

    result = gw.finalize_request_reply_messages(args, "REQ-1")

    assert result["targets"] == 1
    assert result["done"] == ["Reviewer:m_sent:sent"]
    assert "DataEngineer:m_done:done" in result["skipped"]
    assert calls == [("m_sent", "Orchestrator", "gateway integrated reply into final response")]


def test_request_state_module_matches_gateway_request_helpers(monkeypatch, tmp_path: Path) -> None:
    args = argparse.Namespace(
        aoe_team_bin="aoe-team",
        team_dir=tmp_path / ".aoe-team",
        state_file=tmp_path / "gateway_state.json",
    )
    args.team_dir.mkdir(parents=True, exist_ok=True)

    class Proc:
        returncode = 0
        stdout = json.dumps({"request_id": "REQ-1", "counts": {"messages": 1, "assignments": 1, "replies": 0}})
        stderr = ""

    monkeypatch.setattr(gw, "run_command", lambda cmd, env, timeout_sec: Proc())

    assert gw.run_request_query(args, "REQ-1") == request_state.run_request_query(
        args,
        "REQ-1",
        run_command=lambda cmd, env, timeout_sec: Proc(),
    )

    task = {"request_id": "REQ-1", "short_id": "T-001", "alias": "demo", "status": "running"}
    state = {
        "request_id": "REQ-1",
        "complete": False,
        "counts": {"messages": 1, "assignments": 1, "replies": 0},
        "roles": [{"role": "Reviewer", "status": "pending", "message_id": "m-1"}],
        "unresolved_roles": ["Reviewer"],
    }

    assert gw.summarize_request_state(state, task=task) == request_state.summarize_request_state(
        state,
        task=task,
        task_display_label=gw.task_display_label,
    )
    assert gw.render_run_response(state, task=task, report_level="short") == request_state.render_run_response(
        state,
        task=task,
        report_level="short",
        default_report_level=gw.DEFAULT_REPORT_LEVEL,
        task_display_label=gw.task_display_label,
        summarize_state=gw.summarize_state,
    )

    reply_state = {
        "request_id": "REQ-1",
        "reply_messages": [
            {"id": "m-1", "from": "Reviewer", "status": "sent"},
            {"id": "m-2", "from": "Reviewer", "status": "done"},
        ],
    }
    done_calls: list[tuple[str, str, str]] = []

    def _done(_args, message_id: str, actor: str, note: str) -> tuple[bool, str]:
        done_calls.append((message_id, actor, note))
        return True, "ok"

    monkeypatch.setattr(gw, "run_request_query", lambda _args, _rid: reply_state)
    monkeypatch.setattr(gw, "run_message_done", _done)

    assert gw.finalize_request_reply_messages(
        args,
        "REQ-1",
        actor="Orchestrator",
        note="note",
    ) == request_state.finalize_request_reply_messages(
        args,
        "REQ-1",
        run_request_query=lambda _args, _rid: reply_state,
        run_message_done=_done,
        actor="Orchestrator",
        note="note",
    )
    assert done_calls == [("m-1", "Orchestrator", "note"), ("m-1", "Orchestrator", "note")]


def test_tf_exec_module_matches_gateway_exec_helpers(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_TF_EXEC_MODE", "inplace")

    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)

    args_a = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        orch_command_timeout_sec=900,
        aoe_orch_bin="/usr/bin/aoe-orch",
        orch_poll_sec=2.0,
        roles="",
        priority="P2",
        orch_timeout_sec=120,
        no_wait=False,
        _aoe_project_key="demo_proj",
        _aoe_project_alias="O9",
        _aoe_control_mode="retry",
        _aoe_source_request_id="REQ-000",
    )
    args_b = copy.deepcopy(args_a)
    args_b._aoe_default_tf_worker_session_prefix = gw.DEFAULT_TF_WORKER_SESSION_PREFIX
    lane_summary = {
        "execution_lanes": [
            {"lane_id": "L1", "role": "Reviewer", "subtask_ids": ["S1"]},
        ],
        "review_lanes": [
            {"lane_id": "R1", "role": "QA", "depends_on": ["L1"]},
        ],
    }

    dispatch_metadata = {
        "phase2_team_spec": {"execution_mode": "parallel", "execution_groups": [{"group_id": "E1", "role": "Codex-Dev"}]},
        "phase2_execution_plan": {"execution_mode": "parallel", "execution_lanes": [{"lane_id": "L1", "role": "Codex-Dev"}]},
        "phase1_mode": "ensemble",
        "phase1_rounds": 3,
        "phase1_providers": ["codex", "claude"],
    }

    meta_a = gw.ensure_tf_exec_workspace(args_a, "REQ-001", metadata=dispatch_metadata)
    meta_b = tf_exec.ensure_tf_exec_workspace(
        args_b,
        "REQ-002",
        metadata=dispatch_metadata,
        default_tf_exec_mode=gw.DEFAULT_TF_EXEC_MODE,
        default_tf_work_root_name=gw.DEFAULT_TF_WORK_ROOT_NAME,
        default_tf_exec_map_file=gw.DEFAULT_TF_EXEC_MAP_FILE,
        now_iso=gw.now_iso,
        run_command=gw.run_command,
    )
    assert meta_a["project_key"] == meta_b["project_key"] == "demo_proj"
    assert meta_a["project_alias"] == meta_b["project_alias"] == "O9"
    assert meta_a["phase2_execution_plan"]["execution_mode"] == "parallel"
    assert meta_b["phase2_execution_plan"]["execution_mode"] == "parallel"
    assert meta_a["phase1_providers"] == meta_b["phase1_providers"] == ["codex", "claude"]

    specs_a = gw.tf_worker_specs(args_a, "REQ-123", ["Reviewer"], startup_timeout_sec=120, lane_summary=lane_summary)
    specs_b = tf_exec.tf_worker_specs(args_b, "REQ-123", ["Reviewer"], startup_timeout_sec=120, lane_summary=lane_summary)
    assert specs_a == specs_b
    assert specs_a[0]["execution_lane_ids"] == ["L1"]
    assert specs_a[0]["review_lane_ids"] == []

    class Proc:
        returncode = 0
        stdout = json.dumps({"request_id": "REQ-1", "dispatch_plan": [{"role": "DataEngineer"}, {"role": "Reviewer"}]})
        stderr = ""

    original_run_command = gw.run_command
    gw.run_command = lambda cmd, env, timeout_sec: Proc()
    try:
        roles_a = gw.resolve_dispatch_roles_from_preview(args_a, "Check quality", "REQ-1", "", "P2", 120)
        roles_b = tf_exec.resolve_dispatch_roles_from_preview(
            args_b,
            "Check quality",
            "REQ-1",
            "",
            "P2",
            120,
            run_command=gw.run_command,
        )
    finally:
        gw.run_command = original_run_command
    assert roles_a == roles_b == ["DataEngineer", "Reviewer"]


def test_tf_exec_lane_summary_and_role_merge_helpers() -> None:
    metadata = {
        "phase2_execution_plan": {
            "execution_mode": "parallel",
            "execution_lanes": [
                {"lane_id": "L1", "role": "Codex-Dev", "subtask_ids": ["S1"], "parallel": True},
                {"lane_id": "L2", "role": "Codex-Writer", "subtask_ids": ["S2"], "parallel": True},
            ],
            "review_mode": "single",
            "review_lanes": [
                {"lane_id": "R1", "role": "Reviewer", "kind": "verifier", "depends_on": ["L1", "L2"], "parallel": False},
            ],
            "parallel_workers": True,
            "parallel_reviews": False,
            "readonly": True,
        }
    }
    summary = tf_exec.phase2_execution_lane_summary(metadata)
    merged = tf_exec.merge_worker_roles_with_lane_summary(["Reviewer"], summary)

    assert summary["execution_roles"] == ["Codex-Dev", "Codex-Writer"]
    assert summary["review_roles"] == ["Reviewer"]
    assert summary["planned_roles"] == ["Codex-Dev", "Codex-Writer", "Reviewer"]
    assert merged == ["Codex-Dev", "Codex-Writer", "Reviewer"]


def test_infer_natural_run_mode_treats_direct_as_bias_not_force() -> None:
    assert tg_parse.infer_natural_run_mode("로그인 버그를 수정해줘", "direct") == "dispatch"
    assert tg_parse.infer_natural_run_mode("지금 상태 설명해줘", "direct") == "direct"


def test_set_and_clear_project_lock_roundtrip() -> None:
    state = _empty_state()
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": "/tmp/TwinPaper/.aoe-team",
        "tasks": {},
    }

    row = gw.set_project_lock(state, "twinpaper", actor="chat:939062873")

    assert row["project_key"] == "twinpaper"
    assert row["locked_by"] == "chat:939062873"
    assert gw.get_project_lock_key(state) == "twinpaper"
    assert gw.project_lock_label(state) == "O2 (twinpaper)"
    assert state["active"] == "twinpaper"

    assert gw.clear_project_lock(state) is True
    assert gw.get_project_lock_key(state) == ""
    assert gw.clear_project_lock(state) is False


def test_project_state_module_matches_gateway_project_helpers(tmp_path: Path) -> None:
    state = _empty_state()
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {},
    }

    assert gw.normalize_project_name("Twin Paper") == project_state.normalize_project_name("Twin Paper")
    assert gw.normalize_project_alias("o2") == project_state.normalize_project_alias("o2")
    assert gw.extract_project_alias_index("O2") == project_state.extract_project_alias_index("O2")
    assert gw.ensure_project_aliases(state) == project_state.ensure_project_aliases(state)
    assert gw.project_alias_for_key(state, "twinpaper") == project_state.project_alias_for_key(state, "twinpaper")

    state_for_mod = copy.deepcopy(state)
    original_now_iso = gw.now_iso
    try:
        gw.now_iso = lambda: "2026-03-11T12:00:00+0900"
        row = gw.set_project_lock(state, "twinpaper", actor="chat:939062873")
        row_mod = project_state.set_project_lock(
            state_for_mod,
            "twinpaper",
            now_iso=lambda: "2026-03-11T12:00:00+0900",
            actor="chat:939062873",
        )
    finally:
        gw.now_iso = original_now_iso
    assert row == row_mod
    assert gw.get_project_lock_key(state) == project_state.get_project_lock_key(state, bool_from_json=gw.bool_from_json)
    assert gw.project_lock_label(state) == project_state.project_lock_label(state, bool_from_json=gw.bool_from_json)

    key_a, entry_a = gw.get_manager_project(state, "O2")
    key_b, entry_b = project_state.get_manager_project(state, "O2", bool_from_json=gw.bool_from_json)
    assert (key_a, entry_a) == (key_b, entry_b)

    args = argparse.Namespace(project_root=project_root, team_dir=team_dir, foo="bar")
    a_args = gw.make_project_args(args, entry_a, key=key_a)
    b_args = project_state.make_project_args(args, entry_b, key=key_b)
    assert vars(a_args) == vars(b_args)


def test_get_manager_project_respects_hard_project_lock() -> None:
    state = _empty_state()
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": "/tmp/TwinPaper/.aoe-team",
        "tasks": {},
    }
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": "/tmp/Nano",
        "team_dir": "/tmp/Nano/.aoe-team",
        "tasks": {},
    }

    gw.set_project_lock(state, "twinpaper")

    key, _entry = gw.get_manager_project(state, None)
    assert key == "twinpaper"

    key, _entry = gw.get_manager_project(state, "O2")
    assert key == "twinpaper"

    try:
        gw.get_manager_project(state, "O3")
    except RuntimeError as exc:
        text = str(exc)
        assert "project lock active" in text
        assert "use /focus off or /focus O2" in text
    else:
        raise AssertionError("expected project lock conflict")


def test_parse_focus_and_unlock_commands() -> None:
    assert tg_parse.parse_cli_message("aoe focus O2") == {"cmd": "focus", "rest": "O2"}
    assert tg_parse.parse_cli_message("aoe unlock") == {"cmd": "focus", "rest": "off"}
    assert tg_parse.parse_cli_message("aoe orch repair O2") == {"cmd": "orch-repair", "orch": "O2"}

    manager_state = _empty_state()
    resolved = resolver.resolve_message_command(
        text="/unlock",
        slash_only=False,
        manager_state=manager_state,
        chat_id="939062873",
        dry_run=True,
        manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
        get_pending_mode=gw.get_pending_mode,
        get_default_mode=gw.get_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        save_manager_state=lambda path, state: None,
    )

    assert resolved.cmd == "focus"
    assert resolved.rest == "off"


def test_parse_add_provider_shortcuts_and_resolve_slash_add_claude() -> None:
    assert tg_parse.parse_cli_message("aoe add-claude Reviewer") == {
        "cmd": "add-role",
        "role": "Reviewer",
        "provider": "claude",
        "launch": "claude",
        "spawn": True,
    }
    assert tg_parse.parse_cli_message("aoe add-codex Codex-Dev --no-spawn") == {
        "cmd": "add-role",
        "role": "Codex-Dev",
        "provider": "codex",
        "launch": "codex",
        "spawn": False,
    }
    assert tg_parse.parse_cli_message("aoe add-claude --name ClaudeReviewer --no-spawn") == {
        "cmd": "add-role",
        "role": "ClaudeReviewer",
        "provider": "claude",
        "launch": "claude",
        "spawn": False,
    }

    manager_state = _empty_state()
    resolved = resolver.resolve_message_command(
        text="/add-claude --name Reviewer --spawn",
        slash_only=False,
        manager_state=manager_state,
        chat_id="939062873",
        dry_run=True,
        manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
        get_pending_mode=gw.get_pending_mode,
        get_default_mode=gw.get_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        save_manager_state=lambda path, state: None,
    )

    assert resolved.cmd == "add-role"
    assert resolved.add_role_name == "Reviewer"
    assert resolved.add_role_provider == "claude"
    assert resolved.add_role_launch == "claude"
    assert resolved.add_role_spawn is True


def test_summarize_orch_registry_shows_focus_counts_and_sync() -> None:
    state = _empty_state()
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": str(ROOT / ".aoe-team"),
        "last_sync_at": "",
        "last_sync_mode": "scenario",
        "pending_todo": {"todo_id": "TODO-001", "chat_id": "939062873", "selected_at": "2026-03-06T12:00:00+0900"},
        "todos": [
            {"id": "TODO-001", "summary": "first", "priority": "P1", "status": "open"},
            {"id": "TODO-002", "summary": "second", "priority": "P2", "status": "running"},
            {"id": "TODO-003", "summary": "third", "priority": "P2", "status": "blocked"},
        ],
        "tasks": {
            "REQ-1": {
                "request_id": "REQ-1",
                "short_id": "T-001",
                "alias": "login-fix",
                "prompt": "fix login",
                "status": "running",
                "updated_at": "2026-03-06T12:00:00+0900",
            }
        },
    }
    state["active"] = "twinpaper"
    gw.set_project_lock(state, "twinpaper")

    text = gw.summarize_orch_registry(state)

    assert "active: O2 (twinpaper)" in text
    assert "project_lock: O2 (twinpaper)" in text
    assert "* O2 TwinPaper [PENDING] | todo o/r/b=1/1/1 | last_sync=scenario | last_task=T-001 login-fix[running]" in text
    assert "key=twinpaper | root=/tmp/TwinPaper" in text


def test_project_runtime_issue_reports_missing_orchestrator(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)

    issue = runtime_helpers.project_runtime_issue({"team_dir": str(team_dir)})

    assert issue.startswith("missing_orchestrator:")
    assert "orchestrator.json" in issue


def test_append_gateway_event_targets_mirrors_to_root_log(tmp_path: Path) -> None:
    project_team_dir = tmp_path / "project" / ".aoe-team"
    root_team_dir = tmp_path / "mother" / ".aoe-team"
    row = {
        "timestamp": "2026-03-07T19:00:00+0900",
        "event": "dispatch_completed",
        "trace_id": "trace-1",
        "project": "twinpaper",
        "request_id": "REQ-1",
        "task_short_id": "T-001",
        "task_alias": "demo",
        "stage": "close",
        "actor": "telegram:939062873",
        "status": "completed",
        "error_code": "",
        "latency_ms": 123,
        "detail": "ok",
    }

    gw.append_gateway_event_targets(team_dir=project_team_dir, row=row, mirror_team_dir=root_team_dir)

    proj_rows = [json.loads(x) for x in (project_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    root_rows = [json.loads(x) for x in (root_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]

    assert proj_rows[-1]["log_scope"] == "project"
    assert "project_team_dir" not in proj_rows[-1]
    assert root_rows[-1]["log_scope"] == "mother"
    assert root_rows[-1]["project_team_dir"] == str(project_team_dir.resolve())


def test_mirror_tf_backend_runtime_events_writes_project_and_root_rows(tmp_path: Path) -> None:
    project_team_dir = tmp_path / "project" / ".aoe-team"
    root_team_dir = tmp_path / "mother" / ".aoe-team"
    runtime_events = [
        {
            "seq": 1,
            "ts": "2026-03-11T18:00:00+0900",
            "backend": "autogen_core",
            "source": "tf_orchestrator",
            "stage": "request.accepted",
            "kind": "lifecycle",
            "status": "info",
            "summary": "accepted TF request",
            "payload": {"project_key": "O3"},
        },
        {
            "seq": 2,
            "ts": "2026-03-11T18:00:01+0900",
            "backend": "autogen_core",
            "source": "reviewer",
            "stage": "verdict.emitted",
            "kind": "verdict",
            "status": "success",
            "summary": "review verdict emitted",
            "payload": {"verdict": "success"},
        },
    ]

    mirrored = gw.mirror_tf_backend_runtime_events(
        team_dir=project_team_dir,
        backend="autogen_core",
        runtime_events=runtime_events,
        trace_id="trace-runtime-1",
        project="kisti_nanoclustering",
        request_id="REQ-42",
        task={"short_id": "T-042", "alias": "sandbox"},
        mirror_team_dir=root_team_dir,
    )

    assert mirrored == 2
    proj_rows = [json.loads(x) for x in (project_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    root_rows = [json.loads(x) for x in (root_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]

    assert len(proj_rows) == 2
    assert len(root_rows) == 2
    assert proj_rows[0]["event"] == "tf_backend_runtime_event"
    assert proj_rows[0]["backend"] == "autogen_core"
    assert proj_rows[0]["backend_seq"] == 1
    assert proj_rows[1]["backend_kind"] == "verdict"
    assert proj_rows[1]["actor"] == "autogen_core:reviewer"
    assert proj_rows[1]["request_id"] == "REQ-42"
    assert proj_rows[1]["task_short_id"] == "T-042"
    assert proj_rows[1]["log_scope"] == "project"
    assert root_rows[0]["log_scope"] == "mother"
    assert root_rows[0]["project_team_dir"] == str(project_team_dir.resolve())


def test_gateway_events_module_matches_gateway_runtime_event_mirroring(tmp_path: Path) -> None:
    project_a = tmp_path / "a" / ".aoe-team"
    project_b = tmp_path / "b" / ".aoe-team"
    runtime_events = [
        {
            "seq": 1,
            "ts": "2026-03-11T18:00:00+0900",
            "backend": "local",
            "source": "gateway.preview",
            "stage": "roles.resolved",
            "kind": "dispatch",
            "status": "success",
            "summary": "resolved role set",
            "payload": {"roles": ["Reviewer"]},
        }
    ]

    count_a = gw.mirror_tf_backend_runtime_events(
        team_dir=project_a,
        backend="local",
        runtime_events=runtime_events,
        trace_id="trace-a",
        project="demo",
        request_id="REQ-A",
        task={"short_id": "T-001", "alias": "demo"},
    )
    count_b = gateway_events.mirror_backend_runtime_events(
        team_dir=project_b,
        backend="local",
        runtime_events=runtime_events,
        now_iso=gw.now_iso,
        mask_sensitive_text=gw.mask_sensitive_text,
        append_gateway_event_targets=lambda **kwargs: gateway_events.append_gateway_event_targets(
            append_jsonl=gw.append_jsonl,
            **kwargs,
        ),
        trace_id="trace-a",
        project="demo",
        request_id="REQ-A",
        task={"short_id": "T-001", "alias": "demo"},
    )

    rows_a = [json.loads(x) for x in (project_a / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    rows_b = [json.loads(x) for x in (project_b / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]

    assert count_a == count_b == 1
    assert rows_a == rows_b


def test_summarize_orch_registry_marks_unready_project(tmp_path: Path) -> None:
    state = _empty_state()
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": str(team_dir),
        "tasks": {},
    }

    text = gw.summarize_orch_registry(state)

    assert "O2 TwinPaper [UNREADY]" in text
    assert "runtime=missing orchestrator.json" in text


def test_orch_registry_module_matches_gateway_summary_and_status(tmp_path: Path, monkeypatch) -> None:
    state = _empty_state()
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(team_dir),
        "last_sync_mode": "scenario",
        "todos": [{"id": "TODO-001", "summary": "first", "priority": "P1", "status": "open"}],
        "tasks": {},
    }
    state["active"] = "twinpaper"

    gw_text = gw.summarize_orch_registry(state)
    mod_text = orch_registry.summarize_orch_registry(
        state,
        ensure_project_aliases=gw.ensure_project_aliases,
        project_alias_for_key=gw.project_alias_for_key,
        project_lock_label=gw.project_lock_label,
        extract_project_alias_index=gw.extract_project_alias_index,
        bool_from_json=gw.bool_from_json,
        task_display_label=gw.task_display_label,
        normalize_task_status=gw.normalize_task_status,
    )
    assert gw_text == mod_text

    args = argparse.Namespace(
        aoe_orch_bin="aoe-orch",
        project_root=tmp_path / "TwinPaper",
        team_dir=team_dir,
        state_file=tmp_path / "gateway_state.json",
    )

    class Proc:
        returncode = 0
        stdout = "status ok"
        stderr = ""

    monkeypatch.setattr(gw, "run_command", lambda cmd, env, timeout_sec: Proc())
    monkeypatch.setattr(gw, "summarize_gateway_poll_state", lambda path: "poll-summary")

    assert gw.run_aoe_status(args) == orch_registry.run_aoe_status(
        args,
        run_command=lambda cmd, env, timeout_sec: Proc(),
        summarize_gateway_poll_state=lambda path: "poll-summary",
    )


def test_drain_peek_next_todo_skips_unready_project_and_selects_ready_one(tmp_path: Path) -> None:
    state = _empty_state()
    bad_team = tmp_path / "TwinPaper" / ".aoe-team"
    bad_team.mkdir(parents=True, exist_ok=True)
    good_team = tmp_path / "Local" / ".aoe-team"
    good_team.mkdir(parents=True, exist_ok=True)
    (good_team / "orchestrator.json").write_text("{}", encoding="utf-8")

    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(bad_team),
        "todos": [{"id": "TODO-001", "summary": "broken runtime", "priority": "P1", "status": "open"}],
    }
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Local"),
        "team_dir": str(good_team),
        "todos": [{"id": "TODO-001", "summary": "ready runtime", "priority": "P2", "status": "open"}],
    }

    key, todo_id, reason = gw._drain_peek_next_todo(state, "939062873", force=False)

    assert key == "local"
    assert todo_id == "TODO-001"
    assert reason == "candidate"


def test_drain_peek_next_todo_ignores_blocked_rows_when_open_todo_exists(tmp_path: Path) -> None:
    state = _empty_state()
    team = tmp_path / "Local" / ".aoe-team"
    team.mkdir(parents=True, exist_ok=True)
    (team / "orchestrator.json").write_text("{}", encoding="utf-8")

    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Local"),
        "team_dir": str(team),
        "todos": [
            {"id": "TODO-001", "summary": "blocked first", "priority": "P1", "status": "blocked"},
            {"id": "TODO-002", "summary": "open second", "priority": "P2", "status": "open"},
        ],
    }

    key, todo_id, reason = gw._drain_peek_next_todo(state, "939062873", force=False)

    assert key == "local"
    assert todo_id == "TODO-002"
    assert reason == "candidate"


def test_gateway_batch_ops_module_matches_gateway_drain_peek(tmp_path: Path) -> None:
    state = _empty_state()
    team = tmp_path / "Local" / ".aoe-team"
    team.mkdir(parents=True, exist_ok=True)
    (team / "orchestrator.json").write_text("{}", encoding="utf-8")

    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Local"),
        "team_dir": str(team),
        "todos": [
            {"id": "TODO-001", "summary": "blocked first", "priority": "P1", "status": "blocked"},
            {"id": "TODO-002", "summary": "open second", "priority": "P2", "status": "open"},
        ],
    }

    assert gateway_batch_ops.drain_peek_next_todo(state, "939062873", force=False) == gw._drain_peek_next_todo(
        state,
        "939062873",
        force=False,
    )


def test_queue_engine_matches_gateway_and_scheduler_next_selection(tmp_path: Path) -> None:
    state = _empty_state()
    team = tmp_path / "Local" / ".aoe-team"
    team.mkdir(parents=True, exist_ok=True)
    (team / "orchestrator.json").write_text("{}", encoding="utf-8")

    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Local"),
        "team_dir": str(team),
        "todos": [
            {"id": "TODO-001", "summary": "blocked first", "priority": "P1", "status": "blocked"},
            {"id": "TODO-002", "summary": "open second", "priority": "P2", "status": "open"},
        ],
    }

    queue_pick = queue_engine.pick_global_next_candidate(state["projects"], ignore_busy=False, skip_paused=True)
    sched_pick = sched._pick_global_next_candidate(state["projects"], ignore_busy=False, skip_paused=True)
    gw_pick = gw._drain_peek_next_todo(state, "939062873", force=False)

    assert isinstance(queue_pick, dict)
    assert isinstance(sched_pick, dict)
    assert queue_pick["project_key"] == "local"
    assert queue_pick["todo"]["id"] == "TODO-002"
    assert sched_pick["project_key"] == queue_pick["project_key"]
    assert sched_pick["todo"]["id"] == queue_pick["todo"]["id"]
    assert gw_pick == ("local", "TODO-002", "candidate")


def test_transport_module_matches_gateway_transport_exports() -> None:
    previous = os.environ.get("AOE_TG_COMMAND_PREFIXES")
    os.environ["AOE_TG_COMMAND_PREFIXES"] = "!/"
    try:
        body = "alpha\nbeta\n" + ("z" * 300)
        assert gw.split_text(body, 120) == transport.split_text(body, 120)
        assert gw.preferred_command_prefix() == transport.preferred_command_prefix() == "!"
        assert gw.build_quick_reply_keyboard() == transport.build_quick_reply_keyboard()
    finally:
        if previous is None:
            os.environ.pop("AOE_TG_COMMAND_PREFIXES", None)
        else:
            os.environ["AOE_TG_COMMAND_PREFIXES"] = previous


def test_runtime_core_matches_gateway_path_and_default_state_helpers(tmp_path: Path) -> None:
    project_root = runtime_core.resolve_project_root(str(tmp_path))
    team_dir = runtime_core.resolve_team_dir(project_root, None)
    state_file = runtime_core.resolve_state_file(project_root, None)

    assert gw.resolve_project_root(str(tmp_path)) == project_root
    assert gw.resolve_team_dir(project_root, None) == team_dir
    assert gw.resolve_state_file(project_root, None) == state_file

    expected = runtime_core.default_manager_state(project_root, team_dir, now_iso=gw.now_iso)
    actual = gw.default_manager_state(project_root, team_dir)
    assert actual == expected


def test_gateway_events_module_matches_gateway_task_identifiers() -> None:
    task = {"short_id": "T-001", "alias": "demo"}
    assert gw.task_identifiers(task) == gateway_events.task_identifiers(task)


def test_runtime_core_matches_gateway_default_project_registration(tmp_path: Path) -> None:
    state_a = {"active": "missing", "projects": {"demo": {"name": "demo", "project_alias": "O2"}}}
    state_b = copy.deepcopy(state_a)
    project_root = tmp_path
    team_dir = tmp_path / ".aoe-team"

    gw.ensure_default_project_registered(state_a, project_root, team_dir)
    runtime_core.ensure_default_project_registered(
        state_b,
        project_root,
        team_dir,
        now_iso=gw.now_iso,
        bool_from_json=gw.bool_from_json,
        normalize_project_alias=gw.normalize_project_alias,
        normalize_project_name=gw.normalize_project_name,
        sanitize_project_lock_row=gw.sanitize_project_lock_row,
        ensure_project_aliases=gw.ensure_project_aliases,
        backfill_task_aliases=gw.backfill_task_aliases,
    )

    assert state_a == state_b


    assert tg_parse.parse_quick_message("동기화가 계속 꼬이는 이유를 분석해줘") is None
    assert tg_parse.parse_quick_message("자동 실행을 검토해줘") is None


    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    entry = state["projects"]["default"]
    entry["project_alias"] = "O1"
    project_root = Path(str(entry["project_root"]))
    team_dir = Path(str(entry["team_dir"]))
    project_root.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "AOE_TODO.md").write_text("# TODO\n\n- [ ] P1: keep same task\n", encoding="utf-8")
    entry["todos"] = [
        {
            "id": "TODO-001",
            "summary": "keep same task",
            "priority": "P1",
            "status": "open",
            "created_at": "2026-03-05T10:00:00+0900",
            "updated_at": "2026-03-05T10:00:00+0900",
        }
    ]

    sent: list[str] = []
    saves: list[Path] = []
    result = sched.handle_scheduler_command(
        cmd="sync",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="O1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("default", entry, argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: saves.append(path),
        now_iso=lambda: "2026-03-06T12:00:00+0900",
    )

    assert result == {"terminal": True}
    assert saves == [team_dir / "orch_manager_state.json"]
    assert entry["last_sync_at"] == "2026-03-06T12:00:00+0900"
    assert entry["last_sync_mode"] == "scenario"
