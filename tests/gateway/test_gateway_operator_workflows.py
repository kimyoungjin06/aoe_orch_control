#!/usr/bin/env python3
"""Gateway operator workflow regression tests."""

from _gateway_test_support import *  # noqa: F401,F403
def test_orch_map_reply_markup_contains_use_focus_status_todo_and_active_sync_actions() -> None:
    state = _empty_state()
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": "/tmp/TwinPaper/.aoe-team",
    }
    state["active"] = "twinpaper"

    markup = overview._orch_map_reply_markup(state)

    assert isinstance(markup, dict)
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/use O1" not in buttons
    assert "/focus O1" not in buttons
    assert "/use O2" in buttons
    assert "/focus O2" in buttons
    assert "/orch status O1" not in buttons
    assert "/todo O1" not in buttons
    assert "/todo O1 followup" not in buttons
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/todo O2 followup" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/sync O2 1h" in buttons
    assert "/queue" in buttons
    assert "/next" in buttons


def test_orch_map_reply_markup_narrows_to_locked_project() -> None:
    state = _empty_state()
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": "/tmp/TwinPaper/.aoe-team",
    }
    state["active"] = "twinpaper"
    gw.set_project_lock(state, "twinpaper")

    markup = overview._orch_map_reply_markup(state)

    assert isinstance(markup, dict)
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/use O1" not in buttons
    assert "/focus O1" not in buttons
    assert "/use O2" in buttons
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/todo O2 followup" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/sync O2 1h" in buttons
    assert "/focus off" in buttons


def test_orch_map_reply_markup_includes_repair_for_unready_project(tmp_path: Path) -> None:
    state = _empty_state()
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(team_dir),
    }

    markup = overview._orch_map_reply_markup(state)

    assert isinstance(markup, dict)
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/orch repair O2" in buttons


def test_resolve_message_command_parses_slash_orch_repair() -> None:
    resolved = resolver.resolve_message_command(
        text="/orch repair O2",
        slash_only=False,
        manager_state=_empty_state(),
        chat_id="939062873",
        dry_run=True,
        manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
        get_pending_mode=gw.get_pending_mode,
        get_default_mode=gw.get_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        save_manager_state=lambda path, state: None,
    )

    assert resolved.cmd == "orch-repair"
    assert resolved.orch_target == "O2"


def test_orch_repair_rebuilds_missing_runtime(tmp_path: Path) -> None:
    state = _empty_state()
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "overview": "Twin project orchestration",
        "tasks": {},
    }
    state["active"] = "twinpaper"

    messages = []

    def _send(msg: str, **kwargs):
        messages.append(msg)
        return True

    def _run_aoe_init(args, project_root: Path, team_dir: Path, overview: str) -> str:
        team_dir.mkdir(parents=True, exist_ok=True)
        (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
        return "[OK] initialized"

    handled = orch_task_handlers.handle_orch_task_command(
        cmd="orch-repair",
        args=argparse.Namespace(
            project_root=ROOT,
            manager_state_file=tmp_path / "manager_state.json",
            dry_run=False,
        ),
        manager_state=state,
        chat_id="939062873",
        orch_target="O2",
        orch_add_name=None,
        orch_add_path=None,
        orch_add_overview=None,
        orch_add_init=True,
        orch_add_spawn=True,
        orch_add_set_active=True,
        rest="",
        orch_check_request_id=None,
        orch_task_request_id=None,
        orch_pick_request_id=None,
        orch_cancel_request_id=None,
        send=_send,
        log_event=lambda **kwargs: None,
        get_context=lambda target: (_ for _ in ()).throw(RuntimeError("not used")),
        latest_task_request_refs=lambda *args, **kwargs: [],
        set_chat_recent_task_refs=lambda *args, **kwargs: None,
        save_manager_state=lambda path, state: None,
        resolve_project_root=lambda raw: Path(raw).expanduser().resolve(),
        is_path_within=lambda path, root: True,
        register_orch_project=lambda *args, **kwargs: ("", {}),
        run_aoe_init=_run_aoe_init,
        run_aoe_spawn=lambda *args, **kwargs: "[SKIP] spawn",
        now_iso=lambda: "2026-03-07T18:30:00+0900",
        run_aoe_status=lambda p_args: "",
        resolve_chat_task_ref=lambda *args, **kwargs: "",
        resolve_task_request_id=lambda entry, ref: "",
        run_request_query=lambda *args, **kwargs: {},
        sync_task_lifecycle=lambda *args, **kwargs: None,
        resolve_verifier_candidates=lambda text: [],
        touch_chat_recent_task_ref=lambda *args, **kwargs: None,
        set_chat_selected_task_ref=lambda *args, **kwargs: None,
        get_chat_selected_task_ref=lambda *args, **kwargs: "",
        get_task_record=lambda *args, **kwargs: None,
        summarize_request_state=lambda *args, **kwargs: "",
        summarize_three_stage_request=lambda *args, **kwargs: "",
        summarize_task_lifecycle=lambda *args, **kwargs: "",
        task_display_label=lambda *args, **kwargs: "",
        cancel_request_assignments=lambda *args, **kwargs: {},
        lifecycle_set_stage=lambda *args, **kwargs: None,
        summarize_cancel_result=lambda *args, **kwargs: "",
    )

    assert handled is True
    assert (team_dir / "orchestrator.json").exists()
    assert (team_dir / "AOE_TODO.md").exists()
    assert messages
    assert "orch repair finished" in messages[-1]
    assert "- after: ready" in messages[-1]


def test_orch_repair_all_repairs_multiple_projects(tmp_path: Path) -> None:
    state = _empty_state()
    for key, alias in [("twinpaper", "O2"), ("nano", "O3")]:
        project_root = tmp_path / key
        team_dir = project_root / ".aoe-team"
        team_dir.mkdir(parents=True, exist_ok=True)
        state["projects"][key] = {
            "name": key,
            "display_name": key,
            "project_alias": alias,
            "project_root": str(project_root),
            "team_dir": str(team_dir),
            "overview": f"{key} orchestration",
            "tasks": {},
        }

    messages = []

    def _send(msg: str, **kwargs):
        messages.append(msg)
        return True

    def _run_aoe_init(args, project_root: Path, team_dir: Path, overview: str) -> str:
        (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
        return "[OK] initialized"

    handled = orch_task_handlers.handle_orch_task_command(
        cmd="orch-repair",
        args=argparse.Namespace(
            project_root=ROOT,
            manager_state_file=tmp_path / "manager_state.json",
            dry_run=False,
        ),
        manager_state=state,
        chat_id="939062873",
        orch_target="all",
        orch_add_name=None,
        orch_add_path=None,
        orch_add_overview=None,
        orch_add_init=True,
        orch_add_spawn=True,
        orch_add_set_active=True,
        rest="",
        orch_check_request_id=None,
        orch_task_request_id=None,
        orch_pick_request_id=None,
        orch_cancel_request_id=None,
        send=_send,
        log_event=lambda **kwargs: None,
        get_context=lambda target: (_ for _ in ()).throw(RuntimeError("not used")),
        latest_task_request_refs=lambda *args, **kwargs: [],
        set_chat_recent_task_refs=lambda *args, **kwargs: None,
        save_manager_state=lambda path, state: None,
        resolve_project_root=lambda raw: Path(raw).expanduser().resolve(),
        is_path_within=lambda path, root: True,
        register_orch_project=lambda *args, **kwargs: ("", {}),
        run_aoe_init=_run_aoe_init,
        run_aoe_spawn=lambda *args, **kwargs: "[SKIP] spawn",
        now_iso=lambda: "2026-03-07T18:30:00+0900",
        run_aoe_status=lambda p_args: "",
        resolve_chat_task_ref=lambda *args, **kwargs: "",
        resolve_task_request_id=lambda entry, ref: "",
        run_request_query=lambda *args, **kwargs: {},
        sync_task_lifecycle=lambda *args, **kwargs: None,
        resolve_verifier_candidates=lambda text: [],
        touch_chat_recent_task_ref=lambda *args, **kwargs: None,
        set_chat_selected_task_ref=lambda *args, **kwargs: None,
        get_chat_selected_task_ref=lambda *args, **kwargs: "",
        get_task_record=lambda *args, **kwargs: None,
        summarize_request_state=lambda *args, **kwargs: "",
        summarize_three_stage_request=lambda *args, **kwargs: "",
        summarize_task_lifecycle=lambda *args, **kwargs: "",
        task_display_label=lambda *args, **kwargs: "",
        cancel_request_assignments=lambda *args, **kwargs: {},
        lifecycle_set_stage=lambda *args, **kwargs: None,
        summarize_cancel_result=lambda *args, **kwargs: "",
    )

    assert handled is True
    assert messages
    assert "orch repair all finished" in messages[-1]
    assert "- projects: 3" in messages[-1]
    assert "- ready: 3" in messages[-1]


def test_orch_status_under_other_focus_returns_operator_message() -> None:
    state = _empty_state()
    messages = []

    handled = orch_task_handlers.handle_orch_task_command(
        cmd="orch-status",
        args=argparse.Namespace(
            project_root=ROOT,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            dry_run=True,
        ),
        manager_state=state,
        chat_id="939062873",
        orch_target="O2",
        orch_add_name=None,
        orch_add_path=None,
        orch_add_overview=None,
        orch_add_init=True,
        orch_add_spawn=True,
        orch_add_set_active=True,
        rest="",
        orch_check_request_id=None,
        orch_task_request_id=None,
        orch_pick_request_id=None,
        orch_cancel_request_id=None,
        send=lambda msg, **kwargs: messages.append(msg) or True,
        log_event=lambda **kwargs: None,
        get_context=lambda target: (_ for _ in ()).throw(
            RuntimeError("project lock active: O4 (local_map_analysis). requested=O2 (twinpaper). use /focus off or /focus O4")
        ),
        latest_task_request_refs=lambda *args, **kwargs: [],
        set_chat_recent_task_refs=lambda *args, **kwargs: None,
        save_manager_state=lambda path, state: None,
        resolve_project_root=lambda raw: Path(raw).expanduser().resolve(),
        is_path_within=lambda path, root: True,
        register_orch_project=lambda *args, **kwargs: ("", {}),
        run_aoe_init=lambda *args, **kwargs: "",
        run_aoe_spawn=lambda *args, **kwargs: "",
        now_iso=lambda: "2026-03-07T18:30:00+0900",
        run_aoe_status=lambda p_args: "",
        resolve_chat_task_ref=lambda *args, **kwargs: "",
        resolve_task_request_id=lambda entry, ref: "",
        run_request_query=lambda *args, **kwargs: {},
        sync_task_lifecycle=lambda *args, **kwargs: None,
        resolve_verifier_candidates=lambda text: [],
        touch_chat_recent_task_ref=lambda *args, **kwargs: None,
        set_chat_selected_task_ref=lambda *args, **kwargs: None,
        get_chat_selected_task_ref=lambda *args, **kwargs: "",
        get_task_record=lambda *args, **kwargs: None,
        summarize_request_state=lambda *args, **kwargs: "",
        summarize_three_stage_request=lambda *args, **kwargs: "",
        summarize_task_lifecycle=lambda *args, **kwargs: "",
        task_display_label=lambda *args, **kwargs: "",
        cancel_request_assignments=lambda *args, **kwargs: {},
        lifecycle_set_stage=lambda *args, **kwargs: None,
        summarize_cancel_result=lambda *args, **kwargs: "",
    )

    assert handled is True
    assert messages
    assert "orch status blocked by project lock" in messages[-1]


def test_todo_reply_markup_contains_run_done_and_drilldown_actions() -> None:
    entry = {"project_alias": "O2"}
    active_rows = [
        {"id": "TODO-001", "status": "blocked", "blocked_bucket": "manual_followup"},
        {"id": "TODO-002", "status": "running"},
        {"id": "TODO-003", "status": "blocked"},
    ]

    markup = todo_handlers._todo_reply_markup("twinpaper", entry, active_rows)

    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    for expected in [
        "/todo next",
        "/todo followup",
        "/orch status O2",
        "/sync preview O2 1h",
        "/todo ackrun 1",
        "/todo ack 1",
        "/todo done 1",
        "/todo done 2",
        "/todo done 3",
        "/sync O2 1h",
        "/queue",
        "/next",
        "/map",
        "/help",
    ]:
        assert expected in buttons


def test_todo_reply_markup_omits_ack_buttons_without_manual_followup() -> None:
    entry = {"project_alias": "O2"}
    active_rows = [
        {"id": "TODO-001", "status": "open"},
        {"id": "TODO-002", "status": "running"},
        {"id": "TODO-003", "status": "blocked"},
    ]

    markup = todo_handlers._todo_reply_markup("twinpaper", entry, active_rows)

    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/todo ack 1" not in buttons


def test_todo_list_shows_block_count_and_reason(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "need owner input",
                        "priority": "P1",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                        "blocked_reason": "plan gate: critic unresolved after auto-replan",
                    }
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    assert "manual_followup:" in sent[-1]
    assert "blocked x2 [manual_followup] | plan gate: critic unresolved after auto-replan" in sent[-1]


def test_todo_followup_lists_only_manual_followup_rows(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "need owner input",
                        "priority": "P1",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                        "blocked_reason": "plan gate: critic unresolved after auto-replan",
                    },
                    {
                        "id": "TODO-002",
                        "summary": "regular open task",
                        "priority": "P2",
                        "status": "open",
                    },
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="followup",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1]
    assert "todo followup: count=1 active=2" in text
    assert "manual_followup:" in text
    assert "tip: /todo ackrun <번호|TODO-xxx>" in text
    assert "TODO-001" in text
    assert "TODO-002" not in text


def test_todo_syncback_preview_reports_done_append_and_blocked_notes(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text(
        "# Tasks\n- [ ] phase1 rerun\n- [ ] existing backlog\n",
        encoding="utf-8",
    )
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "updated_at": "2026-03-07T21:00:00+0900",
                "todos": [
                    {"id": "TODO-001", "summary": "phase1 rerun", "priority": "P1", "status": "done"},
                    {
                        "id": "TODO-002",
                        "summary": "need owner input",
                        "priority": "P2",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                        "blocked_reason": "plan gate: critic unresolved",
                    },
                ],
                "todo_proposals": [
                    {"id": "PROP-001", "summary": "accepted follow-up", "priority": "P2", "status": "accepted"}
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="syncback preview",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T21:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1]
    assert "todo syncback preview" in text
    assert "- orch: twinpaper (O2)" in text
    assert "- mark_done: 1" in text
    assert "- append_new: 1" in text
    assert "- blocked_notes: 1" in text
    assert "- - [ ] P2: accepted follow-up" in text


def test_todo_ack_reopens_blocked_followup_and_clears_blocked_meta(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "need owner input",
                        "priority": "P1",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                        "blocked_alerted_at": "2026-03-07T00:30:00+0900",
                        "blocked_reason": "plan gate: critic unresolved after auto-replan",
                    }
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="ack 1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result == {"terminal": True}
    row = manager_state["projects"]["twinpaper"]["todos"][0]
    assert row["status"] == "open"
    assert "blocked_count" not in row
    assert "blocked_bucket" not in row
    assert "blocked_alerted_at" not in row
    assert "blocked_reason" not in row
    assert sent
    assert "todo acknowledged" in sent[-1]
    assert "- reopened: yes" in sent[-1]
    assert "- cleared_followup: yes" in sent[-1]


def test_todo_ackrun_reopens_blocked_followup_and_returns_run_transition(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "need owner input",
                        "priority": "P1",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                        "blocked_alerted_at": "2026-03-07T00:30:00+0900",
                        "blocked_reason": "plan gate: critic unresolved after auto-replan",
                    }
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="ackrun 1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result["terminal"] is False
    assert result["cmd"] == "run"
    assert result["orch_target"] == "twinpaper"
    assert result["run_prompt"] == "need owner input"
    assert result["run_force_mode"] == "dispatch"
    assert result["run_auto_source"] == "todo-ackrun"
    row = manager_state["projects"]["twinpaper"]["todos"][0]
    assert row["status"] == "open"
    assert "blocked_count" not in row
    assert "blocked_bucket" not in row
    assert "blocked_alerted_at" not in row
    assert "blocked_reason" not in row
    assert manager_state["projects"]["twinpaper"]["pending_todo"]["todo_id"] == "TODO-001"
    assert sent
    assert "todo ackrun selected" in sent[-1]
    assert "- reopened: yes" in sent[-1]
    assert "- cleared_followup: yes" in sent[-1]


def test_todo_ack_rejects_non_blocked_row(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "regular open task",
                        "priority": "P2",
                        "status": "open",
                    }
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="ack 1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    assert "todo ack blocked: target is not blocked" in sent[-1]


def test_todo_ackrun_rejects_non_blocked_row(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "regular open task",
                        "priority": "P2",
                        "status": "open",
                    }
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="ackrun 1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    assert "todo ackrun blocked: target is not blocked" in sent[-1]


def test_merge_todo_proposals_dedupes_existing_open_proposals_and_todos() -> None:
    entry = {
        "todos": [
            {"id": "TODO-001", "summary": "existing todo"},
        ],
        "todo_seq": 1,
        "todo_proposals": [
            {
                "id": "PROP-001",
                "summary": "existing proposal",
                "priority": "P2",
                "kind": "followup",
                "status": "open",
            }
        ],
        "todo_proposal_seq": 1,
    }

    merged = todo_handlers.merge_todo_proposals(
        entry=entry,
        request_id="REQ-123",
        task={"short_id": "T-123"},
        source_todo_id="TODO-009",
        proposals_data=[
            {"summary": "existing proposal", "priority": "P1", "kind": "followup", "reason": "dup", "confidence": 0.9},
            {"summary": "existing todo", "priority": "P2", "kind": "risk", "reason": "dup", "confidence": 0.6},
            {"summary": "new actionable follow-up", "priority": "P1", "kind": "followup", "reason": "new", "confidence": 0.8},
        ],
        now_iso=lambda: "2026-03-09T09:00:00+0900",
    )

    assert merged["created_count"] == 1
    assert merged["duplicate_count"] == 2
    assert entry["todo_proposals"][-1]["summary"] == "new actionable follow-up"
    assert entry["todo_proposals"][-1]["source_request_id"] == "REQ-123"


def test_todo_state_module_matches_handler_merge_and_syncback_preview(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text(
        "# Tasks\n- [ ] phase1 rerun\n- [ ] existing backlog\n",
        encoding="utf-8",
    )

    entry_a = {
        "project_root": str(project_root),
        "updated_at": "2026-03-09T09:00:00+0900",
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "priority": "P1", "status": "done"},
            {
                "id": "TODO-002",
                "summary": "need owner input",
                "priority": "P2",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "plan gate: critic unresolved",
            },
        ],
        "todo_seq": 2,
        "todo_proposals": [
            {"id": "PROP-001", "summary": "accepted follow-up", "priority": "P2", "status": "accepted"}
        ],
        "todo_proposal_seq": 1,
    }
    entry_b = copy.deepcopy(entry_a)

    merge_kwargs = dict(
        request_id="REQ-123",
        task={"short_id": "T-123"},
        source_todo_id="TODO-009",
        proposals_data=[
            {"summary": "existing backlog", "priority": "P2", "kind": "risk", "reason": "dup", "confidence": 0.6},
            {"summary": "new actionable follow-up", "priority": "P1", "kind": "followup", "reason": "new", "confidence": 0.8},
        ],
        now_iso=lambda: "2026-03-09T09:00:00+0900",
    )

    merged_a = todo_handlers.merge_todo_proposals(entry=entry_a, **merge_kwargs)
    merged_b = todo_state.merge_todo_proposals(entry=entry_b, **merge_kwargs)

    assert merged_a == merged_b
    assert entry_a == entry_b

    plan_a = todo_handlers._preview_syncback_plan(entry_a)
    plan_b = todo_state.preview_syncback_plan(entry_b)
    assert plan_a == plan_b


def test_todo_state_module_matches_handler_accept_and_reject_mutations(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    base_entry = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "todos": [],
        "todo_seq": 0,
        "todo_proposals": [
            {
                "id": "PROP-001",
                "summary": "write release checklist",
                "priority": "P1",
                "kind": "handoff",
                "status": "open",
                "reason": "deployment notes are missing",
                "confidence": 0.9,
                "source_request_id": "REQ-100",
                "source_todo_id": "TODO-000",
            },
            {
                "id": "PROP-002",
                "summary": "collect schema debt",
                "priority": "P2",
                "kind": "debt",
                "status": "open",
                "reason": "schema drift remains",
                "confidence": 0.7,
                "source_request_id": "REQ-101",
            },
        ],
        "todo_proposal_seq": 2,
    }
    manager_state = {"projects": {"twinpaper": copy.deepcopy(base_entry)}}
    entry_state = copy.deepcopy(base_entry)
    sent: list[str] = []

    def _ctx(_raw: str):
        return ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir))

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="accept 1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=_ctx,
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-09T10:00:00+0900",
    )
    assert result == {"terminal": True}

    state_accept = todo_state.accept_todo_proposal(
        entry=entry_state,
        proposal=entry_state["todo_proposals"][0],
        actor="telegram:939062873",
        now="2026-03-09T10:00:00+0900",
    )
    assert state_accept["todo_id"] == "TODO-001"
    assert manager_state["projects"]["twinpaper"] == entry_state

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="reject 1 duplicate debt",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=_ctx,
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-09T10:05:00+0900",
    )
    assert result == {"terminal": True}

    state_reject = todo_state.reject_todo_proposal(
        entry=entry_state,
        proposal=entry_state["todo_proposals"][1],
        actor="telegram:939062873",
        now="2026-03-09T10:05:00+0900",
        reason="duplicate debt",
    )
    assert state_reject["reason"] == "duplicate debt"
    assert manager_state["projects"]["twinpaper"] == entry_state


def test_todo_policy_helpers_cover_syncback_and_proposal_to_todo_rules() -> None:
    row = {
        "id": "TODO-010",
        "summary": "prepare deployment checklist",
        "priority": "P1",
        "status": "running",
        "created_by": "tf-proposal",
        "created_from_request_id": "REQ-900",
    }
    assert todo_policy.todo_row_syncback_target_status(row) == "open"
    assert todo_policy.todo_row_syncback_appendable(row) is True

    done_row = {"summary": "phase1 rerun", "priority": "P2", "status": "done"}
    assert todo_policy.todo_row_syncback_target_status(done_row) == "done"
    assert todo_policy.format_canonical_todo_line("P2", "phase1 rerun", status="done") == "- [x] P2: phase1 rerun"

    proposal = {
        "id": "PROP-001",
        "summary": "write release checklist",
        "priority": "P1",
        "kind": "handoff",
        "status": "accepted",
        "source_request_id": "REQ-100",
        "source_todo_id": "TODO-000",
        "source_file": "docs/handoff.md",
        "source_section": "Next steps",
        "source_reason": "handoff section bullet",
        "source_line": 12,
    }
    todo_row = todo_policy.proposal_to_todo_row(proposal, todo_id="TODO-001", now="2026-03-09T10:00:00+0900")
    assert todo_row["proposal_id"] == "PROP-001"
    assert todo_row["proposal_kind"] == "handoff"
    assert todo_row["created_from_request_id"] == "REQ-100"
    assert todo_row["source_file"] == "docs/handoff.md"
    assert todo_row["source_line"] == 12

    accepted = todo_policy.accepted_proposals_for_syncback(
        [
            {"id": "PROP-001", "status": "accepted"},
            {"id": "PROP-002", "status": "open"},
            {"id": "PROP-003", "status": "rejected"},
        ]
    )
    assert [row["id"] for row in accepted] == ["PROP-001"]


def test_todo_proposals_list_accept_and_reject_flow(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [],
                "todo_seq": 0,
                "todo_proposals": [
                    {
                        "id": "PROP-001",
                        "summary": "write release checklist",
                        "priority": "P1",
                        "kind": "handoff",
                        "status": "open",
                        "reason": "deployment notes are missing",
                        "confidence": 0.9,
                        "source_request_id": "REQ-100",
                        "source_todo_id": "TODO-000",
                    },
                    {
                        "id": "PROP-002",
                        "summary": "collect schema debt",
                        "priority": "P2",
                        "kind": "debt",
                        "status": "open",
                        "reason": "schema drift remains",
                        "confidence": 0.7,
                        "source_request_id": "REQ-101",
                    },
                ],
                "todo_proposal_seq": 2,
            }
        }
    }
    sent: list[tuple[str, dict | None]] = []

    def _send(body: str, **kwargs) -> bool:
        sent.append((body, kwargs.get("reply_markup")))
        return True

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="proposals",
        send=_send,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-09T09:00:00+0900",
    )

    assert result == {"terminal": True}
    assert "todo proposals: open=2" in sent[-1][0]
    buttons = [btn["text"] for row in (sent[-1][1] or {}).get("keyboard", []) for btn in row]
    assert "/todo accept 1" in buttons
    assert "/todo reject 1" in buttons

    sent.clear()
    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="accept 1",
        send=_send,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-09T09:05:00+0900",
    )

    assert result == {"terminal": True}
    proposal = manager_state["projects"]["twinpaper"]["todo_proposals"][0]
    assert proposal["status"] == "accepted"
    assert proposal["accepted_todo_id"] == "TODO-001"
    assert manager_state["projects"]["twinpaper"]["todos"][0]["proposal_id"] == "PROP-001"
    assert "todo proposal accepted" in sent[-1][0]

    sent.clear()
    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="reject 1 duplicate debt",
        send=_send,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-09T09:10:00+0900",
    )

    assert result == {"terminal": True}
    proposal = manager_state["projects"]["twinpaper"]["todo_proposals"][1]
    assert proposal["status"] == "rejected"
    assert proposal["rejected_reason"] == "duplicate debt"
    assert "todo proposal rejected" in sent[-1][0]


def test_todo_next_pending_includes_ok_and_clear_pending_buttons(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "pending_todo": {"todo_id": "TODO-009", "chat_id": "939062873", "selected_at": "2026-03-07T00:50:00+0900"},
                "todos": [
                    {"id": "TODO-001", "summary": "regular open task", "priority": "P2", "status": "open"},
                ],
            }
        }
    }
    sent: list[tuple[str, dict | None]] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="next",
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text, markup = sent[-1]
    assert "todo next blocked: pending todo exists" in text
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/ok" in buttons
    assert "/clear pending" in buttons
    assert "/todo next force" in buttons
    assert "/todo O2" in buttons


def test_todo_next_ignores_blocked_rows_when_open_todo_exists(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "blocked first",
                        "priority": "P1",
                        "status": "blocked",
                    },
                    {
                        "id": "TODO-002",
                        "summary": "open second",
                        "priority": "P2",
                        "status": "open",
                    },
                ],
            }
        }
    }
    saved: list[Path] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="next",
        send=lambda body, **kwargs: True,
        get_context=lambda raw: (
            "twinpaper",
            manager_state["projects"]["twinpaper"],
            argparse.Namespace(project_root=project_root, team_dir=team_dir),
        ),
        save_manager_state=lambda path, manager_state: saved.append(path),
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result["terminal"] is False
    assert result["cmd"] == "run"
    assert result["orch_target"] == "twinpaper"
    assert result["run_prompt"] == "open second"
    assert result["run_force_mode"] == "dispatch"
    pending = manager_state["projects"]["twinpaper"]["pending_todo"]
    assert pending["todo_id"] == "TODO-002"
    assert saved == [team_dir / "orch_manager_state.json"]


def test_todo_ackrun_pending_includes_ok_and_force_buttons(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "pending_todo": {"todo_id": "TODO-009", "chat_id": "939062873", "selected_at": "2026-03-07T00:50:00+0900"},
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "need owner input",
                        "priority": "P1",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                    }
                ],
            }
        }
    }
    sent: list[tuple[str, dict | None]] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="ackrun 1",
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text, markup = sent[-1]
    assert "todo ackrun blocked: pending todo exists" in text
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/ok" in buttons
    assert "/clear pending" in buttons
    assert "/todo O2 ackrun TODO-001 force" in buttons
    assert "/todo O2" in buttons


def test_confirm_required_reply_markup_contains_ok_cancel_and_clear_pending() -> None:
    state = _empty_state()
    sent: list[tuple[str, dict | None]] = []
    saved: list[Path] = []

    handled = run_handlers._handle_run_rate_limit_and_confirm(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            chat_max_running=3,
            chat_daily_cap=20,
        ),
        manager_state=state,
        chat_id="939062873",
        key="twinpaper",
        entry={"project_alias": "O2"},
        run_auto_source="default",
        run_force_mode="dispatch",
        orch_target="O2",
        prompt="rm -rf /tmp/demo",
        summarize_chat_usage=lambda manager_state, chat_id: (0, 0),
        detect_high_risk_prompt=lambda prompt: "destructive_delete",
        set_confirm_action=lambda *args, **kwargs: gw.set_confirm_action(*args, **kwargs),
        save_manager_state=lambda path, manager_state: saved.append(path),
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
    )

    assert handled is True
    assert sent
    text, markup = sent[-1]
    assert "고위험 자동실행 감지" in text
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/ok" in buttons
    assert "/cancel" in buttons
    assert "/clear pending" in buttons


def test_confirm_required_for_orch_action_dispatch_prompt() -> None:
    state = _empty_state()
    sent: list[tuple[str, dict | None]] = []

    handled = run_handlers._handle_run_rate_limit_and_confirm(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            chat_max_running=3,
            chat_daily_cap=20,
        ),
        manager_state=state,
        chat_id="939062873",
        key="twinpaper",
        entry={"project_alias": "O2"},
        run_auto_source="orch-action:work",
        run_force_mode="dispatch",
        orch_target="O2",
        prompt="rm -rf /tmp/demo",
        summarize_chat_usage=lambda manager_state, chat_id: (0, 0),
        detect_high_risk_prompt=lambda prompt: "destructive_delete",
        set_confirm_action=lambda *args, **kwargs: gw.set_confirm_action(*args, **kwargs),
        save_manager_state=lambda path, manager_state: None,
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
    )

    assert handled is True
    assert sent
    assert "고위험 자동실행 감지" in sent[-1][0]


def test_confirmed_run_does_not_reprompt_high_risk_confirmation() -> None:
    state = _empty_state()
    sent: list[tuple[str, dict | None]] = []

    handled = run_handlers._handle_run_rate_limit_and_confirm(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            chat_max_running=3,
            chat_daily_cap=20,
        ),
        manager_state=state,
        chat_id="939062873",
        key="twinpaper",
        entry={"project_alias": "O2"},
        run_auto_source="confirmed",
        run_force_mode="dispatch",
        orch_target="O2",
        prompt="rm -rf /tmp/demo",
        summarize_chat_usage=lambda manager_state, chat_id: (0, 0),
        detect_high_risk_prompt=lambda prompt: "destructive_delete",
        set_confirm_action=lambda *args, **kwargs: gw.set_confirm_action(*args, **kwargs),
        save_manager_state=lambda path, manager_state: None,
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
    )

    assert handled is False
    assert not sent


def test_rate_limit_running_reply_markup_uses_project_context_actions() -> None:
    state = _empty_state()
    sent: list[tuple[str, str, dict | None]] = []

    handled = run_handlers._handle_run_rate_limit_and_confirm(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            chat_max_running=1,
            chat_daily_cap=20,
        ),
        manager_state=state,
        chat_id="939062873",
        key="twinpaper",
        entry={"project_alias": "O2"},
        run_auto_source="default",
        run_force_mode="dispatch",
        orch_target="O2",
        prompt="implement feature",
        summarize_chat_usage=lambda manager_state, chat_id: (1, 0),
        detect_high_risk_prompt=lambda prompt: "",
        set_confirm_action=lambda *args, **kwargs: None,
        save_manager_state=lambda path, manager_state: None,
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
    )

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "rate-limit-running"
    assert "동시 실행 한도를 초과했습니다" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/monitor" in buttons
    assert "/check" in buttons
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons


def test_rate_limit_daily_reply_markup_uses_global_actions_without_project_context() -> None:
    state = _empty_state()
    sent: list[tuple[str, str, dict | None]] = []

    handled = run_handlers._handle_run_rate_limit_and_confirm(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            chat_max_running=3,
            chat_daily_cap=1,
        ),
        manager_state=state,
        chat_id="939062873",
        key="",
        entry=None,
        run_auto_source="default",
        run_force_mode="dispatch",
        orch_target=None,
        prompt="implement feature",
        summarize_chat_usage=lambda manager_state, chat_id: (0, 1),
        detect_high_risk_prompt=lambda prompt: "",
        set_confirm_action=lambda *args, **kwargs: None,
        save_manager_state=lambda path, manager_state: None,
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
    )

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "rate-limit-daily"
    assert "일일 실행 한도에 도달했습니다" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/monitor" in buttons
    assert "/check" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons
    assert "/help" in buttons


def test_enforce_dispatch_policies_verifier_gate_setup_adds_project_quick_actions() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    result = run_handlers._enforce_dispatch_policies(
        dispatch_mode=True,
        args=argparse.Namespace(require_verifier=True),
        key="twinpaper",
        entry={"project_alias": "O2"},
        selected_roles=["Local-Dev"],
        available_roles=["Local-Dev"],
        verifier_candidates=["Reviewer"],
        plan_gate_blocked=False,
        plan_gate_reason="",
        plan_replans=[],
        ensure_verifier_roles=lambda **kwargs: (["Local-Dev"], [], False, []),
        dispatch_roles="Local-Dev",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )

    assert result.terminal is True
    assert result.terminal_reason == "verifier gate: no verifier role is available"
    assert sent
    context, body, markup = sent[-1]
    assert context == "verifier-gate setup"
    assert "no verifier role is available" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/monitor" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons


def test_enforce_dispatch_policies_planning_gate_adds_project_quick_actions() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    result = run_handlers._enforce_dispatch_policies(
        dispatch_mode=True,
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry={"project_alias": "O2"},
        selected_roles=["Local-Dev"],
        available_roles=["Local-Dev", "Reviewer"],
        verifier_candidates=["Reviewer"],
        plan_gate_blocked=True,
        plan_gate_reason="critic unresolved after auto-replan",
        plan_replans=[{"attempt": 1}],
        ensure_verifier_roles=lambda **kwargs: (["Local-Dev"], ["Reviewer"], False, ["Reviewer"]),
        dispatch_roles="Local-Dev",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )

    assert result.terminal is True
    assert result.terminal_reason == "plan gate: critic unresolved after auto-replan"
    assert sent
    context, body, markup = sent[-1]
    assert context == "planning-gate"
    assert "plan gate blocked" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/monitor" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons


def test_run_guards_module_matches_run_guard_exports() -> None:
    assert run_handlers._confirm_required_reply_markup() == run_guards.confirm_required_reply_markup()
    assert run_handlers._rate_limit_reply_markup({"project_alias": "O2"}, "twinpaper") == run_guards.rate_limit_reply_markup({"project_alias": "O2"}, "twinpaper")
    assert run_handlers._rate_limit_reply_markup(None, "") == run_guards.rate_limit_reply_markup(None, "")

    guard_run = run_handlers._resolve_effective_run_options(
        p_args=argparse.Namespace(priority="P2", orch_timeout_sec=120, no_wait=False),
        run_priority_override="P1",
        run_timeout_override=30,
        run_no_wait_override=True,
    )
    guard_mod = run_guards.resolve_effective_run_options(
        p_args=argparse.Namespace(priority="P2", orch_timeout_sec=120, no_wait=False),
        run_priority_override="P1",
        run_timeout_override=30,
        run_no_wait_override=True,
    )
    assert guard_run == guard_mod

    preview_run = run_handlers._build_dry_run_preview(
        key="twinpaper",
        dispatch_mode=True,
        prompt="implement feature",
        dispatch_roles="Reviewer",
        require_verifier=True,
        verifier_roles=["Reviewer"],
        verifier_added=False,
        run_control_mode="retry",
        run_source_request_id="REQ-1",
        planning_enabled=True,
        reuse_source_plan=False,
        plan_data={"subtasks": [{"id": "S1"}]},
        plan_replans=[{"attempt": 1}],
        plan_gate_blocked=False,
        plan_error="",
        effective_priority="P1",
        effective_timeout=60,
        effective_no_wait=False,
    )
    preview_mod = run_guards.build_dry_run_preview(
        key="twinpaper",
        dispatch_mode=True,
        prompt="implement feature",
        dispatch_roles="Reviewer",
        require_verifier=True,
        verifier_roles=["Reviewer"],
        verifier_added=False,
        run_control_mode="retry",
        run_source_request_id="REQ-1",
        planning_enabled=True,
        reuse_source_plan=False,
        plan_data={"subtasks": [{"id": "S1"}]},
        plan_replans=[{"attempt": 1}],
        plan_gate_blocked=False,
        plan_error="",
        effective_priority="P1",
        effective_timeout=60,
        effective_no_wait=False,
    )
    assert preview_run == preview_mod

    sent_run: list[tuple[str, str, dict | None]] = []
    sent_mod: list[tuple[str, str, dict | None]] = []
    logged_run: list[dict] = []
    logged_mod: list[dict] = []
    common = dict(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            chat_max_running=1,
            chat_daily_cap=20,
        ),
        manager_state=_empty_state(),
        chat_id="939062873",
        key="twinpaper",
        entry={"project_alias": "O2"},
        run_auto_source="default",
        run_force_mode="dispatch",
        orch_target="O2",
        prompt="implement feature",
        summarize_chat_usage=lambda manager_state, chat_id: (1, 0),
        detect_high_risk_prompt=lambda prompt: "",
        set_confirm_action=lambda *args, **kwargs: None,
        save_manager_state=lambda path, manager_state: None,
    )
    handled_run = run_handlers._handle_run_rate_limit_and_confirm(
        send=lambda body, **kwargs: sent_run.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged_run.append(kwargs),
        **common,
    )
    handled_mod = run_guards.handle_run_rate_limit_and_confirm(
        send=lambda body, **kwargs: sent_mod.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged_mod.append(kwargs),
        **common,
    )
    assert handled_run == handled_mod == True
    assert sent_run == sent_mod
    assert logged_run == logged_mod

    policy_sent_run: list[tuple[str, str, dict | None]] = []
    policy_sent_mod: list[tuple[str, str, dict | None]] = []
    policy_run = run_handlers._enforce_dispatch_policies(
        dispatch_mode=True,
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry={"project_alias": "O2"},
        selected_roles=["Local-Dev"],
        available_roles=["Local-Dev", "Reviewer"],
        verifier_candidates=["Reviewer"],
        plan_gate_blocked=True,
        plan_gate_reason="critic unresolved after auto-replan",
        plan_replans=[{"attempt": 1}],
        ensure_verifier_roles=lambda **kwargs: (["Local-Dev"], ["Reviewer"], False, ["Reviewer"]),
        dispatch_roles="Local-Dev",
        send=lambda body, **kwargs: policy_sent_run.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    policy_mod = run_guards.enforce_dispatch_policies(
        dispatch_mode=True,
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry={"project_alias": "O2"},
        selected_roles=["Local-Dev"],
        available_roles=["Local-Dev", "Reviewer"],
        verifier_candidates=["Reviewer"],
        plan_gate_blocked=True,
        plan_gate_reason="critic unresolved after auto-replan",
        plan_replans=[{"attempt": 1}],
        ensure_verifier_roles=lambda **kwargs: (["Local-Dev"], ["Reviewer"], False, ["Reviewer"]),
        dispatch_roles="Local-Dev",
        send=lambda body, **kwargs: policy_sent_mod.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    assert policy_run == policy_mod
    assert policy_sent_run == policy_sent_mod

    confirm_state_a = _empty_state()
    confirm_state_b = copy.deepcopy(confirm_state_a)
    gw.set_confirm_action(confirm_state_a, chat_id="939062873", mode="dispatch", prompt="rm -rf /tmp/demo", risk="destructive_delete")
    gw.set_confirm_action(confirm_state_b, chat_id="939062873", mode="dispatch", prompt="rm -rf /tmp/demo", risk="destructive_delete")
    saved_a: list[Path] = []
    saved_b: list[Path] = []
    confirm_sent_a: list[tuple[str, dict]] = []
    confirm_sent_b: list[tuple[str, dict]] = []

    result_a = run_handlers.resolve_confirm_run_transition(
        cmd="confirm-run",
        args=argparse.Namespace(dry_run=False, manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json", confirm_ttl_sec=300),
        manager_state=confirm_state_a,
        chat_id="939062873",
        orch_target="O2",
        send=lambda body, **kwargs: confirm_sent_a.append((body, kwargs)) or True,
        get_confirm_action=gw.get_confirm_action,
        parse_iso_ts=gw.parse_iso_ts,
        clear_confirm_action=gw.clear_confirm_action,
        save_manager_state=lambda path, manager_state: saved_a.append(path),
    )
    result_b = run_guards.resolve_confirm_run_transition(
        cmd="confirm-run",
        args=argparse.Namespace(dry_run=False, manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json", confirm_ttl_sec=300),
        manager_state=confirm_state_b,
        chat_id="939062873",
        orch_target="O2",
        send=lambda body, **kwargs: confirm_sent_b.append((body, kwargs)) or True,
        get_confirm_action=gw.get_confirm_action,
        parse_iso_ts=gw.parse_iso_ts,
        clear_confirm_action=gw.clear_confirm_action,
        save_manager_state=lambda path, manager_state: saved_b.append(path),
    )
    assert result_a == result_b
    assert confirm_state_a == confirm_state_b
    assert confirm_sent_a == confirm_sent_b
    assert saved_a == saved_b


def test_send_dispatch_result_adds_project_quick_actions_for_confirmed_result() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    handled = run_handlers._send_dispatch_result(
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry={"project_alias": "O2"},
        p_args=argparse.Namespace(),
        prompt="dangerous but approved",
        state={"complete": True},
        req_id="REQ-1",
        task=None,
        run_control_mode="normal",
        run_source_request_id="",
        run_auto_source="confirmed",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
        summarize_task_lifecycle=lambda key, task: "",
        synthesize_orchestrator_response=lambda p_args, prompt, state: "synthed",
        render_run_response=lambda state, task=None: "run result",
        finalize_request_reply_messages=lambda args, req_id: {},
    )

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "result"
    assert body == "run result"
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/todo O2" in buttons
    assert "/orch status O2" in buttons
    assert "/monitor" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons


def test_send_dispatch_result_adds_project_quick_actions_for_confirmed_synth() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    handled = run_handlers._send_dispatch_result(
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry={"project_alias": "O2"},
        p_args=argparse.Namespace(),
        prompt="dangerous but approved",
        state={"complete": True, "replies": [{"role": "Reviewer", "text": "ok"}]},
        req_id="REQ-1",
        task=None,
        run_control_mode="normal",
        run_source_request_id="",
        run_auto_source="confirmed",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
        summarize_task_lifecycle=lambda key, task: "",
        synthesize_orchestrator_response=lambda p_args, prompt, state: "synthed",
        render_run_response=lambda state, task=None: "run result",
        finalize_request_reply_messages=lambda args, req_id: {},
    )

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "synth"
    assert body == "synthed"
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/todo O2" in buttons
    assert "/orch status O2" in buttons
    assert "/monitor" in buttons


def test_send_dispatch_result_adds_project_quick_actions_for_verifier_gate_failed() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    handled = run_handlers._send_dispatch_result(
        args=argparse.Namespace(require_verifier=True),
        key="twinpaper",
        entry={"project_alias": "O2"},
        p_args=argparse.Namespace(),
        prompt="needs verification",
        state={"complete": True},
        req_id="REQ-9",
        task={"stages": {"verification": "failed"}},
        run_control_mode="normal",
        run_source_request_id="",
        run_auto_source="todo-next",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
        summarize_task_lifecycle=lambda key, task: "verification failed summary",
        synthesize_orchestrator_response=lambda p_args, prompt, state: "synthed",
        render_run_response=lambda state, task=None: "run result",
        finalize_request_reply_messages=lambda args, req_id: {},
    )

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "verifier-gate failed"
    assert body == "verification failed summary"
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/task REQ-9" in buttons
    assert "/replan REQ-9" in buttons
    assert "/retry REQ-9" in buttons
    assert "/todo O2" in buttons
    assert "/orch status O2" in buttons
    assert "/monitor" in buttons


def test_send_exec_critic_intervention_adds_project_quick_actions() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    run_handlers._send_exec_critic_intervention(
        entry={"project_alias": "O2"},
        key="twinpaper",
        final_req_id="REQ-7",
        verdict="retry",
        reason="critic unresolved after repair",
        exec_attempt=2,
        exec_max_attempts=3,
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )

    assert sent
    context, body, markup = sent[-1]
    assert context == "exec-critic"
    assert "exec critic: intervention needed" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/task REQ-7" in buttons
    assert "/replan REQ-7" in buttons
    assert "/retry REQ-7" in buttons
    assert "/todo O2" in buttons
    assert "/orch status O2" in buttons
    assert "/monitor" in buttons


def test_send_dispatch_exception_adds_project_quick_actions() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    run_handlers._send_dispatch_exception(
        entry={"project_alias": "O2"},
        key="twinpaper",
        todo_id="TODO-001",
        reason="missing orchestrator.json",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )

    assert sent
    context, body, markup = sent[-1]
    assert context == "dispatch-exception"
    assert "dispatch failed before request start" in body
    assert "- reason: missing orchestrator.json" in body
    assert "- todo: TODO-001" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/monitor" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons


def test_exec_results_module_matches_run_response_exports() -> None:
    entry = {"project_alias": "O2"}
    assert run_handlers._confirmed_result_reply_markup(entry, "twinpaper") == exec_results.confirmed_result_reply_markup(entry, "twinpaper")
    assert run_handlers._early_gate_reply_markup(entry, "twinpaper") == exec_results.early_gate_reply_markup(entry, "twinpaper")
    assert run_handlers._intervention_reply_markup(entry, "twinpaper", "REQ-9") == exec_results.intervention_reply_markup(entry, "twinpaper", "REQ-9")

    sent_run: list[tuple[str, str, dict | None]] = []
    sent_mod: list[tuple[str, str, dict | None]] = []
    run_handlers._send_exec_critic_intervention(
        entry=entry,
        key="twinpaper",
        final_req_id="REQ-7",
        verdict="retry",
        reason="critic unresolved after repair",
        exec_attempt=2,
        exec_max_attempts=3,
        send=lambda body, **kwargs: sent_run.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    exec_results.send_exec_critic_intervention(
        entry=entry,
        key="twinpaper",
        final_req_id="REQ-7",
        verdict="retry",
        reason="critic unresolved after repair",
        exec_attempt=2,
        exec_max_attempts=3,
        send=lambda body, **kwargs: sent_mod.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    assert sent_run == sent_mod

    exc_run: list[tuple[str, str, dict | None]] = []
    exc_mod: list[tuple[str, str, dict | None]] = []
    run_handlers._send_dispatch_exception(
        entry=entry,
        key="twinpaper",
        todo_id="TODO-001",
        reason="missing orchestrator.json",
        send=lambda body, **kwargs: exc_run.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    exec_results.send_dispatch_exception(
        entry=entry,
        key="twinpaper",
        todo_id="TODO-001",
        reason="missing orchestrator.json",
        send=lambda body, **kwargs: exc_mod.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    assert exc_run == exc_mod

    result_run: list[tuple[str, str, dict | None]] = []
    result_mod: list[tuple[str, str, dict | None]] = []
    log_run: list[dict] = []
    log_mod: list[dict] = []
    common = dict(
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry=entry,
        p_args=argparse.Namespace(),
        prompt="dangerous but approved",
        state={"complete": True, "replies": [{"role": "Reviewer", "text": "ok"}]},
        req_id="REQ-1",
        task=None,
        run_control_mode="normal",
        run_source_request_id="",
        run_auto_source="confirmed",
        summarize_task_lifecycle=lambda key, task: "",
        synthesize_orchestrator_response=lambda p_args, prompt, state: "synthed",
        render_run_response=lambda state, task=None: "run result",
        finalize_request_reply_messages=lambda args, req_id: {},
    )
    handled_run = run_handlers._send_dispatch_result(
        send=lambda body, **kwargs: result_run.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: log_run.append(kwargs),
        **common,
    )
    handled_mod = exec_results.send_dispatch_result(
        send=lambda body, **kwargs: result_mod.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: log_mod.append(kwargs),
        **common,
    )
    assert handled_run == handled_mod == True
    assert result_run == result_mod
    assert log_run == log_mod


def test_handle_run_or_unknown_command_sends_dispatch_exception_and_returns_true(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {"id": "TODO-001", "summary": "open row", "priority": "P1", "status": "open"},
                ],
                "pending_todo": {"todo_id": "TODO-001", "chat_id": "939062873", "selected_at": "2026-03-07T00:00:00+0900"},
            }
        }
    }
    sent: list[tuple[str, str, dict | None]] = []
    logged: list[dict] = []
    saved: list[Path] = []

    ctx = run_handlers.build_run_context(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=team_dir / "orch_manager_state.json",
            auto_dispatch=False,
            require_verifier=False,
            verifier_roles="",
            task_planning=False,
            plan_max_subtasks=6,
            plan_auto_replan=False,
            plan_replan_attempts=0,
            plan_block_on_critic=False,
            exec_critic=False,
            exec_critic_retry_max=3,
            chat_max_running=3,
            chat_daily_cap=20,
        ),
        manager_state=manager_state,
        chat_id="939062873",
        text="open row",
        rest="open row",
        orch_target="O2",
        run_prompt="open row",
        run_roles_override=None,
        run_priority_override=None,
        run_timeout_override=None,
        run_no_wait_override=None,
        run_force_mode="dispatch",
        run_auto_source="todo-next",
        run_control_mode="normal",
        run_source_request_id="",
        run_source_task={"todo_id": "TODO-001"},
    )

    deps = run_handlers.RunDeps(
        core=run_handlers.RunCoreDeps(
            send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
            log_event=lambda **kwargs: logged.append(kwargs),
            help_text=lambda: "help",
        ),
        guard=run_handlers.RunGuardDeps(
            summarize_chat_usage=lambda manager_state, chat_id: (0, 0),
            detect_high_risk_prompt=lambda prompt: "",
            set_confirm_action=lambda *args, **kwargs: None,
            save_manager_state=lambda path, manager_state: saved.append(path),
        ),
        planning=run_handlers.RunPlanningDeps(
            choose_auto_dispatch_roles=lambda *args, **kwargs: ["Local-Dev"],
            resolve_verifier_candidates=lambda text: [],
            load_orchestrator_roles=lambda team_dir: ["Local-Dev"],
            parse_roles_csv=lambda csv: [token for token in str(csv or "").split(",") if token],
            ensure_verifier_roles=lambda **kwargs: (kwargs.get("selected_roles", []), [], False, []),
            available_worker_roles=lambda roles: roles,
            normalize_task_plan_payload=lambda payload, **kwargs: payload or {},
            build_task_execution_plan=lambda **kwargs: {},
            critique_task_execution_plan=lambda **kwargs: {"approved": True, "issues": [], "recommendations": []},
            critic_has_blockers=lambda critic: False,
            repair_task_execution_plan=lambda **kwargs: {},
            plan_roles_from_subtasks=lambda payload: [],
            build_planned_dispatch_prompt=lambda prompt, plan_data, plan_critic: prompt,
            phase1_ensemble_planning=lambda *args, **kwargs: {},
        ),
        routing=run_handlers.RunRoutingDeps(
            get_context=lambda raw: (
                "twinpaper",
                manager_state["projects"]["twinpaper"],
                argparse.Namespace(
                    project_root=project_root,
                    team_dir=team_dir,
                    roles="Local-Dev",
                    priority="P2",
                    orch_timeout_sec=120,
                    no_wait=False,
                ),
            ),
            run_orchestrator_direct=lambda p_args, prompt: "direct",
            run_aoe_orch=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("missing orchestrator.json")),
            create_request_id=lambda: "REQ-EXC",
            ensure_task_record=lambda **kwargs: {},
            finalize_request_reply_messages=lambda *args, **kwargs: {},
            touch_chat_recent_task_ref=lambda *args, **kwargs: None,
            set_chat_selected_task_ref=lambda *args, **kwargs: None,
            now_iso=lambda: "2026-03-07T01:00:00+0900",
            sync_task_lifecycle=lambda **kwargs: None,
            lifecycle_set_stage=lambda *args, **kwargs: None,
            summarize_task_lifecycle=lambda key, task: "",
            synthesize_orchestrator_response=lambda p_args, prompt, state: "",
            critique_task_result=lambda **kwargs: {"verdict": "success", "reason": ""},
            extract_todo_proposals=lambda *args, **kwargs: [],
            merge_todo_proposals=lambda **kwargs: {"created_count": 0, "created_ids": [], "duplicate_count": 0, "skipped_count": 0},
            render_run_response=lambda state, task=None: "result",
        ),
    )

    handled = run_handlers.handle_run_or_unknown_command(ctx=ctx, deps=deps)

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "dispatch-exception"
    assert "missing orchestrator.json" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert manager_state["projects"]["twinpaper"]["todos"][0]["status"] == "blocked"
    assert "pending_todo" not in manager_state["projects"]["twinpaper"]
    assert saved
    assert any(evt.get("event") == "dispatch_failed" and evt.get("error_code") == "E_DISPATCH" for evt in logged)


def test_handle_run_or_unknown_command_materializes_provisional_task_before_plan_gate(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [],
            }
        }
    }
    sent: list[tuple[str, str, dict | None]] = []
    logged: list[dict] = []
    saved: list[Path] = []

    ctx = run_handlers.build_run_context(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=team_dir / "orch_manager_state.json",
            auto_dispatch=False,
            require_verifier=False,
            verifier_roles="",
            task_planning=True,
            plan_phase1_ensemble=True,
            plan_max_subtasks=6,
            plan_auto_replan=False,
            plan_replan_attempts=0,
            plan_block_on_critic=True,
            exec_critic=False,
            exec_critic_retry_max=3,
            chat_max_running=3,
            chat_daily_cap=20,
        ),
        manager_state=manager_state,
        chat_id="939062873",
        text="plan it",
        rest="plan it",
        orch_target="O2",
        run_prompt="plan it",
        run_roles_override=None,
        run_priority_override=None,
        run_timeout_override=None,
        run_no_wait_override=None,
        run_force_mode="dispatch",
        run_auto_source="default",
        run_control_mode="normal",
        run_source_request_id="",
        run_source_task=None,
    )

    deps = run_handlers.RunDeps(
        core=run_handlers.RunCoreDeps(
            send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
            log_event=lambda **kwargs: logged.append(kwargs),
            help_text=lambda: "help",
        ),
        guard=run_handlers.RunGuardDeps(
            summarize_chat_usage=lambda manager_state, chat_id: (0, 0),
            detect_high_risk_prompt=lambda prompt: "",
            set_confirm_action=lambda *args, **kwargs: None,
            save_manager_state=lambda path, manager_state: saved.append(path),
        ),
        planning=run_handlers.RunPlanningDeps(
            choose_auto_dispatch_roles=lambda *args, **kwargs: ["Local-Dev"],
            resolve_verifier_candidates=lambda text: [],
            load_orchestrator_roles=lambda team_dir: ["Local-Dev", "Reviewer"],
            parse_roles_csv=lambda csv: [token for token in str(csv or "").split(",") if token],
            ensure_verifier_roles=lambda **kwargs: (kwargs.get("selected_roles", []), [], False, []),
            available_worker_roles=lambda roles: roles,
            normalize_task_plan_payload=lambda payload, **kwargs: payload or {},
            build_task_execution_plan=lambda **kwargs: {},
            critique_task_execution_plan=lambda **kwargs: {"approved": True, "issues": [], "recommendations": []},
            critic_has_blockers=lambda critic: False,
            repair_task_execution_plan=lambda **kwargs: {},
            plan_roles_from_subtasks=lambda payload: [],
            build_planned_dispatch_prompt=lambda prompt, plan_data, plan_critic: prompt,
            phase1_ensemble_planning=lambda *args, **kwargs: (
                kwargs.get("report_progress") and kwargs["report_progress"](phase="planner", detail="phase1 round 1/3 provider=codex")
            ) or {
                "plan_data": {"summary": "blocked", "subtasks": []},
                "plan_critic": {"approved": False, "issues": [{"issue": "missing acceptance"}], "recommendations": []},
                "plan_roles": ["Local-Dev"],
                "plan_replans": [{"attempt": 1}],
                "plan_error": "",
                "plan_gate_blocked": True,
                "plan_gate_reason": "missing acceptance",
                "phase1_mode": "ensemble",
                "phase1_rounds": 3,
                "phase1_providers": ["codex", "claude"],
            },
        ),
        routing=run_handlers.RunRoutingDeps(
            get_context=lambda raw: (
                "twinpaper",
                manager_state["projects"]["twinpaper"],
                argparse.Namespace(
                    project_root=project_root,
                    team_dir=team_dir,
                    roles="Local-Dev",
                    priority="P2",
                    orch_timeout_sec=120,
                    no_wait=False,
                ),
            ),
            run_orchestrator_direct=lambda p_args, prompt: "direct",
            run_aoe_orch=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("dispatch should not run")),
            create_request_id=lambda: "REQ-PLAN",
            ensure_task_record=lambda **kwargs: gw.ensure_task_record(
                kwargs["entry"],
                kwargs["request_id"],
                kwargs["prompt"],
                kwargs["mode"],
                kwargs["roles"],
                kwargs["verifier_roles"],
                kwargs["require_verifier"],
            ),
            finalize_request_reply_messages=lambda *args, **kwargs: {},
            touch_chat_recent_task_ref=gw.touch_chat_recent_task_ref,
            set_chat_selected_task_ref=gw.set_chat_selected_task_ref,
            now_iso=lambda: "2026-03-12T23:40:00+0900",
            sync_task_lifecycle=lambda **kwargs: None,
            lifecycle_set_stage=gw.lifecycle_set_stage,
            summarize_task_lifecycle=lambda key, task: "",
            synthesize_orchestrator_response=lambda p_args, prompt, state: "",
            critique_task_result=lambda **kwargs: {"verdict": "success", "reason": ""},
            extract_todo_proposals=lambda *args, **kwargs: [],
            merge_todo_proposals=lambda **kwargs: {"created_count": 0, "created_ids": [], "duplicate_count": 0, "skipped_count": 0},
            render_run_response=lambda state, task=None: "result",
        ),
    )

    handled = run_handlers.handle_run_or_unknown_command(ctx=ctx, deps=deps)

    assert handled is True
    task = manager_state["projects"]["twinpaper"]["tasks"]["REQ-PLAN"]
    assert task["request_id"] == "REQ-PLAN"
    assert task["tf_phase"] == "blocked"
    assert task["tf_phase_reason"] == "missing acceptance"
    assert task["status"] == "failed"
    assert task["plan_gate_reason"] == "missing acceptance"
    assert task["stages"]["planning"] == "failed"
    assert task["stages"]["close"] == "failed"
    assert manager_state["projects"]["twinpaper"]["last_request_id"] == "REQ-PLAN"
    assert sent[-1][0] == "planning-gate"
    assert any(evt.get("event") == "planning_planner" for evt in logged)
    assert saved


def test_handle_run_or_unknown_command_reuses_provisional_request_id_for_dispatch(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [],
            }
        }
    }
    sent: list[tuple[str, str, dict | None]] = []
    logged: list[dict] = []
    metadata_seen: list[dict] = []

    def _run_aoe_orch(_p_args, _prompt, **kwargs):
        metadata = dict(kwargs.get("metadata") or {})
        metadata_seen.append(metadata)
        req_id = str(metadata.get("request_id", "")).strip()
        return {
            "request_id": req_id,
            "complete": True,
            "roles": ["Local-Dev"],
            "role_states": [{"role": "Local-Dev", "status": "done"}],
            "replies": [{"role": "Local-Dev", "text": "done"}],
            "counts": {"assignments": 1, "replies": 1},
            "done_roles": ["Local-Dev"],
            "failed_roles": [],
            "pending_roles": [],
        }

    ctx = run_handlers.build_run_context(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=team_dir / "orch_manager_state.json",
            auto_dispatch=False,
            require_verifier=False,
            verifier_roles="",
            task_planning=True,
            plan_phase1_ensemble=True,
            plan_max_subtasks=6,
            plan_auto_replan=False,
            plan_replan_attempts=0,
            plan_block_on_critic=True,
            exec_critic=False,
            exec_critic_retry_max=3,
            chat_max_running=3,
            chat_daily_cap=20,
        ),
        manager_state=manager_state,
        chat_id="939062873",
        text="run it",
        rest="run it",
        orch_target="O2",
        run_prompt="run it",
        run_roles_override=None,
        run_priority_override=None,
        run_timeout_override=None,
        run_no_wait_override=None,
        run_force_mode="dispatch",
        run_auto_source="default",
        run_control_mode="normal",
        run_source_request_id="",
        run_source_task=None,
    )

    deps = run_handlers.RunDeps(
        core=run_handlers.RunCoreDeps(
            send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
            log_event=lambda **kwargs: logged.append(kwargs),
            help_text=lambda: "help",
        ),
        guard=run_handlers.RunGuardDeps(
            summarize_chat_usage=lambda manager_state, chat_id: (0, 0),
            detect_high_risk_prompt=lambda prompt: "",
            set_confirm_action=lambda *args, **kwargs: None,
            save_manager_state=lambda path, manager_state: None,
        ),
        planning=run_handlers.RunPlanningDeps(
            choose_auto_dispatch_roles=lambda *args, **kwargs: ["Local-Dev"],
            resolve_verifier_candidates=lambda text: [],
            load_orchestrator_roles=lambda team_dir: ["Local-Dev"],
            parse_roles_csv=lambda csv: [token for token in str(csv or "").split(",") if token],
            ensure_verifier_roles=lambda **kwargs: (kwargs.get("selected_roles", []), [], False, []),
            available_worker_roles=lambda roles: roles,
            normalize_task_plan_payload=lambda payload, **kwargs: payload or {},
            build_task_execution_plan=lambda **kwargs: {},
            critique_task_execution_plan=lambda **kwargs: {"approved": True, "issues": [], "recommendations": []},
            critic_has_blockers=lambda critic: False,
            repair_task_execution_plan=lambda **kwargs: {},
            plan_roles_from_subtasks=lambda payload: ["Local-Dev"],
            build_planned_dispatch_prompt=lambda prompt, plan_data, plan_critic: prompt,
            phase1_ensemble_planning=lambda *args, **kwargs: {
                "plan_data": {"summary": "ready", "subtasks": [{"id": "S1", "owner_role": "Local-Dev", "title": "Implement", "goal": "do it", "acceptance": ["done"]}]},
                "plan_critic": {"approved": True, "issues": [], "recommendations": []},
                "plan_roles": ["Local-Dev"],
                "plan_replans": [{"attempt": 1}, {"attempt": 2}, {"attempt": 3}],
                "plan_error": "",
                "plan_gate_blocked": False,
                "plan_gate_reason": "",
                "phase1_mode": "ensemble",
                "phase1_rounds": 3,
                "phase1_providers": ["codex", "claude"],
            },
        ),
        routing=run_handlers.RunRoutingDeps(
            get_context=lambda raw: (
                "twinpaper",
                manager_state["projects"]["twinpaper"],
                argparse.Namespace(
                    project_root=project_root,
                    team_dir=team_dir,
                    roles="Local-Dev",
                    priority="P2",
                    orch_timeout_sec=120,
                    no_wait=False,
                ),
            ),
            run_orchestrator_direct=lambda p_args, prompt: "direct",
            run_aoe_orch=_run_aoe_orch,
            create_request_id=lambda: "REQ-DISPATCH",
            ensure_task_record=lambda **kwargs: gw.ensure_task_record(
                kwargs["entry"],
                kwargs["request_id"],
                kwargs["prompt"],
                kwargs["mode"],
                kwargs["roles"],
                kwargs["verifier_roles"],
                kwargs["require_verifier"],
            ),
            finalize_request_reply_messages=lambda *args, **kwargs: {},
            touch_chat_recent_task_ref=gw.touch_chat_recent_task_ref,
            set_chat_selected_task_ref=gw.set_chat_selected_task_ref,
            now_iso=lambda: "2026-03-12T23:50:00+0900",
            sync_task_lifecycle=gw.sync_task_lifecycle,
            lifecycle_set_stage=gw.lifecycle_set_stage,
            summarize_task_lifecycle=lambda key, task: "",
            synthesize_orchestrator_response=lambda p_args, prompt, state: "",
            critique_task_result=lambda **kwargs: {"verdict": "success", "reason": ""},
            extract_todo_proposals=lambda *args, **kwargs: [],
            merge_todo_proposals=lambda **kwargs: {"created_count": 0, "created_ids": [], "duplicate_count": 0, "skipped_count": 0},
            render_run_response=lambda state, task=None: "result",
        ),
    )

    handled = run_handlers.handle_run_or_unknown_command(ctx=ctx, deps=deps)

    assert handled is True
    assert metadata_seen
    assert metadata_seen[0]["request_id"] == "REQ-DISPATCH"
    tasks = manager_state["projects"]["twinpaper"]["tasks"]
    assert list(tasks.keys()) == ["REQ-DISPATCH"]
    task = tasks["REQ-DISPATCH"]
    assert task["request_id"] == "REQ-DISPATCH"
    assert task["status"] == "completed"
    assert task["tf_phase"] == "completed"
    assert any(row[0] in {"result", "synth"} for row in sent)


def test_filter_phase2_retry_scope_limits_plan_to_target_lanes() -> None:
    plan_data = {
        "summary": "ready",
        "subtasks": [
            {"id": "S1", "owner_role": "Local-Dev", "title": "Implement", "goal": "do impl"},
            {"id": "S2", "owner_role": "Local-Writer", "title": "Document", "goal": "write handoff"},
        ],
        "meta": {
            "phase2_team_spec": {
                "execution_mode": "parallel",
                "execution_groups": [
                    {"group_id": "L1", "role": "Local-Dev", "subtask_ids": ["S1"], "parallel": True},
                    {"group_id": "L2", "role": "Local-Writer", "subtask_ids": ["S2"], "parallel": True},
                ],
                "review_mode": "parallel",
                "review_groups": [
                    {"group_id": "R1", "role": "Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": True},
                    {"group_id": "R2", "role": "Reviewer", "kind": "verifier", "depends_on": ["L2"], "parallel": True},
                ],
                "team_roles": ["Local-Dev", "Local-Writer", "Reviewer"],
                "critic_role": "Reviewer",
                "integration_role": "Reviewer",
            },
            "phase2_execution_plan": {
                "execution_mode": "parallel",
                "execution_lanes": [
                    {"lane_id": "L1", "role": "Local-Dev", "subtask_ids": ["S1"], "parallel": True},
                    {"lane_id": "L2", "role": "Local-Writer", "subtask_ids": ["S2"], "parallel": True},
                ],
                "review_mode": "parallel",
                "review_lanes": [
                    {"lane_id": "R1", "role": "Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": True},
                    {"lane_id": "R2", "role": "Reviewer", "kind": "verifier", "depends_on": ["L2"], "parallel": True},
                ],
                "parallel_workers": True,
                "parallel_reviews": True,
                "readonly": True,
            },
        },
    }
    filtered, scope = run_handlers._filter_phase2_retry_scope(
        plan_data=plan_data,
        run_control_mode="retry",
        run_source_task={
            "exec_critic": {
                "verdict": "retry",
                "action": "retry",
                "rerun_execution_lane_ids": ["L2"],
                "rerun_review_lane_ids": ["R2"],
            }
        },
    )

    assert filtered is not None
    meta = filtered["meta"]
    exec_plan = meta["phase2_execution_plan"]
    assert [row["lane_id"] for row in exec_plan["execution_lanes"]] == ["L2"]
    assert [row["lane_id"] for row in exec_plan["review_lanes"]] == ["R2"]
    assert [row["id"] for row in filtered["subtasks"]] == ["S2"]
    assert scope["planned_roles"] == ["Local-Writer", "Reviewer"]


def test_filter_phase2_retry_scope_honors_operator_selected_lane_subset() -> None:
    plan_data = {
        "summary": "ready",
        "subtasks": [
            {"id": "S1", "owner_role": "Local-Dev", "title": "Implement", "goal": "do impl"},
            {"id": "S2", "owner_role": "Local-Writer", "title": "Document", "goal": "write handoff"},
        ],
        "meta": {
            "phase2_team_spec": {
                "execution_mode": "parallel",
                "execution_groups": [
                    {"group_id": "L1", "role": "Local-Dev", "subtask_ids": ["S1"], "parallel": True},
                    {"group_id": "L2", "role": "Local-Writer", "subtask_ids": ["S2"], "parallel": True},
                ],
                "review_mode": "parallel",
                "review_groups": [
                    {"group_id": "R1", "role": "Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": True},
                    {"group_id": "R2", "role": "Reviewer", "kind": "verifier", "depends_on": ["L2"], "parallel": True},
                ],
                "team_roles": ["Local-Dev", "Local-Writer", "Reviewer"],
                "critic_role": "Reviewer",
                "integration_role": "Reviewer",
            },
            "phase2_execution_plan": {
                "execution_mode": "parallel",
                "execution_lanes": [
                    {"lane_id": "L1", "role": "Local-Dev", "subtask_ids": ["S1"], "parallel": True},
                    {"lane_id": "L2", "role": "Local-Writer", "subtask_ids": ["S2"], "parallel": True},
                ],
                "review_mode": "parallel",
                "review_lanes": [
                    {"lane_id": "R1", "role": "Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": True},
                    {"lane_id": "R2", "role": "Reviewer", "kind": "verifier", "depends_on": ["L2"], "parallel": True},
                ],
                "parallel_workers": True,
                "parallel_reviews": True,
                "readonly": True,
            },
        },
    }

    filtered, scope = run_handlers._filter_phase2_retry_scope(
        plan_data=plan_data,
        run_control_mode="retry",
        run_source_task={
            "exec_critic": {
                "verdict": "retry",
                "action": "retry",
                "rerun_execution_lane_ids": ["L1", "L2"],
                "rerun_review_lane_ids": ["R1", "R2"],
            }
        },
        selected_execution_lane_ids=["L1"],
    )

    assert filtered is not None
    exec_plan = filtered["meta"]["phase2_execution_plan"]
    assert [row["lane_id"] for row in exec_plan["execution_lanes"]] == ["L1"]
    assert [row["lane_id"] for row in exec_plan["review_lanes"]] == ["R1"]
    assert scope["rerun_execution_lane_ids"] == ["L1"]
    assert scope["rerun_review_lane_ids"] == ["R1"]


def test_handle_run_or_unknown_command_retry_filters_phase2_dispatch_to_target_lanes(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [],
            }
        }
    }
    metadata_seen: list[dict[str, Any]] = []
    sent: list[tuple[str, str, dict | None]] = []
    critic_calls = {"count": 0}

    def _run_aoe_orch(_p_args, _prompt, **kwargs):
        metadata = copy.deepcopy(kwargs.get("metadata") or {})
        metadata_seen.append(metadata)
        req_id = str(metadata.get("request_id", "")).strip() or f"REQ-{len(metadata_seen)}"
        return {
            "request_id": req_id,
            "complete": True,
            "roles": ["Local-Dev", "Local-Writer", "Reviewer"],
            "role_states": [
                {"role": "Local-Dev", "status": "done"},
                {"role": "Local-Writer", "status": "done"},
                {"role": "Reviewer", "status": "done"},
            ],
            "replies": [{"role": "Reviewer", "text": "done"}],
            "counts": {"assignments": 1, "replies": 1},
            "done_roles": ["Local-Dev", "Local-Writer", "Reviewer"],
            "failed_roles": [],
            "pending_roles": [],
        }

    def _critique_task_result(*_args, **_kwargs):
        critic_calls["count"] += 1
        if critic_calls["count"] == 1:
            return {
                "verdict": "retry",
                "action": "retry",
                "reason": "rerun writer lane only",
                "rerun_execution_lane_ids": ["L2"],
                "rerun_review_lane_ids": ["R2"],
            }
        return {"verdict": "success", "action": "none", "reason": ""}

    plan_payload = {
        "summary": "ready",
        "subtasks": [
            {"id": "S1", "owner_role": "Local-Dev", "title": "Implement", "goal": "do impl", "acceptance": ["done"]},
            {"id": "S2", "owner_role": "Local-Writer", "title": "Document", "goal": "write handoff", "acceptance": ["done"]},
        ],
        "meta": {
            "phase2_team_spec": {
                "execution_mode": "parallel",
                "execution_groups": [
                    {"group_id": "L1", "role": "Local-Dev", "subtask_ids": ["S1"], "parallel": True},
                    {"group_id": "L2", "role": "Local-Writer", "subtask_ids": ["S2"], "parallel": True},
                ],
                "review_mode": "parallel",
                "review_groups": [
                    {"group_id": "R1", "role": "Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": True},
                    {"group_id": "R2", "role": "Reviewer", "kind": "verifier", "depends_on": ["L2"], "parallel": True},
                ],
                "team_roles": ["Local-Dev", "Local-Writer", "Reviewer"],
                "critic_role": "Reviewer",
                "integration_role": "Reviewer",
            },
            "phase2_execution_plan": {
                "execution_mode": "parallel",
                "execution_lanes": [
                    {"lane_id": "L1", "role": "Local-Dev", "subtask_ids": ["S1"], "parallel": True},
                    {"lane_id": "L2", "role": "Local-Writer", "subtask_ids": ["S2"], "parallel": True},
                ],
                "review_mode": "parallel",
                "review_lanes": [
                    {"lane_id": "R1", "role": "Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": True},
                    {"lane_id": "R2", "role": "Reviewer", "kind": "verifier", "depends_on": ["L2"], "parallel": True},
                ],
                "parallel_workers": True,
                "parallel_reviews": True,
                "readonly": True,
            },
        },
    }

    ctx = run_handlers.build_run_context(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=team_dir / "orch_manager_state.json",
            auto_dispatch=False,
            require_verifier=False,
            verifier_roles="",
            task_planning=True,
            plan_phase1_ensemble=True,
            plan_max_subtasks=6,
            plan_auto_replan=False,
            plan_replan_attempts=0,
            plan_block_on_critic=True,
            exec_critic=True,
            exec_critic_retry_max=3,
            chat_max_running=3,
            chat_daily_cap=20,
        ),
        manager_state=manager_state,
        chat_id="939062873",
        text="run it",
        rest="run it",
        orch_target="O2",
        run_prompt="run it",
        run_roles_override=None,
        run_priority_override=None,
        run_timeout_override=None,
        run_no_wait_override=None,
        run_force_mode="dispatch",
        run_auto_source="default",
        run_control_mode="normal",
        run_source_request_id="",
        run_source_task=None,
    )

    deps = run_handlers.RunDeps(
        core=run_handlers.RunCoreDeps(
            send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
            log_event=lambda **kwargs: None,
            help_text=lambda: "help",
        ),
        guard=run_handlers.RunGuardDeps(
            summarize_chat_usage=lambda manager_state, chat_id: (0, 0),
            detect_high_risk_prompt=lambda prompt: "",
            set_confirm_action=lambda *args, **kwargs: None,
            save_manager_state=lambda *args, **kwargs: None,
        ),
        planning=run_handlers.RunPlanningDeps(
            choose_auto_dispatch_roles=lambda *args, **kwargs: ["Local-Dev", "Local-Writer", "Reviewer"],
            resolve_verifier_candidates=lambda text: [],
            load_orchestrator_roles=lambda team_dir: ["Local-Dev", "Local-Writer", "Reviewer"],
            parse_roles_csv=lambda csv: [token for token in str(csv or "").split(",") if token],
            ensure_verifier_roles=lambda **kwargs: (kwargs.get("selected_roles", []), [], False, []),
            available_worker_roles=lambda roles: roles,
            normalize_task_plan_payload=lambda payload, **kwargs: payload or {},
            build_task_execution_plan=lambda **kwargs: {},
            critique_task_execution_plan=lambda **kwargs: {"approved": True, "issues": [], "recommendations": []},
            critic_has_blockers=lambda critic: False,
            repair_task_execution_plan=lambda **kwargs: {},
            plan_roles_from_subtasks=lambda payload: ["Local-Dev", "Local-Writer", "Reviewer"],
            build_planned_dispatch_prompt=lambda prompt, plan_data, plan_critic: prompt,
            phase1_ensemble_planning=lambda *args, **kwargs: {
                "plan_data": copy.deepcopy(plan_payload),
                "plan_critic": {"approved": True, "issues": [], "recommendations": []},
                "plan_roles": ["Local-Dev", "Local-Writer", "Reviewer"],
                "plan_replans": [{"attempt": 1}, {"attempt": 2}, {"attempt": 3}],
                "plan_error": "",
                "plan_gate_blocked": False,
                "plan_gate_reason": "",
                "phase1_mode": "ensemble",
                "phase1_rounds": 3,
                "phase1_providers": ["codex", "claude"],
            },
        ),
        routing=run_handlers.RunRoutingDeps(
            get_context=lambda raw: (
                "twinpaper",
                manager_state["projects"]["twinpaper"],
                argparse.Namespace(
                    project_root=project_root,
                    team_dir=team_dir,
                    roles="Local-Dev,Local-Writer,Reviewer",
                    priority="P2",
                    orch_timeout_sec=120,
                    no_wait=False,
                ),
            ),
            run_orchestrator_direct=lambda p_args, prompt: "direct",
            run_aoe_orch=_run_aoe_orch,
            create_request_id=lambda: "REQ-RETRY",
            ensure_task_record=lambda **kwargs: gw.ensure_task_record(
                kwargs["entry"],
                kwargs["request_id"],
                kwargs["prompt"],
                kwargs["mode"],
                kwargs["roles"],
                kwargs["verifier_roles"],
                kwargs["require_verifier"],
            ),
            finalize_request_reply_messages=lambda *args, **kwargs: {},
            touch_chat_recent_task_ref=gw.touch_chat_recent_task_ref,
            set_chat_selected_task_ref=gw.set_chat_selected_task_ref,
            now_iso=lambda: "2026-03-12T23:55:00+0900",
            sync_task_lifecycle=gw.sync_task_lifecycle,
            lifecycle_set_stage=gw.lifecycle_set_stage,
            summarize_task_lifecycle=lambda key, task: "",
            synthesize_orchestrator_response=lambda p_args, prompt, state: "",
            critique_task_result=_critique_task_result,
            extract_todo_proposals=lambda *args, **kwargs: [],
            merge_todo_proposals=lambda **kwargs: {"created_count": 0, "created_ids": [], "duplicate_count": 0, "skipped_count": 0},
            render_run_response=lambda state, task=None: "result",
        ),
    )

    handled = run_handlers.handle_run_or_unknown_command(ctx=ctx, deps=deps)

    assert handled is True
    assert len(metadata_seen) == 2
    first_plan = metadata_seen[0]["phase2_execution_plan"]
    second_plan = metadata_seen[1]["phase2_execution_plan"]
    assert [row["lane_id"] for row in first_plan["execution_lanes"]] == ["L1", "L2"]
    assert [row["lane_id"] for row in second_plan["execution_lanes"]] == ["L2"]
    assert [row["lane_id"] for row in second_plan["review_lanes"]] == ["R2"]
    task = manager_state["projects"]["twinpaper"]["tasks"]["REQ-RETRY"]
    assert task["status"] == "completed"
    assert task["tf_phase"] == "completed"
    assert any(row[0] in {"result", "synth"} for row in sent)


def test_todo_next_blocks_unready_project(tmp_path: Path) -> None:
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(tmp_path / "TwinPaper"),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "run broken project",
                        "priority": "P1",
                        "status": "open",
                    }
                ],
                "todo_seq": 1,
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / "state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="twinpaper",
        rest="next",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda target: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace()),
        save_manager_state=lambda *args, **kwargs: None,
        now_iso=lambda: "2026-03-07T18:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    assert "todo next blocked: project runtime is not ready" in sent[-1]
    assert "/orch status O2" in sent[-1]


def test_todo_with_explicit_other_project_under_focus_returns_operator_message(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    twin_root = tmp_path / "TwinPaper"
    nano_root = tmp_path / "Nano"
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(twin_root),
        "team_dir": str(twin_root / ".aoe-team"),
        "tasks": {},
    }
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(nano_root),
        "team_dir": str(nano_root / ".aoe-team"),
        "tasks": {},
    }
    gw.set_project_lock(state, "twinpaper")
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / "state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="O3 next",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw_target: (
            lambda k, e: (k, e, argparse.Namespace(project_root=Path(e["project_root"]), team_dir=Path(e["team_dir"])))
        )(*gw.get_manager_project(state, raw_target)),
        save_manager_state=lambda *args, **kwargs: None,
        now_iso=lambda: "2026-03-07T18:05:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    assert "todo blocked by project lock" in sent[-1]
    assert "/focus off" in sent[-1]


def test_cleanup_terminal_todo_gate_blocks_pending_todo_and_clears_pending() -> None:
    entry = {
        "todos": [
            {
                "id": "TODO-001",
                "summary": "broken queued item",
                "priority": "P2",
                "status": "open",
            }
        ],
        "pending_todo": {
            "todo_id": "TODO-001",
            "chat_id": "939062873",
            "selected_at": "2026-03-06T23:43:25+0900",
        },
    }

    changed = run_handlers._cleanup_terminal_todo_gate(
        entry=entry,
        chat_id="939062873",
        todo_id="",
        pending_todo_used=False,
        run_auto_source="todo:next",
        reason="plan gate: critic unresolved after auto-replan",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    todo = entry["todos"][0]
    assert changed is True
    assert "pending_todo" not in entry
    assert todo["status"] == "blocked"
    assert todo["blocked_reason"] == "plan gate: critic unresolved after auto-replan"
    assert todo["updated_at"] == "2026-03-07T00:00:00+0900"


def test_finalize_todo_after_run_increments_blocked_count_and_clears_it_on_success() -> None:
    entry = {
        "todos": [
            {
                "id": "TODO-001",
                "summary": "blocked row",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_reason": "old plan gate",
            }
        ]
    }

    run_handlers._finalize_todo_after_run(
        entry=entry,
        todo_id="TODO-001",
        status="failed",
        exec_verdict="retry",
        exec_reason="critic unresolved",
        req_id="REQ-2",
        task={"short_id": "T-102"},
        now_iso=lambda: "2026-03-07T00:10:00+0900",
    )

    todo = entry["todos"][0]
    assert todo["status"] == "blocked"
    assert todo["blocked_count"] == 3
    assert todo["blocked_bucket"] == "manual_followup"
    assert todo["blocked_request_id"] == "REQ-2"
    assert todo["blocked_reason"] == "critic unresolved"

    run_handlers._finalize_todo_after_run(
        entry=entry,
        todo_id="TODO-001",
        status="completed",
        exec_verdict="success",
        exec_reason="",
        req_id="REQ-3",
        task={"short_id": "T-103"},
        now_iso=lambda: "2026-03-07T00:20:00+0900",
    )

    assert todo["status"] == "done"
    assert "blocked_count" not in todo
    assert "blocked_bucket" not in todo
    assert "blocked_reason" not in todo
    assert "blocked_request_id" not in todo


def test_manual_followup_alert_is_sent_only_once() -> None:
    entry = {
        "project_alias": "O4",
        "todos": [
            {
                "id": "TODO-004",
                "summary": "need owner input",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            }
        ],
    }
    sent: list[tuple[str, str, dict | None]] = []

    first = run_handlers._maybe_send_manual_followup_alert(
        entry=entry,
        todo_id="TODO-004",
        project_key="local_map_analysis",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        now_iso=lambda: "2026-03-07T00:30:00+0900",
    )
    second = run_handlers._maybe_send_manual_followup_alert(
        entry=entry,
        todo_id="TODO-004",
        project_key="local_map_analysis",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        now_iso=lambda: "2026-03-07T00:31:00+0900",
    )

    assert first is True
    assert second is False
    assert entry["todos"][0]["blocked_alerted_at"] == "2026-03-07T00:30:00+0900"
    assert len(sent) == 1
    assert sent[0][0] == "manual-followup-alert"
    assert "manual follow-up needed" in sent[0][1]
    assert "TODO-004" in sent[0][1]
    assert "/todo O4 followup" in sent[0][1]
    assert "/queue followup" in sent[0][1]
    markup = sent[0][2] or {}
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/todo O4 followup" in buttons
    assert "/queue followup" in buttons


def test_capture_todo_proposals_merges_and_alerts() -> None:
    entry = {
        "project_alias": "O2",
        "todos": [
            {
                "id": "TODO-010",
                "summary": "existing task",
                "priority": "P2",
                "status": "done",
            }
        ],
        "todo_seq": 10,
        "todo_proposals": [],
        "todo_proposal_seq": 0,
    }
    sent: list[tuple[str, str, dict | None]] = []
    logged: list[dict] = []

    result = run_handlers._maybe_capture_todo_proposals(
        args=argparse.Namespace(dry_run=False),
        entry=entry,
        key="twinpaper",
        p_args=argparse.Namespace(),
        prompt="run release prep",
        state={
            "complete": True,
            "replies": [
                {"role": "Local-Writer", "body": "Release note draft is done. We still need a deployment checklist."}
            ],
        },
        req_id="REQ-900",
        task={"todo_id": "TODO-010", "short_id": "T-900"},
        todo_id="TODO-010",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged.append(kwargs),
        now_iso=lambda: "2026-03-09T10:00:00+0900",
        extract_todo_proposals=lambda *args, **kwargs: [
            {
                "summary": "prepare deployment checklist",
                "priority": "P1",
                "kind": "handoff",
                "reason": "release note mentions it is still missing",
                "confidence": 0.88,
            }
        ],
        merge_todo_proposals=todo_handlers.merge_todo_proposals,
    )

    assert result["created_count"] == 1
    assert entry["todo_proposals"][0]["id"] == "PROP-001"
    assert entry["todo_proposals"][0]["source_request_id"] == "REQ-900"
    assert sent
    assert sent[-1][0] == "todo-proposals-alert"
    assert "new todo proposals" in sent[-1][1]
    assert "prepare deployment checklist" in sent[-1][1]
    buttons = [btn["text"] for row in (sent[-1][2] or {}).get("keyboard", []) for btn in row]
    assert "/todo proposals" in buttons
    assert "/todo accept PROP-001" in buttons
    assert any(evt.get("event") == "todo_proposals_created" for evt in logged)


def test_capture_todo_proposals_prefers_backend_native_payload() -> None:
    entry = {
        "project_alias": "O4",
        "todos": [],
        "todo_seq": 0,
        "todo_proposals": [],
        "todo_proposal_seq": 0,
    }
    sent: list[tuple[str, str, dict | None]] = []
    logged: list[dict] = []

    result = run_handlers._maybe_capture_todo_proposals(
        args=argparse.Namespace(dry_run=False),
        entry=entry,
        key="local_map_analysis",
        p_args=argparse.Namespace(),
        prompt="writer handoff prompt",
        state={
            "complete": True,
            "replies": [{"role": "Local-Writer", "body": "hints are present but should not be reparsed"}],
            "followup_proposals": [
                {
                    "summary": "Draft the machine-readable summary table from the canonical backlog",
                    "priority": "P1",
                    "kind": "handoff",
                    "reason": "backend-native writer handoff proposal",
                    "confidence": 0.81,
                }
            ],
        },
        req_id="REQ-901",
        task={"todo_id": "TODO-015", "short_id": "T-901"},
        todo_id="TODO-015",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged.append(kwargs),
        now_iso=lambda: "2026-03-11T21:00:00+0900",
        extract_todo_proposals=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("extractor should not run when backend payload exists")),
        merge_todo_proposals=todo_handlers.merge_todo_proposals,
    )

    assert result["created_count"] == 1
    assert entry["todo_proposals"][0]["source_request_id"] == "REQ-901"
    assert entry["todo_proposals"][0]["kind"] == "handoff"
    assert any(evt.get("event") == "todo_proposals_backend_payload" for evt in logged)
    assert sent[-1][0] == "todo-proposals-alert"


def test_exec_pipeline_module_matches_run_terminal_todo_helpers() -> None:
    entry_a = {
        "project_alias": "O4",
        "todos": [
            {
                "id": "TODO-004",
                "summary": "need owner input",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            }
        ],
        "pending_todo": {
            "todo_id": "TODO-004",
            "chat_id": "939062873",
            "selected_at": "2026-03-06T23:43:25+0900",
        },
    }
    entry_b = copy.deepcopy(entry_a)
    sent_a: list[tuple[str, str, dict | None]] = []
    sent_b: list[tuple[str, str, dict | None]] = []

    changed_a = run_handlers._cleanup_terminal_todo_gate(
        entry=entry_a,
        chat_id="939062873",
        todo_id="",
        pending_todo_used=False,
        run_auto_source="todo:next",
        reason="plan gate: critic unresolved after auto-replan",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )
    changed_b = exec_pipeline.cleanup_terminal_todo_gate(
        entry=entry_b,
        chat_id="939062873",
        todo_id="",
        pending_todo_used=False,
        run_auto_source="todo:next",
        reason="plan gate: critic unresolved after auto-replan",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
        manual_followup_threshold=2,
    )

    assert changed_a == changed_b
    assert entry_a == entry_b

    first_a = run_handlers._maybe_send_manual_followup_alert(
        entry=entry_a,
        todo_id="TODO-004",
        project_key="local_map_analysis",
        send=lambda body, **kwargs: sent_a.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        now_iso=lambda: "2026-03-07T00:30:00+0900",
    )
    first_b = exec_pipeline.maybe_send_manual_followup_alert(
        entry=entry_b,
        todo_id="TODO-004",
        project_key="local_map_analysis",
        send=lambda body, **kwargs: sent_b.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        now_iso=lambda: "2026-03-07T00:30:00+0900",
    )

    assert first_a == first_b == True
    assert sent_a == sent_b
    assert entry_a == entry_b


def test_exec_pipeline_module_matches_run_dispatch_sync_and_proposal_capture() -> None:
    entry_a = {"project_alias": "O2", "last_request_id": ""}
    entry_b = copy.deepcopy(entry_a)
    manager_state_a = {"projects": {"twinpaper": entry_a}}
    manager_state_b = {"projects": {"twinpaper": entry_b}}
    touches_a: list[tuple] = []
    touches_b: list[tuple] = []
    selects_a: list[tuple] = []
    selects_b: list[tuple] = []
    run_calls_a: list[dict] = []
    run_calls_b: list[dict] = []

    def _run_aoe_orch(*_args, **_kwargs):
        run_calls_a.append(dict(_kwargs))
        return {"request_id": "REQ-123", "complete": False, "replies": []}

    def _run_aoe_orch_b(*_args, **_kwargs):
        run_calls_b.append(dict(_kwargs))
        return {"request_id": "REQ-123", "complete": False, "replies": []}

    def _sync_task_lifecycle(**kwargs):
        return {
            "request_id": kwargs["request_data"]["request_id"],
            "status": "running",
            "stages": {"verification": "pending"},
        }

    result_a = run_handlers._dispatch_and_sync_task(
        p_args=argparse.Namespace(),
        dispatch_prompt="dispatch prompt",
        chat_id="939062873",
        dispatch_roles="Reviewer",
        run_priority_override=None,
        run_timeout_override=None,
        run_no_wait_override=None,
        dispatch_metadata={"phase2_execution_plan": {"execution_mode": "single", "execution_lanes": []}},
        key="twinpaper",
        entry=entry_a,
        manager_state=manager_state_a,
        prompt="original prompt",
        selected_roles=["Reviewer"],
        verifier_roles=[],
        require_verifier=False,
        verifier_candidates=[],
        run_aoe_orch=_run_aoe_orch,
        touch_chat_recent_task_ref=lambda *args: touches_a.append(args),
        set_chat_selected_task_ref=lambda *args: selects_a.append(args),
        now_iso=lambda: "2026-03-11T10:00:00+09:00",
        sync_task_lifecycle=_sync_task_lifecycle,
    )
    result_b = exec_pipeline.dispatch_and_sync_task(
        p_args=argparse.Namespace(),
        dispatch_prompt="dispatch prompt",
        chat_id="939062873",
        dispatch_roles="Reviewer",
        run_priority_override=None,
        run_timeout_override=None,
        run_no_wait_override=None,
        dispatch_metadata={"phase2_execution_plan": {"execution_mode": "single", "execution_lanes": []}},
        key="twinpaper",
        entry=entry_b,
        manager_state=manager_state_b,
        prompt="original prompt",
        selected_roles=["Reviewer"],
        verifier_roles=[],
        require_verifier=False,
        verifier_candidates=[],
        run_aoe_orch=_run_aoe_orch_b,
        touch_chat_recent_task_ref=lambda *args: touches_b.append(args),
        set_chat_selected_task_ref=lambda *args: selects_b.append(args),
        now_iso=lambda: "2026-03-11T10:00:00+09:00",
        sync_task_lifecycle=_sync_task_lifecycle,
    )

    assert result_a == result_b
    assert entry_a == entry_b
    assert touches_a == touches_b
    assert selects_a == selects_b
    assert run_calls_a == run_calls_b
    assert run_calls_a[0]["metadata"]["phase2_execution_plan"]["execution_mode"] == "single"

    proposal_entry_a = {
        "project_alias": "O2",
        "todos": [{"id": "TODO-010", "summary": "existing task", "priority": "P2", "status": "done"}],
        "todo_seq": 10,
        "todo_proposals": [],
        "todo_proposal_seq": 0,
    }
    proposal_entry_b = copy.deepcopy(proposal_entry_a)
    sent_a: list[tuple[str, str, dict | None]] = []
    sent_b: list[tuple[str, str, dict | None]] = []
    logged_a: list[dict] = []
    logged_b: list[dict] = []

    common_kwargs = dict(
        args=argparse.Namespace(dry_run=False),
        key="twinpaper",
        p_args=argparse.Namespace(),
        prompt="run release prep",
        state={
            "complete": True,
            "replies": [{"role": "Local-Writer", "body": "Release note draft is done. We still need a deployment checklist."}],
        },
        req_id="REQ-900",
        task={"todo_id": "TODO-010", "short_id": "T-900"},
        todo_id="TODO-010",
        now_iso=lambda: "2026-03-09T10:00:00+0900",
        extract_todo_proposals=lambda *args, **kwargs: [
            {
                "summary": "prepare deployment checklist",
                "priority": "P1",
                "kind": "handoff",
                "reason": "release note mentions it is still missing",
                "confidence": 0.88,
            }
        ],
        merge_todo_proposals=todo_handlers.merge_todo_proposals,
    )

    proposals_a = run_handlers._maybe_capture_todo_proposals(
        entry=proposal_entry_a,
        send=lambda body, **kwargs: sent_a.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged_a.append(kwargs),
        **common_kwargs,
    )
    proposals_b = exec_pipeline.maybe_capture_todo_proposals(
        entry=proposal_entry_b,
        send=lambda body, **kwargs: sent_b.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged_b.append(kwargs),
        **common_kwargs,
    )

    assert proposals_a == proposals_b
    assert proposal_entry_a == proposal_entry_b
    assert sent_a == sent_b
    assert logged_a == logged_b


def test_apply_scenario_items_to_entry_prunes_stale_sync_open_todos() -> None:
    entry = {
        "todos": [
            {
                "id": "TODO-001",
                "summary": "old sync row",
                "priority": "P2",
                "status": "open",
                "created_by": "sync:telegram:test",
            },
            {
                "id": "TODO-002",
                "summary": "manual row",
                "priority": "P2",
                "status": "open",
                "created_by": "manual:user",
            },
        ],
        "todo_seq": 2,
        "pending_todo": {"todo_id": "TODO-001", "chat_id": "939062873"},
    }

    counts = sched._apply_scenario_items_to_entry(
        entry=entry,
        items=[{"summary": "new sync row", "priority": "P1", "status": "open"}],
        chat_id="939062873",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
        dry_run=False,
        source_mode="fallback:files",
        sources=["TODO.md"],
        prune_missing=True,
    )

    assert counts["added"] == 1
    assert counts["pruned"] == 1
    assert "pending_todo" not in entry
    rows = {row["id"]: row for row in entry["todos"]}
    assert rows["TODO-001"]["status"] == "canceled"
    assert rows["TODO-001"]["canceled_reason"] == "sync_prune_missing"
    assert rows["TODO-002"]["status"] == "open"
    assert rows["TODO-003"]["summary"] == "new sync row"


def test_apply_scenario_items_to_entry_prunes_only_same_sync_group() -> None:
    entry = {
        "todos": [
            {
                "id": "TODO-001",
                "summary": "old project sync row",
                "priority": "P2",
                "status": "open",
                "created_by": "sync:telegram:test",
                "sync_managed": True,
                "sync_group": "todo_files",
            },
            {
                "id": "TODO-002",
                "summary": "old ops sync row",
                "priority": "P2",
                "status": "open",
                "created_by": "sync:telegram:test",
                "sync_managed": True,
                "sync_group": "ops",
            },
        ],
        "todo_seq": 2,
    }

    counts = sched._apply_scenario_items_to_entry(
        entry=entry,
        items=[
            {
                "summary": "fresh project sync row",
                "priority": "P1",
                "status": "open",
                "sync_group": "todo_files",
                "sync_source_class": "todo_file",
                "sync_confidence": 0.92,
            }
        ],
        chat_id="939062873",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
        dry_run=False,
        source_mode="fallback:files",
        sources=["notes/project_todo.md"],
        prune_missing=True,
    )

    rows = {row["id"]: row for row in entry["todos"]}
    assert counts["pruned"] == 1
    assert rows["TODO-001"]["status"] == "canceled"
    assert rows["TODO-002"]["status"] == "open"


def test_apply_scenario_items_to_entry_prunes_blocked_stale_sync_rows() -> None:
    entry = {
        "todos": [
            {
                "id": "TODO-001",
                "summary": "stale blocked sync row",
                "priority": "P2",
                "status": "blocked",
                "created_by": "sync:telegram:test",
                "blocked_reason": "old plan gate",
                "sync_group": "todo_files",
            },
        ],
        "todo_seq": 1,
    }

    counts = sched._apply_scenario_items_to_entry(
        entry=entry,
        items=[
            {
                "summary": "fresh project sync row",
                "priority": "P1",
                "status": "open",
                "sync_group": "todo_files",
                "sync_source_class": "todo_file",
                "sync_confidence": 0.92,
            }
        ],
        chat_id="939062873",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
        dry_run=False,
        source_mode="fallback:files",
        sources=["notes/project_todo.md"],
        prune_missing=True,
    )

    rows = {row["id"]: row for row in entry["todos"]}
    assert counts["pruned"] == 1
    assert rows["TODO-001"]["status"] == "canceled"
    assert rows["TODO-001"]["canceled_reason"] == "sync_prune_missing"
    assert "blocked_reason" not in rows["TODO-001"]


def test_sync_replace_blocks_partial_scope_since_window(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    twin_root = tmp_path / "TwinPaper"
    (twin_root / ".aoe-team").mkdir(parents=True, exist_ok=True)
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(twin_root),
        "team_dir": str(twin_root / ".aoe-team"),
        "tasks": {},
    }

    sent: list[str] = []
    args = argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json")

    def _send(body: str, **_kwargs) -> bool:
        sent.append(body)
        return True

    def _get_context(raw_target: str | None):
        key, entry = gw.get_manager_project(state, raw_target)
        return key, entry, argparse.Namespace(project_root=Path(entry["project_root"]), team_dir=Path(entry["team_dir"]))

    result = sched.handle_scheduler_command(
        cmd="sync",
        args=args,
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="replace O2 1h",
        send=_send,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1]
    assert "sync prune blocked" in text
    assert "avoid canceling unrelated todos" in text
    assert "/sync replace <O#|name>" in text


def test_orch_status_reply_markup_contains_monitor_todo_sync_and_focus_controls(tmp_path: Path) -> None:
    manager_state = _empty_state()
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(team_dir),
    }
    entry = manager_state["projects"]["twinpaper"]

    markup = orch_task_handlers._orch_status_reply_markup(manager_state, "twinpaper", entry)
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    for expected in [
        "/todo O2",
        "/todo O2 followup",
        "/orch monitor O2",
        "/sync preview O2 1h",
        "/sync O2 1h",
        "/use O2",
        "/focus O2",
        "/queue",
        "/next",
        "/map",
    ]:
        assert expected in buttons

    gw.set_project_lock(manager_state, "twinpaper")
    markup2 = orch_task_handlers._orch_status_reply_markup(manager_state, "twinpaper", entry)
    buttons2 = [btn["text"] for row in markup2.get("keyboard", []) for btn in row]
    assert "/focus off" in buttons2
    assert "/focus O2" not in buttons2


def test_orch_task_reply_markup_exposes_lane_retry_and_followup_actions(tmp_path: Path) -> None:
    manager_state = _empty_state()
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(team_dir),
    }
    entry = manager_state["projects"]["twinpaper"]
    task = {
        "request_id": "REQ-123",
        "context": {"task_short_id": "T-123"},
        "exec_critic": {
            "verdict": "retry",
            "action": "replan",
            "rerun_execution_lane_ids": ["L2"],
            "rerun_review_lane_ids": ["R2"],
            "manual_followup_execution_lane_ids": ["L2"],
            "manual_followup_review_lane_ids": ["R2"],
        },
    }

    markup = orch_task_handlers._orch_task_reply_markup("twinpaper", entry, "REQ-123", task)
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]

    for expected in [
        "/check T-123",
        "/task T-123",
        "/retry T-123",
        "/retry T-123 lane L2",
        "/retry T-123 lane R2",
        "/replan T-123",
        "/replan T-123 lane L2",
        "/replan T-123 lane R2",
        "/followup T-123",
        "/followup T-123 lane L2",
        "/followup T-123 lane R2",
        "/todo O2 followup",
        "/orch monitor O2",
        "/orch status O2",
        "/queue",
        "/map",
    ]:
        assert expected in buttons


def test_resolve_message_command_parses_retry_lane_selector() -> None:
    manager_state = gw.default_manager_state(ROOT, ROOT / ".aoe-team")

    resolved = resolver.resolve_message_command(
        text="/retry T-123 lane L2,R1",
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

    assert resolved.cmd == "orch-retry"
    assert resolved.orch_retry_request_id == "T-123"
    assert resolved.orch_retry_lane_ids == ["L2", "R1"]


def test_resolve_message_command_parses_followup_lane_selector() -> None:
    manager_state = gw.default_manager_state(ROOT, ROOT / ".aoe-team")

    resolved = resolver.resolve_message_command(
        text="/followup T-123 lane L2,R1",
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

    assert resolved.cmd == "orch-followup"
    assert resolved.orch_followup_request_id == "T-123"
    assert resolved.orch_followup_lane_ids == ["L2", "R1"]


def test_resolve_retry_replan_transition_rejects_invalid_lane_selector() -> None:
    manager_state = _empty_state()
    manager_state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(ROOT),
        "team_dir": str(ROOT / ".aoe-team"),
        "tasks": {
            "REQ-123": {
                "request_id": "REQ-123",
                "prompt": "retry target",
                "roles": ["Local-Dev", "Reviewer"],
                "exec_critic": {
                    "verdict": "retry",
                    "action": "retry",
                    "rerun_execution_lane_ids": ["L2"],
                    "rerun_review_lane_ids": ["R2"],
                },
            }
        },
    }
    sent: list[tuple[str, str]] = []
    result = retry_handlers.resolve_retry_replan_transition(
        cmd="orch-retry",
        args=argparse.Namespace(require_verifier=False, verifier_roles=""),
        manager_state=manager_state,
        chat_id="939062873",
        orch_target="twinpaper",
        orch_retry_request_id="REQ-123",
        orch_replan_request_id=None,
        orch_retry_lane_ids=["L1"],
        orch_replan_lane_ids=None,
        send=lambda text, **kwargs: sent.append((text, kwargs.get("context", ""))) or True,
        get_context=lambda orch: (str(orch or "twinpaper"), manager_state["projects"]["twinpaper"], argparse.Namespace(team_dir=str(ROOT / ".aoe-team"))),
        get_chat_selected_task_ref=lambda *_args, **_kwargs: "",
        resolve_chat_task_ref=lambda *_args, **_kwargs: "REQ-123",
        resolve_task_request_id=lambda entry, ref: ref if ref in entry.get("tasks", {}) else "",
        get_task_record=lambda entry, req_id: entry.get("tasks", {}).get(req_id),
        run_request_query=lambda *_args, **_kwargs: {},
        sync_task_lifecycle=lambda **_kwargs: None,
        resolve_verifier_candidates=lambda _raw: [],
        dedupe_roles=lambda rows: [str(item).strip() for item in rows if str(item).strip()],
        touch_chat_recent_task_ref=lambda *_args, **_kwargs: None,
        set_chat_selected_task_ref=lambda *_args, **_kwargs: None,
    )

    assert result == {"terminal": True}
    assert sent
    assert "requested lanes are not allowed" in sent[-1][0]


def test_resolve_retry_replan_transition_preserves_selected_lane_targets() -> None:
    manager_state = _empty_state()
    manager_state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(ROOT),
        "team_dir": str(ROOT / ".aoe-team"),
        "tasks": {
            "REQ-123": {
                "request_id": "REQ-123",
                "prompt": "retry target",
                "roles": ["Local-Dev", "Reviewer"],
                "exec_critic": {
                    "verdict": "retry",
                    "action": "retry",
                    "rerun_execution_lane_ids": ["L1", "L2"],
                    "rerun_review_lane_ids": ["R1", "R2"],
                },
            }
        },
    }
    result = retry_handlers.resolve_retry_replan_transition(
        cmd="orch-retry",
        args=argparse.Namespace(require_verifier=False, verifier_roles=""),
        manager_state=manager_state,
        chat_id="939062873",
        orch_target="twinpaper",
        orch_retry_request_id="REQ-123",
        orch_replan_request_id=None,
        orch_retry_lane_ids=["L2", "R2"],
        orch_replan_lane_ids=None,
        send=lambda *_args, **_kwargs: True,
        get_context=lambda orch: (str(orch or "twinpaper"), manager_state["projects"]["twinpaper"], argparse.Namespace(team_dir=str(ROOT / ".aoe-team"))),
        get_chat_selected_task_ref=lambda *_args, **_kwargs: "",
        resolve_chat_task_ref=lambda *_args, **_kwargs: "REQ-123",
        resolve_task_request_id=lambda entry, ref: ref if ref in entry.get("tasks", {}) else "",
        get_task_record=lambda entry, req_id: entry.get("tasks", {}).get(req_id),
        run_request_query=lambda *_args, **_kwargs: {},
        sync_task_lifecycle=lambda **_kwargs: None,
        resolve_verifier_candidates=lambda _raw: [],
        dedupe_roles=lambda rows: [str(item).strip() for item in rows if str(item).strip()],
        touch_chat_recent_task_ref=lambda *_args, **_kwargs: None,
        set_chat_selected_task_ref=lambda *_args, **_kwargs: None,
    )

    assert isinstance(result, dict)
    assert result["terminal"] is False
    assert result["run_selected_execution_lane_ids"] == ["L2"]
    assert result["run_selected_review_lane_ids"] == ["R2"]


def test_orch_followup_rejects_invalid_lane_selector() -> None:
    manager_state = _empty_state()
    manager_state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(ROOT),
        "team_dir": str(ROOT / ".aoe-team"),
        "tasks": {
            "REQ-123": {
                "request_id": "REQ-123",
                "status": "failed",
                "prompt": "followup target",
                "context": {"task_short_id": "T-123"},
                "exec_critic": {
                    "verdict": "intervention",
                    "action": "manual_followup",
                    "reason": "Need operator review",
                    "manual_followup_execution_lane_ids": ["L2"],
                    "manual_followup_review_lane_ids": ["R2"],
                },
            }
        },
    }
    sent: list[tuple[str, str]] = []
    handled = orch_task_handlers.handle_orch_task_command(
        cmd="orch-followup",
        args=argparse.Namespace(
            require_verifier=False,
            verifier_roles="",
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            dry_run=True,
        ),
        manager_state=manager_state,
        chat_id="939062873",
        orch_target="twinpaper",
        orch_add_name=None,
        orch_add_path=None,
        orch_add_overview=None,
        orch_add_init=True,
        orch_add_spawn=True,
        orch_add_set_active=True,
        rest="",
        orch_check_request_id=None,
        orch_task_request_id=None,
        orch_pick_request_id=None,
        orch_cancel_request_id=None,
        orch_followup_request_id="REQ-123",
        orch_followup_lane_ids=["L1"],
        send=lambda text, **kwargs: sent.append((text, kwargs.get("context", ""))) or True,
        log_event=lambda **kwargs: None,
        get_context=lambda orch: (str(orch or "twinpaper"), manager_state["projects"]["twinpaper"], argparse.Namespace(team_dir=str(ROOT / ".aoe-team"))),
        latest_task_request_refs=lambda *args, **kwargs: [],
        set_chat_recent_task_refs=lambda *args, **kwargs: None,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_project_root=lambda raw: Path(raw).expanduser().resolve(),
        is_path_within=lambda path, root: True,
        register_orch_project=lambda *args, **kwargs: ("", {}),
        run_aoe_init=lambda *args, **kwargs: "",
        run_aoe_spawn=lambda *args, **kwargs: "",
        now_iso=lambda: "2026-03-13T12:00:00+0900",
        run_aoe_status=lambda *args, **kwargs: "",
        resolve_chat_task_ref=lambda *_args, **_kwargs: "REQ-123",
        resolve_task_request_id=lambda entry, ref: ref if ref in entry.get("tasks", {}) else "",
        run_request_query=lambda *args, **kwargs: {},
        sync_task_lifecycle=lambda *args, **kwargs: None,
        resolve_verifier_candidates=lambda text: [],
        touch_chat_recent_task_ref=lambda *args, **kwargs: None,
        set_chat_selected_task_ref=lambda *args, **kwargs: None,
        get_chat_selected_task_ref=lambda *args, **kwargs: "",
        get_task_record=lambda entry, req_id: entry.get("tasks", {}).get(req_id),
        summarize_request_state=lambda *args, **kwargs: "",
        summarize_three_stage_request=lambda *args, **kwargs: "",
        summarize_task_lifecycle=lambda *args, **kwargs: "",
        task_display_label=lambda task, fallback_request_id="": str((task or {}).get("context", {}).get("task_short_id") or fallback_request_id),
        cancel_request_assignments=lambda *args, **kwargs: {},
        lifecycle_set_stage=lambda *args, **kwargs: None,
        summarize_cancel_result=lambda *args, **kwargs: "",
    )

    assert handled is True
    assert sent
    assert sent[-1][1] == "orch-followup lane invalid"
    assert "requested follow-up lanes are not allowed" in sent[-1][0]


def test_orch_followup_summarizes_allowed_lane_targets() -> None:
    manager_state = _empty_state()
    manager_state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(ROOT),
        "team_dir": str(ROOT / ".aoe-team"),
        "last_request_id": "REQ-123",
        "tasks": {
            "REQ-123": {
                "request_id": "REQ-123",
                "status": "failed",
                "prompt": "followup target",
                "context": {"task_short_id": "T-123"},
                "exec_critic": {
                    "verdict": "intervention",
                    "action": "manual_followup",
                    "reason": "Need operator review",
                    "manual_followup_execution_lane_ids": ["L2"],
                    "manual_followup_review_lane_ids": ["R2"],
                    "rerun_execution_lane_ids": ["L2"],
                },
            }
        },
    }
    sent: list[tuple[str, str, object]] = []
    handled = orch_task_handlers.handle_orch_task_command(
        cmd="orch-followup",
        args=argparse.Namespace(
            require_verifier=False,
            verifier_roles="",
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            dry_run=True,
        ),
        manager_state=manager_state,
        chat_id="939062873",
        orch_target="twinpaper",
        orch_add_name=None,
        orch_add_path=None,
        orch_add_overview=None,
        orch_add_init=True,
        orch_add_spawn=True,
        orch_add_set_active=True,
        rest="",
        orch_check_request_id=None,
        orch_task_request_id=None,
        orch_pick_request_id=None,
        orch_cancel_request_id=None,
        orch_followup_request_id="REQ-123",
        orch_followup_lane_ids=["L2", "R2"],
        send=lambda text, **kwargs: sent.append((text, kwargs.get("context", ""), kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
        get_context=lambda orch: (str(orch or "twinpaper"), manager_state["projects"]["twinpaper"], argparse.Namespace(team_dir=str(ROOT / ".aoe-team"))),
        latest_task_request_refs=lambda *args, **kwargs: [],
        set_chat_recent_task_refs=lambda *args, **kwargs: None,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_project_root=lambda raw: Path(raw).expanduser().resolve(),
        is_path_within=lambda path, root: True,
        register_orch_project=lambda *args, **kwargs: ("", {}),
        run_aoe_init=lambda *args, **kwargs: "",
        run_aoe_spawn=lambda *args, **kwargs: "",
        now_iso=lambda: "2026-03-13T12:00:00+0900",
        run_aoe_status=lambda *args, **kwargs: "",
        resolve_chat_task_ref=lambda *_args, **_kwargs: "REQ-123",
        resolve_task_request_id=lambda entry, ref: ref if ref in entry.get("tasks", {}) else "",
        run_request_query=lambda *args, **kwargs: {},
        sync_task_lifecycle=lambda *args, **kwargs: None,
        resolve_verifier_candidates=lambda text: [],
        touch_chat_recent_task_ref=lambda *args, **kwargs: None,
        set_chat_selected_task_ref=lambda *args, **kwargs: None,
        get_chat_selected_task_ref=lambda *args, **kwargs: "",
        get_task_record=lambda entry, req_id: entry.get("tasks", {}).get(req_id),
        summarize_request_state=lambda *args, **kwargs: "",
        summarize_three_stage_request=lambda *args, **kwargs: "",
        summarize_task_lifecycle=lambda *args, **kwargs: "",
        task_display_label=lambda task, fallback_request_id="": str((task or {}).get("context", {}).get("task_short_id") or fallback_request_id),
        cancel_request_assignments=lambda *args, **kwargs: {},
        lifecycle_set_stage=lambda *args, **kwargs: None,
        summarize_cancel_result=lambda *args, **kwargs: "",
    )

    assert handled is True
    assert sent
    text, context, reply_markup = sent[-1]
    assert context == "orch-followup"
    assert "manual follow-up" in text
    assert "execution lanes: L2" in text
    assert "review lanes: R2" in text
    assert "Need operator review" in text
    buttons = [btn["text"] for row in (reply_markup or {}).get("keyboard", []) for btn in row]
    assert "/followup T-123" in buttons
    assert "/followup T-123 lane L2" in buttons
    assert "/followup T-123 lane R2" in buttons


def test_resolve_message_command_auto_routes_plain_text_from_direct_bias() -> None:
    manager_state = gw.default_manager_state(ROOT, ROOT / ".aoe-team")
    gw.set_default_mode(manager_state, "939062873", "direct")

    resolved = resolver.resolve_message_command(
        text="결측치 규칙을 검토해줘",
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

    assert resolved.cmd == "run"
    assert resolved.run_force_mode == "dispatch"
    assert resolved.run_auto_source.startswith("orch-action:")


def test_resolve_message_command_forces_dispatch_for_repo_mutation_prompt() -> None:
    manager_state = gw.default_manager_state(ROOT, ROOT / ".aoe-team")
    gw.set_default_mode(manager_state, "939062873", "direct")

    resolved = resolver.resolve_message_command(
        text="KRISS 폴더에서 키워드 추세기가 왜 안 되는지 확인하고 고쳐서 다시 푸시해줘",
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

    assert resolved.cmd == "run"
    assert resolved.run_force_mode == "dispatch"
    assert resolved.run_auto_source.startswith("orch-action:")


def test_parse_quick_message_supports_routine_aliases() -> None:
    assert tg_parse.parse_quick_message("todo") == {"cmd": "todo", "rest": ""}
    assert tg_parse.parse_quick_message("다음 할일") == {"cmd": "todo", "rest": "next"}
    assert tg_parse.parse_quick_message("sync preview 1h") == {"cmd": "sync", "rest": "preview 1h"}
    assert tg_parse.parse_quick_message("동기화 미리보기 O2 3h") == {"cmd": "sync", "rest": "preview O2 3h"}
    assert tg_parse.parse_quick_message("오프데스크") == {"cmd": "offdesk", "rest": "status"}
    assert tg_parse.parse_quick_message("퇴근모드") == {"cmd": "offdesk", "rest": "on"}
    assert tg_parse.parse_quick_message("자동 상태") == {"cmd": "auto", "rest": "status"}


def test_parse_quick_message_keeps_non_command_plain_text_free() -> None:
    assert tg_parse.parse_quick_message("동기화가 계속 꼬이는 이유를 분석해줘") is None
    assert tg_parse.parse_quick_message("자동 실행을 검토해줘") is None
