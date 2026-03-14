#!/usr/bin/env python3
"""Gateway management, auto, and offdesk workflow regression tests."""

from _gateway_test_support import *  # noqa: F401,F403
def _call_management_status(
    *,
    tmp_path: Path,
    manager_state: dict,
    cmd: str,
    rest: str,
) -> str:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    sent: list[str] = []
    ok = mgmt_handlers.handle_management_command(
        cmd=cmd,
        args=argparse.Namespace(
            dry_run=True,
            team_dir=team_dir,
            manager_state_file=team_dir / "orch_manager_state.json",
            default_report_level="normal",
        ),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest=rest,
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent.append(body) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=lambda chat_ref, state: "admin",
        is_owner_chat=lambda chat_ref, state: True,
        ensure_chat_aliases=lambda *args, **kwargs: {},
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    assert ok is True
    assert sent
    return sent[-1]


def _call_management_status_with_markup(
    *,
    tmp_path: Path,
    manager_state: dict,
    cmd: str,
    rest: str,
) -> tuple[str, dict | None]:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    sent: list[tuple[str, dict | None]] = []
    ok = mgmt_handlers.handle_management_command(
        cmd=cmd,
        args=argparse.Namespace(
            dry_run=True,
            team_dir=team_dir,
            manager_state_file=team_dir / "orch_manager_state.json",
            default_report_level="normal",
        ),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest=rest,
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    assert ok is True
    assert sent
    return sent[-1]


def _management_control_kwargs(
    *,
    tmp_path: Path,
    manager_state: dict,
    cmd: str,
    rest: str,
    sent: list[tuple[str, dict | None]],
) -> dict:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    return dict(
        cmd=cmd,
        args=argparse.Namespace(
            dry_run=True,
            team_dir=team_dir,
            manager_state_file=team_dir / "orch_manager_state.json",
            default_report_level="normal",
        ),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        rest=rest,
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_project_entry=mgmt_handlers._resolve_project_entry,
        project_lock_row=mgmt_handlers._project_lock_row,
        project_lock_label=mgmt_handlers._project_lock_label,
        parse_replace_sync_flag=mgmt_handlers._parse_replace_sync_flag,
        normalize_prefetch_token=mgmt_handlers._normalize_prefetch_token,
        prefetch_display=mgmt_handlers._prefetch_display,
        compact_reason=mgmt_handlers._compact_reason,
        status_report_level=mgmt_handlers._status_report_level,
        focused_project_snapshot_lines=mgmt_handlers._focused_project_snapshot_lines,
        ops_scope_summary=mgmt_handlers._ops_scope_summary,
        ops_scope_compact_lines=lambda state, limit, detail_level: mgmt_handlers._ops_scope_compact_lines(
            state, limit=limit, detail_level=detail_level
        ),
        sort_offdesk_reports=mgmt_handlers._sort_offdesk_reports,
        offdesk_prepare_targets=mgmt_handlers._offdesk_prepare_targets,
        offdesk_prepare_project_report=mgmt_handlers._offdesk_prepare_project_report,
        offdesk_review_reply_markup=lambda flagged, clean=False, capacity_operator_action="", capacity_recovery_action="": mgmt_handlers._offdesk_review_reply_markup(
            flagged, clean=clean, capacity_operator_action=capacity_operator_action
            , capacity_recovery_action=capacity_recovery_action
        ),
        offdesk_prepare_reply_markup=lambda reports, blocked_count=0, clean=False: mgmt_handlers._offdesk_prepare_reply_markup(
            reports, blocked_count=blocked_count, clean=clean
        ),
        auto_state_path=mgmt_handlers._auto_state_path,
        offdesk_state_path=mgmt_handlers._offdesk_state_path,
        provider_capacity_state_path=mgmt_handlers._provider_capacity_state_path,
        load_auto_state=mgmt_handlers._load_auto_state,
        save_auto_state=mgmt_handlers._save_auto_state,
        load_offdesk_state=mgmt_handlers._load_offdesk_state,
        save_offdesk_state=mgmt_handlers._save_offdesk_state,
        load_provider_capacity_state=mgmt_handlers._load_provider_capacity_state,
        save_provider_capacity_state=mgmt_handlers._save_provider_capacity_state,
        scheduler_session_name=mgmt_handlers._scheduler_session_name,
        tmux_has_session=mgmt_handlers._tmux_has_session,
        tmux_auto_command=mgmt_handlers._tmux_auto_command,
        now_iso=mgmt_handlers._now_iso,
        default_auto_interval_sec=mgmt_handlers.DEFAULT_AUTO_INTERVAL_SEC,
        default_auto_idle_sec=mgmt_handlers.DEFAULT_AUTO_IDLE_SEC,
        default_auto_max_failures=mgmt_handlers.DEFAULT_AUTO_MAX_FAILURES,
        default_offdesk_command=mgmt_handlers.DEFAULT_OFFDESK_COMMAND,
        default_offdesk_prefetch=mgmt_handlers.DEFAULT_OFFDESK_PREFETCH,
        default_offdesk_prefetch_since=mgmt_handlers.DEFAULT_OFFDESK_PREFETCH_SINCE,
        default_offdesk_report_level=mgmt_handlers.DEFAULT_OFFDESK_REPORT_LEVEL,
        default_offdesk_room=mgmt_handlers.DEFAULT_OFFDESK_ROOM,
    )


def _management_chat_kwargs(
    *,
    tmp_path: Path,
    manager_state: dict,
    cmd: str,
    sent: list[tuple[str, dict | None]],
    mode_setting=None,
    lang_setting=None,
    report_setting=None,
    chat_role="admin",
) -> dict:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    return dict(
        cmd=cmd,
        args=argparse.Namespace(
            dry_run=True,
            team_dir=team_dir,
            manager_state_file=team_dir / "orch_manager_state.json",
            default_lang="ko",
            default_report_level="normal",
        ),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role=chat_role,
        mode_setting=mode_setting,
        lang_setting=lang_setting,
        report_setting=report_setting,
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        cmd_prefix=mgmt_handlers._cmd_prefix,
    )


def _management_acl_kwargs(
    *,
    tmp_path: Path,
    manager_state: dict,
    cmd: str,
    rest: str,
    sent: list[tuple[str, dict | None]],
    current_chat_alias="owner",
    came_from_slash=True,
    acl_grant_scope=None,
    acl_grant_chat_id=None,
    acl_revoke_scope=None,
    acl_revoke_chat_id=None,
    args_override: argparse.Namespace | None = None,
) -> dict:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    args_obj = args_override or argparse.Namespace(
        dry_run=True,
        team_dir=team_dir,
        manager_state_file=team_dir / "orch_manager_state.json",
        default_lang="ko",
        default_report_level="normal",
        allow_chat_ids=set(),
        admin_chat_ids=set(),
        readonly_chat_ids=set(),
        deny_by_default=False,
        owner_only=False,
        owner_chat_id="939062873",
        owner_bootstrap_mode="dispatch",
    )
    return dict(
        cmd=cmd,
        args=args_obj,
        manager_state=manager_state,
        chat_id="939062873",
        current_chat_alias=current_chat_alias,
        rest=rest,
        came_from_slash=came_from_slash,
        acl_grant_scope=acl_grant_scope,
        acl_grant_chat_id=acl_grant_chat_id,
        acl_revoke_scope=acl_revoke_scope,
        acl_revoke_chat_id=acl_revoke_chat_id,
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
        project_lock_label=mgmt_handlers._project_lock_label,
    )


def _button_texts(markup: dict | None) -> list[str]:
    if not isinstance(markup, dict):
        return []
    return [btn["text"] for row in markup.get("keyboard", []) for btn in row if isinstance(btn, dict) and "text" in btn]


def test_offdesk_status_includes_focused_project_snapshot(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(tmp_path / "TwinPaper" / ".aoe-team"),
        "todos": [
            {"id": "TODO-1", "summary": "review schema", "status": "open"},
            {"id": "TODO-2", "summary": "run critic", "status": "running"},
            {"id": "TODO-3", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "pending_todo": {"todo_id": "TODO-2"},
        "last_sync_at": "2026-03-06T11:55:00+0900",
        "last_sync_mode": "scenario",
        "tasks": {
            "REQ-1": {
                "short_id": "T-101",
                "prompt": "Review schema and summarize result",
                "status": "running",
                "updated_at": "2026-03-06T12:00:00+0900",
            }
        },
    }
    state["active"] = "twinpaper"
    gw.set_project_lock(state, "twinpaper")

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="status long")

    assert "project snapshot" in text
    assert "- project: O2 TwinPaper [locked]" in text
    assert "- todo: open=1 running=1 blocked=1 followup=1 pending=yes" in text
    assert "- blocked_head: TODO-3 x2 [manual_followup]" in text
    assert "- last_sync: scenario " in text
    assert "- last_task: T-101 Review schema and summarize result [running]" in text


def test_scheduler_control_module_matches_management_focus_transition(tmp_path: Path) -> None:
    state_a = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_b = copy.deepcopy(state_a)
    twin_root = tmp_path / "TwinPaper"
    twin_team = twin_root / ".aoe-team"
    twin_team.mkdir(parents=True, exist_ok=True)
    state_a["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(twin_root),
        "team_dir": str(twin_team),
        "todos": [],
    }
    state_b["projects"]["twinpaper"] = copy.deepcopy(state_a["projects"]["twinpaper"])

    sent_a: list[tuple[str, dict | None]] = []
    sent_b: list[tuple[str, dict | None]] = []

    ok_a = mgmt_handlers.handle_management_command(
        cmd="focus",
        args=argparse.Namespace(
            dry_run=True,
            team_dir=tmp_path / ".aoe-team",
            manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
            default_report_level="normal",
        ),
        manager_state=state_a,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="O2",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_b = scheduler_control.handle_scheduler_control_command(
        **_management_control_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="focus",
            rest="O2",
            sent=sent_b,
        )
    )

    assert ok_a == ok_b == True
    assert state_a == state_b
    assert sent_a == sent_b


def test_scheduler_control_module_matches_management_offdesk_prepare_and_panic(tmp_path: Path) -> None:
    state_a = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_b = copy.deepcopy(state_a)
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    project_entry = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "status": "open"},
            {"id": "TODO-002", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "todo_proposals": [{"id": "PROP-001", "summary": "shadow gate follow-up", "status": "open"}],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }
    state_a["projects"]["twinpaper"] = copy.deepcopy(project_entry)
    state_b["projects"]["twinpaper"] = copy.deepcopy(project_entry)

    sent_prepare_a: list[tuple[str, dict | None]] = []
    sent_prepare_b: list[tuple[str, dict | None]] = []
    body_a, markup_a = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state_a,
        cmd="offdesk",
        rest="prepare O2",
    )
    ok_b = scheduler_control.handle_scheduler_control_command(
        **_management_control_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="offdesk",
            rest="prepare O2",
            sent=sent_prepare_b,
        )
    )
    assert ok_b is True
    assert sent_prepare_b
    body_b, markup_b = sent_prepare_b[-1]
    assert body_a == body_b
    assert markup_a == markup_b

    sent_panic_a: list[tuple[str, dict | None]] = []
    sent_panic_b: list[tuple[str, dict | None]] = []
    state_c = copy.deepcopy(state_a)
    state_d = copy.deepcopy(state_b)
    ok_a2 = mgmt_handlers.handle_management_command(
        cmd="panic",
        args=argparse.Namespace(
            dry_run=True,
            team_dir=tmp_path / ".aoe-team",
            manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
            default_report_level="normal",
        ),
        manager_state=state_c,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="status",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_panic_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_b2 = scheduler_control.handle_scheduler_control_command(
        **_management_control_kwargs(
            tmp_path=tmp_path,
            manager_state=state_d,
            cmd="panic",
            rest="status",
            sent=sent_panic_b,
        )
    )
    assert ok_a2 == ok_b2 == True
    assert state_c == state_d
    assert sent_panic_a == sent_panic_b


def test_offdesk_prepare_targets_deprioritize_rate_limited_project_capacity(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    blocked_root = tmp_path / "Blocked"
    ready_root = tmp_path / "Ready"
    blocked_team = blocked_root / ".aoe-team"
    ready_team = ready_root / ".aoe-team"
    blocked_team.mkdir(parents=True, exist_ok=True)
    ready_team.mkdir(parents=True, exist_ok=True)
    (blocked_team / "orchestrator.json").write_text("{}", encoding="utf-8")
    (ready_team / "orchestrator.json").write_text("{}", encoding="utf-8")

    state["projects"]["blocked"] = {
        "name": "blocked",
        "display_name": "Blocked",
        "project_alias": "O2",
        "project_root": str(blocked_root),
        "team_dir": str(blocked_team),
        "todos": [
            {"id": "TODO-001", "summary": "parked current", "priority": "P1", "status": "running"},
            {"id": "TODO-002", "summary": "open but capacity heavy", "priority": "P1", "status": "open"},
        ],
        "tasks": {
            "r1": {
                "request_id": "r1",
                "todo_id": "TODO-001",
                "status": "running",
                "tf_phase": "rate_limited",
                "rate_limit": {
                    "mode": "blocked",
                    "limited_providers": ["codex", "claude"],
                    "retry_after_sec": 180,
                    "retry_at": "2999-01-01T00:00:00+00:00",
                },
            }
        },
    }
    state["projects"]["ready"] = {
        "name": "ready",
        "display_name": "Ready",
        "project_alias": "O3",
        "project_root": str(ready_root),
        "team_dir": str(ready_team),
        "todos": [{"id": "TODO-010", "summary": "ready work", "priority": "P1", "status": "open"}],
    }

    rows = offdesk_flow.offdesk_prepare_targets(
        state,
        "all",
        project_lock_row=mgmt_handlers._project_lock_row,
        resolve_project_entry=mgmt_handlers._resolve_project_entry,
    )

    assert [key for key, _entry in rows][:2] == ["ready", "blocked"]


def test_management_chat_module_matches_management_handler_modes(tmp_path: Path) -> None:
    state_a = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_b = copy.deepcopy(state_a)

    sent_a: list[tuple[str, dict | None]] = []
    sent_b: list[tuple[str, dict | None]] = []

    ok_a = mgmt_handlers.handle_management_command(
        cmd="mode",
        args=argparse.Namespace(
            dry_run=True,
            team_dir=tmp_path / ".aoe-team",
            manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
            default_lang="ko",
            default_report_level="normal",
        ),
        manager_state=state_a,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting="dispatch",
        lang_setting=None,
        report_setting=None,
        rest="",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_b = mgmt_chat.handle_chat_management_command(
        **_management_chat_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="mode",
            mode_setting="dispatch",
            sent=sent_b,
        )
    )

    assert ok_a == ok_b == True
    assert state_a == state_b
    assert sent_a == sent_b


def test_management_chat_module_matches_management_handler_tutorial_and_cancel(tmp_path: Path) -> None:
    state_a = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_b = copy.deepcopy(state_a)
    gw.set_pending_mode(state_a, "939062873", "dispatch")
    gw.set_pending_mode(state_b, "939062873", "dispatch")
    gw.set_confirm_action(state_a, chat_id="939062873", mode="dispatch", prompt="ship it", risk="high")
    gw.set_confirm_action(state_b, chat_id="939062873", mode="dispatch", prompt="ship it", risk="high")

    sent_tut_a: list[tuple[str, dict | None]] = []
    sent_tut_b: list[tuple[str, dict | None]] = []
    ok_tut_a = mgmt_handlers.handle_management_command(
        cmd="tutorial",
        args=argparse.Namespace(
            dry_run=True,
            team_dir=tmp_path / ".aoe-team",
            manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
            default_lang="ko",
            default_report_level="normal",
        ),
        manager_state=state_a,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_tut_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_tut_b = mgmt_chat.handle_chat_management_command(
        **_management_chat_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="tutorial",
            sent=sent_tut_b,
        )
    )
    assert ok_tut_a == ok_tut_b == True
    assert sent_tut_a == sent_tut_b

    sent_cancel_a: list[tuple[str, dict | None]] = []
    sent_cancel_b: list[tuple[str, dict | None]] = []
    state_c = copy.deepcopy(state_a)
    state_d = copy.deepcopy(state_b)
    ok_cancel_a = mgmt_handlers.handle_management_command(
        cmd="cancel-pending",
        args=argparse.Namespace(
            dry_run=True,
            team_dir=tmp_path / ".aoe-team",
            manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
            default_lang="ko",
            default_report_level="normal",
        ),
        manager_state=state_c,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_cancel_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_cancel_b = mgmt_chat.handle_chat_management_command(
        **_management_chat_kwargs(
            tmp_path=tmp_path,
            manager_state=state_d,
            cmd="cancel-pending",
            sent=sent_cancel_b,
        )
    )

    assert ok_cancel_a == ok_cancel_b == True
    assert state_c == state_d
    assert sent_cancel_a == sent_cancel_b


def test_management_acl_module_matches_management_handler_identity_and_grant(tmp_path: Path) -> None:
    state_a = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_b = copy.deepcopy(state_a)

    sent_a: list[tuple[str, dict | None]] = []
    sent_b: list[tuple[str, dict | None]] = []
    args_a = argparse.Namespace(
        dry_run=True,
        team_dir=tmp_path / ".aoe-team",
        manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
        default_lang="ko",
        default_report_level="normal",
        allow_chat_ids=set(),
        admin_chat_ids=set(),
        readonly_chat_ids=set(),
        deny_by_default=False,
        owner_only=False,
        owner_chat_id="939062873",
        owner_bootstrap_mode="dispatch",
    )
    args_b = copy.deepcopy(args_a)

    ok_a = mgmt_handlers.handle_management_command(
        cmd="grant",
        args=args_a,
        manager_state=state_a,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="admin 12345",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_b = mgmt_acl.handle_acl_management_command(
        **_management_acl_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="grant",
            rest="admin 12345",
            sent=sent_b,
            args_override=args_b,
        )
    )

    assert ok_a == ok_b == True
    assert args_a.allow_chat_ids == args_b.allow_chat_ids
    assert args_a.admin_chat_ids == args_b.admin_chat_ids
    assert args_a.readonly_chat_ids == args_b.readonly_chat_ids
    assert sent_a == sent_b

    sent_who_a: list[tuple[str, dict | None]] = []
    sent_who_b: list[tuple[str, dict | None]] = []
    ok_who_a = mgmt_handlers.handle_management_command(
        cmd="whoami",
        args=args_a,
        manager_state=state_a,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_who_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_who_b = mgmt_acl.handle_acl_management_command(
        **_management_acl_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="whoami",
            rest="",
            sent=sent_who_b,
            args_override=args_b,
        )
    )
    assert ok_who_a == ok_who_b == True
    assert sent_who_a == sent_who_b
def test_offdesk_flow_module_matches_management_prepare_report_and_markup(tmp_path: Path) -> None:
    project_root = tmp_path / "Proj3"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("- [ ] P1: review schema\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")

    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state["projects"]["proj3"] = {
        "name": "proj3",
        "display_name": "Proj3",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "todos": [
            {"id": "TODO-1", "summary": "review schema", "status": "open", "priority": "P1"},
            {
                "id": "TODO-2",
                "summary": "manual item",
                "status": "blocked",
                "blocked_bucket": "manual_followup",
                "blocked_count": 2,
                "blocked_reason": "need review",
            },
        ],
        "todo_proposals": [{"id": "PROP-1", "summary": "follow up", "status": "open"}],
        "last_sync_mode": "scenario",
        "last_sync_at": "2026-03-11T09:30:00+0900",
    }
    entry = state["projects"]["proj3"]

    report_a = mgmt_handlers._offdesk_prepare_project_report(state, "proj3", entry)
    report_b = offdesk_flow.offdesk_prepare_project_report(state, "proj3", entry)

    assert report_a == report_b
    assert mgmt_handlers._offdesk_review_reply_markup([report_a]) == offdesk_flow.offdesk_review_reply_markup([report_b])
    assert mgmt_handlers._offdesk_prepare_reply_markup([report_a], blocked_count=1) == offdesk_flow.offdesk_prepare_reply_markup([report_b], blocked_count=1)


def test_auto_status_includes_active_project_snapshot_without_lock(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Nano"),
        "team_dir": str(tmp_path / "Nano" / ".aoe-team"),
        "todos": [{"id": "TODO-1", "summary": "collect logs", "status": "open"}],
        "pending_todo": {},
        "last_sync_at": "2026-03-06T11:00:00+0900",
        "last_sync_mode": "fallback:files",
        "tasks": {},
    }
    state["active"] = "nano"

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="auto", rest="status long")

    assert "project snapshot" in text
    assert "- project: O3 Nano" in text
    assert "- todo: open=1 running=0 blocked=0 followup=0 pending=no" in text
    assert "- last_sync: fallback:files " in text
    assert "- last_task: -" in text


def test_auto_status_shows_replace_sync_prefetch_mode(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "939062873",
                "command": "fanout",
                "prefetch": "sync_recent",
                "prefetch_since": "12h",
                "prefetch_replace_sync": True,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state = gw.default_manager_state(tmp_path, team_dir)

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="auto", rest="status")

    assert "- prefetch: sync_recent+replace (full-scope; since ignored)" in text


def test_auto_status_shows_next_retry_at_when_rate_limited_work_is_waiting(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "939062873",
                "command": "next",
                "next_retry_at": "2026-03-14T03:10:00+09:00",
                "last_reason": "no_runnable_open_todo",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (team_dir / "provider_capacity.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-14T03:00:30+09:00",
                "providers": {
                    "claude": {
                        "blocked_count": 2,
                        "project_count": 2,
                        "cooldown_level": "critical",
                        "next_retry_at": "2026-03-14T03:10:00+09:00",
                    },
                    "codex": {
                        "blocked_count": 1,
                        "project_count": 1,
                        "cooldown_level": "elevated",
                        "next_retry_at": "2026-03-14T03:10:00+09:00",
                    },
                },
                "override_history": [
                    {
                        "at": "2026-03-14T02:59:00+09:00",
                        "action": "/auto off",
                        "policy_level": "critical",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state = gw.default_manager_state(tmp_path, team_dir)
    state["projects"]["default"]["project_alias"] = "O1"
    state["projects"]["default"]["tasks"] = {
        "req-201": {
            "request_id": "req-201",
            "label": "T-201",
            "status": "running",
            "rate_limit": {
                "mode": "blocked",
                "limited_providers": ["codex", "claude"],
                "retry_after_sec": 180,
                "retry_at": "2026-03-14T03:10:00+09:00",
            },
            "result": {
                "degraded_by": ["claude_rate_limit->codex"],
            },
        }
    }
    state["projects"]["o2"] = {
        "project_alias": "O2",
        "display_name": "Second",
        "tasks": {
            "req-301": {
                "request_id": "req-301",
                "label": "T-301",
                "status": "running",
                "rate_limit": {
                    "mode": "blocked",
                    "limited_providers": ["claude"],
                    "retry_after_sec": 120,
                    "retry_at": "2026-03-14T03:25:00+09:00",
                },
                "result": {},
            }
        },
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="auto", rest="status")

    assert "- last_reason: no_runnable_open_todo" in text
    assert "- next_retry_at: 2026-03-14T03:10:00+09:00" in text
    assert "- provider_capacity: tasks=2 projects=2 providers=claude=2, codex=1" in text
    assert "- capacity_policy: critical | both primary providers are blocked across multiple tasks/projects" in text
    assert "- capacity_operator_action: /auto off" in text
    assert "- capacity_memory_updated_at: 2026-03-14T03:00:30+09:00" in text
    assert "- provider_memory: claude(blocked=2 projects=2 level=critical wait=medium retry=2026-03-14T03:10:00+09:00), codex(blocked=1 projects=1 level=elevated wait=medium retry=2026-03-14T03:10:00+09:00)" in text
    assert "- capacity_override_last: /auto off @ 2026-03-14T02:59:00+09:00 (critical)" in text
    assert "- next_retry_target: O1 T-201 providers=codex,claude degraded=claude_rate_limit->codex" in text


def test_auto_status_short_compacts_failure_reason_and_uses_ops_summary(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "939062873",
                "command": "fanout",
                "prefetch": "sync_recent",
                "prefetch_replace_sync": True,
                "fail_count": 3,
                "fail_candidate": "local_map_analysis:TODO-003",
                "fail_reason": "plan gate: severity, confidence, conflict_flag cutline definition is missing so the candidate selection remains non-reproducible across retries and blocks automation.",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state = gw.default_manager_state(tmp_path, team_dir)
    state["projects"]["o3"] = {
        "name": "o3",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Nano"),
        "team_dir": str(tmp_path / "Nano" / ".aoe-team"),
        "todos": [{"id": "TODO-1", "summary": "collect logs", "priority": "P1", "status": "open"}],
    }
    state["projects"]["o4"] = {
        "name": "o4",
        "display_name": "Local_Map",
        "project_alias": "O4",
        "project_root": str(tmp_path / "Local_Map"),
        "team_dir": str(tmp_path / "Local_Map" / ".aoe-team"),
        "todos": [{"id": "TODO-2", "summary": "build memo", "priority": "P1", "status": "open"}],
    }
    state["active"] = "o4"

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="auto", rest="status short")

    assert "- report_view: short" in text
    assert "- fail_reason: plan gate:" in text
    assert "fail_reason_full" not in text
    assert "ops projects:" in text
    assert "project snapshot" not in text


def test_auto_status_long_includes_full_failure_reason_and_project_snapshot(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "939062873",
                "command": "next",
                "prefetch": "sync_recent",
                "prefetch_replace_sync": False,
                "fail_count": 1,
                "fail_candidate": "o4:TODO-002",
                "fail_reason": "plan gate: severity, confidence, conflict_flag cutline definition is missing so the candidate selection remains non-reproducible across retries and blocks automation.",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state = gw.default_manager_state(tmp_path, team_dir)
    state["projects"]["o4"] = {
        "name": "o4",
        "display_name": "Local_Map",
        "project_alias": "O4",
        "project_root": str(tmp_path / "Local_Map"),
        "team_dir": str(tmp_path / "Local_Map" / ".aoe-team"),
        "todos": [{"id": "TODO-2", "summary": "build memo", "priority": "P1", "status": "open"}],
        "last_sync_at": "2026-03-06T11:00:00+0900",
        "last_sync_mode": "scenario",
    }
    state["active"] = "o4"

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="auto", rest="status long")

    assert "- report_view: long" in text
    assert "- fail_reason_full: plan gate: severity, confidence, conflict_flag cutline definition is missing" in text
    assert "project snapshot" in text
    assert "ops projects:" in text


def test_offdesk_status_shows_replace_sync_prefetch_mode(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "939062873",
                "command": "fanout",
                "prefetch": "sync_recent",
                "prefetch_since": "12h",
                "prefetch_replace_sync": True,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state = gw.default_manager_state(tmp_path, team_dir)

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="status")

    assert "- auto_prefetch: sync_recent+replace (full-scope; since ignored)" in text


def test_offdesk_prepare_reports_runtime_queue_and_next_actions(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "status": "open"},
            {"id": "TODO-002", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "todo_proposals": [{"id": "PROP-001", "summary": "shadow gate follow-up", "status": "open"}],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="prepare O2")

    assert "offdesk prepare" in text
    assert "- scope: O2" in text
    assert "- O2 TwinPaper [warn]" in text
    assert "runtime: ready" in text
    assert "canonical: TODO.md" in text
    assert "scenario_include: TODO.md" in text
    assert "queue: open=1 running=0 blocked=1 followup=1 pending=no proposals=1" in text
    assert "syncback: done=0 reopen=1 append=0 blocked_notes=1" in text
    assert "blocked_head: TODO-002 x2 [manual_followup]" in text
    assert "warn: 1" in text
    assert "- /offdesk on" in text
    assert "- /sync preview O2 24h" in text
    assert "- /todo O2 syncback preview" in text


def test_offdesk_prepare_warns_when_syncback_drift_exists_without_other_issues(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Nano"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] current task\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-000", "summary": "current task", "priority": "P2", "status": "open"},
            {"id": "TODO-001", "summary": "phase1 rerun", "priority": "P1", "status": "done"},
        ],
        "todo_proposals": [],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="prepare O3")

    assert "- O3 Nano [warn]" in text
    assert "syncback: done=1 reopen=1 append=0 blocked_notes=0" in text
    assert "syncback pending (done=1 reopen=1 append=0 blocked_notes=0)" in text


def test_offdesk_prepare_warns_when_last_sync_used_discovery_mode(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "LocalMap"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] verify export\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["local_map"] = {
        "name": "local_map",
        "display_name": "LocalMap",
        "project_alias": "O4",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [{"id": "TODO-001", "summary": "verify export", "priority": "P1", "status": "open"}],
        "todo_proposals": [],
        "last_sync_at": "2026-03-10T20:00:00+0900",
        "last_sync_mode": "scenario-empty->fallback:bootstrap",
        "last_sync_candidate_classes": {"recent_doc": 2, "todo_file": 1},
        "last_sync_candidate_doc_types": {"handoff": 2, "note": 1},
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="prepare O4")

    assert "- O4 LocalMap [warn]" in text
    assert "sync_source: discovery classes=recent_doc=2, todo_file=1 doc_types=handoff=2, note=1" in text
    assert "last sync used non-canonical discovery mode (scenario-empty->fallback:bootstrap)" in text
    assert "first: /todo O4 syncback preview | canonical TODO drift pending syncback" in text
    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="prepare O4",
    )
    buttons = _button_texts(markup)
    assert "/sync bootstrap O4 24h" in buttons


def test_offdesk_prepare_warns_when_last_sync_uses_non_backlog_doc_types(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Research"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] draft memo\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["research"] = {
        "name": "research",
        "display_name": "Research",
        "project_alias": "O5",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [{"id": "TODO-001", "summary": "draft memo", "priority": "P1", "status": "open"}],
        "todo_proposals": [],
        "last_sync_at": "2026-03-10T20:00:00+0900",
        "last_sync_mode": "scenario",
        "last_sync_candidate_classes": {"scenario": 2},
        "last_sync_candidate_doc_types": {"report": 2},
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="review O5")

    assert "offdesk review" in text
    assert "- O5 Research [warn]" in text
    assert "last sync built backlog from non-backlog documents" in text
    assert "first: /todo O5 syncback preview | canonical TODO drift pending syncback" in text
    assert "do: /todo O5 syncback preview, /sync bootstrap O5 24h, /sync preview O5 24h" in text


def test_offdesk_prepare_reports_active_task_lane_summary_and_targets(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "LaneProject"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] lane retry task\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["lane_project"] = {
        "name": "lane_project",
        "display_name": "LaneProject",
        "project_alias": "O6",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [{"id": "TODO-001", "summary": "lane retry task", "priority": "P1", "status": "open"}],
        "todo_proposals": [],
        "last_sync_at": "2026-03-12T20:00:00+0900",
        "last_sync_mode": "scenario",
        "last_sync_candidate_classes": {"scenario": 1},
        "last_sync_candidate_doc_types": {"todo": 1},
        "tasks": {
            "req-lane": {
                "request_id": "req-lane",
                "short_id": "T-101",
                "status": "running",
                "updated_at": "2026-03-12T21:00:00+0900",
                "created_at": "2026-03-12T20:55:00+0900",
                "exec_critic": {
                    "verdict": "retry",
                    "action": "retry",
                    "reason": "review lane requested rerun",
                    "rerun_execution_lane_ids": ["L2"],
                    "rerun_review_lane_ids": ["R1"],
                },
                "result": {
                    "phase2_request_ids": {
                        "execution": ["REQ-L1", "REQ-L2"],
                        "review": "REQ-R1",
                    },
                    "linked_request_ids": ["REQ-L1", "REQ-L2", "REQ-R1"],
                    "phase2_parallelized": True,
                },
                "plan": {
                    "meta": {
                        "phase2_execution_plan": {
                            "execution_lanes": [{"id": "L1"}, {"id": "L2"}],
                            "review_lanes": [{"id": "R1", "depends_on": ["L2"]}],
                        }
                    }
                },
                "lane_states": {
                    "summary": {
                        "execution": {"done": 1, "running": 1},
                        "review": {"waiting_on_dependencies": 1},
                        "review_verdicts": {"retry": 1},
                    }
                },
            }
        },
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="prepare O6")

    assert "- O6 LaneProject [warn]" in text
    assert "active task needs attention (needs_retry)" in text
    assert "active_task: T-101 | running/needs_retry" in text
    assert "active_task_lanes: lanes E2/R1 | exec done=1, running=1 | review waiting_on_dependencies=1 | review_verdict retry=1" in text
    assert "active_task_requests: execution=2 review=1 linked=3 parallel=yes" in text
    assert "active_task_rerun: execution=L2 review=R1" in text


def test_offdesk_prepare_warns_on_active_task_role_mismatch(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "MismatchProject"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] write handoff\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["mismatch"] = {
        "name": "mismatch",
        "display_name": "MismatchProject",
        "project_alias": "O7",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [{"id": "TODO-001", "summary": "write handoff", "status": "open"}],
        "todo_proposals": [],
        "last_sync_at": "2026-03-13T09:00:00+0900",
        "last_sync_mode": "scenario",
        "last_sync_candidate_classes": {"scenario": 1},
        "last_sync_candidate_doc_types": {"todo": 1},
        "tasks": {
            "REQ-777": {
                "request_id": "REQ-777",
                "short_id": "T-777",
                "status": "running",
                "updated_at": "2026-03-13T10:00:00+0900",
                "created_at": "2026-03-13T09:55:00+0900",
                "result": {
                    "requested_roles": ["Codex-Writer", "Codex-Reviewer"],
                    "executed_roles": ["Codex-Analyst", "Codex-Reviewer"],
                    "dropped_roles": ["Codex-Writer"],
                    "added_roles": ["Codex-Analyst"],
                    "role_mismatch": True,
                },
            }
        },
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="prepare O7")

    assert "- O7 MismatchProject [warn]" in text
    assert "task:role_mismatch" in text
    assert "active_task_roles: requested=Codex-Writer, Codex-Reviewer | executed=Codex-Analyst, Codex-Reviewer" in text
    assert "active_task_role_mismatch: dropped=Codex-Writer added=Codex-Analyst" in text


def test_offdesk_review_surfaces_flagged_projects_and_next_actions(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    root_o2 = tmp_path / "TwinPaper"
    team_o2 = root_o2 / ".aoe-team"
    team_o2.mkdir(parents=True, exist_ok=True)
    (root_o2 / "TODO.md").write_text("# Tasks\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_o2 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o2 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(root_o2),
        "team_dir": str(team_o2),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "status": "open"},
            {"id": "TODO-002", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "todo_proposals": [{"id": "PROP-001", "summary": "shadow gate follow-up", "status": "open"}],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    root_o3 = tmp_path / "Nano"
    team_o3 = root_o3 / ".aoe-team"
    team_o3.mkdir(parents=True, exist_ok=True)
    (root_o3 / "TODO.md").write_text("# Tasks\n- [ ] current task\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_o3 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o3 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(root_o3),
        "team_dir": str(team_o3),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-000", "summary": "current task", "priority": "P2", "status": "open"},
            {"id": "TODO-001", "summary": "phase1 rerun", "priority": "P1", "status": "done"},
        ],
        "todo_proposals": [],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="review all")

    assert "offdesk review" in text
    assert "- reviewed: 2" in text
    assert "- flagged: 2" in text
    assert "- O2 TwinPaper [warn]" in text
    assert "proposal_triage: priorities=P2=1 | kinds=followup=1" in text
    assert "proposal_top: PROP-001[P2 followup 0.00] shadow gate follow-up" in text
    assert "do: /todo O2 syncback preview, /todo O2 proposals, /todo O2 followup" in text
    assert "- O3 Nano [warn]" in text
    assert "do: /todo O3 syncback preview" in text
    assert "- resolve flagged items, then /offdesk on" in text


def test_offdesk_review_reply_markup_includes_active_task_retry_actions(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "LaneProject"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] lane retry task\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["lane_project"] = {
        "name": "lane_project",
        "display_name": "LaneProject",
        "project_alias": "O6",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [{"id": "TODO-001", "summary": "lane retry task", "priority": "P1", "status": "open"}],
        "todo_proposals": [],
        "last_sync_at": "2026-03-12T20:00:00+0900",
        "last_sync_mode": "scenario",
        "last_sync_candidate_classes": {"scenario": 1},
        "last_sync_candidate_doc_types": {"todo": 1},
        "tasks": {
            "req-lane": {
                "request_id": "req-lane",
                "short_id": "T-101",
                "status": "running",
                "updated_at": "2026-03-12T21:00:00+0900",
                "created_at": "2026-03-12T20:55:00+0900",
                "exec_critic": {
                    "verdict": "retry",
                    "action": "retry",
                    "reason": "review lane requested rerun",
                    "rerun_execution_lane_ids": ["L2"],
                    "rerun_review_lane_ids": ["R1"],
                },
                "plan": {
                    "meta": {
                        "phase2_execution_plan": {
                            "execution_lanes": [{"id": "L1"}, {"id": "L2"}],
                            "review_lanes": [{"id": "R1", "depends_on": ["L2"]}],
                        }
                    }
                },
                "lane_states": {
                    "summary": {
                        "execution": {"done": 1, "running": 1},
                        "review": {"waiting_on_dependencies": 1},
                        "review_verdicts": {"retry": 1},
                    }
                },
            }
        },
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review O6",
    )

    assert "offdesk review" in body
    assert "active task needs attention (needs_retry)" in body
    assert "/retry T-101 lane L2,R1" in body
    assert "/task T-101" in body
    buttons = _button_texts(markup)
    assert "/task T-101" in buttons
    assert "/retry T-101 lane L2,R1" in buttons
    assert "/orch status O6" in buttons
    assert "/todo O6" in buttons


def test_offdesk_review_prefers_task_link_for_active_planning_task(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "PlanningProject"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] investigate issue\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["planning_proj"] = {
        "name": "planning_proj",
        "display_name": "PlanningProject",
        "project_alias": "O8",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [{"id": "TODO-001", "summary": "investigate issue", "priority": "P1", "status": "open"}],
        "todo_proposals": [{"id": "PROP-001", "summary": "proposal pending review", "status": "open"}],
        "tasks": {
            "req-planning": {
                "request_id": "req-planning",
                "short_id": "T-201",
                "prompt": "Investigate issue and prepare plan",
                "status": "running",
                "tf_phase": "planning",
                "stages": {"planning": "running"},
                "updated_at": "2026-03-13T18:40:00+0900",
                "created_at": "2026-03-13T18:35:00+0900",
            }
        },
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review O8",
    )

    assert "active task in progress (planning)" in body
    assert "first: /task T-201 | active task is still planning" in body
    buttons = _button_texts(markup)
    assert "/task T-201" in buttons
    assert "/todo O8 proposals" in buttons
    assert "/orch status O8" in buttons


def test_offdesk_review_reply_markup_includes_flagged_project_drilldowns(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    root_o2 = tmp_path / "TwinPaper"
    team_o2 = root_o2 / ".aoe-team"
    team_o2.mkdir(parents=True, exist_ok=True)
    (root_o2 / "TODO.md").write_text("# Tasks\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_o2 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o2 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(root_o2),
        "team_dir": str(team_o2),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "status": "open"},
            {"id": "TODO-002", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "todo_proposals": [{"id": "PROP-001", "summary": "shadow gate follow-up", "status": "open"}],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review O2",
    )

    assert "offdesk review" in body
    buttons = _button_texts(markup)
    assert "/todo O2 syncback preview" in buttons
    assert "/todo O2 proposals" in buttons
    assert "/todo O2 followup" in buttons
    assert "/sync preview O2 24h" in buttons
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/offdesk prepare" in buttons
    assert "/map" in buttons
    assert "/help" in buttons


def test_offdesk_review_reply_markup_includes_clean_actions(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    root_o3 = tmp_path / "Nano"
    team_o3 = root_o3 / ".aoe-team"
    team_o3.mkdir(parents=True, exist_ok=True)
    todo_line = todo_policy.format_canonical_todo_line("P1", "current task", status="open")
    (root_o3 / "TODO.md").write_text(f"# Tasks\n{todo_line}\n", encoding="utf-8")
    (team_o3 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o3 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(root_o3),
        "team_dir": str(team_o3),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "current task", "priority": "P1", "status": "open"},
        ],
        "todo_proposals": [],
        "last_sync_at": "2026-03-12T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review O3",
    )

    assert "offdesk review" in body
    assert "- status: clean" in body
    buttons = _button_texts(markup)
    assert "/offdesk on" in buttons
    assert "/auto status" in buttons
    assert "/offdesk prepare" in buttons
    assert "/map" in buttons
    assert "/help" in buttons


def test_offdesk_prepare_reply_markup_includes_flagged_project_drilldowns(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "status": "open"},
            {"id": "TODO-002", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "todo_proposals": [{"id": "PROP-001", "summary": "shadow gate follow-up", "status": "open"}],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="prepare O2",
    )

    assert "offdesk prepare" in body
    buttons = _button_texts(markup)
    assert "/todo O2 syncback preview" in buttons
    assert "/todo O2 proposals" in buttons
    assert "/todo O2 followup" in buttons
    assert "/sync preview O2 24h" in buttons
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/offdesk review" in buttons
    assert "/map" in buttons
    assert "/queue" in buttons
    assert "/help" in buttons


def test_offdesk_prepare_includes_proposal_triage_summary(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    project_root = tmp_path / "Nano"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] current task\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "current task", "priority": "P1", "status": "open"},
        ],
        "todo_proposals": [
            {
                "id": "PROP-001",
                "summary": "prepare handoff summary for owner review",
                "priority": "P1",
                "kind": "handoff",
                "confidence": 0.92,
                "status": "open",
            },
            {
                "id": "PROP-002",
                "summary": "capture residual risk note for benchmark drift",
                "priority": "P2",
                "kind": "risk",
                "confidence": 0.71,
                "status": "open",
            },
        ],
        "last_sync_at": "2026-03-12T20:00:00+0900",
        "last_sync_mode": "scenario",
        "last_sync_candidate_classes": {"scenario": 4},
        "last_sync_candidate_doc_types": {"todo": 4},
    }

    body, _markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="prepare O3",
    )

    assert "proposal_triage: priorities=P1=1, P2=1 | kinds=handoff=1, risk=1" in body
    assert "proposal_top: PROP-001[P1 handoff 0.92] prepare handoff summary for owner review" in body
    assert "PROP-002[P2 risk 0.71] capture residual risk note for benchmark drift" in body
    assert "high-priority proposals pending review (P1=1, P2=1)" in body
    assert "attention: proposals:2, proposal_p1:P1=1, P2=1" in body or "attention: proposal_p1:P1=1, P2=1, proposals:2" in body
    assert "first: /todo O3 syncback preview | canonical TODO drift pending syncback" in body
    assert _button_texts(_markup)[0] == "/todo O3 syncback preview"


def test_offdesk_review_includes_proposal_triage_summary(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    project_root = tmp_path / "LocalMap"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] current task\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["localmap"] = {
        "name": "localmap",
        "display_name": "LocalMap",
        "project_alias": "O4",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "current task", "priority": "P1", "status": "open"},
        ],
        "todo_proposals": [
            {
                "id": "PROP-001",
                "summary": "draft publish-ready caption set for map panels",
                "priority": "P2",
                "kind": "handoff",
                "confidence": 0.88,
                "status": "open",
            }
        ],
        "last_sync_at": "2026-03-12T20:00:00+0900",
        "last_sync_mode": "scenario",
        "last_sync_candidate_classes": {"scenario": 5},
        "last_sync_candidate_doc_types": {"todo": 5},
    }

    body, _markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review O4",
    )

    assert "proposal_triage: priorities=P2=1 | kinds=handoff=1" in body
    assert "proposal_top: PROP-001[P2 handoff 0.88] draft publish-ready caption set for map panels" in body
    assert "first: /todo O4 syncback preview | canonical TODO drift pending syncback" in body


def test_offdesk_review_sorts_flagged_projects_by_severity(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    root_o3 = tmp_path / "Nano"
    team_o3 = root_o3 / ".aoe-team"
    team_o3.mkdir(parents=True, exist_ok=True)
    (root_o3 / "TODO.md").write_text("# Tasks\n- [ ] current task\n", encoding="utf-8")
    (team_o3 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o3 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(root_o3),
        "team_dir": str(team_o3),
        "runtime_ready": True,
        "todos": [{"id": "TODO-001", "summary": "current task", "priority": "P1", "status": "open"}],
        "todo_proposals": [{"id": "PROP-001", "summary": "owner handoff", "priority": "P1", "kind": "handoff", "confidence": 0.9, "status": "open"}],
        "last_sync_at": "2026-03-12T20:00:00+0900",
        "last_sync_mode": "scenario",
        "last_sync_candidate_classes": {"scenario": 1},
        "last_sync_candidate_doc_types": {"todo": 1},
    }

    root_o4 = tmp_path / "Map"
    team_o4 = root_o4 / ".aoe-team"
    team_o4.mkdir(parents=True, exist_ok=True)
    (root_o4 / "TODO.md").write_text("# Tasks\n- [ ] current task\n", encoding="utf-8")
    (team_o4 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o4 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["map"] = {
        "name": "map",
        "display_name": "Map",
        "project_alias": "O4",
        "project_root": str(root_o4),
        "team_dir": str(team_o4),
        "runtime_ready": False,
        "todos": [],
        "todo_proposals": [],
        "last_sync_mode": "never",
    }

    body = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="review all")

    idx_o4 = body.index("- O4 Map [blocked]")
    idx_o3 = body.index("- O3 Nano [warn]")
    assert idx_o4 < idx_o3
    assert "attention: backlog:none, sync:never" in body
    assert "first: /sync bootstrap O4 24h | bootstrap backlog because the project has never been synced" in body


def test_offdesk_review_sorts_rate_limited_projects_by_capacity_pressure(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    root_o3 = tmp_path / "Nano"
    team_o3 = root_o3 / ".aoe-team"
    team_o3.mkdir(parents=True, exist_ok=True)
    (root_o3 / "TODO.md").write_text("# Tasks\n- [ ] current task\n", encoding="utf-8")
    (team_o3 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o3 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(root_o3),
        "team_dir": str(team_o3),
        "runtime_ready": True,
        "todos": [{"id": "TODO-001", "summary": "current task", "priority": "P1", "status": "running"}],
        "tasks": {
            "r1": {
                "request_id": "r1",
                "label": "T-301",
                "status": "running",
                "tf_phase": "rate_limited",
                "rate_limit": {
                    "mode": "blocked",
                    "limited_providers": ["claude"],
                    "retry_after_sec": 180,
                    "retry_at": "2026-03-14T03:20:00+09:00",
                },
            }
        },
        "last_sync_at": "2026-03-12T20:00:00+0900",
        "last_sync_mode": "scenario",
        "last_sync_candidate_classes": {"scenario": 1},
        "last_sync_candidate_doc_types": {"todo": 1},
    }

    root_o4 = tmp_path / "Map"
    team_o4 = root_o4 / ".aoe-team"
    team_o4.mkdir(parents=True, exist_ok=True)
    (root_o4 / "TODO.md").write_text("# Tasks\n- [ ] current task\n", encoding="utf-8")
    (team_o4 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o4 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["map"] = {
        "name": "map",
        "display_name": "Map",
        "project_alias": "O4",
        "project_root": str(root_o4),
        "team_dir": str(team_o4),
        "runtime_ready": True,
        "todos": [{"id": "TODO-001", "summary": "current task", "priority": "P1", "status": "running"}],
        "tasks": {
            "r2": {
                "request_id": "r2",
                "label": "T-401",
                "status": "running",
                "tf_phase": "rate_limited",
                "rate_limit": {
                    "mode": "blocked",
                    "limited_providers": ["codex", "claude"],
                    "retry_after_sec": 180,
                    "retry_at": "2026-03-14T04:00:00+09:00",
                },
            }
        },
        "last_sync_at": "2026-03-12T20:00:00+0900",
        "last_sync_mode": "scenario",
        "last_sync_candidate_classes": {"scenario": 1},
        "last_sync_candidate_doc_types": {"todo": 1},
    }

    body = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="review all")

    idx_o4 = body.index("- O4 Map [warn]")
    idx_o3 = body.index("- O3 Nano [warn]")
    assert idx_o4 < idx_o3


def test_offdesk_prepare_reply_markup_includes_clean_actions(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    root_o3 = tmp_path / "Nano"
    team_o3 = root_o3 / ".aoe-team"
    team_o3.mkdir(parents=True, exist_ok=True)
    todo_line = todo_policy.format_canonical_todo_line("P1", "current task", status="open")
    (root_o3 / "TODO.md").write_text(f"# Tasks\n{todo_line}\n", encoding="utf-8")
    (team_o3 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o3 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(root_o3),
        "team_dir": str(team_o3),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "current task", "priority": "P1", "status": "open"},
        ],
        "todo_proposals": [],
        "last_sync_at": "2026-03-12T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="prepare O3",
    )

    assert "offdesk prepare" in body
    buttons = _button_texts(markup)
    assert "/offdesk on" in buttons
    assert "/offdesk review" in buttons
    assert "/auto status" in buttons
    assert "/map" in buttons
    assert "/queue" in buttons
    assert "/help" in buttons


def test_auto_prefetch_plan_switches_to_replace_sync_full_scope() -> None:
    desc, commands = auto_sched._prefetch_plan("sync_recent", "12h", True)

    assert desc == "sync_recent+replace (full-scope; since ignored)"
    assert commands == [("/sync replace all quiet", "replace")]


def test_auto_prefetch_plan_uses_incremental_files_and_recent_when_replace_disabled() -> None:
    desc, commands = auto_sched._prefetch_plan("sync_recent", "3h", False)

    assert desc == "sync files+salvage all since=3h quiet"
    assert commands == [
        ("/sync files all since 3h quiet", "files"),
        ("/sync salvage all since 3h quiet", "salvage"),
    ]


def test_auto_scheduler_tracks_next_rate_limited_retry_at() -> None:
    state = {
        "projects": {
            "o3": {"tasks": {"r1": {"rate_limit": {"mode": "blocked", "retry_at": "2026-03-14T03:40:00+09:00"}}}},
            "o4": {"tasks": {"r2": {"rate_limit": {"mode": "blocked", "retry_at": "2026-03-14T03:10:00+09:00"}}}},
        }
    }

    next_retry_at = auto_sched._next_rate_limited_retry_at(
        state,
        now=auto_sched._parse_iso_dt("2026-03-14T03:00:00+09:00"),
    )
    assert next_retry_at == "2026-03-13T18:10:00+00:00"


def test_auto_scheduler_adjusts_idle_to_upcoming_retry_at() -> None:
    sleep_sec = auto_sched._adjust_idle_for_retry_at(
        20.0,
        "2026-03-14T03:00:05+09:00",
        now=auto_sched._parse_iso_dt("2026-03-14T03:00:00+09:00"),
    )
    assert 4.0 <= sleep_sec <= 5.0


def test_auto_scheduler_builds_provider_capacity_snapshot() -> None:
    state = {
        "projects": {
            "o3": {
                "project_alias": "O3",
                "tasks": {
                    "req-201": {
                        "label": "T-201",
                        "rate_limit": {
                            "mode": "blocked",
                            "limited_providers": ["codex", "claude"],
                            "retry_at": "2026-03-14T03:10:00+09:00",
                        },
                    }
                },
            },
            "o4": {
                "project_alias": "O4",
                "tasks": {
                    "req-301": {
                        "label": "T-301",
                        "rate_limit": {
                            "mode": "blocked",
                            "limited_providers": ["claude"],
                            "retry_at": "2026-03-14T03:25:00+09:00",
                        },
                    }
                },
            },
        }
    }

    snapshot = auto_sched._provider_capacity_snapshot(
        state,
        now=auto_sched._parse_iso_dt("2026-03-14T03:00:00+09:00"),
    )

    assert snapshot["summary"]["task_count"] == "2"
    assert snapshot["summary"]["project_count"] == "2"
    assert snapshot["summary"]["provider_summary"] == "claude=2, codex=1"
    assert snapshot["summary"]["policy_level"] == "critical"
    assert snapshot["summary"]["operator_action"] == "/auto off"
    assert snapshot["providers"]["claude"]["blocked_count"] == 2
    assert snapshot["providers"]["claude"]["project_count"] == 2
    assert snapshot["providers"]["claude"]["cooldown_level"] == "critical"
    assert snapshot["providers"]["claude"]["retry_wait_bucket"] == "medium"
    assert snapshot["providers"]["codex"]["blocked_count"] == 1
    assert snapshot["providers"]["codex"]["project_count"] == 1
    assert snapshot["providers"]["codex"]["cooldown_level"] == "elevated"
    assert snapshot["providers"]["codex"]["retry_wait_bucket"] == "medium"


def test_auto_scheduler_escalates_single_provider_to_critical_on_long_retry_wait() -> None:
    state = {
        "projects": {
            "o3": {
                "project_alias": "O3",
                "tasks": {
                    "req-201": {
                        "label": "T-201",
                        "rate_limit": {
                            "mode": "blocked",
                            "limited_providers": ["claude"],
                            "retry_at": "2026-03-14T04:00:00+09:00",
                        },
                    }
                },
            }
        }
    }

    snapshot = auto_sched._provider_capacity_snapshot(
        state,
        now=auto_sched._parse_iso_dt("2026-03-14T03:00:00+09:00"),
    )

    assert snapshot["providers"]["claude"]["cooldown_level"] == "critical"
    assert snapshot["providers"]["claude"]["retry_wait_bucket"] == "long"


def test_auto_off_records_provider_capacity_override_history(tmp_path: Path, monkeypatch) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    provider_state_path = team_dir / mgmt_handlers.PROVIDER_CAPACITY_STATE_FILENAME
    provider_state_path.write_text(json.dumps({}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    manager_state = gw.default_manager_state(tmp_path, team_dir)
    manager_state["projects"]["default"]["project_alias"] = "O1"
    manager_state["projects"]["default"]["tasks"] = {
        "req-201": {
            "request_id": "req-201",
            "label": "T-201",
            "status": "running",
            "rate_limit": {
                "mode": "blocked",
                "limited_providers": ["codex", "claude"],
                "retry_at": "2026-03-14T03:10:00+09:00",
            },
        }
    }
    manager_state["projects"]["o2"] = {
        "project_alias": "O2",
        "tasks": {
            "req-301": {
                "request_id": "req-301",
                "label": "T-301",
                "status": "running",
                "rate_limit": {
                    "mode": "blocked",
                    "limited_providers": ["claude"],
                    "retry_at": "2026-03-14T03:25:00+09:00",
                },
            }
        },
    }
    sent: list[tuple[str, dict | None]] = []
    kwargs = _management_control_kwargs(tmp_path=tmp_path, manager_state=manager_state, cmd="auto", rest="off", sent=sent)
    kwargs["args"].dry_run = False
    kwargs["tmux_auto_command"] = lambda args, action: (True, "stopped")
    monkeypatch.setattr(mgmt_handlers, "_now_iso", lambda: "2026-03-14T03:01:00+09:00")
    kwargs["now_iso"] = mgmt_handlers._now_iso

    ok = scheduler_control.handle_scheduler_control_command(**kwargs)

    assert ok is True
    state = mgmt_handlers._load_provider_capacity_state(provider_state_path)
    history = state.get("override_history")
    assert isinstance(history, list) and history
    last = history[-1]
    assert last["action"] == "/auto off"
    assert last["policy_level"] == "critical"
    assert last["providers"] == "claude=2, codex=1"


def test_auto_status_shows_capacity_recovery_action_when_auto_is_disabled_after_override(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": False,
                "chat_id": "939062873",
                "command": "fanout",
                "prefetch": "sync_recent",
                "prefetch_replace_sync": True,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (team_dir / "provider_capacity.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-14T03:30:00+09:00",
                "providers": {},
                "override_history": [
                    {
                        "at": "2026-03-14T03:00:00+09:00",
                        "action": "/auto off",
                        "policy_level": "critical",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state = gw.default_manager_state(tmp_path, team_dir)

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="auto", rest="status")

    assert "- capacity_recovery_action: /auto recover" in text
    assert "- capacity_recovery_reason: capacity cooldown has cleared; resume the auto scheduler" in text
    assert "- capacity_recovery_target: fanout + sync_recent+replace (full-scope; since ignored)" in text


def test_auto_recover_reenables_scheduler_and_records_override_history(tmp_path: Path, monkeypatch) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    auto_state_path = team_dir / "auto_scheduler.json"
    auto_state_path.write_text(
        json.dumps(
            {
                "enabled": False,
                "chat_id": "939062873",
                "command": "next",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    provider_state_path = team_dir / mgmt_handlers.PROVIDER_CAPACITY_STATE_FILENAME
    provider_state_path.write_text(
        json.dumps(
            {
                "updated_at": "2026-03-14T03:30:00+09:00",
                "providers": {
                    "claude": {
                        "blocked_count": 1,
                        "project_count": 1,
                        "cooldown_level": "cooldown",
                        "next_retry_at": "2026-03-14T03:00:00+09:00",
                    }
                },
                "override_history": [
                    {
                        "at": "2026-03-14T02:50:00+09:00",
                        "action": "/auto off",
                        "policy_level": "critical",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    manager_state = gw.default_manager_state(tmp_path, team_dir)
    sent: list[tuple[str, dict | None]] = []
    kwargs = _management_control_kwargs(tmp_path=tmp_path, manager_state=manager_state, cmd="auto", rest="recover", sent=sent)
    kwargs["args"].dry_run = False
    kwargs["tmux_auto_command"] = lambda args, action: (True, "started")
    monkeypatch.setattr(mgmt_handlers, "_now_iso", lambda: "2026-03-14T03:31:00+09:00")
    kwargs["now_iso"] = mgmt_handlers._now_iso

    ok = scheduler_control.handle_scheduler_control_command(**kwargs)

    assert ok is True
    auto_state = mgmt_handlers._load_auto_state(auto_state_path)
    provider_state = mgmt_handlers._load_provider_capacity_state(provider_state_path)
    assert auto_state["enabled"] is True
    assert auto_state["command"] == "next"
    assert auto_state["recovered_at"] == "2026-03-14T03:31:00+09:00"
    assert str(auto_state.get("recovery_grace_until", "")).strip()
    history = provider_state.get("override_history")
    assert isinstance(history, list) and history
    assert history[-1]["action"] == "/auto recover"
    assert "providers" not in provider_state or provider_state.get("providers") == {}
    assert sent
    assert "auto scheduler recovered" in sent[-1][0]
    assert "- resume_target: next" in sent[-1][0]
    assert "- recovery_grace_until:" in sent[-1][0]


def test_offdesk_review_clean_keyboard_includes_auto_recover_when_available(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "provider_capacity.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-14T03:30:00+09:00",
                "providers": {},
                "override_history": [
                    {
                        "at": "2026-03-14T03:00:00+09:00",
                        "action": "/auto off",
                        "policy_level": "critical",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review all",
    )

    assert "- capacity_recovery_action: /auto recover" in body
    assert "- capacity_recovery_target: next" in body
    buttons = _button_texts(markup)
    assert "/auto recover" in buttons


def test_offdesk_review_shows_recovery_grace_until_when_auto_recently_recovered(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "939062873",
                "command": "next",
                "recovered_at": "2026-03-14T03:31:00+09:00",
                "recovery_grace_until": "2026-03-14T03:41:00+00:00",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    body, _markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review all",
    )

    assert "- recovery_grace_until: 2026-03-14T03:41:00+00:00" in body


def _write_tf_exec_map(team_dir: Path, req_id: str, *, mode: str, workdir: Path, run_dir: Path) -> None:
    m = gw.load_tf_exec_map(team_dir)
    m[req_id] = {
        "request_id": req_id,
        "gateway_request_id": req_id,
        "created_at": "2026-02-27T00:00:00+0000",
        "mode": mode,
        # Keep repo_root non-existent to avoid invoking git in tests.
        "repo_root": str(team_dir / "_no_such_repo_"),
        "workdir": str(workdir),
        "run_dir": str(run_dir),
        "branch": "",
        "worktree_created": True,
        "status": "running",
    }
    gw.save_tf_exec_map(team_dir, m)


def test_cleanup_tf_exec_artifacts_success_only_prunes_failed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("AOE_TF_ARTIFACT_POLICY", raising=False)  # default: success-only
    monkeypatch.setenv("AOE_TF_EXEC_CACHE_TTL_HOURS", "0")

    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state_path = team_dir / "orch_manager_state.json"

    req_ok = "REQ-OK"
    req_fail = "REQ-FAIL"
    run_ok = team_dir / "tf_runs" / req_ok
    run_fail = team_dir / "tf_runs" / req_fail
    run_ok.mkdir(parents=True, exist_ok=True)
    run_fail.mkdir(parents=True, exist_ok=True)
    work_ok = tmp_path / "work_ok"
    work_fail = tmp_path / "work_fail"
    work_ok.mkdir(parents=True, exist_ok=True)
    work_fail.mkdir(parents=True, exist_ok=True)

    _write_tf_exec_map(team_dir, req_ok, mode="worktree", workdir=work_ok, run_dir=run_ok)
    _write_tf_exec_map(team_dir, req_fail, mode="worktree", workdir=work_fail, run_dir=run_fail)

    state = {
        "projects": {
            "default": {
                "tasks": {
                    req_ok: {"status": "completed", "exec_critic": {"verdict": "success"}},
                    req_fail: {"status": "failed", "exec_critic": {"verdict": "fail"}},
                }
            }
        }
    }

    gw.cleanup_tf_exec_artifacts(manager_state_path, state)

    tf_map = gw.load_tf_exec_map(team_dir)
    assert req_ok in tf_map
    assert req_fail not in tf_map
    assert run_ok.exists()
    assert work_ok.exists()
    assert not run_fail.exists()
    assert not work_fail.exists()


def test_cleanup_tf_exec_artifacts_none_prunes_all(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_TF_ARTIFACT_POLICY", "none")
    monkeypatch.setenv("AOE_TF_EXEC_CACHE_TTL_HOURS", "0")

    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state_path = team_dir / "orch_manager_state.json"

    req_ok = "REQ-OK"
    req_fail = "REQ-FAIL"
    run_ok = team_dir / "tf_runs" / req_ok
    run_fail = team_dir / "tf_runs" / req_fail
    run_ok.mkdir(parents=True, exist_ok=True)
    run_fail.mkdir(parents=True, exist_ok=True)
    work_ok = tmp_path / "work_ok"
    work_fail = tmp_path / "work_fail"
    work_ok.mkdir(parents=True, exist_ok=True)
    work_fail.mkdir(parents=True, exist_ok=True)

    _write_tf_exec_map(team_dir, req_ok, mode="worktree", workdir=work_ok, run_dir=run_ok)
    _write_tf_exec_map(team_dir, req_fail, mode="worktree", workdir=work_fail, run_dir=run_fail)

    state = {
        "projects": {
            "default": {
                "tasks": {
                    req_ok: {"status": "completed", "exec_critic": {"verdict": "success"}},
                    req_fail: {"status": "failed", "exec_critic": {"verdict": "fail"}},
                }
            }
        }
    }

    gw.cleanup_tf_exec_artifacts(manager_state_path, state)

    tf_map = gw.load_tf_exec_map(team_dir)
    assert tf_map == {}
    assert not run_ok.exists()
    assert not work_ok.exists()
    assert not run_fail.exists()
    assert not work_fail.exists()


def test_cleanup_tf_exec_artifacts_all_keeps_all(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_TF_ARTIFACT_POLICY", "all")
    monkeypatch.setenv("AOE_TF_EXEC_CACHE_TTL_HOURS", "0")

    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state_path = team_dir / "orch_manager_state.json"

    req_ok = "REQ-OK"
    req_fail = "REQ-FAIL"
    run_ok = team_dir / "tf_runs" / req_ok
    run_fail = team_dir / "tf_runs" / req_fail
    run_ok.mkdir(parents=True, exist_ok=True)
    run_fail.mkdir(parents=True, exist_ok=True)
    work_ok = tmp_path / "work_ok"
    work_fail = tmp_path / "work_fail"
    work_ok.mkdir(parents=True, exist_ok=True)
    work_fail.mkdir(parents=True, exist_ok=True)

    _write_tf_exec_map(team_dir, req_ok, mode="worktree", workdir=work_ok, run_dir=run_ok)
    _write_tf_exec_map(team_dir, req_fail, mode="worktree", workdir=work_fail, run_dir=run_fail)

    state = {
        "projects": {
            "default": {
                "tasks": {
                    req_ok: {"status": "completed", "exec_critic": {"verdict": "success"}},
                    req_fail: {"status": "failed", "exec_critic": {"verdict": "fail"}},
                }
            }
        }
    }

    gw.cleanup_tf_exec_artifacts(manager_state_path, state)

    tf_map = gw.load_tf_exec_map(team_dir)
    assert req_ok in tf_map
    assert req_fail in tf_map
    assert run_ok.exists()
    assert work_ok.exists()
    assert run_fail.exists()
    assert work_fail.exists()


def test_sync_preview_uses_fallback_without_mutating_queue(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    entry = state["projects"]["default"]
    entry["project_alias"] = "O1"
    project_root = Path(str(entry["project_root"]))
    team_dir = Path(str(entry["team_dir"]))
    project_root.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text(
        "# TODO\n\n- [ ] P1: finish fallback preview\n- [ ] P2: verify sync preview\n",
        encoding="utf-8",
    )

    sent: list[str] = []
    saves: list[Path] = []
    args = argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json")

    def _send(body: str, **_kwargs) -> bool:
        sent.append(body)
        return True

    def _get_context(raw_target: str | None):
        token = str(raw_target or "").strip()
        if token in {"", "default", "O1"}:
            return "default", entry, argparse.Namespace(project_root=project_root, team_dir=team_dir)
        raise RuntimeError(f"unexpected target: {token}")

    result = sched.handle_scheduler_command(
        cmd="sync",
        args=args,
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="preview O1",
        send=_send,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: saves.append(path),
        now_iso=lambda: "2026-03-06T12:00:00+0900",
    )

    assert result == {"terminal": True}
    assert saves == []
    assert sent
    text = sent[-1]
    assert "sync preview" in text
    assert "mode: scenario" in text
    assert "candidate_classes: todo_file=2" in text
    assert "would_add: 2" in text
    assert "finish fallback preview" in text
    assert entry.get("todos") in (None, [])


def test_sync_with_explicit_other_project_under_focus_returns_operator_message(tmp_path: Path) -> None:
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
        rest="O3",
        send=_send,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-06T12:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1]
    assert "sync blocked by project lock" in text
    assert "- locked: O2" in text
    assert "- requested: O3" in text
    assert "/sync preview O2 1h" in text
    assert "/focus off" in text


def test_next_selects_open_todo_even_when_project_has_blocked_row(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Local"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {},
        "todos": [
            {"id": "TODO-001", "summary": "blocked row", "priority": "P1", "status": "blocked"},
            {"id": "TODO-002", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }

    sent: list[str] = []
    saves: list[Path] = []

    def _send(body: str, **_kwargs) -> bool:
        sent.append(body)
        return True

    def _get_context(raw_target: str | None):
        key, entry = gw.get_manager_project(state, raw_target)
        return key, entry, argparse.Namespace(project_root=Path(entry["project_root"]), team_dir=Path(entry["team_dir"]))

    result = sched.handle_scheduler_command(
        cmd="next",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="",
        send=_send,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: saves.append(path),
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result["terminal"] is False
    assert result["cmd"] == "run"
    assert result["orch_target"] == "local"
    assert result["run_prompt"] == "open row"
    assert state["projects"]["local"]["pending_todo"]["todo_id"] == "TODO-002"
    assert sent
    assert "next selected (global)" in sent[-1]


def test_next_selected_warns_when_manual_followup_blocked_backlog_exists(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Local"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {},
        "todos": [
            {
                "id": "TODO-001",
                "summary": "blocked row",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            },
            {"id": "TODO-002", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }

    sent: list[str] = []

    def _get_context(raw_target: str | None):
        key, entry = gw.get_manager_project(state, raw_target)
        return key, entry, argparse.Namespace(project_root=Path(entry["project_root"]), team_dir=Path(entry["team_dir"]))

    result = sched.handle_scheduler_command(
        cmd="next",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result["terminal"] is False
    assert sent
    assert "attention: blocked backlog TODO-001 x2 [manual_followup] | critic unresolved after repair" in sent[-1]


def test_queue_includes_blocked_head_summary(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Local"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {},
        "todos": [
            {
                "id": "TODO-001",
                "summary": "blocked row",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 3,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            },
            {"id": "TODO-002", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }

    sent: list[str] = []

    result = sched.handle_scheduler_command(
        cmd="queue",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("local", state["projects"]["local"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1]
    assert "open=1 running=0 blocked=1 done=0 followup=1" in text
    assert "blocked_head: TODO-001 x3 [manual_followup] | critic unresolved after repair" in text


def test_queue_reply_markup_includes_followup_button_when_present(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Local"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {},
        "todos": [
            {
                "id": "TODO-001",
                "summary": "blocked row",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            },
            {"id": "TODO-002", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }

    sent: list[tuple[str, dict | None]] = []

    result = sched.handle_scheduler_command(
        cmd="queue",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="",
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_context=lambda raw: ("local", state["projects"]["local"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    markup = sent[-1][1] or {}
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/queue followup" in buttons
    assert "/todo O3" in buttons
    assert "/orch status O3" in buttons


def test_queue_followup_filters_to_projects_with_manual_followup(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    local_root = tmp_path / "Local"
    nano_root = tmp_path / "Nano"
    (local_root / ".aoe-team").mkdir(parents=True, exist_ok=True)
    (nano_root / ".aoe-team").mkdir(parents=True, exist_ok=True)
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(local_root),
        "team_dir": str(local_root / ".aoe-team"),
        "tasks": {},
        "todos": [
            {
                "id": "TODO-001",
                "summary": "blocked row",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            }
        ],
    }
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O4",
        "project_root": str(nano_root),
        "team_dir": str(nano_root / ".aoe-team"),
        "tasks": {},
        "todos": [
            {"id": "TODO-010", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }

    sent: list[tuple[str, dict | None]] = []

    result = sched.handle_scheduler_command(
        cmd="queue",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="followup",
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_context=lambda raw: ("local", state["projects"]["local"], argparse.Namespace(project_root=local_root, team_dir=local_root / ".aoe-team")),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1][0]
    assert "manual follow-up queue" in text
    assert "O3 Local" in text
    assert "followup=1" in text
    assert "O4 Nano" not in text
    markup = sent[-1][1] or {}
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/todo O3 followup" in buttons
    assert "/todo O3 ackrun 1" in buttons
    assert "/orch status O3" in buttons
    assert "/queue" in buttons


def test_queue_followup_empty_includes_focused_project_drilldown(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    local_root = tmp_path / "Local"
    (local_root / ".aoe-team").mkdir(parents=True, exist_ok=True)
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(local_root),
        "team_dir": str(local_root / ".aoe-team"),
        "tasks": {},
        "todos": [
            {"id": "TODO-010", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }
    gw.set_project_lock(state, "local")

    sent: list[tuple[str, dict | None]] = []

    result = sched.handle_scheduler_command(
        cmd="queue",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="followup",
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_context=lambda raw: ("local", state["projects"]["local"], argparse.Namespace(project_root=local_root, team_dir=local_root / ".aoe-team")),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1][0]
    assert text == "manual follow-up queue: empty."
    markup = sent[-1][1] or {}
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/todo O3 followup" in buttons
    assert "/todo O3" in buttons
    assert "/orch status O3" in buttons
    assert "/focus off" in buttons


def test_sync_records_last_sync_even_when_queue_is_unchanged(tmp_path: Path) -> None:
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


def test_offdesk_prepare_shows_rate_limited_and_degraded_active_task(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    entry = state["projects"]["default"]
    entry["project_alias"] = "O1"
    entry["display_name"] = "Demo"
    entry["ops_hidden"] = False
    entry["system_project"] = False
    entry["runtime_ready"] = True
    project_root = Path(str(entry["project_root"]))
    team_dir = Path(str(entry["team_dir"]))
    project_root.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# TODO\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text(f"@include {project_root / 'TODO.md'}\n", encoding="utf-8")
    entry["todos"] = [{"id": "TODO-001", "summary": "resume task", "priority": "P1", "status": "open"}]
    entry["tasks"] = {
        "r_demo": {
            "request_id": "r_demo",
            "label": "T-001",
            "short_id": "T-001",
            "status": "running",
            "roles": ["Codex-Writer", "Codex-Reviewer"],
            "rate_limit": {
                "mode": "blocked",
                "limited_providers": ["codex", "claude"],
                "retry_after_sec": 180,
                "retry_at": "2026-03-14T01:23:00+09:00",
            },
            "result": {
                "degraded_by": ["claude_rate_limit->codex"],
                "requested_roles": ["Codex-Writer", "Codex-Reviewer"],
                "executed_roles": ["Codex-Writer", "Codex-Reviewer"],
            },
            "updated_at": "2026-03-14T01:20:00+0900",
        }
    }

    text = _call_management_status(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="prepare O1",
    )

    assert "task:rate_limited" in text
    assert "capacity:codex,claude" in text
    assert "first: /task T-001 | active task is waiting for provider capacity until 2026-03-14T01:23:00+09:00" in text
    assert "active_task_degraded_by: claude_rate_limit->codex" in text
    assert "active_task_rate_limit: mode=blocked providers=codex,claude retry_after=180s retry_at=2026-03-14T01:23:00+09:00" in text
    assert "provider_capacity: providers=codex,claude retry_at=2026-03-14T01:23:00+09:00 degraded=claude_rate_limit->codex" in text


def test_offdesk_review_surfaces_provider_capacity_for_rate_limited_task(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "provider_capacity.json").write_text(
        json.dumps(
            {
                "updated_at": "2026-03-14T01:21:00+09:00",
                "providers": {
                    "claude": {
                        "blocked_count": 1,
                        "project_count": 1,
                        "cooldown_level": "cooldown",
                        "next_retry_at": "2026-03-14T01:23:00+09:00",
                    },
                    "codex": {
                        "blocked_count": 1,
                        "project_count": 1,
                        "cooldown_level": "elevated",
                        "next_retry_at": "2026-03-14T01:23:00+09:00",
                    },
                },
                "override_history": [
                    {
                        "at": "2026-03-14T01:20:00+09:00",
                        "action": "/auto status",
                        "policy_level": "elevated",
                    }
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    entry = state["projects"]["default"]
    entry["project_alias"] = "O1"
    entry["display_name"] = "Demo"
    entry["ops_hidden"] = False
    entry["system_project"] = False
    entry["runtime_ready"] = True
    project_root = Path(str(entry["project_root"]))
    team_dir = Path(str(entry["team_dir"]))
    project_root.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# TODO\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text(f"@include {project_root / 'TODO.md'}\n", encoding="utf-8")
    entry["todos"] = [{"id": "TODO-001", "summary": "resume task", "priority": "P1", "status": "open"}]
    entry["tasks"] = {
        "r_demo": {
            "request_id": "r_demo",
            "label": "T-001",
            "short_id": "T-001",
            "status": "running",
            "roles": ["Codex-Writer", "Codex-Reviewer"],
            "rate_limit": {
                "mode": "blocked",
                "limited_providers": ["codex", "claude"],
                "retry_after_sec": 180,
                "retry_at": "2026-03-14T01:23:00+09:00",
            },
            "result": {
                "degraded_by": ["claude_rate_limit->codex"],
                "requested_roles": ["Codex-Writer", "Codex-Reviewer"],
                "executed_roles": ["Codex-Writer", "Codex-Reviewer"],
            },
            "updated_at": "2026-03-14T01:20:00+0900",
        }
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review O1",
    )

    assert "offdesk review" in body
    assert "- provider_capacity: tasks=1 projects=1 providers=claude=1, codex=1" in body
    assert "- capacity_policy: elevated | both primary providers are blocked" in body
    assert "- capacity_operator_action: /auto status" in body
    assert "- capacity_memory_updated_at: 2026-03-14T01:21:00+09:00" in body
    assert "- provider_memory: claude(blocked=1 projects=1 level=cooldown wait=short retry=2026-03-14T01:23:00+09:00), codex(blocked=1 projects=1 level=elevated wait=short retry=2026-03-14T01:23:00+09:00)" in body
    assert "- capacity_override_last: /auto status @ 2026-03-14T01:20:00+09:00 (elevated)" in body
    assert "task:rate_limited" in body
    assert "capacity:codex,claude" in body
    assert "first: /task T-001 | active task is waiting for provider capacity until 2026-03-14T01:23:00+09:00" in body
    assert "provider_capacity: providers=codex,claude retry_at=2026-03-14T01:23:00+09:00 degraded=claude_rate_limit->codex" in body
    assert "do: /task T-001, /auto status" in body
    buttons = _button_texts(markup)
    assert "/task T-001" in buttons
    assert "/auto status" in buttons


def test_offdesk_review_promotes_auto_off_when_capacity_policy_is_critical(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    for key, alias in [("p1", "O1"), ("p2", "O2")]:
        project_root = tmp_path / key
        team_dir = project_root / ".aoe-team"
        team_dir.mkdir(parents=True, exist_ok=True)
        (project_root / "TODO.md").write_text("# TODO\n", encoding="utf-8")
        (team_dir / "AOE_TODO.md").write_text(f"@include {project_root / 'TODO.md'}\n", encoding="utf-8")
        state["projects"][key] = {
            "name": key,
            "display_name": key.upper(),
            "project_alias": alias,
            "project_root": str(project_root),
            "team_dir": str(team_dir),
            "runtime_ready": True,
            "todos": [{"id": "TODO-001", "summary": "resume task", "priority": "P1", "status": "open"}],
            "tasks": {
                f"req-{key}": {
                    "request_id": f"req-{key}",
                    "label": f"T-{alias[-1]}01",
                    "short_id": f"T-{alias[-1]}01",
                    "status": "running",
                    "roles": ["Codex-Writer", "Codex-Reviewer"],
                    "rate_limit": {
                        "mode": "blocked",
                        "limited_providers": ["codex", "claude"],
                        "retry_after_sec": 180,
                        "retry_at": "2026-03-14T01:23:00+09:00",
                    },
                    "result": {
                        "degraded_by": ["claude_rate_limit->codex"],
                    },
                    "updated_at": "2026-03-14T01:20:00+0900",
                }
            },
        }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review all",
    )

    assert "- provider_capacity: tasks=2 projects=2 providers=claude=2, codex=2" in body
    assert "- capacity_policy: critical | both primary providers are blocked across multiple tasks/projects" in body
    assert "- capacity_operator_action: /auto off" in body
    buttons = _button_texts(markup)
    assert "/auto off" in buttons
    assert "/auto status" in buttons


def test_next_resumes_parked_rate_limited_todo_after_retry_at(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Local"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {
            "r1": {
                "request_id": "r1",
                "todo_id": "TODO-001",
                "status": "running",
                "tf_phase": "rate_limited",
                "rate_limit": {
                    "mode": "blocked",
                    "limited_providers": ["codex", "claude"],
                    "retry_after_sec": 180,
                    "retry_at": "2000-01-01T00:00:00+00:00",
                },
            }
        },
        "todos": [
            {
                "id": "TODO-001",
                "summary": "resume row",
                "priority": "P1",
                "status": "running",
                "updated_at": "2026-03-14T00:00:00+0900",
            },
            {
                "id": "TODO-002",
                "summary": "open row",
                "priority": "P2",
                "status": "open",
                "created_at": "2026-03-13T23:50:00+0900",
            },
        ],
    }
    sent: list[str] = []
    saves: list[Path] = []

    def _send(body: str, **kwargs: object) -> bool:
        sent.append(body)
        return True

    def _get_context(key: str):
        entry = state["projects"][key]
        return key, entry, argparse.Namespace(project_root=Path(entry["project_root"]), team_dir=Path(entry["team_dir"]))

    result = sched.handle_scheduler_command(
        cmd="next",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="",
        send=_send,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: saves.append(path),
        now_iso=lambda: "2026-03-14T01:00:00+0900",
    )

    assert result["terminal"] is False
    assert result["cmd"] == "run"
    assert result["orch_target"] == "local"
    assert result["run_prompt"] == "resume row"
    assert state["projects"]["local"]["pending_todo"]["todo_id"] == "TODO-001"
    assert sent
    assert "next resumed (global)" in sent[-1]
