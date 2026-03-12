#!/usr/bin/env python3
"""Scheduler-control command handlers extracted from management handlers."""

from typing import Any, Callable, Dict, List


def _handle_focus_command(
    *,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    rest: str,
    send: Callable[..., bool],
    save_manager_state: Callable[..., None],
    resolve_project_entry: Callable[[Dict[str, Any], str], tuple[str, Dict[str, Any]]],
    project_lock_row: Callable[[Dict[str, Any]], Dict[str, Any]],
    project_lock_label: Callable[[Dict[str, Any]], str],
    now_iso: Callable[[], str],
) -> bool:
    tokens = [t for t in str(rest or "").split() if t.strip()]
    sub = (tokens[0].lower() if tokens else "status").strip()
    if sub in {"", "show", "status"}:
        sub = "status"

    row = project_lock_row(manager_state)
    active_key = str(manager_state.get("active", "default") or "default").strip()
    active_label = ""
    try:
        key0, entry0 = resolve_project_entry(manager_state, active_key)
        alias0 = str(entry0.get("project_alias", "")).strip() or key0
        active_label = f"{alias0} ({key0})"
    except Exception:
        active_label = active_key or "-"

    if sub == "status":
        send(
            "project focus lock\n"
            f"- enabled: {'yes' if row else 'no'}\n"
            f"- active_project: {active_label or '-'}\n"
            f"- locked_project: {project_lock_label(manager_state) or '-'}\n"
            "set:\n"
            "- /map\n"
            "- /focus O2\n"
            "- /focus off\n"
            "rules:\n"
            "- /next, /queue, plain text, TF run are pinned to the locked project\n"
            "- /fanout and /auto on fanout stay blocked while lock is enabled",
            context="focus-status",
            with_menu=True,
        )
        return True

    if chat_role == "readonly":
        send(
            "permission denied: readonly chat cannot change project focus.\n"
            "read-only: /focus",
            context="focus-deny",
            with_menu=True,
        )
        return True

    if sub in {"off", "clear", "none", "unlock", "release"}:
        existed = bool(row)
        manager_state.pop("project_lock", None)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            "project focus lock updated\n"
            "- enabled: no\n"
            f"- changed: {'yes' if existed else 'no'}\n"
            f"- active_project: {active_label or '-'}\n"
            "next:\n"
            "- /map\n"
            "- /use O2",
            context="focus-off",
            with_menu=True,
        )
        return True

    target = str(tokens[0] if tokens else "").strip()
    if not target:
        raise RuntimeError("usage: /focus [O#|name|off]")

    key, entry = resolve_project_entry(manager_state, target)
    alias = str(entry.get("project_alias", "")).strip() or key
    manager_state["active"] = key
    manager_state["project_lock"] = {
        "enabled": True,
        "project_key": key,
        "locked_at": now_iso(),
        "locked_by": f"telegram:{chat_id}",
    }
    if not args.dry_run:
        save_manager_state(args.manager_state_file, manager_state)
    send(
        "project focus lock updated\n"
        "- enabled: yes\n"
        f"- locked_project: {alias} ({key})\n"
        "- effect: /next, /queue, plain text, TF run -> this project by default\n"
        "- blocked: /fanout, /auto on fanout\n"
        "next:\n"
        f"- /sync {alias} 1h\n"
        "- /next\n"
        "- /focus off",
        context="focus-on",
        with_menu=True,
    )
    return True


def _handle_panic_command(
    *,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    rest: str,
    send: Callable[..., bool],
    get_default_mode: Callable[[Dict[str, Any], str], str],
    get_pending_mode: Callable[[Dict[str, Any], str], str],
    clear_default_mode: Callable[[Dict[str, Any], str], bool],
    clear_pending_mode: Callable[[Dict[str, Any], str], bool],
    clear_confirm_action: Callable[[Dict[str, Any], str], bool],
    save_manager_state: Callable[..., None],
    auto_state_path: Callable[[Any], Any],
    offdesk_state_path: Callable[[Any], Any],
    load_auto_state: Callable[[Any], Dict[str, Any]],
    save_auto_state: Callable[[Any, Dict[str, Any]], None],
    load_offdesk_state: Callable[[Any], Dict[str, Any]],
    save_offdesk_state: Callable[[Any, Dict[str, Any]], None],
    scheduler_session_name: Callable[[], str],
    tmux_has_session: Callable[[str], bool],
    tmux_auto_command: Callable[[Any, str], tuple[bool, str]],
    now_iso: Callable[[], str],
) -> bool:
    tokens = [t for t in str(rest or "").split() if t.strip()]
    sub = (tokens[0].lower() if tokens else "").strip()
    if sub in {"", "go", "now", "on", "stop"}:
        sub = "stop"
    if sub in {"show"}:
        sub = "status"
    if sub in {"help", "h", "?"}:
        raise RuntimeError("usage: /panic [status]")
    if sub not in {"stop", "status"}:
        raise RuntimeError("usage: /panic [status]")

    auto_path = auto_state_path(args)
    auto_state = load_auto_state(auto_path)
    auto_enabled = bool(auto_state.get("enabled", False))
    auto_chat = str(auto_state.get("chat_id", "")).strip() or "-"

    off_path = offdesk_state_path(args)
    off_state = load_offdesk_state(off_path)
    off_enabled = bool(off_state.get("enabled", False))
    off_chat = str(off_state.get("chat_id", "")).strip() or "-"

    session = scheduler_session_name()
    sess_up = tmux_has_session(session)

    current_default_mode = get_default_mode(manager_state, chat_id) or "off"
    current_pending_mode = get_pending_mode(manager_state, chat_id) or "none"

    if sub == "status":
        lines = [
            "panic switch",
            f"- routing_mode: {current_default_mode}",
            f"- one_shot_pending: {current_pending_mode}",
            f"- auto_enabled: {'yes' if auto_enabled else 'no'} (chat_id={auto_chat})",
            f"- offdesk_enabled: {'yes' if off_enabled else 'no'} (chat_id={off_chat})",
            f"- tmux_scheduler: {session} ({'up' if sess_up else 'down'})",
            "",
            "actions:",
            "- /panic        # stop auto/offdesk + clear pending/confirm + routing off",
            "- /offdesk on   # resume preset",
            "- /auto on fanout recent",
            "- /auto status",
        ]
        send("\n".join(lines).strip(), context="panic-status", with_menu=True)
        return True

    if chat_role == "readonly":
        send(
            "permission denied: readonly chat cannot use /panic.\n"
            "read-only: /panic status",
            context="panic-deny",
            with_menu=True,
        )
        return True

    if args.dry_run:
        tmux_ok, tmux_out = True, "dry-run: skipped tmux auto off"
    else:
        tmux_ok, tmux_out = tmux_auto_command(args, "off")

    auto_state["enabled"] = False
    auto_state["chat_id"] = str(auto_state.get("chat_id", "")).strip() or str(chat_id)
    auto_state["stopped_at"] = now_iso()
    auto_state["stopped_reason"] = "panic"
    if not args.dry_run:
        save_auto_state(auto_path, auto_state)

    if not isinstance(off_state, dict):
        off_state = {}
    off_state["enabled"] = False
    off_state["chat_id"] = str(chat_id)
    off_state["stopped_at"] = now_iso()
    off_state["stopped_reason"] = "panic"
    if not args.dry_run:
        save_offdesk_state(off_path, off_state)

    existed_default = clear_default_mode(manager_state, chat_id)
    cleared_pending = clear_pending_mode(manager_state, chat_id)
    cleared_confirm = clear_confirm_action(manager_state, chat_id)
    if not args.dry_run:
        save_manager_state(args.manager_state_file, manager_state)

    send(
        "panic activated\n"
        "- auto: stopped\n"
        f"- offdesk: {'stopped' if off_enabled else 'already_off'}\n"
        f"- tmux: {'stopped' if tmux_ok else 'stop_failed'}\n"
        f"- detail: {tmux_out or '-'}\n"
        f"- routing_mode: off (changed={'yes' if existed_default else 'no'})\n"
        f"- pending_cleared: {'yes' if cleared_pending else 'no'}\n"
        f"- confirm_cleared: {'yes' if cleared_confirm else 'no'}\n"
        "next:\n"
        "- /offdesk status\n"
        "- /auto status\n"
        "- /offdesk on   (resume)\n"
        "- /mode on      (enable plain-text routing again)",
        context="panic",
        with_menu=True,
    )
    return True


def _handle_offdesk_command(
    *,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    rest: str,
    send: Callable[..., bool],
    get_default_mode: Callable[[Dict[str, Any], str], str],
    get_pending_mode: Callable[[Dict[str, Any], str], str],
    get_chat_report_level: Callable[[Dict[str, Any], str, str], str],
    get_chat_room: Callable[[Dict[str, Any], str, str], str],
    set_default_mode: Callable[[Dict[str, Any], str, str], None],
    set_chat_report_level: Callable[[Dict[str, Any], str, str], None],
    set_chat_room: Callable[[Dict[str, Any], str, str], None],
    clear_default_mode: Callable[[Dict[str, Any], str], bool],
    clear_pending_mode: Callable[[Dict[str, Any], str], bool],
    clear_confirm_action: Callable[[Dict[str, Any], str], bool],
    clear_chat_report_level: Callable[[Dict[str, Any], str], bool],
    save_manager_state: Callable[..., None],
    parse_replace_sync_flag: Callable[[List[str]], bool | None],
    status_report_level: Callable[[List[str], str], str],
    prefetch_display: Callable[[Any, Any, bool], str],
    focused_project_snapshot_lines: Callable[[Dict[str, Any]], List[str]],
    ops_scope_summary: Callable[[Dict[str, Any]], Dict[str, List[str]]],
    ops_scope_compact_lines: Callable[[Dict[str, Any], int, str], List[str]],
    project_lock_row: Callable[[Dict[str, Any]], Dict[str, Any]],
    project_lock_label: Callable[[Dict[str, Any]], str],
    offdesk_prepare_targets: Callable[[Dict[str, Any], str], List[tuple[str, Dict[str, Any]]]],
    offdesk_prepare_project_report: Callable[[Dict[str, Any], str, Dict[str, Any]], Dict[str, Any]],
    offdesk_review_reply_markup: Callable[[List[Dict[str, Any]], bool], Dict[str, Any]],
    offdesk_prepare_reply_markup: Callable[[List[Dict[str, Any]], int, bool], Dict[str, Any]],
    auto_state_path: Callable[[Any], Any],
    offdesk_state_path: Callable[[Any], Any],
    load_auto_state: Callable[[Any], Dict[str, Any]],
    save_auto_state: Callable[[Any, Dict[str, Any]], None],
    load_offdesk_state: Callable[[Any], Dict[str, Any]],
    save_offdesk_state: Callable[[Any, Dict[str, Any]], None],
    tmux_auto_command: Callable[[Any, str], tuple[bool, str]],
    now_iso: Callable[[], str],
    default_offdesk_command: str,
    default_offdesk_prefetch: str,
    default_offdesk_prefetch_since: str,
    default_offdesk_report_level: str,
    default_offdesk_room: str,
    default_auto_interval_sec: int,
    default_auto_idle_sec: int,
) -> bool:
    tokens = [t for t in str(rest or "").split() if t.strip()]
    sub = (tokens[0].lower() if tokens else "status").strip()
    if sub in {"", "show"}:
        sub = "status"
    if sub not in {"status", "on", "off", "start", "stop", "prepare", "preflight", "check", "review"}:
        raise RuntimeError("usage: /offdesk [on|off|status|prepare|review] [replace-sync|O#|name|all]")
    replace_sync = parse_replace_sync_flag(tokens[1:])

    fallback_level = str(getattr(args, "default_report_level", "normal") or "normal").strip().lower()
    current_default_mode = get_default_mode(manager_state, chat_id) or "off"
    current_pending_mode = get_pending_mode(manager_state, chat_id) or "none"
    current_report_level = get_chat_report_level(manager_state, chat_id, fallback_level)
    status_level = status_report_level(tokens, current_report_level)
    current_room = get_chat_room(manager_state, chat_id, default_offdesk_room) or default_offdesk_room

    off_path = offdesk_state_path(args)
    off_state = load_offdesk_state(off_path)
    off_enabled = bool(off_state.get("enabled", False))

    auto_path = auto_state_path(args)
    auto_state = load_auto_state(auto_path)
    auto_enabled = bool(auto_state.get("enabled", False))
    auto_cmd = str(auto_state.get("command", "")).strip().lower() or "next"
    auto_prefetch = str(auto_state.get("prefetch", "")).strip().lower()
    auto_replace_sync = bool(auto_state.get("prefetch_replace_sync", False))
    focus_label = project_lock_label(manager_state) or "-"
    scope_summary = ops_scope_summary(manager_state)
    included_scope = ", ".join(scope_summary.get("included", [])[:6]) or "-"
    excluded_scope = ", ".join(scope_summary.get("excluded", [])[:6]) or "-"

    if sub == "status":
        lines = [
            "offdesk mode",
            f"- enabled: {'yes' if off_enabled else 'no'}",
            f"- project_lock: {focus_label}",
            f"- ops_scope: {included_scope}",
            f"- ops_excluded: {excluded_scope}",
            f"- report_view: {status_level}",
            f"- routing_mode: {current_default_mode}",
            f"- one_shot_pending: {current_pending_mode}",
            f"- report_level: {current_report_level}",
            f"- room: {current_room}",
            f"- auto_enabled: {'yes' if auto_enabled else 'no'}",
            f"- auto_command: {auto_cmd}",
            f"- auto_prefetch: {prefetch_display(auto_prefetch, auto_state.get('prefetch_since', ''), auto_replace_sync)}",
            "",
            "set:",
            "- /offdesk on",
            "- /offdesk on replace-sync",
            "- /offdesk off",
            "- /auto status",
        ]
        snapshot_lines = focused_project_snapshot_lines(manager_state)
        if status_level == "long" and snapshot_lines:
            lines.extend([""] + snapshot_lines)
        compact_lines = ops_scope_compact_lines(manager_state, 4, status_level)
        if compact_lines:
            lines.extend(["", "ops projects:"] + compact_lines)
        send("\n".join(lines).strip(), context="offdesk-status", with_menu=True)
        return True

    if sub in {"prepare", "preflight", "check"}:
        raw_target = ""
        for tok in tokens[1:]:
            low = str(tok or "").strip().lower()
            if low in {
                "replace-sync",
                "sync-replace",
                "replace_prefetch",
                "prefetch-replace",
                "no-replace-sync",
                "safe-sync",
                "no-sync-replace",
            }:
                continue
            raw_target = str(tok or "").strip()
            break
        try:
            targets = offdesk_prepare_targets(manager_state, raw_target)
        except RuntimeError as exc:
            send(str(exc).strip(), context="offdesk-prepare blocked", with_menu=True)
            return True
        if not targets:
            send("offdesk prepare\n- no orch projects registered", context="offdesk-prepare empty", with_menu=True)
            return True

        reports = [offdesk_prepare_project_report(manager_state, key, entry) for key, entry in targets]
        ready_count = sum(1 for row in reports if row.get("status") == "ready")
        warn_count = sum(1 for row in reports if row.get("status") == "warn")
        blocked_count = sum(1 for row in reports if row.get("status") == "blocked")
        scope_label = project_lock_label(manager_state) or ("all" if len(targets) > 1 else reports[0].get("alias", "-"))
        scope_summary = ops_scope_summary(manager_state)
        included_scope = ", ".join(scope_summary.get("included", [])[:6]) or "-"
        excluded_scope = ", ".join(scope_summary.get("excluded", [])[:6]) or "-"
        lines = [
            "offdesk prepare",
            f"- scope: {scope_label}",
            f"- ops_scope: {included_scope}",
            f"- ops_excluded: {excluded_scope}",
            f"- projects: {len(targets)}",
            f"- ready: {ready_count}",
            f"- warn: {warn_count}",
            f"- blocked: {blocked_count}",
            "",
            "projects:",
        ]
        for report in reports:
            lines.extend(report.get("lines") or [])

        lines.extend(["", "next:"])
        if blocked_count == 0:
            lines.append("- /offdesk on")
        else:
            lines.append("- fix blocked items before /offdesk on")
        if len(targets) == 1:
            alias = str(reports[0].get("alias", "")).strip() or "-"
            lines.append(f"- /sync preview {alias} 24h")
            lines.append(f"- /todo {alias}")
            lines.append(f"- /todo {alias} syncback preview")
        else:
            lines.append("- /map")
            lines.append("- /queue")
            lines.append("- /todo proposals")
        send(
            "\n".join(lines).strip(),
            context="offdesk-prepare",
            with_menu=True,
            reply_markup=offdesk_prepare_reply_markup(
                reports,
                blocked_count,
                warn_count == 0 and blocked_count == 0,
            ),
        )
        return True

    if sub == "review":
        raw_target = ""
        for tok in tokens[1:]:
            low = str(tok or "").strip().lower()
            if low in {
                "replace-sync",
                "sync-replace",
                "replace_prefetch",
                "prefetch-replace",
                "no-replace-sync",
                "safe-sync",
                "no-sync-replace",
            }:
                continue
            raw_target = str(tok or "").strip()
            break
        try:
            targets = offdesk_prepare_targets(manager_state, raw_target)
        except RuntimeError as exc:
            send(str(exc).strip(), context="offdesk-review blocked", with_menu=True)
            return True
        if not targets:
            send("offdesk review\n- no orch projects registered", context="offdesk-review empty", with_menu=True)
            return True

        reports = [offdesk_prepare_project_report(manager_state, key, entry) for key, entry in targets]
        flagged = [row for row in reports if str(row.get("status", "")).strip().lower() in {"warn", "blocked"}]
        lines = [
            "offdesk review",
            f"- reviewed: {len(reports)}",
            f"- flagged: {len(flagged)}",
        ]
        if not flagged:
            lines.extend(["- status: clean", "", "next:", "- /offdesk on", "- /auto status"])
            send(
                "\n".join(lines).strip(),
                context="offdesk-review clean",
                with_menu=True,
                reply_markup=offdesk_review_reply_markup([], True),
            )
            return True

        lines.extend(["", "actions:"])
        for row in flagged:
            alias = str(row.get("alias", "")).strip() or "-"
            display = str(row.get("display", "")).strip() or alias
            actions: List[str] = []
            if bool(row.get("syncback_pending", False)):
                actions.append(f"/todo {alias} syncback preview")
            if int(row.get("proposals", 0) or 0) > 0:
                actions.append(f"/todo {alias} proposals")
            if int(row.get("followup_count", 0) or 0) > 0:
                actions.append(f"/todo {alias} followup")
            active_task_label = str(row.get("active_task_label", "")).strip()
            active_task_tf_phase = str(row.get("active_task_tf_phase", "")).strip()
            if active_task_label and active_task_tf_phase in {"needs_retry", "manual_intervention", "critic_review", "blocked"}:
                actions.append(f"/task {active_task_label}")
                actions.append(f"/retry {active_task_label}")
            if bool(row.get("bootstrap_recommended", False)):
                actions.append(f"/sync bootstrap {alias} 24h")
            if (
                int(row.get("blocked_count", 0) or 0) > 0
                or int(row.get("open", 0) or 0) == 0
                or bool(row.get("sync_quality_warn", False))
            ):
                actions.append(f"/sync preview {alias} 24h")
            if bool(row.get("pending_flag", False)) or int(row.get("running", 0) or 0) > 0:
                actions.append(f"/orch status {alias}")
            if not actions:
                actions.append(f"/todo {alias}")
            lines.append(f"- {alias} {display} [{row.get('status', '-')}]")
            note_rows = list(row.get("notes") or [])
            for note in note_rows[:2]:
                lines.append(f"  note: {note}")
            lines.append(f"  do: {', '.join(actions)}")

        lines.extend(["", "next:", "- resolve flagged items, then /offdesk on", "- /offdesk prepare"])
        send(
            "\n".join(lines).strip(),
            context="offdesk-review",
            with_menu=True,
            reply_markup=offdesk_review_reply_markup(flagged, False),
        )
        return True

    if chat_role == "readonly":
        send(
            "permission denied: readonly chat cannot change offdesk mode.\n"
            "read-only: /offdesk (status/prepare only)",
            context="offdesk-deny",
            with_menu=True,
        )
        return True

    if sub in {"off", "stop"}:
        prev = off_state.get("prev") if isinstance(off_state.get("prev"), dict) else {}

        prev_mode_present = bool(prev.get("default_mode_present", False))
        prev_mode = str(prev.get("default_mode", "")).strip().lower()
        if prev_mode_present and prev_mode in {"dispatch", "direct"}:
            set_default_mode(manager_state, chat_id, prev_mode)
        else:
            clear_default_mode(manager_state, chat_id)

        prev_report_present = bool(prev.get("report_level_present", False))
        prev_report = str(prev.get("report_level", "")).strip().lower()
        if prev_report_present and prev_report in {"short", "normal", "long"}:
            set_chat_report_level(manager_state, chat_id, prev_report)
        else:
            clear_chat_report_level(manager_state, chat_id)

        prev_room_present = bool(prev.get("room_present", False))
        prev_room = str(prev.get("room", "")).strip()
        if prev_room_present and prev_room:
            set_chat_room(manager_state, chat_id, prev_room)
        else:
            set_chat_room(manager_state, chat_id, default_offdesk_room)

        cleared_pending = clear_pending_mode(manager_state, chat_id)
        cleared_confirm = clear_confirm_action(manager_state, chat_id)

        auto_state = load_auto_state(auto_path)
        auto_state["enabled"] = False
        auto_state["chat_id"] = str(auto_state.get("chat_id", "")).strip() or str(chat_id)
        auto_state["stopped_at"] = now_iso()
        if not args.dry_run:
            save_auto_state(auto_path, auto_state)

        if args.dry_run:
            ok, out = True, "dry-run: skipped tmux auto off"
        else:
            ok, out = tmux_auto_command(args, "off")

        off_state["enabled"] = False
        off_state["chat_id"] = str(chat_id)
        off_state["stopped_at"] = now_iso()
        if not args.dry_run:
            save_offdesk_state(off_path, off_state)
            save_manager_state(args.manager_state_file, manager_state)

        send(
            "offdesk disabled\n"
            f"- restored_routing_mode: {(get_default_mode(manager_state, chat_id) or 'off')}\n"
            f"- restored_report_level: {get_chat_report_level(manager_state, chat_id, fallback_level)}\n"
            f"- restored_room: {get_chat_room(manager_state, chat_id, default_offdesk_room) or default_offdesk_room}\n"
            f"- pending_cleared: {'yes' if cleared_pending else 'no'}\n"
            f"- confirm_cleared: {'yes' if cleared_confirm else 'no'}\n"
            f"- auto: {'stopped' if ok else 'stop_failed'}\n"
            f"- detail: {out or '-'}\n"
            "next:\n"
            "- /offdesk status\n"
            "- /auto status",
            context="offdesk-off",
            with_menu=True,
        )
        return True

    existing_prev = off_state.get("prev") if isinstance(off_state, dict) else None
    if off_enabled and isinstance(existing_prev, dict):
        prev = dict(existing_prev)
    else:
        sessions = manager_state.get("chat_sessions") if isinstance(manager_state, dict) else {}
        row = sessions.get(str(chat_id)) if isinstance(sessions, dict) else None
        row = row if isinstance(row, dict) else {}
        prev = {
            "default_mode_present": ("default_mode" in row),
            "default_mode": str(row.get("default_mode", "")).strip().lower(),
            "report_level_present": ("report_level" in row),
            "report_level": str(row.get("report_level", "")).strip().lower(),
            "room_present": ("room" in row),
            "room": str(row.get("room", "")).strip(),
        }

    off_state = {
        "enabled": True,
        "chat_id": str(chat_id),
        "started_at": str(off_state.get("started_at", "")).strip() or now_iso(),
        "prev": prev,
    }
    if not args.dry_run:
        save_offdesk_state(off_path, off_state)

    set_chat_report_level(manager_state, chat_id, default_offdesk_report_level)
    set_chat_room(manager_state, chat_id, default_offdesk_room)
    existed_default = clear_default_mode(manager_state, chat_id)
    cleared_pending = clear_pending_mode(manager_state, chat_id)
    cleared_confirm = clear_confirm_action(manager_state, chat_id)

    focus_row = project_lock_row(manager_state)
    offdesk_command = "next" if focus_row else default_offdesk_command
    auto_state = load_auto_state(auto_path)
    auto_state["enabled"] = True
    auto_state["chat_id"] = str(chat_id)
    if "started_at" not in auto_state:
        auto_state["started_at"] = now_iso()
    auto_state["command"] = offdesk_command
    auto_state["prefetch"] = default_offdesk_prefetch
    auto_state["prefetch_replace_sync"] = bool(replace_sync)
    if "prefetch_since" not in auto_state:
        auto_state["prefetch_since"] = default_offdesk_prefetch_since
    auto_state["force"] = False
    if "interval_sec" not in auto_state:
        auto_state["interval_sec"] = default_auto_interval_sec
    if "idle_sec" not in auto_state:
        auto_state["idle_sec"] = default_auto_idle_sec
    if not args.dry_run:
        save_auto_state(auto_path, auto_state)
        save_manager_state(args.manager_state_file, manager_state)

    if args.dry_run:
        ok, out = True, "dry-run: skipped tmux auto on"
    else:
        ok, out = tmux_auto_command(args, "on")

    scope_summary = ops_scope_summary(manager_state)
    included_scope = ", ".join(scope_summary.get("included", [])[:6]) or "-"
    excluded_scope = ", ".join(scope_summary.get("excluded", [])[:6]) or "-"
    body = (
        "offdesk enabled\n"
        f"- ops_scope: {included_scope}\n"
        f"- ops_excluded: {excluded_scope}\n"
        "- routing_mode: off\n"
        f"- report_level: {default_offdesk_report_level}\n"
        f"- room: {default_offdesk_room}\n"
        f"- auto: {'started' if ok else 'start_failed'}\n"
        f"- command: {offdesk_command}\n"
        f"- prefetch: {prefetch_display(default_offdesk_prefetch, default_offdesk_prefetch_since, bool(replace_sync))}\n"
        f"- changed_default_mode: {'yes' if existed_default else 'no'}\n"
        f"- pending_cleared: {'yes' if cleared_pending else 'no'}\n"
        f"- confirm_cleared: {'yes' if cleared_confirm else 'no'}\n"
        f"- detail: {out or '-'}\n"
    )
    if focus_row:
        body += f"- project_lock: {project_lock_label(manager_state)}\n"
        body += "- note: project lock active, offdesk was narrowed to single-project /next mode\n"
    snapshot_lines = focused_project_snapshot_lines(manager_state)
    if snapshot_lines:
        body += "\n" + "\n".join(snapshot_lines) + "\n"
    compact_lines = ops_scope_compact_lines(manager_state, 4, "short")
    if compact_lines:
        body += "\nops projects:\n" + "\n".join(compact_lines) + "\n"
    body += "next:\n- /offdesk status\n- /queue\n- /room tail 30\n- /auto status"
    send(body, context="offdesk-on", with_menu=True)
    return True


def _handle_auto_command(
    *,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    rest: str,
    send: Callable[..., bool],
    get_chat_report_level: Callable[[Dict[str, Any], str, str], str],
    status_report_level: Callable[[List[str], str], str],
    parse_replace_sync_flag: Callable[[List[str]], bool | None],
    normalize_prefetch_token: Callable[[Any], str],
    prefetch_display: Callable[[Any, Any, bool], str],
    compact_reason: Callable[[Any, int], str],
    focused_project_snapshot_lines: Callable[[Dict[str, Any]], List[str]],
    ops_scope_compact_lines: Callable[[Dict[str, Any], int, str], List[str]],
    project_lock_row: Callable[[Dict[str, Any]], Dict[str, Any]],
    project_lock_label: Callable[[Dict[str, Any]], str],
    auto_state_path: Callable[[Any], Any],
    load_auto_state: Callable[[Any], Dict[str, Any]],
    save_auto_state: Callable[[Any, Dict[str, Any]], None],
    scheduler_session_name: Callable[[], str],
    tmux_has_session: Callable[[str], bool],
    tmux_auto_command: Callable[[Any, str], tuple[bool, str]],
    now_iso: Callable[[], str],
    default_auto_interval_sec: int,
    default_auto_idle_sec: int,
    default_auto_max_failures: int,
) -> bool:
    tokens = [t for t in str(rest or "").split() if t.strip()]
    sub = (tokens[0].lower() if tokens else "status").strip()
    if sub in {"", "show"}:
        sub = "status"
    if sub not in {"status", "on", "off", "start", "stop"}:
        raise RuntimeError("usage: /auto [on|off|status]")

    command = None
    for tok in tokens[1:]:
        low = tok.strip().lower()
        if low in {"fanout", "wave", "oneeach", "round"}:
            command = "fanout"
        elif low in {"next", "global"}:
            command = "next"

    prefetch = None
    for tok in tokens[1:]:
        low = tok.strip().lower()
        if low in {"recent", "docs", "prefetch", "sync-recent", "recent-docs"}:
            prefetch = "sync_recent"
        elif low in {"no-recent", "no-docs", "noprefetch", "no-prefetch"}:
            prefetch = ""
    replace_sync = parse_replace_sync_flag(tokens[1:])

    prefetch_since = None
    i = 1
    while i < len(tokens):
        tok = str(tokens[i] or "").strip()
        low = tok.lower()
        if low in {"since", "--since", "-s", "within", "--within"}:
            if i + 1 < len(tokens):
                prefetch_since = str(tokens[i + 1] or "").strip()
                i += 2
            else:
                i += 1
            continue
        if low.startswith("since=") or low.startswith("--since=") or low.startswith("-s=") or low.startswith("within="):
            prefetch_since = tok.split("=", 1)[1].strip() if "=" in tok else ""
            i += 1
            continue
        i += 1

    force = any(t.lower() in {"force", "!", "--force"} for t in tokens[1:])
    interval_sec = None
    idle_sec = None
    max_failures = None
    nums = [t for t in tokens[1:] if t.replace(".", "", 1).isdigit()]
    if nums:
        try:
            interval_sec = max(1, min(300, int(float(nums[0]))))
        except Exception:
            interval_sec = None
    if len(nums) >= 2:
        try:
            idle_sec = max(1, min(3600, int(float(nums[1]))))
        except Exception:
            idle_sec = None

    for tok in tokens[1:]:
        low = tok.strip().lower()
        if not any(
            low.startswith(p)
            for p in {
                "maxfail=",
                "maxfails=",
                "maxfailures=",
                "max_fail=",
                "max_fails=",
                "max_failures=",
            }
        ):
            continue
        raw = tok.split("=", 1)[1].strip() if "=" in tok else ""
        try:
            max_failures = max(1, min(50, int(float(raw))))
        except Exception:
            max_failures = None

    path = auto_state_path(args)
    current = load_auto_state(path)
    enabled = bool(current.get("enabled", False))
    session = scheduler_session_name()
    sess_up = tmux_has_session(session)
    focus_row = project_lock_row(manager_state)
    focus_label = project_lock_label(manager_state) or "-"
    fallback_level = str(getattr(args, "default_report_level", "normal") or "normal").strip().lower()
    current_report_level = get_chat_report_level(manager_state, chat_id, fallback_level)
    status_level = status_report_level(tokens, current_report_level)

    if sub == "status":
        chat_ref = str(current.get("chat_id", "")).strip() or "-"
        eff_force = bool(current.get("force", False))
        eff_command = str(current.get("command", "next")).strip().lower() or "next"
        if eff_command not in {"next", "fanout"}:
            eff_command = "next"
        prefetch_token = normalize_prefetch_token(current.get("prefetch", ""))
        replace_sync_enabled = bool(current.get("prefetch_replace_sync", False))
        eff_interval = int(current.get("interval_sec") or default_auto_interval_sec)
        eff_idle = int(current.get("idle_sec") or default_auto_idle_sec)
        eff_max_fail = int(current.get("max_failures") or default_auto_max_failures)
        last_reason = str(current.get("last_reason", "")).strip()
        last_run = str(current.get("last_run_at", "")).strip()
        last_candidate = str(current.get("last_candidate", "")).strip()
        last_prefetch_at = str(current.get("last_prefetch_at", "")).strip()
        last_prefetch_reason = str(current.get("last_prefetch_reason", "")).strip()
        last_prefetch_mode = str(current.get("last_prefetch_mode", "")).strip()
        stuck_candidate = str(current.get("stuck_candidate", "")).strip()
        stuck_count = int(current.get("stuck_count") or 0)
        fail_count = int(current.get("fail_count") or 0)
        fail_candidate = str(current.get("fail_candidate", "")).strip()
        fail_reason = str(current.get("fail_reason", "")).strip()
        lines = [
            "auto scheduler",
            f"- enabled: {'yes' if enabled else 'no'}",
            f"- project_lock: {focus_label}",
            f"- report_view: {status_level}",
            f"- chat_id: {chat_ref}",
            f"- command: {eff_command}",
            f"- prefetch: {prefetch_display(prefetch_token, current.get('prefetch_since', ''), replace_sync_enabled)}",
            f"- force: {'yes' if eff_force else 'no'}",
            f"- interval_sec: {eff_interval}",
            f"- idle_sec: {eff_idle}",
            f"- max_failures: {eff_max_fail}",
            f"- tmux_session: {session} ({'up' if sess_up else 'down'})",
        ]
        if last_run:
            lines.append(f"- last_run_at: {last_run}")
        if last_candidate:
            lines.append(f"- last_candidate: {last_candidate}")
        if last_reason:
            lines.append(f"- last_reason: {compact_reason(last_reason, 120)}")
        if stuck_count and stuck_candidate:
            lines.append(f"- stuck: {stuck_count} ({stuck_candidate})")
        if fail_count:
            suffix = f" ({fail_candidate})" if fail_candidate else ""
            lines.append(f"- fail_count: {fail_count}{suffix}")
        if fail_reason:
            lines.append(f"- fail_reason: {compact_reason(fail_reason, 120)}")
            if status_level == "long" and compact_reason(fail_reason, 120) != fail_reason:
                lines.append(f"- fail_reason_full: {fail_reason}")
        if last_prefetch_at:
            lines.append(f"- last_prefetch_at: {last_prefetch_at}")
        if last_prefetch_mode:
            lines.append(f"- last_prefetch_mode: {last_prefetch_mode}")
        if last_prefetch_reason:
            lines.append(f"- last_prefetch_reason: {compact_reason(last_prefetch_reason, 120)}")
        snapshot_lines = focused_project_snapshot_lines(manager_state)
        if status_level == "long" and snapshot_lines:
            lines.extend([""] + snapshot_lines)
        compact_lines = ops_scope_compact_lines(manager_state, 4, status_level)
        if compact_lines:
            lines.extend(["", "ops projects:"] + compact_lines)
        lines.extend(
            [
                "",
                "set:",
                "- /auto on",
                "- /auto on fanout",
                "- /auto on fanout recent",
                "- /auto on fanout recent replace-sync",
                "- /auto on fanout recent since 3h",
                "- /auto off",
                "- /auto on force",
                "- /auto on maxfail=3",
                "- /auto on <interval_sec> <idle_sec>",
            ]
        )
        send("\n".join(lines).strip(), context="auto-status", with_menu=True)
        return True

    if chat_role == "readonly":
        send(
            "permission denied: readonly chat cannot change auto scheduler.\n"
            "read-only: /auto (status only)",
            context="auto-deny",
            with_menu=True,
        )
        return True

    if sub in {"off", "stop"}:
        current["enabled"] = False
        current["chat_id"] = str(current.get("chat_id", "")).strip() or str(chat_id)
        current["stopped_at"] = now_iso()
        if not args.dry_run:
            save_auto_state(path, current)
        if args.dry_run:
            ok, out = True, "dry-run: skipped tmux auto off"
        else:
            ok, out = tmux_auto_command(args, "off")
        send(
            "auto scheduler updated\n"
            "- enabled: no\n"
            f"- tmux: {'stopped' if ok else 'stop_failed'}\n"
            f"- detail: {out or '-'}",
            context="auto-off",
            with_menu=True,
        )
        return True

    effective_command = command if command in {"next", "fanout"} else str(current.get("command", "next")).strip().lower() or "next"
    if effective_command not in {"next", "fanout"}:
        effective_command = "next"
    if focus_row and effective_command == "fanout":
        send(
            "auto scheduler blocked\n"
            f"- project_lock: {focus_label}\n"
            "- reason: fanout is a global multi-project wave\n"
            "next:\n"
            "- /auto on next\n"
            "- /offdesk on\n"
            "- /focus off",
            context="auto-on-blocked",
            with_menu=True,
        )
        return True

    current["enabled"] = True
    current["chat_id"] = str(chat_id)
    if "started_at" not in current:
        current["started_at"] = now_iso()
    current["command"] = effective_command
    if prefetch is not None:
        current["prefetch"] = prefetch
    elif "prefetch" not in current:
        current["prefetch"] = ""
    if replace_sync is not None:
        current["prefetch_replace_sync"] = bool(replace_sync)
    elif "prefetch_replace_sync" not in current:
        current["prefetch_replace_sync"] = False
    if prefetch_since is not None:
        current["prefetch_since"] = str(prefetch_since or "").strip()
    elif "prefetch_since" not in current:
        current["prefetch_since"] = ""
    if bool(current.get("prefetch_replace_sync", False)) and not normalize_prefetch_token(current.get("prefetch", "")):
        current["prefetch"] = "sync_recent"
    if not normalize_prefetch_token(current.get("prefetch", "")):
        current["prefetch_replace_sync"] = False
    if force:
        current["force"] = True
    elif "force" not in current:
        current["force"] = False
    if interval_sec is not None:
        current["interval_sec"] = interval_sec
    elif "interval_sec" not in current:
        current["interval_sec"] = default_auto_interval_sec
    if idle_sec is not None:
        current["idle_sec"] = idle_sec
    elif "idle_sec" not in current:
        current["idle_sec"] = default_auto_idle_sec
    if max_failures is not None:
        current["max_failures"] = int(max_failures)
    elif "max_failures" not in current:
        current["max_failures"] = default_auto_max_failures
    if not args.dry_run:
        save_auto_state(path, current)

    if args.dry_run:
        ok, out = True, "dry-run: skipped tmux auto on"
    else:
        ok, out = tmux_auto_command(args, "on")
    prefetch_token = normalize_prefetch_token(current.get("prefetch", ""))
    replace_sync_enabled = bool(current.get("prefetch_replace_sync", False))
    body = (
        "auto scheduler updated\n"
        "- enabled: yes\n"
        f"- command: {str(current.get('command', 'next')).strip() or 'next'}\n"
        f"- prefetch: {prefetch_display(prefetch_token, current.get('prefetch_since', ''), replace_sync_enabled)}\n"
        f"- force: {'yes' if bool(current.get('force', False)) else 'no'}\n"
        f"- interval_sec: {int(current.get('interval_sec') or default_auto_interval_sec)}\n"
        f"- idle_sec: {int(current.get('idle_sec') or default_auto_idle_sec)}\n"
        f"- tmux: {'started' if ok else 'start_failed'}\n"
        f"- detail: {out or '-'}\n"
    )
    if focus_row:
        body += f"- project_lock: {focus_label}\n"
    snapshot_lines = focused_project_snapshot_lines(manager_state)
    if snapshot_lines:
        body += "\n" + "\n".join(snapshot_lines) + "\n"
    body += "next:\n- /queue\n- /auto status"
    send(body, context="auto-on", with_menu=True)
    return True


def handle_scheduler_control_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    rest: str,
    send: Callable[..., bool],
    get_default_mode: Callable[[Dict[str, Any], str], str],
    get_pending_mode: Callable[[Dict[str, Any], str], str],
    get_chat_report_level: Callable[[Dict[str, Any], str, str], str],
    get_chat_room: Callable[[Dict[str, Any], str, str], str],
    set_default_mode: Callable[[Dict[str, Any], str, str], None],
    set_chat_report_level: Callable[[Dict[str, Any], str, str], None],
    set_chat_room: Callable[[Dict[str, Any], str, str], None],
    clear_default_mode: Callable[[Dict[str, Any], str], bool],
    clear_pending_mode: Callable[[Dict[str, Any], str], bool],
    clear_confirm_action: Callable[[Dict[str, Any], str], bool],
    clear_chat_report_level: Callable[[Dict[str, Any], str], bool],
    save_manager_state: Callable[..., None],
    resolve_project_entry: Callable[[Dict[str, Any], str], tuple[str, Dict[str, Any]]],
    project_lock_row: Callable[[Dict[str, Any]], Dict[str, Any]],
    project_lock_label: Callable[[Dict[str, Any]], str],
    parse_replace_sync_flag: Callable[[List[str]], bool | None],
    normalize_prefetch_token: Callable[[Any], str],
    prefetch_display: Callable[[Any, Any, bool], str],
    compact_reason: Callable[[Any, int], str],
    status_report_level: Callable[[List[str], str], str],
    focused_project_snapshot_lines: Callable[[Dict[str, Any]], List[str]],
    ops_scope_summary: Callable[[Dict[str, Any]], Dict[str, List[str]]],
    ops_scope_compact_lines: Callable[[Dict[str, Any], int, str], List[str]],
    offdesk_prepare_targets: Callable[[Dict[str, Any], str], List[tuple[str, Dict[str, Any]]]],
    offdesk_prepare_project_report: Callable[[Dict[str, Any], str, Dict[str, Any]], Dict[str, Any]],
    offdesk_review_reply_markup: Callable[[List[Dict[str, Any]], bool], Dict[str, Any]],
    offdesk_prepare_reply_markup: Callable[[List[Dict[str, Any]], int, bool], Dict[str, Any]],
    auto_state_path: Callable[[Any], Any],
    offdesk_state_path: Callable[[Any], Any],
    load_auto_state: Callable[[Any], Dict[str, Any]],
    save_auto_state: Callable[[Any, Dict[str, Any]], None],
    load_offdesk_state: Callable[[Any], Dict[str, Any]],
    save_offdesk_state: Callable[[Any, Dict[str, Any]], None],
    scheduler_session_name: Callable[[], str],
    tmux_has_session: Callable[[str], bool],
    tmux_auto_command: Callable[[Any, str], tuple[bool, str]],
    now_iso: Callable[[], str],
    default_auto_interval_sec: int,
    default_auto_idle_sec: int,
    default_auto_max_failures: int,
    default_offdesk_command: str,
    default_offdesk_prefetch: str,
    default_offdesk_prefetch_since: str,
    default_offdesk_report_level: str,
    default_offdesk_room: str,
) -> bool:
    if cmd == "focus":
        return _handle_focus_command(
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            chat_role=chat_role,
            rest=rest,
            send=send,
            save_manager_state=save_manager_state,
            resolve_project_entry=resolve_project_entry,
            project_lock_row=project_lock_row,
            project_lock_label=project_lock_label,
            now_iso=now_iso,
        )
    if cmd == "panic":
        return _handle_panic_command(
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            chat_role=chat_role,
            rest=rest,
            send=send,
            get_default_mode=get_default_mode,
            get_pending_mode=get_pending_mode,
            clear_default_mode=clear_default_mode,
            clear_pending_mode=clear_pending_mode,
            clear_confirm_action=clear_confirm_action,
            save_manager_state=save_manager_state,
            auto_state_path=auto_state_path,
            offdesk_state_path=offdesk_state_path,
            load_auto_state=load_auto_state,
            save_auto_state=save_auto_state,
            load_offdesk_state=load_offdesk_state,
            save_offdesk_state=save_offdesk_state,
            scheduler_session_name=scheduler_session_name,
            tmux_has_session=tmux_has_session,
            tmux_auto_command=tmux_auto_command,
            now_iso=now_iso,
        )
    if cmd == "offdesk":
        return _handle_offdesk_command(
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            chat_role=chat_role,
            rest=rest,
            send=send,
            get_default_mode=get_default_mode,
            get_pending_mode=get_pending_mode,
            get_chat_report_level=get_chat_report_level,
            get_chat_room=get_chat_room,
            set_default_mode=set_default_mode,
            set_chat_report_level=set_chat_report_level,
            set_chat_room=set_chat_room,
            clear_default_mode=clear_default_mode,
            clear_pending_mode=clear_pending_mode,
            clear_confirm_action=clear_confirm_action,
            clear_chat_report_level=clear_chat_report_level,
            save_manager_state=save_manager_state,
            parse_replace_sync_flag=parse_replace_sync_flag,
            status_report_level=status_report_level,
            prefetch_display=prefetch_display,
            focused_project_snapshot_lines=focused_project_snapshot_lines,
            ops_scope_summary=ops_scope_summary,
            ops_scope_compact_lines=ops_scope_compact_lines,
            project_lock_row=project_lock_row,
            project_lock_label=project_lock_label,
            offdesk_prepare_targets=offdesk_prepare_targets,
            offdesk_prepare_project_report=offdesk_prepare_project_report,
            offdesk_review_reply_markup=offdesk_review_reply_markup,
            offdesk_prepare_reply_markup=offdesk_prepare_reply_markup,
            auto_state_path=auto_state_path,
            offdesk_state_path=offdesk_state_path,
            load_auto_state=load_auto_state,
            save_auto_state=save_auto_state,
            load_offdesk_state=load_offdesk_state,
            save_offdesk_state=save_offdesk_state,
            tmux_auto_command=tmux_auto_command,
            now_iso=now_iso,
            default_offdesk_command=default_offdesk_command,
            default_offdesk_prefetch=default_offdesk_prefetch,
            default_offdesk_prefetch_since=default_offdesk_prefetch_since,
            default_offdesk_report_level=default_offdesk_report_level,
            default_offdesk_room=default_offdesk_room,
            default_auto_interval_sec=default_auto_interval_sec,
            default_auto_idle_sec=default_auto_idle_sec,
        )
    if cmd == "auto":
        return _handle_auto_command(
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            chat_role=chat_role,
            rest=rest,
            send=send,
            get_chat_report_level=get_chat_report_level,
            status_report_level=status_report_level,
            parse_replace_sync_flag=parse_replace_sync_flag,
            normalize_prefetch_token=normalize_prefetch_token,
            prefetch_display=prefetch_display,
            compact_reason=compact_reason,
            focused_project_snapshot_lines=focused_project_snapshot_lines,
            ops_scope_compact_lines=ops_scope_compact_lines,
            project_lock_row=project_lock_row,
            project_lock_label=project_lock_label,
            auto_state_path=auto_state_path,
            load_auto_state=load_auto_state,
            save_auto_state=save_auto_state,
            scheduler_session_name=scheduler_session_name,
            tmux_has_session=tmux_has_session,
            tmux_auto_command=tmux_auto_command,
            now_iso=now_iso,
            default_auto_interval_sec=default_auto_interval_sec,
            default_auto_idle_sec=default_auto_idle_sec,
            default_auto_max_failures=default_auto_max_failures,
        )
    return False
