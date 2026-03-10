#!/usr/bin/env python3
"""Orchestrator overview handlers for Telegram gateway."""

from typing import Any, Callable, Dict, List, Optional, Tuple

from aoe_tg_ops_policy import list_ops_projects
from aoe_tg_project_runtime import project_hidden_from_ops, project_runtime_issue


def _extract_alias_index(alias: str) -> int:
    token = str(alias or "").strip().upper()
    if token.startswith("O"):
        token = token[1:]
    return int(token) if token.isdigit() else 10**9


def _orch_map_reply_markup(manager_state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    if not isinstance(projects, dict) or not projects:
        return None

    rows: List[Tuple[str, str]] = []
    for key, entry in list_ops_projects(projects):
        alias = str(entry.get("project_alias", "")).strip().upper() or str(key).strip()
        rows.append((alias, str(key)))

    rows.sort(key=lambda item: (_extract_alias_index(item[0]), item[0], item[1]))
    keyboard: List[List[Dict[str, str]]] = []
    raw_lock = manager_state.get("project_lock") if isinstance(manager_state, dict) else {}
    lock_key = ""
    if isinstance(raw_lock, dict) and bool(raw_lock.get("enabled", False)):
        lock_key = str(raw_lock.get("project_key", "")).strip().lower()

    visible_rows = rows
    if lock_key:
        visible_rows = [item for item in rows if item[1] == lock_key]

    for alias, key in visible_rows[:12]:
        entry = projects.get(key) if isinstance(projects, dict) else {}
        unready = isinstance(entry, dict) and bool(project_runtime_issue(entry))
        if lock_key:
            keyboard.append([{"text": f"/use {alias}"}, {"text": "/focus off"}])
        else:
            keyboard.append([{"text": f"/use {alias}"}, {"text": f"/focus {alias}"}])
        keyboard.append([{"text": f"/orch status {alias}"}, {"text": f"/todo {alias}"}, {"text": f"/todo {alias} followup"}])
        if unready:
            keyboard.append([{"text": f"/orch repair {alias}"}])

    active = str(manager_state.get("active", "")).strip().lower()
    active_alias = ""
    active_entry = projects.get(active) if isinstance(projects, dict) else {}
    if isinstance(active_entry, dict) and not project_hidden_from_ops(active_entry):
        active_alias = str(active_entry.get("project_alias", "")).strip().upper()
    if active_alias:
        keyboard.append([{"text": f"/sync preview {active_alias} 1h"}, {"text": f"/sync {active_alias} 1h"}])

    if lock_key:
        keyboard.append([{"text": "/focus off"}])

    keyboard.append([{"text": "/queue"}, {"text": "/next"}, {"text": "/help"}])
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "예: /use O# 또는 /focus O#",
    }

def handle_orch_overview_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    orch_target: Optional[str],
    orch_monitor_limit: Optional[int],
    orch_kpi_hours: Optional[int],
    rest: str,
    send: Callable[..., bool],
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]],
    save_manager_state: Callable[..., None],
    now_iso: Callable[[], str],
    summarize_orch_registry: Callable[[Dict[str, Any]], str],
    backfill_task_aliases: Callable[[Dict[str, Any]], None],
    latest_task_request_refs: Callable[..., list[str]],
    set_chat_recent_task_refs: Callable[..., None],
    get_chat_selected_task_ref: Callable[..., str],
    set_chat_selected_task_ref: Callable[..., None],
    summarize_task_monitor: Callable[..., str],
    summarize_gateway_metrics: Callable[..., str],
    get_manager_project: Callable[[Dict[str, Any], Optional[str]], tuple[str, Dict[str, Any]]],
) -> bool:
    if cmd == "orch-list":
        lock = manager_state.get("project_lock") if isinstance(manager_state, dict) else {}
        lock_active = isinstance(lock, dict) and bool(lock.get("enabled", False)) and str(lock.get("project_key", "")).strip()
        if lock_active:
            quick = "\n\nquick:\n- tap /orch status O# or /todo O# for the locked project\n- tap /focus off to return to global switching"
        else:
            quick = "\n\nquick:\n- tap /use O# to switch active project\n- tap /focus O# to hard lock one project\n- tap /orch status O# or /todo O# to drill down"
        body = summarize_orch_registry(manager_state) + quick
        send(
            body,
            context="orch-list",
            with_menu=False,
            reply_markup=_orch_map_reply_markup(manager_state),
        )
        return True

    if cmd == "orch-monitor":
        key, entry, _p_args = get_context(orch_target)
        backfill_task_aliases(entry)
        limit = max(1, min(50, int(orch_monitor_limit or 12)))
        recent_refs = latest_task_request_refs(entry, limit=limit)
        set_chat_recent_task_refs(manager_state, chat_id, key, recent_refs)
        current_sel = get_chat_selected_task_ref(manager_state, chat_id, key)
        if (not current_sel) and recent_refs:
            set_chat_selected_task_ref(manager_state, chat_id, key, recent_refs[0])
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(summarize_task_monitor(key, entry, limit=limit), context="orch-monitor", with_menu=True)
        return True

    if cmd == "orch-kpi":
        key, _entry, p_args = get_context(orch_target)
        hours = max(1, min(168, int(orch_kpi_hours or 24)))
        send(
            summarize_gateway_metrics(
                p_args.team_dir,
                project_name=key,
                hours=hours,
                state_file=getattr(p_args, "state_file", None),
            ),
            context="orch-kpi",
            with_menu=True,
        )
        return True

    if cmd == "orch-use":
        if not orch_target:
            send("usage: aoe orch use <name>", context="orch-use usage")
            return True
        try:
            key, _ = get_manager_project(manager_state, orch_target)
        except Exception as e:
            send(str(e), context="orch-use blocked", with_menu=True)
            return True
        manager_state["active"] = key
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(f"active orch changed: {key}")
        return True

    if cmd == "orch-pause":
        if not orch_target:
            send("usage: /orch pause <O#|name> [reason]", context="orch-pause usage", with_menu=True)
            return True
        key, entry, _p_args = get_context(orch_target)
        alias = str(entry.get("project_alias", "")).strip() or key
        reason = str(rest or "").strip()
        now = now_iso()
        already = bool(entry.get("paused", False))
        changed = False

        if (not already) or (reason and reason != str(entry.get("paused_reason", "")).strip()):
            entry["paused"] = True
            entry["paused_at"] = now
            entry["paused_by"] = f"telegram:{chat_id}"
            if reason:
                entry["paused_reason"] = reason[:400]
            entry["updated_at"] = now
            changed = True

        if changed and not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)

        summary_reason = str(entry.get("paused_reason", "")).strip() or "-"
        send(
            "orch paused\n"
            f"- orch: {key} ({alias})\n"
            f"- reason: {summary_reason}\n"
            "note: /next, /fanout, /auto, /offdesk will skip paused orch by default.\n"
            "resume:\n"
            f"- /orch resume {alias}",
            context="orch-pause",
            with_menu=True,
        )
        return True

    if cmd == "orch-resume":
        if not orch_target:
            send("usage: /orch resume <O#|name>", context="orch-resume usage", with_menu=True)
            return True
        key, entry, _p_args = get_context(orch_target)
        alias = str(entry.get("project_alias", "")).strip() or key
        now = now_iso()

        if bool(entry.get("paused", False)):
            entry["paused"] = False
            entry["resumed_at"] = now
            entry["resumed_by"] = f"telegram:{chat_id}"
            entry["updated_at"] = now
            if not args.dry_run:
                save_manager_state(args.manager_state_file, manager_state)
            send(
                "orch resumed\n"
                f"- orch: {key} ({alias})\n"
                "next:\n"
                "- /queue\n"
                "- /next",
                context="orch-resume",
                with_menu=True,
            )
            return True

        send(
            "orch already active\n"
            f"- orch: {key} ({alias})",
            context="orch-resume noop",
            with_menu=True,
        )
        return True

    if cmd == "orch-hide":
        if not orch_target:
            send("usage: /orch hide <O#|name> [reason]", context="orch-hide usage", with_menu=True)
            return True
        key, entry, _p_args = get_context(orch_target)
        alias = str(entry.get("project_alias", "")).strip() or key
        reason = str(rest or "").strip() or "hidden by operator"
        now = now_iso()
        changed = False
        if not bool(entry.get("ops_hidden", False)) or reason != str(entry.get("ops_hidden_reason", "")).strip():
            entry["ops_hidden"] = True
            entry["ops_hidden_reason"] = reason[:400]
            entry["updated_at"] = now
            changed = True
        if changed and not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            "orch hidden from ops scope\n"
            f"- orch: {key} ({alias})\n"
            f"- reason: {str(entry.get('ops_hidden_reason', '')).strip() or '-'}\n"
            "note: /map, /queue, /next, /fanout, /offdesk, /auto default scope will skip this orch.\n"
            "restore:\n"
            f"- /orch unhide {alias}",
            context="orch-hide",
            with_menu=True,
        )
        return True

    if cmd == "orch-unhide":
        if not orch_target:
            send("usage: /orch unhide <O#|name>", context="orch-unhide usage", with_menu=True)
            return True
        key, entry, _p_args = get_context(orch_target)
        alias = str(entry.get("project_alias", "")).strip() or key
        if bool(entry.get("ops_hidden", False)):
            entry["ops_hidden"] = False
            entry["ops_hidden_reason"] = ""
            entry["updated_at"] = now_iso()
            if not args.dry_run:
                save_manager_state(args.manager_state_file, manager_state)
            send(
                "orch restored to ops scope\n"
                f"- orch: {key} ({alias})\n"
                "next:\n"
                f"- /orch status {alias}\n"
                "- /map\n"
                "- /queue",
                context="orch-unhide",
                with_menu=True,
            )
            return True
        send(
            "orch already visible in ops scope\n"
            f"- orch: {key} ({alias})",
            context="orch-unhide noop",
            with_menu=True,
        )
        return True

    return False
