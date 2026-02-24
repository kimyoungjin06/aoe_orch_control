#!/usr/bin/env python3
"""Orchestrator overview handlers for Telegram gateway."""

from typing import Any, Callable, Dict, Optional

def handle_orch_overview_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    orch_target: Optional[str],
    orch_monitor_limit: Optional[int],
    orch_kpi_hours: Optional[int],
    send: Callable[..., bool],
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]],
    save_manager_state: Callable[..., None],
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
        send(summarize_orch_registry(manager_state), context="orch-list")
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
            summarize_gateway_metrics(p_args.team_dir, project_name=key, hours=hours),
            context="orch-kpi",
            with_menu=True,
        )
        return True

    if cmd == "orch-use":
        if not orch_target:
            send("usage: aoe orch use <name>", context="orch-use usage")
            return True
        key, _ = get_manager_project(manager_state, orch_target)
        manager_state["active"] = key
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(f"active orch changed: {key}")
        return True

    return False


