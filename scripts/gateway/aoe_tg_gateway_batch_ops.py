#!/usr/bin/env python3
"""Gateway top-level batch ops helpers."""

from __future__ import annotations

import argparse
from typing import Any, Callable, Dict, Optional, Tuple

from aoe_tg_ops_policy import build_batch_finish_message, format_ops_skip_detail, new_ops_skip_counters
from aoe_tg_project_runtime import project_hidden_from_ops, project_runtime_issue
from aoe_tg_queue_engine import project_capacity_snapshot
from aoe_tg_queue_engine import drain_peek_next_todo as queue_drain_peek_next_todo
from aoe_tg_ops_policy import project_queue_snapshot


def parse_drain_args(rest: str) -> Tuple[int, bool]:
    tokens = [t for t in str(rest or "").split() if t.strip()]
    force = any(t.lower() in {"force", "!", "--force"} for t in tokens)
    limit = 10
    for t in tokens:
        if t.isdigit():
            limit = int(t)
            break
        low = t.lower()
        if low in {"all", "*", "until-empty", "until_empty"}:
            limit = 9999
            break
    limit = max(1, min(50, int(limit)))
    return limit, force


def parse_fanout_args(rest: str) -> Tuple[int, bool]:
    tokens = [t for t in str(rest or "").split() if t.strip()]
    force = any(t.lower() in {"force", "!", "--force"} for t in tokens)
    limit = 9999
    for t in tokens:
        if t.isdigit():
            limit = int(t)
            break
    limit = max(1, min(50, int(limit)))
    return limit, force


def drain_peek_next_todo(
    manager_state: Dict[str, Any],
    chat_id: str,
    *,
    force: bool,
) -> Tuple[str, str, str]:
    return queue_drain_peek_next_todo(manager_state, chat_id, force=force)


def handle_drain_command(
    *,
    args: argparse.Namespace,
    token: str,
    chat_id: str,
    rest: str,
    trace_id: str,
    send: Callable[..., bool],
    log_event: Callable[..., None],
    deps: Dict[str, Any],
) -> None:
    limit, force = parse_drain_args(rest)
    force_token = " force" if force else ""
    send(
        "drain started\n"
        f"- limit: {limit}\n"
        f"- force: {'yes' if force else 'no'}\n"
        "next:\n"
        "- /queue (overview)\n"
        "- /next (single step)\n"
        "- /cancel (pending-mode only)\n",
        context="drain-start",
        with_menu=True,
    )
    log_event(event="drain_start", status="running", stage="intake", detail=f"limit={limit} force={'yes' if force else 'no'}")

    executed = 0
    stop_reason = ""

    for i in range(int(limit)):
        manager_state = deps["load_manager_state"](args.manager_state_file, args.project_root, args.team_dir)
        if deps["get_confirm_action"](manager_state, chat_id):
            stop_reason = "confirm_pending"
            break

        if not args.dry_run:
            project_key, todo_id, reason = drain_peek_next_todo(manager_state, chat_id, force=force)
            if not project_key or not todo_id:
                stop_reason = reason
                break
        else:
            project_key, todo_id, reason = "-", "-", "dry_run"

        deps["handle_text_message"](
            args=args,
            token=token,
            chat_id=chat_id,
            text=f"/next{force_token}",
            trace_id=f"{trace_id}/drain-{i+1}",
        )
        executed += 1

        if args.dry_run:
            continue

        manager_state = deps["load_manager_state"](args.manager_state_file, args.project_root, args.team_dir)
        if deps["get_confirm_action"](manager_state, chat_id):
            stop_reason = "confirm_pending"
            break

        projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
        entry = projects.get(project_key) if isinstance(projects, dict) and isinstance(projects.get(project_key), dict) else {}
        todos = entry.get("todos") if isinstance(entry, dict) else None
        status = ""
        if isinstance(todos, list):
            for row in todos:
                if not isinstance(row, dict):
                    continue
                if str(row.get("id", "")).strip() != str(todo_id).strip():
                    continue
                status = str(row.get("status", "")).strip().lower()
                break
        if status in {"blocked", "running", "open"}:
            stop_reason = f"todo_{status or 'unknown'}"
            break

    if not stop_reason:
        stop_reason = "limit_reached" if executed >= limit else "done"
    send(
        build_batch_finish_message(
            title="drain finished",
            executed=executed,
            reason=stop_reason,
            next_lines=["- /queue", "- /next"],
        ),
        context="drain-finish",
        with_menu=True,
    )
    log_event(event="drain_finish", status="completed", stage="close", detail=f"executed={executed} reason={stop_reason}")


def handle_fanout_command(
    *,
    args: argparse.Namespace,
    token: str,
    chat_id: str,
    rest: str,
    trace_id: str,
    send: Callable[..., bool],
    log_event: Callable[..., None],
    deps: Dict[str, Any],
) -> None:
    limit, force = parse_fanout_args(rest)
    force_token = " force" if force else ""

    manager_state = deps["load_manager_state"](args.manager_state_file, args.project_root, args.team_dir)
    locked = deps["project_lock_label"](manager_state)
    if locked:
        send(
            "fanout blocked by project lock\n"
            f"- project_lock: {locked}\n"
            "- reason: fanout is a global multi-project wave\n"
            "next:\n"
            "- /next\n"
            "- /auto on next\n"
            "- /offdesk on\n"
            "- /focus off",
            context="fanout-locked",
            with_menu=True,
        )
        log_event(event="fanout_finish", status="rejected", stage="intake", detail=f"project_lock={locked}")
        return

    send(
        "fanout started\n"
        f"- max_projects: {limit}\n"
        f"- force: {'yes' if force else 'no'}\n"
        "rule: at most 1 todo per project\n"
        "next:\n"
        "- /queue (overview)\n"
        "- /fanout (one wave)\n"
        "- /auto on fanout (continuous)\n",
        context="fanout-start",
        with_menu=True,
    )
    log_event(event="fanout_start", status="running", stage="intake", detail=f"limit={limit} force={'yes' if force else 'no'}")

    executed = 0
    counters = new_ops_skip_counters()
    stop_reason = ""

    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    if not isinstance(projects, dict) or not projects:
        send("fanout: no orch projects registered. use /map and /orch add first.", context="fanout-empty", with_menu=True)
        log_event(event="fanout_finish", status="completed", stage="close", detail="no_projects")
        return

    deps["ensure_project_aliases"](manager_state)

    def _proj_sort_key(k: str) -> Tuple[int, str, int, int, int, str]:
        entry = projects.get(k) if isinstance(projects.get(k), dict) else {}
        alias = deps["normalize_project_alias"](str((entry or {}).get("project_alias", ""))) or "O?"
        capacity = project_capacity_snapshot(entry if isinstance(entry, dict) else {})
        return (
            int(capacity.get("penalty_rank", 0) or 0),
            str(capacity.get("next_retry_at", "") or "9999-12-31T23:59:59+00:00"),
            int(capacity.get("active_count", 0) or 0),
            int(capacity.get("provider_count", 0) or 0),
            deps["extract_project_alias_index"](alias),
            str(k),
        )

    ordered_keys = sorted(
        [str(k) for k, entry in projects.items() if isinstance(entry, dict) and not project_hidden_from_ops(entry)],
        key=_proj_sort_key,
    )

    for idx, project_key in enumerate(ordered_keys[: int(limit)], start=1):
        manager_state = deps["load_manager_state"](args.manager_state_file, args.project_root, args.team_dir)
        if deps["get_confirm_action"](manager_state, chat_id):
            stop_reason = "confirm_pending"
            break

        projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
        entry = projects.get(project_key) if isinstance(projects, dict) and isinstance(projects.get(project_key), dict) else {}

        if project_hidden_from_ops(entry if isinstance(entry, dict) else {}):
            continue
        if (not force) and deps["bool_from_json"]((entry or {}).get("paused"), False):
            counters["paused"] += 1
            continue
        if project_runtime_issue(entry if isinstance(entry, dict) else {}):
            counters["unready"] += 1
            continue

        alias = deps["normalize_project_alias"](str((entry or {}).get("project_alias", ""))) or deps["project_alias_for_key"](manager_state, project_key)
        if not alias:
            counters["missing_alias"] += 1
            continue

        pending = entry.get("pending_todo") if isinstance(entry, dict) else None
        if (not force) and isinstance(pending, dict) and str(pending.get("todo_id", "")).strip():
            counters["pending"] += 1
            continue

        snap = project_queue_snapshot(entry if isinstance(entry, dict) else {})
        open_cnt = int(snap["open_count"])
        busy_cnt = int(snap["running_count"])

        if open_cnt <= 0:
            counters["empty"] += 1
            continue
        if (not force) and busy_cnt > 0:
            counters["busy"] += 1
            continue

        deps["handle_text_message"](
            args=args,
            token=token,
            chat_id=chat_id,
            text=f"/todo {alias} next{force_token}",
            trace_id=f"{trace_id}/fanout-{idx}",
        )
        executed += 1

        if not args.dry_run:
            manager_state = deps["load_manager_state"](args.manager_state_file, args.project_root, args.team_dir)
            if deps["get_confirm_action"](manager_state, chat_id):
                stop_reason = "confirm_pending"
                break

    if not stop_reason:
        stop_reason = "done"

    send(
        build_batch_finish_message(
            title="fanout finished",
            executed=executed,
            reason=stop_reason,
            counters=counters,
            next_lines=["- /queue", "- /fanout"],
        ),
        context="fanout-finish",
        with_menu=True,
    )
    log_event(
        event="fanout_finish",
        status="completed",
        stage="close",
        detail=f"executed={executed} reason={stop_reason} {format_ops_skip_detail(counters)}",
    )


def handle_gc_command(
    *,
    args: argparse.Namespace,
    chat_id: str,
    rest: str,
    manager_state: Dict[str, Any],
    send: Callable[..., bool],
    log_event: Callable[..., None],
    deps: Dict[str, Any],
) -> None:
    _ = chat_id
    tokens = [t for t in str(rest or "").split() if t.strip()]
    sub = (tokens[0].lower() if tokens else "run").strip()
    force = any(t.lower() in {"force", "!", "--force"} for t in tokens[1:]) or (sub in {"force"} and len(tokens) == 1)

    retention_days = deps["room_retention_days"]()
    ttl_hours = deps["tf_exec_cache_ttl_hours"]()
    retention_policy = deps["normalize_tf_exec_retention"]()

    if sub in {"status", "show"}:
        send(
            "gc policy\n"
            f"- room_retention_days: {retention_days} (0 disables)\n"
            f"- tf_artifact_policy: {retention_policy}\n"
            f"- tf_exec_cache_ttl_hours: {ttl_hours} (0 disables; ignored when policy=all)\n"
            "run:\n"
            "- /gc\n"
            "- /gc force",
            context="gc-status",
            with_menu=True,
        )
        return

    if bool(getattr(args, "dry_run", False)):
        send(
            "gc skipped (dry-run)\n"
            f"- room_retention_days: {retention_days}\n"
            f"- tf_artifact_policy: {retention_policy}\n"
            f"- tf_exec_cache_ttl_hours: {ttl_hours}\n"
            "run:\n"
            "- /gc status",
            context="gc-dry-run",
            with_menu=True,
        )
        return

    removed_rooms = deps["cleanup_room_logs"](args.team_dir, force=force)
    removed_tf = deps["cleanup_tf_exec_artifacts"](args.manager_state_file, manager_state)
    log_event(
        event="gc",
        stage="close",
        status="completed",
        detail=f"room_removed={removed_rooms} tf_removed={removed_tf} force={'yes' if force else 'no'}",
    )
    send(
        "gc complete\n"
        f"- room_removed: {removed_rooms}\n"
        f"- tf_exec_removed: {removed_tf}\n"
        f"- room_retention_days: {retention_days}\n"
        f"- tf_artifact_policy: {retention_policy}\n"
        f"- tf_exec_cache_ttl_hours: {ttl_hours}\n"
        f"- force: {'yes' if force else 'no'}\n"
        "next:\n"
        "- /status\n"
        "- /queue\n"
        "- /room tail 20",
        context="gc",
        with_menu=True,
    )
