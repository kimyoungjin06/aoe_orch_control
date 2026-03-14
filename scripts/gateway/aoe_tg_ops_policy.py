#!/usr/bin/env python3
"""Shared ops-scope and queue-selection policy helpers for Telegram gateway."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from aoe_tg_provider_fallback import rate_limit_retry_active
from aoe_tg_project_runtime import project_hidden_from_ops, project_runtime_issue


def project_ops_label(key: str, entry: Dict[str, Any]) -> str:
    alias = str(entry.get("project_alias", "")).strip().upper() or str(key)
    display = str(entry.get("display_name", key)).strip() or str(key)
    return f"{alias} {display}"


def project_ops_exclusion_reason(entry: Dict[str, Any]) -> str:
    reason = str(entry.get("ops_hidden_reason", "")).strip()
    if not reason and bool(entry.get("paused", False)):
        reason = str(entry.get("paused_reason", "")).strip() or "paused"
    return reason or "hidden"


def project_schedulable(
    entry: Dict[str, Any],
    *,
    skip_paused: bool = False,
    require_ready: bool = False,
) -> bool:
    if not isinstance(entry, dict):
        return False
    if project_hidden_from_ops(entry):
        return False
    if skip_paused and bool(entry.get("paused", False)):
        return False
    if require_ready and project_runtime_issue(entry):
        return False
    return True


def list_ops_projects(
    projects: Any,
    *,
    skip_paused: bool = False,
    require_ready: bool = False,
) -> List[Tuple[str, Dict[str, Any]]]:
    rows: List[Tuple[str, Dict[str, Any]]] = []
    if not isinstance(projects, dict):
        return rows
    for key, entry in projects.items():
        if not project_schedulable(
            entry,
            skip_paused=skip_paused,
            require_ready=require_ready,
        ):
            continue
        rows.append((str(key), entry))
    return rows


def visible_ops_project_keys(
    projects: Any,
    *,
    skip_paused: bool = False,
    require_ready: bool = False,
) -> List[str]:
    return [key for key, _entry in list_ops_projects(projects, skip_paused=skip_paused, require_ready=require_ready)]


def summarize_ops_scope(projects: Any) -> Dict[str, List[str]]:
    included: List[str] = []
    excluded: List[str] = []
    if not isinstance(projects, dict):
        return {"included": included, "excluded": excluded}

    for key, entry in projects.items():
        if not isinstance(entry, dict):
            continue
        label = project_ops_label(str(key), entry)
        if project_hidden_from_ops(entry):
            excluded.append(f"{label} ({project_ops_exclusion_reason(entry)})")
            continue
        included.append(label)

    return {"included": included, "excluded": excluded}


def normalize_priority(token: Any) -> str:
    raw = str(token or "").strip().upper()
    if raw in {"P1", "P2", "P3"}:
        return raw
    return "P2"


def priority_rank(priority: Any) -> int:
    return {"P1": 1, "P2": 2, "P3": 3}.get(normalize_priority(priority), 9)


def project_alias(entry: Dict[str, Any], fallback: str) -> str:
    token = str(entry.get("project_alias", "")).strip().upper()
    if re.fullmatch(r"O[1-9]\d{0,2}", token):
        return token
    return str(fallback or "").strip() or "-"


def todo_rows(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = entry.get("todos") if isinstance(entry, dict) else None
    if not isinstance(raw, list):
        return []
    return [row for row in raw if isinstance(row, dict)]


def sorted_open_todos(todos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in todos:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", "open")).strip().lower() or "open"
        if status != "open":
            continue
        rows.append(row)
    rows.sort(
        key=lambda r: (
            priority_rank(r.get("priority", "P2")),
            str(r.get("created_at", "")),
            str(r.get("id", "")),
        )
    )
    return rows


def sorted_resumable_todos(entry: Dict[str, Any]) -> List[Dict[str, Any]]:
    todos = todo_rows(entry)
    rows: List[Dict[str, Any]] = []
    for row in todos:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", "open")).strip().lower() or "open"
        if status != "running":
            continue
        todo_id = str(row.get("id", "")).strip()
        if not todo_id:
            continue
        if linked_task_blocks_todo(entry, todo_id):
            continue
        if not linked_tasks_for_todo(entry, todo_id):
            continue
        rows.append(row)
    rows.sort(
        key=lambda r: (
            priority_rank(r.get("priority", "P2")),
            str(r.get("updated_at", "")),
            str(r.get("created_at", "")),
            str(r.get("id", "")),
        )
    )
    return rows


def linked_tasks_for_todo(entry: Dict[str, Any], todo_id: str) -> List[Dict[str, Any]]:
    token = str(todo_id or "").strip()
    if not token:
        return []
    tasks = entry.get("tasks") if isinstance(entry, dict) else None
    if not isinstance(tasks, dict):
        return []
    return [
        task
        for task in tasks.values()
        if isinstance(task, dict) and str(task.get("todo_id", "")).strip() == token
    ]


def linked_task_blocks_todo(entry: Dict[str, Any], todo_id: str) -> bool:
    for task in linked_tasks_for_todo(entry, todo_id):
        status = str(task.get("status", "")).strip().lower()
        if status not in {"pending", "running"}:
            continue
        tf_phase = str(task.get("tf_phase", "")).strip().lower()
        rate_limit = task.get("rate_limit") if isinstance(task.get("rate_limit"), dict) else {}
        if tf_phase == "rate_limited" or str(rate_limit.get("mode", "")).strip().lower() == "blocked":
            if rate_limit_retry_active(rate_limit):
                return True
            continue
        return True
    return False


def linked_task_blocks_project(entry: Dict[str, Any], todo_id: str) -> bool:
    tasks = linked_tasks_for_todo(entry, todo_id)
    if not tasks:
        return True
    for task in tasks:
        status = str(task.get("status", "")).strip().lower()
        if status not in {"pending", "running"}:
            continue
        tf_phase = str(task.get("tf_phase", "")).strip().lower()
        rate_limit = task.get("rate_limit") if isinstance(task.get("rate_limit"), dict) else {}
        if tf_phase == "rate_limited" or str(rate_limit.get("mode", "")).strip().lower() == "blocked":
            continue
        return True
    return False


def project_queue_snapshot(entry: Dict[str, Any]) -> Dict[str, Any]:
    todos = todo_rows(entry)
    running_count = 0
    blocked_count = 0
    parked_count = 0
    for row in todos:
        status = str(row.get("status", "open")).strip().lower() or "open"
        if status == "running":
            todo_id = str(row.get("id", "")).strip()
            if todo_id and linked_tasks_for_todo(entry, todo_id):
                if linked_task_blocks_project(entry, todo_id):
                    running_count += 1
                else:
                    parked_count += 1
            else:
                running_count += 1
        elif status == "blocked":
            blocked_count += 1
    open_rows = sorted_open_todos(todos)
    resume_rows = sorted_resumable_todos(entry)
    best_open = open_rows[0] if open_rows else None
    best_resume = resume_rows[0] if resume_rows else None
    pending = entry.get("pending_todo") if isinstance(entry, dict) else None
    pending_id = str(pending.get("todo_id", "")).strip() if isinstance(pending, dict) else ""
    return {
        "todos": todos,
        "open_rows": open_rows,
        "resume_rows": resume_rows,
        "best_open": best_open,
        "best_resume": best_resume,
        "open_count": len(open_rows),
        "resume_count": len(resume_rows),
        "running_count": running_count,
        "parked_count": parked_count,
        "blocked_count": blocked_count,
        "pending_id": pending_id,
        "has_running": running_count > 0,
        "has_parked": parked_count > 0,
        "has_blocked": blocked_count > 0,
        "has_pending": bool(pending_id),
    }


def find_pending_todo_for_chat(
    projects: Any,
    chat_id: str,
    *,
    skip_paused: bool = False,
    require_ready: bool = True,
) -> Optional[Tuple[str, Dict[str, Any], Dict[str, Any]]]:
    cid = str(chat_id or "").strip()
    if not cid:
        return None
    hits: List[Tuple[str, str, Dict[str, Any], Dict[str, Any]]] = []
    for key, entry in list_ops_projects(projects, skip_paused=skip_paused, require_ready=require_ready):
        pending = entry.get("pending_todo")
        if not isinstance(pending, dict):
            continue
        if str(pending.get("chat_id", "")).strip() != cid:
            continue
        todo_id = str(pending.get("todo_id", "")).strip()
        if not todo_id:
            continue
        selected_at = str(pending.get("selected_at", "")).strip() or "9999"
        hits.append((selected_at, str(key), entry, pending))
    if not hits:
        return None
    hits.sort(key=lambda row: (row[0], row[1]))
    _selected_at, key, entry, pending = hits[0]
    return key, entry, pending


def new_ops_skip_counters() -> Dict[str, int]:
    return {
        "paused": 0,
        "unready": 0,
        "empty": 0,
        "busy": 0,
        "pending": 0,
        "missing_alias": 0,
    }


def format_ops_skip_counters(counters: Dict[str, Any]) -> List[str]:
    ordered = [
        ("paused", "skipped_paused"),
        ("unready", "skipped_unready"),
        ("empty", "skipped_empty"),
        ("busy", "skipped_busy"),
        ("pending", "skipped_pending"),
        ("missing_alias", "skipped_missing_alias"),
    ]
    lines: List[str] = []
    for key, label in ordered:
        lines.append(f"- {label}: {int(counters.get(key, 0) or 0)}")
    return lines


def format_ops_skip_detail(counters: Dict[str, Any]) -> str:
    ordered = ["paused", "unready", "empty", "busy", "pending", "missing_alias"]
    parts = [f"{key}={int(counters.get(key, 0) or 0)}" for key in ordered]
    return " ".join(parts)


def build_batch_finish_message(
    *,
    title: str,
    executed: int,
    reason: str,
    next_lines: List[str],
    counters: Optional[Dict[str, Any]] = None,
) -> str:
    lines = [
        title,
        f"- executed: {int(executed)}",
    ]
    if isinstance(counters, dict):
        lines.extend(format_ops_skip_counters(counters))
    lines.append(f"- reason: {str(reason or '').strip() or '-'}")
    lines.append("next:")
    for row in next_lines:
        lines.append(row)
    return "\n".join(lines)


def build_no_runnable_todo_message(
    *,
    focus_label: str = "",
    unready_rows: Optional[List[str]] = None,
) -> str:
    rows = list(unready_rows or [])
    locked = str(focus_label or "").strip()
    body = (
        "next: no runnable open todo found.\n"
        "reasons:\n"
    )
    if locked:
        body += f"- locked project {locked} has no runnable todo, or\n"
    else:
        body += "- all projects are empty, or\n"
    body += "- some project has a running todo, or\n"
    body += "- only blocked todos remain, or\n"
    if locked:
        body += "- the locked project is paused, or\n"
    else:
        body += "- some projects are paused, or\n"
    body += (
        "- pending todo exists\n"
        "next:\n"
        "- /todo (per project)\n"
        "- /map\n"
    )
    if rows:
        body += "unready:\n"
        body += "\n".join(rows[:5]) + "\n"
        body += "- /orch status O#   (inspect/fix missing runtime files)\n"
    if locked:
        body += "- /focus off\n"
    body += "- /next force  (ignore busy checks)"
    return body
