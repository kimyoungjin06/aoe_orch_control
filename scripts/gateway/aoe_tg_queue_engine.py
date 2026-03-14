#!/usr/bin/env python3
"""Shared queue selection helpers for scheduler and gateway batch runners."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from aoe_tg_ops_policy import (
    find_pending_todo_for_chat as find_ops_pending_todo_for_chat,
    linked_task_blocks_todo,
    list_ops_projects,
    priority_rank as ops_priority_rank,
    project_alias as ops_project_alias,
    project_queue_snapshot,
)
from aoe_tg_project_runtime import project_hidden_from_ops, project_runtime_issue

_STATUS_OPEN = "open"
_STATUS_RUNNING = "running"
_STATUS_BLOCKED = "blocked"
_STATUS_DONE = "done"
_STATUS_CANCELED = "canceled"


def sorted_active_todos(todos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for row in todos:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
        if status in {_STATUS_DONE, _STATUS_CANCELED}:
            continue
        rows.append(row)

    def _status_rank(st: str) -> int:
        token = str(st or "").strip().lower()
        if token == _STATUS_RUNNING:
            return 0
        if token == _STATUS_BLOCKED:
            return 1
        if token == _STATUS_OPEN:
            return 2
        return 8

    rows.sort(
        key=lambda r: (
            _status_rank(str(r.get("status", _STATUS_OPEN))),
            ops_priority_rank(str(r.get("priority", "P2"))),
            str(r.get("created_at", "")),
            str(r.get("id", "")),
        )
    )
    return rows


def count_todo_statuses(todos: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {
        _STATUS_OPEN: 0,
        _STATUS_RUNNING: 0,
        _STATUS_BLOCKED: 0,
        _STATUS_DONE: 0,
        _STATUS_CANCELED: 0,
    }
    for row in todos:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
        if status not in counts:
            status = _STATUS_OPEN
        counts[status] += 1
    return counts


def find_todo_item(entry: Dict[str, Any], todo_id: str) -> Optional[Dict[str, Any]]:
    token = str(todo_id or "").strip()
    if not token:
        return None
    raw = entry.get("todos")
    if not isinstance(raw, list):
        return None
    for row in raw:
        if not isinstance(row, dict):
            continue
        if str(row.get("id", "")).strip() == token:
            return row
    return None


def has_task_linked_to_todo(entry: Dict[str, Any], todo_id: str) -> bool:
    token = str(todo_id or "").strip()
    if not token:
        return False
    return linked_task_blocks_todo(entry, token)


def pick_global_next_candidate(
    projects: Dict[str, Any],
    *,
    ignore_busy: bool = False,
    skip_paused: bool = False,
) -> Optional[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    for key, entry in list_ops_projects(projects, skip_paused=skip_paused, require_ready=True):
        snap = project_queue_snapshot(entry)
        if not ignore_busy and snap["has_running"]:
            continue
        resume_item = snap["best_resume"] if isinstance(snap.get("best_resume"), dict) else None
        item = resume_item if isinstance(resume_item, dict) else snap["best_open"]
        if not isinstance(item, dict):
            continue
        candidates.append(
            {
                "project_key": str(key),
                "project_alias": ops_project_alias(entry, str(key)),
                "todo": item,
                "selection_kind": "resume" if isinstance(resume_item, dict) and item is resume_item else "open",
                "priority_rank": ops_priority_rank(str(item.get("priority", "P2"))),
                "created_at": str(item.get("created_at", "")),
                "todo_id": str(item.get("id", "")).strip(),
            }
        )
    if not candidates:
        return None
    candidates.sort(
        key=lambda c: (
            int(c.get("priority_rank", 9) or 9),
            str(c.get("created_at", "")),
            str(c.get("project_alias", "")),
            str(c.get("todo_id", "")),
        )
    )
    return candidates[0]


def drain_peek_next_todo(
    manager_state: Dict[str, Any],
    chat_id: str,
    *,
    force: bool,
) -> Tuple[str, str, str]:
    """Return ``(project_key, todo_id, reason)`` for the next runnable item."""

    projects = manager_state.get("projects")
    if not isinstance(projects, dict) or not projects:
        return "", "", "no_projects"

    cid = str(chat_id or "").strip()
    if not cid:
        return "", "", "invalid_chat_id"

    if not force:
        pending_hit = find_ops_pending_todo_for_chat(projects, cid, skip_paused=True, require_ready=True)
        if pending_hit:
            p_key, entry, pending = pending_hit
            p_todo = str(pending.get("todo_id", "")).strip()
            if has_task_linked_to_todo(entry if isinstance(entry, dict) else {}, p_todo):
                return "", "", "pending_has_active_task"
            snap = project_queue_snapshot(entry if isinstance(entry, dict) else {})
            for row in snap["todos"]:
                if str(row.get("id", "")).strip() != p_todo:
                    continue
                summary = str(row.get("summary", "")).strip()
                if not summary:
                    return "", "", "pending_missing_summary"
                return p_key, p_todo, "resume_pending"
            return "", "", "pending_not_found"

    candidates: List[Tuple[int, str, str, str, str]] = []
    skipped_unready = 0
    for key, entry in projects.items():
        if not isinstance(entry, dict):
            continue
        if project_hidden_from_ops(entry):
            continue
        if project_runtime_issue(entry):
            skipped_unready += 1
            continue
        if (not force) and bool(entry.get("paused", False)):
            continue
        snap = project_queue_snapshot(entry)
        if not force and snap["has_running"]:
            continue
        best = snap["best_open"]
        if not isinstance(best, dict):
            continue
        todo_id = str(best.get("id", "")).strip()
        summary = str(best.get("summary", "")).strip()
        if not todo_id or not summary:
            continue
        alias = ops_project_alias(entry, str(key))
        candidates.append(
            (
                ops_priority_rank(str(best.get("priority", "P2"))),
                str(best.get("created_at", "")),
                str(alias),
                str(todo_id),
                str(key),
            )
        )
    if not candidates:
        if skipped_unready > 0:
            return "", "", "unready_project"
        return "", "", "no_runnable_open_todo"
    candidates.sort()
    _rank, _created_at, _alias, todo_id, project_key = candidates[0]
    return project_key, todo_id, "candidate"
