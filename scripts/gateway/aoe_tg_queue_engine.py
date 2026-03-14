#!/usr/bin/env python3
"""Shared queue selection helpers for scheduler and gateway batch runners."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from aoe_tg_ops_policy import (
    find_pending_todo_for_chat as find_ops_pending_todo_for_chat,
    linked_task_blocks_todo,
    linked_tasks_for_todo,
    list_ops_projects,
    priority_rank as ops_priority_rank,
    project_alias as ops_project_alias,
    project_queue_snapshot,
)
from aoe_tg_provider_fallback import parse_retry_at, rate_limit_retry_active
from aoe_tg_project_runtime import project_hidden_from_ops, project_runtime_issue

_STATUS_OPEN = "open"
_STATUS_RUNNING = "running"
_STATUS_BLOCKED = "blocked"
_STATUS_DONE = "done"
_STATUS_CANCELED = "canceled"


def _provider_repeat_count_map(memory_state: Any) -> Dict[str, int]:
    if not isinstance(memory_state, dict):
        return {}
    history = memory_state.get("recovery_repeat_history") if isinstance(memory_state.get("recovery_repeat_history"), list) else []
    counts: Dict[str, int] = {}
    for row in history:
        if not isinstance(row, dict):
            continue
        for alias in row.get("aliases") or []:
            token = str(alias or "").strip().upper()
            if not token:
                continue
            counts[token] = int(counts.get(token, 0) or 0) + 1
    return counts


def _rate_limited_pending_todo(entry: Dict[str, Any], todo_id: str) -> bool:
    token = str(todo_id or "").strip()
    if not token:
        return False
    for task in linked_tasks_for_todo(entry, token):
        if not isinstance(task, dict):
            continue
        status = str(task.get("status", "")).strip().lower()
        if status not in {"pending", "running"}:
            continue
        rate_limit = task.get("rate_limit") if isinstance(task.get("rate_limit"), dict) else {}
        if not rate_limit_retry_active(rate_limit):
            continue
        tf_phase = str(task.get("tf_phase", "")).strip().lower()
        if tf_phase == "rate_limited" or str(rate_limit.get("mode", "")).strip().lower() == "blocked":
            return True
    return False


def project_capacity_snapshot(
    entry: Dict[str, Any],
    *,
    now: Optional[datetime] = None,
    recovery_grace_until: Any = None,
    provider_capacity_state: Any = None,
) -> Dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    grace_until = parse_retry_at(recovery_grace_until)
    tasks = entry.get("tasks") if isinstance(entry, dict) else None
    if not isinstance(tasks, dict):
        return {
            "penalty_rank": 0,
            "active_count": 0,
            "recent_recovered_count": 0,
            "repeat_count": 0,
            "provider_count": 0,
            "next_retry_at": "",
            "limited_providers": [],
        }

    active_count = 0
    recent_recovered_count = 0
    alias = str(entry.get("project_alias", "")).strip().upper()
    repeat_count = int(_provider_repeat_count_map(provider_capacity_state).get(alias, 0) or 0) if alias else 0
    limited_providers: set[str] = set()
    next_retry_dt: Optional[datetime] = None
    for row in tasks.values():
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", "")).strip().lower()
        if status not in {"pending", "running"}:
            continue
        rate_limit = row.get("rate_limit") if isinstance(row.get("rate_limit"), dict) else {}
        if not rate_limit_retry_active(rate_limit, now=current):
            if grace_until is not None and grace_until > current.astimezone(timezone.utc):
                parsed_retry = parse_retry_at(rate_limit.get("retry_at"))
                if parsed_retry is not None and parsed_retry <= current.astimezone(timezone.utc):
                    recent_recovered_count += 1
            continue
        active_count += 1
        for provider in rate_limit.get("limited_providers", []):
            token = str(provider or "").strip().lower()
            if token:
                limited_providers.add(token)
        parsed = parse_retry_at(rate_limit.get("retry_at"))
        if parsed is None:
            continue
        if next_retry_dt is None or parsed < next_retry_dt:
            next_retry_dt = parsed

    penalty_rank = 2 if active_count > 0 else (1 if recent_recovered_count > 0 else 0)
    if penalty_rank > 0 and repeat_count > 0:
        penalty_rank += 1

    return {
        "penalty_rank": penalty_rank,
        "active_count": active_count,
        "recent_recovered_count": recent_recovered_count,
        "repeat_count": repeat_count,
        "provider_count": len(limited_providers),
        "next_retry_at": next_retry_dt.isoformat() if next_retry_dt else "",
        "limited_providers": sorted(limited_providers),
    }


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
    recovery_grace_until: Any = None,
    provider_capacity_state: Any = None,
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
        capacity = project_capacity_snapshot(
            entry,
            recovery_grace_until=recovery_grace_until,
            provider_capacity_state=provider_capacity_state,
        )
        candidates.append(
            {
                "project_key": str(key),
                "project_alias": ops_project_alias(entry, str(key)),
                "todo": item,
                "selection_kind": "resume" if isinstance(resume_item, dict) and item is resume_item else "open",
                "priority_rank": ops_priority_rank(str(item.get("priority", "P2"))),
                "capacity_penalty_rank": int(capacity.get("penalty_rank", 0) or 0),
                "capacity_active_count": int(capacity.get("active_count", 0) or 0),
                "capacity_recent_recovered_count": int(capacity.get("recent_recovered_count", 0) or 0),
                "capacity_repeat_count": int(capacity.get("repeat_count", 0) or 0),
                "capacity_provider_count": int(capacity.get("provider_count", 0) or 0),
                "capacity_next_retry_at": str(capacity.get("next_retry_at", "")),
                "capacity_limited_providers": list(capacity.get("limited_providers", [])),
                "created_at": str(item.get("created_at", "")),
                "todo_id": str(item.get("id", "")).strip(),
            }
        )
    if not candidates:
        return None
    candidates.sort(
        key=lambda c: (
            int(c.get("priority_rank", 9) if c.get("priority_rank", None) is not None else 9),
            int(c.get("capacity_penalty_rank", 9) if c.get("capacity_penalty_rank", None) is not None else 9),
            str(c.get("capacity_next_retry_at", "") or "9999-12-31T23:59:59+00:00"),
            int(c.get("capacity_active_count", 0) if c.get("capacity_active_count", None) is not None else 0),
            int(c.get("capacity_recent_recovered_count", 0) if c.get("capacity_recent_recovered_count", None) is not None else 0),
            int(c.get("capacity_repeat_count", 0) if c.get("capacity_repeat_count", None) is not None else 0),
            int(c.get("capacity_provider_count", 0) if c.get("capacity_provider_count", None) is not None else 0),
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
    recovery_grace_until: Any = None,
    provider_capacity_state: Any = None,
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
        pending_blocked_by_rate_limit = False
        if pending_hit:
            p_key, entry, pending = pending_hit
            p_todo = str(pending.get("todo_id", "")).strip()
            if has_task_linked_to_todo(entry if isinstance(entry, dict) else {}, p_todo):
                if _rate_limited_pending_todo(entry if isinstance(entry, dict) else {}, p_todo):
                    pending_blocked_by_rate_limit = True
                else:
                    return "", "", "pending_has_active_task"
            snap = project_queue_snapshot(entry if isinstance(entry, dict) else {})
            for row in snap["todos"]:
                if str(row.get("id", "")).strip() != p_todo:
                    continue
                if pending_blocked_by_rate_limit:
                    break
                summary = str(row.get("summary", "")).strip()
                if not summary:
                    return "", "", "pending_missing_summary"
                return p_key, p_todo, "resume_pending"
            if not pending_blocked_by_rate_limit:
                return "", "", "pending_not_found"

    candidates: List[Tuple[int, int, str, int, int, int, str, str, str]] = []
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
        capacity = project_capacity_snapshot(
            entry,
            recovery_grace_until=recovery_grace_until,
            provider_capacity_state=provider_capacity_state,
        )
        candidates.append(
            (
                ops_priority_rank(str(best.get("priority", "P2"))),
                int(capacity.get("penalty_rank", 0) or 0),
                str(capacity.get("next_retry_at", "") or "9999-12-31T23:59:59+00:00"),
                int(capacity.get("active_count", 0) or 0),
                int(capacity.get("recent_recovered_count", 0) or 0),
                int(capacity.get("repeat_count", 0) or 0),
                int(capacity.get("provider_count", 0) or 0),
                str(best.get("created_at", "")),
                str(alias),
                str(todo_id),
                str(key),
            )
        )
    if not candidates:
        if skipped_unready > 0:
            return "", "", "unready_project"
        if not force and 'pending_blocked_by_rate_limit' in locals() and pending_blocked_by_rate_limit:
            return "", "", "pending_rate_limited"
        return "", "", "no_runnable_open_todo"
    candidates.sort()
    _rank, _penalty, _retry_at, _blocked, _recent, _repeat, _providers, _created_at, _alias, todo_id, project_key = candidates[0]
    return project_key, todo_id, "candidate"
