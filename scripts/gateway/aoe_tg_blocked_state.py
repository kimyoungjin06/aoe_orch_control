#!/usr/bin/env python3
"""Shared helpers for blocked/manual-followup todo state."""

from __future__ import annotations

from typing import Any, Callable, Dict, List


def blocked_reason_preview(raw: Any, limit: int = 72) -> str:
    text = " ".join(str(raw or "").strip().split())
    if len(text) > limit:
        return text[: max(0, limit - 3)].rstrip() + "..."
    return text


def blocked_bucket_label(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    if token == "manual_followup":
        return "manual_followup"
    return ""


def blocked_count_value(raw: Any, *, minimum: int = 0) -> int:
    try:
        value = int(raw or 0)
    except Exception:
        value = 0
    return max(int(minimum), value)


def blocked_bucket_count(todos: Any, bucket: str, *, blocked_status: str = "blocked") -> int:
    token = blocked_bucket_label(bucket)
    if not token or not isinstance(todos, list):
        return 0
    count = 0
    blocked_token = str(blocked_status or "blocked").strip().lower() or "blocked"
    for todo in todos:
        if not isinstance(todo, dict):
            continue
        status = str(todo.get("status", "open")).strip().lower() or "open"
        if status != blocked_token:
            continue
        if blocked_bucket_label(todo.get("blocked_bucket", "")) == token:
            count += 1
    return count


def manual_followup_indices(rows: List[Dict[str, Any]], *, limit: int = 1, blocked_status: str = "blocked") -> List[int]:
    out: List[int] = []
    blocked_token = str(blocked_status or "blocked").strip().lower() or "blocked"
    for idx, row in enumerate(rows, start=1):
        if len(out) >= max(1, int(limit)):
            break
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", "open")).strip().lower() or "open"
        if status != blocked_token:
            continue
        if blocked_bucket_label(row.get("blocked_bucket", "")) != "manual_followup":
            continue
        out.append(idx)
    return out


def blocked_head_summary(
    todos: Any,
    *,
    blocked_status: str = "blocked",
    priority_rank: Callable[[Any], int] | None = None,
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(todos, list):
        return {}
    blocked_token = str(blocked_status or "blocked").strip().lower() or "blocked"
    for todo in todos:
        if not isinstance(todo, dict):
            continue
        status = str(todo.get("status", "open")).strip().lower() or "open"
        if status != blocked_token:
            continue
        row: Dict[str, Any] = {
            "id": str(todo.get("id", "")).strip() or "-",
            "summary": str(todo.get("summary", "")).strip(),
            "reason": blocked_reason_preview(todo.get("blocked_reason", "")),
            "count": max(1, blocked_count_value(todo.get("blocked_count", 0), minimum=1)),
            "bucket": blocked_bucket_label(todo.get("blocked_bucket", "")),
            "blocked_at": str(todo.get("blocked_at", "")).strip() or str(todo.get("updated_at", "")).strip(),
        }
        if callable(priority_rank):
            row["priority_rank"] = int(priority_rank(todo.get("priority", "P2")))
        rows.append(row)
    if not rows:
        return {}
    rows.sort(
        key=lambda r: (
            -int(r.get("count", 1) or 1),
            int(r.get("priority_rank", 0) or 0),
            str(r.get("blocked_at", "")),
            str(r.get("id", "")),
        )
    )
    return rows[0]


def clear_blocked_meta(item: Dict[str, Any], *, clear_current_request: bool = False) -> bool:
    had_followup = blocked_bucket_label(item.get("blocked_bucket", "")) == "manual_followup"
    item.pop("blocked_reason", None)
    item.pop("blocked_at", None)
    item.pop("blocked_request_id", None)
    item.pop("blocked_count", None)
    item.pop("blocked_bucket", None)
    item.pop("blocked_alerted_at", None)
    if clear_current_request:
        item.pop("current_request_id", None)
    return had_followup


def apply_todo_execution_outcome(
    item: Dict[str, Any],
    *,
    task_status: str,
    exec_verdict: str,
    exec_reason: str,
    req_id: str,
    now: str,
    task_label: str = "",
    manual_followup_threshold: int = 2,
) -> str:
    task_status_token = str(task_status or "").strip().lower()
    verdict_token = str(exec_verdict or "").strip().lower()
    reason = str(exec_reason or "").strip()

    is_success = task_status_token == "completed" and (not verdict_token or verdict_token == "success")
    is_blocked = verdict_token in {"retry", "fail"} or task_status_token == "failed"

    if is_success:
        item["status"] = "done"
        item["done_at"] = now
        item["done_request_id"] = str(req_id or "").strip()
        if task_label:
            item["done_task_label"] = str(task_label).strip()
        clear_blocked_meta(item, clear_current_request=False)
        item["updated_at"] = now
        return "done"

    if is_blocked:
        blocked_count = blocked_count_value(item.get("blocked_count", 0), minimum=0)
        item["status"] = "blocked"
        item["blocked_at"] = now
        item["blocked_request_id"] = str(req_id or "").strip()
        item["blocked_count"] = min(blocked_count + 1, 99)
        if int(item.get("blocked_count", 0) or 0) >= max(1, int(manual_followup_threshold or 1)):
            item["blocked_bucket"] = "manual_followup"
        else:
            item.pop("blocked_bucket", None)
            item.pop("blocked_alerted_at", None)
        if reason:
            item["blocked_reason"] = reason[:240]
        item["updated_at"] = now
        return "blocked"

    item["updated_at"] = now
    return ""
