#!/usr/bin/env python3
"""Sync merge/prune helpers for scheduler."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from aoe_tg_blocked_state import clear_blocked_meta
from aoe_tg_sync_sources import _normalize_summary_key
from aoe_tg_todo_state import ensure_todo_store, find_existing_todo_by_summary, format_todo_id, normalize_priority

_STATUS_OPEN = "open"
_STATUS_BLOCKED = "blocked"
_STATUS_DONE = "done"
_STATUS_CANCELED = "canceled"


def _find_todo_by_id(todos: List[Dict[str, Any]], todo_id: str) -> Optional[Dict[str, Any]]:
    token = str(todo_id or "").strip().upper()
    if not token:
        return None
    for row in todos:
        if not isinstance(row, dict):
            continue
        if str(row.get("id", "")).strip().upper() == token:
            return row
    return None


def _sanitize_sync_counter_map(raw: Any, *, limit: int = 6) -> Dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    rows: List[tuple[str, int]] = []
    for key, value in raw.items():
        token = str(key or "").strip().lower()[:40]
        if not token:
            continue
        try:
            count = int(value or 0)
        except Exception:
            continue
        if count <= 0:
            continue
        rows.append((token, count))
    rows.sort(key=lambda kv: (-kv[1], kv[0]))
    return {key: count for key, count in rows[: max(1, int(limit or 6))]}


def stamp_sync_meta(
    entry: Dict[str, Any],
    *,
    at: str,
    mode: str,
    candidate_classes: Optional[Dict[str, int]] = None,
    candidate_doc_types: Optional[Dict[str, int]] = None,
) -> bool:
    if not isinstance(entry, dict):
        return False
    at_token = str(at or "").strip()[:40]
    mode_token = str(mode or "").strip()[:40]
    classes_token = _sanitize_sync_counter_map(candidate_classes)
    doc_types_token = _sanitize_sync_counter_map(candidate_doc_types)
    changed = False
    if str(entry.get("last_sync_at", "")).strip() != at_token:
        entry["last_sync_at"] = at_token
        changed = True
    if str(entry.get("last_sync_mode", "")).strip() != mode_token:
        entry["last_sync_mode"] = mode_token
        changed = True
    if dict(entry.get("last_sync_candidate_classes") or {}) != classes_token:
        entry["last_sync_candidate_classes"] = dict(classes_token)
        changed = True
    if dict(entry.get("last_sync_candidate_doc_types") or {}) != doc_types_token:
        entry["last_sync_candidate_doc_types"] = dict(doc_types_token)
        changed = True
    if changed and at_token:
        entry["updated_at"] = at_token
    return changed


def apply_scenario_items_to_entry(
    *,
    entry: Dict[str, Any],
    items: List[Dict[str, Any]],
    chat_id: str,
    now_iso: Callable[[], str],
    dry_run: bool,
    source_mode: str = "",
    sources: Optional[List[str]] = None,
    prune_missing: bool = False,
) -> Dict[str, int]:
    todos, seq = ensure_todo_store(entry)

    counts = {
        "parsed": 0,
        "added": 0,
        "updated": 0,
        "done": 0,
        "pruned": 0,
        "skipped_done_missing": 0,
    }
    now = now_iso()
    changed = False
    touched_ids: set[str] = set()
    touched_summaries: set[str] = set()
    active_groups: set[str] = set()
    sync_mode = str(source_mode or "").strip()[:40]
    sync_sources = [str(src or "").strip()[:240] for src in list(sources or []) if str(src or "").strip()]

    for item in items:
        if not isinstance(item, dict):
            continue
        counts["parsed"] += 1
        todo_id = str(item.get("id", "")).strip().upper()
        pr = normalize_priority(str(item.get("priority", "P2")))
        status = str(item.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
        if status not in {_STATUS_OPEN, _STATUS_DONE}:
            status = _STATUS_OPEN
        summary = str(item.get("summary", "")).strip()
        if not summary:
            continue

        row: Optional[Dict[str, Any]] = None
        if todo_id:
            row = _find_todo_by_id(todos, todo_id)
        if row is None:
            row = find_existing_todo_by_summary(todos, summary)

        if row is None:
            if status == _STATUS_DONE:
                counts["skipped_done_missing"] += 1
                continue
            seq = max(0, int(entry.get("todo_seq", seq) or 0))
            seq += 1
            todo_id = format_todo_id(seq)
            entry["todo_seq"] = seq
            new_row: Dict[str, Any] = {
                "id": todo_id,
                "summary": summary[:600],
                "priority": pr,
                "status": _STATUS_OPEN,
                "created_at": now,
                "updated_at": now,
                "created_by": f"sync:telegram:{chat_id}",
                "sync_managed": True,
                "sync_mode": sync_mode,
                "sync_sources": sync_sources[:8],
                "sync_last_seen_at": now,
                "sync_group": str(item.get("sync_group", "")).strip()[:80],
                "sync_source_class": str(item.get("sync_source_class", "")).strip()[:80],
                "sync_confidence": float(item.get("sync_confidence", 0.0) or 0.0),
                "sync_source_file": str(item.get("source_file", "")).strip()[:240],
                "sync_source_section": str(item.get("source_section", "")).strip()[:160],
                "sync_source_reason": str(item.get("source_reason", "")).strip()[:80],
                "sync_source_line": max(0, int(item.get("source_line", 0) or 0)),
            }
            todos.append(new_row)
            touched_ids.add(todo_id)
            touched_summaries.add(_normalize_summary_key(summary))
            if str(new_row.get("sync_group", "")).strip():
                active_groups.add(str(new_row.get("sync_group", "")).strip())
            counts["added"] += 1
            changed = True
            continue

        row_changed = False
        row["sync_managed"] = True
        if sync_mode and str(row.get("sync_mode", "")).strip() != sync_mode:
            row["sync_mode"] = sync_mode
            row_changed = True
        if sync_sources and list(row.get("sync_sources") or []) != sync_sources[:8]:
            row["sync_sources"] = sync_sources[:8]
            row_changed = True
        if str(row.get("sync_last_seen_at", "")).strip() != now:
            row["sync_last_seen_at"] = now
            row_changed = True
        sync_group = str(item.get("sync_group", "")).strip()[:80]
        if sync_group and str(row.get("sync_group", "")).strip() != sync_group:
            row["sync_group"] = sync_group
            row_changed = True
        sync_source_class = str(item.get("sync_source_class", "")).strip()[:80]
        if sync_source_class and str(row.get("sync_source_class", "")).strip() != sync_source_class:
            row["sync_source_class"] = sync_source_class
            row_changed = True
        sync_source_file = str(item.get("source_file", "")).strip()[:240]
        if sync_source_file and str(row.get("sync_source_file", "")).strip() != sync_source_file:
            row["sync_source_file"] = sync_source_file
            row_changed = True
        sync_source_section = str(item.get("source_section", "")).strip()[:160]
        if sync_source_section and str(row.get("sync_source_section", "")).strip() != sync_source_section:
            row["sync_source_section"] = sync_source_section
            row_changed = True
        sync_source_reason = str(item.get("source_reason", "")).strip()[:80]
        if sync_source_reason and str(row.get("sync_source_reason", "")).strip() != sync_source_reason:
            row["sync_source_reason"] = sync_source_reason
            row_changed = True
        try:
            source_line = max(0, int(item.get("source_line", 0) or 0))
        except Exception:
            source_line = 0
        if source_line and int(row.get("sync_source_line", 0) or 0) != source_line:
            row["sync_source_line"] = source_line
            row_changed = True
        try:
            confidence = float(item.get("sync_confidence", 0.0) or 0.0)
        except Exception:
            confidence = 0.0
        if float(row.get("sync_confidence", 0.0) or 0.0) != confidence:
            row["sync_confidence"] = confidence
            row_changed = True
        if summary and str(row.get("summary", "")).strip() != summary:
            row["summary"] = summary[:600]
            row_changed = True
        if pr and str(row.get("priority", "P2")).strip().upper() != pr:
            row["priority"] = pr
            row_changed = True
        touched_ids.add(str(row.get("id", "")).strip().upper())
        touched_summaries.add(_normalize_summary_key(summary))
        if str(row.get("sync_group", "")).strip():
            active_groups.add(str(row.get("sync_group", "")).strip())

        current_status = str(row.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
        if status != current_status:
            row["status"] = status
            row_changed = True
            if status == _STATUS_DONE:
                row["done_at"] = now
                row["done_by"] = f"sync:telegram:{chat_id}"
                clear_blocked_meta(row, clear_current_request=True)
                counts["done"] += 1
            elif status == _STATUS_OPEN:
                row.pop("done_at", None)
                row.pop("done_by", None)
                clear_blocked_meta(row, clear_current_request=False)

        if row_changed:
            row["updated_at"] = now
            counts["updated"] += 1
            changed = True

    if prune_missing:
        pending = entry.get("pending_todo")
        pending_id = str(pending.get("todo_id", "")).strip().upper() if isinstance(pending, dict) else ""
        for row in todos:
            if not isinstance(row, dict):
                continue
            row_id = str(row.get("id", "")).strip().upper()
            if not row_id or row_id in touched_ids:
                continue
            summary_key = _normalize_summary_key(str(row.get("summary", "")))
            if summary_key and summary_key in touched_summaries:
                continue
            status = str(row.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
            if status not in {_STATUS_OPEN, _STATUS_BLOCKED}:
                continue
            sync_managed = bool(row.get("sync_managed")) or str(row.get("created_by", "")).startswith("sync:telegram:")
            if not sync_managed:
                continue
            row_group = str(row.get("sync_group", "")).strip()
            if active_groups and row_group and row_group not in active_groups:
                continue
            if str(row.get("current_request_id", "")).strip():
                continue
            row["status"] = _STATUS_CANCELED
            row["canceled_at"] = now
            row["canceled_by"] = f"sync:telegram:{chat_id}"
            row["canceled_reason"] = "sync_prune_missing"
            clear_blocked_meta(row, clear_current_request=False)
            row["updated_at"] = now
            counts["pruned"] += 1
            changed = True
            if pending_id and row_id == pending_id:
                entry.pop("pending_todo", None)
                pending_id = ""

    if changed:
        entry["updated_at"] = now
        if dry_run:
            pass
    return counts
