#!/usr/bin/env python3
"""Todo proposal store and canonical TODO syncback helpers."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from aoe_tg_blocked_state import blocked_bucket_label, blocked_reason_preview
from aoe_tg_todo_policy import (
    accepted_proposals_for_syncback,
    format_canonical_todo_line,
    normalize_priority,
    normalize_proposal_kind,
    normalize_proposal_priority,
    normalize_proposal_status,
    priority_rank,
    proposal_confidence,
    proposal_summary_key,
    proposal_to_todo_row,
    todo_line_summary,
    todo_row_syncback_appendable,
    todo_row_syncback_target_status,
)

_STATUS_OPEN = "open"
_STATUS_RUNNING = "running"
_STATUS_BLOCKED = "blocked"
_STATUS_DONE = "done"
_STATUS_CANCELED = "canceled"
_PROPOSAL_STATUS_OPEN = "open"
_PROPOSAL_STATUS_ACCEPTED = "accepted"
_PROPOSAL_STATUS_REJECTED = "rejected"
_SYNCBACK_BEGIN = "<!-- AOE_SYNCBACK_NOTES_BEGIN -->"
_SYNCBACK_END = "<!-- AOE_SYNCBACK_NOTES_END -->"


def format_todo_id(seq: int) -> str:
    value = max(1, int(seq))
    return f"TODO-{value:03d}" if value < 1000 else f"TODO-{value}"


def format_proposal_id(seq: int) -> str:
    value = max(1, int(seq))
    return f"PROP-{value:03d}" if value < 1000 else f"PROP-{value}"


def parse_seq_from_todo_id(todo_id: str) -> int:
    token = str(todo_id or "").strip().upper()
    tail = token[5:] if token.startswith("TODO-") else token
    return int(tail) if tail.isdigit() else 0


def parse_seq_from_proposal_id(proposal_id: str) -> int:
    token = str(proposal_id or "").strip().upper()
    tail = token[5:] if token.startswith("PROP-") else token
    return int(tail) if tail.isdigit() else 0


def ensure_todo_store(entry: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    raw = entry.get("todos")
    todos: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        for row in raw:
            if isinstance(row, dict):
                todos.append(row)
    entry["todos"] = todos

    raw_seq = entry.get("todo_seq")
    try:
        seq = max(0, int(raw_seq or 0))
    except Exception:
        seq = 0
    if not seq and todos:
        for row in todos:
            seq = max(seq, parse_seq_from_todo_id(str(row.get("id", ""))))
    entry["todo_seq"] = seq
    return todos, seq


def ensure_todo_proposal_store(entry: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    raw = entry.get("todo_proposals")
    proposals: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        for row in raw:
            if isinstance(row, dict):
                proposals.append(row)
    entry["todo_proposals"] = proposals

    raw_seq = entry.get("todo_proposal_seq")
    try:
        seq = max(0, int(raw_seq or 0))
    except Exception:
        seq = 0
    if not seq and proposals:
        for row in proposals:
            seq = max(seq, parse_seq_from_proposal_id(str(row.get("id", ""))))
    entry["todo_proposal_seq"] = seq
    return proposals, seq


def sorted_open_proposals(proposals: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for row in proposals:
        if not isinstance(row, dict):
            continue
        status = normalize_proposal_status(row.get("status", _PROPOSAL_STATUS_OPEN))
        if status != _PROPOSAL_STATUS_OPEN:
            continue
        rows.append(row)
    rows.sort(
        key=lambda r: (
            priority_rank(str(r.get("priority", "P2"))),
            -float(r.get("confidence", 0.0) or 0.0),
            str(r.get("created_at", "")),
            str(r.get("id", "")),
        )
    )
    return rows


def find_proposal_by_ref(proposals: List[Dict[str, Any]], ref: str) -> Optional[Dict[str, Any]]:
    token = str(ref or "").strip()
    if not token:
        return None

    upper = token.upper()
    for row in proposals:
        if not isinstance(row, dict):
            continue
        if str(row.get("id", "")).strip().upper() == upper:
            return row

    if token.isdigit():
        idx = int(token)
        open_rows = sorted_open_proposals(proposals)
        if 1 <= idx <= len(open_rows):
            return open_rows[idx - 1]

        candidate = format_proposal_id(idx)
        for row in proposals:
            if not isinstance(row, dict):
                continue
            if str(row.get("id", "")).strip().upper() == candidate:
                return row

    return None


def find_existing_todo_by_summary(todos: List[Dict[str, Any]], summary: str) -> Optional[Dict[str, Any]]:
    key = proposal_summary_key(summary)
    if not key:
        return None
    for row in todos:
        if not isinstance(row, dict):
            continue
        if proposal_summary_key(row.get("summary", "")) == key:
            return row
    return None


def canonical_todo_path(entry: Dict[str, Any]) -> Path:
    root = Path(str(entry.get("project_root", "")).strip() or ".").expanduser()
    return (root / "TODO.md").resolve()


def syncback_note_lines(todos: List[Dict[str, Any]], *, now: str) -> List[str]:
    blocked_rows = []
    for row in todos:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
        if status != _STATUS_BLOCKED:
            continue
        blocked_rows.append(row)
    if not blocked_rows:
        return []

    notes: List[str] = [
        "## Syncback Notes (ignored by /sync)",
        _SYNCBACK_BEGIN,
        f"# updated: {now}",
    ]
    blocked_rows.sort(
        key=lambda row: (
            priority_rank(str(row.get("priority", "P2"))),
            str(row.get("updated_at", "")),
            str(row.get("id", "")),
        )
    )
    for row in blocked_rows[:20]:
        todo_id = str(row.get("id", "")).strip() or "-"
        bucket = blocked_bucket_label(row.get("blocked_bucket", ""))
        reason = blocked_reason_preview(row.get("blocked_reason", ""), 120)
        summary = " ".join(str(row.get("summary", "")).strip().split())
        parts = [f"# blocked: {todo_id}"]
        if bucket:
            parts.append(f"[{bucket}]")
        if summary:
            parts.append(f"| {summary}")
        if reason:
            parts.append(f"| {reason}")
        notes.append(" ".join(parts))
    notes.append(_SYNCBACK_END)
    return notes


def find_syncback_note_block(lines: List[str]) -> Tuple[int, int]:
    start = -1
    end = -1
    for idx, line in enumerate(lines):
        if str(line).strip() == _SYNCBACK_BEGIN:
            start = idx
            break
    if start >= 0:
        for idx in range(start + 1, len(lines)):
            if str(lines[idx]).strip() == _SYNCBACK_END:
                end = idx
                break
    if start >= 1 and str(lines[start - 1]).strip() == "## Syncback Notes (ignored by /sync)":
        start -= 1
    if start >= 0 and end >= 0:
        return start, end
    return -1, -1


def _task_insert_index(lines: List[str]) -> int:
    task_heading = re.compile(r"^\s{0,3}#{1,6}\s+(tasks?|todo)\b", flags=re.IGNORECASE)
    next_heading = re.compile(r"^\s{0,3}#{1,6}\s+.+$")
    saw = False
    insert_at = len(lines)
    for idx, line in enumerate(lines):
        stripped = str(line or "").strip()
        if task_heading.match(stripped):
            saw = True
            insert_at = idx + 1
            continue
        if saw and next_heading.match(stripped):
            return idx
        if saw:
            insert_at = idx + 1
    return insert_at


def preview_syncback_plan(entry: Dict[str, Any]) -> Dict[str, Any]:
    canonical = canonical_todo_path(entry)
    if not canonical.exists():
        raise RuntimeError(f"canonical TODO.md not found: {canonical}")

    text = canonical.read_text(encoding="utf-8")
    lines = text.splitlines()
    todos, _seq = ensure_todo_store(entry)
    proposals, _proposal_seq = ensure_todo_proposal_store(entry)

    line_indices: Dict[str, int] = {}
    for idx, line in enumerate(lines):
        key = todo_line_summary(line)
        if key and key not in line_indices:
            line_indices[key] = idx

    updates: List[Tuple[int, str]] = []
    append_lines: List[str] = []
    note_lines = syncback_note_lines(todos, now=str(entry.get("updated_at", "")).strip() or "-")
    seen_append: set[str] = set()
    done_count = 0
    reopen_count = 0
    appended_count = 0

    for row in todos:
        if not isinstance(row, dict):
            continue
        target_status = todo_row_syncback_target_status(row)
        if not target_status:
            continue
        summary = " ".join(str(row.get("summary", "")).strip().split())
        if not summary:
            continue
        key = proposal_summary_key(summary)
        formatted = format_canonical_todo_line(str(row.get("priority", "P2")), summary, status=target_status)
        if key in line_indices:
            idx = line_indices[key]
            if str(lines[idx]).rstrip() != formatted:
                updates.append((idx, formatted))
                if target_status == _STATUS_DONE:
                    done_count += 1
                else:
                    reopen_count += 1
            continue
        if todo_row_syncback_appendable(row):
            if key not in seen_append:
                append_lines.append(formatted)
                seen_append.add(key)
                appended_count += 1

    accepted_rows = accepted_proposals_for_syncback(proposals)
    for row in accepted_rows:
        summary = " ".join(str(row.get("summary", "")).strip().split())
        key = proposal_summary_key(summary)
        if not summary or key in line_indices or key in seen_append:
            continue
        append_lines.append(format_canonical_todo_line(str(row.get("priority", "P2")), summary, status=_STATUS_OPEN))
        seen_append.add(key)
        appended_count += 1

    return {
        "path": canonical,
        "lines": lines,
        "updates": updates,
        "append_lines": append_lines,
        "notes": note_lines,
        "done_count": done_count,
        "reopen_count": reopen_count,
        "append_count": appended_count,
        "blocked_count": sum(
            1 for row in todos if isinstance(row, dict) and str(row.get("status", _STATUS_OPEN)).strip().lower() == _STATUS_BLOCKED
        ),
    }


def apply_syncback_plan(plan: Dict[str, Any]) -> Dict[str, Any]:
    lines = list(plan.get("lines") or [])
    for idx, new_line in sorted(list(plan.get("updates") or []), key=lambda item: int(item[0]), reverse=True):
        if 0 <= int(idx) < len(lines):
            lines[int(idx)] = str(new_line)

    append_lines = [str(line) for line in list(plan.get("append_lines") or []) if str(line).strip()]
    if append_lines:
        insert_at = _task_insert_index(lines)
        prefix: List[str] = []
        if insert_at > 0 and str(lines[insert_at - 1]).strip():
            prefix.append("")
        prefix.extend(append_lines)
        lines[insert_at:insert_at] = prefix

    start, end = find_syncback_note_block(lines)
    note_lines = list(plan.get("notes") or [])
    if start >= 0 and end >= start:
        lines[start : end + 1] = []
        while start < len(lines) and not str(lines[start]).strip():
            del lines[start]
    if note_lines:
        if lines and str(lines[-1]).strip():
            lines.append("")
        lines.extend(note_lines)

    text = "\n".join(lines).rstrip() + "\n"
    path = Path(plan["path"])
    path.write_text(text, encoding="utf-8")
    return {"path": path, "line_count": len(lines)}


def merge_todo_proposals(
    *,
    entry: Dict[str, Any],
    request_id: str,
    task: Optional[Dict[str, Any]],
    source_todo_id: str,
    proposals_data: List[Dict[str, Any]],
    now_iso: Callable[[], str],
) -> Dict[str, Any]:
    proposals, seq = ensure_todo_proposal_store(entry)
    todos, _todo_seq = ensure_todo_store(entry)
    source_request_id = str(request_id or "").strip()
    source_todo = str(source_todo_id or "").strip()
    task_label = ""
    if isinstance(task, dict):
        task_label = str(task.get("alias", "")).strip() or str(task.get("short_id", "")).strip() or source_request_id

    existing_keys = {
        (
            str(row.get("source_request_id", "")).strip(),
            proposal_summary_key(row.get("summary", "")),
        )
        for row in proposals
        if isinstance(row, dict)
    }
    existing_open_or_accepted_summary_keys = {
        proposal_summary_key(row.get("summary", ""))
        for row in proposals
        if isinstance(row, dict)
        and normalize_proposal_status(row.get("status", _PROPOSAL_STATUS_OPEN)) in {_PROPOSAL_STATUS_OPEN, _PROPOSAL_STATUS_ACCEPTED}
    }
    existing_todo_keys = {
        proposal_summary_key(row.get("summary", ""))
        for row in todos
        if isinstance(row, dict)
    }

    created: List[str] = []
    duplicates = 0
    skipped = 0
    now = now_iso()
    for raw in proposals_data:
        if not isinstance(raw, dict):
            continue
        summary = " ".join(str(raw.get("summary", "")).strip().split())
        if not summary:
            skipped += 1
            continue
        summary_key = proposal_summary_key(summary)
        if not summary_key:
            skipped += 1
            continue
        dedupe_key = (source_request_id, summary_key)
        if dedupe_key in existing_keys or summary_key in existing_todo_keys or summary_key in existing_open_or_accepted_summary_keys:
            duplicates += 1
            continue
        seq += 1
        proposal_id = format_proposal_id(seq)
        row: Dict[str, Any] = {
            "id": proposal_id,
            "summary": summary[:600],
            "priority": normalize_proposal_priority(raw.get("priority", "P2")),
            "kind": normalize_proposal_kind(raw.get("kind", "followup")),
            "status": _PROPOSAL_STATUS_OPEN,
            "reason": str(raw.get("reason", "")).strip()[:240],
            "confidence": proposal_confidence(raw.get("confidence", 0.0)),
            "source_request_id": source_request_id[:128],
            "source_todo_id": source_todo[:64],
            "source_task_label": task_label[:120],
            "created_at": now,
            "updated_at": now,
            "created_by": str(raw.get("created_by", "tf")).strip()[:40] or "tf",
        }
        source_file = str(raw.get("source_file", "")).strip()
        source_section = str(raw.get("source_section", "")).strip()
        source_reason = str(raw.get("source_reason", "")).strip()
        if source_file:
            row["source_file"] = source_file[:240]
        if source_section:
            row["source_section"] = source_section[:160]
        if source_reason:
            row["source_reason"] = source_reason[:80]
        try:
            source_line = int(raw.get("source_line", 0) or 0)
        except Exception:
            source_line = 0
        if source_line > 0:
            row["source_line"] = source_line
        proposals.append(row)
        existing_keys.add(dedupe_key)
        existing_open_or_accepted_summary_keys.add(summary_key)
        created.append(proposal_id)

    entry["todo_proposal_seq"] = seq
    if created:
        entry["updated_at"] = now

    return {
        "created_ids": created,
        "created_count": len(created),
        "duplicate_count": duplicates,
        "skipped_count": skipped,
    }


def accept_todo_proposal(
    *,
    entry: Dict[str, Any],
    proposal: Dict[str, Any],
    actor: str,
    now: str,
) -> Dict[str, Any]:
    todos, seq = ensure_todo_store(entry)
    summary = str(proposal.get("summary", "")).strip()
    existing = find_existing_todo_by_summary(todos, summary)
    accepted_todo_id = ""
    created_new = False

    if isinstance(existing, dict):
        accepted_todo_id = str(existing.get("id", "")).strip()
    else:
        seq = max(0, int(entry.get("todo_seq", seq) or 0))
        seq += 1
        accepted_todo_id = format_todo_id(seq)
        entry["todo_seq"] = seq
        todo_row = proposal_to_todo_row(proposal, todo_id=accepted_todo_id, now=now)
        todos.append(todo_row)
        created_new = True

    proposal["status"] = _PROPOSAL_STATUS_ACCEPTED
    proposal["accepted_at"] = now
    proposal["accepted_by"] = str(actor or "").strip()[:64]
    proposal["accepted_todo_id"] = accepted_todo_id[:32]
    proposal["updated_at"] = now
    entry["updated_at"] = now
    return {
        "proposal_id": str(proposal.get("id", "")).strip(),
        "todo_id": accepted_todo_id,
        "created_new": created_new,
        "summary": summary,
    }


def reject_todo_proposal(
    *,
    entry: Dict[str, Any],
    proposal: Dict[str, Any],
    actor: str,
    now: str,
    reason: str = "",
) -> Dict[str, Any]:
    clean_reason = str(reason or "").strip()
    proposal["status"] = _PROPOSAL_STATUS_REJECTED
    proposal["rejected_at"] = now
    proposal["rejected_by"] = str(actor or "").strip()[:64]
    proposal["updated_at"] = now
    if clean_reason:
        proposal["rejected_reason"] = clean_reason[:240]
    else:
        proposal.pop("rejected_reason", None)
    entry["updated_at"] = now
    return {
        "proposal_id": str(proposal.get("id", "")).strip(),
        "summary": str(proposal.get("summary", "")).strip(),
        "reason": clean_reason[:240],
    }
