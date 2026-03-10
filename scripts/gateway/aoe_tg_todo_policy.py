#!/usr/bin/env python3
"""Shared todo/proposal policy helpers."""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List

_PRIORITIES = {"P1", "P2", "P3"}
_STATUS_OPEN = "open"
_STATUS_RUNNING = "running"
_STATUS_BLOCKED = "blocked"
_STATUS_DONE = "done"
_PROPOSAL_STATUS_OPEN = "open"
_PROPOSAL_STATUS_ACCEPTED = "accepted"
_PROPOSAL_STATUS_REJECTED = "rejected"
_PROPOSAL_KINDS = {"followup", "risk", "debt", "handoff"}


def normalize_priority(token: Any) -> str:
    raw = str(token or "").strip().upper()
    if raw in _PRIORITIES:
        return raw
    return "P2"


def priority_rank(priority: Any) -> int:
    p = normalize_priority(priority)
    return {"P1": 1, "P2": 2, "P3": 3}.get(p, 9)


def normalize_proposal_priority(token: Any) -> str:
    return normalize_priority(token)


def normalize_proposal_kind(token: Any) -> str:
    raw = str(token or "").strip().lower()
    if raw in _PROPOSAL_KINDS:
        return raw
    return "followup"


def normalize_proposal_status(token: Any) -> str:
    raw = str(token or "").strip().lower()
    if raw in {_PROPOSAL_STATUS_OPEN, _PROPOSAL_STATUS_ACCEPTED, _PROPOSAL_STATUS_REJECTED}:
        return raw
    return _PROPOSAL_STATUS_OPEN


def proposal_confidence(token: Any) -> float:
    try:
        value = float(token)
    except Exception:
        return 0.0
    return max(0.0, min(1.0, value))


def proposal_summary_key(text: Any) -> str:
    raw = " ".join(str(text or "").strip().split()).lower()
    raw = re.sub(r"^(?:p[1-3]\s*[:\-]\s*|p[1-3]\s+)", "", raw, flags=re.IGNORECASE)
    return raw[:240]


def todo_line_summary(raw_line: str) -> str:
    stripped = str(raw_line or "").strip()
    match = re.match(r"^\s*[-*]\s*\[(?P<chk>[ xX])\]\s*(?P<rest>.+)$", stripped)
    if not match:
        return ""
    rest = str(match.group("rest") or "").strip()
    priority_match = re.match(r"^(P[1-3])(?:\s*[:|-]\s*|\s+)(.+)$", rest, flags=re.IGNORECASE)
    if priority_match:
        rest = str(priority_match.group(2) or "").strip()
    return proposal_summary_key(rest)


def format_canonical_todo_line(priority: Any, summary: Any, *, status: str) -> str:
    chk = "x" if str(status or "").strip().lower() == _STATUS_DONE else " "
    pr = normalize_priority(priority)
    text = " ".join(str(summary or "").strip().split())
    return f"- [{chk}] {pr}: {text}"


def todo_row_syncback_target_status(row: Dict[str, Any]) -> str:
    status = str((row or {}).get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
    if status == _STATUS_DONE:
        return _STATUS_DONE
    if status in {_STATUS_OPEN, _STATUS_RUNNING, _STATUS_BLOCKED}:
        return _STATUS_OPEN
    return ""


def todo_row_syncback_appendable(row: Dict[str, Any]) -> bool:
    created_by = str((row or {}).get("created_by", "")).strip().lower()
    return bool(
        str((row or {}).get("proposal_id", "")).strip()
        or created_by.startswith("telegram:")
        or created_by == "tf-proposal"
        or str((row or {}).get("created_from_request_id", "")).strip()
    )


def accepted_proposals_for_syncback(proposals: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in proposals:
        if not isinstance(row, dict):
            continue
        if normalize_proposal_status(row.get("status")) == _PROPOSAL_STATUS_ACCEPTED:
            out.append(row)
    return out


def proposal_to_todo_row(proposal: Dict[str, Any], *, todo_id: str, now: str) -> Dict[str, Any]:
    summary = str(proposal.get("summary", "")).strip()
    row: Dict[str, Any] = {
        "id": str(todo_id or "").strip()[:32],
        "summary": summary[:600],
        "priority": normalize_priority(str(proposal.get("priority", "P2"))),
        "status": _STATUS_OPEN,
        "created_at": now,
        "updated_at": now,
        "created_by": "tf-proposal",
        "proposal_id": str(proposal.get("id", "")).strip()[:32],
        "proposal_kind": normalize_proposal_kind(proposal.get("kind", "followup"))[:32],
    }
    src_req = str(proposal.get("source_request_id", "")).strip()
    src_todo = str(proposal.get("source_todo_id", "")).strip()
    if src_req:
        row["created_from_request_id"] = src_req[:128]
    if src_todo:
        row["created_from_todo_id"] = src_todo[:64]
    source_file = str(proposal.get("source_file", "")).strip()
    source_section = str(proposal.get("source_section", "")).strip()
    source_reason = str(proposal.get("source_reason", "")).strip()
    if source_file:
        row["source_file"] = source_file[:240]
    if source_section:
        row["source_section"] = source_section[:160]
    if source_reason:
        row["source_reason"] = source_reason[:80]
    try:
        source_line = int(proposal.get("source_line", 0) or 0)
    except Exception:
        source_line = 0
    if source_line > 0:
        row["source_line"] = source_line
    return row
