#!/usr/bin/env python3
"""Normalized runtime event schema for experimental TF backends."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional


RUNTIME_EVENT_REQUIRED_FIELDS = (
    "seq",
    "ts",
    "backend",
    "source",
    "stage",
    "kind",
    "status",
    "summary",
    "payload",
)

RUNTIME_EVENT_KINDS = (
    "lifecycle",
    "dispatch",
    "artifact",
    "verdict",
    "proposal",
    "error",
)

RUNTIME_EVENT_STATUSES = (
    "info",
    "success",
    "warning",
    "error",
)

RUNTIME_EVENT_STAGES = (
    "request.accepted",
    "roles.resolved",
    "workers.ready",
    "dispatch.submitted",
    "runtime.started",
    "runtime.completed",
    "verdict.emitted",
    "proposals.emitted",
    "error.raised",
)

FOLLOWUP_PROPOSAL_REQUIRED_FIELDS = (
    "summary",
    "priority",
    "kind",
    "reason",
    "source_request_id",
    "source_todo_id",
    "confidence",
)

FOLLOWUP_PROPOSAL_PRIORITIES = ("P1", "P2", "P3")
FOLLOWUP_PROPOSAL_KINDS = ("followup", "handoff", "risk", "debt")


def tf_runtime_event_schema() -> Dict[str, Any]:
    return {
        "required_fields": list(RUNTIME_EVENT_REQUIRED_FIELDS),
        "allowed_kinds": list(RUNTIME_EVENT_KINDS),
        "allowed_status": list(RUNTIME_EVENT_STATUSES),
        "recommended_stages": list(RUNTIME_EVENT_STAGES),
        "notes": [
            "backends may emit additional stages, but the envelope must stay stable",
            "payload is backend-specific evidence and must remain JSON-serializable",
            "backlog mutation is outside the backend; proposals are advisory output only",
        ],
    }


def normalize_runtime_event(
    raw: Dict[str, Any],
    *,
    seq: int,
    default_backend: str,
    default_source: str,
    now_iso: Callable[[], str],
) -> Dict[str, Any]:
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    kind = str(raw.get("kind", "lifecycle") or "lifecycle").strip().lower()
    if kind not in RUNTIME_EVENT_KINDS:
        kind = "lifecycle"
    status = str(raw.get("status", "info") or "info").strip().lower()
    if status not in RUNTIME_EVENT_STATUSES:
        status = "info"
    stage = str(raw.get("stage", "runtime.completed") or "runtime.completed").strip().lower()
    if not stage:
        stage = "runtime.completed"
    source = str(raw.get("source", "") or "").strip() or default_source
    backend = str(raw.get("backend", "") or "").strip() or default_backend
    summary = str(raw.get("summary", "") or "").strip()
    if not summary:
        summary = stage.replace(".", " ")
    ts = str(raw.get("ts", "") or "").strip() or now_iso()
    return {
        "seq": max(1, int(seq)),
        "ts": ts,
        "backend": backend,
        "source": source,
        "stage": stage,
        "kind": kind,
        "status": status,
        "summary": summary,
        "payload": payload,
    }


def normalize_runtime_events(
    rows: List[Dict[str, Any]],
    *,
    default_backend: str,
    default_source: str,
    now_iso: Callable[[], str],
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        if not isinstance(row, dict):
            continue
        normalized.append(
            normalize_runtime_event(
                row,
                seq=idx,
                default_backend=default_backend,
                default_source=default_source,
                now_iso=now_iso,
            )
        )
    return normalized


def validate_runtime_event(row: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(row, dict):
        return ["event is not an object"]
    for field in RUNTIME_EVENT_REQUIRED_FIELDS:
        if field not in row:
            errors.append(f"missing:{field}")
    if errors:
        return errors
    try:
        if int(row.get("seq", 0)) <= 0:
            errors.append("invalid:seq")
    except Exception:
        errors.append("invalid:seq")
    if not str(row.get("ts", "") or "").strip():
        errors.append("invalid:ts")
    if str(row.get("kind", "") or "").strip() not in RUNTIME_EVENT_KINDS:
        errors.append("invalid:kind")
    if str(row.get("status", "") or "").strip() not in RUNTIME_EVENT_STATUSES:
        errors.append("invalid:status")
    if not str(row.get("stage", "") or "").strip():
        errors.append("invalid:stage")
    if not str(row.get("backend", "") or "").strip():
        errors.append("invalid:backend")
    if not str(row.get("source", "") or "").strip():
        errors.append("invalid:source")
    if not str(row.get("summary", "") or "").strip():
        errors.append("invalid:summary")
    if not isinstance(row.get("payload"), dict):
        errors.append("invalid:payload")
    return errors


def validate_runtime_events(rows: List[Dict[str, Any]]) -> List[List[str]]:
    return [validate_runtime_event(row) for row in rows]


def tf_followup_proposal_schema() -> Dict[str, Any]:
    return {
        "required_fields": list(FOLLOWUP_PROPOSAL_REQUIRED_FIELDS),
        "allowed_priorities": list(FOLLOWUP_PROPOSAL_PRIORITIES),
        "allowed_kinds": list(FOLLOWUP_PROPOSAL_KINDS),
        "notes": [
            "follow-up proposals are advisory output only",
            "backlog mutation remains in repo-owned todo state modules",
        ],
    }


def normalize_followup_proposal(
    raw: Dict[str, Any],
    *,
    default_source_request_id: str,
    default_source_todo_id: str = "",
) -> Dict[str, Any]:
    priority = str(raw.get("priority", "P2") or "P2").strip().upper()
    if priority not in FOLLOWUP_PROPOSAL_PRIORITIES:
        priority = "P2"
    kind = str(raw.get("kind", "followup") or "followup").strip().lower()
    if kind not in FOLLOWUP_PROPOSAL_KINDS:
        kind = "followup"
    summary = str(raw.get("summary", "") or "").strip()
    reason = str(raw.get("reason", "") or "").strip()
    try:
        confidence = float(raw.get("confidence", 0.7) or 0.7)
    except Exception:
        confidence = 0.7
    confidence = max(0.0, min(1.0, confidence))
    return {
        "summary": summary,
        "priority": priority,
        "kind": kind,
        "reason": reason,
        "source_request_id": str(raw.get("source_request_id", "") or "").strip() or default_source_request_id,
        "source_todo_id": str(raw.get("source_todo_id", "") or "").strip() or default_source_todo_id,
        "confidence": confidence,
    }


def normalize_followup_proposals(
    rows: List[Dict[str, Any]],
    *,
    default_source_request_id: str,
    default_source_todo_id: str = "",
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        normalized.append(
            normalize_followup_proposal(
                row,
                default_source_request_id=default_source_request_id,
                default_source_todo_id=default_source_todo_id,
            )
        )
    return normalized


def validate_followup_proposal(row: Dict[str, Any]) -> List[str]:
    errors: List[str] = []
    if not isinstance(row, dict):
        return ["proposal is not an object"]
    for field in FOLLOWUP_PROPOSAL_REQUIRED_FIELDS:
        if field not in row:
            errors.append(f"missing:{field}")
    if errors:
        return errors
    if not str(row.get("summary", "") or "").strip():
        errors.append("invalid:summary")
    if str(row.get("priority", "") or "").strip() not in FOLLOWUP_PROPOSAL_PRIORITIES:
        errors.append("invalid:priority")
    if str(row.get("kind", "") or "").strip() not in FOLLOWUP_PROPOSAL_KINDS:
        errors.append("invalid:kind")
    if not str(row.get("reason", "") or "").strip():
        errors.append("invalid:reason")
    if not str(row.get("source_request_id", "") or "").strip():
        errors.append("invalid:source_request_id")
    try:
        confidence = float(row.get("confidence", 0.0))
        if confidence < 0.0 or confidence > 1.0:
            errors.append("invalid:confidence")
    except Exception:
        errors.append("invalid:confidence")
    return errors


def validate_followup_proposals(rows: List[Dict[str, Any]]) -> List[List[str]]:
    return [validate_followup_proposal(row) for row in rows]
