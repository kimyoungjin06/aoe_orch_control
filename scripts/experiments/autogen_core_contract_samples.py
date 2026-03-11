#!/usr/bin/env python3
"""Shared sample contract fixtures for AutoGen Core TF experiments."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List

from aoe_tg_tf_event_schema import normalize_followup_proposals


def _clean_list(values: Iterable[Any]) -> List[str]:
    return [str(v).strip() for v in values if str(v).strip()]


def build_sample_followup_proposals(
    *,
    case_id: str,
    project_key: str,
    task_summary: str,
    roles: Iterable[str],
    workload_type: str = "",
    expected_focus: Iterable[Any] = (),
    source_request_id: str = "REQ-SAMPLE",
    source_todo_id: str = "",
) -> List[Dict[str, Any]]:
    roles_clean = _clean_list(roles)
    focus = {str(v).strip().lower() for v in expected_focus if str(v).strip()}
    workload = str(workload_type or "").strip().lower()
    proposals: List[Dict[str, Any]] = []

    if workload in {"synthesis"} or "followup_proposals" in focus:
        proposals.append(
            {
                "summary": f"[{project_key}] Convert synthesized findings into next-step experiment tasks",
                "priority": "P1",
                "kind": "followup",
                "reason": f"{case_id}: synthesis flow should surface concrete next steps from the analyzed notes",
                "source_request_id": source_request_id,
                "source_todo_id": source_todo_id,
                "confidence": 0.82,
            }
        )
    if workload in {"reporting"} or "manual_followup" in focus:
        proposals.append(
            {
                "summary": f"[{project_key}] Review human handoff items from generated report",
                "priority": "P2",
                "kind": "handoff",
                "reason": f"{case_id}: reporting flow should flag manual follow-up explicitly for the operator",
                "source_request_id": source_request_id,
                "source_todo_id": source_todo_id,
                "confidence": 0.76,
            }
        )
    if workload in {"write_candidate"} or "write_path" in focus:
        proposals.append(
            {
                "summary": f"[{project_key}] Verify sandbox write evidence before promoting the change",
                "priority": "P1",
                "kind": "risk",
                "reason": f"{case_id}: write-capable TF runs must leave behind a reviewer-visible promotion gate",
                "source_request_id": source_request_id,
                "source_todo_id": source_todo_id,
                "confidence": 0.79,
            }
        )
    if not proposals and "Reviewer" in roles_clean and ("risk_summary" in focus or workload in {"review"}):
        proposals = []

    return normalize_followup_proposals(
        proposals,
        default_source_request_id=source_request_id,
        default_source_todo_id=source_todo_id,
    )

