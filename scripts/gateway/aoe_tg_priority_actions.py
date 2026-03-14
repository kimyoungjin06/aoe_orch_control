#!/usr/bin/env python3
"""Shared task/offdesk priority action helpers."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from aoe_tg_orch_contract import normalize_tf_phase


def task_lane_target_snapshot(task: Dict[str, Any]) -> Dict[str, List[str]]:
    exec_critic = task.get("exec_critic") if isinstance(task.get("exec_critic"), dict) else {}
    return {
        "rerun_execution_lane_ids": [str(x).strip()[:32] for x in (exec_critic.get("rerun_execution_lane_ids") or []) if str(x).strip()],
        "rerun_review_lane_ids": [str(x).strip()[:32] for x in (exec_critic.get("rerun_review_lane_ids") or []) if str(x).strip()],
        "manual_followup_execution_lane_ids": [
            str(x).strip()[:32] for x in (exec_critic.get("manual_followup_execution_lane_ids") or []) if str(x).strip()
        ],
        "manual_followup_review_lane_ids": [
            str(x).strip()[:32] for x in (exec_critic.get("manual_followup_review_lane_ids") or []) if str(x).strip()
        ],
    }


def task_priority_action_snapshot(
    *,
    label: str,
    tf_phase: str,
    rerun_execution_lane_ids: List[str],
    rerun_review_lane_ids: List[str],
    manual_followup_execution_lane_ids: List[str],
    manual_followup_review_lane_ids: List[str],
) -> Dict[str, str]:
    safe_label = str(label or "").strip()
    safe_phase = normalize_tf_phase(tf_phase, "queued")
    rerun_exec = [str(x).strip() for x in rerun_execution_lane_ids if str(x).strip()]
    rerun_review = [str(x).strip() for x in rerun_review_lane_ids if str(x).strip()]
    manual_exec = [str(x).strip() for x in manual_followup_execution_lane_ids if str(x).strip()]
    manual_review = [str(x).strip() for x in manual_followup_review_lane_ids if str(x).strip()]

    if safe_phase not in {"needs_retry", "critic_review", "manual_intervention", "blocked"}:
        if rerun_exec or rerun_review:
            safe_phase = "needs_retry"
        elif manual_exec or manual_review:
            safe_phase = "manual_intervention"

    def _lane_suffix(exec_ids: List[str], review_ids: List[str]) -> str:
        lane_tokens = [str(x).strip() for x in exec_ids + review_ids if str(x).strip()]
        if not lane_tokens:
            return ""
        return " lane " + ",".join(lane_tokens)

    if safe_label and safe_phase == "planning":
        return {
            "action": f"/task {safe_label}",
            "reason": "active task is still planning",
        }

    if safe_label and safe_phase == "rate_limited":
        return {
            "action": f"/task {safe_label}",
            "reason": "active task is waiting for provider capacity",
        }

    if safe_label and safe_phase in {"needs_retry", "critic_review"}:
        lane_bits: List[str] = []
        if rerun_exec:
            lane_bits.append("execution=" + ",".join(rerun_exec))
        if rerun_review:
            lane_bits.append("review=" + ",".join(rerun_review))
        suffix = f" target {'; '.join(lane_bits)}" if lane_bits else ""
        return {
            "action": f"/retry {safe_label}{_lane_suffix(rerun_exec, rerun_review)}",
            "reason": f"active task requires retry ({safe_phase}){suffix}",
        }

    if safe_label and safe_phase in {"manual_intervention", "blocked"}:
        lane_bits: List[str] = []
        if manual_exec:
            lane_bits.append("execution=" + ",".join(manual_exec))
        if manual_review:
            lane_bits.append("review=" + ",".join(manual_review))
        suffix = f" target {'; '.join(lane_bits)}" if lane_bits else ""
        return {
            "action": f"/followup {safe_label}{_lane_suffix(manual_exec, manual_review)}",
            "reason": f"active task requires operator review ({safe_phase}){suffix}",
        }

    return {"action": "", "reason": ""}


def offdesk_priority_action_snapshot(
    *,
    alias: str,
    active_task_label: str,
    active_task_tf_phase: str,
    active_task_targets: Optional[Dict[str, List[str]]] = None,
    syncback_pending: bool,
    followup_count: int,
    proposal_count: int,
    bootstrap_recommended: bool,
    blocked_count: int,
    open_count: int,
    sync_quality: str,
    sync_quality_warn: bool,
    sync_stale: bool,
    canonical_exists: bool,
    include_ok: bool,
    last_sync_mode: str,
) -> Dict[str, str]:
    task_priority = task_priority_action_snapshot(
        label=active_task_label,
        tf_phase=active_task_tf_phase,
        rerun_execution_lane_ids=list((active_task_targets or {}).get("rerun_execution_lane_ids") or []),
        rerun_review_lane_ids=list((active_task_targets or {}).get("rerun_review_lane_ids") or []),
        manual_followup_execution_lane_ids=list((active_task_targets or {}).get("manual_followup_execution_lane_ids") or []),
        manual_followup_review_lane_ids=list((active_task_targets or {}).get("manual_followup_review_lane_ids") or []),
    )
    if str(task_priority.get("action", "")).strip():
        return task_priority
    if syncback_pending:
        return {
            "action": f"/todo {alias} syncback preview",
            "reason": "canonical TODO drift pending syncback",
        }
    if followup_count > 0:
        return {
            "action": f"/todo {alias} followup",
            "reason": "manual follow-up backlog pending review",
        }
    if proposal_count > 0:
        return {
            "action": f"/todo {alias} proposals",
            "reason": "open todo proposals pending triage",
        }
    if bootstrap_recommended:
        if not canonical_exists:
            return {
                "action": f"/sync bootstrap {alias} 24h",
                "reason": "bootstrap backlog because canonical TODO.md is missing",
            }
        if canonical_exists and not include_ok:
            return {
                "action": f"/sync bootstrap {alias} 24h",
                "reason": "bootstrap backlog because AOE_TODO.md is not linked to canonical TODO.md",
            }
        if last_sync_mode == "never":
            return {
                "action": f"/sync bootstrap {alias} 24h",
                "reason": "bootstrap backlog because the project has never been synced",
            }
        if sync_quality in {"non_backlog_docs", "unknown"}:
            return {
                "action": f"/sync bootstrap {alias} 24h",
                "reason": f"rebuild backlog from canonical/recent docs; last sync source was {sync_quality}",
            }
        if sync_stale:
            return {
                "action": f"/sync bootstrap {alias} 24h",
                "reason": "refresh stale backlog from canonical/recent project documents",
            }
        return {
            "action": f"/sync bootstrap {alias} 24h",
            "reason": "bootstrap backlog from recent project documents",
        }
    if sync_quality_warn:
        if sync_quality == "discovery":
            return {
                "action": f"/sync preview {alias} 24h",
                "reason": "inspect non-canonical discovery sources before execution",
            }
        if sync_quality == "mixed":
            return {
                "action": f"/sync preview {alias} 24h",
                "reason": "inspect mixed canonical and discovery sync sources before execution",
            }
    if blocked_count > 0 or open_count == 0 or sync_quality_warn:
        if open_count == 0:
            return {
                "action": f"/sync preview {alias} 24h",
                "reason": "review sync sources because there is no runnable backlog",
            }
        if blocked_count > 0:
            return {
                "action": f"/sync preview {alias} 24h",
                "reason": "review sync sources before retrying blocked backlog",
            }
        return {
            "action": f"/sync preview {alias} 24h",
            "reason": "review sync source quality before execution",
        }
    return {
        "action": f"/orch status {alias}",
        "reason": "inspect current project runtime and queue state",
    }
