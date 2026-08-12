"""Truthful effect envelopes for Telegram operator results.

The adapter has several mutation paths, but callers and the conversation ledger
must see one consistent description of what changed. This module keeps that
classification separate from rendering and from the workflow executors.
"""

from __future__ import annotations

from typing import Any


EFFECT_SCHEMA = "remote_operator_telegram_effect.v1"


def no_effect() -> dict[str, Any]:
    return {
        "schema": EFFECT_SCHEMA,
        "kind": "none",
        "status": "none",
        "authorized": False,
        "authority_domain": "none",
    }


# selection_status -> (effect kind, authority domain, nested receipt key, action)
PLAN_EFFECTS: dict[str, tuple[str, str, str, str | None]] = {
    "init_previewed": (
        "artifact_write",
        "plan_artifact",
        "project_init_preview",
        "project_init_preview",
    ),
    "init_created": (
        "artifact_write",
        "plan_artifact",
        "project_init_run",
        "project_init",
    ),
    "plan_draft_validated": (
        "artifact_write",
        "plan_artifact",
        "plan_draft",
        "plan_draft",
    ),
    "plan_registered": (
        "plan_registry_mutation",
        "plan_registry",
        "plan_registration",
        "register",
    ),
    "plan_review_approved": (
        "plan_review_resolution",
        "plan_review",
        "plan_review",
        "approved",
    ),
    "plan_launch_prep_prepared": (
        "artifact_write",
        "launch_preparation",
        "plan_launch_prep",
        "prepare",
    ),
    "plan_gate_request_created": (
        "approval_request_creation",
        "offdesk_gate",
        "plan_gate_request",
        "request",
    ),
    "plan_gate_approved": (
        "approval_resolution",
        "offdesk_gate",
        "plan_gate_resolution",
        "approved",
    ),
    "plan_gate_denied": (
        "approval_resolution",
        "offdesk_gate",
        "plan_gate_resolution",
        "denied",
    ),
    "plan_execution_brief_created": (
        "artifact_write",
        "execution_brief",
        "plan_execution_brief",
        "create",
    ),
    "plan_enqueue_handoff_created": (
        "artifact_write",
        "enqueue_handoff",
        "plan_enqueue_handoff",
        "create",
    ),
    "plan_workload_bound": (
        "artifact_write",
        "workload_binding",
        "plan_workload_binding",
        "bind",
    ),
    "plan_enqueued": (
        "queue_mutation",
        "offdesk_task_queue",
        "plan_enqueue_run",
        "enqueue",
    ),
    "plan_runtime_started": (
        "runtime_launch",
        "offdesk_runtime",
        "plan_runtime_start",
        "start",
    ),
    "plan_runtime_monitored": (
        "runtime_observation",
        "offdesk_runtime",
        "plan_runtime_monitor",
        "monitor",
    ),
    "plan_closeout_packet_created": (
        "artifact_write",
        "closeout_packet",
        "plan_closeout_packet",
        "create",
    ),
    "plan_closeout_review_handoff_created": (
        "artifact_write",
        "closeout_review_handoff",
        "plan_closeout_review_handoff",
        "create",
    ),
    "plan_closeout_verdict_recorded": (
        "closeout_review_resolution",
        "accepted_truth_review",
        "plan_closeout_verdict",
        None,
    ),
}


def _receipt_from_plan(
    rendered: dict[str, Any], receipt_key: str
) -> dict[str, Any] | None:
    session = rendered.get("remote_plan_session")
    if not isinstance(session, dict):
        return None
    receipt = session.get(receipt_key)
    return receipt if isinstance(receipt, dict) else None


def _plan_effect(rendered: dict[str, Any]) -> dict[str, Any] | None:
    parsed = rendered.get("parsed_command")
    if not isinstance(parsed, dict):
        return None
    selection_status = str(parsed.get("selection_status") or "")
    spec = PLAN_EFFECTS.get(selection_status)
    if spec is None:
        return None
    kind, authority_domain, receipt_key, fixed_action = spec
    receipt = _receipt_from_plan(rendered, receipt_key)
    if receipt is None:
        return None
    action = fixed_action
    if selection_status == "plan_closeout_verdict_recorded":
        action = str(receipt.get("verdict") or "") or None
    effect = {
        "schema": EFFECT_SCHEMA,
        "kind": kind,
        "status": "applied",
        "authorized": True,
        "authority_domain": authority_domain,
        "source": "remote_plan_session",
        "stage": selection_status,
        "action": action,
        "receipt_schema": str(receipt.get("schema") or "") or None,
        "receipt_status": str(receipt.get("status") or "") or None,
    }
    if selection_status == "plan_closeout_verdict_recorded":
        effect["accepted_truth_recorded"] = bool(
            receipt.get("accepted_truth_recorded")
        )
    return effect


def _dispatch_authority_domain(effect_kind: str) -> str:
    return {
        "session_input": "agent_session_input",
        "session_message": "agent_session_input",
        "decision": "ondesk_decision",
        "recovery": "accepted_truth_recovery",
        "runtime_dispatch": "offdesk_runtime",
        "cancel_task": "offdesk_task_queue",
        "pause": "operator_control",
        "resume": "operator_control",
    }.get(effect_kind, "guarded_control")


def annotate_result_effect(rendered: dict[str, Any]) -> None:
    """Attach one truthful primary effect and its legacy authorization flags."""

    effect = no_effect()
    dispatch = rendered.get("dispatch_result")
    if isinstance(dispatch, dict):
        applied = bool(dispatch.get("ok"))
        effect_kind = str(dispatch.get("kind") or "")
        if not effect_kind and dispatch.get("decision_id"):
            effect_kind = "decision"
        if not effect_kind and dispatch.get("recovery_id"):
            effect_kind = "recovery"
        if not effect_kind:
            effect_kind = "guarded_control"
        action = str(
            dispatch.get("action")
            or dispatch.get("action_kind")
            or dispatch.get("decision")
            or ""
        ) or None
        effect = {
            "schema": EFFECT_SCHEMA,
            "kind": effect_kind,
            "action": action,
            "status": "applied" if applied else "refused",
            "authorized": applied,
            "authority_domain": _dispatch_authority_domain(effect_kind),
            "source": "guarded_dispatch",
            "error": str(dispatch.get("error") or "") or None,
        }
        if applied:
            rendered["read_only"] = False
            rendered["mutation_authorized"] = True
            if effect_kind == "decision":
                rendered["approval_resolution_authorized"] = True
                # Keep the legacy flag scoped to a positive approval grant.
                # The typed authority_domain distinguishes other resolutions.
                rendered["approval_authorized"] = action in {
                    "approve",
                    "approved",
                    "allow",
                    "accepted",
                }
    elif rendered.get("wiki_candidate_recorded"):
        effect = {
            "schema": EFFECT_SCHEMA,
            "kind": "wiki_candidate",
            "status": "recorded",
            "authorized": True,
            "authority_domain": "adaptive_wiki_candidate_store",
            "source": "remember",
        }
        rendered["read_only"] = False
        rendered["mutation_authorized"] = True
    elif rendered.get("feedback_recorded"):
        effect = {
            "schema": EFFECT_SCHEMA,
            "kind": "feedback_record",
            "status": "recorded",
            "authorized": True,
            "authority_domain": "feedback_store",
            "source": "feedback",
        }
        rendered["read_only"] = False
        rendered["mutation_authorized"] = True
    else:
        plan_effect = _plan_effect(rendered)
        if plan_effect is not None:
            effect = plan_effect
            rendered["read_only"] = False
            rendered["mutation_authorized"] = True
            if effect["kind"] == "approval_resolution":
                rendered["approval_resolution_authorized"] = True
                rendered["approval_authorized"] = effect.get("action") == "approved"
    rendered["authority_domain"] = effect["authority_domain"]
    rendered["effect"] = effect
