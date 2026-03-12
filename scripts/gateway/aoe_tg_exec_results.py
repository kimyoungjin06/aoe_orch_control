#!/usr/bin/env python3
"""Execution result and exception response helpers."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from aoe_tg_exec_pipeline import project_alias


def result_request_ids(state: Dict[str, Any], fallback_request_id: str = "") -> List[str]:
    linked = state.get("linked_request_ids") if isinstance(state, dict) else []
    rows = linked if isinstance(linked, list) else []
    merged: List[str] = []
    seen = set()
    for raw in [fallback_request_id, str((state or {}).get("request_id", "")).strip(), *rows]:
        token = str(raw or "").strip()
        if not token or token in seen:
            continue
        seen.add(token)
        merged.append(token)
    return merged


def confirmed_result_reply_markup(entry: Dict[str, Any], key: str) -> Dict[str, Any]:
    alias = project_alias(entry, key)
    return {
        "keyboard": [
            [{"text": f"/todo {alias}"}, {"text": f"/orch status {alias}"}, {"text": "/monitor"}],
            [{"text": f"/sync preview {alias} 1h"}, {"text": "/queue"}, {"text": "/map"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": f"예: /todo {alias} 또는 /orch status {alias}",
    }


def early_gate_reply_markup(entry: Dict[str, Any], key: str) -> Dict[str, Any]:
    alias = project_alias(entry, key)
    return {
        "keyboard": [
            [{"text": f"/orch status {alias}"}, {"text": f"/todo {alias}"}, {"text": "/monitor"}],
            [{"text": f"/sync preview {alias} 1h"}, {"text": "/queue"}, {"text": "/map"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": f"예: /orch status {alias} 또는 /todo {alias}",
    }


def intervention_reply_markup(entry: Dict[str, Any], key: str, req_id: str = "") -> Dict[str, Any]:
    alias = project_alias(entry, key)
    keyboard: List[List[Dict[str, str]]] = []
    req_token = str(req_id or "").strip()
    if req_token:
        keyboard.append(
            [
                {"text": f"/task {req_token}"},
                {"text": f"/replan {req_token}"},
                {"text": f"/retry {req_token}"},
            ]
        )
    keyboard.append([{"text": f"/todo {alias}"}, {"text": f"/orch status {alias}"}, {"text": "/monitor"}])
    keyboard.append([{"text": "/queue"}, {"text": "/map"}, {"text": "/help"}])
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": f"예: /task {req_token or '-'} 또는 /todo {alias}",
    }


def send_exec_critic_intervention(
    *,
    entry: Dict[str, Any],
    key: str,
    final_req_id: str,
    verdict: str,
    reason: str,
    exec_attempt: int,
    exec_max_attempts: int,
    send: Callable[..., bool],
) -> None:
    send(
        "exec critic: intervention needed\n"
        f"- verdict: {verdict}\n"
        f"- reason: {reason or '-'}\n"
        f"- attempts: {exec_attempt}/{exec_max_attempts}\n"
        f"- last_request_id: {final_req_id or '-'}\n"
        "next:\n"
        f"- /task {final_req_id}\n"
        f"- /replan {final_req_id}\n"
        f"- /retry {final_req_id}",
        context="exec-critic",
        with_menu=True,
        reply_markup=intervention_reply_markup(entry, key, final_req_id),
    )


def send_dispatch_exception(
    *,
    entry: Dict[str, Any],
    key: str,
    todo_id: str,
    reason: str,
    send: Callable[..., bool],
) -> None:
    alias = project_alias(entry, key)
    lines = [
        "dispatch failed before request start",
        f"- orch: {key} ({alias})",
        f"- reason: {reason or 'dispatch_failed'}",
    ]
    token = str(todo_id or "").strip()
    if token:
        lines.append(f"- todo: {token}")
    lines.extend(
        [
            "next:",
            f"- /orch status {alias}",
            f"- /todo {alias}",
            f"- /sync preview {alias} 1h",
        ]
    )
    send(
        "\n".join(lines),
        context="dispatch-exception",
        with_menu=True,
        reply_markup=early_gate_reply_markup(entry, key),
    )


def send_dispatch_result(
    *,
    args: Any,
    key: str,
    entry: Dict[str, Any],
    p_args: Any,
    prompt: str,
    state: Dict[str, Any],
    req_id: str,
    task: Optional[Dict[str, Any]],
    run_control_mode: str,
    run_source_request_id: str,
    run_auto_source: str,
    send: Callable[..., bool],
    log_event: Callable[..., None],
    summarize_task_lifecycle: Callable[[str, Dict[str, Any]], str],
    synthesize_orchestrator_response: Callable[[Any, str, Dict[str, Any]], str],
    render_run_response: Callable[..., str],
    finalize_request_reply_messages: Callable[..., Dict[str, Any]],
) -> bool:
    request_ids = result_request_ids(state, req_id)
    reply_markup = confirmed_result_reply_markup(entry, key) if str(run_auto_source or "").strip().lower() == "confirmed" else None
    if task is not None:
        ver_status = str((task.get("stages") or {}).get("verification", "pending"))
        if bool(args.require_verifier) and ver_status == "failed":
            send(
                summarize_task_lifecycle(key, task),
                context="verifier-gate failed",
                reply_markup=intervention_reply_markup(entry, key, req_id),
            )
            log_event(
                event="dispatch_failed",
                project=key,
                request_id=req_id,
                task=task,
                stage="verification",
                status="failed",
                error_code="E_GATE",
                detail="verifier_gate_failed",
            )
            return True

    if bool(state.get("complete", False)) and (state.get("replies") or []):
        try:
            send(synthesize_orchestrator_response(p_args, prompt, state), context="synth", reply_markup=reply_markup)
            for request_id in request_ids:
                try:
                    finalize_request_reply_messages(args, request_id)
                except Exception:
                    pass
            log_event(
                event="dispatch_completed",
                project=key,
                request_id=req_id,
                task=task,
                stage=str((task or {}).get("stage", "close")),
                status=str((task or {}).get("status", "completed")),
                detail=f"control_mode={run_control_mode or 'normal'} source_request_id={run_source_request_id or '-'}",
            )
            return True
        except Exception:
            pass

    send(render_run_response(state, task=task), context="result", reply_markup=reply_markup)
    if bool(state.get("complete", False)):
        for request_id in request_ids:
            try:
                finalize_request_reply_messages(args, request_id)
            except Exception:
                pass
    log_event(
        event="dispatch_result",
        project=key,
        request_id=req_id,
        task=task,
        stage=str((task or {}).get("stage", "close")),
        status=str((task or {}).get("status", "running" if not bool(state.get("complete", False)) else "completed")),
        detail=f"control_mode={run_control_mode or 'normal'} source_request_id={run_source_request_id or '-'}",
    )
    return True
