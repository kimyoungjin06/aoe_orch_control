#!/usr/bin/env python3
"""Execution pipeline helpers for run handler orchestration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

from aoe_tg_blocked_state import apply_todo_execution_outcome, blocked_bucket_label


@dataclass
class DispatchSyncResult:
    state: Dict[str, Any]
    request_id: str
    task: Optional[Dict[str, Any]]


def task_label_for_todo(task: Optional[Dict[str, Any]], fallback_request_id: str) -> str:
    rid = str(fallback_request_id or "").strip()
    if not isinstance(task, dict):
        return rid or "-"
    short_id = str(task.get("short_id", "")).strip().upper()
    alias = str(task.get("alias", "")).strip()
    if short_id and alias:
        return f"{short_id} | {alias}"
    if alias:
        return alias
    if short_id:
        return short_id
    token = str(task.get("request_id", "")).strip()
    return token or rid or "-"


def find_project_todo_item(entry: Dict[str, Any], todo_id: str) -> Optional[Dict[str, Any]]:
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


def attach_todo_to_task_and_entry(
    *,
    entry: Dict[str, Any],
    chat_id: str,
    todo_id: str,
    req_id: str,
    task: Optional[Dict[str, Any]],
    now_iso: Callable[[], str],
) -> None:
    token = str(todo_id or "").strip()
    if not token:
        return
    if isinstance(task, dict) and not str(task.get("todo_id", "")).strip():
        task["todo_id"] = token

    item = find_project_todo_item(entry, token)
    if not isinstance(item, dict):
        return

    now = now_iso()
    item["status"] = "running"
    item["started_at"] = str(item.get("started_at", "")).strip() or now
    item["started_by"] = str(item.get("started_by", "")).strip() or str(item.get("queued_by", "")).strip() or f"telegram:{chat_id}"
    item["updated_at"] = now
    if str(req_id or "").strip():
        item["current_request_id"] = str(req_id).strip()
        item["current_task_label"] = task_label_for_todo(task, req_id)
    item.pop("queued_at", None)
    item.pop("queued_by", None)


def project_alias(entry: Dict[str, Any], fallback: str) -> str:
    token = str(entry.get("project_alias", "")).strip().upper()
    return token or str(fallback or "").strip() or "-"


def effective_todo_token(
    *,
    entry: Dict[str, Any],
    chat_id: str,
    todo_id: str,
    run_auto_source: str,
) -> str:
    token = str(todo_id or "").strip()
    if token:
        return token
    if not str(run_auto_source or "").strip().lower().startswith("todo"):
        return ""
    pending = entry.get("pending_todo")
    if isinstance(pending, dict) and str(pending.get("chat_id", "")).strip() == str(chat_id):
        return str(pending.get("todo_id", "")).strip()
    return ""


def maybe_send_manual_followup_alert(
    *,
    entry: Dict[str, Any],
    todo_id: str,
    project_key: str,
    send: Callable[..., bool],
    now_iso: Callable[[], str],
) -> bool:
    token = str(todo_id or "").strip()
    if not token:
        return False
    item = find_project_todo_item(entry, token)
    if not isinstance(item, dict):
        return False
    if str(item.get("status", "")).strip().lower() != "blocked":
        return False
    if blocked_bucket_label(item.get("blocked_bucket", "")) != "manual_followup":
        return False
    if str(item.get("blocked_alerted_at", "")).strip():
        return False
    try:
        blocked_count = max(1, int(item.get("blocked_count", 0) or 0))
    except Exception:
        blocked_count = 1
    alias = project_alias(entry, project_key)
    summary = " ".join(str(item.get("summary", "")).strip().split())
    if len(summary) > 120:
        summary = summary[:117].rstrip() + "..."
    reason = " ".join(str(item.get("blocked_reason", "")).strip().split())
    if len(reason) > 180:
        reason = reason[:177].rstrip() + "..."
    item["blocked_alerted_at"] = now_iso()
    item["updated_at"] = now_iso()
    lines = [
        "manual follow-up needed",
        f"- orch: {project_key} ({alias})",
        f"- id: {token}",
        f"- blocked_count: {blocked_count}",
    ]
    if summary:
        lines.append(f"- summary: {summary}")
    if reason:
        lines.append(f"- reason: {reason}")
    lines.extend(
        [
            "next:",
            f"- /todo {alias}",
            f"- /todo {alias} followup",
            "- /queue followup",
            f"- /orch status {alias}",
            "- /focus off   (if you need global switch)",
        ]
    )
    reply_markup = {
        "keyboard": [
            [{"text": f"/todo {alias} followup"}, {"text": f"/todo {alias}"}, {"text": f"/orch status {alias}"}],
            [{"text": "/queue followup"}, {"text": "/focus off"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": f"예: /todo {alias} followup",
    }
    send("\n".join(lines), context="manual-followup-alert", with_menu=True, reply_markup=reply_markup)
    return True


def find_todo_proposal_row(entry: Dict[str, Any], proposal_id: str) -> Optional[Dict[str, Any]]:
    token = str(proposal_id or "").strip().upper()
    if not token:
        return None
    rows = entry.get("todo_proposals")
    if not isinstance(rows, list):
        return None
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("id", "")).strip().upper() == token:
            return row
    return None


def maybe_capture_todo_proposals(
    *,
    args: Any,
    entry: Dict[str, Any],
    key: str,
    p_args: Any,
    prompt: str,
    state: Dict[str, Any],
    req_id: str,
    task: Optional[Dict[str, Any]],
    todo_id: str,
    send: Callable[..., bool],
    log_event: Callable[..., None],
    now_iso: Callable[[], str],
    extract_todo_proposals: Callable[..., List[Dict[str, Any]]],
    merge_todo_proposals: Callable[..., Dict[str, Any]],
) -> Dict[str, Any]:
    if not req_id:
        return {"created_count": 0, "created_ids": [], "duplicate_count": 0, "skipped_count": 0}
    replies = state.get("replies") or []
    if not isinstance(replies, list) or not replies:
        return {"created_count": 0, "created_ids": [], "duplicate_count": 0, "skipped_count": 0}

    try:
        proposals_data = extract_todo_proposals(
            p_args,
            prompt,
            state,
            task=task,
        )
    except Exception as exc:
        log_event(
            event="todo_proposals_extract_failed",
            project=key,
            request_id=req_id,
            task=task,
            stage=str((task or {}).get("stage", "close")),
            status="failed",
            error_code="E_TODO_PROPOSALS",
            detail=str(exc)[:240],
        )
        return {"created_count": 0, "created_ids": [], "duplicate_count": 0, "skipped_count": 0}

    if not isinstance(proposals_data, list) or not proposals_data:
        return {"created_count": 0, "created_ids": [], "duplicate_count": 0, "skipped_count": 0}

    source_todo_id = str(todo_id or "").strip()
    if not source_todo_id and isinstance(task, dict):
        source_todo_id = str(task.get("todo_id", "")).strip()

    try:
        merged = merge_todo_proposals(
            entry=entry,
            request_id=req_id,
            task=task,
            source_todo_id=source_todo_id,
            proposals_data=proposals_data,
            now_iso=now_iso,
        )
    except Exception as exc:
        log_event(
            event="todo_proposals_merge_failed",
            project=key,
            request_id=req_id,
            task=task,
            stage=str((task or {}).get("stage", "close")),
            status="failed",
            error_code="E_TODO_PROPOSALS",
            detail=str(exc)[:240],
        )
        return {"created_count": 0, "created_ids": [], "duplicate_count": 0, "skipped_count": 0}

    created_ids = [str(item or "").strip() for item in (merged.get("created_ids") or []) if str(item or "").strip()]
    created_count = int(merged.get("created_count", 0) or 0)
    if created_count <= 0:
        return merged

    alias = project_alias(entry, key)
    lines = [
        "new todo proposals",
        f"- orch: {key} ({alias})",
        f"- source_request: {req_id}",
        f"- created: {created_count}",
    ]
    if source_todo_id:
        lines.append(f"- source_todo: {source_todo_id}")
    for proposal_id in created_ids[:3]:
        row = find_todo_proposal_row(entry, proposal_id)
        if not isinstance(row, dict):
            lines.append(f"- {proposal_id}")
            continue
        summary = " ".join(str(row.get("summary", "")).strip().split())
        if len(summary) > 120:
            summary = summary[:117].rstrip() + "..."
        priority = str(row.get("priority", "P2")).strip().upper() or "P2"
        kind = str(row.get("kind", "followup")).strip().lower() or "followup"
        lines.append(f"- {proposal_id} [{kind}] {priority} | {summary or '-'}")
    lines.extend(
        [
            "next:",
            "- /todo proposals",
            f"- /todo {alias}",
            f"- /orch status {alias}",
        ]
    )
    keyboard: List[List[Dict[str, str]]] = [
        [{"text": "/todo proposals"}, {"text": f"/todo {alias}"}, {"text": f"/orch status {alias}"}],
        [{"text": "/queue"}, {"text": "/map"}],
    ]
    accept_row: List[Dict[str, str]] = []
    reject_row: List[Dict[str, str]] = []
    for proposal_id in created_ids[:2]:
        accept_row.append({"text": f"/todo accept {proposal_id}"})
        reject_row.append({"text": f"/todo reject {proposal_id}"})
    if accept_row:
        keyboard.insert(1, accept_row)
    if reject_row:
        keyboard.insert(2, reject_row)
    send(
        "\n".join(lines),
        context="todo-proposals-alert",
        with_menu=True,
        reply_markup={
            "keyboard": keyboard,
            "resize_keyboard": True,
            "one_time_keyboard": False,
            "input_field_placeholder": "예: /todo proposals 또는 /todo accept PROP-001",
        },
    )
    log_event(
        event="todo_proposals_created",
        project=key,
        request_id=req_id,
        task=task,
        stage=str((task or {}).get("stage", "close")),
        status="completed",
        detail=f"created={created_count} duplicate={int(merged.get('duplicate_count', 0) or 0)} skipped={int(merged.get('skipped_count', 0) or 0)}",
    )
    return merged


def finalize_todo_after_run(
    *,
    entry: Dict[str, Any],
    todo_id: str,
    status: str,
    exec_verdict: str,
    exec_reason: str,
    req_id: str,
    task: Optional[Dict[str, Any]],
    now_iso: Callable[[], str],
    manual_followup_threshold: int,
) -> None:
    token = str(todo_id or "").strip()
    if not token:
        return
    item = find_project_todo_item(entry, token)
    if not isinstance(item, dict):
        return

    now = now_iso()
    task_status = str(status or "").strip().lower()
    verdict = str(exec_verdict or "").strip().lower()
    reason = str(exec_reason or "").strip()

    apply_todo_execution_outcome(
        item,
        task_status=task_status,
        exec_verdict=verdict,
        exec_reason=reason,
        req_id=str(req_id or "").strip(),
        now=now,
        task_label=task_label_for_todo(task, str(req_id or "").strip()),
        manual_followup_threshold=manual_followup_threshold,
    )


def cleanup_terminal_todo_gate(
    *,
    entry: Dict[str, Any],
    chat_id: str,
    todo_id: str,
    pending_todo_used: bool,
    run_auto_source: str,
    reason: str,
    now_iso: Callable[[], str],
    manual_followup_threshold: int,
) -> bool:
    token = effective_todo_token(
        entry=entry,
        chat_id=chat_id,
        todo_id=todo_id,
        run_auto_source=run_auto_source,
    )
    if token and not str(todo_id or "").strip():
        pending_todo_used = True

    if token:
        finalize_todo_after_run(
            entry=entry,
            todo_id=token,
            status="failed",
            exec_verdict="fail",
            exec_reason=str(reason or "dispatch policy blocked").strip()[:240],
            req_id="",
            task=None,
            now_iso=now_iso,
            manual_followup_threshold=manual_followup_threshold,
        )

    pending = entry.get("pending_todo")
    if isinstance(pending, dict) and str(pending.get("chat_id", "")).strip() == str(chat_id):
        pending_id = str(pending.get("todo_id", "")).strip()
        if (not token) or pending_id == token or pending_todo_used:
            entry.pop("pending_todo", None)
            entry["updated_at"] = now_iso()
            return True

    if token:
        entry["updated_at"] = now_iso()
    return False


def dispatch_and_sync_task(
    *,
    p_args: Any,
    dispatch_prompt: str,
    chat_id: str,
    dispatch_roles: str,
    run_priority_override: Optional[str],
    run_timeout_override: Optional[int],
    run_no_wait_override: Optional[bool],
    key: str,
    entry: Dict[str, Any],
    manager_state: Dict[str, Any],
    prompt: str,
    selected_roles: List[str],
    verifier_roles: List[str],
    require_verifier: bool,
    verifier_candidates: List[str],
    run_aoe_orch: Callable[..., Dict[str, Any]],
    touch_chat_recent_task_ref: Callable[..., None],
    set_chat_selected_task_ref: Callable[..., None],
    now_iso: Callable[[], str],
    sync_task_lifecycle: Callable[..., Optional[Dict[str, Any]]],
) -> DispatchSyncResult:
    state = run_aoe_orch(
        p_args,
        dispatch_prompt,
        chat_id=chat_id,
        roles_override=dispatch_roles,
        priority_override=run_priority_override,
        timeout_override=run_timeout_override,
        no_wait_override=run_no_wait_override,
    )

    req_id = str(state.get("request_id", "")).strip()
    if req_id:
        entry["last_request_id"] = req_id
        touch_chat_recent_task_ref(manager_state, chat_id, key, req_id)
        set_chat_selected_task_ref(manager_state, chat_id, key, req_id)
    entry["updated_at"] = now_iso()

    task = sync_task_lifecycle(
        entry=entry,
        request_data=state,
        prompt=prompt,
        mode="dispatch",
        selected_roles=selected_roles,
        verifier_roles=verifier_roles,
        require_verifier=bool(require_verifier),
        verifier_candidates=verifier_candidates,
    )
    if task is not None:
        task["initiator_chat_id"] = str(chat_id)
        task["updated_at"] = now_iso()

    return DispatchSyncResult(
        state=state,
        request_id=req_id,
        task=task,
    )
