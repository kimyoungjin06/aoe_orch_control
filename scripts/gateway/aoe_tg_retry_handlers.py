#!/usr/bin/env python3
"""Retry/Replan transition handlers for Telegram gateway."""

from typing import Any, Callable, Dict, List, Optional


def _normalize_retry_lane_ids(lane_ids: Optional[List[str]]) -> tuple[List[str], List[str]]:
    execution: List[str] = []
    review: List[str] = []
    seen_exec: set[str] = set()
    seen_review: set[str] = set()
    for item in lane_ids or []:
        token = str(item or "").strip()[:32]
        if not token:
            continue
        upper = token.upper()
        if upper.startswith("L"):
            key = upper
            if key not in seen_exec:
                seen_exec.add(key)
                execution.append(token)
        elif upper.startswith("R"):
            key = upper
            if key not in seen_review:
                seen_review.add(key)
                review.append(token)
    return execution, review

def resolve_retry_replan_transition(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    orch_target: Optional[str],
    orch_retry_request_id: Optional[str],
    orch_replan_request_id: Optional[str],
    orch_retry_lane_ids: Optional[List[str]],
    orch_replan_lane_ids: Optional[List[str]],
    send: Callable[..., bool],
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]],
    get_chat_selected_task_ref: Callable[..., str],
    resolve_chat_task_ref: Callable[..., str],
    resolve_task_request_id: Callable[[Dict[str, Any], str], str],
    get_task_record: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    run_request_query: Callable[[Any, str], Dict[str, Any]],
    sync_task_lifecycle: Callable[..., Optional[Dict[str, Any]]],
    resolve_verifier_candidates: Callable[[str], List[str]],
    dedupe_roles: Callable[[Any], List[str]],
    touch_chat_recent_task_ref: Callable[..., None],
    set_chat_selected_task_ref: Callable[..., None],
) -> Optional[Dict[str, Any]]:
    if cmd not in {"orch-retry", "orch-replan"}:
        return None

    key, entry, p_args = get_context(orch_target)
    req_ref = (
        (orch_retry_request_id if cmd == "orch-retry" else orch_replan_request_id)
        or get_chat_selected_task_ref(manager_state, chat_id, key)
        or ""
    ).strip()
    requested_execution_lane_ids, requested_review_lane_ids = _normalize_retry_lane_ids(
        orch_retry_lane_ids if cmd == "orch-retry" else orch_replan_lane_ids
    )
    if not req_ref:
        send(
            (
                f"usage: {'/retry' if cmd == 'orch-retry' else '/replan'} <request_or_alias> "
                f"[lane <L#|R#,...>]\norch={key}"
            ),
            context=f"{cmd} usage",
        )
        return {"terminal": True}

    req_ref = resolve_chat_task_ref(manager_state, chat_id, key, req_ref)
    req_id = resolve_task_request_id(entry, req_ref)
    if not req_id:
        send(f"request not found: {req_ref} (orch={key})", context=f"{cmd} missing")
        return {"terminal": True}

    source_task = get_task_record(entry, req_id)
    if source_task is None:
        try:
            data = run_request_query(p_args, req_id)
            source_task = sync_task_lifecycle(
                entry=entry,
                request_data=data,
                prompt="",
                mode="dispatch",
                selected_roles=None,
                verifier_roles=None,
                require_verifier=bool(args.require_verifier),
                verifier_candidates=resolve_verifier_candidates(args.verifier_roles),
            )
        except Exception:
            source_task = None

    if source_task is None:
        send(f"no lifecycle record for retry/replan target: {req_ref}", context=f"{cmd} missing task")
        return {"terminal": True}

    if requested_execution_lane_ids or requested_review_lane_ids:
        exec_critic = source_task.get("exec_critic") if isinstance(source_task.get("exec_critic"), dict) else {}
        allowed_execution_lane_ids = {
            str(item).strip()[:32]
            for item in (exec_critic.get("rerun_execution_lane_ids") or [])
            if str(item).strip()
        }
        allowed_review_lane_ids = {
            str(item).strip()[:32]
            for item in (exec_critic.get("rerun_review_lane_ids") or [])
            if str(item).strip()
        }
        if not allowed_execution_lane_ids and not allowed_review_lane_ids:
            send(
                (
                    "lane retry targets are not available for this task.\n"
                    f"request_id={req_id}\n"
                    "allowed: none"
                ),
                context=f"{cmd} lane unavailable",
            )
            return {"terminal": True}
        selected_execution_lane_ids = [
            lane for lane in requested_execution_lane_ids if lane in allowed_execution_lane_ids
        ]
        selected_review_lane_ids = [
            lane for lane in requested_review_lane_ids if lane in allowed_review_lane_ids
        ]
        if not selected_execution_lane_ids and not selected_review_lane_ids:
            send(
                (
                    f"requested lanes are not allowed for this task.\nrequest_id={req_id}\n"
                    "allowed execution: {execs}\nallowed review: {reviews}"
                ).format(
                    execs=", ".join(sorted(allowed_execution_lane_ids)) or "-",
                    reviews=", ".join(sorted(allowed_review_lane_ids)) or "-",
                ),
                context=f"{cmd} lane invalid",
            )
            return {"terminal": True}
        requested_execution_lane_ids = selected_execution_lane_ids
        requested_review_lane_ids = selected_review_lane_ids

    src_prompt = str(source_task.get("prompt", "")).strip()
    if not src_prompt:
        send(
            "cannot retry/replan: source task prompt is missing.\n"
            f"request_id={req_id}",
            context=f"{cmd} missing prompt",
        )
        return {"terminal": True}

    source_roles = dedupe_roles(source_task.get("roles") or [])
    source_mode = str(source_task.get("mode", "dispatch")).strip().lower()
    touch_chat_recent_task_ref(manager_state, chat_id, key, req_id)
    set_chat_selected_task_ref(manager_state, chat_id, key, req_id)

    return {
        "terminal": False,
        "cmd": "run",
        "rest": "",
        "orch_target": key,
        "run_prompt": src_prompt,
        "run_roles_override": ",".join(source_roles) if source_roles else None,
        "run_force_mode": "direct" if source_mode == "direct" else "dispatch",
        "run_no_wait_override": False,
        "run_control_mode": "retry" if cmd == "orch-retry" else "replan",
        "run_source_request_id": req_id,
        "run_source_task": source_task,
        "run_selected_execution_lane_ids": requested_execution_lane_ids,
        "run_selected_review_lane_ids": requested_review_lane_ids,
    }
