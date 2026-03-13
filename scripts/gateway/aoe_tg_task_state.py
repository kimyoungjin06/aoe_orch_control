#!/usr/bin/env python3
"""Task store, normalization, alias, and lifecycle mutation helpers."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from aoe_tg_orch_contract import derive_tf_phase, derive_tf_phase_reason, normalize_tf_phase
from aoe_tg_priority_actions import task_lane_target_snapshot, task_priority_action_snapshot


LANE_STATES = ("pending", "running", "done", "failed", "waiting_on_dependencies")
LANE_VERDICTS = ("success", "retry", "fail", "intervention")


def _normalize_lane_status(raw: Any, default: str = "pending") -> str:
    token = str(raw or "").strip().lower()
    if token in LANE_STATES:
        return token
    if token in {"error", "fail"}:
        return "failed"
    if token in {"complete", "completed", "success"}:
        return "done"
    if token in {"in_progress", "in-progress", "working", "active"}:
        return "running"
    if token in {"blocked", "queued"}:
        return "pending"
    return default


def _merge_role_status(prev: str, raw: Any) -> str:
    token = _normalize_lane_status(raw)
    order = {"failed": 4, "running": 3, "done": 2, "pending": 1}
    return token if order.get(token, 0) >= order.get(prev, 0) else prev


def _normalize_lane_verdict(raw: Any, default: str = "") -> str:
    token = str(raw or "").strip().lower()
    if token in LANE_VERDICTS:
        return token
    if token in {"ok", "pass"}:
        return "success"
    if token in {"failed", "error"}:
        return "fail"
    if token in {"escalate"}:
        return "intervention"
    return default


def _normalize_lane_state_rows(raw_rows: Any, *, kind: str) -> List[Dict[str, Any]]:
    rows = raw_rows if isinstance(raw_rows, list) else []
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        lane_id = str(row.get("lane_id", "")).strip() or str(row.get("id", "")).strip()
        if not lane_id:
            continue
        item: Dict[str, Any] = {
            "lane_id": lane_id[:32],
            "role": str(row.get("role", "")).strip()[:64] or ("Reviewer" if kind == "review" else "Worker"),
            "status": _normalize_lane_status(row.get("status")),
        }
        if kind == "execution":
            subtask_ids = [str(x).strip()[:32] for x in (row.get("subtask_ids") or []) if str(x).strip()]
            if subtask_ids:
                item["subtask_ids"] = subtask_ids
        else:
            item["kind"] = str(row.get("kind", "")).strip()[:32] or "verifier"
            depends = [str(x).strip()[:32] for x in (row.get("depends_on") or []) if str(x).strip()]
            if depends:
                item["depends_on"] = depends
            waiting = [str(x).strip()[:32] for x in (row.get("waiting_on") or []) if str(x).strip()]
            if waiting:
                item["waiting_on"] = waiting
            verdict = _normalize_lane_verdict(row.get("verdict"))
            if verdict:
                item["verdict"] = verdict
            action = str(row.get("action", "")).strip().lower()
            if action:
                item["action"] = action[:32]
        reason = str(row.get("reason", "")).strip()
        if reason:
            item["reason"] = reason[:240]
        normalized.append(item)
    return normalized


def _lane_state_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {name: 0 for name in LANE_STATES}
    for row in rows:
        if not isinstance(row, dict):
            continue
        status = _normalize_lane_status(row.get("status"))
        counts[status] = counts.get(status, 0) + 1
    return {key: value for key, value in counts.items() if value}


def _lane_verdict_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {name: 0 for name in LANE_VERDICTS}
    for row in rows:
        if not isinstance(row, dict):
            continue
        verdict = _normalize_lane_verdict(row.get("verdict"))
        if verdict:
            counts[verdict] = counts.get(verdict, 0) + 1
    return {key: value for key, value in counts.items() if value}


def derive_role_execution_snapshot(
    requested_roles: Iterable[str],
    executed_roles: Iterable[str],
    *,
    dedupe_roles: Callable[[Iterable[str]], List[str]],
) -> Dict[str, Any]:
    requested = dedupe_roles(requested_roles or [])
    executed = dedupe_roles(executed_roles or [])
    executed_lookup = {str(role).strip().lower(): str(role).strip() for role in executed if str(role).strip()}
    requested_lookup = {str(role).strip().lower(): str(role).strip() for role in requested if str(role).strip()}
    dropped = [role for role in requested if str(role).strip().lower() not in executed_lookup]
    added = [role for role in executed if str(role).strip().lower() not in requested_lookup]
    return {
        "requested_roles": requested,
        "executed_roles": executed,
        "dropped_roles": dropped,
        "added_roles": added,
        "role_mismatch": bool(dropped or added),
    }


def _execution_lane_catalog(task: Dict[str, Any]) -> List[str]:
    lane_states = task.get("lane_states") if isinstance(task.get("lane_states"), dict) else {}
    execution_rows = lane_states.get("execution") if isinstance(lane_states.get("execution"), list) else []
    lane_ids = [str(row.get("lane_id", "")).strip()[:32] for row in execution_rows if isinstance(row, dict) and str(row.get("lane_id", "")).strip()]
    if lane_ids:
        return lane_ids
    plan = task.get("plan") if isinstance(task.get("plan"), dict) else {}
    meta = plan.get("meta") if isinstance(plan.get("meta"), dict) else {}
    exec_plan = meta.get("phase2_execution_plan") if isinstance(meta.get("phase2_execution_plan"), dict) else {}
    execution_lanes = exec_plan.get("execution_lanes") if isinstance(exec_plan.get("execution_lanes"), list) else []
    return [str(row.get("lane_id", "")).strip()[:32] for row in execution_lanes if isinstance(row, dict) and str(row.get("lane_id", "")).strip()]


def _derive_exec_critic_lane_targets(task: Dict[str, Any], critic: Dict[str, Any]) -> Dict[str, List[str]]:
    lane_states = task.get("lane_states") if isinstance(task.get("lane_states"), dict) else {}
    review_rows = lane_states.get("review") if isinstance(lane_states.get("review"), list) else []
    if not review_rows:
        return {
            "rerun_execution_lane_ids": [],
            "rerun_review_lane_ids": [],
            "manual_followup_execution_lane_ids": [],
            "manual_followup_review_lane_ids": [],
        }

    explicit_rerun_exec = [str(x).strip()[:32] for x in (critic.get("rerun_execution_lane_ids") or []) if str(x).strip()]
    explicit_rerun_review = [str(x).strip()[:32] for x in (critic.get("rerun_review_lane_ids") or []) if str(x).strip()]
    explicit_manual_exec = [str(x).strip()[:32] for x in (critic.get("manual_followup_execution_lane_ids") or []) if str(x).strip()]
    explicit_manual_review = [str(x).strip()[:32] for x in (critic.get("manual_followup_review_lane_ids") or []) if str(x).strip()]

    review_done_or_failed = [
        row for row in review_rows
        if isinstance(row, dict) and str(row.get("status", "")).strip().lower() in {"done", "failed", "running"}
    ]
    if not review_done_or_failed:
        review_done_or_failed = [row for row in review_rows if isinstance(row, dict)]

    derived_review_lane_ids = [str(row.get("lane_id", "")).strip()[:32] for row in review_done_or_failed if str(row.get("lane_id", "")).strip()]
    derived_exec_lane_ids: List[str] = []
    for row in review_done_or_failed:
        if not isinstance(row, dict):
            continue
        for lane_id in (row.get("depends_on") or []):
            token = str(lane_id).strip()[:32]
            if token and token not in derived_exec_lane_ids:
                derived_exec_lane_ids.append(token)
    if not derived_exec_lane_ids:
        derived_exec_lane_ids = _execution_lane_catalog(task)

    return {
        "rerun_execution_lane_ids": explicit_rerun_exec or derived_exec_lane_ids,
        "rerun_review_lane_ids": explicit_rerun_review or derived_review_lane_ids,
        "manual_followup_execution_lane_ids": explicit_manual_exec or derived_exec_lane_ids,
        "manual_followup_review_lane_ids": explicit_manual_review or derived_review_lane_ids,
    }


def apply_review_lane_verdicts(task: Dict[str, Any], critic: Optional[Dict[str, Any]] = None) -> None:
    lane_states = task.get("lane_states")
    if not isinstance(lane_states, dict):
        return
    review_rows = lane_states.get("review")
    if not isinstance(review_rows, list) or not review_rows:
        return

    critic_data = critic if isinstance(critic, dict) else (
        task.get("exec_critic") if isinstance(task.get("exec_critic"), dict) else {}
    )
    verdict = _normalize_lane_verdict(critic_data.get("verdict"))
    action = str(critic_data.get("action", "")).strip().lower()
    reason = str(critic_data.get("reason", critic_data.get("fix", "")) or "").strip()[:240]

    if not verdict:
        for row in review_rows:
            if not isinstance(row, dict):
                continue
            row.pop("verdict", None)
            row.pop("action", None)
        summary = lane_states.get("summary")
        if isinstance(summary, dict):
            summary.pop("review_verdicts", None)
        return

    applied = False
    for row in review_rows:
        if not isinstance(row, dict):
            continue
        row["verdict"] = verdict
        if action:
            row["action"] = action[:32]
        else:
            row.pop("action", None)
        if reason and str(row.get("status", "")).strip().lower() in {"done", "failed", "running"}:
            row["reason"] = reason
        applied = True

    if applied:
        summary = lane_states.get("summary")
        if not isinstance(summary, dict):
            summary = {}
            lane_states["summary"] = summary
        summary["review_verdicts"] = _lane_verdict_counts(review_rows)


def derive_lane_states(
    task: Dict[str, Any],
    snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    plan = task.get("plan") if isinstance(task.get("plan"), dict) else {}
    meta = plan.get("meta") if isinstance(plan.get("meta"), dict) else {}
    exec_plan = meta.get("phase2_execution_plan") if isinstance(meta.get("phase2_execution_plan"), dict) else {}
    execution_lanes = exec_plan.get("execution_lanes") if isinstance(exec_plan.get("execution_lanes"), list) else []
    review_lanes = exec_plan.get("review_lanes") if isinstance(exec_plan.get("review_lanes"), list) else []

    if not execution_lanes and not review_lanes:
        return {}

    role_status: Dict[str, str] = {}
    for row in snapshot.get("rows") or []:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", "")).strip()
        if not role:
            continue
        role_status[role] = _merge_role_status(role_status.get(role, "pending"), row.get("status"))

    complete = bool(snapshot.get("complete", False))
    pending_roles = {str(x).strip() for x in (snapshot.get("pending_roles") or []) if str(x).strip()}
    done_roles = {str(x).strip() for x in (snapshot.get("done_roles") or []) if str(x).strip()}
    failed_roles = {str(x).strip() for x in (snapshot.get("failed_roles") or []) if str(x).strip()}

    def execution_status_for(role: str) -> Tuple[str, str]:
        current = role_status.get(role, "pending")
        if role in failed_roles or current == "failed":
            return "failed", "lane role failed"
        if role in done_roles or current == "done":
            return "done", ""
        if current == "running":
            return "running", ""
        if complete and role in pending_roles:
            return "failed", "request completed before lane finished"
        return "pending", ""

    execution_rows: List[Dict[str, Any]] = []
    execution_status_by_lane: Dict[str, str] = {}
    for row in execution_lanes:
        if not isinstance(row, dict):
            continue
        lane_id = str(row.get("lane_id", "")).strip()
        if not lane_id:
            continue
        role = str(row.get("role", "")).strip() or "Worker"
        status, reason = execution_status_for(role)
        item: Dict[str, Any] = {
            "lane_id": lane_id,
            "role": role,
            "status": status,
            "parallel": bool(row.get("parallel", True)),
        }
        subtask_ids = [str(x).strip() for x in (row.get("subtask_ids") or []) if str(x).strip()]
        if subtask_ids:
            item["subtask_ids"] = subtask_ids
        if reason:
            item["reason"] = reason
        execution_rows.append(item)
        execution_status_by_lane[lane_id] = status

    review_rows_out: List[Dict[str, Any]] = []
    for row in review_lanes:
        if not isinstance(row, dict):
            continue
        lane_id = str(row.get("lane_id", "")).strip()
        if not lane_id:
            continue
        role = str(row.get("role", "")).strip() or "Reviewer"
        depends = [str(x).strip() for x in (row.get("depends_on") or []) if str(x).strip()]
        waiting_on = [
            lane for lane in depends if execution_status_by_lane.get(lane, "pending") not in {"done"}
        ]
        if waiting_on:
            failed_waiting = [lane for lane in waiting_on if execution_status_by_lane.get(lane) == "failed"]
            reason = (
                "waiting on failed execution lane(s): " + ", ".join(failed_waiting)
                if failed_waiting
                else "waiting on execution lane(s): " + ", ".join(waiting_on)
            )
            status = "waiting_on_dependencies"
        else:
            status, reason = execution_status_for(role)
        item = {
            "lane_id": lane_id,
            "role": role,
            "kind": str(row.get("kind", "")).strip() or "verifier",
            "status": status,
            "parallel": bool(row.get("parallel", True)),
        }
        if depends:
            item["depends_on"] = depends
        if waiting_on:
            item["waiting_on"] = waiting_on
        if reason:
            item["reason"] = reason
        review_rows_out.append(item)

    return {
        "execution": execution_rows,
        "review": review_rows_out,
        "summary": {
            "execution": _lane_state_counts(execution_rows),
            "review": _lane_state_counts(review_rows_out),
        },
    }


def refresh_task_tf_state(task: Dict[str, Any]) -> None:
    task["tf_phase"] = normalize_tf_phase(derive_tf_phase(task), "queued")
    reason = derive_tf_phase_reason(task)
    if reason:
        task["tf_phase_reason"] = reason
    else:
        task.pop("tf_phase_reason", None)


def apply_exec_critic_lifecycle(
    task: Dict[str, Any],
    critic: Dict[str, Any],
    *,
    lifecycle_set_stage: Callable[..., None],
) -> None:
    task["exec_critic"] = dict(critic or {})
    verdict = str((critic or {}).get("verdict", "")).strip().lower()
    action = str((critic or {}).get("action", "")).strip().lower()
    reason = str((critic or {}).get("reason", "")).strip()[:240]
    lane_targets = _derive_exec_critic_lane_targets(task, critic if isinstance(critic, dict) else {})
    if verdict == "retry":
        task["exec_critic"]["rerun_execution_lane_ids"] = list(lane_targets["rerun_execution_lane_ids"])
        task["exec_critic"]["rerun_review_lane_ids"] = list(lane_targets["rerun_review_lane_ids"])
        task["exec_critic"].pop("manual_followup_execution_lane_ids", None)
        task["exec_critic"].pop("manual_followup_review_lane_ids", None)
    elif verdict in {"fail", "intervention"}:
        task["exec_critic"]["manual_followup_execution_lane_ids"] = list(lane_targets["manual_followup_execution_lane_ids"])
        task["exec_critic"]["manual_followup_review_lane_ids"] = list(lane_targets["manual_followup_review_lane_ids"])
        task["exec_critic"].pop("rerun_execution_lane_ids", None)
        task["exec_critic"].pop("rerun_review_lane_ids", None)
    else:
        task["exec_critic"].pop("rerun_execution_lane_ids", None)
        task["exec_critic"].pop("rerun_review_lane_ids", None)
        task["exec_critic"].pop("manual_followup_execution_lane_ids", None)
        task["exec_critic"].pop("manual_followup_review_lane_ids", None)

    if verdict == "success":
        lifecycle_set_stage(task=task, stage="integration", status="done", note="exec critic approved")
        close_state = str(((task.get("stages") or {}).get("close", "pending"))).strip().lower()
        if close_state != "done":
            lifecycle_set_stage(task=task, stage="close", status="running", note="awaiting final result packaging")
    elif verdict == "retry":
        if action == "replan":
            lifecycle_set_stage(task=task, stage="planning", status="running", note=reason or "critic requested replan")
        else:
            lifecycle_set_stage(task=task, stage="execution", status="running", note=reason or "critic requested retry")
        lifecycle_set_stage(task=task, stage="integration", status="running", note=reason or f"critic requested {action or 'retry'}")
        lifecycle_set_stage(task=task, stage="close", status="pending")
    elif verdict in {"fail", "intervention"}:
        lifecycle_set_stage(task=task, stage="integration", status="failed", note=reason or verdict)
        lifecycle_set_stage(task=task, stage="close", status="failed", note=reason or verdict)

    apply_review_lane_verdicts(task, critic)
    refresh_task_tf_state(task)


def sanitize_task_record(
    raw_task: Dict[str, Any],
    req_id: str,
    *,
    dedupe_roles: Callable[[Iterable[str]], List[str]],
    lifecycle_stages: Iterable[str],
    normalize_stage_status: Callable[[Any], str],
    normalize_task_status: Callable[[Any], str],
    now_iso: Callable[[], str],
    history_limit: int,
    normalize_task_plan_schema: Callable[..., Dict[str, Any]],
    normalize_plan_critic_payload: Callable[..., Dict[str, Any]],
    normalize_plan_replans_payload: Callable[..., List[Dict[str, Any]]],
    plan_critic_primary_issue: Callable[..., str],
    normalize_exec_critic_payload: Callable[..., Dict[str, Any]],
    build_task_context: Callable[..., Dict[str, str]],
) -> Dict[str, Any]:
    task = dict(raw_task or {})
    rid = str(req_id or task.get("request_id", "")).strip()
    task["request_id"] = rid
    task["mode"] = str(task.get("mode", "dispatch")).strip().lower() or "dispatch"
    if task["mode"] not in {"dispatch", "direct"}:
        task["mode"] = "dispatch"
    task["prompt"] = str(task.get("prompt", "")).strip()
    task["roles"] = dedupe_roles(task.get("roles") or [])
    task["verifier_roles"] = dedupe_roles(task.get("verifier_roles") or [])
    task["require_verifier"] = bool(task.get("require_verifier", False))

    stage_names = tuple(lifecycle_stages)
    raw_stages = task.get("stages")
    stages: Dict[str, str] = {}
    if isinstance(raw_stages, dict):
        for stage_name in stage_names:
            stages[stage_name] = normalize_stage_status(raw_stages.get(stage_name, "pending"))
    else:
        for stage_name in stage_names:
            stages[stage_name] = "pending"
    task["stages"] = stages

    stage = str(task.get("stage", "")).strip().lower()
    if stage not in stage_names:
        stage = "intake"
        for stage_name in stage_names:
            if stages.get(stage_name) in {"running", "done", "failed"}:
                stage = stage_name
    task["stage"] = stage

    history_in = task.get("history")
    history: List[Dict[str, Any]] = []
    if isinstance(history_in, list):
        for item in history_in[-int(history_limit) :]:
            if not isinstance(item, dict):
                continue
            row_stage = str(item.get("stage", "")).strip().lower()
            if row_stage not in stage_names:
                continue
            row_status = normalize_stage_status(item.get("status", "pending"))
            row: Dict[str, Any] = {
                "at": str(item.get("at", "")).strip() or now_iso(),
                "stage": row_stage,
                "status": row_status,
            }
            note = str(item.get("note", "")).strip()
            if note:
                row["note"] = note[:400]
            history.append(row)
    task["history"] = history

    task["status"] = normalize_task_status(task.get("status", "pending"))
    task["created_at"] = str(task.get("created_at", "")).strip() or now_iso()
    task["updated_at"] = str(task.get("updated_at", "")).strip() or now_iso()
    result = task.get("result")
    task["result"] = result if isinstance(result, dict) else {}

    short_id = str(task.get("short_id", "")).strip().upper()
    alias = str(task.get("alias", "")).strip()
    if short_id:
        task["short_id"] = short_id
    if alias:
        task["alias"] = alias

    control_mode = str(task.get("control_mode", "")).strip().lower()
    if control_mode:
        task["control_mode"] = control_mode[:32]
    source_request_id = str(task.get("source_request_id", "")).strip()
    if source_request_id:
        task["source_request_id"] = source_request_id[:128]
    retry_of = str(task.get("retry_of", "")).strip()
    if retry_of:
        task["retry_of"] = retry_of[:128]
    replan_of = str(task.get("replan_of", "")).strip()
    if replan_of:
        task["replan_of"] = replan_of[:128]

    for child_key in ("retry_children", "replan_children"):
        raw_children = task.get(child_key)
        if isinstance(raw_children, list):
            normalized_children = []
            seen_children: Set[str] = set()
            for item in raw_children:
                token = str(item or "").strip()
                if not token or token in seen_children:
                    continue
                seen_children.add(token)
                normalized_children.append(token[:128])
            if normalized_children:
                task[child_key] = normalized_children

    initiator_chat_id = str(task.get("initiator_chat_id", "")).strip()
    if initiator_chat_id:
        task["initiator_chat_id"] = initiator_chat_id[:64]
    todo_id = str(task.get("todo_id", "")).strip()
    if todo_id:
        task["todo_id"] = todo_id[:64]

    todo_priority = str(task.get("todo_priority", "")).strip().upper()
    if todo_priority in {"P1", "P2", "P3"}:
        task["todo_priority"] = todo_priority
    todo_status = str(task.get("todo_status", "")).strip().lower()
    if todo_status:
        task["todo_status"] = todo_status[:32]

    plan = task.get("plan")
    if isinstance(plan, dict):
        workers = []
        raw_meta = plan.get("meta")
        if isinstance(raw_meta, dict) and isinstance(raw_meta.get("worker_roles"), list):
            for row in raw_meta.get("worker_roles") or []:
                token = str(row or "").strip()
                if token and token not in workers:
                    workers.append(token)
        if not workers:
            workers = dedupe_roles((task.get("plan_roles") or []) + (task.get("roles") or [])) or ["Worker"]
        max_subtasks = 0
        raw_subtasks = plan.get("subtasks")
        if isinstance(raw_subtasks, list):
            max_subtasks = len(raw_subtasks)
        task["plan"] = normalize_task_plan_schema(
            plan,
            user_prompt=str(task.get("prompt", "")).strip(),
            workers=workers,
            max_subtasks=max_subtasks or 4,
        )
    plan_critic = task.get("plan_critic")
    if isinstance(plan_critic, dict):
        task["plan_critic"] = normalize_plan_critic_payload(plan_critic, max_items=8)
    plan_roles = task.get("plan_roles")
    if isinstance(plan_roles, list):
        task["plan_roles"] = dedupe_roles(plan_roles)
    plan_replans = task.get("plan_replans")
    if isinstance(plan_replans, list):
        task["plan_replans"] = normalize_plan_replans_payload(plan_replans, keep=history_limit)
    if isinstance(task.get("plan_gate_passed"), bool):
        task["plan_gate_passed"] = bool(task.get("plan_gate_passed"))
    plan_gate_reason = str(task.get("plan_gate_reason", "")).strip()
    if plan_gate_reason:
        task["plan_gate_reason"] = plan_gate_reason[:240]
    elif task.get("plan_gate_passed") is False and isinstance(task.get("plan_critic"), dict):
        lead_issue = plan_critic_primary_issue(task["plan_critic"], limit=240)
        if lead_issue:
            task["plan_gate_reason"] = lead_issue

    exec_critic = task.get("exec_critic")
    if isinstance(exec_critic, dict):
        task["exec_critic"] = normalize_exec_critic_payload(
            exec_critic,
            attempt_no=int(exec_critic.get("attempt", 1) or 1),
            max_attempts=int(exec_critic.get("max_attempts", 1) or 1),
            at=str(exec_critic.get("at", "")).strip() or now_iso(),
        )

    lane_states = task.get("lane_states")
    if isinstance(lane_states, dict):
        execution_rows = _normalize_lane_state_rows(lane_states.get("execution"), kind="execution")
        review_rows = _normalize_lane_state_rows(lane_states.get("review"), kind="review")
        if execution_rows or review_rows:
            task["lane_states"] = {
                "execution": execution_rows,
                "review": review_rows,
                "summary": {
                    "execution": _lane_state_counts(execution_rows),
                    "review": _lane_state_counts(review_rows),
                },
            }
            apply_review_lane_verdicts(task)
        else:
            task.pop("lane_states", None)

    context = build_task_context(
        request_id=rid,
        task=task,
        extra=(task.get("context") if isinstance(task.get("context"), dict) else None),
    )
    if context:
        task["context"] = context

    result = task.get("result")
    if isinstance(result, dict):
        role_snapshot = derive_role_execution_snapshot(
            result.get("requested_roles") or task.get("roles") or [],
            result.get("executed_roles") or result.get("done_roles") or task.get("roles") or [],
            dedupe_roles=dedupe_roles,
        )
        result.update(role_snapshot)

    refresh_task_tf_state(task)

    return task


def ensure_project_tasks(entry: Dict[str, Any]) -> Dict[str, Any]:
    tasks = entry.get("tasks")
    if not isinstance(tasks, dict):
        tasks = {}
        entry["tasks"] = tasks
    return tasks


def normalize_task_alias_key(raw: str) -> str:
    src = str(raw or "").strip().lower()
    out: List[str] = []
    sep = False
    for ch in src:
        if ch.isalnum():
            out.append(ch)
            sep = False
        else:
            if not sep:
                out.append("-")
                sep = True
    return "".join(out).strip("-")


def parse_task_seq_from_short_id(short_id: str) -> int:
    src = str(short_id or "").strip().upper()
    if not src.startswith("T-"):
        return 0
    tail = src[2:]
    return int(tail) if tail.isdigit() else 0


def format_task_short_id(seq: int) -> str:
    value = max(1, int(seq))
    return f"T-{value:03d}" if value < 1000 else f"T-{value}"


def derive_task_alias_base(prompt: str) -> str:
    src = str(prompt or "").strip()
    if not src:
        return "task"

    cleaned: List[str] = []
    for ch in src:
        if ch.isalnum() or ch in {" ", "-", "_"}:
            cleaned.append(ch)
        else:
            cleaned.append(" ")

    tokens = [t.lower() for t in "".join(cleaned).split() if t]
    if not tokens:
        return "task"

    stop = {
        "the",
        "a",
        "an",
        "to",
        "for",
        "and",
        "or",
        "of",
        "해주세요",
        "해줘",
        "요청",
        "작업",
        "진행",
        "지금",
        "바로",
        "좀",
    }
    picked = [t for t in tokens if t not in stop] or tokens

    alias = "-".join(picked[:5]).strip("-_")
    if len(alias) > 48:
        alias = alias[:48].rstrip("-_")
    return alias or "task"


def ensure_task_alias_meta(entry: Dict[str, Any]) -> Tuple[Dict[str, str], int]:
    raw_index = entry.get("task_alias_index")
    if not isinstance(raw_index, dict):
        raw_index = {}
        entry["task_alias_index"] = raw_index

    alias_index: Dict[str, str] = {}
    for key, rid in raw_index.items():
        key_norm = normalize_task_alias_key(str(key or ""))
        rid_norm = str(rid or "").strip()
        if key_norm and rid_norm:
            alias_index[key_norm] = rid_norm
    entry["task_alias_index"] = alias_index

    raw_seq = entry.get("task_seq")
    try:
        seq = max(0, int(raw_seq or 0))
    except Exception:
        seq = 0
    entry["task_seq"] = seq
    return alias_index, seq


def rebuild_task_alias_index(entry: Dict[str, Any]) -> None:
    tasks = ensure_project_tasks(entry)
    _, seq = ensure_task_alias_meta(entry)

    alias_index: Dict[str, str] = {}
    max_seq = max(0, int(seq))

    for req_id, task in tasks.items():
        rid = str(req_id or "").strip()
        if not rid or not isinstance(task, dict):
            continue

        short_id = str(task.get("short_id", "")).strip().upper()
        alias = str(task.get("alias", "")).strip()

        if short_id:
            alias_index[normalize_task_alias_key(short_id)] = rid
            max_seq = max(max_seq, parse_task_seq_from_short_id(short_id))
        if alias:
            alias_index[normalize_task_alias_key(alias)] = rid

    entry["task_alias_index"] = alias_index
    entry["task_seq"] = max_seq


def assign_task_alias(
    entry: Dict[str, Any],
    task: Dict[str, Any],
    prompt: str,
    *,
    rebuild_index: bool = True,
) -> None:
    alias_index, seq = ensure_task_alias_meta(entry)

    req_id = str(task.get("request_id", "")).strip()
    if not req_id:
        return

    short_id = str(task.get("short_id", "")).strip().upper()
    if not short_id:
        next_seq = max(seq, 0)
        while True:
            next_seq += 1
            candidate = format_task_short_id(next_seq)
            key = normalize_task_alias_key(candidate)
            owner = alias_index.get(key)
            if not owner or owner == req_id:
                short_id = candidate
                task["short_id"] = short_id
                entry["task_seq"] = next_seq
                break

    alias = str(task.get("alias", "")).strip()
    if not alias:
        base = derive_task_alias_base(prompt or str(task.get("prompt", "")).strip() or short_id.lower())
        candidate = base
        suffix = 2
        while True:
            key = normalize_task_alias_key(candidate)
            owner = alias_index.get(key)
            if not owner or owner == req_id:
                alias = candidate
                task["alias"] = alias
                break
            candidate = f"{base}-{suffix}"
            suffix += 1

    if rebuild_index:
        rebuild_task_alias_index(entry)


def backfill_task_aliases(entry: Dict[str, Any]) -> None:
    tasks = ensure_project_tasks(entry)
    if not tasks:
        ensure_task_alias_meta(entry)
        return

    rows = sorted(tasks.items(), key=lambda kv: str((kv[1] or {}).get("created_at", "")))
    for req_id, task in rows:
        if not isinstance(task, dict):
            continue
        rid = str(req_id or "").strip()
        if not rid:
            continue
        if not str(task.get("request_id", "")).strip():
            task["request_id"] = rid
        assign_task_alias(entry, task, prompt=str(task.get("prompt", "")), rebuild_index=False)

    rebuild_task_alias_index(entry)


def resolve_task_request_id(entry: Dict[str, Any], request_or_alias: str) -> str:
    token = str(request_or_alias or "").strip()
    if not token:
        return ""

    tasks = ensure_project_tasks(entry)
    if token in tasks:
        return token

    alias_index, _ = ensure_task_alias_meta(entry)
    if not alias_index and tasks:
        backfill_task_aliases(entry)
        alias_index, _ = ensure_task_alias_meta(entry)

    norm = normalize_task_alias_key(token)
    mapped = alias_index.get(norm, "")
    if mapped and mapped in tasks:
        return mapped

    for rid, task in tasks.items():
        if not isinstance(task, dict):
            continue
        short_id = str(task.get("short_id", "")).strip().upper()
        alias = str(task.get("alias", "")).strip()
        if token.upper() == short_id:
            return rid
        if norm and norm == normalize_task_alias_key(alias):
            return rid

    return token


def latest_task_request_refs(entry: Dict[str, Any], limit: int = 12) -> List[str]:
    tasks = ensure_project_tasks(entry)
    if not tasks:
        return []
    backfill_task_aliases(entry)
    rows = sorted(tasks.items(), key=lambda kv: str((kv[1] or {}).get("updated_at", "")), reverse=True)
    cap = max(1, min(50, int(limit)))
    out: List[str] = []
    for req_id, task in rows[:cap]:
        if isinstance(task, dict):
            rid = str(req_id or "").strip()
            if rid:
                out.append(rid)
    return out


def trim_project_tasks(tasks: Dict[str, Any], keep: int) -> None:
    if len(tasks) <= int(keep):
        return
    ordered = sorted(tasks.items(), key=lambda kv: str((kv[1] or {}).get("updated_at", "")), reverse=True)
    keep_keys = {key for key, _ in ordered[: max(1, int(keep))]}
    for key in list(tasks.keys()):
        if key not in keep_keys:
            tasks.pop(key, None)


def get_task_record(entry: Dict[str, Any], request_id: str) -> Optional[Dict[str, Any]]:
    token = resolve_task_request_id(entry, request_id)
    if not token:
        return None
    tasks = ensure_project_tasks(entry)
    item = tasks.get(token)
    return item if isinstance(item, dict) else None


def ensure_task_record(
    entry: Dict[str, Any],
    *,
    request_id: str,
    prompt: str,
    mode: str,
    roles: List[str],
    verifier_roles: List[str],
    require_verifier: bool,
    now_iso: Callable[[], str],
    dedupe_roles: Callable[[Iterable[str]], List[str]],
    build_task_context: Callable[..., Dict[str, str]],
    lifecycle_stages: Iterable[str],
    keep_limit: int,
) -> Dict[str, Any]:
    token = str(request_id or "").strip()
    tasks = ensure_project_tasks(entry)
    now = now_iso()

    item = tasks.get(token)
    if not isinstance(item, dict):
        item = {
            "request_id": token,
            "mode": mode,
            "prompt": prompt.strip(),
            "roles": dedupe_roles(roles),
            "verifier_roles": dedupe_roles(verifier_roles),
            "require_verifier": bool(require_verifier),
            "status": "running",
            "stage": "intake",
            "stages": {name: "pending" for name in lifecycle_stages},
            "history": [],
            "created_at": now,
            "updated_at": now,
            "result": {},
        }
        tasks[token] = item
    else:
        if prompt:
            item["prompt"] = prompt.strip()
        if mode:
            item["mode"] = mode
        if roles:
            item["roles"] = dedupe_roles(roles)
        if verifier_roles:
            item["verifier_roles"] = dedupe_roles(verifier_roles)
        item["require_verifier"] = bool(require_verifier)
        item["updated_at"] = now

    assign_task_alias(entry, item, prompt=prompt, rebuild_index=False)
    item["context"] = build_task_context(request_id=token, entry=entry, task=item)
    trim_project_tasks(tasks, keep=keep_limit)
    rebuild_task_alias_index(entry)
    return item


def lifecycle_set_stage(
    task: Dict[str, Any],
    *,
    stage: str,
    status: str,
    note: str = "",
    lifecycle_stages: Iterable[str],
    normalize_stage_status: Callable[[Any], str],
    now_iso: Callable[[], str],
    history_limit: int,
) -> None:
    stage_names = tuple(lifecycle_stages)
    if stage not in stage_names:
        return

    stages = task.get("stages")
    if not isinstance(stages, dict):
        stages = {name: "pending" for name in stage_names}
        task["stages"] = stages

    prev = str(stages.get(stage, "pending"))
    next_status = normalize_stage_status(status or "pending")
    if prev == next_status and not note:
        return

    stages[stage] = next_status
    task["stage"] = stage

    history = task.get("history")
    if not isinstance(history, list):
        history = []

    event: Dict[str, Any] = {"at": now_iso(), "stage": stage, "status": next_status}
    if note:
        event["note"] = note
    history.append(event)
    if len(history) > int(history_limit):
        history = history[-int(history_limit) :]

    task["history"] = history
    task["updated_at"] = event["at"]


def summarize_task_monitor(
    project_name: str,
    entry: Dict[str, Any],
    *,
    limit: int,
    normalize_task_status: Callable[[Any], str],
    dedupe_roles: Callable[[Iterable[str]], List[str]],
    task_display_label: Callable[[Dict[str, Any], str], str],
    lifecycle_stages: Iterable[str],
) -> str:
    tasks = ensure_project_tasks(entry)
    if not tasks:
        return f"orch: {project_name}\n작업이 없습니다."

    backfill_task_aliases(entry)
    rows = sorted(tasks.items(), key=lambda kv: str((kv[1] or {}).get("updated_at", "")), reverse=True)
    cap = max(1, min(50, int(limit)))
    stage_names = tuple(lifecycle_stages)

    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0}
    invalid_stage_rows = 0
    for _, task in rows:
        if not isinstance(task, dict):
            continue
        status = normalize_task_status(task.get("status", "pending"))
        counts[status] = counts.get(status, 0) + 1
        stage = str(task.get("stage", "")).strip().lower()
        if stage and stage not in stage_names:
            invalid_stage_rows += 1

    lines = [
        f"orch: {project_name}",
        f"task monitor: latest {cap}",
        "format: label | status/stage | roles | updated",
        "summary: total={total} running={running} completed={completed} failed={failed} pending={pending}".format(
            total=len(rows),
            running=counts.get("running", 0),
            completed=counts.get("completed", 0),
            failed=counts.get("failed", 0),
            pending=counts.get("pending", 0),
        ),
    ]
    if invalid_stage_rows:
        lines.append(f"warning: invalid lifecycle stage rows={invalid_stage_rows}")

    for idx, (req_id, task) in enumerate(rows[:cap], start=1):
        if not isinstance(task, dict):
            continue
        label = task_display_label(task, str(req_id or "").strip())
        status = normalize_task_status(task.get("status", "pending"))
        stage = str(task.get("stage", "pending")).strip().lower() or "pending"
        if stage not in stage_names:
            stage = "pending"
        tf_phase = normalize_tf_phase(derive_tf_phase(task), "queued")
        roles = dedupe_roles(task.get("roles") or [])
        role_text = ", ".join(roles[:2])
        if len(roles) > 2:
            role_text += f" +{len(roles) - 2}"
        plan = task.get("plan") if isinstance(task.get("plan"), dict) else {}
        meta = plan.get("meta") if isinstance(plan.get("meta"), dict) else {}
        exec_plan = meta.get("phase2_execution_plan") if isinstance(meta.get("phase2_execution_plan"), dict) else {}
        exec_lanes = exec_plan.get("execution_lanes") if isinstance(exec_plan.get("execution_lanes"), list) else []
        review_lanes = exec_plan.get("review_lanes") if isinstance(exec_plan.get("review_lanes"), list) else []
        lane_text = ""
        if exec_lanes or review_lanes:
            lane_text = f" | lanes E{len(exec_lanes)}/R{len(review_lanes)}"
        lane_states = task.get("lane_states") if isinstance(task.get("lane_states"), dict) else {}
        lane_summary = lane_states.get("summary") if isinstance(lane_states.get("summary"), dict) else {}
        exec_summary = lane_summary.get("execution") if isinstance(lane_summary.get("execution"), dict) else {}
        review_summary = lane_summary.get("review") if isinstance(lane_summary.get("review"), dict) else {}
        review_verdicts = lane_summary.get("review_verdicts") if isinstance(lane_summary.get("review_verdicts"), dict) else {}
        lane_parts: List[str] = []
        if exec_summary:
            lane_parts.append("exec " + ",".join(f"{key}={value}" for key, value in sorted(exec_summary.items())))
        if review_summary:
            lane_parts.append("review " + ",".join(f"{key}={value}" for key, value in sorted(review_summary.items())))
        if review_verdicts:
            lane_parts.append("review_verdict " + ",".join(f"{key}={value}" for key, value in sorted(review_verdicts.items())))
        if lane_parts:
            lane_text += " [" + " | ".join(lane_parts) + "]"
        lane_targets = task_lane_target_snapshot(task)
        rerun_exec = list(lane_targets.get("rerun_execution_lane_ids") or [])
        rerun_review = list(lane_targets.get("rerun_review_lane_ids") or [])
        manual_exec = list(lane_targets.get("manual_followup_execution_lane_ids") or [])
        manual_review = list(lane_targets.get("manual_followup_review_lane_ids") or [])
        result = task.get("result") if isinstance(task.get("result"), dict) else {}
        dropped_roles = [str(x).strip() for x in (result.get("dropped_roles") or []) if str(x).strip()]
        added_roles = [str(x).strip() for x in (result.get("added_roles") or []) if str(x).strip()]
        target_parts: List[str] = []
        if rerun_exec or rerun_review:
            target_parts.append(
                "rerun E:{exec_ids} R:{review_ids}".format(
                    exec_ids=",".join(rerun_exec) if rerun_exec else "-",
                    review_ids=",".join(rerun_review) if rerun_review else "-",
                )
            )
        if manual_exec or manual_review:
            target_parts.append(
                "followup E:{exec_ids} R:{review_ids}".format(
                    exec_ids=",".join(manual_exec) if manual_exec else "-",
                    review_ids=",".join(manual_review) if manual_review else "-",
                )
            )
        if bool(result.get("role_mismatch", False)):
            target_parts.append(
                "roles drop:{dropped} add:{added}".format(
                    dropped=",".join(dropped_roles) if dropped_roles else "-",
                    added=",".join(added_roles) if added_roles else "-",
                )
            )
        if target_parts:
            lane_text += " {" + " | ".join(target_parts) + "}"
        updated = str(task.get("updated_at", "")).strip() or "-"
        lines.append(f"- {idx}. {label} | {status}/{stage}/{tf_phase} | {role_text or '-'}{lane_text} | {updated}")
        priority_action = task_priority_action_snapshot(
            label=label,
            tf_phase=tf_phase,
            rerun_execution_lane_ids=rerun_exec,
            rerun_review_lane_ids=rerun_review,
            manual_followup_execution_lane_ids=manual_exec,
            manual_followup_review_lane_ids=manual_review,
        )
        first_action = str(priority_action.get("action", "")).strip()
        if first_action:
            lines.append(f"  first: {first_action} | {str(priority_action.get('reason', '')).strip() or '-'}")

    lines.append("")
    lines.append("alias map (number/label -> request_id):")
    for idx, (req_id, task) in enumerate(rows[:cap], start=1):
        if not isinstance(task, dict):
            continue
        lines.append(f"- {idx}. {task_display_label(task, str(req_id or '').strip())} -> {req_id}")
    lines.append("")
    lines.append(
        "quick actions: /check <번호|label> /task <번호|label> "
        "/retry <번호|label> [lane <L#|R#>] /replan <번호|label> [lane <L#|R#>] /cancel <번호|label>"
    )
    return "\n".join(lines)


def normalize_role_rows(data: Dict[str, Any], *, dedupe_roles: Callable[[Iterable[str]], List[str]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    role_states = data.get("role_states")
    if isinstance(role_states, list):
        for item in role_states:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            if not role:
                continue
            status = str(item.get("status", "pending")).strip().lower() or "pending"
            rows.append({"role": role, "status": status})

    if rows:
        return rows

    roles_obj = data.get("roles")
    if isinstance(roles_obj, list) and roles_obj and isinstance(roles_obj[0], dict):
        for item in roles_obj:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            if not role:
                continue
            status = str(item.get("status", "pending")).strip().lower() or "pending"
            rows.append({"role": role, "status": status})
        if rows:
            return rows

    done_set = {str(x).strip() for x in (data.get("done_roles") or []) if str(x).strip()}
    failed_set = {str(x).strip() for x in (data.get("failed_roles") or []) if str(x).strip()}
    pending_set = {
        str(x).strip()
        for x in (data.get("pending_roles") or data.get("unresolved_roles") or [])
        if str(x).strip()
    }

    if isinstance(roles_obj, list):
        for item in roles_obj:
            role = str(item).strip()
            if not role:
                continue
            if role in failed_set:
                status = "failed"
            elif role in done_set:
                status = "done"
            elif role in pending_set:
                status = "pending"
            else:
                status = "pending"
            rows.append({"role": role, "status": status})
        if rows:
            return rows

    all_roles = dedupe_roles(list(done_set) + list(failed_set) + list(pending_set))
    for role in all_roles:
        if role in failed_set:
            status = "failed"
        elif role in done_set:
            status = "done"
        else:
            status = "pending"
        rows.append({"role": role, "status": status})
    return rows


def extract_request_snapshot(data: Dict[str, Any], *, dedupe_roles: Callable[[Iterable[str]], List[str]]) -> Dict[str, Any]:
    rows = normalize_role_rows(data, dedupe_roles=dedupe_roles)
    counts = data.get("counts") or {}

    assignments = int(counts.get("assignments", 0) or 0)
    replies = int(counts.get("replies", 0) or 0)
    if assignments <= 0:
        assignments = len(rows)
    if replies <= 0:
        replies = len(data.get("replies") or [])

    done_roles: Set[str] = set()
    failed_roles: Set[str] = set()
    pending_roles: Set[str] = set()

    for row in rows:
        role = str(row.get("role", "")).strip()
        status = str(row.get("status", "pending")).strip().lower()
        if not role:
            continue
        if status in {"failed", "error", "fail"}:
            failed_roles.add(role)
        elif status == "done":
            done_roles.add(role)
        else:
            pending_roles.add(role)

    for role in data.get("done_roles") or []:
        token = str(role).strip()
        if token:
            done_roles.add(token)
            pending_roles.discard(token)
            failed_roles.discard(token)

    for role in data.get("failed_roles") or []:
        token = str(role).strip()
        if token:
            failed_roles.add(token)
            done_roles.discard(token)
            pending_roles.discard(token)

    for role in data.get("pending_roles") or data.get("unresolved_roles") or []:
        token = str(role).strip()
        if token and token not in done_roles and token not in failed_roles:
            pending_roles.add(token)

    request_id = str(data.get("request_id", "")).strip()
    gateway_request_id = str(data.get("gateway_request_id", "")).strip()
    complete = bool(data.get("complete", False))
    return {
        "request_id": request_id,
        "gateway_request_id": gateway_request_id,
        "rows": rows,
        "assignments": assignments,
        "replies": replies,
        "complete": complete,
        "done_roles": sorted(done_roles),
        "failed_roles": sorted(failed_roles),
        "pending_roles": sorted(pending_roles),
    }


def sync_task_lifecycle(
    entry: Dict[str, Any],
    request_data: Dict[str, Any],
    *,
    prompt: str,
    mode: str,
    selected_roles: Optional[List[str]],
    verifier_roles: Optional[List[str]],
    require_verifier: bool,
    verifier_candidates: List[str],
    dedupe_roles: Callable[[Iterable[str]], List[str]],
    ensure_task_record: Callable[..., Dict[str, Any]],
    lifecycle_set_stage: Callable[..., None],
    normalize_task_status: Callable[[Any], str],
    sync_task_exec_context: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, str]],
) -> Optional[Dict[str, Any]]:
    snap = extract_request_snapshot(request_data, dedupe_roles=dedupe_roles)
    request_id = str(snap.get("gateway_request_id", "") or snap.get("request_id", "")).strip()
    if not request_id:
        return None

    rows = snap.get("rows") or []
    inferred_roles = [str(x.get("role", "")).strip() for x in rows if str(x.get("role", "")).strip()]
    roles = dedupe_roles(selected_roles or inferred_roles)

    verifier_keys = {str(c or "").strip().lower() for c in verifier_candidates if str(c or "").strip()}
    inferred_verifiers = [r for r in roles if r.lower() in verifier_keys]
    verifiers = dedupe_roles(verifier_roles or inferred_verifiers)

    task = ensure_task_record(
        entry=entry,
        request_id=request_id,
        prompt=prompt,
        mode=mode,
        roles=roles,
        verifier_roles=verifiers,
        require_verifier=require_verifier,
    )

    assignments = int(snap.get("assignments", 0) or 0)
    replies = int(snap.get("replies", 0) or 0)
    complete = bool(snap.get("complete", False))
    done_roles = set(str(x) for x in (snap.get("done_roles") or []))
    failed_roles = set(str(x) for x in (snap.get("failed_roles") or []))
    pending_roles = set(str(x) for x in (snap.get("pending_roles") or []))

    lifecycle_set_stage(task, "intake", "done")
    lifecycle_set_stage(task, "planning", "done")

    staffing_status = "done" if assignments > 0 else ("running" if roles else "pending")
    lifecycle_set_stage(task, "staffing", staffing_status)

    if failed_roles:
        execution_status = "failed"
    elif complete and assignments > 0 and not pending_roles:
        execution_status = "done"
    elif assignments > 0:
        execution_status = "running"
    else:
        execution_status = "pending"
    lifecycle_set_stage(task, "execution", execution_status)

    ver_note = ""
    if require_verifier:
        if not verifiers:
            verification_status = "failed"
            ver_note = "no verifier role assigned"
        elif any(v in failed_roles for v in verifiers):
            verification_status = "failed"
            ver_note = "verifier role failed"
        elif all(v in done_roles for v in verifiers):
            verification_status = "done"
        elif complete and execution_status == "done":
            verification_status = "failed"
            ver_note = "verifier gate not satisfied"
        elif execution_status in {"running", "done"}:
            verification_status = "running"
        elif execution_status == "failed":
            verification_status = "failed"
        else:
            verification_status = "pending"
    else:
        if execution_status == "done":
            verification_status = "done"
        elif execution_status == "failed":
            verification_status = "failed"
        elif execution_status == "running":
            verification_status = "running"
        else:
            verification_status = "pending"

    lifecycle_set_stage(task, "verification", verification_status, note=ver_note)

    if execution_status == "failed" or verification_status == "failed":
        integration_status = "failed"
    elif verification_status == "done" and (replies > 0 or complete):
        integration_status = "done"
    elif execution_status == "running" or verification_status == "running":
        integration_status = "running"
    else:
        integration_status = "pending"
    lifecycle_set_stage(task, "integration", integration_status)

    if integration_status == "failed":
        close_status = "failed"
    elif integration_status == "done" and complete:
        close_status = "done"
    elif execution_status == "running" or verification_status == "running":
        close_status = "running"
    else:
        close_status = "pending"
    lifecycle_set_stage(task, "close", close_status)

    if close_status == "failed" or verification_status == "failed" or execution_status == "failed":
        overall = "failed"
    elif close_status == "done":
        overall = "completed"
    elif close_status == "running" or execution_status == "running" or verification_status == "running":
        overall = "running"
    else:
        overall = "pending"

    task["status"] = normalize_task_status(overall)
    task["roles"] = roles
    task["verifier_roles"] = verifiers
    task["require_verifier"] = bool(require_verifier)
    task["result"] = {
        "assignments": assignments,
        "replies": replies,
        "complete": complete,
        "done_roles": sorted(done_roles),
        "failed_roles": sorted(failed_roles),
        "pending_roles": sorted(pending_roles),
    }
    requested_roles = request_data.get("requested_roles") if isinstance(request_data.get("requested_roles"), list) else roles
    executed_roles = request_data.get("executed_roles") if isinstance(request_data.get("executed_roles"), list) else inferred_roles
    task["result"].update(
        derive_role_execution_snapshot(
            requested_roles,
            executed_roles,
            dedupe_roles=dedupe_roles,
        )
    )
    linked_request_ids = request_data.get("linked_request_ids")
    if isinstance(linked_request_ids, list) and linked_request_ids:
        task["result"]["linked_request_ids"] = [
            str(value).strip()
            for value in linked_request_ids
            if str(value).strip()
        ]
    phase2_request_ids = request_data.get("phase2_request_ids")
    if isinstance(phase2_request_ids, dict) and phase2_request_ids:
        normalized_phase2_request_ids: Dict[str, Any] = {}
        for key, value in phase2_request_ids.items():
            bucket = str(key).strip()
            if not bucket:
                continue
            if isinstance(value, list):
                tokens = [str(item).strip() for item in value if str(item).strip()]
                if tokens:
                    normalized_phase2_request_ids[bucket] = tokens
            else:
                token = str(value).strip()
                if token:
                    normalized_phase2_request_ids[bucket] = token
        if normalized_phase2_request_ids:
            task["result"]["phase2_request_ids"] = normalized_phase2_request_ids
    if "phase2_review_triggered" in request_data:
        task["result"]["phase2_review_triggered"] = bool(request_data.get("phase2_review_triggered"))
    review_skip = str(request_data.get("phase2_review_skipped_reason", "")).strip()
    if review_skip:
        task["result"]["phase2_review_skipped_reason"] = review_skip[:240]
    lane_states = derive_lane_states(task, snap)
    if lane_states:
        task["lane_states"] = lane_states
        apply_review_lane_verdicts(task)
    else:
        task.pop("lane_states", None)
    refresh_task_tf_state(task)
    sync_task_exec_context(entry, task)
    return task
