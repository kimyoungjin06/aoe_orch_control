#!/usr/bin/env python3
"""Planning pipeline helpers for run handler orchestration."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from aoe_tg_schema import default_plan_critic_payload, normalize_plan_critic_payload, plan_critic_primary_issue


@dataclass
class DispatchModeResult:
    dispatch_mode: bool
    dispatch_roles: str


@dataclass
class PlanMeta:
    selected_roles: List[str] = field(default_factory=list)
    plan_data: Optional[Dict[str, Any]] = None
    plan_critic: Dict[str, Any] = field(default_factory=default_plan_critic_payload)
    plan_roles: List[str] = field(default_factory=list)
    plan_replans: List[Dict[str, Any]] = field(default_factory=list)
    plan_error: str = ""
    plan_gate_blocked: bool = False
    plan_gate_reason: str = ""
    planning_enabled: bool = False
    reuse_source_plan: bool = False


def apply_success_first_prompt_fallbacks(prompt: str) -> tuple[str, List[str]]:
    text = " ".join(str(prompt or "").strip().split())
    if not text:
        return text, []

    low = text.lower()
    notes: List[str] = []

    md_scope = any(token in low for token in (".md", "markdown", "마크다운", "문서", "파일")) or bool(
        re.search(r"(^|[^a-z])md($|[^a-z])", low)
    )
    created_scope = any(
        token in low
        for token in (
            "생성시각",
            "생성 시각",
            "생성된",
            "최초 생성",
            "created time",
            "creation time",
            "created_at",
            "birth time",
            "birthtime",
        )
    )
    latest_scope = any(
        token in low
        for token in (
            "가장 늦게",
            "가장 최근",
            "최신",
            "recent",
            "latest",
            "newest",
            "most recent",
        )
    )

    if md_scope and created_scope and latest_scope:
        notes.append(
            "file_created_time_fallback: exact file birth time may be unavailable; use birthtime if present, else git first-seen time, else filesystem mtime."
        )

    if not notes:
        return text, []

    augmented = text
    if "file_created_time_fallback" in " ".join(notes):
        augmented += (
            "\n\n[Execution Fallback Policy]\n"
            "- Exact file creation time may be unavailable or inconsistent on local Linux filesystems.\n"
            "- When ranking 'latest created' files, use this fallback ladder in order:\n"
            "  1) filesystem birth time if available\n"
            "  2) git first-seen/add time if available\n"
            "  3) filesystem mtime\n"
            "- Continue the task with the best available criterion instead of blocking.\n"
            "- In the final answer, state clearly which criterion was actually used.\n"
        )

    return augmented, notes


def resolve_dispatch_mode_and_roles(
    *,
    run_force_mode: Optional[str],
    run_roles_override: Optional[str],
    project_roles_csv: Optional[str],
    auto_dispatch_enabled: bool,
    prompt: str,
    choose_auto_dispatch_roles: Callable[..., List[str]],
    available_roles: List[str],
    team_dir: Any,
) -> DispatchModeResult:
    explicit_roles = (run_roles_override if run_roles_override is not None else (project_roles_csv or "")).strip()
    auto_roles: List[str] = []
    if auto_dispatch_enabled:
        try:
            auto_roles = choose_auto_dispatch_roles(prompt, available_roles=available_roles, team_dir=team_dir)
        except TypeError:
            auto_roles = choose_auto_dispatch_roles(prompt)
    dispatch_mode = False
    dispatch_roles = explicit_roles

    if run_force_mode == "direct":
        dispatch_mode = False
        dispatch_roles = ""
    elif run_force_mode == "dispatch":
        dispatch_mode = True
        if not dispatch_roles:
            dispatch_roles = ",".join(auto_roles) if auto_roles else "Reviewer"
    elif dispatch_roles:
        dispatch_mode = True
    elif auto_dispatch_enabled and auto_roles:
        dispatch_mode = True
        dispatch_roles = ",".join(auto_roles)

    return DispatchModeResult(
        dispatch_mode=dispatch_mode,
        dispatch_roles=dispatch_roles,
    )


def compute_dispatch_plan(
    *,
    args: Any,
    p_args: Any,
    prompt: str,
    dispatch_mode: bool,
    run_control_mode: str,
    run_source_task: Optional[Dict[str, Any]],
    selected_roles: List[str],
    available_roles: List[str],
    available_worker_roles: Callable[[List[str]], List[str]],
    normalize_task_plan_payload: Callable[..., Dict[str, Any]],
    build_task_execution_plan: Callable[..., Dict[str, Any]],
    critique_task_execution_plan: Callable[..., Dict[str, Any]],
    critic_has_blockers: Callable[[Dict[str, Any]], bool],
    repair_task_execution_plan: Callable[..., Dict[str, Any]],
    plan_roles_from_subtasks: Callable[[Optional[Dict[str, Any]]], List[str]],
    report_progress: Optional[Callable[..., None]] = None,
) -> PlanMeta:
    plan_data: Optional[Dict[str, Any]] = None
    plan_critic: Dict[str, Any] = default_plan_critic_payload()
    plan_roles: List[str] = []
    plan_replans: List[Dict[str, Any]] = []
    plan_error = ""
    plan_gate_blocked = False
    plan_gate_reason = ""
    planning_enabled = bool(args.task_planning) or (run_control_mode == "replan")
    reuse_source_plan = (
        run_control_mode == "retry"
        and isinstance(run_source_task, dict)
        and isinstance(run_source_task.get("plan"), dict)
    )

    if dispatch_mode and (planning_enabled or reuse_source_plan) and not args.dry_run:
        try:
            if reuse_source_plan and isinstance(run_source_task, dict):
                if callable(report_progress):
                    report_progress(phase="reuse", detail="using stored plan from previous attempt")
                source_plan = run_source_task.get("plan")
                plan_data = normalize_task_plan_payload(
                    source_plan if isinstance(source_plan, dict) else None,
                    user_prompt=prompt,
                    workers=available_worker_roles(available_roles),
                    max_subtasks=max(1, int(args.plan_max_subtasks)),
                )
                raw_critic = run_source_task.get("plan_critic")
                plan_critic = normalize_plan_critic_payload(raw_critic, max_items=8)

            if (plan_data is None) and planning_enabled:
                if callable(report_progress):
                    report_progress(phase="planner", detail="building execution plan")
                plan_data = build_task_execution_plan(
                    p_args,
                    user_prompt=prompt,
                    available_roles=available_roles,
                    max_subtasks=max(1, int(args.plan_max_subtasks)),
                )
                if callable(report_progress):
                    report_progress(phase="critic", detail="reviewing generated plan")
                plan_critic = critique_task_execution_plan(p_args, prompt, plan_data)

                if bool(args.plan_auto_replan):
                    max_replans = max(0, int(args.plan_replan_attempts))
                    for attempt in range(1, max_replans + 1):
                        if not isinstance(plan_data, dict) or not critic_has_blockers(plan_critic):
                            break
                        if callable(report_progress):
                            report_progress(
                                phase="repair",
                                detail="critic issues found; auto-replanning",
                                attempt=attempt,
                                total=max_replans,
                            )
                        plan_data = repair_task_execution_plan(
                            p_args,
                            user_prompt=prompt,
                            current_plan=plan_data,
                            critic=plan_critic,
                            available_roles=available_roles,
                            max_subtasks=max(1, int(args.plan_max_subtasks)),
                            attempt_no=attempt,
                        )
                        if callable(report_progress):
                            report_progress(
                                phase="critic",
                                detail="rechecking repaired plan",
                                attempt=attempt,
                                total=max_replans,
                            )
                        plan_critic = critique_task_execution_plan(p_args, prompt, plan_data)
                        plan_replans.append(
                            {
                                "attempt": attempt,
                                "critic": "approved" if not critic_has_blockers(plan_critic) else "needs_fix",
                                "subtasks": len(plan_data.get("subtasks") or []),
                            }
                        )

            plan_roles = plan_roles_from_subtasks(plan_data)
            if not selected_roles and plan_roles:
                selected_roles = plan_roles

            if bool(args.plan_block_on_critic) and isinstance(plan_data, dict) and critic_has_blockers(plan_critic):
                lead_issue = plan_critic_primary_issue(plan_critic, limit=240) or "critic unresolved after auto-replan"
                plan_gate_blocked = True
                plan_gate_reason = lead_issue
                if callable(report_progress):
                    report_progress(phase="blocked", detail=plan_gate_reason)
            elif isinstance(plan_data, dict) and callable(report_progress):
                critic_state = "issues" if critic_has_blockers(plan_critic) else "ok"
                report_progress(
                    phase="ready",
                    detail=f"subtasks={len(plan_data.get('subtasks') or [])} critic={critic_state} replans={len(plan_replans)}",
                )
        except Exception as e:
            plan_data = None
            plan_critic = default_plan_critic_payload()
            plan_roles = []
            plan_replans = []
            plan_error = str(e).strip()[:260]
            if callable(report_progress):
                report_progress(phase="fallback", detail=plan_error or "planning failed; dispatching original prompt")

    return PlanMeta(
        selected_roles=selected_roles,
        plan_data=plan_data,
        plan_critic=plan_critic,
        plan_roles=plan_roles,
        plan_replans=plan_replans,
        plan_error=plan_error,
        plan_gate_blocked=plan_gate_blocked,
        plan_gate_reason=plan_gate_reason,
        planning_enabled=planning_enabled,
        reuse_source_plan=reuse_source_plan,
    )


def emit_planning_progress(
    *,
    phase: str,
    key: str,
    send: Callable[..., bool],
    log_event: Callable[..., None],
    emit_chat: bool,
    detail: str = "",
    attempt: int = 0,
    total: int = 0,
) -> None:
    phase_token = str(phase or "").strip().lower() or "planning"
    suffix = f" attempt={attempt}/{total}" if int(attempt or 0) > 0 and int(total or 0) > 0 else ""
    detail_text = str(detail or "").strip()
    log_status = "running"
    if phase_token in {"ready", "reuse"}:
        log_status = "completed"
    elif phase_token in {"blocked", "fallback"}:
        log_status = "failed"
    log_event(
        event=f"planning_{phase_token}",
        project=key,
        stage="planning",
        status=log_status,
        detail=f"{detail_text}{suffix}".strip(),
    )
    if not emit_chat:
        return

    heading_map = {
        "planner": "planning: planner",
        "critic": "planning: critic",
        "repair": "planning: auto-replan",
        "reuse": "planning: reuse previous plan",
        "ready": "planning: ready",
        "fallback": "planning: fallback",
        "blocked": "planning: blocked",
    }
    lines = [heading_map.get(phase_token, f"planning: {phase_token}")]
    lines.append(f"- orch: {key}")
    if detail_text:
        lines.append(f"- detail: {detail_text}")
    if suffix:
        lines.append(f"- progress: {attempt}/{total}")
    if phase_token == "ready":
        lines.append("- dispatch: starting")
    elif phase_token == "fallback":
        lines.append("- dispatch: original request fallback")
    send("\n".join(lines), context="planning-progress", with_menu=False)


def apply_plan_and_lineage(
    *,
    task: Optional[Dict[str, Any]],
    plan_data: Optional[Dict[str, Any]],
    plan_critic: Dict[str, Any],
    plan_roles: List[str],
    plan_replans: List[Dict[str, Any]],
    plan_error: str,
    critic_has_blockers: Callable[[Dict[str, Any]], bool],
    lifecycle_set_stage: Callable[..., None],
    run_control_mode: str,
    run_source_request_id: str,
    run_source_task: Optional[Dict[str, Any]],
    req_id: str,
    now_iso: Callable[[], str],
) -> None:
    if task is None:
        return

    if isinstance(plan_data, dict):
        task["plan"] = plan_data
        task["plan_critic"] = plan_critic
        task["plan_roles"] = plan_roles
        task["plan_replans"] = plan_replans
        task["plan_gate_passed"] = not critic_has_blockers(plan_critic)
        task["plan_gate_reason"] = plan_critic_primary_issue(plan_critic, limit=240)
        lifecycle_set_stage(
            task,
            "planning",
            "done",
            note=(
                f"subtasks={len(plan_data.get('subtasks') or [])} "
                f"critic={'ok' if not critic_has_blockers(plan_critic) else 'issues'} "
                f"replans={len(plan_replans)}"
            ),
        )
    elif plan_error:
        lifecycle_set_stage(task, "planning", "done", note=f"fallback_no_plan: {plan_error}")

    if run_control_mode not in {"retry", "replan"} or (not run_source_request_id):
        return

    lineage_ts = now_iso()
    task["source_request_id"] = run_source_request_id
    task["control_mode"] = run_control_mode
    context = task.get("context")
    if not isinstance(context, dict):
        context = {}
        task["context"] = context
    context["source_request_id"] = run_source_request_id
    context["control_mode"] = run_control_mode
    if run_control_mode == "retry":
        task["retry_of"] = run_source_request_id
        child_field = "retry_children"
    else:
        task["replan_of"] = run_source_request_id
        child_field = "replan_children"

    if isinstance(run_source_task, dict):
        children = run_source_task.get(child_field)
        if not isinstance(children, list):
            children = []
            run_source_task[child_field] = children
        if req_id and req_id not in children:
            children.append(req_id)
        run_source_task["updated_at"] = lineage_ts
        source_context = run_source_task.get("context")
        if not isinstance(source_context, dict):
            source_context = {}
            run_source_task["context"] = source_context
        if req_id:
            source_context["last_child_request_id"] = req_id
        source_context["last_child_at"] = lineage_ts

