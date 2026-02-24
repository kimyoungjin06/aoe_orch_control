#!/usr/bin/env python3
"""Run and confirmation handler helpers for Telegram gateway."""

from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class DispatchModeResult:
    dispatch_mode: bool
    dispatch_roles: str


@dataclass
class PlanMeta:
    selected_roles: List[str] = field(default_factory=list)
    plan_data: Optional[Dict[str, Any]] = None
    plan_critic: Dict[str, Any] = field(default_factory=lambda: {"approved": True, "issues": [], "recommendations": []})
    plan_roles: List[str] = field(default_factory=list)
    plan_replans: List[Dict[str, Any]] = field(default_factory=list)
    plan_error: str = ""
    plan_gate_blocked: bool = False
    plan_gate_reason: str = ""
    planning_enabled: bool = False
    reuse_source_plan: bool = False


@dataclass
class DispatchSyncResult:
    state: Dict[str, Any]
    request_id: str
    task: Optional[Dict[str, Any]]


@dataclass
class DispatchPolicyResult:
    terminal: bool
    dispatch_roles: str = ""
    selected_roles: List[str] = field(default_factory=list)
    verifier_roles: List[str] = field(default_factory=list)
    verifier_added: bool = False


@dataclass
class EffectiveRunOptions:
    priority: str
    timeout: int
    no_wait: bool


@dataclass
class RunContext:
    cmd: str
    args: Any
    manager_state: Dict[str, Any]
    chat_id: str
    text: str
    rest: str
    orch_target: Optional[str]
    run_prompt: str
    run_roles_override: Optional[str]
    run_priority_override: Optional[str]
    run_timeout_override: Optional[int]
    run_no_wait_override: Optional[bool]
    run_force_mode: Optional[str]
    run_auto_source: str
    run_control_mode: str
    run_source_request_id: str
    run_source_task: Optional[Dict[str, Any]]


@dataclass
class RunCoreDeps:
    send: Callable[..., bool]
    log_event: Callable[..., None]
    help_text: Callable[[], str]


@dataclass
class RunGuardDeps:
    summarize_chat_usage: Callable[[Dict[str, Any], str], tuple[int, int]]
    detect_high_risk_prompt: Callable[[str], str]
    set_confirm_action: Callable[..., None]
    save_manager_state: Callable[..., None]


@dataclass
class RunPlanningDeps:
    choose_auto_dispatch_roles: Callable[[str], List[str]]
    resolve_verifier_candidates: Callable[[Optional[str]], List[str]]
    load_orchestrator_roles: Callable[[Any], List[str]]
    parse_roles_csv: Callable[[Optional[str]], List[str]]
    ensure_verifier_roles: Callable[..., tuple[List[str], List[str], bool, List[str]]]
    available_worker_roles: Callable[[List[str]], List[str]]
    normalize_task_plan_payload: Callable[..., Dict[str, Any]]
    build_task_execution_plan: Callable[..., Dict[str, Any]]
    critique_task_execution_plan: Callable[..., Dict[str, Any]]
    critic_has_blockers: Callable[[Dict[str, Any]], bool]
    repair_task_execution_plan: Callable[..., Dict[str, Any]]
    plan_roles_from_subtasks: Callable[[Optional[Dict[str, Any]]], List[str]]
    build_planned_dispatch_prompt: Callable[[str, Dict[str, Any], Dict[str, Any]], str]


@dataclass
class RunRoutingDeps:
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]]
    run_orchestrator_direct: Callable[[Any, str], str]
    run_aoe_orch: Callable[..., Dict[str, Any]]
    touch_chat_recent_task_ref: Callable[..., None]
    set_chat_selected_task_ref: Callable[..., None]
    now_iso: Callable[[], str]
    sync_task_lifecycle: Callable[..., Optional[Dict[str, Any]]]
    lifecycle_set_stage: Callable[..., None]
    summarize_task_lifecycle: Callable[[str, Dict[str, Any]], str]
    synthesize_orchestrator_response: Callable[[Any, str, Dict[str, Any]], str]
    render_run_response: Callable[..., str]


@dataclass
class RunDeps:
    core: RunCoreDeps
    guard: RunGuardDeps
    planning: RunPlanningDeps
    routing: RunRoutingDeps


def build_run_context(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    text: str,
    rest: str,
    orch_target: Optional[str],
    run_prompt: str,
    run_roles_override: Optional[str],
    run_priority_override: Optional[str],
    run_timeout_override: Optional[int],
    run_no_wait_override: Optional[bool],
    run_force_mode: Optional[str],
    run_auto_source: str,
    run_control_mode: str,
    run_source_request_id: str,
    run_source_task: Optional[Dict[str, Any]],
) -> RunContext:
    return RunContext(
        cmd=cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        text=text,
        rest=rest,
        orch_target=orch_target,
        run_prompt=run_prompt,
        run_roles_override=run_roles_override,
        run_priority_override=run_priority_override,
        run_timeout_override=run_timeout_override,
        run_no_wait_override=run_no_wait_override,
        run_force_mode=run_force_mode,
        run_auto_source=run_auto_source,
        run_control_mode=run_control_mode,
        run_source_request_id=run_source_request_id,
        run_source_task=run_source_task,
    )


def build_run_deps(
    *,
    send: Callable[..., bool],
    log_event: Callable[..., None],
    help_text: Callable[[], str],
    summarize_chat_usage: Callable[[Dict[str, Any], str], tuple[int, int]],
    detect_high_risk_prompt: Callable[[str], str],
    set_confirm_action: Callable[..., None],
    save_manager_state: Callable[..., None],
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]],
    choose_auto_dispatch_roles: Callable[[str], List[str]],
    resolve_verifier_candidates: Callable[[Optional[str]], List[str]],
    load_orchestrator_roles: Callable[[Any], List[str]],
    parse_roles_csv: Callable[[Optional[str]], List[str]],
    ensure_verifier_roles: Callable[..., tuple[List[str], List[str], bool, List[str]]],
    available_worker_roles: Callable[[List[str]], List[str]],
    normalize_task_plan_payload: Callable[..., Dict[str, Any]],
    build_task_execution_plan: Callable[..., Dict[str, Any]],
    critique_task_execution_plan: Callable[..., Dict[str, Any]],
    critic_has_blockers: Callable[[Dict[str, Any]], bool],
    repair_task_execution_plan: Callable[..., Dict[str, Any]],
    plan_roles_from_subtasks: Callable[[Optional[Dict[str, Any]]], List[str]],
    build_planned_dispatch_prompt: Callable[[str, Dict[str, Any], Dict[str, Any]], str],
    run_orchestrator_direct: Callable[[Any, str], str],
    run_aoe_orch: Callable[..., Dict[str, Any]],
    touch_chat_recent_task_ref: Callable[..., None],
    set_chat_selected_task_ref: Callable[..., None],
    now_iso: Callable[[], str],
    sync_task_lifecycle: Callable[..., Optional[Dict[str, Any]]],
    lifecycle_set_stage: Callable[..., None],
    summarize_task_lifecycle: Callable[[str, Dict[str, Any]], str],
    synthesize_orchestrator_response: Callable[[Any, str, Dict[str, Any]], str],
    render_run_response: Callable[..., str],
) -> RunDeps:
    return RunDeps(
        core=RunCoreDeps(
            send=send,
            log_event=log_event,
            help_text=help_text,
        ),
        guard=RunGuardDeps(
            summarize_chat_usage=summarize_chat_usage,
            detect_high_risk_prompt=detect_high_risk_prompt,
            set_confirm_action=set_confirm_action,
            save_manager_state=save_manager_state,
        ),
        planning=RunPlanningDeps(
            choose_auto_dispatch_roles=choose_auto_dispatch_roles,
            resolve_verifier_candidates=resolve_verifier_candidates,
            load_orchestrator_roles=load_orchestrator_roles,
            parse_roles_csv=parse_roles_csv,
            ensure_verifier_roles=ensure_verifier_roles,
            available_worker_roles=available_worker_roles,
            normalize_task_plan_payload=normalize_task_plan_payload,
            build_task_execution_plan=build_task_execution_plan,
            critique_task_execution_plan=critique_task_execution_plan,
            critic_has_blockers=critic_has_blockers,
            repair_task_execution_plan=repair_task_execution_plan,
            plan_roles_from_subtasks=plan_roles_from_subtasks,
            build_planned_dispatch_prompt=build_planned_dispatch_prompt,
        ),
        routing=RunRoutingDeps(
            get_context=get_context,
            run_orchestrator_direct=run_orchestrator_direct,
            run_aoe_orch=run_aoe_orch,
            touch_chat_recent_task_ref=touch_chat_recent_task_ref,
            set_chat_selected_task_ref=set_chat_selected_task_ref,
            now_iso=now_iso,
            sync_task_lifecycle=sync_task_lifecycle,
            lifecycle_set_stage=lifecycle_set_stage,
            summarize_task_lifecycle=summarize_task_lifecycle,
            synthesize_orchestrator_response=synthesize_orchestrator_response,
            render_run_response=render_run_response,
        ),
    )


def _resolve_prompt_or_handle_unknown(
    *,
    cmd: str,
    run_prompt: str,
    rest: str,
    text: str,
    send: Callable[..., bool],
    help_text: Callable[[], str],
) -> Optional[str]:
    if cmd in {"run", "orch-run"}:
        prompt = run_prompt or rest.strip()
        if not prompt:
            send(
                "usage: /run <prompt> | /dispatch <prompt> | /direct <prompt> | aoe run [--direct|--dispatch] [--roles <csv>] [--priority P1|P2|P3] [--timeout-sec N] [--no-wait] <prompt>",
                context="run usage",
            )
            return None
    elif cmd:
        send(f"unknown command: /{cmd}\n\n{help_text()}", context="unknown command", with_menu=True)
        return None
    else:
        prompt = text.strip()

    if not prompt:
        send("empty prompt", context="empty prompt")
        return None
    return prompt


def _handle_run_rate_limit_and_confirm(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    run_auto_source: str,
    run_force_mode: Optional[str],
    orch_target: Optional[str],
    prompt: str,
    summarize_chat_usage: Callable[[Dict[str, Any], str], tuple[int, int]],
    detect_high_risk_prompt: Callable[[str], str],
    set_confirm_action: Callable[..., None],
    save_manager_state: Callable[..., None],
    send: Callable[..., bool],
    log_event: Callable[..., None],
) -> bool:
    if cmd not in {"run", "orch-run"}:
        return False

    max_running = max(0, int(args.chat_max_running))
    daily_cap = max(0, int(args.chat_daily_cap))
    running_count, submitted_today = summarize_chat_usage(manager_state, chat_id)

    if max_running > 0 and running_count >= max_running:
        send(
            "rate limit: 동시 실행 한도를 초과했습니다.\n"
            f"- running_now: {running_count}\n"
            f"- max_running: {max_running}\n"
            "next: /monitor 또는 /check 로 기존 작업을 확인하세요.",
            context="rate-limit-running",
            with_menu=True,
        )
        log_event(
            event="rate_limited",
            stage="intake",
            status="rejected",
            error_code="E_GATE",
            detail=f"type=running running_now={running_count} max={max_running}",
        )
        return True

    if daily_cap > 0 and submitted_today >= daily_cap:
        send(
            "rate limit: 일일 실행 한도에 도달했습니다.\n"
            f"- submitted_today: {submitted_today}\n"
            f"- daily_cap: {daily_cap}\n"
            "next: 내일 다시 시도하거나 cap 설정을 조정하세요.",
            context="rate-limit-daily",
            with_menu=True,
        )
        log_event(
            event="rate_limited",
            stage="intake",
            status="rejected",
            error_code="E_GATE",
            detail=f"type=daily submitted_today={submitted_today} cap={daily_cap}",
        )
        return True

    if run_auto_source != "default":
        return False

    risk = detect_high_risk_prompt(prompt)
    if not risk:
        return False

    set_confirm_action(
        manager_state,
        chat_id=chat_id,
        mode=(run_force_mode or "dispatch"),
        prompt=prompt,
        risk=risk,
        orch=str(orch_target or ""),
    )
    if not args.dry_run:
        save_manager_state(args.manager_state_file, manager_state)
    send(
        "고위험 자동실행 감지: 확인이 필요합니다.\n"
        f"- risk: {risk}\n"
        f"- mode: {run_force_mode or 'dispatch'}\n"
        f"- preview: {prompt[:160]}\n"
        "실행: /ok\n"
        "취소: /cancel",
        context="confirm-required",
        with_menu=True,
    )
    log_event(
        event="confirm_required",
        stage="intake",
        status="pending",
        detail=f"risk={risk} mode={run_force_mode or 'dispatch'} auto_source={run_auto_source}",
    )
    return True


def _resolve_dispatch_mode_and_roles(
    *,
    run_force_mode: Optional[str],
    run_roles_override: Optional[str],
    project_roles_csv: Optional[str],
    auto_dispatch_enabled: bool,
    prompt: str,
    choose_auto_dispatch_roles: Callable[[str], List[str]],
) -> DispatchModeResult:
    explicit_roles = (run_roles_override if run_roles_override is not None else (project_roles_csv or "")).strip()
    auto_roles = choose_auto_dispatch_roles(prompt) if auto_dispatch_enabled else []
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


def _compute_dispatch_plan(
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
) -> PlanMeta:
    plan_data: Optional[Dict[str, Any]] = None
    plan_critic: Dict[str, Any] = {"approved": True, "issues": [], "recommendations": []}
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
                source_plan = run_source_task.get("plan")
                plan_data = normalize_task_plan_payload(
                    source_plan if isinstance(source_plan, dict) else None,
                    user_prompt=prompt,
                    workers=available_worker_roles(available_roles),
                    max_subtasks=max(1, int(args.plan_max_subtasks)),
                )
                raw_critic = run_source_task.get("plan_critic")
                if isinstance(raw_critic, dict):
                    raw_issues = raw_critic.get("issues") or []
                    raw_rec = raw_critic.get("recommendations") or []
                    plan_critic = {
                        "approved": bool(raw_critic.get("approved", True)),
                        "issues": [str(x).strip() for x in raw_issues if str(x).strip()][:8],
                        "recommendations": [str(x).strip() for x in raw_rec if str(x).strip()][:8],
                    }
                else:
                    plan_critic = {"approved": True, "issues": [], "recommendations": []}

            if (plan_data is None) and planning_enabled:
                plan_data = build_task_execution_plan(
                    p_args,
                    user_prompt=prompt,
                    available_roles=available_roles,
                    max_subtasks=max(1, int(args.plan_max_subtasks)),
                )
                plan_critic = critique_task_execution_plan(p_args, prompt, plan_data)

                if bool(args.plan_auto_replan):
                    max_replans = max(0, int(args.plan_replan_attempts))
                    for attempt in range(1, max_replans + 1):
                        if not isinstance(plan_data, dict) or not critic_has_blockers(plan_critic):
                            break
                        plan_data = repair_task_execution_plan(
                            p_args,
                            user_prompt=prompt,
                            current_plan=plan_data,
                            critic=plan_critic,
                            available_roles=available_roles,
                            max_subtasks=max(1, int(args.plan_max_subtasks)),
                            attempt_no=attempt,
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
                issues = plan_critic.get("issues") or []
                lead_issue = str(issues[0]).strip() if issues else "critic unresolved after auto-replan"
                plan_gate_blocked = True
                plan_gate_reason = lead_issue[:240]
        except Exception as e:
            plan_data = None
            plan_critic = {"approved": True, "issues": [], "recommendations": []}
            plan_roles = []
            plan_replans = []
            plan_error = str(e).strip()[:260]

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


def _dispatch_and_sync_task(
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


def _apply_plan_and_lineage(
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
        task["plan_gate_passed"] = (not critic_has_blockers(plan_critic))
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
    if run_control_mode == "retry":
        task["retry_of"] = run_source_request_id
        child_field = "retry_children"
    else:
        task["replan_of"] = run_source_request_id
        child_field = "replan_children"
    lifecycle_set_stage(task, "intake", "done", note=f"{run_control_mode}_of={run_source_request_id}")

    if not isinstance(run_source_task, dict):
        return

    children_raw = run_source_task.get(child_field)
    children: List[str] = []
    if isinstance(children_raw, list):
        for item in children_raw:
            token = str(item or "").strip()
            if token and token not in children:
                children.append(token)
    if req_id and req_id not in children:
        children.append(req_id)
    run_source_task[child_field] = children[-20:]
    run_source_task["updated_at"] = lineage_ts


def _send_dispatch_result(
    *,
    args: Any,
    key: str,
    p_args: Any,
    prompt: str,
    state: Dict[str, Any],
    req_id: str,
    task: Optional[Dict[str, Any]],
    run_control_mode: str,
    run_source_request_id: str,
    send: Callable[..., bool],
    log_event: Callable[..., None],
    summarize_task_lifecycle: Callable[[str, Dict[str, Any]], str],
    synthesize_orchestrator_response: Callable[[Any, str, Dict[str, Any]], str],
    render_run_response: Callable[..., str],
) -> bool:
    if task is not None:
        ver_status = str((task.get("stages") or {}).get("verification", "pending"))
        if bool(args.require_verifier) and ver_status == "failed":
            send(summarize_task_lifecycle(key, task), context="verifier-gate failed")
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
            send(synthesize_orchestrator_response(p_args, prompt, state), context="synth")
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

    send(render_run_response(state, task=task), context="result")
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


def _enforce_dispatch_policies(
    *,
    dispatch_mode: bool,
    args: Any,
    selected_roles: List[str],
    available_roles: List[str],
    verifier_candidates: List[str],
    plan_gate_blocked: bool,
    plan_gate_reason: str,
    plan_replans: List[Dict[str, Any]],
    ensure_verifier_roles: Callable[..., tuple[List[str], List[str], bool, List[str]]],
    dispatch_roles: str,
    send: Callable[..., bool],
) -> DispatchPolicyResult:
    verifier_roles: List[str] = []
    verifier_added = False

    if not dispatch_mode:
        return DispatchPolicyResult(
            terminal=False,
            dispatch_roles=dispatch_roles,
            selected_roles=selected_roles,
            verifier_roles=verifier_roles,
            verifier_added=verifier_added,
        )

    selected_roles, verifier_roles, verifier_added, _available_verifier_roles = ensure_verifier_roles(
        selected_roles=selected_roles,
        available_roles=available_roles,
        verifier_candidates=verifier_candidates,
    )
    dispatch_roles = ",".join(selected_roles)

    if bool(args.require_verifier) and not verifier_roles:
        send(
            "error: verifier gate enabled but no verifier role is available.\n"
            f"required_candidates={', '.join(verifier_candidates) or '-'}\n"
            f"project_roles={', '.join(available_roles) or '-'}\n"
            "hint: add a verifier role (e.g. Reviewer) or disable gate with --no-require-verifier",
            context="verifier-gate setup",
        )
        return DispatchPolicyResult(terminal=True)

    if plan_gate_blocked:
        send(
            "plan gate blocked: critic issues remain after auto-replan.\n"
            f"reason: {plan_gate_reason or 'unresolved issues'}\n"
            "hint: 요청을 더 구체화하거나 역할/범위를 줄여 다시 실행하세요.\n"
            f"replan_attempts: {len(plan_replans)}",
            context="planning-gate",
        )
        return DispatchPolicyResult(terminal=True)

    return DispatchPolicyResult(
        terminal=False,
        dispatch_roles=dispatch_roles,
        selected_roles=selected_roles,
        verifier_roles=verifier_roles,
        verifier_added=verifier_added,
    )


def _resolve_effective_run_options(
    *,
    p_args: Any,
    run_priority_override: Optional[str],
    run_timeout_override: Optional[int],
    run_no_wait_override: Optional[bool],
) -> EffectiveRunOptions:
    return EffectiveRunOptions(
        priority=str(run_priority_override if run_priority_override is not None else p_args.priority),
        timeout=int(run_timeout_override if run_timeout_override is not None else p_args.orch_timeout_sec),
        no_wait=bool(run_no_wait_override if run_no_wait_override is not None else p_args.no_wait),
    )


def _build_dry_run_preview(
    *,
    key: str,
    dispatch_mode: bool,
    prompt: str,
    dispatch_roles: str,
    require_verifier: bool,
    verifier_roles: List[str],
    verifier_added: bool,
    run_control_mode: str,
    run_source_request_id: str,
    planning_enabled: bool,
    reuse_source_plan: bool,
    plan_data: Optional[Dict[str, Any]],
    plan_replans: List[Dict[str, Any]],
    plan_gate_blocked: bool,
    plan_error: str,
    effective_priority: str,
    effective_timeout: int,
    effective_no_wait: bool,
) -> str:
    plan_subtasks = len(plan_data.get("subtasks") or []) if isinstance(plan_data, dict) else 0
    return (
        "[DRY-RUN] orch={orch} mode: {mode}\n"
        "- prompt: {prompt}\n"
        "- roles: {roles}\n"
        "- verifier_required: {ver_req}\n"
        "- verifier_roles: {ver_roles}\n"
        "- verifier_auto_added: {ver_added}\n"
        "- control_mode: {control_mode}\n"
        "- source_request_id: {source_request_id}\n"
        "- task_planning: {plan_enabled}\n"
        "- plan_reused: {plan_reused}\n"
        "- plan_subtasks: {plan_subtasks}\n"
        "- plan_replans: {plan_replans}\n"
        "- plan_gate_blocked: {plan_gate}\n"
        "- plan_error: {plan_error}\n"
        "- priority: {priority}\n"
        "- timeout: {timeout}s\n"
        "- no_wait: {no_wait}"
    ).format(
        orch=key,
        mode="dispatch" if dispatch_mode else "direct",
        prompt=prompt,
        roles=dispatch_roles if dispatch_roles else "-",
        ver_req="yes" if bool(require_verifier) else "no",
        ver_roles=", ".join(verifier_roles) if verifier_roles else "-",
        ver_added="yes" if verifier_added else "no",
        control_mode=run_control_mode or "normal",
        source_request_id=(run_source_request_id or "-"),
        plan_enabled="yes" if planning_enabled else "no",
        plan_reused="yes" if (reuse_source_plan and isinstance(plan_data, dict)) else "no",
        plan_subtasks=plan_subtasks,
        plan_replans=len(plan_replans),
        plan_gate="yes" if plan_gate_blocked else "no",
        plan_error=(plan_error or "-"),
        priority=effective_priority,
        timeout=effective_timeout,
        no_wait="yes" if effective_no_wait else "no",
    )


def resolve_confirm_run_transition(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    orch_target: Optional[str],
    send: Callable[..., bool],
    get_confirm_action: Callable[[Dict[str, Any], str], Dict[str, Any]],
    parse_iso_ts: Callable[[str], Optional[Any]],
    clear_confirm_action: Callable[[Dict[str, Any], str], bool],
    save_manager_state: Callable[..., None],
) -> Optional[Dict[str, Any]]:
    if cmd != "confirm-run":
        return None

    confirm = get_confirm_action(manager_state, chat_id)
    if not confirm:
        send(
            "확인 대기 중인 실행이 없습니다.\n"
            "고위험 평문 자동실행이 감지되면 /ok 로 승인할 수 있습니다.",
            context="confirm-empty",
            with_menu=True,
        )
        return {"terminal": True}

    requested_at = str(confirm.get("requested_at", "")).strip()
    ttl_sec = max(30, int(args.confirm_ttl_sec))
    created_ts = parse_iso_ts(requested_at)
    expired = False
    if created_ts is not None:
        expired = (datetime.now(timezone.utc) - created_ts.astimezone(timezone.utc)).total_seconds() > ttl_sec
    if expired:
        _ = clear_confirm_action(manager_state, chat_id)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            "확인 요청이 만료되었습니다.\n"
            "다시 평문으로 요청하거나 /dispatch 로 재실행하세요.",
            context="confirm-expired",
            with_menu=True,
        )
        return {"terminal": True}

    run_prompt = str(confirm.get("prompt", "")).strip()
    run_force_mode = str(confirm.get("mode", "")).strip().lower() or "dispatch"
    next_orch_target = str(confirm.get("orch", "")).strip() or orch_target
    _ = clear_confirm_action(manager_state, chat_id)
    if not args.dry_run:
        save_manager_state(args.manager_state_file, manager_state)

    return {
        "terminal": False,
        "cmd": "run",
        "run_prompt": run_prompt,
        "run_force_mode": run_force_mode,
        "orch_target": next_orch_target,
        "run_auto_source": "confirmed",
    }


def handle_run_or_unknown_command(
    *,
    ctx: RunContext,
    deps: RunDeps,
) -> bool:
    cmd = ctx.cmd
    args = ctx.args
    manager_state = ctx.manager_state
    chat_id = ctx.chat_id
    text = ctx.text
    rest = ctx.rest
    orch_target = ctx.orch_target
    run_prompt = ctx.run_prompt
    run_roles_override = ctx.run_roles_override
    run_priority_override = ctx.run_priority_override
    run_timeout_override = ctx.run_timeout_override
    run_no_wait_override = ctx.run_no_wait_override
    run_force_mode = ctx.run_force_mode
    run_auto_source = ctx.run_auto_source
    run_control_mode = ctx.run_control_mode
    run_source_request_id = ctx.run_source_request_id
    run_source_task = ctx.run_source_task
    send = deps.core.send
    log_event = deps.core.log_event
    help_text = deps.core.help_text
    summarize_chat_usage = deps.guard.summarize_chat_usage
    detect_high_risk_prompt = deps.guard.detect_high_risk_prompt
    set_confirm_action = deps.guard.set_confirm_action
    save_manager_state = deps.guard.save_manager_state
    get_context = deps.routing.get_context
    choose_auto_dispatch_roles = deps.planning.choose_auto_dispatch_roles
    resolve_verifier_candidates = deps.planning.resolve_verifier_candidates
    load_orchestrator_roles = deps.planning.load_orchestrator_roles
    parse_roles_csv = deps.planning.parse_roles_csv
    ensure_verifier_roles = deps.planning.ensure_verifier_roles
    available_worker_roles = deps.planning.available_worker_roles
    normalize_task_plan_payload = deps.planning.normalize_task_plan_payload
    build_task_execution_plan = deps.planning.build_task_execution_plan
    critique_task_execution_plan = deps.planning.critique_task_execution_plan
    critic_has_blockers = deps.planning.critic_has_blockers
    repair_task_execution_plan = deps.planning.repair_task_execution_plan
    plan_roles_from_subtasks = deps.planning.plan_roles_from_subtasks
    build_planned_dispatch_prompt = deps.planning.build_planned_dispatch_prompt
    run_orchestrator_direct = deps.routing.run_orchestrator_direct
    run_aoe_orch = deps.routing.run_aoe_orch
    touch_chat_recent_task_ref = deps.routing.touch_chat_recent_task_ref
    set_chat_selected_task_ref = deps.routing.set_chat_selected_task_ref
    now_iso = deps.routing.now_iso
    sync_task_lifecycle = deps.routing.sync_task_lifecycle
    lifecycle_set_stage = deps.routing.lifecycle_set_stage
    summarize_task_lifecycle = deps.routing.summarize_task_lifecycle
    synthesize_orchestrator_response = deps.routing.synthesize_orchestrator_response
    render_run_response = deps.routing.render_run_response

    prompt = _resolve_prompt_or_handle_unknown(
        cmd=cmd,
        run_prompt=run_prompt,
        rest=rest,
        text=text,
        send=send,
        help_text=help_text,
    )
    if prompt is None:
        return True

    if _handle_run_rate_limit_and_confirm(
        cmd=cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        run_auto_source=run_auto_source,
        run_force_mode=run_force_mode,
        orch_target=orch_target,
        prompt=prompt,
        summarize_chat_usage=summarize_chat_usage,
        detect_high_risk_prompt=detect_high_risk_prompt,
        set_confirm_action=set_confirm_action,
        save_manager_state=save_manager_state,
        send=send,
        log_event=log_event,
    ):
        return True

    key, entry, p_args = get_context(orch_target)

    dispatch_meta = _resolve_dispatch_mode_and_roles(
        run_force_mode=run_force_mode,
        run_roles_override=run_roles_override,
        project_roles_csv=(p_args.roles or ""),
        auto_dispatch_enabled=bool(args.auto_dispatch),
        prompt=prompt,
        choose_auto_dispatch_roles=choose_auto_dispatch_roles,
    )
    dispatch_mode = bool(dispatch_meta.dispatch_mode)
    dispatch_roles = str(dispatch_meta.dispatch_roles).strip()

    verifier_candidates = resolve_verifier_candidates(args.verifier_roles)
    available_roles = load_orchestrator_roles(p_args.team_dir)
    selected_roles = parse_roles_csv(dispatch_roles)
    plan_meta = _compute_dispatch_plan(
        args=args,
        p_args=p_args,
        prompt=prompt,
        dispatch_mode=dispatch_mode,
        run_control_mode=run_control_mode,
        run_source_task=run_source_task,
        selected_roles=selected_roles,
        available_roles=available_roles,
        available_worker_roles=available_worker_roles,
        normalize_task_plan_payload=normalize_task_plan_payload,
        build_task_execution_plan=build_task_execution_plan,
        critique_task_execution_plan=critique_task_execution_plan,
        critic_has_blockers=critic_has_blockers,
        repair_task_execution_plan=repair_task_execution_plan,
        plan_roles_from_subtasks=plan_roles_from_subtasks,
    )
    selected_roles = list(plan_meta.selected_roles or selected_roles)
    plan_data = plan_meta.plan_data
    plan_critic = plan_meta.plan_critic or {"approved": True, "issues": [], "recommendations": []}
    plan_roles = list(plan_meta.plan_roles or [])
    plan_replans = list(plan_meta.plan_replans or [])
    plan_error = str(plan_meta.plan_error or "")
    plan_gate_blocked = bool(plan_meta.plan_gate_blocked)
    plan_gate_reason = str(plan_meta.plan_gate_reason or "")
    planning_enabled = bool(plan_meta.planning_enabled)
    reuse_source_plan = bool(plan_meta.reuse_source_plan)

    policy = _enforce_dispatch_policies(
        dispatch_mode=dispatch_mode,
        args=args,
        selected_roles=selected_roles,
        available_roles=available_roles,
        verifier_candidates=verifier_candidates,
        plan_gate_blocked=plan_gate_blocked,
        plan_gate_reason=plan_gate_reason,
        plan_replans=plan_replans,
        ensure_verifier_roles=ensure_verifier_roles,
        dispatch_roles=dispatch_roles,
        send=send,
    )
    if bool(policy.terminal):
        return True
    dispatch_roles = str(policy.dispatch_roles or dispatch_roles).strip()
    selected_roles = list(policy.selected_roles or selected_roles)
    verifier_roles = list(policy.verifier_roles or [])
    verifier_added = bool(policy.verifier_added)

    effective = _resolve_effective_run_options(
        p_args=p_args,
        run_priority_override=run_priority_override,
        run_timeout_override=run_timeout_override,
        run_no_wait_override=run_no_wait_override,
    )
    effective_priority = str(effective.priority)
    effective_timeout = int(effective.timeout)
    effective_no_wait = bool(effective.no_wait)

    if args.dry_run:
        preview = _build_dry_run_preview(
            key=key,
            dispatch_mode=dispatch_mode,
            prompt=prompt,
            dispatch_roles=dispatch_roles,
            require_verifier=bool(args.require_verifier),
            verifier_roles=verifier_roles,
            verifier_added=verifier_added,
            run_control_mode=run_control_mode,
            run_source_request_id=run_source_request_id,
            planning_enabled=planning_enabled,
            reuse_source_plan=reuse_source_plan,
            plan_data=plan_data if isinstance(plan_data, dict) else None,
            plan_replans=plan_replans,
            plan_gate_blocked=plan_gate_blocked,
            plan_error=plan_error,
            effective_priority=str(effective_priority),
            effective_timeout=int(effective_timeout),
            effective_no_wait=bool(effective_no_wait),
        )
        send(preview, context="dry-run")
        return True

    if not dispatch_mode:
        direct_reply = run_orchestrator_direct(p_args, prompt)
        send(direct_reply, context="direct")
        log_event(event="direct_reply", project=key, stage="close", status="completed")
        return True

    dispatch_prompt = prompt
    if isinstance(plan_data, dict):
        dispatch_prompt = build_planned_dispatch_prompt(prompt, plan_data, plan_critic)

    dispatch_result = _dispatch_and_sync_task(
        p_args=p_args,
        dispatch_prompt=dispatch_prompt,
        chat_id=chat_id,
        dispatch_roles=dispatch_roles,
        run_priority_override=run_priority_override,
        run_timeout_override=run_timeout_override,
        run_no_wait_override=run_no_wait_override,
        key=key,
        entry=entry,
        manager_state=manager_state,
        prompt=prompt,
        selected_roles=selected_roles,
        verifier_roles=verifier_roles,
        require_verifier=bool(args.require_verifier),
        verifier_candidates=verifier_candidates,
        run_aoe_orch=run_aoe_orch,
        touch_chat_recent_task_ref=touch_chat_recent_task_ref,
        set_chat_selected_task_ref=set_chat_selected_task_ref,
        now_iso=now_iso,
        sync_task_lifecycle=sync_task_lifecycle,
    )
    state = dispatch_result.state
    req_id = str(dispatch_result.request_id)
    task = dispatch_result.task if isinstance(dispatch_result.task, dict) else None

    _apply_plan_and_lineage(
        task=task,
        plan_data=plan_data if isinstance(plan_data, dict) else None,
        plan_critic=plan_critic,
        plan_roles=plan_roles,
        plan_replans=plan_replans,
        plan_error=plan_error,
        critic_has_blockers=critic_has_blockers,
        lifecycle_set_stage=lifecycle_set_stage,
        run_control_mode=run_control_mode,
        run_source_request_id=run_source_request_id,
        run_source_task=run_source_task,
        req_id=req_id,
        now_iso=now_iso,
    )

    if not args.dry_run:
        save_manager_state(args.manager_state_file, manager_state)

    return _send_dispatch_result(
        args=args,
        key=key,
        p_args=p_args,
        prompt=prompt,
        state=state,
        req_id=req_id,
        task=task,
        run_control_mode=run_control_mode,
        run_source_request_id=run_source_request_id,
        send=send,
        log_event=log_event,
        summarize_task_lifecycle=summarize_task_lifecycle,
        synthesize_orchestrator_response=synthesize_orchestrator_response,
        render_run_response=render_run_response,
    )
