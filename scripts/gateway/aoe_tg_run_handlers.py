#!/usr/bin/env python3
"""Run and confirmation handler helpers for Telegram gateway."""

import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from aoe_tg_exec_pipeline import (
    DispatchSyncResult,
    attach_todo_to_task_and_entry as exec_attach_todo_to_task_and_entry,
    cleanup_terminal_todo_gate as exec_cleanup_terminal_todo_gate,
    dispatch_and_sync_task as exec_dispatch_and_sync_task,
    effective_todo_token as exec_effective_todo_token,
    finalize_todo_after_run as exec_finalize_todo_after_run,
    find_project_todo_item as exec_find_project_todo_item,
    find_todo_proposal_row as exec_find_todo_proposal_row,
    maybe_capture_todo_proposals as exec_maybe_capture_todo_proposals,
    maybe_send_manual_followup_alert as exec_maybe_send_manual_followup_alert,
    project_alias as exec_project_alias,
    task_label_for_todo as exec_task_label_for_todo,
)
from aoe_tg_exec_results import (
    confirmed_result_reply_markup as exec_confirmed_result_reply_markup,
    early_gate_reply_markup as exec_early_gate_reply_markup,
    intervention_reply_markup as exec_intervention_reply_markup,
    send_dispatch_exception as exec_send_dispatch_exception,
    send_dispatch_result as exec_send_dispatch_result,
    send_exec_critic_intervention as exec_send_exec_critic_intervention,
)
from aoe_tg_run_guards import (
    DispatchPolicyResult,
    EffectiveRunOptions,
    build_dry_run_preview as guard_build_dry_run_preview,
    confirm_required_reply_markup as guard_confirm_required_reply_markup,
    enforce_dispatch_policies as guard_enforce_dispatch_policies,
    handle_run_rate_limit_and_confirm as guard_handle_run_rate_limit_and_confirm,
    rate_limit_reply_markup as guard_rate_limit_reply_markup,
    resolve_confirm_run_transition as guard_resolve_confirm_run_transition,
    resolve_effective_run_options as guard_resolve_effective_run_options,
)
from aoe_tg_plan_pipeline import (
    DispatchModeResult,
    PlanMeta,
    apply_plan_and_lineage as plan_apply_plan_and_lineage,
    apply_success_first_prompt_fallbacks as plan_apply_success_first_prompt_fallbacks,
    compute_dispatch_plan as plan_compute_dispatch_plan,
    emit_planning_progress as plan_emit_planning_progress,
    resolve_dispatch_mode_and_roles as plan_resolve_dispatch_mode_and_roles,
)


_KNOWN_COMMANDS = [
    "help",
    "status",
    "check",
    "task",
    "monitor",
    "kpi",
    "map",
    "queue",
    "sync",
    "next",
    "fanout",
    "drain",
    "auto",
    "offdesk",
    "panic",
    "todo",
    "room",
    "gc",
    "tf",
    "use",
    "orch",
    "mode",
    "lang",
    "report",
    "replay",
    "ok",
    "whoami",
    "lockme",
    "onlyme",
    "acl",
    "grant",
    "revoke",
    "pick",
    "dispatch",
    "direct",
    "cancel",
    "retry",
    "replan",
    "request",
    "run",
    "clear",
]

_BLOCKED_MANUAL_FOLLOWUP_THRESHOLD = 2


def _cmd_prefix() -> str:
    raw = str(os.environ.get("AOE_TG_COMMAND_PREFIXES", "/") or "/").strip()
    for ch in raw:
        if ch in {"/", "!"}:
            return ch
    return "/"


def _suggest_commands(raw_cmd: str, limit: int = 5) -> List[str]:
    token = str(raw_cmd or "").strip().lower()
    if not token:
        return []
    exact = [c for c in _KNOWN_COMMANDS if c == token]
    if exact:
        return exact
    starts = [c for c in _KNOWN_COMMANDS if c.startswith(token)]
    if starts:
        return starts[: max(1, int(limit))]
    contains = [c for c in _KNOWN_COMMANDS if token in c]
    return contains[: max(1, int(limit))]


def _confirm_required_reply_markup() -> Dict[str, Any]:
    return guard_confirm_required_reply_markup()


def _rate_limit_reply_markup(entry: Optional[Dict[str, Any]] = None, key: str = "") -> Dict[str, Any]:
    return guard_rate_limit_reply_markup(entry, key)


def _confirmed_result_reply_markup(entry: Dict[str, Any], key: str) -> Dict[str, Any]:
    return exec_confirmed_result_reply_markup(entry, key)


def _early_gate_reply_markup(entry: Dict[str, Any], key: str) -> Dict[str, Any]:
    return exec_early_gate_reply_markup(entry, key)


def _intervention_reply_markup(entry: Dict[str, Any], key: str, req_id: str = "") -> Dict[str, Any]:
    return exec_intervention_reply_markup(entry, key, req_id)


def _send_exec_critic_intervention(
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
    exec_send_exec_critic_intervention(
        entry=entry,
        key=key,
        final_req_id=final_req_id,
        verdict=verdict,
        reason=reason,
        exec_attempt=exec_attempt,
        exec_max_attempts=exec_max_attempts,
        send=send,
    )


def _send_dispatch_exception(
    *,
    entry: Dict[str, Any],
    key: str,
    todo_id: str,
    reason: str,
    send: Callable[..., bool],
) -> None:
    exec_send_dispatch_exception(
        entry=entry,
        key=key,
        todo_id=todo_id,
        reason=reason,
        send=send,
    )


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
    choose_auto_dispatch_roles: Callable[..., List[str]]
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
    finalize_request_reply_messages: Callable[..., Dict[str, Any]]
    touch_chat_recent_task_ref: Callable[..., None]
    set_chat_selected_task_ref: Callable[..., None]
    now_iso: Callable[[], str]
    sync_task_lifecycle: Callable[..., Optional[Dict[str, Any]]]
    lifecycle_set_stage: Callable[..., None]
    summarize_task_lifecycle: Callable[[str, Dict[str, Any]], str]
    synthesize_orchestrator_response: Callable[[Any, str, Dict[str, Any]], str]
    critique_task_result: Callable[..., Dict[str, Any]]
    extract_todo_proposals: Callable[..., List[Dict[str, Any]]]
    merge_todo_proposals: Callable[..., Dict[str, Any]]
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
    choose_auto_dispatch_roles: Callable[..., List[str]],
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
    finalize_request_reply_messages: Callable[..., Dict[str, Any]],
    touch_chat_recent_task_ref: Callable[..., None],
    set_chat_selected_task_ref: Callable[..., None],
    now_iso: Callable[[], str],
    sync_task_lifecycle: Callable[..., Optional[Dict[str, Any]]],
    lifecycle_set_stage: Callable[..., None],
    summarize_task_lifecycle: Callable[[str, Dict[str, Any]], str],
    synthesize_orchestrator_response: Callable[[Any, str, Dict[str, Any]], str],
    critique_task_result: Callable[..., Dict[str, Any]],
    extract_todo_proposals: Callable[..., List[Dict[str, Any]]],
    merge_todo_proposals: Callable[..., Dict[str, Any]],
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
            finalize_request_reply_messages=finalize_request_reply_messages,
            touch_chat_recent_task_ref=touch_chat_recent_task_ref,
            set_chat_selected_task_ref=set_chat_selected_task_ref,
            now_iso=now_iso,
            sync_task_lifecycle=sync_task_lifecycle,
            lifecycle_set_stage=lifecycle_set_stage,
            summarize_task_lifecycle=summarize_task_lifecycle,
            synthesize_orchestrator_response=synthesize_orchestrator_response,
            critique_task_result=critique_task_result,
            extract_todo_proposals=extract_todo_proposals,
            merge_todo_proposals=merge_todo_proposals,
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
    p = _cmd_prefix()
    if cmd in {"run", "orch-run"}:
        prompt = run_prompt or rest.strip()
        if not prompt:
            send(
                f"usage: {p}run <prompt> | {p}dispatch <prompt> | {p}direct <prompt> | "
                "aoe run [--direct|--dispatch] [--roles <csv>] [--priority P1|P2|P3] "
                "[--timeout-sec N] [--no-wait] <prompt>",
                context="run usage",
            )
            return None
    elif cmd:
        suggestions = _suggest_commands(cmd)
        sug = ""
        if suggestions:
            sug = "suggest: " + ", ".join(f"{p}{c}" for c in suggestions)
        send(
            "unknown command\n"
            f"- cmd: {p}{cmd}\n"
            + (f"- {sug}\n" if sug else "")
            + f"hint: {p}help (or send just '{p}' for the command menu)",
            context="unknown command",
            with_menu=True,
        )
        return None
    else:
        prompt = text.strip()

    if not prompt:
        send("empty prompt", context="empty prompt")
        return None
    return prompt


def _apply_success_first_prompt_fallbacks(prompt: str) -> tuple[str, List[str]]:
    return plan_apply_success_first_prompt_fallbacks(prompt)


def _handle_run_rate_limit_and_confirm(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    key: str,
    entry: Optional[Dict[str, Any]],
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
    return guard_handle_run_rate_limit_and_confirm(
        cmd=cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        key=key,
        entry=entry,
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
    )


def _task_label_for_todo(task: Optional[Dict[str, Any]], fallback_request_id: str) -> str:
    return exec_task_label_for_todo(task, fallback_request_id)


def _find_project_todo_item(entry: Dict[str, Any], todo_id: str) -> Optional[Dict[str, Any]]:
    return exec_find_project_todo_item(entry, todo_id)


def _attach_todo_to_task_and_entry(
    *,
    entry: Dict[str, Any],
    chat_id: str,
    todo_id: str,
    req_id: str,
    task: Optional[Dict[str, Any]],
    now_iso: Callable[[], str],
) -> None:
    exec_attach_todo_to_task_and_entry(
        entry=entry,
        chat_id=chat_id,
        todo_id=todo_id,
        req_id=req_id,
        task=task,
        now_iso=now_iso,
    )


def _project_alias(entry: Dict[str, Any], fallback: str) -> str:
    return exec_project_alias(entry, fallback)


def _effective_todo_token(
    *,
    entry: Dict[str, Any],
    chat_id: str,
    todo_id: str,
    run_auto_source: str,
) -> str:
    return exec_effective_todo_token(
        entry=entry,
        chat_id=chat_id,
        todo_id=todo_id,
        run_auto_source=run_auto_source,
    )


def _maybe_send_manual_followup_alert(
    *,
    entry: Dict[str, Any],
    todo_id: str,
    project_key: str,
    send: Callable[..., bool],
    now_iso: Callable[[], str],
) -> bool:
    return exec_maybe_send_manual_followup_alert(
        entry=entry,
        todo_id=todo_id,
        project_key=project_key,
        send=send,
        now_iso=now_iso,
    )


def _find_todo_proposal_row(entry: Dict[str, Any], proposal_id: str) -> Optional[Dict[str, Any]]:
    return exec_find_todo_proposal_row(entry, proposal_id)


def _maybe_capture_todo_proposals(
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
    return exec_maybe_capture_todo_proposals(
        args=args,
        entry=entry,
        key=key,
        p_args=p_args,
        prompt=prompt,
        state=state,
        req_id=req_id,
        task=task,
        todo_id=todo_id,
        send=send,
        log_event=log_event,
        now_iso=now_iso,
        extract_todo_proposals=extract_todo_proposals,
        merge_todo_proposals=merge_todo_proposals,
    )


def _finalize_todo_after_run(
    *,
    entry: Dict[str, Any],
    todo_id: str,
    status: str,
    exec_verdict: str,
    exec_reason: str,
    req_id: str,
    task: Optional[Dict[str, Any]],
    now_iso: Callable[[], str],
) -> None:
    exec_finalize_todo_after_run(
        entry=entry,
        todo_id=todo_id,
        status=status,
        exec_verdict=exec_verdict,
        exec_reason=exec_reason,
        req_id=req_id,
        task=task,
        now_iso=now_iso,
        manual_followup_threshold=_BLOCKED_MANUAL_FOLLOWUP_THRESHOLD,
    )


def _cleanup_terminal_todo_gate(
    *,
    entry: Dict[str, Any],
    chat_id: str,
    todo_id: str,
    pending_todo_used: bool,
    run_auto_source: str,
    reason: str,
    now_iso: Callable[[], str],
) -> bool:
    return exec_cleanup_terminal_todo_gate(
        entry=entry,
        chat_id=chat_id,
        todo_id=todo_id,
        pending_todo_used=pending_todo_used,
        run_auto_source=run_auto_source,
        reason=reason,
        now_iso=now_iso,
        manual_followup_threshold=_BLOCKED_MANUAL_FOLLOWUP_THRESHOLD,
    )


def _resolve_dispatch_mode_and_roles(
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
    return plan_resolve_dispatch_mode_and_roles(
        run_force_mode=run_force_mode,
        run_roles_override=run_roles_override,
        project_roles_csv=project_roles_csv,
        auto_dispatch_enabled=auto_dispatch_enabled,
        prompt=prompt,
        choose_auto_dispatch_roles=choose_auto_dispatch_roles,
        available_roles=available_roles,
        team_dir=team_dir,
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
    report_progress: Optional[Callable[..., None]] = None,
) -> PlanMeta:
    return plan_compute_dispatch_plan(
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
        report_progress=report_progress,
    )


def _emit_planning_progress(
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
    plan_emit_planning_progress(
        phase=phase,
        key=key,
        send=send,
        log_event=log_event,
        emit_chat=emit_chat,
        detail=detail,
        attempt=attempt,
        total=total,
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
    return exec_dispatch_and_sync_task(
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
        require_verifier=require_verifier,
        verifier_candidates=verifier_candidates,
        run_aoe_orch=run_aoe_orch,
        touch_chat_recent_task_ref=touch_chat_recent_task_ref,
        set_chat_selected_task_ref=set_chat_selected_task_ref,
        now_iso=now_iso,
        sync_task_lifecycle=sync_task_lifecycle,
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
    plan_apply_plan_and_lineage(
        task=task,
        plan_data=plan_data,
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


def _send_dispatch_result(
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
    return exec_send_dispatch_result(
        args=args,
        key=key,
        entry=entry,
        p_args=p_args,
        prompt=prompt,
        state=state,
        req_id=req_id,
        task=task,
        run_control_mode=run_control_mode,
        run_source_request_id=run_source_request_id,
        run_auto_source=run_auto_source,
        send=send,
        log_event=log_event,
        summarize_task_lifecycle=summarize_task_lifecycle,
        synthesize_orchestrator_response=synthesize_orchestrator_response,
        render_run_response=render_run_response,
        finalize_request_reply_messages=finalize_request_reply_messages,
    )


def _enforce_dispatch_policies(
    *,
    dispatch_mode: bool,
    args: Any,
    key: str,
    entry: Dict[str, Any],
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
    return guard_enforce_dispatch_policies(
        dispatch_mode=dispatch_mode,
        args=args,
        key=key,
        entry=entry,
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


def _resolve_effective_run_options(
    *,
    p_args: Any,
    run_priority_override: Optional[str],
    run_timeout_override: Optional[int],
    run_no_wait_override: Optional[bool],
) -> EffectiveRunOptions:
    return guard_resolve_effective_run_options(
        p_args=p_args,
        run_priority_override=run_priority_override,
        run_timeout_override=run_timeout_override,
        run_no_wait_override=run_no_wait_override,
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
    return guard_build_dry_run_preview(
        key=key,
        dispatch_mode=dispatch_mode,
        prompt=prompt,
        dispatch_roles=dispatch_roles,
        require_verifier=require_verifier,
        verifier_roles=verifier_roles,
        verifier_added=verifier_added,
        run_control_mode=run_control_mode,
        run_source_request_id=run_source_request_id,
        planning_enabled=planning_enabled,
        reuse_source_plan=reuse_source_plan,
        plan_data=plan_data,
        plan_replans=plan_replans,
        plan_gate_blocked=plan_gate_blocked,
        plan_error=plan_error,
        effective_priority=effective_priority,
        effective_timeout=effective_timeout,
        effective_no_wait=effective_no_wait,
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
    return guard_resolve_confirm_run_transition(
        cmd=cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        orch_target=orch_target,
        send=send,
        get_confirm_action=get_confirm_action,
        parse_iso_ts=parse_iso_ts,
        clear_confirm_action=clear_confirm_action,
        save_manager_state=save_manager_state,
    )


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
    critique_task_result = deps.routing.critique_task_result
    extract_todo_proposals = deps.routing.extract_todo_proposals
    merge_todo_proposals = deps.routing.merge_todo_proposals
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

    # Resolve project context early to allow todo linkage (and future policies).
    key, entry, p_args = get_context(orch_target)
    setattr(p_args, "_aoe_control_mode", str(run_control_mode or "").strip().lower())
    setattr(p_args, "_aoe_source_request_id", str(run_source_request_id or "").strip())

    prompt, fallback_notes = _apply_success_first_prompt_fallbacks(prompt)
    if fallback_notes:
        log_event(
            event="run_fallback_applied",
            project=key,
            request_id=str(run_source_request_id or "").strip(),
            task=run_source_task if isinstance(run_source_task, dict) else None,
            stage="intake",
            status="adjusted",
            detail="; ".join(str(note).strip() for note in fallback_notes if str(note).strip())[:280],
        )

    if _handle_run_rate_limit_and_confirm(
        cmd=cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        key=key,
        entry=entry,
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

    todo_id = ""
    pending_todo_used = False
    if isinstance(run_source_task, dict):
        todo_id = str(run_source_task.get("todo_id", "")).strip()
    if (not todo_id) and str(run_auto_source or "").strip().lower().startswith("todo"):
        pending = entry.get("pending_todo")
        if isinstance(pending, dict):
            token = str(pending.get("todo_id", "")).strip()
            if token and str(pending.get("chat_id", "")).strip() == str(chat_id):
                todo_id = token
                pending_todo_used = True

    available_roles = load_orchestrator_roles(p_args.team_dir)
    dispatch_meta = _resolve_dispatch_mode_and_roles(
        run_force_mode=run_force_mode,
        run_roles_override=run_roles_override,
        project_roles_csv=(p_args.roles or ""),
        auto_dispatch_enabled=bool(args.auto_dispatch),
        prompt=prompt,
        choose_auto_dispatch_roles=choose_auto_dispatch_roles,
        available_roles=available_roles,
        team_dir=p_args.team_dir,
    )
    dispatch_mode = bool(dispatch_meta.dispatch_mode)
    dispatch_roles = str(dispatch_meta.dispatch_roles).strip()

    verifier_candidates = resolve_verifier_candidates(args.verifier_roles)

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
            report_progress=None,
        )
        selected_roles = list(plan_meta.selected_roles or selected_roles)
        plan_data = plan_meta.plan_data
        plan_critic = plan_meta.plan_critic or {"approved": True, "issues": [], "recommendations": []}
        plan_replans = list(plan_meta.plan_replans or [])
        plan_error = str(plan_meta.plan_error or "")
        plan_gate_blocked = bool(plan_meta.plan_gate_blocked)
        planning_enabled = bool(plan_meta.planning_enabled)
        reuse_source_plan = bool(plan_meta.reuse_source_plan)

        policy = _enforce_dispatch_policies(
            dispatch_mode=dispatch_mode,
            args=args,
            key=key,
            entry=entry,
            selected_roles=selected_roles,
            available_roles=available_roles,
            verifier_candidates=verifier_candidates,
            plan_gate_blocked=plan_gate_blocked,
            plan_gate_reason=str(plan_meta.plan_gate_reason or ""),
            plan_replans=plan_replans,
            ensure_verifier_roles=ensure_verifier_roles,
            dispatch_roles=dispatch_roles,
            send=send,
        )
        if bool(policy.terminal):
            if not args.dry_run:
                effective_todo_id = _effective_todo_token(
                    entry=entry,
                    chat_id=chat_id,
                    todo_id=todo_id,
                    run_auto_source=run_auto_source,
                )
                _cleanup_terminal_todo_gate(
                    entry=entry,
                    chat_id=chat_id,
                    todo_id=todo_id,
                    pending_todo_used=pending_todo_used,
                    run_auto_source=run_auto_source,
                    reason=str(policy.terminal_reason or "dispatch policy blocked").strip(),
                    now_iso=now_iso,
                )
                _maybe_send_manual_followup_alert(
                    entry=entry,
                    todo_id=effective_todo_id,
                    project_key=key,
                    send=send,
                    now_iso=now_iso,
                )
                save_manager_state(args.manager_state_file, manager_state)
            return True
        dry_dispatch_roles = str(policy.dispatch_roles or dispatch_roles).strip()
        verifier_roles = list(policy.verifier_roles or [])
        verifier_added = bool(policy.verifier_added)

        preview = _build_dry_run_preview(
            key=key,
            dispatch_mode=dispatch_mode,
            prompt=prompt,
            dispatch_roles=dry_dispatch_roles,
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

    exec_enabled = bool(getattr(args, "exec_critic", False))
    exec_max_attempts = max(1, int(getattr(args, "exec_critic_retry_max", 3)))
    exec_attempt = 1
    exec_feedback = ""
    last_exec_critic: Dict[str, Any] = {}

    final_state: Dict[str, Any] = {}
    final_req_id = ""
    final_task: Optional[Dict[str, Any]] = None
    final_control_mode = run_control_mode
    final_source_request_id = run_source_request_id
    emit_planning_chat = str(run_auto_source or "").strip().lower().startswith("todo")

    def _report_plan_progress(*, phase: str, detail: str = "", attempt: int = 0, total: int = 0) -> None:
        _emit_planning_progress(
            phase=phase,
            key=key,
            send=send,
            log_event=log_event,
            emit_chat=emit_planning_chat,
            detail=detail,
            attempt=attempt,
            total=total,
        )

    while True:
        attempt_prompt = prompt
        if exec_feedback:
            attempt_prompt = f"{prompt}\n\n[Exec Critic Feedback]\n{exec_feedback}"

        selected_roles = parse_roles_csv(dispatch_roles)
        plan_meta = _compute_dispatch_plan(
            args=args,
            p_args=p_args,
            prompt=attempt_prompt,
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
            report_progress=_report_plan_progress,
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
            key=key,
            entry=entry,
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
            effective_todo_id = _effective_todo_token(
                entry=entry,
                chat_id=chat_id,
                todo_id=todo_id,
                run_auto_source=run_auto_source,
            )
            _cleanup_terminal_todo_gate(
                entry=entry,
                chat_id=chat_id,
                todo_id=todo_id,
                pending_todo_used=pending_todo_used,
                run_auto_source=run_auto_source,
                reason=str(policy.terminal_reason or "dispatch policy blocked").strip(),
                now_iso=now_iso,
            )
            _maybe_send_manual_followup_alert(
                entry=entry,
                todo_id=effective_todo_id,
                project_key=key,
                send=send,
                now_iso=now_iso,
            )
            if not args.dry_run:
                save_manager_state(args.manager_state_file, manager_state)
            return True
        dispatch_roles_effective = str(policy.dispatch_roles or dispatch_roles).strip()
        selected_roles = list(policy.selected_roles or selected_roles)
        verifier_roles = list(policy.verifier_roles or [])

        dispatch_prompt = attempt_prompt
        if isinstance(plan_data, dict):
            dispatch_prompt = build_planned_dispatch_prompt(attempt_prompt, plan_data, plan_critic)

        try:
            dispatch_result = _dispatch_and_sync_task(
                p_args=p_args,
                dispatch_prompt=dispatch_prompt,
                chat_id=chat_id,
                dispatch_roles=dispatch_roles_effective,
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
        except Exception as exc:
            # If this run originated from `/todo next` or `/next`, the scheduler will have set
            # `pending_todo` before dispatch. When dispatch fails (ex: missing orchestrator.json),
            # leaving `pending_todo` behind causes the auto-scheduler to loop forever resuming it.
            reason = str(exc).strip().splitlines()[0] if str(exc).strip() else "dispatch_failed"
            if (not todo_id) and str(run_auto_source or "").strip().lower().startswith("todo"):
                pending = entry.get("pending_todo")
                if isinstance(pending, dict):
                    token = str(pending.get("todo_id", "")).strip()
                    if token and str(pending.get("chat_id", "")).strip() == str(chat_id):
                        todo_id = token
                        pending_todo_used = True

            if todo_id:
                _finalize_todo_after_run(
                    entry=entry,
                    todo_id=todo_id,
                    status="failed",
                    exec_verdict="fail",
                    exec_reason=f"dispatch_failed: {reason}"[:260],
                    req_id="",
                    task=None,
                    now_iso=now_iso,
                )
                pending = entry.get("pending_todo")
                if (
                    isinstance(pending, dict)
                    and str(pending.get("todo_id", "")).strip() == todo_id
                    and str(pending.get("chat_id", "")).strip() == str(chat_id)
                ):
                    entry.pop("pending_todo", None)
                    pending_todo_used = False
                _maybe_send_manual_followup_alert(
                    entry=entry,
                    todo_id=todo_id,
                    project_key=key,
                    send=send,
                    now_iso=now_iso,
                )

            if pending_todo_used:
                entry.pop("pending_todo", None)
                pending_todo_used = False

            entry["updated_at"] = now_iso()
            if not args.dry_run:
                save_manager_state(args.manager_state_file, manager_state)
            _send_dispatch_exception(
                entry=entry,
                key=key,
                todo_id=todo_id,
                reason=reason,
                send=send,
            )
            log_event(
                event="dispatch_failed",
                project=key,
                request_id="",
                task=None,
                stage="dispatch",
                status="failed",
                error_code="E_DISPATCH",
                detail=reason,
            )
            return True
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

        if todo_id:
            _attach_todo_to_task_and_entry(
                entry=entry,
                chat_id=chat_id,
                todo_id=todo_id,
                req_id=req_id,
                task=task,
                now_iso=now_iso,
            )
            if pending_todo_used:
                entry.pop("pending_todo", None)
                pending_todo_used = False

        final_state = state
        final_req_id = req_id
        final_task = task
        final_control_mode = run_control_mode
        final_source_request_id = run_source_request_id

        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)

        ver_status = ""
        if isinstance(task, dict):
            ver_status = str((task.get("stages") or {}).get("verification", "pending"))

        if (not exec_enabled) or (not bool(state.get("complete", False))) or (not (state.get("replies") or [])):
            break
        if bool(args.require_verifier) and ver_status == "failed":
            break

        try:
            critic = critique_task_result(
                p_args,
                prompt,
                state,
                task,
                exec_attempt,
                exec_max_attempts,
            )
        except Exception as e:
            critic = {
                "verdict": "fail",
                "action": "escalate",
                "reason": f"critic_error: {str(e)[:120]}",
                "fix": "",
                "attempt": exec_attempt,
                "max_attempts": exec_max_attempts,
            }

        last_exec_critic = critic if isinstance(critic, dict) else {}
        if isinstance(task, dict):
            task["exec_critic"] = dict(last_exec_critic)
            task["updated_at"] = now_iso()
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)

        verdict = str(last_exec_critic.get("verdict", "")).strip().lower()
        action = str(last_exec_critic.get("action", "")).strip().lower()
        if verdict == "success":
            break

        if exec_attempt >= exec_max_attempts:
            break

        if verdict != "retry":
            break

        # Apply critic guidance for the next attempt.
        exec_feedback = str(last_exec_critic.get("fix", "")).strip() or str(last_exec_critic.get("reason", "")).strip()
        exec_feedback = exec_feedback[:800]

        run_control_mode = "replan" if action == "replan" else "retry"
        run_source_request_id = req_id
        run_source_task = task
        exec_attempt += 1
        log_event(
            event="exec_critic_retry",
            project=key,
            request_id=req_id,
            task=task,
            stage="integration",
            status="running",
            detail=f"attempt={exec_attempt}/{exec_max_attempts} mode={run_control_mode}",
        )
        continue

    verdict = str(last_exec_critic.get("verdict", "")).strip().lower()
    if exec_enabled and last_exec_critic and verdict in {"retry", "fail"}:
        reason = str(last_exec_critic.get("reason", "")).strip()
        proposal_result = _maybe_capture_todo_proposals(
            args=args,
            entry=entry,
            key=key,
            p_args=p_args,
            prompt=prompt,
            state=final_state,
            req_id=final_req_id,
            task=final_task,
            todo_id=todo_id,
            send=send,
            log_event=log_event,
            now_iso=now_iso,
            extract_todo_proposals=extract_todo_proposals,
            merge_todo_proposals=merge_todo_proposals,
        )
        if todo_id:
            _finalize_todo_after_run(
                entry=entry,
                todo_id=todo_id,
                status=str((final_task or {}).get("status", "")).strip(),
                exec_verdict=verdict,
                exec_reason=reason,
                req_id=final_req_id,
                task=final_task,
                now_iso=now_iso,
            )
            _maybe_send_manual_followup_alert(
                entry=entry,
                todo_id=todo_id,
                project_key=key,
                send=send,
                now_iso=now_iso,
            )
        if (todo_id or int(proposal_result.get("created_count", 0) or 0) > 0) and (not args.dry_run):
            save_manager_state(args.manager_state_file, manager_state)
        _send_exec_critic_intervention(
            entry=entry,
            key=key,
            final_req_id=final_req_id,
            verdict=verdict,
            reason=reason,
            exec_attempt=exec_attempt,
            exec_max_attempts=exec_max_attempts,
            send=send,
        )
        log_event(
            event="exec_critic_blocked",
            project=key,
            request_id=final_req_id,
            task=final_task,
            stage="integration",
            status="failed",
            error_code="E_GATE",
            detail=f"verdict={verdict} attempts={exec_attempt}/{exec_max_attempts}",
        )
        return True

    proposal_result = _maybe_capture_todo_proposals(
        args=args,
        entry=entry,
        key=key,
        p_args=p_args,
        prompt=prompt,
        state=final_state,
        req_id=final_req_id,
        task=final_task,
        todo_id=todo_id,
        send=send,
        log_event=log_event,
        now_iso=now_iso,
        extract_todo_proposals=extract_todo_proposals,
        merge_todo_proposals=merge_todo_proposals,
    )
    if todo_id:
        _finalize_todo_after_run(
            entry=entry,
            todo_id=todo_id,
            status=str((final_task or {}).get("status", "")).strip(),
            exec_verdict=str(last_exec_critic.get("verdict", "")).strip(),
            exec_reason=str(last_exec_critic.get("reason", "")).strip(),
            req_id=final_req_id,
            task=final_task,
            now_iso=now_iso,
        )
        _maybe_send_manual_followup_alert(
            entry=entry,
            todo_id=todo_id,
            project_key=key,
            send=send,
            now_iso=now_iso,
        )
    if (todo_id or int(proposal_result.get("created_count", 0) or 0) > 0) and (not args.dry_run):
        save_manager_state(args.manager_state_file, manager_state)

    return _send_dispatch_result(
        args=args,
        key=key,
        entry=entry,
        p_args=p_args,
        prompt=prompt,
        state=final_state,
        req_id=final_req_id,
        task=final_task,
        run_control_mode=final_control_mode,
        run_source_request_id=final_source_request_id,
        run_auto_source=run_auto_source,
        send=send,
        log_event=log_event,
        summarize_task_lifecycle=summarize_task_lifecycle,
        synthesize_orchestrator_response=synthesize_orchestrator_response,
        render_run_response=render_run_response,
        finalize_request_reply_messages=deps.routing.finalize_request_reply_messages,
    )
