#!/usr/bin/env python3
"""Run intake guards, policy gates, and dry-run preview helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from aoe_tg_exec_pipeline import project_alias
from aoe_tg_exec_results import early_gate_reply_markup


@dataclass
class DispatchPolicyResult:
    terminal: bool
    dispatch_roles: str = ""
    selected_roles: List[str] = field(default_factory=list)
    verifier_roles: List[str] = field(default_factory=list)
    verifier_added: bool = False
    terminal_reason: str = ""


@dataclass
class EffectiveRunOptions:
    priority: str
    timeout: int
    no_wait: bool


def confirm_required_reply_markup() -> Dict[str, Any]:
    return {
        "keyboard": [
            [{"text": "/ok"}, {"text": "/cancel"}, {"text": "/clear pending"}],
            [{"text": "/monitor"}, {"text": "/status"}, {"text": "/help"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "예: /ok 또는 /cancel",
    }


def rate_limit_reply_markup(entry: Optional[Dict[str, Any]] = None, key: str = "") -> Dict[str, Any]:
    if isinstance(entry, dict):
        alias = project_alias(entry, key)
        return {
            "keyboard": [
                [{"text": "/monitor"}, {"text": "/check"}, {"text": f"/orch status {alias}"}],
                [{"text": f"/todo {alias}"}, {"text": "/queue"}, {"text": "/map"}],
            ],
            "resize_keyboard": True,
            "one_time_keyboard": False,
            "input_field_placeholder": f"예: /monitor 또는 /orch status {alias}",
        }
    return {
        "keyboard": [
            [{"text": "/monitor"}, {"text": "/check"}, {"text": "/queue"}],
            [{"text": "/map"}, {"text": "/help"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "예: /monitor 또는 /queue",
    }


def handle_run_rate_limit_and_confirm(
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
            reply_markup=rate_limit_reply_markup(entry, key),
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
            reply_markup=rate_limit_reply_markup(entry, key),
        )
        log_event(
            event="rate_limited",
            stage="intake",
            status="rejected",
            error_code="E_GATE",
            detail=f"type=daily submitted_today={submitted_today} cap={daily_cap}",
        )
        return True

    effective_mode = str(run_force_mode or "dispatch").strip().lower() or "dispatch"
    if effective_mode != "dispatch":
        return False

    if str(run_auto_source or "").strip().lower() == "confirmed":
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
        reply_markup=confirm_required_reply_markup(),
    )
    log_event(
        event="confirm_required",
        stage="intake",
        status="pending",
        detail=f"risk={risk} mode={run_force_mode or 'dispatch'} auto_source={run_auto_source}",
    )
    return True


def enforce_dispatch_policies(
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
            "hint: add a verifier role (e.g. Codex-Reviewer) or disable gate with --no-require-verifier",
            context="verifier-gate setup",
            with_menu=True,
            reply_markup=early_gate_reply_markup(entry, key),
        )
        return DispatchPolicyResult(
            terminal=True,
            terminal_reason="verifier gate: no verifier role is available",
        )

    if plan_gate_blocked:
        send(
            "plan gate blocked: critic issues remain after auto-replan.\n"
            f"reason: {plan_gate_reason or 'unresolved issues'}\n"
            "hint: 요청을 더 구체화하거나 역할/범위를 줄여 다시 실행하세요.\n"
            f"replan_attempts: {len(plan_replans)}",
            context="planning-gate",
            with_menu=True,
            reply_markup=early_gate_reply_markup(entry, key),
        )
        return DispatchPolicyResult(
            terminal=True,
            terminal_reason=f"plan gate: {plan_gate_reason or 'unresolved issues'}",
        )

    return DispatchPolicyResult(
        terminal=False,
        dispatch_roles=dispatch_roles,
        selected_roles=selected_roles,
        verifier_roles=verifier_roles,
        verifier_added=verifier_added,
    )


def resolve_effective_run_options(
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


def build_dry_run_preview(
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
