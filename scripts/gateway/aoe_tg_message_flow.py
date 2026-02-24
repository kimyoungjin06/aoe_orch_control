#!/usr/bin/env python3
"""Message-flow helpers for Telegram gateway."""

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from aoe_tg_command_resolver import ResolvedCommand


@dataclass
class RunTransitionState:
    run_control_mode: str = ""
    run_source_request_id: str = ""
    run_source_task: Optional[Dict[str, Any]] = None


def apply_confirm_transition_to_resolved(resolved: ResolvedCommand, transition: Any) -> bool:
    if not isinstance(transition, dict):
        return False
    if bool(transition.get("terminal")):
        return True

    next_cmd = str(transition.get("cmd", resolved.cmd)).strip()
    if next_cmd:
        resolved.cmd = next_cmd

    resolved.run_prompt = str(transition.get("run_prompt", resolved.run_prompt)).strip()

    next_force_mode = transition.get("run_force_mode")
    if isinstance(next_force_mode, str) and next_force_mode.strip():
        resolved.run_force_mode = next_force_mode.strip()

    next_orch_target = transition.get("orch_target")
    if isinstance(next_orch_target, str) and next_orch_target.strip():
        resolved.orch_target = next_orch_target.strip()

    resolved.run_auto_source = str(transition.get("run_auto_source", resolved.run_auto_source)).strip() or resolved.run_auto_source
    return False


def apply_retry_transition_to_resolved(
    resolved: ResolvedCommand,
    run_transition: RunTransitionState,
    transition: Any,
) -> bool:
    if not isinstance(transition, dict):
        return False
    if bool(transition.get("terminal")):
        return True

    resolved.cmd = str(transition.get("cmd", "run")).strip() or "run"
    resolved.rest = str(transition.get("rest", "")).strip()

    next_orch_target = transition.get("orch_target")
    if isinstance(next_orch_target, str) and next_orch_target.strip():
        resolved.orch_target = next_orch_target.strip()

    resolved.run_prompt = str(transition.get("run_prompt", resolved.run_prompt or "")).strip()
    resolved.run_roles_override = transition.get("run_roles_override")

    next_force_mode = transition.get("run_force_mode")
    if isinstance(next_force_mode, str) and next_force_mode.strip():
        resolved.run_force_mode = next_force_mode.strip()

    resolved.run_no_wait_override = transition.get("run_no_wait_override")
    run_transition.run_control_mode = str(transition.get("run_control_mode", "")).strip() or run_transition.run_control_mode
    run_transition.run_source_request_id = (
        str(transition.get("run_source_request_id", "")).strip() or run_transition.run_source_request_id
    )
    source_task = transition.get("run_source_task")
    if isinstance(source_task, dict):
        run_transition.run_source_task = source_task
    return False


def enforce_command_auth(
    *,
    cmd_key: str,
    chat_role: str,
    chat_id: str,
    args: Any,
    send: Callable[..., bool],
    log_event: Callable[..., None],
    is_owner_chat: Callable[[str, Any], bool],
    readonly_allowed_commands: set[str],
    error_auth_code: str,
) -> bool:
    if chat_role == "unknown":
        if cmd_key not in {"start", "help", "whoami", "lockme"}:
            send("permission denied: unauthorized chat.", context="auth-deny", with_menu=True)
            log_event(event="auth_denied", stage="intake", status="rejected", error_code=error_auth_code, detail=f"role=unknown cmd={cmd_key}")
            return True
    elif cmd_key == "lockme":
        if args.allow_chat_ids and chat_role not in {"admin", "owner"}:
            send("permission denied: /lockme is admin-only after initial claim.", context="auth-deny", with_menu=True)
            log_event(event="auth_denied", stage="intake", status="rejected", error_code=error_auth_code, detail=f"role={chat_role} cmd=lockme")
            return True

    if cmd_key in {"lockme", "grant", "revoke"} and str(args.owner_chat_id or "").strip():
        if not is_owner_chat(chat_id, args):
            send(
                f"permission denied: /{cmd_key} is owner-only.\n"
                f"owner_chat_id: {args.owner_chat_id}",
                context="auth-deny",
                with_menu=True,
            )
            log_event(event="auth_denied", stage="intake", status="rejected", error_code=error_auth_code, detail=f"owner_only cmd={cmd_key}")
            return True
    elif chat_role == "readonly":
        if cmd_key not in readonly_allowed_commands:
            send(
                "permission denied: readonly chat.\n"
                "allowed: /status /check /task /monitor /pick /kpi /help /whoami /mode /acl",
                context="auth-deny",
                with_menu=True,
            )
            log_event(event="auth_denied", stage="intake", status="rejected", error_code=error_auth_code, detail=f"role=readonly cmd={cmd_key}")
            return True
    return False
