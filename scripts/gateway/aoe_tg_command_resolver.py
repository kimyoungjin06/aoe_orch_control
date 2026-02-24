#!/usr/bin/env python3
"""Command resolution layer for incoming Telegram text."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from aoe_tg_acl import parse_acl_command_args, parse_acl_revoke_args
from aoe_tg_parse import normalize_mode_token, parse_cli_message, parse_command, parse_quick_message


@dataclass
class ResolvedCommand:
    cmd: str = ""
    rest: str = ""
    came_from_slash: bool = False

    run_prompt: str = ""
    run_roles_override: Optional[str] = None
    run_priority_override: Optional[str] = None
    run_timeout_override: Optional[int] = None
    run_no_wait_override: Optional[bool] = None
    run_force_mode: Optional[str] = None

    add_role_name: Optional[str] = None
    add_role_provider: Optional[str] = None
    add_role_launch: Optional[str] = None
    add_role_spawn: bool = True

    orch_target: Optional[str] = None
    orch_add_name: Optional[str] = None
    orch_add_path: Optional[str] = None
    orch_add_overview: Optional[str] = None
    orch_add_init: bool = True
    orch_add_spawn: bool = True
    orch_add_set_active: bool = True
    orch_check_request_id: Optional[str] = None
    orch_task_request_id: Optional[str] = None
    orch_pick_request_id: Optional[str] = None
    orch_cancel_request_id: Optional[str] = None
    orch_retry_request_id: Optional[str] = None
    orch_replan_request_id: Optional[str] = None
    orch_monitor_limit: Optional[int] = None
    orch_kpi_hours: Optional[int] = None

    mode_setting: Optional[str] = None
    acl_grant_scope: Optional[str] = None
    acl_grant_chat_id: Optional[str] = None
    acl_revoke_scope: Optional[str] = None
    acl_revoke_chat_id: Optional[str] = None

    run_auto_source: str = ""


def resolve_message_command(
    text: str,
    slash_only: bool,
    manager_state: Dict[str, Any],
    chat_id: str,
    dry_run: bool,
    manager_state_file: Path,
    get_pending_mode: Callable[[Dict[str, Any], str], str],
    get_default_mode: Callable[[Dict[str, Any], str], str],
    clear_pending_mode: Callable[[Dict[str, Any], str], bool],
    save_manager_state: Callable[[Path, Dict[str, Any]], None],
) -> ResolvedCommand:
    out = ResolvedCommand()
    cmd, rest = parse_command(text)
    out.cmd = str(cmd or "").strip().lower()
    out.rest = str(rest or "").strip()
    out.came_from_slash = bool(out.cmd)

    if out.cmd:
        slash_rest = str(out.rest or "").strip()
        if out.cmd in {"menu"}:
            out.cmd = "help"
        elif out.cmd in {"ok", "confirm"}:
            if slash_rest:
                raise RuntimeError("usage: /ok")
            out.cmd = "confirm-run"
        elif out.cmd in {"cancel"}:
            if slash_rest:
                out.cmd = "orch-cancel"
                out.orch_cancel_request_id = slash_rest
            else:
                out.cmd = "cancel-pending"
        elif out.cmd in {"id", "whoami"}:
            out.cmd = "whoami"
        elif out.cmd in {"mode", "inbox", "on", "off"}:
            src_cmd = out.cmd
            out.cmd = "mode"
            if src_cmd in {"inbox", "on"} and not slash_rest:
                mode_arg = "dispatch"
            elif src_cmd == "off" and not slash_rest:
                mode_arg = "off"
            else:
                mode_arg = slash_rest
            out.mode_setting = normalize_mode_token(mode_arg)
            if not out.mode_setting:
                raise RuntimeError("usage: /mode [on|off|direct|dispatch]")
        elif out.cmd in {"lockme", "onlyme"}:
            out.cmd = "lockme"
        elif out.cmd in {"acl", "auth", "permission", "permissions"}:
            out.cmd = "acl"
        elif out.cmd in {"grant"}:
            out.cmd = "grant"
            out.acl_grant_scope, out.acl_grant_chat_id = parse_acl_command_args(
                slash_rest,
                "usage: /grant <allow|admin|readonly> <chat_id|alias>",
            )
        elif out.cmd in {"revoke"}:
            out.cmd = "revoke"
            out.acl_revoke_scope, out.acl_revoke_chat_id = parse_acl_revoke_args(
                slash_rest,
                "usage: /revoke <allow|admin|readonly|all> <chat_id|alias>",
            )
        elif out.cmd in {"retry"}:
            out.cmd = "orch-retry"
            out.orch_retry_request_id = slash_rest or None
        elif out.cmd in {"replan"}:
            out.cmd = "orch-replan"
            out.orch_replan_request_id = slash_rest or None
        elif out.cmd in {"monitor", "tasks", "board"}:
            out.cmd = "orch-monitor"
            if slash_rest:
                monitor_token = slash_rest.split()[0].strip()
                if monitor_token.isdigit():
                    out.orch_monitor_limit = max(1, min(50, int(monitor_token)))
                else:
                    out.orch_target = monitor_token
        elif out.cmd in {"check", "progress"}:
            out.cmd = "orch-check"
            out.orch_check_request_id = slash_rest or None
        elif out.cmd in {"kpi", "metrics"}:
            out.cmd = "orch-kpi"
            if slash_rest:
                kpi_token = slash_rest.split()[0].strip()
                if kpi_token.isdigit():
                    out.orch_kpi_hours = max(1, min(168, int(kpi_token)))
                else:
                    out.orch_target = kpi_token
        elif out.cmd in {"task", "lifecycle"}:
            out.cmd = "orch-task"
            out.orch_task_request_id = slash_rest or None
        elif out.cmd in {"pick", "select"}:
            out.cmd = "orch-pick"
            out.orch_pick_request_id = slash_rest or None
        elif out.cmd in {"dispatch", "team"}:
            if slash_rest:
                out.cmd = "run"
                out.run_force_mode = "dispatch"
                out.run_prompt = slash_rest
            else:
                out.cmd = "quick-dispatch"
        elif out.cmd in {"direct", "ask", "question"}:
            if slash_rest:
                out.cmd = "run"
                out.run_force_mode = "direct"
                out.run_prompt = slash_rest
            else:
                out.cmd = "quick-direct"

    if (not out.cmd) and (not bool(slash_only)):
        quick = parse_quick_message(text)
        if quick:
            out.cmd = str(quick.get("cmd", "")).strip().lower()
            if out.cmd == "request":
                out.rest = str(quick.get("request_id", "")).strip()
            elif out.cmd in {"run", "orch-run"}:
                out.run_prompt = str(quick.get("prompt", "")).strip()
                out.run_roles_override = quick.get("roles")
                out.run_priority_override = quick.get("priority")
                out.run_timeout_override = quick.get("timeout_sec")
                out.run_no_wait_override = bool(quick.get("no_wait", False))
                out.run_force_mode = quick.get("force_mode")
                out.orch_target = quick.get("orch")
            elif out.cmd in {"orch-use", "orch-status"}:
                out.orch_target = quick.get("orch")
            elif out.cmd == "orch-check":
                out.orch_target = quick.get("orch")
                out.orch_check_request_id = quick.get("request_id")
            elif out.cmd == "orch-task":
                out.orch_target = quick.get("orch")
                out.orch_task_request_id = quick.get("request_id")
            elif out.cmd == "orch-pick":
                out.orch_target = quick.get("orch")
                out.orch_pick_request_id = quick.get("request_id")
            elif out.cmd == "orch-cancel":
                out.orch_target = quick.get("orch")
                out.orch_cancel_request_id = quick.get("request_id")
            elif out.cmd == "orch-retry":
                out.orch_target = quick.get("orch")
                out.orch_retry_request_id = quick.get("request_id")
            elif out.cmd == "orch-replan":
                out.orch_target = quick.get("orch")
                out.orch_replan_request_id = quick.get("request_id")
            elif out.cmd == "orch-monitor":
                out.orch_target = quick.get("orch")
                out.orch_monitor_limit = quick.get("limit")
            elif out.cmd == "orch-kpi":
                out.orch_target = quick.get("orch")
                out.orch_kpi_hours = quick.get("hours")
            elif out.cmd == "mode":
                token = str(quick.get("mode", "status")).strip().lower()
                out.mode_setting = token if token in {"status", "dispatch", "direct", "off"} else "invalid"

    if (not out.cmd) and (not bool(slash_only)):
        cli = parse_cli_message(text)
        if cli:
            out.cmd = str(cli.get("cmd", "")).strip().lower()
            if out.cmd == "request":
                out.rest = str(cli.get("request_id", "")).strip()
            elif out.cmd in {"run", "orch-run"}:
                out.run_prompt = str(cli.get("prompt", "")).strip()
                out.run_roles_override = cli.get("roles")
                out.run_priority_override = cli.get("priority")
                out.run_timeout_override = cli.get("timeout_sec")
                out.run_no_wait_override = bool(cli.get("no_wait", False))
                out.run_force_mode = cli.get("force_mode")
                out.orch_target = cli.get("orch")
            elif out.cmd == "add-role":
                out.add_role_name = str(cli.get("role", "")).strip()
                out.add_role_provider = cli.get("provider")
                out.add_role_launch = cli.get("launch")
                out.add_role_spawn = bool(cli.get("spawn", True))
            elif out.cmd in {"orch-use", "orch-status"}:
                out.orch_target = cli.get("orch")
            elif out.cmd == "orch-add":
                out.orch_add_name = str(cli.get("orch", "")).strip()
                out.orch_add_path = str(cli.get("path", "")).strip()
                out.orch_add_overview = cli.get("overview")
                out.orch_add_init = bool(cli.get("init", True))
                out.orch_add_spawn = bool(cli.get("spawn", True))
                out.orch_add_set_active = bool(cli.get("set_active", True))
            elif out.cmd == "orch-check":
                out.orch_target = cli.get("orch")
                out.orch_check_request_id = cli.get("request_id")
            elif out.cmd == "orch-task":
                out.orch_target = cli.get("orch")
                out.orch_task_request_id = cli.get("request_id")
            elif out.cmd == "orch-pick":
                out.orch_target = cli.get("orch")
                out.orch_pick_request_id = cli.get("request_id")
            elif out.cmd == "orch-cancel":
                out.orch_target = cli.get("orch")
                out.orch_cancel_request_id = cli.get("request_id")
            elif out.cmd == "orch-retry":
                out.orch_target = cli.get("orch")
                out.orch_retry_request_id = cli.get("request_id")
            elif out.cmd == "orch-replan":
                out.orch_target = cli.get("orch")
                out.orch_replan_request_id = cli.get("request_id")
            elif out.cmd == "orch-monitor":
                out.orch_target = cli.get("orch")
                out.orch_monitor_limit = cli.get("limit")
            elif out.cmd == "orch-kpi":
                out.orch_target = cli.get("orch")
                out.orch_kpi_hours = cli.get("hours")
            elif out.cmd == "mode":
                token = str(cli.get("mode", "status")).strip().lower()
                out.mode_setting = token if token in {"status", "dispatch", "direct", "off"} else ""
            elif out.cmd == "grant":
                out.acl_grant_scope = str(cli.get("scope", "")).strip().lower() or None
                out.acl_grant_chat_id = str(cli.get("chat_id", "")).strip() or None
            elif out.cmd == "revoke":
                out.acl_revoke_scope = str(cli.get("scope", "")).strip().lower() or None
                out.acl_revoke_chat_id = str(cli.get("chat_id", "")).strip() or None

    if not out.cmd:
        pending_mode = get_pending_mode(manager_state, chat_id)
        pending_prompt = str(text or "").strip()
        if pending_mode in {"dispatch", "direct"} and pending_prompt:
            out.cmd = "run"
            out.run_prompt = pending_prompt
            out.run_force_mode = pending_mode
            out.run_auto_source = "pending"
            if clear_pending_mode(manager_state, chat_id) and (not dry_run):
                save_manager_state(manager_state_file, manager_state)
        elif pending_prompt:
            default_mode = get_default_mode(manager_state, chat_id)
            if default_mode in {"dispatch", "direct"}:
                out.cmd = "run"
                out.run_prompt = pending_prompt
                out.run_force_mode = default_mode
                out.run_auto_source = "default"

    if not out.cmd and bool(slash_only):
        natural = parse_quick_message(text)
        if natural:
            ncmd = str(natural.get("cmd", "")).strip().lower()
            safe_cmds = {
                "help",
                "confirm-run",
                "mode",
                "acl",
                "status",
                "orch-kpi",
                "orch-monitor",
                "orch-check",
                "orch-task",
                "orch-pick",
                "orch-cancel",
                "orch-retry",
                "orch-replan",
                "cancel-pending",
            }
            if ncmd in safe_cmds:
                out.cmd = ncmd
                if ncmd == "orch-check":
                    out.orch_check_request_id = natural.get("request_id")
                elif ncmd == "orch-task":
                    out.orch_task_request_id = natural.get("request_id")
                elif ncmd == "orch-pick":
                    out.orch_pick_request_id = natural.get("request_id")
                elif ncmd == "orch-cancel":
                    out.orch_cancel_request_id = natural.get("request_id")
                elif ncmd == "orch-retry":
                    out.orch_retry_request_id = natural.get("request_id")
                elif ncmd == "orch-replan":
                    out.orch_replan_request_id = natural.get("request_id")
                elif ncmd == "orch-monitor":
                    out.orch_monitor_limit = natural.get("limit")
                elif ncmd == "orch-kpi":
                    out.orch_kpi_hours = natural.get("hours")
                elif ncmd == "mode":
                    token = str(natural.get("mode", "status")).strip().lower()
                    out.mode_setting = token if token in {"status", "dispatch", "direct", "off"} else "invalid"

    return out
