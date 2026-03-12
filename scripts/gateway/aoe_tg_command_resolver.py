#!/usr/bin/env python3
"""Command resolution layer for incoming Telegram text."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from aoe_tg_acl import parse_acl_command_args, parse_acl_revoke_args
from aoe_tg_parse import (
    infer_natural_run_mode,
    normalize_lang_token,
    normalize_mode_token,
    normalize_report_token,
    parse_cli_message,
    parse_command,
    parse_quick_message,
)
from aoe_tg_orch_actions import action_call_to_resolved_command, infer_mother_orch_action_call

_ABBREV_COMMANDS = [
    # Stable surface for prefix-based abbreviation (Telegram UX sugar).
    "on",
    "off",
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
    "focus",
    "unlock",
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
    "add-role",
    "add-claude",
    "add-codex",
    "add-shell",
]


def _expand_command_abbrev(token: str) -> str:
    """Expand a unique prefix into a full command token."""
    raw = str(token or "").strip().lower()
    if not raw:
        return raw
    if raw in _ABBREV_COMMANDS:
        return raw
    matches = [c for c in _ABBREV_COMMANDS if c.startswith(raw)]
    if len(matches) == 1:
        return matches[0]
    return raw


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
    lang_setting: Optional[str] = None
    report_setting: Optional[str] = None
    acl_grant_scope: Optional[str] = None
    acl_grant_chat_id: Optional[str] = None
    acl_revoke_scope: Optional[str] = None
    acl_revoke_chat_id: Optional[str] = None

    run_auto_source: str = ""


def _default_project_key_for_plaintext(manager_state: Dict[str, Any]) -> str:
    projects = manager_state.get("projects")
    if not isinstance(projects, dict):
        return ""
    project_lock = manager_state.get("project_lock")
    if isinstance(project_lock, dict) and bool(project_lock.get("enabled")):
        token = str(project_lock.get("project_key", "")).strip()
        if token and isinstance(projects.get(token), dict):
            return token
    active = str(manager_state.get("active", "")).strip()
    if active and isinstance(projects.get(active), dict):
        return active
    return ""


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

    def _apply_add_role_cli(cli: Dict[str, Any]) -> None:
        out.cmd = "add-role"
        out.add_role_name = str(cli.get("role", "")).strip()
        out.add_role_provider = cli.get("provider")
        out.add_role_launch = cli.get("launch")
        out.add_role_spawn = bool(cli.get("spawn", True))

    cmd, rest = parse_command(text)
    out.cmd = _expand_command_abbrev(str(cmd or "").strip().lower())
    out.rest = str(rest or "").strip()
    out.came_from_slash = bool(out.cmd)

    if out.cmd:
        slash_rest = str(out.rest or "").strip()
        if out.cmd in {"menu"}:
            out.cmd = "help"
        elif out.cmd in {"tutorial", "tut", "guide", "quickstart", "start-here", "onboard", "온보딩", "튜토리얼"}:
            out.cmd = "tutorial"
            out.rest = slash_rest
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
        elif out.cmd in {"replay"}:
            out.cmd = "replay"
            out.rest = slash_rest
        elif out.cmd in {"id", "whoami"}:
            out.cmd = "whoami"
        elif out.cmd in {"add-role", "addrole", "add-claude", "addclaude", "add-codex", "addcodex", "add-shell", "addshell"}:
            cli = parse_cli_message(f"aoe {out.cmd} {slash_rest}".strip())
            if not cli:
                raise RuntimeError("usage: /add-role <Role> [--provider <name>] [--launch <cmd>] [--spawn|--no-spawn]")
            _apply_add_role_cli(cli)
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
        elif out.cmd in {"lang", "language", "locale", "언어"}:
            out.cmd = "lang"
            out.lang_setting = normalize_lang_token(slash_rest)
            if not out.lang_setting:
                raise RuntimeError("usage: /lang [ko|en]")
        elif out.cmd in {"report", "verbosity", "보고", "리포트"}:
            out.cmd = "report"
            out.report_setting = normalize_report_token(slash_rest)
            if not out.report_setting:
                raise RuntimeError("usage: /report [short|normal|long|off]")
        elif out.cmd in {"brief"}:
            out.cmd = "report"
            out.report_setting = "short"
        elif out.cmd in {"verbose", "detail"}:
            out.cmd = "report"
            out.report_setting = "long"
        elif out.cmd == "lockme":
            out.cmd = "lockme"
        elif out.cmd == "onlyme":
            out.cmd = "onlyme"
        elif out.cmd in {"acl", "auth", "permission", "permissions"}:
            out.cmd = "acl"
        elif out.cmd in {"map", "maps", "table"}:
            out.cmd = "orch-list"
        elif out.cmd in {"use", "switch"}:
            out.cmd = "orch-use"
            out.orch_target = slash_rest or None
            if not out.orch_target:
                raise RuntimeError("usage: /use <orch>")
        elif out.cmd in {"focus", "pin"}:
            out.cmd = "focus"
            out.rest = slash_rest
        elif out.cmd in {"unlock", "unfocus", "release"}:
            if slash_rest:
                raise RuntimeError("usage: /unlock")
            out.cmd = "focus"
            out.rest = "off"
        elif out.cmd in {"orch"}:
            tokens = [t for t in str(slash_rest or "").split() if t.strip()]
            if not tokens:
                out.cmd = "orch-list"
            else:
                sub = tokens[0].strip().lower()
                tail = tokens[1:]
                if sub in {"list", "ls", "map"}:
                    out.cmd = "orch-list"
                elif sub in {"use", "switch", "select"}:
                    out.cmd = "orch-use"
                    out.orch_target = tail[0].strip() if tail else None
                    if not out.orch_target:
                        raise RuntimeError("usage: /orch use <O#|name>")
                elif sub in {"pause", "hold", "stop"}:
                    out.cmd = "orch-pause"
                    out.orch_target = tail[0].strip() if tail else None
                    out.rest = " ".join(tail[1:]).strip() if len(tail) > 1 else ""
                    if not out.orch_target:
                        raise RuntimeError("usage: /orch pause <O#|name> [reason]")
                elif sub in {"resume", "unpause", "start"}:
                    out.cmd = "orch-resume"
                    out.orch_target = tail[0].strip() if tail else None
                    if not out.orch_target:
                        raise RuntimeError("usage: /orch resume <O#|name>")
                elif sub in {"hide"}:
                    out.cmd = "orch-hide"
                    out.orch_target = tail[0].strip() if tail else None
                    out.rest = " ".join(tail[1:]).strip() if len(tail) > 1 else ""
                    if not out.orch_target:
                        raise RuntimeError("usage: /orch hide <O#|name> [reason]")
                elif sub in {"unhide", "show"}:
                    out.cmd = "orch-unhide"
                    out.orch_target = tail[0].strip() if tail else None
                    if not out.orch_target:
                        raise RuntimeError("usage: /orch unhide <O#|name>")
                elif sub in {"repair", "init", "fix"}:
                    out.cmd = "orch-repair"
                    out.orch_target = tail[0].strip() if tail else None
                elif sub in {"status", "stat"}:
                    out.cmd = "orch-status"
                    out.orch_target = tail[0].strip() if tail else None
                else:
                    raise RuntimeError("usage: /orch [list|use|pause|resume|repair|status]")
        elif out.cmd in {"todo", "todos"}:
            out.cmd = "todo"
            out.rest = slash_rest
        elif out.cmd in {"room", "rooms", "r"}:
            out.cmd = "room"
            out.rest = slash_rest
        elif out.cmd in {"gc", "cleanup"}:
            out.cmd = "gc"
            out.rest = slash_rest
        elif out.cmd in {"panic", "halt", "stopall", "emergency"}:
            out.cmd = "panic"
            out.rest = slash_rest
        elif out.cmd in {"offdesk", "off-desk", "od", "night"}:
            out.cmd = "offdesk"
            out.rest = slash_rest
        elif out.cmd in {"auto"}:
            out.cmd = "auto"
            out.rest = slash_rest
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
            elif out.cmd in {"orch-use", "orch-status", "orch-repair"}:
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
            elif out.cmd == "lang":
                token = str(quick.get("lang", "status")).strip().lower()
                out.lang_setting = token if token in {"status", "ko", "en"} else "invalid"
            elif out.cmd == "report":
                token = str(quick.get("report", "status")).strip().lower()
                out.report_setting = token if token in {"status", "short", "normal", "long", "off"} else "invalid"
            elif out.cmd == "auto":
                out.rest = str(quick.get("rest", "")).strip()
            elif out.cmd == "replay":
                out.rest = str(quick.get("target", "")).strip()

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
                _apply_add_role_cli(cli)
            elif out.cmd in {"orch-use", "orch-status", "orch-repair"}:
                out.orch_target = cli.get("orch")
            elif out.cmd == "orch-add":
                out.orch_add_name = str(cli.get("orch", "")).strip()
                out.orch_add_path = str(cli.get("path", "")).strip()
                out.orch_add_overview = cli.get("overview")
                out.orch_add_init = bool(cli.get("init", True))
                out.orch_add_spawn = bool(cli.get("spawn", True))
                out.orch_add_set_active = bool(cli.get("set_active", True))
            elif out.cmd in {"orch-pause", "orch-resume", "orch-hide", "orch-unhide"}:
                out.orch_target = cli.get("orch")
                out.rest = str(cli.get("rest", "")).strip()
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
            elif out.cmd == "lang":
                token = str(cli.get("lang", "status")).strip().lower()
                out.lang_setting = token if token in {"status", "ko", "en"} else ""
            elif out.cmd == "report":
                token = str(cli.get("report", "status")).strip().lower()
                out.report_setting = token if token in {"status", "short", "normal", "long", "off"} else ""
            elif out.cmd == "replay":
                out.rest = str(cli.get("target", "")).strip()
            elif out.cmd == "todo":
                out.rest = str(cli.get("rest", "")).strip()
            elif out.cmd == "next":
                out.rest = str(cli.get("rest", "")).strip()
            elif out.cmd == "queue":
                out.rest = str(cli.get("rest", "")).strip()
            elif out.cmd == "drain":
                out.rest = str(cli.get("rest", "")).strip()
            elif out.cmd == "panic":
                out.rest = str(cli.get("rest", "")).strip()
            elif out.cmd == "offdesk":
                out.rest = str(cli.get("rest", "")).strip()
            elif out.cmd == "auto":
                out.rest = str(cli.get("rest", "")).strip()
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
            default_project_key = _default_project_key_for_plaintext(manager_state)
            has_active_task = bool(default_project_key)
            action_call = infer_mother_orch_action_call(
                pending_prompt,
                default_project_key=default_project_key,
                has_active_task=has_active_task,
            )
            mapped = action_call_to_resolved_command(action_call)
            out.cmd = str(mapped.get("cmd", "")).strip().lower()
            out.rest = str(mapped.get("rest", "")).strip()
            out.run_prompt = str(mapped.get("run_prompt", "")).strip()
            out.run_force_mode = mapped.get("run_force_mode")
            out.run_auto_source = (
                str(mapped.get("run_auto_source", "")).strip()
                or f"orch-action:{str(action_call.get('action', '')).strip()}"
            )
            out.orch_target = mapped.get("orch_target") or out.orch_target
            out.orch_task_request_id = mapped.get("orch_task_request_id") or out.orch_task_request_id
            out.orch_retry_request_id = mapped.get("orch_retry_request_id") or out.orch_retry_request_id
            out.orch_replan_request_id = mapped.get("orch_replan_request_id") or out.orch_replan_request_id
            if not out.cmd:
                default_mode = get_default_mode(manager_state, chat_id)
                if default_mode in {"dispatch", "direct"}:
                    effective_mode = default_mode
                    inferred_mode = infer_natural_run_mode(pending_prompt, default_mode)
                    if inferred_mode in {"dispatch", "direct"}:
                        effective_mode = inferred_mode
                    out.cmd = "run"
                    out.run_prompt = pending_prompt
                    out.run_force_mode = effective_mode
                    out.run_auto_source = "default-intent" if effective_mode != default_mode else "default"

    if not out.cmd and bool(slash_only):
        natural = parse_quick_message(text)
        if natural:
            ncmd = str(natural.get("cmd", "")).strip().lower()
            safe_cmds = {
                "help",
                "confirm-run",
                "mode",
                "lang",
                "acl",
                "orch-list",
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
                "replay",
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
                elif ncmd == "lang":
                    token = str(natural.get("lang", "status")).strip().lower()
                    out.lang_setting = token if token in {"status", "ko", "en"} else "invalid"
                elif ncmd == "replay":
                    out.rest = str(natural.get("target", "")).strip()

    return out
