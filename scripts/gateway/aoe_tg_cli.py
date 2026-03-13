#!/usr/bin/env python3
"""Gateway CLI/bootstrap helpers extracted from the monolith."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Dict, Optional


def shutil_which(binary: str) -> Optional[str]:
    for folder in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(folder) / binary
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


def build_parser(*, deps: Dict[str, Any]) -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="aoe-telegram-gateway", description="Telegram polling gateway for aoe-orch")
    p.add_argument("--bot-token", default=os.environ.get("TELEGRAM_BOT_TOKEN", ""))
    p.add_argument("--project-root", default=".")
    p.add_argument("--team-dir")
    p.add_argument("--state-file")
    p.add_argument("--manager-state-file", default=os.environ.get("AOE_ORCH_MANAGER_STATE", ""))
    p.add_argument("--chat-aliases-file", default=os.environ.get("AOE_CHAT_ALIASES_FILE", ""))
    p.add_argument("--instance-lock-file", default=os.environ.get("AOE_GATEWAY_INSTANCE_LOCK", ""))
    p.add_argument("--workspace-root", default=os.environ.get("AOE_WORKSPACE_ROOT", ""))
    p.add_argument(
        "--orch-auto-discover",
        action="store_true",
        default=deps["bool_from_env"](os.environ.get("AOE_ORCH_AUTO_DISCOVER"), False),
        help="auto-register orch projects from `aoe list` under --workspace-root",
    )
    p.add_argument("--no-orch-auto-discover", dest="orch_auto_discover", action="store_false", help="disable orch auto-discovery")
    p.add_argument(
        "--orch-auto-init",
        action="store_true",
        default=deps["bool_from_env"](os.environ.get("AOE_ORCH_AUTO_INIT"), False),
        help="when auto-discover finds a project, create <project_root>/.aoe-team and seed AOE_TODO.md if missing",
    )
    p.add_argument("--no-orch-auto-init", dest="orch_auto_init", action="store_false", help="disable seeding .aoe-team on auto-discover")
    p.add_argument("--owner-chat-id", default=os.environ.get("TELEGRAM_OWNER_CHAT_ID", os.environ.get("AOE_OWNER_CHAT_ID", "")))
    p.add_argument(
        "--owner-only",
        action="store_true",
        default=deps["bool_from_env"](os.environ.get("AOE_OWNER_ONLY"), False),
        help="accept messages only from the owner account (from.id) in private chat",
    )
    p.add_argument("--no-owner-only", dest="owner_only", action="store_false", help="disable owner-only enforcement")
    p.add_argument("--allow-chat-ids", default=os.environ.get("TELEGRAM_ALLOW_CHAT_IDS", ""))
    p.add_argument("--admin-chat-ids", default=os.environ.get("TELEGRAM_ADMIN_CHAT_IDS", ""))
    p.add_argument("--readonly-chat-ids", default=os.environ.get("TELEGRAM_READONLY_CHAT_IDS", ""))
    p.add_argument(
        "--deny-by-default",
        action="store_true",
        default=deps["bool_from_env"](os.environ.get("AOE_DENY_BY_DEFAULT"), deps["DEFAULT_DENY_BY_DEFAULT"]),
        help="deny all chats unless allowlist matches (bootstrap /lockme when empty)",
    )
    p.add_argument("--no-deny-by-default", dest="deny_by_default", action="store_false", help="legacy mode: allow all chats when allowlist is empty")
    p.add_argument("--aoe-orch-bin", default=os.environ.get("AOE_ORCH_BIN", str(Path.home() / ".local/bin/aoe-orch")))
    p.add_argument("--aoe-team-bin", default=os.environ.get("AOE_TEAM_BIN", str(Path.home() / ".local/bin/aoe-team")))
    p.add_argument("--roles", help="fixed role csv passed to aoe-orch run")
    p.add_argument("--default-lang", default=os.environ.get("AOE_DEFAULT_LANG", deps["DEFAULT_UI_LANG"]), help="default interface/help language when chat-specific lang is unset (ko|en)")
    p.add_argument("--default-reply-lang", default=os.environ.get("AOE_DEFAULT_REPLY_LANG", deps["DEFAULT_REPLY_LANG"]), help="default orchestrator answer language (ko|en)")
    p.add_argument("--default-report-level", default=os.environ.get("AOE_DEFAULT_REPORT_LEVEL", deps["DEFAULT_REPORT_LEVEL"]), help="default report verbosity when chat-specific report_level is unset (short|normal|long)")
    p.add_argument("--priority", default="P2")
    p.add_argument("--orch-timeout-sec", type=int, default=deps["DEFAULT_ORCH_TIMEOUT_SEC"])
    p.add_argument("--orch-poll-sec", type=float, default=deps["DEFAULT_ORCH_POLL_SEC"])
    p.add_argument("--orch-command-timeout-sec", type=int, default=deps["DEFAULT_ORCH_COMMAND_TIMEOUT_SEC"])
    p.add_argument("--no-spawn-missing", action="store_true")
    p.add_argument("--no-wait", action="store_true")
    p.add_argument("--auto-dispatch", action="store_true", default=(os.environ.get("AOE_AUTO_DISPATCH", "0").strip().lower() in {"1", "true", "yes", "on"}), help="enable keyword-based automatic dispatch to worker roles")
    p.add_argument("--no-auto-dispatch", dest="auto_dispatch", action="store_false", help="disable keyword-based automatic dispatch (default)")
    p.add_argument("--slash-only", action="store_true", default=deps["bool_from_env"](os.environ.get("AOE_SLASH_ONLY"), deps["DEFAULT_SLASH_ONLY"]), help="require slash commands in Telegram (plain text only allowed in pending mode)")
    p.add_argument("--no-slash-only", dest="slash_only", action="store_false", help="allow loose text parsing and CLI-style text in Telegram")
    p.add_argument("--owner-bootstrap-mode", default=os.environ.get("AOE_OWNER_BOOTSTRAP_MODE", ""), help="owner convenience: if default_mode is unset, set it to dispatch/direct on first owner message")
    p.add_argument("--require-verifier", action="store_true", default=(os.environ.get("AOE_REQUIRE_VERIFIER", "1").strip().lower() in {"1", "true", "yes", "on"}), help="require verifier-role completion before integration/close")
    p.add_argument("--no-require-verifier", dest="require_verifier", action="store_false", help="disable verifier gate")
    p.add_argument("--verifier-roles", default=os.environ.get("AOE_VERIFIER_ROLES", deps["DEFAULT_VERIFIER_ROLES"]), help="comma-separated verifier role names (default: Codex-Reviewer,QA,Verifier)")

    plan_max_raw = (os.environ.get("AOE_PLAN_MAX_SUBTASKS", "") or "").strip()
    try:
        plan_max_default = max(1, int(plan_max_raw or str(deps["DEFAULT_TASK_PLAN_MAX_SUBTASKS"])))
    except ValueError:
        plan_max_default = deps["DEFAULT_TASK_PLAN_MAX_SUBTASKS"]

    plan_replan_raw = (os.environ.get("AOE_PLAN_REPLAN_ATTEMPTS", "") or "").strip()
    try:
        plan_replan_default = max(0, int(plan_replan_raw or str(deps["DEFAULT_TASK_PLAN_REPLAN_ATTEMPTS"])))
    except ValueError:
        plan_replan_default = deps["DEFAULT_TASK_PLAN_REPLAN_ATTEMPTS"]

    plan_phase1_rounds_raw = (os.environ.get("AOE_PLAN_PHASE1_ROUNDS", "") or "").strip()
    try:
        plan_phase1_rounds_default = max(3, int(plan_phase1_rounds_raw or "3"))
    except ValueError:
        plan_phase1_rounds_default = 3

    plan_phase1_min_providers_default = deps["int_from_env"](
        os.environ.get("AOE_PLAN_PHASE1_MIN_PROVIDERS"),
        2,
        1,
        8,
    )

    p.add_argument("--task-planning", action="store_true", default=(os.environ.get("AOE_TASK_PLANNING", "1").strip().lower() in {"1", "true", "yes", "on"}), help="enable planner/critic sub-task decomposition before dispatch")
    p.add_argument("--no-task-planning", dest="task_planning", action="store_false", help="disable planner/critic decomposition")
    p.add_argument("--plan-max-subtasks", type=int, default=plan_max_default, help="maximum subtasks generated by planner")
    p.add_argument("--plan-phase1-ensemble", action="store_true", default=(os.environ.get("AOE_PLAN_PHASE1_ENSEMBLE", "1").strip().lower() in {"1", "true", "yes", "on"}), help="use multi-provider Phase1 planning before execution")
    p.add_argument("--no-plan-phase1-ensemble", dest="plan_phase1_ensemble", action="store_false", help="disable multi-provider Phase1 planning")
    p.add_argument("--plan-phase1-rounds", type=int, default=plan_phase1_rounds_default, help="minimum number of Phase1 planning ensemble rounds (default: 3)")
    p.add_argument("--plan-phase1-providers", default=os.environ.get("AOE_PLAN_PHASE1_PROVIDERS", "codex,claude"), help="comma-separated providers for Phase1 ensemble planning")
    p.add_argument("--plan-phase1-min-providers", type=int, default=plan_phase1_min_providers_default, help="minimum required providers for Phase1 ensemble planning")
    p.add_argument("--plan-auto-replan", action="store_true", default=(os.environ.get("AOE_PLAN_AUTO_REPLAN", "1").strip().lower() in {"1", "true", "yes", "on"}), help="auto-replan when critic finds blocking issues")
    p.add_argument("--no-plan-auto-replan", dest="plan_auto_replan", action="store_false", help="disable automatic replanning")
    p.add_argument("--plan-replan-attempts", type=int, default=plan_replan_default, help="maximum automatic replanning attempts")
    p.add_argument("--plan-block-on-critic", action="store_true", default=(os.environ.get("AOE_PLAN_BLOCK_ON_CRITIC", "1").strip().lower() in {"1", "true", "yes", "on"}), help="block dispatch if critic issues remain after replanning")
    p.add_argument("--no-plan-block-on-critic", dest="plan_block_on_critic", action="store_false", help="allow dispatch even if critic issues remain")
    p.add_argument("--exec-critic", action="store_true", default=deps["bool_from_env"](os.environ.get("AOE_EXEC_CRITIC"), True), help="enable post-execution critic verdict (success/retry/fail) for completed dispatch runs")
    p.add_argument("--no-exec-critic", dest="exec_critic", action="store_false", help="disable post-execution critic verdict and auto-retry logic")
    p.add_argument("--exec-critic-retry-max", type=int, default=deps["int_from_env"](os.environ.get("AOE_EXEC_RETRY_MAX"), 3, 1, 9), help="max total attempts (including the first) when critic returns retry")
    p.add_argument("--poll-timeout-sec", type=int, default=deps["DEFAULT_POLL_TIMEOUT_SEC"])
    p.add_argument("--http-timeout-sec", type=int, default=deps["DEFAULT_HTTP_TIMEOUT_SEC"])
    p.add_argument("--max-text-chars", type=int, default=deps["DEFAULT_MAX_TEXT_CHARS"])
    p.add_argument("--confirm-ttl-sec", type=int, default=deps["int_from_env"](os.environ.get("AOE_CONFIRM_TTL_SEC"), deps["DEFAULT_CONFIRM_TTL_SEC"], 30, 86400), help="seconds to keep high-risk auto-run confirmation pending")
    p.add_argument("--chat-max-running", type=int, default=deps["int_from_env"](os.environ.get("AOE_CHAT_MAX_RUNNING"), deps["DEFAULT_CHAT_MAX_RUNNING"], 0, 50), help="max concurrent pending/running tasks per chat (0 disables)")
    p.add_argument("--chat-daily-cap", type=int, default=deps["int_from_env"](os.environ.get("AOE_CHAT_DAILY_CAP"), deps["DEFAULT_CHAT_DAILY_CAP"], 0, 10000), help="max tasks created per chat per day (0 disables)")
    p.add_argument("--once", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--simulate-text", help="process a single local text message (no telegram polling)")
    p.add_argument("--simulate-chat-id", default="local-sim")
    p.add_argument("--simulate-live", action="store_true", help="allow --simulate-text to execute (default: forces --dry-run for safety)")
    return p


def normalize_main_args(args: Any, *, deps: Dict[str, Any]) -> Any:
    args.project_root = deps["resolve_project_root"](args.project_root)
    args.team_dir = deps["resolve_team_dir"](args.project_root, args.team_dir)
    args.state_file = deps["resolve_state_file"](args.project_root, args.state_file)
    args.manager_state_file = deps["resolve_manager_state_file"](args.team_dir, args.manager_state_file)
    args.chat_aliases_file = deps["resolve_chat_aliases_file"](args.team_dir, args.chat_aliases_file)
    if str(args.instance_lock_file or "").strip():
        args.instance_lock_file = Path(str(args.instance_lock_file)).expanduser().resolve()
    else:
        args.instance_lock_file = (args.team_dir / ".gateway.instance.lock").resolve()
    args.workspace_root = deps["resolve_workspace_root"](args.workspace_root)
    args.owner_chat_id = deps["normalize_owner_chat_id"](args.owner_chat_id)
    args.owner_bootstrap_mode = (
        deps["normalize_mode_token"](str(getattr(args, "owner_bootstrap_mode", "") or "").strip())
        if str(getattr(args, "owner_bootstrap_mode", "") or "").strip()
        else ""
    )
    if args.owner_bootstrap_mode not in {"dispatch", "direct"}:
        args.owner_bootstrap_mode = ""
    args.default_lang = deps["normalize_chat_lang_token"](args.default_lang, deps["DEFAULT_UI_LANG"]) or deps["DEFAULT_UI_LANG"]
    args.default_reply_lang = deps["normalize_chat_lang_token"](args.default_reply_lang, deps["DEFAULT_REPLY_LANG"]) or deps["DEFAULT_REPLY_LANG"]
    raw_default_report = deps["normalize_report_token"](str(getattr(args, "default_report_level", "") or "").strip())
    args.default_report_level = raw_default_report if raw_default_report in {"short", "normal", "long"} else deps["DEFAULT_REPORT_LEVEL"]
    args.allow_chat_ids = deps["parse_csv_set"](args.allow_chat_ids)
    args.admin_chat_ids = deps["parse_csv_set"](args.admin_chat_ids)
    args.readonly_chat_ids = deps["parse_csv_set"](args.readonly_chat_ids)
    args.readonly_chat_ids = {x for x in args.readonly_chat_ids if x not in args.admin_chat_ids}
    args.chat_alias_cache = deps["load_chat_aliases"](args.chat_aliases_file)
    return args


def main(*, deps: Dict[str, Any]) -> int:
    parser = build_parser(deps=deps)
    args = parser.parse_args()
    args = normalize_main_args(args, deps=deps)

    manager_state = deps["load_manager_state"](args.manager_state_file, args.project_root, args.team_dir)
    deps["ensure_default_project_registered"](manager_state, args.project_root, args.team_dir)
    if not args.dry_run:
        deps["save_manager_state"](args.manager_state_file, manager_state)

    token = (args.bot_token or "").strip()
    if not token and not args.simulate_text:
        raise SystemExit("[ERROR] missing bot token (set --bot-token or TELEGRAM_BOT_TOKEN)")

    if bool(getattr(args, "owner_only", False)) and not str(args.owner_chat_id or "").strip():
        raise SystemExit("[ERROR] owner-only requires TELEGRAM_OWNER_CHAT_ID/--owner-chat-id to be set")

    if not Path(args.aoe_orch_bin).exists() and not shutil_which(args.aoe_orch_bin):
        raise SystemExit(f"[ERROR] aoe-orch binary not found: {args.aoe_orch_bin}")

    if not Path(args.aoe_team_bin).exists() and not shutil_which(args.aoe_team_bin):
        raise SystemExit(f"[ERROR] aoe-team binary not found: {args.aoe_team_bin}")

    process_lock = None
    if (not args.simulate_text) and (not args.dry_run):
        try:
            process_lock = deps["acquire_process_lock"](args.instance_lock_file)
        except Exception as e:
            raise SystemExit(f"[ERROR] {e}")

    if args.simulate_text:
        deps["run_simulation"](args, token=token)
        return 0

    rc = deps["run_loop"](args, token=token)
    _ = process_lock
    return rc
