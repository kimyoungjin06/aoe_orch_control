#!/usr/bin/env python3
"""Telegram adapter for guarded Forager Remote Operator projections.

This adapter is intentionally narrow. It maps a small Telegram command surface
to read-only projections, remote Plan Mode receipts, exact gate resolution,
reviewed enqueue, and task-scoped runtime start/monitor receipts. It never
executes arbitrary shell text, starts unbound work, closes out completed work,
accepts runtime output as truth, or mutates project files directly.
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import pathlib
import shlex
import subprocess
import sys
import time
from typing import Any

from telegram_operator.agent import (
    DEFAULT_AGENT_CONFIG_FILE,
    chat_with_agent,
    classify_feedback_kind,
    classify_feedback_with_agent,
)
from telegram_operator.common import (
    RemoteOperatorTelegramError,
    append_jsonl,
    load_json,
    sha256_short,
    utc_now,
    write_json,
)
from telegram_operator.allowlist import (
    dispatch_templates,
    find_dispatch_template,
    load_dispatch_allowlist,
)
from telegram_operator.config import resolve_telegram_config
from telegram_operator.health import listener_health
from telegram_operator.redaction import public_remote_plan_session
from telegram_operator.base import attach_choice_surface, result_base
from telegram_operator.plan_workflow import (
    active_remote_plan_session,
    handle_remote_plan_session_input,
    remote_plan_selection_context,
    remote_plan_session_should_handle_text,
    store_remote_plan_session,
)
from telegram_operator.receipts import create_remote_plan_session
from telegram_operator.schemas import (
    FORBIDDEN_REMOTE_INTENTS,
    INTERACTION_CONTEXT_SCHEMA,
    RESULT_SCHEMA,
)
from telegram_operator.plan_messages import render_project_selection_message
from telegram_operator.dispatch import (
    apply_cancel_task,
    apply_session_input,
    apply_decision_action,
    apply_operator_pause,
    apply_operator_resume,
    apply_recovery_action,
    apply_runtime_dispatch,
    available_action_kinds,
    available_recovery_action_kinds,
    build_confirmation,
    cancellable_tasks_from_surface,
    clear_confirmation,
    confirmation_is_fresh,
    export_workstation_surface,
    find_action_envelope,
    find_recovery_envelope,
    open_decision_actions,
    open_recovery_actions,
    open_runtime_dispatch,
    pop_confirmation,
    runtime_dispatch_item,
    store_confirmation,
)
from telegram_operator.notifier import (
    attention_summary,
    build_attention_notification,
    render_attention_message,
)
from telegram_operator.project_candidates import (
    discover_project_paths,
    relative_path_hint,
    workspace_roots,
)
from telegram_operator.projects import (
    build_project_focus,
    find_project_dir,
    inspect_project_workspace,
    inspection_summary_lines,
    list_project_dir,
    load_registry,
    read_project_file,
    registry_summary,
    resolve_chat_focus,
)
from telegram_operator.persistence import (
    append_chat_history,
    chat_history_for_chat_hash,
    last_context_for_chat_hash,
    load_state,
    remember_context_for_chat_hash,
    save_state,
)
from telegram_operator.rendering import (
    agent_assistant_reply,
    choice_keyboard,
    render_decisions_message,
    render_dispatch_cancel_message,
    render_dispatch_confirm_message,
    render_dispatch_error_message,
    render_dispatch_result_message,
    render_attention_summary_message,
    render_cancel_task_confirm_message,
    render_cancel_task_result_message,
    render_pause_result_message,
    render_recovery_confirm_message,
    render_recovery_message,
    render_recovery_result_message,
    render_runtime_confirm_message,
    render_runtime_disabled_message,
    render_runtime_message,
    render_runtime_result_message,
    render_run_confirm_message,
    render_run_disabled_message,
    render_run_list_message,
    render_resume_confirm_message,
    render_resume_result_message,
    render_tasks_message,
    display_action,
    display_review_status,
    guide_message,
    help_message,
    mobile_card_contract,
    GUIDE_SHEET_MAX_CHARS,
    GUIDE_SHEET_MAX_LINES,
    number,
    profile_label_from_projection,
    projection_payload,
    render_chat_message,
    render_feedback_message,
    render_wiki_candidate_message,
    render_projection_message,
    sanitize_text,
    status_summary,
)
from telegram_operator.routing import (
    parse_remote_command,
    remote_plan_session_command_payload,
)
from telegram_operator.transport import get_updates, send_message
from telegram_operator.wiki import record_remember_candidate


DEFAULT_TELEGRAM_ENV_FILE = pathlib.Path(
    os.environ.get(
        "OFFDESK_TELEGRAM_ENV",
        "/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram.env",
    )
)
DEFAULT_STATE_FILE = pathlib.Path(
    os.environ.get(
        "OFFDESK_REMOTE_OPERATOR_TELEGRAM_STATE",
        str(pathlib.Path.home() / ".cache" / "forager" / "remote_operator_telegram_state.json"),
    )
)
DEFAULT_FEEDBACK_FILE = pathlib.Path(
    os.environ.get(
        "OFFDESK_REMOTE_OPERATOR_TELEGRAM_FEEDBACK",
        str(pathlib.Path.home() / ".cache" / "forager" / "remote_operator_telegram_feedback.jsonl"),
    )
)
DEFAULT_FEEDBACK_INGEST_DIR = pathlib.Path(
    os.environ.get(
        "OFFDESK_REMOTE_OPERATOR_TELEGRAM_FEEDBACK_INGEST_DIR",
        str(pathlib.Path.home() / ".cache" / "forager" / "remote_operator_telegram_feedback_ingest"),
    )
)
DEFAULT_REMOTE_PLAN_ARTIFACT_DIR = pathlib.Path(
    os.environ.get(
        "OFFDESK_REMOTE_OPERATOR_TELEGRAM_PLAN_ARTIFACT_DIR",
        str(pathlib.Path.home() / ".cache" / "forager" / "remote_operator_telegram_plan_sessions"),
    )
)
DEFAULT_LOOP_STATUS_FILE = pathlib.Path(
    os.environ.get(
        "OFFDESK_REMOTE_OPERATOR_TELEGRAM_LOOP_STATUS",
        str(pathlib.Path.home() / ".cache" / "forager" / "remote_operator_telegram_loop.json"),
    )
)
# Off by default: curated /run is opt-in, the operator must point this at a file
# of vetted command templates. Kept distinct from --enable-runtime-dispatch so a
# setup can allow named commands without allowing free-form ones.
_DISPATCH_ALLOWLIST_ENV = os.environ.get("OFFDESK_REMOTE_OPERATOR_DISPATCH_ALLOWLIST", "").strip()
DEFAULT_DISPATCH_ALLOWLIST_FILE = pathlib.Path(_DISPATCH_ALLOWLIST_ENV) if _DISPATCH_ALLOWLIST_ENV else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default=os.environ.get("FORAGER_PROFILE", "default"))
    parser.add_argument("--forager-bin", default=os.environ.get("FORAGER_BIN", "forager"))
    parser.add_argument("--env-file", type=pathlib.Path, default=DEFAULT_TELEGRAM_ENV_FILE)
    parser.add_argument("--state-file", type=pathlib.Path, default=DEFAULT_STATE_FILE)
    parser.add_argument("--feedback-file", type=pathlib.Path, default=DEFAULT_FEEDBACK_FILE)
    parser.add_argument("--feedback-ingest-dir", type=pathlib.Path, default=DEFAULT_FEEDBACK_INGEST_DIR)
    parser.add_argument("--remote-plan-artifact-dir", type=pathlib.Path, default=DEFAULT_REMOTE_PLAN_ARTIFACT_DIR)
    parser.add_argument("--loop-status-file", type=pathlib.Path, default=DEFAULT_LOOP_STATUS_FILE)
    parser.add_argument(
        "--no-decision-feedback-ingest",
        dest="decision_feedback_ingest",
        action="store_false",
        default=True,
        help="Record freeform Telegram feedback JSONL only; do not promote it to offdesk decisions.",
    )
    parser.add_argument("--out", type=pathlib.Path, help="Optional JSON result path.")
    parser.add_argument("--command-text", help="Deterministic command text, for tests or manual dry-runs.")
    parser.add_argument("--send-command-text", help="Render a read-only command and send it to the configured target chat.")
    parser.add_argument("--replay-update-file", type=pathlib.Path, help="Dry-run only: process local Telegram update JSON through the poller.")
    parser.add_argument("--projection-file", type=pathlib.Path, help="Dry-run only: render this read-only projection instead of invoking forager.")
    parser.add_argument("--dry-run", action="store_true", help="Do not call the Telegram API.")
    parser.add_argument("--once", action="store_true", help="Poll Telegram once and answer at most one update.")
    parser.add_argument("--health", action="store_true", help="Report local Telegram listener health and exit.")
    parser.add_argument("--health-max-age-sec", type=int, default=120)
    parser.add_argument(
        "--context-max-age-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_CONTEXT_MAX_AGE_SEC", "86400")),
        help="Maximum age for remembered Telegram card context; negative disables expiry.",
    )
    parser.add_argument(
        "--dispatch-confirm-ttl-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_DISPATCH_CONFIRM_TTL_SEC", "300")),
        help="Lifetime of a pending Telegram execution confirmation token.",
    )
    parser.add_argument(
        "--enable-runtime-dispatch",
        action="store_true",
        default=os.environ.get("OFFDESK_REMOTE_OPERATOR_ENABLE_RUNTIME_DISPATCH", "").strip().lower()
        in {"1", "true", "yes", "on"},
        help="Allow /dispatch to queue an operator-supplied runtime command. Off by default; "
        "this is remote command execution and should only be enabled on trusted setups.",
    )
    parser.add_argument(
        "--dispatch-allowlist-file",
        type=pathlib.Path,
        default=DEFAULT_DISPATCH_ALLOWLIST_FILE,
        help="Path to a JSON file of curated /run command templates ({\"templates\": "
        "[{\"name\", \"runner\", \"command\"}]}). When set, /run lets the operator dispatch "
        "a named, pre-vetted command without --enable-runtime-dispatch; the operator names a "
        "template and never supplies the command. The command is re-resolved from this file at "
        "confirm time, so removing a template revokes it even for an outstanding confirmation.",
    )
    parser.add_argument(
        "--attention-notify",
        action="store_true",
        default=os.environ.get("OFFDESK_REMOTE_OPERATOR_ATTENTION_NOTIFY", "").strip().lower()
        in {"1", "true", "yes", "on"},
        help="On each poll, proactively send the owner chat a card for newly waiting decisions "
        "and recovery follow-ups, so urgent items can be handled without polling first.",
    )
    parser.add_argument(
        "--autonomy-propose",
        action="store_true",
        default=os.environ.get("OFFDESK_REMOTE_OPERATOR_AUTONOMY_PROPOSE", "").strip().lower()
        in {"1", "true", "yes", "on"},
        help="When the workstation goes idle, propose starting the armed overnight autonomy "
        "window via a Telegram confirm card. The operator's tap is the approval; nothing arms itself.",
    )
    parser.add_argument("--autonomy-idle-min", type=int, default=30,
                        help="Idle minutes across watch paths before proposing.")
    parser.add_argument("--autonomy-propose-from-hour", type=int, default=17,
                        help="Do not propose before this local hour (avoids lunch-break proposals).")
    parser.add_argument("--autonomy-until-hour", type=int, default=9,
                        help="Local hour when an approved autonomy window closes.")
    parser.add_argument("--autonomy-backoff-sec", type=int, default=7200,
                        help="Minimum seconds between proposals (unanswered or declined).")
    parser.add_argument("--autonomy-watch-path", action="append", type=pathlib.Path, default=[],
                        help="Activity probe path (repeatable). Defaults to Claude/Codex session dirs.")
    parser.add_argument(
        "--session-notify",
        action="store_true",
        default=os.environ.get("OFFDESK_REMOTE_OPERATOR_SESSION_NOTIFY", "") == "1",
        help="Push a card when a supervised agent session sits in the waiting state "
        "(permission prompt / question) so the operator hears about it away from the desk.",
    )
    parser.add_argument("--session-notify-backoff-sec", type=int, default=1800,
                        help="Minimum seconds between cards for the same session.")
    parser.add_argument("--session-status-file", type=pathlib.Path, default=None,
                        help="Read session status JSON from this file instead of running "
                        "`forager status --json` (replay/testing).")
    parser.add_argument(
        "--attention-reminder-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_ATTENTION_REMINDER_SEC", "0")),
        help="Re-notify a still-waiting attention item after this many seconds. 0 notifies once.",
    )
    parser.add_argument(
        "--agent-intent-mode",
        choices=("auto", "off", "required"),
        default=os.environ.get("OFFDESK_REMOTE_OPERATOR_AGENT_INTENT_MODE", "auto"),
        help="Classify freeform Telegram text with a local agent when available.",
    )
    parser.add_argument("--agent-config-file", type=pathlib.Path, default=DEFAULT_AGENT_CONFIG_FILE)
    parser.add_argument("--agent-provider", default=os.environ.get("OFFDESK_REMOTE_OPERATOR_AGENT_PROVIDER"))
    parser.add_argument("--agent-base-url", action="append", default=[])
    parser.add_argument("--agent-model", action="append", default=[])
    parser.add_argument(
        "--agent-model-candidates",
        default=os.environ.get("OFFDESK_REMOTE_OPERATOR_AGENT_MODELS", ""),
        help="Comma-separated model preference list for Telegram intent classification.",
    )
    parser.add_argument("--agent-timeout-sec", type=int, default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_AGENT_TIMEOUT_SEC", "20")))
    parser.add_argument("--agent-num-ctx", type=int, default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_AGENT_NUM_CTX", "8192")))
    parser.add_argument("--agent-num-predict", type=int, default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_AGENT_NUM_PREDICT", "768")))
    parser.add_argument(
        "--project-init-timeout-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_PROJECT_INIT_TIMEOUT_SEC", "60")),
    )
    parser.add_argument(
        "--plan-draft-timeout-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_PLAN_DRAFT_TIMEOUT_SEC", "60")),
    )
    parser.add_argument(
        "--plan-registration-timeout-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_PLAN_REGISTRATION_TIMEOUT_SEC", "60")),
    )
    parser.add_argument(
        "--plan-review-timeout-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_PLAN_REVIEW_TIMEOUT_SEC", "60")),
    )
    parser.add_argument(
        "--plan-launch-prep-timeout-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_PLAN_LAUNCH_PREP_TIMEOUT_SEC", "60")),
    )
    parser.add_argument(
        "--gate-timeout-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_GATE_TIMEOUT_SEC", "60")),
    )
    parser.add_argument(
        "--execution-brief-ttl-minutes",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_EXECUTION_BRIEF_TTL_MINUTES", "30")),
    )
    parser.add_argument(
        "--enqueue-timeout-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_ENQUEUE_TIMEOUT_SEC", "60")),
    )
    parser.add_argument(
        "--runtime-start-timeout-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_RUNTIME_START_TIMEOUT_SEC", "60")),
    )
    parser.add_argument(
        "--runtime-monitor-timeout-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_RUNTIME_MONITOR_TIMEOUT_SEC", "60")),
    )
    parser.add_argument(
        "--closeout-timeout-sec",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_CLOSEOUT_TIMEOUT_SEC", "60")),
    )
    parser.add_argument(
        "--workspace-root",
        action="append",
        type=pathlib.Path,
        default=[],
        help="Workspace root to scan for remote Plan Mode project candidates. Can be repeated.",
    )
    parser.add_argument(
        "--max-project-candidates",
        type=int,
        default=int(os.environ.get("OFFDESK_REMOTE_OPERATOR_MAX_PROJECT_CANDIDATES", "3")),
        help="Maximum project candidates to present in Telegram planning sessions.",
    )
    parser.add_argument("--max-polls", type=int, help="Stop after this many polls; useful for smoke tests.")
    parser.add_argument("--poll-timeout-sec", type=int, default=5)
    parser.add_argument("--api-timeout-sec", type=int, default=20)
    parser.add_argument("--poll-error-backoff-sec", type=int, default=5)
    parser.add_argument("--max-message-chars", type=int, default=3500)
    return parser.parse_args()










def projection_command(forager_bin: str, profile: str, parsed: dict[str, Any]) -> list[str]:
    argv = [forager_bin]
    if profile:
        argv.extend(["--profile", profile])
    argv.extend(["offdesk", "remote-operator"])
    argv.extend(parsed["argv"])
    argv.extend(["--transport", "telegram", "--json"])
    return argv


def run_projection(forager_bin: str, profile: str, parsed: dict[str, Any]) -> dict[str, Any]:
    command = projection_command(forager_bin, profile, parsed)
    process = subprocess.run(
        command,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if process.returncode != 0:
        detail = sanitize_text(process.stderr.strip() or process.stdout.strip())
        raise RemoteOperatorTelegramError(
            f"forager remote operator projection failed: {detail}"
        )
    try:
        projection = json.loads(process.stdout)
    except json.JSONDecodeError as error:
        raise RemoteOperatorTelegramError("forager projection did not return JSON") from error
    validate_projection(projection, expected_command=parsed.get("command"))
    return projection


def decision_feedback_ingest_command(
    args: argparse.Namespace,
    feedback_path: pathlib.Path,
) -> list[str]:
    argv = [args.forager_bin]
    if args.profile:
        argv.extend(["--profile", args.profile])
    argv.extend(
        [
            "offdesk",
            "decision",
            "ingest-telegram-feedback",
            "--feedback",
            str(feedback_path),
            "--json",
        ]
    )
    return argv


def ingest_feedback_decision(
    args: argparse.Namespace,
    feedback_record: dict[str, Any],
) -> dict[str, Any]:
    if not args.decision_feedback_ingest:
        return {"decision_feedback_ingest_status": "disabled"}
    fingerprint = hashlib.sha256(
        json.dumps(feedback_record, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    message_id = feedback_record.get("message_id")
    suffix = str(message_id) if message_id is not None else fingerprint
    feedback_path = args.feedback_ingest_dir / f"telegram_feedback_{suffix}_{fingerprint}.json"
    write_json(feedback_path, feedback_record)
    command = decision_feedback_ingest_command(args, feedback_path)
    try:
        process = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except OSError as error:
        return {
            "decision_feedback_ingest_status": "error",
            "decision_feedback_ingest_file": str(feedback_path),
            "decision_feedback_ingest_error": sanitize_text(str(error), max_chars=300),
        }
    if process.returncode != 0:
        return {
            "decision_feedback_ingest_status": "error",
            "decision_feedback_ingest_file": str(feedback_path),
            "decision_feedback_ingest_error": sanitize_text(
                process.stderr.strip() or process.stdout.strip(),
                max_chars=300,
            ),
        }
    try:
        report = json.loads(process.stdout)
    except json.JSONDecodeError:
        return {
            "decision_feedback_ingest_status": "error",
            "decision_feedback_ingest_file": str(feedback_path),
            "decision_feedback_ingest_error": "decision ingest did not return JSON",
        }
    return {
        "decision_feedback_ingest_status": "recorded"
        if report.get("appended") is True
        else "existing",
        "decision_feedback_ingest_file": str(feedback_path),
        "decision_feedback_decision_id": report.get("decision_id"),
        "decision_feedback_appended": bool(report.get("appended")),
    }


def load_projection_file(path: pathlib.Path, parsed: dict[str, Any]) -> dict[str, Any]:
    try:
        projection = load_json(path)
    except OSError as error:
        raise RemoteOperatorTelegramError(f"projection file cannot be read: {path}") from error
    except json.JSONDecodeError as error:
        raise RemoteOperatorTelegramError(f"projection file is not valid JSON: {path}") from error
    if not isinstance(projection, dict):
        raise RemoteOperatorTelegramError("projection file must contain one JSON object")
    validate_projection(projection, expected_command=parsed.get("command"))
    return projection


def validate_projection(projection: dict[str, Any], *, expected_command: Any = None) -> None:
    if projection.get("schema") != "remote_operator_readonly_projection.v1":
        raise RemoteOperatorTelegramError("unexpected projection schema")
    if projection.get("read_only") is not True:
        raise RemoteOperatorTelegramError("projection is not read-only")
    if projection.get("mutation_authorized") is not False:
        raise RemoteOperatorTelegramError("projection unexpectedly authorizes mutation")
    if projection.get("approval_authorized") is not False:
        raise RemoteOperatorTelegramError("projection unexpectedly authorizes approval")
    expected = str(expected_command or "").strip()
    actual = str(projection.get("command") or "").strip()
    if expected and actual != expected:
        raise RemoteOperatorTelegramError(
            f"projection command mismatch: expected {expected}, got {actual or 'missing'}"
        )


































































































































































































































































def show_command_for(plan_id: Any) -> str:
    value = str(plan_id or "").strip()
    return f"/show {shlex.quote(value)}" if value else "/plans --latest"


def interaction_context_from_projection(projection: dict[str, Any]) -> dict[str, Any]:
    command = str(projection.get("command") or "").strip()
    payload = projection_payload(projection)
    profile = profile_label_from_projection(projection)
    context: dict[str, Any] = {
        "schema": INTERACTION_CONTEXT_SCHEMA,
        "command": command or "unknown",
        "profile": profile,
        "projection_generated_at": str(projection.get("generated_at") or ""),
        "context_kind": "generic",
        "focus_kind": None,
        "focus_ref": None,
        "focus_label": None,
        "next_command": None,
    }
    if command == "status":
        pending = number(payload, "pending_approvals")
        failed = number(payload, "failed_offdesk_tasks")
        closeout = number(payload, "closeout_required_offdesk_tasks")
        active = number(payload, "active_offdesk_tasks")
        queued = number(payload, "queued_offdesk_tasks")
        if pending:
            context.update(
                {
                    "context_kind": "status_attention",
                    "focus_kind": "approval_queue",
                    "focus_ref": str(pending),
                    "focus_label": f"승인 요청 {pending}개",
                    "next_command": "/pending",
                }
            )
        elif failed or closeout:
            context.update(
                {
                    "context_kind": "status_attention",
                    "focus_kind": "local_review",
                    "focus_ref": f"failed:{failed};closeout:{closeout}",
                    "focus_label": status_summary(payload),
                    "next_command": "/status",
                }
            )
        elif active or queued:
            context.update(
                {
                    "context_kind": "status_activity",
                    "focus_kind": "offdesk_activity",
                    "focus_ref": f"active:{active};queued:{queued}",
                    "focus_label": status_summary(payload),
                    "next_command": "/status",
                }
            )
        else:
            context.update(
                {
                    "context_kind": "status_clear",
                    "focus_kind": "none",
                    "focus_label": "처리할 항목 없음",
                    "next_command": "/status",
                }
            )
    elif command == "pending":
        approvals = payload.get("approvals") if isinstance(payload.get("approvals"), list) else []
        if approvals and isinstance(approvals[0], dict):
            approval = approvals[0]
            context.update(
                {
                    "context_kind": "approval_attention",
                    "focus_kind": "approval",
                    "focus_ref": str(approval.get("approval_id") or "approval"),
                    "focus_label": display_action(approval.get("action")),
                    "next_command": "/pending --all" if len(approvals) > 1 else "/pending",
                }
            )
        else:
            context.update(
                {
                    "context_kind": "approval_clear",
                    "focus_kind": "none",
                    "focus_label": "승인할 항목 없음",
                    "next_command": "/pending",
                }
            )
    elif command == "plans":
        plans = payload.get("plans") if isinstance(payload.get("plans"), list) else []
        if plans and isinstance(plans[0], dict):
            plan = plans[0]
            plan_id = str(plan.get("plan_id") or "plan")
            context.update(
                {
                    "context_kind": "plan_attention",
                    "focus_kind": "plan",
                    "focus_ref": plan_id,
                    "focus_label": display_review_status(plan.get("review_status")),
                    "next_command": show_command_for(plan_id),
                }
            )
        else:
            context.update(
                {
                    "context_kind": "plan_clear",
                    "focus_kind": "none",
                    "focus_label": "등록된 계획 없음",
                    "next_command": "/plans --latest",
                }
            )
    elif command == "show":
        plan = payload.get("plan") if isinstance(payload.get("plan"), dict) else {}
        plan_id = str(plan.get("plan_id") or "unknown")
        context.update(
            {
                "context_kind": "plan_detail",
                "focus_kind": "plan",
                "focus_ref": plan_id,
                "focus_label": display_review_status(plan.get("review_status")),
                "next_command": "/plans --latest",
            }
        )
    return context






def decision_action_choice_context(decisions: list[dict[str, Any]]) -> dict[str, Any] | None:
    """One-tap action buttons for the most urgent open decision.

    Each button is a complete `/decision <id> <action>` command, so tapping it
    dispatches that action (through the usual confirm step) without typing the
    id or action. Scoped to the top open decision and its first few actions to
    keep the mobile card tight; other decisions stay reachable via the text hint.
    """

    for decision in decisions:
        actions = decision.get("actions")
        decision_id = str(decision.get("decision_id") or "").strip()
        if not decision_id or not isinstance(actions, list) or not actions:
            continue
        commands: list[str] = []
        for action in actions[:3]:
            action_kind = str(action.get("action_kind") or "").strip()
            if action_kind:
                commands.append(f"/decision {decision_id} {action_kind}")
        if commands:
            return {"context_kind": "decisions_actions", "choice_commands": commands}
    return None


def finalize_dispatch_result(
    result: dict[str, Any],
    message_preview: str,
    *,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    # A card that stashed a pending confirmation gets one-tap 확인/취소 buttons.
    if context is None and isinstance(result.get("pending_dispatch_confirmation"), dict):
        context = {"context_kind": "dispatch_confirm"}
    attach_choice_surface(result, context)
    result.update(
        {
            "status": "rendered",
            "projection": None,
            "message_preview": message_preview,
            "mobile_card_contract": mobile_card_contract(message_preview),
        }
    )
    return result


def render_dispatch_command(
    args: argparse.Namespace,
    config: dict[str, Any],
    parsed: dict[str, Any],
    result: dict[str, Any],
) -> dict[str, Any]:
    """Render the read-only part of a dispatch command.

    State mutation (storing a confirmation, popping it, running the executor
    chain) happens in run_once so it persists and stays out of the offset
    save path. This function only reads a fresh surface and builds cards.
    """

    result["parsed_command"] = parsed
    command = parsed.get("command")
    generated_at = result["generated_at"]
    if command == "decisions":
        context: dict[str, Any] | None = None
        try:
            surface = export_workstation_surface(args.forager_bin, args.profile)
            decisions = open_decision_actions(surface)
            message_preview = render_decisions_message(
                profile=args.profile, generated_at=generated_at, decisions=decisions
            )
            context = decision_action_choice_context(decisions)
        except RemoteOperatorTelegramError as error:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="결정 목록을 불러오지 못했습니다",
                detail=str(error),
            )
        return finalize_dispatch_result(result, message_preview, context=context)
    if command == "decision":
        decision_id = str(parsed.get("decision_id") or "")
        action_kind = str(parsed.get("decision_action_kind") or "")
        note = str(parsed.get("decision_note") or "")
        try:
            surface = export_workstation_surface(args.forager_bin, args.profile)
            envelope = find_action_envelope(surface, decision_id, action_kind)
            if envelope is None:
                kinds = available_action_kinds(surface, decision_id)
                message_preview = render_dispatch_error_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    headline=f"{decision_id}에 {action_kind} 동작이 없습니다",
                    detail=f"가능한 동작: {', '.join(kinds) or '없음'}",
                )
            else:
                confirmation = build_confirmation(
                    kind="decision",
                    target_id=decision_id,
                    action_kind=action_kind,
                    observed_hash=str(envelope.get("observed_hash") or ""),
                    note=note,
                    chat_hash=None,
                    ttl_sec=args.dispatch_confirm_ttl_sec,
                )
                result["pending_dispatch_confirmation"] = confirmation
                message_preview = render_dispatch_confirm_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    decision_id=decision_id,
                    action_kind=action_kind,
                    note=note,
                    token=confirmation["token"],
                )
        except RemoteOperatorTelegramError as error:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="실행 확인을 준비하지 못했습니다",
                detail=str(error),
            )
        return finalize_dispatch_result(result, message_preview)
    if command == "recovery":
        try:
            surface = export_workstation_surface(args.forager_bin, args.profile)
            recoveries = open_recovery_actions(surface)
            message_preview = render_recovery_message(
                profile=args.profile, generated_at=generated_at, recoveries=recoveries
            )
        except RemoteOperatorTelegramError as error:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="복구 목록을 불러오지 못했습니다",
                detail=str(error),
            )
        return finalize_dispatch_result(result, message_preview)
    if command == "recover":
        closeout_id = str(parsed.get("closeout_id") or "")
        action_kind = str(parsed.get("recovery_action_kind") or "")
        note = str(parsed.get("recovery_note") or "")
        try:
            surface = export_workstation_surface(args.forager_bin, args.profile)
            envelope = find_recovery_envelope(surface, closeout_id, action_kind)
            if envelope is None:
                kinds = available_recovery_action_kinds(surface, closeout_id)
                message_preview = render_dispatch_error_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    headline=f"{closeout_id}에 {action_kind} 복구가 없습니다",
                    detail=f"가능한 동작: {', '.join(kinds) or '없음'}",
                )
            else:
                confirmation = build_confirmation(
                    kind="recovery",
                    target_id=closeout_id,
                    action_kind=action_kind,
                    observed_hash=str(envelope.get("observed_hash") or ""),
                    note=note,
                    chat_hash=None,
                    ttl_sec=args.dispatch_confirm_ttl_sec,
                )
                result["pending_dispatch_confirmation"] = confirmation
                message_preview = render_recovery_confirm_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    closeout_id=closeout_id,
                    action_kind=action_kind,
                    note=note,
                    token=confirmation["token"],
                )
        except RemoteOperatorTelegramError as error:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="복구 확인을 준비하지 못했습니다",
                detail=str(error),
            )
        return finalize_dispatch_result(result, message_preview)
    if command == "runtime":
        try:
            surface = export_workstation_surface(args.forager_bin, args.profile)
            rows = open_runtime_dispatch(surface)
            message_preview = render_runtime_message(
                profile=args.profile,
                generated_at=generated_at,
                rows=rows,
                enabled=bool(args.enable_runtime_dispatch),
            )
        except RemoteOperatorTelegramError as error:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="런타임 대기열을 불러오지 못했습니다",
                detail=str(error),
            )
        return finalize_dispatch_result(result, message_preview)
    if command == "dispatch":
        if not args.enable_runtime_dispatch:
            message_preview = render_runtime_disabled_message(
                profile=args.profile, generated_at=generated_at
            )
            return finalize_dispatch_result(result, message_preview)
        closeout_id = str(parsed.get("closeout_id") or "")
        runner = str(parsed.get("runner") or "")
        command_text = str(parsed.get("dispatch_command_text") or "")
        try:
            surface = export_workstation_surface(args.forager_bin, args.profile)
            item = runtime_dispatch_item(surface, closeout_id)
            if item is None:
                message_preview = render_dispatch_error_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    headline=f"{closeout_id} 런타임 항목이 없습니다",
                    detail="/runtime 로 대기열을 확인하세요.",
                )
            else:
                confirmation = build_confirmation(
                    kind="runtime",
                    target_id=closeout_id,
                    action_kind="dispatch",
                    observed_hash="",
                    note=command_text,
                    chat_hash=None,
                    ttl_sec=args.dispatch_confirm_ttl_sec,
                )
                confirmation["runner"] = runner
                confirmation["command"] = command_text
                result["pending_dispatch_confirmation"] = confirmation
                message_preview = render_runtime_confirm_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    closeout_id=closeout_id,
                    runner=runner,
                    command=command_text,
                    token=confirmation["token"],
                )
        except RemoteOperatorTelegramError as error:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="런타임 확인을 준비하지 못했습니다",
                detail=str(error),
            )
        return finalize_dispatch_result(result, message_preview)
    if command == "run_list":
        allowlist = load_dispatch_allowlist(args.dispatch_allowlist_file)
        message_preview = render_run_list_message(
            profile=args.profile,
            generated_at=generated_at,
            templates=dispatch_templates(allowlist),
            configured=bool(args.dispatch_allowlist_file),
        )
        return finalize_dispatch_result(result, message_preview)
    if command == "run":
        if not args.dispatch_allowlist_file:
            message_preview = render_run_disabled_message(
                profile=args.profile, generated_at=generated_at
            )
            return finalize_dispatch_result(result, message_preview)
        closeout_id = str(parsed.get("closeout_id") or "")
        template_name = str(parsed.get("template_name") or "")
        allowlist = load_dispatch_allowlist(args.dispatch_allowlist_file)
        template = find_dispatch_template(allowlist, template_name)
        if template is None:
            names = ", ".join(str(item.get("name") or "") for item in dispatch_templates(allowlist)[:5])
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline=f"{template_name} 템플릿이 없습니다",
                detail=(f"가능: {names}" if names else "/run 으로 템플릿을 확인하세요."),
            )
            return finalize_dispatch_result(result, message_preview)
        try:
            surface = export_workstation_surface(args.forager_bin, args.profile)
            item = runtime_dispatch_item(surface, closeout_id)
            if item is None:
                message_preview = render_dispatch_error_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    headline=f"{closeout_id} 런타임 항목이 없습니다",
                    detail="/runtime 로 대기열을 확인하세요.",
                )
            else:
                confirmation = build_confirmation(
                    kind="runtime",
                    target_id=closeout_id,
                    action_kind="dispatch",
                    observed_hash="",
                    note=str(template.get("command") or ""),
                    chat_hash=None,
                    ttl_sec=args.dispatch_confirm_ttl_sec,
                )
                confirmation["runner"] = str(template.get("runner") or "")
                confirmation["command"] = str(template.get("command") or "")
                # The template name is authoritative: at confirm time the command
                # is re-resolved from the current allowlist by this name, so the
                # stored command is only a preview and cannot outlive its template.
                confirmation["template"] = str(template.get("name") or "")
                result["pending_dispatch_confirmation"] = confirmation
                message_preview = render_run_confirm_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    closeout_id=closeout_id,
                    template_name=str(template.get("name") or ""),
                    command=str(template.get("command") or ""),
                    token=confirmation["token"],
                )
        except RemoteOperatorTelegramError as error:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="실행 확인을 준비하지 못했습니다",
                detail=str(error),
            )
        return finalize_dispatch_result(result, message_preview)
    if command == "tasks":
        try:
            surface = export_workstation_surface(args.forager_bin, args.profile)
            tasks = cancellable_tasks_from_surface(surface)
            message_preview = render_tasks_message(
                profile=args.profile, generated_at=generated_at, tasks=tasks
            )
        except RemoteOperatorTelegramError as error:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="작업 목록을 불러오지 못했습니다",
                detail=str(error),
            )
        return finalize_dispatch_result(result, message_preview)
    if command == "attention":
        attention_context = None
        try:
            surface = export_workstation_surface(args.forager_bin, args.profile)
            summary = attention_summary(surface)
            message_preview = render_attention_summary_message(
                profile=args.profile, generated_at=generated_at, summary=summary
            )
            top = summary.get("top") if isinstance(summary, dict) else None
            top_command = str(top.get("command_hint") or "") if isinstance(top, dict) else ""
            # One-tap button for the single most urgent action.
            if top_command:
                attention_context = {"next_command": top_command}
        except RemoteOperatorTelegramError as error:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="요약을 불러오지 못했습니다",
                detail=str(error),
            )
        return finalize_dispatch_result(result, message_preview, context=attention_context)
    if command in {"session_approve", "session_deny"}:
        # One tap by operator decision: the waiting card already shows what
        # the agent is asking, so the button acts directly. The executor
        # still re-verifies the session is waiting before sending the key.
        session_id = str(parsed.get("session_input_session_id") or "")
        action = "approve" if command == "session_approve" else "deny"
        try:
            dispatch_result = apply_session_input(
                args.forager_bin, args.profile, session_id, action,
                status_file=args.session_status_file,
                expected_hash=str(parsed.get("session_input_hash") or ""),
                option_key=str(parsed.get("session_input_option") or ""),
            )
        except Exception as error:  # noqa: BLE001 - poll loop must never crash here
            dispatch_result = {"ok": False, "kind": "session_input", "error": sanitize_text(str(error), max_chars=200)}
        result["dispatch_result"] = dispatch_result
        if dispatch_result.get("ok"):
            message_preview = "\n".join(
                [
                    "<b>세션 입력 전송됨</b>",
                    f"{html.escape(str(dispatch_result.get('project') or ''))} · {html.escape(str(dispatch_result.get('session_title') or ''))}",
                    "승인 키(Enter)를 보냈습니다." if action == "approve" else "거부 키(Esc)를 보냈습니다.",
                    "다음 조치: /status",
                ]
            )
        elif str(dispatch_result.get("error") or "") == "prompt_changed":
            new_hash = str(dispatch_result.get("new_hash") or "")
            sid8 = session_id[:8]
            message_preview = "\n".join(
                [
                    "<b>프롬프트가 바뀌어 중단했습니다</b>",
                    f"현재: {html.escape(str(dispatch_result.get('prompt_line') or ''))}",
                    "지금 화면 기준으로 다시 선택하세요.",
                    "다음 조치: 아래 버튼",
                ]
            )
            result["reply_markup_preview"] = {
                "keyboard": [
                    [f"/session_approve {sid8} {new_hash}", f"/session_deny {sid8} {new_hash}"],
                    ["상태", "도움말"],
                ],
                "resize_keyboard": True,
                "one_time_keyboard": False,
            }
        else:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="세션 입력 전송에 실패했습니다",
                detail=str(dispatch_result.get("error") or "unknown"),
            )
        return finalize_dispatch_result(result, message_preview)
    if command == "cancel_task":
        task_id = str(parsed.get("cancel_task_id") or "")
        reason = str(parsed.get("cancel_reason") or "")
        confirmation = build_confirmation(
            kind="cancel_task",
            target_id=task_id,
            action_kind="cancel",
            observed_hash="",
            note=reason,
            chat_hash=None,
            ttl_sec=args.dispatch_confirm_ttl_sec,
        )
        result["pending_dispatch_confirmation"] = confirmation
        message_preview = render_cancel_task_confirm_message(
            profile=args.profile,
            generated_at=generated_at,
            task_id=task_id,
            reason=reason,
            token=confirmation["token"],
        )
        return finalize_dispatch_result(result, message_preview)
    if command == "resume":
        # Re-enabling autonomy is deliberate, so it is confirm-gated.
        confirmation = build_confirmation(
            kind="resume",
            target_id="operator_pause",
            action_kind="resume",
            observed_hash="",
            note="",
            chat_hash=None,
            ttl_sec=args.dispatch_confirm_ttl_sec,
        )
        result["pending_dispatch_confirmation"] = confirmation
        message_preview = render_resume_confirm_message(
            profile=args.profile, generated_at=generated_at, token=confirmation["token"]
        )
        return finalize_dispatch_result(result, message_preview)
    # pause is an immediate emergency stop resolved in run_once; confirm, cancel,
    # and resume are resolved against persisted state there too. This placeholder
    # is only visible on non-live probe paths.
    # this placeholder is only visible on non-live probe paths.
    message_preview = render_dispatch_error_message(
        profile=args.profile,
        generated_at=generated_at,
        headline="라이브 처리 필요",
        detail="이 명령은 라이브 폴링에서 처리됩니다.",
    )
    return finalize_dispatch_result(result, message_preview)


def resolve_dispatch_confirmation(
    args: argparse.Namespace,
    state: dict[str, Any],
    chat_hash: str,
    rendered: dict[str, Any],
    parsed_command: dict[str, Any],
) -> None:
    """Apply the state-touching part of a dispatch command inside run_once."""

    command = parsed_command.get("command")
    generated_at = rendered["generated_at"]
    if command == "pause":
        # Emergency stop is immediate (fail-safe): no confirmation step.
        try:
            pause_result = apply_operator_pause(
                args.forager_bin,
                args.profile,
                reason=str(parsed_command.get("pause_reason") or ""),
            )
        except Exception as error:  # noqa: BLE001 - poll loop must never crash here
            pause_result = {"ok": False, "kind": "pause", "error": sanitize_text(str(error), max_chars=240)}
        rendered["dispatch_result"] = pause_result
        message_preview = render_pause_result_message(
            profile=args.profile, generated_at=generated_at, result=pause_result
        )
        rendered["message_preview"] = message_preview
        rendered["mobile_card_contract"] = mobile_card_contract(message_preview)
        return
    if command in {"decision", "recover", "dispatch", "run", "cancel_task", "resume"}:
        confirmation = rendered.get("pending_dispatch_confirmation")
        if isinstance(confirmation, dict):
            confirmation["chat_id_hash"] = chat_hash
            previous = store_confirmation(state, chat_hash, confirmation)
            if previous is not None:
                # A new request invalidates the old token. The card stays within
                # its line budget; the old /confirm already fails clearly, and
                # this field records the supersede for logs and tests.
                rendered["superseded_pending_confirmation"] = True
        return
    if command == "cancel":
        cleared = clear_confirmation(state, chat_hash)
        message_preview = render_dispatch_cancel_message(
            profile=args.profile, generated_at=generated_at, cleared=cleared
        )
        rendered["message_preview"] = message_preview
        rendered["mobile_card_contract"] = mobile_card_contract(message_preview)
        return
    if command != "confirm":
        return

    token = str(parsed_command.get("confirm_token") or "")
    confirmation = pop_confirmation(state, chat_hash, token)
    message_preview = None
    if confirmation is None:
        message_preview = render_dispatch_error_message(
            profile=args.profile,
            generated_at=generated_at,
            headline="확인 요청을 찾을 수 없습니다",
            detail="/decisions 로 다시 시작하세요.",
        )
    elif not confirmation_is_fresh(confirmation):
        message_preview = render_dispatch_error_message(
            profile=args.profile,
            generated_at=generated_at,
            headline="확인 시간이 만료되었습니다",
            detail="/decisions 로 다시 시작하세요.",
        )
    elif str(confirmation.get("kind") or "") == "runtime":
        template_name = str(confirmation.get("template") or "")
        runner = str(confirmation.get("runner") or "")
        command = str(confirmation.get("command") or "")
        dispatch_allowed = bool(args.enable_runtime_dispatch)
        template_error = None
        if template_name:
            # Curated dispatch: re-resolve runner/command from the current
            # allowlist by name so only a still-vetted template can run. The
            # operator never supplied the command, and this path is allowed
            # without --enable-runtime-dispatch because the command is bounded
            # by the local allowlist rather than free-form operator input.
            template = find_dispatch_template(
                load_dispatch_allowlist(args.dispatch_allowlist_file), template_name
            )
            if template is None:
                template_error = True
            else:
                runner = str(template.get("runner") or "")
                command = str(template.get("command") or "")
                dispatch_allowed = True
        if template_error:
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="실행 템플릿이 더 이상 없습니다",
                detail="/run 으로 템플릿을 확인하세요.",
            )
        elif not dispatch_allowed:
            # Runtime dispatch has no envelope hash; runtime-preflight re-verifies
            # the closeout against the latest decision receipt at execution time.
            message_preview = render_runtime_disabled_message(
                profile=args.profile, generated_at=generated_at
            )
        else:
            try:
                dispatch_result = apply_runtime_dispatch(
                    args.forager_bin,
                    args.profile,
                    closeout_id=str(confirmation.get("target_id") or ""),
                    runner=runner,
                    command=command,
                )
                rendered["dispatch_result"] = dispatch_result
                message_preview = render_runtime_result_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    result=dispatch_result,
                )
            except Exception as error:  # noqa: BLE001 - poll loop must never crash here
                message_preview = render_dispatch_error_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    headline="런타임 디스패치에 실패했습니다",
                    detail=str(error),
                )
    elif str(confirmation.get("kind") or "") == "cancel_task":
        # Cancel is the fail-safe direction; it has no envelope hash to re-check.
        try:
            dispatch_result = apply_cancel_task(
                args.forager_bin,
                args.profile,
                str(confirmation.get("target_id") or ""),
                reason=str(confirmation.get("note") or ""),
            )
            rendered["dispatch_result"] = dispatch_result
            message_preview = render_cancel_task_result_message(
                profile=args.profile,
                generated_at=generated_at,
                result=dispatch_result,
            )
        except Exception as error:  # noqa: BLE001 - poll loop must never crash here
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="작업 취소에 실패했습니다",
                detail=str(error),
            )
    elif str(confirmation.get("kind") or "") == "plan_capture":
        rendered["captured_plan_text"] = str(confirmation.get("note") or "")
        message_preview = "\n".join([
            "<b>계획 후보 등록</b>",
            sanitize_text(rendered["captured_plan_text"], max_chars=140),
            "검토 후 로컬 Plan Mode 또는 야간 창에서 이어집니다.",
            "다음 조치: /plans · /status",
        ])
    elif str(confirmation.get("kind") or "") == "autonomy_start":
        try:
            proc = subprocess.run(
                [sys.executable, str(AUTONOMY_RUNNER), "--arm",
                 "--until-hour", str(int(args.autonomy_until_hour)),
                 "--by", "telegram", "--profile", args.profile,
                 "--reason", str(confirmation.get("note") or "")],
                capture_output=True, text=True, timeout=30,
            )
            ok = proc.returncode == 0
            rendered["dispatch_result"] = {"ok": ok, "kind": "autonomy_start"}
            if ok:
                message_preview = "\n".join([
                    "<b>자율주행 시작</b>",
                    f"{int(args.autonomy_until_hour):02d}:00까지 armed: tick · 야간 증류 · 아침 브리핑.",
                    "이미 승인된 작업만 실행됩니다.",
                    "다음 조치: /status · /pause",
                ])
            else:
                message_preview = render_dispatch_error_message(
                    profile=args.profile, generated_at=generated_at,
                    headline="자율주행 시작에 실패했습니다",
                    detail=sanitize_text((proc.stderr or proc.stdout or "")[:200], max_chars=200),
                )
        except Exception as error:  # noqa: BLE001 - poll loop must never crash here
            message_preview = render_dispatch_error_message(
                profile=args.profile, generated_at=generated_at,
                headline="자율주행 시작에 실패했습니다", detail=str(error),
            )
    elif str(confirmation.get("kind") or "") == "resume":
        try:
            dispatch_result = apply_operator_resume(args.forager_bin, args.profile)
            rendered["dispatch_result"] = dispatch_result
            message_preview = render_resume_result_message(
                profile=args.profile,
                generated_at=generated_at,
                result=dispatch_result,
            )
        except Exception as error:  # noqa: BLE001 - poll loop must never crash here
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="정지 해제에 실패했습니다",
                detail=str(error),
            )
    else:
        kind = str(confirmation.get("kind") or "decision")
        # Support decision confirmations issued before the kind field existed.
        target_id = str(confirmation.get("target_id") or confirmation.get("decision_id") or "")
        is_recovery = kind == "recovery"
        list_hint = "/recovery" if is_recovery else "/decisions"
        try:
            surface = export_workstation_surface(args.forager_bin, args.profile)
            if is_recovery:
                envelope = find_recovery_envelope(
                    surface, target_id, confirmation["action_kind"]
                )
            else:
                envelope = find_action_envelope(
                    surface, target_id, confirmation["action_kind"]
                )
            if envelope is None:
                message_preview = render_dispatch_error_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    headline="동작이 더 이상 없습니다",
                    detail=f"{list_hint} 로 다시 확인하세요.",
                )
            elif str(envelope.get("observed_hash") or "") != confirmation.get("observed_hash"):
                message_preview = render_dispatch_error_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    headline="대상이 변경되었습니다",
                    detail=f"{list_hint} 로 최신 상태를 다시 확인하세요.",
                )
            elif is_recovery:
                dispatch_result = apply_recovery_action(
                    args.forager_bin, args.profile, envelope
                )
                rendered["dispatch_result"] = dispatch_result
                message_preview = render_recovery_result_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    result=dispatch_result,
                )
            else:
                dispatch_result = apply_decision_action(
                    args.forager_bin,
                    args.profile,
                    envelope,
                    note=str(confirmation.get("note") or ""),
                )
                rendered["dispatch_result"] = dispatch_result
                message_preview = render_dispatch_result_message(
                    profile=args.profile,
                    generated_at=generated_at,
                    result=dispatch_result,
                )
        except Exception as error:  # noqa: BLE001 - poll loop must never crash here
            message_preview = render_dispatch_error_message(
                profile=args.profile,
                generated_at=generated_at,
                headline="실행에 실패했습니다",
                detail=str(error),
            )
    if message_preview is not None:
        rendered["message_preview"] = message_preview
        rendered["mobile_card_contract"] = mobile_card_contract(message_preview)


def build_chat_operator_snapshot(
    args: argparse.Namespace,
    *,
    focus_entry: dict[str, Any] | None = None,
    focus_source: str | None = None,
) -> dict[str, Any]:
    """Compact live read-only state handed to the local chat agent.

    Without this the agent only sees chat history, so it answers state and
    workspace questions with "cannot check" and invents slash commands. Every
    section degrades gracefully; the chat must still work if a probe fails.
    """

    snapshot: dict[str, Any] = {"schema": "telegram_chat_operator_snapshot.v1"}
    if isinstance(focus_entry, dict):
        snapshot["project_focus"] = build_project_focus(
            args.forager_bin,
            args.profile,
            focus_entry,
            focus_source,
        )
    try:
        surface = export_workstation_surface(args.forager_bin, args.profile)
        snapshot["workstation"] = {
            "generated_at": surface.get("generated_at"),
            "attention_counts": surface.get("attention_counts"),
            "capacity": surface.get("capacity"),
            "health": [
                {"id": item.get("id"), "status": item.get("status")}
                for item in surface.get("health") or []
                if isinstance(item, dict)
            ],
            "open_decisions": [
                {"decision_id": item.get("decision_id"), "title": item.get("title")}
                for item in open_decision_actions(surface)[:5]
            ],
        }
    except (OSError, RemoteOperatorTelegramError) as error:
        snapshot["workstation"] = {
            "status": "unavailable",
            "error": sanitize_text(str(error), max_chars=120),
        }
    snapshot["autonomy_armed"] = autonomy_is_armed(args.profile)
    registry = load_registry()
    if registry:
        snapshot["registered_projects"] = registry_summary(registry)
    try:
        roots = workspace_roots(args)
        paths = discover_project_paths(roots, max_paths=60)
        snapshot["workspace_projects"] = [relative_path_hint(path, roots) for path in paths][:40]
    except OSError:
        snapshot["workspace_projects"] = []
    return snapshot


def run_chat_tool(
    args: argparse.Namespace,
    request: dict[str, Any],
    *,
    registry: dict[str, dict[str, Any]],
    focus_entry: dict[str, Any] | None,
) -> dict[str, Any]:
    """Execute one read-only chat-agent tool call. Never raises.

    The chat agent may only touch registered projects, and only through the
    bounded read-only helpers in telegram_operator.projects.
    """

    tool = str(request.get("tool") or "")
    key = str(request.get("project") or "").strip() or str((focus_entry or {}).get("key") or "")
    entry = registry.get(key)
    if not isinstance(entry, dict):
        return {
            "status": "unknown_project",
            "project": key or None,
            "registered": sorted(registry.keys())[:20],
        }
    project_dir = find_project_dir(entry, workspace_roots(args))
    if project_dir is None:
        return {"status": "workspace_not_found", "project": key}
    try:
        if tool == "workspace_overview":
            report = inspect_project_workspace(project_dir)
            report["status"] = "ok"
            report["project"] = key
            return report
        if tool == "list_dir":
            return list_project_dir(project_dir, str(request.get("path") or ""))
        if tool == "read_file":
            return read_project_file(project_dir, str(request.get("path") or ""))
    except Exception as error:  # noqa: BLE001 - chat tools must never crash the poll loop
        return {
            "status": "tool_failed",
            "tool": tool[:40],
            "error": sanitize_text(str(error), max_chars=120),
        }
    return {"status": "unknown_tool", "tool": tool[:40]}


def render_command_result(
    args: argparse.Namespace,
    config: dict[str, Any],
    command_text: str,
    *,
    mode: str,
    feedback_context: dict[str, Any] | None = None,
    chat_history: list[dict[str, Any]] | None = None,
    sticky_focus_key: str | None = None,
) -> dict[str, Any]:
    result = result_base(args, config, mode)
    result["command_text"] = sanitize_text(command_text, max_chars=400)
    parsed = parse_remote_command(command_text)
    if not parsed.get("supported"):
        result["parsed_command"] = parsed
        message_preview = help_message(profile=args.profile, generated_at=result["generated_at"])
        attach_choice_surface(result, None)
        result.update(
            {
                "status": "unsupported",
                "reason": parsed.get("reason"),
                "projection": None,
                "message_preview": message_preview,
                "mobile_card_contract": mobile_card_contract(message_preview),
            }
        )
        return result
    if parsed.get("command") == "help":
        result["parsed_command"] = parsed
        message_preview = help_message(profile=args.profile, generated_at=result["generated_at"])
        attach_choice_surface(result, None)
        result.update(
            {
                "status": "rendered",
                "projection": None,
                "message_preview": message_preview,
                "mobile_card_contract": mobile_card_contract(message_preview),
            }
        )
        return result
    if parsed.get("command") == "guide":
        result["parsed_command"] = parsed
        message_preview = guide_message(profile=args.profile, generated_at=result["generated_at"])
        attach_choice_surface(result, None)
        result.update(
            {
                "status": "rendered",
                "projection": None,
                "message_preview": message_preview,
                "mobile_card_contract": mobile_card_contract(
                    message_preview,
                    max_lines=GUIDE_SHEET_MAX_LINES,
                    max_chars=GUIDE_SHEET_MAX_CHARS,
                ),
            }
        )
        return result
    if parsed.get("command") == "chat":
        chat_text = str(parsed.get("chat_text") or command_text)
        registry = load_registry()
        focus_entry, focus_source = resolve_chat_focus(
            chat_text,
            registry,
            sticky_focus_key,
        )
        if focus_entry:
            result["project_focus"] = {
                "key": focus_entry.get("key"),
                "source": focus_source,
            }
        operator_snapshot = build_chat_operator_snapshot(
            args,
            focus_entry=focus_entry,
            focus_source=focus_source,
        )
        tool_outcomes: list[dict[str, Any]] = []

        def execute_chat_tool(request: dict[str, Any]) -> dict[str, Any]:
            outcome = run_chat_tool(args, request, registry=registry, focus_entry=focus_entry)
            tool_outcomes.append({"request": request, "result": outcome})
            return outcome

        agent_intent = chat_with_agent(
            args,
            chat_text,
            feedback_context=feedback_context,
            chat_history=chat_history,
            operator_snapshot=operator_snapshot,
            tool_executor=execute_chat_tool,
        )
        if isinstance(agent_intent, dict) and agent_intent.get("tools_used"):
            result["chat_tools_used"] = agent_intent.get("tools_used")
        if (
            isinstance(agent_intent, dict)
            and agent_intent.get("intent") == "chat"
            and not agent_intent.get("assistant_reply")
        ):
            # The model inspected but never composed an answer; degrade to a
            # deterministic summary of the last successful overview probe.
            overview = next(
                (
                    item["result"]
                    for item in reversed(tool_outcomes)
                    if item["request"].get("tool") == "workspace_overview"
                    and item["result"].get("status") == "ok"
                ),
                None,
            )
            if overview and focus_entry:
                agent_intent = {
                    **agent_intent,
                    "assistant_reply": "\n".join(
                        [f"{focus_entry.get('display_name')} 로컬 상태:"]
                        + inspection_summary_lines(overview)
                    ),
                }
        if isinstance(agent_intent, dict):
            parsed["agent_intent"] = agent_intent
            parsed["agent_chat_reason"] = str(
                agent_intent.get("reason") or agent_intent.get("status") or "unknown"
            )
        result["parsed_command"] = parsed
        message_preview = render_chat_message(
            profile=args.profile,
            generated_at=result["generated_at"],
            chat_text=str(parsed.get("chat_text") or command_text),
            feedback_context=feedback_context,
            agent_intent=agent_intent if isinstance(agent_intent, dict) else None,
        )
        attach_choice_surface(result, feedback_context)
        if isinstance(feedback_context, dict):
            result["feedback_context"] = feedback_context
        result.update(
            {
                "status": "rendered",
                "projection": None,
                "message_preview": message_preview,
                "mobile_card_contract": mobile_card_contract(message_preview),
            }
        )
        return result
    if parsed.get("command") == "feedback":
        result["parsed_command"] = parsed
        message_preview = render_feedback_message(
            profile=args.profile,
            generated_at=result["generated_at"],
            feedback_text=str(parsed.get("feedback_text") or command_text),
            feedback_kind=str(parsed.get("feedback_kind") or "freeform_feedback"),
            feedback_context=feedback_context,
        )
        attach_choice_surface(result, feedback_context)
        if isinstance(feedback_context, dict):
            result["feedback_context"] = feedback_context
        result.update(
            {
                "status": "rendered",
                "projection": None,
                "message_preview": message_preview,
                "mobile_card_contract": mobile_card_contract(message_preview),
            }
        )
        return result
    if parsed.get("command") == "remember":
        result["parsed_command"] = parsed
        message_preview = render_wiki_candidate_message(
            profile=args.profile,
            generated_at=result["generated_at"],
            remember_text=str(parsed.get("remember_text") or command_text),
        )
        attach_choice_surface(result, feedback_context)
        if isinstance(feedback_context, dict):
            result["feedback_context"] = feedback_context
        result.update(
            {
                "status": "rendered",
                "projection": None,
                "message_preview": message_preview,
                "mobile_card_contract": mobile_card_contract(message_preview),
            }
        )
        return result
    if parsed.get("command") == "plan_request":
        agent_intent = classify_feedback_with_agent(
            args,
            str(parsed.get("feedback_text") or command_text),
            feedback_context=feedback_context,
        )
        if isinstance(agent_intent, dict):
            parsed["agent_intent"] = agent_intent
            parsed["feedback_kind"] = "planning_request"
            parsed["reason"] = f"agent_intent:{agent_intent.get('intent') or 'unknown'}"
        result["parsed_command"] = parsed
        message_preview = render_feedback_message(
            profile=args.profile,
            generated_at=result["generated_at"],
            feedback_text=str(parsed.get("feedback_text") or command_text),
            feedback_kind="planning_request",
            feedback_context=feedback_context,
            agent_intent=agent_intent if isinstance(agent_intent, dict) else None,
        )
        attach_choice_surface(result, feedback_context)
        if isinstance(feedback_context, dict):
            result["feedback_context"] = feedback_context
        result.update(
            {
                "status": "rendered",
                "projection": None,
                "message_preview": message_preview,
                "mobile_card_contract": mobile_card_contract(message_preview),
            }
        )
        return result
    if parsed.get("command") in {
        "decisions",
        "decision",
        "recovery",
        "recover",
        "runtime",
        "dispatch",
        "run_list",
        "run",
        "tasks",
        "cancel_task",
        "session_approve",
        "session_deny",
        "pause",
        "resume",
        "attention",
        "confirm",
        "cancel",
    }:
        return render_dispatch_command(args, config, parsed, result)
    result["parsed_command"] = parsed
    if args.projection_file:
        projection = load_projection_file(args.projection_file, parsed)
    else:
        projection = run_projection(args.forager_bin, args.profile, parsed)
    adapter_health = None
    if parsed.get("command") == "status" and (not args.dry_run or args.loop_status_file.exists()):
        adapter_health = listener_health(args, config)
    message_preview = render_projection_message(
        projection,
        max_chars=max(200, int(args.max_message_chars)),
        adapter_health=adapter_health,
    )
    interaction_context = interaction_context_from_projection(projection)
    attach_choice_surface(result, interaction_context)
    result.update(
        {
            "status": "rendered",
            "projection_schema": projection.get("schema"),
            "projection": projection,
            "adapter_health": adapter_health,
            "message_preview": message_preview,
            "mobile_card_contract": mobile_card_contract(message_preview),
        }
    )
    return result


def message_from_update(update: dict[str, Any]) -> dict[str, Any] | None:
    message = update.get("message")
    return message if isinstance(message, dict) else None


def update_text(message: dict[str, Any]) -> str:
    text = message.get("text")
    return str(text or "").strip()


def chat_id_for(message: dict[str, Any]) -> str:
    chat = message.get("chat")
    if not isinstance(chat, dict):
        return ""
    value = chat.get("id")
    return str(value or "").strip()


def user_id_for(message: dict[str, Any]) -> str:
    user = message.get("from")
    if not isinstance(user, dict):
        return ""
    value = user.get("id")
    return str(value or "").strip()


def message_id_for(message: dict[str, Any]) -> int | None:
    value = message.get("message_id")
    return int(value) if isinstance(value, int) else None


def record_feedback(
    args: argparse.Namespace,
    config: dict[str, Any],
    message: dict[str, Any],
    text: str,
    *,
    feedback_context: dict[str, Any] | None = None,
    parsed_command: dict[str, Any] | None = None,
) -> dict[str, Any]:
    feedback_kind = classify_feedback_kind(text)
    agent_intent = None
    if isinstance(parsed_command, dict):
        parsed_kind = str(parsed_command.get("feedback_kind") or "").strip()
        if parsed_kind in {"freeform_feedback", "planning_request"}:
            feedback_kind = parsed_kind
        parsed_agent = parsed_command.get("agent_intent")
        if isinstance(parsed_agent, dict):
            agent_intent = parsed_agent
    record = {
        "schema": "remote_operator_telegram_feedback.v1",
        "received_at": utc_now(),
        "profile": args.profile,
        "chat_id_hash": sha256_short(chat_id_for(message)),
        "user_id_hash": sha256_short(user_id_for(message)),
        "message_id": message_id_for(message),
        "feedback_text": sanitize_text(text, max_chars=2000),
        "feedback_kind": feedback_kind,
        "target_chat_id_hash": config.get("target_chat_id_hash"),
        "feedback_context": feedback_context,
    }
    if agent_intent:
        record["agent_intent"] = agent_intent
    append_jsonl(args.feedback_file, record)
    return {
        "feedback_recorded": True,
        "feedback_file": str(args.feedback_file),
        "feedback_text_chars": len(str(text or "")),
        "feedback_context": feedback_context,
        "feedback_record": record,
    }


def update_is_allowed(config: dict[str, Any], message: dict[str, Any]) -> tuple[bool, str]:
    chat_id = chat_id_for(message)
    user_id = user_id_for(message)
    allowed_chat_ids = config.get("allowed_chat_ids") or set()
    allowed_user_ids = config.get("allowed_user_ids") or set()
    if allowed_chat_ids and chat_id not in allowed_chat_ids:
        return False, "chat_not_allowed"
    if allowed_user_ids and user_id not in allowed_user_ids:
        return False, "user_not_allowed"
    return True, "allowed"


AUTONOMY_RUNNER = pathlib.Path(__file__).resolve().parent / "offdesk_autonomy_run.py"
DEFAULT_AUTONOMY_WATCH = [
    pathlib.Path.home() / ".claude" / "projects",
    pathlib.Path.home() / ".codex" / "sessions",
]


def latest_activity_epoch(paths: list[pathlib.Path], max_depth: int = 3) -> float:
    """Newest mtime across shallow levels of the watch trees (cheap idle probe)."""
    newest = 0.0
    for root in paths:
        if not root.exists():
            continue
        stack = [(root, 0)]
        while stack:
            current, depth = stack.pop()
            try:
                newest = max(newest, current.stat().st_mtime)
                if depth >= max_depth or not current.is_dir():
                    continue
                for child in current.iterdir():
                    stack.append((child, depth + 1))
            except OSError:
                continue
    return newest


def autonomy_is_armed(profile: str) -> bool:
    try:
        sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
        from offdesk_autonomy_run import is_armed  # noqa: PLC0415

        return bool(is_armed(profile))
    except Exception:  # noqa: BLE001 - probe must never break the loop
        return False


def scan_and_propose_autonomy(
    args: argparse.Namespace,
    config: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any] | None:
    """Offer (never start) overnight autonomy when the workstation goes idle.

    The proposal is a normal confirmation card: the operator's 확인 tap arms a
    bounded window; 취소 or silence leaves everything off. Never raises into
    the poll loop.
    """

    if not getattr(args, "autonomy_propose", False):
        return None
    try:
        target_chat_id = str(config.get("target_chat_id") or "").strip()
        chat_hash = str(config.get("target_chat_id_hash") or "").strip()
        if not target_chat_id or not chat_hash:
            return {"status": "no_target_chat"}
        now = time.time()
        local_hour = time.localtime(now).tm_hour
        if not (local_hour >= int(args.autonomy_propose_from_hour) or local_hour < 2):
            return {"status": "outside_window"}
        if autonomy_is_armed(args.profile):
            return {"status": "already_armed"}
        sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
        from offdesk_autonomy_run import profile_dir as autonomy_profile_dir  # noqa: PLC0415

        pause_file = autonomy_profile_dir(args.profile) / "offdesk_operator_pause.json"
        try:
            if pause_file.exists() and json.loads(pause_file.read_text()).get("paused"):
                return {"status": "paused"}
        except (OSError, ValueError):
            pass
        pending = (state.get("pending_dispatch_confirmations_by_chat") or {}).get(chat_hash)
        if isinstance(pending, dict):
            if confirmation_is_fresh(pending):
                return {"status": "confirmation_already_pending"}
            # An expired confirmation (e.g. last night's unanswered proposal)
            # must not block proposals forever.
            clear_confirmation(state, chat_hash)
        last = float(state.get("autonomy_proposal_at") or 0)
        if now - last < int(args.autonomy_backoff_sec):
            return {"status": "backoff"}
        watch = list(args.autonomy_watch_path) or DEFAULT_AUTONOMY_WATCH
        newest = latest_activity_epoch(watch)
        idle_min = int((now - newest) / 60) if newest else 0
        if not newest or idle_min < int(args.autonomy_idle_min):
            return {"status": "active", "idle_min": idle_min}

        confirmation = build_confirmation(
            kind="autonomy_start",
            target_id="overnight",
            action_kind="arm",
            observed_hash="",
            note=f"idle {idle_min}m",
            chat_hash=chat_hash,
            ttl_sec=1800,
        )
        store_confirmation(state, chat_hash, confirmation)
        message = "\n".join(
            [
                "<b>자율주행 제안</b>",
                f"{idle_min}분째 활동이 없습니다.",
                f"승인 시 {int(args.autonomy_until_hour):02d}:00까지: tick 심장박동 · 야간 증류 · 아침 브리핑.",
                "이미 승인된 작업만 실행되며 /pause 로 즉시 중단됩니다.",
                "다음 조치: 확인 또는 취소",
            ]
        )
        send_status = "sent"
        try:
            send_message(
                config, target_chat_id, message, args,
                reply_markup=choice_keyboard({"context_kind": "dispatch_confirm"}),
            )
            send_status = "dry_run" if args.dry_run else "sent"
        except RemoteOperatorTelegramError:
            send_status = "failed"
        state["autonomy_proposal_at"] = now
        return {
            "status": "proposed",
            "idle_min": idle_min,
            "send_status": send_status,
            "message_preview": message,
            "mobile_card_contract": mobile_card_contract(message),
        }
    except Exception as error:  # noqa: BLE001 - poll loop must never crash here
        return {"status": "scan_failed", "error": sanitize_text(str(error), max_chars=240)}


def summarize_waiting_prompt(args: argparse.Namespace, tail: str) -> str:
    """One-line summary of what a waiting session is asking, for the card.

    Captures the tmux pane tail (read-only) and asks the local model to
    compress it; degrades to the last visible prompt lines, then to empty.
    Never raises into the poll loop.
    """

    from telegram_operator.agent import (  # noqa: PLC0415
        call_ollama_intent_agent,
        resolve_agent_config,
        select_agent_runtime,
    )
    try:
        if not tail.strip():
            return ""
        fallback = sanitize_text(tail.splitlines()[-1], max_chars=120)
        agent_config = resolve_agent_config(args)
        if agent_config.get("mode") == "off":
            return fallback
        runtime = select_agent_runtime(agent_config)
        if not runtime:
            return fallback
        prompt = "\n".join(
            [
                "An AI coding agent inside a terminal is paused on a permission prompt.",
                "From the terminal tail below, state in ONE short Korean line what the agent is asking permission to do (the exact command/file if visible).",
                'Return exactly one JSON object: {"summary": "<one line>"}. No markdown.',
                "Terminal tail:",
                tail[-3000:],
            ]
        )
        parsed = call_ollama_intent_agent(runtime, prompt)
        summary = sanitize_text(str(parsed.get("summary") or ""), max_chars=140)
        return summary or fallback
    except Exception:  # noqa: BLE001 - poll loop must never crash here
        return ""


def scan_and_notify_waiting_sessions(
    args: argparse.Namespace,
    config: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any] | None:
    """Push one card per waiting-episode of a supervised agent session.

    A session that flips to waiting gets a card once, then again only after
    the per-session backoff. Sessions that leave the waiting state are
    cleared so the next episode notifies immediately. Never raises into the
    poll loop.
    """

    if not getattr(args, "session_notify", False):
        return None
    target_chat_id = str(config.get("target_chat_id") or "").strip()
    if not target_chat_id:
        return {"status": "no_target_chat"}
    try:
        if args.session_status_file is not None:
            status = json.loads(args.session_status_file.read_text())
        else:
            process = subprocess.run(
                [args.forager_bin, "-p", args.profile, "status", "--json"],
                capture_output=True, text=True, timeout=30,
            )
            if process.returncode != 0:
                return {
                    "status": "scan_failed",
                    "error": sanitize_text(process.stderr or process.stdout, max_chars=200),
                }
            status = json.loads(process.stdout)
        sessions = [s for s in status.get("sessions") or [] if isinstance(s, dict)]
        waiting = [s for s in sessions if s.get("status") == "waiting"]
        notified = state.setdefault("waiting_notified_by_session", {})
        if not isinstance(notified, dict):
            notified = {}
            state["waiting_notified_by_session"] = notified
        now = time.time()
        waiting_ids = {str(s.get("id") or "") for s in waiting}
        # Episode end: forget sessions that are no longer waiting so the next
        # episode notifies right away (entries younger than 300s stay to damp
        # flapping between polls).
        for session_id in list(notified):
            if session_id not in waiting_ids and now - float(notified[session_id]) > 300:
                del notified[session_id]
        prev_waiting = {str(x) for x in state.get("waiting_ids_last_scan") or []}
        state["waiting_ids_last_scan"] = sorted(waiting_ids)
        sent = []
        for session in waiting:
            session_id = str(session.get("id") or "")
            if not session_id:
                continue
            last = float(notified.get(session_id) or 0)
            # A session that was not waiting at the previous scan hit a NEW
            # prompt: notify right away (60s flap guard only). The long
            # backoff is a reminder cadence for one continuously-waiting
            # prompt, not a mute for the session.
            threshold = 60 if session_id not in prev_waiting else int(args.session_notify_backoff_sec)
            if now - last < threshold:
                continue
            title = sanitize_text(str(session.get("title") or session_id), max_chars=60)
            tool = sanitize_text(str(session.get("tool") or "agent"), max_chars=24)
            project = sanitize_text(str(session.get("project") or "미등록"), max_chars=40)
            from telegram_operator.dispatch import (  # noqa: PLC0415
                capture_session_tail,
                find_tmux_session_name,
                pane_prompt_hash,
                parse_prompt_options,
            )

            tmux_name = find_tmux_session_name(session_id)
            tail = capture_session_tail(tmux_name, lines=15) if tmux_name else ""
            prompt_hash = pane_prompt_hash(tail)
            options = parse_prompt_options(tail)
            ask_summary = summarize_waiting_prompt(args, tail)
            lines = [
                "<b>세션 입력 대기</b>",
                f"⏸ {html.escape(project)} · {html.escape(title)} ({html.escape(tool)})",
            ]
            if ask_summary:
                lines.append(f"요청: {html.escape(ask_summary)}")
            else:
                lines.append("에이전트가 승인/입력을 기다리고 있습니다.")
            if options:
                lines.append(
                    " · ".join(
                        f"{opt['key']}. {html.escape(sanitize_text(opt['label'], max_chars=24))}"
                        for opt in options
                    )
                )
            lines.append("아래 버튼 한 번으로 선택합니다.")
            message = "\n".join(lines)
            sid8 = session_id[:8]
            if options:
                option_row = [
                    f"/session_approve {sid8} {prompt_hash} {opt['key']}" for opt in options[:3]
                ]
                keyboard = [option_row, [f"/session_deny {sid8} {prompt_hash}", "상태"]]
            else:
                keyboard = [
                    [f"/session_approve {sid8} {prompt_hash}", f"/session_deny {sid8} {prompt_hash}"],
                    ["상태", "도움말"],
                ]
            approve_markup = {
                "keyboard": keyboard,
                "resize_keyboard": True,
                "one_time_keyboard": False,
            }
            try:
                send_message(config, target_chat_id, message, args, reply_markup=approve_markup)
            except RemoteOperatorTelegramError:
                continue
            notified[session_id] = now
            sent.append(
                {
                    "session_id": session_id,
                    "title": title,
                    "project": project,
                    "prompt_hash": prompt_hash,
                    "pushed_at": utc_now(),
                }
            )
        if not sent:
            return {"status": "no_new_waiting", "waiting_count": len(waiting)}
        return {"status": "notified", "sent": sent, "waiting_count": len(waiting)}
    except Exception as error:  # noqa: BLE001 - poll loop must never crash here
        return {"status": "scan_failed", "error": sanitize_text(str(error), max_chars=240)}


def scan_and_notify_attention(
    args: argparse.Namespace,
    config: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any] | None:
    """Push a card for newly waiting attention items. Never raises into the loop."""

    if not args.attention_notify:
        return None
    target_chat_id = str(config.get("target_chat_id") or "").strip()
    if not target_chat_id:
        return {"status": "no_target_chat"}
    try:
        surface = export_workstation_surface(args.forager_bin, args.profile)
        notification = build_attention_notification(
            surface, state, reminder_sec=int(args.attention_reminder_sec)
        )
    except (OSError, RemoteOperatorTelegramError) as error:
        return {"status": "scan_failed", "error": sanitize_text(str(error), max_chars=240)}
    fresh = notification.get("fresh_items") or []
    if not fresh:
        return {"status": "no_new_attention", "pending_count": notification.get("pending_count")}
    message = render_attention_message(
        profile=args.profile, generated_at=utc_now(), items=fresh
    )
    send_status = "sent"
    message_id = None
    try:
        message_id = send_message(config, target_chat_id, message, args, reply_markup=None)
        send_status = "dry_run" if args.dry_run else "sent"
    except RemoteOperatorTelegramError as error:
        if "Telegram API" not in str(error):
            return {"status": "scan_failed", "error": sanitize_text(str(error), max_chars=240)}
        send_status = "failed"
    return {
        "status": "notified",
        "pending_count": notification.get("pending_count"),
        "notified_count": len(fresh),
        "items": fresh,
        "message_preview": message,
        "mobile_card_contract": mobile_card_contract(message),
        "send_status": send_status,
        "sent_message_id": message_id,
    }


def run_once(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    state = load_state(args.state_file)
    updates = get_updates(config, int(state.get("offset") or 0), args)
    result = result_base(args, config, "live_once")
    result.update({"status": "no_update", "updates_seen": len(updates)})
    max_update_id = int(state.get("offset") or 0) - 1
    for update in updates:
        update_id = update.get("update_id")
        if isinstance(update_id, int):
            max_update_id = max(max_update_id, update_id)
        message = message_from_update(update)
        if not message:
            continue
        allowed, reason = update_is_allowed(config, message)
        if not allowed:
            result.update(
                {
                    "status": "ignored",
                    "reason": reason,
                    "chat_id_hash": sha256_short(chat_id_for(message)),
                    "user_id_hash": sha256_short(user_id_for(message)),
                }
            )
            continue
        text = update_text(message)
        if not text:
            result.update({"status": "ignored", "reason": "empty_message"})
            continue
        chat_hash = sha256_short(chat_id_for(message))
        active_session = active_remote_plan_session(state, chat_hash)
        session_payload = remote_plan_session_command_payload(text) if active_session else None
        if active_session and session_payload:
            rendered = handle_remote_plan_session_input(
                args,
                config,
                state,
                chat_hash=chat_hash,
                session=active_session,
                text=session_payload,
                mode="live_once",
            )
        elif active_session and remote_plan_session_should_handle_text(args, active_session, text):
            rendered = handle_remote_plan_session_input(
                args,
                config,
                state,
                chat_hash=chat_hash,
                session=active_session,
                text=text,
                mode="live_once",
            )
        else:
            feedback_context = last_context_for_chat_hash(
                state,
                chat_hash,
                max_age_sec=args.context_max_age_sec,
            )
            sticky_focus = (state.get("chat_focus_by_chat") or {}).get(chat_hash)
            rendered = render_command_result(
                args,
                config,
                text,
                mode="live_once",
                feedback_context=feedback_context,
                chat_history=chat_history_for_chat_hash(
                    state,
                    chat_hash,
                    max_age_sec=args.context_max_age_sec,
                ),
                sticky_focus_key=str((sticky_focus or {}).get("project_key") or "") or None,
            )
        rendered["updates_seen"] = len(updates)
        if isinstance(update_id, int):
            rendered["processed_update_id"] = update_id
        parsed_command = rendered.get("parsed_command") if isinstance(rendered.get("parsed_command"), dict) else {}
        focus = rendered.get("project_focus")
        if isinstance(focus, dict) and focus.get("source") == "mention" and focus.get("key"):
            state.setdefault("chat_focus_by_chat", {})[chat_hash] = {
                "project_key": focus.get("key"),
                "updated_at": utc_now(),
            }
        if parsed_command.get("command") == "remember":
            remember_text = str(parsed_command.get("remember_text") or text)
            # A persistence failure must not raise before the offset save:
            # that would re-deliver the same update on every poll.
            try:
                record_result = record_remember_candidate(
                    profile=args.profile,
                    text=remember_text,
                    chat_hash=chat_hash,
                    user_hash=sha256_short(user_id_for(message)),
                    message_id=message_id_for(message),
                )
            except (OSError, RemoteOperatorTelegramError) as error:
                record_result = {
                    "wiki_candidate_recorded": False,
                    "wiki_candidate_status": "failed",
                    "wiki_candidate_error": sanitize_text(str(error), max_chars=240),
                }
            rendered.update(record_result)
            rendered["message_preview"] = render_wiki_candidate_message(
                profile=args.profile,
                generated_at=rendered["generated_at"],
                remember_text=remember_text,
                record_result=record_result,
            )
            rendered["mobile_card_contract"] = mobile_card_contract(rendered["message_preview"])
        if parsed_command.get("command") in {"feedback", "plan_request"}:
            feedback_context = last_context_for_chat_hash(
                state,
                chat_hash,
                max_age_sec=args.context_max_age_sec,
            )
            feedback_text = str(parsed_command.get("feedback_text") or parsed_command.get("plan_text") or text)
            feedback_result = record_feedback(
                args,
                config,
                message,
                feedback_text,
                feedback_context=feedback_context,
                parsed_command=parsed_command,
            )
            feedback_record = feedback_result.pop("feedback_record", None)
            rendered.update(feedback_result)
            if isinstance(feedback_record, dict):
                ingest_result = ingest_feedback_decision(args, feedback_record)
                rendered.update(ingest_result)
                rendered["message_preview"] = render_feedback_message(
                    profile=args.profile,
                    generated_at=rendered["generated_at"],
                    feedback_text=feedback_text,
                    feedback_kind=str(parsed_command.get("feedback_kind") or "freeform_feedback"),
                    feedback_context=feedback_context,
                    inbox_status=str(ingest_result.get("decision_feedback_ingest_status") or ""),
                    agent_intent=parsed_command.get("agent_intent")
                    if isinstance(parsed_command.get("agent_intent"), dict)
                    else None,
                )
                rendered["mobile_card_contract"] = mobile_card_contract(rendered["message_preview"])
                if parsed_command.get("command") == "plan_request":
                    session = create_remote_plan_session(
                        args,
                        chat_hash=chat_hash,
                        request_text=feedback_text,
                        parsed_command=parsed_command,
                        feedback_context=feedback_context,
                        decision_id=ingest_result.get("decision_feedback_decision_id"),
                    )
                    store_remote_plan_session(state, chat_hash, session)
                    rendered["remote_plan_session"] = public_remote_plan_session(session)
                    rendered["message_preview"] = render_project_selection_message(
                        profile=args.profile,
                        session=session,
                    )
                    attach_choice_surface(rendered, remote_plan_selection_context(session))
                    rendered["mobile_card_contract"] = mobile_card_contract(rendered["message_preview"])
        if parsed_command.get("command") == "chat":
            agent_intent = (
                parsed_command.get("agent_intent")
                if isinstance(parsed_command.get("agent_intent"), dict)
                else {}
            )
            if agent_intent.get("status") == "classified":
                # The chat agent owns the routing decision (function-calling
                # style); keyword markers are only the no-agent fallback.
                wants_capture = agent_intent.get("intent") == "delegate_work"
                capture_note = str(agent_intent.get("delegation_goal") or "").strip() or text
            else:
                from telegram_operator.agent import classify_feedback_kind  # noqa: PLC0415

                wants_capture = classify_feedback_kind(text) == "planning_request"
                capture_note = text
            pending_now = (state.get("pending_dispatch_confirmations_by_chat") or {}).get(chat_hash)
            if (
                wants_capture
                and not (isinstance(pending_now, dict) and confirmation_is_fresh(pending_now))
                # An active plan session already owns this chat's plain text;
                # questions about the plan must stay chat, not spawn a second
                # capture card.
                and active_remote_plan_session(state, chat_hash) is None
            ):
                # Natural-language delegation: offer to capture it as a plan
                # candidate. The tap is the approval; chat alone records nothing.
                capture = build_confirmation(
                    kind="plan_capture", target_id="plan", action_kind="capture",
                    observed_hash="", note=sanitize_text(capture_note, max_chars=380),
                    chat_hash=chat_hash, ttl_sec=900,
                )
                store_confirmation(state, chat_hash, capture)
                preview = "\n".join([
                    "<b>계획 후보로 등록할까요?</b>",
                    sanitize_text(capture_note, max_chars=150),
                    "확인 시 검토와 야간 실행 대기열에 계획 후보로 기록됩니다.",
                    "다음 조치: 확인 또는 취소",
                ])
                rendered["message_preview"] = preview
                rendered["mobile_card_contract"] = mobile_card_contract(preview)
                rendered["reply_markup_preview"] = choice_keyboard({"context_kind": "dispatch_confirm"})
                rendered["plan_capture_offered"] = True
        if parsed_command.get("command") in {
            "decision",
            "recover",
            "dispatch",
            "run",
            "cancel_task",
            "session_approve",
            "session_deny",
            "pause",
            "resume",
            "confirm",
            "cancel",
        }:
            # Guarded remote execution. Every branch catches its own errors so
            # nothing raises before the offset save and re-delivers the update.
            resolve_dispatch_confirmation(args, state, chat_hash, rendered, parsed_command)
            plan_text = rendered.pop("captured_plan_text", None)
            if plan_text:
                synthetic = {
                    "command": "plan_request", "feedback_kind": "planning_request",
                    "plan_text": plan_text, "feedback_text": plan_text,
                    "reason": "plan_capture_confirm",
                }
                capture_context = last_context_for_chat_hash(
                    state, chat_hash, max_age_sec=args.context_max_age_sec,
                )
                capture_result = record_feedback(
                    args, config, message, plan_text,
                    feedback_context=capture_context, parsed_command=synthetic,
                )
                capture_record = capture_result.pop("feedback_record", None)
                rendered.update(capture_result)
                if isinstance(capture_record, dict):
                    capture_ingest = ingest_feedback_decision(args, capture_record)
                    rendered.update(capture_ingest)
                    # Mirror the direct /plan path: store the session and move
                    # straight into project selection, so a confirmed capture
                    # behaves exactly like typing /plan.
                    session = create_remote_plan_session(
                        args, chat_hash=chat_hash, request_text=plan_text,
                        parsed_command=synthetic, feedback_context=capture_context,
                        decision_id=capture_ingest.get("decision_feedback_decision_id"),
                    )
                    store_remote_plan_session(state, chat_hash, session)
                    rendered["remote_plan_session"] = public_remote_plan_session(session)
                    rendered["message_preview"] = render_project_selection_message(
                        profile=args.profile,
                        session=session,
                    )
                    attach_choice_surface(rendered, remote_plan_selection_context(session))
                    rendered["mobile_card_contract"] = mobile_card_contract(rendered["message_preview"])
        try:
            message_id = send_message(
                config,
                chat_id_for(message),
                rendered["message_preview"],
                args,
                reply_markup=rendered.get("reply_markup_preview")
                if isinstance(rendered.get("reply_markup_preview"), dict)
                else None,
            )
            rendered["send_status"] = "dry_run" if args.dry_run else "sent"
        except RemoteOperatorTelegramError as error:
            if "Telegram API" not in str(error):
                raise
            message_id = None
            rendered["status"] = "send_failed"
            rendered["send_status"] = "failed"
            rendered["send_error"] = sanitize_text(str(error), max_chars=240)
        rendered["sent_message_id"] = message_id
        remember_context_for_chat_hash(state, chat_hash, rendered)
        rendered_parsed = (
            rendered.get("parsed_command")
            if isinstance(rendered.get("parsed_command"), dict)
            else {}
        )
        if rendered_parsed.get("command") == "chat":
            append_chat_history(
                state,
                chat_hash,
                role="operator",
                text=str(rendered_parsed.get("chat_text") or text),
            )
            assistant_reply = agent_assistant_reply(rendered_parsed.get("agent_intent"))
            if assistant_reply:
                append_chat_history(state, chat_hash, role="assistant", text=assistant_reply)
        result = rendered
        break
    attention = scan_and_notify_attention(args, config, state)
    if attention is not None:
        result["attention_notification"] = attention
    autonomy = scan_and_propose_autonomy(args, config, state)
    if autonomy is not None:
        result["autonomy_proposal"] = autonomy
    session_notification = scan_and_notify_waiting_sessions(args, config, state)
    if session_notification is not None:
        result["session_notification"] = session_notification
    offset_advanced = max_update_id >= int(state.get("offset") or 0)
    if offset_advanced:
        state["offset"] = max_update_id + 1
    # A successful attention scan prunes/marks the notified registry, so persist
    # state even when no update advanced the offset; otherwise the same item
    # would re-notify on the next poll.
    attention_touched_state = bool(
        attention and attention.get("status") in {"notified", "no_new_attention"}
    )
    session_touched_state = bool(
        session_notification
        and session_notification.get("status") in {"notified", "no_new_waiting"}
    )
    if offset_advanced or attention_touched_state or session_touched_state:
        save_state(args.state_file, state)
    return result


def loop_summary_base(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    result = result_base(args, config, "live_loop")
    result.update(
        {
            "status": "polling",
            "poll_count": 0,
            "updates_seen": 0,
            "handled_result_count": 0,
            "last_result": None,
            "last_handled_result": None,
        }
    )
    return result


def update_loop_summary(summary: dict[str, Any], result: dict[str, Any]) -> None:
    summary["poll_count"] = int(summary.get("poll_count") or 0) + 1
    summary["updates_seen"] = int(summary.get("updates_seen") or 0) + int(result.get("updates_seen") or 0)
    summary["last_result"] = result
    if result.get("status") not in {"no_update", "poll_error", "loop_error"}:
        summary["handled_result_count"] = int(summary.get("handled_result_count") or 0) + 1
        summary["last_handled_result"] = result


def loop_transport_error_result(
    args: argparse.Namespace,
    config: dict[str, Any],
    error: RemoteOperatorTelegramError,
) -> dict[str, Any]:
    result = result_base(args, config, "live_once")
    result.update(
        {
            "status": "poll_error",
            "updates_seen": 0,
            "reason": "telegram_transport_error",
            "error": sanitize_text(str(error), max_chars=240),
        }
    )
    return result


def loop_internal_error_result(
    args: argparse.Namespace,
    config: dict[str, Any],
    error: Exception,
) -> dict[str, Any]:
    result = result_base(args, config, "live_once")
    result.update(
        {
            "status": "loop_error",
            "updates_seen": 0,
            "reason": "unexpected_loop_exception",
            "error": sanitize_text(f"{type(error).__name__}: {error}", max_chars=240),
        }
    )
    return result


def loop_backoff_if_needed(
    args: argparse.Namespace,
    result: dict[str, Any],
    consecutive_errors: int,
) -> int:
    status = str(result.get("status") or "")
    if status not in {"poll_error", "send_failed", "loop_error"}:
        return 0
    consecutive_errors += 1
    if args.max_polls is None:
        sleep_sec = min(max(0, int(args.poll_error_backoff_sec)) * consecutive_errors, 60)
        if sleep_sec > 0:
            time.sleep(sleep_sec)
    return consecutive_errors


def result_carries_push(result: dict[str, Any]) -> bool:
    """True when a proactive card went out on this poll, even with no inbound update.

    Quiet polls report status "no_update", which run_loop does not print; a
    push buried in one would leave no journal trace, making notify latency
    unmeasurable.
    """

    session = result.get("session_notification")
    if isinstance(session, dict) and session.get("status") == "notified":
        return True
    attention = result.get("attention_notification")
    if isinstance(attention, dict) and attention.get("status") == "notified":
        return True
    autonomy = result.get("autonomy_proposal")
    return isinstance(autonomy, dict) and autonomy.get("status") == "proposed"


def loop_status_path(args: argparse.Namespace) -> pathlib.Path | None:
    if args.out:
        return args.out
    return args.loop_status_file


def run_loop(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    summary = loop_summary_base(args, config)
    max_polls = args.max_polls
    status_path = loop_status_path(args)
    consecutive_errors = 0
    try:
        while max_polls is None or int(summary["poll_count"]) < max_polls:
            try:
                result = run_once(args, config)
            except RemoteOperatorTelegramError as error:
                if "Telegram API" not in str(error):
                    result = loop_internal_error_result(args, config, error)
                else:
                    result = loop_transport_error_result(args, config, error)
            except Exception as error:
                result = loop_internal_error_result(args, config, error)
            update_loop_summary(summary, result)
            if status_path:
                write_json(status_path, summary)
            if max_polls is None and (
                result.get("status") != "no_update" or result_carries_push(result)
            ):
                print(json.dumps(result, ensure_ascii=False), flush=True)
            consecutive_errors = loop_backoff_if_needed(args, result, consecutive_errors)
    except KeyboardInterrupt:
        summary["status"] = "interrupted"
        if status_path:
            write_json(status_path, summary)
        return summary
    summary["status"] = "max_polls_reached" if max_polls is not None else "stopped"
    return summary


def send_command_text(args: argparse.Namespace, config: dict[str, Any]) -> dict[str, Any]:
    target_chat_id = str(config.get("target_chat_id") or "").strip()
    if not target_chat_id:
        raise RemoteOperatorTelegramError("target chat id is missing")
    state = load_state(args.state_file)
    feedback_context = last_context_for_chat_hash(
        state,
        sha256_short(target_chat_id),
        max_age_sec=args.context_max_age_sec,
    )
    rendered = render_command_result(
        args,
        config,
        args.send_command_text or "/status",
        mode="live_send",
        feedback_context=feedback_context,
    )
    if rendered.get("status") != "rendered":
        return rendered
    rendered["sent_message_id"] = send_message(
        config,
        target_chat_id,
        rendered["message_preview"],
        args,
        reply_markup=rendered.get("reply_markup_preview")
        if isinstance(rendered.get("reply_markup_preview"), dict)
        else None,
    )
    remember_context_for_chat_hash(state, sha256_short(target_chat_id), rendered)
    save_state(args.state_file, state)
    return rendered


def emit_result(args: argparse.Namespace, result: dict[str, Any]) -> None:
    if args.out:
        write_json(args.out, result)
    print(json.dumps(result, ensure_ascii=False, indent=2))


def main() -> int:
    args = parse_args()
    try:
        if args.max_polls is not None and args.max_polls < 1:
            raise RemoteOperatorTelegramError("--max-polls must be at least 1")
        if args.once and args.max_polls is not None:
            raise RemoteOperatorTelegramError("--once and --max-polls cannot be used together")
        if args.projection_file and not args.dry_run:
            raise RemoteOperatorTelegramError("--projection-file is only allowed with --dry-run")
        if args.replay_update_file and not args.dry_run:
            raise RemoteOperatorTelegramError("--replay-update-file is only allowed with --dry-run")
        if args.max_polls is not None and not args.replay_update_file and (args.dry_run or args.once or args.send_command_text):
            raise RemoteOperatorTelegramError("--max-polls is only used by the live poller or dry-run replay poller")
        if args.health:
            config = resolve_telegram_config(args.env_file, required=False)
            result = listener_health(args, config)
            emit_result(args, result)
            return 0 if result.get("health_status") == "healthy" else 1
        if args.dry_run:
            config = resolve_telegram_config(args.env_file, required=False)
            if args.replay_update_file:
                result = run_loop(args, config) if args.max_polls is not None else run_once(args, config)
                emit_result(args, result)
                return 0 if result.get("status") != "unsupported" else 2
            command_text = args.command_text or args.send_command_text or "/status"
            state = load_state(args.state_file)
            feedback_context = last_context_for_chat_hash(
                state,
                config.get("target_chat_id_hash"),
                max_age_sec=args.context_max_age_sec,
            )
            result = render_command_result(
                args,
                config,
                command_text,
                mode="dry_run",
                feedback_context=feedback_context,
            )
            emit_result(args, result)
            return 0 if result.get("status") != "unsupported" else 2
        if args.send_command_text:
            config = resolve_telegram_config(args.env_file, required=True)
            result = send_command_text(args, config)
            emit_result(args, result)
            return 0 if result.get("status") != "unsupported" else 2
        config = resolve_telegram_config(args.env_file, required=True)
        if args.enable_runtime_dispatch:
            # Make this powerful mode visible in logs/systemd journal: it lets
            # the allowlisted chat queue arbitrary commands for tick execution.
            print(
                "WARNING: --enable-runtime-dispatch is ON. Allowlisted Telegram chat can "
                "queue arbitrary commands for `forager offdesk tick`. Treat this as remote "
                "command execution and keep the chat allowlist locked down.",
                file=sys.stderr,
                flush=True,
            )
        result = run_once(args, config) if args.once else run_loop(args, config)
        emit_result(args, result)
        return 0
    except RemoteOperatorTelegramError as error:
        result = {
            "schema": RESULT_SCHEMA,
            "generated_at": utc_now(),
            "status": "error",
            "error": sanitize_text(str(error)),
            "read_only": True,
            "mutation_authorized": False,
            "approval_authorized": False,
            "forbidden_remote_intents": list(FORBIDDEN_REMOTE_INTENTS),
        }
        if args.out:
            write_json(args.out, result)
        print(json.dumps(result, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
