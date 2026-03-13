#!/usr/bin/env python3
import argparse
import fcntl
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from aoe_tg_acl import (
    ensure_chat_allowed,
    format_csv_set,
    is_valid_chat_alias,
    is_valid_chat_id,
    normalize_owner_chat_id,
    parse_csv_set,
    resolve_role_from_acl_sets,
)
from aoe_tg_chat_aliases import (
    alias_table_summary,
    ensure_chat_alias,
    ensure_chat_aliases,
    find_chat_alias,
    load_chat_aliases,
    merged_chat_aliases,
    next_chat_alias,
    resolve_chat_aliases_file,
    resolve_chat_ref,
    save_chat_aliases,
    update_chat_alias_cache,
)
from aoe_tg_chat_state import (
    clear_chat_report_level,
    clear_confirm_action,
    clear_default_mode,
    clear_pending_mode,
    get_chat_lang,
    get_chat_recent_task_refs,
    get_chat_report_level,
    get_chat_room,
    get_chat_selected_task_ref,
    get_chat_session_row,
    get_chat_sessions,
    get_confirm_action,
    get_default_mode,
    get_pending_mode,
    normalize_chat_lang_token,
    sanitize_chat_session_row,
    resolve_chat_task_ref,
    set_chat_lang,
    set_chat_recent_task_refs,
    set_chat_report_level,
    set_chat_room,
    set_chat_selected_task_ref,
    set_confirm_action,
    set_default_mode,
    set_pending_mode,
    touch_chat_recent_task_ref,
)
from aoe_tg_command_handlers import (
    build_non_run_context,
    build_non_run_deps,
    handle_non_run_command_pipeline,
)
from aoe_tg_gateway_events import (
    append_gateway_event_targets as gateway_append_gateway_event_targets,
    log_gateway_event as gateway_log_gateway_event,
    mirror_backend_runtime_events as gateway_mirror_backend_runtime_events,
    task_identifiers as gateway_task_identifiers,
)
import aoe_tg_gateway_state as gateway_state_mod
import aoe_tg_gateway_aux as gateway_aux_mod
import aoe_tg_gateway_batch_ops as gateway_batch_ops_mod
import aoe_tg_cli as cli_mod
import aoe_tg_room_runtime as room_runtime_mod
import aoe_tg_orch_registry as orch_registry_mod
import aoe_tg_orch_roles as orch_roles_mod
import aoe_tg_orch_responses as orch_responses_mod
import aoe_tg_plan_ensemble as plan_ensemble_mod
import aoe_tg_poll_loop as poll_loop_mod
import aoe_tg_message_handler as message_handler_mod
import aoe_tg_request_state as request_state_mod
from aoe_tg_role_aliases import canonicalize_role_name
import aoe_tg_tf_exec as tf_exec_mod
import aoe_tg_tf_backend_selection as tf_backend_selection_mod
import aoe_tg_tf_backend_local as tf_backend_local_mod
import aoe_tg_tf_backend_autogen as tf_backend_autogen_mod
from aoe_tg_message_flow import (
    RunTransitionState,
    apply_confirm_transition_to_resolved,
    apply_retry_transition_to_resolved,
    enforce_command_auth,
)
from aoe_tg_runtime_core import (
    acquire_process_lock as runtime_acquire_process_lock,
    default_manager_state as runtime_default_manager_state,
    ensure_default_project_registered as runtime_ensure_default_project_registered,
    load_manager_state as runtime_load_manager_state,
    resolve_project_root,
    resolve_state_file,
    resolve_team_dir,
    save_manager_state as runtime_save_manager_state,
)
from aoe_tg_todo_state import merge_todo_proposals
from aoe_tg_investigations_sync import sync_investigations_docs
from aoe_tg_ops_policy import (
    visible_ops_project_keys,
)
from aoe_tg_package_paths import templates_root, worker_handler_script
import aoe_tg_project_state as project_state_mod
from aoe_tg_project_runtime import project_runtime_label
from aoe_tg_runtime_seed import repair_runtime
from aoe_tg_run_handlers import (
    build_run_context,
    build_run_deps,
    handle_run_or_unknown_command,
    resolve_confirm_run_transition,
)
from aoe_tg_command_resolver import ResolvedCommand, resolve_message_command
from aoe_tg_parse import (
    detect_high_risk_prompt,
    normalize_mode_token,
    normalize_report_token,
    parse_command,
)
from aoe_tg_tf_backend import (
    AUTOGEN_CORE_TF_BACKEND,
    DEFAULT_TF_BACKEND,
    availability_tuple,
    build_tf_backend_deps,
    build_tf_backend_request,
    normalize_tf_backend_name,
)
from aoe_tg_room_handlers import DEFAULT_MAX_EVENT_CHARS, DEFAULT_MAX_FILE_BYTES, DEFAULT_ROOM_NAME, append_room_event, normalize_room_token
from aoe_tg_orch_contract import attach_phase2_team_spec
from aoe_tg_schema import (
    default_plan_critic_payload,
    normalize_exec_critic_payload,
    normalize_plan_critic_payload,
    normalize_plan_replans_payload,
    plan_critic_primary_issue,
    normalize_task_plan_payload as normalize_task_plan_schema,
)
from aoe_tg_task_view import (
    build_task_context as build_task_context_view,
    request_to_tf_id as request_to_tf_id_view,
    summarize_task_lifecycle as summarize_task_lifecycle_view,
    task_display_label as task_display_label_view,
    task_short_to_tf_id as task_short_to_tf_id_view,
)
from aoe_tg_transport import (
    build_quick_reply_keyboard,
    preferred_command_prefix,
    safe_tg_send_text,
    split_text,
    tg_api,
    tg_get_updates,
    tg_send_text,
)
from aoe_tg_task_state import (
    assign_task_alias as assign_task_alias_state,
    backfill_task_aliases as backfill_task_aliases_state,
    derive_task_alias_base as derive_task_alias_base_state,
    ensure_project_tasks as ensure_project_tasks_state,
    ensure_task_alias_meta as ensure_task_alias_meta_state,
    ensure_task_record as ensure_task_record_state,
    extract_request_snapshot as extract_request_snapshot_state,
    format_task_short_id as format_task_short_id_state,
    get_task_record as get_task_record_state,
    latest_task_request_refs as latest_task_request_refs_state,
    lifecycle_set_stage as lifecycle_set_stage_state,
    normalize_task_alias_key as normalize_task_alias_key_state,
    parse_task_seq_from_short_id as parse_task_seq_from_short_id_state,
    rebuild_task_alias_index as rebuild_task_alias_index_state,
    resolve_task_request_id as resolve_task_request_id_state,
    sanitize_task_record as sanitize_task_record_state,
    summarize_task_monitor as summarize_task_monitor_state,
    sync_task_lifecycle as sync_task_lifecycle_state,
    trim_project_tasks as trim_project_tasks_state,
)

DEFAULT_POLL_TIMEOUT_SEC = 25
DEFAULT_HTTP_TIMEOUT_SEC = 60
DEFAULT_ORCH_TIMEOUT_SEC = 600
DEFAULT_ORCH_POLL_SEC = 2.0
DEFAULT_ORCH_COMMAND_TIMEOUT_SEC = 900
DEFAULT_MAX_TEXT_CHARS = 3800
DEFAULT_TASK_HISTORY_LIMIT = 80
DEFAULT_TASK_KEEP_PER_PROJECT = 120
DEFAULT_VERIFIER_ROLES = "Codex-Reviewer,QA,Verifier"
DEFAULT_TASK_PLAN_MAX_SUBTASKS = 4
DEFAULT_TASK_PLAN_REPLAN_ATTEMPTS = 2
DEFAULT_SLASH_ONLY = True
DEFAULT_DENY_BY_DEFAULT = True
DEFAULT_GATEWAY_LOG_MAX_BYTES = 5 * 1024 * 1024
DEFAULT_GATEWAY_LOG_KEEP_FILES = 5
DEFAULT_CONFIRM_TTL_SEC = 300
DEFAULT_CHAT_MAX_RUNNING = 2
DEFAULT_CHAT_DAILY_CAP = 40
DEFAULT_UI_LANG = "ko"
DEFAULT_REPLY_LANG = "ko"
DEFAULT_REPORT_LEVEL = "normal"
DEFAULT_PROJECT_ALIAS_MAX = 999
DEFAULT_GATEWAY_DEDUP_KEEP = 2000
DEFAULT_FAILED_QUEUE_KEEP = 200
DEFAULT_FAILED_QUEUE_TTL_HOURS = 168
DEFAULT_TF_EXEC_MODE = "worktree"  # none|inplace|worktree
DEFAULT_TF_WORK_ROOT_NAME = ".aoe-tf"
DEFAULT_TF_EXEC_MAP_FILE = "tf_exec_map.json"
DEFAULT_TF_EXEC_CACHE_TTL_HOURS = 72
DEFAULT_TF_WORKER_SESSION_PREFIX = "tfw_"
DEFAULT_TF_WORKER_STARTUP_GRACE_SEC = 30
DEFAULT_ROOM_RETENTION_DAYS = 14
DEFAULT_ROOM_AUTOPUBLISH_ROUTE = "project"  # room|project|project-tf|tf
REPLAY_USAGE = "usage: /replay [list|latest|<idx>|<id>|show <idx|id|latest>|purge]"
STATE_SEEN_UPDATE_IDS_KEY = "seen_update_ids"
STATE_SEEN_MESSAGE_KEYS_KEY = "seen_message_keys"
STATE_ACKED_UPDATES_KEY = "acked_updates"
STATE_HANDLED_MESSAGES_KEY = "handled_messages"
STATE_DUPLICATE_SKIPPED_KEY = "duplicate_skipped"
STATE_EMPTY_SKIPPED_KEY = "empty_skipped"
STATE_UNAUTHORIZED_SKIPPED_KEY = "unauthorized_skipped"
STATE_HANDLER_ERRORS_KEY = "handler_errors"
STATE_FAILED_QUEUE_KEY = "failed_queue"
TASK_STAGE_STATUS_ALLOWED = {"pending", "running", "done", "failed"}
TASK_OVERALL_STATUS_ALLOWED = {"pending", "running", "completed", "failed"}
LIFECYCLE_STAGES = (
    "intake",
    "planning",
    "staffing",
    "execution",
    "verification",
    "integration",
    "close",
)

ERROR_COMMAND = "E_COMMAND"
ERROR_TIMEOUT = "E_TIMEOUT"
ERROR_GATE = "E_GATE"
ERROR_ORCH = "E_ORCH"
ERROR_REQUEST = "E_REQUEST"
ERROR_TELEGRAM = "E_TELEGRAM"
ERROR_INTERNAL = "E_INTERNAL"
ERROR_AUTH = "E_AUTH"

READONLY_ALLOWED_COMMANDS = {
    "start",
    "help",
    "tutorial",
    "orch-help",
    "mode",
    "lang",
    "report",
    "whoami",
    "acl",
    "status",
    "orch-status",
    "request",
    "orch-list",
    "orch-monitor",
    "orch-kpi",
    "orch-check",
    "orch-task",
    "orch-pick",
    "todo",
    "room",
    "queue",
    "offdesk",
    "auto",
    "replay-read",
    "cancel-pending",
}


def sync_acl_env_file(args: argparse.Namespace) -> None:
    env_path = args.team_dir / "telegram.env"
    upsert_env_var(env_path, "TELEGRAM_ALLOW_CHAT_IDS", format_csv_set(args.allow_chat_ids))
    upsert_env_var(env_path, "TELEGRAM_ADMIN_CHAT_IDS", format_csv_set(args.admin_chat_ids))
    upsert_env_var(env_path, "TELEGRAM_READONLY_CHAT_IDS", format_csv_set(args.readonly_chat_ids))
    if str(getattr(args, "owner_chat_id", "") or "").strip():
        upsert_env_var(env_path, "TELEGRAM_OWNER_CHAT_ID", str(args.owner_chat_id).strip())
    # Persist one-way safety knobs if they are enabled at runtime.
    # We intentionally do not write "0" values here to avoid accidental downgrades.
    if bool(getattr(args, "deny_by_default", False)):
        upsert_env_var(env_path, "AOE_DENY_BY_DEFAULT", "1")
    if bool(getattr(args, "owner_only", False)):
        upsert_env_var(env_path, "AOE_OWNER_ONLY", "1")
    owner_bootstrap_mode = str(getattr(args, "owner_bootstrap_mode", "") or "").strip().lower()
    if owner_bootstrap_mode in {"dispatch", "direct"}:
        upsert_env_var(env_path, "AOE_OWNER_BOOTSTRAP_MODE", owner_bootstrap_mode)


def dedup_keep_limit() -> int:
    return gateway_state_mod.dedup_keep_limit(
        int_from_env=int_from_env,
        default_keep=DEFAULT_GATEWAY_DEDUP_KEEP,
    )


def failed_queue_keep_limit() -> int:
    return gateway_state_mod.failed_queue_keep_limit(
        int_from_env=int_from_env,
        default_keep=DEFAULT_FAILED_QUEUE_KEEP,
    )


def failed_queue_ttl_hours() -> int:
    return gateway_state_mod.failed_queue_ttl_hours(
        int_from_env=int_from_env,
        default_ttl_hours=DEFAULT_FAILED_QUEUE_TTL_HOURS,
    )


def normalize_recent_tokens(raw: Any, keep: int) -> List[str]:
    return gateway_state_mod.normalize_recent_tokens(raw, keep)


def append_recent_token(tokens: List[str], token: str, keep: int) -> None:
    return gateway_state_mod.append_recent_token(tokens, token, keep)


def message_dedup_key(msg: Dict[str, Any]) -> str:
    return gateway_state_mod.message_dedup_key(msg)


def normalize_failed_queue(raw: Any, keep: int) -> List[Dict[str, Any]]:
    return gateway_state_mod.normalize_failed_queue(
        raw,
        keep,
        failed_queue_ttl_hours=failed_queue_ttl_hours,
        now_iso=now_iso,
        parse_iso_ts=parse_iso_ts,
    )


def enqueue_failed_message(
    state: Dict[str, Any],
    *,
    chat_id: str,
    text: str,
    trace_id: str,
    error_code: str,
    error_detail: str,
    cmd: str = "",
) -> Dict[str, Any]:
    return gateway_state_mod.enqueue_failed_message(
        state,
        chat_id=chat_id,
        text=text,
        trace_id=trace_id,
        error_code=error_code,
        error_detail=error_detail,
        cmd=cmd,
        failed_queue_keep_limit=failed_queue_keep_limit,
        normalize_failed_queue=normalize_failed_queue,
        failed_queue_key=STATE_FAILED_QUEUE_KEY,
        now_iso=now_iso,
    )


def failed_queue_for_chat(state: Dict[str, Any], chat_id: str) -> List[Dict[str, Any]]:
    return gateway_state_mod.failed_queue_for_chat(
        state,
        chat_id,
        failed_queue_keep_limit=failed_queue_keep_limit,
        normalize_failed_queue=normalize_failed_queue,
        failed_queue_key=STATE_FAILED_QUEUE_KEY,
    )


def remove_failed_queue_item(state: Dict[str, Any], item_id: str) -> Optional[Dict[str, Any]]:
    return gateway_state_mod.remove_failed_queue_item(
        state,
        item_id,
        failed_queue_keep_limit=failed_queue_keep_limit,
        normalize_failed_queue=normalize_failed_queue,
        failed_queue_key=STATE_FAILED_QUEUE_KEY,
    )


def purge_failed_queue_for_chat(state: Dict[str, Any], chat_id: str) -> int:
    return gateway_state_mod.purge_failed_queue_for_chat(
        state,
        chat_id,
        failed_queue_keep_limit=failed_queue_keep_limit,
        normalize_failed_queue=normalize_failed_queue,
        failed_queue_key=STATE_FAILED_QUEUE_KEY,
    )


def format_failed_queue_item_detail(row: Dict[str, Any]) -> str:
    return gateway_state_mod.format_failed_queue_item_detail(row, replay_usage=REPLAY_USAGE)


def summarize_failed_queue(state: Dict[str, Any], chat_id: str, limit: int = 8) -> str:
    return gateway_state_mod.summarize_failed_queue(
        state,
        chat_id,
        limit=limit,
        failed_queue_for_chat=failed_queue_for_chat,
        replay_usage=REPLAY_USAGE,
    )


def resolve_failed_queue_item(state: Dict[str, Any], chat_id: str, target: str) -> Tuple[Optional[Dict[str, Any]], str]:
    return gateway_state_mod.resolve_failed_queue_item(
        state,
        chat_id,
        target,
        failed_queue_for_chat=failed_queue_for_chat,
    )


def load_state(path: Path) -> Dict[str, Any]:
    return gateway_state_mod.load_state(
        path,
        acked_updates_key=STATE_ACKED_UPDATES_KEY,
        handled_messages_key=STATE_HANDLED_MESSAGES_KEY,
        duplicate_skipped_key=STATE_DUPLICATE_SKIPPED_KEY,
        empty_skipped_key=STATE_EMPTY_SKIPPED_KEY,
        unauthorized_skipped_key=STATE_UNAUTHORIZED_SKIPPED_KEY,
        handler_errors_key=STATE_HANDLER_ERRORS_KEY,
        failed_queue_key=STATE_FAILED_QUEUE_KEY,
        seen_update_ids_key=STATE_SEEN_UPDATE_IDS_KEY,
        seen_message_keys_key=STATE_SEEN_MESSAGE_KEYS_KEY,
        dedup_keep_limit=dedup_keep_limit,
        failed_queue_keep_limit=failed_queue_keep_limit,
        normalize_recent_tokens=normalize_recent_tokens,
        normalize_failed_queue=normalize_failed_queue,
    )


def save_state(path: Path, state: Dict[str, Any]) -> None:
    return gateway_state_mod.save_state(
        path,
        state,
        acked_updates_key=STATE_ACKED_UPDATES_KEY,
        handled_messages_key=STATE_HANDLED_MESSAGES_KEY,
        duplicate_skipped_key=STATE_DUPLICATE_SKIPPED_KEY,
        empty_skipped_key=STATE_EMPTY_SKIPPED_KEY,
        unauthorized_skipped_key=STATE_UNAUTHORIZED_SKIPPED_KEY,
        handler_errors_key=STATE_HANDLER_ERRORS_KEY,
        failed_queue_key=STATE_FAILED_QUEUE_KEY,
        seen_update_ids_key=STATE_SEEN_UPDATE_IDS_KEY,
        seen_message_keys_key=STATE_SEEN_MESSAGE_KEYS_KEY,
        dedup_keep_limit=dedup_keep_limit,
        failed_queue_keep_limit=failed_queue_keep_limit,
        normalize_recent_tokens=normalize_recent_tokens,
        normalize_failed_queue=normalize_failed_queue,
    )


def summarize_gateway_poll_state(state_file: Optional[Any], project_name: str = "") -> str:
    return gateway_state_mod.summarize_gateway_poll_state(
        state_file,
        project_name=project_name,
        load_state=load_state,
        acked_updates_key=STATE_ACKED_UPDATES_KEY,
        handled_messages_key=STATE_HANDLED_MESSAGES_KEY,
        duplicate_skipped_key=STATE_DUPLICATE_SKIPPED_KEY,
        empty_skipped_key=STATE_EMPTY_SKIPPED_KEY,
        unauthorized_skipped_key=STATE_UNAUTHORIZED_SKIPPED_KEY,
        handler_errors_key=STATE_HANDLER_ERRORS_KEY,
        failed_queue_key=STATE_FAILED_QUEUE_KEY,
        seen_update_ids_key=STATE_SEEN_UPDATE_IDS_KEY,
        seen_message_keys_key=STATE_SEEN_MESSAGE_KEYS_KEY,
        normalize_recent_tokens=normalize_recent_tokens,
        dedup_keep_limit=dedup_keep_limit,
        parse_iso_ts=parse_iso_ts,
    )


def upsert_env_var(path: Path, key: str, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows: List[str] = []
    if path.exists():
        rows = path.read_text(encoding="utf-8").splitlines()

    out: List[str] = []
    replaced = False
    prefix = f"{key}="
    for row in rows:
        if row.startswith(prefix):
            out.append(f"{key}={value}")
            replaced = True
        else:
            out.append(row)

    if not replaced:
        out.append(f"{key}={value}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text("\n".join(out).rstrip("\n") + "\n", encoding="utf-8")
    os.replace(tmp, path)


def resolve_manager_state_file(team_dir: Path, explicit_state_file: Optional[str]) -> Path:
    if explicit_state_file:
        return Path(explicit_state_file).expanduser().resolve()
    env_path = (os.environ.get("AOE_ORCH_MANAGER_STATE") or "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()
    return team_dir / "orch_manager_state.json"


def resolve_workspace_root(raw: Optional[str]) -> Optional[Path]:
    src = (raw or "").strip()
    if not src:
        return None
    return Path(src).expanduser().resolve()


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def bool_from_env(raw: Optional[str], default: bool) -> bool:
    if raw is None:
        return default
    token = str(raw).strip().lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    return default


def bool_from_json(raw: Any, default: bool) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    if isinstance(raw, (int, float)):
        return bool(raw)
    token = str(raw).strip().lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    return default


def int_from_env(raw: Optional[str], default: int, minimum: int, maximum: int) -> int:
    token = str(raw or "").strip()
    try:
        value = int(token) if token else int(default)
    except Exception:
        value = int(default)
    return max(int(minimum), min(int(maximum), value))


def parse_iso_ts(raw: str) -> Optional[datetime]:
    src = str(raw or "").strip()
    if not src:
        return None
    try:
        return datetime.strptime(src, "%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        pass
    # Accept RFC3339 offsets (+00:00) and "Z" suffix.
    src2 = src[:-1] + "+00:00" if src.endswith("Z") else src
    try:
        return datetime.fromisoformat(src2)
    except Exception:
        return None


def percentile(values: List[int], pct: float) -> int:
    if not values:
        return 0
    ordered = sorted(int(v) for v in values)
    if len(ordered) == 1:
        return ordered[0]
    rank = max(0.0, min(1.0, float(pct))) * (len(ordered) - 1)
    lo = int(rank)
    hi = min(len(ordered) - 1, lo + 1)
    if lo == hi:
        return ordered[lo]
    frac = rank - lo
    return int(round((ordered[lo] * (1.0 - frac)) + (ordered[hi] * frac)))


def today_key_local() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def date_key_from_iso(raw: str) -> str:
    parsed = parse_iso_ts(raw)
    if parsed is not None:
        return parsed.astimezone().strftime("%Y-%m-%d")
    text = str(raw or "").strip()
    if len(text) >= 10 and re.fullmatch(r"\d{4}-\d{2}-\d{2}", text[:10]):
        return text[:10]
    return ""


def compact_age_label(raw: str) -> str:
    parsed = parse_iso_ts(raw)
    if parsed is None:
        return "-"
    try:
        delta = datetime.now(parsed.tzinfo or timezone.utc) - parsed
    except Exception:
        try:
            delta = datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)
        except Exception:
            return "-"
    seconds = int(delta.total_seconds())
    if seconds < 0:
        seconds = 0
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 48:
        return f"{hours}h ago"
    days = hours // 24
    if days < 30:
        return f"{days}d ago"
    return parsed.astimezone().strftime("%Y-%m-%d")


def summarize_chat_usage(state: Dict[str, Any], chat_id: str) -> Tuple[int, int]:
    cid = str(chat_id or "").strip()
    if not cid:
        return 0, 0
    projects = state.get("projects")
    if not isinstance(projects, dict):
        return 0, 0

    today = today_key_local()
    running = 0
    submitted_today = 0
    for entry in projects.values():
        if not isinstance(entry, dict):
            continue
        tasks = entry.get("tasks")
        if not isinstance(tasks, dict):
            continue
        for task in tasks.values():
            if not isinstance(task, dict):
                continue
            owner = str(task.get("initiator_chat_id", "")).strip()
            if owner != cid:
                continue
            status = normalize_task_status(task.get("status", "pending"))
            if status in {"pending", "running"}:
                running += 1
            if date_key_from_iso(str(task.get("created_at", ""))) == today:
                submitted_today += 1
    return running, submitted_today


def mask_sensitive_text(raw: str) -> str:
    text = str(raw or "")
    if not text:
        return text

    text = re.sub(r"\b\d{8,}:[A-Za-z0-9_-]{20,}\b", "[REDACTED_TELEGRAM_TOKEN]", text)
    text = re.sub(
        r"(?i)\b(password|passwd|token|api[_-]?key|secret)\s*[:=]\s*([^\s]+)",
        lambda m: f"{m.group(1)}=[REDACTED]",
        text,
    )
    text = re.sub(r"(?i)\bbearer\s+[A-Za-z0-9._=-]+\b", "Bearer [REDACTED]", text)
    return text


def normalize_stage_status(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    if token in TASK_STAGE_STATUS_ALLOWED:
        return token
    aliases = {
        "complete": "done",
        "completed": "done",
        "success": "done",
        "active": "running",
        "in_progress": "running",
        "progress": "running",
        "fail": "failed",
        "error": "failed",
    }
    return aliases.get(token, "pending")


def normalize_task_status(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    if token in TASK_OVERALL_STATUS_ALLOWED:
        return token
    aliases = {
        "done": "completed",
        "complete": "completed",
        "success": "completed",
        "fail": "failed",
        "error": "failed",
        "active": "running",
        "in_progress": "running",
        "progress": "running",
    }
    return aliases.get(token, "pending")


def normalize_project_name(name: str) -> str:
    return project_state_mod.normalize_project_name(name)


def normalize_project_alias(token: str, max_alias: int = DEFAULT_PROJECT_ALIAS_MAX) -> str:
    return project_state_mod.normalize_project_alias(token, max_alias=max_alias)


def extract_project_alias_index(alias: str) -> int:
    return project_state_mod.extract_project_alias_index(alias)


def ensure_project_aliases(state: Dict[str, Any], max_alias: int = DEFAULT_PROJECT_ALIAS_MAX) -> Dict[str, str]:
    return project_state_mod.ensure_project_aliases(state, max_alias=max_alias)


def project_alias_for_key(state: Dict[str, Any], project_key: str) -> str:
    return project_state_mod.project_alias_for_key(state, project_key)


def sanitize_project_lock_row(raw: Any, projects: Any) -> Dict[str, Any]:
    return project_state_mod.sanitize_project_lock_row(
        raw,
        projects,
        bool_from_json=bool_from_json,
    )


def get_project_lock_row(state: Dict[str, Any]) -> Dict[str, Any]:
    return project_state_mod.get_project_lock_row(state, bool_from_json=bool_from_json)


def get_project_lock_key(state: Dict[str, Any]) -> str:
    return project_state_mod.get_project_lock_key(state, bool_from_json=bool_from_json)


def set_project_lock(state: Dict[str, Any], project_key: str, actor: str = "") -> Dict[str, Any]:
    return project_state_mod.set_project_lock(
        state,
        project_key,
        now_iso=now_iso,
        actor=actor,
    )


def clear_project_lock(state: Dict[str, Any]) -> bool:
    return project_state_mod.clear_project_lock(state, bool_from_json=bool_from_json)


def project_lock_label(state: Dict[str, Any]) -> str:
    return project_state_mod.project_lock_label(state, bool_from_json=bool_from_json)


def is_path_within(target: Path, root: Optional[Path]) -> bool:
    if root is None:
        return True
    try:
        target.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def default_manager_state(project_root: Path, team_dir: Path) -> Dict[str, Any]:
    return runtime_default_manager_state(project_root, team_dir, now_iso=now_iso)


def sanitize_task_record(raw_task: Dict[str, Any], req_id: str) -> Dict[str, Any]:
    return sanitize_task_record_state(
        raw_task,
        req_id,
        dedupe_roles=dedupe_roles,
        lifecycle_stages=LIFECYCLE_STAGES,
        normalize_stage_status=normalize_stage_status,
        normalize_task_status=normalize_task_status,
        now_iso=now_iso,
        history_limit=DEFAULT_TASK_HISTORY_LIMIT,
        normalize_task_plan_schema=normalize_task_plan_schema,
        normalize_plan_critic_payload=normalize_plan_critic_payload,
        normalize_plan_replans_payload=normalize_plan_replans_payload,
        plan_critic_primary_issue=plan_critic_primary_issue,
        normalize_exec_critic_payload=normalize_exec_critic_payload,
        build_task_context=build_task_context,
    )


def load_manager_state(path: Path, project_root: Path, team_dir: Path) -> Dict[str, Any]:
    return runtime_load_manager_state(
        path,
        project_root,
        team_dir,
        default_manager_state=default_manager_state,
        now_iso=now_iso,
        normalize_project_name=normalize_project_name,
        sanitize_task_record=sanitize_task_record,
        trim_project_tasks=trim_project_tasks,
        normalize_task_alias_key=normalize_task_alias_key,
        bool_from_json=bool_from_json,
        normalize_project_alias=normalize_project_alias,
        backfill_task_aliases=backfill_task_aliases,
        ensure_project_aliases=ensure_project_aliases,
        sanitize_project_lock_row=sanitize_project_lock_row,
        sanitize_chat_session_row=sanitize_chat_session_row,
    )


def save_manager_state(path: Path, state: Dict[str, Any]) -> None:
    return runtime_save_manager_state(
        path,
        state,
        now_iso=now_iso,
        sync_investigations_docs=sync_investigations_docs,
        cleanup_tf_exec_artifacts=cleanup_tf_exec_artifacts,
        cleanup_room_logs=cleanup_room_logs,
    )


def acquire_process_lock(lock_path: Path) -> Any:
    return runtime_acquire_process_lock(lock_path, now_iso=now_iso)


def append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    return room_runtime_mod.append_jsonl(
        path,
        row,
        int_from_env=int_from_env,
        default_max_bytes=DEFAULT_GATEWAY_LOG_MAX_BYTES,
        default_keep_files=DEFAULT_GATEWAY_LOG_KEEP_FILES,
    )


def room_retention_days() -> int:
    return room_runtime_mod.room_retention_days(
        int_from_env=int_from_env,
        default_room_retention_days=DEFAULT_ROOM_RETENTION_DAYS,
    )


def cleanup_room_logs(team_dir: Path, *, force: bool = False) -> int:
    return room_runtime_mod.cleanup_room_logs(
        team_dir,
        force=force,
        room_retention_days=room_retention_days,
        today_key_local=today_key_local,
    )


def room_autopublish_enabled() -> bool:
    return room_runtime_mod.room_autopublish_enabled(bool_from_env=bool_from_env)


def normalize_room_autopublish_route(raw: Optional[str]) -> str:
    return room_runtime_mod.normalize_room_autopublish_route(
        raw,
        default_room_autopublish_route=DEFAULT_ROOM_AUTOPUBLISH_ROUTE,
    )


def room_autopublish_route() -> str:
    return room_runtime_mod.room_autopublish_route(
        normalize_room_autopublish_route=normalize_room_autopublish_route,
    )


def _room_autopublish_title(event: str) -> str:
    return room_runtime_mod.room_autopublish_title(event)


def room_autopublish_event(
    *,
    team_dir: Path,
    manager_state: Dict[str, Any],
    chat_id: str,
    event: str,
    project: str,
    request_id: str,
    task: Optional[Dict[str, Any]],
    stage: str,
    status: str,
    error_code: str,
    detail: str,
) -> None:
    return room_runtime_mod.room_autopublish_event(
        team_dir=team_dir,
        manager_state=manager_state,
        chat_id=chat_id,
        event=event,
        project=project,
        request_id=request_id,
        task=task,
        stage=stage,
        status=status,
        error_code=error_code,
        detail=detail,
        room_autopublish_enabled=room_autopublish_enabled,
        project_alias_for_key=project_alias_for_key,
        get_chat_room=get_chat_room,
        normalize_room_token=normalize_room_token,
        room_autopublish_route=room_autopublish_route,
        int_from_env=int_from_env,
        task_display_label=task_display_label,
        append_room_event=append_room_event,
        now_iso=now_iso,
        default_room_name=DEFAULT_ROOM_NAME,
        default_max_event_chars=DEFAULT_MAX_EVENT_CHARS,
        default_max_file_bytes=DEFAULT_MAX_FILE_BYTES,
    )


def handle_replay_command(
    *,
    args: argparse.Namespace,
    token: str,
    chat_id: str,
    target: str,
    send: Any,
    log_event: Any,
) -> bool:
    return gateway_aux_mod.handle_replay_command(
        args=args,
        token=token,
        chat_id=chat_id,
        target=target,
        send=send,
        log_event=log_event,
        load_state=load_state,
        save_state=save_state,
        normalize_failed_queue=normalize_failed_queue,
        failed_queue_keep_limit=failed_queue_keep_limit,
        state_failed_queue_key=STATE_FAILED_QUEUE_KEY,
        summarize_failed_queue=summarize_failed_queue,
        purge_failed_queue_for_chat=purge_failed_queue_for_chat,
        resolve_failed_queue_item=resolve_failed_queue_item,
        format_failed_queue_item_detail=format_failed_queue_item_detail,
        remove_failed_queue_item=remove_failed_queue_item,
        parse_command=parse_command,
        handle_text_message=handle_text_message,
        preferred_command_prefix=preferred_command_prefix,
        replay_usage=REPLAY_USAGE,
    )


def summarize_gateway_metrics(
    team_dir: Path,
    project_name: str,
    hours: int = 24,
    state_file: Optional[Any] = None,
) -> str:
    return gateway_aux_mod.summarize_gateway_metrics(
        team_dir,
        project_name,
        hours=hours,
        state_file=state_file,
        summarize_gateway_poll_state=summarize_gateway_poll_state,
        parse_iso_ts=parse_iso_ts,
        percentile=percentile,
        error_internal=ERROR_INTERNAL,
    )


def task_identifiers(task: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    return gateway_task_identifiers(task)


def append_gateway_event_targets(*, team_dir: Path, row: Dict[str, Any], mirror_team_dir: Optional[Path] = None) -> None:
    return gateway_append_gateway_event_targets(
        team_dir=team_dir,
        row=row,
        append_jsonl=append_jsonl,
        mirror_team_dir=mirror_team_dir,
    )


def log_gateway_event(
    team_dir: Path,
    event: str,
    trace_id: str = "",
    project: str = "",
    request_id: str = "",
    task: Optional[Dict[str, Any]] = None,
    stage: str = "",
    actor: str = "gateway",
    status: str = "",
    error_code: str = "",
    latency_ms: int = 0,
    detail: str = "",
    mirror_team_dir: Optional[Path] = None,
) -> None:
    return gateway_log_gateway_event(
        team_dir=team_dir,
        event=event,
        now_iso=now_iso,
        mask_sensitive_text=mask_sensitive_text,
        append_gateway_event_targets=append_gateway_event_targets,
        trace_id=trace_id,
        project=project,
        request_id=request_id,
        task=task,
        stage=stage,
        actor=actor,
        status=status,
        error_code=error_code,
        latency_ms=latency_ms,
        detail=detail,
        mirror_team_dir=mirror_team_dir,
    )


def mirror_tf_backend_runtime_events(
    *,
    team_dir: Path,
    backend: str,
    runtime_events: List[Dict[str, Any]],
    trace_id: str = "",
    project: str = "",
    request_id: str = "",
    task: Optional[Dict[str, Any]] = None,
    mirror_team_dir: Optional[Path] = None,
) -> int:
    return gateway_mirror_backend_runtime_events(
        team_dir=team_dir,
        backend=backend,
        runtime_events=runtime_events,
        now_iso=now_iso,
        mask_sensitive_text=mask_sensitive_text,
        append_gateway_event_targets=append_gateway_event_targets,
        trace_id=trace_id,
        project=project,
        request_id=request_id,
        task=task,
        mirror_team_dir=mirror_team_dir,
    )


def classify_handler_error(err: Exception) -> Tuple[str, str, str]:
    return gateway_aux_mod.classify_handler_error(
        err,
        error_timeout=ERROR_TIMEOUT,
        error_command=ERROR_COMMAND,
        error_gate=ERROR_GATE,
        error_auth=ERROR_AUTH,
        error_request=ERROR_REQUEST,
        error_telegram=ERROR_TELEGRAM,
        error_orch=ERROR_ORCH,
        error_internal=ERROR_INTERNAL,
    )


def format_error_message(error_code: str, user_message: str, next_step: str, detail: str = "") -> str:
    return gateway_aux_mod.format_error_message(
        error_code,
        user_message,
        next_step,
        detail=detail,
        mask_sensitive_text=mask_sensitive_text,
    )


def ensure_default_project_registered(state: Dict[str, Any], project_root: Path, team_dir: Path) -> None:
    return runtime_ensure_default_project_registered(
        state,
        project_root,
        team_dir,
        now_iso=now_iso,
        bool_from_json=bool_from_json,
        normalize_project_alias=normalize_project_alias,
        normalize_project_name=normalize_project_name,
        sanitize_project_lock_row=sanitize_project_lock_row,
        ensure_project_aliases=ensure_project_aliases,
        backfill_task_aliases=backfill_task_aliases,
    )


def get_manager_project(state: Dict[str, Any], name: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    return project_state_mod.get_manager_project(
        state,
        name,
        bool_from_json=bool_from_json,
    )


def make_project_args(args: argparse.Namespace, entry: Dict[str, Any], key: str = "") -> argparse.Namespace:
    return project_state_mod.make_project_args(args, entry, key=key)


def register_orch_project(
    state: Dict[str, Any],
    name: str,
    project_root: Path,
    team_dir: Path,
    overview: str,
    set_active: bool,
) -> Tuple[str, Dict[str, Any]]:
    return project_state_mod.register_orch_project(
        state,
        name,
        project_root,
        team_dir,
        overview,
        set_active,
        now_iso=now_iso,
        trim_project_tasks=trim_project_tasks,
        bool_from_json=bool_from_json,
    )



def parse_roles_csv(raw: Optional[str]) -> List[str]:
    return orch_roles_mod.parse_roles_csv(raw)


def dedupe_roles(roles: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in roles:
        token = canonicalize_role_name(item)
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def load_orchestrator_roles(team_dir: Path) -> List[str]:
    return orch_roles_mod.load_orchestrator_roles(team_dir)


def load_orchestrator_role_profiles(team_dir: Path, available_roles: Optional[List[str]] = None) -> List[Dict[str, str]]:
    return orch_roles_mod.load_orchestrator_role_profiles(team_dir, available_roles)


def resolve_verifier_candidates(raw: Optional[str]) -> List[str]:
    return orch_roles_mod.resolve_verifier_candidates(raw, default_verifier_roles=DEFAULT_VERIFIER_ROLES)


def ensure_verifier_roles(
    selected_roles: List[str],
    available_roles: List[str],
    verifier_candidates: List[str],
) -> Tuple[List[str], List[str], bool, List[str]]:
    return orch_roles_mod.ensure_verifier_roles(
        selected_roles=selected_roles,
        available_roles=available_roles,
        verifier_candidates=verifier_candidates,
    )


def normalize_role_rows(data: Dict[str, Any]) -> List[Dict[str, str]]:
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
    pending_set = {str(x).strip() for x in (data.get("pending_roles") or data.get("unresolved_roles") or []) if str(x).strip()}

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


def extract_request_snapshot(data: Dict[str, Any]) -> Dict[str, Any]:
    return extract_request_snapshot_state(data, dedupe_roles=dedupe_roles)


def ensure_project_tasks(entry: Dict[str, Any]) -> Dict[str, Any]:
    return ensure_project_tasks_state(entry)


def normalize_task_alias_key(raw: str) -> str:
    return normalize_task_alias_key_state(raw)


def parse_task_seq_from_short_id(short_id: str) -> int:
    return parse_task_seq_from_short_id_state(short_id)


def format_task_short_id(seq: int) -> str:
    return format_task_short_id_state(seq)


def derive_task_alias_base(prompt: str) -> str:
    return derive_task_alias_base_state(prompt)


def ensure_task_alias_meta(entry: Dict[str, Any]) -> Tuple[Dict[str, str], int]:
    return ensure_task_alias_meta_state(entry)


def task_display_label(task: Dict[str, Any], fallback_request_id: str = "") -> str:
    return task_display_label_view(task, fallback_request_id=fallback_request_id)


def task_short_to_tf_id(short_id: str) -> str:
    return task_short_to_tf_id_view(short_id)


def request_to_tf_id(request_id: str) -> str:
    return request_to_tf_id_view(request_id)


def build_task_context(
    *,
    request_id: str = "",
    entry: Optional[Dict[str, Any]] = None,
    task: Optional[Dict[str, Any]] = None,
    tf_meta: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    return build_task_context_view(
        request_id=request_id,
        entry=entry,
        task=task,
        tf_meta=tf_meta,
        extra=extra,
    )


def rebuild_task_alias_index(entry: Dict[str, Any]) -> None:
    rebuild_task_alias_index_state(entry)


def assign_task_alias(
    entry: Dict[str, Any],
    task: Dict[str, Any],
    prompt: str,
    rebuild_index: bool = True,
) -> None:
    assign_task_alias_state(entry, task, prompt, rebuild_index=rebuild_index)


def backfill_task_aliases(entry: Dict[str, Any]) -> None:
    backfill_task_aliases_state(entry)


def resolve_task_request_id(entry: Dict[str, Any], request_or_alias: str) -> str:
    return resolve_task_request_id_state(entry, request_or_alias)


def latest_task_request_refs(entry: Dict[str, Any], limit: int = 12) -> List[str]:
    return latest_task_request_refs_state(entry, limit=limit)


def summarize_task_monitor(project_name: str, entry: Dict[str, Any], limit: int = 12) -> str:
    return summarize_task_monitor_state(
        project_name,
        entry,
        limit=limit,
        normalize_task_status=normalize_task_status,
        dedupe_roles=dedupe_roles,
        task_display_label=task_display_label,
        lifecycle_stages=LIFECYCLE_STAGES,
    )


def trim_project_tasks(tasks: Dict[str, Any], keep: int = DEFAULT_TASK_KEEP_PER_PROJECT) -> None:
    trim_project_tasks_state(tasks, keep=keep)


def get_task_record(entry: Dict[str, Any], request_id: str) -> Optional[Dict[str, Any]]:
    return get_task_record_state(entry, request_id)


def ensure_task_record(
    entry: Dict[str, Any],
    request_id: str,
    prompt: str,
    mode: str,
    roles: List[str],
    verifier_roles: List[str],
    require_verifier: bool,
) -> Dict[str, Any]:
    return ensure_task_record_state(
        entry,
        request_id=request_id,
        prompt=prompt,
        mode=mode,
        roles=roles,
        verifier_roles=verifier_roles,
        require_verifier=require_verifier,
        now_iso=now_iso,
        dedupe_roles=dedupe_roles,
        build_task_context=build_task_context,
        lifecycle_stages=LIFECYCLE_STAGES,
        keep_limit=DEFAULT_TASK_KEEP_PER_PROJECT,
    )


def lifecycle_set_stage(task: Dict[str, Any], stage: str, status: str, note: str = "") -> None:
    lifecycle_set_stage_state(
        task,
        stage=stage,
        status=status,
        note=note,
        lifecycle_stages=LIFECYCLE_STAGES,
        normalize_stage_status=normalize_stage_status,
        now_iso=now_iso,
        history_limit=DEFAULT_TASK_HISTORY_LIMIT,
    )


def sync_task_lifecycle(
    entry: Dict[str, Any],
    request_data: Dict[str, Any],
    prompt: str,
    mode: str,
    selected_roles: Optional[List[str]],
    verifier_roles: Optional[List[str]],
    require_verifier: bool,
    verifier_candidates: List[str],
) -> Optional[Dict[str, Any]]:
    return sync_task_lifecycle_state(
        entry,
        request_data,
        prompt=prompt,
        mode=mode,
        selected_roles=selected_roles,
        verifier_roles=verifier_roles,
        require_verifier=require_verifier,
        verifier_candidates=verifier_candidates,
        dedupe_roles=dedupe_roles,
        ensure_task_record=ensure_task_record,
        lifecycle_set_stage=lifecycle_set_stage,
        normalize_task_status=normalize_task_status,
        sync_task_exec_context=sync_task_exec_context,
    )


def summarize_task_lifecycle(project_name: str, task: Dict[str, Any]) -> str:
    return summarize_task_lifecycle_view(project_name, task)



def run_aoe_init(
    args: argparse.Namespace,
    project_root: Path,
    team_dir: Path,
    overview: str,
) -> str:
    cfg = team_dir / "orchestrator.json"
    if cfg.exists():
        return "[SKIP] already initialized (.aoe-team/orchestrator.json exists)"

    cmd = [
        args.aoe_orch_bin,
        "init",
        "--project-root",
        str(project_root),
        "--team-dir",
        str(team_dir),
        "--overview",
        overview,
    ]
    proc = run_command(cmd, env=None, timeout_sec=max(60, int(args.orch_command_timeout_sec)))
    text = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        low = text.lower()
        if "file exists" in low and "agents.md" in low:
            logs = repair_runtime(
                aoe_orch_bin=args.aoe_orch_bin,
                template_root=templates_root(),
                project_root=project_root,
                team_dir=team_dir,
                overview=overview,
                timeout_sec=max(60, int(args.orch_command_timeout_sec)),
                force=False,
            )
            return "\n".join(["[FALLBACK] runtime seeded without touching project-root AGENTS.md", *logs])
        raise RuntimeError(f"aoe-orch init failed: {text[:1200]}")
    return text or "[OK] initialized"


def run_aoe_spawn(args: argparse.Namespace, project_root: Path, team_dir: Path) -> str:
    cmd = [
        args.aoe_orch_bin,
        "spawn",
        "--project-root",
        str(project_root),
        "--team-dir",
        str(team_dir),
    ]
    proc = run_command(cmd, env=None, timeout_sec=max(60, int(args.orch_command_timeout_sec)))
    text = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        raise RuntimeError(f"aoe-orch spawn failed: {text[:1200]}")
    return text or "[OK] spawned"


def summarize_three_stage_request(
    project_name: str,
    request_data: Dict[str, Any],
    task: Optional[Dict[str, Any]] = None,
) -> str:
    request_id = str(request_data.get("request_id", "-")).strip() or "-"
    counts = request_data.get("counts") or {}
    assignments = int(counts.get("assignments", 0) or 0)
    replies = int(counts.get("replies", 0) or 0)
    complete = bool(request_data.get("complete", False))

    roles = request_data.get("roles") or []
    running: List[str] = []
    failed: List[str] = []
    done: List[str] = []

    for row in roles:
        role = str(row.get("role", "?")).strip() or "?"
        status = str(row.get("status", "?")).strip().lower()
        item = f"{role}({status})"
        if status in {"done"}:
            done.append(item)
        elif status in {"failed", "error", "fail"}:
            failed.append(item)
        else:
            running.append(item)

    stage1 = "완료" if assignments > 0 else "대기"
    if failed:
        stage2 = "이슈"
    elif running:
        stage2 = "진행중"
    elif assignments > 0:
        stage2 = "완료"
    else:
        stage2 = "대기"

    if complete and not failed:
        stage3 = "완료"
    elif replies > 0:
        stage3 = "부분완료"
    else:
        stage3 = "대기"

    lines = [
        f"orch: {project_name}",
        f"task: {task_display_label(task or {}, fallback_request_id=request_id)}",
        f"request_id: {request_id}",
        "3단계 진행확인",
        f"1) 접수/배정: {stage1} (assignments={assignments})",
        f"2) 실행: {stage2}" + (f" | running={', '.join(running)}" if running else ""),
        f"3) 완료/회신: {stage3} (replies={replies}, complete={'yes' if complete else 'no'})",
    ]

    if done:
        lines.append("done: " + ", ".join(done))
    if failed:
        lines.append("failed: " + ", ".join(failed))

    unresolved = request_data.get("unresolved_roles") or []
    if unresolved:
        lines.append("unresolved: " + ", ".join(str(x) for x in unresolved))

    return "\n".join(lines)



def summarize_orch_registry(state: Dict[str, Any]) -> str:
    return orch_registry_mod.summarize_orch_registry(
        state,
        ensure_project_aliases=ensure_project_aliases,
        project_alias_for_key=project_alias_for_key,
        project_lock_label=project_lock_label,
        extract_project_alias_index=extract_project_alias_index,
        bool_from_json=bool_from_json,
        task_display_label=task_display_label,
        normalize_task_status=normalize_task_status,
    )

def run_command(cmd: List[str], env: Optional[Dict[str, str]], timeout_sec: int) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        env=env,
        timeout=max(5, int(timeout_sec)),
    )


def choose_auto_dispatch_roles(
    prompt: str,
    available_roles: Optional[List[str]] = None,
    team_dir: Optional[Path] = None,
) -> List[str]:
    return orch_roles_mod.choose_auto_dispatch_roles(
        prompt,
        available_roles=available_roles,
        team_dir=team_dir,
    )


def run_codex_exec(args: argparse.Namespace, prompt: str, timeout_sec: int = 480) -> str:
    fd, out_path_raw = tempfile.mkstemp(prefix="aoe_tg_", suffix=".txt")
    os.close(fd)
    out_path = Path(out_path_raw)

    perm_mode = (os.environ.get("AOE_CODEX_PERMISSION_MODE", "full") or "full").strip().lower()
    run_as_root_raw = (os.environ.get("AOE_CODEX_RUN_AS_ROOT", "0") or "0").strip().lower()
    run_as_root = run_as_root_raw in {"1", "true", "yes", "on"}

    cmd = [
        "codex",
        "exec",
        "--skip-git-repo-check",
        "--disable",
        "multi_agent",
        "-C",
        str(args.project_root),
        "-o",
        str(out_path),
        prompt,
    ]

    if perm_mode in {"full", "unsafe", "bypass", "dangerous"}:
        cmd.extend(["--dangerously-bypass-approvals-and-sandbox"])
    elif perm_mode in {"danger", "danger-full-access"}:
        cmd.extend(["--sandbox", "danger-full-access"])
    elif perm_mode in {"workspace", "workspace-write", "safe", ""}:
        cmd.extend(["--sandbox", "workspace-write"])
    elif perm_mode in {"read-only", "readonly"}:
        cmd.extend(["--sandbox", "read-only"])
    else:
        cmd.extend(["--sandbox", "workspace-write"])

    root_output_mode = False
    if run_as_root:
        can_sudo = subprocess.run(
            ["sudo", "-n", "true"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode == 0
        if can_sudo:
            env_pairs: List[str] = []
            for k in [
                "HOME",
                "OPENAI_API_KEY",
                "OPENAI_BASE_URL",
                "OPENAI_ORG_ID",
                "OPENAI_PROJECT_ID",
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "NO_PROXY",
                "ALL_PROXY",
            ]:
                v = os.environ.get(k, "")
                if v:
                    env_pairs.append(f"{k}={v}")
            cmd = ["sudo", "-n", "env", *env_pairs, *cmd]
            root_output_mode = True

    try:
        if root_output_mode:
            # In sticky /tmp, sudo process may fail to overwrite pre-created user temp files.
            try:
                out_path.unlink(missing_ok=True)
            except Exception:
                pass
        proc = run_command(cmd, env=None, timeout_sec=timeout_sec)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            raise RuntimeError(f"codex exec failed: {detail[:1000]}")

        body = ""
        if out_path.exists():
            try:
                body = out_path.read_text(encoding="utf-8").strip()
            except Exception:
                body = ""

        if not body:
            body = (proc.stdout or "").strip()

        if not body:
            raise RuntimeError("codex exec returned empty output")

        return body
    finally:
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            pass


def run_claude_exec(args: argparse.Namespace, prompt: str, timeout_sec: int = 480) -> str:
    perm_mode = (os.environ.get("AOE_CLAUDE_PERMISSION_MODE", os.environ.get("AOE_CODEX_PERMISSION_MODE", "full")) or "full").strip().lower()
    run_as_root_raw = (os.environ.get("AOE_CLAUDE_RUN_AS_ROOT", os.environ.get("AOE_CODEX_RUN_AS_ROOT", "0")) or "0").strip().lower()
    run_as_root = run_as_root_raw in {"1", "true", "yes", "on"}

    cmd = [
        "claude",
        "-p",
        prompt,
        "--output-format",
        "text",
        "--add-dir",
        str(args.project_root),
        "--no-session-persistence",
    ]

    if perm_mode in {"full", "unsafe", "bypass", "dangerous", "danger", "danger-full-access"}:
        cmd.extend(["--dangerously-skip-permissions", "--permission-mode", "bypassPermissions"])
    elif perm_mode in {"workspace", "workspace-write", "safe", ""}:
        cmd.extend(["--permission-mode", "acceptEdits"])
    elif perm_mode in {"read-only", "readonly"}:
        cmd.extend(["--permission-mode", "plan"])
    elif perm_mode in {"auto", "default", "dontask", "dont-ask", "acceptedits", "bypasspermissions", "plan"}:
        mode_map = {
            "dontask": "dontAsk",
            "dont-ask": "dontAsk",
            "acceptedits": "acceptEdits",
            "bypasspermissions": "bypassPermissions",
        }
        cmd.extend(["--permission-mode", mode_map.get(perm_mode, perm_mode)])
    else:
        cmd.extend(["--dangerously-skip-permissions", "--permission-mode", "bypassPermissions"])

    if run_as_root:
        can_sudo = subprocess.run(
            ["sudo", "-n", "true"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode == 0
        if can_sudo:
            env_pairs: List[str] = []
            for k in [
                "HOME",
                "PATH",
                "ANTHROPIC_API_KEY",
                "ANTHROPIC_BASE_URL",
                "ANTHROPIC_AUTH_TOKEN",
                "CLAUDE_CODE_USE_BEDROCK",
                "CLAUDE_CODE_OAUTH_TOKEN",
                "CLAUDE_CONFIG_DIR",
                "AWS_REGION",
                "AWS_DEFAULT_REGION",
                "AWS_PROFILE",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
                "AWS_SESSION_TOKEN",
                "HTTP_PROXY",
                "HTTPS_PROXY",
                "NO_PROXY",
                "ALL_PROXY",
            ]:
                v = os.environ.get(k, "")
                if v:
                    env_pairs.append(f"{k}={v}")
            cmd = ["sudo", "-n", "env", *env_pairs, *cmd]

    proc = subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        cwd=str(args.project_root),
        timeout=max(5, int(timeout_sec)),
    )
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"claude exec failed: {detail[:1000]}")
    body = (proc.stdout or "").strip()
    if not body:
        raise RuntimeError("claude exec returned empty output")
    return body
def parse_json_object_from_text(text: str) -> Optional[Dict[str, Any]]:
    src = (text or "").strip()
    if not src:
        return None

    try:
        obj = json.loads(src)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass

    decoder = json.JSONDecoder()
    for i, ch in enumerate(src):
        if ch != "{":
            continue
        try:
            obj, _ = decoder.raw_decode(src[i:])
        except Exception:
            continue
        if isinstance(obj, dict):
            return obj

    return None


def available_worker_roles(available_roles: List[str]) -> List[str]:
    return orch_roles_mod.available_worker_roles(available_roles)


def normalize_task_plan_payload(
    parsed: Optional[Dict[str, Any]],
    user_prompt: str,
    workers: List[str],
    max_subtasks: int,
) -> Dict[str, Any]:
    return normalize_task_plan_schema(
        parsed,
        user_prompt=user_prompt,
        workers=workers,
        max_subtasks=max_subtasks,
    )


def critic_has_blockers(critic: Dict[str, Any]) -> bool:
    approved = bool(critic.get("approved", True))
    issues = critic.get("issues") or []
    return (not approved) or bool(issues)


def planning_stage_timeout_sec(args: argparse.Namespace, stage: str) -> int:
    stage_token = str(stage or "").strip().lower()
    env_map = {
        "planner": "AOE_PLAN_PLANNER_TIMEOUT_SEC",
        "critic": "AOE_PLAN_CRITIC_TIMEOUT_SEC",
        "repair": "AOE_PLAN_REPAIR_TIMEOUT_SEC",
    }
    default_caps = {
        "planner": 240,
        "critic": 180,
        "repair": 240,
    }
    min_floors = {
        "planner": 60,
        "critic": 45,
        "repair": 60,
    }
    try:
        base = int(getattr(args, "orch_command_timeout_sec", DEFAULT_ORCH_COMMAND_TIMEOUT_SEC) or DEFAULT_ORCH_COMMAND_TIMEOUT_SEC)
    except Exception:
        base = DEFAULT_ORCH_COMMAND_TIMEOUT_SEC
    cap = int(default_caps.get(stage_token, 180))
    floor = int(min_floors.get(stage_token, 60))

    raw_override = os.environ.get(env_map.get(stage_token, ""), "").strip()
    if raw_override:
        try:
            override = int(raw_override)
            return max(floor, min(override, max(base, floor)))
        except Exception:
            pass

    return max(floor, min(cap, max(base, floor)))


def build_task_execution_plan(
    args: argparse.Namespace,
    user_prompt: str,
    available_roles: List[str],
    max_subtasks: int,
) -> Dict[str, Any]:
    workers = available_worker_roles(available_roles)

    planner_prompt = (
        "너는 작업 오케스트레이션 planner다. 사용자 요청을 실행 가능한 sub-task 계획으로 분해해라.\n"
        "반드시 JSON 객체만 출력한다. 설명 문장 금지.\n"
        "JSON 스키마:\n"
        "{\n"
        "  \"summary\": \"한 줄 요약\",\n"
        "  \"subtasks\": [\n"
        "    {\"id\":\"S1\", \"title\":\"...\", \"goal\":\"...\", \"owner_role\":\"ROLE\", \"acceptance\":[\"...\"]}\n"
        "  ]\n"
        "}\n"
        "제약:\n"
        f"- owner_role은 다음 중 하나만 사용: {', '.join(workers)}\n"
        f"- subtasks는 1~{max(1, int(max_subtasks))}개\n"
        "- 각 subtask는 서로 다른 산출물을 갖도록 분해\n"
        "- acceptance는 검증 가능한 문장 1~3개\n\n"
        f"사용자 요청:\n{user_prompt.strip()}\n"
    )

    raw = run_codex_exec(args, planner_prompt, timeout_sec=planning_stage_timeout_sec(args, "planner"))
    parsed = parse_json_object_from_text(raw)
    return normalize_task_plan_payload(parsed, user_prompt=user_prompt, workers=workers, max_subtasks=max_subtasks)


def critique_task_execution_plan(
    args: argparse.Namespace,
    user_prompt: str,
    plan: Dict[str, Any],
) -> Dict[str, Any]:
    payload = json.dumps(plan, ensure_ascii=False)
    critic_prompt = (
        "너는 task plan critic이다. 아래 계획의 누락/과도분해/검증불가 항목을 점검해라.\n"
        "반드시 JSON 객체만 출력한다. 설명 문장 금지.\n"
        "JSON 스키마:\n"
        "{\n"
        "  \"approved\": true|false,\n"
        "  \"issues\": [\"...\"],\n"
        "  \"recommendations\": [\"...\"]\n"
        "}\n"
        "규칙:\n"
        "- issues는 치명/중요 문제만\n"
        "- recommendations는 실행 가능한 수정 제안만\n\n"
        f"사용자 요청:\n{user_prompt.strip()}\n\n"
        f"plan:\n{payload}\n"
    )

    try:
        raw = run_codex_exec(args, critic_prompt, timeout_sec=planning_stage_timeout_sec(args, "critic"))
        parsed = parse_json_object_from_text(raw)
    except Exception:
        parsed = None

    return normalize_plan_critic_payload(parsed, max_items=5)


def repair_task_execution_plan(
    args: argparse.Namespace,
    user_prompt: str,
    current_plan: Dict[str, Any],
    critic: Dict[str, Any],
    available_roles: List[str],
    max_subtasks: int,
    attempt_no: int,
) -> Dict[str, Any]:
    workers = available_worker_roles(available_roles)
    current_payload = json.dumps(current_plan, ensure_ascii=False)
    critic_payload = json.dumps(critic, ensure_ascii=False)

    repair_prompt = (
        "너는 task planner다. critic 이슈를 반영해 계획을 고쳐라.\n"
        "반드시 JSON 객체만 출력한다. 설명 문장 금지.\n"
        "JSON 스키마:\n"
        "{\n"
        "  \"summary\": \"한 줄 요약\",\n"
        "  \"subtasks\": [\n"
        "    {\"id\":\"S1\", \"title\":\"...\", \"goal\":\"...\", \"owner_role\":\"ROLE\", \"acceptance\":[\"...\"]}\n"
        "  ]\n"
        "}\n"
        "제약:\n"
        f"- owner_role은 다음 중 하나만 사용: {', '.join(workers)}\n"
        f"- subtasks는 1~{max(1, int(max_subtasks))}개\n"
        "- acceptance는 검증 가능한 문장 1~3개\n"
        "- critic issues를 가능한 한 모두 해소\n\n"
        f"attempt: {int(attempt_no)}\n"
        f"사용자 요청:\n{user_prompt.strip()}\n\n"
        f"current_plan:\n{current_payload}\n\n"
        f"critic:\n{critic_payload}\n"
    )

    raw = run_codex_exec(args, repair_prompt, timeout_sec=planning_stage_timeout_sec(args, "repair"))
    parsed = parse_json_object_from_text(raw)
    return normalize_task_plan_payload(parsed, user_prompt=user_prompt, workers=workers, max_subtasks=max_subtasks)


def plan_roles_from_subtasks(plan: Dict[str, Any]) -> List[str]:
    rows = plan.get("subtasks")
    roles: List[str] = []
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            role = str(row.get("owner_role", row.get("role", ""))).strip()
            if role:
                roles.append(role)
    return dedupe_roles(roles)


def build_planned_dispatch_prompt(
    user_prompt: str,
    plan: Dict[str, Any],
    critic: Dict[str, Any],
) -> str:
    subtasks = plan.get("subtasks") or []
    summary = str(plan.get("summary", "")).strip()
    meta = plan.get("meta") if isinstance(plan.get("meta"), dict) else {}
    team_spec = meta.get("phase2_team_spec") if isinstance(meta.get("phase2_team_spec"), dict) else {}

    lines: List[str] = []
    lines.append("원사용자 요청:")
    lines.append(user_prompt.strip())
    lines.append("")
    if summary:
        lines.append("계획 요약:")
        lines.append(summary)
        lines.append("")

    lines.append("실행할 sub-task:")
    for row in subtasks:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("id", "")).strip() or "S"
        title = str(row.get("title", "")).strip() or "subtask"
        goal = str(row.get("goal", "")).strip() or title
        role = str(row.get("owner_role", "")).strip() or "Worker"
        lines.append(f"- {sid} [{role}] {title}: {goal}")

    execution_groups = team_spec.get("execution_groups") if isinstance(team_spec.get("execution_groups"), list) else []
    review_groups = team_spec.get("review_groups") if isinstance(team_spec.get("review_groups"), list) else []
    if execution_groups:
        lines.append("")
        lines.append(
            "Phase2 execution lanes: {mode}".format(
                mode=str(team_spec.get("execution_mode", "single")).strip() or "single"
            )
        )
        for row in execution_groups[:8]:
            if not isinstance(row, dict):
                continue
            gid = str(row.get("group_id", "")).strip() or "E"
            role = str(row.get("role", "")).strip() or "Worker"
            subtask_ids = [str(item).strip() for item in (row.get("subtask_ids") or []) if str(item).strip()]
            lines.append(f"- lane {gid} [{role}] -> {', '.join(subtask_ids) if subtask_ids else '-'}")
    if review_groups:
        lines.append("")
        lines.append(
            "Phase2 critic lanes: {mode}".format(
                mode=str(team_spec.get("review_mode", "skip")).strip() or "skip"
            )
        )
        for row in review_groups[:6]:
            if not isinstance(row, dict):
                continue
            gid = str(row.get("group_id", "")).strip() or "R"
            role = str(row.get("role", "")).strip() or "Codex-Reviewer"
            kind = str(row.get("kind", "")).strip() or "verifier"
            depends_on = [str(item).strip() for item in (row.get("depends_on") or []) if str(item).strip()]
            dep_txt = f" after {', '.join(depends_on)}" if depends_on else ""
            lines.append(f"- review {gid} [{role}/{kind}]{dep_txt}")

    issues = critic.get("issues") or []
    recs = critic.get("recommendations") or []
    approved = not critic_has_blockers(critic)

    if not approved or issues or recs:
        lines.append("")
        lines.append("critic 체크:")
        if issues:
            for item in issues[:5]:
                lines.append(f"- issue: {str(item)}")
        if recs:
            for item in recs[:5]:
                lines.append(f"- fix: {str(item)}")

    lines.append("")
    lines.append("Phase2 실행 규칙:")
    lines.append("- 가능한 역할은 병렬로 동시에 진행한다.")
    lines.append("- critic/verifier 역할은 핵심 산출물에 대해 병렬로 비판 검토한다.")
    lines.append("- 실행 결과는 역할별 산출물 + 검증 근거 + 남은 리스크를 명확히 남긴다.")
    lines.append("")
    lines.append("위 계획과 체크사항을 반영해 역할별 실행/검증 결과를 산출해라.")
    return "\n".join(lines)


def run_phase1_ensemble_planning(
    args: argparse.Namespace,
    user_prompt: str,
    available_roles: List[str],
    report_progress: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    providers_csv = str(getattr(args, "plan_phase1_providers", "codex,claude") or "codex,claude")
    requested = []
    for token in providers_csv.split(","):
        item = str(token or "").strip().lower()
        if item and item not in requested:
            requested.append(item)
    if not requested:
        requested = ["codex", "claude"]

    runner_catalog: Dict[str, tuple[str, Callable[[str, int], str]]] = {
        "codex": ("codex", lambda prompt, timeout_sec: run_codex_exec(args, prompt, timeout_sec=timeout_sec)),
        "claude": ("claude", lambda prompt, timeout_sec: run_claude_exec(args, prompt, timeout_sec=timeout_sec)),
    }
    unsupported = [name for name in requested if name not in runner_catalog]
    if unsupported:
        detail = f"unsupported phase1 providers: {', '.join(unsupported)}"
        return {
            "plan_data": None,
            "plan_critic": default_plan_critic_payload(),
            "plan_roles": [],
            "plan_replans": [],
            "plan_error": detail,
            "plan_gate_blocked": True,
            "plan_gate_reason": detail,
            "phase1_rounds": 0,
            "phase1_mode": "ensemble",
            "phase1_providers": requested,
        }

    available_execs: Dict[str, Callable[[str, int], str]] = {}
    missing_binaries: List[str] = []
    for name in requested:
        binary, runner = runner_catalog[name]
        if shutil.which(binary):
            available_execs[name] = runner
        else:
            missing_binaries.append(binary)

    min_providers = max(1, int(getattr(args, "plan_phase1_min_providers", 2) or 2))
    if len(available_execs) < min_providers:
        detail = (
            f"phase1 ensemble requires at least {min_providers} providers; "
            f"available={','.join(sorted(available_execs)) or 'none'} "
            f"missing={','.join(missing_binaries) or 'none'}"
        )
        return {
            "plan_data": None,
            "plan_critic": default_plan_critic_payload(),
            "plan_roles": [],
            "plan_replans": [],
            "plan_error": detail,
            "plan_gate_blocked": True,
            "plan_gate_reason": detail,
            "phase1_rounds": 0,
            "phase1_mode": "ensemble",
            "phase1_providers": list(available_execs),
        }

    return plan_ensemble_mod.run_phase1_ensemble_planning(
        args=args,
        user_prompt=user_prompt,
        available_roles=available_roles,
        normalize_task_plan_payload=normalize_task_plan_payload,
        parse_json_object_from_text=parse_json_object_from_text,
        run_provider_execs=available_execs,
        plan_roles_from_subtasks=plan_roles_from_subtasks,
        report_progress=report_progress,
    )


def run_orchestrator_direct(args: argparse.Namespace, user_prompt: str, reply_lang: str = DEFAULT_REPLY_LANG) -> str:
    return orch_responses_mod.run_orchestrator_direct(
        args,
        user_prompt,
        reply_lang=reply_lang,
        default_reply_lang=DEFAULT_REPLY_LANG,
        normalize_chat_lang_token=normalize_chat_lang_token,
        run_codex_exec=run_codex_exec,
    )

def synthesize_orchestrator_response(
    args: argparse.Namespace,
    user_prompt: str,
    state: Dict[str, Any],
    reply_lang: str = DEFAULT_REPLY_LANG,
) -> str:
    return orch_responses_mod.synthesize_orchestrator_response(
        args,
        user_prompt,
        state,
        reply_lang=reply_lang,
        default_reply_lang=DEFAULT_REPLY_LANG,
        normalize_chat_lang_token=normalize_chat_lang_token,
        run_codex_exec=run_codex_exec,
    )


def critique_task_execution_result(
    args: argparse.Namespace,
    user_prompt: str,
    state: Dict[str, Any],
    task: Optional[Dict[str, Any]] = None,
    attempt_no: int = 1,
    max_attempts: int = 3,
    reply_lang: str = DEFAULT_REPLY_LANG,
) -> Dict[str, Any]:
    return orch_responses_mod.critique_task_execution_result(
        args,
        user_prompt,
        state,
        task=task,
        attempt_no=attempt_no,
        max_attempts=max_attempts,
        reply_lang=reply_lang,
        default_reply_lang=DEFAULT_REPLY_LANG,
        normalize_chat_lang_token=normalize_chat_lang_token,
        mask_sensitive_text=mask_sensitive_text,
        run_codex_exec=run_codex_exec,
        parse_json_object_from_text=parse_json_object_from_text,
        normalize_exec_critic_payload=normalize_exec_critic_payload,
        now_iso=now_iso,
    )


def extract_followup_todo_proposals(
    args: argparse.Namespace,
    user_prompt: str,
    state: Dict[str, Any],
    task: Optional[Dict[str, Any]] = None,
    reply_lang: str = DEFAULT_REPLY_LANG,
) -> List[Dict[str, Any]]:
    return orch_responses_mod.extract_followup_todo_proposals(
        args,
        user_prompt,
        state,
        task=task,
        reply_lang=reply_lang,
        default_reply_lang=DEFAULT_REPLY_LANG,
        default_orch_command_timeout_sec=DEFAULT_ORCH_COMMAND_TIMEOUT_SEC,
        normalize_chat_lang_token=normalize_chat_lang_token,
        mask_sensitive_text=mask_sensitive_text,
        run_codex_exec=run_codex_exec,
        parse_json_object_from_text=parse_json_object_from_text,
    )


def create_request_id() -> str:
    return tf_exec_mod.create_request_id()


def sanitize_fs_token(raw: str, fallback: str = "default") -> str:
    return tf_exec_mod.sanitize_fs_token(raw, fallback)


def tf_exec_map_path(team_dir: Path) -> Path:
    return tf_exec_mod.tf_exec_map_path(team_dir, DEFAULT_TF_EXEC_MAP_FILE)


def load_tf_exec_map(team_dir: Path) -> Dict[str, Any]:
    return tf_exec_mod.load_tf_exec_map(team_dir, DEFAULT_TF_EXEC_MAP_FILE)


def save_tf_exec_map(team_dir: Path, data: Dict[str, Any]) -> None:
    return tf_exec_mod.save_tf_exec_map(team_dir, data, DEFAULT_TF_EXEC_MAP_FILE)


def tf_worker_runner_path() -> Path:
    return tf_exec_mod.tf_worker_runner_path()


def tf_worker_session_name(request_id: str, role: str) -> str:
    return tf_exec_mod.tf_worker_session_name(
        request_id,
        role,
        default_prefix=DEFAULT_TF_WORKER_SESSION_PREFIX,
    )


def tf_worker_specs(
    args: argparse.Namespace,
    request_id: str,
    roles: List[str],
    startup_timeout_sec: int,
    lane_summary: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    args._aoe_default_tf_worker_session_prefix = DEFAULT_TF_WORKER_SESSION_PREFIX
    return tf_exec_mod.tf_worker_specs(args, request_id, roles, startup_timeout_sec, lane_summary=lane_summary)


def preview_tf_worker_sessions(
    args: argparse.Namespace,
    request_id: str,
    roles: List[str],
    startup_timeout_sec: int,
    lane_summary: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    args._aoe_default_tf_worker_session_prefix = DEFAULT_TF_WORKER_SESSION_PREFIX
    return tf_exec_mod.preview_tf_worker_sessions(args, request_id, roles, startup_timeout_sec, lane_summary=lane_summary)


def spawn_tf_worker_sessions(
    args: argparse.Namespace,
    request_id: str,
    roles: List[str],
    startup_timeout_sec: int,
    lane_summary: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    args._aoe_default_tf_worker_session_prefix = DEFAULT_TF_WORKER_SESSION_PREFIX
    return tf_exec_mod.spawn_tf_worker_sessions(
        args,
        request_id,
        roles,
        startup_timeout_sec,
        lane_summary=lane_summary,
        run_command=run_command,
    )


def cleanup_tf_worker_sessions(tf_entry: Dict[str, Any]) -> None:
    return tf_exec_mod.cleanup_tf_worker_sessions(tf_entry, run_command=run_command)


def resolve_dispatch_roles_from_preview(
    args: argparse.Namespace,
    prompt: str,
    request_id: str,
    roles_override: str,
    priority: str,
    timeout_sec: int,
) -> List[str]:
    return tf_exec_mod.resolve_dispatch_roles_from_preview(
        args,
        prompt,
        request_id,
        roles_override,
        priority,
        timeout_sec,
        run_command=run_command,
    )


def load_tf_exec_meta(team_dir: Path, request_id: str) -> Dict[str, Any]:
    return tf_exec_mod.load_tf_exec_meta(team_dir, request_id, DEFAULT_TF_EXEC_MAP_FILE)


def sync_task_exec_context(entry: Dict[str, Any], task: Dict[str, Any]) -> Dict[str, str]:
    return tf_exec_mod.sync_task_exec_context(
        entry,
        task,
        build_task_context=build_task_context,
        default_tf_exec_map_file=DEFAULT_TF_EXEC_MAP_FILE,
        now_iso=now_iso,
    )


def finalize_tf_exec_meta(team_dir: Path, request_id: str, state: Dict[str, Any]) -> None:
    return tf_exec_mod.finalize_tf_exec_meta(
        team_dir,
        request_id,
        state,
        default_tf_exec_map_file=DEFAULT_TF_EXEC_MAP_FILE,
        now_iso=now_iso,
    )


def tf_work_root(project_root: Path) -> Path:
    return tf_exec_mod.tf_work_root(project_root, DEFAULT_TF_WORK_ROOT_NAME)


def normalize_tf_exec_mode(raw: Optional[str]) -> str:
    return tf_exec_mod.normalize_tf_exec_mode(raw, DEFAULT_TF_EXEC_MODE)


def normalize_tf_exec_retention() -> str:
    return tf_exec_mod.normalize_tf_exec_retention()


def tf_exec_cache_ttl_hours() -> int:
    return tf_exec_mod.tf_exec_cache_ttl_hours(
        int_from_env=int_from_env,
        default_ttl_hours=DEFAULT_TF_EXEC_CACHE_TTL_HOURS,
    )


def is_git_repo(path: Path) -> bool:
    return tf_exec_mod.is_git_repo(path, run_command=run_command)


def git_worktree_add(repo_root: Path, workdir: Path, branch: str) -> Tuple[bool, str]:
    return tf_exec_mod.git_worktree_add(repo_root, workdir, branch, run_command=run_command)


def git_worktree_remove(repo_root: Path, workdir: Path) -> None:
    return tf_exec_mod.git_worktree_remove(repo_root, workdir, run_command=run_command)


def git_branch_delete(repo_root: Path, branch: str) -> None:
    return tf_exec_mod.git_branch_delete(repo_root, branch, run_command=run_command)


def ensure_tf_exec_workspace(
    args: argparse.Namespace,
    request_id: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return tf_exec_mod.ensure_tf_exec_workspace(
        args,
        request_id,
        metadata=metadata,
        default_tf_exec_mode=DEFAULT_TF_EXEC_MODE,
        default_tf_work_root_name=DEFAULT_TF_WORK_ROOT_NAME,
        default_tf_exec_map_file=DEFAULT_TF_EXEC_MAP_FILE,
        now_iso=now_iso,
        run_command=run_command,
    )


def _task_exec_verdict(task: Dict[str, Any]) -> str:
    return tf_exec_mod.task_exec_verdict(task)


def _is_task_success(task: Dict[str, Any]) -> bool:
    return tf_exec_mod.is_task_success(task)


def cleanup_tf_exec_entry(entry: Dict[str, Any]) -> None:
    return tf_exec_mod.cleanup_tf_exec_entry(entry, run_command=run_command)


def cleanup_tf_exec_artifacts(manager_state_path: Path, state: Dict[str, Any]) -> int:
    return tf_exec_mod.cleanup_tf_exec_artifacts(
        manager_state_path,
        state,
        default_tf_exec_map_file=DEFAULT_TF_EXEC_MAP_FILE,
        default_tf_exec_cache_ttl_hours=DEFAULT_TF_EXEC_CACHE_TTL_HOURS,
        now_iso=now_iso,
        parse_iso_ts=parse_iso_ts,
        int_from_env=int_from_env,
        run_command=run_command,
    )


def run_aoe_orch(
    args: argparse.Namespace,
    prompt: str,
    chat_id: str,
    roles_override: Optional[str] = None,
    priority_override: Optional[str] = None,
    timeout_override: Optional[int] = None,
    no_wait_override: Optional[bool] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    selection = tf_backend_selection_mod.resolve_effective_tf_backend(Path(str(args.team_dir)))
    backend_name = normalize_tf_backend_name(selection.get("effective_backend"), default=DEFAULT_TF_BACKEND)
    adapter = (
        tf_backend_autogen_mod.autogen_core_backend()
        if backend_name == AUTOGEN_CORE_TF_BACKEND
        else tf_backend_local_mod.local_backend()
    )
    available, availability_reason = availability_tuple(adapter.availability())
    if not available:
        config_path = str(selection.get("config_path", "") or "").strip()
        config_hint = f" config={config_path}" if config_path else ""
        raise RuntimeError(
            f"tf backend unavailable: backend={backend_name}"
            f" reason={availability_reason or 'unavailable'}"
            f" selection={selection.get('selection_reason', 'default_local')}{config_hint}"
        )

    request_metadata = {
        "backend": backend_name,
        "selection_reason": str(selection.get("selection_reason", "") or ""),
        "profile": str(selection.get("profile", "") or ""),
        "sandbox_only": bool(selection.get("sandbox_only", True)),
        "config_path": str(selection.get("config_path", "") or ""),
    }
    if isinstance(metadata, dict):
        for key, value in metadata.items():
            if not isinstance(key, str):
                continue
            request_metadata[str(key).strip()] = value

    request = build_tf_backend_request(
        args=args,
        prompt=prompt,
        chat_id=chat_id,
        roles_override=roles_override,
        priority_override=priority_override,
        timeout_override=timeout_override,
        no_wait_override=no_wait_override,
        metadata=request_metadata,
    )
    deps = build_tf_backend_deps(
        default_tf_exec_mode=DEFAULT_TF_EXEC_MODE,
        default_tf_work_root_name=DEFAULT_TF_WORK_ROOT_NAME,
        default_tf_exec_map_file=DEFAULT_TF_EXEC_MAP_FILE,
        default_tf_worker_startup_grace_sec=DEFAULT_TF_WORKER_STARTUP_GRACE_SEC,
        now_iso=now_iso,
        run_command=run_command,
    )
    result = adapter.run(request, deps)
    if not isinstance(result, dict):
        result = {"result": result}
    result = dict(result)
    result["backend"] = backend_name
    result["backend_profile"] = str(selection.get("profile", "") or "")
    result["backend_selection_reason"] = str(selection.get("selection_reason", "") or "")
    result["backend_config_path"] = str(selection.get("config_path", "") or "")
    result["backend_availability_reason"] = str(availability_reason or "")

    runtime_events = result.get("runtime_events")
    if not isinstance(runtime_events, list):
        runtime_events = result.get("events")
    if isinstance(runtime_events, list) and runtime_events:
        mirror_tf_backend_runtime_events(
            team_dir=Path(str(args.team_dir)),
            backend=backend_name,
            runtime_events=runtime_events,
            trace_id=str(getattr(args, "_aoe_trace_id", "") or ""),
            project=str(getattr(args, "_aoe_project_key", "") or ""),
            request_id=str(result.get("request_id", "") or ""),
            task=result.get("task") if isinstance(result.get("task"), dict) else None,
            mirror_team_dir=Path(str(getattr(args, "_aoe_root_team_dir", args.team_dir))),
        )
    return result


def run_aoe_add_role(
    args: argparse.Namespace,
    role: str,
    provider: Optional[str],
    launch: Optional[str],
    spawn: bool,
) -> str:
    cmd: List[str] = [
        args.aoe_orch_bin,
        "add-role",
        "--project-root",
        str(args.project_root),
        "--team-dir",
        str(args.team_dir),
        "--role",
        role,
        "--json",
    ]

    if provider:
        cmd.extend(["--provider", provider])
    if launch:
        cmd.extend(["--launch", launch])
    if spawn:
        cmd.append("--spawn")
    else:
        cmd.append("--no-spawn")

    proc = run_command(cmd, env=None, timeout_sec=60)
    payload = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        raise RuntimeError(f"aoe-orch add-role failed: {payload[:1200]}")

    try:
        data = json.loads(payload)
    except Exception:
        return payload or f"[OK] role added: {role}"

    if not isinstance(data, dict):
        return payload or f"[OK] role added: {role}"

    r = str(data.get("role", role))
    sess = str(data.get("session", ""))
    prov = str(data.get("provider", provider or "codex"))
    launch_used = str(data.get("launch", launch or ""))
    exists = bool(data.get("exists", False))
    updated = bool(data.get("updated", False))

    lines = [f"role ready: {r}", f"provider: {prov}"]
    if launch_used:
        lines.append(f"launch: {launch_used}")
    if sess:
        lines.append(f"session: {sess}")
    lines.append(f"exists_before: {'yes' if exists else 'no'}")
    lines.append(f"updated: {'yes' if updated else 'no'}")

    spawn_info = data.get("spawn_info") or {}
    spawned = spawn_info.get("spawned") or []
    existing_rows = spawn_info.get("existing") or []
    failed = spawn_info.get("failed") or []
    if spawned:
        lines.append(f"spawned: {len(spawned)}")
    if existing_rows:
        lines.append(f"already_running: {len(existing_rows)}")
    if failed:
        lines.append(f"spawn_failed: {len(failed)}")

    return "\n".join(lines)


def run_aoe_status(args: argparse.Namespace) -> str:
    return orch_registry_mod.run_aoe_status(
        args,
        run_command=run_command,
        summarize_gateway_poll_state=summarize_gateway_poll_state,
    )


def run_request_query(args: argparse.Namespace, request_id: str) -> Dict[str, Any]:
    return request_state_mod.run_request_query(args, request_id, run_command=run_command)


def run_message_fail(
    args: argparse.Namespace,
    message_id: str,
    actor: str,
    note: str,
) -> Tuple[bool, str]:
    return request_state_mod.run_message_fail(
        args,
        message_id,
        actor,
        note,
        run_command=run_command,
    )


def run_message_done(
    args: argparse.Namespace,
    message_id: str,
    actor: str,
    note: str,
) -> Tuple[bool, str]:
    return request_state_mod.run_message_done(
        args,
        message_id,
        actor,
        note,
        run_command=run_command,
    )


def finalize_request_reply_messages(
    args: argparse.Namespace,
    request_id: str,
    actor: str = "Orchestrator",
    note: str = "gateway integrated reply into final response",
) -> Dict[str, Any]:
    return request_state_mod.finalize_request_reply_messages(
        args,
        request_id,
        run_request_query=run_request_query,
        run_message_done=run_message_done,
        actor=actor,
        note=note,
    )


def cancel_request_assignments(
    args: argparse.Namespace,
    request_data: Dict[str, Any],
    note: str,
) -> Dict[str, Any]:
    return request_state_mod.cancel_request_assignments(
        args,
        request_data,
        note,
        run_message_fail=run_message_fail,
    )


def summarize_cancel_result(
    project_name: str,
    request_id: str,
    task: Optional[Dict[str, Any]],
    result: Dict[str, Any],
) -> str:
    return request_state_mod.summarize_cancel_result(
        project_name,
        request_id,
        task,
        result,
        task_display_label=task_display_label,
    )


def summarize_state(state: Dict[str, Any]) -> str:
    return request_state_mod.summarize_state(state)


def render_run_response(
    state: Dict[str, Any],
    task: Optional[Dict[str, Any]] = None,
    report_level: str = DEFAULT_REPORT_LEVEL,
) -> str:
    return request_state_mod.render_run_response(
        state,
        task=task,
        report_level=report_level,
        default_report_level=DEFAULT_REPORT_LEVEL,
        task_display_label=task_display_label,
        summarize_state=summarize_state,
    )



def summarize_request_state(state: Dict[str, Any], task: Optional[Dict[str, Any]] = None) -> str:
    return request_state_mod.summarize_request_state(
        state,
        task=task,
        task_display_label=task_display_label,
    )



def help_text(ui_lang: str = DEFAULT_UI_LANG) -> str:
    p = preferred_command_prefix()
    text = (
        "AOE Telegram Gateway commands\n"
        f"command prefix: {p}  (env: AOE_TG_COMMAND_PREFIXES; supports '/' and/or '!')\n"
        f"tip: unique abbreviations are accepted (ex: {p}st -> {p}status, {p}cle -> {p}clear)\n"
        "\n"
        "routine (copy/paste examples)\n"
        f"- {p}tutorial                  # quickstart guide\n"
        f"- {p}map                       # project map (O1..)\n"
        f"- {p}use O2                    # switch active project (soft focus)\n"
        f"- {p}focus O2                  # hard lock to one project\n"
        f"- {p}sync all 1h               # seed queue from scenario files; falls back to project todo docs if scenario is empty\n"
        f"- {p}sync                      # repeat last {p}sync args (chat-local)\n"
        f"- {p}queue                     # global todo queue\n"
        f"- {p}queue followup            # projects with manual follow-up backlog only\n"
        f"- {p}fanout                    # one todo per project wave\n"
        f"- {p}offdesk on                # after-work preset (auto fanout recent)\n"
        f"- {p}auto status               # scheduler status\n"
        f"- {p}panic                     # emergency stop (auto/offdesk off)\n"
        f"- {p}clear pending             # clear pending/confirm\n"
        f"- {p}room tail 20               # latest room events\n"
        "\n"
        "Quick mode (prefix-only default)\n"
        "- /status /check /task /monitor /kpi /map /help /tutorial\n"
        "- /queue  (global todo queue view)\n"
        "- /queue followup  (projects with manual_followup backlog only)\n"
        "- /sync [O#|name|all] [since 3h|1h]  (import <project_root>/.aoe-team/AOE_TODO.md into queue; if empty, fallback to todo-ish files/recent docs; empty args repeats last /sync)\n"
        "- /sync preview [replace] [O#|name|all] [since 3h|1h]  (show source files, source classes/confidence, and would-add/update/done/prune counts without changing queue; plain /sync fallback now bootstraps from recent md docs + salvage + todo files)\n"
        "- /sync bootstrap [O#|name|all] [since 24h]  (explicit bootstrap path: prefer recent docs + salvage when canonical backlog is missing, stale, or untrusted)\n"
        "- /sync recent [O#|name|all] [N] [since 3h]  (scan N recent todo-ish docs; default N=3)\n"
        "- /sync salvage [O#|name|all] [N] [since 3h]  (broader recent-doc salvage: recovers 'next steps/남은 일/follow-up' sections; loose follow-ups go to /todo proposals)\n"
        "- /sync files [O#|name|all] [N] [since 3h]  (scan todo-ish files by filename; default N=80)\n"
        "- /sync replace [O#|name]  (full-scope sync + cancel stale sync-managed open todos that no longer appear in source)\n"
        "- optional override: <project>/.aoe-team/sync_policy.json  (path globs / confidence / group tuning)\n"
        "- /next   (global todo scheduler)\n"
        "- /fanout (one todo per project wave)\n"
        "- /drain  (repeat /next N times)\n"
        "- /auto   (background /next loop via tmux scheduler; stops on confirm/stuck/too-many-failures)\n"
        "- /auto on fanout recent since 12h maxfail=3  (idle prefetch: /sync files all since 12h + /sync salvage all since 12h)\n"
        "- /auto on fanout recent replace-sync  (idle prefetch: /sync replace all quiet; full-scope, since ignored)\n"
        "- /offdesk [on|off|status|prepare|review]  (preset: report short + routing off + auto fanout recent; prepare = preflight, review = flagged-project drill-down)\n"
        "- /offdesk on replace-sync  (same preset, but idle prefetch uses /sync replace all quiet)\n"
        "- /panic  (emergency stop: auto/offdesk off + clear pending/confirm + routing off)\n"
        "- /clear  (clear pending/routing/room/queue; safe defaults)\n"
        "- /todo   (project backlog)\n"
        "- /todo proposals   (TF follow-up proposal inbox)\n"
        "- /todo followup   (manual follow-up backlog only)\n"
        "- /todo add [P1|P2|P3] <summary>\n"
        "- /todo accept <PROP-xxx|number>   (promote proposal into main todo queue)\n"
        "- /todo reject <PROP-xxx|number> [reason]   (discard proposal)\n"
        "- /todo ack <TODO-xxx|number>   (reopen blocked todo after manual review)\n"
        "- /todo ackrun <TODO-xxx|number>   (reopen blocked todo and dispatch it now)\n"
        "- /todo syncback [preview]   (write runtime done/blocked notes/new accepted items back to canonical TODO.md)\n"
        "- /todo done <TODO-xxx|number>\n"
        "- /todo next   (run next open todo)\n"
        "- /room   (ephemeral board: /room post|tail|list|use)\n"
        "- /gc     (cleanup room logs + tf exec cache)\n"
        "- /tf     (proof checks, local; writes report under docs/investigations_mo; ex: /tf mod2-proof tags | /tf mod2-proof latest)\n"
        "- /use <O1|name> (active orch switch; soft focus)\n"
        "- /focus [O1|name|off] (hard project lock / unlock)\n"
        "- /orch pause <O#|name> [reason]\n"
        "- /orch resume <O#|name>\n"
        "- /orch hide <O#|name> [reason]\n"
        "- /orch unhide <O#|name>\n"
        "- /mode [on|off|direct]\n"
        "- /on /off\n"
        "- /lang [ko|en]\n"
        "- /report [short|normal|long|off]\n"
        "- /replay [list|latest|<idx>|<id>|show <idx|id|latest>|purge]\n"
        "- /ok (고위험 자동실행 확인)\n"
        "- /whoami /lockme /onlyme\n"
        "- /acl /grant /revoke\n"
        "- /pick [번호|task_label]   (빈칸이면 최근 목록)\n"
        "- /dispatch <요청>   (서브에이전트 배정)\n"
        "- /direct <질문>     (오케스트레이터 직접 답변)\n"
        "- /dispatch 또는 /direct만 입력하면 다음 메시지 1회 모드\n"
        "- /cancel (대기 모드 해제)\n"
        "\n"
        "Slash mode\n"
        "- /help\n"
        "- /status\n"
        "- /mode [on|off|direct|dispatch]\n"
        "- /lang [ko|en]\n"
        "- /report [short|normal|long|off]\n"
        "- /on /off\n"
        "- /replay [list|latest|<idx>|<id>|show <idx|id|latest>|purge]\n"
        "- /ok\n"
        "- /onlyme   # 1:1 owner-only claim (lock + owner_only)\n"
        "- /acl\n"
        "- /grant <allow|admin|readonly> <chat_id|alias>\n"
        "- /revoke <allow|admin|readonly|all> <chat_id|alias>\n"
        "- /kpi [hours]\n"
        "- /map\n"
        "- /use <O1|name>          # active project switch (soft focus)\n"
        "- /focus [O1|name|off]    # hard lock one project / unlock\n"
        "- 단일 프로젝트 권장 흐름: /map -> /use O# -> /focus O# -> 평문 또는 /sync O# -> /next\n"
        "- /use 후에는 평문/TF가 해당 프로젝트를 기본 타겟으로 사용\n"
        "- /focus 후에는 /queue, /next, /sync all, /offdesk가 해당 프로젝트에 맞게 축소되고 /fanout은 차단됨\n"
        "- /queue\n"
        "- /sync [all|O#|name]\n"
        "- /sync preview [replace] [all|O#|name] [since 3h|1h]\n"
        "- /sync recent [O#|name|all] [N]\n"
        "- /sync salvage [O#|name|all] [N]\n"
        "- /sync files [O#|name|all] [N]\n"
        "- /sync replace [O#|name]\n"
        "- optional: <project>/.aoe-team/sync_policy.json\n"
        "- /next                   # active project 우선 단일 실행\n"
        "- /fanout [N] [force]     # global wave, 프로젝트별 1개씩\n"
        "- /drain [N] [force]\n"
        "- /auto [on|off|status [short|long]]\n"
        "- /auto on fanout recent since 12h maxfail=3\n"
        "- /auto on fanout recent replace-sync\n"
        "- /offdesk [on|off|status [short|long]|prepare|review]\n"
        "- /offdesk on replace-sync\n"
        "- /panic [status]\n"
        "- /clear [pending|routing|room|queue]\n"
        "- /todo\n"
        "- /todo proposals\n"
        "- /todo add [P1|P2|P3] <summary>\n"
        "- /todo accept <PROP-xxx|number>\n"
        "- /todo reject <PROP-xxx|number> [reason]\n"
        "- /todo ack <TODO-xxx|number>\n"
        "- /todo ackrun <TODO-xxx|number>\n"
        "- /todo syncback [preview]\n"
        "- /todo done <TODO-xxx|number>\n"
        "- /todo next\n"
        "- /tf [list|<recipe> [tag]]\n"
        "- /room [list|use|post|tail]\n"
        "- /gc [force]\n"
        "- /orch pause <O#|name> [reason]\n"
        "- /orch resume <O#|name>\n"
        "- /orch hide <O#|name> [reason]\n"
        "- /orch unhide <O#|name>\n"
        "- /orch repair [all|O#|name]\n"
        "- /pick [number|request_or_alias]  # empty shows recent menu\n"
        "- /cancel [request_or_alias]\n"
        "- /retry <request_or_alias> [lane <L#|R#,...>]\n"
        "- /replan <request_or_alias> [lane <L#|R#,...>]\n"
        "- /followup <request_or_alias> [lane <L#|R#,...>]\n"
        "- /request <request_or_alias>\n"
        "- /run <prompt>\n"
        "- /add-role <Role|--name Name> [--provider <name>] [--launch <cmd>] [--spawn|--no-spawn]\n"
        "- /add-claude <Role|--name Name> [--launch <cmd>] [--spawn|--no-spawn]\n"
        "- /add-codex <Role|--name Name> [--launch <cmd>] [--spawn|--no-spawn]\n"
        "\n"
        "CLI mode\n"
        "- aoe status\n"
        "- aoe mode [on|off|direct|dispatch]\n"
        "- aoe lang [ko|en]\n"
        "- aoe report [short|normal|long|off]\n"
        "- aoe on | aoe off\n"
        "- aoe replay [list|latest|<idx>|<id>|show <idx|id|latest>|purge]\n"
        "- aoe ok\n"
        "- aoe acl\n"
        "- aoe grant <allow|admin|readonly> <chat_id|alias>\n"
        "- aoe revoke <allow|admin|readonly|all> <chat_id|alias>\n"
        "- aoe kpi [hours]\n"
        "- aoe map\n"
        "- aoe orch use <name>     # set active project (soft focus)\n"
        "- aoe focus [O#|name|off]\n"
        "- aoe unlock\n"
        "- aoe queue\n"
        "- aoe drain [N] [force]\n"
        "- aoe fanout [N] [force]  # global wave\n"
        "- aoe auto [on|off|status]\n"
        "- aoe offdesk [on|off|status]\n"
        "- aoe panic [status]\n"
        "- aoe monitor [limit]\n"
        "- aoe next                # active project 우선 단일 실행\n"
        "- aoe todo [add|done|next] ...\n"
        "- aoe room [list|use|post|tail] ...\n"
        "- aoe gc [force]\n"
        "- aoe pick <number|request_or_alias>\n"
        "- aoe cancel [request_or_alias]\n"
        "- aoe retry <request_or_alias> [lane <L#|R#,...>]\n"
        "- aoe replan <request_or_alias> [lane <L#|R#,...>]\n"
        "- aoe followup <request_or_alias> [lane <L#|R#,...>]\n"
        "- aoe request <request_or_alias>\n"
        "- aoe run [--direct|--dispatch] [--roles <csv>] [--priority P1|P2|P3] [--timeout-sec N] [--no-wait] <prompt>\n"
        "- aoe add-role <Role|--name Name> [--provider <name>] [--launch <cmd>] [--spawn|--no-spawn]\n"
        "- aoe add-claude <Role|--name Name> [--launch <cmd>] [--spawn|--no-spawn]\n"
        "- aoe add-codex <Role|--name Name> [--launch <cmd>] [--spawn|--no-spawn]\n"
        "\n"
        "Orch Manager\n"
        "- aoe orch list (or: aoe orch map)\n"
        "- aoe orch use <name>\n"
        "- aoe orch add <name> --path <project_root> [--overview <text>] [--init|--no-init] [--spawn|--no-spawn]\n"
        "- aoe orch repair [all|--orch <name>]\n"
        "- aoe orch pause <name> [reason]\n"
        "- aoe orch resume <name>\n"
        "- aoe orch hide <name> [reason]\n"
        "- aoe orch unhide <name>\n"
        "- aoe orch status [--orch <name>]\n"
        "- aoe orch kpi [--orch <name>] [--hours <n>]\n"
        "- aoe orch monitor [--orch <name>] [--limit <n>]\n"
        "- aoe orch run [--orch <name>] [--direct|--dispatch] [--roles <csv>] [--priority P1|P2|P3] [--timeout-sec N] [--no-wait] <prompt>\n"
        "- aoe orch check [--orch <name>] [<request_or_alias>]   # 3단계 진행확인\n"
        "- aoe orch task [--orch <name>] [<request_or_alias>]    # lifecycle 상태\n"
        "- aoe orch pick [--orch <name>] <number|request_or_alias>\n"
        "- aoe orch cancel [--orch <name>] [<request_or_alias>]\n"
        "- aoe orch retry [--orch <name>] <request_or_alias>\n"
        "- aoe orch replan [--orch <name>] <request_or_alias>\n"
        "\n"
        "Routing\n"
        "- default: prefix-only (plain text ignored unless pending/default mode)\n"
        "- soft focus: /use <O#|name> sets the default project used by plain text and TF commands\n"
        "- hard lock: /focus <O#|name> narrows /queue, /next, /sync all, /offdesk to one project and blocks /fanout\n"
        "- unlock: /focus off (or /unlock)\n"
        "- default access: deny-by-default (allowlist required)\n"
        "- bootstrap: when allowlist is empty, only /lockme|/whoami|/help is accepted\n"
        "- owner-only: /onlyme locks to current chat and enables private-DM owner gate\n"
        "- owner gate: /lockme /grant /revoke are owner-only when TELEGRAM_OWNER_CHAT_ID is set\n"
        "- dispatch only when explicit (--dispatch or --roles)\n"
        "- auto dispatch: disabled by default (enable with --auto-dispatch)\n"
        "- force dispatch: --dispatch\n"
        "- force direct: --direct\n"
        "- slash-only default: enabled (disable with --no-slash-only)\n"
        "- verifier gate: on by default (disable with --no-require-verifier)\n"
        "- task planning: on by default (disable with --no-task-planning)\n"
        "- planning gate: auto-replan + block on critic issues by default\n"
    )
    if p != "/":
        # Replace "/cmd" tokens while avoiding URL-like `http://...`.
        import re as _re

        text = _re.sub(r"(?<!:)/(\\w)", f"{p}\\1", text)

    lang = normalize_chat_lang_token(ui_lang, DEFAULT_UI_LANG) or DEFAULT_UI_LANG
    if lang != "en":
        return text
    return (
        text
        .replace("고위험 자동실행 확인", "confirm high-risk auto execution")
        .replace("서브에이전트 배정", "sub-agent assignment")
        .replace("오케스트레이터 직접 답변", "orchestrator direct reply")
        .replace("다음 메시지 1회 모드", "one-shot next-message mode")
        .replace("대기 모드 해제", "clear pending mode")
        .replace("3단계 진행확인", "3-stage progress")
        .replace("lifecycle 상태", "lifecycle status")
    )


def is_bootstrap_allowed_command(text: str) -> bool:
    cmd, _ = parse_command(text)
    return cmd in {"start", "help", "tutorial", "id", "whoami", "lockme", "onlyme"}


def is_owner_chat(chat_id: str, args: argparse.Namespace) -> bool:
    owner = normalize_owner_chat_id(getattr(args, "owner_chat_id", ""))
    return bool(owner) and (str(chat_id).strip() == owner)


def resolve_chat_role(chat_id: str, args: argparse.Namespace) -> str:
    if is_owner_chat(chat_id, args):
        return "owner"
    return resolve_role_from_acl_sets(
        chat_id=chat_id,
        allow_chat_ids=args.allow_chat_ids,
        admin_chat_ids=args.admin_chat_ids,
        readonly_chat_ids=args.readonly_chat_ids,
        deny_by_default=bool(args.deny_by_default),
    )


def _parse_drain_args(rest: str) -> tuple[int, bool]:
    return gateway_batch_ops_mod.parse_drain_args(rest)


def _parse_fanout_args(rest: str) -> tuple[int, bool]:
    return gateway_batch_ops_mod.parse_fanout_args(rest)


def _drain_peek_next_todo(
    manager_state: Dict[str, Any],
    chat_id: str,
    *,
    force: bool,
) -> tuple[str, str, str]:
    return gateway_batch_ops_mod.drain_peek_next_todo(manager_state, chat_id, force=force)


def handle_drain_command(
    *,
    args: argparse.Namespace,
    token: str,
    chat_id: str,
    rest: str,
    trace_id: str,
    send: Callable[..., bool],
    log_event: Callable[..., None],
) -> None:
    return gateway_batch_ops_mod.handle_drain_command(
        args=args,
        token=token,
        chat_id=chat_id,
        rest=rest,
        trace_id=trace_id,
        send=send,
        log_event=log_event,
        deps=globals(),
    )


def handle_fanout_command(
    *,
    args: argparse.Namespace,
    token: str,
    chat_id: str,
    rest: str,
    trace_id: str,
    send: Callable[..., bool],
    log_event: Callable[..., None],
) -> None:
    return gateway_batch_ops_mod.handle_fanout_command(
        args=args,
        token=token,
        chat_id=chat_id,
        rest=rest,
        trace_id=trace_id,
        send=send,
        log_event=log_event,
        deps=globals(),
    )


def handle_gc_command(
    *,
    args: argparse.Namespace,
    chat_id: str,
    rest: str,
    manager_state: Dict[str, Any],
    send: Callable[..., bool],
    log_event: Callable[..., None],
) -> None:
    return gateway_batch_ops_mod.handle_gc_command(
        args=args,
        chat_id=chat_id,
        rest=rest,
        manager_state=manager_state,
        send=send,
        log_event=log_event,
        deps=globals(),
    )


def handle_text_message(
    args: argparse.Namespace,
    token: str,
    chat_id: str,
    text: str,
    trace_id: str = "",
) -> None:
    return message_handler_mod.handle_text_message(
        args,
        token,
        chat_id,
        text,
        trace_id=trace_id,
        deps=globals(),
    )


def iter_message_updates(updates: Iterable[Dict[str, Any]]) -> Iterable[Tuple[int, Dict[str, Any]]]:
    yield from poll_loop_mod.iter_message_updates(updates)


def run_simulation(args: argparse.Namespace, token: str) -> None:
    return poll_loop_mod.run_simulation(
        args,
        token,
        handle_text_message=handle_text_message,
    )


def run_loop(args: argparse.Namespace, token: str) -> int:
    return poll_loop_mod.run_loop(
        args,
        token,
        load_state=load_state,
        save_state=save_state,
        dedup_keep_limit=dedup_keep_limit,
        normalize_recent_tokens=normalize_recent_tokens,
        message_dedup_key=message_dedup_key,
        append_recent_token=append_recent_token,
        tg_get_updates=tg_get_updates,
        ensure_chat_allowed=ensure_chat_allowed,
        is_bootstrap_allowed_command=is_bootstrap_allowed_command,
        safe_tg_send_text=safe_tg_send_text,
        log_gateway_event=log_gateway_event,
        handle_text_message=handle_text_message,
        preferred_command_prefix=preferred_command_prefix,
        state_acked_updates_key=STATE_ACKED_UPDATES_KEY,
        state_handled_messages_key=STATE_HANDLED_MESSAGES_KEY,
        state_duplicate_skipped_key=STATE_DUPLICATE_SKIPPED_KEY,
        state_empty_skipped_key=STATE_EMPTY_SKIPPED_KEY,
        state_unauthorized_skipped_key=STATE_UNAUTHORIZED_SKIPPED_KEY,
        state_handler_errors_key=STATE_HANDLER_ERRORS_KEY,
        error_auth=ERROR_AUTH,
    )


def build_parser() -> argparse.ArgumentParser:
    return cli_mod.build_parser(deps=globals())


def main() -> int:
    return cli_mod.main(deps=globals())


def shutil_which(binary: str) -> Optional[str]:
    return cli_mod.shutil_which(binary)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
    except BrokenPipeError:
        raise SystemExit(0)
