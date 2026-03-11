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
    task_identifiers as gateway_task_identifiers,
)
import aoe_tg_gateway_state as gateway_state_mod
import aoe_tg_tf_exec as tf_exec_mod
from aoe_tg_message_flow import (
    RunTransitionState,
    apply_confirm_transition_to_resolved,
    apply_retry_transition_to_resolved,
    enforce_command_auth,
)
from aoe_tg_queue_engine import drain_peek_next_todo as queue_drain_peek_next_todo
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
    build_batch_finish_message,
    format_ops_skip_detail,
    new_ops_skip_counters,
    project_queue_snapshot,
    visible_ops_project_keys,
)
from aoe_tg_package_paths import templates_root, worker_handler_script
from aoe_tg_project_runtime import project_hidden_from_ops, project_runtime_issue, project_runtime_label
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
from aoe_tg_room_handlers import DEFAULT_MAX_EVENT_CHARS, DEFAULT_MAX_FILE_BYTES, DEFAULT_ROOM_NAME, append_room_event, normalize_room_token
from aoe_tg_schema import (
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
DEFAULT_VERIFIER_ROLES = "Reviewer,QA,Verifier"
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
    src = (name or "").strip().lower()
    out = []
    for ch in src:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out.append(ch)
        else:
            out.append("_")
    token = "".join(out).strip("._-")
    return token or "default"


def normalize_project_alias(token: str, max_alias: int = DEFAULT_PROJECT_ALIAS_MAX) -> str:
    raw = str(token or "").strip().upper()
    if not raw:
        return ""
    if raw.startswith("O"):
        body = raw[1:]
    else:
        body = raw
    if not body.isdigit():
        return ""
    idx = int(body)
    if idx < 1 or idx > int(max_alias):
        return ""
    return f"O{idx}"


def extract_project_alias_index(alias: str) -> int:
    token = normalize_project_alias(alias)
    if not token:
        return 10**9
    return int(token[1:])


def ensure_project_aliases(state: Dict[str, Any], max_alias: int = DEFAULT_PROJECT_ALIAS_MAX) -> Dict[str, str]:
    projects = state.get("projects") or {}
    if not isinstance(projects, dict):
        return {}

    alias_to_key: Dict[str, str] = {}
    used: Set[int] = set()

    # Two-pass assignment to keep aliases stable:
    # 1) Reserve all valid existing aliases first (so new projects never "steal" O2).
    # 2) Assign smallest unused aliases to the remaining projects.
    needs_assign: List[str] = []

    for key in sorted(projects.keys()):
        entry = projects.get(key)
        if not isinstance(entry, dict):
            continue
        alias = normalize_project_alias(str(entry.get("project_alias", "")), max_alias=max_alias)
        idx = extract_project_alias_index(alias)
        if alias and idx not in used:
            entry["project_alias"] = alias
            alias_to_key[alias] = key
            used.add(idx)
            continue

        entry["project_alias"] = ""
        needs_assign.append(key)

    for key in needs_assign:
        entry = projects.get(key)
        if not isinstance(entry, dict):
            continue
        pick = ""
        for cand in range(1, int(max_alias) + 1):
            if cand not in used:
                pick = f"O{cand}"
                used.add(cand)
                break
        if pick:
            entry["project_alias"] = pick
            alias_to_key[pick] = key
        else:
            entry["project_alias"] = ""

    return alias_to_key


def project_alias_for_key(state: Dict[str, Any], project_key: str) -> str:
    projects = state.get("projects") or {}
    if not isinstance(projects, dict):
        return ""
    entry = projects.get(str(project_key or ""))
    if not isinstance(entry, dict):
        return ""
    alias = normalize_project_alias(str(entry.get("project_alias", "")))
    if alias:
        return alias
    ensure_project_aliases(state)
    entry = projects.get(str(project_key or ""))
    if not isinstance(entry, dict):
        return ""
    return normalize_project_alias(str(entry.get("project_alias", "")))


def sanitize_project_lock_row(raw: Any, projects: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    if not isinstance(projects, dict):
        return {}

    enabled = bool_from_json(raw.get("enabled"), False)
    key = normalize_project_name(str(raw.get("project_key", raw.get("key", ""))))
    if (not enabled) or (not key):
        return {}

    entry = projects.get(key)
    if not isinstance(entry, dict):
        return {}

    out: Dict[str, Any] = {
        "enabled": True,
        "project_key": key,
    }
    locked_at = str(raw.get("locked_at", "")).strip()
    locked_by = str(raw.get("locked_by", "")).strip()
    if locked_at:
        out["locked_at"] = locked_at[:40]
    if locked_by:
        out["locked_by"] = locked_by[:120]
    return out


def get_project_lock_row(state: Dict[str, Any]) -> Dict[str, Any]:
    projects = state.get("projects")
    row = sanitize_project_lock_row(state.get("project_lock"), projects)
    if row:
        state["project_lock"] = row
        return row
    state.pop("project_lock", None)
    return {}


def get_project_lock_key(state: Dict[str, Any]) -> str:
    row = get_project_lock_row(state)
    return str(row.get("project_key", "")).strip()


def set_project_lock(state: Dict[str, Any], project_key: str, actor: str = "") -> Dict[str, Any]:
    key = normalize_project_name(str(project_key or ""))
    projects = state.get("projects")
    if not isinstance(projects, dict) or not isinstance(projects.get(key), dict):
        raise RuntimeError(f"unknown orch project for lock: {project_key}")
    row: Dict[str, Any] = {
        "enabled": True,
        "project_key": key,
        "locked_at": now_iso(),
    }
    actor_token = str(actor or "").strip()
    if actor_token:
        row["locked_by"] = actor_token[:120]
    state["project_lock"] = row
    state["active"] = key
    return row


def clear_project_lock(state: Dict[str, Any]) -> bool:
    existed = bool(get_project_lock_row(state))
    state.pop("project_lock", None)
    return existed


def project_lock_label(state: Dict[str, Any]) -> str:
    key = get_project_lock_key(state)
    if not key:
        return ""
    alias = project_alias_for_key(state, key) or "-"
    return f"{alias} ({key})"


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
    task = dict(raw_task or {})
    rid = str(req_id or task.get("request_id", "")).strip()
    task["request_id"] = rid
    task["mode"] = str(task.get("mode", "dispatch")).strip().lower() or "dispatch"
    if task["mode"] not in {"dispatch", "direct"}:
        task["mode"] = "dispatch"
    task["prompt"] = str(task.get("prompt", "")).strip()
    task["roles"] = dedupe_roles(task.get("roles") or [])
    task["verifier_roles"] = dedupe_roles(task.get("verifier_roles") or [])
    task["require_verifier"] = bool(task.get("require_verifier", False))

    raw_stages = task.get("stages")
    stages: Dict[str, str] = {}
    if isinstance(raw_stages, dict):
        for stage_name in LIFECYCLE_STAGES:
            stages[stage_name] = normalize_stage_status(raw_stages.get(stage_name, "pending"))
    else:
        for stage_name in LIFECYCLE_STAGES:
            stages[stage_name] = "pending"
    task["stages"] = stages

    stage = str(task.get("stage", "")).strip().lower()
    if stage not in LIFECYCLE_STAGES:
        stage = "intake"
        for stage_name in LIFECYCLE_STAGES:
            if stages.get(stage_name) in {"running", "done", "failed"}:
                stage = stage_name
    task["stage"] = stage

    history_in = task.get("history")
    history: List[Dict[str, Any]] = []
    if isinstance(history_in, list):
        for item in history_in[-DEFAULT_TASK_HISTORY_LIMIT:]:
            if not isinstance(item, dict):
                continue
            row_stage = str(item.get("stage", "")).strip().lower()
            if row_stage not in LIFECYCLE_STAGES:
                continue
            row_status = normalize_stage_status(item.get("status", "pending"))
            row: Dict[str, Any] = {
                "at": str(item.get("at", "")).strip() or now_iso(),
                "stage": row_stage,
                "status": row_status,
            }
            note = str(item.get("note", "")).strip()
            if note:
                row["note"] = note[:400]
            history.append(row)
    task["history"] = history

    task["status"] = normalize_task_status(task.get("status", "pending"))
    task["created_at"] = str(task.get("created_at", "")).strip() or now_iso()
    task["updated_at"] = str(task.get("updated_at", "")).strip() or now_iso()
    result = task.get("result")
    task["result"] = result if isinstance(result, dict) else {}

    short_id = str(task.get("short_id", "")).strip().upper()
    alias = str(task.get("alias", "")).strip()
    if short_id:
        task["short_id"] = short_id
    if alias:
        task["alias"] = alias

    control_mode = str(task.get("control_mode", "")).strip().lower()
    if control_mode:
        task["control_mode"] = control_mode[:32]
    source_request_id = str(task.get("source_request_id", "")).strip()
    if source_request_id:
        task["source_request_id"] = source_request_id[:128]
    retry_of = str(task.get("retry_of", "")).strip()
    if retry_of:
        task["retry_of"] = retry_of[:128]
    replan_of = str(task.get("replan_of", "")).strip()
    if replan_of:
        task["replan_of"] = replan_of[:128]

    for child_key in ("retry_children", "replan_children"):
        raw_children = task.get(child_key)
        if isinstance(raw_children, list):
            normalized_children = []
            seen_children: Set[str] = set()
            for item in raw_children:
                token = str(item or "").strip()
                if not token or token in seen_children:
                    continue
                seen_children.add(token)
                normalized_children.append(token[:128])
            if normalized_children:
                task[child_key] = normalized_children

    initiator_chat_id = str(task.get("initiator_chat_id", "")).strip()
    if initiator_chat_id:
        task["initiator_chat_id"] = initiator_chat_id[:64]
    todo_id = str(task.get("todo_id", "")).strip()
    if todo_id:
        task["todo_id"] = todo_id[:64]

    todo_priority = str(task.get("todo_priority", "")).strip().upper()
    if todo_priority in {"P1", "P2", "P3"}:
        task["todo_priority"] = todo_priority
    todo_status = str(task.get("todo_status", "")).strip().lower()
    if todo_status:
        task["todo_status"] = todo_status[:32]

    plan = task.get("plan")
    if isinstance(plan, dict):
        workers = []
        raw_meta = plan.get("meta")
        if isinstance(raw_meta, dict) and isinstance(raw_meta.get("worker_roles"), list):
            for row in raw_meta.get("worker_roles") or []:
                token = str(row or "").strip()
                if token and token not in workers:
                    workers.append(token)
        if not workers:
            workers = dedupe_roles((task.get("plan_roles") or []) + (task.get("roles") or [])) or ["Worker"]
        max_subtasks = 0
        raw_subtasks = plan.get("subtasks")
        if isinstance(raw_subtasks, list):
            max_subtasks = len(raw_subtasks)
        task["plan"] = normalize_task_plan_schema(
            plan,
            user_prompt=str(task.get("prompt", "")).strip(),
            workers=workers,
            max_subtasks=max_subtasks or 4,
        )
    plan_critic = task.get("plan_critic")
    if isinstance(plan_critic, dict):
        task["plan_critic"] = normalize_plan_critic_payload(plan_critic, max_items=8)
    plan_roles = task.get("plan_roles")
    if isinstance(plan_roles, list):
        task["plan_roles"] = dedupe_roles(plan_roles)
    plan_replans = task.get("plan_replans")
    if isinstance(plan_replans, list):
        task["plan_replans"] = normalize_plan_replans_payload(plan_replans, keep=DEFAULT_TASK_HISTORY_LIMIT)
    if isinstance(task.get("plan_gate_passed"), bool):
        task["plan_gate_passed"] = bool(task.get("plan_gate_passed"))
    plan_gate_reason = str(task.get("plan_gate_reason", "")).strip()
    if plan_gate_reason:
        task["plan_gate_reason"] = plan_gate_reason[:240]
    elif task.get("plan_gate_passed") is False and isinstance(task.get("plan_critic"), dict):
        lead_issue = plan_critic_primary_issue(task["plan_critic"], limit=240)
        if lead_issue:
            task["plan_gate_reason"] = lead_issue

    exec_critic = task.get("exec_critic")
    if isinstance(exec_critic, dict):
        task["exec_critic"] = normalize_exec_critic_payload(
            exec_critic,
            attempt_no=int(exec_critic.get("attempt", 1) or 1),
            max_attempts=int(exec_critic.get("max_attempts", 1) or 1),
            at=str(exec_critic.get("at", "")).strip() or now_iso(),
        )

    context = build_task_context(
        request_id=rid,
        task=task,
        extra=(task.get("context") if isinstance(task.get("context"), dict) else None),
    )
    if context:
        task["context"] = context

    return task


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
    path.parent.mkdir(parents=True, exist_ok=True)
    max_bytes = int_from_env(
        os.environ.get("AOE_GATEWAY_LOG_MAX_BYTES"),
        DEFAULT_GATEWAY_LOG_MAX_BYTES,
        minimum=64 * 1024,
        maximum=256 * 1024 * 1024,
    )
    keep_files = int_from_env(
        os.environ.get("AOE_GATEWAY_LOG_KEEP_FILES"),
        DEFAULT_GATEWAY_LOG_KEEP_FILES,
        minimum=1,
        maximum=30,
    )
    if path.exists() and path.stat().st_size >= max_bytes:
        for idx in range(keep_files - 1, 0, -1):
            src = path.with_name(path.name + f".{idx}")
            dst = path.with_name(path.name + f".{idx + 1}")
            if src.exists():
                if dst.exists():
                    dst.unlink(missing_ok=True)
                src.replace(dst)
        first = path.with_name(path.name + ".1")
        if first.exists():
            first.unlink(missing_ok=True)
        path.replace(first)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def room_retention_days() -> int:
    # 0 disables GC (keep forever).
    return int_from_env(
        os.environ.get("AOE_ROOM_RETENTION_DAYS"),
        DEFAULT_ROOM_RETENTION_DAYS,
        minimum=0,
        maximum=3650,
    )


def cleanup_room_logs(team_dir: Path, *, force: bool = False) -> int:
    days = room_retention_days()
    if days <= 0:
        return 0
    root = (team_dir / "logs" / "rooms").resolve()
    if not root.exists() or not root.is_dir():
        return 0

    marker = root / ".gc_last"
    today = today_key_local()
    try:
        if (not force) and marker.exists():
            prev = marker.read_text(encoding="utf-8", errors="replace").strip()
            if (prev.split() or [""])[0] == today:
                return 0
    except Exception:
        pass

    keep_from = datetime.now().date() - timedelta(days=max(0, days - 1))
    removed: List[Path] = []

    for path in root.rglob("*.jsonl"):
        if not path.is_file():
            continue
        name = path.name
        if len(name) < 10:
            continue
        key = name[:10]
        try:
            file_date = datetime.strptime(key, "%Y-%m-%d").date()
        except Exception:
            continue
        if file_date >= keep_from:
            continue
        try:
            path.unlink(missing_ok=True)
            removed.append(path)
        except Exception:
            pass

    if removed:
        parents = {p.parent for p in removed}
        for d in sorted(parents, key=lambda p: len(p.parts), reverse=True):
            cur = d
            while cur != root and cur.exists():
                try:
                    next(cur.iterdir())
                    break
                except StopIteration:
                    try:
                        cur.rmdir()
                    except Exception:
                        break
                    cur = cur.parent
                except Exception:
                    break

    try:
        marker.write_text(f"{today} removed={len(removed)}\n", encoding="utf-8")
    except Exception:
        pass
    return len(removed)


def room_autopublish_enabled() -> bool:
    return bool_from_env(os.environ.get("AOE_ROOM_AUTOPUBLISH"), True)


def normalize_room_autopublish_route(raw: Optional[str]) -> str:
    token = str(raw or "").strip().lower()
    if token in {"room", "chat", "current"}:
        return "room"
    if token in {"project", "orch", "o"}:
        return "project"
    if token in {"project-tf", "project_tf", "orch-tf", "orch_tf", "tf-project"}:
        return "project-tf"
    if token in {"tf", "taskforce"}:
        return "tf"
    return DEFAULT_ROOM_AUTOPUBLISH_ROUTE


def room_autopublish_route() -> str:
    return normalize_room_autopublish_route(os.environ.get("AOE_ROOM_AUTOPUBLISH_ROUTE"))


_ROOM_AUTOPUBLISH_EVENTS = {
    "dispatch_completed",
    "dispatch_failed",
    "exec_critic_retry",
    "exec_critic_blocked",
}


def _room_autopublish_title(event: str) -> str:
    mapping = {
        "dispatch_completed": "done",
        "dispatch_failed": "failed",
        "exec_critic_retry": "retry",
        "exec_critic_blocked": "blocked",
    }
    token = str(event or "").strip()
    return mapping.get(token, token or "event")


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
    if not room_autopublish_enabled():
        return
    if str(event or "").strip() not in _ROOM_AUTOPUBLISH_EVENTS:
        return

    project_alias = project_alias_for_key(manager_state, project) or str(project or "").strip() or "-"
    tf_id = ""
    if isinstance(task, dict):
        short_id = str(task.get("short_id", "")).strip().upper()
        if short_id:
            tf_id = re.sub(r"^T-", "TF-", short_id)
            if not tf_id.startswith("TF-"):
                tf_id = "TF-" + re.sub(r"[^A-Z0-9._-]+", "_", short_id).strip("._-")[:24]

    # Routing:
    # - If user explicitly selected a non-global room, always respect it.
    # - Otherwise, pick based on policy (default: per-project).
    selected_room = get_chat_room(manager_state, chat_id, DEFAULT_ROOM_NAME) or DEFAULT_ROOM_NAME
    selected_room = normalize_room_token(selected_room)
    if selected_room != DEFAULT_ROOM_NAME:
        room = selected_room
    else:
        route = room_autopublish_route()
        if route == "room":
            room = selected_room
        elif route == "tf":
            room = tf_id or project_alias or DEFAULT_ROOM_NAME
        elif route == "project-tf":
            room = f"{project_alias}/{tf_id}" if (project_alias and tf_id) else (project_alias or DEFAULT_ROOM_NAME)
        else:
            room = project_alias or DEFAULT_ROOM_NAME
        room = normalize_room_token(room)

    max_chars = int_from_env(
        os.environ.get("AOE_ROOM_MAX_EVENT_CHARS"),
        DEFAULT_MAX_EVENT_CHARS,
        minimum=200,
        maximum=20000,
    )
    max_file_bytes = int_from_env(
        os.environ.get("AOE_ROOM_MAX_FILE_BYTES"),
        DEFAULT_MAX_FILE_BYTES,
        minimum=64 * 1024,
        maximum=50 * 1024 * 1024,
    )

    label = task_display_label(task, request_id) if isinstance(task, dict) else (str(request_id or "").strip() or "-")
    todo_id = str(task.get("todo_id", "")).strip() if isinstance(task, dict) else ""

    verdict = ""
    action = ""
    reason = ""
    if isinstance(task, dict) and isinstance(task.get("exec_critic"), dict):
        ec = task.get("exec_critic") or {}
        verdict = str(ec.get("verdict", "")).strip().lower()
        action = str(ec.get("action", "")).strip().lower()
        reason = str(ec.get("reason", "")).strip()

    title = _room_autopublish_title(event)
    prefix = f"[{project_alias}]"
    if todo_id:
        prefix = f"{prefix} {todo_id}"

    extras: List[str] = []
    if verdict:
        extras.append(f"verdict={verdict}")
    if action and action not in {"-", "none", "ok"}:
        extras.append(f"action={action}")
    if stage:
        extras.append(f"stage={stage}")
    if status:
        extras.append(f"status={status}")
    if error_code:
        extras.append(f"error={error_code}")
    extra_tail = (" (" + " ".join(extras) + ")") if extras else ""

    text = f"{prefix} {title}: {label}{extra_tail}"
    if event in {"exec_critic_blocked", "dispatch_failed"} and reason:
        clipped_reason = reason if len(reason) <= 240 else (reason[:240] + "...")
        text = text + f"\nreason: {clipped_reason}"
    elif event == "exec_critic_retry" and detail:
        clipped_detail = detail if len(detail) <= 240 else (detail[:240] + "...")
        text = text + f"\nnext: {clipped_detail}"

    if len(text) > max_chars:
        text = text[: max_chars - 20] + " ...(truncated)"

    append_room_event(
        team_dir=team_dir,
        room=room,
        event={
            "ts": now_iso(),
            "actor": "gateway",
            "kind": "event",
            "event": str(event),
            "project": str(project),
            "project_alias": project_alias,
            "request_id": str(request_id),
            "task_label": label,
            "todo_id": todo_id,
            "stage": str(stage),
            "status": str(status),
            "error_code": str(error_code),
            "detail": str(detail),
            "text": text,
        },
        max_file_bytes=max_file_bytes,
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
    loop_state = load_state(args.state_file)
    loop_state[STATE_FAILED_QUEUE_KEY] = normalize_failed_queue(
        loop_state.get(STATE_FAILED_QUEUE_KEY),
        failed_queue_keep_limit(),
    )
    save_state(args.state_file, loop_state)
    pick = str(target or "").strip()
    pick_lower = pick.lower()
    if pick_lower in {"", "list", "ls", "status"}:
        send(summarize_failed_queue(loop_state, chat_id), context="replay-list", with_menu=True)
        return True

    if pick_lower == "purge":
        removed = purge_failed_queue_for_chat(loop_state, chat_id)
        save_state(args.state_file, loop_state)
        send(
            f"replay purge done\n- removed: {removed}\n- chat: {chat_id}",
            context="replay-purge",
            with_menu=True,
        )
        log_event(
            event="replay_purged",
            stage="intake",
            status="accepted",
            detail=f"chat={chat_id} removed={removed}",
        )
        return True

    show_target = ""
    parts = pick.split(None, 1)
    action = str(parts[0]).strip().lower() if parts else ""
    if action == "show":
        if len(parts) < 2 or not str(parts[1]).strip():
            send(REPLAY_USAGE, context="replay-usage", with_menu=True)
            return True
        show_target = str(parts[1]).strip()

    resolve_target = show_target or pick
    item, err = resolve_failed_queue_item(loop_state, chat_id, resolve_target)
    if item is None:
        send(f"{err}\n{summarize_failed_queue(loop_state, chat_id)}", context="replay-miss", with_menu=True)
        return True

    if show_target:
        send(format_failed_queue_item_detail(item), context="replay-show", with_menu=True)
        return True

    removed = remove_failed_queue_item(loop_state, str(item.get("id", "")).strip()) or item
    save_state(args.state_file, loop_state)

    replay_text = str(removed.get("text", "")).strip()
    if not replay_text:
        send("replay item has empty text", context="replay-empty", with_menu=True)
        return True
    replay_cmd, _ = parse_command(replay_text)
    if str(replay_cmd or "").strip().lower() == "replay":
        send("replay blocked: nested /replay payload", context="replay-blocked", with_menu=True)
        return True

    replay_id = str(removed.get("id", "")).strip() or "n/a"
    send(
        f"replay start\n- id: {replay_id}\n- source_cmd: {removed.get('cmd') or '-'}\n- source_error: {removed.get('error_code') or '-'}",
        context="replay-start",
    )
    log_event(
        event="replay_started",
        stage="intake",
        status="accepted",
        detail=f"id={replay_id} source_cmd={removed.get('cmd') or '-'} source_error={removed.get('error_code') or '-'}",
    )
    handle_text_message(args, token, chat_id, replay_text, trace_id=f"replay-{replay_id}")
    return True


def summarize_gateway_metrics(
    team_dir: Path,
    project_name: str,
    hours: int = 24,
    state_file: Optional[Any] = None,
) -> str:
    cap_hours = max(1, min(168, int(hours or 24)))
    poll_state_path = state_file if state_file is not None else (team_dir / "telegram_gateway_state.json")
    poll_summary = summarize_gateway_poll_state(poll_state_path, project_name=project_name)
    path = team_dir / "logs" / "gateway_events.jsonl"
    if not path.exists():
        return f"orch: {project_name}\nmetrics: no data file\nwindow_hours: {cap_hours}\n{poll_summary}"

    cutoff = datetime.now(timezone.utc) - timedelta(hours=cap_hours)
    total = 0
    incoming = 0
    accepted = 0
    rejected = 0
    sent_ok = 0
    sent_fail = 0
    dispatch_done = 0
    direct_done = 0
    errors = 0
    error_codes: Dict[str, int] = {}
    latencies: List[int] = []
    trace_state: Dict[str, Dict[str, bool]] = {}

    def touch_trace(trace: str) -> Optional[Dict[str, bool]]:
        token = str(trace or "").strip()
        if not token:
            return None
        row = trace_state.get(token)
        if row is None:
            row = {"accepted": False, "success": False, "failed": False}
            trace_state[token] = row
        return row

    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    row = json.loads(raw)
                except Exception:
                    continue
                if not isinstance(row, dict):
                    continue
                ts = parse_iso_ts(str(row.get("timestamp", "")))
                if ts is None:
                    continue
                if ts.astimezone(timezone.utc) < cutoff:
                    continue

                total += 1
                event = str(row.get("event", "")).strip()
                status = str(row.get("status", "")).strip().lower()
                trace_id = str(row.get("trace_id", "")).strip()
                trace = touch_trace(trace_id)
                if event == "incoming_message":
                    incoming += 1
                elif event == "command_resolved":
                    if status == "accepted":
                        accepted += 1
                        if trace is not None:
                            trace["accepted"] = True
                elif event == "input_rejected":
                    rejected += 1
                elif event == "send_message":
                    if status == "sent":
                        sent_ok += 1
                        if trace is not None:
                            trace["success"] = True
                    else:
                        sent_fail += 1
                        if trace is not None:
                            trace["failed"] = True
                elif event == "dispatch_completed":
                    dispatch_done += 1
                    if trace is not None:
                        trace["success"] = True
                elif event == "direct_reply":
                    direct_done += 1
                    if trace is not None:
                        trace["success"] = True
                elif event == "dispatch_result":
                    if status == "failed":
                        if trace is not None:
                            trace["failed"] = True
                    else:
                        if trace is not None:
                            trace["success"] = True
                elif event == "handler_error":
                    errors += 1
                    code = str(row.get("error_code", "")).strip() or ERROR_INTERNAL
                    error_codes[code] = error_codes.get(code, 0) + 1
                    if trace is not None:
                        trace["failed"] = True

                try:
                    latency = int(row.get("latency_ms", 0) or 0)
                except Exception:
                    latency = 0
                if latency > 0:
                    latencies.append(latency)
    except Exception:
        return f"orch: {project_name}\nmetrics: failed to read log\nwindow_hours: {cap_hours}\n{poll_summary}"

    send_total = sent_ok + sent_fail
    send_success_rate = (100.0 * sent_ok / send_total) if send_total > 0 else 0.0
    accepted_traces = [v for v in trace_state.values() if bool(v.get("accepted"))]
    cmd_success = 0
    cmd_failed = 0
    cmd_pending = 0
    for row in accepted_traces:
        failed = bool(row.get("failed"))
        success = bool(row.get("success"))
        if failed:
            cmd_failed += 1
        elif success:
            cmd_success += 1
        else:
            cmd_pending += 1
    cmd_done = cmd_success + cmd_failed
    cmd_success_rate = (100.0 * cmd_success / cmd_done) if cmd_done > 0 else 0.0

    p50 = percentile(latencies, 0.50)
    p95 = percentile(latencies, 0.95)

    lines = [
        f"orch: {project_name}",
        f"window_hours: {cap_hours}",
        f"events: total={total} incoming={incoming} accepted={accepted} rejected={rejected}",
        f"commands: success={cmd_success} failed={cmd_failed} pending={cmd_pending} success_rate={cmd_success_rate:.1f}%",
        f"send: ok={sent_ok} fail={sent_fail} success_rate={send_success_rate:.1f}%",
        f"completion: dispatch={dispatch_done} direct={direct_done} errors={errors}",
        f"latency_ms: p50={p50} p95={p95} samples={len(latencies)}",
    ]
    if error_codes:
        rows = ", ".join(f"{k}={v}" for k, v in sorted(error_codes.items()))
        lines.append(f"error_codes: {rows}")
    lines.append(poll_summary)
    return "\n".join(lines)


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


def classify_handler_error(err: Exception) -> Tuple[str, str, str]:
    if isinstance(err, subprocess.TimeoutExpired):
        return (
            ERROR_TIMEOUT,
            "요청 처리 시간이 제한을 초과했습니다.",
            "/task 또는 /check로 진행 상태를 확인하세요.",
        )

    msg = str(err or "").strip()
    low = msg.lower()
    if (
        ("usage:" in low)
        or ("unknown option" in low)
        or ("unknown command" in low)
        or ("invalid cli format" in low)
        or ("invalid priority" in low)
        or ("must be integer" in low)
        or ("unknown orch project" in low)
        or ("unknown chat alias" in low)
        or ("chat target must be" in low)
    ):
        return (ERROR_COMMAND, "명령 형식이 올바르지 않습니다.", "/help로 명령 예시를 확인하세요.")
    if "plan gate blocked" in low or "critic" in low:
        return (ERROR_GATE, "계획 검증 게이트에서 차단되었습니다.", "요청 범위를 좁혀 /dispatch로 다시 실행하세요.")
    if "verifier gate" in low:
        return (ERROR_GATE, "검증 역할(verifier) 요건이 충족되지 않았습니다.", "/status로 역할 구성을 확인하세요.")
    if "permission denied" in low or "unauthorized" in low:
        return (ERROR_AUTH, "권한이 없습니다.", "/whoami로 현재 chat 권한을 확인하세요.")
    if "aoe-team request failed" in low or "request returned non-json" in low:
        return (ERROR_REQUEST, "요청 상태를 조회하지 못했습니다.", "잠시 후 /check 또는 /task를 다시 실행하세요.")
    if "telegram api" in low or "sendmessage failed" in low:
        return (ERROR_TELEGRAM, "텔레그램 전송 과정에서 오류가 발생했습니다.", "잠시 후 같은 명령을 다시 실행하세요.")
    if "aoe-orch run failed" in low or "aoe-orch" in low:
        return (ERROR_ORCH, "오케스트레이터 실행 중 오류가 발생했습니다.", "/status로 시스템 상태를 확인하세요.")
    return (ERROR_INTERNAL, "내부 처리 중 오류가 발생했습니다.", "/help 또는 /status로 상태를 확인하세요.")


def format_error_message(error_code: str, user_message: str, next_step: str, detail: str = "") -> str:
    lines = [
        f"error_code: {error_code}",
        user_message,
    ]
    token = mask_sensitive_text(str(detail or "").strip())
    if token:
        lines.append(f"detail: {token[:180]}")
    lines.append(f"next: {next_step}")
    return "\n".join(lines)


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
    projects = state.get("projects") or {}
    if not isinstance(projects, dict) or not projects:
        raise RuntimeError("no orch projects registered")

    lock_key = get_project_lock_key(state)
    raw_name = str(name or lock_key or str(state.get("active", "default"))).strip()
    key = normalize_project_name(raw_name)
    entry = projects.get(key)
    if not isinstance(entry, dict):
        alias = normalize_project_alias(raw_name)
        if alias:
            alias_map = ensure_project_aliases(state)
            alias_key = str(alias_map.get(alias, "")).strip()
            if alias_key:
                key = alias_key
                entry = projects.get(key)
    if not isinstance(entry, dict):
        known = ", ".join(sorted(projects.keys()))
        alias_map = ensure_project_aliases(state)
        alias_known = ", ".join(
            f"{a}->{k}" for a, k in sorted(alias_map.items(), key=lambda kv: extract_project_alias_index(kv[0]))
        )
        if alias_known:
            raise RuntimeError(f"unknown orch project: {raw_name or key} (known: {known}; aliases: {alias_known})")
        raise RuntimeError(f"unknown orch project: {raw_name or key} (known: {known})")
    if lock_key and key != lock_key:
        lock_alias = project_alias_for_key(state, lock_key) or "-"
        req_alias = project_alias_for_key(state, key) or "-"
        raise RuntimeError(
            f"project lock active: {lock_alias} ({lock_key}). "
            f"requested={req_alias} ({key}). use /focus off or /focus {lock_alias}"
        )
    return key, entry


def make_project_args(args: argparse.Namespace, entry: Dict[str, Any], key: str = "") -> argparse.Namespace:
    copied = argparse.Namespace(**vars(args))
    copied.project_root = Path(str(entry.get("project_root", args.project_root))).expanduser().resolve()
    copied.team_dir = Path(str(entry.get("team_dir", copied.project_root / '.aoe-team'))).expanduser().resolve()
    project_key = normalize_project_name(str(key or entry.get("name", "")))
    copied._aoe_project_key = project_key or normalize_project_name(str(copied.project_root.name))
    copied._aoe_project_alias = normalize_project_alias(str(entry.get("project_alias", "")))
    copied._aoe_project_display_name = str(entry.get("display_name", "")).strip() or copied._aoe_project_key
    return copied


def register_orch_project(
    state: Dict[str, Any],
    name: str,
    project_root: Path,
    team_dir: Path,
    overview: str,
    set_active: bool,
) -> Tuple[str, Dict[str, Any]]:
    key = normalize_project_name(name)
    projects = state.setdefault("projects", {})
    if not isinstance(projects, dict):
        state["projects"] = {}
        projects = state["projects"]

    existing = projects.get(key)
    entry = {
        "name": key,
        "display_name": (name or key).strip() or key,
        "project_alias": "",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "overview": (overview or "").strip(),
        "last_request_id": "",
        "tasks": {},
        "task_alias_index": {},
        "task_seq": 0,
        "todos": [],
        "todo_seq": 0,
        "todo_proposals": [],
        "todo_proposal_seq": 0,
        "system_project": False,
        "ops_hidden": False,
        "ops_hidden_reason": "",
        "paused": False,
        "paused_at": "",
        "paused_by": "",
        "paused_reason": "",
        "resumed_at": "",
        "resumed_by": "",
        "last_sync_at": "",
        "last_sync_mode": "",
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }
    if isinstance(existing, dict):
        entry["created_at"] = str(existing.get("created_at", entry["created_at"]))
        entry["project_alias"] = normalize_project_alias(str(existing.get("project_alias", "")))
        entry["paused"] = bool_from_json(existing.get("paused"), False)
        entry["paused_at"] = str(existing.get("paused_at", "")).strip()
        entry["paused_by"] = str(existing.get("paused_by", "")).strip()
        entry["paused_reason"] = str(existing.get("paused_reason", "")).strip()[:400]
        entry["system_project"] = bool_from_json(existing.get("system_project"), False)
        entry["ops_hidden"] = bool_from_json(existing.get("ops_hidden"), False)
        entry["ops_hidden_reason"] = str(existing.get("ops_hidden_reason", "")).strip()[:400]
        entry["resumed_at"] = str(existing.get("resumed_at", "")).strip()
        entry["resumed_by"] = str(existing.get("resumed_by", "")).strip()
        entry["last_sync_at"] = str(existing.get("last_sync_at", "")).strip()[:40]
        entry["last_sync_mode"] = str(existing.get("last_sync_mode", "")).strip()[:40]
        if not entry["overview"]:
            entry["overview"] = str(existing.get("overview", "")).strip()
        old_req = str(existing.get("last_request_id", "")).strip()
        if old_req:
            entry["last_request_id"] = old_req
        old_tasks = existing.get("tasks")
        if isinstance(old_tasks, dict):
            entry["tasks"] = old_tasks
            trim_project_tasks(entry["tasks"])
        old_alias_index = existing.get("task_alias_index")
        if isinstance(old_alias_index, dict):
            entry["task_alias_index"] = old_alias_index
        old_seq = existing.get("task_seq")
        try:
            entry["task_seq"] = max(0, int(old_seq or entry.get("task_seq", 0)))
        except Exception:
            pass
        old_todos = existing.get("todos")
        if isinstance(old_todos, list):
            entry["todos"] = old_todos
        old_todo_seq = existing.get("todo_seq")
        try:
            entry["todo_seq"] = max(0, int(old_todo_seq or entry.get("todo_seq", 0)))
        except Exception:
            pass
        old_proposals = existing.get("todo_proposals")
        if isinstance(old_proposals, list):
            entry["todo_proposals"] = old_proposals
        old_proposal_seq = existing.get("todo_proposal_seq")
        try:
            entry["todo_proposal_seq"] = max(0, int(old_proposal_seq or entry.get("todo_proposal_seq", 0)))
        except Exception:
            pass
        old_pending = existing.get("pending_todo")
        if isinstance(old_pending, dict):
            pt_id = str(old_pending.get("todo_id", "")).strip()
            pt_chat = str(old_pending.get("chat_id", "")).strip()
            pt_selected = str(old_pending.get("selected_at", "")).strip()
            if pt_id and pt_chat:
                entry["pending_todo"] = {
                    "todo_id": pt_id[:32],
                    "chat_id": pt_chat[:32],
                    "selected_at": pt_selected or entry.get("updated_at", "") or now_iso(),
                }
    projects[key] = entry
    ensure_project_aliases(state)

    if set_active:
        state["active"] = key

    return key, entry



def parse_roles_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    out: List[str] = []
    seen: Set[str] = set()
    for item in str(raw).split(","):
        token = item.strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def dedupe_roles(roles: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: Set[str] = set()
    for item in roles:
        token = str(item or "").strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def load_orchestrator_roles(team_dir: Path) -> List[str]:
    cfg = team_dir / "orchestrator.json"
    if not cfg.exists():
        return []

    try:
        data = json.loads(cfg.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, dict):
        return []

    roles: List[str] = []
    coordinator = data.get("coordinator")
    if isinstance(coordinator, dict):
        role = str(coordinator.get("role", "")).strip()
        if role:
            roles.append(role)

    agents = data.get("agents")
    if isinstance(agents, list):
        for row in agents:
            if isinstance(row, dict):
                role = str(row.get("role", "")).strip()
            else:
                role = str(row).strip()
            if role:
                roles.append(role)

    return dedupe_roles(roles)


def load_orchestrator_role_profiles(team_dir: Path, available_roles: Optional[List[str]] = None) -> List[Dict[str, str]]:
    roles = dedupe_roles(available_roles or load_orchestrator_roles(team_dir))
    profiles: List[Dict[str, str]] = []
    for role in roles:
        mission = ""
        agent_doc = team_dir / "agents" / role / "AGENTS.md"
        if agent_doc.exists():
            try:
                text = agent_doc.read_text(encoding="utf-8")
                match = re.search(r"^## Mission\s*\n(.+?)(?:\n## |\Z)", text, flags=re.MULTILINE | re.DOTALL)
                if match:
                    mission = " ".join(line.strip() for line in match.group(1).splitlines() if line.strip())
            except Exception:
                mission = ""
        profiles.append({"role": role, "role_key": role.lower(), "mission": mission.strip()})
    return profiles


def resolve_verifier_candidates(raw: Optional[str]) -> List[str]:
    parsed = parse_roles_csv(raw or DEFAULT_VERIFIER_ROLES)
    return parsed or parse_roles_csv(DEFAULT_VERIFIER_ROLES)


def ensure_verifier_roles(
    selected_roles: List[str],
    available_roles: List[str],
    verifier_candidates: List[str],
) -> Tuple[List[str], List[str], bool, List[str]]:
    selected = dedupe_roles(selected_roles)
    available = dedupe_roles(available_roles)

    candidate_keys = [c.lower() for c in verifier_candidates if c]
    selected_verifiers = [r for r in selected if r.lower() in candidate_keys]

    available_verifiers: List[str] = []
    for cand in verifier_candidates:
        ckey = cand.lower()
        for role in available:
            if role.lower() == ckey and role not in available_verifiers:
                available_verifiers.append(role)

    added = False
    if not selected_verifiers and available_verifiers:
        selected.append(available_verifiers[0])
        selected_verifiers = [available_verifiers[0]]
        added = True

    return dedupe_roles(selected), dedupe_roles(selected_verifiers), added, available_verifiers


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



def _registry_todo_counts(entry: Dict[str, Any]) -> Dict[str, int]:
    counts = {"open": 0, "running": 0, "blocked": 0, "done": 0}
    raw = entry.get("todos")
    todos = raw if isinstance(raw, list) else []
    for row in todos:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", "open")).strip().lower() or "open"
        if status == "canceled":
            continue
        if status not in counts:
            status = "open"
        counts[status] += 1
    return counts


def _registry_latest_task(entry: Dict[str, Any]) -> Tuple[str, str]:
    tasks = entry.get("tasks")
    if not isinstance(tasks, dict) or not tasks:
        return "-", "-"
    best_req = ""
    best_task: Optional[Dict[str, Any]] = None
    best_key = ""
    for req_id, task in tasks.items():
        if not isinstance(task, dict):
            continue
        updated = str(task.get("updated_at", "")).strip() or str(task.get("created_at", "")).strip()
        if updated >= best_key:
            best_key = updated
            best_req = str(req_id or "").strip()
            best_task = task
    if not best_task:
        return "-", "-"
    return task_display_label(best_task, fallback_request_id=best_req), normalize_task_status(best_task.get("status", "pending"))


def _short_root_label(raw: str) -> str:
    root = str(raw or "").strip()
    if not root:
        return "-"
    try:
        path = Path(root)
        tail = path.name or root
    except Exception:
        tail = root
    if len(root) <= 72:
        return root
    return f".../{tail}"


def summarize_orch_registry(state: Dict[str, Any]) -> str:
    active = normalize_project_name(str(state.get("active", "default")))
    projects = state.get("projects") or {}
    if not isinstance(projects, dict) or not projects:
        return "orch registry empty"

    alias_map = ensure_project_aliases(state)
    active_alias = project_alias_for_key(state, active) or "-"
    locked = project_lock_label(state)
    lines = [f"active: {active_alias} ({active})", "project map:"]
    if locked:
        lines.insert(1, f"project_lock: {locked}")

    def _sort_key(k: str) -> Tuple[int, str]:
        alias = normalize_project_alias(str((projects.get(k) or {}).get("project_alias", "")))
        return (extract_project_alias_index(alias), k)

    visible_keys = visible_ops_project_keys(projects)
    for key in sorted(visible_keys, key=_sort_key):
        entry = projects.get(key) or {}
        marker = "*" if key == active else "-"
        alias = normalize_project_alias(str(entry.get("project_alias", ""))) or "O?"
        root = str(entry.get("project_root", "")).strip()
        paused = bool_from_json(entry.get("paused"), False)
        paused_tag = " [PAUSED]" if paused else ""
        unready_issue = project_runtime_issue(entry)
        unready_tag = " [UNREADY]" if unready_issue else ""
        display = str(entry.get("display_name", key)).strip() or key
        counts = _registry_todo_counts(entry)
        pending = entry.get("pending_todo")
        pending_tag = " [PENDING]" if isinstance(pending, dict) and str(pending.get("todo_id", "")).strip() else ""
        last_task_label, last_task_status = _registry_latest_task(entry)
        task_disp = last_task_label.replace(" | ", " ") if last_task_label != "-" else "-"
        if task_disp != "-" and last_task_status != "-":
            task_disp = f"{task_disp}[{last_task_status}]"
        sync_at = str(entry.get("last_sync_at", "")).strip()
        sync_mode = str(entry.get("last_sync_mode", "")).strip()
        sync_age = compact_age_label(sync_at)
        if sync_mode and sync_age != "-":
            sync_disp = f"{sync_mode} {sync_age}"
        elif sync_mode:
            sync_disp = sync_mode
        else:
            sync_disp = sync_age
        if not sync_disp or sync_disp == "-":
            sync_disp = "never"
        lines.append(
            f"{marker} {alias} {display}{paused_tag}{unready_tag}{pending_tag} | "
            f"todo o/r/b={counts['open']}/{counts['running']}/{counts['blocked']} | "
            f"last_sync={sync_disp} | last_task={task_disp}"
        )
        lines.append(f"  key={key} | root={_short_root_label(root)}")
        if unready_issue:
            lines.append(f"  runtime={project_runtime_label(entry)}")

    if alias_map:
        rows = [
            f"{a}:{k}"
            for a, k in sorted(alias_map.items(), key=lambda kv: extract_project_alias_index(kv[0]))
            if str(k) in visible_keys
        ]
        if rows:
            lines.append("aliases: " + ", ".join(rows))
    return "\n".join(lines)

def run_command(cmd: List[str], env: Optional[Dict[str, str]], timeout_sec: int) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        env=env,
        timeout=max(5, int(timeout_sec)),
    )


def _role_name_aliases(role: str) -> List[str]:
    token = str(role or "").strip()
    if not token:
        return []
    aliases = {
        token.lower(),
        token.replace("-", " ").lower(),
        token.replace("_", " ").lower(),
        re.sub(r"(?<!^)([A-Z])", r" \1", token).strip().lower(),
    }
    norm = token.lower()
    if "review" in norm:
        aliases.update({"reviewer", "qa", "verifier", "critic"})
    if "data" in norm:
        aliases.update({"dataengineer", "data engineer"})
    if any(x in norm for x in {"dev", "engineer", "coder", "builder"}):
        aliases.update({"developer", "engineer", "builder", "implementer"})
    if any(x in norm for x in {"writer", "doc", "scribe"}):
        aliases.update({"writer", "doc writer"})
    if any(x in norm for x in {"anal", "analysis", "research", "tuner"}):
        aliases.update({"analyst", "analysis", "researcher", "tuner"})
    return [alias for alias in aliases if alias]


def _role_score_from_text(prompt_lower: str, role: str, mission: str) -> int:
    name_key = str(role or "").strip().lower()
    mission_lower = str(mission or "").strip().lower()
    score = 0

    review_keys = (
        "review", "risk", "regression", "test", "qa", "bug", "verify", "validation", "inspect", "check",
        "리뷰", "리스크", "회귀", "테스트", "버그", "검증", "점검", "확인", "검토",
    )
    data_keys = (
        "data", "dataset", "etl", "schema", "sql", "table", "column", "csv", "pipeline", "null", "quality",
        "데이터", "스키마", "결측", "컬럼", "테이블", "적재", "정합성", "품질",
    )
    build_keys = (
        "implement", "implementation", "build", "code", "fix", "patch", "refactor", "develop",
        "개발", "구현", "수정", "패치", "리팩토링", "코드",
    )
    doc_keys = (
        "document", "documentation", "docs", "summary", "report", "writeup", "guide", "readme", "tutorial", "handoff",
        "문서", "요약", "보고", "보고서", "가이드", "튜토리얼", "인수인계",
    )
    analysis_keys = ("analyze", "analysis", "research", "compare", "benchmark", "investigate", "분석", "조사", "비교", "벤치마크", "리서치")

    if any(k in prompt_lower for k in review_keys):
        if any(k in name_key for k in ("review", "qa", "verif")) or any(k in mission_lower for k in ("risk", "regression", "test", "review")):
            score += 6
    if any(k in prompt_lower for k in data_keys):
        if any(k in name_key for k in ("data", "etl", "sql", "schema")) or any(k in mission_lower for k in ("data", "etl", "schema", "quality")):
            score += 6
    if any(k in prompt_lower for k in build_keys):
        build_name_hit = any(k in name_key for k in ("dev", "coder", "builder")) or (
            ("engineer" in name_key) and ("data" not in name_key)
        )
        if build_name_hit or any(k in mission_lower for k in ("implement", "build", "code", "develop")):
            score += 5
    if any(k in prompt_lower for k in doc_keys):
        if any(k in name_key for k in ("writer", "doc", "scribe")) or any(k in mission_lower for k in ("document", "report", "summary")):
            score += 4
    if any(k in prompt_lower for k in analysis_keys):
        if any(k in name_key for k in ("anal", "research", "tuner")) or any(k in mission_lower for k in ("analysis", "research", "compare")):
            score += 4

    short_check = ("한 문장" in prompt_lower) or ("짧게" in prompt_lower) or ("3개만" in prompt_lower) or ("존재 여부" in prompt_lower)
    if short_check and ("review" in name_key or "verif" in name_key or "qa" in name_key):
        score += 2

    return score


def choose_auto_dispatch_roles(
    prompt: str,
    available_roles: Optional[List[str]] = None,
    team_dir: Optional[Path] = None,
) -> List[str]:
    prompt_text = str(prompt or "").strip()
    prompt_lower = prompt_text.lower()
    team_dir_path = Path(team_dir).expanduser().resolve() if team_dir else None

    profiles = load_orchestrator_role_profiles(team_dir_path, available_roles) if team_dir_path else [
        {"role": role, "role_key": str(role).lower(), "mission": ""} for role in dedupe_roles(available_roles or [])
    ]
    if not profiles:
        profiles = [
            {"role": "DataEngineer", "role_key": "dataengineer", "mission": ""},
            {"role": "Reviewer", "role_key": "reviewer", "mission": ""},
            {"role": "Local-Dev", "role_key": "local-dev", "mission": ""},
            {"role": "Local-Writer", "role_key": "local-writer", "mission": ""},
            {"role": "Local-Analyst", "role_key": "local-analyst", "mission": ""},
        ]

    explicit: List[str] = []
    for profile in profiles:
        role = str(profile.get("role", "")).strip()
        if not role or role.lower() == "orchestrator":
            continue
        for alias in _role_name_aliases(role):
            if alias in prompt_lower:
                explicit.append(role)
                break
    explicit = dedupe_roles(explicit)
    if explicit:
        return explicit

    has_review_signal = any(token in prompt_lower for token in ("review", "risk", "regression", "test", "qa", "verify", "리뷰", "검토", "검증", "테스트", "리스크"))
    has_data_signal = any(token in prompt_lower for token in ("data", "dataset", "etl", "schema", "sql", "csv", "pipeline", "데이터", "스키마", "결측", "적재", "정합성"))
    has_build_signal = any(token in prompt_lower for token in ("implement", "build", "code", "fix", "patch", "refactor", "개발", "구현", "수정", "패치", "리팩토링", "코드"))
    has_doc_signal = any(token in prompt_lower for token in ("document", "documentation", "docs", "summary", "report", "readme", "guide", "tutorial", "문서", "요약", "보고", "보고서", "가이드", "튜토리얼"))
    has_analysis_signal = any(token in prompt_lower for token in ("analyze", "analysis", "research", "compare", "benchmark", "investigate", "분석", "조사", "비교", "벤치마크", "리서치"))

    wants_multi = any(
        token in prompt_lower
        for token in ("각각", "둘 다", "둘다", "together", "cross-check", "교차", "병렬", "분리", "tf", "team", "sub-task", "subtask", "역할별")
    )
    category_hits = sum(1 for flag in (has_review_signal, has_data_signal, has_build_signal, has_doc_signal, has_analysis_signal) if flag)
    if category_hits >= 2:
        wants_multi = True

    scored: List[Tuple[int, str]] = []
    for profile in profiles:
        role = str(profile.get("role", "")).strip()
        if not role or role.lower() == "orchestrator":
            continue
        score = _role_score_from_text(prompt_lower, role, str(profile.get("mission", "")))
        if score > 0:
            scored.append((score, role))

    if not scored:
        data_keys = ("data", "dataset", "etl", "schema", "sql", "pipeline", "품질", "데이터", "스키마", "적재", "정합성", "검증")
        review_keys = ("review", "risk", "regression", "test", "qa", "bug", "리뷰", "리스크", "회귀", "테스트", "버그", "검토")
        build_keys = ("implement", "build", "code", "fix", "patch", "refactor", "개발", "구현", "수정", "패치", "리팩토링", "코드")
        doc_keys = ("document", "documentation", "docs", "summary", "report", "guide", "readme", "tutorial", "문서", "요약", "보고", "보고서", "가이드", "튜토리얼")
        analysis_keys = ("analyze", "analysis", "research", "compare", "benchmark", "investigate", "분석", "조사", "비교", "벤치마크", "리서치")
        both_keys = ("both", "둘 다", "둘다", "각각", "cross-check", "교차")
        roles: List[str] = []
        if any(k in prompt_lower for k in data_keys):
            roles.append("DataEngineer")
        if any(k in prompt_lower for k in review_keys):
            roles.append("Reviewer")
        if any(k in prompt_lower for k in build_keys):
            roles.append("Local-Dev")
        if any(k in prompt_lower for k in doc_keys):
            roles.append("Local-Writer")
        if any(k in prompt_lower for k in analysis_keys):
            roles.append("Local-Analyst")
        if not roles and any(k in prompt_lower for k in both_keys):
            roles = ["DataEngineer", "Reviewer"]
        return dedupe_roles([r for r in roles if r])

    scored.sort(key=lambda item: (-item[0], item[1].lower()))
    top_score = scored[0][0]
    selected = [scored[0][1]]
    if wants_multi:
        for score, role in scored[1:3]:
            if score >= max(3, top_score - 2):
                selected.append(role)
    return dedupe_roles(selected)


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
    workers = [r for r in dedupe_roles(available_roles) if r.lower() != "orchestrator"]
    return workers or ["DataEngineer", "Reviewer", "Local-Dev", "Local-Writer", "Local-Analyst"]


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
    lines.append("위 계획과 체크사항을 반영해 역할별 실행/검증 결과를 산출해라.")
    return "\n".join(lines)


def run_orchestrator_direct(args: argparse.Namespace, user_prompt: str, reply_lang: str = DEFAULT_REPLY_LANG) -> str:
    lang = normalize_chat_lang_token(reply_lang, DEFAULT_REPLY_LANG) or DEFAULT_REPLY_LANG
    if lang == "en":
        prompt = (
            "You are a project orchestrator. Reply naturally to a Telegram user.\n"
            "Principles:\n"
            "- English\n"
            "- Do not expose internal role/protocol/request-id unless user explicitly asks\n"
            "- Do not overclaim or assert unsupported facts\n"
            "- Be concise and practical; suggest next action only when useful\n\n"
            f"User request:\n{user_prompt.strip()}\n"
        )
    else:
        prompt = (
            "너는 프로젝트 오케스트레이터다. 텔레그램 사용자와 자연스럽게 대화하듯 답해라.\n"
            "원칙:\n"
            "- 한국어\n"
            "- 사용자가 묻지 않으면 내부 역할/프로토콜/요청ID를 노출하지 않는다\n"
            "- 과장하거나 근거 없는 수치를 단정하지 않는다\n"
            "- 실무적으로 간결하게 답하고, 필요할 때만 다음 행동을 제안한다\n\n"
            f"사용자 요청:\n{user_prompt.strip()}\n"
        )
    return run_codex_exec(args, prompt, timeout_sec=min(900, max(90, int(args.orch_command_timeout_sec))))

def synthesize_orchestrator_response(
    args: argparse.Namespace,
    user_prompt: str,
    state: Dict[str, Any],
    reply_lang: str = DEFAULT_REPLY_LANG,
) -> str:
    replies = state.get("replies") or []
    chunks: List[str] = []
    for r in replies[:8]:
        role = str(r.get("role", r.get("from", "agent"))).strip() or "agent"
        body = str(r.get("body", "")).strip()
        if body:
            chunks.append(f"[{role}]\n{body}")

    joined = "\n\n".join(chunks).strip() or "(no replies)"
    lang = normalize_chat_lang_token(reply_lang, DEFAULT_REPLY_LANG) or DEFAULT_REPLY_LANG
    if lang == "en":
        prompt = (
            "You are a team orchestrator. Merge sub-agent replies into a single user-facing answer.\n"
            "Rules:\n"
            "- English\n"
            "- Hide operational details such as roles/protocol/request-id\n"
            "- Resolve contradictions conservatively; state uncertainty when needed\n"
            "- Do not assert unsupported facts\n"
            "- Keep a single coherent voice for the user\n\n"
            f"User request:\n{user_prompt.strip()}\n\n"
            f"Sub-agent replies:\n{joined}\n"
        )
    else:
        prompt = (
            "너는 팀 오케스트레이터다. 아래 서브에이전트 답변을 사용자용 단일 답변으로 통합해라.\n"
            "규칙:\n"
            "- 한국어\n"
            "- 내부 역할명/프로토콜/요청ID 같은 운영 디테일은 숨긴다\n"
            "- 서로 모순되는 내용은 보수적으로 정리하고, 불확실하면 불확실하다고 명시한다\n"
            "- 실행 근거 없는 수치/사실은 단정하지 않는다\n"
            "- 사용자에게는 자연스러운 한 목소리로 답한다\n\n"
            f"사용자 요청:\n{user_prompt.strip()}\n\n"
            f"서브에이전트 답변:\n{joined}\n"
        )
    return run_codex_exec(args, prompt, timeout_sec=min(900, max(90, int(args.orch_command_timeout_sec))))


def critique_task_execution_result(
    args: argparse.Namespace,
    user_prompt: str,
    state: Dict[str, Any],
    task: Optional[Dict[str, Any]] = None,
    attempt_no: int = 1,
    max_attempts: int = 3,
    reply_lang: str = DEFAULT_REPLY_LANG,
) -> Dict[str, Any]:
    """Return an execution-level critic verdict for a completed dispatch.

    Output schema (normalized):
    - verdict: success|retry|fail
    - action: none|retry|replan|escalate
    - reason: short string (<= 200 chars)
    - fix: optional short guidance string (<= 600 chars)
    """

    attempt_no = max(1, int(attempt_no))
    max_attempts = max(1, int(max_attempts))

    replies = state.get("replies") or []
    chunks: List[str] = []
    for r in replies[:8]:
        if not isinstance(r, dict):
            continue
        role = str(r.get("role", r.get("from", "agent"))).strip() or "agent"
        body = str(r.get("body", "")).strip()
        if not body:
            continue
        body = mask_sensitive_text(body)
        if len(body) > 1600:
            body = body[:1597] + "..."
        chunks.append(f"[{role}]\n{body}")
    joined = "\n\n".join(chunks).strip() or "(no replies)"

    plan_hint = ""
    if isinstance(task, dict) and isinstance(task.get("plan"), dict):
        plan = task.get("plan") or {}
        summary = str(plan.get("summary", "")).strip()
        st = plan.get("subtasks") or []
        titles: List[str] = []
        if isinstance(st, list):
            for row in st[:6]:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title", "")).strip() or str(row.get("goal", "")).strip()
                if title:
                    titles.append(title)
        if summary or titles:
            plan_hint = "plan_summary: {s}\nplan_subtasks: {n}\nplan_titles: {t}".format(
                s=summary or "-",
                n=len(st) if isinstance(st, list) else 0,
                t=" | ".join(titles) if titles else "-",
            )

    lang = normalize_chat_lang_token(reply_lang, DEFAULT_REPLY_LANG) or DEFAULT_REPLY_LANG
    if lang == "en":
        critic_prompt = (
            "You are an execution critic for a multi-agent task.\n"
            "Your job: decide whether the outputs satisfy the user's request.\n"
            "Return ONLY a JSON object. No prose.\n"
            "Schema:\n"
            "{\n"
            "  \"verdict\": \"success\"|\"retry\"|\"fail\",\n"
            "  \"action\": \"none\"|\"retry\"|\"replan\"|\"escalate\",\n"
            "  \"reason\": \"short reason\",\n"
            "  \"fix\": \"short guidance for next attempt (optional)\"\n"
            "}\n"
            "Rules:\n"
            "- success: requirements are met with correct/usable output.\n"
            "- retry: missing/weak parts can be fixed automatically.\n"
            "- fail: needs operator decision or requirements are ambiguous.\n"
            "- If attempt is near max, prefer fail/escalate over endless retries.\n\n"
            f"attempt: {attempt_no}/{max_attempts}\n"
            f"User request:\n{user_prompt.strip()}\n\n"
            + (f"{plan_hint}\n\n" if plan_hint else "")
            + f"Sub-agent replies:\n{joined}\n"
        )
    else:
        critic_prompt = (
            "너는 멀티에이전트 실행 결과를 판정하는 execution critic이다.\n"
            "목표: 아래 결과가 사용자 요청을 충족하는지 판정하고, 필요하면 재시도/재계획 지침을 제시한다.\n"
            "반드시 JSON 객체만 출력한다. 설명 문장 금지.\n"
            "JSON 스키마:\n"
            "{\n"
            "  \"verdict\": \"success\"|\"retry\"|\"fail\",\n"
            "  \"action\": \"none\"|\"retry\"|\"replan\"|\"escalate\",\n"
            "  \"reason\": \"짧은 이유(200자 이내)\",\n"
            "  \"fix\": \"다음 시도에서 바꿀 점(선택, 600자 이내)\"\n"
            "}\n"
            "규칙:\n"
            "- success: 요구사항 충족, 결과가 실무적으로 사용 가능.\n"
            "- retry: 일부 미흡/누락이 있으나 자동 재시도로 개선 가능.\n"
            "- fail: 요구 불명확/환경 제약/결정 필요 등으로 운영자 개입이 필요.\n"
            "- attempt가 max에 가까우면 무한 재시도 대신 fail/escalate를 우선.\n\n"
            f"attempt: {attempt_no}/{max_attempts}\n"
            f"사용자 요청:\n{user_prompt.strip()}\n\n"
            + (f"{plan_hint}\n\n" if plan_hint else "")
            + f"서브에이전트 답변:\n{joined}\n"
        )

    raw = run_codex_exec(args, critic_prompt, timeout_sec=min(600, max(60, int(args.orch_command_timeout_sec))))
    parsed = parse_json_object_from_text(raw)

    return normalize_exec_critic_payload(
        parsed,
        attempt_no=attempt_no,
        max_attempts=max_attempts,
        at=now_iso(),
    )


def extract_followup_todo_proposals(
    args: argparse.Namespace,
    user_prompt: str,
    state: Dict[str, Any],
    task: Optional[Dict[str, Any]] = None,
    reply_lang: str = DEFAULT_REPLY_LANG,
) -> List[Dict[str, Any]]:
    replies = state.get("replies") or []
    chunks: List[str] = []
    for row in replies[:8]:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", row.get("from", "agent"))).strip() or "agent"
        body = str(row.get("body", "")).strip()
        if not body:
            continue
        body = mask_sensitive_text(body)
        if len(body) > 1400:
            body = body[:1397] + "..."
        chunks.append(f"[{role}]\n{body}")
    if not chunks:
        return []

    task_lines: List[str] = []
    if isinstance(task, dict):
        todo_id = str(task.get("todo_id", "")).strip()
        if todo_id:
            task_lines.append(f"- source_todo_id: {todo_id}")
        plan = task.get("plan")
        if isinstance(plan, dict):
            summary = str(plan.get("summary", "")).strip()
            if summary:
                task_lines.append(f"- plan_summary: {summary[:240]}")
        plan_roles = task.get("plan_roles")
        if isinstance(plan_roles, list):
            roles = [str(item or "").strip() for item in plan_roles if str(item or "").strip()]
            if roles:
                task_lines.append(f"- plan_roles: {', '.join(roles[:6])}")
    task_hint = "\n".join(task_lines).strip()
    lang = normalize_chat_lang_token(reply_lang, DEFAULT_REPLY_LANG) or DEFAULT_REPLY_LANG
    joined = "\n\n".join(chunks).strip()

    if lang == "en":
        prompt = (
            "You extract follow-up todo proposals from a completed multi-agent task.\n"
            "Return JSON only. No markdown, no prose.\n"
            "Schema:\n"
            "{\n"
            "  \"proposals\": [\n"
            "    {\"summary\":\"...\", \"priority\":\"P1|P2|P3\", \"kind\":\"followup|risk|debt|handoff\", \"reason\":\"...\", \"confidence\":0.0}\n"
            "  ]\n"
            "}\n"
            "Rules:\n"
            "- Propose only NEW actionable follow-up tasks.\n"
            "- Do not restate the original task, completed work, or pure notes.\n"
            "- Max 5 proposals.\n"
            "- Use an empty list if no follow-up work is needed.\n\n"
            f"User request:\n{mask_sensitive_text(user_prompt.strip())}\n\n"
            + (f"Task context:\n{task_hint}\n\n" if task_hint else "")
            + f"Agent replies:\n{joined}\n"
        )
    else:
        prompt = (
            "너는 완료된 멀티에이전트 작업 결과에서 후속 todo proposal만 추출한다.\n"
            "반드시 JSON만 출력한다. 마크다운/설명문 금지.\n"
            "스키마:\n"
            "{\n"
            "  \"proposals\": [\n"
            "    {\"summary\":\"...\", \"priority\":\"P1|P2|P3\", \"kind\":\"followup|risk|debt|handoff\", \"reason\":\"...\", \"confidence\":0.0}\n"
            "  ]\n"
            "}\n"
            "규칙:\n"
            "- 새로운 실행형 후속 작업만 제안한다.\n"
            "- 원래 작업의 재진술, 이미 끝난 일, 단순 메모/관찰은 제외한다.\n"
            "- 최대 5개.\n"
            "- 후속 작업이 없으면 빈 배열을 반환한다.\n\n"
            f"사용자 요청:\n{mask_sensitive_text(user_prompt.strip())}\n\n"
            + (f"작업 문맥:\n{task_hint}\n\n" if task_hint else "")
            + f"에이전트 응답:\n{joined}\n"
        )

    try:
        raw = run_codex_exec(
            args,
            prompt,
            timeout_sec=min(180, max(60, int(getattr(args, "orch_command_timeout_sec", DEFAULT_ORCH_COMMAND_TIMEOUT_SEC) or DEFAULT_ORCH_COMMAND_TIMEOUT_SEC) // 4)),
        )
    except Exception:
        return []

    parsed = parse_json_object_from_text(raw)
    if not isinstance(parsed, dict):
        return []
    rows = parsed.get("proposals")
    if not isinstance(rows, list):
        return []

    user_key = re.sub(r"\s+", " ", str(user_prompt or "").strip()).lower()
    seen: Set[str] = set()
    normalized: List[Dict[str, Any]] = []
    for row in rows[:8]:
        if not isinstance(row, dict):
            continue
        summary = " ".join(str(row.get("summary", "")).strip().split())
        if not summary:
            continue
        summary_key = re.sub(r"\s+", " ", summary).lower()
        if not summary_key or summary_key == user_key or summary_key in seen:
            continue
        seen.add(summary_key)
        priority = str(row.get("priority", "P2")).strip().upper() or "P2"
        if priority not in {"P1", "P2", "P3"}:
            priority = "P2"
        kind = str(row.get("kind", "followup")).strip().lower() or "followup"
        if kind not in {"followup", "risk", "debt", "handoff"}:
            kind = "followup"
        reason = " ".join(str(row.get("reason", "")).strip().split())
        try:
            confidence = float(row.get("confidence", 0.0) or 0.0)
        except Exception:
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))
        normalized.append(
            {
                "summary": summary[:600],
                "priority": priority,
                "kind": kind,
                "reason": reason[:240],
                "confidence": confidence,
            }
        )
        if len(normalized) >= 5:
            break

    return normalized


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


def tf_worker_specs(args: argparse.Namespace, request_id: str, roles: List[str], startup_timeout_sec: int) -> List[Dict[str, str]]:
    args._aoe_default_tf_worker_session_prefix = DEFAULT_TF_WORKER_SESSION_PREFIX
    return tf_exec_mod.tf_worker_specs(args, request_id, roles, startup_timeout_sec)


def preview_tf_worker_sessions(args: argparse.Namespace, request_id: str, roles: List[str], startup_timeout_sec: int) -> Dict[str, Any]:
    args._aoe_default_tf_worker_session_prefix = DEFAULT_TF_WORKER_SESSION_PREFIX
    return tf_exec_mod.preview_tf_worker_sessions(args, request_id, roles, startup_timeout_sec)


def spawn_tf_worker_sessions(args: argparse.Namespace, request_id: str, roles: List[str], startup_timeout_sec: int) -> Dict[str, Any]:
    args._aoe_default_tf_worker_session_prefix = DEFAULT_TF_WORKER_SESSION_PREFIX
    return tf_exec_mod.spawn_tf_worker_sessions(args, request_id, roles, startup_timeout_sec, run_command=run_command)


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


def ensure_tf_exec_workspace(args: argparse.Namespace, request_id: str) -> Dict[str, Any]:
    return tf_exec_mod.ensure_tf_exec_workspace(
        args,
        request_id,
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
) -> Dict[str, Any]:
    return tf_exec_mod.run_aoe_orch(
        args,
        prompt,
        chat_id,
        default_tf_exec_mode=DEFAULT_TF_EXEC_MODE,
        default_tf_work_root_name=DEFAULT_TF_WORK_ROOT_NAME,
        default_tf_exec_map_file=DEFAULT_TF_EXEC_MAP_FILE,
        default_tf_worker_startup_grace_sec=DEFAULT_TF_WORKER_STARTUP_GRACE_SEC,
        now_iso=now_iso,
        run_command=run_command,
        roles_override=roles_override,
        priority_override=priority_override,
        timeout_override=timeout_override,
        no_wait_override=no_wait_override,
    )


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
    cmd = [
        args.aoe_orch_bin,
        "status",
        "--project-root",
        str(args.project_root),
        "--team-dir",
        str(args.team_dir),
    ]
    proc = run_command(cmd, env=None, timeout_sec=60)
    text = (proc.stdout or proc.stderr or "").strip()
    poll_summary = summarize_gateway_poll_state(getattr(args, "state_file", None))
    if proc.returncode != 0:
        # Status should never crash the gateway; show best-effort diagnostics + gateway poll state.
        lines: List[str] = []
        if text:
            lines.append(text[:2000])
        else:
            lines.append(f"[ERROR] aoe-orch status failed (exit={proc.returncode})")

        low = text.lower()
        if "config not found" in low or "orchestrator.json" in low:
            lines.append("")
            lines.append("[HINT] orch is registered but not initialized for this project.")
            lines.append("[HINT] fix (telegram): !orch add <name> --path <project_root>")
            lines.append(f"[HINT] missing: {Path(str(args.team_dir)) / 'orchestrator.json'}")

        if poll_summary:
            lines.append("")
            lines.append(poll_summary)
        return "\n".join(lines).strip()
    if text:
        return f"{text}\n\n{poll_summary}"
    return poll_summary


def run_request_query(args: argparse.Namespace, request_id: str) -> Dict[str, Any]:
    cmd = [
        args.aoe_team_bin,
        "request",
        "--request-id",
        request_id,
        "--json",
    ]
    env = os.environ.copy()
    env["AOE_TEAM_DIR"] = str(args.team_dir)

    proc = run_command(cmd, env=env, timeout_sec=60)
    payload = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        raise RuntimeError(f"aoe-team request failed: {payload[:1200]}")

    try:
        data = json.loads(payload)
    except Exception as e:
        raise RuntimeError(f"aoe-team request returned non-JSON output: {payload[:800]}") from e

    if not isinstance(data, dict):
        raise RuntimeError("aoe-team request JSON is not an object")
    return data


def run_message_fail(
    args: argparse.Namespace,
    message_id: str,
    actor: str,
    note: str,
) -> Tuple[bool, str]:
    cmd = [
        args.aoe_team_bin,
        "fail",
        message_id,
        "--force",
        "--note",
        note,
    ]
    if actor:
        cmd.extend(["--for", actor])

    env = os.environ.copy()
    env["AOE_TEAM_DIR"] = str(args.team_dir)

    proc = run_command(cmd, env=env, timeout_sec=60)
    payload = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        return False, payload
    return True, payload


def run_message_done(
    args: argparse.Namespace,
    message_id: str,
    actor: str,
    note: str,
) -> Tuple[bool, str]:
    cmd = [
        args.aoe_team_bin,
        "done",
        message_id,
        "--note",
        note,
    ]
    if actor:
        cmd.extend(["--for", actor])

    env = os.environ.copy()
    env["AOE_TEAM_DIR"] = str(args.team_dir)

    proc = run_command(cmd, env=env, timeout_sec=60)
    payload = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        return False, payload
    return True, payload


def finalize_request_reply_messages(
    args: argparse.Namespace,
    request_id: str,
    actor: str = "Orchestrator",
    note: str = "gateway integrated reply into final response",
) -> Dict[str, Any]:
    state = run_request_query(args, request_id)
    replies = state.get("reply_messages") or []
    targets: List[Tuple[str, str, str]] = []
    skipped: List[str] = []

    for row in replies:
        if not isinstance(row, dict):
            continue
        message_id = str(row.get("id", "")).strip()
        status = str(row.get("status", "")).strip().lower()
        sender = str(row.get("from", "")).strip() or "?"
        if not message_id:
            skipped.append(f"{sender}(no_id)")
            continue
        if status in {"done", "failed"}:
            skipped.append(f"{sender}:{message_id}:{status}")
            continue
        targets.append((message_id, sender, status or "sent"))

    completed: List[str] = []
    failed: List[str] = []
    for message_id, sender, status in targets:
        ok, detail = run_message_done(args, message_id=message_id, actor=actor, note=note)
        label = f"{sender}:{message_id}:{status}"
        if ok:
            completed.append(label)
        else:
            failed.append(f"{label}:{detail[:120]}")

    return {
        "request_id": str(request_id or "").strip(),
        "targets": len(targets),
        "done": completed,
        "failed": failed,
        "skipped": skipped,
    }


def cancel_request_assignments(
    args: argparse.Namespace,
    request_data: Dict[str, Any],
    note: str,
) -> Dict[str, Any]:
    roles = request_data.get("roles") or []
    targets: List[Tuple[str, str, str]] = []
    skipped: List[str] = []

    for row in roles:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", "")).strip()
        status = str(row.get("status", "")).strip().lower()
        message_id = str(row.get("message_id", "")).strip()
        if not message_id:
            skipped.append(f"{role or '?'}(no_message_id)")
            continue
        if status in {"done", "failed", "error", "fail"}:
            skipped.append(f"{role or '?'}({status or 'terminal'})")
            continue
        targets.append((role, status, message_id))

    canceled: List[str] = []
    failed: List[str] = []
    for role, status, message_id in targets:
        ok, detail = run_message_fail(args, message_id=message_id, actor=role, note=note)
        label = f"{role or '?'}:{message_id}:{status or 'pending'}"
        if ok:
            canceled.append(label)
        else:
            failed.append(f"{label}:{detail[:120]}")

    return {
        "request_id": str(request_data.get("request_id", "")).strip(),
        "targets": len(targets),
        "canceled": canceled,
        "failed": failed,
        "skipped": skipped,
    }


def summarize_cancel_result(
    project_name: str,
    request_id: str,
    task: Optional[Dict[str, Any]],
    result: Dict[str, Any],
) -> str:
    label = task_display_label(task or {}, fallback_request_id=request_id)
    targets = int(result.get("targets", 0) or 0)
    canceled = result.get("canceled") or []
    failed = result.get("failed") or []
    skipped = result.get("skipped") or []
    lines = [
        f"orch: {project_name}",
        f"task: {label}",
        f"request_id: {request_id}",
        f"cancel: targets={targets} canceled={len(canceled)} failed={len(failed)} skipped={len(skipped)}",
    ]
    if canceled:
        lines.append("canceled_roles: " + ", ".join(canceled[:6]))
    if failed:
        lines.append("cancel_failures: " + ", ".join(failed[:4]))
    if skipped:
        lines.append("skipped: " + ", ".join(skipped[:6]))
    return "\n".join(lines)


def summarize_state(state: Dict[str, Any]) -> str:
    request_id = str(state.get("request_id", "-"))
    complete = bool(state.get("complete", False))
    timed_out = bool(state.get("timed_out", False))
    roles = state.get("role_states") or state.get("roles") or []
    replies = state.get("replies") or []

    lines: List[str] = []
    lines.append(f"request_id: {request_id}")
    lines.append(f"complete: {'yes' if complete else 'no'}")
    if "timed_out" in state:
        lines.append(f"timed_out: {'yes' if timed_out else 'no'}")
    if "elapsed_sec" in state:
        lines.append(f"elapsed_sec: {state.get('elapsed_sec')}")

    if roles:
        lines.append("")
        lines.append("roles")
        for row in roles:
            role = str(row.get("role", "?"))
            status = str(row.get("status", "?"))
            mid = str(row.get("message_id", ""))
            lines.append(f"- {role}: {status} {mid}")

    if replies:
        lines.append("")
        lines.append("latest replies")
        for r in replies[:6]:
            role = str(r.get("role", r.get("from", "?")))
            body = str(r.get("body", "")).replace("\n", " ").strip()
            if len(body) > 220:
                body = body[:217] + "..."
            if body:
                lines.append(f"- {role}: {body}")

    if not complete:
        lines.append("")
        lines.append(f"hint: /request {request_id}")

    return "\n".join(lines)


def render_run_response(
    state: Dict[str, Any],
    task: Optional[Dict[str, Any]] = None,
    report_level: str = DEFAULT_REPORT_LEVEL,
) -> str:
    request_id = str(state.get("request_id", "-")).strip() or "-"
    row = task or {}
    label = task_display_label(row, fallback_request_id=request_id)
    task_ref = str(row.get("short_id") or row.get("alias") or row.get("request_id") or request_id).strip() or request_id
    complete = bool(state.get("complete", False))
    replies = state.get("replies") or []

    rendered: List[Tuple[str, str]] = []
    for item in replies:
        role = str(item.get("role", item.get("from", "assistant"))).strip() or "assistant"
        body = str(item.get("body", "")).strip()
        if body:
            rendered.append((role, body))

    level = str(report_level or DEFAULT_REPORT_LEVEL).strip().lower()
    if level not in {"short", "normal", "long"}:
        level = DEFAULT_REPORT_LEVEL

    if level == "short":
        if not complete:
            return f"접수: {label}\n다음: /check {task_ref} | /task {task_ref} | /monitor"
        status = str((row.get("status") if isinstance(row, dict) else "") or "completed").strip().lower() or "completed"
        if bool(state.get("timed_out", False)):
            status = "timed_out"
        return f"완료: {label}\n상태: {status}\n상세: /task {task_ref} (또는 /request {task_ref})"

    if complete and rendered:
        if level != "long" and len(rendered) == 1:
            return rendered[0][1]

        lines: List[str] = []
        if level == "long":
            lines.append(f"task: {label}")
            lines.append(f"request_id: {request_id}")
            lines.append("")
        for role, body in rendered[:6]:
            lines.append(f"[{role}]")
            lines.append(body)
            lines.append("")
        return "\n".join(lines).strip()

    if not complete:
        if level == "long":
            return f"task: {label}\n{summarize_state(state)}"
        return f"작업 접수됨: {label}\n진행: 진행 {label}\n상세: 상세 {label}"

    return f"작업 완료: {label}\n(에이전트 본문 응답이 아직 없습니다)"



def summarize_request_state(state: Dict[str, Any], task: Optional[Dict[str, Any]] = None) -> str:
    request_id = str(state.get("request_id", "-"))
    counts = state.get("counts") or {}
    roles = state.get("roles") or []
    unresolved = state.get("unresolved_roles") or []

    lines: List[str] = []
    lines.append(f"task: {task_display_label(task or {}, fallback_request_id=request_id)}")
    lines.append(f"request_id: {request_id}")
    lines.append(
        "counts: messages={m} assignments={a} replies={r}".format(
            m=counts.get("messages", 0),
            a=counts.get("assignments", 0),
            r=counts.get("replies", 0),
        )
    )
    lines.append(f"complete: {'yes' if state.get('complete') else 'no'}")

    if roles:
        lines.append("")
        lines.append("roles")
        for row in roles:
            lines.append(f"- {row.get('role')}: {row.get('status')} {row.get('message_id')}")

    if unresolved:
        lines.append("")
        lines.append("unresolved: " + ", ".join(str(x) for x in unresolved))

    return "\n".join(lines)



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
        "- /retry <request_or_alias>\n"
        "- /replan <request_or_alias>\n"
        "- /request <request_or_alias>\n"
        "- /run <prompt>\n"
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
        "- aoe retry <request_or_alias>\n"
        "- aoe replan <request_or_alias>\n"
        "- aoe request <request_or_alias>\n"
        "- aoe run [--direct|--dispatch] [--roles <csv>] [--priority P1|P2|P3] [--timeout-sec N] [--no-wait] <prompt>\n"
        "- aoe add-role <Role> [--provider <name>] [--launch <cmd>] [--spawn|--no-spawn]\n"
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
    """Parse /drain arguments.

    Supported:
    - /drain            -> default limit
    - /drain 5          -> run up to 5 items
    - /drain 20 force   -> ignore busy checks (same as /next force)
    """
    tokens = [t for t in str(rest or "").split() if t.strip()]
    force = any(t.lower() in {"force", "!", "--force"} for t in tokens)
    limit = 10
    for t in tokens:
        if t.isdigit():
            limit = int(t)
            break
        low = t.lower()
        if low in {"all", "*", "until-empty", "until_empty"}:
            limit = 9999
            break
    # Keep bounded by default for safety.
    limit = max(1, min(50, int(limit)))
    return limit, force


def _parse_fanout_args(rest: str) -> tuple[int, bool]:
    """Parse /fanout arguments.

    Supported:
    - /fanout            -> run one todo per project (bounded)
    - /fanout 5          -> run up to 5 projects
    - /fanout force      -> ignore busy/pending checks (same semantics as /todo next force)
    """
    tokens = [t for t in str(rest or "").split() if t.strip()]
    force = any(t.lower() in {"force", "!", "--force"} for t in tokens)
    limit = 9999
    for t in tokens:
        if t.isdigit():
            limit = int(t)
            break
    # Keep bounded by default for safety (projects can be many, and each run can be long).
    limit = max(1, min(50, int(limit)))
    return limit, force


def _drain_peek_next_todo(
    manager_state: Dict[str, Any],
    chat_id: str,
    *,
    force: bool,
) -> tuple[str, str, str]:
    return queue_drain_peek_next_todo(manager_state, chat_id, force=force)


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
    limit, force = _parse_drain_args(rest)
    force_token = " force" if force else ""
    send(
        "drain started\n"
        f"- limit: {limit}\n"
        f"- force: {'yes' if force else 'no'}\n"
        "next:\n"
        "- /queue (overview)\n"
        "- /next (single step)\n"
        "- /cancel (pending-mode only)\n",
        context="drain-start",
        with_menu=True,
    )
    log_event(event="drain_start", status="running", stage="intake", detail=f"limit={limit} force={'yes' if force else 'no'}")

    executed = 0
    stop_reason = ""

    for i in range(int(limit)):
        # Avoid chaining when a confirm is pending.
        manager_state = load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
        if get_confirm_action(manager_state, chat_id):
            stop_reason = "confirm_pending"
            break

        if not args.dry_run:
            project_key, todo_id, reason = _drain_peek_next_todo(manager_state, chat_id, force=force)
            if not project_key or not todo_id:
                stop_reason = reason
                break
        else:
            # Dry-run cannot reliably observe todo state transitions; just run the loop.
            project_key, todo_id, reason = "-", "-", "dry_run"

        # Execute one scheduling step using the real /next handler.
        handle_text_message(
            args=args,
            token=token,
            chat_id=chat_id,
            text=f"/next{force_token}",
            trace_id=f"{trace_id}/drain-{i+1}",
        )
        executed += 1

        if args.dry_run:
            continue

        manager_state = load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
        if get_confirm_action(manager_state, chat_id):
            stop_reason = "confirm_pending"
            break

        projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
        entry = projects.get(project_key) if isinstance(projects, dict) and isinstance(projects.get(project_key), dict) else {}
        todos = entry.get("todos") if isinstance(entry, dict) else None
        status = ""
        if isinstance(todos, list):
            for row in todos:
                if not isinstance(row, dict):
                    continue
                if str(row.get("id", "")).strip() != str(todo_id).strip():
                    continue
                status = str(row.get("status", "")).strip().lower()
                break
        if status in {"blocked", "running", "open"}:
            stop_reason = f"todo_{status or 'unknown'}"
            break

    if not stop_reason:
        stop_reason = "limit_reached" if executed >= limit else "done"
    send(
        build_batch_finish_message(
            title="drain finished",
            executed=executed,
            reason=stop_reason,
            next_lines=["- /queue", "- /next"],
        ),
        context="drain-finish",
        with_menu=True,
    )
    log_event(event="drain_finish", status="completed", stage="close", detail=f"executed={executed} reason={stop_reason}")


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
    """Run a single "fanout" wave: at most one todo per project.

    This is a fairness-oriented batch mode:
    - For each registered orch project (O1..), attempt `/todo O# next`
    - Skip projects with pending/running todo unless forced
    - Blocked todos remain visible, but do not freeze other open todos in the same project
    - Runs are sequential (safe) and use the regular run pipeline
    """
    limit, force = _parse_fanout_args(rest)
    force_token = " force" if force else ""

    manager_state = load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
    locked = project_lock_label(manager_state)
    if locked:
        send(
            "fanout blocked by project lock\n"
            f"- project_lock: {locked}\n"
            "- reason: fanout is a global multi-project wave\n"
            "next:\n"
            "- /next\n"
            "- /auto on next\n"
            "- /offdesk on\n"
            "- /focus off",
            context="fanout-locked",
            with_menu=True,
        )
        log_event(event="fanout_finish", status="rejected", stage="intake", detail=f"project_lock={locked}")
        return

    send(
        "fanout started\n"
        f"- max_projects: {limit}\n"
        f"- force: {'yes' if force else 'no'}\n"
        "rule: at most 1 todo per project\n"
        "next:\n"
        "- /queue (overview)\n"
        "- /fanout (one wave)\n"
        "- /auto on fanout (continuous)\n",
        context="fanout-start",
        with_menu=True,
    )
    log_event(event="fanout_start", status="running", stage="intake", detail=f"limit={limit} force={'yes' if force else 'no'}")

    executed = 0
    counters = new_ops_skip_counters()
    stop_reason = ""

    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    if not isinstance(projects, dict) or not projects:
        send("fanout: no orch projects registered. use /map and /orch add first.", context="fanout-empty", with_menu=True)
        log_event(event="fanout_finish", status="completed", stage="close", detail="no_projects")
        return

    # Ensure aliases are available for stable ordering and /todo O# override.
    alias_map = ensure_project_aliases(manager_state)
    _ = alias_map  # keep visible for debugging, even if unused below.

    def _proj_sort_key(k: str) -> tuple[int, str]:
        entry = projects.get(k) if isinstance(projects.get(k), dict) else {}
        alias = normalize_project_alias(str((entry or {}).get("project_alias", ""))) or "O?"
        return (extract_project_alias_index(alias), str(k))

    ordered_keys = sorted(
        [str(k) for k, entry in projects.items() if isinstance(entry, dict) and not project_hidden_from_ops(entry)],
        key=_proj_sort_key,
    )

    for idx, project_key in enumerate(ordered_keys[: int(limit)], start=1):
        # Avoid chaining when a confirm is pending (operator gate).
        manager_state = load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
        if get_confirm_action(manager_state, chat_id):
            stop_reason = "confirm_pending"
            break

        projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
        entry = projects.get(project_key) if isinstance(projects, dict) and isinstance(projects.get(project_key), dict) else {}

        if project_hidden_from_ops(entry if isinstance(entry, dict) else {}):
            continue
        if (not force) and bool_from_json((entry or {}).get("paused"), False):
            counters["paused"] += 1
            continue
        if project_runtime_issue(entry if isinstance(entry, dict) else {}):
            counters["unready"] += 1
            continue

        alias = normalize_project_alias(str((entry or {}).get("project_alias", ""))) or project_alias_for_key(manager_state, project_key)
        if not alias:
            counters["missing_alias"] += 1
            continue

        pending = entry.get("pending_todo") if isinstance(entry, dict) else None
        if (not force) and isinstance(pending, dict) and str(pending.get("todo_id", "")).strip():
            counters["pending"] += 1
            continue

        snap = project_queue_snapshot(entry if isinstance(entry, dict) else {})
        open_cnt = int(snap["open_count"])
        busy_cnt = int(snap["running_count"])

        if open_cnt <= 0:
            counters["empty"] += 1
            continue
        if (not force) and busy_cnt > 0:
            counters["busy"] += 1
            continue

        # Execute one per project using the real /todo handler (which dispatches into run pipeline).
        handle_text_message(
            args=args,
            token=token,
            chat_id=chat_id,
            text=f"/todo {alias} next{force_token}",
            trace_id=f"{trace_id}/fanout-{idx}",
        )
        executed += 1

        # If fanout just created a confirm-pending gate, stop to avoid spamming.
        if not args.dry_run:
            manager_state = load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
            if get_confirm_action(manager_state, chat_id):
                stop_reason = "confirm_pending"
                break

    if not stop_reason:
        stop_reason = "done"

    send(
        build_batch_finish_message(
            title="fanout finished",
            executed=executed,
            reason=stop_reason,
            counters=counters,
            next_lines=["- /queue", "- /fanout"],
        ),
        context="fanout-finish",
        with_menu=True,
    )
    log_event(
        event="fanout_finish",
        status="completed",
        stage="close",
        detail=(
            f"executed={executed} reason={stop_reason} {format_ops_skip_detail(counters)}"
        ),
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
    tokens = [t for t in str(rest or "").split() if t.strip()]
    sub = (tokens[0].lower() if tokens else "run").strip()
    force = any(t.lower() in {"force", "!", "--force"} for t in tokens[1:]) or (sub in {"force"} and len(tokens) == 1)

    retention_days = room_retention_days()
    ttl_hours = tf_exec_cache_ttl_hours()
    retention_policy = normalize_tf_exec_retention()

    if sub in {"status", "show"}:
        send(
            "gc policy\n"
            f"- room_retention_days: {retention_days} (0 disables)\n"
            f"- tf_artifact_policy: {retention_policy}\n"
            f"- tf_exec_cache_ttl_hours: {ttl_hours} (0 disables; ignored when policy=all)\n"
            "run:\n"
            "- /gc\n"
            "- /gc force",
            context="gc-status",
            with_menu=True,
        )
        return

    if bool(getattr(args, "dry_run", False)):
        send(
            "gc skipped (dry-run)\n"
            f"- room_retention_days: {retention_days}\n"
            f"- tf_artifact_policy: {retention_policy}\n"
            f"- tf_exec_cache_ttl_hours: {ttl_hours}\n"
            "run:\n"
            "- /gc status",
            context="gc-dry-run",
            with_menu=True,
        )
        return

    removed_rooms = cleanup_room_logs(args.team_dir, force=force)
    removed_tf = cleanup_tf_exec_artifacts(args.manager_state_file, manager_state)
    log_event(
        event="gc",
        stage="close",
        status="completed",
        detail=f"room_removed={removed_rooms} tf_removed={removed_tf} force={'yes' if force else 'no'}",
    )
    send(
        "gc complete\n"
        f"- room_removed: {removed_rooms}\n"
        f"- tf_exec_removed: {removed_tf}\n"
        f"- room_retention_days: {retention_days}\n"
        f"- tf_artifact_policy: {retention_policy}\n"
        f"- tf_exec_cache_ttl_hours: {ttl_hours}\n"
        f"- force: {'yes' if force else 'no'}\n"
        "next:\n"
        "- /status\n"
        "- /queue\n"
        "- /room tail 20",
        context="gc",
        with_menu=True,
    )


def handle_text_message(
    args: argparse.Namespace,
    token: str,
    chat_id: str,
    text: str,
    trace_id: str = "",
) -> None:
    started_at = time.time()
    message_trace_id = str(trace_id or "").strip() or f"chat-{chat_id}-{int(started_at * 1000)}"
    # Expose invocation info to handlers (ex: avoid auto-scheduler polluting chat history).
    # This is best-effort and never blocks message handling.
    try:
        args._aoe_trace_id = message_trace_id
        args._aoe_invocation = "auto" if message_trace_id.startswith("auto-") else "chat"
    except Exception:
        pass
    raw_text = str(text or "")
    text_preview = raw_text if len(raw_text) <= 200 else raw_text[:197] + "..."
    text_preview = mask_sensitive_text(text_preview)
    resolved = ResolvedCommand()
    run_transition = RunTransitionState()

    manager_state = load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
    ensure_default_project_registered(manager_state, args.project_root, args.team_dir)
    # Optional: make owner UX "just type" by setting a default routing mode automatically
    # the first time the owner sends a message (useful in slash-only mode).
    try:
        owner_bootstrap_mode = str(getattr(args, "owner_bootstrap_mode", "") or "").strip().lower()
        if owner_bootstrap_mode and is_owner_chat(chat_id, args):
            if not get_default_mode(manager_state, chat_id):
                set_default_mode(manager_state, chat_id, owner_bootstrap_mode)
                if not args.dry_run:
                    save_manager_state(args.manager_state_file, manager_state)
    except Exception:
        # Never block message handling on bootstrap conveniences.
        pass
    default_log_team_dir = args.team_dir
    root_log_team_dir = Path(str(args.team_dir)).expanduser().resolve()
    try:
        _key0, _entry0 = get_manager_project(manager_state, None)
        default_log_team_dir = Path(str(_entry0.get("team_dir", str(args.team_dir)))).expanduser().resolve()
    except Exception:
        default_log_team_dir = args.team_dir
    log_ctx: Dict[str, Path] = {"team_dir": default_log_team_dir}

    def elapsed_ms() -> int:
        return max(0, int((time.time() - started_at) * 1000))

    def log_event(
        event: str,
        project: str = "",
        request_id: str = "",
        task: Optional[Dict[str, Any]] = None,
        stage: str = "",
        status: str = "",
        error_code: str = "",
        detail: str = "",
    ) -> None:
        if args.dry_run:
            return
        log_gateway_event(
            team_dir=log_ctx["team_dir"],
            mirror_team_dir=root_log_team_dir,
            event=event,
            trace_id=message_trace_id,
            project=project,
            request_id=request_id,
            task=task,
            stage=stage,
            actor=f"telegram:{chat_id}",
            status=status,
            error_code=error_code,
            latency_ms=elapsed_ms(),
            detail=detail,
        )
        try:
            room_autopublish_event(
                team_dir=args.team_dir,
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
            )
        except Exception:
            pass

    def send(
        body: str,
        context: str = "",
        with_menu: bool = False,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> bool:
        retries = int_from_env(os.environ.get("AOE_TG_SEND_RETRIES"), default=2, minimum=0, maximum=8)
        base_delay_ms = int_from_env(os.environ.get("AOE_TG_SEND_RETRY_DELAY_MS"), default=300, minimum=50, maximum=5000)
        attempt = 0
        ok = False
        if reply_markup is None and with_menu:
            reply_markup = build_quick_reply_keyboard()
        while True:
            attempt += 1
            ok = safe_tg_send_text(
                token=token,
                chat_id=chat_id,
                text=body,
                max_chars=args.max_text_chars,
                timeout_sec=args.http_timeout_sec,
                dry_run=args.dry_run,
                verbose=args.verbose,
                context=context,
                reply_markup=reply_markup,
            )
            if ok or attempt > retries:
                break
            delay = (base_delay_ms * (2 ** (attempt - 1))) / 1000.0
            time.sleep(min(8.0, delay))
        log_event(
            event="send_message",
            status="sent" if ok else "failed",
            error_code="" if ok else ERROR_TELEGRAM,
            detail=(
                f"context={context} with_menu={'yes' if with_menu else 'no'} "
                f"chars={len(str(body or ''))} attempts={attempt}"
            ),
        )
        return ok

    def get_context(name_override: Optional[str]) -> Tuple[str, Dict[str, Any], argparse.Namespace]:
        key, entry = get_manager_project(manager_state, name_override)
        p_args = make_project_args(args, entry, key=key)
        log_ctx["team_dir"] = p_args.team_dir
        return key, entry, p_args

    def _skip_synth() -> str:
        raise RuntimeError("synth disabled by report_level")

    try:
        log_event(event="incoming_message", status="received", stage="intake", detail=text_preview)
        resolved = resolve_message_command(
            text=text,
            slash_only=bool(args.slash_only),
            manager_state=manager_state,
            chat_id=chat_id,
            dry_run=bool(args.dry_run),
            manager_state_file=args.manager_state_file,
            get_pending_mode=get_pending_mode,
            get_default_mode=get_default_mode,
            clear_pending_mode=clear_pending_mode,
            save_manager_state=save_manager_state,
        )
        chat_ui_lang = get_chat_lang(manager_state, chat_id, str(args.default_lang))
        chat_report_level = get_chat_report_level(
            manager_state,
            chat_id,
            str(getattr(args, "default_report_level", DEFAULT_REPORT_LEVEL) or DEFAULT_REPORT_LEVEL),
        )

        if not resolved.cmd and bool(args.slash_only):
            p = preferred_command_prefix()
            if chat_ui_lang == "en":
                slash_hint = (
                    "Input format: command-prefix only.\n"
                    f"Example: {p}dispatch <request>, {p}direct <question>, {p}mode on, {p}lang en, {p}monitor, {p}check, {p}task, {p}pick, {p}map, {p}help\n"
                    f"Tip: {p}dispatch or {p}direct enables one-shot plain text for the next message; {p}mode sets default plain-text routing."
                )
            else:
                slash_hint = (
                    "입력 형식: prefix 명령만 지원합니다.\n"
                    f"예시: {p}dispatch <요청>, {p}direct <질문>, {p}mode on, {p}lang en, {p}monitor, {p}check, {p}task, {p}pick, {p}map, {p}help\n"
                    f"참고: {p}dispatch 또는 {p}direct는 다음 메시지 1회 평문 허용, {p}mode는 기본 평문 라우팅 모드를 고정합니다."
                )
            send(
                slash_hint,
                context="slash-only-hint",
                with_menu=True,
            )
            log_event(event="input_rejected", stage="intake", status="rejected", error_code=ERROR_COMMAND, detail="slash_only")
            return

        cmd_key = resolved.cmd or "run-default"
        if cmd_key == "replay":
            replay_scope = str(resolved.rest or "").strip().lower()
            replay_action = replay_scope.split(" ", 1)[0] if replay_scope else ""
            if replay_action in {"", "list", "ls", "status", "show"}:
                cmd_key = "replay-read"
            else:
                cmd_key = "replay-write"
        log_event(event="command_resolved", stage="intake", status="accepted", detail=f"cmd={cmd_key}")

        chat_role = resolve_chat_role(chat_id, args)
        if enforce_command_auth(
            cmd_key=cmd_key,
            chat_role=chat_role,
            chat_id=chat_id,
            args=args,
            send=send,
            log_event=log_event,
            is_owner_chat=is_owner_chat,
            readonly_allowed_commands=READONLY_ALLOWED_COMMANDS,
            error_auth_code=ERROR_AUTH,
        ):
            return

        current_chat_alias = ensure_chat_alias(args, chat_id, persist=(not args.dry_run))
        chat_reply_lang = normalize_chat_lang_token(str(args.default_reply_lang), DEFAULT_REPLY_LANG) or DEFAULT_REPLY_LANG

        if resolved.cmd == "replay":
            handle_replay_command(
                args=args,
                token=token,
                chat_id=chat_id,
                target=resolved.rest,
                send=send,
                log_event=log_event,
            )
            return

        if resolved.cmd == "drain":
            handle_drain_command(
                args=args,
                token=token,
                chat_id=chat_id,
                rest=resolved.rest,
                trace_id=message_trace_id,
                send=send,
                log_event=log_event,
            )
            return

        if resolved.cmd == "fanout":
            handle_fanout_command(
                args=args,
                token=token,
                chat_id=chat_id,
                rest=resolved.rest,
                trace_id=message_trace_id,
                send=send,
                log_event=log_event,
            )
            return

        if resolved.cmd == "gc":
            handle_gc_command(
                args=args,
                chat_id=chat_id,
                rest=resolved.rest,
                manager_state=manager_state,
                send=send,
                log_event=log_event,
            )
            return

        confirm_transition = resolve_confirm_run_transition(
            cmd=resolved.cmd,
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            orch_target=resolved.orch_target,
            send=send,
            get_confirm_action=get_confirm_action,
            parse_iso_ts=parse_iso_ts,
            clear_confirm_action=clear_confirm_action,
            save_manager_state=save_manager_state,
        )
        if apply_confirm_transition_to_resolved(resolved, confirm_transition):
            return

        non_run_ctx = build_non_run_context(
            resolved=resolved,
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            chat_role=chat_role,
            current_chat_alias=current_chat_alias,
        )
        non_run_deps = build_non_run_deps(
            send=send,
            log_event=log_event,
            get_context=get_context,
            save_manager_state=save_manager_state,
            help_text=lambda: help_text(chat_ui_lang),
            get_default_mode=get_default_mode,
            get_pending_mode=get_pending_mode,
            get_chat_lang=get_chat_lang,
            get_chat_report_level=get_chat_report_level,
            get_chat_room=get_chat_room,
            set_default_mode=set_default_mode,
            set_pending_mode=set_pending_mode,
            set_chat_lang=set_chat_lang,
            set_chat_report_level=set_chat_report_level,
            set_chat_room=set_chat_room,
            clear_default_mode=clear_default_mode,
            clear_pending_mode=clear_pending_mode,
            clear_confirm_action=clear_confirm_action,
            clear_chat_report_level=clear_chat_report_level,
            resolve_chat_role=resolve_chat_role,
            is_owner_chat=is_owner_chat,
            ensure_chat_aliases=ensure_chat_aliases,
            find_chat_alias=find_chat_alias,
            alias_table_summary=alias_table_summary,
            resolve_chat_ref=resolve_chat_ref,
            ensure_chat_alias=ensure_chat_alias,
            sync_acl_env_file=sync_acl_env_file,
            summarize_orch_registry=summarize_orch_registry,
            backfill_task_aliases=backfill_task_aliases,
            latest_task_request_refs=latest_task_request_refs,
            set_chat_recent_task_refs=set_chat_recent_task_refs,
            get_chat_selected_task_ref=get_chat_selected_task_ref,
            set_chat_selected_task_ref=set_chat_selected_task_ref,
            summarize_task_monitor=summarize_task_monitor,
            summarize_gateway_metrics=summarize_gateway_metrics,
            get_manager_project=get_manager_project,
            resolve_project_root=resolve_project_root,
            is_path_within=is_path_within,
            register_orch_project=register_orch_project,
            run_aoe_init=run_aoe_init,
            run_aoe_spawn=run_aoe_spawn,
            now_iso=now_iso,
            run_aoe_status=run_aoe_status,
            resolve_chat_task_ref=resolve_chat_task_ref,
            resolve_task_request_id=resolve_task_request_id,
            run_request_query=run_request_query,
            sync_task_lifecycle=sync_task_lifecycle,
            resolve_verifier_candidates=resolve_verifier_candidates,
            touch_chat_recent_task_ref=touch_chat_recent_task_ref,
            get_task_record=get_task_record,
            summarize_request_state=summarize_request_state,
            summarize_three_stage_request=summarize_three_stage_request,
            summarize_task_lifecycle=summarize_task_lifecycle,
            task_display_label=task_display_label,
            cancel_request_assignments=cancel_request_assignments,
            lifecycle_set_stage=lifecycle_set_stage,
            summarize_cancel_result=summarize_cancel_result,
            dedupe_roles=dedupe_roles,
            run_aoe_add_role=run_aoe_add_role,
        )
        non_run_result = handle_non_run_command_pipeline(
            ctx=non_run_ctx,
            deps=non_run_deps,
        )
        if non_run_result.terminal:
            return
        if apply_retry_transition_to_resolved(resolved, run_transition, non_run_result.retry_transition):
            return

        run_ctx = build_run_context(
            cmd=resolved.cmd,
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            text=text,
            rest=resolved.rest,
            orch_target=resolved.orch_target,
            run_prompt=resolved.run_prompt,
            run_roles_override=resolved.run_roles_override,
            run_priority_override=resolved.run_priority_override,
            run_timeout_override=resolved.run_timeout_override,
            run_no_wait_override=resolved.run_no_wait_override,
            run_force_mode=resolved.run_force_mode,
            run_auto_source=resolved.run_auto_source,
            run_control_mode=run_transition.run_control_mode,
            run_source_request_id=run_transition.run_source_request_id,
            run_source_task=run_transition.run_source_task,
        )
        run_deps = build_run_deps(
            send=send,
            log_event=log_event,
            help_text=lambda: help_text(chat_ui_lang),
            summarize_chat_usage=summarize_chat_usage,
            detect_high_risk_prompt=detect_high_risk_prompt,
            set_confirm_action=set_confirm_action,
            save_manager_state=save_manager_state,
            get_context=get_context,
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
            run_orchestrator_direct=lambda p_args, prompt: run_orchestrator_direct(
                p_args,
                prompt,
                reply_lang=chat_reply_lang,
            ),
            run_aoe_orch=run_aoe_orch,
            finalize_request_reply_messages=finalize_request_reply_messages,
            touch_chat_recent_task_ref=touch_chat_recent_task_ref,
            set_chat_selected_task_ref=set_chat_selected_task_ref,
            now_iso=now_iso,
            sync_task_lifecycle=sync_task_lifecycle,
            lifecycle_set_stage=lifecycle_set_stage,
            summarize_task_lifecycle=summarize_task_lifecycle,
            synthesize_orchestrator_response=lambda p_args, prompt, state: synthesize_orchestrator_response(
                p_args,
                prompt,
                state,
                reply_lang=chat_reply_lang,
            )
            if chat_report_level == "normal"
            else _skip_synth(),
            critique_task_result=lambda p_args, prompt, state, task, attempt_no, max_attempts: critique_task_execution_result(
                p_args,
                prompt,
                state,
                task=task,
                attempt_no=attempt_no,
                max_attempts=max_attempts,
                reply_lang=chat_reply_lang,
            ),
            extract_todo_proposals=lambda p_args, prompt, state, task=None: extract_followup_todo_proposals(
                p_args,
                prompt,
                state,
                task=task,
                reply_lang=chat_reply_lang,
            ),
            merge_todo_proposals=merge_todo_proposals,
            render_run_response=lambda state, task=None: render_run_response(
                state,
                task=task,
                report_level=chat_report_level,
            ),
        )

        if handle_run_or_unknown_command(
            ctx=run_ctx,
            deps=run_deps,
        ):
            return

    except Exception as e:
        if getattr(args, "verbose", False):
            try:
                import traceback

                traceback.print_exc()
            except Exception:
                pass
        error_code, user_msg, next_step = classify_handler_error(e)
        replay_hint = ""
        if str(raw_text or "").strip():
            try:
                loop_state = load_state(args.state_file)
                item = enqueue_failed_message(
                    loop_state,
                    chat_id=chat_id,
                    text=raw_text,
                    trace_id=message_trace_id,
                    error_code=error_code,
                    error_detail=str(e),
                    cmd=resolved.cmd,
                )
                save_state(args.state_file, loop_state)
                rid = str(item.get("id", "")).strip()
                if rid:
                    p = preferred_command_prefix()
                    replay_hint = f"\nreplay: {p}replay {rid}"
            except Exception:
                replay_hint = ""
        send(
            format_error_message(error_code, user_msg, next_step, detail=str(e)) + replay_hint,
            context="handler error",
            with_menu=True,
        )
        log_event(
            event="handler_error",
            stage="close",
            status="failed",
            error_code=error_code,
            detail=str(e),
        )


def iter_message_updates(updates: Iterable[Dict[str, Any]]) -> Iterable[Tuple[int, Dict[str, Any]]]:
    for upd in updates:
        if not isinstance(upd, dict):
            continue
        update_id = upd.get("update_id")
        if not isinstance(update_id, int):
            continue
        msg = upd.get("message")
        if isinstance(msg, dict):
            yield update_id, msg


def run_simulation(args: argparse.Namespace, token: str) -> None:
    chat_id = str(args.simulate_chat_id)
    if args.verbose:
        print(f"[SIM] chat_id={chat_id} text={args.simulate_text}")
    original_dry = bool(args.dry_run)
    # Safety default: simulate-text is dry-run unless explicitly enabled.
    if not bool(getattr(args, "simulate_live", False)):
        args.dry_run = True
    try:
        handle_text_message(args, token, chat_id, args.simulate_text, trace_id=f"sim-{int(time.time() * 1000)}")
    finally:
        args.dry_run = original_dry


def run_loop(args: argparse.Namespace, token: str) -> int:
    state = load_state(args.state_file)
    offset = int(state.get("offset", 0) or 0)
    processed = int(state.get("processed", 0) or 0)
    acked_updates = int(state.get(STATE_ACKED_UPDATES_KEY, processed) or 0)
    handled_messages = int(state.get(STATE_HANDLED_MESSAGES_KEY, processed) or 0)
    duplicate_skipped = int(state.get(STATE_DUPLICATE_SKIPPED_KEY, 0) or 0)
    empty_skipped = int(state.get(STATE_EMPTY_SKIPPED_KEY, 0) or 0)
    unauthorized_skipped = int(state.get(STATE_UNAUTHORIZED_SKIPPED_KEY, 0) or 0)
    handler_errors = int(state.get(STATE_HANDLER_ERRORS_KEY, 0) or 0)
    dedup_keep = dedup_keep_limit()
    seen_update_ids = normalize_recent_tokens(state.get(STATE_SEEN_UPDATE_IDS_KEY), dedup_keep)
    seen_message_keys = normalize_recent_tokens(state.get(STATE_SEEN_MESSAGE_KEYS_KEY), dedup_keep)
    seen_update_set = set(seen_update_ids)
    seen_message_set = set(seen_message_keys)
    state[STATE_SEEN_UPDATE_IDS_KEY] = seen_update_ids
    state[STATE_SEEN_MESSAGE_KEYS_KEY] = seen_message_keys
    state["offset"] = offset
    state[STATE_ACKED_UPDATES_KEY] = acked_updates
    state[STATE_HANDLED_MESSAGES_KEY] = handled_messages
    state[STATE_DUPLICATE_SKIPPED_KEY] = duplicate_skipped
    state[STATE_EMPTY_SKIPPED_KEY] = empty_skipped
    state[STATE_UNAUTHORIZED_SKIPPED_KEY] = unauthorized_skipped
    state[STATE_HANDLER_ERRORS_KEY] = handler_errors
    state["processed"] = handled_messages

    unauthorized_sent: Set[str] = set()

    while True:
        try:
            updates = tg_get_updates(
                token=token,
                offset=offset,
                poll_timeout_sec=args.poll_timeout_sec,
                timeout_sec=args.http_timeout_sec,
            )
        except Exception as e:
            if args.verbose:
                print(f"[ERROR] getUpdates failed: {e}", file=sys.stderr, flush=True)
            time.sleep(2)
            continue

        handled_any = False

        for update_id, msg in iter_message_updates(updates):
            handled_any = True
            offset = max(offset, update_id + 1)
            state["offset"] = offset

            chat = msg.get("chat") if isinstance(msg.get("chat"), dict) else {}
            chat_id = str(chat.get("id", ""))
            chat_type = str(chat.get("type", "") or "").strip().lower()
            sender = msg.get("from") if isinstance(msg.get("from"), dict) else {}
            sender_id = str(sender.get("id", ""))
            text = str(msg.get("text", "") or "")
            msg_key = message_dedup_key(msg)
            update_token = str(update_id)
            duplicate = (update_token in seen_update_set) or (bool(msg_key) and msg_key in seen_message_set)

            append_recent_token(seen_update_ids, update_token, dedup_keep)
            seen_update_set.add(update_token)
            if msg_key:
                append_recent_token(seen_message_keys, msg_key, dedup_keep)
                seen_message_set.add(msg_key)
            acked_updates += 1
            state[STATE_ACKED_UPDATES_KEY] = acked_updates

            if duplicate:
                duplicate_skipped += 1
                state[STATE_DUPLICATE_SKIPPED_KEY] = duplicate_skipped
                state["processed"] = handled_messages
                save_state(args.state_file, state)
                if args.verbose:
                    print(f"[SKIP] duplicate update_id={update_id} message_key={msg_key or '-'}")
                if not args.dry_run:
                    log_gateway_event(
                        team_dir=args.team_dir,
                        event="duplicate_update_skipped",
                        trace_id=f"upd-{update_id}",
                        stage="intake",
                        actor=f"telegram:{chat_id or '-'}",
                        status="skipped",
                        detail=f"message_key={msg_key or '-'}",
                    )
                continue

            if not chat_id or not text:
                empty_skipped += 1
                state[STATE_EMPTY_SKIPPED_KEY] = empty_skipped
                state["processed"] = handled_messages
                save_state(args.state_file, state)
                continue

            if args.verbose:
                preview = text if len(text) <= 120 else text[:117] + "..."
                print(f"[UPDATE] update_id={update_id} chat_id={chat_id} text={preview}")

            allowed = False
            if bool(getattr(args, "owner_only", False)):
                owner = str(getattr(args, "owner_chat_id", "") or "").strip()
                allowed = bool(owner) and (chat_type == "private") and (chat_id == owner) and (sender_id == owner)
            else:
                allowed = ensure_chat_allowed(
                    chat_id,
                    args.allow_chat_ids,
                    args.admin_chat_ids,
                    args.readonly_chat_ids,
                    bool(args.deny_by_default),
                    getattr(args, "owner_chat_id", ""),
                )
            bootstrap_allowed = False
            acl_empty = (not args.allow_chat_ids) and (not args.admin_chat_ids) and (not args.readonly_chat_ids)
            if (not allowed) and (not bool(getattr(args, "owner_only", False))) and bool(args.deny_by_default) and acl_empty:
                bootstrap_allowed = is_bootstrap_allowed_command(text)
                if bootstrap_allowed:
                    allowed = True

            if not allowed:
                unauthorized_skipped += 1
                state[STATE_UNAUTHORIZED_SKIPPED_KEY] = unauthorized_skipped
                state["processed"] = handled_messages
                save_state(args.state_file, state)
                if args.verbose:
                    print(f"[SKIP] unauthorized chat_id={chat_id}")
                if chat_id not in unauthorized_sent:
                    unauthorized_text = "not allowed."
                    if bool(getattr(args, "owner_only", False)):
                        unauthorized_text = "not allowed. owner-only mode: DM the bot from the owner account."
                    elif bool(args.deny_by_default) and acl_empty:
                        unauthorized_text = "not allowed. gateway is locked. use /lockme to claim this bot."
                    safe_tg_send_text(
                        token=token,
                        chat_id=chat_id,
                        text=unauthorized_text,
                        max_chars=args.max_text_chars,
                        timeout_sec=args.http_timeout_sec,
                        dry_run=args.dry_run,
                        verbose=args.verbose,
                        context="unauthorized",
                    )
                    log_gateway_event(
                        team_dir=args.team_dir,
                        event="unauthorized_message",
                        trace_id=f"upd-{update_id}",
                        stage="intake",
                        actor=f"telegram:{chat_id}",
                        status="rejected",
                        error_code=ERROR_AUTH,
                        detail=text if len(text) <= 200 else (text[:197] + "..."),
                    )
                    unauthorized_sent.add(chat_id)
                continue

            state["processed"] = handled_messages
            save_state(args.state_file, state)
            try:
                handle_text_message(args, token, chat_id, text, trace_id=f"upd-{update_id}")
            except Exception as e:
                handler_errors += 1
                state[STATE_HANDLER_ERRORS_KEY] = handler_errors
                if args.verbose:
                    print(f"[ERROR] message handling failed: chat_id={chat_id} error={e}", file=sys.stderr, flush=True)
            handled_messages += 1
            processed = handled_messages
            state[STATE_HANDLED_MESSAGES_KEY] = handled_messages
            state["processed"] = handled_messages
            save_state(args.state_file, state)

        if handled_any:
            state["offset"] = offset
            state["processed"] = handled_messages
            save_state(args.state_file, state)

        if args.once:
            break

    return 0


def build_parser() -> argparse.ArgumentParser:
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
        default=bool_from_env(os.environ.get("AOE_ORCH_AUTO_DISCOVER"), False),
        help="auto-register orch projects from `aoe list` under --workspace-root",
    )
    p.add_argument(
        "--no-orch-auto-discover",
        dest="orch_auto_discover",
        action="store_false",
        help="disable orch auto-discovery",
    )
    p.add_argument(
        "--orch-auto-init",
        action="store_true",
        default=bool_from_env(os.environ.get("AOE_ORCH_AUTO_INIT"), False),
        help="when auto-discover finds a project, create <project_root>/.aoe-team and seed AOE_TODO.md if missing",
    )
    p.add_argument(
        "--no-orch-auto-init",
        dest="orch_auto_init",
        action="store_false",
        help="disable seeding .aoe-team on auto-discover",
    )
    p.add_argument(
        "--owner-chat-id",
        default=os.environ.get("TELEGRAM_OWNER_CHAT_ID", os.environ.get("AOE_OWNER_CHAT_ID", "")),
    )
    p.add_argument(
        "--owner-only",
        action="store_true",
        default=bool_from_env(os.environ.get("AOE_OWNER_ONLY"), False),
        help="accept messages only from the owner account (from.id) in private chat",
    )
    p.add_argument(
        "--no-owner-only",
        dest="owner_only",
        action="store_false",
        help="disable owner-only enforcement",
    )
    p.add_argument("--allow-chat-ids", default=os.environ.get("TELEGRAM_ALLOW_CHAT_IDS", ""))
    p.add_argument("--admin-chat-ids", default=os.environ.get("TELEGRAM_ADMIN_CHAT_IDS", ""))
    p.add_argument("--readonly-chat-ids", default=os.environ.get("TELEGRAM_READONLY_CHAT_IDS", ""))
    p.add_argument(
        "--deny-by-default",
        action="store_true",
        default=bool_from_env(os.environ.get("AOE_DENY_BY_DEFAULT"), DEFAULT_DENY_BY_DEFAULT),
        help="deny all chats unless allowlist matches (bootstrap /lockme when empty)",
    )
    p.add_argument(
        "--no-deny-by-default",
        dest="deny_by_default",
        action="store_false",
        help="legacy mode: allow all chats when allowlist is empty",
    )

    p.add_argument("--aoe-orch-bin", default=os.environ.get("AOE_ORCH_BIN", str(Path.home() / ".local/bin/aoe-orch")))
    p.add_argument("--aoe-team-bin", default=os.environ.get("AOE_TEAM_BIN", str(Path.home() / ".local/bin/aoe-team")))

    p.add_argument("--roles", help="fixed role csv passed to aoe-orch run")
    p.add_argument(
        "--default-lang",
        default=os.environ.get("AOE_DEFAULT_LANG", DEFAULT_UI_LANG),
        help="default interface/help language when chat-specific lang is unset (ko|en)",
    )
    p.add_argument(
        "--default-reply-lang",
        default=os.environ.get("AOE_DEFAULT_REPLY_LANG", DEFAULT_REPLY_LANG),
        help="default orchestrator answer language (ko|en)",
    )
    p.add_argument(
        "--default-report-level",
        default=os.environ.get("AOE_DEFAULT_REPORT_LEVEL", DEFAULT_REPORT_LEVEL),
        help="default report verbosity when chat-specific report_level is unset (short|normal|long)",
    )
    p.add_argument("--priority", default="P2")
    p.add_argument("--orch-timeout-sec", type=int, default=DEFAULT_ORCH_TIMEOUT_SEC)
    p.add_argument("--orch-poll-sec", type=float, default=DEFAULT_ORCH_POLL_SEC)
    p.add_argument("--orch-command-timeout-sec", type=int, default=DEFAULT_ORCH_COMMAND_TIMEOUT_SEC)
    p.add_argument("--no-spawn-missing", action="store_true")
    p.add_argument("--no-wait", action="store_true")
    p.add_argument(
        "--auto-dispatch",
        action="store_true",
        default=(os.environ.get("AOE_AUTO_DISPATCH", "0").strip().lower() in {"1", "true", "yes", "on"}),
        help="enable keyword-based automatic dispatch to worker roles",
    )
    p.add_argument(
        "--no-auto-dispatch",
        dest="auto_dispatch",
        action="store_false",
        help="disable keyword-based automatic dispatch (default)",
    )
    p.add_argument(
        "--slash-only",
        action="store_true",
        default=bool_from_env(os.environ.get("AOE_SLASH_ONLY"), DEFAULT_SLASH_ONLY),
        help="require slash commands in Telegram (plain text only allowed in pending mode)",
    )
    p.add_argument(
        "--no-slash-only",
        dest="slash_only",
        action="store_false",
        help="allow loose text parsing and CLI-style text in Telegram",
    )
    p.add_argument(
        "--owner-bootstrap-mode",
        default=os.environ.get("AOE_OWNER_BOOTSTRAP_MODE", ""),
        help="owner convenience: if default_mode is unset, set it to dispatch/direct on first owner message",
    )
    p.add_argument(
        "--require-verifier",
        action="store_true",
        default=(os.environ.get("AOE_REQUIRE_VERIFIER", "1").strip().lower() in {"1", "true", "yes", "on"}),
        help="require verifier-role completion before integration/close",
    )
    p.add_argument(
        "--no-require-verifier",
        dest="require_verifier",
        action="store_false",
        help="disable verifier gate",
    )
    p.add_argument(
        "--verifier-roles",
        default=os.environ.get("AOE_VERIFIER_ROLES", DEFAULT_VERIFIER_ROLES),
        help="comma-separated verifier role names (default: Reviewer,QA,Verifier)",
    )

    plan_max_raw = (os.environ.get("AOE_PLAN_MAX_SUBTASKS", "") or "").strip()
    try:
        plan_max_default = max(1, int(plan_max_raw or str(DEFAULT_TASK_PLAN_MAX_SUBTASKS)))
    except ValueError:
        plan_max_default = DEFAULT_TASK_PLAN_MAX_SUBTASKS

    plan_replan_raw = (os.environ.get("AOE_PLAN_REPLAN_ATTEMPTS", "") or "").strip()
    try:
        plan_replan_default = max(0, int(plan_replan_raw or str(DEFAULT_TASK_PLAN_REPLAN_ATTEMPTS)))
    except ValueError:
        plan_replan_default = DEFAULT_TASK_PLAN_REPLAN_ATTEMPTS

    p.add_argument(
        "--task-planning",
        action="store_true",
        default=(os.environ.get("AOE_TASK_PLANNING", "1").strip().lower() in {"1", "true", "yes", "on"}),
        help="enable planner/critic sub-task decomposition before dispatch",
    )
    p.add_argument(
        "--no-task-planning",
        dest="task_planning",
        action="store_false",
        help="disable planner/critic decomposition",
    )
    p.add_argument(
        "--plan-max-subtasks",
        type=int,
        default=plan_max_default,
        help="maximum subtasks generated by planner",
    )
    p.add_argument(
        "--plan-auto-replan",
        action="store_true",
        default=(os.environ.get("AOE_PLAN_AUTO_REPLAN", "1").strip().lower() in {"1", "true", "yes", "on"}),
        help="auto-replan when critic finds blocking issues",
    )
    p.add_argument(
        "--no-plan-auto-replan",
        dest="plan_auto_replan",
        action="store_false",
        help="disable automatic replanning",
    )
    p.add_argument(
        "--plan-replan-attempts",
        type=int,
        default=plan_replan_default,
        help="maximum automatic replanning attempts",
    )
    p.add_argument(
        "--plan-block-on-critic",
        action="store_true",
        default=(os.environ.get("AOE_PLAN_BLOCK_ON_CRITIC", "1").strip().lower() in {"1", "true", "yes", "on"}),
        help="block dispatch if critic issues remain after replanning",
    )
    p.add_argument(
        "--no-plan-block-on-critic",
        dest="plan_block_on_critic",
        action="store_false",
        help="allow dispatch even if critic issues remain",
    )

    p.add_argument(
        "--exec-critic",
        action="store_true",
        default=bool_from_env(os.environ.get("AOE_EXEC_CRITIC"), True),
        help="enable post-execution critic verdict (success/retry/fail) for completed dispatch runs",
    )
    p.add_argument(
        "--no-exec-critic",
        dest="exec_critic",
        action="store_false",
        help="disable post-execution critic verdict and auto-retry logic",
    )
    p.add_argument(
        "--exec-critic-retry-max",
        type=int,
        default=int_from_env(os.environ.get("AOE_EXEC_RETRY_MAX"), 3, 1, 9),
        help="max total attempts (including the first) when critic returns retry",
    )

    p.add_argument("--poll-timeout-sec", type=int, default=DEFAULT_POLL_TIMEOUT_SEC)
    p.add_argument("--http-timeout-sec", type=int, default=DEFAULT_HTTP_TIMEOUT_SEC)
    p.add_argument("--max-text-chars", type=int, default=DEFAULT_MAX_TEXT_CHARS)
    p.add_argument(
        "--confirm-ttl-sec",
        type=int,
        default=int_from_env(os.environ.get("AOE_CONFIRM_TTL_SEC"), DEFAULT_CONFIRM_TTL_SEC, 30, 86400),
        help="seconds to keep high-risk auto-run confirmation pending",
    )
    p.add_argument(
        "--chat-max-running",
        type=int,
        default=int_from_env(os.environ.get("AOE_CHAT_MAX_RUNNING"), DEFAULT_CHAT_MAX_RUNNING, 0, 50),
        help="max concurrent pending/running tasks per chat (0 disables)",
    )
    p.add_argument(
        "--chat-daily-cap",
        type=int,
        default=int_from_env(os.environ.get("AOE_CHAT_DAILY_CAP"), DEFAULT_CHAT_DAILY_CAP, 0, 10000),
        help="max tasks created per chat per day (0 disables)",
    )

    p.add_argument("--once", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")

    p.add_argument("--simulate-text", help="process a single local text message (no telegram polling)")
    p.add_argument("--simulate-chat-id", default="local-sim")
    p.add_argument(
        "--simulate-live",
        action="store_true",
        help="allow --simulate-text to execute (default: forces --dry-run for safety)",
    )

    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    args.project_root = resolve_project_root(args.project_root)
    args.team_dir = resolve_team_dir(args.project_root, args.team_dir)
    args.state_file = resolve_state_file(args.project_root, args.state_file)
    args.manager_state_file = resolve_manager_state_file(args.team_dir, args.manager_state_file)
    args.chat_aliases_file = resolve_chat_aliases_file(args.team_dir, args.chat_aliases_file)
    if str(args.instance_lock_file or "").strip():
        args.instance_lock_file = Path(str(args.instance_lock_file)).expanduser().resolve()
    else:
        args.instance_lock_file = (args.team_dir / ".gateway.instance.lock").resolve()
    args.workspace_root = resolve_workspace_root(args.workspace_root)
    args.owner_chat_id = normalize_owner_chat_id(args.owner_chat_id)
    args.owner_bootstrap_mode = (
        normalize_mode_token(str(getattr(args, "owner_bootstrap_mode", "") or "").strip())
        if str(getattr(args, "owner_bootstrap_mode", "") or "").strip()
        else ""
    )
    if args.owner_bootstrap_mode not in {"dispatch", "direct"}:
        args.owner_bootstrap_mode = ""
    args.default_lang = normalize_chat_lang_token(args.default_lang, DEFAULT_UI_LANG) or DEFAULT_UI_LANG
    args.default_reply_lang = normalize_chat_lang_token(args.default_reply_lang, DEFAULT_REPLY_LANG) or DEFAULT_REPLY_LANG
    raw_default_report = normalize_report_token(str(getattr(args, "default_report_level", "") or "").strip())
    args.default_report_level = raw_default_report if raw_default_report in {"short", "normal", "long"} else DEFAULT_REPORT_LEVEL
    args.allow_chat_ids = parse_csv_set(args.allow_chat_ids)
    args.admin_chat_ids = parse_csv_set(args.admin_chat_ids)
    args.readonly_chat_ids = parse_csv_set(args.readonly_chat_ids)
    args.readonly_chat_ids = {x for x in args.readonly_chat_ids if x not in args.admin_chat_ids}
    args.chat_alias_cache = load_chat_aliases(args.chat_aliases_file)

    manager_state = load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
    ensure_default_project_registered(manager_state, args.project_root, args.team_dir)
    if not args.dry_run:
        save_manager_state(args.manager_state_file, manager_state)

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
            process_lock = acquire_process_lock(args.instance_lock_file)
        except Exception as e:
            raise SystemExit(f"[ERROR] {e}")

    if args.simulate_text:
        run_simulation(args, token=token)
        return 0

    rc = run_loop(args, token=token)
    _ = process_lock
    return rc


def shutil_which(binary: str) -> Optional[str]:
    for folder in os.environ.get("PATH", "").split(os.pathsep):
        candidate = Path(folder) / binary
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
    except BrokenPipeError:
        raise SystemExit(0)
