#!/usr/bin/env python3
import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
import time
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from aoe_tg_acl import (
    ensure_chat_allowed,
    format_csv_set,
    is_valid_chat_alias,
    is_valid_chat_id,
    normalize_owner_chat_id,
    parse_csv_set,
    resolve_role_from_acl_sets,
)
from aoe_tg_command_handlers import (
    build_non_run_context,
    build_non_run_deps,
    handle_non_run_command_pipeline,
)
from aoe_tg_message_flow import (
    RunTransitionState,
    apply_confirm_transition_to_resolved,
    apply_retry_transition_to_resolved,
    enforce_command_auth,
)
from aoe_tg_run_handlers import (
    build_run_context,
    build_run_deps,
    handle_run_or_unknown_command,
    resolve_confirm_run_transition,
)
from aoe_tg_command_resolver import ResolvedCommand, resolve_message_command
from aoe_tg_parse import (
    detect_high_risk_prompt,
    parse_command,
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
    "orch-help",
    "mode",
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
    "cancel-pending",
}


def sync_acl_env_file(args: argparse.Namespace) -> None:
    env_path = args.team_dir / "telegram.env"
    upsert_env_var(env_path, "TELEGRAM_ALLOW_CHAT_IDS", format_csv_set(args.allow_chat_ids))
    upsert_env_var(env_path, "TELEGRAM_ADMIN_CHAT_IDS", format_csv_set(args.admin_chat_ids))
    upsert_env_var(env_path, "TELEGRAM_READONLY_CHAT_IDS", format_csv_set(args.readonly_chat_ids))
    if str(getattr(args, "owner_chat_id", "") or "").strip():
        upsert_env_var(env_path, "TELEGRAM_OWNER_CHAT_ID", str(args.owner_chat_id).strip())


def resolve_chat_aliases_file(team_dir: Path, explicit_path: Optional[str]) -> Path:
    if explicit_path:
        return Path(explicit_path).expanduser().resolve()
    env_path = (os.environ.get("AOE_CHAT_ALIASES_FILE") or "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()
    return team_dir / "telegram_chat_aliases.json"


def load_chat_aliases(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}

    out: Dict[str, str] = {}
    seen_chat_ids: Set[str] = set()
    for key in sorted(data.keys(), key=lambda x: int(x) if str(x).isdigit() else 10**9):
        alias = str(key or "").strip()
        chat_id = str(data.get(key) or "").strip()
        if not is_valid_chat_alias(alias):
            continue
        if not is_valid_chat_id(chat_id):
            continue
        if chat_id in seen_chat_ids:
            continue
        out[alias] = chat_id
        seen_chat_ids.add(chat_id)
    return out


def save_chat_aliases(path: Path, aliases: Dict[str, str]) -> None:
    sanitized: Dict[str, str] = {}
    seen_chat_ids: Set[str] = set()
    for alias in sorted(aliases.keys(), key=lambda x: int(str(x)) if str(x).isdigit() else 10**9):
        a = str(alias or "").strip()
        chat_id = str(aliases.get(alias) or "").strip()
        if not is_valid_chat_alias(a):
            continue
        if not is_valid_chat_id(chat_id):
            continue
        if chat_id in seen_chat_ids:
            continue
        sanitized[a] = chat_id
        seen_chat_ids.add(chat_id)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def merged_chat_aliases(args: argparse.Namespace) -> Dict[str, str]:
    rows = load_chat_aliases(args.chat_aliases_file)
    cache = getattr(args, "chat_alias_cache", {})
    if not isinstance(cache, dict):
        return rows
    seen = set(rows.values())
    for alias in sorted(cache.keys(), key=lambda x: int(str(x)) if str(x).isdigit() else 10**9):
        a = str(alias or "").strip()
        chat_id = str(cache.get(alias) or "").strip()
        if not is_valid_chat_alias(a):
            continue
        if not is_valid_chat_id(chat_id):
            continue
        if a in rows:
            continue
        if chat_id in seen:
            continue
        rows[a] = chat_id
        seen.add(chat_id)
    return rows


def update_chat_alias_cache(args: argparse.Namespace, aliases: Dict[str, str]) -> None:
    args.chat_alias_cache = dict(aliases)


def find_chat_alias(aliases: Dict[str, str], chat_id: str) -> str:
    cid = str(chat_id or "").strip()
    for alias, mapped in aliases.items():
        if str(mapped).strip() == cid:
            return str(alias).strip()
    return ""


def next_chat_alias(aliases: Dict[str, str], max_alias: int = 999) -> str:
    used = {int(k) for k in aliases.keys() if str(k).isdigit()}
    for idx in range(1, max_alias + 1):
        if idx not in used:
            return str(idx)
    return ""


def ensure_chat_alias(
    args: argparse.Namespace,
    chat_id: str,
    persist: bool = True,
    aliases: Optional[Dict[str, str]] = None,
) -> str:
    cid = str(chat_id or "").strip()
    if not is_valid_chat_id(cid):
        return ""
    rows = aliases if isinstance(aliases, dict) else merged_chat_aliases(args)
    existing = find_chat_alias(rows, cid)
    if existing:
        update_chat_alias_cache(args, rows)
        return existing
    alias = next_chat_alias(rows)
    if not alias:
        return ""
    rows[alias] = cid
    update_chat_alias_cache(args, rows)
    if persist and (not args.dry_run):
        save_chat_aliases(args.chat_aliases_file, rows)
    return alias


def ensure_chat_aliases(args: argparse.Namespace, chat_ids: Iterable[str], persist: bool = True) -> Dict[str, str]:
    aliases = merged_chat_aliases(args)
    changed = False
    for raw in chat_ids:
        cid = str(raw or "").strip()
        if not is_valid_chat_id(cid):
            continue
        if find_chat_alias(aliases, cid):
            continue
        alias = next_chat_alias(aliases)
        if not alias:
            break
        aliases[alias] = cid
        changed = True
    update_chat_alias_cache(args, aliases)
    if changed and persist and (not args.dry_run):
        save_chat_aliases(args.chat_aliases_file, aliases)
    return aliases


def resolve_chat_ref(args: argparse.Namespace, chat_ref: str) -> Tuple[str, str]:
    token = str(chat_ref or "").strip()
    if is_valid_chat_id(token):
        alias = ensure_chat_alias(args, token, persist=(not args.dry_run))
        return token, alias
    if is_valid_chat_alias(token):
        aliases = merged_chat_aliases(args)
        chat_id = str(aliases.get(token, "")).strip()
        if is_valid_chat_id(chat_id):
            return chat_id, token
        raise RuntimeError(f"unknown chat alias: {token} (use /acl)")
    raise RuntimeError("chat target must be chat_id or alias")


def alias_table_summary(args: argparse.Namespace, limit: int = 30) -> str:
    aliases = merged_chat_aliases(args)
    if not aliases:
        return "(empty)"

    rows: List[str] = []
    for alias in sorted(aliases.keys(), key=lambda x: int(x)):
        chat_id = str(aliases.get(alias, "")).strip()
        if not is_valid_chat_id(chat_id):
            continue
        role = resolve_role_from_acl_sets(
            chat_id=chat_id,
            allow_chat_ids=args.allow_chat_ids,
            admin_chat_ids=args.admin_chat_ids,
            readonly_chat_ids=args.readonly_chat_ids,
            deny_by_default=bool(args.deny_by_default),
        )
        rows.append(f"{alias}:{chat_id}[{role}]")
        if len(rows) >= max(1, int(limit)):
            break
    return ", ".join(rows) if rows else "(empty)"


def resolve_project_root(raw: str) -> Path:
    return Path(raw).expanduser().resolve()


def resolve_team_dir(project_root: Path, explicit_team_dir: Optional[str]) -> Path:
    if explicit_team_dir:
        return Path(explicit_team_dir).expanduser().resolve()
    env_dir = os.environ.get("AOE_TEAM_DIR")
    if env_dir:
        return Path(env_dir).expanduser().resolve()
    return project_root / ".aoe-team"


def resolve_state_file(project_root: Path, explicit_state_file: Optional[str]) -> Path:
    if explicit_state_file:
        return Path(explicit_state_file).expanduser().resolve()
    return project_root / ".aoe-team" / "telegram_gateway_state.json"


def load_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"offset": 0, "updated_at": "", "processed": 0}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"offset": 0, "updated_at": "", "processed": 0}
    if not isinstance(data, dict):
        return {"offset": 0, "updated_at": "", "processed": 0}
    return data


def save_state(path: Path, offset: int, processed: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "offset": int(offset),
        "processed": int(processed),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


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


def sanitize_chat_session_row(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}

    row: Dict[str, Any] = {}
    mode = str(raw.get("pending_mode", "")).strip().lower()
    if mode in {"dispatch", "direct"}:
        row["pending_mode"] = mode
    default_mode = str(raw.get("default_mode", "")).strip().lower()
    if default_mode in {"dispatch", "direct"}:
        row["default_mode"] = default_mode
    raw_confirm = raw.get("confirm_action")
    if isinstance(raw_confirm, dict):
        confirm_mode = str(raw_confirm.get("mode", "")).strip().lower()
        confirm_prompt = str(raw_confirm.get("prompt", "")).strip()
        if confirm_mode in {"dispatch", "direct"} and confirm_prompt:
            confirm_row: Dict[str, Any] = {
                "mode": confirm_mode,
                "prompt": confirm_prompt[:2000],
                "requested_at": str(raw_confirm.get("requested_at", "")).strip() or now_iso(),
                "risk": str(raw_confirm.get("risk", "")).strip()[:80],
            }
            orch_name = str(raw_confirm.get("orch", "")).strip()
            if orch_name:
                confirm_row["orch"] = orch_name
            row["confirm_action"] = confirm_row

    recent_in = raw.get("recent_task_refs")
    recent_out: Dict[str, List[str]] = {}
    if isinstance(recent_in, dict):
        for pname, refs in recent_in.items():
            project_key = normalize_project_name(str(pname or ""))
            if not project_key or not isinstance(refs, list):
                continue
            dedup: List[str] = []
            seen: Set[str] = set()
            for item in refs:
                rid = str(item or "").strip()
                if not rid or rid in seen:
                    continue
                seen.add(rid)
                dedup.append(rid)
                if len(dedup) >= 50:
                    break
            if dedup:
                recent_out[project_key] = dedup
    if recent_out:
        row["recent_task_refs"] = recent_out

    selected_in = raw.get("selected_task_refs")
    selected_out: Dict[str, str] = {}
    if isinstance(selected_in, dict):
        for pname, rid in selected_in.items():
            project_key = normalize_project_name(str(pname or ""))
            request_id = str(rid or "").strip()
            if project_key and request_id:
                selected_out[project_key] = request_id
    if selected_out:
        row["selected_task_refs"] = selected_out

    if row:
        row["updated_at"] = str(raw.get("updated_at", "")).strip() or now_iso()
    return row


def is_path_within(target: Path, root: Optional[Path]) -> bool:
    if root is None:
        return True
    try:
        target.resolve().relative_to(root.resolve())
        return True
    except Exception:
        return False


def default_manager_state(project_root: Path, team_dir: Path) -> Dict[str, Any]:
    return {
        "version": 1,
        "active": "default",
        "updated_at": now_iso(),
        "chat_sessions": {},
        "projects": {
            "default": {
                "name": "default",
                "display_name": "default",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "overview": "",
                "last_request_id": "",
                "tasks": {},
                "task_alias_index": {},
                "task_seq": 0,
                "created_at": now_iso(),
                "updated_at": now_iso(),
            }
        },
    }


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

    return task


def load_manager_state(path: Path, project_root: Path, team_dir: Path) -> Dict[str, Any]:
    fallback = default_manager_state(project_root, team_dir)
    if not path.exists():
        return fallback
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback
    if not isinstance(data, dict):
        return fallback

    projects = data.get("projects")
    if not isinstance(projects, dict) or not projects:
        return fallback

    normalized: Dict[str, Dict[str, Any]] = {}
    for raw_key, raw_entry in projects.items():
        key = normalize_project_name(str(raw_key))
        if not key or not isinstance(raw_entry, dict):
            continue
        root = str(raw_entry.get("project_root", "")).strip()
        if not root:
            continue
        td = str(raw_entry.get("team_dir", "")).strip()
        if not td:
            td = str(Path(root).expanduser().resolve() / ".aoe-team")
        raw_tasks = raw_entry.get("tasks")
        tasks: Dict[str, Any] = {}
        if isinstance(raw_tasks, dict):
            for req_id, task in raw_tasks.items():
                rid = str(req_id or "").strip()
                if not rid or not isinstance(task, dict):
                    continue
                tasks[rid] = sanitize_task_record(task, rid)
            trim_project_tasks(tasks)

        raw_alias_index = raw_entry.get("task_alias_index")
        task_alias_index: Dict[str, str] = {}
        if isinstance(raw_alias_index, dict):
            for akey, rid in raw_alias_index.items():
                key_norm = normalize_task_alias_key(str(akey or ""))
                rid_norm = str(rid or "").strip()
                if key_norm and rid_norm:
                    task_alias_index[key_norm] = rid_norm

        raw_seq = raw_entry.get("task_seq")
        try:
            task_seq = max(0, int(raw_seq or 0))
        except Exception:
            task_seq = 0

        normalized[key] = {
            "name": key,
            "display_name": str(raw_entry.get("display_name", key)).strip() or key,
            "project_root": str(Path(root).expanduser().resolve()),
            "team_dir": str(Path(td).expanduser().resolve()),
            "overview": str(raw_entry.get("overview", "")).strip(),
            "last_request_id": str(raw_entry.get("last_request_id", "")).strip(),
            "tasks": tasks,
            "task_alias_index": task_alias_index,
            "task_seq": task_seq,
            "created_at": str(raw_entry.get("created_at", "")).strip() or now_iso(),
            "updated_at": str(raw_entry.get("updated_at", "")).strip() or now_iso(),
        }

    if not normalized:
        return fallback

    active = normalize_project_name(str(data.get("active", "default")))
    if active not in normalized:
        active = sorted(normalized.keys())[0]

    for entry in normalized.values():
        if isinstance(entry, dict):
            backfill_task_aliases(entry)

    raw_chat = data.get("chat_sessions")
    chat_sessions: Dict[str, Any] = {}
    if isinstance(raw_chat, dict):
        for k, v in raw_chat.items():
            cid = str(k or "").strip()
            if not cid:
                continue
            row = sanitize_chat_session_row(v)
            if row:
                chat_sessions[cid] = row

    return {
        "version": 1,
        "active": active,
        "updated_at": str(data.get("updated_at", "")).strip() or now_iso(),
        "chat_sessions": chat_sessions,
        "projects": normalized,
    }


def save_manager_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(state)
    payload["updated_at"] = now_iso()
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def acquire_process_lock(lock_path: Path) -> Any:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fh = lock_path.open("a+", encoding="utf-8")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        fh.close()
        raise RuntimeError(f"another gateway process is already running (lock={lock_path})")
    fh.seek(0)
    fh.truncate(0)
    fh.write(f"pid={os.getpid()} started_at={now_iso()}\n")
    fh.flush()
    return fh


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


def summarize_gateway_metrics(team_dir: Path, project_name: str, hours: int = 24) -> str:
    cap_hours = max(1, min(168, int(hours or 24)))
    path = team_dir / "logs" / "gateway_events.jsonl"
    if not path.exists():
        return f"orch: {project_name}\nmetrics: no data file\nwindow_hours: {cap_hours}"

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
        return f"orch: {project_name}\nmetrics: failed to read log\nwindow_hours: {cap_hours}"

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
    return "\n".join(lines)


def task_identifiers(task: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    if not isinstance(task, dict):
        return "", ""
    short_id = str(task.get("short_id", "")).strip()
    alias = str(task.get("alias", "")).strip()
    return short_id, alias


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
) -> None:
    short_id, alias = task_identifiers(task)
    row: Dict[str, Any] = {
        "timestamp": now_iso(),
        "event": str(event or "").strip() or "event",
        "trace_id": str(trace_id or "").strip(),
        "project": str(project or "").strip(),
        "request_id": str(request_id or "").strip(),
        "task_short_id": short_id,
        "task_alias": alias,
        "stage": str(stage or "").strip(),
        "actor": str(actor or "").strip() or "gateway",
        "status": str(status or "").strip(),
        "error_code": str(error_code or "").strip(),
        "latency_ms": max(0, int(latency_ms or 0)),
        "detail": mask_sensitive_text(str(detail or "").strip())[:800],
    }
    try:
        append_jsonl(team_dir / "logs" / "gateway_events.jsonl", row)
    except Exception:
        return


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
    chat_sessions = state.get("chat_sessions")
    if not isinstance(chat_sessions, dict):
        state["chat_sessions"] = {}

    projects = state.setdefault("projects", {})
    if not isinstance(projects, dict):
        state["projects"] = {}
        projects = state["projects"]

    if "default" not in projects:
        projects["default"] = {
            "name": "default",
            "display_name": "default",
            "project_root": str(project_root),
            "team_dir": str(team_dir),
            "overview": "",
            "last_request_id": "",
            "tasks": {},
            "task_alias_index": {},
            "task_seq": 0,
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }

    for entry in projects.values():
        if isinstance(entry, dict):
            if "tasks" not in entry or not isinstance(entry.get("tasks"), dict):
                entry["tasks"] = {}
            if "task_alias_index" not in entry or not isinstance(entry.get("task_alias_index"), dict):
                entry["task_alias_index"] = {}
            try:
                entry["task_seq"] = max(0, int(entry.get("task_seq", 0) or 0))
            except Exception:
                entry["task_seq"] = 0
            backfill_task_aliases(entry)

    active = normalize_project_name(str(state.get("active", "default")))
    if active not in projects:
        state["active"] = "default"


def get_chat_sessions(state: Dict[str, Any]) -> Dict[str, Any]:
    sessions = state.get("chat_sessions")
    if not isinstance(sessions, dict):
        sessions = {}
        state["chat_sessions"] = sessions
    return sessions


def get_chat_session_row(state: Dict[str, Any], chat_id: str, create: bool = False) -> Dict[str, Any]:
    token = str(chat_id or "").strip()
    if not token:
        return {}
    sessions = get_chat_sessions(state)
    row = sessions.get(token)
    if not isinstance(row, dict):
        if not create:
            return {}
        row = {}
        sessions[token] = row
    return row


def get_pending_mode(state: Dict[str, Any], chat_id: str) -> str:
    row = get_chat_session_row(state, chat_id, create=False)
    mode = str(row.get("pending_mode", "")).strip().lower()
    return mode if mode in {"dispatch", "direct"} else ""


def set_pending_mode(state: Dict[str, Any], chat_id: str, mode: str) -> None:
    normalized = str(mode or "").strip().lower()
    if normalized not in {"dispatch", "direct"}:
        return
    row = get_chat_session_row(state, chat_id, create=True)
    if not row:
        return
    row["pending_mode"] = normalized
    row["updated_at"] = now_iso()


def clear_pending_mode(state: Dict[str, Any], chat_id: str) -> bool:
    token = str(chat_id or "").strip()
    if not token:
        return False
    sessions = get_chat_sessions(state)
    row = sessions.get(token)
    if not isinstance(row, dict):
        return False
    existed = "pending_mode" in row
    row.pop("pending_mode", None)
    if existed:
        row["updated_at"] = now_iso()
    if not any(k in row for k in ("pending_mode", "default_mode", "confirm_action", "recent_task_refs", "selected_task_refs")):
        sessions.pop(token, None)
    return existed


def get_default_mode(state: Dict[str, Any], chat_id: str) -> str:
    row = get_chat_session_row(state, chat_id, create=False)
    mode = str(row.get("default_mode", "")).strip().lower()
    return mode if mode in {"dispatch", "direct"} else ""


def set_default_mode(state: Dict[str, Any], chat_id: str, mode: str) -> None:
    normalized = str(mode or "").strip().lower()
    if normalized not in {"dispatch", "direct"}:
        return
    row = get_chat_session_row(state, chat_id, create=True)
    if not row:
        return
    row["default_mode"] = normalized
    row["updated_at"] = now_iso()


def clear_default_mode(state: Dict[str, Any], chat_id: str) -> bool:
    token = str(chat_id or "").strip()
    if not token:
        return False
    sessions = get_chat_sessions(state)
    row = sessions.get(token)
    if not isinstance(row, dict):
        return False
    existed = "default_mode" in row
    row.pop("default_mode", None)
    if existed:
        row["updated_at"] = now_iso()
    if not any(k in row for k in ("pending_mode", "default_mode", "confirm_action", "recent_task_refs", "selected_task_refs")):
        sessions.pop(token, None)
    return existed


def get_confirm_action(state: Dict[str, Any], chat_id: str) -> Dict[str, Any]:
    row = get_chat_session_row(state, chat_id, create=False)
    raw = row.get("confirm_action")
    if not isinstance(raw, dict):
        return {}
    mode = str(raw.get("mode", "")).strip().lower()
    prompt = str(raw.get("prompt", "")).strip()
    if mode not in {"dispatch", "direct"} or not prompt:
        return {}
    out: Dict[str, Any] = {
        "mode": mode,
        "prompt": prompt,
        "requested_at": str(raw.get("requested_at", "")).strip() or now_iso(),
        "risk": str(raw.get("risk", "")).strip(),
    }
    orch = str(raw.get("orch", "")).strip()
    if orch:
        out["orch"] = orch
    return out


def set_confirm_action(
    state: Dict[str, Any],
    chat_id: str,
    mode: str,
    prompt: str,
    risk: str = "",
    orch: str = "",
) -> None:
    normalized_mode = str(mode or "").strip().lower()
    text = str(prompt or "").strip()
    if normalized_mode not in {"dispatch", "direct"} or not text:
        return
    row = get_chat_session_row(state, chat_id, create=True)
    if not row:
        return
    confirm_row: Dict[str, Any] = {
        "mode": normalized_mode,
        "prompt": text[:2000],
        "requested_at": now_iso(),
        "risk": str(risk or "").strip()[:80],
    }
    orch_name = str(orch or "").strip()
    if orch_name:
        confirm_row["orch"] = orch_name
    row["confirm_action"] = confirm_row
    row["updated_at"] = now_iso()


def clear_confirm_action(state: Dict[str, Any], chat_id: str) -> bool:
    token = str(chat_id or "").strip()
    if not token:
        return False
    sessions = get_chat_sessions(state)
    row = sessions.get(token)
    if not isinstance(row, dict):
        return False
    existed = "confirm_action" in row
    row.pop("confirm_action", None)
    if existed:
        row["updated_at"] = now_iso()
    if not any(k in row for k in ("pending_mode", "default_mode", "confirm_action", "recent_task_refs", "selected_task_refs")):
        sessions.pop(token, None)
    return existed


def get_chat_recent_task_refs(state: Dict[str, Any], chat_id: str, project_name: str) -> List[str]:
    row = get_chat_session_row(state, chat_id, create=False)
    refs_map = row.get("recent_task_refs")
    if not isinstance(refs_map, dict):
        return []
    refs = refs_map.get(normalize_project_name(project_name), [])
    if not isinstance(refs, list):
        return []
    out: List[str] = []
    for item in refs:
        rid = str(item or "").strip()
        if rid:
            out.append(rid)
    return out


def set_chat_recent_task_refs(
    state: Dict[str, Any],
    chat_id: str,
    project_name: str,
    refs: List[str],
) -> None:
    row = get_chat_session_row(state, chat_id, create=True)
    if not row:
        return
    key = normalize_project_name(project_name)
    refs_map = row.get("recent_task_refs")
    if not isinstance(refs_map, dict):
        refs_map = {}
        row["recent_task_refs"] = refs_map

    dedup: List[str] = []
    seen: Set[str] = set()
    for item in refs:
        rid = str(item or "").strip()
        if not rid or rid in seen:
            continue
        seen.add(rid)
        dedup.append(rid)
        if len(dedup) >= 50:
            break

    if dedup:
        refs_map[key] = dedup
    else:
        refs_map.pop(key, None)
    if not refs_map:
        row.pop("recent_task_refs", None)

    selected_map = row.get("selected_task_refs")
    if isinstance(selected_map, dict):
        current = str(selected_map.get(key, "")).strip()
        if current and current not in dedup:
            selected_map.pop(key, None)
        if not selected_map:
            row.pop("selected_task_refs", None)

    row["updated_at"] = now_iso()


def touch_chat_recent_task_ref(
    state: Dict[str, Any],
    chat_id: str,
    project_name: str,
    request_id: str,
) -> None:
    rid = str(request_id or "").strip()
    if not rid:
        return
    refs = get_chat_recent_task_refs(state, chat_id, project_name)
    merged = [rid] + [x for x in refs if x != rid]
    set_chat_recent_task_refs(state, chat_id, project_name, merged[:50])


def get_chat_selected_task_ref(state: Dict[str, Any], chat_id: str, project_name: str) -> str:
    row = get_chat_session_row(state, chat_id, create=False)
    selected_map = row.get("selected_task_refs")
    if not isinstance(selected_map, dict):
        return ""
    return str(selected_map.get(normalize_project_name(project_name), "")).strip()


def set_chat_selected_task_ref(
    state: Dict[str, Any],
    chat_id: str,
    project_name: str,
    request_id: str,
) -> None:
    row = get_chat_session_row(state, chat_id, create=True)
    if not row:
        return
    key = normalize_project_name(project_name)
    selected_map = row.get("selected_task_refs")
    if not isinstance(selected_map, dict):
        selected_map = {}
        row["selected_task_refs"] = selected_map
    rid = str(request_id or "").strip()
    if rid:
        selected_map[key] = rid
    else:
        selected_map.pop(key, None)
    if not selected_map:
        row.pop("selected_task_refs", None)
    row["updated_at"] = now_iso()


def resolve_chat_task_ref(state: Dict[str, Any], chat_id: str, project_name: str, raw_ref: str) -> str:
    token = str(raw_ref or "").strip()
    if not token:
        return ""
    if token.isdigit():
        refs = get_chat_recent_task_refs(state, chat_id, project_name)
        idx = int(token)
        if 1 <= idx <= len(refs):
            return refs[idx - 1]
    return token


def get_manager_project(state: Dict[str, Any], name: Optional[str]) -> Tuple[str, Dict[str, Any]]:
    projects = state.get("projects") or {}
    if not isinstance(projects, dict) or not projects:
        raise RuntimeError("no orch projects registered")

    key = normalize_project_name(name or str(state.get("active", "default")))
    entry = projects.get(key)
    if not isinstance(entry, dict):
        known = ", ".join(sorted(projects.keys()))
        raise RuntimeError(f"unknown orch project: {key} (known: {known})")
    return key, entry


def make_project_args(args: argparse.Namespace, entry: Dict[str, Any]) -> argparse.Namespace:
    copied = argparse.Namespace(**vars(args))
    copied.project_root = Path(str(entry.get("project_root", args.project_root))).expanduser().resolve()
    copied.team_dir = Path(str(entry.get("team_dir", copied.project_root / '.aoe-team'))).expanduser().resolve()
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
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "overview": (overview or "").strip(),
        "last_request_id": "",
        "tasks": {},
        "task_alias_index": {},
        "task_seq": 0,
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }
    if isinstance(existing, dict):
        entry["created_at"] = str(existing.get("created_at", entry["created_at"]))
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
    projects[key] = entry

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
    rows = normalize_role_rows(data)
    counts = data.get("counts") or {}

    assignments = int(counts.get("assignments", 0) or 0)
    replies = int(counts.get("replies", 0) or 0)
    if assignments <= 0:
        assignments = len(rows)
    if replies <= 0:
        replies = len(data.get("replies") or [])

    done_roles: Set[str] = set()
    failed_roles: Set[str] = set()
    pending_roles: Set[str] = set()

    for row in rows:
        role = str(row.get("role", "")).strip()
        status = str(row.get("status", "pending")).strip().lower()
        if not role:
            continue
        if status in {"failed", "error", "fail"}:
            failed_roles.add(role)
        elif status == "done":
            done_roles.add(role)
        else:
            pending_roles.add(role)

    for role in data.get("done_roles") or []:
        token = str(role).strip()
        if token:
            done_roles.add(token)
            pending_roles.discard(token)
            failed_roles.discard(token)

    for role in data.get("failed_roles") or []:
        token = str(role).strip()
        if token:
            failed_roles.add(token)
            done_roles.discard(token)
            pending_roles.discard(token)

    for role in data.get("pending_roles") or data.get("unresolved_roles") or []:
        token = str(role).strip()
        if token and token not in done_roles and token not in failed_roles:
            pending_roles.add(token)

    request_id = str(data.get("request_id", "")).strip()
    complete = bool(data.get("complete", False))

    return {
        "request_id": request_id,
        "rows": rows,
        "assignments": assignments,
        "replies": replies,
        "complete": complete,
        "done_roles": sorted(done_roles),
        "failed_roles": sorted(failed_roles),
        "pending_roles": sorted(pending_roles),
    }


def ensure_project_tasks(entry: Dict[str, Any]) -> Dict[str, Any]:
    tasks = entry.get("tasks")
    if not isinstance(tasks, dict):
        tasks = {}
        entry["tasks"] = tasks
    return tasks


def normalize_task_alias_key(raw: str) -> str:
    src = str(raw or "").strip().lower()
    out: List[str] = []
    sep = False
    for ch in src:
        if ch.isalnum():
            out.append(ch)
            sep = False
        else:
            if not sep:
                out.append("-")
                sep = True
    return "".join(out).strip("-")


def parse_task_seq_from_short_id(short_id: str) -> int:
    src = str(short_id or "").strip().upper()
    if not src.startswith("T-"):
        return 0
    tail = src[2:]
    return int(tail) if tail.isdigit() else 0


def format_task_short_id(seq: int) -> str:
    value = max(1, int(seq))
    return f"T-{value:03d}" if value < 1000 else f"T-{value}"


def derive_task_alias_base(prompt: str) -> str:
    src = str(prompt or "").strip()
    if not src:
        return "task"

    cleaned: List[str] = []
    for ch in src:
        if ch.isalnum() or ch in {" ", "-", "_"}:
            cleaned.append(ch)
        else:
            cleaned.append(" ")

    tokens = [t.lower() for t in "".join(cleaned).split() if t]
    if not tokens:
        return "task"

    stop = {
        "the", "a", "an", "to", "for", "and", "or", "of",
        "해주세요", "해줘", "요청", "작업", "진행", "지금", "바로", "좀",
    }
    picked = [t for t in tokens if t not in stop] or tokens

    alias = "-".join(picked[:5]).strip("-_")
    if len(alias) > 48:
        alias = alias[:48].rstrip("-_")
    return alias or "task"


def ensure_task_alias_meta(entry: Dict[str, Any]) -> Tuple[Dict[str, str], int]:
    raw_index = entry.get("task_alias_index")
    if not isinstance(raw_index, dict):
        raw_index = {}
        entry["task_alias_index"] = raw_index

    alias_index: Dict[str, str] = {}
    for key, rid in raw_index.items():
        key_norm = normalize_task_alias_key(str(key or ""))
        rid_norm = str(rid or "").strip()
        if key_norm and rid_norm:
            alias_index[key_norm] = rid_norm
    entry["task_alias_index"] = alias_index

    raw_seq = entry.get("task_seq")
    try:
        seq = max(0, int(raw_seq or 0))
    except Exception:
        seq = 0
    entry["task_seq"] = seq
    return alias_index, seq


def task_display_label(task: Dict[str, Any], fallback_request_id: str = "") -> str:
    short_id = str(task.get("short_id", "")).strip().upper()
    alias = str(task.get("alias", "")).strip()
    if short_id and alias:
        return f"{short_id} | {alias}"
    if alias:
        return alias
    if short_id:
        return short_id
    rid = str(task.get("request_id", "")).strip() or str(fallback_request_id or "").strip()
    return rid if rid else "-"


def rebuild_task_alias_index(entry: Dict[str, Any]) -> None:
    tasks = ensure_project_tasks(entry)
    _, seq = ensure_task_alias_meta(entry)

    alias_index: Dict[str, str] = {}
    max_seq = max(0, int(seq))

    for req_id, task in tasks.items():
        rid = str(req_id or "").strip()
        if not rid or not isinstance(task, dict):
            continue

        short_id = str(task.get("short_id", "")).strip().upper()
        alias = str(task.get("alias", "")).strip()

        if short_id:
            alias_index[normalize_task_alias_key(short_id)] = rid
            max_seq = max(max_seq, parse_task_seq_from_short_id(short_id))
        if alias:
            alias_index[normalize_task_alias_key(alias)] = rid

    entry["task_alias_index"] = alias_index
    entry["task_seq"] = max_seq


def assign_task_alias(
    entry: Dict[str, Any],
    task: Dict[str, Any],
    prompt: str,
    rebuild_index: bool = True,
) -> None:
    tasks = ensure_project_tasks(entry)
    alias_index, seq = ensure_task_alias_meta(entry)

    req_id = str(task.get("request_id", "")).strip()
    if not req_id:
        return

    short_id = str(task.get("short_id", "")).strip().upper()
    if not short_id:
        next_seq = max(seq, 0)
        while True:
            next_seq += 1
            candidate = format_task_short_id(next_seq)
            key = normalize_task_alias_key(candidate)
            owner = alias_index.get(key)
            if not owner or owner == req_id:
                short_id = candidate
                task["short_id"] = short_id
                entry["task_seq"] = next_seq
                break

    alias = str(task.get("alias", "")).strip()
    if not alias:
        base = derive_task_alias_base(prompt or str(task.get("prompt", "")).strip() or short_id.lower())
        candidate = base
        suffix = 2
        while True:
            key = normalize_task_alias_key(candidate)
            owner = alias_index.get(key)
            if not owner or owner == req_id:
                alias = candidate
                task["alias"] = alias
                break
            candidate = f"{base}-{suffix}"
            suffix += 1

    if rebuild_index:
        rebuild_task_alias_index(entry)


def backfill_task_aliases(entry: Dict[str, Any]) -> None:
    tasks = ensure_project_tasks(entry)
    if not tasks:
        ensure_task_alias_meta(entry)
        return

    rows = sorted(
        tasks.items(),
        key=lambda kv: str((kv[1] or {}).get("created_at", "")),
    )
    for req_id, task in rows:
        if not isinstance(task, dict):
            continue
        rid = str(req_id or "").strip()
        if not rid:
            continue
        if not str(task.get("request_id", "")).strip():
            task["request_id"] = rid
        assign_task_alias(entry, task, prompt=str(task.get("prompt", "")), rebuild_index=False)

    rebuild_task_alias_index(entry)


def resolve_task_request_id(entry: Dict[str, Any], request_or_alias: str) -> str:
    token = str(request_or_alias or "").strip()
    if not token:
        return ""

    tasks = ensure_project_tasks(entry)
    if token in tasks:
        return token

    alias_index, _ = ensure_task_alias_meta(entry)
    if not alias_index and tasks:
        backfill_task_aliases(entry)
        alias_index, _ = ensure_task_alias_meta(entry)

    norm = normalize_task_alias_key(token)
    mapped = alias_index.get(norm, "")
    if mapped and mapped in tasks:
        return mapped

    # fallback linear scan when index is stale
    for rid, task in tasks.items():
        if not isinstance(task, dict):
            continue
        short_id = str(task.get("short_id", "")).strip().upper()
        alias = str(task.get("alias", "")).strip()
        if token.upper() == short_id:
            return rid
        if norm and norm == normalize_task_alias_key(alias):
            return rid

    return token


def latest_task_request_refs(entry: Dict[str, Any], limit: int = 12) -> List[str]:
    tasks = ensure_project_tasks(entry)
    if not tasks:
        return []
    backfill_task_aliases(entry)
    rows = sorted(
        tasks.items(),
        key=lambda kv: str((kv[1] or {}).get("updated_at", "")),
        reverse=True,
    )
    cap = max(1, min(50, int(limit)))
    out: List[str] = []
    for req_id, task in rows[:cap]:
        if isinstance(task, dict):
            rid = str(req_id or "").strip()
            if rid:
                out.append(rid)
    return out


def summarize_task_monitor(project_name: str, entry: Dict[str, Any], limit: int = 12) -> str:
    tasks = ensure_project_tasks(entry)
    if not tasks:
        return f"orch: {project_name}\n작업이 없습니다."

    backfill_task_aliases(entry)
    rows = sorted(
        tasks.items(),
        key=lambda kv: str((kv[1] or {}).get("updated_at", "")),
        reverse=True,
    )
    cap = max(1, min(50, int(limit)))

    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0}
    invalid_stage_rows = 0
    for _, task in rows:
        if not isinstance(task, dict):
            continue
        status = normalize_task_status(task.get("status", "pending"))
        counts[status] = counts.get(status, 0) + 1
        stage = str(task.get("stage", "")).strip().lower()
        if stage and stage not in LIFECYCLE_STAGES:
            invalid_stage_rows += 1

    lines = [
        f"orch: {project_name}",
        f"task monitor: latest {cap}",
        "format: label | status/stage | roles | updated",
        "summary: total={total} running={running} completed={completed} failed={failed} pending={pending}".format(
            total=len(rows),
            running=counts.get("running", 0),
            completed=counts.get("completed", 0),
            failed=counts.get("failed", 0),
            pending=counts.get("pending", 0),
        ),
    ]
    if invalid_stage_rows:
        lines.append(f"warning: invalid lifecycle stage rows={invalid_stage_rows}")

    for idx, (req_id, task) in enumerate(rows[:cap], start=1):
        if not isinstance(task, dict):
            continue
        label = task_display_label(task, fallback_request_id=req_id)
        status = normalize_task_status(task.get("status", "pending"))
        stage = str(task.get("stage", "pending")).strip().lower() or "pending"
        if stage not in LIFECYCLE_STAGES:
            stage = "pending"
        roles = dedupe_roles(task.get("roles") or [])
        role_text = ", ".join(roles[:2])
        if len(roles) > 2:
            role_text += f" +{len(roles) - 2}"
        updated = str(task.get("updated_at", "")).strip() or "-"
        lines.append(f"- {idx}. {label} | {status}/{stage} | {role_text or '-'} | {updated}")

    lines.append("")
    lines.append("alias map (number/label -> request_id):")
    for idx, (req_id, task) in enumerate(rows[:cap], start=1):
        if not isinstance(task, dict):
            continue
        lines.append(f"- {idx}. {task_display_label(task, fallback_request_id=req_id)} -> {req_id}")
    lines.append("")
    lines.append("quick actions: /check <번호|label> /task <번호|label> /retry <번호|label> /replan <번호|label> /cancel <번호|label>")

    return "\n".join(lines)


def trim_project_tasks(tasks: Dict[str, Any], keep: int = DEFAULT_TASK_KEEP_PER_PROJECT) -> None:
    if len(tasks) <= int(keep):
        return
    ordered = sorted(
        tasks.items(),
        key=lambda kv: str((kv[1] or {}).get("updated_at", "")),
        reverse=True,
    )
    keep_keys = {k for k, _ in ordered[: max(1, int(keep))]}
    for key in list(tasks.keys()):
        if key not in keep_keys:
            tasks.pop(key, None)


def get_task_record(entry: Dict[str, Any], request_id: str) -> Optional[Dict[str, Any]]:
    token = resolve_task_request_id(entry, request_id)
    if not token:
        return None
    tasks = ensure_project_tasks(entry)
    item = tasks.get(token)
    return item if isinstance(item, dict) else None


def ensure_task_record(
    entry: Dict[str, Any],
    request_id: str,
    prompt: str,
    mode: str,
    roles: List[str],
    verifier_roles: List[str],
    require_verifier: bool,
) -> Dict[str, Any]:
    token = str(request_id or "").strip()
    tasks = ensure_project_tasks(entry)
    now = now_iso()

    item = tasks.get(token)
    if not isinstance(item, dict):
        item = {
            "request_id": token,
            "mode": mode,
            "prompt": prompt.strip(),
            "roles": dedupe_roles(roles),
            "verifier_roles": dedupe_roles(verifier_roles),
            "require_verifier": bool(require_verifier),
            "status": "running",
            "stage": "intake",
            "stages": {name: "pending" for name in LIFECYCLE_STAGES},
            "history": [],
            "created_at": now,
            "updated_at": now,
            "result": {},
        }
        tasks[token] = item
    else:
        if prompt:
            item["prompt"] = prompt.strip()
        if mode:
            item["mode"] = mode
        if roles:
            item["roles"] = dedupe_roles(roles)
        if verifier_roles:
            item["verifier_roles"] = dedupe_roles(verifier_roles)
        item["require_verifier"] = bool(require_verifier)
        item["updated_at"] = now

    assign_task_alias(entry, item, prompt=prompt, rebuild_index=False)
    trim_project_tasks(tasks)
    rebuild_task_alias_index(entry)
    return item


def lifecycle_set_stage(task: Dict[str, Any], stage: str, status: str, note: str = "") -> None:
    if stage not in LIFECYCLE_STAGES:
        return

    stages = task.get("stages")
    if not isinstance(stages, dict):
        stages = {name: "pending" for name in LIFECYCLE_STAGES}
        task["stages"] = stages

    prev = str(stages.get(stage, "pending"))
    next_status = normalize_stage_status(status or "pending")
    if prev == next_status and not note:
        return

    stages[stage] = next_status
    task["stage"] = stage

    history = task.get("history")
    if not isinstance(history, list):
        history = []

    event: Dict[str, Any] = {
        "at": now_iso(),
        "stage": stage,
        "status": next_status,
    }
    if note:
        event["note"] = note
    history.append(event)
    if len(history) > DEFAULT_TASK_HISTORY_LIMIT:
        history = history[-DEFAULT_TASK_HISTORY_LIMIT:]

    task["history"] = history
    task["updated_at"] = event["at"]


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
    snap = extract_request_snapshot(request_data)
    request_id = str(snap.get("request_id", "")).strip()
    if not request_id:
        return None

    rows = snap.get("rows") or []
    inferred_roles = [str(x.get("role", "")).strip() for x in rows if str(x.get("role", "")).strip()]
    roles = dedupe_roles(selected_roles or inferred_roles)

    inferred_verifiers = [r for r in roles if r.lower() in {c.lower() for c in verifier_candidates}]
    verifiers = dedupe_roles(verifier_roles or inferred_verifiers)

    task = ensure_task_record(
        entry=entry,
        request_id=request_id,
        prompt=prompt,
        mode=mode,
        roles=roles,
        verifier_roles=verifiers,
        require_verifier=require_verifier,
    )

    assignments = int(snap.get("assignments", 0) or 0)
    replies = int(snap.get("replies", 0) or 0)
    complete = bool(snap.get("complete", False))
    done_roles = set(str(x) for x in (snap.get("done_roles") or []))
    failed_roles = set(str(x) for x in (snap.get("failed_roles") or []))
    pending_roles = set(str(x) for x in (snap.get("pending_roles") or []))

    lifecycle_set_stage(task, "intake", "done")
    lifecycle_set_stage(task, "planning", "done")

    staffing_status = "done" if assignments > 0 else ("running" if roles else "pending")
    lifecycle_set_stage(task, "staffing", staffing_status)

    if failed_roles:
        execution_status = "failed"
    elif complete and assignments > 0 and not pending_roles:
        execution_status = "done"
    elif assignments > 0:
        execution_status = "running"
    else:
        execution_status = "pending"
    lifecycle_set_stage(task, "execution", execution_status)

    ver_note = ""
    if require_verifier:
        if not verifiers:
            verification_status = "failed"
            ver_note = "no verifier role assigned"
        elif any(v in failed_roles for v in verifiers):
            verification_status = "failed"
            ver_note = "verifier role failed"
        elif all(v in done_roles for v in verifiers):
            verification_status = "done"
        elif complete and execution_status == "done":
            verification_status = "failed"
            ver_note = "verifier gate not satisfied"
        elif execution_status in {"running", "done"}:
            verification_status = "running"
        elif execution_status == "failed":
            verification_status = "failed"
        else:
            verification_status = "pending"
    else:
        if execution_status == "done":
            verification_status = "done"
        elif execution_status == "failed":
            verification_status = "failed"
        elif execution_status == "running":
            verification_status = "running"
        else:
            verification_status = "pending"

    lifecycle_set_stage(task, "verification", verification_status, note=ver_note)

    if execution_status == "failed" or verification_status == "failed":
        integration_status = "failed"
    elif verification_status == "done" and (replies > 0 or complete):
        integration_status = "done"
    elif execution_status == "running" or verification_status == "running":
        integration_status = "running"
    else:
        integration_status = "pending"
    lifecycle_set_stage(task, "integration", integration_status)

    if integration_status == "failed":
        close_status = "failed"
    elif integration_status == "done" and complete:
        close_status = "done"
    elif execution_status == "running" or verification_status == "running":
        close_status = "running"
    else:
        close_status = "pending"
    lifecycle_set_stage(task, "close", close_status)

    if close_status == "failed" or verification_status == "failed" or execution_status == "failed":
        overall = "failed"
    elif close_status == "done":
        overall = "completed"
    elif close_status == "running" or execution_status == "running" or verification_status == "running":
        overall = "running"
    else:
        overall = "pending"

    task["status"] = normalize_task_status(overall)
    task["roles"] = roles
    task["verifier_roles"] = verifiers
    task["require_verifier"] = bool(require_verifier)
    task["updated_at"] = now_iso()
    task["result"] = {
        "assignments": assignments,
        "replies": replies,
        "complete": complete,
        "done_roles": sorted(done_roles),
        "failed_roles": sorted(failed_roles),
        "pending_roles": sorted(pending_roles),
    }

    trim_project_tasks(ensure_project_tasks(entry))
    return task


def summarize_task_lifecycle(project_name: str, task: Dict[str, Any]) -> str:
    request_id = str(task.get("request_id", "-")).strip() or "-"
    label = task_display_label(task, fallback_request_id=request_id)
    status = str(task.get("status", "pending"))
    mode = str(task.get("mode", "dispatch"))
    roles = dedupe_roles(task.get("roles") or [])
    verifiers = dedupe_roles(task.get("verifier_roles") or [])

    stages = task.get("stages") or {}

    lines = [
        f"orch: {project_name}",
        f"task: {label}",
        f"request_id: {request_id}",
        f"status: {status}",
        f"mode: {mode}",
        f"roles: {', '.join(roles) if roles else '-'}",
        f"verifier_roles: {', '.join(verifiers) if verifiers else '-'}",
        "lifecycle:",
    ]

    for name in LIFECYCLE_STAGES:
        lines.append(f"- {name}: {str(stages.get(name, 'pending'))}")

    plan = task.get("plan")
    if isinstance(plan, dict):
        subtasks = plan.get("subtasks") or []
        plan_summary = str(plan.get("summary", "")).strip()
        if plan_summary:
            lines.append("plan_summary: " + plan_summary)
        lines.append(f"plan_subtasks: {len(subtasks)}")

        owner_counts: Dict[str, int] = {}
        for row in subtasks:
            if not isinstance(row, dict):
                continue
            role = str(row.get("owner_role", "")).strip() or "Worker"
            owner_counts[role] = owner_counts.get(role, 0) + 1
        if owner_counts:
            lines.append(
                "plan_owner_load: " + ", ".join(f"{role}={cnt}" for role, cnt in owner_counts.items())
            )

        for row in subtasks[:6]:
            if not isinstance(row, dict):
                continue
            sid = str(row.get("id", "")).strip() or "S"
            role = str(row.get("owner_role", "")).strip() or "Worker"
            title = str(row.get("title", "")).strip() or str(row.get("goal", "")).strip() or "subtask"
            lines.append(f"- plan {sid} [{role}] {title}")

    critic = task.get("plan_critic")
    if isinstance(critic, dict):
        issues = critic.get("issues") or []
        recs = critic.get("recommendations") or []
        approved = not critic_has_blockers(critic)
        lines.append(f"plan_critic: {'approved' if approved else 'needs_fix'}")
        for item in issues[:4]:
            token = str(item or "").strip()
            if token:
                lines.append("- issue: " + token)
        for item in recs[:4]:
            token = str(item or "").strip()
            if token:
                lines.append("- recommendation: " + token)

    gate = task.get("plan_gate_passed")
    if isinstance(gate, bool):
        lines.append(f"plan_gate: {'passed' if gate else 'blocked'}")

    replans = task.get("plan_replans")
    if isinstance(replans, list) and replans:
        lines.append(f"plan_replans: {len(replans)}")
        for row in replans[-3:]:
            if not isinstance(row, dict):
                continue
            attempt = int(row.get("attempt", 0) or 0)
            verdict = str(row.get("critic", "")).strip() or "unknown"
            subtasks = int(row.get("subtasks", 0) or 0)
            lines.append(f"- replan#{attempt}: critic={verdict} subtasks={subtasks}")

    result = task.get("result") or {}
    if isinstance(result, dict) and result:
        lines.append(
            "summary: assignments={a} replies={r} complete={c}".format(
                a=int(result.get("assignments", 0) or 0),
                r=int(result.get("replies", 0) or 0),
                c="yes" if bool(result.get("complete", False)) else "no",
            )
        )
        failed = result.get("failed_roles") or []
        pending = result.get("pending_roles") or []
        if failed:
            lines.append("failed_roles: " + ", ".join(str(x) for x in failed))
        if pending:
            lines.append("pending_roles: " + ", ".join(str(x) for x in pending))

    history = task.get("history") or []
    if isinstance(history, list) and history:
        lines.append("recent:")
        for ev in history[-6:]:
            if not isinstance(ev, dict):
                continue
            at = str(ev.get("at", ""))
            stage = str(ev.get("stage", ""))
            st = str(ev.get("status", ""))
            note = str(ev.get("note", "")).strip()
            row = f"- {at} {stage}:{st}"
            if note:
                row += f" ({note})"
            lines.append(row)

    return "\n".join(lines)



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
        "--overview",
        overview,
    ]
    proc = run_command(cmd, env=None, timeout_sec=max(60, int(args.orch_command_timeout_sec)))
    text = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
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
    active = normalize_project_name(str(state.get("active", "default")))
    projects = state.get("projects") or {}
    if not isinstance(projects, dict) or not projects:
        return "orch registry empty"

    lines = [f"active: {active}", "projects:"]
    for key in sorted(projects.keys()):
        entry = projects.get(key) or {}
        marker = "*" if key == active else "-"
        root = str(entry.get("project_root", "")).strip()
        last_req = str(entry.get("last_request_id", "")).strip()
        last_task_label = "-"
        if last_req:
            task = get_task_record(entry, last_req)
            if isinstance(task, dict):
                last_task_label = task_display_label(task, fallback_request_id=last_req)
            else:
                last_task_label = last_req
        lines.append(f"{marker} {key} | root={root} | last_task={last_task_label}")
    return "\n".join(lines)



def split_text(text: str, max_chars: int) -> List[str]:
    max_chars = max(200, int(max_chars))
    src = (text or "").strip()
    if not src:
        return ["(empty)"]
    if len(src) <= max_chars:
        return [src]

    lines = src.splitlines()
    chunks: List[str] = []
    buf: List[str] = []
    size = 0

    def flush() -> None:
        nonlocal buf, size
        if buf:
            chunks.append("\n".join(buf))
            buf = []
            size = 0

    for line in lines:
        candidate = line if len(line) <= max_chars else line[: max_chars - 3] + "..."
        add_len = len(candidate) + (1 if buf else 0)
        if size + add_len > max_chars:
            flush()
        buf.append(candidate)
        size += add_len

    flush()
    return chunks


def tg_api(token: str, method: str, payload: Dict[str, Any], timeout_sec: int) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise RuntimeError(f"Telegram API HTTP error ({method}): {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Telegram API URL error ({method}): {e}") from e

    try:
        data = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"Telegram API invalid JSON ({method}): {raw[:300]}") from e

    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error ({method}): {data}")

    return data


def tg_send_text(
    token: str,
    chat_id: str,
    text: str,
    max_chars: int,
    timeout_sec: int,
    dry_run: bool,
    reply_markup: Optional[Dict[str, Any]] = None,
) -> None:
    chunks = split_text(text, max_chars)
    for i, chunk in enumerate(chunks):
        if dry_run:
            print(f"[DRY-SEND chat_id={chat_id}]\n{chunk}\n")
            if i == 0 and isinstance(reply_markup, dict):
                print(f"[DRY-MARKUP chat_id={chat_id}] {json.dumps(reply_markup, ensure_ascii=False)}")
            continue

        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": chunk,
            "disable_web_page_preview": True,
        }
        if i == 0 and isinstance(reply_markup, dict):
            payload["reply_markup"] = reply_markup

        tg_api(
            token,
            "sendMessage",
            payload,
            timeout_sec=timeout_sec,
        )


def safe_tg_send_text(
    token: str,
    chat_id: str,
    text: str,
    max_chars: int,
    timeout_sec: int,
    dry_run: bool,
    verbose: bool,
    context: str = "",
    reply_markup: Optional[Dict[str, Any]] = None,
) -> bool:
    try:
        tg_send_text(token, chat_id, text, max_chars, timeout_sec, dry_run, reply_markup=reply_markup)
        return True
    except Exception as e:
        if verbose:
            suffix = f" ({context})" if context else ""
            print(f"[ERROR] sendMessage failed{suffix}: chat_id={chat_id} error={e}", file=sys.stderr, flush=True)
        return False


def tg_get_updates(token: str, offset: int, poll_timeout_sec: int, timeout_sec: int) -> List[Dict[str, Any]]:
    payload = {
        "offset": int(offset),
        "timeout": int(poll_timeout_sec),
        "allowed_updates": ["message"],
    }
    data = tg_api(token, "getUpdates", payload, timeout_sec=timeout_sec)
    result = data.get("result", [])
    if not isinstance(result, list):
        return []
    return [x for x in result if isinstance(x, dict)]


def build_quick_reply_keyboard() -> Dict[str, Any]:
    return {
        "keyboard": [
            [{"text": "/status"}, {"text": "/check"}],
            [{"text": "/task"}, {"text": "/monitor"}, {"text": "/pick"}],
            [{"text": "/kpi"}, {"text": "/cancel"}],
            [{"text": "/dispatch"}, {"text": "/direct"}],
            [{"text": "/help"}, {"text": "/whoami"}, {"text": "/acl"}, {"text": "/mode"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
        "input_field_placeholder": "예: /dispatch 결측치 규칙 정리해줘",
    }


def run_command(cmd: List[str], env: Optional[Dict[str, str]], timeout_sec: int) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd,
        text=True,
        capture_output=True,
        env=env,
        timeout=max(5, int(timeout_sec)),
    )


def choose_auto_dispatch_roles(prompt: str) -> List[str]:
    lower = prompt.lower()
    roles: List[str] = []

    data_keys = (
        "data",
        "dataset",
        "etl",
        "schema",
        "sql",
        "pipeline",
        "품질",
        "데이터",
        "스키마",
        "적재",
        "정합성",
        "검증",
    )
    review_keys = (
        "review",
        "risk",
        "regression",
        "test",
        "qa",
        "bug",
        "리뷰",
        "리스크",
        "회귀",
        "테스트",
        "버그",
        "검토",
    )
    both_keys = ("both", "둘 다", "둘다", "각각", "cross-check", "교차")

    if any(k in lower for k in data_keys):
        roles.append("DataEngineer")
    if any(k in lower for k in review_keys):
        if "Reviewer" not in roles:
            roles.append("Reviewer")

    if not roles and any(k in lower for k in both_keys):
        roles = ["DataEngineer", "Reviewer"]

    return roles


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
    return workers or ["Reviewer"]


def normalize_task_plan_payload(
    parsed: Optional[Dict[str, Any]],
    user_prompt: str,
    workers: List[str],
    max_subtasks: int,
) -> Dict[str, Any]:
    role_map = {r.lower(): r for r in workers}

    summary = ""
    raw_subtasks: List[Any] = []
    if isinstance(parsed, dict):
        summary = str(parsed.get("summary", "")).strip()
        st = parsed.get("subtasks")
        if isinstance(st, list):
            raw_subtasks = st

    normalized: List[Dict[str, Any]] = []
    for i, row in enumerate(raw_subtasks, start=1):
        if not isinstance(row, dict):
            continue

        sid = str(row.get("id", f"S{i}")).strip() or f"S{i}"
        title = str(row.get("title", "")).strip() or str(row.get("goal", "")).strip() or f"Subtask {i}"
        goal = str(row.get("goal", "")).strip() or title

        role_raw = str(row.get("owner_role", row.get("role", ""))).strip()
        role = role_map.get(role_raw.lower(), workers[min(i - 1, len(workers) - 1)])

        acceptance_in = row.get("acceptance")
        acceptance: List[str] = []
        if isinstance(acceptance_in, list):
            for item in acceptance_in:
                token = str(item or "").strip()
                if token:
                    acceptance.append(token)
        if not acceptance:
            acceptance = [f"{title} 결과가 사용자 요청과 직접 연결되어 설명된다."]

        normalized.append(
            {
                "id": sid,
                "title": title,
                "goal": goal,
                "owner_role": role,
                "acceptance": acceptance[:3],
            }
        )

    limit = max(1, int(max_subtasks))
    normalized = normalized[:limit]

    if not normalized:
        normalized = [
            {
                "id": "S1",
                "title": "요청 핵심 실행",
                "goal": user_prompt.strip(),
                "owner_role": workers[0],
                "acceptance": ["요청에 대한 실행/검증 결과가 사용자 관점으로 정리된다."],
            }
        ]

    if not summary:
        summary = f"subtasks={len(normalized)}"

    return {
        "summary": summary,
        "subtasks": normalized,
        "meta": {
            "max_subtasks": limit,
            "worker_roles": workers,
        },
    }


def critic_has_blockers(critic: Dict[str, Any]) -> bool:
    approved = bool(critic.get("approved", True))
    issues = critic.get("issues") or []
    return (not approved) or bool(issues)


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

    raw = run_codex_exec(args, planner_prompt, timeout_sec=min(600, max(90, int(args.orch_command_timeout_sec))))
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
        raw = run_codex_exec(args, critic_prompt, timeout_sec=min(480, max(90, int(args.orch_command_timeout_sec))))
        parsed = parse_json_object_from_text(raw)
    except Exception:
        parsed = None

    approved = True
    issues: List[str] = []
    recommendations: List[str] = []

    if isinstance(parsed, dict):
        approved = bool(parsed.get("approved", True))

        raw_issues = parsed.get("issues")
        if isinstance(raw_issues, list):
            for item in raw_issues:
                token = str(item or "").strip()
                if token:
                    issues.append(token)

        raw_rec = parsed.get("recommendations")
        if isinstance(raw_rec, list):
            for item in raw_rec:
                token = str(item or "").strip()
                if token:
                    recommendations.append(token)

    return {
        "approved": approved,
        "issues": issues[:5],
        "recommendations": recommendations[:5],
    }


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

    raw = run_codex_exec(args, repair_prompt, timeout_sec=min(600, max(90, int(args.orch_command_timeout_sec))))
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


def run_orchestrator_direct(args: argparse.Namespace, user_prompt: str) -> str:
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

def synthesize_orchestrator_response(args: argparse.Namespace, user_prompt: str, state: Dict[str, Any]) -> str:
    replies = state.get("replies") or []
    chunks: List[str] = []
    for r in replies[:8]:
        role = str(r.get("role", r.get("from", "agent"))).strip() or "agent"
        body = str(r.get("body", "")).strip()
        if body:
            chunks.append(f"[{role}]\n{body}")

    joined = "\n\n".join(chunks).strip() or "(no replies)"
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


def run_aoe_orch(
    args: argparse.Namespace,
    prompt: str,
    chat_id: str,
    roles_override: Optional[str] = None,
    priority_override: Optional[str] = None,
    timeout_override: Optional[int] = None,
    no_wait_override: Optional[bool] = None,
) -> Dict[str, Any]:
    effective_roles = args.roles if roles_override is None else (roles_override or "")
    effective_priority = (priority_override or args.priority or "P2").upper().strip()
    if effective_priority not in {"P1", "P2", "P3"}:
        effective_priority = "P2"
    effective_timeout = max(1, int(args.orch_timeout_sec if timeout_override is None else timeout_override))
    effective_no_wait = bool(args.no_wait if no_wait_override is None else no_wait_override)

    cmd: List[str] = [
        args.aoe_orch_bin,
        "run",
        "--project-root",
        str(args.project_root),
        "--team-dir",
        str(args.team_dir),
        "--priority",
        effective_priority,
        "--timeout-sec",
        str(effective_timeout),
        "--poll-sec",
        str(args.orch_poll_sec),
        "--channel",
        "telegram",
        "--origin",
        f"telegram:{chat_id}",
        "--json",
    ]

    if effective_roles:
        cmd.extend(["--roles", effective_roles])
    if args.no_spawn_missing:
        cmd.append("--no-spawn-missing")
    if effective_no_wait:
        cmd.append("--no-wait")

    cmd.append(prompt)

    proc = run_command(cmd, env=None, timeout_sec=args.orch_command_timeout_sec)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"aoe-orch run failed: {detail[:1000]}")

    payload = (proc.stdout or "").strip()
    try:
        data = json.loads(payload)
    except Exception as e:
        raise RuntimeError(f"aoe-orch run returned non-JSON output: {payload[:800]}") from e

    if not isinstance(data, dict):
        raise RuntimeError("aoe-orch run JSON is not an object")
    return data


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
    if proc.returncode != 0:
        raise RuntimeError(f"aoe-orch status failed: {text[:1200]}")
    return text


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


def render_run_response(state: Dict[str, Any], task: Optional[Dict[str, Any]] = None) -> str:
    request_id = str(state.get("request_id", "-")).strip() or "-"
    label = task_display_label(task or {}, fallback_request_id=request_id)
    complete = bool(state.get("complete", False))
    replies = state.get("replies") or []

    rendered: List[Tuple[str, str]] = []
    for item in replies:
        role = str(item.get("role", item.get("from", "assistant"))).strip() or "assistant"
        body = str(item.get("body", "")).strip()
        if body:
            rendered.append((role, body))

    if complete and rendered:
        if len(rendered) == 1:
            return rendered[0][1]

        lines: List[str] = []
        for role, body in rendered[:6]:
            lines.append(f"[{role}]")
            lines.append(body)
            lines.append("")
        return "\n".join(lines).strip()

    if not complete:
        return (
            f"작업 접수됨: {label}\n"
            f"진행: 진행 {label}\n"
            f"상세: 상세 {label}"
        )

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



def help_text() -> str:
    return (
        "AOE Telegram Gateway commands\n"
        "Quick mode (slash-only default)\n"
        "- /status /check /task /monitor /kpi /help\n"
        "- /mode [on|off|direct]\n"
        "- /on /off\n"
        "- /ok (고위험 자동실행 확인)\n"
        "- /whoami /lockme\n"
        "- /acl /grant /revoke\n"
        "- /pick <번호|task_label>\n"
        "- /dispatch <요청>   (서브에이전트 배정)\n"
        "- /direct <질문>     (오케스트레이터 직접 답변)\n"
        "- /dispatch 또는 /direct만 입력하면 다음 메시지 1회 모드\n"
        "- /cancel (대기 모드 해제)\n"
        "\n"
        "Slash mode\n"
        "- /help\n"
        "- /status\n"
        "- /mode [on|off|direct|dispatch]\n"
        "- /on /off\n"
        "- /ok\n"
        "- /acl\n"
        "- /grant <allow|admin|readonly> <chat_id|alias>\n"
        "- /revoke <allow|admin|readonly|all> <chat_id|alias>\n"
        "- /kpi [hours]\n"
        "- /pick <number|request_or_alias>\n"
        "- /cancel [request_or_alias]\n"
        "- /retry <request_or_alias>\n"
        "- /replan <request_or_alias>\n"
        "- /request <request_or_alias>\n"
        "- /run <prompt>\n"
        "\n"
        "CLI mode\n"
        "- aoe status\n"
        "- aoe mode [on|off|direct|dispatch]\n"
        "- aoe on | aoe off\n"
        "- aoe ok\n"
        "- aoe acl\n"
        "- aoe grant <allow|admin|readonly> <chat_id|alias>\n"
        "- aoe revoke <allow|admin|readonly|all> <chat_id|alias>\n"
        "- aoe kpi [hours]\n"
        "- aoe monitor [limit]\n"
        "- aoe pick <number|request_or_alias>\n"
        "- aoe cancel [request_or_alias]\n"
        "- aoe retry <request_or_alias>\n"
        "- aoe replan <request_or_alias>\n"
        "- aoe request <request_or_alias>\n"
        "- aoe run [--direct|--dispatch] [--roles <csv>] [--priority P1|P2|P3] [--timeout-sec N] [--no-wait] <prompt>\n"
        "- aoe add-role <Role> [--provider <name>] [--launch <cmd>] [--spawn|--no-spawn]\n"
        "\n"
        "Orch Manager\n"
        "- aoe orch list\n"
        "- aoe orch use <name>\n"
        "- aoe orch add <name> --path <project_root> [--overview <text>] [--init|--no-init] [--spawn|--no-spawn]\n"
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
        "- default: slash-only (plain text ignored unless pending/default mode)\n"
        "- default access: deny-by-default (allowlist required)\n"
        "- bootstrap: when allowlist is empty, only /lockme|/whoami|/help is accepted\n"
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


def is_bootstrap_allowed_command(text: str) -> bool:
    cmd, _ = parse_command(text)
    return cmd in {"start", "help", "id", "whoami", "lockme", "onlyme"}


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


def handle_text_message(
    args: argparse.Namespace,
    token: str,
    chat_id: str,
    text: str,
    trace_id: str = "",
) -> None:
    started_at = time.time()
    message_trace_id = str(trace_id or "").strip() or f"chat-{chat_id}-{int(started_at * 1000)}"
    raw_text = str(text or "")
    text_preview = raw_text if len(raw_text) <= 200 else raw_text[:197] + "..."
    text_preview = mask_sensitive_text(text_preview)
    resolved = ResolvedCommand()
    run_transition = RunTransitionState()

    manager_state = load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
    ensure_default_project_registered(manager_state, args.project_root, args.team_dir)
    default_log_team_dir = args.team_dir
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

    def send(body: str, context: str = "", with_menu: bool = False) -> bool:
        retries = int_from_env(os.environ.get("AOE_TG_SEND_RETRIES"), default=2, minimum=0, maximum=8)
        base_delay_ms = int_from_env(os.environ.get("AOE_TG_SEND_RETRY_DELAY_MS"), default=300, minimum=50, maximum=5000)
        attempt = 0
        ok = False
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
                reply_markup=(build_quick_reply_keyboard() if with_menu else None),
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
        p_args = make_project_args(args, entry)
        log_ctx["team_dir"] = p_args.team_dir
        return key, entry, p_args

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

        if not resolved.cmd and bool(args.slash_only):
            send(
                "입력 형식: 슬래시 명령만 지원합니다.\n"
                "예시: /dispatch <요청>, /direct <질문>, /mode on, /monitor, /check, /task, /pick, /help\n"
                "참고: /dispatch 또는 /direct는 다음 메시지 1회 평문 허용, /mode는 기본 평문 라우팅 모드를 고정합니다.",
                context="slash-only-hint",
                with_menu=True,
            )
            log_event(event="input_rejected", stage="intake", status="rejected", error_code=ERROR_COMMAND, detail="slash_only")
            return

        cmd_key = resolved.cmd or "run-default"
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
            help_text=help_text,
            get_default_mode=get_default_mode,
            get_pending_mode=get_pending_mode,
            set_default_mode=set_default_mode,
            set_pending_mode=set_pending_mode,
            clear_default_mode=clear_default_mode,
            clear_pending_mode=clear_pending_mode,
            clear_confirm_action=clear_confirm_action,
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
            help_text=help_text,
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
        )

        if handle_run_or_unknown_command(
            ctx=run_ctx,
            deps=run_deps,
        ):
            return

    except Exception as e:
        error_code, user_msg, next_step = classify_handler_error(e)
        send(
            format_error_message(error_code, user_msg, next_step, detail=str(e)),
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
    args.dry_run = True
    try:
        handle_text_message(args, token, chat_id, args.simulate_text, trace_id=f"sim-{int(time.time() * 1000)}")
    finally:
        args.dry_run = original_dry


def run_loop(args: argparse.Namespace, token: str) -> int:
    state = load_state(args.state_file)
    offset = int(state.get("offset", 0) or 0)
    processed = int(state.get("processed", 0) or 0)

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

            chat = msg.get("chat", {})
            chat_id = str(chat.get("id", ""))
            text = str(msg.get("text", "") or "")

            if not chat_id or not text:
                continue

            if args.verbose:
                preview = text if len(text) <= 120 else text[:117] + "..."
                print(f"[UPDATE] update_id={update_id} chat_id={chat_id} text={preview}")

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
            if (not allowed) and bool(args.deny_by_default) and acl_empty:
                bootstrap_allowed = is_bootstrap_allowed_command(text)
                if bootstrap_allowed:
                    allowed = True

            if not allowed:
                if args.verbose:
                    print(f"[SKIP] unauthorized chat_id={chat_id}")
                if chat_id not in unauthorized_sent:
                    unauthorized_text = "not allowed."
                    if bool(args.deny_by_default) and acl_empty:
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

            try:
                handle_text_message(args, token, chat_id, text, trace_id=f"upd-{update_id}")
            except Exception as e:
                if args.verbose:
                    print(f"[ERROR] message handling failed: chat_id={chat_id} error={e}", file=sys.stderr, flush=True)
            processed += 1

        if handled_any:
            save_state(args.state_file, offset=offset, processed=processed)

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
        "--owner-chat-id",
        default=os.environ.get("TELEGRAM_OWNER_CHAT_ID", os.environ.get("AOE_OWNER_CHAT_ID", "")),
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
