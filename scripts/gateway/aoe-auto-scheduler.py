#!/usr/bin/env python3
"""Non-blocking Mother-Orch auto scheduler (tmux sidecar).

This process runs outside the Telegram polling gateway so long-running TF runs
do not block message intake. It reuses gateway logic by importing
`scripts/gateway/aoe-telegram-gateway.py` dynamically and calling
`handle_text_message()` with `/next` (or `/fanout`) periodically.

Runtime control is via a small JSON file under `.aoe-team/` (default:
`auto_scheduler.json`) which is updated by the Telegram command `/auto`.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


DEFAULT_INTERVAL_SEC = 2.0
DEFAULT_IDLE_SEC = 20.0
DEFAULT_PREFETCH_MIN_INTERVAL_SEC = 60.0
DEFAULT_PREFETCH_SINCE = "12h"
DEFAULT_MAX_FAILURES = 3
PROVIDER_CAPACITY_STATE_FILE = "provider_capacity.json"


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S%z")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _save_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(data)
    payload["updated_at"] = _now_iso()
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _parse_iso_dt(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _next_rate_limited_retry_at(state: Dict[str, Any], *, now: Optional[datetime] = None) -> str:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    projects = state.get("projects") if isinstance(state, dict) else {}
    best: Optional[datetime] = None
    if not isinstance(projects, dict):
        return ""
    for entry in projects.values():
        if not isinstance(entry, dict):
            continue
        tasks = entry.get("tasks")
        if not isinstance(tasks, dict):
            continue
        for task in tasks.values():
            if not isinstance(task, dict):
                continue
            rate_limit = task.get("rate_limit") if isinstance(task.get("rate_limit"), dict) else {}
            if str(rate_limit.get("mode", "")).strip().lower() != "blocked":
                continue
            retry_at = _parse_iso_dt(rate_limit.get("retry_at"))
            if retry_at is None or retry_at <= current:
                continue
            if best is None or retry_at < best:
                best = retry_at
    return best.isoformat() if isinstance(best, datetime) else ""


def _adjust_idle_for_retry_at(idle_sec: float, retry_at: str, *, now: Optional[datetime] = None) -> float:
    parsed = _parse_iso_dt(retry_at)
    if parsed is None:
        return float(idle_sec)
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    remaining = (parsed - current.astimezone(timezone.utc)).total_seconds()
    if remaining <= 0:
        return 1.0
    return max(1.0, min(float(idle_sec), remaining))


def _provider_capacity_policy(summary: Dict[str, Any]) -> Dict[str, str]:
    try:
        task_count = int(summary.get("task_count", 0) or 0)
    except Exception:
        task_count = 0
    try:
        project_count = int(summary.get("project_count", 0) or 0)
    except Exception:
        project_count = 0
    provider_counts = summary.get("provider_counts") if isinstance(summary.get("provider_counts"), dict) else {}
    provider_names = {str(key or "").strip().lower() for key in provider_counts.keys() if str(key or "").strip()}
    if task_count <= 0:
        return {}
    both_primary = {"codex", "claude"}.issubset(provider_names)
    if both_primary and (task_count >= 2 or project_count >= 2):
        return {
            "level": "critical",
            "reason": "both primary providers are blocked across multiple tasks/projects",
            "operator_action": "/auto off",
        }
    if both_primary:
        return {
            "level": "elevated",
            "reason": "both primary providers are blocked",
            "operator_action": "/auto status",
        }
    if task_count >= 2 or project_count >= 2:
        return {
            "level": "elevated",
            "reason": "provider cooldown is affecting multiple tasks/projects",
            "operator_action": "/offdesk review",
        }
    return {
        "level": "cooldown",
        "reason": "provider cooldown is isolated to a single task",
        "operator_action": "/auto status",
    }


def _provider_cooldown_level(
    blocked_count: int,
    project_count: int,
    next_retry_at: str,
    *,
    now: Optional[datetime] = None,
) -> str:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    retry_dt = _parse_iso_dt(next_retry_at)
    retry_wait_sec = max(0.0, (retry_dt - current.astimezone(timezone.utc)).total_seconds()) if retry_dt else 0.0
    if blocked_count >= 3 or (blocked_count >= 2 and project_count >= 2) or retry_wait_sec >= 1800:
        return "critical"
    if blocked_count >= 2 and project_count >= 2:
        return "critical"
    if blocked_count >= 2 or project_count >= 2 or retry_wait_sec >= 600:
        return "elevated"
    return "cooldown"


def _provider_retry_wait_bucket(
    next_retry_at: str,
    *,
    now: Optional[datetime] = None,
) -> str:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    retry_dt = _parse_iso_dt(next_retry_at)
    retry_wait_sec = max(0.0, (retry_dt - current.astimezone(timezone.utc)).total_seconds()) if retry_dt else 0.0
    if retry_wait_sec >= 1800:
        return "long"
    if retry_wait_sec >= 540:
        return "medium"
    return "short"


def _provider_capacity_snapshot(state: Dict[str, Any], *, now: Optional[datetime] = None) -> Dict[str, Any]:
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    current_iso = current.astimezone(timezone.utc).isoformat()
    projects = state.get("projects") if isinstance(state, dict) else {}
    if not isinstance(projects, dict):
        return {"summary": {}, "providers": {}}

    provider_rows: Dict[str, Dict[str, Any]] = {}
    project_aliases: set[str] = set()
    task_count = 0
    next_retry_at = ""
    best_retry_dt: Optional[datetime] = None

    for key, entry in projects.items():
        if not isinstance(entry, dict):
            continue
        alias = str(entry.get("project_alias", "")).strip().upper() or str(key or "").strip() or "-"
        tasks = entry.get("tasks")
        if not isinstance(tasks, dict):
            continue
        project_has_limited_task = False
        for request_id, task in tasks.items():
            if not isinstance(task, dict):
                continue
            rate_limit = task.get("rate_limit") if isinstance(task.get("rate_limit"), dict) else {}
            if str(rate_limit.get("mode", "")).strip().lower() != "blocked":
                continue
            task_count += 1
            project_has_limited_task = True
            task_ref = str(task.get("label", "")).strip() or str(task.get("short_id", "")).strip() or str(request_id or "").strip() or "-"
            retry_at = str(rate_limit.get("retry_at", "")).strip()
            retry_dt = _parse_iso_dt(retry_at)
            if retry_dt is not None and retry_dt > current and (best_retry_dt is None or retry_dt < best_retry_dt):
                best_retry_dt = retry_dt
                next_retry_at = retry_dt.isoformat()
            for raw_provider in rate_limit.get("limited_providers") or []:
                provider = str(raw_provider or "").strip().lower()
                if not provider:
                    continue
                row = provider_rows.setdefault(
                    provider,
                    {
                        "blocked_count": 0,
                        "projects": set(),
                        "tasks": set(),
                        "last_retry_at": "",
                        "last_seen_at": current_iso,
                    },
                )
                row["blocked_count"] = int(row.get("blocked_count", 0) or 0) + 1
                row["projects"].add(alias)
                row["tasks"].add(f"{alias}:{task_ref}")
                row["last_seen_at"] = current_iso
                prev_next = _parse_iso_dt(row.get("next_retry_at"))
                if retry_dt is not None and (prev_next is None or retry_dt < prev_next):
                    row["next_retry_at"] = retry_dt.isoformat()
                if retry_dt is not None:
                    prev_retry = _parse_iso_dt(row.get("last_retry_at"))
                    if prev_retry is None or retry_dt > prev_retry:
                        row["last_retry_at"] = retry_dt.isoformat()
        if project_has_limited_task:
            project_aliases.add(alias)

    if task_count <= 0:
        return {"summary": {}, "providers": {}}

    provider_counts = {provider: int(row.get("blocked_count", 0) or 0) for provider, row in provider_rows.items()}
    ordered = sorted(provider_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    provider_summary = ", ".join(f"{provider}={count}" for provider, count in ordered) or "-"
    summary = {
        "task_count": str(task_count),
        "project_count": str(len(project_aliases)),
        "provider_summary": provider_summary,
        "provider_counts": provider_counts,
        "next_retry_at": next_retry_at,
    }
    policy = _provider_capacity_policy(summary)
    if policy:
        summary["policy_level"] = str(policy.get("level", "")).strip()
        summary["policy_reason"] = str(policy.get("reason", "")).strip()
        summary["operator_action"] = str(policy.get("operator_action", "")).strip()

    normalized_rows: Dict[str, Dict[str, Any]] = {}
    for provider, row in provider_rows.items():
        projects = sorted(str(x) for x in row.get("projects", set()) if str(x).strip())
        tasks = sorted(str(x) for x in row.get("tasks", set()) if str(x).strip())
        blocked_count = int(row.get("blocked_count", 0) or 0)
        next_retry = str(row.get("next_retry_at", "")).strip()
        normalized_rows[provider] = {
            "blocked_count": blocked_count,
            "project_count": len(projects),
            "task_count": len(tasks),
            "projects": projects,
            "tasks": tasks,
            "next_retry_at": next_retry,
            "last_retry_at": str(row.get("last_retry_at", "")).strip(),
            "last_seen_at": current_iso,
            "cooldown_level": _provider_cooldown_level(blocked_count, len(projects), next_retry, now=current),
            "retry_wait_bucket": _provider_retry_wait_bucket(next_retry, now=current),
        }
    return {"summary": summary, "providers": normalized_rows}


def _auto_enabled(auto_state: Dict[str, Any]) -> bool:
    return bool(auto_state.get("enabled", False))


def _auto_chat_id(auto_state: Dict[str, Any], fallback: str) -> str:
    token = str(auto_state.get("chat_id", "")).strip()
    return token or str(fallback or "").strip()


def _auto_force(auto_state: Dict[str, Any], fallback: bool) -> bool:
    if "force" in auto_state:
        return bool(auto_state.get("force", False))
    return bool(fallback)


def _auto_command(auto_state: Dict[str, Any], fallback: str) -> str:
    token = str(auto_state.get("command", "")).strip().lower()
    if token in {"next", "fanout"}:
        return token
    fb = str(fallback or "").strip().lower()
    return fb if fb in {"next", "fanout"} else "next"


def _auto_prefetch(auto_state: Dict[str, Any]) -> str:
    token = str(auto_state.get("prefetch", "")).strip().lower()
    if token in {"recent", "recent_docs", "sync-recent"}:
        token = "sync_recent"
    return token if token in {"sync_recent"} else ""


def _auto_prefetch_replace_sync(auto_state: Dict[str, Any]) -> bool:
    return bool(auto_state.get("prefetch_replace_sync", False))


def _auto_prefetch_min_interval(auto_state: Dict[str, Any], fallback: float) -> float:
    raw = auto_state.get("prefetch_min_interval_sec")
    try:
        val = float(raw)
    except Exception:
        val = float(fallback)
    return max(5.0, min(3600.0, val))


def _auto_prefetch_since(auto_state: Dict[str, Any], fallback: str) -> str:
    token = str(auto_state.get("prefetch_since", "")).strip()
    if token:
        return token
    env = str(os.environ.get("AOE_AUTO_PREFETCH_SINCE", "")).strip()
    return env or str(fallback or "").strip()


def _prefetch_plan(prefetch: str, prefetch_since: str, replace_sync: bool) -> Tuple[str, list[Tuple[str, str]]]:
    if prefetch != "sync_recent":
        return "", []
    if replace_sync:
        return "sync_recent+replace (full-scope; since ignored)", [("/sync replace all quiet", "replace")]
    since_arg = f" since {prefetch_since}" if prefetch_since else ""
    since_disp = prefetch_since or "-"
    return (
        f"sync files+salvage all since={since_disp} quiet",
        [
            (f"/sync files all{since_arg} quiet", "files"),
            (f"/sync salvage all{since_arg} quiet", "salvage"),
        ],
    )


def _auto_interval(auto_state: Dict[str, Any], fallback: float) -> float:
    raw = auto_state.get("interval_sec")
    try:
        val = float(raw)
    except Exception:
        val = float(fallback)
    return max(0.5, min(300.0, val))


def _auto_idle(auto_state: Dict[str, Any], fallback: float) -> float:
    raw = auto_state.get("idle_sec")
    try:
        val = float(raw)
    except Exception:
        val = float(fallback)
    return max(1.0, min(3600.0, val))


def _auto_max_failures(auto_state: Dict[str, Any], fallback: int) -> int:
    raw = auto_state.get("max_failures")
    try:
        val = int(raw)
    except Exception:
        env = str(os.environ.get("AOE_AUTO_MAX_FAILURES", "") or "").strip()
        try:
            val = int(env)
        except Exception:
            val = int(fallback)
    return max(1, min(50, int(val)))


def _load_gateway_module(gateway_path: Path) -> Any:
    sys.path.insert(0, str(gateway_path.parent.resolve()))
    spec = importlib.util.spec_from_file_location("aoe_telegram_gateway", str(gateway_path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load gateway spec: {gateway_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _build_gateway_args(gw: Any, project_root: Path, team_dir: Path, verbose: bool) -> argparse.Namespace:
    # Reuse the gateway parser so env-based defaults are consistent.
    parser = gw.build_parser()
    argv = [
        "--project-root",
        str(project_root),
        "--team-dir",
        str(team_dir),
    ]
    if verbose:
        argv.append("--verbose")
    args = parser.parse_args(argv)

    # Apply the same normalization steps as gateway.main(), but skip instance lock / run_loop.
    args.project_root = gw.resolve_project_root(args.project_root)
    args.team_dir = gw.resolve_team_dir(args.project_root, args.team_dir)
    args.state_file = gw.resolve_state_file(args.project_root, getattr(args, "state_file", None))
    args.manager_state_file = gw.resolve_manager_state_file(args.team_dir, getattr(args, "manager_state_file", ""))
    args.chat_aliases_file = gw.resolve_chat_aliases_file(args.team_dir, getattr(args, "chat_aliases_file", ""))
    if str(getattr(args, "instance_lock_file", "") or "").strip():
        args.instance_lock_file = Path(str(args.instance_lock_file)).expanduser().resolve()
    else:
        args.instance_lock_file = (args.team_dir / ".gateway.instance.lock").resolve()

    args.workspace_root = gw.resolve_workspace_root(getattr(args, "workspace_root", ""))
    args.owner_chat_id = gw.normalize_owner_chat_id(getattr(args, "owner_chat_id", ""))
    args.default_lang = gw.normalize_chat_lang_token(args.default_lang, gw.DEFAULT_UI_LANG) or gw.DEFAULT_UI_LANG
    args.default_reply_lang = gw.normalize_chat_lang_token(args.default_reply_lang, gw.DEFAULT_REPLY_LANG) or gw.DEFAULT_REPLY_LANG
    raw_default_report = gw.normalize_report_token(str(getattr(args, "default_report_level", "") or "").strip())
    args.default_report_level = raw_default_report if raw_default_report in {"short", "normal", "long"} else gw.DEFAULT_REPORT_LEVEL

    args.allow_chat_ids = gw.parse_csv_set(getattr(args, "allow_chat_ids", ""))
    args.admin_chat_ids = gw.parse_csv_set(getattr(args, "admin_chat_ids", ""))
    args.readonly_chat_ids = gw.parse_csv_set(getattr(args, "readonly_chat_ids", ""))
    args.readonly_chat_ids = {x for x in args.readonly_chat_ids if x not in args.admin_chat_ids}
    args.chat_alias_cache = gw.load_chat_aliases(args.chat_aliases_file)

    return args


def _peek_next(gw: Any, args: argparse.Namespace, chat_id: str, force: bool) -> Tuple[str, str, str]:
    state = gw.load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
    recovery_grace_until = ""
    try:
        auto_state_path = Path(str(getattr(args, "team_dir", "."))).expanduser().resolve() / "auto_scheduler.json"
        auto_state = _load_json(auto_state_path)
        if bool(auto_state.get("enabled", False)):
            recovery_grace_until = str(auto_state.get("recovery_grace_until", "")).strip()
    except Exception:
        recovery_grace_until = ""
    try:
        return gw._drain_peek_next_todo(state, chat_id, force=force, recovery_grace_until=recovery_grace_until)
    except Exception:
        return "", "", "peek_error"


def _is_confirm_pending(gw: Any, args: argparse.Namespace, chat_id: str) -> bool:
    try:
        state = gw.load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
    except Exception:
        return False
    try:
        return bool(gw.get_confirm_action(state, chat_id))
    except Exception:
        return False


def _did_candidate_make_progress(gw: Any, args: argparse.Namespace, chat_id: str, project_key: str, todo_id: str) -> bool:
    """Best-effort loop guard.

    Progress is when:
    - pending_todo cleared, or
    - todo status moved away from 'open', or
    - a task got linked to this todo_id.
    """

    try:
        state = gw.load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
    except Exception:
        return True

    projects = state.get("projects") if isinstance(state, dict) else {}
    entry = projects.get(project_key) if isinstance(projects, dict) and isinstance(projects.get(project_key), dict) else {}
    if not isinstance(entry, dict):
        return True

    pending = entry.get("pending_todo")
    if not (
        isinstance(pending, dict)
        and str(pending.get("chat_id", "")).strip() == str(chat_id or "").strip()
        and str(pending.get("todo_id", "")).strip() == str(todo_id or "").strip()
    ):
        return True

    todos = entry.get("todos")
    if isinstance(todos, list):
        for row in todos:
            if not isinstance(row, dict):
                continue
            if str(row.get("id", "")).strip() != str(todo_id or "").strip():
                continue
            st = str(row.get("status", "open")).strip().lower() or "open"
            if st != "open":
                return True

    tasks = entry.get("tasks")
    if isinstance(tasks, dict):
        for t in tasks.values():
            if not isinstance(t, dict):
                continue
            if str(t.get("todo_id", "")).strip() != str(todo_id or "").strip():
                continue
            return True

    return False


def _candidate_todo_status(gw: Any, args: argparse.Namespace, project_key: str, todo_id: str) -> Tuple[str, str]:
    """Return (status, blocked_reason) for the todo row, best-effort."""

    try:
        state = gw.load_manager_state(args.manager_state_file, args.project_root, args.team_dir)
    except Exception:
        return "", ""

    projects = state.get("projects") if isinstance(state, dict) else {}
    entry = projects.get(project_key) if isinstance(projects, dict) and isinstance(projects.get(project_key), dict) else {}
    if not isinstance(entry, dict):
        return "", ""

    todo_token = str(todo_id or "").strip()
    if not todo_token:
        return "", ""

    raw_todos = entry.get("todos")
    todos = [r for r in raw_todos if isinstance(r, dict)] if isinstance(raw_todos, list) else []
    for row in todos:
        if str(row.get("id", "")).strip() != todo_token:
            continue
        status = str(row.get("status", "")).strip().lower()
        blocked_reason = str(row.get("blocked_reason", "")).strip()
        return status, blocked_reason

    return "", ""


def main() -> int:
    p = argparse.ArgumentParser(prog="aoe-auto-scheduler", description="AOE Mother-Orch auto scheduler (tmux sidecar)")
    p.add_argument("--project-root", default=".")
    p.add_argument("--team-dir", default="")
    p.add_argument("--auto-state-file", default="")
    p.add_argument("--chat-id", default=os.environ.get("TELEGRAM_OWNER_CHAT_ID", os.environ.get("AOE_OWNER_CHAT_ID", "")))
    p.add_argument("--force", action="store_true", help="ignore busy checks (same as /next force)")
    p.add_argument("--interval-sec", type=float, default=DEFAULT_INTERVAL_SEC)
    p.add_argument("--idle-sec", type=float, default=DEFAULT_IDLE_SEC)
    p.add_argument("--once", action="store_true", help="run a single scheduling attempt then exit")
    p.add_argument("--verbose", action="store_true")
    args0 = p.parse_args()

    project_root = Path(args0.project_root).expanduser().resolve()
    team_dir = Path(args0.team_dir).expanduser().resolve() if str(args0.team_dir).strip() else (project_root / ".aoe-team")
    auto_state_path = (
        Path(args0.auto_state_file).expanduser().resolve()
        if str(args0.auto_state_file).strip()
        else (team_dir / "auto_scheduler.json").resolve()
    )
    provider_capacity_state_path = (team_dir / PROVIDER_CAPACITY_STATE_FILE).resolve()
    chat_id_fallback = str(args0.chat_id or "").strip()

    gateway_path = (project_root / "scripts" / "gateway" / "aoe-telegram-gateway.py").resolve()
    if not gateway_path.exists():
        raise SystemExit(f"[ERROR] gateway not found: {gateway_path}")

    token = (os.environ.get("TELEGRAM_BOT_TOKEN") or "").strip()
    if not token:
        raise SystemExit("[ERROR] missing TELEGRAM_BOT_TOKEN (source .aoe-team/telegram.env)")

    gw = _load_gateway_module(gateway_path)
    gw_args = _build_gateway_args(gw, project_root, team_dir, verbose=bool(args0.verbose))
    gw_args.dry_run = False
    # Mark this process as auto invocation (used for history suppression and loop guards).
    gw_args._aoe_invocation = "auto"

    if not chat_id_fallback:
        raise SystemExit("[ERROR] missing chat id (set TELEGRAM_OWNER_CHAT_ID or pass --chat-id)")

    last_idle_reason = ""
    while True:
        auto_state = _load_json(auto_state_path)
        enabled = _auto_enabled(auto_state)
        chat_id = _auto_chat_id(auto_state, chat_id_fallback)
        force = _auto_force(auto_state, bool(args0.force))
        command = _auto_command(auto_state, "next")
        prefetch = _auto_prefetch(auto_state)
        replace_sync = _auto_prefetch_replace_sync(auto_state)
        prefetch_min_interval = _auto_prefetch_min_interval(auto_state, DEFAULT_PREFETCH_MIN_INTERVAL_SEC)
        prefetch_since = _auto_prefetch_since(auto_state, DEFAULT_PREFETCH_SINCE)
        interval_sec = _auto_interval(auto_state, float(args0.interval_sec))
        idle_sec = _auto_idle(auto_state, float(args0.idle_sec))

        try:
            manager_state = gw.load_manager_state(gw_args.manager_state_file, gw_args.project_root, gw_args.team_dir)
            lock_key = str(gw.get_project_lock_key(manager_state)).strip() if hasattr(gw, "get_project_lock_key") else ""
        except Exception:
            lock_key = ""
        if lock_key and command == "fanout":
            command = "next"
            auto_state["command"] = "next"
            auto_state["last_reason"] = f"project_lock:{lock_key}:forced_next"
            _save_json(auto_state_path, auto_state)

        if not enabled:
            last_idle_reason = "disabled"
            if args0.once:
                return 0
            time.sleep(2.0)
            continue

        # Stop the auto loop when a manual confirm is pending, otherwise /next may spam forever.
        if _is_confirm_pending(gw, gw_args, chat_id):
            last_idle_reason = "confirm_pending"
            try:
                auto_state["enabled"] = False
                auto_state["stopped_at"] = _now_iso()
                auto_state["stopped_reason"] = last_idle_reason
                auto_state["last_reason"] = last_idle_reason
                _save_json(auto_state_path, auto_state)
            except Exception:
                pass
            try:
                gw.safe_tg_send_text(
                    token,
                    chat_id,
                    "AUTO stopped: confirm pending.\n"
                    "- resolve: /ok or /cancel\n"
                    "- then: /auto on\n",
                    max_chars=3800,
                    timeout_sec=60,
                    dry_run=False,
                    verbose=bool(args0.verbose),
                    context="auto-confirm-pending",
                )
            except Exception:
                pass
            if args0.once:
                return 0
            time.sleep(max(2.0, float(idle_sec)))
            continue

        project_key, todo_id, reason = _peek_next(gw, gw_args, chat_id, force=force)
        if not project_key or not todo_id:
            # Optional prefetch: when idle (no runnable todo), try to seed queue from recent docs.
            if prefetch == "sync_recent" and (reason or "") == "no_runnable_open_todo":
                now = time.time()
                last_ts = 0.0
                try:
                    last_ts = float(auto_state.get("last_prefetch_ts") or 0.0)
                except Exception:
                    last_ts = 0.0
                if (now - last_ts) >= float(prefetch_min_interval):
                    trace_id = f"auto-{int(now * 1000)}"
                    prefetch_desc, prefetch_commands = _prefetch_plan(prefetch, prefetch_since, replace_sync)
                    if args0.verbose:
                        print(
                            f"[AUTO] prefetch: {prefetch_desc or '-'} (min_interval={prefetch_min_interval}s)",
                            flush=True,
                        )
                    try:
                        auto_state["last_prefetch_at"] = _now_iso()
                        auto_state["last_prefetch_ts"] = int(now)
                        auto_state["last_prefetch_reason"] = str(reason or "").strip() or "idle"
                        auto_state["last_prefetch_mode"] = "replace_sync" if replace_sync else "sync_recent"
                        _save_json(auto_state_path, auto_state)
                    except Exception:
                        pass
                    try:
                        for cmd_text, label in prefetch_commands:
                            gw.handle_text_message(
                                gw_args,
                                token,
                                chat_id,
                                cmd_text,
                                trace_id=f"{trace_id}/prefetch/{label}",
                            )
                    except Exception as exc:
                        if args0.verbose:
                            print(f"[AUTO] prefetch failed: {exc}", flush=True)
                    project_key, todo_id, reason = _peek_next(gw, gw_args, chat_id, force=force)

            if not project_key or not todo_id:
                last_idle_reason = reason or "idle"
                next_retry_at = ""
                try:
                    state = gw.load_manager_state(gw_args.manager_state_file, gw_args.project_root, gw_args.team_dir)
                    next_retry_at = _next_rate_limited_retry_at(state)
                    capacity_snapshot = _provider_capacity_snapshot(state)
                except Exception:
                    next_retry_at = ""
                    capacity_snapshot = {"summary": {}, "providers": {}}
                sleep_sec = _adjust_idle_for_retry_at(idle_sec, next_retry_at) if next_retry_at else idle_sec
                if args0.verbose:
                    retry_hint = f" next_retry_at={next_retry_at}" if next_retry_at else ""
                    print(f"[AUTO] idle: reason={last_idle_reason}{retry_hint} sleep={sleep_sec}s", flush=True)
                # write last reason for status visibility (best-effort)
                try:
                    auto_state["last_reason"] = last_idle_reason
                    auto_state["last_checked_at"] = _now_iso()
                    if next_retry_at:
                        auto_state["next_retry_at"] = next_retry_at
                    else:
                        auto_state.pop("next_retry_at", None)
                    _save_json(auto_state_path, auto_state)
                    previous_capacity = _load_json(provider_capacity_state_path)
                    capacity_payload = dict(capacity_snapshot)
                    history = previous_capacity.get("override_history") if isinstance(previous_capacity.get("override_history"), list) else []
                    if history:
                        capacity_payload["override_history"] = [row for row in history if isinstance(row, dict)][-10:]
                    _save_json(provider_capacity_state_path, capacity_payload)
                except Exception:
                    pass
                if args0.once:
                    return 0
                time.sleep(sleep_sec)
                continue

        if args0.verbose:
            print(
                f"[AUTO] run: /{command}{' force' if force else ''} (candidate={project_key}:{todo_id})",
                flush=True,
            )
        try:
            auto_state["last_reason"] = reason or "run"
            auto_state["last_run_at"] = _now_iso()
            auto_state["last_candidate"] = f"{project_key}:{todo_id}"
            auto_state["last_command"] = command
            _save_json(auto_state_path, auto_state)
        except Exception:
            pass

        trace_id = f"auto-{int(time.time() * 1000)}"
        gw.handle_text_message(
            gw_args,
            token,
            chat_id,
            f"/{command}{' force' if force else ''}",
            trace_id=trace_id,
        )

        # Loop guard: if the same pending todo survives after a run attempt, stop to avoid infinite spam.
        progressed = True
        try:
            progressed = _did_candidate_make_progress(gw, gw_args, chat_id, project_key, todo_id)
        except Exception:
            progressed = True

        try:
            auto_state2 = _load_json(auto_state_path)
        except Exception:
            auto_state2 = dict(auto_state)

        try:
            if progressed:
                auto_state2.pop("stuck_candidate", None)
                auto_state2.pop("stuck_count", None)
                auto_state2.pop("stuck_since", None)
                _save_json(auto_state_path, auto_state2)
            else:
                candidate = f"{project_key}:{todo_id}"
                if str(auto_state2.get("stuck_candidate", "")).strip() == candidate:
                    auto_state2["stuck_count"] = int(auto_state2.get("stuck_count") or 0) + 1
                else:
                    auto_state2["stuck_candidate"] = candidate
                    auto_state2["stuck_count"] = 1
                    auto_state2["stuck_since"] = _now_iso()
                auto_state2["last_reason"] = "stuck_no_progress"
                if int(auto_state2.get("stuck_count") or 0) >= 5:
                    auto_state2["enabled"] = False
                    auto_state2["stopped_at"] = _now_iso()
                    auto_state2["stopped_reason"] = "stuck_no_progress"
                    _save_json(auto_state_path, auto_state2)
                    try:
                        gw.safe_tg_send_text(
                            token,
                            chat_id,
                            "AUTO stopped: stuck (no progress after /next).\n"
                            f"- candidate: {candidate}\n"
                            "next:\n"
                            "- /queue\n"
                            f"- /todo {project_key}\n"
                            "- /next force\n"
                            "- /panic\n",
                            max_chars=3800,
                            timeout_sec=60,
                            dry_run=False,
                            verbose=bool(args0.verbose),
                            context="auto-stuck",
                        )
                    except Exception:
                        pass
                else:
                    _save_json(auto_state_path, auto_state2)

            # Failure budget: if repeated runs keep blocking/failing, stop auto to avoid spamming.
            if bool(auto_state2.get("enabled", True)):
                max_failures = _auto_max_failures(auto_state2, DEFAULT_MAX_FAILURES)
                status, blocked_reason = _candidate_todo_status(gw, gw_args, project_key, todo_id)
                status = str(status or "").strip().lower()

                fail_count = 0
                try:
                    fail_count = int(auto_state2.get("fail_count") or 0)
                except Exception:
                    fail_count = 0

                if status in {"done", "running"}:
                    fail_count = 0
                    auto_state2.pop("fail_count", None)
                    auto_state2.pop("fail_candidate", None)
                    auto_state2.pop("fail_status", None)
                    auto_state2.pop("fail_reason", None)
                elif status in {"blocked", "failed"}:
                    fail_count += 1
                    auto_state2["fail_count"] = fail_count
                    auto_state2["fail_candidate"] = f"{project_key}:{todo_id}"
                    auto_state2["fail_status"] = status
                    if blocked_reason:
                        auto_state2["fail_reason"] = blocked_reason[:240]
                    else:
                        auto_state2.pop("fail_reason", None)
                elif status == "open" and progressed:
                    # We ran something, but the todo is still open: treat as failure-like.
                    fail_count += 1
                    auto_state2["fail_count"] = fail_count
                    auto_state2["fail_candidate"] = f"{project_key}:{todo_id}"
                    auto_state2["fail_status"] = "open"
                    auto_state2["fail_reason"] = "no_effect_after_run"

                if fail_count and int(fail_count) >= int(max_failures):
                    candidate = f"{project_key}:{todo_id}"
                    auto_state2["enabled"] = False
                    auto_state2["stopped_at"] = _now_iso()
                    auto_state2["stopped_reason"] = "too_many_failures"
                    auto_state2["last_reason"] = "too_many_failures"
                    _save_json(auto_state_path, auto_state2)
                    try:
                        reason_line = f"- reason: {auto_state2.get('fail_reason','-')}" if auto_state2.get("fail_reason") else ""
                        gw.safe_tg_send_text(
                            token,
                            chat_id,
                            "AUTO stopped: too many failures.\n"
                            f"- candidate: {candidate}\n"
                            f"- fail_count: {fail_count}/{max_failures}\n"
                            + (reason_line + "\n" if reason_line else "")
                            + "next:\n"
                            "- /queue\n"
                            "- /auto off\n"
                            "- /auto on\n"
                            "- /panic\n",
                            max_chars=3800,
                            timeout_sec=60,
                            dry_run=False,
                            verbose=bool(args0.verbose),
                            context="auto-too-many-failures",
                        )
                    except Exception:
                        pass
                else:
                    _save_json(auto_state_path, auto_state2)
        except Exception:
            pass

        if args0.once:
            return 0
        time.sleep(interval_sec)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(0)
