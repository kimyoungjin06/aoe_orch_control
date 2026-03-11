#!/usr/bin/env python3
"""Runtime core helpers for gateway path resolution and state persistence."""

from __future__ import annotations

import fcntl
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Optional


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


def default_manager_state(project_root: Path, team_dir: Path, *, now_iso: Callable[[], str]) -> Dict[str, Any]:
    timestamp = now_iso()
    return {
        "version": 1,
        "active": "default",
        "project_lock": {},
        "updated_at": timestamp,
        "chat_sessions": {},
        "projects": {
            "default": {
                "name": "default",
                "display_name": "default",
                "project_alias": "O1",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "overview": "",
                "last_request_id": "",
                "tasks": {},
                "task_alias_index": {},
                "task_seq": 0,
                "todos": [],
                "todo_seq": 0,
                "todo_proposals": [],
                "todo_proposal_seq": 0,
                "system_project": True,
                "ops_hidden": True,
                "ops_hidden_reason": "internal fallback project",
                "paused": False,
                "paused_at": "",
                "paused_by": "",
                "paused_reason": "",
                "resumed_at": "",
                "resumed_by": "",
                "last_sync_at": "",
                "last_sync_mode": "",
                "created_at": timestamp,
                "updated_at": timestamp,
            }
        },
    }


def ensure_default_project_registered(
    state: Dict[str, Any],
    project_root: Path,
    team_dir: Path,
    *,
    now_iso: Callable[[], str],
    bool_from_json: Callable[[Any, bool], bool],
    normalize_project_alias: Callable[[str], str],
    normalize_project_name: Callable[[str], str],
    sanitize_project_lock_row: Callable[[Any, Dict[str, Any]], Dict[str, Any]],
    ensure_project_aliases: Callable[[Dict[str, Any]], Any],
    backfill_task_aliases: Callable[[Dict[str, Any]], Any],
) -> None:
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
            "project_alias": "O1",
            "project_root": str(project_root),
            "team_dir": str(team_dir),
            "overview": "",
            "last_request_id": "",
            "tasks": {},
            "task_alias_index": {},
            "task_seq": 0,
            "todos": [],
            "todo_seq": 0,
            "system_project": True,
            "ops_hidden": True,
            "ops_hidden_reason": "internal fallback project",
            "created_at": now_iso(),
            "updated_at": now_iso(),
        }

    for entry in projects.values():
        if isinstance(entry, dict):
            if "tasks" not in entry or not isinstance(entry.get("tasks"), dict):
                entry["tasks"] = {}
            if "task_alias_index" not in entry or not isinstance(entry.get("task_alias_index"), dict):
                entry["task_alias_index"] = {}
            if "todos" not in entry or not isinstance(entry.get("todos"), list):
                entry["todos"] = []
            entry["project_alias"] = normalize_project_alias(str(entry.get("project_alias", "")))
            entry["system_project"] = bool_from_json(entry.get("system_project"), str(entry.get("name", "")).strip().lower() == "default")
            entry["ops_hidden"] = bool_from_json(entry.get("ops_hidden"), bool(entry.get("system_project")))
            entry["ops_hidden_reason"] = str(entry.get("ops_hidden_reason", "")).strip()[:400]
            try:
                entry["task_seq"] = max(0, int(entry.get("task_seq", 0) or 0))
            except Exception:
                entry["task_seq"] = 0
            try:
                entry["todo_seq"] = max(0, int(entry.get("todo_seq", 0) or 0))
            except Exception:
                entry["todo_seq"] = 0
            backfill_task_aliases(entry)

    active = normalize_project_name(str(state.get("active", "default")))
    if active not in projects:
        state["active"] = "default"
    project_lock = sanitize_project_lock_row(state.get("project_lock"), projects)
    if project_lock:
        state["project_lock"] = project_lock
        state["active"] = str(project_lock.get("project_key", state.get("active", "default"))).strip() or "default"
    else:
        state.pop("project_lock", None)
    ensure_project_aliases(state)


def save_manager_state(
    path: Path,
    state: Dict[str, Any],
    *,
    now_iso: Callable[[], str],
    sync_investigations_docs: Callable[[Path, Dict[str, Any]], None],
    cleanup_tf_exec_artifacts: Callable[[Path, Dict[str, Any]], None],
    cleanup_room_logs: Callable[[Path], int],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(state)
    payload["updated_at"] = now_iso()
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)
    try:
        sync_investigations_docs(path, payload)
    except Exception as exc:
        print(f"[WARN] investigations_mo sync skipped: {exc}", file=sys.stderr)
    try:
        cleanup_tf_exec_artifacts(path, payload)
    except Exception as exc:
        print(f"[WARN] tf_exec cleanup skipped: {exc}", file=sys.stderr)
    try:
        cleanup_room_logs(path.parent.resolve())
    except Exception as exc:
        print(f"[WARN] room log gc skipped: {exc}", file=sys.stderr)


def acquire_process_lock(lock_path: Path, *, now_iso: Callable[[], str]) -> Any:
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
