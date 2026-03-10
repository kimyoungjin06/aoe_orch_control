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
