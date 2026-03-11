#!/usr/bin/env python3
"""Project alias/lock/registry state helpers extracted from the gateway monolith."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Set, Tuple

from aoe_tg_task_view import normalize_project_alias, normalize_project_name


def extract_project_alias_index(alias: str) -> int:
    token = normalize_project_alias(alias)
    if not token:
        return 10**9
    return int(token[1:])


def ensure_project_aliases(state: Dict[str, Any], max_alias: int = 999) -> Dict[str, str]:
    projects = state.get("projects") or {}
    if not isinstance(projects, dict):
        return {}

    alias_to_key: Dict[str, str] = {}
    used: Set[int] = set()
    needs_assign = []

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


def sanitize_project_lock_row(
    raw: Any,
    projects: Any,
    *,
    bool_from_json: Callable[[Any, bool], bool],
) -> Dict[str, Any]:
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


def get_project_lock_row(
    state: Dict[str, Any],
    *,
    bool_from_json: Callable[[Any, bool], bool],
) -> Dict[str, Any]:
    projects = state.get("projects")
    row = sanitize_project_lock_row(state.get("project_lock"), projects, bool_from_json=bool_from_json)
    if row:
        state["project_lock"] = row
        return row
    state.pop("project_lock", None)
    return {}


def get_project_lock_key(
    state: Dict[str, Any],
    *,
    bool_from_json: Callable[[Any, bool], bool],
) -> str:
    row = get_project_lock_row(state, bool_from_json=bool_from_json)
    return str(row.get("project_key", "")).strip()


def set_project_lock(
    state: Dict[str, Any],
    project_key: str,
    *,
    now_iso: Callable[[], str],
    actor: str = "",
) -> Dict[str, Any]:
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


def clear_project_lock(
    state: Dict[str, Any],
    *,
    bool_from_json: Callable[[Any, bool], bool],
) -> bool:
    existed = bool(get_project_lock_row(state, bool_from_json=bool_from_json))
    state.pop("project_lock", None)
    return existed


def project_lock_label(
    state: Dict[str, Any],
    *,
    bool_from_json: Callable[[Any, bool], bool],
) -> str:
    key = get_project_lock_key(state, bool_from_json=bool_from_json)
    if not key:
        return ""
    alias = project_alias_for_key(state, key) or "-"
    return f"{alias} ({key})"


def get_manager_project(
    state: Dict[str, Any],
    name: Optional[str],
    *,
    bool_from_json: Callable[[Any, bool], bool],
) -> Tuple[str, Dict[str, Any]]:
    projects = state.get("projects") or {}
    if not isinstance(projects, dict) or not projects:
        raise RuntimeError("no orch projects registered")

    lock_key = get_project_lock_key(state, bool_from_json=bool_from_json)
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


def make_project_args(args: Any, entry: Dict[str, Any], key: str = "") -> argparse.Namespace:
    copied = argparse.Namespace(**vars(args))
    copied._aoe_root_team_dir = str(getattr(args, "_aoe_root_team_dir", args.team_dir))
    copied.project_root = Path(str(entry.get("project_root", args.project_root))).expanduser().resolve()
    copied.team_dir = Path(str(entry.get("team_dir", copied.project_root / ".aoe-team"))).expanduser().resolve()
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
    *,
    now_iso: Callable[[], str],
    trim_project_tasks: Callable[[Dict[str, Any]], Any],
    bool_from_json: Callable[[Any, bool], bool],
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
