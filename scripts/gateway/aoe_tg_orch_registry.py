#!/usr/bin/env python3
"""Orchestrator registry/status helpers extracted from the gateway monolith."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple

from aoe_tg_ops_policy import visible_ops_project_keys
from aoe_tg_ops_view import compact_age_label
from aoe_tg_project_runtime import project_runtime_issue, project_runtime_label
from aoe_tg_task_view import normalize_project_alias, normalize_project_name


def registry_todo_counts(entry: Dict[str, Any]) -> Dict[str, int]:
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


def registry_latest_task(
    entry: Dict[str, Any],
    *,
    task_display_label: Callable[..., str],
    normalize_task_status: Callable[[Any], str],
) -> Tuple[str, str]:
    tasks = entry.get("tasks")
    if not isinstance(tasks, dict) or not tasks:
        return "-", "-"
    best_req = ""
    best_task: Dict[str, Any] | None = None
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


def short_root_label(raw: str) -> str:
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


def summarize_orch_registry(
    state: Dict[str, Any],
    *,
    ensure_project_aliases: Callable[[Dict[str, Any]], Dict[str, str]],
    project_alias_for_key: Callable[[Dict[str, Any], str], str],
    project_lock_label: Callable[[Dict[str, Any]], str],
    extract_project_alias_index: Callable[[str], int],
    bool_from_json: Callable[[Any, bool], bool],
    task_display_label: Callable[..., str],
    normalize_task_status: Callable[[Any], str],
) -> str:
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
        counts = registry_todo_counts(entry)
        pending = entry.get("pending_todo")
        pending_tag = " [PENDING]" if isinstance(pending, dict) and str(pending.get("todo_id", "")).strip() else ""
        last_task_label, last_task_status = registry_latest_task(
            entry,
            task_display_label=task_display_label,
            normalize_task_status=normalize_task_status,
        )
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
        lines.append(f"  key={key} | root={short_root_label(root)}")
        if unready_issue:
            lines.append(f"  runtime={project_runtime_label(entry)}")

    if alias_map:
        rows = [
            f"{alias}:{key}"
            for alias, key in sorted(alias_map.items(), key=lambda kv: extract_project_alias_index(kv[0]))
            if str(key) in visible_keys
        ]
        if rows:
            lines.append("aliases: " + ", ".join(rows))
    return "\n".join(lines)


def run_aoe_status(
    args: Any,
    *,
    run_command: Callable[..., Any],
    summarize_gateway_poll_state: Callable[[Any], str],
) -> str:
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
