#!/usr/bin/env python3
"""Ops-oriented rendering helpers for Telegram gateway status views."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

from aoe_tg_blocked_state import blocked_bucket_count, blocked_bucket_label, blocked_head_summary, blocked_reason_preview
from aoe_tg_ops_policy import list_ops_projects, project_queue_snapshot


def compact_age_label(raw_ts: str) -> str:
    text = str(raw_ts or "").strip()
    if not text:
        return "-"
    normalized = text
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    if re.search(r"[+-]\d{4}$", normalized):
        normalized = normalized[:-2] + ":" + normalized[-2:]
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception:
        return text
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(dt.tzinfo)
    delta = int((now - dt).total_seconds())
    direction = "ago"
    if delta < 0:
        delta = abs(delta)
        direction = "from now"
    if delta < 60:
        return f"{delta}s {direction}"
    if delta < 3600:
        return f"{delta // 60}m {direction}"
    if delta < 86400:
        return f"{delta // 3600}h {direction}"
    return f"{delta // 86400}d {direction}"

def render_project_snapshot_lines(*, key: str, entry: Dict[str, Any], locked: bool = False) -> List[str]:
    if not key or not isinstance(entry, dict):
        return []
    alias = str(entry.get("project_alias", "")).strip().upper() or key
    display = str(entry.get("display_name", key)).strip() or key
    todos = entry.get("todos") if isinstance(entry.get("todos"), list) else []
    snap = project_queue_snapshot(entry)
    pending_flag = "yes" if snap["has_pending"] else "no"
    manual_followup_count = blocked_bucket_count(todos, "manual_followup")

    sync_mode = str(entry.get("last_sync_mode", "")).strip()
    sync_age = compact_age_label(str(entry.get("last_sync_at", "")).strip())
    if sync_mode and sync_age != "-":
        sync_disp = f"{sync_mode} {sync_age}"
    else:
        sync_disp = sync_mode or sync_age
    if not sync_disp or sync_disp == "-":
        sync_disp = "never"

    latest_task = "-"
    latest_key = ""
    tasks = entry.get("tasks")
    if isinstance(tasks, dict):
        for req_id, task in tasks.items():
            if not isinstance(task, dict):
                continue
            updated = str(task.get("updated_at", "")).strip() or str(task.get("created_at", "")).strip()
            if updated >= latest_key:
                latest_key = updated
                short_id = str(task.get("short_id", "")).strip() or str(req_id or "").strip() or "-"
                prompt = " ".join(str(task.get("prompt", "")).strip().split())
                if len(prompt) > 42:
                    prompt = prompt[:39].rstrip() + "..."
                status = str(task.get("status", "")).strip().lower() or "-"
                latest_task = short_id
                if prompt:
                    latest_task += f" {prompt}"
                if status != "-":
                    latest_task += f" [{status}]"

    marker = " [locked]" if locked else ""
    lines = [
        "project snapshot",
        f"- project: {alias} {display}{marker}",
        (
            f"- todo: open={snap['open_count']} running={snap['running_count']} "
            f"blocked={snap['blocked_count']} followup={manual_followup_count} pending={pending_flag}"
        ),
        f"- last_sync: {sync_disp}",
        f"- last_task: {latest_task}",
    ]
    blocked_head = blocked_head_summary(todos)
    if blocked_head:
        blocked_line = f"- blocked_head: {blocked_head.get('id', '-')} x{blocked_head.get('count', 1)}"
        bucket = str(blocked_head.get("bucket", "")).strip()
        if bucket:
            blocked_line += f" [{bucket}]"
        reason = str(blocked_head.get("reason", "")).strip()
        if reason:
            blocked_line += f" | {reason}"
        lines.append(blocked_line)
    return lines


def render_ops_scope_compact_lines(
    projects: Any,
    *,
    limit: int = 4,
    detail_level: str = "short",
) -> List[str]:
    if not isinstance(projects, dict):
        return []
    detail = "long" if str(detail_level or "").strip().lower() == "long" else "short"
    rows: List[Tuple[int, str, List[str]]] = []
    for key, entry in list_ops_projects(projects):
        alias = str(entry.get("project_alias", "")).strip().upper() or str(key)
        display = str(entry.get("display_name", key)).strip() or str(key)
        token = alias[1:] if alias.startswith("O") else alias
        alias_idx = int(token) if token.isdigit() else 10**9
        todos = entry.get("todos") if isinstance(entry.get("todos"), list) else []
        snap = project_queue_snapshot(entry)
        best_open = snap["best_open"] if isinstance(snap["best_open"], dict) else {}
        next_id = str(best_open.get("id", "")).strip() or "-"
        next_pr = str(best_open.get("priority", "P2")).strip().upper() or "P2"
        next_summary = " ".join(str(best_open.get("summary", "")).strip().split())
        if len(next_summary) > 60:
            next_summary = next_summary[:57] + "..."
        flags: List[str] = []
        if bool(entry.get("paused", False)):
            flags.append("paused")
        if snap["has_pending"]:
            flags.append("pending")
        followup = blocked_bucket_count(todos, "manual_followup")
        pending_flag = "yes" if snap["has_pending"] else "no"
        sync_mode = str(entry.get("last_sync_mode", "")).strip()
        sync_age = compact_age_label(str(entry.get("last_sync_at", "")).strip())
        sync_disp = f"{sync_mode} {sync_age}".strip() if sync_mode else sync_age
        line = [f"- {alias} {display}: open={snap['open_count']} running={snap['running_count']} blocked={snap['blocked_count']}"]
        if followup > 0:
            line[0] += f" followup={followup}"
        if flags:
            line[0] += f" ({','.join(flags)})"
        line.append(f"  next: {next_pr} {next_id} | {next_summary or '-'}")
        if detail == "long":
            line.append(f"  last_sync: {sync_disp or 'never'} | pending={pending_flag}")
            blocked_head = blocked_head_summary(todos)
            if blocked_head:
                blocked_line = f"  blocked_head: {blocked_head.get('id', '-')} x{blocked_head.get('count', 1)}"
                bucket = str(blocked_head.get('bucket', '')).strip()
                if bucket:
                    blocked_line += f" [{bucket}]"
                reason = str(blocked_head.get("reason", "")).strip()
                if reason:
                    blocked_line += f" | {reason}"
                line.append(blocked_line)
        rows.append((alias_idx, alias, line))
    rows.sort(key=lambda row: (row[0], row[1]))
    out: List[str] = []
    for _idx, _alias, pair in rows[: max(1, limit)]:
        out.extend(pair)
    return out
