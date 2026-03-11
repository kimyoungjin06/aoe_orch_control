#!/usr/bin/env python3
"""Gateway event logging helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple


def task_identifiers(task: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    if not isinstance(task, dict):
        return "", ""
    short_id = str(task.get("short_id", "")).strip()
    alias = str(task.get("alias", "")).strip()
    return short_id, alias


def append_gateway_event_targets(
    *,
    team_dir: Path,
    row: Dict[str, Any],
    append_jsonl: Callable[[Path, Dict[str, Any]], None],
    mirror_team_dir: Optional[Path] = None,
) -> None:
    primary = Path(team_dir).expanduser().resolve()
    payload = dict(row)
    payload["log_scope"] = "project"
    append_jsonl(primary / "logs" / "gateway_events.jsonl", payload)

    if mirror_team_dir is None:
        return
    mirror = Path(mirror_team_dir).expanduser().resolve()
    if mirror == primary:
        return

    mirror_row = dict(row)
    mirror_row["log_scope"] = "mother"
    mirror_row["project_team_dir"] = str(primary)
    append_jsonl(mirror / "logs" / "gateway_events.jsonl", mirror_row)


def log_gateway_event(
    *,
    team_dir: Path,
    event: str,
    now_iso: Callable[[], str],
    mask_sensitive_text: Callable[[str], str],
    append_gateway_event_targets: Callable[..., None],
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
        append_gateway_event_targets(team_dir=team_dir, row=row, mirror_team_dir=mirror_team_dir)
    except Exception:
        return
