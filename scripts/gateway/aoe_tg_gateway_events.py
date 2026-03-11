#!/usr/bin/env python3
"""Gateway event logging helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


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


def mirror_backend_runtime_events(
    *,
    team_dir: Path,
    backend: str,
    runtime_events: Iterable[Dict[str, Any]],
    now_iso: Callable[[], str],
    mask_sensitive_text: Callable[[str], str],
    append_gateway_event_targets: Callable[..., None],
    trace_id: str = "",
    project: str = "",
    request_id: str = "",
    task: Optional[Dict[str, Any]] = None,
    mirror_team_dir: Optional[Path] = None,
) -> int:
    short_id, alias = task_identifiers(task)
    rows: List[Dict[str, Any]] = []
    backend_name = str(backend or "").strip() or "tf_backend"

    for raw in runtime_events or []:
        if not isinstance(raw, dict):
            continue
        payload = raw.get("payload")
        if not isinstance(payload, dict):
            payload = {}
        stage = str(raw.get("stage", "") or "").strip()
        source = str(raw.get("source", "") or "").strip() or backend_name
        kind = str(raw.get("kind", "") or "").strip()
        status = str(raw.get("status", "") or "").strip()
        summary = str(raw.get("summary", "") or "").strip() or stage or "backend runtime event"
        seq = max(1, int(raw.get("seq", 0) or 0))
        event_ts = str(raw.get("ts", "") or "").strip() or now_iso()

        payload_text = ""
        if payload:
            try:
                payload_text = json.dumps(payload, ensure_ascii=False, sort_keys=True)
            except Exception:
                payload_text = str(payload)
        detail = summary
        if payload_text:
            detail = f"{detail} | payload={payload_text}"

        row: Dict[str, Any] = {
            "timestamp": event_ts,
            "event": "tf_backend_runtime_event",
            "trace_id": str(trace_id or "").strip(),
            "project": str(project or "").strip(),
            "request_id": str(request_id or "").strip(),
            "task_short_id": short_id,
            "task_alias": alias,
            "stage": stage,
            "actor": f"{backend_name}:{source}"[:96],
            "status": status,
            "error_code": "E_TF_BACKEND_RUNTIME" if status == "error" else "",
            "latency_ms": 0,
            "detail": mask_sensitive_text(detail)[:800],
            "backend": backend_name,
            "backend_seq": seq,
            "backend_source": source,
            "backend_kind": kind,
            "backend_summary": summary[:240],
            "backend_payload": payload,
        }
        rows.append(row)

    for row in rows:
        try:
            append_gateway_event_targets(team_dir=team_dir, row=row, mirror_team_dir=mirror_team_dir)
        except Exception:
            continue
    return len(rows)
