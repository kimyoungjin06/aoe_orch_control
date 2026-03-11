#!/usr/bin/env python3
"""Room log retention and autopublish helpers extracted from the gateway monolith."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

ROOM_AUTOPUBLISH_EVENTS = {
    "dispatch_completed",
    "dispatch_failed",
    "exec_critic_retry",
    "exec_critic_blocked",
}


def append_jsonl(
    path: Path,
    row: Dict[str, Any],
    *,
    int_from_env: Callable[..., int],
    default_max_bytes: int,
    default_keep_files: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    max_bytes = int_from_env(
        os.environ.get("AOE_GATEWAY_LOG_MAX_BYTES"),
        default_max_bytes,
        minimum=64 * 1024,
        maximum=256 * 1024 * 1024,
    )
    keep_files = int_from_env(
        os.environ.get("AOE_GATEWAY_LOG_KEEP_FILES"),
        default_keep_files,
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


def room_retention_days(*, int_from_env: Callable[..., int], default_room_retention_days: int) -> int:
    return int_from_env(
        os.environ.get("AOE_ROOM_RETENTION_DAYS"),
        default_room_retention_days,
        minimum=0,
        maximum=3650,
    )


def cleanup_room_logs(
    team_dir: Path,
    *,
    force: bool = False,
    room_retention_days: Callable[[], int],
    today_key_local: Callable[[], str],
) -> int:
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


def room_autopublish_enabled(*, bool_from_env: Callable[..., bool]) -> bool:
    return bool_from_env(os.environ.get("AOE_ROOM_AUTOPUBLISH"), True)


def normalize_room_autopublish_route(raw: Optional[str], *, default_room_autopublish_route: str) -> str:
    token = str(raw or "").strip().lower()
    if token in {"room", "chat", "current"}:
        return "room"
    if token in {"project", "orch", "o"}:
        return "project"
    if token in {"project-tf", "project_tf", "orch-tf", "orch_tf", "tf-project"}:
        return "project-tf"
    if token in {"tf", "taskforce"}:
        return "tf"
    return default_room_autopublish_route


def room_autopublish_route(
    *,
    normalize_room_autopublish_route: Callable[[Optional[str]], str],
) -> str:
    return normalize_room_autopublish_route(os.environ.get("AOE_ROOM_AUTOPUBLISH_ROUTE"))


def room_autopublish_title(event: str) -> str:
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
    room_autopublish_enabled: Callable[[], bool],
    project_alias_for_key: Callable[[Dict[str, Any], str], str],
    get_chat_room: Callable[[Dict[str, Any], str, str], str],
    normalize_room_token: Callable[[str], str],
    room_autopublish_route: Callable[[], str],
    int_from_env: Callable[..., int],
    task_display_label: Callable[[Optional[Dict[str, Any]], str], str],
    append_room_event: Callable[..., Any],
    now_iso: Callable[[], str],
    default_room_name: str,
    default_max_event_chars: int,
    default_max_file_bytes: int,
) -> None:
    if not room_autopublish_enabled():
        return
    if str(event or "").strip() not in ROOM_AUTOPUBLISH_EVENTS:
        return

    project_alias = project_alias_for_key(manager_state, project) or str(project or "").strip() or "-"
    tf_id = ""
    if isinstance(task, dict):
        short_id = str(task.get("short_id", "")).strip().upper()
        if short_id:
            tf_id = re.sub(r"^T-", "TF-", short_id)
            if not tf_id.startswith("TF-"):
                tf_id = "TF-" + re.sub(r"[^A-Z0-9._-]+", "_", short_id).strip("._-")[:24]

    selected_room = get_chat_room(manager_state, chat_id, default_room_name) or default_room_name
    selected_room = normalize_room_token(selected_room)
    if selected_room != default_room_name:
        room = selected_room
    else:
        route = room_autopublish_route()
        if route == "room":
            room = selected_room
        elif route == "tf":
            room = tf_id or project_alias or default_room_name
        elif route == "project-tf":
            room = f"{project_alias}/{tf_id}" if (project_alias and tf_id) else (project_alias or default_room_name)
        else:
            room = project_alias or default_room_name
        room = normalize_room_token(room)

    max_chars = int_from_env(
        os.environ.get("AOE_ROOM_MAX_EVENT_CHARS"),
        default_max_event_chars,
        minimum=200,
        maximum=20000,
    )
    max_file_bytes = int_from_env(
        os.environ.get("AOE_ROOM_MAX_FILE_BYTES"),
        default_max_file_bytes,
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

    title = room_autopublish_title(event)
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
