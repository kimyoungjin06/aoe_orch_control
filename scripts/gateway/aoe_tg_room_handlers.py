#!/usr/bin/env python3
"""Room (chat/board) handlers for Telegram gateway.

Rooms are *ephemeral* conversation logs, designed to feel like Discord channels
without polluting docs/ or todo discovery.

Storage:
- `.aoe-team/logs/rooms/<room>/<YYYY-MM-DD(.N)?>.jsonl`

Policy:
- Default retention is time-based (days) and enforced by gateway housekeeping.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple


DEFAULT_ROOM_NAME = "global"
DEFAULT_TAIL_LINES = 20
DEFAULT_MAX_EVENT_CHARS = 3500
DEFAULT_MAX_FILE_BYTES = 1 * 1024 * 1024  # 1MB per daily shard


_ROOM_SEG_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _now_iso_local() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S%z")


def _today_key_local() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _sanitize_seg(raw: str, fallback: str) -> str:
    token = _ROOM_SEG_RE.sub("_", str(raw or "").strip()).strip("._-")
    return token or fallback


def normalize_room_token(raw: str) -> str:
    token = str(raw or "").strip()
    if not token:
        return DEFAULT_ROOM_NAME
    low = token.lower()
    if low in {"g", "global", "main", "lobby"}:
        return DEFAULT_ROOM_NAME
    # Collapse repeated slashes and strip unsafe parts.
    parts: List[str] = []
    for seg in token.split("/"):
        seg = seg.strip()
        if not seg or seg in {".", ".."}:
            continue
        parts.append(_sanitize_seg(seg, "room"))
        if len(parts) >= 4:
            break
    return "/".join(parts) if parts else DEFAULT_ROOM_NAME


def _rooms_root(team_dir: Path) -> Path:
    return (team_dir / "logs" / "rooms").resolve()


def _room_dir(team_dir: Path, room: str) -> Path:
    token = normalize_room_token(room)
    root = _rooms_root(team_dir)
    cur = root
    for seg in token.split("/"):
        cur = cur / _sanitize_seg(seg, "room")
    return cur


def _iter_room_files(room_dir: Path) -> List[Path]:
    if not room_dir.exists() or not room_dir.is_dir():
        return []
    # YYYY-MM-DD.jsonl or YYYY-MM-DD.N.jsonl
    files = [p for p in room_dir.iterdir() if p.is_file() and re.fullmatch(r"\d{4}-\d{2}-\d{2}(?:\.\d+)?\.jsonl", p.name)]
    files.sort(key=lambda p: p.name)
    return files


def _pick_append_file(room_dir: Path, date_key: str, max_bytes: int) -> Path:
    room_dir.mkdir(parents=True, exist_ok=True)
    base = room_dir / f"{date_key}.jsonl"
    if not base.exists():
        return base
    try:
        if base.stat().st_size < int(max_bytes):
            return base
    except Exception:
        return base

    # Roll to YYYY-MM-DD.1.jsonl, YYYY-MM-DD.2.jsonl, ...
    idx = 1
    while idx <= 99:
        cand = room_dir / f"{date_key}.{idx}.jsonl"
        if not cand.exists():
            return cand
        try:
            if cand.stat().st_size < int(max_bytes):
                return cand
        except Exception:
            return cand
        idx += 1
    return base


def append_room_event(
    *,
    team_dir: Path,
    room: str,
    event: Dict[str, Any],
    max_file_bytes: int,
) -> Path:
    token = normalize_room_token(room)
    room_dir = _room_dir(team_dir, token)
    path = _pick_append_file(room_dir, _today_key_local(), max_file_bytes)
    payload = dict(event)
    payload["room"] = token
    payload["ts"] = str(payload.get("ts") or _now_iso_local())
    line = json.dumps(payload, ensure_ascii=False)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    return path


def tail_room_events(*, team_dir: Path, room: str, limit: int) -> List[Dict[str, Any]]:
    token = normalize_room_token(room)
    room_dir = _room_dir(team_dir, token)
    files = _iter_room_files(room_dir)
    if not files:
        return []

    want = max(1, min(100, int(limit or DEFAULT_TAIL_LINES)))
    out: List[Dict[str, Any]] = []

    # Read from newest files backward.
    for path in reversed(files[-10:]):
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            continue
        for raw in reversed(lines):
            if not raw.strip():
                continue
            try:
                row = json.loads(raw)
            except Exception:
                row = {"ts": "-", "text": raw.strip()}
            if isinstance(row, dict):
                out.append(row)
            if len(out) >= want:
                return list(reversed(out))
    return list(reversed(out))


def list_rooms(*, team_dir: Path, limit: int = 50) -> List[Tuple[str, float]]:
    root = _rooms_root(team_dir)
    if not root.exists() or not root.is_dir():
        return []

    rows: List[Tuple[str, float]] = []
    # Find any directory that contains at least one jsonl shard.
    for p in sorted(root.rglob("*.jsonl")):
        try:
            rel = p.parent.relative_to(root)
        except Exception:
            continue
        room = "/".join(rel.parts)
        if not room:
            continue
        try:
            mt = float(p.stat().st_mtime)
        except Exception:
            mt = 0.0
        rows.append((room, mt))

    if not rows:
        return []

    # Keep latest mtime per room.
    latest: Dict[str, float] = {}
    for room, mt in rows:
        latest[room] = max(latest.get(room, 0.0), mt)

    out = [(room, mt) for room, mt in latest.items()]
    out.sort(key=lambda r: r[1], reverse=True)
    return out[: max(1, min(200, int(limit)))]


def _room_usage() -> str:
    return (
        "room (ephemeral board)\n"
        "- /room              # status\n"
        "- /room list\n"
        "- /room use <name>   # ex) global | O1 | O1/TF-ALPHA\n"
        "- /room post <text>\n"
        "- /room tail [N]\n"
    )


@dataclass
class RoomDeps:
    send: Callable[..., bool]
    now_iso: Callable[[], str]
    get_chat_room: Callable[[Dict[str, Any], str, str], str]
    set_chat_room: Callable[[Dict[str, Any], str, str], None]
    save_manager_state: Callable[..., None]


def handle_room_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    rest: str,
    deps: RoomDeps,
) -> bool:
    if cmd != "room":
        return False

    team_dir: Path = getattr(args, "team_dir", None)
    team_dir = Path(str(team_dir)).expanduser().resolve()

    tokens = [t for t in str(rest or "").split() if t.strip()]
    sub = (tokens[0].lower() if tokens else "status").strip()
    sub_args = tokens[1:]

    if sub in {"help", "h", "?"}:
        deps.send(_room_usage(), context="room-help", with_menu=True)
        return True

    current = deps.get_chat_room(manager_state, chat_id, DEFAULT_ROOM_NAME)

    if sub in {"", "status", "show"}:
        deps.send(
            "room\n"
            f"- current: {current}\n"
            "set:\n"
            "- /room use <name>\n"
            "- /room post <text>\n"
            "- /room tail [N]\n"
            "- /room list",
            context="room-status",
            with_menu=True,
        )
        return True

    if sub == "list":
        rooms = list_rooms(team_dir=team_dir, limit=60)
        if not rooms:
            deps.send(
                "room list\n"
                "- (empty)\n"
                "next:\n"
                "- /room post <text>",
                context="room-list-empty",
                with_menu=True,
            )
            return True
        lines = ["room list", f"- current: {current}", ""]
        for idx, (name, _mt) in enumerate(rooms[:50], start=1):
            lines.append(f"{idx}. {name}")
        lines.extend(["", "next:", "- /room use <name>", "- /room tail 20"])
        deps.send("\n".join(lines).strip(), context="room-list", with_menu=True)
        return True

    if sub == "use":
        if chat_role == "readonly":
            deps.send("permission denied: readonly chat cannot change room.", context="room-deny", with_menu=True)
            return True
        if not sub_args:
            raise RuntimeError("usage: /room use <name>")
        name = normalize_room_token(" ".join(sub_args).strip())
        deps.set_chat_room(manager_state, chat_id, name)
        if not bool(getattr(args, "dry_run", False)):
            deps.save_manager_state(args.manager_state_file, manager_state)
        deps.send(f"room updated\n- current: {name}", context="room-use", with_menu=True)
        return True

    if sub == "post":
        if chat_role == "readonly":
            deps.send("permission denied: readonly chat cannot post.", context="room-deny", with_menu=True)
            return True
        text = " ".join(sub_args).strip()
        if not text:
            raise RuntimeError("usage: /room post <text>")
        try:
            import os

            max_chars = max(200, min(20000, int(os.environ.get("AOE_ROOM_MAX_EVENT_CHARS", str(DEFAULT_MAX_EVENT_CHARS)))))
            max_file_bytes = max(
                64 * 1024,
                min(50 * 1024 * 1024, int(os.environ.get("AOE_ROOM_MAX_FILE_BYTES", str(DEFAULT_MAX_FILE_BYTES)))),
            )
        except Exception:
            max_chars = DEFAULT_MAX_EVENT_CHARS
            max_file_bytes = DEFAULT_MAX_FILE_BYTES

        clipped = text if len(text) <= max_chars else (text[:max_chars] + " ...(truncated)")
        actor = str(chat_id)
        path = append_room_event(
            team_dir=team_dir,
            room=current,
            event={
                "ts": deps.now_iso(),
                "actor": actor,
                "kind": "post",
                "text": clipped,
            },
            max_file_bytes=max_file_bytes,
        )
        deps.send(
            "room posted\n"
            f"- room: {current}\n"
            f"- shard: {path.relative_to(team_dir).as_posix()}",
            context="room-post",
            with_menu=True,
        )
        return True

    if sub == "tail":
        n = DEFAULT_TAIL_LINES
        if sub_args and sub_args[0].isdigit():
            n = max(1, min(100, int(sub_args[0])))
        rows = tail_room_events(team_dir=team_dir, room=current, limit=n)
        if not rows:
            deps.send(
                "room tail\n"
                f"- room: {current}\n"
                "- (empty)\n"
                "next:\n"
                "- /room post <text>",
                context="room-tail-empty",
                with_menu=True,
            )
            return True
        lines = ["room tail", f"- room: {current}", ""]
        for row in rows:
            if not isinstance(row, dict):
                continue
            ts = str(row.get("ts", "")).strip() or "-"
            actor = str(row.get("actor", "")).strip() or "-"
            text = str(row.get("text", "")).strip()
            text = re.sub(r"\s+", " ", text)
            if len(text) > 220:
                text = text[:220] + "..."
            lines.append(f"- {ts} {actor}: {text}")
        deps.send("\n".join(lines).strip(), context="room-tail", with_menu=True)
        return True

    raise RuntimeError("usage: /room [list|use|post|tail]")
