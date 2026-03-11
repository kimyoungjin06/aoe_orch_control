#!/usr/bin/env python3
"""Gateway poll/replay state helpers extracted from the gateway monolith."""

from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def dedup_keep_limit(*, int_from_env, default_keep: int) -> int:
    return int_from_env(
        os.environ.get("AOE_GATEWAY_DEDUP_KEEP"),
        default=default_keep,
        minimum=100,
        maximum=20000,
    )


def failed_queue_keep_limit(*, int_from_env, default_keep: int) -> int:
    return int_from_env(
        os.environ.get("AOE_GATEWAY_FAILED_KEEP"),
        default=default_keep,
        minimum=10,
        maximum=5000,
    )


def failed_queue_ttl_hours(*, int_from_env, default_ttl_hours: int) -> int:
    return int_from_env(
        os.environ.get("AOE_GATEWAY_FAILED_TTL_HOURS"),
        default=default_ttl_hours,
        minimum=0,
        maximum=8760,
    )


def normalize_recent_tokens(raw: Any, keep: int) -> List[str]:
    out: List[str] = []
    seen = set()
    if isinstance(raw, list):
        for item in raw:
            token = str(item or "").strip()
            if not token or token in seen:
                continue
            seen.add(token)
            out.append(token)
    if len(out) > keep:
        out = out[-keep:]
    return out


def append_recent_token(tokens: List[str], token: str, keep: int) -> None:
    value = str(token or "").strip()
    if not value:
        return
    try:
        tokens.remove(value)
    except ValueError:
        pass
    tokens.append(value)
    if len(tokens) > keep:
        del tokens[:-keep]


def message_dedup_key(msg: Dict[str, Any]) -> str:
    if not isinstance(msg, dict):
        return ""
    chat = msg.get("chat")
    if not isinstance(chat, dict):
        return ""
    chat_id = str(chat.get("id", "")).strip()
    if not chat_id:
        return ""
    message_id_raw = msg.get("message_id")
    message_id = str(message_id_raw if message_id_raw is not None else "").strip()
    if not message_id:
        return ""
    return f"{chat_id}:{message_id}"


def normalize_failed_queue(
    raw: Any,
    keep: int,
    *,
    failed_queue_ttl_hours,
    now_iso,
    parse_iso_ts,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    ttl_hours = failed_queue_ttl_hours()
    cutoff_utc: Optional[datetime] = None
    if ttl_hours > 0:
        cutoff_utc = datetime.now(timezone.utc) - timedelta(hours=ttl_hours)
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                continue
            fid = str(item.get("id", "")).strip()
            chat_id = str(item.get("chat_id", "")).strip()
            text = str(item.get("text", "")).strip()
            if not fid or not chat_id or not text:
                continue
            at_value = str(item.get("at", "")).strip() or now_iso()
            parsed_at = parse_iso_ts(at_value)
            if parsed_at is None:
                at_value = now_iso()
                parsed_at = parse_iso_ts(at_value)
            if cutoff_utc is not None and parsed_at is not None:
                try:
                    if parsed_at.astimezone(timezone.utc) < cutoff_utc:
                        continue
                except Exception:
                    pass
            row: Dict[str, Any] = {
                "id": fid[:64],
                "at": at_value,
                "chat_id": chat_id[:64],
                "text": text[:4000],
                "trace_id": str(item.get("trace_id", "")).strip()[:120],
                "error_code": str(item.get("error_code", "")).strip()[:32],
                "error": str(item.get("error", "")).strip()[:400],
                "cmd": str(item.get("cmd", "")).strip()[:40],
            }
            if row["id"] and row["chat_id"] and row["text"]:
                out.append(row)
    if len(out) > keep:
        out = out[-keep:]
    return out


def enqueue_failed_message(
    state: Dict[str, Any],
    *,
    chat_id: str,
    text: str,
    trace_id: str,
    error_code: str,
    error_detail: str,
    cmd: str = "",
    failed_queue_keep_limit,
    normalize_failed_queue,
    failed_queue_key: str,
    now_iso,
) -> Dict[str, Any]:
    keep = failed_queue_keep_limit()
    queue = normalize_failed_queue(state.get(failed_queue_key), keep)
    item = {
        "id": uuid.uuid4().hex[:16],
        "at": now_iso(),
        "chat_id": str(chat_id or "").strip(),
        "text": str(text or "").strip()[:4000],
        "trace_id": str(trace_id or "").strip()[:120],
        "error_code": str(error_code or "").strip()[:32],
        "error": str(error_detail or "").strip()[:400],
        "cmd": str(cmd or "").strip()[:40],
    }
    queue.append(item)
    if len(queue) > keep:
        queue = queue[-keep:]
    state[failed_queue_key] = queue
    return item


def failed_queue_for_chat(
    state: Dict[str, Any],
    chat_id: str,
    *,
    failed_queue_keep_limit,
    normalize_failed_queue,
    failed_queue_key: str,
) -> List[Dict[str, Any]]:
    keep = failed_queue_keep_limit()
    queue = normalize_failed_queue(state.get(failed_queue_key), keep)
    token = str(chat_id or "").strip()
    if not token:
        return []
    return [row for row in queue if str(row.get("chat_id", "")).strip() == token]


def remove_failed_queue_item(
    state: Dict[str, Any],
    item_id: str,
    *,
    failed_queue_keep_limit,
    normalize_failed_queue,
    failed_queue_key: str,
) -> Optional[Dict[str, Any]]:
    keep = failed_queue_keep_limit()
    queue = normalize_failed_queue(state.get(failed_queue_key), keep)
    target = str(item_id or "").strip()
    if not target:
        state[failed_queue_key] = queue
        return None
    picked: Optional[Dict[str, Any]] = None
    kept: List[Dict[str, Any]] = []
    for row in queue:
        if picked is None and str(row.get("id", "")).strip() == target:
            picked = row
            continue
        kept.append(row)
    state[failed_queue_key] = kept
    return picked


def purge_failed_queue_for_chat(
    state: Dict[str, Any],
    chat_id: str,
    *,
    failed_queue_keep_limit,
    normalize_failed_queue,
    failed_queue_key: str,
) -> int:
    keep = failed_queue_keep_limit()
    queue = normalize_failed_queue(state.get(failed_queue_key), keep)
    token = str(chat_id or "").strip()
    if not token:
        state[failed_queue_key] = queue
        return 0
    removed = 0
    kept: List[Dict[str, Any]] = []
    for row in queue:
        if str(row.get("chat_id", "")).strip() == token:
            removed += 1
        else:
            kept.append(row)
    state[failed_queue_key] = kept
    return removed


def format_failed_queue_item_detail(row: Dict[str, Any], *, replay_usage: str) -> str:
    text = str(row.get("text", "")).strip()
    if len(text) > 1200:
        text = text[:1197] + "..."
    text = text.replace("\n", "\\n")
    error_detail = str(row.get("error", "")).strip()
    if len(error_detail) > 400:
        error_detail = error_detail[:397] + "..."
    error_detail = error_detail.replace("\n", "\\n")
    rid = str(row.get("id", "")).strip() or "-"
    return (
        "replay item\n"
        f"- id: {rid}\n"
        f"- at: {row.get('at') or '-'}\n"
        f"- chat: {row.get('chat_id') or '-'}\n"
        f"- cmd: {row.get('cmd') or '-'}\n"
        f"- error_code: {row.get('error_code') or '-'}\n"
        f"- trace_id: {row.get('trace_id') or '-'}\n"
        f"- text: {text or '-'}\n"
        f"- error: {error_detail or '-'}\n"
        f"- run: /replay {rid}"
    )


def summarize_failed_queue(state: Dict[str, Any], chat_id: str, *, limit: int = 8, failed_queue_for_chat, replay_usage: str) -> str:
    rows = failed_queue_for_chat(state, chat_id)
    if not rows:
        return f"replay queue: empty\n{replay_usage}"
    view = list(reversed(rows))
    cap = max(1, min(30, int(limit)))
    lines = [f"replay queue: {len(rows)} pending (chat={chat_id})", replay_usage]
    for i, row in enumerate(view[:cap], start=1):
        body = str(row.get("text", "")).replace("\n", " ").strip()
        if len(body) > 70:
            body = body[:67] + "..."
        lines.append(
            f"{i}. id={row.get('id')} cmd={row.get('cmd') or '-'} code={row.get('error_code') or '-'} "
            f"at={row.get('at')} text={body}"
        )
    return "\n".join(lines)


def resolve_failed_queue_item(state: Dict[str, Any], chat_id: str, target: str, *, failed_queue_for_chat) -> Tuple[Optional[Dict[str, Any]], str]:
    rows = failed_queue_for_chat(state, chat_id)
    if not rows:
        return None, "replay queue: empty"
    view = list(reversed(rows))
    token = str(target or "").strip().lower()
    if token in {"", "latest", "last", "new"}:
        return view[0], ""
    if token.isdigit():
        idx = int(token)
        if idx < 1 or idx > len(view):
            return None, f"replay index out of range: {idx} (1..{len(view)})"
        return view[idx - 1], ""
    for row in view:
        if str(row.get("id", "")).strip().lower() == token:
            return row, ""
    return None, f"replay target not found: {target}"


def load_state(
    path: Path,
    *,
    acked_updates_key: str,
    handled_messages_key: str,
    duplicate_skipped_key: str,
    empty_skipped_key: str,
    unauthorized_skipped_key: str,
    handler_errors_key: str,
    failed_queue_key: str,
    seen_update_ids_key: str,
    seen_message_keys_key: str,
    dedup_keep_limit,
    failed_queue_keep_limit,
    normalize_recent_tokens,
    normalize_failed_queue,
) -> Dict[str, Any]:
    base = {
        "offset": 0,
        "updated_at": "",
        "processed": 0,
        acked_updates_key: 0,
        handled_messages_key: 0,
        duplicate_skipped_key: 0,
        empty_skipped_key: 0,
        unauthorized_skipped_key: 0,
        handler_errors_key: 0,
        failed_queue_key: [],
        seen_update_ids_key: [],
        seen_message_keys_key: [],
    }
    if not path.exists():
        return base
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return base
    if not isinstance(data, dict):
        return base

    out = dict(base)
    try:
        out["offset"] = max(0, int(data.get("offset", 0) or 0))
    except Exception:
        out["offset"] = 0
    try:
        out["processed"] = max(0, int(data.get("processed", 0) or 0))
    except Exception:
        out["processed"] = 0
    processed_legacy = int(out["processed"])
    for key, fallback in (
        (acked_updates_key, processed_legacy),
        (handled_messages_key, processed_legacy),
        (duplicate_skipped_key, 0),
        (empty_skipped_key, 0),
        (unauthorized_skipped_key, 0),
        (handler_errors_key, 0),
    ):
        try:
            out[key] = max(0, int(data.get(key, fallback) or 0))
        except Exception:
            out[key] = int(fallback)
    out["processed"] = int(out.get(handled_messages_key, processed_legacy))
    out["updated_at"] = str(data.get("updated_at", "")).strip()

    keep = dedup_keep_limit()
    out[seen_update_ids_key] = normalize_recent_tokens(data.get(seen_update_ids_key), keep)
    out[seen_message_keys_key] = normalize_recent_tokens(data.get(seen_message_keys_key), keep)
    out[failed_queue_key] = normalize_failed_queue(data.get(failed_queue_key), failed_queue_keep_limit())
    return out


def save_state(
    path: Path,
    state: Dict[str, Any],
    *,
    acked_updates_key: str,
    handled_messages_key: str,
    duplicate_skipped_key: str,
    empty_skipped_key: str,
    unauthorized_skipped_key: str,
    handler_errors_key: str,
    failed_queue_key: str,
    seen_update_ids_key: str,
    seen_message_keys_key: str,
    dedup_keep_limit,
    failed_queue_keep_limit,
    normalize_recent_tokens,
    normalize_failed_queue,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    keep = dedup_keep_limit()
    try:
        offset = max(0, int(state.get("offset", 0) or 0))
    except Exception:
        offset = 0
    try:
        handled_messages = max(0, int(state.get(handled_messages_key, state.get("processed", 0)) or 0))
    except Exception:
        handled_messages = 0
    try:
        acked_updates = max(0, int(state.get(acked_updates_key, handled_messages) or 0))
    except Exception:
        acked_updates = handled_messages
    try:
        duplicate_skipped = max(0, int(state.get(duplicate_skipped_key, 0) or 0))
    except Exception:
        duplicate_skipped = 0
    try:
        empty_skipped = max(0, int(state.get(empty_skipped_key, 0) or 0))
    except Exception:
        empty_skipped = 0
    try:
        unauthorized_skipped = max(0, int(state.get(unauthorized_skipped_key, 0) or 0))
    except Exception:
        unauthorized_skipped = 0
    try:
        handler_errors = max(0, int(state.get(handler_errors_key, 0) or 0))
    except Exception:
        handler_errors = 0

    payload = {
        "offset": offset,
        "processed": handled_messages,
        acked_updates_key: acked_updates,
        handled_messages_key: handled_messages,
        duplicate_skipped_key: duplicate_skipped,
        empty_skipped_key: empty_skipped,
        unauthorized_skipped_key: unauthorized_skipped,
        handler_errors_key: handler_errors,
        failed_queue_key: normalize_failed_queue(state.get(failed_queue_key), failed_queue_keep_limit()),
        seen_update_ids_key: normalize_recent_tokens(state.get(seen_update_ids_key), keep),
        seen_message_keys_key: normalize_recent_tokens(state.get(seen_message_keys_key), keep),
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def summarize_gateway_poll_state(
    state_file: Optional[Any],
    *,
    project_name: str = "",
    load_state,
    acked_updates_key: str,
    handled_messages_key: str,
    duplicate_skipped_key: str,
    empty_skipped_key: str,
    unauthorized_skipped_key: str,
    handler_errors_key: str,
    failed_queue_key: str,
    seen_update_ids_key: str,
    seen_message_keys_key: str,
    normalize_recent_tokens,
    dedup_keep_limit,
    parse_iso_ts,
) -> str:
    path_raw = str(state_file or "").strip()
    if path_raw:
        path = Path(path_raw).expanduser().resolve()
    else:
        path = Path(".")
    if not path_raw:
        return f"poll_state: unavailable (orch={project_name or '-'}, state_file=unset)"
    if not path.exists():
        return f"poll_state: unavailable (orch={project_name or '-'}, state_file_missing={path})"

    state = load_state(path)
    offset = max(0, int(state.get("offset", 0) or 0))
    acked = max(0, int(state.get(acked_updates_key, state.get("processed", 0)) or 0))
    handled = max(0, int(state.get(handled_messages_key, state.get("processed", 0)) or 0))
    duplicates = max(0, int(state.get(duplicate_skipped_key, 0) or 0))
    unauthorized = max(0, int(state.get(unauthorized_skipped_key, 0) or 0))
    empty = max(0, int(state.get(empty_skipped_key, 0) or 0))
    handler_errors = max(0, int(state.get(handler_errors_key, 0) or 0))
    poll_updated_at = str(state.get("updated_at", "")).strip() or "-"
    seen_updates = len(normalize_recent_tokens(state.get(seen_update_ids_key), dedup_keep_limit()))
    seen_messages = len(normalize_recent_tokens(state.get(seen_message_keys_key), dedup_keep_limit()))

    failed_queue_total = 0
    last_failed_at = "-"
    fq = state.get(failed_queue_key)
    if isinstance(fq, list) and fq:
        failed_queue_total = len(fq)
        best_ts: Optional[datetime] = None
        best_raw = ""
        for row in fq:
            if not isinstance(row, dict):
                continue
            at_raw = str(row.get("at", "")).strip()
            ts = parse_iso_ts(at_raw) if at_raw else None
            if ts is not None and (best_ts is None or ts > best_ts):
                best_ts = ts
                best_raw = at_raw
            elif not best_raw and at_raw:
                best_raw = at_raw
        last_failed_at = best_raw or last_failed_at
    return (
        f"poll_state: acked={acked} handled={handled} duplicates={duplicates} "
        f"unauthorized={unauthorized} empty={empty} handler_errors={handler_errors} "
        f"failed_queue_total={failed_queue_total} last_failed_at={last_failed_at}\n"
        f"poll_cursor: offset={offset} updated_at={poll_updated_at} seen_update_ids={seen_updates} seen_message_keys={seen_messages}"
    )
