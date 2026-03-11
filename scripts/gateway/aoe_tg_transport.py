#!/usr/bin/env python3
"""Telegram transport helpers shared by gateway entrypoints."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional


def split_text(text: str, max_chars: int) -> List[str]:
    max_chars = max(200, int(max_chars))
    src = (text or "").strip()
    if not src:
        return ["(empty)"]
    if len(src) <= max_chars:
        return [src]

    lines = src.splitlines()
    chunks: List[str] = []
    buf: List[str] = []
    size = 0

    def flush() -> None:
        nonlocal buf, size
        if buf:
            chunks.append("\n".join(buf))
            buf = []
            size = 0

    for line in lines:
        candidate = line if len(line) <= max_chars else line[: max_chars - 3] + "..."
        add_len = len(candidate) + (1 if buf else 0)
        if size + add_len > max_chars:
            flush()
        buf.append(candidate)
        size += add_len

    flush()
    return chunks


def tg_api(token: str, method: str, payload: Dict[str, Any], timeout_sec: int) -> Dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace") if hasattr(e, "read") else str(e)
        raise RuntimeError(f"Telegram API HTTP error ({method}): {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(f"Telegram API URL error ({method}): {e}") from e

    try:
        data = json.loads(raw)
    except Exception as e:
        raise RuntimeError(f"Telegram API invalid JSON ({method}): {raw[:300]}") from e

    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error ({method}): {data}")

    return data


def tg_send_text(
    token: str,
    chat_id: str,
    text: str,
    max_chars: int,
    timeout_sec: int,
    dry_run: bool,
    reply_markup: Optional[Dict[str, Any]] = None,
) -> None:
    chunks = split_text(text, max_chars)
    for i, chunk in enumerate(chunks):
        if dry_run or (not str(token or "").strip()):
            print(f"[DRY-SEND chat_id={chat_id}]\n{chunk}\n")
            if i == 0 and isinstance(reply_markup, dict):
                print(f"[DRY-MARKUP chat_id={chat_id}] {json.dumps(reply_markup, ensure_ascii=False)}")
            continue

        payload: Dict[str, Any] = {
            "chat_id": chat_id,
            "text": chunk,
            "disable_web_page_preview": True,
        }
        if i == 0 and isinstance(reply_markup, dict):
            payload["reply_markup"] = reply_markup

        tg_api(
            token,
            "sendMessage",
            payload,
            timeout_sec=timeout_sec,
        )


def safe_tg_send_text(
    token: str,
    chat_id: str,
    text: str,
    max_chars: int,
    timeout_sec: int,
    dry_run: bool,
    verbose: bool,
    context: str = "",
    reply_markup: Optional[Dict[str, Any]] = None,
) -> bool:
    try:
        tg_send_text(token, chat_id, text, max_chars, timeout_sec, dry_run, reply_markup=reply_markup)
        return True
    except Exception as e:
        if verbose:
            suffix = f" ({context})" if context else ""
            print(f"[ERROR] sendMessage failed{suffix}: chat_id={chat_id} error={e}", file=sys.stderr, flush=True)
        return False


def tg_get_updates(token: str, offset: int, poll_timeout_sec: int, timeout_sec: int) -> List[Dict[str, Any]]:
    payload = {
        "offset": int(offset),
        "timeout": int(poll_timeout_sec),
        "allowed_updates": ["message"],
    }
    data = tg_api(token, "getUpdates", payload, timeout_sec=timeout_sec)
    result = data.get("result", [])
    if not isinstance(result, list):
        return []
    return [x for x in result if isinstance(x, dict)]


def preferred_command_prefix() -> str:
    raw = str(os.environ.get("AOE_TG_COMMAND_PREFIXES", "/") or "/").strip()
    for ch in raw:
        if ch in {"/", "!"}:
            return ch
    return "/"


def build_quick_reply_keyboard() -> Dict[str, Any]:
    p = preferred_command_prefix()
    return {
        "keyboard": [
            [{"text": f"{p}status"}, {"text": f"{p}help"}, {"text": f"{p}tutorial"}],
            [{"text": f"{p}map"}, {"text": f"{p}queue"}, {"text": f"{p}sync"}, {"text": f"{p}next"}],
            [{"text": f"{p}fanout"}, {"text": f"{p}auto"}, {"text": f"{p}offdesk"}, {"text": f"{p}panic"}],
            [{"text": f"{p}monitor"}, {"text": f"{p}check"}, {"text": f"{p}task"}, {"text": f"{p}pick"}],
            [{"text": f"{p}todo"}, {"text": f"{p}room"}, {"text": f"{p}clear"}, {"text": f"{p}gc"}],
            [{"text": f"{p}dispatch"}, {"text": f"{p}direct"}, {"text": f"{p}mode"}, {"text": f"{p}lang"}, {"text": f"{p}report"}],
            [{"text": f"{p}whoami"}, {"text": f"{p}acl"}],
        ],
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "is_persistent": True,
        "input_field_placeholder": f"예: {p}tutorial 또는 {p}dispatch 결측치 규칙 정리해줘",
    }
