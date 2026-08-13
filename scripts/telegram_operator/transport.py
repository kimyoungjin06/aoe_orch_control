"""Telegram Bot API transport helpers."""

from __future__ import annotations

import http.client
import json
import os
import urllib.error
import urllib.request
from typing import Any

from .common import RemoteOperatorTelegramError, load_json


def telegram_api(token: str, method: str, payload: dict[str, Any], timeout_sec: int) -> dict[str, Any]:
    api_base = os.environ.get(
        "OFFDESK_REMOTE_OPERATOR_TELEGRAM_API_BASE_URL",
        "https://api.telegram.org",
    ).rstrip("/")
    url = f"{api_base}/bot{token}/{method}"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace") if hasattr(error, "read") else str(error)
        raise RemoteOperatorTelegramError(f"Telegram API HTTP error ({method}): {detail}") from error
    except (TimeoutError, http.client.RemoteDisconnected, ConnectionError) as error:
        raise RemoteOperatorTelegramError(
            f"Telegram API transport error ({method}): {type(error).__name__}: {error}"
        ) from error
    except urllib.error.URLError as error:
        raise RemoteOperatorTelegramError(f"Telegram API URL error ({method}): {error}") from error
    except json.JSONDecodeError as error:
        raise RemoteOperatorTelegramError(f"Telegram API invalid JSON ({method})") from error
    if not data.get("ok"):
        raise RemoteOperatorTelegramError(f"Telegram API error ({method}): {data}")
    return data


def get_updates(config: dict[str, Any], offset: int, args: Any) -> list[dict[str, Any]]:
    if args.replay_update_file:
        value = load_json(args.replay_update_file)
        if isinstance(value, dict) and isinstance(value.get("result"), list):
            raw_updates = value["result"]
        elif isinstance(value, list):
            raw_updates = value
        elif isinstance(value, dict):
            raw_updates = [value]
        else:
            raw_updates = []
        updates = [item for item in raw_updates if isinstance(item, dict)]
        return [
            item
            for item in updates
            if not isinstance(item.get("update_id"), int) or item["update_id"] >= int(offset)
        ]
    data = telegram_api(
        config["token"],
        "getUpdates",
        {
            "offset": int(offset),
            "timeout": max(0, int(args.poll_timeout_sec)),
            "allowed_updates": ["message"],
        },
        timeout_sec=max(int(args.api_timeout_sec), int(args.poll_timeout_sec) + 10),
    )
    updates = data.get("result", [])
    return [item for item in updates if isinstance(item, dict)] if isinstance(updates, list) else []


def send_message(
    config: dict[str, Any],
    chat_id: str,
    message: str,
    args: Any,
    *,
    reply_markup: dict[str, Any] | None = None,
) -> int | None:
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    if args.dry_run:
        return None
    data = telegram_api(
        config["token"],
        "sendMessage",
        payload,
        timeout_sec=max(1, int(args.api_timeout_sec)),
    )
    result = data.get("result")
    if isinstance(result, dict) and isinstance(result.get("message_id"), int):
        return int(result["message_id"])
    return None


def edit_message(
    config: dict[str, Any],
    chat_id: str,
    message_id: int,
    message: str,
    args: Any,
) -> bool:
    """Best-effort edit of a previously sent message (e.g. expiring a stale
    action card). Returns False instead of raising on dry-run or API refusal
    (edits fail once a message is older than Telegram's 48h edit window)."""

    if args.dry_run:
        return False
    try:
        telegram_api(
            config["token"],
            "editMessageText",
            {
                "chat_id": chat_id,
                "message_id": int(message_id),
                "text": message,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout_sec=max(1, int(args.api_timeout_sec)),
        )
        return True
    except Exception:  # noqa: BLE001 - expiry edits must never break the loop
        return False
