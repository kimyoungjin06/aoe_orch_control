"""Telegram adapter configuration resolution."""

from __future__ import annotations

import os
import pathlib
import sys
from typing import Any

from .common import (
    RemoteOperatorTelegramError,
    csv_values,
    parse_env_file,
    sha256_short,
)


def default_telegram_env_file() -> pathlib.Path:
    configured = os.environ.get("OFFDESK_TELEGRAM_ENV", "").strip()
    if configured:
        return pathlib.Path(configured).expanduser()
    if sys.platform.startswith("linux"):
        config_root = pathlib.Path(
            os.environ.get("XDG_CONFIG_HOME", pathlib.Path.home() / ".config")
        )
        return config_root / "forager" / "telegram.env"
    return pathlib.Path.home() / ".forager" / "telegram.env"


def resolve_telegram_config(env_file: pathlib.Path, *, required: bool) -> dict[str, Any]:
    env = parse_env_file(env_file, required=required)
    token = env.get("TELEGRAM_BOT_TOKEN", "").strip()
    owner_chat_id = env.get("TELEGRAM_OWNER_CHAT_ID", "").strip()
    allowed_chat_ids = set(csv_values(env.get("TELEGRAM_ALLOW_CHAT_IDS", "")))
    allowed_chat_ids.update(csv_values(env.get("TELEGRAM_ALLOWED_CHAT_IDS", "")))
    if owner_chat_id:
        allowed_chat_ids.add(owner_chat_id)
    owner_user_id = env.get("TELEGRAM_OWNER_USER_ID", "").strip()
    allowed_user_ids = set(csv_values(env.get("TELEGRAM_ALLOW_USER_IDS", "")))
    allowed_user_ids.update(csv_values(env.get("TELEGRAM_ALLOWED_USER_IDS", "")))
    if owner_user_id:
        allowed_user_ids.add(owner_user_id)
    target_chat_id = owner_chat_id or next(iter(sorted(allowed_chat_ids)), "")
    if required and not token:
        raise RemoteOperatorTelegramError("TELEGRAM_BOT_TOKEN is missing")
    if required and not allowed_chat_ids:
        raise RemoteOperatorTelegramError(
            "TELEGRAM_OWNER_CHAT_ID or TELEGRAM_ALLOW_CHAT_IDS is required"
        )
    return {
        "token": token,
        "target_chat_id": target_chat_id,
        "target_chat_id_hash": sha256_short(target_chat_id) if target_chat_id else None,
        "allowed_chat_ids": allowed_chat_ids,
        "allowed_user_ids": allowed_user_ids,
        "chat_allowlist_configured": bool(allowed_chat_ids),
        "user_allowlist_configured": bool(allowed_user_ids),
        "env_file": str(env_file),
    }
