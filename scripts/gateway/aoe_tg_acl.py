#!/usr/bin/env python3
"""ACL and chat authorization helpers for aoe-telegram-gateway."""

import re
import shlex
from typing import Optional, Set, Tuple

ACL_SCOPE_ALIASES = {
    "allow": "allow",
    "allowed": "allow",
    "admin": "admin",
    "owner": "admin",
    "readonly": "readonly",
    "read": "readonly",
    "ro": "readonly",
    "all": "all",
}


def parse_csv_set(raw: Optional[str]) -> Set[str]:
    if not raw:
        return set()
    out: Set[str] = set()
    for item in raw.split(","):
        token = item.strip()
        if token:
            out.add(token)
    return out


def format_csv_set(values: Set[str]) -> str:
    return ",".join(sorted(str(x).strip() for x in values if str(x).strip()))


def normalize_acl_scope(raw: str) -> str:
    token = str(raw or "").strip().lower()
    return ACL_SCOPE_ALIASES.get(token, "")


def is_valid_chat_id(raw: str) -> bool:
    token = str(raw or "").strip()
    return bool(re.fullmatch(r"-?\d{5,20}", token))


def is_valid_chat_alias(raw: str) -> bool:
    token = str(raw or "").strip()
    return bool(re.fullmatch(r"[1-9]\d{0,2}", token))


def is_valid_chat_ref(raw: str) -> bool:
    token = str(raw or "").strip()
    return is_valid_chat_id(token) or is_valid_chat_alias(token)


def parse_acl_command_args(rest: str, usage: str) -> Tuple[str, str]:
    text = str(rest or "").strip()
    try:
        parts = shlex.split(text) if text else []
    except ValueError as e:
        raise RuntimeError(f"{usage} ({e})")
    if len(parts) != 2:
        raise RuntimeError(usage)

    scope = normalize_acl_scope(parts[0])
    if not scope or scope == "all":
        raise RuntimeError(usage)

    chat_ref = str(parts[1] or "").strip()
    if not is_valid_chat_ref(chat_ref):
        raise RuntimeError(f"{usage} (chat target must be chat_id or alias)")

    return scope, chat_ref


def parse_acl_revoke_args(rest: str, usage: str) -> Tuple[str, str]:
    text = str(rest or "").strip()
    try:
        parts = shlex.split(text) if text else []
    except ValueError as e:
        raise RuntimeError(f"{usage} ({e})")
    if len(parts) != 2:
        raise RuntimeError(usage)

    scope = normalize_acl_scope(parts[0])
    if not scope:
        raise RuntimeError(usage)

    chat_ref = str(parts[1] or "").strip()
    if not is_valid_chat_ref(chat_ref):
        raise RuntimeError(f"{usage} (chat target must be chat_id or alias)")

    return scope, chat_ref


def normalize_owner_chat_id(raw: str) -> str:
    token = str(raw or "").strip()
    return token if is_valid_chat_id(token) else ""


def resolve_role_from_acl_sets(
    chat_id: str,
    allow_chat_ids: Set[str],
    admin_chat_ids: Set[str],
    readonly_chat_ids: Set[str],
    deny_by_default: bool,
) -> str:
    cid = str(chat_id or "").strip()
    if not cid:
        return "unknown"
    if cid in admin_chat_ids:
        return "admin"
    if cid in readonly_chat_ids:
        return "readonly"
    if cid in allow_chat_ids:
        # allow and admin are intentionally equivalent in current policy.
        return "admin"
    if (not allow_chat_ids) and (not admin_chat_ids) and (not readonly_chat_ids) and (not bool(deny_by_default)):
        return "admin"
    return "unknown"


def ensure_chat_allowed(
    chat_id: str,
    allow_chat_ids: Set[str],
    admin_chat_ids: Set[str],
    readonly_chat_ids: Set[str],
    deny_by_default: bool,
    owner_chat_id: str = "",
) -> bool:
    owner = normalize_owner_chat_id(owner_chat_id)
    if owner and str(chat_id).strip() == owner:
        return True

    merged = set(allow_chat_ids) | set(admin_chat_ids) | set(readonly_chat_ids)
    if not merged:
        return not bool(deny_by_default)

    return str(chat_id) in merged
