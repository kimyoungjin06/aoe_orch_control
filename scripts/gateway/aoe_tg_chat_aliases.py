#!/usr/bin/env python3
"""Chat alias persistence helpers extracted from the gateway monolith."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict, Iterable, Optional, Set, Tuple

from aoe_tg_acl import is_valid_chat_alias, is_valid_chat_id, resolve_role_from_acl_sets


def resolve_chat_aliases_file(team_dir: Path, explicit_path: Optional[str]) -> Path:
    if explicit_path:
        return Path(explicit_path).expanduser().resolve()
    env_path = (os.environ.get("AOE_CHAT_ALIASES_FILE") or "").strip()
    if env_path:
        return Path(env_path).expanduser().resolve()
    return team_dir / "telegram_chat_aliases.json"


def load_chat_aliases(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(data, dict):
        return {}

    out: Dict[str, str] = {}
    seen_chat_ids: Set[str] = set()
    for key in sorted(data.keys(), key=lambda x: int(x) if str(x).isdigit() else 10**9):
        alias = str(key or "").strip()
        chat_id = str(data.get(key) or "").strip()
        if not is_valid_chat_alias(alias) or not is_valid_chat_id(chat_id) or chat_id in seen_chat_ids:
            continue
        out[alias] = chat_id
        seen_chat_ids.add(chat_id)
    return out


def save_chat_aliases(path: Path, aliases: Dict[str, str]) -> None:
    sanitized: Dict[str, str] = {}
    seen_chat_ids: Set[str] = set()
    for alias in sorted(aliases.keys(), key=lambda x: int(str(x)) if str(x).isdigit() else 10**9):
        a = str(alias or "").strip()
        chat_id = str(aliases.get(alias) or "").strip()
        if not is_valid_chat_alias(a) or not is_valid_chat_id(chat_id) or chat_id in seen_chat_ids:
            continue
        sanitized[a] = chat_id
        seen_chat_ids.add(chat_id)

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(sanitized, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def merged_chat_aliases(args: argparse.Namespace) -> Dict[str, str]:
    rows = load_chat_aliases(args.chat_aliases_file)
    cache = getattr(args, "chat_alias_cache", {})
    if not isinstance(cache, dict):
        return rows
    seen = set(rows.values())
    for alias in sorted(cache.keys(), key=lambda x: int(str(x)) if str(x).isdigit() else 10**9):
        a = str(alias or "").strip()
        chat_id = str(cache.get(alias) or "").strip()
        if not is_valid_chat_alias(a) or not is_valid_chat_id(chat_id):
            continue
        if a in rows or chat_id in seen:
            continue
        rows[a] = chat_id
        seen.add(chat_id)
    return rows


def update_chat_alias_cache(args: argparse.Namespace, aliases: Dict[str, str]) -> None:
    args.chat_alias_cache = dict(aliases)


def find_chat_alias(aliases: Dict[str, str], chat_id: str) -> str:
    cid = str(chat_id or "").strip()
    for alias, mapped in aliases.items():
        if str(mapped).strip() == cid:
            return str(alias).strip()
    return ""


def next_chat_alias(aliases: Dict[str, str], max_alias: int = 999) -> str:
    used = {int(k) for k in aliases.keys() if str(k).isdigit()}
    for idx in range(1, max_alias + 1):
        if idx not in used:
            return str(idx)
    return ""


def ensure_chat_alias(
    args: argparse.Namespace,
    chat_id: str,
    persist: bool = True,
    aliases: Optional[Dict[str, str]] = None,
) -> str:
    cid = str(chat_id or "").strip()
    if not is_valid_chat_id(cid):
        return ""
    rows = aliases if isinstance(aliases, dict) else merged_chat_aliases(args)
    existing = find_chat_alias(rows, cid)
    if existing:
        update_chat_alias_cache(args, rows)
        return existing
    alias = next_chat_alias(rows)
    if not alias:
        return ""
    rows[alias] = cid
    update_chat_alias_cache(args, rows)
    if persist and (not args.dry_run):
        save_chat_aliases(args.chat_aliases_file, rows)
    return alias


def ensure_chat_aliases(args: argparse.Namespace, chat_ids: Iterable[str], persist: bool = True) -> Dict[str, str]:
    aliases = merged_chat_aliases(args)
    changed = False
    for raw in chat_ids:
        cid = str(raw or "").strip()
        if not is_valid_chat_id(cid):
            continue
        if find_chat_alias(aliases, cid):
            continue
        alias = next_chat_alias(aliases)
        if not alias:
            break
        aliases[alias] = cid
        changed = True
    update_chat_alias_cache(args, aliases)
    if changed and persist and (not args.dry_run):
        save_chat_aliases(args.chat_aliases_file, aliases)
    return aliases


def resolve_chat_ref(args: argparse.Namespace, chat_ref: str) -> Tuple[str, str]:
    token = str(chat_ref or "").strip()
    if is_valid_chat_id(token):
        alias = ensure_chat_alias(args, token, persist=(not args.dry_run))
        return token, alias
    if is_valid_chat_alias(token):
        aliases = merged_chat_aliases(args)
        chat_id = str(aliases.get(token, "")).strip()
        if is_valid_chat_id(chat_id):
            return chat_id, token
        raise RuntimeError(f"unknown chat alias: {token} (use /acl)")
    raise RuntimeError("chat target must be chat_id or alias")


def alias_table_summary(args: argparse.Namespace, limit: int = 30) -> str:
    aliases = merged_chat_aliases(args)
    if not aliases:
        return "(empty)"

    rows = []
    for alias in sorted(aliases.keys(), key=lambda x: int(x)):
        chat_id = str(aliases.get(alias, "")).strip()
        if not is_valid_chat_id(chat_id):
            continue
        role = resolve_role_from_acl_sets(
            chat_id=chat_id,
            allow_chat_ids=args.allow_chat_ids,
            admin_chat_ids=args.admin_chat_ids,
            readonly_chat_ids=args.readonly_chat_ids,
            deny_by_default=bool(args.deny_by_default),
        )
        rows.append(f"{alias}:{chat_id}[{role}]")
        if len(rows) >= max(1, int(limit)):
            break
    return ", ".join(rows) if rows else "(empty)"
