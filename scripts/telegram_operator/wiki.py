"""Adaptive wiki bridge for Telegram operator memory candidates."""

from __future__ import annotations

import json
import os
import pathlib
import sys
import uuid
from contextlib import contextmanager
from typing import Any

from .common import RemoteOperatorTelegramError, load_json, sha256_short, utc_now, write_json
from .rendering import sanitize_text

ADAPTIVE_WIKI_VERSION = "2026-05-14.v0"
ADAPTIVE_WIKI_CANDIDATES_FILE = "adaptive_wiki_candidates.json"
ADAPTIVE_WIKI_LOCK_FILE = "adaptive_wiki.lock"


def normalize_profile_name(profile: Any) -> str:
    name = str(profile or "default").strip() or "default"
    if name in {".", ".."} or "/" in name or "\\" in name or "\0" in name:
        raise RemoteOperatorTelegramError(f"invalid profile name: {name}")
    return name


def app_dir() -> pathlib.Path:
    home = pathlib.Path(os.environ.get("HOME") or pathlib.Path.home())
    if sys.platform.startswith("linux"):
        config_home = pathlib.Path(os.environ.get("XDG_CONFIG_HOME") or home / ".config")
        primary = config_home / "forager"
        legacy_paths = [config_home / "agent-of-empires", home / ".agent-of-empires"]
    else:
        primary = home / ".forager"
        legacy_paths = [home / ".agent-of-empires"]
    if primary.exists():
        return primary
    for path in legacy_paths:
        if path.exists():
            return path
    return primary


def profile_dir(profile: Any) -> pathlib.Path:
    path = app_dir() / "profiles" / normalize_profile_name(profile)
    path.mkdir(parents=True, exist_ok=True)
    return path


def candidates_path(profile: Any) -> pathlib.Path:
    return profile_dir(profile) / ADAPTIVE_WIKI_CANDIDATES_FILE


def normalize_key(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def push_unique(values: list[Any], value: Any) -> None:
    text = str(value or "").strip()
    if text and text not in values:
        values.append(text)


@contextmanager
def adaptive_wiki_lock(path: pathlib.Path):
    try:
        import fcntl
    except ImportError as error:
        raise RemoteOperatorTelegramError(
            "Telegram wiki updates require local file-lock support"
        ) from error
    lock_path = path.parent / ADAPTIVE_WIKI_LOCK_FILE
    lock_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    except OSError as error:
        raise RemoteOperatorTelegramError(
            f"adaptive wiki state lock failed: {type(error).__name__}"
        ) from error
    finally:
        if "descriptor" in locals():
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


def load_candidate_state(path: pathlib.Path) -> dict[str, Any]:
    empty = {"version": ADAPTIVE_WIKI_VERSION, "candidates": []}
    if not path.exists():
        return empty
    try:
        state = load_json(path)
    except (OSError, json.JSONDecodeError) as error:
        raise RemoteOperatorTelegramError(
            f"adaptive wiki candidate state is unreadable and was not reset: "
            f"{path} ({type(error).__name__})"
        ) from error
    if not isinstance(state, dict):
        raise RemoteOperatorTelegramError(
            f"adaptive wiki candidate state must be a JSON object: {path}"
        )
    candidates = state.setdefault("candidates", [])
    if not isinstance(candidates, list):
        raise RemoteOperatorTelegramError(
            f"adaptive wiki candidate list is invalid and was not reset: {path}"
        )
    state.setdefault("version", ADAPTIVE_WIKI_VERSION)
    return state


def source_hash_for_candidate(*, text: str, chat_hash: str | None, message_id: int | None) -> str:
    payload = {
        "chat_id_hash": chat_hash,
        "message_id": message_id,
        "remember_text": sanitize_text(text, max_chars=2000),
    }
    return sha256_short(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def candidate_from_remember_text(
    *,
    text: str,
    chat_hash: str | None,
    user_hash: str | None,
    message_id: int | None,
    project_key: str | None = None,
) -> dict[str, Any]:
    now = utc_now()
    claim = sanitize_text(text, max_chars=2000)
    evidence_refs = [f"telegram:message:{message_id}"] if message_id is not None else ["telegram:remember"]
    source_refs = ["telegram.remote_operator.remember"]
    if chat_hash:
        source_refs.append(f"telegram:chat:{chat_hash}")
    if user_hash:
        source_refs.append(f"telegram:user:{user_hash}")
    scope = "project" if project_key else "user_global"
    scope_ref = str(project_key or "*")
    return {
        "id": f"wiki_candidate_{uuid.uuid4()}",
        "kind": "preference",
        "scope": scope,
        "scope_ref": scope_ref,
        "claim": claim,
        "suggested_ai_instruction": (
            "After local review and promotion, treat this as an operator preference: "
            + claim
        ),
        "human_summary": "Telegram /remember request: " + claim,
        "evidence_refs": evidence_refs,
        "signal_kind": "explicit_preference",
        "origin": "operator_explicit",
        "source_refs": source_refs,
        "source_hashes": [
            source_hash_for_candidate(text=claim, chat_hash=chat_hash, message_id=message_id)
        ],
        "suggested_scope": {"scope": scope, "scope_ref": scope_ref},
        "review_reason": (
            "Operator explicitly used /remember in Telegram. This is only a candidate "
            "until local adaptive wiki review promotes or rejects it."
        ),
        "occurrence_count": 1,
        "confidence": "explicit",
        "created_at": now,
        "updated_at": now,
        "last_seen_at": now,
    }


def record_remember_candidate(
    *,
    profile: Any,
    text: str,
    chat_hash: str | None,
    user_hash: str | None,
    message_id: int | None,
    project_key: str | None = None,
) -> dict[str, Any]:
    claim = sanitize_text(text, max_chars=2000)
    if not claim:
        raise RemoteOperatorTelegramError("/remember requires text")
    path = candidates_path(profile)
    with adaptive_wiki_lock(path):
        state = load_candidate_state(path)
        candidates = state["candidates"]
        candidate_key = normalize_key(claim)
        status = "recorded"
        candidate = None
        for existing in candidates:
            if not isinstance(existing, dict):
                continue
            if (
                existing.get("kind") == "preference"
                and existing.get("scope") == ("project" if project_key else "user_global")
                and existing.get("scope_ref") == str(project_key or "*")
                and normalize_key(existing.get("claim")) == candidate_key
            ):
                candidate = existing
                break
        incoming = candidate_from_remember_text(
            text=claim,
            chat_hash=chat_hash,
            user_hash=user_hash,
            message_id=message_id,
            project_key=project_key,
        )
        if candidate is None:
            candidate = incoming
            candidates.append(candidate)
        else:
            status = "updated"
            candidate["occurrence_count"] = int(candidate.get("occurrence_count") or 0) + 1
            candidate["updated_at"] = incoming["updated_at"]
            candidate["last_seen_at"] = incoming["last_seen_at"]
            candidate["suggested_ai_instruction"] = incoming["suggested_ai_instruction"]
            candidate["human_summary"] = incoming["human_summary"]
            candidate["review_reason"] = incoming["review_reason"]
            candidate["confidence"] = "explicit"
            for field in ("evidence_refs", "source_refs", "source_hashes"):
                values = candidate.setdefault(field, [])
                if not isinstance(values, list):
                    values = []
                    candidate[field] = values
                for value in incoming.get(field, []):
                    push_unique(values, value)
        write_json(path, state)
    return {
        "wiki_candidate_recorded": True,
        "wiki_candidate_status": status,
        "wiki_candidate_id": candidate.get("id"),
        "wiki_candidate_path": str(path),
        "adaptive_wiki_candidate": candidate,
    }
