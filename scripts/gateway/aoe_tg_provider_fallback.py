#!/usr/bin/env python3
"""Provider rate-limit detection and fallback helpers."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional


_RATE_LIMIT_PATTERNS = (
    r"\brate[\s_-]*limit(?:ed|ing)?\b",
    r"\b429\b",
    r"\btoo many requests\b",
    r"\bretry[\s_-]*after\b",
    r"\bquota\b",
    r"\boverloaded\b",
    r"\bcapacity\b",
)

_RATE_LIMIT_RE = re.compile("|".join(_RATE_LIMIT_PATTERNS), re.IGNORECASE)

_PROVIDER_FALLBACKS = {
    "claude": "codex",
    "codex": "claude",
}


def is_rate_limit_error(raw: object) -> bool:
    text = str(raw or "").strip()
    if not text:
        return False
    return bool(_RATE_LIMIT_RE.search(text))


def fallback_provider_for(raw: Optional[str]) -> str:
    token = str(raw or "").strip().lower()
    return _PROVIDER_FALLBACKS.get(token, "")


def load_provider_capacity_state(team_dir: Any) -> dict:
    token = str(team_dir or "").strip()
    if not token:
        return {}
    try:
        path = Path(token).expanduser().resolve() / "provider_capacity.json"
    except Exception:
        return {}
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def extract_retry_after_sec(raw: object, *, default: int = 300) -> int:
    text = str(raw or "").strip()
    if not text:
        return max(60, int(default or 300))
    match = re.search(r"retry[\s_-]*after[^0-9]*(\d+)", text, re.IGNORECASE)
    if match:
        try:
            value = int(match.group(1))
            return max(60, value)
        except Exception:
            pass
    return max(60, int(default or 300))


def compute_retry_at_iso(
    retry_after_sec: int,
    *,
    now: Optional[datetime] = None,
) -> str:
    base = now or datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    target = base.astimezone(timezone.utc) + timedelta(seconds=max(60, int(retry_after_sec or 300)))
    return target.replace(microsecond=0).isoformat()


def parse_retry_at(raw: Any) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def rate_limit_retry_active(snapshot: object, *, now: Optional[datetime] = None) -> bool:
    data = snapshot if isinstance(snapshot, dict) else {}
    if str(data.get("mode", "")).strip().lower() != "blocked":
        return False
    parsed = parse_retry_at(data.get("retry_at"))
    if parsed is None:
        return True
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return parsed > current.astimezone(timezone.utc)


def provider_cooldown_active(memory_state: Any, provider: str, *, now: Optional[datetime] = None) -> bool:
    if not isinstance(memory_state, dict):
        return False
    providers = memory_state.get("providers") if isinstance(memory_state.get("providers"), dict) else {}
    row = providers.get(str(provider or "").strip().lower()) if isinstance(providers, dict) else None
    if not isinstance(row, dict):
        return False
    parsed = parse_retry_at(row.get("next_retry_at") or row.get("last_retry_at"))
    if parsed is None:
        return False
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return parsed > current.astimezone(timezone.utc)


def proactive_fallback_provider(
    provider: str,
    *,
    memory_state: Any,
    available_providers: Iterable[str] = (),
    now: Optional[datetime] = None,
) -> str:
    origin = str(provider or "").strip().lower()
    fallback = fallback_provider_for(origin)
    if not fallback or fallback == origin:
        return ""
    available = {str(item or "").strip().lower() for item in (available_providers or []) if str(item or "").strip()}
    if available and fallback not in available:
        return ""
    if not provider_cooldown_active(memory_state, origin, now=now):
        return ""
    if provider_cooldown_active(memory_state, fallback, now=now):
        return ""
    return fallback


def build_rate_limit_snapshot(
    *,
    mode: str,
    limited_providers: Iterable[str],
    degraded_by: Iterable[str] = (),
    retry_after_sec: int = 300,
) -> dict:
    providers = [str(item).strip().lower() for item in (limited_providers or []) if str(item).strip()]
    degraded = [str(item).strip() for item in (degraded_by or []) if str(item).strip()]
    retry_after = max(60, int(retry_after_sec or 300))
    return {
        "mode": str(mode or "").strip().lower(),
        "limited_providers": providers,
        "degraded_by": degraded,
        "retry_after_sec": retry_after,
        "retry_at": compute_retry_at_iso(retry_after),
    }
