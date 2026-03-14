#!/usr/bin/env python3
"""Provider rate-limit detection and fallback helpers."""

from __future__ import annotations

import re
from typing import Iterable, Optional


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


def build_rate_limit_snapshot(
    *,
    mode: str,
    limited_providers: Iterable[str],
    degraded_by: Iterable[str] = (),
    retry_after_sec: int = 300,
) -> dict:
    providers = [str(item).strip().lower() for item in (limited_providers or []) if str(item).strip()]
    degraded = [str(item).strip() for item in (degraded_by or []) if str(item).strip()]
    return {
        "mode": str(mode or "").strip().lower(),
        "limited_providers": providers,
        "degraded_by": degraded,
        "retry_after_sec": max(60, int(retry_after_sec or 300)),
    }
