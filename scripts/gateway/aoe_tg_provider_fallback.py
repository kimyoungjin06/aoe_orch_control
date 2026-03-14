#!/usr/bin/env python3
"""Provider rate-limit detection and fallback helpers."""

from __future__ import annotations

import re
from typing import Optional


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
}


def is_rate_limit_error(raw: object) -> bool:
    text = str(raw or "").strip()
    if not text:
        return False
    return bool(_RATE_LIMIT_RE.search(text))


def fallback_provider_for(raw: Optional[str]) -> str:
    token = str(raw or "").strip().lower()
    return _PROVIDER_FALLBACKS.get(token, "")
