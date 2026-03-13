#!/usr/bin/env python3
"""Role naming compatibility helpers.

This module preserves compatibility with older role names while the canonical
naming scheme moves to provider-explicit `Codex-*` roles.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List


ROLE_RENAMES = {
    "Reviewer": "Codex-Reviewer",
    "Local-Dev": "Codex-Dev",
    "Local-Writer": "Codex-Writer",
    "Local-Analyst": "Codex-Analyst",
}

LEGACY_ROLE_NAMES = {new: old for old, new in ROLE_RENAMES.items()}
_ROLE_CASEFOLD_MAP = {name.casefold(): new for name, new in ROLE_RENAMES.items()}
_ROLE_CASEFOLD_MAP.update({name.casefold(): name for name in LEGACY_ROLE_NAMES})


def canonicalize_role_name(raw: object) -> str:
    token = str(raw or "").strip()
    if not token:
        return ""
    return _ROLE_CASEFOLD_MAP.get(token.casefold(), token)


def canonicalize_roles(rows: Iterable[object]) -> List[str]:
    out: List[str] = []
    seen = set()
    for row in rows or []:
        token = canonicalize_role_name(row)
        if not token:
            continue
        key = token.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def role_name_variants(raw: object) -> List[str]:
    canonical = canonicalize_role_name(raw)
    if not canonical:
        return []
    legacy = LEGACY_ROLE_NAMES.get(canonical)
    variants = [canonical]
    if legacy and legacy.casefold() != canonical.casefold():
        variants.append(legacy)
    return variants


def resolve_role_asset(base_dir: Path, role: object, leaf: str) -> Path:
    root = Path(base_dir)
    variants = role_name_variants(role) or [str(role or "").strip()]
    for variant in variants:
        path = root / variant / leaf
        if path.exists():
            return path
    return root / variants[0] / leaf


def resolve_role_file(base_dir: Path, role: object, suffix: str) -> Path:
    root = Path(base_dir)
    variants = role_name_variants(role) or [str(role or "").strip()]
    for variant in variants:
        path = root / f"{variant}{suffix}"
        if path.exists():
            return path
    return root / f"{variants[0]}{suffix}"
