#!/usr/bin/env python3
"""Project-local sandbox TF backend selection helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

from aoe_tg_tf_backend import DEFAULT_TF_BACKEND, normalize_tf_backend_name


DEFAULT_TF_BACKEND_CONFIG_NAME = "tf_backend.json"


def _bool_from_json(raw: Any, default: bool = False) -> bool:
    if isinstance(raw, bool):
        return raw
    token = str(raw or "").strip().lower()
    if not token:
        return default
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    return default


def resolve_tf_backend_config_path(team_dir: Path, explicit: Optional[str] = None) -> Path:
    if explicit:
        return Path(explicit).expanduser().resolve()
    return Path(team_dir).expanduser().resolve() / DEFAULT_TF_BACKEND_CONFIG_NAME


def sanitize_tf_backend_selection(raw: Any, *, config_path: Optional[Path] = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "enabled": False,
        "backend": DEFAULT_TF_BACKEND,
        "profile": "",
        "sandbox_only": True,
        "notes": "",
        "config_path": str(config_path) if config_path is not None else "",
        "selection_reason": "default_local",
    }
    if not isinstance(raw, dict):
        return result

    enabled = _bool_from_json(raw.get("enabled"), False)
    backend = normalize_tf_backend_name(raw.get("backend"), default=DEFAULT_TF_BACKEND)
    profile = str(raw.get("profile", "") or "").strip().lower()[:64]
    sandbox_only = _bool_from_json(raw.get("sandbox_only"), True)
    notes = str(raw.get("notes", "") or "").strip()[:240]

    result.update(
        {
            "enabled": enabled,
            "backend": backend,
            "profile": profile,
            "sandbox_only": sandbox_only,
            "notes": notes,
        }
    )
    if enabled and backend != DEFAULT_TF_BACKEND:
        if sandbox_only and profile != "sandbox":
            result["selection_reason"] = "sandbox_guard"
        else:
            result["selection_reason"] = "sandbox_config"
    elif enabled and backend == DEFAULT_TF_BACKEND:
        result["selection_reason"] = "sandbox_local"
    return result


def load_tf_backend_selection(team_dir: Path, explicit: Optional[str] = None) -> Dict[str, Any]:
    path = resolve_tf_backend_config_path(team_dir, explicit)
    if not path.exists():
        return sanitize_tf_backend_selection({}, config_path=path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        row = sanitize_tf_backend_selection({}, config_path=path)
        row["selection_reason"] = "invalid_config"
        return row
    return sanitize_tf_backend_selection(payload, config_path=path)


def resolve_effective_tf_backend(team_dir: Path, explicit: Optional[str] = None) -> Dict[str, Any]:
    row = load_tf_backend_selection(team_dir, explicit)
    effective = DEFAULT_TF_BACKEND
    if row.get("enabled") and row.get("backend") != DEFAULT_TF_BACKEND:
        if (not row.get("sandbox_only")) or str(row.get("profile", "")).strip().lower() == "sandbox":
            effective = str(row.get("backend", DEFAULT_TF_BACKEND)).strip() or DEFAULT_TF_BACKEND
    out = dict(row)
    out["effective_backend"] = normalize_tf_backend_name(effective)
    return out

