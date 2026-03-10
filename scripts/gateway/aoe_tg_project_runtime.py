#!/usr/bin/env python3
"""Project runtime readiness helpers for Telegram gateway orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict


def project_hidden_from_ops(entry: Dict[str, Any]) -> bool:
    if not isinstance(entry, dict):
        return False
    raw = entry.get("ops_hidden")
    if isinstance(raw, bool):
        return raw
    token = str(raw or "").strip().lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    raw_system = entry.get("system_project")
    if isinstance(raw_system, bool):
        return raw_system
    system_token = str(raw_system or "").strip().lower()
    return system_token in {"1", "true", "yes", "on"}


def project_runtime_issue(entry: Dict[str, Any]) -> str:
    if not isinstance(entry, dict):
        return "invalid_entry"

    team_dir_raw = str(entry.get("team_dir", "")).strip()
    if not team_dir_raw:
        return "missing_team_dir"

    try:
        team_dir = Path(team_dir_raw).expanduser()
    except Exception:
        return f"invalid_team_dir:{team_dir_raw}"

    if (not team_dir.exists()) or (not team_dir.is_dir()):
        return f"missing_team_dir:{team_dir}"

    cfg = team_dir / "orchestrator.json"
    if not cfg.exists():
        return f"missing_orchestrator:{cfg}"

    return ""


def project_runtime_ready(entry: Dict[str, Any]) -> bool:
    return not bool(project_runtime_issue(entry))


def project_runtime_label(entry: Dict[str, Any]) -> str:
    issue = project_runtime_issue(entry)
    if not issue:
        return "ready"
    if issue == "invalid_entry":
        return "invalid entry"
    if issue == "missing_team_dir":
        return "missing team_dir"
    if issue.startswith("invalid_team_dir:"):
        return f"invalid team_dir ({issue.split(':', 1)[1]})"
    if issue.startswith("missing_team_dir:"):
        return f"missing team_dir ({issue.split(':', 1)[1]})"
    if issue.startswith("missing_orchestrator:"):
        return f"missing orchestrator.json ({issue.split(':', 1)[1]})"
    return issue
