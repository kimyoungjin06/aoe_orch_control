"""Project registry: the single source of truth for multi-project routing.

The registry file (default ``~/.config/forager/projects.toml``) maps each
managed project to its workspace path patterns, forager session group, and
wiki knowledge plane. Fan-out (routing operator input to a project) and
fan-in (aggregating status across projects) both resolve through it.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import tomllib
from typing import Any

PROJECT_REGISTRY_SCHEMA = "forager_project_registry.v1"


def default_registry_path() -> pathlib.Path:
    cfg = pathlib.Path(os.environ.get("XDG_CONFIG_HOME", pathlib.Path.home() / ".config"))
    return pathlib.Path(
        os.environ.get("FORAGER_PROJECT_REGISTRY", str(cfg / "forager" / "projects.toml"))
    )


def load_registry(path: pathlib.Path | None = None) -> dict[str, dict[str, Any]]:
    """Return {project_key: entry} or {} when the registry is absent/invalid."""

    registry_path = path or default_registry_path()
    try:
        raw = tomllib.loads(registry_path.read_text(encoding="utf-8"))
    except (OSError, ValueError, tomllib.TOMLDecodeError):
        return {}
    if raw.get("schema") != PROJECT_REGISTRY_SCHEMA:
        return {}
    projects = raw.get("projects")
    if not isinstance(projects, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for key, entry in projects.items():
        if not isinstance(entry, dict):
            continue
        patterns = [
            str(item).strip()
            for item in (entry.get("workspace_patterns") or [])
            if str(item).strip()
        ]
        normalized[str(key)] = {
            "key": str(key),
            "display_name": str(entry.get("display_name") or key),
            "workspace_patterns": patterns,
            "session_group": str(entry.get("session_group") or "").strip() or None,
            "wiki_profile": str(entry.get("wiki_profile") or "").strip() or None,
        }
    return normalized


def resolve_project_for_path(
    path: str, registry: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    """Match a filesystem path to a project via substring workspace patterns."""

    text = str(path or "")
    if not text:
        return None
    for entry in registry.values():
        for pattern in entry.get("workspace_patterns") or []:
            if pattern and pattern in text:
                return entry
    return None


def registry_summary(registry: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    """Compact projection of the registry for prompts and cards."""

    return [
        {
            "key": entry["key"],
            "display_name": entry["display_name"],
            "wiki_profile": entry.get("wiki_profile"),
        }
        for entry in registry.values()
    ]


def resolve_project_mention(
    text: str, registry: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    """Match a chat message to a registered project by key, display name, or folder name.

    Case-insensitive substring match; the longest alias wins so a message that
    contains both a short and a long project name resolves to the more
    specific one. Aliases shorter than 3 characters are ignored to avoid
    accidental hits inside ordinary words.
    """

    normalized = str(text or "").lower()
    if not normalized:
        return None
    best: tuple[int, dict[str, Any]] | None = None
    for entry in registry.values():
        aliases = {
            str(entry.get("key") or "").lower(),
            str(entry.get("display_name") or "").lower(),
        }
        for pattern in entry.get("workspace_patterns") or []:
            aliases.add(str(pattern).rsplit("/", 1)[-1].lower())
        for alias in aliases:
            if len(alias) >= 3 and alias in normalized and (best is None or len(alias) > best[0]):
                best = (len(alias), entry)
    return best[1] if best else None


def resolve_chat_focus(
    text: str,
    registry: dict[str, dict[str, Any]],
    sticky_key: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Resolve which project a chat message is about.

    An explicit mention in this message wins; otherwise the chat's sticky
    focus (the last mentioned project) carries over, so follow-up questions
    that drop the project name keep their subject.
    """

    mention = resolve_project_mention(text, registry)
    if mention:
        return mention, "mention"
    sticky = registry.get(str(sticky_key or ""))
    if isinstance(sticky, dict):
        return sticky, "sticky"
    return None, None


def build_project_focus(
    forager_bin: str,
    profile: str,
    entry: dict[str, Any],
    source: str | None,
    *,
    timeout_sec: int = 15,
) -> dict[str, Any]:
    """Live read-only context for one registered project, for chat grounding.

    Every probe degrades to a status marker instead of raising: grounding
    must never break the chat reply path.
    """

    focus: dict[str, Any] = {
        "key": entry.get("key"),
        "display_name": entry.get("display_name"),
        "wiki_profile": entry.get("wiki_profile"),
        "workspace_patterns": entry.get("workspace_patterns"),
        "focus_source": source,
    }
    try:
        status = _forager_json(
            forager_bin,
            ["--profile", profile, "status", "--json"] if profile else ["status", "--json"],
            timeout_sec=timeout_sec,
        )
        rows = [
            row
            for row in (status.get("sessions") or [])
            if isinstance(row, dict) and row.get("project") == entry.get("key")
        ]
        focus["sessions"] = [
            {"title": row.get("title"), "tool": row.get("tool"), "status": row.get("status")}
            for row in rows[:8]
        ]
        counts: dict[str, int] = {}
        for row in rows:
            state = str(row.get("status") or "unknown")
            counts[state] = counts.get(state, 0) + 1
        focus["session_counts"] = counts
    except (OSError, ValueError, subprocess.TimeoutExpired):
        focus["sessions"] = []
        focus["sessions_status"] = "unavailable"
    wiki_profile = str(entry.get("wiki_profile") or "")
    if wiki_profile:
        try:
            entries = _forager_json(
                forager_bin,
                ["--profile", wiki_profile, "offdesk", "wiki", "entries", "--json"],
                timeout_sec=timeout_sec,
                expect="list",
            )
            recent = sorted(
                (item for item in entries if isinstance(item, dict)),
                key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""),
            )[-3:]
            focus["wiki"] = {
                "entry_count": len(entries),
                "recent_claims": [str(item.get("claim") or "")[:160] for item in reversed(recent)],
            }
        except (OSError, ValueError, subprocess.TimeoutExpired):
            focus["wiki"] = {"status": "unavailable"}
    return focus


def _forager_json(
    forager_bin: str,
    argv_tail: list[str],
    *,
    timeout_sec: int,
    expect: str = "dict",
) -> Any:
    process = subprocess.run(
        [str(forager_bin), *argv_tail],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        timeout=timeout_sec,
    )
    if process.returncode != 0:
        raise ValueError(f"forager {' '.join(argv_tail)} failed")
    parsed = json.loads(process.stdout)
    if expect == "dict" and not isinstance(parsed, dict):
        raise ValueError("expected a JSON object")
    if expect == "list" and not isinstance(parsed, list):
        raise ValueError("expected a JSON array")
    return parsed
