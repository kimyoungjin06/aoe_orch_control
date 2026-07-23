"""Project registry: the single source of truth for multi-project routing.

The registry file (default ``~/.config/forager/projects.toml``) maps each
managed project to its workspace path patterns, forager session group, and
wiki knowledge plane. Fan-out (routing operator input to a project) and
fan-in (aggregating status across projects) both resolve through it.
"""

from __future__ import annotations

import datetime
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


def find_project_dir(
    entry: dict[str, Any], roots: list[pathlib.Path]
) -> pathlib.Path | None:
    """Locate the working tree of a registered project under the workspace roots."""

    patterns = [str(p) for p in (entry.get("workspace_patterns") or []) if str(p).strip()]
    if not patterns:
        return None
    for root in roots:
        for pattern in patterns:
            candidate = root / pattern
            if candidate.is_dir():
                return candidate
        try:
            children = [child for child in root.iterdir() if child.is_dir()]
        except OSError:
            continue
        for child in children:
            if any(pattern in str(child) for pattern in patterns):
                return child
    return None


KEY_DOC_NAMES = (
    "PROJECT_STATE.md",
    "README.md",
    "AGENTS.md",
    "CLAUDE.md",
    "RETURN_PACKAGE.md",
)

SKIP_DIR_NAMES = {
    ".git",
    "__pycache__",
    "node_modules",
    "target",
    ".venv",
    "venv",
    ".cache",
}


def inspect_project_workspace(
    path: pathlib.Path, *, timeout_sec: int = 10
) -> dict[str, Any]:
    """Bounded read-only probe of a project working tree for chat grounding.

    This is the tool behind the chat agent's 'inspect_project' decision. It
    must stay read-only and every probe degrades to a marker instead of
    raising.
    """

    report: dict[str, Any] = {"path": str(path)}

    def git(*argv: str) -> str:
        proc = subprocess.run(
            ["git", "-C", str(path), *argv],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=timeout_sec,
        )
        if proc.returncode != 0:
            raise ValueError(f"git {' '.join(argv)} failed")
        return proc.stdout

    try:
        report["git_branch"] = git("rev-parse", "--abbrev-ref", "HEAD").strip()
        dirty = [line for line in git("status", "--short").splitlines() if line.strip()]
        report["git_dirty_count"] = len(dirty)
        report["git_dirty_sample"] = dirty[:10]
        report["git_recent_commits"] = git(
            "log", "-5", "--format=%ad %s", "--date=short"
        ).splitlines()
    except (OSError, ValueError, subprocess.TimeoutExpired):
        report["git_status"] = "unavailable"

    def mtime_iso(stat: os.stat_result) -> str:
        return (
            datetime.datetime.fromtimestamp(stat.st_mtime, tz=datetime.timezone.utc)
            .strftime("%Y-%m-%dT%H:%M:%SZ")
        )

    key_docs: list[dict[str, Any]] = []
    for name in KEY_DOC_NAMES:
        doc = path / name
        try:
            if doc.is_file():
                stat = doc.stat()
                key_docs.append(
                    {"name": name, "modified_at": mtime_iso(stat), "size_bytes": stat.st_size}
                )
        except OSError:
            continue
    report["key_docs"] = key_docs

    # Newest files, two levels deep, so "what changed recently" is answerable
    # without an unbounded tree walk.
    newest: list[tuple[float, str, os.stat_result]] = []
    examined = 0
    try:
        for child in path.iterdir():
            if examined > 2000:
                break
            if child.name.startswith(".") or child.name in SKIP_DIR_NAMES:
                continue
            entries = [child]
            if child.is_dir():
                try:
                    entries = list(child.iterdir())
                except OSError:
                    continue
            for item in entries:
                examined += 1
                if examined > 2000:
                    break
                if item.name.startswith(".") or item.name in SKIP_DIR_NAMES:
                    continue
                try:
                    if item.is_file():
                        stat = item.stat()
                        newest.append((stat.st_mtime, str(item.relative_to(path)), stat))
                except OSError:
                    continue
    except OSError:
        pass
    newest.sort(reverse=True)
    report["recently_modified"] = [
        {"file": rel, "modified_at": mtime_iso(stat)} for _, rel, stat in newest[:5]
    ]
    return report


def inspection_summary_lines(inspection: dict[str, Any]) -> list[str]:
    """Deterministic short summary of an inspection, for the no-agent fallback."""

    lines: list[str] = []
    branch = inspection.get("git_branch")
    if branch:
        dirty = int(inspection.get("git_dirty_count") or 0)
        lines.append(f"git: {branch} · 변경 {dirty}건")
        commits = inspection.get("git_recent_commits") or []
        if commits:
            lines.append(f"최근 커밋: {commits[0]}")
    else:
        lines.append("git 정보 없음")
    for item in (inspection.get("recently_modified") or [])[:2]:
        lines.append(f"최근 수정: {item.get('file')} ({item.get('modified_at')})")
    return lines


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
