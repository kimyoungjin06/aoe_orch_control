"""Project registry: the single source of truth for multi-project routing.

The registry file (default ``~/.config/forager/projects.toml``) maps each
managed project to its workspace path patterns, forager session group, and
wiki knowledge plane. Fan-out (routing operator input to a project) and
fan-in (aggregating status across projects) both resolve through it.
"""

from __future__ import annotations

import concurrent.futures
import datetime
import json
import os
import pathlib
import re
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
            "aliases": [
                str(item).strip()
                for item in (entry.get("aliases") or [])
                if str(item).strip()
            ],
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
            "aliases": list(entry.get("aliases") or []),
            "wiki_profile": entry.get("wiki_profile"),
        }
        for entry in registry.values()
    ]


def _project_aliases(entry: dict[str, Any]) -> set[str]:
    aliases = {
        str(entry.get("key") or "").strip().lower(),
        str(entry.get("display_name") or "").strip().lower(),
    }
    aliases.update(str(alias).strip().lower() for alias in entry.get("aliases") or [])
    aliases.update(
        str(pattern).rstrip("/").rsplit("/", 1)[-1].strip().lower()
        for pattern in entry.get("workspace_patterns") or []
    )
    return {alias for alias in aliases if alias}


def _alias_is_mentioned(text: str, alias: str) -> bool:
    if alias.isascii():
        return bool(
            re.search(
                rf"(?<![a-z0-9_]){re.escape(alias)}(?![a-z0-9_])",
                text,
                flags=re.IGNORECASE,
            )
        )
    return alias in text


def project_mention_matches(
    text: str, registry: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    """Return all projects tied for the most-specific mention."""

    normalized = str(text or "").lower()
    if not normalized:
        return []
    matched: list[tuple[int, dict[str, Any]]] = []
    for entry in registry.values():
        lengths = [
            len(alias)
            for alias in _project_aliases(entry)
            if len(alias) >= 3 and _alias_is_mentioned(normalized, alias)
        ]
        if lengths:
            matched.append((max(lengths), entry))
    if not matched:
        return []
    best_length = max(length for length, _entry in matched)
    return [entry for length, entry in matched if length == best_length]


def resolve_project_mention(
    text: str, registry: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    """Match a chat message to a registered project by key, display name, or folder name.

    Case-insensitive substring match; the longest alias wins so a message that
    contains both a short and a long project name resolves to the more
    specific one. Aliases shorter than 3 characters are ignored to avoid
    accidental hits inside ordinary words.
    """

    matches = project_mention_matches(text, registry)
    return matches[0] if len(matches) == 1 else None


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

    mentions = project_mention_matches(text, registry)
    if len(mentions) == 1:
        return mentions[0], "mention"
    if mentions:
        return None, "ambiguous"
    sticky = registry.get(str(sticky_key or ""))
    if isinstance(sticky, dict):
        return sticky, "sticky"
    return None, None


def resolve_project_selector(
    selector: str, registry: dict[str, dict[str, Any]]
) -> dict[str, Any] | None:
    """Resolve an explicit project selector without substring guessing."""

    matches = project_selector_matches(selector, registry)
    return matches[0] if len(matches) == 1 else None


def project_selector_matches(
    selector: str, registry: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    """Return every project matching an exact selector so ties stay visible."""

    wanted = str(selector or "").strip().lower()
    if not wanted:
        return []
    matches = []
    for entry in registry.values():
        if wanted in _project_aliases(entry):
            matches.append(entry)
    return matches


def build_wiki_plane_summary(
    forager_bin: str,
    entry: dict[str, Any],
    *,
    include_entries: bool,
    timeout_sec: int = 15,
) -> dict[str, Any]:
    """Read one registered project's candidate queue and promoted knowledge."""

    wiki_profile = str(entry.get("wiki_profile") or "").strip()
    summary: dict[str, Any] = {
        "project_key": entry.get("key"),
        "display_name": entry.get("display_name"),
        "wiki_profile": wiki_profile or None,
    }
    if not wiki_profile:
        summary["status"] = "not_configured"
        return summary
    try:
        candidates = _forager_json(
            forager_bin,
            ["--profile", wiki_profile, "offdesk", "wiki", "candidates", "--json"],
            timeout_sec=timeout_sec,
            expect="list",
        )
        recent_candidates = sorted(
            (item for item in candidates if isinstance(item, dict)),
            key=lambda item: str(item.get("last_seen_at") or item.get("updated_at") or ""),
            reverse=True,
        )[:3]
        summary.update(
            {
                "status": "ok",
                "candidate_count": len(candidates),
                "recent_candidates": [
                    {
                        "id": item.get("id"),
                        "kind": item.get("kind"),
                        "claim": str(item.get("claim") or "")[:160],
                    }
                    for item in recent_candidates
                ],
            }
        )
        if include_entries:
            entries = _forager_json(
                forager_bin,
                ["--profile", wiki_profile, "offdesk", "wiki", "entries", "--json"],
                timeout_sec=timeout_sec,
                expect="list",
            )
            promoted = [
                item
                for item in entries
                if isinstance(item, dict) and item.get("status") == "promoted"
            ]
            summary["entry_count"] = len(entries)
            summary["promoted_count"] = len(promoted)
            recent_entries = sorted(
                promoted,
                key=lambda item: str(item.get("updated_at") or item.get("created_at") or ""),
                reverse=True,
            )[:3]
            summary["recent_claims"] = [
                str(item.get("claim") or "")[:160] for item in recent_entries
            ]
    except (OSError, ValueError, subprocess.TimeoutExpired):
        summary["status"] = "unavailable"
    return summary


def build_wiki_portfolio_summary(
    forager_bin: str,
    registry: dict[str, dict[str, Any]],
    *,
    timeout_sec: int = 4,
) -> dict[str, Any]:
    """Aggregate candidate pressure across registered wiki planes."""

    entries: list[dict[str, Any]] = []
    seen_profiles: set[str] = set()
    for entry in registry.values():
        wiki_profile = str(entry.get("wiki_profile") or "").strip()
        if not wiki_profile or wiki_profile in seen_profiles:
            continue
        seen_profiles.add(wiki_profile)
        entries.append(entry)
    rows: list[dict[str, Any]] = []
    if entries:
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(6, len(entries))) as pool:
            rows = list(
                pool.map(
                    lambda entry: build_wiki_plane_summary(
                        forager_bin,
                        entry,
                        include_entries=False,
                        timeout_sec=timeout_sec,
                    ),
                    entries,
                )
            )
    rows.sort(
        key=lambda row: (
            -int(row.get("candidate_count") or 0),
            str(row.get("display_name") or row.get("project_key") or "").lower(),
        )
    )
    available = [row for row in rows if row.get("status") == "ok"]
    return {
        "schema": "telegram_wiki_portfolio.v1",
        "candidate_count": sum(int(row.get("candidate_count") or 0) for row in available),
        "projects_with_candidates": sum(
            1 for row in available if int(row.get("candidate_count") or 0) > 0
        ),
        "unavailable_projects": sum(1 for row in rows if row.get("status") == "unavailable"),
        "projects": rows,
    }


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
        focus["wiki"] = build_wiki_plane_summary(
            forager_bin,
            entry,
            include_entries=True,
            timeout_sec=timeout_sec,
        )
    return focus


def build_project_portfolio_summary(
    forager_bin: str,
    profile: str,
    registry: dict[str, dict[str, Any]],
    roots: list[pathlib.Path],
    *,
    timeout_sec: int = 10,
) -> dict[str, Any]:
    """Return the registered project list with real path and session readiness."""

    session_counts: dict[str, dict[str, int]] = {}
    try:
        status = _forager_json(
            forager_bin,
            ["--profile", profile, "status", "--json"]
            if profile
            else ["status", "--json"],
            timeout_sec=timeout_sec,
        )
        for session in status.get("sessions") or []:
            if not isinstance(session, dict):
                continue
            key = str(session.get("project") or "")
            state = str(session.get("status") or "unknown")
            counts = session_counts.setdefault(key, {})
            counts[state] = counts.get(state, 0) + 1
    except (OSError, ValueError, subprocess.TimeoutExpired):
        status = {}
    rows: list[dict[str, Any]] = []
    for entry in registry.values():
        key = str(entry.get("key") or "")
        counts = session_counts.get(key, {})
        rows.append(
            {
                "key": key,
                "display_name": entry.get("display_name"),
                "aliases": list(entry.get("aliases") or []),
                "workspace_status": "available"
                if find_project_dir(entry, roots) is not None
                else "missing",
                "session_counts": counts,
                "active_sessions": sum(
                    int(counts.get(state) or 0) for state in ("running", "waiting", "idle")
                ),
            }
        )
    rows.sort(
        key=lambda row: (
            -int(row.get("active_sessions") or 0),
            0 if row.get("workspace_status") == "available" else 1,
            str(row.get("display_name") or row.get("key") or "").lower(),
        )
    )
    return {
        "schema": "telegram_project_portfolio.v1",
        "registered_count": len(rows),
        "available_count": sum(1 for row in rows if row["workspace_status"] == "available"),
        "active_count": sum(1 for row in rows if int(row["active_sessions"]) > 0),
        "status_available": bool(status),
        "projects": rows,
    }


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
            children = [
                child
                for child in root.iterdir()
                if child.is_dir() and not child.name.startswith(".")
            ]
        except OSError:
            continue
        for child in children:
            if any(pattern in str(child) for pattern in patterns):
                return child
        # Some managed projects live under a collection directory such as
        # Workspace/0.1.NIMS/EpiMS. Search exactly one additional level so a
        # short registry pattern can resolve that layout without an unbounded
        # recursive walk across every worktree.
        for child in children[:80]:
            try:
                grandchildren = [
                    item
                    for item in child.iterdir()
                    if item.is_dir() and not item.name.startswith(".")
                ]
            except OSError:
                continue
            for grandchild in grandchildren[:120]:
                if any(
                    pattern.lower() in str(grandchild.relative_to(root)).lower()
                    for pattern in patterns
                ):
                    return grandchild
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


def _resolve_inside(project_dir: pathlib.Path, rel: str) -> pathlib.Path | None:
    """Resolve a relative path and refuse anything that escapes the project."""

    rel = str(rel or "").strip().lstrip("/")
    try:
        base = project_dir.resolve()
        candidate = (project_dir / rel).resolve() if rel else base
    except OSError:
        return None
    if candidate != base and base not in candidate.parents:
        return None
    return candidate


def list_project_dir(
    project_dir: pathlib.Path, rel: str, *, max_entries: int = 40
) -> dict[str, Any]:
    """Read-only chat tool: entries of one directory inside the project."""

    target = _resolve_inside(project_dir, rel)
    if target is None or not target.is_dir():
        return {"status": "not_a_directory", "path": rel}
    try:
        children = sorted(target.iterdir(), key=lambda c: (c.is_file(), c.name.lower()))
    except OSError:
        return {"status": "unreadable", "path": rel}
    entries: list[dict[str, Any]] = []
    truncated = False
    for child in children:
        if child.name.startswith(".") or child.name in SKIP_DIR_NAMES:
            continue
        if len(entries) >= max_entries:
            truncated = True
            break
        entries.append({"name": child.name, "kind": "dir" if child.is_dir() else "file"})
    return {"status": "ok", "path": rel, "entries": entries, "truncated": truncated}


def read_project_file(
    project_dir: pathlib.Path, rel: str, *, max_chars: int = 4000
) -> dict[str, Any]:
    """Read-only chat tool: bounded head of one text file inside the project.

    Content is handed only to the local chat model as grounding; it is not
    echoed into adapter results or Telegram messages.
    """

    target = _resolve_inside(project_dir, rel)
    if target is None or not target.is_file():
        return {"status": "not_a_file", "path": rel}
    try:
        with target.open("rb") as handle:
            raw = handle.read(max_chars * 4 + 1)
    except OSError:
        return {"status": "unreadable", "path": rel}
    if b"\x00" in raw[:1024]:
        return {"status": "binary_file", "path": rel}
    text = raw.decode("utf-8", errors="replace")
    truncated = len(raw) > max_chars * 4 or len(text) > max_chars
    return {"status": "ok", "path": rel, "content": text[:max_chars], "truncated": truncated}


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
