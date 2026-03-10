#!/usr/bin/env python3
"""Auto-discover Orch projects from Agent of Empires (aoe) sessions.

Goal
- When the user already uses `aoe` (Agent of Empires) to manage local sessions,
  the Telegram Mother-Orch should not require manual `/orch add ...` for every
  project. We can derive project roots from `aoe list --json --all`.

Heuristics
- Only consider session paths that are within a configured workspace root.
- Project root is the first directory segment under the workspace root.
  Example:
    workspace_root=/home/me/Workspace
    session_path=/home/me/Workspace/1.2.8.TwinPaper/modules/02_golden_set
    -> project_root=/home/me/Workspace/1.2.8.TwinPaper
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

_VERSION_PREFIX_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+\.")
_SLUG_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def strip_version_prefix(name: str) -> str:
    token = str(name or "").strip()
    if not token:
        return ""
    stripped = _VERSION_PREFIX_RE.sub("", token).strip()
    return stripped or token


def slugify_project_key(name: str) -> str:
    """Return a stable, safe project key token."""

    base = strip_version_prefix(name)
    token = str(base or "").strip().lower()
    token = _SLUG_NON_ALNUM_RE.sub("_", token)
    token = re.sub(r"_+", "_", token).strip("_")
    return token or "project"


def unique_project_key(base: str, existing: set[str]) -> str:
    token = slugify_project_key(base)
    if token not in existing:
        return token
    for i in range(2, 1000):
        cand = f"{token}_{i}"
        if cand not in existing:
            return cand
    return f"{token}_999"


def load_aoe_sessions(*, aoe_bin: str = "aoe", timeout_sec: int = 6) -> List[Dict[str, Any]]:
    """Best-effort: return [] on any error."""

    try:
        proc = subprocess.run(
            [aoe_bin, "list", "--json", "--all"],
            capture_output=True,
            text=True,
            timeout=max(1, int(timeout_sec)),
        )
    except FileNotFoundError:
        return []
    except subprocess.TimeoutExpired:
        return []
    except Exception:
        return []

    if proc.returncode != 0:
        return []

    raw = (proc.stdout or "").strip()
    if not raw:
        return []

    try:
        data = json.loads(raw)
    except Exception:
        return []
    if not isinstance(data, list):
        return []

    rows: List[Dict[str, Any]] = []
    for item in data:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def derive_workspace_project_root(*, session_path: Path, workspace_root: Path) -> Optional[Path]:
    """Map a session working directory to a workspace-level project root."""

    try:
        rel = session_path.resolve().relative_to(workspace_root.resolve())
    except Exception:
        return None

    if not rel.parts:
        return None

    root = (workspace_root / rel.parts[0]).expanduser().resolve()
    if root.exists() and root.is_dir():
        return root
    return None


def discover_workspace_projects_from_aoe(
    *,
    workspace_root: Path,
    aoe_bin: str = "aoe",
    timeout_sec: int = 6,
) -> Dict[Path, Dict[str, Any]]:
    """Return map: project_root -> {display_name, groups, sessions}."""

    ws = Path(workspace_root).expanduser().resolve()
    if not ws.exists() or not ws.is_dir():
        return {}

    rows = load_aoe_sessions(aoe_bin=aoe_bin, timeout_sec=timeout_sec)
    buckets: Dict[Path, Dict[str, Any]] = {}

    for row in rows:
        raw_path = str(row.get("path", "") or "").strip()
        if not raw_path:
            continue
        sess_path = Path(raw_path).expanduser()
        if not sess_path.exists():
            continue
        if not sess_path.is_dir():
            sess_path = sess_path.parent
        sess_path = sess_path.resolve()

        project_root = derive_workspace_project_root(session_path=sess_path, workspace_root=ws)
        if not project_root:
            continue

        bucket = buckets.setdefault(project_root, {"groups": set(), "sessions": 0})
        group = str(row.get("group", "") or "").strip()
        if group:
            bucket["groups"].add(group)
        bucket["sessions"] = int(bucket.get("sessions", 0) or 0) + 1

    out: Dict[Path, Dict[str, Any]] = {}
    for root, meta in buckets.items():
        groups = sorted(str(x) for x in (meta.get("groups") or set()) if str(x).strip())
        display = strip_version_prefix(root.name) or root.name
        out[root] = {
            "display_name": display,
            "groups": groups,
            "sessions": int(meta.get("sessions", 0) or 0),
        }
    return out


def seed_team_dir_from_template(*, team_dir: Path, template_path: Path) -> None:
    """Create <team_dir> and seed AOE_TODO.md if missing.

    This is intentionally best-effort: callers may run it against many projects.
    """

    td = Path(team_dir).expanduser().resolve()
    td.mkdir(parents=True, exist_ok=True)
    (td / "logs").mkdir(parents=True, exist_ok=True)

    todo_path = td / "AOE_TODO.md"
    if todo_path.exists():
        return

    content = ""
    tp = Path(template_path).expanduser().resolve()
    if tp.exists() and tp.is_file():
        try:
            content = tp.read_text(encoding="utf-8")
        except Exception:
            content = ""

    if not content.strip():
        content = "# AOE_TODO\n\n## Tasks\n\n- [ ] P2: (fill me)\n"

    try:
        todo_path.write_text(content, encoding="utf-8")
    except Exception:
        # Ignore write failures (permissions, RO mounts, etc.)
        return
