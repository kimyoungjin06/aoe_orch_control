#!/usr/bin/env python3
"""Runtime seeding helpers for orch projects.

This module exists because `aoe-orch init` may refuse to touch an existing
project root when a user-managed `AGENTS.md` is already present. In that case
we still want to materialize `.aoe-team/orchestrator.json` and companion
runtime files without overwriting the project root.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List

from aoe_tg_role_aliases import canonicalize_role_name, resolve_role_asset, resolve_role_file


DEFAULT_REPAIR_AGENTS = [
    "DataEngineer:codex",
    "Codex-Reviewer:codex",
    "Claude-Reviewer:claude",
    "Codex-Dev:codex",
    "Codex-Writer:codex",
    "Claude-Writer:claude",
    "Codex-Analyst:codex",
    "Claude-Analyst:claude",
]


def _parse_first_json_object(text: str) -> Dict[str, Any]:
    raw = str(text or "")
    start = raw.find("{")
    end = raw.rfind("}")
    if start < 0 or end < start:
        raise RuntimeError(f"failed to parse aoe-orch dry-run output: {raw[:1200]}")
    payload = json.loads(raw[start : end + 1])
    if not isinstance(payload, dict):
        raise RuntimeError("unexpected aoe-orch dry-run payload")
    return payload


def preview_orchestrator_spec(
    *,
    aoe_orch_bin: str,
    project_root: Path,
    team_dir: Path,
    overview: str,
    timeout_sec: int,
) -> Dict[str, Any]:
    cmd = [
        aoe_orch_bin,
        "init",
        "--dry-run",
        "--project-root",
        str(project_root),
        "--team-dir",
        str(team_dir),
        "--overview",
        overview,
        "--coordinator",
        "Orchestrator",
        "--coordinator-provider",
        "codex",
        "--default-provider",
        "codex",
        "--agents",
        ",".join(DEFAULT_REPAIR_AGENTS),
    ]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=max(60, int(timeout_sec)),
        check=False,
    )
    text = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        raise RuntimeError(f"aoe-orch init --dry-run failed: {text[:1200]}")
    return _parse_first_json_object(text)


def _team_manifest_from_spec(spec: Dict[str, Any], *, project_root: Path, overview: str) -> Dict[str, Any]:
    coordinator = canonicalize_role_name((spec.get("coordinator") or {}).get("role", "Orchestrator")) or "Orchestrator"
    roles: List[str] = [coordinator]
    for row in spec.get("agents") or []:
        if not isinstance(row, dict):
            continue
        role = canonicalize_role_name(row.get("role", ""))
        if role and role not in roles:
            roles.append(role)
    return {
        "version": 1,
        "project_root": str(project_root),
        "project_name": project_root.name,
        "coordinator": coordinator,
        "roles": roles,
        "created_at": str(spec.get("created_at", "")),
        "overview": overview,
    }


def _write_json_if_needed(path: Path, data: Dict[str, Any], *, force: bool) -> str:
    if path.exists() and not force:
        return f"[SKIP] {path} (exists)"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return f"[COPY] {path}"


def _copy_file_if_needed(src: Path, dst: Path, *, force: bool) -> str:
    if not src.exists():
        return f"[SKIP] {dst} (template missing)"
    if dst.exists() and not force:
        return f"[SKIP] {dst} (exists)"
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return f"[COPY] {dst}"


def seed_runtime_from_spec(
    *,
    template_root: Path,
    project_root: Path,
    team_dir: Path,
    overview: str,
    spec: Dict[str, Any],
    force: bool = False,
) -> List[str]:
    logs: List[str] = []
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "logs").mkdir(parents=True, exist_ok=True)

    spec = dict(spec)
    coordinator = spec.get("coordinator")
    if isinstance(coordinator, dict):
        coordinator = dict(coordinator)
        coordinator["role"] = canonicalize_role_name(coordinator.get("role", "Orchestrator")) or "Orchestrator"
        spec["coordinator"] = coordinator
    normalized_agents: List[Dict[str, Any]] = []
    for row in spec.get("agents") or []:
        if not isinstance(row, dict):
            continue
        item = dict(row)
        item["role"] = canonicalize_role_name(item.get("role", "")) or str(item.get("role", "")).strip()
        normalized_agents.append(item)
    if normalized_agents:
        spec["agents"] = normalized_agents
    spec["project_root"] = str(project_root)
    spec["team_dir"] = str(team_dir)
    spec["overview"] = overview
    logs.append(_write_json_if_needed(team_dir / "orchestrator.json", spec, force=force))
    logs.append(_write_json_if_needed(team_dir / "team.json", _team_manifest_from_spec(spec, project_root=project_root, overview=overview), force=force))

    for name in ["AOE_TODO.md", "telegram.env.sample", "sync_policy.sample.json"]:
        logs.append(_copy_file_if_needed(template_root / name, team_dir / name, force=force))

    coordinator = canonicalize_role_name((spec.get("coordinator") or {}).get("role", "Orchestrator")) or "Orchestrator"
    roles = [coordinator]
    for row in spec.get("agents") or []:
        if not isinstance(row, dict):
            continue
        role = canonicalize_role_name(row.get("role", ""))
        if role and role not in roles:
            roles.append(role)

    for role in roles:
        agent_tpl = resolve_role_asset(template_root / "agents", role, "AGENTS.md")
        worker_tpl = resolve_role_file(template_root / "workers", role, ".json")
        logs.append(_copy_file_if_needed(agent_tpl, team_dir / "agents" / (canonicalize_role_name(role) or role) / "AGENTS.md", force=force))
        logs.append(_copy_file_if_needed(worker_tpl, team_dir / "workers" / f"{canonicalize_role_name(role) or role}.json", force=force))

    return [row for row in logs if row]


def repair_runtime(
    *,
    aoe_orch_bin: str,
    template_root: Path,
    project_root: Path,
    team_dir: Path,
    overview: str,
    timeout_sec: int,
    force: bool = False,
) -> List[str]:
    spec_path = team_dir / "orchestrator.json"
    spec: Dict[str, Any]
    if spec_path.exists():
        spec = json.loads(spec_path.read_text(encoding="utf-8"))
        if not isinstance(spec, dict):
            raise RuntimeError(f"invalid orchestrator config: {spec_path}")
    else:
        spec = preview_orchestrator_spec(
            aoe_orch_bin=aoe_orch_bin,
            project_root=project_root,
            team_dir=team_dir,
            overview=overview,
            timeout_sec=timeout_sec,
        )
    return seed_runtime_from_spec(
        template_root=template_root,
        project_root=project_root,
        team_dir=team_dir,
        overview=overview,
        spec=spec,
        force=force,
    )
