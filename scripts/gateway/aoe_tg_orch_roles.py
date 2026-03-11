#!/usr/bin/env python3
"""Orchestrator role/profile helpers extracted from the gateway monolith."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from aoe_tg_task_view import dedupe_roles


DEFAULT_WORKER_ROLE_POOL = [
    "DataEngineer",
    "Reviewer",
    "Local-Dev",
    "Local-Writer",
    "Local-Analyst",
]


def parse_roles_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    out: List[str] = []
    seen = set()
    for item in str(raw).split(","):
        token = item.strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def load_orchestrator_roles(team_dir: Path) -> List[str]:
    cfg = team_dir / "orchestrator.json"
    if not cfg.exists():
        return []

    try:
        data = json.loads(cfg.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(data, dict):
        return []

    roles: List[str] = []
    coordinator = data.get("coordinator")
    if isinstance(coordinator, dict):
        role = str(coordinator.get("role", "")).strip()
        if role:
            roles.append(role)

    agents = data.get("agents")
    if isinstance(agents, list):
        for row in agents:
            if isinstance(row, dict):
                role = str(row.get("role", "")).strip()
            else:
                role = str(row).strip()
            if role:
                roles.append(role)

    return dedupe_roles(roles)


def load_orchestrator_role_profiles(team_dir: Path, available_roles: Optional[List[str]] = None) -> List[Dict[str, str]]:
    roles = dedupe_roles(available_roles or load_orchestrator_roles(team_dir))
    profiles: List[Dict[str, str]] = []
    for role in roles:
        mission = ""
        agent_doc = team_dir / "agents" / role / "AGENTS.md"
        if agent_doc.exists():
            try:
                text = agent_doc.read_text(encoding="utf-8")
                match = re.search(r"^## Mission\s*\n(.+?)(?:\n## |\Z)", text, flags=re.MULTILINE | re.DOTALL)
                if match:
                    mission = " ".join(line.strip() for line in match.group(1).splitlines() if line.strip())
            except Exception:
                mission = ""
        profiles.append({"role": role, "role_key": role.lower(), "mission": mission.strip()})
    return profiles


def resolve_verifier_candidates(raw: Optional[str], *, default_verifier_roles: str) -> List[str]:
    parsed = parse_roles_csv(raw or default_verifier_roles)
    return parsed or parse_roles_csv(default_verifier_roles)


def ensure_verifier_roles(
    selected_roles: List[str],
    available_roles: List[str],
    verifier_candidates: List[str],
) -> Tuple[List[str], List[str], bool, List[str]]:
    selected = dedupe_roles(selected_roles)
    available = dedupe_roles(available_roles)

    candidate_keys = [c.lower() for c in verifier_candidates if c]
    selected_verifiers = [r for r in selected if r.lower() in candidate_keys]

    available_verifiers: List[str] = []
    for cand in verifier_candidates:
        ckey = cand.lower()
        for role in available:
            if role.lower() == ckey and role not in available_verifiers:
                available_verifiers.append(role)

    added = False
    if not selected_verifiers and available_verifiers:
        selected.append(available_verifiers[0])
        selected_verifiers = [available_verifiers[0]]
        added = True

    return dedupe_roles(selected), dedupe_roles(selected_verifiers), added, available_verifiers


def _role_name_aliases(role: str) -> List[str]:
    token = str(role or "").strip()
    if not token:
        return []
    aliases = {
        token.lower(),
        token.replace("-", " ").lower(),
        token.replace("_", " ").lower(),
        re.sub(r"(?<!^)([A-Z])", r" \1", token).strip().lower(),
    }
    norm = token.lower()
    if "review" in norm:
        aliases.update({"reviewer", "qa", "verifier", "critic"})
    if "data" in norm:
        aliases.update({"dataengineer", "data engineer"})
    if any(x in norm for x in {"dev", "engineer", "coder", "builder"}):
        aliases.update({"developer", "engineer", "builder", "implementer"})
    if any(x in norm for x in {"writer", "doc", "scribe"}):
        aliases.update({"writer", "doc writer"})
    if any(x in norm for x in {"anal", "analysis", "research", "tuner"}):
        aliases.update({"analyst", "analysis", "researcher", "tuner"})
    return [alias for alias in aliases if alias]


def _role_score_from_text(prompt_lower: str, role: str, mission: str) -> int:
    name_key = str(role or "").strip().lower()
    mission_lower = str(mission or "").strip().lower()
    score = 0

    review_keys = (
        "review", "risk", "regression", "test", "qa", "bug", "verify", "validation", "inspect", "check",
        "리뷰", "리스크", "회귀", "테스트", "버그", "검증", "점검", "확인", "검토",
    )
    data_keys = (
        "data", "dataset", "etl", "schema", "sql", "table", "column", "csv", "pipeline", "null", "quality",
        "데이터", "스키마", "결측", "컬럼", "테이블", "적재", "정합성", "품질",
    )
    build_keys = (
        "implement", "implementation", "build", "code", "fix", "patch", "refactor", "develop",
        "개발", "구현", "수정", "패치", "리팩토링", "코드",
    )
    doc_keys = (
        "document", "documentation", "docs", "summary", "report", "writeup", "guide", "readme", "tutorial", "handoff",
        "문서", "요약", "보고", "보고서", "가이드", "튜토리얼", "인수인계",
    )
    analysis_keys = ("analyze", "analysis", "research", "compare", "benchmark", "investigate", "분석", "조사", "비교", "벤치마크", "리서치")

    if any(k in prompt_lower for k in review_keys):
        if any(k in name_key for k in ("review", "qa", "verif")) or any(k in mission_lower for k in ("risk", "regression", "test", "review")):
            score += 6
    if any(k in prompt_lower for k in data_keys):
        if any(k in name_key for k in ("data", "etl", "sql", "schema")) or any(k in mission_lower for k in ("data", "etl", "schema", "quality")):
            score += 6
    if any(k in prompt_lower for k in build_keys):
        build_name_hit = any(k in name_key for k in ("dev", "coder", "builder")) or (("engineer" in name_key) and ("data" not in name_key))
        if build_name_hit or any(k in mission_lower for k in ("implement", "build", "code", "develop")):
            score += 5
    if any(k in prompt_lower for k in doc_keys):
        if any(k in name_key for k in ("writer", "doc", "scribe")) or any(k in mission_lower for k in ("document", "report", "summary")):
            score += 4
    if any(k in prompt_lower for k in analysis_keys):
        if any(k in name_key for k in ("anal", "research", "tuner")) or any(k in mission_lower for k in ("analysis", "research", "compare")):
            score += 4

    short_check = ("한 문장" in prompt_lower) or ("짧게" in prompt_lower) or ("3개만" in prompt_lower) or ("존재 여부" in prompt_lower)
    if short_check and ("review" in name_key or "verif" in name_key or "qa" in name_key):
        score += 2

    return score


def choose_auto_dispatch_roles(
    prompt: str,
    available_roles: Optional[List[str]] = None,
    team_dir: Optional[Path] = None,
) -> List[str]:
    prompt_text = str(prompt or "").strip()
    prompt_lower = prompt_text.lower()
    team_dir_path = Path(team_dir).expanduser().resolve() if team_dir else None

    profiles = load_orchestrator_role_profiles(team_dir_path, available_roles) if team_dir_path else [
        {"role": role, "role_key": str(role).lower(), "mission": ""} for role in dedupe_roles(available_roles or [])
    ]
    if not profiles:
        profiles = [
            {"role": role, "role_key": role.lower(), "mission": ""}
            for role in DEFAULT_WORKER_ROLE_POOL
        ]

    explicit: List[str] = []
    for profile in profiles:
        role = str(profile.get("role", "")).strip()
        if not role or role.lower() == "orchestrator":
            continue
        for alias in _role_name_aliases(role):
            if alias in prompt_lower:
                explicit.append(role)
                break
    explicit = dedupe_roles(explicit)
    if explicit:
        return explicit

    has_review_signal = any(token in prompt_lower for token in ("review", "risk", "regression", "test", "qa", "verify", "리뷰", "검토", "검증", "테스트", "리스크"))
    has_data_signal = any(token in prompt_lower for token in ("data", "dataset", "etl", "schema", "sql", "csv", "pipeline", "데이터", "스키마", "결측", "적재", "정합성"))
    has_build_signal = any(token in prompt_lower for token in ("implement", "build", "code", "fix", "patch", "refactor", "개발", "구현", "수정", "패치", "리팩토링", "코드"))
    has_doc_signal = any(token in prompt_lower for token in ("document", "documentation", "docs", "summary", "report", "readme", "guide", "tutorial", "문서", "요약", "보고", "보고서", "가이드", "튜토리얼"))
    has_analysis_signal = any(token in prompt_lower for token in ("analyze", "analysis", "research", "compare", "benchmark", "investigate", "분석", "조사", "비교", "벤치마크", "리서치"))

    wants_multi = any(
        token in prompt_lower
        for token in ("각각", "둘 다", "둘다", "together", "cross-check", "교차", "병렬", "분리", "tf", "team", "sub-task", "subtask", "역할별")
    )
    category_hits = sum(1 for flag in (has_review_signal, has_data_signal, has_build_signal, has_doc_signal, has_analysis_signal) if flag)
    if category_hits >= 2:
        wants_multi = True

    scored: List[Tuple[int, str]] = []
    for profile in profiles:
        role = str(profile.get("role", "")).strip()
        if not role or role.lower() == "orchestrator":
            continue
        score = _role_score_from_text(prompt_lower, role, str(profile.get("mission", "")))
        if score > 0:
            scored.append((score, role))

    if not scored:
        data_keys = ("data", "dataset", "etl", "schema", "sql", "pipeline", "품질", "데이터", "스키마", "적재", "정합성", "검증")
        review_keys = ("review", "risk", "regression", "test", "qa", "bug", "리뷰", "리스크", "회귀", "테스트", "버그", "검토")
        build_keys = ("implement", "build", "code", "fix", "patch", "refactor", "개발", "구현", "수정", "패치", "리팩토링", "코드")
        doc_keys = ("document", "documentation", "docs", "summary", "report", "guide", "readme", "tutorial", "문서", "요약", "보고", "보고서", "가이드", "튜토리얼")
        analysis_keys = ("analyze", "analysis", "research", "compare", "benchmark", "investigate", "분석", "조사", "비교", "벤치마크", "리서치")
        both_keys = ("both", "둘 다", "둘다", "각각", "cross-check", "교차")
        roles: List[str] = []
        if any(k in prompt_lower for k in data_keys):
            roles.append("DataEngineer")
        if any(k in prompt_lower for k in review_keys):
            roles.append("Reviewer")
        if any(k in prompt_lower for k in build_keys):
            roles.append("Local-Dev")
        if any(k in prompt_lower for k in doc_keys):
            roles.append("Local-Writer")
        if any(k in prompt_lower for k in analysis_keys):
            roles.append("Local-Analyst")
        if not roles and any(k in prompt_lower for k in both_keys):
            roles = ["DataEngineer", "Reviewer"]
        return dedupe_roles([r for r in roles if r])

    scored.sort(key=lambda item: (-item[0], item[1].lower()))
    top_score = scored[0][0]
    selected = [scored[0][1]]
    if wants_multi:
        for score, role in scored[1:3]:
            if score >= max(3, top_score - 2):
                selected.append(role)
    return dedupe_roles(selected)


def available_worker_roles(
    available_roles: List[str],
    *,
    default_pool: Optional[List[str]] = None,
) -> List[str]:
    workers = [r for r in dedupe_roles(available_roles) if r.lower() != "orchestrator"]
    return workers or dedupe_roles(default_pool or DEFAULT_WORKER_ROLE_POOL)
