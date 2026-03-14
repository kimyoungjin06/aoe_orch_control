#!/usr/bin/env python3
"""Orchestrator role/profile helpers extracted from the gateway monolith."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from aoe_tg_role_aliases import canonicalize_role_name, canonicalize_roles, resolve_role_asset
from aoe_tg_task_view import dedupe_roles


DEFAULT_WORKER_ROLE_POOL = [
    "DataEngineer",
    "Codex-Reviewer",
    "Claude-Reviewer",
    "Codex-Dev",
    "Codex-Writer",
    "Claude-Writer",
    "Codex-Analyst",
    "Claude-Analyst",
]

CLAUDE_COMPANION_ROLES = {
    "Codex-Reviewer": "Claude-Reviewer",
    "Codex-Writer": "Claude-Writer",
    "Codex-Analyst": "Claude-Analyst",
}

REVIEW_SIGNAL_KEYS = (
    "review", "risk", "regression", "test", "qa", "bug", "verify", "validation", "inspect", "check",
    "리뷰", "리스크", "회귀", "테스트", "버그", "검증", "점검", "확인", "검토",
)
DATA_SIGNAL_KEYS = (
    "data", "dataset", "etl", "schema", "sql", "table", "column", "csv", "pipeline", "null", "quality",
    "데이터", "스키마", "결측", "컬럼", "테이블", "적재", "정합성", "품질",
)
BUILD_SIGNAL_KEYS = (
    "implement", "implementation", "build", "code", "fix", "patch", "refactor", "develop",
    "개발", "구현", "수정", "패치", "리팩토링", "코드",
)
DOC_SIGNAL_KEYS = (
    "document", "documentation", "docs", "summary", "report", "writeup", "guide", "readme", "tutorial", "handoff",
    "문서", "요약", "보고", "보고서", "가이드", "튜토리얼", "인수인계", "작성",
)
ANALYSIS_SIGNAL_KEYS = (
    "analyze", "analysis", "research", "compare", "benchmark", "investigate",
    "분석", "조사", "비교", "벤치마크", "리서치", "추천안", "트레이드오프",
)
MULTI_SIGNAL_KEYS = (
    "각각", "둘 다", "둘다", "together", "cross-check", "교차", "병렬", "분리",
    "tf", "team", "sub-task", "subtask", "역할별",
)
SINGLE_ROLE_ONLY_KEYS = (" only ", " solo ", " single ", "단독", "혼자", "하나만")

ROLE_ORDER = {
    "DataEngineer": 10,
    "Codex-Dev": 20,
    "Codex-Writer": 30,
    "Claude-Writer": 31,
    "Codex-Analyst": 40,
    "Claude-Analyst": 41,
    "Codex-Reviewer": 50,
    "Claude-Reviewer": 51,
}


def parse_roles_csv(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    out: List[str] = []
    seen = set()
    for item in str(raw).split(","):
        token = canonicalize_role_name(item)
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
        role = canonicalize_role_name(coordinator.get("role", ""))
        if role:
            roles.append(role)

    agents = data.get("agents")
    if isinstance(agents, list):
        for row in agents:
            if isinstance(row, dict):
                role = canonicalize_role_name(row.get("role", ""))
            else:
                role = canonicalize_role_name(row)
            if role:
                roles.append(role)

    return dedupe_roles(canonicalize_roles(roles))


def load_orchestrator_role_profiles(team_dir: Path, available_roles: Optional[List[str]] = None) -> List[Dict[str, str]]:
    roles = dedupe_roles(canonicalize_roles(available_roles or load_orchestrator_roles(team_dir)))
    profiles: List[Dict[str, str]] = []
    for role in roles:
        mission = ""
        agent_doc = resolve_role_asset(team_dir / "agents", role, "AGENTS.md")
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
    selected = dedupe_roles(canonicalize_roles(selected_roles))
    available = dedupe_roles(canonicalize_roles(available_roles))

    candidate_keys = [canonicalize_role_name(c).lower() for c in verifier_candidates if c]
    selected_verifiers = [r for r in selected if r.lower() in candidate_keys]

    available_verifiers: List[str] = []
    for cand in verifier_candidates:
        ckey = canonicalize_role_name(cand).lower()
        for role in available:
            if role.lower() == ckey and role not in available_verifiers:
                available_verifiers.append(role)

    added = False
    if not selected_verifiers and available_verifiers:
        has_worklike = any(
            str(role) in {
                "DataEngineer",
                "Codex-Dev",
                "Codex-Writer",
                "Claude-Writer",
                "Codex-Analyst",
                "Claude-Analyst",
            }
            for role in selected
        )
        if has_worklike:
            preferred_verifiers: List[str] = []
            if "Codex-Reviewer" in available_verifiers:
                preferred_verifiers.append("Codex-Reviewer")
            if "Claude-Reviewer" in available and "Claude-Reviewer" not in preferred_verifiers:
                preferred_verifiers.append("Claude-Reviewer")
            if not preferred_verifiers:
                preferred_verifiers.append(available_verifiers[0])
            for role in preferred_verifiers:
                if role not in selected:
                    selected.append(role)
            selected_verifiers = preferred_verifiers
        else:
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


def _ordered_roles(rows: List[str]) -> List[str]:
    return dedupe_roles(
        sorted(
            rows,
            key=lambda role: (
                ROLE_ORDER.get(str(role).strip(), 999),
                str(role).lower().startswith("claude-"),
                str(role).lower(),
            ),
        )
    )


def _has_any(prompt_lower: str, keys: tuple[str, ...]) -> bool:
    return any(token in prompt_lower for token in keys)


def _single_role_only_requested(prompt_lower: str) -> bool:
    return _has_any(prompt_lower, SINGLE_ROLE_ONLY_KEYS)


def _add_companion_roles(
    roles: List[str],
    *,
    available_roles: List[str],
    prompt_lower: str,
) -> List[str]:
    if _single_role_only_requested(prompt_lower):
        return dedupe_roles(roles)
    available_set = {str(role).strip() for role in available_roles if str(role).strip()}
    expanded = list(roles)
    for role in list(roles):
        companion = CLAUDE_COMPANION_ROLES.get(role)
        if companion and companion in available_set and companion not in expanded:
            expanded.append(companion)
    return _ordered_roles(expanded)


def _add_default_review_pair(
    roles: List[str],
    *,
    available_roles: List[str],
    prompt_lower: str,
) -> List[str]:
    if _single_role_only_requested(prompt_lower):
        return dedupe_roles(canonicalize_roles(roles))
    current = dedupe_roles(canonicalize_roles(roles))
    has_review = any(any(key in str(role).lower() for key in ("review", "critic", "verif")) for role in current)
    has_worklike = any(
        str(role) in {"DataEngineer", "Codex-Dev", "Codex-Writer", "Claude-Writer", "Codex-Analyst", "Claude-Analyst"}
        for role in current
    )
    if not has_worklike or has_review:
        return current
    available_set = {str(role).strip() for role in available_roles if str(role).strip()}
    if "Codex-Reviewer" in available_set:
        current.append("Codex-Reviewer")
    if "Claude-Reviewer" in available_set:
        current.append("Claude-Reviewer")
    return _ordered_roles(current)


def _role_score_from_text(prompt_lower: str, role: str, mission: str) -> int:
    name_key = str(role or "").strip().lower()
    mission_lower = str(mission or "").strip().lower()
    score = 0

    if _has_any(prompt_lower, REVIEW_SIGNAL_KEYS):
        if any(k in name_key for k in ("review", "qa", "verif")) or any(k in mission_lower for k in ("risk", "regression", "test", "review")):
            score += 6
    if _has_any(prompt_lower, DATA_SIGNAL_KEYS):
        if any(k in name_key for k in ("data", "etl", "sql", "schema")) or any(k in mission_lower for k in ("data", "etl", "schema", "quality")):
            score += 6
    if _has_any(prompt_lower, BUILD_SIGNAL_KEYS):
        build_name_hit = any(k in name_key for k in ("dev", "coder", "builder")) or (("engineer" in name_key) and ("data" not in name_key))
        if build_name_hit or any(k in mission_lower for k in ("implement", "build", "code", "develop")):
            score += 5
    if _has_any(prompt_lower, DOC_SIGNAL_KEYS):
        if any(k in name_key for k in ("writer", "doc", "scribe")) or any(k in mission_lower for k in ("document", "report", "summary")):
            score += 4
    if _has_any(prompt_lower, ANALYSIS_SIGNAL_KEYS):
        if any(k in name_key for k in ("anal", "research", "tuner")) or any(k in mission_lower for k in ("analysis", "research", "compare")):
            score += 4

    short_check = ("한 문장" in prompt_lower) or ("짧게" in prompt_lower) or ("3개만" in prompt_lower) or ("존재 여부" in prompt_lower)
    if short_check and ("review" in name_key or "verif" in name_key or "qa" in name_key):
        score += 2

    return score


def _build_prompt_role_preset(
    *,
    prompt_lower: str,
    available_roles: List[str],
    has_review_signal: bool,
    has_data_signal: bool,
    has_build_signal: bool,
    has_doc_signal: bool,
    has_analysis_signal: bool,
) -> List[str]:
    available_set = {str(role).strip() for role in available_roles if str(role).strip()}
    roles: List[str] = []

    if has_data_signal and "DataEngineer" in available_set:
        roles.append("DataEngineer")
    if has_build_signal and "Codex-Dev" in available_set:
        roles.append("Codex-Dev")
    if has_doc_signal and "Codex-Writer" in available_set:
        roles.append("Codex-Writer")
    if has_analysis_signal and "Codex-Analyst" in available_set:
        roles.append("Codex-Analyst")

    if not roles:
        if has_review_signal and "Codex-Reviewer" in available_set:
            roles.append("Codex-Reviewer")
        elif has_review_signal and "Claude-Reviewer" in available_set:
            roles.append("Claude-Reviewer")
        else:
            return []

    roles = _add_companion_roles(
        roles,
        available_roles=available_roles,
        prompt_lower=prompt_lower,
    )
    if has_review_signal or any((has_data_signal, has_build_signal, has_doc_signal, has_analysis_signal)):
        roles = _add_default_review_pair(
            roles,
            available_roles=available_roles,
            prompt_lower=prompt_lower,
        )
    return roles


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

    has_review_signal = _has_any(prompt_lower, REVIEW_SIGNAL_KEYS)
    has_data_signal = _has_any(prompt_lower, DATA_SIGNAL_KEYS)
    has_build_signal = _has_any(prompt_lower, BUILD_SIGNAL_KEYS)
    has_doc_signal = _has_any(prompt_lower, DOC_SIGNAL_KEYS)
    has_analysis_signal = _has_any(prompt_lower, ANALYSIS_SIGNAL_KEYS)

    wants_multi = _has_any(prompt_lower, MULTI_SIGNAL_KEYS)
    category_hits = sum(1 for flag in (has_review_signal, has_data_signal, has_build_signal, has_doc_signal, has_analysis_signal) if flag)
    if category_hits >= 2:
        wants_multi = True

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
        chosen = _ordered_roles(explicit)
        chosen = _add_companion_roles(
            chosen,
            available_roles=[str(profile.get("role", "")).strip() for profile in profiles],
            prompt_lower=prompt_lower,
        )
        if wants_multi or any(flag for flag in (has_build_signal, has_doc_signal, has_analysis_signal)):
            chosen = _add_default_review_pair(
                chosen,
                available_roles=[str(profile.get("role", "")).strip() for profile in profiles],
                prompt_lower=prompt_lower,
            )
        return chosen

    preset_roles = _build_prompt_role_preset(
        prompt_lower=prompt_lower,
        available_roles=[str(profile.get("role", "")).strip() for profile in profiles],
        has_review_signal=has_review_signal,
        has_data_signal=has_data_signal,
        has_build_signal=has_build_signal,
        has_doc_signal=has_doc_signal,
        has_analysis_signal=has_analysis_signal,
    )
    if preset_roles:
        return preset_roles

    scored: List[Tuple[int, str]] = []
    for profile in profiles:
        role = str(profile.get("role", "")).strip()
        if not role or role.lower() == "orchestrator":
            continue
        score = _role_score_from_text(prompt_lower, role, str(profile.get("mission", "")))
        if score > 0:
            scored.append((score, role))

    if not scored:
        both_keys = ("both", "둘 다", "둘다", "각각", "cross-check", "교차")
        roles: List[str] = []
        available_set = {str(profile.get("role", "")).strip() for profile in profiles if str(profile.get("role", "")).strip()}
        if _has_any(prompt_lower, DATA_SIGNAL_KEYS):
            roles.append("DataEngineer")
        if _has_any(prompt_lower, REVIEW_SIGNAL_KEYS):
            roles.append("Codex-Reviewer")
            if "Claude-Reviewer" in available_set:
                roles.append("Claude-Reviewer")
        if _has_any(prompt_lower, BUILD_SIGNAL_KEYS):
            roles.append("Codex-Dev")
        if _has_any(prompt_lower, DOC_SIGNAL_KEYS):
            roles.append("Codex-Writer")
            if "Claude-Writer" in available_set:
                roles.append("Claude-Writer")
        if _has_any(prompt_lower, ANALYSIS_SIGNAL_KEYS):
            roles.append("Codex-Analyst")
            if "Claude-Analyst" in available_set:
                roles.append("Claude-Analyst")
        if not roles and any(k in prompt_lower for k in both_keys):
            roles = ["DataEngineer", "Codex-Reviewer"]
            if "Claude-Reviewer" in available_set:
                roles.append("Claude-Reviewer")
        roles = _add_companion_roles(
            [r for r in roles if r],
            available_roles=[str(profile.get("role", "")).strip() for profile in profiles],
            prompt_lower=prompt_lower,
        )
        if wants_multi or any(flag for flag in (has_build_signal, has_doc_signal, has_analysis_signal, has_data_signal)):
            roles = _add_default_review_pair(
                roles,
                available_roles=[str(profile.get("role", "")).strip() for profile in profiles],
                prompt_lower=prompt_lower,
            )
        return roles

    scored.sort(key=lambda item: (-item[0], item[1].lower().startswith("claude-"), item[1].lower()))
    top_score = scored[0][0]
    selected = [scored[0][1]]
    if wants_multi:
        for score, role in scored[1:3]:
            if score >= max(3, top_score - 2):
                selected.append(role)
    selected = _add_companion_roles(
        selected,
        available_roles=[str(profile.get("role", "")).strip() for profile in profiles],
        prompt_lower=prompt_lower,
    )
    if wants_multi or any(flag for flag in (has_build_signal, has_doc_signal, has_analysis_signal)):
        selected = _add_default_review_pair(
            selected,
            available_roles=[str(profile.get("role", "")).strip() for profile in profiles],
            prompt_lower=prompt_lower,
        )
    return selected


def available_worker_roles(
    available_roles: List[str],
    *,
    default_pool: Optional[List[str]] = None,
) -> List[str]:
    workers = [r for r in dedupe_roles(available_roles) if r.lower() != "orchestrator"]
    return workers or dedupe_roles(default_pool or DEFAULT_WORKER_ROLE_POOL)
