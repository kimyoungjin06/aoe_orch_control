#!/usr/bin/env python3
"""Phase1 ensemble planning helpers.

Phase1 is planner-only:
- same mission is given to Codex and Claude
- plans are critiqued and shared for multiple rounds
- only after the plan is stable do we enter Phase2 execution
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from typing import Any, Callable, Dict, List, Optional

from aoe_tg_schema import default_plan_critic_payload, normalize_plan_critic_payload, plan_critic_primary_issue


def _trim_text(raw: Any, limit: int) -> str:
    return str(raw or "").strip()[: max(0, int(limit or 0))]


def _dedupe_lines(rows: List[str], *, limit: int) -> List[str]:
    out: List[str] = []
    for row in rows:
        token = _trim_text(row, 240)
        if token and token not in out:
            out.append(token)
    return out[: max(1, int(limit or 1))]


def _planner_prompt(
    *,
    user_prompt: str,
    provider: str,
    workers: List[str],
    max_subtasks: int,
    round_no: int,
    total_rounds: int,
    shared_feedback: str,
) -> str:
    feedback = f"\n공유된 이전 회차 피드백:\n{shared_feedback}\n" if shared_feedback else ""
    return (
        "너는 TF Phase1 planner다. 지금은 실행이 아니라 계획 수립 단계다.\n"
        "같은 미션이 여러 planner(Codex/Claude)에게 병렬로 전달되고, 각 회차마다 서로의 비판 내용을 반영해 계획을 개선한다.\n"
        "반드시 JSON 객체만 출력한다. 설명 문장 금지.\n"
        "JSON 스키마:\n"
        "{\n"
        '  "summary": "한 줄 요약",\n'
        '  "subtasks": [\n'
        '    {"id":"S1", "title":"...", "goal":"...", "owner_role":"ROLE", "acceptance":["..."]}\n'
        "  ]\n"
        "}\n"
        "규칙:\n"
        f"- planner_provider: {provider}\n"
        f"- round: {round_no}/{total_rounds}\n"
        f"- owner_role은 다음 중 하나만 사용: {', '.join(workers)}\n"
        f"- subtasks는 1~{max(1, int(max_subtasks))}개\n"
        "- 각 subtask는 겹치지 않는 산출물과 검증 기준을 가져야 한다\n"
        "- 실행팀이 병렬로 일할 수 있도록 독립 가능한 단위로 분해한다\n"
        "- Codex-Reviewer/critic이 최종 검증할 수 있도록 acceptance를 구체적으로 쓴다\n"
        "- 계획이 덜 완성됐으면 범위를 줄이고, ambiguity를 드러내라\n"
        f"{feedback}\n"
        f"사용자 요청:\n{user_prompt.strip()}\n"
    )


def _critic_prompt(
    *,
    user_prompt: str,
    provider: str,
    planner_provider: str,
    plan: Dict[str, Any],
    round_no: int,
    total_rounds: int,
) -> str:
    payload = json.dumps(plan, ensure_ascii=False)
    return (
        "너는 TF Phase1 critic이다. 아래 계획이 실제 실행 단계(Phase2)로 넘어갈 만큼 충분히 구체적인지 비판적으로 검토해라.\n"
        "반드시 JSON 객체만 출력한다. 설명 문장 금지.\n"
        "JSON 스키마:\n"
        "{\n"
        '  "approved": true|false,\n'
        '  "issues": ["..."],\n'
        '  "recommendations": ["..."]\n'
        "}\n"
        "규칙:\n"
        f"- critic_provider: {provider}\n"
        f"- planner_provider: {planner_provider}\n"
        f"- round: {round_no}/{total_rounds}\n"
        "- execution gap, role mismatch, acceptance weakness, hidden dependency를 우선 지적한다\n"
        "- plans that are too broad or not parallelizable should not be approved\n"
        "- issues는 정말 dispatch를 막을 문제만 적는다\n\n"
        f"사용자 요청:\n{user_prompt.strip()}\n\n"
        f"plan:\n{payload}\n"
    )


def _candidate_score(critic: Dict[str, Any], plan: Dict[str, Any]) -> tuple[int, int, int]:
    issues = critic.get("issues") or []
    approved = bool(critic.get("approved", True)) and not bool(issues)
    subtasks = plan.get("subtasks") or []
    return (
        0 if approved else 1,
        len(issues),
        -len(subtasks) if isinstance(subtasks, list) else 0,
    )


def _render_shared_feedback(round_candidates: List[Dict[str, Any]], *, best_idx: int) -> str:
    lines: List[str] = []
    for idx, row in enumerate(round_candidates, start=1):
        provider = str(row.get("provider", "")).strip() or f"planner-{idx}"
        plan = row.get("plan") if isinstance(row.get("plan"), dict) else {}
        critic = row.get("critic") if isinstance(row.get("critic"), dict) else default_plan_critic_payload()
        summary = _trim_text(plan.get("summary", ""), 180) or "no summary"
        marker = "best" if (idx - 1) == best_idx else "alt"
        lines.append(f"[{provider}|{marker}] {summary}")
        for issue in (critic.get("issues") or [])[:3]:
            lines.append(f"- issue: {_trim_text(issue, 200)}")
        for rec in (critic.get("recommendations") or [])[:3]:
            lines.append(f"- fix: {_trim_text(rec, 200)}")
    return "\n".join(lines).strip()


def _run_parallel_calls(
    providers: List[str],
    run_one: Callable[[str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if len(providers) <= 1:
        return [run_one(providers[0])] if providers else []

    max_workers = max(1, len(providers))
    ordered: Dict[str, Dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="aoe-phase1") as pool:
        futures = {provider: pool.submit(run_one, provider) for provider in providers}
        for provider, fut in futures.items():
            try:
                ordered[provider] = fut.result()
            except Exception as exc:
                ordered[provider] = {
                    "provider": provider,
                    "plan": None,
                    "critic": {
                        "approved": False,
                        "issues": [f"{provider} execution failed: {_trim_text(exc, 180)}"],
                        "recommendations": [],
                    },
                }
    return [ordered[p] for p in providers if p in ordered]


def run_phase1_ensemble_planning(
    *,
    args: Any,
    user_prompt: str,
    available_roles: List[str],
    normalize_task_plan_payload: Callable[..., Dict[str, Any]],
    parse_json_object_from_text: Callable[[str], Optional[Dict[str, Any]]],
    run_provider_execs: Dict[str, Callable[[str, int], str]],
    plan_roles_from_subtasks: Callable[[Optional[Dict[str, Any]]], List[str]],
    report_progress: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    workers = [str(r).strip() for r in (available_roles or []) if str(r).strip()] or ["Codex-Reviewer"]
    providers_csv = str(getattr(args, "plan_phase1_providers", "codex,claude") or "codex,claude")
    preferred = []
    for token in providers_csv.split(","):
        item = str(token or "").strip().lower()
        if item and item not in preferred:
            preferred.append(item)
    if not preferred:
        preferred = ["codex", "claude"]

    providers = [name for name in preferred if callable(run_provider_execs.get(name))]
    if not providers:
        return {
            "plan_data": None,
            "plan_critic": default_plan_critic_payload(),
            "plan_roles": [],
            "plan_replans": [],
            "plan_error": "no planning providers available",
            "plan_gate_blocked": True,
            "plan_gate_reason": "no planning providers available",
            "phase1_rounds": 0,
            "phase1_mode": "ensemble",
            "phase1_providers": [],
        }

    rounds = max(3, int(getattr(args, "plan_phase1_rounds", 3) or 3))
    max_subtasks = max(1, int(getattr(args, "plan_max_subtasks", 3) or 3))
    planner_timeout = max(60, min(int(getattr(args, "orch_command_timeout_sec", 240) or 240), 240))
    critic_timeout = max(45, min(int(getattr(args, "orch_command_timeout_sec", 180) or 180), 180))

    best_plan: Optional[Dict[str, Any]] = None
    best_critic: Dict[str, Any] = default_plan_critic_payload()
    best_roles: List[str] = []
    shared_feedback = ""
    plan_replans: List[Dict[str, Any]] = []

    for round_no in range(1, rounds + 1):
        def _run_planner_provider(provider: str) -> Dict[str, Any]:
            if callable(report_progress):
                report_progress(
                    phase="planner",
                    detail=f"phase1 round {round_no}/{rounds} provider={provider}",
                    attempt=round_no,
                    total=rounds,
                )
            planner_prompt = _planner_prompt(
                user_prompt=user_prompt,
                provider=provider,
                workers=workers,
                max_subtasks=max_subtasks,
                round_no=round_no,
                total_rounds=rounds,
                shared_feedback=shared_feedback,
            )
            try:
                raw_plan = run_provider_execs[provider](planner_prompt, planner_timeout)
                parsed_plan = parse_json_object_from_text(raw_plan)
                plan = normalize_task_plan_payload(
                    parsed_plan,
                    user_prompt=user_prompt,
                    workers=workers,
                    max_subtasks=max_subtasks,
                )
            except Exception as exc:
                return {
                    "provider": provider,
                    "plan": None,
                    "critic": {
                        "approved": False,
                        "issues": [f"{provider} planner failed: {_trim_text(exc, 180)}"],
                        "recommendations": [],
                    },
                }

            issues: List[str] = []
            recommendations: List[str] = []
            approvals: List[bool] = []
            def _run_critic_provider(critic_provider: str) -> Dict[str, Any]:
                if callable(report_progress):
                    report_progress(
                        phase="critic",
                        detail=f"phase1 round {round_no}/{rounds} planner={provider} critic={critic_provider}",
                        attempt=round_no,
                        total=rounds,
                    )
                critic_prompt = _critic_prompt(
                    user_prompt=user_prompt,
                    provider=critic_provider,
                    planner_provider=provider,
                    plan=plan,
                    round_no=round_no,
                    total_rounds=rounds,
                )
                try:
                    raw_critic = run_provider_execs[critic_provider](critic_prompt, critic_timeout)
                    parsed_critic = parse_json_object_from_text(raw_critic)
                    return normalize_plan_critic_payload(parsed_critic, max_items=5)
                except Exception as exc:
                    return {
                        "approved": False,
                        "issues": [f"{critic_provider} critic failed: {_trim_text(exc, 180)}"],
                        "recommendations": [],
                    }

            critic_rows = _run_parallel_calls(providers, _run_critic_provider)
            for critic in critic_rows:
                approvals.append(bool(critic.get("approved", True)) and not bool(critic.get("issues") or []))
                issues.extend([_trim_text(item, 240) for item in (critic.get("issues") or [])])
                recommendations.extend([_trim_text(item, 240) for item in (critic.get("recommendations") or [])])

            aggregate_critic = {
                "approved": bool(approvals) and all(approvals),
                "issues": _dedupe_lines(issues, limit=8),
                "recommendations": _dedupe_lines(recommendations, limit=8),
            }
            return {
                "provider": provider,
                "plan": plan,
                "critic": aggregate_critic,
            }

        round_candidates = _run_parallel_calls(providers, _run_planner_provider)

        viable = [row for row in round_candidates if isinstance(row.get("plan"), dict)]
        if not viable:
            return {
                "plan_data": None,
                "plan_critic": default_plan_critic_payload(),
                "plan_roles": [],
                "plan_replans": plan_replans,
                "plan_error": f"phase1 round {round_no}: no valid planner output",
                "plan_gate_blocked": True,
                "plan_gate_reason": f"phase1 round {round_no}: no valid planner output",
                "phase1_rounds": round_no,
                "phase1_mode": "ensemble",
                "phase1_providers": providers,
            }

        scored = sorted(
            enumerate(viable),
            key=lambda item: _candidate_score(item[1]["critic"], item[1]["plan"]),
        )
        best_idx, best_candidate = scored[0]
        best_plan = best_candidate["plan"]
        best_critic = best_candidate["critic"]
        best_roles = plan_roles_from_subtasks(best_plan)
        plan_replans.append(
            {
                "attempt": round_no,
                "critic": "approved" if not bool(best_critic.get("issues") or []) and bool(best_critic.get("approved", True)) else "needs_fix",
                "subtasks": len(best_plan.get("subtasks") or []),
                "providers": providers[:],
                "best_provider": best_candidate["provider"],
                "issues": len(best_critic.get("issues") or []),
            }
        )
        shared_feedback = _render_shared_feedback(viable, best_idx=best_idx)

    plan_gate_blocked = bool(getattr(args, "plan_block_on_critic", True)) and bool(best_critic.get("issues") or [])
    plan_gate_reason = plan_critic_primary_issue(best_critic, limit=240) if plan_gate_blocked else ""
    return {
        "plan_data": best_plan,
        "plan_critic": best_critic,
        "plan_roles": best_roles,
        "plan_replans": plan_replans,
        "plan_error": "",
        "plan_gate_blocked": plan_gate_blocked,
        "plan_gate_reason": plan_gate_reason,
        "phase1_rounds": rounds,
        "phase1_mode": "ensemble",
        "phase1_providers": providers,
    }
