#!/usr/bin/env python3
"""Schema coercion helpers for planner / critic payloads."""

from __future__ import annotations

from typing import Any, Dict, List


def _trim_text(raw: Any, limit: int) -> str:
    return str(raw or "").strip()[: max(0, int(limit))]


def default_plan_critic_payload() -> Dict[str, Any]:
    return {"approved": True, "issues": [], "recommendations": []}


def default_exec_critic_payload(
    *,
    verdict: str = "fail",
    action: str = "escalate",
    reason: str = "critic_parse_error",
    fix: str = "",
    attempt_no: int = 1,
    max_attempts: int = 3,
    at: str = "",
) -> Dict[str, Any]:
    return {
        "verdict": verdict,
        "action": action,
        "reason": _trim_text(reason, 200),
        "fix": _trim_text(fix, 600),
        "attempt": max(1, int(attempt_no or 1)),
        "max_attempts": max(1, int(max_attempts or 1)),
        "at": str(at or "").strip(),
    }


def normalize_task_plan_payload(
    parsed: Any,
    *,
    user_prompt: str,
    workers: List[str],
    max_subtasks: int,
) -> Dict[str, Any]:
    role_map = {str(r).strip().lower(): str(r).strip() for r in (workers or []) if str(r).strip()}
    worker_list = list(role_map.values()) or ["Worker"]

    summary = ""
    raw_subtasks: List[Any] = []
    meta_in: Dict[str, Any] = {}
    if isinstance(parsed, dict):
        summary = str(parsed.get("summary", "")).strip()
        if isinstance(parsed.get("subtasks"), list):
            raw_subtasks = parsed.get("subtasks") or []
        if isinstance(parsed.get("meta"), dict):
            meta_in = dict(parsed.get("meta") or {})

    normalized: List[Dict[str, Any]] = []
    for i, row in enumerate(raw_subtasks, start=1):
        if not isinstance(row, dict):
            continue
        sid = str(row.get("id", f"S{i}")).strip() or f"S{i}"
        title = str(row.get("title", "")).strip() or str(row.get("goal", "")).strip() or f"Subtask {i}"
        goal = str(row.get("goal", "")).strip() or title

        role_raw = str(row.get("owner_role", row.get("role", ""))).strip()
        if role_raw and role_raw.lower() in role_map:
            role = role_map[role_raw.lower()]
        elif role_raw:
            role = role_raw
        else:
            role = worker_list[min(i - 1, len(worker_list) - 1)]

        acceptance: List[str] = []
        raw_acceptance = row.get("acceptance")
        if isinstance(raw_acceptance, list):
            for item in raw_acceptance:
                token = str(item or "").strip()
                if token:
                    acceptance.append(token[:240])
        if not acceptance:
            acceptance = [f"{title} 결과가 사용자 요청과 직접 연결되어 설명된다."]

        normalized.append(
            {
                "id": sid[:32],
                "title": title[:160],
                "goal": goal[:400],
                "owner_role": role[:64],
                "acceptance": acceptance[:3],
            }
        )

    limit = max(1, int(max_subtasks or 1))
    normalized = normalized[:limit]
    if not normalized:
        normalized = [
            {
                "id": "S1",
                "title": "요청 핵심 실행",
                "goal": str(user_prompt or "").strip() or "사용자 요청 실행",
                "owner_role": worker_list[0],
                "acceptance": ["요청에 대한 실행/검증 결과가 사용자 관점으로 정리된다."],
            }
        ]

    if not summary:
        summary = f"subtasks={len(normalized)}"

    meta_worker_roles = meta_in.get("worker_roles")
    worker_roles: List[str] = []
    if isinstance(meta_worker_roles, list):
        for row in meta_worker_roles:
            token = str(row or "").strip()
            if token and token not in worker_roles:
                worker_roles.append(token[:64])
    if not worker_roles:
        worker_roles = worker_list[:]

    return {
        "summary": summary[:240],
        "subtasks": normalized,
        "meta": {
            "max_subtasks": limit,
            "worker_roles": worker_roles,
        },
    }


def normalize_plan_critic_payload(parsed: Any, *, max_items: int = 5) -> Dict[str, Any]:
    approved = True
    issues: List[str] = []
    recommendations: List[str] = []

    if isinstance(parsed, dict):
        approved = bool(parsed.get("approved", True))
        raw_issues = parsed.get("issues")
        if isinstance(raw_issues, list):
            for item in raw_issues:
                token = str(item or "").strip()
                if token:
                    issues.append(token[:240])
        raw_recs = parsed.get("recommendations")
        if isinstance(raw_recs, list):
            for item in raw_recs:
                token = str(item or "").strip()
                if token:
                    recommendations.append(token[:240])

    return {
        "approved": approved,
        "issues": issues[: max(1, int(max_items or 1))],
        "recommendations": recommendations[: max(1, int(max_items or 1))],
    }


def plan_critic_primary_issue(parsed: Any, *, limit: int = 240) -> str:
    payload = normalize_plan_critic_payload(parsed, max_items=1)
    issues = payload.get("issues") or []
    if not issues:
        return ""
    return _trim_text(issues[0], limit)


def normalize_plan_replans_payload(raw: Any, *, keep: int = 80) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not isinstance(raw, list):
        return rows
    for item in raw[-max(1, int(keep or 1)) :]:
        if not isinstance(item, dict):
            continue
        try:
            attempt = max(1, int(item.get("attempt", 0) or 0))
        except Exception:
            attempt = 1
        critic = str(item.get("critic", "")).strip().lower()
        if critic not in {"approved", "needs_fix"}:
            critic = "unknown"
        try:
            subtasks = max(0, int(item.get("subtasks", 0) or 0))
        except Exception:
            subtasks = 0
        rows.append({"attempt": attempt, "critic": critic, "subtasks": subtasks})
    return rows


def normalize_exec_critic_payload(
    parsed: Any,
    *,
    attempt_no: int,
    max_attempts: int,
    at: str,
) -> Dict[str, Any]:
    verdict = "fail"
    action = "escalate"
    reason = "critic_parse_error"
    fix = ""

    if isinstance(parsed, dict):
        verdict_map = {
            "success": "success",
            "ok": "success",
            "pass": "success",
            "retry": "retry",
            "retriable": "retry",
            "fail": "fail",
            "failed": "fail",
            "error": "fail",
            "성공": "success",
            "재시도": "retry",
            "재실행": "retry",
            "실패": "fail",
        }
        action_map = {
            "none": "none",
            "noop": "none",
            "retry": "retry",
            "replan": "replan",
            "escalate": "escalate",
        }
        vraw = str(parsed.get("verdict", "")).strip().lower()
        araw = str(parsed.get("action", "")).strip().lower()
        verdict = verdict_map.get(vraw, verdict)
        action = action_map.get(araw, "")
        reason = _trim_text(parsed.get("reason", "") or reason, 200) or reason
        fix = _trim_text(parsed.get("fix", ""), 600)

    if verdict == "success":
        action = "none"
    elif verdict == "retry":
        action = action if action in {"retry", "replan"} else "retry"
    else:
        verdict = "fail"
        action = "escalate"

    return default_exec_critic_payload(
        verdict=verdict,
        action=action,
        reason=reason,
        fix=fix,
        attempt_no=attempt_no,
        max_attempts=max_attempts,
        at=at,
    )
