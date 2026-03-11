#!/usr/bin/env python3
"""Orchestrator direct/synthesis/critic/proposal helpers extracted from the gateway monolith."""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, List, Optional


def run_orchestrator_direct(
    args: Any,
    user_prompt: str,
    *,
    reply_lang: str,
    default_reply_lang: str,
    normalize_chat_lang_token: Callable[[str, str], str],
    run_codex_exec: Callable[..., str],
) -> str:
    lang = normalize_chat_lang_token(reply_lang, default_reply_lang) or default_reply_lang
    if lang == "en":
        prompt = (
            "You are a project orchestrator. Reply naturally to a Telegram user.\n"
            "Principles:\n"
            "- English\n"
            "- Do not expose internal role/protocol/request-id unless user explicitly asks\n"
            "- Do not overclaim or assert unsupported facts\n"
            "- Be concise and practical; suggest next action only when useful\n\n"
            f"User request:\n{user_prompt.strip()}\n"
        )
    else:
        prompt = (
            "너는 프로젝트 오케스트레이터다. 텔레그램 사용자와 자연스럽게 대화하듯 답해라.\n"
            "원칙:\n"
            "- 한국어\n"
            "- 사용자가 묻지 않으면 내부 역할/프로토콜/요청ID를 노출하지 않는다\n"
            "- 과장하거나 근거 없는 수치를 단정하지 않는다\n"
            "- 실무적으로 간결하게 답하고, 필요할 때만 다음 행동을 제안한다\n\n"
            f"사용자 요청:\n{user_prompt.strip()}\n"
        )
    return run_codex_exec(args, prompt, timeout_sec=min(900, max(90, int(args.orch_command_timeout_sec))))


def synthesize_orchestrator_response(
    args: Any,
    user_prompt: str,
    state: Dict[str, Any],
    *,
    reply_lang: str,
    default_reply_lang: str,
    normalize_chat_lang_token: Callable[[str, str], str],
    run_codex_exec: Callable[..., str],
) -> str:
    replies = state.get("replies") or []
    chunks: List[str] = []
    for row in replies[:8]:
        role = str(row.get("role", row.get("from", "agent"))).strip() or "agent"
        body = str(row.get("body", "")).strip()
        if body:
            chunks.append(f"[{role}]\n{body}")

    joined = "\n\n".join(chunks).strip() or "(no replies)"
    lang = normalize_chat_lang_token(reply_lang, default_reply_lang) or default_reply_lang
    if lang == "en":
        prompt = (
            "You are a team orchestrator. Merge sub-agent replies into a single user-facing answer.\n"
            "Rules:\n"
            "- English\n"
            "- Hide operational details such as roles/protocol/request-id\n"
            "- Resolve contradictions conservatively; state uncertainty when needed\n"
            "- Do not assert unsupported facts\n"
            "- Keep a single coherent voice for the user\n\n"
            f"User request:\n{user_prompt.strip()}\n\n"
            f"Sub-agent replies:\n{joined}\n"
        )
    else:
        prompt = (
            "너는 팀 오케스트레이터다. 아래 서브에이전트 답변을 사용자용 단일 답변으로 통합해라.\n"
            "규칙:\n"
            "- 한국어\n"
            "- 내부 역할명/프로토콜/요청ID 같은 운영 디테일은 숨긴다\n"
            "- 서로 모순되는 내용은 보수적으로 정리하고, 불확실하면 불확실하다고 명시한다\n"
            "- 실행 근거 없는 수치/사실은 단정하지 않는다\n"
            "- 사용자에게는 자연스러운 한 목소리로 답한다\n\n"
            f"사용자 요청:\n{user_prompt.strip()}\n\n"
            f"서브에이전트 답변:\n{joined}\n"
        )
    return run_codex_exec(args, prompt, timeout_sec=min(900, max(90, int(args.orch_command_timeout_sec))))


def critique_task_execution_result(
    args: Any,
    user_prompt: str,
    state: Dict[str, Any],
    *,
    task: Optional[Dict[str, Any]],
    attempt_no: int,
    max_attempts: int,
    reply_lang: str,
    default_reply_lang: str,
    normalize_chat_lang_token: Callable[[str, str], str],
    mask_sensitive_text: Callable[[str], str],
    run_codex_exec: Callable[..., str],
    parse_json_object_from_text: Callable[[str], Optional[Dict[str, Any]]],
    normalize_exec_critic_payload: Callable[..., Dict[str, Any]],
    now_iso: Callable[[], str],
) -> Dict[str, Any]:
    attempt_no = max(1, int(attempt_no))
    max_attempts = max(1, int(max_attempts))

    replies = state.get("replies") or []
    chunks: List[str] = []
    for row in replies[:8]:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", row.get("from", "agent"))).strip() or "agent"
        body = str(row.get("body", "")).strip()
        if not body:
            continue
        body = mask_sensitive_text(body)
        if len(body) > 1600:
            body = body[:1597] + "..."
        chunks.append(f"[{role}]\n{body}")
    joined = "\n\n".join(chunks).strip() or "(no replies)"

    plan_hint = ""
    if isinstance(task, dict) and isinstance(task.get("plan"), dict):
        plan = task.get("plan") or {}
        summary = str(plan.get("summary", "")).strip()
        subtasks = plan.get("subtasks") or []
        titles: List[str] = []
        if isinstance(subtasks, list):
            for row in subtasks[:6]:
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title", "")).strip() or str(row.get("goal", "")).strip()
                if title:
                    titles.append(title)
        if summary or titles:
            plan_hint = "plan_summary: {s}\nplan_subtasks: {n}\nplan_titles: {t}".format(
                s=summary or "-",
                n=len(subtasks) if isinstance(subtasks, list) else 0,
                t=" | ".join(titles) if titles else "-",
            )

    lang = normalize_chat_lang_token(reply_lang, default_reply_lang) or default_reply_lang
    if lang == "en":
        critic_prompt = (
            "You are an execution critic for a multi-agent task.\n"
            "Your job: decide whether the outputs satisfy the user's request.\n"
            "Return ONLY a JSON object. No prose.\n"
            "Schema:\n"
            "{\n"
            "  \"verdict\": \"success\"|\"retry\"|\"fail\",\n"
            "  \"action\": \"none\"|\"retry\"|\"replan\"|\"escalate\",\n"
            "  \"reason\": \"short reason\",\n"
            "  \"fix\": \"short guidance for next attempt (optional)\"\n"
            "}\n"
            "Rules:\n"
            "- success: requirements are met with correct/usable output.\n"
            "- retry: missing/weak parts can be fixed automatically.\n"
            "- fail: needs operator decision or requirements are ambiguous.\n"
            "- If attempt is near max, prefer fail/escalate over endless retries.\n\n"
            f"attempt: {attempt_no}/{max_attempts}\n"
            f"User request:\n{user_prompt.strip()}\n\n"
            + (f"{plan_hint}\n\n" if plan_hint else "")
            + f"Sub-agent replies:\n{joined}\n"
        )
    else:
        critic_prompt = (
            "너는 멀티에이전트 실행 결과를 판정하는 execution critic이다.\n"
            "목표: 아래 결과가 사용자 요청을 충족하는지 판정하고, 필요하면 재시도/재계획 지침을 제시한다.\n"
            "반드시 JSON 객체만 출력한다. 설명 문장 금지.\n"
            "JSON 스키마:\n"
            "{\n"
            "  \"verdict\": \"success\"|\"retry\"|\"fail\",\n"
            "  \"action\": \"none\"|\"retry\"|\"replan\"|\"escalate\",\n"
            "  \"reason\": \"짧은 이유(200자 이내)\",\n"
            "  \"fix\": \"다음 시도에서 바꿀 점(선택, 600자 이내)\"\n"
            "}\n"
            "규칙:\n"
            "- success: 요구사항 충족, 결과가 실무적으로 사용 가능.\n"
            "- retry: 일부 미흡/누락이 있으나 자동 재시도로 개선 가능.\n"
            "- fail: 요구 불명확/환경 제약/결정 필요 등으로 운영자 개입이 필요.\n"
            "- attempt가 max에 가까우면 무한 재시도 대신 fail/escalate를 우선.\n\n"
            f"attempt: {attempt_no}/{max_attempts}\n"
            f"사용자 요청:\n{user_prompt.strip()}\n\n"
            + (f"{plan_hint}\n\n" if plan_hint else "")
            + f"서브에이전트 답변:\n{joined}\n"
        )

    raw = run_codex_exec(args, critic_prompt, timeout_sec=min(600, max(60, int(args.orch_command_timeout_sec))))
    parsed = parse_json_object_from_text(raw)

    return normalize_exec_critic_payload(
        parsed,
        attempt_no=attempt_no,
        max_attempts=max_attempts,
        at=now_iso(),
    )


def extract_followup_todo_proposals(
    args: Any,
    user_prompt: str,
    state: Dict[str, Any],
    *,
    task: Optional[Dict[str, Any]],
    reply_lang: str,
    default_reply_lang: str,
    default_orch_command_timeout_sec: int,
    normalize_chat_lang_token: Callable[[str, str], str],
    mask_sensitive_text: Callable[[str], str],
    run_codex_exec: Callable[..., str],
    parse_json_object_from_text: Callable[[str], Optional[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    replies = state.get("replies") or []
    chunks: List[str] = []
    for row in replies[:8]:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", row.get("from", "agent"))).strip() or "agent"
        body = str(row.get("body", "")).strip()
        if not body:
            continue
        body = mask_sensitive_text(body)
        if len(body) > 1400:
            body = body[:1397] + "..."
        chunks.append(f"[{role}]\n{body}")
    if not chunks:
        return []

    task_lines: List[str] = []
    if isinstance(task, dict):
        todo_id = str(task.get("todo_id", "")).strip()
        if todo_id:
            task_lines.append(f"- source_todo_id: {todo_id}")
        plan = task.get("plan")
        if isinstance(plan, dict):
            summary = str(plan.get("summary", "")).strip()
            if summary:
                task_lines.append(f"- plan_summary: {summary[:240]}")
        plan_roles = task.get("plan_roles")
        if isinstance(plan_roles, list):
            roles = [str(item or "").strip() for item in plan_roles if str(item or "").strip()]
            if roles:
                task_lines.append(f"- plan_roles: {', '.join(roles[:6])}")
    task_hint = "\n".join(task_lines).strip()
    lang = normalize_chat_lang_token(reply_lang, default_reply_lang) or default_reply_lang
    joined = "\n\n".join(chunks).strip()

    if lang == "en":
        prompt = (
            "You extract follow-up todo proposals from a completed multi-agent task.\n"
            "Return JSON only. No markdown, no prose.\n"
            "Schema:\n"
            "{\n"
            "  \"proposals\": [\n"
            "    {\"summary\":\"...\", \"priority\":\"P1|P2|P3\", \"kind\":\"followup|risk|debt|handoff\", \"reason\":\"...\", \"confidence\":0.0}\n"
            "  ]\n"
            "}\n"
            "Rules:\n"
            "- Propose only NEW actionable follow-up tasks.\n"
            "- Do not restate the original task, completed work, or pure notes.\n"
            "- Max 5 proposals.\n"
            "- Use an empty list if no follow-up work is needed.\n\n"
            f"User request:\n{mask_sensitive_text(user_prompt.strip())}\n\n"
            + (f"Task context:\n{task_hint}\n\n" if task_hint else "")
            + f"Agent replies:\n{joined}\n"
        )
    else:
        prompt = (
            "너는 완료된 멀티에이전트 작업 결과에서 후속 todo proposal만 추출한다.\n"
            "반드시 JSON만 출력한다. 마크다운/설명문 금지.\n"
            "스키마:\n"
            "{\n"
            "  \"proposals\": [\n"
            "    {\"summary\":\"...\", \"priority\":\"P1|P2|P3\", \"kind\":\"followup|risk|debt|handoff\", \"reason\":\"...\", \"confidence\":0.0}\n"
            "  ]\n"
            "}\n"
            "규칙:\n"
            "- 새로운 실행형 후속 작업만 제안한다.\n"
            "- 원래 작업의 재진술, 이미 끝난 일, 단순 메모/관찰은 제외한다.\n"
            "- 최대 5개.\n"
            "- 후속 작업이 없으면 빈 배열을 반환한다.\n\n"
            f"사용자 요청:\n{mask_sensitive_text(user_prompt.strip())}\n\n"
            + (f"작업 문맥:\n{task_hint}\n\n" if task_hint else "")
            + f"에이전트 응답:\n{joined}\n"
        )

    try:
        raw = run_codex_exec(
            args,
            prompt,
            timeout_sec=min(
                180,
                max(
                    60,
                    int(getattr(args, "orch_command_timeout_sec", default_orch_command_timeout_sec) or default_orch_command_timeout_sec) // 4,
                ),
            ),
        )
    except Exception:
        return []

    parsed = parse_json_object_from_text(raw)
    if not isinstance(parsed, dict):
        return []
    rows = parsed.get("proposals")
    if not isinstance(rows, list):
        return []

    user_key = re.sub(r"\s+", " ", str(user_prompt or "").strip()).lower()
    seen = set()
    normalized: List[Dict[str, Any]] = []
    for row in rows[:8]:
        if not isinstance(row, dict):
            continue
        summary = " ".join(str(row.get("summary", "")).strip().split())
        if not summary:
            continue
        summary_key = re.sub(r"\s+", " ", summary).lower()
        if not summary_key or summary_key == user_key or summary_key in seen:
            continue
        seen.add(summary_key)
        priority = str(row.get("priority", "P2")).strip().upper() or "P2"
        if priority not in {"P1", "P2", "P3"}:
            priority = "P2"
        kind = str(row.get("kind", "followup")).strip().lower() or "followup"
        if kind not in {"followup", "risk", "debt", "handoff"}:
            kind = "followup"
        reason = " ".join(str(row.get("reason", "")).strip().split())
        try:
            confidence = float(row.get("confidence", 0.0) or 0.0)
        except Exception:
            confidence = 0.0
        confidence = max(0.0, min(1.0, confidence))
        normalized.append(
            {
                "summary": summary[:600],
                "priority": priority,
                "kind": kind,
                "reason": reason[:240],
                "confidence": confidence,
            }
        )
        if len(normalized) >= 5:
            break

    return normalized
