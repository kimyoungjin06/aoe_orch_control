"""Local agent routing for Telegram chat and feedback classification."""

from __future__ import annotations

import json
import os
import pathlib
import re
import sys
import urllib.error
from typing import Any

from offdesk_llm_endpoint import (
    DEFAULT_CODING_MODEL_CANDIDATES,
    LlmProviderError,
    call_ollama_json,
    default_ollama_base_urls,
    provider_status,
    resolve_provider_config,
    select_provider_runtime as select_llm_provider_runtime,
)

from .common import RemoteOperatorTelegramError, csv_values, unique_nonempty
from .rendering import ASSISTANT_REPLY_MAX_CHARS, sanitize_text
from .routing import COMMAND_SURFACE, CORE_OR_SLASH_COMMANDS, SESSION_INPUT_COMMANDS


DEFAULT_AGENT_CONFIG_FILE = pathlib.Path(
    os.environ.get(
        "OFFDESK_REMOTE_OPERATOR_AGENT_CONFIG",
        str(pathlib.Path(os.environ.get("XDG_CONFIG_HOME", pathlib.Path.home() / ".config")) / "forager" / "config.toml"),
    )
)
AGENT_INTENT_SCHEMA = "telegram_agent_intent.v1"
SESSION_MESSAGE_MIN_CONFIDENCE = 0.8
AGENT_CHAT_TEXT_MAX_CHARS = 4000
DEFAULT_AGENT_BASE_URLS = (
    *default_ollama_base_urls(),
)
DEFAULT_AGENT_MODEL_CANDIDATES = DEFAULT_CODING_MODEL_CANDIDATES


def arg_was_provided(flag: str) -> bool:
    return any(raw == flag or raw.startswith(flag + "=") for raw in sys.argv[1:])


KNOWN_COMMAND_TOKENS = frozenset(CORE_OR_SLASH_COMMANDS) | frozenset(SESSION_INPUT_COMMANDS)
# A slash command mention: "/word" at a token start, not followed by more path
# segments, so filesystem paths like /home/user stay untouched.
SLASH_COMMAND_MENTION = re.compile(r"(?<![\w/.~-])/([A-Za-z][A-Za-z0-9_-]*)(?![A-Za-z0-9_/-])")


def scrub_unknown_commands(text: str | None) -> str | None:
    """Replace hallucinated slash commands in model output with /help.

    The local model occasionally invents commands (e.g. /list, /projects); the
    operator then types them and hits unsupported_remote_operator_command. Any
    slash mention outside the real command surface is rewritten to /help.
    """

    if not text:
        return text

    def replace(match: re.Match[str]) -> str:
        name = match.group(1).lower().replace("-", "_")
        if name in KNOWN_COMMAND_TOKENS:
            return match.group(0)
        return "/help"

    return SLASH_COMMAND_MENTION.sub(replace, text)


def classify_feedback_kind(text: str) -> str:
    normalized = str(text or "").strip().lower()
    planning_markers = (
        "자율주행",
        "야간주행",
        "야간 주행",
        "밤샘",
        "overnight",
        "night run",
        "계획",
        "plan",
        "offdesk",
        "진행",
        "처리",
        "검토해볼까",
        "시작",
        "맡기",
    )
    if any(marker in normalized for marker in planning_markers):
        return "planning_request"
    return "freeform_feedback"


def looks_like_session_relay_request(
    text: str, *, has_session_context: bool = False
) -> bool:
    normalized = str(text or "").strip().lower()
    target_markers = ("codex", "claude", "에이전트", "세션", "코덱스", "클로드")
    relay_markers = (
        "전달",
        "전해",
        "말해",
        "시켜",
        "지시",
        "계속하라고",
        "tell ",
        "ask ",
        "send ",
    )
    explicit = any(marker in normalized for marker in target_markers) and any(
        marker in normalized for marker in relay_markers
    )
    followup_markers = (
        "계속",
        "진행",
        "해줘",
        "수정",
        "확인해",
        "돌려",
        "알려줘",
        "continue",
        "proceed",
    )
    return explicit or (
        has_session_context
        and any(marker in normalized for marker in followup_markers)
    )


def resolve_agent_config(args: Any) -> dict[str, Any]:
    try:
        return resolve_provider_config(
            config_file=args.agent_config_file,
            section_paths=(
                ("offdesk", "remote_operator", "agent"),
                ("remote_operator", "agent"),
                ("remote_operator", "telegram", "agent"),
                ("offdesk", "llm", "provider"),
                ("llm", "provider"),
            ),
            mode=str(args.agent_intent_mode or "auto"),
            mode_explicit=arg_was_provided("--agent-intent-mode"),
            provider=args.agent_provider,
            provider_explicit=arg_was_provided("--agent-provider"),
            base_urls=args.agent_base_url,
            models=args.agent_model,
            model_candidates=csv_values(args.agent_model_candidates)
            + list(DEFAULT_AGENT_MODEL_CANDIDATES),
            timeout_sec=int(args.agent_timeout_sec),
            timeout_explicit=arg_was_provided("--agent-timeout-sec"),
            num_ctx=int(args.agent_num_ctx),
            num_ctx_explicit=arg_was_provided("--agent-num-ctx"),
            num_predict=int(args.agent_num_predict),
            num_predict_explicit=arg_was_provided("--agent-num-predict"),
            env_mode_key="OFFDESK_REMOTE_OPERATOR_AGENT_INTENT_MODE",
            env_provider_key="OFFDESK_REMOTE_OPERATOR_AGENT_PROVIDER",
            env_base_url_keys=(
                "OFFDESK_REMOTE_OPERATOR_AGENT_BASE_URL",
                "OFFDESK_LLM_BASE_URL",
                "OLLAMA_BASE_URL",
            ),
            env_model_keys=(
                "OFFDESK_REMOTE_OPERATOR_AGENT_MODELS",
                "OFFDESK_LLM_MODELS",
                "OFFDESK_OLLAMA_MODEL",
                "OFFDESK_LLM_MODEL",
            ),
            env_timeout_key="OFFDESK_REMOTE_OPERATOR_AGENT_TIMEOUT_SEC",
            env_num_ctx_key="OFFDESK_REMOTE_OPERATOR_AGENT_NUM_CTX",
            env_num_predict_key="OFFDESK_REMOTE_OPERATOR_AGENT_NUM_PREDICT",
            default_provider="ollama",
            default_base_urls=list(DEFAULT_AGENT_BASE_URLS),
            default_models=list(DEFAULT_AGENT_MODEL_CANDIDATES),
        )
    except LlmProviderError as error:
        raise RemoteOperatorTelegramError(str(error)) from error


def select_agent_runtime(agent_config: dict[str, Any]) -> dict[str, Any] | None:
    try:
        return select_llm_provider_runtime(agent_config)
    except LlmProviderError as error:
        raise RemoteOperatorTelegramError(str(error)) from error


def build_agent_intent_prompt(
    *,
    feedback_text: str,
    deterministic_feedback_kind: str,
    feedback_context: dict[str, Any] | None,
) -> str:
    context = feedback_context if isinstance(feedback_context, dict) else {}
    payload = {
        "telegram_text": sanitize_text(feedback_text, max_chars=1200),
        "deterministic_hint": deterministic_feedback_kind,
        "last_interaction_context": context,
    }
    return "\n".join(
        [
            "You are the Telegram intent classifier for a generic Offdesk remote operator harness.",
            "Classify the operator's freeform Telegram message. You are not allowed to approve, launch, dispatch, run shell commands, mutate files, resolve approvals, or retarget providers.",
            "Return exactly one JSON object. Do not include markdown.",
            "Allowed intent values: feedback, plan_request, execution_request, approval_attempt, unsafe_mutation, clarification, unknown.",
            "Use feedback_kind=planning_request only when the text should become a Plan Mode candidate. Otherwise use feedback_kind=freeform_feedback.",
            "If execution is requested, classify intent as execution_request but do not imply authorization.",
            "When you set requires_clarification=true, write clarifying_question in the same language as telegram_text and keep it short enough for a mobile chat card.",
            "For ordinary freeform feedback, write assistant_reply as a direct conversational answer in the same language as telegram_text. Keep it short, useful, and read-only.",
            "JSON schema:",
            json.dumps(
                {
                    "intent": "feedback",
                    "feedback_kind": "freeform_feedback",
                    "confidence": 0.0,
                    "project_hint": None,
                    "goal": None,
                    "timebox": None,
                    "requires_clarification": False,
                    "clarifying_question": None,
                    "assistant_reply": "short read-only reply for the operator",
                    "reason": "short reason",
                    "non_authorized": [
                        "execution",
                        "approval",
                        "shell",
                        "git mutation",
                    ],
                },
                ensure_ascii=False,
            ),
            "Input:",
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
        ]
    )


def build_agent_chat_prompt(
    *,
    chat_text: str,
    feedback_context: dict[str, Any] | None,
    chat_history: list[dict[str, Any]] | None = None,
    operator_snapshot: dict[str, Any] | None = None,
    tool_results: list[dict[str, Any]] | None = None,
    tool_calls_left: int = 0,
) -> str:
    context = feedback_context if isinstance(feedback_context, dict) else {}
    history = [
        {
            "role": str(entry.get("role") or ""),
            "text": sanitize_text(str(entry.get("text") or ""), max_chars=400),
        }
        for entry in (chat_history or [])
        if isinstance(entry, dict) and str(entry.get("text") or "").strip()
    ]
    conversation = {
        "telegram_text": sanitize_text(chat_text, max_chars=AGENT_CHAT_TEXT_MAX_CHARS),
        "last_interaction_context": context,
        "recent_chat_history": history[-8:],
        "tool_results": tool_results or [],
    }
    ground_truth = {
        "operator_snapshot": operator_snapshot if isinstance(operator_snapshot, dict) else {},
        "supported_commands": [
            {"usage": usage, "desc": desc} for usage, desc, _group in COMMAND_SURFACE
        ],
    }
    if tool_calls_left > 0:
        tool_budget_line = (
            f"You have {tool_calls_left} tool calls left for this message. Use one only when "
            "the ground truth and tool_results do not already answer the operator."
        )
    else:
        tool_budget_line = (
            "You have 0 tool calls left. You MUST finish now with 'answer' or 'propose_plan'."
        )
    return "\n".join(
        [
            "You are the Telegram chat assistant for a generic Offdesk remote operator harness.",
            "Work out what the operator needs, use the read-only tools when the ground truth is not enough, then finish with exactly one terminal action. Keep the final answer short, useful, and in the same language as telegram_text.",
            "recent_chat_history lists earlier turns in this Telegram chat, oldest first. Use it ONLY to resolve follow-up questions and pronouns; telegram_text is the message to answer now.",
            "Never repeat one of your earlier replies verbatim. If the operator follows up on the same topic, add new detail, answer the follow-up directly, or state plainly that you have nothing new to add.",
            "You may inspect state and may deliver an operator message to one live supervised agent session. You are not allowed to approve, launch, dispatch, run shell commands, mutate files directly, resolve approvals, or retarget providers.",
            "Respond with EXACTLY ONE JSON object per turn, no markdown. Four forms:",
            '{"action": "tool", "tool": "workspace_overview | list_dir | read_file | service_probe", "project": "<registered project key>", "path": "<relative path, list_dir/read_file only>", "port": 8771, "reason": "short"}',
            '{"action": "answer", "assistant_reply": "short direct read-only reply", "confidence": 0.9, "requires_clarification": false, "clarifying_question": null, "reason": "short"}',
            '{"action": "propose_plan", "delegation_goal": "the request restated as ONE actionable goal, same language", "assistant_reply": "one-line acknowledgment", "confidence": 0.9, "reason": "short"}',
            '{"action": "send_agent", "session_id": "exact id from live_agent_sessions", "message": "only the instruction or answer to type into that agent", "assistant_reply": "one-line delivery acknowledgment", "confidence": 0.9, "reason": "short"}',
            "Terminal action guide: 'send_agent' when the operator clearly asks an existing local agent to do, continue, change, check, or receive something, or directly answers the prompt of conversation_session. Use the exact session id from live_agent_sessions. Strip transport wording such as 'tell Codex' and put only the intended agent message in message. If more than one session could match, you MUST use 'answer' with requires_clarification=true and name the candidates. Never choose the first session from a list merely because its tool is codex or claude. A question ABOUT an agent ('지금 뭐 처리하고 있어?') is 'answer', not 'send_agent'. Use 'propose_plan' only when the operator delegates new harness work without selecting an existing live session. The harness shows a confirm card for delegation_goal; never tell the operator to type /plan. When you inspected files first, fold what you found into delegation_goal. Use 'answer' for status questions, opinions, and small talk.",
            "Read-only tools (run against a registered project's working tree):",
            "- workspace_overview(project): git branch, dirty files, recent commits, key document timestamps, recently modified files",
            "- list_dir(project, path): entries of one directory ('' or omitted = project root)",
            "- read_file(project, path): first 4000 characters of one text file",
            "- service_probe(port): current local TCP LISTEN observation; use this for claims that a local port or service is up/down",
            tool_budget_line,
            "Use tools when the operator asks about local files or current project contents, or wants a diagnosis before planning; skip them when operator_snapshot/project_focus already answer the question. When no project is named, use the project_focus project; when there is none either, finish with 'answer' and requires_clarification=true asking which registered project they mean.",
            "When comparing projects, you may call tools on different projects across rounds; do not compare from memory when one side lacks ground truth.",
            "You have NO web access and NO external data sources. send_agent delivers text to an already running local agent; it does not run a shell command itself. When no live session can receive the request, say so plainly and offer a plan candidate instead.",
            "tool_results in the conversation input lists what your tool calls THIS message returned, oldest first. Trust them over recent_chat_history.",
            "The operator's wording is a request or report, not observed ground truth. Never turn 'the service is down' in telegram_text into a confirmed state unless service_probe or operator_snapshot independently observes it. Label an unverified operator report as unverified.",
            "Conversation input:",
            json.dumps(conversation, ensure_ascii=False, sort_keys=True),
            "CURRENT GROUND TRUTH (read this last, trust it over everything above):",
            "operator_snapshot is the live read-only workstation state as of THIS message: attention counts, health, open decisions, running-capacity, registered_projects (key, display name, wiki profile), workspace_projects (folder hints), and autonomy_armed.",
            "Every number or state claim in your reply MUST come from operator_snapshot or tool_results, never from recent_chat_history. If history mentions counts that the snapshot no longer shows, the snapshot is right and the old counts are resolved.",
            "When the operator names a project, resolve it against registered_projects keys and display names first; unregistered folders are context, not managed projects.",
            "project_focus, when present, is the live state of the ONE project this conversation is about (focus_source 'mention' = named in this message, 'sticky' = carried over from an earlier message): its sessions with tool and status, session_counts, and its adaptive-wiki candidate_count, promoted_count, recent_candidates, and recent_claims. Answer project questions from these concrete facts, never with a generic 'the project is registered' line.",
            "live_agent_sessions is the complete current set eligible for text delivery. conversation_session, when present, is the session selected by a replied-to waiting card or the last successful delivery in this chat. Prefer it for pronouns and direct follow-up instructions, but never use it if it is absent from live_agent_sessions.",
            "supported_commands is the COMPLETE slash-command surface. Never mention, suggest, or invent a slash command that is not listed there.",
            json.dumps(ground_truth, ensure_ascii=False, sort_keys=True),
        ]
    )


def call_ollama_intent_agent(runtime: dict[str, Any], prompt: str) -> dict[str, Any]:
    return call_ollama_json(runtime, prompt, temperature=0.1)


def clamp_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = default
    return max(0.0, min(1.0, parsed))


def short_optional_text(value: Any, max_chars: int = 240) -> str | None:
    text = sanitize_text(str(value or "").strip(), max_chars=max_chars)
    return text or None


def normalize_agent_intent(
    parsed: dict[str, Any],
    *,
    runtime: dict[str, Any],
    deterministic_feedback_kind: str,
) -> dict[str, Any]:
    allowed_intents = {
        "feedback",
        "plan_request",
        "execution_request",
        "approval_attempt",
        "unsafe_mutation",
        "clarification",
        "unknown",
    }
    intent = str(parsed.get("intent") or "").strip().lower()
    if intent not in allowed_intents:
        intent = "unknown"
    requested_kind = str(parsed.get("feedback_kind") or "").strip()
    if requested_kind not in {"freeform_feedback", "planning_request"}:
        requested_kind = (
            "planning_request"
            if intent in {"plan_request", "execution_request"}
            else deterministic_feedback_kind
        )
    non_authorized = unique_nonempty(
        list(parsed.get("non_authorized") if isinstance(parsed.get("non_authorized"), list) else [])
        + ["execution", "approval", "shell", "git mutation"]
    )
    return {
        "schema": AGENT_INTENT_SCHEMA,
        "status": "classified",
        "source": "ollama",
        "provider": runtime.get("provider"),
        "base_url": runtime.get("base_url"),
        "model": runtime.get("model"),
        "intent": intent,
        "feedback_kind": requested_kind,
        "confidence": clamp_float(parsed.get("confidence")),
        "project_hint": short_optional_text(parsed.get("project_hint"), max_chars=120),
        "goal": short_optional_text(parsed.get("goal"), max_chars=240),
        "timebox": short_optional_text(parsed.get("timebox"), max_chars=120),
        "requires_clarification": bool(parsed.get("requires_clarification")),
        "clarifying_question": scrub_unknown_commands(
            short_optional_text(parsed.get("clarifying_question"), max_chars=240)
        ),
        "assistant_reply": scrub_unknown_commands(
            short_optional_text(parsed.get("assistant_reply"), max_chars=260)
        ),
        "reason": short_optional_text(parsed.get("reason"), max_chars=240),
        "non_authorized": non_authorized,
        "config_sources": list(runtime.get("config_sources") or []),
    }


def fallback_agent_intent(
    *,
    reason: str,
    deterministic_feedback_kind: str,
    agent_config: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema": AGENT_INTENT_SCHEMA,
        "status": "fallback",
        "source": "deterministic",
        "reason": sanitize_text(reason, max_chars=240),
        "intent": "plan_request"
        if deterministic_feedback_kind == "planning_request"
        else "feedback",
        "feedback_kind": deterministic_feedback_kind,
        "confidence": 0.25,
        "assistant_reply": None,
        "provider": agent_config.get("provider"),
        "configured_models": list(agent_config.get("models") or [])[:4],
        "non_authorized": ["execution", "approval", "shell", "git mutation"],
    }


def fallback_agent_chat(*, reason: str, agent_config: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema": AGENT_INTENT_SCHEMA,
        "status": "fallback",
        "source": "deterministic",
        "reason": sanitize_text(reason, max_chars=240),
        "intent": "chat",
        "feedback_kind": "chat",
        "confidence": 0.0,
        "assistant_reply": None,
        "provider": agent_config.get("provider"),
        "configured_models": list(agent_config.get("models") or [])[:4],
        "non_authorized": ["execution", "approval", "shell", "git mutation"],
    }


def normalize_agent_chat(parsed: dict[str, Any], *, runtime: dict[str, Any]) -> dict[str, Any]:
    # The chat agent owns the routing decision. Terminal action
    # 'propose_plan' maps to the delegate_work intent that triggers the
    # plan-capture confirm card. 'send_agent' maps to a text delivery request.
    # Everything else, including older model output, degrades to plain chat.
    action = str(parsed.get("action") or "").strip().lower()
    if action == "propose_plan":
        intent = "delegate_work"
    elif action == "send_agent":
        intent = "session_message"
    elif action == "answer":
        intent = "chat"
    else:
        intent = str(parsed.get("intent") or "").strip().lower()
        if intent not in {"delegate_work", "session_message"}:
            intent = "chat"
    non_authorized_defaults = ["approval", "shell", "git mutation"]
    if intent != "session_message":
        non_authorized_defaults.append("execution")
    non_authorized = unique_nonempty(
        list(parsed.get("non_authorized") if isinstance(parsed.get("non_authorized"), list) else [])
        + non_authorized_defaults
    )
    return {
        "schema": AGENT_INTENT_SCHEMA,
        "status": "classified",
        "source": "ollama",
        "provider": runtime.get("provider"),
        "base_url": runtime.get("base_url"),
        "model": runtime.get("model"),
        "intent": intent,
        "delegation_goal": short_optional_text(parsed.get("delegation_goal"), max_chars=380)
        if intent == "delegate_work"
        else None,
        "session_id": short_optional_text(parsed.get("session_id"), max_chars=64)
        if intent == "session_message"
        else None,
        "session_message": short_optional_text(parsed.get("message"), max_chars=2000)
        if intent == "session_message"
        else None,
        "feedback_kind": "chat",
        "confidence": clamp_float(parsed.get("confidence")),
        "requires_clarification": bool(parsed.get("requires_clarification")),
        "clarifying_question": scrub_unknown_commands(
            short_optional_text(parsed.get("clarifying_question"), max_chars=240)
        ),
        "assistant_reply": scrub_unknown_commands(
            short_optional_text(parsed.get("assistant_reply"), max_chars=ASSISTANT_REPLY_MAX_CHARS)
        ),
        "reason": short_optional_text(parsed.get("reason"), max_chars=240),
        "non_authorized": non_authorized,
        "config_sources": list(runtime.get("config_sources") or []),
    }


def _session_alias_is_mentioned(text: str, alias: Any) -> bool:
    candidate = str(alias or "").strip().lower()
    if len(candidate) < 2:
        return False
    normalized = str(text or "").lower()
    if candidate.isascii():
        pattern = rf"(?<![a-z0-9]){re.escape(candidate)}(?![a-z0-9])"
        return re.search(pattern, normalized) is not None
    return candidate in normalized


def _session_candidate_label(session: dict[str, Any]) -> str:
    project = str(session.get("project") or "미등록")
    title = str(session.get("title") or session.get("tool") or "agent")
    tool = str(session.get("tool") or "agent")
    return sanitize_text(f"{project} · {title} ({tool})", max_chars=100)


def _session_message_clarification(
    intent: dict[str, Any],
    *,
    status: str,
    candidates: list[dict[str, Any]],
    question: str | None = None,
) -> dict[str, Any]:
    labels = [_session_candidate_label(session) for session in candidates[:5]]
    if not question:
        if labels:
            question = "어느 로컬 에이전트인가요? " + ", ".join(labels)
        else:
            question = "대상 프로젝트와 로컬 에이전트 이름을 함께 알려주세요."
    return {
        **intent,
        "intent": "chat",
        "requested_session_id": intent.get("session_id"),
        "session_id": None,
        "session_message": None,
        "session_target_status": status,
        "session_target_candidates": [
            {
                "id": str(session.get("id") or ""),
                "project": str(session.get("project") or ""),
                "title": str(session.get("title") or ""),
                "tool": str(session.get("tool") or ""),
            }
            for session in candidates[:5]
        ],
        "requires_clarification": True,
        "clarifying_question": sanitize_text(question, max_chars=240),
        "assistant_reply": sanitize_text(question, max_chars=240),
        "reason": status,
    }


def validate_session_message_intent(
    intent: dict[str, Any],
    *,
    chat_text: str,
    operator_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    """Fail closed unless one deterministic live session matches the request."""

    if intent.get("status") != "classified" or intent.get("intent") != "session_message":
        return intent

    snapshot = operator_snapshot if isinstance(operator_snapshot, dict) else {}
    sessions = [
        session
        for session in snapshot.get("live_agent_sessions") or []
        if isinstance(session, dict) and str(session.get("id") or "")
    ]
    if bool(intent.get("requires_clarification")):
        return _session_message_clarification(
            intent,
            status="model_requires_clarification",
            candidates=sessions,
            question=str(intent.get("clarifying_question") or "") or None,
        )
    if clamp_float(intent.get("confidence")) < SESSION_MESSAGE_MIN_CONFIDENCE:
        return _session_message_clarification(
            intent,
            status="session_target_low_confidence",
            candidates=sessions,
        )
    if not str(intent.get("session_message") or "").strip():
        return _session_message_clarification(
            intent,
            status="session_message_empty",
            candidates=sessions,
            question="로컬 에이전트에 전달할 내용을 다시 알려주세요.",
        )
    if not sessions:
        return _session_message_clarification(
            intent,
            status="no_live_agent_sessions",
            candidates=[],
            question="현재 입력을 받을 수 있는 로컬 에이전트 세션이 없습니다.",
        )

    candidates = sessions
    conversation = snapshot.get("conversation_session")
    conversation = conversation if isinstance(conversation, dict) else None
    focus = snapshot.get("project_focus")
    focus = focus if isinstance(focus, dict) else None
    focus_source = str((focus or {}).get("focus_source") or "")
    focus_key = str((focus or {}).get("key") or "")

    if conversation and str(conversation.get("source") or "") == "reply_card":
        conversation_id = str(conversation.get("id") or "")
        candidates = [session for session in sessions if str(session.get("id") or "") == conversation_id]
    elif focus_source == "mention" and focus_key:
        candidates = [session for session in sessions if str(session.get("project") or "") == focus_key]
    elif conversation:
        conversation_id = str(conversation.get("id") or "")
        candidates = [session for session in sessions if str(session.get("id") or "") == conversation_id]

    if len(candidates) > 1:
        title_matches = [
            session
            for session in candidates
            if _session_alias_is_mentioned(chat_text, session.get("title"))
        ]
        if title_matches:
            candidates = title_matches
        else:
            tool_matches = [
                session
                for session in candidates
                if _session_alias_is_mentioned(chat_text, session.get("tool"))
            ]
            if tool_matches:
                candidates = tool_matches

    if len(candidates) != 1:
        return _session_message_clarification(
            intent,
            status="session_target_ambiguous" if candidates else "session_target_not_found",
            candidates=candidates,
        )

    selected = candidates[0]
    selected_id = str(selected.get("id") or "")
    if str(intent.get("session_id") or "") != selected_id:
        return _session_message_clarification(
            intent,
            status="model_session_target_mismatch",
            candidates=candidates,
            question=(
                f"대상은 {_session_candidate_label(selected)}로 보입니다. "
                "그 세션에 전달할까요?"
            ),
        )

    return {
        **intent,
        "session_id": selected_id,
        "session_target_status": "resolved",
        "session_target_candidates": [
            {
                "id": selected_id,
                "project": str(selected.get("project") or ""),
                "title": str(selected.get("title") or ""),
                "tool": str(selected.get("tool") or ""),
            }
        ],
        "requires_clarification": False,
    }


MAX_CHAT_TOOL_CALLS = 3


def extract_tool_request(parsed: dict[str, Any]) -> dict[str, Any] | None:
    """Return a normalized tool request when the model asked for one."""

    action = str(parsed.get("action") or "").strip().lower()
    if action == "tool":
        return {
            "tool": str(parsed.get("tool") or "").strip().lower(),
            "project": str(parsed.get("project") or "").strip(),
            "path": str(parsed.get("path") or "").strip(),
            "port": parsed.get("port"),
        }
    # Legacy shape from the previous prompt generation.
    if str(parsed.get("intent") or "").strip().lower() == "inspect_project":
        return {"tool": "workspace_overview", "project": "", "path": ""}
    return None


def chat_with_agent(
    args: Any,
    chat_text: str,
    *,
    feedback_context: dict[str, Any] | None = None,
    chat_history: list[dict[str, Any]] | None = None,
    operator_snapshot: dict[str, Any] | None = None,
    tool_executor: Any = None,
    initial_tool_results: list[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    """Agentic chat loop: the model may call read-only tools before finishing.

    tool_executor is a callable(dict) -> dict provided by the adapter; it runs
    one read-only tool request and returns its result. Without it the model
    gets no tool budget and must answer in one shot.
    """

    agent_config = resolve_agent_config(args)
    if agent_config.get("mode") == "off":
        return fallback_agent_chat(reason="local_agent_disabled", agent_config=agent_config)
    runtime = select_agent_runtime(agent_config)
    if not runtime:
        return fallback_agent_chat(reason="local_agent_unavailable", agent_config=agent_config)
    tool_results: list[dict[str, Any]] = list(initial_tool_results or [])
    tools_used: list[dict[str, Any]] = []
    for item in tool_results:
        request = item.get("request") if isinstance(item, dict) else {}
        outcome = item.get("result") if isinstance(item, dict) else {}
        tools_used.append(
            {
                "tool": request.get("tool") if isinstance(request, dict) else None,
                "project": request.get("project") or None
                if isinstance(request, dict)
                else None,
                "path": request.get("path") or None if isinstance(request, dict) else None,
                "port": request.get("port") if isinstance(request, dict) else None,
                "status": outcome.get("status") if isinstance(outcome, dict) else None,
            }
        )
    while True:
        calls_left = (
            MAX_CHAT_TOOL_CALLS - len(tool_results) if callable(tool_executor) else 0
        )
        prompt = build_agent_chat_prompt(
            chat_text=chat_text,
            feedback_context=feedback_context,
            chat_history=chat_history,
            operator_snapshot=operator_snapshot,
            tool_results=tool_results,
            tool_calls_left=calls_left,
        )
        try:
            parsed = call_ollama_intent_agent(runtime, prompt)
        except (OSError, TimeoutError, urllib.error.URLError, json.JSONDecodeError, ValueError) as error:
            if agent_config.get("mode") == "required":
                raise RemoteOperatorTelegramError(f"local agent chat failed: {error}") from error
            return fallback_agent_chat(
                reason=f"local_agent_failed:{type(error).__name__}",
                agent_config=agent_config,
            )
        request = extract_tool_request(parsed)
        if request and callable(tool_executor) and calls_left > 0:
            try:
                outcome = tool_executor(request)
            except Exception as error:  # noqa: BLE001 - poll loop must never crash here
                outcome = {"status": "tool_failed", "error": str(error)[:120]}
            if not isinstance(outcome, dict):
                outcome = {"status": "tool_failed"}
            tool_results.append({"request": request, "result": outcome})
            tools_used.append(
                {
                    "tool": request.get("tool"),
                    "project": request.get("project") or None,
                    "path": request.get("path") or None,
                    "port": request.get("port"),
                    "status": outcome.get("status"),
                }
            )
            continue
        normalized = normalize_agent_chat(parsed, runtime=runtime)
        normalized["tool_rounds"] = len(tool_results)
        normalized["tools_used"] = tools_used
        return normalized


def classify_feedback_with_agent(
    args: Any,
    feedback_text: str,
    *,
    feedback_context: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    deterministic_feedback_kind = classify_feedback_kind(feedback_text)
    agent_config = resolve_agent_config(args)
    if agent_config.get("mode") == "off":
        return None
    runtime = select_agent_runtime(agent_config)
    if not runtime:
        return fallback_agent_intent(
            reason="local_agent_unavailable",
            deterministic_feedback_kind=deterministic_feedback_kind,
            agent_config=agent_config,
        )
    prompt = build_agent_intent_prompt(
        feedback_text=feedback_text,
        deterministic_feedback_kind=deterministic_feedback_kind,
        feedback_context=feedback_context,
    )
    try:
        parsed = call_ollama_intent_agent(runtime, prompt)
    except (OSError, TimeoutError, urllib.error.URLError, json.JSONDecodeError, ValueError) as error:
        if agent_config.get("mode") == "required":
            raise RemoteOperatorTelegramError(f"local agent intent classification failed: {error}") from error
        return fallback_agent_intent(
            reason=f"local_agent_failed:{type(error).__name__}",
            deterministic_feedback_kind=deterministic_feedback_kind,
            agent_config=agent_config,
        )
    return normalize_agent_intent(
        parsed,
        runtime=runtime,
        deterministic_feedback_kind=deterministic_feedback_kind,
    )


def agent_runtime_status(args: Any) -> dict[str, Any]:
    try:
        return provider_status(resolve_agent_config(args))
    except RemoteOperatorTelegramError as error:
        return {
            "schema": "offdesk_llm_provider_resolution.v1",
            "status": "error",
            "error": sanitize_text(str(error), max_chars=240),
        }
