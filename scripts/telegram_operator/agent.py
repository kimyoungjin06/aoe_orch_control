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
        "telegram_text": sanitize_text(chat_text, max_chars=1200),
        "last_interaction_context": context,
        "recent_chat_history": history[-8:],
    }
    ground_truth = {
        "operator_snapshot": operator_snapshot if isinstance(operator_snapshot, dict) else {},
        "supported_commands": [
            {"usage": usage, "desc": desc} for usage, desc, _group in COMMAND_SURFACE
        ],
    }
    return "\n".join(
        [
            "You are the Telegram chat assistant for a generic Offdesk remote operator harness.",
            "Answer the operator's plain Telegram message directly. Keep the answer short, useful, and in the same language as telegram_text.",
            "recent_chat_history lists earlier turns in this Telegram chat, oldest first. Use it ONLY to resolve follow-up questions and pronouns; telegram_text is the message to answer now.",
            "Never repeat one of your earlier replies verbatim. If the operator follows up on the same topic, add new detail, answer the follow-up directly, or state plainly that you have nothing new to add.",
            "You are read-only. You are not allowed to approve, launch, dispatch, run shell commands, mutate files, resolve approvals, or retarget providers.",
            "FIRST decide what the operator's message IS, and return that decision as intent:",
            "intent 'delegate_work': the operator is handing the harness work to do -- plan, review, investigate, build, fix, run something ('~해줘', '~해볼까', '~하자', imperatives, requests). Set delegation_goal to their request restated as ONE actionable goal in the same language. The harness will show a confirm card that captures it as a plan candidate; do NOT tell the operator to type /plan and do NOT answer with inspection commands. assistant_reply may be a one-line acknowledgment.",
            "intent 'inspect_project': the operator wants you to actually look at a project's CURRENT local state -- files, git, recent changes ('로컬 상태 확인해', '파일 좀 봐줘', '새로 에이전트 만들어서 확인해') -- and the answer is not already in operator_snapshot. The harness will run a read-only workspace inspection and call you again with local_inspection filled; answer from it then. Set delegation_goal to null.",
            "intent 'chat': everything else -- questions, status checks, opinions, thanks, small talk. A question ABOUT work in progress ('지금 뭐 처리하고 있어?') is chat, even when it contains words like 계획/처리/진행. Set delegation_goal to null and answer directly.",
            "For pure inspection questions, recommend the matching supported command instead of asking the operator for file paths or treating the chat as authorization.",
            "Conversation input:",
            json.dumps(conversation, ensure_ascii=False, sort_keys=True),
            "CURRENT GROUND TRUTH (read this last, trust it over everything above):",
            "operator_snapshot is the live read-only workstation state as of THIS message: attention counts, health, open decisions, running-capacity, registered_projects (key, display name, wiki profile), workspace_projects (folder hints), and autonomy_armed.",
            "Every number or state claim in your reply MUST come from operator_snapshot, never from recent_chat_history. If history mentions counts that the snapshot no longer shows, the snapshot is right and the old counts are resolved.",
            "When the operator names a project, resolve it against registered_projects keys and display names first; unregistered folders are context, not managed projects.",
            "project_focus, when present, is the live state of the ONE project this conversation is about (focus_source 'mention' = named in this message, 'sticky' = carried over from an earlier message): its sessions with tool and status, session_counts, and its adaptive-wiki entry_count plus recent_claims. Answer project questions from these concrete facts (which sessions exist, what state they are in, what was recently learned), never with a generic 'the project is registered' line.",
            "When project_focus is absent and the operator asks about 'the project' without naming one, ask which registered project they mean.",
            "local_inspection, when present, is the fresh read-only workspace probe you requested via inspect_project: working-tree path, git branch, dirty files, recent commits, key document timestamps, recently modified files. Answer NOW from it and never return intent inspect_project again in this turn. If local_inspection.status is 'no_project', ask which registered project to inspect; if 'workspace_not_found', say the project folder was not found.",
            "supported_commands is the COMPLETE slash-command surface. Never mention, suggest, or invent a slash command that is not listed there.",
            json.dumps(ground_truth, ensure_ascii=False, sort_keys=True),
            "Return exactly one JSON object. Do not include markdown.",
            "JSON schema:",
            json.dumps(
                {
                    "intent": "chat | delegate_work | inspect_project",
                    "delegation_goal": None,
                    "confidence": 0.0,
                    "requires_clarification": False,
                    "clarifying_question": None,
                    "assistant_reply": "short direct read-only reply for the operator",
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
    non_authorized = unique_nonempty(
        list(parsed.get("non_authorized") if isinstance(parsed.get("non_authorized"), list) else [])
        + ["execution", "approval", "shell", "git mutation"]
    )
    # The chat agent owns the routing decision (function-calling style):
    # 'delegate_work' triggers the plan-capture confirm card, and
    # 'inspect_project' triggers a read-only workspace probe plus a second
    # agent call; anything unrecognized degrades to plain chat.
    intent = str(parsed.get("intent") or "").strip().lower()
    if intent not in {"delegate_work", "inspect_project"}:
        intent = "chat"
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


def chat_with_agent(
    args: Any,
    chat_text: str,
    *,
    feedback_context: dict[str, Any] | None = None,
    chat_history: list[dict[str, Any]] | None = None,
    operator_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    agent_config = resolve_agent_config(args)
    if agent_config.get("mode") == "off":
        return fallback_agent_chat(reason="local_agent_disabled", agent_config=agent_config)
    runtime = select_agent_runtime(agent_config)
    if not runtime:
        return fallback_agent_chat(reason="local_agent_unavailable", agent_config=agent_config)
    prompt = build_agent_chat_prompt(
        chat_text=chat_text,
        feedback_context=feedback_context,
        chat_history=chat_history,
        operator_snapshot=operator_snapshot,
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
    return normalize_agent_chat(parsed, runtime=runtime)


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
