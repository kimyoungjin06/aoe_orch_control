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
        "telegram_text": sanitize_text(chat_text, max_chars=1200),
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
            "You are read-only. You are not allowed to approve, launch, dispatch, run shell commands, mutate files, resolve approvals, or retarget providers.",
            "Respond with EXACTLY ONE JSON object per turn, no markdown. Three forms:",
            '{"action": "tool", "tool": "workspace_overview | list_dir | read_file", "project": "<registered project key>", "path": "<relative path, list_dir/read_file only>", "reason": "short"}',
            '{"action": "answer", "assistant_reply": "short direct read-only reply", "confidence": 0.9, "requires_clarification": false, "clarifying_question": null, "reason": "short"}',
            '{"action": "propose_plan", "delegation_goal": "the request restated as ONE actionable goal, same language", "assistant_reply": "one-line acknowledgment", "confidence": 0.9, "reason": "short"}',
            "Terminal action guide: 'propose_plan' when the operator hands the harness work to do -- plan, build, fix, diagnose-and-replan ('~해줘', '~해볼까', '~하자', '계획 세워'). The harness shows a confirm card capturing delegation_goal as a plan candidate; never tell the operator to type /plan. When you inspected files first, fold what you found into delegation_goal so the plan reflects the actual current state. 'answer' for everything else: questions, status checks, opinions, small talk. A question ABOUT work ('지금 뭐 처리하고 있어?') is an 'answer', even when it contains words like 계획/처리/진행.",
            "Read-only tools (run against a registered project's working tree):",
            "- workspace_overview(project): git branch, dirty files, recent commits, key document timestamps, recently modified files",
            "- list_dir(project, path): entries of one directory ('' or omitted = project root)",
            "- read_file(project, path): first 4000 characters of one text file",
            tool_budget_line,
            "Use tools when the operator asks about local files or current project contents, or wants a diagnosis before planning; skip them when operator_snapshot/project_focus already answer the question. When no project is named, use the project_focus project; when there is none either, finish with 'answer' and requires_clarification=true asking which registered project they mean.",
            "When comparing projects, you may call tools on different projects across rounds; do not compare from memory when one side lacks ground truth.",
            "You have NO web access, NO external data sources, and you cannot run code or commands. When the operator asks for something none of your tools or supported_commands cover, say plainly that this chat cannot do it and name the nearest supported alternative (e.g. capture it as a plan candidate); never imply you could do it with more detail.",
            "tool_results in the conversation input lists what your tool calls THIS message returned, oldest first. Trust them over recent_chat_history.",
            "Conversation input:",
            json.dumps(conversation, ensure_ascii=False, sort_keys=True),
            "CURRENT GROUND TRUTH (read this last, trust it over everything above):",
            "operator_snapshot is the live read-only workstation state as of THIS message: attention counts, health, open decisions, running-capacity, registered_projects (key, display name, wiki profile), workspace_projects (folder hints), and autonomy_armed.",
            "Every number or state claim in your reply MUST come from operator_snapshot or tool_results, never from recent_chat_history. If history mentions counts that the snapshot no longer shows, the snapshot is right and the old counts are resolved.",
            "When the operator names a project, resolve it against registered_projects keys and display names first; unregistered folders are context, not managed projects.",
            "project_focus, when present, is the live state of the ONE project this conversation is about (focus_source 'mention' = named in this message, 'sticky' = carried over from an earlier message): its sessions with tool and status, session_counts, and its adaptive-wiki entry_count plus recent_claims. Answer project questions from these concrete facts, never with a generic 'the project is registered' line.",
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
    non_authorized = unique_nonempty(
        list(parsed.get("non_authorized") if isinstance(parsed.get("non_authorized"), list) else [])
        + ["execution", "approval", "shell", "git mutation"]
    )
    # The chat agent owns the routing decision. Terminal action
    # 'propose_plan' maps to the delegate_work intent that triggers the
    # plan-capture confirm card; everything else (including the legacy
    # intent-form output of older models) degrades to plain chat.
    action = str(parsed.get("action") or "").strip().lower()
    if action == "propose_plan":
        intent = "delegate_work"
    elif action == "answer":
        intent = "chat"
    else:
        intent = str(parsed.get("intent") or "").strip().lower()
        if intent != "delegate_work":
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


MAX_CHAT_TOOL_CALLS = 3


def extract_tool_request(parsed: dict[str, Any]) -> dict[str, str] | None:
    """Return a normalized tool request when the model asked for one."""

    action = str(parsed.get("action") or "").strip().lower()
    if action == "tool":
        return {
            "tool": str(parsed.get("tool") or "").strip().lower(),
            "project": str(parsed.get("project") or "").strip(),
            "path": str(parsed.get("path") or "").strip(),
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
    tool_results: list[dict[str, Any]] = []
    tools_used: list[dict[str, Any]] = []
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
