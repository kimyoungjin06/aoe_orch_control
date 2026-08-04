"""Pure Telegram command routing helpers."""

from __future__ import annotations

import shlex
from typing import Any


BUTTON_COMMAND_ALIASES = {
    "상태": "/status",
    "승인 대기": "/pending",
    "전체 승인": "/pending --all",
    "계획": "/plans --latest",
    "도움말": "/help",
}
CORE_BUTTON_LABELS = ("상태", "승인 대기", "계획", "도움말")
CORE_OR_SLASH_COMMANDS = {
    "start",
    "help",
    "guide",
    "qna",
    "usage",
    "commands",
    "status",
    "pending",
    "plans",
    "show",
    "chat",
    "ask",
    "feedback",
    "note",
    "remember",
    "plan",
    "plan_request",
    "decisions",
    "decision",
    "recovery",
    "recover",
    "runtime",
    "dispatch",
    "run",
    "tasks",
    "cancel_task",
    "session_approve",
    "session_deny",
    "pause",
    "resume",
    "attention",
    "confirm",
    "cancel",
}
SESSION_INPUT_COMMANDS = {"select", "choose", "path", "workload", "session_input", "plan_input"}
# One (usage, description, group) row per operator-facing command. This is the
# single source of truth for the /guide reference sheet and for the command
# surface handed to the local chat agent, so keep it in sync with
# parse_remote_command when commands are added or removed.
COMMAND_SURFACE = (
    ("/attention", "한눈 요약 (가장 급한 것부터)", "조회"),
    ("/status", "offdesk 상태 요약", "조회"),
    ("/pending [--all]", "승인 대기 항목", "조회"),
    ("/plans", "계획 목록", "조회"),
    ("/show <id>", "항목 상세 보기", "조회"),
    ("/decisions", "결정 대기함", "조회"),
    ("/recovery", "복구 대기 항목", "조회"),
    ("/runtime", "런타임 디스패치 상태", "조회"),
    ("/tasks", "실행 중 작업 목록", "조회"),
    ("/decision <id> <action> [메모]", "결정 처리 (확인 카드 후 실행)", "실행"),
    ("/recover <id>", "복구 실행", "실행"),
    ("/run [name]", "사전 승인된 명령 목록/실행", "실행"),
    ("/confirm", "대기 중인 확인 승인", "실행"),
    ("/cancel", "대기 중인 확인 취소", "실행"),
    ("/cancel_task <id> [이유]", "작업 취소", "실행"),
    ("/session_approve <id> [hash] [n]", "대기 세션 승인(기본 Enter, n=옵션 번호)", "실행"),
    ("/session_deny <id> [hash]", "대기 세션 거부 키 전송(Esc)", "실행"),
    ("/plan <내용>", "계획 후보로 기록 (로컬 Plan Mode에서 이어감)", "기록"),
    ("/feedback <내용>", "의견 기록", "기록"),
    ("/remember <내용>", "위키 후보로 기록", "기록"),
    ("/pause [이유]", "전체 일시정지", "제어"),
    ("/resume", "재개", "제어"),
    ("/chat <내용>", "에이전트 채팅 (평문과 동일)", "도움말"),
    ("/guide", "전체 사용 안내 (별칭: /qna)", "도움말"),
    ("/help", "짧은 도움말", "도움말"),
)
DISPATCH_BUTTON_ALIASES = {
    "결정 목록": "/decisions",
    "복구 목록": "/recovery",
    "런타임 목록": "/runtime",
    "실행 목록": "/run",
    "확인": "/confirm",
    "취소": "/cancel",
}


def normalize_command_name(raw: str) -> str:
    text = raw.strip()
    if text.startswith("/"):
        text = text[1:]
    if "@" in text:
        text = text.split("@", 1)[0]
    return text.strip().lower().replace("-", "_")


def unsupported_command(command_text: str, reason: str) -> dict[str, Any]:
    return {
        "supported": False,
        "command": None,
        "argv": [],
        "reason": reason,
        "command_text": command_text,
    }


def parse_remote_command(command_text: str) -> dict[str, Any]:
    text = str(command_text or "").strip()
    if not text:
        return unsupported_command(text, "empty_command")
    original_text = text
    alias = BUTTON_COMMAND_ALIASES.get(text) or DISPATCH_BUTTON_ALIASES.get(text)
    if alias:
        text = alias
    if not text.startswith("/"):
        return {
            "supported": True,
            "command": "chat",
            "argv": [],
            "reason": "plain_text_chat",
            "command_text": original_text,
            "chat_text": original_text,
        }
    # /dispatch carries an arbitrary shell command after " -- "; parse it from
    # the raw text so quoting in the command survives shlex tokenization.
    if normalize_command_name(text.split(None, 1)[0]) == "dispatch":
        return parse_dispatch_command(original_text, text)
    try:
        tokens = shlex.split(text)
    except ValueError as error:
        return unsupported_command(original_text, f"parse_error:{error}")
    if not tokens:
        return unsupported_command(original_text, "empty_command")

    command = normalize_command_name(tokens[0])
    args = tokens[1:]
    if command in {"start", "help"}:
        return {"supported": True, "command": "help", "argv": [], "reason": "help"}
    if command in {"guide", "qna", "usage", "commands", "사용법"}:
        return {"supported": True, "command": "guide", "argv": [], "reason": "guide"}
    if command in {"chat", "ask"}:
        chat_text = " ".join(args).strip()
        if not chat_text:
            return unsupported_command(original_text, "chat_requires_text")
        return {
            "supported": True,
            "command": "chat",
            "argv": [],
            "reason": "explicit_chat_command",
            "command_text": original_text,
            "chat_text": chat_text,
        }
    if command in {"feedback", "note"}:
        feedback_text = " ".join(args).strip()
        if not feedback_text:
            return unsupported_command(original_text, f"{command}_requires_text")
        return {
            "supported": True,
            "command": "feedback",
            "argv": [],
            "reason": f"explicit_{command}_command",
            "command_text": original_text,
            "feedback_text": feedback_text,
            "feedback_kind": "freeform_feedback",
        }
    if command == "remember":
        remember_text = " ".join(args).strip()
        if not remember_text:
            return unsupported_command(original_text, "remember_requires_text")
        return {
            "supported": True,
            "command": "remember",
            "argv": [],
            "reason": "explicit_remember_command",
            "command_text": original_text,
            "remember_text": remember_text,
        }
    if command in {"plan", "plan_request"}:
        plan_text = " ".join(args).strip()
        if not plan_text:
            return unsupported_command(original_text, "plan_requires_text")
        return {
            "supported": True,
            "command": "plan_request",
            "argv": [],
            "reason": "explicit_plan_command",
            "command_text": original_text,
            "feedback_text": plan_text,
            "plan_text": plan_text,
            "feedback_kind": "planning_request",
        }
    if command == "status":
        if args:
            return unsupported_command(original_text, "status_accepts_no_arguments")
        return {"supported": True, "command": "status", "argv": ["status"]}
    if command == "pending":
        argv = ["pending"]
        for arg in args:
            if arg == "--all":
                argv.append("--all")
            else:
                return unsupported_command(original_text, f"unsupported_pending_argument:{arg}")
        return {"supported": True, "command": "pending", "argv": argv}
    if command == "plans":
        return parse_plans_command(original_text, args)
    if command == "show":
        return parse_show_command(original_text, args)
    if command == "decisions":
        if args:
            return unsupported_command(original_text, "decisions_accepts_no_arguments")
        return {
            "supported": True,
            "command": "decisions",
            "argv": [],
            "reason": "explicit_decisions_command",
            "command_text": original_text,
        }
    if command == "decision":
        return parse_decision_command(original_text, args)
    if command == "recovery":
        if args:
            return unsupported_command(original_text, "recovery_accepts_no_arguments")
        return {
            "supported": True,
            "command": "recovery",
            "argv": [],
            "reason": "explicit_recovery_command",
            "command_text": original_text,
        }
    if command == "recover":
        return parse_recover_command(original_text, args)
    if command == "runtime":
        if args:
            return unsupported_command(original_text, "runtime_accepts_no_arguments")
        return {
            "supported": True,
            "command": "runtime",
            "argv": [],
            "reason": "explicit_runtime_command",
            "command_text": original_text,
        }
    if command == "run":
        return parse_run_command(original_text, args)
    if command == "tasks":
        if args:
            return unsupported_command(original_text, "tasks_accepts_no_arguments")
        return {
            "supported": True,
            "command": "tasks",
            "argv": [],
            "reason": "explicit_tasks_command",
            "command_text": original_text,
        }
    if command == "attention":
        if args:
            return unsupported_command(original_text, "attention_accepts_no_arguments")
        return {
            "supported": True,
            "command": "attention",
            "argv": [],
            "reason": "explicit_attention_command",
            "command_text": original_text,
        }
    if command == "pause":
        reason = " ".join(args).strip()
        return {
            "supported": True,
            "command": "pause",
            "argv": [],
            "reason": "explicit_pause_command",
            "command_text": original_text,
            "pause_reason": reason,
        }
    if command == "resume":
        if args:
            return unsupported_command(original_text, "resume_accepts_no_arguments")
        return {
            "supported": True,
            "command": "resume",
            "argv": [],
            "reason": "explicit_resume_command",
            "command_text": original_text,
        }
    if command in ("session_approve", "session_deny"):
        if not args or not args[0].strip():
            return unsupported_command(original_text, "session_input_requires_session_id")
        return {
            "supported": True,
            "command": command,
            "argv": [],
            "reason": "explicit_session_input_command",
            "command_text": original_text,
            "session_input_session_id": args[0].strip(),
            "session_input_hash": args[1].strip() if len(args) > 1 else "",
            "session_input_option": args[2].strip() if len(args) > 2 and args[2].strip().isdigit() else "",
        }
    if command == "cancel_task":
        if not args:
            return unsupported_command(original_text, "cancel_task_requires_task_id")
        task_id = args[0].strip()
        reason = " ".join(args[1:]).strip()
        if not task_id:
            return unsupported_command(original_text, "cancel_task_requires_task_id")
        return {
            "supported": True,
            "command": "cancel_task",
            "argv": [],
            "reason": "explicit_cancel_task_command",
            "command_text": original_text,
            "cancel_task_id": task_id,
            "cancel_reason": reason,
        }
    if command == "confirm":
        # An empty token confirms the single pending confirmation (the "확인"
        # button); an explicit token is still accepted for typed confirms.
        token = args[0].strip() if args else ""
        return {
            "supported": True,
            "command": "confirm",
            "argv": [],
            "reason": "explicit_confirm_command",
            "command_text": original_text,
            "confirm_token": token,
        }
    if command == "cancel":
        return {
            "supported": True,
            "command": "cancel",
            "argv": [],
            "reason": "explicit_cancel_command",
            "command_text": original_text,
        }
    return unsupported_command(original_text, "unsupported_remote_operator_command")


def parse_decision_command(command_text: str, args: list[str]) -> dict[str, Any]:
    if len(args) < 2:
        return unsupported_command(command_text, "decision_requires_id_and_action")
    decision_id = args[0].strip()
    action_kind = args[1].strip().lower()
    note = " ".join(args[2:]).strip()
    if not decision_id or not action_kind:
        return unsupported_command(command_text, "decision_requires_id_and_action")
    return {
        "supported": True,
        "command": "decision",
        "argv": [],
        "reason": "explicit_decision_command",
        "command_text": command_text,
        "decision_id": decision_id,
        "decision_action_kind": action_kind,
        "decision_note": note,
    }


def parse_dispatch_command(original_text: str, text: str) -> dict[str, Any]:
    remainder = text.split(None, 1)
    tail = remainder[1] if len(remainder) > 1 else ""
    if " -- " not in tail:
        return unsupported_command(original_text, "dispatch_requires_command_after_dashes")
    left, command = tail.split(" -- ", 1)
    command = command.strip()
    try:
        left_tokens = shlex.split(left)
    except ValueError as error:
        return unsupported_command(original_text, f"parse_error:{error}")
    if len(left_tokens) < 2 or not command:
        return unsupported_command(original_text, "dispatch_requires_closeout_runner_and_command")
    return {
        "supported": True,
        "command": "dispatch",
        "argv": [],
        "reason": "explicit_dispatch_command",
        "command_text": original_text,
        "closeout_id": left_tokens[0],
        "runner": left_tokens[1],
        "dispatch_command_text": command,
    }


def parse_run_command(command_text: str, args: list[str]) -> dict[str, Any]:
    # /run                    -> list curated templates (read-only)
    # /run --list             -> same
    # /run <closeout> <name>  -> dispatch the named template (confirm-gated)
    positional = [arg for arg in args if arg != "--list"]
    if not positional:
        return {
            "supported": True,
            "command": "run_list",
            "argv": [],
            "reason": "explicit_run_list_command",
            "command_text": command_text,
        }
    if len(positional) < 2:
        return unsupported_command(command_text, "run_requires_closeout_and_template")
    closeout_id = positional[0].strip()
    template_name = positional[1].strip()
    if not closeout_id or not template_name:
        return unsupported_command(command_text, "run_requires_closeout_and_template")
    return {
        "supported": True,
        "command": "run",
        "argv": [],
        "reason": "explicit_run_command",
        "command_text": command_text,
        "closeout_id": closeout_id,
        "template_name": template_name,
    }


def parse_recover_command(command_text: str, args: list[str]) -> dict[str, Any]:
    if len(args) < 2:
        return unsupported_command(command_text, "recover_requires_id_and_action")
    closeout_id = args[0].strip()
    action_kind = args[1].strip().lower()
    note = " ".join(args[2:]).strip()
    if not closeout_id or not action_kind:
        return unsupported_command(command_text, "recover_requires_id_and_action")
    return {
        "supported": True,
        "command": "recover",
        "argv": [],
        "reason": "explicit_recover_command",
        "command_text": command_text,
        "closeout_id": closeout_id,
        "recovery_action_kind": action_kind,
        "recovery_note": note,
    }


def parse_plans_command(command_text: str, args: list[str]) -> dict[str, Any]:
    argv = ["plans"]
    index = 0
    value_flags = {"--project-key", "--task-id", "--profile-key", "--artifact-kind"}
    while index < len(args):
        arg = args[index]
        if arg == "--latest":
            argv.append(arg)
            index += 1
            continue
        if arg in value_flags:
            if index + 1 >= len(args):
                return unsupported_command(command_text, f"missing_value:{arg}")
            value = args[index + 1].strip()
            if not value:
                return unsupported_command(command_text, f"empty_value:{arg}")
            argv.extend([arg, value])
            index += 2
            continue
        return unsupported_command(command_text, f"unsupported_plans_argument:{arg}")
    return {"supported": True, "command": "plans", "argv": argv}


def parse_show_command(command_text: str, args: list[str]) -> dict[str, Any]:
    if len(args) != 1 or not args[0].strip():
        return unsupported_command(command_text, "show_requires_one_plan_ref")
    return {"supported": True, "command": "show", "argv": ["show", args[0].strip()]}


def is_core_or_slash_command_text(text: str) -> bool:
    stripped = str(text or "").strip()
    if stripped in BUTTON_COMMAND_ALIASES:
        return True
    if not stripped.startswith("/"):
        return False
    try:
        first = shlex.split(stripped)[0]
    except (ValueError, IndexError):
        first = stripped.split(maxsplit=1)[0]
    return normalize_command_name(first) in CORE_OR_SLASH_COMMANDS


def remote_plan_session_command_payload(text: str) -> str | None:
    stripped = str(text or "").strip()
    if not stripped.startswith("/"):
        return None
    try:
        tokens = shlex.split(stripped)
    except ValueError:
        return None
    if not tokens:
        return None
    command = normalize_command_name(tokens[0])
    if command not in SESSION_INPUT_COMMANDS:
        return None
    payload = " ".join(tokens[1:]).strip()
    return payload or None
