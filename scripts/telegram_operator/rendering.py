"""Telegram message rendering and mobile card contracts."""

from __future__ import annotations

import html
import re
from typing import Any

from .routing import BUTTON_COMMAND_ALIASES, COMMAND_SURFACE, CORE_BUTTON_LABELS


MOBILE_CARD_CONTRACT_SCHEMA = "telegram_mobile_card_contract.v1"
CHOICE_SURFACE_CONTRACT_SCHEMA = "telegram_choice_surface_contract.v1"
REMOTE_PLAN_SESSION_CONTEXT_KIND = "remote_plan_project_selection"
REMOTE_PLAN_INIT_CONTEXT_KIND = "remote_plan_init_review"
MOBILE_CARD_MAX_LINES = 5
MOBILE_CARD_MAX_CHARS = 360
# The /guide reference sheet is deliberately not a glanceable card: it is the
# one message allowed to enumerate the whole command surface, so it gets its
# own (still bounded) budget.
GUIDE_SHEET_MAX_LINES = 36
GUIDE_SHEET_MAX_CHARS = 2000
# Reply budget inside the 360-char mobile card once the title and
# next-action lines are accounted for. Normalization and rendering must
# agree on this so replies are truncated exactly once.
ASSISTANT_REPLY_MAX_CHARS = 260
MOBILE_CARD_FORBIDDEN_TERMS = (
    "Forager Remote Status",
    "Read-only",
    "상태:",
    "다음:",
    "맥락:",
    "기준 ",
    "검증:",
    "sha256:",
    "shell",
    "launch-prep",
    "runtime_handle_alive",
)


def sanitize_text(text: str, *, max_chars: int = 1200) -> str:
    safe = str(text or "")
    safe = re.sub(r"bot[0-9]+:[A-Za-z0-9_-]+", "bot[redacted]", safe)
    safe = re.sub(r"(?i)(telegram_bot_token|bot_token|token)=\S+", r"\1=[redacted]", safe)
    safe = re.sub(r"sk-[A-Za-z0-9_-]{12,}", "sk-[redacted]", safe)
    if len(safe) > max_chars:
        safe = safe[:max_chars] + "...[truncated]"
    return safe


def projection_payload(projection: dict[str, Any]) -> dict[str, Any]:
    payload = projection.get("payload")
    return payload if isinstance(payload, dict) else {}


def projection_card(projection: dict[str, Any]) -> dict[str, Any]:
    card = projection.get("card")
    return card if isinstance(card, dict) else {}


def profile_label_from_projection(projection: dict[str, Any]) -> str:
    payload = projection_payload(projection)
    value = payload.get("profile") or projection.get("forager_profile") or "default"
    return sanitize_text(str(value), max_chars=80)


def title_with_profile(title: str, profile: Any) -> str:
    safe_profile = str(profile or "default").strip()
    if safe_profile and safe_profile != "default":
        return f"<b>{html.escape(str(title))}</b> · <code>{html.escape(safe_profile)}</code>"
    return f"<b>{html.escape(str(title))}</b>"


def number(value: dict[str, Any], key: str) -> int:
    raw = value.get(key)
    return int(raw) if isinstance(raw, int) else 0


def status_headline(payload: dict[str, Any]) -> str:
    pending = number(payload, "pending_approvals")
    failed = number(payload, "failed_offdesk_tasks")
    closeout = number(payload, "closeout_required_offdesk_tasks")
    active = number(payload, "active_offdesk_tasks")
    queued = number(payload, "queued_offdesk_tasks")
    if pending:
        return f"승인 요청 {pending}개가 먼저입니다."
    if failed:
        return f"실패한 자율주행 {failed}개를 확인해야 합니다."
    if closeout:
        return f"마무리 확인 {closeout}개가 남았습니다."
    if active:
        return f"자율주행 {active}개가 진행 중입니다."
    if queued:
        return f"자율주행 {queued}개가 대기 중입니다."
    return "처리할 항목이 없습니다."


def primary_status_kind(payload: dict[str, Any]) -> str:
    if number(payload, "pending_approvals"):
        return "pending"
    if number(payload, "failed_offdesk_tasks"):
        return "failed"
    if number(payload, "closeout_required_offdesk_tasks"):
        return "closeout"
    if number(payload, "active_offdesk_tasks"):
        return "active"
    if number(payload, "queued_offdesk_tasks"):
        return "queued"
    return "none"


def status_summary(payload: dict[str, Any], primary: str = "none") -> str:
    pending = number(payload, "pending_approvals")
    failed = number(payload, "failed_offdesk_tasks")
    closeout = number(payload, "closeout_required_offdesk_tasks")
    active = number(payload, "active_offdesk_tasks")
    queued = number(payload, "queued_offdesk_tasks")
    parts: list[str] = []
    if pending and primary != "pending":
        parts.append(f"승인 {pending}")
    if failed and primary != "failed":
        parts.append(f"실패 {failed}")
    if closeout and primary != "closeout":
        parts.append(f"마무리 {closeout}")
    if (active or queued) and primary not in {"active", "queued"}:
        parts.append(f"진행 {active} / 대기 {queued}")
    return "그 밖에 " + " · ".join(parts) if parts else ""


def status_next_action(payload: dict[str, Any]) -> str:
    pending = number(payload, "pending_approvals")
    failed = number(payload, "failed_offdesk_tasks")
    closeout = number(payload, "closeout_required_offdesk_tasks")
    if pending:
        return "아래 버튼으로 승인 내용 보기"
    if failed or closeout:
        return "로컬에서 실패/마무리 항목을 점검하세요."
    return "새 알림이 오면 다시 확인하세요."


def display_action(value: Any) -> str:
    text = str(value or "").strip()
    labels = {
        "approve_plan": "계획 승인",
        "approve_launch": "실행 승인",
        "deny_launch": "실행 거절",
        "provider_fallback": "모델 대체",
        "provider_retarget": "모델 변경",
    }
    return labels.get(text, text.replace("_", " ") or "확인 필요")


def display_review_status(value: Any) -> str:
    text = str(value or "").strip()
    labels = {
        "accepted": "승인됨",
        "approved": "승인됨",
        "pending": "검토 대기",
        "missing": "검토 없음",
        "not_reviewed": "검토 없음",
        "revision_required": "수정 필요",
        "rejected": "거절됨",
        "review_unknown": "검토 상태 불명",
        "unknown": "상태 불명",
    }
    return labels.get(text, text.replace("_", " ") or "상태 불명")


def display_next_action(value: Any) -> str:
    text = str(value or "").strip()
    labels = {
        "inspect": "내용 확인",
        "review": "리뷰 필요",
        "approve": "승인 검토",
        "launch_prep": "실행 준비 확인",
        "launch": "실행 검토",
        "closeout": "마무리 확인",
    }
    return labels.get(text, text.replace("_", " ") or "내용 확인")


def safe_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [sanitize_text(str(item), max_chars=400) for item in value if str(item).strip()]


def render_projection_message(
    projection: dict[str, Any],
    *,
    max_chars: int,
    adapter_health: dict[str, Any] | None = None,
) -> str:
    command = str(projection.get("command") or "").strip()
    if command == "status":
        message = render_status_message(projection, adapter_health=adapter_health)
    elif command == "pending":
        message = render_pending_message(projection)
    elif command == "plans":
        message = render_plans_message(projection)
    elif command == "show":
        message = render_show_message(projection)
    else:
        message = render_generic_projection_message(projection)
    if len(message) > max_chars:
        return message[: max(0, max_chars - 20)] + "\n...[truncated]"
    return message


def render_status_message(
    projection: dict[str, Any],
    *,
    adapter_health: dict[str, Any] | None = None,
) -> str:
    payload = projection_payload(projection)
    profile = profile_label_from_projection(projection)
    lines = [
        title_with_profile("Forager 점검", profile),
        status_headline(payload),
    ]
    summary = status_summary(payload, primary_status_kind(payload))
    if summary:
        lines.append(summary)
    adapter_line = adapter_status_line(adapter_health)
    if adapter_line:
        lines.append(adapter_line)
    lines.append(status_next_action(payload))
    return "\n".join(lines)


def readiness_for_action(adapter_health: dict[str, Any] | None, action: str) -> dict[str, Any] | None:
    if not isinstance(adapter_health, dict):
        return None
    readiness = adapter_health.get("action_readiness")
    if not isinstance(readiness, list):
        return None
    for item in readiness:
        if isinstance(item, dict) and str(item.get("action") or "") == action:
            return item
    return None


def adapter_status_line(adapter_health: dict[str, Any] | None) -> str:
    if not isinstance(adapter_health, dict):
        return ""
    health_status = str(adapter_health.get("health_status") or "").strip()
    build_plan = readiness_for_action(adapter_health, "build_plan") or {}
    build_plan_status = str(build_plan.get("status") or "").strip()
    if health_status == "healthy" and build_plan_status == "healthy":
        return "원격 정상 · 계획 준비 가능"
    if health_status == "degraded" or build_plan_status == "blocked":
        return "부분 장애: 새 계획/야간주행 막힘"
    if health_status == "unhealthy":
        return "원격 수신 장애: 로컬 CLI 확인"
    return ""


def render_pending_message(projection: dict[str, Any]) -> str:
    payload = projection_payload(projection)
    profile = profile_label_from_projection(projection)
    approvals = payload.get("approvals") if isinstance(payload.get("approvals"), list) else []
    lines = [
        title_with_profile("승인 대기", profile),
    ]
    if approvals:
        expired_count = sum(1 for item in approvals if isinstance(item, dict) and item.get("expired"))
        expired_suffix = f" 만료 {expired_count}개 포함." if expired_count else ""
        lines.append(f"승인 요청 {number(payload, 'approval_count')}개가 기다립니다.{expired_suffix}")
    else:
        lines.append("승인할 항목이 없습니다.")
    action_labels: list[str] = []
    for approval in approvals[:2]:
        if not isinstance(approval, dict):
            continue
        expired = " 만료" if approval.get("expired") else ""
        action_labels.append(html.escape(display_action(approval.get("action"))) + expired)
    if action_labels:
        lines.append(" · ".join(action_labels))
    if len(approvals) > 2:
        lines.append(f"외 {len(approvals) - 2}개 더 있음")
    next_line = "승인은 로컬에서 판단하세요." if approvals else "새 승인 요청이 오면 다시 확인하세요."
    lines.append(next_line)
    return "\n".join(lines)


def render_plans_message(projection: dict[str, Any]) -> str:
    payload = projection_payload(projection)
    profile = profile_label_from_projection(projection)
    plans = payload.get("plans") if isinstance(payload.get("plans"), list) else []
    lines = [
        title_with_profile("자율주행 계획", profile),
    ]
    if plans:
        lines.append(f"계획 {number(payload, 'plan_count')}개가 있습니다.")
    else:
        lines.append("등록된 계획이 없습니다.")
    for plan in plans[:2]:
        if not isinstance(plan, dict):
            continue
        lines.append(
            html.escape(str(plan.get("plan_id") or "plan"))
            + " · "
            + html.escape(display_review_status(plan.get("review_status")))
        )
    if len(plans) > 2:
        lines.append(f"외 {len(plans) - 2}개 더 있음")
    next_line = "아래 버튼으로 계획 상세 보기" if plans else "계획을 등록한 뒤 다시 확인하세요."
    lines.append(next_line)
    return "\n".join(lines)


def render_show_message(projection: dict[str, Any]) -> str:
    payload = projection_payload(projection)
    profile = profile_label_from_projection(projection)
    plan = payload.get("plan") if isinstance(payload.get("plan"), dict) else {}
    reviews = payload.get("reviews") if isinstance(payload.get("reviews"), list) else []
    launch_preps = payload.get("launch_preps") if isinstance(payload.get("launch_preps"), list) else []
    lines = [
        title_with_profile("계획 상세", profile),
        f"계획: {html.escape(str(plan.get('plan_id') or 'unknown'))}",
        f"리뷰: {html.escape(display_review_status(plan.get('review_status')))} / 실행 준비 {len(launch_preps)}개",
        f"다음 조치: {html.escape(display_next_action(plan.get('next_safe_action')))}",
    ]
    if reviews:
        latest = reviews[-1] if isinstance(reviews[-1], dict) else {}
        lines.append(
            "최근 리뷰: "
            + html.escape(str(latest.get("decision") or "unknown"))
            + " by "
            + html.escape(str(latest.get("reviewer") or "operator"))
        )
    return "\n".join(lines)


def render_generic_projection_message(projection: dict[str, Any]) -> str:
    card = projection_card(projection)
    title = html.escape(str(card.get("title") or "Forager"))
    summary_lines = safe_string_list(card.get("summary_lines"))
    lines = [
        f"<b>{title}</b>",
        html.escape(summary_lines[0] if summary_lines else "내용 확인"),
    ]
    for item in summary_lines[1:3]:
        lines.append(html.escape(item))
    lines.append("세부 내용은 로컬에서 확인하세요.")
    return "\n".join(lines)


def interaction_context_label(context: dict[str, Any] | None) -> str:
    if not isinstance(context, dict):
        return ""
    focus_kind = str(context.get("focus_kind") or "").strip()
    focus_ref = str(context.get("focus_ref") or "").strip()
    focus_label = str(context.get("focus_label") or "").strip()
    if focus_kind == "plan" and focus_ref:
        suffix = f" · {focus_label}" if focus_label else ""
        return f"계획 {focus_ref}{suffix}"
    if focus_kind == "approval" and focus_ref:
        suffix = f" · {focus_label}" if focus_label else ""
        return f"승인 {focus_ref}{suffix}"
    if focus_label:
        return focus_label
    command = str(context.get("command") or "").strip()
    return command


def mobile_card_contract(
    message: str,
    *,
    max_lines: int = MOBILE_CARD_MAX_LINES,
    max_chars: int = MOBILE_CARD_MAX_CHARS,
) -> dict[str, Any]:
    lines = str(message or "").splitlines()
    content_lines = [line.strip() for line in lines if line.strip()]
    warnings: list[str] = []
    if len(lines) > max_lines:
        warnings.append("too_many_lines")
    if len(str(message or "")) > max_chars:
        warnings.append("too_many_chars")
    has_title = bool(content_lines and content_lines[0].startswith("<b>"))
    body_lines = content_lines[1:] if has_title else content_lines
    action_markers = (
        "아래 버튼",
        "로컬에서",
        "다시 확인",
        "직접 의견",
        "직접 입력",
        "세부 내용",
        "다음 조치:",
    )

    def is_action_line(line: str) -> bool:
        return any(marker in line for marker in action_markers)

    has_status_headline = any(
        not line.startswith("기준 ") and not is_action_line(line)
        for line in body_lines
    )
    has_next_action = any(is_action_line(line) for line in body_lines)
    if not has_title:
        warnings.append("missing_title")
    if not has_status_headline:
        warnings.append("missing_status_headline")
    if not has_next_action:
        warnings.append("missing_next_action")
    leaked_terms = [term for term in MOBILE_CARD_FORBIDDEN_TERMS if term in message]
    if leaked_terms:
        warnings.append("forbidden_terms:" + ",".join(leaked_terms))
    # Telegram parses the message as HTML: any "<tag>" outside the small
    # supported set 400s the whole sendMessage. Placeholders like <id> must
    # be &lt;-escaped; this warning makes every dry-run test catch them.
    rogue_tags = sorted(
        {
            match.group(1).lower()
            for match in re.finditer(r"</?([a-zA-Z][a-zA-Z0-9_-]*)", str(message or ""))
            if match.group(1).lower()
            not in {"b", "strong", "i", "em", "code", "pre", "a", "s", "u", "tg-spoiler"}
        }
    )
    if rogue_tags:
        warnings.append("unescaped_html_tags:" + ",".join(rogue_tags))
    return {
        "schema": MOBILE_CARD_CONTRACT_SCHEMA,
        "line_count": len(lines),
        "char_count": len(str(message or "")),
        "max_lines": max_lines,
        "max_chars": max_chars,
        "has_title": has_title,
        "has_status_headline": has_status_headline,
        "has_next_action": has_next_action,
        "warnings": warnings,
    }


def choice_keyboard(context: dict[str, Any] | None = None) -> dict[str, Any]:
    rows: list[list[str]] = []
    seen: set[str] = set()

    def add_row(*labels: str) -> None:
        row: list[str] = []
        for label in labels:
            text = str(label or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            row.append(text)
        if row:
            rows.append(row)

    context_kind = str(context.get("context_kind") or "") if isinstance(context, dict) else ""
    next_command = str(context.get("next_command") or "").strip() if isinstance(context, dict) else ""
    if context_kind == REMOTE_PLAN_SESSION_CONTEXT_KIND:
        choice_labels = context.get("choice_labels") if isinstance(context, dict) else []
        labels = [str(label or "").strip() for label in choice_labels if str(label or "").strip()] if isinstance(choice_labels, list) else []
        for index in range(0, len(labels), 2):
            add_row(*labels[index : index + 2])
        add_row("다시 스캔", "보류")
        add_row("상태", "계획")
        return {
            "keyboard": rows,
            "resize_keyboard": True,
            "one_time_keyboard": False,
            "input_field_placeholder": "번호/프로젝트명 입력 · 질문은 채팅",
        }
    if context_kind == REMOTE_PLAN_INIT_CONTEXT_KIND:
        choice_labels = context.get("choice_labels") if isinstance(context, dict) else []
        labels = [str(label or "").strip() for label in choice_labels if str(label or "").strip()] if isinstance(choice_labels, list) else []
        if labels:
            add_row(*labels[:2])
            add_row(*labels[2:4])
        else:
            add_row("초기화 검토", "다시 선택")
            add_row("보류")
        add_row("상태", "계획")
        return {
            "keyboard": rows,
            "resize_keyboard": True,
            "one_time_keyboard": False,
            "input_field_placeholder": "작업 버튼/경로 입력 · 질문은 채팅",
        }
    if context_kind == "dispatch_confirm":
        # Confirm cards get one-tap 확인/취소 buttons so the operator does not
        # have to type /confirm <token>.
        add_row("확인", "취소")
    choice_commands = context.get("choice_commands") if isinstance(context, dict) else None
    if isinstance(choice_commands, list):
        # Each command is a complete slash command (e.g. "/decision <id> revise").
        # Tapping the button sends that text, so the operator dispatches the
        # decision action in one tap instead of typing the id and action.
        commands = [str(command or "").strip() for command in choice_commands if str(command or "").strip()]
        for index in range(0, len(commands), 2):
            add_row(*commands[index : index + 2])
    if next_command and next_command not in {"/status", "/pending", "/plans --latest", "/help"}:
        add_row(next_command)
    if context_kind == "status_attention":
        add_row("승인 대기", "계획")
        add_row("상태", "도움말")
    elif context_kind == "approval_attention":
        add_row("전체 승인", "상태")
        add_row("승인 대기", "계획")
        add_row("도움말")
    elif context_kind == "plan_attention":
        add_row("계획", "상태")
        add_row("승인 대기", "도움말")
    elif context_kind == "plan_detail":
        add_row("계획", "상태")
        add_row("승인 대기", "도움말")
    elif context_kind == "decisions_actions":
        add_row("승인 대기", "상태")
        add_row("계획", "도움말")
    else:
        add_row("상태", "승인 대기")
        add_row("계획", "도움말")
    for label in CORE_BUTTON_LABELS:
        if label not in seen:
            add_row(label)
    return {
        "keyboard": rows,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "평문은 채팅 · /feedback은 기록",
    }


def button_resolves_to(button_text: str, command_text: str) -> bool:
    button = str(button_text or "").strip()
    command = str(command_text or "").strip()
    return bool(command) and (button == command or BUTTON_COMMAND_ALIASES.get(button) == command)


def choice_surface_contract(
    reply_markup: dict[str, Any] | None,
    context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    warnings: list[str] = []
    keyboard = reply_markup.get("keyboard") if isinstance(reply_markup, dict) else None
    button_texts: list[str] = []
    if isinstance(keyboard, list):
        for row in keyboard:
            if not isinstance(row, list):
                continue
            for button in row:
                if isinstance(button, str):
                    button_texts.append(button)
                elif isinstance(button, dict):
                    button_texts.append(str(button.get("text") or ""))
    else:
        warnings.append("missing_keyboard")
    context_kind = str(context.get("context_kind") or "") if isinstance(context, dict) else ""
    if context_kind in {REMOTE_PLAN_SESSION_CONTEXT_KIND, REMOTE_PLAN_INIT_CONTEXT_KIND}:
        required_buttons = ("상태", "계획")
    else:
        required_buttons = CORE_BUTTON_LABELS
    for label in required_buttons:
        if label not in button_texts:
            warnings.append(f"missing_button:{label}")
    placeholder = ""
    if isinstance(reply_markup, dict):
        placeholder = str(reply_markup.get("input_field_placeholder") or "")
    if not any(marker in placeholder for marker in ("채팅", "의견", "직접 입력")):
        warnings.append("missing_freeform_placeholder")
    next_command = str(context.get("next_command") or "").strip() if isinstance(context, dict) else ""
    has_contextual_choice = False
    choice_labels = context.get("choice_labels") if isinstance(context, dict) else None
    if isinstance(choice_labels, list) and choice_labels:
        expected = [str(label or "").strip() for label in choice_labels if str(label or "").strip()]
        has_contextual_choice = any(label in button_texts for label in expected)
        if not has_contextual_choice:
            warnings.append("missing_contextual_choice:choice_labels")
    choice_commands = context.get("choice_commands") if isinstance(context, dict) else None
    if isinstance(choice_commands, list) and choice_commands:
        expected_commands = [str(command or "").strip() for command in choice_commands if str(command or "").strip()]
        has_contextual_choice = any(command in button_texts for command in expected_commands)
        if not has_contextual_choice:
            warnings.append("missing_contextual_choice:choice_commands")
    if next_command:
        has_contextual_choice = any(button_resolves_to(button, next_command) for button in button_texts)
        if not has_contextual_choice:
            warnings.append(f"missing_contextual_choice:{next_command}")
    return {
        "schema": CHOICE_SURFACE_CONTRACT_SCHEMA,
        "button_texts": button_texts,
        "has_freeform_placeholder": any(marker in placeholder for marker in ("채팅", "의견", "직접 입력")),
        "context_kind": context.get("context_kind") if isinstance(context, dict) else None,
        "context_command": next_command or None,
        "has_contextual_choice": has_contextual_choice,
        "warnings": warnings,
    }


def help_message(*, profile: Any, generated_at: Any) -> str:
    return "\n".join(
        [
            title_with_profile("Forager 원격 조작", profile),
            "평문은 에이전트 채팅으로 답합니다.",
            "한눈 요약: /attention · 전체 사용 안내: /guide",
            "실행/정지: /decisions · /recovery · /run · /tasks · /pause",
            "다음 조치: /status · /feedback · /plan",
        ]
    )


def guide_message(*, profile: Any, generated_at: Any) -> str:
    """Full usage reference sheet built from the COMMAND_SURFACE truth table.

    Rendered under the guide-sheet budget, not the 5-line card contract; this
    is the one place the operator can learn the whole surface from the phone.
    """

    groups: dict[str, list[str]] = {}
    for usage, desc, group in COMMAND_SURFACE:
        groups.setdefault(group, []).append(
            f"{html.escape(usage)} · {html.escape(desc)}"
        )
    lines = [
        title_with_profile("Forager 사용 안내", profile),
        "평문 메시지 = 에이전트 채팅. 상태·작업공간 질문에 실데이터로 답합니다.",
        "할 일을 맡기려면 /plan &lt;목표&gt; 로 계획 후보를 큐잉하세요.",
    ]
    for group in ("조회", "실행", "기록", "제어", "도움말"):
        entries = groups.get(group)
        if not entries:
            continue
        lines.append(f"<b>[{group}]</b>")
        lines.extend(entries)
    lines.append("자율주행: 저녁 유휴 시 제안 카드가 오면 확인 탭으로 승인, /pause 로 즉시 중단.")
    lines.append("다음 조치: /attention · /status")
    return "\n".join(lines)


def render_decisions_message(*, profile: Any, generated_at: Any, decisions: list[dict[str, Any]]) -> str:
    lines = [title_with_profile("결정 목록", profile)]
    open_decisions = [decision for decision in decisions if decision.get("actions")]
    if not open_decisions:
        lines.append("처리 대기 중인 결정이 없습니다.")
        lines.append("다음 조치: /status · /pending")
        return "\n".join(lines)
    for decision in open_decisions[:3]:
        actions = " · ".join(action["action_kind"] for action in decision["actions"][:3])
        title = str(decision.get("title") or decision.get("decision_id") or "")
        if len(title) > 60:
            title = title[:57] + "..."
        lines.append(f"{html.escape(title)}: {html.escape(actions)}")
    lines.append("다음 조치: /decision &lt;id&gt; &lt;action&gt; [note]")
    return "\n".join(lines)


def render_runtime_message(*, profile: Any, generated_at: Any, rows: list[dict[str, Any]], enabled: bool) -> str:
    lines = [title_with_profile("런타임 대기열", profile)]
    if not rows:
        lines.append("디스패치 대기 중인 항목이 없습니다.")
        lines.append("다음 조치: /decisions · /status")
        return "\n".join(lines)
    for row in rows[:3]:
        closeout = str(row.get("closeout_id") or "")
        if len(closeout) > 44:
            closeout = closeout[:41] + "..."
        marker = " (대기열 등록됨)" if row.get("already_queued") else ""
        lines.append(f"{html.escape(closeout)}{html.escape(marker)}")
    if enabled:
        lines.append("다음 조치: /dispatch &lt;closeout-id&gt; &lt;runner&gt; -- &lt;명령&gt;")
    else:
        lines.append("실행 비활성. --enable-runtime-dispatch 필요")
    return "\n".join(lines)


def render_runtime_disabled_message(*, profile: Any, generated_at: Any) -> str:
    return "\n".join(
        [
            title_with_profile("런타임 실행 비활성", profile),
            "원격 런타임 디스패치가 꺼져 있습니다.",
            "로컬에서 --enable-runtime-dispatch 로 켜야 합니다.",
            "다음 조치: /runtime · /status",
        ]
    )


def render_runtime_confirm_message(
    *,
    profile: Any,
    generated_at: Any,
    closeout_id: str,
    runner: str,
    command: str,
    token: str,
) -> str:
    lines = [title_with_profile("런타임 디스패치 확인", profile)]
    lines.append(f"클로즈아웃 {html.escape(str(closeout_id))} / {html.escape(str(runner))}")
    lines.append(f"명령: {html.escape(sanitize_text(command, max_chars=120))}")
    lines.append("확인 시 tick에서 실행 대기열에 올립니다.")
    lines.append(f"다음 조치: /confirm {html.escape(str(token))} 또는 취소")
    return "\n".join(lines)


def render_runtime_result_message(*, profile: Any, generated_at: Any, result: dict[str, Any]) -> str:
    lines = [title_with_profile("런타임 디스패치 결과", profile)]
    closeout_id = str(result.get("closeout_id") or "")
    if result.get("ok"):
        lines.append(f"클로즈아웃 {html.escape(closeout_id)} 대기열 등록됨.")
        lines.append("tick 실행 전까지 프로세스는 시작되지 않습니다.")
    else:
        stage = str(result.get("stage") or "unknown")
        lines.append(f"등록되지 않음 ({html.escape(stage)}).")
        error = str(result.get("error") or "")
        if error:
            lines.append(html.escape(dispatch_safe_detail(error)))
    lines.append("다음 조치: /runtime · /status")
    return "\n".join(lines)


def render_run_list_message(
    *,
    profile: Any,
    generated_at: Any,
    templates: list[dict[str, Any]],
    configured: bool,
) -> str:
    lines = [title_with_profile("실행 템플릿", profile)]
    if not configured:
        lines.append("큐레이션 실행이 설정되지 않았습니다.")
        lines.append("로컬에서 --dispatch-allowlist-file 로 켜야 합니다.")
        lines.append("다음 조치: /runtime · /status")
        return "\n".join(lines)
    if not templates:
        lines.append("등록된 실행 템플릿이 없습니다.")
        lines.append("다음 조치: /runtime · /status")
        return "\n".join(lines)
    for template in templates[:3]:
        name = str(template.get("name") or "")
        description = sanitize_text(str(template.get("description") or ""), max_chars=48)
        if description:
            lines.append(f"{html.escape(name)}: {html.escape(description)}")
        else:
            lines.append(html.escape(name))
    lines.append("다음 조치: /run &lt;closeout-id&gt; &lt;name&gt;")
    return "\n".join(lines)


def render_run_disabled_message(*, profile: Any, generated_at: Any) -> str:
    return "\n".join(
        [
            title_with_profile("큐레이션 실행 비활성", profile),
            "이름 붙은 실행 템플릿이 설정되지 않았습니다.",
            "로컬에서 --dispatch-allowlist-file 로 켜야 합니다.",
            "다음 조치: /run · /status",
        ]
    )


def render_run_confirm_message(
    *,
    profile: Any,
    generated_at: Any,
    closeout_id: str,
    template_name: str,
    command: str,
    token: str,
) -> str:
    lines = [title_with_profile("실행 확인", profile)]
    lines.append(f"클로즈아웃 {html.escape(str(closeout_id))} / {html.escape(str(template_name))}")
    lines.append(f"명령: {html.escape(sanitize_text(command, max_chars=120))}")
    lines.append("확인 시 tick에서 실행 대기열에 올립니다.")
    lines.append(f"다음 조치: /confirm {html.escape(str(token))} 또는 취소")
    return "\n".join(lines)


def render_attention_summary_message(*, profile: Any, generated_at: Any, summary: dict[str, Any]) -> str:
    lines = [title_with_profile("조치 필요 요약", profile)]
    total = int(summary.get("total") or 0)
    if total == 0:
        lines.append("지금 조치가 필요한 항목이 없습니다.")
        lines.append("다음 조치: /status · /pending")
        return "\n".join(lines)
    lines.append(
        f"결정 {int(summary.get('decision_count') or 0)} · "
        f"복구 {int(summary.get('recovery_count') or 0)} · "
        f"작업 {int(summary.get('task_count') or 0)}"
    )
    top = summary.get("top")
    if isinstance(top, dict):
        title = sanitize_text(str(top.get("title") or ""), max_chars=60)
        lines.append(f"먼저: {html.escape(title)} → {html.escape(str(top.get('command_hint') or ''))}")
    lines.append("다음 조치: /decisions · /recovery · /tasks")
    return "\n".join(lines)


def render_pause_result_message(*, profile: Any, generated_at: Any, result: dict[str, Any]) -> str:
    lines = [title_with_profile("전면 정지", profile)]
    if result.get("ok") and result.get("paused"):
        lines.append("신규 작업 시작을 전면 중단했습니다.")
        reason = str(result.get("reason") or "")
        if reason:
            lines.append(f"사유: {html.escape(sanitize_text(reason, max_chars=120))}")
        lines.append("실행 중 작업은 계속될 수 있습니다.")
    else:
        lines.append("정지에 실패했습니다.")
        error = str(result.get("error") or "")
        if error:
            lines.append(html.escape(dispatch_safe_detail(error)))
    lines.append("다음 조치: /resume · /status")
    return "\n".join(lines)


def render_resume_confirm_message(*, profile: Any, generated_at: Any, token: str) -> str:
    return "\n".join(
        [
            title_with_profile("정지 해제 확인", profile),
            "자율 실행을 다시 시작합니다.",
            "신규 작업이 다시 dispatch됩니다.",
            f"다음 조치: /confirm {html.escape(str(token))} 또는 취소",
        ]
    )


def render_resume_result_message(*, profile: Any, generated_at: Any, result: dict[str, Any]) -> str:
    lines = [title_with_profile("정지 해제 결과", profile)]
    if result.get("ok") and not result.get("paused"):
        lines.append("전면 정지를 해제했습니다.")
        lines.append("신규 작업 dispatch가 다시 진행됩니다.")
    else:
        lines.append("해제에 실패했습니다.")
        error = str(result.get("error") or "")
        if error:
            lines.append(html.escape(dispatch_safe_detail(error)))
    lines.append("다음 조치: /tasks · /status")
    return "\n".join(lines)


def render_tasks_message(*, profile: Any, generated_at: Any, tasks: list[dict[str, Any]]) -> str:
    lines = [title_with_profile("실행 작업", profile)]
    if not tasks:
        lines.append("취소 가능한 작업이 없습니다.")
        lines.append("다음 조치: /status · /pending")
        return "\n".join(lines)
    for task in tasks[:3]:
        task_id = str(task.get("task_id") or "")
        if len(task_id) > 40:
            task_id = task_id[:37] + "..."
        status = str(task.get("status") or "")
        lines.append(f"{html.escape(task_id)} · {html.escape(status)}")
    lines.append("다음 조치: /cancel-task &lt;task-id&gt; [사유]")
    return "\n".join(lines)


def render_cancel_task_confirm_message(
    *,
    profile: Any,
    generated_at: Any,
    task_id: str,
    reason: str,
    token: str,
) -> str:
    lines = [title_with_profile("작업 취소 확인", profile)]
    lines.append(f"작업 {html.escape(str(task_id))}")
    if reason:
        lines.append(f"사유: {html.escape(sanitize_text(reason, max_chars=120))}")
    lines.append("확인 시 취소 표시(러너는 별도).")
    lines.append(f"다음 조치: /confirm {html.escape(str(token))} 또는 취소")
    return "\n".join(lines)


def render_cancel_task_result_message(*, profile: Any, generated_at: Any, result: dict[str, Any]) -> str:
    lines = [title_with_profile("작업 취소 결과", profile)]
    task_id = str(result.get("task_id") or "")
    if result.get("ok"):
        if result.get("changed"):
            lines.append(f"작업 {html.escape(task_id)} 취소 표시됨.")
            lines.append("백그라운드 러너는 계속 실행 중일 수 있습니다.")
        else:
            lines.append(f"작업 {html.escape(task_id)}: 변경 없음.")
            detail = str(result.get("message") or "")
            if detail:
                lines.append(html.escape(dispatch_safe_detail(detail)))
    else:
        lines.append(f"취소 실패: {html.escape(task_id)}")
        error = str(result.get("error") or "")
        if error:
            lines.append(html.escape(dispatch_safe_detail(error)))
    lines.append("다음 조치: /tasks · /status")
    return "\n".join(lines)


def render_recovery_message(*, profile: Any, generated_at: Any, recoveries: list[dict[str, Any]]) -> str:
    lines = [title_with_profile("복구 목록", profile)]
    open_recoveries = [recovery for recovery in recoveries if recovery.get("actions")]
    if not open_recoveries:
        lines.append("처리 대기 중인 복구 항목이 없습니다.")
        lines.append("다음 조치: /status · /decisions")
        return "\n".join(lines)
    for recovery in open_recoveries[:3]:
        actions = " · ".join(action["action_kind"] for action in recovery["actions"][:2])
        closeout = str(recovery.get("closeout_id") or "")
        if len(closeout) > 40:
            closeout = closeout[:37] + "..."
        lines.append(f"{html.escape(closeout)}: {html.escape(actions)}")
    lines.append("다음 조치: /recover &lt;closeout-id&gt; &lt;action&gt; [note]")
    return "\n".join(lines)


def render_recovery_confirm_message(
    *,
    profile: Any,
    generated_at: Any,
    closeout_id: str,
    action_kind: str,
    note: str,
    token: str,
) -> str:
    lines = [title_with_profile("복구 확인 필요", profile)]
    lines.append(f"클로즈아웃 {html.escape(str(closeout_id))} → {html.escape(str(action_kind))}")
    if note:
        lines.append(f"메모: {html.escape(sanitize_text(note, max_chars=120))}")
    lines.append("검증만 하며 수용 기록은 아닙니다.")
    lines.append(f"다음 조치: /confirm {html.escape(str(token))} 또는 취소")
    return "\n".join(lines)


def render_recovery_result_message(*, profile: Any, generated_at: Any, result: dict[str, Any]) -> str:
    lines = [title_with_profile("복구 결과", profile)]
    closeout_id = str(result.get("closeout_id") or "")
    action_kind = str(result.get("action_kind") or "")
    if result.get("ok"):
        lines.append(f"클로즈아웃 {html.escape(closeout_id)} → {html.escape(action_kind)} 검증됨.")
        lines.append("아직 실제 복구/수용 기록은 아닙니다.")
    else:
        stage = str(result.get("stage") or "unknown")
        lines.append(f"검증되지 않음 ({html.escape(stage)}).")
        error = str(result.get("error") or "")
        if error:
            lines.append(html.escape(dispatch_safe_detail(error)))
    lines.append("다음 조치: /recovery · /status")
    return "\n".join(lines)


def render_dispatch_confirm_message(
    *,
    profile: Any,
    generated_at: Any,
    decision_id: str,
    action_kind: str,
    note: str,
    token: str,
) -> str:
    lines = [title_with_profile("실행 확인 필요", profile)]
    lines.append(f"결정 {html.escape(str(decision_id))} → {html.escape(str(action_kind))}")
    if note:
        lines.append(f"메모: {html.escape(sanitize_text(note, max_chars=120))}")
    lines.append("아직 실행하지 않았습니다.")
    lines.append(f"다음 조치: /confirm {html.escape(str(token))} 또는 취소")
    return "\n".join(lines)


def render_dispatch_result_message(*, profile: Any, generated_at: Any, result: dict[str, Any]) -> str:
    lines = [title_with_profile("실행 결과", profile)]
    decision_id = str(result.get("decision_id") or "")
    action_kind = str(result.get("action_kind") or "")
    if result.get("ok"):
        lines.append(f"결정 {html.escape(decision_id)} → {html.escape(action_kind)} 적용됨.")
        if result.get("closeout_appended"):
            lines.append("결정 원장에 기록했습니다.")
    else:
        stage = str(result.get("stage") or "unknown")
        lines.append(f"적용되지 않음 ({html.escape(stage)}).")
        error = str(result.get("error") or "")
        if error:
            lines.append(html.escape(dispatch_safe_detail(error)))
    lines.append("다음 조치: /decisions · /status")
    return "\n".join(lines)


def dispatch_safe_detail(text: str) -> str:
    """Trim CLI-facing terms out of an operator card detail line."""

    detail = sanitize_text(text, max_chars=160)
    for term in ("receipt", "preview", "sha256:"):
        detail = detail.replace(term, "").replace(term.capitalize(), "")
    return " ".join(detail.split())


def render_dispatch_cancel_message(*, profile: Any, generated_at: Any, cleared: bool) -> str:
    return "\n".join(
        [
            title_with_profile("실행 취소", profile),
            "대기 중이던 실행 확인을 취소했습니다." if cleared else "취소할 실행 확인이 없습니다.",
            "아직 아무 작업도 실행하지 않았습니다.",
            "다음 조치: /decisions · /status",
        ]
    )


def render_dispatch_error_message(*, profile: Any, generated_at: Any, headline: str, detail: str) -> str:
    return "\n".join(
        [
            title_with_profile("실행 불가", profile),
            html.escape(sanitize_text(headline, max_chars=120)),
            html.escape(dispatch_safe_detail(detail)),
            "다음 조치: /decisions · /status",
        ]
    )


def render_chat_message(
    *,
    profile: Any,
    generated_at: Any,
    chat_text: str,
    feedback_context: dict[str, Any] | None = None,
    agent_intent: dict[str, Any] | None = None,
) -> str:
    clarifying_question = agent_clarifying_question(agent_intent)
    if clarifying_question:
        return "\n".join(
            [
                title_with_profile("확인 필요", profile),
                html.escape(clarifying_question),
                "다음 조치: /plan 또는 /feedback",
            ]
        )
    assistant_reply = agent_assistant_reply(agent_intent)
    lines = [title_with_profile("Forager 응답", profile)]
    if assistant_reply:
        lines.append(html.escape(assistant_reply))
    else:
        fallback_reason = agent_fallback_reason_line(agent_intent)
        if fallback_reason:
            lines.append(html.escape(fallback_reason))
        else:
            lines.append("채팅 에이전트 응답을 만들 수 없습니다.")
    lines.append("다음 조치: /status · /feedback · /plan")
    context_label = interaction_context_label(feedback_context)
    if context_label:
        lines.append(f"참조: {html.escape(context_label)}")
    return "\n".join(lines)


def render_feedback_message(
    *,
    profile: Any,
    generated_at: Any,
    feedback_text: str,
    feedback_kind: str = "freeform_feedback",
    feedback_context: dict[str, Any] | None = None,
    inbox_status: str | None = None,
    agent_intent: dict[str, Any] | None = None,
) -> str:
    is_planning_request = feedback_kind == "planning_request"
    clarifying_question = agent_clarifying_question(agent_intent)
    if clarifying_question:
        return "\n".join(
            [
                title_with_profile("확인 필요", profile),
                "모델이 범위 확인을 요청했습니다.",
                html.escape(clarifying_question),
                "직접 입력으로 범위만 알려주세요.",
            ]
        )
    assistant_reply = agent_assistant_reply(agent_intent)
    if assistant_reply and not is_planning_request:
        lines = [
            title_with_profile("Forager 응답", profile),
            html.escape(assistant_reply),
        ]
        context_label = interaction_context_label(feedback_context)
        if context_label:
            lines.append(f"참조: {html.escape(context_label)}")
        lines.append("아래 버튼으로 상태/계획을 이어서 확인하세요.")
        return "\n".join(lines)
    if inbox_status in {"recorded", "existing"}:
        status_line = "검토 목록에 넣었습니다." if is_planning_request else "의견을 검토 목록에 넣었습니다."
    elif inbox_status == "error":
        status_line = "요청은 저장했지만 검토 등록은 실패했습니다." if is_planning_request else "의견은 저장했지만 검토 목록 등록은 실패했습니다."
    else:
        status_line = "계획 요청을 저장했습니다." if is_planning_request else "의견을 저장했습니다."
    lines = [
        title_with_profile("계획 요청 접수" if is_planning_request else "의견 접수", profile),
        status_line,
    ]
    context_label = interaction_context_label(feedback_context)
    if context_label and not is_planning_request:
        lines.append(f"관련: {html.escape(context_label)}")
    if is_planning_request:
        lines.append("아직 실행은 시작하지 않았습니다.")
        lines.append("로컬에서 계획으로 바꾸세요.")
    else:
        lines.append("로컬에서 검토합니다.")
    return "\n".join(lines)


def render_wiki_candidate_message(
    *,
    profile: Any,
    generated_at: Any,
    remember_text: str,
    record_result: dict[str, Any] | None = None,
) -> str:
    status = str((record_result or {}).get("wiki_candidate_status") or "preview")
    if status == "recorded":
        status_line = "위키 후보로 저장했습니다."
    elif status == "updated":
        status_line = "기존 위키 후보에 반영했습니다."
    elif status == "failed":
        status_line = "위키 후보 저장에 실패했습니다. 로컬 상태를 확인하세요."
    else:
        status_line = "위키 후보 저장 미리보기입니다."
    return "\n".join(
        [
            title_with_profile("위키 후보", profile),
            status_line,
            "아직 런타임 지식은 아닙니다.",
            "다음 조치: offdesk wiki review",
        ]
    )


def agent_assistant_reply(agent_intent: dict[str, Any] | None) -> str | None:
    if not isinstance(agent_intent, dict):
        return None
    if str(agent_intent.get("status") or "") != "classified":
        return None
    reply = sanitize_text(
        str(agent_intent.get("assistant_reply") or "").strip(),
        max_chars=ASSISTANT_REPLY_MAX_CHARS,
    )
    return reply or None


AGENT_FALLBACK_REASON_LINES = {
    "local_agent_disabled": "로컬 에이전트가 꺼져 있습니다.",
    "local_agent_unavailable": "로컬 에이전트에 연결할 수 없습니다.",
}


def agent_fallback_reason_line(agent_intent: dict[str, Any] | None) -> str | None:
    if not isinstance(agent_intent, dict):
        return None
    if str(agent_intent.get("status") or "") != "fallback":
        return None
    reason = str(agent_intent.get("reason") or "").strip()
    if reason in AGENT_FALLBACK_REASON_LINES:
        return AGENT_FALLBACK_REASON_LINES[reason]
    if reason.startswith("local_agent_failed:"):
        detail = reason.split(":", 1)[1]
        return f"로컬 에이전트 호출 실패: {detail}"
    return "로컬 에이전트를 사용할 수 없습니다."


def agent_clarifying_question(agent_intent: dict[str, Any] | None) -> str | None:
    if not isinstance(agent_intent, dict):
        return None
    if not bool(agent_intent.get("requires_clarification")):
        return None
    question = sanitize_text(str(agent_intent.get("clarifying_question") or "").strip(), max_chars=180)
    return question or None
