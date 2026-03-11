#!/usr/bin/env python3
"""Chat-scoped management command helpers for Telegram gateway."""

from typing import Any, Callable, Dict, Optional


def tutorial_text(*, lang: str, cmd_prefix: str) -> str:
    p = str(cmd_prefix or "/").strip() or "/"
    lang_token = str(lang or "").strip().lower()
    if lang_token == "en":
        return (
            "tutorial (quickstart)\n"
            f"- prefix: {p} (both {p} and / can be accepted depending on env)\n"
            "\n"
            "1) Lock access (recommended)\n"
            f"- {p}onlyme\n"
            "\n"
            "2) Map projects (O1..)\n"
            f"- {p}map\n"
            "\n"
            "3) Lock the active project (recommended before work)\n"
            f"- {p}use O2\n"
            f"- {p}focus O2   # hard lock (recommended)\n"
            "- after /use, plain text and TF commands target that project by default\n"
            "- after /focus, global wave commands are blocked or narrowed to that project\n"
            "- if /map shows [UNREADY], run /orch repair O2 before sync/next\n"
            "\n"
            "4) Seed queue from todos\n"
            f"- {p}sync O2 1h   # single-project mode\n"
            f"- {p}sync all 1h  # global refresh\n"
            f"- {p}sync         # repeats last sync args (chat-local)\n"
            "\n"
            "5) Run\n"
            f"- {p}next     # run one in the active project\n"
            f"- {p}fanout   # global one-per-project wave\n"
            f"- {p}todo proposals   # TF-generated follow-up inbox\n"
            f"- {p}todo accept PROP-001 | {p}todo reject PROP-001\n"
            "\n"
            "6) After-work mode\n"
            f"- {p}offdesk prepare\n"
            f"- {p}offdesk review\n"
            f"- {p}offdesk on\n"
            f"- {p}auto status\n"
            f"- {p}panic    # emergency stop\n"
            f"- {p}todo syncback preview   # review what will be written back to TODO.md\n"
            "\n"
            "tips\n"
            f"- send just '{p}' to open the command menu\n"
            f"- {p}dispatch or {p}direct enables one-shot plain text for the next message\n"
            f"- for single-project work, prefer {p}use -> {p}sync O# -> {p}next\n"
            f"- finish with {p}focus off when you want global scheduling again\n"
        )
    return (
        "튜토리얼 (빠른 시작)\n"
        f"- prefix: {p} (환경변수 AOE_TG_COMMAND_PREFIXES에 따라 !/ 둘 다 허용 가능)\n"
        "\n"
        "1) 접근 잠금 (권장)\n"
        f"- {p}onlyme\n"
        "\n"
        "2) 프로젝트 맵(O1..) 갱신\n"
        f"- {p}map\n"
        "\n"
        "3) 작업할 프로젝트 고정(권장)\n"
        f"- {p}use O2\n"
        f"- {p}focus O2   # hard lock (권장)\n"
        "- /use 이후 평문/TF 명령은 해당 프로젝트를 기본 타겟으로 사용\n"
        "- /focus 이후 전역 wave 명령은 차단되거나 해당 프로젝트로 축소됨\n"
        "- /map 에 [UNREADY]가 보이면 /orch repair O2 후에 sync/next 진행\n"
        "\n"
        "4) Todo 큐 시드(seed)\n"
        f"- {p}sync O2 1h   # 단일 프로젝트 모드\n"
        f"- {p}sync all 1h  # 전체 갱신\n"
        f"- {p}sync         # 직전 sync 인자 재사용(채팅별)\n"
        "\n"
        "5) 실행\n"
        f"- {p}next     # active 프로젝트에서 하나 실행\n"
        f"- {p}fanout   # 프로젝트별 1개씩 global wave\n"
        f"- {p}todo proposals   # TF가 만든 follow-up inbox 확인\n"
        f"- {p}todo accept PROP-001 | {p}todo reject PROP-001\n"
        "\n"
        "6) 퇴근 모드(off-desk)\n"
        f"- {p}offdesk prepare\n"
        f"- {p}offdesk on\n"
        f"- {p}auto status\n"
        f"- {p}panic    # 긴급 중지\n"
        f"- {p}todo syncback preview   # TODO.md에 반영될 변경사항 미리보기\n"
        "\n"
        "팁\n"
        f"- '{p}'만 보내면 커맨드 메뉴가 열린다\n"
        f"- {p}dispatch 또는 {p}direct는 다음 메시지 1회 평문 허용\n"
        f"- 단일 프로젝트 작업은 보통 {p}use -> {p}sync O# -> {p}next 흐름이 안전하다\n"
        f"- 다시 전역 스케줄링하려면 {p}focus off\n"
    )


def handle_chat_management_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    mode_setting: Optional[str],
    lang_setting: Optional[str],
    report_setting: Optional[str],
    send: Callable[..., bool],
    get_default_mode: Callable[[Dict[str, Any], str], str],
    get_pending_mode: Callable[[Dict[str, Any], str], str],
    get_chat_lang: Callable[[Dict[str, Any], str, str], str],
    get_chat_report_level: Callable[[Dict[str, Any], str, str], str],
    set_default_mode: Callable[[Dict[str, Any], str, str], None],
    set_pending_mode: Callable[[Dict[str, Any], str, str], None],
    set_chat_lang: Callable[[Dict[str, Any], str, str], None],
    set_chat_report_level: Callable[[Dict[str, Any], str, str], None],
    clear_default_mode: Callable[[Dict[str, Any], str], bool],
    clear_pending_mode: Callable[[Dict[str, Any], str], bool],
    clear_confirm_action: Callable[[Dict[str, Any], str], bool],
    clear_chat_report_level: Callable[[Dict[str, Any], str], bool],
    save_manager_state: Callable[..., None],
    cmd_prefix: Callable[[], str],
) -> bool:
    if cmd == "tutorial":
        ui_lang = get_chat_lang(manager_state, chat_id, "ko")
        send(tutorial_text(lang=ui_lang, cmd_prefix=cmd_prefix()), context="tutorial", with_menu=True)
        return True

    if cmd == "mode":
        current_default_mode = get_default_mode(manager_state, chat_id)
        current_pending_mode = get_pending_mode(manager_state, chat_id)
        requested_mode = str(mode_setting or "").strip().lower() or "status"
        if requested_mode not in {"status", "dispatch", "direct", "off"}:
            raise RuntimeError("usage: /mode [on|off|direct|dispatch]")

        if requested_mode == "status":
            send(
                "routing mode\n"
                f"- default_mode: {current_default_mode or 'off'}\n"
                f"- one_shot_pending: {current_pending_mode or 'none'}\n"
                "- set: /mode on | /mode direct | /mode off\n"
                "- shortcut: /on | /off\n"
                "- tip: /mode on = 자동 라우팅(질문은 direct, 작업은 TF)\n"
                "- tip: /mode direct = direct 우선, 하지만 강한 작업 요청은 TF로 승격됩니다.",
                context="mode-status",
                with_menu=True,
            )
            return True

        if chat_role == "readonly":
            send(
                "permission denied: readonly chat cannot change routing mode.\n"
                "read-only: /mode (status only)",
                context="mode-deny",
                with_menu=True,
            )
            return True

        if requested_mode == "off":
            existed_default = clear_default_mode(manager_state, chat_id)
            cleared_pending = clear_pending_mode(manager_state, chat_id)
            cleared_confirm = clear_confirm_action(manager_state, chat_id)
            if not args.dry_run:
                save_manager_state(args.manager_state_file, manager_state)
            send(
                "routing mode updated\n"
                "- default_mode: off\n"
                f"- changed: {'yes' if existed_default else 'no'}\n"
                f"- one_shot_pending_cleared: {'yes' if cleared_pending else 'no'}\n"
                f"- confirm_request_cleared: {'yes' if cleared_confirm else 'no'}",
                context="mode-off",
                with_menu=True,
            )
            return True

        set_default_mode(manager_state, chat_id, requested_mode)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        body = (
            "routing mode updated\n"
            f"- default_mode: {requested_mode}\n"
            f"- one_shot_pending: {current_pending_mode or 'none'}\n"
        )
        if requested_mode == "dispatch":
            body += "- input_behavior: plain text -> auto routing (question=direct, work=TF)\n"
        else:
            body += "- input_behavior: plain text -> direct-biased auto routing\n"
        body += "- disable: /mode off (or /off)"
        send(body, context="mode-set", with_menu=True)
        return True

    if cmd == "lang":
        fallback_lang = str(getattr(args, "default_lang", "ko") or "ko").strip().lower()
        current_lang = get_chat_lang(manager_state, chat_id, fallback_lang)
        requested_lang = str(lang_setting or "").strip().lower() or "status"
        if requested_lang not in {"status", "ko", "en"}:
            raise RuntimeError("usage: /lang [ko|en]")

        if requested_lang == "status":
            send(
                "interface language\n"
                f"- current: {current_lang}\n"
                f"- default: {fallback_lang}\n"
                "- set: /lang ko | /lang en",
                context="lang-status",
                with_menu=True,
            )
            return True

        if chat_role == "readonly":
            send(
                "permission denied: readonly chat cannot change interface language.\n"
                "read-only: /lang (status only)",
                context="lang-deny",
                with_menu=True,
            )
            return True

        set_chat_lang(manager_state, chat_id, requested_lang)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            "interface language updated\n"
            f"- ui_language: {requested_lang}\n"
            "- usage: /lang ko | /lang en",
            context="lang-set",
            with_menu=True,
        )
        return True

    if cmd == "report":
        fallback_level = str(getattr(args, "default_report_level", "normal") or "normal").strip().lower()
        current_level = get_chat_report_level(manager_state, chat_id, fallback_level)
        requested_level = str(report_setting or "").strip().lower() or "status"
        if requested_level not in {"status", "short", "normal", "long", "off"}:
            raise RuntimeError("usage: /report [short|normal|long|off]")

        if requested_level == "status":
            send(
                "report verbosity\n"
                f"- current: {current_level}\n"
                f"- default: {fallback_level}\n"
                "- set: /report short | /report normal | /report long\n"
                "- reset: /report off\n"
                "- note: short=요약(합성 응답 생략), normal=기본(합성), long=역할별 원문(합성 생략)",
                context="report-status",
                with_menu=True,
            )
            return True

        if chat_role == "readonly":
            send(
                "permission denied: readonly chat cannot change report verbosity.\n"
                "read-only: /report (status only)",
                context="report-deny",
                with_menu=True,
            )
            return True

        if requested_level == "off":
            existed = clear_chat_report_level(manager_state, chat_id)
            if not args.dry_run:
                save_manager_state(args.manager_state_file, manager_state)
            send(
                "report verbosity updated\n"
                "- report_level: default\n"
                f"- changed: {'yes' if existed else 'no'}",
                context="report-off",
                with_menu=True,
            )
            return True

        set_chat_report_level(manager_state, chat_id, requested_level)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            "report verbosity updated\n"
            f"- report_level: {requested_level}\n"
            "- show: /report",
            context="report-set",
            with_menu=True,
        )
        return True

    if cmd == "quick-dispatch":
        set_pending_mode(manager_state, chat_id, "dispatch")
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            "dispatch 모드 활성화: 다음 메시지 1개를 팀 작업으로 배정합니다.\n"
            "바로 실행: /dispatch <요청>\n"
            "취소: /cancel",
            context="quick-dispatch",
            with_menu=True,
        )
        return True

    if cmd == "quick-direct":
        set_pending_mode(manager_state, chat_id, "direct")
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            "direct 모드 활성화: 다음 메시지 1개를 오케스트레이터가 직접 답변합니다.\n"
            "바로 실행: /direct <질문>\n"
            "취소: /cancel",
            context="quick-direct",
            with_menu=True,
        )
        return True

    if cmd == "cancel-pending":
        existed = clear_pending_mode(manager_state, chat_id)
        cleared_confirm = clear_confirm_action(manager_state, chat_id)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            (
                "대기 모드/확인 요청을 해제했습니다."
                if (existed or cleared_confirm)
                else "해제할 대기 모드나 확인 요청이 없습니다."
            ),
            context="cancel-pending",
            with_menu=True,
        )
        return True

    return False
