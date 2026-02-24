#!/usr/bin/env python3
"""Management command handlers for Telegram gateway."""

from typing import Any, Callable, Dict, Optional

from aoe_tg_acl import (
    format_csv_set,
    parse_acl_command_args,
    parse_acl_revoke_args,
    resolve_role_from_acl_sets,
)

def handle_management_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    current_chat_alias: str,
    mode_setting: Optional[str],
    rest: str,
    came_from_slash: bool,
    acl_grant_scope: Optional[str],
    acl_grant_chat_id: Optional[str],
    acl_revoke_scope: Optional[str],
    acl_revoke_chat_id: Optional[str],
    send: Callable[..., bool],
    log_event: Callable[..., None],
    help_text: Callable[[], str],
    get_default_mode: Callable[[Dict[str, Any], str], str],
    get_pending_mode: Callable[[Dict[str, Any], str], str],
    set_default_mode: Callable[[Dict[str, Any], str, str], None],
    set_pending_mode: Callable[[Dict[str, Any], str, str], None],
    clear_default_mode: Callable[[Dict[str, Any], str], bool],
    clear_pending_mode: Callable[[Dict[str, Any], str], bool],
    clear_confirm_action: Callable[[Dict[str, Any], str], bool],
    save_manager_state: Callable[..., None],
    resolve_chat_role: Callable[[str, Any], str],
    is_owner_chat: Callable[[str, Any], bool],
    ensure_chat_aliases: Callable[..., Dict[str, str]],
    find_chat_alias: Callable[[Dict[str, str], str], str],
    alias_table_summary: Callable[[Any], str],
    resolve_chat_ref: Callable[[Any, str], tuple[str, str]],
    ensure_chat_alias: Callable[..., str],
    sync_acl_env_file: Callable[[Any], None],
) -> bool:
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
                "- tip: /mode on 후에는 평문을 바로 작업으로 보낼 수 있습니다.",
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
        send(
            "routing mode updated\n"
            f"- default_mode: {requested_mode}\n"
            f"- one_shot_pending: {current_pending_mode or 'none'}\n"
            f"- input_behavior: plain text -> {requested_mode}\n"
            "- disable: /mode off (or /off)",
            context="mode-set",
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

    if cmd == "whoami":
        if args.allow_chat_ids:
            current_allow = ",".join(sorted(args.allow_chat_ids))
        else:
            current_allow = "(empty: locked)" if bool(args.deny_by_default) else "(empty: all chats allowed)"
        role = resolve_chat_role(chat_id, args)
        current_default_mode = get_default_mode(manager_state, chat_id)
        current_pending_mode = get_pending_mode(manager_state, chat_id)
        owner_chat_id = str(args.owner_chat_id or "").strip() or "(unset)"
        send(
            "telegram identity\n"
            f"- chat_id: {chat_id}\n"
            f"- alias: {current_chat_alias or '-'}\n"
            f"- role: {role}\n"
            f"- owner_chat_id: {owner_chat_id}\n"
            f"- is_owner: {'yes' if is_owner_chat(chat_id, args) else 'no'}\n"
            f"- allowlist: {current_allow}\n"
            f"- deny_by_default: {'yes' if bool(args.deny_by_default) else 'no'}\n"
            f"- default_mode: {current_default_mode or 'off'}\n"
            f"- one_shot_pending: {current_pending_mode or 'none'}\n"
            "- lock: /lockme\n"
            "- mode: /mode\n"
            "- acl: /acl",
            context="whoami",
            with_menu=True,
        )
        return True

    if cmd == "acl":
        aliases = ensure_chat_aliases(
            args,
            set(args.allow_chat_ids) | set(args.admin_chat_ids) | set(args.readonly_chat_ids) | {str(chat_id)},
            persist=(not args.dry_run),
        )
        allow_rows = format_csv_set(args.allow_chat_ids) or "(empty)"
        admin_rows = format_csv_set(args.admin_chat_ids) or "(empty)"
        readonly_rows = format_csv_set(args.readonly_chat_ids) or "(empty)"
        role = resolve_chat_role(chat_id, args)
        send(
            "access control list\n"
            f"- deny_by_default: {'yes' if bool(args.deny_by_default) else 'no'}\n"
            f"- my_chat_id: {chat_id}\n"
            f"- my_alias: {find_chat_alias(aliases, chat_id) or current_chat_alias or '-'}\n"
            f"- my_role: {role}\n"
            f"- owner_chat_id: {str(args.owner_chat_id or '').strip() or '(unset)'}\n"
            f"- allow: {allow_rows}\n"
            f"- admin: {admin_rows}\n"
            f"- readonly: {readonly_rows}\n"
            f"- aliases: {alias_table_summary(args)}\n"
            "commands:\n"
            "- /grant <allow|admin|readonly> <chat_id|alias>\n"
            "- /revoke <allow|admin|readonly|all> <chat_id|alias>",
            context="acl",
            with_menu=True,
        )
        return True

    if cmd == "grant":
        scope = str(acl_grant_scope or "").strip().lower()
        target_chat_ref = str(acl_grant_chat_id or "").strip()
        if (not scope or not target_chat_ref) and came_from_slash:
            scope, target_chat_ref = parse_acl_command_args(
                rest,
                "usage: /grant <allow|admin|readonly> <chat_id|alias>",
            )
        if not scope or not target_chat_ref:
            raise RuntimeError("usage: aoe grant <allow|admin|readonly> <chat_id|alias>")

        target_chat_id, target_alias = resolve_chat_ref(args, target_chat_ref)

        if scope == "allow":
            args.allow_chat_ids.add(target_chat_id)
            args.readonly_chat_ids.discard(target_chat_id)
        elif scope == "admin":
            args.admin_chat_ids.add(target_chat_id)
            args.readonly_chat_ids.discard(target_chat_id)
        elif scope == "readonly":
            args.readonly_chat_ids.add(target_chat_id)
            args.allow_chat_ids.discard(target_chat_id)
            args.admin_chat_ids.discard(target_chat_id)
        else:
            raise RuntimeError("usage: aoe grant <allow|admin|readonly> <chat_id|alias>")
        args.readonly_chat_ids = {
            x for x in args.readonly_chat_ids if (x not in args.admin_chat_ids) and (x not in args.allow_chat_ids)
        }
        target_alias = target_alias or ensure_chat_alias(args, target_chat_id, persist=(not args.dry_run))

        if not args.dry_run:
            sync_acl_env_file(args)

        target_role = resolve_role_from_acl_sets(
            chat_id=target_chat_id,
            allow_chat_ids=args.allow_chat_ids,
            admin_chat_ids=args.admin_chat_ids,
            readonly_chat_ids=args.readonly_chat_ids,
            deny_by_default=bool(args.deny_by_default),
        )
        log_event(
            event="acl_update",
            stage="intake",
            status="completed",
            detail=f"action=grant scope={scope} target={target_chat_id} alias={target_alias or '-'} by={chat_id}",
        )
        send(
            "acl updated\n"
            f"- action: grant\n"
            f"- scope: {scope}\n"
            f"- target: {(target_alias + ' (' + target_chat_id + ')') if target_alias else target_chat_id}\n"
            f"- role_now: {target_role}",
            context="grant",
            with_menu=True,
        )
        return True

    if cmd == "revoke":
        scope = str(acl_revoke_scope or "").strip().lower()
        target_chat_ref = str(acl_revoke_chat_id or "").strip()
        if (not scope or not target_chat_ref) and came_from_slash:
            scope, target_chat_ref = parse_acl_revoke_args(
                rest,
                "usage: /revoke <allow|admin|readonly|all> <chat_id|alias>",
            )
        if not scope or not target_chat_ref:
            raise RuntimeError("usage: aoe revoke <allow|admin|readonly|all> <chat_id|alias>")
        if scope not in {"allow", "admin", "readonly", "all"}:
            raise RuntimeError("usage: aoe revoke <allow|admin|readonly|all> <chat_id|alias>")
        target_chat_id, target_alias = resolve_chat_ref(args, target_chat_ref)

        next_allow = set(args.allow_chat_ids)
        next_admin = set(args.admin_chat_ids)
        next_readonly = set(args.readonly_chat_ids)

        if scope in {"allow", "all"}:
            next_allow.discard(target_chat_id)
        if scope in {"admin", "all"}:
            next_admin.discard(target_chat_id)
        if scope in {"readonly", "all"}:
            next_readonly.discard(target_chat_id)

        if bool(args.deny_by_default) and str(target_chat_id) == str(chat_id) and (not is_owner_chat(chat_id, args)):
            caller_after_role = resolve_role_from_acl_sets(
                chat_id=chat_id,
                allow_chat_ids=next_allow,
                admin_chat_ids=next_admin,
                readonly_chat_ids=next_readonly,
                deny_by_default=True,
            )
            if caller_after_role != "admin":
                send(
                    "blocked: self-revoke would remove admin access in deny-by-default mode.\n"
                    "next: /grant admin <other_chat_id|alias> 후 다시 시도하세요.",
                    context="revoke-guard",
                    with_menu=True,
                )
                return True

        args.allow_chat_ids = next_allow
        args.admin_chat_ids = next_admin
        args.readonly_chat_ids = {
            x for x in next_readonly if (x not in args.admin_chat_ids) and (x not in args.allow_chat_ids)
        }

        if not args.dry_run:
            sync_acl_env_file(args)

        target_role = resolve_role_from_acl_sets(
            chat_id=target_chat_id,
            allow_chat_ids=args.allow_chat_ids,
            admin_chat_ids=args.admin_chat_ids,
            readonly_chat_ids=args.readonly_chat_ids,
            deny_by_default=bool(args.deny_by_default),
        )
        log_event(
            event="acl_update",
            stage="intake",
            status="completed",
            detail=f"action=revoke scope={scope} target={target_chat_id} alias={target_alias or '-'} by={chat_id}",
        )
        send(
            "acl updated\n"
            f"- action: revoke\n"
            f"- scope: {scope}\n"
            f"- target: {(target_alias + ' (' + target_chat_id + ')') if target_alias else target_chat_id}\n"
            f"- role_now: {target_role}",
            context="revoke",
            with_menu=True,
        )
        return True

    if cmd == "lockme":
        prev_allow = ",".join(sorted(args.allow_chat_ids)) if args.allow_chat_ids else "-"
        prev_admin = ",".join(sorted(args.admin_chat_ids)) if args.admin_chat_ids else "-"
        prev_readonly = ",".join(sorted(args.readonly_chat_ids)) if args.readonly_chat_ids else "-"
        prev_owner = str(args.owner_chat_id or "").strip() or "-"
        args.allow_chat_ids = {str(chat_id)}
        args.admin_chat_ids = set()
        args.readonly_chat_ids = set()
        args.owner_chat_id = str(chat_id)

        persist_error = ""
        if not args.dry_run:
            try:
                sync_acl_env_file(args)
            except Exception as e:
                persist_error = str(e)

        log_event(
            event="allowlist_update",
            stage="intake",
            status="completed" if not persist_error else "partial",
            error_code="" if not persist_error else "E_INTERNAL",
            detail=(
                f"prev_allow={prev_allow} prev_admin={prev_admin} prev_readonly={prev_readonly} "
                f"prev_owner={prev_owner} next_allow={chat_id} next_owner={chat_id}"
            ),
        )

        msg = (
            "access locked to current chat.\n"
            f"- allowed_chat_id: {chat_id}\n"
            f"- owner_chat_id: {chat_id}\n"
            "- cleared_admin_readonly: yes\n"
            "- apply_now: yes\n"
            f"- persist_on_restart: {'yes' if not persist_error else 'no'}"
        )
        if persist_error:
            msg += f"\n- persist_error: {persist_error[:180]}"
        send(msg, context="lockme", with_menu=True)
        return True

    if cmd in {"start", "help", "orch-help"}:
        send(help_text(), context="help", with_menu=True)
        return True

    return False


