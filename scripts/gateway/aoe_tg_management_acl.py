#!/usr/bin/env python3
"""ACL and identity management command helpers for Telegram gateway."""

from typing import Any, Callable, Dict

from aoe_tg_acl import (
    format_csv_set,
    parse_acl_command_args,
    parse_acl_revoke_args,
    resolve_role_from_acl_sets,
)


def handle_acl_management_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    current_chat_alias: str,
    rest: str,
    came_from_slash: bool,
    acl_grant_scope: str | None,
    acl_grant_chat_id: str | None,
    acl_revoke_scope: str | None,
    acl_revoke_chat_id: str | None,
    send: Callable[..., bool],
    log_event: Callable[..., None],
    get_default_mode: Callable[[Dict[str, Any], str], str],
    get_pending_mode: Callable[[Dict[str, Any], str], str],
    get_chat_lang: Callable[[Dict[str, Any], str, str], str],
    get_chat_report_level: Callable[[Dict[str, Any], str, str], str],
    resolve_chat_role: Callable[[str, Any], str],
    is_owner_chat: Callable[[str, Any], bool],
    ensure_chat_aliases: Callable[..., Dict[str, str]],
    find_chat_alias: Callable[[Dict[str, str], str], str],
    alias_table_summary: Callable[[Any], str],
    resolve_chat_ref: Callable[[Any, str], tuple[str, str]],
    ensure_chat_alias: Callable[..., str],
    sync_acl_env_file: Callable[[Any], None],
    project_lock_label: Callable[[Dict[str, Any]], str],
) -> bool:
    if cmd == "whoami":
        if bool(getattr(args, "owner_only", False)):
            current_allow = "(ignored: owner-only)"
        elif args.allow_chat_ids:
            current_allow = ",".join(sorted(args.allow_chat_ids))
        else:
            current_allow = "(empty: locked)" if bool(args.deny_by_default) else "(empty: all chats allowed)"
        role = resolve_chat_role(chat_id, args)
        current_default_mode = get_default_mode(manager_state, chat_id)
        current_pending_mode = get_pending_mode(manager_state, chat_id)
        current_lang = get_chat_lang(manager_state, chat_id, str(getattr(args, "default_lang", "ko") or "ko"))
        current_report = get_chat_report_level(
            manager_state,
            chat_id,
            str(getattr(args, "default_report_level", "normal") or "normal"),
        )
        owner_chat_id = str(args.owner_chat_id or "").strip() or "(unset)"
        send(
            "telegram identity\n"
            f"- chat_id: {chat_id}\n"
            f"- alias: {current_chat_alias or '-'}\n"
            f"- role: {role}\n"
            f"- project_lock: {project_lock_label(manager_state) or 'off'}\n"
            f"- owner_chat_id: {owner_chat_id}\n"
            f"- owner_only: {'yes' if bool(getattr(args, 'owner_only', False)) else 'no'}\n"
            f"- is_owner: {'yes' if is_owner_chat(chat_id, args) else 'no'}\n"
            f"- allowlist: {current_allow}\n"
            f"- deny_by_default: {'yes' if bool(args.deny_by_default) else 'no'}\n"
            f"- default_mode: {current_default_mode or 'off'}\n"
            f"- one_shot_pending: {current_pending_mode or 'none'}\n"
            f"- ui_language: {current_lang}\n"
            f"- report_level: {current_report}\n"
            "- lock: /lockme\n"
            "- mode: /mode\n"
            "- lang: /lang\n"
            "- report: /report\n"
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

    if cmd == "onlyme":
        prev_allow = ",".join(sorted(args.allow_chat_ids)) if args.allow_chat_ids else "-"
        prev_admin = ",".join(sorted(args.admin_chat_ids)) if args.admin_chat_ids else "-"
        prev_readonly = ",".join(sorted(args.readonly_chat_ids)) if args.readonly_chat_ids else "-"
        prev_owner = str(args.owner_chat_id or "").strip() or "-"
        prev_owner_only = "yes" if bool(getattr(args, "owner_only", False)) else "no"
        prev_deny = "yes" if bool(getattr(args, "deny_by_default", False)) else "no"

        args.allow_chat_ids = {str(chat_id)}
        args.admin_chat_ids = set()
        args.readonly_chat_ids = set()
        args.owner_chat_id = str(chat_id)
        args.deny_by_default = True
        args.owner_only = True
        if str(getattr(args, "owner_bootstrap_mode", "") or "").strip().lower() not in {"dispatch", "direct"}:
            args.owner_bootstrap_mode = "dispatch"

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
                f"action=onlyme prev_allow={prev_allow} prev_admin={prev_admin} prev_readonly={prev_readonly} "
                f"prev_owner={prev_owner} prev_owner_only={prev_owner_only} prev_deny={prev_deny} "
                f"next_allow={chat_id} next_owner={chat_id} next_owner_only=yes next_deny=yes"
            ),
        )

        msg = (
            "access locked (owner-only).\n"
            f"- owner_chat_id: {chat_id}\n"
            "- owner_only: yes (private DM only)\n"
            "- deny_by_default: yes\n"
            "- cleared_admin_readonly: yes\n"
            "- apply_now: yes\n"
            f"- persist_on_restart: {'yes' if not persist_error else 'no'}\n"
            "- next: /whoami, /mode on, then plain text"
        )
        if persist_error:
            msg += f"\n- persist_error: {persist_error[:180]}"
        send(msg, context="onlyme", with_menu=True)
        return True

    return False
