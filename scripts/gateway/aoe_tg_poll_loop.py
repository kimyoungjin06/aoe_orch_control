#!/usr/bin/env python3
"""Telegram polling loop helpers extracted from the gateway monolith."""

from __future__ import annotations

import sys
import time
from typing import Any, Callable, Dict, Iterable, Optional, Set, Tuple


def iter_message_updates(updates: Iterable[Dict[str, Any]]) -> Iterable[Tuple[int, Dict[str, Any]]]:
    for upd in updates:
        if not isinstance(upd, dict):
            continue
        update_id = upd.get("update_id")
        if not isinstance(update_id, int):
            continue
        msg = upd.get("message")
        if isinstance(msg, dict):
            yield update_id, msg


def run_simulation(
    args: Any,
    token: str,
    *,
    handle_text_message: Callable[..., Any],
) -> None:
    chat_id = str(args.simulate_chat_id)
    if args.verbose:
        print(f"[SIM] chat_id={chat_id} text={args.simulate_text}")
    original_dry = bool(args.dry_run)
    if not bool(getattr(args, "simulate_live", False)):
        args.dry_run = True
    try:
        handle_text_message(args, token, chat_id, args.simulate_text, trace_id=f"sim-{int(time.time() * 1000)}")
    finally:
        args.dry_run = original_dry


def run_loop(
    args: Any,
    token: str,
    *,
    load_state: Callable[..., Dict[str, Any]],
    save_state: Callable[..., Any],
    dedup_keep_limit: Callable[[], int],
    normalize_recent_tokens: Callable[[Any, int], Any],
    message_dedup_key: Callable[[Dict[str, Any]], str],
    append_recent_token: Callable[[Any, str, int], Any],
    tg_get_updates: Callable[..., Any],
    ensure_chat_allowed: Callable[..., bool],
    is_bootstrap_allowed_command: Callable[[str], bool],
    safe_tg_send_text: Callable[..., Any],
    log_gateway_event: Callable[..., Any],
    handle_text_message: Callable[..., Any],
    preferred_command_prefix: Callable[[], str],
    state_acked_updates_key: str,
    state_handled_messages_key: str,
    state_duplicate_skipped_key: str,
    state_empty_skipped_key: str,
    state_unauthorized_skipped_key: str,
    state_handler_errors_key: str,
    error_auth: str,
) -> int:
    state = load_state(args.state_file)
    offset = int(state.get("offset", 0) or 0)
    processed = int(state.get("processed", 0) or 0)
    acked_updates = int(state.get(state_acked_updates_key, processed) or 0)
    handled_messages = int(state.get(state_handled_messages_key, processed) or 0)
    duplicate_skipped = int(state.get(state_duplicate_skipped_key, 0) or 0)
    empty_skipped = int(state.get(state_empty_skipped_key, 0) or 0)
    unauthorized_skipped = int(state.get(state_unauthorized_skipped_key, 0) or 0)
    handler_errors = int(state.get(state_handler_errors_key, 0) or 0)
    dedup_keep = dedup_keep_limit()
    seen_update_ids = normalize_recent_tokens(state.get("seen_update_ids"), dedup_keep)
    seen_message_keys = normalize_recent_tokens(state.get("seen_message_keys"), dedup_keep)
    seen_update_set = set(seen_update_ids)
    seen_message_set = set(seen_message_keys)
    state["seen_update_ids"] = seen_update_ids
    state["seen_message_keys"] = seen_message_keys
    state["offset"] = offset
    state[state_acked_updates_key] = acked_updates
    state[state_handled_messages_key] = handled_messages
    state[state_duplicate_skipped_key] = duplicate_skipped
    state[state_empty_skipped_key] = empty_skipped
    state[state_unauthorized_skipped_key] = unauthorized_skipped
    state[state_handler_errors_key] = handler_errors
    state["processed"] = handled_messages

    unauthorized_sent: Set[str] = set()

    while True:
        try:
            updates = tg_get_updates(
                token=token,
                offset=offset,
                poll_timeout_sec=args.poll_timeout_sec,
                timeout_sec=args.http_timeout_sec,
            )
        except Exception as e:
            if args.verbose:
                print(f"[ERROR] getUpdates failed: {e}", file=sys.stderr, flush=True)
            time.sleep(2)
            continue

        handled_any = False

        for update_id, msg in iter_message_updates(updates):
            handled_any = True
            offset = max(offset, update_id + 1)
            state["offset"] = offset

            chat = msg.get("chat") if isinstance(msg.get("chat"), dict) else {}
            chat_id = str(chat.get("id", ""))
            chat_type = str(chat.get("type", "") or "").strip().lower()
            sender = msg.get("from") if isinstance(msg.get("from"), dict) else {}
            sender_id = str(sender.get("id", ""))
            text = str(msg.get("text", "") or "")
            msg_key = message_dedup_key(msg)
            update_token = str(update_id)
            duplicate = (update_token in seen_update_set) or (bool(msg_key) and msg_key in seen_message_set)

            append_recent_token(seen_update_ids, update_token, dedup_keep)
            seen_update_set.add(update_token)
            if msg_key:
                append_recent_token(seen_message_keys, msg_key, dedup_keep)
                seen_message_set.add(msg_key)
            acked_updates += 1
            state[state_acked_updates_key] = acked_updates

            if duplicate:
                duplicate_skipped += 1
                state[state_duplicate_skipped_key] = duplicate_skipped
                state["processed"] = handled_messages
                save_state(args.state_file, state)
                if args.verbose:
                    print(f"[SKIP] duplicate update_id={update_id} message_key={msg_key or '-'}")
                if not args.dry_run:
                    log_gateway_event(
                        team_dir=args.team_dir,
                        event="duplicate_update_skipped",
                        trace_id=f"upd-{update_id}",
                        stage="intake",
                        actor=f"telegram:{chat_id or '-'}",
                        status="skipped",
                        detail=f"message_key={msg_key or '-'}",
                    )
                continue

            if not chat_id or not text:
                empty_skipped += 1
                state[state_empty_skipped_key] = empty_skipped
                state["processed"] = handled_messages
                save_state(args.state_file, state)
                continue

            if args.verbose:
                preview = text if len(text) <= 120 else text[:117] + "..."
                print(f"[UPDATE] update_id={update_id} chat_id={chat_id} text={preview}")

            allowed = False
            if bool(getattr(args, "owner_only", False)):
                owner = str(getattr(args, "owner_chat_id", "") or "").strip()
                allowed = bool(owner) and (chat_type == "private") and (chat_id == owner) and (sender_id == owner)
            else:
                allowed = ensure_chat_allowed(
                    chat_id,
                    args.allow_chat_ids,
                    args.admin_chat_ids,
                    args.readonly_chat_ids,
                    bool(args.deny_by_default),
                    getattr(args, "owner_chat_id", ""),
                )
            bootstrap_allowed = False
            acl_empty = (not args.allow_chat_ids) and (not args.admin_chat_ids) and (not args.readonly_chat_ids)
            if (not allowed) and (not bool(getattr(args, "owner_only", False))) and bool(args.deny_by_default) and acl_empty:
                bootstrap_allowed = is_bootstrap_allowed_command(text)
                if bootstrap_allowed:
                    allowed = True

            if not allowed:
                unauthorized_skipped += 1
                state[state_unauthorized_skipped_key] = unauthorized_skipped
                state["processed"] = handled_messages
                save_state(args.state_file, state)
                if args.verbose:
                    print(f"[SKIP] unauthorized chat_id={chat_id}")
                if chat_id not in unauthorized_sent:
                    unauthorized_text = "not allowed."
                    if bool(getattr(args, "owner_only", False)):
                        unauthorized_text = "not allowed. owner-only mode: DM the bot from the owner account."
                    elif bool(args.deny_by_default) and acl_empty:
                        unauthorized_text = "not allowed. gateway is locked. use /lockme to claim this bot."
                    safe_tg_send_text(
                        token=token,
                        chat_id=chat_id,
                        text=unauthorized_text,
                        max_chars=args.max_text_chars,
                        timeout_sec=args.http_timeout_sec,
                        dry_run=args.dry_run,
                        verbose=args.verbose,
                        context="unauthorized",
                    )
                    log_gateway_event(
                        team_dir=args.team_dir,
                        event="unauthorized_message",
                        trace_id=f"upd-{update_id}",
                        stage="intake",
                        actor=f"telegram:{chat_id}",
                        status="rejected",
                        error_code=error_auth,
                        detail=text if len(text) <= 200 else (text[:197] + "..."),
                    )
                    unauthorized_sent.add(chat_id)
                continue

            state["processed"] = handled_messages
            save_state(args.state_file, state)
            try:
                handle_text_message(args, token, chat_id, text, trace_id=f"upd-{update_id}")
            except Exception as e:
                handler_errors += 1
                state[state_handler_errors_key] = handler_errors
                if args.verbose:
                    print(f"[ERROR] message handling failed: chat_id={chat_id} error={e}", file=sys.stderr, flush=True)
            handled_messages += 1
            processed = handled_messages
            state[state_handled_messages_key] = handled_messages
            state["processed"] = handled_messages
            save_state(args.state_file, state)

        if handled_any:
            state["offset"] = offset
            state["processed"] = handled_messages
            save_state(args.state_file, state)

        if args.once:
            break

    return 0
