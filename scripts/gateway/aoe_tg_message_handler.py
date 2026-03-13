#!/usr/bin/env python3
"""High-level Telegram message orchestration extracted from the gateway monolith."""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple


def handle_text_message(
    args: Any,
    token: str,
    chat_id: str,
    text: str,
    trace_id: str = "",
    *,
    deps: Dict[str, Any],
) -> None:
    started_at = time.time()
    message_trace_id = str(trace_id or "").strip() or f"chat-{chat_id}-{int(started_at * 1000)}"
    try:
        args._aoe_trace_id = message_trace_id
        args._aoe_invocation = "auto" if message_trace_id.startswith("auto-") else "chat"
    except Exception:
        pass

    raw_text = str(text or "")
    text_preview = raw_text if len(raw_text) <= 200 else raw_text[:197] + "..."
    text_preview = deps["mask_sensitive_text"](text_preview)
    resolved = deps["ResolvedCommand"]()
    run_transition = deps["RunTransitionState"]()

    manager_state = deps["load_manager_state"](args.manager_state_file, args.project_root, args.team_dir)
    deps["ensure_default_project_registered"](manager_state, args.project_root, args.team_dir)

    try:
        owner_bootstrap_mode = str(getattr(args, "owner_bootstrap_mode", "") or "").strip().lower()
        if owner_bootstrap_mode and deps["is_owner_chat"](chat_id, args):
            if not deps["get_default_mode"](manager_state, chat_id):
                deps["set_default_mode"](manager_state, chat_id, owner_bootstrap_mode)
                if not args.dry_run:
                    deps["save_manager_state"](args.manager_state_file, manager_state)
    except Exception:
        pass

    default_log_team_dir = args.team_dir
    root_log_team_dir = Path(str(args.team_dir)).expanduser().resolve()
    try:
        _key0, _entry0 = deps["get_manager_project"](manager_state, None)
        default_log_team_dir = Path(str(_entry0.get("team_dir", str(args.team_dir)))).expanduser().resolve()
    except Exception:
        default_log_team_dir = args.team_dir
    log_ctx: Dict[str, Path] = {"team_dir": default_log_team_dir}

    def elapsed_ms() -> int:
        return max(0, int((time.time() - started_at) * 1000))

    def log_event(
        event: str,
        project: str = "",
        request_id: str = "",
        task: Optional[Dict[str, Any]] = None,
        stage: str = "",
        status: str = "",
        error_code: str = "",
        detail: str = "",
    ) -> None:
        if args.dry_run:
            return
        deps["log_gateway_event"](
            team_dir=log_ctx["team_dir"],
            mirror_team_dir=root_log_team_dir,
            event=event,
            trace_id=message_trace_id,
            project=project,
            request_id=request_id,
            task=task,
            stage=stage,
            actor=f"telegram:{chat_id}",
            status=status,
            error_code=error_code,
            latency_ms=elapsed_ms(),
            detail=detail,
        )
        try:
            deps["room_autopublish_event"](
                team_dir=args.team_dir,
                manager_state=manager_state,
                chat_id=chat_id,
                event=event,
                project=project,
                request_id=request_id,
                task=task,
                stage=stage,
                status=status,
                error_code=error_code,
                detail=detail,
            )
        except Exception:
            pass

    def send(
        body: str,
        context: str = "",
        with_menu: bool = False,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> bool:
        retries = deps["int_from_env"](os.environ.get("AOE_TG_SEND_RETRIES"), default=2, minimum=0, maximum=8)
        base_delay_ms = deps["int_from_env"](
            os.environ.get("AOE_TG_SEND_RETRY_DELAY_MS"), default=300, minimum=50, maximum=5000
        )
        attempt = 0
        ok = False
        if reply_markup is None and with_menu:
            reply_markup = deps["build_quick_reply_keyboard"]()
        while True:
            attempt += 1
            ok = deps["safe_tg_send_text"](
                token=token,
                chat_id=chat_id,
                text=body,
                max_chars=args.max_text_chars,
                timeout_sec=args.http_timeout_sec,
                dry_run=args.dry_run,
                verbose=args.verbose,
                context=context,
                reply_markup=reply_markup,
            )
            if ok or attempt > retries:
                break
            delay = (base_delay_ms * (2 ** (attempt - 1))) / 1000.0
            time.sleep(min(8.0, delay))
        log_event(
            event="send_message",
            status="sent" if ok else "failed",
            error_code="" if ok else deps["ERROR_TELEGRAM"],
            detail=(
                f"context={context} with_menu={'yes' if with_menu else 'no'} "
                f"chars={len(str(body or ''))} attempts={attempt}"
            ),
        )
        return ok

    def get_context(name_override: Optional[str]) -> Tuple[str, Dict[str, Any], Any]:
        key, entry = deps["get_manager_project"](manager_state, name_override)
        p_args = deps["make_project_args"](args, entry, key=key)
        log_ctx["team_dir"] = p_args.team_dir
        return key, entry, p_args

    def _skip_synth() -> str:
        raise RuntimeError("synth disabled by report_level")

    try:
        log_event(event="incoming_message", status="received", stage="intake", detail=text_preview)
        resolved = deps["resolve_message_command"](
            text=text,
            slash_only=bool(args.slash_only),
            manager_state=manager_state,
            chat_id=chat_id,
            dry_run=bool(args.dry_run),
            manager_state_file=args.manager_state_file,
            get_pending_mode=deps["get_pending_mode"],
            get_default_mode=deps["get_default_mode"],
            clear_pending_mode=deps["clear_pending_mode"],
            save_manager_state=deps["save_manager_state"],
        )
        chat_ui_lang = deps["get_chat_lang"](manager_state, chat_id, str(args.default_lang))
        chat_report_level = deps["get_chat_report_level"](
            manager_state,
            chat_id,
            str(getattr(args, "default_report_level", deps["DEFAULT_REPORT_LEVEL"]) or deps["DEFAULT_REPORT_LEVEL"]),
        )

        if not resolved.cmd and bool(args.slash_only):
            p = deps["preferred_command_prefix"]()
            if chat_ui_lang == "en":
                slash_hint = (
                    "Input format: command-prefix only.\n"
                    f"Example: {p}dispatch <request>, {p}direct <question>, {p}mode on, {p}lang en, {p}monitor, {p}check, {p}task, {p}pick, {p}map, {p}help\n"
                    f"Tip: {p}dispatch or {p}direct enables one-shot plain text for the next message; {p}mode sets default plain-text routing."
                )
            else:
                slash_hint = (
                    "입력 형식: prefix 명령만 지원합니다.\n"
                    f"예시: {p}dispatch <요청>, {p}direct <질문>, {p}mode on, {p}lang en, {p}monitor, {p}check, {p}task, {p}pick, {p}map, {p}help\n"
                    f"참고: {p}dispatch 또는 {p}direct는 다음 메시지 1회 평문 허용, {p}mode는 기본 평문 라우팅 모드를 고정합니다."
                )
            send(slash_hint, context="slash-only-hint", with_menu=True)
            log_event(
                event="input_rejected",
                stage="intake",
                status="rejected",
                error_code=deps["ERROR_COMMAND"],
                detail="slash_only",
            )
            return

        cmd_key = resolved.cmd or "run-default"
        if cmd_key == "replay":
            replay_scope = str(resolved.rest or "").strip().lower()
            replay_action = replay_scope.split(" ", 1)[0] if replay_scope else ""
            if replay_action in {"", "list", "ls", "status", "show"}:
                cmd_key = "replay-read"
            else:
                cmd_key = "replay-write"
        log_event(event="command_resolved", stage="intake", status="accepted", detail=f"cmd={cmd_key}")

        chat_role = deps["resolve_chat_role"](chat_id, args)
        if deps["enforce_command_auth"](
            cmd_key=cmd_key,
            chat_role=chat_role,
            chat_id=chat_id,
            args=args,
            send=send,
            log_event=log_event,
            is_owner_chat=deps["is_owner_chat"],
            readonly_allowed_commands=deps["READONLY_ALLOWED_COMMANDS"],
            error_auth_code=deps["ERROR_AUTH"],
        ):
            return

        current_chat_alias = deps["ensure_chat_alias"](args, chat_id, persist=(not args.dry_run))
        chat_reply_lang = deps["normalize_chat_lang_token"](
            str(args.default_reply_lang), deps["DEFAULT_REPLY_LANG"]
        ) or deps["DEFAULT_REPLY_LANG"]

        if resolved.cmd == "replay":
            deps["handle_replay_command"](
                args=args,
                token=token,
                chat_id=chat_id,
                target=resolved.rest,
                send=send,
                log_event=log_event,
            )
            return

        if resolved.cmd == "drain":
            deps["handle_drain_command"](
                args=args,
                token=token,
                chat_id=chat_id,
                rest=resolved.rest,
                trace_id=message_trace_id,
                send=send,
                log_event=log_event,
            )
            return

        if resolved.cmd == "fanout":
            deps["handle_fanout_command"](
                args=args,
                token=token,
                chat_id=chat_id,
                rest=resolved.rest,
                trace_id=message_trace_id,
                send=send,
                log_event=log_event,
            )
            return

        if resolved.cmd == "gc":
            deps["handle_gc_command"](
                args=args,
                chat_id=chat_id,
                rest=resolved.rest,
                manager_state=manager_state,
                send=send,
                log_event=log_event,
            )
            return

        confirm_transition = deps["resolve_confirm_run_transition"](
            cmd=resolved.cmd,
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            orch_target=resolved.orch_target,
            send=send,
            get_confirm_action=deps["get_confirm_action"],
            parse_iso_ts=deps["parse_iso_ts"],
            clear_confirm_action=deps["clear_confirm_action"],
            save_manager_state=deps["save_manager_state"],
        )
        if deps["apply_confirm_transition_to_resolved"](resolved, confirm_transition):
            return

        non_run_ctx = deps["build_non_run_context"](
            resolved=resolved,
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            chat_role=chat_role,
            current_chat_alias=current_chat_alias,
        )
        non_run_deps = deps["build_non_run_deps"](
            send=send,
            log_event=log_event,
            get_context=get_context,
            save_manager_state=deps["save_manager_state"],
            help_text=lambda: deps["help_text"](chat_ui_lang),
            get_default_mode=deps["get_default_mode"],
            get_pending_mode=deps["get_pending_mode"],
            get_chat_lang=deps["get_chat_lang"],
            get_chat_report_level=deps["get_chat_report_level"],
            get_chat_room=deps["get_chat_room"],
            set_default_mode=deps["set_default_mode"],
            set_pending_mode=deps["set_pending_mode"],
            set_chat_lang=deps["set_chat_lang"],
            set_chat_report_level=deps["set_chat_report_level"],
            set_chat_room=deps["set_chat_room"],
            clear_default_mode=deps["clear_default_mode"],
            clear_pending_mode=deps["clear_pending_mode"],
            clear_confirm_action=deps["clear_confirm_action"],
            clear_chat_report_level=deps["clear_chat_report_level"],
            resolve_chat_role=deps["resolve_chat_role"],
            is_owner_chat=deps["is_owner_chat"],
            ensure_chat_aliases=deps["ensure_chat_aliases"],
            find_chat_alias=deps["find_chat_alias"],
            alias_table_summary=deps["alias_table_summary"],
            resolve_chat_ref=deps["resolve_chat_ref"],
            ensure_chat_alias=deps["ensure_chat_alias"],
            sync_acl_env_file=deps["sync_acl_env_file"],
            summarize_orch_registry=deps["summarize_orch_registry"],
            backfill_task_aliases=deps["backfill_task_aliases"],
            latest_task_request_refs=deps["latest_task_request_refs"],
            set_chat_recent_task_refs=deps["set_chat_recent_task_refs"],
            get_chat_selected_task_ref=deps["get_chat_selected_task_ref"],
            set_chat_selected_task_ref=deps["set_chat_selected_task_ref"],
            summarize_task_monitor=deps["summarize_task_monitor"],
            summarize_gateway_metrics=deps["summarize_gateway_metrics"],
            get_manager_project=deps["get_manager_project"],
            resolve_project_root=deps["resolve_project_root"],
            is_path_within=deps["is_path_within"],
            register_orch_project=deps["register_orch_project"],
            run_aoe_init=deps["run_aoe_init"],
            run_aoe_spawn=deps["run_aoe_spawn"],
            now_iso=deps["now_iso"],
            run_aoe_status=deps["run_aoe_status"],
            resolve_chat_task_ref=deps["resolve_chat_task_ref"],
            resolve_task_request_id=deps["resolve_task_request_id"],
            run_request_query=deps["run_request_query"],
            sync_task_lifecycle=deps["sync_task_lifecycle"],
            resolve_verifier_candidates=deps["resolve_verifier_candidates"],
            touch_chat_recent_task_ref=deps["touch_chat_recent_task_ref"],
            get_task_record=deps["get_task_record"],
            summarize_request_state=deps["summarize_request_state"],
            summarize_three_stage_request=deps["summarize_three_stage_request"],
            summarize_task_lifecycle=deps["summarize_task_lifecycle"],
            task_display_label=deps["task_display_label"],
            cancel_request_assignments=deps["cancel_request_assignments"],
            lifecycle_set_stage=deps["lifecycle_set_stage"],
            summarize_cancel_result=deps["summarize_cancel_result"],
            dedupe_roles=deps["dedupe_roles"],
            run_aoe_add_role=deps["run_aoe_add_role"],
        )
        non_run_result = deps["handle_non_run_command_pipeline"](ctx=non_run_ctx, deps=non_run_deps)
        if non_run_result.terminal:
            return
        if deps["apply_retry_transition_to_resolved"](resolved, run_transition, non_run_result.retry_transition):
            return

        run_ctx = deps["build_run_context"](
            cmd=resolved.cmd,
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            text=text,
            rest=resolved.rest,
            orch_target=resolved.orch_target,
            run_prompt=resolved.run_prompt,
            run_roles_override=resolved.run_roles_override,
            run_priority_override=resolved.run_priority_override,
            run_timeout_override=resolved.run_timeout_override,
            run_no_wait_override=resolved.run_no_wait_override,
            run_force_mode=resolved.run_force_mode,
            run_auto_source=resolved.run_auto_source,
            run_control_mode=run_transition.run_control_mode,
            run_source_request_id=run_transition.run_source_request_id,
            run_source_task=run_transition.run_source_task,
            run_selected_execution_lane_ids=run_transition.run_selected_execution_lane_ids,
            run_selected_review_lane_ids=run_transition.run_selected_review_lane_ids,
        )
        run_deps = deps["build_run_deps"](
            send=send,
            log_event=log_event,
            help_text=lambda: deps["help_text"](chat_ui_lang),
            summarize_chat_usage=deps["summarize_chat_usage"],
            detect_high_risk_prompt=deps["detect_high_risk_prompt"],
            set_confirm_action=deps["set_confirm_action"],
            save_manager_state=deps["save_manager_state"],
            get_context=get_context,
            choose_auto_dispatch_roles=deps["choose_auto_dispatch_roles"],
            resolve_verifier_candidates=deps["resolve_verifier_candidates"],
            load_orchestrator_roles=deps["load_orchestrator_roles"],
            parse_roles_csv=deps["parse_roles_csv"],
            ensure_verifier_roles=deps["ensure_verifier_roles"],
            available_worker_roles=deps["available_worker_roles"],
            normalize_task_plan_payload=deps["normalize_task_plan_payload"],
            build_task_execution_plan=deps["build_task_execution_plan"],
            critique_task_execution_plan=deps["critique_task_execution_plan"],
            critic_has_blockers=deps["critic_has_blockers"],
            repair_task_execution_plan=deps["repair_task_execution_plan"],
            plan_roles_from_subtasks=deps["plan_roles_from_subtasks"],
            build_planned_dispatch_prompt=deps["build_planned_dispatch_prompt"],
            phase1_ensemble_planning=deps["run_phase1_ensemble_planning"],
            run_orchestrator_direct=lambda p_args, prompt: deps["run_orchestrator_direct"](
                p_args,
                prompt,
                reply_lang=chat_reply_lang,
            ),
            run_aoe_orch=deps["run_aoe_orch"],
            create_request_id=deps["create_request_id"],
            ensure_task_record=deps["ensure_task_record"],
            finalize_request_reply_messages=deps["finalize_request_reply_messages"],
            touch_chat_recent_task_ref=deps["touch_chat_recent_task_ref"],
            set_chat_selected_task_ref=deps["set_chat_selected_task_ref"],
            now_iso=deps["now_iso"],
            sync_task_lifecycle=deps["sync_task_lifecycle"],
            lifecycle_set_stage=deps["lifecycle_set_stage"],
            summarize_task_lifecycle=deps["summarize_task_lifecycle"],
            synthesize_orchestrator_response=(
                lambda p_args, prompt, state: deps["synthesize_orchestrator_response"](
                    p_args,
                    prompt,
                    state,
                    reply_lang=chat_reply_lang,
                )
                if chat_report_level == "normal"
                else _skip_synth()
            ),
            critique_task_result=lambda p_args, prompt, state, task, attempt_no, max_attempts: deps[
                "critique_task_execution_result"
            ](
                p_args,
                prompt,
                state,
                task=task,
                attempt_no=attempt_no,
                max_attempts=max_attempts,
                reply_lang=chat_reply_lang,
            ),
            extract_todo_proposals=lambda p_args, prompt, state, task=None: deps["extract_followup_todo_proposals"](
                p_args,
                prompt,
                state,
                task=task,
                reply_lang=chat_reply_lang,
            ),
            merge_todo_proposals=deps["merge_todo_proposals"],
            render_run_response=lambda state, task=None: deps["render_run_response"](
                state,
                task=task,
                report_level=chat_report_level,
            ),
        )

        if deps["handle_run_or_unknown_command"](ctx=run_ctx, deps=run_deps):
            return

    except Exception as e:
        if getattr(args, "verbose", False):
            try:
                import traceback

                traceback.print_exc()
            except Exception:
                pass
        error_code, user_msg, next_step = deps["classify_handler_error"](e)
        replay_hint = ""
        if str(raw_text or "").strip():
            try:
                loop_state = deps["load_state"](args.state_file)
                item = deps["enqueue_failed_message"](
                    loop_state,
                    chat_id=chat_id,
                    text=raw_text,
                    trace_id=message_trace_id,
                    error_code=error_code,
                    error_detail=str(e),
                    cmd=resolved.cmd,
                )
                deps["save_state"](args.state_file, loop_state)
                rid = str(item.get("id", "")).strip()
                if rid:
                    p = deps["preferred_command_prefix"]()
                    replay_hint = f"\nreplay: {p}replay {rid}"
            except Exception:
                replay_hint = ""
        send(
            deps["format_error_message"](error_code, user_msg, next_step, detail=str(e)) + replay_hint,
            context="handler error",
            with_menu=True,
        )
        log_event(
            event="handler_error",
            stage="close",
            status="failed",
            error_code=error_code,
            detail=str(e),
        )
