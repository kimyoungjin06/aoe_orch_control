#!/usr/bin/env python3
"""Command pipeline orchestration for Telegram gateway."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from aoe_tg_command_resolver import ResolvedCommand
from aoe_tg_management_handlers import handle_management_command
from aoe_tg_orch_discovery import (
    discover_workspace_projects_from_aoe,
    seed_team_dir_from_template,
    unique_project_key,
)
from aoe_tg_orch_overview_handlers import handle_orch_overview_command
from aoe_tg_orch_task_handlers import handle_orch_task_command
from aoe_tg_room_handlers import RoomDeps, handle_room_command
from aoe_tg_retry_handlers import resolve_retry_replan_transition
from aoe_tg_role_handlers import handle_add_role_command
from aoe_tg_scheduler_handlers import handle_scheduler_command
from aoe_tg_tf_handlers import handle_tf_command
from aoe_tg_todo_handlers import handle_todo_command


@dataclass
class NonRunCommandResult:
    terminal: bool = False
    retry_transition: Optional[Dict[str, Any]] = None


@dataclass
class NonRunContext:
    resolved: ResolvedCommand
    args: Any
    manager_state: Dict[str, Any]
    chat_id: str
    chat_role: str
    current_chat_alias: str


@dataclass
class NonRunDeps:
    send: Callable[..., bool]
    log_event: Callable[..., None]
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]]
    save_manager_state: Callable[..., None]
    help_text: Callable[[], str]
    get_default_mode: Callable[[Dict[str, Any], str], str]
    get_pending_mode: Callable[[Dict[str, Any], str], str]
    get_chat_lang: Callable[[Dict[str, Any], str, str], str]
    get_chat_report_level: Callable[[Dict[str, Any], str, str], str]
    get_chat_room: Callable[[Dict[str, Any], str, str], str]
    set_default_mode: Callable[[Dict[str, Any], str, str], None]
    set_pending_mode: Callable[[Dict[str, Any], str, str], None]
    set_chat_lang: Callable[[Dict[str, Any], str, str], None]
    set_chat_report_level: Callable[[Dict[str, Any], str, str], None]
    set_chat_room: Callable[[Dict[str, Any], str, str], None]
    clear_default_mode: Callable[[Dict[str, Any], str], bool]
    clear_pending_mode: Callable[[Dict[str, Any], str], bool]
    clear_confirm_action: Callable[[Dict[str, Any], str], bool]
    clear_chat_report_level: Callable[[Dict[str, Any], str], bool]
    resolve_chat_role: Callable[[str, Any], str]
    is_owner_chat: Callable[[str, Any], bool]
    ensure_chat_aliases: Callable[..., Dict[str, str]]
    find_chat_alias: Callable[[Dict[str, str], str], str]
    alias_table_summary: Callable[[Any], str]
    resolve_chat_ref: Callable[[Any, str], tuple[str, str]]
    ensure_chat_alias: Callable[..., str]
    sync_acl_env_file: Callable[[Any], None]
    summarize_orch_registry: Callable[[Dict[str, Any]], str]
    backfill_task_aliases: Callable[[Dict[str, Any]], None]
    latest_task_request_refs: Callable[..., list[str]]
    set_chat_recent_task_refs: Callable[..., None]
    get_chat_selected_task_ref: Callable[..., str]
    set_chat_selected_task_ref: Callable[..., None]
    summarize_task_monitor: Callable[..., str]
    summarize_gateway_metrics: Callable[..., str]
    get_manager_project: Callable[[Dict[str, Any], Optional[str]], tuple[str, Dict[str, Any]]]
    resolve_project_root: Callable[[str], Any]
    is_path_within: Callable[[Any, Any], bool]
    register_orch_project: Callable[..., tuple[str, Dict[str, Any]]]
    run_aoe_init: Callable[..., str]
    run_aoe_spawn: Callable[..., str]
    now_iso: Callable[[], str]
    run_aoe_status: Callable[[Any], str]
    resolve_chat_task_ref: Callable[..., str]
    resolve_task_request_id: Callable[[Dict[str, Any], str], str]
    run_request_query: Callable[[Any, str], Dict[str, Any]]
    sync_task_lifecycle: Callable[..., Optional[Dict[str, Any]]]
    resolve_verifier_candidates: Callable[[str], List[str]]
    touch_chat_recent_task_ref: Callable[..., None]
    get_task_record: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]]
    summarize_request_state: Callable[..., str]
    summarize_three_stage_request: Callable[..., str]
    summarize_task_lifecycle: Callable[..., str]
    task_display_label: Callable[..., str]
    cancel_request_assignments: Callable[..., Dict[str, Any]]
    lifecycle_set_stage: Callable[..., None]
    summarize_cancel_result: Callable[..., str]
    dedupe_roles: Callable[[Any], List[str]]
    run_aoe_add_role: Callable[..., str]


_ORCH_DISCOVERY_TRIGGER_CMDS = {
    # Registry views/actions
    "orch-list",
    "orch-use",
    "orch-pause",
    "orch-resume",
    "orch-hide",
    "orch-unhide",
    "orch-repair",
    "orch-status",
    "orch-monitor",
    "orch-kpi",
    # Global orchestration (needs full registry)
    "sync",
    "queue",
    "next",
    "fanout",
    "auto",
    "offdesk",
}


def _auto_discover_orchs_if_enabled(*, cmd: str, args: Any, manager_state: Dict[str, Any], deps: NonRunDeps) -> None:
    if cmd not in _ORCH_DISCOVERY_TRIGGER_CMDS:
        return
    if not bool(getattr(args, "orch_auto_discover", False)):
        return

    projects = manager_state.get("projects")
    if not isinstance(projects, dict):
        return

    ws_raw = getattr(args, "workspace_root", None)
    if not ws_raw:
        return
    try:
        workspace_root = Path(str(ws_raw)).expanduser().resolve()
    except Exception:
        return
    if not workspace_root.exists() or not workspace_root.is_dir():
        return

    existing_roots: set[Path] = set()
    existing_keys: set[str] = set(str(k) for k in projects.keys())
    for _k, entry in projects.items():
        if not isinstance(entry, dict):
            continue
        pr = str(entry.get("project_root", "")).strip()
        if not pr:
            continue
        try:
            existing_roots.add(Path(pr).expanduser().resolve())
        except Exception:
            continue

    discovered = discover_workspace_projects_from_aoe(workspace_root=workspace_root)
    if not discovered:
        return

    changed = False
    tpl = Path(str(getattr(args, "project_root", ".") or ".")).expanduser().resolve() / "templates/aoe-team/AOE_TODO.md"

    for root, meta in sorted(discovered.items(), key=lambda kv: str(kv[0])):
        if root in existing_roots:
            continue

        display_name = str(meta.get("display_name", "")).strip() or root.name
        key = unique_project_key(display_name or root.name, existing_keys)
        existing_keys.add(key)

        team_dir = (root / ".aoe-team").resolve()
        deps.register_orch_project(
            manager_state,
            name=key,
            project_root=root,
            team_dir=team_dir,
            overview="",
            set_active=False,
        )
        # Keep display_name human-friendly even if key got suffixed.
        proj = manager_state.get("projects", {}).get(key)
        if isinstance(proj, dict):
            proj["display_name"] = display_name

        if bool(getattr(args, "orch_auto_init", False)):
            seed_team_dir_from_template(team_dir=team_dir, template_path=tpl)

        existing_roots.add(root)
        changed = True

    if changed and not bool(getattr(args, "dry_run", False)):
        deps.save_manager_state(args.manager_state_file, manager_state)


def build_non_run_context(
    *,
    resolved: ResolvedCommand,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    current_chat_alias: str,
) -> NonRunContext:
    return NonRunContext(
        resolved=resolved,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        chat_role=chat_role,
        current_chat_alias=current_chat_alias,
    )


def build_non_run_deps(
    *,
    send: Callable[..., bool],
    log_event: Callable[..., None],
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]],
    save_manager_state: Callable[..., None],
    help_text: Callable[[], str],
    get_default_mode: Callable[[Dict[str, Any], str], str],
    get_pending_mode: Callable[[Dict[str, Any], str], str],
    get_chat_lang: Callable[[Dict[str, Any], str, str], str],
    get_chat_report_level: Callable[[Dict[str, Any], str, str], str],
    get_chat_room: Callable[[Dict[str, Any], str, str], str],
    set_default_mode: Callable[[Dict[str, Any], str, str], None],
    set_pending_mode: Callable[[Dict[str, Any], str, str], None],
    set_chat_lang: Callable[[Dict[str, Any], str, str], None],
    set_chat_report_level: Callable[[Dict[str, Any], str, str], None],
    set_chat_room: Callable[[Dict[str, Any], str, str], None],
    clear_default_mode: Callable[[Dict[str, Any], str], bool],
    clear_pending_mode: Callable[[Dict[str, Any], str], bool],
    clear_confirm_action: Callable[[Dict[str, Any], str], bool],
    clear_chat_report_level: Callable[[Dict[str, Any], str], bool],
    resolve_chat_role: Callable[[str, Any], str],
    is_owner_chat: Callable[[str, Any], bool],
    ensure_chat_aliases: Callable[..., Dict[str, str]],
    find_chat_alias: Callable[[Dict[str, str], str], str],
    alias_table_summary: Callable[[Any], str],
    resolve_chat_ref: Callable[[Any, str], tuple[str, str]],
    ensure_chat_alias: Callable[..., str],
    sync_acl_env_file: Callable[[Any], None],
    summarize_orch_registry: Callable[[Dict[str, Any]], str],
    backfill_task_aliases: Callable[[Dict[str, Any]], None],
    latest_task_request_refs: Callable[..., list[str]],
    set_chat_recent_task_refs: Callable[..., None],
    get_chat_selected_task_ref: Callable[..., str],
    set_chat_selected_task_ref: Callable[..., None],
    summarize_task_monitor: Callable[..., str],
    summarize_gateway_metrics: Callable[..., str],
    get_manager_project: Callable[[Dict[str, Any], Optional[str]], tuple[str, Dict[str, Any]]],
    resolve_project_root: Callable[[str], Any],
    is_path_within: Callable[[Any, Any], bool],
    register_orch_project: Callable[..., tuple[str, Dict[str, Any]]],
    run_aoe_init: Callable[..., str],
    run_aoe_spawn: Callable[..., str],
    now_iso: Callable[[], str],
    run_aoe_status: Callable[[Any], str],
    resolve_chat_task_ref: Callable[..., str],
    resolve_task_request_id: Callable[[Dict[str, Any], str], str],
    run_request_query: Callable[[Any, str], Dict[str, Any]],
    sync_task_lifecycle: Callable[..., Optional[Dict[str, Any]]],
    resolve_verifier_candidates: Callable[[str], List[str]],
    touch_chat_recent_task_ref: Callable[..., None],
    get_task_record: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    summarize_request_state: Callable[..., str],
    summarize_three_stage_request: Callable[..., str],
    summarize_task_lifecycle: Callable[..., str],
    task_display_label: Callable[..., str],
    cancel_request_assignments: Callable[..., Dict[str, Any]],
    lifecycle_set_stage: Callable[..., None],
    summarize_cancel_result: Callable[..., str],
    dedupe_roles: Callable[[Any], List[str]],
    run_aoe_add_role: Callable[..., str],
) -> NonRunDeps:
    return NonRunDeps(
        send=send,
        log_event=log_event,
        get_context=get_context,
        save_manager_state=save_manager_state,
        help_text=help_text,
        get_default_mode=get_default_mode,
        get_pending_mode=get_pending_mode,
        get_chat_lang=get_chat_lang,
        get_chat_report_level=get_chat_report_level,
        get_chat_room=get_chat_room,
        set_default_mode=set_default_mode,
        set_pending_mode=set_pending_mode,
        set_chat_lang=set_chat_lang,
        set_chat_report_level=set_chat_report_level,
        set_chat_room=set_chat_room,
        clear_default_mode=clear_default_mode,
        clear_pending_mode=clear_pending_mode,
        clear_confirm_action=clear_confirm_action,
        clear_chat_report_level=clear_chat_report_level,
        resolve_chat_role=resolve_chat_role,
        is_owner_chat=is_owner_chat,
        ensure_chat_aliases=ensure_chat_aliases,
        find_chat_alias=find_chat_alias,
        alias_table_summary=alias_table_summary,
        resolve_chat_ref=resolve_chat_ref,
        ensure_chat_alias=ensure_chat_alias,
        sync_acl_env_file=sync_acl_env_file,
        summarize_orch_registry=summarize_orch_registry,
        backfill_task_aliases=backfill_task_aliases,
        latest_task_request_refs=latest_task_request_refs,
        set_chat_recent_task_refs=set_chat_recent_task_refs,
        get_chat_selected_task_ref=get_chat_selected_task_ref,
        set_chat_selected_task_ref=set_chat_selected_task_ref,
        summarize_task_monitor=summarize_task_monitor,
        summarize_gateway_metrics=summarize_gateway_metrics,
        get_manager_project=get_manager_project,
        resolve_project_root=resolve_project_root,
        is_path_within=is_path_within,
        register_orch_project=register_orch_project,
        run_aoe_init=run_aoe_init,
        run_aoe_spawn=run_aoe_spawn,
        now_iso=now_iso,
        run_aoe_status=run_aoe_status,
        resolve_chat_task_ref=resolve_chat_task_ref,
        resolve_task_request_id=resolve_task_request_id,
        run_request_query=run_request_query,
        sync_task_lifecycle=sync_task_lifecycle,
        resolve_verifier_candidates=resolve_verifier_candidates,
        touch_chat_recent_task_ref=touch_chat_recent_task_ref,
        get_task_record=get_task_record,
        summarize_request_state=summarize_request_state,
        summarize_three_stage_request=summarize_three_stage_request,
        summarize_task_lifecycle=summarize_task_lifecycle,
        task_display_label=task_display_label,
        cancel_request_assignments=cancel_request_assignments,
        lifecycle_set_stage=lifecycle_set_stage,
        summarize_cancel_result=summarize_cancel_result,
        dedupe_roles=dedupe_roles,
        run_aoe_add_role=run_aoe_add_role,
    )


def handle_non_run_command_pipeline(
    *,
    ctx: NonRunContext,
    deps: NonRunDeps,
) -> NonRunCommandResult:
    resolved = ctx.resolved
    args = ctx.args
    manager_state = ctx.manager_state
    chat_id = ctx.chat_id
    chat_role = ctx.chat_role
    current_chat_alias = ctx.current_chat_alias

    _auto_discover_orchs_if_enabled(cmd=resolved.cmd, args=args, manager_state=manager_state, deps=deps)

    if handle_management_command(
        cmd=resolved.cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        chat_role=chat_role,
        current_chat_alias=current_chat_alias,
        mode_setting=resolved.mode_setting,
        lang_setting=resolved.lang_setting,
        report_setting=resolved.report_setting,
        rest=resolved.rest,
        came_from_slash=resolved.came_from_slash,
        acl_grant_scope=resolved.acl_grant_scope,
        acl_grant_chat_id=resolved.acl_grant_chat_id,
        acl_revoke_scope=resolved.acl_revoke_scope,
        acl_revoke_chat_id=resolved.acl_revoke_chat_id,
        send=deps.send,
        log_event=deps.log_event,
        help_text=deps.help_text,
        get_default_mode=deps.get_default_mode,
        get_pending_mode=deps.get_pending_mode,
        get_chat_lang=deps.get_chat_lang,
        get_chat_report_level=deps.get_chat_report_level,
        get_chat_room=deps.get_chat_room,
        set_default_mode=deps.set_default_mode,
        set_pending_mode=deps.set_pending_mode,
        set_chat_lang=deps.set_chat_lang,
        set_chat_report_level=deps.set_chat_report_level,
        set_chat_room=deps.set_chat_room,
        clear_default_mode=deps.clear_default_mode,
        clear_pending_mode=deps.clear_pending_mode,
        clear_confirm_action=deps.clear_confirm_action,
        clear_chat_report_level=deps.clear_chat_report_level,
        save_manager_state=deps.save_manager_state,
        resolve_chat_role=deps.resolve_chat_role,
        is_owner_chat=deps.is_owner_chat,
        ensure_chat_aliases=deps.ensure_chat_aliases,
        find_chat_alias=deps.find_chat_alias,
        alias_table_summary=deps.alias_table_summary,
        resolve_chat_ref=deps.resolve_chat_ref,
        ensure_chat_alias=deps.ensure_chat_alias,
        sync_acl_env_file=deps.sync_acl_env_file,
    ):
        return NonRunCommandResult(terminal=True)

    if handle_room_command(
        cmd=resolved.cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        chat_role=chat_role,
        rest=resolved.rest,
        deps=RoomDeps(
            send=deps.send,
            now_iso=deps.now_iso,
            get_chat_room=deps.get_chat_room,
            set_chat_room=deps.set_chat_room,
            save_manager_state=deps.save_manager_state,
        ),
    ):
        return NonRunCommandResult(terminal=True)

    if handle_orch_overview_command(
        cmd=resolved.cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        orch_target=resolved.orch_target,
        orch_monitor_limit=resolved.orch_monitor_limit,
        orch_kpi_hours=resolved.orch_kpi_hours,
        rest=resolved.rest,
        send=deps.send,
        get_context=deps.get_context,
        save_manager_state=deps.save_manager_state,
        now_iso=deps.now_iso,
        summarize_orch_registry=deps.summarize_orch_registry,
        backfill_task_aliases=deps.backfill_task_aliases,
        latest_task_request_refs=deps.latest_task_request_refs,
        set_chat_recent_task_refs=deps.set_chat_recent_task_refs,
        get_chat_selected_task_ref=deps.get_chat_selected_task_ref,
        set_chat_selected_task_ref=deps.set_chat_selected_task_ref,
        summarize_task_monitor=deps.summarize_task_monitor,
        summarize_gateway_metrics=deps.summarize_gateway_metrics,
        get_manager_project=deps.get_manager_project,
    ):
        return NonRunCommandResult(terminal=True)

    tf_transition = handle_tf_command(
        cmd=resolved.cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        chat_role=chat_role,
        rest=resolved.rest,
        send=deps.send,
        get_context=deps.get_context,
    )
    if isinstance(tf_transition, dict):
        return NonRunCommandResult(
            terminal=bool(tf_transition.get("terminal", True)),
            retry_transition=tf_transition,
        )

    scheduler_transition = handle_scheduler_command(
        cmd=resolved.cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        chat_role=chat_role,
        orch_target=resolved.orch_target,
        rest=resolved.rest,
        send=deps.send,
        get_context=deps.get_context,
        save_manager_state=deps.save_manager_state,
        now_iso=deps.now_iso,
    )
    if isinstance(scheduler_transition, dict):
        return NonRunCommandResult(
            terminal=bool(scheduler_transition.get("terminal", True)),
            retry_transition=scheduler_transition,
        )

    todo_transition = handle_todo_command(
        cmd=resolved.cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        chat_role=chat_role,
        orch_target=resolved.orch_target,
        rest=resolved.rest,
        send=deps.send,
        get_context=deps.get_context,
        save_manager_state=deps.save_manager_state,
        now_iso=deps.now_iso,
    )
    if isinstance(todo_transition, dict):
        return NonRunCommandResult(
            terminal=bool(todo_transition.get("terminal", True)),
            retry_transition=todo_transition,
        )

    if handle_orch_task_command(
        cmd=resolved.cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        orch_target=resolved.orch_target,
        orch_add_name=resolved.orch_add_name,
        orch_add_path=resolved.orch_add_path,
        orch_add_overview=resolved.orch_add_overview,
        orch_add_init=resolved.orch_add_init,
        orch_add_spawn=resolved.orch_add_spawn,
        orch_add_set_active=resolved.orch_add_set_active,
        rest=resolved.rest,
        orch_check_request_id=resolved.orch_check_request_id,
        orch_task_request_id=resolved.orch_task_request_id,
        orch_pick_request_id=resolved.orch_pick_request_id,
        orch_cancel_request_id=resolved.orch_cancel_request_id,
        send=deps.send,
        log_event=deps.log_event,
        get_context=deps.get_context,
        latest_task_request_refs=deps.latest_task_request_refs,
        set_chat_recent_task_refs=deps.set_chat_recent_task_refs,
        save_manager_state=deps.save_manager_state,
        resolve_project_root=deps.resolve_project_root,
        is_path_within=deps.is_path_within,
        register_orch_project=deps.register_orch_project,
        run_aoe_init=deps.run_aoe_init,
        run_aoe_spawn=deps.run_aoe_spawn,
        now_iso=deps.now_iso,
        run_aoe_status=deps.run_aoe_status,
        resolve_chat_task_ref=deps.resolve_chat_task_ref,
        resolve_task_request_id=deps.resolve_task_request_id,
        run_request_query=deps.run_request_query,
        sync_task_lifecycle=deps.sync_task_lifecycle,
        resolve_verifier_candidates=deps.resolve_verifier_candidates,
        touch_chat_recent_task_ref=deps.touch_chat_recent_task_ref,
        set_chat_selected_task_ref=deps.set_chat_selected_task_ref,
        get_chat_selected_task_ref=deps.get_chat_selected_task_ref,
        get_task_record=deps.get_task_record,
        summarize_request_state=deps.summarize_request_state,
        summarize_three_stage_request=deps.summarize_three_stage_request,
        summarize_task_lifecycle=deps.summarize_task_lifecycle,
        task_display_label=deps.task_display_label,
        cancel_request_assignments=deps.cancel_request_assignments,
        lifecycle_set_stage=deps.lifecycle_set_stage,
        summarize_cancel_result=deps.summarize_cancel_result,
    ):
        return NonRunCommandResult(terminal=True)

    retry_transition = resolve_retry_replan_transition(
        cmd=resolved.cmd,
        args=args,
        manager_state=manager_state,
        chat_id=chat_id,
        orch_target=resolved.orch_target,
        orch_retry_request_id=resolved.orch_retry_request_id,
        orch_replan_request_id=resolved.orch_replan_request_id,
        send=deps.send,
        get_context=deps.get_context,
        get_chat_selected_task_ref=deps.get_chat_selected_task_ref,
        resolve_chat_task_ref=deps.resolve_chat_task_ref,
        resolve_task_request_id=deps.resolve_task_request_id,
        get_task_record=deps.get_task_record,
        run_request_query=deps.run_request_query,
        sync_task_lifecycle=deps.sync_task_lifecycle,
        resolve_verifier_candidates=deps.resolve_verifier_candidates,
        dedupe_roles=deps.dedupe_roles,
        touch_chat_recent_task_ref=deps.touch_chat_recent_task_ref,
        set_chat_selected_task_ref=deps.set_chat_selected_task_ref,
    )
    if isinstance(retry_transition, dict):
        return NonRunCommandResult(
            terminal=bool(retry_transition.get("terminal")),
            retry_transition=retry_transition,
        )

    if handle_add_role_command(
        cmd=resolved.cmd,
        args=args,
        add_role_name=resolved.add_role_name,
        add_role_provider=resolved.add_role_provider,
        add_role_launch=resolved.add_role_launch,
        add_role_spawn=resolved.add_role_spawn,
        send=deps.send,
        get_context=deps.get_context,
        run_aoe_add_role=deps.run_aoe_add_role,
    ):
        return NonRunCommandResult(terminal=True)

    return NonRunCommandResult()
