#!/usr/bin/env python3
"""Orchestrator task lifecycle handlers for Telegram gateway."""

from typing import Any, Callable, Dict, List, Optional

def handle_orch_task_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    orch_target: Optional[str],
    orch_add_name: Optional[str],
    orch_add_path: Optional[str],
    orch_add_overview: Optional[str],
    orch_add_init: bool,
    orch_add_spawn: bool,
    orch_add_set_active: bool,
    rest: str,
    orch_check_request_id: Optional[str],
    orch_task_request_id: Optional[str],
    orch_pick_request_id: Optional[str],
    orch_cancel_request_id: Optional[str],
    send: Callable[..., bool],
    log_event: Callable[..., None],
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]],
    save_manager_state: Callable[..., None],
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
    set_chat_selected_task_ref: Callable[..., None],
    get_chat_selected_task_ref: Callable[..., str],
    get_task_record: Callable[[Dict[str, Any], str], Optional[Dict[str, Any]]],
    summarize_request_state: Callable[..., str],
    summarize_three_stage_request: Callable[..., str],
    summarize_task_lifecycle: Callable[..., str],
    task_display_label: Callable[..., str],
    cancel_request_assignments: Callable[..., Dict[str, Any]],
    lifecycle_set_stage: Callable[..., None],
    summarize_cancel_result: Callable[..., str],
) -> bool:
    if cmd == "orch-add":
        if not orch_add_name or not orch_add_path:
            send(
                "usage: aoe orch add <name> --path <project_root> [--overview <text>] [--init|--no-init] [--spawn|--no-spawn]",
                context="orch-add usage",
            )
            return True

        project_root = resolve_project_root(orch_add_path)
        if args.workspace_root and not is_path_within(project_root, args.workspace_root):
            send(
                f"error: path must be under workspace root ({args.workspace_root})\npath={project_root}",
                context="orch-add path",
            )
            return True

        team_dir = project_root / ".aoe-team"
        overview = (orch_add_overview or "").strip() or f"{orch_add_name} project orchestration"

        if args.dry_run:
            send(
                "[DRY-RUN] orch add\n"
                f"- name: {orch_add_name}\n"
                f"- path: {project_root}\n"
                f"- team: {team_dir}\n"
                f"- init: {'yes' if orch_add_init else 'no'}\n"
                f"- spawn: {'yes' if orch_add_spawn else 'no'}\n"
                f"- set_active: {'yes' if orch_add_set_active else 'no'}",
                context="orch-add dry-run",
            )
            return True

        project_root.mkdir(parents=True, exist_ok=True)
        key, entry = register_orch_project(
            manager_state,
            name=orch_add_name,
            project_root=project_root,
            team_dir=team_dir,
            overview=overview,
            set_active=orch_add_set_active,
        )

        init_logs: List[str] = []
        cfg_exists = (team_dir / "orchestrator.json").exists()
        should_init = orch_add_init or (not cfg_exists)
        if should_init:
            init_logs.append(run_aoe_init(args, project_root=project_root, team_dir=team_dir, overview=overview))

        if orch_add_spawn:
            init_logs.append(run_aoe_spawn(args, project_root=project_root, team_dir=team_dir))

        entry["updated_at"] = now_iso()
        save_manager_state(args.manager_state_file, manager_state)

        lines = [
            f"orch ready: {key}",
            f"root: {entry.get('project_root')}",
            f"team: {entry.get('team_dir')}",
            f"active: {'yes' if manager_state.get('active') == key else 'no'}",
        ]
        if init_logs:
            lines.append("logs:")
            for row in init_logs:
                short = row.strip().splitlines()
                lines.append(short[-1] if short else "(empty)")
        send("\n".join(lines), context="orch-add")
        return True

    if cmd in {"status", "orch-status"}:
        key, entry, p_args = get_context(orch_target)
        status = run_aoe_status(p_args)
        send(
            f"orch: {key}\nroot: {entry.get('project_root')}\nteam: {entry.get('team_dir')}\nlast_request: {entry.get('last_request_id') or '-'}\n\n{status}",
            context="status",
        )
        return True

    if cmd == "request":
        if not rest:
            send("usage: /request <request_or_alias> | aoe request <request_or_alias>", context="request usage")
            return True
        key, entry, p_args = get_context(None)
        req_ref = resolve_chat_task_ref(manager_state, chat_id, key, rest)
        req_id = resolve_task_request_id(entry, req_ref)
        data = run_request_query(p_args, req_id)
        entry["last_request_id"] = str(data.get("request_id", req_id)).strip() or req_id
        entry["updated_at"] = now_iso()
        task = sync_task_lifecycle(
            entry=entry,
            request_data=data,
            prompt="",
            mode="dispatch",
            selected_roles=None,
            verifier_roles=None,
            require_verifier=bool(args.require_verifier),
            verifier_candidates=resolve_verifier_candidates(args.verifier_roles),
        )
        touch_chat_recent_task_ref(manager_state, chat_id, key, req_id)
        set_chat_selected_task_ref(manager_state, chat_id, key, req_id)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(f"orch: {key}\n" + summarize_request_state(data, task=task), context="request")
        return True

    if cmd == "orch-check":
        key, entry, p_args = get_context(orch_target)
        req_ref = (
            orch_check_request_id
            or get_chat_selected_task_ref(manager_state, chat_id, key)
            or str(entry.get("last_request_id", "")).strip()
            or ""
        ).strip()
        req_ref = resolve_chat_task_ref(manager_state, chat_id, key, req_ref)
        req_id = resolve_task_request_id(entry, req_ref)
        if not req_id:
            send(f"no request id. usage: aoe orch check [--orch <name>] [<request_or_alias>]\norch={key}", context="orch-check usage")
            return True
        data = run_request_query(p_args, req_id)
        entry["last_request_id"] = str(data.get("request_id", req_id)).strip() or req_id
        entry["updated_at"] = now_iso()
        task = sync_task_lifecycle(
            entry=entry,
            request_data=data,
            prompt="",
            mode="dispatch",
            selected_roles=None,
            verifier_roles=None,
            require_verifier=bool(args.require_verifier),
            verifier_candidates=resolve_verifier_candidates(args.verifier_roles),
        )
        touch_chat_recent_task_ref(manager_state, chat_id, key, req_id)
        set_chat_selected_task_ref(manager_state, chat_id, key, req_id)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(summarize_three_stage_request(key, data, task=task), context="orch-check")
        return True

    if cmd == "orch-task":
        key, entry, p_args = get_context(orch_target)
        req_ref = (
            orch_task_request_id
            or get_chat_selected_task_ref(manager_state, chat_id, key)
            or str(entry.get("last_request_id", "")).strip()
            or ""
        ).strip()
        req_ref = resolve_chat_task_ref(manager_state, chat_id, key, req_ref)
        req_id = resolve_task_request_id(entry, req_ref)
        if not req_id:
            send(f"no request id. usage: aoe orch task [--orch <name>] [<request_or_alias>]\norch={key}", context="orch-task usage")
            return True

        task = get_task_record(entry, req_id)
        if task is None:
            try:
                data = run_request_query(p_args, req_id)
                task = sync_task_lifecycle(
                    entry=entry,
                    request_data=data,
                    prompt="",
                    mode="dispatch",
                    selected_roles=None,
                    verifier_roles=None,
                    require_verifier=bool(args.require_verifier),
                    verifier_candidates=resolve_verifier_candidates(args.verifier_roles),
                )
                entry["last_request_id"] = str(data.get("request_id", req_id)).strip() or req_id
                entry["updated_at"] = now_iso()
            except Exception:
                task = None

        if task is None:
            send(f"no lifecycle record: request_or_alias={req_ref or req_id} (orch={key})", context="orch-task missing")
            return True

        touch_chat_recent_task_ref(manager_state, chat_id, key, req_id)
        set_chat_selected_task_ref(manager_state, chat_id, key, req_id)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(summarize_task_lifecycle(key, task), context="orch-task")
        return True

    if cmd == "orch-pick":
        key, entry, _p_args = get_context(orch_target)
        req_ref = str(orch_pick_request_id or "").strip()
        if not req_ref:
            send(
                "usage: /pick <number|request_or_alias> | aoe pick <number|request_or_alias>",
                context="orch-pick usage",
                with_menu=True,
            )
            return True
        req_ref = resolve_chat_task_ref(manager_state, chat_id, key, req_ref)
        req_id = resolve_task_request_id(entry, req_ref)
        if not req_id:
            send(f"task not found: {orch_pick_request_id} (orch={key})", context="orch-pick missing", with_menu=True)
            return True

        task = get_task_record(entry, req_id)
        set_chat_selected_task_ref(manager_state, chat_id, key, req_id)
        touch_chat_recent_task_ref(manager_state, chat_id, key, req_id)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)

        label = task_display_label(task or {}, fallback_request_id=req_id)
        send(
            "selected task updated\n"
            f"- orch: {key}\n"
            f"- task: {label}\n"
            f"- request_id: {req_id}\n"
            "next: /check, /task, /retry, /replan, /cancel",
            context="orch-pick",
            with_menu=True,
        )
        return True

    if cmd == "orch-cancel":
        key, entry, p_args = get_context(orch_target)
        req_ref = (
            orch_cancel_request_id
            or get_chat_selected_task_ref(manager_state, chat_id, key)
            or str(entry.get("last_request_id", "")).strip()
            or ""
        ).strip()
        req_ref = resolve_chat_task_ref(manager_state, chat_id, key, req_ref)
        req_id = resolve_task_request_id(entry, req_ref)
        if not req_id:
            send(
                f"no request id. usage: /cancel <request_or_alias> | aoe orch cancel [--orch <name>] [<request_or_alias>]\norch={key}",
                context="orch-cancel usage",
            )
            return True

        state_before = run_request_query(p_args, req_id)
        note = f"canceled by telegram:{chat_id}"
        cancel_result = cancel_request_assignments(p_args, state_before, note=note)
        try:
            state_after = run_request_query(p_args, req_id)
        except Exception:
            state_after = state_before

        entry["last_request_id"] = str(state_after.get("request_id", req_id)).strip() or req_id
        entry["updated_at"] = now_iso()
        task = sync_task_lifecycle(
            entry=entry,
            request_data=state_after,
            prompt="",
            mode="dispatch",
            selected_roles=None,
            verifier_roles=None,
            require_verifier=bool(args.require_verifier),
            verifier_candidates=resolve_verifier_candidates(args.verifier_roles),
        )
        if task is not None:
            lifecycle_set_stage(task, "execution", "failed", note=note)
            lifecycle_set_stage(task, "verification", "failed", note=note)
            lifecycle_set_stage(task, "integration", "failed", note=note)
            lifecycle_set_stage(task, "close", "failed", note=note)
            task["status"] = "failed"
            task["canceled"] = True
            task["canceled_at"] = now_iso()
            task["canceled_by"] = f"telegram:{chat_id}"
            task["updated_at"] = now_iso()

        touch_chat_recent_task_ref(manager_state, chat_id, key, req_id)
        set_chat_selected_task_ref(manager_state, chat_id, key, req_id)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)

        send(
            summarize_cancel_result(key, req_id, task=task, result=cancel_result),
            context="orch-cancel",
            with_menu=True,
        )
        log_event(
            event="dispatch_canceled",
            project=key,
            request_id=req_id,
            task=task,
            stage="close",
            status="failed",
        )
        return True

    return False


