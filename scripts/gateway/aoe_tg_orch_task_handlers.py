#!/usr/bin/env python3
"""Orchestrator task lifecycle handlers for Telegram gateway."""

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from aoe_tg_project_runtime import project_hidden_from_ops, project_runtime_issue


_SCENARIO_FILENAME = "AOE_TODO.md"
_DEFAULT_SCENARIO_TEMPLATE = """# AOE_TODO.md

Project scenario (per-project, runtime file).

This file is imported into the Mother-Orch todo queue via `/sync`.
Only task lines are parsed; everything else is ignored.

## Tasks

# Optional: keep your canonical todo in `<project_root>/TODO.md` and include it here.
@include ../TODO.md

# Supported formats (parsed by /sync):
# - [ ] summary        (open; default priority: P2)
# - summary            (open; default priority: P2)
# - 1. summary         (open; default priority: P2)
# - [ ] P1: summary
# - [x] P2: summary
# - P3: summary
#
# Notes:
# - You can include an explicit TODO id to update an existing item:
#   - [ ] TODO-123 P2: Adjust thresholds
# - Done lines ([x]) only mark done when the matching TODO already exists.

# - [ ] P2: (write your task here)

## Examples (ignored by /sync)

```text
- [ ] P1: First task
- [ ] P2: Second task
```
"""


def _project_alias(entry: Dict[str, Any], fallback: str) -> str:
    token = str(entry.get("project_alias", "")).strip().upper()
    return token or str(fallback or "").strip() or "-"


def _project_sort_key(key: str, entry: Dict[str, Any]) -> tuple[int, str, str]:
    alias = _project_alias(entry, key)
    token = alias[1:] if alias.startswith("O") else alias
    idx = int(token) if token.isdigit() else 10**9
    return idx, alias, str(key)


def _resolve_registered_project(manager_state: Dict[str, Any], target: Optional[str]) -> tuple[str, Dict[str, Any]]:
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    if not isinstance(projects, dict) or not projects:
        raise RuntimeError("no orch projects registered")

    raw = str(target or manager_state.get("active", "default")).strip() or "default"
    raw_key = raw.lower()
    raw_alias = raw.upper()

    direct = projects.get(raw_key)
    if isinstance(direct, dict):
        return raw_key, direct

    for key, entry in projects.items():
        if not isinstance(entry, dict):
            continue
        if raw_key == str(key).strip().lower():
            return str(key), entry
        alias = str(entry.get("project_alias", "")).strip().upper()
        if alias and alias == raw_alias:
            return str(key), entry
        display = str(entry.get("display_name", "") or entry.get("name", "")).strip().lower()
        if display and display == raw_key:
            return str(key), entry

    known = ", ".join(sorted(str(k) for k in projects.keys()))
    raise RuntimeError(f"unknown orch project: {raw} (known: {known})")


def _repair_registered_project(
    *,
    args: Any,
    entry: Dict[str, Any],
    key: str,
    resolve_project_root: Callable[[str], Any],
    run_aoe_init: Callable[..., str],
    now_iso: Callable[[], str],
) -> Dict[str, Any]:
    project_root = resolve_project_root(str(entry.get("project_root", "") or ""))
    team_dir = Path(str(entry.get("team_dir", "") or project_root / ".aoe-team")).expanduser().resolve()
    entry["project_root"] = str(project_root)
    entry["team_dir"] = str(team_dir)
    alias = _project_alias(entry, key)
    before_issue = project_runtime_issue(entry)
    overview = str(entry.get("overview", "")).strip() or f"{entry.get('display_name') or alias} project orchestration"

    logs: List[str] = []
    logs.append(
        ensure_scenario_file(
            template_root=Path(args.project_root).expanduser().resolve(),
            team_dir=team_dir,
            dry_run=bool(args.dry_run),
        )
    )
    logs.append(run_aoe_init(args, project_root=project_root, team_dir=team_dir, overview=overview))

    entry["updated_at"] = now_iso()
    after_issue = project_runtime_issue(entry)
    return {
        "key": key,
        "alias": alias,
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "before": before_issue or "ready",
        "after": after_issue or "ready",
        "logs": logs,
        "ready": not bool(after_issue),
    }


def _orch_status_reply_markup(manager_state: Dict[str, Any], key: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    alias = _project_alias(entry, key)
    raw_lock = manager_state.get("project_lock") if isinstance(manager_state, dict) else {}
    lock_key = ""
    if isinstance(raw_lock, dict) and bool(raw_lock.get("enabled", False)):
        lock_key = str(raw_lock.get("project_key", "")).strip().lower()

    focus_button = "/focus off" if lock_key == str(key).strip().lower() else f"/focus {alias}"
    hide_button = f"/orch {'unhide' if project_hidden_from_ops(entry) else 'hide'} {alias}"
    issue = project_runtime_issue(entry)
    if issue:
        keyboard: List[List[Dict[str, str]]] = [
            [{"text": f"/orch repair {alias}"}, {"text": f"/sync preview {alias} 1h"}],
            [{"text": f"/use {alias}"}, {"text": focus_button}, {"text": hide_button}],
            [{"text": "/map"}, {"text": "/help"}],
        ]
    else:
        keyboard = [
            [{"text": f"/todo {alias}"}, {"text": f"/todo {alias} followup"}, {"text": f"/orch monitor {alias}"}],
            [{"text": f"/sync preview {alias} 1h"}],
            [{"text": f"/sync {alias} 1h"}, {"text": f"/use {alias}"}, {"text": focus_button}],
            [{"text": hide_button}],
            [{"text": "/queue"}, {"text": "/next"}, {"text": "/map"}],
        ]
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": f"예: /todo {alias} 또는 /orch monitor {alias}",
    }


def ensure_scenario_file(*, template_root: Path, team_dir: Path, dry_run: bool) -> str:
    """Ensure `.aoe-team/AOE_TODO.md` exists for a project.

    The scheduler (`/sync`) imports this file into the Mother-Orch todo queue.
    """

    dst = (team_dir / _SCENARIO_FILENAME).resolve()
    if dst.exists():
        return f"[SKIP] scenario exists ({dst})"
    if dry_run:
        return f"[DRY-RUN] scenario create ({dst})"

    team_dir.mkdir(parents=True, exist_ok=True)

    template_path = (template_root / "templates" / "aoe-team" / _SCENARIO_FILENAME).resolve()
    text = _DEFAULT_SCENARIO_TEMPLATE
    try:
        if template_path.exists():
            text = template_path.read_text(encoding="utf-8")
    except Exception:
        # Keep the default template.
        pass

    dst.write_text(text, encoding="utf-8")
    return f"[OK] scenario created ({dst})"

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
    latest_task_request_refs: Callable[..., list[str]],
    set_chat_recent_task_refs: Callable[..., None],
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
                f"- scenario: {team_dir / _SCENARIO_FILENAME} (create_if_missing: yes)\n"
                f"- init: {'yes' if orch_add_init else 'no'}\n"
                f"- spawn: {'yes' if orch_add_spawn else 'no'}\n"
                f"- set_active: {'yes' if orch_add_set_active else 'no'}",
                context="orch-add dry-run",
            )
            return True

        project_root.mkdir(parents=True, exist_ok=True)
        scenario_log = ""
        try:
            scenario_log = ensure_scenario_file(
                template_root=Path(args.project_root).expanduser().resolve(),
                team_dir=team_dir,
                dry_run=bool(args.dry_run),
            )
        except Exception as exc:
            scenario_log = f"[WARN] scenario create failed ({team_dir / _SCENARIO_FILENAME}): {exc}"
        key, entry = register_orch_project(
            manager_state,
            name=orch_add_name,
            project_root=project_root,
            team_dir=team_dir,
            overview=overview,
            set_active=orch_add_set_active,
        )

        init_logs: List[str] = []
        if scenario_log:
            init_logs.append(scenario_log)
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

    if cmd == "orch-repair":
        target_token = str(orch_target or "").strip().lower()
        if target_token in {"all", "*", "global"}:
            projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
            rows: List[tuple[str, Dict[str, Any]]] = []
            if isinstance(projects, dict):
                for key, entry in projects.items():
                    if isinstance(entry, dict):
                        rows.append((str(key), entry))
            rows.sort(key=lambda item: _project_sort_key(item[0], item[1]))
            if not rows:
                send("no orch projects registered", context="orch-repair-all empty", with_menu=True)
                return True
            results: List[Dict[str, Any]] = []
            for key, entry in rows:
                try:
                    results.append(
                        _repair_registered_project(
                            args=args,
                            entry=entry,
                            key=key,
                            resolve_project_root=resolve_project_root,
                            run_aoe_init=run_aoe_init,
                            now_iso=now_iso,
                        )
                    )
                except Exception as exc:
                    results.append(
                        {
                            "key": key,
                            "alias": _project_alias(entry, key),
                            "project_root": str(entry.get("project_root", "") or ""),
                            "team_dir": str(entry.get("team_dir", "") or ""),
                            "before": project_runtime_issue(entry) or "ready",
                            "after": f"failed:{exc}",
                            "logs": [f"[ERROR] {exc}"],
                            "ready": False,
                        }
                    )
            if not args.dry_run:
                save_manager_state(args.manager_state_file, manager_state)
            ready_count = sum(1 for row in results if bool(row.get("ready")))
            lines = [
                "orch repair all finished",
                f"- projects: {len(results)}",
                f"- ready: {ready_count}",
                f"- failed: {len(results) - ready_count}",
                "results:",
            ]
            for row in results:
                lines.append(
                    f"- {row.get('alias')} {row.get('key')}: {row.get('before')} -> {row.get('after')}"
                )
            lines.extend(
                [
                    "next:",
                    "- /map",
                    "- /sync preview all 1h",
                ]
            )
            send("\n".join(lines), context="orch-repair-all", with_menu=True)
            return True

        key, entry = _resolve_registered_project(manager_state, orch_target)
        try:
            result = _repair_registered_project(
                args=args,
                entry=entry,
                key=key,
                resolve_project_root=resolve_project_root,
                run_aoe_init=run_aoe_init,
                now_iso=now_iso,
            )
        except Exception as exc:
            send(f"orch repair failed\n- orch: {key}\n- error: {exc}", context="orch-repair failed", with_menu=True)
            return True
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        lines = [
            "orch repair finished",
            f"- orch: {result.get('key')} ({result.get('alias')})",
            f"- root: {result.get('project_root')}",
            f"- team: {result.get('team_dir')}",
            f"- before: {result.get('before')}",
            f"- after: {result.get('after')}",
        ]
        logs = result.get("logs") or []
        if isinstance(logs, list) and logs:
            lines.append("logs:")
            for row in logs:
                short = row.strip().splitlines()
                lines.append(f"- {short[-1] if short else '(empty)'}")
        lines.extend(
            [
                "next:",
                f"- /orch status {result.get('alias')}",
                f"- /sync preview {result.get('alias')} 1h",
            ]
        )
        if bool(result.get("ready")):
            lines.append(f"- /todo {result.get('alias')}")
        send(
            "\n".join(lines),
            context="orch-repair",
            with_menu=True,
            reply_markup=_orch_status_reply_markup(manager_state, key, entry),
        )
        return True

    if cmd in {"status", "orch-status"}:
        try:
            key, entry, p_args = get_context(orch_target)
        except Exception as exc:
            text = str(exc)
            if "project lock active:" in text.lower():
                send(
                    "orch status blocked by project lock\n"
                    f"- {text}\n"
                    "next:\n"
                    "- /focus off\n"
                    "- /map",
                    context="orch-status blocked",
                    with_menu=True,
                )
                return True
            raise
        lock = manager_state.get("project_lock") if isinstance(manager_state, dict) else {}
        lock_key = str((lock or {}).get("project_key", "")).strip().lower() if isinstance(lock, dict) and bool(lock.get("enabled", False)) else ""
        lock_line = ""
        if lock_key:
            projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
            lock_entry = projects.get(lock_key) if isinstance(projects, dict) else {}
            lock_alias = str((lock_entry or {}).get("project_alias", "")).strip() or lock_key
            lock_line = f"project_lock: {lock_alias} ({lock_key})\n"
        active_tf_count = 0
        pending_tf = 0
        running_tf = 0
        tasks = entry.get("tasks")
        if isinstance(tasks, dict):
            for task in tasks.values():
                if not isinstance(task, dict):
                    continue
                status = str(task.get("status", "pending")).strip().lower() or "pending"
                if status == "running":
                    running_tf += 1
                elif status == "pending":
                    pending_tf += 1
                else:
                    continue
        active_tf_count = pending_tf + running_tf
        status = ""
        try:
            team_dir = Path(str(entry.get("team_dir", "") or "")).expanduser()
            cfg = (team_dir / "orchestrator.json").resolve() if str(team_dir) else None
            if cfg and not cfg.exists():
                status = (
                    "[WARN] orch config missing (orchestrator.json)\n"
                    f"missing: {cfg}\n"
                    f"fix: /orch repair {str(entry.get('project_alias', '')).strip() or key}"
                )
            else:
                status = run_aoe_status(p_args)
        except Exception as exc:
            status = f"[WARN] status unavailable: {exc}"
        send(
            f"orch: {key}\nroot: {entry.get('project_root')}\nteam: {entry.get('team_dir')}\n{lock_line}last_request: {entry.get('last_request_id') or '-'}\n"
            f"active_tf_count: {active_tf_count} (pending={pending_tf} running={running_tf})\n\n{status}",
            context="status",
            with_menu=False,
            reply_markup=_orch_status_reply_markup(manager_state, key, entry),
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
            limit = 9
            recent_refs = latest_task_request_refs(entry, limit=limit)
            set_chat_recent_task_refs(manager_state, chat_id, key, recent_refs)
            current_sel = get_chat_selected_task_ref(manager_state, chat_id, key)
            if (not current_sel) and recent_refs:
                set_chat_selected_task_ref(manager_state, chat_id, key, recent_refs[0])
            if not args.dry_run:
                save_manager_state(args.manager_state_file, manager_state)

            if not recent_refs:
                send(
                    f"orch: {key}\n"
                    "최근 작업이 없습니다.\n\n"
                    "start: /dispatch <요청>",
                    context="orch-pick empty",
                    with_menu=True,
                )
                return True

            lines = [
                f"orch: {key}",
                "pick: 최근 작업 선택",
                "",
                "recent:",
            ]
            for idx, rid in enumerate(recent_refs, start=1):
                task = get_task_record(entry, rid) or {}
                label = task_display_label(task, fallback_request_id=rid)
                lines.append(f"- {idx}. {label}")
            lines.append("")
            lines.append("tap: /pick 1..9  (또는 /pick <T-xxx|alias>)")

            keyboard = []
            row = []
            for idx in range(1, len(recent_refs) + 1):
                row.append({"text": f"/pick {idx}"})
                if len(row) >= 3:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            keyboard.append([{"text": "/task"}, {"text": "/check"}, {"text": "/monitor 9"}])
            keyboard.append([{"text": "/status"}, {"text": "/map"}, {"text": "/help"}])
            reply_markup = {
                "keyboard": keyboard,
                "resize_keyboard": True,
                "one_time_keyboard": True,
                "input_field_placeholder": "예: /pick 3 또는 /pick T-005",
            }

            send(
                "\n".join(lines),
                context="orch-pick menu",
                with_menu=False,
                reply_markup=reply_markup,
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
