#!/usr/bin/env python3
"""Management command handlers for Telegram gateway."""

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import aoe_tg_management_acl as mgmt_acl_mod
import aoe_tg_management_chat as mgmt_chat_mod
from aoe_tg_ops_view import (
    blocked_bucket_count as ops_view_blocked_bucket_count,
    blocked_head_summary as ops_view_blocked_head_summary,
    compact_age_label as ops_view_compact_age_label,
)
import aoe_tg_offdesk_flow as offdesk_flow_mod
import aoe_tg_scheduler_control_handlers as scheduler_control_mod
from aoe_tg_project_state import (
    get_manager_project,
    get_project_lock_row as get_project_lock_row_state,
    project_alias_for_key,
    project_lock_label as project_lock_label_state,
)

AUTO_STATE_FILENAME = "auto_scheduler.json"
OFFDESK_STATE_FILENAME = "offdesk_state.json"
PROVIDER_CAPACITY_STATE_FILENAME = "provider_capacity.json"
DEFAULT_AUTO_INTERVAL_SEC = 2
DEFAULT_AUTO_IDLE_SEC = 20
DEFAULT_AUTO_MAX_FAILURES = 3
DEFAULT_OFFDESK_COMMAND = "fanout"
DEFAULT_OFFDESK_PREFETCH = "sync_recent"
DEFAULT_OFFDESK_PREFETCH_SINCE = (os.environ.get("AOE_OFFDESK_PREFETCH_SINCE") or "12h").strip() or "12h"
DEFAULT_OFFDESK_REPORT_LEVEL = "short"
DEFAULT_OFFDESK_ROOM = "global"
_SCENARIO_INCLUDE_PREFIX = "@include"


def _cmd_prefix() -> str:
    return offdesk_flow_mod.cmd_prefix()


def _normalize_prefetch_token(raw: Any) -> str:
    return offdesk_flow_mod.normalize_prefetch_token(raw)


def _parse_replace_sync_flag(tokens: List[str]) -> Optional[bool]:
    return offdesk_flow_mod.parse_replace_sync_flag(tokens)


def _prefetch_display(prefetch: Any, prefetch_since: Any, replace_sync: bool) -> str:
    return offdesk_flow_mod.prefetch_display(prefetch, prefetch_since, replace_sync)


def _compact_age_label(raw_ts: str) -> str:
    return ops_view_compact_age_label(raw_ts)


def _compact_reason(raw: Any, limit: int = 120) -> str:
    return offdesk_flow_mod.compact_reason(raw, limit=limit)


def _status_report_level(tokens: List[str], fallback: str) -> str:
    return offdesk_flow_mod.status_report_level(tokens, fallback)


def _focused_project_entry(manager_state: Dict[str, Any]) -> Tuple[str, Dict[str, Any], bool]:
    return offdesk_flow_mod.focused_project_entry(manager_state, project_lock_row=_project_lock_row)


def _blocked_reason_preview(raw: Any, limit: int = 72) -> str:
    text = " ".join(str(raw or "").strip().split())
    if len(text) > limit:
        return text[: max(0, limit - 3)].rstrip() + "..."
    return text


def _blocked_bucket_label(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    if token == "manual_followup":
        return "manual_followup"
    return ""


def _blocked_head_summary(todos: Any) -> Dict[str, Any]:
    return ops_view_blocked_head_summary(todos)


def _blocked_bucket_count(todos: Any, bucket: str) -> int:
    return ops_view_blocked_bucket_count(todos, bucket)


def _focused_project_snapshot_lines(manager_state: Dict[str, Any]) -> List[str]:
    return offdesk_flow_mod.focused_project_snapshot_lines(
        manager_state,
        project_lock_row=_project_lock_row,
    )


def _ops_scope_summary(manager_state: Dict[str, Any]) -> Dict[str, List[str]]:
    return offdesk_flow_mod.ops_scope_summary(manager_state)


def _ops_scope_compact_lines(manager_state: Dict[str, Any], *, limit: int = 4, detail_level: str = "short") -> List[str]:
    return offdesk_flow_mod.ops_scope_compact_lines(manager_state, limit=limit, detail_level=detail_level)


def _canonical_todo_path(entry: Dict[str, Any]) -> Path:
    return offdesk_flow_mod.canonical_todo_path(entry)


def _scenario_path(entry: Dict[str, Any]) -> Path:
    return offdesk_flow_mod.scenario_path(entry)


def _scenario_include_targets(entry: Dict[str, Any]) -> List[Tuple[str, bool]]:
    return offdesk_flow_mod.scenario_include_targets(entry, include_prefix=_SCENARIO_INCLUDE_PREFIX)


def _parse_iso_datetime(raw: str) -> Optional[datetime]:
    return offdesk_flow_mod.parse_iso_datetime(raw)


def _alias_index(alias: str) -> int:
    return offdesk_flow_mod.alias_index(alias)


def _offdesk_prepare_targets(manager_state: Dict[str, Any], raw_target: str) -> List[Tuple[str, Dict[str, Any]]]:
    return offdesk_flow_mod.offdesk_prepare_targets(
        manager_state,
        raw_target,
        project_lock_row=_project_lock_row,
        resolve_project_entry=_resolve_project_entry,
    )


def _offdesk_prepare_project_report(manager_state: Dict[str, Any], key: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    return offdesk_flow_mod.offdesk_prepare_project_report(manager_state, key, entry)


def _sort_offdesk_reports(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return offdesk_flow_mod.sort_offdesk_reports(reports)


def _offdesk_review_reply_markup(
    flagged: List[Dict[str, Any]],
    *,
    clean: bool = False,
    capacity_operator_action: str = "",
) -> Dict[str, Any]:
    return offdesk_flow_mod.offdesk_review_reply_markup(
        flagged,
        clean=clean,
        capacity_operator_action=capacity_operator_action,
    )


def _offdesk_prepare_reply_markup(
    reports: List[Dict[str, Any]],
    *,
    blocked_count: int = 0,
    clean: bool = False,
) -> Dict[str, Any]:
    return offdesk_flow_mod.offdesk_prepare_reply_markup(reports, blocked_count=blocked_count, clean=clean)


def _clear_usage() -> str:
    p = _cmd_prefix()
    return (
        "clear\n"
        f"- {p}clear pending              # clear one-shot pending + confirm\n"
        f"- {p}clear routing              # clear default_mode + pending + confirm\n"
        f"- {p}clear room [name]           # wipe room logs (ephemeral board)\n"
        f"- {p}clear queue [O#|name] [sync|open|all]\n"
        "  - sync: remove OPEN todos created by /sync (default)\n"
        "  - open: remove all OPEN todos\n"
        "  - all : remove todos except DONE/CANCELED (keeps history)\n"
    )


def _resolve_project_entry(manager_state: Dict[str, Any], raw_target: str) -> Tuple[str, Dict[str, Any]]:
    return get_manager_project(manager_state, raw_target, bool_from_json=_bool_from_json)


def _project_lock_row(manager_state: Dict[str, Any]) -> Dict[str, Any]:
    return get_project_lock_row_state(manager_state, bool_from_json=_bool_from_json)


def _project_lock_label(manager_state: Dict[str, Any]) -> str:
    return project_lock_label_state(manager_state, bool_from_json=_bool_from_json)


def _project_lock_conflict_text(manager_state: Dict[str, Any], requested_key: str) -> str:
    row = _project_lock_row(manager_state)
    lock_key = str(row.get("project_key", "")).strip()
    if not lock_key or requested_key == lock_key:
        return ""
    locked_alias = project_alias_for_key(manager_state, lock_key) or lock_key
    req_alias = project_alias_for_key(manager_state, requested_key) or requested_key
    return (
        "project lock active\n"
        f"- locked: {locked_alias} ({lock_key})\n"
        f"- requested: {req_alias} ({requested_key})\n"
        "next:\n"
        f"- /focus {req_alias}\n"
        "- /focus off"
    )


def _tutorial_text(*, lang: str) -> str:
    p = _cmd_prefix()
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


def _now_iso() -> str:
    return offdesk_flow_mod.now_iso()


def _bool_from_json(raw: Any, default: bool) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    if isinstance(raw, (int, float)):
        return bool(raw)
    token = str(raw).strip().lower()
    if token in {"1", "true", "yes", "on"}:
        return True
    if token in {"0", "false", "no", "off"}:
        return False
    return default


def _auto_state_path(args: Any) -> Path:
    return offdesk_flow_mod.auto_state_path(args, filename=AUTO_STATE_FILENAME)


def _offdesk_state_path(args: Any) -> Path:
    return offdesk_flow_mod.offdesk_state_path(args, filename=OFFDESK_STATE_FILENAME)


def _provider_capacity_state_path(args: Any) -> Path:
    return offdesk_flow_mod.provider_capacity_state_path(args, filename=PROVIDER_CAPACITY_STATE_FILENAME)


def _load_auto_state(path: Path) -> Dict[str, Any]:
    return offdesk_flow_mod.load_auto_state(path)


def _save_auto_state(path: Path, state: Dict[str, Any]) -> None:
    return offdesk_flow_mod.save_auto_state(path, state)


def _load_offdesk_state(path: Path) -> Dict[str, Any]:
    return offdesk_flow_mod.load_offdesk_state(path)


def _save_offdesk_state(path: Path, state: Dict[str, Any]) -> None:
    return offdesk_flow_mod.save_offdesk_state(path, state)


def _load_provider_capacity_state(path: Path) -> Dict[str, Any]:
    return offdesk_flow_mod.load_provider_capacity_state(path)


def _save_provider_capacity_state(path: Path, state: Dict[str, Any]) -> None:
    return offdesk_flow_mod.save_provider_capacity_state(path, state)


def _scheduler_session_name() -> str:
    return offdesk_flow_mod.scheduler_session_name()


def _tmux_has_session(session_name: str) -> bool:
    return offdesk_flow_mod.tmux_has_session(session_name)


def _tmux_auto_command(args: Any, action: str) -> Tuple[bool, str]:
    return offdesk_flow_mod.tmux_auto_command(args, action)


def handle_management_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    current_chat_alias: str,
    mode_setting: Optional[str],
    lang_setting: Optional[str],
    report_setting: Optional[str],
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
    if cmd == "clear":
        tokens = [t for t in str(rest or "").split() if t.strip()]
        sub = (tokens[0].lower() if tokens else "status").strip()
        sub_args = tokens[1:]

        if sub in {"help", "h", "?", "status", "show"}:
            current_default_mode = get_default_mode(manager_state, chat_id) or "off"
            current_pending_mode = get_pending_mode(manager_state, chat_id) or "none"
            room = get_chat_room(manager_state, chat_id, DEFAULT_OFFDESK_ROOM) or DEFAULT_OFFDESK_ROOM
            chat_sessions = manager_state.get("chat_sessions", {})
            chat_state = chat_sessions.get(str(chat_id), {}) if isinstance(chat_sessions, dict) else {}
            confirm_present = "yes" if (isinstance(chat_state, dict) and bool(chat_state.get("confirm_action"))) else "no"
            send(
                "clear (status)\n"
                f"- default_mode: {current_default_mode}\n"
                f"- pending_mode: {current_pending_mode}\n"
                f"- confirm_pending: {confirm_present}\n"
                f"- room: {room}\n"
                "\n"
                + _clear_usage(),
                context="clear-status",
                with_menu=True,
            )
            return True

        if chat_role == "readonly":
            p = _cmd_prefix()
            send(
                f"permission denied: readonly chat cannot use {p}clear.\n" + _clear_usage(),
                context="clear-deny",
                with_menu=True,
            )
            return True

        if sub in {"pending", "cancel"}:
            cleared_pending = clear_pending_mode(manager_state, chat_id)
            cleared_confirm = clear_confirm_action(manager_state, chat_id)
            if (cleared_pending or cleared_confirm) and (not args.dry_run):
                save_manager_state(args.manager_state_file, manager_state)
            send(
                "cleared\n"
                "- scope: pending\n"
                f"- pending_cleared: {'yes' if cleared_pending else 'no'}\n"
                f"- confirm_cleared: {'yes' if cleared_confirm else 'no'}",
                context="clear-pending",
                with_menu=True,
            )
            return True

        if sub in {"routing", "mode"}:
            existed_default = clear_default_mode(manager_state, chat_id)
            cleared_pending = clear_pending_mode(manager_state, chat_id)
            cleared_confirm = clear_confirm_action(manager_state, chat_id)
            if (existed_default or cleared_pending or cleared_confirm) and (not args.dry_run):
                save_manager_state(args.manager_state_file, manager_state)
            send(
                "cleared\n"
                "- scope: routing\n"
                f"- default_mode_off: {'yes' if existed_default else 'no'}\n"
                f"- pending_cleared: {'yes' if cleared_pending else 'no'}\n"
                f"- confirm_cleared: {'yes' if cleared_confirm else 'no'}",
                context="clear-routing",
                with_menu=True,
            )
            return True

        if sub == "room":
            from aoe_tg_room_handlers import normalize_room_token  # local import to keep deps light

            room_raw = str(sub_args[0] if sub_args else (get_chat_room(manager_state, chat_id, DEFAULT_OFFDESK_ROOM) or DEFAULT_OFFDESK_ROOM)).strip()
            room_token = normalize_room_token(room_raw)
            team_dir = Path(str(getattr(args, "team_dir", ""))).expanduser().resolve()
            rooms_root = (team_dir / "logs" / "rooms").resolve()
            room_dir = (rooms_root.joinpath(*room_token.split("/"))).resolve()
            try:
                room_dir.relative_to(rooms_root)
            except Exception:
                send(f"refusing to clear unsafe room path: {room_token}", context="clear-room unsafe", with_menu=True)
                return True

            removed_files = 0
            if room_dir.exists() and room_dir.is_dir():
                try:
                    removed_files = len([p for p in room_dir.rglob("*.jsonl") if p.is_file()])
                except Exception:
                    removed_files = 0
                if not args.dry_run:
                    shutil.rmtree(room_dir, ignore_errors=True)

            send(
                "cleared\n"
                "- scope: room\n"
                f"- room: {room_token}\n"
                f"- removed_jsonl: {removed_files}",
                context="clear-room",
                with_menu=True,
            )
            return True

        if sub in {"queue", "todo", "todos"}:
            mode = "sync"
            target = ""
            for tok in sub_args:
                low = tok.strip().lower()
                up = tok.strip().upper()
                if up.startswith("O") and up[1:].isdigit():
                    target = up
                    continue
                if low in {"sync", "open", "all"}:
                    mode = low
                    continue
                if not target:
                    target = tok.strip()

            try:
                key, entry = _resolve_project_entry(manager_state, target)
            except Exception as e:
                send(str(e) + "\n\n" + _clear_usage(), context="clear-queue missing", with_menu=True)
                return True

            raw = entry.get("todos")
            todos = [r for r in raw if isinstance(r, dict)] if isinstance(raw, list) else []
            keep = []
            removed = 0
            removed_ids = set()
            for row in todos:
                st = str(row.get("status", "open")).strip().lower() or "open"
                created_by = str(row.get("created_by", "")).strip().lower()
                is_done = st in {"done", "canceled"}
                is_open = st == "open"
                is_sync = created_by.startswith("sync:")
                drop = False
                if mode == "sync":
                    drop = is_open and is_sync
                elif mode == "open":
                    drop = is_open
                elif mode == "all":
                    drop = not is_done
                if drop:
                    removed += 1
                    rid = str(row.get("id", "")).strip()
                    if rid:
                        removed_ids.add(rid)
                    continue
                keep.append(row)

            pending = entry.get("pending_todo")
            if isinstance(pending, dict):
                pt = str(pending.get("todo_id", "")).strip()
                if pt and pt in removed_ids:
                    entry.pop("pending_todo", None)

            entry["todos"] = keep
            if removed:
                entry["updated_at"] = _now_iso()
                if not args.dry_run:
                    save_manager_state(args.manager_state_file, manager_state)
            send(
                "cleared\n"
                "- scope: queue\n"
                f"- orch: {key}\n"
                f"- mode: {mode}\n"
                f"- removed: {removed}\n"
                f"- remaining: {len(keep)}",
                context="clear-queue",
                with_menu=True,
            )
            return True

        send("usage:\n" + _clear_usage(), context="clear-usage", with_menu=True)
        return True

    if cmd == "tutorial":
        return mgmt_chat_mod.handle_chat_management_command(
            cmd=cmd,
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            chat_role=chat_role,
            mode_setting=mode_setting,
            lang_setting=lang_setting,
            report_setting=report_setting,
            send=send,
            get_default_mode=get_default_mode,
            get_pending_mode=get_pending_mode,
            get_chat_lang=get_chat_lang,
            get_chat_report_level=get_chat_report_level,
            set_default_mode=set_default_mode,
            set_pending_mode=set_pending_mode,
            set_chat_lang=set_chat_lang,
            set_chat_report_level=set_chat_report_level,
            clear_default_mode=clear_default_mode,
            clear_pending_mode=clear_pending_mode,
            clear_confirm_action=clear_confirm_action,
            clear_chat_report_level=clear_chat_report_level,
            save_manager_state=save_manager_state,
            cmd_prefix=_cmd_prefix,
        )

    if cmd in {"focus", "panic", "offdesk", "auto"}:
        return scheduler_control_mod.handle_scheduler_control_command(
            cmd=cmd,
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            chat_role=chat_role,
            rest=rest,
            send=send,
            get_default_mode=get_default_mode,
            get_pending_mode=get_pending_mode,
            get_chat_report_level=get_chat_report_level,
            get_chat_room=get_chat_room,
            set_default_mode=set_default_mode,
            set_chat_report_level=set_chat_report_level,
            set_chat_room=set_chat_room,
            clear_default_mode=clear_default_mode,
            clear_pending_mode=clear_pending_mode,
            clear_confirm_action=clear_confirm_action,
            clear_chat_report_level=clear_chat_report_level,
            save_manager_state=save_manager_state,
            resolve_project_entry=_resolve_project_entry,
            project_lock_row=_project_lock_row,
            project_lock_label=_project_lock_label,
            parse_replace_sync_flag=_parse_replace_sync_flag,
            normalize_prefetch_token=_normalize_prefetch_token,
            prefetch_display=_prefetch_display,
            compact_reason=_compact_reason,
            status_report_level=_status_report_level,
            focused_project_snapshot_lines=_focused_project_snapshot_lines,
            ops_scope_summary=_ops_scope_summary,
            ops_scope_compact_lines=lambda state, limit, detail_level: _ops_scope_compact_lines(
                state, limit=limit, detail_level=detail_level
            ),
            offdesk_prepare_targets=_offdesk_prepare_targets,
            offdesk_prepare_project_report=_offdesk_prepare_project_report,
            sort_offdesk_reports=_sort_offdesk_reports,
            offdesk_review_reply_markup=lambda flagged, clean=False, capacity_operator_action="": _offdesk_review_reply_markup(
                flagged,
                clean=clean,
                capacity_operator_action=capacity_operator_action,
            ),
            offdesk_prepare_reply_markup=lambda reports, blocked_count=0, clean=False: _offdesk_prepare_reply_markup(
                reports, blocked_count=blocked_count, clean=clean
            ),
            auto_state_path=_auto_state_path,
            offdesk_state_path=_offdesk_state_path,
            provider_capacity_state_path=_provider_capacity_state_path,
            load_auto_state=_load_auto_state,
            save_auto_state=_save_auto_state,
            load_offdesk_state=_load_offdesk_state,
            save_offdesk_state=_save_offdesk_state,
            load_provider_capacity_state=_load_provider_capacity_state,
            save_provider_capacity_state=_save_provider_capacity_state,
            scheduler_session_name=_scheduler_session_name,
            tmux_has_session=_tmux_has_session,
            tmux_auto_command=_tmux_auto_command,
            now_iso=_now_iso,
            default_auto_interval_sec=DEFAULT_AUTO_INTERVAL_SEC,
            default_auto_idle_sec=DEFAULT_AUTO_IDLE_SEC,
            default_auto_max_failures=DEFAULT_AUTO_MAX_FAILURES,
            default_offdesk_command=DEFAULT_OFFDESK_COMMAND,
            default_offdesk_prefetch=DEFAULT_OFFDESK_PREFETCH,
            default_offdesk_prefetch_since=DEFAULT_OFFDESK_PREFETCH_SINCE,
            default_offdesk_report_level=DEFAULT_OFFDESK_REPORT_LEVEL,
            default_offdesk_room=DEFAULT_OFFDESK_ROOM,
        )

    if cmd in {"mode", "lang", "report", "quick-dispatch", "quick-direct", "cancel-pending"}:
        return mgmt_chat_mod.handle_chat_management_command(
            cmd=cmd,
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            chat_role=chat_role,
            mode_setting=mode_setting,
            lang_setting=lang_setting,
            report_setting=report_setting,
            send=send,
            get_default_mode=get_default_mode,
            get_pending_mode=get_pending_mode,
            get_chat_lang=get_chat_lang,
            get_chat_report_level=get_chat_report_level,
            set_default_mode=set_default_mode,
            set_pending_mode=set_pending_mode,
            set_chat_lang=set_chat_lang,
            set_chat_report_level=set_chat_report_level,
            clear_default_mode=clear_default_mode,
            clear_pending_mode=clear_pending_mode,
            clear_confirm_action=clear_confirm_action,
            clear_chat_report_level=clear_chat_report_level,
            save_manager_state=save_manager_state,
            cmd_prefix=_cmd_prefix,
        )

    if cmd in {"whoami", "acl", "grant", "revoke", "lockme", "onlyme"}:
        return mgmt_acl_mod.handle_acl_management_command(
            cmd=cmd,
            args=args,
            manager_state=manager_state,
            chat_id=chat_id,
            current_chat_alias=current_chat_alias,
            rest=rest,
            came_from_slash=came_from_slash,
            acl_grant_scope=acl_grant_scope,
            acl_grant_chat_id=acl_grant_chat_id,
            acl_revoke_scope=acl_revoke_scope,
            acl_revoke_chat_id=acl_revoke_chat_id,
            send=send,
            log_event=log_event,
            get_default_mode=get_default_mode,
            get_pending_mode=get_pending_mode,
            get_chat_lang=get_chat_lang,
            get_chat_report_level=get_chat_report_level,
            resolve_chat_role=resolve_chat_role,
            is_owner_chat=is_owner_chat,
            ensure_chat_aliases=ensure_chat_aliases,
            find_chat_alias=find_chat_alias,
            alias_table_summary=alias_table_summary,
            resolve_chat_ref=resolve_chat_ref,
            ensure_chat_alias=ensure_chat_alias,
            sync_acl_env_file=sync_acl_env_file,
            project_lock_label=_project_lock_label,
        )

    if cmd in {"start", "help", "orch-help"}:
        send(help_text(), context="help", with_menu=True)
        return True

    return False
