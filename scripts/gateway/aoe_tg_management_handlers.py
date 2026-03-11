#!/usr/bin/env python3
"""Management command handlers for Telegram gateway."""

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from aoe_tg_acl import (
    format_csv_set,
    parse_acl_command_args,
    parse_acl_revoke_args,
    resolve_role_from_acl_sets,
)
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


def _offdesk_review_reply_markup(flagged: List[Dict[str, Any]], *, clean: bool = False) -> Dict[str, Any]:
    return offdesk_flow_mod.offdesk_review_reply_markup(flagged, clean=clean)


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


def _load_auto_state(path: Path) -> Dict[str, Any]:
    return offdesk_flow_mod.load_auto_state(path)


def _save_auto_state(path: Path, state: Dict[str, Any]) -> None:
    return offdesk_flow_mod.save_auto_state(path, state)


def _load_offdesk_state(path: Path) -> Dict[str, Any]:
    return offdesk_flow_mod.load_offdesk_state(path)


def _save_offdesk_state(path: Path, state: Dict[str, Any]) -> None:
    return offdesk_flow_mod.save_offdesk_state(path, state)


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
        ui_lang = get_chat_lang(manager_state, chat_id, "ko")
        send(_tutorial_text(lang=ui_lang), context="tutorial", with_menu=True)
        return True

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
            offdesk_review_reply_markup=lambda flagged, clean=False: _offdesk_review_reply_markup(flagged, clean=clean),
            offdesk_prepare_reply_markup=lambda reports, blocked_count=0, clean=False: _offdesk_prepare_reply_markup(
                reports, blocked_count=blocked_count, clean=clean
            ),
            auto_state_path=_auto_state_path,
            offdesk_state_path=_offdesk_state_path,
            load_auto_state=_load_auto_state,
            save_auto_state=_save_auto_state,
            load_offdesk_state=_load_offdesk_state,
            save_offdesk_state=_save_offdesk_state,
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
            f"- project_lock: {_project_lock_label(manager_state) or 'off'}\n"
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

    if cmd in {"start", "help", "orch-help"}:
        send(help_text(), context="help", with_menu=True)
        return True

    return False
