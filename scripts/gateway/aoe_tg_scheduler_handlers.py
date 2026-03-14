#!/usr/bin/env python3
"""Scheduler command handlers for Telegram gateway.

This module implements a minimal "Mother-Orch" scheduler:
- /next : pick the next runnable todo across all registered orch projects
- /queue : summarize todo queue across all registered orch projects

Design goals:
- Keep operator input minimal (single command).
- Prefer safety: do not start a new todo when a project already has a running todo,
  or when a pending todo exists (unless forced).
- Keep blocked todos visible, but do not let them freeze unrelated open todos in the same project.
- Return a transition dict compatible with the NonRun->Run pipeline.
"""

from __future__ import annotations

import copy
import fnmatch
import json
from pathlib import Path
import os
import re
import heapq
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from aoe_tg_blocked_state import (
    blocked_bucket_count as blocked_bucket_count_base,
    blocked_bucket_label as blocked_bucket_label_base,
    blocked_head_summary as blocked_head_summary_base,
    blocked_reason_preview as blocked_reason_preview_base,
    manual_followup_indices as manual_followup_indices_base,
)
from aoe_tg_ops_policy import (
    build_no_runnable_todo_message,
    find_pending_todo_for_chat as find_ops_pending_todo_for_chat,
    list_ops_projects,
    priority_rank as ops_priority_rank,
    project_alias as ops_project_alias,
    project_queue_snapshot,
    project_schedulable,
    sorted_open_todos as ops_sorted_open_todos,
)
from aoe_tg_project_runtime import project_hidden_from_ops, project_runtime_issue, project_runtime_label
from aoe_tg_queue_engine import (
    count_todo_statuses as queue_count_todo_statuses,
    find_todo_item as queue_find_todo_item,
    has_task_linked_to_todo as queue_has_task_linked_to_todo,
    pick_global_next_candidate as queue_pick_global_next_candidate,
    sorted_active_todos as queue_sorted_active_todos,
)
from aoe_tg_sync_merge import apply_scenario_items_to_entry as _apply_scenario_items_to_entry
from aoe_tg_sync_merge import stamp_sync_meta as _stamp_sync_meta
from aoe_tg_todo_state import merge_todo_proposals

_PRIORITIES = {"P1", "P2", "P3"}
_STATUS_OPEN = "open"
_STATUS_RUNNING = "running"
_STATUS_BLOCKED = "blocked"
_STATUS_DONE = "done"
_STATUS_CANCELED = "canceled"
_SCENARIO_FILENAME = "AOE_TODO.md"
_SCENARIO_INCLUDE_PREFIX = "@include"

_DISCOVERY_DEFAULT_DOCS_LIMIT = 3
_DISCOVERY_DEFAULT_CANDIDATE_KEEP = 250
_DISCOVERY_DEFAULT_MAX_BYTES = 512 * 1024
_DISCOVERY_ALLOWED_EXTS = {".md", ".txt", ".rst"}
_DISCOVERY_DEFAULT_TODO_FILES_LIMIT = 80
_DISCOVERY_EXCLUDE_DIRS = {
    ".github",
    ".git",
    ".aoe-team",
    ".venv",
    "archive",
    "venv",
    "templates",
    "__pycache__",
    "node_modules",
    "dist",
    "build",
    "target",
    "out",
    ".pytest_cache",
}

_LAST_CMD_ARGS_KEY = "last_cmd_args"

from aoe_tg_sync_sources import (
    _apply_sync_policy,
    _attach_item_provenance,
    _choose_sync_row,
    _classify_sync_source,
    _discover_recent_doc_todos,
    _discover_salvage_doc_proposals,
    _discover_salvage_doc_todos,
    _discover_sync_fallback_todos,
    _discover_todo_file_todos,
    _doc_has_any_markdown_checkbox,
    _doc_has_todo_markers,
    _extract_explicit_todo_id,
    _extract_salvage_proposal_items_from_doc,
    _extract_todo_items_from_doc,
    _has_following_top_level_actionable_child,
    _heading_is_done,
    _heading_is_meta,
    _is_excluded_doc_path,
    _is_within,
    _line_is_plain_done_label,
    _line_is_plain_meta_label,
    _line_is_plain_todo_label,
    _load_project_sync_policy,
    _normalize_summary_key,
    _parse_doc_section_bullet,
    _parse_doc_todo_line,
    _parse_scenario_lines,
    _path_has_todo_hint,
    _proposal_from_salvage_row,
    _recent_doc_candidates,
    _rel_display,
    _salvage_heading,
    _scenario_include_tokens,
    _strip_summary_marker,
    _summary_has_completion_marker,
    _summary_has_meta_prefix,
    _summary_is_done_label,
    _summary_is_meta_label,
    _summary_is_non_actionable,
    _summary_is_reference_note,
    _summary_is_structural_title,
    _summary_label_key,
    _sync_candidate_allowed,
    _tag_sync_items,
    _todo_file_candidates,
    _todo_heading,
)


def _project_lock_row(manager_state: Dict[str, Any]) -> Dict[str, Any]:
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    raw = manager_state.get("project_lock") if isinstance(manager_state, dict) else {}
    if not isinstance(raw, dict) or not isinstance(projects, dict):
        return {}
    if not bool(raw.get("enabled", False)):
        return {}
    key = str(raw.get("project_key", "")).strip().lower()
    entry = projects.get(key)
    if not key or not isinstance(entry, dict):
        return {}
    return {"enabled": True, "project_key": key}




def _is_auto_invocation(args: Any) -> bool:
    return str(getattr(args, "_aoe_invocation", "") or "").strip().lower() == "auto"


def _get_last_cmd_args(state: Dict[str, Any], chat_id: str, cmd: str) -> str:
    sessions = state.get("chat_sessions")
    if not isinstance(sessions, dict):
        return ""
    row = sessions.get(str(chat_id).strip())
    if not isinstance(row, dict):
        return ""
    last = row.get(_LAST_CMD_ARGS_KEY)
    if not isinstance(last, dict):
        return ""
    return str(last.get(str(cmd or "").strip().lower(), "")).strip()


def _set_last_cmd_args(state: Dict[str, Any], chat_id: str, cmd: str, rest: str, now: str) -> bool:
    cid = str(chat_id or "").strip()
    token = str(cmd or "").strip().lower()
    val = str(rest or "").strip()
    if not cid or not token or not val:
        return False
    sessions = state.get("chat_sessions")
    if not isinstance(sessions, dict):
        sessions = {}
        state["chat_sessions"] = sessions
    row = sessions.get(cid)
    if not isinstance(row, dict):
        row = {}
        sessions[cid] = row
    last = row.get(_LAST_CMD_ARGS_KEY)
    if not isinstance(last, dict):
        last = {}
        row[_LAST_CMD_ARGS_KEY] = last
    last[token] = val[:800]
    row["updated_at"] = str(now or "").strip() or row.get("updated_at", "")
    return True


def _parse_since_seconds(raw: str) -> int:
    """Parse a short duration token like 90m, 3h, 2d into seconds."""

    token = str(raw or "").strip().lower()
    if not token:
        return 0
    m = re.fullmatch(r"(?P<num>\d+(?:\.\d+)?)(?P<unit>s|m|h|d)", token)
    if not m:
        return 0
    try:
        value = float(m.group("num"))
    except Exception:
        return 0
    unit = m.group("unit")
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit, 0)
    seconds = int(value * mult)
    return max(0, seconds)
























































































def _preview_item_line(row: Dict[str, Any]) -> str:
    status = str(row.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
    pr = _normalize_priority(str(row.get("priority", "P2")))
    summary = str(row.get("summary", "")).strip().replace("\n", " ")
    if len(summary) > 96:
        summary = summary[:93] + "..."
    source_class = str(row.get("sync_source_class", "")).strip()
    doc_type = str(row.get("sync_doc_type", "")).strip()
    try:
        confidence = float(row.get("sync_confidence", 0.0) or 0.0)
    except Exception:
        confidence = 0.0
    meta_bits: List[str] = []
    if source_class:
        meta_bits.append(source_class)
    if doc_type:
        meta_bits.append(f"dtype={doc_type}")
    if source_class:
        meta_bits.append(f"{confidence:.2f}")
    meta = f" {{{' '.join(meta_bits)}}}" if meta_bits else ""
    src_file = str(row.get("source_file", "")).strip()
    src_section = str(row.get("source_section", "")).strip()
    src_reason = str(row.get("source_reason", "")).strip()
    try:
        src_line = int(row.get("source_line", 0) or 0)
    except Exception:
        src_line = 0
    prov_parts: List[str] = []
    if src_file:
        prov_parts.append(f"@{src_file}")
    if src_section:
        prov_parts.append(f"#{src_section}")
    if src_line > 0:
        prov_parts.append(f"L{src_line}")
    if src_reason:
        prov_parts.append(f"via {src_reason}")
    prov = f" [{' '.join(prov_parts)}]" if prov_parts else ""
    return f"[{status}] {pr}{meta} {summary or '-'}{prov}"


def _summarize_sync_candidate_classes(items: List[Dict[str, Any]]) -> str:
    counts: Dict[str, int] = {}
    for row in items:
        if not isinstance(row, dict):
            continue
        key = str(row.get("sync_source_class", "")).strip() or "unknown"
        counts[key] = counts.get(key, 0) + 1
    if not counts:
        return "-"
    ordered = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return ", ".join(f"{name}={count}" for name, count in ordered[:6])


def _summarize_sync_candidate_doc_types(items: List[Dict[str, Any]]) -> str:
    counts: Dict[str, int] = {}
    for row in items:
        if not isinstance(row, dict):
            continue
        key = str(row.get("sync_doc_type", "")).strip() or "unknown"
        counts[key] = counts.get(key, 0) + 1
    if not counts:
        return "-"
    ordered = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return ", ".join(f"{name}={count}" for name, count in ordered[:6])


def _sync_candidate_counter(items: List[Dict[str, Any]], field: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in items:
        if not isinstance(row, dict):
            continue
        key = str(row.get(field, "")).strip() or "unknown"
        counts[key] = counts.get(key, 0) + 1
    ordered = sorted(counts.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
    return {name: count for name, count in ordered[:6]}


def _build_sync_diagnostics(
    *,
    mode: str,
    source_mode: str,
    key: str,
    alias: str,
    policy_path: str,
    sources: List[str],
    include_warnings: List[str],
    items: List[Dict[str, Any]],
    proposal_payloads: List[Dict[str, Any]],
    inspect_lines: List[str],
) -> List[str]:
    notes: List[str] = []
    lower_sources = [str(src or "").lower() for src in sources]
    lower_inspect = [str(line or "").lower() for line in inspect_lines]
    has_test_source = any("aoe_sync_test" in token or "aoe-sync-test" in token for token in lower_sources)
    missing_include = next((w for w in include_warnings if str(w).startswith("include_missing:")), "")

    if has_test_source:
        notes.append("test source active: scenario currently imports a sync test file; replace it with canonical project backlog before production use")

    if missing_include:
        target = str(missing_include).split(":", 1)[1] if ":" in str(missing_include) else "TODO.md"
        notes.append(f"scenario include missing: {target}; update .aoe-team/AOE_TODO.md include path or rely on /sync salvage for bootstrap")

    if (not policy_path) and (
        (not items and not proposal_payloads)
        or any("skipped:low-confidence" in line for line in lower_inspect)
        or any("ops/" in token or "/ops/" in token for token in lower_sources)
    ):
        notes.append("no project sync_policy.json: project is using only global defaults; add one if important docs live outside standard todo paths")

    if mode == "scenario" and (not items) and proposal_payloads:
        notes.append("scenario is empty, but salvage found follow-up proposals; review /todo proposals or widen the salvage time window")

    if (not items) and (not proposal_payloads) and any("skipped:no-todo-marker" in line for line in lower_inspect):
        notes.append(f"recent docs exist but did not expose actionable sections; try /sync salvage {alias or key} 72h")

    if items:
        open_count = sum(1 for row in items if str(row.get("status", "")).strip().lower() == _STATUS_OPEN)
        done_count = sum(1 for row in items if str(row.get("status", "")).strip().lower() == _STATUS_DONE)
        if open_count == 0 and done_count > 0:
            notes.append(
                "recent source produced only completed rows; widen the time window or point AOE_TODO.md at a canonical next-step backlog before off-desk execution"
            )

    if source_mode == "bootstrap_docs" or source_mode.endswith(":bootstrap"):
        notes.append("bootstrap mode merged recent docs, salvage sections, and todo files; review source provenance before using replace/prune")

    return notes[:4]


def _render_sync_lock_message(*, locked_label: str, requested_label: str) -> str:
    locked = str(locked_label or "-").strip() or "-"
    requested = str(requested_label or "-").strip() or "-"
    return (
        "sync blocked by project lock\n"
        f"- locked: {locked}\n"
        f"- requested: {requested}\n"
        "- meaning: this chat is currently pinned to one project\n"
        "next:\n"
        f"- /sync preview {locked} 1h\n"
        f"- /sync {locked} 1h\n"
        "- /sync all 1h   # while focus is on, this narrows to the locked project\n"
        "- /focus off"
    )


def _alias_index(alias: str) -> int:
    token = str(alias or "").strip().upper()
    if token.startswith("O"):
        token = token[1:]
    return int(token) if token.isdigit() else 10**9


def _normalize_priority(token: str) -> str:
    raw = str(token or "").strip().upper()
    if raw in _PRIORITIES:
        return raw
    return "P2"


def _priority_rank(priority: str) -> int:
    return ops_priority_rank(priority)


def _project_alias(entry: Dict[str, Any], fallback: str) -> str:
    return ops_project_alias(entry, fallback)


def _sorted_open_todos(todos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return ops_sorted_open_todos(todos)


def _sorted_active_todos(todos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return queue_sorted_active_todos(todos)


def _count_todo_statuses(todos: List[Dict[str, Any]]) -> Dict[str, int]:
    return queue_count_todo_statuses(todos)


def _blocked_reason_preview(raw: Any, limit: int = 72) -> str:
    return blocked_reason_preview_base(raw, limit=limit)


def _blocked_bucket_label(raw: Any) -> str:
    return blocked_bucket_label_base(raw)


def _blocked_head_summary(todos: List[Dict[str, Any]]) -> Dict[str, Any]:
    return blocked_head_summary_base(todos, blocked_status=_STATUS_BLOCKED, priority_rank=_priority_rank)


def _blocked_bucket_count(todos: List[Dict[str, Any]], bucket: str) -> int:
    return blocked_bucket_count_base(todos, bucket, blocked_status=_STATUS_BLOCKED)


def _manual_followup_indices(todos: List[Dict[str, Any]], limit: int = 1) -> List[int]:
    return manual_followup_indices_base(_sorted_active_todos(todos), limit=limit, blocked_status=_STATUS_BLOCKED)


def _queue_reply_markup(
    rows: List[Dict[str, Any]],
    *,
    followup_only: bool,
    focus_key: str,
    fallback_alias: str = "",
) -> Dict[str, Any]:
    keyboard: List[List[Dict[str, str]]] = []
    for row in rows[:3]:
        alias = str(row.get("alias", "")).strip().upper()
        if not alias:
            continue
        if followup_only:
            row_buttons: List[Dict[str, str]] = [{"text": f"/todo {alias} followup"}]
            ack_ref = str(row.get("manual_followup_ack_ref", "")).strip()
            if ack_ref:
                row_buttons.append({"text": f"/todo {alias} ackrun {ack_ref}"})
            row_buttons.append({"text": f"/orch status {alias}"})
            keyboard.append(row_buttons[:3])
        else:
            keyboard.append([{"text": f"/todo {alias}"}, {"text": f"/orch status {alias}"}])

    fallback = str(fallback_alias or "").strip().upper()
    if followup_only and (not keyboard) and fallback:
        keyboard.append([{"text": f"/todo {fallback} followup"}, {"text": f"/todo {fallback}"}, {"text": f"/orch status {fallback}"}])

    utility_row: List[Dict[str, str]] = []
    if followup_only:
        utility_row.extend([{"text": "/queue"}, {"text": "/map"}, {"text": "/help"}])
    else:
        has_followup = any(int(row.get("manual_followup_count", 0) or 0) > 0 for row in rows)
        if has_followup:
            utility_row.append({"text": "/queue followup"})
        utility_row.extend([{"text": "/next"}, {"text": "/map"}])
    if utility_row:
        keyboard.append(utility_row[:3])

    if focus_key:
        keyboard.append([{"text": "/focus off"}])

    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "예: /queue followup 또는 /todo O# followup",
    }


def _find_pending_todo_for_chat(
    projects: Dict[str, Any],
    chat_id: str,
    *,
    skip_paused: bool = False,
) -> Optional[Tuple[str, Dict[str, Any]]]:
    hit = find_ops_pending_todo_for_chat(projects, chat_id, skip_paused=skip_paused, require_ready=True)
    if not hit:
        return None
    key, _entry, pending = hit
    return key, pending


def _find_todo_item(entry: Dict[str, Any], todo_id: str) -> Optional[Dict[str, Any]]:
    return queue_find_todo_item(entry, todo_id)


def _has_task_linked_to_todo(entry: Dict[str, Any], todo_id: str) -> bool:
    return queue_has_task_linked_to_todo(entry, todo_id)


def _pick_global_next_candidate(
    projects: Dict[str, Any],
    *,
    ignore_busy: bool = False,
    skip_paused: bool = False,
) -> Optional[Dict[str, Any]]:
    return queue_pick_global_next_candidate(
        projects,
        ignore_busy=ignore_busy,
        skip_paused=skip_paused,
    )


def handle_scheduler_command(
    *,
    cmd: str,
    args: Any,
    manager_state: Dict[str, Any],
    chat_id: str,
    chat_role: str,
    orch_target: Optional[str],
    rest: str,
    send: Callable[..., bool],
    get_context: Callable[[Optional[str]], tuple[str, Dict[str, Any], Any]],
    save_manager_state: Callable[..., None],
    now_iso: Callable[[], str],
) -> Optional[Dict[str, Any]]:
    """Handle /next (Mother-Orch global todo scheduler).

    Returns:
      - {"terminal": True} when handled and no run is needed
      - transition dict with {"terminal": False, "cmd":"run", ...} to continue into run pipeline
      - None when cmd does not match
    """
    if cmd not in {"next", "queue", "sync"}:
        return None

    projects = manager_state.get("projects")
    if not isinstance(projects, dict) or not projects:
        send("no orch projects registered. use /map and /orch add first.", context="sched-empty", with_menu=True)
        return {"terminal": True}
    focus_row = _project_lock_row(manager_state)
    focus_key = str(focus_row.get("project_key", "")).strip()
    focus_entry = projects.get(focus_key) if focus_key and isinstance(projects.get(focus_key), dict) else {}
    focus_alias = _project_alias(focus_entry if isinstance(focus_entry, dict) else {}, focus_key) if focus_key else ""

    if cmd == "sync":
        recalled_last_args = False
        raw_rest = str(rest or "").strip()
        user_provided_args = bool(raw_rest)
        if not raw_rest:
            last = _get_last_cmd_args(manager_state, chat_id, "sync")
            if last:
                raw_rest = last
                rest = last
                recalled_last_args = True

        tokens = [t for t in str(rest or "").split() if t.strip()]
        quiet = False
        preview = False
        prune_missing = False
        filtered: List[str] = []
        for tok in tokens:
            low = tok.strip().lower()
            if low in {"quiet", "--quiet", "-q"}:
                quiet = True
                continue
            if low in {"preview", "inspect", "--preview", "--inspect"}:
                preview = True
                continue
            if low in {"prune", "replace", "rebuild", "--prune", "--replace"}:
                prune_missing = True
                continue
            filtered.append(tok)
        tokens = filtered
        history_candidate = raw_rest if (user_provided_args and (not _is_auto_invocation(args)) and (not preview)) else ""

        since_seconds = 0
        since_label = ""
        filtered_since: List[str] = []
        i = 0
        while i < len(tokens):
            tok = str(tokens[i] or "").strip()
            low = tok.lower()

            raw_val = ""
            if low in {"since", "--since", "-s", "within", "--within"}:
                if i + 1 < len(tokens):
                    raw_val = str(tokens[i + 1] or "").strip()
                    i += 2
                else:
                    i += 1
                secs = _parse_since_seconds(raw_val)
                if secs > 0:
                    since_seconds = secs
                    since_label = raw_val
                continue

            if low.startswith("since=") or low.startswith("--since=") or low.startswith("-s="):
                raw_val = tok.split("=", 1)[1].strip() if "=" in tok else ""
                secs = _parse_since_seconds(raw_val)
                if secs > 0:
                    since_seconds = secs
                    since_label = raw_val
                    i += 1
                    continue

            filtered_since.append(tok)
            i += 1

        tokens = filtered_since
        # Shorthand: allow trailing duration token like "1h" without writing "since 1h".
        if since_seconds <= 0 and tokens:
            tail = str(tokens[-1] or "").strip()
            secs = _parse_since_seconds(tail)
            if secs > 0:
                since_seconds = secs
                since_label = tail
                tokens = tokens[:-1]
        if since_seconds > 0:
            min_mtime = max(0.0, float(time.time()) - float(since_seconds))
        else:
            min_mtime = 0.0

        mode = "scenario"
        if tokens:
            head = tokens[0].strip().lower()
            if head in {"recent", "docs", "scan"}:
                mode = "recent_docs"
                tokens = tokens[1:]
            elif head in {"salvage"}:
                mode = "salvage_docs"
                tokens = tokens[1:]
            elif head in {"bootstrap", "recover"}:
                mode = "bootstrap_docs"
                tokens = tokens[1:]
            elif head in {"files", "todo-files", "todofiles"}:
                mode = "todo_files"
                tokens = tokens[1:]

        docs_limit = _DISCOVERY_DEFAULT_DOCS_LIMIT
        if mode in {"recent_docs", "salvage_docs", "bootstrap_docs"} and tokens and tokens[-1].isdigit():
            docs_limit = max(1, min(50, int(tokens[-1])))
            tokens = tokens[:-1]

        files_limit = _DISCOVERY_DEFAULT_TODO_FILES_LIMIT
        if mode == "todo_files" and tokens and tokens[-1].isdigit():
            files_limit = max(1, min(400, int(tokens[-1])))
            tokens = tokens[:-1]

        if prune_missing and (mode in {"recent_docs", "salvage_docs", "bootstrap_docs"} or since_seconds > 0):
            if mode == "recent_docs":
                detail = "recent_docs mode"
            elif mode == "salvage_docs":
                detail = "salvage_docs mode"
            elif mode == "bootstrap_docs":
                detail = "bootstrap_docs mode"
            else:
                detail = f"since {since_label or 'window'}"
            send(
                "sync prune blocked\n"
                "- reason: prune/replace needs a full-scope sync to avoid canceling unrelated todos\n"
                f"- scope: {detail}\n"
                "next:\n"
                "- /sync preview replace <O#|name>\n"
                "- /sync replace <O#|name>\n"
                "- or run plain /sync recent ... without replace",
                context="sync-prune-blocked",
                with_menu=True,
            )
            return {"terminal": True}

        target_token = tokens[0].strip() if tokens else ""
        want_all = False
        if mode == "scenario":
            want_all = (not target_token) or target_token.lower() in {"all", "*"}
        else:
            want_all = bool(target_token) and target_token.lower() in {"all", "*"}

        targets: List[Tuple[str, Dict[str, Any]]] = []
        lock_narrowed = False
        if want_all:
            if focus_key and isinstance(focus_entry, dict):
                targets.append((focus_key, focus_entry))
                lock_narrowed = True
            else:
                targets.extend(list_ops_projects(projects))
        else:
            # allow alias/name: O1, default, etc (or active project when empty)
            requested_label = str(target_token or orch_target or "").strip()
            try:
                key, entry, _p_args = get_context(target_token or orch_target)
            except Exception as exc:
                if focus_key and "project lock active" in str(exc).strip().lower():
                    send(
                        _render_sync_lock_message(
                            locked_label=focus_alias or focus_key,
                            requested_label=requested_label or "-",
                        ),
                        context="sync-locked",
                        with_menu=True,
                    )
                    return {"terminal": True}
                raise
            if focus_key and key != focus_key:
                send(
                    _render_sync_lock_message(
                        locked_label=focus_alias or focus_key,
                        requested_label=_project_alias(entry, key),
                    ),
                    context="sync-locked",
                    with_menu=True,
                )
                return {"terminal": True}
            targets.append((key, entry))

        total = {
            "parsed": 0,
            "added": 0,
            "updated": 0,
            "done": 0,
            "pruned": 0,
            "proposed": 0,
            "proposal_duplicates": 0,
            "missing": 0,
            "skipped_stale": 0,
            "skipped_done_missing": 0,
            "docs_used": 0,
            "docs_scanned": 0,
            "files_used": 0,
            "files_scanned": 0,
        }
        total_candidate_classes: Dict[str, int] = {}
        total_candidate_doc_types: Dict[str, int] = {}
        per_project_lines: List[str] = []
        preview_blocks: List[str] = []
        any_changed = False
        proposal_changed = False
        sync_meta_changed = False
        sync_mark_at = now_iso()

        for key, entry in targets:
            alias = _project_alias(entry, key)
            display = str(entry.get("display_name", "")).strip() or str(entry.get("name", "")).strip() or key
            team_dir = Path(str(entry.get("team_dir", "") or "")).expanduser().resolve()
            items: List[Dict[str, Any]] = []
            sources: List[str] = []
            meta: Dict[str, Any] = {}
            project_note = ""
            scenario_exists = False
            scenario_rel = ""
            source_mode = mode
            inspect_lines: List[str] = []
            proposal_payloads: List[Dict[str, Any]] = []
            proposal_meta: Dict[str, Any] = {}
            proposal_sources: List[str] = []
            diagnosis_lines: List[str] = []
            sync_policy = _load_project_sync_policy(entry, team_dir)
            policy_path = str(sync_policy.get("_policy_path", "")).strip() if isinstance(sync_policy, dict) else ""

            include_warnings: List[str] = []
            project_root_raw = str(entry.get("project_root", "") or "").strip()
            project_root = Path(project_root_raw).expanduser().resolve() if project_root_raw else team_dir.parent.resolve()

            if mode == "scenario":
                scenario_path = (team_dir / _SCENARIO_FILENAME).resolve()
                if scenario_path.exists():
                    scenario_exists = True
                    scenario_rel = _rel_display(scenario_path, project_root)
                    try:
                        raw_text = scenario_path.read_text(encoding="utf-8")
                    except Exception as e:
                        if preview:
                            preview_blocks.append(
                                "\n".join(
                                    [
                                        f"- {alias} {display} ({key})",
                                        f"  mode: scenario",
                                        f"  scenario: {scenario_rel or _SCENARIO_FILENAME}",
                                        f"  error: read_failed ({e})",
                                    ]
                                )
                            )
                        else:
                            per_project_lines.append(f"- {key}: read_failed ({e})")
                        continue

                    include_tokens = _scenario_include_tokens(raw_text)
                    include_files: List[Path] = []
                    scenario_info = _classify_sync_source(scenario_path, project_root, mode="scenario")
                    direct_items = _tag_sync_items(_parse_scenario_lines(raw_text), rel=scenario_rel or str(scenario_path), info=scenario_info)
                    if preview:
                        inspect_lines.append(f"{scenario_rel or _SCENARIO_FILENAME} -> used:{len(direct_items)}")
                    for tok in include_tokens:
                        try:
                            candidate = Path(tok).expanduser()
                            resolved = (
                                (scenario_path.parent / candidate).resolve() if not candidate.is_absolute() else candidate.resolve()
                            )
                        except Exception:
                            include_warnings.append(f"include_invalid:{tok}")
                            continue
                        if resolved.suffix.lower() and resolved.suffix.lower() not in _DISCOVERY_ALLOWED_EXTS:
                            include_warnings.append(f"include_ext_blocked:{resolved.name}")
                            continue
                        if not _is_within(resolved, project_root):
                            include_warnings.append(f"include_outside_root:{resolved.name}")
                            continue
                        if not resolved.exists() or not resolved.is_file():
                            include_warnings.append(f"include_missing:{resolved.name}")
                            continue
                        try:
                            st_inc = resolved.stat()
                            if st_inc.st_size <= 0 or st_inc.st_size > int(_DISCOVERY_DEFAULT_MAX_BYTES):
                                include_warnings.append(f"include_too_large:{resolved.name}")
                                continue
                        except Exception:
                            include_warnings.append(f"include_stat_failed:{resolved.name}")
                            continue
                        include_files.append(resolved)

                    items = direct_items
                    for inc in include_files:
                        try:
                            inc_text = inc.read_text(encoding="utf-8", errors="replace")
                        except Exception:
                            include_warnings.append(f"include_read_failed:{inc.name}")
                            continue
                        inc_info = _classify_sync_source(inc, project_root, mode="scenario")
                        inc_info["source_class"] = "scenario"
                        inc_info["sync_group"] = "scenario"
                        inc_info["confidence"] = max(0.98, float(inc_info.get("confidence", 0.0) or 0.0))
                        inc_items = _tag_sync_items(
                            _parse_scenario_lines(inc_text),
                            rel=_rel_display(inc, project_root),
                            info=inc_info,
                        )
                        items.extend(inc_items)
                        if preview and len(inspect_lines) < 12:
                            inspect_lines.append(f"{_rel_display(inc, project_root)} -> used:{len(inc_items)}")

                    sources = [scenario_rel or str(scenario_path)]
                    for inc in include_files:
                        sources.append(_rel_display(inc, project_root))

                    if not items:
                        fallback_mode, fallback_items, fallback_meta, fallback_sources = _discover_sync_fallback_todos(
                            project_root=project_root,
                            docs_limit=docs_limit,
                            files_limit=files_limit,
                            max_bytes=_DISCOVERY_DEFAULT_MAX_BYTES,
                            min_mtime=min_mtime,
                            sync_policy=sync_policy,
                        )
                        if fallback_mode:
                            items = fallback_items
                            meta = fallback_meta
                            sources = fallback_sources
                            project_note = f"(scenario-empty->fallback:{fallback_mode})"
                            source_mode = f"scenario-empty->fallback:{fallback_mode}"
                            inspect_lines.extend([str(line) for line in (meta.get("preview") or [])[:8]])
                            if fallback_mode == "files":
                                total["files_used"] += int(meta.get("files_used", 0) or 0)
                                total["files_scanned"] += int(meta.get("scanned", 0) or 0)
                            elif fallback_mode == "recent":
                                total["docs_used"] += int(meta.get("docs_used", 0) or 0)
                                total["docs_scanned"] += int(meta.get("scanned", 0) or 0)
                            elif fallback_mode == "salvage":
                                total["docs_used"] += int(meta.get("docs_used", 0) or 0)
                                total["docs_scanned"] += int(meta.get("scanned", 0) or 0)
                            elif fallback_mode == "bootstrap":
                                total["docs_used"] += int(meta.get("docs_used", 0) or 0)
                                total["docs_scanned"] += int(meta.get("docs_scanned", 0) or 0)
                                total["files_used"] += int(meta.get("files_used", 0) or 0)
                                total["files_scanned"] += int(meta.get("files_scanned", 0) or 0)

                else:
                    if preview:
                        inspect_lines.append(f"{_SCENARIO_FILENAME} -> missing")
                    # Fallback: allow `sync all` to work even when a project has not been bootstrapped
                    # with `.aoe-team/AOE_TODO.md` yet.
                    fallback_mode, items, meta, sources = _discover_sync_fallback_todos(
                        project_root=project_root,
                        docs_limit=docs_limit,
                        files_limit=files_limit,
                        max_bytes=_DISCOVERY_DEFAULT_MAX_BYTES,
                        min_mtime=min_mtime,
                        sync_policy=sync_policy,
                    )
                    if fallback_mode == "files":
                        total["files_used"] += int(meta.get("files_used", 0) or 0)
                        total["files_scanned"] += int(meta.get("scanned", 0) or 0)
                    elif fallback_mode == "recent":
                        total["docs_used"] += int(meta.get("docs_used", 0) or 0)
                        total["docs_scanned"] += int(meta.get("scanned", 0) or 0)
                    elif fallback_mode == "salvage":
                        total["docs_used"] += int(meta.get("docs_used", 0) or 0)
                        total["docs_scanned"] += int(meta.get("scanned", 0) or 0)
                    elif fallback_mode == "bootstrap":
                        total["docs_used"] += int(meta.get("docs_used", 0) or 0)
                        total["docs_scanned"] += int(meta.get("docs_scanned", 0) or 0)
                        total["files_used"] += int(meta.get("files_used", 0) or 0)
                        total["files_scanned"] += int(meta.get("files_scanned", 0) or 0)
                    if not items:
                        total["missing"] += 1
                        diagnosis_lines = _build_sync_diagnostics(
                            mode=mode,
                            source_mode=f"fallback:{fallback_mode or 'none'}",
                            key=key,
                            alias=alias,
                            policy_path=policy_path,
                            sources=list(sources or []),
                            include_warnings=list(include_warnings or []),
                            items=[],
                            proposal_payloads=[],
                            inspect_lines=list(inspect_lines or []) + list(meta.get("preview") or []),
                        )
                        if not preview:
                            sync_meta_changed = _stamp_sync_meta(
                                entry,
                                at=sync_mark_at,
                                mode=f"fallback:{fallback_mode or 'none'}",
                                candidate_classes={},
                                candidate_doc_types={},
                            ) or sync_meta_changed
                        if preview:
                            block = [
                                f"- {alias} {display} ({key})",
                                "  mode: scenario-missing",
                                f"  root: {project_root}",
                                f"  scenario: {_SCENARIO_FILENAME}",
                                "  result: no fallback todos found",
                            ]
                            inspect_lines.extend([str(line) for line in (meta.get("preview") or [])[:8]])
                            if inspect_lines:
                                block.append("  inspected:")
                                for line in inspect_lines[:8]:
                                    block.append(f"    - {line}")
                            if diagnosis_lines:
                                block.append("  diagnosis:")
                                for line in diagnosis_lines:
                                    block.append(f"    - {line}")
                            preview_blocks.append("\n".join(block))
                        else:
                            per_project_lines.append(f"- {key}: missing {_SCENARIO_FILENAME}")
                            for note in diagnosis_lines[:2]:
                                per_project_lines.append(f"  diag: {note}")
                        continue
                    project_note = f"(fallback:{fallback_mode})" if fallback_mode else "(fallback)"
                    source_mode = f"fallback:{fallback_mode}" if fallback_mode else "fallback"
                    inspect_lines.extend([str(line) for line in (meta.get("preview") or [])[:8]])

            else:
                if mode == "recent_docs":
                    items, meta, sources = _discover_recent_doc_todos(
                        project_root=project_root,
                        docs_limit=docs_limit,
                        candidate_keep=_DISCOVERY_DEFAULT_CANDIDATE_KEEP,
                        max_bytes=_DISCOVERY_DEFAULT_MAX_BYTES,
                        min_mtime=min_mtime,
                        sync_policy=sync_policy,
                    )
                    inspect_lines.extend([str(line) for line in (meta.get("preview") or [])[:8]])
                    total["docs_used"] += int(meta.get("docs_used", 0) or 0)
                    total["docs_scanned"] += int(meta.get("scanned", 0) or 0)
                    if not items:
                        total["missing"] += 1
                        diagnosis_lines = _build_sync_diagnostics(
                            mode=mode,
                            source_mode="recent_docs",
                            key=key,
                            alias=alias,
                            policy_path=policy_path,
                            sources=list(sources or []),
                            include_warnings=list(include_warnings or []),
                            items=[],
                            proposal_payloads=[],
                            inspect_lines=list(inspect_lines or []) + list(meta.get("preview") or []),
                        )
                        if not preview:
                            sync_meta_changed = _stamp_sync_meta(
                                entry,
                                at=sync_mark_at,
                                mode="recent_docs",
                                candidate_classes={},
                                candidate_doc_types={},
                            ) or sync_meta_changed
                        if preview:
                            block = [
                                f"- {alias} {display} ({key})",
                                "  mode: recent_docs",
                                f"  root: {project_root}",
                                "  result: no recent todo docs found",
                            ]
                            if inspect_lines:
                                block.append("  inspected:")
                                for line in inspect_lines[:8]:
                                    block.append(f"    - {line}")
                            if diagnosis_lines:
                                block.append("  diagnosis:")
                                for line in diagnosis_lines:
                                    block.append(f"    - {line}")
                            preview_blocks.append("\n".join(block))
                        else:
                            per_project_lines.append(f"- {key}: no recent todo docs found")
                            for note in diagnosis_lines[:2]:
                                per_project_lines.append(f"  diag: {note}")
                        continue
                elif mode == "salvage_docs":
                    items, meta, sources = _discover_salvage_doc_todos(
                        project_root=project_root,
                        docs_limit=max(docs_limit, 5),
                        candidate_keep=max(_DISCOVERY_DEFAULT_CANDIDATE_KEEP, 500),
                        max_bytes=_DISCOVERY_DEFAULT_MAX_BYTES,
                        min_mtime=min_mtime,
                        sync_policy=sync_policy,
                    )
                    inspect_lines.extend([str(line) for line in (meta.get("preview") or [])[:8]])
                    total["docs_used"] += int(meta.get("docs_used", 0) or 0)
                    total["docs_scanned"] += int(meta.get("scanned", 0) or 0)
                elif mode == "bootstrap_docs":
                    fallback_mode, items, meta, sources = _discover_sync_fallback_todos(
                        project_root=project_root,
                        docs_limit=max(docs_limit, 5),
                        files_limit=files_limit,
                        max_bytes=_DISCOVERY_DEFAULT_MAX_BYTES,
                        min_mtime=min_mtime,
                        sync_policy=sync_policy,
                    )
                    source_mode = "bootstrap_docs"
                    if fallback_mode:
                        source_mode = f"bootstrap_docs:{fallback_mode}"
                    inspect_lines.extend([str(line) for line in (meta.get("preview") or [])[:8]])
                    total["docs_used"] += int(meta.get("docs_used", 0) or 0)
                    total["docs_scanned"] += int(meta.get("docs_scanned", 0) or 0)
                    total["files_used"] += int(meta.get("files_used", 0) or 0)
                    total["files_scanned"] += int(meta.get("files_scanned", 0) or 0)
                else:
                    items, meta, sources = _discover_todo_file_todos(
                        project_root=project_root,
                        files_limit=files_limit,
                        max_bytes=_DISCOVERY_DEFAULT_MAX_BYTES,
                        min_mtime=min_mtime,
                        sync_policy=sync_policy,
                    )
                    inspect_lines.extend([str(line) for line in (meta.get("preview") or [])[:8]])
                    total["files_used"] += int(meta.get("files_used", 0) or 0)
                    total["files_scanned"] += int(meta.get("scanned", 0) or 0)
                    if not items:
                        total["missing"] += 1
                        diagnosis_lines = _build_sync_diagnostics(
                            mode=mode,
                            source_mode="todo_files",
                            key=key,
                            alias=alias,
                            policy_path=policy_path,
                            sources=list(sources or []),
                            include_warnings=list(include_warnings or []),
                            items=[],
                            proposal_payloads=[],
                            inspect_lines=list(inspect_lines or []) + list(meta.get("preview") or []),
                        )
                        if not preview:
                            sync_meta_changed = _stamp_sync_meta(
                                entry,
                                at=sync_mark_at,
                                mode="todo_files",
                                candidate_classes={},
                                candidate_doc_types={},
                            ) or sync_meta_changed
                        if preview:
                            block = [
                                f"- {alias} {display} ({key})",
                                "  mode: todo_files",
                                f"  root: {project_root}",
                                "  result: no todo files found",
                            ]
                            if inspect_lines:
                                block.append("  inspected:")
                                for line in inspect_lines[:8]:
                                    block.append(f"    - {line}")
                            if diagnosis_lines:
                                block.append("  diagnosis:")
                                for line in diagnosis_lines:
                                    block.append(f"    - {line}")
                            preview_blocks.append("\n".join(block))
                        else:
                            per_project_lines.append(f"- {key}: no todo files found")
                            for note in diagnosis_lines[:2]:
                                per_project_lines.append(f"  diag: {note}")
                        continue

            if items:
                best_by_summary: Dict[str, Dict[str, Any]] = {}
                for row in items:
                    if not isinstance(row, dict):
                        continue
                    skey = _normalize_summary_key(str(row.get("summary", "")))
                    if not skey:
                        continue
                    prev = best_by_summary.get(skey)
                    if isinstance(prev, dict):
                        best_by_summary[skey] = _choose_sync_row(prev, row)
                    else:
                        best_by_summary[skey] = row
                items = list(best_by_summary.values())

            if mode in {"salvage_docs", "bootstrap_docs"} or str(source_mode or "").endswith(":salvage") or str(source_mode or "").startswith("fallback:bootstrap"):
                proposal_payloads, proposal_meta, proposal_sources = _discover_salvage_doc_proposals(
                    project_root=project_root,
                    docs_limit=max(docs_limit, 5),
                    candidate_keep=max(_DISCOVERY_DEFAULT_CANDIDATE_KEEP, 500),
                    max_bytes=_DISCOVERY_DEFAULT_MAX_BYTES,
                    min_mtime=min_mtime,
                    sync_policy=sync_policy,
                )

            if include_warnings:
                # Best-effort: keep warnings short and do not fail sync.
                short = ", ".join(include_warnings[:3])
                if len(include_warnings) > 3:
                    short += f", +{len(include_warnings) - 3}"
                if preview:
                    inspect_lines.append(f"include_warn:{short}")
                else:
                    per_project_lines.append(f"- {key}: include_warn {short}")

            if mode in {"salvage_docs", "bootstrap_docs"} and (not items) and (not proposal_payloads):
                total["missing"] += 1
                diagnosis_lines = _build_sync_diagnostics(
                    mode=mode,
                    source_mode=str(source_mode or mode),
                    key=key,
                    alias=alias,
                    policy_path=policy_path,
                    sources=list(sources or []) + list(proposal_sources or []),
                    include_warnings=list(include_warnings or []),
                    items=[],
                    proposal_payloads=[],
                    inspect_lines=list(inspect_lines or []) + list(proposal_meta.get("preview") or []),
                )
                if not preview:
                    sync_meta_changed = _stamp_sync_meta(
                        entry,
                        at=sync_mark_at,
                        mode=str(source_mode or mode),
                        candidate_classes={},
                        candidate_doc_types={},
                    ) or sync_meta_changed
                if preview:
                    block = [
                        f"- {alias} {display} ({key})",
                        f"  mode: {source_mode or mode}",
                        f"  root: {project_root}",
                        (
                            "  result: no salvage todo docs found"
                            if mode == "salvage_docs"
                            else "  result: no bootstrap backlog candidates found"
                        ),
                    ]
                    if inspect_lines:
                        block.append("  inspected:")
                        for line in inspect_lines[:8]:
                            block.append(f"    - {line}")
                    if proposal_meta.get("preview"):
                        block.append("  proposal_scan:")
                        for line in list(proposal_meta.get("preview") or [])[:6]:
                            block.append(f"    - {line}")
                    if diagnosis_lines:
                        block.append("  diagnosis:")
                        for line in diagnosis_lines:
                            block.append(f"    - {line}")
                    preview_blocks.append("\n".join(block))
                else:
                    if mode == "salvage_docs":
                        per_project_lines.append(f"- {key}: no salvage todo docs found")
                    else:
                        per_project_lines.append(f"- {key}: no bootstrap backlog candidates found")
                    for note in diagnosis_lines[:2]:
                        per_project_lines.append(f"  diag: {note}")
                continue

            if mode == "scenario" and scenario_exists and (not items):
                total["missing"] += 1
                diagnosis_lines = _build_sync_diagnostics(
                    mode=mode,
                    source_mode="scenario-empty",
                    key=key,
                    alias=alias,
                    policy_path=policy_path,
                    sources=list(sources or []),
                    include_warnings=list(include_warnings or []),
                    items=[],
                    proposal_payloads=list(proposal_payloads or []),
                    inspect_lines=list(inspect_lines or []) + list(proposal_meta.get("preview") or []),
                )
                if not preview:
                    sync_meta_changed = _stamp_sync_meta(
                        entry,
                        at=sync_mark_at,
                        mode="scenario-empty",
                        candidate_classes={},
                        candidate_doc_types={},
                    ) or sync_meta_changed
                if preview:
                    block = [
                        f"- {alias} {display} ({key})",
                        "  mode: scenario",
                        f"  root: {project_root}",
                        f"  scenario: {scenario_rel or _SCENARIO_FILENAME}",
                        "  result: scenario empty, no fallback todos found",
                    ]
                    if inspect_lines:
                        block.append("  inspected:")
                        for line in inspect_lines[:8]:
                            block.append(f"    - {line}")
                    if diagnosis_lines:
                        block.append("  diagnosis:")
                        for line in diagnosis_lines:
                            block.append(f"    - {line}")
                    preview_blocks.append("\n".join(block))
                else:
                    per_project_lines.append(f"- {key}: scenario empty, no fallback todos found")
                    for note in diagnosis_lines[:2]:
                        per_project_lines.append(f"  diag: {note}")
                continue

            counts = _apply_scenario_items_to_entry(
                entry=(copy.deepcopy(entry) if preview else entry),
                items=items,
                chat_id=str(chat_id),
                now_iso=now_iso,
                dry_run=(True if preview else bool(args.dry_run)),
                source_mode=source_mode or mode,
                sources=sources,
                prune_missing=prune_missing,
            )
            total["parsed"] += int(counts.get("parsed", 0) or 0)
            total["added"] += int(counts.get("added", 0) or 0)
            total["updated"] += int(counts.get("updated", 0) or 0)
            total["done"] += int(counts.get("done", 0) or 0)
            total["pruned"] += int(counts.get("pruned", 0) or 0)
            total["skipped_done_missing"] += int(counts.get("skipped_done_missing", 0) or 0)
            for row in items:
                if not isinstance(row, dict):
                    continue
                cls = str(row.get("sync_source_class", "")).strip() or "unknown"
                total_candidate_classes[cls] = total_candidate_classes.get(cls, 0) + 1
                dtype = str(row.get("sync_doc_type", "")).strip() or "unknown"
                total_candidate_doc_types[dtype] = total_candidate_doc_types.get(dtype, 0) + 1

            changed = bool(
                (counts.get("added") or 0)
                or (counts.get("updated") or 0)
                or (counts.get("done") or 0)
                or (counts.get("pruned") or 0)
            )
            if changed and (not preview):
                any_changed = True
            project_candidate_classes = _sync_candidate_counter(items, "sync_source_class")
            project_candidate_doc_types = _sync_candidate_counter(items, "sync_doc_type")
            if not preview:
                sync_meta_changed = _stamp_sync_meta(
                    entry,
                    at=sync_mark_at,
                    mode=source_mode or mode,
                    candidate_classes=project_candidate_classes,
                    candidate_doc_types=project_candidate_doc_types,
                ) or sync_meta_changed

            proposal_result: Dict[str, Any] = {
                "created_count": 0,
                "duplicate_count": 0,
                "skipped_count": 0,
                "created_ids": [],
            }
            if proposal_payloads:
                total["proposal_duplicates"] += len(proposal_payloads) - len(
                    {_normalize_summary_key(str(row.get("summary", ""))) for row in proposal_payloads if isinstance(row, dict)}
                )
                if not preview:
                    proposal_result = merge_todo_proposals(
                        entry=entry,
                        request_id=f"sync-salvage:{key}:{sync_mark_at}",
                        task=None,
                        source_todo_id="",
                        proposals_data=proposal_payloads,
                        now_iso=now_iso,
                    )
                    if int(proposal_result.get("created_count", 0) or 0) > 0:
                        proposal_changed = True
                        entry["updated_at"] = sync_mark_at
                total["proposed"] += int(
                    (len(proposal_payloads) if preview else proposal_result.get("created_count", 0)) or 0
                )
                total["proposal_duplicates"] += int(
                    (0 if preview else proposal_result.get("duplicate_count", 0)) or 0
                )

            diagnosis_lines = _build_sync_diagnostics(
                mode=mode,
                source_mode=str(source_mode or ""),
                key=key,
                alias=alias,
                policy_path=policy_path,
                sources=list(sources or []),
                include_warnings=list(include_warnings or []),
                items=list(items or []),
                proposal_payloads=list(proposal_payloads or []),
                inspect_lines=list(inspect_lines or []) + list(proposal_meta.get("preview") or []),
            )

            if preview:
                block = [
                    f"- {alias} {display} ({key})",
                    f"  mode: {source_mode}",
                    f"  root: {project_root}",
                ]
                if scenario_exists:
                    block.append(f"  scenario: {scenario_rel or _SCENARIO_FILENAME}")
                if sources:
                    src = ", ".join(sources[:4])
                    if len(src) > 160:
                        src = src[:157] + "..."
                    block.append(f"  sources: {src}")
                if policy_path:
                    block.append(f"  policy: {_rel_display(Path(policy_path), project_root)}")
                if mode in {"recent_docs", "salvage_docs"}:
                    block.append(
                        f"  discovery: docs_used={meta.get('docs_used', 0)}/{docs_limit} scanned={meta.get('scanned', 0)}"
                    )
                elif mode == "bootstrap_docs":
                    block.append(
                        f"  discovery: docs_used={meta.get('docs_used', 0)}/{max(docs_limit, 5)} "
                        f"docs_scanned={meta.get('docs_scanned', 0)} "
                        f"files_used={meta.get('files_used', 0)} files_scanned={meta.get('files_scanned', 0)}"
                    )
                elif mode == "todo_files":
                    block.append(
                        f"  discovery: files_used={meta.get('files_used', 0)}/{files_limit} scanned={meta.get('scanned', 0)}"
                    )
                elif source_mode.endswith(":files"):
                    block.append(
                        f"  discovery: files_used={meta.get('files_used', 0)} scanned={meta.get('scanned', 0)}"
                    )
                elif source_mode.endswith(":recent"):
                    block.append(
                        f"  discovery: docs_used={meta.get('docs_used', 0)} scanned={meta.get('scanned', 0)}"
                    )
                elif source_mode.endswith(":salvage"):
                    block.append(
                        f"  discovery: docs_used={meta.get('docs_used', 0)} scanned={meta.get('scanned', 0)}"
                    )
                elif source_mode.endswith(":bootstrap"):
                    block.append(
                        f"  discovery: docs_used={meta.get('docs_used', 0)} docs_scanned={meta.get('docs_scanned', 0)} "
                        f"files_used={meta.get('files_used', 0)} files_scanned={meta.get('files_scanned', 0)}"
                    )
                block.append(
                    f"  effect: parsed={counts.get('parsed', 0)} would_add={counts.get('added', 0)} "
                    f"would_update={counts.get('updated', 0)} would_done={counts.get('done', 0)}"
                    + (f" would_prune={counts.get('pruned', 0)}" if prune_missing else "")
                )
                if proposal_payloads:
                    block.append(f"  would_propose: {len(proposal_payloads)}")
                block.append(f"  candidate_classes: {_summarize_sync_candidate_classes(items)}")
                block.append(f"  candidate_doc_types: {_summarize_sync_candidate_doc_types(items)}")
                skipped_done = int(counts.get("skipped_done_missing", 0) or 0)
                if skipped_done:
                    block.append(f"  skipped_done_missing: {skipped_done}")
                if diagnosis_lines:
                    block.append("  diagnosis:")
                    for line in diagnosis_lines:
                        block.append(f"    - {line}")
                if inspect_lines:
                    block.append("  inspected:")
                    for line in inspect_lines[:8]:
                        block.append(f"    - {line}")
                if items:
                    block.append("  candidates:")
                    for row in items[:6]:
                        block.append(f"    - {_preview_item_line(row)}")
                    if len(items) > 6:
                        block.append(f"    - ... {len(items) - 6} more")
                if proposal_payloads:
                    block.append("  proposals:")
                    for row in proposal_payloads[:4]:
                        src_file = str(row.get("source_file", "")).strip()
                        src_section = str(row.get("source_section", "")).strip()
                        prov = []
                        if src_file:
                            prov.append(f"@{src_file}")
                        if src_section:
                            prov.append(f"#{src_section}")
                        suffix = f" [{' '.join(prov)}]" if prov else ""
                        block.append(
                            f"    - [{str(row.get('kind', 'followup')).strip() or 'followup'} {float(row.get('confidence', 0.0) or 0.0):.2f}] "
                            f"{str(row.get('summary', '')).strip()[:120]}{suffix}"
                        )
                    if len(proposal_payloads) > 4:
                        block.append(f"    - ... {len(proposal_payloads) - 4} more")
                preview_blocks.append("\n".join(block))
                continue

            if mode == "scenario":
                src = ", ".join(sources[:3])
                if len(src) > 120:
                    src = src[:117] + "..."
                per_project_lines.append(
                        (
                            f"- {key}: parsed={counts.get('parsed', 0)} added={counts.get('added', 0)} "
                            f"updated={counts.get('updated', 0)} done={counts.get('done', 0)}"
                            + (f" pruned={counts.get('pruned', 0)}" if prune_missing else "")
                            + (f" {project_note}" if project_note else "")
                            + (f" src={src}" if project_note and src else "")
                            + (" (no changes)" if (not changed) else "")
                    ).strip()
                )
            else:
                src = ", ".join(sources[: max(0, docs_limit)])
                if len(src) > 120:
                    src = src[:117] + "..."
                if mode in {"recent_docs", "salvage_docs"}:
                    per_project_lines.append(
                        (
                            f"- {key}: docs={meta.get('docs_used', 0)}/{docs_limit} scanned={meta.get('scanned', 0)} "
                            f"parsed={counts.get('parsed', 0)} added={counts.get('added', 0)} "
                            f"updated={counts.get('updated', 0)} done={counts.get('done', 0)}"
                            + (f" proposed={proposal_result.get('created_count', 0)}" if proposal_payloads else "")
                            + (f" pruned={counts.get('pruned', 0)}" if prune_missing else "")
                            + (" (no changes)" if (not changed) else "")
                            + (f" src={src}" if src else "")
                        ).strip()
                    )
                elif mode == "bootstrap_docs":
                    per_project_lines.append(
                        (
                            f"- {key}: docs={meta.get('docs_used', 0)}/{max(docs_limit, 5)} "
                            f"docs_scanned={meta.get('docs_scanned', 0)} "
                            f"files={meta.get('files_used', 0)}/{files_limit} "
                            f"files_scanned={meta.get('files_scanned', 0)} "
                            f"parsed={counts.get('parsed', 0)} added={counts.get('added', 0)} "
                            f"updated={counts.get('updated', 0)} done={counts.get('done', 0)}"
                            + (f" proposed={proposal_result.get('created_count', 0)}" if proposal_payloads else "")
                            + (f" pruned={counts.get('pruned', 0)}" if prune_missing else "")
                            + (" (no changes)" if (not changed) else "")
                            + (f" src={src}" if src else "")
                        ).strip()
                    )
                else:
                    per_project_lines.append(
                        (
                            f"- {key}: files={meta.get('files_used', 0)}/{files_limit} scanned={meta.get('scanned', 0)} "
                            f"parsed={counts.get('parsed', 0)} added={counts.get('added', 0)} "
                            f"updated={counts.get('updated', 0)} done={counts.get('done', 0)}"
                            + (f" pruned={counts.get('pruned', 0)}" if prune_missing else "")
                            + (" (no changes)" if (not changed) else "")
                            + (f" src={src}" if src else "")
                        ).strip()
                    )
                for note in diagnosis_lines[:2]:
                    per_project_lines.append(f"  diag: {note}")

        if preview:
            lines: List[str] = ["sync preview"]
            if focus_key:
                lines.append(f"- project_lock: {focus_alias or focus_key}")
                if lock_narrowed:
                    lines.append("- scope: narrowed to locked project")
            if recalled_last_args and raw_rest:
                lines.append(f"- args: {raw_rest} (reused)")
            if mode == "recent_docs":
                lines.append("- mode: recent_docs")
                lines.append(f"- docs_per_project: {docs_limit}")
            elif mode == "salvage_docs":
                lines.append("- mode: salvage_docs")
                lines.append(f"- docs_per_project: {max(docs_limit, 5)}")
            elif mode == "bootstrap_docs":
                lines.append("- mode: bootstrap_docs")
                lines.append(f"- docs_per_project: {max(docs_limit, 5)}")
                lines.append(f"- files_per_project: {files_limit}")
            elif mode == "todo_files":
                lines.append("- mode: todo_files")
                lines.append(f"- files_per_project: {files_limit}")
            else:
                lines.append("- mode: scenario")
            if since_seconds > 0 and since_label:
                lines.append(f"- since: {since_label}")
            lines.append(f"- projects: {len(targets)}")
            if total_candidate_classes:
                ordered = sorted(total_candidate_classes.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
                lines.append("- candidate_classes: " + ", ".join(f"{k}={v}" for k, v in ordered[:6]))
            if total_candidate_doc_types:
                ordered = sorted(total_candidate_doc_types.items(), key=lambda kv: (-int(kv[1]), str(kv[0])))
                lines.append("- candidate_doc_types: " + ", ".join(f"{k}={v}" for k, v in ordered[:6]))
            lines.append(f"- parsed: {total['parsed']}")
            lines.append(f"- would_add: {total['added']}")
            lines.append(f"- would_update: {total['updated']}")
            lines.append(f"- would_done: {total['done']}")
            if total["proposed"]:
                lines.append(f"- would_propose: {total['proposed']}")
            if prune_missing:
                lines.append(f"- would_prune: {total['pruned']}")
            if total["missing"]:
                lines.append(f"- missing: {total['missing']}")
            skipped_done = int(total.get("skipped_done_missing", 0) or 0)
            if skipped_done:
                lines.append(f"- skipped_done_missing: {skipped_done}")
            if preview_blocks:
                lines.append("")
                lines.append("projects:")
                for idx, block in enumerate(preview_blocks[:8]):
                    if idx:
                        lines.append("")
                    lines.extend(block.splitlines())
                if len(preview_blocks) > 8:
                    lines.append("")
                    lines.append(f"... ({len(preview_blocks) - 8} more projects)")
            lines.extend(
                [
                    "",
                    "next:",
                    "- /sync <same args without preview>   # actually import",
                    "- /queue",
                    "- /next",
                ]
            )
            send("\n".join(lines).strip(), context="sync-preview", with_menu=True)
            return {"terminal": True}

        history_changed = False
        if history_candidate and (not args.dry_run):
            history_changed = _set_last_cmd_args(manager_state, chat_id, "sync", history_candidate, now_iso())

        if (any_changed or proposal_changed) and (not args.dry_run):
            save_manager_state(args.manager_state_file, manager_state)
        elif sync_meta_changed and (not args.dry_run):
            save_manager_state(args.manager_state_file, manager_state)
        elif history_changed and (not args.dry_run):
            save_manager_state(args.manager_state_file, manager_state)

        if quiet:
            # Quiet mode: do not spam output unless this run actually changed the queue.
            if any_changed or proposal_changed:
                msg_lines: List[str] = ["sync updated"]
                if recalled_last_args and raw_rest:
                    msg_lines.append(f"- args: {raw_rest} (reused)")
                if mode == "recent_docs":
                    msg_lines.append("- mode: recent_docs")
                    msg_lines.append(f"- docs_per_project: {docs_limit}")
                elif mode == "salvage_docs":
                    msg_lines.append("- mode: salvage_docs")
                    msg_lines.append(f"- docs_per_project: {max(docs_limit, 5)}")
                elif mode == "bootstrap_docs":
                    msg_lines.append("- mode: bootstrap_docs")
                    msg_lines.append(f"- docs_per_project: {max(docs_limit, 5)}")
                    msg_lines.append(f"- files_per_project: {files_limit}")
                elif mode == "todo_files":
                    msg_lines.append("- mode: todo_files")
                    msg_lines.append(f"- files_per_project: {files_limit}")
                if since_seconds > 0 and since_label:
                    msg_lines.append(f"- since: {since_label}")
                msg_lines.append(f"- projects: {len(targets)}")
                if mode == "scenario":
                    msg_lines.append(f"- missing_files: {total['missing']}")
                    if since_seconds > 0:
                        msg_lines.append(f"- skipped_stale: {total['skipped_stale']}")
                else:
                    if mode in {"recent_docs", "salvage_docs", "bootstrap_docs"}:
                        msg_lines.append(f"- missing_docs: {total['missing']}")
                    else:
                        msg_lines.append(f"- missing_files: {total['missing']}")
                msg_lines.append(f"- added: {total['added']}")
                msg_lines.append(f"- updated: {total['updated']}")
                msg_lines.append(f"- done: {total['done']}")
                if total["proposed"]:
                    msg_lines.append(f"- proposed: {total['proposed']}")
                if prune_missing:
                    msg_lines.append(f"- pruned: {total['pruned']}")
                send("\n".join(msg_lines).strip(), context="sync-quiet", with_menu=True)
            return {"terminal": True}

        lines: List[str] = ["sync finished"]
        if focus_key:
            lines.append(f"- project_lock: {focus_alias or focus_key}")
            if lock_narrowed:
                lines.append("- scope: narrowed to locked project")
        if recalled_last_args and raw_rest:
            lines.append(f"- args: {raw_rest} (reused)")
        if mode == "recent_docs":
            lines.append("- mode: recent_docs")
            lines.append(f"- docs_per_project: {docs_limit}")
        elif mode == "salvage_docs":
            lines.append("- mode: salvage_docs")
            lines.append(f"- docs_per_project: {max(docs_limit, 5)}")
        elif mode == "bootstrap_docs":
            lines.append("- mode: bootstrap_docs")
            lines.append(f"- docs_per_project: {max(docs_limit, 5)}")
            lines.append(f"- files_per_project: {files_limit}")
        elif mode == "todo_files":
            lines.append("- mode: todo_files")
            lines.append(f"- files_per_project: {files_limit}")
        if since_seconds > 0 and since_label:
            lines.append(f"- since: {since_label}")
        lines.append(f"- projects: {len(targets)}")
        if mode == "scenario":
            lines.append(f"- missing_files: {total['missing']}")
            if since_seconds > 0:
                lines.append(f"- skipped_stale: {total['skipped_stale']}")
        else:
            if mode in {"recent_docs", "salvage_docs", "bootstrap_docs"}:
                lines.append(f"- missing_docs: {total['missing']}")
                lines.append(f"- docs_used: {total.get('docs_used', 0)}")
                lines.append(f"- docs_scanned: {total.get('docs_scanned', 0)}")
                if mode == "bootstrap_docs":
                    lines.append(f"- files_used: {total.get('files_used', 0)}")
                    lines.append(f"- files_scanned: {total.get('files_scanned', 0)}")
            else:
                lines.append(f"- missing_files: {total['missing']}")
                lines.append(f"- files_used: {total.get('files_used', 0)}")
                lines.append(f"- files_scanned: {total.get('files_scanned', 0)}")
        lines.append(f"- parsed: {total['parsed']}")
        lines.append(f"- added: {total['added']}")
        lines.append(f"- updated: {total['updated']}")
        lines.append(f"- done: {total['done']}")
        if total["proposed"]:
            lines.append(f"- proposed: {total['proposed']}")
        if prune_missing:
            lines.append(f"- pruned: {total['pruned']}")
        skipped_done = int(total.get("skipped_done_missing", 0) or 0)
        if skipped_done:
            lines.append(f"- skipped_done_missing: {skipped_done}")
        if per_project_lines:
            lines.append("")
            lines.append("details:")
            lines.extend(per_project_lines[:30])
            if len(per_project_lines) > 30:
                lines.append(f"... ({len(per_project_lines) - 30} more)")
        lines.extend(["", "next:", "- /queue", "- /next", "- /fanout", "- /auto on"])
        send("\n".join(lines).strip(), context="sync", with_menu=True)
        return {"terminal": True}

    if cmd == "queue":
        queue_tokens = [t for t in str(rest or "").split() if t.strip()]
        followup_only = bool(queue_tokens and str(queue_tokens[0]).strip().lower() in {"followup", "fu"})
        fallback_alias = ""
        fallback_key = str(focus_key or "").strip()
        fallback_entry = focus_entry if focus_key and isinstance(focus_entry, dict) else None
        if not isinstance(fallback_entry, dict):
            active_key = str(manager_state.get("active", "")).strip().lower()
            active_entry = projects.get(active_key) if isinstance(projects, dict) else None
            if isinstance(active_entry, dict):
                fallback_entry = active_entry
                fallback_key = active_key
        if isinstance(fallback_entry, dict):
            fallback_alias = _project_alias(fallback_entry, str(fallback_key or manager_state.get("active", "") or ""))
        rows: List[Dict[str, Any]] = []
        iter_projects = (
            [(focus_key, focus_entry)]
            if focus_key and isinstance(focus_entry, dict)
            else list_ops_projects(projects)
        )
        for key, entry in iter_projects:
            alias = _project_alias(entry, str(key))
            display = str(entry.get("display_name", "")).strip() or str(entry.get("name", "")).strip() or str(key)
            if len(display) > 24:
                display = display[:21] + "..."
            raw = entry.get("todos")
            todos: List[Dict[str, Any]] = [r for r in raw if isinstance(r, dict)] if isinstance(raw, list) else []
            counts = _count_todo_statuses(todos)
            open_rows = _sorted_open_todos(todos)
            next_item = open_rows[0] if open_rows else None

            pending = entry.get("pending_todo")
            pending_id = ""
            pending_chat = ""
            if isinstance(pending, dict):
                pending_id = str(pending.get("todo_id", "")).strip()
                pending_chat = str(pending.get("chat_id", "")).strip()

            flags: List[str] = []
            if bool(entry.get("paused", False)):
                flags.append("paused")
            if pending_id:
                flags.append("pending")
            if counts.get(_STATUS_RUNNING, 0) > 0:
                flags.append("running")
            if counts.get(_STATUS_BLOCKED, 0) > 0:
                flags.append("blocked")

            next_summary = ""
            next_id = ""
            next_pr = ""
            if isinstance(next_item, dict):
                next_id = str(next_item.get("id", "")).strip()
                next_pr = _normalize_priority(str(next_item.get("priority", "P2")))
                next_summary = str(next_item.get("summary", "")).strip().replace("\n", " ")
                if len(next_summary) > 64:
                    next_summary = next_summary[:61] + "..."
            manual_followup_count = _blocked_bucket_count(todos, "manual_followup")
            followup_indices = _manual_followup_indices(todos, limit=1)
            if followup_only and manual_followup_count <= 0:
                continue

            rows.append(
                {
                    "alias": alias,
                    "key": str(key),
                    "display": display,
                    "open": int(counts.get(_STATUS_OPEN, 0) or 0),
                    "running": int(counts.get(_STATUS_RUNNING, 0) or 0),
                    "blocked": int(counts.get(_STATUS_BLOCKED, 0) or 0),
                    "done": int(counts.get(_STATUS_DONE, 0) or 0),
                    "canceled": int(counts.get(_STATUS_CANCELED, 0) or 0),
                    "flags": ",".join(flags),
                    "pending_id": pending_id,
                    "pending_self": "yes" if (pending_id and pending_chat == str(chat_id)) else "no",
                    "next_id": next_id,
                    "next_pr": next_pr,
                    "next_summary": next_summary,
                    "blocked_head": _blocked_head_summary(todos),
                    "manual_followup_count": manual_followup_count,
                    "manual_followup_ack_ref": str(followup_indices[0]) if followup_indices else "",
                }
            )

        if not rows:
            msg = "manual follow-up queue: empty." if followup_only else "queue: no projects found."
            send(
                msg,
                context="queue-followup-none" if followup_only else "queue-none",
                with_menu=True,
                reply_markup=_queue_reply_markup(
                    rows,
                    followup_only=followup_only,
                    focus_key=str(focus_key or ""),
                    fallback_alias=fallback_alias,
                ),
            )
            return {"terminal": True}

        rows.sort(key=lambda r: (_alias_index(r.get("alias", "")), str(r.get("alias", "")), str(r.get("key", ""))))

        limit = 12
        lines: List[str] = []
        if followup_only:
            lines.append("manual follow-up queue")
        else:
            lines.append("project todo queue" if focus_key else "global todo queue")
        if focus_key:
            lines.append(f"- project_lock: {focus_alias or focus_key}")
        lines.append(f"- projects: {len(rows)}")
        lines.append("")
        for row in rows[:limit]:
            alias = row.get("alias", "-")
            display = row.get("display", "")
            counts_part = "open={o} running={r} blocked={b} done={d}".format(
                o=row.get("open", 0),
                r=row.get("running", 0),
                b=row.get("blocked", 0),
                d=row.get("done", 0),
            )
            manual_followup_count = int(row.get("manual_followup_count", 0) or 0)
            if manual_followup_count > 0:
                counts_part += f" followup={manual_followup_count}"
            flag = str(row.get("flags", "")).strip()
            if flag:
                counts_part = counts_part + f" ({flag})"
            lines.append(f"- {alias} {display}: {counts_part}")
            if str(row.get("pending_id", "")).strip():
                suffix = " (yours)" if str(row.get("pending_self", "no")) == "yes" else ""
                lines.append(f"  pending: {row.get('pending_id')}{suffix}")
            blocked_head = row.get("blocked_head") if isinstance(row.get("blocked_head"), dict) else {}
            if blocked_head:
                blocked_line = f"  blocked_head: {blocked_head.get('id', '-')} x{blocked_head.get('count', 1)}"
                bucket = str(blocked_head.get("bucket", "")).strip()
                if bucket:
                    blocked_line += f" [{bucket}]"
                reason = str(blocked_head.get("reason", "")).strip()
                if reason:
                    blocked_line += f" | {reason}"
                lines.append(blocked_line)
            if str(row.get("next_id", "")).strip():
                next_line = f"  next: {row.get('next_pr','P2')} {row.get('next_id')} | {row.get('next_summary') or '-'}"
                lines.append(next_line)
        if len(rows) > limit:
            lines.append("")
            lines.append(f"... and {len(rows) - limit} more")

        lines.append("")
        lines.append("next:")
        if followup_only:
            lines.append("- /todo followup   (current project follow-up backlog)")
        lines.append("- /next   (pick and run next todo)")
        lines.append("- /todo   (view/edit current project)")
        lines.append("- /use O# (switch active project)")
        if focus_key:
            lines.append("- /focus off (unlock global scheduling)")
        send(
            "\n".join(lines).strip(),
            context="queue",
            with_menu=True,
            reply_markup=_queue_reply_markup(
                rows,
                followup_only=followup_only,
                focus_key=str(focus_key or ""),
                fallback_alias=fallback_alias,
            ),
        )
        return {"terminal": True}

    if chat_role == "readonly":
        send(
            "permission denied: readonly chat cannot start scheduling.\n"
            "read-only: /status /monitor /todo (list) ...",
            context="next-deny",
            with_menu=True,
        )
        return {"terminal": True}

    tokens = [t for t in str(rest or "").split() if t.strip()]
    force = any(t.lower() in {"force", "!", "--force"} for t in tokens)

    # 0) If a pending todo exists for this chat, try to resume it instead of selecting a new one.
    candidate_projects = ({focus_key: focus_entry} if focus_key and isinstance(focus_entry, dict) else projects)
    unready_rows: List[str] = []
    for p_key, p_entry in candidate_projects.items():
        if not isinstance(p_entry, dict):
            continue
        if not project_schedulable(p_entry):
            continue
        if not project_runtime_issue(p_entry):
            continue
        alias = _project_alias(p_entry, str(p_key))
        unready_rows.append(f"- {alias} ({p_key}): {project_runtime_label(p_entry)}")
    pending_hit = _find_pending_todo_for_chat(candidate_projects, chat_id, skip_paused=not force)
    if pending_hit and not force:
        p_key, pending = pending_hit
        p_todo_id = str(pending.get("todo_id", "")).strip()
        try:
            key, entry, _p_args = get_context(p_key)
        except Exception:
            key = p_key
            entry = projects.get(p_key) if isinstance(projects.get(p_key), dict) else {}

        alias = _project_alias(entry if isinstance(entry, dict) else {}, key)
        if isinstance(entry, dict) and _has_task_linked_to_todo(entry, p_todo_id):
            send(
                "next blocked: pending todo already has an active task\n"
                f"- orch: {key} ({alias})\n"
                f"- pending: {p_todo_id}\n"
                "next:\n"
                "- /todo (list)\n"
                "- /monitor",
                context="next-pending-has-task",
                with_menu=True,
            )
            return {"terminal": True}

        todo_item = _find_todo_item(entry if isinstance(entry, dict) else {}, p_todo_id) if isinstance(entry, dict) else None
        summary = str((todo_item or {}).get("summary", "")).strip() if isinstance(todo_item, dict) else ""
        if not summary:
            send(
                "next blocked: pending todo exists but summary was not found\n"
                f"- orch: {key} ({alias})\n"
                f"- pending: {p_todo_id}\n"
                "next:\n"
                f"- /todo {key}\n"
                "- /next force  (override)",
                context="next-pending-missing",
                with_menu=True,
            )
            return {"terminal": True}

        summary_preview = summary.replace("\n", " ").strip()
        if len(summary_preview) > 220:
            summary_preview = summary_preview[:217] + "..."
        send(
            "next resumed: pending todo\n"
            f"- orch: {key} ({alias})\n"
            f"- id: {p_todo_id}\n"
            f"- summary: {summary_preview or '-'}\n"
            "dispatch starting...",
            context="next-resume",
            with_menu=True,
        )
        return {
            "terminal": False,
            "cmd": "run",
            "orch_target": key,
            "run_prompt": summary,
            "run_force_mode": "dispatch",
            "run_auto_source": "todo-next-global",
        }

    # 1) Pick a candidate across all projects.
    candidate = _pick_global_next_candidate(candidate_projects, ignore_busy=force, skip_paused=not force)
    if not candidate:
        body = build_no_runnable_todo_message(
            focus_label=focus_alias or focus_key,
            unready_rows=unready_rows,
        )
        send(body, context="next-none", with_menu=True)
        return {"terminal": True}

    key = str(candidate.get("project_key", "")).strip()
    todo = candidate.get("todo") if isinstance(candidate.get("todo"), dict) else {}
    selection_kind = str(candidate.get("selection_kind", "")).strip().lower()

    # Resolve entry via get_context to ensure target exists and uses normalized key.
    key, entry, _p_args = get_context(key)
    alias = _project_alias(entry, key)

    todo_id = str(todo.get("id", "")).strip() or "-"
    pr = _normalize_priority(str(todo.get("priority", "P2")))
    summary = str(todo.get("summary", "")).strip()
    if not summary:
        send(
            "next blocked: selected todo has empty summary\n"
            f"- orch: {key} ({alias})\n"
            f"- id: {todo_id}\n"
            "next:\n"
            f"- /todo {key}\n"
            "- /next force",
            context="next-empty-summary",
            with_menu=True,
        )
        return {"terminal": True}

    now = now_iso()
    todo["updated_at"] = now
    todo["queued_at"] = str(todo.get("queued_at", "")).strip() or now
    todo["queued_by"] = str(todo.get("queued_by", "")).strip() or f"telegram:{chat_id}"
    entry["pending_todo"] = {"todo_id": todo_id, "chat_id": str(chat_id), "selected_at": now}
    entry["updated_at"] = now
    if not args.dry_run:
        save_manager_state(args.manager_state_file, manager_state)

    summary_preview = summary.replace("\n", " ").strip()
    if len(summary_preview) > 220:
        summary_preview = summary_preview[:217] + "..."
    lines = [
        "next resumed (global)" if selection_kind == "resume" else "next selected (global)",
        f"- orch: {key} ({alias})",
        f"- id: {todo_id}",
        f"- priority: {pr}",
        f"- summary: {summary_preview or '-'}",
    ]
    blocked_head = _blocked_head_summary(entry.get("todos") if isinstance(entry.get("todos"), list) else [])
    if blocked_head and str(blocked_head.get("bucket", "")).strip() == "manual_followup":
        attention = f"- attention: blocked backlog {blocked_head.get('id', '-')} x{blocked_head.get('count', 1)} [manual_followup]"
        reason = str(blocked_head.get("reason", "")).strip()
        if reason:
            attention += f" | {reason}"
        lines.append(attention)
    lines.append("dispatch starting...")
    send("\n".join(lines), context="next-selected", with_menu=True)

    return {
        "terminal": False,
        "cmd": "run",
        "orch_target": key,
        "run_prompt": summary,
        "run_force_mode": "dispatch",
        "run_auto_source": "todo-next-global",
    }
