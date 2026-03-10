#!/usr/bin/env python3
"""Todo queue handlers for Telegram gateway.

Todo is project-level backlog (not TF lifecycle). Keep it simple:
- /todo                : list open items
- /todo add <summary>  : add new item (optional priority prefix P1/P2/P3)
- /todo done <id|num>  : mark item done
- /todo next           : pick next open item and run it as a TF (dispatch)

Optional orch override:
- /todo O2
- /todo O2 add ...
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Tuple

from aoe_tg_blocked_state import (
    blocked_bucket_label as _blocked_bucket_label,
    blocked_reason_preview as _blocked_reason_preview,
    clear_blocked_meta as _clear_blocked_meta,
    manual_followup_indices as _manual_followup_indices,
)
from aoe_tg_ops_policy import project_queue_snapshot
from aoe_tg_project_runtime import project_runtime_issue, project_runtime_label
from aoe_tg_todo_state import (
    accept_todo_proposal as _accept_todo_proposal,
    apply_syncback_plan as _apply_syncback_plan,
    ensure_todo_proposal_store as _ensure_todo_proposal_store,
    ensure_todo_store as _ensure_todo_store,
    find_proposal_by_ref as _find_proposal_by_ref,
    format_todo_id as _format_todo_id,
    merge_todo_proposals,
    normalize_priority as _normalize_priority,
    normalize_proposal_kind as _normalize_proposal_kind,
    normalize_proposal_priority as _normalize_proposal_priority,
    normalize_proposal_status as _normalize_proposal_status,
    preview_syncback_plan as _preview_syncback_plan,
    priority_rank as _priority_rank,
    proposal_confidence as _proposal_confidence,
    proposal_summary_key as _proposal_summary_key,
    reject_todo_proposal as _reject_todo_proposal,
    sorted_open_proposals as _sorted_open_proposals,
)

_PRIORITIES = {"P1", "P2", "P3"}
_STATUS_OPEN = "open"
_STATUS_RUNNING = "running"
_STATUS_BLOCKED = "blocked"
_STATUS_DONE = "done"
_STATUS_CANCELED = "canceled"
_PROPOSAL_STATUS_OPEN = "open"
_PROPOSAL_STATUS_ACCEPTED = "accepted"
_PROPOSAL_STATUS_REJECTED = "rejected"


def _project_alias(entry: Dict[str, Any], fallback: str) -> str:
    token = str(entry.get("project_alias", "")).strip().upper()
    return token or str(fallback or "").strip() or "-"


def _todo_reply_markup(key: str, entry: Dict[str, Any], active_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    alias = _project_alias(entry, key)
    proposal_rows, _proposal_seq = _ensure_todo_proposal_store(entry)
    open_proposals = _sorted_open_proposals(proposal_rows)
    keyboard: List[List[Dict[str, str]]] = [
        [{"text": "/todo next"}, {"text": "/todo followup"}, {"text": f"/orch status {alias}"}],
        [{"text": f"/sync preview {alias} 1h"}, {"text": "/todo syncback preview"}],
    ]
    if open_proposals:
        keyboard.append([{"text": "/todo proposals"}])
    ackrun_row: List[Dict[str, str]] = []
    for idx in _manual_followup_indices(active_rows, limit=3):
        ackrun_row.append({"text": f"/todo ackrun {idx}"})
    if ackrun_row:
        keyboard.append(ackrun_row)
    ack_row: List[Dict[str, str]] = []
    for idx in _manual_followup_indices(active_rows, limit=3):
        ack_row.append({"text": f"/todo ack {idx}"})
    if ack_row:
        keyboard.append(ack_row)
    done_row: List[Dict[str, str]] = []
    for idx, _row in enumerate(active_rows[:3], start=1):
        done_row.append({"text": f"/todo done {idx}"})
    if done_row:
        keyboard.append(done_row)
    keyboard.append([{"text": f"/sync {alias} 1h"}, {"text": "/queue"}, {"text": "/next"}])
    keyboard.append([{"text": "/map"}, {"text": "/help"}])
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": f"예: /todo ackrun 1 또는 /todo next ({alias})",
    }


def _todo_empty_reply_markup(key: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    alias = _project_alias(entry, key)
    proposal_rows, _proposal_seq = _ensure_todo_proposal_store(entry)
    open_proposals = _sorted_open_proposals(proposal_rows)
    keyboard: List[List[Dict[str, str]]] = [
        [{"text": f"/orch status {alias}"}, {"text": f"/sync preview {alias} 1h"}],
        [{"text": "/todo syncback preview"}, {"text": f"/sync {alias} 1h"}, {"text": "/map"}, {"text": "/help"}],
    ]
    if open_proposals:
        keyboard.insert(1, [{"text": "/todo proposals"}])
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": f"예: /sync preview {alias} 1h",
    }


def _todo_pending_reply_markup(alias: str, *, include_force: str = "") -> Dict[str, Any]:
    force_cmd = str(include_force or "").strip()
    keyboard: List[List[Dict[str, str]]] = [
        [{"text": "/ok"}, {"text": "/clear pending"}, {"text": f"/todo {alias}"}],
        [{"text": f"/orch status {alias}"}, {"text": "/monitor"}, {"text": "/help"}],
    ]
    if force_cmd:
        keyboard.insert(1, [{"text": force_cmd}])
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "예: /ok 또는 /clear pending",
    }


def _parse_orch_override(tokens: List[str]) -> Tuple[Optional[str], List[str]]:
    if not tokens:
        return None, tokens
    head = tokens[0].strip()
    if re.fullmatch(r"O[1-9]\d{0,2}", head.upper()):
        return head, tokens[1:]
    return None, tokens


def _parse_add_payload(raw: str) -> Tuple[str, str]:
    text = str(raw or "").strip()
    if not text:
        return ("P2", "")
    m = re.match(r"^(P[1-3])(?:\s+|:)\s*(.+)$", text, flags=re.IGNORECASE)
    if m:
        return (_normalize_priority(m.group(1)), m.group(2).strip())
    parts = text.split(maxsplit=1)
    if parts and _normalize_priority(parts[0]) in _PRIORITIES:
        pr = _normalize_priority(parts[0])
        summary = parts[1].strip() if len(parts) > 1 else ""
        return (pr, summary)
    return ("P2", text)


def _sorted_open_todos(todos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    open_rows = []
    for row in todos:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
        if status != _STATUS_OPEN:
            continue
        open_rows.append(row)
    open_rows.sort(
        key=lambda r: (
            _priority_rank(str(r.get("priority", "P2"))),
            str(r.get("created_at", "")),
            str(r.get("id", "")),
        )
    )
    return open_rows


def _status_rank(status: str) -> int:
    token = str(status or "").strip().lower()
    if token == _STATUS_RUNNING:
        return 0
    if token == _STATUS_BLOCKED:
        return 1
    if token == _STATUS_OPEN:
        return 2
    if token in {_STATUS_DONE, _STATUS_CANCELED}:
        return 9
    return 8


def _sorted_active_todos(todos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = []
    for row in todos:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
        if status in {_STATUS_DONE, _STATUS_CANCELED}:
            continue
        rows.append(row)
    rows.sort(
        key=lambda r: (
            _status_rank(str(r.get("status", _STATUS_OPEN))),
            _priority_rank(str(r.get("priority", "P2"))),
            str(r.get("created_at", "")),
            str(r.get("id", "")),
        )
    )
    return rows


def _blocked_meta_suffix(row: Dict[str, Any]) -> str:
    status = str(row.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
    if status != _STATUS_BLOCKED:
        return ""
    try:
        blocked_count = max(1, int(row.get("blocked_count", 0) or 0))
    except Exception:
        blocked_count = 1
    suffix = f" blocked x{blocked_count}"
    bucket = _blocked_bucket_label(row.get("blocked_bucket", ""))
    if bucket:
        suffix += f" [{bucket}]"
    reason = _blocked_reason_preview(row.get("blocked_reason", ""))
    if reason:
        suffix += f" | {reason}"
    return suffix


def _proposal_kind_label(token: Any) -> str:
    return _normalize_proposal_kind(token)


def _proposal_confidence_label(token: Any) -> str:
    value = _proposal_confidence(token)
    return f"{int(round(value * 100.0))}%"


def _find_todo_by_ref(todos: List[Dict[str, Any]], ref: str) -> Optional[Dict[str, Any]]:
    token = str(ref or "").strip()
    if not token:
        return None

    # 1) direct ID match
    upper = token.upper()
    for row in todos:
        if not isinstance(row, dict):
            continue
        if str(row.get("id", "")).strip().upper() == upper:
            return row

    # 2) numeric ref -> open list index or seq-derived ID
    if token.isdigit():
        idx = int(token)
        active_rows = _sorted_active_todos(todos)
        if 1 <= idx <= len(active_rows):
            return active_rows[idx - 1]

        cand = _format_todo_id(idx)
        for row in todos:
            if not isinstance(row, dict):
                continue
            if str(row.get("id", "")).strip().upper() == cand:
                return row

    return None


def _todo_usage() -> str:
    return (
        "todo queue\n"
        "- /todo\n"
        "- /todo proposals  (show TF follow-up proposals inbox)\n"
        "- /todo followup  (manual follow-up backlog only)\n"
        "- /todo syncback [preview]  (write runtime done/notes/proposals back to canonical TODO.md)\n"
        "- /todo add [P1|P2|P3] <summary>\n"
        "- /todo accept <PROP-xxx|number>  (promote proposal into main todo queue)\n"
        "- /todo reject <PROP-xxx|number> [reason]  (discard proposal)\n"
        "- /todo ack <TODO-xxx|number>  (reopen blocked todo)\n"
        "- /todo ackrun <TODO-xxx|number>  (reopen blocked todo and dispatch it now)\n"
        "- /todo done <TODO-xxx|number>\n"
        "- /todo next  (run next open todo)\n"
        "- (optional) /todo O2 ...  # orch override\n"
    )


def _queue_todo_for_dispatch(
    *,
    item: Dict[str, Any],
    entry: Dict[str, Any],
    key: str,
    chat_id: str,
    args: Any,
    manager_state: Dict[str, Any],
    save_manager_state: Callable[..., None],
    now_iso: Callable[[], str],
) -> Dict[str, Any]:
    todo_id = str(item.get("id", "")).strip() or "-"
    summary = str(item.get("summary", "")).strip()
    now = now_iso()
    item["status"] = _STATUS_OPEN
    item["updated_at"] = now
    item["queued_at"] = str(item.get("queued_at", "")).strip() or now
    item["queued_by"] = str(item.get("queued_by", "")).strip() or f"telegram:{chat_id}"
    entry["pending_todo"] = {"todo_id": todo_id, "chat_id": str(chat_id), "selected_at": now}
    entry["updated_at"] = now
    if not args.dry_run:
        save_manager_state(args.manager_state_file, manager_state)
    return {
        "todo_id": todo_id,
        "priority": _normalize_priority(str(item.get("priority", "P2"))),
        "summary": summary,
        "summary_preview": (summary.replace("\n", " ").strip()[:217] + "...") if len(summary.replace("\n", " ").strip()) > 220 else summary.replace("\n", " ").strip(),
    }


def _dispatch_force_requested(tokens: List[str], start_index: int = 1) -> bool:
    return any(str(t or "").strip().lower() in {"force", "!", "--force"} for t in tokens[start_index:])




def handle_todo_command(
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
    if cmd != "todo":
        return None

    tokens = [t for t in str(rest or "").split() if t.strip()]
    orch_override, tokens = _parse_orch_override(tokens)
    target = orch_override or orch_target
    try:
        key, entry, _p_args = get_context(target)
    except RuntimeError as exc:
        text = str(exc).strip()
        if "project lock active" in text:
            send(
                "todo blocked by project lock\n"
                f"- detail: {text}\n"
                "next:\n"
                "- /map\n"
                "- /focus off\n"
                "- /todo   (use locked project)",
                context="todo-lock blocked",
                with_menu=True,
            )
            return {"terminal": True}
        raise

    todos, seq = _ensure_todo_store(entry)
    proposals, _proposal_seq = _ensure_todo_proposal_store(entry)
    if tokens and tokens[0].lower() in {"help", "h", "?"}:
        send(_todo_usage(), context="todo-help", with_menu=True)
        return {"terminal": True}

    sub = tokens[0].lower() if tokens else "list"
    if chat_role == "readonly" and sub not in {"list", "ls", "show", "help", "h", "?"}:
        send(
            "permission denied: readonly chat cannot modify todo.\n"
            "read-only: /todo (list only)",
            context="todo-deny",
            with_menu=True,
        )
        return {"terminal": True}

    if sub in {"list", "ls", "show", "followup", "fu"} or not tokens:
        active_rows = _sorted_active_todos(todos)
        manual_followup_ids = {
            str(row.get("id", "")).strip()
            for row in active_rows
            if str(row.get("status", _STATUS_OPEN)).strip().lower() == _STATUS_BLOCKED
            and _blocked_bucket_label(row.get("blocked_bucket", "")) == "manual_followup"
        }
        followup_only = sub in {"followup", "fu"}
        done_cnt = 0
        canceled_cnt = 0
        for row in todos:
            if not isinstance(row, dict):
                continue
            st = str(row.get("status", "")).strip().lower()
            if st == _STATUS_DONE:
                done_cnt += 1
            elif st == _STATUS_CANCELED:
                canceled_cnt += 1

        lines = [
            f"orch: {key}",
            (
                f"todo followup: count={len(manual_followup_ids)} active={len(active_rows)} done={done_cnt} canceled={canceled_cnt}"
                if followup_only
                else f"todo: active={len(active_rows)} followup={len(manual_followup_ids)} done={done_cnt} canceled={canceled_cnt}"
            ),
        ]
        if not active_rows:
            lines.append("")
            lines.append("(empty) add with: /todo add <summary>")
            send(
                "\n".join(lines),
                context="todo-list empty",
                with_menu=False,
                reply_markup=_todo_empty_reply_markup(key, entry),
            )
            return {"terminal": True}
        if followup_only and not manual_followup_ids:
            lines.append("")
            lines.append("(empty) no manual_followup backlog")
            send(
                "\n".join(lines),
                context="todo-followup empty",
                with_menu=False,
                reply_markup=_todo_reply_markup(key, entry, active_rows),
            )
            return {"terminal": True}

        visible_rows = active_rows[:20]

        def _append_row(idx: int, row: Dict[str, Any]) -> None:
            todo_id = str(row.get("id", "")).strip() or "-"
            status = str(row.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
            pr = _normalize_priority(str(row.get("priority", "P2")))
            summary = str(row.get("summary", "")).strip().replace("\n", " ")
            if len(summary) > 120:
                summary = summary[:117] + "..."
            extra = ""
            req_id = str(row.get("current_request_id", "")).strip()
            if req_id:
                extra = f" task={req_id}"
            blocked_suffix = _blocked_meta_suffix(row)
            lines.append(f"- {idx}. [{status}] {todo_id} | {pr} | {summary or '-'}{extra}{blocked_suffix}")

        if manual_followup_ids:
            lines.append("")
            lines.append("manual_followup:")
            for idx, row in enumerate(visible_rows, start=1):
                if str(row.get("id", "")).strip() not in manual_followup_ids:
                    continue
                _append_row(idx, row)
            if followup_only:
                lines.append("")
                lines.append("tip: /todo ackrun <번호|TODO-xxx> 로 확인 후 바로 재실행하거나, /todo ack <번호|TODO-xxx> 로 open 상태만 복구할 수 있습니다.")

        remaining_rows = [row for row in visible_rows if str(row.get("id", "")).strip() not in manual_followup_ids]
        if (not followup_only) and remaining_rows:
            lines.append("")
            lines.append("active:")
            for idx, row in enumerate(visible_rows, start=1):
                if str(row.get("id", "")).strip() in manual_followup_ids:
                    continue
                _append_row(idx, row)
        lines.append("")
        lines.append("quick: /todo next | /todo followup | /todo ackrun <번호|TODO-xxx> | /todo ack <번호|TODO-xxx> | /todo done <번호|TODO-xxx>")
        send(
            "\n".join(lines),
            context="todo-followup" if followup_only else "todo-list",
            with_menu=False,
            reply_markup=_todo_reply_markup(key, entry, active_rows),
        )
        return {"terminal": True}

    if sub in {"proposals", "proposal", "inbox"}:
        open_rows = _sorted_open_proposals(proposals)
        accepted_cnt = sum(1 for row in proposals if isinstance(row, dict) and _normalize_proposal_status(row.get("status")) == _PROPOSAL_STATUS_ACCEPTED)
        rejected_cnt = sum(1 for row in proposals if isinstance(row, dict) and _normalize_proposal_status(row.get("status")) == _PROPOSAL_STATUS_REJECTED)
        lines = [
            f"orch: {key}",
            f"todo proposals: open={len(open_rows)} accepted={accepted_cnt} rejected={rejected_cnt}",
        ]
        if not open_rows:
            lines.append("")
            lines.append("(empty) no pending TF follow-up proposals")
            send("\n".join(lines), context="todo-proposals empty", with_menu=False, reply_markup=_todo_empty_reply_markup(key, entry))
            return {"terminal": True}

        alias = _project_alias(entry, key)
        for idx, row in enumerate(open_rows[:20], start=1):
            pid = str(row.get("id", "")).strip() or "-"
            pr = _normalize_proposal_priority(row.get("priority", "P2"))
            kind = _proposal_kind_label(row.get("kind", "followup"))
            conf = _proposal_confidence_label(row.get("confidence", 0.0))
            summary = str(row.get("summary", "")).strip().replace("\n", " ")
            if len(summary) > 120:
                summary = summary[:117] + "..."
            reason = _blocked_reason_preview(row.get("reason", ""), 96)
            source_req = str(row.get("source_request_id", "")).strip() or "-"
            source_todo = str(row.get("source_todo_id", "")).strip()
            src_suffix = f" | src={source_req}"
            if source_todo:
                src_suffix += f" todo={source_todo}"
            line = f"- {idx}. [{kind}] {pid} | {pr} | conf={conf} | {summary or '-'}{src_suffix}"
            if reason:
                line += f" | reason={reason}"
            lines.append(line)
        lines.append("")
        lines.append("quick: /todo accept <번호|PROP-xxx> | /todo reject <번호|PROP-xxx> [reason] | /todo")
        reply_markup = {
            "keyboard": [
                [{"text": "/todo proposals"}, {"text": f"/orch status {alias}"}, {"text": f"/todo {alias}"}],
                [{"text": "/map"}, {"text": "/help"}],
            ],
            "resize_keyboard": True,
            "one_time_keyboard": False,
            "input_field_placeholder": f"예: /todo accept 1 ({alias})",
        }
        accept_row: List[Dict[str, str]] = []
        reject_row: List[Dict[str, str]] = []
        for idx, _row in enumerate(open_rows[:3], start=1):
            accept_row.append({"text": f"/todo accept {idx}"})
            reject_row.append({"text": f"/todo reject {idx}"})
        if accept_row:
            reply_markup["keyboard"].insert(1, accept_row)
        if reject_row:
            reply_markup["keyboard"].insert(2, reject_row)
        send("\n".join(lines), context="todo-proposals", with_menu=False, reply_markup=reply_markup)
        return {"terminal": True}

    if sub in {"syncback", "writeback", "export"}:
        preview = any(str(tok or "").strip().lower() in {"preview", "dry", "dry-run", "--dry-run", "--preview"} for tok in tokens[1:])
        try:
            plan = _preview_syncback_plan(entry)
        except RuntimeError as exc:
            send(str(exc).strip(), context="todo-syncback missing", with_menu=True)
            return {"terminal": True}

        alias = _project_alias(entry, key)
        lines = [
            f"todo syncback{' preview' if preview else ''}",
            f"- orch: {key} ({alias})",
            f"- target: {plan['path']}",
            f"- mark_done: {int(plan.get('done_count', 0) or 0)}",
            f"- reopen_open: {int(plan.get('reopen_count', 0) or 0)}",
            f"- append_new: {int(plan.get('append_count', 0) or 0)}",
            f"- blocked_notes: {int(plan.get('blocked_count', 0) or 0)}",
        ]
        updates = list(plan.get("updates") or [])
        append_lines = list(plan.get("append_lines") or [])
        if updates:
            lines.append("updates:")
            for idx, new_line in updates[:4]:
                lines.append(f"- L{int(idx) + 1}: {str(new_line)[:180]}")
            if len(updates) > 4:
                lines.append(f"- ... {len(updates) - 4} more")
        if append_lines:
            lines.append("appends:")
            for line in append_lines[:4]:
                lines.append(f"- {str(line)[:180]}")
            if len(append_lines) > 4:
                lines.append(f"- ... {len(append_lines) - 4} more")
        if preview:
            lines.extend(
                [
                    "next:",
                    f"- /todo {alias} syncback",
                    f"- /todo {alias}",
                    f"- /orch status {alias}",
                ]
            )
            send("\n".join(lines), context="todo-syncback preview", with_menu=True)
            return {"terminal": True}

        result = _apply_syncback_plan(plan)
        lines.append(f"- applied: yes ({result['line_count']} lines)")
        lines.extend(
            [
                "next:",
                f"- /todo {alias}",
                f"- /sync preview {alias} 24h",
            ]
        )
        send("\n".join(lines), context="todo-syncback", with_menu=True)
        return {"terminal": True}

    if sub in {"next", "run", "start"}:
        force = _dispatch_force_requested(tokens, 1)
        issue = project_runtime_issue(entry)
        if issue:
            alias = _project_alias(entry, key)
            send(
                "todo next blocked: project runtime is not ready\n"
                f"- orch: {key} ({alias})\n"
                f"- reason: {project_runtime_label(entry)}\n"
                "next:\n"
                f"- /orch status {alias}\n"
                "- fix runtime files, then retry /todo next",
                context="todo-next unready",
                with_menu=True,
            )
            return {"terminal": True}
        pending = entry.get("pending_todo")
        if (
            (not force)
            and isinstance(pending, dict)
            and str(pending.get("todo_id", "")).strip()
            and str(pending.get("chat_id", "")).strip() == str(chat_id)
        ):
            todo_id = str(pending.get("todo_id", "")).strip()
            alias = _project_alias(entry, key)
            send(
                "todo next blocked: pending todo exists (awaiting dispatch/approval)\n"
                f"- orch: {key}\n"
                f"- pending: {todo_id}\n"
                "next:\n"
                "- /todo (list)\n"
                "- /ok (if confirm is pending)\n"
                "- /todo next force  (override)",
                context="todo-next pending",
                with_menu=True,
                reply_markup=_todo_pending_reply_markup(alias, include_force="/todo next force"),
            )
            return {"terminal": True}
        queue_snap = project_queue_snapshot(entry)
        if queue_snap["has_running"] and not force:
            active_rows = _sorted_active_todos(todos)
            busy = [r for r in active_rows if str(r.get("status", "")).strip().lower() == _STATUS_RUNNING]
            head = busy[0] if busy else {}
            todo_id = str(head.get("id", "")).strip() or "-"
            pr = _normalize_priority(str(head.get("priority", "P2")))
            summary = str(head.get("summary", "")).strip().replace("\n", " ")
            if len(summary) > 200:
                summary = summary[:197] + "..."
            send(
                "todo next blocked: active todo exists\n"
                f"- orch: {key}\n"
                f"- current: {todo_id} | {pr} | {summary or '-'}\n"
                "next:\n"
                "- /todo (list)\n"
                f"- /todo done {todo_id}\n"
                "- /todo next force  (override)",
                context="todo-next busy",
                with_menu=True,
            )
            return {"terminal": True}

        open_rows = _sorted_open_todos(todos)
        if not open_rows:
            send(
                f"orch: {key}\n"
                "no open todo.\n"
                "add: /todo add <summary>",
                context="todo-next empty",
                with_menu=True,
            )
            return {"terminal": True}

        item = open_rows[0]
        todo_id = str(item.get("id", "")).strip() or "-"
        pr = _normalize_priority(str(item.get("priority", "P2")))
        summary = str(item.get("summary", "")).strip()
        queued = _queue_todo_for_dispatch(
            item=item,
            entry=entry,
            key=key,
            chat_id=chat_id,
            args=args,
            manager_state=manager_state,
            save_manager_state=save_manager_state,
            now_iso=now_iso,
        )
        send(
            "todo next selected\n"
            f"- orch: {key}\n"
            f"- id: {queued['todo_id']}\n"
            f"- priority: {queued['priority']}\n"
            f"- summary: {queued['summary_preview'] or '-'}\n"
            "dispatch starting...",
            context="todo-next selected",
            with_menu=True,
        )

        return {
            "terminal": False,
            "cmd": "run",
            "orch_target": key,
            "run_prompt": summary,
            "run_force_mode": "dispatch",
            "run_auto_source": "todo-next",
        }

    if sub in {"add", "new", "+"}:
        payload = str(rest or "")
        # remove orch override token if present
        if orch_override:
            payload = payload.split(None, 1)[1] if len(payload.split(None, 1)) > 1 else ""
        payload = payload.strip()
        # remove subcmd
        payload = payload.split(None, 1)[1] if len(payload.split(None, 1)) > 1 else ""
        pr, summary = _parse_add_payload(payload)
        if not summary:
            send("usage: /todo add [P1|P2|P3] <summary>", context="todo-add usage", with_menu=True)
            return {"terminal": True}

        seq = max(0, int(entry.get("todo_seq", seq) or 0))
        seq += 1
        todo_id = _format_todo_id(seq)
        entry["todo_seq"] = seq

        now = now_iso()
        todos.append(
            {
                "id": todo_id,
                "summary": summary[:600],
                "priority": _normalize_priority(pr),
                "status": _STATUS_OPEN,
                "created_at": now,
                "updated_at": now,
                "created_by": f"telegram:{chat_id}",
            }
        )
        entry["updated_at"] = now
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)

        send(
            "todo added\n"
            f"- orch: {key}\n"
            f"- id: {todo_id}\n"
            f"- priority: {_normalize_priority(pr)}\n"
            f"- summary: {summary[:200]}",
            context="todo-add",
            with_menu=True,
        )
        return {"terminal": True}

    if sub in {"ack", "reopen", "resume"}:
        if len(tokens) < 2:
            send("usage: /todo ack <TODO-xxx|number>", context="todo-ack usage", with_menu=True)
            return {"terminal": True}
        ref = tokens[1].strip()
        item = _find_todo_by_ref(todos, ref)
        if item is None:
            send(f"todo not found: {ref}\n\n{_todo_usage()}", context="todo-ack missing", with_menu=True)
            return {"terminal": True}

        status = str(item.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
        if status != _STATUS_BLOCKED:
            todo_id = str(item.get("id", "")).strip() or ref
            send(
                "todo ack blocked: target is not blocked\n"
                f"- orch: {key}\n"
                f"- id: {todo_id}\n"
                "next:\n"
                "- /todo\n"
                "- /todo followup",
                context="todo-ack not-blocked",
                with_menu=True,
            )
            return {"terminal": True}

        now = now_iso()
        todo_id = str(item.get("id", "")).strip()
        had_followup = _clear_blocked_meta(item, clear_current_request=True)
        item["status"] = _STATUS_OPEN
        item["updated_at"] = now
        entry["updated_at"] = now
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)

        summary = str(item.get("summary", "")).strip()
        if len(summary) > 200:
            summary = summary[:197] + "..."
        send(
            "todo acknowledged\n"
            f"- orch: {key}\n"
            f"- id: {todo_id or '-'}\n"
            f"- reopened: yes\n"
            f"- cleared_followup: {'yes' if had_followup else 'no'}\n"
            f"- summary: {summary or '-'}\n"
            "next:\n"
            "- /todo\n"
            "- /todo next",
            context="todo-ack",
            with_menu=True,
        )
        return {"terminal": True}

    if sub in {"ackrun", "rerun", "resume-run"}:
        if len(tokens) < 2:
            send("usage: /todo ackrun <TODO-xxx|number> [force]", context="todo-ackrun usage", with_menu=True)
            return {"terminal": True}
        ref = tokens[1].strip()
        force = _dispatch_force_requested(tokens, 2)
        item = _find_todo_by_ref(todos, ref)
        if item is None:
            send(f"todo not found: {ref}\n\n{_todo_usage()}", context="todo-ackrun missing", with_menu=True)
            return {"terminal": True}

        status = str(item.get("status", _STATUS_OPEN)).strip().lower() or _STATUS_OPEN
        todo_id = str(item.get("id", "")).strip() or ref
        if status != _STATUS_BLOCKED:
            send(
                "todo ackrun blocked: target is not blocked\n"
                f"- orch: {key}\n"
                f"- id: {todo_id}\n"
                "next:\n"
                "- /todo\n"
                "- /todo followup",
                context="todo-ackrun not-blocked",
                with_menu=True,
            )
            return {"terminal": True}

        issue = project_runtime_issue(entry)
        if issue:
            alias = _project_alias(entry, key)
            send(
                "todo ackrun blocked: project runtime is not ready\n"
                f"- orch: {key} ({alias})\n"
                f"- id: {todo_id}\n"
                f"- reason: {project_runtime_label(entry)}\n"
                "next:\n"
                f"- /orch status {alias}\n"
                "- fix runtime files, then retry /todo ackrun",
                context="todo-ackrun unready",
                with_menu=True,
            )
            return {"terminal": True}

        pending = entry.get("pending_todo")
        if (
            (not force)
            and isinstance(pending, dict)
            and str(pending.get("todo_id", "")).strip()
            and str(pending.get("chat_id", "")).strip() == str(chat_id)
        ):
            pending_id = str(pending.get("todo_id", "")).strip()
            alias = _project_alias(entry, key)
            send(
                "todo ackrun blocked: pending todo exists (awaiting dispatch/approval)\n"
                f"- orch: {key}\n"
                f"- pending: {pending_id}\n"
                "next:\n"
                "- /todo (list)\n"
                "- /ok (if confirm is pending)\n"
                "- /todo ackrun <TODO-xxx|number> force  (override)",
                context="todo-ackrun pending",
                with_menu=True,
                reply_markup=_todo_pending_reply_markup(alias, include_force=f"/todo {alias} ackrun {todo_id} force"),
            )
            return {"terminal": True}

        active_rows = _sorted_active_todos(todos)
        running = [
            row
            for row in active_rows
            if str(row.get("status", _STATUS_OPEN)).strip().lower() == _STATUS_RUNNING
            and str(row.get("id", "")).strip() != todo_id
        ]
        if running and not force:
            head = running[0]
            head_id = str(head.get("id", "")).strip() or "-"
            pr = _normalize_priority(str(head.get("priority", "P2")))
            summary = str(head.get("summary", "")).strip().replace("\n", " ")
            if len(summary) > 200:
                summary = summary[:197] + "..."
            send(
                "todo ackrun blocked: active running todo exists\n"
                f"- orch: {key}\n"
                f"- current: {head_id} | {pr} | {summary or '-'}\n"
                "next:\n"
                "- /todo (list)\n"
                f"- /todo done {head_id}\n"
                "- /todo ackrun <TODO-xxx|number> force  (override)",
                context="todo-ackrun busy",
                with_menu=True,
            )
            return {"terminal": True}

        now = now_iso()
        had_followup = _clear_blocked_meta(item, clear_current_request=True)
        item["status"] = _STATUS_OPEN
        item["updated_at"] = now
        queued = _queue_todo_for_dispatch(
            item=item,
            entry=entry,
            key=key,
            chat_id=chat_id,
            args=args,
            manager_state=manager_state,
            save_manager_state=save_manager_state,
            now_iso=now_iso,
        )
        send(
            "todo ackrun selected\n"
            f"- orch: {key}\n"
            f"- id: {queued['todo_id']}\n"
            f"- reopened: yes\n"
            f"- cleared_followup: {'yes' if had_followup else 'no'}\n"
            f"- priority: {queued['priority']}\n"
            f"- summary: {queued['summary_preview'] or '-'}\n"
            "dispatch starting...",
            context="todo-ackrun selected",
            with_menu=True,
        )
        return {
            "terminal": False,
            "cmd": "run",
            "orch_target": key,
            "run_prompt": queued["summary"],
            "run_force_mode": "dispatch",
            "run_auto_source": "todo-ackrun",
        }

    if sub in {"done", "finish", "complete", "completed"}:
        if len(tokens) < 2:
            send("usage: /todo done <TODO-xxx|number>", context="todo-done usage", with_menu=True)
            return {"terminal": True}
        ref = tokens[1].strip()
        item = _find_todo_by_ref(todos, ref)
        if item is None:
            send(f"todo not found: {ref}\n\n{_todo_usage()}", context="todo-done missing", with_menu=True)
            return {"terminal": True}

        now = now_iso()
        todo_id = str(item.get("id", "")).strip()
        item["status"] = _STATUS_DONE
        item["done_at"] = now
        item["updated_at"] = now
        item["done_by"] = f"telegram:{chat_id}"
        _clear_blocked_meta(item, clear_current_request=True)
        pending = entry.get("pending_todo")
        if (
            todo_id
            and isinstance(pending, dict)
            and str(pending.get("todo_id", "")).strip() == todo_id
            and str(pending.get("chat_id", "")).strip() == str(chat_id)
        ):
            entry.pop("pending_todo", None)
        entry["updated_at"] = now
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)

        todo_id = todo_id or "-"
        summary = str(item.get("summary", "")).strip()
        if len(summary) > 200:
            summary = summary[:197] + "..."
        send(
            "todo done\n"
            f"- orch: {key}\n"
            f"- id: {todo_id}\n"
            f"- summary: {summary or '-'}\n"
            "next: /todo",
            context="todo-done",
            with_menu=True,
        )
        return {"terminal": True}

    if sub in {"accept", "promote"}:
        if len(tokens) < 2:
            send("usage: /todo accept <PROP-xxx|number>", context="todo-accept usage", with_menu=True)
            return {"terminal": True}
        ref = tokens[1].strip()
        proposal = _find_proposal_by_ref(proposals, ref)
        if proposal is None:
            send(f"proposal not found: {ref}\n\n{_todo_usage()}", context="todo-accept missing", with_menu=True)
            return {"terminal": True}
        if _normalize_proposal_status(proposal.get("status")) != _PROPOSAL_STATUS_OPEN:
            send(
                "todo accept blocked: proposal is not open\n"
                f"- orch: {key}\n"
                f"- id: {str(proposal.get('id', '')).strip() or ref}",
                context="todo-accept not-open",
                with_menu=True,
            )
            return {"terminal": True}

        accepted = _accept_todo_proposal(
            entry=entry,
            proposal=proposal,
            actor=f"telegram:{chat_id}",
            now=now_iso(),
        )
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            "todo proposal accepted\n"
            f"- orch: {key}\n"
            f"- proposal: {accepted['proposal_id'] or ref}\n"
            f"- todo: {accepted['todo_id'] or '-'}\n"
            f"- created_new: {'yes' if accepted['created_new'] else 'no'}\n"
            f"- summary: {str(accepted['summary'])[:200] or '-'}",
            context="todo-accept",
            with_menu=True,
        )
        return {"terminal": True}

    if sub in {"reject", "drop"}:
        if len(tokens) < 2:
            send("usage: /todo reject <PROP-xxx|number> [reason]", context="todo-reject usage", with_menu=True)
            return {"terminal": True}
        ref = tokens[1].strip()
        proposal = _find_proposal_by_ref(proposals, ref)
        if proposal is None:
            send(f"proposal not found: {ref}\n\n{_todo_usage()}", context="todo-reject missing", with_menu=True)
            return {"terminal": True}
        if _normalize_proposal_status(proposal.get("status")) != _PROPOSAL_STATUS_OPEN:
            send(
                "todo reject blocked: proposal is not open\n"
                f"- orch: {key}\n"
                f"- id: {str(proposal.get('id', '')).strip() or ref}",
                context="todo-reject not-open",
                with_menu=True,
            )
            return {"terminal": True}
        reason = str(" ".join(tokens[2:]) if len(tokens) > 2 else "").strip()
        rejected = _reject_todo_proposal(
            entry=entry,
            proposal=proposal,
            actor=f"telegram:{chat_id}",
            now=now_iso(),
            reason=reason,
        )
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            "todo proposal rejected\n"
            f"- orch: {key}\n"
            f"- proposal: {rejected['proposal_id'] or ref}\n"
            + (f"- reason: {str(rejected['reason'])[:200]}\n" if rejected["reason"] else "")
            + f"- summary: {str(rejected['summary'])[:200] or '-'}",
            context="todo-reject",
            with_menu=True,
        )
        return {"terminal": True}

    send("unknown todo subcommand.\n\n" + _todo_usage(), context="todo-unknown", with_menu=True)
    return {"terminal": True}
