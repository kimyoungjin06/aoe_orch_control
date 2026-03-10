#!/usr/bin/env python3
"""Management command handlers for Telegram gateway."""

import json
import os
import re
import subprocess
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from aoe_tg_acl import (
    format_csv_set,
    parse_acl_command_args,
    parse_acl_revoke_args,
    resolve_role_from_acl_sets,
)
from aoe_tg_ops_policy import list_ops_projects, summarize_ops_scope
from aoe_tg_ops_view import (
    blocked_bucket_count as ops_view_blocked_bucket_count,
    blocked_head_summary as ops_view_blocked_head_summary,
    compact_age_label as ops_view_compact_age_label,
    render_ops_scope_compact_lines,
    render_project_snapshot_lines,
)
from aoe_tg_package_paths import team_tmux_script
from aoe_tg_project_runtime import project_hidden_from_ops, project_runtime_issue, project_runtime_label
from aoe_tg_todo_state import preview_syncback_plan

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
    raw = str(os.environ.get("AOE_TG_COMMAND_PREFIXES", "/") or "/").strip()
    for ch in raw:
        if ch in {"/", "!"}:
            return ch
    return "/"


def _normalize_prefetch_token(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    if token in {"recent", "recent_docs", "sync-recent"}:
        token = "sync_recent"
    return token if token in {"sync_recent"} else ""


def _parse_replace_sync_flag(tokens: List[str]) -> Optional[bool]:
    result: Optional[bool] = None
    for tok in tokens:
        low = str(tok or "").strip().lower()
        if low in {"replace-sync", "sync-replace", "replace_prefetch", "prefetch-replace"}:
            result = True
        elif low in {"no-replace-sync", "safe-sync", "no-sync-replace"}:
            result = False
    return result


def _prefetch_display(prefetch: Any, prefetch_since: Any, replace_sync: bool) -> str:
    token = _normalize_prefetch_token(prefetch)
    since_disp = str(prefetch_since or "").strip() or "-"
    if token == "sync_recent" and replace_sync:
        return "sync_recent+replace (full-scope; since ignored)"
    if token == "sync_recent":
        return f"sync_recent+salvage (since={since_disp})"
    return "-"


def _compact_age_label(raw_ts: str) -> str:
    return ops_view_compact_age_label(raw_ts)


def _compact_reason(raw: Any, limit: int = 120) -> str:
    text = " ".join(str(raw or "").strip().split())
    if len(text) > limit:
        return text[: max(0, limit - 3)].rstrip() + "..."
    return text


def _status_report_level(tokens: List[str], fallback: str) -> str:
    explicit = ""
    for tok in tokens[1:]:
        low = str(tok or "").strip().lower()
        if low in {"short", "brief", "compact", "간단", "짧게", "요약"}:
            explicit = "short"
        elif low in {"long", "detail", "detailed", "verbose", "full", "상세", "자세히"}:
            explicit = "long"
    if explicit:
        return explicit
    base = str(fallback or "").strip().lower()
    return "long" if base == "long" else "short"


def _focused_project_entry(manager_state: Dict[str, Any]) -> Tuple[str, Dict[str, Any], bool]:
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    if not isinstance(projects, dict) or not projects:
        return "", {}, False
    row = _project_lock_row(manager_state)
    locked = bool(row)
    key = str(row.get("project_key", "")).strip().lower()
    if not key:
        key = str(manager_state.get("active", "default") or "default").strip().lower()
    entry = projects.get(key)
    if not isinstance(entry, dict):
        return "", {}, locked
    return key, entry, locked


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
    key, entry, locked = _focused_project_entry(manager_state)
    if not key or not entry:
        return []
    return render_project_snapshot_lines(key=key, entry=entry, locked=locked)


def _ops_scope_summary(manager_state: Dict[str, Any]) -> Dict[str, List[str]]:
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    return summarize_ops_scope(projects)


def _ops_scope_compact_lines(manager_state: Dict[str, Any], *, limit: int = 4, detail_level: str = "short") -> List[str]:
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    return render_ops_scope_compact_lines(projects, limit=limit, detail_level=detail_level)


def _canonical_todo_path(entry: Dict[str, Any]) -> Path:
    root = Path(str(entry.get("project_root", "")).strip() or ".").expanduser()
    return (root / "TODO.md").resolve()


def _scenario_path(entry: Dict[str, Any]) -> Path:
    root = Path(str(entry.get("project_root", "")).strip() or ".").expanduser()
    return (root / ".aoe-team" / "AOE_TODO.md").resolve()


def _scenario_include_targets(entry: Dict[str, Any]) -> List[Tuple[str, bool]]:
    path = _scenario_path(entry)
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return []
    out: List[Tuple[str, bool]] = []
    for raw_line in text.splitlines():
        stripped = str(raw_line or "").strip()
        if not stripped.lower().startswith(_SCENARIO_INCLUDE_PREFIX):
            continue
        payload = stripped[len(_SCENARIO_INCLUDE_PREFIX) :].strip()
        if payload.startswith(":"):
            payload = payload[1:].strip()
        if not payload:
            continue
        target = Path(payload).expanduser()
        resolved = target if target.is_absolute() else (path.parent / target).resolve()
        rel = payload
        try:
            rel = str(resolved.relative_to(path.parent.parent))
        except Exception:
            rel = payload
        out.append((rel, resolved.exists()))
    return out


def _parse_iso_datetime(raw: str) -> Optional[datetime]:
    text = str(raw or "").strip()
    if not text:
        return None
    normalized = text
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    if re.search(r"[+-]\d{4}$", normalized):
        normalized = normalized[:-2] + ":" + normalized[-2:]
    try:
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def _alias_index(alias: str) -> int:
    token = str(alias or "").strip().upper()
    if token.startswith("O"):
        token = token[1:]
    return int(token) if token.isdigit() else 10**9


def _offdesk_prepare_targets(manager_state: Dict[str, Any], raw_target: str) -> List[Tuple[str, Dict[str, Any]]]:
    token = str(raw_target or "").strip()
    locked = _project_lock_row(manager_state)
    if token:
        if token.lower() == "all":
            if locked:
                key = str(locked.get("project_key", "")).strip().lower()
                projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
                entry = projects.get(key) if isinstance(projects, dict) else None
                return [(key, entry)] if isinstance(entry, dict) else []
            projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
            rows = list_ops_projects(projects)
            rows.sort(key=lambda kv: _alias_index(str(kv[1].get("project_alias", "")).strip() or str(kv[0])))
            return rows
        key, entry = _resolve_project_entry(manager_state, token)
        return [(key, entry)]

    if locked:
        key = str(locked.get("project_key", "")).strip().lower()
        projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
        entry = projects.get(key) if isinstance(projects, dict) else None
        return [(key, entry)] if isinstance(entry, dict) else []

    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    rows = list_ops_projects(projects)
    rows.sort(key=lambda kv: _alias_index(str(kv[1].get("project_alias", "")).strip() or str(kv[0])))
    return rows


def _offdesk_prepare_project_report(manager_state: Dict[str, Any], key: str, entry: Dict[str, Any]) -> Dict[str, Any]:
    alias = str(entry.get("project_alias", "")).strip().upper() or key
    display = str(entry.get("display_name", "")).strip() or key
    todos = entry.get("todos") if isinstance(entry.get("todos"), list) else []
    proposals = entry.get("todo_proposals") if isinstance(entry.get("todo_proposals"), list) else []
    counts = {name: 0 for name in ["open", "running", "blocked", "done", "canceled"]}
    for row in todos:
        if not isinstance(row, dict):
            continue
        status = str(row.get("status", "open")).strip().lower() or "open"
        if status not in counts:
            status = "open"
        counts[status] += 1
    open_proposals = sum(
        1
        for row in proposals
        if isinstance(row, dict) and str(row.get("status", "open")).strip().lower() == "open"
    )
    pending = entry.get("pending_todo")
    pending_flag = bool(isinstance(pending, dict) and str(pending.get("todo_id", "")).strip())
    runtime_issue = project_runtime_issue(entry)
    runtime_label = project_runtime_label(entry) if runtime_issue else "ready"
    canonical_path = _canonical_todo_path(entry)
    canonical_exists = canonical_path.exists()
    scenario = _scenario_path(entry)
    scenario_exists = scenario.exists()
    includes = _scenario_include_targets(entry)
    canonical_rel = "TODO.md"
    include_ok = False
    include_display = "-"
    syncback_counts = {"done": 0, "reopen": 0, "append": 0, "blocked": 0}
    syncback_pending = False
    syncback_error = ""
    if includes:
        include_display = ", ".join(f"{rel}{'' if exists else ' (missing)'}" for rel, exists in includes[:3])
        for rel, exists in includes:
            if exists and (rel == canonical_rel or rel.endswith("/TODO.md")):
                include_ok = True
                break
    if canonical_exists:
        try:
            plan = preview_syncback_plan(entry)
            syncback_counts = {
                "done": int(plan.get("done_count", 0) or 0),
                "reopen": int(plan.get("reopen_count", 0) or 0),
                "append": int(plan.get("append_count", 0) or 0),
                "blocked": int(plan.get("blocked_count", 0) or 0),
            }
            syncback_pending = any(syncback_counts.values())
        except Exception as exc:
            syncback_error = " ".join(str(exc).strip().split())[:180]
    last_sync_mode = str(entry.get("last_sync_mode", "")).strip() or "never"
    last_sync_at = str(entry.get("last_sync_at", "")).strip()
    last_sync_disp = _compact_age_label(last_sync_at)
    last_sync_dt = _parse_iso_datetime(last_sync_at)
    sync_stale = False
    if last_sync_dt is not None:
        now = datetime.now(last_sync_dt.tzinfo or timezone.utc)
        try:
            sync_stale = (now - last_sync_dt).total_seconds() > 24 * 3600
        except Exception:
            sync_stale = False
    manual_followup_count = _blocked_bucket_count(todos, "manual_followup")
    blocked_head = _blocked_head_summary(todos)
    notes: List[str] = []
    status = "ready"
    if runtime_issue:
        status = "blocked"
        notes.append(f"runtime not ready: {runtime_label}")
    if not scenario_exists:
        status = "blocked"
        notes.append("missing .aoe-team/AOE_TODO.md")
    if not canonical_exists:
        status = "warn" if status == "ready" else status
        notes.append("missing canonical TODO.md")
    if canonical_exists and not include_ok:
        status = "warn" if status == "ready" else status
        notes.append("AOE_TODO.md does not include canonical TODO.md")
    if counts["open"] == 0 and counts["running"] == 0 and counts["blocked"] == 0 and open_proposals == 0:
        status = "blocked" if status == "ready" else status
        notes.append("no runnable backlog")
    if pending_flag:
        status = "warn" if status == "ready" else status
        notes.append("pending todo awaiting dispatch/approval")
    if counts["running"] > 0:
        status = "warn" if status == "ready" else status
        notes.append("task already running")
    if counts["blocked"] > 0:
        status = "warn" if status == "ready" else status
        notes.append(f"blocked backlog present ({counts['blocked']})")
    if manual_followup_count > 0:
        status = "warn" if status == "ready" else status
        notes.append(f"manual follow-up backlog present ({manual_followup_count})")
    if open_proposals > 0:
        status = "warn" if status == "ready" else status
        notes.append(f"open todo proposals pending review ({open_proposals})")
    if syncback_pending:
        status = "warn" if status == "ready" else status
        notes.append(
            "syncback pending "
            f"(done={syncback_counts['done']} reopen={syncback_counts['reopen']} "
            f"append={syncback_counts['append']} blocked_notes={syncback_counts['blocked']})"
        )
    if syncback_error:
        status = "warn" if status == "ready" else status
        notes.append(f"syncback preview failed: {syncback_error}")
    if last_sync_mode == "never" or not last_sync_at:
        status = "warn" if status == "ready" else status
        notes.append("queue has not been synced yet")
    elif sync_stale:
        status = "warn" if status == "ready" else status
        notes.append(f"last sync is stale ({last_sync_disp})")

    lines = [
        f"- {alias} {display} [{status}]",
        f"  runtime: {runtime_label}",
        f"  canonical: {canonical_rel if canonical_exists else 'missing TODO.md'}",
        f"  scenario_include: {include_display}",
        f"  queue: open={counts['open']} running={counts['running']} blocked={counts['blocked']} followup={manual_followup_count} pending={'yes' if pending_flag else 'no'} proposals={open_proposals}",
        f"  syncback: done={syncback_counts['done']} reopen={syncback_counts['reopen']} append={syncback_counts['append']} blocked_notes={syncback_counts['blocked']}",
        f"  last_sync: {last_sync_mode} {last_sync_disp}".rstrip(),
    ]
    if blocked_head:
        head = f"  blocked_head: {blocked_head.get('id', '-')} x{blocked_head.get('count', 1)}"
        bucket = str(blocked_head.get("bucket", "")).strip()
        reason = str(blocked_head.get("reason", "")).strip()
        if bucket:
            head += f" [{bucket}]"
        if reason:
            head += f" | {reason}"
        lines.append(head)
    if notes:
        lines.append("  notes:")
        for note in notes[:4]:
            lines.append(f"    - {note}")
    return {
        "status": status,
        "lines": lines,
        "alias": alias,
        "display": display,
        "open": counts["open"],
        "running": counts["running"],
        "blocked_count": counts["blocked"],
        "followup_count": manual_followup_count,
        "proposals": open_proposals,
        "syncback_pending": syncback_pending,
        "syncback_counts": dict(syncback_counts),
        "pending_flag": pending_flag,
        "notes": list(notes),
    }


def _offdesk_review_reply_markup(flagged: List[Dict[str, Any]], *, clean: bool = False) -> Dict[str, Any]:
    keyboard: List[List[Dict[str, str]]] = []
    if clean:
        keyboard.extend(
            [
                [{"text": "/offdesk on"}, {"text": "/auto status"}],
                [{"text": "/offdesk prepare"}, {"text": "/map"}, {"text": "/help"}],
            ]
        )
        return {
            "keyboard": keyboard,
            "resize_keyboard": True,
            "one_time_keyboard": False,
            "input_field_placeholder": "예: /offdesk on",
        }

    for row in flagged[:3]:
        alias = str(row.get("alias", "")).strip() or "-"
        primary: List[Dict[str, str]] = []
        secondary: List[Dict[str, str]] = []
        if bool(row.get("syncback_pending", False)):
            primary.append({"text": f"/todo {alias} syncback preview"})
        if int(row.get("proposals", 0) or 0) > 0 and len(primary) < 3:
            primary.append({"text": f"/todo {alias} proposals"})
        if int(row.get("followup_count", 0) or 0) > 0 and len(primary) < 3:
            primary.append({"text": f"/todo {alias} followup"})
        if primary:
            keyboard.append(primary[:3])

        if int(row.get("blocked_count", 0) or 0) > 0 or int(row.get("open", 0) or 0) == 0:
            secondary.append({"text": f"/sync preview {alias} 24h"})
        secondary.append({"text": f"/orch status {alias}"})
        secondary.append({"text": f"/todo {alias}"})
        seen: set[str] = set()
        deduped_secondary: List[Dict[str, str]] = []
        for btn in secondary:
            text = str(btn.get("text", "")).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            deduped_secondary.append(btn)
        if deduped_secondary:
            keyboard.append(deduped_secondary[:3])

    keyboard.append([{"text": "/offdesk prepare"}, {"text": "/map"}, {"text": "/help"}])
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "예: /todo O3 syncback preview",
    }


def _offdesk_prepare_reply_markup(
    reports: List[Dict[str, Any]],
    *,
    blocked_count: int = 0,
    clean: bool = False,
) -> Dict[str, Any]:
    keyboard: List[List[Dict[str, str]]] = []
    if clean:
        keyboard.extend(
            [
                [{"text": "/offdesk on"}, {"text": "/offdesk review"}, {"text": "/auto status"}],
                [{"text": "/map"}, {"text": "/queue"}, {"text": "/help"}],
            ]
        )
        return {
            "keyboard": keyboard,
            "resize_keyboard": True,
            "one_time_keyboard": False,
            "input_field_placeholder": "예: /offdesk on",
        }

    flagged = [row for row in reports if str(row.get("status", "")).strip().lower() in {"warn", "blocked"}]
    for row in flagged[:3]:
        alias = str(row.get("alias", "")).strip() or "-"
        primary: List[Dict[str, str]] = []
        secondary: List[Dict[str, str]] = []

        if bool(row.get("syncback_pending", False)):
            primary.append({"text": f"/todo {alias} syncback preview"})
        if int(row.get("proposals", 0) or 0) > 0 and len(primary) < 3:
            primary.append({"text": f"/todo {alias} proposals"})
        if int(row.get("followup_count", 0) or 0) > 0 and len(primary) < 3:
            primary.append({"text": f"/todo {alias} followup"})
        if primary:
            keyboard.append(primary[:3])

        secondary.append({"text": f"/sync preview {alias} 24h"})
        secondary.append({"text": f"/orch status {alias}"})
        secondary.append({"text": f"/todo {alias}"})
        keyboard.append(secondary[:3])

    footer: List[Dict[str, str]] = []
    if blocked_count == 0:
        footer.append({"text": "/offdesk on"})
    footer.append({"text": "/offdesk review"})
    footer.append({"text": "/help"})
    keyboard.append(footer[:3])
    keyboard.append([{"text": "/map"}, {"text": "/queue"}])
    return {
        "keyboard": keyboard,
        "resize_keyboard": True,
        "one_time_keyboard": False,
        "input_field_placeholder": "예: /offdesk review",
    }


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
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    if not isinstance(projects, dict) or not projects:
        raise RuntimeError("no orch projects registered")

    token = str(raw_target or "").strip()
    if not token:
        token = str(manager_state.get("active", "default") or "default").strip()

    key = token.strip().lower()
    entry = projects.get(key)
    if isinstance(entry, dict):
        conflict = _project_lock_conflict_text(manager_state, key)
        if conflict:
            raise RuntimeError(conflict)
        return key, entry

    # Alias match: O#
    up = token.strip().upper()
    if up.startswith("O"):
        for k, v in projects.items():
            if not isinstance(v, dict):
                continue
            if str(v.get("project_alias", "")).strip().upper() == up:
                conflict = _project_lock_conflict_text(manager_state, str(k))
                if conflict:
                    raise RuntimeError(conflict)
                return str(k), v

    known = ", ".join(sorted(str(k) for k in projects.keys()))
    raise RuntimeError(f"unknown orch project: {token} (known: {known})")


def _project_lock_row(manager_state: Dict[str, Any]) -> Dict[str, Any]:
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    raw = manager_state.get("project_lock") if isinstance(manager_state, dict) else {}
    if not isinstance(raw, dict) or not isinstance(projects, dict):
        return {}
    if not bool(raw.get("enabled", False)):
        return {}
    key = str(raw.get("project_key", "")).strip().lower()
    if not key:
        return {}
    entry = projects.get(key)
    if not isinstance(entry, dict):
        return {}
    out = {"enabled": True, "project_key": key}
    locked_at = str(raw.get("locked_at", "")).strip()
    locked_by = str(raw.get("locked_by", "")).strip()
    if locked_at:
        out["locked_at"] = locked_at[:40]
    if locked_by:
        out["locked_by"] = locked_by[:120]
    return out


def _project_lock_label(manager_state: Dict[str, Any]) -> str:
    row = _project_lock_row(manager_state)
    key = str(row.get("project_key", "")).strip()
    if not key:
        return ""
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    entry = projects.get(key) if isinstance(projects, dict) else {}
    alias = str((entry or {}).get("project_alias", "")).strip() or key
    return f"{alias} ({key})"


def _project_lock_conflict_text(manager_state: Dict[str, Any], requested_key: str) -> str:
    row = _project_lock_row(manager_state)
    lock_key = str(row.get("project_key", "")).strip()
    if not lock_key or requested_key == lock_key:
        return ""
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    locked_entry = projects.get(lock_key) if isinstance(projects, dict) else {}
    req_entry = projects.get(requested_key) if isinstance(projects, dict) else {}
    locked_alias = str((locked_entry or {}).get("project_alias", "")).strip() or lock_key
    req_alias = str((req_entry or {}).get("project_alias", "")).strip() or requested_key
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
    # Avoid importing gateway helpers here; keep this module standalone.
    import time as _time

    return _time.strftime("%Y-%m-%dT%H:%M:%S%z")


def _auto_state_path(args: Any) -> Path:
    team_dir = getattr(args, "team_dir", None)
    if isinstance(team_dir, Path):
        return (team_dir / AUTO_STATE_FILENAME).resolve()
    return Path(str(team_dir or ".")).expanduser().resolve() / AUTO_STATE_FILENAME


def _offdesk_state_path(args: Any) -> Path:
    team_dir = getattr(args, "team_dir", None)
    if isinstance(team_dir, Path):
        return (team_dir / OFFDESK_STATE_FILENAME).resolve()
    return Path(str(team_dir or ".")).expanduser().resolve() / OFFDESK_STATE_FILENAME


def _load_auto_state(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _save_auto_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(state)
    payload["updated_at"] = _now_iso()
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _load_offdesk_state(path: Path) -> Dict[str, Any]:
    return _load_auto_state(path)


def _save_offdesk_state(path: Path, state: Dict[str, Any]) -> None:
    _save_auto_state(path, state)


def _scheduler_session_name() -> str:
    return (os.environ.get("AOE_TMUX_SCHEDULER_SESSION") or "aoe_mo_scheduler").strip() or "aoe_mo_scheduler"


def _tmux_has_session(session_name: str) -> bool:
    token = str(session_name or "").strip()
    if not token:
        return False
    try:
        proc = subprocess.run(["tmux", "has-session", "-t", token], capture_output=True, text=True, check=False)
        return proc.returncode == 0
    except Exception:
        return False


def _tmux_auto_command(args: Any, action: str) -> Tuple[bool, str]:
    script = team_tmux_script().resolve()
    if not script.exists():
        return False, f"tmux script not found: {script}"
    if not os.access(script, os.X_OK):
        return False, f"tmux script not executable: {script}"
    try:
        env = dict(os.environ)
        project_root = Path(str(getattr(args, "project_root", ".") or ".")).expanduser().resolve()
        team_dir = Path(str(getattr(args, "team_dir", project_root / ".aoe-team") or (project_root / ".aoe-team"))).expanduser().resolve()
        env["AOE_PROJECT_ROOT"] = str(project_root)
        env["AOE_TEAM_DIR"] = str(team_dir)
        proc = subprocess.run([str(script), "auto", action], capture_output=True, text=True, check=False, env=env)
        out = (proc.stdout or proc.stderr or "").strip()
        return proc.returncode == 0, out
    except Exception as e:
        return False, str(e)


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

    if cmd == "focus":
        tokens = [t for t in str(rest or "").split() if t.strip()]
        sub = (tokens[0].lower() if tokens else "status").strip()
        if sub in {"", "show", "status"}:
            sub = "status"

        row = _project_lock_row(manager_state)
        active_key = str(manager_state.get("active", "default") or "default").strip()
        active_label = ""
        try:
            key0, entry0 = _resolve_project_entry(manager_state, active_key)
            alias0 = str(entry0.get("project_alias", "")).strip() or key0
            active_label = f"{alias0} ({key0})"
        except Exception:
            active_label = active_key or "-"

        if sub == "status":
            send(
                "project focus lock\n"
                f"- enabled: {'yes' if row else 'no'}\n"
                f"- active_project: {active_label or '-'}\n"
                f"- locked_project: {_project_lock_label(manager_state) or '-'}\n"
                "set:\n"
                "- /map\n"
                "- /focus O2\n"
                "- /focus off\n"
                "rules:\n"
                "- /next, /queue, plain text, TF run are pinned to the locked project\n"
                "- /fanout and /auto on fanout stay blocked while lock is enabled",
                context="focus-status",
                with_menu=True,
            )
            return True

        if chat_role == "readonly":
            send(
                "permission denied: readonly chat cannot change project focus.\n"
                "read-only: /focus",
                context="focus-deny",
                with_menu=True,
            )
            return True

        if sub in {"off", "clear", "none", "unlock", "release"}:
            existed = bool(row)
            manager_state.pop("project_lock", None)
            if not args.dry_run:
                save_manager_state(args.manager_state_file, manager_state)
            send(
                "project focus lock updated\n"
                "- enabled: no\n"
                f"- changed: {'yes' if existed else 'no'}\n"
                f"- active_project: {active_label or '-'}\n"
                "next:\n"
                "- /map\n"
                "- /use O2",
                context="focus-off",
                with_menu=True,
            )
            return True

        target = str(tokens[0] if tokens else "").strip()
        if not target:
            raise RuntimeError("usage: /focus [O#|name|off]")

        key, entry = _resolve_project_entry(manager_state, target)
        alias = str(entry.get("project_alias", "")).strip() or key
        manager_state["active"] = key
        manager_state["project_lock"] = {
            "enabled": True,
            "project_key": key,
            "locked_at": _now_iso(),
            "locked_by": f"telegram:{chat_id}",
        }
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)
        send(
            "project focus lock updated\n"
            "- enabled: yes\n"
            f"- locked_project: {alias} ({key})\n"
            "- effect: /next, /queue, plain text, TF run -> this project by default\n"
            "- blocked: /fanout, /auto on fanout\n"
            "next:\n"
            f"- /sync {alias} 1h\n"
            "- /next\n"
            "- /focus off",
            context="focus-on",
            with_menu=True,
        )
        return True

    if cmd == "panic":
        tokens = [t for t in str(rest or "").split() if t.strip()]
        sub = (tokens[0].lower() if tokens else "").strip()
        if sub in {"", "go", "now", "on", "stop"}:
            sub = "stop"
        if sub in {"show"}:
            sub = "status"
        if sub in {"help", "h", "?"}:
            raise RuntimeError("usage: /panic [status]")
        if sub not in {"stop", "status"}:
            raise RuntimeError("usage: /panic [status]")

        auto_path = _auto_state_path(args)
        auto_state = _load_auto_state(auto_path)
        auto_enabled = bool(auto_state.get("enabled", False))
        auto_chat = str(auto_state.get("chat_id", "")).strip() or "-"

        off_path = _offdesk_state_path(args)
        off_state = _load_offdesk_state(off_path)
        off_enabled = bool(off_state.get("enabled", False))
        off_chat = str(off_state.get("chat_id", "")).strip() or "-"

        session = _scheduler_session_name()
        sess_up = _tmux_has_session(session)

        current_default_mode = get_default_mode(manager_state, chat_id) or "off"
        current_pending_mode = get_pending_mode(manager_state, chat_id) or "none"

        if sub == "status":
            lines = [
                "panic switch",
                f"- routing_mode: {current_default_mode}",
                f"- one_shot_pending: {current_pending_mode}",
                f"- auto_enabled: {'yes' if auto_enabled else 'no'} (chat_id={auto_chat})",
                f"- offdesk_enabled: {'yes' if off_enabled else 'no'} (chat_id={off_chat})",
                f"- tmux_scheduler: {session} ({'up' if sess_up else 'down'})",
                "",
                "actions:",
                "- /panic        # stop auto/offdesk + clear pending/confirm + routing off",
                "- /offdesk on   # resume preset",
                "- /auto on fanout recent",
                "- /auto status",
            ]
            send("\n".join(lines).strip(), context="panic-status", with_menu=True)
            return True

        if chat_role == "readonly":
            send(
                "permission denied: readonly chat cannot use /panic.\n"
                "read-only: /panic status",
                context="panic-deny",
                with_menu=True,
            )
            return True

        # 1) Stop the tmux sidecar scheduler first (best-effort).
        if args.dry_run:
            tmux_ok, tmux_out = True, "dry-run: skipped tmux auto off"
        else:
            tmux_ok, tmux_out = _tmux_auto_command(args, "off")

        # 2) Disable auto/offdesk state flags (so even if tmux is down, sidecar will stop).
        auto_state["enabled"] = False
        auto_state["chat_id"] = str(auto_state.get("chat_id", "")).strip() or str(chat_id)
        auto_state["stopped_at"] = _now_iso()
        auto_state["stopped_reason"] = "panic"
        if not args.dry_run:
            _save_auto_state(auto_path, auto_state)

        if not isinstance(off_state, dict):
            off_state = {}
        off_state["enabled"] = False
        off_state["chat_id"] = str(chat_id)
        off_state["stopped_at"] = _now_iso()
        off_state["stopped_reason"] = "panic"
        if not args.dry_run:
            _save_offdesk_state(off_path, off_state)

        # 3) Clear any chat-level routing/pending/confirm so no more accidental dispatch.
        existed_default = clear_default_mode(manager_state, chat_id)
        cleared_pending = clear_pending_mode(manager_state, chat_id)
        cleared_confirm = clear_confirm_action(manager_state, chat_id)
        if not args.dry_run:
            save_manager_state(args.manager_state_file, manager_state)

        send(
            "panic activated\n"
            "- auto: stopped\n"
            f"- offdesk: {'stopped' if off_enabled else 'already_off'}\n"
            f"- tmux: {'stopped' if tmux_ok else 'stop_failed'}\n"
            f"- detail: {tmux_out or '-'}\n"
            f"- routing_mode: off (changed={'yes' if existed_default else 'no'})\n"
            f"- pending_cleared: {'yes' if cleared_pending else 'no'}\n"
            f"- confirm_cleared: {'yes' if cleared_confirm else 'no'}\n"
            "next:\n"
            "- /offdesk status\n"
            "- /auto status\n"
            "- /offdesk on   (resume)\n"
            "- /mode on      (enable plain-text routing again)",
            context="panic",
            with_menu=True,
        )
        return True

    if cmd == "offdesk":
        tokens = [t for t in str(rest or "").split() if t.strip()]
        sub = (tokens[0].lower() if tokens else "status").strip()
        if sub in {"", "show"}:
            sub = "status"
        if sub not in {"status", "on", "off", "start", "stop", "prepare", "preflight", "check", "review"}:
            raise RuntimeError("usage: /offdesk [on|off|status|prepare|review] [replace-sync|O#|name|all]")
        replace_sync = _parse_replace_sync_flag(tokens[1:])

        fallback_level = str(getattr(args, "default_report_level", "normal") or "normal").strip().lower()
        current_default_mode = get_default_mode(manager_state, chat_id) or "off"
        current_pending_mode = get_pending_mode(manager_state, chat_id) or "none"
        current_report_level = get_chat_report_level(manager_state, chat_id, fallback_level)
        status_level = _status_report_level(tokens, current_report_level)
        current_room = get_chat_room(manager_state, chat_id, DEFAULT_OFFDESK_ROOM) or DEFAULT_OFFDESK_ROOM

        off_path = _offdesk_state_path(args)
        off_state = _load_offdesk_state(off_path)
        off_enabled = bool(off_state.get("enabled", False))

        auto_path = _auto_state_path(args)
        auto_state = _load_auto_state(auto_path)
        auto_enabled = bool(auto_state.get("enabled", False))
        auto_cmd = str(auto_state.get("command", "")).strip().lower() or "next"
        auto_prefetch = _normalize_prefetch_token(auto_state.get("prefetch", ""))
        auto_replace_sync = bool(auto_state.get("prefetch_replace_sync", False))
        focus_label = _project_lock_label(manager_state) or "-"
        scope_summary = _ops_scope_summary(manager_state)
        included_scope = ", ".join(scope_summary.get("included", [])[:6]) or "-"
        excluded_scope = ", ".join(scope_summary.get("excluded", [])[:6]) or "-"

        if sub == "status":
            lines = [
                "offdesk mode",
                f"- enabled: {'yes' if off_enabled else 'no'}",
                f"- project_lock: {focus_label}",
                f"- ops_scope: {included_scope}",
                f"- ops_excluded: {excluded_scope}",
                f"- report_view: {status_level}",
                f"- routing_mode: {current_default_mode}",
                f"- one_shot_pending: {current_pending_mode}",
                f"- report_level: {current_report_level}",
                f"- room: {current_room}",
                f"- auto_enabled: {'yes' if auto_enabled else 'no'}",
                f"- auto_command: {auto_cmd}",
                f"- auto_prefetch: {_prefetch_display(auto_prefetch, auto_state.get('prefetch_since', ''), auto_replace_sync)}",
                "",
                "set:",
                "- /offdesk on",
                "- /offdesk on replace-sync",
                "- /offdesk off",
                "- /auto status",
            ]
            snapshot_lines = _focused_project_snapshot_lines(manager_state)
            if status_level == "long" and snapshot_lines:
                lines.extend([""] + snapshot_lines)
            compact_lines = _ops_scope_compact_lines(manager_state, detail_level=status_level)
            if compact_lines:
                lines.extend(["", "ops projects:"] + compact_lines)
            send("\n".join(lines).strip(), context="offdesk-status", with_menu=True)
            return True

        if sub in {"prepare", "preflight", "check"}:
            raw_target = ""
            for tok in tokens[1:]:
                low = str(tok or "").strip().lower()
                if low in {
                    "replace-sync",
                    "sync-replace",
                    "replace_prefetch",
                    "prefetch-replace",
                    "no-replace-sync",
                    "safe-sync",
                    "no-sync-replace",
                }:
                    continue
                raw_target = str(tok or "").strip()
                break
            try:
                targets = _offdesk_prepare_targets(manager_state, raw_target)
            except RuntimeError as exc:
                send(str(exc).strip(), context="offdesk-prepare blocked", with_menu=True)
                return True
            if not targets:
                send("offdesk prepare\n- no orch projects registered", context="offdesk-prepare empty", with_menu=True)
                return True

            reports = [_offdesk_prepare_project_report(manager_state, key, entry) for key, entry in targets]
            ready_count = sum(1 for row in reports if row.get("status") == "ready")
            warn_count = sum(1 for row in reports if row.get("status") == "warn")
            blocked_count = sum(1 for row in reports if row.get("status") == "blocked")
            scope_label = _project_lock_label(manager_state) or ("all" if len(targets) > 1 else reports[0].get("alias", "-"))
            scope_summary = _ops_scope_summary(manager_state)
            included_scope = ", ".join(scope_summary.get("included", [])[:6]) or "-"
            excluded_scope = ", ".join(scope_summary.get("excluded", [])[:6]) or "-"
            lines = [
                "offdesk prepare",
                f"- scope: {scope_label}",
                f"- ops_scope: {included_scope}",
                f"- ops_excluded: {excluded_scope}",
                f"- projects: {len(targets)}",
                f"- ready: {ready_count}",
                f"- warn: {warn_count}",
                f"- blocked: {blocked_count}",
                "",
                "projects:",
            ]
            for report in reports:
                lines.extend(report.get("lines") or [])

            lines.extend(["", "next:"])
            if blocked_count == 0:
                lines.append("- /offdesk on")
            else:
                lines.append("- fix blocked items before /offdesk on")
            if len(targets) == 1:
                alias = str(reports[0].get("alias", "")).strip() or "-"
                lines.append(f"- /sync preview {alias} 24h")
                lines.append(f"- /todo {alias}")
                lines.append(f"- /todo {alias} syncback preview")
            else:
                lines.append("- /map")
                lines.append("- /queue")
                lines.append("- /todo proposals")
            send(
                "\n".join(lines).strip(),
                context="offdesk-prepare",
                with_menu=True,
                reply_markup=_offdesk_prepare_reply_markup(
                    reports,
                    blocked_count=blocked_count,
                    clean=(warn_count == 0 and blocked_count == 0),
                ),
            )
            return True

        if sub == "review":
            raw_target = ""
            for tok in tokens[1:]:
                low = str(tok or "").strip().lower()
                if low in {
                    "replace-sync",
                    "sync-replace",
                    "replace_prefetch",
                    "prefetch-replace",
                    "no-replace-sync",
                    "safe-sync",
                    "no-sync-replace",
                }:
                    continue
                raw_target = str(tok or "").strip()
                break
            try:
                targets = _offdesk_prepare_targets(manager_state, raw_target)
            except RuntimeError as exc:
                send(str(exc).strip(), context="offdesk-review blocked", with_menu=True)
                return True
            if not targets:
                send("offdesk review\n- no orch projects registered", context="offdesk-review empty", with_menu=True)
                return True

            reports = [_offdesk_prepare_project_report(manager_state, key, entry) for key, entry in targets]
            flagged = [row for row in reports if str(row.get("status", "")).strip().lower() in {"warn", "blocked"}]
            lines = [
                "offdesk review",
                f"- reviewed: {len(reports)}",
                f"- flagged: {len(flagged)}",
            ]
            if not flagged:
                lines.extend(
                    [
                        "- status: clean",
                        "",
                        "next:",
                        "- /offdesk on",
                        "- /auto status",
                    ]
                )
                send(
                    "\n".join(lines).strip(),
                    context="offdesk-review clean",
                    with_menu=True,
                    reply_markup=_offdesk_review_reply_markup([], clean=True),
                )
                return True

            lines.extend(["", "actions:"])
            for row in flagged:
                alias = str(row.get("alias", "")).strip() or "-"
                display = str(row.get("display", "")).strip() or alias
                actions: List[str] = []
                if bool(row.get("syncback_pending", False)):
                    actions.append(f"/todo {alias} syncback preview")
                if int(row.get("proposals", 0) or 0) > 0:
                    actions.append(f"/todo {alias} proposals")
                if int(row.get("followup_count", 0) or 0) > 0:
                    actions.append(f"/todo {alias} followup")
                if int(row.get("blocked_count", 0) or 0) > 0 or int(row.get("open", 0) or 0) == 0:
                    actions.append(f"/sync preview {alias} 24h")
                if bool(row.get("pending_flag", False)) or int(row.get("running", 0) or 0) > 0:
                    actions.append(f"/orch status {alias}")
                if not actions:
                    actions.append(f"/todo {alias}")
                lines.append(f"- {alias} {display} [{row.get('status', '-')}]")
                note_rows = list(row.get("notes") or [])
                for note in note_rows[:2]:
                    lines.append(f"  note: {note}")
                lines.append(f"  do: {', '.join(actions)}")

            lines.extend(["", "next:", "- resolve flagged items, then /offdesk on", "- /offdesk prepare"])
            send(
                "\n".join(lines).strip(),
                context="offdesk-review",
                with_menu=True,
                reply_markup=_offdesk_review_reply_markup(flagged),
            )
            return True

        if chat_role == "readonly":
            send(
                "permission denied: readonly chat cannot change offdesk mode.\n"
                "read-only: /offdesk (status/prepare only)",
                context="offdesk-deny",
                with_menu=True,
            )
            return True

        if sub in {"off", "stop"}:
            prev = off_state.get("prev") if isinstance(off_state.get("prev"), dict) else {}

            prev_mode_present = bool(prev.get("default_mode_present", False))
            prev_mode = str(prev.get("default_mode", "")).strip().lower()
            if prev_mode_present and prev_mode in {"dispatch", "direct"}:
                set_default_mode(manager_state, chat_id, prev_mode)
            else:
                clear_default_mode(manager_state, chat_id)

            prev_report_present = bool(prev.get("report_level_present", False))
            prev_report = str(prev.get("report_level", "")).strip().lower()
            if prev_report_present and prev_report in {"short", "normal", "long"}:
                set_chat_report_level(manager_state, chat_id, prev_report)
            else:
                clear_chat_report_level(manager_state, chat_id)

            prev_room_present = bool(prev.get("room_present", False))
            prev_room = str(prev.get("room", "")).strip()
            if prev_room_present and prev_room:
                set_chat_room(manager_state, chat_id, prev_room)
            else:
                set_chat_room(manager_state, chat_id, DEFAULT_OFFDESK_ROOM)

            cleared_pending = clear_pending_mode(manager_state, chat_id)
            cleared_confirm = clear_confirm_action(manager_state, chat_id)

            auto_state = _load_auto_state(auto_path)
            auto_state["enabled"] = False
            auto_state["chat_id"] = str(auto_state.get("chat_id", "")).strip() or str(chat_id)
            auto_state["stopped_at"] = _now_iso()
            if not args.dry_run:
                _save_auto_state(auto_path, auto_state)

            if args.dry_run:
                ok, out = True, "dry-run: skipped tmux auto off"
            else:
                ok, out = _tmux_auto_command(args, "off")

            off_state["enabled"] = False
            off_state["chat_id"] = str(chat_id)
            off_state["stopped_at"] = _now_iso()
            if not args.dry_run:
                _save_offdesk_state(off_path, off_state)
                save_manager_state(args.manager_state_file, manager_state)

            send(
                "offdesk disabled\n"
                f"- restored_routing_mode: {(get_default_mode(manager_state, chat_id) or 'off')}\n"
                f"- restored_report_level: {get_chat_report_level(manager_state, chat_id, fallback_level)}\n"
                f"- restored_room: {get_chat_room(manager_state, chat_id, DEFAULT_OFFDESK_ROOM) or DEFAULT_OFFDESK_ROOM}\n"
                f"- pending_cleared: {'yes' if cleared_pending else 'no'}\n"
                f"- confirm_cleared: {'yes' if cleared_confirm else 'no'}\n"
                f"- auto: {'stopped' if ok else 'stop_failed'}\n"
                f"- detail: {out or '-'}\n"
                "next:\n"
                "- /offdesk status\n"
                "- /auto status",
                context="offdesk-off",
                with_menu=True,
            )
            return True

        # on/start
        existing_prev = off_state.get("prev") if isinstance(off_state, dict) else None
        if off_enabled and isinstance(existing_prev, dict):
            prev = dict(existing_prev)
        else:
            sessions = manager_state.get("chat_sessions") if isinstance(manager_state, dict) else {}
            row = sessions.get(str(chat_id)) if isinstance(sessions, dict) else None
            row = row if isinstance(row, dict) else {}
            prev = {
                "default_mode_present": ("default_mode" in row),
                "default_mode": str(row.get("default_mode", "")).strip().lower(),
                "report_level_present": ("report_level" in row),
                "report_level": str(row.get("report_level", "")).strip().lower(),
                "room_present": ("room" in row),
                "room": str(row.get("room", "")).strip(),
            }

        off_state = {
            "enabled": True,
            "chat_id": str(chat_id),
            "started_at": str(off_state.get("started_at", "")).strip() or _now_iso(),
            "prev": prev,
        }
        if not args.dry_run:
            _save_offdesk_state(off_path, off_state)

        # Apply safer off-desk chat settings.
        set_chat_report_level(manager_state, chat_id, DEFAULT_OFFDESK_REPORT_LEVEL)
        set_chat_room(manager_state, chat_id, DEFAULT_OFFDESK_ROOM)
        existed_default = clear_default_mode(manager_state, chat_id)
        cleared_pending = clear_pending_mode(manager_state, chat_id)
        cleared_confirm = clear_confirm_action(manager_state, chat_id)

        # Enable auto scheduler (fanout wave + idle prefetch).
        focus_row = _project_lock_row(manager_state)
        offdesk_command = "next" if focus_row else DEFAULT_OFFDESK_COMMAND
        auto_state = _load_auto_state(auto_path)
        auto_state["enabled"] = True
        auto_state["chat_id"] = str(chat_id)
        if "started_at" not in auto_state:
            auto_state["started_at"] = _now_iso()
        auto_state["command"] = offdesk_command
        auto_state["prefetch"] = DEFAULT_OFFDESK_PREFETCH
        auto_state["prefetch_replace_sync"] = bool(replace_sync)
        if "prefetch_since" not in auto_state:
            auto_state["prefetch_since"] = DEFAULT_OFFDESK_PREFETCH_SINCE
        auto_state["force"] = False
        if "interval_sec" not in auto_state:
            auto_state["interval_sec"] = DEFAULT_AUTO_INTERVAL_SEC
        if "idle_sec" not in auto_state:
            auto_state["idle_sec"] = DEFAULT_AUTO_IDLE_SEC
        if not args.dry_run:
            _save_auto_state(auto_path, auto_state)
            save_manager_state(args.manager_state_file, manager_state)

        if args.dry_run:
            ok, out = True, "dry-run: skipped tmux auto on"
        else:
            ok, out = _tmux_auto_command(args, "on")

        scope_summary = _ops_scope_summary(manager_state)
        included_scope = ", ".join(scope_summary.get("included", [])[:6]) or "-"
        excluded_scope = ", ".join(scope_summary.get("excluded", [])[:6]) or "-"
        body = (
            "offdesk enabled\n"
            f"- ops_scope: {included_scope}\n"
            f"- ops_excluded: {excluded_scope}\n"
            "- routing_mode: off\n"
            f"- report_level: {DEFAULT_OFFDESK_REPORT_LEVEL}\n"
            f"- room: {DEFAULT_OFFDESK_ROOM}\n"
            f"- auto: {'started' if ok else 'start_failed'}\n"
            f"- command: {offdesk_command}\n"
            f"- prefetch: {_prefetch_display(DEFAULT_OFFDESK_PREFETCH, DEFAULT_OFFDESK_PREFETCH_SINCE, bool(replace_sync))}\n"
            f"- changed_default_mode: {'yes' if existed_default else 'no'}\n"
            f"- pending_cleared: {'yes' if cleared_pending else 'no'}\n"
            f"- confirm_cleared: {'yes' if cleared_confirm else 'no'}\n"
            f"- detail: {out or '-'}\n"
        )
        if focus_row:
            body += f"- project_lock: {_project_lock_label(manager_state)}\n"
            body += "- note: project lock active, offdesk was narrowed to single-project /next mode\n"
        snapshot_lines = _focused_project_snapshot_lines(manager_state)
        if snapshot_lines:
            body += "\n" + "\n".join(snapshot_lines) + "\n"
        compact_lines = _ops_scope_compact_lines(manager_state, detail_level="short")
        if compact_lines:
            body += "\nops projects:\n" + "\n".join(compact_lines) + "\n"
        body += (
            "next:\n"
            "- /offdesk status\n"
            "- /queue\n"
            "- /room tail 30\n"
            "- /auto status"
        )
        send(body, context="offdesk-on", with_menu=True)
        return True

    if cmd == "auto":
        tokens = [t for t in str(rest or "").split() if t.strip()]
        sub = (tokens[0].lower() if tokens else "status").strip()
        if sub in {"", "show"}:
            sub = "status"
        if sub not in {"status", "on", "off", "start", "stop"}:
            raise RuntimeError("usage: /auto [on|off|status]")

        command: Optional[str] = None
        for tok in tokens[1:]:
            low = tok.strip().lower()
            if low in {"fanout", "wave", "oneeach", "round"}:
                command = "fanout"
            elif low in {"next", "global"}:
                command = "next"

        prefetch: Optional[str] = None
        for tok in tokens[1:]:
            low = tok.strip().lower()
            if low in {"recent", "docs", "prefetch", "sync-recent", "recent-docs"}:
                prefetch = "sync_recent"
            elif low in {"no-recent", "no-docs", "noprefetch", "no-prefetch"}:
                prefetch = ""
        replace_sync = _parse_replace_sync_flag(tokens[1:])

        prefetch_since: Optional[str] = None
        i = 1
        while i < len(tokens):
            tok = str(tokens[i] or "").strip()
            low = tok.lower()
            if low in {"since", "--since", "-s", "within", "--within"}:
                if i + 1 < len(tokens):
                    prefetch_since = str(tokens[i + 1] or "").strip()
                    i += 2
                else:
                    i += 1
                continue
            if low.startswith("since=") or low.startswith("--since=") or low.startswith("-s=") or low.startswith("within="):
                prefetch_since = tok.split("=", 1)[1].strip() if "=" in tok else ""
                i += 1
                continue
            i += 1

        force = any(t.lower() in {"force", "!", "--force"} for t in tokens[1:])
        interval_sec = None
        idle_sec = None
        max_failures = None
        # Optional numeric tokens: first number -> interval_sec, second -> idle_sec
        nums = [t for t in tokens[1:] if t.replace(".", "", 1).isdigit()]
        if nums:
            try:
                interval_sec = max(1, min(300, int(float(nums[0]))))
            except Exception:
                interval_sec = None
        if len(nums) >= 2:
            try:
                idle_sec = max(1, min(3600, int(float(nums[1]))))
            except Exception:
                idle_sec = None

        for tok in tokens[1:]:
            low = tok.strip().lower()
            if not any(
                low.startswith(p)
                for p in {
                    "maxfail=",
                    "maxfails=",
                    "maxfailures=",
                    "max_fail=",
                    "max_fails=",
                    "max_failures=",
                }
            ):
                continue
            raw = tok.split("=", 1)[1].strip() if "=" in tok else ""
            try:
                max_failures = max(1, min(50, int(float(raw))))
            except Exception:
                max_failures = None

        path = _auto_state_path(args)
        current = _load_auto_state(path)
        enabled = bool(current.get("enabled", False))
        session = _scheduler_session_name()
        sess_up = _tmux_has_session(session)
        focus_row = _project_lock_row(manager_state)
        focus_label = _project_lock_label(manager_state) or "-"
        fallback_level = str(getattr(args, "default_report_level", "normal") or "normal").strip().lower()
        current_report_level = get_chat_report_level(manager_state, chat_id, fallback_level)
        status_level = _status_report_level(tokens, current_report_level)

        if sub == "status":
            chat_ref = str(current.get("chat_id", "")).strip() or "-"
            eff_force = bool(current.get("force", False))
            eff_command = str(current.get("command", "next")).strip().lower() or "next"
            if eff_command not in {"next", "fanout"}:
                eff_command = "next"
            prefetch_token = _normalize_prefetch_token(current.get("prefetch", ""))
            replace_sync_enabled = bool(current.get("prefetch_replace_sync", False))
            eff_interval = int(current.get("interval_sec") or DEFAULT_AUTO_INTERVAL_SEC)
            eff_idle = int(current.get("idle_sec") or DEFAULT_AUTO_IDLE_SEC)
            eff_max_fail = int(current.get("max_failures") or DEFAULT_AUTO_MAX_FAILURES)
            last_reason = str(current.get("last_reason", "")).strip()
            last_run = str(current.get("last_run_at", "")).strip()
            last_candidate = str(current.get("last_candidate", "")).strip()
            last_prefetch_at = str(current.get("last_prefetch_at", "")).strip()
            last_prefetch_reason = str(current.get("last_prefetch_reason", "")).strip()
            last_prefetch_mode = str(current.get("last_prefetch_mode", "")).strip()
            stuck_candidate = str(current.get("stuck_candidate", "")).strip()
            stuck_count = int(current.get("stuck_count") or 0)
            fail_count = int(current.get("fail_count") or 0)
            fail_candidate = str(current.get("fail_candidate", "")).strip()
            fail_reason = str(current.get("fail_reason", "")).strip()
            lines = [
                "auto scheduler",
                f"- enabled: {'yes' if enabled else 'no'}",
                f"- project_lock: {focus_label}",
                f"- report_view: {status_level}",
                f"- chat_id: {chat_ref}",
                f"- command: {eff_command}",
                f"- prefetch: {_prefetch_display(prefetch_token, current.get('prefetch_since', ''), replace_sync_enabled)}",
                f"- force: {'yes' if eff_force else 'no'}",
                f"- interval_sec: {eff_interval}",
                f"- idle_sec: {eff_idle}",
                f"- max_failures: {eff_max_fail}",
                f"- tmux_session: {session} ({'up' if sess_up else 'down'})",
            ]
            if last_run:
                lines.append(f"- last_run_at: {last_run}")
            if last_candidate:
                lines.append(f"- last_candidate: {last_candidate}")
            if last_reason:
                lines.append(f"- last_reason: {_compact_reason(last_reason)}")
            if stuck_count and stuck_candidate:
                lines.append(f"- stuck: {stuck_count} ({stuck_candidate})")
            if fail_count:
                suffix = f" ({fail_candidate})" if fail_candidate else ""
                lines.append(f"- fail_count: {fail_count}{suffix}")
            if fail_reason:
                lines.append(f"- fail_reason: {_compact_reason(fail_reason)}")
                if status_level == "long" and _compact_reason(fail_reason) != fail_reason:
                    lines.append(f"- fail_reason_full: {fail_reason}")
            if last_prefetch_at:
                lines.append(f"- last_prefetch_at: {last_prefetch_at}")
            if last_prefetch_mode:
                lines.append(f"- last_prefetch_mode: {last_prefetch_mode}")
            if last_prefetch_reason:
                lines.append(f"- last_prefetch_reason: {_compact_reason(last_prefetch_reason)}")
            snapshot_lines = _focused_project_snapshot_lines(manager_state)
            if status_level == "long" and snapshot_lines:
                lines.extend([""] + snapshot_lines)
            compact_lines = _ops_scope_compact_lines(manager_state, detail_level=status_level)
            if compact_lines:
                lines.extend(["", "ops projects:"] + compact_lines)
            lines.extend(
                [
                    "",
                    "set:",
                    "- /auto on",
                    "- /auto on fanout",
                    "- /auto on fanout recent",
                    "- /auto on fanout recent replace-sync",
                    "- /auto on fanout recent since 3h",
                    "- /auto off",
                    "- /auto on force",
                    "- /auto on maxfail=3",
                    "- /auto on <interval_sec> <idle_sec>",
                ]
            )
            send("\n".join(lines).strip(), context="auto-status", with_menu=True)
            return True

        if chat_role == "readonly":
            send(
                "permission denied: readonly chat cannot change auto scheduler.\n"
                "read-only: /auto (status only)",
                context="auto-deny",
                with_menu=True,
            )
            return True

        if sub in {"off", "stop"}:
            current["enabled"] = False
            current["chat_id"] = str(current.get("chat_id", "")).strip() or str(chat_id)
            current["stopped_at"] = _now_iso()
            if not args.dry_run:
                _save_auto_state(path, current)
            if args.dry_run:
                ok, out = True, "dry-run: skipped tmux auto off"
            else:
                ok, out = _tmux_auto_command(args, "off")
            send(
                "auto scheduler updated\n"
                "- enabled: no\n"
                f"- tmux: {'stopped' if ok else 'stop_failed'}\n"
                f"- detail: {out or '-'}",
                context="auto-off",
                with_menu=True,
            )
            return True

        # on/start
        effective_command = command if command in {"next", "fanout"} else str(current.get("command", "next")).strip().lower() or "next"
        if effective_command not in {"next", "fanout"}:
            effective_command = "next"
        if focus_row and effective_command == "fanout":
            send(
                "auto scheduler blocked\n"
                f"- project_lock: {focus_label}\n"
                "- reason: fanout is a global multi-project wave\n"
                "next:\n"
                "- /auto on next\n"
                "- /offdesk on\n"
                "- /focus off",
                context="auto-on-blocked",
                with_menu=True,
            )
            return True

        current["enabled"] = True
        current["chat_id"] = str(chat_id)
        if "started_at" not in current:
            current["started_at"] = _now_iso()
        current["command"] = effective_command
        if prefetch is not None:
            current["prefetch"] = prefetch
        elif "prefetch" not in current:
            current["prefetch"] = ""
        if replace_sync is not None:
            current["prefetch_replace_sync"] = bool(replace_sync)
        elif "prefetch_replace_sync" not in current:
            current["prefetch_replace_sync"] = False
        if prefetch_since is not None:
            current["prefetch_since"] = str(prefetch_since or "").strip()
        elif "prefetch_since" not in current:
            current["prefetch_since"] = ""
        if bool(current.get("prefetch_replace_sync", False)) and not _normalize_prefetch_token(current.get("prefetch", "")):
            current["prefetch"] = "sync_recent"
        if not _normalize_prefetch_token(current.get("prefetch", "")):
            current["prefetch_replace_sync"] = False
        if force:
            current["force"] = True
        elif "force" not in current:
            current["force"] = False
        if interval_sec is not None:
            current["interval_sec"] = interval_sec
        elif "interval_sec" not in current:
            current["interval_sec"] = DEFAULT_AUTO_INTERVAL_SEC
        if idle_sec is not None:
            current["idle_sec"] = idle_sec
        elif "idle_sec" not in current:
            current["idle_sec"] = DEFAULT_AUTO_IDLE_SEC
        if max_failures is not None:
            current["max_failures"] = int(max_failures)
        elif "max_failures" not in current:
            current["max_failures"] = DEFAULT_AUTO_MAX_FAILURES
        if not args.dry_run:
            _save_auto_state(path, current)

        if args.dry_run:
            ok, out = True, "dry-run: skipped tmux auto on"
        else:
            ok, out = _tmux_auto_command(args, "on")
        prefetch_token = _normalize_prefetch_token(current.get("prefetch", ""))
        replace_sync_enabled = bool(current.get("prefetch_replace_sync", False))
        body = (
            "auto scheduler updated\n"
            "- enabled: yes\n"
            f"- command: {str(current.get('command', 'next')).strip() or 'next'}\n"
            f"- prefetch: {_prefetch_display(prefetch_token, current.get('prefetch_since', ''), replace_sync_enabled)}\n"
            f"- force: {'yes' if bool(current.get('force', False)) else 'no'}\n"
            f"- interval_sec: {int(current.get('interval_sec') or DEFAULT_AUTO_INTERVAL_SEC)}\n"
            f"- idle_sec: {int(current.get('idle_sec') or DEFAULT_AUTO_IDLE_SEC)}\n"
            f"- tmux: {'started' if ok else 'start_failed'}\n"
            f"- detail: {out or '-'}\n"
        )
        if focus_row:
            body += f"- project_lock: {focus_label}\n"
        snapshot_lines = _focused_project_snapshot_lines(manager_state)
        if snapshot_lines:
            body += "\n" + "\n".join(snapshot_lines) + "\n"
        body += "next:\n- /queue\n- /auto status"
        send(body, context="auto-on", with_menu=True)
        return True

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
