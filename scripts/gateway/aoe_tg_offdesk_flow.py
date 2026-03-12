#!/usr/bin/env python3
"""Offdesk/auto helper flow extracted from management handlers."""

from __future__ import annotations

import json
import os
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from aoe_tg_ops_policy import list_ops_projects, summarize_ops_scope
from aoe_tg_ops_view import (
    blocked_bucket_count,
    blocked_head_summary,
    compact_age_label,
    render_ops_scope_compact_lines,
    render_project_snapshot_lines,
)
from aoe_tg_package_paths import team_tmux_script
from aoe_tg_project_runtime import project_runtime_issue, project_runtime_label
from aoe_tg_todo_state import preview_syncback_plan


def cmd_prefix() -> str:
    raw = str(os.environ.get("AOE_TG_COMMAND_PREFIXES", "/") or "/").strip()
    for ch in raw:
        if ch in {"/", "!"}:
            return ch
    return "/"


def normalize_prefetch_token(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    if token in {"recent", "recent_docs", "sync-recent"}:
        token = "sync_recent"
    return token if token in {"sync_recent"} else ""


def parse_replace_sync_flag(tokens: List[str]) -> Optional[bool]:
    result: Optional[bool] = None
    for tok in tokens:
        low = str(tok or "").strip().lower()
        if low in {"replace-sync", "sync-replace", "replace_prefetch", "prefetch-replace"}:
            result = True
        elif low in {"no-replace-sync", "safe-sync", "no-sync-replace"}:
            result = False
    return result


def prefetch_display(prefetch: Any, prefetch_since: Any, replace_sync: bool) -> str:
    token = normalize_prefetch_token(prefetch)
    since_disp = str(prefetch_since or "").strip() or "-"
    if token == "sync_recent" and replace_sync:
        return "sync_recent+replace (full-scope; since ignored)"
    if token == "sync_recent":
        return f"sync_recent+salvage (since={since_disp})"
    return "-"


def compact_reason(raw: Any, limit: int = 120) -> str:
    text = " ".join(str(raw or "").strip().split())
    if len(text) > limit:
        return text[: max(0, limit - 3)].rstrip() + "..."
    return text


def _sync_counter_map(raw: Any) -> Dict[str, int]:
    if not isinstance(raw, dict):
        return {}
    counts: Dict[str, int] = {}
    for key, value in raw.items():
        token = str(key or "").strip().lower()
        if not token:
            continue
        try:
            count = int(value or 0)
        except Exception:
            continue
        if count <= 0:
            continue
        counts[token] = count
    ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return {key: count for key, count in ordered[:6]}


def _sync_counter_summary(raw: Any) -> str:
    counts = _sync_counter_map(raw)
    if not counts:
        return "-"
    return ", ".join(f"{key}={count}" for key, count in counts.items())


def _sync_quality_snapshot(entry: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(entry.get("last_sync_mode", "")).strip() or "never"
    classes = _sync_counter_map(entry.get("last_sync_candidate_classes"))
    doc_types = _sync_counter_map(entry.get("last_sync_candidate_doc_types"))
    has_backlog_docs = any(key in {"todo", "handoff"} for key in doc_types)
    non_backlog_docs = [key for key in doc_types if key not in {"todo", "handoff"}]
    quality = "unknown"
    note = ""
    warn = False

    if mode == "never":
        quality = "never"
    elif mode == "scenario" and has_backlog_docs and not non_backlog_docs:
        quality = "canonical"
    elif ("fallback:" in mode) or mode in {"recent_docs", "salvage_docs", "todo_files"}:
        quality = "discovery"
        warn = True
        note = f"last sync used non-canonical discovery mode ({mode})"
    elif doc_types and not has_backlog_docs:
        quality = "non_backlog_docs"
        warn = True
        note = "last sync built backlog from non-backlog documents"
    elif has_backlog_docs and non_backlog_docs:
        quality = "mixed"
    elif has_backlog_docs:
        quality = "backlog_docs"

    return {
        "quality": quality,
        "warn": warn,
        "note": note,
        "candidate_classes": classes,
        "candidate_doc_types": doc_types,
        "classes_summary": _sync_counter_summary(classes),
        "doc_types_summary": _sync_counter_summary(doc_types),
    }


def status_report_level(tokens: List[str], fallback: str) -> str:
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


def focused_project_entry(
    manager_state: Dict[str, Any],
    *,
    project_lock_row: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> Tuple[str, Dict[str, Any], bool]:
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    if not isinstance(projects, dict) or not projects:
        return "", {}, False
    row = project_lock_row(manager_state)
    locked = bool(row)
    key = str(row.get("project_key", "")).strip().lower()
    if not key:
        key = str(manager_state.get("active", "default") or "default").strip().lower()
    entry = projects.get(key)
    if not isinstance(entry, dict):
        return "", {}, locked
    return key, entry, locked


def focused_project_snapshot_lines(
    manager_state: Dict[str, Any],
    *,
    project_lock_row: Callable[[Dict[str, Any]], Dict[str, Any]],
) -> List[str]:
    key, entry, locked = focused_project_entry(manager_state, project_lock_row=project_lock_row)
    if not key or not entry:
        return []
    return render_project_snapshot_lines(key=key, entry=entry, locked=locked)


def ops_scope_summary(manager_state: Dict[str, Any]) -> Dict[str, List[str]]:
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    return summarize_ops_scope(projects)


def ops_scope_compact_lines(manager_state: Dict[str, Any], *, limit: int = 4, detail_level: str = "short") -> List[str]:
    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    return render_ops_scope_compact_lines(projects, limit=limit, detail_level=detail_level)


def canonical_todo_path(entry: Dict[str, Any]) -> Path:
    root = Path(str(entry.get("project_root", "")).strip() or ".").expanduser()
    return (root / "TODO.md").resolve()


def scenario_path(entry: Dict[str, Any]) -> Path:
    root = Path(str(entry.get("project_root", "")).strip() or ".").expanduser()
    return (root / ".aoe-team" / "AOE_TODO.md").resolve()


def scenario_include_targets(entry: Dict[str, Any], *, include_prefix: str = "@include") -> List[Tuple[str, bool]]:
    path = scenario_path(entry)
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8")
    except Exception:
        return []
    out: List[Tuple[str, bool]] = []
    for raw_line in text.splitlines():
        stripped = str(raw_line or "").strip()
        if not stripped.lower().startswith(include_prefix):
            continue
        payload = stripped[len(include_prefix) :].strip()
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


def parse_iso_datetime(raw: str) -> Optional[datetime]:
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


def alias_index(alias: str) -> int:
    token = str(alias or "").strip().upper()
    if token.startswith("O"):
        token = token[1:]
    return int(token) if token.isdigit() else 10**9


def offdesk_prepare_targets(
    manager_state: Dict[str, Any],
    raw_target: str,
    *,
    project_lock_row: Callable[[Dict[str, Any]], Dict[str, Any]],
    resolve_project_entry: Callable[[Dict[str, Any], str], Tuple[str, Dict[str, Any]]],
) -> List[Tuple[str, Dict[str, Any]]]:
    token = str(raw_target or "").strip()
    locked = project_lock_row(manager_state)
    if token:
        if token.lower() == "all":
            if locked:
                key = str(locked.get("project_key", "")).strip().lower()
                projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
                entry = projects.get(key) if isinstance(projects, dict) else None
                return [(key, entry)] if isinstance(entry, dict) else []
            projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
            rows = list_ops_projects(projects)
            rows.sort(key=lambda kv: alias_index(str(kv[1].get("project_alias", "")).strip() or str(kv[0])))
            return rows
        key, entry = resolve_project_entry(manager_state, token)
        return [(key, entry)]

    if locked:
        key = str(locked.get("project_key", "")).strip().lower()
        projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
        entry = projects.get(key) if isinstance(projects, dict) else None
        return [(key, entry)] if isinstance(entry, dict) else []

    projects = manager_state.get("projects") if isinstance(manager_state, dict) else {}
    rows = list_ops_projects(projects)
    rows.sort(key=lambda kv: alias_index(str(kv[1].get("project_alias", "")).strip() or str(kv[0])))
    return rows


def offdesk_prepare_project_report(manager_state: Dict[str, Any], key: str, entry: Dict[str, Any]) -> Dict[str, Any]:
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
    canonical_path = canonical_todo_path(entry)
    canonical_exists = canonical_path.exists()
    scenario = scenario_path(entry)
    scenario_exists = scenario.exists()
    includes = scenario_include_targets(entry)
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
    last_sync_disp = compact_age_label(last_sync_at)
    sync_quality = _sync_quality_snapshot(entry)
    last_sync_dt = parse_iso_datetime(last_sync_at)
    sync_stale = False
    if last_sync_dt is not None:
        now = datetime.now(last_sync_dt.tzinfo or timezone.utc)
        try:
            sync_stale = (now - last_sync_dt).total_seconds() > 24 * 3600
        except Exception:
            sync_stale = False
    manual_followup_count = blocked_bucket_count(todos, "manual_followup")
    blocked_head = blocked_head_summary(todos)
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
    if bool(sync_quality.get("warn")):
        status = "warn" if status == "ready" else status
        note = str(sync_quality.get("note", "")).strip()
        if note:
            notes.append(note)
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
        "  sync_source: "
        f"{sync_quality.get('quality', 'unknown')} "
        f"classes={sync_quality.get('classes_summary', '-')} "
        f"doc_types={sync_quality.get('doc_types_summary', '-')}".rstrip(),
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
        "sync_quality": str(sync_quality.get("quality", "")).strip(),
        "sync_quality_warn": bool(sync_quality.get("warn", False)),
        "sync_candidate_classes": dict(sync_quality.get("candidate_classes") or {}),
        "sync_candidate_doc_types": dict(sync_quality.get("candidate_doc_types") or {}),
        "notes": list(notes),
    }


def offdesk_review_reply_markup(flagged: List[Dict[str, Any]], *, clean: bool = False) -> Dict[str, Any]:
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


def offdesk_prepare_reply_markup(
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


def now_iso() -> str:
    import time as _time

    return _time.strftime("%Y-%m-%dT%H:%M:%S%z")


def auto_state_path(args: Any, *, filename: str) -> Path:
    return Path(str(getattr(args, "team_dir", "."))).expanduser().resolve() / filename


def offdesk_state_path(args: Any, *, filename: str) -> Path:
    return Path(str(getattr(args, "team_dir", "."))).expanduser().resolve() / filename


def load_auto_state(path: Path) -> Dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_auto_state(path: Path, state: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(state)
    payload["updated_at"] = now_iso()
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def load_offdesk_state(path: Path) -> Dict[str, Any]:
    return load_auto_state(path)


def save_offdesk_state(path: Path, state: Dict[str, Any]) -> None:
    save_auto_state(path, state)


def scheduler_session_name() -> str:
    return (os.environ.get("AOE_TMUX_SCHEDULER_SESSION") or "aoe_mo_scheduler").strip() or "aoe_mo_scheduler"


def tmux_has_session(session_name: str) -> bool:
    token = str(session_name or "").strip()
    if not token:
        return False
    try:
        proc = subprocess.run(["tmux", "has-session", "-t", token], capture_output=True, text=True, check=False)
        return proc.returncode == 0
    except Exception:
        return False


def tmux_auto_command(args: Any, action: str) -> Tuple[bool, str]:
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
    except Exception as exc:
        return False, str(exc)
    out = (proc.stdout or proc.stderr or "").strip()
    return proc.returncode == 0, out
