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

from aoe_tg_orch_contract import derive_tf_phase, normalize_tf_phase
from aoe_tg_ops_policy import list_ops_projects, summarize_ops_scope
from aoe_tg_ops_view import (
    blocked_bucket_count,
    blocked_head_summary,
    compact_age_label,
    render_ops_scope_compact_lines,
    render_project_snapshot_lines,
)
from aoe_tg_package_paths import team_tmux_script
from aoe_tg_priority_actions import (
    offdesk_priority_action_snapshot,
    task_lane_target_snapshot,
)
from aoe_tg_project_runtime import project_runtime_issue, project_runtime_label
from aoe_tg_task_view import task_display_label
from aoe_tg_todo_policy import (
    normalize_proposal_kind,
    normalize_proposal_priority,
    priority_rank,
    proposal_confidence,
)
from aoe_tg_todo_state import preview_syncback_plan, sorted_open_proposals


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
    elif ("fallback:" in mode) or mode in {"recent_docs", "salvage_docs", "bootstrap_docs", "todo_files"}:
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


def _proposal_counter_summary(raw: Dict[str, int], *, order: Callable[[str], Any] | None = None) -> str:
    if not raw:
        return "-"
    rows = list(raw.items())
    if callable(order):
        rows.sort(key=lambda kv: (order(kv[0]), kv[0]))
    else:
        rows.sort(key=lambda kv: (-kv[1], kv[0]))
    return ", ".join(f"{key}={count}" for key, count in rows[:4])


def _proposal_triage_snapshot(entry: Dict[str, Any]) -> Dict[str, Any]:
    proposals = entry.get("todo_proposals") if isinstance(entry.get("todo_proposals"), list) else []
    open_rows = sorted_open_proposals(proposals)
    if not open_rows:
        return {
            "open_count": 0,
            "priority_counts": {},
            "kind_counts": {},
            "priority_summary": "-",
            "kind_summary": "-",
            "top_rows": [],
            "top_summary": "-",
            "high_priority": False,
        }

    priority_counts: Dict[str, int] = {}
    kind_counts: Dict[str, int] = {}
    top_rows: List[Dict[str, Any]] = []
    for row in open_rows:
        pr = normalize_proposal_priority(row.get("priority", "P2"))
        kind = normalize_proposal_kind(row.get("kind", "followup"))
        priority_counts[pr] = int(priority_counts.get(pr, 0)) + 1
        kind_counts[kind] = int(kind_counts.get(kind, 0)) + 1
    for row in open_rows[:2]:
        summary = " ".join(str(row.get("summary", "")).strip().split())
        if len(summary) > 72:
            summary = summary[:69].rstrip() + "..."
        top_rows.append(
            {
                "id": str(row.get("id", "")).strip() or "-",
                "priority": normalize_proposal_priority(row.get("priority", "P2")),
                "kind": normalize_proposal_kind(row.get("kind", "followup")),
                "confidence": proposal_confidence(row.get("confidence", 0.0)),
                "summary": summary or "-",
            }
        )
    top_summary_parts = []
    for row in top_rows:
        top_summary_parts.append(
            "{pid}[{priority} {kind} {conf:.2f}] {summary}".format(
                pid=row["id"],
                priority=row["priority"],
                kind=row["kind"],
                conf=float(row["confidence"]),
                summary=row["summary"],
            )
        )
    return {
        "open_count": len(open_rows),
        "priority_counts": priority_counts,
        "kind_counts": kind_counts,
        "priority_summary": _proposal_counter_summary(priority_counts, order=priority_rank),
        "kind_summary": _proposal_counter_summary(kind_counts),
        "top_rows": top_rows,
        "top_summary": " || ".join(top_summary_parts) if top_summary_parts else "-",
        "high_priority": bool(priority_counts.get("P1", 0)),
    }


def _normalize_task_status(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    if token in {"pending", "running", "completed", "failed"}:
        return token
    aliases = {
        "done": "completed",
        "complete": "completed",
        "success": "completed",
        "fail": "failed",
        "error": "failed",
        "active": "running",
        "in_progress": "running",
        "progress": "running",
    }
    return aliases.get(token, "pending")


def _compact_counter_summary(raw: Any) -> str:
    if not isinstance(raw, dict):
        return "-"
    rows: List[tuple[str, int]] = []
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
        rows.append((token, count))
    if not rows:
        return "-"
    rows.sort(key=lambda kv: (kv[0]))
    return ", ".join(f"{key}={count}" for key, count in rows)


def _latest_task_snapshot(entry: Dict[str, Any]) -> Dict[str, Any]:
    tasks = entry.get("tasks")
    if not isinstance(tasks, dict) or not tasks:
        return {}

    def sort_key(item: tuple[str, Dict[str, Any]]) -> tuple[int, str, str]:
        req_id, task = item
        if not isinstance(task, dict):
            return (0, "", str(req_id or ""))
        status = _normalize_task_status(task.get("status", "pending"))
        priority = {"running": 4, "pending": 3, "failed": 2, "completed": 1}.get(status, 0)
        updated = str(task.get("updated_at", "")).strip() or str(task.get("created_at", "")).strip()
        return (priority, updated, str(req_id or "").strip())

    best_req = ""
    best_task: Dict[str, Any] | None = None
    for req_id, task in tasks.items():
        if not isinstance(task, dict):
            continue
        if best_task is None or sort_key((req_id, task)) > sort_key((best_req, best_task)):
            best_req = str(req_id or "").strip()
            best_task = task
    if not isinstance(best_task, dict):
        return {}

    status = _normalize_task_status(best_task.get("status", "pending"))
    tf_phase = normalize_tf_phase(derive_tf_phase(best_task), "queued")
    label = task_display_label(best_task, fallback_request_id=best_req)
    plan = best_task.get("plan") if isinstance(best_task.get("plan"), dict) else {}
    meta = plan.get("meta") if isinstance(plan.get("meta"), dict) else {}
    exec_plan = meta.get("phase2_execution_plan") if isinstance(meta.get("phase2_execution_plan"), dict) else {}
    execution_lanes = exec_plan.get("execution_lanes") if isinstance(exec_plan.get("execution_lanes"), list) else []
    review_lanes = exec_plan.get("review_lanes") if isinstance(exec_plan.get("review_lanes"), list) else []
    lane_states = best_task.get("lane_states") if isinstance(best_task.get("lane_states"), dict) else {}
    lane_summary = lane_states.get("summary") if isinstance(lane_states.get("summary"), dict) else {}
    exec_summary = lane_summary.get("execution") if isinstance(lane_summary.get("execution"), dict) else {}
    review_summary = lane_summary.get("review") if isinstance(lane_summary.get("review"), dict) else {}
    review_verdicts = lane_summary.get("review_verdicts") if isinstance(lane_summary.get("review_verdicts"), dict) else {}
    lane_targets = task_lane_target_snapshot(best_task)
    rerun_exec = list(lane_targets.get("rerun_execution_lane_ids") or [])
    rerun_review = list(lane_targets.get("rerun_review_lane_ids") or [])
    manual_exec = list(lane_targets.get("manual_followup_execution_lane_ids") or [])
    manual_review = list(lane_targets.get("manual_followup_review_lane_ids") or [])
    result = best_task.get("result") if isinstance(best_task.get("result"), dict) else {}
    phase2_request_ids = result.get("phase2_request_ids") if isinstance(result.get("phase2_request_ids"), dict) else {}
    linked_request_ids = result.get("linked_request_ids") if isinstance(result.get("linked_request_ids"), list) else []

    def _request_bucket_count(value: Any) -> int:
        if isinstance(value, list):
            return len([str(item).strip() for item in value if str(item).strip()])
        if isinstance(value, str):
            return 1 if value.strip() else 0
        return 0

    requested_roles = [str(x).strip() for x in (result.get("requested_roles") or []) if str(x).strip()]
    executed_roles = [str(x).strip() for x in (result.get("executed_roles") or []) if str(x).strip()]
    dropped_roles = [str(x).strip() for x in (result.get("dropped_roles") or []) if str(x).strip()]
    added_roles = [str(x).strip() for x in (result.get("added_roles") or []) if str(x).strip()]
    degraded_by = [str(x).strip() for x in (result.get("degraded_by") or []) if str(x).strip()]
    rate_limit = best_task.get("rate_limit") if isinstance(best_task.get("rate_limit"), dict) else {}
    return {
        "request_id": best_req,
        "label": label,
        "status": status,
        "tf_phase": tf_phase,
        "execution_lane_count": len(execution_lanes),
        "review_lane_count": len(review_lanes),
        "execution_summary": dict(exec_summary),
        "review_summary": dict(review_summary),
        "review_verdicts": dict(review_verdicts),
        "rerun_execution_lane_ids": rerun_exec,
        "rerun_review_lane_ids": rerun_review,
        "manual_followup_execution_lane_ids": manual_exec,
        "manual_followup_review_lane_ids": manual_review,
        "lane_targets": lane_targets,
        "requested_roles": requested_roles,
        "executed_roles": executed_roles,
        "dropped_roles": dropped_roles,
        "added_roles": added_roles,
        "role_mismatch": bool(result.get("role_mismatch", False)),
        "degraded_by": degraded_by,
        "rate_limit": dict(rate_limit),
        "phase2_execution_request_count": _request_bucket_count(phase2_request_ids.get("execution")),
        "phase2_review_request_count": _request_bucket_count(phase2_request_ids.get("review")),
        "linked_request_count": len([str(item).strip() for item in linked_request_ids if str(item).strip()]),
        "phase2_parallelized": bool(result.get("phase2_parallelized", False)),
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
    proposal_triage = _proposal_triage_snapshot(entry)
    latest_task = _latest_task_snapshot(entry)
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
    attention: List[str] = []
    severity_score = 0
    status = "ready"
    if runtime_issue:
        status = "blocked"
        notes.append(f"runtime not ready: {runtime_label}")
        attention.append("runtime")
        severity_score += 100
    if not scenario_exists:
        status = "blocked"
        notes.append("missing .aoe-team/AOE_TODO.md")
        attention.append("bootstrap")
        severity_score += 90
    if not canonical_exists:
        status = "warn" if status == "ready" else status
        notes.append("missing canonical TODO.md")
        attention.append("bootstrap")
        severity_score += 70
    if canonical_exists and not include_ok:
        status = "warn" if status == "ready" else status
        notes.append("AOE_TODO.md does not include canonical TODO.md")
        attention.append("bootstrap")
        severity_score += 65
    if counts["open"] == 0 and counts["running"] == 0 and counts["blocked"] == 0 and open_proposals == 0:
        status = "blocked" if status == "ready" else status
        notes.append("no runnable backlog")
        attention.append("backlog:none")
        severity_score += 85
    if pending_flag:
        status = "warn" if status == "ready" else status
        notes.append("pending todo awaiting dispatch/approval")
        attention.append("pending")
        severity_score += 20
    if counts["running"] > 0:
        status = "warn" if status == "ready" else status
        notes.append("task already running")
        attention.append("running")
        severity_score += 15
    if counts["blocked"] > 0:
        status = "warn" if status == "ready" else status
        notes.append(f"blocked backlog present ({counts['blocked']})")
        attention.append(f"blocked:{counts['blocked']}")
        severity_score += 45
    if manual_followup_count > 0:
        status = "warn" if status == "ready" else status
        notes.append(f"manual follow-up backlog present ({manual_followup_count})")
        attention.append(f"followup:{manual_followup_count}")
        severity_score += 55
    task_tf_phase = str(latest_task.get("tf_phase", "")).strip()
    task_status = str(latest_task.get("status", "")).strip()
    if task_tf_phase in {"needs_retry", "manual_intervention", "blocked", "critic_review"}:
        status = "warn" if status == "ready" else status
        notes.append(f"active task needs attention ({task_tf_phase})")
        attention.append(f"task:{task_tf_phase}")
        severity_score += 60
    elif task_tf_phase == "rate_limited":
        status = "warn" if status == "ready" else status
        notes.append("active task is waiting for provider capacity")
        attention.append("task:rate_limited")
        severity_score += 40
    elif task_status in {"running", "pending"}:
        status = "warn" if status == "ready" else status
        notes.append(f"active task in progress ({task_tf_phase or task_status})")
        attention.append(f"task:{task_tf_phase or task_status}")
        severity_score += 15
    degraded_by = list(latest_task.get("degraded_by") or [])
    if degraded_by:
        status = "warn" if status == "ready" else status
        notes.append("active task degraded by " + ",".join(degraded_by))
        attention.append("task:degraded")
        severity_score += 10
    if bool(latest_task.get("role_mismatch", False)):
        status = "warn" if status == "ready" else status
        dropped = list(latest_task.get("dropped_roles") or [])
        added = list(latest_task.get("added_roles") or [])
        notes.append(
            "active task role mismatch (dropped={dropped} added={added})".format(
                dropped=",".join(dropped) if dropped else "-",
                added=",".join(added) if added else "-",
            )
        )
        attention.append("task:role_mismatch")
        severity_score += 35
    if open_proposals > 0:
        status = "warn" if status == "ready" else status
        notes.append(f"open todo proposals pending review ({open_proposals})")
        attention.append(f"proposals:{open_proposals}")
        severity_score += 25
        if bool(proposal_triage.get("high_priority", False)):
            notes.append(f"high-priority proposals pending review ({proposal_triage.get('priority_summary', '-')})")
            attention.append(f"proposal_p1:{proposal_triage.get('priority_summary', '-')}")
            severity_score += 20
    if syncback_pending:
        status = "warn" if status == "ready" else status
        notes.append(
            "syncback pending "
            f"(done={syncback_counts['done']} reopen={syncback_counts['reopen']} "
            f"append={syncback_counts['append']} blocked_notes={syncback_counts['blocked']})"
        )
        attention.append("syncback")
        severity_score += 15
    if syncback_error:
        status = "warn" if status == "ready" else status
        notes.append(f"syncback preview failed: {syncback_error}")
        attention.append("syncback:error")
        severity_score += 20
    if bool(sync_quality.get("warn")):
        status = "warn" if status == "ready" else status
        note = str(sync_quality.get("note", "")).strip()
        if note:
            notes.append(note)
        attention.append(f"sync:{sync_quality.get('quality', 'unknown')}")
        severity_score += 35
    if last_sync_mode == "never" or not last_sync_at:
        status = "warn" if status == "ready" else status
        notes.append("queue has not been synced yet")
        attention.append("sync:never")
        severity_score += 30
    elif sync_stale:
        status = "warn" if status == "ready" else status
        notes.append(f"last sync is stale ({last_sync_disp})")
        attention.append(f"sync:stale:{last_sync_disp}")
        severity_score += 25

    dedup_attention: List[str] = []
    seen_attention: set[str] = set()
    for item in attention:
        token = str(item or "").strip()
        if not token or token in seen_attention:
            continue
        seen_attention.add(token)
        dedup_attention.append(token)
    attention_summary = ", ".join(dedup_attention[:4]) if dedup_attention else "-"

    bootstrap_recommended = (
        (not canonical_exists)
        or (canonical_exists and not include_ok)
        or (last_sync_mode == "never")
        or bool(sync_quality.get("warn", False))
        or bool(sync_stale)
    )
    priority_action = offdesk_priority_action_snapshot(
        alias=alias,
        active_task_label=str(latest_task.get("label", "")).strip(),
        active_task_tf_phase=task_tf_phase,
        active_task_targets=latest_task.get("lane_targets") if isinstance(latest_task.get("lane_targets"), dict) else None,
        syncback_pending=syncback_pending,
        followup_count=manual_followup_count,
        proposal_count=open_proposals,
        bootstrap_recommended=bootstrap_recommended,
        blocked_count=counts["blocked"],
        open_count=counts["open"],
        sync_quality=str(sync_quality.get("quality", "")).strip(),
        sync_quality_warn=bool(sync_quality.get("warn", False)),
        sync_stale=bool(sync_stale),
        canonical_exists=bool(canonical_exists),
        include_ok=bool(include_ok),
        last_sync_mode=last_sync_mode,
    )

    lines = [
        f"- {alias} {display} [{status}]",
        f"  attention: {attention_summary}",
        f"  first: {priority_action.get('action', '-')} | {priority_action.get('reason', '-')}",
        f"  runtime: {runtime_label}",
        f"  canonical: {canonical_rel if canonical_exists else 'missing TODO.md'}",
        f"  scenario_include: {include_display}",
        f"  queue: open={counts['open']} running={counts['running']} blocked={counts['blocked']} followup={manual_followup_count} pending={'yes' if pending_flag else 'no'} proposals={open_proposals}",
        f"  proposal_triage: priorities={proposal_triage.get('priority_summary', '-')} | kinds={proposal_triage.get('kind_summary', '-')}",
        f"  syncback: done={syncback_counts['done']} reopen={syncback_counts['reopen']} append={syncback_counts['append']} blocked_notes={syncback_counts['blocked']}",
        f"  last_sync: {last_sync_mode} {last_sync_disp}".rstrip(),
        "  sync_source: "
        f"{sync_quality.get('quality', 'unknown')} "
        f"classes={sync_quality.get('classes_summary', '-')} "
        f"doc_types={sync_quality.get('doc_types_summary', '-')}".rstrip(),
    ]
    if int(proposal_triage.get("open_count", 0) or 0) > 0:
        lines.append(f"  proposal_top: {proposal_triage.get('top_summary', '-')}")
    if latest_task:
        lane_parts = [f"lanes E{int(latest_task.get('execution_lane_count', 0) or 0)}/R{int(latest_task.get('review_lane_count', 0) or 0)}"]
        exec_summary_disp = _compact_counter_summary(latest_task.get("execution_summary"))
        review_summary_disp = _compact_counter_summary(latest_task.get("review_summary"))
        verdict_disp = _compact_counter_summary(latest_task.get("review_verdicts"))
        if exec_summary_disp != "-":
            lane_parts.append(f"exec {exec_summary_disp}")
        if review_summary_disp != "-":
            lane_parts.append(f"review {review_summary_disp}")
        if verdict_disp != "-":
            lane_parts.append(f"review_verdict {verdict_disp}")
        lines.append(
            f"  active_task: {latest_task.get('label', '-')} | {latest_task.get('status', '-')}/{latest_task.get('tf_phase', '-')}"
        )
        if latest_task.get("requested_roles") or latest_task.get("executed_roles"):
            lines.append(
                "  active_task_roles: requested={requested} | executed={executed}".format(
                    requested=", ".join(latest_task.get("requested_roles") or []) or "-",
                    executed=", ".join(latest_task.get("executed_roles") or []) or "-",
                )
            )
        if bool(latest_task.get("role_mismatch", False)):
            lines.append(
                "  active_task_role_mismatch: dropped={dropped} added={added}".format(
                    dropped=",".join(latest_task.get("dropped_roles") or []) or "-",
                    added=",".join(latest_task.get("added_roles") or []) or "-",
                )
            )
        if degraded_by:
            lines.append("  active_task_degraded_by: " + ",".join(degraded_by))
        active_rate_limit = latest_task.get("rate_limit") if isinstance(latest_task.get("rate_limit"), dict) else {}
        if active_rate_limit:
            providers = [str(x).strip() for x in (active_rate_limit.get("limited_providers") or []) if str(x).strip()]
            retry_after = int(active_rate_limit.get("retry_after_sec", 0) or 0)
            lines.append(
                "  active_task_rate_limit: mode={mode} providers={providers} retry_after={retry}".format(
                    mode=str(active_rate_limit.get("mode", "")).strip() or "-",
                    providers=",".join(providers) if providers else "-",
                    retry=(f"{retry_after}s" if retry_after > 0 else "-"),
                )
            )
        lines.append("  active_task_lanes: " + " | ".join(lane_parts))
        exec_request_count = int(latest_task.get("phase2_execution_request_count", 0) or 0)
        review_request_count = int(latest_task.get("phase2_review_request_count", 0) or 0)
        linked_request_count = int(latest_task.get("linked_request_count", 0) or 0)
        if exec_request_count or review_request_count or linked_request_count or bool(latest_task.get("phase2_parallelized", False)):
            lines.append(
                "  active_task_requests: execution={execution} review={review} linked={linked} parallel={parallel}".format(
                    execution=exec_request_count,
                    review=review_request_count,
                    linked=linked_request_count,
                    parallel="yes" if bool(latest_task.get("phase2_parallelized", False)) else "no",
                )
            )
        rerun_exec = list(latest_task.get("rerun_execution_lane_ids") or [])
        rerun_review = list(latest_task.get("rerun_review_lane_ids") or [])
        manual_exec = list(latest_task.get("manual_followup_execution_lane_ids") or [])
        manual_review = list(latest_task.get("manual_followup_review_lane_ids") or [])
        if rerun_exec or rerun_review:
            lines.append(
                "  active_task_rerun: execution={exec_ids} review={review_ids}".format(
                    exec_ids=",".join(rerun_exec) if rerun_exec else "-",
                    review_ids=",".join(rerun_review) if rerun_review else "-",
                )
            )
        if manual_exec or manual_review:
            lines.append(
                "  active_task_followup: execution={exec_ids} review={review_ids}".format(
                    exec_ids=",".join(manual_exec) if manual_exec else "-",
                    review_ids=",".join(manual_review) if manual_review else "-",
                )
            )
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
        "proposal_triage": dict(proposal_triage),
        "syncback_pending": syncback_pending,
        "syncback_counts": dict(syncback_counts),
        "pending_flag": pending_flag,
        "active_task_request_id": str(latest_task.get("request_id", "")).strip(),
        "active_task_label": str(latest_task.get("label", "")).strip(),
        "active_task_tf_phase": str(latest_task.get("tf_phase", "")).strip(),
        "active_task_status": str(latest_task.get("status", "")).strip(),
        "bootstrap_recommended": bootstrap_recommended,
        "sync_quality": str(sync_quality.get("quality", "")).strip(),
        "sync_quality_warn": bool(sync_quality.get("warn", False)),
        "sync_candidate_classes": dict(sync_quality.get("candidate_classes") or {}),
        "sync_candidate_doc_types": dict(sync_quality.get("candidate_doc_types") or {}),
        "severity_score": int(severity_score),
        "attention_summary": attention_summary,
        "priority_action": str(priority_action.get("action", "")).strip(),
        "priority_reason": str(priority_action.get("reason", "")).strip(),
        "notes": list(notes),
    }


def sort_offdesk_reports(reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def _status_rank(row: Dict[str, Any]) -> int:
        status = str(row.get("status", "ready")).strip().lower()
        return {"blocked": 0, "warn": 1, "ready": 2}.get(status, 9)

    def _alias_rank(row: Dict[str, Any]) -> int:
        return alias_index(str(row.get("alias", "")).strip() or str(row.get("display", "")))

    return sorted(
        [row for row in reports if isinstance(row, dict)],
        key=lambda row: (
            _status_rank(row),
            -int(row.get("severity_score", 0) or 0),
            _alias_rank(row),
            str(row.get("display", "")),
        ),
    )


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
        tertiary: List[Dict[str, str]] = []
        priority_action = str(row.get("priority_action", "")).strip()
        if priority_action:
            primary.append({"text": priority_action})
        if bool(row.get("syncback_pending", False)):
            primary.append({"text": f"/todo {alias} syncback preview"})
        if int(row.get("proposals", 0) or 0) > 0:
            primary.append({"text": f"/todo {alias} proposals"})
        if int(row.get("followup_count", 0) or 0) > 0:
            primary.append({"text": f"/todo {alias} followup"})
        if primary:
            dedup_primary: List[Dict[str, str]] = []
            seen_primary: set[str] = set()
            for btn in primary:
                text = str(btn.get("text", "")).strip()
                if not text or text in seen_primary:
                    continue
                seen_primary.add(text)
                dedup_primary.append(btn)
            for idx in range(0, len(dedup_primary), 3):
                keyboard.append(dedup_primary[idx : idx + 3])

        active_task_label = str(row.get("active_task_label", "")).strip()
        active_task_tf_phase = str(row.get("active_task_tf_phase", "")).strip()
        if active_task_label and active_task_tf_phase in {"needs_retry", "manual_intervention", "critic_review", "blocked", "rate_limited"}:
            tertiary.append({"text": f"/task {active_task_label}"})

        if int(row.get("blocked_count", 0) or 0) > 0 or int(row.get("open", 0) or 0) == 0:
            secondary.append({"text": f"/sync preview {alias} 24h"})
        if bool(row.get("bootstrap_recommended", False)):
            secondary.append({"text": f"/sync bootstrap {alias} 24h"})
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
            for idx in range(0, len(deduped_secondary), 3):
                keyboard.append(deduped_secondary[idx : idx + 3])
        if tertiary:
            keyboard.append(tertiary[:3])

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
        tertiary: List[Dict[str, str]] = []
        priority_action = str(row.get("priority_action", "")).strip()
        if priority_action:
            primary.append({"text": priority_action})

        if bool(row.get("syncback_pending", False)):
            primary.append({"text": f"/todo {alias} syncback preview"})
        if int(row.get("proposals", 0) or 0) > 0:
            primary.append({"text": f"/todo {alias} proposals"})
        if int(row.get("followup_count", 0) or 0) > 0:
            primary.append({"text": f"/todo {alias} followup"})
        if primary:
            dedup_primary: List[Dict[str, str]] = []
            seen_primary: set[str] = set()
            for btn in primary:
                text = str(btn.get("text", "")).strip()
                if not text or text in seen_primary:
                    continue
                seen_primary.add(text)
                dedup_primary.append(btn)
            for idx in range(0, len(dedup_primary), 3):
                keyboard.append(dedup_primary[idx : idx + 3])

        active_task_label = str(row.get("active_task_label", "")).strip()
        active_task_tf_phase = str(row.get("active_task_tf_phase", "")).strip()
        if active_task_label and active_task_tf_phase in {"needs_retry", "manual_intervention", "critic_review", "blocked", "rate_limited"}:
            tertiary.append({"text": f"/task {active_task_label}"})

        if bool(row.get("bootstrap_recommended", False)):
            secondary.append({"text": f"/sync bootstrap {alias} 24h"})
        secondary.append({"text": f"/sync preview {alias} 24h"})
        secondary.append({"text": f"/orch status {alias}"})
        secondary.append({"text": f"/todo {alias}"})
        for idx in range(0, len(secondary), 3):
            keyboard.append(secondary[idx : idx + 3])
        if tertiary:
            keyboard.append(tertiary[:3])

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
