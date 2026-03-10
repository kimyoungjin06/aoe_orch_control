#!/usr/bin/env python3
"""Auto-sync docs/investigations_mo artifacts from manager state."""

from __future__ import annotations

import io
import json
import os
import re
import shutil
import subprocess
import tarfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

DEFAULT_RUNLOG_EVENTS_LIMIT = 12
DEFAULT_RUNLOG_BYTES_LIMIT = 256 * 1024

DEFAULT_TF_DOC_MODE = "single"  # "legacy" keeps ongoing/note/handoff scaffolds
DEFAULT_TF_ARTIFACT_POLICY = "success-only"  # "success-only" | "all" | "none"

AUTOGEN_TODO_QUEUE_BEGIN = "<!-- AOE_AUTOGEN_TODO_QUEUE:BEGIN -->"
AUTOGEN_TODO_QUEUE_END = "<!-- AOE_AUTOGEN_TODO_QUEUE:END -->"


def _now_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = text.rstrip("\n") + "\n"
    try:
        if path.exists() and path.read_text(encoding="utf-8") == payload:
            return
    except Exception:
        pass
    path.write_text(payload, encoding="utf-8")


def _tf_doc_mode() -> str:
    token = str(os.environ.get("AOE_TF_DOC_MODE", DEFAULT_TF_DOC_MODE) or "").strip().lower()
    if token in {"legacy", "scaffold", "multi"}:
        return "legacy"
    return "single"


def _tf_artifact_policy() -> str:
    token = str(os.environ.get("AOE_TF_ARTIFACT_POLICY", DEFAULT_TF_ARTIFACT_POLICY) or "").strip().lower()
    if token in {"all", "keep-all"}:
        return "all"
    if token in {"none", "off"}:
        return "none"
    return "success-only"


def _normalize_key(raw: Any) -> str:
    return str(raw or "").strip().lower()


def _sanitize_folder(raw: Any, fallback: str) -> str:
    token = re.sub(r"[^A-Za-z0-9._-]+", "_", str(raw or "").strip()).strip("._-")
    return token or fallback


def _normalize_project_alias(raw: Any, fallback: str) -> str:
    token = str(raw or "").strip().upper()
    if re.fullmatch(r"O[1-9]\d{0,2}", token):
        return token
    if re.fullmatch(r"[1-9]\d{0,2}", token):
        return f"O{token}"
    return _sanitize_folder(fallback, "default")


def _normalize_status(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    if token in {"pending", "running", "completed", "failed"}:
        return token
    aliases = {
        "done": "completed",
        "complete": "completed",
        "success": "completed",
        "error": "failed",
        "active": "running",
        "in_progress": "running",
    }
    return aliases.get(token, "pending")


def _load_tf_exec_map(team_dir: Path) -> Dict[str, Any]:
    path = team_dir / "tf_exec_map.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _tf_exec_meta_line(exec_meta: Dict[str, Any], key: str) -> str:
    val = str(exec_meta.get(key, "") or "").strip()
    return val or "-"


def _task_short_id(task: Dict[str, Any], req_id: str) -> str:
    short = str(task.get("short_id", "")).strip().upper()
    if short:
        return short
    token = re.sub(r"[^A-Za-z0-9]+", "", str(req_id or "").upper())[:8]
    return f"T-{token or 'UNK'}"


def _tf_id_from_task(task: Dict[str, Any], req_id: str) -> str:
    short = _task_short_id(task, req_id)
    tf = re.sub(r"^T-", "TF-", short)
    if tf.startswith("TF-"):
        return tf
    token = re.sub(r"[^A-Za-z0-9]+", "", str(tf or ""))[:12]
    return f"TF-{token or 'UNK'}"


def _task_objective(task: Dict[str, Any]) -> str:
    text = str(task.get("prompt", "")).replace("\n", " ").strip()
    if not text:
        return "-"
    return text[:77] + "..." if len(text) > 80 else text


def _sorted_tasks(tasks: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    rows: List[Tuple[str, Dict[str, Any]]] = []
    for req_id, task in (tasks or {}).items():
        if not isinstance(task, dict):
            continue
        rid = str(req_id or "").strip()
        if not rid:
            continue
        rows.append((rid, task))
    rows.sort(
        key=lambda kv: (
            str((kv[1] or {}).get("updated_at", "")),
            str((kv[1] or {}).get("created_at", "")),
        ),
        reverse=True,
    )
    return rows


def _pick_active_request_id(entry: Dict[str, Any], tasks: Dict[str, Any]) -> str:
    last_req = str(entry.get("last_request_id", "")).strip()
    if last_req and last_req in tasks:
        return last_req
    rows = _sorted_tasks(tasks)
    for req_id, task in rows:
        if _normalize_status(task.get("status")) in {"running", "pending"}:
            return req_id
    return rows[0][0] if rows else ""


def _ensure_project_tf_scaffold(root: Path, project_alias: str, tf_id: str, *, tf_doc_mode: str) -> None:
    project_dir = root / "projects" / project_alias
    tf_dir = project_dir / "tfs" / tf_id
    for path in [root / "registry", project_dir / "archive", tf_dir]:
        path.mkdir(parents=True, exist_ok=True)

    defaults: Dict[Path, str] = {
        root / "README.md": (
            "# Multi-Agent Investigations\n\n"
            "Auto-managed workspace for project and TF context.\n"
        ),
        project_dir / "ongoing.md": (
            f"# {project_alias} Project Ongoing\n\n"
            "## Objective\n"
            "- __fill project objective__\n\n"
            "## Todo Queue\n"
            f"{AUTOGEN_TODO_QUEUE_BEGIN}\n"
            "| todo_id | summary | priority | status | current_tf | next action |\n"
            "|---|---|---|---|---|---|\n"
            "| - | (empty) | - | - | - | add with `/todo add <summary>` |\n"
            f"{AUTOGEN_TODO_QUEUE_END}\n\n"
            "## Notes\n"
            "- project-level constraints and policy notes only\n"
        ),
        project_dir / "note.md": (
            f"# {project_alias} Project Notes\n\n"
            "## Accepted Project Decisions\n"
        ),
        project_dir / "archive" / "README.md": (
            f"# {project_alias} Project Archive\n\n"
            "Store project-level deferred/cancelled artifacts.\n"
        ),
    }
    if tf_doc_mode == "legacy":
        (tf_dir / "archive").mkdir(parents=True, exist_ok=True)
        defaults.update(
            {
                tf_dir / "ongoing.md": (
                    f"# {tf_id} TF Ongoing\n\n"
                    "## Objective\n"
                    "- __fill tf objective__\n\n"
                    "## Plan\n"
                    "- step1: __fill__\n"
                    "- step2: __fill__\n"
                ),
                tf_dir / "note.md": (
                    f"# {tf_id} TF Notes\n\n"
                    "## Accepted TF Decisions\n"
                ),
                tf_dir / "handoff.md": (
                    f"# {tf_id} Handoff\n\n"
                    "## Handoff Target\n"
                    "- next_tf: __fill or N/A__\n\n"
                    "## Context Package\n"
                    "- completed: __fill__\n"
                    "- open risks: __fill__\n"
                    "- required evidence: __fill__\n"
                    "- resume point: __fill__\n"
                ),
                tf_dir / "archive" / "README.md": (
                    f"# {tf_id} Archive\n\n"
                    "Store failed/deferred/blocked TF artifacts.\n"
                ),
            }
        )
    else:
        defaults.update(
            {
                tf_dir / "report.md": (
                    f"# {tf_id} Report\n\n"
                    "- This file is auto-generated from `.aoe-team/orch_manager_state.json`.\n"
                    "- Policy: one TF keeps one report. Execution artifacts are retained on success only.\n"
                ),
            }
        )
    for path, body in defaults.items():
        if not path.exists():
            _write_text(path, body)


def _normalize_todo_status(raw: Any) -> str:
    token = str(raw or "").strip().lower()
    if token in {"open", "running", "blocked", "done", "canceled"}:
        return token
    aliases = {
        "pending": "open",
        "active": "running",
        "in_progress": "running",
        "completed": "done",
        "failed": "blocked",
        "cancelled": "canceled",
    }
    return aliases.get(token, "open")


def _normalize_todo_priority(raw: Any) -> str:
    token = str(raw or "").strip().upper()
    return token if token in {"P1", "P2", "P3"} else "P2"


def _todo_status_rank(status: str) -> int:
    token = _normalize_todo_status(status)
    if token == "running":
        return 0
    if token == "blocked":
        return 1
    if token == "open":
        return 2
    if token in {"done", "canceled"}:
        return 9
    return 8


def _todo_priority_rank(priority: str) -> int:
    token = _normalize_todo_priority(priority)
    return {"P1": 1, "P2": 2, "P3": 3}.get(token, 9)


def _sorted_todos(raw: Any) -> List[Dict[str, Any]]:
    todos: List[Dict[str, Any]] = []
    if isinstance(raw, list):
        for row in raw:
            if isinstance(row, dict):
                todos.append(row)
    todos.sort(
        key=lambda r: (
            _todo_status_rank(str(r.get("status", "open"))),
            _todo_priority_rank(str(r.get("priority", "P2"))),
            str(r.get("created_at", "")),
            str(r.get("id", "")),
        )
    )
    return todos


def _pick_task_for_todo(todo_id: str, tasks: Dict[str, Any]) -> Tuple[str, str]:
    token = str(todo_id or "").strip()
    if not token or not isinstance(tasks, dict):
        return "", ""
    hits: List[Tuple[int, str, str, Dict[str, Any]]] = []
    for req_id, task in tasks.items():
        if not isinstance(task, dict):
            continue
        if str(task.get("todo_id", "")).strip() != token:
            continue
        status = _normalize_status(task.get("status"))
        rank = 0 if status in {"running", "pending"} else 1
        updated = str(task.get("updated_at", "")).strip()
        hits.append((rank, updated, str(req_id), task))
    if not hits:
        return "", ""
    # Prefer active first (rank 0), then newest updated_at.
    hits.sort(key=lambda r: r[1], reverse=True)
    hits.sort(key=lambda r: r[0])
    _best_rank, _best_updated, best_req, best_task = hits[0]
    return _tf_id_from_task(best_task, best_req), _task_short_id(best_task, best_req)


def _render_project_todo_queue(entry: Dict[str, Any], tasks: Dict[str, Any]) -> str:
    todos = _sorted_todos(entry.get("todos"))
    lines: List[str] = []
    lines.append("| todo_id | summary | priority | status | current_tf | next action |")
    lines.append("|---|---|---|---|---|---|")
    if not todos:
        lines.append("| - | (empty) | - | - | - | add with `/todo add <summary>` |")
        return "\n".join(lines)

    emitted = 0
    for row in todos:
        todo_id = str(row.get("id", "")).strip() or "-"
        summary = str(row.get("summary", "")).strip().replace("\n", " ")
        if "|" in summary:
            summary = summary.replace("|", "/")
        if len(summary) > 72:
            summary = summary[:69] + "..."
        pr = _normalize_todo_priority(row.get("priority"))
        st = _normalize_todo_status(row.get("status"))
        tf_id, short_id = _pick_task_for_todo(todo_id, tasks)
        tf_cell = tf_id or "-"
        next_action = "-"
        if st == "open":
            next_action = "`/todo next` or `/next`"
        elif st == "running":
            next_action = f"`/check {short_id}`" if short_id else "`/check`"
        elif st == "blocked":
            reason = str(row.get("blocked_reason", "")).strip()
            if reason:
                reason = reason.replace("|", "/")
                if len(reason) > 60:
                    reason = reason[:57] + "..."
                next_action = f"unblock: {reason}"
            else:
                next_action = "unblock then retry"
        lines.append(f"| {todo_id} | {summary or '-'} | {pr} | {st} | {tf_cell} | {next_action} |")
        emitted += 1
        if emitted >= 20:
            break
    return "\n".join(lines)


def _upsert_project_todo_queue(path: Path, table_text: str) -> None:
    if not path.exists():
        return
    try:
        src = path.read_text(encoding="utf-8")
    except Exception:
        return

    if AUTOGEN_TODO_QUEUE_BEGIN in src and AUTOGEN_TODO_QUEUE_END in src:
        a = src.find(AUTOGEN_TODO_QUEUE_BEGIN)
        b = src.find(AUTOGEN_TODO_QUEUE_END)
        if a != -1 and b != -1 and a < b:
            head = src[:a].rstrip("\n")
            tail = src[b + len(AUTOGEN_TODO_QUEUE_END) :].lstrip("\n")
            payload = "\n".join(
                [
                    head,
                    AUTOGEN_TODO_QUEUE_BEGIN,
                    table_text.strip(),
                    AUTOGEN_TODO_QUEUE_END,
                    tail,
                ]
            ).rstrip("\n")
            _write_text(path, payload)
            return

    lines = src.splitlines()
    idx = -1
    for i, ln in enumerate(lines):
        if ln.strip() == "## Todo Queue":
            idx = i
            break
    if idx == -1:
        return

    j = len(lines)
    for k in range(idx + 1, len(lines)):
        if lines[k].startswith("## "):
            j = k
            break

    new_lines: List[str] = []
    new_lines.extend(lines[: idx + 1])
    new_lines.append(AUTOGEN_TODO_QUEUE_BEGIN)
    new_lines.extend(table_text.strip().splitlines())
    new_lines.append(AUTOGEN_TODO_QUEUE_END)
    new_lines.extend(lines[j:])
    _write_text(path, "\n".join(new_lines))


def _read_tail_bytes(path: Path, max_bytes: int) -> bytes:
    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            size = f.tell()
            take = min(int(max_bytes), max(0, size))
            if take <= 0:
                return b""
            f.seek(-take, 2)
            return f.read(take)
    except Exception:
        return b""


def _load_recent_gateway_events(path: Path, limit: int) -> List[Dict[str, Any]]:
    raw = _read_tail_bytes(path, DEFAULT_RUNLOG_BYTES_LIMIT)
    if not raw:
        return []
    text = raw.decode("utf-8", errors="replace")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    out: List[Dict[str, Any]] = []
    for ln in reversed(lines):
        try:
            row = json.loads(ln)
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
        if len(out) >= int(limit):
            break
    return list(reversed(out))


def _render_runlog_recent(
    *,
    events: List[Dict[str, Any]],
    active_project_alias: str,
    limit: int,
) -> str:
    lines = [
        "# Recent RunLog (Condensed)",
        "",
        "source: .aoe-team/logs/gateway_events.jsonl",
        "",
        "## Recent Items",
    ]
    if not events:
        lines.append("1. (empty)")
        lines.append("- no recent events")
        return "\n".join(lines)

    def _is_interesting(ev: str) -> bool:
        token = str(ev or "").strip().lower()
        return token in {
            "dispatch_completed",
            "dispatch_result",
            "dispatch_failed",
            "direct_reply",
            "handler_error",
            "rate_limited",
            "confirm_required",
            "exec_critic_retry",
            "exec_critic_blocked",
            "unauthorized_message",
        }

    picked: List[Dict[str, Any]] = []
    for row in reversed(events):
        if _is_interesting(row.get("event")):
            picked.append(row)
        if len(picked) >= int(limit):
            break
    if not picked:
        picked = events[-int(limit) :]
    picked = list(reversed(picked))

    for idx, row in enumerate(picked, start=1):
        ts = str(row.get("timestamp", "")).strip() or "-"
        project = str(row.get("project", "")).strip() or active_project_alias or "-"
        task_short = str(row.get("task_short_id", "")).strip()
        task_alias = str(row.get("task_alias", "")).strip()
        rid = str(row.get("request_id", "")).strip()
        stage = str(row.get("stage", "")).strip()
        status = str(row.get("status", "")).strip()
        event = str(row.get("event", "")).strip() or "event"
        err = str(row.get("error_code", "")).strip()
        detail = str(row.get("detail", "")).strip()

        task_label = ""
        if task_short and task_alias:
            task_label = f"{task_short} | {task_alias}"
        elif task_short:
            task_label = task_short
        elif task_alias:
            task_label = task_alias
        elif rid:
            task_label = rid
        else:
            task_label = "-"

        lines.append(f"{idx}. {task_label}")
        parts: List[str] = []
        parts.append(f"{ts} project={project} event={event}")
        if stage or status:
            parts.append(f"stage={stage or '-'} status={status or '-'}")
        if err:
            parts.append(f"error={err}")
        if detail:
            parts.append(f"detail={detail[:160]}")
        lines.append("- " + " | ".join(parts))
        lines.append("")

    return "\n".join(lines).rstrip()


def _render_project_registry(
    *,
    projects: Dict[str, Any],
    active_key: str,
) -> str:
    lines = [
        "# Project Registry",
        "",
        "| project_alias | purpose | status | ongoing_doc | note_doc |",
        "|---|---|---|---|---|",
    ]
    for key in sorted(projects.keys()):
        entry = projects.get(key) or {}
        if not isinstance(entry, dict):
            continue
        alias = _normalize_project_alias(entry.get("project_alias"), key)
        purpose = str(entry.get("overview", "")).strip() or str(entry.get("display_name", "")).strip() or key
        status = "active" if _normalize_key(key) == _normalize_key(active_key) else "inactive"
        ongoing_doc = f"docs/investigations_mo/projects/{alias}/ongoing.md"
        note_doc = f"docs/investigations_mo/projects/{alias}/note.md"
        lines.append(f"| {alias} | {purpose.replace('|', '/')} | {status} | `{ongoing_doc}` | `{note_doc}` |")
    if len(lines) == 4:
        lines.append("| - | - | - | - | - |")
    lines.extend(
        [
            "",
            "## Rules",
            "- active project is one in lock file",
            "- add new project row before first TF creation",
        ]
    )
    return "\n".join(lines)


def _render_project_lock(project_alias: str, tf_id: str, *, tf_doc_mode: str) -> str:
    tf_mode = str(tf_doc_mode or "").strip().lower()
    if tf_mode != "legacy":
        return "\n".join(
            [
                f"active_project: {project_alias}",
                f"active_tf: {tf_id}",
                f"effective_date: {_now_date()}",
                "owner: mother-orch",
                f'rationale: \"{project_alias} / {tf_id} is current highest-priority execution focus.\"',
                "",
                "runtime_source_of_truth:",
                "  - .aoe-team/orch_manager_state.json",
                "",
                "doc_source_of_truth:",
                "  - docs/investigations_mo/registry/project_lock.yaml",
                "",
                "active_paths:",
                f"  project_ongoing: docs/investigations_mo/projects/{project_alias}/ongoing.md",
                f"  project_note: docs/investigations_mo/projects/{project_alias}/note.md",
                f"  tf_report: docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/report.md",
                "",
                "entry_conditions:",
                '  - \"project objective is written\"',
                '  - \"TF objective is explicitly stated in report.md\"',
                "",
                "exit_conditions:",
                '  - \"critic accepted and integration completed\"',
                '  - \"or escalated with unblock conditions\"',
            ]
        )
    return "\n".join(
        [
            f"active_project: {project_alias}",
            f"active_tf: {tf_id}",
            f"effective_date: {_now_date()}",
            "owner: mother-orch",
            f'rationale: "{project_alias} / {tf_id} is current highest-priority execution focus."',
            "",
            "runtime_source_of_truth:",
            "  - .aoe-team/orch_manager_state.json",
            "",
            "doc_source_of_truth:",
            "  - docs/investigations_mo/registry/project_lock.yaml",
            "",
            "active_paths:",
            f"  project_ongoing: docs/investigations_mo/projects/{project_alias}/ongoing.md",
            f"  project_note: docs/investigations_mo/projects/{project_alias}/note.md",
            f"  tf_ongoing: docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/ongoing.md",
            f"  tf_note: docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/note.md",
            f"  tf_handoff: docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/handoff.md",
            "",
            "entry_conditions:",
            '  - "project and tf objective are written"',
            '  - "next action is explicitly assigned"',
            "",
            "exit_conditions:",
            '  - "critic accepted and integration completed"',
            '  - "or escalated with unblock conditions"',
        ]
    )


def _objective_from_text(raw: Any, max_len: int = 200) -> str:
    text = str(raw or "").strip()
    if not text:
        return "-"
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > int(max_len):
        return text[: max(0, int(max_len) - 3)] + "..."
    return text


def _plan_lines_from_task(task: Dict[str, Any], max_items: int = 9) -> List[str]:
    plan = task.get("plan") if isinstance(task, dict) else None
    if not isinstance(plan, dict):
        return []
    subtasks = plan.get("subtasks") or []
    if not isinstance(subtasks, list) or not subtasks:
        return []

    lines: List[str] = []
    for row in subtasks[: max(1, int(max_items))]:
        if not isinstance(row, dict):
            continue
        sid = str(row.get("id") or row.get("sid") or "").strip() or "S"
        title = str(row.get("title") or "").strip()
        goal = str(row.get("goal") or "").strip()
        role = str(row.get("owner_role") or "").strip() or "Worker"

        headline = title or goal or "-"
        suffix = ""
        if title and goal and title != goal:
            suffix = f": {goal}"
        lines.append(f"- {sid} [{role}] {headline}{suffix}")

    return lines


def _maybe_fill_placeholder_line(path: Path, placeholder_line: str, replacement_line: str) -> None:
    try:
        src = path.read_text(encoding="utf-8")
    except Exception:
        return
    if placeholder_line not in src:
        return
    out = src.replace(placeholder_line, replacement_line)
    if out != src:
        _write_text(path, out)


def _maybe_fill_tf_plan(path: Path, plan_lines: List[str]) -> None:
    if not plan_lines:
        return
    try:
        src = path.read_text(encoding="utf-8")
    except Exception:
        return
    placeholder = "- step1: __fill__\n- step2: __fill__"
    if placeholder not in src:
        return
    out = src.replace(placeholder, "\n".join(plan_lines))
    if out != src:
        _write_text(path, out)


def _maybe_fill_active_docs(
    *,
    docs_root: Path,
    project_alias: str,
    tf_id: str,
    project_objective: str,
    tf_objective: str,
    tf_plan_lines: List[str],
    tf_doc_mode: str,
) -> None:
    project_ongoing = docs_root / "projects" / project_alias / "ongoing.md"

    proj_obj = _objective_from_text(project_objective)
    if proj_obj != "-":
        _maybe_fill_placeholder_line(
            project_ongoing,
            "- __fill project objective__",
            f"- {proj_obj}",
        )

    if str(tf_doc_mode or "").strip().lower() == "legacy":
        tf_ongoing = docs_root / "projects" / project_alias / "tfs" / tf_id / "ongoing.md"
        tf_obj = _objective_from_text(tf_objective)
        if tf_obj != "-":
            _maybe_fill_placeholder_line(
                tf_ongoing,
                "- __fill tf objective__",
                f"- {tf_obj}",
            )
        _maybe_fill_tf_plan(tf_ongoing, tf_plan_lines)


def _render_tf_registry(project_alias: str, rows: Iterable[Tuple[str, Dict[str, Any]]], *, tf_doc_mode: str) -> str:
    tf_mode = str(tf_doc_mode or "").strip().lower()
    if tf_mode != "legacy":
        lines = [
            "# TF Registry",
            "",
            "| tf_id | project_alias | objective | status | exec_verdict | owner | created_at | closed_at | report_doc |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
        for req_id, task in rows:
            tf_id = _tf_id_from_task(task, req_id)
            status = _normalize_status(task.get("status"))
            created_at = str(task.get("created_at", "")).strip() or "-"
            updated_at = str(task.get("updated_at", "")).strip() or "-"
            closed_at = updated_at if status in {"completed", "failed"} else "-"
            owner = str(task.get("control_mode", "")).strip() or "project-orch"
            verdict, _reason = _exec_verdict_and_reason(task)
            objective = _task_objective(task).replace("|", "/")
            report_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/report.md"
            lines.append(
                f"| {tf_id} | {project_alias} | {objective} | {status} | {verdict} | {owner} | {created_at} | {closed_at} | `{report_doc}` |"
            )
        if len(lines) == 4:
            lines.append("| - | - | - | - | - | - | - | - | - |")
        lines.extend(
            [
                "",
                "## Status",
                "- `pending`, `running`, `completed`, `failed`",
                "- `exec_verdict`: `success`, `retry`, `fail` (when available)",
            ]
        )
        return "\n".join(lines)
    lines = [
        "# TF Registry",
        "",
        "| tf_id | project_alias | objective | status | exec_verdict | owner | created_at | closed_at | ongoing_doc | handoff_doc |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for req_id, task in rows:
        tf_id = _tf_id_from_task(task, req_id)
        status = _normalize_status(task.get("status"))
        created_at = str(task.get("created_at", "")).strip() or "-"
        updated_at = str(task.get("updated_at", "")).strip() or "-"
        closed_at = updated_at if status in {"completed", "failed"} else "-"
        owner = str(task.get("control_mode", "")).strip() or "project-orch"
        exec_critic = task.get("exec_critic") if isinstance(task, dict) else None
        exec_verdict = "-"
        if isinstance(exec_critic, dict):
            v = str(exec_critic.get("verdict", "")).strip().lower()
            if v in {"success", "retry", "fail"}:
                exec_verdict = v
        objective = _task_objective(task).replace("|", "/")
        ongoing_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/ongoing.md"
        handoff_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/handoff.md"
        lines.append(
            f"| {tf_id} | {project_alias} | {objective} | {status} | {exec_verdict} | {owner} | {created_at} | {closed_at} | `{ongoing_doc}` | `{handoff_doc}` |"
        )
    if len(lines) == 4:
        lines.append("| - | - | - | - | - | - | - | - | - | - |")
    lines.extend(
        [
            "",
            "## Status",
            "- `pending`, `running`, `completed`, `failed`",
            "- `exec_verdict`: `success`, `retry`, `fail` (when available)",
        ]
    )
    return "\n".join(lines)


def _resolve_source_request_id(task: Dict[str, Any]) -> str:
    for key in ("source_request_id", "retry_of", "replan_of"):
        token = str(task.get(key, "")).strip()
        if token:
            return token
    return ""


def _render_handoff_index(
    project_alias: str,
    rows: Iterable[Tuple[str, Dict[str, Any]]],
    task_map: Dict[str, Dict[str, Any]],
    *,
    tf_doc_mode: str,
) -> str:
    lines = ["handoff_id,project_alias,from_tf,to_tf,task_id,created_at,doc,status"]
    for req_id, task in rows:
        source_req = _resolve_source_request_id(task)
        if not source_req:
            continue
        source_task = task_map.get(source_req, {})
        from_tf = _tf_id_from_task(source_task if isinstance(source_task, dict) else {}, source_req)
        to_tf = _tf_id_from_task(task, req_id)
        created_at = str(task.get("created_at", "")).strip() or str(task.get("updated_at", "")).strip() or "-"
        if str(tf_doc_mode or "").strip().lower() == "legacy":
            doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{to_tf}/handoff.md"
        else:
            doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{to_tf}/report.md"
        status = _normalize_status(task.get("status"))
        handoff_id = f"H-{project_alias}-{to_tf}-{re.sub(r'[^A-Za-z0-9]+', '', str(req_id))[:8] or 'X'}"
        lines.append(f"{handoff_id},{project_alias},{from_tf},{to_tf},{req_id},{created_at},{doc},{status}")
    return "\n".join(lines)


def _csv_cell(raw: Any, max_len: int = 240) -> str:
    text = str(raw or "").replace("\n", " ").replace("\r", " ").strip()
    text = re.sub(r"\s+", " ", text).strip()
    if "," in text:
        text = text.replace(",", " ")
        text = re.sub(r"\s+", " ", text).strip()
    if len(text) > int(max_len):
        return text[: max(0, int(max_len) - 3)] + "..."
    return text


def _exec_verdict_and_reason(task: Dict[str, Any]) -> Tuple[str, str]:
    exec_critic = task.get("exec_critic") if isinstance(task, dict) else None
    verdict = "-"
    reason = ""
    if isinstance(exec_critic, dict):
        v = str(exec_critic.get("verdict", "")).strip().lower()
        if v in {"success", "retry", "fail"}:
            verdict = v
        reason = str(exec_critic.get("reason", "")).strip()
    return verdict, reason


def _child_tf_ids(task: Dict[str, Any], task_map: Dict[str, Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for field in ("retry_children", "replan_children"):
        raw = task.get(field)
        if not isinstance(raw, list):
            continue
        for rid in raw:
            token = str(rid or "").strip()
            if not token:
                continue
            child_task = task_map.get(token) if isinstance(task_map.get(token), dict) else {}
            tf = _tf_id_from_task(child_task if isinstance(child_task, dict) else {}, token)
            if tf and tf not in seen:
                seen.add(tf)
                out.append(tf)
    return out


def _render_tf_report_md(
    *,
    project_alias: str,
    tf_id: str,
    request_id: str,
    task: Dict[str, Any],
    status: str,
    verdict: str,
    reason: str,
    next_tf: str,
    open_risks: str,
    evidence: str,
    resume_point: str,
    child_tfs: List[str],
    tf_doc_mode: str,
    artifact_policy: str,
    exec_meta: Optional[Dict[str, Any]] = None,
) -> str:
    short = _task_short_id(task, request_id)
    alias = str(task.get("alias", "")).strip()
    stage = str(task.get("stage", "")).strip() or "-"
    created_at = str(task.get("created_at", "")).strip() or "-"
    updated_at = str(task.get("updated_at", "")).strip() or "-"
    closed_at = updated_at if status in {"completed", "failed"} else "-"
    objective = str(task.get("prompt", "")).strip() or "-"

    plan_lines = _plan_lines_from_task(task)
    if not plan_lines:
        plan_lines = ["- (no plan captured)"]

    verdict_line = verdict if verdict != "-" else "-"
    if status in {"completed", "failed"} and verdict_line == "-":
        verdict_line = "success" if status == "completed" else "fail"

    lines: List[str] = []
    lines.append(f"# {tf_id} Report")
    lines.append("")
    lines.append("## Snapshot")
    lines.append(f"- project_alias: {project_alias}")
    lines.append(f"- tf_id: {tf_id}")
    lines.append(f"- task_label: {short}{' | ' + alias if alias else ''}")
    lines.append(f"- request_id: {request_id}")
    lines.append(f"- status: {status}")
    lines.append(f"- stage: {stage}")
    lines.append(f"- exec_verdict: {verdict_line}")
    if reason:
        lines.append(f"- exec_reason: {_objective_from_text(reason, 220)}")
    lines.append(f"- created_at: {created_at}")
    lines.append(f"- updated_at: {updated_at}")
    lines.append(f"- closed_at: {closed_at}")
    if isinstance(exec_meta, dict) and exec_meta:
        lines.append("")
        lines.append("## Exec Workspace")
        lines.append(f"- exec_mode: {_tf_exec_meta_line(exec_meta, 'mode')}")
        lines.append(f"- workdir: {_tf_exec_meta_line(exec_meta, 'workdir')}")
        lines.append(f"- run_dir: {_tf_exec_meta_line(exec_meta, 'run_dir')}")
        br = _tf_exec_meta_line(exec_meta, "branch")
        if br != "-":
            lines.append(f"- branch: {br}")
    lines.append("")
    lines.append("## Objective")
    lines.append(objective.rstrip() if objective.strip() else "-")
    lines.append("")
    lines.append("## Plan")
    lines.extend(plan_lines)
    lines.append("")
    lines.append("## Outcome")
    lines.append(f"- next_tf: {next_tf}")
    if child_tfs:
        lines.append(f"- child_tfs: {' | '.join(child_tfs[:9])}")
    lines.append(f"- open_risks: {open_risks}")
    lines.append(f"- resume_point: {resume_point}")
    lines.append("")
    lines.append("## Evidence")
    lines.append(f"- {evidence}")
    lines.append("")
    lines.append("## Retention Policy")
    lines.append(f"- tf_doc_mode: {tf_doc_mode}")
    lines.append(f"- tf_artifacts: {artifact_policy}")
    lines.append("- note: artifacts are bundled to `projects/<alias>/archive/<tf_id>.tar.gz` on success.")
    lines.append("")
    lines.append("## History (Tail)")
    hist = task.get("history")
    if isinstance(hist, list) and hist:
        tail = hist[-8:]
        for row in tail:
            if not isinstance(row, dict):
                continue
            at = str(row.get("at", "")).strip() or "-"
            stg = str(row.get("stage", "")).strip() or "-"
            st = str(row.get("status", "")).strip() or "-"
            lines.append(f"- {at} stage={stg} status={st}")
    else:
        lines.append("- (empty)")
    return "\n".join(lines).rstrip()


def _prune_tf_legacy_docs(tf_dir: Path) -> None:
    # Keep exactly one TF doc (`report.md`). Remove legacy scaffold docs and archive folder.
    for name in ("ongoing.md", "note.md", "handoff.md"):
        path = tf_dir / name
        try:
            if path.exists() and path.is_file():
                path.unlink()
        except Exception:
            pass
    archive_dir = tf_dir / "archive"
    try:
        if archive_dir.exists() and archive_dir.is_dir():
            shutil.rmtree(archive_dir)
    except Exception:
        pass


def _migrate_legacy_tf_scaffolds_to_single_report(*, docs_root: Path, project_alias: str) -> None:
    """Best-effort migration: ensure every TF dir has report.md only (no legacy docs)."""

    root = docs_root / "projects" / project_alias / "tfs"
    if not root.exists() or not root.is_dir():
        return
    for tf_dir in sorted([p for p in root.iterdir() if p.is_dir()]):
        report = tf_dir / "report.md"
        if not report.exists():
            tf_id = tf_dir.name
            _write_text(
                report,
                "\n".join(
                    [
                        f"# {tf_id} Report",
                        "",
                        "- migrated: legacy scaffold detected (no matching task in manager state)",
                        "- note: this file is auto-generated placeholder; real TF reports come from tasks.",
                    ]
                ),
            )
        _prune_tf_legacy_docs(tf_dir)


def _is_tf_success(status: str, verdict: str) -> bool:
    st = _normalize_status(status)
    v = str(verdict or "").strip().lower()
    if st != "completed":
        return False
    if v in {"retry", "fail"}:
        return False
    return True


def _tar_add_text(tar: tarfile.TarFile, arcname: str, text: str) -> None:
    payload = (text.rstrip("\n") + "\n").encode("utf-8")
    info = tarfile.TarInfo(name=arcname)
    info.size = len(payload)
    info.mtime = int(time.time())
    tar.addfile(info, io.BytesIO(payload))


def _tar_add_json(tar: tarfile.TarFile, arcname: str, obj: Any) -> None:
    try:
        text = json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=True)
    except Exception:
        text = json.dumps({"error": "failed to serialize"}, ensure_ascii=True)
    _tar_add_text(tar, arcname, text)


def _filter_events_for_task(
    events: List[Dict[str, Any]],
    *,
    request_id: str,
    task_short_id: str,
    task_alias: str,
    project_alias: str,
    project_key: str = "",
) -> List[Dict[str, Any]]:
    rid = str(request_id or "").strip()
    sid = str(task_short_id or "").strip()
    alias = str(task_alias or "").strip()
    proj_alias = str(project_alias or "").strip()
    proj_key = str(project_key or "").strip()
    proj_tokens = {x for x in {proj_alias, proj_key} if x}
    out: List[Dict[str, Any]] = []
    for row in events or []:
        if not isinstance(row, dict):
            continue
        # Events store `project` as the project key (e.g. "twinpaper"), while docs use
        # human aliases (e.g. "O2"). Accept either to avoid empty runlog excerpts.
        if proj_tokens and str(row.get("project", "")).strip() not in {"", *proj_tokens}:
            continue
        if rid and str(row.get("request_id", "")).strip() == rid:
            out.append(row)
            continue
        if sid and str(row.get("task_short_id", "")).strip() == sid:
            out.append(row)
            continue
        if alias and str(row.get("task_alias", "")).strip() == alias:
            out.append(row)
            continue
    return out


def _render_task_runlog_excerpt(events: List[Dict[str, Any]], *, limit: int = 36) -> str:
    lines = [
        "# RunLog Excerpt",
        "",
        "source: .aoe-team/logs/gateway_events.jsonl (tail-only)",
        "",
    ]
    if not events:
        lines.append("- (empty)")
        return "\n".join(lines)
    for row in list(events)[-max(1, int(limit)) :]:
        ts = str(row.get("timestamp", "")).strip() or "-"
        ev = str(row.get("event", "")).strip() or "-"
        stg = str(row.get("stage", "")).strip() or "-"
        st = str(row.get("status", "")).strip() or "-"
        err = str(row.get("error_code", "")).strip()
        detail = str(row.get("detail", "")).strip()
        parts = [f"{ts} event={ev}", f"stage={stg}", f"status={st}"]
        if err:
            parts.append(f"error={err}")
        if detail:
            parts.append(f"detail={detail[:200]}")
        lines.append("- " + " | ".join(parts))
    return "\n".join(lines)


def _capture_cmd_output(
    cmd: List[str],
    *,
    cwd: Path,
    timeout_sec: int = 20,
    max_bytes: int = 256 * 1024,
) -> str:
    """Run a local command and capture text output (best-effort, size-capped)."""
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
            timeout=max(1, int(timeout_sec)),
        )
        out = (proc.stdout or "").strip()
        err = (proc.stderr or "").strip()
        body = out
        if err:
            body = (body + "\n\n[stderr]\n" + err).strip() if body else ("[stderr]\n" + err)
        payload = f"$ {' '.join(cmd)}\nexit={proc.returncode}\n\n{body or '(no output)'}\n"
        if len(payload) > max_bytes:
            payload = payload[:max_bytes] + "\n...(truncated)...\n"
        return payload.rstrip("\n")
    except Exception as e:
        return f"$ {' '.join(cmd)}\n(error: {e})"


def _is_git_repo_dir(path: Path) -> bool:
    """Cheap git check (supports both `.git/` and worktree `.git` file)."""
    try:
        dotgit = path / ".git"
        return bool(dotgit.is_dir() or dotgit.is_file())
    except Exception:
        return False


def _maybe_write_tf_archive_bundle_single(
    *,
    docs_root: Path,
    project_key: str,
    project_alias: str,
    tf_id: str,
    task: Dict[str, Any],
    request_id: str,
    status: str,
    verdict: str,
    reason: str,
    events_path: Path,
    manager_state_path: Path,
    force: bool = False,
    exec_meta: Optional[Dict[str, Any]] = None,
) -> None:
    if (not force) and (not _is_tf_success(status, verdict)):
        return

    project_dir = docs_root / "projects" / project_alias
    tf_dir = project_dir / "tfs" / tf_id
    report = tf_dir / "report.md"
    if not report.exists():
        return

    bundle = project_dir / "archive" / f"{tf_id}.tar.gz"
    if bundle.exists():
        return
    bundle.parent.mkdir(parents=True, exist_ok=True)

    events = _load_recent_gateway_events(events_path, limit=240)
    short = _task_short_id(task, request_id)
    alias = str(task.get("alias", "")).strip()
    filtered = _filter_events_for_task(
        events,
        request_id=request_id,
        task_short_id=short,
        task_alias=alias,
        project_alias=project_alias,
        project_key=project_key,
    )

    snapshot = {
        "project_alias": project_alias,
        "tf_id": tf_id,
        "request_id": request_id,
        "task_short_id": short,
        "task_alias": alias,
        "status": status,
        "exec_verdict": verdict,
        "exec_reason": reason,
        "manager_state_path": str(manager_state_path),
        "task": task,
    }

    try:
        with tarfile.open(bundle, "w:gz") as tar:
            arc_report = report.relative_to(project_dir)
            tar.add(report, arcname=str(arc_report))
            _tar_add_json(tar, str(Path("tfs") / tf_id / "task_snapshot.json"), snapshot)
            _tar_add_text(tar, str(Path("tfs") / tf_id / "runlog_excerpt.md"), _render_task_runlog_excerpt(filtered))
            if isinstance(exec_meta, dict) and exec_meta:
                _tar_add_json(tar, str(Path("tfs") / tf_id / "exec" / "exec_meta.json"), exec_meta)

                run_dir = Path(str(exec_meta.get("run_dir", "") or "")).expanduser()
                if run_dir.exists() and run_dir.is_dir():
                    kept = 0
                    omitted: List[str] = []
                    for p in sorted(run_dir.rglob("*")):
                        if kept >= 80:
                            omitted.append("(max_files reached)")
                            break
                        try:
                            if p.is_dir():
                                continue
                            if p.is_symlink():
                                omitted.append(str(p.relative_to(run_dir)) + " (symlink)")
                                continue
                            size = p.stat().st_size
                        except Exception:
                            continue
                        if size > 256 * 1024:
                            omitted.append(str(p.relative_to(run_dir)) + f" ({size} bytes)")
                            continue
                        arc = Path("tfs") / tf_id / "exec" / "run_dir" / p.relative_to(run_dir)
                        tar.add(p, arcname=str(arc))
                        kept += 1
                    if omitted:
                        _tar_add_text(
                            tar,
                            str(Path("tfs") / tf_id / "exec" / "run_dir_omitted.txt"),
                            "\n".join(omitted),
                        )

                workdir = Path(str(exec_meta.get("workdir", "") or "")).expanduser()
                if workdir.exists() and workdir.is_dir() and _is_git_repo_dir(workdir):
                    _tar_add_text(
                        tar,
                        str(Path("tfs") / tf_id / "exec" / "git" / "head.txt"),
                        _capture_cmd_output(["git", "log", "-1", "--oneline", "--no-color"], cwd=workdir),
                    )
                    _tar_add_text(
                        tar,
                        str(Path("tfs") / tf_id / "exec" / "git" / "status_porcelain.txt"),
                        _capture_cmd_output(["git", "status", "--porcelain=v1", "-b"], cwd=workdir),
                    )
                    _tar_add_text(
                        tar,
                        str(Path("tfs") / tf_id / "exec" / "git" / "diff_stat.txt"),
                        _capture_cmd_output(["git", "diff", "--stat", "--no-color"], cwd=workdir),
                    )
                    _tar_add_text(
                        tar,
                        str(Path("tfs") / tf_id / "exec" / "git" / "diff_name_status.txt"),
                        _capture_cmd_output(["git", "diff", "--name-status", "--no-color"], cwd=workdir),
                    )
                    _tar_add_text(
                        tar,
                        str(Path("tfs") / tf_id / "exec" / "git" / "diff_patch.patch"),
                        _capture_cmd_output(
                            ["git", "diff", "--patch", "--no-color"],
                            cwd=workdir,
                            timeout_sec=30,
                            max_bytes=512 * 1024,
                        ),
                    )
    except Exception:
        # best-effort; never fail the gateway on archive packaging issues
        try:
            if bundle.exists():
                bundle.unlink()
        except Exception:
            pass


def _prune_tf_archive_bundle_single(*, docs_root: Path, project_alias: str, tf_id: str) -> None:
    bundle = docs_root / "projects" / project_alias / "archive" / f"{tf_id}.tar.gz"
    try:
        if bundle.exists() and bundle.is_file():
            bundle.unlink()
    except Exception:
        pass


def _maybe_fill_handoff_doc(
    *,
    docs_root: Path,
    project_alias: str,
    tf_id: str,
    next_tf: str,
    completed: str,
    open_risks: str,
    evidence: str,
    resume_point: str,
) -> None:
    path = docs_root / "projects" / project_alias / "tfs" / tf_id / "handoff.md"
    try:
        src = path.read_text(encoding="utf-8")
    except Exception:
        return

    replacements = {
        "- next_tf: __fill or N/A__": f"- next_tf: {next_tf}",
        "- completed: __fill__": f"- completed: {completed}",
        "- open risks: __fill__": f"- open risks: {open_risks}",
        "- required evidence: __fill__": f"- required evidence: {evidence}",
        "- resume point: __fill__": f"- resume point: {resume_point}",
    }
    out = src
    for needle, repl in replacements.items():
        if needle in out:
            out = out.replace(needle, repl)
    if out != src:
        _write_text(path, out)


def _maybe_write_tf_archive_summary(
    *,
    docs_root: Path,
    project_alias: str,
    tf_id: str,
    status: str,
    exec_verdict: str,
    request_id: str,
    objective: str,
    closed_at: str,
    next_tf: str,
    open_risks: str,
    evidence: str,
    resume_point: str,
) -> None:
    path = docs_root / "projects" / project_alias / "tfs" / tf_id / "archive" / "close_summary.md"
    if path.exists():
        return
    title = f"# {tf_id} Close Summary (auto)"
    lines = [
        title,
        "",
        f"- status: {status}",
        f"- exec_verdict: {exec_verdict}",
        f"- request_id: {request_id}",
        f"- objective: {_objective_from_text(objective, 220)}",
        f"- closed_at: {closed_at or '-'}",
        f"- next_tf: {next_tf}",
        f"- open_risks: {open_risks}",
        f"- evidence: {evidence}",
        f"- resume_point: {resume_point}",
        "",
        "notes:",
        "- this file is auto-generated once at TF close/transition time.",
    ]
    _write_text(path, "\n".join(lines))


def _render_tf_close_index_csv(
    *,
    project_alias: str,
    rows: Iterable[Tuple[str, Dict[str, Any]]],
    tf_doc_mode: str,
) -> str:
    tf_mode = str(tf_doc_mode or "").strip().lower()
    if tf_mode != "legacy":
        lines = [
            "project_alias,tf_id,task_label,request_id,status,exec_verdict,closed_at,report_doc,archive_bundle",
        ]
        for req_id, task in rows:
            status = _normalize_status(task.get("status"))
            if status not in {"completed", "failed"}:
                continue
            tf_id = _tf_id_from_task(task, req_id)
            short = _task_short_id(task, req_id)
            alias = str(task.get("alias", "")).strip()
            label = f"{short} | {alias}" if short and alias else (alias or short or req_id)
            verdict, _reason = _exec_verdict_and_reason(task)
            closed_at = str(task.get("updated_at", "")).strip() or "-"
            report_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/report.md"
            bundle = f"docs/investigations_mo/projects/{project_alias}/archive/{tf_id}.tar.gz"
            lines.append(
                ",".join(
                    [
                        _csv_cell(project_alias),
                        _csv_cell(tf_id),
                        _csv_cell(label),
                        _csv_cell(req_id),
                        _csv_cell(status),
                        _csv_cell(verdict),
                        _csv_cell(closed_at),
                        _csv_cell(report_doc),
                        _csv_cell(bundle),
                    ]
                )
            )
        return "\n".join(lines)
    lines = [
        "project_alias,tf_id,task_label,request_id,status,exec_verdict,closed_at,handoff_doc,note_doc",
    ]
    for req_id, task in rows:
        status = _normalize_status(task.get("status"))
        if status not in {"completed", "failed"}:
            continue
        tf_id = _tf_id_from_task(task, req_id)
        short = _task_short_id(task, req_id)
        alias = str(task.get("alias", "")).strip()
        label = f"{short} | {alias}" if short and alias else (alias or short or req_id)
        verdict, _reason = _exec_verdict_and_reason(task)
        closed_at = str(task.get("updated_at", "")).strip() or "-"
        handoff_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/handoff.md"
        note_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/note.md"
        lines.append(
            ",".join(
                [
                    _csv_cell(project_alias),
                    _csv_cell(tf_id),
                    _csv_cell(label),
                    _csv_cell(req_id),
                    _csv_cell(status),
                    _csv_cell(verdict),
                    _csv_cell(closed_at),
                    _csv_cell(handoff_doc),
                    _csv_cell(note_doc),
                ]
            )
        )
    return "\n".join(lines)


def _maybe_write_tf_archive_bundle(
    *,
    docs_root: Path,
    project_alias: str,
    tf_id: str,
) -> None:
    project_dir = docs_root / "projects" / project_alias
    tf_dir = project_dir / "tfs" / tf_id
    if not tf_dir.exists() or not tf_dir.is_dir():
        return

    bundle = project_dir / "archive" / f"{tf_id}.tar.gz"
    if bundle.exists():
        return
    bundle.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tarfile.open(bundle, "w:gz") as tar:
            for path in sorted(tf_dir.rglob("*")):
                if not path.is_file():
                    continue
                arc = path.relative_to(project_dir)
                tar.add(path, arcname=str(arc))
    except Exception:
        # best-effort; never fail the gateway on archive packaging issues
        return


def _iter_all_tasks(projects: Dict[str, Any]) -> Iterable[Tuple[str, str, Dict[str, Any]]]:
    for key, entry in (projects or {}).items():
        if not isinstance(entry, dict):
            continue
        alias = _normalize_project_alias(entry.get("project_alias"), key)
        raw_tasks = entry.get("tasks")
        tasks = raw_tasks if isinstance(raw_tasks, dict) else {}
        for req_id, task in _sorted_tasks(tasks):
            if isinstance(task, dict):
                yield alias, req_id, task


def _render_global_tf_registry(*, projects: Dict[str, Any], limit: int = 240, tf_doc_mode: str) -> str:
    tf_mode = str(tf_doc_mode or "").strip().lower()
    rows: List[Tuple[str, str, Dict[str, Any]]] = list(_iter_all_tasks(projects))
    rows.sort(
        key=lambda r: (
            str((r[2] or {}).get("updated_at", "")),
            str((r[2] or {}).get("created_at", "")),
        ),
        reverse=True,
    )

    if tf_mode != "legacy":
        lines = [
            "# TF Registry (All Projects)",
            "",
            "| tf_id | project_alias | objective | status | exec_verdict | owner | created_at | closed_at | report_doc |",
            "|---|---|---|---|---|---|---|---|---|",
        ]
    else:
        lines = [
            "# TF Registry (All Projects)",
            "",
            "| tf_id | project_alias | objective | status | exec_verdict | owner | created_at | closed_at | ongoing_doc | handoff_doc |",
            "|---|---|---|---|---|---|---|---|---|---|",
        ]

    for project_alias, req_id, task in rows[: max(1, int(limit))]:
        tf_id = _tf_id_from_task(task, req_id)
        status = _normalize_status(task.get("status"))
        created_at = str(task.get("created_at", "")).strip() or "-"
        updated_at = str(task.get("updated_at", "")).strip() or "-"
        closed_at = updated_at if status in {"completed", "failed"} else "-"
        owner = str(task.get("control_mode", "")).strip() or "project-orch"
        verdict, _reason = _exec_verdict_and_reason(task)
        objective = _task_objective(task).replace("|", "/")
        if tf_mode != "legacy":
            report_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/report.md"
            lines.append(
                f"| {tf_id} | {project_alias} | {objective} | {status} | {verdict} | {owner} | {created_at} | {closed_at} | `{report_doc}` |"
            )
        else:
            ongoing_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/ongoing.md"
            handoff_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/handoff.md"
            lines.append(
                f"| {tf_id} | {project_alias} | {objective} | {status} | {verdict} | {owner} | {created_at} | {closed_at} | `{ongoing_doc}` | `{handoff_doc}` |"
            )

    if len(lines) == 4:
        if tf_mode != "legacy":
            lines.append("| - | - | - | - | - | - | - | - | - |")
        else:
            lines.append("| - | - | - | - | - | - | - | - | - | - |")
    lines.extend(
        [
            "",
            "## Status",
            "- `pending`, `running`, `completed`, `failed`",
            "- `exec_verdict`: `success`, `retry`, `fail` (when available)",
        ]
    )
    return "\n".join(lines)


def _render_global_tf_close_index_csv(*, projects: Dict[str, Any], limit: int = 600, tf_doc_mode: str) -> str:
    tf_mode = str(tf_doc_mode or "").strip().lower()
    if tf_mode != "legacy":
        lines = [
            "project_alias,tf_id,task_label,request_id,status,exec_verdict,closed_at,report_doc,archive_bundle",
        ]
    else:
        lines = [
            "project_alias,tf_id,task_label,request_id,status,exec_verdict,closed_at,handoff_doc,note_doc",
        ]
    rows: List[Tuple[str, str, Dict[str, Any]]] = list(_iter_all_tasks(projects))
    rows.sort(
        key=lambda r: str((r[2] or {}).get("updated_at", "")),
        reverse=True,
    )
    emitted = 0
    for project_alias, req_id, task in rows:
        status = _normalize_status(task.get("status"))
        if status not in {"completed", "failed"}:
            continue
        tf_id = _tf_id_from_task(task, req_id)
        short = _task_short_id(task, req_id)
        alias = str(task.get("alias", "")).strip()
        label = f"{short} | {alias}" if short and alias else (alias or short or req_id)
        verdict, _reason = _exec_verdict_and_reason(task)
        closed_at = str(task.get("updated_at", "")).strip() or "-"
        if tf_mode != "legacy":
            report_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/report.md"
            bundle = f"docs/investigations_mo/projects/{project_alias}/archive/{tf_id}.tar.gz"
            lines.append(
                ",".join(
                    [
                        _csv_cell(project_alias),
                        _csv_cell(tf_id),
                        _csv_cell(label),
                        _csv_cell(req_id),
                        _csv_cell(status),
                        _csv_cell(verdict),
                        _csv_cell(closed_at),
                        _csv_cell(report_doc),
                        _csv_cell(bundle),
                    ]
                )
            )
        else:
            handoff_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/handoff.md"
            note_doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{tf_id}/note.md"
            lines.append(
                ",".join(
                    [
                        _csv_cell(project_alias),
                        _csv_cell(tf_id),
                        _csv_cell(label),
                        _csv_cell(req_id),
                        _csv_cell(status),
                        _csv_cell(verdict),
                        _csv_cell(closed_at),
                        _csv_cell(handoff_doc),
                        _csv_cell(note_doc),
                    ]
                )
            )
        emitted += 1
        if emitted >= max(1, int(limit)):
            break
    return "\n".join(lines)


def _render_global_handoff_index(*, projects: Dict[str, Any], limit: int = 600, tf_doc_mode: str) -> str:
    tf_mode = str(tf_doc_mode or "").strip().lower()
    lines = ["handoff_id,project_alias,from_tf,to_tf,task_id,created_at,doc,status"]
    emitted = 0
    for key, entry in (projects or {}).items():
        if not isinstance(entry, dict):
            continue
        project_alias = _normalize_project_alias(entry.get("project_alias"), key)
        raw_tasks = entry.get("tasks")
        tasks = raw_tasks if isinstance(raw_tasks, dict) else {}
        rows = _sorted_tasks(tasks)
        task_map: Dict[str, Dict[str, Any]] = {}
        for rid, t in tasks.items():
            if isinstance(t, dict):
                task_map[str(rid)] = t
        for req_id, task in rows:
            if not isinstance(task, dict):
                continue
            source_req = _resolve_source_request_id(task)
            if not source_req:
                continue
            source_task = task_map.get(source_req, {})
            from_tf = _tf_id_from_task(source_task if isinstance(source_task, dict) else {}, source_req)
            to_tf = _tf_id_from_task(task, req_id)
            created_at = str(task.get("created_at", "")).strip() or str(task.get("updated_at", "")).strip() or "-"
            if tf_mode == "legacy":
                doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{to_tf}/handoff.md"
            else:
                doc = f"docs/investigations_mo/projects/{project_alias}/tfs/{to_tf}/report.md"
            status = _normalize_status(task.get("status"))
            handoff_id = f"H-{project_alias}-{to_tf}-{re.sub(r'[^A-Za-z0-9]+', '', str(req_id))[:8] or 'X'}"
            lines.append(f"{handoff_id},{project_alias},{from_tf},{to_tf},{req_id},{created_at},{doc},{status}")
            emitted += 1
            if emitted >= max(1, int(limit)):
                return "\n".join(lines)
    return "\n".join(lines)


def sync_investigations_docs(manager_state_path: Path, state: Dict[str, Any]) -> None:
    if not isinstance(state, dict):
        return
    projects = state.get("projects")
    if not isinstance(projects, dict) or not projects:
        return

    tf_doc_mode = _tf_doc_mode()
    tf_artifact_policy = _tf_artifact_policy()

    active_key = _normalize_key(state.get("active", ""))
    if active_key not in projects or not isinstance(projects.get(active_key), dict):
        active_key = ""
        for key in sorted(projects.keys()):
            if isinstance(projects.get(key), dict):
                active_key = key
                break
    if not active_key:
        return

    # Prefer anchoring docs under the gateway's own workspace (manager_state_path),
    # so multi-project scheduling does not "move" the documentation root.
    docs_root: Optional[Path] = None
    team_dir: Optional[Path] = None
    try:
        team_dir = manager_state_path.parent.resolve()
        control_root = team_dir.parent.resolve()
        docs_root = (control_root / "docs" / "investigations_mo").resolve()
    except Exception:
        docs_root = None
        team_dir = None

    active_entry = projects.get(active_key)
    if not isinstance(active_entry, dict):
        return

    # Fallback when manager_state_path is in a non-standard location.
    if docs_root is None or team_dir is None:
        project_root_raw = str(active_entry.get("project_root", "")).strip()
        if not project_root_raw:
            return
        project_root = Path(project_root_raw).expanduser().resolve()
        docs_root = (project_root / "docs" / "investigations_mo").resolve()
        team_dir = Path(str(active_entry.get("team_dir", project_root / ".aoe-team"))).expanduser().resolve()

    tf_exec_map: Dict[str, Any] = _load_tf_exec_map(team_dir)

    active_project_alias = _normalize_project_alias(active_entry.get("project_alias"), active_key)
    raw_active_tasks = active_entry.get("tasks")
    active_tasks = raw_active_tasks if isinstance(raw_active_tasks, dict) else {}
    active_rows = _sorted_tasks(active_tasks)
    active_req = _pick_active_request_id(active_entry, active_tasks)
    active_task = active_tasks.get(active_req) if active_req in active_tasks and isinstance(active_tasks.get(active_req), dict) else {}
    active_tf = _tf_id_from_task(active_task if isinstance(active_task, dict) else {}, active_req or "active")

    # Global registries
    _write_text(
        docs_root / "registry" / "project_lock.yaml",
        _render_project_lock(active_project_alias, active_tf, tf_doc_mode=tf_doc_mode),
    )
    _write_text(
        docs_root / "registry" / "project_registry.md",
        _render_project_registry(projects=projects, active_key=active_key),
    )
    events_path = team_dir / "logs" / "gateway_events.jsonl"
    events = _load_recent_gateway_events(events_path, limit=max(DEFAULT_RUNLOG_EVENTS_LIMIT * 5, 60))
    _write_text(
        docs_root / "registry" / "runlog_recent.md",
        _render_runlog_recent(events=events, active_project_alias=active_project_alias, limit=DEFAULT_RUNLOG_EVENTS_LIMIT),
    )
    _write_text(docs_root / "registry" / "tf_registry.md", _render_global_tf_registry(projects=projects, tf_doc_mode=tf_doc_mode))
    _write_text(
        docs_root / "registry" / "handoff_index.csv",
        _render_global_handoff_index(projects=projects, tf_doc_mode=tf_doc_mode),
    )
    _write_text(
        docs_root / "registry" / "tf_close_index.csv",
        _render_global_tf_close_index_csv(projects=projects, tf_doc_mode=tf_doc_mode),
    )

    # Per-project scaffold + close artifacts (cheap, idempotent).
    for key, entry in projects.items():
        if not isinstance(entry, dict):
            continue
        project_alias = _normalize_project_alias(entry.get("project_alias"), key)
        raw_tasks = entry.get("tasks")
        tasks = raw_tasks if isinstance(raw_tasks, dict) else {}
        rows = _sorted_tasks(tasks)

        proj_req = _pick_active_request_id(entry, tasks)
        proj_task = tasks.get(proj_req) if proj_req in tasks and isinstance(tasks.get(proj_req), dict) else {}
        proj_tf = _tf_id_from_task(proj_task if isinstance(proj_task, dict) else {}, proj_req or "active")

        _ensure_project_tf_scaffold(docs_root, project_alias, proj_tf, tf_doc_mode=tf_doc_mode)
        project_objective = str(entry.get("overview", "")).strip()
        tf_objective = str((proj_task or {}).get("prompt", "")).strip() if isinstance(proj_task, dict) else ""
        tf_plan_lines = _plan_lines_from_task(proj_task) if isinstance(proj_task, dict) else []
        _maybe_fill_active_docs(
            docs_root=docs_root,
            project_alias=project_alias,
            tf_id=proj_tf,
            project_objective=project_objective,
            tf_objective=tf_objective,
            tf_plan_lines=tf_plan_lines,
            tf_doc_mode=tf_doc_mode,
        )
        _upsert_project_todo_queue(
            docs_root / "projects" / project_alias / "ongoing.md",
            _render_project_todo_queue(entry, tasks),
        )

        task_map: Dict[str, Dict[str, Any]] = {}
        for rid, t in tasks.items():
            if isinstance(t, dict):
                task_map[str(rid)] = t

        # Ensure active TF has a single report, even while running.
        if tf_doc_mode != "legacy" and isinstance(proj_task, dict) and proj_req:
            tf_dir = docs_root / "projects" / project_alias / "tfs" / proj_tf
            report = tf_dir / "report.md"
            status = _normalize_status((proj_task or {}).get("status"))
            verdict, reason = _exec_verdict_and_reason(proj_task)
            evidence = _csv_cell(
                f"request_id={proj_req} | logs=.aoe-team/logs/gateway_events.jsonl | state=.aoe-team/orch_manager_state.json",
                240,
            )
            child_tfs = _child_tf_ids(proj_task, task_map)
            _write_text(
                report,
	                _render_tf_report_md(
	                    project_alias=project_alias,
	                    tf_id=proj_tf,
	                    request_id=proj_req,
	                    task=proj_task,
	                    status=status,
	                    verdict=verdict,
	                    reason=reason,
	                    next_tf=(" | ".join(child_tfs[:3]) if child_tfs else "continue"),
	                    open_risks="-",
	                    evidence=evidence,
	                    resume_point="continue",
	                    child_tfs=child_tfs,
	                    tf_doc_mode=tf_doc_mode,
	                    artifact_policy=tf_artifact_policy,
	                    exec_meta=(tf_exec_map.get(proj_req) if isinstance(tf_exec_map.get(proj_req), dict) else None),
	                ),
	            )
            _prune_tf_legacy_docs(tf_dir)

        # Limit to latest items to keep sync cheap.
        max_items = 20 if tf_doc_mode == "legacy" else 240
        for req_id, task in rows[:max_items]:
            if not isinstance(task, dict):
                continue
            status = _normalize_status(task.get("status"))
            child_tfs = _child_tf_ids(task, task_map)
            if tf_doc_mode == "legacy" and status not in {"completed", "failed"} and not child_tfs:
                continue

            tf_id = _tf_id_from_task(task, req_id)
            _ensure_project_tf_scaffold(docs_root, project_alias, tf_id, tf_doc_mode=tf_doc_mode)

            verdict, reason = _exec_verdict_and_reason(task)
            short = _task_short_id(task, req_id)
            objective = _task_objective(task)
            completed = _csv_cell(
                f"{short} {objective} verdict={verdict if verdict != '-' else status} request_id={req_id}",
                260,
            )

            next_tf = "N/A"
            if child_tfs:
                next_tf = " | ".join(child_tfs[:3])
            else:
                if verdict == "success":
                    next_tf = "N/A"
                elif verdict == "retry":
                    next_tf = "TBD"
                elif verdict == "fail" or status == "failed":
                    next_tf = "escalate"
                elif status in {"running", "pending"}:
                    next_tf = "continue"

            risks = "-"
            if verdict in {"retry", "fail"} or status == "failed":
                parts: List[str] = []
                if reason:
                    parts.append(reason[:180])
                res = task.get("result") if isinstance(task.get("result"), dict) else {}
                failed = res.get("failed_roles") or []
                pending = res.get("pending_roles") or []
                if failed:
                    parts.append("failed_roles=" + ",".join(str(x) for x in failed)[:120])
                if pending and verdict == "retry":
                    parts.append("pending_roles=" + ",".join(str(x) for x in pending)[:120])
                risks = _csv_cell(" ; ".join([p for p in parts if p]) or "-", 240)

            evidence = _csv_cell(
                f"request_id={req_id} | logs=.aoe-team/logs/gateway_events.jsonl | state=.aoe-team/orch_manager_state.json",
                240,
            )
            resume = "continue" if status in {"running", "pending"} else "next todo"
            if verdict == "retry":
                resume = "retry/replan required"
            elif verdict == "fail" or status == "failed":
                resume = "owner intervention"
            resume_point = _csv_cell(resume, 80)

            if tf_doc_mode == "legacy":
                _maybe_fill_handoff_doc(
                    docs_root=docs_root,
                    project_alias=project_alias,
                    tf_id=tf_id,
                    next_tf=next_tf,
                    completed=completed,
                    open_risks=risks,
                    evidence=evidence,
                    resume_point=resume_point,
                )
                if status in {"completed", "failed"}:
                    closed_at = str(task.get("updated_at", "")).strip() or "-"
                    _maybe_write_tf_archive_summary(
                        docs_root=docs_root,
                        project_alias=project_alias,
                        tf_id=tf_id,
                        status=status,
                        exec_verdict=(verdict if verdict != "-" else status),
                        request_id=req_id,
                        objective=str(task.get("prompt", "")).strip(),
                        closed_at=closed_at,
                        next_tf=next_tf,
                        open_risks=risks,
                        evidence=evidence,
                        resume_point=resume_point,
                    )
                    _maybe_write_tf_archive_bundle(
                        docs_root=docs_root,
                        project_alias=project_alias,
                        tf_id=tf_id,
                    )
            else:
                tf_dir = docs_root / "projects" / project_alias / "tfs" / tf_id
                report = tf_dir / "report.md"
                _write_text(
                    report,
	                    _render_tf_report_md(
	                        project_alias=project_alias,
	                        tf_id=tf_id,
	                        request_id=req_id,
	                        task=task,
	                        status=status,
	                        verdict=verdict,
	                        reason=reason,
	                        next_tf=next_tf,
	                        open_risks=risks,
	                        evidence=evidence,
	                        resume_point=resume_point,
	                        child_tfs=child_tfs,
	                        tf_doc_mode=tf_doc_mode,
	                        artifact_policy=tf_artifact_policy,
	                        exec_meta=(tf_exec_map.get(req_id) if isinstance(tf_exec_map.get(req_id), dict) else None),
	                    ),
	                )
                _prune_tf_legacy_docs(tf_dir)

                if status in {"completed", "failed"}:
                    if tf_artifact_policy == "none":
                        _prune_tf_archive_bundle_single(docs_root=docs_root, project_alias=project_alias, tf_id=tf_id)
                    elif tf_artifact_policy == "all":
                        _maybe_write_tf_archive_bundle_single(
                            docs_root=docs_root,
                            project_key=key,
                            project_alias=project_alias,
                            tf_id=tf_id,
                            task=task,
                            request_id=req_id,
                            status=status,
                            verdict=verdict,
                            reason=reason,
                            events_path=events_path,
                            manager_state_path=manager_state_path,
                            force=True,
                            exec_meta=(tf_exec_map.get(req_id) if isinstance(tf_exec_map.get(req_id), dict) else None),
                        )
                    else:
                        if _is_tf_success(status, verdict):
                            _maybe_write_tf_archive_bundle_single(
                                docs_root=docs_root,
                                project_key=key,
                                project_alias=project_alias,
                                tf_id=tf_id,
                                task=task,
                                request_id=req_id,
                                status=status,
                                verdict=verdict,
                                reason=reason,
                                events_path=events_path,
                                manager_state_path=manager_state_path,
                                exec_meta=(tf_exec_map.get(req_id) if isinstance(tf_exec_map.get(req_id), dict) else None),
                            )
                        else:
                            _prune_tf_archive_bundle_single(
                                docs_root=docs_root,
                                project_alias=project_alias,
                                tf_id=tf_id,
                            )

        if tf_doc_mode != "legacy":
            _migrate_legacy_tf_scaffolds_to_single_report(docs_root=docs_root, project_alias=project_alias)
