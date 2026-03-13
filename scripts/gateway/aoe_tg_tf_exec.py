#!/usr/bin/env python3
"""TF execution/worktree/session helpers extracted from the gateway monolith."""

from __future__ import annotations

import argparse
import concurrent.futures
import copy
import json
import os
import re
import shlex
import shutil
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from aoe_tg_package_paths import worker_handler_script
from aoe_tg_task_view import dedupe_roles, normalize_project_alias, normalize_project_name, request_to_tf_id


def create_request_id() -> str:
    ts = datetime.now().strftime("%Y%m%d%H%M%S")
    return f"r_{ts}_{uuid.uuid4().hex[:8]}"


def sanitize_fs_token(raw: str, fallback: str = "default") -> str:
    token = re.sub(r"[^A-Za-z0-9._-]+", "_", str(raw or "").strip()).strip("._-")
    return token or fallback


def tf_exec_map_path(team_dir: Path, default_tf_exec_map_file: str) -> Path:
    return team_dir / default_tf_exec_map_file


def load_tf_exec_map(team_dir: Path, default_tf_exec_map_file: str) -> Dict[str, Any]:
    path = tf_exec_map_path(team_dir, default_tf_exec_map_file)
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_tf_exec_map(team_dir: Path, data: Dict[str, Any], default_tf_exec_map_file: str) -> None:
    path = tf_exec_map_path(team_dir, default_tf_exec_map_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp.{uuid.uuid4().hex[:8]}")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def tf_worker_runner_path() -> Path:
    return (Path(__file__).resolve().parent.parent / "team" / "aoe-tf-worker-session.py").resolve()


def tf_worker_session_name(
    request_id: str,
    role: str,
    *,
    default_prefix: str,
) -> str:
    rid = sanitize_fs_token(str(request_id or "").strip().lower(), "req")[:32]
    role_key = sanitize_fs_token(str(role or "").strip().lower(), "worker")[:24]
    prefix = str(os.environ.get("AOE_TF_WORKER_SESSION_PREFIX", default_prefix) or "").strip() or default_prefix
    return f"{prefix}{rid}_{role_key}"


def tf_worker_specs(
    args: argparse.Namespace,
    request_id: str,
    roles: List[str],
    startup_timeout_sec: int,
    lane_summary: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    runner = tf_worker_runner_path()
    handler = worker_handler_script().resolve()
    env_file = (Path(str(args.team_dir)) / "telegram.env").resolve()
    run_dir = (Path(str(args.team_dir)) / "tf_runs" / request_id).resolve()
    workers_dir = (run_dir / "workers").resolve()
    logs_dir = (run_dir / "logs").resolve()
    workers_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    execution_lanes = lane_summary.get("execution_lanes") if isinstance(lane_summary, dict) and isinstance(lane_summary.get("execution_lanes"), list) else []
    review_lanes = lane_summary.get("review_lanes") if isinstance(lane_summary, dict) and isinstance(lane_summary.get("review_lanes"), list) else []
    execution_lane_map: Dict[str, List[Dict[str, Any]]] = {}
    review_lane_map: Dict[str, List[Dict[str, Any]]] = {}
    for row in execution_lanes:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", "")).strip()
        if not role:
            continue
        execution_lane_map.setdefault(role, []).append(row)
    for row in review_lanes:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", "")).strip()
        if not role:
            continue
        review_lane_map.setdefault(role, []).append(row)

    specs: List[Dict[str, Any]] = []
    default_prefix = str(getattr(args, "_aoe_default_tf_worker_session_prefix", "") or "tfw_")
    for role in dedupe_roles(roles):
        role_key = sanitize_fs_token(role.lower(), "worker")
        session = tf_worker_session_name(request_id, role, default_prefix=default_prefix)
        state_file = (workers_dir / f"{role_key}.state.json").resolve()
        log_file = (logs_dir / f"worker_{role_key}.console.log").resolve()
        exec_rows = execution_lane_map.get(role, [])
        review_rows = review_lane_map.get(role, [])
        cmd = [
            str(runner),
            "--project-root",
            str(args.project_root),
            "--team-dir",
            str(args.team_dir),
            "--role",
            role,
            "--request-id",
            request_id,
            "--handler-cmd",
            str(handler),
            "--state-file",
            str(state_file),
            "--startup-timeout-sec",
            str(max(10, int(startup_timeout_sec))),
            "--exec-timeout-sec",
            str(max(60, int(args.orch_command_timeout_sec))),
            "--aoe-orch-bin",
            str(args.aoe_orch_bin),
        ]
        shell_parts: List[str] = ["set -e"]
        if env_file.exists():
            shell_parts.extend(["set -a", f". {shlex.quote(str(env_file))}", "set +a"])
        shell_parts.append(
            "exec {cmd} >> {log} 2>&1".format(
                cmd=" ".join(shlex.quote(part) for part in cmd),
                log=shlex.quote(str(log_file)),
            )
        )
        specs.append(
            {
                "role": role,
                "session": session,
                "state_file": str(state_file),
                "log_file": str(log_file),
                "shell": "; ".join(shell_parts),
                "execution_lane_ids": [
                    str(row.get("lane_id", "")).strip()
                    for row in exec_rows
                    if str(row.get("lane_id", "")).strip()
                ],
                "execution_subtask_ids": dedupe_roles(
                    str(item).strip()
                    for row in exec_rows
                    if isinstance(row, dict)
                    for item in (row.get("subtask_ids") or [])
                    if str(item).strip()
                ),
                "review_lane_ids": [
                    str(row.get("lane_id", "")).strip()
                    for row in review_rows
                    if str(row.get("lane_id", "")).strip()
                ],
                "review_depends_on": dedupe_roles(
                    str(item).strip()
                    for row in review_rows
                    if isinstance(row, dict)
                    for item in (row.get("depends_on") or [])
                    if str(item).strip()
                ),
            }
        )
    return specs


def preview_tf_worker_sessions(
    args: argparse.Namespace,
    request_id: str,
    roles: List[str],
    startup_timeout_sec: int,
    lane_summary: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    specs = tf_worker_specs(args, request_id, roles, startup_timeout_sec, lane_summary=lane_summary)
    return {
        "tmux_available": shutil.which("tmux") is not None,
        "sessions": [
            {
                "role": str(spec.get("role", "")).strip(),
                "session": str(spec.get("session", "")).strip(),
                "log_file": str(spec.get("log_file", "")).strip(),
                "execution_lane_ids": list(spec.get("execution_lane_ids") or []),
                "review_lane_ids": list(spec.get("review_lane_ids") or []),
            }
            for spec in specs
        ],
    }


def spawn_tf_worker_sessions(
    args: argparse.Namespace,
    request_id: str,
    roles: List[str],
    startup_timeout_sec: int,
    lane_summary: Optional[Dict[str, Any]] = None,
    *,
    run_command,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "tmux_available": shutil.which("tmux") is not None,
        "spawned": [],
        "existing": [],
        "failed": [],
        "sessions": [],
    }
    if not result["tmux_available"]:
        result["failed"].append({"role": "all", "error": "tmux not found"})
        return result

    for spec in tf_worker_specs(args, request_id, roles, startup_timeout_sec, lane_summary=lane_summary):
        role = str(spec.get("role", "")).strip()
        session = str(spec.get("session", "")).strip()
        log_file = str(spec.get("log_file", "")).strip()
        session_row = {
            "role": role,
            "session": session,
            "log_file": log_file,
            "execution_lane_ids": list(spec.get("execution_lane_ids") or []),
            "execution_subtask_ids": list(spec.get("execution_subtask_ids") or []),
            "review_lane_ids": list(spec.get("review_lane_ids") or []),
            "review_depends_on": list(spec.get("review_depends_on") or []),
        }
        result["sessions"].append(session_row)
        if not session:
            result["failed"].append({"role": role, "error": "missing session"})
            continue
        if run_command(["tmux", "has-session", "-t", session], env=None, timeout_sec=10).returncode == 0:
            result["existing"].append(dict(session_row))
            continue
        proc = run_command(
            ["tmux", "new-session", "-d", "-s", session, "-c", str(args.project_root), "bash", "-lc", str(spec.get("shell", "")).strip()],
            env=None,
            timeout_sec=20,
        )
        if proc.returncode != 0:
            result["failed"].append(
                {
                    "role": role,
                    "session": session,
                    "error": ((proc.stderr or proc.stdout or "").strip()[:400] or f"exit={proc.returncode}"),
                }
            )
            continue
        result["spawned"].append(dict(session_row))
    return result


def cleanup_tf_worker_sessions(tf_entry: Dict[str, Any], *, run_command) -> None:
    if not isinstance(tf_entry, dict) or shutil.which("tmux") is None:
        return
    sessions = tf_entry.get("worker_sessions")
    if not isinstance(sessions, list):
        return
    for row in sessions:
        if not isinstance(row, dict):
            continue
        session = str(row.get("session", "")).strip()
        if not session:
            continue
        try:
            _ = run_command(["tmux", "kill-session", "-t", session], env=None, timeout_sec=10)
        except Exception:
            continue


def parse_roles_csv(raw: Optional[str]) -> List[str]:
    items = re.split(r"[\s,;/]+", str(raw or "").strip())
    return dedupe_roles(item for item in items if str(item).strip())


def phase2_execution_lane_summary(metadata: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    data = metadata if isinstance(metadata, dict) else {}
    plan = data.get("phase2_execution_plan")
    if not isinstance(plan, dict):
        return {
            "execution_lanes": [],
            "review_lanes": [],
            "execution_roles": [],
            "review_roles": [],
            "planned_roles": [],
            "parallel_workers": False,
            "parallel_reviews": False,
            "readonly": True,
        }

    execution_rows = plan.get("execution_lanes") if isinstance(plan.get("execution_lanes"), list) else []
    review_rows = plan.get("review_lanes") if isinstance(plan.get("review_lanes"), list) else []
    execution_roles = dedupe_roles(
        str(row.get("role", "")).strip()
        for row in execution_rows
        if isinstance(row, dict) and str(row.get("role", "")).strip()
    )
    review_roles = dedupe_roles(
        str(row.get("role", "")).strip()
        for row in review_rows
        if isinstance(row, dict) and str(row.get("role", "")).strip()
    )
    return {
        "execution_lanes": execution_rows,
        "review_lanes": review_rows,
        "execution_roles": execution_roles,
        "review_roles": review_roles,
        "planned_roles": dedupe_roles(execution_roles + review_roles),
        "parallel_workers": bool(plan.get("parallel_workers", len(execution_rows) > 1)),
        "parallel_reviews": bool(plan.get("parallel_reviews", len(review_rows) > 1)),
        "readonly": bool(plan.get("readonly", True)),
    }


def merge_worker_roles_with_lane_summary(preview_roles: List[str], lane_summary: Optional[Dict[str, Any]]) -> List[str]:
    planned = lane_summary.get("planned_roles") if isinstance(lane_summary, dict) else []
    planned_roles = [str(role).strip() for role in (planned or []) if str(role).strip()]
    preview = [str(role).strip() for role in (preview_roles or []) if str(role).strip()]
    if planned_roles:
        return dedupe_roles(planned_roles + preview)
    return dedupe_roles(preview)


def lane_summary_subset(lane_summary: Optional[Dict[str, Any]], *, phase: str) -> Dict[str, Any]:
    data = lane_summary if isinstance(lane_summary, dict) else {}
    execution_rows = data.get("execution_lanes") if isinstance(data.get("execution_lanes"), list) else []
    review_rows = data.get("review_lanes") if isinstance(data.get("review_lanes"), list) else []
    if phase == "execution":
        exec_rows = execution_rows
        rev_rows: List[Dict[str, Any]] = []
    elif phase == "review":
        exec_rows = []
        rev_rows = review_rows
    else:
        exec_rows = execution_rows
        rev_rows = review_rows

    execution_roles = dedupe_roles(
        str(row.get("role", "")).strip()
        for row in exec_rows
        if isinstance(row, dict) and str(row.get("role", "")).strip()
    )
    review_roles = dedupe_roles(
        str(row.get("role", "")).strip()
        for row in rev_rows
        if isinstance(row, dict) and str(row.get("role", "")).strip()
    )
    return {
        "execution_lanes": exec_rows,
        "review_lanes": rev_rows,
        "execution_roles": execution_roles,
        "review_roles": review_roles,
        "planned_roles": dedupe_roles(execution_roles + review_roles),
        "parallel_workers": bool(data.get("parallel_workers", len(exec_rows) > 1)) if exec_rows else False,
        "parallel_reviews": bool(data.get("parallel_reviews", len(rev_rows) > 1)) if rev_rows else False,
        "readonly": bool(data.get("readonly", True)),
    }


def stage_review_prompt(base_prompt: str, execution_state: Dict[str, Any], review_lane_summary: Dict[str, Any]) -> str:
    exec_request_id = str(execution_state.get("request_id", "")).strip() or str(execution_state.get("gateway_request_id", "")).strip()
    review_rows = review_lane_summary.get("review_lanes") if isinstance(review_lane_summary, dict) else []
    lane_lines: List[str] = []
    for row in review_rows[:6]:
        if not isinstance(row, dict):
            continue
        lane_id = str(row.get("lane_id", "")).strip() or "R"
        role = str(row.get("role", "")).strip() or "Reviewer"
        kind = str(row.get("kind", "")).strip() or "verifier"
        depends_on = [str(x).strip() for x in (row.get("depends_on") or []) if str(x).strip()]
        suffix = f" after {', '.join(depends_on)}" if depends_on else ""
        lane_lines.append(f"- {lane_id} [{role}/{kind}]{suffix}")
    lane_block = "\n".join(lane_lines) if lane_lines else "- reviewer lane"
    return (
        str(base_prompt or "").rstrip()
        + "\n\n"
        + "Phase2 review-only pass.\n"
        + "Review the completed execution outputs. Do not start new implementation work.\n"
        + (f"Execution request: {exec_request_id}\n" if exec_request_id else "")
        + "Review lanes:\n"
        + lane_block
        + "\n"
    )


def merge_request_states(execution_state: Dict[str, Any], review_state: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(review_state, dict) or not review_state:
        merged = dict(execution_state or {})
        linked = [str(merged.get("request_id", "")).strip()]
        merged["linked_request_ids"] = [token for token in linked if token]
        return merged

    exec_data = execution_state if isinstance(execution_state, dict) else {}
    review_data = review_state if isinstance(review_state, dict) else {}
    merged = dict(exec_data)
    merged["request_id"] = str(exec_data.get("request_id", "")).strip() or str(review_data.get("request_id", "")).strip()
    merged["review_request_id"] = str(review_data.get("request_id", "")).strip()
    linked_ids = dedupe_roles(
        [
            str(exec_data.get("request_id", "")).strip(),
            str(review_data.get("request_id", "")).strip(),
        ]
    )
    if linked_ids:
        merged["linked_request_ids"] = linked_ids
    merged["complete"] = bool(exec_data.get("complete", False)) and bool(review_data.get("complete", False))
    merged["timed_out"] = bool(exec_data.get("timed_out", False)) or bool(review_data.get("timed_out", False))

    replies = []
    for row in (exec_data.get("replies") or []):
        replies.append(row)
    for row in (review_data.get("replies") or []):
        replies.append(row)
    if replies:
        merged["replies"] = replies

    reply_messages = []
    for row in (exec_data.get("reply_messages") or []):
        reply_messages.append(row)
    for row in (review_data.get("reply_messages") or []):
        reply_messages.append(row)
    if reply_messages:
        merged["reply_messages"] = reply_messages

    role_rows: Dict[str, Dict[str, Any]] = {}
    status_rank = {"failed": 4, "error": 4, "fail": 4, "running": 3, "done": 2, "pending": 1}
    for source in ((exec_data.get("role_states") or exec_data.get("roles") or []), (review_data.get("role_states") or review_data.get("roles") or [])):
        for row in source:
            if not isinstance(row, dict):
                continue
            role = str(row.get("role", "")).strip()
            if not role:
                continue
            status = str(row.get("status", "pending")).strip().lower() or "pending"
            prev = role_rows.get(role)
            if prev is None or status_rank.get(status, 0) >= status_rank.get(str(prev.get("status", "")).strip().lower(), 0):
                role_rows[role] = dict(row)
    if role_rows:
        merged["role_states"] = list(role_rows.values())

    done_roles = dedupe_roles(list(exec_data.get("done_roles") or []) + list(review_data.get("done_roles") or []))
    failed_roles = dedupe_roles(list(exec_data.get("failed_roles") or []) + list(review_data.get("failed_roles") or []))
    pending_roles = dedupe_roles(list(exec_data.get("pending_roles") or []) + list(review_data.get("pending_roles") or []))
    merged["done_roles"] = [role for role in done_roles if role not in failed_roles and role not in pending_roles]
    merged["failed_roles"] = failed_roles
    merged["pending_roles"] = [role for role in pending_roles if role not in failed_roles]

    exec_counts = exec_data.get("counts") if isinstance(exec_data.get("counts"), dict) else {}
    review_counts = review_data.get("counts") if isinstance(review_data.get("counts"), dict) else {}
    merged["counts"] = {
        "assignments": int(exec_counts.get("assignments", len(exec_data.get("role_states") or exec_data.get("roles") or [])) or 0)
        + int(review_counts.get("assignments", len(review_data.get("role_states") or review_data.get("roles") or [])) or 0),
        "replies": int(exec_counts.get("replies", len(exec_data.get("replies") or [])) or 0)
        + int(review_counts.get("replies", len(review_data.get("replies") or [])) or 0),
    }
    merged["phase2_review_triggered"] = True
    return merged


def lane_request_id(base_request_id: str, stage_name: str, lane_id: str) -> str:
    base = sanitize_fs_token(str(base_request_id or "").strip(), "req")[:48]
    stage = sanitize_fs_token(str(stage_name or "").strip().lower(), "stage")[:16]
    lane = sanitize_fs_token(str(lane_id or "").strip().upper(), "lane")[:16]
    return f"{base}-{stage}-{lane}"


def single_lane_summary(lane_summary: Optional[Dict[str, Any]], *, phase: str, lane_row: Dict[str, Any]) -> Dict[str, Any]:
    data = lane_summary if isinstance(lane_summary, dict) else {}
    if phase == "review":
        rev_rows = [copy.deepcopy(lane_row)]
        exec_rows: List[Dict[str, Any]] = []
    else:
        exec_rows = [copy.deepcopy(lane_row)]
        rev_rows = []
    execution_roles = dedupe_roles(
        str(row.get("role", "")).strip()
        for row in exec_rows
        if isinstance(row, dict) and str(row.get("role", "")).strip()
    )
    review_roles = dedupe_roles(
        str(row.get("role", "")).strip()
        for row in rev_rows
        if isinstance(row, dict) and str(row.get("role", "")).strip()
    )
    return {
        "execution_lanes": exec_rows,
        "review_lanes": rev_rows,
        "execution_roles": execution_roles,
        "review_roles": review_roles,
        "planned_roles": dedupe_roles(execution_roles + review_roles),
        "parallel_workers": False,
        "parallel_reviews": False,
        "readonly": bool(data.get("readonly", True)),
    }


def aggregate_parallel_stage_states(
    states: List[Dict[str, Any]],
    *,
    gateway_request_id: str,
    phase2_stage: str,
) -> Dict[str, Any]:
    ordered_states = [row for row in states if isinstance(row, dict)]
    linked_tokens: List[str] = []
    for row in ordered_states:
        nested_linked = row.get("linked_request_ids") if isinstance(row.get("linked_request_ids"), list) else []
        if nested_linked:
            linked_tokens.extend(str(item).strip() for item in nested_linked if str(item).strip())
            continue
        token = str(row.get("request_id", "")).strip() or str(row.get("gateway_request_id", "")).strip()
        if token:
            linked_tokens.append(token)
    linked_ids = dedupe_roles(linked_tokens)
    aggregate: Dict[str, Any] = {
        "request_id": str(gateway_request_id or (linked_ids[0] if linked_ids else "")).strip(),
        "gateway_request_id": str(gateway_request_id or "").strip(),
        "phase2_stage": str(phase2_stage or "").strip(),
        "linked_request_ids": linked_ids,
        "complete": bool(ordered_states) and all(bool(row.get("complete", False)) for row in ordered_states),
        "timed_out": any(bool(row.get("timed_out", False)) for row in ordered_states),
        "replies": [],
        "reply_messages": [],
        "role_states": [],
        "done_roles": [],
        "failed_roles": [],
        "pending_roles": [],
        "counts": {"assignments": 0, "replies": 0},
    }

    role_rows: Dict[str, Dict[str, Any]] = {}
    status_rank = {"failed": 4, "error": 4, "fail": 4, "running": 3, "done": 2, "pending": 1}
    done_roles: List[str] = []
    failed_roles: List[str] = []
    pending_roles: List[str] = []

    for row in ordered_states:
        aggregate["replies"].extend(row.get("replies") or [])
        aggregate["reply_messages"].extend(row.get("reply_messages") or [])
        counts = row.get("counts") if isinstance(row.get("counts"), dict) else {}
        aggregate["counts"]["assignments"] += int(counts.get("assignments", len(row.get("role_states") or row.get("roles") or [])) or 0)
        aggregate["counts"]["replies"] += int(counts.get("replies", len(row.get("replies") or [])) or 0)
        for role_row in (row.get("role_states") or row.get("roles") or []):
            if not isinstance(role_row, dict):
                continue
            role = str(role_row.get("role", "")).strip()
            if not role:
                continue
            status = str(role_row.get("status", "pending")).strip().lower() or "pending"
            prev = role_rows.get(role)
            if prev is None or status_rank.get(status, 0) >= status_rank.get(str(prev.get("status", "")).strip().lower(), 0):
                role_rows[role] = dict(role_row)
        done_roles.extend(list(row.get("done_roles") or []))
        failed_roles.extend(list(row.get("failed_roles") or []))
        pending_roles.extend(list(row.get("pending_roles") or []))

    aggregate["role_states"] = list(role_rows.values())
    aggregate["done_roles"] = [role for role in dedupe_roles(done_roles) if role not in dedupe_roles(failed_roles + pending_roles)]
    aggregate["failed_roles"] = dedupe_roles(failed_roles)
    aggregate["pending_roles"] = [role for role in dedupe_roles(pending_roles) if role not in aggregate["failed_roles"]]
    return aggregate


def parse_json_object_from_text(text: str) -> Optional[Dict[str, Any]]:
    payload = str(text or "").strip()
    if not payload:
        return None
    try:
        data = json.loads(payload)
        return data if isinstance(data, dict) else None
    except Exception:
        pass
    match = re.search(r"(\{[\s\S]*\})", payload)
    if not match:
        return None
    try:
        data = json.loads(match.group(1))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def resolve_dispatch_roles_from_preview(
    args: argparse.Namespace,
    prompt: str,
    request_id: str,
    roles_override: str,
    priority: str,
    timeout_sec: int,
    *,
    run_command,
) -> List[str]:
    cmd: List[str] = [
        args.aoe_orch_bin,
        "run",
        "--project-root",
        str(args.project_root),
        "--team-dir",
        str(args.team_dir),
        "--priority",
        priority,
        "--request-id",
        request_id,
        "--timeout-sec",
        str(max(1, int(timeout_sec))),
        "--poll-sec",
        str(args.orch_poll_sec),
        "--json",
        "--dry-run",
        "--no-spawn-missing",
    ]
    if roles_override:
        cmd.extend(["--roles", roles_override])
    cmd.append(prompt)
    proc = run_command(cmd, env=None, timeout_sec=max(30, min(300, int(args.orch_command_timeout_sec))))
    payload = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        raise RuntimeError(f"aoe-orch run dry-run failed: {payload[:1000]}")
    data = parse_json_object_from_text(payload)
    if data is None:
        raise RuntimeError(f"aoe-orch run dry-run returned non-JSON output: {payload[:800]}")

    roles: List[str] = []
    dispatch_plan = data.get("dispatch_plan")
    if isinstance(dispatch_plan, list):
        for row in dispatch_plan:
            if not isinstance(row, dict):
                continue
            role = str(row.get("role", "")).strip()
            if role:
                roles.append(role)
    if roles:
        return dedupe_roles(roles)
    return parse_roles_csv(roles_override)


def load_tf_exec_meta(team_dir: Path, request_id: str, default_tf_exec_map_file: str) -> Dict[str, Any]:
    token = str(request_id or "").strip()
    if not token:
        return {}
    tf_map = load_tf_exec_map(team_dir, default_tf_exec_map_file)
    row = tf_map.get(token) if isinstance(tf_map, dict) else None
    return row if isinstance(row, dict) else {}


def sync_task_exec_context(
    entry: Dict[str, Any],
    task: Dict[str, Any],
    *,
    build_task_context,
    default_tf_exec_map_file: str,
    now_iso,
) -> Dict[str, str]:
    request_id = str(task.get("request_id", "")).strip()
    team_dir_raw = str(entry.get("team_dir", "")).strip() if isinstance(entry, dict) else ""
    team_dir = Path(team_dir_raw).expanduser().resolve() if team_dir_raw else None

    tf_meta = load_tf_exec_meta(team_dir, request_id, default_tf_exec_map_file) if team_dir is not None else {}
    if not tf_meta and team_dir is not None and isinstance(task.get("result"), dict):
        linked = task["result"].get("linked_request_ids")
        if isinstance(linked, list):
            for candidate in linked:
                token = str(candidate).strip()
                if not token:
                    continue
                tf_meta = load_tf_exec_meta(team_dir, token, default_tf_exec_map_file)
                if tf_meta:
                    break
    context = build_task_context(request_id=request_id, entry=entry, task=task, tf_meta=tf_meta)
    if context:
        task["context"] = context

    if team_dir is None or not request_id:
        return context

    tf_map = load_tf_exec_map(team_dir, default_tf_exec_map_file)
    row = tf_map.get(request_id) if isinstance(tf_map, dict) else None
    if not isinstance(row, dict) and isinstance(task.get("result"), dict):
        linked = task["result"].get("linked_request_ids")
        if isinstance(linked, list):
            for candidate in linked:
                token = str(candidate).strip()
                if not token:
                    continue
                row = tf_map.get(token) if isinstance(tf_map, dict) else None
                if isinstance(row, dict):
                    request_id = token
                    break
    if not isinstance(row, dict):
        return context

    changed = False
    updates = {
        "project_key": context.get("project_key", ""),
        "project_alias": context.get("project_alias", ""),
        "project_root": context.get("project_root", ""),
        "team_dir": context.get("team_dir", ""),
        "tf_id": context.get("tf_id", ""),
        "task_short_id": context.get("task_short_id", ""),
        "task_alias": context.get("task_alias", ""),
        "workdir": context.get("workdir", ""),
        "run_dir": context.get("run_dir", ""),
        "branch": context.get("branch", ""),
        "control_mode": context.get("control_mode", ""),
        "source_request_id": context.get("source_request_id", ""),
        "gateway_request_id": context.get("gateway_request_id", "") or request_id,
    }
    exec_mode = context.get("exec_mode", "")
    if exec_mode and str(row.get("mode", "")).strip() != exec_mode:
        row["mode"] = exec_mode
        changed = True

    for key, value in updates.items():
        if str(row.get(key, "")).strip() != str(value or "").strip():
            row[key] = value
            changed = True

    if changed:
        row["updated_at"] = now_iso()
        tf_map[request_id] = row
        save_tf_exec_map(team_dir, tf_map, default_tf_exec_map_file)

    return context


def finalize_tf_exec_meta(
    team_dir: Path,
    request_id: str,
    state: Dict[str, Any],
    *,
    default_tf_exec_map_file: str,
    now_iso,
) -> None:
    token = str(request_id or "").strip()
    if not token:
        return
    tf_map = load_tf_exec_map(team_dir, default_tf_exec_map_file)
    tf_row = tf_map.get(token) if isinstance(tf_map, dict) else None
    if not isinstance(tf_row, dict):
        return

    complete = bool(state.get("complete", False))
    timed_out = bool(state.get("timed_out", False))
    replies = state.get("replies") or state.get("reply_messages") or []
    roles = state.get("roles") or state.get("role_states") or []
    failed_roles = 0
    if isinstance(roles, list):
        for role_row in roles:
            if not isinstance(role_row, dict):
                continue
            role_status = str(role_row.get("status", "")).strip().lower()
            if role_status in {"failed", "fail", "error"}:
                failed_roles += 1

    if timed_out or failed_roles > 0:
        status = "failed"
    elif complete:
        status = "completed"
    else:
        status = "running"

    closed_at = now_iso()
    tf_row["status"] = status
    tf_row["complete"] = complete
    tf_row["timed_out"] = timed_out
    tf_row["reply_count"] = len(replies) if isinstance(replies, list) else 0
    tf_row["role_count"] = len(roles) if isinstance(roles, list) else 0
    tf_row["failed_role_count"] = failed_roles
    tf_row["updated_at"] = closed_at
    if status in {"completed", "failed"}:
        tf_row["closed_at"] = closed_at
    tf_map[token] = tf_row
    save_tf_exec_map(team_dir, tf_map, default_tf_exec_map_file)

    run_dir_raw = str(tf_row.get("run_dir", "") or "").strip()
    if not run_dir_raw:
        return
    try:
        run_dir = Path(run_dir_raw).expanduser()
        meta_path = run_dir / "meta.json"
        run_meta: Dict[str, Any] = {}
        if meta_path.exists():
            loaded = json.loads(meta_path.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                run_meta = loaded
        run_meta.update(tf_row)
        meta_path.write_text(json.dumps(run_meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except Exception:
        pass


def tf_work_root(project_root: Path, default_tf_work_root_name: str) -> Path:
    raw = str(os.environ.get("AOE_TF_WORK_ROOT", "") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return (project_root.parent / default_tf_work_root_name).resolve()


def normalize_tf_exec_mode(raw: Optional[str], default_tf_exec_mode: str) -> str:
    token = str(raw or "").strip().lower()
    if not token:
        token = str(os.environ.get("AOE_TF_EXEC_MODE", default_tf_exec_mode) or "").strip().lower()
    if token in {"0", "off", "none", "disable", "disabled"}:
        return "none"
    if token in {"inplace", "workspace", "project", "root"}:
        return "inplace"
    return "worktree"


def normalize_tf_exec_retention() -> str:
    token = str(os.environ.get("AOE_TF_ARTIFACT_POLICY", "success-only") or "").strip().lower()
    if token in {"all", "keep-all"}:
        return "all"
    if token in {"none", "off"}:
        return "none"
    return "success-only"


def tf_exec_cache_ttl_hours(*, int_from_env, default_ttl_hours: int) -> int:
    return int_from_env(
        os.environ.get("AOE_TF_EXEC_CACHE_TTL_HOURS"),
        default_ttl_hours,
        minimum=0,
        maximum=8760,
    )


def is_git_repo(path: Path, *, run_command) -> bool:
    proc = run_command(["git", "-C", str(path), "rev-parse", "--is-inside-work-tree"], env=None, timeout_sec=15)
    return proc.returncode == 0 and (proc.stdout or "").strip() in {"true", "TRUE", "1"}


def git_worktree_add(repo_root: Path, workdir: Path, branch: str, *, run_command) -> Tuple[bool, str]:
    if workdir.exists():
        return False, f"workdir exists: {workdir}"
    workdir.parent.mkdir(parents=True, exist_ok=True)
    proc = run_command(["git", "-C", str(repo_root), "worktree", "add", "-b", branch, str(workdir), "HEAD"], env=None, timeout_sec=180)
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        return False, detail[:1200]
    return True, ""


def git_worktree_remove(repo_root: Path, workdir: Path, *, run_command) -> None:
    _ = run_command(["git", "-C", str(repo_root), "worktree", "remove", "--force", str(workdir)], env=None, timeout_sec=180)


def git_branch_delete(repo_root: Path, branch: str, *, run_command) -> None:
    if not branch:
        return
    _ = run_command(["git", "-C", str(repo_root), "branch", "-D", branch], env=None, timeout_sec=60)


def ensure_tf_exec_workspace(
    args: argparse.Namespace,
    request_id: str,
    *,
    metadata: Optional[Dict[str, Any]] = None,
    default_tf_exec_mode: str,
    default_tf_work_root_name: str,
    default_tf_exec_map_file: str,
    now_iso,
    run_command,
) -> Dict[str, Any]:
    team_dir: Path = args.team_dir
    project_root: Path = args.project_root
    project_key = normalize_project_name(str(getattr(args, "_aoe_project_key", "") or project_root.name))
    project_alias = normalize_project_alias(str(getattr(args, "_aoe_project_alias", "")))
    control_mode = str(getattr(args, "_aoe_control_mode", "")).strip().lower()
    source_request_id = str(getattr(args, "_aoe_source_request_id", "")).strip()

    mode = normalize_tf_exec_mode(None, default_tf_exec_mode)
    run_dir = (team_dir / "tf_runs" / request_id).resolve()
    run_dir.mkdir(parents=True, exist_ok=True)

    workdir = project_root.resolve()
    repo_root = project_root.resolve()
    branch = ""
    created_worktree = False
    failure_reason = ""

    if mode == "worktree":
        if is_git_repo(project_root, run_command=run_command):
            base = tf_work_root(project_root, default_tf_work_root_name)
            proj_tag = sanitize_fs_token(project_root.name, "project")
            workdir = (base / proj_tag / request_id).resolve()
            branch = f"aoe/tf/{request_id}"
            created_worktree, failure_reason = git_worktree_add(repo_root, workdir, branch, run_command=run_command)
            if not created_worktree:
                mode = "inplace"
                workdir = project_root.resolve()
                branch = ""
        else:
            mode = "inplace"

    meta: Dict[str, Any] = {
        "request_id": request_id,
        "gateway_request_id": request_id,
        "created_at": now_iso(),
        "mode": mode,
        "project_key": project_key,
        "project_alias": project_alias,
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tf_id": request_to_tf_id(request_id),
        "task_short_id": "",
        "task_alias": "",
        "control_mode": control_mode,
        "source_request_id": source_request_id,
        "repo_root": str(repo_root),
        "workdir": str(workdir),
        "run_dir": str(run_dir),
        "branch": branch,
        "worktree_created": bool(created_worktree),
        "worktree_error": failure_reason[:400] if failure_reason else "",
        "status": "running",
    }

    if isinstance(metadata, dict):
        phase2_team_spec = metadata.get("phase2_team_spec")
        if isinstance(phase2_team_spec, dict) and phase2_team_spec:
            meta["phase2_team_spec"] = phase2_team_spec
        phase2_execution_plan = metadata.get("phase2_execution_plan")
        if isinstance(phase2_execution_plan, dict) and phase2_execution_plan:
            meta["phase2_execution_plan"] = phase2_execution_plan
        phase1_mode = str(metadata.get("phase1_mode", "")).strip().lower()
        if phase1_mode:
            meta["phase1_mode"] = phase1_mode
        try:
            phase1_rounds = max(0, int(metadata.get("phase1_rounds", 0) or 0))
        except Exception:
            phase1_rounds = 0
        if phase1_rounds > 0:
            meta["phase1_rounds"] = phase1_rounds
        phase1_providers = metadata.get("phase1_providers")
        if isinstance(phase1_providers, list) and phase1_providers:
            meta["phase1_providers"] = [
                str(row).strip() for row in phase1_providers if str(row).strip()
            ]
        lane_summary = phase2_execution_lane_summary(metadata)
        if lane_summary["execution_roles"]:
            meta["execution_lane_roles"] = list(lane_summary["execution_roles"])
        if lane_summary["review_roles"]:
            meta["review_lane_roles"] = list(lane_summary["review_roles"])
        if lane_summary["planned_roles"]:
            meta["planned_roles_from_lanes"] = list(lane_summary["planned_roles"])
        meta["parallel_workers"] = bool(lane_summary["parallel_workers"])
        meta["parallel_reviews"] = bool(lane_summary["parallel_reviews"])
        meta["readonly"] = bool(lane_summary["readonly"])

    try:
        (run_dir / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    except Exception:
        pass

    tf_map = load_tf_exec_map(team_dir, default_tf_exec_map_file)
    tf_map[str(request_id)] = meta
    save_tf_exec_map(team_dir, tf_map, default_tf_exec_map_file)
    return meta


def task_exec_verdict(task: Dict[str, Any]) -> str:
    ec = task.get("exec_critic") if isinstance(task, dict) else None
    if not isinstance(ec, dict):
        return "-"
    verdict = str(ec.get("verdict", "")).strip().lower()
    return verdict if verdict in {"success", "retry", "fail"} else "-"


def is_task_success(task: Dict[str, Any]) -> bool:
    status = str(task.get("status", "")).strip().lower()
    if status != "completed":
        return False
    verdict = task_exec_verdict(task)
    if verdict in {"retry", "fail"}:
        return False
    return True


def cleanup_tf_exec_entry(entry: Dict[str, Any], *, run_command) -> None:
    if not isinstance(entry, dict):
        return
    cleanup_tf_worker_sessions(entry, run_command=run_command)
    mode = str(entry.get("mode", "")).strip().lower()
    repo_root = Path(str(entry.get("repo_root", "") or "")).expanduser()
    workdir = Path(str(entry.get("workdir", "") or "")).expanduser()
    run_dir = Path(str(entry.get("run_dir", "") or "")).expanduser()
    branch = str(entry.get("branch", "")).strip()

    if mode == "worktree":
        try:
            if repo_root and repo_root.exists():
                git_worktree_remove(repo_root, workdir, run_command=run_command)
                git_branch_delete(repo_root, branch, run_command=run_command)
        except Exception:
            pass
        try:
            if workdir.exists():
                shutil.rmtree(workdir, ignore_errors=True)
        except Exception:
            pass

    try:
        if run_dir.exists():
            shutil.rmtree(run_dir, ignore_errors=True)
    except Exception:
        pass


def cleanup_tf_exec_artifacts(
    manager_state_path: Path,
    state: Dict[str, Any],
    *,
    default_tf_exec_map_file: str,
    default_tf_exec_cache_ttl_hours: int,
    now_iso,
    parse_iso_ts,
    int_from_env,
    run_command,
) -> int:
    if not isinstance(state, dict):
        return 0
    team_dir = manager_state_path.parent.resolve()
    tf_map = load_tf_exec_map(team_dir, default_tf_exec_map_file)
    if not tf_map:
        return 0

    tasks_by_id: Dict[str, Dict[str, Any]] = {}
    projects = state.get("projects") if isinstance(state.get("projects"), dict) else {}
    for _key, entry in (projects or {}).items():
        if not isinstance(entry, dict):
            continue
        tasks = entry.get("tasks") if isinstance(entry.get("tasks"), dict) else {}
        for rid, task in (tasks or {}).items():
            if isinstance(task, dict):
                tasks_by_id[str(rid)] = task

    retention = normalize_tf_exec_retention()
    ttl_hours = tf_exec_cache_ttl_hours(int_from_env=int_from_env, default_ttl_hours=default_tf_exec_cache_ttl_hours) if retention != "all" else 0
    now_utc = datetime.now(timezone.utc)
    removed_count = 0
    changed = False
    for rid, entry in list(tf_map.items()):
        task = tasks_by_id.get(str(rid))
        if not isinstance(task, dict):
            continue
        status = str(task.get("status", "")).strip().lower()
        if status not in {"completed", "failed"}:
            continue
        success = is_task_success(task)

        if retention == "none":
            should_delete = True
        elif retention == "all":
            should_delete = False
        else:
            should_delete = not success

        if should_delete:
            cleanup_tf_exec_entry(entry if isinstance(entry, dict) else {}, run_command=run_command)
            del tf_map[rid]
            removed_count += 1
            changed = True
        else:
            if ttl_hours > 0:
                closed_raw = str(task.get("updated_at", "")).strip() or str(task.get("created_at", "")).strip()
                if not closed_raw and isinstance(entry, dict):
                    closed_raw = str(entry.get("updated_at", "")).strip() or str(entry.get("created_at", "")).strip()
                closed_ts = parse_iso_ts(closed_raw)
                if closed_ts is not None and closed_ts.tzinfo is None:
                    closed_ts = closed_ts.replace(tzinfo=timezone.utc)
                if closed_ts is not None:
                    age = (now_utc - closed_ts.astimezone(timezone.utc)).total_seconds()
                    if age > (float(ttl_hours) * 3600.0):
                        cleanup_tf_exec_entry(entry if isinstance(entry, dict) else {}, run_command=run_command)
                        del tf_map[rid]
                        removed_count += 1
                        changed = True
                        continue
            if isinstance(entry, dict) and str(entry.get("status", "")) != status:
                entry["status"] = status
                entry["exec_verdict"] = task_exec_verdict(task)
                entry["updated_at"] = now_iso()
                tf_map[rid] = entry
                changed = True

    if changed:
        save_tf_exec_map(team_dir, tf_map, default_tf_exec_map_file)
    return removed_count


def run_aoe_orch(
    args: argparse.Namespace,
    prompt: str,
    chat_id: str,
    *,
    default_tf_exec_mode: str,
    default_tf_work_root_name: str,
    default_tf_exec_map_file: str,
    default_tf_worker_startup_grace_sec: int,
    now_iso,
    run_command,
    roles_override: Optional[str] = None,
    priority_override: Optional[str] = None,
    timeout_override: Optional[int] = None,
    no_wait_override: Optional[bool] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    lane_summary = phase2_execution_lane_summary(metadata)
    effective_roles = args.roles if roles_override is None else (roles_override or "")
    if not str(effective_roles or "").strip() and lane_summary["planned_roles"]:
        effective_roles = ",".join(str(role).strip() for role in lane_summary["planned_roles"] if str(role).strip())
    effective_priority = (priority_override or args.priority or "P2").upper().strip()
    if effective_priority not in {"P1", "P2", "P3"}:
        effective_priority = "P2"
    effective_timeout = max(1, int(args.orch_timeout_sec if timeout_override is None else timeout_override))
    effective_no_wait = bool(args.no_wait if no_wait_override is None else no_wait_override)

    def cleanup_request_artifacts(request_id: str, tf_meta: Dict[str, Any]) -> None:
        try:
            cleanup_tf_worker_sessions(tf_meta, run_command=run_command)
            cleanup_tf_exec_entry(tf_meta, run_command=run_command)
            tf_map = load_tf_exec_map(args.team_dir, default_tf_exec_map_file)
            if request_id in tf_map:
                del tf_map[request_id]
                save_tf_exec_map(args.team_dir, tf_map, default_tf_exec_map_file)
        except Exception:
            pass

    def requested_stage_request_id(stage_name: str, stage_metadata: Optional[Dict[str, Any]]) -> str:
        if not isinstance(stage_metadata, dict):
            return ""
        token = str(stage_metadata.get("request_id", "") or stage_metadata.get("gateway_request_id", "")).strip()
        return token

    def execute_stage(
        *,
        stage_name: str,
        stage_prompt: str,
        stage_roles_csv: str,
        stage_lane_summary: Dict[str, Any],
        stage_metadata: Optional[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
        request_id = requested_stage_request_id(stage_name, stage_metadata) or create_request_id()
        tf_meta = ensure_tf_exec_workspace(
            args,
            request_id,
            metadata=stage_metadata,
            default_tf_exec_mode=default_tf_exec_mode,
            default_tf_work_root_name=default_tf_work_root_name,
            default_tf_exec_map_file=default_tf_exec_map_file,
            now_iso=now_iso,
            run_command=run_command,
        )
        try:
            preview_roles = resolve_dispatch_roles_from_preview(
                args,
                stage_prompt,
                request_id=request_id,
                roles_override=stage_roles_csv,
                priority=effective_priority,
                timeout_sec=effective_timeout,
                run_command=run_command,
            )
            worker_roles = merge_worker_roles_with_lane_summary(preview_roles, stage_lane_summary)
            if not worker_roles:
                raise RuntimeError(f"aoe-orch run preview resolved no worker roles for {stage_name}")

            worker_sessions = spawn_tf_worker_sessions(
                args,
                request_id=request_id,
                roles=worker_roles,
                startup_timeout_sec=(effective_timeout + default_tf_worker_startup_grace_sec),
                lane_summary=stage_lane_summary,
                run_command=run_command,
            )
            ready_count = len(worker_sessions.get("spawned") or []) + len(worker_sessions.get("existing") or [])
            if ready_count < len(worker_roles):
                cleanup_tf_worker_sessions({"worker_sessions": worker_sessions.get("sessions") or []}, run_command=run_command)
                detail_rows = worker_sessions.get("failed") or []
                detail = "; ".join(
                    f"{str(row.get('role', '?'))}:{str(row.get('error', 'spawn_failed'))}"
                    for row in detail_rows[:8]
                    if isinstance(row, dict)
                )
                raise RuntimeError(f"tf worker spawn failed: {detail or 'unknown error'}")

            tf_meta["phase2_stage"] = stage_name
            tf_meta["target_roles"] = dedupe_roles(worker_roles)
            if stage_lane_summary["execution_roles"]:
                tf_meta["execution_lane_roles"] = list(stage_lane_summary["execution_roles"])
            if stage_lane_summary["review_roles"]:
                tf_meta["review_lane_roles"] = list(stage_lane_summary["review_roles"])
            if stage_lane_summary["planned_roles"]:
                tf_meta["planned_roles_from_lanes"] = list(stage_lane_summary["planned_roles"])
            tf_meta["parallel_workers"] = bool(stage_lane_summary["parallel_workers"])
            tf_meta["parallel_reviews"] = bool(stage_lane_summary["parallel_reviews"])
            tf_meta["worker_sessions"] = worker_sessions.get("sessions") or []
            tf_meta["updated_at"] = now_iso()
            try:
                run_dir = Path(str(tf_meta.get("run_dir", "") or "")).expanduser()
                if run_dir:
                    (run_dir / "meta.json").write_text(json.dumps(tf_meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            except Exception:
                pass
            try:
                tf_map = load_tf_exec_map(args.team_dir, default_tf_exec_map_file)
                tf_map[str(request_id)] = tf_meta
                save_tf_exec_map(args.team_dir, tf_map, default_tf_exec_map_file)
            except Exception:
                pass
        except Exception:
            cleanup_request_artifacts(request_id, tf_meta)
            raise

        cmd: List[str] = [
            args.aoe_orch_bin,
            "run",
            "--project-root",
            str(args.project_root),
            "--team-dir",
            str(args.team_dir),
            "--priority",
            effective_priority,
            "--request-id",
            request_id,
            "--timeout-sec",
            str(effective_timeout),
            "--poll-sec",
            str(args.orch_poll_sec),
            "--channel",
            "telegram",
            "--origin",
            f"telegram:{chat_id}",
            "--json",
        ]
        if stage_roles_csv:
            cmd.extend(["--roles", stage_roles_csv])
        cmd.append("--no-spawn-missing")
        if effective_no_wait:
            cmd.append("--no-wait")
        cmd.append(stage_prompt)

        proc = run_command(cmd, env=None, timeout_sec=args.orch_command_timeout_sec)
        if proc.returncode != 0:
            cleanup_request_artifacts(request_id, tf_meta)
            detail = (proc.stderr or proc.stdout or "").strip()
            raise RuntimeError(f"aoe-orch run failed: {detail[:1000]}")

        payload = (proc.stdout or "").strip()
        try:
            data = json.loads(payload)
        except Exception as e:
            cleanup_request_artifacts(request_id, tf_meta)
            raise RuntimeError(f"aoe-orch run returned non-JSON output: {payload[:800]}") from e

        if not isinstance(data, dict):
            cleanup_request_artifacts(request_id, tf_meta)
            raise RuntimeError("aoe-orch run JSON is not an object")
        if str(data.get("request_id", "")).strip() and str(data.get("request_id", "")).strip() != request_id:
            data["gateway_request_id"] = request_id
        data["phase2_stage"] = stage_name
        data["tf_workers"] = worker_sessions
        data["planned_roles"] = dedupe_roles(worker_roles)
        if stage_lane_summary["planned_roles"]:
            data["planned_roles_from_lanes"] = list(stage_lane_summary["planned_roles"])
        if stage_lane_summary["execution_roles"]:
            data["execution_lane_roles"] = list(stage_lane_summary["execution_roles"])
        if stage_lane_summary["review_roles"]:
            data["review_lane_roles"] = list(stage_lane_summary["review_roles"])
        data["parallel_workers"] = bool(stage_lane_summary["parallel_workers"])
        data["parallel_reviews"] = bool(stage_lane_summary["parallel_reviews"])
        if isinstance(stage_metadata, dict) and stage_metadata:
            data["dispatch_metadata"] = dict(stage_metadata)
        try:
            finalize_tf_exec_meta(args.team_dir, request_id, data, default_tf_exec_map_file=default_tf_exec_map_file, now_iso=now_iso)
        except Exception:
            pass
        if not effective_no_wait:
            cleanup_tf_worker_sessions(tf_meta, run_command=run_command)
        return data, tf_meta, worker_roles

    def execute_stage_fanout(
        *,
        stage_name: str,
        stage_prompt: str,
        stage_lane_summary: Dict[str, Any],
        stage_metadata: Optional[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
        phase = "review" if stage_name == "review" else "execution"
        lane_rows = stage_lane_summary.get("review_lanes") if phase == "review" else stage_lane_summary.get("execution_lanes")
        if not isinstance(lane_rows, list) or len(lane_rows) <= 1:
            return execute_stage(
                stage_name=stage_name,
                stage_prompt=stage_prompt,
                stage_roles_csv=",".join(stage_lane_summary.get("planned_roles") or []),
                stage_lane_summary=stage_lane_summary,
                stage_metadata=stage_metadata,
            )

        parent_request_id = requested_stage_request_id(stage_name, stage_metadata) or create_request_id()
        parent_gateway_request_id = str(parent_request_id).strip()
        rows_with_ids = [
            (str(row.get("lane_id", "")).strip() or f"{phase[:1].upper()}{idx + 1}", row)
            for idx, row in enumerate(lane_rows)
            if isinstance(row, dict)
        ]
        lane_states: Dict[str, Dict[str, Any]] = {}
        lane_metas: Dict[str, Dict[str, Any]] = {}
        lane_role_rows: Dict[str, List[str]] = {}
        lane_workers: List[Dict[str, Any]] = []

        def _run_lane(lane_id: str, lane_row: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Dict[str, Any], List[str]]:
            lane_meta = dict(stage_metadata or {})
            lane_meta["request_id"] = lane_request_id(parent_gateway_request_id, stage_name, lane_id)
            lane_meta["gateway_request_id"] = parent_gateway_request_id
            lane_meta["phase2_stage"] = stage_name
            lane_meta["phase2_lane_id"] = lane_id
            lane_lane_summary = single_lane_summary(stage_lane_summary, phase=phase, lane_row=lane_row)
            lane_prompt = stage_prompt
            if phase == "review":
                lane_prompt = stage_review_prompt(stage_prompt, {"request_id": parent_gateway_request_id}, lane_lane_summary)
            return (
                lane_id,
                *execute_stage(
                    stage_name=stage_name,
                    stage_prompt=lane_prompt,
                    stage_roles_csv=str(lane_row.get("role", "")).strip(),
                    stage_lane_summary=lane_lane_summary,
                    stage_metadata=lane_meta,
                ),
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(rows_with_ids))) as executor:
            future_map = {
                executor.submit(_run_lane, lane_id, lane_row): lane_id
                for lane_id, lane_row in rows_with_ids
            }
            for future in concurrent.futures.as_completed(future_map):
                lane_id = future_map[future]
                result_lane_id, lane_state, lane_meta, lane_roles = future.result()
                lane_states[result_lane_id] = lane_state
                lane_metas[result_lane_id] = lane_meta
                lane_role_rows[result_lane_id] = lane_roles
                lane_workers.append(
                    {
                        "lane_id": result_lane_id,
                        "sessions": (lane_state.get("tf_workers") or {}).get("sessions") or [],
                        "execution_lane_ids": list((lane_state.get("tf_workers") or {}).get("sessions", [{}])[0].get("execution_lane_ids", []))
                        if isinstance((lane_state.get("tf_workers") or {}).get("sessions"), list) and (lane_state.get("tf_workers") or {}).get("sessions")
                        else [],
                        "review_lane_ids": list((lane_state.get("tf_workers") or {}).get("sessions", [{}])[0].get("review_lane_ids", []))
                        if isinstance((lane_state.get("tf_workers") or {}).get("sessions"), list) and (lane_state.get("tf_workers") or {}).get("sessions")
                        else [],
                    }
                )

        ordered_states = [lane_states[lane_id] for lane_id, _row in rows_with_ids if lane_id in lane_states]
        aggregate_state = aggregate_parallel_stage_states(
            ordered_states,
            gateway_request_id=parent_gateway_request_id,
            phase2_stage=stage_name,
        )
        aggregate_state["phase2_parallelized"] = True
        aggregate_state["planned_roles"] = dedupe_roles(
            role
            for lane_id, _row in rows_with_ids
            for role in (lane_role_rows.get(lane_id) or [])
        )
        aggregate_state["tf_workers"] = {
            "parallel": True,
            "lanes": lane_workers,
            "sessions": [
                session
                for row in lane_workers
                for session in (row.get("sessions") or [])
                if isinstance(session, dict)
            ],
        }
        if isinstance(stage_metadata, dict) and stage_metadata:
            aggregate_state["dispatch_metadata"] = dict(stage_metadata)
        first_meta = next((lane_metas[lane_id] for lane_id, _row in rows_with_ids if lane_id in lane_metas), {})
        if isinstance(first_meta, dict) and first_meta.get("phase2_execution_plan"):
            aggregate_state.setdefault("dispatch_metadata", {})["phase2_execution_plan"] = first_meta["phase2_execution_plan"]
        return aggregate_state, first_meta if isinstance(first_meta, dict) else {}, aggregate_state.get("planned_roles") or []

    stage_execution = lane_summary_subset(lane_summary, phase="execution")
    stage_review = lane_summary_subset(lane_summary, phase="review")
    staged_review = (
        not effective_no_wait
        and bool(stage_execution["planned_roles"])
        and bool(stage_review["planned_roles"])
    )

    stage1_roles_csv = effective_roles
    if staged_review and stage_execution["planned_roles"]:
        stage1_roles_csv = ",".join(stage_execution["planned_roles"])
    stage1_metadata = dict(metadata or {})
    if staged_review:
        stage1_metadata["phase2_stage"] = "execution"
    stage1_name = "execution" if staged_review else "combined"
    stage1_summary = stage_execution if staged_review else lane_summary
    stage1_parallel = (
        not effective_no_wait
        and stage1_name == "execution"
        and bool(stage1_summary.get("parallel_workers"))
        and len(stage1_summary.get("execution_lanes") or []) > 1
    )
    if stage1_parallel:
        stage1_state, stage1_meta, stage1_roles = execute_stage_fanout(
            stage_name=stage1_name,
            stage_prompt=prompt,
            stage_lane_summary=stage1_summary,
            stage_metadata=stage1_metadata,
        )
    else:
        stage1_state, stage1_meta, stage1_roles = execute_stage(
            stage_name=stage1_name,
            stage_prompt=prompt,
            stage_roles_csv=stage1_roles_csv,
            stage_lane_summary=stage1_summary,
            stage_metadata=stage1_metadata,
        )

    final_state = dict(stage1_state)
    if staged_review:
        exec_failed = bool(stage1_state.get("failed_roles") or [])
        exec_pending = bool(stage1_state.get("pending_roles") or [])
        exec_complete = bool(stage1_state.get("complete", False))
        if exec_complete and (not exec_failed) and (not exec_pending):
            review_prompt = stage_review_prompt(prompt, stage1_state, stage_review)
            stage2_metadata = dict(metadata or {})
            stage2_metadata.pop("request_id", None)
            stage2_metadata.pop("gateway_request_id", None)
            stage2_metadata["phase2_stage"] = "review"
            stage2_metadata["phase2_parent_request_id"] = str(stage1_state.get("request_id", "")).strip() or str(stage1_state.get("gateway_request_id", "")).strip()
            stage2_parallel = (
                bool(stage_review.get("parallel_reviews"))
                and len(stage_review.get("review_lanes") or []) > 1
            )
            if stage2_parallel:
                stage2_state, stage2_meta, stage2_roles = execute_stage_fanout(
                    stage_name="review",
                    stage_prompt=prompt,
                    stage_lane_summary=stage_review,
                    stage_metadata=stage2_metadata,
                )
            else:
                stage2_state, stage2_meta, stage2_roles = execute_stage(
                    stage_name="review",
                    stage_prompt=review_prompt,
                    stage_roles_csv=",".join(stage_review["planned_roles"]),
                    stage_lane_summary=stage_review,
                    stage_metadata=stage2_metadata,
                )
            final_state = aggregate_parallel_stage_states(
                [stage1_state, stage2_state],
                gateway_request_id=str(stage1_state.get("gateway_request_id", "")).strip() or str(stage1_state.get("request_id", "")).strip(),
                phase2_stage="combined",
            )
            final_state["phase2_review_triggered"] = True
            final_state["phase2_request_ids"] = {
                "execution": list(stage1_state.get("linked_request_ids") or []) or (
                    str(stage1_state.get("request_id", "")).strip() or str(stage1_state.get("gateway_request_id", "")).strip()
                ),
                "review": list(stage2_state.get("linked_request_ids") or []) or (
                    str(stage2_state.get("request_id", "")).strip() or str(stage2_state.get("gateway_request_id", "")).strip()
                ),
            }
            final_state["tf_workers"] = {
                "execution": stage1_state.get("tf_workers"),
                "review": stage2_state.get("tf_workers"),
            }
            final_state["planned_roles"] = dedupe_roles(stage1_roles + stage2_roles)
            final_state["request_id"] = str(stage1_state.get("gateway_request_id", "")).strip() or str(stage1_state.get("request_id", "")).strip()
            final_state["gateway_request_id"] = str(stage1_state.get("gateway_request_id", "")).strip() or str(stage1_state.get("request_id", "")).strip()
            if stage1_meta.get("phase2_execution_plan"):
                final_state.setdefault("dispatch_metadata", {})["phase2_execution_plan"] = stage1_meta["phase2_execution_plan"]
        else:
            final_state["phase2_review_triggered"] = False
            final_state["phase2_review_skipped_reason"] = "execution_not_ready"

    if lane_summary["planned_roles"]:
        final_state["planned_roles_from_lanes"] = list(lane_summary["planned_roles"])
    if lane_summary["execution_roles"]:
        final_state["execution_lane_roles"] = list(lane_summary["execution_roles"])
    if lane_summary["review_roles"]:
        final_state["review_lane_roles"] = list(lane_summary["review_roles"])
    final_state["parallel_workers"] = bool(lane_summary["parallel_workers"])
    final_state["parallel_reviews"] = bool(lane_summary["parallel_reviews"])
    if isinstance(metadata, dict) and metadata:
        dispatch_meta = final_state.get("dispatch_metadata") if isinstance(final_state.get("dispatch_metadata"), dict) else {}
        dispatch_meta.update(metadata)
        final_state["dispatch_metadata"] = dispatch_meta
    return final_state
