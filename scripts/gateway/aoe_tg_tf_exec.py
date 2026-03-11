#!/usr/bin/env python3
"""TF execution/worktree/session helpers extracted from the gateway monolith."""

from __future__ import annotations

import argparse
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
    tmp = path.with_suffix(path.suffix + ".tmp")
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
) -> List[Dict[str, str]]:
    runner = tf_worker_runner_path()
    handler = worker_handler_script().resolve()
    env_file = (Path(str(args.team_dir)) / "telegram.env").resolve()
    run_dir = (Path(str(args.team_dir)) / "tf_runs" / request_id).resolve()
    workers_dir = (run_dir / "workers").resolve()
    logs_dir = (run_dir / "logs").resolve()
    workers_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    specs: List[Dict[str, str]] = []
    default_prefix = str(getattr(args, "_aoe_default_tf_worker_session_prefix", "") or "tfw_")
    for role in dedupe_roles(roles):
        role_key = sanitize_fs_token(role.lower(), "worker")
        session = tf_worker_session_name(request_id, role, default_prefix=default_prefix)
        state_file = (workers_dir / f"{role_key}.state.json").resolve()
        log_file = (logs_dir / f"worker_{role_key}.console.log").resolve()
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
            }
        )
    return specs


def preview_tf_worker_sessions(args: argparse.Namespace, request_id: str, roles: List[str], startup_timeout_sec: int) -> Dict[str, Any]:
    specs = tf_worker_specs(args, request_id, roles, startup_timeout_sec)
    return {
        "tmux_available": shutil.which("tmux") is not None,
        "sessions": [
            {
                "role": str(spec.get("role", "")).strip(),
                "session": str(spec.get("session", "")).strip(),
                "log_file": str(spec.get("log_file", "")).strip(),
            }
            for spec in specs
        ],
    }


def spawn_tf_worker_sessions(args: argparse.Namespace, request_id: str, roles: List[str], startup_timeout_sec: int, *, run_command) -> Dict[str, Any]:
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

    for spec in tf_worker_specs(args, request_id, roles, startup_timeout_sec):
        role = str(spec.get("role", "")).strip()
        session = str(spec.get("session", "")).strip()
        log_file = str(spec.get("log_file", "")).strip()
        result["sessions"].append({"role": role, "session": session, "log_file": log_file})
        if not session:
            result["failed"].append({"role": role, "error": "missing session"})
            continue
        if run_command(["tmux", "has-session", "-t", session], env=None, timeout_sec=10).returncode == 0:
            result["existing"].append({"role": role, "session": session, "log_file": log_file})
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
        result["spawned"].append({"role": role, "session": session, "log_file": log_file})
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
    context = build_task_context(request_id=request_id, entry=entry, task=task, tf_meta=tf_meta)
    if context:
        task["context"] = context

    if team_dir is None or not request_id:
        return context

    tf_map = load_tf_exec_map(team_dir, default_tf_exec_map_file)
    row = tf_map.get(request_id) if isinstance(tf_map, dict) else None
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
) -> Dict[str, Any]:
    effective_roles = args.roles if roles_override is None else (roles_override or "")
    effective_priority = (priority_override or args.priority or "P2").upper().strip()
    if effective_priority not in {"P1", "P2", "P3"}:
        effective_priority = "P2"
    effective_timeout = max(1, int(args.orch_timeout_sec if timeout_override is None else timeout_override))
    effective_no_wait = bool(args.no_wait if no_wait_override is None else no_wait_override)

    request_id = create_request_id()
    tf_meta = ensure_tf_exec_workspace(
        args,
        request_id,
        default_tf_exec_mode=default_tf_exec_mode,
        default_tf_work_root_name=default_tf_work_root_name,
        default_tf_exec_map_file=default_tf_exec_map_file,
        now_iso=now_iso,
        run_command=run_command,
    )
    try:
        worker_roles = resolve_dispatch_roles_from_preview(
            args,
            prompt,
            request_id=request_id,
            roles_override=effective_roles,
            priority=effective_priority,
            timeout_sec=effective_timeout,
            run_command=run_command,
        )
        if not worker_roles:
            raise RuntimeError("aoe-orch run preview resolved no worker roles")

        worker_sessions = spawn_tf_worker_sessions(
            args,
            request_id=request_id,
            roles=worker_roles,
            startup_timeout_sec=(effective_timeout + default_tf_worker_startup_grace_sec),
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

        tf_meta["target_roles"] = dedupe_roles(worker_roles)
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
        try:
            cleanup_tf_exec_entry(tf_meta, run_command=run_command)
            tf_map = load_tf_exec_map(args.team_dir, default_tf_exec_map_file)
            if request_id in tf_map:
                del tf_map[request_id]
                save_tf_exec_map(args.team_dir, tf_map, default_tf_exec_map_file)
        except Exception:
            pass
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
    if effective_roles:
        cmd.extend(["--roles", effective_roles])
    cmd.append("--no-spawn-missing")
    if effective_no_wait:
        cmd.append("--no-wait")
    cmd.append(prompt)

    proc = run_command(cmd, env=None, timeout_sec=args.orch_command_timeout_sec)
    if proc.returncode != 0:
        try:
            cleanup_tf_worker_sessions(tf_meta, run_command=run_command)
            cleanup_tf_exec_entry(tf_meta, run_command=run_command)
            tf_map = load_tf_exec_map(args.team_dir, default_tf_exec_map_file)
            if request_id in tf_map:
                del tf_map[request_id]
                save_tf_exec_map(args.team_dir, tf_map, default_tf_exec_map_file)
        except Exception:
            pass
        detail = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"aoe-orch run failed: {detail[:1000]}")

    payload = (proc.stdout or "").strip()
    try:
        data = json.loads(payload)
    except Exception as e:
        try:
            cleanup_tf_worker_sessions(tf_meta, run_command=run_command)
            cleanup_tf_exec_entry(tf_meta, run_command=run_command)
            tf_map = load_tf_exec_map(args.team_dir, default_tf_exec_map_file)
            if request_id in tf_map:
                del tf_map[request_id]
                save_tf_exec_map(args.team_dir, tf_map, default_tf_exec_map_file)
        except Exception:
            pass
        raise RuntimeError(f"aoe-orch run returned non-JSON output: {payload[:800]}") from e

    if not isinstance(data, dict):
        try:
            cleanup_tf_worker_sessions(tf_meta, run_command=run_command)
            cleanup_tf_exec_entry(tf_meta, run_command=run_command)
            tf_map = load_tf_exec_map(args.team_dir, default_tf_exec_map_file)
            if request_id in tf_map:
                del tf_map[request_id]
                save_tf_exec_map(args.team_dir, tf_map, default_tf_exec_map_file)
        except Exception:
            pass
        raise RuntimeError("aoe-orch run JSON is not an object")
    if str(data.get("request_id", "")).strip() and str(data.get("request_id", "")).strip() != request_id:
        data["gateway_request_id"] = request_id
    try:
        finalize_tf_exec_meta(args.team_dir, request_id, data, default_tf_exec_map_file=default_tf_exec_map_file, now_iso=now_iso)
    except Exception:
        pass
    data["tf_workers"] = worker_sessions
    data["planned_roles"] = dedupe_roles(worker_roles)
    if not effective_no_wait:
        cleanup_tf_worker_sessions(tf_meta, run_command=run_command)
    return data
