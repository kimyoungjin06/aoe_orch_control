#!/usr/bin/env python3
"""Task store, alias, and lifecycle mutation helpers."""

from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple


def ensure_project_tasks(entry: Dict[str, Any]) -> Dict[str, Any]:
    tasks = entry.get("tasks")
    if not isinstance(tasks, dict):
        tasks = {}
        entry["tasks"] = tasks
    return tasks


def normalize_task_alias_key(raw: str) -> str:
    src = str(raw or "").strip().lower()
    out: List[str] = []
    sep = False
    for ch in src:
        if ch.isalnum():
            out.append(ch)
            sep = False
        else:
            if not sep:
                out.append("-")
                sep = True
    return "".join(out).strip("-")


def parse_task_seq_from_short_id(short_id: str) -> int:
    src = str(short_id or "").strip().upper()
    if not src.startswith("T-"):
        return 0
    tail = src[2:]
    return int(tail) if tail.isdigit() else 0


def format_task_short_id(seq: int) -> str:
    value = max(1, int(seq))
    return f"T-{value:03d}" if value < 1000 else f"T-{value}"


def derive_task_alias_base(prompt: str) -> str:
    src = str(prompt or "").strip()
    if not src:
        return "task"

    cleaned: List[str] = []
    for ch in src:
        if ch.isalnum() or ch in {" ", "-", "_"}:
            cleaned.append(ch)
        else:
            cleaned.append(" ")

    tokens = [t.lower() for t in "".join(cleaned).split() if t]
    if not tokens:
        return "task"

    stop = {
        "the",
        "a",
        "an",
        "to",
        "for",
        "and",
        "or",
        "of",
        "해주세요",
        "해줘",
        "요청",
        "작업",
        "진행",
        "지금",
        "바로",
        "좀",
    }
    picked = [t for t in tokens if t not in stop] or tokens

    alias = "-".join(picked[:5]).strip("-_")
    if len(alias) > 48:
        alias = alias[:48].rstrip("-_")
    return alias or "task"


def ensure_task_alias_meta(entry: Dict[str, Any]) -> Tuple[Dict[str, str], int]:
    raw_index = entry.get("task_alias_index")
    if not isinstance(raw_index, dict):
        raw_index = {}
        entry["task_alias_index"] = raw_index

    alias_index: Dict[str, str] = {}
    for key, rid in raw_index.items():
        key_norm = normalize_task_alias_key(str(key or ""))
        rid_norm = str(rid or "").strip()
        if key_norm and rid_norm:
            alias_index[key_norm] = rid_norm
    entry["task_alias_index"] = alias_index

    raw_seq = entry.get("task_seq")
    try:
        seq = max(0, int(raw_seq or 0))
    except Exception:
        seq = 0
    entry["task_seq"] = seq
    return alias_index, seq


def rebuild_task_alias_index(entry: Dict[str, Any]) -> None:
    tasks = ensure_project_tasks(entry)
    _, seq = ensure_task_alias_meta(entry)

    alias_index: Dict[str, str] = {}
    max_seq = max(0, int(seq))

    for req_id, task in tasks.items():
        rid = str(req_id or "").strip()
        if not rid or not isinstance(task, dict):
            continue

        short_id = str(task.get("short_id", "")).strip().upper()
        alias = str(task.get("alias", "")).strip()

        if short_id:
            alias_index[normalize_task_alias_key(short_id)] = rid
            max_seq = max(max_seq, parse_task_seq_from_short_id(short_id))
        if alias:
            alias_index[normalize_task_alias_key(alias)] = rid

    entry["task_alias_index"] = alias_index
    entry["task_seq"] = max_seq


def assign_task_alias(
    entry: Dict[str, Any],
    task: Dict[str, Any],
    prompt: str,
    *,
    rebuild_index: bool = True,
) -> None:
    alias_index, seq = ensure_task_alias_meta(entry)

    req_id = str(task.get("request_id", "")).strip()
    if not req_id:
        return

    short_id = str(task.get("short_id", "")).strip().upper()
    if not short_id:
        next_seq = max(seq, 0)
        while True:
            next_seq += 1
            candidate = format_task_short_id(next_seq)
            key = normalize_task_alias_key(candidate)
            owner = alias_index.get(key)
            if not owner or owner == req_id:
                short_id = candidate
                task["short_id"] = short_id
                entry["task_seq"] = next_seq
                break

    alias = str(task.get("alias", "")).strip()
    if not alias:
        base = derive_task_alias_base(prompt or str(task.get("prompt", "")).strip() or short_id.lower())
        candidate = base
        suffix = 2
        while True:
            key = normalize_task_alias_key(candidate)
            owner = alias_index.get(key)
            if not owner or owner == req_id:
                alias = candidate
                task["alias"] = alias
                break
            candidate = f"{base}-{suffix}"
            suffix += 1

    if rebuild_index:
        rebuild_task_alias_index(entry)


def backfill_task_aliases(entry: Dict[str, Any]) -> None:
    tasks = ensure_project_tasks(entry)
    if not tasks:
        ensure_task_alias_meta(entry)
        return

    rows = sorted(tasks.items(), key=lambda kv: str((kv[1] or {}).get("created_at", "")))
    for req_id, task in rows:
        if not isinstance(task, dict):
            continue
        rid = str(req_id or "").strip()
        if not rid:
            continue
        if not str(task.get("request_id", "")).strip():
            task["request_id"] = rid
        assign_task_alias(entry, task, prompt=str(task.get("prompt", "")), rebuild_index=False)

    rebuild_task_alias_index(entry)


def resolve_task_request_id(entry: Dict[str, Any], request_or_alias: str) -> str:
    token = str(request_or_alias or "").strip()
    if not token:
        return ""

    tasks = ensure_project_tasks(entry)
    if token in tasks:
        return token

    alias_index, _ = ensure_task_alias_meta(entry)
    if not alias_index and tasks:
        backfill_task_aliases(entry)
        alias_index, _ = ensure_task_alias_meta(entry)

    norm = normalize_task_alias_key(token)
    mapped = alias_index.get(norm, "")
    if mapped and mapped in tasks:
        return mapped

    for rid, task in tasks.items():
        if not isinstance(task, dict):
            continue
        short_id = str(task.get("short_id", "")).strip().upper()
        alias = str(task.get("alias", "")).strip()
        if token.upper() == short_id:
            return rid
        if norm and norm == normalize_task_alias_key(alias):
            return rid

    return token


def latest_task_request_refs(entry: Dict[str, Any], limit: int = 12) -> List[str]:
    tasks = ensure_project_tasks(entry)
    if not tasks:
        return []
    backfill_task_aliases(entry)
    rows = sorted(tasks.items(), key=lambda kv: str((kv[1] or {}).get("updated_at", "")), reverse=True)
    cap = max(1, min(50, int(limit)))
    out: List[str] = []
    for req_id, task in rows[:cap]:
        if isinstance(task, dict):
            rid = str(req_id or "").strip()
            if rid:
                out.append(rid)
    return out


def trim_project_tasks(tasks: Dict[str, Any], keep: int) -> None:
    if len(tasks) <= int(keep):
        return
    ordered = sorted(tasks.items(), key=lambda kv: str((kv[1] or {}).get("updated_at", "")), reverse=True)
    keep_keys = {key for key, _ in ordered[: max(1, int(keep))]}
    for key in list(tasks.keys()):
        if key not in keep_keys:
            tasks.pop(key, None)


def get_task_record(entry: Dict[str, Any], request_id: str) -> Optional[Dict[str, Any]]:
    token = resolve_task_request_id(entry, request_id)
    if not token:
        return None
    tasks = ensure_project_tasks(entry)
    item = tasks.get(token)
    return item if isinstance(item, dict) else None


def ensure_task_record(
    entry: Dict[str, Any],
    *,
    request_id: str,
    prompt: str,
    mode: str,
    roles: List[str],
    verifier_roles: List[str],
    require_verifier: bool,
    now_iso: Callable[[], str],
    dedupe_roles: Callable[[Iterable[str]], List[str]],
    build_task_context: Callable[..., Dict[str, str]],
    lifecycle_stages: Iterable[str],
    keep_limit: int,
) -> Dict[str, Any]:
    token = str(request_id or "").strip()
    tasks = ensure_project_tasks(entry)
    now = now_iso()

    item = tasks.get(token)
    if not isinstance(item, dict):
        item = {
            "request_id": token,
            "mode": mode,
            "prompt": prompt.strip(),
            "roles": dedupe_roles(roles),
            "verifier_roles": dedupe_roles(verifier_roles),
            "require_verifier": bool(require_verifier),
            "status": "running",
            "stage": "intake",
            "stages": {name: "pending" for name in lifecycle_stages},
            "history": [],
            "created_at": now,
            "updated_at": now,
            "result": {},
        }
        tasks[token] = item
    else:
        if prompt:
            item["prompt"] = prompt.strip()
        if mode:
            item["mode"] = mode
        if roles:
            item["roles"] = dedupe_roles(roles)
        if verifier_roles:
            item["verifier_roles"] = dedupe_roles(verifier_roles)
        item["require_verifier"] = bool(require_verifier)
        item["updated_at"] = now

    assign_task_alias(entry, item, prompt=prompt, rebuild_index=False)
    item["context"] = build_task_context(request_id=token, entry=entry, task=item)
    trim_project_tasks(tasks, keep=keep_limit)
    rebuild_task_alias_index(entry)
    return item


def lifecycle_set_stage(
    task: Dict[str, Any],
    *,
    stage: str,
    status: str,
    note: str = "",
    lifecycle_stages: Iterable[str],
    normalize_stage_status: Callable[[Any], str],
    now_iso: Callable[[], str],
    history_limit: int,
) -> None:
    stage_names = tuple(lifecycle_stages)
    if stage not in stage_names:
        return

    stages = task.get("stages")
    if not isinstance(stages, dict):
        stages = {name: "pending" for name in stage_names}
        task["stages"] = stages

    prev = str(stages.get(stage, "pending"))
    next_status = normalize_stage_status(status or "pending")
    if prev == next_status and not note:
        return

    stages[stage] = next_status
    task["stage"] = stage

    history = task.get("history")
    if not isinstance(history, list):
        history = []

    event: Dict[str, Any] = {"at": now_iso(), "stage": stage, "status": next_status}
    if note:
        event["note"] = note
    history.append(event)
    if len(history) > int(history_limit):
        history = history[-int(history_limit) :]

    task["history"] = history
    task["updated_at"] = event["at"]


def summarize_task_monitor(
    project_name: str,
    entry: Dict[str, Any],
    *,
    limit: int,
    normalize_task_status: Callable[[Any], str],
    dedupe_roles: Callable[[Iterable[str]], List[str]],
    task_display_label: Callable[[Dict[str, Any], str], str],
    lifecycle_stages: Iterable[str],
) -> str:
    tasks = ensure_project_tasks(entry)
    if not tasks:
        return f"orch: {project_name}\n작업이 없습니다."

    backfill_task_aliases(entry)
    rows = sorted(tasks.items(), key=lambda kv: str((kv[1] or {}).get("updated_at", "")), reverse=True)
    cap = max(1, min(50, int(limit)))
    stage_names = tuple(lifecycle_stages)

    counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0}
    invalid_stage_rows = 0
    for _, task in rows:
        if not isinstance(task, dict):
            continue
        status = normalize_task_status(task.get("status", "pending"))
        counts[status] = counts.get(status, 0) + 1
        stage = str(task.get("stage", "")).strip().lower()
        if stage and stage not in stage_names:
            invalid_stage_rows += 1

    lines = [
        f"orch: {project_name}",
        f"task monitor: latest {cap}",
        "format: label | status/stage | roles | updated",
        "summary: total={total} running={running} completed={completed} failed={failed} pending={pending}".format(
            total=len(rows),
            running=counts.get("running", 0),
            completed=counts.get("completed", 0),
            failed=counts.get("failed", 0),
            pending=counts.get("pending", 0),
        ),
    ]
    if invalid_stage_rows:
        lines.append(f"warning: invalid lifecycle stage rows={invalid_stage_rows}")

    for idx, (req_id, task) in enumerate(rows[:cap], start=1):
        if not isinstance(task, dict):
            continue
        label = task_display_label(task, str(req_id or "").strip())
        status = normalize_task_status(task.get("status", "pending"))
        stage = str(task.get("stage", "pending")).strip().lower() or "pending"
        if stage not in stage_names:
            stage = "pending"
        roles = dedupe_roles(task.get("roles") or [])
        role_text = ", ".join(roles[:2])
        if len(roles) > 2:
            role_text += f" +{len(roles) - 2}"
        updated = str(task.get("updated_at", "")).strip() or "-"
        lines.append(f"- {idx}. {label} | {status}/{stage} | {role_text or '-'} | {updated}")

    lines.append("")
    lines.append("alias map (number/label -> request_id):")
    for idx, (req_id, task) in enumerate(rows[:cap], start=1):
        if not isinstance(task, dict):
            continue
        lines.append(f"- {idx}. {task_display_label(task, str(req_id or '').strip())} -> {req_id}")
    lines.append("")
    lines.append("quick actions: /check <번호|label> /task <번호|label> /retry <번호|label> /replan <번호|label> /cancel <번호|label>")
    return "\n".join(lines)


def normalize_role_rows(data: Dict[str, Any], *, dedupe_roles: Callable[[Iterable[str]], List[str]]) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    role_states = data.get("role_states")
    if isinstance(role_states, list):
        for item in role_states:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            if not role:
                continue
            status = str(item.get("status", "pending")).strip().lower() or "pending"
            rows.append({"role": role, "status": status})

    if rows:
        return rows

    roles_obj = data.get("roles")
    if isinstance(roles_obj, list) and roles_obj and isinstance(roles_obj[0], dict):
        for item in roles_obj:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            if not role:
                continue
            status = str(item.get("status", "pending")).strip().lower() or "pending"
            rows.append({"role": role, "status": status})
        if rows:
            return rows

    done_set = {str(x).strip() for x in (data.get("done_roles") or []) if str(x).strip()}
    failed_set = {str(x).strip() for x in (data.get("failed_roles") or []) if str(x).strip()}
    pending_set = {
        str(x).strip()
        for x in (data.get("pending_roles") or data.get("unresolved_roles") or [])
        if str(x).strip()
    }

    if isinstance(roles_obj, list):
        for item in roles_obj:
            role = str(item).strip()
            if not role:
                continue
            if role in failed_set:
                status = "failed"
            elif role in done_set:
                status = "done"
            elif role in pending_set:
                status = "pending"
            else:
                status = "pending"
            rows.append({"role": role, "status": status})
        if rows:
            return rows

    all_roles = dedupe_roles(list(done_set) + list(failed_set) + list(pending_set))
    for role in all_roles:
        if role in failed_set:
            status = "failed"
        elif role in done_set:
            status = "done"
        else:
            status = "pending"
        rows.append({"role": role, "status": status})
    return rows


def extract_request_snapshot(data: Dict[str, Any], *, dedupe_roles: Callable[[Iterable[str]], List[str]]) -> Dict[str, Any]:
    rows = normalize_role_rows(data, dedupe_roles=dedupe_roles)
    counts = data.get("counts") or {}

    assignments = int(counts.get("assignments", 0) or 0)
    replies = int(counts.get("replies", 0) or 0)
    if assignments <= 0:
        assignments = len(rows)
    if replies <= 0:
        replies = len(data.get("replies") or [])

    done_roles: Set[str] = set()
    failed_roles: Set[str] = set()
    pending_roles: Set[str] = set()

    for row in rows:
        role = str(row.get("role", "")).strip()
        status = str(row.get("status", "pending")).strip().lower()
        if not role:
            continue
        if status in {"failed", "error", "fail"}:
            failed_roles.add(role)
        elif status == "done":
            done_roles.add(role)
        else:
            pending_roles.add(role)

    for role in data.get("done_roles") or []:
        token = str(role).strip()
        if token:
            done_roles.add(token)
            pending_roles.discard(token)
            failed_roles.discard(token)

    for role in data.get("failed_roles") or []:
        token = str(role).strip()
        if token:
            failed_roles.add(token)
            done_roles.discard(token)
            pending_roles.discard(token)

    for role in data.get("pending_roles") or data.get("unresolved_roles") or []:
        token = str(role).strip()
        if token and token not in done_roles and token not in failed_roles:
            pending_roles.add(token)

    request_id = str(data.get("request_id", "")).strip()
    complete = bool(data.get("complete", False))
    return {
        "request_id": request_id,
        "rows": rows,
        "assignments": assignments,
        "replies": replies,
        "complete": complete,
        "done_roles": sorted(done_roles),
        "failed_roles": sorted(failed_roles),
        "pending_roles": sorted(pending_roles),
    }


def sync_task_lifecycle(
    entry: Dict[str, Any],
    request_data: Dict[str, Any],
    *,
    prompt: str,
    mode: str,
    selected_roles: Optional[List[str]],
    verifier_roles: Optional[List[str]],
    require_verifier: bool,
    verifier_candidates: List[str],
    dedupe_roles: Callable[[Iterable[str]], List[str]],
    ensure_task_record: Callable[..., Dict[str, Any]],
    lifecycle_set_stage: Callable[..., None],
    normalize_task_status: Callable[[Any], str],
    sync_task_exec_context: Callable[[Dict[str, Any], Dict[str, Any]], Dict[str, str]],
) -> Optional[Dict[str, Any]]:
    snap = extract_request_snapshot(request_data, dedupe_roles=dedupe_roles)
    request_id = str(snap.get("request_id", "")).strip()
    if not request_id:
        return None

    rows = snap.get("rows") or []
    inferred_roles = [str(x.get("role", "")).strip() for x in rows if str(x.get("role", "")).strip()]
    roles = dedupe_roles(selected_roles or inferred_roles)

    verifier_keys = {str(c or "").strip().lower() for c in verifier_candidates if str(c or "").strip()}
    inferred_verifiers = [r for r in roles if r.lower() in verifier_keys]
    verifiers = dedupe_roles(verifier_roles or inferred_verifiers)

    task = ensure_task_record(
        entry=entry,
        request_id=request_id,
        prompt=prompt,
        mode=mode,
        roles=roles,
        verifier_roles=verifiers,
        require_verifier=require_verifier,
    )

    assignments = int(snap.get("assignments", 0) or 0)
    replies = int(snap.get("replies", 0) or 0)
    complete = bool(snap.get("complete", False))
    done_roles = set(str(x) for x in (snap.get("done_roles") or []))
    failed_roles = set(str(x) for x in (snap.get("failed_roles") or []))
    pending_roles = set(str(x) for x in (snap.get("pending_roles") or []))

    lifecycle_set_stage(task, "intake", "done")
    lifecycle_set_stage(task, "planning", "done")

    staffing_status = "done" if assignments > 0 else ("running" if roles else "pending")
    lifecycle_set_stage(task, "staffing", staffing_status)

    if failed_roles:
        execution_status = "failed"
    elif complete and assignments > 0 and not pending_roles:
        execution_status = "done"
    elif assignments > 0:
        execution_status = "running"
    else:
        execution_status = "pending"
    lifecycle_set_stage(task, "execution", execution_status)

    ver_note = ""
    if require_verifier:
        if not verifiers:
            verification_status = "failed"
            ver_note = "no verifier role assigned"
        elif any(v in failed_roles for v in verifiers):
            verification_status = "failed"
            ver_note = "verifier role failed"
        elif all(v in done_roles for v in verifiers):
            verification_status = "done"
        elif complete and execution_status == "done":
            verification_status = "failed"
            ver_note = "verifier gate not satisfied"
        elif execution_status in {"running", "done"}:
            verification_status = "running"
        elif execution_status == "failed":
            verification_status = "failed"
        else:
            verification_status = "pending"
    else:
        if execution_status == "done":
            verification_status = "done"
        elif execution_status == "failed":
            verification_status = "failed"
        elif execution_status == "running":
            verification_status = "running"
        else:
            verification_status = "pending"

    lifecycle_set_stage(task, "verification", verification_status, note=ver_note)

    if execution_status == "failed" or verification_status == "failed":
        integration_status = "failed"
    elif verification_status == "done" and (replies > 0 or complete):
        integration_status = "done"
    elif execution_status == "running" or verification_status == "running":
        integration_status = "running"
    else:
        integration_status = "pending"
    lifecycle_set_stage(task, "integration", integration_status)

    if integration_status == "failed":
        close_status = "failed"
    elif integration_status == "done" and complete:
        close_status = "done"
    elif execution_status == "running" or verification_status == "running":
        close_status = "running"
    else:
        close_status = "pending"
    lifecycle_set_stage(task, "close", close_status)

    if close_status == "failed" or verification_status == "failed" or execution_status == "failed":
        overall = "failed"
    elif close_status == "done":
        overall = "completed"
    elif close_status == "running" or execution_status == "running" or verification_status == "running":
        overall = "running"
    else:
        overall = "pending"

    task["status"] = normalize_task_status(overall)
    task["roles"] = roles
    task["verifier_roles"] = verifiers
    task["require_verifier"] = bool(require_verifier)
    task["result"] = {
        "assignments": assignments,
        "replies": replies,
        "complete": complete,
        "done_roles": sorted(done_roles),
        "failed_roles": sorted(failed_roles),
        "pending_roles": sorted(pending_roles),
    }
    sync_task_exec_context(entry, task)
    return task
