#!/usr/bin/env python3
"""Request/message state helpers extracted from the gateway monolith."""

from __future__ import annotations

import json
import os
from typing import Any, Callable, Dict, List, Optional, Tuple


def run_request_query(args: Any, request_id: str, *, run_command: Callable[..., Any]) -> Dict[str, Any]:
    cmd = [
        args.aoe_team_bin,
        "request",
        "--request-id",
        request_id,
        "--json",
    ]
    env = os.environ.copy()
    env["AOE_TEAM_DIR"] = str(args.team_dir)

    proc = run_command(cmd, env=env, timeout_sec=60)
    payload = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        raise RuntimeError(f"aoe-team request failed: {payload[:1200]}")

    try:
        data = json.loads(payload)
    except Exception as e:
        raise RuntimeError(f"aoe-team request returned non-JSON output: {payload[:800]}") from e

    if not isinstance(data, dict):
        raise RuntimeError("aoe-team request JSON is not an object")
    return data


def run_message_fail(
    args: Any,
    message_id: str,
    actor: str,
    note: str,
    *,
    run_command: Callable[..., Any],
) -> Tuple[bool, str]:
    cmd = [
        args.aoe_team_bin,
        "fail",
        message_id,
        "--force",
        "--note",
        note,
    ]
    if actor:
        cmd.extend(["--for", actor])

    env = os.environ.copy()
    env["AOE_TEAM_DIR"] = str(args.team_dir)

    proc = run_command(cmd, env=env, timeout_sec=60)
    payload = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        return False, payload
    return True, payload


def run_message_done(
    args: Any,
    message_id: str,
    actor: str,
    note: str,
    *,
    run_command: Callable[..., Any],
) -> Tuple[bool, str]:
    cmd = [
        args.aoe_team_bin,
        "done",
        message_id,
        "--note",
        note,
    ]
    if actor:
        cmd.extend(["--for", actor])

    env = os.environ.copy()
    env["AOE_TEAM_DIR"] = str(args.team_dir)

    proc = run_command(cmd, env=env, timeout_sec=60)
    payload = (proc.stdout or proc.stderr or "").strip()
    if proc.returncode != 0:
        return False, payload
    return True, payload


def finalize_request_reply_messages(
    args: Any,
    request_id: str,
    *,
    run_request_query: Callable[[Any, str], Dict[str, Any]],
    run_message_done: Callable[..., Tuple[bool, str]],
    actor: str = "Orchestrator",
    note: str = "gateway integrated reply into final response",
) -> Dict[str, Any]:
    state = run_request_query(args, request_id)
    replies = state.get("reply_messages") or []
    targets: List[Tuple[str, str, str]] = []
    skipped: List[str] = []

    for row in replies:
        if not isinstance(row, dict):
            continue
        message_id = str(row.get("id", "")).strip()
        status = str(row.get("status", "")).strip().lower()
        sender = str(row.get("from", "")).strip() or "?"
        if not message_id:
            skipped.append(f"{sender}(no_id)")
            continue
        if status in {"done", "failed"}:
            skipped.append(f"{sender}:{message_id}:{status}")
            continue
        targets.append((message_id, sender, status or "sent"))

    completed: List[str] = []
    failed: List[str] = []
    for message_id, sender, status in targets:
        ok, detail = run_message_done(args, message_id=message_id, actor=actor, note=note)
        label = f"{sender}:{message_id}:{status}"
        if ok:
            completed.append(label)
        else:
            failed.append(f"{label}:{detail[:120]}")

    return {
        "request_id": str(request_id or "").strip(),
        "targets": len(targets),
        "done": completed,
        "failed": failed,
        "skipped": skipped,
    }


def cancel_request_assignments(
    args: Any,
    request_data: Dict[str, Any],
    note: str,
    *,
    run_message_fail: Callable[..., Tuple[bool, str]],
) -> Dict[str, Any]:
    roles = request_data.get("roles") or []
    targets: List[Tuple[str, str, str]] = []
    skipped: List[str] = []

    for row in roles:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", "")).strip()
        status = str(row.get("status", "")).strip().lower()
        message_id = str(row.get("message_id", "")).strip()
        if not message_id:
            skipped.append(f"{role or '?'}(no_message_id)")
            continue
        if status in {"done", "failed", "error", "fail"}:
            skipped.append(f"{role or '?'}({status or 'terminal'})")
            continue
        targets.append((role, status, message_id))

    canceled: List[str] = []
    failed: List[str] = []
    for role, status, message_id in targets:
        ok, detail = run_message_fail(args, message_id=message_id, actor=role, note=note)
        label = f"{role or '?'}:{message_id}:{status or 'pending'}"
        if ok:
            canceled.append(label)
        else:
            failed.append(f"{label}:{detail[:120]}")

    return {
        "request_id": str(request_data.get("request_id", "")).strip(),
        "targets": len(targets),
        "canceled": canceled,
        "failed": failed,
        "skipped": skipped,
    }


def summarize_cancel_result(
    project_name: str,
    request_id: str,
    task: Optional[Dict[str, Any]],
    result: Dict[str, Any],
    *,
    task_display_label: Callable[..., str],
) -> str:
    label = task_display_label(task or {}, fallback_request_id=request_id)
    targets = int(result.get("targets", 0) or 0)
    canceled = result.get("canceled") or []
    failed = result.get("failed") or []
    skipped = result.get("skipped") or []
    lines = [
        f"orch: {project_name}",
        f"task: {label}",
        f"request_id: {request_id}",
        f"cancel: targets={targets} canceled={len(canceled)} failed={len(failed)} skipped={len(skipped)}",
    ]
    if canceled:
        lines.append("canceled_roles: " + ", ".join(canceled[:6]))
    if failed:
        lines.append("cancel_failures: " + ", ".join(failed[:4]))
    if skipped:
        lines.append("skipped: " + ", ".join(skipped[:6]))
    return "\n".join(lines)


def summarize_state(state: Dict[str, Any]) -> str:
    request_id = str(state.get("request_id", "-"))
    complete = bool(state.get("complete", False))
    timed_out = bool(state.get("timed_out", False))
    roles = state.get("role_states") or state.get("roles") or []
    replies = state.get("replies") or []

    lines: List[str] = []
    lines.append(f"request_id: {request_id}")
    lines.append(f"complete: {'yes' if complete else 'no'}")
    if "timed_out" in state:
        lines.append(f"timed_out: {'yes' if timed_out else 'no'}")
    if "elapsed_sec" in state:
        lines.append(f"elapsed_sec: {state.get('elapsed_sec')}")

    if roles:
        lines.append("")
        lines.append("roles")
        for row in roles:
            role = str(row.get("role", "?"))
            status = str(row.get("status", "?"))
            mid = str(row.get("message_id", ""))
            lines.append(f"- {role}: {status} {mid}")

    if replies:
        lines.append("")
        lines.append("latest replies")
        for row in replies[:6]:
            role = str(row.get("role", row.get("from", "?")))
            body = str(row.get("body", "")).replace("\n", " ").strip()
            if len(body) > 220:
                body = body[:217] + "..."
            if body:
                lines.append(f"- {role}: {body}")

    if not complete:
        lines.append("")
        lines.append(f"hint: /request {request_id}")

    return "\n".join(lines)


def render_run_response(
    state: Dict[str, Any],
    *,
    task: Optional[Dict[str, Any]] = None,
    report_level: str,
    default_report_level: str,
    task_display_label: Callable[..., str],
    summarize_state: Callable[[Dict[str, Any]], str],
) -> str:
    request_id = str(state.get("request_id", "-")).strip() or "-"
    row = task or {}
    label = task_display_label(row, fallback_request_id=request_id)
    task_ref = str(row.get("short_id") or row.get("alias") or row.get("request_id") or request_id).strip() or request_id
    complete = bool(state.get("complete", False))
    replies = state.get("replies") or []

    rendered: List[Tuple[str, str]] = []
    for item in replies:
        role = str(item.get("role", item.get("from", "assistant"))).strip() or "assistant"
        body = str(item.get("body", "")).strip()
        if body:
            rendered.append((role, body))

    level = str(report_level or default_report_level).strip().lower()
    if level not in {"short", "normal", "long"}:
        level = default_report_level

    if level == "short":
        if not complete:
            return f"접수: {label}\n다음: /check {task_ref} | /task {task_ref} | /monitor"
        status = str((row.get("status") if isinstance(row, dict) else "") or "completed").strip().lower() or "completed"
        if bool(state.get("timed_out", False)):
            status = "timed_out"
        return f"완료: {label}\n상태: {status}\n상세: /task {task_ref} (또는 /request {task_ref})"

    if complete and rendered:
        if level != "long" and len(rendered) == 1:
            return rendered[0][1]

        lines: List[str] = []
        if level == "long":
            lines.append(f"task: {label}")
            lines.append(f"request_id: {request_id}")
            lines.append("")
        for role, body in rendered[:6]:
            lines.append(f"[{role}]")
            lines.append(body)
            lines.append("")
        return "\n".join(lines).strip()

    if not complete:
        if level == "long":
            return f"task: {label}\n{summarize_state(state)}"
        return f"작업 접수됨: {label}\n진행: 진행 {label}\n상세: 상세 {label}"

    return f"작업 완료: {label}\n(에이전트 본문 응답이 아직 없습니다)"


def summarize_request_state(
    state: Dict[str, Any],
    *,
    task: Optional[Dict[str, Any]] = None,
    task_display_label: Callable[..., str],
) -> str:
    request_id = str(state.get("request_id", "-"))
    counts = state.get("counts") or {}
    roles = state.get("roles") or []
    unresolved = state.get("unresolved_roles") or []

    lines: List[str] = []
    lines.append(f"task: {task_display_label(task or {}, fallback_request_id=request_id)}")
    lines.append(f"request_id: {request_id}")
    lines.append(
        "counts: messages={m} assignments={a} replies={r}".format(
            m=counts.get("messages", 0),
            a=counts.get("assignments", 0),
            r=counts.get("replies", 0),
        )
    )
    lines.append(f"complete: {'yes' if state.get('complete') else 'no'}")

    if roles:
        lines.append("")
        lines.append("roles")
        for row in roles:
            lines.append(f"- {row.get('role')}: {row.get('status')} {row.get('message_id')}")

    if unresolved:
        lines.append("")
        lines.append("unresolved: " + ", ".join(str(x) for x in unresolved))

    return "\n".join(lines)
