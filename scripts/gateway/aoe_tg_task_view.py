#!/usr/bin/env python3
"""Task view helpers extracted from the gateway monolith."""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, Iterable, List, Optional


DEFAULT_PROJECT_ALIAS_MAX = 999
LIFECYCLE_STAGES = (
    "intake",
    "planning",
    "staffing",
    "execution",
    "verification",
    "integration",
    "close",
)


def normalize_project_name(name: str) -> str:
    src = (name or "").strip().lower()
    out = []
    for ch in src:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out.append(ch)
        else:
            out.append("_")
    token = "".join(out).strip("._-")
    return token or "default"


def normalize_project_alias(token: str, max_alias: int = DEFAULT_PROJECT_ALIAS_MAX) -> str:
    raw = str(token or "").strip().upper()
    if not raw:
        return ""
    body = raw[1:] if raw.startswith("O") else raw
    if not body.isdigit():
        return ""
    idx = int(body)
    if idx < 1 or idx > int(max_alias):
        return ""
    return f"O{idx}"


def dedupe_roles(roles: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in roles:
        token = str(item or "").strip()
        if not token:
            continue
        key = token.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(token)
    return out


def critic_has_blockers(critic: Dict[str, Any]) -> bool:
    approved = bool(critic.get("approved", True))
    issues = critic.get("issues") or []
    return (not approved) or bool(issues)


def task_display_label(task: Dict[str, Any], fallback_request_id: str = "") -> str:
    short_id = str(task.get("short_id", "")).strip().upper()
    alias = str(task.get("alias", "")).strip()
    if short_id and alias:
        return f"{short_id} | {alias}"
    if alias:
        return alias
    if short_id:
        return short_id
    rid = str(task.get("request_id", "")).strip() or str(fallback_request_id or "").strip()
    return rid if rid else "-"


def task_short_to_tf_id(short_id: str) -> str:
    short = str(short_id or "").strip().upper()
    if not short:
        return ""
    tf_id = re.sub(r"^T-", "TF-", short)
    if tf_id.startswith("TF-"):
        return tf_id
    token = re.sub(r"[^A-Z0-9._-]+", "_", short).strip("._-")
    return f"TF-{token[:24] or 'UNK'}"


def request_to_tf_id(request_id: str) -> str:
    token = re.sub(r"[^A-Z0-9._-]+", "_", str(request_id or "").strip().upper()).strip("._-")
    return f"TF-REQ-{(token[:24] or 'UNK')}"


def build_task_context(
    *,
    request_id: str = "",
    entry: Optional[Dict[str, Any]] = None,
    task: Optional[Dict[str, Any]] = None,
    tf_meta: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    context: Dict[str, str] = {}

    def put(key: str, value: Any, transform: Optional[Callable[[str], str]] = None) -> None:
        token = str(value or "").strip()
        if not token:
            return
        if transform is not None:
            token = transform(token)
        if token:
            context[key] = token

    for source in (extra, task.get("context") if isinstance(task, dict) else None):
        if not isinstance(source, dict):
            continue
        put("project_key", source.get("project_key"), normalize_project_name)
        put("project_alias", source.get("project_alias"), normalize_project_alias)
        put("project_root", source.get("project_root"))
        put("team_dir", source.get("team_dir"))
        put("tf_id", source.get("tf_id"))
        put("task_short_id", source.get("task_short_id"), lambda s: s.upper())
        put("task_alias", source.get("task_alias"))
        put("workdir", source.get("workdir"))
        put("run_dir", source.get("run_dir"))
        put("branch", source.get("branch"))
        put("exec_mode", source.get("exec_mode"))
        put("source_request_id", source.get("source_request_id"))
        put("control_mode", source.get("control_mode"))
        put("gateway_request_id", source.get("gateway_request_id"))

    if isinstance(tf_meta, dict):
        put("project_key", tf_meta.get("project_key"), normalize_project_name)
        put("project_alias", tf_meta.get("project_alias"), normalize_project_alias)
        put("project_root", tf_meta.get("project_root"))
        put("team_dir", tf_meta.get("team_dir"))
        put("tf_id", tf_meta.get("tf_id"))
        put("task_short_id", tf_meta.get("task_short_id"), lambda s: s.upper())
        put("task_alias", tf_meta.get("task_alias"))
        put("workdir", tf_meta.get("workdir"))
        put("run_dir", tf_meta.get("run_dir"))
        put("branch", tf_meta.get("branch"))
        put("exec_mode", tf_meta.get("mode"))
        put("source_request_id", tf_meta.get("source_request_id"))
        put("control_mode", tf_meta.get("control_mode"))
        put("gateway_request_id", tf_meta.get("gateway_request_id") or tf_meta.get("request_id"))

    if isinstance(entry, dict):
        put("project_key", entry.get("name"), normalize_project_name)
        put("project_alias", entry.get("project_alias"), normalize_project_alias)
        put("project_root", entry.get("project_root"))
        put("team_dir", entry.get("team_dir"))

    if isinstance(task, dict):
        put("task_short_id", task.get("short_id"), lambda s: s.upper())
        put("task_alias", task.get("alias"))
        put("source_request_id", task.get("source_request_id"))
        put("control_mode", task.get("control_mode"))

    if context.get("task_short_id"):
        context["tf_id"] = task_short_to_tf_id(context["task_short_id"])
    elif not context.get("tf_id"):
        context["tf_id"] = request_to_tf_id(request_id)

    if request_id and not context.get("gateway_request_id"):
        context["gateway_request_id"] = str(request_id).strip()

    return context


def summarize_task_lifecycle(project_name: str, task: Dict[str, Any]) -> str:
    request_id = str(task.get("request_id", "-")).strip() or "-"
    label = task_display_label(task, fallback_request_id=request_id)
    status = str(task.get("status", "pending"))
    mode = str(task.get("mode", "dispatch"))
    roles = dedupe_roles(task.get("roles") or [])
    verifiers = dedupe_roles(task.get("verifier_roles") or [])
    stages = task.get("stages") or {}

    lines = [
        f"orch: {project_name}",
        f"task: {label}",
        f"request_id: {request_id}",
        f"status: {status}",
        f"mode: {mode}",
        f"roles: {', '.join(roles) if roles else '-'}",
        f"verifier_roles: {', '.join(verifiers) if verifiers else '-'}",
    ]

    context = build_task_context(request_id=request_id, task=task)
    if context:
        lines.append(
            "context: {proj} ({key}) / {tf}".format(
                proj=context.get("project_alias", "-"),
                key=context.get("project_key", "-"),
                tf=context.get("tf_id", "-"),
            )
        )
        if context.get("task_alias"):
            lines.append(f"context_alias: {context.get('task_alias')}")
        if context.get("workdir"):
            lines.append(f"context_workdir: {context.get('workdir')}")
        if context.get("run_dir"):
            lines.append(f"context_run_dir: {context.get('run_dir')}")
        if context.get("source_request_id"):
            lines.append(
                "context_lineage: {mode} <- {source}".format(
                    mode=context.get("control_mode", "dispatch") or "dispatch",
                    source=context.get("source_request_id"),
                )
            )

    lines.append("lifecycle:")
    for name in LIFECYCLE_STAGES:
        lines.append(f"- {name}: {str(stages.get(name, 'pending'))}")

    plan = task.get("plan")
    if isinstance(plan, dict):
        subtasks = plan.get("subtasks") or []
        plan_summary = str(plan.get("summary", "")).strip()
        if plan_summary:
            lines.append("plan_summary: " + plan_summary)
        lines.append(f"plan_subtasks: {len(subtasks)}")

        owner_counts: Dict[str, int] = {}
        for row in subtasks:
            if not isinstance(row, dict):
                continue
            role = str(row.get("owner_role", "")).strip() or "Worker"
            owner_counts[role] = owner_counts.get(role, 0) + 1
        if owner_counts:
            lines.append("plan_owner_load: " + ", ".join(f"{role}={cnt}" for role, cnt in owner_counts.items()))

        for row in subtasks[:6]:
            if not isinstance(row, dict):
                continue
            sid = str(row.get("id", "")).strip() or "S"
            role = str(row.get("owner_role", "")).strip() or "Worker"
            title = str(row.get("title", "")).strip() or str(row.get("goal", "")).strip() or "subtask"
            lines.append(f"- plan {sid} [{role}] {title}")

    critic = task.get("plan_critic")
    if isinstance(critic, dict):
        issues = critic.get("issues") or []
        recs = critic.get("recommendations") or []
        approved = not critic_has_blockers(critic)
        lines.append(f"plan_critic: {'approved' if approved else 'needs_fix'}")
        for item in issues[:4]:
            token = str(item or "").strip()
            if token:
                lines.append("- issue: " + token)
        for item in recs[:4]:
            token = str(item or "").strip()
            if token:
                lines.append("- recommendation: " + token)

    gate = task.get("plan_gate_passed")
    if isinstance(gate, bool):
        lines.append(f"plan_gate: {'passed' if gate else 'blocked'}")
        if gate is False:
            gate_reason = str(task.get("plan_gate_reason", "")).strip()
            if gate_reason:
                lines.append("plan_gate_reason: " + gate_reason[:240])

    replans = task.get("plan_replans")
    if isinstance(replans, list) and replans:
        lines.append(f"plan_replans: {len(replans)}")
        for row in replans[-3:]:
            if not isinstance(row, dict):
                continue
            attempt = int(row.get("attempt", 0) or 0)
            verdict = str(row.get("critic", "")).strip() or "unknown"
            subtasks = int(row.get("subtasks", 0) or 0)
            lines.append(f"- replan#{attempt}: critic={verdict} subtasks={subtasks}")

    exec_critic = task.get("exec_critic")
    if isinstance(exec_critic, dict):
        verdict = str(exec_critic.get("verdict", "")).strip() or "unknown"
        action = str(exec_critic.get("action", "")).strip() or "-"
        reason = str(exec_critic.get("reason", "")).strip()
        attempt = int(exec_critic.get("attempt", 0) or 0)
        max_attempts = int(exec_critic.get("max_attempts", 0) or 0)
        at = str(exec_critic.get("at", "")).strip()
        lines.append(f"exec_critic: {verdict} (action={action})")
        if attempt and max_attempts:
            lines.append(f"exec_attempts: {attempt}/{max_attempts}")
        if reason:
            lines.append("exec_reason: " + reason[:240])
        if at:
            lines.append("exec_critic_at: " + at)

    result = task.get("result")
    if isinstance(result, dict):
        lines.append(
            "summary: assignments={a} replies={r} complete={c}".format(
                a=int(result.get("assignments", 0) or 0),
                r=int(result.get("replies", 0) or 0),
                c="yes" if bool(result.get("complete", False)) else "no",
            )
        )
        failed = result.get("failed_roles") or []
        pending = result.get("pending_roles") or []
        if failed:
            lines.append("failed_roles: " + ", ".join(str(x) for x in failed))
        if pending:
            lines.append("pending_roles: " + ", ".join(str(x) for x in pending))

    history = task.get("history") or []
    if isinstance(history, list) and history:
        lines.append("recent:")
        for ev in history[-6:]:
            if not isinstance(ev, dict):
                continue
            stage = str(ev.get("stage", "")).strip() or "-"
            status = str(ev.get("status", "")).strip() or "-"
            at = str(ev.get("at", "")).strip() or "-"
            note = str(ev.get("note", "")).strip()
            suffix = f" | {note}" if note else ""
            lines.append(f"- {at} {stage}={status}{suffix}")

    return "\n".join(lines)
