#!/usr/bin/env python3
"""Task view helpers extracted from the gateway monolith."""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, Iterable, List, Optional

from aoe_tg_orch_contract import derive_tf_phase, derive_tf_phase_reason, normalize_tf_phase


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
        f"tf_phase: {normalize_tf_phase(derive_tf_phase(task), 'queued')}",
        f"mode: {mode}",
        f"roles: {', '.join(roles) if roles else '-'}",
        f"verifier_roles: {', '.join(verifiers) if verifiers else '-'}",
    ]
    tf_phase_reason = str(task.get("tf_phase_reason", "")).strip() or derive_tf_phase_reason(task)
    if tf_phase_reason:
        lines.append(f"tf_phase_reason: {tf_phase_reason}")

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
        meta = plan.get("meta") if isinstance(plan.get("meta"), dict) else {}
        team_spec = meta.get("phase2_team_spec") if isinstance(meta.get("phase2_team_spec"), dict) else {}
        execution_plan = meta.get("phase2_execution_plan") if isinstance(meta.get("phase2_execution_plan"), dict) else {}
        lane_states = task.get("lane_states") if isinstance(task.get("lane_states"), dict) else {}
        execution_lane_state_rows = lane_states.get("execution") if isinstance(lane_states.get("execution"), list) else []
        review_lane_state_rows = lane_states.get("review") if isinstance(lane_states.get("review"), list) else []
        execution_lane_status = {
            str(row.get("lane_id", "")).strip(): str(row.get("status", "")).strip()
            for row in execution_lane_state_rows
            if isinstance(row, dict)
        }
        review_lane_status = {
            str(row.get("lane_id", "")).strip(): str(row.get("status", "")).strip()
            for row in review_lane_state_rows
            if isinstance(row, dict)
        }
        review_lane_verdict = {
            str(row.get("lane_id", "")).strip(): str(row.get("verdict", "")).strip()
            for row in review_lane_state_rows
            if isinstance(row, dict) and str(row.get("verdict", "")).strip()
        }
        review_lane_action = {
            str(row.get("lane_id", "")).strip(): str(row.get("action", "")).strip()
            for row in review_lane_state_rows
            if isinstance(row, dict) and str(row.get("action", "")).strip()
        }
        lane_summary = lane_states.get("summary") if isinstance(lane_states.get("summary"), dict) else {}
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

        execution_groups = team_spec.get("execution_groups") if isinstance(team_spec.get("execution_groups"), list) else []
        if execution_groups:
            lines.append(
                "phase2_execution: {mode} lanes={count}".format(
                    mode=str(team_spec.get("execution_mode", "single")).strip() or "single",
                    count=len(execution_groups),
                )
            )
            for row in execution_groups[:6]:
                if not isinstance(row, dict):
                    continue
                gid = str(row.get("group_id", "")).strip() or "E"
                role = str(row.get("role", "")).strip() or "Worker"
                subtask_ids = [str(item).strip() for item in (row.get("subtask_ids") or []) if str(item).strip()]
                lines.append(f"- lane {gid} [{role}] -> {', '.join(subtask_ids) if subtask_ids else '-'}")

        review_groups = team_spec.get("review_groups") if isinstance(team_spec.get("review_groups"), list) else []
        if review_groups:
            lines.append(
                "phase2_review: {mode} lanes={count}".format(
                    mode=str(team_spec.get("review_mode", "skip")).strip() or "skip",
                    count=len(review_groups),
                )
            )
            for row in review_groups[:4]:
                if not isinstance(row, dict):
                    continue
                gid = str(row.get("group_id", "")).strip() or "R"
                role = str(row.get("role", "")).strip() or "Reviewer"
                kind = str(row.get("kind", "")).strip() or "verifier"
                lines.append(f"- review {gid} [{role}/{kind}]")

        execution_lanes = execution_plan.get("execution_lanes") if isinstance(execution_plan.get("execution_lanes"), list) else []
        review_lanes = execution_plan.get("review_lanes") if isinstance(execution_plan.get("review_lanes"), list) else []
        if not execution_lanes and execution_lane_state_rows:
            execution_lanes = [
                {
                    "lane_id": str(row.get("lane_id", "")).strip(),
                    "role": str(row.get("role", "")).strip(),
                    "subtask_ids": list(row.get("subtask_ids") or []),
                    "parallel": bool(row.get("parallel", False)),
                }
                for row in execution_lane_state_rows
                if isinstance(row, dict) and str(row.get("lane_id", "")).strip()
            ]
        if not review_lanes and review_lane_state_rows:
            review_lanes = [
                {
                    "lane_id": str(row.get("lane_id", "")).strip(),
                    "role": str(row.get("role", "")).strip(),
                    "kind": str(row.get("kind", "")).strip() or "verifier",
                    "depends_on": list(row.get("depends_on") or []),
                    "parallel": bool(row.get("parallel", False)),
                }
                for row in review_lane_state_rows
                if isinstance(row, dict) and str(row.get("lane_id", "")).strip()
            ]
        if execution_plan:
            lines.append(
                "phase2_exec_plan: {mode} workers_parallel={workers} reviews_parallel={reviews} readonly={readonly}".format(
                    mode=str(execution_plan.get("execution_mode", "single")).strip() or "single",
                    workers="yes" if bool(execution_plan.get("parallel_workers", len(execution_lanes) > 1)) else "no",
                    reviews="yes" if bool(execution_plan.get("parallel_reviews", len(review_lanes) > 1)) else "no",
                    readonly="yes" if bool(execution_plan.get("readonly", True)) else "no",
                )
            )
        if lane_summary:
            exec_counts = lane_summary.get("execution") if isinstance(lane_summary.get("execution"), dict) else {}
            review_counts = lane_summary.get("review") if isinstance(lane_summary.get("review"), dict) else {}
            review_verdicts = lane_summary.get("review_verdicts") if isinstance(lane_summary.get("review_verdicts"), dict) else {}
            parts: List[str] = []
            if exec_counts:
                parts.append("exec " + ", ".join(f"{key}={value}" for key, value in sorted(exec_counts.items())))
            if review_counts:
                parts.append("review " + ", ".join(f"{key}={value}" for key, value in sorted(review_counts.items())))
            if review_verdicts:
                parts.append("review_verdict " + ", ".join(f"{key}={value}" for key, value in sorted(review_verdicts.items())))
            if parts:
                lines.append("phase2_lane_state: " + " | ".join(parts))
        for row in execution_lanes[:6]:
            if not isinstance(row, dict):
                continue
            gid = str(row.get("lane_id", "")).strip() or "L"
            role = str(row.get("role", "")).strip() or "Worker"
            subtask_ids = [str(item).strip() for item in (row.get("subtask_ids") or []) if str(item).strip()]
            mode = "parallel" if bool(row.get("parallel", True)) else "serial"
            status = execution_lane_status.get(gid, "")
            suffix = f" [{status}]" if status else ""
            lines.append(f"- exec {gid} [{role}/{mode}]{suffix} -> {', '.join(subtask_ids) if subtask_ids else '-'}")
        for row in review_lanes[:4]:
            if not isinstance(row, dict):
                continue
            gid = str(row.get("lane_id", "")).strip() or "R"
            role = str(row.get("role", "")).strip() or "Reviewer"
            kind = str(row.get("kind", "")).strip() or "verifier"
            depends = [str(item).strip() for item in (row.get("depends_on") or []) if str(item).strip()]
            mode = "parallel" if bool(row.get("parallel", True)) else "serial"
            suffix = f" after {', '.join(depends)}" if depends else ""
            status = review_lane_status.get(gid, "")
            status_suffix = f" [{status}]" if status else ""
            verdict = review_lane_verdict.get(gid, "")
            action = review_lane_action.get(gid, "")
            verdict_suffix = ""
            if verdict:
                verdict_suffix = f" -> {verdict}"
                if action and action != "none":
                    verdict_suffix += f"/{action}"
            lines.append(f"- critic {gid} [{role}/{kind}/{mode}]{status_suffix}{verdict_suffix}{suffix}")

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
        rerun_exec = [str(x).strip() for x in (exec_critic.get("rerun_execution_lane_ids") or []) if str(x).strip()]
        rerun_review = [str(x).strip() for x in (exec_critic.get("rerun_review_lane_ids") or []) if str(x).strip()]
        manual_exec = [str(x).strip() for x in (exec_critic.get("manual_followup_execution_lane_ids") or []) if str(x).strip()]
        manual_review = [str(x).strip() for x in (exec_critic.get("manual_followup_review_lane_ids") or []) if str(x).strip()]
        if rerun_exec or rerun_review:
            lines.append(
                "exec_rerun_targets: execution={exec} review={review}".format(
                    exec=", ".join(rerun_exec) if rerun_exec else "-",
                    review=", ".join(rerun_review) if rerun_review else "-",
                )
            )
        if manual_exec or manual_review:
            lines.append(
                "exec_manual_followup_targets: execution={exec} review={review}".format(
                    exec=", ".join(manual_exec) if manual_exec else "-",
                    review=", ".join(manual_review) if manual_review else "-",
                )
            )

    result = task.get("result")
    if isinstance(result, dict):
        lines.append(
            "summary: assignments={a} replies={r} complete={c}".format(
                a=int(result.get("assignments", 0) or 0),
                r=int(result.get("replies", 0) or 0),
                c="yes" if bool(result.get("complete", False)) else "no",
            )
        )
        phase2_request_ids = result.get("phase2_request_ids") if isinstance(result.get("phase2_request_ids"), dict) else {}
        if phase2_request_ids:
            exec_req = str(phase2_request_ids.get("execution", "")).strip() or "-"
            review_req = str(phase2_request_ids.get("review", "")).strip() or "-"
            lines.append(f"phase2_requests: execution={exec_req} review={review_req}")
        if "phase2_review_triggered" in result:
            lines.append("phase2_review_triggered: " + ("yes" if bool(result.get("phase2_review_triggered")) else "no"))
        review_skip_reason = str(result.get("phase2_review_skipped_reason", "")).strip()
        if review_skip_reason:
            lines.append("phase2_review_skip_reason: " + review_skip_reason[:240])
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
