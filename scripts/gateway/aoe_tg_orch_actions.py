#!/usr/bin/env python3
"""Mother-Orch Action API contract helpers.

This module defines the stable action surface Mother-Orch may use when it
decides whether a plaintext request should become:

- a status lookup
- a read-only inspection
- a real work dispatch
- a control-plane mutation

The goal is to keep this seam backend-agnostic so later adapters
(Telegram/CLI/MCP) can share one normalized contract.
"""

from __future__ import annotations

from typing import Any, Dict, List


INTENT_CLASSES = ("status", "inspect", "work", "control")
RISK_LEVELS = ("safe", "runtime_mutation", "canonical_mutation")


ACTION_ALIASES = {
    "map": "list_projects",
    "projects": "list_projects",
    "list_projects": "list_projects",
    "focus": "focus_project",
    "use": "focus_project",
    "focus_project": "focus_project",
    "unlock": "clear_focus",
    "focus_off": "clear_focus",
    "clear_focus": "clear_focus",
    "status": "get_project_status",
    "project_status": "get_project_status",
    "get_project_status": "get_project_status",
    "monitor": "monitor_project",
    "monitor_project": "monitor_project",
    "queue": "get_queue",
    "get_queue": "get_queue",
    "task": "get_task",
    "get_task": "get_task",
    "followup": "list_followups",
    "followups": "list_followups",
    "list_followups": "list_followups",
    "sync_preview": "sync_preview",
    "preview_sync": "sync_preview",
    "sync": "sync_apply",
    "sync_apply": "sync_apply",
    "sync_salvage": "sync_salvage",
    "salvage": "sync_salvage",
    "dispatch": "dispatch_task",
    "run": "dispatch_task",
    "work": "dispatch_task",
    "dispatch_task": "dispatch_task",
    "retry": "retry_task",
    "retry_task": "retry_task",
    "replan": "replan_task",
    "replan_task": "replan_task",
    "accept_proposal": "accept_proposal",
    "proposal_accept": "accept_proposal",
    "reject_proposal": "reject_proposal",
    "proposal_reject": "reject_proposal",
    "syncback_preview": "syncback_preview",
    "syncback_apply": "syncback_apply",
    "offdesk_prepare": "offdesk_prepare",
    "offdesk_review": "offdesk_review",
    "offdesk_start": "offdesk_start",
    "offdesk_on": "offdesk_start",
    "offdesk_status": "offdesk_status",
}


ACTION_CATALOG: Dict[str, Dict[str, Any]] = {
    "list_projects": {
        "family": "project_registry",
        "intent_class": "status",
        "risk_level": "safe",
        "requires_project": False,
        "operator_surface": ["/map"],
        "required_args": [],
        "notes": ["Return visible project registry, scope, and quick project labels."],
    },
    "focus_project": {
        "family": "project_registry",
        "intent_class": "control",
        "risk_level": "runtime_mutation",
        "requires_project": True,
        "operator_surface": ["/use O#", "/focus O#"],
        "required_args": [],
        "notes": ["Pin subsequent plain-language work to one project until cleared."],
    },
    "clear_focus": {
        "family": "project_registry",
        "intent_class": "control",
        "risk_level": "runtime_mutation",
        "requires_project": False,
        "operator_surface": ["/focus off", "/unlock"],
        "required_args": [],
        "notes": ["Clear project lock / focus state."],
    },
    "get_project_status": {
        "family": "project_status",
        "intent_class": "status",
        "risk_level": "safe",
        "requires_project": True,
        "operator_surface": ["/orch status O#"],
        "required_args": [],
        "notes": ["Return compact project state, queue counts, latest task, and lock state."],
    },
    "monitor_project": {
        "family": "project_status",
        "intent_class": "status",
        "risk_level": "safe",
        "requires_project": True,
        "operator_surface": ["/monitor", "/orch monitor O#"],
        "required_args": [],
        "notes": ["Return active task and TF lifecycle detail for operator monitoring."],
    },
    "get_queue": {
        "family": "backlog",
        "intent_class": "status",
        "risk_level": "safe",
        "requires_project": True,
        "operator_surface": ["/queue", "/todo O#"],
        "required_args": [],
        "notes": ["Return runnable open todo rows, blocked head, and follow-up summary."],
    },
    "get_task": {
        "family": "task_runtime",
        "intent_class": "status",
        "risk_level": "safe",
        "requires_project": False,
        "operator_surface": ["/task T-###", "/task REQ-..."],
        "required_args": ["task_ref"],
        "notes": ["Return detailed task lifecycle, tf_phase, lane state, and artifacts."],
    },
    "list_followups": {
        "family": "backlog",
        "intent_class": "status",
        "risk_level": "safe",
        "requires_project": True,
        "operator_surface": ["/todo O# followup", "/queue followup"],
        "required_args": [],
        "notes": ["Return blocked/manual_followup backlog needing operator action."],
    },
    "sync_preview": {
        "family": "backlog_sync",
        "intent_class": "inspect",
        "risk_level": "safe",
        "requires_project": True,
        "operator_surface": ["/sync preview O# 24h"],
        "required_args": [],
        "notes": ["Read sources and show what would change without mutating queue state."],
    },
    "sync_apply": {
        "family": "backlog_sync",
        "intent_class": "work",
        "risk_level": "runtime_mutation",
        "requires_project": True,
        "operator_surface": ["/sync O# 24h", "/sync replace O#"],
        "required_args": [],
        "notes": ["Mutate runtime queue from canonical/recent/salvage sources."],
    },
    "sync_salvage": {
        "family": "backlog_sync",
        "intent_class": "inspect",
        "risk_level": "runtime_mutation",
        "requires_project": True,
        "operator_surface": ["/sync salvage O# 24h"],
        "required_args": [],
        "notes": ["Recover queue/proposals from recent work documents when canonical TODO is missing."],
    },
    "dispatch_task": {
        "family": "tf_execution",
        "intent_class": "work",
        "risk_level": "runtime_mutation",
        "requires_project": True,
        "operator_surface": ["/dispatch ...", "plain text work request"],
        "required_args": ["objective"],
        "notes": ["Create or reuse a task, run Phase1 planning, then execute TF lanes."],
    },
    "retry_task": {
        "family": "tf_execution",
        "intent_class": "work",
        "risk_level": "runtime_mutation",
        "requires_project": False,
        "operator_surface": ["/retry T-###"],
        "required_args": ["task_ref"],
        "notes": ["Retry an existing task, possibly using lane-targeted rerun metadata."],
    },
    "replan_task": {
        "family": "tf_execution",
        "intent_class": "work",
        "risk_level": "runtime_mutation",
        "requires_project": False,
        "operator_surface": ["/replan T-###"],
        "required_args": ["task_ref"],
        "notes": ["Re-enter Phase1 planning for an existing task."],
    },
    "accept_proposal": {
        "family": "proposal_inbox",
        "intent_class": "control",
        "risk_level": "runtime_mutation",
        "requires_project": True,
        "operator_surface": ["/todo accept PROP-###"],
        "required_args": ["proposal_ref"],
        "notes": ["Promote a follow-up proposal into runtime todo queue."],
    },
    "reject_proposal": {
        "family": "proposal_inbox",
        "intent_class": "control",
        "risk_level": "runtime_mutation",
        "requires_project": True,
        "operator_surface": ["/todo reject PROP-### reason"],
        "required_args": ["proposal_ref"],
        "notes": ["Reject a follow-up proposal while preserving audit trail."],
    },
    "syncback_preview": {
        "family": "canonical_backlog",
        "intent_class": "inspect",
        "risk_level": "safe",
        "requires_project": True,
        "operator_surface": ["/todo O# syncback preview"],
        "required_args": [],
        "notes": ["Show drift between runtime queue/proposals and canonical TODO.md."],
    },
    "syncback_apply": {
        "family": "canonical_backlog",
        "intent_class": "control",
        "risk_level": "canonical_mutation",
        "requires_project": True,
        "operator_surface": ["/todo O# syncback apply"],
        "required_args": [],
        "notes": ["Apply approved runtime drift back into canonical TODO.md."],
    },
    "offdesk_prepare": {
        "family": "offdesk",
        "intent_class": "status",
        "risk_level": "safe",
        "requires_project": False,
        "operator_surface": ["/offdesk prepare"],
        "required_args": [],
        "notes": ["Compute readiness, drift, followups, and syncback warnings before night runs."],
    },
    "offdesk_review": {
        "family": "offdesk",
        "intent_class": "status",
        "risk_level": "safe",
        "requires_project": False,
        "operator_surface": ["/offdesk review"],
        "required_args": [],
        "notes": ["List only flagged projects and direct operator to corrective actions."],
    },
    "offdesk_start": {
        "family": "offdesk",
        "intent_class": "control",
        "risk_level": "runtime_mutation",
        "requires_project": False,
        "operator_surface": ["/offdesk on", "/auto on"],
        "required_args": [],
        "notes": ["Start automation after readiness checks are acceptable."],
    },
    "offdesk_status": {
        "family": "offdesk",
        "intent_class": "status",
        "risk_level": "safe",
        "requires_project": False,
        "operator_surface": ["/offdesk status", "/auto status"],
        "required_args": [],
        "notes": ["Return compact automation state, scope, and latest result/failure."],
    },
}


def _contains_any(text: str, markers: List[str] | tuple[str, ...]) -> bool:
    low = _trim_text(text, 4000).lower()
    return any(str(marker).lower() in low for marker in markers)


def infer_mother_orch_action_call(
    prompt: str,
    *,
    default_project_key: str = "",
    has_active_task: bool = False,
) -> Dict[str, Any]:
    text = _trim_text(prompt, 2000)
    low = text.lower()
    if not text:
        raise RuntimeError("missing plaintext prompt for Mother-Orch action inference")

    offdesk_prepare_markers = ("offdesk prepare", "오프데스크 준비", "퇴근 준비", "야간 준비")
    offdesk_review_markers = ("offdesk review", "오프데스크 검토", "퇴근 검토", "야간 검토")
    offdesk_start_markers = ("offdesk on", "퇴근모드 시작", "오프데스크 시작", "야간 실행 시작")
    offdesk_status_markers = ("offdesk status", "auto status", "오프데스크 상태", "자동 상태")
    map_markers = ("프로젝트 목록", "프로젝트들", "workspace", "워크스페이스", "map", "프로젝트 맵")
    queue_markers = ("queue", "대기열", "todo", "할일", "백로그")
    followup_markers = ("followup", "follow-up", "후속", "수동개입", "manual followup")
    sync_markers = ("sync", "동기화", "불러와", "가져와", "salvage")
    preview_markers = ("preview", "미리보기", "보기만", "점검")
    status_markers = ("상태", "진행", "결과", "언제", "모니터", "monitor", "progress", "result", "status")
    inspect_markers = (
        "확인",
        "정리",
        "조사",
        "분석",
        "검토",
        "살펴",
        "요약",
        "찾아",
        "봐줘",
        "check",
        "inspect",
        "review",
        "analyze",
        "investigate",
        "summarize",
    )
    work_markers = (
        "수정",
        "고쳐",
        "고치",
        "구현",
        "작성",
        "반영",
        "푸시",
        "push",
        "커밋",
        "commit",
        "머지",
        "merge",
        "설치",
        "실행",
        "진행",
        "진행해",
        "fix",
        "implement",
        "write",
        "apply",
        "ship",
        "deploy",
    )

    if _contains_any(low, offdesk_prepare_markers):
        return normalize_mother_orch_action_call({"action": "offdesk_prepare"})
    if _contains_any(low, offdesk_review_markers):
        return normalize_mother_orch_action_call({"action": "offdesk_review"})
    if _contains_any(low, offdesk_start_markers):
        return normalize_mother_orch_action_call({"action": "offdesk_start"})
    if _contains_any(low, offdesk_status_markers):
        return normalize_mother_orch_action_call({"action": "offdesk_status"})

    if _contains_any(low, followup_markers):
        return normalize_mother_orch_action_call({"action": "list_followups", "project_key": default_project_key})
    if _contains_any(low, queue_markers):
        return normalize_mother_orch_action_call({"action": "get_queue", "project_key": default_project_key})
    if _contains_any(low, map_markers):
        return normalize_mother_orch_action_call({"action": "list_projects"})

    if _contains_any(low, sync_markers):
        action = "sync_preview" if _contains_any(low, preview_markers) else "sync_apply"
        return normalize_mother_orch_action_call(
            {"action": action, "project_key": default_project_key, "window": "24h"}
        )

    if has_active_task and _contains_any(low, status_markers):
        return normalize_mother_orch_action_call({"action": "monitor_project", "project_key": default_project_key})

    if _contains_any(low, status_markers) and not _contains_any(low, work_markers):
        return normalize_mother_orch_action_call({"action": "monitor_project", "project_key": default_project_key})

    if _contains_any(low, work_markers):
        return normalize_mother_orch_action_call(
            {
                "action": "dispatch_task",
                "project_key": default_project_key,
                "objective": text,
                "readonly": False,
            }
        )

    if _contains_any(low, inspect_markers):
        return normalize_mother_orch_action_call(
            {
                "action": "dispatch_task",
                "project_key": default_project_key,
                "objective": text,
                "readonly": True,
            }
        )

    return normalize_mother_orch_action_call(
        {
            "action": "dispatch_task",
            "project_key": default_project_key,
            "objective": text,
            "readonly": True,
        }
    )


def action_call_to_resolved_command(call: Any) -> Dict[str, Any]:
    row = call if isinstance(call, dict) else {}
    action = _canonical_action_name(row.get("action"))
    project_key = _trim_text(row.get("project_key", ""), 64)
    args = row.get("args") if isinstance(row.get("args"), dict) else {}

    if action == "list_projects":
        return {"cmd": "orch-list"}
    if action == "focus_project":
        return {"cmd": "orch-use", "orch_target": project_key or None}
    if action == "clear_focus":
        return {"cmd": "focus", "rest": "off"}
    if action == "get_project_status":
        return {"cmd": "orch-status", "orch_target": project_key or None}
    if action == "monitor_project":
        return {"cmd": "orch-monitor", "orch_target": project_key or None}
    if action == "get_queue":
        return {"cmd": "queue", "rest": project_key}
    if action == "get_task":
        return {"cmd": "orch-task", "orch_task_request_id": _trim_text(args.get("task_ref", ""), 120)}
    if action == "list_followups":
        rest = f"{project_key} followup".strip() if project_key else "followup"
        return {"cmd": "todo", "rest": rest}
    if action == "sync_preview":
        window = _trim_text(args.get("window", "24h"), 32) or "24h"
        rest = " ".join(part for part in ("preview", project_key, window) if part)
        return {"cmd": "sync", "rest": rest}
    if action == "sync_apply":
        window = _trim_text(args.get("window", "24h"), 32) or "24h"
        replace = bool(args.get("replace", False))
        rest = " ".join(part for part in (("replace" if replace else ""), project_key, window) if part)
        return {"cmd": "sync", "rest": rest}
    if action == "sync_salvage":
        window = _trim_text(args.get("window", "24h"), 32) or "24h"
        rest = " ".join(part for part in ("salvage", project_key, window) if part)
        return {"cmd": "sync", "rest": rest}
    if action == "dispatch_task":
        return {
            "cmd": "run",
            "run_prompt": _trim_text(args.get("objective", ""), 1200),
            "run_force_mode": "dispatch",
            "run_auto_source": f"orch-action:{row.get('intent_class', 'work')}",
        }
    if action == "retry_task":
        return {"cmd": "orch-retry", "orch_retry_request_id": _trim_text(args.get("task_ref", ""), 120)}
    if action == "replan_task":
        return {"cmd": "orch-replan", "orch_replan_request_id": _trim_text(args.get("task_ref", ""), 120)}
    if action == "accept_proposal":
        return {"cmd": "todo", "rest": f"accept {_trim_text(args.get('proposal_ref', ''), 120)}".strip()}
    if action == "reject_proposal":
        reason = _trim_text(args.get("reason", ""), 240)
        rest = f"reject {_trim_text(args.get('proposal_ref', ''), 120)} {reason}".strip()
        return {"cmd": "todo", "rest": rest}
    if action == "syncback_preview":
        rest = f"{project_key} syncback preview".strip() if project_key else "syncback preview"
        return {"cmd": "todo", "rest": rest}
    if action == "syncback_apply":
        rest = f"{project_key} syncback apply".strip() if project_key else "syncback apply"
        return {"cmd": "todo", "rest": rest}
    if action == "offdesk_prepare":
        return {"cmd": "offdesk", "rest": "prepare"}
    if action == "offdesk_review":
        return {"cmd": "offdesk", "rest": "review"}
    if action == "offdesk_start":
        return {"cmd": "offdesk", "rest": "on"}
    if action == "offdesk_status":
        return {"cmd": "offdesk", "rest": "status"}
    raise RuntimeError(f"unsupported Mother-Orch action mapping: {action}")


def _trim_text(raw: Any, limit: int) -> str:
    return str(raw or "").strip()[: max(0, int(limit or 0))]


def _normalize_bool(raw: Any, default: bool) -> bool:
    if isinstance(raw, bool):
        return raw
    token = str(raw or "").strip().lower()
    if token in {"1", "true", "yes", "on", "y"}:
        return True
    if token in {"0", "false", "no", "off", "n"}:
        return False
    return bool(default)


def _canonical_action_name(raw: Any) -> str:
    token = _trim_text(raw, 64).lower().replace(" ", "_")
    if not token:
        raise RuntimeError("missing Mother-Orch action name")
    canonical = ACTION_ALIASES.get(token, token)
    if canonical not in ACTION_CATALOG:
        raise RuntimeError(f"unknown Mother-Orch action: {raw}")
    return canonical


def _normalize_project_key(raw: Any, default_project_key: str = "") -> str:
    token = _trim_text(raw, 64) or _trim_text(default_project_key, 64)
    return token


def mother_orch_action_api_schema() -> Dict[str, Any]:
    actions: Dict[str, Any] = {}
    for name, row in ACTION_CATALOG.items():
        actions[name] = {
            "family": row["family"],
            "intent_class": row["intent_class"],
            "risk_level": row["risk_level"],
            "requires_project": bool(row["requires_project"]),
            "required_args": list(row["required_args"]),
            "operator_surface": list(row["operator_surface"]),
            "notes": list(row["notes"]),
        }
    return {
        "contract": "mother_orch.action_api.v1",
        "intent_classes": list(INTENT_CLASSES),
        "risk_levels": list(RISK_LEVELS),
        "actions": actions,
        "notes": [
            "This is the stable control-plane seam Mother-Orch should use before any MCP adapter is added.",
            "Adapters may convert Telegram/CLI/MCP requests into this call envelope, but must not bypass it.",
            "Action API owns intent and mutation boundaries; execution backends stay downstream.",
        ],
    }


def list_mother_orch_actions() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for name in sorted(ACTION_CATALOG.keys()):
        row = ACTION_CATALOG[name]
        rows.append(
            {
                "action": name,
                "family": row["family"],
                "intent_class": row["intent_class"],
                "risk_level": row["risk_level"],
                "requires_project": bool(row["requires_project"]),
                "required_args": list(row["required_args"]),
                "operator_surface": list(row["operator_surface"]),
            }
        )
    return rows


def normalize_mother_orch_action_call(
    raw: Any,
    *,
    default_project_key: str = "",
) -> Dict[str, Any]:
    data = raw if isinstance(raw, dict) else {"action": raw}
    action = _canonical_action_name(data.get("action"))
    definition = ACTION_CATALOG[action]

    args_in = data.get("args")
    args = dict(args_in) if isinstance(args_in, dict) else {}
    project_key = _normalize_project_key(
        data.get("project_key", args.get("project_key", "")),
        default_project_key=default_project_key,
    )
    if definition["requires_project"] and not project_key:
        raise RuntimeError(f"{action} requires project_key")

    normalized_args: Dict[str, Any] = {}
    if action in {"get_task", "retry_task", "replan_task"}:
        task_ref = _trim_text(data.get("task_ref", args.get("task_ref", "")), 120)
        if not task_ref:
            raise RuntimeError(f"{action} requires task_ref")
        normalized_args["task_ref"] = task_ref
    elif action in {"accept_proposal", "reject_proposal"}:
        proposal_ref = _trim_text(data.get("proposal_ref", args.get("proposal_ref", "")), 120)
        if not proposal_ref:
            raise RuntimeError(f"{action} requires proposal_ref")
        normalized_args["proposal_ref"] = proposal_ref
        reject_reason = _trim_text(data.get("reason", args.get("reason", "")), 240)
        if action == "reject_proposal" and reject_reason:
            normalized_args["reason"] = reject_reason
    elif action == "dispatch_task":
        objective = _trim_text(
            data.get("objective", data.get("prompt", args.get("objective", args.get("prompt", "")))),
            1200,
        )
        if not objective:
            raise RuntimeError("dispatch_task requires objective")
        normalized_args["objective"] = objective
        normalized_args["title"] = _trim_text(data.get("title", args.get("title", "")), 160) or objective[:160]
        normalized_args["readonly"] = _normalize_bool(data.get("readonly", args.get("readonly", False)), False)
        roles = args.get("requested_roles", data.get("requested_roles", []))
        if isinstance(roles, list):
            normalized_args["requested_roles"] = [_trim_text(item, 64) for item in roles if _trim_text(item, 64)]
    elif action in {"sync_preview", "sync_apply", "sync_salvage"}:
        normalized_args["window"] = _trim_text(data.get("window", args.get("window", "")), 32) or "24h"
        if action == "sync_apply":
            normalized_args["replace"] = _normalize_bool(data.get("replace", args.get("replace", False)), False)
    elif action in {"syncback_preview", "syncback_apply"}:
        normalized_args["include_blocked_notes"] = _normalize_bool(
            data.get("include_blocked_notes", args.get("include_blocked_notes", True)),
            True,
        )

    for key in definition["required_args"]:
        if key not in normalized_args:
            raise RuntimeError(f"{action} requires {key}")

    return {
        "action": action,
        "family": definition["family"],
        "intent_class": definition["intent_class"],
        "risk_level": definition["risk_level"],
        "project_key": project_key,
        "readonly": definition["risk_level"] == "safe"
        if action != "dispatch_task"
        else bool(normalized_args.get("readonly", False)),
        "mutates_runtime": definition["risk_level"] in {"runtime_mutation", "canonical_mutation"},
        "mutates_canonical": definition["risk_level"] == "canonical_mutation",
        "operator_surface": list(definition["operator_surface"]),
        "args": normalized_args,
    }
