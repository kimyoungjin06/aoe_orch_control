#!/usr/bin/env python3
"""Regression tests for Orch core contract helpers."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GW_DIR = ROOT / "scripts" / "gateway"
MOD_FILE = GW_DIR / "aoe_tg_orch_contract.py"

if str(GW_DIR) not in sys.path:
    sys.path.insert(0, str(GW_DIR))

_spec = importlib.util.spec_from_file_location("aoe_tg_orch_contract_mod", MOD_FILE)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


def test_normalize_orch_task_spec_fills_defaults_and_lineage() -> None:
    row = mod.normalize_orch_task_spec(
        {
            "title": "Rebuild nightly queue",
            "objective": "Recover actionable backlog from recent work and prepare offdesk execution",
            "roles": ["Local-Analyst", "Reviewer"],
            "acceptance": ["Queue is rebuilt from recent docs", "Operator can review drift before offdesk"],
            "retry_budget": {"max_retries": 4, "critic_owned": False},
            "source_ref": {"todo_id": "TODO-101"},
        },
        task_id="TASK-101",
        request_id="REQ-101",
        project_key="O4",
        source="offdesk",
    )

    assert row["task_id"] == "TASK-101"
    assert row["request_id"] == "REQ-101"
    assert row["project_key"] == "O4"
    assert row["source"] == "offdesk"
    assert row["priority"] == "P2"
    assert row["readonly"] is True
    assert row["approval_mode"] == "policy"
    assert row["requested_roles"] == ["Local-Analyst", "Reviewer"]
    assert row["source_ref"]["todo_id"] == "TODO-101"
    assert row["retry_budget"] == {"max_retries": 4, "critic_owned": False}


def test_normalize_tf_plan_uses_requested_roles_and_block_reasons() -> None:
    spec = mod.normalize_orch_task_spec(
        {
            "title": "Summarize benchmark drift",
            "objective": "Produce a compact benchmark drift note",
            "requested_roles": ["Local-Writer", "Reviewer"],
        },
        task_id="TASK-201",
        project_key="O3",
    )
    plan = mod.normalize_tf_plan(
        {
            "status": "blocked",
            "summary": "Need a narrower benchmark slice",
            "subtasks": [{"goal": "Draft a short benchmark note"}],
        },
        task_spec=spec,
    )

    assert plan["status"] == "blocked"
    assert plan["assignments"][0]["role"] == "Local-Writer"
    assert plan["execution_order"] == ["Local-Writer"]
    assert plan["critic"]["role"] == "Local-Writer"
    assert plan["blocking_issues"] == ["planner did not provide an executable route"]
    assert plan["meta"]["phase2_team_spec"]["execution_mode"] == "single"
    assert plan["meta"]["phase2_team_spec"]["execution_groups"][0]["role"] == "Local-Writer"


def test_normalize_phase2_team_spec_builds_parallel_execution_and_review_groups() -> None:
    spec = mod.normalize_phase2_team_spec(
        None,
        plan={
            "summary": "parallel build",
            "subtasks": [
                {"id": "S1", "title": "Implement", "goal": "build feature", "owner_role": "Local-Dev"},
                {"id": "S2", "title": "Document", "goal": "write handoff", "owner_role": "Local-Writer"},
            ],
        },
        roles=["Local-Dev", "Local-Writer", "Reviewer"],
        verifier_roles=["Reviewer", "QA"],
        require_verifier=True,
    )

    assert spec["execution_mode"] == "parallel"
    assert [row["role"] for row in spec["execution_groups"]] == ["Local-Dev", "Local-Writer"]
    assert spec["review_mode"] == "parallel"
    assert [row["role"] for row in spec["review_groups"]] == ["Reviewer", "QA"]
    assert spec["critic_role"] == "Reviewer"
    assert "Reviewer" in spec["team_roles"]


def test_normalize_phase2_team_spec_expands_claude_companion_execution_and_review_groups() -> None:
    spec = mod.normalize_phase2_team_spec(
        None,
        plan={
            "summary": "parallel doc and analysis",
            "subtasks": [
                {"id": "S1", "title": "Document", "goal": "write handoff", "owner_role": "Local-Writer"},
                {"id": "S2", "title": "Analyze", "goal": "compare options", "owner_role": "Local-Analyst"},
            ],
        },
        roles=[
            "Local-Writer",
            "Claude-Writer",
            "Local-Analyst",
            "Claude-Analyst",
            "Reviewer",
            "Claude-Reviewer",
        ],
        verifier_roles=["Reviewer"],
        require_verifier=True,
    )

    assert [row["role"] for row in spec["execution_groups"]] == [
        "Local-Writer",
        "Claude-Writer",
        "Local-Analyst",
        "Claude-Analyst",
    ]
    assert [row["role"] for row in spec["review_groups"]] == ["Reviewer", "Claude-Reviewer"]
    assert spec["execution_mode"] == "parallel"
    assert spec["review_mode"] == "parallel"
    assert "Claude-Writer" in spec["team_roles"]
    assert "Claude-Analyst" in spec["team_roles"]
    assert "Claude-Reviewer" in spec["team_roles"]


def test_normalize_tf_verdict_coerces_retry_and_manual_followup() -> None:
    verdict = mod.normalize_tf_verdict(
        {
            "verdict": "replan",
            "summary": "Need a narrower slice",
            "reason": "The current scope is too broad for a safe report",
            "fix": "Split by module and rerun",
            "artifacts": [{"path": "reports/drift.md", "kind": "report"}],
        },
        request_id="REQ-301",
        tf_id="TF-301",
        attempt=2,
        max_attempts=3,
    )

    assert verdict["status"] == "retry"
    assert verdict["action"] == "retry"
    assert verdict["request_id"] == "REQ-301"
    assert verdict["tf_id"] == "TF-301"
    assert verdict["attempt"] == 2
    assert verdict["max_attempts"] == 3
    assert verdict["manual_followup"] is False
    assert verdict["artifacts"] == [
        {"path": "reports/drift.md", "kind": "report", "summary": "reports/drift.md"},
    ]


def test_normalize_orch_followup_proposals_extends_base_schema() -> None:
    rows = mod.normalize_orch_followup_proposals(
        [
            {
                "summary": "Add a handoff note for the reviewer",
                "kind": "handoff",
                "priority": "P1",
                "reason": "The reviewer needs a compact artifact",
                "confidence": 0.82,
            }
        ],
        source_request_id="REQ-401",
        source_todo_id="TODO-401",
        source_tf_id="TF-401",
        owner_role="Local-Writer",
    )

    assert rows == [
        {
            "summary": "Add a handoff note for the reviewer",
            "priority": "P1",
            "kind": "handoff",
            "reason": "The reviewer needs a compact artifact",
            "source_request_id": "REQ-401",
            "source_todo_id": "TODO-401",
            "confidence": 0.82,
            "source_tf_id": "TF-401",
            "owner_role": "Local-Writer",
            "acceptance": [
                "Proposal can be accepted into backlog without re-reading the full TF transcript: Add a handoff note for the reviewer",
            ],
        }
    ]


def test_orch_runtime_event_schema_wraps_backend_neutral_contract() -> None:
    schema = mod.orch_runtime_event_schema()
    assert schema["contract"] == "orch.runtime_event.v1"
    assert "required_fields" in schema
    assert "runtime event contract is shared by local and experimental TF backends" in schema["notes"]


def test_derive_tf_phase_prefers_planning_block_then_retry_then_completed() -> None:
    blocked = mod.derive_tf_phase(
        {
            "status": "failed",
            "stages": {"planning": "done", "execution": "pending"},
            "plan_gate_passed": False,
            "plan_gate_reason": "missing acceptance",
            "exec_critic": {"verdict": "retry", "reason": "need evidence"},
        }
    )
    retry = mod.derive_tf_phase(
        {
            "status": "failed",
            "stages": {"planning": "done", "execution": "done", "verification": "done"},
            "plan_gate_passed": True,
            "exec_critic": {"verdict": "retry", "reason": "need evidence"},
        }
    )
    completed = mod.derive_tf_phase(
        {
            "status": "completed",
            "stages": {"planning": "done", "execution": "done", "verification": "done", "integration": "done", "close": "done"},
        }
    )

    assert blocked == "blocked"
    assert retry == "needs_retry"
    assert completed == "completed"


def test_derive_tf_phase_reason_uses_gate_then_exec_then_stage_failure() -> None:
    assert (
        mod.derive_tf_phase_reason(
            {"plan_gate_reason": "missing acceptance", "exec_critic": {"reason": "need evidence"}}
        )
        == "missing acceptance"
    )
    assert mod.derive_tf_phase_reason({"exec_critic": {"reason": "need evidence"}}) == "need evidence"
    assert mod.derive_tf_phase_reason({"stages": {"verification": "failed"}}) == "verification failed"
