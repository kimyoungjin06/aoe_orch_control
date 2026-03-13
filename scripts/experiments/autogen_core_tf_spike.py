#!/usr/bin/env python3
"""Phase 0 dry-run spike for an AutoGen Core TF backend.

This script does not execute AutoGen agents.
It emits a structured plan showing how one TF run could map into an
AutoGen Core runtime while keeping backlog and control-plane ownership in
`aoe_orch_control`.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parents[2]
GW_DIR = ROOT / "scripts" / "gateway"
EXP_DIR = Path(__file__).resolve().parent
if str(GW_DIR) not in sys.path:
    sys.path.insert(0, str(GW_DIR))
if str(EXP_DIR) not in sys.path:
    sys.path.insert(0, str(EXP_DIR))

from aoe_tg_tf_backend import AUTOGEN_CORE_TF_BACKEND, DEFAULT_TF_BACKEND, normalize_tf_backend_name
from aoe_tg_tf_backend_autogen import autogen_core_installed, autogen_core_version
from aoe_tg_tf_event_schema import normalize_runtime_events, tf_followup_proposal_schema, tf_runtime_event_schema
from aoe_tg_tf_exec import parse_roles_csv
from autogen_core_contract_samples import build_sample_followup_proposals


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")


def normalize_roles(raw: str) -> List[str]:
    roles = parse_roles_csv(raw)
    return roles or ["Codex-Dev", "Reviewer"]


def build_agent_specs(roles: List[str]) -> List[Dict[str, Any]]:
    agents: List[Dict[str, Any]] = [
        {
            "agent_id": "tf_orchestrator",
            "agent_type": "RoutedAgent",
            "responsibility": "coordinate TF execution, collect responses, emit final verdict",
        }
    ]
    for role in roles:
        agents.append(
            {
                "agent_id": f"role_{role.lower().replace(' ', '_').replace('-', '_')}",
                "agent_type": "RoutedAgent",
                "role": role,
                "responsibility": f"execute role-specific work for {role}",
            }
        )
    if "Reviewer" not in roles:
        agents.append(
            {
                "agent_id": "role_reviewer",
                "agent_type": "RoutedAgent",
                "role": "Reviewer",
                "responsibility": "verify execution quality and produce pass/retry/fail verdict",
            }
        )
    return agents


def build_runtime_plan(args: argparse.Namespace) -> Dict[str, Any]:
    roles = normalize_roles(args.roles)
    installed = autogen_core_installed()
    version = autogen_core_version()
    workspace = str(Path(args.workspace).expanduser().resolve()) if args.workspace else ""
    expected_focus = [token.strip() for token in str(getattr(args, "expected_focus", "") or "").split(",") if token.strip()]
    runtime_events = normalize_runtime_events(
        [
            {
                "source": "tf_orchestrator",
                "stage": "request.accepted",
                "kind": "lifecycle",
                "status": "info",
                "summary": "accepted TF request and initialized AutoGen runtime plan",
                "payload": {"project_key": str(args.project_key or "").strip()},
            },
            {
                "source": "tf_orchestrator",
                "stage": "roles.resolved",
                "kind": "dispatch",
                "status": "success",
                "summary": "resolved requested role set for the TF runtime",
                "payload": {"roles": roles},
            },
            {
                "source": "autogen_runtime",
                "stage": "runtime.started",
                "kind": "lifecycle",
                "status": "info",
                "summary": "bootstrapped in-process AutoGen runtime",
                "payload": {"runtime": "SingleThreadedAgentRuntime"},
            },
            {
                "source": "reviewer",
                "stage": "verdict.emitted",
                "kind": "verdict",
                "status": "success",
                "summary": "reviewer emits success|retry|fail verdict back to orchestrator",
                "payload": {"verdict": "success|retry|fail"},
            },
            {
                "source": "tf_orchestrator",
                "stage": "proposals.emitted",
                "kind": "proposal",
                "status": "info",
                "summary": "normalized follow-up proposals returned to aoe_orch_control",
                "payload": {"target": "proposal inbox"},
            },
        ],
        default_backend=normalize_tf_backend_name(AUTOGEN_CORE_TF_BACKEND),
        default_source="tf_orchestrator",
        now_iso=now_iso,
    )
    sample_proposals = build_sample_followup_proposals(
        case_id=str(getattr(args, "case_id", "") or "standalone_spike"),
        project_key=str(args.project_key or "").strip(),
        task_summary=str(args.task or "").strip(),
        roles=roles,
        workload_type=str(getattr(args, "workload_type", "") or "").strip(),
        expected_focus=expected_focus,
        source_request_id=str(getattr(args, "source_request_id", "") or "REQ-AUTOGEN-SPIKE"),
        source_todo_id="",
    )
    return {
        "generated_at": now_iso(),
        "phase": "phase0_design_spike",
        "backend_candidate": normalize_tf_backend_name(AUTOGEN_CORE_TF_BACKEND),
        "current_default_backend": DEFAULT_TF_BACKEND,
        "autogen_core": {
            "installed": installed,
            "version": version,
            "runtime": "SingleThreadedAgentRuntime",
            "transport_model": "in-process message bus",
        },
        "tf_request": {
            "project_key": str(args.project_key or "").strip(),
            "task_summary": str(args.task or "").strip(),
            "workspace": workspace,
            "roles": roles,
            "retry_budget": max(0, int(args.retry_budget)),
            "approval_required": bool(args.approval_required),
        },
        "agent_specs": build_agent_specs(roles),
        "message_flow": [
            "project_orch -> tf_orchestrator : tf_request",
            "tf_orchestrator -> role agents : role-specific work packets",
            "role agents -> reviewer/orchestrator : artifacts + evidence",
            "reviewer -> tf_orchestrator : success|retry|fail verdict",
            "tf_orchestrator -> aoe_orch_control : normalized result + follow-up proposals",
        ],
        "ownership_boundary": {
            "owned_by_aoe_orch_control": [
                "telegram control plane",
                "project registry and lock state",
                "runtime todo queue",
                "proposal inbox",
                "canonical TODO syncback",
                "offdesk scheduling",
            ],
            "owned_by_autogen_core_backend": [
                "intra-TF role messaging",
                "role handoff and aggregation",
                "runtime-local verdict production",
            ],
        },
        "normalized_output_contract": {
            "required_fields": [
                "request_id",
                "backend",
                "status",
                "verdict",
                "artifacts",
                "events",
                "followup_proposals",
            ],
            "followup_proposal_fields": [
                "summary",
                "priority",
                "kind",
                "reason",
                "source_request_id",
                "source_todo_id",
                "confidence",
            ],
            "runtime_event_schema": tf_runtime_event_schema(),
            "followup_proposal_schema": tf_followup_proposal_schema(),
        },
        "sample_runtime_events": runtime_events,
        "sample_followup_proposals": sample_proposals,
        "warnings": [
            "This spike does not execute AutoGen agents.",
            "Backlog state mutation remains outside the backend.",
            (
                "AutoGen Core is not installed in the current environment."
                if not installed
                else "AutoGen Core is installed; live backend integration is still intentionally disabled."
            ),
        ],
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Emit a dry-run AutoGen Core TF backend plan.")
    p.add_argument("--project-key", default="O3", help="project/orch key label for the TF request")
    p.add_argument("--task", required=True, help="task summary for the TF spike")
    p.add_argument("--roles", default="Codex-Dev,Reviewer", help="comma-separated TF roles")
    p.add_argument("--workspace", default="", help="workspace path for the TF run")
    p.add_argument("--retry-budget", type=int, default=3, help="retry budget for the TF run")
    p.add_argument("--approval-required", action="store_true", help="mark the TF as approval-gated")
    p.add_argument("--case-id", default="standalone_spike", help="benchmark/spike case identifier")
    p.add_argument("--workload-type", default="", help="benchmark workload type label")
    p.add_argument("--expected-focus", default="", help="comma-separated benchmark focus tags")
    p.add_argument("--source-request-id", default="REQ-AUTOGEN-SPIKE", help="synthetic request id for contract samples")
    return p


def main() -> int:
    args = build_parser().parse_args()
    payload = build_runtime_plan(args)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
