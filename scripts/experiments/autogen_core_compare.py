#!/usr/bin/env python3
"""Compare local TF backend contract vs AutoGen Core design contract.

This is a Phase 0/1 development harness.
It does not mutate runtime queue state and does not execute live AutoGen agents.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

ROOT = Path(__file__).resolve().parents[2]
GW_DIR = ROOT / "scripts" / "gateway"
if str(GW_DIR) not in sys.path:
    sys.path.insert(0, str(GW_DIR))

from aoe_tg_tf_backend import AUTOGEN_CORE_TF_BACKEND, DEFAULT_TF_BACKEND
from aoe_tg_tf_backend_autogen import autogen_core_installed, autogen_core_version
from aoe_tg_tf_backend_local import local_backend

from autogen_core_tf_spike import build_runtime_plan as build_autogen_runtime_plan


DEFAULT_BENCHMARK_FILE = ROOT / "docs" / "benchmarks" / "autogen_core_tf_benchmark_set.json"


def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")


def load_benchmarks(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError("benchmark file must be a JSON object")
    cases = data.get("cases")
    if not isinstance(cases, list):
        raise RuntimeError("benchmark file missing cases list")
    return data


def filter_cases(cases: Iterable[Dict[str, Any]], selected_ids: List[str]) -> List[Dict[str, Any]]:
    if not selected_ids:
        return [dict(case) for case in cases if isinstance(case, dict)]
    wanted = {str(item).strip() for item in selected_ids if str(item).strip()}
    return [dict(case) for case in cases if isinstance(case, dict) and str(case.get("id", "")).strip() in wanted]


def build_local_runtime_plan(case: Dict[str, Any]) -> Dict[str, Any]:
    roles = [str(role).strip() for role in case.get("roles", []) if str(role).strip()]
    task = str(case.get("task", "")).strip()
    project_key = str(case.get("project_key", "")).strip()
    return {
        "generated_at": now_iso(),
        "phase": "phase0_local_contract",
        "backend_candidate": DEFAULT_TF_BACKEND,
        "backend_availability": local_backend().availability().__dict__,
        "local_backend": {
            "runtime": "aoe-orch + tmux request-scoped workers",
            "transport_model": "process + tmux session orchestration",
            "worker_spawn_model": "request-scoped worker sessions",
        },
        "tf_request": {
            "project_key": project_key,
            "task_summary": task,
            "workspace": "",
            "roles": roles,
            "retry_budget": max(0, int(case.get("retry_budget", 3))),
            "approval_required": bool(case.get("approval_required", False)),
        },
        "message_flow": [
            "project_orch -> aoe-orch preview : resolve roles and execution shape",
            "gateway -> tmux worker sessions : spawn request-scoped workers",
            "gateway -> aoe-orch run : execute TF request",
            "workers/reviewer -> gateway : artifacts, verdict, follow-up proposals",
            "gateway -> aoe_orch_control : normalized request/task/todo state updates",
        ],
        "ownership_boundary": {
            "owned_by_aoe_orch_control": [
                "telegram control plane",
                "project registry and lock state",
                "runtime todo queue",
                "proposal inbox",
                "canonical TODO syncback",
                "offdesk scheduling",
                "tf workspace lifecycle",
            ],
            "owned_by_local_backend": [
                "aoe-orch preview and request execution",
                "request-scoped worker session spawning",
                "local role execution and reviewer feedback",
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
        },
    }


def make_autogen_args(case: Dict[str, Any]) -> argparse.Namespace:
    roles = ",".join(str(role).strip() for role in case.get("roles", []) if str(role).strip())
    return argparse.Namespace(
        project_key=str(case.get("project_key", "")).strip(),
        task=str(case.get("task", "")).strip(),
        roles=roles,
        workspace="",
        retry_budget=max(0, int(case.get("retry_budget", 3))),
        approval_required=bool(case.get("approval_required", False)),
    )


def compare_contracts(local_plan: Dict[str, Any], autogen_plan: Dict[str, Any], case: Dict[str, Any]) -> Dict[str, Any]:
    local_request = dict(local_plan.get("tf_request") or {})
    autogen_request = dict(autogen_plan.get("tf_request") or {})
    local_contract = dict(local_plan.get("normalized_output_contract") or {})
    autogen_contract = dict(autogen_plan.get("normalized_output_contract") or {})
    return {
        "case_id": str(case.get("id", "")).strip(),
        "request_shape_match": {
            "project_key": local_request.get("project_key") == autogen_request.get("project_key"),
            "task_summary": local_request.get("task_summary") == autogen_request.get("task_summary"),
            "roles": list(local_request.get("roles") or []) == list(autogen_request.get("roles") or []),
            "retry_budget": local_request.get("retry_budget") == autogen_request.get("retry_budget"),
            "approval_required": local_request.get("approval_required") == autogen_request.get("approval_required"),
        },
        "output_contract_match": {
            "required_fields": list(local_contract.get("required_fields") or []) == list(autogen_contract.get("required_fields") or []),
            "proposal_fields": list(local_contract.get("followup_proposal_fields") or []) == list(autogen_contract.get("followup_proposal_fields") or []),
        },
        "ownership_model": {
            "local_backlog_owned_by_repo": True,
            "autogen_backlog_owned_by_repo": True,
            "safe_for_live_replacement": False,
        },
        "autogen_runtime": {
            "installed": bool((autogen_plan.get("autogen_core") or {}).get("installed", False)),
            "version": str((autogen_plan.get("autogen_core") or {}).get("version", "")).strip(),
        },
    }


def run_case(case: Dict[str, Any]) -> Dict[str, Any]:
    local_plan = build_local_runtime_plan(case)
    autogen_plan = build_autogen_runtime_plan(make_autogen_args(case))
    comparison = compare_contracts(local_plan, autogen_plan, case)
    return {
        "case": case,
        "local_plan": local_plan,
        "autogen_plan": autogen_plan,
        "comparison": comparison,
    }


def build_summary(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    all_cases = len(results)
    autogen_installed_cases = sum(
        1 for row in results if bool(((row.get("autogen_plan") or {}).get("autogen_core") or {}).get("installed", False))
    )
    request_shape_ok = sum(
        1
        for row in results
        if all(bool(v) for v in ((row.get("comparison") or {}).get("request_shape_match") or {}).values())
    )
    contract_ok = sum(
        1
        for row in results
        if all(bool(v) for v in ((row.get("comparison") or {}).get("output_contract_match") or {}).values())
    )
    return {
        "generated_at": now_iso(),
        "benchmark_count": all_cases,
        "autogen_core_installed_cases": autogen_installed_cases,
        "request_shape_match_cases": request_shape_ok,
        "output_contract_match_cases": contract_ok,
        "safe_for_live_replacement": False,
        "recommendation": (
            "keep autogen_core in experiment-only mode until backend output parity and event mirroring are implemented"
        ),
    }


def format_markdown(results: List[Dict[str, Any]], summary: Dict[str, Any]) -> str:
    lines = [
        "# AutoGen Core TF Compare",
        "",
        f"- generated_at: {summary['generated_at']}",
        f"- benchmark_count: {summary['benchmark_count']}",
        f"- request_shape_match_cases: {summary['request_shape_match_cases']}",
        f"- output_contract_match_cases: {summary['output_contract_match_cases']}",
        f"- safe_for_live_replacement: {summary['safe_for_live_replacement']}",
        f"- recommendation: {summary['recommendation']}",
        "",
        "| case | roles | local runtime | autogen installed | request shape | output contract |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in results:
        case = row["case"]
        comparison = row["comparison"]
        local_plan = row["local_plan"]
        autogen_plan = row["autogen_plan"]
        roles = ",".join(case.get("roles", []))
        request_ok = all(bool(v) for v in comparison["request_shape_match"].values())
        contract_ok = all(bool(v) for v in comparison["output_contract_match"].values())
        lines.append(
            "| {case_id} | {roles} | {runtime} | {installed} | {request_ok} | {contract_ok} |".format(
                case_id=case.get("id", ""),
                roles=roles or "-",
                runtime=((local_plan.get("local_backend") or {}).get("runtime", "-")),
                installed=((autogen_plan.get("autogen_core") or {}).get("installed", False)),
                request_ok=request_ok,
                contract_ok=contract_ok,
            )
        )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Compare the local TF backend contract against the AutoGen Core design contract.")
    p.add_argument("--benchmark-file", default=str(DEFAULT_BENCHMARK_FILE), help="path to benchmark JSON file")
    p.add_argument("--case", action="append", default=[], help="benchmark case id to run (repeatable)")
    p.add_argument("--format", choices=("json", "markdown"), default="json", help="output format")
    return p


def main() -> int:
    args = build_parser().parse_args()
    benchmark_file = Path(str(args.benchmark_file)).expanduser().resolve()
    payload = load_benchmarks(benchmark_file)
    cases = filter_cases(payload.get("cases", []), list(args.case or []))
    if not cases:
        raise SystemExit("no benchmark cases selected")
    results = [run_case(case) for case in cases]
    summary = build_summary(results)
    output = {
        "benchmark_file": str(benchmark_file),
        "summary": summary,
        "results": results,
    }
    if args.format == "markdown":
        print(format_markdown(results, summary))
    else:
        print(json.dumps(output, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
