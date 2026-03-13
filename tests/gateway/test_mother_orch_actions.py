#!/usr/bin/env python3
"""Regression tests for Mother-Orch Action API helpers."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GW_DIR = ROOT / "scripts" / "gateway"
MOD_FILE = GW_DIR / "aoe_tg_orch_actions.py"

if str(GW_DIR) not in sys.path:
    sys.path.insert(0, str(GW_DIR))

_spec = importlib.util.spec_from_file_location("aoe_tg_orch_actions_mod", MOD_FILE)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


def test_action_api_schema_exposes_intent_and_mutation_boundaries() -> None:
    schema = mod.mother_orch_action_api_schema()

    assert schema["contract"] == "mother_orch.action_api.v1"
    assert schema["intent_classes"] == ["status", "inspect", "work", "control"]
    assert schema["risk_levels"] == ["safe", "runtime_mutation", "canonical_mutation"]
    assert schema["actions"]["dispatch_task"]["intent_class"] == "work"
    assert schema["actions"]["syncback_apply"]["risk_level"] == "canonical_mutation"


def test_list_mother_orch_actions_contains_core_operator_surface() -> None:
    rows = mod.list_mother_orch_actions()
    names = {row["action"] for row in rows}

    assert "list_projects" in names
    assert "dispatch_task" in names
    assert "offdesk_prepare" in names
    assert "syncback_apply" in names
    assert "sync_bootstrap" in names


def test_normalize_action_call_maps_alias_to_project_status() -> None:
    row = mod.normalize_mother_orch_action_call({"action": "status", "project_key": "O4"})

    assert row["action"] == "get_project_status"
    assert row["family"] == "project_status"
    assert row["intent_class"] == "status"
    assert row["risk_level"] == "safe"
    assert row["project_key"] == "O4"
    assert row["readonly"] is True
    assert row["mutates_runtime"] is False


def test_normalize_dispatch_task_requires_objective_and_preserves_roles() -> None:
    row = mod.normalize_mother_orch_action_call(
        {
            "action": "dispatch",
            "project_key": "O3",
            "prompt": "최근 TODO와 분석 문맥을 보고 다음 실행 우선순위를 정리해줘",
            "requested_roles": ["Codex-Analyst", "Codex-Reviewer"],
        }
    )

    assert row["action"] == "dispatch_task"
    assert row["intent_class"] == "work"
    assert row["project_key"] == "O3"
    assert row["readonly"] is False
    assert row["mutates_runtime"] is True
    assert row["args"]["objective"].startswith("최근 TODO")
    assert row["args"]["requested_roles"] == ["Codex-Analyst", "Codex-Reviewer"]


def test_normalize_retry_task_requires_task_ref() -> None:
    row = mod.normalize_mother_orch_action_call({"action": "retry", "task_ref": "T-003"})

    assert row["action"] == "retry_task"
    assert row["args"]["task_ref"] == "T-003"


def test_normalize_focus_uses_default_project_key() -> None:
    row = mod.normalize_mother_orch_action_call({"action": "focus"}, default_project_key="O4")

    assert row["action"] == "focus_project"
    assert row["project_key"] == "O4"
    assert row["mutates_runtime"] is True


def test_normalize_syncback_apply_marks_canonical_mutation() -> None:
    row = mod.normalize_mother_orch_action_call({"action": "syncback_apply", "project_key": "O3"})

    assert row["risk_level"] == "canonical_mutation"
    assert row["mutates_runtime"] is True
    assert row["mutates_canonical"] is True


def test_unknown_action_raises_runtime_error() -> None:
    try:
        mod.normalize_mother_orch_action_call({"action": "invent_new_magic"})
    except RuntimeError as exc:
        assert "unknown Mother-Orch action" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


def test_infer_action_call_maps_status_prompt_to_monitor_project() -> None:
    row = mod.infer_mother_orch_action_call(
        "결과는 언제 나와? 지금 상태 알려줘",
        default_project_key="O3",
        has_active_task=True,
    )

    assert row["action"] == "monitor_project"
    assert row["project_key"] == "O3"
    assert row["intent_class"] == "status"


def test_infer_action_call_maps_inspection_prompt_to_dispatch_task_readonly() -> None:
    row = mod.infer_mother_orch_action_call(
        "데이터 추출이 완료되었는지 확인 부탁해",
        default_project_key="O4",
        has_active_task=False,
    )

    assert row["action"] == "dispatch_task"
    assert row["project_key"] == "O4"
    assert row["readonly"] is True
    assert row["args"]["objective"] == "데이터 추출이 완료되었는지 확인 부탁해"


def test_action_call_to_resolved_command_maps_monitor_and_offdesk() -> None:
    monitor = mod.action_call_to_resolved_command(
        mod.normalize_mother_orch_action_call({"action": "monitor_project", "project_key": "O3"})
    )
    offdesk = mod.action_call_to_resolved_command(
        mod.normalize_mother_orch_action_call({"action": "offdesk_prepare"})
    )

    assert monitor["cmd"] == "orch-monitor"
    assert monitor["orch_target"] == "O3"
    assert offdesk["cmd"] == "offdesk"
    assert offdesk["rest"] == "prepare"


def test_action_call_maps_bootstrap_alias_to_sync_bootstrap_command() -> None:
    row = mod.normalize_mother_orch_action_call({"action": "bootstrap", "project_key": "O4", "window": "48h"})
    resolved = mod.action_call_to_resolved_command(row)

    assert row["action"] == "sync_bootstrap"
    assert resolved["cmd"] == "sync"
    assert resolved["rest"] == "bootstrap O4 48h"
