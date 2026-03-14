#!/usr/bin/env python3
"""Regression tests for Phase1 ensemble planning."""

from __future__ import annotations

import importlib.util
import json
import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[2]
GW_DIR = ROOT / "scripts" / "gateway"
PLAN_FILE = GW_DIR / "aoe_tg_plan_ensemble.py"
PIPELINE_FILE = GW_DIR / "aoe_tg_plan_pipeline.py"
GW_FILE = GW_DIR / "aoe-telegram-gateway.py"

if str(GW_DIR) not in sys.path:
    sys.path.insert(0, str(GW_DIR))


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


ensemble_mod = _load_module(PLAN_FILE, "aoe_tg_plan_ensemble_mod")
pipeline_mod = _load_module(PIPELINE_FILE, "aoe_tg_plan_pipeline_mod")
gw = _load_module(GW_FILE, "aoe_telegram_gateway_mod_phase1")


def test_phase1_ensemble_runs_three_rounds_and_uses_both_providers() -> None:
    prompts: list[str] = []

    def _runner(name: str):
        def _run(prompt: str, timeout_sec: int) -> str:
            prompts.append(f"{name}:{prompt.splitlines()[0]}")
            return '{"summary":"plan from %s","subtasks":[{"id":"S1","title":"Draft","goal":"Write the plan","owner_role":"Codex-Writer","acceptance":["has a concise plan"]}]}' % name
        return _run

    args = SimpleNamespace(
        plan_phase1_providers="codex,claude",
        plan_phase1_rounds=3,
        plan_max_subtasks=3,
        orch_command_timeout_sec=120,
        plan_block_on_critic=True,
    )

    result = ensemble_mod.run_phase1_ensemble_planning(
        args=args,
        user_prompt="Prepare a stable execution plan",
        available_roles=["Codex-Writer", "Codex-Reviewer"],
        normalize_task_plan_payload=gw.normalize_task_plan_payload,
        parse_json_object_from_text=gw.parse_json_object_from_text,
        run_provider_execs={"codex": _runner("codex"), "claude": _runner("claude")},
        plan_roles_from_subtasks=gw.plan_roles_from_subtasks,
        report_progress=None,
    )

    assert result["phase1_mode"] == "ensemble"
    assert result["phase1_rounds"] == 3
    assert result["phase1_providers"] == ["codex", "claude"]
    assert len(result["plan_replans"]) == 3
    assert result["plan_data"]["subtasks"][0]["owner_role"] == "Codex-Writer"
    assert result["plan_gate_blocked"] is False
    assert any(item.startswith("codex:") for item in prompts)
    assert any(item.startswith("claude:") for item in prompts)


def test_phase1_ensemble_launches_round1_planners_in_parallel() -> None:
    start_times: dict[str, float] = {}
    lock = threading.Lock()

    def _runner(name: str):
        def _run(prompt: str, timeout_sec: int) -> str:
            line1 = prompt.splitlines()[0]
            if "TF Phase1 planner" in line1:
                with lock:
                    start_times.setdefault(name, time.monotonic())
                time.sleep(0.25)
                return '{"summary":"plan from %s","subtasks":[{"id":"S1","title":"Draft","goal":"Write the plan","owner_role":"Codex-Writer","acceptance":["has a concise plan"]}]}' % name
            time.sleep(0.25)
            return '{"approved": true, "issues": [], "recommendations": []}'
        return _run

    args = SimpleNamespace(
        plan_phase1_providers="codex,claude",
        plan_phase1_rounds=3,
        plan_max_subtasks=3,
        orch_command_timeout_sec=120,
        plan_block_on_critic=True,
    )

    ensemble_mod.run_phase1_ensemble_planning(
        args=args,
        user_prompt="Prepare a stable execution plan",
        available_roles=["Codex-Writer", "Codex-Reviewer"],
        normalize_task_plan_payload=gw.normalize_task_plan_payload,
        parse_json_object_from_text=gw.parse_json_object_from_text,
        run_provider_execs={"codex": _runner("codex"), "claude": _runner("claude")},
        plan_roles_from_subtasks=gw.plan_roles_from_subtasks,
        report_progress=None,
    )

    assert set(start_times) == {"codex", "claude"}
    assert abs(start_times["codex"] - start_times["claude"]) < 0.15


def test_phase1_ensemble_falls_back_to_codex_when_claude_is_rate_limited() -> None:
    calls: list[str] = []

    def _codex(prompt: str, timeout_sec: int) -> str:
        calls.append("codex")
        if "critic이다" in prompt:
            return '{"approved": true, "issues": [], "recommendations": []}'
        return '{"summary":"plan from codex","subtasks":[{"id":"S1","title":"Draft","goal":"Write the plan","owner_role":"Codex-Writer","acceptance":["has a concise plan"]}]}'

    def _claude(prompt: str, timeout_sec: int) -> str:
        calls.append("claude")
        raise RuntimeError("429 rate limit exceeded; retry after 60s")

    args = SimpleNamespace(
        plan_phase1_providers="codex,claude",
        plan_phase1_rounds=3,
        plan_max_subtasks=3,
        orch_command_timeout_sec=120,
        plan_block_on_critic=True,
    )

    result = ensemble_mod.run_phase1_ensemble_planning(
        args=args,
        user_prompt="Prepare a stable execution plan",
        available_roles=["Codex-Writer", "Codex-Reviewer"],
        normalize_task_plan_payload=gw.normalize_task_plan_payload,
        parse_json_object_from_text=gw.parse_json_object_from_text,
        run_provider_execs={"codex": _codex, "claude": _claude},
        plan_roles_from_subtasks=gw.plan_roles_from_subtasks,
        report_progress=None,
    )

    assert result["plan_gate_blocked"] is False
    assert result["plan_data"]["summary"] == "plan from codex"
    assert calls.count("claude") >= 1
    assert calls.count("codex") >= 2
    assert result["rate_limit"]["mode"] == "degraded"
    assert result["rate_limit"]["degraded_by"] == ["claude_rate_limit->codex"]


def test_phase1_ensemble_falls_back_to_claude_when_codex_is_rate_limited() -> None:
    calls: list[str] = []

    def _codex(prompt: str, timeout_sec: int) -> str:
        calls.append("codex")
        raise RuntimeError("429 rate limit exceeded; retry after 120s")

    def _claude(prompt: str, timeout_sec: int) -> str:
        calls.append("claude")
        if "critic이다" in prompt:
            return '{"approved": true, "issues": [], "recommendations": []}'
        return '{"summary":"plan from claude","subtasks":[{"id":"S1","title":"Draft","goal":"Write the plan","owner_role":"Codex-Writer","acceptance":["has a concise plan"]}]}'

    args = SimpleNamespace(
        plan_phase1_providers="codex,claude",
        plan_phase1_rounds=3,
        plan_max_subtasks=3,
        orch_command_timeout_sec=120,
        plan_block_on_critic=True,
    )

    result = ensemble_mod.run_phase1_ensemble_planning(
        args=args,
        user_prompt="Prepare a stable execution plan",
        available_roles=["Codex-Writer", "Codex-Reviewer"],
        normalize_task_plan_payload=gw.normalize_task_plan_payload,
        parse_json_object_from_text=gw.parse_json_object_from_text,
        run_provider_execs={"codex": _codex, "claude": _claude},
        plan_roles_from_subtasks=gw.plan_roles_from_subtasks,
        report_progress=None,
    )

    assert result["plan_gate_blocked"] is False
    assert result["plan_data"]["summary"] == "plan from claude"
    assert calls.count("codex") >= 1
    assert calls.count("claude") >= 2
    assert result["rate_limit"]["mode"] == "degraded"
    assert result["rate_limit"]["degraded_by"] == ["codex_rate_limit->claude"]


def test_phase1_ensemble_blocks_when_all_providers_are_rate_limited() -> None:
    def _limited(prompt: str, timeout_sec: int) -> str:
        raise RuntimeError("429 rate limit exceeded; retry after 180s")

    args = SimpleNamespace(
        plan_phase1_providers="codex,claude",
        plan_phase1_rounds=3,
        plan_max_subtasks=3,
        orch_command_timeout_sec=120,
        plan_block_on_critic=True,
    )

    result = ensemble_mod.run_phase1_ensemble_planning(
        args=args,
        user_prompt="Prepare a stable execution plan",
        available_roles=["Codex-Writer", "Codex-Reviewer"],
        normalize_task_plan_payload=gw.normalize_task_plan_payload,
        parse_json_object_from_text=gw.parse_json_object_from_text,
        run_provider_execs={"codex": _limited, "claude": _limited},
        plan_roles_from_subtasks=gw.plan_roles_from_subtasks,
        report_progress=None,
    )

    assert result["plan_gate_blocked"] is True
    assert result["plan_data"] is None
    assert result["rate_limit"]["mode"] == "blocked"
    assert sorted(result["rate_limit"]["limited_providers"]) == ["claude", "codex"]
    assert result["rate_limit"]["retry_after_sec"] == 180
    assert "retry_at" in result["rate_limit"]
    assert str(result["rate_limit"]["retry_at"]).strip()


def test_phase1_ensemble_uses_proactive_cooldown_fallback_from_provider_memory(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "provider_capacity.json").write_text(
        json.dumps(
            {
                "providers": {
                    "claude": {
                        "blocked_count": 1,
                        "project_count": 1,
                        "cooldown_level": "cooldown",
                        "next_retry_at": "2099-03-14T03:10:00+09:00",
                    }
                }
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    calls: list[str] = []

    def _codex(prompt: str, timeout_sec: int) -> str:
        calls.append("codex")
        if "critic이다" in prompt:
            return '{"approved": true, "issues": [], "recommendations": []}'
        return '{"summary":"plan from codex","subtasks":[{"id":"S1","title":"Draft","goal":"Write the plan","owner_role":"Codex-Writer","acceptance":["has a concise plan"]}]}'

    def _claude(prompt: str, timeout_sec: int) -> str:
        calls.append("claude")
        raise AssertionError("claude should not be called while cooldown memory is active")

    args = SimpleNamespace(
        team_dir=team_dir,
        plan_phase1_providers="claude,codex",
        plan_phase1_rounds=3,
        plan_max_subtasks=3,
        orch_command_timeout_sec=120,
        plan_block_on_critic=True,
    )

    result = ensemble_mod.run_phase1_ensemble_planning(
        args=args,
        user_prompt="Prepare a stable execution plan",
        available_roles=["Codex-Writer", "Codex-Reviewer"],
        normalize_task_plan_payload=gw.normalize_task_plan_payload,
        parse_json_object_from_text=gw.parse_json_object_from_text,
        run_provider_execs={"codex": _codex, "claude": _claude},
        plan_roles_from_subtasks=gw.plan_roles_from_subtasks,
        report_progress=None,
    )

    assert result["plan_gate_blocked"] is False
    assert result["plan_data"]["summary"] == "plan from codex"
    assert calls.count("claude") == 0
    assert calls.count("codex") >= 2
    assert result["rate_limit"]["degraded_by"] == ["claude_rate_limit->codex"]


def test_resolve_dispatch_mode_defaults_to_tf_dispatch_when_not_forced_direct() -> None:
    result = pipeline_mod.resolve_dispatch_mode_and_roles(
        run_force_mode=None,
        run_roles_override=None,
        project_roles_csv="",
        auto_dispatch_enabled=False,
        prompt="로그인 버그를 수정하고 회귀 리스크를 검토해줘",
        choose_auto_dispatch_roles=lambda *args, **kwargs: [],
        available_roles=["Codex-Dev", "Codex-Reviewer"],
        team_dir=None,
    )

    assert result.dispatch_mode is True
    assert result.dispatch_roles == "Codex-Reviewer"


def test_compute_dispatch_plan_uses_phase1_plan_roles_for_phase2_execution() -> None:
    args = SimpleNamespace(
        task_planning=True,
        dry_run=False,
        plan_phase1_ensemble=True,
        plan_phase1_rounds=3,
        plan_phase1_providers="codex,claude",
        plan_phase1_min_providers=2,
        plan_max_subtasks=4,
        plan_auto_replan=True,
        plan_replan_attempts=1,
        plan_block_on_critic=True,
    )
    phases: list[dict] = []

    meta = pipeline_mod.compute_dispatch_plan(
        args=args,
        p_args=args,
        prompt="계획 수립 후 병렬 실행팀을 꾸려라",
        dispatch_mode=True,
        run_control_mode="normal",
        run_source_task=None,
        selected_roles=["Codex-Reviewer"],
        available_roles=["Codex-Dev", "Codex-Writer", "Codex-Reviewer"],
        available_worker_roles=lambda roles: roles,
        normalize_task_plan_payload=lambda parsed, **_kwargs: parsed,
        build_task_execution_plan=lambda *_args, **_kwargs: {},
        critique_task_execution_plan=lambda *_args, **_kwargs: {"approved": True, "issues": [], "recommendations": []},
        critic_has_blockers=lambda critic: (not bool(critic.get("approved", True))) or bool(critic.get("issues") or []),
        repair_task_execution_plan=lambda *_args, **_kwargs: {},
        plan_roles_from_subtasks=lambda plan: [row["owner_role"] for row in (plan.get("subtasks") or [])] if isinstance(plan, dict) else [],
        phase1_ensemble_planning=lambda *_args, **kwargs: {
            "plan_data": {
                "summary": "phase1 plan",
                "subtasks": [
                    {"id": "S1", "title": "implement", "goal": "do implementation", "owner_role": "Codex-Dev", "acceptance": ["code complete"]},
                    {"id": "S2", "title": "write", "goal": "document plan", "owner_role": "Codex-Writer", "acceptance": ["report complete"]},
                ],
            },
            "plan_critic": {"approved": True, "issues": [], "recommendations": []},
            "plan_roles": ["Codex-Dev", "Codex-Writer"],
            "plan_replans": [{"attempt": 1}, {"attempt": 2}, {"attempt": 3}],
            "plan_error": "",
            "plan_gate_blocked": False,
            "plan_gate_reason": "",
            "phase1_mode": "ensemble",
            "phase1_rounds": 3,
            "phase1_providers": ["codex", "claude"],
        },
        report_progress=lambda **kwargs: phases.append(kwargs),
    )

    assert meta.selected_roles == ["Codex-Dev", "Codex-Writer"]
    assert meta.phase1_mode == "ensemble"
    assert meta.phase1_rounds == 3
    assert meta.phase1_providers == ["codex", "claude"]
    assert phases[-1]["phase"] == "ready"


def test_normalize_task_plan_payload_derives_phase2_team_spec() -> None:
    plan = gw.normalize_task_plan_payload(
        {
            "summary": "parallel execution",
            "subtasks": [
                {"id": "S1", "title": "Implement", "goal": "build feature", "owner_role": "Codex-Dev", "acceptance": ["done"]},
                {"id": "S2", "title": "Document", "goal": "write handoff", "owner_role": "Codex-Writer", "acceptance": ["done"]},
            ],
        },
        user_prompt="계획 수립 후 병렬 실행팀을 꾸려라",
        workers=["Codex-Dev", "Codex-Writer", "Codex-Reviewer"],
        max_subtasks=4,
    )

    spec = plan["meta"]["phase2_team_spec"]
    execution_plan = plan["meta"]["phase2_execution_plan"]
    assert spec["execution_mode"] == "parallel"
    assert [row["role"] for row in spec["execution_groups"]] == ["Codex-Dev", "Codex-Writer"]
    assert [row["role"] for row in spec["review_groups"]] == ["Codex-Reviewer"]
    assert execution_plan["execution_mode"] == "parallel"
    assert [row["role"] for row in execution_plan["execution_lanes"]] == ["Codex-Dev", "Codex-Writer"]
    assert [row["role"] for row in execution_plan["review_lanes"]] == ["Codex-Reviewer"]
    assert execution_plan["parallel_workers"] is True
    assert execution_plan["readonly"] is True


def test_build_planned_dispatch_prompt_includes_phase2_team_lanes() -> None:
    plan = gw.normalize_task_plan_payload(
        {
            "summary": "parallel execution",
            "subtasks": [
                {"id": "S1", "title": "Implement", "goal": "build feature", "owner_role": "Codex-Dev", "acceptance": ["done"]},
                {"id": "S2", "title": "Document", "goal": "write handoff", "owner_role": "Codex-Writer", "acceptance": ["done"]},
            ],
        },
        user_prompt="계획 수립 후 병렬 실행팀을 꾸려라",
        workers=["Codex-Dev", "Codex-Writer", "Codex-Reviewer"],
        max_subtasks=4,
    )
    plan = gw.attach_phase2_team_spec(
        plan,
        roles=["Codex-Dev", "Codex-Writer", "Codex-Reviewer"],
        verifier_roles=["Codex-Reviewer"],
        require_verifier=True,
    )

    prompt = gw.build_planned_dispatch_prompt(
        "원 요청",
        plan,
        {"approved": True, "issues": [], "recommendations": []},
    )

    assert "Phase2 execution lanes: parallel" in prompt
    assert "lane E1 [Codex-Dev] -> S1" in prompt
    assert "Phase2 critic lanes: single" in prompt
    assert "review R1 [Codex-Reviewer/verifier]" in prompt


def test_normalize_task_plan_payload_with_companion_workers_derives_parallel_claude_lanes() -> None:
    plan = gw.normalize_task_plan_payload(
        {
            "summary": "parallel reporting",
            "subtasks": [
                {"id": "S1", "title": "Document", "goal": "write handoff", "owner_role": "Codex-Writer", "acceptance": ["done"]},
                {"id": "S2", "title": "Analyze", "goal": "compare options", "owner_role": "Codex-Analyst", "acceptance": ["done"]},
            ],
        },
        user_prompt="계획 수립 후 병렬 실행팀을 꾸려라",
        workers=["Codex-Writer", "Claude-Writer", "Codex-Analyst", "Claude-Analyst", "Codex-Reviewer", "Claude-Reviewer"],
        max_subtasks=4,
    )
    plan = gw.attach_phase2_team_spec(
        plan,
        roles=["Codex-Writer", "Claude-Writer", "Codex-Analyst", "Claude-Analyst", "Codex-Reviewer", "Claude-Reviewer"],
        verifier_roles=["Codex-Reviewer"],
        require_verifier=True,
    )

    spec = plan["meta"]["phase2_team_spec"]
    execution_plan = plan["meta"]["phase2_execution_plan"]
    assert [row["role"] for row in spec["execution_groups"]] == [
        "Codex-Writer",
        "Claude-Writer",
        "Codex-Analyst",
        "Claude-Analyst",
    ]
    assert [row["role"] for row in spec["review_groups"]] == ["Codex-Reviewer", "Claude-Reviewer"]
    assert [row["role"] for row in execution_plan["execution_lanes"]] == [
        "Codex-Writer",
        "Claude-Writer",
        "Codex-Analyst",
        "Claude-Analyst",
    ]
    assert [row["role"] for row in execution_plan["review_lanes"]] == ["Codex-Reviewer", "Claude-Reviewer"]
