#!/usr/bin/env python3
"""Gateway backend and integration regression tests."""

from _gateway_test_support import *  # noqa: F401,F403
def test_tf_backend_normalization_and_labels() -> None:
    assert tf_backend.normalize_tf_backend_name("") == "local"
    assert tf_backend.normalize_tf_backend_name("default") == "local"
    assert tf_backend.normalize_tf_backend_name("autogen") == "autogen_core"
    assert tf_backend.normalize_tf_backend_name("autogen-core") == "autogen_core"
    assert tf_backend.backend_runtime_label("aoe") == "local"
    assert tf_backend.backend_runtime_label("autogen_core") == "autogen_core"


def test_tf_backend_selection_defaults_to_local_and_enforces_sandbox_guard(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True)

    default_row = tf_backend_selection.resolve_effective_tf_backend(team_dir)
    assert default_row["effective_backend"] == "local"
    assert default_row["selection_reason"] == "default_local"

    (team_dir / "tf_backend.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "backend": "autogen_core",
                "profile": "research",
                "sandbox_only": True,
            }
        ),
        encoding="utf-8",
    )
    guarded_row = tf_backend_selection.resolve_effective_tf_backend(team_dir)
    assert guarded_row["backend"] == "autogen_core"
    assert guarded_row["effective_backend"] == "local"
    assert guarded_row["selection_reason"] == "sandbox_guard"

    (team_dir / "tf_backend.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "backend": "autogen_core",
                "profile": "sandbox",
                "sandbox_only": True,
            }
        ),
        encoding="utf-8",
    )
    sandbox_row = tf_backend_selection.resolve_effective_tf_backend(team_dir)
    assert sandbox_row["effective_backend"] == "autogen_core"
    assert sandbox_row["selection_reason"] == "sandbox_config"


def test_tf_runtime_event_schema_normalizes_and_validates() -> None:
    rows = tf_event_schema.normalize_runtime_events(
        [
            {
                "stage": "request.accepted",
                "kind": "lifecycle",
                "status": "info",
                "summary": "accepted request",
                "payload": {"project_key": "O3"},
            },
            {
                "source": "reviewer",
                "stage": "verdict.emitted",
                "kind": "verdict",
                "status": "success",
                "summary": "review verdict emitted",
                "payload": {"verdict": "success"},
            },
        ],
        default_backend="local",
        default_source="gateway",
        now_iso=lambda: "2026-03-11T00:00:00+0000",
    )

    assert rows[0]["backend"] == "local"
    assert rows[0]["source"] == "gateway"
    assert rows[0]["seq"] == 1
    assert rows[1]["seq"] == 2
    assert tf_event_schema.tf_runtime_event_schema()["required_fields"] == list(tf_event_schema.RUNTIME_EVENT_REQUIRED_FIELDS)
    assert tf_event_schema.validate_runtime_events(rows) == [[], []]


def test_autogen_compare_includes_runtime_event_contract() -> None:
    case = {
        "id": "demo_case",
        "project_key": "O3",
        "task": "Summarize latest analysis findings and propose next steps",
        "roles": ["Codex-Analyst", "Codex-Reviewer"],
        "retry_budget": 3,
        "approval_required": False,
    }

    result = autogen_compare.run_case(case)
    comparison = result["comparison"]
    summary = autogen_compare.build_summary([result])

    assert comparison["output_contract_match"]["runtime_event_schema"] is True
    assert comparison["runtime_event_contract"]["local_valid"] is True
    assert comparison["runtime_event_contract"]["autogen_valid"] is True
    assert comparison["runtime_event_contract"]["local_event_count"] > 0
    assert comparison["runtime_event_contract"]["autogen_event_count"] > 0
    assert comparison["proposal_contract"]["local_valid"] is True
    assert comparison["proposal_contract"]["autogen_valid"] is True
    assert comparison["proposal_contract"]["exact_payload_match"] is True
    assert summary["runtime_event_contract_cases"] == 1
    assert summary["proposal_contract_cases"] == 1


def test_local_tf_backend_delegates_to_run_aoe_orch() -> None:
    calls: dict = {}

    def fake_now_iso() -> str:
        return "2026-03-11T00:00:00+0000"

    def fake_run_command(*args, **kwargs):
        raise AssertionError("run_command should not be called directly in this wrapper test")

    def fake_run_aoe_orch(args, prompt, chat_id, **kwargs):
        calls["args"] = args
        calls["prompt"] = prompt
        calls["chat_id"] = chat_id
        calls["kwargs"] = kwargs
        return {"request_id": "REQ-1", "status": "submitted"}

    original = tf_backend_local.run_aoe_orch
    tf_backend_local.run_aoe_orch = fake_run_aoe_orch
    try:
        request = tf_backend.build_tf_backend_request(
            args=argparse.Namespace(project_root=str(ROOT), team_dir=str(ROOT / ".aoe-team")),
            prompt="review this change",
            chat_id="chat-1",
            roles_override="Codex-Reviewer",
            priority_override="P1",
            timeout_override=30,
            no_wait_override=True,
            metadata={"phase2_execution_plan": {"execution_mode": "single", "execution_lanes": []}},
        )
        deps = tf_backend.build_tf_backend_deps(
            default_tf_exec_mode="local",
            default_tf_work_root_name=".aoe-tf",
            default_tf_exec_map_file="tf_exec_map.json",
            default_tf_worker_startup_grace_sec=45,
            now_iso=fake_now_iso,
            run_command=fake_run_command,
        )
        result = tf_backend_local.local_backend().run(request, deps)
    finally:
        tf_backend_local.run_aoe_orch = original

    assert result["request_id"] == "REQ-1"
    assert calls["prompt"] == "review this change"
    assert calls["chat_id"] == "chat-1"
    assert calls["kwargs"]["roles_override"] == "Codex-Reviewer"
    assert calls["kwargs"]["priority_override"] == "P1"
    assert calls["kwargs"]["timeout_override"] == 30
    assert calls["kwargs"]["no_wait_override"] is True
    assert calls["kwargs"]["metadata"]["phase2_execution_plan"]["execution_mode"] == "single"


def test_autogen_backend_reports_availability_and_stays_not_implemented(tmp_path: Path) -> None:
    availability = tf_backend_autogen.autogen_core_backend().availability()
    assert isinstance(availability.available, bool)
    if availability.available:
        project_root = tmp_path / "autogen_backend_test"
        team_dir = project_root / ".aoe-team"
        team_dir.mkdir(parents=True, exist_ok=True)
        (project_root / "TODO.md").write_text(
            "\n".join(
                [
                    "# Test TODO",
                    "",
                    "## Tasks",
                    "",
                    "- [ ] P1: Validate the AutoGen sandbox backend against a canonical TODO source.",
                    "- [ ] P2: Summarize the extracted backlog items for operator review.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        result = tf_backend_autogen.autogen_core_backend().run(
            tf_backend.build_tf_backend_request(
                args=argparse.Namespace(
                    project_root=str(project_root),
                    team_dir=str(team_dir),
                    _aoe_project_key="O2",
                ),
                prompt="Summarize the canonical backlog and confirm the first next step.",
                chat_id="chat-1",
                roles_override="Codex-Analyst,Codex-Reviewer",
            ),
            tf_backend.build_tf_backend_deps(
                default_tf_exec_mode="local",
                default_tf_work_root_name=".aoe-tf",
                default_tf_exec_map_file="tf_exec_map.json",
                default_tf_worker_startup_grace_sec=45,
                now_iso=lambda: "2026-03-11T00:00:00+0000",
                run_command=lambda *args, **kwargs: None,
            ),
        )
        assert result["status"] == "completed"
        assert result["complete"] is True
        assert result["verdict"] in {"success", "fail"}
        assert len(result["replies"]) == 2
        assert result["counts"]["assignments"] == 2
        assert result["counts"]["replies"] == 2
        assert result["followup_proposals"]
        assert all(not errs for errs in tf_event_schema.validate_runtime_events(result["runtime_events"]))
        assert all(not errs for errs in tf_event_schema.validate_followup_proposals(result["followup_proposals"]))
        assert result["replies"][0]["role"] == "Codex-Analyst"
        assert result["replies"][1]["role"] == "Codex-Reviewer"
    else:
        assert "not installed" in availability.reason


def test_gateway_run_aoe_orch_executes_real_autogen_backend_when_available(tmp_path: Path) -> None:
    if not tf_backend_autogen.autogen_core_backend().availability().available:
        return
    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    root_team_dir = tmp_path / "mother" / ".aoe-team"
    team_dir.mkdir(parents=True)
    root_team_dir.mkdir(parents=True)
    (project_root / "TODO.md").write_text(
        "\n".join(
            [
                "# Pilot TODO",
                "",
                "## Tasks",
                "",
                "- [ ] P1: Produce a read-only backlog summary for the sandbox pilot.",
                "- [ ] P2: Confirm the next review focus without changing files.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (team_dir / "tf_backend.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "backend": "autogen_core",
                "profile": "sandbox",
                "sandbox_only": True,
            }
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        aoe_orch_bin="aoe-orch",
        _aoe_root_team_dir=str(root_team_dir),
        _aoe_project_key="O2",
        _aoe_trace_id="trace-autogen-real",
    )

    result = gw.run_aoe_orch(
        args,
        "Summarize the canonical Twin backlog and identify the first next focus.",
        "chat-1",
        roles_override="Codex-Analyst,Codex-Reviewer",
    )

    assert result["backend"] == "autogen_core"
    assert result["backend_profile"] == "sandbox"
    assert result["complete"] is True
    assert result["counts"]["replies"] == 2
    assert result["followup_proposals"]
    assert all(not errs for errs in tf_event_schema.validate_runtime_events(result["runtime_events"]))

    project_rows = [
        json.loads(line)
        for line in (team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    root_rows = [
        json.loads(line)
        for line in (root_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(row["event"] == "tf_backend_runtime_event" and row["backend"] == "autogen_core" for row in project_rows)
    assert any(row.get("project_team_dir") == str(team_dir.resolve()) for row in root_rows)


def test_gateway_run_aoe_orch_executes_writer_shape_with_real_autogen_backend_when_available(tmp_path: Path) -> None:
    if not tf_backend_autogen.autogen_core_backend().availability().available:
        return
    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    root_team_dir = tmp_path / "mother" / ".aoe-team"
    team_dir.mkdir(parents=True)
    root_team_dir.mkdir(parents=True)
    (project_root / "TODO.md").write_text(
        "\n".join(
            [
                "# Pilot TODO",
                "",
                "## Tasks",
                "",
                "- [ ] P1: Draft an operator-facing handoff from the canonical backlog.",
                "- [ ] P2: Highlight the first item that still needs explicit human review.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (team_dir / "tf_backend.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "backend": "autogen_core",
                "profile": "sandbox",
                "sandbox_only": True,
            }
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        aoe_orch_bin="aoe-orch",
        _aoe_root_team_dir=str(root_team_dir),
        _aoe_project_key="O4",
        _aoe_trace_id="trace-autogen-writer",
    )

    result = gw.run_aoe_orch(
        args,
        "Draft a short operator-facing handoff report from the canonical backlog without modifying files.",
        "chat-1",
        roles_override="Codex-Writer,Codex-Reviewer",
    )

    assert result["backend"] == "autogen_core"
    assert result["complete"] is True
    assert result["replies"][0]["role"] == "Codex-Writer"
    assert "Codex-Writer handoff" in result["replies"][0]["body"]
    assert "Codex-Writer, Codex-Reviewer" in result["replies"][1]["body"]
    assert result["followup_proposals"]
    assert result["followup_proposals"][0]["kind"] == "handoff"


@pytest.mark.smoke
def test_handle_text_message_operator_triggered_sandbox_run_merges_backend_native_proposals(
    tmp_path: Path, monkeypatch
) -> None:
    if not tf_backend_autogen.autogen_core_backend().availability().available:
        return

    root_root = tmp_path / "mother"
    root_team_dir = root_root / ".aoe-team"
    project_root = tmp_path / "local_map_analysis"
    team_dir = project_root / ".aoe-team"
    root_team_dir.mkdir(parents=True)
    team_dir.mkdir(parents=True)

    (project_root / "TODO.md").write_text(
        "\n".join(
            [
                "# Local Map Analysis TODO",
                "",
                "## Tasks",
                "",
                "- [ ] P1: Build the strict as-of time-generalization readout memo across completed fields and origin years.",
                "- [ ] P1: Produce the machine-readable summary table with required robustness metrics and deltas.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (team_dir / "tf_backend.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "backend": "autogen_core",
                "profile": "sandbox",
                "sandbox_only": True,
            }
        ),
        encoding="utf-8",
    )

    manager_state = gw.default_manager_state(root_root, root_team_dir)
    manager_state["projects"]["local_map_analysis"] = {
        "name": "local_map_analysis",
        "display_name": "Local Map Analysis",
        "project_alias": "O4",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "overview": "",
        "todos": [],
        "todo_seq": 0,
        "todo_proposals": [],
        "todo_proposal_seq": 0,
        "tasks": {},
        "task_aliases": {},
        "task_alias_seq": 0,
        "last_request_id": "",
        "created_at": "2026-03-11T20:00:00+0900",
        "updated_at": "2026-03-11T20:00:00+0900",
    }
    manager_state["active"] = "local_map_analysis"
    manager_state_file = root_team_dir / "orch_manager_state.json"
    manager_state_file.write_text(json.dumps(manager_state, ensure_ascii=False, indent=2), encoding="utf-8")

    sent: list[dict] = []
    monkeypatch.setattr(gw, "safe_tg_send_text", lambda **kwargs: sent.append(kwargs) or True)
    monkeypatch.setattr(gw, "room_autopublish_event", lambda **_kwargs: None)

    parser = gw.build_parser()
    args = parser.parse_args(
        [
            "--project-root",
            str(root_root),
            "--team-dir",
            str(root_team_dir),
            "--manager-state-file",
            str(manager_state_file),
            "--allow-chat-ids",
            "operator-test",
            "--admin-chat-ids",
            "operator-test",
            "--no-task-planning",
            "--no-exec-critic",
            "--default-report-level",
            "short",
        ]
    )
    args = cli_mod.normalize_main_args(args, deps=gw.__dict__)

    gw.handle_text_message(
        args,
        token="",
        chat_id="operator-test",
        text="/dispatch Draft a short operator-facing handoff report from the canonical backlog without modifying files.",
        trace_id="trace-operator-sandbox",
    )

    final_state = json.loads(manager_state_file.read_text(encoding="utf-8"))
    entry = final_state["projects"]["local_map_analysis"]
    proposals = entry.get("todo_proposals") or []

    assert len(proposals) == 2
    assert proposals[0]["status"] == "open"
    assert proposals[0]["source_request_id"]
    assert proposals[0]["created_by"] == "tf"
    assert any(row.get("context") == "todo-proposals-alert" for row in sent)
    assert any(row.get("context") == "result" for row in sent)

    project_rows = [
        json.loads(line)
        for line in (team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    root_rows = [
        json.loads(line)
        for line in (root_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert any(row["event"] == "tf_backend_runtime_event" and row["backend"] == "autogen_core" for row in project_rows)
    assert any(row["event"] == "todo_proposals_created" for row in project_rows)
    assert any(row.get("project_team_dir") == str(team_dir.resolve()) for row in root_rows)


@pytest.mark.smoke
def test_handle_text_message_operator_triggered_sandbox_run_exposes_todo_proposals_inbox(
    tmp_path: Path, monkeypatch
) -> None:
    if not tf_backend_autogen.autogen_core_backend().availability().available:
        return

    root_root = tmp_path / "mother"
    root_team_dir = root_root / ".aoe-team"
    project_root = tmp_path / "local_map_analysis"
    team_dir = project_root / ".aoe-team"
    root_team_dir.mkdir(parents=True)
    team_dir.mkdir(parents=True)

    (project_root / "TODO.md").write_text(
        "\n".join(
            [
                "# Local Map Analysis TODO",
                "",
                "## Tasks",
                "",
                "- [ ] P1: Build the strict as-of time-generalization readout memo across completed fields and origin years.",
                "- [ ] P1: Produce the machine-readable summary table with required robustness metrics and deltas.",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (team_dir / "tf_backend.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "backend": "autogen_core",
                "profile": "sandbox",
                "sandbox_only": True,
            }
        ),
        encoding="utf-8",
    )

    manager_state = gw.default_manager_state(root_root, root_team_dir)
    manager_state["projects"]["local_map_analysis"] = {
        "name": "local_map_analysis",
        "display_name": "Local Map Analysis",
        "project_alias": "O4",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "overview": "",
        "todos": [],
        "todo_seq": 0,
        "todo_proposals": [],
        "todo_proposal_seq": 0,
        "tasks": {},
        "task_aliases": {},
        "task_alias_seq": 0,
        "last_request_id": "",
        "created_at": "2026-03-11T20:00:00+0900",
        "updated_at": "2026-03-11T20:00:00+0900",
    }
    manager_state["active"] = "local_map_analysis"
    manager_state_file = root_team_dir / "orch_manager_state.json"
    manager_state_file.write_text(json.dumps(manager_state, ensure_ascii=False, indent=2), encoding="utf-8")

    sent: list[dict] = []
    monkeypatch.setattr(gw, "safe_tg_send_text", lambda **kwargs: sent.append(kwargs) or True)
    monkeypatch.setattr(gw, "room_autopublish_event", lambda **_kwargs: None)

    parser = gw.build_parser()
    args = parser.parse_args(
        [
            "--project-root",
            str(root_root),
            "--team-dir",
            str(root_team_dir),
            "--manager-state-file",
            str(manager_state_file),
            "--allow-chat-ids",
            "operator-test",
            "--admin-chat-ids",
            "operator-test",
            "--no-task-planning",
            "--no-exec-critic",
            "--default-report-level",
            "short",
        ]
    )
    args = cli_mod.normalize_main_args(args, deps=gw.__dict__)

    gw.handle_text_message(
        args,
        token="",
        chat_id="operator-test",
        text="/dispatch Draft a short operator-facing handoff report from the canonical backlog without modifying files.",
        trace_id="trace-operator-sandbox-inbox",
    )
    sent.clear()

    gw.handle_text_message(
        args,
        token="",
        chat_id="operator-test",
        text="/todo proposals",
        trace_id="trace-operator-proposals-list",
    )

    assert sent
    proposals_msgs = [row for row in sent if row.get("context") == "todo-proposals"]
    assert proposals_msgs
    body = proposals_msgs[-1]["text"]
    assert "todo proposals: open=2" in body
    assert "PROP-001" in body
    assert "PROP-002" in body
    assert "strict as-of time-generalization readout" in body
    assert "machine-readable summary table" in body


def test_gateway_run_aoe_orch_uses_local_backend_by_default(tmp_path: Path) -> None:
    team_dir = tmp_path / "project" / ".aoe-team"
    team_dir.mkdir(parents=True)
    args = argparse.Namespace(
        project_root=tmp_path / "project",
        team_dir=team_dir,
        aoe_orch_bin="aoe-orch",
        _aoe_root_team_dir=str(tmp_path / "mother" / ".aoe-team"),
        _aoe_project_key="o3",
        _aoe_trace_id="trace-local",
    )
    calls: dict = {}

    class _FakeLocalBackend:
        backend_name = "local"

        def availability(self):
            return tf_backend.TFBackendAvailability(True, "")

        def run(self, request, deps):
            calls["request"] = request
            calls["deps"] = deps
            return {"request_id": "REQ-LOCAL", "status": "submitted"}

    original_local_backend = gw.tf_backend_local_mod.local_backend
    original_autogen_backend = gw.tf_backend_autogen_mod.autogen_core_backend
    gw.tf_backend_local_mod.local_backend = lambda: _FakeLocalBackend()
    gw.tf_backend_autogen_mod.autogen_core_backend = lambda: (_ for _ in ()).throw(AssertionError("autogen backend should not be selected"))
    try:
        result = gw.run_aoe_orch(
            args,
            "review this",
            "chat-1",
            roles_override="Codex-Reviewer",
            metadata={"phase2_execution_plan": {"execution_mode": "single", "execution_lanes": [{"lane_id": "L1", "role": "Codex-Reviewer"}]}},
        )
    finally:
        gw.tf_backend_local_mod.local_backend = original_local_backend
        gw.tf_backend_autogen_mod.autogen_core_backend = original_autogen_backend

    assert result["backend"] == "local"
    assert result["backend_selection_reason"] == "default_local"
    assert calls["request"].prompt == "review this"
    assert calls["request"].roles_override == "Codex-Reviewer"
    assert calls["request"].metadata["phase2_execution_plan"]["execution_mode"] == "single"
    assert calls["deps"].default_tf_exec_map_file == gw.DEFAULT_TF_EXEC_MAP_FILE


def test_run_claude_exec_full_mode_uses_add_dir_and_bypass_permissions(tmp_path: Path, monkeypatch) -> None:
    calls: list[list[str]] = []

    class Proc:
        def __init__(self, code: int = 0, stdout: str = "ok", stderr: str = "") -> None:
            self.returncode = code
            self.stdout = stdout
            self.stderr = stderr

    def _fake_run(cmd, *args, **kwargs):
        row = list(cmd)
        calls.append(row)
        if row[:3] == ["sudo", "-n", "true"]:
            return Proc(code=1)
        return Proc()

    monkeypatch.setattr(gw.subprocess, "run", _fake_run)
    monkeypatch.setenv("AOE_CLAUDE_PERMISSION_MODE", "full")
    args = argparse.Namespace(project_root=tmp_path)

    body = gw.run_claude_exec(args, "hello", timeout_sec=33)

    assert body == "ok"
    cmd = calls[-1]
    assert cmd[0] == "claude"
    assert "--add-dir" in cmd
    assert str(tmp_path) in cmd
    assert "--dangerously-skip-permissions" in cmd
    idx = cmd.index("--permission-mode")
    assert cmd[idx + 1] == "bypassPermissions"
    assert "--tools" not in cmd


def test_run_claude_exec_readonly_maps_to_plan_mode(tmp_path: Path, monkeypatch) -> None:
    calls: list[list[str]] = []

    class Proc:
        def __init__(self, code: int = 0, stdout: str = "ok", stderr: str = "") -> None:
            self.returncode = code
            self.stdout = stdout
            self.stderr = stderr

    def _fake_run(cmd, *args, **kwargs):
        row = list(cmd)
        calls.append(row)
        if row[:3] == ["sudo", "-n", "true"]:
            return Proc(code=1)
        return Proc()

    monkeypatch.setattr(gw.subprocess, "run", _fake_run)
    monkeypatch.setenv("AOE_CLAUDE_PERMISSION_MODE", "read-only")
    args = argparse.Namespace(project_root=tmp_path)

    body = gw.run_claude_exec(args, "hello", timeout_sec=21)

    assert body == "ok"
    cmd = calls[-1]
    idx = cmd.index("--permission-mode")
    assert cmd[idx + 1] == "plan"
    assert "--dangerously-skip-permissions" not in cmd


def test_run_claude_exec_can_wrap_with_sudo_when_root_mode_enabled(tmp_path: Path, monkeypatch) -> None:
    calls: list[list[str]] = []

    class Proc:
        def __init__(self, code: int = 0, stdout: str = "ok", stderr: str = "") -> None:
            self.returncode = code
            self.stdout = stdout
            self.stderr = stderr

    def _fake_run(cmd, *args, **kwargs):
        row = list(cmd)
        calls.append(row)
        if row[:3] == ["sudo", "-n", "true"]:
            return Proc(code=0)
        return Proc()

    monkeypatch.setattr(gw.subprocess, "run", _fake_run)
    monkeypatch.setenv("AOE_CLAUDE_PERMISSION_MODE", "full")
    monkeypatch.setenv("AOE_CLAUDE_RUN_AS_ROOT", "1")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "demo-key")
    args = argparse.Namespace(project_root=tmp_path)

    body = gw.run_claude_exec(args, "hello", timeout_sec=21)

    assert body == "ok"
    assert calls[0][:3] == ["sudo", "-n", "true"]
    wrapped = calls[-1]
    assert wrapped[:3] == ["sudo", "-n", "env"]
    assert "ANTHROPIC_API_KEY=demo-key" in wrapped
    assert "claude" in wrapped


def test_local_run_aoe_orch_stages_review_lanes_after_execution(monkeypatch, tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True)
    monkeypatch.setenv("AOE_TF_EXEC_MODE", "inplace")
    request_ids = iter(["REQ-EXEC", "REQ-REVIEW"])
    monkeypatch.setattr(tf_exec, "create_request_id", lambda: next(request_ids))

    recorded_roles: list[tuple[str, str]] = []

    class Proc:
        def __init__(self, returncode=0, stdout="", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run_command(cmd, env=None, timeout_sec=0):
        if cmd[:3] == ["git", "-C", str(project_root)]:
            return Proc(returncode=1, stdout="", stderr="not git")
        if cmd[:2] == ["tmux", "has-session"]:
            return Proc(returncode=1, stdout="", stderr="")
        if cmd[:2] == ["tmux", "new-session"]:
            return Proc(returncode=0, stdout="", stderr="")
        if cmd[:2] == ["tmux", "kill-session"]:
            return Proc(returncode=0, stdout="", stderr="")
        if cmd[:2] == ["aoe-orch", "run"] and "--dry-run" in cmd:
            roles = cmd[cmd.index("--roles") + 1] if "--roles" in cmd else ""
            dispatch_roles = [token.strip() for token in roles.split(",") if token.strip()]
            payload = {"request_id": cmd[cmd.index("--request-id") + 1], "dispatch_plan": [{"role": role} for role in dispatch_roles]}
            return Proc(returncode=0, stdout=json.dumps(payload), stderr="")
        if cmd[:2] == ["aoe-orch", "run"]:
            roles = cmd[cmd.index("--roles") + 1] if "--roles" in cmd else ""
            req_id = cmd[cmd.index("--request-id") + 1]
            recorded_roles.append((req_id, roles))
            if roles == "Codex-Dev":
                payload = {
                    "request_id": req_id,
                    "complete": True,
                    "counts": {"assignments": 1, "replies": 1},
                    "roles": [{"role": "Codex-Dev", "status": "done", "message_id": "MSG-EXEC"}],
                    "done_roles": ["Codex-Dev"],
                    "failed_roles": [],
                    "pending_roles": [],
                    "replies": [{"role": "Codex-Dev", "body": "execution done"}],
                    "reply_messages": [{"id": "REP-EXEC", "from": "Codex-Dev", "status": "sent"}],
                }
                return Proc(returncode=0, stdout=json.dumps(payload), stderr="")
            if roles == "Codex-Reviewer":
                payload = {
                    "request_id": req_id,
                    "complete": True,
                    "counts": {"assignments": 1, "replies": 1},
                    "roles": [{"role": "Codex-Reviewer", "status": "done", "message_id": "MSG-REVIEW"}],
                    "done_roles": ["Codex-Reviewer"],
                    "failed_roles": [],
                    "pending_roles": [],
                    "replies": [{"role": "Codex-Reviewer", "body": "review done"}],
                    "reply_messages": [{"id": "REP-REVIEW", "from": "Codex-Reviewer", "status": "sent"}],
                }
                return Proc(returncode=0, stdout=json.dumps(payload), stderr="")
            raise AssertionError(f"unexpected roles {roles}")
        raise AssertionError(f"unexpected command: {cmd}")

    args = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        aoe_orch_bin="aoe-orch",
        orch_poll_sec=2.0,
        orch_timeout_sec=180,
        orch_command_timeout_sec=180,
        no_wait=False,
        roles="",
        priority="P2",
    )

    result = tf_exec.run_aoe_orch(
        args,
        "Implement and then review",
        "chat-1",
        default_tf_exec_mode=gw.DEFAULT_TF_EXEC_MODE,
        default_tf_work_root_name=gw.DEFAULT_TF_WORK_ROOT_NAME,
        default_tf_exec_map_file=gw.DEFAULT_TF_EXEC_MAP_FILE,
        default_tf_worker_startup_grace_sec=gw.DEFAULT_TF_WORKER_STARTUP_GRACE_SEC,
        now_iso=gw.now_iso,
        run_command=fake_run_command,
        roles_override="Codex-Dev,Codex-Reviewer",
        metadata={
            "phase2_execution_plan": {
                "execution_mode": "single",
                "execution_lanes": [{"lane_id": "L1", "role": "Codex-Dev", "subtask_ids": ["S1"], "parallel": False}],
                "review_mode": "single",
                "review_lanes": [{"lane_id": "R1", "role": "Codex-Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": False}],
                "parallel_workers": False,
                "parallel_reviews": False,
                "readonly": True,
            }
        },
    )

    assert recorded_roles == [("REQ-EXEC", "Codex-Dev"), ("REQ-REVIEW", "Codex-Reviewer")]
    assert result["phase2_review_triggered"] is True
    assert result["phase2_request_ids"] == {"execution": "REQ-EXEC", "review": "REQ-REVIEW"}
    assert result["linked_request_ids"] == ["REQ-EXEC", "REQ-REVIEW"]
    assert len(result["replies"]) == 2
    assert {row["role"] for row in result["role_states"]} == {"Codex-Dev", "Codex-Reviewer"}
    assert result["tf_workers"]["execution"]["sessions"][0]["execution_lane_ids"] == ["L1"]
    assert result["tf_workers"]["review"]["sessions"][0]["review_lane_ids"] == ["R1"]


def test_local_run_aoe_orch_fanouts_parallel_execution_and_review_lanes(monkeypatch, tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True)
    monkeypatch.setenv("AOE_TF_EXEC_MODE", "inplace")
    request_ids = iter(["REQ-REVIEW-BASE"])
    monkeypatch.setattr(tf_exec, "create_request_id", lambda: next(request_ids))

    recorded_roles: list[tuple[str, str]] = []

    class Proc:
        def __init__(self, returncode=0, stdout="", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run_command(cmd, env=None, timeout_sec=0):
        if cmd[:3] == ["git", "-C", str(project_root)]:
            return Proc(returncode=1, stdout="", stderr="not git")
        if cmd[:2] == ["tmux", "has-session"]:
            return Proc(returncode=1, stdout="", stderr="")
        if cmd[:2] == ["tmux", "new-session"]:
            return Proc(returncode=0, stdout="", stderr="")
        if cmd[:2] == ["tmux", "kill-session"]:
            return Proc(returncode=0, stdout="", stderr="")
        if cmd[:2] == ["aoe-orch", "run"] and "--dry-run" in cmd:
            roles = cmd[cmd.index("--roles") + 1] if "--roles" in cmd else ""
            dispatch_roles = [token.strip() for token in roles.split(",") if token.strip()]
            payload = {"request_id": cmd[cmd.index("--request-id") + 1], "dispatch_plan": [{"role": role} for role in dispatch_roles]}
            return Proc(returncode=0, stdout=json.dumps(payload), stderr="")
        if cmd[:2] == ["aoe-orch", "run"]:
            roles = cmd[cmd.index("--roles") + 1] if "--roles" in cmd else ""
            req_id = cmd[cmd.index("--request-id") + 1]
            recorded_roles.append((req_id, roles))
            payload = {
                "request_id": req_id,
                "complete": True,
                "counts": {"assignments": 1, "replies": 1},
                "roles": [{"role": roles, "status": "done", "message_id": f"MSG-{roles}"}],
                "done_roles": [roles],
                "failed_roles": [],
                "pending_roles": [],
                "replies": [{"role": roles, "body": f"{roles} done"}],
                "reply_messages": [{"id": f"REP-{roles}", "from": roles, "status": "sent"}],
            }
            return Proc(returncode=0, stdout=json.dumps(payload), stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    args = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        aoe_orch_bin="aoe-orch",
        orch_poll_sec=2.0,
        orch_timeout_sec=180,
        orch_command_timeout_sec=180,
        no_wait=False,
        roles="",
        priority="P2",
    )

    result = tf_exec.run_aoe_orch(
        args,
        "Implement, document, and review",
        "chat-1",
        default_tf_exec_mode=gw.DEFAULT_TF_EXEC_MODE,
        default_tf_work_root_name=gw.DEFAULT_TF_WORK_ROOT_NAME,
        default_tf_exec_map_file=gw.DEFAULT_TF_EXEC_MAP_FILE,
        default_tf_worker_startup_grace_sec=gw.DEFAULT_TF_WORKER_STARTUP_GRACE_SEC,
        now_iso=gw.now_iso,
        run_command=fake_run_command,
        roles_override="Codex-Dev,Codex-Writer,Codex-Reviewer,Claude-Reviewer",
        metadata={
            "request_id": "REQ-TOP",
            "gateway_request_id": "REQ-TOP",
            "phase2_execution_plan": {
                "execution_mode": "parallel",
                "execution_lanes": [
                    {"lane_id": "L1", "role": "Codex-Dev", "subtask_ids": ["S1"], "parallel": True},
                    {"lane_id": "L2", "role": "Codex-Writer", "subtask_ids": ["S2"], "parallel": True},
                ],
                "review_mode": "parallel",
                "review_lanes": [
                    {"lane_id": "R1", "role": "Codex-Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": True},
                    {"lane_id": "R2", "role": "Claude-Reviewer", "kind": "verifier", "depends_on": ["L2"], "parallel": True},
                ],
                "parallel_workers": True,
                "parallel_reviews": True,
                "readonly": True,
            },
        },
    )

    execution_ids = result["phase2_request_ids"]["execution"]
    review_ids = result["phase2_request_ids"]["review"]
    assert execution_ids == ["REQ-TOP-execution-L1", "REQ-TOP-execution-L2"]
    assert review_ids == ["REQ-REVIEW-BASE-review-R1", "REQ-REVIEW-BASE-review-R2"]
    assert set(result["linked_request_ids"]) == set(execution_ids + review_ids)
    assert result["request_id"] == "REQ-TOP"
    assert result["gateway_request_id"] == "REQ-TOP"
    assert {role for _req, role in recorded_roles} == {"Codex-Dev", "Codex-Writer", "Codex-Reviewer", "Claude-Reviewer"}
    assert result["tf_workers"]["execution"]["parallel"] is True
    assert len(result["tf_workers"]["execution"]["lanes"]) == 2
    assert result["tf_workers"]["review"]["parallel"] is True
    assert len(result["tf_workers"]["review"]["lanes"]) == 2


def test_local_run_aoe_orch_preserves_same_role_parallel_lane_role_states(monkeypatch, tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True)
    monkeypatch.setenv("AOE_TF_EXEC_MODE", "inplace")
    request_ids = iter(["REQ-REVIEW-BASE"])
    monkeypatch.setattr(tf_exec, "create_request_id", lambda: next(request_ids))

    class Proc:
        def __init__(self, returncode=0, stdout="", stderr=""):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = stderr

    def fake_run_command(cmd, env=None, timeout_sec=0):
        if cmd[:3] == ["git", "-C", str(project_root)]:
            return Proc(returncode=1, stdout="", stderr="not git")
        if cmd[:2] == ["tmux", "has-session"]:
            return Proc(returncode=1, stdout="", stderr="")
        if cmd[:2] == ["tmux", "new-session"]:
            return Proc(returncode=0, stdout="", stderr="")
        if cmd[:2] == ["tmux", "kill-session"]:
            return Proc(returncode=0, stdout="", stderr="")
        if cmd[:2] == ["aoe-orch", "run"] and "--dry-run" in cmd:
            roles = cmd[cmd.index("--roles") + 1] if "--roles" in cmd else ""
            payload = {"request_id": cmd[cmd.index("--request-id") + 1], "dispatch_plan": [{"role": token.strip()} for token in roles.split(",") if token.strip()]}
            return Proc(returncode=0, stdout=json.dumps(payload), stderr="")
        if cmd[:2] == ["aoe-orch", "run"]:
            roles = cmd[cmd.index("--roles") + 1] if "--roles" in cmd else ""
            req_id = cmd[cmd.index("--request-id") + 1]
            payload = {
                "request_id": req_id,
                "complete": True,
                "counts": {"assignments": 1, "replies": 1},
                "roles": [{"role": roles, "status": "done", "message_id": f"MSG-{req_id}"}],
                "done_roles": [roles],
                "failed_roles": [],
                "pending_roles": [],
                "replies": [{"role": roles, "body": f"{roles} done"}],
                "reply_messages": [{"id": f"REP-{req_id}", "from": roles, "status": "sent"}],
            }
            return Proc(returncode=0, stdout=json.dumps(payload), stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    args = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        aoe_orch_bin="aoe-orch",
        orch_poll_sec=2.0,
        orch_timeout_sec=180,
        orch_command_timeout_sec=180,
        no_wait=False,
        roles="",
        priority="P2",
    )

    result = tf_exec.run_aoe_orch(
        args,
        "Analyze in parallel and review in parallel",
        "chat-1",
        default_tf_exec_mode=gw.DEFAULT_TF_EXEC_MODE,
        default_tf_work_root_name=gw.DEFAULT_TF_WORK_ROOT_NAME,
        default_tf_exec_map_file=gw.DEFAULT_TF_EXEC_MAP_FILE,
        default_tf_worker_startup_grace_sec=gw.DEFAULT_TF_WORKER_STARTUP_GRACE_SEC,
        now_iso=gw.now_iso,
        run_command=fake_run_command,
        roles_override="Codex-Analyst,Codex-Reviewer",
        metadata={
            "request_id": "REQ-TOP",
            "gateway_request_id": "REQ-TOP",
            "phase2_execution_plan": {
                "execution_mode": "parallel",
                "execution_lanes": [
                    {"lane_id": "L1", "role": "Codex-Analyst", "subtask_ids": ["S1"], "parallel": True},
                    {"lane_id": "L2", "role": "Codex-Analyst", "subtask_ids": ["S2"], "parallel": True},
                ],
                "review_mode": "parallel",
                "review_lanes": [
                    {"lane_id": "R1", "role": "Codex-Reviewer", "kind": "verifier", "depends_on": ["L1"], "parallel": True},
                    {"lane_id": "R2", "role": "Codex-Reviewer", "kind": "verifier", "depends_on": ["L2"], "parallel": True},
                ],
                "parallel_workers": True,
                "parallel_reviews": True,
                "readonly": True,
            },
        },
    )

    role_rows = result["role_states"]
    assert len(role_rows) == 4
    assert sorted((row.get("role"), row.get("lane_id"), row.get("phase2_stage")) for row in role_rows) == [
        ("Codex-Analyst", "L1", "execution"),
        ("Codex-Analyst", "L2", "execution"),
        ("Codex-Reviewer", "R1", "review"),
        ("Codex-Reviewer", "R2", "review"),
    ]


def test_send_dispatch_result_finalizes_linked_request_ids() -> None:
    sent: list[dict] = []
    finalized: list[str] = []
    logged: list[dict] = []
    ok = exec_results.send_dispatch_result(
        args=object(),
        key="demo",
        entry={"project_alias": "O3"},
        p_args=argparse.Namespace(default_report_level="short", report_level="short"),
        prompt="Review output",
        state={
            "request_id": "REQ-EXEC",
            "linked_request_ids": ["REQ-EXEC", "REQ-REVIEW"],
            "complete": True,
            "replies": [{"role": "Codex-Reviewer", "body": "done"}],
        },
        req_id="REQ-EXEC",
        task=None,
        run_control_mode="dispatch",
        run_source_request_id="",
        run_auto_source="",
        send=lambda text, **kwargs: sent.append({"text": text, **kwargs}) or True,
        log_event=lambda **kwargs: logged.append(kwargs),
        summarize_task_lifecycle=lambda *_args, **_kwargs: "summary",
        synthesize_orchestrator_response=lambda *_args, **_kwargs: "synthesized",
        render_run_response=lambda *_args, **_kwargs: "rendered",
        finalize_request_reply_messages=lambda _args, rid: finalized.append(rid) or {"request_id": rid},
    )

    assert ok is True
    assert finalized == ["REQ-EXEC", "REQ-REVIEW"]
    assert sent[-1]["context"] == "synth"
    assert logged[-1]["event"] == "dispatch_completed"


def test_gateway_run_aoe_orch_selects_sandbox_backend_and_mirrors_runtime_events(tmp_path: Path) -> None:
    team_dir = tmp_path / "project" / ".aoe-team"
    root_team_dir = tmp_path / "mother" / ".aoe-team"
    team_dir.mkdir(parents=True)
    root_team_dir.mkdir(parents=True)
    (team_dir / "tf_backend.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "backend": "autogen_core",
                "profile": "sandbox",
                "sandbox_only": True,
            }
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        project_root=tmp_path / "project",
        team_dir=team_dir,
        aoe_orch_bin="aoe-orch",
        _aoe_root_team_dir=str(root_team_dir),
        _aoe_project_key="o3",
        _aoe_trace_id="trace-autogen",
    )

    class _FakeAutoGenBackend:
        backend_name = "autogen_core"

        def availability(self):
            return tf_backend.TFBackendAvailability(True, "installed")

        def run(self, request, deps):
            return {
                "request_id": "REQ-AUTOGEN",
                "status": "submitted",
                "runtime_events": [
                    {
                        "seq": 1,
                        "ts": "2026-03-11T19:00:00+0900",
                        "backend": "autogen_core",
                        "source": "tf_orchestrator",
                        "stage": "request.accepted",
                        "kind": "lifecycle",
                        "status": "info",
                        "summary": "accepted sandbox request",
                        "payload": {"project_key": "O3"},
                    }
                ],
            }

    original_local_backend = gw.tf_backend_local_mod.local_backend
    original_autogen_backend = gw.tf_backend_autogen_mod.autogen_core_backend
    gw.tf_backend_local_mod.local_backend = lambda: (_ for _ in ()).throw(AssertionError("local backend should not be selected"))
    gw.tf_backend_autogen_mod.autogen_core_backend = lambda: _FakeAutoGenBackend()
    try:
        result = gw.run_aoe_orch(args, "sandbox review", "chat-1")
    finally:
        gw.tf_backend_local_mod.local_backend = original_local_backend
        gw.tf_backend_autogen_mod.autogen_core_backend = original_autogen_backend

    assert result["backend"] == "autogen_core"
    assert result["backend_profile"] == "sandbox"
    assert result["backend_selection_reason"] == "sandbox_config"

    project_rows = [
        json.loads(line)
        for line in (team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    root_rows = [
        json.loads(line)
        for line in (root_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert project_rows[-1]["event"] == "tf_backend_runtime_event"
    assert project_rows[-1]["backend"] == "autogen_core"
    assert root_rows[-1]["project_team_dir"] == str(team_dir.resolve())


def test_gateway_run_aoe_orch_raises_when_selected_backend_is_unavailable(tmp_path: Path) -> None:
    team_dir = tmp_path / "project" / ".aoe-team"
    team_dir.mkdir(parents=True)
    (team_dir / "tf_backend.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "backend": "autogen_core",
                "profile": "sandbox",
                "sandbox_only": True,
            }
        ),
        encoding="utf-8",
    )
    args = argparse.Namespace(
        project_root=tmp_path / "project",
        team_dir=team_dir,
        aoe_orch_bin="aoe-orch",
        _aoe_root_team_dir=str(tmp_path / "mother" / ".aoe-team"),
        _aoe_project_key="o3",
        _aoe_trace_id="trace-fail",
    )

    class _UnavailableAutoGenBackend:
        backend_name = "autogen_core"

        def availability(self):
            return tf_backend.TFBackendAvailability(False, "autogen_core missing")

        def run(self, request, deps):
            raise AssertionError("run should not be called when backend is unavailable")

    original_autogen_backend = gw.tf_backend_autogen_mod.autogen_core_backend
    gw.tf_backend_autogen_mod.autogen_core_backend = lambda: _UnavailableAutoGenBackend()
    try:
        try:
            gw.run_aoe_orch(args, "sandbox review", "chat-1")
            raise AssertionError("expected RuntimeError")
        except RuntimeError as exc:
            assert "tf backend unavailable" in str(exc)
            assert "autogen_core missing" in str(exc)
    finally:
        gw.tf_backend_autogen_mod.autogen_core_backend = original_autogen_backend
