#!/usr/bin/env python3
"""Gateway CLI regression tests (ported from shell smoke/error scripts)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

import pytest

ROOT = Path(__file__).resolve().parents[2]
GW = ROOT / "scripts/gateway/aoe-telegram-gateway.py"


def _now_utc_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")


def _run_gateway(
    *,
    simulate_text: str,
    allow_chat_ids: str = "test",
    simulate_chat_id: str = "test",
    extra_args: Optional[Iterable[str]] = None,
) -> str:
    extra = list(extra_args or [])
    has_alias_file_flag = "--chat-aliases-file" in extra
    has_manager_state_file_flag = "--manager-state-file" in extra
    alias_path: Optional[str] = None
    state_path: Optional[str] = None
    if not has_alias_file_flag:
        fd, alias_path = tempfile.mkstemp(prefix="gw_aliases_", suffix=".json")
        os.close(fd)
        Path(alias_path).write_text("{}", encoding="utf-8")
    if not has_manager_state_file_flag:
        fd, state_path = tempfile.mkstemp(prefix="gw_manager_state_", suffix=".json")
        os.close(fd)
        # Keep smoke cases deterministic: do not read repo-local runtime state.
        Path(state_path).write_text(
            json.dumps(_base_state(chat_id=str(simulate_chat_id), session_patch={}), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    cmd = [
        sys.executable,
        str(GW),
        "--project-root",
        str(ROOT),
        "--aoe-orch-bin",
        "/bin/echo",
        "--aoe-team-bin",
        "/bin/echo",
        "--allow-chat-ids",
        allow_chat_ids,
        "--once",
        "--dry-run",
        "--simulate-chat-id",
        simulate_chat_id,
        "--simulate-text",
        simulate_text,
    ]
    if alias_path:
        cmd.extend(["--chat-aliases-file", alias_path])
    if state_path:
        cmd.extend(["--manager-state-file", state_path])
    if extra:
        cmd[2:2] = extra
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise AssertionError(
                "gateway command failed\n"
                f"cmd={' '.join(cmd)}\n"
                f"returncode={proc.returncode}\n"
                f"stdout=\n{proc.stdout}\n"
                f"stderr=\n{proc.stderr}"
            )
        return proc.stdout
    finally:
        if alias_path:
            try:
                Path(alias_path).unlink(missing_ok=True)
            except Exception:
                pass
        if state_path:
            try:
                Path(state_path).unlink(missing_ok=True)
            except Exception:
                pass


def _base_state(*, chat_id: str, session_patch: Optional[Dict[str, object]] = None) -> Dict[str, object]:
    patch = dict(session_patch or {})
    session = {
        "updated_at": "2026-02-24T00:00:00+0000",
        **patch,
    }
    return {
        "version": 1,
        "active": "default",
        "updated_at": "2026-02-24T00:00:00+0000",
        "chat_sessions": {
            chat_id: session,
        },
        "projects": {
            "default": {
                "name": "default",
                "display_name": "default",
                "project_alias": "O1",
                "project_root": str(ROOT),
                "team_dir": str(ROOT / ".aoe-team"),
                "overview": "",
                "last_request_id": "",
                "tasks": {},
                "task_alias_index": {},
                "task_seq": 0,
                "todos": [],
                "todo_seq": 0,
                "system_project": False,
                "ops_hidden": False,
                "ops_hidden_reason": "",
                "created_at": "2026-02-24T00:00:00+0000",
                "updated_at": "2026-02-24T00:00:00+0000",
            }
        },
    }


def _seed_ready_project_runtime(entry: Dict[str, object], *, tmp_path: Path, slug: str) -> None:
    project_root = tmp_path / slug
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text(
        json.dumps({"name": slug, "roles": []}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (team_dir / "team.json").write_text(
        json.dumps({"name": slug}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    entry["project_root"] = str(project_root)
    entry["team_dir"] = str(team_dir)


def _write_state(tmp_path: Path, *, chat_id: str, session_patch: Optional[Dict[str, object]] = None) -> Path:
    state = _base_state(chat_id=chat_id, session_patch=session_patch)
    path = tmp_path / "manager_state.json"
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_gateway_poll_state(tmp_path: Path) -> Path:
    path = tmp_path / "gateway_state.json"
    payload = {
        "offset": 1234,
        "processed": 12,
        "acked_updates": 15,
        "handled_messages": 12,
        "duplicate_skipped": 2,
        "empty_skipped": 1,
        "unauthorized_skipped": 0,
        "handler_errors": 1,
        "seen_update_ids": ["100", "101", "102"],
        "seen_message_keys": ["test:10", "test:11"],
        "updated_at": "2026-02-26T00:00:00+0000",
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_gateway_poll_state_with_failed_queue(tmp_path: Path) -> Path:
    path = tmp_path / "gateway_state_with_failed.json"
    now = _now_utc_compact()
    payload = {
        "offset": 2000,
        "processed": 5,
        "acked_updates": 6,
        "handled_messages": 5,
        "duplicate_skipped": 1,
        "empty_skipped": 0,
        "unauthorized_skipped": 0,
        "handler_errors": 1,
        "seen_update_ids": ["100", "101"],
        "seen_message_keys": ["test:10"],
        "failed_queue": [
            {
                "id": "f001",
                "at": now,
                "chat_id": "test",
                "text": "/help",
                "trace_id": "upd-100",
                "error_code": "E_COMMAND",
                "error": "sample",
                "cmd": "grant",
            }
        ],
        "updated_at": now,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_gateway_poll_state_with_failed_queue_multi_chat(tmp_path: Path) -> Path:
    path = tmp_path / "gateway_state_with_failed_multi.json"
    at1 = _now_utc_compact()
    at2 = _now_utc_compact()
    payload = {
        "offset": 2000,
        "processed": 6,
        "acked_updates": 7,
        "handled_messages": 6,
        "duplicate_skipped": 1,
        "empty_skipped": 0,
        "unauthorized_skipped": 0,
        "handler_errors": 2,
        "seen_update_ids": ["100", "101", "102"],
        "seen_message_keys": ["test:10", "other:20"],
        "failed_queue": [
            {
                "id": "f001",
                "at": at1,
                "chat_id": "test",
                "text": "/help",
                "trace_id": "upd-100",
                "error_code": "E_COMMAND",
                "error": "sample",
                "cmd": "grant",
            },
            {
                "id": "f002",
                "at": at2,
                "chat_id": "other",
                "text": "/status",
                "trace_id": "upd-101",
                "error_code": "E_INTERNAL",
                "error": "other-chat-sample",
                "cmd": "retry",
            },
        ],
        "updated_at": at2,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_gateway_poll_state_with_failed_queue_ttl(tmp_path: Path) -> Path:
    path = tmp_path / "gateway_state_with_failed_ttl.json"
    payload = {
        "offset": 3000,
        "processed": 5,
        "acked_updates": 6,
        "handled_messages": 5,
        "duplicate_skipped": 0,
        "empty_skipped": 0,
        "unauthorized_skipped": 0,
        "handler_errors": 1,
        "seen_update_ids": ["300", "301"],
        "seen_message_keys": ["test:30"],
        "failed_queue": [
            {
                "id": "old01",
                "at": "2025-01-01T00:00:00+0000",
                "chat_id": "test",
                "text": "/help",
                "trace_id": "upd-300",
                "error_code": "E_COMMAND",
                "error": "expired-sample",
                "cmd": "grant",
            },
            {
                "id": "new01",
                "at": _now_utc_compact(),
                "chat_id": "test",
                "text": "/status",
                "trace_id": "upd-301",
                "error_code": "E_INTERNAL",
                "error": "fresh-sample",
                "cmd": "retry",
            },
        ],
        "updated_at": _now_utc_compact(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


@pytest.mark.parametrize(
    ("simulate_text", "expect"),
    [
        ("/help", "Quick mode"),
        ("/whoami", "chat_id: test"),
        ("/mode", "routing mode"),
        ("/mode on", "default_mode: dispatch"),
        ("/on", "default_mode: dispatch"),
        ("/off", "default_mode: off"),
        ("/lang", "interface language"),
        ("/lang en", "ui_language: en"),
        ("/report", "report verbosity"),
        ("/report short", "report_level: short"),
        ("/lockme", "cleared_admin_readonly: yes"),
        ("/onlyme", "owner_only: yes"),
        ("/acl", "access control list"),
        ("/map", "project map:"),
        ("/monitor 2", "orch: default"),
        ("/monitor O1", "orch: default"),
        ("모니터 2", "orch: default"),
        ("/kpi 24", "window_hours:"),
        ("/pick", "최근 작업이 없습니다"),
        ("/todo", "todo: active="),
        ("/drain 1", "drain finished"),
        ("/fanout 1", "fanout finished"),
        ("안녕", "입력 형식: prefix 명령만 지원합니다"),
        ("/dispatch 샘플 작업 실행", "[DRY-RUN] orch="),
    ],
)
@pytest.mark.smoke
def test_smoke_cases(simulate_text: str, expect: str) -> None:
    out = _run_gateway(simulate_text=simulate_text)
    assert expect in out


@pytest.mark.smoke
def test_whoami_owner_flag() -> None:
    out = _run_gateway(
        simulate_text="/whoami",
        allow_chat_ids="99999",
        simulate_chat_id="99999",
        extra_args=["--owner-chat-id", "99999"],
    )
    assert "is_owner: yes" in out


@pytest.mark.smoke
def test_acl_alias_map() -> None:
    out = _run_gateway(
        simulate_text="/acl",
        allow_chat_ids="123456789",
        simulate_chat_id="123456789",
    )
    assert "my_alias: 1" in out


@pytest.mark.smoke
def test_orch_alias_use() -> None:
    out = _run_gateway(
        simulate_text="aoe orch use O1",
        extra_args=["--no-slash-only"],
    )
    assert "active orch changed: default" in out


@pytest.mark.smoke
def test_default_mode_plain_routing(tmp_path: Path) -> None:
    state_file = _write_state(
        tmp_path,
        chat_id="test",
        session_patch={"default_mode": "dispatch"},
    )
    out = _run_gateway(
        simulate_text="평문 라우팅 테스트",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "[DRY-RUN] orch=" in out


@pytest.mark.smoke
def test_default_mode_dispatch_question_routes_direct(tmp_path: Path) -> None:
    state_file = _write_state(
        tmp_path,
        chat_id="test",
        session_patch={"default_mode": "dispatch"},
    )
    out = _run_gateway(
        simulate_text="orch 하나를 새로 실행하려면 어떻게 해야해?",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "[DRY-RUN] orch=default mode: direct" in out


@pytest.mark.smoke
def test_default_mode_dispatch_task_stays_dispatch(tmp_path: Path) -> None:
    state_file = _write_state(
        tmp_path,
        chat_id="test",
        session_patch={"default_mode": "dispatch"},
    )
    out = _run_gateway(
        simulate_text="데이터 정리 스크립트 수정해줘",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "[DRY-RUN] orch=default mode: dispatch" in out


@pytest.mark.smoke
def test_owner_bootstrap_mode_enables_plain_text_routing(tmp_path: Path) -> None:
    state_file = _write_state(
        tmp_path,
        chat_id="99999",
        session_patch={},  # default_mode unset
    )
    out = _run_gateway(
        simulate_text="평문 요청 (bootstrap)",
        allow_chat_ids="99999",
        simulate_chat_id="99999",
        extra_args=[
            "--manager-state-file",
            str(state_file),
            "--owner-chat-id",
            "99999",
            "--owner-bootstrap-mode",
            "dispatch",
        ],
    )
    assert "[DRY-RUN] orch=" in out


@pytest.mark.smoke
def test_owner_bootstrap_mode_does_not_apply_to_non_owner(tmp_path: Path) -> None:
    state_file = _write_state(
        tmp_path,
        chat_id="88888",
        session_patch={},  # default_mode unset
    )
    out = _run_gateway(
        simulate_text="평문 요청 (non-owner)",
        allow_chat_ids="88888",
        simulate_chat_id="88888",
        extra_args=[
            "--manager-state-file",
            str(state_file),
            "--owner-chat-id",
            "99999",
            "--owner-bootstrap-mode",
            "dispatch",
        ],
    )
    assert "입력 형식: prefix 명령만 지원합니다" in out


@pytest.mark.smoke
def test_default_mode_cli_precedence(tmp_path: Path) -> None:
    state_file = _write_state(
        tmp_path,
        chat_id="test",
        session_patch={"default_mode": "dispatch"},
    )
    out = _run_gateway(
        simulate_text="aoe mode off",
        extra_args=["--manager-state-file", str(state_file), "--no-slash-only"],
    )
    assert "routing mode updated" in out
    assert "[DRY-RUN] orch=" not in out


@pytest.mark.smoke
def test_auto_scheduler_cli_on_parses_rest() -> None:
    out = _run_gateway(
        simulate_text="aoe auto on",
        extra_args=["--no-slash-only"],
    )
    assert "auto scheduler updated" in out
    assert "- enabled: yes" in out


@pytest.mark.smoke
def test_queue_global_view(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)

    default = projects.get("default") or {}
    assert isinstance(default, dict)
    default["project_alias"] = "O1"
    default["todos"] = [
        {
            "id": "TODO-001",
            "summary": "first todo",
            "priority": "P2",
            "status": "open",
            "created_at": "2026-02-24T00:00:00+0000",
        }
    ]
    default["todo_seq"] = 1

    projects["proj2"] = {
        "name": "proj2",
        "display_name": "proj2",
        "project_alias": "O2",
        "project_root": str(ROOT),
        "team_dir": str(ROOT / ".aoe-team"),
        "overview": "",
        "last_request_id": "",
        "tasks": {},
        "task_alias_index": {},
        "task_seq": 0,
        "todos": [
            {
                "id": "TODO-001",
                "summary": "urgent",
                "priority": "P1",
                "status": "open",
                "created_at": "2026-02-24T00:00:00+0000",
            }
        ],
        "todo_seq": 1,
        "created_at": "2026-02-24T00:00:00+0000",
        "updated_at": "2026-02-24T00:00:00+0000",
    }

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/queue",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "global todo queue" in out
    assert "O1" in out
    assert "O2" in out
    assert "TODO-001" in out


@pytest.mark.smoke
def test_queue_cli_parsing(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    default["project_alias"] = "O1"
    default["todos"] = [
        {
            "id": "TODO-001",
            "summary": "first todo",
            "priority": "P2",
            "status": "open",
            "created_at": "2026-02-24T00:00:00+0000",
        }
    ]
    default["todo_seq"] = 1

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="aoe queue",
        extra_args=["--manager-state-file", str(state_file), "--no-slash-only"],
    )
    assert "global todo queue" in out


@pytest.mark.smoke
def test_sync_imports_project_scenario_file(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    default["project_alias"] = "O1"

    team_dir = tmp_path / "proj_team_dir"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "AOE_TODO.md").write_text(
        "# scenario\n\n- [ ] P1: first synced todo\n- [ ] P2: second synced todo\n",
        encoding="utf-8",
    )
    default["team_dir"] = str(team_dir)

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/sync",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "sync finished" in out
    assert "- missing_files: 0" in out
    assert "- added: 2" in out
    assert "default: parsed=2 added=2" in out


@pytest.mark.smoke
def test_sync_falls_back_to_todo_files_when_scenario_is_empty(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    default["project_alias"] = "O1"

    proj_root = tmp_path / "proj_root"
    team_dir = proj_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "AOE_TODO.md").write_text("# AOE_TODO.md\n\n## Tasks\n\n", encoding="utf-8")
    (proj_root / "TODO.md").write_text("- [ ] P1: file fallback todo\n- [ ] second fallback todo\n", encoding="utf-8")
    default["project_root"] = str(proj_root)
    default["team_dir"] = str(team_dir)

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/sync",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "sync finished" in out
    assert "- missing_files: 0" in out
    assert "- added: 1" in out
    assert "scenario-empty->fallback:bootstrap" in out
    assert "src=TODO.md" in out


@pytest.mark.smoke
def test_sync_todo_file_ignores_meta_sections_and_prefix_lines(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    default["project_alias"] = "O1"

    proj_root = tmp_path / "proj_root"
    team_dir = proj_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "AOE_TODO.md").write_text("# AOE_TODO.md\n\n## Tasks\n\n", encoding="utf-8")
    (proj_root / "TODO.md").write_text(
        "# Next TODO\n\n"
        "Purpose:\n"
        "- Explain why this file exists\n"
        "- 기준: keep the baseline policy fixed\n\n"
        "## P0 (next)\n"
        "1) **External bucket follow-ups**\n"
        "- v1 is complete. 다음은:\n"
        "- Build the summary table\n"
        "- 설계 선택지(우선순위):\n",
        encoding="utf-8",
    )
    default["project_root"] = str(proj_root)
    default["team_dir"] = str(team_dir)

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/sync",
        extra_args=["--manager-state-file", str(state_file)],
    )

    assert "sync finished" in out
    assert "- added: 1" in out
    assert "scenario-empty->fallback:bootstrap" in out
    assert "skipped_done_missing" not in out


@pytest.mark.smoke
def test_sync_falls_back_to_recent_docs_when_scenario_is_empty(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    default["project_alias"] = "O1"

    proj_root = tmp_path / "proj_root"
    team_dir = proj_root / ".aoe-team"
    docs_dir = proj_root / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "AOE_TODO.md").write_text("# AOE_TODO.md\n\n## Tasks\n\n", encoding="utf-8")
    (docs_dir / "meeting-notes.md").write_text("# Todo\n- P1: recent fallback todo\n", encoding="utf-8")
    default["project_root"] = str(proj_root)
    default["team_dir"] = str(team_dir)

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/sync",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "sync finished" in out
    assert "- missing_files: 0" in out
    assert "- added: 1" in out
    assert "scenario-empty->fallback:bootstrap" in out
    assert "src=docs/meeting-notes.md" in out


@pytest.mark.smoke
def test_sync_recent_imports_from_project_docs(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    default["project_alias"] = "O1"

    proj_root = tmp_path / "proj_root"
    docs_dir = proj_root / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    team_dir = proj_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    default["project_root"] = str(proj_root)
    default["team_dir"] = str(team_dir)

    # Most recent file without markers (should be skipped), then 3 docs with todo markers.
    (docs_dir / "c.md").write_text("just notes\n", encoding="utf-8")
    (docs_dir / "d.md").write_text("TODO: fourth\n", encoding="utf-8")
    (docs_dir / "b.md").write_text("- [ ] P3: third\n", encoding="utf-8")
    (docs_dir / "a.md").write_text("# Notes\n\n## Todo\n- P1: first\n- P2: second\n", encoding="utf-8")

    base = 1_700_000_000
    os.utime(docs_dir / "a.md", (base - 30, base - 30))
    os.utime(docs_dir / "b.md", (base - 20, base - 20))
    os.utime(docs_dir / "d.md", (base - 10, base - 10))
    os.utime(docs_dir / "c.md", (base, base))

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/sync recent",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "sync finished" in out
    assert "- mode: recent_docs" in out
    assert "- missing_docs: 0" in out
    assert "- added: 3" in out
    assert "docs=2/3" in out
    assert "docs/d.md" in out


@pytest.mark.smoke
def test_drain_runs_multiple_steps(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    _seed_ready_project_runtime(default, tmp_path=tmp_path, slug="default")
    default["project_alias"] = "O1"
    default["todos"] = [
        {
            "id": "TODO-001",
            "summary": "first todo",
            "priority": "P2",
            "status": "open",
            "created_at": "2026-02-24T00:00:00+0000",
        }
    ]
    default["todo_seq"] = 1

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/drain 2",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert out.count("[DRY-RUN] orch=") >= 2


@pytest.mark.smoke
def test_fanout_runs_one_per_project(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    _seed_ready_project_runtime(default, tmp_path=tmp_path, slug="default")
    default["project_alias"] = "O1"
    default["todos"] = [
        {
            "id": "TODO-001",
            "summary": "first todo",
            "priority": "P2",
            "status": "open",
            "created_at": "2026-02-24T00:00:00+0000",
        }
    ]
    default["todo_seq"] = 1

    projects["proj2"] = {
        "name": "proj2",
        "display_name": "proj2",
        "project_alias": "O2",
        "project_root": "",
        "team_dir": "",
        "overview": "",
        "last_request_id": "",
        "tasks": {},
        "task_alias_index": {},
        "task_seq": 0,
        "todos": [
            {
                "id": "TODO-001",
                "summary": "second todo",
                "priority": "P2",
                "status": "open",
                "created_at": "2026-02-24T00:00:00+0000",
            }
        ],
        "todo_seq": 1,
        "created_at": "2026-02-24T00:00:00+0000",
        "updated_at": "2026-02-24T00:00:00+0000",
    }
    proj2 = projects["proj2"]
    assert isinstance(proj2, dict)
    _seed_ready_project_runtime(proj2, tmp_path=tmp_path, slug="proj2")

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/fanout",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert out.count("[DRY-RUN] orch=") >= 2


@pytest.mark.smoke
def test_fanout_skips_paused_projects(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    _seed_ready_project_runtime(default, tmp_path=tmp_path, slug="default")
    default["project_alias"] = "O1"
    default["todos"] = [
        {
            "id": "TODO-001",
            "summary": "first todo",
            "priority": "P2",
            "status": "open",
            "created_at": "2026-02-24T00:00:00+0000",
        }
    ]
    default["todo_seq"] = 1

    projects["proj2"] = {
        "name": "proj2",
        "display_name": "proj2",
        "project_alias": "O2",
        "project_root": "",
        "team_dir": "",
        "overview": "",
        "last_request_id": "",
        "tasks": {},
        "task_alias_index": {},
        "task_seq": 0,
        "paused": True,
        "todos": [
            {
                "id": "TODO-001",
                "summary": "second todo (paused)",
                "priority": "P1",
                "status": "open",
                "created_at": "2026-02-24T00:00:00+0000",
            }
        ],
        "todo_seq": 1,
        "created_at": "2026-02-24T00:00:00+0000",
        "updated_at": "2026-02-24T00:00:00+0000",
    }
    proj2 = projects["proj2"]
    assert isinstance(proj2, dict)
    _seed_ready_project_runtime(proj2, tmp_path=tmp_path, slug="proj2")

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/fanout",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "fanout finished" in out
    assert "- skipped_paused: 1" in out
    assert "orch=proj2" not in out


@pytest.mark.smoke
def test_fanout_does_not_treat_blocked_rows_as_busy_when_open_todo_exists(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    _seed_ready_project_runtime(default, tmp_path=tmp_path, slug="default")
    default["project_alias"] = "O1"
    default["todos"] = [
        {
            "id": "TODO-001",
            "summary": "blocked row",
            "priority": "P1",
            "status": "blocked",
            "created_at": "2026-02-24T00:00:00+0000",
        },
        {
            "id": "TODO-002",
            "summary": "open row",
            "priority": "P2",
            "status": "open",
            "created_at": "2026-02-24T00:00:01+0000",
        },
    ]
    default["todo_seq"] = 2

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/fanout",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "fanout finished" in out
    assert "- skipped_busy: 0" in out
    assert "[DRY-RUN] orch=default" in out


@pytest.mark.smoke
def test_next_skips_paused_projects(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)

    default = projects.get("default") or {}
    assert isinstance(default, dict)
    _seed_ready_project_runtime(default, tmp_path=tmp_path, slug="default")
    default["project_alias"] = "O1"
    default["paused"] = True
    default["todos"] = [
        {
            "id": "TODO-001",
            "summary": "paused todo",
            "priority": "P1",
            "status": "open",
            "created_at": "2026-02-24T00:00:00+0000",
        }
    ]
    default["todo_seq"] = 1

    projects["proj2"] = {
        "name": "proj2",
        "display_name": "proj2",
        "project_alias": "O2",
        "project_root": "",
        "team_dir": "",
        "overview": "",
        "last_request_id": "",
        "tasks": {},
        "task_alias_index": {},
        "task_seq": 0,
        "paused": False,
        "todos": [
            {
                "id": "TODO-001",
                "summary": "active todo",
                "priority": "P2",
                "status": "open",
                "created_at": "2026-02-24T00:00:00+0000",
            }
        ],
        "todo_seq": 1,
        "created_at": "2026-02-24T00:00:00+0000",
        "updated_at": "2026-02-24T00:00:00+0000",
    }
    proj2 = projects["proj2"]
    assert isinstance(proj2, dict)
    _seed_ready_project_runtime(proj2, tmp_path=tmp_path, slug="proj2")

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/next",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "next selected (global)" in out
    assert "orch: proj2 (O2)" in out


@pytest.mark.smoke
def test_map_shows_paused_projects(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={"default_mode": "direct"})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    default["project_alias"] = "O1"
    default["paused"] = True

    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    out = _run_gateway(
        simulate_text="/map",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "[PAUSED]" in out


@pytest.mark.smoke
def test_auto_scheduler_cli_fanout_sets_command() -> None:
    out = _run_gateway(
        simulate_text="aoe auto on fanout",
        extra_args=["--no-slash-only"],
    )
    assert "auto scheduler updated" in out
    assert "- command: fanout" in out


@pytest.mark.smoke
def test_auto_scheduler_cli_fanout_recent_sets_prefetch() -> None:
    out = _run_gateway(
        simulate_text="aoe auto on fanout recent",
        extra_args=["--no-slash-only"],
    )
    assert "auto scheduler updated" in out
    assert "- command: fanout" in out
    assert "- prefetch: sync_recent" in out


@pytest.mark.smoke
def test_auto_scheduler_cli_fanout_recent_replace_sync_sets_full_scope_prefetch() -> None:
    out = _run_gateway(
        simulate_text="aoe auto on fanout recent replace-sync",
        extra_args=["--no-slash-only"],
    )
    assert "auto scheduler updated" in out
    assert "- command: fanout" in out
    assert "- prefetch: sync_recent+replace (full-scope; since ignored)" in out


@pytest.mark.smoke
def test_offdesk_status_dry_run(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    out = _run_gateway(
        simulate_text="/offdesk status",
        extra_args=["--team-dir", str(team_dir)],
    )
    assert "offdesk mode" in out
    assert "- enabled: no" in out
    assert "- auto_enabled: no" in out


@pytest.mark.smoke
def test_offdesk_on_dry_run_enables_preset(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    state_file = _write_state(
        tmp_path,
        chat_id="test",
        session_patch={
            "default_mode": "dispatch",
            "report_level": "long",
            "room": "O9",
        },
    )
    out = _run_gateway(
        simulate_text="/offdesk on",
        extra_args=["--team-dir", str(team_dir), "--manager-state-file", str(state_file)],
    )
    assert "offdesk enabled" in out
    assert "- report_level: short" in out
    assert "- routing_mode: off" in out
    assert "- command: fanout" in out
    assert "- prefetch: sync_recent" in out


@pytest.mark.smoke
def test_offdesk_on_replace_sync_dry_run_enables_full_scope_prefetch(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    state_file = _write_state(
        tmp_path,
        chat_id="test",
        session_patch={
            "default_mode": "dispatch",
            "report_level": "long",
            "room": "O9",
        },
    )
    out = _run_gateway(
        simulate_text="/offdesk on replace-sync",
        extra_args=["--team-dir", str(team_dir), "--manager-state-file", str(state_file)],
    )
    assert "offdesk enabled" in out
    assert "- command: fanout" in out
    assert "- prefetch: sync_recent+replace (full-scope; since ignored)" in out


@pytest.mark.smoke
def test_offdesk_off_dry_run_restores_previous_settings(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "offdesk_state.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "test",
                "started_at": "2026-02-26T00:00:00+0000",
                "prev": {
                    "default_mode_present": True,
                    "default_mode": "dispatch",
                    "report_level_present": True,
                    "report_level": "long",
                    "room_present": True,
                    "room": "O9",
                },
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state_file = _write_state(tmp_path, chat_id="test", session_patch={"report_level": "short"})
    out = _run_gateway(
        simulate_text="/offdesk off",
        extra_args=["--team-dir", str(team_dir), "--manager-state-file", str(state_file)],
    )
    assert "offdesk disabled" in out
    assert "- restored_routing_mode: dispatch" in out
    assert "- restored_report_level: long" in out
    assert "- restored_room: O9" in out


@pytest.mark.smoke
def test_panic_status_dry_run(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    out = _run_gateway(
        simulate_text="/panic status",
        extra_args=["--team-dir", str(team_dir)],
    )
    assert "panic switch" in out
    assert "- auto_enabled: no" in out
    assert "- offdesk_enabled: no" in out


@pytest.mark.smoke
def test_panic_dry_run_stops_auto_and_clears_routing(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    state_file = _write_state(tmp_path, chat_id="test", session_patch={"default_mode": "dispatch"})
    out = _run_gateway(
        simulate_text="/panic",
        extra_args=["--team-dir", str(team_dir), "--manager-state-file", str(state_file)],
    )
    assert "panic activated" in out
    assert "- auto: stopped" in out
    assert "- routing_mode: off" in out
    assert "dry-run: skipped tmux auto off" in out


@pytest.mark.smoke
def test_help_uses_chat_lang_for_command_description(tmp_path: Path) -> None:
    state_file = _write_state(
        tmp_path,
        chat_id="test",
        session_patch={"lang": "en"},
    )
    out = _run_gateway(
        simulate_text="/help",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "confirm high-risk auto execution" in out


@pytest.mark.smoke
def test_room_post_and_tail(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    out = _run_gateway(
        simulate_text="/room post hello",
        extra_args=["--team-dir", str(team_dir)],
    )
    assert "room posted" in out
    out = _run_gateway(
        simulate_text="/room tail 1",
        extra_args=["--team-dir", str(team_dir)],
    )
    assert "hello" in out


@pytest.mark.smoke
def test_gc_dry_run_skips(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    out = _run_gateway(
        simulate_text="/gc",
        extra_args=["--team-dir", str(team_dir)],
    )
    assert "gc skipped (dry-run)" in out


@pytest.mark.smoke
def test_readonly_room_post_denied(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    room_dir = team_dir / "logs" / "rooms" / "global"
    room_dir.mkdir(parents=True, exist_ok=True)
    (room_dir / "2026-02-28.jsonl").write_text(
        '{"ts":"2026-02-28T00:00:00+0000","actor":"test","kind":"post","text":"seed"}\n',
        encoding="utf-8",
    )
    out = _run_gateway(
        simulate_text="/room list",
        extra_args=["--team-dir", str(team_dir), "--readonly-chat-ids", "test"],
    )
    assert "room list" in out
    assert "global" in out
    out = _run_gateway(
        simulate_text="/room post nope",
        extra_args=["--team-dir", str(team_dir), "--readonly-chat-ids", "test"],
    )
    assert "permission denied: readonly chat cannot post." in out


@pytest.mark.smoke
def test_readonly_lang_set_denied() -> None:
    out = _run_gateway(
        simulate_text="/lang en",
        allow_chat_ids="test",
        simulate_chat_id="test",
        extra_args=["--readonly-chat-ids", "test"],
    )
    assert "permission denied: readonly chat cannot change interface language." in out


@pytest.mark.smoke
def test_risk_confirm_required(tmp_path: Path) -> None:
    state_file = _write_state(
        tmp_path,
        chat_id="test",
        session_patch={"default_mode": "dispatch"},
    )
    out = _run_gateway(
        simulate_text="rm -rf /tmp/demo",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "고위험 자동실행 감지" in out


@pytest.mark.smoke
def test_risk_confirm_ok(tmp_path: Path) -> None:
    state_file = _write_state(
        tmp_path,
        chat_id="test",
        session_patch={
            "confirm_action": {
                "mode": "dispatch",
                "prompt": "rm -rf /tmp/demo",
                "risk": "destructive_delete",
                "requested_at": _now_utc_compact(),
            }
        },
    )
    out = _run_gateway(
        simulate_text="/ok",
        extra_args=["--manager-state-file", str(state_file), "--confirm-ttl-sec", "86400"],
    )
    assert "[DRY-RUN] orch=" in out


@pytest.mark.smoke
def test_mode_off_clears_pending_and_confirm(tmp_path: Path) -> None:
    state_file = _write_state(
        tmp_path,
        chat_id="test",
        session_patch={
            "default_mode": "dispatch",
            "pending_mode": "dispatch",
            "confirm_action": {
                "mode": "dispatch",
                "prompt": "dangerous task",
                "risk": "destructive_delete",
                "requested_at": "2026-02-24T00:00:00+0000",
            },
        },
    )
    out = _run_gateway(
        simulate_text="/off",
        extra_args=["--manager-state-file", str(state_file)],
    )
    assert "one_shot_pending_cleared: yes" in out
    assert "confirm_request_cleared: yes" in out


@pytest.mark.smoke
def test_status_includes_poll_state_counters(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    _seed_ready_project_runtime(default, tmp_path=tmp_path, slug="default")
    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    poll_state = _write_gateway_poll_state(tmp_path)
    out = _run_gateway(
        simulate_text="/status",
        extra_args=["--state-file", str(poll_state), "--manager-state-file", str(state_file)],
    )
    assert "poll_state: acked=15 handled=12 duplicates=2" in out
    assert "failed_queue_total=0" in out
    assert "last_failed_at=-" in out
    assert "poll_cursor: offset=1234" in out
    assert "updated_at=2026-02-26T00:00:00+0000" in out
    assert "active_tf_count: 0 (pending=0 running=0)" in out


@pytest.mark.smoke
def test_kpi_includes_poll_state_counters(tmp_path: Path) -> None:
    state_file = _write_state(tmp_path, chat_id="test")
    poll_state = _write_gateway_poll_state(tmp_path)
    out = _run_gateway(
        simulate_text="/kpi 24",
        extra_args=["--state-file", str(poll_state), "--manager-state-file", str(state_file)],
    )
    assert "window_hours: 24" in out
    assert "poll_state: acked=15 handled=12 duplicates=2" in out
    assert "failed_queue_total=0" in out
    assert "last_failed_at=-" in out
    assert "poll_cursor: offset=1234" in out
    assert "updated_at=2026-02-26T00:00:00+0000" in out


@pytest.mark.smoke
def test_status_poll_state_includes_failed_queue_summary(tmp_path: Path) -> None:
    state = _base_state(chat_id="test", session_patch={})
    projects = state.get("projects") or {}
    assert isinstance(projects, dict)
    default = projects.get("default") or {}
    assert isinstance(default, dict)
    _seed_ready_project_runtime(default, tmp_path=tmp_path, slug="default")
    state_file = tmp_path / "manager_state.json"
    state_file.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    poll_state = _write_gateway_poll_state_with_failed_queue_multi_chat(tmp_path)
    out = _run_gateway(
        simulate_text="/status",
        extra_args=["--state-file", str(poll_state), "--manager-state-file", str(state_file)],
    )
    assert "failed_queue_total=2" in out
    assert "last_failed_at=" in out
    assert "last_failed_at=-" not in out


@pytest.mark.smoke
def test_replay_list(tmp_path: Path) -> None:
    poll_state = _write_gateway_poll_state_with_failed_queue(tmp_path)
    out = _run_gateway(
        simulate_text="/replay",
        extra_args=["--state-file", str(poll_state)],
    )
    assert "replay queue: 1 pending" in out
    assert "id=f001" in out


@pytest.mark.smoke
def test_replay_executes_failed_payload(tmp_path: Path) -> None:
    poll_state = _write_gateway_poll_state_with_failed_queue(tmp_path)
    out = _run_gateway(
        simulate_text="/replay 1",
        extra_args=["--state-file", str(poll_state)],
    )
    assert "replay start" in out
    assert "Quick mode" in out
    state = json.loads(poll_state.read_text(encoding="utf-8"))
    assert state.get("failed_queue", []) == []


@pytest.mark.smoke
def test_replay_show_details(tmp_path: Path) -> None:
    poll_state = _write_gateway_poll_state_with_failed_queue(tmp_path)
    out = _run_gateway(
        simulate_text="/replay show f001",
        extra_args=["--state-file", str(poll_state)],
    )
    assert "replay item" in out
    assert "- id: f001" in out
    assert "- run: /replay f001" in out
    state = json.loads(poll_state.read_text(encoding="utf-8"))
    queue = state.get("failed_queue", [])
    assert isinstance(queue, list) and len(queue) == 1
    assert queue[0].get("id") == "f001"


@pytest.mark.smoke
def test_replay_purge_chat_scope(tmp_path: Path) -> None:
    poll_state = _write_gateway_poll_state_with_failed_queue_multi_chat(tmp_path)
    out = _run_gateway(
        simulate_text="/replay purge",
        extra_args=["--state-file", str(poll_state)],
    )
    assert "replay purge done" in out
    assert "- removed: 1" in out
    state = json.loads(poll_state.read_text(encoding="utf-8"))
    queue = state.get("failed_queue", [])
    assert isinstance(queue, list) and len(queue) == 1
    assert queue[0].get("id") == "f002"


@pytest.mark.smoke
def test_replay_cli_show_details(tmp_path: Path) -> None:
    poll_state = _write_gateway_poll_state_with_failed_queue(tmp_path)
    out = _run_gateway(
        simulate_text="aoe replay show f001",
        extra_args=["--state-file", str(poll_state), "--no-slash-only"],
    )
    assert "replay item" in out
    assert "- id: f001" in out


@pytest.mark.smoke
def test_readonly_replay_list_allowed(tmp_path: Path) -> None:
    poll_state = _write_gateway_poll_state_with_failed_queue(tmp_path)
    out = _run_gateway(
        simulate_text="/replay list",
        allow_chat_ids="",
        extra_args=["--state-file", str(poll_state), "--readonly-chat-ids", "test"],
    )
    assert "replay queue: 1 pending" in out
    assert "id=f001" in out


@pytest.mark.smoke
def test_readonly_replay_run_denied(tmp_path: Path) -> None:
    poll_state = _write_gateway_poll_state_with_failed_queue(tmp_path)
    out = _run_gateway(
        simulate_text="/replay 1",
        allow_chat_ids="",
        extra_args=["--state-file", str(poll_state), "--readonly-chat-ids", "test"],
    )
    assert "permission denied: readonly chat." in out
    state = json.loads(poll_state.read_text(encoding="utf-8"))
    queue = state.get("failed_queue", [])
    assert isinstance(queue, list) and len(queue) == 1
    assert queue[0].get("id") == "f001"


@pytest.mark.smoke
def test_replay_ttl_prunes_expired(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    poll_state = _write_gateway_poll_state_with_failed_queue_ttl(tmp_path)
    monkeypatch.setenv("AOE_GATEWAY_FAILED_TTL_HOURS", "1")
    out = _run_gateway(
        simulate_text="/replay list",
        extra_args=["--state-file", str(poll_state)],
    )
    assert "replay queue: 1 pending" in out
    assert "id=new01" in out
    assert "id=old01" not in out
    state = json.loads(poll_state.read_text(encoding="utf-8"))
    queue = state.get("failed_queue", [])
    assert isinstance(queue, list) and len(queue) == 1
    assert queue[0].get("id") == "new01"


@pytest.mark.smoke
def test_handler_error_enqueues_failed_replay_item(tmp_path: Path) -> None:
    poll_state = _write_gateway_poll_state(tmp_path)
    out = _run_gateway(
        simulate_text="/grant admin abc",
        extra_args=["--state-file", str(poll_state)],
    )
    assert "replay: /replay" in out
    state = json.loads(poll_state.read_text(encoding="utf-8"))
    queue = state.get("failed_queue", [])
    assert isinstance(queue, list) and len(queue) >= 1
    assert queue[-1].get("chat_id") == "test"
    assert "/grant admin abc" in str(queue[-1].get("text", ""))


@pytest.mark.parametrize(
    "simulate_text",
    [
        "aoe run --priority X hello",
        "aoe retry",
        "aoe replan",
        "aoe mode weird",
        "aoe on now please",
        "aoe ok now",
        "aoe replay show",
        "aoe grant admin abc",
        "aoe revoke nope 123456",
        "aoe revoke all 999",
    ],
)
@pytest.mark.error
def test_error_cases(simulate_text: str) -> None:
    out = _run_gateway(
        simulate_text=simulate_text,
        extra_args=["--no-slash-only"],
    )
    assert "error_code: E_COMMAND" in out


@pytest.mark.error
def test_error_unknown_project_has_friendly_message() -> None:
    out = _run_gateway(
        simulate_text="aoe orch use no_such_project",
        extra_args=["--no-slash-only"],
    )
    assert "unknown orch project: no_such_project" in out


@pytest.mark.error
def test_owner_only_grant_deny() -> None:
    out = _run_gateway(
        simulate_text="aoe grant admin 123456",
        allow_chat_ids="test,99999",
        extra_args=["--owner-chat-id", "99999", "--no-slash-only"],
    )
    assert "owner-only" in out


@pytest.mark.error
def test_owner_only_lockme_deny() -> None:
    out = _run_gateway(
        simulate_text="/lockme",
        allow_chat_ids="test,99999",
        extra_args=["--owner-chat-id", "99999"],
    )
    assert "owner-only" in out
