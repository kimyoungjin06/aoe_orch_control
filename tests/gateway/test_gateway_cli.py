#!/usr/bin/env python3
"""Gateway CLI regression tests (ported from shell smoke/error scripts)."""

from __future__ import annotations

import json
import subprocess
import sys
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
    if extra_args:
        cmd[2:2] = list(extra_args)
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
                "project_root": str(ROOT),
                "team_dir": str(ROOT / ".aoe-team"),
                "overview": "",
                "last_request_id": "",
                "tasks": {},
                "task_alias_index": {},
                "task_seq": 0,
                "created_at": "2026-02-24T00:00:00+0000",
                "updated_at": "2026-02-24T00:00:00+0000",
            }
        },
    }


def _write_state(tmp_path: Path, *, chat_id: str, session_patch: Optional[Dict[str, object]] = None) -> Path:
    state = _base_state(chat_id=chat_id, session_patch=session_patch)
    path = tmp_path / "manager_state.json"
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
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
        ("/lockme", "cleared_admin_readonly: yes"),
        ("/acl", "access control list"),
        ("/monitor 2", "orch: default"),
        ("모니터 2", "orch: default"),
        ("/kpi 24", "window_hours:"),
        ("/pick", "usage: /pick"),
        ("안녕", "슬래시 명령만 지원합니다"),
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


@pytest.mark.parametrize(
    "simulate_text",
    [
        "aoe run --priority X hello",
        "aoe orch use no_such_project",
        "aoe retry",
        "aoe replan",
        "aoe mode weird",
        "aoe on now please",
        "aoe ok now",
        "aoe grant admin abc",
        "aoe revoke nope 123456",
        "aoe revoke all 1",
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
