#!/usr/bin/env python3
"""Room autopublish routing regression tests."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GW_DIR = ROOT / "scripts/gateway"
GW_FILE = GW_DIR / "aoe-telegram-gateway.py"

if str(GW_DIR) not in sys.path:
    sys.path.insert(0, str(GW_DIR))

_spec = importlib.util.spec_from_file_location("aoe_telegram_gateway_mod", GW_FILE)
assert _spec and _spec.loader
gw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gw)


def _read_last_jsonl(path: Path) -> dict:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    assert lines, f"empty jsonl: {path}"
    return json.loads(lines[-1])


def test_room_autopublish_routes_to_project_when_global(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_ROOM_AUTOPUBLISH", "1")
    monkeypatch.setenv("AOE_ROOM_AUTOPUBLISH_ROUTE", "project")

    team_dir = tmp_path / ".aoe-team"
    state = {
        "version": 1,
        "active": "default",
        "chat_sessions": {},
        "projects": {
            "default": {
                "name": "default",
                "project_alias": "O1",
            }
        },
    }

    gw.room_autopublish_event(
        team_dir=team_dir,
        manager_state=state,
        chat_id="test",
        event="dispatch_completed",
        project="default",
        request_id="REQ-1",
        task={"request_id": "REQ-1", "short_id": "T-ALPHA", "alias": "hello"},
        stage="close",
        status="completed",
        error_code="",
        detail="",
    )

    room_dir = team_dir / "logs" / "rooms" / "O1"
    files = sorted(room_dir.glob("*.jsonl"))
    assert files, f"expected room files under {room_dir}"
    row = _read_last_jsonl(files[-1])
    assert row.get("room") == "O1"
    assert "[O1]" in str(row.get("text", ""))


def test_room_autopublish_routes_to_project_tf(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_ROOM_AUTOPUBLISH", "1")
    monkeypatch.setenv("AOE_ROOM_AUTOPUBLISH_ROUTE", "project-tf")

    team_dir = tmp_path / ".aoe-team"
    state = {
        "version": 1,
        "active": "default",
        "chat_sessions": {},
        "projects": {
            "default": {
                "name": "default",
                "project_alias": "O7",
            }
        },
    }

    gw.room_autopublish_event(
        team_dir=team_dir,
        manager_state=state,
        chat_id="test",
        event="dispatch_completed",
        project="default",
        request_id="REQ-2",
        task={"request_id": "REQ-2", "short_id": "T-BETA", "alias": "world"},
        stage="close",
        status="completed",
        error_code="",
        detail="",
    )

    room_dir = team_dir / "logs" / "rooms" / "O7" / "TF-BETA"
    files = sorted(room_dir.glob("*.jsonl"))
    assert files, f"expected room files under {room_dir}"
    row = _read_last_jsonl(files[-1])
    assert row.get("room") == "O7/TF-BETA"


def test_room_autopublish_respects_explicit_room_override(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_ROOM_AUTOPUBLISH", "1")
    monkeypatch.setenv("AOE_ROOM_AUTOPUBLISH_ROUTE", "project")

    team_dir = tmp_path / ".aoe-team"
    state = {
        "version": 1,
        "active": "default",
        "chat_sessions": {
            "test": {
                "room": "myroom",
            }
        },
        "projects": {
            "default": {
                "name": "default",
                "project_alias": "O1",
            }
        },
    }

    gw.room_autopublish_event(
        team_dir=team_dir,
        manager_state=state,
        chat_id="test",
        event="dispatch_completed",
        project="default",
        request_id="REQ-3",
        task={"request_id": "REQ-3", "short_id": "T-GAMMA", "alias": "override"},
        stage="close",
        status="completed",
        error_code="",
        detail="",
    )

    room_dir = team_dir / "logs" / "rooms" / "myroom"
    files = sorted(room_dir.glob("*.jsonl"))
    assert files, f"expected room files under {room_dir}"
    row = _read_last_jsonl(files[-1])
    assert row.get("room") == "myroom"

