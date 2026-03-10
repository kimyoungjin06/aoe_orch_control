#!/usr/bin/env python3
"""Run-loop idempotency and offset checkpoint tests."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


ROOT = Path(__file__).resolve().parents[2]
GW_DIR = ROOT / "scripts/gateway"
GW_FILE = GW_DIR / "aoe-telegram-gateway.py"

if str(GW_DIR) not in sys.path:
    sys.path.insert(0, str(GW_DIR))

_spec = importlib.util.spec_from_file_location("aoe_telegram_gateway_mod_runloop", GW_FILE)
assert _spec and _spec.loader
gw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gw)


def _args(tmp_path: Path) -> SimpleNamespace:
    return SimpleNamespace(
        state_file=tmp_path / "telegram_gateway_state.json",
        poll_timeout_sec=1,
        http_timeout_sec=1,
        allow_chat_ids={"111"},
        admin_chat_ids=set(),
        readonly_chat_ids=set(),
        deny_by_default=True,
        owner_chat_id="",
        owner_only=False,
        max_text_chars=3800,
        dry_run=True,
        verbose=False,
        once=True,
        team_dir=tmp_path,
    )


def test_run_loop_skips_duplicate_message_by_chat_message_id(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    args = _args(tmp_path)
    calls: list[tuple[str, str]] = []

    updates = [
        {
            "update_id": 1000,
            "message": {"message_id": 10, "chat": {"id": "111"}, "text": "hello"},
        },
        {
            "update_id": 1001,
            "message": {"message_id": 10, "chat": {"id": "111"}, "text": "hello"},
        },
    ]

    def fake_get_updates(*, token: str, offset: int, poll_timeout_sec: int, timeout_sec: int):
        return [u for u in updates if int(u["update_id"]) >= int(offset)]

    def fake_handle_text_message(*_args, **_kwargs):
        calls.append((_args[2], _args[3]))

    monkeypatch.setattr(gw, "tg_get_updates", fake_get_updates)
    monkeypatch.setattr(gw, "handle_text_message", fake_handle_text_message)

    rc = gw.run_loop(args, token="test-token")
    assert rc == 0
    assert calls == [("111", "hello")]

    state = json.loads(args.state_file.read_text(encoding="utf-8"))
    assert int(state["offset"]) == 1002
    assert int(state["acked_updates"]) == 2
    assert int(state["handled_messages"]) == 1
    assert int(state["duplicate_skipped"]) == 1
    assert int(state["processed"]) == 1
    assert "1000" in state.get("seen_update_ids", [])
    assert "1001" in state.get("seen_update_ids", [])
    assert "111:10" in state.get("seen_message_keys", [])


def test_run_loop_persists_offset_before_handler_side_effect(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    args = _args(tmp_path)
    call_count = 0

    updates = [
        {
            "update_id": 200,
            "message": {"message_id": 20, "chat": {"id": "111"}, "text": "first"},
        }
    ]

    def fake_get_updates(*, token: str, offset: int, poll_timeout_sec: int, timeout_sec: int):
        return [u for u in updates if int(u["update_id"]) >= int(offset)]

    def fake_handle_raises(*_args, **_kwargs):
        nonlocal call_count
        call_count += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(gw, "tg_get_updates", fake_get_updates)
    monkeypatch.setattr(gw, "handle_text_message", fake_handle_raises)

    rc1 = gw.run_loop(args, token="test-token")
    assert rc1 == 0
    assert call_count == 1

    state1 = json.loads(args.state_file.read_text(encoding="utf-8"))
    assert int(state1["offset"]) == 201
    assert int(state1["acked_updates"]) == 1
    assert int(state1["handled_messages"]) == 1
    assert int(state1["handler_errors"]) == 1
    assert int(state1["processed"]) == 1

    def fake_handle_ok(*_args, **_kwargs):
        nonlocal call_count
        call_count += 1

    monkeypatch.setattr(gw, "handle_text_message", fake_handle_ok)

    rc2 = gw.run_loop(args, token="test-token")
    assert rc2 == 0
    assert call_count == 1
    state2 = json.loads(args.state_file.read_text(encoding="utf-8"))
    assert int(state2["offset"]) == 201
    assert int(state2["acked_updates"]) == 1
    assert int(state2["handled_messages"]) == 1


def test_run_loop_owner_only_filters_by_sender_and_private_chat(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.owner_only = True
    args.owner_chat_id = "111"
    args.allow_chat_ids = set()
    args.deny_by_default = True

    calls: list[tuple[str, str]] = []

    updates = [
        {
            "update_id": 10,
            "message": {
                "message_id": 1,
                "chat": {"id": "111", "type": "private"},
                "from": {"id": "111"},
                "text": "hello",
            },
        },
        {
            "update_id": 11,
            "message": {
                "message_id": 2,
                "chat": {"id": "222", "type": "private"},
                "from": {"id": "222"},
                "text": "nope",
            },
        },
        {
            "update_id": 12,
            "message": {
                "message_id": 3,
                "chat": {"id": "-333", "type": "group"},
                "from": {"id": "111"},
                "text": "group-nope",
            },
        },
    ]

    def fake_get_updates(*, token: str, offset: int, poll_timeout_sec: int, timeout_sec: int):
        return [u for u in updates if int(u["update_id"]) >= int(offset)]

    def fake_handle_text_message(*_args, **_kwargs):
        calls.append((_args[2], _args[3]))

    monkeypatch.setattr(gw, "tg_get_updates", fake_get_updates)
    monkeypatch.setattr(gw, "handle_text_message", fake_handle_text_message)

    rc = gw.run_loop(args, token="test-token")
    assert rc == 0
    assert calls == [("111", "hello")]

    state = json.loads(args.state_file.read_text(encoding="utf-8"))
    assert int(state["offset"]) == 13
    assert int(state["acked_updates"]) == 3
    assert int(state["handled_messages"]) == 1
    assert int(state["unauthorized_skipped"]) == 2
