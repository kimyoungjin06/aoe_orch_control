#!/usr/bin/env python3
"""State helper regression tests for chat session persistence."""

from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import os
import pytest
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GW_DIR = ROOT / "scripts/gateway"
GW_FILE = GW_DIR / "aoe-telegram-gateway.py"
AUTO_SCHED_FILE = GW_DIR / "aoe-auto-scheduler.py"

if str(GW_DIR) not in sys.path:
    sys.path.insert(0, str(GW_DIR))

import aoe_tg_command_resolver as resolver
import aoe_tg_blocked_state as blocked_state
import aoe_tg_chat_aliases as chat_aliases
import aoe_tg_cli as cli_mod
import aoe_tg_chat_state as chat_state
import aoe_tg_exec_pipeline as exec_pipeline
import aoe_tg_exec_results as exec_results
import aoe_tg_gateway_events as gateway_events
import aoe_tg_gateway_aux as gateway_aux
import aoe_tg_gateway_batch_ops as gateway_batch_ops
import aoe_tg_gateway_state as gateway_state
import aoe_tg_management_acl as mgmt_acl
import aoe_tg_management_chat as mgmt_chat
import aoe_tg_management_handlers as mgmt_handlers
import aoe_tg_scheduler_control_handlers as scheduler_control
import aoe_tg_message_handler as message_handler
import aoe_tg_offdesk_flow as offdesk_flow
import aoe_tg_ops_policy as ops_policy
import aoe_tg_ops_view as ops_view
import aoe_tg_room_runtime as room_runtime
import aoe_tg_orch_registry as orch_registry
import aoe_tg_orch_roles as orch_roles
import aoe_tg_orch_responses as orch_responses
import aoe_tg_orch_overview_handlers as overview
import aoe_tg_orch_task_handlers as orch_task_handlers
import aoe_tg_parse as tg_parse
import aoe_tg_poll_loop as poll_loop
import aoe_tg_project_state as project_state
import aoe_tg_request_state as request_state
import aoe_tg_run_guards as run_guards
import aoe_tg_plan_pipeline as plan_pipeline
import aoe_tg_project_runtime as runtime_helpers
import aoe_tg_runtime_core as runtime_core
import aoe_tg_queue_engine as queue_engine
import aoe_tg_todo_policy as todo_policy
import aoe_tg_run_handlers as run_handlers
import aoe_tg_scheduler_handlers as sched
import aoe_tg_schema as schema
import aoe_tg_task_state as task_state
import aoe_tg_task_view as task_view
import aoe_tg_tf_backend as tf_backend
import aoe_tg_tf_backend_autogen as tf_backend_autogen
import aoe_tg_tf_backend_selection as tf_backend_selection
import aoe_tg_tf_event_schema as tf_event_schema
import aoe_tg_tf_backend_local as tf_backend_local
import aoe_tg_tf_exec as tf_exec
import aoe_tg_todo_handlers as todo_handlers
import aoe_tg_todo_state as todo_state
import aoe_tg_transport as transport

_spec = importlib.util.spec_from_file_location("aoe_telegram_gateway_mod", GW_FILE)
assert _spec and _spec.loader
gw = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gw)

_auto_spec = importlib.util.spec_from_file_location("aoe_auto_scheduler_mod", AUTO_SCHED_FILE)
assert _auto_spec and _auto_spec.loader
auto_sched = importlib.util.module_from_spec(_auto_spec)
_auto_spec.loader.exec_module(auto_sched)

COMPARE_FILE = ROOT / "scripts" / "experiments" / "autogen_core_compare.py"
_compare_spec = importlib.util.spec_from_file_location("aoe_autogen_compare_mod", COMPARE_FILE)
assert _compare_spec and _compare_spec.loader
autogen_compare = importlib.util.module_from_spec(_compare_spec)
_compare_spec.loader.exec_module(autogen_compare)


def _empty_state() -> dict:
    return gw.default_manager_state(ROOT, ROOT / ".aoe-team")


def test_default_manager_state_initializes_todo_proposals() -> None:
    state = _empty_state()
    project = state["projects"]["default"]

    assert project["todo_proposals"] == []
    assert project["todo_proposal_seq"] == 0


def test_set_default_mode_creates_chat_session_row() -> None:
    state = _empty_state()
    assert state.get("chat_sessions") == {}

    gw.set_default_mode(state, "939062873", "dispatch")

    assert gw.get_default_mode(state, "939062873") == "dispatch"
    assert state["chat_sessions"]["939062873"]["default_mode"] == "dispatch"


def test_set_pending_mode_creates_chat_session_row() -> None:
    state = _empty_state()
    assert state.get("chat_sessions") == {}

    gw.set_pending_mode(state, "939062873", "direct")

    assert gw.get_pending_mode(state, "939062873") == "direct"
    assert state["chat_sessions"]["939062873"]["pending_mode"] == "direct"


def test_set_chat_lang_creates_chat_session_row() -> None:
    state = _empty_state()
    assert state.get("chat_sessions") == {}

    gw.set_chat_lang(state, "939062873", "en")

    assert gw.get_chat_lang(state, "939062873", "ko") == "en"
    assert state["chat_sessions"]["939062873"]["lang"] == "en"


def test_set_chat_room_creates_chat_session_row() -> None:
    state = _empty_state()
    assert state.get("chat_sessions") == {}

    gw.set_chat_room(state, "939062873", "O1/TF-ALPHA")

    assert gw.get_chat_room(state, "939062873", "global") == "O1/TF-ALPHA"
    assert state["chat_sessions"]["939062873"]["room"] == "O1/TF-ALPHA"


def test_set_confirm_action_creates_chat_session_row() -> None:
    state = _empty_state()
    assert state.get("chat_sessions") == {}

    gw.set_confirm_action(state, chat_id="939062873", mode="dispatch", prompt="rm -rf /tmp/demo", risk="destructive_delete")

    action = gw.get_confirm_action(state, "939062873")
    assert action.get("mode") == "dispatch"
    assert "rm -rf /tmp/demo" in action.get("prompt", "")


def test_set_recent_and_selected_task_refs_create_chat_session_row() -> None:
    state = _empty_state()
    assert state.get("chat_sessions") == {}

    gw.set_chat_recent_task_refs(state, "939062873", "default", ["REQ-1", "REQ-2"])
    gw.set_chat_selected_task_ref(state, "939062873", "default", "REQ-2")

    refs = gw.get_chat_recent_task_refs(state, "939062873", "default")
    selected = gw.get_chat_selected_task_ref(state, "939062873", "default")
    assert refs[:2] == ["REQ-1", "REQ-2"]
    assert selected == "REQ-2"


def test_chat_aliases_module_matches_gateway_exports(tmp_path: Path) -> None:
    alias_file = tmp_path / "aliases.json"
    args_a = argparse.Namespace(
        chat_aliases_file=alias_file,
        chat_alias_cache={"2": "939062874"},
        dry_run=False,
        allow_chat_ids={"939062873", "939062874"},
        admin_chat_ids=set(),
        readonly_chat_ids=set(),
        deny_by_default=False,
    )
    args_b = copy.deepcopy(args_a)
    alias_file.write_text('{"1":"939062873"}\n', encoding="utf-8")

    assert gw.resolve_chat_aliases_file(tmp_path, "") == chat_aliases.resolve_chat_aliases_file(tmp_path, "")
    assert gw.load_chat_aliases(alias_file) == chat_aliases.load_chat_aliases(alias_file)
    assert gw.merged_chat_aliases(args_a) == chat_aliases.merged_chat_aliases(args_b)
    assert gw.find_chat_alias({"1": "939062873"}, "939062873") == chat_aliases.find_chat_alias({"1": "939062873"}, "939062873")
    assert gw.next_chat_alias({"1": "939062873"}) == chat_aliases.next_chat_alias({"1": "939062873"})
    assert gw.ensure_chat_alias(args_a, "939062875") == chat_aliases.ensure_chat_alias(args_b, "939062875")
    assert gw.ensure_chat_aliases(args_a, ["939062876", "939062877"]) == chat_aliases.ensure_chat_aliases(args_b, ["939062876", "939062877"])
    assert gw.resolve_chat_ref(args_a, "1") == chat_aliases.resolve_chat_ref(args_b, "1")
    assert gw.alias_table_summary(args_a) == chat_aliases.alias_table_summary(args_b)


def test_orch_roles_module_matches_gateway_exports(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Reviewer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Local-Dev").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Reviewer" / "AGENTS.md").write_text(
        "# AGENTS.md - Reviewer\n\n## Mission\nFind risks, regressions, and missing tests before merge.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Local-Dev" / "AGENTS.md").write_text(
        "# AGENTS.md - Local-Dev\n\n## Mission\nImplement code changes and fix application bugs.\n",
        encoding="utf-8",
    )
    (team_dir / "orchestrator.json").write_text(
        json.dumps(
            {
                "coordinator": {"role": "Orchestrator"},
                "agents": [{"role": "Reviewer"}, {"role": "Local-Dev"}],
            }
        ),
        encoding="utf-8",
    )

    assert gw.parse_roles_csv("Reviewer, Local-Dev,Reviewer") == orch_roles.parse_roles_csv("Reviewer, Local-Dev,Reviewer")
    assert gw.load_orchestrator_roles(team_dir) == orch_roles.load_orchestrator_roles(team_dir)
    assert gw.load_orchestrator_role_profiles(team_dir) == orch_roles.load_orchestrator_role_profiles(team_dir)
    assert gw.resolve_verifier_candidates("") == orch_roles.resolve_verifier_candidates("", default_verifier_roles=gw.DEFAULT_VERIFIER_ROLES)
    assert gw.ensure_verifier_roles(["Local-Dev"], ["Reviewer", "Local-Dev"], ["Reviewer"]) == orch_roles.ensure_verifier_roles(
        ["Local-Dev"],
        ["Reviewer", "Local-Dev"],
        ["Reviewer"],
    )
    assert gw.choose_auto_dispatch_roles(
        "로그인 버그를 수정하고 회귀 리스크도 같이 검토해줘.",
        available_roles=["Local-Dev", "Reviewer"],
        team_dir=team_dir,
    ) == orch_roles.choose_auto_dispatch_roles(
        "로그인 버그를 수정하고 회귀 리스크도 같이 검토해줘.",
        available_roles=["Local-Dev", "Reviewer"],
        team_dir=team_dir,
    )
    assert gw.available_worker_roles([]) == orch_roles.available_worker_roles([])


def test_gateway_state_module_matches_gateway_poll_and_replay_helpers(tmp_path: Path) -> None:
    state_payload = {
        "offset": 12,
        "processed": 3,
        gw.STATE_ACKED_UPDATES_KEY: 4,
        gw.STATE_HANDLED_MESSAGES_KEY: 3,
        gw.STATE_DUPLICATE_SKIPPED_KEY: 1,
        gw.STATE_EMPTY_SKIPPED_KEY: 1,
        gw.STATE_UNAUTHORIZED_SKIPPED_KEY: 0,
        gw.STATE_HANDLER_ERRORS_KEY: 1,
        gw.STATE_FAILED_QUEUE_KEY: [
            {
                "id": "abc",
                "at": "2026-03-11T12:00:00+0900",
                "chat_id": "939062873",
                "text": "retry me",
                "trace_id": "trace-1",
                "error_code": "E_INTERNAL",
                "error": "boom",
                "cmd": "run",
            }
        ],
        gw.STATE_SEEN_UPDATE_IDS_KEY: ["10", "11", "12"],
        gw.STATE_SEEN_MESSAGE_KEYS_KEY: ["939062873:1"],
        "updated_at": "2026-03-11T12:00:00+0900",
    }
    path = tmp_path / "gateway_state.json"
    path.write_text(json.dumps(state_payload), encoding="utf-8")

    gw_loaded = gw.load_state(path)
    mod_loaded = gateway_state.load_state(
        path,
        acked_updates_key=gw.STATE_ACKED_UPDATES_KEY,
        handled_messages_key=gw.STATE_HANDLED_MESSAGES_KEY,
        duplicate_skipped_key=gw.STATE_DUPLICATE_SKIPPED_KEY,
        empty_skipped_key=gw.STATE_EMPTY_SKIPPED_KEY,
        unauthorized_skipped_key=gw.STATE_UNAUTHORIZED_SKIPPED_KEY,
        handler_errors_key=gw.STATE_HANDLER_ERRORS_KEY,
        failed_queue_key=gw.STATE_FAILED_QUEUE_KEY,
        seen_update_ids_key=gw.STATE_SEEN_UPDATE_IDS_KEY,
        seen_message_keys_key=gw.STATE_SEEN_MESSAGE_KEYS_KEY,
        dedup_keep_limit=gw.dedup_keep_limit,
        failed_queue_keep_limit=gw.failed_queue_keep_limit,
        normalize_recent_tokens=gw.normalize_recent_tokens,
        normalize_failed_queue=gw.normalize_failed_queue,
    )
    assert gw_loaded == mod_loaded
    assert gw.normalize_recent_tokens(["1", "2", "2"], 5) == gateway_state.normalize_recent_tokens(["1", "2", "2"], 5)
    assert gw.message_dedup_key({"chat": {"id": "939062873"}, "message_id": 7}) == gateway_state.message_dedup_key({"chat": {"id": "939062873"}, "message_id": 7})
    assert gw.summarize_failed_queue(gw_loaded, "939062873") == gateway_state.summarize_failed_queue(
        mod_loaded,
        "939062873",
        failed_queue_for_chat=lambda st, cid: gateway_state.failed_queue_for_chat(
            st,
            cid,
            failed_queue_keep_limit=gw.failed_queue_keep_limit,
            normalize_failed_queue=gw.normalize_failed_queue,
            failed_queue_key=gw.STATE_FAILED_QUEUE_KEY,
        ),
        replay_usage=gw.REPLAY_USAGE,
    )
    assert gw.resolve_failed_queue_item(gw_loaded, "939062873", "latest") == gateway_state.resolve_failed_queue_item(
        mod_loaded,
        "939062873",
        "latest",
        failed_queue_for_chat=lambda st, cid: gateway_state.failed_queue_for_chat(
            st,
            cid,
            failed_queue_keep_limit=gw.failed_queue_keep_limit,
            normalize_failed_queue=gw.normalize_failed_queue,
            failed_queue_key=gw.STATE_FAILED_QUEUE_KEY,
        ),
    )

    path_a = tmp_path / "save_a.json"
    path_b = tmp_path / "save_b.json"
    gw.save_state(path_a, gw_loaded)
    gateway_state.save_state(
        path_b,
        mod_loaded,
        acked_updates_key=gw.STATE_ACKED_UPDATES_KEY,
        handled_messages_key=gw.STATE_HANDLED_MESSAGES_KEY,
        duplicate_skipped_key=gw.STATE_DUPLICATE_SKIPPED_KEY,
        empty_skipped_key=gw.STATE_EMPTY_SKIPPED_KEY,
        unauthorized_skipped_key=gw.STATE_UNAUTHORIZED_SKIPPED_KEY,
        handler_errors_key=gw.STATE_HANDLER_ERRORS_KEY,
        failed_queue_key=gw.STATE_FAILED_QUEUE_KEY,
        seen_update_ids_key=gw.STATE_SEEN_UPDATE_IDS_KEY,
        seen_message_keys_key=gw.STATE_SEEN_MESSAGE_KEYS_KEY,
        dedup_keep_limit=gw.dedup_keep_limit,
        failed_queue_keep_limit=gw.failed_queue_keep_limit,
        normalize_recent_tokens=gw.normalize_recent_tokens,
        normalize_failed_queue=gw.normalize_failed_queue,
    )
    assert gw.load_state(path_a) == gw.load_state(path_b)


def test_cli_module_matches_gateway_parser_defaults_and_args() -> None:
    argv = [
        "--simulate-text",
        "hello",
        "--simulate-chat-id",
        "test",
        "--allow-chat-ids",
        "1,2",
        "--chat-daily-cap",
        "5",
    ]

    assert vars(gw.build_parser().parse_args(argv)) == vars(cli_mod.build_parser(deps=gw.__dict__).parse_args(argv))


def test_cli_module_normalizes_main_args_like_gateway_flow(tmp_path: Path) -> None:
    project_root = tmp_path / "proj"
    team_dir = project_root / ".aoe-team"
    alias_file = tmp_path / "aliases.json"
    project_root.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    alias_file.write_text("{}", encoding="utf-8")

    base_args = argparse.Namespace(
        project_root=str(project_root),
        team_dir=None,
        state_file=None,
        manager_state_file="",
        chat_aliases_file=str(alias_file),
        instance_lock_file="",
        workspace_root="",
        owner_chat_id="939062873",
        owner_bootstrap_mode="Dispatch",
        default_lang="ko",
        default_reply_lang="en",
        default_report_level="LONG",
        allow_chat_ids="1,2",
        admin_chat_ids="2,3",
        readonly_chat_ids="3,4",
    )

    manual_args = copy.deepcopy(base_args)
    manual_args.project_root = gw.resolve_project_root(manual_args.project_root)
    manual_args.team_dir = gw.resolve_team_dir(manual_args.project_root, manual_args.team_dir)
    manual_args.state_file = gw.resolve_state_file(manual_args.project_root, manual_args.state_file)
    manual_args.manager_state_file = gw.resolve_manager_state_file(manual_args.team_dir, manual_args.manager_state_file)
    manual_args.chat_aliases_file = gw.resolve_chat_aliases_file(manual_args.team_dir, manual_args.chat_aliases_file)
    if str(manual_args.instance_lock_file or "").strip():
        manual_args.instance_lock_file = Path(str(manual_args.instance_lock_file)).expanduser().resolve()
    else:
        manual_args.instance_lock_file = (manual_args.team_dir / ".gateway.instance.lock").resolve()
    manual_args.workspace_root = gw.resolve_workspace_root(manual_args.workspace_root)
    manual_args.owner_chat_id = gw.normalize_owner_chat_id(manual_args.owner_chat_id)
    manual_args.owner_bootstrap_mode = (
        gw.normalize_mode_token(str(getattr(manual_args, "owner_bootstrap_mode", "") or "").strip())
        if str(getattr(manual_args, "owner_bootstrap_mode", "") or "").strip()
        else ""
    )
    if manual_args.owner_bootstrap_mode not in {"dispatch", "direct"}:
        manual_args.owner_bootstrap_mode = ""
    manual_args.default_lang = gw.normalize_chat_lang_token(manual_args.default_lang, gw.DEFAULT_UI_LANG) or gw.DEFAULT_UI_LANG
    manual_args.default_reply_lang = (
        gw.normalize_chat_lang_token(manual_args.default_reply_lang, gw.DEFAULT_REPLY_LANG) or gw.DEFAULT_REPLY_LANG
    )
    raw_default_report = gw.normalize_report_token(str(getattr(manual_args, "default_report_level", "") or "").strip())
    manual_args.default_report_level = (
        raw_default_report if raw_default_report in {"short", "normal", "long"} else gw.DEFAULT_REPORT_LEVEL
    )
    manual_args.allow_chat_ids = gw.parse_csv_set(manual_args.allow_chat_ids)
    manual_args.admin_chat_ids = gw.parse_csv_set(manual_args.admin_chat_ids)
    manual_args.readonly_chat_ids = gw.parse_csv_set(manual_args.readonly_chat_ids)
    manual_args.readonly_chat_ids = {
        value for value in manual_args.readonly_chat_ids if value not in manual_args.admin_chat_ids
    }
    manual_args.chat_alias_cache = gw.load_chat_aliases(manual_args.chat_aliases_file)

    cli_args = cli_mod.normalize_main_args(copy.deepcopy(base_args), deps=gw.__dict__)

    assert cli_args.project_root == manual_args.project_root
    assert cli_args.team_dir == manual_args.team_dir
    assert cli_args.state_file == manual_args.state_file
    assert cli_args.manager_state_file == manual_args.manager_state_file
    assert cli_args.chat_aliases_file == manual_args.chat_aliases_file
    assert cli_args.instance_lock_file == manual_args.instance_lock_file
    assert cli_args.workspace_root == manual_args.workspace_root
    assert cli_args.owner_chat_id == manual_args.owner_chat_id
    assert cli_args.owner_bootstrap_mode == manual_args.owner_bootstrap_mode == "dispatch"
    assert cli_args.default_lang == manual_args.default_lang
    assert cli_args.default_reply_lang == manual_args.default_reply_lang
    assert cli_args.default_report_level == manual_args.default_report_level
    assert cli_args.allow_chat_ids == manual_args.allow_chat_ids
    assert cli_args.admin_chat_ids == manual_args.admin_chat_ids
    assert cli_args.readonly_chat_ids == manual_args.readonly_chat_ids
    assert cli_args.chat_alias_cache == manual_args.chat_alias_cache


def test_poll_loop_module_matches_gateway_iter_and_simulation_helpers() -> None:
    updates = [
        {"update_id": 1, "message": {"chat": {"id": "1"}, "text": "hello"}},
        {"update_id": "bad", "message": {"chat": {"id": "2"}, "text": "skip"}},
        {"update_id": 2, "edited_message": {"chat": {"id": "3"}, "text": "skip"}},
        {"update_id": 3, "message": {"chat": {"id": "4"}, "text": "world"}},
    ]
    assert list(gw.iter_message_updates(updates)) == list(poll_loop.iter_message_updates(updates))

    calls_gw = []
    calls_mod = []

    def _fake_handler(call_log, args, token, chat_id, text, trace_id=""):
        call_log.append(
            {
                "token": token,
                "chat_id": chat_id,
                "text": text,
                "trace_id": trace_id,
                "dry_run": bool(args.dry_run),
            }
        )

    args_gw = argparse.Namespace(
        simulate_chat_id="939062873",
        simulate_text="hello",
        verbose=False,
        dry_run=False,
        simulate_live=False,
    )
    args_mod = copy.deepcopy(args_gw)

    original_handle_text_message = gw.handle_text_message
    try:
        gw.handle_text_message = lambda *a, **k: _fake_handler(calls_gw, *a, **k)
        gw.run_simulation(args_gw, "token-1")
    finally:
        gw.handle_text_message = original_handle_text_message

    poll_loop.run_simulation(
        args_mod,
        "token-1",
        handle_text_message=lambda *a, **k: _fake_handler(calls_mod, *a, **k),
    )

    assert calls_gw == calls_mod
    assert args_gw.dry_run is False
    assert args_mod.dry_run is False


def test_poll_loop_run_loop_processes_single_allowed_message(tmp_path: Path) -> None:
    args = argparse.Namespace(
        state_file=tmp_path / "gateway_state.json",
        poll_timeout_sec=1,
        http_timeout_sec=1,
        dry_run=True,
        verbose=False,
        once=True,
        allow_chat_ids=set(),
        admin_chat_ids=set(),
        readonly_chat_ids=set(),
        deny_by_default=False,
        owner_chat_id="",
        owner_only=False,
        max_text_chars=4000,
        team_dir=tmp_path,
    )
    handled = []
    saved_states = []
    updates = [{"update_id": 7, "message": {"chat": {"id": "939062873", "type": "private"}, "from": {"id": "939062873"}, "message_id": 11, "text": "hello"}}]

    rc = poll_loop.run_loop(
        args,
        "token-1",
        load_state=lambda _path: {},
        save_state=lambda _path, state: saved_states.append(copy.deepcopy(state)),
        dedup_keep_limit=lambda: 32,
        normalize_recent_tokens=lambda values, _limit: list(values or []),
        message_dedup_key=lambda msg: gw.message_dedup_key(msg),
        append_recent_token=lambda seq, token, _limit: seq.append(token) if token not in seq else None,
        tg_get_updates=lambda **_kwargs: updates,
        ensure_chat_allowed=lambda *_a, **_k: True,
        is_bootstrap_allowed_command=lambda _text: False,
        safe_tg_send_text=lambda **_kwargs: True,
        log_gateway_event=lambda **_kwargs: None,
        handle_text_message=lambda *_a, **_k: handled.append("ok"),
        preferred_command_prefix=lambda: "/",
        state_acked_updates_key=gw.STATE_ACKED_UPDATES_KEY,
        state_handled_messages_key=gw.STATE_HANDLED_MESSAGES_KEY,
        state_duplicate_skipped_key=gw.STATE_DUPLICATE_SKIPPED_KEY,
        state_empty_skipped_key=gw.STATE_EMPTY_SKIPPED_KEY,
        state_unauthorized_skipped_key=gw.STATE_UNAUTHORIZED_SKIPPED_KEY,
        state_handler_errors_key=gw.STATE_HANDLER_ERRORS_KEY,
        error_auth=gw.ERROR_AUTH,
    )

    assert rc == 0
    assert handled == ["ok"]
    assert saved_states
    assert saved_states[-1][gw.STATE_HANDLED_MESSAGES_KEY] == 1


def test_gateway_aux_module_matches_error_and_metrics_helpers(tmp_path: Path) -> None:
    err = RuntimeError("unknown orch project: demo")
    assert gw.classify_handler_error(err) == gateway_aux.classify_handler_error(
        err,
        error_timeout=gw.ERROR_TIMEOUT,
        error_command=gw.ERROR_COMMAND,
        error_gate=gw.ERROR_GATE,
        error_auth=gw.ERROR_AUTH,
        error_request=gw.ERROR_REQUEST,
        error_telegram=gw.ERROR_TELEGRAM,
        error_orch=gw.ERROR_ORCH,
        error_internal=gw.ERROR_INTERNAL,
    )
    assert gw.format_error_message("E_TEST", "failed", "/help", detail="token=secret") == gateway_aux.format_error_message(
        "E_TEST",
        "failed",
        "/help",
        detail="token=secret",
        mask_sensitive_text=gw.mask_sensitive_text,
    )

    team_dir = tmp_path / ".aoe-team"
    log_dir = team_dir / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / "gateway_events.jsonl").write_text(
        "\n".join(
            [
                json.dumps({"timestamp": gw.now_iso(), "event": "incoming_message", "trace_id": "t1", "latency_ms": 5}),
                json.dumps({"timestamp": gw.now_iso(), "event": "command_resolved", "trace_id": "t1", "status": "accepted", "latency_ms": 7}),
                json.dumps({"timestamp": gw.now_iso(), "event": "send_message", "trace_id": "t1", "status": "sent", "latency_ms": 9}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    assert gw.summarize_gateway_metrics(team_dir, "demo", hours=24) == gateway_aux.summarize_gateway_metrics(
        team_dir,
        "demo",
        hours=24,
        summarize_gateway_poll_state=gw.summarize_gateway_poll_state,
        parse_iso_ts=gw.parse_iso_ts,
        percentile=gw.percentile,
        error_internal=gw.ERROR_INTERNAL,
    )


def test_gateway_aux_module_matches_replay_list_path(tmp_path: Path) -> None:
    args = argparse.Namespace(state_file=tmp_path / "gateway_state.json")
    state = {
        gw.STATE_FAILED_QUEUE_KEY: [
            {
                "id": "abc",
                "at": "2026-03-11T12:00:00+0900",
                "chat_id": "939062873",
                "text": "/status",
                "trace_id": "trace-1",
                "error_code": "E_INTERNAL",
                "error": "boom",
                "cmd": "run",
            }
        ]
    }
    sent = []
    logged = []
    saved = []

    result = gateway_aux.handle_replay_command(
        args=args,
        token="token-1",
        chat_id="939062873",
        target="list",
        send=lambda body, **kwargs: sent.append((body, kwargs)),
        log_event=lambda **kwargs: logged.append(kwargs),
        load_state=lambda _path: copy.deepcopy(state),
        save_state=lambda _path, payload: saved.append(copy.deepcopy(payload)),
        normalize_failed_queue=gw.normalize_failed_queue,
        failed_queue_keep_limit=gw.failed_queue_keep_limit,
        state_failed_queue_key=gw.STATE_FAILED_QUEUE_KEY,
        summarize_failed_queue=gw.summarize_failed_queue,
        purge_failed_queue_for_chat=gw.purge_failed_queue_for_chat,
        resolve_failed_queue_item=gw.resolve_failed_queue_item,
        format_failed_queue_item_detail=gw.format_failed_queue_item_detail,
        remove_failed_queue_item=gw.remove_failed_queue_item,
        parse_command=gw.parse_command,
        handle_text_message=lambda *_a, **_k: None,
        preferred_command_prefix=gw.preferred_command_prefix,
        replay_usage=gw.REPLAY_USAGE,
    )

    assert result is True
    assert sent
    assert "replay queue: 1 pending" in sent[0][0].lower()
    assert saved
    assert not logged


def test_message_handler_module_handles_slash_only_hint(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    sent = []
    args = argparse.Namespace(
        slash_only=True,
        manager_state_file=tmp_path / "orch_manager_state.json",
        project_root=tmp_path,
        team_dir=team_dir,
        owner_bootstrap_mode="",
        dry_run=True,
        default_lang="ko",
        default_reply_lang="ko",
        default_report_level="normal",
        max_text_chars=4000,
        http_timeout_sec=1,
        verbose=False,
    )

    deps = {
        "mask_sensitive_text": lambda s: s,
        "ResolvedCommand": gw.ResolvedCommand,
        "RunTransitionState": gw.RunTransitionState,
        "load_manager_state": lambda *_a, **_k: _empty_state(),
        "ensure_default_project_registered": lambda *_a, **_k: None,
        "is_owner_chat": lambda *_a, **_k: False,
        "get_default_mode": lambda *_a, **_k: "",
        "set_default_mode": lambda *_a, **_k: None,
        "save_manager_state": lambda *_a, **_k: None,
        "get_manager_project": lambda *_a, **_k: (
            "default",
            {"team_dir": str(team_dir), "project_root": str(tmp_path)},
        ),
        "make_project_args": lambda base_args, entry, key="": argparse.Namespace(
            **vars(base_args),
            team_dir=Path(str(entry["team_dir"])),
            project_root=Path(str(entry["project_root"])),
            _aoe_project_key=key or "default",
        ),
        "log_gateway_event": lambda **_k: None,
        "room_autopublish_event": lambda **_k: None,
        "int_from_env": gw.int_from_env,
        "build_quick_reply_keyboard": lambda: {"keyboard": []},
        "safe_tg_send_text": lambda **kwargs: sent.append(kwargs) or True,
        "ERROR_TELEGRAM": gw.ERROR_TELEGRAM,
        "resolve_message_command": lambda **_k: gw.ResolvedCommand(),
        "get_pending_mode": lambda *_a, **_k: "",
        "clear_pending_mode": lambda *_a, **_k: None,
        "get_chat_lang": lambda *_a, **_k: "ko",
        "get_chat_report_level": lambda *_a, **_k: "normal",
        "DEFAULT_REPORT_LEVEL": gw.DEFAULT_REPORT_LEVEL,
        "preferred_command_prefix": lambda: "/",
        "ERROR_COMMAND": gw.ERROR_COMMAND,
    }

    message_handler.handle_text_message(
        args,
        "token-1",
        "939062873",
        "plain text",
        deps=deps,
    )

    assert sent
    assert "입력 형식" in sent[0]["text"]


def test_room_runtime_module_matches_gateway_route_and_gc_helpers(tmp_path: Path) -> None:
    assert gw.normalize_room_autopublish_route("project_tf") == room_runtime.normalize_room_autopublish_route(
        "project_tf",
        default_room_autopublish_route=gw.DEFAULT_ROOM_AUTOPUBLISH_ROUTE,
    )
    assert gw._room_autopublish_title("dispatch_failed") == room_runtime.room_autopublish_title("dispatch_failed")

    team_dir = tmp_path / ".aoe-team"
    room_dir = team_dir / "logs" / "rooms" / "demo"
    room_dir.mkdir(parents=True, exist_ok=True)
    old_file = room_dir / "2026-01-01.jsonl"
    old_file.write_text("{}\n", encoding="utf-8")

    removed = room_runtime.cleanup_room_logs(
        team_dir,
        force=True,
        room_retention_days=lambda: 1,
        today_key_local=lambda: "2026-03-11",
    )
    assert removed == 1
    assert not old_file.exists()


def test_gateway_batch_ops_module_matches_gateway_parse_helpers() -> None:
    assert gateway_batch_ops.parse_drain_args("5 force") == gw._parse_drain_args("5 force")
    assert gateway_batch_ops.parse_drain_args("all") == gw._parse_drain_args("all")
    assert gateway_batch_ops.parse_fanout_args("3 force") == gw._parse_fanout_args("3 force")
    assert gateway_batch_ops.parse_fanout_args("") == gw._parse_fanout_args("")


def test_offdesk_flow_module_matches_management_prefetch_and_state_helpers(tmp_path: Path, monkeypatch) -> None:
    previous = os.environ.get("AOE_TG_COMMAND_PREFIXES")
    os.environ["AOE_TG_COMMAND_PREFIXES"] = "!/"
    monkeypatch.setattr(offdesk_flow, "now_iso", lambda: "2026-03-11T10:00:00+0900")
    try:
        args = argparse.Namespace(team_dir=tmp_path / ".aoe-team", project_root=tmp_path)
        assert offdesk_flow.cmd_prefix() == mgmt_handlers._cmd_prefix() == "!"
        assert offdesk_flow.normalize_prefetch_token("recent_docs") == mgmt_handlers._normalize_prefetch_token("recent_docs")
        assert offdesk_flow.parse_replace_sync_flag(["replace-sync"]) == mgmt_handlers._parse_replace_sync_flag(["replace-sync"])
        assert offdesk_flow.prefetch_display("sync_recent", "3h", True) == mgmt_handlers._prefetch_display("sync_recent", "3h", True)
        assert offdesk_flow.status_report_level(["status", "long"], "short") == mgmt_handlers._status_report_level(["status", "long"], "short")
        assert offdesk_flow.auto_state_path(args, filename=mgmt_handlers.AUTO_STATE_FILENAME) == mgmt_handlers._auto_state_path(args)
        assert offdesk_flow.offdesk_state_path(args, filename=mgmt_handlers.OFFDESK_STATE_FILENAME) == mgmt_handlers._offdesk_state_path(args)

        state_a = tmp_path / "a.json"
        state_b = tmp_path / "b.json"
        payload = {"enabled": True, "chat_id": "939062873"}
        mgmt_handlers._save_auto_state(state_a, payload)
        offdesk_flow.save_auto_state(state_b, payload)
        assert mgmt_handlers._load_auto_state(state_a) == offdesk_flow.load_auto_state(state_b)
    finally:
        if previous is None:
            os.environ.pop("AOE_TG_COMMAND_PREFIXES", None)
        else:
            os.environ["AOE_TG_COMMAND_PREFIXES"] = previous


def test_room_runtime_module_builds_expected_autopublish_event() -> None:
    events = []
    manager_state = _empty_state()
    manager_state["projects"]["default"]["project_alias"] = "O1"

    room_runtime.room_autopublish_event(
        team_dir=ROOT / ".aoe-team",
        manager_state=manager_state,
        chat_id="939062873",
        event="dispatch_completed",
        project="default",
        request_id="REQ-1",
        task={"short_id": "T-001", "todo_id": "TODO-001"},
        stage="close",
        status="completed",
        error_code="",
        detail="done",
        room_autopublish_enabled=lambda: True,
        project_alias_for_key=gw.project_alias_for_key,
        get_chat_room=lambda *_a, **_k: gw.DEFAULT_ROOM_NAME,
        normalize_room_token=gw.normalize_room_token,
        room_autopublish_route=lambda: "project",
        int_from_env=gw.int_from_env,
        task_display_label=gw.task_display_label,
        append_room_event=lambda **kwargs: events.append(kwargs),
        now_iso=lambda: "2026-03-11T12:00:00+0900",
        default_room_name=gw.DEFAULT_ROOM_NAME,
        default_max_event_chars=gw.DEFAULT_MAX_EVENT_CHARS,
        default_max_file_bytes=gw.DEFAULT_MAX_FILE_BYTES,
    )

    assert events
    assert events[0]["room"] == "O1"
    assert events[0]["event"]["todo_id"] == "TODO-001"


def test_chat_state_module_matches_gateway_chat_session_exports() -> None:
    state_a = _empty_state()
    state_b = copy.deepcopy(state_a)

    gw.set_default_mode(state_a, "939062873", "dispatch")
    chat_state.set_default_mode(state_b, "939062873", "dispatch")
    gw.set_pending_mode(state_a, "939062873", "direct")
    chat_state.set_pending_mode(state_b, "939062873", "direct")
    gw.set_chat_lang(state_a, "939062873", "en")
    chat_state.set_chat_lang(state_b, "939062873", "en")
    gw.set_chat_report_level(state_a, "939062873", "long")
    chat_state.set_chat_report_level(state_b, "939062873", "long")
    gw.set_chat_room(state_a, "939062873", "O3/TF-ALPHA")
    chat_state.set_chat_room(state_b, "939062873", "O3/TF-ALPHA")
    gw.set_confirm_action(state_a, "939062873", "dispatch", "rm -rf /tmp/demo", risk="destructive_delete", orch="Twin")
    chat_state.set_confirm_action(state_b, "939062873", "dispatch", "rm -rf /tmp/demo", risk="destructive_delete", orch="Twin")
    gw.set_chat_recent_task_refs(state_a, "939062873", "Twin Paper", ["REQ-1", "REQ-2", "REQ-1"])
    chat_state.set_chat_recent_task_refs(state_b, "939062873", "Twin Paper", ["REQ-1", "REQ-2", "REQ-1"])
    gw.set_chat_selected_task_ref(state_a, "939062873", "Twin Paper", "REQ-2")
    chat_state.set_chat_selected_task_ref(state_b, "939062873", "Twin Paper", "REQ-2")

    assert gw.get_default_mode(state_a, "939062873") == chat_state.get_default_mode(state_b, "939062873")
    assert gw.get_pending_mode(state_a, "939062873") == chat_state.get_pending_mode(state_b, "939062873")
    assert gw.get_chat_lang(state_a, "939062873", "ko") == chat_state.get_chat_lang(state_b, "939062873", "ko")
    assert gw.get_chat_report_level(state_a, "939062873", "normal") == chat_state.get_chat_report_level(state_b, "939062873", "normal")
    assert gw.get_chat_room(state_a, "939062873", "global") == chat_state.get_chat_room(state_b, "939062873", "global")
    assert gw.get_confirm_action(state_a, "939062873").get("mode") == chat_state.get_confirm_action(state_b, "939062873").get("mode")
    assert gw.get_chat_recent_task_refs(state_a, "939062873", "Twin Paper") == chat_state.get_chat_recent_task_refs(state_b, "939062873", "Twin Paper")
    assert gw.get_chat_selected_task_ref(state_a, "939062873", "Twin Paper") == chat_state.get_chat_selected_task_ref(state_b, "939062873", "Twin Paper")
    assert gw.resolve_chat_task_ref(state_a, "939062873", "Twin Paper", "2") == chat_state.resolve_chat_task_ref(state_b, "939062873", "Twin Paper", "2")

    raw_row = {
        "pending_mode": "dispatch",
        "default_mode": "direct",
        "lang": "한국어",
        "report_level": "short",
        "room": "main",
        "confirm_action": {"mode": "dispatch", "prompt": "echo hi"},
        "recent_task_refs": {"Twin Paper": ["REQ-1", "REQ-1", "REQ-2"]},
        "selected_task_refs": {"Twin Paper": "REQ-2"},
    }
    assert gw.sanitize_chat_session_row(raw_row) == chat_state.sanitize_chat_session_row(raw_row)

    assert gw.clear_pending_mode(state_a, "939062873") == chat_state.clear_pending_mode(state_b, "939062873")
    assert gw.clear_default_mode(state_a, "939062873") == chat_state.clear_default_mode(state_b, "939062873")
    assert gw.clear_chat_report_level(state_a, "939062873") == chat_state.clear_chat_report_level(state_b, "939062873")
    assert gw.clear_confirm_action(state_a, "939062873") == chat_state.clear_confirm_action(state_b, "939062873")


def test_planning_stage_timeout_sec_caps_long_global_timeout() -> None:
    args = argparse.Namespace(orch_command_timeout_sec=900)

    assert gw.planning_stage_timeout_sec(args, "planner") == 240
    assert gw.planning_stage_timeout_sec(args, "critic") == 180
    assert gw.planning_stage_timeout_sec(args, "repair") == 240


def test_apply_success_first_prompt_fallbacks_for_latest_created_markdown_request() -> None:
    prompt, notes = run_handlers._apply_success_first_prompt_fallbacks(
        "각 프로젝트별로 5시간 내로 가장 늦게 생성된 10개 md를 살펴보고 업데이트 부탁해"
    )

    assert notes
    assert "[Execution Fallback Policy]" in prompt
    assert "birth time" in prompt
    assert "git first-seen/add time" in prompt
    assert "filesystem mtime" in prompt


def test_sync_salvage_creates_proposals_when_only_loose_followups_exist(tmp_path: Path) -> None:
    project_root = tmp_path / "DemoProject"
    team_dir = project_root / ".aoe-team"
    docs_dir = project_root / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "handoff.md").write_text(
        "# Research Handoff\n\n"
        "## Next steps\n"
        "- validate the overnight bootstrap path\n"
        "- review the queue status before morning standup\n",
        encoding="utf-8",
    )

    manager_state = gw.default_manager_state(project_root, team_dir)
    entry = manager_state["projects"]["default"]
    entry["project_root"] = str(project_root)
    entry["team_dir"] = str(team_dir)
    entry["display_name"] = "DemoProject"
    entry["project_alias"] = "O1"

    sent: list[tuple[str, dict]] = []

    def _send(body: str, **kwargs) -> bool:
        sent.append((body, kwargs))
        return True

    def _get_context(target: str | None):
        token = str(target or "default").strip().lower()
        if token in {"default", "o1", "demoproject"}:
            return "default", entry, argparse.Namespace(team_dir=team_dir, project_root=project_root)
        raise RuntimeError(f"unknown orch project: {target}")

    saved = {"n": 0}

    def _save_manager_state(*_args, **_kwargs) -> None:
        saved["n"] += 1

    result = sched.handle_scheduler_command(
        cmd="sync",
        args=argparse.Namespace(dry_run=False, manager_state_file=str(team_dir / "orch_manager_state.json")),
        manager_state=manager_state,
        chat_id="owner",
        chat_role="owner",
        orch_target=None,
        rest="salvage default since 5h",
        send=_send,
        get_context=_get_context,
        save_manager_state=_save_manager_state,
        now_iso=lambda: "2026-03-10T00:10:00+0900",
    )

    assert result == {"terminal": True}
    assert entry.get("todos") == []
    proposals = entry.get("todo_proposals") or []
    assert [row.get("summary") for row in proposals] == [
        "validate the overnight bootstrap path",
        "review the queue status before morning standup",
    ]
    assert saved["n"] >= 1
    assert sent
    body = sent[-1][0]
    assert "mode: salvage_docs" in body
    assert "- proposed: 2" in body


def test_ops_policy_summarizes_visible_and_hidden_projects() -> None:
    projects = {
        "default": {
            "project_alias": "O1",
            "display_name": "default",
            "system_project": True,
            "ops_hidden": True,
            "ops_hidden_reason": "internal fallback project",
        },
        "twinpaper": {
            "project_alias": "O2",
            "display_name": "TwinPaper",
            "ops_hidden": True,
            "ops_hidden_reason": "project on hold",
        },
        "nano": {
            "project_alias": "O3",
            "display_name": "Nano",
        },
    }

    scope = ops_policy.summarize_ops_scope(projects)

    assert scope["included"] == ["O3 Nano"]
    assert scope["excluded"] == [
        "O1 default (internal fallback project)",
        "O2 TwinPaper (project on hold)",
    ]


def test_ops_policy_list_projects_can_skip_paused_and_require_ready(tmp_path: Path) -> None:
    ready_root = tmp_path / "Ready"
    ready_team = ready_root / ".aoe-team"
    ready_team.mkdir(parents=True, exist_ok=True)
    (ready_team / "orchestrator.json").write_text("{}", encoding="utf-8")

    paused_root = tmp_path / "Paused"
    paused_team = paused_root / ".aoe-team"
    paused_team.mkdir(parents=True, exist_ok=True)
    (paused_team / "orchestrator.json").write_text("{}", encoding="utf-8")

    projects = {
        "ready": {
            "project_alias": "O3",
            "display_name": "Ready",
            "project_root": str(ready_root),
            "team_dir": str(ready_team),
            "paused": False,
        },
        "paused": {
            "project_alias": "O4",
            "display_name": "Paused",
            "project_root": str(paused_root),
            "team_dir": str(paused_team),
            "paused": True,
        },
        "broken": {
            "project_alias": "O5",
            "display_name": "Broken",
            "project_root": str(tmp_path / "Broken"),
            "team_dir": str(tmp_path / "Broken" / ".aoe-team"),
            "paused": False,
        },
    }

    visible_keys = [key for key, _entry in ops_policy.list_ops_projects(projects)]
    schedulable_keys = [
        key for key, _entry in ops_policy.list_ops_projects(projects, skip_paused=True, require_ready=True)
    ]

    assert visible_keys == ["ready", "paused", "broken"]
    assert schedulable_keys == ["ready"]


def test_ops_policy_builders_render_next_none_and_batch_finish() -> None:
    no_next = ops_policy.build_no_runnable_todo_message(
        focus_label="O4 Local_Map_Analysis",
        unready_rows=["- O3 (nano): missing orchestrator.json"],
    )
    batch = ops_policy.build_batch_finish_message(
        title="fanout finished",
        executed=2,
        reason="done",
        counters={"paused": 1, "unready": 0, "empty": 3, "busy": 0, "pending": 1, "missing_alias": 0},
        next_lines=["- /queue", "- /fanout"],
    )

    assert "locked project O4 Local_Map_Analysis has no runnable todo" in no_next
    assert "unready:" in no_next
    assert "- /next force" in no_next
    assert "fanout finished" in batch
    assert "- executed: 2" in batch
    assert "- skipped_paused: 1" in batch
    assert "- skipped_empty: 3" in batch
    assert "- reason: done" in batch


def test_ops_view_renders_snapshot_and_compact_scope_lines() -> None:
    entry = {
        "project_alias": "O4",
        "display_name": "Local_Map",
        "todos": [
            {"id": "TODO-1", "summary": "blocked item", "priority": "P1", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
            {"id": "TODO-2", "summary": "open item", "priority": "P2", "status": "open"},
        ],
        "pending_todo": {"todo_id": "TODO-2"},
        "last_sync_at": "2026-03-06T11:00:00+0900",
        "last_sync_mode": "scenario",
        "tasks": {
            "REQ-1": {
                "short_id": "T-101",
                "prompt": "Review Local Map backlog and summarize",
                "status": "running",
                "updated_at": "2026-03-06T12:00:00+0900",
            }
        },
    }
    projects = {"local_map": entry}

    snapshot = ops_view.render_project_snapshot_lines(key="local_map", entry=entry, locked=True)
    compact = ops_view.render_ops_scope_compact_lines(projects, detail_level="long")

    assert snapshot[0] == "project snapshot"
    assert "- project: O4 Local_Map [locked]" in snapshot
    assert "- todo: open=1 running=0 blocked=1 followup=1 pending=yes" in snapshot
    assert any("blocked_head: TODO-1 x2 [manual_followup]" in line for line in snapshot)
    assert compact
    assert compact[0].startswith("- O4 Local_Map: open=1 running=0 blocked=1 followup=1")
    assert any("next: P2 TODO-2 | open item" in line for line in compact)


def test_emit_planning_progress_logs_and_sends_chat_message() -> None:
    sent: list[tuple[str, dict]] = []
    logged: list[dict] = []

    def _send(body: str, **kwargs) -> bool:
        sent.append((body, kwargs))
        return True

    def _log_event(**kwargs) -> None:
        logged.append(kwargs)

    run_handlers._emit_planning_progress(
        phase="repair",
        key="local_map_analysis",
        send=_send,
        log_event=_log_event,
        emit_chat=True,
        detail="critic issues found; auto-replanning",
        attempt=1,
        total=2,
    )

    assert logged[-1]["event"] == "planning_repair"
    assert logged[-1]["status"] == "running"
    assert "attempt=1/2" in logged[-1]["detail"]
    assert sent
    assert "planning: auto-replan" in sent[-1][0]
    assert "- orch: local_map_analysis" in sent[-1][0]
    assert "- progress: 1/2" in sent[-1][0]
    assert sent[-1][1]["context"] == "planning-progress"


def test_compute_dispatch_plan_reports_progress_sequence() -> None:
    args = argparse.Namespace(
        task_planning=True,
        dry_run=False,
        plan_max_subtasks=4,
        plan_auto_replan=True,
        plan_replan_attempts=1,
        plan_block_on_critic=True,
    )
    phases: list[dict] = []
    critic_call_count = {"n": 0}

    def _build(*_args, **_kwargs):
        return {
            "summary": "plan",
            "subtasks": [{"id": "S1", "title": "build", "goal": "build", "owner_role": "DataEngineer", "acceptance": ["ok"]}],
        }

    def _critic(*_args, **_kwargs):
        critic_call_count["n"] += 1
        if critic_call_count["n"] == 1:
            return {"approved": False, "issues": ["fix this"], "recommendations": ["repair"]}
        return {"approved": True, "issues": [], "recommendations": []}

    def _repair(*_args, **_kwargs):
        return {
            "summary": "plan-fixed",
            "subtasks": [{"id": "S1", "title": "fixed", "goal": "fixed", "owner_role": "DataEngineer", "acceptance": ["ok"]}],
        }

    meta = run_handlers._compute_dispatch_plan(
        args=args,
        p_args=argparse.Namespace(),
        prompt="build something",
        dispatch_mode=True,
        run_control_mode="normal",
        run_source_task=None,
        selected_roles=[],
        available_roles=["DataEngineer", "Reviewer"],
        available_worker_roles=lambda roles: roles,
        normalize_task_plan_payload=lambda parsed, **_kwargs: parsed,
        build_task_execution_plan=_build,
        critique_task_execution_plan=_critic,
        critic_has_blockers=lambda critic: (not bool(critic.get("approved", True))) or bool(critic.get("issues") or []),
        repair_task_execution_plan=_repair,
        plan_roles_from_subtasks=lambda plan: ["DataEngineer"] if isinstance(plan, dict) else [],
        report_progress=lambda **kwargs: phases.append(kwargs),
    )

    assert meta.plan_gate_blocked is False
    assert [row["phase"] for row in phases] == ["planner", "critic", "repair", "critic", "ready"]
    assert phases[2]["attempt"] == 1
    assert phases[2]["total"] == 1


def test_plan_pipeline_module_matches_run_planning_exports() -> None:
    prompt = "각 프로젝트별로 5시간 내로 가장 늦게 생성된 10개 md를 살펴보고 업데이트 부탁해"
    assert run_handlers._apply_success_first_prompt_fallbacks(prompt) == plan_pipeline.apply_success_first_prompt_fallbacks(prompt)

    def _choose_roles(user_prompt: str, **_kwargs):
        if "analyze" in user_prompt.lower():
            return ["Analyst", "Reviewer"]
        return ["Reviewer"]

    run_mode = run_handlers._resolve_dispatch_mode_and_roles(
        run_force_mode=None,
        run_roles_override="",
        project_roles_csv="",
        auto_dispatch_enabled=True,
        prompt="analyze this change",
        choose_auto_dispatch_roles=_choose_roles,
        available_roles=["Analyst", "Reviewer"],
        team_dir=ROOT,
    )
    module_mode = plan_pipeline.resolve_dispatch_mode_and_roles(
        run_force_mode=None,
        run_roles_override="",
        project_roles_csv="",
        auto_dispatch_enabled=True,
        prompt="analyze this change",
        choose_auto_dispatch_roles=_choose_roles,
        available_roles=["Analyst", "Reviewer"],
        team_dir=ROOT,
    )
    assert run_mode == module_mode

    run_sent: list[tuple[str, dict]] = []
    module_sent: list[tuple[str, dict]] = []
    run_logged: list[dict] = []
    module_logged: list[dict] = []

    run_handlers._emit_planning_progress(
        phase="repair",
        key="local_map_analysis",
        send=lambda body, **kwargs: run_sent.append((body, kwargs)) or True,
        log_event=lambda **kwargs: run_logged.append(kwargs),
        emit_chat=True,
        detail="critic issues found; auto-replanning",
        attempt=1,
        total=2,
    )
    plan_pipeline.emit_planning_progress(
        phase="repair",
        key="local_map_analysis",
        send=lambda body, **kwargs: module_sent.append((body, kwargs)) or True,
        log_event=lambda **kwargs: module_logged.append(kwargs),
        emit_chat=True,
        detail="critic issues found; auto-replanning",
        attempt=1,
        total=2,
    )

    assert run_logged == module_logged
    assert run_sent == module_sent


def test_plan_pipeline_module_matches_run_compute_and_lineage_helpers() -> None:
    args = argparse.Namespace(
        task_planning=True,
        dry_run=False,
        plan_max_subtasks=4,
        plan_auto_replan=True,
        plan_replan_attempts=1,
        plan_block_on_critic=True,
    )
    critic_call_count = {"n": 0}

    def _build(*_args, **_kwargs):
        return {
            "summary": "plan",
            "subtasks": [{"id": "S1", "title": "build", "goal": "build", "owner_role": "DataEngineer", "acceptance": ["ok"]}],
        }

    def _critic(*_args, **_kwargs):
        critic_call_count["n"] += 1
        if critic_call_count["n"] == 1:
            return {"approved": False, "issues": ["fix this"], "recommendations": ["repair"]}
        return {"approved": True, "issues": [], "recommendations": []}

    def _repair(*_args, **_kwargs):
        return {
            "summary": "plan-fixed",
            "subtasks": [{"id": "S1", "title": "fixed", "goal": "fixed", "owner_role": "DataEngineer", "acceptance": ["ok"]}],
        }

    run_phases: list[dict] = []
    module_phases: list[dict] = []
    critic_has_blockers = lambda critic: (not bool(critic.get("approved", True))) or bool(critic.get("issues") or [])

    run_meta = run_handlers._compute_dispatch_plan(
        args=args,
        p_args=argparse.Namespace(),
        prompt="build something",
        dispatch_mode=True,
        run_control_mode="normal",
        run_source_task=None,
        selected_roles=[],
        available_roles=["DataEngineer", "Reviewer"],
        available_worker_roles=lambda roles: roles,
        normalize_task_plan_payload=lambda parsed, **_kwargs: parsed,
        build_task_execution_plan=_build,
        critique_task_execution_plan=_critic,
        critic_has_blockers=critic_has_blockers,
        repair_task_execution_plan=_repair,
        plan_roles_from_subtasks=lambda plan: ["DataEngineer"] if isinstance(plan, dict) else [],
        report_progress=lambda **kwargs: run_phases.append(kwargs),
    )

    critic_call_count["n"] = 0
    module_meta = plan_pipeline.compute_dispatch_plan(
        args=args,
        p_args=argparse.Namespace(),
        prompt="build something",
        dispatch_mode=True,
        run_control_mode="normal",
        run_source_task=None,
        selected_roles=[],
        available_roles=["DataEngineer", "Reviewer"],
        available_worker_roles=lambda roles: roles,
        normalize_task_plan_payload=lambda parsed, **_kwargs: parsed,
        build_task_execution_plan=_build,
        critique_task_execution_plan=_critic,
        critic_has_blockers=critic_has_blockers,
        repair_task_execution_plan=_repair,
        plan_roles_from_subtasks=lambda plan: ["DataEngineer"] if isinstance(plan, dict) else [],
        report_progress=lambda **kwargs: module_phases.append(kwargs),
    )

    assert run_meta == module_meta
    assert run_phases == module_phases

    task_a = {"request_id": "REQ-001", "context": {}}
    task_b = {"request_id": "REQ-001", "context": {}}
    source_a = {"request_id": "REQ-000", "context": {}}
    source_b = {"request_id": "REQ-000", "context": {}}
    notes_a: list[tuple[tuple, dict]] = []
    notes_b: list[tuple[tuple, dict]] = []

    kwargs = dict(
        task=task_a,
        plan_data={"subtasks": [{"id": "S1"}]},
        plan_critic={"approved": True, "issues": [], "recommendations": []},
        plan_roles=["DataEngineer"],
        plan_replans=[{"attempt": 1, "critic": "approved", "subtasks": 1}],
        plan_error="",
        critic_has_blockers=critic_has_blockers,
        lifecycle_set_stage=lambda *args, **kwargs: notes_a.append((args, kwargs)),
        run_control_mode="retry",
        run_source_request_id="REQ-000",
        run_source_task=source_a,
        req_id="REQ-001",
        now_iso=lambda: "2026-03-11T10:00:00+09:00",
    )
    run_handlers._apply_plan_and_lineage(**kwargs)

    kwargs["task"] = task_b
    kwargs["run_source_task"] = source_b
    kwargs["lifecycle_set_stage"] = lambda *args, **kwargs: notes_b.append((args, kwargs))
    plan_pipeline.apply_plan_and_lineage(**kwargs)

    assert task_a == task_b
    assert source_a == source_b
    assert notes_a == notes_b


def test_save_manager_state_syncs_investigations_registry_files(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project = state["projects"]["default"]
    project["project_alias"] = "O7"
    project["last_request_id"] = "REQ-B"
    project["tasks"] = {
        "REQ-A": {
            "short_id": "T-ALPHA",
            "prompt": "Build baseline",
            "status": "completed",
            "created_at": "2026-02-26T10:00:00+00:00",
            "updated_at": "2026-02-26T10:10:00+00:00",
            "control_mode": "project-orch",
        },
        "REQ-B": {
            "short_id": "T-BETA",
            "prompt": "Validate result",
            "status": "running",
            "source_request_id": "REQ-A",
            "created_at": "2026-02-26T11:00:00+00:00",
            "updated_at": "2026-02-26T11:05:00+00:00",
            "control_mode": "mother-orch",
        },
    }
    state_path = tmp_path / ".aoe-team" / "orch_manager_state.json"
    gw.save_manager_state(state_path, state)

    registry_root = tmp_path / "docs" / "investigations_mo" / "registry"
    project_lock = (registry_root / "project_lock.yaml").read_text(encoding="utf-8")
    tf_registry = (registry_root / "tf_registry.md").read_text(encoding="utf-8")
    handoff_index = (registry_root / "handoff_index.csv").read_text(encoding="utf-8")

    assert "active_project: O7" in project_lock
    assert "active_tf: TF-BETA" in project_lock
    assert "| TF-BETA | O7 | Validate result | running | - | mother-orch |" in tf_registry
    assert "H-O7-TF-BETA-REQB,O7,TF-ALPHA,TF-BETA,REQ-B" in handoff_index


def test_save_manager_state_syncs_registry_for_empty_tasks(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_path = tmp_path / ".aoe-team" / "orch_manager_state.json"
    gw.save_manager_state(state_path, state)

    registry_root = tmp_path / "docs" / "investigations_mo" / "registry"
    project_lock = (registry_root / "project_lock.yaml").read_text(encoding="utf-8")
    tf_registry = (registry_root / "tf_registry.md").read_text(encoding="utf-8")
    handoff_index = (registry_root / "handoff_index.csv").read_text(encoding="utf-8")

    assert "active_project: O1" in project_lock
    assert "active_tf: TF-ACTIVE" in project_lock
    # Default doc mode is "single": global TF registry uses a single report doc column.
    assert "| - | - | - | - | - | - | - | - | - |" in tf_registry
    assert handoff_index.strip() == "handoff_id,project_alias,from_tf,to_tf,task_id,created_at,doc,status"


def test_load_manager_state_preserves_todo_proposals_and_lineage_fields(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    state_path = team_dir / "orch_manager_state.json"
    team_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "active": "default",
        "projects": {
            "default": {
                "name": "default",
                "display_name": "default",
                "project_alias": "O1",
                "project_root": str(tmp_path),
                "team_dir": str(team_dir),
                "tasks": {},
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "follow up export",
                        "priority": "P2",
                        "status": "open",
                        "proposal_id": "PROP-001",
                        "proposal_kind": "followup",
                        "created_from_request_id": "REQ-123",
                        "created_from_todo_id": "TODO-000",
                    }
                ],
                "todo_seq": 1,
                "todo_proposals": [
                    {
                        "id": "PROP-001",
                        "summary": "follow up export",
                        "priority": "P2",
                        "kind": "followup",
                        "status": "open",
                        "reason": "result left one manual export step",
                        "confidence": 0.8,
                        "source_request_id": "REQ-123",
                        "source_todo_id": "TODO-000",
                        "source_task_label": "T-123",
                    }
                ],
                "todo_proposal_seq": 1,
            }
        },
    }
    state_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    loaded = gw.load_manager_state(state_path, tmp_path, team_dir)
    loaded_runtime = runtime_core.load_manager_state(
        state_path,
        tmp_path,
        team_dir,
        default_manager_state=gw.default_manager_state,
        now_iso=gw.now_iso,
        normalize_project_name=gw.normalize_project_name,
        sanitize_task_record=gw.sanitize_task_record,
        trim_project_tasks=gw.trim_project_tasks,
        normalize_task_alias_key=gw.normalize_task_alias_key,
        bool_from_json=gw.bool_from_json,
        normalize_project_alias=gw.normalize_project_alias,
        backfill_task_aliases=gw.backfill_task_aliases,
        ensure_project_aliases=gw.ensure_project_aliases,
        sanitize_project_lock_row=gw.sanitize_project_lock_row,
        sanitize_chat_session_row=gw.sanitize_chat_session_row,
    )
    project = loaded["projects"]["default"]

    assert loaded_runtime == loaded
    assert project["todo_proposal_seq"] == 1
    assert project["todo_proposals"][0]["source_request_id"] == "REQ-123"
    assert project["todo_proposals"][0]["confidence"] == 0.8
    assert project["todos"][0]["proposal_id"] == "PROP-001"
    assert project["todos"][0]["created_from_request_id"] == "REQ-123"


def test_extract_followup_todo_proposals_normalizes_json_payload() -> None:
    def _fake_run_codex_exec(args, prompt, timeout_sec=0):
        return json.dumps(
            {
                "proposals": [
                    {
                        "summary": "prepare deployment checklist",
                        "priority": "P1",
                        "kind": "handoff",
                        "reason": "release notes mention it is missing",
                        "confidence": 0.88,
                    },
                    {
                        "summary": "prepare deployment checklist",
                        "priority": "P3",
                        "kind": "note",
                        "reason": "duplicate",
                        "confidence": 2.0,
                    },
                ]
            },
            ensure_ascii=False,
        )

    original = gw.run_codex_exec
    gw.run_codex_exec = _fake_run_codex_exec
    try:
        rows = gw.extract_followup_todo_proposals(
            argparse.Namespace(orch_command_timeout_sec=120),
            "run release prep",
            {"replies": [{"role": "Local-Writer", "body": "release note draft is done; deployment checklist is still missing"}]},
            task={"todo_id": "TODO-001", "plan": {"summary": "release prep"}},
            reply_lang="en",
        )
    finally:
        gw.run_codex_exec = original

    assert len(rows) == 1
    assert rows[0]["summary"] == "prepare deployment checklist"
    assert rows[0]["priority"] == "P1"
    assert rows[0]["kind"] == "handoff"
    assert rows[0]["confidence"] == 0.88


def test_orch_responses_module_matches_gateway_wrappers() -> None:
    def _fake_run_codex_exec(args, prompt, timeout_sec=0):
        if "proposals" in prompt:
            return json.dumps(
                {
                    "proposals": [
                        {
                            "summary": "prepare deployment checklist",
                            "priority": "P1",
                            "kind": "handoff",
                            "reason": "release notes mention it is missing",
                            "confidence": 0.88,
                        }
                    ]
                },
                ensure_ascii=False,
            )
        if "\"verdict\"" in prompt or "execution critic" in prompt or "execution critic이다" in prompt:
            return json.dumps(
                {
                    "verdict": "retry",
                    "action": "replan",
                    "reason": "missing validation",
                    "fix": "add verifier pass",
                },
                ensure_ascii=False,
            )
        return "ok"

    args = argparse.Namespace(orch_command_timeout_sec=120)
    state = {"replies": [{"role": "Reviewer", "body": "need one more validation step"}]}
    task = {"todo_id": "TODO-001", "plan": {"summary": "release prep", "subtasks": [{"title": "draft"}]}}

    original = gw.run_codex_exec
    gw.run_codex_exec = _fake_run_codex_exec
    try:
        assert gw.run_orchestrator_direct(args, "hello", reply_lang="ko") == orch_responses.run_orchestrator_direct(
            args,
            "hello",
            reply_lang="ko",
            default_reply_lang=gw.DEFAULT_REPLY_LANG,
            normalize_chat_lang_token=gw.normalize_chat_lang_token,
            run_codex_exec=_fake_run_codex_exec,
        )
        assert gw.synthesize_orchestrator_response(args, "hello", state, reply_lang="ko") == orch_responses.synthesize_orchestrator_response(
            args,
            "hello",
            state,
            reply_lang="ko",
            default_reply_lang=gw.DEFAULT_REPLY_LANG,
            normalize_chat_lang_token=gw.normalize_chat_lang_token,
            run_codex_exec=_fake_run_codex_exec,
        )
        assert gw.critique_task_execution_result(
            args,
            "hello",
            state,
            task=task,
            attempt_no=1,
            max_attempts=3,
            reply_lang="ko",
        ) == orch_responses.critique_task_execution_result(
            args,
            "hello",
            state,
            task=task,
            attempt_no=1,
            max_attempts=3,
            reply_lang="ko",
            default_reply_lang=gw.DEFAULT_REPLY_LANG,
            normalize_chat_lang_token=gw.normalize_chat_lang_token,
            mask_sensitive_text=gw.mask_sensitive_text,
            run_codex_exec=_fake_run_codex_exec,
            parse_json_object_from_text=gw.parse_json_object_from_text,
            normalize_exec_critic_payload=gw.normalize_exec_critic_payload,
            now_iso=gw.now_iso,
        )
        assert gw.extract_followup_todo_proposals(
            args,
            "run release prep",
            state,
            task=task,
            reply_lang="ko",
        ) == orch_responses.extract_followup_todo_proposals(
            args,
            "run release prep",
            state,
            task=task,
            reply_lang="ko",
            default_reply_lang=gw.DEFAULT_REPLY_LANG,
            default_orch_command_timeout_sec=gw.DEFAULT_ORCH_COMMAND_TIMEOUT_SEC,
            normalize_chat_lang_token=gw.normalize_chat_lang_token,
            mask_sensitive_text=gw.mask_sensitive_text,
            run_codex_exec=_fake_run_codex_exec,
            parse_json_object_from_text=gw.parse_json_object_from_text,
        )
    finally:
        gw.run_codex_exec = original


def test_ensure_tf_exec_workspace_records_project_envelope(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_TF_EXEC_MODE", "inplace")

    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)

    args = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        _aoe_project_key="demo_proj",
        _aoe_project_alias="O9",
        _aoe_control_mode="retry",
        _aoe_source_request_id="REQ-000",
    )

    meta = gw.ensure_tf_exec_workspace(args, "REQ-001")
    tf_map = gw.load_tf_exec_map(team_dir)

    assert meta["project_key"] == "demo_proj"
    assert meta["project_alias"] == "O9"
    assert meta["project_root"] == str(project_root)
    assert meta["team_dir"] == str(team_dir)
    assert meta["control_mode"] == "retry"
    assert meta["source_request_id"] == "REQ-000"
    assert meta["tf_id"].startswith("TF-REQ-")
    assert tf_map["REQ-001"]["project_key"] == "demo_proj"
    assert tf_map["REQ-001"]["project_alias"] == "O9"


def test_sync_task_lifecycle_attaches_exec_context_and_updates_tf_exec_map(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)

    req_id = "REQ-CTX"
    run_dir = team_dir / "tf_runs" / req_id
    workdir = tmp_path / "work_ctx"
    run_dir.mkdir(parents=True, exist_ok=True)
    workdir.mkdir(parents=True, exist_ok=True)
    _write_tf_exec_map(team_dir, req_id, mode="inplace", workdir=workdir, run_dir=run_dir)

    entry = {
        "name": "demo_proj",
        "project_alias": "O9",
        "project_root": str(tmp_path),
        "team_dir": str(team_dir),
        "tasks": {},
        "task_alias_index": {},
        "task_seq": 0,
    }
    request_data = {
        "request_id": req_id,
        "role_states": [{"role": "Reviewer", "status": "done"}],
        "counts": {"assignments": 1, "replies": 1},
        "complete": True,
    }

    task = gw.sync_task_lifecycle(
        entry=entry,
        request_data=request_data,
        prompt="Validate output",
        mode="dispatch",
        selected_roles=["Reviewer"],
        verifier_roles=[],
        require_verifier=False,
        verifier_candidates=["Reviewer"],
    )
    assert isinstance(task, dict)

    context = task.get("context") or {}
    assert context["project_key"] == "demo_proj"
    assert context["project_alias"] == "O9"
    assert context["project_root"] == str(tmp_path)
    assert context["team_dir"] == str(team_dir)
    assert context["workdir"] == str(workdir)
    assert context["run_dir"] == str(run_dir)
    assert context["task_short_id"] == task["short_id"]
    assert context["tf_id"] == gw.task_short_to_tf_id(task["short_id"])

    tf_map = gw.load_tf_exec_map(team_dir)
    row = tf_map[req_id]
    assert row["project_key"] == "demo_proj"
    assert row["project_alias"] == "O9"
    assert row["task_short_id"] == task["short_id"]
    assert row["task_alias"] == task["alias"]
    assert row["tf_id"] == gw.task_short_to_tf_id(task["short_id"])


def test_sanitize_task_record_preserves_context_and_lineage_fields() -> None:
    task = gw.sanitize_task_record(
        {
            "short_id": "t-007",
            "alias": "demo-task",
            "control_mode": "retry",
            "source_request_id": "REQ-000",
            "plan": {"summary": "do work"},
            "exec_critic": {"verdict": "retry"},
            "context": {
                "project_key": "demo_proj",
                "project_alias": "o9",
                "project_root": "/tmp/project",
                "team_dir": "/tmp/project/.aoe-team",
                "workdir": "/tmp/project/work",
                "run_dir": "/tmp/project/.aoe-team/tf_runs/REQ-007",
            },
        },
        "REQ-007",
    )

    assert task["control_mode"] == "retry"
    assert task["source_request_id"] == "REQ-000"
    assert task["plan"]["summary"] == "do work"
    assert task["exec_critic"]["verdict"] == "retry"
    assert task["context"]["project_key"] == "demo_proj"
    assert task["context"]["project_alias"] == "O9"
    assert task["context"]["task_short_id"] == "T-007"
    assert task["context"]["tf_id"] == "TF-007"


def test_schema_normalizes_plan_and_exec_critic_payloads() -> None:
    plan = schema.normalize_task_plan_payload(
        {
            "summary": "demo",
            "subtasks": [
                {"title": "collect data", "role": "Local-Analyst", "acceptance": ["done"]},
                {"id": "S2", "goal": "write memo", "owner_role": "UnknownRole"},
            ],
        },
        user_prompt="analyze and summarize",
        workers=["Local-Analyst", "Local-Writer"],
        max_subtasks=2,
    )
    critic = schema.normalize_plan_critic_payload(
        {"approved": False, "issues": ["missing acceptance"], "recommendations": ["tighten output contract"]},
        max_items=5,
    )
    exec_critic = schema.normalize_exec_critic_payload(
        {"verdict": "재시도", "action": "", "reason": "증거 부족", "fix": "evidence 추가"},
        attempt_no=2,
        max_attempts=3,
        at="2026-03-10T10:00:00+0900",
    )

    assert plan["summary"] == "demo"
    assert plan["subtasks"][0]["owner_role"] == "Local-Analyst"
    assert plan["subtasks"][1]["owner_role"] == "UnknownRole"
    assert critic["approved"] is False
    assert critic["issues"] == ["missing acceptance"]
    assert exec_critic["verdict"] == "retry"
    assert exec_critic["action"] == "retry"
    assert exec_critic["reason"] == "증거 부족"


def test_sanitize_task_record_normalizes_nested_schema_fields() -> None:
    task = gw.sanitize_task_record(
        {
            "prompt": "do work",
            "plan": {
                "summary": " messy ",
                "subtasks": [{"goal": "collect data", "role": "Reviewer"}],
            },
            "plan_critic": {"approved": False, "issues": ["  missing acceptance  "], "recommendations": [" add checks "]},
            "plan_replans": [{"attempt": "2", "critic": "bad", "subtasks": "3"}],
            "plan_gate_passed": False,
            "exec_critic": {"verdict": "ok", "action": "retry", "reason": " all good ", "attempt": "2", "max_attempts": "4"},
        },
        "REQ-009",
    )

    assert task["plan"]["summary"] == "messy"
    assert task["plan"]["subtasks"][0]["owner_role"] == "Reviewer"
    assert task["plan_critic"]["issues"] == ["missing acceptance"]
    assert task["plan_replans"] == [{"attempt": 2, "critic": "unknown", "subtasks": 3}]
    assert task["plan_gate_reason"] == "missing acceptance"
    assert task["exec_critic"]["verdict"] == "success"
    assert task["exec_critic"]["action"] == "none"


def test_plan_critic_primary_issue_and_lifecycle_summary_use_schema_reason() -> None:
    issue = schema.plan_critic_primary_issue({"approved": False, "issues": ["  missing acceptance criteria  "]})
    assert issue == "missing acceptance criteria"

    summary = gw.summarize_task_lifecycle(
        "Demo",
        {
            "request_id": "REQ-101",
            "status": "failed",
            "mode": "dispatch",
            "roles": ["Local-Dev"],
            "verifier_roles": ["Reviewer"],
            "stages": {"planning": "failed"},
            "plan": {
                "summary": "demo plan",
                "subtasks": [{"id": "S1", "title": "collect data", "owner_role": "Local-Dev"}],
            },
            "plan_critic": {"approved": False, "issues": ["missing acceptance criteria"]},
            "plan_gate_passed": False,
            "plan_gate_reason": issue,
            "exec_critic": {
                "verdict": "retry",
                "action": "replan",
                "reason": "need a stricter acceptance contract",
                "attempt": 2,
                "max_attempts": 3,
                "at": "2026-03-10T10:00:00+0900",
            },
        },
    )

    assert "plan_gate: blocked" in summary
    assert "plan_gate_reason: missing acceptance criteria" in summary
    assert "exec_critic: retry (action=replan)" in summary
    assert "exec_attempts: 2/3" in summary
    assert "exec_reason: need a stricter acceptance contract" in summary


def test_blocked_state_helpers_clear_and_promote_manual_followup() -> None:
    item = {
        "status": "running",
        "blocked_count": 1,
        "blocked_bucket": "manual_followup",
        "blocked_reason": "needs operator decision",
        "blocked_alerted_at": "2026-03-10T09:00:00+0900",
        "current_request_id": "REQ-9",
    }

    outcome = blocked_state.apply_todo_execution_outcome(
        item,
        task_status="failed",
        exec_verdict="retry",
        exec_reason="missing evidence",
        req_id="REQ-10",
        now="2026-03-10T10:00:00+0900",
        task_label="T-010 demo",
        manual_followup_threshold=2,
    )

    assert outcome == "blocked"
    assert item["status"] == "blocked"
    assert item["blocked_count"] == 2
    assert item["blocked_bucket"] == "manual_followup"
    assert item["blocked_reason"] == "missing evidence"

    had_followup = blocked_state.clear_blocked_meta(item, clear_current_request=True)
    assert had_followup is True
    assert "blocked_bucket" not in item
    assert "blocked_reason" not in item
    assert "current_request_id" not in item


def test_blocked_state_helpers_render_manual_followup_summary() -> None:
    rows = [
        {"id": "TODO-1", "status": "blocked", "blocked_bucket": "manual_followup", "blocked_reason": "need review", "blocked_count": 2, "updated_at": "2026-03-10T10:00:00+0900"},
        {"id": "TODO-2", "status": "open"},
        {"id": "TODO-3", "status": "blocked", "blocked_reason": "later", "blocked_count": 1, "updated_at": "2026-03-10T11:00:00+0900"},
    ]

    assert blocked_state.manual_followup_indices(rows, limit=3) == [1]
    assert blocked_state.blocked_bucket_count(rows, "manual_followup") == 1
    head = blocked_state.blocked_head_summary(rows)
    assert head["id"] == "TODO-1"
    assert head["bucket"] == "manual_followup"
    assert head["reason"] == "need review"


def test_task_view_module_matches_gateway_lifecycle_summary() -> None:
    task = {
        "request_id": "REQ-202",
        "short_id": "T-202",
        "alias": "demo-task",
        "status": "running",
        "mode": "dispatch",
        "roles": ["Local-Dev", "Reviewer"],
        "verifier_roles": ["Reviewer"],
        "stages": {"planning": "done", "execution": "running"},
        "context": {
            "project_key": "demo_proj",
            "project_alias": "O9",
            "task_short_id": "T-202",
            "source_request_id": "REQ-101",
            "control_mode": "retry",
        },
        "plan": {"summary": "demo", "subtasks": [{"id": "S1", "title": "collect", "owner_role": "Local-Dev"}]},
        "plan_critic": {"approved": False, "issues": ["missing acceptance"]},
        "plan_gate_passed": False,
        "plan_gate_reason": "missing acceptance",
        "exec_critic": {"verdict": "retry", "action": "replan", "reason": "need evidence", "attempt": 1, "max_attempts": 3, "at": "2026-03-10T10:00:00+0900"},
        "result": {"assignments": 1, "replies": 0, "complete": False, "pending_roles": ["Local-Dev"]},
        "history": [{"at": "2026-03-10T10:00:00+0900", "stage": "planning", "status": "done", "note": "critic issues"}],
    }

    expected = gw.summarize_task_lifecycle("Demo", task)
    actual = task_view.summarize_task_lifecycle("Demo", task)
    assert actual == expected


def test_task_state_module_matches_gateway_alias_and_monitor_helpers() -> None:
    entry = {
        "tasks": {
            "REQ-1": {
                "request_id": "REQ-1",
                "prompt": "collect data and write memo",
                "status": "running",
                "stage": "execution",
                "roles": ["Local-Dev", "Reviewer"],
                "updated_at": "2026-03-10T10:00:00+0900",
                "created_at": "2026-03-10T09:00:00+0900",
            }
        },
        "task_alias_index": {},
        "task_seq": 0,
    }

    task_state.backfill_task_aliases(entry)
    assert gw.resolve_task_request_id(entry, "T-001") == task_state.resolve_task_request_id(entry, "T-001")
    assert gw.resolve_task_request_id(entry, "collect-data-write-memo") == task_state.resolve_task_request_id(
        entry, "collect-data-write-memo"
    )

    gw_summary = gw.summarize_task_monitor("Demo", entry, limit=5)
    state_summary = task_state.summarize_task_monitor(
        "Demo",
        entry,
        limit=5,
        normalize_task_status=gw.normalize_task_status,
        dedupe_roles=gw.dedupe_roles,
        task_display_label=gw.task_display_label,
        lifecycle_stages=gw.LIFECYCLE_STAGES,
    )
    assert state_summary == gw_summary


def test_task_state_snapshot_and_sync_match_gateway() -> None:
    request_data = {
        "request_id": "REQ-301",
        "role_states": [
            {"role": "Local-Dev", "status": "done"},
            {"role": "Reviewer", "status": "pending"},
        ],
        "counts": {"assignments": 2, "replies": 1},
        "complete": False,
    }
    assert task_state.extract_request_snapshot(request_data, dedupe_roles=gw.dedupe_roles) == gw.extract_request_snapshot(
        request_data
    )

    entry_a = {"name": "demo_proj", "project_alias": "O9", "project_root": "/tmp/demo", "team_dir": "/tmp/demo/.aoe-team", "tasks": {}, "task_alias_index": {}, "task_seq": 0}
    entry_b = copy.deepcopy(entry_a)

    task_a = gw.sync_task_lifecycle(
        entry=entry_a,
        request_data=request_data,
        prompt="Validate output",
        mode="dispatch",
        selected_roles=["Local-Dev", "Reviewer"],
        verifier_roles=["Reviewer"],
        require_verifier=True,
        verifier_candidates=["Reviewer"],
    )
    task_b = task_state.sync_task_lifecycle(
        entry_b,
        request_data,
        prompt="Validate output",
        mode="dispatch",
        selected_roles=["Local-Dev", "Reviewer"],
        verifier_roles=["Reviewer"],
        require_verifier=True,
        verifier_candidates=["Reviewer"],
        dedupe_roles=gw.dedupe_roles,
        ensure_task_record=gw.ensure_task_record,
        lifecycle_set_stage=gw.lifecycle_set_stage,
        normalize_task_status=gw.normalize_task_status,
        sync_task_exec_context=lambda entry, task: task.get("context", {}) if isinstance(task, dict) else {},
    )

    assert task_a is not None
    assert task_b is not None
    assert task_b["status"] == task_a["status"]
    assert task_b["roles"] == task_a["roles"]
    assert task_b["verifier_roles"] == task_a["verifier_roles"]
    assert task_b["result"] == task_a["result"]
    assert task_b["stages"] == task_a["stages"]


def test_task_state_sanitize_task_record_matches_gateway(monkeypatch) -> None:
    monkeypatch.setattr(gw, "now_iso", lambda: "2026-03-11T10:00:00+0900")
    raw_task = {
        "mode": "weird",
        "prompt": "  Review the output  ",
        "roles": ["Local-Dev", "Reviewer", "Local-Dev"],
        "verifier_roles": ["Reviewer", "Reviewer"],
        "require_verifier": 1,
        "stages": {"planning": "complete", "execution": "active", "garbage": "bad"},
        "stage": "unknown",
        "history": [
            {"at": "", "stage": "planning", "status": "success", "note": "ready"},
            {"at": "", "stage": "bad", "status": "oops"},
        ],
        "status": "done",
        "short_id": "t-008",
        "alias": " review-output ",
        "control_mode": "DISPATCH",
        "source_request_id": "REQ-00123456789",
        "retry_of": "REQ-0001",
        "replan_of": "REQ-0002",
        "retry_children": ["REQ-010", "REQ-010", ""],
        "replan_children": ["REQ-020", "REQ-021", "REQ-020"],
        "initiator_chat_id": "939062873",
        "todo_id": "TODO-004",
        "todo_priority": "p2",
        "todo_status": "RUNNING",
        "plan": {
            "summary": "demo",
            "meta": {"worker_roles": ["Reviewer", "Local-Dev", "Reviewer"]},
            "subtasks": [{"id": "S1", "title": "check", "owner_role": "Reviewer"}],
        },
        "plan_critic": {"approved": False, "issues": ["missing acceptance"]},
        "plan_roles": ["Reviewer", "Local-Dev", "Reviewer"],
        "plan_replans": [{"attempt": "2", "critic": "retry", "subtasks": "3"}],
        "plan_gate_passed": False,
        "exec_critic": {
            "verdict": "success",
            "action": "none",
            "attempt": 1,
            "max_attempts": 3,
            "at": "",
        },
        "context": {"project_key": "demo"},
    }

    expected = gw.sanitize_task_record(copy.deepcopy(raw_task), "REQ-777")
    actual = task_state.sanitize_task_record(
        copy.deepcopy(raw_task),
        "REQ-777",
        dedupe_roles=gw.dedupe_roles,
        lifecycle_stages=gw.LIFECYCLE_STAGES,
        normalize_stage_status=gw.normalize_stage_status,
        normalize_task_status=gw.normalize_task_status,
        now_iso=gw.now_iso,
        history_limit=gw.DEFAULT_TASK_HISTORY_LIMIT,
        normalize_task_plan_schema=gw.normalize_task_plan_schema,
        normalize_plan_critic_payload=gw.normalize_plan_critic_payload,
        normalize_plan_replans_payload=gw.normalize_plan_replans_payload,
        plan_critic_primary_issue=gw.plan_critic_primary_issue,
        normalize_exec_critic_payload=gw.normalize_exec_critic_payload,
        build_task_context=gw.build_task_context,
    )

    assert actual == expected


def test_tf_worker_specs_use_request_scoped_session_and_logs(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)

    args = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        orch_command_timeout_sec=900,
        aoe_orch_bin="/usr/bin/aoe-orch",
    )

    specs = gw.tf_worker_specs(args, "REQ-123", ["Reviewer"], startup_timeout_sec=120)

    assert len(specs) == 1
    spec = specs[0]
    assert spec["session"].startswith("tfw_req-123_reviewer")
    assert "aoe-tf-worker-session.py" in spec["shell"]
    assert "scripts/team/runtime/worker_codex_handler.sh" in spec["shell"]
    assert str(team_dir / "telegram.env") not in spec["shell"] or ". " in spec["shell"]
    assert str(team_dir / "tf_runs" / "REQ-123" / "logs" / "worker_reviewer.console.log") in spec["log_file"]


def test_resolve_dispatch_roles_from_preview_reads_dispatch_plan(monkeypatch) -> None:
    args = argparse.Namespace(
        aoe_orch_bin="/usr/bin/aoe-orch",
        project_root=Path("/tmp/project"),
        team_dir=Path("/tmp/project/.aoe-team"),
        orch_poll_sec=2.0,
        orch_command_timeout_sec=900,
    )

    class Proc:
        returncode = 0
        stdout = json.dumps(
            {
                "request_id": "REQ-1",
                "dispatch_plan": [
                    {"role": "DataEngineer", "title": "A"},
                    {"role": "Reviewer", "title": "B"},
                ],
            }
        )
        stderr = ""

    monkeypatch.setattr(gw, "run_command", lambda cmd, env, timeout_sec: Proc())
    roles = gw.resolve_dispatch_roles_from_preview(
        args,
        "Check quality",
        request_id="REQ-1",
        roles_override="",
        priority="P2",
        timeout_sec=120,
    )
    assert roles == ["DataEngineer", "Reviewer"]


def test_choose_auto_dispatch_roles_prefers_reviewer_for_simple_check(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Reviewer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "DataEngineer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Reviewer" / "AGENTS.md").write_text(
        "# AGENTS.md - Reviewer\n\n## Mission\nFind risks, regressions, and missing tests before merge.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "DataEngineer" / "AGENTS.md").write_text(
        "# AGENTS.md - DataEngineer\n\n## Mission\nOwn data ingestion, ETL quality, and schema consistency.\n",
        encoding="utf-8",
    )

    roles = gw.choose_auto_dispatch_roles(
        "현재 프로젝트 루트에서 .github가 있는지만 확인하고 한 문장으로 답해줘.",
        available_roles=["DataEngineer", "Reviewer"],
        team_dir=team_dir,
    )

    assert roles == ["Reviewer"]


def test_choose_auto_dispatch_roles_builds_multi_role_tf_from_prompt_mix(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Local-Dev").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Reviewer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Local-Writer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Local-Dev" / "AGENTS.md").write_text(
        "# AGENTS.md - Local-Dev\n\n## Mission\nImplement code changes and fix application bugs.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Reviewer" / "AGENTS.md").write_text(
        "# AGENTS.md - Reviewer\n\n## Mission\nFind risks, regressions, and missing tests before merge.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Local-Writer" / "AGENTS.md").write_text(
        "# AGENTS.md - Local-Writer\n\n## Mission\nWrite concise documents and reports.\n",
        encoding="utf-8",
    )

    roles = gw.choose_auto_dispatch_roles(
        "로그인 버그를 수정하고 회귀 리스크도 같이 검토해줘.",
        available_roles=["Local-Dev", "Reviewer", "Local-Writer"],
        team_dir=team_dir,
    )

    assert set(roles) == {"Local-Dev", "Reviewer"}
    assert len(roles) == 2


def test_choose_auto_dispatch_roles_picks_local_analyst_for_analysis_prompt(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Local-Analyst").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Reviewer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Local-Analyst" / "AGENTS.md").write_text(
        "# AGENTS.md - Local-Analyst\n\n## Mission\nInvestigate project state, compare options, and surface defensible recommendations.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Reviewer" / "AGENTS.md").write_text(
        "# AGENTS.md - Reviewer\n\n## Mission\nFind risks, regressions, and missing tests before merge.\n",
        encoding="utf-8",
    )

    roles = gw.choose_auto_dispatch_roles(
        "현재 구조를 조사하고 두 방식의 트레이드오프를 비교해서 추천안을 정리해줘.",
        available_roles=["Local-Analyst", "Reviewer"],
        team_dir=team_dir,
    )

    assert roles == ["Local-Analyst"]


def test_choose_auto_dispatch_roles_prefers_local_writer_for_doc_request(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Local-Writer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Local-Dev").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Local-Writer" / "AGENTS.md").write_text(
        "# AGENTS.md - Local-Writer\n\n## Mission\nWrite concise project documents, summaries, and handoff notes that people can use immediately.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Local-Dev" / "AGENTS.md").write_text(
        "# AGENTS.md - Local-Dev\n\n## Mission\nImplement code changes, debug failures, and return verifiable fixes.\n",
        encoding="utf-8",
    )

    roles = gw.choose_auto_dispatch_roles(
        "배포 전에 문서를 정리하고 요약 보고서를 작성해줘.",
        available_roles=["Local-Writer", "Local-Dev"],
        team_dir=team_dir,
    )

    assert roles == ["Local-Writer"]


def test_available_worker_roles_uses_expanded_default_pool() -> None:
    assert gw.available_worker_roles([]) == [
        "DataEngineer",
        "Reviewer",
        "Local-Dev",
        "Local-Writer",
        "Local-Analyst",
    ]


def test_finalize_tf_exec_meta_marks_failed_roles_and_syncs_run_meta(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    req_id = "REQ-FAIL"
    run_dir = team_dir / "tf_runs" / req_id
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "meta.json").write_text(json.dumps({"request_id": req_id, "status": "running"}) + "\n", encoding="utf-8")

    tf_map = {
        req_id: {
            "request_id": req_id,
            "run_dir": str(run_dir),
            "status": "running",
        }
    }
    gw.save_tf_exec_map(team_dir, tf_map)

    gw.finalize_tf_exec_meta(
        team_dir,
        req_id,
        {
            "request_id": req_id,
            "complete": True,
            "roles": [{"role": "Reviewer", "status": "failed"}],
            "reply_messages": [],
        },
    )

    tf_row = gw.load_tf_exec_map(team_dir)[req_id]
    run_meta = json.loads((run_dir / "meta.json").read_text(encoding="utf-8"))

    assert tf_row["status"] == "failed"
    assert tf_row["failed_role_count"] == 1
    assert run_meta["status"] == "failed"
    assert run_meta["failed_role_count"] == 1


def test_finalize_request_reply_messages_marks_only_unresolved(monkeypatch, tmp_path: Path) -> None:
    args = argparse.Namespace(
        aoe_team_bin="aoe-team",
        team_dir=tmp_path / ".aoe-team",
    )
    args.team_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        gw,
        "run_request_query",
        lambda _args, _rid: {
            "request_id": "REQ-1",
            "reply_messages": [
                {"id": "m_sent", "from": "Reviewer", "status": "sent"},
                {"id": "m_done", "from": "DataEngineer", "status": "done"},
            ],
        },
    )

    calls: list[tuple[str, str, str]] = []

    def _fake_done(_args, message_id: str, actor: str, note: str) -> tuple[bool, str]:
        calls.append((message_id, actor, note))
        return True, f"done {message_id}"

    monkeypatch.setattr(gw, "run_message_done", _fake_done)

    result = gw.finalize_request_reply_messages(args, "REQ-1")

    assert result["targets"] == 1
    assert result["done"] == ["Reviewer:m_sent:sent"]
    assert "DataEngineer:m_done:done" in result["skipped"]
    assert calls == [("m_sent", "Orchestrator", "gateway integrated reply into final response")]


def test_request_state_module_matches_gateway_request_helpers(monkeypatch, tmp_path: Path) -> None:
    args = argparse.Namespace(
        aoe_team_bin="aoe-team",
        team_dir=tmp_path / ".aoe-team",
        state_file=tmp_path / "gateway_state.json",
    )
    args.team_dir.mkdir(parents=True, exist_ok=True)

    class Proc:
        returncode = 0
        stdout = json.dumps({"request_id": "REQ-1", "counts": {"messages": 1, "assignments": 1, "replies": 0}})
        stderr = ""

    monkeypatch.setattr(gw, "run_command", lambda cmd, env, timeout_sec: Proc())

    assert gw.run_request_query(args, "REQ-1") == request_state.run_request_query(
        args,
        "REQ-1",
        run_command=lambda cmd, env, timeout_sec: Proc(),
    )

    task = {"request_id": "REQ-1", "short_id": "T-001", "alias": "demo", "status": "running"}
    state = {
        "request_id": "REQ-1",
        "complete": False,
        "counts": {"messages": 1, "assignments": 1, "replies": 0},
        "roles": [{"role": "Reviewer", "status": "pending", "message_id": "m-1"}],
        "unresolved_roles": ["Reviewer"],
    }

    assert gw.summarize_request_state(state, task=task) == request_state.summarize_request_state(
        state,
        task=task,
        task_display_label=gw.task_display_label,
    )
    assert gw.render_run_response(state, task=task, report_level="short") == request_state.render_run_response(
        state,
        task=task,
        report_level="short",
        default_report_level=gw.DEFAULT_REPORT_LEVEL,
        task_display_label=gw.task_display_label,
        summarize_state=gw.summarize_state,
    )

    reply_state = {
        "request_id": "REQ-1",
        "reply_messages": [
            {"id": "m-1", "from": "Reviewer", "status": "sent"},
            {"id": "m-2", "from": "Reviewer", "status": "done"},
        ],
    }
    done_calls: list[tuple[str, str, str]] = []

    def _done(_args, message_id: str, actor: str, note: str) -> tuple[bool, str]:
        done_calls.append((message_id, actor, note))
        return True, "ok"

    monkeypatch.setattr(gw, "run_request_query", lambda _args, _rid: reply_state)
    monkeypatch.setattr(gw, "run_message_done", _done)

    assert gw.finalize_request_reply_messages(
        args,
        "REQ-1",
        actor="Orchestrator",
        note="note",
    ) == request_state.finalize_request_reply_messages(
        args,
        "REQ-1",
        run_request_query=lambda _args, _rid: reply_state,
        run_message_done=_done,
        actor="Orchestrator",
        note="note",
    )
    assert done_calls == [("m-1", "Orchestrator", "note"), ("m-1", "Orchestrator", "note")]


def test_tf_exec_module_matches_gateway_exec_helpers(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_TF_EXEC_MODE", "inplace")

    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)

    args_a = argparse.Namespace(
        project_root=project_root,
        team_dir=team_dir,
        orch_command_timeout_sec=900,
        aoe_orch_bin="/usr/bin/aoe-orch",
        orch_poll_sec=2.0,
        roles="",
        priority="P2",
        orch_timeout_sec=120,
        no_wait=False,
        _aoe_project_key="demo_proj",
        _aoe_project_alias="O9",
        _aoe_control_mode="retry",
        _aoe_source_request_id="REQ-000",
    )
    args_b = copy.deepcopy(args_a)
    args_b._aoe_default_tf_worker_session_prefix = gw.DEFAULT_TF_WORKER_SESSION_PREFIX

    meta_a = gw.ensure_tf_exec_workspace(args_a, "REQ-001")
    meta_b = tf_exec.ensure_tf_exec_workspace(
        args_b,
        "REQ-002",
        default_tf_exec_mode=gw.DEFAULT_TF_EXEC_MODE,
        default_tf_work_root_name=gw.DEFAULT_TF_WORK_ROOT_NAME,
        default_tf_exec_map_file=gw.DEFAULT_TF_EXEC_MAP_FILE,
        now_iso=gw.now_iso,
        run_command=gw.run_command,
    )
    assert meta_a["project_key"] == meta_b["project_key"] == "demo_proj"
    assert meta_a["project_alias"] == meta_b["project_alias"] == "O9"

    specs_a = gw.tf_worker_specs(args_a, "REQ-123", ["Reviewer"], startup_timeout_sec=120)
    specs_b = tf_exec.tf_worker_specs(args_b, "REQ-123", ["Reviewer"], startup_timeout_sec=120)
    assert specs_a == specs_b

    class Proc:
        returncode = 0
        stdout = json.dumps({"request_id": "REQ-1", "dispatch_plan": [{"role": "DataEngineer"}, {"role": "Reviewer"}]})
        stderr = ""

    original_run_command = gw.run_command
    gw.run_command = lambda cmd, env, timeout_sec: Proc()
    try:
        roles_a = gw.resolve_dispatch_roles_from_preview(args_a, "Check quality", "REQ-1", "", "P2", 120)
        roles_b = tf_exec.resolve_dispatch_roles_from_preview(
            args_b,
            "Check quality",
            "REQ-1",
            "",
            "P2",
            120,
            run_command=gw.run_command,
        )
    finally:
        gw.run_command = original_run_command
    assert roles_a == roles_b == ["DataEngineer", "Reviewer"]


def test_infer_natural_run_mode_treats_direct_as_bias_not_force() -> None:
    assert tg_parse.infer_natural_run_mode("로그인 버그를 수정해줘", "direct") == "dispatch"
    assert tg_parse.infer_natural_run_mode("지금 상태 설명해줘", "direct") == "direct"


def test_set_and_clear_project_lock_roundtrip() -> None:
    state = _empty_state()
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": "/tmp/TwinPaper/.aoe-team",
        "tasks": {},
    }

    row = gw.set_project_lock(state, "twinpaper", actor="chat:939062873")

    assert row["project_key"] == "twinpaper"
    assert row["locked_by"] == "chat:939062873"
    assert gw.get_project_lock_key(state) == "twinpaper"
    assert gw.project_lock_label(state) == "O2 (twinpaper)"
    assert state["active"] == "twinpaper"

    assert gw.clear_project_lock(state) is True
    assert gw.get_project_lock_key(state) == ""
    assert gw.clear_project_lock(state) is False


def test_project_state_module_matches_gateway_project_helpers(tmp_path: Path) -> None:
    state = _empty_state()
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {},
    }

    assert gw.normalize_project_name("Twin Paper") == project_state.normalize_project_name("Twin Paper")
    assert gw.normalize_project_alias("o2") == project_state.normalize_project_alias("o2")
    assert gw.extract_project_alias_index("O2") == project_state.extract_project_alias_index("O2")
    assert gw.ensure_project_aliases(state) == project_state.ensure_project_aliases(state)
    assert gw.project_alias_for_key(state, "twinpaper") == project_state.project_alias_for_key(state, "twinpaper")

    state_for_mod = copy.deepcopy(state)
    original_now_iso = gw.now_iso
    try:
        gw.now_iso = lambda: "2026-03-11T12:00:00+0900"
        row = gw.set_project_lock(state, "twinpaper", actor="chat:939062873")
        row_mod = project_state.set_project_lock(
            state_for_mod,
            "twinpaper",
            now_iso=lambda: "2026-03-11T12:00:00+0900",
            actor="chat:939062873",
        )
    finally:
        gw.now_iso = original_now_iso
    assert row == row_mod
    assert gw.get_project_lock_key(state) == project_state.get_project_lock_key(state, bool_from_json=gw.bool_from_json)
    assert gw.project_lock_label(state) == project_state.project_lock_label(state, bool_from_json=gw.bool_from_json)

    key_a, entry_a = gw.get_manager_project(state, "O2")
    key_b, entry_b = project_state.get_manager_project(state, "O2", bool_from_json=gw.bool_from_json)
    assert (key_a, entry_a) == (key_b, entry_b)

    args = argparse.Namespace(project_root=project_root, team_dir=team_dir, foo="bar")
    a_args = gw.make_project_args(args, entry_a, key=key_a)
    b_args = project_state.make_project_args(args, entry_b, key=key_b)
    assert vars(a_args) == vars(b_args)


def test_get_manager_project_respects_hard_project_lock() -> None:
    state = _empty_state()
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": "/tmp/TwinPaper/.aoe-team",
        "tasks": {},
    }
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": "/tmp/Nano",
        "team_dir": "/tmp/Nano/.aoe-team",
        "tasks": {},
    }

    gw.set_project_lock(state, "twinpaper")

    key, _entry = gw.get_manager_project(state, None)
    assert key == "twinpaper"

    key, _entry = gw.get_manager_project(state, "O2")
    assert key == "twinpaper"

    try:
        gw.get_manager_project(state, "O3")
    except RuntimeError as exc:
        text = str(exc)
        assert "project lock active" in text
        assert "use /focus off or /focus O2" in text
    else:
        raise AssertionError("expected project lock conflict")


def test_parse_focus_and_unlock_commands() -> None:
    assert tg_parse.parse_cli_message("aoe focus O2") == {"cmd": "focus", "rest": "O2"}
    assert tg_parse.parse_cli_message("aoe unlock") == {"cmd": "focus", "rest": "off"}
    assert tg_parse.parse_cli_message("aoe orch repair O2") == {"cmd": "orch-repair", "orch": "O2"}

    manager_state = _empty_state()
    resolved = resolver.resolve_message_command(
        text="/unlock",
        slash_only=False,
        manager_state=manager_state,
        chat_id="939062873",
        dry_run=True,
        manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
        get_pending_mode=gw.get_pending_mode,
        get_default_mode=gw.get_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        save_manager_state=lambda path, state: None,
    )

    assert resolved.cmd == "focus"
    assert resolved.rest == "off"


def test_summarize_orch_registry_shows_focus_counts_and_sync() -> None:
    state = _empty_state()
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": str(ROOT / ".aoe-team"),
        "last_sync_at": "",
        "last_sync_mode": "scenario",
        "pending_todo": {"todo_id": "TODO-001", "chat_id": "939062873", "selected_at": "2026-03-06T12:00:00+0900"},
        "todos": [
            {"id": "TODO-001", "summary": "first", "priority": "P1", "status": "open"},
            {"id": "TODO-002", "summary": "second", "priority": "P2", "status": "running"},
            {"id": "TODO-003", "summary": "third", "priority": "P2", "status": "blocked"},
        ],
        "tasks": {
            "REQ-1": {
                "request_id": "REQ-1",
                "short_id": "T-001",
                "alias": "login-fix",
                "prompt": "fix login",
                "status": "running",
                "updated_at": "2026-03-06T12:00:00+0900",
            }
        },
    }
    state["active"] = "twinpaper"
    gw.set_project_lock(state, "twinpaper")

    text = gw.summarize_orch_registry(state)

    assert "active: O2 (twinpaper)" in text
    assert "project_lock: O2 (twinpaper)" in text
    assert "* O2 TwinPaper [PENDING] | todo o/r/b=1/1/1 | last_sync=scenario | last_task=T-001 login-fix[running]" in text
    assert "key=twinpaper | root=/tmp/TwinPaper" in text


def test_project_runtime_issue_reports_missing_orchestrator(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)

    issue = runtime_helpers.project_runtime_issue({"team_dir": str(team_dir)})

    assert issue.startswith("missing_orchestrator:")
    assert "orchestrator.json" in issue


def test_append_gateway_event_targets_mirrors_to_root_log(tmp_path: Path) -> None:
    project_team_dir = tmp_path / "project" / ".aoe-team"
    root_team_dir = tmp_path / "mother" / ".aoe-team"
    row = {
        "timestamp": "2026-03-07T19:00:00+0900",
        "event": "dispatch_completed",
        "trace_id": "trace-1",
        "project": "twinpaper",
        "request_id": "REQ-1",
        "task_short_id": "T-001",
        "task_alias": "demo",
        "stage": "close",
        "actor": "telegram:939062873",
        "status": "completed",
        "error_code": "",
        "latency_ms": 123,
        "detail": "ok",
    }

    gw.append_gateway_event_targets(team_dir=project_team_dir, row=row, mirror_team_dir=root_team_dir)

    proj_rows = [json.loads(x) for x in (project_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    root_rows = [json.loads(x) for x in (root_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]

    assert proj_rows[-1]["log_scope"] == "project"
    assert "project_team_dir" not in proj_rows[-1]
    assert root_rows[-1]["log_scope"] == "mother"
    assert root_rows[-1]["project_team_dir"] == str(project_team_dir.resolve())


def test_mirror_tf_backend_runtime_events_writes_project_and_root_rows(tmp_path: Path) -> None:
    project_team_dir = tmp_path / "project" / ".aoe-team"
    root_team_dir = tmp_path / "mother" / ".aoe-team"
    runtime_events = [
        {
            "seq": 1,
            "ts": "2026-03-11T18:00:00+0900",
            "backend": "autogen_core",
            "source": "tf_orchestrator",
            "stage": "request.accepted",
            "kind": "lifecycle",
            "status": "info",
            "summary": "accepted TF request",
            "payload": {"project_key": "O3"},
        },
        {
            "seq": 2,
            "ts": "2026-03-11T18:00:01+0900",
            "backend": "autogen_core",
            "source": "reviewer",
            "stage": "verdict.emitted",
            "kind": "verdict",
            "status": "success",
            "summary": "review verdict emitted",
            "payload": {"verdict": "success"},
        },
    ]

    mirrored = gw.mirror_tf_backend_runtime_events(
        team_dir=project_team_dir,
        backend="autogen_core",
        runtime_events=runtime_events,
        trace_id="trace-runtime-1",
        project="kisti_nanoclustering",
        request_id="REQ-42",
        task={"short_id": "T-042", "alias": "sandbox"},
        mirror_team_dir=root_team_dir,
    )

    assert mirrored == 2
    proj_rows = [json.loads(x) for x in (project_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    root_rows = [json.loads(x) for x in (root_team_dir / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]

    assert len(proj_rows) == 2
    assert len(root_rows) == 2
    assert proj_rows[0]["event"] == "tf_backend_runtime_event"
    assert proj_rows[0]["backend"] == "autogen_core"
    assert proj_rows[0]["backend_seq"] == 1
    assert proj_rows[1]["backend_kind"] == "verdict"
    assert proj_rows[1]["actor"] == "autogen_core:reviewer"
    assert proj_rows[1]["request_id"] == "REQ-42"
    assert proj_rows[1]["task_short_id"] == "T-042"
    assert proj_rows[1]["log_scope"] == "project"
    assert root_rows[0]["log_scope"] == "mother"
    assert root_rows[0]["project_team_dir"] == str(project_team_dir.resolve())


def test_gateway_events_module_matches_gateway_runtime_event_mirroring(tmp_path: Path) -> None:
    project_a = tmp_path / "a" / ".aoe-team"
    project_b = tmp_path / "b" / ".aoe-team"
    runtime_events = [
        {
            "seq": 1,
            "ts": "2026-03-11T18:00:00+0900",
            "backend": "local",
            "source": "gateway.preview",
            "stage": "roles.resolved",
            "kind": "dispatch",
            "status": "success",
            "summary": "resolved role set",
            "payload": {"roles": ["Reviewer"]},
        }
    ]

    count_a = gw.mirror_tf_backend_runtime_events(
        team_dir=project_a,
        backend="local",
        runtime_events=runtime_events,
        trace_id="trace-a",
        project="demo",
        request_id="REQ-A",
        task={"short_id": "T-001", "alias": "demo"},
    )
    count_b = gateway_events.mirror_backend_runtime_events(
        team_dir=project_b,
        backend="local",
        runtime_events=runtime_events,
        now_iso=gw.now_iso,
        mask_sensitive_text=gw.mask_sensitive_text,
        append_gateway_event_targets=lambda **kwargs: gateway_events.append_gateway_event_targets(
            append_jsonl=gw.append_jsonl,
            **kwargs,
        ),
        trace_id="trace-a",
        project="demo",
        request_id="REQ-A",
        task={"short_id": "T-001", "alias": "demo"},
    )

    rows_a = [json.loads(x) for x in (project_a / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]
    rows_b = [json.loads(x) for x in (project_b / "logs" / "gateway_events.jsonl").read_text(encoding="utf-8").splitlines() if x.strip()]

    assert count_a == count_b == 1
    assert rows_a == rows_b


def test_summarize_orch_registry_marks_unready_project(tmp_path: Path) -> None:
    state = _empty_state()
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": str(team_dir),
        "tasks": {},
    }

    text = gw.summarize_orch_registry(state)

    assert "O2 TwinPaper [UNREADY]" in text
    assert "runtime=missing orchestrator.json" in text


def test_orch_registry_module_matches_gateway_summary_and_status(tmp_path: Path, monkeypatch) -> None:
    state = _empty_state()
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(team_dir),
        "last_sync_mode": "scenario",
        "todos": [{"id": "TODO-001", "summary": "first", "priority": "P1", "status": "open"}],
        "tasks": {},
    }
    state["active"] = "twinpaper"

    gw_text = gw.summarize_orch_registry(state)
    mod_text = orch_registry.summarize_orch_registry(
        state,
        ensure_project_aliases=gw.ensure_project_aliases,
        project_alias_for_key=gw.project_alias_for_key,
        project_lock_label=gw.project_lock_label,
        extract_project_alias_index=gw.extract_project_alias_index,
        bool_from_json=gw.bool_from_json,
        task_display_label=gw.task_display_label,
        normalize_task_status=gw.normalize_task_status,
    )
    assert gw_text == mod_text

    args = argparse.Namespace(
        aoe_orch_bin="aoe-orch",
        project_root=tmp_path / "TwinPaper",
        team_dir=team_dir,
        state_file=tmp_path / "gateway_state.json",
    )

    class Proc:
        returncode = 0
        stdout = "status ok"
        stderr = ""

    monkeypatch.setattr(gw, "run_command", lambda cmd, env, timeout_sec: Proc())
    monkeypatch.setattr(gw, "summarize_gateway_poll_state", lambda path: "poll-summary")

    assert gw.run_aoe_status(args) == orch_registry.run_aoe_status(
        args,
        run_command=lambda cmd, env, timeout_sec: Proc(),
        summarize_gateway_poll_state=lambda path: "poll-summary",
    )


def test_drain_peek_next_todo_skips_unready_project_and_selects_ready_one(tmp_path: Path) -> None:
    state = _empty_state()
    bad_team = tmp_path / "TwinPaper" / ".aoe-team"
    bad_team.mkdir(parents=True, exist_ok=True)
    good_team = tmp_path / "Local" / ".aoe-team"
    good_team.mkdir(parents=True, exist_ok=True)
    (good_team / "orchestrator.json").write_text("{}", encoding="utf-8")

    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(bad_team),
        "todos": [{"id": "TODO-001", "summary": "broken runtime", "priority": "P1", "status": "open"}],
    }
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Local"),
        "team_dir": str(good_team),
        "todos": [{"id": "TODO-001", "summary": "ready runtime", "priority": "P2", "status": "open"}],
    }

    key, todo_id, reason = gw._drain_peek_next_todo(state, "939062873", force=False)

    assert key == "local"
    assert todo_id == "TODO-001"
    assert reason == "candidate"


def test_drain_peek_next_todo_ignores_blocked_rows_when_open_todo_exists(tmp_path: Path) -> None:
    state = _empty_state()
    team = tmp_path / "Local" / ".aoe-team"
    team.mkdir(parents=True, exist_ok=True)
    (team / "orchestrator.json").write_text("{}", encoding="utf-8")

    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Local"),
        "team_dir": str(team),
        "todos": [
            {"id": "TODO-001", "summary": "blocked first", "priority": "P1", "status": "blocked"},
            {"id": "TODO-002", "summary": "open second", "priority": "P2", "status": "open"},
        ],
    }

    key, todo_id, reason = gw._drain_peek_next_todo(state, "939062873", force=False)

    assert key == "local"
    assert todo_id == "TODO-002"
    assert reason == "candidate"


def test_gateway_batch_ops_module_matches_gateway_drain_peek(tmp_path: Path) -> None:
    state = _empty_state()
    team = tmp_path / "Local" / ".aoe-team"
    team.mkdir(parents=True, exist_ok=True)
    (team / "orchestrator.json").write_text("{}", encoding="utf-8")

    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Local"),
        "team_dir": str(team),
        "todos": [
            {"id": "TODO-001", "summary": "blocked first", "priority": "P1", "status": "blocked"},
            {"id": "TODO-002", "summary": "open second", "priority": "P2", "status": "open"},
        ],
    }

    assert gateway_batch_ops.drain_peek_next_todo(state, "939062873", force=False) == gw._drain_peek_next_todo(
        state,
        "939062873",
        force=False,
    )


def test_queue_engine_matches_gateway_and_scheduler_next_selection(tmp_path: Path) -> None:
    state = _empty_state()
    team = tmp_path / "Local" / ".aoe-team"
    team.mkdir(parents=True, exist_ok=True)
    (team / "orchestrator.json").write_text("{}", encoding="utf-8")

    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Local"),
        "team_dir": str(team),
        "todos": [
            {"id": "TODO-001", "summary": "blocked first", "priority": "P1", "status": "blocked"},
            {"id": "TODO-002", "summary": "open second", "priority": "P2", "status": "open"},
        ],
    }

    queue_pick = queue_engine.pick_global_next_candidate(state["projects"], ignore_busy=False, skip_paused=True)
    sched_pick = sched._pick_global_next_candidate(state["projects"], ignore_busy=False, skip_paused=True)
    gw_pick = gw._drain_peek_next_todo(state, "939062873", force=False)

    assert isinstance(queue_pick, dict)
    assert isinstance(sched_pick, dict)
    assert queue_pick["project_key"] == "local"
    assert queue_pick["todo"]["id"] == "TODO-002"
    assert sched_pick["project_key"] == queue_pick["project_key"]
    assert sched_pick["todo"]["id"] == queue_pick["todo"]["id"]
    assert gw_pick == ("local", "TODO-002", "candidate")


def test_transport_module_matches_gateway_transport_exports() -> None:
    previous = os.environ.get("AOE_TG_COMMAND_PREFIXES")
    os.environ["AOE_TG_COMMAND_PREFIXES"] = "!/"
    try:
        body = "alpha\nbeta\n" + ("z" * 300)
        assert gw.split_text(body, 120) == transport.split_text(body, 120)
        assert gw.preferred_command_prefix() == transport.preferred_command_prefix() == "!"
        assert gw.build_quick_reply_keyboard() == transport.build_quick_reply_keyboard()
    finally:
        if previous is None:
            os.environ.pop("AOE_TG_COMMAND_PREFIXES", None)
        else:
            os.environ["AOE_TG_COMMAND_PREFIXES"] = previous


def test_runtime_core_matches_gateway_path_and_default_state_helpers(tmp_path: Path) -> None:
    project_root = runtime_core.resolve_project_root(str(tmp_path))
    team_dir = runtime_core.resolve_team_dir(project_root, None)
    state_file = runtime_core.resolve_state_file(project_root, None)

    assert gw.resolve_project_root(str(tmp_path)) == project_root
    assert gw.resolve_team_dir(project_root, None) == team_dir
    assert gw.resolve_state_file(project_root, None) == state_file

    expected = runtime_core.default_manager_state(project_root, team_dir, now_iso=gw.now_iso)
    actual = gw.default_manager_state(project_root, team_dir)
    assert actual == expected


def test_gateway_events_module_matches_gateway_task_identifiers() -> None:
    task = {"short_id": "T-001", "alias": "demo"}
    assert gw.task_identifiers(task) == gateway_events.task_identifiers(task)


def test_runtime_core_matches_gateway_default_project_registration(tmp_path: Path) -> None:
    state_a = {"active": "missing", "projects": {"demo": {"name": "demo", "project_alias": "O2"}}}
    state_b = copy.deepcopy(state_a)
    project_root = tmp_path
    team_dir = tmp_path / ".aoe-team"

    gw.ensure_default_project_registered(state_a, project_root, team_dir)
    runtime_core.ensure_default_project_registered(
        state_b,
        project_root,
        team_dir,
        now_iso=gw.now_iso,
        bool_from_json=gw.bool_from_json,
        normalize_project_alias=gw.normalize_project_alias,
        normalize_project_name=gw.normalize_project_name,
        sanitize_project_lock_row=gw.sanitize_project_lock_row,
        ensure_project_aliases=gw.ensure_project_aliases,
        backfill_task_aliases=gw.backfill_task_aliases,
    )

    assert state_a == state_b


def test_orch_map_reply_markup_contains_use_focus_status_todo_and_active_sync_actions() -> None:
    state = _empty_state()
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": "/tmp/TwinPaper/.aoe-team",
    }
    state["active"] = "twinpaper"

    markup = overview._orch_map_reply_markup(state)

    assert isinstance(markup, dict)
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/use O1" in buttons
    assert "/focus O1" in buttons
    assert "/use O2" in buttons
    assert "/focus O2" in buttons
    assert "/orch status O1" in buttons
    assert "/todo O1" in buttons
    assert "/todo O1 followup" in buttons
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/todo O2 followup" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/sync O2 1h" in buttons
    assert "/queue" in buttons
    assert "/next" in buttons


def test_orch_map_reply_markup_narrows_to_locked_project() -> None:
    state = _empty_state()
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": "/tmp/TwinPaper",
        "team_dir": "/tmp/TwinPaper/.aoe-team",
    }
    state["active"] = "twinpaper"
    gw.set_project_lock(state, "twinpaper")

    markup = overview._orch_map_reply_markup(state)

    assert isinstance(markup, dict)
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/use O1" not in buttons
    assert "/focus O1" not in buttons
    assert "/use O2" in buttons
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/todo O2 followup" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/sync O2 1h" in buttons
    assert "/focus off" in buttons


def test_orch_map_reply_markup_includes_repair_for_unready_project(tmp_path: Path) -> None:
    state = _empty_state()
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(team_dir),
    }

    markup = overview._orch_map_reply_markup(state)

    assert isinstance(markup, dict)
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/orch repair O2" in buttons


def test_resolve_message_command_parses_slash_orch_repair() -> None:
    resolved = resolver.resolve_message_command(
        text="/orch repair O2",
        slash_only=False,
        manager_state=_empty_state(),
        chat_id="939062873",
        dry_run=True,
        manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
        get_pending_mode=gw.get_pending_mode,
        get_default_mode=gw.get_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        save_manager_state=lambda path, state: None,
    )

    assert resolved.cmd == "orch-repair"
    assert resolved.orch_target == "O2"


def test_orch_repair_rebuilds_missing_runtime(tmp_path: Path) -> None:
    state = _empty_state()
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "overview": "Twin project orchestration",
        "tasks": {},
    }
    state["active"] = "twinpaper"

    messages = []

    def _send(msg: str, **kwargs):
        messages.append(msg)
        return True

    def _run_aoe_init(args, project_root: Path, team_dir: Path, overview: str) -> str:
        team_dir.mkdir(parents=True, exist_ok=True)
        (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
        return "[OK] initialized"

    handled = orch_task_handlers.handle_orch_task_command(
        cmd="orch-repair",
        args=argparse.Namespace(
            project_root=ROOT,
            manager_state_file=tmp_path / "manager_state.json",
            dry_run=False,
        ),
        manager_state=state,
        chat_id="939062873",
        orch_target="O2",
        orch_add_name=None,
        orch_add_path=None,
        orch_add_overview=None,
        orch_add_init=True,
        orch_add_spawn=True,
        orch_add_set_active=True,
        rest="",
        orch_check_request_id=None,
        orch_task_request_id=None,
        orch_pick_request_id=None,
        orch_cancel_request_id=None,
        send=_send,
        log_event=lambda **kwargs: None,
        get_context=lambda target: (_ for _ in ()).throw(RuntimeError("not used")),
        latest_task_request_refs=lambda *args, **kwargs: [],
        set_chat_recent_task_refs=lambda *args, **kwargs: None,
        save_manager_state=lambda path, state: None,
        resolve_project_root=lambda raw: Path(raw).expanduser().resolve(),
        is_path_within=lambda path, root: True,
        register_orch_project=lambda *args, **kwargs: ("", {}),
        run_aoe_init=_run_aoe_init,
        run_aoe_spawn=lambda *args, **kwargs: "[SKIP] spawn",
        now_iso=lambda: "2026-03-07T18:30:00+0900",
        run_aoe_status=lambda p_args: "",
        resolve_chat_task_ref=lambda *args, **kwargs: "",
        resolve_task_request_id=lambda entry, ref: "",
        run_request_query=lambda *args, **kwargs: {},
        sync_task_lifecycle=lambda *args, **kwargs: None,
        resolve_verifier_candidates=lambda text: [],
        touch_chat_recent_task_ref=lambda *args, **kwargs: None,
        set_chat_selected_task_ref=lambda *args, **kwargs: None,
        get_chat_selected_task_ref=lambda *args, **kwargs: "",
        get_task_record=lambda *args, **kwargs: None,
        summarize_request_state=lambda *args, **kwargs: "",
        summarize_three_stage_request=lambda *args, **kwargs: "",
        summarize_task_lifecycle=lambda *args, **kwargs: "",
        task_display_label=lambda *args, **kwargs: "",
        cancel_request_assignments=lambda *args, **kwargs: {},
        lifecycle_set_stage=lambda *args, **kwargs: None,
        summarize_cancel_result=lambda *args, **kwargs: "",
    )

    assert handled is True
    assert (team_dir / "orchestrator.json").exists()
    assert (team_dir / "AOE_TODO.md").exists()
    assert messages
    assert "orch repair finished" in messages[-1]
    assert "- after: ready" in messages[-1]


def test_orch_repair_all_repairs_multiple_projects(tmp_path: Path) -> None:
    state = _empty_state()
    for key, alias in [("twinpaper", "O2"), ("nano", "O3")]:
        project_root = tmp_path / key
        team_dir = project_root / ".aoe-team"
        team_dir.mkdir(parents=True, exist_ok=True)
        state["projects"][key] = {
            "name": key,
            "display_name": key,
            "project_alias": alias,
            "project_root": str(project_root),
            "team_dir": str(team_dir),
            "overview": f"{key} orchestration",
            "tasks": {},
        }

    messages = []

    def _send(msg: str, **kwargs):
        messages.append(msg)
        return True

    def _run_aoe_init(args, project_root: Path, team_dir: Path, overview: str) -> str:
        (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
        return "[OK] initialized"

    handled = orch_task_handlers.handle_orch_task_command(
        cmd="orch-repair",
        args=argparse.Namespace(
            project_root=ROOT,
            manager_state_file=tmp_path / "manager_state.json",
            dry_run=False,
        ),
        manager_state=state,
        chat_id="939062873",
        orch_target="all",
        orch_add_name=None,
        orch_add_path=None,
        orch_add_overview=None,
        orch_add_init=True,
        orch_add_spawn=True,
        orch_add_set_active=True,
        rest="",
        orch_check_request_id=None,
        orch_task_request_id=None,
        orch_pick_request_id=None,
        orch_cancel_request_id=None,
        send=_send,
        log_event=lambda **kwargs: None,
        get_context=lambda target: (_ for _ in ()).throw(RuntimeError("not used")),
        latest_task_request_refs=lambda *args, **kwargs: [],
        set_chat_recent_task_refs=lambda *args, **kwargs: None,
        save_manager_state=lambda path, state: None,
        resolve_project_root=lambda raw: Path(raw).expanduser().resolve(),
        is_path_within=lambda path, root: True,
        register_orch_project=lambda *args, **kwargs: ("", {}),
        run_aoe_init=_run_aoe_init,
        run_aoe_spawn=lambda *args, **kwargs: "[SKIP] spawn",
        now_iso=lambda: "2026-03-07T18:30:00+0900",
        run_aoe_status=lambda p_args: "",
        resolve_chat_task_ref=lambda *args, **kwargs: "",
        resolve_task_request_id=lambda entry, ref: "",
        run_request_query=lambda *args, **kwargs: {},
        sync_task_lifecycle=lambda *args, **kwargs: None,
        resolve_verifier_candidates=lambda text: [],
        touch_chat_recent_task_ref=lambda *args, **kwargs: None,
        set_chat_selected_task_ref=lambda *args, **kwargs: None,
        get_chat_selected_task_ref=lambda *args, **kwargs: "",
        get_task_record=lambda *args, **kwargs: None,
        summarize_request_state=lambda *args, **kwargs: "",
        summarize_three_stage_request=lambda *args, **kwargs: "",
        summarize_task_lifecycle=lambda *args, **kwargs: "",
        task_display_label=lambda *args, **kwargs: "",
        cancel_request_assignments=lambda *args, **kwargs: {},
        lifecycle_set_stage=lambda *args, **kwargs: None,
        summarize_cancel_result=lambda *args, **kwargs: "",
    )

    assert handled is True
    assert messages
    assert "orch repair all finished" in messages[-1]
    assert "- projects: 3" in messages[-1]
    assert "- ready: 3" in messages[-1]


def test_orch_status_under_other_focus_returns_operator_message() -> None:
    state = _empty_state()
    messages = []

    handled = orch_task_handlers.handle_orch_task_command(
        cmd="orch-status",
        args=argparse.Namespace(
            project_root=ROOT,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            dry_run=True,
        ),
        manager_state=state,
        chat_id="939062873",
        orch_target="O2",
        orch_add_name=None,
        orch_add_path=None,
        orch_add_overview=None,
        orch_add_init=True,
        orch_add_spawn=True,
        orch_add_set_active=True,
        rest="",
        orch_check_request_id=None,
        orch_task_request_id=None,
        orch_pick_request_id=None,
        orch_cancel_request_id=None,
        send=lambda msg, **kwargs: messages.append(msg) or True,
        log_event=lambda **kwargs: None,
        get_context=lambda target: (_ for _ in ()).throw(
            RuntimeError("project lock active: O4 (local_map_analysis). requested=O2 (twinpaper). use /focus off or /focus O4")
        ),
        latest_task_request_refs=lambda *args, **kwargs: [],
        set_chat_recent_task_refs=lambda *args, **kwargs: None,
        save_manager_state=lambda path, state: None,
        resolve_project_root=lambda raw: Path(raw).expanduser().resolve(),
        is_path_within=lambda path, root: True,
        register_orch_project=lambda *args, **kwargs: ("", {}),
        run_aoe_init=lambda *args, **kwargs: "",
        run_aoe_spawn=lambda *args, **kwargs: "",
        now_iso=lambda: "2026-03-07T18:30:00+0900",
        run_aoe_status=lambda p_args: "",
        resolve_chat_task_ref=lambda *args, **kwargs: "",
        resolve_task_request_id=lambda entry, ref: "",
        run_request_query=lambda *args, **kwargs: {},
        sync_task_lifecycle=lambda *args, **kwargs: None,
        resolve_verifier_candidates=lambda text: [],
        touch_chat_recent_task_ref=lambda *args, **kwargs: None,
        set_chat_selected_task_ref=lambda *args, **kwargs: None,
        get_chat_selected_task_ref=lambda *args, **kwargs: "",
        get_task_record=lambda *args, **kwargs: None,
        summarize_request_state=lambda *args, **kwargs: "",
        summarize_three_stage_request=lambda *args, **kwargs: "",
        summarize_task_lifecycle=lambda *args, **kwargs: "",
        task_display_label=lambda *args, **kwargs: "",
        cancel_request_assignments=lambda *args, **kwargs: {},
        lifecycle_set_stage=lambda *args, **kwargs: None,
        summarize_cancel_result=lambda *args, **kwargs: "",
    )

    assert handled is True
    assert messages
    assert "orch status blocked by project lock" in messages[-1]


def test_todo_reply_markup_contains_run_done_and_drilldown_actions() -> None:
    entry = {"project_alias": "O2"}
    active_rows = [
        {"id": "TODO-001", "status": "blocked", "blocked_bucket": "manual_followup"},
        {"id": "TODO-002", "status": "running"},
        {"id": "TODO-003", "status": "blocked"},
    ]

    markup = todo_handlers._todo_reply_markup("twinpaper", entry, active_rows)

    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    for expected in [
        "/todo next",
        "/todo followup",
        "/orch status O2",
        "/sync preview O2 1h",
        "/todo ackrun 1",
        "/todo ack 1",
        "/todo done 1",
        "/todo done 2",
        "/todo done 3",
        "/sync O2 1h",
        "/queue",
        "/next",
        "/map",
        "/help",
    ]:
        assert expected in buttons


def test_todo_reply_markup_omits_ack_buttons_without_manual_followup() -> None:
    entry = {"project_alias": "O2"}
    active_rows = [
        {"id": "TODO-001", "status": "open"},
        {"id": "TODO-002", "status": "running"},
        {"id": "TODO-003", "status": "blocked"},
    ]

    markup = todo_handlers._todo_reply_markup("twinpaper", entry, active_rows)

    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/todo ack 1" not in buttons


def test_todo_list_shows_block_count_and_reason(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "need owner input",
                        "priority": "P1",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                        "blocked_reason": "plan gate: critic unresolved after auto-replan",
                    }
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    assert "manual_followup:" in sent[-1]
    assert "blocked x2 [manual_followup] | plan gate: critic unresolved after auto-replan" in sent[-1]


def test_todo_followup_lists_only_manual_followup_rows(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "need owner input",
                        "priority": "P1",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                        "blocked_reason": "plan gate: critic unresolved after auto-replan",
                    },
                    {
                        "id": "TODO-002",
                        "summary": "regular open task",
                        "priority": "P2",
                        "status": "open",
                    },
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="followup",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1]
    assert "todo followup: count=1 active=2" in text
    assert "manual_followup:" in text
    assert "tip: /todo ackrun <번호|TODO-xxx>" in text
    assert "TODO-001" in text
    assert "TODO-002" not in text


def test_todo_syncback_preview_reports_done_append_and_blocked_notes(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text(
        "# Tasks\n- [ ] phase1 rerun\n- [ ] existing backlog\n",
        encoding="utf-8",
    )
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "updated_at": "2026-03-07T21:00:00+0900",
                "todos": [
                    {"id": "TODO-001", "summary": "phase1 rerun", "priority": "P1", "status": "done"},
                    {
                        "id": "TODO-002",
                        "summary": "need owner input",
                        "priority": "P2",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                        "blocked_reason": "plan gate: critic unresolved",
                    },
                ],
                "todo_proposals": [
                    {"id": "PROP-001", "summary": "accepted follow-up", "priority": "P2", "status": "accepted"}
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="syncback preview",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T21:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1]
    assert "todo syncback preview" in text
    assert "- orch: twinpaper (O2)" in text
    assert "- mark_done: 1" in text
    assert "- append_new: 1" in text
    assert "- blocked_notes: 1" in text
    assert "- - [ ] P2: accepted follow-up" in text


def test_todo_ack_reopens_blocked_followup_and_clears_blocked_meta(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "need owner input",
                        "priority": "P1",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                        "blocked_alerted_at": "2026-03-07T00:30:00+0900",
                        "blocked_reason": "plan gate: critic unresolved after auto-replan",
                    }
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="ack 1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result == {"terminal": True}
    row = manager_state["projects"]["twinpaper"]["todos"][0]
    assert row["status"] == "open"
    assert "blocked_count" not in row
    assert "blocked_bucket" not in row
    assert "blocked_alerted_at" not in row
    assert "blocked_reason" not in row
    assert sent
    assert "todo acknowledged" in sent[-1]
    assert "- reopened: yes" in sent[-1]
    assert "- cleared_followup: yes" in sent[-1]


def test_todo_ackrun_reopens_blocked_followup_and_returns_run_transition(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "need owner input",
                        "priority": "P1",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                        "blocked_alerted_at": "2026-03-07T00:30:00+0900",
                        "blocked_reason": "plan gate: critic unresolved after auto-replan",
                    }
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="ackrun 1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result["terminal"] is False
    assert result["cmd"] == "run"
    assert result["orch_target"] == "twinpaper"
    assert result["run_prompt"] == "need owner input"
    assert result["run_force_mode"] == "dispatch"
    assert result["run_auto_source"] == "todo-ackrun"
    row = manager_state["projects"]["twinpaper"]["todos"][0]
    assert row["status"] == "open"
    assert "blocked_count" not in row
    assert "blocked_bucket" not in row
    assert "blocked_alerted_at" not in row
    assert "blocked_reason" not in row
    assert manager_state["projects"]["twinpaper"]["pending_todo"]["todo_id"] == "TODO-001"
    assert sent
    assert "todo ackrun selected" in sent[-1]
    assert "- reopened: yes" in sent[-1]
    assert "- cleared_followup: yes" in sent[-1]


def test_todo_ack_rejects_non_blocked_row(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "regular open task",
                        "priority": "P2",
                        "status": "open",
                    }
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="ack 1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    assert "todo ack blocked: target is not blocked" in sent[-1]


def test_todo_ackrun_rejects_non_blocked_row(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "regular open task",
                        "priority": "P2",
                        "status": "open",
                    }
                ],
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="ackrun 1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    assert "todo ackrun blocked: target is not blocked" in sent[-1]


def test_merge_todo_proposals_dedupes_existing_open_proposals_and_todos() -> None:
    entry = {
        "todos": [
            {"id": "TODO-001", "summary": "existing todo"},
        ],
        "todo_seq": 1,
        "todo_proposals": [
            {
                "id": "PROP-001",
                "summary": "existing proposal",
                "priority": "P2",
                "kind": "followup",
                "status": "open",
            }
        ],
        "todo_proposal_seq": 1,
    }

    merged = todo_handlers.merge_todo_proposals(
        entry=entry,
        request_id="REQ-123",
        task={"short_id": "T-123"},
        source_todo_id="TODO-009",
        proposals_data=[
            {"summary": "existing proposal", "priority": "P1", "kind": "followup", "reason": "dup", "confidence": 0.9},
            {"summary": "existing todo", "priority": "P2", "kind": "risk", "reason": "dup", "confidence": 0.6},
            {"summary": "new actionable follow-up", "priority": "P1", "kind": "followup", "reason": "new", "confidence": 0.8},
        ],
        now_iso=lambda: "2026-03-09T09:00:00+0900",
    )

    assert merged["created_count"] == 1
    assert merged["duplicate_count"] == 2
    assert entry["todo_proposals"][-1]["summary"] == "new actionable follow-up"
    assert entry["todo_proposals"][-1]["source_request_id"] == "REQ-123"


def test_todo_state_module_matches_handler_merge_and_syncback_preview(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    project_root.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text(
        "# Tasks\n- [ ] phase1 rerun\n- [ ] existing backlog\n",
        encoding="utf-8",
    )

    entry_a = {
        "project_root": str(project_root),
        "updated_at": "2026-03-09T09:00:00+0900",
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "priority": "P1", "status": "done"},
            {
                "id": "TODO-002",
                "summary": "need owner input",
                "priority": "P2",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "plan gate: critic unresolved",
            },
        ],
        "todo_seq": 2,
        "todo_proposals": [
            {"id": "PROP-001", "summary": "accepted follow-up", "priority": "P2", "status": "accepted"}
        ],
        "todo_proposal_seq": 1,
    }
    entry_b = copy.deepcopy(entry_a)

    merge_kwargs = dict(
        request_id="REQ-123",
        task={"short_id": "T-123"},
        source_todo_id="TODO-009",
        proposals_data=[
            {"summary": "existing backlog", "priority": "P2", "kind": "risk", "reason": "dup", "confidence": 0.6},
            {"summary": "new actionable follow-up", "priority": "P1", "kind": "followup", "reason": "new", "confidence": 0.8},
        ],
        now_iso=lambda: "2026-03-09T09:00:00+0900",
    )

    merged_a = todo_handlers.merge_todo_proposals(entry=entry_a, **merge_kwargs)
    merged_b = todo_state.merge_todo_proposals(entry=entry_b, **merge_kwargs)

    assert merged_a == merged_b
    assert entry_a == entry_b

    plan_a = todo_handlers._preview_syncback_plan(entry_a)
    plan_b = todo_state.preview_syncback_plan(entry_b)
    assert plan_a == plan_b


def test_todo_state_module_matches_handler_accept_and_reject_mutations(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    base_entry = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "todos": [],
        "todo_seq": 0,
        "todo_proposals": [
            {
                "id": "PROP-001",
                "summary": "write release checklist",
                "priority": "P1",
                "kind": "handoff",
                "status": "open",
                "reason": "deployment notes are missing",
                "confidence": 0.9,
                "source_request_id": "REQ-100",
                "source_todo_id": "TODO-000",
            },
            {
                "id": "PROP-002",
                "summary": "collect schema debt",
                "priority": "P2",
                "kind": "debt",
                "status": "open",
                "reason": "schema drift remains",
                "confidence": 0.7,
                "source_request_id": "REQ-101",
            },
        ],
        "todo_proposal_seq": 2,
    }
    manager_state = {"projects": {"twinpaper": copy.deepcopy(base_entry)}}
    entry_state = copy.deepcopy(base_entry)
    sent: list[str] = []

    def _ctx(_raw: str):
        return ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir))

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="accept 1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=_ctx,
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-09T10:00:00+0900",
    )
    assert result == {"terminal": True}

    state_accept = todo_state.accept_todo_proposal(
        entry=entry_state,
        proposal=entry_state["todo_proposals"][0],
        actor="telegram:939062873",
        now="2026-03-09T10:00:00+0900",
    )
    assert state_accept["todo_id"] == "TODO-001"
    assert manager_state["projects"]["twinpaper"] == entry_state

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="reject 1 duplicate debt",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=_ctx,
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-09T10:05:00+0900",
    )
    assert result == {"terminal": True}

    state_reject = todo_state.reject_todo_proposal(
        entry=entry_state,
        proposal=entry_state["todo_proposals"][1],
        actor="telegram:939062873",
        now="2026-03-09T10:05:00+0900",
        reason="duplicate debt",
    )
    assert state_reject["reason"] == "duplicate debt"
    assert manager_state["projects"]["twinpaper"] == entry_state


def test_todo_policy_helpers_cover_syncback_and_proposal_to_todo_rules() -> None:
    row = {
        "id": "TODO-010",
        "summary": "prepare deployment checklist",
        "priority": "P1",
        "status": "running",
        "created_by": "tf-proposal",
        "created_from_request_id": "REQ-900",
    }
    assert todo_policy.todo_row_syncback_target_status(row) == "open"
    assert todo_policy.todo_row_syncback_appendable(row) is True

    done_row = {"summary": "phase1 rerun", "priority": "P2", "status": "done"}
    assert todo_policy.todo_row_syncback_target_status(done_row) == "done"
    assert todo_policy.format_canonical_todo_line("P2", "phase1 rerun", status="done") == "- [x] P2: phase1 rerun"

    proposal = {
        "id": "PROP-001",
        "summary": "write release checklist",
        "priority": "P1",
        "kind": "handoff",
        "status": "accepted",
        "source_request_id": "REQ-100",
        "source_todo_id": "TODO-000",
        "source_file": "docs/handoff.md",
        "source_section": "Next steps",
        "source_reason": "handoff section bullet",
        "source_line": 12,
    }
    todo_row = todo_policy.proposal_to_todo_row(proposal, todo_id="TODO-001", now="2026-03-09T10:00:00+0900")
    assert todo_row["proposal_id"] == "PROP-001"
    assert todo_row["proposal_kind"] == "handoff"
    assert todo_row["created_from_request_id"] == "REQ-100"
    assert todo_row["source_file"] == "docs/handoff.md"
    assert todo_row["source_line"] == 12

    accepted = todo_policy.accepted_proposals_for_syncback(
        [
            {"id": "PROP-001", "status": "accepted"},
            {"id": "PROP-002", "status": "open"},
            {"id": "PROP-003", "status": "rejected"},
        ]
    )
    assert [row["id"] for row in accepted] == ["PROP-001"]


def test_todo_proposals_list_accept_and_reject_flow(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [],
                "todo_seq": 0,
                "todo_proposals": [
                    {
                        "id": "PROP-001",
                        "summary": "write release checklist",
                        "priority": "P1",
                        "kind": "handoff",
                        "status": "open",
                        "reason": "deployment notes are missing",
                        "confidence": 0.9,
                        "source_request_id": "REQ-100",
                        "source_todo_id": "TODO-000",
                    },
                    {
                        "id": "PROP-002",
                        "summary": "collect schema debt",
                        "priority": "P2",
                        "kind": "debt",
                        "status": "open",
                        "reason": "schema drift remains",
                        "confidence": 0.7,
                        "source_request_id": "REQ-101",
                    },
                ],
                "todo_proposal_seq": 2,
            }
        }
    }
    sent: list[tuple[str, dict | None]] = []

    def _send(body: str, **kwargs) -> bool:
        sent.append((body, kwargs.get("reply_markup")))
        return True

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="proposals",
        send=_send,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-09T09:00:00+0900",
    )

    assert result == {"terminal": True}
    assert "todo proposals: open=2" in sent[-1][0]
    buttons = [btn["text"] for row in (sent[-1][1] or {}).get("keyboard", []) for btn in row]
    assert "/todo accept 1" in buttons
    assert "/todo reject 1" in buttons

    sent.clear()
    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="accept 1",
        send=_send,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-09T09:05:00+0900",
    )

    assert result == {"terminal": True}
    proposal = manager_state["projects"]["twinpaper"]["todo_proposals"][0]
    assert proposal["status"] == "accepted"
    assert proposal["accepted_todo_id"] == "TODO-001"
    assert manager_state["projects"]["twinpaper"]["todos"][0]["proposal_id"] == "PROP-001"
    assert "todo proposal accepted" in sent[-1][0]

    sent.clear()
    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="reject 1 duplicate debt",
        send=_send,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-09T09:10:00+0900",
    )

    assert result == {"terminal": True}
    proposal = manager_state["projects"]["twinpaper"]["todo_proposals"][1]
    assert proposal["status"] == "rejected"
    assert proposal["rejected_reason"] == "duplicate debt"
    assert "todo proposal rejected" in sent[-1][0]


def test_todo_next_pending_includes_ok_and_clear_pending_buttons(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "pending_todo": {"todo_id": "TODO-009", "chat_id": "939062873", "selected_at": "2026-03-07T00:50:00+0900"},
                "todos": [
                    {"id": "TODO-001", "summary": "regular open task", "priority": "P2", "status": "open"},
                ],
            }
        }
    }
    sent: list[tuple[str, dict | None]] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="next",
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text, markup = sent[-1]
    assert "todo next blocked: pending todo exists" in text
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/ok" in buttons
    assert "/clear pending" in buttons
    assert "/todo next force" in buttons
    assert "/todo O2" in buttons


def test_todo_next_ignores_blocked_rows_when_open_todo_exists(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "blocked first",
                        "priority": "P1",
                        "status": "blocked",
                    },
                    {
                        "id": "TODO-002",
                        "summary": "open second",
                        "priority": "P2",
                        "status": "open",
                    },
                ],
            }
        }
    }
    saved: list[Path] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="next",
        send=lambda body, **kwargs: True,
        get_context=lambda raw: (
            "twinpaper",
            manager_state["projects"]["twinpaper"],
            argparse.Namespace(project_root=project_root, team_dir=team_dir),
        ),
        save_manager_state=lambda path, manager_state: saved.append(path),
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result["terminal"] is False
    assert result["cmd"] == "run"
    assert result["orch_target"] == "twinpaper"
    assert result["run_prompt"] == "open second"
    assert result["run_force_mode"] == "dispatch"
    pending = manager_state["projects"]["twinpaper"]["pending_todo"]
    assert pending["todo_id"] == "TODO-002"
    assert saved == [team_dir / "orch_manager_state.json"]


def test_todo_ackrun_pending_includes_ok_and_force_buttons(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "pending_todo": {"todo_id": "TODO-009", "chat_id": "939062873", "selected_at": "2026-03-07T00:50:00+0900"},
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "need owner input",
                        "priority": "P1",
                        "status": "blocked",
                        "blocked_count": 2,
                        "blocked_bucket": "manual_followup",
                    }
                ],
            }
        }
    }
    sent: list[tuple[str, dict | None]] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="O2",
        rest="ackrun 1",
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_context=lambda raw: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T01:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text, markup = sent[-1]
    assert "todo ackrun blocked: pending todo exists" in text
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/ok" in buttons
    assert "/clear pending" in buttons
    assert "/todo O2 ackrun TODO-001 force" in buttons
    assert "/todo O2" in buttons


def test_confirm_required_reply_markup_contains_ok_cancel_and_clear_pending() -> None:
    state = _empty_state()
    sent: list[tuple[str, dict | None]] = []
    saved: list[Path] = []

    handled = run_handlers._handle_run_rate_limit_and_confirm(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            chat_max_running=3,
            chat_daily_cap=20,
        ),
        manager_state=state,
        chat_id="939062873",
        key="twinpaper",
        entry={"project_alias": "O2"},
        run_auto_source="default",
        run_force_mode="dispatch",
        orch_target="O2",
        prompt="rm -rf /tmp/demo",
        summarize_chat_usage=lambda manager_state, chat_id: (0, 0),
        detect_high_risk_prompt=lambda prompt: "destructive_delete",
        set_confirm_action=lambda *args, **kwargs: gw.set_confirm_action(*args, **kwargs),
        save_manager_state=lambda path, manager_state: saved.append(path),
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
    )

    assert handled is True
    assert sent
    text, markup = sent[-1]
    assert "고위험 자동실행 감지" in text
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/ok" in buttons
    assert "/cancel" in buttons
    assert "/clear pending" in buttons


def test_rate_limit_running_reply_markup_uses_project_context_actions() -> None:
    state = _empty_state()
    sent: list[tuple[str, str, dict | None]] = []

    handled = run_handlers._handle_run_rate_limit_and_confirm(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            chat_max_running=1,
            chat_daily_cap=20,
        ),
        manager_state=state,
        chat_id="939062873",
        key="twinpaper",
        entry={"project_alias": "O2"},
        run_auto_source="default",
        run_force_mode="dispatch",
        orch_target="O2",
        prompt="implement feature",
        summarize_chat_usage=lambda manager_state, chat_id: (1, 0),
        detect_high_risk_prompt=lambda prompt: "",
        set_confirm_action=lambda *args, **kwargs: None,
        save_manager_state=lambda path, manager_state: None,
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
    )

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "rate-limit-running"
    assert "동시 실행 한도를 초과했습니다" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/monitor" in buttons
    assert "/check" in buttons
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons


def test_rate_limit_daily_reply_markup_uses_global_actions_without_project_context() -> None:
    state = _empty_state()
    sent: list[tuple[str, str, dict | None]] = []

    handled = run_handlers._handle_run_rate_limit_and_confirm(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            chat_max_running=3,
            chat_daily_cap=1,
        ),
        manager_state=state,
        chat_id="939062873",
        key="",
        entry=None,
        run_auto_source="default",
        run_force_mode="dispatch",
        orch_target=None,
        prompt="implement feature",
        summarize_chat_usage=lambda manager_state, chat_id: (0, 1),
        detect_high_risk_prompt=lambda prompt: "",
        set_confirm_action=lambda *args, **kwargs: None,
        save_manager_state=lambda path, manager_state: None,
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
    )

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "rate-limit-daily"
    assert "일일 실행 한도에 도달했습니다" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/monitor" in buttons
    assert "/check" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons
    assert "/help" in buttons


def test_enforce_dispatch_policies_verifier_gate_setup_adds_project_quick_actions() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    result = run_handlers._enforce_dispatch_policies(
        dispatch_mode=True,
        args=argparse.Namespace(require_verifier=True),
        key="twinpaper",
        entry={"project_alias": "O2"},
        selected_roles=["Local-Dev"],
        available_roles=["Local-Dev"],
        verifier_candidates=["Reviewer"],
        plan_gate_blocked=False,
        plan_gate_reason="",
        plan_replans=[],
        ensure_verifier_roles=lambda **kwargs: (["Local-Dev"], [], False, []),
        dispatch_roles="Local-Dev",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )

    assert result.terminal is True
    assert result.terminal_reason == "verifier gate: no verifier role is available"
    assert sent
    context, body, markup = sent[-1]
    assert context == "verifier-gate setup"
    assert "no verifier role is available" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/monitor" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons


def test_enforce_dispatch_policies_planning_gate_adds_project_quick_actions() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    result = run_handlers._enforce_dispatch_policies(
        dispatch_mode=True,
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry={"project_alias": "O2"},
        selected_roles=["Local-Dev"],
        available_roles=["Local-Dev", "Reviewer"],
        verifier_candidates=["Reviewer"],
        plan_gate_blocked=True,
        plan_gate_reason="critic unresolved after auto-replan",
        plan_replans=[{"attempt": 1}],
        ensure_verifier_roles=lambda **kwargs: (["Local-Dev"], ["Reviewer"], False, ["Reviewer"]),
        dispatch_roles="Local-Dev",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )

    assert result.terminal is True
    assert result.terminal_reason == "plan gate: critic unresolved after auto-replan"
    assert sent
    context, body, markup = sent[-1]
    assert context == "planning-gate"
    assert "plan gate blocked" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/monitor" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons


def test_run_guards_module_matches_run_guard_exports() -> None:
    assert run_handlers._confirm_required_reply_markup() == run_guards.confirm_required_reply_markup()
    assert run_handlers._rate_limit_reply_markup({"project_alias": "O2"}, "twinpaper") == run_guards.rate_limit_reply_markup({"project_alias": "O2"}, "twinpaper")
    assert run_handlers._rate_limit_reply_markup(None, "") == run_guards.rate_limit_reply_markup(None, "")

    guard_run = run_handlers._resolve_effective_run_options(
        p_args=argparse.Namespace(priority="P2", orch_timeout_sec=120, no_wait=False),
        run_priority_override="P1",
        run_timeout_override=30,
        run_no_wait_override=True,
    )
    guard_mod = run_guards.resolve_effective_run_options(
        p_args=argparse.Namespace(priority="P2", orch_timeout_sec=120, no_wait=False),
        run_priority_override="P1",
        run_timeout_override=30,
        run_no_wait_override=True,
    )
    assert guard_run == guard_mod

    preview_run = run_handlers._build_dry_run_preview(
        key="twinpaper",
        dispatch_mode=True,
        prompt="implement feature",
        dispatch_roles="Reviewer",
        require_verifier=True,
        verifier_roles=["Reviewer"],
        verifier_added=False,
        run_control_mode="retry",
        run_source_request_id="REQ-1",
        planning_enabled=True,
        reuse_source_plan=False,
        plan_data={"subtasks": [{"id": "S1"}]},
        plan_replans=[{"attempt": 1}],
        plan_gate_blocked=False,
        plan_error="",
        effective_priority="P1",
        effective_timeout=60,
        effective_no_wait=False,
    )
    preview_mod = run_guards.build_dry_run_preview(
        key="twinpaper",
        dispatch_mode=True,
        prompt="implement feature",
        dispatch_roles="Reviewer",
        require_verifier=True,
        verifier_roles=["Reviewer"],
        verifier_added=False,
        run_control_mode="retry",
        run_source_request_id="REQ-1",
        planning_enabled=True,
        reuse_source_plan=False,
        plan_data={"subtasks": [{"id": "S1"}]},
        plan_replans=[{"attempt": 1}],
        plan_gate_blocked=False,
        plan_error="",
        effective_priority="P1",
        effective_timeout=60,
        effective_no_wait=False,
    )
    assert preview_run == preview_mod

    sent_run: list[tuple[str, str, dict | None]] = []
    sent_mod: list[tuple[str, str, dict | None]] = []
    logged_run: list[dict] = []
    logged_mod: list[dict] = []
    common = dict(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
            chat_max_running=1,
            chat_daily_cap=20,
        ),
        manager_state=_empty_state(),
        chat_id="939062873",
        key="twinpaper",
        entry={"project_alias": "O2"},
        run_auto_source="default",
        run_force_mode="dispatch",
        orch_target="O2",
        prompt="implement feature",
        summarize_chat_usage=lambda manager_state, chat_id: (1, 0),
        detect_high_risk_prompt=lambda prompt: "",
        set_confirm_action=lambda *args, **kwargs: None,
        save_manager_state=lambda path, manager_state: None,
    )
    handled_run = run_handlers._handle_run_rate_limit_and_confirm(
        send=lambda body, **kwargs: sent_run.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged_run.append(kwargs),
        **common,
    )
    handled_mod = run_guards.handle_run_rate_limit_and_confirm(
        send=lambda body, **kwargs: sent_mod.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged_mod.append(kwargs),
        **common,
    )
    assert handled_run == handled_mod == True
    assert sent_run == sent_mod
    assert logged_run == logged_mod

    policy_sent_run: list[tuple[str, str, dict | None]] = []
    policy_sent_mod: list[tuple[str, str, dict | None]] = []
    policy_run = run_handlers._enforce_dispatch_policies(
        dispatch_mode=True,
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry={"project_alias": "O2"},
        selected_roles=["Local-Dev"],
        available_roles=["Local-Dev", "Reviewer"],
        verifier_candidates=["Reviewer"],
        plan_gate_blocked=True,
        plan_gate_reason="critic unresolved after auto-replan",
        plan_replans=[{"attempt": 1}],
        ensure_verifier_roles=lambda **kwargs: (["Local-Dev"], ["Reviewer"], False, ["Reviewer"]),
        dispatch_roles="Local-Dev",
        send=lambda body, **kwargs: policy_sent_run.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    policy_mod = run_guards.enforce_dispatch_policies(
        dispatch_mode=True,
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry={"project_alias": "O2"},
        selected_roles=["Local-Dev"],
        available_roles=["Local-Dev", "Reviewer"],
        verifier_candidates=["Reviewer"],
        plan_gate_blocked=True,
        plan_gate_reason="critic unresolved after auto-replan",
        plan_replans=[{"attempt": 1}],
        ensure_verifier_roles=lambda **kwargs: (["Local-Dev"], ["Reviewer"], False, ["Reviewer"]),
        dispatch_roles="Local-Dev",
        send=lambda body, **kwargs: policy_sent_mod.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    assert policy_run == policy_mod
    assert policy_sent_run == policy_sent_mod

    confirm_state_a = _empty_state()
    confirm_state_b = copy.deepcopy(confirm_state_a)
    gw.set_confirm_action(confirm_state_a, chat_id="939062873", mode="dispatch", prompt="rm -rf /tmp/demo", risk="destructive_delete")
    gw.set_confirm_action(confirm_state_b, chat_id="939062873", mode="dispatch", prompt="rm -rf /tmp/demo", risk="destructive_delete")
    saved_a: list[Path] = []
    saved_b: list[Path] = []
    confirm_sent_a: list[tuple[str, dict]] = []
    confirm_sent_b: list[tuple[str, dict]] = []

    result_a = run_handlers.resolve_confirm_run_transition(
        cmd="confirm-run",
        args=argparse.Namespace(dry_run=False, manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json", confirm_ttl_sec=300),
        manager_state=confirm_state_a,
        chat_id="939062873",
        orch_target="O2",
        send=lambda body, **kwargs: confirm_sent_a.append((body, kwargs)) or True,
        get_confirm_action=gw.get_confirm_action,
        parse_iso_ts=gw.parse_iso_ts,
        clear_confirm_action=gw.clear_confirm_action,
        save_manager_state=lambda path, manager_state: saved_a.append(path),
    )
    result_b = run_guards.resolve_confirm_run_transition(
        cmd="confirm-run",
        args=argparse.Namespace(dry_run=False, manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json", confirm_ttl_sec=300),
        manager_state=confirm_state_b,
        chat_id="939062873",
        orch_target="O2",
        send=lambda body, **kwargs: confirm_sent_b.append((body, kwargs)) or True,
        get_confirm_action=gw.get_confirm_action,
        parse_iso_ts=gw.parse_iso_ts,
        clear_confirm_action=gw.clear_confirm_action,
        save_manager_state=lambda path, manager_state: saved_b.append(path),
    )
    assert result_a == result_b
    assert confirm_state_a == confirm_state_b
    assert confirm_sent_a == confirm_sent_b
    assert saved_a == saved_b


def test_send_dispatch_result_adds_project_quick_actions_for_confirmed_result() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    handled = run_handlers._send_dispatch_result(
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry={"project_alias": "O2"},
        p_args=argparse.Namespace(),
        prompt="dangerous but approved",
        state={"complete": True},
        req_id="REQ-1",
        task=None,
        run_control_mode="normal",
        run_source_request_id="",
        run_auto_source="confirmed",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
        summarize_task_lifecycle=lambda key, task: "",
        synthesize_orchestrator_response=lambda p_args, prompt, state: "synthed",
        render_run_response=lambda state, task=None: "run result",
        finalize_request_reply_messages=lambda args, req_id: {},
    )

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "result"
    assert body == "run result"
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/todo O2" in buttons
    assert "/orch status O2" in buttons
    assert "/monitor" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons


def test_send_dispatch_result_adds_project_quick_actions_for_confirmed_synth() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    handled = run_handlers._send_dispatch_result(
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry={"project_alias": "O2"},
        p_args=argparse.Namespace(),
        prompt="dangerous but approved",
        state={"complete": True, "replies": [{"role": "Reviewer", "text": "ok"}]},
        req_id="REQ-1",
        task=None,
        run_control_mode="normal",
        run_source_request_id="",
        run_auto_source="confirmed",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
        summarize_task_lifecycle=lambda key, task: "",
        synthesize_orchestrator_response=lambda p_args, prompt, state: "synthed",
        render_run_response=lambda state, task=None: "run result",
        finalize_request_reply_messages=lambda args, req_id: {},
    )

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "synth"
    assert body == "synthed"
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/todo O2" in buttons
    assert "/orch status O2" in buttons
    assert "/monitor" in buttons


def test_send_dispatch_result_adds_project_quick_actions_for_verifier_gate_failed() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    handled = run_handlers._send_dispatch_result(
        args=argparse.Namespace(require_verifier=True),
        key="twinpaper",
        entry={"project_alias": "O2"},
        p_args=argparse.Namespace(),
        prompt="needs verification",
        state={"complete": True},
        req_id="REQ-9",
        task={"stages": {"verification": "failed"}},
        run_control_mode="normal",
        run_source_request_id="",
        run_auto_source="todo-next",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: None,
        summarize_task_lifecycle=lambda key, task: "verification failed summary",
        synthesize_orchestrator_response=lambda p_args, prompt, state: "synthed",
        render_run_response=lambda state, task=None: "run result",
        finalize_request_reply_messages=lambda args, req_id: {},
    )

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "verifier-gate failed"
    assert body == "verification failed summary"
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/task REQ-9" in buttons
    assert "/replan REQ-9" in buttons
    assert "/retry REQ-9" in buttons
    assert "/todo O2" in buttons
    assert "/orch status O2" in buttons
    assert "/monitor" in buttons


def test_send_exec_critic_intervention_adds_project_quick_actions() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    run_handlers._send_exec_critic_intervention(
        entry={"project_alias": "O2"},
        key="twinpaper",
        final_req_id="REQ-7",
        verdict="retry",
        reason="critic unresolved after repair",
        exec_attempt=2,
        exec_max_attempts=3,
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )

    assert sent
    context, body, markup = sent[-1]
    assert context == "exec-critic"
    assert "exec critic: intervention needed" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/task REQ-7" in buttons
    assert "/replan REQ-7" in buttons
    assert "/retry REQ-7" in buttons
    assert "/todo O2" in buttons
    assert "/orch status O2" in buttons
    assert "/monitor" in buttons


def test_send_dispatch_exception_adds_project_quick_actions() -> None:
    sent: list[tuple[str, str, dict | None]] = []

    run_handlers._send_dispatch_exception(
        entry={"project_alias": "O2"},
        key="twinpaper",
        todo_id="TODO-001",
        reason="missing orchestrator.json",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )

    assert sent
    context, body, markup = sent[-1]
    assert context == "dispatch-exception"
    assert "dispatch failed before request start" in body
    assert "- reason: missing orchestrator.json" in body
    assert "- todo: TODO-001" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/monitor" in buttons
    assert "/sync preview O2 1h" in buttons
    assert "/queue" in buttons
    assert "/map" in buttons


def test_exec_results_module_matches_run_response_exports() -> None:
    entry = {"project_alias": "O2"}
    assert run_handlers._confirmed_result_reply_markup(entry, "twinpaper") == exec_results.confirmed_result_reply_markup(entry, "twinpaper")
    assert run_handlers._early_gate_reply_markup(entry, "twinpaper") == exec_results.early_gate_reply_markup(entry, "twinpaper")
    assert run_handlers._intervention_reply_markup(entry, "twinpaper", "REQ-9") == exec_results.intervention_reply_markup(entry, "twinpaper", "REQ-9")

    sent_run: list[tuple[str, str, dict | None]] = []
    sent_mod: list[tuple[str, str, dict | None]] = []
    run_handlers._send_exec_critic_intervention(
        entry=entry,
        key="twinpaper",
        final_req_id="REQ-7",
        verdict="retry",
        reason="critic unresolved after repair",
        exec_attempt=2,
        exec_max_attempts=3,
        send=lambda body, **kwargs: sent_run.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    exec_results.send_exec_critic_intervention(
        entry=entry,
        key="twinpaper",
        final_req_id="REQ-7",
        verdict="retry",
        reason="critic unresolved after repair",
        exec_attempt=2,
        exec_max_attempts=3,
        send=lambda body, **kwargs: sent_mod.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    assert sent_run == sent_mod

    exc_run: list[tuple[str, str, dict | None]] = []
    exc_mod: list[tuple[str, str, dict | None]] = []
    run_handlers._send_dispatch_exception(
        entry=entry,
        key="twinpaper",
        todo_id="TODO-001",
        reason="missing orchestrator.json",
        send=lambda body, **kwargs: exc_run.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    exec_results.send_dispatch_exception(
        entry=entry,
        key="twinpaper",
        todo_id="TODO-001",
        reason="missing orchestrator.json",
        send=lambda body, **kwargs: exc_mod.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
    )
    assert exc_run == exc_mod

    result_run: list[tuple[str, str, dict | None]] = []
    result_mod: list[tuple[str, str, dict | None]] = []
    log_run: list[dict] = []
    log_mod: list[dict] = []
    common = dict(
        args=argparse.Namespace(require_verifier=False),
        key="twinpaper",
        entry=entry,
        p_args=argparse.Namespace(),
        prompt="dangerous but approved",
        state={"complete": True, "replies": [{"role": "Reviewer", "text": "ok"}]},
        req_id="REQ-1",
        task=None,
        run_control_mode="normal",
        run_source_request_id="",
        run_auto_source="confirmed",
        summarize_task_lifecycle=lambda key, task: "",
        synthesize_orchestrator_response=lambda p_args, prompt, state: "synthed",
        render_run_response=lambda state, task=None: "run result",
        finalize_request_reply_messages=lambda args, req_id: {},
    )
    handled_run = run_handlers._send_dispatch_result(
        send=lambda body, **kwargs: result_run.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: log_run.append(kwargs),
        **common,
    )
    handled_mod = exec_results.send_dispatch_result(
        send=lambda body, **kwargs: result_mod.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: log_mod.append(kwargs),
        **common,
    )
    assert handled_run == handled_mod == True
    assert result_run == result_mod
    assert log_run == log_mod


def test_handle_run_or_unknown_command_sends_dispatch_exception_and_returns_true(tmp_path: Path) -> None:
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(project_root),
                "team_dir": str(team_dir),
                "todos": [
                    {"id": "TODO-001", "summary": "open row", "priority": "P1", "status": "open"},
                ],
                "pending_todo": {"todo_id": "TODO-001", "chat_id": "939062873", "selected_at": "2026-03-07T00:00:00+0900"},
            }
        }
    }
    sent: list[tuple[str, str, dict | None]] = []
    logged: list[dict] = []
    saved: list[Path] = []

    ctx = run_handlers.build_run_context(
        cmd="run",
        args=argparse.Namespace(
            dry_run=False,
            manager_state_file=team_dir / "orch_manager_state.json",
            auto_dispatch=False,
            require_verifier=False,
            verifier_roles="",
            task_planning=False,
            plan_max_subtasks=6,
            plan_auto_replan=False,
            plan_replan_attempts=0,
            plan_block_on_critic=False,
            exec_critic=False,
            exec_critic_retry_max=3,
            chat_max_running=3,
            chat_daily_cap=20,
        ),
        manager_state=manager_state,
        chat_id="939062873",
        text="open row",
        rest="open row",
        orch_target="O2",
        run_prompt="open row",
        run_roles_override=None,
        run_priority_override=None,
        run_timeout_override=None,
        run_no_wait_override=None,
        run_force_mode="dispatch",
        run_auto_source="todo-next",
        run_control_mode="normal",
        run_source_request_id="",
        run_source_task={"todo_id": "TODO-001"},
    )

    deps = run_handlers.RunDeps(
        core=run_handlers.RunCoreDeps(
            send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
            log_event=lambda **kwargs: logged.append(kwargs),
            help_text=lambda: "help",
        ),
        guard=run_handlers.RunGuardDeps(
            summarize_chat_usage=lambda manager_state, chat_id: (0, 0),
            detect_high_risk_prompt=lambda prompt: "",
            set_confirm_action=lambda *args, **kwargs: None,
            save_manager_state=lambda path, manager_state: saved.append(path),
        ),
        planning=run_handlers.RunPlanningDeps(
            choose_auto_dispatch_roles=lambda *args, **kwargs: ["Local-Dev"],
            resolve_verifier_candidates=lambda text: [],
            load_orchestrator_roles=lambda team_dir: ["Local-Dev"],
            parse_roles_csv=lambda csv: [token for token in str(csv or "").split(",") if token],
            ensure_verifier_roles=lambda **kwargs: (kwargs.get("selected_roles", []), [], False, []),
            available_worker_roles=lambda roles: roles,
            normalize_task_plan_payload=lambda payload, **kwargs: payload or {},
            build_task_execution_plan=lambda **kwargs: {},
            critique_task_execution_plan=lambda **kwargs: {"approved": True, "issues": [], "recommendations": []},
            critic_has_blockers=lambda critic: False,
            repair_task_execution_plan=lambda **kwargs: {},
            plan_roles_from_subtasks=lambda payload: [],
            build_planned_dispatch_prompt=lambda prompt, plan_data, plan_critic: prompt,
        ),
        routing=run_handlers.RunRoutingDeps(
            get_context=lambda raw: (
                "twinpaper",
                manager_state["projects"]["twinpaper"],
                argparse.Namespace(
                    project_root=project_root,
                    team_dir=team_dir,
                    roles="Local-Dev",
                    priority="P2",
                    orch_timeout_sec=120,
                    no_wait=False,
                ),
            ),
            run_orchestrator_direct=lambda p_args, prompt: "direct",
            run_aoe_orch=lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("missing orchestrator.json")),
            finalize_request_reply_messages=lambda *args, **kwargs: {},
            touch_chat_recent_task_ref=lambda *args, **kwargs: None,
            set_chat_selected_task_ref=lambda *args, **kwargs: None,
            now_iso=lambda: "2026-03-07T01:00:00+0900",
            sync_task_lifecycle=lambda **kwargs: None,
            lifecycle_set_stage=lambda *args, **kwargs: None,
            summarize_task_lifecycle=lambda key, task: "",
            synthesize_orchestrator_response=lambda p_args, prompt, state: "",
            critique_task_result=lambda **kwargs: {"verdict": "success", "reason": ""},
            extract_todo_proposals=lambda *args, **kwargs: [],
            merge_todo_proposals=lambda **kwargs: {"created_count": 0, "created_ids": [], "duplicate_count": 0, "skipped_count": 0},
            render_run_response=lambda state, task=None: "result",
        ),
    )

    handled = run_handlers.handle_run_or_unknown_command(ctx=ctx, deps=deps)

    assert handled is True
    assert sent
    context, body, markup = sent[-1]
    assert context == "dispatch-exception"
    assert "missing orchestrator.json" in body
    buttons = [btn["text"] for row in (markup or {}).get("keyboard", []) for btn in row]
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert manager_state["projects"]["twinpaper"]["todos"][0]["status"] == "blocked"
    assert "pending_todo" not in manager_state["projects"]["twinpaper"]
    assert saved
    assert any(evt.get("event") == "dispatch_failed" and evt.get("error_code") == "E_DISPATCH" for evt in logged)


def test_todo_next_blocks_unready_project(tmp_path: Path) -> None:
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state = {
        "projects": {
            "twinpaper": {
                "name": "twinpaper",
                "display_name": "TwinPaper",
                "project_alias": "O2",
                "project_root": str(tmp_path / "TwinPaper"),
                "team_dir": str(team_dir),
                "todos": [
                    {
                        "id": "TODO-001",
                        "summary": "run broken project",
                        "priority": "P1",
                        "status": "open",
                    }
                ],
                "todo_seq": 1,
            }
        }
    }
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / "state.json"),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        orch_target="twinpaper",
        rest="next",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda target: ("twinpaper", manager_state["projects"]["twinpaper"], argparse.Namespace()),
        save_manager_state=lambda *args, **kwargs: None,
        now_iso=lambda: "2026-03-07T18:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    assert "todo next blocked: project runtime is not ready" in sent[-1]
    assert "/orch status O2" in sent[-1]


def test_todo_with_explicit_other_project_under_focus_returns_operator_message(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    twin_root = tmp_path / "TwinPaper"
    nano_root = tmp_path / "Nano"
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(twin_root),
        "team_dir": str(twin_root / ".aoe-team"),
        "tasks": {},
    }
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(nano_root),
        "team_dir": str(nano_root / ".aoe-team"),
        "tasks": {},
    }
    gw.set_project_lock(state, "twinpaper")
    sent: list[str] = []

    result = todo_handlers.handle_todo_command(
        cmd="todo",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / "state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="O3 next",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw_target: (
            lambda k, e: (k, e, argparse.Namespace(project_root=Path(e["project_root"]), team_dir=Path(e["team_dir"])))
        )(*gw.get_manager_project(state, raw_target)),
        save_manager_state=lambda *args, **kwargs: None,
        now_iso=lambda: "2026-03-07T18:05:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    assert "todo blocked by project lock" in sent[-1]
    assert "/focus off" in sent[-1]


def test_cleanup_terminal_todo_gate_blocks_pending_todo_and_clears_pending() -> None:
    entry = {
        "todos": [
            {
                "id": "TODO-001",
                "summary": "broken queued item",
                "priority": "P2",
                "status": "open",
            }
        ],
        "pending_todo": {
            "todo_id": "TODO-001",
            "chat_id": "939062873",
            "selected_at": "2026-03-06T23:43:25+0900",
        },
    }

    changed = run_handlers._cleanup_terminal_todo_gate(
        entry=entry,
        chat_id="939062873",
        todo_id="",
        pending_todo_used=False,
        run_auto_source="todo:next",
        reason="plan gate: critic unresolved after auto-replan",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    todo = entry["todos"][0]
    assert changed is True
    assert "pending_todo" not in entry
    assert todo["status"] == "blocked"
    assert todo["blocked_reason"] == "plan gate: critic unresolved after auto-replan"
    assert todo["updated_at"] == "2026-03-07T00:00:00+0900"


def test_finalize_todo_after_run_increments_blocked_count_and_clears_it_on_success() -> None:
    entry = {
        "todos": [
            {
                "id": "TODO-001",
                "summary": "blocked row",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_reason": "old plan gate",
            }
        ]
    }

    run_handlers._finalize_todo_after_run(
        entry=entry,
        todo_id="TODO-001",
        status="failed",
        exec_verdict="retry",
        exec_reason="critic unresolved",
        req_id="REQ-2",
        task={"short_id": "T-102"},
        now_iso=lambda: "2026-03-07T00:10:00+0900",
    )

    todo = entry["todos"][0]
    assert todo["status"] == "blocked"
    assert todo["blocked_count"] == 3
    assert todo["blocked_bucket"] == "manual_followup"
    assert todo["blocked_request_id"] == "REQ-2"
    assert todo["blocked_reason"] == "critic unresolved"

    run_handlers._finalize_todo_after_run(
        entry=entry,
        todo_id="TODO-001",
        status="completed",
        exec_verdict="success",
        exec_reason="",
        req_id="REQ-3",
        task={"short_id": "T-103"},
        now_iso=lambda: "2026-03-07T00:20:00+0900",
    )

    assert todo["status"] == "done"
    assert "blocked_count" not in todo
    assert "blocked_bucket" not in todo
    assert "blocked_reason" not in todo
    assert "blocked_request_id" not in todo


def test_manual_followup_alert_is_sent_only_once() -> None:
    entry = {
        "project_alias": "O4",
        "todos": [
            {
                "id": "TODO-004",
                "summary": "need owner input",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            }
        ],
    }
    sent: list[tuple[str, str, dict | None]] = []

    first = run_handlers._maybe_send_manual_followup_alert(
        entry=entry,
        todo_id="TODO-004",
        project_key="local_map_analysis",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        now_iso=lambda: "2026-03-07T00:30:00+0900",
    )
    second = run_handlers._maybe_send_manual_followup_alert(
        entry=entry,
        todo_id="TODO-004",
        project_key="local_map_analysis",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        now_iso=lambda: "2026-03-07T00:31:00+0900",
    )

    assert first is True
    assert second is False
    assert entry["todos"][0]["blocked_alerted_at"] == "2026-03-07T00:30:00+0900"
    assert len(sent) == 1
    assert sent[0][0] == "manual-followup-alert"
    assert "manual follow-up needed" in sent[0][1]
    assert "TODO-004" in sent[0][1]
    assert "/todo O4 followup" in sent[0][1]
    assert "/queue followup" in sent[0][1]
    markup = sent[0][2] or {}
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/todo O4 followup" in buttons
    assert "/queue followup" in buttons


def test_capture_todo_proposals_merges_and_alerts() -> None:
    entry = {
        "project_alias": "O2",
        "todos": [
            {
                "id": "TODO-010",
                "summary": "existing task",
                "priority": "P2",
                "status": "done",
            }
        ],
        "todo_seq": 10,
        "todo_proposals": [],
        "todo_proposal_seq": 0,
    }
    sent: list[tuple[str, str, dict | None]] = []
    logged: list[dict] = []

    result = run_handlers._maybe_capture_todo_proposals(
        args=argparse.Namespace(dry_run=False),
        entry=entry,
        key="twinpaper",
        p_args=argparse.Namespace(),
        prompt="run release prep",
        state={
            "complete": True,
            "replies": [
                {"role": "Local-Writer", "body": "Release note draft is done. We still need a deployment checklist."}
            ],
        },
        req_id="REQ-900",
        task={"todo_id": "TODO-010", "short_id": "T-900"},
        todo_id="TODO-010",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged.append(kwargs),
        now_iso=lambda: "2026-03-09T10:00:00+0900",
        extract_todo_proposals=lambda *args, **kwargs: [
            {
                "summary": "prepare deployment checklist",
                "priority": "P1",
                "kind": "handoff",
                "reason": "release note mentions it is still missing",
                "confidence": 0.88,
            }
        ],
        merge_todo_proposals=todo_handlers.merge_todo_proposals,
    )

    assert result["created_count"] == 1
    assert entry["todo_proposals"][0]["id"] == "PROP-001"
    assert entry["todo_proposals"][0]["source_request_id"] == "REQ-900"
    assert sent
    assert sent[-1][0] == "todo-proposals-alert"
    assert "new todo proposals" in sent[-1][1]
    assert "prepare deployment checklist" in sent[-1][1]
    buttons = [btn["text"] for row in (sent[-1][2] or {}).get("keyboard", []) for btn in row]
    assert "/todo proposals" in buttons
    assert "/todo accept PROP-001" in buttons
    assert any(evt.get("event") == "todo_proposals_created" for evt in logged)


def test_capture_todo_proposals_prefers_backend_native_payload() -> None:
    entry = {
        "project_alias": "O4",
        "todos": [],
        "todo_seq": 0,
        "todo_proposals": [],
        "todo_proposal_seq": 0,
    }
    sent: list[tuple[str, str, dict | None]] = []
    logged: list[dict] = []

    result = run_handlers._maybe_capture_todo_proposals(
        args=argparse.Namespace(dry_run=False),
        entry=entry,
        key="local_map_analysis",
        p_args=argparse.Namespace(),
        prompt="writer handoff prompt",
        state={
            "complete": True,
            "replies": [{"role": "Local-Writer", "body": "hints are present but should not be reparsed"}],
            "followup_proposals": [
                {
                    "summary": "Draft the machine-readable summary table from the canonical backlog",
                    "priority": "P1",
                    "kind": "handoff",
                    "reason": "backend-native writer handoff proposal",
                    "confidence": 0.81,
                }
            ],
        },
        req_id="REQ-901",
        task={"todo_id": "TODO-015", "short_id": "T-901"},
        todo_id="TODO-015",
        send=lambda body, **kwargs: sent.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged.append(kwargs),
        now_iso=lambda: "2026-03-11T21:00:00+0900",
        extract_todo_proposals=lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("extractor should not run when backend payload exists")),
        merge_todo_proposals=todo_handlers.merge_todo_proposals,
    )

    assert result["created_count"] == 1
    assert entry["todo_proposals"][0]["source_request_id"] == "REQ-901"
    assert entry["todo_proposals"][0]["kind"] == "handoff"
    assert any(evt.get("event") == "todo_proposals_backend_payload" for evt in logged)
    assert sent[-1][0] == "todo-proposals-alert"


def test_exec_pipeline_module_matches_run_terminal_todo_helpers() -> None:
    entry_a = {
        "project_alias": "O4",
        "todos": [
            {
                "id": "TODO-004",
                "summary": "need owner input",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            }
        ],
        "pending_todo": {
            "todo_id": "TODO-004",
            "chat_id": "939062873",
            "selected_at": "2026-03-06T23:43:25+0900",
        },
    }
    entry_b = copy.deepcopy(entry_a)
    sent_a: list[tuple[str, str, dict | None]] = []
    sent_b: list[tuple[str, str, dict | None]] = []

    changed_a = run_handlers._cleanup_terminal_todo_gate(
        entry=entry_a,
        chat_id="939062873",
        todo_id="",
        pending_todo_used=False,
        run_auto_source="todo:next",
        reason="plan gate: critic unresolved after auto-replan",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )
    changed_b = exec_pipeline.cleanup_terminal_todo_gate(
        entry=entry_b,
        chat_id="939062873",
        todo_id="",
        pending_todo_used=False,
        run_auto_source="todo:next",
        reason="plan gate: critic unresolved after auto-replan",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
        manual_followup_threshold=2,
    )

    assert changed_a == changed_b
    assert entry_a == entry_b

    first_a = run_handlers._maybe_send_manual_followup_alert(
        entry=entry_a,
        todo_id="TODO-004",
        project_key="local_map_analysis",
        send=lambda body, **kwargs: sent_a.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        now_iso=lambda: "2026-03-07T00:30:00+0900",
    )
    first_b = exec_pipeline.maybe_send_manual_followup_alert(
        entry=entry_b,
        todo_id="TODO-004",
        project_key="local_map_analysis",
        send=lambda body, **kwargs: sent_b.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        now_iso=lambda: "2026-03-07T00:30:00+0900",
    )

    assert first_a == first_b == True
    assert sent_a == sent_b
    assert entry_a == entry_b


def test_exec_pipeline_module_matches_run_dispatch_sync_and_proposal_capture() -> None:
    entry_a = {"project_alias": "O2", "last_request_id": ""}
    entry_b = copy.deepcopy(entry_a)
    manager_state_a = {"projects": {"twinpaper": entry_a}}
    manager_state_b = {"projects": {"twinpaper": entry_b}}
    touches_a: list[tuple] = []
    touches_b: list[tuple] = []
    selects_a: list[tuple] = []
    selects_b: list[tuple] = []

    def _run_aoe_orch(*_args, **_kwargs):
        return {"request_id": "REQ-123", "complete": False, "replies": []}

    def _sync_task_lifecycle(**kwargs):
        return {
            "request_id": kwargs["request_data"]["request_id"],
            "status": "running",
            "stages": {"verification": "pending"},
        }

    result_a = run_handlers._dispatch_and_sync_task(
        p_args=argparse.Namespace(),
        dispatch_prompt="dispatch prompt",
        chat_id="939062873",
        dispatch_roles="Reviewer",
        run_priority_override=None,
        run_timeout_override=None,
        run_no_wait_override=None,
        key="twinpaper",
        entry=entry_a,
        manager_state=manager_state_a,
        prompt="original prompt",
        selected_roles=["Reviewer"],
        verifier_roles=[],
        require_verifier=False,
        verifier_candidates=[],
        run_aoe_orch=_run_aoe_orch,
        touch_chat_recent_task_ref=lambda *args: touches_a.append(args),
        set_chat_selected_task_ref=lambda *args: selects_a.append(args),
        now_iso=lambda: "2026-03-11T10:00:00+09:00",
        sync_task_lifecycle=_sync_task_lifecycle,
    )
    result_b = exec_pipeline.dispatch_and_sync_task(
        p_args=argparse.Namespace(),
        dispatch_prompt="dispatch prompt",
        chat_id="939062873",
        dispatch_roles="Reviewer",
        run_priority_override=None,
        run_timeout_override=None,
        run_no_wait_override=None,
        key="twinpaper",
        entry=entry_b,
        manager_state=manager_state_b,
        prompt="original prompt",
        selected_roles=["Reviewer"],
        verifier_roles=[],
        require_verifier=False,
        verifier_candidates=[],
        run_aoe_orch=_run_aoe_orch,
        touch_chat_recent_task_ref=lambda *args: touches_b.append(args),
        set_chat_selected_task_ref=lambda *args: selects_b.append(args),
        now_iso=lambda: "2026-03-11T10:00:00+09:00",
        sync_task_lifecycle=_sync_task_lifecycle,
    )

    assert result_a == result_b
    assert entry_a == entry_b
    assert touches_a == touches_b
    assert selects_a == selects_b

    proposal_entry_a = {
        "project_alias": "O2",
        "todos": [{"id": "TODO-010", "summary": "existing task", "priority": "P2", "status": "done"}],
        "todo_seq": 10,
        "todo_proposals": [],
        "todo_proposal_seq": 0,
    }
    proposal_entry_b = copy.deepcopy(proposal_entry_a)
    sent_a: list[tuple[str, str, dict | None]] = []
    sent_b: list[tuple[str, str, dict | None]] = []
    logged_a: list[dict] = []
    logged_b: list[dict] = []

    common_kwargs = dict(
        args=argparse.Namespace(dry_run=False),
        key="twinpaper",
        p_args=argparse.Namespace(),
        prompt="run release prep",
        state={
            "complete": True,
            "replies": [{"role": "Local-Writer", "body": "Release note draft is done. We still need a deployment checklist."}],
        },
        req_id="REQ-900",
        task={"todo_id": "TODO-010", "short_id": "T-900"},
        todo_id="TODO-010",
        now_iso=lambda: "2026-03-09T10:00:00+0900",
        extract_todo_proposals=lambda *args, **kwargs: [
            {
                "summary": "prepare deployment checklist",
                "priority": "P1",
                "kind": "handoff",
                "reason": "release note mentions it is still missing",
                "confidence": 0.88,
            }
        ],
        merge_todo_proposals=todo_handlers.merge_todo_proposals,
    )

    proposals_a = run_handlers._maybe_capture_todo_proposals(
        entry=proposal_entry_a,
        send=lambda body, **kwargs: sent_a.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged_a.append(kwargs),
        **common_kwargs,
    )
    proposals_b = exec_pipeline.maybe_capture_todo_proposals(
        entry=proposal_entry_b,
        send=lambda body, **kwargs: sent_b.append((kwargs.get("context", ""), body, kwargs.get("reply_markup"))) or True,
        log_event=lambda **kwargs: logged_b.append(kwargs),
        **common_kwargs,
    )

    assert proposals_a == proposals_b
    assert proposal_entry_a == proposal_entry_b
    assert sent_a == sent_b
    assert logged_a == logged_b


def test_apply_scenario_items_to_entry_prunes_stale_sync_open_todos() -> None:
    entry = {
        "todos": [
            {
                "id": "TODO-001",
                "summary": "old sync row",
                "priority": "P2",
                "status": "open",
                "created_by": "sync:telegram:test",
            },
            {
                "id": "TODO-002",
                "summary": "manual row",
                "priority": "P2",
                "status": "open",
                "created_by": "manual:user",
            },
        ],
        "todo_seq": 2,
        "pending_todo": {"todo_id": "TODO-001", "chat_id": "939062873"},
    }

    counts = sched._apply_scenario_items_to_entry(
        entry=entry,
        items=[{"summary": "new sync row", "priority": "P1", "status": "open"}],
        chat_id="939062873",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
        dry_run=False,
        source_mode="fallback:files",
        sources=["TODO.md"],
        prune_missing=True,
    )

    assert counts["added"] == 1
    assert counts["pruned"] == 1
    assert "pending_todo" not in entry
    rows = {row["id"]: row for row in entry["todos"]}
    assert rows["TODO-001"]["status"] == "canceled"
    assert rows["TODO-001"]["canceled_reason"] == "sync_prune_missing"
    assert rows["TODO-002"]["status"] == "open"
    assert rows["TODO-003"]["summary"] == "new sync row"


def test_apply_scenario_items_to_entry_prunes_only_same_sync_group() -> None:
    entry = {
        "todos": [
            {
                "id": "TODO-001",
                "summary": "old project sync row",
                "priority": "P2",
                "status": "open",
                "created_by": "sync:telegram:test",
                "sync_managed": True,
                "sync_group": "todo_files",
            },
            {
                "id": "TODO-002",
                "summary": "old ops sync row",
                "priority": "P2",
                "status": "open",
                "created_by": "sync:telegram:test",
                "sync_managed": True,
                "sync_group": "ops",
            },
        ],
        "todo_seq": 2,
    }

    counts = sched._apply_scenario_items_to_entry(
        entry=entry,
        items=[
            {
                "summary": "fresh project sync row",
                "priority": "P1",
                "status": "open",
                "sync_group": "todo_files",
                "sync_source_class": "todo_file",
                "sync_confidence": 0.92,
            }
        ],
        chat_id="939062873",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
        dry_run=False,
        source_mode="fallback:files",
        sources=["notes/project_todo.md"],
        prune_missing=True,
    )

    rows = {row["id"]: row for row in entry["todos"]}
    assert counts["pruned"] == 1
    assert rows["TODO-001"]["status"] == "canceled"
    assert rows["TODO-002"]["status"] == "open"


def test_apply_scenario_items_to_entry_prunes_blocked_stale_sync_rows() -> None:
    entry = {
        "todos": [
            {
                "id": "TODO-001",
                "summary": "stale blocked sync row",
                "priority": "P2",
                "status": "blocked",
                "created_by": "sync:telegram:test",
                "blocked_reason": "old plan gate",
                "sync_group": "todo_files",
            },
        ],
        "todo_seq": 1,
    }

    counts = sched._apply_scenario_items_to_entry(
        entry=entry,
        items=[
            {
                "summary": "fresh project sync row",
                "priority": "P1",
                "status": "open",
                "sync_group": "todo_files",
                "sync_source_class": "todo_file",
                "sync_confidence": 0.92,
            }
        ],
        chat_id="939062873",
        now_iso=lambda: "2026-03-07T00:00:00+0900",
        dry_run=False,
        source_mode="fallback:files",
        sources=["notes/project_todo.md"],
        prune_missing=True,
    )

    rows = {row["id"]: row for row in entry["todos"]}
    assert counts["pruned"] == 1
    assert rows["TODO-001"]["status"] == "canceled"
    assert rows["TODO-001"]["canceled_reason"] == "sync_prune_missing"
    assert "blocked_reason" not in rows["TODO-001"]


def test_sync_replace_blocks_partial_scope_since_window(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    twin_root = tmp_path / "TwinPaper"
    (twin_root / ".aoe-team").mkdir(parents=True, exist_ok=True)
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(twin_root),
        "team_dir": str(twin_root / ".aoe-team"),
        "tasks": {},
    }

    sent: list[str] = []
    args = argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json")

    def _send(body: str, **_kwargs) -> bool:
        sent.append(body)
        return True

    def _get_context(raw_target: str | None):
        key, entry = gw.get_manager_project(state, raw_target)
        return key, entry, argparse.Namespace(project_root=Path(entry["project_root"]), team_dir=Path(entry["team_dir"]))

    result = sched.handle_scheduler_command(
        cmd="sync",
        args=args,
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="replace O2 1h",
        send=_send,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1]
    assert "sync prune blocked" in text
    assert "avoid canceling unrelated todos" in text
    assert "/sync replace <O#|name>" in text


def test_orch_status_reply_markup_contains_monitor_todo_sync_and_focus_controls(tmp_path: Path) -> None:
    manager_state = _empty_state()
    team_dir = tmp_path / "TwinPaper" / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    manager_state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(team_dir),
    }
    entry = manager_state["projects"]["twinpaper"]

    markup = orch_task_handlers._orch_status_reply_markup(manager_state, "twinpaper", entry)
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    for expected in [
        "/todo O2",
        "/todo O2 followup",
        "/orch monitor O2",
        "/sync preview O2 1h",
        "/sync O2 1h",
        "/use O2",
        "/focus O2",
        "/queue",
        "/next",
        "/map",
    ]:
        assert expected in buttons

    gw.set_project_lock(manager_state, "twinpaper")
    markup2 = orch_task_handlers._orch_status_reply_markup(manager_state, "twinpaper", entry)
    buttons2 = [btn["text"] for row in markup2.get("keyboard", []) for btn in row]
    assert "/focus off" in buttons2
    assert "/focus O2" not in buttons2


def test_resolve_message_command_auto_routes_plain_text_from_direct_bias() -> None:
    manager_state = gw.default_manager_state(ROOT, ROOT / ".aoe-team")
    gw.set_default_mode(manager_state, "939062873", "direct")

    resolved = resolver.resolve_message_command(
        text="결측치 규칙을 검토해줘",
        slash_only=False,
        manager_state=manager_state,
        chat_id="939062873",
        dry_run=True,
        manager_state_file=ROOT / ".aoe-team" / "orch_manager_state.json",
        get_pending_mode=gw.get_pending_mode,
        get_default_mode=gw.get_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        save_manager_state=lambda path, state: None,
    )

    assert resolved.cmd == "run"
    assert resolved.run_force_mode == "dispatch"
    assert resolved.run_auto_source == "default-intent"


def test_parse_quick_message_supports_routine_aliases() -> None:
    assert tg_parse.parse_quick_message("todo") == {"cmd": "todo", "rest": ""}
    assert tg_parse.parse_quick_message("다음 할일") == {"cmd": "todo", "rest": "next"}
    assert tg_parse.parse_quick_message("sync preview 1h") == {"cmd": "sync", "rest": "preview 1h"}
    assert tg_parse.parse_quick_message("동기화 미리보기 O2 3h") == {"cmd": "sync", "rest": "preview O2 3h"}
    assert tg_parse.parse_quick_message("오프데스크") == {"cmd": "offdesk", "rest": "status"}
    assert tg_parse.parse_quick_message("퇴근모드") == {"cmd": "offdesk", "rest": "on"}
    assert tg_parse.parse_quick_message("자동 상태") == {"cmd": "auto", "rest": "status"}


def test_parse_quick_message_keeps_non_command_plain_text_free() -> None:
    assert tg_parse.parse_quick_message("동기화가 계속 꼬이는 이유를 분석해줘") is None
    assert tg_parse.parse_quick_message("자동 실행을 검토해줘") is None


def _call_management_status(
    *,
    tmp_path: Path,
    manager_state: dict,
    cmd: str,
    rest: str,
) -> str:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    sent: list[str] = []
    ok = mgmt_handlers.handle_management_command(
        cmd=cmd,
        args=argparse.Namespace(
            dry_run=True,
            team_dir=team_dir,
            manager_state_file=team_dir / "orch_manager_state.json",
            default_report_level="normal",
        ),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest=rest,
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent.append(body) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=lambda chat_ref, state: "admin",
        is_owner_chat=lambda chat_ref, state: True,
        ensure_chat_aliases=lambda *args, **kwargs: {},
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    assert ok is True
    assert sent
    return sent[-1]


def _call_management_status_with_markup(
    *,
    tmp_path: Path,
    manager_state: dict,
    cmd: str,
    rest: str,
) -> tuple[str, dict | None]:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    sent: list[tuple[str, dict | None]] = []
    ok = mgmt_handlers.handle_management_command(
        cmd=cmd,
        args=argparse.Namespace(
            dry_run=True,
            team_dir=team_dir,
            manager_state_file=team_dir / "orch_manager_state.json",
            default_report_level="normal",
        ),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest=rest,
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    assert ok is True
    assert sent
    return sent[-1]


def _management_control_kwargs(
    *,
    tmp_path: Path,
    manager_state: dict,
    cmd: str,
    rest: str,
    sent: list[tuple[str, dict | None]],
) -> dict:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    return dict(
        cmd=cmd,
        args=argparse.Namespace(
            dry_run=True,
            team_dir=team_dir,
            manager_state_file=team_dir / "orch_manager_state.json",
            default_report_level="normal",
        ),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role="admin",
        rest=rest,
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_project_entry=mgmt_handlers._resolve_project_entry,
        project_lock_row=mgmt_handlers._project_lock_row,
        project_lock_label=mgmt_handlers._project_lock_label,
        parse_replace_sync_flag=mgmt_handlers._parse_replace_sync_flag,
        normalize_prefetch_token=mgmt_handlers._normalize_prefetch_token,
        prefetch_display=mgmt_handlers._prefetch_display,
        compact_reason=mgmt_handlers._compact_reason,
        status_report_level=mgmt_handlers._status_report_level,
        focused_project_snapshot_lines=mgmt_handlers._focused_project_snapshot_lines,
        ops_scope_summary=mgmt_handlers._ops_scope_summary,
        ops_scope_compact_lines=lambda state, limit, detail_level: mgmt_handlers._ops_scope_compact_lines(
            state, limit=limit, detail_level=detail_level
        ),
        offdesk_prepare_targets=mgmt_handlers._offdesk_prepare_targets,
        offdesk_prepare_project_report=mgmt_handlers._offdesk_prepare_project_report,
        offdesk_review_reply_markup=lambda flagged, clean=False: mgmt_handlers._offdesk_review_reply_markup(
            flagged, clean=clean
        ),
        offdesk_prepare_reply_markup=lambda reports, blocked_count=0, clean=False: mgmt_handlers._offdesk_prepare_reply_markup(
            reports, blocked_count=blocked_count, clean=clean
        ),
        auto_state_path=mgmt_handlers._auto_state_path,
        offdesk_state_path=mgmt_handlers._offdesk_state_path,
        load_auto_state=mgmt_handlers._load_auto_state,
        save_auto_state=mgmt_handlers._save_auto_state,
        load_offdesk_state=mgmt_handlers._load_offdesk_state,
        save_offdesk_state=mgmt_handlers._save_offdesk_state,
        scheduler_session_name=mgmt_handlers._scheduler_session_name,
        tmux_has_session=mgmt_handlers._tmux_has_session,
        tmux_auto_command=mgmt_handlers._tmux_auto_command,
        now_iso=mgmt_handlers._now_iso,
        default_auto_interval_sec=mgmt_handlers.DEFAULT_AUTO_INTERVAL_SEC,
        default_auto_idle_sec=mgmt_handlers.DEFAULT_AUTO_IDLE_SEC,
        default_auto_max_failures=mgmt_handlers.DEFAULT_AUTO_MAX_FAILURES,
        default_offdesk_command=mgmt_handlers.DEFAULT_OFFDESK_COMMAND,
        default_offdesk_prefetch=mgmt_handlers.DEFAULT_OFFDESK_PREFETCH,
        default_offdesk_prefetch_since=mgmt_handlers.DEFAULT_OFFDESK_PREFETCH_SINCE,
        default_offdesk_report_level=mgmt_handlers.DEFAULT_OFFDESK_REPORT_LEVEL,
        default_offdesk_room=mgmt_handlers.DEFAULT_OFFDESK_ROOM,
    )


def _management_chat_kwargs(
    *,
    tmp_path: Path,
    manager_state: dict,
    cmd: str,
    sent: list[tuple[str, dict | None]],
    mode_setting=None,
    lang_setting=None,
    report_setting=None,
    chat_role="admin",
) -> dict:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    return dict(
        cmd=cmd,
        args=argparse.Namespace(
            dry_run=True,
            team_dir=team_dir,
            manager_state_file=team_dir / "orch_manager_state.json",
            default_lang="ko",
            default_report_level="normal",
        ),
        manager_state=manager_state,
        chat_id="939062873",
        chat_role=chat_role,
        mode_setting=mode_setting,
        lang_setting=lang_setting,
        report_setting=report_setting,
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        cmd_prefix=mgmt_handlers._cmd_prefix,
    )


def _management_acl_kwargs(
    *,
    tmp_path: Path,
    manager_state: dict,
    cmd: str,
    rest: str,
    sent: list[tuple[str, dict | None]],
    current_chat_alias="owner",
    came_from_slash=True,
    acl_grant_scope=None,
    acl_grant_chat_id=None,
    acl_revoke_scope=None,
    acl_revoke_chat_id=None,
    args_override: argparse.Namespace | None = None,
) -> dict:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    args_obj = args_override or argparse.Namespace(
        dry_run=True,
        team_dir=team_dir,
        manager_state_file=team_dir / "orch_manager_state.json",
        default_lang="ko",
        default_report_level="normal",
        allow_chat_ids=set(),
        admin_chat_ids=set(),
        readonly_chat_ids=set(),
        deny_by_default=False,
        owner_only=False,
        owner_chat_id="939062873",
        owner_bootstrap_mode="dispatch",
    )
    return dict(
        cmd=cmd,
        args=args_obj,
        manager_state=manager_state,
        chat_id="939062873",
        current_chat_alias=current_chat_alias,
        rest=rest,
        came_from_slash=came_from_slash,
        acl_grant_scope=acl_grant_scope,
        acl_grant_chat_id=acl_grant_chat_id,
        acl_revoke_scope=acl_revoke_scope,
        acl_revoke_chat_id=acl_revoke_chat_id,
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
        project_lock_label=mgmt_handlers._project_lock_label,
    )


def _button_texts(markup: dict | None) -> list[str]:
    if not isinstance(markup, dict):
        return []
    return [btn["text"] for row in markup.get("keyboard", []) for btn in row if isinstance(btn, dict) and "text" in btn]


def test_offdesk_status_includes_focused_project_snapshot(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(tmp_path / "TwinPaper"),
        "team_dir": str(tmp_path / "TwinPaper" / ".aoe-team"),
        "todos": [
            {"id": "TODO-1", "summary": "review schema", "status": "open"},
            {"id": "TODO-2", "summary": "run critic", "status": "running"},
            {"id": "TODO-3", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "pending_todo": {"todo_id": "TODO-2"},
        "last_sync_at": "2026-03-06T11:55:00+0900",
        "last_sync_mode": "scenario",
        "tasks": {
            "REQ-1": {
                "short_id": "T-101",
                "prompt": "Review schema and summarize result",
                "status": "running",
                "updated_at": "2026-03-06T12:00:00+0900",
            }
        },
    }
    state["active"] = "twinpaper"
    gw.set_project_lock(state, "twinpaper")

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="status long")

    assert "project snapshot" in text
    assert "- project: O2 TwinPaper [locked]" in text
    assert "- todo: open=1 running=1 blocked=1 followup=1 pending=yes" in text
    assert "- blocked_head: TODO-3 x2 [manual_followup]" in text
    assert "- last_sync: scenario " in text
    assert "- last_task: T-101 Review schema and summarize result [running]" in text


def test_scheduler_control_module_matches_management_focus_transition(tmp_path: Path) -> None:
    state_a = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_b = copy.deepcopy(state_a)
    twin_root = tmp_path / "TwinPaper"
    twin_team = twin_root / ".aoe-team"
    twin_team.mkdir(parents=True, exist_ok=True)
    state_a["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(twin_root),
        "team_dir": str(twin_team),
        "todos": [],
    }
    state_b["projects"]["twinpaper"] = copy.deepcopy(state_a["projects"]["twinpaper"])

    sent_a: list[tuple[str, dict | None]] = []
    sent_b: list[tuple[str, dict | None]] = []

    ok_a = mgmt_handlers.handle_management_command(
        cmd="focus",
        args=argparse.Namespace(
            dry_run=True,
            team_dir=tmp_path / ".aoe-team",
            manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
            default_report_level="normal",
        ),
        manager_state=state_a,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="O2",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_b = scheduler_control.handle_scheduler_control_command(
        **_management_control_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="focus",
            rest="O2",
            sent=sent_b,
        )
    )

    assert ok_a == ok_b == True
    assert state_a == state_b
    assert sent_a == sent_b


def test_scheduler_control_module_matches_management_offdesk_prepare_and_panic(tmp_path: Path) -> None:
    state_a = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_b = copy.deepcopy(state_a)
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    project_entry = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "status": "open"},
            {"id": "TODO-002", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "todo_proposals": [{"id": "PROP-001", "summary": "shadow gate follow-up", "status": "open"}],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }
    state_a["projects"]["twinpaper"] = copy.deepcopy(project_entry)
    state_b["projects"]["twinpaper"] = copy.deepcopy(project_entry)

    sent_prepare_a: list[tuple[str, dict | None]] = []
    sent_prepare_b: list[tuple[str, dict | None]] = []
    body_a, markup_a = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state_a,
        cmd="offdesk",
        rest="prepare O2",
    )
    ok_b = scheduler_control.handle_scheduler_control_command(
        **_management_control_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="offdesk",
            rest="prepare O2",
            sent=sent_prepare_b,
        )
    )
    assert ok_b is True
    assert sent_prepare_b
    body_b, markup_b = sent_prepare_b[-1]
    assert body_a == body_b
    assert markup_a == markup_b

    sent_panic_a: list[tuple[str, dict | None]] = []
    sent_panic_b: list[tuple[str, dict | None]] = []
    state_c = copy.deepcopy(state_a)
    state_d = copy.deepcopy(state_b)
    ok_a2 = mgmt_handlers.handle_management_command(
        cmd="panic",
        args=argparse.Namespace(
            dry_run=True,
            team_dir=tmp_path / ".aoe-team",
            manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
            default_report_level="normal",
        ),
        manager_state=state_c,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="status",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_panic_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_b2 = scheduler_control.handle_scheduler_control_command(
        **_management_control_kwargs(
            tmp_path=tmp_path,
            manager_state=state_d,
            cmd="panic",
            rest="status",
            sent=sent_panic_b,
        )
    )
    assert ok_a2 == ok_b2 == True
    assert state_c == state_d
    assert sent_panic_a == sent_panic_b


def test_management_chat_module_matches_management_handler_modes(tmp_path: Path) -> None:
    state_a = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_b = copy.deepcopy(state_a)

    sent_a: list[tuple[str, dict | None]] = []
    sent_b: list[tuple[str, dict | None]] = []

    ok_a = mgmt_handlers.handle_management_command(
        cmd="mode",
        args=argparse.Namespace(
            dry_run=True,
            team_dir=tmp_path / ".aoe-team",
            manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
            default_lang="ko",
            default_report_level="normal",
        ),
        manager_state=state_a,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting="dispatch",
        lang_setting=None,
        report_setting=None,
        rest="",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_b = mgmt_chat.handle_chat_management_command(
        **_management_chat_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="mode",
            mode_setting="dispatch",
            sent=sent_b,
        )
    )

    assert ok_a == ok_b == True
    assert state_a == state_b
    assert sent_a == sent_b


def test_management_chat_module_matches_management_handler_tutorial_and_cancel(tmp_path: Path) -> None:
    state_a = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_b = copy.deepcopy(state_a)
    gw.set_pending_mode(state_a, "939062873", "dispatch")
    gw.set_pending_mode(state_b, "939062873", "dispatch")
    gw.set_confirm_action(state_a, chat_id="939062873", mode="dispatch", prompt="ship it", risk="high")
    gw.set_confirm_action(state_b, chat_id="939062873", mode="dispatch", prompt="ship it", risk="high")

    sent_tut_a: list[tuple[str, dict | None]] = []
    sent_tut_b: list[tuple[str, dict | None]] = []
    ok_tut_a = mgmt_handlers.handle_management_command(
        cmd="tutorial",
        args=argparse.Namespace(
            dry_run=True,
            team_dir=tmp_path / ".aoe-team",
            manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
            default_lang="ko",
            default_report_level="normal",
        ),
        manager_state=state_a,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_tut_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_tut_b = mgmt_chat.handle_chat_management_command(
        **_management_chat_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="tutorial",
            sent=sent_tut_b,
        )
    )
    assert ok_tut_a == ok_tut_b == True
    assert sent_tut_a == sent_tut_b

    sent_cancel_a: list[tuple[str, dict | None]] = []
    sent_cancel_b: list[tuple[str, dict | None]] = []
    state_c = copy.deepcopy(state_a)
    state_d = copy.deepcopy(state_b)
    ok_cancel_a = mgmt_handlers.handle_management_command(
        cmd="cancel-pending",
        args=argparse.Namespace(
            dry_run=True,
            team_dir=tmp_path / ".aoe-team",
            manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
            default_lang="ko",
            default_report_level="normal",
        ),
        manager_state=state_c,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_cancel_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_cancel_b = mgmt_chat.handle_chat_management_command(
        **_management_chat_kwargs(
            tmp_path=tmp_path,
            manager_state=state_d,
            cmd="cancel-pending",
            sent=sent_cancel_b,
        )
    )

    assert ok_cancel_a == ok_cancel_b == True
    assert state_c == state_d
    assert sent_cancel_a == sent_cancel_b


def test_management_acl_module_matches_management_handler_identity_and_grant(tmp_path: Path) -> None:
    state_a = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state_b = copy.deepcopy(state_a)

    sent_a: list[tuple[str, dict | None]] = []
    sent_b: list[tuple[str, dict | None]] = []
    args_a = argparse.Namespace(
        dry_run=True,
        team_dir=tmp_path / ".aoe-team",
        manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json",
        default_lang="ko",
        default_report_level="normal",
        allow_chat_ids=set(),
        admin_chat_ids=set(),
        readonly_chat_ids=set(),
        deny_by_default=False,
        owner_only=False,
        owner_chat_id="939062873",
        owner_bootstrap_mode="dispatch",
    )
    args_b = copy.deepcopy(args_a)

    ok_a = mgmt_handlers.handle_management_command(
        cmd="grant",
        args=args_a,
        manager_state=state_a,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="admin 12345",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_b = mgmt_acl.handle_acl_management_command(
        **_management_acl_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="grant",
            rest="admin 12345",
            sent=sent_b,
            args_override=args_b,
        )
    )

    assert ok_a == ok_b == True
    assert args_a.allow_chat_ids == args_b.allow_chat_ids
    assert args_a.admin_chat_ids == args_b.admin_chat_ids
    assert args_a.readonly_chat_ids == args_b.readonly_chat_ids
    assert sent_a == sent_b

    sent_who_a: list[tuple[str, dict | None]] = []
    sent_who_b: list[tuple[str, dict | None]] = []
    ok_who_a = mgmt_handlers.handle_management_command(
        cmd="whoami",
        args=args_a,
        manager_state=state_a,
        chat_id="939062873",
        chat_role="admin",
        current_chat_alias="owner",
        mode_setting=None,
        lang_setting=None,
        report_setting=None,
        rest="",
        came_from_slash=True,
        acl_grant_scope=None,
        acl_grant_chat_id=None,
        acl_revoke_scope=None,
        acl_revoke_chat_id=None,
        send=lambda body, **kwargs: sent_who_a.append((body, kwargs.get("reply_markup"))) or True,
        log_event=lambda *args, **kwargs: None,
        help_text=lambda: "help",
        get_default_mode=gw.get_default_mode,
        get_pending_mode=gw.get_pending_mode,
        get_chat_lang=gw.get_chat_lang,
        get_chat_report_level=gw.get_chat_report_level,
        get_chat_room=gw.get_chat_room,
        set_default_mode=gw.set_default_mode,
        set_pending_mode=gw.set_pending_mode,
        set_chat_lang=gw.set_chat_lang,
        set_chat_report_level=gw.set_chat_report_level,
        set_chat_room=gw.set_chat_room,
        clear_default_mode=gw.clear_default_mode,
        clear_pending_mode=gw.clear_pending_mode,
        clear_confirm_action=gw.clear_confirm_action,
        clear_chat_report_level=gw.clear_chat_report_level,
        save_manager_state=lambda *args, **kwargs: None,
        resolve_chat_role=gw.resolve_chat_role,
        is_owner_chat=gw.is_owner_chat,
        ensure_chat_aliases=gw.ensure_chat_aliases,
        find_chat_alias=lambda aliases, chat_ref: "",
        alias_table_summary=lambda aliases: "",
        resolve_chat_ref=lambda aliases, chat_ref: (str(chat_ref), ""),
        ensure_chat_alias=lambda *args, **kwargs: "owner",
        sync_acl_env_file=lambda args: None,
    )
    ok_who_b = mgmt_acl.handle_acl_management_command(
        **_management_acl_kwargs(
            tmp_path=tmp_path,
            manager_state=state_b,
            cmd="whoami",
            rest="",
            sent=sent_who_b,
            args_override=args_b,
        )
    )
    assert ok_who_a == ok_who_b == True
    assert sent_who_a == sent_who_b
def test_offdesk_flow_module_matches_management_prepare_report_and_markup(tmp_path: Path) -> None:
    project_root = tmp_path / "Proj3"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("- [ ] P1: review schema\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")

    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state["projects"]["proj3"] = {
        "name": "proj3",
        "display_name": "Proj3",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "todos": [
            {"id": "TODO-1", "summary": "review schema", "status": "open", "priority": "P1"},
            {
                "id": "TODO-2",
                "summary": "manual item",
                "status": "blocked",
                "blocked_bucket": "manual_followup",
                "blocked_count": 2,
                "blocked_reason": "need review",
            },
        ],
        "todo_proposals": [{"id": "PROP-1", "summary": "follow up", "status": "open"}],
        "last_sync_mode": "scenario",
        "last_sync_at": "2026-03-11T09:30:00+0900",
    }
    entry = state["projects"]["proj3"]

    report_a = mgmt_handlers._offdesk_prepare_project_report(state, "proj3", entry)
    report_b = offdesk_flow.offdesk_prepare_project_report(state, "proj3", entry)

    assert report_a == report_b
    assert mgmt_handlers._offdesk_review_reply_markup([report_a]) == offdesk_flow.offdesk_review_reply_markup([report_b])
    assert mgmt_handlers._offdesk_prepare_reply_markup([report_a], blocked_count=1) == offdesk_flow.offdesk_prepare_reply_markup([report_b], blocked_count=1)


def test_auto_status_includes_active_project_snapshot_without_lock(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Nano"),
        "team_dir": str(tmp_path / "Nano" / ".aoe-team"),
        "todos": [{"id": "TODO-1", "summary": "collect logs", "status": "open"}],
        "pending_todo": {},
        "last_sync_at": "2026-03-06T11:00:00+0900",
        "last_sync_mode": "fallback:files",
        "tasks": {},
    }
    state["active"] = "nano"

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="auto", rest="status long")

    assert "project snapshot" in text
    assert "- project: O3 Nano" in text
    assert "- todo: open=1 running=0 blocked=0 followup=0 pending=no" in text
    assert "- last_sync: fallback:files " in text
    assert "- last_task: -" in text


def test_auto_status_shows_replace_sync_prefetch_mode(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "939062873",
                "command": "fanout",
                "prefetch": "sync_recent",
                "prefetch_since": "12h",
                "prefetch_replace_sync": True,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state = gw.default_manager_state(tmp_path, team_dir)

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="auto", rest="status")

    assert "- prefetch: sync_recent+replace (full-scope; since ignored)" in text


def test_auto_status_short_compacts_failure_reason_and_uses_ops_summary(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "939062873",
                "command": "fanout",
                "prefetch": "sync_recent",
                "prefetch_replace_sync": True,
                "fail_count": 3,
                "fail_candidate": "local_map_analysis:TODO-003",
                "fail_reason": "plan gate: severity, confidence, conflict_flag cutline definition is missing so the candidate selection remains non-reproducible across retries and blocks automation.",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state = gw.default_manager_state(tmp_path, team_dir)
    state["projects"]["o3"] = {
        "name": "o3",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(tmp_path / "Nano"),
        "team_dir": str(tmp_path / "Nano" / ".aoe-team"),
        "todos": [{"id": "TODO-1", "summary": "collect logs", "priority": "P1", "status": "open"}],
    }
    state["projects"]["o4"] = {
        "name": "o4",
        "display_name": "Local_Map",
        "project_alias": "O4",
        "project_root": str(tmp_path / "Local_Map"),
        "team_dir": str(tmp_path / "Local_Map" / ".aoe-team"),
        "todos": [{"id": "TODO-2", "summary": "build memo", "priority": "P1", "status": "open"}],
    }
    state["active"] = "o4"

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="auto", rest="status short")

    assert "- report_view: short" in text
    assert "- fail_reason: plan gate:" in text
    assert "fail_reason_full" not in text
    assert "ops projects:" in text
    assert "project snapshot" not in text


def test_auto_status_long_includes_full_failure_reason_and_project_snapshot(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "939062873",
                "command": "next",
                "prefetch": "sync_recent",
                "prefetch_replace_sync": False,
                "fail_count": 1,
                "fail_candidate": "o4:TODO-002",
                "fail_reason": "plan gate: severity, confidence, conflict_flag cutline definition is missing so the candidate selection remains non-reproducible across retries and blocks automation.",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state = gw.default_manager_state(tmp_path, team_dir)
    state["projects"]["o4"] = {
        "name": "o4",
        "display_name": "Local_Map",
        "project_alias": "O4",
        "project_root": str(tmp_path / "Local_Map"),
        "team_dir": str(tmp_path / "Local_Map" / ".aoe-team"),
        "todos": [{"id": "TODO-2", "summary": "build memo", "priority": "P1", "status": "open"}],
        "last_sync_at": "2026-03-06T11:00:00+0900",
        "last_sync_mode": "scenario",
    }
    state["active"] = "o4"

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="auto", rest="status long")

    assert "- report_view: long" in text
    assert "- fail_reason_full: plan gate: severity, confidence, conflict_flag cutline definition is missing" in text
    assert "project snapshot" in text
    assert "ops projects:" in text


def test_offdesk_status_shows_replace_sync_prefetch_mode(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "auto_scheduler.json").write_text(
        json.dumps(
            {
                "enabled": True,
                "chat_id": "939062873",
                "command": "fanout",
                "prefetch": "sync_recent",
                "prefetch_since": "12h",
                "prefetch_replace_sync": True,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    state = gw.default_manager_state(tmp_path, team_dir)

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="status")

    assert "- auto_prefetch: sync_recent+replace (full-scope; since ignored)" in text


def test_offdesk_prepare_reports_runtime_queue_and_next_actions(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "status": "open"},
            {"id": "TODO-002", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "todo_proposals": [{"id": "PROP-001", "summary": "shadow gate follow-up", "status": "open"}],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="prepare O2")

    assert "offdesk prepare" in text
    assert "- scope: O2" in text
    assert "- O2 TwinPaper [warn]" in text
    assert "runtime: ready" in text
    assert "canonical: TODO.md" in text
    assert "scenario_include: TODO.md" in text
    assert "queue: open=1 running=0 blocked=1 followup=1 pending=no proposals=1" in text
    assert "syncback: done=0 reopen=1 append=0 blocked_notes=1" in text
    assert "blocked_head: TODO-002 x2 [manual_followup]" in text
    assert "warn: 1" in text
    assert "- /offdesk on" in text
    assert "- /sync preview O2 24h" in text
    assert "- /todo O2 syncback preview" in text


def test_offdesk_prepare_warns_when_syncback_drift_exists_without_other_issues(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Nano"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] current task\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-000", "summary": "current task", "priority": "P2", "status": "open"},
            {"id": "TODO-001", "summary": "phase1 rerun", "priority": "P1", "status": "done"},
        ],
        "todo_proposals": [],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="prepare O3")

    assert "- O3 Nano [warn]" in text
    assert "syncback: done=1 reopen=1 append=0 blocked_notes=0" in text
    assert "syncback pending (done=1 reopen=1 append=0 blocked_notes=0)" in text


def test_offdesk_review_surfaces_flagged_projects_and_next_actions(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    root_o2 = tmp_path / "TwinPaper"
    team_o2 = root_o2 / ".aoe-team"
    team_o2.mkdir(parents=True, exist_ok=True)
    (root_o2 / "TODO.md").write_text("# Tasks\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_o2 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o2 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(root_o2),
        "team_dir": str(team_o2),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "status": "open"},
            {"id": "TODO-002", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "todo_proposals": [{"id": "PROP-001", "summary": "shadow gate follow-up", "status": "open"}],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    root_o3 = tmp_path / "Nano"
    team_o3 = root_o3 / ".aoe-team"
    team_o3.mkdir(parents=True, exist_ok=True)
    (root_o3 / "TODO.md").write_text("# Tasks\n- [ ] current task\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_o3 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o3 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(root_o3),
        "team_dir": str(team_o3),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-000", "summary": "current task", "priority": "P2", "status": "open"},
            {"id": "TODO-001", "summary": "phase1 rerun", "priority": "P1", "status": "done"},
        ],
        "todo_proposals": [],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    text = _call_management_status(tmp_path=tmp_path, manager_state=state, cmd="offdesk", rest="review all")

    assert "offdesk review" in text
    assert "- reviewed: 2" in text
    assert "- flagged: 2" in text
    assert "- O2 TwinPaper [warn]" in text
    assert "do: /todo O2 syncback preview, /todo O2 proposals, /todo O2 followup, /sync preview O2 24h" in text
    assert "- O3 Nano [warn]" in text
    assert "do: /todo O3 syncback preview" in text
    assert "- resolve flagged items, then /offdesk on" in text


def test_offdesk_review_reply_markup_includes_flagged_project_drilldowns(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    root_o2 = tmp_path / "TwinPaper"
    team_o2 = root_o2 / ".aoe-team"
    team_o2.mkdir(parents=True, exist_ok=True)
    (root_o2 / "TODO.md").write_text("# Tasks\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_o2 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o2 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(root_o2),
        "team_dir": str(team_o2),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "status": "open"},
            {"id": "TODO-002", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "todo_proposals": [{"id": "PROP-001", "summary": "shadow gate follow-up", "status": "open"}],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review O2",
    )

    assert "offdesk review" in body
    buttons = _button_texts(markup)
    assert "/todo O2 syncback preview" in buttons
    assert "/todo O2 proposals" in buttons
    assert "/todo O2 followup" in buttons
    assert "/sync preview O2 24h" in buttons
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/offdesk prepare" in buttons
    assert "/map" in buttons
    assert "/help" in buttons


def test_offdesk_review_reply_markup_includes_clean_actions(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    root_o3 = tmp_path / "Nano"
    team_o3 = root_o3 / ".aoe-team"
    team_o3.mkdir(parents=True, exist_ok=True)
    todo_line = todo_policy.format_canonical_todo_line("P1", "current task", status="open")
    (root_o3 / "TODO.md").write_text(f"# Tasks\n{todo_line}\n", encoding="utf-8")
    (team_o3 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o3 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(root_o3),
        "team_dir": str(team_o3),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "current task", "priority": "P1", "status": "open"},
        ],
        "todo_proposals": [],
        "last_sync_at": "2026-03-10T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="review O3",
    )

    assert "offdesk review" in body
    assert "- status: clean" in body
    buttons = _button_texts(markup)
    assert "/offdesk on" in buttons
    assert "/auto status" in buttons
    assert "/offdesk prepare" in buttons
    assert "/map" in buttons
    assert "/help" in buttons


def test_offdesk_prepare_reply_markup_includes_flagged_project_drilldowns(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    project_root = tmp_path / "TwinPaper"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text("# Tasks\n- [ ] phase1 rerun\n", encoding="utf-8")
    (team_dir / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "phase1 rerun", "status": "open"},
            {"id": "TODO-002", "summary": "need owner input", "status": "blocked", "blocked_count": 2, "blocked_bucket": "manual_followup"},
        ],
        "todo_proposals": [{"id": "PROP-001", "summary": "shadow gate follow-up", "status": "open"}],
        "last_sync_at": "2026-03-07T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="prepare O2",
    )

    assert "offdesk prepare" in body
    buttons = _button_texts(markup)
    assert "/todo O2 syncback preview" in buttons
    assert "/todo O2 proposals" in buttons
    assert "/todo O2 followup" in buttons
    assert "/sync preview O2 24h" in buttons
    assert "/orch status O2" in buttons
    assert "/todo O2" in buttons
    assert "/offdesk review" in buttons
    assert "/map" in buttons
    assert "/queue" in buttons
    assert "/help" in buttons


def test_offdesk_prepare_reply_markup_includes_clean_actions(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")

    root_o3 = tmp_path / "Nano"
    team_o3 = root_o3 / ".aoe-team"
    team_o3.mkdir(parents=True, exist_ok=True)
    todo_line = todo_policy.format_canonical_todo_line("P1", "current task", status="open")
    (root_o3 / "TODO.md").write_text(f"# Tasks\n{todo_line}\n", encoding="utf-8")
    (team_o3 / "AOE_TODO.md").write_text("@include ../TODO.md\n", encoding="utf-8")
    (team_o3 / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(root_o3),
        "team_dir": str(team_o3),
        "runtime_ready": True,
        "todos": [
            {"id": "TODO-001", "summary": "current task", "priority": "P1", "status": "open"},
        ],
        "todo_proposals": [],
        "last_sync_at": "2026-03-10T20:00:00+0900",
        "last_sync_mode": "scenario",
    }

    body, markup = _call_management_status_with_markup(
        tmp_path=tmp_path,
        manager_state=state,
        cmd="offdesk",
        rest="prepare O3",
    )

    assert "offdesk prepare" in body
    buttons = _button_texts(markup)
    assert "/offdesk on" in buttons
    assert "/offdesk review" in buttons
    assert "/auto status" in buttons
    assert "/map" in buttons
    assert "/queue" in buttons
    assert "/help" in buttons


def test_auto_prefetch_plan_switches_to_replace_sync_full_scope() -> None:
    desc, commands = auto_sched._prefetch_plan("sync_recent", "12h", True)

    assert desc == "sync_recent+replace (full-scope; since ignored)"
    assert commands == [("/sync replace all quiet", "replace")]


def test_auto_prefetch_plan_uses_incremental_files_and_recent_when_replace_disabled() -> None:
    desc, commands = auto_sched._prefetch_plan("sync_recent", "3h", False)

    assert desc == "sync files+recent all since=3h quiet"
    assert commands == [
        ("/sync files all since 3h quiet", "files"),
        ("/sync recent all since 3h quiet", "recent"),
    ]


def _write_tf_exec_map(team_dir: Path, req_id: str, *, mode: str, workdir: Path, run_dir: Path) -> None:
    m = gw.load_tf_exec_map(team_dir)
    m[req_id] = {
        "request_id": req_id,
        "gateway_request_id": req_id,
        "created_at": "2026-02-27T00:00:00+0000",
        "mode": mode,
        # Keep repo_root non-existent to avoid invoking git in tests.
        "repo_root": str(team_dir / "_no_such_repo_"),
        "workdir": str(workdir),
        "run_dir": str(run_dir),
        "branch": "",
        "worktree_created": True,
        "status": "running",
    }
    gw.save_tf_exec_map(team_dir, m)


def test_cleanup_tf_exec_artifacts_success_only_prunes_failed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("AOE_TF_ARTIFACT_POLICY", raising=False)  # default: success-only
    monkeypatch.setenv("AOE_TF_EXEC_CACHE_TTL_HOURS", "0")

    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state_path = team_dir / "orch_manager_state.json"

    req_ok = "REQ-OK"
    req_fail = "REQ-FAIL"
    run_ok = team_dir / "tf_runs" / req_ok
    run_fail = team_dir / "tf_runs" / req_fail
    run_ok.mkdir(parents=True, exist_ok=True)
    run_fail.mkdir(parents=True, exist_ok=True)
    work_ok = tmp_path / "work_ok"
    work_fail = tmp_path / "work_fail"
    work_ok.mkdir(parents=True, exist_ok=True)
    work_fail.mkdir(parents=True, exist_ok=True)

    _write_tf_exec_map(team_dir, req_ok, mode="worktree", workdir=work_ok, run_dir=run_ok)
    _write_tf_exec_map(team_dir, req_fail, mode="worktree", workdir=work_fail, run_dir=run_fail)

    state = {
        "projects": {
            "default": {
                "tasks": {
                    req_ok: {"status": "completed", "exec_critic": {"verdict": "success"}},
                    req_fail: {"status": "failed", "exec_critic": {"verdict": "fail"}},
                }
            }
        }
    }

    gw.cleanup_tf_exec_artifacts(manager_state_path, state)

    tf_map = gw.load_tf_exec_map(team_dir)
    assert req_ok in tf_map
    assert req_fail not in tf_map
    assert run_ok.exists()
    assert work_ok.exists()
    assert not run_fail.exists()
    assert not work_fail.exists()


def test_cleanup_tf_exec_artifacts_none_prunes_all(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_TF_ARTIFACT_POLICY", "none")
    monkeypatch.setenv("AOE_TF_EXEC_CACHE_TTL_HOURS", "0")

    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state_path = team_dir / "orch_manager_state.json"

    req_ok = "REQ-OK"
    req_fail = "REQ-FAIL"
    run_ok = team_dir / "tf_runs" / req_ok
    run_fail = team_dir / "tf_runs" / req_fail
    run_ok.mkdir(parents=True, exist_ok=True)
    run_fail.mkdir(parents=True, exist_ok=True)
    work_ok = tmp_path / "work_ok"
    work_fail = tmp_path / "work_fail"
    work_ok.mkdir(parents=True, exist_ok=True)
    work_fail.mkdir(parents=True, exist_ok=True)

    _write_tf_exec_map(team_dir, req_ok, mode="worktree", workdir=work_ok, run_dir=run_ok)
    _write_tf_exec_map(team_dir, req_fail, mode="worktree", workdir=work_fail, run_dir=run_fail)

    state = {
        "projects": {
            "default": {
                "tasks": {
                    req_ok: {"status": "completed", "exec_critic": {"verdict": "success"}},
                    req_fail: {"status": "failed", "exec_critic": {"verdict": "fail"}},
                }
            }
        }
    }

    gw.cleanup_tf_exec_artifacts(manager_state_path, state)

    tf_map = gw.load_tf_exec_map(team_dir)
    assert tf_map == {}
    assert not run_ok.exists()
    assert not work_ok.exists()
    assert not run_fail.exists()
    assert not work_fail.exists()


def test_cleanup_tf_exec_artifacts_all_keeps_all(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("AOE_TF_ARTIFACT_POLICY", "all")
    monkeypatch.setenv("AOE_TF_EXEC_CACHE_TTL_HOURS", "0")

    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    manager_state_path = team_dir / "orch_manager_state.json"

    req_ok = "REQ-OK"
    req_fail = "REQ-FAIL"
    run_ok = team_dir / "tf_runs" / req_ok
    run_fail = team_dir / "tf_runs" / req_fail
    run_ok.mkdir(parents=True, exist_ok=True)
    run_fail.mkdir(parents=True, exist_ok=True)
    work_ok = tmp_path / "work_ok"
    work_fail = tmp_path / "work_fail"
    work_ok.mkdir(parents=True, exist_ok=True)
    work_fail.mkdir(parents=True, exist_ok=True)

    _write_tf_exec_map(team_dir, req_ok, mode="worktree", workdir=work_ok, run_dir=run_ok)
    _write_tf_exec_map(team_dir, req_fail, mode="worktree", workdir=work_fail, run_dir=run_fail)

    state = {
        "projects": {
            "default": {
                "tasks": {
                    req_ok: {"status": "completed", "exec_critic": {"verdict": "success"}},
                    req_fail: {"status": "failed", "exec_critic": {"verdict": "fail"}},
                }
            }
        }
    }

    gw.cleanup_tf_exec_artifacts(manager_state_path, state)

    tf_map = gw.load_tf_exec_map(team_dir)
    assert req_ok in tf_map
    assert req_fail in tf_map
    assert run_ok.exists()
    assert work_ok.exists()
    assert run_fail.exists()
    assert work_fail.exists()


def test_sync_preview_uses_fallback_without_mutating_queue(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    entry = state["projects"]["default"]
    entry["project_alias"] = "O1"
    project_root = Path(str(entry["project_root"]))
    team_dir = Path(str(entry["team_dir"]))
    project_root.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    (project_root / "TODO.md").write_text(
        "# TODO\n\n- [ ] P1: finish fallback preview\n- [ ] P2: verify sync preview\n",
        encoding="utf-8",
    )

    sent: list[str] = []
    saves: list[Path] = []
    args = argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json")

    def _send(body: str, **_kwargs) -> bool:
        sent.append(body)
        return True

    def _get_context(raw_target: str | None):
        token = str(raw_target or "").strip()
        if token in {"", "default", "O1"}:
            return "default", entry, argparse.Namespace(project_root=project_root, team_dir=team_dir)
        raise RuntimeError(f"unexpected target: {token}")

    result = sched.handle_scheduler_command(
        cmd="sync",
        args=args,
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="preview O1",
        send=_send,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: saves.append(path),
        now_iso=lambda: "2026-03-06T12:00:00+0900",
    )

    assert result == {"terminal": True}
    assert saves == []
    assert sent
    text = sent[-1]
    assert "sync preview" in text
    assert "mode: fallback:files" in text
    assert "TODO.md -> used:2" in text
    assert "would_add: 2" in text
    assert "finish fallback preview" in text
    assert entry.get("todos") in (None, [])


def test_sync_with_explicit_other_project_under_focus_returns_operator_message(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    twin_root = tmp_path / "TwinPaper"
    nano_root = tmp_path / "Nano"
    state["projects"]["twinpaper"] = {
        "name": "twinpaper",
        "display_name": "TwinPaper",
        "project_alias": "O2",
        "project_root": str(twin_root),
        "team_dir": str(twin_root / ".aoe-team"),
        "tasks": {},
    }
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O3",
        "project_root": str(nano_root),
        "team_dir": str(nano_root / ".aoe-team"),
        "tasks": {},
    }
    gw.set_project_lock(state, "twinpaper")

    sent: list[str] = []
    args = argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json")

    def _send(body: str, **_kwargs) -> bool:
        sent.append(body)
        return True

    def _get_context(raw_target: str | None):
        key, entry = gw.get_manager_project(state, raw_target)
        return key, entry, argparse.Namespace(project_root=Path(entry["project_root"]), team_dir=Path(entry["team_dir"]))

    result = sched.handle_scheduler_command(
        cmd="sync",
        args=args,
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="O3",
        send=_send,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-06T12:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1]
    assert "sync blocked by project lock" in text
    assert "- locked: O2" in text
    assert "- requested: O3" in text
    assert "/sync preview O2 1h" in text
    assert "/focus off" in text


def test_next_selects_open_todo_even_when_project_has_blocked_row(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Local"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {},
        "todos": [
            {"id": "TODO-001", "summary": "blocked row", "priority": "P1", "status": "blocked"},
            {"id": "TODO-002", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }

    sent: list[str] = []
    saves: list[Path] = []

    def _send(body: str, **_kwargs) -> bool:
        sent.append(body)
        return True

    def _get_context(raw_target: str | None):
        key, entry = gw.get_manager_project(state, raw_target)
        return key, entry, argparse.Namespace(project_root=Path(entry["project_root"]), team_dir=Path(entry["team_dir"]))

    result = sched.handle_scheduler_command(
        cmd="next",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="",
        send=_send,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: saves.append(path),
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result["terminal"] is False
    assert result["cmd"] == "run"
    assert result["orch_target"] == "local"
    assert result["run_prompt"] == "open row"
    assert state["projects"]["local"]["pending_todo"]["todo_id"] == "TODO-002"
    assert sent
    assert "next selected (global)" in sent[-1]


def test_next_selected_warns_when_manual_followup_blocked_backlog_exists(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Local"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "orchestrator.json").write_text("{}", encoding="utf-8")
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {},
        "todos": [
            {
                "id": "TODO-001",
                "summary": "blocked row",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            },
            {"id": "TODO-002", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }

    sent: list[str] = []

    def _get_context(raw_target: str | None):
        key, entry = gw.get_manager_project(state, raw_target)
        return key, entry, argparse.Namespace(project_root=Path(entry["project_root"]), team_dir=Path(entry["team_dir"]))

    result = sched.handle_scheduler_command(
        cmd="next",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=_get_context,
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result["terminal"] is False
    assert sent
    assert "attention: blocked backlog TODO-001 x2 [manual_followup] | critic unresolved after repair" in sent[-1]


def test_queue_includes_blocked_head_summary(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Local"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {},
        "todos": [
            {
                "id": "TODO-001",
                "summary": "blocked row",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 3,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            },
            {"id": "TODO-002", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }

    sent: list[str] = []

    result = sched.handle_scheduler_command(
        cmd="queue",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("local", state["projects"]["local"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1]
    assert "open=1 running=0 blocked=1 done=0 followup=1" in text
    assert "blocked_head: TODO-001 x3 [manual_followup] | critic unresolved after repair" in text


def test_queue_reply_markup_includes_followup_button_when_present(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    project_root = tmp_path / "Local"
    team_dir = project_root / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(project_root),
        "team_dir": str(team_dir),
        "tasks": {},
        "todos": [
            {
                "id": "TODO-001",
                "summary": "blocked row",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            },
            {"id": "TODO-002", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }

    sent: list[tuple[str, dict | None]] = []

    result = sched.handle_scheduler_command(
        cmd="queue",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="",
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_context=lambda raw: ("local", state["projects"]["local"], argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    markup = sent[-1][1] or {}
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/queue followup" in buttons
    assert "/todo O3" in buttons
    assert "/orch status O3" in buttons


def test_queue_followup_filters_to_projects_with_manual_followup(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    local_root = tmp_path / "Local"
    nano_root = tmp_path / "Nano"
    (local_root / ".aoe-team").mkdir(parents=True, exist_ok=True)
    (nano_root / ".aoe-team").mkdir(parents=True, exist_ok=True)
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(local_root),
        "team_dir": str(local_root / ".aoe-team"),
        "tasks": {},
        "todos": [
            {
                "id": "TODO-001",
                "summary": "blocked row",
                "priority": "P1",
                "status": "blocked",
                "blocked_count": 2,
                "blocked_bucket": "manual_followup",
                "blocked_reason": "critic unresolved after repair",
            }
        ],
    }
    state["projects"]["nano"] = {
        "name": "nano",
        "display_name": "Nano",
        "project_alias": "O4",
        "project_root": str(nano_root),
        "team_dir": str(nano_root / ".aoe-team"),
        "tasks": {},
        "todos": [
            {"id": "TODO-010", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }

    sent: list[tuple[str, dict | None]] = []

    result = sched.handle_scheduler_command(
        cmd="queue",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="followup",
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_context=lambda raw: ("local", state["projects"]["local"], argparse.Namespace(project_root=local_root, team_dir=local_root / ".aoe-team")),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1][0]
    assert "manual follow-up queue" in text
    assert "O3 Local" in text
    assert "followup=1" in text
    assert "O4 Nano" not in text
    markup = sent[-1][1] or {}
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/todo O3 followup" in buttons
    assert "/todo O3 ackrun 1" in buttons
    assert "/orch status O3" in buttons
    assert "/queue" in buttons


def test_queue_followup_empty_includes_focused_project_drilldown(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    local_root = tmp_path / "Local"
    (local_root / ".aoe-team").mkdir(parents=True, exist_ok=True)
    state["projects"]["local"] = {
        "name": "local",
        "display_name": "Local",
        "project_alias": "O3",
        "project_root": str(local_root),
        "team_dir": str(local_root / ".aoe-team"),
        "tasks": {},
        "todos": [
            {"id": "TODO-010", "summary": "open row", "priority": "P2", "status": "open"},
        ],
    }
    gw.set_project_lock(state, "local")

    sent: list[tuple[str, dict | None]] = []

    result = sched.handle_scheduler_command(
        cmd="queue",
        args=argparse.Namespace(dry_run=False, manager_state_file=tmp_path / ".aoe-team" / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="followup",
        send=lambda body, **kwargs: sent.append((body, kwargs.get("reply_markup"))) or True,
        get_context=lambda raw: ("local", state["projects"]["local"], argparse.Namespace(project_root=local_root, team_dir=local_root / ".aoe-team")),
        save_manager_state=lambda path, manager_state: None,
        now_iso=lambda: "2026-03-07T00:00:00+0900",
    )

    assert result == {"terminal": True}
    assert sent
    text = sent[-1][0]
    assert text == "manual follow-up queue: empty."
    markup = sent[-1][1] or {}
    buttons = [btn["text"] for row in markup.get("keyboard", []) for btn in row]
    assert "/todo O3 followup" in buttons
    assert "/todo O3" in buttons
    assert "/orch status O3" in buttons
    assert "/focus off" in buttons


def test_sync_records_last_sync_even_when_queue_is_unchanged(tmp_path: Path) -> None:
    state = gw.default_manager_state(tmp_path, tmp_path / ".aoe-team")
    entry = state["projects"]["default"]
    entry["project_alias"] = "O1"
    project_root = Path(str(entry["project_root"]))
    team_dir = Path(str(entry["team_dir"]))
    project_root.mkdir(parents=True, exist_ok=True)
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "AOE_TODO.md").write_text("# TODO\n\n- [ ] P1: keep same task\n", encoding="utf-8")
    entry["todos"] = [
        {
            "id": "TODO-001",
            "summary": "keep same task",
            "priority": "P1",
            "status": "open",
            "created_at": "2026-03-05T10:00:00+0900",
            "updated_at": "2026-03-05T10:00:00+0900",
        }
    ]

    sent: list[str] = []
    saves: list[Path] = []
    result = sched.handle_scheduler_command(
        cmd="sync",
        args=argparse.Namespace(dry_run=False, manager_state_file=team_dir / "orch_manager_state.json"),
        manager_state=state,
        chat_id="939062873",
        chat_role="admin",
        orch_target=None,
        rest="O1",
        send=lambda body, **kwargs: sent.append(body) or True,
        get_context=lambda raw: ("default", entry, argparse.Namespace(project_root=project_root, team_dir=team_dir)),
        save_manager_state=lambda path, manager_state: saves.append(path),
        now_iso=lambda: "2026-03-06T12:00:00+0900",
    )

    assert result == {"terminal": True}
    assert saves == [team_dir / "orch_manager_state.json"]
    assert entry["last_sync_at"] == "2026-03-06T12:00:00+0900"
    assert entry["last_sync_mode"] == "scenario"


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
        "roles": ["Local-Analyst", "Reviewer"],
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
            roles_override="Reviewer",
            priority_override="P1",
            timeout_override=30,
            no_wait_override=True,
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
    assert calls["kwargs"]["roles_override"] == "Reviewer"
    assert calls["kwargs"]["priority_override"] == "P1"
    assert calls["kwargs"]["timeout_override"] == 30
    assert calls["kwargs"]["no_wait_override"] is True


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
                roles_override="Local-Analyst,Reviewer",
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
        assert result["replies"][0]["role"] == "Local-Analyst"
        assert result["replies"][1]["role"] == "Reviewer"
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
        roles_override="Local-Analyst,Reviewer",
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
        roles_override="Local-Writer,Reviewer",
    )

    assert result["backend"] == "autogen_core"
    assert result["complete"] is True
    assert result["replies"][0]["role"] == "Local-Writer"
    assert "Local-Writer handoff" in result["replies"][0]["body"]
    assert "Local-Writer, Reviewer" in result["replies"][1]["body"]
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
        result = gw.run_aoe_orch(args, "review this", "chat-1", roles_override="Reviewer")
    finally:
        gw.tf_backend_local_mod.local_backend = original_local_backend
        gw.tf_backend_autogen_mod.autogen_core_backend = original_autogen_backend

    assert result["backend"] == "local"
    assert result["backend_selection_reason"] == "default_local"
    assert calls["request"].prompt == "review this"
    assert calls["request"].roles_override == "Reviewer"
    assert calls["deps"].default_tf_exec_map_file == gw.DEFAULT_TF_EXEC_MAP_FILE


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
