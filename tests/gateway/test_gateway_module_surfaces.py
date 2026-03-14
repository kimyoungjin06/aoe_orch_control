#!/usr/bin/env python3
"""Gateway module surface and parity regression tests."""

from _gateway_test_support import *  # noqa: F401,F403

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
    (team_dir / "agents" / "Codex-Reviewer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Codex-Dev").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Codex-Reviewer" / "AGENTS.md").write_text(
        "# AGENTS.md - Codex-Reviewer\n\n## Mission\nFind risks, regressions, and missing tests before merge.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Codex-Dev" / "AGENTS.md").write_text(
        "# AGENTS.md - Codex-Dev\n\n## Mission\nImplement code changes and fix application bugs.\n",
        encoding="utf-8",
    )
    (team_dir / "orchestrator.json").write_text(
        json.dumps(
            {
                "coordinator": {"role": "Orchestrator"},
                "agents": [{"role": "Codex-Reviewer"}, {"role": "Codex-Dev"}],
            }
        ),
        encoding="utf-8",
    )

    assert gw.parse_roles_csv("Codex-Reviewer, Codex-Dev,Codex-Reviewer") == orch_roles.parse_roles_csv("Codex-Reviewer, Codex-Dev,Codex-Reviewer")
    assert orch_roles.parse_roles_csv("Reviewer,Codex-Dev") == ["Codex-Reviewer", "Codex-Dev"]
    assert gw.load_orchestrator_roles(team_dir) == orch_roles.load_orchestrator_roles(team_dir)
    assert gw.load_orchestrator_role_profiles(team_dir) == orch_roles.load_orchestrator_role_profiles(team_dir)
    assert gw.resolve_verifier_candidates("") == orch_roles.resolve_verifier_candidates("", default_verifier_roles=gw.DEFAULT_VERIFIER_ROLES)
    assert gw.ensure_verifier_roles(["Codex-Dev"], ["Codex-Reviewer", "Codex-Dev"], ["Codex-Reviewer"]) == orch_roles.ensure_verifier_roles(
        ["Codex-Dev"],
        ["Codex-Reviewer", "Codex-Dev"],
        ["Codex-Reviewer"],
    )
    assert gw.choose_auto_dispatch_roles(
        "로그인 버그를 수정하고 회귀 리스크도 같이 검토해줘.",
        available_roles=["Codex-Dev", "Codex-Reviewer"],
        team_dir=team_dir,
    ) == orch_roles.choose_auto_dispatch_roles(
        "로그인 버그를 수정하고 회귀 리스크도 같이 검토해줘.",
        available_roles=["Codex-Dev", "Codex-Reviewer"],
        team_dir=team_dir,
    )
    assert gw.available_worker_roles([]) == orch_roles.available_worker_roles([])


def test_orch_roles_canonicalize_legacy_local_roles(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Local-Writer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Local-Writer" / "AGENTS.md").write_text(
        "# AGENTS.md - Local-Writer\n\n## Mission\nWrite concise project documents.\n",
        encoding="utf-8",
    )
    (team_dir / "orchestrator.json").write_text(
        json.dumps(
            {
                "coordinator": {"role": "Orchestrator"},
                "agents": [{"role": "Local-Writer"}],
            }
        ),
        encoding="utf-8",
    )

    assert orch_roles.parse_roles_csv("Local-Writer,Codex-Reviewer") == ["Codex-Writer", "Codex-Reviewer"]
    assert orch_roles.load_orchestrator_roles(team_dir) == ["Orchestrator", "Codex-Writer"]
    profiles = orch_roles.load_orchestrator_role_profiles(team_dir)
    assert profiles[1]["role"] == "Codex-Writer"
    assert profiles[1]["mission"] == "Write concise project documents."


def test_ensure_verifier_roles_adds_reviewer_pair_for_worklike_team() -> None:
    selected, verifier_roles, added, available = orch_roles.ensure_verifier_roles(
        ["Codex-Writer", "Claude-Writer"],
        ["Codex-Writer", "Claude-Writer", "Codex-Reviewer", "Claude-Reviewer"],
        ["Codex-Reviewer", "Claude-Reviewer"],
    )

    assert added is True
    assert available == ["Codex-Reviewer", "Claude-Reviewer"]
    assert verifier_roles == ["Codex-Reviewer", "Claude-Reviewer"]
    assert selected == ["Codex-Writer", "Claude-Writer", "Codex-Reviewer", "Claude-Reviewer"]


def test_runtime_seed_migrates_legacy_local_roles_to_codex_names(tmp_path: Path) -> None:
    template_root = ROOT / "templates" / "aoe-team"
    project_root = tmp_path / "project"
    team_dir = project_root / ".aoe-team"
    project_root.mkdir(parents=True, exist_ok=True)

    spec = {
        "coordinator": {"role": "Orchestrator", "provider": "codex", "launch": "codex"},
        "agents": [{"role": "Local-Writer", "provider": "codex", "launch": "codex"}],
    }

    logs = runtime_seed.seed_runtime_from_spec(
        template_root=template_root,
        project_root=project_root,
        team_dir=team_dir,
        overview="demo",
        spec=spec,
        force=True,
    )

    orch = json.loads((team_dir / "orchestrator.json").read_text(encoding="utf-8"))
    roles = [row["role"] for row in orch["agents"]]
    assert "Codex-Writer" in roles
    assert "Local-Writer" not in roles
    assert (team_dir / "agents" / "Codex-Writer" / "AGENTS.md").exists()
    assert (team_dir / "workers" / "Codex-Writer.json").exists()
    assert any("Codex-Writer" in row for row in logs)


def test_choose_auto_dispatch_roles_normalizes_legacy_local_names_and_adds_review_pair(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    (team_dir / "agents" / "Local-Writer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Local-Writer" / "AGENTS.md").write_text(
        "# AGENTS.md - Local-Writer\n\n## Mission\nWrite concise project documents.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Codex-Reviewer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Codex-Reviewer" / "AGENTS.md").write_text(
        "# AGENTS.md - Codex-Reviewer\n\n## Mission\nReview outputs for risks.\n",
        encoding="utf-8",
    )
    (team_dir / "agents" / "Claude-Reviewer").mkdir(parents=True, exist_ok=True)
    (team_dir / "agents" / "Claude-Reviewer" / "AGENTS.md").write_text(
        "# AGENTS.md - Claude-Reviewer\n\n## Mission\nCross-check reviewer output.\n",
        encoding="utf-8",
    )
    (team_dir / "orchestrator.json").write_text(
        json.dumps(
            {
                "coordinator": {"role": "Orchestrator"},
                "agents": [
                    {"role": "Local-Writer"},
                    {"role": "Codex-Reviewer"},
                    {"role": "Claude-Reviewer"},
                ],
            }
        ),
        encoding="utf-8",
    )

    roles = orch_roles.choose_auto_dispatch_roles(
        "문서 정리해서 보고서 작성해줘",
        team_dir=team_dir,
    )

    assert "Codex-Writer" in roles
    assert "Local-Writer" not in roles
    assert "Codex-Reviewer" in roles
    assert "Claude-Reviewer" in roles


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
        assert offdesk_flow.provider_capacity_state_path(args, filename=mgmt_handlers.PROVIDER_CAPACITY_STATE_FILENAME) == mgmt_handlers._provider_capacity_state_path(args)

        state_a = tmp_path / "a.json"
        state_b = tmp_path / "b.json"
        state_c = tmp_path / "c.json"
        payload = {"enabled": True, "chat_id": "939062873"}
        mgmt_handlers._save_auto_state(state_a, payload)
        offdesk_flow.save_auto_state(state_b, payload)
        assert mgmt_handlers._load_auto_state(state_a) == offdesk_flow.load_auto_state(state_b)
        mgmt_handlers._save_provider_capacity_state(state_c, payload)
        assert mgmt_handlers._load_provider_capacity_state(state_c) == offdesk_flow.load_provider_capacity_state(state_c)
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
