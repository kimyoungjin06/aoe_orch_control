#!/usr/bin/env python3
"""Shared gateway test bootstrap and module imports."""

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
import aoe_tg_priority_actions as priority_actions
import aoe_tg_project_state as project_state
import aoe_tg_request_state as request_state
import aoe_tg_retry_handlers as retry_handlers
import aoe_tg_run_guards as run_guards
import aoe_tg_plan_pipeline as plan_pipeline
import aoe_tg_project_runtime as runtime_helpers
import aoe_tg_runtime_core as runtime_core
import aoe_tg_queue_engine as queue_engine
import aoe_tg_runtime_seed as runtime_seed
import aoe_tg_todo_policy as todo_policy
import aoe_tg_run_handlers as run_handlers
import aoe_tg_sync_catalog as sync_catalog
import aoe_tg_sync_discovery as sync_discovery
import aoe_tg_sync_extract as sync_extract
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


__all__ = [
    name
    for name in globals()
    if not name.startswith("__") and (not name.startswith("_") or name == "_empty_state")
]
