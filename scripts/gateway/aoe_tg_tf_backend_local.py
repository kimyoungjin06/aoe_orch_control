#!/usr/bin/env python3
"""Local TF backend wrapper.

This adapter preserves the current behavior by delegating to `run_aoe_orch`.
It exists so future backends can share the same call boundary.
"""

from __future__ import annotations

from typing import Any, Dict

from aoe_tg_tf_backend import TFBackendAdapter, TFBackendAvailability, TFBackendDeps, TFBackendRequest
from aoe_tg_tf_exec import run_aoe_orch


class LocalTFBackend(TFBackendAdapter):
    backend_name = "local"

    def availability(self) -> TFBackendAvailability:
        return TFBackendAvailability(True, "")

    def run(self, request: TFBackendRequest, deps: TFBackendDeps) -> Dict[str, Any]:
        return run_aoe_orch(
            request.args,
            request.prompt,
            chat_id=request.chat_id,
            default_tf_exec_mode=deps.default_tf_exec_mode,
            default_tf_work_root_name=deps.default_tf_work_root_name,
            default_tf_exec_map_file=deps.default_tf_exec_map_file,
            default_tf_worker_startup_grace_sec=deps.default_tf_worker_startup_grace_sec,
            now_iso=deps.now_iso,
            run_command=deps.run_command,
            roles_override=request.roles_override or None,
            priority_override=request.priority_override,
            timeout_override=request.timeout_override,
            no_wait_override=request.no_wait_override,
        )


def local_backend() -> LocalTFBackend:
    return LocalTFBackend()
