#!/usr/bin/env python3
"""AutoGen Core TF backend placeholder.

This module intentionally does not implement live execution yet.
It defines the boundary and availability checks for a future backend adapter.
"""

from __future__ import annotations

import importlib.util
from importlib import metadata
from typing import Any, Dict

from aoe_tg_tf_backend import TFBackendAdapter, TFBackendAvailability, TFBackendDeps, TFBackendRequest


def autogen_core_installed() -> bool:
    return importlib.util.find_spec("autogen_core") is not None


def autogen_core_version() -> str:
    for name in ("autogen-core", "autogen_core"):
        try:
            return str(metadata.version(name))
        except Exception:
            continue
    return ""


class AutoGenCoreTFBackend(TFBackendAdapter):
    backend_name = "autogen_core"

    def availability(self) -> TFBackendAvailability:
        if autogen_core_installed():
            version = autogen_core_version()
            detail = f"installed{f' ({version})' if version else ''}"
            return TFBackendAvailability(True, detail)
        return TFBackendAvailability(
            False,
            "autogen_core is not installed; Phase 0 currently supports only dry-run spike design",
        )

    def run(self, request: TFBackendRequest, deps: TFBackendDeps) -> Dict[str, Any]:
        raise NotImplementedError(
            "AutoGen Core backend is not wired into live TF execution yet. "
            "Use scripts/experiments/autogen_core_tf_spike.py for Phase 0 dry-run planning."
        )


def autogen_core_backend() -> AutoGenCoreTFBackend:
    return AutoGenCoreTFBackend()
