#!/usr/bin/env python3
"""TF execution backend interface.

This module defines a narrow adapter seam for TF execution engines.

The current repository owns the higher-level orchestration model:

- Telegram control plane
- project registry / focus / lock
- runtime queue and todo proposals
- sync / salvage / syncback
- offdesk / auto scheduling

Backends are responsible only for executing one TF request and returning a
normalized result payload.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Protocol, Tuple


DEFAULT_TF_BACKEND = "local"
AUTOGEN_CORE_TF_BACKEND = "autogen_core"
SUPPORTED_TF_BACKENDS = (DEFAULT_TF_BACKEND, AUTOGEN_CORE_TF_BACKEND)


def normalize_tf_backend_name(raw: Optional[str], *, default: str = DEFAULT_TF_BACKEND) -> str:
    token = str(raw or "").strip().lower()
    if token in {"", "default"}:
        token = default
    aliases = {
        "local": DEFAULT_TF_BACKEND,
        "aoe": DEFAULT_TF_BACKEND,
        "orch": DEFAULT_TF_BACKEND,
        "autogen": AUTOGEN_CORE_TF_BACKEND,
        "autogen-core": AUTOGEN_CORE_TF_BACKEND,
        "autogen_core": AUTOGEN_CORE_TF_BACKEND,
    }
    return aliases.get(token, default)


@dataclass(frozen=True)
class TFBackendRequest:
    """Normalized request passed into a TF execution backend."""

    args: Any
    prompt: str
    chat_id: str
    roles_override: str = ""
    priority_override: Optional[str] = None
    timeout_override: Optional[int] = None
    no_wait_override: Optional[bool] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


def normalize_backend_metadata(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    data = raw if isinstance(raw, dict) else {}
    out: Dict[str, Any] = {}
    for key, value in data.items():
        if not isinstance(key, str):
            continue
        out[str(key).strip()] = value
    return out


@dataclass(frozen=True)
class TFBackendDeps:
    """Execution dependencies injected into the backend adapter."""

    default_tf_exec_mode: str
    default_tf_work_root_name: str
    default_tf_exec_map_file: str
    default_tf_worker_startup_grace_sec: int
    now_iso: Callable[[], str]
    run_command: Callable[..., Any]


@dataclass(frozen=True)
class TFBackendAvailability:
    available: bool
    reason: str = ""


class TFBackendAdapter(Protocol):
    backend_name: str

    def availability(self) -> TFBackendAvailability:
        ...

    def run(self, request: TFBackendRequest, deps: TFBackendDeps) -> Dict[str, Any]:
        ...


def build_tf_backend_request(
    *,
    args: Any,
    prompt: str,
    chat_id: str,
    roles_override: Optional[str] = None,
    priority_override: Optional[str] = None,
    timeout_override: Optional[int] = None,
    no_wait_override: Optional[bool] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> TFBackendRequest:
    return TFBackendRequest(
        args=args,
        prompt=str(prompt or ""),
        chat_id=str(chat_id or ""),
        roles_override=str(roles_override or ""),
        priority_override=priority_override,
        timeout_override=timeout_override,
        no_wait_override=no_wait_override,
        metadata=normalize_backend_metadata(metadata),
    )


def build_tf_backend_deps(
    *,
    default_tf_exec_mode: str,
    default_tf_work_root_name: str,
    default_tf_exec_map_file: str,
    default_tf_worker_startup_grace_sec: int,
    now_iso: Callable[[], str],
    run_command: Callable[..., Any],
) -> TFBackendDeps:
    return TFBackendDeps(
        default_tf_exec_mode=str(default_tf_exec_mode or DEFAULT_TF_BACKEND),
        default_tf_work_root_name=str(default_tf_work_root_name or ""),
        default_tf_exec_map_file=str(default_tf_exec_map_file or ""),
        default_tf_worker_startup_grace_sec=max(0, int(default_tf_worker_startup_grace_sec)),
        now_iso=now_iso,
        run_command=run_command,
    )


def backend_runtime_label(name: str) -> str:
    token = normalize_tf_backend_name(name)
    if token == AUTOGEN_CORE_TF_BACKEND:
        return "autogen_core"
    return DEFAULT_TF_BACKEND


def availability_tuple(availability: TFBackendAvailability) -> Tuple[bool, str]:
    return bool(availability.available), str(availability.reason or "")
