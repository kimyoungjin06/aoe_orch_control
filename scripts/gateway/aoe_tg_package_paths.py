#!/usr/bin/env python3
"""Package-relative path helpers.

Keep the installable package tree separate from generated `.aoe-team` runtime
state. These helpers always point at versioned assets that live inside the
repository/package, never inside a project runtime directory.
"""

from __future__ import annotations

from pathlib import Path


def package_root() -> Path:
    return Path(__file__).resolve().parents[2]


def templates_root() -> Path:
    return package_root() / "templates" / "aoe-team"


def team_stack_script() -> Path:
    return package_root() / "scripts" / "team" / "aoe-team-stack.sh"


def team_tmux_script() -> Path:
    return package_root() / "scripts" / "team" / "runtime" / "telegram_tmux.sh"


def worker_handler_script() -> Path:
    return package_root() / "scripts" / "team" / "runtime" / "worker_codex_handler.sh"


def bootstrap_runtime_script() -> Path:
    return package_root() / "scripts" / "team" / "bootstrap_runtime_templates.sh"
