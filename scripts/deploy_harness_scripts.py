#!/usr/bin/env python3
"""Deploy harness scripts to a stable path outside the git working tree.

Services must not execute scripts from the repo checkout: a branch switch or
merge removes/changes files mid-run (the autonomy tick died with 'No such
file' during the PR-189 merge window). Installers call deploy() and point
ExecStart at the stable copy; refresh happens only on deliberate reinstall.
"""

from __future__ import annotations

import pathlib
import shutil

REPO_SCRIPTS = pathlib.Path(__file__).resolve().parent
STABLE_SCRIPTS = pathlib.Path.home() / ".local" / "share" / "forager" / "scripts"


def deploy() -> pathlib.Path:
    STABLE_SCRIPTS.mkdir(parents=True, exist_ok=True)
    shutil.copytree(
        REPO_SCRIPTS,
        STABLE_SCRIPTS,
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )
    return STABLE_SCRIPTS


if __name__ == "__main__":
    print(deploy())
