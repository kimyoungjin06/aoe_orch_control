#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict

TERMINAL_STATUSES = {"done", "failed"}


def safe_name(value: str) -> str:
    token = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in str(value or "").strip())
    return token or "unknown"


def request_id_of(message: Dict[str, Any]) -> str:
    for key in ("request_id", "thread_id", "id"):
        token = str(message.get(key, "")).strip()
        if token:
            return token
    return ""


def matches_request_assignment(path: Path, role: str, request_id: str) -> bool:
    try:
        message = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(message, dict):
        return False
    parent_id = message.get("parent_id")
    if parent_id not in {None, ""}:
        return False
    if request_id_of(message) != request_id:
        return False
    if safe_name(message.get("to", "")) != safe_name(role):
        return False
    status = str(message.get("status", "sent")).strip().lower() or "sent"
    return status not in TERMINAL_STATUSES


def wait_for_assignment(team_dir: Path, role: str, request_id: str, timeout_sec: int) -> bool:
    deadline = time.monotonic() + max(5, int(timeout_sec))
    msg_dir = team_dir / "messages"
    while time.monotonic() <= deadline:
        if msg_dir.exists():
            for path in sorted(msg_dir.glob("*.json")):
                if matches_request_assignment(path, role, request_id):
                    return True
        time.sleep(0.5)
    return False


def main() -> int:
    p = argparse.ArgumentParser(description="Wait for a request-specific assignment, then run one aoe worker pass.")
    p.add_argument("--project-root", required=True)
    p.add_argument("--team-dir", required=True)
    p.add_argument("--role", required=True)
    p.add_argument("--request-id", required=True)
    p.add_argument("--handler-cmd", required=True)
    p.add_argument("--state-file", required=True)
    p.add_argument("--startup-timeout-sec", type=int, default=120)
    p.add_argument("--exec-timeout-sec", type=int, default=900)
    p.add_argument("--aoe-orch-bin", required=True)
    args = p.parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    team_dir = Path(args.team_dir).expanduser().resolve()
    state_file = Path(args.state_file).expanduser().resolve()
    state_file.parent.mkdir(parents=True, exist_ok=True)

    request_id = str(args.request_id).strip()
    role = str(args.role).strip()
    if not wait_for_assignment(team_dir, role, request_id, max(5, int(args.startup_timeout_sec))):
        print(f"[TF-WORKER] timeout waiting role={role} request_id={request_id}", flush=True)
        return 0

    cmd = [
        str(args.aoe_orch_bin),
        "worker",
        "--project-root",
        str(project_root),
        "--team-dir",
        str(team_dir),
        "--for",
        role,
        "--request-id",
        request_id,
        "--handler-cmd",
        str(args.handler_cmd),
        "--state-file",
        str(state_file),
        "--reset-state",
        "--once",
        "--quiet",
        "--interval-sec",
        "0.5",
        "--max-messages",
        "8",
        "--exec-timeout-sec",
        str(max(60, int(args.exec_timeout_sec))),
    ]
    proc = subprocess.run(cmd, text=True)
    return int(proc.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
