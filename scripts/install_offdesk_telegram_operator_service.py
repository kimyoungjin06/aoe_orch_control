#!/usr/bin/env python3
"""Install the Forager Telegram remote-operator as a systemd user service."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
from typing import Any


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = pathlib.Path(
    os.environ.get(
        "OFFDESK_TELEGRAM_ENV",
        "/home/kimyoungjin06/Desktop/Workspace/aoe_orch_control/.aoe-team/telegram.env",
    )
)
DEFAULT_CACHE_DIR = pathlib.Path.home() / ".cache" / "forager"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--service-name", default="forager-telegram-operator.service")
    parser.add_argument("--repo-root", type=pathlib.Path, default=REPO_ROOT)
    parser.add_argument("--python-bin", default=sys.executable or "python3")
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
    from forager_bin import resolve_forager_bin

    parser.add_argument("--forager-bin", type=pathlib.Path, default=pathlib.Path(resolve_forager_bin()))
    parser.add_argument("--env-file", type=pathlib.Path, default=DEFAULT_ENV_FILE)
    parser.add_argument("--profile", default=os.environ.get("FORAGER_PROFILE", "default"))
    parser.add_argument("--state-file", type=pathlib.Path, default=DEFAULT_CACHE_DIR / "remote_operator_telegram_state.json")
    parser.add_argument("--feedback-file", type=pathlib.Path, default=DEFAULT_CACHE_DIR / "remote_operator_telegram_feedback.jsonl")
    parser.add_argument("--feedback-ingest-dir", type=pathlib.Path, default=DEFAULT_CACHE_DIR / "remote_operator_telegram_feedback_ingest")
    parser.add_argument("--loop-status-file", type=pathlib.Path, default=DEFAULT_CACHE_DIR / "remote_operator_telegram_loop.json")
    parser.add_argument("--include-watchdog", action="store_true", help="Also install a timer-backed external watchdog.")
    parser.add_argument("--watchdog-service-name", default="forager-telegram-operator-watchdog.service")
    parser.add_argument("--watchdog-timer-name", default="forager-telegram-operator-watchdog.timer")
    parser.add_argument("--watchdog-state-file", type=pathlib.Path, default=DEFAULT_CACHE_DIR / "remote_operator_telegram_watchdog_state.json")
    parser.add_argument("--watchdog-interval-sec", type=int, default=120)
    parser.add_argument("--watchdog-health-max-age-sec", type=int, default=180)
    parser.add_argument("--watchdog-alert-min-interval-sec", type=int, default=1800)
    parser.add_argument("--poll-timeout-sec", type=int, default=30)
    parser.add_argument("--api-timeout-sec", type=int, default=45)
    parser.add_argument("--poll-error-backoff-sec", type=int, default=5)
    parser.add_argument(
        "--attention-notify",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Proactively push newly waiting decisions/recovery items to the owner chat "
        "(recommended for urgent handling). Use --no-attention-notify to disable.",
    )
    parser.add_argument("--attention-reminder-sec", type=int, default=0)
    parser.add_argument(
        "--autonomy-propose",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Propose starting the armed overnight autonomy window via Telegram when the "
        "workstation goes idle. Off by default; the operator's tap is the only approval.",
    )
    parser.add_argument(
        "--session-notify",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Push a card when a supervised agent session sits waiting for input "
        "(recommended with forager go). Use --no-session-notify to disable.",
    )
    parser.add_argument(
        "--dispatch-allowlist-file",
        type=pathlib.Path,
        default=None,
        help="Path to a JSON file of curated /run command templates. When set, the operator can "
        "dispatch named, pre-vetted commands over Telegram without the free-form "
        "--enable-runtime-dispatch. Off by default.",
    )
    parser.add_argument("--install", action="store_true", help="Write the unit into ~/.config/systemd/user.")
    parser.add_argument("--enable", action="store_true", help="Enable the service after installing it.")
    parser.add_argument("--start", action="store_true", help="Start the service after installing it.")
    parser.add_argument("--restart", action="store_true", help="Restart the service after installing it.")
    parser.add_argument("--dry-run", action="store_true", help="Print the unit and do not write or call systemctl.")
    return parser.parse_args()


def systemd_arg(value: Any) -> str:
    text = str(value)
    if re.fullmatch(r"[A-Za-z0-9_@%+=:,./-]+", text):
        return text
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def service_dir() -> pathlib.Path:
    return pathlib.Path.home() / ".config" / "systemd" / "user"


def service_path(service_name: str) -> pathlib.Path:
    return service_dir() / service_name


def render_unit(args: argparse.Namespace) -> str:
    # Deploy scripts to a stable path so branch switches in the repo cannot
    # break the running service, then execute from that copy.
    sys.path.insert(0, str(args.repo_root / "scripts"))
    from deploy_harness_scripts import deploy

    script = deploy() / "offdesk_remote_operator_telegram.py"
    command = [
        args.python_bin,
        script,
        "--profile",
        args.profile,
        "--forager-bin",
        args.forager_bin,
        "--env-file",
        args.env_file,
        "--state-file",
        args.state_file,
        "--feedback-file",
        args.feedback_file,
        "--feedback-ingest-dir",
        args.feedback_ingest_dir,
        "--loop-status-file",
        args.loop_status_file,
        "--poll-timeout-sec",
        args.poll_timeout_sec,
        "--api-timeout-sec",
        args.api_timeout_sec,
        "--poll-error-backoff-sec",
        args.poll_error_backoff_sec,
    ]
    if args.attention_notify:
        command.append("--attention-notify")
        if int(args.attention_reminder_sec) > 0:
            command.extend(["--attention-reminder-sec", args.attention_reminder_sec])
    if args.autonomy_propose:
        command.append("--autonomy-propose")
    if args.session_notify:
        command.append("--session-notify")
    if args.dispatch_allowlist_file is not None:
        command.extend(["--dispatch-allowlist-file", args.dispatch_allowlist_file])
    exec_start = " ".join(systemd_arg(item) for item in command)
    return "\n".join(
        [
            "[Unit]",
            "Description=Forager Telegram remote operator",
            "After=network-online.target",
            "Wants=network-online.target",
            "StartLimitIntervalSec=0",
            "",
            "[Service]",
            "Type=simple",
            f"WorkingDirectory={systemd_arg(args.repo_root)}",
            f"ExecStart={exec_start}",
            "Restart=always",
            "RestartSec=5",
            "NoNewPrivileges=true",
            # The session-notify scan and session-input dispatch shell out to
            # tmux, which on user-managed hosts often lives in ~/.local/bin --
            # a path the systemd user manager does not include by default.
            "Environment=PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin",
            # No PrivateTmp: the tmux server socket lives in /tmp/tmux-<uid>/
            # and must stay reachable from this service.
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )


def render_watchdog_service_unit(args: argparse.Namespace) -> str:
    script = args.repo_root / "scripts" / "offdesk_remote_operator_watchdog.py"
    command = [
        args.python_bin,
        script,
        "--profile",
        args.profile,
        "--env-file",
        args.env_file,
        "--loop-status-file",
        args.loop_status_file,
        "--state-file",
        args.watchdog_state_file,
        "--service-name",
        args.service_name,
        "--systemd-mode",
        "required",
        "--health-max-age-sec",
        args.watchdog_health_max_age_sec,
        "--alert-min-interval-sec",
        args.watchdog_alert_min_interval_sec,
        "--api-timeout-sec",
        args.api_timeout_sec,
    ]
    exec_start = " ".join(systemd_arg(item) for item in command)
    return "\n".join(
        [
            "[Unit]",
            "Description=Forager Telegram remote operator watchdog",
            "After=network-online.target",
            "Wants=network-online.target",
            "",
            "[Service]",
            "Type=oneshot",
            f"WorkingDirectory={systemd_arg(args.repo_root)}",
            f"ExecStart={exec_start}",
            "NoNewPrivileges=true",
            "PrivateTmp=true",
            "Environment=PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin",
            "",
        ]
    )


def render_watchdog_timer_unit(args: argparse.Namespace) -> str:
    interval = max(30, int(args.watchdog_interval_sec))
    return "\n".join(
        [
            "[Unit]",
            "Description=Run Forager Telegram remote operator watchdog",
            "",
            "[Timer]",
            "OnBootSec=2min",
            f"OnUnitActiveSec={interval}s",
            "AccuracySec=30s",
            f"Unit={args.watchdog_service_name}",
            "",
            "[Install]",
            "WantedBy=timers.target",
            "",
        ]
    )


def systemctl_user(*args: str) -> None:
    subprocess.run(["systemctl", "--user", *args], check=True)


def main() -> int:
    args = parse_args()
    unit = render_unit(args)
    path = service_path(args.service_name)
    watchdog_service_unit = render_watchdog_service_unit(args) if args.include_watchdog else None
    watchdog_timer_unit = render_watchdog_timer_unit(args) if args.include_watchdog else None
    watchdog_service_path = service_path(args.watchdog_service_name)
    watchdog_timer_path = service_path(args.watchdog_timer_name)
    report: dict[str, Any] = {
        "schema": "forager_telegram_operator_systemd_install.v1",
        "service_name": args.service_name,
        "service_path": str(path),
        "repo_root": str(args.repo_root),
        "loop_status_file": str(args.loop_status_file),
        "watchdog_included": bool(args.include_watchdog),
        "watchdog_service_name": args.watchdog_service_name if args.include_watchdog else None,
        "watchdog_timer_name": args.watchdog_timer_name if args.include_watchdog else None,
        "watchdog_service_path": str(watchdog_service_path) if args.include_watchdog else None,
        "watchdog_timer_path": str(watchdog_timer_path) if args.include_watchdog else None,
        "watchdog_state_file": str(args.watchdog_state_file) if args.include_watchdog else None,
        "installed": False,
        "enabled": False,
        "started": False,
        "restarted": False,
        "watchdog_installed": False,
        "watchdog_enabled": False,
        "watchdog_started": False,
    }
    if args.dry_run or not args.install:
        report["unit_preview"] = unit
        if args.include_watchdog:
            report["watchdog_service_unit_preview"] = watchdog_service_unit
            report["watchdog_timer_unit_preview"] = watchdog_timer_unit
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(unit, encoding="utf-8")
    report["installed"] = True
    if args.include_watchdog:
        watchdog_service_path.write_text(str(watchdog_service_unit), encoding="utf-8")
        watchdog_timer_path.write_text(str(watchdog_timer_unit), encoding="utf-8")
        report["watchdog_installed"] = True
    systemctl_user("daemon-reload")
    if args.enable:
        systemctl_user("enable", args.service_name)
        report["enabled"] = True
        if args.include_watchdog:
            systemctl_user("enable", args.watchdog_timer_name)
            report["watchdog_enabled"] = True
    if args.restart:
        systemctl_user("restart", args.service_name)
        report["restarted"] = True
        if args.include_watchdog:
            systemctl_user("restart", args.watchdog_timer_name)
            report["watchdog_started"] = True
    elif args.start:
        systemctl_user("start", args.service_name)
        report["started"] = True
        if args.include_watchdog:
            systemctl_user("start", args.watchdog_timer_name)
            report["watchdog_started"] = True
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
