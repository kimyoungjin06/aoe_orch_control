#!/usr/bin/env python3
"""Install systemd user timers for armed-gated overnight autonomy.

Renders three timer/service pairs that call offdesk_autonomy_run.py:

  forager-autonomy-tick     every 10 minutes  (offdesk tick heartbeat)
  forager-autonomy-distill  daily 02:00       (operator-owned nightly playbook)
  forager-autonomy-brief    daily 08:50       (morning brief, then disarm)

The timers fire regardless, but every service exits immediately unless the
operator has armed the window (Telegram proposal card or
`offdesk_autonomy_run.py --arm`), so installing them is inert by itself.
`forager offdesk pause` still halts dispatch independently of arming.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "scripts"))
from deploy_harness_scripts import deploy as deploy_harness_scripts

# Timers execute from the stable copy so repo checkouts cannot break them.
RUNNER = deploy_harness_scripts() / "offdesk_autonomy_run.py"

UNITS = [
    ("forager-autonomy-tick", "*:00/10", "tick", "Forager offdesk tick heartbeat (armed-gated)"),
    ("forager-autonomy-distill", "*-*-* 02:00:00", "distill", "Forager nightly wiki distillation (armed-gated)"),
    ("forager-autonomy-brief", "*-*-* 08:50:00", "brief", "Forager morning brief + disarm (armed-gated)"),
]


def systemd_arg(value) -> str:
    text = str(value)
    if re.fullmatch(r"[A-Za-z0-9_@%+=:,./-]+", text):
        return text
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def service_dir() -> pathlib.Path:
    return pathlib.Path.home() / ".config" / "systemd" / "user"


def render_service(name: str, task: str, description: str, python_bin: str, profile: str) -> str:
    exec_start = " ".join(
        systemd_arg(part)
        for part in [python_bin, str(RUNNER), "--task", task, "--profile", profile]
    )
    return "\n".join(
        [
            "[Unit]",
            f"Description={description}",
            "",
            "[Service]",
            "Type=oneshot",
            f"WorkingDirectory={systemd_arg(REPO_ROOT)}",
            f"ExecStart={exec_start}",
            "NoNewPrivileges=true",
            # tmux and the stable forager binary live in ~/.local/bin, which
            # the systemd user manager's default PATH does not include.
            "Environment=PATH=%h/.local/bin:/usr/local/bin:/usr/bin:/bin",
            "",
        ]
    )


def render_timer(name: str, on_calendar: str, description: str) -> str:
    return "\n".join(
        [
            "[Unit]",
            f"Description={description} timer",
            "",
            "[Timer]",
            f"OnCalendar={on_calendar}",
            "Persistent=false",
            "",
            "[Install]",
            "WantedBy=timers.target",
            "",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--python-bin", default=sys.executable or "python3")
    parser.add_argument("--profile", default="default")
    parser.add_argument("--install", action="store_true", help="Write units into ~/.config/systemd/user.")
    parser.add_argument("--enable", action="store_true", help="Enable + start the timers after installing.")
    parser.add_argument("--uninstall", action="store_true", help="Disable and remove the units.")
    parser.add_argument("--dry-run", action="store_true", help="Print the units without writing anything.")
    args = parser.parse_args()

    units = []
    for name, cal, task, desc in UNITS:
        units.append((f"{name}.service", render_service(name, task, desc, args.python_bin, args.profile)))
        units.append((f"{name}.timer", render_timer(name, cal, desc)))

    if args.uninstall:
        for name, _, _, _ in UNITS:
            subprocess.run(["systemctl", "--user", "disable", "--now", f"{name}.timer"], capture_output=True)
            for suffix in (".service", ".timer"):
                (service_dir() / f"{name}{suffix}").unlink(missing_ok=True)
        subprocess.run(["systemctl", "--user", "daemon-reload"], capture_output=True)
        print("autonomy timers removed")
        return 0

    if args.dry_run or not args.install:
        for filename, body in units:
            print(f"===== {filename} =====")
            print(body)
        if not args.install:
            print("(dry run; pass --install [--enable] to write units)")
        return 0

    service_dir().mkdir(parents=True, exist_ok=True)
    for filename, body in units:
        (service_dir() / filename).write_text(body)
        print(f"wrote {service_dir() / filename}")
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
    if args.enable:
        for name, _, _, _ in UNITS:
            subprocess.run(["systemctl", "--user", "enable", "--now", f"{name}.timer"], check=False, capture_output=True)
        print("timers enabled; they stay inert until autonomy is armed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
