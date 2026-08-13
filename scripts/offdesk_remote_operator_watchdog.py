#!/usr/bin/env python3
"""External watchdog for the Telegram Remote Operator listener.

The listener can only report failures while it is still alive. This watchdog is
intentionally separate: it reads the listener loop-status file, checks the user
systemd service when available, and sends one short emergency Telegram message
when remote operation is not currently reliable.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import re
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Any

from telegram_operator.config import default_telegram_env_file


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
DEFAULT_ENV_FILE = default_telegram_env_file()
DEFAULT_CACHE_DIR = pathlib.Path.home() / ".cache" / "forager"
WATCHDOG_SCHEMA = "remote_operator_telegram_watchdog.v1"
STATE_SCHEMA = "remote_operator_telegram_watchdog_state.v1"
MAX_ALERT_LINES = 5
MAX_ALERT_CHARS = 360


class WatchdogError(RuntimeError):
    pass


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", default=os.environ.get("FORAGER_PROFILE", "default"))
    parser.add_argument("--env-file", type=pathlib.Path, default=DEFAULT_ENV_FILE)
    parser.add_argument(
        "--loop-status-file",
        type=pathlib.Path,
        default=DEFAULT_CACHE_DIR / "remote_operator_telegram_loop.json",
    )
    parser.add_argument(
        "--state-file",
        type=pathlib.Path,
        default=DEFAULT_CACHE_DIR / "remote_operator_telegram_watchdog_state.json",
    )
    parser.add_argument("--service-name", default="forager-telegram-operator.service")
    parser.add_argument("--systemctl-bin", default=os.environ.get("SYSTEMCTL_BIN", "systemctl"))
    parser.add_argument(
        "--systemd-mode",
        choices=("auto", "off", "required"),
        default=os.environ.get("OFFDESK_REMOTE_OPERATOR_WATCHDOG_SYSTEMD_MODE", "auto"),
        help="auto warns when systemd is unavailable; required treats it as unhealthy.",
    )
    parser.add_argument("--health-max-age-sec", type=int, default=180)
    parser.add_argument("--alert-min-interval-sec", type=int, default=1800)
    parser.add_argument("--api-timeout-sec", type=int, default=20)
    parser.add_argument("--dry-run", action="store_true", help="Do not send Telegram messages or update alert state.")
    parser.add_argument("--out", type=pathlib.Path, help="Optional JSON report path.")
    return parser.parse_args()


def write_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_env_file(path: pathlib.Path, *, required: bool) -> dict[str, str]:
    if not path.exists():
        if required:
            raise WatchdogError(f"telegram env file not found: {path}")
        return {}
    values: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            values[key] = value
    return values


def csv_values(raw: str) -> list[str]:
    return [item.strip() for item in str(raw or "").split(",") if item.strip()]


def resolve_telegram_config(env_file: pathlib.Path, *, required: bool) -> dict[str, Any]:
    env = parse_env_file(env_file, required=required)
    token = str(env.get("TELEGRAM_BOT_TOKEN") or "").strip()
    owner_chat_id = str(env.get("TELEGRAM_OWNER_CHAT_ID") or "").strip()
    allowed_chat_ids = set(csv_values(env.get("TELEGRAM_ALLOW_CHAT_IDS", "")))
    allowed_chat_ids.update(csv_values(env.get("TELEGRAM_ALLOWED_CHAT_IDS", "")))
    if owner_chat_id:
        allowed_chat_ids.add(owner_chat_id)
    target_chat_id = owner_chat_id or next(iter(sorted(allowed_chat_ids)), "")
    if required and not token:
        raise WatchdogError("TELEGRAM_BOT_TOKEN is missing")
    if required and not target_chat_id:
        raise WatchdogError("TELEGRAM_OWNER_CHAT_ID or TELEGRAM_ALLOW_CHAT_IDS is required")
    return {
        "token_configured": bool(token),
        "target_chat_configured": bool(target_chat_id),
        "token": token,
        "target_chat_id": target_chat_id,
    }


def sanitize_text(value: Any, *, max_chars: int = 240) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = re.sub(r"\d{5,}:[A-Za-z0-9_-]{10,}", "[redacted-telegram-token]", text)
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)].rstrip() + "..."


def parse_timestamp(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def listener_probe(args: argparse.Namespace) -> dict[str, Any]:
    path = args.loop_status_file
    issues: list[str] = []
    loop_status: dict[str, Any] = {}
    if not path.exists():
        issues.append("loop_status_missing")
    else:
        try:
            loaded = load_json(path)
            loop_status = loaded if isinstance(loaded, dict) else {}
            if not loop_status:
                issues.append("loop_status_unreadable")
        except (OSError, json.JSONDecodeError):
            issues.append("loop_status_unreadable")
    last_result = loop_status.get("last_result") if isinstance(loop_status.get("last_result"), dict) else {}
    last_poll_at = parse_timestamp(last_result.get("generated_at") or loop_status.get("generated_at"))
    last_poll_age_sec = None
    if last_poll_at:
        last_poll_age_sec = max(0, int((dt.datetime.now(dt.timezone.utc) - last_poll_at).total_seconds()))
        if last_poll_age_sec > max(1, int(args.health_max_age_sec)):
            issues.append("last_poll_stale")
    elif loop_status:
        issues.append("last_poll_missing")
    listener_status = str(loop_status.get("status") or "")
    if loop_status and listener_status not in {"polling", "max_polls_reached"}:
        issues.append("listener_not_polling")
    last_result_status = str(last_result.get("status") or "")
    if last_result_status == "poll_error":
        issues.append("last_poll_transport_error")
    if last_result_status == "send_failed":
        issues.append("last_send_transport_error")
    if last_result_status == "loop_error":
        issues.append("last_loop_internal_error")
    return {
        "status_file": str(path),
        "status_file_exists": path.exists(),
        "listener_status": loop_status.get("status"),
        "poll_count": loop_status.get("poll_count"),
        "updates_seen": loop_status.get("updates_seen"),
        "handled_result_count": loop_status.get("handled_result_count"),
        "last_poll_age_sec": last_poll_age_sec,
        "last_result_status": last_result.get("status"),
        "issues": issues,
    }


def systemd_probe(args: argparse.Namespace) -> dict[str, Any]:
    if args.systemd_mode == "off":
        return {
            "mode": "off",
            "service_name": args.service_name,
            "active_state": "not_checked",
            "issues": [],
            "warnings": [],
        }
    command = [args.systemctl_bin, "--user", "is-active", args.service_name]
    try:
        process = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=8,
        )
    except (OSError, subprocess.SubprocessError) as error:
        issue = "systemd_unavailable"
        return {
            "mode": args.systemd_mode,
            "service_name": args.service_name,
            "active_state": "unknown",
            "command": " ".join(command),
            "issues": [issue] if args.systemd_mode == "required" else [],
            "warnings": [issue] if args.systemd_mode == "auto" else [],
            "error": sanitize_text(f"{type(error).__name__}: {error}"),
        }
    active_state = (process.stdout or process.stderr or "unknown").strip().splitlines()[0]
    issues: list[str] = []
    if active_state == "failed":
        issues.append("systemd_service_failed")
    elif active_state not in {"active", "activating", "reloading"}:
        issues.append("systemd_service_inactive")
    return {
        "mode": args.systemd_mode,
        "service_name": args.service_name,
        "active_state": active_state,
        "exit_code": process.returncode,
        "issues": issues,
        "warnings": [],
        "stderr": sanitize_text(process.stderr, max_chars=240) if process.stderr else None,
    }


def config_issues(config: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if not config.get("token_configured"):
        issues.append("telegram_bot_token_missing")
    if not config.get("target_chat_configured"):
        issues.append("telegram_target_chat_missing")
    return issues


def alert_key(issues: list[str]) -> str:
    return "|".join(sorted(set(issues)))


def load_watchdog_state(path: pathlib.Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema": STATE_SCHEMA}
    try:
        loaded = load_json(path)
    except (OSError, json.JSONDecodeError):
        return {"schema": STATE_SCHEMA, "state_error": "watchdog_state_unreadable"}
    return loaded if isinstance(loaded, dict) else {"schema": STATE_SCHEMA}


def is_rate_limited(state: dict[str, Any], key: str, min_interval_sec: int) -> tuple[bool, int | None]:
    if min_interval_sec <= 0:
        return False, None
    if str(state.get("last_alert_key") or "") != key:
        return False, None
    last_alert_at = parse_timestamp(state.get("last_alert_at"))
    if not last_alert_at:
        return False, None
    age_sec = max(0, int((dt.datetime.now(dt.timezone.utc) - last_alert_at).total_seconds()))
    return age_sec < min_interval_sec, age_sec


def issue_label(issues: list[str]) -> str:
    priority = [
        ("systemd_service_failed", "service failed"),
        ("systemd_service_inactive", "service inactive"),
        ("loop_status_missing", "listener heartbeat missing"),
        ("last_poll_stale", "listener stale"),
        ("last_poll_transport_error", "telegram transport error"),
        ("last_send_transport_error", "telegram send failed"),
        ("last_loop_internal_error", "listener internal error"),
        ("telegram_bot_token_missing", "telegram token missing"),
        ("telegram_target_chat_missing", "telegram chat missing"),
        ("listener_not_polling", "listener not polling"),
    ]
    issue_set = set(issues)
    for key, label in priority:
        if key in issue_set:
            return label
    return issues[0] if issues else "unknown"


def recovery_commands(args: argparse.Namespace) -> list[str]:
    health_command = (
        "scripts/offdesk_remote_operator_telegram.py --health "
        f"--env-file {args.env_file} --loop-status-file {args.loop_status_file}"
    )
    return [
        f"systemctl --user status {args.service_name}",
        f"systemctl --user restart {args.service_name}",
        health_command,
    ]


def alert_message(issues: list[str], args: argparse.Namespace) -> str:
    label = issue_label(issues)
    next_action = f"systemctl --user restart {args.service_name}"
    lines = [
        "Remote Operator 고장",
        f"상태: {label}",
        "야간주행: 불가",
        f"다음: {next_action}",
    ]
    message = "\n".join(lines)
    if len(message) > MAX_ALERT_CHARS:
        lines[-1] = "다음: 로컬에서 listener health 확인"
    return "\n".join(lines[:MAX_ALERT_LINES])[:MAX_ALERT_CHARS]


def telegram_api(token: str, method: str, payload: dict[str, Any], timeout_sec: int) -> dict[str, Any]:
    request = urllib.request.Request(
        f"https://api.telegram.org/bot{token}/{method}",
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace") if hasattr(error, "read") else str(error)
        raise WatchdogError(f"Telegram API HTTP error ({method}): {sanitize_text(detail)}") from error
    except urllib.error.URLError as error:
        raise WatchdogError(f"Telegram API URL error ({method}): {sanitize_text(error)}") from error
    except json.JSONDecodeError as error:
        raise WatchdogError(f"Telegram API invalid JSON ({method})") from error
    if not data.get("ok"):
        raise WatchdogError(f"Telegram API error ({method}): {sanitize_text(data)}")
    return data


def send_alert(config: dict[str, Any], message: str, args: argparse.Namespace) -> int | None:
    if not config.get("token") or not config.get("target_chat_id"):
        raise WatchdogError("telegram alert target is not configured")
    data = telegram_api(
        str(config["token"]),
        "sendMessage",
        {
            "chat_id": str(config["target_chat_id"]),
            "text": message,
            "disable_web_page_preview": True,
        },
        timeout_sec=max(1, int(args.api_timeout_sec)),
    )
    result = data.get("result")
    if isinstance(result, dict) and isinstance(result.get("message_id"), int):
        return int(result["message_id"])
    return None


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    config = resolve_telegram_config(args.env_file, required=False)
    listener = listener_probe(args)
    systemd = systemd_probe(args)
    issues = [
        *config_issues(config),
        *listener["issues"],
        *systemd["issues"],
    ]
    warnings = list(systemd.get("warnings") or [])
    if issues:
        health_status = "unhealthy"
    elif warnings:
        health_status = "degraded"
    else:
        health_status = "healthy"
    commands = recovery_commands(args)
    key = alert_key(issues)
    state = load_watchdog_state(args.state_file)
    suppressed, last_alert_age_sec = is_rate_limited(state, key, int(args.alert_min_interval_sec))
    alert_needed = health_status == "unhealthy"
    message = alert_message(issues, args) if alert_needed else ""
    alert: dict[str, Any] = {
        "needed": alert_needed,
        "sent": False,
        "suppressed": bool(alert_needed and suppressed),
        "reason": None,
        "alert_key": key or None,
        "last_alert_age_sec": last_alert_age_sec,
        "message_preview": message or None,
        "line_count": len(message.splitlines()) if message else 0,
        "char_count": len(message),
        "max_lines": MAX_ALERT_LINES,
        "max_chars": MAX_ALERT_CHARS,
    }
    if alert_needed and suppressed:
        alert["reason"] = "rate_limited"
    elif alert_needed and args.dry_run:
        alert["reason"] = "dry_run"
    elif alert_needed:
        try:
            alert["message_id"] = send_alert(config, message, args)
            alert["sent"] = True
            alert["reason"] = "sent"
            write_json(
                args.state_file,
                {
                    "schema": STATE_SCHEMA,
                    "last_alert_at": utc_now(),
                    "last_alert_key": key,
                    "last_health_status": health_status,
                    "last_issues": issues,
                },
            )
        except WatchdogError as error:
            alert["reason"] = "send_failed"
            alert["error"] = sanitize_text(str(error), max_chars=240)
    else:
        alert["reason"] = "healthy"
    return {
        "schema": WATCHDOG_SCHEMA,
        "generated_at": utc_now(),
        "profile": args.profile,
        "health_status": health_status,
        "issues": issues,
        "warnings": warnings,
        "listener": listener,
        "systemd": systemd,
        "recovery_commands": commands,
        "alert": alert,
        "read_only": True,
        "mutation_authorized": False,
        "approval_authorized": False,
    }


def emit_result(args: argparse.Namespace, report: dict[str, Any]) -> None:
    if args.out:
        write_json(args.out, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))


def main() -> int:
    args = parse_args()
    try:
        report = build_report(args)
        emit_result(args, report)
        return 0 if report.get("health_status") == "healthy" else 1
    except WatchdogError as error:
        report = {
            "schema": WATCHDOG_SCHEMA,
            "generated_at": utc_now(),
            "health_status": "unhealthy",
            "issues": ["watchdog_error"],
            "error": sanitize_text(str(error)),
            "read_only": True,
            "mutation_authorized": False,
            "approval_authorized": False,
        }
        if args.out:
            write_json(args.out, report)
        print(json.dumps(report, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
