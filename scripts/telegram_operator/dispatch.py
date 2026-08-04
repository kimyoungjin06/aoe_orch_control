"""Guarded remote execution surface for the Telegram operator.

This module lets the operator drive the existing receipt-gated ondesk
executors from Telegram without widening their authority. Every mutation
follows the same shape the local web bridge uses:

1. Export a fresh operator-safe ``workstation_surface.v1``.
2. Rebuild the executable action envelope from that surface, never from
   operator-supplied JSON.
3. Require an explicit one-tap confirmation bound to the decision id,
   action kind, and observed hash, with a nonce and TTL.
4. Run the canonical ondesk executor chain, which independently
   re-validates the observed hash, nonce, and expiry before recording a
   receipt.

The module never executes arbitrary shell text and never records accepted
truth. It only orchestrates commands the CLI already exposes.
"""

from __future__ import annotations

import hashlib
import json
import pathlib
import re
import subprocess
import uuid
from typing import Any

from .common import RemoteOperatorTelegramError, utc_now
from .persistence import parse_utc_timestamp
from .rendering import sanitize_text

CONFIRMATION_SCHEMA = "telegram_dispatch_confirmation.v1"
DISPATCH_RESULT_SCHEMA = "telegram_dispatch_result.v1"
DEFAULT_CONFIRMATION_TTL_SEC = 300
WORKSTATION_SURFACE_SCHEMA = "workstation_surface.v1"
ACTION_ENVELOPE_SCHEMA = "action_envelope.v1"


def run_forager_json(
    forager_bin: str,
    profile: str,
    argv_tail: list[str],
    *,
    label: str,
) -> dict[str, Any]:
    """Run a forager subcommand that emits a single JSON object on stdout."""

    argv = [forager_bin]
    if profile:
        argv.extend(["--profile", profile])
    argv.extend(argv_tail)
    try:
        process = subprocess.run(
            argv,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except (OSError, ValueError) as error:
        # ValueError covers operator text with an embedded NUL byte, which
        # subprocess rejects; convert it to a handled adapter error so it
        # cannot escape into the poll loop.
        raise RemoteOperatorTelegramError(f"{label} could not start: {error}") from error
    if process.returncode != 0:
        detail = sanitize_text(process.stderr.strip() or process.stdout.strip(), max_chars=240)
        raise RemoteOperatorTelegramError(f"{label} failed: {detail}")
    try:
        parsed = json.loads(process.stdout)
    except json.JSONDecodeError as error:
        raise RemoteOperatorTelegramError(f"{label} did not return JSON") from error
    if not isinstance(parsed, dict):
        raise RemoteOperatorTelegramError(f"{label} did not return a JSON object")
    return parsed


def export_workstation_surface(forager_bin: str, profile: str) -> dict[str, Any]:
    surface = run_forager_json(
        forager_bin,
        profile,
        ["ondesk", "workstation-surface", "--json"],
        label="workstation surface export",
    )
    if surface.get("schema") != WORKSTATION_SURFACE_SCHEMA:
        raise RemoteOperatorTelegramError(
            f"unexpected workstation surface schema: {surface.get('schema')}"
        )
    redaction = surface.get("redaction")
    if not isinstance(redaction, dict) or redaction.get("operator_safe") is not True:
        raise RemoteOperatorTelegramError("refusing non-operator-safe workstation surface")
    return surface


def decision_inbox_items(surface: dict[str, Any]) -> list[dict[str, Any]]:
    inbox = surface.get("decision_inbox")
    if not isinstance(inbox, dict):
        return []
    items = inbox.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def open_decision_actions(surface: dict[str, Any]) -> list[dict[str, Any]]:
    """Summarize open decisions and their available action kinds."""

    decisions: list[dict[str, Any]] = []
    for item in decision_inbox_items(surface):
        envelopes = item.get("action_envelopes")
        actions: list[dict[str, Any]] = []
        if isinstance(envelopes, list):
            for envelope in envelopes:
                if not isinstance(envelope, dict):
                    continue
                action_kind = str(envelope.get("action_kind") or "").strip()
                if not action_kind:
                    continue
                latest = envelope.get("latest_receipt")
                stale = bool(latest.get("stale")) if isinstance(latest, dict) else False
                actions.append(
                    {
                        "action_kind": action_kind,
                        "observed_hash": str(envelope.get("observed_hash") or ""),
                        "requires_confirmation": bool(envelope.get("requires_confirmation")),
                        "stale": stale,
                    }
                )
        decisions.append(
            {
                "decision_id": str(item.get("decision_id") or ""),
                "title": sanitize_text(str(item.get("title") or ""), max_chars=120),
                "status": str(item.get("status") or ""),
                "actions": actions,
            }
        )
    return decisions


def find_action_envelope(
    surface: dict[str, Any],
    decision_id: str,
    action_kind: str,
) -> dict[str, Any] | None:
    wanted_decision = str(decision_id or "").strip()
    wanted_action = str(action_kind or "").strip().lower()
    for item in decision_inbox_items(surface):
        if str(item.get("decision_id") or "").strip() != wanted_decision:
            continue
        envelopes = item.get("action_envelopes")
        if not isinstance(envelopes, list):
            return None
        for envelope in envelopes:
            if not isinstance(envelope, dict):
                continue
            if str(envelope.get("action_kind") or "").strip().lower() == wanted_action:
                return envelope
    return None


def available_action_kinds(surface: dict[str, Any], decision_id: str) -> list[str]:
    for decision in open_decision_actions(surface):
        if decision["decision_id"] == str(decision_id or "").strip():
            return [action["action_kind"] for action in decision["actions"]]
    return []


def recovery_surface_items(surface: dict[str, Any]) -> list[dict[str, Any]]:
    recovery = surface.get("accepted_truth_recovery")
    if not isinstance(recovery, dict):
        return []
    items = recovery.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def open_recovery_actions(surface: dict[str, Any]) -> list[dict[str, Any]]:
    """Summarize accepted-truth recovery follow-ups and their action kinds."""

    recoveries: list[dict[str, Any]] = []
    for item in recovery_surface_items(surface):
        envelopes = item.get("action_envelopes")
        actions: list[dict[str, Any]] = []
        if isinstance(envelopes, list):
            for envelope in envelopes:
                if not isinstance(envelope, dict):
                    continue
                action_kind = str(envelope.get("action_kind") or "").strip()
                if not action_kind:
                    continue
                latest = envelope.get("latest_receipt")
                stale = bool(latest.get("stale")) if isinstance(latest, dict) else False
                actions.append(
                    {
                        "action_kind": action_kind,
                        "observed_hash": str(envelope.get("observed_hash") or ""),
                        "stale": stale,
                    }
                )
        recoveries.append(
            {
                "closeout_id": str(item.get("closeout_id") or ""),
                "stage": str(item.get("stage") or ""),
                "acceptance_status": str(item.get("acceptance_status") or ""),
                "next_safe_action": sanitize_text(
                    str(item.get("next_safe_action") or ""), max_chars=120
                ),
                "actions": actions,
            }
        )
    return recoveries


def find_recovery_envelope(
    surface: dict[str, Any],
    closeout_id: str,
    action_kind: str,
) -> dict[str, Any] | None:
    wanted_closeout = str(closeout_id or "").strip()
    wanted_action = str(action_kind or "").strip().lower()
    for item in recovery_surface_items(surface):
        if str(item.get("closeout_id") or "").strip() != wanted_closeout:
            continue
        envelopes = item.get("action_envelopes")
        if not isinstance(envelopes, list):
            return None
        for envelope in envelopes:
            if not isinstance(envelope, dict):
                continue
            if str(envelope.get("action_kind") or "").strip().lower() == wanted_action:
                return envelope
    return None


def available_recovery_action_kinds(surface: dict[str, Any], closeout_id: str) -> list[str]:
    for recovery in open_recovery_actions(surface):
        if recovery["closeout_id"] == str(closeout_id or "").strip():
            return [action["action_kind"] for action in recovery["actions"]]
    return []


def build_confirmation(
    *,
    kind: str,
    target_id: str,
    action_kind: str,
    observed_hash: str,
    note: str,
    chat_hash: str | None,
    ttl_sec: int = DEFAULT_CONFIRMATION_TTL_SEC,
) -> dict[str, Any]:
    now = utc_now()
    token = uuid.uuid4().hex[:12]
    expiry_seconds = max(30, int(ttl_sec))
    return {
        "schema": CONFIRMATION_SCHEMA,
        "token": token,
        "kind": str(kind or "decision").strip().lower(),
        "target_id": str(target_id or "").strip(),
        "action_kind": str(action_kind or "").strip().lower(),
        "observed_hash": str(observed_hash or ""),
        "note": sanitize_text(note, max_chars=400),
        "chat_id_hash": chat_hash,
        "created_at": now,
        "ttl_sec": expiry_seconds,
    }


def store_confirmation(
    state: dict[str, Any], chat_hash: str | None, confirmation: dict[str, Any]
) -> dict[str, Any] | None:
    """Store a pending confirmation, returning any it superseded.

    Only one pending confirmation per chat: a new request supersedes the old,
    so the previous token stops matching on /confirm. The returned previous
    confirmation lets callers tell the operator a pending action was replaced.
    """

    pending = state.setdefault("pending_dispatch_confirmations_by_chat", {})
    if not isinstance(pending, dict):
        pending = {}
        state["pending_dispatch_confirmations_by_chat"] = pending
    key = str(chat_hash or "")
    previous = pending.get(key)
    pending[key] = confirmation
    return previous if isinstance(previous, dict) else None


def confirmation_is_fresh(confirmation: dict[str, Any]) -> bool:
    created = parse_utc_timestamp(confirmation.get("created_at"))
    if created is None:
        return False
    ttl = int(confirmation.get("ttl_sec") or DEFAULT_CONFIRMATION_TTL_SEC)
    age = (parse_utc_timestamp(utc_now()) - created).total_seconds()
    return age <= ttl


def clear_confirmation(state: dict[str, Any], chat_hash: str | None) -> bool:
    pending = state.get("pending_dispatch_confirmations_by_chat")
    if not isinstance(pending, dict):
        return False
    return pending.pop(str(chat_hash or ""), None) is not None


def pop_confirmation(
    state: dict[str, Any],
    chat_hash: str | None,
    token: str,
) -> dict[str, Any] | None:
    pending = state.get("pending_dispatch_confirmations_by_chat")
    if not isinstance(pending, dict):
        return None
    key = str(chat_hash or "")
    confirmation = pending.get(key)
    if not isinstance(confirmation, dict):
        return None
    token = str(token or "").strip()
    # An empty token confirms the single pending confirmation for this chat (the
    # "확인" button path); a non-empty token must match the pending one exactly
    # (explicit /confirm <token>).
    if token and str(confirmation.get("token") or "") != token:
        return None
    # A matched confirmation is single-use whether or not the chain later succeeds.
    del pending[key]
    return confirmation


def apply_decision_action(
    forager_bin: str,
    profile: str,
    envelope: dict[str, Any],
    *,
    note: str,
) -> dict[str, Any]:
    """Run the full ondesk decision executor chain for one action envelope.

    Returns a compact result summary. The CLI re-validates the envelope's
    observed hash, nonce, and expiry, so a stale envelope is rejected here
    with a receipt rather than mutating anything.
    """

    result: dict[str, Any] = {
        "schema": DISPATCH_RESULT_SCHEMA,
        "ok": False,
        "stage": "action_envelope",
        "decision_id": str((envelope.get("target_ref") or {}).get("decision_id") or ""),
        "action_kind": str(envelope.get("action_kind") or ""),
    }
    envelope_path = _write_temp_envelope(envelope)
    try:
        validated = run_forager_json(
            forager_bin,
            profile,
            ["ondesk", "action-envelope", "--envelope", str(envelope_path), "--json"],
            label="action envelope validation",
        )
        receipt = validated.get("receipt") if isinstance(validated.get("receipt"), dict) else {}
        receipt_status = str(receipt.get("result_status") or "")
        result["receipt_id"] = receipt.get("receipt_id")
        result["receipt_status"] = receipt_status
        # action-envelope records a read-only "validated_preview" receipt when
        # the envelope still matches the current decision ledger; anything else
        # (or a stale flag) means the envelope must not proceed.
        if receipt.get("stale") or receipt_status != "validated_preview":
            result["stage"] = "action_envelope_rejected"
            result["error"] = sanitize_text(
                str(receipt.get("reason") or "envelope rejected"), max_chars=240
            )
            return result

        preflight = run_forager_json(
            forager_bin,
            profile,
            ["ondesk", "action-preflight", "--receipt-id", str(receipt["receipt_id"]), "--json"],
            label="action preflight",
        )
        preflight_obj = preflight.get("preflight") if isinstance(preflight.get("preflight"), dict) else {}
        result["preflight_id"] = preflight_obj.get("preflight_id")
        if str(preflight_obj.get("result_status") or "") != "ready_for_executor":
            result["stage"] = "action_preflight_rejected"
            result["error"] = sanitize_text(
                str(preflight_obj.get("reason") or "preflight not ready"), max_chars=240
            )
            return result

        decision_argv = [
            "ondesk",
            "action-decision",
            "--preflight-id",
            str(preflight_obj["preflight_id"]),
            "--json",
        ]
        cleaned_note = sanitize_text(note, max_chars=400)
        if cleaned_note:
            decision_argv.extend(["--note", cleaned_note])
        execution = run_forager_json(
            forager_bin,
            profile,
            decision_argv,
            label="decision action",
        )
        execution_obj = execution.get("execution") if isinstance(execution.get("execution"), dict) else {}
        execution_status = str(execution_obj.get("result_status") or "")
        result["execution_id"] = execution_obj.get("execution_id")
        result["execution_status"] = execution_status
        result["decision"] = execution_obj.get("decision")
        result["decision_appended"] = bool(execution.get("decision_appended"))

        closeout = run_forager_json(
            forager_bin,
            profile,
            [
                "ondesk",
                "action-closeout",
                "--execution-id",
                str(execution_obj["execution_id"]),
                "--json",
            ],
            label="decision action closeout",
        )
        closeout_obj = closeout.get("closeout") if isinstance(closeout.get("closeout"), dict) else {}
        result["closeout_status"] = str(closeout_obj.get("result_status") or "")
        result["closeout_appended"] = bool(closeout.get("closeout_appended"))

        if execution_status == "applied":
            result["ok"] = True
            result["stage"] = "applied"
        else:
            # A "blocked" execution is a valid, recorded outcome (e.g. revise
            # without a direction note), not a transport failure.
            result["stage"] = "execution_not_applied"
            result["error"] = sanitize_text(
                str(execution_obj.get("reason") or f"execution {execution_status}"),
                max_chars=240,
            )
        return result
    finally:
        try:
            envelope_path.unlink()
        except OSError:
            pass


CANCELLABLE_TASK_STATUSES_EXCLUDED = {"completed", "cancelled"}


def find_tmux_session_name(session_id: str) -> str | None:
    """Resolve the tmux session whose name ends with the 8-char id suffix."""

    suffix = "_" + str(session_id or "")[:8]
    try:
        listing = subprocess.run(
            ["tmux", "ls", "-F", "#S"], check=False, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, text=True, timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    names = [name for name in listing.stdout.splitlines() if name.endswith(suffix)]
    return names[0] if len(names) == 1 else None


def capture_session_tail(tmux_name: str, *, lines: int = 30) -> str:
    """Read-only tail of a tmux pane, for summarizing what the agent asks."""

    try:
        capture = subprocess.run(
            ["tmux", "capture-pane", "-p", "-t", tmux_name], check=False,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    rows = [line.rstrip() for line in capture.stdout.splitlines()]
    rows = [line for line in rows if line.strip()]
    return "\n".join(rows[-lines:])


def pane_prompt_hash(tail: str) -> str:
    """Stable short fingerprint of the visible prompt, for informed consent."""

    return hashlib.sha256(tail.encode("utf-8")).hexdigest()[:8] if tail else ""


PROMPT_OPTION_RE = re.compile(r"^\s*(?:[❯>]\s*)?(\d)[.)]\s+(\S.{0,50})")


def parse_prompt_options(tail: str) -> list[dict[str, str]]:
    """Extract numbered choices (e.g. '1. Yes  2. No') from a prompt pane."""

    options: dict[str, str] = {}
    for line in tail.splitlines():
        match = PROMPT_OPTION_RE.match(line)
        if match:
            options[match.group(1)] = match.group(2).strip()
    if len(options) < 2:
        return []
    return [{"key": key, "label": options[key]} for key in sorted(options)][:4]


def apply_session_input(
    forager_bin: str,
    profile: str,
    session_id: str,
    action: str,
    *,
    status_file: Any = None,
    expected_hash: str = "",
    option_key: str = "",
) -> dict[str, Any]:
    """Send one approve/deny keystroke to a waiting supervised session.

    Re-verifies at confirm time that the session still waits for input,
    resolves its tmux session by the id suffix in the session name, then
    sends Enter (accept the highlighted default) or Escape (dismiss). It
    never types text into the agent.
    """

    result: dict[str, Any] = {"ok": False, "kind": "session_input", "action": action}
    if status_file is not None:
        try:
            status = json.loads(pathlib.Path(status_file).read_text())
        except (OSError, json.JSONDecodeError) as error:
            result["error"] = f"status_unavailable:{error}"
            return result
    else:
        status = run_forager_json(forager_bin, profile, ["status", "--json"], label="session status")
    rows = [row for row in status.get("sessions") or [] if isinstance(row, dict)]
    matches = [row for row in rows if str(row.get("id") or "").startswith(session_id)]
    if len(matches) != 1:
        result["error"] = "session_not_found" if not matches else "session_id_ambiguous"
        return result
    session = matches[0]
    result["session_title"] = str(session.get("title") or "")
    result["project"] = str(session.get("project") or "")
    if str(session.get("status") or "") != "waiting":
        result["error"] = f"session_not_waiting:{session.get('status')}"
        return result
    tmux_name = find_tmux_session_name(str(session.get("id") or ""))
    if tmux_name is None:
        result["error"] = "tmux_session_not_found"
        return result
    if expected_hash:
        # Informed consent: the keystroke applies to the prompt the operator
        # saw on the card. If the pane moved on, refuse and report the new
        # state instead of blind-approving whatever is there now.
        tail = capture_session_tail(tmux_name, lines=15)
        current_hash = pane_prompt_hash(tail)
        if current_hash != expected_hash:
            result["error"] = "prompt_changed"
            result["new_hash"] = current_hash
            result["prompt_line"] = sanitize_text(tail.splitlines()[-1] if tail else "", max_chars=100)
            return result
    if action == "approve" and option_key:
        key = option_key
    else:
        key = "Enter" if action == "approve" else "Escape"
    try:
        subprocess.run(
            ["tmux", "send-keys", "-t", tmux_name, key],
            check=True, timeout=10,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError) as error:
        result["error"] = f"tmux_failed:{error}"
        return result
    result["ok"] = True
    return result


def cancellable_tasks_from_surface(surface: dict[str, Any]) -> list[dict[str, Any]]:
    """List tasks that can still be cancelled, drawn from project task rows."""

    tasks: list[dict[str, Any]] = []
    projects = surface.get("projects")
    if not isinstance(projects, list):
        return tasks
    for project in projects:
        if not isinstance(project, dict):
            continue
        items = project.get("task_items")
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            status = str(item.get("status") or "").strip().lower()
            if status in CANCELLABLE_TASK_STATUSES_EXCLUDED:
                continue
            task_id = str(item.get("task_id") or "").strip()
            if not task_id:
                continue
            tasks.append(
                {
                    "task_id": task_id,
                    "status": status,
                    "title": sanitize_text(str(item.get("title") or task_id), max_chars=80),
                    "project_key": str(
                        item.get("project_key") or project.get("project_key") or ""
                    ),
                }
            )
    return tasks


def apply_cancel_task(
    forager_bin: str,
    profile: str,
    task_id: str,
    *,
    reason: str,
) -> dict[str, Any]:
    """Mark a durable offdesk task cancelled.

    This is the fail-safe direction (it stops the harness from continuing or
    resuming the task) but it does not kill an already-running background
    process; the caller surfaces that nuance to the operator.
    """

    result: dict[str, Any] = {
        "schema": DISPATCH_RESULT_SCHEMA,
        "ok": False,
        "kind": "cancel_task",
        "task_id": str(task_id or ""),
    }
    argv = ["offdesk", "cancel-task", str(task_id)]
    cleaned = sanitize_text(reason, max_chars=200)
    if cleaned:
        argv.extend(["--reason", cleaned])
    argv.append("--json")
    try:
        report = run_forager_json(forager_bin, profile, argv, label="cancel task")
    except RemoteOperatorTelegramError as error:
        result["stage"] = "cancel_failed"
        result["error"] = sanitize_text(str(error), max_chars=240)
        return result
    result["ok"] = True
    result["stage"] = "cancelled" if report.get("changed") else "unchanged"
    result["changed"] = bool(report.get("changed"))
    result["previous_status"] = report.get("previous_status")
    result["status"] = report.get("status")
    result["message"] = sanitize_text(str(report.get("message") or ""), max_chars=160)
    return result


def apply_operator_pause(
    forager_bin: str,
    profile: str,
    *,
    reason: str,
) -> dict[str, Any]:
    """Engage the global operator pause: halt all new offdesk dispatch."""

    result: dict[str, Any] = {"schema": DISPATCH_RESULT_SCHEMA, "ok": False, "kind": "pause"}
    argv = ["offdesk", "pause", "--by", "telegram"]
    cleaned = sanitize_text(reason, max_chars=200)
    if cleaned:
        argv.extend(["--reason", cleaned])
    argv.append("--json")
    try:
        state = run_forager_json(forager_bin, profile, argv, label="operator pause")
    except RemoteOperatorTelegramError as error:
        result["error"] = sanitize_text(str(error), max_chars=240)
        return result
    result["ok"] = True
    result["paused"] = bool(state.get("paused"))
    result["reason"] = sanitize_text(str(state.get("reason") or ""), max_chars=160)
    return result


def apply_operator_resume(forager_bin: str, profile: str) -> dict[str, Any]:
    """Clear the global operator pause so new dispatch can proceed again."""

    result: dict[str, Any] = {"schema": DISPATCH_RESULT_SCHEMA, "ok": False, "kind": "resume"}
    try:
        state = run_forager_json(
            forager_bin,
            profile,
            ["offdesk", "unpause", "--by", "telegram", "--json"],
            label="operator resume",
        )
    except RemoteOperatorTelegramError as error:
        result["error"] = sanitize_text(str(error), max_chars=240)
        return result
    result["ok"] = True
    result["paused"] = bool(state.get("paused"))
    return result


def runtime_dispatch_items(surface: dict[str, Any]) -> list[dict[str, Any]]:
    runtime = surface.get("runtime_dispatch")
    if not isinstance(runtime, dict):
        return []
    items = runtime.get("items")
    if not isinstance(items, list):
        return []
    return [item for item in items if isinstance(item, dict)]


def open_runtime_dispatch(surface: dict[str, Any]) -> list[dict[str, Any]]:
    """Summarize post-closeout handoffs ready for runtime dispatch."""

    rows: list[dict[str, Any]] = []
    for item in runtime_dispatch_items(surface):
        latest = item.get("latest_receipt")
        already_queued = (
            str(latest.get("result_status") or "") == "queued"
            if isinstance(latest, dict)
            else False
        )
        rows.append(
            {
                "closeout_id": str(item.get("closeout_id") or ""),
                "decision_id": str(item.get("decision_id") or ""),
                "title": sanitize_text(str(item.get("title") or ""), max_chars=120),
                "stage": str(item.get("stage") or ""),
                "already_queued": already_queued,
            }
        )
    return rows


def runtime_dispatch_item(surface: dict[str, Any], closeout_id: str) -> dict[str, Any] | None:
    wanted = str(closeout_id or "").strip()
    for item in runtime_dispatch_items(surface):
        if str(item.get("closeout_id") or "").strip() == wanted:
            return item
    return None


def apply_runtime_dispatch(
    forager_bin: str,
    profile: str,
    *,
    closeout_id: str,
    runner: str,
    command: str,
) -> dict[str, Any]:
    """Preflight a receipted closeout and queue a runtime task.

    runtime-preflight re-verifies the closeout against the latest canonical
    decision receipt at dispatch time, so a changed decision is rejected here
    rather than queued. The queued command runs later only through
    `forager offdesk tick` and the scheduler gate; this call does not launch a
    process.
    """

    result: dict[str, Any] = {
        "schema": DISPATCH_RESULT_SCHEMA,
        "ok": False,
        "stage": "runtime_preflight",
        "kind": "runtime",
        "closeout_id": str(closeout_id or ""),
        "runner": str(runner or ""),
        "command_preview": sanitize_text(command, max_chars=120),
    }
    preflight = run_forager_json(
        forager_bin,
        profile,
        ["ondesk", "runtime-preflight", "--closeout-id", str(closeout_id), "--json"],
        label="runtime dispatch preflight",
    )
    pf = preflight.get("preflight") if isinstance(preflight.get("preflight"), dict) else {}
    result["preflight_id"] = pf.get("preflight_id")
    if str(pf.get("result_status") or "") != "ready_for_runtime_dispatch":
        result["stage"] = "runtime_preflight_rejected"
        result["error"] = sanitize_text(
            str(pf.get("reason") or "runtime preflight not ready"), max_chars=240
        )
        return result

    dispatch = run_forager_json(
        forager_bin,
        profile,
        [
            "ondesk",
            "runtime-dispatch",
            "--preflight-id",
            str(pf["preflight_id"]),
            "--runner",
            str(runner),
            "--cmd",
            str(command),
            "--json",
        ],
        label="runtime dispatch",
    )
    receipt = dispatch.get("receipt") if isinstance(dispatch.get("receipt"), dict) else {}
    result["receipt_id"] = receipt.get("receipt_id")
    result["task_id"] = receipt.get("task_id")
    dispatch_status = str(receipt.get("result_status") or "")
    result["dispatch_status"] = dispatch_status
    result["task_enqueued"] = bool(dispatch.get("task_enqueued"))
    if dispatch_status == "queued":
        result["ok"] = True
        result["stage"] = "queued"
    else:
        result["stage"] = "runtime_dispatch_not_queued"
        result["error"] = sanitize_text(
            str(receipt.get("reason") or f"runtime dispatch {dispatch_status}"), max_chars=240
        )
    return result


def apply_recovery_action(
    forager_bin: str,
    profile: str,
    envelope: dict[str, Any],
) -> dict[str, Any]:
    """Validate an accepted-truth recovery envelope and record its receipt.

    This intentionally stops at validation: recording accepted truth or
    running the fallback command remains a separate explicit local step that
    this surface does not perform.
    """

    result: dict[str, Any] = {
        "schema": DISPATCH_RESULT_SCHEMA,
        "ok": False,
        "stage": "recovery_envelope",
        "kind": "recovery",
        "closeout_id": str((envelope.get("target_ref") or {}).get("closeout_id") or ""),
        "action_kind": str(envelope.get("action_kind") or ""),
    }
    envelope_path = _write_temp_envelope(envelope)
    try:
        validated = run_forager_json(
            forager_bin,
            profile,
            [
                "ondesk",
                "accepted-truth-recovery-envelope",
                "--envelope",
                str(envelope_path),
                "--json",
            ],
            label="recovery envelope validation",
        )
        receipt = validated.get("receipt") if isinstance(validated.get("receipt"), dict) else {}
        receipt_status = str(receipt.get("result_status") or "")
        result["receipt_id"] = receipt.get("receipt_id")
        result["receipt_status"] = receipt_status
        if receipt.get("stale") or receipt_status != "validated_preview":
            result["stage"] = "recovery_envelope_rejected"
            result["error"] = sanitize_text(
                str(receipt.get("reason") or "recovery envelope rejected"), max_chars=240
            )
            return result
        result["ok"] = True
        result["stage"] = "recovery_validated"
        return result
    finally:
        try:
            envelope_path.unlink()
        except OSError:
            pass


def _write_temp_envelope(envelope: dict[str, Any]):
    import os
    import pathlib
    import tempfile

    directory = pathlib.Path(tempfile.gettempdir()) / "forager_telegram_dispatch"
    directory.mkdir(parents=True, exist_ok=True)
    handle, name = tempfile.mkstemp(prefix="envelope_", suffix=".json", dir=directory)
    with os.fdopen(handle, "w", encoding="utf-8") as writer:
        writer.write(json.dumps(envelope, ensure_ascii=False, indent=2) + "\n")
    return pathlib.Path(name)
