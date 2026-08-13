"""Durable inbound-update journal for at-most-once guarded mutations."""

from __future__ import annotations

import json
import pathlib
from typing import Any

from .common import append_jsonl, utc_now, write_private_text_atomic
from .persistence import RemoteOperatorStateError


UPDATE_JOURNAL_SCHEMA = "remote_operator_telegram_update_journal.v2"
LEGACY_UPDATE_JOURNAL_SCHEMA = "remote_operator_telegram_update_journal.v1"
VALID_STATUSES = {
    "started",
    "effect_committed",
    "retry_authorized",
    "completed",
}
UNRESOLVED_STATUSES = {"started", "effect_committed"}


def _records(path: pathlib.Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise RemoteOperatorStateError(
            f"update journal is unreadable: {path} ({type(error).__name__})"
        ) from error
    records: list[dict[str, Any]] = []
    input_hash_by_update: dict[int, str] = {}
    for line_number, raw in enumerate(lines, start=1):
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as error:
            raise RemoteOperatorStateError(
                f"update journal contains invalid JSON at line {line_number}: {path}"
            ) from error
        if not isinstance(record, dict):
            raise RemoteOperatorStateError(
                f"update journal row {line_number} must be an object: {path}"
            )
        schema = record.get("schema")
        if schema not in {UPDATE_JOURNAL_SCHEMA, LEGACY_UPDATE_JOURNAL_SCHEMA}:
            raise RemoteOperatorStateError(
                f"update journal row {line_number} has an unsupported schema: {path}"
            )
        status = record.get("status")
        record_update_id = record.get("update_id")
        input_hash = record.get("input_hash")
        # The short-lived v1 format omitted input_hash from its completed row.
        # Recover it from the preceding started row so deployed journals remain
        # readable while all new transitions are written as v2.
        if (
            schema == LEGACY_UPDATE_JOURNAL_SCHEMA
            and isinstance(record_update_id, int)
            and not isinstance(record_update_id, bool)
            and (not isinstance(input_hash, str) or not input_hash)
        ):
            input_hash = input_hash_by_update.get(record_update_id)
            if input_hash:
                record = dict(record)
                record["input_hash"] = input_hash
        if (
            status not in VALID_STATUSES
            or isinstance(record_update_id, bool)
            or not isinstance(record_update_id, int)
            or not isinstance(input_hash, str)
            or not input_hash
        ):
            raise RemoteOperatorStateError(
                f"update journal row {line_number} is invalid: {path}"
            )
        input_hash_by_update[record_update_id] = input_hash
        records.append(record)
    return records


def latest_update_records(path: pathlib.Path) -> dict[int, dict[str, Any]]:
    latest: dict[int, dict[str, Any]] = {}
    for record in _records(path):
        latest[int(record["update_id"])] = record
    return latest


def latest_update_record(path: pathlib.Path, update_id: int) -> dict[str, Any] | None:
    return latest_update_records(path).get(update_id)


def _assert_input_matches(
    previous: dict[str, Any], *, update_id: int, input_hash: str
) -> None:
    if previous.get("input_hash") != input_hash:
        raise RemoteOperatorStateError(
            f"update {update_id} input hash does not match its durable journal entry"
        )


def journal_health_issue(path: pathlib.Path) -> str | None:
    latest = latest_update_records(path)
    if any(record.get("status") == "started" for record in latest.values()):
        return "update_journal_incomplete"
    if any(record.get("status") == "effect_committed" for record in latest.values()):
        return "update_journal_delivery_incomplete"
    return None


def journal_inspection(
    path: pathlib.Path, *, update_id: int | None = None
) -> dict[str, Any]:
    records = _records(path)
    latest = latest_update_records(path)
    if update_id is not None:
        records = [record for record in records if record["update_id"] == update_id]
        latest = (
            {update_id: latest[update_id]} if update_id in latest else {}
        )
    status_counts: dict[str, int] = {}
    for record in latest.values():
        status = str(record.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    return {
        "schema": "remote_operator_telegram_update_journal_inspection.v1",
        "generated_at": utc_now(),
        "path": str(path),
        "update_id": update_id,
        "record_count": len(records),
        "update_count": len(latest),
        "status_counts": status_counts,
        "unresolved_update_ids": sorted(
            key
            for key, record in latest.items()
            if record.get("status") in UNRESOLVED_STATUSES
        ),
        "latest_records": [latest[key] for key in sorted(latest)],
        "read_only": True,
        "mutation_authorized": False,
        "approval_authorized": False,
    }


def begin_update(
    path: pathlib.Path,
    *,
    update_id: int,
    input_hash: str,
    chat_hash: str,
    user_hash: str,
) -> dict[str, Any] | None:
    previous = latest_update_record(path, update_id)
    if previous:
        _assert_input_matches(previous, update_id=update_id, input_hash=input_hash)
        if previous.get("status") == "completed":
            return previous
        if previous.get("status") == "retry_authorized":
            previous = None
        else:
            raise RemoteOperatorStateError(
                f"update {update_id} has an incomplete durable journal entry; "
                "inspect and reconcile it before retrying"
            )
    record = {
        "schema": UPDATE_JOURNAL_SCHEMA,
        "recorded_at": utc_now(),
        "status": "started",
        "update_id": update_id,
        "input_hash": input_hash,
        "chat_id_hash": chat_hash,
        "user_id_hash": user_hash,
    }
    try:
        append_jsonl(path, record, durable=True)
    except OSError as error:
        raise RemoteOperatorStateError(
            f"update journal could not record update {update_id}: {type(error).__name__}"
        ) from error
    return None


def mark_effect_committed(
    path: pathlib.Path,
    *,
    update_id: int,
    input_hash: str,
    result: dict[str, Any],
) -> None:
    previous = latest_update_record(path, update_id)
    if previous is None:
        raise RemoteOperatorStateError(
            f"update {update_id} has no started journal entry"
        )
    _assert_input_matches(previous, update_id=update_id, input_hash=input_hash)
    if previous.get("status") in {"effect_committed", "completed"}:
        return
    if previous.get("status") != "started":
        raise RemoteOperatorStateError(
            f"update {update_id} cannot commit an effect from {previous.get('status')}"
        )
    record = {
        "schema": UPDATE_JOURNAL_SCHEMA,
        "recorded_at": utc_now(),
        "status": "effect_committed",
        "update_id": update_id,
        "input_hash": input_hash,
        "result_status": str(result.get("status") or "unknown"),
        "effect": result.get("effect") if isinstance(result.get("effect"), dict) else None,
        "delivery_id": result.get("delivery_id"),
    }
    try:
        append_jsonl(path, record, durable=True)
    except OSError as error:
        raise RemoteOperatorStateError(
            f"update journal could not commit update {update_id}: {type(error).__name__}"
        ) from error


def complete_update(
    path: pathlib.Path,
    *,
    update_id: int,
    input_hash: str,
    result: dict[str, Any],
) -> None:
    previous = latest_update_record(path, update_id)
    if previous is None:
        raise RemoteOperatorStateError(
            f"update {update_id} has no durable journal entry"
        )
    _assert_input_matches(previous, update_id=update_id, input_hash=input_hash)
    if previous.get("status") == "completed":
        return
    if previous.get("status") != "effect_committed":
        raise RemoteOperatorStateError(
            f"update {update_id} cannot complete from {previous.get('status')}"
        )
    record = {
        "schema": UPDATE_JOURNAL_SCHEMA,
        "recorded_at": utc_now(),
        "status": "completed",
        "update_id": update_id,
        "input_hash": input_hash,
        "result_status": str(result.get("status") or previous.get("result_status") or "unknown"),
        "send_status": str(result.get("send_status") or "unknown"),
        "conversation_record_status": str(
            result.get("conversation_record_status") or "unknown"
        ),
        "effect": (
            result.get("effect")
            if isinstance(result.get("effect"), dict)
            else previous.get("effect")
        ),
        "delivery_id": result.get("delivery_id") or previous.get("delivery_id"),
        "sent_message_id": result.get("sent_message_id"),
    }
    try:
        append_jsonl(path, record, durable=True)
    except OSError as error:
        raise RemoteOperatorStateError(
            f"update journal could not complete update {update_id}: {type(error).__name__}"
        ) from error


def reconcile_update(
    path: pathlib.Path,
    *,
    update_id: int,
    resolution: str,
    reason: str,
) -> dict[str, Any]:
    previous = latest_update_record(path, update_id)
    if previous is None:
        raise RemoteOperatorStateError(f"update {update_id} is not present in the journal")
    if previous.get("status") != "started":
        raise RemoteOperatorStateError(
            f"only a started update can be manually reconciled; update {update_id} "
            f"is {previous.get('status')}"
        )
    reason = str(reason or "").strip()
    if not reason:
        raise RemoteOperatorStateError("journal reconciliation requires a non-empty reason")
    if resolution not in {"retry", "complete"}:
        raise RemoteOperatorStateError(
            "journal reconciliation resolution must be retry or complete"
        )
    status = "retry_authorized" if resolution == "retry" else "completed"
    record = {
        "schema": UPDATE_JOURNAL_SCHEMA,
        "recorded_at": utc_now(),
        "status": status,
        "update_id": update_id,
        "input_hash": previous["input_hash"],
        "manual_reconciliation": True,
        "resolution": resolution,
        "reason": reason,
        "result_status": "manual_retry_authorized" if resolution == "retry" else "manually_completed",
        "send_status": "not_attempted",
        "effect": None,
    }
    try:
        append_jsonl(path, record, durable=True)
    except OSError as error:
        raise RemoteOperatorStateError(
            f"update journal could not reconcile update {update_id}: {type(error).__name__}"
        ) from error
    return {
        "schema": "remote_operator_telegram_update_journal_reconciliation.v1",
        "generated_at": utc_now(),
        "update_id": update_id,
        "resolution": resolution,
        "status": status,
        "reason": reason,
        "read_only": False,
        "mutation_authorized": True,
        "authority_domain": "telegram_update_journal",
        "approval_authorized": False,
    }


def compact_update_journal(
    path: pathlib.Path, *, committed_before_update_id: int
) -> dict[str, Any]:
    before_record_count = len(_records(path))
    latest = latest_update_records(path)
    retained = [
        record
        for update_id, record in sorted(latest.items())
        if record.get("status") != "completed"
        or update_id >= committed_before_update_id
    ]
    normalized = [{**record, "schema": UPDATE_JOURNAL_SCHEMA} for record in retained]
    content = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        for record in normalized
    )
    write_private_text_atomic(path, content)
    return {
        "before_record_count": before_record_count,
        "after_record_count": len(normalized),
        "before_update_count": len(latest),
        "removed_update_count": len(latest) - len(normalized),
    }
