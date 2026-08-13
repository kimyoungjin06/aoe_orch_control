"""Durable Telegram reply outbox for committed inbound updates."""

from __future__ import annotations

import json
import pathlib
from typing import Any

from .common import append_jsonl, sha256_short, utc_now, write_private_text_atomic
from .persistence import RemoteOperatorStateError


REPLY_OUTBOX_SCHEMA = "remote_operator_telegram_reply_outbox.v1"
VALID_STATUSES = {"queued", "failed", "delivered"}
PENDING_STATUSES = {"queued", "failed"}


def _records(path: pathlib.Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as error:
        raise RemoteOperatorStateError(
            f"reply outbox is unreadable: {path} ({type(error).__name__})"
        ) from error
    records: list[dict[str, Any]] = []
    for line_number, raw in enumerate(lines, start=1):
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as error:
            raise RemoteOperatorStateError(
                f"reply outbox contains invalid JSON at line {line_number}: {path}"
            ) from error
        if not isinstance(record, dict):
            raise RemoteOperatorStateError(
                f"reply outbox row {line_number} must be an object: {path}"
            )
        update_id = record.get("update_id")
        if (
            record.get("schema") != REPLY_OUTBOX_SCHEMA
            or record.get("status") not in VALID_STATUSES
            or not isinstance(record.get("delivery_id"), str)
            or isinstance(update_id, bool)
            or not isinstance(update_id, int)
            or not isinstance(record.get("input_hash"), str)
            or not isinstance(record.get("payload_hash"), str)
        ):
            raise RemoteOperatorStateError(
                f"reply outbox row {line_number} is invalid: {path}"
            )
        records.append(record)
    return records


def latest_delivery_records(path: pathlib.Path) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in _records(path):
        latest[str(record["delivery_id"])] = record
    return latest


def pending_deliveries(path: pathlib.Path) -> list[dict[str, Any]]:
    return sorted(
        (
            record
            for record in latest_delivery_records(path).values()
            if record.get("status") in PENDING_STATUSES
        ),
        key=lambda record: (str(record.get("queued_at") or ""), record["delivery_id"]),
    )


def outbox_health_issue(path: pathlib.Path) -> str | None:
    return "reply_delivery_pending" if pending_deliveries(path) else None


def outbox_inspection(path: pathlib.Path) -> dict[str, Any]:
    latest = latest_delivery_records(path)
    status_counts: dict[str, int] = {}
    for record in latest.values():
        status = str(record.get("status") or "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
    pending = [
        {
            "delivery_id": record["delivery_id"],
            "update_id": record["update_id"],
            "status": record["status"],
            "queued_at": record.get("queued_at"),
            "attempt_count": int(record.get("attempt_count") or 0),
            "last_error": record.get("last_error"),
        }
        for record in pending_deliveries(path)
    ]
    return {
        "schema": "remote_operator_telegram_reply_outbox_inspection.v1",
        "path": str(path),
        "delivery_count": len(latest),
        "status_counts": status_counts,
        "pending_count": len(pending),
        "pending": pending,
    }


def _payload_hash(
    *, message: str, reply_markup: dict[str, Any] | None, chat_id: str
) -> str:
    return sha256_short(
        json.dumps(
            {
                "chat_id": str(chat_id),
                "message": str(message),
                "reply_markup": reply_markup,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


def queue_reply(
    path: pathlib.Path,
    *,
    update_id: int,
    input_hash: str,
    chat_id: str,
    message: str,
    reply_markup: dict[str, Any] | None,
    result: dict[str, Any],
) -> dict[str, Any]:
    delivery_id = f"telegram_update:{update_id}"
    payload_hash = _payload_hash(
        message=message, reply_markup=reply_markup, chat_id=chat_id
    )
    previous = latest_delivery_records(path).get(delivery_id)
    if previous:
        if (
            previous.get("input_hash") != input_hash
            or previous.get("payload_hash") != payload_hash
        ):
            raise RemoteOperatorStateError(
                f"reply outbox payload for update {update_id} does not match its durable entry"
            )
        return previous
    record = {
        "schema": REPLY_OUTBOX_SCHEMA,
        "recorded_at": utc_now(),
        "queued_at": utc_now(),
        "status": "queued",
        "delivery_id": delivery_id,
        "update_id": update_id,
        "input_hash": input_hash,
        "payload_hash": payload_hash,
        "chat_id": str(chat_id),
        "message": str(message),
        "reply_markup": reply_markup,
        "result_status": str(result.get("status") or "unknown"),
        "effect": result.get("effect") if isinstance(result.get("effect"), dict) else None,
        "attempt_count": 0,
    }
    try:
        append_jsonl(path, record, durable=True)
    except OSError as error:
        raise RemoteOperatorStateError(
            f"reply outbox could not queue update {update_id}: {type(error).__name__}"
        ) from error
    return record


def _append_transition(
    path: pathlib.Path,
    record: dict[str, Any],
    *,
    status: str,
    message_id: int | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    transitioned = dict(record)
    transitioned.update(
        {
            "recorded_at": utc_now(),
            "status": status,
            "attempt_count": int(record.get("attempt_count") or 0) + 1,
        }
    )
    if status == "delivered":
        transitioned["delivered_at"] = utc_now()
        transitioned["sent_message_id"] = message_id
        transitioned.pop("last_error", None)
    else:
        transitioned["last_error"] = str(error or "delivery failed")
    try:
        append_jsonl(path, transitioned, durable=True)
    except OSError as append_error:
        raise RemoteOperatorStateError(
            f"reply outbox could not record {status} for update {record['update_id']}: "
            f"{type(append_error).__name__}"
        ) from append_error
    return transitioned


def mark_delivery_failed(
    path: pathlib.Path, record: dict[str, Any], *, error: str
) -> dict[str, Any]:
    return _append_transition(path, record, status="failed", error=error)


def mark_delivery_delivered(
    path: pathlib.Path, record: dict[str, Any], *, message_id: int | None
) -> dict[str, Any]:
    return _append_transition(
        path, record, status="delivered", message_id=message_id
    )


def compact_reply_outbox(
    path: pathlib.Path, *, committed_before_update_id: int
) -> dict[str, Any]:
    before_record_count = len(_records(path))
    latest = latest_delivery_records(path)
    retained = [
        record
        for record in sorted(latest.values(), key=lambda item: str(item["delivery_id"]))
        if record.get("status") != "delivered"
        or int(record["update_id"]) >= committed_before_update_id
    ]
    content = "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n"
        for record in retained
    )
    write_private_text_atomic(path, content)
    return {
        "before_record_count": before_record_count,
        "after_record_count": len(retained),
        "before_delivery_count": len(latest),
        "after_delivery_count": len(retained),
        "removed_delivery_count": len(latest) - len(retained),
    }
