"""Shared helpers for the Telegram remote operator adapter."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import pathlib
import secrets
import stat
from typing import Any


class RemoteOperatorTelegramError(RuntimeError):
    pass


PRIVATE_FILE_MODE = 0o600


def private_file_mode_issue(path: pathlib.Path) -> str | None:
    """Return a fail-closed reason when a local secret is readable by peers."""

    try:
        mode = stat.S_IMODE(path.stat().st_mode)
    except OSError as error:
        return f"unreadable:{error}"
    if mode & 0o077:
        return f"permissions_too_open:{mode:04o}"
    return None


def require_private_file(path: pathlib.Path, *, label: str) -> None:
    issue = private_file_mode_issue(path)
    if issue:
        raise RemoteOperatorTelegramError(
            f"{label} must be private ({issue}); run: chmod 600 {path}"
        )


def _private_text_writer(path: pathlib.Path, *, append: bool):
    flags = os.O_WRONLY | os.O_CREAT | (os.O_APPEND if append else os.O_TRUNC)
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, PRIVATE_FILE_MODE)
    try:
        os.fchmod(descriptor, PRIVATE_FILE_MODE)
        return os.fdopen(descriptor, "a" if append else "w", encoding="utf-8")
    except BaseException:
        os.close(descriptor)
        raise


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path: pathlib.Path, value: Any) -> None:
    write_private_text_atomic(
        path,
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
    )


def write_private_text_atomic(path: pathlib.Path, content: str) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    tmp = path.with_name(
        f".{path.name}.{os.getpid()}.{secrets.token_hex(8)}.tmp"
    )
    try:
        with _private_text_writer(tmp, append=False) as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        tmp.replace(path)
        path.chmod(PRIVATE_FILE_MODE)
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            flags |= os.O_DIRECTORY
        parent_descriptor = os.open(path.parent, flags)
        try:
            os.fsync(parent_descriptor)
        finally:
            os.close(parent_descriptor)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise


def append_jsonl(path: pathlib.Path, value: Any, *, durable: bool = False) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with _private_text_writer(path, append=True) as handle:
        handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n")
        if durable:
            handle.flush()
            os.fsync(handle.fileno())


def load_json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def parse_env_file(path: pathlib.Path, *, required: bool) -> dict[str, str]:
    if not path.exists():
        if required:
            raise RemoteOperatorTelegramError(f"telegram env file not found: {path}")
        return {}
    require_private_file(path, label="telegram env file")
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


def unique_nonempty(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def sha256_short(value: str) -> str:
    digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()
    return f"sha256:{digest[:16]}"
