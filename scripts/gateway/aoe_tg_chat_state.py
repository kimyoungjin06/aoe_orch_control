#!/usr/bin/env python3
"""Chat/session state helpers extracted from the gateway monolith."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List

from aoe_tg_room_handlers import DEFAULT_ROOM_NAME, normalize_room_token
from aoe_tg_task_view import normalize_project_name


DEFAULT_UI_LANG = "ko"
DEFAULT_REPORT_LEVEL = "normal"
_SESSION_KEEP_KEYS = (
    "pending_mode",
    "default_mode",
    "lang",
    "report_level",
    "room",
    "confirm_action",
    "recent_task_refs",
    "selected_task_refs",
    "last_cmd_args",
)


def now_iso() -> str:
    return datetime.now().astimezone().replace(microsecond=0).isoformat()


def normalize_chat_lang_token(raw: Any, fallback: str = "") -> str:
    token = str(raw or "").strip().lower()
    aliases = {
        "ko": "ko",
        "kr": "ko",
        "kor": "ko",
        "korean": "ko",
        "한국어": "ko",
        "한글": "ko",
        "en": "en",
        "eng": "en",
        "english": "en",
        "영어": "en",
    }
    normalized = aliases.get(token, "")
    if normalized:
        return normalized
    fb = str(fallback or "").strip().lower()
    return aliases.get(fb, "")


def sanitize_chat_session_row(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}

    row: Dict[str, Any] = {}
    mode = str(raw.get("pending_mode", "")).strip().lower()
    if mode in {"dispatch", "direct"}:
        row["pending_mode"] = mode
    default_mode = str(raw.get("default_mode", "")).strip().lower()
    if default_mode in {"dispatch", "direct"}:
        row["default_mode"] = default_mode
    lang = normalize_chat_lang_token(raw.get("lang"), "")
    if lang in {"ko", "en"}:
        row["lang"] = lang
    report_level = str(raw.get("report_level", "")).strip().lower()
    if report_level in {"short", "normal", "long"}:
        row["report_level"] = report_level
    room = normalize_room_token(str(raw.get("room", "")).strip())
    if room and room != DEFAULT_ROOM_NAME:
        row["room"] = room

    raw_last_cmd = raw.get("last_cmd_args")
    last_cmd_args: Dict[str, str] = {}
    if isinstance(raw_last_cmd, dict):
        for k, v in raw_last_cmd.items():
            key = str(k or "").strip().lower()
            val = str(v or "").strip()
            if not key or not val or len(key) > 40:
                continue
            last_cmd_args[key] = val[:800]
    if last_cmd_args:
        row["last_cmd_args"] = last_cmd_args

    raw_confirm = raw.get("confirm_action")
    if isinstance(raw_confirm, dict):
        confirm_mode = str(raw_confirm.get("mode", "")).strip().lower()
        confirm_prompt = str(raw_confirm.get("prompt", "")).strip()
        if confirm_mode in {"dispatch", "direct"} and confirm_prompt:
            confirm_row: Dict[str, Any] = {
                "mode": confirm_mode,
                "prompt": confirm_prompt[:2000],
                "requested_at": str(raw_confirm.get("requested_at", "")).strip() or now_iso(),
                "risk": str(raw_confirm.get("risk", "")).strip()[:80],
            }
            orch_name = str(raw_confirm.get("orch", "")).strip()
            if orch_name:
                confirm_row["orch"] = orch_name
            row["confirm_action"] = confirm_row

    recent_in = raw.get("recent_task_refs")
    recent_out: Dict[str, List[str]] = {}
    if isinstance(recent_in, dict):
        for pname, refs in recent_in.items():
            project_key = normalize_project_name(str(pname or ""))
            if not project_key or not isinstance(refs, list):
                continue
            dedup: List[str] = []
            seen = set()
            for item in refs:
                rid = str(item or "").strip()
                if not rid or rid in seen:
                    continue
                seen.add(rid)
                dedup.append(rid)
                if len(dedup) >= 50:
                    break
            if dedup:
                recent_out[project_key] = dedup
    if recent_out:
        row["recent_task_refs"] = recent_out

    selected_in = raw.get("selected_task_refs")
    selected_out: Dict[str, str] = {}
    if isinstance(selected_in, dict):
        for pname, rid in selected_in.items():
            project_key = normalize_project_name(str(pname or ""))
            request_id = str(rid or "").strip()
            if project_key and request_id:
                selected_out[project_key] = request_id
    if selected_out:
        row["selected_task_refs"] = selected_out

    if row:
        row["updated_at"] = str(raw.get("updated_at", "")).strip() or now_iso()
    return row


def get_chat_sessions(state: Dict[str, Any]) -> Dict[str, Any]:
    sessions = state.get("chat_sessions")
    if not isinstance(sessions, dict):
        sessions = {}
        state["chat_sessions"] = sessions
    return sessions


def get_chat_session_row(state: Dict[str, Any], chat_id: str, create: bool = False) -> Dict[str, Any]:
    token = str(chat_id or "").strip()
    if not token:
        return {}
    sessions = get_chat_sessions(state)
    row = sessions.get(token)
    if not isinstance(row, dict):
        if not create:
            return {}
        row = {}
        sessions[token] = row
    return row


def _cleanup_chat_session(state: Dict[str, Any], chat_id: str) -> None:
    token = str(chat_id or "").strip()
    if not token:
        return
    sessions = get_chat_sessions(state)
    row = sessions.get(token)
    if not isinstance(row, dict):
        return
    if not any(key in row for key in _SESSION_KEEP_KEYS):
        sessions.pop(token, None)


def get_pending_mode(state: Dict[str, Any], chat_id: str) -> str:
    row = get_chat_session_row(state, chat_id, create=False)
    mode = str(row.get("pending_mode", "")).strip().lower()
    return mode if mode in {"dispatch", "direct"} else ""


def set_pending_mode(state: Dict[str, Any], chat_id: str, mode: str) -> None:
    normalized = str(mode or "").strip().lower()
    if normalized not in {"dispatch", "direct"} or not str(chat_id or "").strip():
        return
    row = get_chat_session_row(state, chat_id, create=True)
    row["pending_mode"] = normalized
    row["updated_at"] = now_iso()


def clear_pending_mode(state: Dict[str, Any], chat_id: str) -> bool:
    token = str(chat_id or "").strip()
    if not token:
        return False
    sessions = get_chat_sessions(state)
    row = sessions.get(token)
    if not isinstance(row, dict):
        return False
    existed = "pending_mode" in row
    row.pop("pending_mode", None)
    if existed:
        row["updated_at"] = now_iso()
    _cleanup_chat_session(state, token)
    return existed


def get_default_mode(state: Dict[str, Any], chat_id: str) -> str:
    row = get_chat_session_row(state, chat_id, create=False)
    mode = str(row.get("default_mode", "")).strip().lower()
    return mode if mode in {"dispatch", "direct"} else ""


def set_default_mode(state: Dict[str, Any], chat_id: str, mode: str) -> None:
    normalized = str(mode or "").strip().lower()
    if normalized not in {"dispatch", "direct"} or not str(chat_id or "").strip():
        return
    row = get_chat_session_row(state, chat_id, create=True)
    row["default_mode"] = normalized
    row["updated_at"] = now_iso()


def clear_default_mode(state: Dict[str, Any], chat_id: str) -> bool:
    token = str(chat_id or "").strip()
    if not token:
        return False
    sessions = get_chat_sessions(state)
    row = sessions.get(token)
    if not isinstance(row, dict):
        return False
    existed = "default_mode" in row
    row.pop("default_mode", None)
    if existed:
        row["updated_at"] = now_iso()
    _cleanup_chat_session(state, token)
    return existed


def get_chat_lang(state: Dict[str, Any], chat_id: str, fallback: str = DEFAULT_UI_LANG) -> str:
    row = get_chat_session_row(state, chat_id, create=False)
    mode = normalize_chat_lang_token(row.get("lang", ""), fallback)
    if mode in {"ko", "en"}:
        return mode
    normalized_fallback = normalize_chat_lang_token(fallback, DEFAULT_UI_LANG)
    return normalized_fallback if normalized_fallback in {"ko", "en"} else DEFAULT_UI_LANG


def set_chat_lang(state: Dict[str, Any], chat_id: str, lang: str) -> None:
    normalized = normalize_chat_lang_token(lang, "")
    if normalized not in {"ko", "en"} or not str(chat_id or "").strip():
        return
    row = get_chat_session_row(state, chat_id, create=True)
    row["lang"] = normalized
    row["updated_at"] = now_iso()


def get_chat_report_level(state: Dict[str, Any], chat_id: str, fallback: str = DEFAULT_REPORT_LEVEL) -> str:
    row = get_chat_session_row(state, chat_id, create=False)
    token = str(row.get("report_level", "")).strip().lower()
    if token in {"short", "normal", "long"}:
        return token
    fb = str(fallback or "").strip().lower()
    return fb if fb in {"short", "normal", "long"} else DEFAULT_REPORT_LEVEL


def set_chat_report_level(state: Dict[str, Any], chat_id: str, level: str) -> None:
    normalized = str(level or "").strip().lower()
    if normalized not in {"short", "normal", "long"} or not str(chat_id or "").strip():
        return
    row = get_chat_session_row(state, chat_id, create=True)
    row["report_level"] = normalized
    row["updated_at"] = now_iso()


def get_chat_room(state: Dict[str, Any], chat_id: str, fallback: str = DEFAULT_ROOM_NAME) -> str:
    row = get_chat_session_row(state, chat_id, create=False)
    token = normalize_room_token(str(row.get("room", "")).strip())
    if token and token != DEFAULT_ROOM_NAME:
        return token
    fb = normalize_room_token(str(fallback or "").strip())
    return fb or DEFAULT_ROOM_NAME


def set_chat_room(state: Dict[str, Any], chat_id: str, room: str) -> None:
    if not str(chat_id or "").strip():
        return
    token = normalize_room_token(str(room or "").strip())
    row = get_chat_session_row(state, chat_id, create=True)
    if token and token != DEFAULT_ROOM_NAME:
        row["room"] = token
    else:
        row.pop("room", None)
    row["updated_at"] = now_iso()


def clear_chat_report_level(state: Dict[str, Any], chat_id: str) -> bool:
    token = str(chat_id or "").strip()
    if not token:
        return False
    sessions = get_chat_sessions(state)
    row = sessions.get(token)
    if not isinstance(row, dict):
        return False
    existed = "report_level" in row
    row.pop("report_level", None)
    if existed:
        row["updated_at"] = now_iso()
    _cleanup_chat_session(state, token)
    return existed


def get_confirm_action(state: Dict[str, Any], chat_id: str) -> Dict[str, Any]:
    row = get_chat_session_row(state, chat_id, create=False)
    raw = row.get("confirm_action")
    if not isinstance(raw, dict):
        return {}
    mode = str(raw.get("mode", "")).strip().lower()
    prompt = str(raw.get("prompt", "")).strip()
    if mode not in {"dispatch", "direct"} or not prompt:
        return {}
    out: Dict[str, Any] = {
        "mode": mode,
        "prompt": prompt,
        "requested_at": str(raw.get("requested_at", "")).strip() or now_iso(),
        "risk": str(raw.get("risk", "")).strip(),
    }
    orch = str(raw.get("orch", "")).strip()
    if orch:
        out["orch"] = orch
    return out


def set_confirm_action(
    state: Dict[str, Any],
    chat_id: str,
    mode: str,
    prompt: str,
    risk: str = "",
    orch: str = "",
) -> None:
    normalized_mode = str(mode or "").strip().lower()
    text = str(prompt or "").strip()
    if normalized_mode not in {"dispatch", "direct"} or not text or not str(chat_id or "").strip():
        return
    row = get_chat_session_row(state, chat_id, create=True)
    confirm_row: Dict[str, Any] = {
        "mode": normalized_mode,
        "prompt": text[:2000],
        "requested_at": now_iso(),
        "risk": str(risk or "").strip()[:80],
    }
    orch_name = str(orch or "").strip()
    if orch_name:
        confirm_row["orch"] = orch_name
    row["confirm_action"] = confirm_row
    row["updated_at"] = now_iso()


def clear_confirm_action(state: Dict[str, Any], chat_id: str) -> bool:
    token = str(chat_id or "").strip()
    if not token:
        return False
    sessions = get_chat_sessions(state)
    row = sessions.get(token)
    if not isinstance(row, dict):
        return False
    existed = "confirm_action" in row
    row.pop("confirm_action", None)
    if existed:
        row["updated_at"] = now_iso()
    _cleanup_chat_session(state, token)
    return existed


def get_chat_recent_task_refs(state: Dict[str, Any], chat_id: str, project_name: str) -> List[str]:
    row = get_chat_session_row(state, chat_id, create=False)
    refs_map = row.get("recent_task_refs")
    if not isinstance(refs_map, dict):
        return []
    refs = refs_map.get(normalize_project_name(project_name), [])
    if not isinstance(refs, list):
        return []
    out: List[str] = []
    for item in refs:
        rid = str(item or "").strip()
        if rid:
            out.append(rid)
    return out


def set_chat_recent_task_refs(state: Dict[str, Any], chat_id: str, project_name: str, refs: List[str]) -> None:
    if not str(chat_id or "").strip():
        return
    row = get_chat_session_row(state, chat_id, create=True)
    key = normalize_project_name(project_name)
    refs_map = row.get("recent_task_refs")
    if not isinstance(refs_map, dict):
        refs_map = {}
        row["recent_task_refs"] = refs_map

    dedup: List[str] = []
    seen = set()
    for item in refs:
        rid = str(item or "").strip()
        if not rid or rid in seen:
            continue
        seen.add(rid)
        dedup.append(rid)
        if len(dedup) >= 50:
            break

    if dedup:
        refs_map[key] = dedup
    else:
        refs_map.pop(key, None)
    if not refs_map:
        row.pop("recent_task_refs", None)

    selected_map = row.get("selected_task_refs")
    if isinstance(selected_map, dict):
        current = str(selected_map.get(key, "")).strip()
        if current and current not in dedup:
            selected_map.pop(key, None)
        if not selected_map:
            row.pop("selected_task_refs", None)

    row["updated_at"] = now_iso()


def touch_chat_recent_task_ref(state: Dict[str, Any], chat_id: str, project_name: str, request_id: str) -> None:
    rid = str(request_id or "").strip()
    if not rid:
        return
    refs = get_chat_recent_task_refs(state, chat_id, project_name)
    merged = [rid] + [x for x in refs if x != rid]
    set_chat_recent_task_refs(state, chat_id, project_name, merged[:50])


def get_chat_selected_task_ref(state: Dict[str, Any], chat_id: str, project_name: str) -> str:
    row = get_chat_session_row(state, chat_id, create=False)
    selected_map = row.get("selected_task_refs")
    if not isinstance(selected_map, dict):
        return ""
    return str(selected_map.get(normalize_project_name(project_name), "")).strip()


def set_chat_selected_task_ref(state: Dict[str, Any], chat_id: str, project_name: str, request_id: str) -> None:
    if not str(chat_id or "").strip():
        return
    row = get_chat_session_row(state, chat_id, create=True)
    key = normalize_project_name(project_name)
    selected_map = row.get("selected_task_refs")
    if not isinstance(selected_map, dict):
        selected_map = {}
        row["selected_task_refs"] = selected_map
    rid = str(request_id or "").strip()
    if rid:
        selected_map[key] = rid
    else:
        selected_map.pop(key, None)
    if not selected_map:
        row.pop("selected_task_refs", None)
    row["updated_at"] = now_iso()


def resolve_chat_task_ref(state: Dict[str, Any], chat_id: str, project_name: str, raw_ref: str) -> str:
    token = str(raw_ref or "").strip()
    if not token:
        return ""
    if token.isdigit():
        refs = get_chat_recent_task_refs(state, chat_id, project_name)
        idx = int(token)
        if 1 <= idx <= len(refs):
            return refs[idx - 1]
        return ""
    return token
