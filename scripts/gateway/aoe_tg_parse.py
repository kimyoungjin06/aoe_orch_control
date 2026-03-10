#!/usr/bin/env python3
"""Input parsing helpers for aoe-telegram-gateway."""

import os
import re
import shlex
from typing import Any, Dict, List, Optional, Tuple

from aoe_tg_acl import is_valid_chat_ref, normalize_acl_scope


def detect_high_risk_prompt(prompt: str) -> str:
    text = str(prompt or "").strip()
    if not text:
        return ""
    low = text.lower()

    regex_markers: List[Tuple[str, str]] = [
        (r"\brm\s+-rf\b", "destructive_delete"),
        (r"\bmkfs(\.| )", "filesystem_format"),
        (r"\bdd\s+if=", "raw_disk_write"),
        (r"\bshutdown\b", "shutdown"),
        (r"\breboot\b", "reboot"),
        (r"\bpoweroff\b", "poweroff"),
        (r"\bdrop\s+database\b", "drop_database"),
        (r"\btruncate\s+table\b", "truncate_table"),
        (r"\bdelete\s+from\b", "sql_delete"),
        (r"\bvisudo\b", "sudoers_edit"),
    ]
    for pattern, label in regex_markers:
        if re.search(pattern, low):
            return label

    keyword_markers: List[Tuple[str, str]] = [
        ("delete all", "delete_all"),
        ("format disk", "format_disk"),
        ("factory reset", "factory_reset"),
        ("wipe", "wipe"),
        ("초기화", "k_reset"),
        ("포맷", "k_format"),
        ("전부 삭제", "k_delete_all"),
        ("전체 삭제", "k_delete_all"),
        ("데이터 삭제", "k_delete_data"),
        ("재부팅", "k_reboot"),
    ]
    for token, label in keyword_markers:
        if token in low:
            return label
    return ""


def parse_command(text: str) -> Tuple[str, str]:
    text = (text or "").strip()
    if not text:
        return "", text

    # Command prefix is configurable to avoid collisions with Unix paths.
    # Example: set `AOE_TG_COMMAND_PREFIXES=!/` to prefer "!" and still accept "/".
    raw_prefixes = str(os.environ.get("AOE_TG_COMMAND_PREFIXES", "/") or "/").strip()
    prefixes = ""
    for ch in raw_prefixes:
        if ch in {"/", "!"} and ch not in prefixes:
            prefixes += ch
    if not prefixes:
        prefixes = "/"

    if text[0] not in prefixes:
        return "", text

    # Treat a bare prefix ("/" or "!") as a command-palette shortcut.
    if len(text) == 1:
        return "help", ""

    first, _, rest = text.partition(" ")
    token = first[1:]
    if "@" in token:
        token = token.split("@", 1)[0]
    # Guard: treat "/home/..."/"/etc/..." as plain text, not a command.
    if "/" in token:
        return "", text
    return token.lower().strip(), rest.strip()


def normalize_mode_token(raw: str) -> str:
    token = str(raw or "").strip().lower()
    aliases = {
        "": "status",
        "status": "status",
        "show": "status",
        "current": "status",
        "now": "status",
        "확인": "status",
        "현재": "status",
        "dispatch": "dispatch",
        "team": "dispatch",
        "task": "dispatch",
        "작업": "dispatch",
        "팀작업": "dispatch",
        "on": "dispatch",
        "enable": "dispatch",
        "enabled": "dispatch",
        "start": "dispatch",
        "켜기": "dispatch",
        "활성화": "dispatch",
        "direct": "direct",
        "ask": "direct",
        "question": "direct",
        "질문": "direct",
        "직접": "direct",
        "off": "off",
        "none": "off",
        "disable": "off",
        "clear": "off",
        "stop": "off",
        "해제": "off",
        "끄기": "off",
    }
    return aliases.get(token, "")


def normalize_lang_token(raw: str) -> str:
    token = str(raw or "").strip().lower()
    aliases = {
        "": "status",
        "status": "status",
        "show": "status",
        "current": "status",
        "now": "status",
        "확인": "status",
        "현재": "status",
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
    return aliases.get(token, "")


def normalize_report_token(raw: str) -> str:
    token = str(raw or "").strip().lower()
    aliases = {
        "": "status",
        "status": "status",
        "show": "status",
        "current": "status",
        "now": "status",
        "확인": "status",
        "현재": "status",
        # levels
        "short": "short",
        "brief": "short",
        "compact": "short",
        "minimal": "short",
        "1": "short",
        "짧게": "short",
        "요약": "short",
        "간단": "short",
        "normal": "normal",
        "default": "normal",
        "standard": "normal",
        "2": "normal",
        "보통": "normal",
        "기본": "normal",
        "long": "long",
        "detail": "long",
        "detailed": "long",
        "verbose": "long",
        "full": "long",
        "3": "long",
        "상세": "long",
        "자세히": "long",
        # reset
        "off": "off",
        "none": "off",
        "reset": "off",
        "해제": "off",
        "끄기": "off",
    }
    return aliases.get(token, "")


def normalize_loose_text(raw: str) -> str:
    return " ".join(str(raw or "").strip().split())


def infer_natural_run_mode(prompt: str, default_mode: str = "dispatch") -> str:
    base = str(default_mode or "").strip().lower()
    if base not in {"dispatch", "direct"}:
        base = "dispatch"

    text = normalize_loose_text(prompt)
    if not text:
        return base
    low = text.lower()

    direct_markers = (
        "?",
        "어떻게",
        "방법",
        "왜",
        "무엇",
        "뭐",
        "설명",
        "알려",
        "가능",
        "how ",
        "what ",
        "why ",
        "where ",
        "when ",
        "can ",
        "could ",
        "would ",
        "explain",
        "help me",
    )
    dispatch_markers = (
        "구현",
        "수정",
        "작성",
        "설치",
        "삭제",
        "이동",
        "생성",
        "실행",
        "진행",
        "만들어",
        "점검",
        "검증",
        "검토",
        "분석",
        "조사",
        "리팩토링",
        "commit",
        "build",
        "deploy",
        "fix ",
        "implement",
        "install",
        "delete",
        "create",
        "update",
        "verify",
        "validate",
        "inspect",
        "review",
        "analyze",
        "investigate",
    )

    direct_score = 0
    dispatch_score = 0

    if text.endswith("?"):
        direct_score += 2
    if any(marker in low for marker in direct_markers):
        direct_score += 2
    if any(marker in low for marker in dispatch_markers):
        dispatch_score += 2

    # Plain "check/report/status" requests are usually read/query intent.
    if any(marker in low for marker in ("확인", "상태", "리포트", "보고", "요약", "조회", "check", "status", "report", "summary")):
        direct_score += 1

    if base == "direct":
        if dispatch_score >= 2 and dispatch_score > direct_score:
            return "dispatch"
        return "direct"

    if direct_score > dispatch_score and direct_score >= 2:
        return "direct"
    if dispatch_score > direct_score and dispatch_score >= 2:
        return "dispatch"
    return base


def parse_quick_message(text: str) -> Optional[Dict[str, Any]]:
    norm = normalize_loose_text(text)
    if not norm or norm.startswith("/"):
        return None

    low = norm.lower()

    if low in {"help", "도움말", "메뉴", "menu"}:
        return {"cmd": "help"}

    if low in {"ok", "확인실행", "실행확인"}:
        return {"cmd": "confirm-run"}

    if low in {"mode", "모드"}:
        return {"cmd": "mode", "mode": "status"}
    if low in {"inbox"}:
        return {"cmd": "mode", "mode": "dispatch"}
    if low in {"on", "켜기", "활성화"}:
        return {"cmd": "mode", "mode": "dispatch"}
    if low in {"off", "끄기", "해제"}:
        return {"cmd": "mode", "mode": "off"}
    if low.startswith("mode "):
        mode_token = normalize_mode_token(norm.split(" ", 1)[1].strip())
        if mode_token:
            return {"cmd": "mode", "mode": mode_token}
        return {"cmd": "mode", "mode": "invalid"}
    if low.startswith("모드 "):
        mode_token = normalize_mode_token(norm.split(" ", 1)[1].strip())
        if mode_token:
            return {"cmd": "mode", "mode": mode_token}
        return {"cmd": "mode", "mode": "invalid"}

    if low in {"acl", "권한", "권한설정", "permissions", "permission"}:
        return {"cmd": "acl"}

    if low in {"lang", "language", "언어"}:
        return {"cmd": "lang", "lang": "status"}
    if low.startswith("lang "):
        lang_token = normalize_lang_token(norm.split(" ", 1)[1].strip())
        if lang_token:
            return {"cmd": "lang", "lang": lang_token}
        return {"cmd": "lang", "lang": "invalid"}
    if low.startswith("language "):
        lang_token = normalize_lang_token(norm.split(" ", 1)[1].strip())
        if lang_token:
            return {"cmd": "lang", "lang": lang_token}
        return {"cmd": "lang", "lang": "invalid"}
    if low.startswith("언어 "):
        lang_token = normalize_lang_token(norm.split(" ", 1)[1].strip())
        if lang_token:
            return {"cmd": "lang", "lang": lang_token}
        return {"cmd": "lang", "lang": "invalid"}

    if low in {"report", "verbosity", "보고", "리포트"}:
        return {"cmd": "report", "report": "status"}
    if low.startswith("report "):
        rep_token = normalize_report_token(norm.split(" ", 1)[1].strip())
        if rep_token:
            return {"cmd": "report", "report": rep_token}
        return {"cmd": "report", "report": "invalid"}
    if low.startswith("verbosity "):
        rep_token = normalize_report_token(norm.split(" ", 1)[1].strip())
        if rep_token:
            return {"cmd": "report", "report": rep_token}
        return {"cmd": "report", "report": "invalid"}
    if low.startswith("보고 "):
        rep_token = normalize_report_token(norm.split(" ", 1)[1].strip())
        if rep_token:
            return {"cmd": "report", "report": rep_token}
        return {"cmd": "report", "report": "invalid"}

    if low in {"status", "상태", "현재 상태", "현재상태"}:
        return {"cmd": "status"}

    if low in {"map", "맵", "지도", "매핑", "테이블"}:
        return {"cmd": "orch-list"}

    if low in {"todo", "todos", "할일", "할 일", "백로그"}:
        return {"cmd": "todo", "rest": ""}
    if low in {"todo next", "다음 todo", "다음 할일", "다음 할 일", "할일 실행", "todo 실행"}:
        return {"cmd": "todo", "rest": "next"}
    if low.startswith("todo "):
        return {"cmd": "todo", "rest": norm.split(" ", 1)[1].strip()}

    if low in {"sync", "동기화"}:
        return {"cmd": "sync", "rest": ""}
    if low in {"sync preview", "sync inspect", "동기화 미리보기", "동기화 프리뷰"}:
        return {"cmd": "sync", "rest": "preview"}
    if low.startswith("sync preview "):
        return {"cmd": "sync", "rest": f"preview {norm.split(' ', 2)[2].strip()}"}
    if low.startswith("sync inspect "):
        return {"cmd": "sync", "rest": f"preview {norm.split(' ', 2)[2].strip()}"}
    if low.startswith("동기화 미리보기 "):
        return {"cmd": "sync", "rest": f"preview {norm.split(' ', 2)[2].strip()}"}
    if low.startswith("동기화 프리뷰 "):
        return {"cmd": "sync", "rest": f"preview {norm.split(' ', 2)[2].strip()}"}
    if low.startswith("sync "):
        return {"cmd": "sync", "rest": norm.split(" ", 1)[1].strip()}
    if low.startswith("동기화 "):
        return {"cmd": "sync", "rest": norm.split(" ", 1)[1].strip()}

    if low in {"offdesk", "오프데스크", "offdesk status", "오프데스크 상태", "퇴근상태", "퇴근 상태"}:
        return {"cmd": "offdesk", "rest": "status"}
    if low in {"offdesk on", "오프데스크 켜기", "퇴근모드", "퇴근 모드"}:
        return {"cmd": "offdesk", "rest": "on"}
    if low in {"offdesk off", "오프데스크 끄기"}:
        return {"cmd": "offdesk", "rest": "off"}

    if low in {"auto", "오토", "자동", "auto status", "자동상태", "자동 상태"}:
        return {"cmd": "auto", "rest": "status"}
    if low in {"auto on", "자동 켜기"}:
        return {"cmd": "auto", "rest": "on"}
    if low in {"auto off", "자동 끄기"}:
        return {"cmd": "auto", "rest": "off"}

    if low in {"queue", "큐", "대기열"}:
        return {"cmd": "queue"}

    if low in {"kpi", "지표", "메트릭", "metrics"}:
        return {"cmd": "orch-kpi"}
    if low.startswith("kpi "):
        tail = norm.split(" ", 1)[1].strip()
        if tail.isdigit():
            return {"cmd": "orch-kpi", "hours": max(1, min(168, int(tail)))}
        return {"cmd": "orch-kpi"}

    if low in {"모니터", "작업목록", "목록", "monitor", "tasks"}:
        return {"cmd": "orch-monitor"}
    if low.startswith("모니터 ") or low.startswith("작업목록 "):
        tail = norm.split(" ", 1)[1].strip()
        if tail.isdigit():
            return {"cmd": "orch-monitor", "limit": max(1, min(50, int(tail)))}
        return {"cmd": "orch-monitor"}

    if low in {"진행", "진행 확인", "진행확인", "check"}:
        return {"cmd": "orch-check"}
    if low.startswith("진행 "):
        return {"cmd": "orch-check", "request_id": norm.split(" ", 1)[1].strip()}
    if low.startswith("check "):
        return {"cmd": "orch-check", "request_id": norm.split(" ", 1)[1].strip()}
    if low.startswith("확인 "):
        return {"cmd": "orch-check", "request_id": norm.split(" ", 1)[1].strip()}

    if low in {"상세", "상세 상태", "상세상태", "task", "lifecycle", "라이프사이클"}:
        return {"cmd": "orch-task"}
    if low.startswith("상세 "):
        return {"cmd": "orch-task", "request_id": norm.split(" ", 1)[1].strip()}
    if low.startswith("task "):
        return {"cmd": "orch-task", "request_id": norm.split(" ", 1)[1].strip()}
    if low.startswith("상태 "):
        return {"cmd": "orch-task", "request_id": norm.split(" ", 1)[1].strip()}

    if low in {"pick", "선택"}:
        return {"cmd": "orch-pick"}
    if low.startswith("pick "):
        return {"cmd": "orch-pick", "request_id": norm.split(" ", 1)[1].strip()}
    if low.startswith("선택 "):
        return {"cmd": "orch-pick", "request_id": norm.split(" ", 1)[1].strip()}

    if low.startswith("retry ") or low.startswith("재시도 ") or low.startswith("다시 "):
        return {"cmd": "orch-retry", "request_id": norm.split(" ", 1)[1].strip()}
    if low.startswith("replan ") or low.startswith("재계획 "):
        return {"cmd": "orch-replan", "request_id": norm.split(" ", 1)[1].strip()}
    if low.startswith("cancel ") or low.startswith("취소 "):
        return {"cmd": "orch-cancel", "request_id": norm.split(" ", 1)[1].strip()}

    if low in {"취소", "cancel", "취소해"}:
        return {"cmd": "cancel-pending"}

    if low in {"replay", "재실행큐", "재실행"}:
        return {"cmd": "replay"}
    if low in {"재실행 비우기", "재실행큐 비우기"}:
        return {"cmd": "replay", "target": "purge"}
    if low.startswith("재실행 상세 "):
        return {"cmd": "replay", "target": f"show {norm.split(' ', 2)[2].strip()}"}
    if low.startswith("replay "):
        return {"cmd": "replay", "target": norm.split(" ", 1)[1].strip()}
    if low.startswith("재실행 "):
        return {"cmd": "replay", "target": norm.split(" ", 1)[1].strip()}

    if low in {"팀작업", "작업", "dispatch"}:
        return {"cmd": "quick-dispatch"}
    if low in {"직접질문", "직접", "질문", "direct"}:
        return {"cmd": "quick-direct"}

    dispatch_prefixes = ("팀작업:", "팀작업 ", "작업:", "작업 ", "dispatch:", "dispatch ")
    for prefix in dispatch_prefixes:
        if low.startswith(prefix):
            prompt = norm[len(prefix) :].strip()
            if not prompt:
                return {"cmd": "quick-dispatch"}
            return {
                "cmd": "run",
                "prompt": prompt,
                "force_mode": "dispatch",
            }

    direct_prefixes = ("질문:", "질문 ", "직접:", "직접 ", "direct:", "direct ")
    for prefix in direct_prefixes:
        if low.startswith(prefix):
            prompt = norm[len(prefix) :].strip()
            if not prompt:
                return {"cmd": "quick-direct"}
            return {
                "cmd": "run",
                "prompt": prompt,
                "force_mode": "direct",
            }

    return None


def parse_cli_message(text: str) -> Optional[Dict[str, Any]]:
    raw = (text or "").strip()
    if not raw or raw.startswith("/"):
        return None

    try:
        parts = shlex.split(raw)
    except ValueError as e:
        raise RuntimeError(f"invalid CLI format: {e}") from e

    if not parts:
        return None

    first = parts[0].lower().strip()
    if first in {"aoe", "orch", "aoe-orch"}:
        parts = parts[1:]

    if not parts:
        return {"cmd": "help"}

    cmd = parts[0].lower().strip()
    argv = parts[1:]

    if cmd in {"help", "status"}:
        return {"cmd": cmd}
    if cmd == "map":
        return {"cmd": "orch-list"}

    if cmd in {"acl", "auth", "permissions"}:
        if argv:
            raise RuntimeError("usage: aoe acl")
        return {"cmd": "acl"}

    if cmd in {"mode", "inbox", "on", "off"}:
        if len(argv) > 1:
            raise RuntimeError("usage: aoe mode [on|off|direct|dispatch]")
        if cmd in {"inbox", "on"} and len(argv) == 0:
            token = "dispatch"
        elif cmd == "off" and len(argv) == 0:
            token = "off"
        else:
            token = argv[0] if argv else ""
        normalized = normalize_mode_token(token)
        if not normalized:
            raise RuntimeError("usage: aoe mode [on|off|direct|dispatch]")
        if normalized == "status":
            return {"cmd": "mode", "mode": "status"}
        return {"cmd": "mode", "mode": normalized}

    if cmd in {"lang", "language"}:
        if len(argv) > 1:
            raise RuntimeError("usage: aoe lang [ko|en]")
        token = argv[0] if argv else ""
        normalized = normalize_lang_token(token)
        if not normalized:
            raise RuntimeError("usage: aoe lang [ko|en]")
        if normalized == "status":
            return {"cmd": "lang", "lang": "status"}
        return {"cmd": "lang", "lang": normalized}

    if cmd in {"report", "verbosity"}:
        if len(argv) > 1:
            raise RuntimeError("usage: aoe report [short|normal|long|off]")
        token = argv[0] if argv else ""
        normalized = normalize_report_token(token)
        if not normalized:
            raise RuntimeError("usage: aoe report [short|normal|long|off]")
        if normalized == "status":
            return {"cmd": "report", "report": "status"}
        return {"cmd": "report", "report": normalized}

    if cmd in {"ok", "confirm"}:
        if argv:
            raise RuntimeError("usage: aoe ok")
        return {"cmd": "confirm-run"}

    if cmd == "grant":
        if len(argv) != 2:
            raise RuntimeError("usage: aoe grant <allow|admin|readonly> <chat_id|alias>")
        scope = normalize_acl_scope(argv[0])
        chat_ref = str(argv[1] or "").strip()
        if scope not in {"allow", "admin", "readonly"} or (not is_valid_chat_ref(chat_ref)):
            raise RuntimeError("usage: aoe grant <allow|admin|readonly> <chat_id|alias>")
        return {"cmd": "grant", "scope": scope, "chat_id": chat_ref}

    if cmd == "revoke":
        if len(argv) != 2:
            raise RuntimeError("usage: aoe revoke <allow|admin|readonly|all> <chat_id|alias>")
        scope = normalize_acl_scope(argv[0])
        chat_ref = str(argv[1] or "").strip()
        if scope not in {"allow", "admin", "readonly", "all"} or (not is_valid_chat_ref(chat_ref)):
            raise RuntimeError("usage: aoe revoke <allow|admin|readonly|all> <chat_id|alias>")
        return {"cmd": "revoke", "scope": scope, "chat_id": chat_ref}

    if cmd in {"kpi", "metrics"}:
        hours: Optional[int] = None
        if len(argv) == 1:
            if not argv[0].isdigit():
                raise RuntimeError("usage: aoe kpi [hours]")
            hours = max(1, min(168, int(argv[0])))
        elif len(argv) > 1:
            raise RuntimeError("usage: aoe kpi [hours]")
        return {"cmd": "orch-kpi", "hours": hours}

    if cmd in {"monitor", "tasks", "task-list"}:
        limit: Optional[int] = None
        if len(argv) == 1:
            if not argv[0].isdigit():
                raise RuntimeError("usage: aoe monitor [limit]")
            limit = max(1, min(50, int(argv[0])))
        elif len(argv) > 1:
            raise RuntimeError("usage: aoe monitor [limit]")
        return {"cmd": "orch-monitor", "limit": limit}

    if cmd in {"focus", "pin"}:
        if len(argv) > 1:
            raise RuntimeError("usage: aoe focus [O#|name|off]")
        return {"cmd": "focus", "rest": (argv[0].strip() if argv else "")}

    if cmd in {"unlock", "unfocus", "release"}:
        if argv:
            raise RuntimeError("usage: aoe unlock")
        return {"cmd": "focus", "rest": "off"}

    if cmd in {"todo", "todos"}:
        # Keep the rest as-is so Telegram slash UX and CLI UX share the same handler.
        return {"cmd": "todo", "rest": " ".join(argv).strip()}

    if cmd in {"room", "rooms", "r"}:
        return {"cmd": "room", "rest": " ".join(argv).strip()}

    if cmd in {"gc", "cleanup"}:
        return {"cmd": "gc", "rest": " ".join(argv).strip()}

    if cmd in {"offdesk", "off-desk", "od", "night"}:
        return {"cmd": "offdesk", "rest": " ".join(argv).strip()}

    if cmd in {"panic", "halt", "stopall", "emergency"}:
        return {"cmd": "panic", "rest": " ".join(argv).strip()}

    if cmd in {"auto"}:
        return {"cmd": "auto", "rest": " ".join(argv).strip()}

    if cmd in {"next"}:
        # Mother-Orch global scheduling (same handler as Telegram /next).
        return {"cmd": "next", "rest": " ".join(argv).strip()}

    if cmd in {"queue"}:
        # Mother-Orch global queue view (same handler as Telegram /queue).
        return {"cmd": "queue", "rest": " ".join(argv).strip()}

    if cmd in {"drain"}:
        # Continuous scheduling: run /next repeatedly up to a limit.
        return {"cmd": "drain", "rest": " ".join(argv).strip()}

    if cmd in {"fanout"}:
        # Fair scheduling wave: run at most one todo per project.
        return {"cmd": "fanout", "rest": " ".join(argv).strip()}

    if cmd in {"pick", "select"}:
        if len(argv) != 1:
            raise RuntimeError("usage: aoe pick <number|request_or_alias>")
        return {"cmd": "orch-pick", "request_id": argv[0].strip()}

    if cmd == "cancel":
        if len(argv) == 0:
            return {"cmd": "cancel-pending"}
        if len(argv) == 1:
            return {"cmd": "orch-cancel", "request_id": argv[0].strip()}
        raise RuntimeError("usage: aoe cancel [<request_or_alias>]")

    if cmd == "replay":
        if len(argv) == 0:
            return {"cmd": "replay", "target": ""}
        if len(argv) == 1:
            if str(argv[0]).strip().lower() == "show":
                raise RuntimeError("usage: aoe replay [list|latest|<idx>|<id>|show <idx|id|latest>|purge]")
            return {"cmd": "replay", "target": argv[0].strip()}
        if len(argv) == 2 and str(argv[0]).strip().lower() == "show":
            return {"cmd": "replay", "target": f"show {argv[1].strip()}"}
        raise RuntimeError("usage: aoe replay [list|latest|<idx>|<id>|show <idx|id|latest>|purge]")

    if cmd == "retry":
        if len(argv) != 1:
            raise RuntimeError("usage: aoe retry <request_or_alias>")
        return {"cmd": "orch-retry", "request_id": argv[0].strip()}

    if cmd == "replan":
        if len(argv) != 1:
            raise RuntimeError("usage: aoe replan <request_or_alias>")
        return {"cmd": "orch-replan", "request_id": argv[0].strip()}

    if cmd == "request":
        if len(argv) != 1:
            raise RuntimeError("usage: aoe request <request_or_alias>")
        return {"cmd": "request", "request_id": argv[0].strip()}

    if cmd == "run":
        roles: Optional[str] = None
        priority: Optional[str] = None
        timeout_sec: Optional[int] = None
        no_wait = False
        force_mode: Optional[str] = None
        prompt_tokens: List[str] = []

        i = 0
        while i < len(argv):
            tok = argv[i]
            if tok == "--":
                prompt_tokens.extend(argv[i + 1 :])
                break
            if tok == "--roles":
                i += 1
                if i >= len(argv):
                    raise RuntimeError("usage: aoe run --roles <csv> <prompt>")
                roles = argv[i].strip()
            elif tok == "--priority":
                i += 1
                if i >= len(argv):
                    raise RuntimeError("usage: aoe run --priority <P1|P2|P3> <prompt>")
                priority = argv[i].strip().upper()
                if priority not in {"P1", "P2", "P3"}:
                    raise RuntimeError("invalid priority (use P1/P2/P3)")
            elif tok == "--timeout-sec":
                i += 1
                if i >= len(argv):
                    raise RuntimeError("usage: aoe run --timeout-sec <seconds> <prompt>")
                try:
                    timeout_sec = max(1, int(argv[i]))
                except ValueError:
                    raise RuntimeError("--timeout-sec must be an integer")
            elif tok == "--no-wait":
                no_wait = True
            elif tok == "--direct":
                if force_mode == "dispatch":
                    raise RuntimeError("cannot use --direct with --dispatch")
                force_mode = "direct"
            elif tok == "--dispatch":
                if force_mode == "direct":
                    raise RuntimeError("cannot use --dispatch with --direct")
                force_mode = "dispatch"
            elif tok.startswith("--"):
                raise RuntimeError(f"unknown option: {tok}")
            else:
                prompt_tokens.extend(argv[i:])
                break
            i += 1

        prompt = " ".join(prompt_tokens).strip()
        if not prompt:
            raise RuntimeError(
                "usage: aoe run [--direct|--dispatch] [--roles <csv>] [--priority P1|P2|P3] [--timeout-sec N] [--no-wait] <prompt>"
            )

        return {
            "cmd": "run",
            "prompt": prompt,
            "roles": roles,
            "priority": priority,
            "timeout_sec": timeout_sec,
            "no_wait": no_wait,
            "force_mode": force_mode,
        }

    if cmd in {"add-role", "addrole"}:
        if not argv:
            raise RuntimeError("usage: aoe add-role <Role> [--provider <name>] [--launch <cmd>] [--spawn|--no-spawn]")

        role = ""
        provider: Optional[str] = None
        launch: Optional[str] = None
        spawn = True

        i = 0
        while i < len(argv):
            tok = argv[i]
            if tok == "--provider":
                i += 1
                if i >= len(argv):
                    raise RuntimeError("usage: --provider <name>")
                provider = argv[i].strip()
            elif tok == "--launch":
                i += 1
                if i >= len(argv):
                    raise RuntimeError("usage: --launch <command>")
                launch = argv[i]
            elif tok == "--spawn":
                spawn = True
            elif tok == "--no-spawn":
                spawn = False
            elif tok.startswith("--"):
                raise RuntimeError(f"unknown option: {tok}")
            else:
                if role:
                    raise RuntimeError("usage: aoe add-role <Role> [options]")
                role = tok.strip()
            i += 1

        if not role:
            raise RuntimeError("usage: aoe add-role <Role> [--provider <name>] [--launch <cmd>] [--spawn|--no-spawn]")

        return {
            "cmd": "add-role",
            "role": role,
            "provider": provider,
            "launch": launch,
            "spawn": spawn,
        }

    if cmd == "role":
        if not argv:
            raise RuntimeError("usage: aoe role add <Role> [options]")
        sub_cmd = argv[0].lower().strip()
        if sub_cmd != "add":
            raise RuntimeError("usage: aoe role add <Role> [options]")
        forwarded = "aoe add-role " + " ".join(shlex.quote(x) for x in argv[1:])
        return parse_cli_message(forwarded)

    if cmd == "orch":
        if not argv:
            return {"cmd": "orch-help"}

        sub = argv[0].lower().strip()
        sub_argv = argv[1:]

        if sub in {"help", "h"}:
            return {"cmd": "orch-help"}

        if sub in {"list", "ls", "map"}:
            return {"cmd": "orch-list"}

        if sub in {"use", "switch", "select"}:
            if len(sub_argv) != 1:
                raise RuntimeError("usage: aoe orch use <name>")
            return {"cmd": "orch-use", "orch": sub_argv[0].strip()}

        if sub in {"pick", "focus"}:
            orch_name: Optional[str] = None
            request_id: Optional[str] = None
            i = 0
            while i < len(sub_argv):
                tok = sub_argv[i]
                if tok == "--orch":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError(f"usage: aoe orch {sub} [--orch <name>] <number|request_or_alias>")
                    orch_name = sub_argv[i].strip()
                elif tok.startswith("--"):
                    raise RuntimeError(f"unknown option: {tok}")
                else:
                    if request_id is not None:
                        raise RuntimeError(f"usage: aoe orch {sub} [--orch <name>] <number|request_or_alias>")
                    request_id = tok.strip()
                i += 1
            if not request_id:
                raise RuntimeError(f"usage: aoe orch {sub} [--orch <name>] <number|request_or_alias>")
            return {"cmd": "orch-pick", "orch": orch_name, "request_id": request_id}

        if sub in {"status", "stat"}:
            orch_name: Optional[str] = None
            i = 0
            while i < len(sub_argv):
                tok = sub_argv[i]
                if tok == "--orch":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: aoe orch status [--orch <name>]")
                    orch_name = sub_argv[i].strip()
                elif tok.startswith("--"):
                    raise RuntimeError(f"unknown option: {tok}")
                else:
                    if orch_name is not None:
                        raise RuntimeError("usage: aoe orch status [--orch <name>]")
                    orch_name = tok.strip()
                i += 1
            return {"cmd": "orch-status", "orch": orch_name}

        if sub in {"repair", "init", "fix"}:
            orch_name: Optional[str] = None
            i = 0
            while i < len(sub_argv):
                tok = sub_argv[i]
                if tok == "--orch":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: aoe orch repair [--orch <name>]")
                    orch_name = sub_argv[i].strip()
                elif tok.startswith("--"):
                    raise RuntimeError(f"unknown option: {tok}")
                else:
                    if orch_name is not None:
                        raise RuntimeError("usage: aoe orch repair [--orch <name>]")
                    orch_name = tok.strip()
                i += 1
            return {"cmd": "orch-repair", "orch": orch_name}

        if sub in {"pause", "hold", "stop"}:
            if not sub_argv:
                raise RuntimeError("usage: aoe orch pause <name> [reason]")
            orch_name = sub_argv[0].strip()
            reason = " ".join(sub_argv[1:]).strip()
            return {"cmd": "orch-pause", "orch": orch_name, "rest": reason}

        if sub in {"resume", "unpause", "start"}:
            if not sub_argv:
                raise RuntimeError("usage: aoe orch resume <name>")
            orch_name = sub_argv[0].strip()
            return {"cmd": "orch-resume", "orch": orch_name}

        if sub in {"hide"}:
            if not sub_argv:
                raise RuntimeError("usage: aoe orch hide <name> [reason]")
            orch_name = sub_argv[0].strip()
            reason = " ".join(sub_argv[1:]).strip()
            return {"cmd": "orch-hide", "orch": orch_name, "rest": reason}

        if sub in {"unhide", "show"}:
            if not sub_argv:
                raise RuntimeError("usage: aoe orch unhide <name>")
            orch_name = sub_argv[0].strip()
            return {"cmd": "orch-unhide", "orch": orch_name}

        if sub in {"add", "create"}:
            orch_name = ""
            path = ""
            overview: Optional[str] = None
            do_init = True
            do_spawn = True
            set_active = True

            i = 0
            while i < len(sub_argv):
                tok = sub_argv[i]
                if tok == "--path":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: aoe orch add <name> --path <project_root> [--overview <text>] [--init|--no-init] [--spawn|--no-spawn]")
                    path = sub_argv[i].strip()
                elif tok == "--overview":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: --overview <text>")
                    overview = sub_argv[i]
                elif tok == "--init":
                    do_init = True
                elif tok == "--no-init":
                    do_init = False
                elif tok == "--spawn":
                    do_spawn = True
                elif tok == "--no-spawn":
                    do_spawn = False
                elif tok == "--set-active":
                    set_active = True
                elif tok == "--no-set-active":
                    set_active = False
                elif tok.startswith("--"):
                    raise RuntimeError(f"unknown option: {tok}")
                else:
                    if orch_name:
                        raise RuntimeError("usage: aoe orch add <name> --path <project_root> [options]")
                    orch_name = tok.strip()
                i += 1

            if not orch_name or not path:
                raise RuntimeError("usage: aoe orch add <name> --path <project_root> [--overview <text>] [--init|--no-init] [--spawn|--no-spawn]")

            return {
                "cmd": "orch-add",
                "orch": orch_name,
                "path": path,
                "overview": overview,
                "init": do_init,
                "spawn": do_spawn,
                "set_active": set_active,
            }

        if sub == "run":
            orch_name: Optional[str] = None
            passthrough: List[str] = []
            i = 0
            while i < len(sub_argv):
                tok = sub_argv[i]
                if tok == "--orch":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: aoe orch run [--orch <name>] [--direct|--dispatch] [--roles <csv>] [--priority P1|P2|P3] [--timeout-sec N] [--no-wait] <prompt>")
                    orch_name = sub_argv[i].strip()
                else:
                    passthrough.append(tok)
                i += 1

            forwarded = "aoe run " + " ".join(shlex.quote(x) for x in passthrough)
            parsed = parse_cli_message(forwarded)
            if not isinstance(parsed, dict) or parsed.get("cmd") != "run":
                raise RuntimeError("usage: aoe orch run [--orch <name>] [--direct|--dispatch] [--roles <csv>] [--priority P1|P2|P3] [--timeout-sec N] [--no-wait] <prompt>")
            parsed["cmd"] = "orch-run"
            parsed["orch"] = orch_name
            return parsed

        if sub in {"check", "stage", "3step", "3-stage"}:
            orch_name: Optional[str] = None
            request_id: Optional[str] = None
            i = 0
            while i < len(sub_argv):
                tok = sub_argv[i]
                if tok == "--orch":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: aoe orch check [--orch <name>] [<request_or_alias>]")
                    orch_name = sub_argv[i].strip()
                elif tok.startswith("--"):
                    raise RuntimeError(f"unknown option: {tok}")
                else:
                    if request_id is not None:
                        raise RuntimeError("usage: aoe orch check [--orch <name>] [<request_or_alias>]")
                    request_id = tok.strip()
                i += 1
            return {"cmd": "orch-check", "orch": orch_name, "request_id": request_id}

        if sub in {"task", "lifecycle", "life"}:
            orch_name: Optional[str] = None
            request_id: Optional[str] = None
            i = 0
            while i < len(sub_argv):
                tok = sub_argv[i]
                if tok == "--orch":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: aoe orch task [--orch <name>] [<request_or_alias>]")
                    orch_name = sub_argv[i].strip()
                elif tok.startswith("--"):
                    raise RuntimeError(f"unknown option: {tok}")
                else:
                    if request_id is not None:
                        raise RuntimeError("usage: aoe orch task [--orch <name>] [<request_or_alias>]")
                    request_id = tok.strip()
                i += 1
            return {"cmd": "orch-task", "orch": orch_name, "request_id": request_id}

        if sub in {"cancel", "retry", "replan"}:
            orch_name: Optional[str] = None
            request_id: Optional[str] = None
            i = 0
            while i < len(sub_argv):
                tok = sub_argv[i]
                if tok == "--orch":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError(f"usage: aoe orch {sub} [--orch <name>] <request_or_alias>")
                    orch_name = sub_argv[i].strip()
                elif tok.startswith("--"):
                    raise RuntimeError(f"unknown option: {tok}")
                else:
                    if request_id is not None:
                        raise RuntimeError(f"usage: aoe orch {sub} [--orch <name>] <request_or_alias>")
                    request_id = tok.strip()
                i += 1
            if sub != "cancel" and not request_id:
                raise RuntimeError(f"usage: aoe orch {sub} [--orch <name>] <request_or_alias>")
            return {"cmd": f"orch-{sub}", "orch": orch_name, "request_id": request_id}

        if sub in {"monitor", "tasks", "board"}:
            orch_name: Optional[str] = None
            limit: Optional[int] = None
            i = 0
            while i < len(sub_argv):
                tok = sub_argv[i]
                if tok == "--orch":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: aoe orch monitor [--orch <name>] [--limit <n>]")
                    orch_name = sub_argv[i].strip()
                elif tok == "--limit":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: aoe orch monitor [--orch <name>] [--limit <n>]")
                    if not str(sub_argv[i]).isdigit():
                        raise RuntimeError("--limit must be integer")
                    limit = max(1, min(50, int(sub_argv[i])))
                elif tok.startswith("--"):
                    raise RuntimeError(f"unknown option: {tok}")
                else:
                    if not str(tok).isdigit():
                        raise RuntimeError("usage: aoe orch monitor [--orch <name>] [--limit <n>]")
                    limit = max(1, min(50, int(tok)))
                i += 1
            return {"cmd": "orch-monitor", "orch": orch_name, "limit": limit}

        if sub in {"kpi", "metrics"}:
            orch_name: Optional[str] = None
            hours: Optional[int] = None
            i = 0
            while i < len(sub_argv):
                tok = sub_argv[i]
                if tok == "--orch":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: aoe orch kpi [--orch <name>] [--hours <n>]")
                    orch_name = sub_argv[i].strip()
                elif tok == "--hours":
                    i += 1
                    if i >= len(sub_argv):
                        raise RuntimeError("usage: aoe orch kpi [--orch <name>] [--hours <n>]")
                    if not str(sub_argv[i]).isdigit():
                        raise RuntimeError("--hours must be integer")
                    hours = max(1, min(168, int(sub_argv[i])))
                elif tok.startswith("--"):
                    raise RuntimeError(f"unknown option: {tok}")
                else:
                    if not str(tok).isdigit():
                        raise RuntimeError("usage: aoe orch kpi [--orch <name>] [--hours <n>]")
                    hours = max(1, min(168, int(tok)))
                i += 1
            return {"cmd": "orch-kpi", "orch": orch_name, "hours": hours}

        raise RuntimeError("usage: aoe orch <help|list|map|use|pick|add|status|pause|resume|run|check|task|cancel|retry|replan|monitor|kpi>")

    return None
