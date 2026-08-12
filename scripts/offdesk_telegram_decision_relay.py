#!/usr/bin/env python3
"""Relay Offdesk council operator decisions through Telegram.

This helper is intentionally narrow. It only asks the configured operator chat
for a continuation decision after a council result needs human input. It does
not approve mutations, edit Forager state directly, or treat Telegram history as
canonical state.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import hashlib
import html
import json
import os
import pathlib
import re
import secrets
import sys
import time
import urllib.error
import urllib.request
from typing import Any

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from operator_state_card import mobile_card_contract
from telegram_operator.config import default_telegram_env_file


REPO_ROOT = SCRIPT_DIR.parents[0]
DEFAULT_TELEGRAM_ENV_FILE = default_telegram_env_file()
DEFAULT_LIVE_ACTIVE_REQUEST_REGISTRY = pathlib.Path(
    os.environ.get(
        "OFFDESK_TELEGRAM_ACTIVE_REQUEST_REGISTRY",
        str(pathlib.Path.home() / ".cache" / "forager" / "telegram_active_requests.json"),
    )
)
ACTIVE_REQUEST_REGISTRY_LOCK_TIMEOUT_SEC = 5.0
VALID_DECISIONS = ("continue", "revise", "block", "stop")
CUSTOM_DIRECTION_DECISION = "custom_direction"
DECISION_LABELS = {
    "continue": "계속",
    CUSTOM_DIRECTION_DECISION: "기타",
    "revise": "수정",
    "block": "보류",
    "stop": "중단",
    "approve": "승인",
    "deny": "거부",
    "defer": "상세 검토",
    "start_ondesk_review": "WebUI 검토 시작",
    "keep_pending": "대기 유지",
    "defer_ondesk": "나중에 보기",
}
STATUS_LABELS = {
    "awaiting_input": "설명 입력 대기",
    "ambiguous_input": "응답 확인 필요",
    "pending": "의사결정 대기",
    "accepted": "응답 반영",
    "timeout": "응답 시간초과",
    "dry_run": "dry run",
}
DEFAULT_MESSAGE_TYPE = "council_decision"
TEMPLATE_SPECS = {
    "approval_request": {
        "heading": "Forager Approval",
        "natural_input_prompts": {
            "revise": "수정 방향을 자연어로 답장하세요.",
            "block": "보류 이유와 재개 조건을 자연어로 답장하세요.",
        },
    },
    "council_decision": {
        "heading": "Forager Council",
        "natural_input_prompts": {
            "revise": "수정 방향을 자연어로 답장하세요. 예: 검증 범위를 줄이고 council을 다시 실행해.",
            "block": "보류 이유와 재개 조건을 자연어로 답장하세요.",
        },
    },
    "direction_choice": {
        "heading": "Forager Plan Choice",
        "natural_input_prompts": {
            CUSTOM_DIRECTION_DECISION: "원하는 방향을 자연어로 답장하세요.",
        },
    },
    "ondesk_handoff": {
        "heading": "Forager Ondesk Handoff",
        "natural_input_prompts": {
            "defer_ondesk": "언제 다시 볼지 자연어로 답장하세요. 예: 30분 뒤 다시 알려줘.",
        },
    },
}
DEFAULT_ONDESK_HANDOFF_OPTIONS = (
    {
        "id": "start_ondesk_review",
        "label": "WebUI 검토 시작",
        "description": "WebUI에서 closeout, wiki, prompt package를 열어 아침 검토를 시작합니다.",
    },
    {
        "id": "keep_pending",
        "label": "대기 유지",
        "description": "자동 전환하지 않고 pending ondesk review 상태로 유지합니다.",
    },
    {
        "id": "defer_ondesk",
        "label": "나중에",
        "description": "검토를 미루고 재알림 조건을 자연어로 남깁니다.",
        "natural_input_prompt": "언제 다시 볼지 자연어로 답장하세요. 예: 30분 뒤 다시 알려줘.",
    },
)
FIELD_LABELS = {
    "artifacts": "아티팩트",
    "blockers": "막힘",
    "council_decision": "Council 판단",
    "decision": "결정",
    "episode": "Episode",
    "evidence": "근거",
    "next_action": "다음 행동",
    "next_safe_actions": "다음 안전 행동",
    "operator_question": "확인할 점",
    "recommendation": "추천",
    "reason": "이유",
    "risks": "위험",
    "safety_boundary": "안전 경계",
    "status": "상태",
}
FIELD_PRIORITY = (
    "council_decision",
    "decision",
    "recommendation",
    "next_action",
    "next_safe_actions",
    "operator_question",
    "blockers",
    "risks",
    "reason",
    "safety_boundary",
    "status",
    "episode",
)
DETAIL_TEMPLATE = """<b>{headline}</b>
{why_recommendation_section}{judgment_route_section}{evidence_sufficiency_section}{review_surface_section}{failure_section}{evidence_section}{council_section}{next_action_section}{decision_impact_section}{reply_example_section}"""
SECTION_TEMPLATE = """

<b>{title}</b>
{body}"""
DECISION_ALIASES: dict[str, tuple[str, ...]] = {
    "continue": (
        "continue",
        "go",
        "ok",
        "okay",
        "yes",
        "y",
        "좋아",
        "진행",
        "계속",
        "가자",
        "고",
        "오케이",
        "ㅇㅋ",
        "ㅇㅇ",
    ),
    "revise": (
        "revise",
        "retry",
        "redo",
        "수정",
        "보완",
        "다시",
        "고쳐",
        "재검토",
    ),
    "block": (
        "block",
        "hold",
        "보류",
        "차단",
        "막아",
        "대기",
    ),
    "stop": (
        "stop",
        "abort",
        "cancel",
        "중단",
        "멈춰",
        "그만",
        "정지",
        "종료",
        "취소",
    ),
}


class RelayError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=pathlib.Path, required=True, help="Operator-safe decision request JSON.")
    parser.add_argument("--out", type=pathlib.Path, required=True, help="Decision relay result JSON.")
    parser.add_argument("--env-file", type=pathlib.Path, default=DEFAULT_TELEGRAM_ENV_FILE)
    parser.add_argument("--timeout-sec", type=int, default=int(os.environ.get("OFFDESK_TELEGRAM_DECISION_TIMEOUT_SEC", "1800")))
    parser.add_argument("--poll-interval-sec", type=float, default=float(os.environ.get("OFFDESK_TELEGRAM_DECISION_POLL_INTERVAL_SEC", "5")))
    parser.add_argument("--dry-run", action="store_true", help="Render artifacts without sending Telegram messages.")
    parser.add_argument(
        "--keep-reply-keyboard",
        action="store_true",
        help="Do not remove an existing Telegram persistent reply keyboard before sending the decision card.",
    )
    parser.add_argument(
        "--decision-text",
        help="Deterministic test/manual input. Must include the request id and one of: continue, revise, block, stop.",
    )
    parser.add_argument(
        "--active-request-registry",
        type=pathlib.Path,
        help=(
            "Path to the active Telegram decision registry. Live relays default to a shared "
            "Forager cache; dry-run/manual calls default beside --out."
        ),
    )
    return parser.parse_args()


def utc_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write_json(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def load_json(path: pathlib.Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_json_if_exists(value: Any) -> Any:
    raw = str(value or "").strip()
    if not raw:
        return None
    path = pathlib.Path(raw)
    if not path.exists() or not path.is_file():
        return None
    try:
        return load_json(path)
    except (OSError, json.JSONDecodeError):
        return None


def parse_utc_datetime(value: Any) -> dt.datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def resolve_active_request_registry(args: argparse.Namespace) -> pathlib.Path:
    if args.active_request_registry:
        return args.active_request_registry
    if os.environ.get("OFFDESK_TELEGRAM_ACTIVE_REQUEST_REGISTRY"):
        return DEFAULT_LIVE_ACTIVE_REQUEST_REGISTRY
    if args.dry_run or args.decision_text:
        return args.out.with_name("telegram_active_requests.json")
    return DEFAULT_LIVE_ACTIVE_REQUEST_REGISTRY


def active_request_key(state: dict[str, Any]) -> str:
    parts = [
        str(state.get(field) or "").strip()
        for field in ("request_id", "out_path", "state_path")
        if str(state.get(field) or "").strip()
    ]
    raw = "\n".join(parts) if parts else json.dumps(state, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def load_active_registry(path: pathlib.Path) -> dict[str, Any]:
    value = load_json_if_exists(path)
    if isinstance(value, dict):
        entries = value.get("entries")
        if isinstance(entries, list):
            return {**value, "entries": entries}
    return {"schema": "telegram_active_requests.v1", "entries": []}


def prune_active_registry_entries(
    registry: dict[str, Any],
    *,
    now: dt.datetime | None = None,
) -> tuple[list[dict[str, Any]], int]:
    if now is None:
        now = dt.datetime.now(dt.timezone.utc)
    entries = registry.get("entries", [])
    if not isinstance(entries, list):
        return [], 0
    active: list[dict[str, Any]] = []
    pruned = 0
    for entry in entries:
        if not isinstance(entry, dict):
            pruned += 1
            continue
        if entry.get("status") != "pending":
            pruned += 1
            continue
        expires_at = parse_utc_datetime(entry.get("expires_at"))
        if expires_at is not None and expires_at < now:
            pruned += 1
            continue
        active.append(entry)
    return active, pruned


def active_registry_entries(registry: dict[str, Any], *, now: dt.datetime | None = None) -> list[dict[str, Any]]:
    active, _pruned = prune_active_registry_entries(registry, now=now)
    return active


def active_registry_lock_path(path: pathlib.Path) -> pathlib.Path:
    return path.with_name(f"{path.name}.lock")


@contextlib.contextmanager
def locked_active_registry(
    path: pathlib.Path,
    *,
    timeout_sec: float = ACTIVE_REQUEST_REGISTRY_LOCK_TIMEOUT_SEC,
):
    lock_path = active_registry_lock_path(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0.0, timeout_sec)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        while True:
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError as error:
                if time.monotonic() >= deadline:
                    raise RelayError(f"active_request_registry_lock_timeout: {lock_path}") from error
                time.sleep(0.05)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def write_json_atomic(path: pathlib.Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(6)}.tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
    finally:
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass


def write_active_registry(path: pathlib.Path, entries: list[dict[str, Any]], *, stale_removed: int = 0) -> None:
    write_json_atomic(
        path,
        {
            "schema": "telegram_active_requests.v1",
            "updated_at": utc_now(),
            "stale_removed": stale_removed,
            "write_mode": "locked_atomic",
            "entries": entries,
        },
    )


def register_active_request(
    path: pathlib.Path,
    *,
    state: dict[str, Any],
    request_id: str,
    message_type: str,
    target_chat_id_hash: str,
    timeout_sec: int,
) -> dict[str, Any]:
    with locked_active_registry(path):
        now = dt.datetime.now(dt.timezone.utc)
        registry = load_active_registry(path)
        active, stale_removed = prune_active_registry_entries(registry, now=now)
        key = active_request_key(state)
        active = [entry for entry in active if entry.get("key") != key]
        expires_at = now + dt.timedelta(seconds=max(1, timeout_sec))
        entry = {
            "key": key,
            "request_id_hash": hashlib.sha256(request_id.encode("utf-8")).hexdigest()[:16],
            "message_type": message_type,
            "state_path": str(state.get("state_path") or ""),
            "out_path": str(state.get("out_path") or ""),
            "target_chat_id_hash": target_chat_id_hash,
            "status": "pending",
            "created_at": utc_now(),
            "expires_at": expires_at.isoformat(),
        }
        entries = [*active, entry]
        write_active_registry(path, entries, stale_removed=stale_removed)
    return {
        "registry_path": str(path.resolve()),
        "current_key_hash": hashlib.sha256(entry["key"].encode("utf-8")).hexdigest()[:16],
        "active_request_count": len(entries),
        "stale_removed": stale_removed,
        "write_mode": "locked_atomic",
        "free_text_policy": "request_id_or_reply_or_single_active_request",
    }


def complete_active_request(path: pathlib.Path, state: dict[str, Any], *, status: str) -> None:
    with locked_active_registry(path):
        registry = load_active_registry(path)
        key = active_request_key(state)
        active, stale_removed = prune_active_registry_entries(registry)
        entries = [entry for entry in active if entry.get("key") != key]
        if status in {"ambiguous_input", "awaiting_input"}:
            current = next((entry for entry in active if entry.get("key") == key), None)
            if current:
                current = dict(current)
                current["status"] = "pending"
                current["updated_at"] = utc_now()
                entries.append(current)
        write_active_registry(path, entries, stale_removed=stale_removed)


def active_request_count(path: pathlib.Path) -> int:
    return len(active_registry_entries(load_active_registry(path)))


def list_values(value: Any, *, limit: int = 5) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value[:limit] if str(item).strip()]
    if value is None:
        return []
    text = str(value).strip()
    return [text] if text else []


def list_items(value: Any, *, limit: int = 5) -> list[Any]:
    if isinstance(value, list):
        return [item for item in value[:limit] if item not in (None, "")]
    if value in (None, ""):
        return []
    return [value]


def nested_get(value: Any, *keys: str) -> Any:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def parse_env_file(path: pathlib.Path) -> dict[str, str]:
    if not path.exists():
        raise RelayError(f"telegram env file not found: {path}")
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


def resolve_telegram_config(env_file: pathlib.Path) -> dict[str, Any]:
    env = parse_env_file(env_file)
    token = env.get("TELEGRAM_BOT_TOKEN", "").strip()
    owner_chat_id = env.get("TELEGRAM_OWNER_CHAT_ID", "").strip()
    allowed_chat_ids = csv_values(env.get("TELEGRAM_ALLOW_CHAT_IDS", ""))
    target_chat_id = owner_chat_id or (allowed_chat_ids[0] if allowed_chat_ids else "")
    if not token:
        raise RelayError("TELEGRAM_BOT_TOKEN is missing")
    if not target_chat_id:
        raise RelayError("TELEGRAM_OWNER_CHAT_ID or TELEGRAM_ALLOW_CHAT_IDS is required")
    accepted_chat_ids = {target_chat_id}
    if owner_chat_id:
        accepted_chat_ids.add(owner_chat_id)
    return {
        "token": token,
        "target_chat_id": target_chat_id,
        "accepted_chat_ids": accepted_chat_ids,
        "owner_configured": bool(owner_chat_id),
        "allow_list_configured": bool(allowed_chat_ids),
        "env_file": str(env_file),
    }


def chat_hash(chat_id: str) -> str:
    digest = hashlib.sha256(str(chat_id).encode("utf-8")).hexdigest()
    return f"sha256:{digest[:16]}"


def compact(value: Any, max_chars: int) -> str:
    text = json.dumps(value, ensure_ascii=False, indent=2) if not isinstance(value, str) else value
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars)] + "\n...<truncated>"


def label_for_key(key: str) -> str:
    return FIELD_LABELS.get(key, key.replace("_", " ").strip() or key)


def display_bool(value: Any) -> str:
    if value is True:
        return "예"
    if value is False:
        return "아니오"
    return str(value)


def display_decision(value: Any) -> str:
    decision = str(value or "").strip()
    labels = {
        "continue": "계속",
        "revise": "수정",
        "block": "보류",
        "stop": "중단",
        "approve": "승인",
        "deny": "거부",
        "defer": "상세 검토",
        "needs_council_execution": "Council 실행 필요",
        CUSTOM_DIRECTION_DECISION: "기타 방향",
        "start_ondesk_review": "WebUI 검토 시작",
        "keep_pending": "대기 유지",
        "defer_ondesk": "나중에 보기",
    }
    return labels.get(decision, decision)


def display_failure_category(value: Any) -> str:
    category = str(value or "").strip()
    labels = {
        "pass": "통과",
        "contract_anchor_failure": "필수 기준 누락",
        "json_contract_failure": "JSON 계약 불일치",
        "format_failure": "형식 오류",
        "safety_failure": "안전 경계 위반",
        "pass_with_canonicalization": "표현 보정 후 통과",
    }
    return labels.get(category, category)


def display_case_name(value: Any) -> str:
    case = str(value or "").strip()
    labels = {
        "research_reportability_status_json": "보고 가능성 상태 점검",
        "evidence_collection_current_state_json": "현재 근거 상태 점검",
        "critique_open_explore_direction_change": "open-explore 방향 변경 비판",
        "module03_root_entrypoint": "Module 03 진입점 확인",
    }
    return labels.get(case, case.replace("_", " "))


def display_gap(value: Any) -> str:
    gap = str(value or "").strip()
    labels = {
        "missing_contract_anchor": "필수 기준을 뒷받침하는 근거가 부족함",
    }
    return labels.get(gap, gap)


def display_evidence_line(value: Any) -> str:
    text = str(value or "").strip()
    labels = {
        "direction_review_checks.nooption_primary_validated_gate_pass = false": (
            "no-option primary validated gate가 실패했습니다."
        ),
        "direction_review_checks.nooption_restart_validated_rate_gate_pass = false": (
            "no-option restart validated rate gate가 실패했습니다."
        ),
        "direction_review_checks.nooption_q_gate_pass = false (in some paired runs)": (
            "일부 paired run에서 no-option q gate가 실패했습니다."
        ),
        "no-option evidence fails primary objective gate despite execution": (
            "no-option 근거는 실행됐지만 primary objective gate를 통과하지 못했습니다."
        ),
        "primary objective gate failed despite execution": (
            "primary objective gate는 실행 후에도 통과하지 못했습니다."
        ),
    }
    return labels.get(text, text)


def display_next_action(value: Any) -> str:
    if isinstance(value, dict):
        return display_next_safe_action(value)
    text = str(value or "").strip()
    labels = {
        "diagnose why no-option primary objective gate fails": (
            "no-option primary objective gate 실패 원인을 진단합니다."
        ),
        "diagnose primary objective gate failure": (
            "primary objective gate 실패 원인을 진단합니다."
        ),
        "preserve no-option/singlex paired comparison until gate passes": (
            "gate 통과 전까지 no-option/singlex paired 비교를 유지합니다."
        ),
        "do not promote evidence to reportable claim": (
            "현재 근거를 reportable claim으로 승격하지 않습니다."
        ),
    }
    return labels.get(text, text)


def display_next_safe_action(value: Any) -> str:
    if not isinstance(value, dict):
        return display_next_action(value)
    kind = str(value.get("kind") or "next").replace("_", " ").strip()
    detail = str(value.get("detail") or "").strip()
    review = value.get("requires_operator_review")
    suffix = "operator review required" if review is True else "monitoring step" if review is False else ""
    text = f"{kind}: {detail}" if detail else kind
    return f"{text} ({suffix})" if suffix else text


def subject_from_request(request: dict[str, Any], brief: dict[str, Any] | None = None) -> str:
    if isinstance(brief, dict) and str(brief.get("subject") or "").strip():
        return str(brief.get("subject") or "").strip()
    context = brief.get("context") if isinstance(brief, dict) and isinstance(brief.get("context"), dict) else {}
    case = context.get("case") if isinstance(context, dict) else None
    return display_case_name(case or request.get("title") or "승인 요청")


def primary_reason_from_failure(failure: Any, fallback: str = "") -> str:
    if isinstance(failure, dict):
        missing = list_values(failure.get("missing"), limit=2)
        if missing:
            return f"{', '.join(missing)} 미통과"
        if failure.get("category"):
            return display_failure_category(failure.get("category"))
    return fallback.strip()


def decision_impacts_for_request(request: dict[str, Any]) -> dict[str, str]:
    if is_direction_choice(request):
        impacts = {
            option["id"]: option.get("description") or "이 방향으로 다음 작업 기준을 고정합니다."
            for option in direction_options(request)
        }
        if allow_custom_direction(request):
            impacts[CUSTOM_DIRECTION_DECISION] = "버튼 선택 후 원하는 방향을 자연어로 직접 지정합니다."
        return impacts
    return {
        "continue": "현재 경고를 감수하고 다음 episode로 진행합니다.",
        "revise": "자연어로 수정 방향을 남기고 다음 episode를 그 방향으로 진행합니다.",
        "block": "지금은 멈추고 재개 조건이나 추가 확인이 필요하다고 기록합니다.",
        "stop": "이 런을 닫고 closeout 또는 별도 검토로 전환합니다.",
    }


def canonical_recommendation_for_request(request: dict[str, Any], recommendation: Any) -> str:
    value = str(recommendation or "").strip()
    if not value:
        return ""
    if is_direction_choice(request):
        option_ids = {option["id"] for option in direction_options(request)}
        return value if value in option_ids else ""
    return value


def default_reply_examples(request: dict[str, Any]) -> dict[str, str]:
    examples: dict[str, str] = {}
    decisions = set(natural_input_decisions(request))
    if "revise" in decisions:
        examples["revise"] = "primary gate 실패 원인을 먼저 진단하고 reportable claim 승격은 금지해."
    if "block" in decisions:
        examples["block"] = "primary gate 원인 분석 전까지 멈추고 재개 조건을 다시 정리해."
    if CUSTOM_DIRECTION_DECISION in decisions:
        examples[CUSTOM_DIRECTION_DECISION] = "안정화를 먼저 하고 템플릿 확장은 문서만 남겨."
    return examples


def default_if_no_reply_for_request(request: dict[str, Any]) -> str:
    record = request.get("decision_record")
    if not isinstance(record, dict):
        return ""
    for key in ("judgment_route", "route"):
        route = record.get(key)
        if isinstance(route, dict):
            value = str(route.get("default_if_no_reply") or "").strip()
            if value:
                return value
    return ""


def evidence_sufficiency_for_brief(brief: dict[str, Any]) -> str:
    evidence_count = len(list_values(brief.get("evidence"), limit=20))
    next_safe_count = len(list_items(brief.get("next_safe_actions"), limit=20))
    if evidence_count:
        return f"핵심 근거 {evidence_count}건이 요약되어 있고, 생략된 원천은 decision/review evidence refs에서 회수합니다."
    if isinstance(brief.get("review_surface"), dict):
        return "review_surface.v1 요약이 포함되어 있어 상세 검토 표면에서 원천 상태를 회수합니다."
    if next_safe_count:
        return f"근거 요약은 제한적이지만 next_safe_action {next_safe_count}건이 검토 순서를 제공합니다."
    return "근거 요약이 부족합니다. 더 자세히 보기나 review surface에서 원천 상태를 확인해야 합니다."


def normalize_approval_brief(request: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    council = raw.get("council") if isinstance(raw.get("council"), dict) else {}
    context = raw.get("context") if isinstance(raw.get("context"), dict) else {}
    raw_recommendation = str(raw.get("recommendation") or council.get("recommendation") or "").strip()
    recommendation = canonical_recommendation_for_request(request, raw_recommendation)
    failure = raw.get("failure") if isinstance(raw.get("failure"), dict) else {}
    primary_reason = str(raw.get("primary_reason") or "").strip()
    if not primary_reason and raw_recommendation and not recommendation:
        primary_reason = raw_recommendation
    if not primary_reason:
        primary_reason = primary_reason_from_failure(failure, str(raw.get("why_it_matters") or ""))
    evidence = raw.get("evidence")
    if evidence is None:
        evidence = raw.get("key_evidence")
    next_action = raw.get("next_action")
    if next_action is None:
        next_action = raw.get("next_actions")
    next_safe_actions = raw.get("next_safe_actions")
    if next_safe_actions is None:
        next_safe_actions = raw.get("next_safe_action")
    normalized = dict(raw)
    normalized["schema"] = str(raw.get("schema") or "approval_brief.v1")
    normalized["subject"] = subject_from_request(request, raw)
    normalized["recommendation"] = recommendation
    normalized["primary_reason"] = primary_reason
    normalized["failure"] = failure
    normalized["evidence"] = list_values(evidence, limit=8)
    normalized["key_evidence"] = normalized["evidence"]
    normalized["next_action"] = list_values(next_action, limit=6)
    normalized["next_safe_actions"] = list_items(next_safe_actions, limit=6)
    normalized["context"] = context
    judgment_summary = str(raw.get("judgment_route_summary") or "").strip()
    if not judgment_summary:
        judgment_summary = primary_judgment_route_line(request)
    if judgment_summary:
        normalized["judgment_route_summary"] = judgment_summary
    evidence_sufficiency = str(raw.get("evidence_sufficiency") or "").strip()
    if not evidence_sufficiency:
        evidence_sufficiency = evidence_sufficiency_for_brief(normalized)
    if evidence_sufficiency:
        normalized["evidence_sufficiency"] = evidence_sufficiency
    default_if_no_reply = str(raw.get("default_if_no_reply") or "").strip()
    if not default_if_no_reply:
        default_if_no_reply = default_if_no_reply_for_request(request)
    if default_if_no_reply:
        normalized["default_if_no_reply"] = default_if_no_reply
    normalized["council"] = {
        **council,
        "recommendation": recommendation
        or ("" if is_direction_choice(request) else council.get("recommendation")),
    }
    if "decision_impacts" not in normalized or not isinstance(normalized.get("decision_impacts"), dict):
        normalized["decision_impacts"] = decision_impacts_for_request(request)
    if "reply_examples" not in normalized or not isinstance(normalized.get("reply_examples"), (dict, list)):
        normalized["reply_examples"] = default_reply_examples(request)
    normalized["scope"] = str(
        raw.get("scope")
        or "다음 episode 진행 방식만 승인합니다. 파일 변경, cleanup, provider 변경, wiki 승인은 별도 승인입니다."
    )
    normalized["question"] = str(raw.get("question") or approval_question(request))
    return normalized


def approval_brief_text_fields(brief: dict[str, Any]) -> list[tuple[str, str]]:
    fields: list[tuple[str, str]] = []
    for key in (
        "recommendation",
        "subject",
        "primary_reason",
        "judgment_route_summary",
        "evidence_sufficiency",
        "default_if_no_reply",
        "scope",
        "question",
    ):
        value = str(brief.get(key) or "").strip()
        if value:
            fields.append((key, value))
    for key in ("summary_lines", "why_recommendation", "evidence", "key_evidence", "next_action"):
        for index, value in enumerate(list_values(brief.get(key), limit=12)):
            fields.append((f"{key}[{index}]", value))
    for index, value in enumerate(list_items(brief.get("next_safe_actions"), limit=12)):
        fields.append((f"next_safe_actions[{index}]", display_next_safe_action(value)))
    options = brief.get("options")
    if isinstance(options, list):
        for index, option in enumerate(options):
            if not isinstance(option, dict):
                continue
            for key in ("id", "label", "description", "natural_input_prompt", "prompt"):
                value = str(option.get(key) or "").strip()
                if value:
                    fields.append((f"options[{index}].{key}", value))
    impacts = brief.get("decision_impacts")
    if isinstance(impacts, dict):
        for key, value in impacts.items():
            text = str(value or "").strip()
            if text:
                fields.append((f"decision_impacts.{key}", text))
    examples = brief.get("reply_examples")
    if isinstance(examples, dict):
        for key, value in examples.items():
            text = str(value or "").strip()
            if text:
                fields.append((f"reply_examples.{key}", text))
    elif isinstance(examples, list):
        for index, value in enumerate(list_values(examples, limit=12)):
            fields.append((f"reply_examples[{index}]", value))
    return fields


def forbidden_artifact_basenames(request: dict[str, Any]) -> set[str]:
    artifacts = request.get("artifacts")
    if not isinstance(artifacts, dict):
        return set()
    basenames: set[str] = set()
    for value in artifacts.values():
        if not isinstance(value, str):
            continue
        if "/" not in value and "\\" not in value:
            continue
        name = pathlib.Path(value).name.strip()
        if name:
            basenames.add(name)
    return basenames


def unsafe_user_text_failures(request: dict[str, Any], brief: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    request_id = request_id_for(request, "")
    artifact_basenames = forbidden_artifact_basenames(request)
    trace_keys = (
        "decision_request_id",
        "request_id",
        "state_path",
        "callback_data",
        "raw_text_preview",
        "telegram_decision_state",
    )
    for field, text in approval_brief_text_fields(brief):
        lowered = text.lower()
        if "<pre>" in lowered:
            failures.append(f"{field}:raw_json_or_pre_dump")
        if re.search(r"(^|[\s'\"(])(/home/|/tmp/|/var/|/Users/|[A-Za-z]:\\)", text):
            failures.append(f"{field}:raw_path")
        if re.search(r"\b[A-Za-z0-9_./-]+\.json\b", text) and any(name in text for name in artifact_basenames):
            failures.append(f"{field}:artifact_filename")
        if request_id and request_id in text:
            failures.append(f"{field}:request_id_leak")
        if any(key in text for key in trace_keys):
            failures.append(f"{field}:trace_key_leak")
        if re.search(r"(TELEGRAM_BOT_TOKEN|sk-[A-Za-z0-9_-]{8,}|fake-token)", text, re.IGNORECASE):
            failures.append(f"{field}:secret_like")
    return failures


def approval_scope_has_boundary(scope: str) -> bool:
    lowered = scope.lower()
    return any(
        marker in lowered
        for marker in (
            "does not",
            "not approve",
            "not authorize",
            "별도 승인",
            "승인하지",
            "승인되지",
        )
    )


def validate_approval_brief(request: dict[str, Any], *, explicit: bool) -> dict[str, Any]:
    brief = brief_for(request)
    failures: list[str] = []
    warnings: list[str] = []
    if not brief:
        failures.append("approval_brief:missing")
        return {
            "schema": None,
            "mode": "explicit" if explicit else "inferred",
            "valid": False,
            "failures": failures if explicit else [],
            "warnings": [] if explicit else failures,
        }

    schema = str(brief.get("schema") or "").strip()
    message_type = message_type_for(request)
    allowed_schemas = {"approval_brief.v1"}
    if message_type == "ondesk_handoff":
        allowed_schemas.add("ondesk_handoff_brief.v1")
    if schema not in allowed_schemas:
        failures.append(f"schema:unsupported:{schema or 'missing'}")

    required_text_fields = ("recommendation", "subject", "scope", "question")
    for field in required_text_fields:
        if not str(brief.get(field) or "").strip():
            failures.append(f"{field}:missing")

    summary_lines = brief.get("summary_lines")
    if not isinstance(summary_lines, list) or not list_values(summary_lines, limit=5):
        failures.append("summary_lines:missing")
    elif len(list_values(summary_lines, limit=8)) > 3:
        warnings.append("summary_lines:too_many_for_compact_card")

    scope = str(brief.get("scope") or "").strip()
    if scope and not approval_scope_has_boundary(scope):
        failures.append("scope:missing_non_authorized_boundary")

    recommendation = str(brief.get("recommendation") or "").strip()
    if recommendation:
        available_decisions = {action["decision"] for action in action_specs_for(request)}
        if recommendation not in available_decisions:
            failures.append(f"recommendation:not_available_action:{recommendation}")

    if judgment_route_for(request) and not str(brief.get("judgment_route_summary") or "").strip():
        warnings.append("judgment_route_summary:missing")
    if not str(brief.get("evidence_sufficiency") or "").strip():
        warnings.append("evidence_sufficiency:missing")
    default_if_no_reply = default_if_no_reply_for_request(request)
    if default_if_no_reply and not str(brief.get("default_if_no_reply") or "").strip():
        warnings.append("default_if_no_reply:missing")
    impacts = brief.get("decision_impacts")
    if not isinstance(impacts, dict) or not impacts:
        warnings.append("decision_impacts:missing")

    options = brief.get("options")
    if options is not None:
        if not isinstance(options, list) or not options:
            failures.append("options:not_non_empty_list")
        elif isinstance(options, list):
            for index, option in enumerate(options):
                if not isinstance(option, dict):
                    failures.append(f"options[{index}]:not_object")
                    continue
                for field in ("id", "label", "description"):
                    if not str(option.get(field) or "").strip():
                        failures.append(f"options[{index}].{field}:missing")

    failures.extend(unsafe_user_text_failures(request, brief))
    return {
        "schema": schema or None,
        "mode": "explicit" if explicit else "inferred",
        "valid": not failures,
        "failures": failures if explicit else [],
        "warnings": warnings if explicit else [*failures, *warnings],
    }


def approval_brief_from_operator_brief(request: dict[str, Any], operator_brief: dict[str, Any]) -> dict[str, Any]:
    council = operator_brief.get("council") if isinstance(operator_brief.get("council"), dict) else {}
    context = operator_brief.get("context") if isinstance(operator_brief.get("context"), dict) else {}
    recommendation = str(council.get("recommendation") or "").strip()
    failure = operator_brief.get("failure") if isinstance(operator_brief.get("failure"), dict) else {}
    claim_status = str(context.get("claim_status") or "").strip()
    summary_lines: list[str] = []
    if claim_status == "pending_not_reportable":
        summary_lines.append("현재 결과는 reportable claim으로 승격할 수 없습니다.")
    primary_reason = primary_reason_from_failure(failure, str(operator_brief.get("why_it_matters") or ""))
    if primary_reason:
        summary_lines.append(f"이유: {primary_reason}.")
    if recommendation:
        agreement = council.get("agreement")
        agreement_text = "리뷰어 합의" if agreement is True else "리뷰어 합의 없음" if agreement is False else "합의 정보 없음"
        summary_lines.append(f"Council: {display_decision(recommendation)} 권고, {agreement_text}.")
    why_recommendation: list[str] = []
    if claim_status == "pending_not_reportable":
        why_recommendation.append("실행은 됐지만 승격 기준을 통과하지 못했습니다.")
    if recommendation == "revise":
        why_recommendation.append("지금 계속하면 non-reportable 상태를 반복할 가능성이 큽니다.")
    elif recommendation == "block":
        why_recommendation.append("재개 조건 없이 진행하면 같은 blocker를 반복할 가능성이 큽니다.")
    elif recommendation == "stop":
        why_recommendation.append("다음 episode보다 closeout과 별도 검토가 더 적합합니다.")
    return normalize_approval_brief(
        request,
        {
            "schema": "approval_brief.v1",
            "subject": subject_from_request(request, operator_brief),
            "recommendation": recommendation,
            "primary_reason": primary_reason,
            "summary_lines": summary_lines,
            "why_recommendation": why_recommendation,
            "failure": failure,
            "evidence": operator_brief.get("key_evidence"),
            "next_action": operator_brief.get("next_action"),
            "council": council,
            "context": context,
            "source": "operator_brief",
        },
    )


def brief_from_artifacts(request: dict[str, Any]) -> dict[str, Any] | None:
    artifacts = request.get("artifacts")
    if not isinstance(artifacts, dict):
        return None
    episode = load_json_if_exists(artifacts.get("episode_record"))
    council = load_json_if_exists(artifacts.get("council"))
    if not isinstance(episode, dict) and not isinstance(council, dict):
        return None
    summary = request.get("summary") if isinstance(request.get("summary"), dict) else {}
    consensus = council.get("consensus", {}) if isinstance(council, dict) else {}
    if not isinstance(consensus, dict):
        consensus = {}
    reviews = council.get("reviews", []) if isinstance(council, dict) else []
    if not isinstance(reviews, list):
        reviews = []
    iteration = episode.get("iteration") if isinstance(episode, dict) else summary.get("iteration")
    case = episode.get("case") if isinstance(episode, dict) else summary.get("case")
    failure_category = episode.get("failure_category") if isinstance(episode, dict) else None
    missing = list_values(episode.get("must_missing") if isinstance(episode, dict) else None, limit=4)
    episode_json = episode.get("json", {}) if isinstance(episode, dict) else {}
    if not isinstance(episode_json, dict):
        episode_json = {}
    blocking_evidence = list_values(episode_json.get("blocking_evidence"), limit=4)
    key_evidence = blocking_evidence or list_values(episode_json.get("evidence_available"), limit=4)
    evidence_gaps = list_values(consensus.get("evidence_gaps"), limit=4)
    next_actions = list_values(episode_json.get("next_action"), limit=4)
    if not next_actions:
        candidates = consensus.get("next_episode_candidates")
        if isinstance(candidates, list):
            next_actions = [
                str(candidate.get("objective") or "").strip()
                for candidate in candidates[:3]
                if isinstance(candidate, dict) and str(candidate.get("objective") or "").strip()
            ]
    reviewer_decisions = consensus.get("reviewer_decisions") or summary.get("reviewer_decisions") or {}
    recommendation = str(consensus.get("decision") or summary.get("council_decision") or "needs_review").strip()
    headline_bits = []
    if iteration is not None:
        headline_bits.append(f"Episode {iteration}")
    if case:
        headline_bits.append(display_case_name(case))
    headline_prefix = " / ".join(headline_bits) or str(request.get("title") or "Council decision")
    if failure_category and failure_category != "pass":
        headline = f"{headline_prefix}: {display_failure_category(failure_category)}"
    else:
        headline = f"{headline_prefix}: Council {recommendation}"
    why = ""
    claim_status = episode_json.get("claim_status")
    baseline_status = episode_json.get("baseline_evidence_status")
    if missing:
        why = f"{', '.join(missing)} 항목이 충족되지 않아 Council 판단이 필요합니다."
    elif claim_status or baseline_status:
        why = f"현재 상태는 {claim_status or baseline_status}이며 다음 episode 진행 판단이 필요합니다."
    elif recommendation and recommendation != "continue":
        why = f"Council이 {recommendation}를 제안했습니다."
    return {
        "headline": headline,
        "why_it_matters": why,
        "failure": {
            "passed": episode.get("passed") if isinstance(episode, dict) else None,
            "category": failure_category,
            "missing": missing,
        },
        "council": {
            "recommendation": recommendation,
            "agreement": consensus.get("agreement"),
            "reviewer_decisions": reviewer_decisions,
            "evidence_gaps": evidence_gaps,
        },
        "key_evidence": key_evidence,
        "next_action": next_actions,
        "context": {
            "iteration": iteration,
            "case": case,
            "baseline_evidence_status": baseline_status,
            "claim_status": claim_status,
        },
    }


def request_with_operator_brief(request: dict[str, Any]) -> dict[str, Any]:
    if isinstance(request.get("operator_brief"), dict):
        return request
    brief = brief_from_artifacts(request)
    if not brief:
        return request
    enriched = dict(request)
    enriched["operator_brief"] = brief
    return enriched


def request_with_approval_brief(request: dict[str, Any]) -> dict[str, Any]:
    enriched = request_with_operator_brief(request)
    if isinstance(enriched.get("approval_brief"), dict):
        normalized = dict(enriched)
        normalized["approval_brief"] = normalize_approval_brief(enriched, enriched["approval_brief"])
        return normalized
    operator_brief = enriched.get("operator_brief")
    if isinstance(operator_brief, dict):
        normalized = dict(enriched)
        normalized["approval_brief"] = approval_brief_from_operator_brief(enriched, operator_brief)
        return normalized
    summary = enriched.get("summary")
    if isinstance(summary, dict):
        raw = {
            "schema": "approval_brief.v1",
            "subject": display_case_name(summary.get("case") or enriched.get("title") or "승인 요청"),
            "recommendation": summary.get("recommendation") or summary.get("council_decision") or summary.get("decision") or "",
            "primary_reason": summary.get("reason") or summary.get("operator_question") or "",
            "summary_lines": [
                str(value).strip()
                for value in (
                    summary.get("operator_question"),
                    summary.get("reason"),
                    summary.get("safety_boundary"),
                )
                if str(value or "").strip()
            ],
            "council": {
                "recommendation": summary.get("council_decision") or summary.get("decision") or "",
                "agreement": summary.get("agreement"),
                "reviewer_decisions": summary.get("reviewer_decisions", {}),
            },
            "source": "summary",
        }
        normalized = dict(enriched)
        normalized["approval_brief"] = normalize_approval_brief(enriched, raw)
        return normalized
    return enriched


def brief_for(request: dict[str, Any]) -> dict[str, Any]:
    brief = request.get("approval_brief")
    if isinstance(brief, dict):
        return brief
    brief = request.get("operator_brief")
    return brief if isinstance(brief, dict) else {}


def decision_record_for(request: dict[str, Any]) -> dict[str, Any]:
    record = request.get("decision_record")
    return record if isinstance(record, dict) else {}


def message_type_for(request: dict[str, Any]) -> str:
    raw = str(request.get("message_type") or request.get("type") or DEFAULT_MESSAGE_TYPE).strip()
    return raw if raw in TEMPLATE_SPECS else DEFAULT_MESSAGE_TYPE


def is_direction_choice(request: dict[str, Any]) -> bool:
    return message_type_for(request) == "direction_choice"


def is_ondesk_handoff(request: dict[str, Any]) -> bool:
    return message_type_for(request) == "ondesk_handoff"


def template_spec_for(request: dict[str, Any]) -> dict[str, Any]:
    return TEMPLATE_SPECS[message_type_for(request)]


def direction_options(request: dict[str, Any]) -> list[dict[str, str]]:
    raw_options = request.get("options")
    if raw_options is None:
        raw_options = request.get("choices")
    if raw_options is None:
        brief = brief_for(request)
        raw_options = brief.get("options")
    if raw_options is None:
        brief = brief_for(request)
        raw_options = brief.get("choices")
    if not isinstance(raw_options, list):
        if is_ondesk_handoff(request):
            raw_options = [dict(option) for option in DEFAULT_ONDESK_HANDOFF_OPTIONS]
        else:
            return []
    options: list[dict[str, str]] = []
    for index, raw in enumerate(raw_options, start=1):
        if isinstance(raw, dict):
            option_id = str(raw.get("id") or raw.get("key") or raw.get("decision") or f"option_{index}").strip()
            label = str(raw.get("label") or raw.get("title") or f"안 {index}").strip()
            description = str(raw.get("description") or raw.get("summary") or raw.get("impact") or "").strip()
            prompt = str(raw.get("natural_input_prompt") or raw.get("prompt") or "").strip()
        else:
            option_id = f"option_{index}"
            label = str(raw).strip() or f"안 {index}"
            description = ""
            prompt = ""
        options.append(
            {
                "id": option_id or f"option_{index}",
                "label": label or f"안 {index}",
                "description": description,
                "prompt": prompt,
                "index": str(index),
            }
        )
    return options


def allow_custom_direction(request: dict[str, Any]) -> bool:
    if not is_direction_choice(request):
        return False
    return request.get("allow_custom", True) is not False


def link_specs_for(request: dict[str, Any]) -> list[dict[str, str]]:
    raw_links: Any = request.get("links")
    if raw_links is None:
        brief = brief_for(request)
        raw_links = brief.get("links")
    if raw_links is None and str(request.get("webui_url") or "").strip():
        raw_links = [{"label": "WebUI 열기", "url": str(request.get("webui_url") or "").strip()}]
    if not isinstance(raw_links, list):
        return []
    links: list[dict[str, str]] = []
    for raw in raw_links[:3]:
        if not isinstance(raw, dict):
            continue
        url = str(raw.get("url") or raw.get("href") or "").strip()
        if not re.match(r"^https?://", url):
            continue
        label = str(raw.get("label") or raw.get("title") or "링크 열기").strip()
        links.append({"label": label[:64] or "링크 열기", "url": url})
    return links


def natural_input_prompts_for(request: dict[str, Any]) -> dict[str, str]:
    prompts = template_spec_for(request).get("natural_input_prompts", {})
    merged = dict(prompts) if isinstance(prompts, dict) else {}
    for option in direction_options(request):
        if option.get("prompt"):
            merged[option["id"]] = option["prompt"]
    return merged


def natural_input_prompt(request: dict[str, Any], decision: str) -> str:
    return str(natural_input_prompts_for(request).get(decision) or "").strip()


def natural_input_decisions(request: dict[str, Any]) -> list[str]:
    return [action["decision"] for action in action_specs_for(request) if action.get("natural_input_prompt")]


def render_input_policy(request: dict[str, Any]) -> str:
    labels = [action["label"] for action in action_specs_for(request) if action.get("natural_input_prompt")]
    if not labels:
        return ""
    return f"\n<code>{html.escape('/'.join(labels))}</code>는 버튼 선택 후 설명 답장이 필요합니다."


def render_reply_hint(request: dict[str, Any]) -> str:
    options = direction_options(request)
    if options:
        hints = [option["index"] for option in direction_options(request)[:3]]
        if allow_custom_direction(request):
            hints.append("기타")
        if hints:
            return f"답장도 가능합니다: <code>{html.escape(' / '.join(hints))}</code>"
    return "답장도 가능합니다: <code>좋아</code>, <code>수정</code>, <code>보류</code>, <code>중단</code>"


def render_choices_section(request: dict[str, Any]) -> str:
    options = direction_options(request)
    if not options:
        return ""
    lines = []
    for option in options[:5]:
        line = f"- {option['index']}. {html.escape(option['label'])}"
        if option.get("description"):
            line = f"{line}: {html.escape(compact(option['description'], 220))}"
        lines.append(line)
    if len(options) > 5:
        lines.append(f"- 외 {len(options) - 5}개 선택지는 내부 로그에 보존")
    return SECTION_TEMPLATE.format(title="선택지", body="\n".join(lines)) + "\n"


def action_specs_for(request: dict[str, Any]) -> list[dict[str, str]]:
    options = direction_options(request)
    if options:
        actions: list[dict[str, str]] = [
            {
                "key": f"o{index}",
                "decision": option["id"],
                "label": f"{option['index']}. {option['label']}",
                "natural_input_prompt": natural_input_prompt(request, option["id"]),
            }
            for index, option in enumerate(options, start=1)
        ]
        if allow_custom_direction(request):
            actions.append(
                {
                    "key": "x",
                    "decision": CUSTOM_DIRECTION_DECISION,
                    "label": "기타",
                    "natural_input_prompt": natural_input_prompt(request, CUSTOM_DIRECTION_DECISION),
                }
            )
        return actions
    return [
        {"key": "c", "decision": "continue", "label": "계속", "natural_input_prompt": ""},
        {"key": "r", "decision": "revise", "label": "수정", "natural_input_prompt": natural_input_prompt(request, "revise")},
        {"key": "b", "decision": "block", "label": "보류", "natural_input_prompt": natural_input_prompt(request, "block")},
        {"key": "s", "decision": "stop", "label": "중단", "natural_input_prompt": ""},
    ]


def render_input_prompt(prompt: str) -> str:
    if not prompt:
        return ""
    return SECTION_TEMPLATE.format(
        title="설명 요청",
        body=(
            f"- {html.escape(prompt)}\n"
            "- 다른 결정을 원하면 새 버튼을 누르세요."
        ),
    )


def ordered_items(payload: dict[str, Any]) -> list[tuple[str, Any]]:
    seen: set[str] = set()
    items: list[tuple[str, Any]] = []
    for key in FIELD_PRIORITY:
        if key in payload:
            items.append((key, payload[key]))
            seen.add(key)
    for key, value in payload.items():
        if key not in seen:
            items.append((str(key), value))
    return items


def inline_value(value: Any, *, max_chars: int = 220) -> str:
    if value is None:
        return "없음"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return compact(" ".join(value.split()), max_chars)
    if isinstance(value, list):
        simple_items = [inline_value(item, max_chars=80) for item in value[:4]]
        suffix = f" 외 {len(value) - 4}개" if len(value) > 4 else ""
        return compact("; ".join(simple_items) + suffix, max_chars)
    if isinstance(value, dict):
        parts = [
            f"{label_for_key(str(key))}: {inline_value(item, max_chars=80)}"
            for key, item in ordered_items(value)[:4]
        ]
        suffix = f"; 외 {len(value) - 4}개" if len(value) > 4 else ""
        return compact("; ".join(parts) + suffix, max_chars)
    return compact(str(value), max_chars)


def human_payload_lines(payload: Any, *, max_items: int, max_chars: int) -> list[str]:
    if isinstance(payload, dict):
        items = ordered_items(payload)[:max_items]
        lines = [
            f"- {label_for_key(str(key))}: {html.escape(inline_value(value, max_chars=max_chars))}"
            for key, value in items
        ]
        if len(payload) > max_items:
            lines.append(f"- 외 {len(payload) - max_items}개 항목은 내부 로그에 보존")
        return lines
    if isinstance(payload, list):
        lines = [f"- {html.escape(inline_value(item, max_chars=max_chars))}" for item in payload[:max_items]]
        if len(payload) > max_items:
            lines.append(f"- 외 {len(payload) - max_items}개 항목은 내부 로그에 보존")
        return lines
    return [f"- {html.escape(inline_value(payload, max_chars=max_chars))}"]


def render_payload_section(title: str, payload: Any, *, max_items: int, max_chars: int) -> str:
    if payload is None:
        return ""
    lines = human_payload_lines(payload, max_items=max_items, max_chars=max_chars)
    if not lines:
        return ""
    return SECTION_TEMPLATE.format(title=html.escape(title), body="\n".join(lines))


def render_lines_section(title: str, lines: list[str], *, max_items: int = 5, max_chars: int = 220) -> str:
    cleaned = [compact(str(line).strip(), max_chars) for line in lines if str(line).strip()]
    if not cleaned:
        return ""
    return SECTION_TEMPLATE.format(
        title=html.escape(title),
        body="\n".join(f"- {html.escape(line)}" for line in cleaned[:max_items]),
    )


def render_blockquote(lines: list[str], *, expandable: bool = False, max_items: int = 5, max_chars: int = 280) -> str:
    cleaned = [compact(str(line).strip(), max_chars) for line in lines if str(line).strip()]
    if not cleaned:
        return ""
    tag = "blockquote expandable" if expandable else "blockquote"
    body = "\n".join(html.escape(line) for line in cleaned[:max_items])
    return f"<{tag}>{body}</blockquote>"


def render_quote_section(
    title: str,
    lines: list[str],
    *,
    expandable: bool = False,
    max_items: int = 5,
    max_chars: int = 280,
) -> str:
    quote = render_blockquote(lines, expandable=expandable, max_items=max_items, max_chars=max_chars)
    if not quote:
        return ""
    return SECTION_TEMPLATE.format(title=html.escape(title), body=quote)


def render_why_section(brief: dict[str, Any]) -> str:
    why = str(brief.get("why_it_matters") or "").strip()
    return render_lines_section("왜 중요한가", [why], max_items=1, max_chars=320)


def render_failure_section(brief: dict[str, Any]) -> str:
    failure = brief.get("failure")
    if not isinstance(failure, dict):
        return ""
    lines: list[str] = []
    if failure.get("passed") is not None:
        lines.append(f"결과: {'통과' if failure.get('passed') else '실패'}")
    if failure.get("category"):
        lines.append(f"분류: {display_failure_category(failure.get('category'))}")
    missing = list_values(failure.get("missing"), limit=5)
    if missing:
        lines.append(f"누락 기준: {', '.join(missing)}")
    return render_lines_section("실패 요약", lines, max_items=5, max_chars=260)


def render_evidence_section(brief: dict[str, Any], *, detailed: bool = False) -> str:
    evidence = list_values(brief.get("key_evidence"), limit=6 if detailed else 4)
    rendered = [display_evidence_line(item) for item in evidence]
    if detailed:
        return render_quote_section(
            "핵심 근거",
            rendered,
            expandable=len(rendered) > 3,
            max_items=6,
            max_chars=300,
        )
    return render_lines_section(
        "핵심 근거",
        rendered,
        max_items=4,
        max_chars=300,
    )


def render_council_section(request: dict[str, Any], brief: dict[str, Any], *, detailed: bool = False) -> str:
    record = decision_record_for(request)
    if record and not isinstance(record.get("council_review"), dict):
        return ""
    council = brief.get("council")
    if not isinstance(council, dict):
        return ""
    lines: list[str] = []
    if council.get("recommendation"):
        lines.append(f"추천: {display_decision(council.get('recommendation'))}")
    if council.get("agreement") is not None:
        lines.append(f"합의: {display_bool(council.get('agreement'))}")
    reviewer_decisions = council.get("reviewer_decisions")
    if isinstance(reviewer_decisions, dict) and reviewer_decisions:
        lines.append(
            "리뷰어: "
            + ", ".join(f"{key}={display_decision(value)}" for key, value in reviewer_decisions.items())
        )
    if detailed:
        gaps = list_values(council.get("evidence_gaps"), limit=5)
        if gaps:
            lines.append(f"부족한 근거: {', '.join(display_gap(gap) for gap in gaps)}")
    return render_lines_section("Council 판단", lines, max_items=5, max_chars=280)


def render_next_action_section(brief: dict[str, Any], *, detailed: bool = False) -> str:
    actions = [display_next_action(item) for item in list_values(brief.get("next_action"), limit=5 if detailed else 3)]
    safe_actions = [
        display_next_safe_action(item)
        for item in list_items(brief.get("next_safe_actions"), limit=5 if detailed else 3)
    ]
    return render_lines_section(
        "권장 다음 행동",
        [*actions, *safe_actions],
        max_items=5 if detailed else 3,
        max_chars=300,
    )


def render_recurrence_section(brief: dict[str, Any]) -> str:
    recurrence = brief.get("recurrence")
    if not recurrence:
        return ""
    if isinstance(recurrence, dict):
        lines = human_payload_lines(recurrence, max_items=4, max_chars=220)
        return SECTION_TEMPLATE.format(title="반복 신호", body="\n".join(lines))
    return render_lines_section("반복 신호", [str(recurrence)], max_items=1, max_chars=260)


def brief_recommendation(request: dict[str, Any]) -> str:
    brief = brief_for(request)
    council = brief.get("council")
    if isinstance(council, dict) and council.get("recommendation"):
        return str(council.get("recommendation") or "").strip()
    summary = request.get("summary")
    if isinstance(summary, dict):
        return str(summary.get("council_decision") or summary.get("decision") or "").strip()
    return ""


def recommendation_headline(request: dict[str, Any]) -> str:
    brief = brief_for(request)
    case = subject_from_request(request, brief)
    recommendation = brief_recommendation(request)
    if is_ondesk_handoff(request):
        return f"Ondesk 전환 브리핑: {case}"
    if is_direction_choice(request):
        return f"방향 선택: {case}"
    if recommendation:
        return f"{display_decision(recommendation)} 권고: {case}"
    return f"승인 요청: {case}"


def approval_summary_lines(request: dict[str, Any]) -> list[str]:
    brief = brief_for(request)
    explicit_lines = list_values(brief.get("summary_lines"), limit=3)
    judgment_line = primary_judgment_route_line(request)
    if explicit_lines:
        return ([judgment_line] if judgment_line else []) + explicit_lines[:3]
    lines: list[str] = []
    if judgment_line:
        lines.append(judgment_line)
    context = brief.get("context") if isinstance(brief.get("context"), dict) else {}
    claim_status = str(context.get("claim_status") or "").strip()
    if claim_status == "pending_not_reportable":
        lines.append("현재 결과는 reportable claim으로 승격할 수 없습니다.")
    why = str(brief.get("why_it_matters") or "").strip()
    failure = brief.get("failure")
    if isinstance(failure, dict):
        missing = list_values(failure.get("missing"), limit=2)
        if missing:
            lines.append(f"이유: {', '.join(missing)} 미통과.")
        elif failure.get("category"):
            lines.append(f"이유: {display_failure_category(failure.get('category'))}.")
    elif str(brief.get("primary_reason") or "").strip():
        lines.append(f"이유: {brief.get('primary_reason')}.")
    if not lines and why:
        lines.append(why)
    council = brief.get("council")
    if isinstance(council, dict):
        recommendation = council.get("recommendation")
        agreement = council.get("agreement")
        if recommendation:
            agreement_text = "리뷰어 합의" if agreement is True else "리뷰어 합의 없음" if agreement is False else "합의 정보 없음"
            lines.append(f"Council: {display_decision(recommendation)} 권고, {agreement_text}.")
    if not lines:
        summary = request.get("summary")
        if isinstance(summary, dict):
            recommendation = summary.get("recommendation") or summary.get("council_decision") or summary.get("decision")
            reason = summary.get("reason") or summary.get("operator_question") or summary.get("next_action")
            if recommendation:
                lines.append(f"추천: {inline_value(recommendation, max_chars=160)}")
            if reason:
                lines.append(f"근거: {inline_value(reason, max_chars=220)}")
    return lines[:3]


def render_approval_summary(request: dict[str, Any]) -> str:
    lines = approval_summary_lines(request)
    if not lines:
        return ""
    return render_blockquote(lines, max_items=4, max_chars=320) + "\n"


def render_status_section(
    request: dict[str, Any],
    *,
    status: str,
    decision: str | None,
    reason: str,
) -> str:
    if status == "pending" and not decision and not reason:
        return ""
    status_line = STATUS_LABELS.get(status, status)
    if decision:
        status_line = f"{status_line}: {display_decision(decision)}"
    if reason:
        reason_label = "버튼" if reason == "button" else reason[:160]
        status_line = f"{status_line} ({reason_label})"
    return f"<b>상태</b>: <code>{html.escape(status_line)}</code>\n"


def compact_card_text(value: Any, max_chars: int) -> str:
    text = str(value or "").replace("\n", " ")
    text = " ".join(text.split())
    if len(text) <= max_chars:
        return text
    return text[: max(1, max_chars - 3)].rstrip() + "..."


def compact_status_text(status: str, decision: str | None, reason: str) -> str:
    if status == "pending" and not decision and not reason:
        return ""
    status_line = STATUS_LABELS.get(status, status)
    if decision:
        status_line = f"{status_line}: {display_decision(decision)}"
    if reason:
        reason_label = "버튼" if reason == "button" else compact_card_text(reason, 80)
        status_line = f"{status_line} ({reason_label})"
    return status_line


def compact_choices_line(request: dict[str, Any]) -> str:
    options = direction_options(request)
    if options:
        labels = [f"{option['index']}. {option['label']}" for option in options[:2]]
        if allow_custom_direction(request):
            labels.append("기타")
        elif len(options) > 2:
            labels.append(f"외 {len(options) - 2}개")
        return "선택: " + " / ".join(compact_card_text(label, 24) for label in labels)
    return ""


def compact_scope_text(scope: str) -> str:
    replacements = {
        "runtime dispatch": "실행 시작",
        "dispatch": "실행",
        "shell": "명령 실행",
        "cleanup": "정리",
        "provider": "모델",
        "wiki promotion": "wiki 승격",
    }
    text = scope
    for raw, replacement in replacements.items():
        text = text.replace(raw, replacement)
    return compact_card_text(text, 92)


def compact_next_action_text(request: dict[str, Any], input_prompt: str) -> str:
    if input_prompt:
        return compact_card_text(input_prompt, 92)
    question = approval_question(request)
    if natural_input_decisions(request):
        question = f"{question} 버튼 또는 직접 답장 가능"
    return compact_card_text(question, 92)


def render_compact_decision_card(
    request: dict[str, Any],
    *,
    status: str,
    decision: str | None,
    reason: str,
    input_prompt: str,
) -> str:
    lines = [f"<b>{html.escape(compact_card_text(recommendation_headline(request), 80))}</b>"]
    status_line = compact_status_text(status, decision, reason)
    if status_line:
        lines.append(html.escape(status_line))

    summary_lines = approval_summary_lines(request)
    choices_line = compact_choices_line(request)
    middle_candidates: list[str] = []
    if choices_line and is_direction_choice(request):
        middle_candidates.append(choices_line)
    middle_candidates.extend(summary_lines[:2])
    if choices_line and not is_direction_choice(request):
        middle_candidates.append(choices_line)
    for item in middle_candidates:
        if len(lines) >= 3:
            break
        lines.append(html.escape(compact_card_text(item, 118)))

    lines.append(f"다음 조치: {html.escape(compact_next_action_text(request, input_prompt))}")
    lines.append(f"권한: {html.escape(compact_scope_text(approval_scope(request)))}")
    return "\n".join(lines[:5])


def approval_question(request: dict[str, Any]) -> str:
    brief = brief_for(request)
    if str(brief.get("question") or "").strip():
        return str(brief.get("question") or "").strip()
    if is_ondesk_handoff(request):
        return "WebUI에서 ondesk 검토를 시작할까요?"
    if is_direction_choice(request):
        return "어떤 방향으로 진행할까요?"
    return "어떻게 진행할까요?"


def approval_scope(request: dict[str, Any]) -> str:
    brief = brief_for(request)
    scope = str(brief.get("scope") or "").strip()
    if scope:
        return scope
    if is_ondesk_handoff(request):
        return (
            "Telegram은 알림과 진입 확인만 기록합니다. wiki promotion, delete, "
            "cleanup, provider 변경은 WebUI/CLI에서 별도 승인합니다."
        )
    return "다음 episode 진행 방식만 승인합니다. 파일 변경, cleanup, provider 변경, wiki 승인은 별도 승인입니다."


def recommended_decision_for(request: dict[str, Any]) -> str:
    recommendation = brief_recommendation(request)
    if any(option["id"] == recommendation for option in direction_options(request)):
        return recommendation
    return recommendation if recommendation in VALID_DECISIONS else ""


def action_button_label(request: dict[str, Any], action: dict[str, Any]) -> str:
    label = str(action.get("label") or action.get("decision") or "").strip()
    if action.get("decision") == recommended_decision_for(request):
        return f"{label}(권장)"
    return label


def render_why_recommendation_section(brief: dict[str, Any]) -> str:
    explicit_lines = list_values(brief.get("why_recommendation"), limit=5)
    if explicit_lines:
        return render_lines_section("왜 이 추천인가", explicit_lines, max_items=5, max_chars=300)
    council = brief.get("council")
    recommendation = ""
    if isinstance(council, dict):
        recommendation = str(council.get("recommendation") or "").strip()
    lines: list[str] = []
    context = brief.get("context") if isinstance(brief.get("context"), dict) else {}
    if context.get("claim_status") == "pending_not_reportable":
        lines.append("실행은 됐지만 승격 기준을 통과하지 못했습니다.")
    if recommendation == "revise":
        lines.append("지금 계속하면 non-reportable 상태를 반복할 가능성이 큽니다.")
    elif recommendation == "block":
        lines.append("재개 조건 없이 진행하면 같은 blocker를 반복할 가능성이 큽니다.")
    elif recommendation == "stop":
        lines.append("다음 episode보다 closeout과 별도 검토가 더 적합합니다.")
    return render_lines_section("왜 이 추천인가", lines, max_items=3, max_chars=300)


def judgment_route_for(request: dict[str, Any]) -> dict[str, Any]:
    record = decision_record_for(request)
    route = record.get("judgment_route")
    return route if isinstance(route, dict) else {}


def display_judgment_evaluator(value: Any) -> str:
    labels = {
        "council": "Council",
        "single_harness": "단일 harness",
        "deterministic_gate": "결정적 gate",
        "user": "사용자",
    }
    raw = str(value or "").strip()
    return labels.get(raw, raw or "미지정")


def primary_judgment_route_line(request: dict[str, Any]) -> str:
    route = judgment_route_for(request)
    if not route:
        return ""
    evaluator = display_judgment_evaluator(route.get("evaluator"))
    reason = str(route.get("reason") or "").strip()
    if reason:
        return f"판단 경로: {evaluator} - {compact(reason, 180)}"
    return f"판단 경로: {evaluator}"


def render_judgment_route_section(request: dict[str, Any]) -> str:
    route = judgment_route_for(request)
    if not route:
        return ""
    lines: list[str] = []
    evaluator = display_judgment_evaluator(route.get("evaluator"))
    if evaluator:
        lines.append(f"평가자: {evaluator}")
    reason = str(route.get("reason") or "").strip()
    if reason:
        lines.append(f"이유: {reason}")
    default = str(route.get("default_if_no_reply") or "").strip()
    if default:
        lines.append(f"무응답 기본값: {display_decision(default)}")
    basis = list_values(route.get("policy_basis"), limit=4)
    if basis:
        lines.append("정책 근거: " + "; ".join(basis))
    return render_lines_section("판단 경로", lines, max_items=5, max_chars=300)


def render_evidence_sufficiency_section(brief: dict[str, Any]) -> str:
    lines: list[str] = []
    sufficiency = str(brief.get("evidence_sufficiency") or "").strip()
    if sufficiency:
        lines.append(sufficiency)
    default_if_no_reply = str(brief.get("default_if_no_reply") or "").strip()
    if default_if_no_reply:
        lines.append(f"무응답 기본값: {display_decision(default_if_no_reply)}")
    return render_lines_section("증거 충분성", lines, max_items=3, max_chars=300)


def render_review_surface_section(brief: dict[str, Any]) -> str:
    surface = brief.get("review_surface")
    if not isinstance(surface, dict):
        return ""
    lines: list[str] = []
    status = surface.get("status") if isinstance(surface.get("status"), dict) else {}
    accepted_truth = (
        surface.get("accepted_truth") if isinstance(surface.get("accepted_truth"), dict) else {}
    )
    closeout = surface.get("closeout") if isinstance(surface.get("closeout"), dict) else {}
    runtime = surface.get("runtime") if isinstance(surface.get("runtime"), dict) else {}
    decisions = surface.get("decisions") if isinstance(surface.get("decisions"), dict) else {}
    adaptive_wiki = surface.get("adaptive_wiki") if isinstance(surface.get("adaptive_wiki"), dict) else {}
    implementation_packet = (
        surface.get("implementation_packet")
        if isinstance(surface.get("implementation_packet"), dict)
        else {}
    )
    if status.get("summary"):
        lines.append(f"상태: {status.get('summary')}")
    elif status.get("label"):
        lines.append(f"상태: {status.get('label')} ({status.get('severity') or 'unknown'})")
    if accepted_truth.get("status"):
        reason = str(accepted_truth.get("reason") or "").strip()
        line = f"Accepted truth: {accepted_truth.get('status')}"
        if accepted_truth.get("receipt_acceptance_status"):
            line += f" / receipt {accepted_truth.get('receipt_acceptance_status')}"
        if accepted_truth.get("accepted_receipt_id"):
            line += f" / {accepted_truth.get('accepted_receipt_id')}"
        if reason:
            line += f" - {reason}"
        lines.append(line)
    if closeout.get("review_status") or closeout.get("execution_status"):
        lines.append(
            f"Closeout: review {closeout.get('review_status') or 'unknown'}, "
            f"execution {closeout.get('execution_status') or 'unknown'}"
        )
    if implementation_packet:
        packet_id = str(implementation_packet.get("packet_id") or "implementation packet").strip()
        outcome = str(implementation_packet.get("outcome") or "unknown").strip()
        safe_to_delegate = implementation_packet.get("safe_to_delegate")
        revision_count = len(list_items(implementation_packet.get("required_revisions"), limit=20))
        missing_decision_count = len(list_items(implementation_packet.get("missing_decisions"), limit=20))
        readiness = f"Implementation packet: {packet_id}, outcome {outcome}"
        if isinstance(safe_to_delegate, bool):
            readiness += f", safe_to_delegate {str(safe_to_delegate).lower()}"
        if revision_count or missing_decision_count:
            readiness += f", revisions {revision_count}건, missing decisions {missing_decision_count}건"
        lines.append(readiness)
        goal = str(implementation_packet.get("goal") or "").strip()
        if goal:
            lines.append(f"설계 목표: {goal}")
    risks = closeout.get("unresolved_risks") if isinstance(closeout.get("unresolved_risks"), list) else []
    if risks:
        lines.append("남은 위험: " + "; ".join(str(item) for item in risks[:3]))
    if runtime.get("progress_summary"):
        lines.append(f"Runtime: {runtime.get('progress_summary')}")
    open_count = decisions.get("open_count") if isinstance(decisions.get("open_count"), int) else 0
    wiki_candidates = (
        adaptive_wiki.get("candidate_count") if isinstance(adaptive_wiki.get("candidate_count"), int) else 0
    )
    review_due = (
        adaptive_wiki.get("review_due_count") if isinstance(adaptive_wiki.get("review_due_count"), int) else 0
    )
    if open_count or wiki_candidates or review_due:
        lines.append(f"Review queue: decisions {open_count}건, wiki candidates {wiki_candidates}건, due {review_due}건")
    artifact_summaries = surface.get("artifact_summaries")
    if isinstance(artifact_summaries, list):
        for item in artifact_summaries[:3]:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label") or "Artifact").strip()
            why = str(item.get("why_it_matters") or "").strip()
            if why:
                lines.append(f"{label}: {why}")
    return render_quote_section("Morning Review Surface", lines, expandable=len(lines) > 4, max_items=8, max_chars=320)


def render_reply_example_section(request: dict[str, Any]) -> str:
    brief = brief_for(request)
    raw_examples = brief.get("reply_examples")
    if isinstance(raw_examples, dict):
        examples = [
            f"{display_decision(decision)} 예: {example}"
            for decision, example in raw_examples.items()
            if str(example or "").strip()
        ]
        return render_lines_section("답장 예시", examples, max_items=4, max_chars=320)
    if isinstance(raw_examples, list):
        return render_lines_section("답장 예시", list_values(raw_examples, limit=4), max_items=4, max_chars=320)
    examples: list[str] = []
    decisions = set(natural_input_decisions(request))
    if "revise" in decisions:
        examples.append("수정 예: primary gate 실패 원인을 먼저 진단하고 reportable claim 승격은 금지해.")
    if "block" in decisions:
        examples.append("보류 예: primary gate 원인 분석 전까지 멈추고 재개 조건을 다시 정리해.")
    if CUSTOM_DIRECTION_DECISION in decisions:
        examples.append("기타 예: 안정화를 먼저 하고 템플릿 확장은 문서만 남겨.")
    return render_lines_section("답장 예시", examples, max_items=3, max_chars=320)


def render_decision_impact_section(request: dict[str, Any]) -> str:
    brief = brief_for(request)
    impacts = brief.get("decision_impacts")
    if isinstance(impacts, dict) and impacts:
        lines = [
            f"{display_decision(decision)}: {impact}"
            for decision, impact in impacts.items()
            if str(impact or "").strip()
        ]
        return render_lines_section("선택별 의미", lines, max_items=6, max_chars=300)
    if is_direction_choice(request):
        lines = []
        for option in direction_options(request)[:5]:
            description = option.get("description") or "이 방향으로 다음 작업 기준을 고정합니다."
            lines.append(f"{option['index']}. {option['label']}: {description}")
        if allow_custom_direction(request):
            lines.append("기타: 버튼 선택 후 원하는 방향을 자연어로 직접 지정합니다.")
        return render_lines_section("선택별 의미", lines, max_items=6, max_chars=300)
    return render_lines_section(
        "선택별 의미",
        [
            "계속: 현재 경고를 감수하고 다음 episode로 진행합니다.",
            "수정: 자연어로 수정 방향을 남기고 다음 episode를 그 방향으로 진행합니다.",
            "보류: 지금은 멈추고 재개 조건이나 추가 확인이 필요하다고 기록합니다.",
            "중단: 이 런을 닫고 closeout 또는 별도 검토로 전환합니다.",
        ],
        max_items=4,
        max_chars=300,
    )


def render_detail_fallback(request: dict[str, Any], request_id: str) -> str:
    summary = request.get("summary")
    return (
        SECTION_TEMPLATE.format(
            title="상세 정보 부족",
            body=(
                "- 사용자용 브리프를 만들 episode/council 구조 정보가 부족합니다.\n"
                "- 아래 요약은 request summary에서 추출한 임시 판단 근거입니다."
            ),
        )
        + render_payload_section("요청 요약", summary, max_items=8, max_chars=280)
        + render_decision_impact_section(request)
    )


def render_detail_card(request: dict[str, Any], request_id: str) -> str:
    brief = brief_for(request)
    recommendation = brief_recommendation(request)
    if is_ondesk_handoff(request):
        headline = "Ondesk 전환 상세"
    elif is_direction_choice(request):
        headline = "방향 선택 상세"
    else:
        headline = f"{display_decision(recommendation)} 권고의 근거" if recommendation else "승인 요청의 근거"
    if brief:
        why_recommendation_section = render_why_recommendation_section(brief)
        judgment_route_section = render_judgment_route_section(request)
        evidence_sufficiency_section = render_evidence_sufficiency_section(brief)
        review_surface_section = render_review_surface_section(brief)
        failure_section = render_failure_section(brief)
        evidence_section = render_evidence_section(brief, detailed=True)
        council_section = render_council_section(request, brief, detailed=True)
        next_action_section = render_next_action_section(brief, detailed=True)
        if not (
            why_recommendation_section
            or judgment_route_section
            or evidence_sufficiency_section
            or review_surface_section
            or failure_section
            or evidence_section
            or council_section
            or next_action_section
        ):
            return f"<b>{html.escape(headline)}</b>" + render_detail_fallback(request, request_id)
        return DETAIL_TEMPLATE.format(
            headline=html.escape(headline),
            why_recommendation_section=why_recommendation_section,
            judgment_route_section=judgment_route_section,
            evidence_sufficiency_section=evidence_sufficiency_section,
            review_surface_section=review_surface_section,
            failure_section=failure_section,
            evidence_section=evidence_section,
            council_section=council_section,
            next_action_section=next_action_section,
            decision_impact_section=render_decision_impact_section(request),
            reply_example_section=render_reply_example_section(request),
        )
    return f"<b>{html.escape(headline)}</b>" + render_detail_fallback(request, request_id)


def request_id_for(request: dict[str, Any], fallback: str) -> str:
    for key in ("decision_request_id", "request_id", "task_id", "id"):
        value = str(request.get(key) or "").strip()
        if value:
            return value
    return fallback


def render_message(request: dict[str, Any], request_id: str) -> str:
    return render_decision_card(request, request_id, status="pending")


def render_decision_card(
    request: dict[str, Any],
    request_id: str,
    *,
    status: str,
    decision: str | None = None,
    reason: str = "",
    input_prompt: str = "",
) -> str:
    return render_compact_decision_card(
        request,
        status=status,
        decision=decision,
        reason=reason,
        input_prompt=input_prompt,
    )


def decision_state_path(out_path: pathlib.Path) -> pathlib.Path:
    stem = out_path.stem or "telegram_decision"
    return out_path.with_name(f"{stem}.telegram_decision_state.json")


def decision_state_path_for_state(state: dict[str, Any], out_path: pathlib.Path) -> pathlib.Path:
    raw_path = str(state.get("state_path") or "").strip()
    return pathlib.Path(raw_path) if raw_path else decision_state_path(out_path)


def write_decision_state(state: dict[str, Any], out_path: pathlib.Path) -> None:
    write_json(decision_state_path_for_state(state, out_path), state)


def build_decision_state(request: dict[str, Any], request_id: str, out_path: pathlib.Path) -> dict[str, Any]:
    short_id = secrets.token_hex(4)
    actions = {action["key"]: action for action in action_specs_for(request)}
    tokens = {code: f"fg:{short_id}:{code}" for code in (*actions.keys(), "m")}
    return {
        "created_at": utc_now(),
        "status": "pending",
        "request_id": request_id,
        "short_id_hash": hashlib.sha256(short_id.encode("utf-8")).hexdigest()[:16],
        "tokens": tokens,
        "actions": actions,
        "request": request,
        "state_path": str(decision_state_path(out_path).resolve()),
        "out_path": str(out_path.resolve()),
    }


def finalize_decision_state(state: dict[str, Any], out_path: pathlib.Path, result: dict[str, Any]) -> None:
    final_state = dict(state)
    tokens = final_state.pop("tokens", {})
    if isinstance(tokens, dict):
        final_state["token_hashes"] = {
            str(key): hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]
            for key, value in tokens.items()
        }
    final_state["status"] = result.get("status")
    final_state["decision"] = result.get("decision")
    final_state["reason"] = result.get("reason")
    final_state["input_mode"] = result.get("input_mode")
    final_state["received_at"] = result.get("received_at")
    if result.get("ambiguous_events"):
        final_state["ambiguous_events"] = result.get("ambiguous_events")
    final_state["completed_at"] = utc_now()
    write_decision_state(final_state, out_path)


def inline_keyboard(state: dict[str, Any], _request_id: str) -> dict[str, Any]:
    tokens = state.get("tokens", {})
    actions = state.get("actions", {})
    request = state.get("request", {})
    if not isinstance(request, dict):
        request = {}
    link_rows: list[list[dict[str, Any]]] = [
        [{"text": link["label"], "url": link["url"]}]
        for link in link_specs_for(request)
    ]
    action_rows: list[list[dict[str, Any]]] = []
    if isinstance(actions, dict) and actions:
        if is_direction_choice(request) or is_ondesk_handoff(request):
            for key, action in actions.items():
                action_rows.append(
                    [
                        {
                            "text": action_button_label(request, action),
                            "callback_data": tokens.get(key, ""),
                        }
                    ]
                )
        else:
            row: list[dict[str, Any]] = []
            for key, action in actions.items():
                row.append(
                    {
                        "text": action_button_label(request, action),
                        "callback_data": tokens.get(key, ""),
                    }
                )
                if len(row) == 2:
                    action_rows.append(row)
                    row = []
            if row:
                action_rows.append(row)
    return {
        "inline_keyboard": [
            *link_rows,
            *action_rows,
            [
                {"text": "근거 보기", "callback_data": tokens.get("m", "")},
            ],
        ]
    }


def telegram_api(token: str, method: str, payload: dict[str, Any], timeout_sec: int = 20) -> dict[str, Any]:
    url = f"https://api.telegram.org/bot{token}/{method}"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_sec) as response:
            data = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace") if hasattr(error, "read") else str(error)
        raise RelayError(f"Telegram API HTTP error ({method}): {detail}") from error
    except urllib.error.URLError as error:
        raise RelayError(f"Telegram API URL error ({method}): {error}") from error
    except json.JSONDecodeError as error:
        raise RelayError(f"Telegram API invalid JSON ({method})") from error
    if not data.get("ok"):
        raise RelayError(f"Telegram API error ({method}): {data}")
    return data


def is_html_parse_error(error: RelayError) -> bool:
    text = str(error).lower()
    return "can't parse entities" in text or "unsupported start tag" in text or "can't find end tag" in text


def downgrade_telegram_html(message: str) -> str:
    return message.replace("<blockquote expandable>", "<blockquote>")


def strip_telegram_html(message: str) -> str:
    text = message
    text = re.sub(r"</?(?:b|strong|i|em|u|ins|s|strike|del|code|pre)>", "", text)
    text = re.sub(r"</?blockquote(?:\s+expandable)?>", "", text)
    text = re.sub(r"</?tg-spoiler>", "", text)
    text = re.sub(r"<span\s+class=\"tg-spoiler\">", "", text)
    text = re.sub(r"</span>", "", text)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text)


def get_updates(token: str, offset: int, poll_timeout_sec: int) -> list[dict[str, Any]]:
    data = telegram_api(
        token,
        "getUpdates",
        {
            "offset": int(offset),
            "timeout": int(poll_timeout_sec),
            "allowed_updates": ["message", "callback_query"],
        },
        timeout_sec=max(20, int(poll_timeout_sec) + 10),
    )
    result = data.get("result", [])
    return [item for item in result if isinstance(item, dict)] if isinstance(result, list) else []


def send_message(
    token: str,
    chat_id: str,
    message: str,
    *,
    reply_markup: dict[str, Any] | None = None,
) -> int | None:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        data = telegram_api(token, "sendMessage", payload)
    except RelayError as error:
        if reply_markup and "copy_text" in str(error):
            fallback_markup = {
                "inline_keyboard": [
                    [
                        {
                            key: value
                            for key, value in button.items()
                            if key != "copy_text"
                        }
                        for button in row
                        if button.get("copy_text") is None
                    ]
                    for row in reply_markup.get("inline_keyboard", [])
                ]
            }
            fallback_markup["inline_keyboard"] = [row for row in fallback_markup["inline_keyboard"] if row]
            payload["reply_markup"] = fallback_markup
            data = telegram_api(token, "sendMessage", payload)
        elif is_html_parse_error(error):
            downgraded_payload = dict(payload)
            downgraded_payload["text"] = downgrade_telegram_html(message)
            try:
                data = telegram_api(token, "sendMessage", downgraded_payload)
            except RelayError as downgraded_error:
                if not is_html_parse_error(downgraded_error):
                    raise
                plain_payload = dict(payload)
                plain_payload.pop("parse_mode", None)
                plain_payload["text"] = strip_telegram_html(message)
                data = telegram_api(token, "sendMessage", plain_payload)
        else:
            raise
    result = data.get("result")
    if isinstance(result, dict):
        message_id = result.get("message_id")
        return int(message_id) if isinstance(message_id, int) else None
    return None


def send_reply_keyboard_remove(token: str, chat_id: str) -> int | None:
    data = telegram_api(
        token,
        "sendMessage",
        {
            "chat_id": chat_id,
            "text": "Forager decision mode",
            "reply_markup": {"remove_keyboard": True},
            "disable_notification": True,
        },
    )
    result = data.get("result")
    if isinstance(result, dict):
        message_id = result.get("message_id")
        return int(message_id) if isinstance(message_id, int) else None
    return None


def delete_message(token: str, chat_id: str, message_id: int) -> bool:
    telegram_api(token, "deleteMessage", {"chat_id": chat_id, "message_id": int(message_id)})
    return True


def cleanup_reply_keyboard(token: str, chat_id: str, *, enabled: bool) -> dict[str, Any]:
    report: dict[str, Any] = {
        "enabled": bool(enabled),
        "attempted": False,
        "status": "skipped" if not enabled else "pending",
    }
    if not enabled:
        return report
    report["attempted"] = True
    try:
        message_id = send_reply_keyboard_remove(token, chat_id)
        report["status"] = "sent"
        report["cleanup_message_id"] = message_id
        if message_id is not None:
            try:
                report["deleted"] = delete_message(token, chat_id, message_id)
            except RelayError:
                report["deleted"] = False
                report["delete_error"] = "telegram_cleanup_message_delete_failed"
        else:
            report["deleted"] = False
    except RelayError:
        report["status"] = "failed"
        report["error"] = "telegram_reply_keyboard_remove_failed"
    return report


def answer_callback_query(token: str, callback_id: str, text: str, *, show_alert: bool = False) -> None:
    telegram_api(
        token,
        "answerCallbackQuery",
        {
            "callback_query_id": callback_id,
            "text": text[:200],
            "show_alert": show_alert,
        },
    )


def edit_message_text(
    token: str,
    chat_id: str,
    message_id: int,
    text: str,
    *,
    reply_markup: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup is not None:
        payload["reply_markup"] = reply_markup
    telegram_api(token, "editMessageText", payload)


def current_update_offset(token: str) -> int:
    updates = get_updates(token, offset=0, poll_timeout_sec=0)
    update_ids = [int(update["update_id"]) for update in updates if isinstance(update.get("update_id"), int)]
    return (max(update_ids) + 1) if update_ids else 0


def find_decision(text: str) -> str | None:
    normalized = str(text or "").strip().lower()
    tokens = re.findall(r"[A-Za-z0-9_.:-]+", normalized)
    for decision in ("stop", "block", "revise", "continue"):
        if decision in tokens:
            return decision
        for alias in DECISION_ALIASES[decision]:
            if alias.isascii():
                if alias in tokens:
                    return decision
            elif alias in normalized:
                return decision
    return None


def parse_decision_text(text: str, request_id: str, *, require_request_id: bool) -> dict[str, Any] | None:
    normalized = str(text or "").strip()
    if not normalized:
        return None
    matched_request_id = request_id in normalized
    if require_request_id and not matched_request_id:
        return None
    decision = find_decision(normalized)
    if decision is None:
        return None
    reason = normalized
    for needle in (request_id if matched_request_id else "", decision):
        if needle:
            reason = re.sub(re.escape(needle), "", reason, count=1, flags=re.IGNORECASE).strip()
    for alias in DECISION_ALIASES[decision]:
        if alias and alias in reason.lower():
            reason = re.sub(re.escape(alias), "", reason, count=1, flags=re.IGNORECASE).strip()
            break
    reason = re.sub(r"^[/:!#\s-]+", "", reason).strip()
    return {
        "decision": decision,
        "reason": reason[:1000],
        "matched_request_id": matched_request_id,
        "raw_text_preview": normalized[:240],
    }


def remove_once(text: str, pattern: str) -> str:
    return re.sub(pattern, "", text, count=1, flags=re.IGNORECASE).strip()


def parse_direction_choice_text(
    text: str,
    request: dict[str, Any],
    request_id: str,
    *,
    require_request_id: bool,
) -> dict[str, Any] | None:
    normalized = str(text or "").strip()
    if not normalized:
        return None
    matched_request_id = request_id in normalized
    if require_request_id and not matched_request_id:
        return None
    lowered = normalized.lower()
    compact_tokens = set(re.findall(r"[A-Za-z0-9_.:-]+", lowered))
    if allow_custom_direction(request):
        custom_match = re.search(r"기타|other|custom|직접|다른\s*방향", lowered)
        if custom_match:
            reason = normalized
            if matched_request_id:
                reason = remove_once(reason, re.escape(request_id))
            reason = remove_once(reason, r"기타|other|custom|직접|다른\s*방향")
            reason = re.sub(r"^[/:!#\s.)-]+", "", reason).strip()
            return {
                "decision": CUSTOM_DIRECTION_DECISION,
                "decision_label": DECISION_LABELS[CUSTOM_DIRECTION_DECISION],
                "reason": reason[:1000],
                "matched_request_id": matched_request_id,
                "raw_text_preview": normalized[:240],
            }
    for option in direction_options(request):
        index = option["index"]
        option_id = option["id"]
        label = option["label"]
        index_match = re.search(rf"(^|\s){re.escape(index)}(번|안)?($|[\s:.)-])", normalized)
        id_match = option_id.lower() in compact_tokens
        label_match = bool(label and label.lower() in lowered)
        if not (index_match or id_match or label_match):
            continue
        reason = normalized
        if matched_request_id:
            reason = remove_once(reason, re.escape(request_id))
        if index_match:
            reason = remove_once(reason, rf"(^|\s){re.escape(index)}(번|안)?($|[\s:.)-])")
        elif id_match:
            reason = remove_once(reason, re.escape(option_id))
        elif label_match:
            reason = remove_once(reason, re.escape(label))
        reason = re.sub(r"^[/:!#\s.)-]+", "", reason).strip()
        return {
            "decision": option_id,
            "decision_label": f"{index}. {label}",
            "reason": reason[:1000],
            "matched_request_id": matched_request_id,
            "raw_text_preview": normalized[:240],
        }
    if is_ondesk_handoff(request) and find_decision(normalized) == "continue":
        recommendation = recommended_decision_for(request)
        if recommendation:
            reason = normalized
            if matched_request_id:
                reason = remove_once(reason, re.escape(request_id))
            for alias in DECISION_ALIASES["continue"]:
                if alias and alias in reason.lower():
                    reason = remove_once(reason, re.escape(alias))
                    break
            reason = re.sub(r"^[/:!#\s.)-]+", "", reason).strip()
            return {
                "decision": recommendation,
                "decision_label": display_decision(recommendation),
                "reason": reason[:1000],
                "matched_request_id": matched_request_id,
                "raw_text_preview": normalized[:240],
            }
    return None


def parse_operator_decision_text(
    text: str,
    request: dict[str, Any],
    request_id: str,
    *,
    require_request_id: bool,
) -> dict[str, Any] | None:
    if direction_options(request):
        return parse_direction_choice_text(text, request, request_id, require_request_id=require_request_id)
    return parse_decision_text(text, request_id, require_request_id=require_request_id)


def callback_chat_id(callback: dict[str, Any]) -> str:
    message = callback.get("message")
    if isinstance(message, dict):
        chat = message.get("chat")
        if isinstance(chat, dict) and chat.get("id") is not None:
            return str(chat.get("id"))
    sender = callback.get("from")
    if isinstance(sender, dict) and sender.get("id") is not None:
        return str(sender.get("id"))
    return ""


def callback_message_id(callback: dict[str, Any]) -> int | None:
    message = callback.get("message")
    if isinstance(message, dict) and isinstance(message.get("message_id"), int):
        return int(message["message_id"])
    return None


def reply_to_message_id(message: dict[str, Any]) -> int | None:
    reply = message.get("reply_to_message")
    if isinstance(reply, dict) and isinstance(reply.get("message_id"), int):
        return int(reply["message_id"])
    return None


def parse_callback_action(data: str, state: dict[str, Any]) -> dict[str, str] | None:
    tokens = state.get("tokens", {})
    actions = state.get("actions", {})
    if not isinstance(tokens, dict) or not isinstance(actions, dict):
        return None
    for action_code, action in actions.items():
        if data == tokens.get(action_code) and isinstance(action, dict):
            return {str(key): str(value) for key, value in action.items()}
    return None


def text_decision_scope(
    parsed: dict[str, Any],
    *,
    message: dict[str, Any] | None,
    state: dict[str, Any],
    active_registry_path: pathlib.Path,
) -> dict[str, Any]:
    if parsed.get("matched_request_id"):
        return {"accepted": True, "reason": "request_id"}
    if message is not None:
        reply_id = reply_to_message_id(message)
        current_message_id = state.get("telegram_message_id")
        if isinstance(current_message_id, int) and reply_id == current_message_id:
            return {"accepted": True, "reason": "telegram_reply_to_decision_card"}
    count = active_request_count(active_registry_path)
    if count <= 1:
        return {"accepted": True, "reason": "single_active_request", "active_request_count": count}
    return {
        "accepted": False,
        "reason": "unscoped_text_matches_multiple_active_requests",
        "active_request_count": count,
    }


def ambiguous_input_result(
    base_result: dict[str, Any],
    parsed: dict[str, Any],
    *,
    input_mode: str,
    text_scope: dict[str, Any],
) -> dict[str, Any]:
    event = ambiguous_event(parsed, input_mode=input_mode, text_scope=text_scope)
    return {
        **base_result,
        "status": "ambiguous_input",
        "received_at": event["received_at"],
        "decision": None,
        "candidate_decision": event["candidate_decision"],
        "decision_label": event["decision_label"],
        "reason": event["reason"],
        "matched_request_id": event["matched_request_id"],
        "input_mode": input_mode,
        "text_scope": text_scope,
        "raw_text_preview": event.get("raw_text_preview"),
        "ambiguous_events": [event],
    }


def ambiguous_event(
    parsed: dict[str, Any],
    *,
    input_mode: str,
    text_scope: dict[str, Any],
    source_update_id: Any = None,
    chat_id_hash: str | None = None,
) -> dict[str, Any]:
    event = {
        "received_at": utc_now(),
        "candidate_decision": parsed.get("decision"),
        "decision_label": parsed.get("decision_label") or display_decision(parsed.get("decision")),
        "reason": text_scope.get("reason") or "unscoped_text_requires_request_context",
        "matched_request_id": parsed.get("matched_request_id"),
        "input_mode": input_mode,
        "text_scope": text_scope,
        "raw_text_preview": parsed.get("raw_text_preview"),
    }
    if source_update_id is not None:
        event["source_update_id"] = source_update_id
    if chat_id_hash:
        event["chat_id_hash"] = chat_id_hash
    return event


def with_ambiguous_events(result: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    if events:
        result = dict(result)
        result["ambiguous_events"] = events
    return result


def poll_for_decision(
    *,
    token: str,
    accepted_chat_ids: set[str],
    request_id: str,
    state: dict[str, Any],
    offset: int,
    timeout_sec: int,
    poll_interval_sec: float,
    active_registry_path: pathlib.Path,
) -> dict[str, Any]:
    deadline = time.time() + max(0, timeout_sec)
    next_offset = offset
    pending_natural_input: dict[str, Any] | None = None
    ambiguous_events: list[dict[str, Any]] = []
    while time.time() <= deadline:
        updates = get_updates(token, offset=next_offset, poll_timeout_sec=min(10, max(1, int(poll_interval_sec))))
        for update in updates:
            update_id = update.get("update_id")
            if isinstance(update_id, int):
                next_offset = max(next_offset, update_id + 1)
            callback = update.get("callback_query")
            if isinstance(callback, dict):
                chat_id = callback_chat_id(callback)
                callback_id = str(callback.get("id") or "")
                if chat_id not in accepted_chat_ids:
                    if callback_id:
                        answer_callback_query(token, callback_id, "허용되지 않은 chat입니다.", show_alert=True)
                    continue
                data = str(callback.get("data") or "")
                if data == state.get("tokens", {}).get("m"):
                    send_message(
                        token,
                        chat_id,
                        render_detail_card(state.get("request", {}), request_id),
                    )
                    answer_callback_query(token, callback_id, "상세 설명을 보냈습니다.")
                    continue
                action = parse_callback_action(data, state)
                if action is not None:
                    decision = str(action.get("decision") or "").strip()
                    decision_label = str(action.get("label") or DECISION_LABELS.get(decision, decision)).strip()
                    prompt = str(action.get("natural_input_prompt") or natural_input_prompt(state.get("request", {}), decision)).strip()
                    if prompt:
                        message_id = callback_message_id(callback)
                        pending_natural_input = {
                            "decision": decision,
                            "decision_label": decision_label,
                            "chat_id": chat_id,
                            "chat_id_hash": chat_hash(chat_id),
                            "message_id": message_id,
                            "prompt": prompt,
                            "prompted_at": utc_now(),
                            "source_update_id": update_id,
                            "callback_data_hash": hashlib.sha256(data.encode("utf-8")).hexdigest()[:16],
                        }
                        answer_callback_query(
                            token,
                            callback_id,
                            f"{decision_label} 설명을 답장해주세요.",
                            show_alert=True,
                        )
                        if message_id is not None:
                            try:
                                edit_message_text(
                                    token,
                                    chat_id,
                                    message_id,
                                    render_decision_card(
                                        state.get("request", {}),
                                        request_id,
                                        status="awaiting_input",
                                        decision=decision,
                                        reason="설명 대기",
                                        input_prompt=prompt,
                                    ),
                                    reply_markup=inline_keyboard(state, request_id),
                                )
                            except RelayError:
                                pass
                        continue
                    reason = "button"
                    answer_callback_query(token, callback_id, f"기록됨: {decision_label}")
                    message_id = callback_message_id(callback)
                    if message_id is not None:
                        try:
                            edit_message_text(
                                token,
                                chat_id,
                                message_id,
                                render_decision_card(
                                    state.get("request", {}),
                                    request_id,
                                    status="accepted",
                                    decision=decision,
                                    reason=reason,
                                ),
                                reply_markup={"inline_keyboard": []},
                            )
                        except RelayError:
                            pass
                    return with_ambiguous_events(
                        {
                            "status": "accepted",
                            "received_at": utc_now(),
                            "source_update_id": update_id,
                            "chat_id_hash": chat_hash(chat_id),
                            "decision": decision,
                            "decision_label": decision_label,
                            "reason": reason,
                            "matched_request_id": True,
                            "input_mode": "callback",
                            "callback_data_hash": hashlib.sha256(data.encode("utf-8")).hexdigest()[:16],
                        },
                        ambiguous_events,
                    )
            message = update.get("message")
            if not isinstance(message, dict):
                continue
            chat = message.get("chat")
            chat_id = str(chat.get("id")) if isinstance(chat, dict) and chat.get("id") is not None else ""
            if chat_id not in accepted_chat_ids:
                continue
            raw_text = str(message.get("text") or "").strip()
            if pending_natural_input and chat_id == pending_natural_input.get("chat_id") and raw_text:
                decision = str(pending_natural_input.get("decision") or "")
                decision_label = str(pending_natural_input.get("decision_label") or DECISION_LABELS.get(decision, decision))
                reason = raw_text[:1000]
                message_id = pending_natural_input.get("message_id")
                if isinstance(message_id, int):
                    try:
                        edit_message_text(
                            token,
                            chat_id,
                            message_id,
                            render_decision_card(
                                state.get("request", {}),
                                request_id,
                                status="accepted",
                                decision=decision,
                                reason="설명 수신",
                            ),
                            reply_markup={"inline_keyboard": []},
                        )
                    except RelayError:
                        pass
                return with_ambiguous_events(
                    {
                        "status": "accepted",
                        "received_at": utc_now(),
                        "source_update_id": update_id,
                        "chat_id_hash": chat_hash(chat_id),
                        "decision": decision,
                        "decision_label": decision_label,
                        "reason": reason,
                        "matched_request_id": False,
                        "input_mode": "callback_then_message",
                        "natural_language_required": True,
                        "prompted_at": pending_natural_input.get("prompted_at"),
                        "callback_data_hash": pending_natural_input.get("callback_data_hash"),
                        "raw_text_preview": raw_text[:240],
                    },
                    ambiguous_events,
                )
            parsed = parse_operator_decision_text(
                raw_text,
                state.get("request", {}),
                request_id,
                require_request_id=False,
            )
            if parsed is None:
                continue
            text_scope = text_decision_scope(
                parsed,
                message=message,
                state=state,
                active_registry_path=active_registry_path,
            )
            if not text_scope.get("accepted"):
                ambiguous_events.append(
                    ambiguous_event(
                        parsed,
                        input_mode="message",
                        text_scope=text_scope,
                        source_update_id=update_id,
                        chat_id_hash=chat_hash(chat_id),
                    )
                )
                send_message(
                    token,
                    chat_id,
                    (
                        "어느 요청에 대한 답장인지 확인이 필요합니다.\n"
                        "해당 의사결정 카드에 reply로 답하거나 버튼을 눌러주세요."
                    ),
                )
                continue
            prompt = natural_input_prompt(state.get("request", {}), str(parsed.get("decision") or ""))
            if prompt and not str(parsed.get("reason") or "").strip():
                message_id = state.get("telegram_message_id")
                pending_natural_input = {
                    "decision": parsed.get("decision"),
                    "chat_id": chat_id,
                    "chat_id_hash": chat_hash(chat_id),
                    "message_id": message_id,
                    "prompt": prompt,
                    "prompted_at": utc_now(),
                    "source_update_id": update_id,
                    "callback_data_hash": None,
                }
                if isinstance(message_id, int):
                    try:
                        edit_message_text(
                            token,
                            chat_id,
                            message_id,
                            render_decision_card(
                                state.get("request", {}),
                                request_id,
                                status="awaiting_input",
                                decision=str(parsed.get("decision") or ""),
                                reason="설명 대기",
                                input_prompt=prompt,
                            ),
                            reply_markup=inline_keyboard(state, request_id),
                        )
                    except RelayError:
                        pass
                continue
            return with_ambiguous_events(
                {
                    "status": "accepted",
                    "received_at": utc_now(),
                    "source_update_id": update_id,
                    "chat_id_hash": chat_hash(chat_id),
                    "input_mode": "message",
                    "natural_language_required": bool(prompt),
                    "text_scope": text_scope,
                    **parsed,
                },
                ambiguous_events,
            )
        if time.time() < deadline:
            time.sleep(max(0.2, poll_interval_sec))
    return with_ambiguous_events(
        {
            "status": "timeout",
            "received_at": None,
            "decision": None,
            "reason": "",
        },
        ambiguous_events,
    )


def run(args: argparse.Namespace) -> dict[str, Any]:
    raw_request = load_json(args.request)
    if not isinstance(raw_request, dict):
        raise RelayError("request JSON must be an object")
    explicit_approval_brief = isinstance(raw_request.get("approval_brief"), dict)
    request = request_with_approval_brief(raw_request)
    approval_brief_validation = validate_approval_brief(
        raw_request if explicit_approval_brief else request,
        explicit=explicit_approval_brief,
    )
    if approval_brief_validation.get("failures"):
        failures = "; ".join(str(item) for item in approval_brief_validation["failures"])
        raise RelayError(f"approval_brief_validation_failed: {failures}")
    request_id = request_id_for(request, args.request.stem)
    message = render_message(request, request_id)
    state = build_decision_state(request, request_id, args.out)
    write_decision_state(state, args.out)
    action_specs = action_specs_for(request)
    link_specs = link_specs_for(request)

    config = resolve_telegram_config(args.env_file)
    active_registry_path = resolve_active_request_registry(args)
    active_guard = register_active_request(
        active_registry_path,
        state=state,
        request_id=request_id,
        message_type=message_type_for(request),
        target_chat_id_hash=chat_hash(config["target_chat_id"]),
        timeout_sec=max(1, args.timeout_sec),
    )
    state["active_request_guard"] = active_guard
    write_decision_state(state, args.out)
    base_result = {
        "created_at": utc_now(),
        "request_path": str(args.request.resolve()),
        "request_id": request_id,
        "env_file": str(args.env_file.resolve()),
        "target_chat_id_hash": chat_hash(config["target_chat_id"]),
        "owner_configured": config["owner_configured"],
        "allow_list_configured": config["allow_list_configured"],
        "message_preview": compact(message, 1800),
        "mobile_card_contract": mobile_card_contract(message),
        "state_path": state["state_path"],
        "approval_brief_schema": brief_for(request).get("schema"),
        "approval_brief_validation": approval_brief_validation,
        "decision_record_schema": decision_record_for(request).get("schema"),
        "decision_record_id": decision_record_for(request).get("decision_id"),
        "decision_record_status": decision_record_for(request).get("status"),
        "keyboard": {
            "inline": True,
            "actions": [action["decision"] for action in action_specs] + ["more_details"],
            "labels": (
                [link["label"] for link in link_specs]
                + [action_button_label(request, action) for action in action_specs]
                + ["근거 보기"]
            ),
            "links": [link["label"] for link in link_specs],
            "natural_input_required": natural_input_decisions(request),
        },
        "detail_preview": compact(render_detail_card(request, request_id), 3600),
        "message_type": message_type_for(request),
        "reply_keyboard_cleanup": {
            "enabled": not bool(args.keep_reply_keyboard),
            "attempted": False,
            "status": "not_sent",
        },
        "active_request_guard": active_guard,
        "dry_run": bool(args.dry_run),
        "timeout_sec": max(0, args.timeout_sec),
    }

    registry_completion_status: str | None = None

    def finish(result: dict[str, Any]) -> dict[str, Any]:
        nonlocal registry_completion_status
        finalize_decision_state(state, args.out, result)
        registry_completion_status = str(result.get("status") or "")
        return result

    try:
        if args.decision_text:
            parsed = parse_operator_decision_text(args.decision_text, request, request_id, require_request_id=False)
            if parsed is None:
                result = {
                    **base_result,
                    "status": "ignored",
                    "decision": None,
                    "reason": "decision_text_did_not_match_request",
                    "input_mode": "manual",
                }
                return finish(result)
            text_scope = text_decision_scope(
                parsed,
                message=None,
                state=state,
                active_registry_path=active_registry_path,
            )
            if not text_scope.get("accepted"):
                result = ambiguous_input_result(
                    base_result,
                    parsed,
                    input_mode="manual",
                    text_scope=text_scope,
                )
                return finish(result)
            result = {**base_result, "status": "accepted", "received_at": utc_now(), "input_mode": "manual", **parsed}
            result["text_scope"] = text_scope
            return finish(result)

        if args.dry_run:
            result = {**base_result, "status": "dry_run", "decision": None, "reason": "dry_run_no_message_sent"}
            return finish(result)

        offset = current_update_offset(config["token"])
        cleanup_report = cleanup_reply_keyboard(
            config["token"],
            config["target_chat_id"],
            enabled=not bool(args.keep_reply_keyboard),
        )
        message_id = send_message(
            config["token"],
            config["target_chat_id"],
            message,
            reply_markup=inline_keyboard(state, request_id),
        )
        state["telegram_message_id"] = message_id
        state["target_chat_id_hash"] = chat_hash(config["target_chat_id"])
        write_decision_state(state, args.out)
        result = poll_for_decision(
            token=config["token"],
            accepted_chat_ids=set(config["accepted_chat_ids"]),
            request_id=request_id,
            state=state,
            offset=offset,
            timeout_sec=max(0, args.timeout_sec),
            poll_interval_sec=max(0.2, args.poll_interval_sec),
            active_registry_path=active_registry_path,
        )
        result = {
            **base_result,
            "message_sent": True,
            "telegram_message_id": message_id,
            "reply_keyboard_cleanup": cleanup_report,
            **result,
        }
        return finish(result)
    finally:
        complete_active_request(active_registry_path, state, status=registry_completion_status or "error")


def main() -> int:
    args = parse_args()
    try:
        result = run(args)
        write_json(args.out, result)
        print(
            json.dumps(
                {
                    "status": result.get("status"),
                    "decision": result.get("decision"),
                    "request_id": result.get("request_id"),
                    "out": str(args.out),
                },
                ensure_ascii=False,
            )
        )
        return 0 if result.get("status") == "accepted" else 2
    except (OSError, json.JSONDecodeError, RelayError) as error:
        result = {
            "created_at": utc_now(),
            "status": "error",
            "decision": None,
            "error": repr(error),
            "request_path": str(args.request),
        }
        write_json(args.out, result)
        print(json.dumps({"status": "error", "error": repr(error), "out": str(args.out)}, ensure_ascii=False))
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
