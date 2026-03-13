#!/usr/bin/env python3
"""Document todo extraction heuristics for sync source discovery."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from aoe_tg_sync_catalog import (
    _attach_item_provenance,
    _normalize_priority,
    _salvage_heading,
    _todo_heading,
)

_STATUS_OPEN = "open"
_STATUS_DONE = "done"


def _normalize_summary_key(summary: str) -> str:
    text = str(summary or "").strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text


def _extract_explicit_todo_id(text: str) -> Tuple[str, str]:
    """Return (todo_id, stripped_text) if TODO-123 style token exists."""
    raw = str(text or "")
    m = re.search(r"\bTODO-\d+\b", raw, flags=re.IGNORECASE)
    if not m:
        return "", raw
    todo_id = m.group(0).strip().upper()
    stripped = (raw[: m.start()] + " " + raw[m.end() :]).strip()
    stripped = re.sub(r"\s+", " ", stripped)
    return todo_id, stripped


def _heading_is_done(heading: str) -> bool:
    text = str(heading or "").strip().lower()
    if not text:
        return False
    keywords = (
        "done",
        "completed",
        "complete",
        "finished",
        "finish",
        "closed",
        "resolved",
        "완료",
        "완료됨",
        "종료",
        "끝",
    )
    return any(k in text for k in keywords)


def _heading_is_meta(heading: str) -> bool:
    """Return True when a heading is likely non-action context."""

    text = str(heading or "").strip().lower()
    if not text:
        return False
    keywords = (
        "purpose",
        "update",
        "updates",
        "status legend",
        "legend",
        "notes",
        "note",
        "stamp",
        "context",
        "background",
        "overview",
        "exit criteria",
        "criteria",
        "guidance",
        "참고",
        "배경",
        "업데이트",
        "요약",
        "설명",
        "목적",
        "범례",
        "상태",
        "기준",
        "출처",
    )
    return any(k in text for k in keywords)


def _strip_summary_marker(text: str) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    raw = re.sub(r"^\s*[-*]\s+", "", raw)
    raw = re.sub(r"^\s*\d+[.)]\s+", "", raw)
    raw = raw.strip()
    changed = True
    while changed and raw:
        changed = False
        for left, right in (("**", "**"), ("__", "__"), ("*", "*"), ("_", "_"), ("`", "`")):
            if raw.startswith(left) and raw.endswith(right) and len(raw) > (len(left) + len(right)):
                raw = raw[len(left) : len(raw) - len(right)].strip()
                changed = True
    return raw


def _summary_label_key(text: str) -> str:
    raw = _strip_summary_marker(text)
    if not raw:
        return ""
    if ":" in raw and not raw.rstrip().endswith(":"):
        return ""
    if len(raw) > 80:
        return ""
    if len(re.findall(r"\S+", raw)) > 8:
        return ""
    raw = raw.rstrip(":").strip()
    raw = re.sub(r"\s*\([^)]*\)\s*$", "", raw).strip()
    raw = re.sub(r"\s+", " ", raw)
    return raw.lower()


def _summary_is_meta_label(text: str) -> bool:
    label = _summary_label_key(text)
    if not label:
        return False
    keywords = {
        "purpose",
        "update",
        "updates",
        "stamp",
        "status legend",
        "legend",
        "note",
        "notes",
        "context",
        "background",
        "overview",
        "guidance",
        "criteria",
        "exit criteria",
        "summary",
        "objective",
        "objectives",
        "목적",
        "업데이트",
        "배경",
        "요약",
        "설명",
        "범례",
        "상태",
        "기준",
        "출처",
        "참고",
        "산출",
        "산출물",
        "설계 선택지",
        "남은 일",
        "후속 작업",
        "remaining work",
        "next steps",
    }
    return label in keywords


def _summary_is_done_label(text: str) -> bool:
    label = _summary_label_key(text)
    if not label:
        return False
    keywords = {
        "done",
        "completed",
        "complete",
        "finished",
        "finish",
        "closed",
        "resolved",
        "완료",
        "완료됨",
        "종료",
        "끝",
    }
    return label in keywords


def _summary_has_meta_prefix(text: str) -> bool:
    raw = _strip_summary_marker(text)
    if not raw:
        return False
    lowered = re.sub(r"\s+", " ", raw).lower()
    prefixes = (
        "purpose",
        "update",
        "updates",
        "stamp",
        "context",
        "background",
        "overview",
        "guidance",
        "criteria",
        "exit criteria",
        "summary",
        "objective",
        "objectives",
        "deliverable",
        "deliverables",
        "output",
        "outputs",
        "목적",
        "업데이트",
        "배경",
        "요약",
        "설명",
        "기준",
        "출처",
        "참고",
        "산출",
        "산출물",
        "산출(최소)",
        "설계 선택지",
        "남은 일",
        "후속 작업",
        "remaining work",
        "next steps",
    )
    for prefix in prefixes:
        if lowered.startswith(prefix):
            tail = lowered[len(prefix) :].lstrip()
            if tail.startswith(":"):
                return True
    return False


def _summary_has_completion_marker(text: str) -> bool:
    raw = _strip_summary_marker(text)
    if not raw:
        return False
    lowered = re.sub(r"\s+", " ", raw).lower()
    if re.search(r"\b(completed|done|finished|resolved)\s+on\s+\d{4}-\d{2}-\d{2}\b", lowered):
        return True
    if re.search(r"\b(완료|종료)\s*(일자|날짜)?\s*[:(]?\s*\d{4}-\d{2}-\d{2}", lowered):
        return True
    return False


def _summary_is_reference_note(text: str) -> bool:
    raw = _strip_summary_marker(text)
    if not raw:
        return False
    lowered = re.sub(r"\s+", " ", raw).lower()
    prefixes = (
        "audit memo:",
        "memo:",
        "note:",
        "notes:",
        "link:",
        "doc:",
        "document:",
        "draft:",
        "reference:",
        "ref:",
    )
    if not any(lowered.startswith(prefix) for prefix in prefixes):
        return False
    reference_markers = ("`", "/", ".md", ".tex", ".pdf", ".csv", ".json", ".yaml", ".yml", ".ipynb")
    return any(marker in raw for marker in reference_markers)


def _summary_is_structural_title(text: str) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return False
    return bool(
        re.fullmatch(r"\*\*[^*].*?\*\*", raw)
        or re.fullmatch(r"__.+__", raw)
        or re.fullmatch(r"`.+`", raw)
    )


def _summary_is_non_actionable(text: str) -> bool:
    raw = _strip_summary_marker(text)
    lowered = re.sub(r"\s+", " ", raw).lower()
    if raw.rstrip().endswith(":"):
        intro_markers = (
            "다음은",
            "following",
            "follow-up",
            "follow ups",
            "details",
            "산출 완료",
            "completed",
            "closed",
        )
        if any(marker in lowered for marker in intro_markers):
            return True
    return (
        _summary_is_done_label(text)
        or _summary_has_completion_marker(text)
        or _summary_is_reference_note(text)
        or _summary_is_meta_label(text)
        or _summary_has_meta_prefix(text)
    )


def _line_is_plain_meta_label(text: str) -> bool:
    return _summary_is_meta_label(text)


def _line_is_plain_todo_label(text: str, *, allow_salvage: bool = False) -> bool:
    raw = _summary_label_key(text)
    return bool(raw) and _todo_heading(raw, allow_salvage=allow_salvage)


def _line_is_plain_done_label(text: str) -> bool:
    return _summary_is_done_label(text)


def _doc_has_any_markdown_checkbox(text: str) -> bool:
    return bool(re.search(r"(?m)^\s*[-*]\s*\[[ xX]\]\s+.+", str(text or "")))


def _parse_doc_todo_line(*, rest: str, status: str) -> Optional[Dict[str, Any]]:
    todo_id, rest = _extract_explicit_todo_id(rest)
    text = str(rest or "").strip()
    if not text:
        return None
    pr = "P2"
    summary = text
    m = re.match(r"^(P[1-3])(?:\s*[:|-]\s*|\s+)(.+)$", text, flags=re.IGNORECASE)
    if m:
        pr = _normalize_priority(m.group(1))
        summary = str(m.group(2) or "").strip()
    summary = summary.strip().lstrip(":|-").strip()
    if not summary:
        return None
    if _summary_is_non_actionable(summary):
        return None
    return {"id": todo_id, "priority": pr, "status": status, "summary": summary[:600]}


def _parse_doc_section_bullet(*, rest: str, status: str, allow_loose: bool) -> Optional[Dict[str, Any]]:
    """Parse a bullet under a Todo/Tasks heading."""

    todo_id, rest = _extract_explicit_todo_id(rest)
    text = str(rest or "").strip()
    if not text:
        return None

    m = re.match(r"^(P[1-3])(?:\s*[:|-]\s*|\s+)(.+)$", text, flags=re.IGNORECASE)
    if m:
        pr = _normalize_priority(m.group(1))
        summary = str(m.group(2) or "").strip()
    else:
        if not allow_loose:
            return None
        pr = "P2"
        summary = text
    summary = summary.strip().lstrip(":|-").strip()
    if not summary:
        return None
    if _summary_is_non_actionable(summary):
        return None
    return {"id": todo_id, "priority": pr, "status": status, "summary": summary[:600]}


def _has_following_top_level_actionable_child(
    lines: List[str],
    *,
    start_index: int,
    current_indent_len: int,
    allow_any_checkbox: bool,
) -> bool:
    limit = min(len(lines), start_index + 7)
    for j in range(start_index + 1, limit):
        raw = str(lines[j] or "")
        stripped = raw.strip()
        if not stripped:
            continue
        if re.match(r"^\s{0,3}#{1,6}\s+.+$", stripped):
            break
        m_bullet = re.match(r"^(?P<indent>\s*)[-*]\s+(?P<rest>.+)$", raw)
        m_num = re.match(r"^(?P<indent>\s*)\d+[.)]\s+(?P<rest>.+)$", raw)
        m_list = m_bullet or m_num
        if not m_list:
            continue
        indent = str(m_list.group("indent") or "")
        indent_len = len(indent.replace("\t", "    "))
        if indent_len < current_indent_len:
            break
        if indent_len > current_indent_len:
            continue
        rest = str(m_list.group("rest") or "").strip()
        m_chk = re.match(r"^\[(?P<chk>[ xX])\]\s*(?P<rest>.+)$", rest)
        if m_chk:
            rest = str(m_chk.group("rest") or "").strip()
        row = _parse_doc_section_bullet(rest=rest, status=_STATUS_OPEN, allow_loose=bool(allow_any_checkbox))
        if row:
            if _summary_is_structural_title(str(row.get("summary", ""))):
                break
            return True
    return False


def _extract_todo_items_from_doc(
    text: str,
    *,
    allow_any_checkbox: bool,
    allow_salvage_sections: bool = False,
) -> List[Dict[str, Any]]:
    """Conservative todo extraction for arbitrary docs."""

    items: List[Dict[str, Any]] = []
    seen: set[str] = set()
    in_code = False
    doc_is_todo = bool(allow_any_checkbox)
    section_is_meta = False
    section_is_done = False
    current_section = ""

    checkbox_only = _doc_has_any_markdown_checkbox(text)
    all_lines = str(text or "").splitlines()

    for line_no, raw_line in enumerate(all_lines, start=1):
        line = raw_line.rstrip("\n")
        stripped = line.strip()
        if not stripped:
            continue
        is_list_style_line = bool(re.match(r"^\s*(?:[-*]|\d+[.)])\s+.+$", line))

        fence = stripped.startswith("```") or stripped.startswith("~~~")
        if fence:
            in_code = not in_code
            continue
        if in_code:
            continue

        m_heading = re.match(r"^\s{0,3}#{1,6}\s+(?P<h>.+)$", stripped)
        if m_heading:
            heading = str(m_heading.group("h") or "").strip()
            current_section = heading[:160]
            if _todo_heading(heading, allow_salvage=allow_salvage_sections):
                doc_is_todo = True
            section_is_meta = _heading_is_meta(heading)
            section_is_done = _heading_is_done(heading)
            continue

        if _line_is_plain_todo_label(stripped, allow_salvage=allow_salvage_sections):
            if not is_list_style_line:
                doc_is_todo = True
                section_is_meta = False
                section_is_done = False
                current_section = _strip_summary_marker(stripped).rstrip(":").strip()[:160]
            continue

        if _line_is_plain_done_label(stripped):
            if not is_list_style_line:
                doc_is_todo = True
                section_is_meta = False
                section_is_done = True
                current_section = _strip_summary_marker(stripped).rstrip(":").strip()[:160]
            continue

        if _line_is_plain_meta_label(stripped):
            if not is_list_style_line:
                section_is_meta = True
                section_is_done = False
                current_section = _strip_summary_marker(stripped).rstrip(":").strip()[:160]
            continue

        m_cb = re.match(r"^\s*[-*]\s*\[(?P<chk>[ xX])\]\s*(?P<rest>.+)$", stripped)
        if m_cb:
            if section_is_meta or not doc_is_todo:
                continue
            chk = str(m_cb.group("chk") or "").strip().lower()
            status = _STATUS_DONE if chk == "x" else _STATUS_OPEN
            row = _parse_doc_todo_line(rest=str(m_cb.group("rest") or "").strip(), status=status)
            if row:
                row = _attach_item_provenance(
                    row,
                    source_section=current_section,
                    source_reason="doc_checkbox",
                    source_line=line_no,
                )
                key = _normalize_summary_key(str(row.get("summary", "")))
                if key and key not in seen:
                    seen.add(key)
                    items.append(row)
            continue

        m_todo = re.match(r"^\s*(?:[-*]\s*)?(?:todo|할일)\s*[:\\-]\s*(?P<rest>.+)$", stripped, flags=re.IGNORECASE)
        if m_todo:
            if section_is_meta:
                continue
            row = _parse_doc_todo_line(rest=str(m_todo.group("rest") or "").strip(), status=_STATUS_OPEN)
            if row:
                row = _attach_item_provenance(
                    row,
                    source_section=current_section or "Todo",
                    source_reason="doc_todo_prefix",
                    source_line=line_no,
                )
                key = _normalize_summary_key(str(row.get("summary", "")))
                if key and key not in seen:
                    seen.add(key)
                    items.append(row)
            continue

        if checkbox_only or (not doc_is_todo) or section_is_meta:
            continue

        m_bullet = re.match(r"^(?P<indent>\s*)[-*]\s+(?P<rest>.+)$", line)
        m_num = re.match(r"^(?P<indent>\s*)\d+[.)]\s+(?P<rest>.+)$", line)
        m_list = m_bullet or m_num
        if not m_list:
            continue

        indent = str(m_list.group("indent") or "")
        indent_len = len(indent.replace("\t", "    "))
        if indent_len > 1:
            continue

        rest = str(m_list.group("rest") or "").strip()
        status = _STATUS_DONE if section_is_done else _STATUS_OPEN
        m_chk = re.match(r"^\[(?P<chk>[ xX])\]\s*(?P<rest>.+)$", rest)
        if m_chk:
            chk = str(m_chk.group("chk") or "").strip().lower()
            status = _STATUS_DONE if chk == "x" else _STATUS_OPEN
            rest = str(m_chk.group("rest") or "").strip()
        row = _parse_doc_section_bullet(rest=rest, status=status, allow_loose=bool(allow_any_checkbox))
        if row:
            if _summary_is_structural_title(str(row.get("summary", ""))) and _has_following_top_level_actionable_child(
                all_lines,
                start_index=line_no - 1,
                current_indent_len=indent_len,
                allow_any_checkbox=bool(allow_any_checkbox),
            ):
                continue
            row = _attach_item_provenance(
                row,
                source_section=current_section,
                source_reason="doc_section_bullet",
                source_line=line_no,
            )
            key = _normalize_summary_key(str(row.get("summary", "")))
            if key and key not in seen:
                seen.add(key)
                items.append(row)

    return items[:120]


def _extract_salvage_proposal_items_from_doc(text: str) -> List[Dict[str, Any]]:
    """Recover softer follow-up bullets from handoff-style docs into proposals."""

    items: List[Dict[str, Any]] = []
    seen: set[str] = set()
    in_code = False
    section_is_meta = False
    section_is_done = False
    current_section = ""
    in_salvage = False
    all_lines = str(text or "").splitlines()

    for line_no, raw_line in enumerate(all_lines, start=1):
        line = raw_line.rstrip("\n")
        stripped = line.strip()
        if not stripped:
            continue
        is_list_style_line = bool(re.match(r"^\s*(?:[-*]|\d+[.)])\s+.+$", line))

        fence = stripped.startswith("```") or stripped.startswith("~~~")
        if fence:
            in_code = not in_code
            continue
        if in_code:
            continue

        m_heading = re.match(r"^\s{0,3}#{1,6}\s+(?P<h>.+)$", stripped)
        if m_heading:
            heading = str(m_heading.group("h") or "").strip()
            current_section = heading[:160]
            in_salvage = _salvage_heading(heading)
            section_is_meta = _heading_is_meta(heading)
            section_is_done = _heading_is_done(heading)
            continue

        if _line_is_plain_meta_label(stripped):
            if not is_list_style_line:
                section_is_meta = True
                section_is_done = False
                current_section = _strip_summary_marker(stripped).rstrip(":").strip()[:160]
            continue

        if _line_is_plain_done_label(stripped):
            if not is_list_style_line:
                section_is_done = True
                section_is_meta = False
                current_section = _strip_summary_marker(stripped).rstrip(":").strip()[:160]
            continue

        if _line_is_plain_todo_label(stripped, allow_salvage=True):
            if not is_list_style_line:
                in_salvage = _salvage_heading(stripped) or not _todo_heading(stripped)
                section_is_meta = False
                section_is_done = False
                current_section = _strip_summary_marker(stripped).rstrip(":").strip()[:160]
            continue

        if (not in_salvage) or section_is_meta or section_is_done:
            continue

        m_bullet = re.match(r"^(?P<indent>\s*)[-*]\s+(?P<rest>.+)$", line)
        m_num = re.match(r"^(?P<indent>\s*)\d+[.)]\s+(?P<rest>.+)$", line)
        m_list = m_bullet or m_num
        if not m_list:
            continue

        indent = str(m_list.group("indent") or "")
        indent_len = len(indent.replace("\t", "    "))
        if indent_len > 1:
            continue

        rest = str(m_list.group("rest") or "").strip()
        m_chk = re.match(r"^\[(?P<chk>[ xX])\]\s*(?P<rest>.+)$", rest)
        if m_chk:
            chk = str(m_chk.group("chk") or "").strip().lower()
            if chk == "x":
                continue
            rest = str(m_chk.group("rest") or "").strip()

        row = _parse_doc_section_bullet(rest=rest, status=_STATUS_OPEN, allow_loose=True)
        if row:
            if _summary_is_structural_title(str(row.get("summary", ""))) and _has_following_top_level_actionable_child(
                all_lines,
                start_index=line_no - 1,
                current_indent_len=indent_len,
                allow_any_checkbox=True,
            ):
                continue
            row = _attach_item_provenance(
                row,
                source_section=current_section,
                source_reason="salvage_section_bullet",
                source_line=line_no,
            )
            key = _normalize_summary_key(str(row.get("summary", "")))
            if key and key not in seen:
                seen.add(key)
                items.append(row)

    return items[:60]
