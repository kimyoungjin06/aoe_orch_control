#!/usr/bin/env python3
"""Sync source discovery and document extraction helpers for scheduler."""

from __future__ import annotations

import heapq
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from aoe_tg_sync_catalog import (
    _apply_sync_policy,
    _attach_item_provenance,
    _choose_sync_row,
    _classify_sync_source,
    _doc_has_todo_markers,
    _load_project_sync_policy,
    _normalize_priority,
    _path_has_todo_hint,
    _promote_explicit_todo_marker_confidence,
    _rel_display,
    _salvage_heading,
    _scenario_include_tokens,
    _sync_candidate_allowed,
    _tag_sync_items,
    _todo_heading,
)

_STATUS_OPEN = "open"
_STATUS_DONE = "done"
_DISCOVERY_DEFAULT_DOCS_LIMIT = 3
_DISCOVERY_DEFAULT_CANDIDATE_KEEP = 250
_DISCOVERY_DEFAULT_MAX_BYTES = 512 * 1024
_DISCOVERY_ALLOWED_EXTS = {".md", ".txt", ".rst"}
_DISCOVERY_DEFAULT_TODO_FILES_LIMIT = 80
_DISCOVERY_EXCLUDE_DIRS = {
    ".github",
    ".git",
    ".aoe-team",
    ".venv",
    "archive",
    "venv",
    "templates",
    "__pycache__",
    "node_modules",
    "dist",
    "build",
    "target",
    "out",
    ".pytest_cache",
}

def _is_excluded_doc_path(path: Path) -> bool:
    """Skip known test/fixture docs so they don't get scheduled by accident."""
    name = str(getattr(path, "name", "") or "").lower()
    if not name:
        return False
    return "aoe_sync_test" in name or "aoe-sync-test" in name

def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except Exception:
        return False


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
    """Return True when a heading is likely non-action context.

    Used to avoid importing checklists from sections like Purpose/Legend/etc.
    """

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
    return bool(re.search("(?m)^\\s*[-*]\\s*\\[[ xX]\\]\\s+.+", str(text or "")))

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
    """Parse a bullet under a Todo/Tasks heading.

    Be stricter than free-form doc extraction to avoid pulling in command docs.
    Require explicit priority (P1/P2/P3) for non-checkbox bullets.
    """

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

def _extract_todo_items_from_doc(
    text: str,
    *,
    allow_any_checkbox: bool,
    allow_salvage_sections: bool = False,
) -> List[Dict[str, Any]]:
    """Conservative todo extraction for arbitrary docs.

    Only accepts:
    - markdown checkboxes (- [ ] / - [x])
    - TODO: ... lines
    - bullets under a heading that looks like "Todo/Tasks/할일"
    """

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
            if section_is_meta:
                continue
            if not doc_is_todo:
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

        # If the document already uses checkboxes anywhere, treat those as the
        # source of truth and avoid importing loose bullets (often context).
        if checkbox_only:
            continue

        if (not doc_is_todo) or section_is_meta:
            continue

        m_bullet = re.match("^(?P<indent>\\s*)[-*]\\s+(?P<rest>.+)$", line)
        m_num = re.match("^(?P<indent>\\s*)\\d+[.)]\\s+(?P<rest>.+)$", line)
        m_list = m_bullet or m_num
        if not m_list:
            continue

        indent = str(m_list.group("indent") or "")
        indent_len = len(indent.replace("\t", "    "))
        # Skip nested list items: they are usually details/examples.
        if indent_len > 1:
            continue

        rest = str(m_list.group("rest") or "").strip()
        status = _STATUS_DONE if section_is_done else _STATUS_OPEN
        m_chk = re.match("^\\[(?P<chk>[ xX])\\]\\s*(?P<rest>.+)$", rest)
        if m_chk:
            chk = str(m_chk.group("chk") or "").strip().lower()
            status = _STATUS_DONE if chk == "x" else _STATUS_OPEN
            rest = str(m_chk.group("rest") or "").strip()
        # Only accept loose bullets for clearly todo-ish docs (ex: filename contains TODO).
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

    # Hard cap to avoid huge accidental imports.
    return items[:120]

def _extract_salvage_proposal_items_from_doc(text: str) -> List[Dict[str, Any]]:
    """Recover softer follow-up bullets from handoff-style docs into proposal candidates.

    This is intentionally broader than main todo extraction, but still limited to
    salvage-ish sections such as "Next steps/남은 일/Follow-up". Results should go
    to proposal inbox, not directly to the runnable queue.
    """

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

def _recent_doc_candidates(
    project_root: Path,
    *,
    keep: int,
    max_bytes: int,
    min_mtime: float,
) -> List[Tuple[float, Path]]:
    """Return up to `keep` most-recent doc candidates (mtime desc)."""

    root = Path(project_root).expanduser().resolve()
    heap: List[Tuple[float, str]] = []

    if not root.exists() or not root.is_dir():
        return []

    for dirpath, dirnames, filenames in os.walk(root):
        # prune noisy dirs early
        dirnames[:] = [d for d in dirnames if d not in _DISCOVERY_EXCLUDE_DIRS]
        for name in filenames:
            ext = Path(name).suffix.lower()
            if ext not in _DISCOVERY_ALLOWED_EXTS:
                continue
            path = (Path(dirpath) / name).resolve()
            if _is_excluded_doc_path(path):
                continue
            try:
                st = path.stat()
            except Exception:
                continue
            if float(st.st_mtime) < float(min_mtime):
                continue
            if st.st_size <= 0 or st.st_size > int(max_bytes):
                continue
            key = (float(st.st_mtime), str(path))
            if len(heap) < int(keep):
                heapq.heappush(heap, key)
            else:
                if key > heap[0]:
                    heapq.heapreplace(heap, key)

    heap.sort(reverse=True)
    return [(mt, Path(p)) for mt, p in heap]

def _todo_file_candidates(
    project_root: Path,
    *,
    keep: int,
    max_bytes: int,
    min_mtime: float,
) -> List[Tuple[float, Path]]:
    """Return up to `keep` todo-ish file candidates (mtime desc).

    Selection is filename-based (todo/task keywords) to avoid importing arbitrary docs.
    """

    root = Path(project_root).expanduser().resolve()
    out: List[Tuple[float, Path]] = []

    if not root.exists() or not root.is_dir():
        return []

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _DISCOVERY_EXCLUDE_DIRS]
        for name in filenames:
            path = (Path(dirpath) / name).resolve()
            if _is_excluded_doc_path(path):
                continue
            if not _path_has_todo_hint(path):
                continue
            ext = path.suffix.lower()
            if ext and ext not in _DISCOVERY_ALLOWED_EXTS:
                continue
            try:
                st = path.stat()
            except Exception:
                continue
            if float(st.st_mtime) < float(min_mtime):
                continue
            if st.st_size <= 0 or st.st_size > int(max_bytes):
                continue
            out.append((float(st.st_mtime), path))

    out.sort(reverse=True)
    cap = max(1, int(keep)) if int(keep or 0) > 0 else len(out)
    return out[:cap]

def _discover_todo_file_todos(
    *,
    project_root: Path,
    files_limit: int,
    max_bytes: int,
    min_mtime: float,
    sync_policy: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[str]]:
    """Return (items, meta, sources) extracted from todo-ish files by filename."""

    files_limit = max(1, min(400, int(files_limit or _DISCOVERY_DEFAULT_TODO_FILES_LIMIT)))
    max_bytes = max(32 * 1024, min(10 * 1024 * 1024, int(max_bytes or _DISCOVERY_DEFAULT_MAX_BYTES)))

    items: List[Dict[str, Any]] = []
    sources: List[str] = []
    preview: List[str] = []
    scanned = 0
    used = 0
    seen: Dict[str, Dict[str, Any]] = {}
    root = Path(project_root).expanduser().resolve()

    candidates = _todo_file_candidates(project_root, keep=files_limit, max_bytes=max_bytes, min_mtime=min_mtime)
    for _mt, path in candidates:
        scanned += 1
        rel = _rel_display(path, root)
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:read-failed")
            continue
        info = _apply_sync_policy(_classify_sync_source(path, root, mode="todo_files"), rel=rel, policy=sync_policy)
        if not _sync_candidate_allowed(info):
            if len(preview) < 12:
                reason = str(info.get("policy_reason", "")).strip()
                reason_suffix = f" policy={reason}" if reason else ""
                preview.append(
                    f"{rel} -> skipped:low-confidence({info.get('source_class')} {float(info.get('confidence', 0.0)):.2f}){reason_suffix}"
                )
            continue
        extracted = _tag_sync_items(_extract_todo_items_from_doc(text, allow_any_checkbox=True), rel=rel, info=info)
        if not extracted:
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:no-actionable-items")
            continue
        used += 1
        if len(preview) < 12:
            preview.append(
                f"{rel} -> used:{len(extracted)} class={info.get('source_class')} conf={float(info.get('confidence', 0.0)):.2f}"
            )
        for row in extracted:
            if not isinstance(row, dict):
                continue
            key = _normalize_summary_key(str(row.get("summary", "")))
            if not key:
                continue
            prev = seen.get(key)
            if isinstance(prev, dict):
                seen[key] = _choose_sync_row(prev, row)
            else:
                seen[key] = row
        sources.append(rel)

        if len(seen) >= 240:
            break

    items = list(seen.values())

    meta = {
        "candidates": len(candidates),
        "scanned": scanned,
        "files_used": used,
        "items_found": len(items),
        "preview": preview,
    }
    return items, meta, sources

def _discover_recent_doc_todos(
    *,
    project_root: Path,
    docs_limit: int,
    candidate_keep: int,
    max_bytes: int,
    min_mtime: float,
    sync_policy: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[str]]:
    """Return (items, meta, sources) extracted from recent documents."""

    docs_limit = max(1, min(9, int(docs_limit or _DISCOVERY_DEFAULT_DOCS_LIMIT)))
    candidate_keep = max(30, min(2000, int(candidate_keep or _DISCOVERY_DEFAULT_CANDIDATE_KEEP)))
    max_bytes = max(32 * 1024, min(10 * 1024 * 1024, int(max_bytes or _DISCOVERY_DEFAULT_MAX_BYTES)))

    items: List[Dict[str, Any]] = []
    sources: List[str] = []
    preview: List[str] = []
    scanned = 0
    used = 0
    root = Path(project_root).expanduser().resolve()
    seen: Dict[str, Dict[str, Any]] = {}

    candidates = _recent_doc_candidates(project_root, keep=candidate_keep, max_bytes=max_bytes, min_mtime=min_mtime)
    for _mt, path in candidates:
        scanned += 1
        rel = _rel_display(path, root)
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:read-failed")
            continue
        name_hint = _path_has_todo_hint(path)
        if not (name_hint or _doc_has_todo_markers(text)):
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:no-todo-marker")
            continue
        info = _apply_sync_policy(_classify_sync_source(path, root, mode="recent_docs"), rel=rel, policy=sync_policy)
        info = _promote_explicit_todo_marker_confidence(info, text)
        if not _sync_candidate_allowed(info):
            if len(preview) < 12:
                reason = str(info.get("policy_reason", "")).strip()
                reason_suffix = f" policy={reason}" if reason else ""
                preview.append(
                    f"{rel} -> skipped:low-confidence({info.get('source_class')} {float(info.get('confidence', 0.0)):.2f}){reason_suffix}"
                )
            continue
        extracted = _tag_sync_items(_extract_todo_items_from_doc(text, allow_any_checkbox=name_hint), rel=rel, info=info)
        if not extracted:
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:no-actionable-items")
            continue
        used += 1
        if len(preview) < 12:
            preview.append(
                f"{rel} -> used:{len(extracted)} class={info.get('source_class')} conf={float(info.get('confidence', 0.0)):.2f}"
            )
        for row in extracted:
            if not isinstance(row, dict):
                continue
            key = _normalize_summary_key(str(row.get("summary", "")))
            if not key:
                continue
            prev = seen.get(key)
            if isinstance(prev, dict):
                seen[key] = _choose_sync_row(prev, row)
            else:
                seen[key] = row
        sources.append(rel)
        if used >= docs_limit:
            break

    items = list(seen.values())

    meta = {
        "candidates": len(candidates),
        "scanned": scanned,
        "docs_used": used,
        "items_found": len(items),
        "preview": preview,
    }
    return items, meta, sources

def _discover_salvage_doc_todos(
    *,
    project_root: Path,
    docs_limit: int,
    candidate_keep: int,
    max_bytes: int,
    min_mtime: float,
    sync_policy: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[str]]:
    """Best-effort off-desk salvage for recent documents without explicit todo markers.

    This intentionally scans broader recent markdown docs and treats headings like
    "Next steps/남은 일/Follow-up" as todo-ish sections. It is used only after
    stricter sources fail, so success matters more than precision.
    """

    docs_limit = max(1, min(12, int(docs_limit or _DISCOVERY_DEFAULT_DOCS_LIMIT)))
    candidate_keep = max(30, min(2500, int(candidate_keep or _DISCOVERY_DEFAULT_CANDIDATE_KEEP)))
    max_bytes = max(32 * 1024, min(10 * 1024 * 1024, int(max_bytes or _DISCOVERY_DEFAULT_MAX_BYTES)))

    items: List[Dict[str, Any]] = []
    sources: List[str] = []
    preview: List[str] = []
    scanned = 0
    used = 0
    root = Path(project_root).expanduser().resolve()
    seen: Dict[str, Dict[str, Any]] = {}

    candidates = _recent_doc_candidates(project_root, keep=candidate_keep, max_bytes=max_bytes, min_mtime=min_mtime)
    for _mt, path in candidates:
        scanned += 1
        rel = _rel_display(path, root)
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:read-failed")
            continue
        info = _apply_sync_policy(_classify_sync_source(path, root, mode="salvage_docs"), rel=rel, policy=sync_policy)
        info = _promote_explicit_todo_marker_confidence(info, text)
        if not _sync_candidate_allowed(info):
            if len(preview) < 12:
                reason = str(info.get("policy_reason", "")).strip()
                reason_suffix = f" policy={reason}" if reason else ""
                preview.append(
                    f"{rel} -> skipped:low-confidence({info.get('source_class')} {float(info.get('confidence', 0.0)):.2f}){reason_suffix}"
                )
            continue
        extracted = _tag_sync_items(
            _extract_todo_items_from_doc(text, allow_any_checkbox=_path_has_todo_hint(path), allow_salvage_sections=True),
            rel=rel,
            info=info,
        )
        if not extracted:
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:no-salvage-items")
            continue
        used += 1
        if len(preview) < 12:
            preview.append(
                f"{rel} -> used:{len(extracted)} class={info.get('source_class')} conf={float(info.get('confidence', 0.0)):.2f}"
            )
        for row in extracted:
            if not isinstance(row, dict):
                continue
            key = _normalize_summary_key(str(row.get("summary", "")))
            if not key:
                continue
            prev = seen.get(key)
            if isinstance(prev, dict):
                seen[key] = _choose_sync_row(prev, row)
            else:
                seen[key] = row
        sources.append(rel)
        if used >= docs_limit:
            break

    items = list(seen.values())
    meta = {
        "candidates": len(candidates),
        "scanned": scanned,
        "docs_used": used,
        "items_found": len(items),
        "preview": preview,
    }
    return items, meta, sources

def _proposal_from_salvage_row(row: Dict[str, Any]) -> Dict[str, Any]:
    summary = " ".join(str(row.get("summary", "")).strip().split())
    out: Dict[str, Any] = {
        "summary": summary[:600],
        "priority": _normalize_priority(str(row.get("priority", "P2"))),
        "kind": "handoff",
        "confidence": min(0.69, max(0.45, float(row.get("sync_confidence", 0.0) or 0.0) - 0.12)),
        "reason": "sync salvage from recent handoff-style document",
        "created_by": "sync-salvage",
    }
    for field in ("source_file", "source_section", "source_reason"):
        val = str(row.get(field, "")).strip()
        if val:
            out[field] = val
    try:
        line_no = int(row.get("source_line", 0) or 0)
    except Exception:
        line_no = 0
    if line_no > 0:
        out["source_line"] = line_no
    return out

def _discover_salvage_doc_proposals(
    *,
    project_root: Path,
    docs_limit: int,
    candidate_keep: int,
    max_bytes: int,
    min_mtime: float,
    sync_policy: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], List[str]]:
    """Recover softer follow-up bullets into proposal inbox candidates."""

    docs_limit = max(1, min(12, int(docs_limit or _DISCOVERY_DEFAULT_DOCS_LIMIT)))
    candidate_keep = max(30, min(2500, int(candidate_keep or _DISCOVERY_DEFAULT_CANDIDATE_KEEP)))
    max_bytes = max(32 * 1024, min(10 * 1024 * 1024, int(max_bytes or _DISCOVERY_DEFAULT_MAX_BYTES)))

    proposals: List[Dict[str, Any]] = []
    sources: List[str] = []
    preview: List[str] = []
    scanned = 0
    used = 0
    root = Path(project_root).expanduser().resolve()
    seen: Dict[str, Dict[str, Any]] = {}

    candidates = _recent_doc_candidates(project_root, keep=candidate_keep, max_bytes=max_bytes, min_mtime=min_mtime)
    for _mt, path in candidates:
        scanned += 1
        rel = _rel_display(path, root)
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:read-failed")
            continue
        info = _apply_sync_policy(_classify_sync_source(path, root, mode="salvage_docs"), rel=rel, policy=sync_policy)
        if str(info.get("policy_reason", "")).strip() == "excluded":
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:excluded")
            continue
        if str(info.get("source_class", "")).strip() != "salvage_doc":
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:non-salvage-source({info.get('source_class')})")
            continue
        extracted = _extract_salvage_proposal_items_from_doc(text)
        if not extracted:
            if len(preview) < 12:
                preview.append(f"{rel} -> skipped:no-salvage-proposals")
            continue
        used += 1
        if len(preview) < 12:
            preview.append(f"{rel} -> proposed:{len(extracted)}")
        for row in extracted:
            if not isinstance(row, dict):
                continue
            tagged = _tag_sync_items([row], rel=rel, info=info)
            if not tagged:
                continue
            proposal = _proposal_from_salvage_row(tagged[0])
            key = _normalize_summary_key(str(proposal.get("summary", "")))
            if not key:
                continue
            prev = seen.get(key)
            if isinstance(prev, dict):
                prev_conf = float(prev.get("confidence", 0.0) or 0.0)
                cand_conf = float(proposal.get("confidence", 0.0) or 0.0)
                if cand_conf > prev_conf:
                    seen[key] = proposal
            else:
                seen[key] = proposal
        sources.append(rel)
        if used >= docs_limit:
            break

    proposals = list(seen.values())
    meta = {
        "candidates": len(candidates),
        "scanned": scanned,
        "docs_used": used,
        "items_found": len(proposals),
        "preview": preview,
    }
    return proposals, meta, sources

def _discover_sync_fallback_todos(
    *,
    project_root: Path,
    docs_limit: int,
    files_limit: int,
    max_bytes: int,
    min_mtime: float,
    sync_policy: Optional[Dict[str, Any]] = None,
) -> Tuple[str, List[Dict[str, Any]], Dict[str, Any], List[str]]:
    """Best-effort fallback for plain `/sync`.

    Recent markdown work should drive bootstrap when no canonical scenario exists.
    So this combines:
    - recent docs with explicit todo markers
    - salvage docs with follow-up sections
    - todo-ish files by filename

    Items are deduped by summary and the higher-confidence source wins.
    """
    discovered: List[Tuple[str, List[Dict[str, Any]], Dict[str, Any], List[str]]] = []

    try:
        recent_items, recent_meta, recent_sources = _discover_recent_doc_todos(
            project_root=project_root,
            docs_limit=max(docs_limit, 5),
            candidate_keep=max(_DISCOVERY_DEFAULT_CANDIDATE_KEEP, 500),
            max_bytes=max_bytes,
            min_mtime=min_mtime,
            sync_policy=sync_policy,
        )
    except Exception:
        recent_items, recent_meta, recent_sources = [], {}, []
    if recent_items or recent_sources or recent_meta:
        discovered.append(("recent", recent_items, recent_meta, recent_sources))

    try:
        salvage_items, salvage_meta, salvage_sources = _discover_salvage_doc_todos(
            project_root=project_root,
            docs_limit=max(docs_limit, 5),
            candidate_keep=max(_DISCOVERY_DEFAULT_CANDIDATE_KEEP, 500),
            max_bytes=max_bytes,
            min_mtime=min_mtime,
            sync_policy=sync_policy,
        )
    except Exception:
        salvage_items, salvage_meta, salvage_sources = [], {}, []
    if salvage_items or salvage_sources or salvage_meta:
        discovered.append(("salvage", salvage_items, salvage_meta, salvage_sources))

    try:
        file_items, file_meta, file_sources = _discover_todo_file_todos(
            project_root=project_root,
            files_limit=files_limit,
            max_bytes=max_bytes,
            min_mtime=min_mtime,
            sync_policy=sync_policy,
        )
    except Exception:
        file_items, file_meta, file_sources = [], {}, []
    if file_items or file_sources or file_meta:
        discovered.append(("files", file_items, file_meta, file_sources))

    seen: Dict[str, Dict[str, Any]] = {}
    source_order: List[str] = []
    preview: List[str] = []
    docs_used = 0
    docs_scanned = 0
    files_used = 0
    files_scanned = 0
    active_modes: List[str] = []

    for label, items, meta, sources in discovered:
        if items:
            active_modes.append(label)
        for row in items:
            if not isinstance(row, dict):
                continue
            key = _normalize_summary_key(str(row.get("summary", "")))
            if not key:
                continue
            prev = seen.get(key)
            if isinstance(prev, dict):
                seen[key] = _choose_sync_row(prev, row)
            else:
                seen[key] = row
        for src in sources:
            token = str(src or "").strip()
            if token and token not in source_order:
                source_order.append(token)
        for line in list(meta.get("preview") or [])[:4]:
            preview.append(f"[{label}] {line}")
        if label in {"recent", "salvage"}:
            docs_used += int(meta.get("docs_used", 0) or 0)
            docs_scanned += int(meta.get("scanned", 0) or 0)
        elif label == "files":
            files_used += int(meta.get("files_used", 0) or 0)
            files_scanned += int(meta.get("scanned", 0) or 0)

    items = list(seen.values())
    if not items:
        return "", [], {}, []

    meta: Dict[str, Any] = {
        "candidates": max(
            int(recent_meta.get("candidates", 0) or 0),
            int(salvage_meta.get("candidates", 0) or 0),
            int(file_meta.get("candidates", 0) or 0),
        ),
        "scanned": max(docs_scanned, files_scanned, 0),
        "docs_used": docs_used,
        "docs_scanned": docs_scanned,
        "files_used": files_used,
        "files_scanned": files_scanned,
        "items_found": len(items),
        "preview": preview[:16],
        "active_modes": active_modes,
    }

    # Preserve legacy simple labels when only one source family contributed.
    if active_modes == ["files"]:
        return "files", items, meta, source_order
    if active_modes == ["recent"]:
        return "recent", items, meta, source_order
    if active_modes == ["salvage"]:
        return "salvage", items, meta, source_order
    return "bootstrap", items, meta, source_order

def _parse_scenario_lines(text: str) -> List[Dict[str, Any]]:
    """Parse `.aoe-team/AOE_TODO.md` into normalized todo items.

    Supported line formats:
    - `- [ ] P1: summary` (open)
    - `- [x] P2: summary` (done)
    - `- P3: summary` (open)
    """
    items: List[Dict[str, Any]] = []
    in_code = False
    in_tasks = False
    saw_tasks_heading = False
    current_section = ""

    for line_no, raw_line in enumerate(str(text or "").splitlines(), start=1):
        line = raw_line.rstrip("\n")
        stripped = line.strip()
        if not stripped:
            continue

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
            if _todo_heading(heading):
                in_tasks = True
                saw_tasks_heading = True
            else:
                if saw_tasks_heading:
                    in_tasks = False
            continue

        # Allow comments in the scenario file.
        if stripped.startswith("#"):
            continue

        # If the file has a Tasks/Todo heading, only parse within that section.
        if saw_tasks_heading and not in_tasks:
            continue

        status = _STATUS_OPEN
        rest = ""

        m_cb = re.match(r"^\s*[-*]\s*\[(?P<chk>[ xX])\]\s*(?P<rest>.+)$", stripped)
        if m_cb:
            chk = str(m_cb.group("chk") or "").strip().lower()
            status = _STATUS_DONE if chk == "x" else _STATUS_OPEN
            rest = str(m_cb.group("rest") or "").strip()
        else:
            m_bullet = re.match(r"^\s*[-*]\s+(?P<rest>.+)$", stripped)
            m_num = re.match(r"^\s*\d+\.\s+(?P<rest>.+)$", stripped)
            m_list = m_bullet or m_num
            if not m_list:
                continue
            rest = str(m_list.group("rest") or "").strip() if m_list else ""
            if not rest:
                continue

        todo_id, rest = _extract_explicit_todo_id(rest)

        # Priority is optional for checkbox lines (default: P2),
        # but required for non-checkbox bullets to avoid importing prose.
        m_pr = re.match(r"^(P[1-3])(?:\s*[:|-]\s*|\s+)(.+)$", rest, flags=re.IGNORECASE)
        if m_pr:
            pr = _normalize_priority(m_pr.group(1))
            summary = str(m_pr.group(2) or "").strip()
        else:
            if m_cb:
                pr = "P2"
                summary = rest
            else:
                if in_tasks:
                    pr = "P2"
                    summary = rest
                else:
                    continue
        summary = summary.strip().lstrip(":|-").strip()
        if not summary:
            continue

        row = _attach_item_provenance(
            {
                "id": todo_id,
                "priority": pr,
                "status": status,
                "summary": summary[:600],
            },
            source_section=current_section,
            source_reason="scenario_checkbox" if m_cb else "scenario_list",
            source_line=line_no,
        )
        if row:
            items.append(row)

    # Hard cap to avoid huge accidental imports.
    return items[:300]
