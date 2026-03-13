#!/usr/bin/env python3
"""Sync source discovery and document extraction helpers for scheduler."""

from __future__ import annotations

import heapq
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from aoe_tg_sync_extract import (
    _doc_has_any_markdown_checkbox,
    _extract_explicit_todo_id,
    _extract_salvage_proposal_items_from_doc,
    _extract_todo_items_from_doc,
    _has_following_top_level_actionable_child,
    _heading_is_done,
    _heading_is_meta,
    _line_is_plain_done_label,
    _line_is_plain_meta_label,
    _line_is_plain_todo_label,
    _normalize_summary_key,
    _parse_doc_section_bullet,
    _parse_doc_todo_line,
    _strip_summary_marker,
    _summary_has_completion_marker,
    _summary_has_meta_prefix,
    _summary_is_done_label,
    _summary_is_meta_label,
    _summary_is_non_actionable,
    _summary_is_reference_note,
    _summary_is_structural_title,
    _summary_label_key,
)
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
