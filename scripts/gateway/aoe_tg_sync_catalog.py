#!/usr/bin/env python3
"""Sync source classification, policy, and provenance helpers."""

from __future__ import annotations

import fnmatch
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_SCENARIO_FILENAME = "AOE_TODO.md"
_SCENARIO_INCLUDE_PREFIX = "@include"


def _normalize_priority(token: str) -> str:
    raw = str(token or "").strip().upper()
    return raw if raw in {"P1", "P2", "P3"} else "P2"


def _rel_display(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except Exception:
        return str(path)


def _todo_heading(line: str, *, allow_salvage: bool = False) -> bool:
    text = str(line or "").strip().lower()
    if not text:
        return False
    keywords = ["todo", "to-do", "to do", "tasks", "action items", "할일", "해야 할 일", "해야할일"]
    if allow_salvage:
        keywords.extend(
            [
                "next step",
                "next steps",
                "follow-up",
                "follow up",
                "followups",
                "follow ups",
                "remaining work",
                "remaining items",
                "open items",
                "pending work",
                "action plan",
                "next actions",
                "다음 단계",
                "다음 작업",
                "후속 작업",
                "남은 일",
                "남은 작업",
                "오픈 이슈",
            ]
        )
    return any(k in text for k in keywords)


def _scenario_include_tokens(text: str) -> List[str]:
    """Return include paths from scenario text.

    Supported:
    - `@include relative/or/absolute/path.md`
    - `@include: path.md`
    """

    out: List[str] = []
    seen: set[str] = set()
    in_code = False
    in_tasks = False
    saw_tasks_heading = False

    for raw_line in str(text or "").splitlines():
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
            if _todo_heading(heading):
                in_tasks = True
                saw_tasks_heading = True
            elif saw_tasks_heading:
                in_tasks = False
            continue

        if stripped.startswith("#"):
            continue
        if saw_tasks_heading and not in_tasks:
            continue

        low = stripped.lower()
        if not low.startswith(_SCENARIO_INCLUDE_PREFIX):
            continue

        rest = stripped[len(_SCENARIO_INCLUDE_PREFIX) :].strip()
        if rest.startswith(":"):
            rest = rest[1:].strip()
        if not rest:
            continue
        if (rest.startswith('"') and rest.endswith('"')) or (rest.startswith("'") and rest.endswith("'")):
            rest = rest[1:-1].strip()
        if not rest:
            continue

        if rest in seen:
            continue
        seen.add(rest)
        out.append(rest)

    return out[:20]


def _doc_has_todo_markers(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    keywords = ("todo", "to-do", "tasks", "action items", "할일", "해야 할 일", "해야할일")
    return any(k in lowered for k in keywords)


def _path_has_todo_hint(path: Path) -> bool:
    name = str(path.name or "").lower()
    if not name:
        return False
    keywords = (
        "todo",
        "to-do",
        "todolist",
        "task",
        "tasks",
        "action_items",
        "action-items",
        "actionitems",
        "할일",
        "해야할일",
        "해야_할_일",
    )
    return any(k in name for k in keywords)


def _salvage_heading(line: str) -> bool:
    text = str(line or "").strip().lower()
    if not text:
        return False
    keywords = (
        "next step",
        "next steps",
        "follow-up",
        "follow up",
        "followups",
        "follow ups",
        "remaining work",
        "remaining items",
        "open items",
        "pending work",
        "action plan",
        "next actions",
        "handoff",
        "다음 단계",
        "다음 작업",
        "후속 작업",
        "남은 일",
        "남은 작업",
        "오픈 이슈",
        "인계",
    )
    return any(k in text for k in keywords)


def _load_project_sync_policy(entry: Dict[str, Any], team_dir: Path) -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    inline = entry.get("sync_policy")
    if isinstance(inline, dict):
        merged.update(inline)

    policy_path = Path(team_dir).expanduser().resolve() / "sync_policy.json"
    if policy_path.exists():
        try:
            loaded = json.loads(policy_path.read_text(encoding="utf-8"))
        except Exception:
            loaded = {}
        if isinstance(loaded, dict):
            merged.update(loaded)
            merged["_policy_path"] = str(policy_path)
    return merged


def _apply_sync_policy(info: Dict[str, Any], *, rel: str, policy: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out = dict(info or {})
    if not isinstance(policy, dict) or not policy:
        return out

    rel_norm = str(rel or "").strip()
    rel_low = rel_norm.lower()

    exclude_globs = [str(x).strip() for x in list(policy.get("exclude_globs") or []) if str(x).strip()]
    if any(fnmatch.fnmatch(rel_norm, pat) or fnmatch.fnmatch(rel_low, pat.lower()) for pat in exclude_globs):
        out["confidence"] = 0.0
        out["policy_reason"] = "excluded"
        return out

    include_globs = [str(x).strip() for x in list(policy.get("include_globs") or []) if str(x).strip()]
    if any(fnmatch.fnmatch(rel_norm, pat) or fnmatch.fnmatch(rel_low, pat.lower()) for pat in include_globs):
        try:
            out["confidence"] = max(float(out.get("confidence", 0.0) or 0.0), 0.95)
        except Exception:
            out["confidence"] = 0.95
        out["policy_reason"] = "included"

    class_conf = policy.get("class_confidence")
    source_class = str(out.get("source_class", "")).strip()
    if isinstance(class_conf, dict) and source_class:
        raw_val = class_conf.get(source_class)
        try:
            if raw_val is not None:
                out["confidence"] = float(raw_val)
                out["policy_reason"] = f"class_confidence:{source_class}"
        except Exception:
            pass

    doc_type_conf = policy.get("doc_type_confidence")
    doc_type = str(out.get("doc_type", "")).strip()
    if isinstance(doc_type_conf, dict) and doc_type:
        raw_val = doc_type_conf.get(doc_type)
        try:
            if raw_val is not None:
                out["confidence"] = float(raw_val)
                out["policy_reason"] = f"doc_type_confidence:{doc_type}"
        except Exception:
            pass

    group_map = policy.get("group_overrides")
    if isinstance(group_map, dict) and source_class:
        raw_group = str(group_map.get(source_class, "")).strip()
        if raw_group:
            out["sync_group"] = raw_group[:80]
            out["policy_reason"] = f"group_override:{source_class}"

    try:
        min_conf = float(policy.get("min_confidence", out.get("confidence", 0.0)) or 0.0)
    except Exception:
        min_conf = float(out.get("confidence", 0.0) or 0.0)
    out["policy_min_confidence"] = min_conf
    return out


def _classify_sync_doc_type(path: Path, root: Path) -> str:
    rel = _rel_display(path, root)
    rel_low = rel.lower()
    name_low = str(path.name or "").lower()

    if name_low == _SCENARIO_FILENAME.lower():
        return "scenario"

    if any(marker in rel_low for marker in ("/archive/", "/archives/", "/old/", "/deprecated/")):
        return "archive"
    if any(marker in rel_low for marker in ("/00_ops/", "/ops/", "ops/", "/runbook/", "/checklist/")):
        return "ops"
    if _path_has_todo_hint(path):
        return "todo"

    handoff_markers = ("handoff", "hand-off", "handover", "hand-over", "인계")
    report_markers = ("report", "results", "readout", "summary", "brief", "dashboard")
    spec_markers = ("spec", "specs", "prd", "design", "requirements")
    manuscript_markers = ("manuscript", "draft", "paper", "quarto", "ncomms", "submission")
    note_markers = ("note", "notes", "memo", "meeting", "journal", "log", "update")

    if any(token in rel_low or token in name_low for token in handoff_markers):
        return "handoff"
    if any(token in rel_low or token in name_low for token in report_markers):
        return "report"
    if any(token in rel_low or token in name_low for token in spec_markers):
        return "spec"
    if any(token in rel_low or token in name_low for token in manuscript_markers):
        return "manuscript"
    if "/docs/research/" in rel_low or rel_low.startswith("docs/research/"):
        return "research_note"
    if any(token in rel_low or token in name_low for token in note_markers):
        return "note"
    return "doc"


def _source_confidence_for(mode: str, doc_type: str) -> float:
    token = str(mode or "").strip()
    dtype = str(doc_type or "").strip()
    if token == "salvage_docs":
        return {
            "handoff": 0.86,
            "report": 0.72,
            "spec": 0.60,
            "manuscript": 0.52,
            "note": 0.64,
            "research_note": 0.72,
            "doc": 0.62,
        }.get(dtype, 0.62)
    if token == "recent_docs":
        return {
            "handoff": 0.82,
            "report": 0.70,
            "spec": 0.58,
            "manuscript": 0.50,
            "note": 0.60,
            "research_note": 0.64,
            "doc": 0.58,
        }.get(dtype, 0.58)
    return 0.60


def _classify_sync_source(path: Path, root: Path, *, mode: str) -> Dict[str, Any]:
    name_low = str(path.name or "").lower()
    doc_type = _classify_sync_doc_type(path, root)

    if name_low == _SCENARIO_FILENAME.lower():
        return {"source_class": "scenario", "sync_group": "scenario", "confidence": 1.0, "doc_type": "scenario"}

    if doc_type == "archive":
        return {"source_class": "archive_todo", "sync_group": "archive", "confidence": 0.25, "doc_type": doc_type}
    if doc_type == "ops":
        return {"source_class": "ops_todo", "sync_group": "ops", "confidence": 0.45, "doc_type": doc_type}
    if _path_has_todo_hint(path):
        return {"source_class": "todo_file", "sync_group": "todo_files", "confidence": 0.92, "doc_type": doc_type}
    if str(mode or "").strip() == "salvage_docs":
        return {
            "source_class": "salvage_doc",
            "sync_group": "salvage_docs",
            "confidence": _source_confidence_for("salvage_docs", doc_type),
            "doc_type": doc_type,
        }
    if str(mode or "").strip() == "recent_docs":
        return {
            "source_class": "recent_doc",
            "sync_group": "recent_docs",
            "confidence": _source_confidence_for("recent_docs", doc_type),
            "doc_type": doc_type,
        }
    return {
        "source_class": "doc",
        "sync_group": "docs",
        "confidence": 0.60,
        "doc_type": doc_type,
    }


def _sync_candidate_allowed(info: Dict[str, Any]) -> bool:
    source_class = str(info.get("source_class", "")).strip().lower()
    try:
        confidence = float(info.get("confidence", 0.0) or 0.0)
    except Exception:
        confidence = 0.0
    try:
        min_confidence = float(info.get("policy_min_confidence", 0.70) or 0.70)
    except Exception:
        min_confidence = 0.70
    if source_class == "scenario":
        return True
    return confidence >= min_confidence


def _attach_item_provenance(
    row: Optional[Dict[str, Any]],
    *,
    source_section: str = "",
    source_reason: str = "",
    source_line: int = 0,
) -> Optional[Dict[str, Any]]:
    if not isinstance(row, dict):
        return None
    out = dict(row)
    section = str(source_section or "").strip()
    reason = str(source_reason or "").strip()
    try:
        line_no = max(0, int(source_line or 0))
    except Exception:
        line_no = 0
    if section:
        out["source_section"] = section[:160]
    if reason:
        out["source_reason"] = reason[:80]
    if line_no > 0:
        out["source_line"] = line_no
    return out


def _promote_explicit_todo_marker_confidence(info: Dict[str, Any], text: str) -> Dict[str, Any]:
    out = dict(info or {})
    source_class = str(out.get("source_class", "")).strip()
    if source_class not in {"recent_doc", "salvage_doc", "doc"}:
        return out
    if not _doc_has_todo_markers(text):
        return out
    try:
        current = float(out.get("confidence", 0.0) or 0.0)
    except Exception:
        current = 0.0
    boosted = max(current, 0.76)
    if boosted > current:
        out["confidence"] = boosted
        if not str(out.get("policy_reason", "")).strip():
            out["policy_reason"] = "todo_markers"
    return out


def _tag_sync_items(items: List[Dict[str, Any]], *, rel: str, info: Dict[str, Any]) -> List[Dict[str, Any]]:
    tagged: List[Dict[str, Any]] = []
    source_class = str(info.get("source_class", "")).strip()
    sync_group = str(info.get("sync_group", "")).strip()
    confidence = float(info.get("confidence", 0.0) or 0.0)
    doc_type = str(info.get("doc_type", "")).strip()
    for row in items:
        if not isinstance(row, dict):
            continue
        tagged_row = dict(row)
        tagged_row["source_file"] = rel
        tagged_row["sync_source_class"] = source_class
        tagged_row["sync_group"] = sync_group
        tagged_row["sync_confidence"] = round(confidence, 2)
        if doc_type:
            tagged_row["sync_doc_type"] = doc_type
        tagged.append(tagged_row)
    return tagged


def _choose_sync_row(existing: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, Any]:
    try:
        existing_conf = float(existing.get("sync_confidence", 0.0) or 0.0)
    except Exception:
        existing_conf = 0.0
    try:
        candidate_conf = float(candidate.get("sync_confidence", 0.0) or 0.0)
    except Exception:
        candidate_conf = 0.0
    if candidate_conf > existing_conf:
        return candidate
    return existing
