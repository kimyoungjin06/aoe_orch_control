#!/usr/bin/env python3
"""Regression tests for /sync scenario parsing."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
GW_DIR = ROOT / "scripts" / "gateway"
MOD_FILE = GW_DIR / "aoe_tg_scheduler_handlers.py"

if str(GW_DIR) not in sys.path:
    sys.path.insert(0, str(GW_DIR))

_spec = importlib.util.spec_from_file_location("aoe_tg_scheduler_handlers_mod", MOD_FILE)
assert _spec and _spec.loader
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


def test_parse_scenario_lines_ignores_template_text() -> None:
    template = (ROOT / "templates" / "aoe-team" / "AOE_TODO.md").read_text(encoding="utf-8")
    items = mod._parse_scenario_lines(template)
    assert items == []


def test_parse_scenario_lines_parses_task_formats() -> None:
    text = """
# AOE_TODO.md

Project scenario.

## Tasks

- [ ] First open (no priority)
- Fix without checkbox
1. Numbered task
- [ ] P1: Prioritized open
- [x] P2: Completed item
- P3: Lower priority
- [ ] TODO-123 P2: Adjust thresholds

```text
- [ ] P1: codefence should be ignored
```
"""
    items = mod._parse_scenario_lines(text)
    assert [i["priority"] for i in items] == ["P2", "P2", "P2", "P1", "P2", "P3", "P2"]
    assert [i["status"] for i in items] == ["open", "open", "open", "open", "done", "open", "open"]
    assert [i["summary"] for i in items] == [
        "First open (no priority)",
        "Fix without checkbox",
        "Numbered task",
        "Prioritized open",
        "Completed item",
        "Lower priority",
        "Adjust thresholds",
    ]
    assert items[6]["id"] == "TODO-123"
    assert items[0]["source_section"] == "Tasks"
    assert items[0]["source_reason"] == "scenario_checkbox"
    assert items[0]["source_line"] > 0


def test_extract_todo_items_from_doc_skips_plain_meta_sections_in_todo_docs() -> None:
    text = """
# Next Analysis TODO

Purpose:
- Explain why this document exists
- 기준: keep the baseline policy fixed

## P0 (next)
1) **External bucket follow-ups**
- v1 is complete. 다음은:
- Build a one-page summary table
- 설계 선택지(우선순위):

## P1
- **Actor role transitions**
"""
    items = mod._extract_todo_items_from_doc(text, allow_any_checkbox=True)

    assert [row["summary"] for row in items] == [
        "Build a one-page summary table",
        "**Actor role transitions**",
    ]
    assert items[0]["source_section"] == "P0 (next)"
    assert items[0]["source_reason"] == "doc_section_bullet"
    assert items[1]["source_section"] == "P1"


def test_extract_todo_items_from_doc_skips_completed_and_reference_note_rows() -> None:
    text = """
## P0 (next)
0) **Canonical sync (completed on 2026-03-07)**
- Audit memo: `docs/research/03_manuscript/drafts/ncomms/ncomms-canonical-sync-audit-20260307.md`
- Closed:

## P1 (next)
1) **RQ2 framing cleanup**
- Replace broad “generalization/transferability” rhetoric with “boundary-conditioned OA6 evidence” where possible.
"""
    items = mod._extract_todo_items_from_doc(text, allow_any_checkbox=True)

    assert [row["summary"] for row in items] == [
        "Replace broad “generalization/transferability” rhetoric with “boundary-conditioned OA6 evidence” where possible.",
    ]


def test_extract_todo_items_from_doc_skips_update_sections_as_meta() -> None:
    text = """
# Ncomm-track Next Analysis TODO

Update:
- IC2S2 submission is complete.
- This memo now serves the active NComm manuscript path rather than a post-IC2S2 transition note.

## P0 (next)
- P1: build the canonical backlog from the actual next-step section
"""
    items = mod._extract_todo_items_from_doc(text, allow_any_checkbox=True)

    assert [row["summary"] for row in items] == [
        "build the canonical backlog from the actual next-step section",
    ]


def test_extract_todo_items_from_doc_skips_structural_parent_when_actionable_child_follows() -> None:
    text = """
## P2 (next)
3) **External bucket follow-ups (v1 → v2)**
- v1(External 단일 버킷 + `external_share`/`internal_hhi`)는 산출 완료. 다음은 “외부가 어디로 향하는가”의 보강:
  - Option2(Supp): `external:<field_id>` 분해 버킷(교차분야 spillover 정량화).
  - (필요 시) `peer_n`을 internal/external로 분해한 보조지표 추가(해석 보강).

4) **Motif group × mechanism × risk (최소 요약)**
- 4+1 motif 그룹별로 CP-aligned deltas(BC/DC/CC + actor HHI)를 요약해서 “동일한 모티프라도 메커니즘 패턴이 다른가?”를 1장 표로 확인.
"""
    items = mod._extract_todo_items_from_doc(text, allow_any_checkbox=True)

    assert [row["summary"] for row in items] == [
        "**External bucket follow-ups (v1 → v2)**",
        "4+1 motif 그룹별로 CP-aligned deltas(BC/DC/CC + actor HHI)를 요약해서 “동일한 모티프라도 메커니즘 패턴이 다른가?”를 1장 표로 확인.",
    ]


def test_extract_todo_items_from_doc_skips_remaining_work_meta_label() -> None:
    text = """
## P2 (next)
- 남은 일:
  - `v2`와 peer-count follow-up을 Main/SI 어디까지 넣을지 더 압축할지 결정.
- P1: 실제 작업 항목
"""
    items = mod._extract_todo_items_from_doc(text, allow_any_checkbox=True)

    assert [row["summary"] for row in items] == ["실제 작업 항목"]


def test_extract_todo_items_from_doc_skips_meta_prefixes_inside_todo_heading() -> None:
    text = """
# Notes

## Todo
- Purpose:
- 목적: explain context only
- 산출(최소): produce a baseline snapshot
- P1: implement the actual follow-up
- P2 review the second task
"""
    items = mod._extract_todo_items_from_doc(text, allow_any_checkbox=False)

    assert [row["summary"] for row in items] == [
        "implement the actual follow-up",
        "review the second task",
    ]
    assert [row["priority"] for row in items] == ["P1", "P2"]


def test_discover_todo_file_todos_skips_low_confidence_ops_sources(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    (project_root / "docs" / "research" / "00_ops" / "current").mkdir(parents=True, exist_ok=True)
    (project_root / "notes").mkdir(parents=True, exist_ok=True)

    (project_root / "docs" / "research" / "00_ops" / "current" / "todo-ops.md").write_text(
        "# Ops TODO\n\n- P1: operator reminder\n",
        encoding="utf-8",
    )
    (project_root / "notes" / "project_todo.md").write_text(
        "# Project TODO\n\n- P1: real project task\n",
        encoding="utf-8",
    )

    items, meta, sources = mod._discover_todo_file_todos(
        project_root=project_root,
        files_limit=20,
        max_bytes=512 * 1024,
        min_mtime=0.0,
    )

    assert [row["summary"] for row in items] == ["real project task"]
    assert items[0]["sync_source_class"] == "todo_file"
    assert sources == ["notes/project_todo.md"]
    preview = "\n".join(meta.get("preview") or [])
    assert "skipped:low-confidence(ops_todo 0.45)" in preview


def test_discover_salvage_doc_todos_recovers_next_steps_sections_without_todo_marker(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    docs_dir = project_root / "docs" / "research"
    docs_dir.mkdir(parents=True, exist_ok=True)

    (docs_dir / "analysis-update.md").write_text(
        "# Analysis Update\n\n"
        "## Next steps\n"
        "- P1: validate the salvage bootstrap on recent docs\n"
        "- P2 review the summary before off-desk handoff\n",
        encoding="utf-8",
    )

    recent_items, recent_meta, recent_sources = mod._discover_recent_doc_todos(
        project_root=project_root,
        docs_limit=3,
        candidate_keep=50,
        max_bytes=512 * 1024,
        min_mtime=0.0,
    )
    assert recent_items == []
    assert recent_sources == []
    assert "skipped:no-todo-marker" in "\n".join(recent_meta.get("preview") or [])

    salvage_items, salvage_meta, salvage_sources = mod._discover_salvage_doc_todos(
        project_root=project_root,
        docs_limit=3,
        candidate_keep=50,
        max_bytes=512 * 1024,
        min_mtime=0.0,
    )

    assert [row["summary"] for row in salvage_items] == [
        "validate the salvage bootstrap on recent docs",
        "review the summary before off-desk handoff",
    ]
    assert all(row["sync_source_class"] == "salvage_doc" for row in salvage_items)
    assert salvage_sources == ["docs/research/analysis-update.md"]
    assert "used:2 class=salvage_doc conf=0.78" in "\n".join(salvage_meta.get("preview") or [])


def test_discover_sync_fallback_todos_uses_salvage_after_files_and_recent_fail(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    docs_dir = project_root / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    (docs_dir / "handoff.md").write_text(
        "# Handoff\n\n"
        "## 남은 일\n"
        "- P1: close the queue gap for off-desk bootstrap\n",
        encoding="utf-8",
    )

    mode, items, meta, sources = mod._discover_sync_fallback_todos(
        project_root=project_root,
        docs_limit=3,
        files_limit=10,
        max_bytes=512 * 1024,
        min_mtime=0.0,
    )

    assert mode == "salvage"
    assert [row["summary"] for row in items] == ["close the queue gap for off-desk bootstrap"]
    assert meta.get("docs_used") == 1
    assert sources == ["docs/handoff.md"]


def test_discover_sync_fallback_todos_bootstraps_from_recent_docs_and_todo_files(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    docs_dir = project_root / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    (docs_dir / "handoff.md").write_text(
        "# Handoff\n\n"
        "## Next steps\n"
        "- P1: recover backlog from recent md documents\n",
        encoding="utf-8",
    )
    (project_root / "TODO.md").write_text(
        "# TODO\n\n- [ ] P2: sync the formal todo file as well\n",
        encoding="utf-8",
    )

    mode, items, meta, sources = mod._discover_sync_fallback_todos(
        project_root=project_root,
        docs_limit=3,
        files_limit=10,
        max_bytes=512 * 1024,
        min_mtime=0.0,
    )

    assert mode == "bootstrap"
    assert sorted(row["summary"] for row in items) == sorted(
        [
            "recover backlog from recent md documents",
            "sync the formal todo file as well",
        ]
    )
    assert meta.get("docs_used") >= 1
    assert meta.get("files_used") >= 1
    assert "docs/handoff.md" in sources
    assert "TODO.md" in sources


def test_discover_salvage_doc_proposals_recovers_loose_followups(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    docs_dir = project_root / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)

    (docs_dir / "night-handoff.md").write_text(
        "# Night Handoff\n\n"
        "## Next steps\n"
        "- validate the nightly backlog recovery path\n"
        "- review the handoff note before off-desk execution\n",
        encoding="utf-8",
    )

    proposals, meta, sources = mod._discover_salvage_doc_proposals(
        project_root=project_root,
        docs_limit=3,
        candidate_keep=50,
        max_bytes=512 * 1024,
        min_mtime=0.0,
    )

    assert [row["summary"] for row in proposals] == [
        "validate the nightly backlog recovery path",
        "review the handoff note before off-desk execution",
    ]
    assert all(row["kind"] == "handoff" for row in proposals)
    assert all(row["created_by"] == "sync-salvage" for row in proposals)
    assert sources == ["docs/night-handoff.md"]
    assert "proposed:2" in "\n".join(meta.get("preview") or [])


def test_load_project_sync_policy_reads_team_file(tmp_path: Path) -> None:
    team_dir = tmp_path / ".aoe-team"
    team_dir.mkdir(parents=True, exist_ok=True)
    (team_dir / "sync_policy.json").write_text(
        '{\n  "exclude_globs": ["docs/research/00_ops/current/*.md"],\n  "min_confidence": 0.8\n}\n',
        encoding="utf-8",
    )

    policy = mod._load_project_sync_policy({}, team_dir)

    assert policy["exclude_globs"] == ["docs/research/00_ops/current/*.md"]
    assert policy["min_confidence"] == 0.8
    assert str(policy["_policy_path"]).endswith(".aoe-team/sync_policy.json")


def test_discover_todo_file_todos_respects_sync_policy_include_globs(tmp_path: Path) -> None:
    project_root = tmp_path / "project"
    ops_dir = project_root / "docs" / "research" / "00_ops" / "current"
    ops_dir.mkdir(parents=True, exist_ok=True)

    (ops_dir / "todo-ops.md").write_text(
        "# Ops TODO\n\n- P1: operator reminder that should be included\n",
        encoding="utf-8",
    )

    items, meta, sources = mod._discover_todo_file_todos(
        project_root=project_root,
        files_limit=20,
        max_bytes=512 * 1024,
        min_mtime=0.0,
        sync_policy={"include_globs": ["docs/research/00_ops/current/todo-ops.md"]},
    )

    assert [row["summary"] for row in items] == ["operator reminder that should be included"]
    assert items[0]["sync_source_class"] == "ops_todo"
    assert sources == ["docs/research/00_ops/current/todo-ops.md"]
    preview = "\n".join(meta.get("preview") or [])
    assert "used:1 class=ops_todo conf=0.95" in preview


def test_preview_item_line_includes_provenance_fields() -> None:
    line = mod._preview_item_line(
        {
            "status": "open",
            "priority": "P1",
            "summary": "Build the summary table",
            "sync_source_class": "todo_file",
            "sync_confidence": 0.92,
            "source_file": "docs/TODO.md",
            "source_section": "P0 (next)",
            "source_line": 17,
            "source_reason": "doc_section_bullet",
        }
    )

    assert "@docs/TODO.md" in line
    assert "#P0 (next)" in line
    assert "L17" in line
    assert "via doc_section_bullet" in line
