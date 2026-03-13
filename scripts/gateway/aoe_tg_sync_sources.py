#!/usr/bin/env python3
"""Sync source discovery and document extraction compatibility surface."""

from __future__ import annotations

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
from aoe_tg_sync_discovery import (
    _discover_recent_doc_todos,
    _discover_salvage_doc_proposals,
    _discover_salvage_doc_todos,
    _discover_sync_fallback_todos,
    _discover_todo_file_todos,
    _is_excluded_doc_path,
    _is_within,
    _parse_scenario_lines,
    _proposal_from_salvage_row,
    _recent_doc_candidates,
    _todo_file_candidates,
)
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

