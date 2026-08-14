//! Adaptive-wiki projection presentation.
//!
//! Command handlers retain query, budget, and policy construction, store access,
//! and runtime-policy acknowledgement authority. This module only renders
//! read-only projection results.

use anyhow::Result;

use super::adaptive_wiki_agent_modes_label;
use crate::offdesk::{AdaptiveWikiProjectionComparisonReport, AdaptiveWikiProjectionReport};

pub(super) fn present_wiki_projection(
    report: &AdaptiveWikiProjectionReport,
    include_report: bool,
    json: bool,
) -> Result<()> {
    if json {
        if include_report {
            println!("{}", serde_json::to_string_pretty(report)?);
        } else {
            println!("{}", serde_json::to_string_pretty(&report.selected)?);
        }
        return Ok(());
    }

    if include_report {
        print_wiki_projection_report(report);
        return Ok(());
    }

    if report.selected.is_empty() {
        println!("No adaptive wiki projection entries found.");
        return Ok(());
    }

    println!(
        "{:<44} {:<16} {:<16} {:<18} INSTRUCTION",
        "ID", "SCOPE", "ACTIVATION", "AGENT_MODES"
    );
    for entry in &report.selected {
        println!(
            "{:<44} {:<16} {:<16} {:<18} {}",
            entry.id,
            format!("{:?}", entry.scope).to_lowercase(),
            format!("{:?}", entry.activation_mode).to_lowercase(),
            adaptive_wiki_agent_modes_label(&entry.agent_modes),
            entry.instruction
        );
    }
    Ok(())
}

pub(super) fn present_wiki_projection_comparison(
    report: &AdaptiveWikiProjectionComparisonReport,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }

    println!("Adaptive wiki projection review-expired policy comparison");
    println!(
        "  budget: entries={} context_chars={} instruction_chars={}",
        report.budget.max_entries,
        report.budget.max_context_chars,
        report.budget.max_instruction_chars
    );
    println!(
        "  warn:   selected={} rejected={} review_expired_projected={} context_chars={}",
        report.summary.warn_selected,
        report.summary.warn_rejected,
        report.warn.summary.review_expired_projected,
        report.summary.warn_estimated_context_chars
    );
    println!(
        "  strict: selected={} rejected={} review_expired_projected={} context_chars={}",
        report.summary.strict_selected,
        report.summary.strict_rejected,
        report.strict.summary.review_expired_projected,
        report.summary.strict_estimated_context_chars
    );
    if !report.summary.selected_only_in_warn.is_empty() {
        println!(
            "  selected only in warn: {}",
            report.summary.selected_only_in_warn.join(", ")
        );
    }
    if !report.summary.selected_only_in_strict.is_empty() {
        println!(
            "  selected only in strict: {}",
            report.summary.selected_only_in_strict.join(", ")
        );
    }
    if !report.summary.review_expired_excluded.is_empty() {
        println!(
            "  review_expired_excluded: {}",
            report.summary.review_expired_excluded.join(", ")
        );
    }
    Ok(())
}

fn print_wiki_projection_report(report: &AdaptiveWikiProjectionReport) {
    println!(
        "Adaptive wiki projection: {} selected, {} rejected, {} conflicts, {} review-expired ({} matching promoted entries)",
        report.summary.selected,
        report.summary.rejected,
        report.summary.conflicts,
        report.summary.review_expired_projected,
        report.summary.promoted_scope_matches
    );
    println!(
        "  budget: entries={} context_chars={} instruction_chars={}",
        report.budget.max_entries,
        report.budget.max_context_chars,
        report.budget.max_instruction_chars
    );
    println!(
        "  policy: review_expired={:?}",
        report.policy.review_expired
    );
    println!(
        "  estimated_context_chars: {}",
        report.summary.estimated_context_chars
    );
    if report.summary.instructions_truncated > 0 {
        println!(
            "  instructions_truncated: {}",
            report.summary.instructions_truncated
        );
    }
    if !report.selected.is_empty() {
        println!("  selected:");
        for entry in &report.selected {
            println!(
                "    {} {:?} {:?}:{} {:?} agent_modes={} evidence={}",
                entry.id,
                entry.kind,
                entry.scope,
                entry.scope_ref,
                entry.confidence,
                adaptive_wiki_agent_modes_label(&entry.agent_modes),
                entry.evidence_count
            );
        }
    }
    if !report.rejected.is_empty() {
        println!("  rejected:");
        for rejection in &report.rejected {
            println!(
                "    {} {:?}: {}",
                rejection.entry_id, rejection.reason, rejection.detail
            );
        }
    }
    if !report.conflicts.is_empty() {
        println!("  conflicts:");
        for conflict in &report.conflicts {
            println!(
                "    {} <-> {} {}: {}",
                conflict.entry_id,
                conflict.conflicting_entry_id,
                conflict.signature,
                conflict.detail
            );
        }
    }
    if !report.review_expired.is_empty() {
        println!("  review_expired:");
        for warning in &report.review_expired {
            println!(
                "    {} {:?}: review_after={} {}",
                warning.entry_id, warning.scope, warning.review_after, warning.detail
            );
        }
    }
}
