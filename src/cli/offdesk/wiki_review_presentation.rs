//! Adaptive-wiki curator review report presentation.
//!
//! Command handlers retain queue-filter validation, store selection, report
//! generation, and report-file writes. This module only renders completed
//! reports for JSON and terminal consumers.

use anyhow::Result;

use crate::offdesk::AdaptiveWikiReviewReport;

pub(super) fn present_wiki_review_report(
    report: &AdaptiveWikiReviewReport,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }

    let action = if report.dry_run { "planned" } else { "wrote" };
    println!(
        "Adaptive wiki review report {} {} proposals ({} open, {} filtered out) at {}",
        action,
        report.summary.proposals,
        report.summary.open_proposals,
        report.summary.filtered_out_proposals,
        report.report_dir
    );
    println!(
        "  checked: {} entries, {} candidates, {} usage records, {} audit records, {} correction records, {} review events",
        report.summary.entries_checked,
        report.summary.candidates_checked,
        report.summary.usage_records_checked,
        report.summary.audit_records_checked,
        report.summary.correction_records_checked,
        report.summary.review_events_checked
    );
    println!(
        "  lint: {} errors, {} warnings, {} info",
        report.summary.lint_errors, report.summary.lint_warnings, report.summary.lint_info
    );
    println!(
        "  lifecycle: {} with events, {} accepted, {} rejected, {} superseded",
        report.summary.proposals_with_events,
        report.summary.accepted_proposals,
        report.summary.rejected_proposals,
        report.summary.superseded_proposals
    );
    println!(
        "  promotion receipts: {} checked, {} invalid files, {} promoted entries covered, {} missing receipts",
        report.summary.promotion_receipts_checked,
        report.summary.promotion_receipt_files_invalid,
        report.summary.promoted_entries_with_promotion_receipt,
        report.summary.promoted_entries_missing_promotion_receipt
    );
    if report.summary.stale_decision_proposals > 0 {
        println!(
            "  stale decisions: {} need renewed review",
            report.summary.stale_decision_proposals
        );
    }
    for proposal in &report.proposals {
        let lifecycle = proposal
            .lifecycle
            .as_ref()
            .map(|lifecycle| {
                let stale = if lifecycle.stale { ", stale" } else { "" };
                format!(
                    "{:?} by {}{}",
                    lifecycle.decision,
                    empty_dash(&lifecycle.actor),
                    stale
                )
            })
            .unwrap_or_else(|| "Open".to_string());
        println!(
            "  - {:?} {} {} ({:?}, {}): {}",
            proposal.action,
            proposal.subject_kind,
            proposal.subject_id,
            proposal.risk,
            lifecycle,
            proposal.title
        );
        if let Some(lifecycle) = proposal.lifecycle.as_ref() {
            println!(
                "    lifecycle: event={} at={} reason={}",
                lifecycle.latest_event_id,
                lifecycle.decided_at.to_rfc3339(),
                empty_dash(&lifecycle.reason)
            );
            if !lifecycle.stale_evidence_refs.is_empty() {
                println!(
                    "    stale evidence: {}",
                    lifecycle.stale_evidence_refs.join(", ")
                );
            }
        }
        if let Some(command) = proposal.suggested_command.as_deref() {
            println!("    command: {command}");
        }
        println!("    evidence: {}", proposal.evidence_refs.join(", "));
    }
    Ok(())
}

fn empty_dash(value: &str) -> &str {
    if value.is_empty() {
        "-"
    } else {
        value
    }
}
