//! Adaptive-wiki episode and evidence report presentation.
//!
//! Command handlers retain query and filter construction, profile and store
//! selection, report generation, and report-file writes. This module only
//! renders completed reports for JSON and terminal consumers.

use anyhow::Result;

use super::wiki_scope_label;
use crate::offdesk::{
    AdaptiveWikiCorrectionRecurrenceReport, AdaptiveWikiEpisodeEvaluationReport,
    AdaptiveWikiLiveEpisodeTraceReport, AdaptiveWikiPromotionEvidenceChainReport,
};

pub(super) fn present_wiki_episode_evaluation_report(
    report: &AdaptiveWikiEpisodeEvaluationReport,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }

    let action = if report.dry_run { "planned" } else { "wrote" };
    let status = if report.passed { "passed" } else { "failed" };
    println!(
        "Adaptive wiki episode evaluation {action} at {} ({status})",
        report.report_dir
    );
    println!("  target: {}", report.target_entry_id);
    println!(
        "  in-scope: {} entries  out-of-scope: {} entries",
        report.summary.in_scope_projection_count, report.summary.out_of_scope_projection_count
    );
    println!(
        "  checks: target_in_scope={} target_out_of_scope={} scope_leakage={} review_expired_projected={} deprecated_projected={} projected_without_evidence={}",
        report.summary.target_entry_in_scope,
        report.summary.target_entry_out_of_scope,
        report.summary.scope_leakage_count,
        report.summary.review_expired_entry_projected,
        report.summary.deprecated_entry_projected,
        report.summary.projected_without_evidence
    );
    if report.failures.is_empty() {
        println!("  failures: none");
    } else {
        println!("  failures:");
        for failure in &report.failures {
            println!("    - {failure}");
        }
    }
    if !report.in_scope_projection.is_empty() {
        println!(
            "  in-scope ids: {}",
            report
                .in_scope_projection
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if !report.out_of_scope_projection.is_empty() {
        println!(
            "  out-of-scope ids: {}",
            report
                .out_of_scope_projection
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    Ok(())
}

pub(super) fn present_wiki_live_episode_trace_report(
    report: &AdaptiveWikiLiveEpisodeTraceReport,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }

    let action = if report.dry_run { "planned" } else { "wrote" };
    println!(
        "Adaptive wiki live episode trace {action} {} events at {}",
        report.summary.events, report.report_dir
    );
    println!(
        "  tasks: {}  runtime usage: {}  projections: {}  candidates: {}  corrections: {}",
        report.summary.task_events,
        report.summary.runtime_usage_events,
        report.summary.projection_events,
        report.summary.candidate_events,
        report.summary.correction_events
    );
    println!(
        "  promotions: {}  completed: {}  failed: {}  resume pending: {}  rollbacks: {}",
        report.summary.promotion_events,
        report.summary.completion_events,
        report.summary.failure_events,
        report.summary.resume_pending_events,
        report.summary.rollback_events
    );
    if report.summary.usage_without_task > 0 {
        println!(
            "  usage without matching task: {}",
            report.summary.usage_without_task
        );
    }
    for event in &report.events {
        println!(
            "  - {:?} {} task={} request={} entries={} {}",
            event.kind,
            event.occurred_at.to_rfc3339(),
            event.task_id.as_deref().unwrap_or("-"),
            event.request_id.as_deref().unwrap_or("-"),
            event.entry_ids.join(","),
            event.summary
        );
    }
    Ok(())
}

pub(super) fn present_wiki_correction_recurrence_report(
    report: &AdaptiveWikiCorrectionRecurrenceReport,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }

    let action = if report.dry_run { "planned" } else { "wrote" };
    println!(
        "Adaptive wiki correction recurrence {action} at {} ({:?})",
        report.report_dir, report.assessment
    );
    println!("  entry: {}", report.entry_id);
    if let Some(scope) = &report.scope {
        println!(
            "  scope: {}",
            wiki_scope_label(scope.scope, &scope.scope_ref)
        );
    }
    if let Some(promotion_at) = report.promotion_at {
        println!("  promotion: {}", promotion_at.to_rfc3339());
    }
    println!(
        "  corrections: pre={} post={} delta={}",
        report.summary.pre_promotion_correction_events,
        report.summary.post_promotion_correction_events,
        report.summary.recurrence_delta
    );
    println!(
        "  post usage={} failures={} counterexamples={} recurrence_per_1000={}",
        report.summary.post_promotion_usage_events,
        report.summary.post_promotion_failure_events,
        report.summary.post_promotion_counterexample_events,
        report.summary.post_promotion_recurrence_per_1000
    );
    if !report.failures.is_empty() {
        println!("  failures:");
        for failure in &report.failures {
            println!("    - {failure}");
        }
    }
    Ok(())
}

pub(super) fn present_wiki_promotion_chain_report(
    report: &AdaptiveWikiPromotionEvidenceChainReport,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }

    let action = if report.dry_run { "planned" } else { "wrote" };
    println!(
        "Adaptive wiki promotion evidence chain {action} at {}",
        report.report_dir
    );
    println!("  entry: {}", report.entry_id);
    println!(
        "  promotion audit={} candidate snapshot={} entry snapshot={} current entry={}",
        report.summary.promotion_audit_found,
        report.summary.candidate_snapshot_present,
        report.summary.entry_snapshot_present,
        report.summary.current_entry_present
    );
    println!(
        "  usage records={} related audits={} failures={}",
        report.summary.usage_records, report.summary.related_audit_records, report.summary.failures
    );
    if let Some(audit) = &report.promotion_audit {
        println!(
            "  promoted at {} candidate={} actor={}",
            audit.created_at.to_rfc3339(),
            audit.candidate_id.as_deref().unwrap_or("-"),
            audit.actor
        );
    }
    if !report.failures.is_empty() {
        println!("  failures:");
        for failure in &report.failures {
            println!("    - {failure}");
        }
    }
    Ok(())
}
