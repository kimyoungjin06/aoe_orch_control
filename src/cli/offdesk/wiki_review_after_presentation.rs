//! Adaptive-wiki review-after attention read models and presentation.
//!
//! Command handlers retain store access and review-renewal persistence. This
//! module only derives the read-only attention report and renders it for
//! operators.

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use super::{operator_safe_json_value, shell_quote_arg, wiki_scope_label};
use crate::offdesk::{
    AdaptiveWikiHumanEntry, AdaptiveWikiKind, AdaptiveWikiQuery, AdaptiveWikiScope,
    AdaptiveWikiStatus,
};

#[derive(Serialize)]
pub(super) struct WikiReviewAfterReport {
    generated_at: DateTime<Utc>,
    query: AdaptiveWikiQuery,
    near_expiry_hours: i64,
    pub(super) summary: WikiReviewAfterReportSummary,
    entries: Vec<WikiReviewAfterReportItem>,
}

#[derive(Default, Serialize)]
pub(super) struct WikiReviewAfterReportSummary {
    pub(super) scoped_promoted: usize,
    pub(super) with_review_after: usize,
    pub(super) missing_review_after: usize,
    pub(super) expired: usize,
    pub(super) near_expiry: usize,
    pub(super) attention: usize,
}

#[derive(Serialize)]
struct WikiReviewAfterReportItem {
    id: String,
    kind: AdaptiveWikiKind,
    scope: AdaptiveWikiScope,
    scope_ref: String,
    review_after: DateTime<Utc>,
    hours_until_review: i64,
    status: String,
    renew_command_template: String,
}

pub(super) fn build_review_after_report(
    entries: Vec<AdaptiveWikiHumanEntry>,
    query: AdaptiveWikiQuery,
    near_expiry_hours: i64,
    now: DateTime<Utc>,
) -> WikiReviewAfterReport {
    let near_expiry_window = Duration::hours(near_expiry_hours);
    let mut summary = WikiReviewAfterReportSummary::default();
    let mut attention = Vec::new();
    for entry in entries
        .into_iter()
        .filter(|entry| entry.status == AdaptiveWikiStatus::Promoted)
    {
        summary.scoped_promoted += 1;
        let Some(review_after) = entry.review_after else {
            summary.missing_review_after += 1;
            continue;
        };
        summary.with_review_after += 1;
        if review_after <= now {
            summary.expired += 1;
            attention.push(review_after_report_item(
                entry,
                review_after,
                "expired",
                now,
            ));
        } else if review_after <= now + near_expiry_window {
            summary.near_expiry += 1;
            attention.push(review_after_report_item(
                entry,
                review_after,
                "near_expiry",
                now,
            ));
        }
    }
    summary.attention = attention.len();
    attention.sort_by_key(|entry| (review_after_status_order(&entry.status), entry.review_after));
    WikiReviewAfterReport {
        generated_at: now,
        query,
        near_expiry_hours,
        summary,
        entries: attention,
    }
}

pub(super) fn present_review_after_report(
    report: &WikiReviewAfterReport,
    json: bool,
) -> Result<()> {
    if json {
        let value = operator_safe_json_value(serde_json::to_value(report)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    println!("Adaptive wiki review_after attention report");
    println!(
        "  scoped_promoted: {}  with_review_after: {}  missing_review_after: {}",
        report.summary.scoped_promoted,
        report.summary.with_review_after,
        report.summary.missing_review_after
    );
    println!(
        "  expired: {}  near_expiry: {}  attention: {}",
        report.summary.expired, report.summary.near_expiry, report.summary.attention
    );
    if report.entries.is_empty() {
        println!("No promoted adaptive wiki entries need review_after attention.");
        return Ok(());
    }
    println!(
        "{:<40} {:<12} {:<28} {:<14} SCOPE",
        "ID", "STATUS", "REVIEW_AFTER", "HOURS_LEFT"
    );
    for entry in &report.entries {
        println!(
            "{:<40} {:<12} {:<28} {:<14} {}",
            entry.id,
            entry.status,
            entry.review_after,
            entry.hours_until_review,
            wiki_scope_label(entry.scope, &entry.scope_ref)
        );
        println!("  renew: {}", entry.renew_command_template);
    }
    Ok(())
}

fn review_after_report_item(
    entry: AdaptiveWikiHumanEntry,
    review_after: DateTime<Utc>,
    status: &str,
    now: DateTime<Utc>,
) -> WikiReviewAfterReportItem {
    WikiReviewAfterReportItem {
        renew_command_template: renew_review_after_command_template(&entry.id),
        id: entry.id,
        kind: entry.kind,
        scope: entry.scope,
        scope_ref: entry.scope_ref,
        review_after,
        hours_until_review: review_after.signed_duration_since(now).num_hours(),
        status: status.to_string(),
    }
}

fn review_after_status_order(status: &str) -> u8 {
    match status {
        "expired" => 0,
        "near_expiry" => 1,
        _ => 2,
    }
}

pub(super) fn renew_review_after_command_template(entry_id: &str) -> String {
    format!(
        "forager offdesk wiki renew-review-after {} --review-after <rfc3339> --reason <reason>",
        shell_quote_arg(&crate::offdesk::operator_safe_text(entry_id))
    )
}
