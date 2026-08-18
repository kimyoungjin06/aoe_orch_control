//! Offdesk task-lifecycle presentation.
//!
//! Command handlers and stores retain task mutation, resume-artifact updates,
//! and denied-approval supersession. This module only renders completed
//! lifecycle reports.

use anyhow::Result;
use serde::Serialize;

use super::operator_safe_json_value;
use crate::offdesk::{operator_safe_text, OffdeskTaskLifecycleReport, OffdeskTaskStatus};

#[derive(Serialize)]
struct RetryTaskLifecycleReport<'a> {
    #[serde(flatten)]
    report: &'a OffdeskTaskLifecycleReport,
    superseded_denied_approvals: usize,
}

pub(super) fn present_task_lifecycle_report(
    report: &OffdeskTaskLifecycleReport,
    json: bool,
) -> Result<()> {
    if json {
        present_operator_safe_json(report)?;
        return Ok(());
    }

    println!(
        "{} offdesk task {}: {} -> {} ({})",
        if report.changed {
            "Updated"
        } else {
            "Unchanged"
        },
        operator_safe_text(&report.task.task_id),
        task_status_label(report.previous_status),
        task_status_label(report.status),
        operator_safe_text(&report.message)
    );
    if let Some(ticket_id) = report.task.background_ticket_id.as_deref() {
        println!("  ticket: {}", operator_safe_text(ticket_id));
    }
    if !report.task.reason.trim().is_empty() {
        println!("  reason: {}", operator_safe_text(&report.task.reason));
    }
    if let Some(error) = report.task.last_error.as_deref() {
        println!("  error:  {}", operator_safe_text(error));
    }
    Ok(())
}

pub(super) fn present_retry_task_lifecycle_report(
    report: &OffdeskTaskLifecycleReport,
    superseded_denied_approvals: usize,
    json: bool,
    include_denied_reset: bool,
) -> Result<()> {
    if json {
        present_operator_safe_json(&RetryTaskLifecycleReport {
            report,
            superseded_denied_approvals,
        })?;
        return Ok(());
    }

    present_task_lifecycle_report(report, false)?;
    if include_denied_reset {
        println!(
            "  superseded denied approvals: {}",
            superseded_denied_approvals
        );
    }
    Ok(())
}

pub(super) fn task_status_label(status: OffdeskTaskStatus) -> &'static str {
    match status {
        OffdeskTaskStatus::Queued => "queued",
        OffdeskTaskStatus::PendingApproval => "pending-approval",
        OffdeskTaskStatus::Launched => "launched",
        OffdeskTaskStatus::Running => "running",
        OffdeskTaskStatus::Completed => "completed",
        OffdeskTaskStatus::Failed => "failed",
        OffdeskTaskStatus::ResumePending => "resume-pending",
        OffdeskTaskStatus::Cancelled => "cancelled",
    }
}

fn present_operator_safe_json(value: &impl Serialize) -> Result<()> {
    let value = operator_safe_json_value(serde_json::to_value(value)?);
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
