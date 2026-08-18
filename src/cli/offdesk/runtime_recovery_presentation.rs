//! Offdesk runtime-recovery presentation.
//!
//! Command handlers retain resume-store reads, background polling, recovery
//! validation, and acknowledgement persistence. This module only renders
//! completed resume and acknowledgement results.

use anyhow::Result;
use chrono::Utc;

use super::{operator_safe_json_value, BackgroundAckReport};
use crate::offdesk::{operator_safe_text, ResumeStatus, TaskResumeState};

pub(super) fn present_resume_states(states: &[TaskResumeState], json: bool) -> Result<()> {
    if json {
        let value = operator_safe_json_value(serde_json::to_value(states)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    if states.is_empty() {
        println!("No task resume artifacts found.");
        return Ok(());
    }

    let now = Utc::now();
    println!(
        "{:<24} {:<16} {:<8} {:<18} NEXT STEP",
        "TASK", "STATUS", "FRESH", "RUNNER"
    );
    for state in states {
        let fresh = if state.status == ResumeStatus::ResumePending {
            if state.is_fresh_at(now) {
                "fresh"
            } else {
                "stale"
            }
        } else {
            "-"
        };
        println!(
            "{:<24} {:<16} {:<8} {:<18} {}",
            operator_safe_text(&state.task_id),
            format!("{:?}", state.status).to_lowercase(),
            fresh,
            operator_safe_text(&state.runner_target),
            operator_safe_text(&state.next_safe_resume_step)
        );
        println!("  resume_id: {}", operator_safe_text(&state.resume_id()));
        for evidence in state.evidence.iter().take(3) {
            let kind = operator_safe_text(&evidence.kind);
            let summary = operator_safe_text(&evidence.summary);
            let path = evidence.path.as_deref().map(operator_safe_text);
            let present = evidence
                .present
                .map(|present| if present { "present" } else { "missing" });
            match (path.as_deref(), present) {
                (Some(path), Some(present)) => {
                    println!("  evidence: {kind}: {summary} ({present}, {path})");
                }
                (Some(path), None) => {
                    println!("  evidence: {kind}: {summary} ({path})");
                }
                _ => println!("  evidence: {kind}: {summary}"),
            }
        }
        if state.evidence.len() > 3 {
            println!("  evidence: +{} more", state.evidence.len() - 3);
        }
        if let Some(tail) = state.last_log_tail.as_deref() {
            println!("  tail: {}", operator_safe_text(tail));
        }
    }
    Ok(())
}

pub(super) fn present_background_ack_report(
    report: &BackgroundAckReport,
    json: bool,
) -> Result<()> {
    if json {
        let value = operator_safe_json_value(serde_json::to_value(report)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }
    println!(
        "Acknowledged background recovery {} -> {:?}",
        operator_safe_text(&report.ticket_id),
        report.status.decision.phase
    );
    println!(
        "  by:     {}",
        operator_safe_text(&report.acknowledgement.acknowledged_by)
    );
    println!(
        "  reason: {}",
        operator_safe_text(&report.acknowledgement.reason)
    );
    if !report.linked_task_ids.is_empty() {
        let task_ids = report
            .linked_task_ids
            .iter()
            .map(|task_id| operator_safe_text(task_id))
            .collect::<Vec<_>>();
        println!("  tasks:  {}", task_ids.join(", "));
    }
    println!(
        "  note:   no retry, resume, closeout, cleanup, or accepted-truth action is authorized by this acknowledgement"
    );
    Ok(())
}
