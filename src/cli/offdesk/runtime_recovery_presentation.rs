//! Offdesk runtime-recovery presentation.
//!
//! Command handlers retain resume-store reads, background polling, task
//! reconciliation, recovery validation, and acknowledgement persistence. This
//! module only renders completed runtime-recovery results.

use anyhow::Result;
use chrono::Utc;

use super::{operator_safe_json_value, BackgroundAckReport, BackgroundProbeStatus};
use crate::offdesk::{
    operator_safe_text, BackgroundPollOutcome, BackgroundProbe, BackgroundRecoveryDecision,
    OffdeskModeAssessment, OffdeskNextSafeAction, ResumeStatus, TaskResumeState,
};

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

pub(super) fn present_background_poll_outcomes(
    outcomes: &[BackgroundPollOutcome],
    json: bool,
) -> Result<()> {
    if json {
        present_operator_safe_json(outcomes)?;
        return Ok(());
    }
    if outcomes.is_empty() {
        println!("No matching background runner probes found.");
        return Ok(());
    }
    for outcome in outcomes {
        present_background_probe(&outcome.probe, &outcome.decision, &outcome.mode_assessment);
        present_next_safe_action(&outcome.next_safe_action);
    }
    Ok(())
}

pub(super) fn present_background_statuses(
    statuses: &[BackgroundProbeStatus],
    json: bool,
) -> Result<()> {
    if json {
        present_operator_safe_json(statuses)?;
        return Ok(());
    }
    if statuses.is_empty() {
        println!("No background runner probes found.");
        return Ok(());
    }
    for status in statuses {
        present_background_probe(&status.probe, &status.decision, &status.mode_assessment);
    }
    Ok(())
}

fn present_operator_safe_json(value: &(impl serde::Serialize + ?Sized)) -> Result<()> {
    let value = operator_safe_json_value(serde_json::to_value(value)?);
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn present_background_probe(
    probe: &BackgroundProbe,
    decision: &BackgroundRecoveryDecision,
    mode_assessment: &OffdeskModeAssessment,
) {
    println!(
        "{} {:?} -> {:?}: {}",
        operator_safe_text(&probe.ticket_id),
        probe.runner_kind,
        decision.phase,
        operator_safe_text(&decision.evidence)
    );
    present_mode_assessment(mode_assessment);
    if let Some(observed_at) = probe.last_observed_at {
        println!("  observed_at: {observed_at}");
    }
    if let Some(tail) = probe.last_log_tail.as_deref() {
        println!("  tail: {}", operator_safe_text(tail));
    }
}

fn present_mode_assessment(assessment: &OffdeskModeAssessment) {
    println!(
        "  mode_verdict: {} risk: {}",
        assessment.mode_verdict.label(),
        assessment.mode_risk.label()
    );
    println!(
        "  mode_risk_detail: {}",
        operator_safe_text(&assessment.mode_risk_detail)
    );
    if assessment.review_stage_required {
        println!("  review_stage_required: true");
    }
}

fn present_next_safe_action(action: &OffdeskNextSafeAction) {
    println!("  next:    {}", operator_safe_text(&action.detail));
    if !action.commands.is_empty() {
        let commands = action
            .commands
            .iter()
            .map(|command| operator_safe_text(command))
            .collect::<Vec<_>>();
        println!("  command: {}", commands.join(" | "));
    }
    if action.requires_operator_review {
        println!("  review:  operator review required");
    }
}
