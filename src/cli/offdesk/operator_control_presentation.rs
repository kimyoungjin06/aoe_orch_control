//! Offdesk operator-control status presentation.
//!
//! Command handlers retain learning-signal scans and operator-pause reads and
//! writes. This module only renders their completed results for JSON and
//! terminal consumers.

use anyhow::Result;

use super::operator_safe_json_value;
use crate::offdesk::{operator_safe_text, LearningScanReport, OperatorPauseState};

pub(super) fn present_learning_scan_report(report: &LearningScanReport, json: bool) -> Result<()> {
    if json {
        let value = operator_safe_json_value(serde_json::to_value(report)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }
    if report.emitted.is_empty() {
        println!(
            "No new learning signals ({} already recorded).",
            report.skipped_already_processed
        );
        return Ok(());
    }
    println!(
        "Emitted {} learning candidate(s) ({} already recorded):",
        report.emitted.len(),
        report.skipped_already_processed
    );
    for signal in &report.emitted {
        println!(
            "  [{}] {}",
            signal.source.as_str(),
            operator_safe_text(&signal.claim)
        );
    }
    println!("Candidates are recommendation-only; review with `forager offdesk wiki candidates`.");
    Ok(())
}

pub(super) fn present_operator_pause_state(state: &OperatorPauseState, json: bool) -> Result<()> {
    if json {
        let value = operator_safe_json_value(serde_json::to_value(state)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }
    if state.paused {
        println!("Offdesk dispatch is PAUSED; new work is held until resume.");
        if let Some(reason) = state.reason.as_deref() {
            println!("  reason: {}", operator_safe_text(reason));
        }
    } else {
        println!("Offdesk dispatch is active.");
    }
    Ok(())
}
