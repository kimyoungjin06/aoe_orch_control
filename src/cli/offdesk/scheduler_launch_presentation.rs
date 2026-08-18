//! Offdesk scheduler-gate and background-launch presentation.
//!
//! Command handlers retain scheduler evaluation, approval creation, background
//! launch execution, persistence, and adaptive-wiki usage recording. This
//! module only renders completed gate and launch outcomes.

use anyhow::Result;

use super::{
    adaptive_wiki_agent_mode_cli_value, adaptive_wiki_agent_modes_label, operator_safe_json_value,
};
use crate::offdesk::{
    operator_safe_text, BackgroundLaunchOutcome, SchedulerGateOutcome, SchedulerGateStatus,
};

pub(super) fn present_scheduler_gate_outcome(
    outcome: &SchedulerGateOutcome,
    json: bool,
) -> Result<()> {
    if json {
        present_operator_safe_json(outcome)?;
        return Ok(());
    }

    match outcome.status {
        SchedulerGateStatus::Proceed => {
            println!(
                "Proceed: {} ({}) via {:?}",
                operator_safe_text(&outcome.capability_id),
                operator_safe_text(&outcome.risk_level),
                outcome.approval_mode
            );
        }
        SchedulerGateStatus::PendingApproval => {
            println!(
                "Pending approval: {} ({})",
                operator_safe_text(&outcome.capability_id),
                operator_safe_text(&outcome.risk_level)
            );
            if let Some(approval) = &outcome.approval {
                println!(
                    "  approval_id: {}",
                    operator_safe_text(&approval.approval_id)
                );
                println!(
                    "  action_id:   {}",
                    operator_safe_text(approval.action_id())
                );
                if !approval.preview.trim().is_empty() {
                    println!("  preview:     {}", operator_safe_text(&approval.preview));
                }
                if !approval.reason.trim().is_empty() {
                    println!("  reason:      {}", operator_safe_text(&approval.reason));
                }
            }
        }
        SchedulerGateStatus::Denied => {
            println!(
                "Denied: {} - {}",
                operator_safe_text(&outcome.capability_id),
                operator_safe_text(&outcome.reason)
            );
        }
        SchedulerGateStatus::Blocked => {
            println!(
                "Blocked: {} - {}",
                operator_safe_text(&outcome.capability_id),
                operator_safe_text(&outcome.reason)
            );
            if let Some(capacity) = outcome.provider_capacity.as_ref() {
                println!("  provider:  {}", operator_safe_text(&capacity.provider_id));
                println!(
                    "  model:     {}",
                    capacity
                        .model
                        .as_deref()
                        .map(operator_safe_text)
                        .unwrap_or_else(|| "-".to_string())
                );
                println!(
                    "  scope:     {}",
                    operator_safe_text(&capacity.matched_scope)
                );
                if let Some(retry_at) = outcome.retry_at {
                    println!("  retry_at:  {retry_at}");
                }
            }
            if let Some(fallback) = outcome.provider_fallback.as_ref() {
                let recommended = fallback
                    .candidates
                    .iter()
                    .filter(|candidate| candidate.recommended)
                    .count();
                println!(
                    "  fallback:  {} candidates, {} recommended",
                    fallback.candidates.len(),
                    recommended
                );
            }
        }
    }
    if !outcome.adaptive_wiki.is_empty() {
        println!("  adaptive_wiki: {} entries", outcome.adaptive_wiki.len());
        for entry in outcome.adaptive_wiki.iter().take(3) {
            println!(
                "    - {} {:?} {:?} agent_modes={}: {}",
                operator_safe_text(&entry.id),
                entry.scope,
                entry.activation_mode,
                adaptive_wiki_agent_modes_label(&entry.agent_modes),
                operator_safe_text(&entry.instruction)
            );
        }
    }
    if !outcome.adaptive_wiki_runtime.is_empty() {
        println!(
            "  adaptive_wiki_runtime: {} entries policy review_expired={:?}",
            outcome.adaptive_wiki_runtime.len(),
            outcome.adaptive_wiki_runtime_policy.review_expired
        );
    }
    if let Some(decision) = outcome.adaptive_wiki_runtime_decision.as_ref() {
        println!(
            "  adaptive_wiki_runtime_decision: {:?} ({})",
            decision.status,
            operator_safe_text(&decision.reason)
        );
    }
    Ok(())
}

pub(super) fn present_background_launch_outcome(
    outcome: &BackgroundLaunchOutcome,
    json: bool,
) -> Result<()> {
    if json {
        present_operator_safe_json(outcome)?;
        return Ok(());
    }

    present_scheduler_gate_outcome(&outcome.gate, false)?;
    if let Some(probe) = outcome.probe.as_ref() {
        println!("  ticket_id: {}", operator_safe_text(&probe.ticket_id));
        println!("  runner:    {:?}", probe.runner_kind);
        println!("  phase:     {:?}", probe.phase);
        if let Some(agent_mode) = probe.agent_mode {
            println!(
                "  agent_mode: {}",
                adaptive_wiki_agent_mode_cli_value(agent_mode)
            );
        }
        if let Some(packet) = probe.implementation_packet.as_ref() {
            println!(
                "  packet:    {} ({})",
                operator_safe_text(&packet.packet_id),
                operator_safe_text(&packet.outcome)
            );
        }
    }
    Ok(())
}

fn present_operator_safe_json(value: &impl serde::Serialize) -> Result<()> {
    let value = operator_safe_json_value(serde_json::to_value(value)?);
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}
