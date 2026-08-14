//! Adaptive-wiki runtime policy acknowledgement read models and presentation.
//!
//! Command handlers retain store access, acknowledgement validation, and
//! append-only persistence. This module only derives read-only attention
//! reports and renders acknowledgement results for operators.

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use super::{adaptive_wiki_agent_mode_cli_value, operator_safe_json_value, shell_quote_arg};
use crate::offdesk::{
    AdaptiveWikiAgentModeFilter, AdaptiveWikiProjectionBudget, AdaptiveWikiProjectionPolicy,
    AdaptiveWikiQuery, AdaptiveWikiRuntimePolicyAckScopeMode,
    AdaptiveWikiRuntimePolicyAcknowledgement, AdaptiveWikiRuntimePolicyDecision,
    AdaptiveWikiRuntimePolicyDecisionStatus,
};

#[derive(Serialize)]
pub(super) struct WikiRuntimePolicyAckReport {
    generated_at: DateTime<Utc>,
    near_expiry_hours: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<AdaptiveWikiQuery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    budget: Option<AdaptiveWikiProjectionBudget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decision: Option<AdaptiveWikiRuntimePolicyDecision>,
    pub(super) summary: WikiRuntimePolicyAckReportSummary,
    acknowledgements: Vec<WikiRuntimePolicyAckReportItem>,
}

#[derive(Default, Serialize)]
pub(super) struct WikiRuntimePolicyAckReportSummary {
    pub(super) total: usize,
    pub(super) active: usize,
    pub(super) expired: usize,
    pub(super) near_expiry: usize,
    pub(super) suggested_actions: usize,
    pub(super) query_applied: usize,
    pub(super) query_blocked: usize,
    pub(super) query_stale: usize,
    pub(super) query_expired: usize,
}

#[derive(Serialize)]
struct WikiRuntimePolicyAckReportItem {
    id: String,
    scope_mode: AdaptiveWikiRuntimePolicyAckScopeMode,
    query: AdaptiveWikiQuery,
    policy: AdaptiveWikiProjectionPolicy,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    minutes_until_expiry: i64,
    status: Vec<String>,
    review_expired_excluded: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    suggested_action: Option<WikiRuntimePolicyAckSuggestedAction>,
}

#[derive(Serialize)]
struct WikiRuntimePolicyAckSuggestedAction {
    kind: String,
    detail: String,
    compare_command_template: String,
    ack_command_template: String,
}

pub(super) fn build_runtime_policy_ack_report(
    acknowledgements: Vec<AdaptiveWikiRuntimePolicyAcknowledgement>,
    query: Option<AdaptiveWikiQuery>,
    budget: Option<AdaptiveWikiProjectionBudget>,
    decision: Option<AdaptiveWikiRuntimePolicyDecision>,
    near_expiry_hours: i64,
    now: DateTime<Utc>,
) -> WikiRuntimePolicyAckReport {
    let near_expiry_window = Duration::hours(near_expiry_hours);
    let decision_ack_id = decision
        .as_ref()
        .and_then(|decision| decision.acknowledgement_id.as_deref());
    let decision_status = decision.as_ref().map(|decision| decision.status);
    let query_ref = query.as_ref();
    let budget_ref = budget.as_ref();
    let mut summary = WikiRuntimePolicyAckReportSummary {
        total: acknowledgements.len(),
        ..WikiRuntimePolicyAckReportSummary::default()
    };
    let acknowledgements = acknowledgements
        .into_iter()
        .map(|acknowledgement| {
            let mut status = Vec::new();
            let expired = acknowledgement.expires_at <= now;
            let near_expiry =
                !expired && acknowledgement.expires_at <= now + near_expiry_window;
            if expired {
                summary.expired += 1;
                status.push("expired".to_string());
            } else {
                summary.active += 1;
                status.push("active".to_string());
            }
            if near_expiry {
                summary.near_expiry += 1;
                status.push("near_expiry".to_string());
            }
            let mut query_status = None;
            if decision_ack_id == Some(acknowledgement.id.as_str()) {
                match decision_status {
                    Some(AdaptiveWikiRuntimePolicyDecisionStatus::AppliedAcknowledged)
                    | Some(
                        AdaptiveWikiRuntimePolicyDecisionStatus::AppliedProjectArtifactAcknowledged,
                    ) => {
                        summary.query_applied += 1;
                        status.push("query_applied".to_string());
                    }
                    Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedScopeModeBlocked) => {
                        summary.query_blocked += 1;
                        status.push("query_blocked_by_session_scope".to_string());
                        query_status = decision_status;
                    }
                    Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedStaleAcknowledgement) => {
                        summary.query_stale += 1;
                        status.push("query_stale_comparison".to_string());
                        query_status = decision_status;
                    }
                    Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedExpiredAcknowledgement) => {
                        summary.query_expired += 1;
                        status.push("query_expired_acknowledgement".to_string());
                        query_status = decision_status;
                    }
                    _ => {}
                }
            }
            let suggested_action = runtime_policy_ack_suggested_action(
                &acknowledgement,
                expired,
                near_expiry,
                query_status,
                query_ref,
                budget_ref,
            );
            if suggested_action.is_some() {
                summary.suggested_actions += 1;
            }
            WikiRuntimePolicyAckReportItem {
                id: acknowledgement.id,
                scope_mode: acknowledgement.scope_mode,
                query: acknowledgement.query,
                policy: acknowledgement.policy,
                created_at: acknowledgement.created_at,
                expires_at: acknowledgement.expires_at,
                minutes_until_expiry: acknowledgement
                    .expires_at
                    .signed_duration_since(now)
                    .num_minutes(),
                status,
                review_expired_excluded: acknowledgement.review_expired_excluded,
                suggested_action,
            }
        })
        .collect();

    WikiRuntimePolicyAckReport {
        generated_at: now,
        near_expiry_hours,
        query,
        budget,
        decision,
        summary,
        acknowledgements,
    }
}

pub(super) fn present_runtime_policy_acknowledgements(
    acknowledgements: &[AdaptiveWikiRuntimePolicyAcknowledgement],
    json: bool,
) -> Result<()> {
    if json {
        let value = operator_safe_json_value(serde_json::to_value(acknowledgements)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    if acknowledgements.is_empty() {
        println!("No adaptive wiki runtime policy acknowledgements found.");
        return Ok(());
    }

    println!(
        "{:<48} {:<18} {:<22} {:<22} POLICY",
        "ID", "SCOPE_MODE", "CREATED_AT", "EXPIRES_AT"
    );
    for acknowledgement in acknowledgements {
        println!(
            "{:<48} {:<18} {:<22} {:<22} review_expired={:?}",
            acknowledgement.id,
            runtime_ack_scope_mode_label(acknowledgement.scope_mode),
            acknowledgement.created_at,
            acknowledgement.expires_at,
            acknowledgement.policy.review_expired
        );
    }
    Ok(())
}

pub(super) fn present_runtime_policy_acknowledgement(
    acknowledgement: &AdaptiveWikiRuntimePolicyAcknowledgement,
    json: bool,
) -> Result<()> {
    if json {
        let value = operator_safe_json_value(serde_json::to_value(acknowledgement)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    println!(
        "Recorded adaptive wiki runtime policy acknowledgement {}",
        acknowledgement.id
    );
    println!(
        "  policy: review_expired={:?}",
        acknowledgement.policy.review_expired
    );
    println!(
        "  scope_mode: {}",
        runtime_ack_scope_mode_label(acknowledgement.scope_mode)
    );
    println!("  comparison_hash: {}", acknowledgement.comparison_hash);
    println!("  expires_at: {}", acknowledgement.expires_at);
    if !acknowledgement.review_expired_excluded.is_empty() {
        println!(
            "  review_expired_excluded: {}",
            acknowledgement.review_expired_excluded.join(", ")
        );
    }
    if !acknowledgement.selected_only_in_warn.is_empty() {
        println!(
            "  selected_only_in_warn: {}",
            acknowledgement.selected_only_in_warn.join(", ")
        );
    }
    if !acknowledgement.selected_only_in_strict.is_empty() {
        println!(
            "  selected_only_in_strict: {}",
            acknowledgement.selected_only_in_strict.join(", ")
        );
    }
    if !acknowledgement.reason.trim().is_empty() {
        println!("  reason: {}", acknowledgement.reason);
    }
    Ok(())
}

pub(super) fn present_runtime_policy_ack_report(
    report: &WikiRuntimePolicyAckReport,
    json: bool,
) -> Result<()> {
    if json {
        let value = operator_safe_json_value(serde_json::to_value(report)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    println!("Adaptive wiki runtime policy acknowledgement report");
    println!(
        "  total: {}  active: {}  near_expiry: {}  expired: {}",
        report.summary.total,
        report.summary.active,
        report.summary.near_expiry,
        report.summary.expired
    );
    if let Some(decision) = report.decision.as_ref() {
        println!(
            "  query_decision: {:?} ack={} reason={}",
            decision.status,
            decision.acknowledgement_id.as_deref().unwrap_or("-"),
            decision.reason
        );
    }
    if report.acknowledgements.is_empty() {
        println!("No adaptive wiki runtime policy acknowledgements found.");
        return Ok(());
    }
    println!(
        "{:<48} {:<18} {:<34} {:<22} STATUS",
        "ID", "SCOPE_MODE", "QUERY", "EXPIRES_AT"
    );
    for acknowledgement in &report.acknowledgements {
        println!(
            "{:<48} {:<18} {:<34} {:<22} {}",
            acknowledgement.id,
            runtime_ack_scope_mode_label(acknowledgement.scope_mode),
            runtime_ack_query_label(&acknowledgement.query),
            acknowledgement.expires_at,
            acknowledgement.status.join(",")
        );
        if let Some(action) = acknowledgement.suggested_action.as_ref() {
            println!("  suggested_action: {}", action.kind);
            println!("    {}", action.detail);
            println!("    compare: {}", action.compare_command_template);
            println!("    ack:     {}", action.ack_command_template);
        }
    }
    Ok(())
}

fn runtime_policy_ack_suggested_action(
    acknowledgement: &AdaptiveWikiRuntimePolicyAcknowledgement,
    expired: bool,
    near_expiry: bool,
    query_status: Option<AdaptiveWikiRuntimePolicyDecisionStatus>,
    report_query: Option<&AdaptiveWikiQuery>,
    report_budget: Option<&AdaptiveWikiProjectionBudget>,
) -> Option<WikiRuntimePolicyAckSuggestedAction> {
    match query_status {
        Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedScopeModeBlocked) => {
            let query = report_query.unwrap_or(&acknowledgement.query);
            let budget = report_budget.unwrap_or(&acknowledgement.budget);
            Some(WikiRuntimePolicyAckSuggestedAction {
                kind: "record_exact_query_acknowledgement".to_string(),
                detail: "Project/artifact acknowledgement cannot apply while session-scoped projection entries are present; review the exact query comparison and append a new exact-query acknowledgement.".to_string(),
                compare_command_template: runtime_policy_compare_command_template(query, budget),
                ack_command_template: runtime_policy_ack_command_template(
                    query,
                    budget,
                    AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery,
                ),
            })
        }
        Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedStaleAcknowledgement) => {
            Some(WikiRuntimePolicyAckSuggestedAction {
                kind: "recompare_and_append_acknowledgement".to_string(),
                detail: "The current strict runtime comparison no longer matches this acknowledgement hash; review the comparison again and append a new acknowledgement.".to_string(),
                compare_command_template: runtime_policy_compare_command_template(
                    &acknowledgement.query,
                    &acknowledgement.budget,
                ),
                ack_command_template: runtime_policy_ack_command_template(
                    &acknowledgement.query,
                    &acknowledgement.budget,
                    acknowledgement.scope_mode,
                ),
            })
        }
        Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedExpiredAcknowledgement) => {
            Some(WikiRuntimePolicyAckSuggestedAction {
                kind: "recompare_and_append_acknowledgement".to_string(),
                detail: "The matching strict runtime acknowledgement is expired; review the comparison again and append a new acknowledgement instead of extending the old record.".to_string(),
                compare_command_template: runtime_policy_compare_command_template(
                    &acknowledgement.query,
                    &acknowledgement.budget,
                ),
                ack_command_template: runtime_policy_ack_command_template(
                    &acknowledgement.query,
                    &acknowledgement.budget,
                    acknowledgement.scope_mode,
                ),
            })
        }
        _ if expired => Some(WikiRuntimePolicyAckSuggestedAction {
            kind: "recompare_and_append_acknowledgement".to_string(),
            detail: "This acknowledgement is expired; review the comparison again and append a new acknowledgement instead of extending the old record.".to_string(),
            compare_command_template: runtime_policy_compare_command_template(
                &acknowledgement.query,
                &acknowledgement.budget,
            ),
            ack_command_template: runtime_policy_ack_command_template(
                &acknowledgement.query,
                &acknowledgement.budget,
                acknowledgement.scope_mode,
            ),
        }),
        _ if near_expiry => Some(WikiRuntimePolicyAckSuggestedAction {
            kind: "review_before_expiry".to_string(),
            detail: "This acknowledgement is near expiry; review the comparison before it expires and append a new acknowledgement if strict runtime should continue.".to_string(),
            compare_command_template: runtime_policy_compare_command_template(
                &acknowledgement.query,
                &acknowledgement.budget,
            ),
            ack_command_template: runtime_policy_ack_command_template(
                &acknowledgement.query,
                &acknowledgement.budget,
                acknowledgement.scope_mode,
            ),
        }),
        _ => None,
    }
}

fn runtime_policy_compare_command_template(
    query: &AdaptiveWikiQuery,
    budget: &AdaptiveWikiProjectionBudget,
) -> String {
    let mut parts = vec![
        "forager".to_string(),
        "offdesk".to_string(),
        "wiki".to_string(),
        "projection".to_string(),
    ];
    append_runtime_policy_query_args(&mut parts, query);
    if query.agent_mode_filter == AdaptiveWikiAgentModeFilter::SharedWhenUnspecified {
        parts.push("--runtime-agent-mode-default".to_string());
    }
    append_runtime_policy_budget_args(&mut parts, budget);
    parts.push("--compare-review-expired-policy".to_string());
    parts.push("--json".to_string());
    parts.join(" ")
}

fn runtime_policy_ack_command_template(
    query: &AdaptiveWikiQuery,
    budget: &AdaptiveWikiProjectionBudget,
    scope_mode: AdaptiveWikiRuntimePolicyAckScopeMode,
) -> String {
    let mut parts = vec![
        "forager".to_string(),
        "offdesk".to_string(),
        "wiki".to_string(),
        "ack-runtime-policy".to_string(),
    ];
    if scope_mode != AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery {
        parts.push("--scope-mode".to_string());
        parts.push(shell_quote_arg(runtime_ack_scope_mode_cli_value(
            scope_mode,
        )));
    }
    append_runtime_policy_query_args(&mut parts, query);
    append_runtime_policy_budget_args(&mut parts, budget);
    parts.push("--reason".to_string());
    parts.push("<reason>".to_string());
    parts
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn append_runtime_policy_query_args(parts: &mut Vec<String>, query: &AdaptiveWikiQuery) {
    if let Some(session_id) = query.session_id.as_deref() {
        parts.push("--session-id".to_string());
        parts.push(shell_quote_arg(session_id));
    }
    if let Some(project_key) = query.project_key.as_deref() {
        parts.push("--project-key".to_string());
        parts.push(shell_quote_arg(project_key));
    }
    if let Some(artifact_kind) = query.artifact_kind.as_deref() {
        parts.push("--artifact-kind".to_string());
        parts.push(shell_quote_arg(artifact_kind));
    }
    if let Some(agent_mode) = query.agent_mode {
        parts.push("--agent-mode".to_string());
        parts.push(shell_quote_arg(adaptive_wiki_agent_mode_cli_value(
            agent_mode,
        )));
    }
}

fn append_runtime_policy_budget_args(
    parts: &mut Vec<String>,
    budget: &AdaptiveWikiProjectionBudget,
) {
    let default = AdaptiveWikiProjectionBudget::default();
    if budget.max_entries != default.max_entries {
        parts.push("--max-entries".to_string());
        parts.push(budget.max_entries.to_string());
    }
    if budget.max_context_chars != default.max_context_chars {
        parts.push("--max-context-chars".to_string());
        parts.push(budget.max_context_chars.to_string());
    }
    if budget.max_instruction_chars != default.max_instruction_chars {
        parts.push("--max-instruction-chars".to_string());
        parts.push(budget.max_instruction_chars.to_string());
    }
}

fn runtime_ack_scope_mode_cli_value(mode: AdaptiveWikiRuntimePolicyAckScopeMode) -> &'static str {
    match mode {
        AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery => "exact-query",
        AdaptiveWikiRuntimePolicyAckScopeMode::ProjectArtifact => "project-artifact",
    }
}

fn runtime_ack_scope_mode_label(mode: AdaptiveWikiRuntimePolicyAckScopeMode) -> &'static str {
    match mode {
        AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery => "exact_query",
        AdaptiveWikiRuntimePolicyAckScopeMode::ProjectArtifact => "project_artifact",
    }
}

fn runtime_ack_query_label(query: &AdaptiveWikiQuery) -> String {
    let session = query.session_id.as_deref().unwrap_or("-");
    let project = query.project_key.as_deref().unwrap_or("-");
    let artifact = query.artifact_kind.as_deref().unwrap_or("-");
    let agent_mode = query
        .agent_mode
        .map(adaptive_wiki_agent_mode_cli_value)
        .unwrap_or("-");
    format!("s:{session} p:{project} a:{artifact} m:{agent_mode}")
}
