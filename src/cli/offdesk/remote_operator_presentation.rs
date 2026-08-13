//! Read-only Remote Operator projection and presentation adapter.
//!
//! Callers retain status and approval queries. This module converts their
//! observations into operator-safe payloads, cards, and terminal output.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{RemoteOperatorPendingArgs, RemoteOperatorStatusArgs};
use crate::offdesk::{
    operator_safe_text, ApprovalStatus, OffdeskNextSafeAction, OffdeskPendingApprovalView,
    RiskLevel,
};

#[derive(Serialize)]
pub(super) struct RemoteOperatorProjection<T>
where
    T: Serialize,
{
    schema: String,
    generated_at: DateTime<Utc>,
    forager_profile: String,
    transport: String,
    source_surface: String,
    command: String,
    phase: String,
    read_only: bool,
    mutation_authorized: bool,
    approval_authorized: bool,
    allowed_remote_intents: Vec<String>,
    forbidden_remote_intents: Vec<String>,
    card: RemoteOperatorCard,
    payload: T,
}

#[derive(Clone, Serialize)]
pub(super) struct RemoteOperatorCard {
    pub(super) title: String,
    pub(super) summary_lines: Vec<String>,
    pub(super) detail_lines: Vec<String>,
    pub(super) observed_hash: String,
    pub(super) remote_actions: Vec<String>,
    pub(super) disabled_remote_actions: Vec<String>,
}

#[derive(Serialize)]
struct RemoteOperatorStatusPayload {
    profile: String,
    waiting: usize,
    running: usize,
    idle: usize,
    stopped: usize,
    error: usize,
    total: usize,
    resume_pending_fresh: usize,
    resume_pending_stale: usize,
    pending_approvals: usize,
    queued_offdesk_tasks: usize,
    active_offdesk_tasks: usize,
    offdesk_tasks_pending_approval: usize,
    failed_offdesk_tasks: usize,
    resume_pending_offdesk_tasks: usize,
    cancelled_offdesk_tasks: usize,
    stale_background_runs: usize,
    failed_background_runs: usize,
    closeout_required_offdesk_tasks: usize,
    next_safe_actions: Vec<RemoteOperatorNextSafeActionSummary>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorNextSafeActionSummary {
    kind: String,
    detail: String,
    requires_operator_review: bool,
}

#[derive(Serialize)]
struct RemoteOperatorPendingPayload {
    include_all: bool,
    approval_count: usize,
    approvals: Vec<RemoteOperatorApprovalSummary>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorApprovalSummaryCore {
    approval_id: String,
    action_id: String,
    status: ApprovalStatus,
    expired: bool,
    action: String,
    project_key: String,
    request_id: String,
    task_id: String,
    risk_level: RiskLevel,
    preview: String,
    reason: String,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    next_safe_action: RemoteOperatorNextSafeActionSummary,
    remote_actions: Vec<String>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorApprovalSummary {
    #[serde(flatten)]
    core: RemoteOperatorApprovalSummaryCore,
    observed_hash: String,
}

pub(super) fn present_remote_operator_status(
    profile: &str,
    args: &RemoteOperatorStatusArgs,
    status: &Value,
) -> Result<()> {
    let payload = remote_operator_status_payload(status);
    let observed_hash = observed_hash_for(&payload)?;
    let card = remote_operator_status_card(&payload, observed_hash);
    let projection = remote_operator_projection(profile, &args.transport, "status", card, payload);
    present_remote_operator_projection(&projection, args.json)
}

pub(super) fn present_remote_operator_pending(
    profile: &str,
    args: &RemoteOperatorPendingArgs,
    approval_views: &[OffdeskPendingApprovalView],
    observed_at: DateTime<Utc>,
) -> Result<()> {
    let approvals = approval_views
        .iter()
        .map(|view| remote_operator_approval_summary(view, observed_at))
        .collect::<Result<Vec<_>>>()?;
    let payload = RemoteOperatorPendingPayload {
        include_all: args.all,
        approval_count: approvals.len(),
        approvals,
    };
    let observed_hash = observed_hash_for(&payload)?;
    let card = remote_operator_pending_card(&payload, observed_hash);
    let projection = remote_operator_projection(profile, &args.transport, "pending", card, payload);
    present_remote_operator_projection(&projection, args.json)
}

pub(super) fn remote_operator_projection<T>(
    profile: &str,
    transport: &str,
    command: &str,
    card: RemoteOperatorCard,
    payload: T,
) -> RemoteOperatorProjection<T>
where
    T: Serialize,
{
    RemoteOperatorProjection {
        schema: "remote_operator_readonly_projection.v1".to_string(),
        generated_at: Utc::now(),
        forager_profile: operator_safe_text(profile),
        transport: operator_safe_text(transport),
        source_surface: format!("remote_operator.{}", operator_safe_text(transport)),
        command: command.to_string(),
        phase: "read_only_surface".to_string(),
        read_only: true,
        mutation_authorized: false,
        approval_authorized: false,
        allowed_remote_intents: vec![
            "inspect_status".to_string(),
            "inspect_pending".to_string(),
            "inspect_plans".to_string(),
            "inspect_plan".to_string(),
        ],
        forbidden_remote_intents: vec![
            "approve_plan".to_string(),
            "approve_launch".to_string(),
            "deny_launch".to_string(),
            "enqueue".to_string(),
            "launch".to_string(),
            "dispatch".to_string(),
            "shell".to_string(),
            "git_push".to_string(),
            "delete".to_string(),
            "provider_retarget".to_string(),
        ],
        card,
        payload,
    }
}

pub(super) fn remote_operator_card(
    title: impl Into<String>,
    summary_lines: Vec<String>,
    detail_lines: Vec<String>,
    observed_hash: String,
    remote_actions: Vec<String>,
) -> RemoteOperatorCard {
    RemoteOperatorCard {
        title: title.into(),
        summary_lines,
        detail_lines,
        observed_hash,
        remote_actions,
        disabled_remote_actions: vec![
            "approve_plan".to_string(),
            "approve_launch".to_string(),
            "deny_launch".to_string(),
            "enqueue".to_string(),
            "launch".to_string(),
            "dispatch".to_string(),
            "shell".to_string(),
        ],
    }
}

pub(super) fn present_remote_operator_projection<T>(
    projection: &RemoteOperatorProjection<T>,
    json: bool,
) -> Result<()>
where
    T: Serialize,
{
    if json {
        println!("{}", serde_json::to_string_pretty(projection)?);
        return Ok(());
    }

    println!("{}", projection.card.title);
    println!("  transport: {}", projection.transport);
    println!("  surface:   {}", projection.source_surface);
    println!("  mode:      read-only");
    println!("  hash:      {}", projection.card.observed_hash);
    for line in &projection.card.summary_lines {
        println!("  - {line}");
    }
    if !projection.card.detail_lines.is_empty() {
        println!("Details:");
        for line in &projection.card.detail_lines {
            println!("  - {line}");
        }
    }
    println!("  note: remote launch, dispatch, shell execution, and mutation are disabled");
    Ok(())
}

pub(super) fn observed_hash_for<T>(value: &T) -> Result<String>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn remote_operator_status_payload(status: &Value) -> RemoteOperatorStatusPayload {
    RemoteOperatorStatusPayload {
        profile: json_string_field(status, "profile").unwrap_or_else(|| "default".to_string()),
        waiting: json_usize_field(status, "waiting"),
        running: json_usize_field(status, "running"),
        idle: json_usize_field(status, "idle"),
        stopped: json_usize_field(status, "stopped"),
        error: json_usize_field(status, "error"),
        total: json_usize_field(status, "total"),
        resume_pending_fresh: json_usize_field(status, "resume_pending_fresh"),
        resume_pending_stale: json_usize_field(status, "resume_pending_stale"),
        pending_approvals: json_usize_field(status, "pending_approvals"),
        queued_offdesk_tasks: json_usize_field(status, "queued_offdesk_tasks"),
        active_offdesk_tasks: json_usize_field(status, "active_offdesk_tasks"),
        offdesk_tasks_pending_approval: json_usize_field(status, "offdesk_tasks_pending_approval"),
        failed_offdesk_tasks: json_usize_field(status, "failed_offdesk_tasks"),
        resume_pending_offdesk_tasks: json_usize_field(status, "resume_pending_offdesk_tasks"),
        cancelled_offdesk_tasks: json_usize_field(status, "cancelled_offdesk_tasks"),
        stale_background_runs: json_usize_field(status, "stale_background_runs"),
        failed_background_runs: json_usize_field(status, "failed_background_runs"),
        closeout_required_offdesk_tasks: json_usize_field(
            status,
            "closeout_required_offdesk_tasks",
        ),
        next_safe_actions: status
            .get("offdesk_next_safe_actions")
            .and_then(Value::as_array)
            .map(|actions| {
                actions
                    .iter()
                    .map(remote_operator_next_safe_action_from_value)
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn remote_operator_next_safe_action_from_value(
    value: &Value,
) -> RemoteOperatorNextSafeActionSummary {
    RemoteOperatorNextSafeActionSummary {
        kind: json_string_field(value, "kind").unwrap_or_else(|| "unknown".to_string()),
        detail: json_string_field(value, "detail")
            .map(|value| operator_safe_text(&value))
            .unwrap_or_else(|| "No detail provided.".to_string()),
        requires_operator_review: value
            .get("requires_operator_review")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

fn remote_operator_approval_summary(
    view: &OffdeskPendingApprovalView,
    observed_at: DateTime<Utc>,
) -> Result<RemoteOperatorApprovalSummary> {
    let approval = &view.approval;
    let core = RemoteOperatorApprovalSummaryCore {
        approval_id: operator_safe_text(&approval.approval_id),
        action_id: operator_safe_text(approval.action_id()),
        status: approval.status,
        expired: approval.status == ApprovalStatus::Pending && approval.expires_at < observed_at,
        action: operator_safe_text(&approval.action),
        project_key: operator_safe_text(&approval.project_key),
        request_id: operator_safe_text(&approval.request_id),
        task_id: operator_safe_text(&approval.task_id),
        risk_level: approval.risk_level,
        preview: operator_safe_text(&approval.preview),
        reason: operator_safe_text(&approval.reason),
        created_at: approval.created_at,
        expires_at: approval.expires_at,
        next_safe_action: remote_operator_next_safe_action_from_offdesk(&view.next_safe_action),
        remote_actions: vec!["inspect_approval".to_string()],
    };
    let observed_hash = observed_hash_for(&core)?;
    Ok(RemoteOperatorApprovalSummary {
        core,
        observed_hash,
    })
}

fn remote_operator_next_safe_action_from_offdesk(
    action: &OffdeskNextSafeAction,
) -> RemoteOperatorNextSafeActionSummary {
    RemoteOperatorNextSafeActionSummary {
        kind: operator_safe_text(&action.kind),
        detail: operator_safe_text(&action.detail),
        requires_operator_review: action.requires_operator_review,
    }
}

fn remote_operator_status_card(
    payload: &RemoteOperatorStatusPayload,
    observed_hash: String,
) -> RemoteOperatorCard {
    let detail_lines = payload
        .next_safe_actions
        .iter()
        .take(3)
        .map(|action| format!("next: {} ({})", action.detail, action.kind))
        .collect();
    remote_operator_card(
        "Forager Remote Status",
        vec![
            format!(
                "sessions: {} waiting / {} running / {} total",
                payload.waiting, payload.running, payload.total
            ),
            format!(
                "offdesk: {} pending approvals / {} queued / {} active / {} failed",
                payload.pending_approvals,
                payload.queued_offdesk_tasks,
                payload.active_offdesk_tasks + payload.offdesk_tasks_pending_approval,
                payload.failed_offdesk_tasks
            ),
            format!(
                "closeout required: {}",
                payload.closeout_required_offdesk_tasks
            ),
        ],
        detail_lines,
        observed_hash,
        vec!["inspect_status".to_string()],
    )
}

fn remote_operator_pending_card(
    payload: &RemoteOperatorPendingPayload,
    observed_hash: String,
) -> RemoteOperatorCard {
    let expired = payload
        .approvals
        .iter()
        .filter(|approval| approval.core.expired)
        .count();
    let detail_lines = payload
        .approvals
        .iter()
        .take(3)
        .map(|approval| {
            format!(
                "{}: {} {}",
                approval.core.approval_id,
                approval.core.action,
                approval_status_label(approval.core.status)
            )
        })
        .collect();
    remote_operator_card(
        "Forager Remote Pending",
        vec![
            format!("approvals: {}", payload.approval_count),
            format!("expired pending approvals: {expired}"),
            "remote launch and mutation remain disabled".to_string(),
        ],
        detail_lines,
        observed_hash,
        vec!["inspect_pending".to_string()],
    )
}

fn approval_status_label(status: ApprovalStatus) -> &'static str {
    match status {
        ApprovalStatus::Pending => "pending",
        ApprovalStatus::Approved => "approved",
        ApprovalStatus::Denied => "denied",
        ApprovalStatus::Expired => "expired",
        ApprovalStatus::Superseded => "superseded",
    }
}

fn json_string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
}

fn json_usize_field(value: &Value, field: &str) -> usize {
    value
        .get(field)
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn projection_keeps_every_mutating_intent_disabled() {
        let card = remote_operator_card(
            "Status",
            Vec::new(),
            Vec::new(),
            "sha256:test".to_string(),
            vec!["inspect_status".to_string()],
        );
        let projection = remote_operator_projection(
            "default",
            "telegram",
            "status",
            card,
            json!({"waiting": 0}),
        );

        assert!(projection.read_only);
        assert!(!projection.mutation_authorized);
        assert!(!projection.approval_authorized);
        assert!(projection
            .forbidden_remote_intents
            .iter()
            .any(|intent| intent == "dispatch"));
        assert!(projection
            .card
            .disabled_remote_actions
            .iter()
            .any(|action| action == "shell"));
    }

    #[test]
    fn status_payload_redacts_next_action_detail() {
        let payload = remote_operator_status_payload(&json!({
            "profile": "default",
            "waiting": 1,
            "offdesk_next_safe_actions": [{
                "kind": "operator_review",
                "detail": "inspect token=sk-secretsecretsecretsecret",
                "requires_operator_review": true
            }]
        }));

        assert_eq!(payload.waiting, 1);
        assert_eq!(payload.running, 0);
        assert_eq!(
            payload.next_safe_actions[0].detail,
            "inspect token=[REDACTED]"
        );
        assert!(payload.next_safe_actions[0].requires_operator_review);
    }
}
