//! Typed Offdesk plan-review validation and record construction.
//!
//! CLI adapters resolve registry entries, allocate append-only paths, and
//! persist records. This module owns review input policy and authority limits.

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

use crate::offdesk::operator_safe_text;

pub const OFFDESK_PLAN_REVIEW_SCHEMA: &str = "offdesk_plan_review.v1";
pub const OFFDESK_PLAN_REQUIRED_DENIALS: [&str; 8] = [
    "enqueue",
    "launch",
    "approval",
    "file movement",
    "archive",
    "delete",
    "wiki promotion",
    "accepted truth",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum OffdeskPlanReviewDecision {
    Approved,
    RevisionRequired,
    Rejected,
}

impl OffdeskPlanReviewDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::RevisionRequired => "revision_required",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OffdeskPlanReviewRecord {
    pub schema: String,
    pub reviewed_at: DateTime<Utc>,
    pub review_id: String,
    pub plan_id: String,
    pub forager_profile: String,
    pub registration_path: String,
    pub source_sha256: String,
    pub decision: OffdeskPlanReviewDecision,
    pub reviewer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review_provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review_file: Option<String>,
    pub reason: String,
    pub blockers: Vec<String>,
    pub followups: Vec<String>,
    pub ready_for_launch_preparation_candidate: bool,
    pub ready_for_enqueue: bool,
    pub read_only_project_state: bool,
    pub applies_file_operations: bool,
    pub artifacts: OffdeskPlanReviewArtifacts,
    pub does_not_authorize: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OffdeskPlanReviewArtifacts {
    pub registration_json: String,
    pub copied_source_json: Option<String>,
    pub review_record_json: String,
}

pub struct OffdeskPlanReviewBuildInput<'a> {
    pub reviewed_at: DateTime<Utc>,
    pub review_id: &'a str,
    pub plan_id: &'a str,
    pub forager_profile: &'a str,
    pub registration_path: &'a str,
    pub source_sha256: &'a str,
    pub decision: OffdeskPlanReviewDecision,
    pub reviewer: &'a str,
    pub review_provider: Option<&'a str>,
    pub review_file: Option<String>,
    pub reason: &'a str,
    pub blockers: &'a [String],
    pub followups: &'a [String],
    pub registration_ready_for_operator_review: bool,
    pub registration_ready_for_launch_preparation: bool,
    pub registration_ready_for_enqueue: bool,
    pub registration_validation_failures: &'a [String],
    pub copied_source_json: Option<&'a str>,
    pub review_record_json: &'a str,
}

pub fn validate_offdesk_plan_review_input(
    decision: OffdeskPlanReviewDecision,
    reason: &str,
    blockers: &[String],
) -> Result<()> {
    if reason.trim().is_empty() {
        bail!("Offdesk plan review reason is required");
    }
    if decision == OffdeskPlanReviewDecision::Approved && !blockers.is_empty() {
        bail!("approved Offdesk plan review cannot include blockers");
    }
    Ok(())
}

pub fn build_offdesk_plan_review_record(
    input: OffdeskPlanReviewBuildInput<'_>,
) -> Result<OffdeskPlanReviewRecord> {
    validate_offdesk_plan_review_input(input.decision, input.reason, input.blockers)?;

    let ready_for_launch_preparation_candidate = input.decision
        == OffdeskPlanReviewDecision::Approved
        && input.registration_ready_for_operator_review
        && !input.registration_ready_for_launch_preparation
        && !input.registration_ready_for_enqueue
        && input.registration_validation_failures.is_empty();

    Ok(OffdeskPlanReviewRecord {
        schema: OFFDESK_PLAN_REVIEW_SCHEMA.to_string(),
        reviewed_at: input.reviewed_at,
        review_id: input.review_id.to_string(),
        plan_id: input.plan_id.to_string(),
        forager_profile: operator_safe_text(input.forager_profile),
        registration_path: input.registration_path.to_string(),
        source_sha256: input.source_sha256.to_string(),
        decision: input.decision,
        reviewer: operator_safe_text(input.reviewer.trim()),
        review_provider: input
            .review_provider
            .map(|value| operator_safe_text(value.trim())),
        review_file: input.review_file.map(|path| operator_safe_text(&path)),
        reason: truncate_text(&operator_safe_text(input.reason.trim()), 2000),
        blockers: safe_text_list(input.blockers),
        followups: safe_text_list(input.followups),
        ready_for_launch_preparation_candidate,
        ready_for_enqueue: false,
        read_only_project_state: true,
        applies_file_operations: false,
        artifacts: OffdeskPlanReviewArtifacts {
            registration_json: input.registration_path.to_string(),
            copied_source_json: input.copied_source_json.map(ToOwned::to_owned),
            review_record_json: input.review_record_json.to_string(),
        },
        does_not_authorize: offdesk_plan_review_denials(),
    })
}

pub fn offdesk_plan_registration_denials() -> Vec<String> {
    OFFDESK_PLAN_REQUIRED_DENIALS
        .iter()
        .map(|denial| (*denial).to_string())
        .collect()
}

pub fn offdesk_plan_review_denials() -> Vec<String> {
    let mut denials = offdesk_plan_registration_denials();
    denials.push("launch preparation without a separate command".to_string());
    denials
}

fn safe_text_list(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| operator_safe_text(value.trim()))
        .filter(|value| !value.is_empty())
        .collect()
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        value.to_string()
    } else {
        format!(
            "{}...[truncated]",
            value.chars().take(max_chars).collect::<String>()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_input<'a>(
        blockers: &'a [String],
        followups: &'a [String],
        failures: &'a [String],
    ) -> OffdeskPlanReviewBuildInput<'a> {
        OffdeskPlanReviewBuildInput {
            reviewed_at: "2026-08-13T01:02:03Z".parse().expect("valid timestamp"),
            review_id: "plan_review_12345678",
            plan_id: "plan_123",
            forager_profile: "forager-ops",
            registration_path: "/tmp/registration.json",
            source_sha256: "abc123",
            decision: OffdeskPlanReviewDecision::Approved,
            reviewer: " operator ",
            review_provider: Some(" codex "),
            review_file: Some("/tmp/review.txt".to_string()),
            reason: " Ready for the separate preparation step. ",
            blockers,
            followups,
            registration_ready_for_operator_review: true,
            registration_ready_for_launch_preparation: false,
            registration_ready_for_enqueue: false,
            registration_validation_failures: failures,
            copied_source_json: Some("/tmp/source.json"),
            review_record_json: "/tmp/review.json",
        }
    }

    #[test]
    fn builds_normalized_read_only_review_record() {
        let blockers = Vec::new();
        let followups = vec!["  prepare packet  ".to_string(), "   ".to_string()];
        let failures = Vec::new();

        let record =
            build_offdesk_plan_review_record(build_input(&blockers, &followups, &failures))
                .expect("review record");

        assert_eq!(record.schema, OFFDESK_PLAN_REVIEW_SCHEMA);
        assert_eq!(record.reviewer, "operator");
        assert_eq!(record.review_provider.as_deref(), Some("codex"));
        assert_eq!(record.reason, "Ready for the separate preparation step.");
        assert_eq!(record.followups, vec!["prepare packet"]);
        assert!(record.ready_for_launch_preparation_candidate);
        assert!(!record.ready_for_enqueue);
        assert!(record.read_only_project_state);
        assert!(!record.applies_file_operations);
        assert!(record
            .does_not_authorize
            .contains(&"launch preparation without a separate command".to_string()));
    }

    #[test]
    fn rejects_empty_reason_and_approved_blockers() {
        let blocker = vec!["needs revision".to_string()];

        assert_eq!(
            validate_offdesk_plan_review_input(
                OffdeskPlanReviewDecision::RevisionRequired,
                "   ",
                &[]
            )
            .expect_err("empty reason must fail")
            .to_string(),
            "Offdesk plan review reason is required"
        );
        assert_eq!(
            validate_offdesk_plan_review_input(
                OffdeskPlanReviewDecision::Approved,
                "reviewed",
                &blocker
            )
            .expect_err("approved blockers must fail")
            .to_string(),
            "approved Offdesk plan review cannot include blockers"
        );
    }

    #[test]
    fn readiness_requires_clean_registration_and_truncates_reason() {
        let blockers = Vec::new();
        let followups = Vec::new();
        let failures = vec!["authority_missing:launch".to_string()];
        let mut input = build_input(&blockers, &followups, &failures);
        let long_reason = "a".repeat(2001);
        input.reason = &long_reason;

        let record = build_offdesk_plan_review_record(input).expect("review record");

        assert!(!record.ready_for_launch_preparation_candidate);
        assert_eq!(record.reason.chars().count(), 2014);
        assert!(record.reason.ends_with("...[truncated]"));
    }

    #[test]
    fn decision_uses_snake_case_json_contract() {
        let value = serde_json::to_value(OffdeskPlanReviewDecision::RevisionRequired)
            .expect("serialize decision");
        assert_eq!(value, serde_json::json!("revision_required"));
    }
}
