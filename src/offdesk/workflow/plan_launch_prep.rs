//! Typed Offdesk launch-preparation selection and packet construction.
//!
//! CLI adapters load review records, allocate append-only paths, and persist
//! packets. This module owns review selection, eligibility, and packet policy.

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::offdesk::operator_safe_text;

use super::plan_review::{
    offdesk_plan_review_denials, OffdeskPlanReviewDecision, OffdeskPlanReviewRecord,
};

pub const OFFDESK_PLAN_LAUNCH_PREP_SCHEMA: &str = "offdesk_plan_launch_prep.v1";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OffdeskPlanLaunchPrepPacket {
    pub schema: String,
    pub prepared_at: DateTime<Utc>,
    pub prep_id: String,
    pub plan_id: String,
    pub forager_profile: String,
    pub prepared_by: String,
    pub registration_path: String,
    pub source_path: String,
    pub source_sha256: String,
    pub review_id: String,
    pub review_decision: OffdeskPlanReviewDecision,
    pub review_record_json: String,
    pub artifact_kind: String,
    pub plan_schema: String,
    pub profile_key: Option<String>,
    pub project_key: Option<String>,
    pub request_id: Option<String>,
    pub task_id: Option<String>,
    pub selected_plan_path: Option<String>,
    pub required_first_reads: Vec<String>,
    pub launch_preparation_candidate: bool,
    pub ready_for_launch: bool,
    pub ready_for_enqueue: bool,
    pub next_safe_action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    pub read_only_project_state: bool,
    pub applies_file_operations: bool,
    pub artifacts: OffdeskPlanLaunchPrepArtifacts,
    pub does_not_authorize: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OffdeskPlanLaunchPrepArtifacts {
    pub registration_json: String,
    pub copied_source_json: Option<String>,
    pub review_record_json: String,
    pub launch_prep_json: String,
}

pub struct OffdeskPlanLaunchPrepBuildInput<'a> {
    pub prepared_at: DateTime<Utc>,
    pub prep_id: &'a str,
    pub plan_id: &'a str,
    pub forager_profile: &'a str,
    pub prepared_by: &'a str,
    pub registration_path: &'a str,
    pub source_path: &'a str,
    pub source_sha256: &'a str,
    pub review: &'a OffdeskPlanReviewRecord,
    pub artifact_kind: &'a str,
    pub plan_schema: &'a str,
    pub profile_key: Option<&'a str>,
    pub project_key: Option<&'a str>,
    pub request_id: Option<&'a str>,
    pub task_id: Option<&'a str>,
    pub selected_plan_path: Option<&'a str>,
    pub copied_source_json: Option<&'a str>,
    pub notes: Option<&'a str>,
    pub launch_prep_json: &'a str,
}

pub fn select_offdesk_plan_review<'a>(
    reviews: &'a [OffdeskPlanReviewRecord],
    review_id: Option<&str>,
) -> Result<&'a OffdeskPlanReviewRecord> {
    if let Some(review_id) = review_id {
        return reviews
            .iter()
            .find(|review| review.review_id == review_id)
            .ok_or_else(|| anyhow::anyhow!("Offdesk plan review not found: {}", review_id));
    }
    reviews
        .last()
        .ok_or_else(|| anyhow::anyhow!("Offdesk plan launch-prep requires an approved review"))
}

pub fn validate_offdesk_plan_launch_prep(
    review: &OffdeskPlanReviewRecord,
    registration_source_sha256: &str,
) -> Result<()> {
    if review.decision != OffdeskPlanReviewDecision::Approved {
        bail!(
            "Offdesk plan launch-prep requires an approved review; latest review {} is {}",
            review.review_id,
            review.decision.as_str()
        );
    }
    if !review.ready_for_launch_preparation_candidate {
        bail!(
            "Offdesk plan review {} is not a launch-preparation candidate",
            review.review_id
        );
    }
    if review.source_sha256 != registration_source_sha256 {
        bail!(
            "Offdesk plan review {} source hash does not match registration",
            review.review_id
        );
    }
    Ok(())
}

pub fn build_offdesk_plan_launch_prep_packet(
    input: OffdeskPlanLaunchPrepBuildInput<'_>,
) -> Result<OffdeskPlanLaunchPrepPacket> {
    validate_offdesk_plan_launch_prep(input.review, input.source_sha256)?;

    let mut required_first_reads = vec![
        input.registration_path.to_string(),
        input.review.artifacts.review_record_json.clone(),
    ];
    if let Some(path) = input.copied_source_json {
        required_first_reads.push(path.to_string());
    }
    if let Some(path) = input.selected_plan_path {
        if !required_first_reads.iter().any(|existing| existing == path) {
            required_first_reads.push(path.to_string());
        }
    }

    Ok(OffdeskPlanLaunchPrepPacket {
        schema: OFFDESK_PLAN_LAUNCH_PREP_SCHEMA.to_string(),
        prepared_at: input.prepared_at,
        prep_id: input.prep_id.to_string(),
        plan_id: input.plan_id.to_string(),
        forager_profile: operator_safe_text(input.forager_profile),
        prepared_by: operator_safe_text(input.prepared_by.trim()),
        registration_path: input.registration_path.to_string(),
        source_path: input.source_path.to_string(),
        source_sha256: input.source_sha256.to_string(),
        review_id: input.review.review_id.clone(),
        review_decision: input.review.decision,
        review_record_json: input.review.artifacts.review_record_json.clone(),
        artifact_kind: input.artifact_kind.to_string(),
        plan_schema: input.plan_schema.to_string(),
        profile_key: input.profile_key.map(ToOwned::to_owned),
        project_key: input.project_key.map(ToOwned::to_owned),
        request_id: input.request_id.map(ToOwned::to_owned),
        task_id: input.task_id.map(ToOwned::to_owned),
        selected_plan_path: input.selected_plan_path.map(ToOwned::to_owned),
        required_first_reads,
        launch_preparation_candidate: true,
        ready_for_launch: false,
        ready_for_enqueue: false,
        next_safe_action: "build_execution_brief_then_use_existing_offdesk_gate".to_string(),
        notes: input
            .notes
            .map(|value| truncate_text(&operator_safe_text(value), 2000)),
        read_only_project_state: true,
        applies_file_operations: false,
        artifacts: OffdeskPlanLaunchPrepArtifacts {
            registration_json: input.registration_path.to_string(),
            copied_source_json: input.copied_source_json.map(ToOwned::to_owned),
            review_record_json: input.review.artifacts.review_record_json.clone(),
            launch_prep_json: input.launch_prep_json.to_string(),
        },
        does_not_authorize: offdesk_plan_launch_prep_denials(),
    })
}

pub fn offdesk_plan_launch_prep_denials() -> Vec<String> {
    let mut denials = offdesk_plan_review_denials();
    denials.push("dispatch".to_string());
    denials
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
    use crate::offdesk::{OffdeskPlanReviewArtifacts, OFFDESK_PLAN_REVIEW_SCHEMA};

    fn review(
        review_id: &str,
        decision: OffdeskPlanReviewDecision,
        candidate: bool,
        source_sha256: &str,
    ) -> OffdeskPlanReviewRecord {
        OffdeskPlanReviewRecord {
            schema: OFFDESK_PLAN_REVIEW_SCHEMA.to_string(),
            reviewed_at: "2026-08-13T01:00:00Z".parse().expect("valid timestamp"),
            review_id: review_id.to_string(),
            plan_id: "plan_123".to_string(),
            forager_profile: "forager-ops".to_string(),
            registration_path: "/tmp/registration.json".to_string(),
            source_sha256: source_sha256.to_string(),
            decision,
            reviewer: "operator".to_string(),
            review_provider: None,
            review_file: None,
            reason: "reviewed".to_string(),
            blockers: Vec::new(),
            followups: Vec::new(),
            ready_for_launch_preparation_candidate: candidate,
            ready_for_enqueue: false,
            read_only_project_state: true,
            applies_file_operations: false,
            artifacts: OffdeskPlanReviewArtifacts {
                registration_json: "/tmp/registration.json".to_string(),
                copied_source_json: Some("/tmp/source.json".to_string()),
                review_record_json: format!("/tmp/{review_id}.json"),
            },
            does_not_authorize: offdesk_plan_review_denials(),
        }
    }

    fn build_input<'a>(review: &'a OffdeskPlanReviewRecord) -> OffdeskPlanLaunchPrepBuildInput<'a> {
        OffdeskPlanLaunchPrepBuildInput {
            prepared_at: "2026-08-13T02:00:00Z".parse().expect("valid timestamp"),
            prep_id: "plan_launch_prep_12345678",
            plan_id: "plan_123",
            forager_profile: "forager-ops",
            prepared_by: " operator ",
            registration_path: "/tmp/registration.json",
            source_path: "/workspace/OVERNIGHT_PLAN.json",
            source_sha256: "abc123",
            review,
            artifact_kind: "offdesk_multiturn_plan",
            plan_schema: "offdesk_multiturn_plan.v1",
            profile_key: Some("generic"),
            project_key: Some("project"),
            request_id: Some("request"),
            task_id: Some("task"),
            selected_plan_path: Some("/tmp/selected.json"),
            copied_source_json: Some("/tmp/source.json"),
            notes: Some("Prepare the execution brief next."),
            launch_prep_json: "/tmp/launch_prep.json",
        }
    }

    #[test]
    fn selects_specific_or_latest_review() {
        let first = review(
            "review_first",
            OffdeskPlanReviewDecision::RevisionRequired,
            false,
            "abc123",
        );
        let latest = review(
            "review_latest",
            OffdeskPlanReviewDecision::Approved,
            true,
            "abc123",
        );
        let reviews = vec![first, latest];

        assert_eq!(
            select_offdesk_plan_review(&reviews, Some("review_first"))
                .expect("specific review")
                .review_id,
            "review_first"
        );
        assert_eq!(
            select_offdesk_plan_review(&reviews, None)
                .expect("latest review")
                .review_id,
            "review_latest"
        );
    }

    #[test]
    fn selection_reports_missing_review_contracts() {
        assert_eq!(
            select_offdesk_plan_review(&[], None)
                .expect_err("review is required")
                .to_string(),
            "Offdesk plan launch-prep requires an approved review"
        );
        assert_eq!(
            select_offdesk_plan_review(&[], Some("review_missing"))
                .expect_err("specific review must exist")
                .to_string(),
            "Offdesk plan review not found: review_missing"
        );
    }

    #[test]
    fn validation_requires_approved_candidate_with_matching_hash() {
        let rejected = review(
            "review_rejected",
            OffdeskPlanReviewDecision::Rejected,
            false,
            "abc123",
        );
        assert_eq!(
            validate_offdesk_plan_launch_prep(&rejected, "abc123")
                .expect_err("rejected review must fail")
                .to_string(),
            "Offdesk plan launch-prep requires an approved review; latest review review_rejected is rejected"
        );

        let not_candidate = review(
            "review_not_candidate",
            OffdeskPlanReviewDecision::Approved,
            false,
            "abc123",
        );
        assert_eq!(
            validate_offdesk_plan_launch_prep(&not_candidate, "abc123")
                .expect_err("non-candidate review must fail")
                .to_string(),
            "Offdesk plan review review_not_candidate is not a launch-preparation candidate"
        );

        let stale = review(
            "review_stale",
            OffdeskPlanReviewDecision::Approved,
            true,
            "old_hash",
        );
        assert_eq!(
            validate_offdesk_plan_launch_prep(&stale, "abc123")
                .expect_err("stale review must fail")
                .to_string(),
            "Offdesk plan review review_stale source hash does not match registration"
        );
    }

    #[test]
    fn builds_read_only_packet_with_ordered_first_reads() {
        let review = review(
            "review_approved",
            OffdeskPlanReviewDecision::Approved,
            true,
            "abc123",
        );

        let packet =
            build_offdesk_plan_launch_prep_packet(build_input(&review)).expect("launch packet");

        assert_eq!(packet.schema, OFFDESK_PLAN_LAUNCH_PREP_SCHEMA);
        assert_eq!(packet.prepared_by, "operator");
        assert_eq!(packet.review_id, "review_approved");
        assert_eq!(
            packet.required_first_reads,
            vec![
                "/tmp/registration.json",
                "/tmp/review_approved.json",
                "/tmp/source.json",
                "/tmp/selected.json"
            ]
        );
        assert!(packet.launch_preparation_candidate);
        assert!(!packet.ready_for_launch);
        assert!(!packet.ready_for_enqueue);
        assert!(packet.read_only_project_state);
        assert!(!packet.applies_file_operations);
        assert!(packet.does_not_authorize.contains(&"dispatch".to_string()));
    }

    #[test]
    fn selected_plan_path_is_not_duplicated_and_notes_are_bounded() {
        let review = review(
            "review_approved",
            OffdeskPlanReviewDecision::Approved,
            true,
            "abc123",
        );
        let mut input = build_input(&review);
        input.selected_plan_path = Some("/tmp/source.json");
        let long_notes = "a".repeat(2001);
        input.notes = Some(&long_notes);

        let packet = build_offdesk_plan_launch_prep_packet(input).expect("launch packet");

        assert_eq!(
            packet.required_first_reads,
            vec![
                "/tmp/registration.json",
                "/tmp/review_approved.json",
                "/tmp/source.json"
            ]
        );
        let notes = packet.notes.expect("bounded notes");
        assert_eq!(notes.chars().count(), 2014);
        assert!(notes.ends_with("...[truncated]"));
    }
}
