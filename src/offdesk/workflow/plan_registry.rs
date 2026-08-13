//! Typed Offdesk plan-registry read models.
//!
//! CLI adapters traverse registry directories, parse stored JSON, and resolve
//! paths. This module combines the loaded registration, review, and launch-prep
//! records into stable list and detail projections.

use serde::Serialize;

use super::plan_launch_prep::OffdeskPlanLaunchPrepPacket;
use super::plan_registration::OffdeskPlanRegistration;
use super::plan_review::{OffdeskPlanReviewDecision, OffdeskPlanReviewRecord};

#[derive(Debug, Clone, Serialize)]
pub struct OffdeskPlanRegistryItem {
    pub plan_id: String,
    pub registration_path: String,
    pub registration: OffdeskPlanRegistration,
    pub review_state: OffdeskPlanReviewState,
    pub review_count: usize,
    pub latest_review: Option<OffdeskPlanReviewRecord>,
    pub launch_prep_count: usize,
    pub latest_launch_prep: Option<OffdeskPlanLaunchPrepPacket>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OffdeskPlanRegistryDetail {
    pub plan_id: String,
    pub registration_path: String,
    pub registration: OffdeskPlanRegistration,
    pub review_state: OffdeskPlanReviewState,
    pub review_count: usize,
    pub latest_review: Option<OffdeskPlanReviewRecord>,
    pub reviews: Vec<OffdeskPlanReviewRecord>,
    pub launch_prep_count: usize,
    pub latest_launch_prep: Option<OffdeskPlanLaunchPrepPacket>,
    pub launch_preps: Vec<OffdeskPlanLaunchPrepPacket>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OffdeskPlanReviewState {
    pub status: String,
    pub ready_for_launch_preparation_candidate: bool,
    pub next_safe_action: String,
    pub latest_review_id: Option<String>,
}

pub fn build_offdesk_plan_registry_item(
    plan_id: String,
    registration_path: String,
    registration: OffdeskPlanRegistration,
    reviews: &[OffdeskPlanReviewRecord],
    launch_preps: &[OffdeskPlanLaunchPrepPacket],
) -> OffdeskPlanRegistryItem {
    let latest_review = reviews.last().cloned();
    let latest_launch_prep = launch_preps.last().cloned();
    OffdeskPlanRegistryItem {
        plan_id,
        registration_path,
        registration,
        review_state: build_offdesk_plan_review_state(latest_review.as_ref()),
        review_count: reviews.len(),
        latest_review,
        launch_prep_count: launch_preps.len(),
        latest_launch_prep,
    }
}

pub fn build_offdesk_plan_registry_detail(
    item: OffdeskPlanRegistryItem,
    reviews: Vec<OffdeskPlanReviewRecord>,
    launch_preps: Vec<OffdeskPlanLaunchPrepPacket>,
) -> OffdeskPlanRegistryDetail {
    let latest_review = reviews.last().cloned();
    let latest_launch_prep = launch_preps.last().cloned();
    OffdeskPlanRegistryDetail {
        plan_id: item.plan_id,
        registration_path: item.registration_path,
        registration: item.registration,
        review_state: build_offdesk_plan_review_state(latest_review.as_ref()),
        review_count: reviews.len(),
        latest_review,
        reviews,
        launch_prep_count: launch_preps.len(),
        latest_launch_prep,
        launch_preps,
    }
}

pub fn build_offdesk_plan_review_state(
    latest_review: Option<&OffdeskPlanReviewRecord>,
) -> OffdeskPlanReviewState {
    let Some(review) = latest_review else {
        return OffdeskPlanReviewState {
            status: "unreviewed".to_string(),
            ready_for_launch_preparation_candidate: false,
            next_safe_action: "record_operator_review".to_string(),
            latest_review_id: None,
        };
    };
    let (status, next_safe_action) = match review.decision {
        OffdeskPlanReviewDecision::Approved => (
            "approved",
            if review.ready_for_launch_preparation_candidate {
                "prepare_launch_packet"
            } else {
                "inspect_review_blockers"
            },
        ),
        OffdeskPlanReviewDecision::RevisionRequired => ("revision_required", "revise_plan"),
        OffdeskPlanReviewDecision::Rejected => ("rejected", "discard_or_replace_plan"),
    };
    OffdeskPlanReviewState {
        status: status.to_string(),
        ready_for_launch_preparation_candidate: review.ready_for_launch_preparation_candidate,
        next_safe_action: next_safe_action.to_string(),
        latest_review_id: Some(review.review_id.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offdesk::{
        offdesk_plan_launch_prep_denials, offdesk_plan_registration_denials,
        offdesk_plan_review_denials, OffdeskPlanLaunchPrepArtifacts,
        OffdeskPlanRegistrationArtifacts, OffdeskPlanReviewArtifacts,
        OFFDESK_PLAN_LAUNCH_PREP_SCHEMA, OFFDESK_PLAN_REGISTRATION_SCHEMA,
        OFFDESK_PLAN_REVIEW_SCHEMA,
    };

    fn registration() -> OffdeskPlanRegistration {
        OffdeskPlanRegistration {
            schema: OFFDESK_PLAN_REGISTRATION_SCHEMA.to_string(),
            registered_at: "2026-08-13T01:00:00Z".parse().expect("valid timestamp"),
            forager_profile: "forager-ops".to_string(),
            source_path: "/workspace/OVERNIGHT_PLAN.json".to_string(),
            source_sha256: "abc123".to_string(),
            artifact_kind: "offdesk_multiturn_plan".to_string(),
            plan_schema: "offdesk_multiturn_plan.v1".to_string(),
            profile_key: Some("generic".to_string()),
            profile_name: None,
            project_key: Some("project".to_string()),
            request_id: Some("request".to_string()),
            task_id: Some("task".to_string()),
            ready_for_operator_review: true,
            ready_for_launch_preparation: false,
            ready_for_enqueue: false,
            validation_failures: Vec::new(),
            decision: None,
            consensus: None,
            selected_plan_path: None,
            dry_run: false,
            artifacts: OffdeskPlanRegistrationArtifacts {
                registry_dir: Some("/tmp/plan_123".to_string()),
                registration_json: Some("/tmp/plan_123/registration.json".to_string()),
                copied_source_json: Some("/tmp/plan_123/source.json".to_string()),
            },
            does_not_authorize: offdesk_plan_registration_denials(),
        }
    }

    fn review(
        review_id: &str,
        decision: OffdeskPlanReviewDecision,
        candidate: bool,
    ) -> OffdeskPlanReviewRecord {
        OffdeskPlanReviewRecord {
            schema: OFFDESK_PLAN_REVIEW_SCHEMA.to_string(),
            reviewed_at: "2026-08-13T02:00:00Z".parse().expect("valid timestamp"),
            review_id: review_id.to_string(),
            plan_id: "plan_123".to_string(),
            forager_profile: "forager-ops".to_string(),
            registration_path: "/tmp/plan_123/registration.json".to_string(),
            source_sha256: "abc123".to_string(),
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
                registration_json: "/tmp/plan_123/registration.json".to_string(),
                copied_source_json: Some("/tmp/plan_123/source.json".to_string()),
                review_record_json: format!("/tmp/plan_123/{review_id}.json"),
            },
            does_not_authorize: offdesk_plan_review_denials(),
        }
    }

    fn launch_prep(prep_id: &str) -> OffdeskPlanLaunchPrepPacket {
        OffdeskPlanLaunchPrepPacket {
            schema: OFFDESK_PLAN_LAUNCH_PREP_SCHEMA.to_string(),
            prepared_at: "2026-08-13T03:00:00Z".parse().expect("valid timestamp"),
            prep_id: prep_id.to_string(),
            plan_id: "plan_123".to_string(),
            forager_profile: "forager-ops".to_string(),
            prepared_by: "operator".to_string(),
            registration_path: "/tmp/plan_123/registration.json".to_string(),
            source_path: "/workspace/OVERNIGHT_PLAN.json".to_string(),
            source_sha256: "abc123".to_string(),
            review_id: "review_approved".to_string(),
            review_decision: OffdeskPlanReviewDecision::Approved,
            review_record_json: "/tmp/plan_123/review_approved.json".to_string(),
            artifact_kind: "offdesk_multiturn_plan".to_string(),
            plan_schema: "offdesk_multiturn_plan.v1".to_string(),
            profile_key: Some("generic".to_string()),
            project_key: Some("project".to_string()),
            request_id: Some("request".to_string()),
            task_id: Some("task".to_string()),
            selected_plan_path: None,
            required_first_reads: vec!["/tmp/plan_123/registration.json".to_string()],
            launch_preparation_candidate: true,
            ready_for_launch: false,
            ready_for_enqueue: false,
            next_safe_action: "build_execution_brief_then_use_existing_offdesk_gate".to_string(),
            notes: None,
            read_only_project_state: true,
            applies_file_operations: false,
            artifacts: OffdeskPlanLaunchPrepArtifacts {
                registration_json: "/tmp/plan_123/registration.json".to_string(),
                copied_source_json: Some("/tmp/plan_123/source.json".to_string()),
                review_record_json: "/tmp/plan_123/review_approved.json".to_string(),
                launch_prep_json: format!("/tmp/plan_123/{prep_id}.json"),
            },
            does_not_authorize: offdesk_plan_launch_prep_denials(),
        }
    }

    #[test]
    fn unreviewed_state_requires_operator_review() {
        let state = build_offdesk_plan_review_state(None);

        assert_eq!(state.status, "unreviewed");
        assert!(!state.ready_for_launch_preparation_candidate);
        assert_eq!(state.next_safe_action, "record_operator_review");
        assert!(state.latest_review_id.is_none());
    }

    #[test]
    fn approved_state_distinguishes_launch_candidates() {
        let candidate = review(
            "review_candidate",
            OffdeskPlanReviewDecision::Approved,
            true,
        );
        let blocked = review("review_blocked", OffdeskPlanReviewDecision::Approved, false);

        let candidate_state = build_offdesk_plan_review_state(Some(&candidate));
        assert_eq!(candidate_state.status, "approved");
        assert_eq!(candidate_state.next_safe_action, "prepare_launch_packet");
        assert_eq!(
            candidate_state.latest_review_id.as_deref(),
            Some("review_candidate")
        );

        let blocked_state = build_offdesk_plan_review_state(Some(&blocked));
        assert_eq!(blocked_state.status, "approved");
        assert_eq!(blocked_state.next_safe_action, "inspect_review_blockers");
    }

    #[test]
    fn non_approved_states_preserve_safe_actions() {
        let revision = review(
            "review_revision",
            OffdeskPlanReviewDecision::RevisionRequired,
            false,
        );
        let rejected = review(
            "review_rejected",
            OffdeskPlanReviewDecision::Rejected,
            false,
        );

        let revision_state = build_offdesk_plan_review_state(Some(&revision));
        assert_eq!(revision_state.status, "revision_required");
        assert_eq!(revision_state.next_safe_action, "revise_plan");

        let rejected_state = build_offdesk_plan_review_state(Some(&rejected));
        assert_eq!(rejected_state.status, "rejected");
        assert_eq!(rejected_state.next_safe_action, "discard_or_replace_plan");
    }

    #[test]
    fn list_and_detail_models_share_latest_history() {
        let revision = review(
            "review_revision",
            OffdeskPlanReviewDecision::RevisionRequired,
            false,
        );
        let approved = review("review_approved", OffdeskPlanReviewDecision::Approved, true);
        let prep = launch_prep("prep_latest");
        let reviews = vec![revision, approved];
        let launch_preps = vec![prep];

        let item = build_offdesk_plan_registry_item(
            "plan_123".to_string(),
            "/tmp/plan_123/registration.json".to_string(),
            registration(),
            &reviews,
            &launch_preps,
        );
        assert_eq!(item.review_count, 2);
        assert_eq!(item.review_state.status, "approved");
        assert_eq!(
            item.latest_review
                .as_ref()
                .map(|review| review.review_id.as_str()),
            Some("review_approved")
        );
        assert_eq!(item.launch_prep_count, 1);

        let detail = build_offdesk_plan_registry_detail(item, reviews, launch_preps);
        assert_eq!(detail.review_count, 2);
        assert_eq!(detail.reviews.len(), 2);
        assert_eq!(
            detail.review_state.next_safe_action,
            "prepare_launch_packet"
        );
        assert_eq!(
            detail
                .latest_launch_prep
                .as_ref()
                .map(|packet| packet.prep_id.as_str()),
            Some("prep_latest")
        );
        assert_eq!(detail.launch_preps.len(), 1);
    }
}
