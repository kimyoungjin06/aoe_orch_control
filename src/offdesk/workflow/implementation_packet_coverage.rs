//! Typed implementation-packet closeout coverage policy.
//!
//! CLI adapters discover packet files and execution receipts. This module
//! owns the policy that turns those observations into coverage states.

use crate::offdesk::{
    ImplementationPacketSummary, WorkSliceExecutionReceipt, WorkSliceExecutionStatus,
    WorkSliceReceiptProducerRole, WorkSliceVerificationStatus,
};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImplementationPacketCoverageStatus {
    Completed,
    Deferred,
    Missing,
    Drifted,
}

impl ImplementationPacketCoverageStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Deferred => "deferred",
            Self::Missing => "missing",
            Self::Drifted => "drifted",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ImplementationPacketExecutionEvidence {
    pub has_completed: bool,
    pub has_active: bool,
    pub has_failed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImplementationPacketGoalCoverage {
    pub status: ImplementationPacketCoverageStatus,
    pub reason: &'static str,
}

#[derive(Default, Serialize)]
pub struct CloseoutImplementationPacketCoverage {
    pub packet_count: usize,
    pub completed: usize,
    pub deferred: usize,
    pub missing: usize,
    pub drifted: usize,
    pub detail_items: usize,
    pub detail_items_completed: usize,
    pub detail_items_deferred: usize,
    pub detail_items_missing: usize,
    pub detail_items_drifted: usize,
    pub items: Vec<CloseoutImplementationPacketCoverageItem>,
}

#[derive(Serialize)]
pub struct CloseoutImplementationPacketCoverageItem {
    pub packet_id: String,
    pub project_key: String,
    pub goal: String,
    pub success_state: String,
    pub outcome: String,
    pub safe_to_delegate: bool,
    pub goal_status: &'static str,
    pub reason: String,
    pub evidence_refs: Vec<String>,
    pub required_revisions: Vec<String>,
    pub drift_signals: Vec<String>,
    pub missing_decisions: Vec<String>,
    pub work_slice_count: usize,
    pub validation_item_count: usize,
    pub expected_artifact_count: usize,
    pub detail_source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail_error: Option<String>,
    pub work_slices: Vec<CloseoutPacketCoverageDetail>,
    pub validation_items: Vec<CloseoutPacketCoverageDetail>,
    pub expected_artifacts: Vec<CloseoutPacketCoverageDetail>,
}

#[derive(Default, Serialize)]
pub struct CloseoutPacketCoverageDetail {
    pub category: &'static str,
    pub label: String,
    pub status: &'static str,
    pub reason: String,
    pub evidence_refs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receipt_role: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trust_tier: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reported_status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub claim_status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verification_summary: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub verification_refs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_observation_status: Option<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub validation_refs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub open_questions: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub drift_signals: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_safe_action: Option<String>,
}

pub struct CloseoutImplementationPacketCoverageInput {
    pub summary: ImplementationPacketSummary,
    pub goal_coverage: ImplementationPacketGoalCoverage,
    pub evidence_refs: Vec<String>,
    pub detail_source: &'static str,
    pub detail_error: Option<String>,
    pub work_slices: Vec<CloseoutPacketCoverageDetail>,
    pub validation_items: Vec<CloseoutPacketCoverageDetail>,
    pub expected_artifacts: Vec<CloseoutPacketCoverageDetail>,
}

pub fn build_closeout_implementation_packet_coverage(
    inputs: Vec<CloseoutImplementationPacketCoverageInput>,
) -> CloseoutImplementationPacketCoverage {
    let mut coverage = CloseoutImplementationPacketCoverage::default();
    for input in inputs {
        count_packet_status(&mut coverage, input.goal_coverage.status);
        count_packet_details(&mut coverage, &input.work_slices);
        count_packet_details(&mut coverage, &input.validation_items);
        count_packet_details(&mut coverage, &input.expected_artifacts);

        let summary = input.summary;
        coverage
            .items
            .push(CloseoutImplementationPacketCoverageItem {
                packet_id: summary.packet_id,
                project_key: summary.project_key,
                goal: summary.goal,
                success_state: summary.success_state,
                outcome: summary.outcome,
                safe_to_delegate: summary.safe_to_delegate,
                goal_status: input.goal_coverage.status.as_str(),
                reason: input.goal_coverage.reason.to_string(),
                evidence_refs: input.evidence_refs.into_iter().take(20).collect(),
                required_revisions: summary.required_revisions,
                drift_signals: summary.drift_signals,
                missing_decisions: summary.missing_decisions,
                work_slice_count: summary.work_slice_count,
                validation_item_count: summary.validation_item_count,
                expected_artifact_count: summary.expected_artifact_count,
                detail_source: input.detail_source,
                detail_error: input.detail_error,
                work_slices: input.work_slices,
                validation_items: input.validation_items,
                expected_artifacts: input.expected_artifacts,
            });
    }
    coverage.packet_count = coverage.items.len();
    coverage
}

fn count_packet_status(
    coverage: &mut CloseoutImplementationPacketCoverage,
    status: ImplementationPacketCoverageStatus,
) {
    match status {
        ImplementationPacketCoverageStatus::Completed => coverage.completed += 1,
        ImplementationPacketCoverageStatus::Deferred => coverage.deferred += 1,
        ImplementationPacketCoverageStatus::Missing => coverage.missing += 1,
        ImplementationPacketCoverageStatus::Drifted => coverage.drifted += 1,
    }
}

fn count_packet_details(
    coverage: &mut CloseoutImplementationPacketCoverage,
    details: &[CloseoutPacketCoverageDetail],
) {
    for detail in details {
        coverage.detail_items += 1;
        match detail.status {
            "completed" => coverage.detail_items_completed += 1,
            "deferred" => coverage.detail_items_deferred += 1,
            "missing" => coverage.detail_items_missing += 1,
            "drifted" => coverage.detail_items_drifted += 1,
            _ => {}
        }
    }
}

pub fn assess_implementation_packet_goal(
    summary: &ImplementationPacketSummary,
    evidence: ImplementationPacketExecutionEvidence,
) -> ImplementationPacketGoalCoverage {
    if !summary.safe_to_delegate
        || !summary.outcome.eq_ignore_ascii_case("pass")
        || !summary.required_revisions.is_empty()
        || !summary.drift_signals.is_empty()
        || !summary.missing_decisions.is_empty()
    {
        return ImplementationPacketGoalCoverage {
            status: ImplementationPacketCoverageStatus::Drifted,
            reason: "Implementation packet alignment was not clean; revise the packet or resolve listed drift before accepting the run.",
        };
    }
    if evidence.has_failed {
        return ImplementationPacketGoalCoverage {
            status: ImplementationPacketCoverageStatus::Drifted,
            reason: "Execution evidence shows failed, cancelled, stale, or reconstructable work for this packet.",
        };
    }
    if evidence.has_active {
        return ImplementationPacketGoalCoverage {
            status: ImplementationPacketCoverageStatus::Deferred,
            reason: "Execution is still queued, running, pending approval, or waiting for resume.",
        };
    }
    if evidence.has_completed {
        return ImplementationPacketGoalCoverage {
            status: ImplementationPacketCoverageStatus::Completed,
            reason: "Execution evidence exists for this packet; acceptance still depends on closeout review and first-read verification.",
        };
    }
    ImplementationPacketGoalCoverage {
        status: ImplementationPacketCoverageStatus::Missing,
        reason: "The packet is linked to closeout, but no task or background completion evidence was found.",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImplementationPacketDetailCoverage {
    pub status: ImplementationPacketCoverageStatus,
    pub reason: &'static str,
}

pub fn assess_implementation_packet_detail(
    packet_status: ImplementationPacketCoverageStatus,
    has_match: bool,
) -> ImplementationPacketDetailCoverage {
    if packet_status != ImplementationPacketCoverageStatus::Completed {
        return ImplementationPacketDetailCoverage {
            status: packet_status,
            reason: "Packet-level status prevents item-level acceptance.",
        };
    }
    if has_match {
        ImplementationPacketDetailCoverage {
            status: ImplementationPacketCoverageStatus::Completed,
            reason: "Closeout evidence matched this packet item.",
        }
    } else {
        ImplementationPacketDetailCoverage {
            status: ImplementationPacketCoverageStatus::Missing,
            reason: "No closeout artifact or evidence ref matched this packet item.",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkSliceReceiptTrustTier {
    RuntimeObservation,
    WorkerClaim,
    SourceVerified,
    ReviewJudgment,
    CloseoutVerified,
    CloseoutObservation,
    LegacyReceipt,
}

impl WorkSliceReceiptTrustTier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RuntimeObservation => "runtime_observation",
            Self::WorkerClaim => "worker_claim",
            Self::SourceVerified => "source_verified",
            Self::ReviewJudgment => "review_judgment",
            Self::CloseoutVerified => "closeout_verified",
            Self::CloseoutObservation => "closeout_observation",
            Self::LegacyReceipt => "legacy_receipt",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkSliceReceiptCoverage {
    pub effective_status: WorkSliceExecutionStatus,
    pub role: WorkSliceReceiptProducerRole,
    pub trust_tier: WorkSliceReceiptTrustTier,
}

impl WorkSliceReceiptCoverage {
    pub fn role_label(self) -> &'static str {
        match self.role {
            WorkSliceReceiptProducerRole::RunnerObservation => "Runner observation",
            WorkSliceReceiptProducerRole::WorkerClaim => "Worker claim",
            WorkSliceReceiptProducerRole::CloseoutCollector => "Closeout observation",
            WorkSliceReceiptProducerRole::DeterministicVerification => "Deterministic verification",
            WorkSliceReceiptProducerRole::ReviewJudgment => "Review judgment",
            WorkSliceReceiptProducerRole::LegacyReceipt => "Legacy receipt",
        }
    }
}

pub fn assess_work_slice_receipt(receipt: &WorkSliceExecutionReceipt) -> WorkSliceReceiptCoverage {
    let role = receipt.resolved_producer_role();
    WorkSliceReceiptCoverage {
        effective_status: effective_work_slice_status(
            receipt.status,
            role,
            receipt.verification_status,
        ),
        role,
        trust_tier: work_slice_receipt_trust_tier(role, receipt.verification_status),
    }
}

fn effective_work_slice_status(
    reported_status: WorkSliceExecutionStatus,
    role: WorkSliceReceiptProducerRole,
    verification_status: WorkSliceVerificationStatus,
) -> WorkSliceExecutionStatus {
    if reported_status != WorkSliceExecutionStatus::Completed {
        return reported_status;
    }
    match role {
        WorkSliceReceiptProducerRole::DeterministicVerification
        | WorkSliceReceiptProducerRole::ReviewJudgment => WorkSliceExecutionStatus::Completed,
        WorkSliceReceiptProducerRole::CloseoutCollector
            if verification_status.is_independently_verified() =>
        {
            WorkSliceExecutionStatus::Completed
        }
        _ => WorkSliceExecutionStatus::Deferred,
    }
}

fn work_slice_receipt_trust_tier(
    role: WorkSliceReceiptProducerRole,
    verification_status: WorkSliceVerificationStatus,
) -> WorkSliceReceiptTrustTier {
    match role {
        WorkSliceReceiptProducerRole::RunnerObservation => {
            WorkSliceReceiptTrustTier::RuntimeObservation
        }
        WorkSliceReceiptProducerRole::WorkerClaim => WorkSliceReceiptTrustTier::WorkerClaim,
        WorkSliceReceiptProducerRole::DeterministicVerification => {
            WorkSliceReceiptTrustTier::SourceVerified
        }
        WorkSliceReceiptProducerRole::ReviewJudgment => WorkSliceReceiptTrustTier::ReviewJudgment,
        WorkSliceReceiptProducerRole::CloseoutCollector
            if verification_status.is_independently_verified() =>
        {
            WorkSliceReceiptTrustTier::CloseoutVerified
        }
        WorkSliceReceiptProducerRole::CloseoutCollector => {
            WorkSliceReceiptTrustTier::CloseoutObservation
        }
        WorkSliceReceiptProducerRole::LegacyReceipt => WorkSliceReceiptTrustTier::LegacyReceipt,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn packet_summary() -> ImplementationPacketSummary {
        ImplementationPacketSummary {
            packet_id: "packet-1".to_string(),
            created_at: "2026-08-13T00:00:00Z".to_string(),
            project_key: "forager".to_string(),
            artifact_dir: "/tmp/packet-1".to_string(),
            packet_path: "/tmp/packet-1/IMPLEMENTATION_PACKET.json".to_string(),
            alignment_review_path: "/tmp/packet-1/RECURSIVE_ALIGNMENT_REVIEW.json".to_string(),
            markdown_path: "/tmp/packet-1/IMPLEMENTATION_PACKET.md".to_string(),
            goal: "Move coverage policy into a typed workflow".to_string(),
            success_state: "CLI keeps only artifact discovery and rendering".to_string(),
            preferred_worker: "codex".to_string(),
            safe_to_delegate: true,
            outcome: "pass".to_string(),
            required_revisions: Vec::new(),
            drift_signals: Vec::new(),
            missing_decisions: Vec::new(),
            work_slice_count: 1,
            capability_mapping_count: 1,
            validation_item_count: 1,
            stop_condition_count: 1,
            expected_artifact_count: 1,
        }
    }

    #[test]
    fn packet_goal_requires_clean_alignment_before_completion() {
        let mut summary = packet_summary();
        summary
            .required_revisions
            .push("Resolve scope drift".to_string());
        let coverage = assess_implementation_packet_goal(
            &summary,
            ImplementationPacketExecutionEvidence {
                has_completed: true,
                ..Default::default()
            },
        );
        assert_eq!(coverage.status, ImplementationPacketCoverageStatus::Drifted);
    }

    #[test]
    fn packet_goal_evidence_precedence_is_fail_active_complete_missing() {
        let summary = packet_summary();
        let failed = assess_implementation_packet_goal(
            &summary,
            ImplementationPacketExecutionEvidence {
                has_completed: true,
                has_active: true,
                has_failed: true,
            },
        );
        assert_eq!(failed.status, ImplementationPacketCoverageStatus::Drifted);

        let active = assess_implementation_packet_goal(
            &summary,
            ImplementationPacketExecutionEvidence {
                has_completed: true,
                has_active: true,
                has_failed: false,
            },
        );
        assert_eq!(active.status, ImplementationPacketCoverageStatus::Deferred);

        let completed = assess_implementation_packet_goal(
            &summary,
            ImplementationPacketExecutionEvidence {
                has_completed: true,
                ..Default::default()
            },
        );
        assert_eq!(
            completed.status,
            ImplementationPacketCoverageStatus::Completed
        );

        let missing = assess_implementation_packet_goal(&summary, Default::default());
        assert_eq!(missing.status, ImplementationPacketCoverageStatus::Missing);
    }

    #[test]
    fn detail_acceptance_requires_completed_packet_and_matching_evidence() {
        let matched = assess_implementation_packet_detail(
            ImplementationPacketCoverageStatus::Completed,
            true,
        );
        assert_eq!(
            matched.status,
            ImplementationPacketCoverageStatus::Completed
        );

        let unmatched = assess_implementation_packet_detail(
            ImplementationPacketCoverageStatus::Completed,
            false,
        );
        assert_eq!(
            unmatched.status,
            ImplementationPacketCoverageStatus::Missing
        );

        let deferred =
            assess_implementation_packet_detail(ImplementationPacketCoverageStatus::Deferred, true);
        assert_eq!(
            deferred.status,
            ImplementationPacketCoverageStatus::Deferred
        );
    }

    #[test]
    fn coverage_builder_owns_record_shape_and_status_counters() {
        let mut drifted_summary = packet_summary();
        drifted_summary.packet_id = "packet-2".to_string();
        let evidence_refs = (0..25)
            .map(|index| format!("evidence-{index}"))
            .collect::<Vec<_>>();
        let detail = |status| CloseoutPacketCoverageDetail {
            category: "validation",
            label: format!("{status} item"),
            status,
            reason: "test detail".to_string(),
            ..Default::default()
        };

        let coverage = build_closeout_implementation_packet_coverage(vec![
            CloseoutImplementationPacketCoverageInput {
                summary: packet_summary(),
                goal_coverage: ImplementationPacketGoalCoverage {
                    status: ImplementationPacketCoverageStatus::Completed,
                    reason: "completed packet",
                },
                evidence_refs,
                detail_source: "implementation_packet",
                detail_error: None,
                work_slices: vec![detail("completed")],
                validation_items: vec![detail("deferred"), detail("missing")],
                expected_artifacts: vec![detail("drifted")],
            },
            CloseoutImplementationPacketCoverageInput {
                summary: drifted_summary,
                goal_coverage: ImplementationPacketGoalCoverage {
                    status: ImplementationPacketCoverageStatus::Drifted,
                    reason: "drifted packet",
                },
                evidence_refs: Vec::new(),
                detail_source: "summary_only",
                detail_error: Some("packet unavailable".to_string()),
                work_slices: Vec::new(),
                validation_items: Vec::new(),
                expected_artifacts: Vec::new(),
            },
        ]);

        assert_eq!(coverage.packet_count, 2);
        assert_eq!(coverage.completed, 1);
        assert_eq!(coverage.drifted, 1);
        assert_eq!(coverage.detail_items, 4);
        assert_eq!(coverage.detail_items_completed, 1);
        assert_eq!(coverage.detail_items_deferred, 1);
        assert_eq!(coverage.detail_items_missing, 1);
        assert_eq!(coverage.detail_items_drifted, 1);
        assert_eq!(coverage.items[0].goal_status, "completed");
        assert_eq!(coverage.items[0].evidence_refs.len(), 20);

        let value = serde_json::to_value(&coverage).expect("coverage should serialize");
        assert_eq!(value["items"][0]["goal_status"], "completed");
        assert!(value["items"][0].get("detail_error").is_none());
        assert_eq!(value["items"][1]["detail_error"], "packet unavailable");
    }

    #[test]
    fn worker_completion_remains_deferred_without_independent_verification() {
        let status = effective_work_slice_status(
            WorkSliceExecutionStatus::Completed,
            WorkSliceReceiptProducerRole::WorkerClaim,
            WorkSliceVerificationStatus::Unverified,
        );
        assert_eq!(status, WorkSliceExecutionStatus::Deferred);
        assert_eq!(
            work_slice_receipt_trust_tier(
                WorkSliceReceiptProducerRole::WorkerClaim,
                WorkSliceVerificationStatus::Unverified,
            ),
            WorkSliceReceiptTrustTier::WorkerClaim
        );
    }

    #[test]
    fn deterministic_and_verified_closeout_completion_are_accepted() {
        let deterministic = effective_work_slice_status(
            WorkSliceExecutionStatus::Completed,
            WorkSliceReceiptProducerRole::DeterministicVerification,
            WorkSliceVerificationStatus::Unverified,
        );
        assert_eq!(deterministic, WorkSliceExecutionStatus::Completed);

        let closeout = effective_work_slice_status(
            WorkSliceExecutionStatus::Completed,
            WorkSliceReceiptProducerRole::CloseoutCollector,
            WorkSliceVerificationStatus::EvidenceObserved,
        );
        assert_eq!(closeout, WorkSliceExecutionStatus::Completed);
        assert_eq!(
            work_slice_receipt_trust_tier(
                WorkSliceReceiptProducerRole::CloseoutCollector,
                WorkSliceVerificationStatus::EvidenceObserved,
            ),
            WorkSliceReceiptTrustTier::CloseoutVerified
        );
    }
}
