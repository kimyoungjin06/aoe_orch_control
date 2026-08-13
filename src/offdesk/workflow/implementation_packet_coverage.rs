//! Typed implementation-packet closeout coverage policy.
//!
//! CLI adapters discover packet files and execution receipts. This module
//! owns the policy that turns those observations into coverage states.

use crate::offdesk::{
    ImplementationPacketSummary, WorkSliceExecutionReceipt, WorkSliceExecutionStatus,
    WorkSliceReceiptProducerRole, WorkSliceVerificationStatus,
};

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
