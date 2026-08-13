//! Typed Offdesk workflow transitions and receipt construction.

mod closeout_receipt;
mod closeout_records;
mod decision;
mod implementation_packet_coverage;
mod plan_launch_prep;
mod plan_registration;
mod plan_review;
mod wiki_proposal_receipt;

pub use closeout_receipt::{
    build_closeout_receipt, closeout_receipt_decisions_from_value,
    closeout_receipt_evidence_status, closeout_receipt_next_safe_action,
    closeout_receipt_string_list, closeout_receipt_wiki_state,
    closeout_retention_status_after_preserve_in_place, CloseoutReceipt,
    CloseoutReceiptArtifactPathsInput, CloseoutReceiptArtifacts, CloseoutReceiptBuildInput,
    CloseoutReceiptDecision, CloseoutReceiptTaskRef, CloseoutResolvedDecision, CloseoutVerdict,
};
pub use closeout_records::{
    build_closeout_decision_record, build_closeout_retirement_record, build_closeout_review_record,
    CloseoutDecisionRecordBuildInput, CloseoutDecisionResolution, CloseoutDecisionResolutionRecord,
    CloseoutRetirementRecord, CloseoutRetirementRecordBuildInput, CloseoutReviewArtifactPaths,
    CloseoutReviewRecord, CloseoutReviewRecordBuildInput,
};
pub use decision::{
    normalize_decision_choice, receipt_decision_record, resolve_decision_record,
    DecisionReceiptInput, DecisionResolutionInput,
};
pub use implementation_packet_coverage::{
    assess_implementation_packet_detail, assess_implementation_packet_goal,
    assess_work_slice_receipt, build_closeout_implementation_packet_coverage,
    CloseoutImplementationPacketCoverage, CloseoutImplementationPacketCoverageInput,
    CloseoutImplementationPacketCoverageItem, CloseoutPacketCoverageDetail,
    ImplementationPacketCoverageStatus, ImplementationPacketDetailCoverage,
    ImplementationPacketExecutionEvidence, ImplementationPacketGoalCoverage,
    WorkSliceReceiptCoverage, WorkSliceReceiptTrustTier,
};
pub use plan_launch_prep::{
    build_offdesk_plan_launch_prep_packet, offdesk_plan_launch_prep_denials,
    select_offdesk_plan_review, validate_offdesk_plan_launch_prep, OffdeskPlanLaunchPrepArtifacts,
    OffdeskPlanLaunchPrepBuildInput, OffdeskPlanLaunchPrepPacket, OFFDESK_PLAN_LAUNCH_PREP_SCHEMA,
};
pub use plan_registration::{
    build_offdesk_plan_registration, offdesk_plan_registration_denials,
    validate_offdesk_plan_input, OffdeskPlanInputSummary, OffdeskPlanRegistration,
    OffdeskPlanRegistrationArtifacts, OffdeskPlanRegistrationBuildInput,
    OFFDESK_PLAN_REGISTRATION_SCHEMA, OFFDESK_PLAN_REQUIRED_DENIALS,
};
pub use plan_review::{
    build_offdesk_plan_review_record, offdesk_plan_review_denials,
    validate_offdesk_plan_review_input, OffdeskPlanReviewArtifacts, OffdeskPlanReviewBuildInput,
    OffdeskPlanReviewDecision, OffdeskPlanReviewRecord, OFFDESK_PLAN_REVIEW_SCHEMA,
};
pub use wiki_proposal_receipt::{
    build_adaptive_wiki_proposal_receipt, AdaptiveWikiProposalReceipt,
    AdaptiveWikiProposalReceiptCheck, AdaptiveWikiProposalReceiptInput,
    AdaptiveWikiProposalReceiptSubject,
};
