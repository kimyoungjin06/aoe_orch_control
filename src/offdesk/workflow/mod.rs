//! Typed Offdesk workflow transitions and receipt construction.

mod closeout_receipt;
mod closeout_records;
mod decision;
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
pub use wiki_proposal_receipt::{
    build_adaptive_wiki_proposal_receipt, AdaptiveWikiProposalReceipt,
    AdaptiveWikiProposalReceiptCheck, AdaptiveWikiProposalReceiptInput,
    AdaptiveWikiProposalReceiptSubject,
};
