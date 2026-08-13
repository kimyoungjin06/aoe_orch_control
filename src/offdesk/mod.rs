//! Offdesk orchestration safety rails and durable artifacts.
//!
//! This module keeps the canonical state in Forager-owned JSON artifacts. The
//! helpers are intentionally side-effect-light so the scheduler, dashboard,
//! Telegram bridge, and future worker backends can share the same policy logic.

pub mod adaptive_wiki;
pub mod approval;
pub mod background;
pub mod capability;
pub mod control_loop;
pub mod decision;
pub mod implementation_packet;
pub mod learning_signals;
pub mod mode_contract;
pub mod mutation;
pub mod operator_pause;
pub mod orchestration;
pub mod provider;
pub mod redaction;
pub mod resume;
pub mod runner;
pub mod scheduler;
pub mod task_queue;
pub mod tick_lock;
pub mod workflow;

pub use adaptive_wiki::{
    build_ai_projection, build_ai_projection_report, build_graph_export_files,
    build_human_projection, build_runtime_projection, build_usage_records,
    build_usage_records_with_policy, AdaptiveWikiActivationMode, AdaptiveWikiAgentMode,
    AdaptiveWikiAgentModeFilter, AdaptiveWikiAiProjection, AdaptiveWikiAuditAction,
    AdaptiveWikiAuditRecord, AdaptiveWikiCandidate, AdaptiveWikiCandidateInput,
    AdaptiveWikiCandidateState, AdaptiveWikiConfidence, AdaptiveWikiCorrectionKind,
    AdaptiveWikiCorrectionRecord, AdaptiveWikiCorrectionRecurrenceAssessment,
    AdaptiveWikiCorrectionRecurrenceReport, AdaptiveWikiCorrectionRecurrenceSummary,
    AdaptiveWikiEntry, AdaptiveWikiEntryEdit, AdaptiveWikiEntryState,
    AdaptiveWikiEpisodeEvaluationReport, AdaptiveWikiEpisodeEvaluationSummary,
    AdaptiveWikiEpisodeTraceStep, AdaptiveWikiGraphEdge, AdaptiveWikiGraphNode,
    AdaptiveWikiGraphReport, AdaptiveWikiGraphSummary, AdaptiveWikiHumanCandidate,
    AdaptiveWikiHumanEntry, AdaptiveWikiHumanProjection, AdaptiveWikiKind, AdaptiveWikiLintIssue,
    AdaptiveWikiLintReport, AdaptiveWikiLintSeverity, AdaptiveWikiLintSummary,
    AdaptiveWikiLiveEpisodeEvent, AdaptiveWikiLiveEpisodeEventKind, AdaptiveWikiLiveEpisodeFilter,
    AdaptiveWikiLiveEpisodeSummary, AdaptiveWikiLiveEpisodeTraceReport,
    AdaptiveWikiMarkdownExportFile, AdaptiveWikiMarkdownExportReport,
    AdaptiveWikiMarkdownExportSummary, AdaptiveWikiOrigin, AdaptiveWikiProjectionBudget,
    AdaptiveWikiProjectionComparisonReport, AdaptiveWikiProjectionComparisonSummary,
    AdaptiveWikiProjectionConflict, AdaptiveWikiProjectionConflictPolarity,
    AdaptiveWikiProjectionPolicy, AdaptiveWikiProjectionRejection,
    AdaptiveWikiProjectionRejectionReason, AdaptiveWikiProjectionReport,
    AdaptiveWikiProjectionReviewExpired, AdaptiveWikiProjectionReviewExpiredPolicy,
    AdaptiveWikiProjectionSummary, AdaptiveWikiPromotionEvidenceChainReport,
    AdaptiveWikiPromotionEvidenceChainSummary, AdaptiveWikiPromotionReceipt,
    AdaptiveWikiPromotionReceiptAuthority, AdaptiveWikiQuery, AdaptiveWikiReviewProposal,
    AdaptiveWikiReviewProposalAction, AdaptiveWikiReviewProposalDecision,
    AdaptiveWikiReviewProposalEventRecord, AdaptiveWikiReviewProposalLifecycle,
    AdaptiveWikiReviewQueueFilter, AdaptiveWikiReviewReport, AdaptiveWikiReviewReportSummary,
    AdaptiveWikiReviewRisk, AdaptiveWikiRuntimePolicyAckScopeMode,
    AdaptiveWikiRuntimePolicyAcknowledgement, AdaptiveWikiRuntimePolicyDecision,
    AdaptiveWikiRuntimePolicyDecisionStatus, AdaptiveWikiRuntimeProjection,
    AdaptiveWikiRuntimeProjectionResolution, AdaptiveWikiScope, AdaptiveWikiScopeSuggestion,
    AdaptiveWikiSignalKind, AdaptiveWikiStatus, AdaptiveWikiStore, AdaptiveWikiUsageContext,
    AdaptiveWikiUsageRecord,
};
pub use approval::{
    ActionApprovalMetadata, ActionApprovalRequest, ApprovalBrief, ApprovalBriefOption,
    ApprovalDecision, ApprovalLedger, ApprovalLedgerSession, ApprovalMode, ApprovalScope,
    ApprovalStatus, ArtifactRetentionApprovalMetadata, ExecutionBrief, PendingActionApproval,
    ProviderFallbackApplyScope, ProviderFallbackApprovalMetadata, RiskLevel,
};
pub use background::{
    BackgroundProbe, BackgroundRecoveryAcknowledgement, BackgroundRecoveryDecision,
    BackgroundRunStore, BackgroundRunnerKind, BackgroundRunnerPhase,
};
pub use capability::{
    default_capability_registry, CapabilityArtifactCheck, CapabilityArtifactContract,
    CapabilityArtifactRef, CapabilityDescriptor, CapabilityRegistry, CapabilityRisk,
};
pub use control_loop::{
    load_offdesk_status_summary, reconcile_tasks_with_background_outcomes, run_offdesk_tick,
    OffdeskStatusSummary, OffdeskTickOptions, OffdeskTickReport,
};
pub use decision::{
    CouncilReview, DecisionLedger, DecisionMateriality, DecisionOption, DecisionRaisedBy,
    DecisionReceipt, DecisionRecord, DecisionRecordView, DecisionRequest, DecisionRoute,
    DecisionRouteTarget, DecisionStatus, DecisionTraceRef, DecisionValidationIssue,
    DecisionValidationSeverity, ExecutionHandoff, JudgmentEvaluator, JudgmentRoute,
    DECISION_RECORD_SCHEMA, JUDGMENT_ROUTE_SCHEMA,
};
pub use implementation_packet::{
    draft_implementation_packet, implementation_packet_from_path,
    implementation_packet_record_from_path, latest_implementation_packet_for_project,
    operator_safe_implementation_packet_summary, work_slice_execution_receipts_from_path,
    AlignmentReviewOutcome, ImplementationAlignment, ImplementationCapabilityMapping,
    ImplementationCloseout, ImplementationDesign, ImplementationExecution, ImplementationPacket,
    ImplementationPacketDraftInput, ImplementationPacketSummary, ImplementationScope,
    ImplementationSourceIntent, ImplementationValidation, LatestImplementationPacket,
    RecursiveAlignmentChecks, RecursiveAlignmentReview, WorkSliceExecutionReceipt,
    WorkSliceExecutionStatus, WorkSliceReceiptProducerRole, WorkSliceVerificationStatus,
    IMPLEMENTATION_PACKETS_DIR, IMPLEMENTATION_PACKET_FILE, IMPLEMENTATION_PACKET_MD_FILE,
    IMPLEMENTATION_PACKET_SCHEMA, RECURSIVE_ALIGNMENT_REVIEW_FILE,
    RECURSIVE_ALIGNMENT_REVIEW_SCHEMA, WORK_SLICE_EXECUTION_RECEIPTS_FILE,
    WORK_SLICE_EXECUTION_RECEIPT_SCHEMA,
};
pub use learning_signals::{
    scan_and_emit_learning_signals, EmittedLearningSignal, LearningScanReport,
    LearningSignalSource, LearningSignalStore, LEARNING_SIGNALS_FILE, LEARNING_SIGNALS_SCHEMA,
};
pub use mode_contract::{
    assess_offdesk_mode, mode_requires_separate_review, OffdeskModeAssessment,
    OffdeskModeLifecycle, OffdeskModeRisk, OffdeskModeVerdict,
};
pub use mutation::{
    MutationRestoreOperation, MutationRestorePlan, MutationSnapshot, MutationSnapshotRequest,
    MutationSnapshotStore, MutationSnapshotVerification, SnapshotPolicy,
};
pub use operator_pause::{
    OperatorPauseState, OperatorPauseStore, OPERATOR_PAUSE_FILE, OPERATOR_PAUSE_SCHEMA,
};
pub use orchestration::{load_orchestration_signals, OrchestrationSignals};
pub use provider::{
    classify_provider_error, classify_provider_error_with_context, default_provider_profile,
    default_provider_profiles, recommend_provider_fallback, ProviderCapacityState,
    ProviderCapacityStatus, ProviderCapacityStore, ProviderDescriptor, ProviderErrorClassification,
    ProviderErrorInput, ProviderErrorReason, ProviderFallbackAuthStatus, ProviderFallbackCandidate,
    ProviderFallbackRecommendation, ProviderFallbackSource, ProviderKind, ProviderProfile,
    ProviderRecoveryAction,
};
pub use redaction::{
    force_redact, force_redact_with_report, operator_safe_report, operator_safe_text,
    strip_runner_context, strip_runner_context_with_report, RedactionOutcome,
};
pub use resume::{
    ResumeEvidence, ResumePendingInput, ResumeStatus, TaskResumeState, TaskResumeStore,
};
pub use runner::{
    launch_background_command, launch_background_command_with_gate_outcome, launch_background_run,
    poll_background_runs, BackgroundLaunchOutcome, BackgroundLaunchRequest, BackgroundPollOutcome,
    LocalCommandLaunchSpec,
};
pub use scheduler::{
    is_provider_capacity_block, ProviderCapacityGateSummary, SchedulerGate, SchedulerGateOutcome,
    SchedulerGateRequest, SchedulerGateStatus,
};
pub use task_queue::{
    count_tasks, ensure_resume_review_next_safe_action, next_safe_action_for_background_poll,
    next_safe_action_for_pending_approval, pending_approval_operator_view,
    pending_approval_operator_views, status_next_safe_actions_from_summary,
    tick_next_safe_actions_from_report, OffdeskCloseoutStateSummary, OffdeskNextSafeAction,
    OffdeskPendingApprovalView, OffdeskStatusNextSafeActionInput, OffdeskTask, OffdeskTaskCounts,
    OffdeskTaskInput, OffdeskTaskLifecycleAction, OffdeskTaskLifecycleReport,
    OffdeskTaskNextSafeAction, OffdeskTaskStatus, OffdeskTaskStore, OffdeskTaskView,
    OffdeskTickReportInput,
};
pub use tick_lock::{OffdeskTickLockGuard, OffdeskTickLockMetadata};
pub use workflow::{
    assess_implementation_packet_detail, assess_implementation_packet_goal,
    assess_work_slice_receipt, build_adaptive_wiki_proposal_receipt,
    build_closeout_decision_record, build_closeout_implementation_packet_coverage,
    build_closeout_receipt, build_closeout_retirement_record, build_closeout_review_record,
    build_offdesk_plan_launch_prep_packet, build_offdesk_plan_registration,
    build_offdesk_plan_review_record, closeout_receipt_decisions_from_value,
    closeout_receipt_evidence_status, closeout_receipt_next_safe_action,
    closeout_receipt_string_list, closeout_receipt_wiki_state,
    closeout_retention_status_after_preserve_in_place, normalize_decision_choice,
    offdesk_plan_launch_prep_denials, offdesk_plan_registration_denials,
    offdesk_plan_review_denials, receipt_decision_record, resolve_decision_record,
    select_offdesk_plan_review, validate_offdesk_plan_input, validate_offdesk_plan_launch_prep,
    validate_offdesk_plan_review_input, AdaptiveWikiProposalReceipt,
    AdaptiveWikiProposalReceiptCheck, AdaptiveWikiProposalReceiptInput,
    AdaptiveWikiProposalReceiptSubject, CloseoutDecisionRecordBuildInput,
    CloseoutDecisionResolution, CloseoutDecisionResolutionRecord,
    CloseoutImplementationPacketCoverage, CloseoutImplementationPacketCoverageInput,
    CloseoutImplementationPacketCoverageItem, CloseoutPacketCoverageDetail, CloseoutReceipt,
    CloseoutReceiptArtifactPathsInput, CloseoutReceiptArtifacts, CloseoutReceiptBuildInput,
    CloseoutReceiptDecision, CloseoutReceiptTaskRef, CloseoutResolvedDecision,
    CloseoutRetirementRecord, CloseoutRetirementRecordBuildInput, CloseoutReviewArtifactPaths,
    CloseoutReviewRecord, CloseoutReviewRecordBuildInput, CloseoutVerdict, DecisionReceiptInput,
    DecisionResolutionInput, ImplementationPacketCoverageStatus,
    ImplementationPacketDetailCoverage, ImplementationPacketExecutionEvidence,
    ImplementationPacketGoalCoverage, OffdeskPlanInputSummary, OffdeskPlanLaunchPrepArtifacts,
    OffdeskPlanLaunchPrepBuildInput, OffdeskPlanLaunchPrepPacket, OffdeskPlanRegistration,
    OffdeskPlanRegistrationArtifacts, OffdeskPlanRegistrationBuildInput,
    OffdeskPlanReviewArtifacts, OffdeskPlanReviewBuildInput, OffdeskPlanReviewDecision,
    OffdeskPlanReviewRecord, WorkSliceReceiptCoverage, WorkSliceReceiptTrustTier,
    OFFDESK_PLAN_LAUNCH_PREP_SCHEMA, OFFDESK_PLAN_REGISTRATION_SCHEMA,
    OFFDESK_PLAN_REQUIRED_DENIALS, OFFDESK_PLAN_REVIEW_SCHEMA,
};
