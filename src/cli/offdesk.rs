//! `forager offdesk` operator commands.

mod closeout_records;
mod closeout_render;
mod closeout_report;
mod decision_ingest;
mod deck;
mod hosted_harness;
mod operator_control_presentation;
mod parsing;
mod plan_commands;
mod plan_presentation;
mod plan_queries;
mod plan_registry;
mod remote_operator_presentation;
mod runtime_recovery_presentation;
mod task_lifecycle_presentation;
mod wiki_audit_presentation;
mod wiki_brief_presentation;
mod wiki_catalog;
mod wiki_event_presentation;
mod wiki_evidence_presentation;
mod wiki_mutation_presentation;
mod wiki_projection_presentation;
mod wiki_proposal_handoff;
mod wiki_proposal_receipts;
mod wiki_review_after_presentation;
mod wiki_review_presentation;
mod wiki_runtime_policy_ack_presentation;

use closeout_records::{
    build_closeout_decision_record, build_closeout_retire_record, build_closeout_review_record,
};
use closeout_render::{
    render_closeout_plan_markdown, render_closeout_return_package, render_commercial_review_packet,
};
use closeout_report::build_closeout_report;
use decision_ingest::{ingest_telegram_decision, ingest_telegram_feedback};
pub use decision_ingest::{DecisionIngestTelegramArgs, DecisionIngestTelegramFeedbackArgs};
use deck::run_deck;
pub use deck::DeckArgs;
use hosted_harness::{
    build_harness_prompt_packet, hosted_harness_profile, hosted_harness_profiles,
    HarnessPromptRequest,
};
use operator_control_presentation::{present_learning_scan_report, present_operator_pause_state};
use parsing::*;
use plan_commands::{
    prepare_offdesk_plan_launch, record_offdesk_plan_review, register_offdesk_plan,
};
use plan_presentation::{
    present_offdesk_plan_launch_prep_packet, present_offdesk_plan_registration,
    present_offdesk_plan_registry_detail, present_offdesk_plan_registry_items,
    present_offdesk_plan_review_record, present_remote_operator_plan_detail,
    present_remote_operator_plans,
};
use plan_queries::{query_offdesk_plan_detail, query_offdesk_plans, OffdeskPlanListQuery};
use remote_operator_presentation::{
    present_remote_operator_pending, present_remote_operator_status,
};
use runtime_recovery_presentation::{
    present_background_ack_report, present_background_poll_outcomes, present_background_statuses,
    present_resume_states,
};
use task_lifecycle_presentation::{
    present_retry_task_lifecycle_report, present_task_lifecycle_report, task_status_label,
};
use wiki_audit_presentation::{
    present_wiki_graph, present_wiki_lint, present_wiki_markdown_export,
};
use wiki_brief_presentation::{
    build_wiki_brief_read_model, present_wiki_brief, present_wiki_brief_write_confirmation,
    render_wiki_brief_markdown,
};
use wiki_catalog::{wiki_candidates, wiki_entries, wiki_show};
pub use wiki_catalog::{WikiListArgs, WikiShowArgs};
use wiki_event_presentation::{
    present_wiki_corrections, present_wiki_proposal_event, present_wiki_proposal_events,
};
use wiki_evidence_presentation::{
    present_wiki_correction_recurrence_report, present_wiki_episode_evaluation_report,
    present_wiki_live_episode_trace_report, present_wiki_promotion_chain_report,
};
use wiki_mutation_presentation::{present_wiki_mutation, WikiMutationResult};
use wiki_projection_presentation::{present_wiki_projection, present_wiki_projection_comparison};
use wiki_proposal_handoff::wiki_proposal_handoff;
pub use wiki_proposal_handoff::WikiProposalHandoffArgs;
use wiki_proposal_receipts::wiki_proposal_receipt;
use wiki_review_after_presentation::{
    build_review_after_report, present_review_after_report, WikiReviewAfterReportSummary,
};
use wiki_review_presentation::present_wiki_review_report;
use wiki_runtime_policy_ack_presentation::{
    build_runtime_policy_ack_report, present_runtime_policy_ack_report,
    present_runtime_policy_acknowledgement, present_runtime_policy_acknowledgements,
    WikiRuntimePolicyAckReportSummary,
};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use uuid::Uuid;

use super::project_audit::{
    audit_recommendations_for_project, AuditRecommendation, DocumentationAuditProfile,
};
use crate::offdesk::{
    assess_implementation_packet_detail, assess_implementation_packet_goal, assess_offdesk_mode,
    assess_work_slice_receipt, build_closeout_implementation_packet_coverage,
    build_graph_export_files, build_usage_records_with_policy, default_capability_registry,
    implementation_packet_from_path, implementation_packet_record_from_path,
    latest_implementation_packet_for_project, launch_background_command, launch_background_run,
    operator_safe_report, pending_approval_operator_views, poll_background_runs,
    receipt_decision_record as transition_receipt_decision_record, recommend_provider_fallback,
    reconcile_tasks_with_background_outcomes,
    resolve_decision_record as transition_resolve_decision_record, run_offdesk_tick,
    scan_and_emit_learning_signals, work_slice_execution_receipts_from_path, ActionApprovalRequest,
    AdaptiveWikiActivationMode, AdaptiveWikiAgentMode, AdaptiveWikiAgentModeFilter,
    AdaptiveWikiAuditAction, AdaptiveWikiAuditRecord, AdaptiveWikiCandidate,
    AdaptiveWikiCandidateInput, AdaptiveWikiConfidence, AdaptiveWikiEntry, AdaptiveWikiEntryEdit,
    AdaptiveWikiHumanCandidate, AdaptiveWikiHumanEntry, AdaptiveWikiKind,
    AdaptiveWikiLiveEpisodeFilter, AdaptiveWikiOrigin, AdaptiveWikiProjectionBudget,
    AdaptiveWikiProjectionPolicy, AdaptiveWikiProjectionReviewExpiredPolicy,
    AdaptiveWikiPromotionReceipt, AdaptiveWikiPromotionReceiptAuthority, AdaptiveWikiQuery,
    AdaptiveWikiReviewProposal, AdaptiveWikiReviewProposalAction,
    AdaptiveWikiReviewProposalDecision, AdaptiveWikiReviewProposalEventRecord,
    AdaptiveWikiReviewQueueFilter, AdaptiveWikiRuntimePolicyAckScopeMode, AdaptiveWikiScope,
    AdaptiveWikiScopeSuggestion, AdaptiveWikiSignalKind, AdaptiveWikiStore,
    AdaptiveWikiUsageContext, ApprovalLedger, ApprovalStatus, BackgroundLaunchOutcome,
    BackgroundLaunchRequest, BackgroundProbe, BackgroundRecoveryAcknowledgement,
    BackgroundRecoveryDecision, BackgroundRunStore, BackgroundRunnerKind, BackgroundRunnerPhase,
    CapabilityArtifactRef, CapabilityDescriptor,
    CloseoutDecisionResolution as WorkflowCloseoutDecisionResolution,
    CloseoutImplementationPacketCoverage, CloseoutImplementationPacketCoverageInput,
    CloseoutPacketCoverageDetail, CloseoutReviewRecord, CloseoutVerdict, DecisionLedger,
    DecisionReceiptInput, DecisionRecord, DecisionRecordView, DecisionResolutionInput,
    DecisionStatus, DecisionValidationIssue, ExecutionBrief, ImplementationPacket,
    ImplementationPacketCoverageStatus, ImplementationPacketExecutionEvidence,
    ImplementationPacketSummary, LatestImplementationPacket, LocalCommandLaunchSpec,
    MutationRestoreOperation, MutationRestorePlan, MutationSnapshot, MutationSnapshotStore,
    MutationSnapshotVerification, OffdeskModeAssessment, OffdeskModeLifecycle,
    OffdeskNextSafeAction, OffdeskPendingApprovalView, OffdeskPlanReviewDecision, OffdeskTask,
    OffdeskTaskInput, OffdeskTaskStatus, OffdeskTaskStore, OffdeskTaskView, OffdeskTickOptions,
    OperatorPauseStore, PendingActionApproval, ProviderCapacityState, ProviderCapacityStore,
    ProviderFallbackRecommendation, RiskLevel, SchedulerGate, SchedulerGateRequest,
    SchedulerGateStatus, TaskResumeState, TaskResumeStore, WorkSliceExecutionReceipt,
    WorkSliceExecutionStatus, WORK_SLICE_EXECUTION_RECEIPTS_FILE,
};
use crate::session::{get_profile_dir, resolved_app_dir_path, DEFAULT_PROFILE};

#[derive(Subcommand)]
pub enum OffdeskCommands {
    /// List hosted harness agent profile contracts
    Harnesses(JsonArgs),

    /// Build a compact hosted harness start prompt from first-read artifacts
    HarnessPrompt(HarnessPromptArgs),

    /// Validate and register a read-only Offdesk planning artifact
    Plan(PlanArgs),

    /// List registered read-only Offdesk planning artifacts
    Plans(PlansArgs),

    /// Show one registered read-only Offdesk planning artifact
    PlanShow(PlanShowArgs),

    /// Record an operator review for a registered Offdesk planning artifact
    PlanReview(PlanReviewArgs),

    /// Build a read-only launch-preparation packet from an approved plan review
    PlanLaunchPrep(PlanLaunchPrepArgs),

    /// Render read-only Remote Operator projections for mobile/chat transports
    RemoteOperator {
        #[command(subcommand)]
        command: RemoteOperatorCommands,
    },

    /// List pending action approvals
    Pending(PendingArgs),

    /// Evaluate whether an offdesk capability may execute now
    Gate(GateArgs),

    /// Gate and record a background runner launch
    Launch(LaunchArgs),

    /// Enqueue a durable offdesk task
    Enqueue(EnqueueArgs),

    /// Run one offdesk control-loop pass
    Tick(TickArgs),

    /// Show durable offdesk tasks
    Tasks(TasksArgs),

    /// List canonical Offdesk decision records
    Decisions(DecisionsArgs),

    /// Inspect one canonical Offdesk decision record
    Decision(DecisionArgs),

    /// Show provider capacity cooldown state
    ProviderCapacity(JsonArgs),

    /// Recommend provider/model fallbacks without retargeting tasks
    ProviderFallback(ProviderFallbackArgs),

    /// Mark a durable task cancelled without stopping its background runner
    CancelTask(CancelTaskArgs),

    /// Halt all new offdesk dispatch until resumed (existing runs keep polling)
    Pause(PauseArgs),

    /// Clear the global operator pause so new dispatch can proceed again
    Unpause(UnpauseArgs),

    /// Show the current global operator pause state
    #[command(name = "pause-status")]
    PauseStatus(JsonArgs),

    /// Emit adaptive-wiki learning candidates from observed denials, failures,
    /// and resume-recovery rows (recommendation-only; runs each event once)
    #[command(name = "learning-scan")]
    LearningScan(JsonArgs),

    /// Requeue a failed, resume-pending, or cancelled durable task
    RetryTask(RetryTaskArgs),

    /// Accept recovery for a resume-pending task and requeue it
    ResumeTask(TaskLifecycleArgs),

    /// Discard a failed or resume-pending task
    AbandonTask(TaskLifecycleArgs),

    /// Poll background runner probes, persist phase transitions, and reconcile task status
    Poll(PollArgs),

    /// Approve the oldest or targeted pending action
    #[command(alias = "approve")]
    Ok(ResolveArgs),

    /// Deny the oldest or targeted pending action
    #[command(alias = "deny")]
    Cancel(ResolveArgs),

    /// Show task resume artifacts
    Resume(JsonArgs),

    /// Show background runner recovery probes
    Background(JsonArgs),

    /// Acknowledge a stale or failed background probe after linked tasks are cancelled
    BackgroundAck(BackgroundAckArgs),

    /// Show Task Team capability metadata
    Capabilities(JsonArgs),

    /// List pre-mutation checkpoint snapshots
    Snapshots(JsonArgs),

    /// Show and verify a pre-mutation checkpoint snapshot
    Snapshot(MutationSnapshotArgs),

    /// Show a dry-run rollback plan without modifying files
    RestorePlan(MutationSnapshotArgs),

    /// Emit a sanitized read-only debug bundle
    DebugBundle(DebugBundleArgs),

    /// Summarize read-only Offdesk maintenance risks
    MaintenanceReport(MaintenanceReportArgs),

    /// Create or reuse an approval request for a maintenance action
    MaintenanceRequest(MaintenanceRequestArgs),

    /// Generate a Marp-compatible review deck from a read-only Offdesk artifact
    Deck(DeckArgs),

    /// Generate a mandatory closeout plan and commercial review packet
    Closeout(CloseoutArgs),

    /// Record a reviewed closeout verdict without applying file operations
    CloseoutReview(CloseoutReviewArgs),

    /// Resolve a closeout receipt open decision without applying file operations
    CloseoutDecision(CloseoutDecisionArgs),

    /// Retire an evidence-incomplete historical closeout without accepting truth
    CloseoutRetire(CloseoutRetireArgs),

    /// Inspect adaptive wiki candidates, entries, projections, and lint
    Wiki(WikiArgs),
}

#[derive(Args)]
pub struct PendingArgs {
    /// Include resolved and expired approvals
    #[arg(long)]
    all: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct GateArgs {
    /// Capability ID from `forager offdesk capabilities`
    capability_id: String,

    /// Project key for approval and audit correlation
    #[arg(long)]
    project_key: String,

    /// Request ID for approval and audit correlation
    #[arg(long)]
    request_id: String,

    /// Task ID for approval and audit correlation
    #[arg(long)]
    task_id: String,

    /// Mutation class to match against an ExecutionBrief envelope
    #[arg(long)]
    mutation_class: Option<String>,

    /// JSON file containing an ExecutionBrief
    #[arg(long)]
    brief: Option<PathBuf>,

    /// Provider ID to check against provider capacity cooldown state
    #[arg(long)]
    provider_id: Option<String>,

    /// Provider model to check against provider capacity cooldown state
    #[arg(long)]
    model: Option<String>,

    /// Artifact reference in ARTIFACT_ID=PATH form
    #[arg(long = "artifact", value_parser = parse_artifact_ref)]
    artifact_refs: Vec<CapabilityArtifactRef>,

    /// Artifact kind used to match adaptive wiki entries
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Agent work mode used to match adaptive wiki entries
    #[arg(long, value_parser = parse_adaptive_wiki_agent_mode)]
    agent_mode: Option<AdaptiveWikiAgentMode>,

    /// Operator-safe action preview
    #[arg(long, default_value = "")]
    preview: String,

    /// Reason shown when approval is required
    #[arg(long, default_value = "")]
    reason: String,

    /// Source surface recorded on generated approval rows
    #[arg(long, default_value = "cli")]
    source_surface: String,

    /// Pending approval TTL in minutes
    #[arg(long, default_value_t = 30)]
    ttl_minutes: i64,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct LaunchArgs {
    /// Capability ID from `forager offdesk capabilities`
    capability_id: String,

    /// Runner backend to record: local-tmux, local-background, github-runner, remote-worker
    #[arg(long, value_parser = parse_background_runner_kind)]
    runner: BackgroundRunnerKind,

    /// Project key for approval and audit correlation
    #[arg(long)]
    project_key: String,

    /// Request ID for approval and audit correlation
    #[arg(long)]
    request_id: String,

    /// Task ID for approval and audit correlation
    #[arg(long)]
    task_id: String,

    /// Mutation class to match against an ExecutionBrief envelope
    #[arg(long)]
    mutation_class: Option<String>,

    /// JSON file containing an ExecutionBrief
    #[arg(long)]
    brief: Option<PathBuf>,

    /// Provider ID to check against provider capacity cooldown state
    #[arg(long)]
    provider_id: Option<String>,

    /// Provider model to check against provider capacity cooldown state
    #[arg(long)]
    model: Option<String>,

    /// Artifact reference in ARTIFACT_ID=PATH form
    #[arg(long = "artifact", value_parser = parse_artifact_ref)]
    artifact_refs: Vec<CapabilityArtifactRef>,

    /// Implementation packet JSON or artifact directory to bind to this launch
    #[arg(long)]
    implementation_packet: Option<PathBuf>,

    /// Artifact kind used to match adaptive wiki entries
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Agent work mode used to match adaptive wiki entries
    #[arg(long, value_parser = parse_adaptive_wiki_agent_mode)]
    agent_mode: Option<AdaptiveWikiAgentMode>,

    /// Stable ticket ID. Generated if omitted.
    #[arg(long)]
    ticket_id: Option<String>,

    /// Redacted launch spec summary to store with the ticket
    #[arg(long)]
    launch_spec: Option<String>,

    /// Shell command to execute for local-background or local-tmux runners
    #[arg(long = "cmd")]
    command: Option<String>,

    /// Working directory for --cmd. Defaults to the current directory.
    #[arg(long)]
    workdir: Option<PathBuf>,

    /// Log artifact path for --cmd stdout and stderr
    #[arg(long)]
    log_artifact: Option<PathBuf>,

    /// Result sidecar path used by poll to mark the ticket completed
    #[arg(long)]
    result_artifact: Option<PathBuf>,

    /// Whether a local runtime handle is alive immediately after launch
    #[arg(long, default_value_t = true)]
    runtime_alive: bool,

    /// Whether a local_background launch spec can be reconstructed after restart
    #[arg(long)]
    provider_launch_spec_reconstructable: bool,

    /// External ack timeout in seconds
    #[arg(long, default_value_t = 300)]
    ack_timeout_sec: i64,

    /// Operator-safe action preview
    #[arg(long, default_value = "")]
    preview: String,

    /// Reason shown when approval is required
    #[arg(long, default_value = "")]
    reason: String,

    /// Source surface recorded on generated approval rows
    #[arg(long, default_value = "cli")]
    source_surface: String,

    /// Pending approval TTL in minutes
    #[arg(long, default_value_t = 30)]
    ttl_minutes: i64,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct EnqueueArgs {
    /// Capability ID from `forager offdesk capabilities`
    capability_id: String,

    /// Runner backend to use: local-tmux or local-background
    #[arg(long, value_parser = parse_background_runner_kind)]
    runner: BackgroundRunnerKind,

    /// Project key for approval and audit correlation
    #[arg(long)]
    project_key: String,

    /// Request ID for approval and audit correlation
    #[arg(long)]
    request_id: String,

    /// Task ID. Generated if omitted.
    #[arg(long)]
    task_id: Option<String>,

    /// Shell command to execute when the task is dispatched
    #[arg(long = "cmd")]
    command: String,

    /// Working directory for --cmd. Defaults to the current directory.
    #[arg(long)]
    workdir: Option<PathBuf>,

    /// JSON file containing an ExecutionBrief to store with the task
    #[arg(long)]
    brief: Option<PathBuf>,

    /// Mutation class to match against an ExecutionBrief envelope
    #[arg(long)]
    mutation_class: Option<String>,

    /// Provider ID to check against provider capacity cooldown state when dispatched
    #[arg(long)]
    provider_id: Option<String>,

    /// Provider model to check against provider capacity cooldown state when dispatched
    #[arg(long)]
    model: Option<String>,

    /// Artifact reference in ARTIFACT_ID=PATH form
    #[arg(long = "artifact", value_parser = parse_artifact_ref)]
    artifact_refs: Vec<CapabilityArtifactRef>,

    /// Implementation packet JSON or artifact directory to bind to this task
    #[arg(long)]
    implementation_packet: Option<PathBuf>,

    /// Artifact kind used to match adaptive wiki entries
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Agent work mode used to match adaptive wiki entries
    #[arg(long, value_parser = parse_adaptive_wiki_agent_mode)]
    agent_mode: Option<AdaptiveWikiAgentMode>,

    /// Operator-safe action preview
    #[arg(long, default_value = "")]
    preview: String,

    /// Reason shown when approval is required
    #[arg(long, default_value = "")]
    reason: String,

    /// Do not dispatch before this RFC3339 timestamp
    #[arg(long)]
    not_before: Option<String>,

    /// Log artifact path for command stdout and stderr
    #[arg(long)]
    log_artifact: Option<PathBuf>,

    /// Result sidecar path used by tick to mark the task completed
    #[arg(long)]
    result_artifact: Option<PathBuf>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct TickArgs {
    /// Maximum queued tasks to dispatch in this tick
    #[arg(long, default_value_t = 10)]
    limit: usize,

    /// Restrict this tick to one project key
    #[arg(long)]
    project_key: Option<String>,

    /// Restrict this tick to one task ID
    #[arg(long)]
    task_id: Option<String>,

    /// Treat previous free lock metadata as stale after this many minutes
    #[arg(long, default_value_t = 30)]
    lock_stale_minutes: i64,

    /// Record notification cooldown state in minutes while polling background runs
    #[arg(long)]
    notify_cooldown_minutes: Option<i64>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct PollArgs {
    /// Ticket ID to poll. Defaults to all tickets.
    ticket_id: Option<String>,

    /// Record notification cooldown state in minutes
    #[arg(long)]
    notify_cooldown_minutes: Option<i64>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct BackgroundAckArgs {
    /// Background ticket ID to acknowledge
    ticket_id: String,

    /// Operator reason for suppressing further recovery attention
    #[arg(long)]
    reason: String,

    /// Operator or surface recording this acknowledgement
    #[arg(long, default_value = "cli")]
    by: String,

    /// Source surface recorded on the acknowledgement
    #[arg(long, default_value = "cli")]
    source_surface: String,

    /// Permit acknowledgement when no durable task is linked to the background ticket
    #[arg(long)]
    allow_unlinked: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct ResolveArgs {
    /// Approval ID to resolve. Defaults to the oldest pending approval.
    approval_id: Option<String>,

    /// Operator or surface resolving this approval
    #[arg(long, default_value = "cli")]
    by: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct JsonArgs {
    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct HarnessPromptArgs {
    /// Hosted harness ID from `forager offdesk harnesses`
    harness_id: String,

    /// Short task instruction for the hosted harness
    #[arg(long)]
    task: String,

    /// Artifact or source file the hosted harness must read first
    #[arg(long = "first-read")]
    first_reads: Vec<PathBuf>,

    /// Result sidecar path the hosted harness should write or inspect
    #[arg(long)]
    result_artifact: Option<PathBuf>,

    /// Working directory the hosted harness should treat as the task root
    #[arg(long)]
    workdir: Option<PathBuf>,

    /// Write the generated prompt markdown to this path
    #[arg(long)]
    output: Option<PathBuf>,

    /// Override the total first-read artifact budget in bytes
    #[arg(long)]
    max_first_read_total_bytes: Option<u64>,

    /// Fail when first-read artifacts are missing or exceed the budget
    #[arg(long)]
    strict_first_read_budget: bool,

    /// Output packet metadata as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct PlanArgs {
    /// `offdesk_multiturn_plan.v1` or `offdesk_planner_council.v1` JSON to register
    input: PathBuf,

    /// Optional project key for correlation
    #[arg(long)]
    project_key: Option<String>,

    /// Optional request ID for correlation
    #[arg(long)]
    request_id: Option<String>,

    /// Optional task ID for correlation
    #[arg(long)]
    task_id: Option<String>,

    /// Validate without writing profile-local registry artifacts
    #[arg(long)]
    dry_run: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct PlansArgs {
    /// Filter by project key
    #[arg(long)]
    project_key: Option<String>,

    /// Filter by task ID
    #[arg(long)]
    task_id: Option<String>,

    /// Filter by planning profile key
    #[arg(long)]
    profile_key: Option<String>,

    /// Filter by artifact kind, such as offdesk_multiturn_plan or offdesk_planner_council
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Return only the newest matching registration
    #[arg(long)]
    latest: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct PlanShowArgs {
    /// Plan ID from `forager offdesk plans`, or a registration/source path
    plan_ref: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct PlanReviewArgs {
    /// Plan ID from `forager offdesk plans`, or a registration/source path
    plan_ref: String,

    /// Operator review decision. This command never enqueues or launches work.
    #[arg(long, value_enum)]
    decision: OffdeskPlanReviewDecision,

    /// Reviewer or reviewing model label
    #[arg(long, default_value = "operator")]
    reviewer: String,

    /// Model/provider label used for review
    #[arg(long)]
    review_provider: Option<String>,

    /// Optional path to the raw review output
    #[arg(long)]
    review_file: Option<PathBuf>,

    /// Required review rationale. Secrets are redacted before persistence.
    #[arg(long)]
    reason: String,

    /// Blocking issue reported by review; may be passed multiple times
    #[arg(long = "blocker")]
    blockers: Vec<String>,

    /// Follow-up requested by review; may be passed multiple times
    #[arg(long = "follow-up")]
    followups: Vec<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct PlanLaunchPrepArgs {
    /// Plan ID from `forager offdesk plans`, or a registration/source path
    plan_ref: String,

    /// Use a specific approved review ID instead of the latest review
    #[arg(long)]
    review_id: Option<String>,

    /// Operator or surface preparing the packet
    #[arg(long, default_value = "operator")]
    prepared_by: String,

    /// Optional preparation note. Secrets are redacted before persistence.
    #[arg(long)]
    notes: Option<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Subcommand)]
pub enum RemoteOperatorCommands {
    /// Render a read-only status projection for a remote operator surface
    Status(RemoteOperatorStatusArgs),

    /// Render read-only pending approval summaries without resolving or expiring them
    Pending(RemoteOperatorPendingArgs),

    /// Render read-only Offdesk plan summaries for a remote operator surface
    Plans(RemoteOperatorPlansArgs),

    /// Render one read-only Offdesk plan detail projection
    Show(RemoteOperatorShowArgs),
}

#[derive(Args)]
pub struct RemoteOperatorStatusArgs {
    /// Remote transport label used for projection metadata
    #[arg(long, default_value = "telegram")]
    transport: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct RemoteOperatorPendingArgs {
    /// Remote transport label used for projection metadata
    #[arg(long, default_value = "telegram")]
    transport: String,

    /// Include resolved approvals in addition to pending approval rows
    #[arg(long)]
    all: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct RemoteOperatorPlansArgs {
    /// Remote transport label used for projection metadata
    #[arg(long, default_value = "telegram")]
    transport: String,

    /// Filter by project key
    #[arg(long)]
    project_key: Option<String>,

    /// Filter by task ID
    #[arg(long)]
    task_id: Option<String>,

    /// Filter by planning profile key
    #[arg(long)]
    profile_key: Option<String>,

    /// Filter by artifact kind, such as offdesk_multiturn_plan or offdesk_planner_council
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Return only the newest matching registration
    #[arg(long)]
    latest: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct RemoteOperatorShowArgs {
    /// Remote transport label used for projection metadata
    #[arg(long, default_value = "telegram")]
    transport: String,

    /// Plan ID from `forager offdesk plans`, or a registration/source path
    plan_ref: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct TasksArgs {
    /// Filter tasks by project key
    #[arg(long)]
    project_key: Option<String>,

    /// Filter tasks by exact task ID
    #[arg(long)]
    task_id: Option<String>,

    /// Filter tasks by status. Repeat for multiple statuses.
    #[arg(long, value_parser = parse_offdesk_task_status)]
    status: Vec<OffdeskTaskStatus>,

    /// Return only the newest matching task by updated_at
    #[arg(long)]
    latest: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiProposalEventsArgs {
    /// Filter lifecycle events by proposal id
    #[arg(long)]
    proposal_id: Option<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiRecordProposalEventArgs {
    /// Curator review proposal id
    proposal_id: String,

    /// Operator decision for the proposal
    #[arg(long, value_parser = parse_adaptive_wiki_proposal_decision)]
    decision: AdaptiveWikiReviewProposalDecision,

    /// Proposal action that was reviewed
    #[arg(long, value_parser = parse_adaptive_wiki_review_action)]
    proposal_action: Option<AdaptiveWikiReviewProposalAction>,

    /// Proposal subject kind, such as entry or candidate
    #[arg(long, default_value = "")]
    subject_kind: String,

    /// Proposal subject id
    #[arg(long, default_value = "")]
    subject_id: String,

    /// Operator or surface recording the decision
    #[arg(long, default_value = "cli")]
    by: String,

    /// Required reason for accepting, rejecting, or superseding the proposal
    #[arg(long)]
    reason: String,

    /// Evidence ref that supports this proposal decision
    #[arg(long = "evidence-ref")]
    evidence_refs: Vec<String>,

    /// Previous proposal id superseded by this decision
    #[arg(long)]
    supersedes: Option<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiCloseProposalArgs {
    /// Current curator review proposal id
    proposal_id: String,

    /// Operator or surface recording the decision
    #[arg(long, default_value = "cli")]
    by: String,

    /// Required reason for accepting, rejecting, or superseding the proposal
    #[arg(long)]
    reason: String,

    /// Extra evidence ref that supports this proposal decision
    #[arg(long = "evidence-ref")]
    evidence_refs: Vec<String>,

    /// Previous proposal id superseded by this decision
    #[arg(long)]
    supersedes: Option<String>,

    /// Allow recording a new lifecycle event for a non-stale decided proposal
    #[arg(long)]
    allow_decided: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiProposalReceiptArgs {
    /// Curator review proposal id that the receipt should link
    proposal_id: String,

    /// Adaptive wiki mutation audit id produced by the executed mutation command
    #[arg(long)]
    audit_id: String,

    /// Proposal lifecycle event id recorded for the operator decision
    #[arg(long)]
    event_id: String,

    /// Previewed handoff command that the operator executed or reviewed
    #[arg(long)]
    command: String,

    /// Write the sanitized receipt JSON to an audit artifact file
    #[arg(long)]
    export: bool,

    /// Write the sanitized receipt JSON to this path
    #[arg(long)]
    output: Option<PathBuf>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct DebugBundleArgs {
    /// Output as JSON
    #[arg(long)]
    json: bool,

    /// Write the sanitized bundle JSON to a diagnostics file
    #[arg(long)]
    export: bool,

    /// Write the sanitized bundle JSON to this path
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Args)]
pub struct MaintenanceReportArgs {
    /// Output as JSON
    #[arg(long)]
    json: bool,

    /// Hours before review_after expiry to flag adaptive wiki entries
    #[arg(long, default_value_t = 168)]
    wiki_review_near_expiry_hours: i64,

    /// Hours before runtime policy acknowledgement expiry to flag attention
    #[arg(long, default_value_t = 6)]
    wiki_runtime_ack_near_expiry_hours: i64,
}

#[derive(Args)]
pub struct MaintenanceRequestArgs {
    /// Bounded maintenance action kind to request approval for
    #[arg(long, value_parser = parse_maintenance_action_kind)]
    kind: MaintenanceActionKind,

    /// Project key for approval and audit correlation
    #[arg(long)]
    project_key: String,

    /// Request ID for approval and audit correlation
    #[arg(long)]
    request_id: String,

    /// Task ID for approval identity. Defaults to maintenance-{kind}-{target-id}
    #[arg(long)]
    task_id: Option<String>,

    /// Optional target identifier used for approval deduplication and review
    #[arg(long)]
    target_id: Option<String>,

    /// Override the default risk for this maintenance kind
    #[arg(long, value_parser = parse_risk_level)]
    risk: Option<RiskLevel>,

    /// Operator-safe action preview
    #[arg(long)]
    preview: String,

    /// Reason shown when approval is required
    #[arg(long)]
    reason: String,

    /// Source surface recorded on generated approval rows
    #[arg(long, default_value = "cli")]
    source_surface: String,

    /// Pending approval TTL in minutes
    #[arg(long, default_value_t = 30)]
    ttl_minutes: i64,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct CloseoutArgs {
    /// Project key to close out. Defaults to all projects in the profile.
    #[arg(long)]
    project_key: Option<String>,

    /// Request ID to close out
    #[arg(long)]
    request_id: Option<String>,

    /// Task ID to close out
    #[arg(long)]
    task_id: Option<String>,

    /// Optional project workdir for read-only git status evidence
    #[arg(long)]
    workdir: Option<PathBuf>,

    /// Include read-only git status and diff-stat from --workdir or matched task workdir
    #[arg(long)]
    include_git: bool,

    /// Commercial model/provider label expected to review move/delete/archive decisions
    #[arg(long, default_value = "commercial")]
    review_provider: String,

    /// Write closeout artifacts to this directory
    #[arg(long)]
    output: Option<PathBuf>,

    /// Accepted for explicit operator intent; closeout never applies file operations
    #[arg(long)]
    dry_run: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct CloseoutReviewArgs {
    /// Closeout ID from `forager offdesk closeout`
    #[arg(long)]
    closeout_id: Option<String>,

    /// Closeout artifact directory containing closeout_plan.json
    #[arg(long)]
    artifact_dir: Option<PathBuf>,

    /// Commercial review verdict
    #[arg(long, value_enum)]
    verdict: CloseoutReviewVerdict,

    /// Reviewer or reviewing model label
    #[arg(long, default_value = "operator")]
    reviewer: String,

    /// Commercial model/provider label used for review
    #[arg(long)]
    review_provider: Option<String>,

    /// Optional path to the raw commercial review output
    #[arg(long)]
    review_file: Option<PathBuf>,

    /// Unsafe operation reported by review; may be passed multiple times
    #[arg(long)]
    unsafe_operation: Vec<String>,

    /// Missing evidence reported by review; may be passed multiple times
    #[arg(long)]
    missing_evidence: Vec<String>,

    /// Required first-read path reported by review; may be passed multiple times
    #[arg(long)]
    required_first_read: Vec<String>,

    /// Short review note. Secrets are redacted before persistence.
    #[arg(long)]
    notes: Option<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct CloseoutDecisionArgs {
    /// Closeout ID from `forager offdesk closeout`
    #[arg(long)]
    closeout_id: Option<String>,

    /// Closeout artifact directory containing closeout_plan.json
    #[arg(long)]
    artifact_dir: Option<PathBuf>,

    /// Open decision kind to resolve, for example archive_review
    #[arg(long)]
    kind: String,

    /// Resolution to record. This command never moves, archives, or deletes files.
    #[arg(long, value_enum)]
    decision: CloseoutDecisionResolutionArg,

    /// Reviewer or operator label
    #[arg(long, default_value = "operator")]
    reviewer: String,

    /// Required rationale for the decision. Secrets are redacted before persistence.
    #[arg(long)]
    reason: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct CloseoutRetireArgs {
    /// Closeout ID from `forager offdesk closeout`
    #[arg(long)]
    closeout_id: Option<String>,

    /// Closeout artifact directory containing closeout_plan.json
    #[arg(long)]
    artifact_dir: Option<PathBuf>,

    /// Reviewer or operator label
    #[arg(long, default_value = "operator")]
    reviewer: String,

    /// Required rationale for retiring the closeout as evidence-incomplete.
    #[arg(long)]
    reason: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum CloseoutReviewVerdict {
    Approved,
    Revise,
    Blocked,
}

impl From<CloseoutReviewVerdict> for CloseoutVerdict {
    fn from(verdict: CloseoutReviewVerdict) -> Self {
        match verdict {
            CloseoutReviewVerdict::Approved => Self::Approved,
            CloseoutReviewVerdict::Revise => Self::Revise,
            CloseoutReviewVerdict::Blocked => Self::Blocked,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum CloseoutDecisionResolutionArg {
    PreserveInPlace,
}

impl From<CloseoutDecisionResolutionArg> for WorkflowCloseoutDecisionResolution {
    fn from(decision: CloseoutDecisionResolutionArg) -> Self {
        match decision {
            CloseoutDecisionResolutionArg::PreserveInPlace => Self::PreserveInPlace,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum MaintenanceActionKind {
    RuntimeRecovery,
    WikiRuntimeAck,
    WikiReviewAfter,
    WikiMutation,
    ProviderCapacity,
    ArtifactCleanup,
    ServiceRestart,
    SystemChange,
}

impl MaintenanceActionKind {
    fn cli_value(self) -> &'static str {
        match self {
            Self::RuntimeRecovery => "runtime_recovery",
            Self::WikiRuntimeAck => "wiki_runtime_ack",
            Self::WikiReviewAfter => "wiki_review_after",
            Self::WikiMutation => "wiki_mutation",
            Self::ProviderCapacity => "provider_capacity",
            Self::ArtifactCleanup => "artifact_cleanup",
            Self::ServiceRestart => "service_restart",
            Self::SystemChange => "system_change",
        }
    }

    fn action_id(self) -> &'static str {
        match self {
            Self::RuntimeRecovery => "maintenance.runtime_recovery",
            Self::WikiRuntimeAck => "maintenance.wiki_runtime_ack",
            Self::WikiReviewAfter => "maintenance.wiki_review_after",
            Self::WikiMutation => "maintenance.wiki_mutation",
            Self::ProviderCapacity => "maintenance.provider_capacity",
            Self::ArtifactCleanup => "maintenance.artifact_cleanup",
            Self::ServiceRestart => "maintenance.service_restart",
            Self::SystemChange => "maintenance.system_change",
        }
    }

    fn default_risk(self) -> RiskLevel {
        match self {
            Self::RuntimeRecovery | Self::ProviderCapacity => RiskLevel::RuntimeMutation,
            Self::WikiRuntimeAck | Self::WikiReviewAfter | Self::WikiMutation => {
                RiskLevel::CanonicalMutation
            }
            Self::ArtifactCleanup => RiskLevel::Destructive,
            Self::ServiceRestart | Self::SystemChange => RiskLevel::ExternalSideEffect,
        }
    }
}

#[derive(Args)]
pub struct ProviderFallbackArgs {
    /// Current provider ID that is blocked or under review
    #[arg(long)]
    provider_id: String,

    /// Current provider model to exclude from fallback candidates
    #[arg(long)]
    model: Option<String>,

    /// Runner role used to filter compatible cross-provider candidates
    #[arg(long, default_value = "worker")]
    runner_role: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct CancelTaskArgs {
    /// Offdesk task ID to cancel
    task_id: String,

    /// Operator reason to store on the task
    #[arg(long)]
    reason: Option<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct PauseArgs {
    /// Reason to record for the pause
    #[arg(long)]
    reason: Option<String>,

    /// Actor engaging the pause
    #[arg(long, default_value = "cli")]
    by: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct UnpauseArgs {
    /// Actor clearing the pause
    #[arg(long, default_value = "cli")]
    by: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct TaskLifecycleArgs {
    /// Offdesk task ID to update
    task_id: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct DecisionsArgs {
    /// Filter by project key
    #[arg(long)]
    project_key: Option<String>,

    /// Filter by task ID
    #[arg(long)]
    task_id: Option<String>,

    /// Filter by decision status, such as user_pending or auto_resolved
    #[arg(long)]
    status: Vec<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct DecisionArgs {
    #[command(subcommand)]
    command: DecisionCommands,
}

#[derive(Subcommand)]
pub enum DecisionCommands {
    /// Show one canonical Offdesk decision record
    Show(DecisionShowArgs),

    /// Resolve a decision into an append-only execution handoff
    Resolve(DecisionResolveArgs),

    /// Close a handoff-ready decision with an append-only receipt
    Receipt(DecisionReceiptArgs),

    /// Ingest a Telegram relay result into the canonical decision ledger
    IngestTelegram(DecisionIngestTelegramArgs),

    /// Promote Telegram freeform feedback into the canonical decision inbox
    IngestTelegramFeedback(DecisionIngestTelegramFeedbackArgs),
}

#[derive(Args)]
pub struct DecisionShowArgs {
    /// Decision ID to inspect
    decision_id: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct DecisionResolveArgs {
    /// Decision ID to resolve
    decision_id: String,

    /// Operator or policy choice, such as continue, revise, block, stop, deny, or defer
    #[arg(long)]
    decision: String,

    /// Required rationale or natural-language direction for revise/block/custom choices
    #[arg(long, default_value = "")]
    note: String,

    /// Actor recording the resolution
    #[arg(long, default_value = "operator")]
    by: String,

    /// Override execution handoff target
    #[arg(long)]
    target: Option<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct DecisionReceiptArgs {
    /// Decision ID to close
    decision_id: String,

    /// Actor recording the receipt
    #[arg(long, default_value = "operator")]
    by: String,

    /// Result status for the consumed handoff
    #[arg(long, default_value = "closed")]
    result_status: String,

    /// Evidence summary line. Repeat for multiple lines.
    #[arg(long = "evidence")]
    evidence_summary: Vec<String>,

    /// Remaining review item. Repeat for multiple lines.
    #[arg(long = "remaining-review")]
    remaining_review: Vec<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct RetryTaskArgs {
    /// Offdesk task ID to retry
    task_id: String,

    /// Supersede matching denied approval rows so the next tick creates a new approval
    #[arg(long)]
    new_approval: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct MutationSnapshotArgs {
    /// Mutation snapshot ID
    mutation_id: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiArgs {
    #[command(subcommand)]
    command: WikiCommands,
}

#[derive(Subcommand)]
pub enum WikiCommands {
    /// List first-class adaptive wiki correction records
    Corrections(JsonArgs),

    /// List adaptive wiki review proposal lifecycle events
    ProposalEvents(WikiProposalEventsArgs),

    /// Record an operator decision for a curator review proposal
    RecordProposalEvent(WikiRecordProposalEventArgs),

    /// Accept a current curator review proposal and copy its metadata into the event
    AcceptProposal(WikiCloseProposalArgs),

    /// Reject a current curator review proposal and copy its metadata into the event
    RejectProposal(WikiCloseProposalArgs),

    /// Mark a current curator review proposal superseded and copy its metadata into the event
    SupersedeProposal(WikiCloseProposalArgs),

    /// Preview the governed mutation handoff command for a current proposal
    ProposalHandoff(WikiProposalHandoffArgs),

    /// Link a handoff preview, mutation audit, and lifecycle event without mutating state
    ProposalReceipt(WikiProposalReceiptArgs),

    /// List adaptive wiki candidates
    Candidates(WikiListArgs),

    /// List adaptive wiki entries
    Entries(WikiListArgs),

    /// Show one adaptive wiki entry or candidate
    Show(WikiShowArgs),

    /// Show the AI projection for a scope
    Projection(WikiProjectionArgs),

    /// Render a compact, skepticism-aware knowledge brief for session start
    Brief(WikiBriefArgs),

    /// List strict runtime projection policy acknowledgements
    RuntimePolicyAcks(JsonArgs),

    /// Report strict runtime projection acknowledgements that need attention
    RuntimePolicyAckReport(WikiRuntimePolicyAckReportArgs),

    /// Report promoted entries whose review_after needs attention
    ReviewAfterReport(WikiReviewAfterReportArgs),

    /// Acknowledge strict review_after exclusion for runtime projection
    AckRuntimePolicy(WikiRuntimePolicyAckArgs),

    /// Lint adaptive wiki state
    Lint(JsonArgs),

    /// Export adaptive wiki state as a one-way markdown vault
    ExportMarkdown(WikiExportMarkdownArgs),

    /// Export a read-only adaptive wiki tag graph
    Graph(WikiGraphArgs),

    /// Generate a recommendation-only adaptive wiki review report
    Review(WikiReviewArgs),

    /// Evaluate one adaptive wiki entry across in-scope and out-of-scope projections
    EvaluateEpisode(WikiEpisodeArgs),

    /// Trace live task/probe/wiki evidence for adaptive behavior review
    EpisodeTrace(WikiEpisodeTraceArgs),

    /// Evaluate whether corrections recur after an entry is promoted
    EvaluateRecurrence(WikiRecurrenceArgs),

    /// Reconstruct the evidence chain captured at promotion time
    PromotionChain(WikiPromotionChainArgs),

    /// Record an operator-authored learning candidate (e.g. from a doc review)
    #[command(name = "record-candidate")]
    RecordCandidate(WikiRecordCandidateArgs),

    /// Promote a candidate into a scoped wiki entry
    Promote(WikiPromoteArgs),

    /// Reject a candidate without creating an entry
    Reject(WikiRejectArgs),

    /// Change an entry scope
    Rescope(WikiRescopeArgs),

    /// Edit an entry's classification, modes, text, or evidence refs in place
    Edit(WikiEditArgs),

    /// Add controlled/proposed tags to an entry (e.g. facet/* or domain/*)
    #[command(name = "add-tag")]
    AddTag(WikiAddTagArgs),

    /// Deprecate an entry so it no longer appears in AI projection
    Deprecate(WikiDeprecateArgs),

    /// Renew an entry review_after timestamp without changing scope or instruction
    RenewReviewAfter(WikiRenewReviewAfterArgs),

    /// Add a counterexample evidence ref to an entry
    AddCounterexample(WikiCounterexampleArgs),

    /// Attach governed runbook support refs to a procedure entry
    UpdateRunbook(WikiRunbookArgs),
}

#[derive(Args)]
pub struct WikiBriefArgs {
    /// Project key scope to match
    #[arg(long)]
    project_key: Option<String>,

    /// Artifact kind scope to match
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Agent work mode to project for (omit for shared/universal entries)
    #[arg(long, value_parser = parse_adaptive_wiki_agent_mode)]
    agent_mode: Option<AdaptiveWikiAgentMode>,

    /// Maximum entries in the brief
    #[arg(long, default_value_t = 12)]
    max_entries: usize,

    /// Write the brief to this path instead of stdout
    #[arg(long)]
    out: Option<std::path::PathBuf>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiProjectionArgs {
    /// Session/request scope to match
    #[arg(long)]
    session_id: Option<String>,

    /// Project key scope to match
    #[arg(long)]
    project_key: Option<String>,

    /// Artifact kind scope to match
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Agent work mode scope to match
    #[arg(long, value_parser = parse_adaptive_wiki_agent_mode)]
    agent_mode: Option<AdaptiveWikiAgentMode>,

    /// Use the scheduler's shared-only default when no agent mode is supplied.
    #[arg(long, hide = true)]
    runtime_agent_mode_default: bool,

    /// Return the projection policy report instead of only selected entries
    #[arg(long)]
    report: bool,

    /// Compare default warn policy with strict review_after exclusion
    #[arg(long)]
    compare_review_expired_policy: bool,

    /// Maximum selected projection entries
    #[arg(long)]
    max_entries: Option<usize>,

    /// Maximum estimated runtime context characters
    #[arg(long)]
    max_context_chars: Option<usize>,

    /// Maximum characters kept per projected instruction; 0 disables truncation
    #[arg(long)]
    max_instruction_chars: Option<usize>,

    /// Exclude entries that are past review_after from the projection report
    #[arg(long)]
    exclude_review_expired: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiRuntimePolicyAckArgs {
    /// Session/request scope to match exactly
    #[arg(long)]
    session_id: Option<String>,

    /// Project key scope to match
    #[arg(long)]
    project_key: Option<String>,

    /// Artifact kind scope to match
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Agent work mode scope to match
    #[arg(long, value_parser = parse_adaptive_wiki_agent_mode)]
    agent_mode: Option<AdaptiveWikiAgentMode>,

    /// Acknowledgement scope: exact-query or project-artifact
    #[arg(long, default_value = "exact-query", value_parser = parse_adaptive_wiki_runtime_policy_ack_scope_mode)]
    scope_mode: AdaptiveWikiRuntimePolicyAckScopeMode,

    /// Maximum selected projection entries
    #[arg(long)]
    max_entries: Option<usize>,

    /// Maximum estimated runtime context characters
    #[arg(long)]
    max_context_chars: Option<usize>,

    /// Maximum characters kept per projected instruction; 0 disables truncation
    #[arg(long)]
    max_instruction_chars: Option<usize>,

    /// Acknowledgement TTL in hours
    #[arg(long, default_value_t = 24)]
    ttl_hours: i64,

    /// Operator reason for enabling strict runtime projection in this scope
    #[arg(long, default_value = "")]
    reason: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiRuntimePolicyAckReportArgs {
    /// Session/request scope to evaluate for query-specific ack applicability
    #[arg(long)]
    session_id: Option<String>,

    /// Project key scope to evaluate for query-specific ack applicability
    #[arg(long)]
    project_key: Option<String>,

    /// Artifact kind scope to evaluate for query-specific ack applicability
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Agent work mode scope to evaluate for query-specific ack applicability
    #[arg(long, value_parser = parse_adaptive_wiki_agent_mode)]
    agent_mode: Option<AdaptiveWikiAgentMode>,

    /// Maximum selected projection entries
    #[arg(long)]
    max_entries: Option<usize>,

    /// Maximum estimated runtime context characters
    #[arg(long)]
    max_context_chars: Option<usize>,

    /// Maximum characters kept per projected instruction; 0 disables truncation
    #[arg(long)]
    max_instruction_chars: Option<usize>,

    /// Mark active acknowledgements expiring within this many hours
    #[arg(long, default_value_t = 6)]
    near_expiry_hours: i64,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiReviewAfterReportArgs {
    /// Session/request scope to match
    #[arg(long)]
    session_id: Option<String>,

    /// Project key scope to match
    #[arg(long)]
    project_key: Option<String>,

    /// Artifact kind scope to match
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Agent work mode scope to match
    #[arg(long, value_parser = parse_adaptive_wiki_agent_mode)]
    agent_mode: Option<AdaptiveWikiAgentMode>,

    /// Mark entries needing review within this many hours
    #[arg(long, default_value_t = 168)]
    near_expiry_hours: i64,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiExportMarkdownArgs {
    /// Directory to write the markdown vault into; defaults to the active profile's wiki-vault
    #[arg(long)]
    output: Option<PathBuf>,

    /// Preview export files without writing them
    #[arg(long)]
    dry_run: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiGraphArgs {
    /// Optional directory to write graph.json and graph.md into
    #[arg(long)]
    output: Option<PathBuf>,

    /// Preview graph export files without writing them
    #[arg(long)]
    dry_run: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiReviewArgs {
    /// Preview recommendations without writing report files
    #[arg(long)]
    dry_run: bool,

    /// Show proposals that are open or have stale lifecycle decisions
    #[arg(long)]
    active_only: bool,

    /// Show proposals with non-stale accepted, rejected, or superseded decisions
    #[arg(long)]
    decided_only: bool,

    /// Show proposals whose latest lifecycle decision is stale
    #[arg(long)]
    stale_only: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiEpisodeArgs {
    /// Promoted adaptive wiki entry id expected to appear only in the in-scope projection
    entry_id: String,

    /// In-scope session/request id to match
    #[arg(long)]
    session_id: Option<String>,

    /// In-scope project key to match
    #[arg(long)]
    project_key: Option<String>,

    /// In-scope artifact kind to match
    #[arg(long)]
    artifact_kind: Option<String>,

    /// In-scope agent work mode to match
    #[arg(long, value_parser = parse_adaptive_wiki_agent_mode)]
    agent_mode: Option<AdaptiveWikiAgentMode>,

    /// Out-of-scope session/request id. Defaults to a generated non-matching value.
    #[arg(long)]
    out_session_id: Option<String>,

    /// Out-of-scope project key. Defaults to a generated non-matching value.
    #[arg(long)]
    out_project_key: Option<String>,

    /// Out-of-scope artifact kind. Defaults to a generated non-matching value.
    #[arg(long)]
    out_artifact_kind: Option<String>,

    /// Out-of-scope agent work mode. Defaults to a generated non-matching mode when possible.
    #[arg(long, value_parser = parse_adaptive_wiki_agent_mode)]
    out_agent_mode: Option<AdaptiveWikiAgentMode>,

    /// Preview the report without writing report files
    #[arg(long)]
    dry_run: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiEpisodeTraceArgs {
    /// Filter trace events by request id
    #[arg(long)]
    request_id: Option<String>,

    /// Filter trace events by task id
    #[arg(long)]
    task_id: Option<String>,

    /// Filter trace events by project key
    #[arg(long)]
    project_key: Option<String>,

    /// Filter trace events by artifact kind
    #[arg(long)]
    artifact_kind: Option<String>,

    /// Filter trace events by adaptive wiki entry id
    #[arg(long)]
    entry_id: Option<String>,

    /// Preview the trace without writing report files
    #[arg(long)]
    dry_run: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiRecurrenceArgs {
    /// Promoted adaptive wiki entry id to evaluate
    entry_id: String,

    /// Preview the report without writing report files
    #[arg(long)]
    dry_run: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiPromotionChainArgs {
    /// Promoted adaptive wiki entry id to reconstruct
    entry_id: String,

    /// Preview the report without writing report files
    #[arg(long)]
    dry_run: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiRecordCandidateArgs {
    /// Knowledge kind
    #[arg(long, value_parser = parse_adaptive_wiki_kind)]
    kind: AdaptiveWikiKind,

    /// Applicability scope
    #[arg(long, value_parser = parse_adaptive_wiki_scope)]
    scope: AdaptiveWikiScope,

    /// Scope reference (e.g. project key). Required unless scope is user_global.
    #[arg(long)]
    scope_ref: Option<String>,

    /// One-line durable claim
    #[arg(long)]
    claim: String,

    /// Compact instruction for the AI projection
    #[arg(long, default_value = "")]
    ai_instruction: String,

    /// Operator-facing governance summary
    #[arg(long, default_value = "")]
    human_summary: String,

    /// Evidence reference (repeatable), e.g. doc:/path/AGENTS.md#section
    #[arg(long = "evidence-ref")]
    evidence_refs: Vec<String>,

    /// Agent work mode this candidate applies to (repeatable; omit for universal)
    #[arg(long = "agent-mode", value_parser = parse_adaptive_wiki_agent_mode)]
    agent_modes: Vec<AdaptiveWikiAgentMode>,

    /// Controlled core tag (repeatable), e.g. domain/twinpaper or harness/dispatch
    #[arg(long = "core-tag")]
    core_tags: Vec<String>,

    /// Proposed (reviewable) tag (repeatable)
    #[arg(long = "proposed-tag")]
    proposed_tags: Vec<String>,

    /// Confidence level
    #[arg(long, default_value = "explicit", value_parser = parse_adaptive_wiki_confidence)]
    confidence: AdaptiveWikiConfidence,

    /// Provenance of this candidate: who observed it
    #[arg(long, default_value = "operator_explicit", value_parser = parse_adaptive_wiki_origin)]
    origin: AdaptiveWikiOrigin,

    /// What kind of signal produced this candidate. operator_correction also
    /// appends a first-class correction record for recurrence evaluation.
    #[arg(long, default_value = "imported_doc", value_parser = parse_adaptive_wiki_signal_kind)]
    signal_kind: AdaptiveWikiSignalKind,

    /// Why this is worth reviewing/promoting
    #[arg(long, default_value = "")]
    review_reason: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiPromoteArgs {
    /// Adaptive wiki candidate id
    candidate_id: String,

    /// Scope for the promoted entry. Defaults to the candidate scope.
    #[arg(long, value_parser = parse_adaptive_wiki_scope)]
    scope: Option<AdaptiveWikiScope>,

    /// Scope reference for the promoted entry. Required when --scope is used.
    #[arg(long)]
    scope_ref: Option<String>,

    /// Activation mode for the promoted entry
    #[arg(long, default_value = "confirm", value_parser = parse_adaptive_wiki_activation_mode)]
    activation_mode: AdaptiveWikiActivationMode,

    /// Agent work mode this promoted entry should apply to. Repeat for multiple modes; omit to keep candidate modes.
    #[arg(long = "agent-mode", value_parser = parse_adaptive_wiki_agent_mode)]
    agent_modes: Vec<AdaptiveWikiAgentMode>,

    /// Operator or surface performing the review
    #[arg(long, default_value = "cli")]
    by: String,

    /// Optional promotion reason for audit
    #[arg(long, default_value = "")]
    reason: String,

    /// Review window in days: entries must be re-reviewed after this horizon
    /// (skepticism-by-default; 0 disables)
    #[arg(long, default_value_t = 90)]
    review_after_days: i64,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiRejectArgs {
    /// Adaptive wiki candidate id
    candidate_id: String,

    /// Reason for rejecting the candidate
    #[arg(long)]
    reason: String,

    /// Operator or surface performing the review
    #[arg(long, default_value = "cli")]
    by: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiRescopeArgs {
    /// Adaptive wiki entry id
    entry_id: String,

    /// New entry scope
    #[arg(long, value_parser = parse_adaptive_wiki_scope)]
    scope: AdaptiveWikiScope,

    /// New entry scope reference
    #[arg(long)]
    scope_ref: String,

    /// Operator or surface performing the review
    #[arg(long, default_value = "cli")]
    by: String,

    /// Optional rescope reason for audit
    #[arg(long, default_value = "")]
    reason: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiEditArgs {
    /// Adaptive wiki entry id
    entry_id: String,

    /// Replace the knowledge kind
    #[arg(long, value_parser = parse_adaptive_wiki_kind)]
    kind: Option<AdaptiveWikiKind>,

    /// Replace the agent-mode scope (repeatable)
    #[arg(long = "agent-mode", value_parser = parse_adaptive_wiki_agent_mode)]
    agent_modes: Vec<AdaptiveWikiAgentMode>,

    /// Make the entry apply to every agent mode
    #[arg(long)]
    clear_agent_modes: bool,

    /// Replace the durable claim
    #[arg(long)]
    claim: Option<String>,

    /// Replace the compact AI instruction
    #[arg(long)]
    ai_instruction: Option<String>,

    /// Replace the operator-facing summary
    #[arg(long)]
    human_summary: Option<String>,

    /// Add an evidence reference (repeatable)
    #[arg(long = "evidence-ref")]
    evidence_refs: Vec<String>,

    /// Operator or surface performing the edit
    #[arg(long, default_value = "cli")]
    by: String,

    /// Optional edit reason for audit
    #[arg(long, default_value = "")]
    reason: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiAddTagArgs {
    /// Adaptive wiki entry id
    entry_id: String,

    /// Controlled core tag (repeatable), e.g. facet/research or domain/twinpaper
    #[arg(long = "core-tag")]
    core_tags: Vec<String>,

    /// Proposed (reviewable) tag (repeatable)
    #[arg(long = "proposed-tag")]
    proposed_tags: Vec<String>,

    /// Operator or surface performing the retag
    #[arg(long, default_value = "cli")]
    by: String,

    /// Optional retag reason for audit
    #[arg(long, default_value = "")]
    reason: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiDeprecateArgs {
    /// Adaptive wiki entry id
    entry_id: String,

    /// Reason for deprecating the entry
    #[arg(long)]
    reason: String,

    /// Operator or surface performing the review
    #[arg(long, default_value = "cli")]
    by: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiRenewReviewAfterArgs {
    /// Adaptive wiki entry id
    entry_id: String,

    /// New review_after timestamp in RFC3339 format
    #[arg(long, value_parser = parse_rfc3339_datetime)]
    review_after: DateTime<Utc>,

    /// Reason for renewing the review timestamp
    #[arg(long)]
    reason: String,

    /// Operator or surface performing the review
    #[arg(long, default_value = "cli")]
    by: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiCounterexampleArgs {
    /// Adaptive wiki entry id
    entry_id: String,

    /// Evidence ref that contradicts or limits the entry
    #[arg(long)]
    evidence_ref: String,

    /// Reason for recording the counterexample
    #[arg(long)]
    reason: String,

    /// Operator or surface performing the review
    #[arg(long, default_value = "cli")]
    by: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct WikiRunbookArgs {
    /// Adaptive wiki procedure entry id
    entry_id: String,

    /// Human/export support ref such as references/foo.md, templates/foo.md, or scripts/foo.sh
    #[arg(long)]
    support_ref: Vec<String>,

    /// Capability id this procedure is relevant to
    #[arg(long)]
    capability_id: Vec<String>,

    /// Required artifact kind this procedure depends on
    #[arg(long)]
    required_artifact_kind: Vec<String>,

    /// Reason for updating the runbook metadata
    #[arg(long)]
    reason: String,

    /// Operator or surface performing the review
    #[arg(long, default_value = "cli")]
    by: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Serialize)]
struct BackgroundProbeStatus {
    probe: BackgroundProbe,
    decision: BackgroundRecoveryDecision,
    #[serde(flatten)]
    mode_assessment: OffdeskModeAssessment,
}

#[derive(Serialize)]
struct BackgroundAckReport {
    ticket_id: String,
    linked_task_ids: Vec<String>,
    acknowledgement: BackgroundRecoveryAcknowledgement,
    status: BackgroundProbeStatus,
}

#[derive(Serialize)]
struct MutationSnapshotListItem {
    mutation_id: String,
    target_path: String,
    mutation_kind: String,
    created_at: DateTime<Utc>,
    rollback_available: bool,
    blockers: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
struct DebugBundleRedactionSummary {
    text_fields_checked: usize,
    changed_text_fields: usize,
    runner_context_removed: usize,
    secrets_redacted: usize,
}

#[derive(Default)]
struct DebugBundleRedactor {
    summary: DebugBundleRedactionSummary,
}

#[derive(Serialize)]
struct OffdeskDebugBundle {
    generated_at: DateTime<Utc>,
    profile: String,
    profile_dir: String,
    read_only: bool,
    redaction_applied: bool,
    approvals: Value,
    tasks: Value,
    resume_states: Value,
    background_runs: Value,
    capabilities: Value,
    provider_capacity: Value,
    adaptive_wiki: Value,
    adaptive_wiki_usage: Value,
    adaptive_wiki_corrections: Value,
    adaptive_wiki_review_events: Value,
    adaptive_wiki_runtime_policy_acknowledgements: Value,
    adaptive_wiki_runtime_policy_ack_attention_summary: WikiRuntimePolicyAckReportSummary,
    adaptive_wiki_review_after_attention_summary: WikiReviewAfterReportSummary,
    redaction_summary: DebugBundleRedactionSummary,
}

#[derive(Serialize)]
struct DebugBundleExportReceipt<'a> {
    exported_to: String,
    bytes_written: usize,
    bundle: &'a OffdeskDebugBundle,
}

struct DebugBundleExport {
    path: PathBuf,
    bytes_written: usize,
}

#[derive(Serialize)]
struct OffdeskMaintenanceReport {
    generated_at: DateTime<Utc>,
    profile: String,
    profile_dir: String,
    read_only: bool,
    tasks: MaintenanceTaskSummary,
    background_runs: MaintenanceBackgroundSummary,
    approvals: MaintenanceApprovalSummary,
    resume_states: MaintenanceResumeSummary,
    provider_capacity: MaintenanceProviderCapacitySummary,
    adaptive_wiki_runtime_policy_ack_attention_summary: WikiRuntimePolicyAckReportSummary,
    adaptive_wiki_review_after_attention_summary: WikiReviewAfterReportSummary,
    recommended_actions: Vec<MaintenanceRecommendedAction>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    next_safe_actions: Vec<OffdeskNextSafeAction>,
}

#[derive(Serialize)]
struct MaintenanceApprovalRequestReport {
    generated_at: DateTime<Utc>,
    action_kind: MaintenanceActionKind,
    action: String,
    project_key: String,
    request_id: String,
    task_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_id: Option<String>,
    risk_level: RiskLevel,
    status: String,
    detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    approval: Option<Value>,
    next_commands: Vec<String>,
}

#[derive(Default, Serialize)]
struct MaintenanceModeSummary {
    by_verdict: BTreeMap<String, usize>,
    by_risk: BTreeMap<String, usize>,
    review_stage_required: usize,
}

#[derive(Default, Serialize)]
struct MaintenanceTaskSummary {
    total: usize,
    by_status: BTreeMap<String, usize>,
    by_agent_mode: BTreeMap<String, usize>,
    missing_agent_mode: usize,
    mode: MaintenanceModeSummary,
}

#[derive(Default, Serialize)]
struct MaintenanceBackgroundSummary {
    total: usize,
    by_phase: BTreeMap<String, usize>,
    by_agent_mode: BTreeMap<String, usize>,
    missing_agent_mode: usize,
    mode: MaintenanceModeSummary,
}

#[derive(Default, Serialize)]
struct MaintenanceApprovalSummary {
    total: usize,
    by_status: BTreeMap<String, usize>,
    pending: usize,
}

#[derive(Default, Serialize)]
struct MaintenanceResumeSummary {
    total: usize,
    by_status: BTreeMap<String, usize>,
}

#[derive(Default, Serialize)]
struct MaintenanceProviderCapacitySummary {
    total: usize,
    by_status: BTreeMap<String, usize>,
    attention: usize,
}

#[derive(Serialize)]
struct MaintenanceRecommendedAction {
    kind: &'static str,
    detail: String,
    command: &'static str,
}

#[derive(Serialize)]
struct OffdeskCloseoutReport {
    generated_at: DateTime<Utc>,
    closeout_id: String,
    profile: String,
    profile_dir: String,
    artifact_dir: String,
    dry_run: bool,
    operator_requested_dry_run: bool,
    read_only_project_state: bool,
    filters: CloseoutFilters,
    summary: CloseoutSummary,
    source_observation: CloseoutSourceObservation,
    implementation_packet_coverage: CloseoutImplementationPacketCoverage,
    tasks: Vec<CloseoutTask>,
    background_runs: Vec<CloseoutBackgroundRun>,
    file_operations: Vec<CloseoutFileOperation>,
    required_first_reads: Vec<CloseoutReadRef>,
    decision_records: Vec<CloseoutDecisionRecord>,
    open_decisions: Vec<CloseoutDecision>,
    verification_commands: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    documentation_governance: Option<CloseoutDocumentationGovernance>,
    review_contract: CloseoutReviewContract,
    #[serde(skip_serializing_if = "Option::is_none")]
    git_snapshot: Option<CloseoutGitSnapshot>,
    artifacts: CloseoutArtifactPaths,
}

#[derive(Default, Serialize)]
struct CloseoutFilters {
    #[serde(skip_serializing_if = "Option::is_none")]
    project_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_id: Option<String>,
}

#[derive(Default, Serialize)]
struct CloseoutSummary {
    tasks_scanned: usize,
    background_runs_scanned: usize,
    completed_tasks: usize,
    active_or_blocked_tasks: usize,
    file_operations: usize,
    keep_operations: usize,
    archive_candidates: usize,
    delete_candidates: usize,
    operations_requiring_commercial_review: usize,
    operations_requiring_human_approval: usize,
    decision_records_scanned: usize,
    open_decision_records: usize,
    invalid_decision_records: usize,
    implementation_packets_scanned: usize,
    packet_goals_completed: usize,
    packet_goals_deferred: usize,
    packet_goals_missing: usize,
    packet_goals_drifted: usize,
    packet_detail_items: usize,
    packet_detail_items_completed: usize,
    packet_detail_items_deferred: usize,
    packet_detail_items_missing: usize,
    packet_detail_items_drifted: usize,
    missing_artifacts: usize,
    return_package_required: bool,
}

struct CloseoutPacketAggregate {
    summary: ImplementationPacketSummary,
    evidence_refs: BTreeSet<String>,
    match_refs: BTreeMap<String, String>,
    source_observation_status: &'static str,
    source_refs: Vec<String>,
    receipt_search_dirs: BTreeSet<String>,
    task_ids: BTreeSet<String>,
    background_ticket_ids: BTreeSet<String>,
    has_completed_evidence: bool,
    has_active_evidence: bool,
    has_failed_evidence: bool,
}

struct LoadedWorkSliceExecutionReceipt {
    receipt: WorkSliceExecutionReceipt,
    source: String,
}

struct CloseoutPacketDetailGroups {
    detail_source: &'static str,
    detail_error: Option<String>,
    work_slices: Vec<CloseoutPacketCoverageDetail>,
    validation_items: Vec<CloseoutPacketCoverageDetail>,
    expected_artifacts: Vec<CloseoutPacketCoverageDetail>,
}

#[derive(Serialize)]
struct CloseoutTask {
    task_id: String,
    request_id: String,
    project_key: String,
    status: OffdeskTaskStatus,
    capability_id: String,
    runner_kind: BackgroundRunnerKind,
    workdir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent_mode: Option<AdaptiveWikiAgentMode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    background_ticket_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_artifact_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    log_artifact_path: Option<String>,
    artifact_refs: Vec<CapabilityArtifactRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    implementation_packet: Option<crate::offdesk::ImplementationPacketSummary>,
    #[serde(skip)]
    receipt_search_dirs: Vec<String>,
    preview: String,
    reason: String,
}

#[derive(Serialize)]
struct CloseoutBackgroundRun {
    ticket_id: String,
    runner_kind: BackgroundRunnerKind,
    phase: BackgroundRunnerPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    working_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_artifact_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    log_artifact_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    implementation_packet: Option<crate::offdesk::ImplementationPacketSummary>,
    runtime_handle_alive: bool,
    result_artifact_present: bool,
    log_artifact_present: bool,
    #[serde(skip)]
    receipt_search_dirs: Vec<String>,
}

#[derive(Serialize)]
struct CloseoutFileOperation {
    operation: &'static str,
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    destination: Option<String>,
    source: String,
    risk: &'static str,
    reason: String,
    evidence_refs: Vec<String>,
    present: bool,
    requires_commercial_review: bool,
    requires_human_approval: bool,
}

#[derive(Serialize)]
struct CloseoutReadRef {
    path: String,
    reason: String,
    present: bool,
}

#[derive(Serialize)]
struct CloseoutDecisionRecord {
    source_path: String,
    record: DecisionRecord,
    validation_issues: Vec<DecisionValidationIssue>,
}

#[derive(Serialize)]
struct CloseoutDecision {
    kind: &'static str,
    detail: String,
    suggested_command: String,
}

#[derive(Serialize)]
struct CloseoutDocumentationGovernance {
    workdir: String,
    audit_profile: String,
    command: String,
    recommendation_count: usize,
    recommendations: Vec<CloseoutDocumentationRecommendation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct CloseoutDocumentationRecommendation {
    priority: String,
    kind: String,
    title: String,
    suggested_action: String,
    paths: Vec<String>,
}

#[derive(Serialize)]
struct CloseoutReviewContract {
    provider: String,
    required: bool,
    applies_to_operations: Vec<&'static str>,
    required_verdicts: Vec<&'static str>,
    decision_schema: Value,
    safety_rules: Vec<&'static str>,
    packet_path: String,
}

#[derive(Serialize)]
struct CloseoutGitSnapshot {
    workdir: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_short: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    diff_stat: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Serialize)]
struct CloseoutSourceObservation {
    schema: &'static str,
    generated_at: DateTime<Utc>,
    source_kind: &'static str,
    enabled: bool,
    available: bool,
    status: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    workdir: Option<String>,
    base_ref: &'static str,
    changed_file_count: usize,
    changed_files_truncated: bool,
    changed_files: Vec<CloseoutSourceChangedFile>,
    artifact_refs: Vec<String>,
    warnings: Vec<String>,
}

#[derive(Serialize)]
struct CloseoutSourceChangedFile {
    path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    old_path: Option<String>,
    status: &'static str,
    additions: usize,
    deletions: usize,
}

#[derive(Serialize)]
struct CloseoutArtifactPaths {
    closeout_plan_json: String,
    closeout_plan_markdown: String,
    cleanup_manifest_json: String,
    commercial_review_packet: String,
    return_package_markdown: String,
}

const CLOSEOUT_SOURCE_OBSERVATION_BASE_REF: &str = "HEAD";
const CLOSEOUT_SOURCE_OBSERVATION_CHANGED_FILE_LIMIT: usize = 100;
const CLOSEOUT_SOURCE_OBSERVATION_REF_LIMIT: usize = 5;

pub async fn run(profile: &str, command: OffdeskCommands) -> Result<()> {
    match command {
        OffdeskCommands::Harnesses(args) => harnesses(args).await,
        OffdeskCommands::HarnessPrompt(args) => harness_prompt(args).await,
        OffdeskCommands::Plan(args) => plan(profile, args).await,
        OffdeskCommands::Plans(args) => plans(profile, args).await,
        OffdeskCommands::PlanShow(args) => plan_show(profile, args).await,
        OffdeskCommands::PlanReview(args) => plan_review(profile, args).await,
        OffdeskCommands::PlanLaunchPrep(args) => plan_launch_prep(profile, args).await,
        OffdeskCommands::RemoteOperator { command } => remote_operator(profile, command).await,
        OffdeskCommands::Pending(args) => pending(profile, args).await,
        OffdeskCommands::Gate(args) => gate(profile, args).await,
        OffdeskCommands::Launch(args) => launch(profile, args).await,
        OffdeskCommands::Enqueue(args) => enqueue(profile, args).await,
        OffdeskCommands::Tick(args) => tick(profile, args).await,
        OffdeskCommands::Tasks(args) => tasks(profile, args).await,
        OffdeskCommands::Decisions(args) => decisions(profile, args).await,
        OffdeskCommands::Decision(args) => decision(profile, args).await,
        OffdeskCommands::ProviderCapacity(args) => provider_capacity(profile, args).await,
        OffdeskCommands::ProviderFallback(args) => provider_fallback(profile, args).await,
        OffdeskCommands::CancelTask(args) => cancel_task(profile, args).await,
        OffdeskCommands::Pause(args) => pause_dispatch(profile, args).await,
        OffdeskCommands::Unpause(args) => unpause_dispatch(profile, args).await,
        OffdeskCommands::PauseStatus(args) => pause_status(profile, args).await,
        OffdeskCommands::LearningScan(args) => learning_scan(profile, args).await,
        OffdeskCommands::RetryTask(args) => retry_task(profile, args).await,
        OffdeskCommands::ResumeTask(args) => resume_task(profile, args).await,
        OffdeskCommands::AbandonTask(args) => abandon_task(profile, args).await,
        OffdeskCommands::Poll(args) => poll(profile, args).await,
        OffdeskCommands::Ok(args) => resolve(profile, args, true).await,
        OffdeskCommands::Cancel(args) => resolve(profile, args, false).await,
        OffdeskCommands::Resume(args) => resume(profile, args).await,
        OffdeskCommands::Background(args) => background(profile, args).await,
        OffdeskCommands::BackgroundAck(args) => background_ack(profile, args).await,
        OffdeskCommands::Capabilities(args) => capabilities(args).await,
        OffdeskCommands::Snapshots(args) => snapshots(profile, args).await,
        OffdeskCommands::Snapshot(args) => snapshot(profile, args).await,
        OffdeskCommands::RestorePlan(args) => restore_plan(profile, args).await,
        OffdeskCommands::DebugBundle(args) => debug_bundle(profile, args).await,
        OffdeskCommands::MaintenanceReport(args) => maintenance_report(profile, args).await,
        OffdeskCommands::MaintenanceRequest(args) => maintenance_request(profile, args).await,
        OffdeskCommands::Deck(args) => run_deck(args),
        OffdeskCommands::Closeout(args) => closeout(profile, args).await,
        OffdeskCommands::CloseoutReview(args) => closeout_review(profile, args).await,
        OffdeskCommands::CloseoutDecision(args) => closeout_decision(profile, args).await,
        OffdeskCommands::CloseoutRetire(args) => closeout_retire(profile, args).await,
        OffdeskCommands::Wiki(args) => wiki(profile, args).await,
    }
}

async fn enqueue(profile: &str, args: EnqueueArgs) -> Result<()> {
    let now = Utc::now();
    let brief = load_execution_brief(args.brief.as_ref())?;
    let profile_dir = get_profile_dir(profile)?;
    let implementation_packet = resolve_implementation_packet_context(
        &profile_dir,
        &args.project_key,
        args.implementation_packet.as_deref(),
    )?;
    let mut artifact_refs = args.artifact_refs;
    attach_implementation_packet_artifact_refs(&mut artifact_refs, implementation_packet.as_ref());
    let task = OffdeskTask::new(
        OffdeskTaskInput {
            task_id: args.task_id,
            request_id: args.request_id,
            project_key: args.project_key,
            capability_id: args.capability_id,
            runner_kind: args.runner,
            command: args.command,
            workdir: args
                .workdir
                .unwrap_or(std::env::current_dir()?)
                .to_string_lossy()
                .into_owned(),
            execution_brief: brief,
            not_before: parse_rfc3339(args.not_before.as_deref())?,
            mutation_class: args.mutation_class,
            artifact_refs,
            implementation_packet: implementation_packet
                .as_ref()
                .map(|packet| packet.summary.clone()),
            artifact_kind: args.artifact_kind,
            agent_mode: args.agent_mode,
            provider_id: args.provider_id,
            model: args.model,
            preview: args.preview,
            reason: args.reason,
            log_artifact_path: args
                .log_artifact
                .map(|path| path.to_string_lossy().into_owned()),
            result_artifact_path: args
                .result_artifact
                .map(|path| path.to_string_lossy().into_owned()),
        },
        now,
    );

    task_store(profile)?.enqueue(task.clone())?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&task.operator_view())?);
        return Ok(());
    }

    println!("Enqueued offdesk task {}", task.task_id);
    println!("  capability: {}", task.capability_id);
    println!("  runner:     {:?}", task.runner_kind);
    if let Some(packet) = task.implementation_packet.as_ref() {
        println!("  packet:     {} ({})", packet.packet_id, packet.outcome);
    }
    Ok(())
}

async fn tick(profile: &str, args: TickArgs) -> Result<()> {
    let mut options = OffdeskTickOptions::new(Utc::now());
    options.limit = args.limit.max(1);
    options.project_key = args.project_key;
    options.task_id = args.task_id;
    options.lock_stale_after = Duration::minutes(args.lock_stale_minutes.max(1));
    options.notification_cooldown = args
        .notify_cooldown_minutes
        .map(|minutes| Duration::minutes(minutes.max(1)));
    let report = run_offdesk_tick(get_profile_dir(profile)?, options)?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    println!(
        "Tick: {} launched, {} pending approval, {} completed, {} resume pending, {} failed",
        report.launched,
        report.pending_approval,
        report.completed,
        report.resume_pending,
        report.failed
    );
    if report.provider_deferred > 0 {
        println!("  provider deferred: {}", report.provider_deferred);
    }
    if report.provider_retargeted > 0 {
        println!("  provider retargeted: {}", report.provider_retargeted);
    }
    if report.skipped > 0 {
        println!("  skipped by limit: {}", report.skipped);
    }
    print_next_safe_actions(&report.next_safe_actions);
    Ok(())
}

async fn tasks(profile: &str, args: TasksArgs) -> Result<()> {
    let mut task_views: Vec<OffdeskTaskView> = task_store(profile)?
        .load()?
        .into_iter()
        .filter(|task| task_matches_tasks_filter(task, &args))
        .map(|task| task.operator_view())
        .collect();
    if args.latest {
        task_views.sort_by_key(|task| task.updated_at);
        if let Some(latest) = task_views.pop() {
            task_views = vec![latest];
        }
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&task_views)?);
        return Ok(());
    }

    if task_views.is_empty() {
        println!("No offdesk tasks found.");
        return Ok(());
    }

    print_tasks(&task_views);
    Ok(())
}

fn task_matches_tasks_filter(task: &OffdeskTask, args: &TasksArgs) -> bool {
    if let Some(project_key) = args.project_key.as_deref() {
        if task.project_key != project_key {
            return false;
        }
    }
    if let Some(task_id) = args.task_id.as_deref() {
        if task.task_id != task_id {
            return false;
        }
    }
    if !args.status.is_empty() && !args.status.contains(&task.status) {
        return false;
    }
    true
}

async fn decisions(profile: &str, args: DecisionsArgs) -> Result<()> {
    let mut records = DecisionLedger::new(read_only_profile_dir(profile)?).load()?;
    records.retain(|record| decision_matches_filter(record, &args));
    records.sort_by_key(|record| record.updated_at);

    if args.json {
        let views: Vec<DecisionRecordView> =
            records.into_iter().map(DecisionRecordView::from).collect();
        println!("{}", serde_json::to_string_pretty(&views)?);
        return Ok(());
    }

    if records.is_empty() {
        println!("No offdesk decisions found.");
        return Ok(());
    }

    print_decisions(&records);
    Ok(())
}

fn decision_matches_filter(record: &DecisionRecord, args: &DecisionsArgs) -> bool {
    if let Some(project_key) = args.project_key.as_deref() {
        if record.project_key != project_key {
            return false;
        }
    }
    if let Some(task_id) = args.task_id.as_deref() {
        if record.task_id != task_id {
            return false;
        }
    }
    if !args.status.is_empty()
        && !args
            .status
            .iter()
            .any(|status| status == record.status.as_str())
    {
        return false;
    }
    true
}

async fn decision(profile: &str, args: DecisionArgs) -> Result<()> {
    match args.command {
        DecisionCommands::Show(args) => decision_show(profile, args).await,
        DecisionCommands::Resolve(args) => decision_resolve(profile, args).await,
        DecisionCommands::Receipt(args) => decision_receipt(profile, args).await,
        DecisionCommands::IngestTelegram(args) => ingest_telegram_decision(profile, args),
        DecisionCommands::IngestTelegramFeedback(args) => ingest_telegram_feedback(profile, args),
    }
}

async fn decision_show(profile: &str, args: DecisionShowArgs) -> Result<()> {
    let Some(record) =
        DecisionLedger::new(read_only_profile_dir(profile)?).find(&args.decision_id)?
    else {
        bail!("decision not found: {}", args.decision_id);
    };

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&DecisionRecordView::from(record))?
        );
        return Ok(());
    }

    print_decision(&record);
    Ok(())
}

async fn decision_resolve(profile: &str, args: DecisionResolveArgs) -> Result<()> {
    let ledger = DecisionLedger::new(get_profile_dir(profile)?);
    let Some(record) = ledger.find(&args.decision_id)? else {
        bail!("decision not found: {}", args.decision_id);
    };
    let updated = resolve_decision_record(record, &args)?;
    ledger.append(&updated)?;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&DecisionRecordView::from(updated))?
        );
        return Ok(());
    }

    print_decision(&updated);
    Ok(())
}

async fn decision_receipt(profile: &str, args: DecisionReceiptArgs) -> Result<()> {
    let ledger = DecisionLedger::new(get_profile_dir(profile)?);
    let Some(record) = ledger.find(&args.decision_id)? else {
        bail!("decision not found: {}", args.decision_id);
    };
    let updated = receipt_decision_record(record, &args)?;
    ledger.append(&updated)?;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&DecisionRecordView::from(updated))?
        );
        return Ok(());
    }

    print_decision(&updated);
    Ok(())
}

fn resolve_decision_record(
    record: DecisionRecord,
    args: &DecisionResolveArgs,
) -> Result<DecisionRecord> {
    transition_resolve_decision_record(
        record,
        &DecisionResolutionInput {
            decision: args.decision.clone(),
            note: args.note.clone(),
            by: args.by.clone(),
            target: args.target.clone(),
        },
    )
}

fn receipt_decision_record(
    record: DecisionRecord,
    args: &DecisionReceiptArgs,
) -> Result<DecisionRecord> {
    transition_receipt_decision_record(
        record,
        &DecisionReceiptInput {
            by: args.by.clone(),
            result_status: args.result_status.clone(),
            evidence_summary: args.evidence_summary.clone(),
            remaining_review: args.remaining_review.clone(),
        },
    )
}

async fn provider_capacity(profile: &str, args: JsonArgs) -> Result<()> {
    let states = ProviderCapacityStore::new(read_only_profile_dir(profile)?).load()?;

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&states)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    if states.is_empty() {
        println!("No provider capacity state found.");
        return Ok(());
    }

    print_provider_capacity(&states);
    Ok(())
}

async fn provider_fallback(profile: &str, args: ProviderFallbackArgs) -> Result<()> {
    let profile_dir = read_only_profile_dir(profile)?;
    let recommendation = recommend_provider_fallback(
        &ProviderCapacityStore::new(profile_dir),
        &args.provider_id,
        args.model.as_deref(),
        "operator requested provider fallback recommendation",
        &args.runner_role,
        Utc::now(),
    )?;

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&recommendation)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    print_provider_fallback(&recommendation);
    Ok(())
}

async fn wiki(profile: &str, args: WikiArgs) -> Result<()> {
    match args.command {
        WikiCommands::Corrections(args) => wiki_corrections(profile, args).await,
        WikiCommands::ProposalEvents(args) => wiki_proposal_events(profile, args).await,
        WikiCommands::RecordProposalEvent(args) => wiki_record_proposal_event(profile, args).await,
        WikiCommands::AcceptProposal(args) => {
            wiki_close_proposal(profile, args, AdaptiveWikiReviewProposalDecision::Accepted).await
        }
        WikiCommands::RejectProposal(args) => {
            wiki_close_proposal(profile, args, AdaptiveWikiReviewProposalDecision::Rejected).await
        }
        WikiCommands::SupersedeProposal(args) => {
            wiki_close_proposal(
                profile,
                args,
                AdaptiveWikiReviewProposalDecision::Superseded,
            )
            .await
        }
        WikiCommands::ProposalHandoff(args) => wiki_proposal_handoff(profile, args).await,
        WikiCommands::ProposalReceipt(args) => wiki_proposal_receipt(profile, args).await,
        WikiCommands::Candidates(args) => wiki_candidates(profile, args).await,
        WikiCommands::Entries(args) => wiki_entries(profile, args).await,
        WikiCommands::Show(args) => wiki_show(profile, args).await,
        WikiCommands::Projection(args) => wiki_projection(profile, args).await,
        WikiCommands::Brief(args) => wiki_brief(profile, args).await,
        WikiCommands::RuntimePolicyAcks(args) => wiki_runtime_policy_acks(profile, args).await,
        WikiCommands::RuntimePolicyAckReport(args) => {
            wiki_runtime_policy_ack_report(profile, args).await
        }
        WikiCommands::ReviewAfterReport(args) => wiki_review_after_report(profile, args).await,
        WikiCommands::AckRuntimePolicy(args) => wiki_ack_runtime_policy(profile, args).await,
        WikiCommands::Lint(args) => wiki_lint(profile, args).await,
        WikiCommands::ExportMarkdown(args) => wiki_export_markdown(profile, args).await,
        WikiCommands::Graph(args) => wiki_graph(profile, args).await,
        WikiCommands::Review(args) => wiki_review(profile, args).await,
        WikiCommands::EvaluateEpisode(args) => wiki_evaluate_episode(profile, args).await,
        WikiCommands::EpisodeTrace(args) => wiki_episode_trace(profile, args).await,
        WikiCommands::EvaluateRecurrence(args) => wiki_evaluate_recurrence(profile, args).await,
        WikiCommands::PromotionChain(args) => wiki_promotion_chain(profile, args).await,
        WikiCommands::RecordCandidate(args) => wiki_record_candidate(profile, args).await,
        WikiCommands::Promote(args) => wiki_promote(profile, args).await,
        WikiCommands::Reject(args) => wiki_reject(profile, args).await,
        WikiCommands::Rescope(args) => wiki_rescope(profile, args).await,
        WikiCommands::Edit(args) => wiki_edit(profile, args).await,
        WikiCommands::AddTag(args) => wiki_add_tag(profile, args).await,
        WikiCommands::Deprecate(args) => wiki_deprecate(profile, args).await,
        WikiCommands::RenewReviewAfter(args) => wiki_renew_review_after(profile, args).await,
        WikiCommands::AddCounterexample(args) => wiki_add_counterexample(profile, args).await,
        WikiCommands::UpdateRunbook(args) => wiki_update_runbook(profile, args).await,
    }
}

async fn wiki_proposal_events(profile: &str, args: WikiProposalEventsArgs) -> Result<()> {
    let mut events = wiki_store(profile)?.load_review_proposal_events()?;
    if let Some(proposal_id) = args.proposal_id.as_deref() {
        events.retain(|event| event.proposal_id == proposal_id);
    }

    present_wiki_proposal_events(&events, args.json)
}

async fn wiki_record_proposal_event(
    profile: &str,
    args: WikiRecordProposalEventArgs,
) -> Result<()> {
    require_non_empty_arg("proposal_id", &args.proposal_id)?;
    require_non_empty_arg("--reason", &args.reason)?;
    let evidence_refs = args
        .evidence_refs
        .iter()
        .map(|value| crate::offdesk::operator_safe_text(value.trim()))
        .filter(|value| !value.is_empty())
        .collect();
    let event = AdaptiveWikiReviewProposalEventRecord {
        id: format!("wiki_review_event_{}", Uuid::new_v4()),
        proposal_id: crate::offdesk::operator_safe_text(args.proposal_id.trim()),
        decision: args.decision,
        proposal_action: args.proposal_action,
        subject_kind: crate::offdesk::operator_safe_text(args.subject_kind.trim()),
        subject_id: crate::offdesk::operator_safe_text(args.subject_id.trim()),
        actor: crate::offdesk::operator_safe_text(args.by.trim()),
        reason: crate::offdesk::operator_safe_text(args.reason.trim()),
        evidence_refs,
        supersedes: args
            .supersedes
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(crate::offdesk::operator_safe_text),
        created_at: Utc::now(),
    };
    writable_wiki_store(profile)?.append_review_proposal_event(&event)?;

    present_wiki_proposal_event(&event, args.json)
}

async fn wiki_close_proposal(
    profile: &str,
    args: WikiCloseProposalArgs,
    decision: AdaptiveWikiReviewProposalDecision,
) -> Result<()> {
    require_non_empty_arg("proposal_id", &args.proposal_id)?;
    require_non_empty_arg("--reason", &args.reason)?;
    let now = Utc::now();
    let store = writable_wiki_store(profile)?;
    let report =
        store.generate_review_report_filtered(true, now, AdaptiveWikiReviewQueueFilter::All)?;
    let proposal = report
        .proposals
        .iter()
        .find(|proposal| proposal.id == args.proposal_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Adaptive wiki review proposal not found: {}",
                args.proposal_id
            )
        })?;
    if !args.allow_decided && proposal_has_non_stale_decision(proposal) {
        bail!(
            "proposal {} already has a non-stale lifecycle decision; pass --allow-decided to record another event",
            proposal.id
        );
    }

    let event = AdaptiveWikiReviewProposalEventRecord {
        id: format!("wiki_review_event_{}", Uuid::new_v4()),
        proposal_id: crate::offdesk::operator_safe_text(&proposal.id),
        decision,
        proposal_action: Some(proposal.action),
        subject_kind: crate::offdesk::operator_safe_text(&proposal.subject_kind),
        subject_id: crate::offdesk::operator_safe_text(&proposal.subject_id),
        actor: crate::offdesk::operator_safe_text(args.by.trim()),
        reason: crate::offdesk::operator_safe_text(args.reason.trim()),
        evidence_refs: proposal_decision_evidence_refs(proposal, &args.evidence_refs),
        supersedes: args
            .supersedes
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(crate::offdesk::operator_safe_text),
        created_at: now,
    };
    store.append_review_proposal_event(&event)?;

    present_wiki_proposal_event(&event, args.json)
}

fn proposal_has_non_stale_decision(proposal: &AdaptiveWikiReviewProposal) -> bool {
    proposal.lifecycle.as_ref().is_some_and(|lifecycle| {
        !lifecycle.stale && lifecycle.decision != AdaptiveWikiReviewProposalDecision::Unknown
    })
}

fn proposal_decision_evidence_refs(
    proposal: &AdaptiveWikiReviewProposal,
    extra_refs: &[String],
) -> Vec<String> {
    let mut refs = Vec::new();
    for value in proposal.evidence_refs.iter().chain(extra_refs.iter()) {
        let safe = crate::offdesk::operator_safe_text(value.trim());
        if !safe.is_empty() && !refs.contains(&safe) {
            refs.push(safe);
        }
    }
    refs
}

async fn wiki_corrections(profile: &str, args: JsonArgs) -> Result<()> {
    let corrections = wiki_store(profile)?.load_correction_records()?;

    present_wiki_corrections(&corrections, args.json)
}

impl WikiBriefArgs {
    /// Brief scoped to one project plane, written to a file. Used by
    /// `forager go` to refresh `.wiki-brief.md` before launching an agent.
    pub(crate) fn scoped_to_file(project_key: String, out: std::path::PathBuf) -> Self {
        Self {
            project_key: Some(project_key),
            artifact_kind: None,
            agent_mode: None,
            max_entries: 12,
            out: Some(out),
            json: false,
        }
    }
}

pub(crate) async fn wiki_brief(profile: &str, args: WikiBriefArgs) -> Result<()> {
    let store = wiki_store(profile)?;
    let query = wiki_query(
        &None,
        &args.project_key,
        &args.artifact_kind,
        args.agent_mode,
    );
    let budget = AdaptiveWikiProjectionBudget {
        max_entries: args.max_entries,
        ..Default::default()
    };
    let report = store.ai_projection_report(&query, budget)?;
    let entries_state = store.load_entries()?;
    let now = Utc::now();
    let scope_label = args
        .project_key
        .clone()
        .unwrap_or_else(|| "user_global".to_string());
    let brief =
        build_wiki_brief_read_model(profile, &scope_label, now, &report, &entries_state.entries);

    if args.json {
        return present_wiki_brief(&brief, true);
    }
    if let Some(out) = &args.out {
        let body = render_wiki_brief_markdown(&brief);
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(out, format!("{body}\n"))?;
        present_wiki_brief_write_confirmation(&brief, out);
    } else {
        present_wiki_brief(&brief, false)?;
    }
    Ok(())
}

async fn wiki_projection(profile: &str, args: WikiProjectionArgs) -> Result<()> {
    let mut query = wiki_query(
        &args.session_id,
        &args.project_key,
        &args.artifact_kind,
        args.agent_mode,
    );
    if args.runtime_agent_mode_default {
        query.agent_mode_filter = AdaptiveWikiAgentModeFilter::SharedWhenUnspecified;
    }
    let budget = wiki_projection_budget(&args);
    if args.compare_review_expired_policy {
        if args.exclude_review_expired {
            bail!(
                "--compare-review-expired-policy already compares warn and exclude policies; omit --exclude-review-expired"
            );
        }
        let comparison =
            wiki_store(profile)?.ai_projection_review_expired_policy_comparison(&query, budget)?;
        return present_wiki_projection_comparison(&comparison, args.json);
    }
    let policy = wiki_projection_policy(&args);
    let report = wiki_store(profile)?.ai_projection_report_with_policy(&query, budget, policy)?;

    present_wiki_projection(&report, args.report, args.json)
}

fn wiki_projection_budget(args: &WikiProjectionArgs) -> AdaptiveWikiProjectionBudget {
    let mut budget = AdaptiveWikiProjectionBudget::default();
    if let Some(max_entries) = args.max_entries {
        budget.max_entries = max_entries;
    }
    if let Some(max_context_chars) = args.max_context_chars {
        budget.max_context_chars = max_context_chars;
    }
    if let Some(max_instruction_chars) = args.max_instruction_chars {
        budget.max_instruction_chars = max_instruction_chars;
    }
    budget
}

fn wiki_projection_policy(args: &WikiProjectionArgs) -> AdaptiveWikiProjectionPolicy {
    AdaptiveWikiProjectionPolicy {
        review_expired: if args.exclude_review_expired {
            AdaptiveWikiProjectionReviewExpiredPolicy::Exclude
        } else {
            AdaptiveWikiProjectionReviewExpiredPolicy::Warn
        },
    }
}

async fn wiki_runtime_policy_acks(profile: &str, args: JsonArgs) -> Result<()> {
    let acknowledgements = wiki_store(profile)?.load_runtime_policy_acknowledgements()?;
    present_runtime_policy_acknowledgements(&acknowledgements, args.json)
}

async fn wiki_runtime_policy_ack_report(
    profile: &str,
    args: WikiRuntimePolicyAckReportArgs,
) -> Result<()> {
    let now = Utc::now();
    let near_expiry_hours = args.near_expiry_hours.max(1);
    let store = wiki_store(profile)?;
    let acknowledgements = store.load_runtime_policy_acknowledgements()?;
    let query = if args.session_id.is_some()
        || args.project_key.is_some()
        || args.artifact_kind.is_some()
        || args.agent_mode.is_some()
    {
        Some(runtime_wiki_query(
            &args.session_id,
            &args.project_key,
            &args.artifact_kind,
            args.agent_mode,
        ))
    } else {
        None
    };
    let budget = query
        .as_ref()
        .map(|_| wiki_runtime_policy_ack_report_budget(&args));
    let decision = if let (Some(query), Some(budget)) = (query.as_ref(), budget.clone()) {
        Some(
            store
                .runtime_projection_with_policy_acknowledgement(
                    query,
                    budget,
                    strict_runtime_review_expired_policy(),
                    now,
                )?
                .decision,
        )
    } else {
        None
    };
    let report = build_runtime_policy_ack_report(
        acknowledgements,
        query,
        budget,
        decision,
        near_expiry_hours,
        now,
    );
    present_runtime_policy_ack_report(&report, args.json)
}

async fn wiki_review_after_report(profile: &str, args: WikiReviewAfterReportArgs) -> Result<()> {
    let now = Utc::now();
    let near_expiry_hours = args.near_expiry_hours.max(1);
    let query = wiki_query(
        &args.session_id,
        &args.project_key,
        &args.artifact_kind,
        args.agent_mode,
    );
    let projection = wiki_store(profile)?.human_projection(&query)?;
    let report = build_review_after_report(projection.entries, query, near_expiry_hours, now);

    present_review_after_report(&report, args.json)
}

async fn wiki_ack_runtime_policy(profile: &str, args: WikiRuntimePolicyAckArgs) -> Result<()> {
    if args.session_id.is_none()
        && args.project_key.is_none()
        && args.artifact_kind.is_none()
        && args.agent_mode.is_none()
    {
        bail!(
            "strict runtime policy acknowledgement requires at least one scope: --session-id, --project-key, --artifact-kind, or --agent-mode"
        );
    }
    let query = match args.scope_mode {
        AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery => runtime_wiki_query(
            &args.session_id,
            &args.project_key,
            &args.artifact_kind,
            args.agent_mode,
        ),
        AdaptiveWikiRuntimePolicyAckScopeMode::ProjectArtifact => {
            if args.session_id.is_some() {
                bail!("--scope-mode project-artifact must omit --session-id");
            }
            if args.project_key.is_none() || args.artifact_kind.is_none() {
                bail!("--scope-mode project-artifact requires --project-key and --artifact-kind");
            }
            runtime_wiki_query(
                &None,
                &args.project_key,
                &args.artifact_kind,
                args.agent_mode,
            )
        }
    };
    let budget = wiki_runtime_policy_ack_budget(&args);
    let acknowledgement = wiki_store(profile)?.acknowledge_runtime_strict_review_expired_policy(
        &query,
        budget,
        args.scope_mode,
        Duration::hours(args.ttl_hours.max(1)),
        &args.reason,
        Utc::now(),
    )?;

    present_runtime_policy_acknowledgement(&acknowledgement, args.json)
}

fn wiki_runtime_policy_ack_budget(args: &WikiRuntimePolicyAckArgs) -> AdaptiveWikiProjectionBudget {
    let mut budget = AdaptiveWikiProjectionBudget::default();
    if let Some(max_entries) = args.max_entries {
        budget.max_entries = max_entries;
    }
    if let Some(max_context_chars) = args.max_context_chars {
        budget.max_context_chars = max_context_chars;
    }
    if let Some(max_instruction_chars) = args.max_instruction_chars {
        budget.max_instruction_chars = max_instruction_chars;
    }
    budget
}

fn wiki_runtime_policy_ack_report_budget(
    args: &WikiRuntimePolicyAckReportArgs,
) -> AdaptiveWikiProjectionBudget {
    let mut budget = AdaptiveWikiProjectionBudget::default();
    if let Some(max_entries) = args.max_entries {
        budget.max_entries = max_entries;
    }
    if let Some(max_context_chars) = args.max_context_chars {
        budget.max_context_chars = max_context_chars;
    }
    if let Some(max_instruction_chars) = args.max_instruction_chars {
        budget.max_instruction_chars = max_instruction_chars;
    }
    budget
}

fn strict_runtime_review_expired_policy() -> AdaptiveWikiProjectionPolicy {
    AdaptiveWikiProjectionPolicy {
        review_expired: AdaptiveWikiProjectionReviewExpiredPolicy::Exclude,
    }
}

fn adaptive_wiki_agent_mode_cli_value(mode: AdaptiveWikiAgentMode) -> &'static str {
    match mode {
        AdaptiveWikiAgentMode::Planning => "planning",
        AdaptiveWikiAgentMode::Development => "development",
        AdaptiveWikiAgentMode::Analysis => "analysis",
        AdaptiveWikiAgentMode::Writing => "writing",
        AdaptiveWikiAgentMode::Critique => "critique",
        AdaptiveWikiAgentMode::Review => "review",
        AdaptiveWikiAgentMode::Maintenance => "maintenance",
    }
}

fn adaptive_wiki_agent_modes_label(modes: &[AdaptiveWikiAgentMode]) -> String {
    if modes.is_empty() {
        return "all".to_string();
    }
    modes
        .iter()
        .map(|mode| adaptive_wiki_agent_mode_cli_value(*mode))
        .collect::<Vec<_>>()
        .join(",")
}

async fn wiki_lint(profile: &str, args: JsonArgs) -> Result<()> {
    let report = wiki_store(profile)?.lint(Utc::now())?;
    present_wiki_lint(&report, args.json)
}

async fn wiki_export_markdown(profile: &str, args: WikiExportMarkdownArgs) -> Result<()> {
    let store = wiki_store(profile)?;
    let output = args
        .output
        .unwrap_or_else(|| store.default_markdown_vault_dir());
    let report = store.export_markdown(&output, args.dry_run, Utc::now())?;
    present_wiki_markdown_export(&report, args.json)
}

async fn wiki_graph(profile: &str, args: WikiGraphArgs) -> Result<()> {
    let report = wiki_store(profile)?.graph_report(Utc::now())?;
    let files = if args.output.is_some() {
        build_graph_export_files(&report)?
    } else {
        Vec::new()
    };
    if let Some(output) = args.output.as_ref() {
        if !args.dry_run {
            write_wiki_graph_export(output, &files)?;
        }
    }

    present_wiki_graph(
        &report,
        args.output.as_deref(),
        args.dry_run,
        files.len(),
        args.json,
    )
}

fn write_wiki_graph_export(output: &Path, files: &[(String, String)]) -> Result<()> {
    fs::create_dir_all(output)
        .with_context(|| format!("create adaptive wiki graph export {}", output.display()))?;
    for (relative_path, content) in files {
        let path = output.join(relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "create adaptive wiki graph export directory {}",
                    parent.display()
                )
            })?;
        }
        fs::write(&path, content)
            .with_context(|| format!("write adaptive wiki graph export {}", path.display()))?;
    }
    Ok(())
}

async fn wiki_review(profile: &str, args: WikiReviewArgs) -> Result<()> {
    let store = if args.dry_run {
        wiki_store(profile)?
    } else {
        writable_wiki_store(profile)?
    };
    let queue_filter = wiki_review_queue_filter(&args)?;
    let report = store.generate_review_report_filtered(args.dry_run, Utc::now(), queue_filter)?;
    present_wiki_review_report(&report, args.json)
}

fn wiki_review_queue_filter(args: &WikiReviewArgs) -> Result<AdaptiveWikiReviewQueueFilter> {
    let selected = args.active_only as u8 + args.decided_only as u8 + args.stale_only as u8;
    if selected > 1 {
        bail!("choose only one of --active-only, --decided-only, or --stale-only");
    }
    if args.active_only {
        Ok(AdaptiveWikiReviewQueueFilter::Active)
    } else if args.decided_only {
        Ok(AdaptiveWikiReviewQueueFilter::Decided)
    } else if args.stale_only {
        Ok(AdaptiveWikiReviewQueueFilter::Stale)
    } else {
        Ok(AdaptiveWikiReviewQueueFilter::All)
    }
}

async fn wiki_evaluate_episode(profile: &str, args: WikiEpisodeArgs) -> Result<()> {
    let in_scope_query = wiki_query(
        &args.session_id,
        &args.project_key,
        &args.artifact_kind,
        args.agent_mode,
    );
    let out_of_scope_query = wiki_episode_out_of_scope_query(&args);
    let store = if args.dry_run {
        wiki_store(profile)?
    } else {
        writable_wiki_store(profile)?
    };
    let report = store.generate_episode_evaluation_report(
        &args.entry_id,
        in_scope_query,
        out_of_scope_query,
        args.dry_run,
        Utc::now(),
    )?;

    present_wiki_episode_evaluation_report(&report, args.json)
}

async fn wiki_episode_trace(profile: &str, args: WikiEpisodeTraceArgs) -> Result<()> {
    let profile_dir = if args.dry_run {
        read_only_profile_dir(profile)?
    } else {
        get_profile_dir(profile)?
    };
    let filter = AdaptiveWikiLiveEpisodeFilter {
        request_id: clean_optional_string(&args.request_id),
        task_id: clean_optional_string(&args.task_id),
        project_key: clean_optional_string(&args.project_key),
        artifact_kind: clean_optional_string(&args.artifact_kind),
        entry_id: clean_optional_string(&args.entry_id),
    };
    let store = AdaptiveWikiStore::new(&profile_dir);
    let report = store.generate_live_episode_trace_report(
        &OffdeskTaskStore::new(&profile_dir).load()?,
        &BackgroundRunStore::new(&profile_dir).load()?,
        &TaskResumeStore::new(&profile_dir).load()?,
        filter,
        args.dry_run,
        Utc::now(),
    )?;

    present_wiki_live_episode_trace_report(&report, args.json)
}

async fn wiki_evaluate_recurrence(profile: &str, args: WikiRecurrenceArgs) -> Result<()> {
    let profile_dir = if args.dry_run {
        read_only_profile_dir(profile)?
    } else {
        get_profile_dir(profile)?
    };
    let report = AdaptiveWikiStore::new(&profile_dir).generate_correction_recurrence_report(
        &OffdeskTaskStore::new(&profile_dir).load()?,
        &BackgroundRunStore::new(&profile_dir).load()?,
        &TaskResumeStore::new(&profile_dir).load()?,
        &args.entry_id,
        args.dry_run,
        Utc::now(),
    )?;

    present_wiki_correction_recurrence_report(&report, args.json)
}

async fn wiki_promotion_chain(profile: &str, args: WikiPromotionChainArgs) -> Result<()> {
    let profile_dir = if args.dry_run {
        read_only_profile_dir(profile)?
    } else {
        get_profile_dir(profile)?
    };
    let report = AdaptiveWikiStore::new(&profile_dir).generate_promotion_evidence_chain_report(
        &args.entry_id,
        args.dry_run,
        Utc::now(),
    )?;

    present_wiki_promotion_chain_report(&report, args.json)
}

async fn wiki_record_candidate(profile: &str, args: WikiRecordCandidateArgs) -> Result<()> {
    let scope_ref = args
        .scope_ref
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if args.scope != AdaptiveWikiScope::UserGlobal && scope_ref.is_none() {
        bail!("--scope-ref is required when --scope is not user_global");
    }
    if args.claim.trim().is_empty() {
        bail!("--claim must not be empty");
    }

    let evidence_refs: Vec<String> = args
        .evidence_refs
        .iter()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .collect();

    let input = AdaptiveWikiCandidateInput {
        kind: args.kind,
        scope: args.scope,
        scope_ref: scope_ref.unwrap_or("*").to_string(),
        claim: args.claim.trim().to_string(),
        suggested_ai_instruction: args.ai_instruction.trim().to_string(),
        human_summary: args.human_summary.trim().to_string(),
        // Primary evidence lands in evidence_refs; the full doc list is kept as
        // source provenance so nothing from the review is lost.
        evidence_ref: evidence_refs.first().cloned(),
        signal_kind: args.signal_kind,
        origin: args.origin,
        source_refs: evidence_refs.clone(),
        source_hashes: Vec::new(),
        suggested_scope: None,
        agent_modes: args.agent_modes.clone(),
        core_tags: args.core_tags.clone(),
        proposed_tags: args.proposed_tags.clone(),
        review_reason: args.review_reason.trim().to_string(),
        confidence: args.confidence,
    };

    let store = writable_wiki_store(profile)?;
    let candidate = store.record_candidate(input, Utc::now())?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&candidate)?);
    } else {
        println!("Recorded candidate {}", candidate.id);
        println!(
            "  {:?} · {:?}:{} · confidence {:?}",
            candidate.kind, candidate.scope, candidate.scope_ref, candidate.confidence
        );
        println!("  claim: {}", candidate.claim);
        println!("  occurrences: {}", candidate.occurrence_count);
        println!(
            "  promote: forager -p {profile} offdesk wiki promote {} --activation-mode context_only",
            candidate.id
        );
    }
    Ok(())
}

async fn wiki_promote(profile: &str, args: WikiPromoteArgs) -> Result<()> {
    if args.scope_ref.is_some() && args.scope.is_none() {
        bail!("--scope-ref requires --scope for wiki promote");
    }
    if args
        .scope
        .is_some_and(|scope| scope != AdaptiveWikiScope::UserGlobal)
        && args
            .scope_ref
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .is_none()
    {
        bail!("--scope-ref is required when --scope is not user_global");
    }
    let now = Utc::now();
    let store = writable_wiki_store(profile)?;
    let candidate = find_wiki_candidate(&store, &args.candidate_id)?.ok_or_else(|| {
        anyhow::anyhow!("Adaptive wiki candidate not found: {}", args.candidate_id)
    })?;
    let scope_override = args.scope.map(|scope| AdaptiveWikiScopeSuggestion {
        scope,
        scope_ref: args
            .scope_ref
            .clone()
            .unwrap_or_else(|| default_wiki_scope_ref(scope)),
    });
    let entry = store
        .promote_candidate_scoped_with_agent_modes(
            &args.candidate_id,
            args.activation_mode,
            scope_override.clone(),
            args.agent_modes.clone(),
            now,
        )?
        .ok_or_else(|| {
            anyhow::anyhow!("Adaptive wiki candidate not found: {}", args.candidate_id)
        })?;
    // Skepticism by default: every promotion carries a review horizon so stale
    // knowledge surfaces as STALE in briefs/reports instead of aging silently.
    let entry = if args.review_after_days > 0 {
        store
            .renew_review_after(
                &entry.id,
                now + chrono::Duration::days(args.review_after_days),
                now,
            )?
            .unwrap_or(entry)
    } else {
        entry
    };
    let candidate_snapshot = human_candidate(candidate.clone());
    let entry_snapshot = human_entry(entry.clone());
    let audit = wiki_audit_record(WikiAuditRecordInput {
        action: AdaptiveWikiAuditAction::Promote,
        subject_id: &entry.id,
        candidate_id: Some(&candidate.id),
        entry_id: Some(&entry.id),
        actor: &args.by,
        reason: &args.reason,
        evidence_ref: None,
        before_scope: Some(wiki_candidate_scope(&candidate)),
        after_scope: Some(wiki_entry_scope(&entry)),
        activation_mode: Some(args.activation_mode),
        candidate_snapshot: Some(candidate_snapshot.clone()),
        entry_snapshot: Some(entry_snapshot.clone()),
        now,
    });
    store.append_audit(&audit)?;
    let promotion_receipt = AdaptiveWikiPromotionReceipt {
        schema: AdaptiveWikiPromotionReceipt::schema_name().to_string(),
        receipt_id: format!("wiki_promotion_receipt_{}", Uuid::new_v4()),
        generated_at: now,
        status: "promoted".to_string(),
        read_only_review_artifact: true,
        candidate_id: candidate.id.clone(),
        entry_id: entry.id.clone(),
        audit_id: audit.id.clone(),
        actor: audit.actor.clone(),
        reason: audit.reason.clone(),
        activation_mode: args.activation_mode,
        before_scope: wiki_candidate_scope(&candidate),
        after_scope: wiki_entry_scope(&entry),
        candidate_snapshot,
        entry_snapshot: entry_snapshot.clone(),
        authority: AdaptiveWikiPromotionReceiptAuthority {
            canonical_mutation_recorded: true,
            does_not_authorize: vec![
                "future automatic projection without current policy checks".to_string(),
                "cleanup, archive, file movement, or deletion".to_string(),
                "provider/model retargeting or runtime launch".to_string(),
                "accepted truth for task outputs".to_string(),
            ],
        },
    };
    let promotion_receipt_path = store.write_promotion_receipt(&promotion_receipt)?;
    let result = WikiMutationResult::Promote {
        entry: entry_snapshot,
        audit,
        promotion_receipt: Box::new(promotion_receipt),
        promotion_receipt_path: crate::offdesk::operator_safe_text(
            promotion_receipt_path.to_string_lossy().as_ref(),
        ),
    };
    present_wiki_mutation(&result, args.json)
}

async fn wiki_reject(profile: &str, args: WikiRejectArgs) -> Result<()> {
    require_non_empty_arg("--reason", &args.reason)?;
    let now = Utc::now();
    let store = writable_wiki_store(profile)?;
    let candidate = store.reject_candidate(&args.candidate_id)?.ok_or_else(|| {
        anyhow::anyhow!("Adaptive wiki candidate not found: {}", args.candidate_id)
    })?;
    let audit = wiki_audit_record(WikiAuditRecordInput {
        action: AdaptiveWikiAuditAction::Reject,
        subject_id: &candidate.id,
        candidate_id: Some(&candidate.id),
        entry_id: None,
        actor: &args.by,
        reason: &args.reason,
        evidence_ref: None,
        before_scope: Some(wiki_candidate_scope(&candidate)),
        after_scope: None,
        activation_mode: None,
        candidate_snapshot: None,
        entry_snapshot: None,
        now,
    });
    store.append_audit(&audit)?;
    let result = WikiMutationResult::Reject {
        candidate: human_candidate(candidate),
        audit,
    };
    present_wiki_mutation(&result, args.json)
}

async fn wiki_rescope(profile: &str, args: WikiRescopeArgs) -> Result<()> {
    require_non_empty_arg("--scope-ref", &args.scope_ref)?;
    let now = Utc::now();
    let store = writable_wiki_store(profile)?;
    let before = find_wiki_entry(&store, &args.entry_id)?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let entry = store
        .rescope_entry(&args.entry_id, args.scope, &args.scope_ref, now)?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let audit = wiki_audit_record(WikiAuditRecordInput {
        action: AdaptiveWikiAuditAction::Rescope,
        subject_id: &entry.id,
        candidate_id: None,
        entry_id: Some(&entry.id),
        actor: &args.by,
        reason: &args.reason,
        evidence_ref: None,
        before_scope: Some(wiki_entry_scope(&before)),
        after_scope: Some(wiki_entry_scope(&entry)),
        activation_mode: None,
        candidate_snapshot: None,
        entry_snapshot: None,
        now,
    });
    store.append_audit(&audit)?;
    let result = WikiMutationResult::Rescope {
        entry: human_entry(entry),
        audit,
    };
    present_wiki_mutation(&result, args.json)
}

async fn wiki_edit(profile: &str, args: WikiEditArgs) -> Result<()> {
    if !args.agent_modes.is_empty() && args.clear_agent_modes {
        bail!("wiki edit cannot combine --agent-mode with --clear-agent-modes");
    }
    if args.kind.is_none()
        && args.agent_modes.is_empty()
        && !args.clear_agent_modes
        && args.claim.is_none()
        && args.ai_instruction.is_none()
        && args.human_summary.is_none()
        && args.evidence_refs.is_empty()
    {
        bail!("wiki edit needs at least one classification, mode, text, or evidence change");
    }
    let now = Utc::now();
    let store = writable_wiki_store(profile)?;
    let replacement_modes = if args.clear_agent_modes {
        Some(Vec::new())
    } else if args.agent_modes.is_empty() {
        None
    } else {
        Some(args.agent_modes)
    };
    let entry = store
        .edit_entry(
            &args.entry_id,
            AdaptiveWikiEntryEdit {
                kind: args.kind,
                agent_modes: replacement_modes,
                claim: args.claim,
                ai_instruction: args.ai_instruction,
                human_summary: args.human_summary,
                add_evidence_refs: args.evidence_refs,
            },
            now,
        )?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let entry_snapshot = human_entry(entry);
    let audit = wiki_audit_record(WikiAuditRecordInput {
        action: AdaptiveWikiAuditAction::Edit,
        subject_id: &entry_snapshot.id,
        candidate_id: None,
        entry_id: Some(&entry_snapshot.id),
        actor: &args.by,
        reason: &args.reason,
        evidence_ref: None,
        before_scope: None,
        after_scope: None,
        activation_mode: None,
        candidate_snapshot: None,
        entry_snapshot: Some(entry_snapshot.clone()),
        now,
    });
    store.append_audit(&audit)?;
    let result = WikiMutationResult::Edit {
        entry: entry_snapshot,
        audit,
    };
    present_wiki_mutation(&result, args.json)
}

async fn wiki_add_tag(profile: &str, args: WikiAddTagArgs) -> Result<()> {
    if args.core_tags.is_empty() && args.proposed_tags.is_empty() {
        bail!("wiki add-tag needs at least one --core-tag or --proposed-tag");
    }
    let now = Utc::now();
    let store = writable_wiki_store(profile)?;
    let entry = store
        .add_entry_tags(&args.entry_id, &args.core_tags, &args.proposed_tags, now)?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let audit = wiki_audit_record(WikiAuditRecordInput {
        action: AdaptiveWikiAuditAction::Retag,
        subject_id: &entry.id,
        candidate_id: None,
        entry_id: Some(&entry.id),
        actor: &args.by,
        reason: &args.reason,
        evidence_ref: None,
        before_scope: None,
        after_scope: None,
        activation_mode: None,
        candidate_snapshot: None,
        entry_snapshot: None,
        now,
    });
    store.append_audit(&audit)?;
    let result = WikiMutationResult::Retag {
        entry: human_entry(entry),
        audit,
    };
    present_wiki_mutation(&result, args.json)
}

async fn wiki_deprecate(profile: &str, args: WikiDeprecateArgs) -> Result<()> {
    require_non_empty_arg("--reason", &args.reason)?;
    let now = Utc::now();
    let store = writable_wiki_store(profile)?;
    let before = find_wiki_entry(&store, &args.entry_id)?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let entry = store
        .deprecate_entry(&args.entry_id, now)?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let audit = wiki_audit_record(WikiAuditRecordInput {
        action: AdaptiveWikiAuditAction::Deprecate,
        subject_id: &entry.id,
        candidate_id: None,
        entry_id: Some(&entry.id),
        actor: &args.by,
        reason: &args.reason,
        evidence_ref: None,
        before_scope: Some(wiki_entry_scope(&before)),
        after_scope: Some(wiki_entry_scope(&entry)),
        activation_mode: None,
        candidate_snapshot: None,
        entry_snapshot: None,
        now,
    });
    store.append_audit(&audit)?;
    let result = WikiMutationResult::Deprecate {
        entry: human_entry(entry),
        audit,
    };
    present_wiki_mutation(&result, args.json)
}

async fn wiki_renew_review_after(profile: &str, args: WikiRenewReviewAfterArgs) -> Result<()> {
    require_non_empty_arg("--reason", &args.reason)?;
    let now = Utc::now();
    if args.review_after <= now {
        bail!("--review-after must be in the future");
    }
    let store = writable_wiki_store(profile)?;
    let before = find_wiki_entry(&store, &args.entry_id)?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let previous_review_after = before.review_after;
    let entry = store
        .renew_review_after(&args.entry_id, args.review_after, now)?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let entry_snapshot = human_entry(entry.clone());
    let audit = wiki_audit_record(WikiAuditRecordInput {
        action: AdaptiveWikiAuditAction::RenewReviewAfter,
        subject_id: &entry.id,
        candidate_id: None,
        entry_id: Some(&entry.id),
        actor: &args.by,
        reason: &args.reason,
        evidence_ref: None,
        before_scope: Some(wiki_entry_scope(&before)),
        after_scope: Some(wiki_entry_scope(&entry)),
        activation_mode: None,
        candidate_snapshot: None,
        entry_snapshot: Some(entry_snapshot.clone()),
        now,
    });
    store.append_audit(&audit)?;
    let result = WikiMutationResult::RenewReviewAfter {
        entry: entry_snapshot,
        previous_review_after,
        audit,
    };
    present_wiki_mutation(&result, args.json)
}

async fn wiki_add_counterexample(profile: &str, args: WikiCounterexampleArgs) -> Result<()> {
    require_non_empty_arg("--evidence-ref", &args.evidence_ref)?;
    require_non_empty_arg("--reason", &args.reason)?;
    let now = Utc::now();
    let store = writable_wiki_store(profile)?;
    let before = find_wiki_entry(&store, &args.entry_id)?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let entry = store
        .add_counterexample(&args.entry_id, &args.evidence_ref, now)?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let audit = wiki_audit_record(WikiAuditRecordInput {
        action: AdaptiveWikiAuditAction::AddCounterexample,
        subject_id: &entry.id,
        candidate_id: None,
        entry_id: Some(&entry.id),
        actor: &args.by,
        reason: &args.reason,
        evidence_ref: Some(&args.evidence_ref),
        before_scope: Some(wiki_entry_scope(&before)),
        after_scope: Some(wiki_entry_scope(&entry)),
        activation_mode: None,
        candidate_snapshot: None,
        entry_snapshot: None,
        now,
    });
    store.append_audit(&audit)?;
    let result = WikiMutationResult::AddCounterexample {
        entry: human_entry(entry),
        audit,
    };
    present_wiki_mutation(&result, args.json)
}

async fn wiki_update_runbook(profile: &str, args: WikiRunbookArgs) -> Result<()> {
    require_non_empty_arg("--reason", &args.reason)?;
    if args.support_ref.is_empty()
        && args.capability_id.is_empty()
        && args.required_artifact_kind.is_empty()
    {
        bail!(
            "at least one --support-ref, --capability-id, or --required-artifact-kind is required"
        );
    }
    let now = Utc::now();
    let store = writable_wiki_store(profile)?;
    let before = find_wiki_entry(&store, &args.entry_id)?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    if before.kind != AdaptiveWikiKind::Procedure {
        bail!(
            "Adaptive wiki entry {} is {:?}, not Procedure",
            args.entry_id,
            before.kind
        );
    }
    let entry = store
        .update_runbook_refs(
            &args.entry_id,
            &args.support_ref,
            &args.capability_id,
            &args.required_artifact_kind,
            now,
        )?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let audit = wiki_audit_record(WikiAuditRecordInput {
        action: AdaptiveWikiAuditAction::UpdateRunbook,
        subject_id: &entry.id,
        candidate_id: None,
        entry_id: Some(&entry.id),
        actor: &args.by,
        reason: &args.reason,
        evidence_ref: args.support_ref.first().map(String::as_str),
        before_scope: Some(wiki_entry_scope(&before)),
        after_scope: Some(wiki_entry_scope(&entry)),
        activation_mode: None,
        candidate_snapshot: None,
        entry_snapshot: None,
        now,
    });
    store.append_audit(&audit)?;
    let result = WikiMutationResult::UpdateRunbook {
        entry: human_entry(entry),
        audit,
    };
    present_wiki_mutation(&result, args.json)
}

async fn cancel_task(profile: &str, args: CancelTaskArgs) -> Result<()> {
    let report =
        task_store(profile)?.cancel_task(&args.task_id, args.reason.as_deref(), Utc::now())?;
    present_task_lifecycle_report(&report, args.json)
}

async fn pause_dispatch(profile: &str, args: PauseArgs) -> Result<()> {
    let state = OperatorPauseStore::new(get_profile_dir(profile)?).pause(
        args.reason.as_deref(),
        Some(&args.by),
        Utc::now(),
    )?;
    present_operator_pause_state(&state, args.json)
}

async fn unpause_dispatch(profile: &str, args: UnpauseArgs) -> Result<()> {
    let state =
        OperatorPauseStore::new(get_profile_dir(profile)?).resume(Some(&args.by), Utc::now())?;
    present_operator_pause_state(&state, args.json)
}

async fn pause_status(profile: &str, args: JsonArgs) -> Result<()> {
    let state = OperatorPauseStore::new(get_profile_dir(profile)?).load()?;
    present_operator_pause_state(&state, args.json)
}

async fn learning_scan(profile: &str, args: JsonArgs) -> Result<()> {
    let report = scan_and_emit_learning_signals(get_profile_dir(profile)?, Utc::now())?;
    present_learning_scan_report(&report, args.json)
}

async fn retry_task(profile: &str, args: RetryTaskArgs) -> Result<()> {
    let now = Utc::now();
    let report = task_store(profile)?.retry_task(&args.task_id, now)?;
    let superseded_denied_approvals = if args.new_approval {
        approval_ledger(profile)?
            .supersede_denied_for_task(
                &report.task.project_key,
                &report.task.request_id,
                &report.task.task_id,
                &report.task.capability_id,
                "cli",
                now,
            )?
            .len()
    } else {
        0
    };
    present_retry_task_lifecycle_report(
        &report,
        superseded_denied_approvals,
        args.json,
        args.new_approval,
    )
}

async fn resume_task(profile: &str, args: TaskLifecycleArgs) -> Result<()> {
    let report = task_store(profile)?.resume_task(&args.task_id, Utc::now())?;
    present_task_lifecycle_report(&report, args.json)
}

async fn abandon_task(profile: &str, args: TaskLifecycleArgs) -> Result<()> {
    let report = task_store(profile)?.abandon_task(&args.task_id, Utc::now())?;
    present_task_lifecycle_report(&report, args.json)
}

async fn gate(profile: &str, args: GateArgs) -> Result<()> {
    let brief = load_execution_brief(args.brief.as_ref())?;

    let mut request = SchedulerGateRequest::new(
        args.capability_id,
        args.project_key,
        args.request_id,
        args.task_id,
    );
    request.mutation_class = args.mutation_class;
    request.artifact_refs = args.artifact_refs;
    request.artifact_kind = args.artifact_kind;
    request.agent_mode = args.agent_mode;
    request.preview = args.preview;
    request.reason = args.reason;
    request.source_surface = args.source_surface;
    request.ttl = Duration::minutes(args.ttl_minutes.max(1));
    request.provider_id = args.provider_id;
    request.model = args.model;

    let profile_dir = get_profile_dir(profile)?;
    let outcome = SchedulerGate::with_provider_capacity(
        ApprovalLedger::new(&profile_dir),
        ProviderCapacityStore::new(&profile_dir),
    )
    .with_adaptive_wiki(AdaptiveWikiStore::new(&profile_dir))
    .evaluate(request, brief.as_ref(), Utc::now())?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&outcome)?);
        return Ok(());
    }

    print_gate_outcome(&outcome);
    Ok(())
}

async fn launch(profile: &str, args: LaunchArgs) -> Result<()> {
    let command = args.command;
    let workdir = args.workdir;
    let log_artifact = args.log_artifact;
    let result_artifact = args.result_artifact;
    let agent_mode = args.agent_mode;
    let json = args.json;
    let brief = load_execution_brief(args.brief.as_ref())?;
    let profile_dir = get_profile_dir(profile)?;
    let implementation_packet = resolve_implementation_packet_context(
        &profile_dir,
        &args.project_key,
        args.implementation_packet.as_deref(),
    )?;
    let mut artifact_refs = args.artifact_refs;
    attach_implementation_packet_artifact_refs(&mut artifact_refs, implementation_packet.as_ref());
    let mut gate_request = SchedulerGateRequest::new(
        args.capability_id,
        args.project_key,
        args.request_id,
        args.task_id,
    );
    gate_request.mutation_class = args.mutation_class;
    gate_request.artifact_refs = artifact_refs;
    gate_request.artifact_kind = args.artifact_kind;
    gate_request.agent_mode = agent_mode;
    gate_request.preview = args.preview;
    gate_request.reason = args.reason;
    gate_request.source_surface = args.source_surface;
    gate_request.ttl = Duration::minutes(args.ttl_minutes.max(1));
    gate_request.provider_id = args.provider_id;
    gate_request.model = args.model;

    let mut launch_request = BackgroundLaunchRequest::new(gate_request, args.runner);
    launch_request.ticket_id = args.ticket_id;
    launch_request.launch_spec_summary = args.launch_spec;
    launch_request.implementation_packet = implementation_packet
        .as_ref()
        .map(|packet| packet.summary.clone());
    launch_request.runtime_handle_alive = args.runtime_alive;
    launch_request.provider_launch_spec_reconstructable = args.provider_launch_spec_reconstructable;
    launch_request.ack_timeout_sec = args.ack_timeout_sec;

    let gate = SchedulerGate::with_provider_capacity(
        ApprovalLedger::new(&profile_dir),
        ProviderCapacityStore::new(&profile_dir),
    )
    .with_adaptive_wiki(AdaptiveWikiStore::new(&profile_dir));
    let store = BackgroundRunStore::new(&profile_dir);
    let now = Utc::now();
    let outcome = if let Some(command) = command {
        let mut command_spec =
            LocalCommandLaunchSpec::new(command, workdir.unwrap_or(std::env::current_dir()?));
        command_spec.log_artifact_path = log_artifact;
        command_spec.result_artifact_path = result_artifact;
        launch_background_command(
            &gate,
            &store,
            launch_request,
            brief.as_ref(),
            now,
            command_spec,
        )?
    } else {
        launch_background_run(&gate, &store, launch_request, brief.as_ref(), now)?
    };
    append_adaptive_wiki_usage_for_launch(&profile_dir, &outcome, agent_mode, now)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&outcome)?);
        return Ok(());
    }

    print_gate_outcome(&outcome.gate);
    if let Some(probe) = outcome.probe {
        println!("  ticket_id: {}", probe.ticket_id);
        println!("  runner:    {:?}", probe.runner_kind);
        println!("  phase:     {:?}", probe.phase);
        if let Some(agent_mode) = probe.agent_mode {
            println!(
                "  agent_mode: {}",
                adaptive_wiki_agent_mode_cli_value(agent_mode)
            );
        }
        if let Some(packet) = probe.implementation_packet.as_ref() {
            println!("  packet:    {} ({})", packet.packet_id, packet.outcome);
        }
    }
    Ok(())
}

async fn poll(profile: &str, args: PollArgs) -> Result<()> {
    let now = Utc::now();
    let notification_cooldown = args
        .notify_cooldown_minutes
        .map(|minutes| Duration::minutes(minutes.max(1)));
    let outcomes = poll_background_runs(
        &background_store(profile)?,
        args.ticket_id.as_deref(),
        now,
        notification_cooldown,
    )?;
    reconcile_tasks_with_background_outcomes(get_profile_dir(profile)?, &outcomes, now)?;
    present_background_poll_outcomes(&outcomes, args.json)
}

async fn pending(profile: &str, args: PendingArgs) -> Result<()> {
    let ledger = approval_ledger(profile)?;
    let now = Utc::now();
    ledger.expire_due(now)?;
    let approvals: Vec<PendingActionApproval> = ledger
        .load()?
        .into_iter()
        .filter(|approval| args.all || approval.status == ApprovalStatus::Pending)
        .collect();
    let approval_views = pending_approval_operator_views(approvals, now);

    if args.json {
        println!("{}", serde_json::to_string_pretty(&approval_views)?);
        return Ok(());
    }

    if approval_views.is_empty() {
        println!("No offdesk approvals found.");
        return Ok(());
    }

    print_approval_views(&approval_views);
    Ok(())
}

async fn resolve(profile: &str, args: ResolveArgs, approve: bool) -> Result<()> {
    let ledger = approval_ledger(profile)?;
    let now = Utc::now();
    let resolved = if approve {
        ledger.approve_pending(args.approval_id.as_deref(), &args.by, now)?
    } else {
        ledger.deny_pending(args.approval_id.as_deref(), &args.by, now)?
    };

    let Some(resolved) = resolved else {
        if let Some(approval_id) = args.approval_id {
            bail!("Pending offdesk approval not found: {}", approval_id);
        }
        println!("No pending offdesk approvals.");
        return Ok(());
    };

    if !approve {
        record_approval_denial_candidate(profile, &resolved, now)?;
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&resolved)?);
        return Ok(());
    }

    let verb = if approve { "Approved" } else { "Denied" };
    println!(
        "{} offdesk approval {}: {} ({:?})",
        verb, resolved.approval_id, resolved.action, resolved.risk_level
    );
    Ok(())
}

fn record_approval_denial_candidate(
    profile: &str,
    approval: &PendingActionApproval,
    now: DateTime<Utc>,
) -> Result<()> {
    if approval.status != ApprovalStatus::Denied {
        return Ok(());
    }

    let (scope, scope_ref) = if !approval.project_key.trim().is_empty() {
        (AdaptiveWikiScope::Project, approval.project_key.clone())
    } else {
        (AdaptiveWikiScope::Session, approval.request_id.clone())
    };
    let denial_detail = first_non_empty(&[&approval.reason, &approval.preview, &approval.action])
        .unwrap_or("operator denied approval");
    let safe_action = crate::offdesk::operator_safe_text(&approval.action);
    let safe_detail = crate::offdesk::operator_safe_text(denial_detail);
    let safe_task_id = crate::offdesk::operator_safe_text(&approval.task_id);
    let claim = format!(
        "Operator denied `{}` for task `{}`: {}",
        safe_action, safe_task_id, safe_detail
    );
    let instruction = format!(
        "Before retrying `{}`, review the previous denial and ask for explicit operator confirmation.",
        safe_action
    );
    let source_refs = vec![
        format!(
            "approval:{}",
            crate::offdesk::operator_safe_text(&approval.approval_id)
        ),
        format!("task:{}", safe_task_id),
        format!(
            "request:{}",
            crate::offdesk::operator_safe_text(&approval.request_id)
        ),
    ];
    let suggested_scope = AdaptiveWikiScopeSuggestion {
        scope,
        scope_ref: scope_ref.clone(),
    };

    AdaptiveWikiStore::new(get_profile_dir(profile)?).record_candidate(
        AdaptiveWikiCandidateInput {
            kind: AdaptiveWikiKind::PolicyRule,
            scope,
            scope_ref,
            claim,
            suggested_ai_instruction: instruction,
            human_summary: "Captured from an explicit operator approval denial.".to_string(),
            evidence_ref: Some(format!(
                "approval:{}",
                crate::offdesk::operator_safe_text(&approval.approval_id)
            )),
            signal_kind: AdaptiveWikiSignalKind::ApprovalDenial,
            origin: AdaptiveWikiOrigin::OperatorExplicit,
            source_refs,
            source_hashes: Vec::new(),
            suggested_scope: Some(suggested_scope),
            agent_modes: Vec::new(),
            core_tags: vec!["risk/operator-denial".to_string()],
            proposed_tags: Vec::new(),
            review_reason:
                "Operator denied an Offdesk approval; review before promoting as durable policy."
                    .to_string(),
            confidence: AdaptiveWikiConfidence::Explicit,
        },
        now,
    )?;

    Ok(())
}

fn append_adaptive_wiki_usage_for_launch(
    profile_dir: &Path,
    outcome: &BackgroundLaunchOutcome,
    agent_mode: Option<AdaptiveWikiAgentMode>,
    now: DateTime<Utc>,
) -> Result<()> {
    let Some(probe) = outcome.probe.as_ref() else {
        return Ok(());
    };
    if probe.adaptive_wiki_entry_ids.is_empty() {
        return Ok(());
    }
    let records = build_usage_records_with_policy(
        &outcome.gate.adaptive_wiki_runtime,
        AdaptiveWikiUsageContext {
            task_id: probe.task_id.as_deref().unwrap_or("-"),
            request_id: probe.request_id.as_deref().unwrap_or("-"),
            project_key: probe.project_key.as_deref().unwrap_or("-"),
            artifact_kind: None,
            agent_mode,
            projection_kind: "runtime_probe",
            projection_policy: Some(outcome.gate.adaptive_wiki_runtime_policy),
            now,
        },
    );
    AdaptiveWikiStore::new(profile_dir).append_usage_records(&records)
}

fn background_probe_status(probe: BackgroundProbe, now: DateTime<Utc>) -> BackgroundProbeStatus {
    let decision = probe.evaluate(now);
    let mode_assessment = assess_offdesk_mode(
        probe.agent_mode,
        background_mode_lifecycle(&decision, probe.result_artifact_present),
    );
    BackgroundProbeStatus {
        probe,
        decision,
        mode_assessment,
    }
}

fn background_mode_lifecycle(
    decision: &BackgroundRecoveryDecision,
    result_artifact_present: bool,
) -> OffdeskModeLifecycle {
    match decision.phase {
        BackgroundRunnerPhase::Completed | BackgroundRunnerPhase::ResultReceived
            if result_artifact_present =>
        {
            OffdeskModeLifecycle::CompletedWithResult
        }
        BackgroundRunnerPhase::Completed | BackgroundRunnerPhase::ResultReceived => {
            OffdeskModeLifecycle::CompletedWithoutResult
        }
        BackgroundRunnerPhase::Failed
        | BackgroundRunnerPhase::StaleNoAck
        | BackgroundRunnerPhase::StaleLostCallback
        | BackgroundRunnerPhase::Reconstructable => OffdeskModeLifecycle::Blocked,
        BackgroundRunnerPhase::RecoveryAcknowledged => OffdeskModeLifecycle::Cancelled,
        BackgroundRunnerPhase::Launched
        | BackgroundRunnerPhase::HandoffEmitted
        | BackgroundRunnerPhase::PickupAcknowledged => OffdeskModeLifecycle::Running,
    }
}

async fn resume(profile: &str, args: JsonArgs) -> Result<()> {
    let states = resume_store(profile)?.load()?;
    present_resume_states(&states, args.json)
}

async fn background(profile: &str, args: JsonArgs) -> Result<()> {
    let now = Utc::now();
    let statuses: Vec<BackgroundProbeStatus> =
        poll_background_runs(&background_store(profile)?, None, now, None)?
            .into_iter()
            .map(|outcome| BackgroundProbeStatus {
                mode_assessment: outcome.mode_assessment,
                decision: outcome.decision,
                probe: outcome.probe,
            })
            .collect();
    present_background_statuses(&statuses, args.json)
}

async fn background_ack(profile: &str, args: BackgroundAckArgs) -> Result<()> {
    let now = Utc::now();
    let store = background_store(profile)?;
    let outcomes = poll_background_runs(&store, Some(&args.ticket_id), now, None)?;
    let outcome = outcomes
        .first()
        .with_context(|| format!("background ticket not found: {}", args.ticket_id))?;

    if outcome.decision.phase == BackgroundRunnerPhase::RecoveryAcknowledged {
        let ack = outcome
            .probe
            .operator_recovery_ack
            .clone()
            .context("background probe is acknowledged but missing acknowledgement metadata")?;
        let report = BackgroundAckReport {
            ticket_id: outcome.probe.ticket_id.clone(),
            linked_task_ids: ack.linked_task_ids.clone(),
            acknowledgement: ack,
            status: BackgroundProbeStatus {
                probe: outcome.probe.clone(),
                decision: outcome.decision.clone(),
                mode_assessment: outcome.mode_assessment.clone(),
            },
        };
        present_background_ack_report(&report, args.json)?;
        return Ok(());
    }

    if !is_background_recovery_attention_phase(outcome.decision.phase) {
        bail!(
            "background ticket {} is {:?}; acknowledgement is only allowed for stale or failed recovery states",
            outcome.probe.ticket_id,
            outcome.decision.phase
        );
    }

    let profile_dir = get_profile_dir(profile)?;
    let linked_tasks = linked_tasks_for_background(&profile_dir, &outcome.probe)?;
    if linked_tasks.is_empty() && !args.allow_unlinked {
        bail!(
            "background ticket {} is not linked to a durable task; pass --allow-unlinked only after separate evidence review",
            outcome.probe.ticket_id
        );
    }
    let blocking_tasks = linked_tasks
        .iter()
        .filter(|task| task.status != OffdeskTaskStatus::Cancelled)
        .map(|task| format!("{}:{:?}", task.task_id, task.status))
        .collect::<Vec<_>>();
    if !blocking_tasks.is_empty() {
        bail!(
            "background ticket {} still has non-cancelled linked tasks: {}; use resume-task, retry-task, or abandon-task first",
            outcome.probe.ticket_id,
            blocking_tasks.join(", ")
        );
    }

    let linked_task_ids = linked_tasks
        .iter()
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    let acknowledgement = BackgroundRecoveryAcknowledgement {
        acknowledged_at: now,
        acknowledged_by: crate::offdesk::operator_safe_text(&args.by),
        reason: crate::offdesk::operator_safe_text(&args.reason),
        previous_phase: outcome.decision.phase,
        linked_task_ids: linked_task_ids.clone(),
        source_surface: crate::offdesk::operator_safe_text(&args.source_surface),
        does_not_authorize: background_ack_does_not_authorize(),
    };

    let mut probes = store.load()?;
    let updated_probe = {
        let probe = probes
            .iter_mut()
            .find(|probe| probe.ticket_id == outcome.probe.ticket_id)
            .context("background ticket disappeared while recording acknowledgement")?;
        probe.operator_recovery_ack = Some(acknowledgement.clone());
        probe.phase = BackgroundRunnerPhase::RecoveryAcknowledged;
        probe.last_observed_at = Some(now);
        probe.last_recovery_evidence = Some(
            "operator acknowledged background recovery; no result is accepted from this probe"
                .to_string(),
        );
        probe.last_recovery_terminal = Some(true);
        probe.clone()
    };
    store.save(&probes)?;

    let status = background_probe_status(updated_probe.clone(), now);
    let report = BackgroundAckReport {
        ticket_id: updated_probe.ticket_id.clone(),
        linked_task_ids,
        acknowledgement,
        status,
    };
    present_background_ack_report(&report, args.json)?;
    Ok(())
}

fn is_background_recovery_attention_phase(phase: BackgroundRunnerPhase) -> bool {
    matches!(
        phase,
        BackgroundRunnerPhase::Failed
            | BackgroundRunnerPhase::StaleNoAck
            | BackgroundRunnerPhase::StaleLostCallback
            | BackgroundRunnerPhase::Reconstructable
    )
}

fn linked_tasks_for_background(
    profile_dir: &Path,
    probe: &BackgroundProbe,
) -> Result<Vec<OffdeskTask>> {
    let tasks = OffdeskTaskStore::new(profile_dir).load()?;
    Ok(tasks
        .into_iter()
        .filter(|task| {
            task.background_ticket_id.as_deref() == Some(probe.ticket_id.as_str())
                || probe.task_id.as_deref() == Some(task.task_id.as_str())
        })
        .collect())
}

fn background_ack_does_not_authorize() -> Vec<String> {
    vec![
        "accepting any Offdesk output as truth".to_string(),
        "closing out or promoting result artifacts".to_string(),
        "retrying or resuming runtime work".to_string(),
        "moving, archiving, or deleting files".to_string(),
    ]
}

async fn capabilities(args: JsonArgs) -> Result<()> {
    let registry = default_capability_registry();
    let capabilities = registry.all();

    if args.json {
        println!("{}", serde_json::to_string_pretty(capabilities)?);
        return Ok(());
    }

    print_capabilities(capabilities);
    Ok(())
}

async fn harnesses(args: JsonArgs) -> Result<()> {
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(hosted_harness_profiles())?
        );
        return Ok(());
    }

    println!("Hosted harness agent profiles");
    println!("Current support target: Codex CLI and Claude Code");
    println!();
    for profile in hosted_harness_profiles() {
        let command = profile.launch_command.unwrap_or("manual integration");
        println!(
            "- {} ({}) [{}]",
            profile.display_name, profile.id, profile.support_status
        );
        println!("  launch:  {}", command);
        println!("  runner:  {}", profile.runner);
        println!("  scope:   {}", profile.mutation_scope);
        println!(
            "  prompt:  {} (inline <= {} bytes, first-read <= {} bytes total)",
            profile.prompt_contract.strategy,
            profile.prompt_contract.inline_context_budget_bytes,
            profile.prompt_contract.first_read_total_budget_bytes
        );
        println!(
            "  reads:   {}",
            profile.prompt_contract.preferred_first_reads.join(", ")
        );
        println!("  result:  {}", profile.result_artifact);
        println!("  failure: {}", profile.failure_signal);
        println!("  note:    {}", profile.notes);
    }
    Ok(())
}

async fn harness_prompt(args: HarnessPromptArgs) -> Result<()> {
    let profile = hosted_harness_profile(&args.harness_id)
        .with_context(|| format!("unknown hosted harness id `{}`", args.harness_id))?;
    let json = args.json;
    let strict = args.strict_first_read_budget;
    let packet = build_harness_prompt_packet(
        profile,
        HarnessPromptRequest {
            task: args.task,
            first_reads: args.first_reads,
            result_artifact: args.result_artifact,
            workdir: args.workdir,
            output: args.output,
            max_first_read_total_bytes: args.max_first_read_total_bytes,
        },
    )?;

    if strict && packet.first_read_budget_status != "ok" {
        bail!(
            "first-read budget guard failed: {}",
            packet.warnings.join("; ")
        );
    }

    if let Some(output_path) = packet.output_path.as_deref() {
        let path = Path::new(output_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create prompt output dir {}", parent.display()))?;
        }
        write_new_file(path, packet.prompt_markdown.as_bytes())
            .with_context(|| format!("write harness prompt {}", path.display()))?;
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&packet)?);
        return Ok(());
    }

    if let Some(output_path) = packet.output_path.as_deref() {
        println!("Wrote hosted harness prompt: {output_path}");
    } else {
        println!("{}", packet.prompt_markdown);
    }
    for warning in &packet.warnings {
        println!("warning: {warning}");
    }
    Ok(())
}

async fn plan(profile: &str, args: PlanArgs) -> Result<()> {
    let registration = register_offdesk_plan(profile, &args)?;
    present_offdesk_plan_registration(&registration, args.json)
}

async fn plans(profile: &str, args: PlansArgs) -> Result<()> {
    let items = query_offdesk_plans(profile, &OffdeskPlanListQuery::from_plans_args(&args))?;
    present_offdesk_plan_registry_items(&items, args.json)
}

async fn plan_show(profile: &str, args: PlanShowArgs) -> Result<()> {
    let detail = query_offdesk_plan_detail(profile, &args.plan_ref)?;
    present_offdesk_plan_registry_detail(&detail, args.json)
}

async fn plan_review(profile: &str, args: PlanReviewArgs) -> Result<()> {
    let record = record_offdesk_plan_review(profile, &args)?;
    present_offdesk_plan_review_record(&record, args.json)
}

async fn plan_launch_prep(profile: &str, args: PlanLaunchPrepArgs) -> Result<()> {
    let packet = prepare_offdesk_plan_launch(profile, &args)?;
    present_offdesk_plan_launch_prep_packet(&packet, args.json)
}

async fn remote_operator(profile: &str, command: RemoteOperatorCommands) -> Result<()> {
    match command {
        RemoteOperatorCommands::Status(args) => remote_operator_status(profile, args).await,
        RemoteOperatorCommands::Pending(args) => remote_operator_pending(profile, args).await,
        RemoteOperatorCommands::Plans(args) => remote_operator_plans(profile, args).await,
        RemoteOperatorCommands::Show(args) => remote_operator_show(profile, args).await,
    }
}

async fn remote_operator_status(profile: &str, args: RemoteOperatorStatusArgs) -> Result<()> {
    let status = super::status::current_status_json_value(profile)?;
    present_remote_operator_status(profile, &args, &status)
}

async fn remote_operator_pending(profile: &str, args: RemoteOperatorPendingArgs) -> Result<()> {
    let now = Utc::now();
    let mut approvals = approval_ledger(profile)?.load()?;
    if !args.all {
        approvals.retain(|approval| approval.status == ApprovalStatus::Pending);
    }
    approvals.sort_by_key(|approval| approval.created_at);
    let approval_views = pending_approval_operator_views(approvals, now);
    present_remote_operator_pending(profile, &args, &approval_views, now)
}

async fn remote_operator_plans(profile: &str, args: RemoteOperatorPlansArgs) -> Result<()> {
    let items = query_offdesk_plans(
        profile,
        &OffdeskPlanListQuery::from_remote_operator_args(&args),
    )?;
    present_remote_operator_plans(profile, &args, &items)
}

async fn remote_operator_show(profile: &str, args: RemoteOperatorShowArgs) -> Result<()> {
    let detail = query_offdesk_plan_detail(profile, &args.plan_ref)?;
    present_remote_operator_plan_detail(profile, &args, &detail)
}

async fn snapshots(profile: &str, args: JsonArgs) -> Result<()> {
    let store = mutation_snapshot_store(profile)?;
    let now = Utc::now();
    let items = store
        .list()?
        .into_iter()
        .map(|snapshot| {
            let verification = store.verify_snapshot(&snapshot.mutation_id, now)?;
            Ok(snapshot_list_item(snapshot, verification))
        })
        .collect::<Result<Vec<_>>>()?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&items)?);
        return Ok(());
    }

    if items.is_empty() {
        println!("No mutation snapshots found.");
        return Ok(());
    }

    print_snapshot_list(&items);
    Ok(())
}

async fn snapshot(profile: &str, args: MutationSnapshotArgs) -> Result<()> {
    let verification =
        mutation_snapshot_store(profile)?.verify_snapshot(&args.mutation_id, Utc::now())?;
    if !verification.snapshot_present {
        bail!("Mutation snapshot not found: {}", args.mutation_id);
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&verification)?);
        return Ok(());
    }

    print_snapshot_verification(&verification);
    Ok(())
}

async fn restore_plan(profile: &str, args: MutationSnapshotArgs) -> Result<()> {
    let plan = mutation_snapshot_store(profile)?.restore_plan(&args.mutation_id, Utc::now())?;
    if plan.target_path.is_empty() {
        bail!("Mutation snapshot not found: {}", args.mutation_id);
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&plan)?);
        return Ok(());
    }

    print_restore_plan(&plan);
    Ok(())
}

async fn debug_bundle(profile: &str, args: DebugBundleArgs) -> Result<()> {
    let bundle = build_debug_bundle(profile)?;
    let export = if args.export || args.output.is_some() {
        Some(write_debug_bundle_export(
            profile,
            &bundle,
            args.output.as_ref(),
        )?)
    } else {
        None
    };

    if args.json {
        if let Some(export) = export.as_ref() {
            let receipt = DebugBundleExportReceipt {
                exported_to: operator_safe_report(export.path.to_string_lossy().as_ref()).text,
                bytes_written: export.bytes_written,
                bundle: &bundle,
            };
            println!("{}", serde_json::to_string_pretty(&receipt)?);
        } else {
            println!("{}", serde_json::to_string_pretty(&bundle)?);
        }
        return Ok(());
    }

    print_debug_bundle_summary(&bundle);
    if let Some(export) = export.as_ref() {
        println!(
            "  exported_to:        {}",
            operator_safe_report(export.path.to_string_lossy().as_ref()).text
        );
        println!("  bytes_written:      {}", export.bytes_written);
    }
    Ok(())
}

fn build_debug_bundle(profile: &str) -> Result<OffdeskDebugBundle> {
    let profile_dir = read_only_profile_dir(profile)?;
    let generated_at = Utc::now();
    let mut redactor = DebugBundleRedactor::default();

    let approvals = redactor.value(serde_json::to_value(
        ApprovalLedger::new(&profile_dir).load()?,
    )?);

    let task_views = OffdeskTaskStore::new(&profile_dir)
        .load()?
        .into_iter()
        .map(|task| task.operator_view())
        .collect::<Vec<_>>();
    let tasks = redactor.value(serde_json::to_value(task_views)?);

    let resume_states = redactor.value(serde_json::to_value(
        TaskResumeStore::new(&profile_dir).load()?,
    )?);

    let background_runs = BackgroundRunStore::new(&profile_dir)
        .load()?
        .into_iter()
        .map(|probe| background_probe_status(probe, generated_at))
        .collect::<Vec<_>>();
    let background_runs = redactor.value(serde_json::to_value(background_runs)?);

    let capabilities = redactor.value(serde_json::to_value(default_capability_registry().all())?);

    let provider_capacity = redactor.value(serde_json::to_value(
        ProviderCapacityStore::new(&profile_dir).load()?,
    )?);

    let wiki_store = AdaptiveWikiStore::new(&profile_dir);
    let all_wiki_query = crate::offdesk::AdaptiveWikiQuery {
        session_id: None,
        project_key: None,
        artifact_kind: None,
        agent_mode: None,
        agent_mode_filter: AdaptiveWikiAgentModeFilter::AllWhenUnspecified,
    };
    let wiki_projection = wiki_store.human_projection(&all_wiki_query)?;
    let adaptive_wiki_review_after_attention_summary = build_review_after_report(
        wiki_projection.entries.clone(),
        all_wiki_query,
        168,
        generated_at,
    )
    .summary;
    let adaptive_wiki = redactor.value(serde_json::to_value(wiki_projection)?);
    let adaptive_wiki_usage =
        redactor.value(serde_json::to_value(wiki_store.load_usage_records()?)?);
    let adaptive_wiki_corrections =
        redactor.value(serde_json::to_value(wiki_store.load_correction_records()?)?);
    let adaptive_wiki_review_events = redactor.value(serde_json::to_value(
        wiki_store.load_review_proposal_events()?,
    )?);
    let runtime_policy_acknowledgements = wiki_store.load_runtime_policy_acknowledgements()?;
    let adaptive_wiki_runtime_policy_ack_attention_summary = build_runtime_policy_ack_report(
        runtime_policy_acknowledgements.clone(),
        None,
        None,
        None,
        6,
        generated_at,
    )
    .summary;
    let adaptive_wiki_runtime_policy_acknowledgements =
        redactor.value(serde_json::to_value(runtime_policy_acknowledgements)?);

    let profile_name = if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    };
    let profile = redactor.text(profile_name);
    let profile_dir = redactor.text(profile_dir.to_string_lossy().as_ref());
    let redaction_summary = redactor.summary;
    Ok(OffdeskDebugBundle {
        generated_at,
        profile,
        profile_dir,
        read_only: true,
        redaction_applied: true,
        approvals,
        tasks,
        resume_states,
        background_runs,
        capabilities,
        provider_capacity,
        adaptive_wiki,
        adaptive_wiki_usage,
        adaptive_wiki_corrections,
        adaptive_wiki_review_events,
        adaptive_wiki_runtime_policy_acknowledgements,
        adaptive_wiki_runtime_policy_ack_attention_summary,
        adaptive_wiki_review_after_attention_summary,
        redaction_summary,
    })
}

async fn maintenance_report(profile: &str, args: MaintenanceReportArgs) -> Result<()> {
    let report = build_maintenance_report(profile, &args)?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_maintenance_report(&report);
    Ok(())
}

async fn maintenance_request(profile: &str, args: MaintenanceRequestArgs) -> Result<()> {
    let json = args.json;
    let report = build_maintenance_request(profile, args)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_maintenance_request_report(&report);
    Ok(())
}

async fn closeout(profile: &str, args: CloseoutArgs) -> Result<()> {
    let json = args.json;
    let report = build_closeout_report(profile, &args)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_closeout_report(&report);
    Ok(())
}

async fn closeout_review(profile: &str, args: CloseoutReviewArgs) -> Result<()> {
    let json = args.json;
    let record = build_closeout_review_record(profile, &args)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&record)?);
        return Ok(());
    }

    print_closeout_review_record(&record);
    Ok(())
}

async fn closeout_decision(profile: &str, args: CloseoutDecisionArgs) -> Result<()> {
    let json = args.json;
    let record = build_closeout_decision_record(profile, &args)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&record)?);
        return Ok(());
    }

    print_closeout_review_record(&record);
    Ok(())
}

async fn closeout_retire(profile: &str, args: CloseoutRetireArgs) -> Result<()> {
    let json = args.json;
    let record = build_closeout_retire_record(profile, &args)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&record)?);
        return Ok(());
    }

    print_closeout_review_record(&record);
    Ok(())
}

fn closeout_task_matches(task: &OffdeskTask, args: &CloseoutArgs) -> bool {
    option_matches(args.project_key.as_deref(), &task.project_key)
        && option_matches(args.request_id.as_deref(), &task.request_id)
        && option_matches(args.task_id.as_deref(), &task.task_id)
}

fn closeout_probe_matches(probe: &BackgroundProbe, args: &CloseoutArgs) -> bool {
    option_matches(
        args.project_key.as_deref(),
        probe.project_key.as_deref().unwrap_or(""),
    ) && option_matches(
        args.request_id.as_deref(),
        probe.request_id.as_deref().unwrap_or(""),
    ) && option_matches(
        args.task_id.as_deref(),
        probe.task_id.as_deref().unwrap_or(""),
    )
}

fn option_matches(filter: Option<&str>, value: &str) -> bool {
    match filter {
        Some(filter) => filter == value,
        None => true,
    }
}

fn closeout_task_summary(task: &OffdeskTask) -> CloseoutTask {
    let view = task.operator_view();
    let receipt_search_dirs = closeout_receipt_search_dirs_for_task(task);
    CloseoutTask {
        task_id: view.task_id,
        request_id: view.request_id,
        project_key: view.project_key,
        status: view.status,
        capability_id: view.capability_id,
        runner_kind: view.runner_kind,
        workdir: crate::offdesk::operator_safe_text(&view.workdir),
        agent_mode: view.agent_mode,
        background_ticket_id: view.background_ticket_id,
        result_artifact_path: view
            .result_artifact_path
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        log_artifact_path: view
            .log_artifact_path
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        artifact_refs: view.artifact_refs,
        implementation_packet: view.implementation_packet,
        receipt_search_dirs,
        preview: view.preview,
        reason: view.reason,
    }
}

fn closeout_background_summary(probe: &BackgroundProbe) -> CloseoutBackgroundRun {
    let receipt_search_dirs = closeout_receipt_search_dirs_for_background(probe);
    CloseoutBackgroundRun {
        ticket_id: crate::offdesk::operator_safe_text(&probe.ticket_id),
        runner_kind: probe.runner_kind,
        phase: probe.phase,
        project_key: probe
            .project_key
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        request_id: probe
            .request_id
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        task_id: probe
            .task_id
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        working_dir: probe
            .working_dir
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        result_artifact_path: probe
            .result_artifact_path
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        log_artifact_path: probe
            .log_artifact_path
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        implementation_packet: probe
            .implementation_packet
            .as_ref()
            .map(crate::offdesk::operator_safe_implementation_packet_summary),
        runtime_handle_alive: probe.runtime_handle_alive,
        result_artifact_present: probe.result_artifact_present,
        log_artifact_present: probe.log_artifact_present,
        receipt_search_dirs,
    }
}

fn closeout_receipt_search_dirs_for_task(task: &OffdeskTask) -> Vec<String> {
    let mut dirs = BTreeSet::new();
    closeout_add_receipt_search_path(&mut dirs, Some(&task.workdir));
    closeout_add_receipt_search_path(&mut dirs, task.result_artifact_path.as_deref());
    closeout_add_receipt_search_path(&mut dirs, task.log_artifact_path.as_deref());
    if let Some(packet) = task.implementation_packet.as_ref() {
        closeout_add_receipt_search_path(&mut dirs, Some(&packet.artifact_dir));
        closeout_add_receipt_search_path(&mut dirs, Some(&packet.packet_path));
    }
    for artifact in &task.artifact_refs {
        closeout_add_receipt_search_path(&mut dirs, artifact.path.as_deref());
    }
    dirs.into_iter().collect()
}

fn closeout_receipt_search_dirs_for_background(probe: &BackgroundProbe) -> Vec<String> {
    let mut dirs = BTreeSet::new();
    closeout_add_receipt_search_path(&mut dirs, probe.working_dir.as_deref());
    closeout_add_receipt_search_path(&mut dirs, probe.result_artifact_path.as_deref());
    closeout_add_receipt_search_path(&mut dirs, probe.log_artifact_path.as_deref());
    if let Some(packet) = probe.implementation_packet.as_ref() {
        closeout_add_receipt_search_path(&mut dirs, Some(&packet.artifact_dir));
        closeout_add_receipt_search_path(&mut dirs, Some(&packet.packet_path));
    }
    dirs.into_iter().collect()
}

fn closeout_add_receipt_search_path(dirs: &mut BTreeSet<String>, path: Option<&str>) {
    let Some(path) = path.map(str::trim).filter(|path| !path.is_empty()) else {
        return;
    };
    let path = Path::new(path);
    let dir = if path.is_dir() {
        path
    } else {
        path.parent().unwrap_or(path)
    };
    dirs.insert(dir.to_string_lossy().to_string());
}

struct CloseoutFileOperationInput<'a> {
    operation: &'static str,
    path: &'a str,
    destination: Option<String>,
    source: String,
    risk: &'static str,
    reason: &'a str,
    evidence_refs: Vec<String>,
    present: bool,
    requires_commercial_review: bool,
    requires_human_approval: bool,
}

fn closeout_file_operation(input: CloseoutFileOperationInput<'_>) -> CloseoutFileOperation {
    CloseoutFileOperation {
        operation: input.operation,
        path: crate::offdesk::operator_safe_text(input.path),
        destination: input
            .destination
            .map(|value| crate::offdesk::operator_safe_text(&value)),
        source: crate::offdesk::operator_safe_text(&input.source),
        risk: input.risk,
        reason: crate::offdesk::operator_safe_text(input.reason),
        evidence_refs: input
            .evidence_refs
            .into_iter()
            .map(|value| crate::offdesk::operator_safe_text(&value))
            .collect(),
        present: input.present,
        requires_commercial_review: input.requires_commercial_review,
        requires_human_approval: input.requires_human_approval,
    }
}

fn closeout_file_operations(
    tasks: &[OffdeskTask],
    background_runs: &[BackgroundProbe],
) -> Vec<CloseoutFileOperation> {
    let mut operations = Vec::new();

    for task in tasks {
        let evidence = vec![format!("task:{}", task.task_id)];
        if let Some(path) = task.result_artifact_path.as_deref() {
            operations.push(closeout_file_operation(CloseoutFileOperationInput {
                operation: "keep",
                path,
                destination: None,
                source: format!("task:{} result_artifact", task.task_id),
                risk: "low",
                reason: "Result artifacts are provenance anchors for Ondesk return.",
                evidence_refs: evidence.clone(),
                present: path_present(path, None),
                requires_commercial_review: false,
                requires_human_approval: false,
            }));
        }
        if let Some(path) = task.log_artifact_path.as_deref() {
            operations.push(closeout_file_operation(CloseoutFileOperationInput {
                operation: "archive_candidate",
                path,
                destination: archive_destination_for(path),
                source: format!("task:{} log_artifact", task.task_id),
                risk: "medium",
                reason:
                    "Raw logs should be preserved or archived before any deletion is considered.",
                evidence_refs: evidence.clone(),
                present: path_present(path, None),
                requires_commercial_review: true,
                requires_human_approval: true,
            }));
        }
        for artifact in &task.artifact_refs {
            if let Some(path) = artifact.path.as_deref() {
                operations.push(closeout_file_operation(CloseoutFileOperationInput {
                    operation: "keep",
                    path,
                    destination: None,
                    source: format!(
                        "task:{} artifact_ref:{}",
                        task.task_id, artifact.artifact_id
                    ),
                    risk: "low",
                    reason: "Declared task artifacts must remain available for review.",
                    evidence_refs: vec![
                        format!("task:{}", task.task_id),
                        format!("artifact:{}", artifact.artifact_id),
                    ],
                    present: path_present(path, Some(artifact.present)),
                    requires_commercial_review: false,
                    requires_human_approval: false,
                }));
            }
        }
    }

    for probe in background_runs {
        let evidence = vec![format!("background:{}", probe.ticket_id)];
        if let Some(path) = probe.result_artifact_path.as_deref() {
            operations.push(closeout_file_operation(CloseoutFileOperationInput {
                operation: "keep",
                path,
                destination: None,
                source: format!("background:{} result_artifact", probe.ticket_id),
                risk: "low",
                reason: "Background result artifacts are required for morning review.",
                evidence_refs: evidence.clone(),
                present: path_present(path, Some(probe.result_artifact_present)),
                requires_commercial_review: false,
                requires_human_approval: false,
            }));
        }
        if let Some(path) = probe.log_artifact_path.as_deref() {
            operations.push(closeout_file_operation(CloseoutFileOperationInput {
                operation: "archive_candidate",
                path,
                destination: archive_destination_for(path),
                source: format!("background:{} log_artifact", probe.ticket_id),
                risk: "medium",
                reason: "Background logs may be large but should be archived while referenced.",
                evidence_refs: evidence,
                present: path_present(path, Some(probe.log_artifact_present)),
                requires_commercial_review: true,
                requires_human_approval: true,
            }));
        }
    }

    operations
}

fn path_present(path: &str, explicit: Option<bool>) -> bool {
    explicit.unwrap_or(false) || Path::new(path).exists()
}

fn archive_destination_for(path: &str) -> Option<String> {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| format!("archive/{name}"))
}

fn resolve_implementation_packet_context(
    profile_dir: &Path,
    project_key: &str,
    explicit_path: Option<&Path>,
) -> Result<Option<LatestImplementationPacket>> {
    let packet = if let Some(path) = explicit_path {
        Some(implementation_packet_from_path(path).with_context(|| {
            format!(
                "load implementation packet for project {} from {}",
                crate::offdesk::operator_safe_text(project_key),
                crate::offdesk::operator_safe_text(path.to_string_lossy().as_ref())
            )
        })?)
    } else {
        latest_implementation_packet_for_project(profile_dir, Some(project_key))?
    };
    if let Some(packet) = packet.as_ref() {
        if packet.summary.project_key != project_key {
            bail!(
                "implementation packet project_key {} does not match requested project_key {}",
                packet.summary.project_key,
                crate::offdesk::operator_safe_text(project_key)
            );
        }
    }
    Ok(packet)
}

fn attach_implementation_packet_artifact_refs(
    artifact_refs: &mut Vec<CapabilityArtifactRef>,
    packet: Option<&LatestImplementationPacket>,
) {
    let Some(packet) = packet else {
        return;
    };
    push_unique_artifact_ref(artifact_refs, "implementation_packet", &packet.packet_path);
    push_unique_artifact_ref(
        artifact_refs,
        "recursive_alignment_review",
        &packet.alignment_review_path,
    );
    push_unique_artifact_ref(
        artifact_refs,
        "implementation_packet_markdown",
        &packet.markdown_path,
    );
}

fn push_unique_artifact_ref(
    artifact_refs: &mut Vec<CapabilityArtifactRef>,
    artifact_id: &str,
    path: &Path,
) {
    if artifact_refs
        .iter()
        .any(|artifact| artifact.artifact_id == artifact_id)
    {
        return;
    }
    artifact_refs.push(CapabilityArtifactRef::new(
        artifact_id.to_string(),
        Some(path.to_string_lossy().into_owned()),
    ));
}

fn closeout_git_snapshot(
    args: &CloseoutArgs,
    tasks: &[OffdeskTask],
) -> Result<Option<CloseoutGitSnapshot>> {
    let workdir = args
        .workdir
        .clone()
        .or_else(|| tasks.first().map(|task| PathBuf::from(&task.workdir)));
    let Some(workdir) = workdir else {
        return Ok(Some(CloseoutGitSnapshot {
            workdir: "-".to_string(),
            status_short: None,
            diff_stat: None,
            error: Some("no workdir supplied and no matched task workdir found".to_string()),
        }));
    };
    let workdir_label = crate::offdesk::operator_safe_text(workdir.to_string_lossy().as_ref());
    if !workdir.exists() {
        return Ok(Some(CloseoutGitSnapshot {
            workdir: workdir_label,
            status_short: None,
            diff_stat: None,
            error: Some("workdir does not exist".to_string()),
        }));
    }
    Ok(Some(CloseoutGitSnapshot {
        workdir: workdir_label,
        status_short: closeout_git_output(&workdir, &["status", "--short"])?,
        diff_stat: closeout_git_output(&workdir, &["diff", "--stat"])?,
        error: None,
    }))
}

fn closeout_source_observation(
    args: &CloseoutArgs,
    tasks: &[CloseoutTask],
    background_runs: &[CloseoutBackgroundRun],
    generated_at: DateTime<Utc>,
) -> CloseoutSourceObservation {
    let artifact_refs = closeout_source_observation_artifact_refs(tasks, background_runs);
    if !args.include_git {
        return CloseoutSourceObservation {
            schema: "source_observation.v1",
            generated_at,
            source_kind: "git_worktree",
            enabled: false,
            available: false,
            status: "not_requested",
            workdir: None,
            base_ref: CLOSEOUT_SOURCE_OBSERVATION_BASE_REF,
            changed_file_count: 0,
            changed_files_truncated: false,
            changed_files: Vec::new(),
            artifact_refs,
            warnings: vec![
                "Run closeout with --include-git to attach read-only source observation."
                    .to_string(),
            ],
        };
    }

    let workdir = args
        .workdir
        .clone()
        .or_else(|| closeout_project_workdir_from_closeout_task_artifacts(tasks))
        .or_else(|| tasks.first().map(|task| PathBuf::from(&task.workdir)));
    let Some(workdir) = workdir else {
        return CloseoutSourceObservation {
            schema: "source_observation.v1",
            generated_at,
            source_kind: "git_worktree",
            enabled: true,
            available: false,
            status: "unavailable",
            workdir: None,
            base_ref: CLOSEOUT_SOURCE_OBSERVATION_BASE_REF,
            changed_file_count: 0,
            changed_files_truncated: false,
            changed_files: Vec::new(),
            artifact_refs,
            warnings: vec![
                "No workdir was supplied and no matched task workdir could be inferred."
                    .to_string(),
            ],
        };
    };
    let workdir_label = crate::offdesk::operator_safe_text(workdir.to_string_lossy().as_ref());
    if !workdir.exists() {
        return CloseoutSourceObservation {
            schema: "source_observation.v1",
            generated_at,
            source_kind: "git_worktree",
            enabled: true,
            available: false,
            status: "unavailable",
            workdir: Some(workdir_label),
            base_ref: CLOSEOUT_SOURCE_OBSERVATION_BASE_REF,
            changed_file_count: 0,
            changed_files_truncated: false,
            changed_files: Vec::new(),
            artifact_refs,
            warnings: vec!["Workdir does not exist.".to_string()],
        };
    }

    let mut warnings = Vec::new();
    let changed_files = match crate::git::diff::compute_changed_files(
        &workdir,
        CLOSEOUT_SOURCE_OBSERVATION_BASE_REF,
    ) {
        Ok(files) => files,
        Err(error) => {
            warnings.push(format!(
                "Changed-file observation failed: {}",
                crate::offdesk::operator_safe_text(&error.to_string())
            ));
            Vec::new()
        }
    };
    let available = warnings.is_empty();
    let changed_file_count = changed_files.len();
    let changed_files_truncated =
        changed_file_count > CLOSEOUT_SOURCE_OBSERVATION_CHANGED_FILE_LIMIT;
    let changed_files = changed_files
        .into_iter()
        .take(CLOSEOUT_SOURCE_OBSERVATION_CHANGED_FILE_LIMIT)
        .map(|file| CloseoutSourceChangedFile {
            path: crate::offdesk::operator_safe_text(file.path.to_string_lossy().as_ref()),
            old_path: file
                .old_path
                .as_ref()
                .map(|path| crate::offdesk::operator_safe_text(path.to_string_lossy().as_ref())),
            status: file.status.label(),
            additions: file.additions,
            deletions: file.deletions,
        })
        .collect::<Vec<_>>();
    let status = if !available {
        "unavailable"
    } else if changed_file_count > 0 {
        "observed"
    } else {
        "clean"
    };

    CloseoutSourceObservation {
        schema: "source_observation.v1",
        generated_at,
        source_kind: "git_worktree",
        enabled: true,
        available,
        status,
        workdir: Some(workdir_label),
        base_ref: CLOSEOUT_SOURCE_OBSERVATION_BASE_REF,
        changed_file_count,
        changed_files_truncated,
        changed_files,
        artifact_refs,
        warnings,
    }
}

fn closeout_source_observation_artifact_refs(
    tasks: &[CloseoutTask],
    background_runs: &[CloseoutBackgroundRun],
) -> Vec<String> {
    let mut refs = BTreeSet::new();
    for task in tasks {
        closeout_source_add_artifact_ref(&mut refs, task.result_artifact_path.as_deref());
        closeout_source_add_artifact_ref(&mut refs, task.log_artifact_path.as_deref());
        for artifact in &task.artifact_refs {
            closeout_source_add_artifact_ref(&mut refs, artifact.path.as_deref());
        }
    }
    for run in background_runs {
        closeout_source_add_artifact_ref(&mut refs, run.result_artifact_path.as_deref());
        closeout_source_add_artifact_ref(&mut refs, run.log_artifact_path.as_deref());
    }
    refs.into_iter().take(20).collect()
}

fn closeout_project_workdir_from_closeout_task_artifacts(
    tasks: &[CloseoutTask],
) -> Option<PathBuf> {
    tasks.iter().find_map(|task| {
        task.result_artifact_path
            .as_deref()
            .and_then(closeout_project_workdir_from_artifact_path)
            .or_else(|| {
                task.log_artifact_path
                    .as_deref()
                    .and_then(closeout_project_workdir_from_artifact_path)
            })
            .or_else(|| {
                task.artifact_refs.iter().find_map(|artifact| {
                    artifact
                        .path
                        .as_deref()
                        .and_then(closeout_project_workdir_from_artifact_path)
                })
            })
    })
}

fn closeout_source_add_artifact_ref(refs: &mut BTreeSet<String>, path: Option<&str>) {
    let Some(path) = path.map(str::trim).filter(|path| !path.is_empty()) else {
        return;
    };
    refs.insert(crate::offdesk::operator_safe_text(path));
}

fn closeout_source_observation_refs(observation: &CloseoutSourceObservation) -> Vec<String> {
    observation
        .changed_files
        .iter()
        .take(CLOSEOUT_SOURCE_OBSERVATION_REF_LIMIT)
        .map(|file| format!("source:git:{}:{}", file.status, file.path))
        .collect()
}

fn closeout_git_output(workdir: &Path, args: &[&str]) -> Result<Option<String>> {
    let output = Command::new("git")
        .args(args)
        .current_dir(workdir)
        .output()?;
    let raw = if output.status.success() {
        String::from_utf8_lossy(&output.stdout).to_string()
    } else {
        String::from_utf8_lossy(&output.stderr).to_string()
    };
    let safe = crate::offdesk::operator_safe_text(raw.trim());
    if safe.is_empty() {
        Ok(None)
    } else {
        Ok(Some(truncate_closeout_text(&safe, 12_000)))
    }
}

fn closeout_decision_records(
    profile_dir: &Path,
    tasks: &[OffdeskTask],
    background_runs: &[BackgroundProbe],
    args: &CloseoutArgs,
) -> Result<Vec<CloseoutDecisionRecord>> {
    let mut roots = BTreeSet::new();
    roots.insert(profile_dir.to_path_buf());
    for task in tasks {
        closeout_add_decision_root(&mut roots, Some(task.workdir.as_str()));
        closeout_add_decision_root(&mut roots, task.log_artifact_path.as_deref());
        closeout_add_decision_root(&mut roots, task.result_artifact_path.as_deref());
        for artifact in &task.artifact_refs {
            closeout_add_decision_root(&mut roots, artifact.path.as_deref());
        }
    }
    for probe in background_runs {
        closeout_add_decision_root(&mut roots, probe.working_dir.as_deref());
        closeout_add_decision_root(&mut roots, probe.log_artifact_path.as_deref());
        closeout_add_decision_root(&mut roots, probe.result_artifact_path.as_deref());
    }

    let mut by_decision_id = BTreeMap::<String, CloseoutDecisionRecord>::new();
    for root in roots {
        let ledger = DecisionLedger::new(&root);
        let source_path = ledger.path();
        if !source_path.exists() {
            continue;
        }
        for record in ledger
            .load()
            .with_context(|| format!("read closeout decision ledger {}", source_path.display()))?
        {
            if !closeout_decision_record_matches(&record, args) {
                continue;
            }
            let candidate =
                closeout_decision_record_from_source(source_path.display().to_string(), record);
            let decision_id = candidate.record.decision_id.clone();
            let replace = by_decision_id
                .get(&decision_id)
                .map(|existing| existing.record.updated_at < candidate.record.updated_at)
                .unwrap_or(true);
            if replace {
                by_decision_id.insert(decision_id, candidate);
            }
        }
    }

    let mut records = by_decision_id.into_values().collect::<Vec<_>>();
    records.sort_by(|left, right| {
        left.record
            .updated_at
            .cmp(&right.record.updated_at)
            .then_with(|| left.record.decision_id.cmp(&right.record.decision_id))
    });
    Ok(records)
}

fn closeout_add_decision_root(roots: &mut BTreeSet<PathBuf>, value: Option<&str>) {
    let Some(raw) = value else {
        return;
    };
    let text = raw.trim();
    if text.is_empty() {
        return;
    }
    let path = PathBuf::from(text);
    if path.is_dir() {
        roots.insert(path);
    } else if let Some(parent) = path.parent() {
        roots.insert(parent.to_path_buf());
    }
}

fn closeout_decision_record_matches(record: &DecisionRecord, args: &CloseoutArgs) -> bool {
    if let Some(project_key) = args.project_key.as_deref() {
        if record.project_key != project_key {
            return false;
        }
    }
    if let Some(request_id) = args.request_id.as_deref() {
        if record.request_id != request_id {
            return false;
        }
    }
    if let Some(task_id) = args.task_id.as_deref() {
        if record.task_id != task_id {
            return false;
        }
    }
    true
}

fn closeout_decision_record_from_source(
    source_path: String,
    record: DecisionRecord,
) -> CloseoutDecisionRecord {
    let validation_issues = record.validation_issues();
    CloseoutDecisionRecord {
        source_path,
        record,
        validation_issues,
    }
}

fn closeout_decision_record_is_open(decision: &CloseoutDecisionRecord) -> bool {
    if !decision.validation_issues.is_empty() {
        return true;
    }
    match decision.record.status {
        DecisionStatus::AutoResolved | DecisionStatus::Denied | DecisionStatus::Receipted => false,
        DecisionStatus::Applied => decision.record.decision_receipt.is_none(),
        DecisionStatus::Draft
        | DecisionStatus::CouncilReview
        | DecisionStatus::UserPending
        | DecisionStatus::Approved
        | DecisionStatus::Revised
        | DecisionStatus::Deferred
        | DecisionStatus::HandoffReady => true,
    }
}

fn closeout_decision_record_subject(record: &DecisionRecord) -> &str {
    record
        .approval_brief
        .as_ref()
        .map(|brief| brief.subject.as_str())
        .filter(|subject| !subject.trim().is_empty())
        .unwrap_or(record.decision_request.summary.as_str())
}

fn closeout_implementation_packet_coverage(
    tasks: &[CloseoutTask],
    background_runs: &[CloseoutBackgroundRun],
    source_observation: &CloseoutSourceObservation,
) -> CloseoutImplementationPacketCoverage {
    let mut packets = BTreeMap::<String, CloseoutPacketAggregate>::new();
    let source_refs = closeout_source_observation_refs(source_observation);
    for task in tasks {
        let Some(summary) = task.implementation_packet.as_ref() else {
            continue;
        };
        let task_id = crate::offdesk::operator_safe_text(&task.task_id);
        let entry = closeout_packet_entry(
            &mut packets,
            summary,
            source_observation.status,
            &source_refs,
        );
        entry.task_ids.insert(task.task_id.clone());
        if let Some(ticket_id) = task.background_ticket_id.as_deref() {
            entry.background_ticket_ids.insert(ticket_id.to_string());
        }
        entry
            .receipt_search_dirs
            .extend(task.receipt_search_dirs.iter().cloned());
        entry.evidence_refs.insert(format!(
            "task:{task_id}:status:{}",
            closeout_task_status_label(task.status)
        ));
        if task.result_artifact_path.is_some() {
            entry
                .evidence_refs
                .insert(format!("task:{task_id}:result_artifact"));
        }
        if let Some(path) = task.result_artifact_path.as_deref() {
            closeout_packet_add_match_ref(
                entry,
                path,
                &format!("task:{task_id}:result:{}", closeout_path_tail(path)),
            );
        }
        if task.log_artifact_path.is_some() {
            entry
                .evidence_refs
                .insert(format!("task:{task_id}:log_artifact"));
        }
        if let Some(path) = task.log_artifact_path.as_deref() {
            closeout_packet_add_match_ref(
                entry,
                path,
                &format!("task:{task_id}:log:{}", closeout_path_tail(path)),
            );
        }
        for artifact in &task.artifact_refs {
            closeout_packet_add_match_ref(
                entry,
                &artifact.artifact_id,
                &format!("task:{task_id}:artifact:{}", artifact.artifact_id),
            );
            if let Some(path) = artifact.path.as_deref() {
                closeout_packet_add_match_ref(
                    entry,
                    path,
                    &format!(
                        "task:{task_id}:artifact:{}:{}",
                        artifact.artifact_id,
                        closeout_path_tail(path)
                    ),
                );
            }
        }
        match task.status {
            OffdeskTaskStatus::Completed => entry.has_completed_evidence = true,
            OffdeskTaskStatus::Failed | OffdeskTaskStatus::Cancelled => {
                entry.has_failed_evidence = true
            }
            OffdeskTaskStatus::Queued
            | OffdeskTaskStatus::PendingApproval
            | OffdeskTaskStatus::Launched
            | OffdeskTaskStatus::Running
            | OffdeskTaskStatus::ResumePending => entry.has_active_evidence = true,
        }
    }

    for run in background_runs {
        let Some(summary) = run.implementation_packet.as_ref() else {
            continue;
        };
        let ticket_id = crate::offdesk::operator_safe_text(&run.ticket_id);
        let entry = closeout_packet_entry(
            &mut packets,
            summary,
            source_observation.status,
            &source_refs,
        );
        entry.background_ticket_ids.insert(run.ticket_id.clone());
        if let Some(task_id) = run.task_id.as_deref() {
            entry.task_ids.insert(task_id.to_string());
        }
        entry
            .receipt_search_dirs
            .extend(run.receipt_search_dirs.iter().cloned());
        entry.evidence_refs.insert(format!(
            "background:{ticket_id}:phase:{}",
            closeout_background_phase_label(run.phase)
        ));
        if run.result_artifact_present {
            entry
                .evidence_refs
                .insert(format!("background:{ticket_id}:result_artifact"));
        }
        if let Some(path) = run.result_artifact_path.as_deref() {
            closeout_packet_add_match_ref(
                entry,
                path,
                &format!("background:{ticket_id}:result:{}", closeout_path_tail(path)),
            );
        }
        if run.log_artifact_present {
            entry
                .evidence_refs
                .insert(format!("background:{ticket_id}:log_artifact"));
        }
        if let Some(path) = run.log_artifact_path.as_deref() {
            closeout_packet_add_match_ref(
                entry,
                path,
                &format!("background:{ticket_id}:log:{}", closeout_path_tail(path)),
            );
        }
        match run.phase {
            BackgroundRunnerPhase::Completed | BackgroundRunnerPhase::ResultReceived => {
                entry.has_completed_evidence = true
            }
            BackgroundRunnerPhase::Failed
            | BackgroundRunnerPhase::StaleNoAck
            | BackgroundRunnerPhase::StaleLostCallback
            | BackgroundRunnerPhase::Reconstructable
            | BackgroundRunnerPhase::RecoveryAcknowledged => entry.has_failed_evidence = true,
            BackgroundRunnerPhase::Launched
            | BackgroundRunnerPhase::HandoffEmitted
            | BackgroundRunnerPhase::PickupAcknowledged => entry.has_active_evidence = true,
        }
    }

    let mut coverage_inputs = Vec::new();
    for aggregate in packets.into_values() {
        let goal_coverage = assess_implementation_packet_goal(
            &aggregate.summary,
            ImplementationPacketExecutionEvidence {
                has_completed: aggregate.has_completed_evidence,
                has_active: aggregate.has_active_evidence,
                has_failed: aggregate.has_failed_evidence,
            },
        );
        let details = closeout_packet_detail_coverage(&aggregate, goal_coverage.status);
        coverage_inputs.push(CloseoutImplementationPacketCoverageInput {
            summary: aggregate.summary,
            goal_coverage,
            evidence_refs: aggregate.evidence_refs.into_iter().collect(),
            detail_source: details.detail_source,
            detail_error: details.detail_error,
            work_slices: details.work_slices,
            validation_items: details.validation_items,
            expected_artifacts: details.expected_artifacts,
        });
    }
    build_closeout_implementation_packet_coverage(coverage_inputs)
}

fn closeout_packet_entry<'a>(
    packets: &'a mut BTreeMap<String, CloseoutPacketAggregate>,
    summary: &ImplementationPacketSummary,
    source_observation_status: &'static str,
    source_refs: &[String],
) -> &'a mut CloseoutPacketAggregate {
    let summary = crate::offdesk::operator_safe_implementation_packet_summary(summary);
    let key = closeout_packet_key(&summary);
    packets
        .entry(key)
        .or_insert_with(|| CloseoutPacketAggregate {
            receipt_search_dirs: closeout_packet_summary_receipt_search_dirs(&summary),
            summary,
            evidence_refs: BTreeSet::new(),
            match_refs: BTreeMap::new(),
            source_observation_status,
            source_refs: source_refs.to_vec(),
            task_ids: BTreeSet::new(),
            background_ticket_ids: BTreeSet::new(),
            has_completed_evidence: false,
            has_active_evidence: false,
            has_failed_evidence: false,
        })
}

fn closeout_packet_summary_receipt_search_dirs(
    summary: &ImplementationPacketSummary,
) -> BTreeSet<String> {
    let mut dirs = BTreeSet::new();
    closeout_add_receipt_search_path(&mut dirs, Some(&summary.artifact_dir));
    closeout_add_receipt_search_path(&mut dirs, Some(&summary.packet_path));
    dirs
}

fn closeout_packet_key(summary: &ImplementationPacketSummary) -> String {
    let packet_id = summary.packet_id.trim();
    if !packet_id.is_empty() {
        return packet_id.to_string();
    }
    let packet_path = summary.packet_path.trim();
    if !packet_path.is_empty() {
        return packet_path.to_string();
    }
    format!("{}:{}", summary.project_key, summary.created_at)
}

fn closeout_packet_add_match_ref(
    aggregate: &mut CloseoutPacketAggregate,
    candidate: &str,
    evidence_ref: &str,
) {
    let candidate = candidate.trim();
    if candidate.is_empty() {
        return;
    }
    aggregate.match_refs.insert(
        closeout_match_text(candidate),
        crate::offdesk::operator_safe_text(evidence_ref),
    );
}

fn closeout_packet_detail_coverage(
    aggregate: &CloseoutPacketAggregate,
    packet_status: ImplementationPacketCoverageStatus,
) -> CloseoutPacketDetailGroups {
    let packet_path = aggregate.summary.packet_path.trim();
    if packet_path.is_empty() {
        return CloseoutPacketDetailGroups {
            detail_source: "summary_only",
            detail_error: Some("implementation packet path is unavailable".to_string()),
            work_slices: closeout_summary_only_details(
                "work_slice",
                aggregate.summary.work_slice_count,
                packet_status,
                aggregate,
            ),
            validation_items: closeout_summary_only_details(
                "validation",
                aggregate.summary.validation_item_count,
                packet_status,
                aggregate,
            ),
            expected_artifacts: closeout_summary_only_details(
                "expected_artifact",
                aggregate.summary.expected_artifact_count,
                packet_status,
                aggregate,
            ),
        };
    }

    match implementation_packet_record_from_path(Path::new(packet_path)) {
        Ok(packet) => {
            let (work_slice_receipts, receipt_error) = closeout_load_work_slice_receipts(aggregate);
            let detail_source = if work_slice_receipts.is_empty() {
                "implementation_packet"
            } else {
                "implementation_packet_and_work_slice_receipts"
            };
            CloseoutPacketDetailGroups {
                detail_source,
                detail_error: receipt_error,
                work_slices: closeout_work_slice_details(
                    &packet.design.work_slices,
                    packet_status,
                    &work_slice_receipts,
                    aggregate,
                ),
                validation_items: closeout_validation_item_details(
                    &packet,
                    aggregate,
                    packet_status,
                ),
                expected_artifacts: closeout_expected_artifact_details(
                    &packet.closeout.expected_artifacts,
                    aggregate,
                    packet_status,
                ),
            }
        }
        Err(error) => CloseoutPacketDetailGroups {
            detail_source: "summary_only",
            detail_error: Some(crate::offdesk::operator_safe_text(&error.to_string())),
            work_slices: closeout_summary_only_details(
                "work_slice",
                aggregate.summary.work_slice_count,
                packet_status,
                aggregate,
            ),
            validation_items: closeout_summary_only_details(
                "validation",
                aggregate.summary.validation_item_count,
                packet_status,
                aggregate,
            ),
            expected_artifacts: closeout_summary_only_details(
                "expected_artifact",
                aggregate.summary.expected_artifact_count,
                packet_status,
                aggregate,
            ),
        },
    }
}

fn closeout_work_slice_details(
    work_slices: &[String],
    packet_status: ImplementationPacketCoverageStatus,
    receipts: &[LoadedWorkSliceExecutionReceipt],
    aggregate: &CloseoutPacketAggregate,
) -> Vec<CloseoutPacketCoverageDetail> {
    work_slices
        .iter()
        .enumerate()
        .map(|(index, slice)| {
            if let Some(receipt) = closeout_work_slice_receipt_for(receipts, index, slice) {
                return closeout_work_slice_detail_from_receipt(slice, receipt, aggregate);
            }
            CloseoutPacketCoverageDetail {
                category: "work_slice",
                label: crate::offdesk::operator_safe_text(slice),
                status: packet_status.as_str(),
                reason: "Work-slice execution evidence is not itemized yet; this item inherits the packet-level closeout status and needs manual review.".to_string(),
                evidence_refs: Vec::new(),
                receipt_source: None,
                receipt_role: None,
                trust_tier: None,
                reported_status: None,
                claim_status: None,
                verification_status: None,
                verification_summary: None,
                verification_refs: Vec::new(),
                source_observation_status: Some(aggregate.source_observation_status),
                source_refs: aggregate.source_refs.clone(),
                summary: None,
                validation_refs: Vec::new(),
                artifact_refs: Vec::new(),
                open_questions: Vec::new(),
                drift_signals: Vec::new(),
                next_safe_action: None,
            }
        })
        .collect()
}

fn closeout_load_work_slice_receipts(
    aggregate: &CloseoutPacketAggregate,
) -> (Vec<LoadedWorkSliceExecutionReceipt>, Option<String>) {
    let mut receipts = Vec::new();
    let mut errors = Vec::new();
    for dir in &aggregate.receipt_search_dirs {
        let path = Path::new(dir).join(WORK_SLICE_EXECUTION_RECEIPTS_FILE);
        match work_slice_execution_receipts_from_path(&path) {
            Ok(records) => {
                for receipt in records {
                    if closeout_work_slice_receipt_matches(aggregate, &receipt) {
                        receipts.push(LoadedWorkSliceExecutionReceipt {
                            receipt,
                            source: crate::offdesk::operator_safe_text(
                                path.to_string_lossy().as_ref(),
                            ),
                        });
                    }
                }
            }
            Err(error) => errors.push(crate::offdesk::operator_safe_text(&error.to_string())),
        }
    }
    let error = if errors.is_empty() {
        None
    } else {
        Some(errors.into_iter().take(3).collect::<Vec<_>>().join("; "))
    };
    (receipts, error)
}

fn closeout_work_slice_receipt_matches(
    aggregate: &CloseoutPacketAggregate,
    receipt: &WorkSliceExecutionReceipt,
) -> bool {
    if !closeout_optional_text_matches(&receipt.packet_id, &aggregate.summary.packet_id) {
        return false;
    }
    if !receipt.project_key.trim().is_empty()
        && !closeout_optional_text_matches(&receipt.project_key, &aggregate.summary.project_key)
    {
        return false;
    }
    if let Some(task_id) = receipt
        .task_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
    {
        if !aggregate.task_ids.is_empty()
            && !aggregate
                .task_ids
                .iter()
                .any(|known| closeout_optional_text_matches(task_id, known))
        {
            return false;
        }
    }
    if let Some(ticket_id) = receipt
        .background_ticket_id
        .as_deref()
        .map(str::trim)
        .filter(|id| !id.is_empty())
    {
        if !aggregate.background_ticket_ids.is_empty()
            && !aggregate
                .background_ticket_ids
                .iter()
                .any(|known| closeout_optional_text_matches(ticket_id, known))
        {
            return false;
        }
    }
    true
}

fn closeout_optional_text_matches(left: &str, right: &str) -> bool {
    let left = left.trim();
    let right = right.trim();
    if left.is_empty() || right.is_empty() {
        return true;
    }
    left == right || closeout_match_text(left) == closeout_match_text(right)
}

fn closeout_work_slice_receipt_for<'a>(
    receipts: &'a [LoadedWorkSliceExecutionReceipt],
    slice_index: usize,
    slice_label: &str,
) -> Option<&'a LoadedWorkSliceExecutionReceipt> {
    let normalized_label = closeout_match_text(slice_label);
    receipts
        .iter()
        .find(|loaded| closeout_match_text(&loaded.receipt.slice_label) == normalized_label)
        .or_else(|| {
            receipts
                .iter()
                .find(|loaded| loaded.receipt.slice_index == Some(slice_index))
        })
        .or_else(|| {
            receipts.iter().find(|loaded| {
                loaded.receipt.slice_id.as_deref().is_some_and(|slice_id| {
                    let slice_id = closeout_match_text(slice_id);
                    slice_id == format!("slice-{}", slice_index)
                        || slice_id == format!("slice-{}", slice_index + 1)
                        || slice_id == format!("slice_{}", slice_index)
                        || slice_id == format!("slice_{}", slice_index + 1)
                })
            })
        })
}

fn closeout_work_slice_detail_from_receipt(
    packet_slice_label: &str,
    loaded: &LoadedWorkSliceExecutionReceipt,
    aggregate: &CloseoutPacketAggregate,
) -> CloseoutPacketCoverageDetail {
    let receipt = &loaded.receipt;
    let coverage = assess_work_slice_receipt(receipt);
    let role = coverage.role;
    let reported_status = receipt.status.as_str();
    let status = coverage.effective_status.as_str();
    let trust_tier = coverage.trust_tier.as_str();
    let summary = crate::offdesk::operator_safe_text(&receipt.summary);
    let mut reason = if summary.is_empty() {
        format!("{} reports `{reported_status}`.", coverage.role_label())
    } else {
        format!(
            "{} reports `{reported_status}`: {summary}",
            coverage.role_label()
        )
    };
    if status != reported_status {
        reason.push_str(" Closeout keeps this slice deferred until independent source or review verification reconciles the claim.");
    }
    let verification_summary = crate::offdesk::operator_safe_text(&receipt.verification_summary);
    CloseoutPacketCoverageDetail {
        category: "work_slice",
        label: crate::offdesk::operator_safe_text(packet_slice_label),
        status,
        reason,
        evidence_refs: receipt
            .evidence_refs
            .iter()
            .map(|value| crate::offdesk::operator_safe_text(value))
            .collect(),
        receipt_source: Some(loaded.source.clone()),
        receipt_role: Some(role.as_str()),
        trust_tier: Some(trust_tier),
        reported_status: (status != reported_status).then_some(reported_status),
        claim_status: receipt
            .resolved_claim_status()
            .map(WorkSliceExecutionStatus::as_str),
        verification_status: Some(receipt.verification_status.as_str()),
        verification_summary: if verification_summary.is_empty() {
            None
        } else {
            Some(verification_summary)
        },
        verification_refs: receipt
            .verification_refs
            .iter()
            .map(|value| crate::offdesk::operator_safe_text(value))
            .collect(),
        source_observation_status: Some(aggregate.source_observation_status),
        source_refs: aggregate.source_refs.clone(),
        summary: if summary.is_empty() {
            None
        } else {
            Some(summary)
        },
        validation_refs: receipt
            .validation_refs
            .iter()
            .map(|value| crate::offdesk::operator_safe_text(value))
            .collect(),
        artifact_refs: receipt
            .artifact_refs
            .iter()
            .map(|value| crate::offdesk::operator_safe_text(value))
            .collect(),
        open_questions: receipt
            .open_questions
            .iter()
            .map(|value| crate::offdesk::operator_safe_text(value))
            .collect(),
        drift_signals: receipt
            .drift_signals
            .iter()
            .map(|value| crate::offdesk::operator_safe_text(value))
            .collect(),
        next_safe_action: if receipt.next_safe_action.trim().is_empty() {
            None
        } else {
            Some(crate::offdesk::operator_safe_text(
                &receipt.next_safe_action,
            ))
        },
    }
}

fn closeout_validation_item_details(
    packet: &ImplementationPacket,
    aggregate: &CloseoutPacketAggregate,
    packet_status: ImplementationPacketCoverageStatus,
) -> Vec<CloseoutPacketCoverageDetail> {
    let mut items = Vec::new();
    closeout_push_validation_details(
        &mut items,
        "validation_test",
        &packet.validation.tests,
        aggregate,
        packet_status,
    );
    closeout_push_validation_details(
        &mut items,
        "smoke_check",
        &packet.validation.smoke_checks,
        aggregate,
        packet_status,
    );
    closeout_push_validation_details(
        &mut items,
        "manual_review",
        &packet.validation.manual_review,
        aggregate,
        packet_status,
    );
    closeout_push_validation_details(
        &mut items,
        "evidence_required",
        &packet.validation.evidence_required,
        aggregate,
        packet_status,
    );
    items
}

fn closeout_push_validation_details(
    items: &mut Vec<CloseoutPacketCoverageDetail>,
    category: &'static str,
    labels: &[String],
    aggregate: &CloseoutPacketAggregate,
    packet_status: ImplementationPacketCoverageStatus,
) {
    for label in labels {
        let evidence_refs = closeout_packet_matching_refs(aggregate, label);
        let detail_coverage =
            assess_implementation_packet_detail(packet_status, !evidence_refs.is_empty());
        items.push(CloseoutPacketCoverageDetail {
            category,
            label: crate::offdesk::operator_safe_text(label),
            status: detail_coverage.status.as_str(),
            reason: detail_coverage.reason.to_string(),
            evidence_refs,
            receipt_source: None,
            receipt_role: None,
            trust_tier: None,
            reported_status: None,
            claim_status: None,
            verification_status: None,
            verification_summary: None,
            verification_refs: Vec::new(),
            source_observation_status: None,
            source_refs: Vec::new(),
            summary: None,
            validation_refs: Vec::new(),
            artifact_refs: Vec::new(),
            open_questions: Vec::new(),
            drift_signals: Vec::new(),
            next_safe_action: None,
        });
    }
}

fn closeout_expected_artifact_details(
    expected_artifacts: &[String],
    aggregate: &CloseoutPacketAggregate,
    packet_status: ImplementationPacketCoverageStatus,
) -> Vec<CloseoutPacketCoverageDetail> {
    expected_artifacts
        .iter()
        .map(|artifact| {
            let evidence_refs = closeout_packet_matching_refs(aggregate, artifact);
            let detail_coverage =
                assess_implementation_packet_detail(packet_status, !evidence_refs.is_empty());
            CloseoutPacketCoverageDetail {
                category: "expected_artifact",
                label: crate::offdesk::operator_safe_text(artifact),
                status: detail_coverage.status.as_str(),
                reason: detail_coverage.reason.to_string(),
                evidence_refs,
                receipt_source: None,
                receipt_role: None,
                trust_tier: None,
                reported_status: None,
                claim_status: None,
                verification_status: None,
                verification_summary: None,
                verification_refs: Vec::new(),
                source_observation_status: None,
                source_refs: Vec::new(),
                summary: None,
                validation_refs: Vec::new(),
                artifact_refs: Vec::new(),
                open_questions: Vec::new(),
                drift_signals: Vec::new(),
                next_safe_action: None,
            }
        })
        .collect()
}

fn closeout_summary_only_details(
    category: &'static str,
    count: usize,
    packet_status: ImplementationPacketCoverageStatus,
    aggregate: &CloseoutPacketAggregate,
) -> Vec<CloseoutPacketCoverageDetail> {
    (0..count)
        .map(|index| CloseoutPacketCoverageDetail {
            category,
            label: format!("{category}_{}", index + 1),
            status: packet_status.as_str(),
            reason: "Only the packet summary was available, so item text could not be inspected."
                .to_string(),
            evidence_refs: Vec::new(),
            receipt_source: None,
            receipt_role: None,
            trust_tier: None,
            reported_status: None,
            claim_status: None,
            verification_status: None,
            verification_summary: None,
            verification_refs: Vec::new(),
            source_observation_status: (category == "work_slice")
                .then_some(aggregate.source_observation_status),
            source_refs: if category == "work_slice" {
                aggregate.source_refs.clone()
            } else {
                Vec::new()
            },
            summary: None,
            validation_refs: Vec::new(),
            artifact_refs: Vec::new(),
            open_questions: Vec::new(),
            drift_signals: Vec::new(),
            next_safe_action: None,
        })
        .collect()
}

fn closeout_packet_matching_refs(
    aggregate: &CloseoutPacketAggregate,
    requirement: &str,
) -> Vec<String> {
    let requirement = closeout_match_text(requirement);
    if requirement.is_empty() {
        return Vec::new();
    }
    aggregate
        .match_refs
        .iter()
        .filter(|(candidate, _)| {
            let basename = closeout_match_basename(candidate);
            candidate.contains(&requirement)
                || requirement.contains(candidate.as_str())
                || (!basename.is_empty()
                    && (basename.contains(&requirement) || requirement.contains(&basename)))
        })
        .map(|(_, evidence)| evidence.clone())
        .take(5)
        .collect()
}

fn closeout_match_text(value: &str) -> String {
    let mut out = String::new();
    let mut last_space = false;
    for ch in value.chars().flat_map(char::to_lowercase) {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '/' | '\\') {
            out.push(ch);
            last_space = false;
        } else if !last_space {
            out.push(' ');
            last_space = true;
        }
    }
    out.trim().to_string()
}

fn closeout_match_basename(value: &str) -> String {
    value
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(value)
        .trim()
        .to_string()
}

fn closeout_path_tail(path: &str) -> String {
    crate::offdesk::operator_safe_text(
        Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(path),
    )
}

fn closeout_task_status_label(status: OffdeskTaskStatus) -> &'static str {
    match status {
        OffdeskTaskStatus::Queued => "queued",
        OffdeskTaskStatus::PendingApproval => "pending_approval",
        OffdeskTaskStatus::Launched => "launched",
        OffdeskTaskStatus::Running => "running",
        OffdeskTaskStatus::Completed => "completed",
        OffdeskTaskStatus::Failed => "failed",
        OffdeskTaskStatus::ResumePending => "resume_pending",
        OffdeskTaskStatus::Cancelled => "cancelled",
    }
}

fn closeout_background_phase_label(phase: BackgroundRunnerPhase) -> &'static str {
    match phase {
        BackgroundRunnerPhase::Launched => "launched",
        BackgroundRunnerPhase::HandoffEmitted => "handoff_emitted",
        BackgroundRunnerPhase::PickupAcknowledged => "pickup_acknowledged",
        BackgroundRunnerPhase::ResultReceived => "result_received",
        BackgroundRunnerPhase::Completed => "completed",
        BackgroundRunnerPhase::Failed => "failed",
        BackgroundRunnerPhase::StaleNoAck => "stale_no_ack",
        BackgroundRunnerPhase::StaleLostCallback => "stale_lost_callback",
        BackgroundRunnerPhase::Reconstructable => "reconstructable",
        BackgroundRunnerPhase::RecoveryAcknowledged => "recovery_acknowledged",
    }
}

fn closeout_open_decisions(
    tasks: &[OffdeskTask],
    operations: &[CloseoutFileOperation],
    decision_records: &[CloseoutDecisionRecord],
    git_snapshot: Option<&CloseoutGitSnapshot>,
    args: &CloseoutArgs,
    documentation_governance: Option<&CloseoutDocumentationGovernance>,
    implementation_packet_coverage: &CloseoutImplementationPacketCoverage,
) -> Vec<CloseoutDecision> {
    let mut decisions = Vec::new();
    let active_or_blocked = tasks
        .iter()
        .filter(|task| {
            !matches!(
                task.status,
                OffdeskTaskStatus::Completed | OffdeskTaskStatus::Cancelled
            )
        })
        .count();
    if active_or_blocked > 0 {
        decisions.push(CloseoutDecision {
            kind: "non_terminal_task",
            detail: format!("{active_or_blocked} matched tasks are not terminal yet."),
            suggested_command: "forager offdesk tasks --json".to_string(),
        });
    }
    let missing = operations
        .iter()
        .filter(|operation| !operation.present)
        .count();
    if missing > 0 {
        decisions.push(CloseoutDecision {
            kind: "missing_artifact",
            detail: format!("{missing} referenced artifacts are missing or not yet observed."),
            suggested_command: "forager offdesk poll --json".to_string(),
        });
    }
    let unresolved_packets = implementation_packet_coverage.deferred
        + implementation_packet_coverage.missing
        + implementation_packet_coverage.drifted
        + implementation_packet_coverage.detail_items_deferred
        + implementation_packet_coverage.detail_items_missing
        + implementation_packet_coverage.detail_items_drifted;
    if unresolved_packets > 0 {
        decisions.push(CloseoutDecision {
            kind: "implementation_packet_coverage_review",
            detail: format!(
                "{unresolved_packets} implementation packet coverage item(s) need review: packet goals {} deferred, {} missing, {} drifted; detail items {} deferred, {} missing, {} drifted.",
                implementation_packet_coverage.deferred,
                implementation_packet_coverage.missing,
                implementation_packet_coverage.drifted,
                implementation_packet_coverage.detail_items_deferred,
                implementation_packet_coverage.detail_items_missing,
                implementation_packet_coverage.detail_items_drifted
            ),
            suggested_command:
                "Review `implementation_packet_coverage` in closeout_plan.json before accepting this run."
                    .to_string(),
        });
    }
    let archive_candidates = operations
        .iter()
        .filter(|operation| operation.operation == "archive_candidate")
        .count();
    if archive_candidates > 0 {
        decisions.push(CloseoutDecision {
            kind: "archive_review",
            detail: format!(
                "{archive_candidates} archive candidates require commercial review and human approval."
            ),
            suggested_command: format!(
                "Review {}",
                args.output
                    .as_ref()
                    .map(|path| path.join("COMMERCIAL_REVIEW_PACKET.md").display().to_string())
                    .unwrap_or_else(|| "COMMERCIAL_REVIEW_PACKET.md".to_string())
            ),
        });
    }
    for decision in decision_records
        .iter()
        .filter(|decision| closeout_decision_record_is_open(decision))
    {
        let subject = closeout_decision_record_subject(&decision.record);
        let detail = format!(
            "Decision {} is {}: {}",
            decision.record.decision_id,
            decision.record.status.as_str(),
            subject
        );
        decisions.push(CloseoutDecision {
            kind: "decision_record_review",
            detail: truncate_closeout_text(&crate::offdesk::operator_safe_text(&detail), 500),
            suggested_command:
                "Review `decision_records` in closeout_plan.json before accepting this run."
                    .to_string(),
        });
        if !decision.validation_issues.is_empty() {
            decisions.push(CloseoutDecision {
                kind: "decision_record_validation",
                detail: format!(
                    "Decision {} has {} validation issue(s).",
                    crate::offdesk::operator_safe_text(&decision.record.decision_id),
                    decision.validation_issues.len()
                ),
                suggested_command:
                    "Review `decision_records[].validation_issues` in closeout_plan.json."
                        .to_string(),
            });
        }
    }
    if let Some(snapshot) = git_snapshot {
        if snapshot.status_short.is_some()
            || snapshot.diff_stat.is_some()
            || snapshot.error.is_some()
        {
            decisions.push(CloseoutDecision {
                kind: "git_state_review",
                detail: "Git state is included and must be reviewed before Ondesk return."
                    .to_string(),
                suggested_command: "git status --short && git diff --stat".to_string(),
            });
        }
    }
    if let Some(governance) = documentation_governance {
        if governance.error.is_some() {
            decisions.push(CloseoutDecision {
                kind: "documentation_governance_audit",
                detail: "Documentation governance audit could not be completed for the closeout workdir.".to_string(),
                suggested_command: governance.command.clone(),
            });
        } else if governance.recommendation_count > 0 {
            decisions.push(CloseoutDecision {
                kind: "documentation_governance_review",
                detail: format!(
                    "{} documentation governance recommendation(s) should be reviewed before Ondesk return.",
                    governance.recommendation_count
                ),
                suggested_command: governance.command.clone(),
            });
        }
    }
    decisions
}

fn closeout_verification_commands(
    args: &CloseoutArgs,
    documentation_governance: Option<&CloseoutDocumentationGovernance>,
) -> Vec<String> {
    let mut commands = vec![
        "forager offdesk poll --json".to_string(),
        "forager offdesk tasks --json".to_string(),
        "forager offdesk maintenance-report --json".to_string(),
        "forager offdesk wiki review --json".to_string(),
    ];
    if let Some(governance) = documentation_governance {
        commands.push(governance.command.clone());
    }
    if let Some(project_key) = args.project_key.as_deref() {
        commands.push(format!(
            "forager ondesk prompt-package --project-key {}",
            crate::offdesk::operator_safe_text(project_key)
        ));
    }
    if args.include_git {
        commands.push("git status --short && git diff --stat".to_string());
    }
    commands
}

fn closeout_documentation_governance(
    args: &CloseoutArgs,
    tasks: &[OffdeskTask],
) -> Option<CloseoutDocumentationGovernance> {
    let workdir = closeout_project_workdir(args, tasks)?;
    let workdir_label = crate::offdesk::operator_safe_text(workdir.to_string_lossy().as_ref());
    let command = format!(
        "forager project audit-docs {} --audit-profile standard --json",
        shell_arg(&workdir_label)
    );
    if !workdir.exists() {
        return Some(CloseoutDocumentationGovernance {
            workdir: workdir_label,
            audit_profile: "standard".to_string(),
            command,
            recommendation_count: 0,
            recommendations: Vec::new(),
            error: Some("workdir does not exist".to_string()),
        });
    }

    match audit_recommendations_for_project(&workdir, DocumentationAuditProfile::Standard, 100_000)
    {
        Ok(recommendations) => {
            let recommendation_count = recommendations.len();
            Some(CloseoutDocumentationGovernance {
                workdir: workdir_label,
                audit_profile: "standard".to_string(),
                command,
                recommendation_count,
                recommendations: recommendations
                    .into_iter()
                    .take(5)
                    .map(closeout_documentation_recommendation)
                    .collect(),
                error: None,
            })
        }
        Err(error) => Some(CloseoutDocumentationGovernance {
            workdir: workdir_label,
            audit_profile: "standard".to_string(),
            command,
            recommendation_count: 0,
            recommendations: Vec::new(),
            error: Some(crate::offdesk::operator_safe_text(&error.to_string())),
        }),
    }
}

fn closeout_project_workdir(args: &CloseoutArgs, tasks: &[OffdeskTask]) -> Option<PathBuf> {
    args.workdir
        .clone()
        .or_else(|| closeout_project_workdir_from_task_artifacts(tasks))
        .or_else(|| tasks.first().map(|task| PathBuf::from(&task.workdir)))
}

fn closeout_project_workdir_from_task_artifacts(tasks: &[OffdeskTask]) -> Option<PathBuf> {
    tasks.iter().find_map(|task| {
        task.result_artifact_path
            .as_deref()
            .and_then(closeout_project_workdir_from_artifact_path)
            .or_else(|| {
                task.log_artifact_path
                    .as_deref()
                    .and_then(closeout_project_workdir_from_artifact_path)
            })
            .or_else(|| {
                task.artifact_refs.iter().find_map(|artifact| {
                    artifact
                        .path
                        .as_deref()
                        .and_then(closeout_project_workdir_from_artifact_path)
                })
            })
    })
}

fn closeout_project_workdir_from_artifact_path(path: &str) -> Option<PathBuf> {
    let path = Path::new(path);
    let artifact_dir = if path.is_dir() { path } else { path.parent()? };
    for ancestor in artifact_dir.ancestors() {
        for manifest_name in ["prepared_task.json", "manifest.json"] {
            let manifest_path = ancestor.join(manifest_name);
            let repo = closeout_project_workdir_from_manifest(&manifest_path);
            if repo.is_some() {
                return repo;
            }
        }
    }
    None
}

fn closeout_project_workdir_from_manifest(path: &Path) -> Option<PathBuf> {
    let content = fs::read_to_string(path).ok()?;
    let manifest = serde_json::from_str::<Value>(&content).ok()?;
    manifest
        .get("repo")
        .or_else(|| manifest.get("project_path"))
        .or_else(|| manifest.get("target_repo"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != "-")
        .map(PathBuf::from)
}

fn closeout_documentation_recommendation(
    recommendation: AuditRecommendation,
) -> CloseoutDocumentationRecommendation {
    CloseoutDocumentationRecommendation {
        priority: recommendation.priority,
        kind: recommendation.kind,
        title: recommendation.title,
        suggested_action: recommendation.suggested_action,
        paths: recommendation.paths.into_iter().take(5).collect(),
    }
}

fn summarize_closeout(
    tasks: &[CloseoutTask],
    background_runs: &[CloseoutBackgroundRun],
    operations: &[CloseoutFileOperation],
    decision_records: &[CloseoutDecisionRecord],
    implementation_packet_coverage: &CloseoutImplementationPacketCoverage,
) -> CloseoutSummary {
    let mut summary = CloseoutSummary {
        tasks_scanned: tasks.len(),
        background_runs_scanned: background_runs.len(),
        completed_tasks: tasks
            .iter()
            .filter(|task| task.status == OffdeskTaskStatus::Completed)
            .count(),
        active_or_blocked_tasks: tasks
            .iter()
            .filter(|task| {
                !matches!(
                    task.status,
                    OffdeskTaskStatus::Completed | OffdeskTaskStatus::Cancelled
                )
            })
            .count(),
        file_operations: operations.len(),
        decision_records_scanned: decision_records.len(),
        open_decision_records: decision_records
            .iter()
            .filter(|decision| closeout_decision_record_is_open(decision))
            .count(),
        invalid_decision_records: decision_records
            .iter()
            .filter(|decision| !decision.validation_issues.is_empty())
            .count(),
        implementation_packets_scanned: implementation_packet_coverage.packet_count,
        packet_goals_completed: implementation_packet_coverage.completed,
        packet_goals_deferred: implementation_packet_coverage.deferred,
        packet_goals_missing: implementation_packet_coverage.missing,
        packet_goals_drifted: implementation_packet_coverage.drifted,
        packet_detail_items: implementation_packet_coverage.detail_items,
        packet_detail_items_completed: implementation_packet_coverage.detail_items_completed,
        packet_detail_items_deferred: implementation_packet_coverage.detail_items_deferred,
        packet_detail_items_missing: implementation_packet_coverage.detail_items_missing,
        packet_detail_items_drifted: implementation_packet_coverage.detail_items_drifted,
        return_package_required: true,
        ..CloseoutSummary::default()
    };
    for operation in operations {
        match operation.operation {
            "keep" => summary.keep_operations += 1,
            "archive_candidate" => summary.archive_candidates += 1,
            "delete_candidate" => summary.delete_candidates += 1,
            _ => {}
        }
        if operation.requires_commercial_review {
            summary.operations_requiring_commercial_review += 1;
        }
        if operation.requires_human_approval {
            summary.operations_requiring_human_approval += 1;
        }
        if !operation.present {
            summary.missing_artifacts += 1;
        }
    }
    summary
}

fn print_closeout_report(report: &OffdeskCloseoutReport) {
    println!("Offdesk closeout plan");
    println!("  generated_at: {}", report.generated_at);
    println!("  closeout_id:  {}", report.closeout_id);
    println!("  profile:      {}", report.profile);
    println!("  artifact_dir: {}", report.artifact_dir);
    println!(
        "  tasks:        scanned={} completed={} active_or_blocked={}",
        report.summary.tasks_scanned,
        report.summary.completed_tasks,
        report.summary.active_or_blocked_tasks
    );
    println!(
        "  operations:   keep={} archive={} delete={} review_required={}",
        report.summary.keep_operations,
        report.summary.archive_candidates,
        report.summary.delete_candidates,
        report.summary.operations_requiring_commercial_review
    );
    if report.summary.implementation_packets_scanned > 0 {
        println!(
            "  packets:      scanned={} completed={} deferred={} missing={} drifted={}",
            report.summary.implementation_packets_scanned,
            report.summary.packet_goals_completed,
            report.summary.packet_goals_deferred,
            report.summary.packet_goals_missing,
            report.summary.packet_goals_drifted
        );
        if report.summary.packet_detail_items > 0 {
            println!(
                "  packet items: completed={} deferred={} missing={} drifted={} total={}",
                report.summary.packet_detail_items_completed,
                report.summary.packet_detail_items_deferred,
                report.summary.packet_detail_items_missing,
                report.summary.packet_detail_items_drifted,
                report.summary.packet_detail_items
            );
        }
    }
    println!("  dry_run:      true (no project files moved or deleted)");
    println!("Artifacts:");
    println!("  plan:         {}", report.artifacts.closeout_plan_json);
    println!(
        "  markdown:     {}",
        report.artifacts.closeout_plan_markdown
    );
    println!(
        "  review:       {}",
        report.artifacts.commercial_review_packet
    );
    println!(
        "  return:       {}",
        report.artifacts.return_package_markdown
    );
    if !report.open_decisions.is_empty() {
        println!("Open decisions:");
        for decision in &report.open_decisions {
            println!("  - {}: {}", decision.kind, decision.detail);
        }
    }
}

fn print_closeout_review_record(record: &CloseoutReviewRecord) {
    println!("Offdesk closeout review");
    println!("  reviewed_at:  {}", record.reviewed_at);
    println!("  review_id:    {}", record.review_id);
    println!("  closeout_id:  {}", record.closeout_id);
    println!("  verdict:      {}", record.verdict.as_str());
    println!(
        "  acceptance:   {}",
        record.closeout_receipt.acceptance_status
    );
    println!("  reviewer:     {}", record.reviewer);
    if let Some(provider) = record.review_provider.as_deref() {
        println!("  provider:     {provider}");
    }
    if let Some(resolution) = record.decision_resolution.as_ref() {
        println!("  decision:     {}", resolution.kind);
        println!("  resolution:   {}", resolution.decision);
        println!("  reason:       {}", resolution.reason);
    }
    println!("  project file mutations: none");
    println!("Artifacts:");
    println!("  plan:         {}", record.artifacts.closeout_plan_json);
    println!("  review:       {}", record.artifacts.review_record_json);
    println!("  receipt:      {}", record.artifacts.closeout_receipt_json);
    println!(
        "  return:       {}",
        record.artifacts.return_package_markdown
    );
    if !record.unsafe_operations.is_empty() {
        println!("Unsafe operations:");
        for operation in &record.unsafe_operations {
            println!("  - {operation}");
        }
    }
    if !record.missing_evidence.is_empty() {
        println!("Missing evidence:");
        for evidence in &record.missing_evidence {
            println!("  - {evidence}");
        }
    }
}

fn truncate_closeout_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        value.to_string()
    } else {
        format!(
            "{}...[truncated]",
            value.chars().take(max_chars).collect::<String>()
        )
    }
}

fn shell_arg(value: &str) -> String {
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '/' | '.' | '_' | '-' | ':'))
    {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

fn short_uuid() -> String {
    Uuid::new_v4().to_string()[..8].to_string()
}

fn build_maintenance_request(
    profile: &str,
    args: MaintenanceRequestArgs,
) -> Result<MaintenanceApprovalRequestReport> {
    let preview = require_non_empty_arg("--preview", &args.preview)?.to_string();
    let reason = require_non_empty_arg("--reason", &args.reason)?.to_string();
    let project_key = require_non_empty_arg("--project-key", &args.project_key)?.to_string();
    let request_id = require_non_empty_arg("--request-id", &args.request_id)?.to_string();
    let target_id = clean_optional_string(&args.target_id);
    let task_id = clean_optional_string(&args.task_id)
        .unwrap_or_else(|| maintenance_default_task_id(args.kind, target_id.as_deref()));
    let risk_level = args.risk.unwrap_or_else(|| args.kind.default_risk());
    if risk_level == RiskLevel::Safe {
        bail!("maintenance-request requires an approval-gated risk; use maintenance-report for read-only checks");
    }

    let generated_at = Utc::now();
    let action = args.kind.action_id().to_string();
    let mut request = ActionApprovalRequest::new(
        project_key.clone(),
        request_id.clone(),
        task_id.clone(),
        action.clone(),
        risk_level,
    );
    request.mutation_class = Some(action.clone());
    request.preview = preview;
    request.reason = reason;
    request.source_surface = args.source_surface;
    request.ttl = Duration::minutes(args.ttl_minutes.max(1));

    let ledger = ApprovalLedger::new(get_profile_dir(profile)?);
    let (mut session, _) = ledger.begin_session(generated_at)?;
    let pending = session.ensure_pending_without_consuming_grant(request, generated_at)?;
    session.flush()?;

    let approvals = ledger.load()?;
    let approval = pending
        .or_else(|| {
            matching_maintenance_approval(
                &approvals,
                &project_key,
                &request_id,
                &task_id,
                &action,
                risk_level,
            )
        })
        .map(|approval| serde_json::to_value(approval).map(operator_safe_json_value))
        .transpose()?;
    let approval_status = approval
        .as_ref()
        .and_then(|approval| approval["status"].as_str())
        .unwrap_or("not_created");
    let status = maintenance_request_status(approval_status).to_string();
    let detail = maintenance_request_detail(approval_status);
    let next_commands = maintenance_request_next_commands(approval.as_ref());

    Ok(MaintenanceApprovalRequestReport {
        generated_at,
        action_kind: args.kind,
        action,
        project_key: crate::offdesk::operator_safe_text(&project_key),
        request_id: crate::offdesk::operator_safe_text(&request_id),
        task_id: crate::offdesk::operator_safe_text(&task_id),
        target_id: target_id.map(|value| crate::offdesk::operator_safe_text(&value)),
        risk_level,
        status,
        detail,
        approval,
        next_commands,
    })
}

fn matching_maintenance_approval(
    approvals: &[PendingActionApproval],
    project_key: &str,
    request_id: &str,
    task_id: &str,
    action: &str,
    risk_level: RiskLevel,
) -> Option<PendingActionApproval> {
    approvals
        .iter()
        .find(|approval| {
            approval.project_key == project_key
                && approval.request_id == request_id
                && approval.task_id == task_id
                && approval.action == action
                && approval.risk_level == risk_level
                && approval.status == ApprovalStatus::Pending
        })
        .or_else(|| {
            approvals.iter().find(|approval| {
                approval.project_key == project_key
                    && approval.request_id == request_id
                    && approval.task_id == task_id
                    && approval.action == action
                    && approval.risk_level == risk_level
            })
        })
        .cloned()
}

fn maintenance_request_status(approval_status: &str) -> &'static str {
    match approval_status {
        "pending" => "pending_approval",
        "approved" => "already_approved",
        "denied" => "previously_denied",
        "expired" => "expired",
        "superseded" => "superseded",
        _ => "not_created",
    }
}

fn maintenance_request_detail(approval_status: &str) -> String {
    match approval_status {
        "pending" => "Maintenance action approval is pending or was reused.".to_string(),
        "approved" => {
            "A matching maintenance approval already exists; this command did not consume it."
                .to_string()
        }
        "denied" => {
            "A matching maintenance approval was previously denied; create a new scoped request if needed."
                .to_string()
        }
        "expired" => "A matching maintenance approval is expired.".to_string(),
        "superseded" => "A matching maintenance approval is superseded.".to_string(),
        _ => "No maintenance approval was created.".to_string(),
    }
}

fn maintenance_request_next_commands(approval: Option<&Value>) -> Vec<String> {
    let Some(approval_id) = approval.and_then(|approval| approval["approval_id"].as_str()) else {
        return vec!["forager offdesk pending".to_string()];
    };
    vec![
        format!("forager offdesk ok {approval_id}"),
        format!("forager offdesk deny {approval_id}"),
        "after approval, run the reviewed maintenance command explicitly".to_string(),
    ]
}

fn build_maintenance_report(
    profile: &str,
    args: &MaintenanceReportArgs,
) -> Result<OffdeskMaintenanceReport> {
    let profile_dir = read_only_profile_dir(profile)?;
    let generated_at = Utc::now();

    let tasks = OffdeskTaskStore::new(&profile_dir)
        .load()?
        .into_iter()
        .map(|task| task.operator_view())
        .collect::<Vec<_>>();
    let task_summary = summarize_maintenance_tasks(&tasks);

    let background_runs = BackgroundRunStore::new(&profile_dir)
        .load()?
        .into_iter()
        .map(|probe| background_probe_status(probe, generated_at))
        .collect::<Vec<_>>();
    let background_summary = summarize_maintenance_background(&background_runs);

    let approvals = ApprovalLedger::new(&profile_dir).load()?;
    let approval_summary = summarize_maintenance_approvals(&approvals);

    let resume_states = TaskResumeStore::new(&profile_dir).load()?;
    let resume_summary = summarize_maintenance_resume(&resume_states);

    let provider_capacity_states = ProviderCapacityStore::new(&profile_dir).load()?;
    let provider_capacity_summary =
        summarize_maintenance_provider_capacity(&provider_capacity_states);

    let wiki_store = AdaptiveWikiStore::new(&profile_dir);
    let all_wiki_query = AdaptiveWikiQuery {
        session_id: None,
        project_key: None,
        artifact_kind: None,
        agent_mode: None,
        agent_mode_filter: AdaptiveWikiAgentModeFilter::AllWhenUnspecified,
    };
    let wiki_projection = wiki_store.human_projection(&all_wiki_query)?;
    let wiki_review_near_expiry_hours = args.wiki_review_near_expiry_hours.max(1);
    let adaptive_wiki_review_after_attention_summary = build_review_after_report(
        wiki_projection.entries,
        all_wiki_query,
        wiki_review_near_expiry_hours,
        generated_at,
    )
    .summary;

    let runtime_policy_acknowledgements = wiki_store.load_runtime_policy_acknowledgements()?;
    let wiki_runtime_ack_near_expiry_hours = args.wiki_runtime_ack_near_expiry_hours.max(1);
    let adaptive_wiki_runtime_policy_ack_attention_summary = build_runtime_policy_ack_report(
        runtime_policy_acknowledgements,
        None,
        None,
        None,
        wiki_runtime_ack_near_expiry_hours,
        generated_at,
    )
    .summary;

    let recommended_actions = maintenance_recommended_actions(
        &task_summary,
        &background_summary,
        &approval_summary,
        &resume_summary,
        &provider_capacity_summary,
        &adaptive_wiki_runtime_policy_ack_attention_summary,
        &adaptive_wiki_review_after_attention_summary,
    );
    let next_safe_actions = maintenance_next_safe_actions(&recommended_actions);

    let profile_name = if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    };
    Ok(OffdeskMaintenanceReport {
        generated_at,
        profile: operator_safe_report(profile_name).text,
        profile_dir: operator_safe_report(profile_dir.to_string_lossy().as_ref()).text,
        read_only: true,
        tasks: task_summary,
        background_runs: background_summary,
        approvals: approval_summary,
        resume_states: resume_summary,
        provider_capacity: provider_capacity_summary,
        adaptive_wiki_runtime_policy_ack_attention_summary,
        adaptive_wiki_review_after_attention_summary,
        recommended_actions,
        next_safe_actions,
    })
}

fn summarize_maintenance_tasks(tasks: &[OffdeskTaskView]) -> MaintenanceTaskSummary {
    let mut summary = MaintenanceTaskSummary {
        total: tasks.len(),
        ..MaintenanceTaskSummary::default()
    };
    for task in tasks {
        increment_count(&mut summary.by_status, enum_label(task.status));
        record_agent_mode(task.agent_mode, &mut summary.by_agent_mode);
        if task.agent_mode.is_none() {
            summary.missing_agent_mode += 1;
        }
        record_mode_assessment(&task.mode_assessment, &mut summary.mode);
    }
    summary
}

fn summarize_maintenance_background(
    statuses: &[BackgroundProbeStatus],
) -> MaintenanceBackgroundSummary {
    let mut summary = MaintenanceBackgroundSummary {
        total: statuses.len(),
        ..MaintenanceBackgroundSummary::default()
    };
    for status in statuses {
        increment_count(&mut summary.by_phase, enum_label(status.probe.phase));
        record_agent_mode(status.probe.agent_mode, &mut summary.by_agent_mode);
        if status.probe.agent_mode.is_none() {
            summary.missing_agent_mode += 1;
        }
        record_mode_assessment(&status.mode_assessment, &mut summary.mode);
    }
    summary
}

fn summarize_maintenance_approvals(
    approvals: &[PendingActionApproval],
) -> MaintenanceApprovalSummary {
    let mut summary = MaintenanceApprovalSummary {
        total: approvals.len(),
        ..MaintenanceApprovalSummary::default()
    };
    for approval in approvals {
        let status = enum_label(approval.status);
        if status == "pending" {
            summary.pending += 1;
        }
        increment_count(&mut summary.by_status, status);
    }
    summary
}

fn summarize_maintenance_resume(states: &[TaskResumeState]) -> MaintenanceResumeSummary {
    let mut summary = MaintenanceResumeSummary {
        total: states.len(),
        ..MaintenanceResumeSummary::default()
    };
    for state in states {
        increment_count(&mut summary.by_status, enum_label(state.status));
    }
    summary
}

fn summarize_maintenance_provider_capacity(
    states: &[ProviderCapacityState],
) -> MaintenanceProviderCapacitySummary {
    let mut summary = MaintenanceProviderCapacitySummary {
        total: states.len(),
        ..MaintenanceProviderCapacitySummary::default()
    };
    for state in states {
        let status = enum_label(state.status);
        if status != "available" {
            summary.attention += 1;
        }
        increment_count(&mut summary.by_status, status);
    }
    summary
}

fn record_agent_mode(
    agent_mode: Option<AdaptiveWikiAgentMode>,
    counts: &mut BTreeMap<String, usize>,
) {
    let label = agent_mode
        .map(adaptive_wiki_agent_mode_cli_value)
        .unwrap_or("missing");
    increment_count(counts, label.to_string());
}

fn record_mode_assessment(
    assessment: &OffdeskModeAssessment,
    summary: &mut MaintenanceModeSummary,
) {
    increment_count(
        &mut summary.by_verdict,
        assessment.mode_verdict.label().to_string(),
    );
    increment_count(
        &mut summary.by_risk,
        assessment.mode_risk.label().to_string(),
    );
    if assessment.review_stage_required {
        summary.review_stage_required += 1;
    }
}

fn increment_count(counts: &mut BTreeMap<String, usize>, key: String) {
    *counts.entry(key).or_insert(0) += 1;
}

fn enum_label(value: impl Serialize) -> String {
    match serde_json::to_value(value) {
        Ok(Value::String(value)) => value,
        Ok(value) => value.to_string(),
        Err(_) => "unknown".to_string(),
    }
}

fn maintenance_risk_count(summary: &MaintenanceModeSummary, risk: &str) -> usize {
    summary.by_risk.get(risk).copied().unwrap_or(0)
}

fn maintenance_recommended_actions(
    tasks: &MaintenanceTaskSummary,
    background_runs: &MaintenanceBackgroundSummary,
    approvals: &MaintenanceApprovalSummary,
    resume_states: &MaintenanceResumeSummary,
    provider_capacity: &MaintenanceProviderCapacitySummary,
    runtime_ack_summary: &WikiRuntimePolicyAckReportSummary,
    review_after_summary: &WikiReviewAfterReportSummary,
) -> Vec<MaintenanceRecommendedAction> {
    let mut actions = Vec::new();
    if approvals.pending > 0 {
        actions.push(MaintenanceRecommendedAction {
            kind: "pending_approval",
            detail: format!(
                "{} pending approvals need an operator decision.",
                approvals.pending
            ),
            command: "forager offdesk pending",
        });
    }
    let review_required = maintenance_risk_count(&tasks.mode, "operator_review_required")
        + maintenance_risk_count(&background_runs.mode, "operator_review_required");
    if review_required > 0 {
        actions.push(MaintenanceRecommendedAction {
            kind: "operator_review",
            detail: format!("{review_required} completed mode-scoped items need separate review."),
            command: "forager offdesk tasks",
        });
    }
    let missing_result = maintenance_risk_count(&tasks.mode, "missing_result_artifact")
        + maintenance_risk_count(&background_runs.mode, "missing_result_artifact");
    if missing_result > 0 {
        actions.push(MaintenanceRecommendedAction {
            kind: "missing_result_artifact",
            detail: format!("{missing_result} completed items have no result artifact to inspect."),
            command: "forager offdesk tasks --json",
        });
    }
    let runtime_recovery = maintenance_risk_count(&tasks.mode, "runtime_recovery_required")
        + maintenance_risk_count(&background_runs.mode, "runtime_recovery_required");
    if runtime_recovery > 0 || resume_states.total > 0 {
        actions.push(MaintenanceRecommendedAction {
            kind: "runtime_recovery",
            detail: format!(
                "{runtime_recovery} mode assessments need recovery; {} resume records exist.",
                resume_states.total
            ),
            command: "forager offdesk resume",
        });
    }
    let missing_mode = tasks.missing_agent_mode + background_runs.missing_agent_mode;
    if missing_mode > 0 {
        actions.push(MaintenanceRecommendedAction {
            kind: "missing_agent_mode",
            detail: format!("{missing_mode} durable records are missing agent_mode scope."),
            command: "forager offdesk debug-bundle",
        });
    }
    if provider_capacity.attention > 0 {
        actions.push(MaintenanceRecommendedAction {
            kind: "provider_capacity",
            detail: format!(
                "{} provider capacity records are cooling down or blocked.",
                provider_capacity.attention
            ),
            command: "forager offdesk provider-capacity",
        });
    }
    let runtime_ack_attention = runtime_ack_summary.expired
        + runtime_ack_summary.near_expiry
        + runtime_ack_summary.suggested_actions;
    if runtime_ack_attention > 0 {
        actions.push(MaintenanceRecommendedAction {
            kind: "wiki_runtime_ack",
            detail: format!(
                "{runtime_ack_attention} runtime policy acknowledgement signals need attention."
            ),
            command: "forager offdesk wiki runtime-policy-ack-report",
        });
    }
    if review_after_summary.attention > 0 {
        actions.push(MaintenanceRecommendedAction {
            kind: "wiki_review_after",
            detail: format!(
                "{} adaptive wiki entries are expired or near review_after.",
                review_after_summary.attention
            ),
            command: "forager offdesk wiki review-after-report",
        });
    }
    actions
}

fn maintenance_next_safe_actions(
    recommended_actions: &[MaintenanceRecommendedAction],
) -> Vec<OffdeskNextSafeAction> {
    recommended_actions
        .iter()
        .map(|action| {
            OffdeskNextSafeAction::new(
                maintenance_next_safe_action_kind(action.kind),
                action.detail.clone(),
                vec![action.command.to_string()],
                true,
            )
        })
        .collect()
}

fn maintenance_next_safe_action_kind(kind: &str) -> &'static str {
    match kind {
        "pending_approval" => "approval_pending",
        "operator_review" => "review_required",
        "missing_result_artifact" => "result_artifact_missing",
        "runtime_recovery" => "recovery_required",
        "missing_agent_mode" => "mode_scope_required",
        "provider_capacity" => "provider_attention",
        "wiki_runtime_ack" | "wiki_review_after" => "wiki_review_required",
        _ => "maintenance_attention",
    }
}

fn write_debug_bundle_export(
    profile: &str,
    bundle: &OffdeskDebugBundle,
    output: Option<&PathBuf>,
) -> Result<DebugBundleExport> {
    let bytes = serde_json::to_vec_pretty(bundle)?;

    if let Some(path) = output {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).with_context(|| {
                format!("create debug bundle export directory {}", parent.display())
            })?;
        }
        let bytes_written = write_new_file(path, &bytes)
            .with_context(|| format!("write debug bundle export {}", path.display()))?;
        return Ok(DebugBundleExport {
            path: path.clone(),
            bytes_written,
        });
    }

    let export_dir = read_only_profile_dir(profile)?.join("debug_bundles");
    fs::create_dir_all(&export_dir).with_context(|| {
        format!(
            "create debug bundle export directory {}",
            export_dir.display()
        )
    })?;
    let timestamp = bundle.generated_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let filename = if attempt == 0 {
            format!("offdesk_debug_bundle_{timestamp}.json")
        } else {
            format!("offdesk_debug_bundle_{timestamp}_{attempt:03}.json")
        };
        let path = export_dir.join(filename);
        match write_new_file(&path, &bytes) {
            Ok(bytes_written) => {
                return Ok(DebugBundleExport {
                    path,
                    bytes_written,
                })
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("write debug bundle export {}", path.display()));
            }
        }
    }

    bail!(
        "could not allocate debug bundle export path in {}",
        export_dir.display()
    )
}

fn write_new_file(path: &Path, bytes: &[u8]) -> io::Result<usize> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    Ok(bytes.len())
}

fn operator_safe_json_value(value: Value) -> Value {
    match value {
        Value::String(text) => Value::String(crate::offdesk::operator_safe_text(&text)),
        Value::Array(values) => {
            Value::Array(values.into_iter().map(operator_safe_json_value).collect())
        }
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(key, value)| (key, operator_safe_json_value(value)))
                .collect(),
        ),
        other => other,
    }
}

fn approval_ledger(profile: &str) -> Result<ApprovalLedger> {
    Ok(ApprovalLedger::new(get_profile_dir(profile)?))
}

fn resume_store(profile: &str) -> Result<TaskResumeStore> {
    Ok(TaskResumeStore::new(get_profile_dir(profile)?))
}

fn background_store(profile: &str) -> Result<BackgroundRunStore> {
    Ok(BackgroundRunStore::new(get_profile_dir(profile)?))
}

fn task_store(profile: &str) -> Result<OffdeskTaskStore> {
    Ok(OffdeskTaskStore::new(get_profile_dir(profile)?))
}

fn wiki_store(profile: &str) -> Result<AdaptiveWikiStore> {
    Ok(AdaptiveWikiStore::new(read_only_profile_dir(profile)?))
}

fn writable_wiki_store(profile: &str) -> Result<AdaptiveWikiStore> {
    Ok(AdaptiveWikiStore::new(get_profile_dir(profile)?))
}

fn mutation_snapshot_store(profile: &str) -> Result<MutationSnapshotStore> {
    Ok(MutationSnapshotStore::new(get_profile_dir(profile)?))
}

fn wiki_query(
    session_id: &Option<String>,
    project_key: &Option<String>,
    artifact_kind: &Option<String>,
    agent_mode: Option<AdaptiveWikiAgentMode>,
) -> AdaptiveWikiQuery {
    AdaptiveWikiQuery {
        session_id: clean_optional_string(session_id),
        project_key: clean_optional_string(project_key),
        artifact_kind: clean_optional_string(artifact_kind),
        agent_mode,
        agent_mode_filter: AdaptiveWikiAgentModeFilter::AllWhenUnspecified,
    }
}

fn runtime_wiki_query(
    session_id: &Option<String>,
    project_key: &Option<String>,
    artifact_kind: &Option<String>,
    agent_mode: Option<AdaptiveWikiAgentMode>,
) -> AdaptiveWikiQuery {
    let mut query = wiki_query(session_id, project_key, artifact_kind, agent_mode);
    query.agent_mode_filter = AdaptiveWikiAgentModeFilter::SharedWhenUnspecified;
    query
}

fn wiki_episode_out_of_scope_query(args: &WikiEpisodeArgs) -> AdaptiveWikiQuery {
    let mut query = wiki_query(
        &args.out_session_id,
        &args.out_project_key,
        &args.out_artifact_kind,
        args.out_agent_mode,
    );
    if query.session_id.is_none() {
        query.session_id =
            clean_optional_string(&args.session_id).map(|value| format!("out-of-scope-{value}"));
    }
    if query.project_key.is_none() {
        query.project_key =
            clean_optional_string(&args.project_key).map(|value| format!("out-of-scope-{value}"));
    }
    if query.artifact_kind.is_none() {
        query.artifact_kind =
            clean_optional_string(&args.artifact_kind).map(|value| format!("out-of-scope-{value}"));
    }
    if query.agent_mode.is_none() {
        query.agent_mode = args.agent_mode.map(out_of_scope_agent_mode);
    }
    if query.session_id.is_none()
        && query.project_key.is_none()
        && query.artifact_kind.is_none()
        && query.agent_mode.is_none()
    {
        query.project_key = Some("episode-out-of-scope".to_string());
    }
    query
}

fn out_of_scope_agent_mode(mode: AdaptiveWikiAgentMode) -> AdaptiveWikiAgentMode {
    match mode {
        AdaptiveWikiAgentMode::Planning => AdaptiveWikiAgentMode::Development,
        AdaptiveWikiAgentMode::Development => AdaptiveWikiAgentMode::Analysis,
        AdaptiveWikiAgentMode::Analysis => AdaptiveWikiAgentMode::Writing,
        AdaptiveWikiAgentMode::Writing => AdaptiveWikiAgentMode::Critique,
        AdaptiveWikiAgentMode::Critique => AdaptiveWikiAgentMode::Review,
        AdaptiveWikiAgentMode::Review => AdaptiveWikiAgentMode::Maintenance,
        AdaptiveWikiAgentMode::Maintenance => AdaptiveWikiAgentMode::Planning,
    }
}

fn clean_optional_string(value: &Option<String>) -> Option<String> {
    value
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn maintenance_default_task_id(kind: MaintenanceActionKind, target_id: Option<&str>) -> String {
    let mut task_id = format!("maintenance-{}", kind.cli_value().replace('_', "-"));
    if let Some(target_id) = target_id {
        task_id.push('-');
        task_id.push_str(&sanitize_id_fragment(target_id));
    }
    task_id
}

fn sanitize_id_fragment(value: &str) -> String {
    let mut sanitized = value
        .chars()
        .filter_map(|ch| {
            if ch.is_ascii_alphanumeric() {
                Some(ch.to_ascii_lowercase())
            } else if ch == '-' || ch == '_' {
                Some(ch)
            } else if ch.is_whitespace() || ch == '/' || ch == '.' || ch == ':' {
                Some('-')
            } else {
                None
            }
        })
        .collect::<String>();
    while sanitized.contains("--") {
        sanitized = sanitized.replace("--", "-");
    }
    sanitized = sanitized.trim_matches('-').to_string();
    if sanitized.is_empty() {
        "target".to_string()
    } else {
        sanitized.chars().take(64).collect()
    }
}

fn first_non_empty<'a>(values: &[&'a str]) -> Option<&'a str> {
    values
        .iter()
        .map(|value| value.trim())
        .find(|value| !value.is_empty())
}

fn require_non_empty_arg<'a>(name: &str, value: &'a str) -> Result<&'a str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(trimmed)
}

fn find_wiki_candidate(
    store: &AdaptiveWikiStore,
    candidate_id: &str,
) -> Result<Option<AdaptiveWikiCandidate>> {
    Ok(store
        .load_candidates()?
        .candidates
        .into_iter()
        .find(|candidate| candidate.id == candidate_id))
}

fn find_wiki_entry(store: &AdaptiveWikiStore, entry_id: &str) -> Result<Option<AdaptiveWikiEntry>> {
    Ok(store
        .load_entries()?
        .entries
        .into_iter()
        .find(|entry| entry.id == entry_id))
}

fn human_entry(entry: AdaptiveWikiEntry) -> AdaptiveWikiHumanEntry {
    crate::offdesk::build_human_projection(&[entry], &[], &AdaptiveWikiQuery::default())
        .entries
        .into_iter()
        .next()
        .expect("one human entry projection")
}

fn human_candidate(candidate: AdaptiveWikiCandidate) -> AdaptiveWikiHumanCandidate {
    crate::offdesk::build_human_projection(&[], &[candidate], &AdaptiveWikiQuery::default())
        .candidates
        .into_iter()
        .next()
        .expect("one human candidate projection")
}

fn wiki_entry_scope(entry: &AdaptiveWikiEntry) -> AdaptiveWikiScopeSuggestion {
    AdaptiveWikiScopeSuggestion {
        scope: entry.scope,
        scope_ref: crate::offdesk::operator_safe_text(&entry.scope_ref),
    }
}

fn wiki_candidate_scope(candidate: &AdaptiveWikiCandidate) -> AdaptiveWikiScopeSuggestion {
    AdaptiveWikiScopeSuggestion {
        scope: candidate.scope,
        scope_ref: crate::offdesk::operator_safe_text(&candidate.scope_ref),
    }
}

struct WikiAuditRecordInput<'a> {
    action: AdaptiveWikiAuditAction,
    subject_id: &'a str,
    candidate_id: Option<&'a str>,
    entry_id: Option<&'a str>,
    actor: &'a str,
    reason: &'a str,
    evidence_ref: Option<&'a str>,
    before_scope: Option<AdaptiveWikiScopeSuggestion>,
    after_scope: Option<AdaptiveWikiScopeSuggestion>,
    activation_mode: Option<AdaptiveWikiActivationMode>,
    candidate_snapshot: Option<AdaptiveWikiHumanCandidate>,
    entry_snapshot: Option<AdaptiveWikiHumanEntry>,
    now: DateTime<Utc>,
}

fn wiki_audit_record(input: WikiAuditRecordInput<'_>) -> AdaptiveWikiAuditRecord {
    AdaptiveWikiAuditRecord {
        id: format!("wiki_audit_{}", uuid::Uuid::new_v4()),
        action: input.action,
        subject_id: crate::offdesk::operator_safe_text(input.subject_id),
        candidate_id: input.candidate_id.map(crate::offdesk::operator_safe_text),
        entry_id: input.entry_id.map(crate::offdesk::operator_safe_text),
        actor: crate::offdesk::operator_safe_text(input.actor.trim()),
        reason: crate::offdesk::operator_safe_text(input.reason.trim()),
        evidence_ref: input
            .evidence_ref
            .map(|value| crate::offdesk::operator_safe_text(value.trim()))
            .filter(|value| !value.is_empty()),
        before_scope: input.before_scope,
        after_scope: input.after_scope,
        activation_mode: input.activation_mode,
        candidate_snapshot: input.candidate_snapshot,
        entry_snapshot: input.entry_snapshot,
        created_at: input.now,
    }
}

fn default_wiki_scope_ref(scope: AdaptiveWikiScope) -> String {
    match scope {
        AdaptiveWikiScope::UserGlobal => "*".to_string(),
        AdaptiveWikiScope::Session => "-".to_string(),
        AdaptiveWikiScope::ArtifactKind | AdaptiveWikiScope::Project => "*".to_string(),
    }
}

fn read_only_profile_dir(profile: &str) -> Result<PathBuf> {
    let profile_name = crate::session::normalize_profile_name(profile)?;
    Ok(resolved_app_dir_path()?.join("profiles").join(profile_name))
}

impl DebugBundleRedactor {
    fn text(&mut self, input: &str) -> String {
        self.summary.text_fields_checked += 1;
        let outcome = operator_safe_report(input);
        if outcome.changed {
            self.summary.changed_text_fields += 1;
            self.summary.runner_context_removed += outcome.runner_context_removed;
            self.summary.secrets_redacted += outcome.secrets_redacted;
        }
        outcome.text
    }

    fn value(&mut self, value: Value) -> Value {
        match value {
            Value::String(text) => Value::String(self.text(&text)),
            Value::Array(values) => {
                Value::Array(values.into_iter().map(|value| self.value(value)).collect())
            }
            Value::Object(map) => Value::Object(
                map.into_iter()
                    .map(|(key, value)| (key, self.value(value)))
                    .collect(),
            ),
            other => other,
        }
    }
}

fn load_execution_brief(path: Option<&PathBuf>) -> Result<Option<ExecutionBrief>> {
    let Some(path) = path else {
        return Ok(None);
    };
    let content = std::fs::read_to_string(path)?;
    Ok(Some(serde_json::from_str::<ExecutionBrief>(&content)?))
}

fn shell_quote_arg(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn print_gate_outcome(outcome: &crate::offdesk::SchedulerGateOutcome) {
    match outcome.status {
        SchedulerGateStatus::Proceed => {
            println!(
                "Proceed: {} ({}) via {:?}",
                outcome.capability_id, outcome.risk_level, outcome.approval_mode
            );
        }
        SchedulerGateStatus::PendingApproval => {
            println!(
                "Pending approval: {} ({})",
                outcome.capability_id, outcome.risk_level
            );
            if let Some(approval) = &outcome.approval {
                println!("  approval_id: {}", approval.approval_id);
                println!("  action_id:   {}", approval.action_id());
                if !approval.preview.trim().is_empty() {
                    println!("  preview:     {}", approval.preview);
                }
                if !approval.reason.trim().is_empty() {
                    println!("  reason:      {}", approval.reason);
                }
            }
        }
        SchedulerGateStatus::Denied => {
            println!("Denied: {} - {}", outcome.capability_id, outcome.reason);
        }
        SchedulerGateStatus::Blocked => {
            println!("Blocked: {} - {}", outcome.capability_id, outcome.reason);
            if let Some(capacity) = outcome.provider_capacity.as_ref() {
                println!("  provider:  {}", capacity.provider_id);
                println!("  model:     {}", capacity.model.as_deref().unwrap_or("-"));
                println!("  scope:     {}", capacity.matched_scope);
                if let Some(retry_at) = outcome.retry_at {
                    println!("  retry_at:  {retry_at}");
                }
            }
            if let Some(fallback) = outcome.provider_fallback.as_ref() {
                let recommended = fallback
                    .candidates
                    .iter()
                    .filter(|candidate| candidate.recommended)
                    .count();
                println!(
                    "  fallback:  {} candidates, {} recommended",
                    fallback.candidates.len(),
                    recommended
                );
            }
        }
    }
    if !outcome.adaptive_wiki.is_empty() {
        println!("  adaptive_wiki: {} entries", outcome.adaptive_wiki.len());
        for entry in outcome.adaptive_wiki.iter().take(3) {
            println!(
                "    - {} {:?} {:?} agent_modes={}: {}",
                entry.id,
                entry.scope,
                entry.activation_mode,
                adaptive_wiki_agent_modes_label(&entry.agent_modes),
                entry.instruction
            );
        }
    }
    if !outcome.adaptive_wiki_runtime.is_empty() {
        println!(
            "  adaptive_wiki_runtime: {} entries policy review_expired={:?}",
            outcome.adaptive_wiki_runtime.len(),
            outcome.adaptive_wiki_runtime_policy.review_expired
        );
    }
    if let Some(decision) = outcome.adaptive_wiki_runtime_decision.as_ref() {
        println!(
            "  adaptive_wiki_runtime_decision: {:?} ({})",
            decision.status, decision.reason
        );
    }
}

fn wiki_scope_label(scope: AdaptiveWikiScope, scope_ref: &str) -> String {
    format!("{:?}:{}", scope, scope_ref).to_lowercase()
}

fn print_approval_views(approvals: &[OffdeskPendingApprovalView]) {
    println!(
        "{:<44} {:<44} {:<10} {:<18} {:<24} ACTION",
        "APPROVAL ID", "ACTION ID", "STATUS", "RISK", "TASK"
    );
    for approval_view in approvals {
        let approval = &approval_view.approval;
        println!(
            "{:<44} {:<44} {:<10} {:<18} {:<24} {}",
            approval.approval_id,
            approval.action_id(),
            format!("{:?}", approval.status).to_lowercase(),
            format!("{:?}", approval.risk_level).to_lowercase(),
            approval.task_id,
            approval.action
        );
        if !approval.preview.trim().is_empty() {
            println!("  preview: {}", approval.preview);
        }
        if !approval.reason.trim().is_empty() {
            println!("  reason:  {}", approval.reason);
        }
        if let Some(brief) = approval
            .metadata
            .as_ref()
            .and_then(crate::offdesk::ActionApprovalMetadata::approval_brief)
        {
            println!(
                "  prompt: {} recommendation for {}",
                brief.recommendation, brief.subject
            );
            for line in brief.summary_lines.iter().take(3) {
                println!("    {}", line);
            }
            println!("  question: {}", brief.question);
            println!("  scope: {}", brief.scope);
        }
        if let Some(metadata) = approval
            .metadata
            .as_ref()
            .and_then(crate::offdesk::ActionApprovalMetadata::as_provider_fallback)
        {
            println!(
                "  fallback target: {} model {} ({})",
                metadata.current_provider_id,
                metadata.current_model.as_deref().unwrap_or("-"),
                format!("{:?}", metadata.apply_scope).to_lowercase()
            );
            for candidate in metadata.candidates.iter().take(metadata.candidate_limit) {
                println!(
                    "    - {} {} ({:?})",
                    candidate.provider_id,
                    candidate.model.as_deref().unwrap_or("-"),
                    candidate.source
                );
            }
        }
        if let Some(metadata) = approval
            .metadata
            .as_ref()
            .and_then(crate::offdesk::ActionApprovalMetadata::as_artifact_retention)
        {
            println!(
                "  artifact: {} [{} / {}]",
                metadata.label, metadata.retention_class, metadata.review_status
            );
            println!(
                "  requested: {} recommended: {}",
                metadata.requested_action, metadata.recommended_action
            );
        }
        print_next_safe_action(&approval_view.next_safe_action);
    }
}

fn print_tasks(tasks: &[OffdeskTaskView]) {
    let open = tasks
        .iter()
        .filter(|task| !is_terminal_task_status(task.status))
        .collect::<Vec<_>>();
    let terminal = tasks
        .iter()
        .filter(|task| is_terminal_task_status(task.status))
        .collect::<Vec<_>>();

    if !open.is_empty() {
        println!("Open tasks:");
        print_task_rows(&open);
    }
    if !terminal.is_empty() {
        if !open.is_empty() {
            println!();
        }
        println!("Terminal tasks:");
        print_task_rows(&terminal);
    }
}

fn print_decisions(records: &[DecisionRecord]) {
    println!(
        "{:<28} {:<16} {:<10} {:<18} {:<10} {:<18} SUBJECT",
        "DECISION", "STATUS", "MATERIAL", "EVAL", "TARGET", "TASK"
    );
    for record in records {
        let evaluator = record
            .judgment_route
            .as_ref()
            .map(|route| route.evaluator.as_str())
            .unwrap_or("-");
        let target = record
            .route
            .as_ref()
            .map(|route| route.target.as_str())
            .unwrap_or("-");
        let subject = record
            .approval_brief
            .as_ref()
            .map(|brief| brief.subject.as_str())
            .unwrap_or(record.decision_request.kind.as_str());
        println!(
            "{:<28} {:<16} {:<10} {:<18} {:<10} {:<18} {}",
            record.decision_id,
            record.status.as_str(),
            record.materiality.as_str(),
            evaluator,
            target,
            record.task_id,
            subject
        );
        if !record.decision_request.summary.trim().is_empty() {
            println!("  summary: {}", record.decision_request.summary);
        }
        if let Some(judgment) = record.judgment_route.as_ref() {
            println!(
                "  judgment: {} ({})",
                judgment.evaluator.as_str(),
                judgment.reason
            );
        }
        if let Some(route) = record.route.as_ref() {
            println!("  route:   {} ({})", route.target.as_str(), route.reason);
        }
        let issue_count = record.validation_issues().len();
        if issue_count > 0 {
            println!("  validation_issues: {}", issue_count);
        }
    }
}

fn print_decision(record: &DecisionRecord) {
    println!("decision: {}", record.decision_id);
    println!("status:   {}", record.status.as_str());
    println!("material: {}", record.materiality.as_str());
    println!("project:  {}", record.project_key);
    println!("request:  {}", record.request_id);
    println!("task:     {}", record.task_id);
    println!("raised:   {}", record.raised_by.as_str());
    println!("source:   {}", record.source_surface);
    println!("updated:  {}", record.updated_at);
    println!();
    println!("Decision request:");
    println!("  kind:     {}", record.decision_request.kind);
    println!("  summary:  {}", record.decision_request.summary);
    println!("  needed:   {}", record.decision_request.decision_needed);
    println!("  scope:    {}", record.decision_request.current_scope);
    if !record.decision_request.non_authorized_scope.is_empty() {
        println!(
            "  not authorized: {}",
            record.decision_request.non_authorized_scope.join(", ")
        );
    }
    if let Some(council) = record.council_review.as_ref() {
        println!();
        println!("Council:");
        println!("  recommendation: {}", council.recommendation);
        if let Some(agreement) = council.agreement {
            println!("  agreement:      {}", agreement);
        }
        if !council.reviewer_decisions.is_empty() {
            println!("  reviewers:");
            for (reviewer, decision) in &council.reviewer_decisions {
                println!("    - {}: {}", reviewer, decision);
            }
        }
    }
    if let Some(judgment) = record.judgment_route.as_ref() {
        println!();
        println!("Judgment route:");
        println!("  evaluator: {}", judgment.evaluator.as_str());
        println!("  reason:    {}", judgment.reason);
        println!(
            "  selected:  {} by {}",
            judgment.selected_at, judgment.selected_by
        );
        if let Some(default) = judgment.default_if_no_reply.as_deref() {
            println!("  default:   {}", default);
        }
        if !judgment.policy_basis.is_empty() {
            println!("  policy:");
            for basis in &judgment.policy_basis {
                println!("    - {}", basis);
            }
        }
    }
    if let Some(route) = record.route.as_ref() {
        println!();
        println!("Delivery route:");
        println!("  target:  {}", route.target.as_str());
        println!("  reason:  {}", route.reason);
        if let Some(default) = route.default_if_no_reply.as_deref() {
            println!("  default: {}", default);
        }
    }
    if let Some(brief) = record.approval_brief.as_ref() {
        println!();
        println!("Approval brief:");
        println!("  recommendation: {}", brief.recommendation);
        println!("  subject:        {}", brief.subject);
        println!("  question:       {}", brief.question);
        println!("  scope:          {}", brief.scope);
    }
    if let Some(handoff) = record.execution_handoff.as_ref() {
        println!();
        println!("Execution handoff:");
        println!("  handoff_id: {}", handoff.handoff_id);
        println!("  target:     {}", handoff.target);
        println!("  direction:  {}", handoff.approved_direction);
        println!("  scope:      {}", handoff.approved_scope);
    }
    if let Some(receipt) = record.decision_receipt.as_ref() {
        println!();
        println!("Decision receipt:");
        println!("  receipt_id: {}", receipt.receipt_id);
        println!("  decision:   {}", receipt.final_decision);
        println!(
            "  resolved:   {} by {}",
            receipt.resolved_at, receipt.resolved_by
        );
        println!("  result:     {}", receipt.result_status);
    }
    let validation_issues = record.validation_issues();
    if !validation_issues.is_empty() {
        println!();
        println!("Validation issues:");
        for issue in validation_issues {
            println!(
                "  - {:?}: {} ({})",
                issue.severity, issue.code, issue.detail
            );
        }
    }
}

fn print_task_rows(tasks: &[&OffdeskTaskView]) {
    println!(
        "{:<24} {:<18} {:<18} {:<14} TICKET",
        "TASK", "STATUS", "CAPABILITY", "RUNNER"
    );
    for task in tasks {
        println!(
            "{:<24} {:<18} {:<18} {:<14} {}",
            task.task_id,
            task_status_label(task.status),
            task.capability_id,
            format!("{:?}", task.runner_kind).to_lowercase(),
            task.background_ticket_id.as_deref().unwrap_or("-")
        );
        if !task.preview.trim().is_empty() {
            println!("  preview: {}", task.preview);
        }
        if let Some(last_error) = task.last_error.as_deref() {
            println!("  error:   {}", last_error);
        }
        if !task.last_adaptive_wiki_entry_ids.is_empty() {
            println!(
                "  adaptive_wiki: {}",
                task.last_adaptive_wiki_entry_ids.join(", ")
            );
        }
        if let Some(agent_mode) = task.agent_mode {
            println!(
                "  agent_mode: {}",
                adaptive_wiki_agent_mode_cli_value(agent_mode)
            );
        }
        print_mode_assessment(&task.mode_assessment);
        if task.provider_id.is_some() || task.model.is_some() {
            println!(
                "  provider: {} model: {}",
                task.provider_id.as_deref().unwrap_or("-"),
                task.model.as_deref().unwrap_or("-")
            );
        }
        if let Some(artifact_kind) = task.artifact_kind.as_deref() {
            println!("  artifact_kind: {}", artifact_kind);
        }
        if let Some(fallback) = task.last_provider_fallback.as_ref() {
            let recommended = fallback
                .candidates
                .iter()
                .filter(|candidate| candidate.recommended)
                .count();
            println!(
                "  fallback: {} candidates, {} recommended",
                fallback.candidates.len(),
                recommended
            );
            for candidate in fallback
                .candidates
                .iter()
                .filter(|candidate| candidate.recommended)
                .take(3)
            {
                println!(
                    "    - {} {} ({:?})",
                    candidate.provider_id,
                    candidate.model.as_deref().unwrap_or("-"),
                    candidate.source
                );
            }
        }
        if let Some(not_before) = task.not_before {
            println!("  not_before: {not_before}");
        }
        if let Some(last_gate_status) = task.last_gate_status {
            println!(
                "  gate:    {}",
                format!("{:?}", last_gate_status).to_lowercase()
            );
        }
        print_next_safe_action(&task.next_safe_action);
    }
}

fn print_next_safe_actions(actions: &[OffdeskNextSafeAction]) {
    if actions.is_empty() {
        return;
    }
    println!("Next safe actions:");
    for action in actions {
        print_next_safe_action(action);
    }
}

fn print_next_safe_action(action: &OffdeskNextSafeAction) {
    println!("  next:    {}", action.detail);
    if !action.commands.is_empty() {
        println!("  command: {}", action.commands.join(" | "));
    }
    if action.requires_operator_review {
        println!("  review:  operator review required");
    }
}

fn print_mode_assessment(assessment: &OffdeskModeAssessment) {
    println!(
        "  mode_verdict: {} risk: {}",
        assessment.mode_verdict.label(),
        assessment.mode_risk.label()
    );
    println!("  mode_risk_detail: {}", assessment.mode_risk_detail);
    if assessment.review_stage_required {
        println!("  review_stage_required: true");
    }
}

fn print_provider_fallback(recommendation: &ProviderFallbackRecommendation) {
    println!(
        "Provider fallback for {} model {}",
        recommendation.current_provider_id,
        recommendation.current_model.as_deref().unwrap_or("-")
    );
    println!("  trigger: {}", recommendation.trigger_reason);
    if recommendation.candidates.is_empty() {
        println!("  no fallback candidates found");
        return;
    }
    println!(
        "{:<20} {:<28} {:<30} {:<14} {:<14} RECOMMENDED",
        "PROVIDER", "MODEL", "SOURCE", "AUTH", "CAPACITY"
    );
    for candidate in &recommendation.candidates {
        println!(
            "{:<20} {:<28} {:<30} {:<14} {:<14} {}",
            candidate.provider_id,
            candidate.model.as_deref().unwrap_or("-"),
            format!("{:?}", candidate.source).to_lowercase(),
            format!("{:?}", candidate.auth_status).to_lowercase(),
            format!("{:?}", candidate.capacity_status).to_lowercase(),
            if candidate.recommended { "yes" } else { "no" }
        );
        println!("  reason: {}", candidate.reason);
    }
}

fn print_provider_capacity(states: &[ProviderCapacityState]) {
    println!(
        "{:<20} {:<24} {:<14} {:<16} COOLDOWN_UNTIL",
        "PROVIDER", "MODEL", "STATUS", "REASON"
    );
    for state in states {
        println!(
            "{:<20} {:<24} {:<14} {:<16} {}",
            crate::offdesk::operator_safe_text(&state.provider_id),
            state
                .model
                .as_deref()
                .map(crate::offdesk::operator_safe_text)
                .unwrap_or_else(|| "-".to_string()),
            format!("{:?}", state.status).to_lowercase(),
            format!("{:?}", state.reason).to_lowercase(),
            state
                .cooldown_until
                .map(|cooldown_until| cooldown_until.to_string())
                .unwrap_or_else(|| "-".to_string())
        );
        if let Some(summary) = state.last_error_summary.as_deref() {
            println!("  summary: {}", crate::offdesk::operator_safe_text(summary));
        }
    }
}

fn is_terminal_task_status(status: OffdeskTaskStatus) -> bool {
    matches!(
        status,
        OffdeskTaskStatus::Completed | OffdeskTaskStatus::Cancelled
    )
}

fn parse_offdesk_task_status(value: &str) -> std::result::Result<OffdeskTaskStatus, String> {
    match value.trim().to_ascii_lowercase().replace('_', "-").as_str() {
        "queued" => Ok(OffdeskTaskStatus::Queued),
        "pending-approval" => Ok(OffdeskTaskStatus::PendingApproval),
        "launched" => Ok(OffdeskTaskStatus::Launched),
        "running" => Ok(OffdeskTaskStatus::Running),
        "completed" => Ok(OffdeskTaskStatus::Completed),
        "failed" => Ok(OffdeskTaskStatus::Failed),
        "resume-pending" => Ok(OffdeskTaskStatus::ResumePending),
        "cancelled" => Ok(OffdeskTaskStatus::Cancelled),
        _ => Err("expected one of: queued, pending-approval, launched, running, completed, failed, resume-pending, cancelled".to_string()),
    }
}

fn print_capabilities(capabilities: &[CapabilityDescriptor]) {
    println!(
        "{:<24} {:<20} {:<18} {:<8} LABEL",
        "CAPABILITY", "OWNER", "RISK", "OFFDESK"
    );
    for capability in capabilities {
        println!(
            "{:<24} {:<20} {:<18} {:<8} {}",
            capability.capability_id,
            capability.owner_module,
            format!("{:?}", capability.risk_level).to_lowercase(),
            if capability.offdesk_allowed {
                "yes"
            } else {
                "no"
            },
            capability.dashboard_label
        );
    }
}

fn print_debug_bundle_summary(bundle: &OffdeskDebugBundle) {
    println!("Offdesk debug bundle");
    println!("  generated_at:       {}", bundle.generated_at);
    println!("  profile:            {}", bundle.profile);
    println!("  profile_dir:        {}", bundle.profile_dir);
    println!("  read_only:          {}", bundle.read_only);
    println!(
        "  approvals:          {}",
        json_array_len(&bundle.approvals)
    );
    println!("  tasks:              {}", json_array_len(&bundle.tasks));
    println!(
        "  resume_states:      {}",
        json_array_len(&bundle.resume_states)
    );
    println!(
        "  background_runs:    {}",
        json_array_len(&bundle.background_runs)
    );
    println!(
        "  provider_capacity:  {}",
        json_array_len(&bundle.provider_capacity)
    );
    println!(
        "  wiki_usage:         {}",
        json_array_len(&bundle.adaptive_wiki_usage)
    );
    println!(
        "  wiki_corrections:   {}",
        json_array_len(&bundle.adaptive_wiki_corrections)
    );
    println!(
        "  wiki_review_events: {}",
        json_array_len(&bundle.adaptive_wiki_review_events)
    );
    println!(
        "  wiki_runtime_acks:  {}",
        json_array_len(&bundle.adaptive_wiki_runtime_policy_acknowledgements)
    );
    println!(
        "  wiki_ack_attention: expired={} near_expiry={} suggested_actions={}",
        bundle
            .adaptive_wiki_runtime_policy_ack_attention_summary
            .expired,
        bundle
            .adaptive_wiki_runtime_policy_ack_attention_summary
            .near_expiry,
        bundle
            .adaptive_wiki_runtime_policy_ack_attention_summary
            .suggested_actions
    );
    println!(
        "  wiki_review_after:  expired={} near_expiry={} missing_review_after={}",
        bundle.adaptive_wiki_review_after_attention_summary.expired,
        bundle
            .adaptive_wiki_review_after_attention_summary
            .near_expiry,
        bundle
            .adaptive_wiki_review_after_attention_summary
            .missing_review_after
    );
    println!(
        "  redaction:          {} changed fields, {} context blocks, {} secrets",
        bundle.redaction_summary.changed_text_fields,
        bundle.redaction_summary.runner_context_removed,
        bundle.redaction_summary.secrets_redacted
    );
}

fn print_maintenance_report(report: &OffdeskMaintenanceReport) {
    println!("Offdesk maintenance report");
    println!("  generated_at:       {}", report.generated_at);
    println!("  profile:            {}", report.profile);
    println!("  profile_dir:        {}", report.profile_dir);
    println!("  read_only:          {}", report.read_only);
    println!(
        "  tasks:              total={} status=[{}] risk=[{}]",
        report.tasks.total,
        format_counts(&report.tasks.by_status),
        format_counts(&report.tasks.mode.by_risk)
    );
    println!(
        "  task_modes:         agent=[{}] review_required={}",
        format_counts(&report.tasks.by_agent_mode),
        report.tasks.mode.review_stage_required
    );
    println!(
        "  background_runs:    total={} phase=[{}] risk=[{}]",
        report.background_runs.total,
        format_counts(&report.background_runs.by_phase),
        format_counts(&report.background_runs.mode.by_risk)
    );
    println!(
        "  approvals:          total={} pending={} status=[{}]",
        report.approvals.total,
        report.approvals.pending,
        format_counts(&report.approvals.by_status)
    );
    println!(
        "  resume_states:      total={} status=[{}]",
        report.resume_states.total,
        format_counts(&report.resume_states.by_status)
    );
    println!(
        "  provider_capacity:  total={} attention={} status=[{}]",
        report.provider_capacity.total,
        report.provider_capacity.attention,
        format_counts(&report.provider_capacity.by_status)
    );
    println!(
        "  wiki_ack_attention: expired={} near_expiry={} suggested_actions={}",
        report
            .adaptive_wiki_runtime_policy_ack_attention_summary
            .expired,
        report
            .adaptive_wiki_runtime_policy_ack_attention_summary
            .near_expiry,
        report
            .adaptive_wiki_runtime_policy_ack_attention_summary
            .suggested_actions
    );
    println!(
        "  wiki_review_after:  expired={} near_expiry={} missing_review_after={}",
        report.adaptive_wiki_review_after_attention_summary.expired,
        report
            .adaptive_wiki_review_after_attention_summary
            .near_expiry,
        report
            .adaptive_wiki_review_after_attention_summary
            .missing_review_after
    );
    if report.recommended_actions.is_empty() {
        println!("No maintenance actions recommended.");
    } else {
        println!("Recommended actions:");
        for action in &report.recommended_actions {
            println!("  - {}: {}", action.kind, action.detail);
            println!("    command: {}", action.command);
        }
    }
    print_next_safe_actions(&report.next_safe_actions);
}

fn print_maintenance_request_report(report: &MaintenanceApprovalRequestReport) {
    println!("Offdesk maintenance approval request");
    println!("  generated_at: {}", report.generated_at);
    println!("  action:       {}", report.action);
    println!("  kind:         {}", report.action_kind.cli_value());
    println!("  risk:         {}", enum_label(report.risk_level));
    println!("  status:       {}", report.status);
    println!("  detail:       {}", report.detail);
    println!("  project_key:  {}", report.project_key);
    println!("  request_id:   {}", report.request_id);
    println!("  task_id:      {}", report.task_id);
    if let Some(target_id) = &report.target_id {
        println!("  target_id:    {}", target_id);
    }
    if let Some(approval) = &report.approval {
        if let Some(approval_id) = approval["approval_id"].as_str() {
            println!("  approval_id:  {}", approval_id);
        }
    }
    if !report.next_commands.is_empty() {
        println!("Next commands:");
        for command in &report.next_commands {
            println!("  - {}", command);
        }
    }
}

fn format_counts(counts: &BTreeMap<String, usize>) -> String {
    if counts.is_empty() {
        return "-".to_string();
    }
    counts
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn json_array_len(value: &Value) -> usize {
    value.as_array().map_or(0, Vec::len)
}

fn snapshot_list_item(
    snapshot: MutationSnapshot,
    verification: MutationSnapshotVerification,
) -> MutationSnapshotListItem {
    MutationSnapshotListItem {
        mutation_id: snapshot.mutation_id,
        target_path: snapshot.target_path,
        mutation_kind: snapshot.mutation_kind,
        created_at: snapshot.created_at,
        rollback_available: verification.rollback_available,
        blockers: verification.blockers,
    }
}

fn print_snapshot_list(items: &[MutationSnapshotListItem]) {
    println!(
        "{:<44} {:<14} {:<9} TARGET",
        "MUTATION ID", "KIND", "ROLLBACK"
    );
    for item in items {
        println!(
            "{:<44} {:<14} {:<9} {}",
            item.mutation_id,
            item.mutation_kind,
            if item.rollback_available { "yes" } else { "no" },
            item.target_path
        );
        if !item.blockers.is_empty() {
            println!("  blockers: {}", item.blockers.join("; "));
        }
    }
}

fn print_snapshot_verification(verification: &MutationSnapshotVerification) {
    let Some(snapshot) = verification.snapshot.as_ref() else {
        println!("Mutation snapshot not found: {}", verification.mutation_id);
        return;
    };
    println!("Snapshot {}", snapshot.mutation_id);
    println!("  target:              {}", snapshot.target_path);
    println!("  mutation_kind:       {}", snapshot.mutation_kind);
    println!("  rollback_available:  {}", verification.rollback_available);
    println!(
        "  target_exists_now:   {}",
        verification
            .target_exists_now
            .map(|exists| exists.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "  target_matches_before: {}",
        verification
            .target_current_matches_before
            .map(|matches| matches.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    if let Some(path) = verification.before_snapshot_path.as_deref() {
        println!("  before_snapshot:     {path}");
    }
    if !verification.blockers.is_empty() {
        println!(
            "  blockers:            {}",
            verification.blockers.join("; ")
        );
    }
}

fn print_restore_plan(plan: &MutationRestorePlan) {
    println!("Restore plan {}", plan.mutation_id);
    println!("  target:             {}", plan.target_path);
    println!("  operation:          {:?}", plan.operation);
    println!("  rollback_available: {}", plan.rollback_available);
    match plan.operation {
        MutationRestoreOperation::RestoreFile => {
            if let Some(path) = plan.before_snapshot_path.as_deref() {
                println!("  source:             {path}");
            } else {
                println!("  source:             empty file");
            }
        }
        MutationRestoreOperation::DeleteFile => {
            println!("  source:             target did not exist before mutation");
        }
        MutationRestoreOperation::Unavailable => {}
    }
    if !plan.blockers.is_empty() {
        println!("  blockers:           {}", plan.blockers.join("; "));
    }
}
