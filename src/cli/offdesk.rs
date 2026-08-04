//! `forager offdesk` operator commands.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use clap::{Args, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use uuid::Uuid;

use super::project_audit::{
    audit_recommendations_for_project, AuditRecommendation, DocumentationAuditProfile,
};
use crate::offdesk::{
    assess_offdesk_mode, build_graph_export_files, build_usage_records_with_policy,
    default_capability_registry, implementation_packet_from_path,
    implementation_packet_record_from_path, latest_implementation_packet_for_project,
    launch_background_command, launch_background_run, operator_safe_report, operator_safe_text,
    pending_approval_operator_views, poll_background_runs, recommend_provider_fallback,
    reconcile_tasks_with_background_outcomes, run_offdesk_tick, scan_and_emit_learning_signals,
    work_slice_execution_receipts_from_path, ActionApprovalRequest, AdaptiveWikiActivationMode,
    AdaptiveWikiAgentMode, AdaptiveWikiAgentModeFilter, AdaptiveWikiAuditAction,
    AdaptiveWikiAuditRecord, AdaptiveWikiCandidate, AdaptiveWikiCandidateInput,
    AdaptiveWikiConfidence, AdaptiveWikiCorrectionRecurrenceReport, AdaptiveWikiEntry,
    AdaptiveWikiEpisodeEvaluationReport, AdaptiveWikiGraphReport, AdaptiveWikiHumanCandidate,
    AdaptiveWikiHumanEntry, AdaptiveWikiKind, AdaptiveWikiLintReport,
    AdaptiveWikiLiveEpisodeFilter, AdaptiveWikiLiveEpisodeTraceReport,
    AdaptiveWikiMarkdownExportReport, AdaptiveWikiOrigin, AdaptiveWikiProjectionBudget,
    AdaptiveWikiProjectionComparisonReport, AdaptiveWikiProjectionPolicy,
    AdaptiveWikiProjectionReport, AdaptiveWikiProjectionReviewExpiredPolicy,
    AdaptiveWikiPromotionEvidenceChainReport, AdaptiveWikiPromotionReceipt,
    AdaptiveWikiPromotionReceiptAuthority, AdaptiveWikiQuery, AdaptiveWikiReviewProposal,
    AdaptiveWikiReviewProposalAction, AdaptiveWikiReviewProposalDecision,
    AdaptiveWikiReviewProposalEventRecord, AdaptiveWikiReviewQueueFilter, AdaptiveWikiReviewReport,
    AdaptiveWikiRuntimePolicyAckScopeMode, AdaptiveWikiRuntimePolicyAcknowledgement,
    AdaptiveWikiRuntimePolicyDecision, AdaptiveWikiRuntimePolicyDecisionStatus, AdaptiveWikiScope,
    AdaptiveWikiScopeSuggestion, AdaptiveWikiSignalKind, AdaptiveWikiStore,
    AdaptiveWikiUsageContext, ApprovalBrief, ApprovalBriefOption, ApprovalLedger, ApprovalStatus,
    BackgroundLaunchOutcome, BackgroundLaunchRequest, BackgroundProbe,
    BackgroundRecoveryAcknowledgement, BackgroundRecoveryDecision, BackgroundRunStore,
    BackgroundRunnerKind, BackgroundRunnerPhase, CapabilityArtifactRef, CapabilityDescriptor,
    DecisionLedger, DecisionMateriality, DecisionOption, DecisionRaisedBy, DecisionReceipt,
    DecisionRecord, DecisionRecordView, DecisionRequest, DecisionRoute, DecisionRouteTarget,
    DecisionStatus, DecisionTraceRef, DecisionValidationIssue, ExecutionBrief, ExecutionHandoff,
    ImplementationPacket, ImplementationPacketSummary, JudgmentEvaluator, JudgmentRoute,
    LatestImplementationPacket, LearningScanReport, LocalCommandLaunchSpec,
    MutationRestoreOperation, MutationRestorePlan, MutationSnapshot, MutationSnapshotStore,
    MutationSnapshotVerification, OffdeskModeAssessment, OffdeskModeLifecycle,
    OffdeskNextSafeAction, OffdeskPendingApprovalView, OffdeskTask, OffdeskTaskInput,
    OffdeskTaskLifecycleReport, OffdeskTaskStatus, OffdeskTaskStore, OffdeskTaskView,
    OffdeskTickOptions, OperatorPauseState, OperatorPauseStore, PendingActionApproval,
    ProviderCapacityState, ProviderCapacityStore, ProviderFallbackRecommendation, ResumeStatus,
    RiskLevel, SchedulerGate, SchedulerGateRequest, SchedulerGateStatus, TaskResumeState,
    TaskResumeStore, WorkSliceExecutionReceipt, WorkSliceExecutionStatus,
    WorkSliceReceiptProducerRole, WorkSliceVerificationStatus, DECISION_RECORD_SCHEMA,
    JUDGMENT_ROUTE_SCHEMA, WORK_SLICE_EXECUTION_RECEIPTS_FILE,
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

#[derive(Serialize)]
struct HostedHarnessProfileView {
    id: &'static str,
    display_name: &'static str,
    support_status: &'static str,
    launch_command: Option<&'static str>,
    runner: &'static str,
    mutation_scope: &'static str,
    prompt_contract: HostedHarnessPromptContract,
    evidence_sources: &'static [&'static str],
    result_artifact: &'static str,
    failure_signal: &'static str,
    closeout_package: &'static str,
    retention_policy: &'static str,
    notes: &'static str,
}

#[derive(Serialize)]
struct HostedHarnessPromptContract {
    strategy: &'static str,
    inline_context_budget_bytes: usize,
    first_read_file_budget_bytes: u64,
    first_read_total_budget_bytes: u64,
    first_read_required: bool,
    preferred_first_reads: &'static [&'static str],
    discouraged_inline_context: &'static [&'static str],
    invocation_hint: &'static str,
}

const COMPACT_FIRST_READ_PROMPT_CONTRACT: HostedHarnessPromptContract = HostedHarnessPromptContract {
    strategy: "compact_prompt_with_first_read_artifacts",
    inline_context_budget_bytes: 4096,
    first_read_file_budget_bytes: 65_536,
    first_read_total_budget_bytes: 262_144,
    first_read_required: true,
    preferred_first_reads: &[
        "RETURN_PACKAGE.md",
        "closeout_plan.json",
        "result.json",
        "focused task brief",
        "operator-selected source files",
    ],
    discouraged_inline_context: &[
        "full git diff",
        "large logs",
        "raw scrollback",
        "entire repository inventory",
    ],
    invocation_hint: "Pass a short task prompt, keep large context in artifacts, and make the harness read only the listed first-read paths.",
};

const PLANNED_PROMPT_CONTRACT: HostedHarnessPromptContract = HostedHarnessPromptContract {
    strategy: "unvalidated",
    inline_context_budget_bytes: 0,
    first_read_file_budget_bytes: 65_536,
    first_read_total_budget_bytes: 262_144,
    first_read_required: true,
    preferred_first_reads: &["to be defined by integration smoke"],
    discouraged_inline_context: &["full git diff", "large logs", "raw scrollback"],
    invocation_hint:
        "Do not promote this harness until a compact prompt and first-read artifact smoke passes.",
};

const HOSTED_HARNESS_PROFILES: &[HostedHarnessProfileView] = &[
    HostedHarnessProfileView {
        id: "codex",
        display_name: "Codex CLI",
        support_status: "supported",
        launch_command: Some("codex"),
        runner: "local-tmux",
        mutation_scope: "operator-selected repository or disposable worktree",
        prompt_contract: COMPACT_FIRST_READ_PROMPT_CONTRACT,
        evidence_sources: &[
            "tmux pane capture",
            "Forager background run record",
            "runner log artifact when launched through offdesk",
            "result artifact declared on the task",
            "offdesk closeout package",
        ],
        result_artifact: "task-declared result sidecar or closeout RETURN_PACKAGE.md",
        failure_signal: "missing tmux runtime, nonzero runner exit, stale heartbeat/progress, or missing result artifact",
        closeout_package: "offdesk closeout plan plus Ondesk return package",
        retention_policy: "preserve command summary, logs, result sidecar, closeout package, and review verdict",
        notes: "Primary supported harness for current Forager golden-loop work.",
    },
    HostedHarnessProfileView {
        id: "claude",
        display_name: "Claude Code",
        support_status: "supported",
        launch_command: Some("claude"),
        runner: "local-tmux",
        mutation_scope: "operator-selected repository or disposable worktree",
        prompt_contract: COMPACT_FIRST_READ_PROMPT_CONTRACT,
        evidence_sources: &[
            "tmux pane capture",
            "Forager background run record",
            "runner log artifact when launched through offdesk",
            "result artifact declared on the task",
            "offdesk closeout package",
        ],
        result_artifact: "task-declared result sidecar or closeout RETURN_PACKAGE.md",
        failure_signal: "missing tmux runtime, nonzero runner exit, stale heartbeat/progress, or missing result artifact",
        closeout_package: "offdesk closeout plan plus Ondesk return package",
        retention_policy: "preserve command summary, logs, result sidecar, closeout package, and review verdict",
        notes: "Primary supported harness alongside Codex for current Forager golden-loop work.",
    },
    HostedHarnessProfileView {
        id: "gemini",
        display_name: "Gemini CLI",
        support_status: "planned",
        launch_command: Some("gemini"),
        runner: "local-tmux",
        mutation_scope: "not yet part of the supported golden loop",
        prompt_contract: PLANNED_PROMPT_CONTRACT,
        evidence_sources: &["to be validated with a disposable smoke task"],
        result_artifact: "planned",
        failure_signal: "planned",
        closeout_package: "planned",
        retention_policy: "planned",
        notes: "Registry entry exists, but the hosted harness evidence contract is not yet validated.",
    },
    HostedHarnessProfileView {
        id: "openhands",
        display_name: "OpenHands",
        support_status: "planned",
        launch_command: None,
        runner: "external-or-local",
        mutation_scope: "not yet part of the supported golden loop",
        prompt_contract: PLANNED_PROMPT_CONTRACT,
        evidence_sources: &["to be defined after integration smoke"],
        result_artifact: "planned",
        failure_signal: "planned",
        closeout_package: "planned",
        retention_policy: "planned",
        notes: "Future integration candidate; not a current support target.",
    },
    HostedHarnessProfileView {
        id: "aider",
        display_name: "Aider",
        support_status: "planned",
        launch_command: None,
        runner: "local-tmux",
        mutation_scope: "not yet part of the supported golden loop",
        prompt_contract: PLANNED_PROMPT_CONTRACT,
        evidence_sources: &["to be defined after integration smoke"],
        result_artifact: "planned",
        failure_signal: "planned",
        closeout_package: "planned",
        retention_policy: "planned",
        notes: "Future integration candidate; not a current support target.",
    },
];

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum OffdeskPlanReviewDecision {
    Approved,
    RevisionRequired,
    Rejected,
}

impl OffdeskPlanReviewDecision {
    fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::RevisionRequired => "revision_required",
            Self::Rejected => "rejected",
        }
    }
}

#[derive(Serialize)]
struct HostedHarnessPromptPacket {
    harness_id: String,
    display_name: String,
    support_status: String,
    prompt_strategy: String,
    inline_context_budget_bytes: usize,
    first_read_file_budget_bytes: u64,
    first_read_total_budget_bytes: u64,
    first_read_required: bool,
    first_read_total_bytes: u64,
    first_read_budget_status: String,
    task: String,
    workdir: Option<String>,
    first_reads: Vec<HostedHarnessFirstRead>,
    result_artifact: Option<String>,
    output_path: Option<String>,
    warnings: Vec<String>,
    prompt_markdown: String,
}

#[derive(Serialize)]
struct HostedHarnessFirstRead {
    path: String,
    present: bool,
    size_bytes: Option<u64>,
    over_file_budget: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct OffdeskPlanRegistration {
    schema: String,
    registered_at: DateTime<Utc>,
    forager_profile: String,
    source_path: String,
    source_sha256: String,
    artifact_kind: String,
    plan_schema: String,
    profile_key: Option<String>,
    profile_name: Option<String>,
    project_key: Option<String>,
    request_id: Option<String>,
    task_id: Option<String>,
    ready_for_operator_review: bool,
    ready_for_launch_preparation: bool,
    ready_for_enqueue: bool,
    validation_failures: Vec<String>,
    decision: Option<Value>,
    consensus: Option<Value>,
    selected_plan_path: Option<String>,
    dry_run: bool,
    artifacts: OffdeskPlanRegistrationArtifacts,
    does_not_authorize: Vec<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct OffdeskPlanRegistrationArtifacts {
    registry_dir: Option<String>,
    registration_json: Option<String>,
    copied_source_json: Option<String>,
}

#[derive(Serialize)]
struct OffdeskPlanRegistryItem {
    plan_id: String,
    registration_path: String,
    registration: OffdeskPlanRegistration,
    review_state: OffdeskPlanReviewState,
    review_count: usize,
    latest_review: Option<OffdeskPlanReviewRecord>,
    launch_prep_count: usize,
    latest_launch_prep: Option<OffdeskPlanLaunchPrepPacket>,
}

#[derive(Serialize)]
struct OffdeskPlanRegistryDetail {
    plan_id: String,
    registration_path: String,
    registration: OffdeskPlanRegistration,
    review_state: OffdeskPlanReviewState,
    review_count: usize,
    latest_review: Option<OffdeskPlanReviewRecord>,
    reviews: Vec<OffdeskPlanReviewRecord>,
    launch_prep_count: usize,
    latest_launch_prep: Option<OffdeskPlanLaunchPrepPacket>,
    launch_preps: Vec<OffdeskPlanLaunchPrepPacket>,
}

#[derive(Clone, Serialize)]
struct OffdeskPlanReviewState {
    status: String,
    ready_for_launch_preparation_candidate: bool,
    next_safe_action: String,
    latest_review_id: Option<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct OffdeskPlanReviewRecord {
    schema: String,
    reviewed_at: DateTime<Utc>,
    review_id: String,
    plan_id: String,
    forager_profile: String,
    registration_path: String,
    source_sha256: String,
    decision: OffdeskPlanReviewDecision,
    reviewer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    review_provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    review_file: Option<String>,
    reason: String,
    blockers: Vec<String>,
    followups: Vec<String>,
    ready_for_launch_preparation_candidate: bool,
    ready_for_enqueue: bool,
    read_only_project_state: bool,
    applies_file_operations: bool,
    artifacts: OffdeskPlanReviewArtifacts,
    does_not_authorize: Vec<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct OffdeskPlanReviewArtifacts {
    registration_json: String,
    copied_source_json: Option<String>,
    review_record_json: String,
}

#[derive(Clone, Deserialize, Serialize)]
struct OffdeskPlanLaunchPrepPacket {
    schema: String,
    prepared_at: DateTime<Utc>,
    prep_id: String,
    plan_id: String,
    forager_profile: String,
    prepared_by: String,
    registration_path: String,
    source_path: String,
    source_sha256: String,
    review_id: String,
    review_decision: OffdeskPlanReviewDecision,
    review_record_json: String,
    artifact_kind: String,
    plan_schema: String,
    profile_key: Option<String>,
    project_key: Option<String>,
    request_id: Option<String>,
    task_id: Option<String>,
    selected_plan_path: Option<String>,
    required_first_reads: Vec<String>,
    launch_preparation_candidate: bool,
    ready_for_launch: bool,
    ready_for_enqueue: bool,
    next_safe_action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    notes: Option<String>,
    read_only_project_state: bool,
    applies_file_operations: bool,
    artifacts: OffdeskPlanLaunchPrepArtifacts,
    does_not_authorize: Vec<String>,
}

#[derive(Clone, Deserialize, Serialize)]
struct OffdeskPlanLaunchPrepArtifacts {
    registration_json: String,
    copied_source_json: Option<String>,
    review_record_json: String,
    launch_prep_json: String,
}

#[derive(Serialize)]
struct RemoteOperatorProjection<T>
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
struct RemoteOperatorCard {
    title: String,
    summary_lines: Vec<String>,
    detail_lines: Vec<String>,
    observed_hash: String,
    remote_actions: Vec<String>,
    disabled_remote_actions: Vec<String>,
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

#[derive(Serialize)]
struct RemoteOperatorPlansPayload {
    filters: RemoteOperatorPlanFilters,
    plan_count: usize,
    plans: Vec<RemoteOperatorPlanSummary>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorPlanFilters {
    project_key: Option<String>,
    task_id: Option<String>,
    profile_key: Option<String>,
    artifact_kind: Option<String>,
    latest: bool,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorPlanSummaryCore {
    plan_id: String,
    artifact_kind: String,
    plan_schema: String,
    profile_key: Option<String>,
    project_key: Option<String>,
    request_id: Option<String>,
    task_id: Option<String>,
    registered_at: DateTime<Utc>,
    source_sha256: String,
    review_status: String,
    review_count: usize,
    latest_review_id: Option<String>,
    launch_prep_count: usize,
    latest_launch_prep_id: Option<String>,
    ready_for_operator_review: bool,
    launch_preparation_candidate: bool,
    ready_for_enqueue: bool,
    next_safe_action: String,
    remote_actions: Vec<String>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorPlanSummary {
    #[serde(flatten)]
    core: RemoteOperatorPlanSummaryCore,
    observed_hash: String,
}

#[derive(Serialize)]
struct RemoteOperatorPlanDetailPayload {
    plan: RemoteOperatorPlanSummary,
    reviews: Vec<RemoteOperatorPlanReviewSummary>,
    launch_preps: Vec<RemoteOperatorLaunchPrepSummary>,
    does_not_authorize: Vec<String>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorPlanReviewSummary {
    review_id: String,
    reviewed_at: DateTime<Utc>,
    decision: OffdeskPlanReviewDecision,
    reviewer: String,
    ready_for_launch_preparation_candidate: bool,
    ready_for_enqueue: bool,
    blockers: Vec<String>,
    followups: Vec<String>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorLaunchPrepSummary {
    prep_id: String,
    prepared_at: DateTime<Utc>,
    review_id: String,
    launch_preparation_candidate: bool,
    ready_for_launch: bool,
    ready_for_enqueue: bool,
    next_safe_action: String,
}

struct OffdeskPlanInputSummary {
    artifact_kind: &'static str,
    plan_schema: String,
    profile_key: Option<String>,
    profile_name: Option<String>,
    ready_for_operator_review: bool,
    ready_for_launch_preparation: bool,
    ready_for_enqueue: bool,
    decision: Option<Value>,
    consensus: Option<Value>,
    selected_plan_path: Option<String>,
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
pub struct WikiProposalHandoffArgs {
    /// Current curator review proposal id
    proposal_id: String,

    /// Operator-selected mutation path to preview when the proposal is manual
    #[arg(long, value_parser = parse_wiki_proposal_handoff_mutation)]
    mutation: Option<WikiProposalHandoffMutation>,

    /// Scope for a parameterized rescope handoff
    #[arg(long, value_parser = parse_adaptive_wiki_scope)]
    scope: Option<AdaptiveWikiScope>,

    /// Scope reference for a parameterized rescope handoff
    #[arg(long)]
    scope_ref: Option<String>,

    /// Evidence ref for a parameterized counterexample handoff
    #[arg(long = "evidence-ref")]
    evidence_ref: Option<String>,

    /// Entry to deprecate for a parameterized merge cleanup or conflict handoff
    #[arg(long = "deprecated-entry-id")]
    deprecated_entry_id: Option<String>,

    /// Operator rationale to include in the previewed mutation command
    #[arg(long)]
    reason: Option<String>,

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WikiProposalHandoffMutation {
    Rescope,
    Deprecate,
    AddCounterexample,
    DeprecateDuplicate,
    Split,
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
pub struct DeckArgs {
    /// Source Offdesk JSON artifact to summarize into a Marp deck
    #[arg(long = "from")]
    source: PathBuf,

    /// Artifact shape. Use auto unless the source is ambiguous.
    #[arg(long, value_enum, default_value = "auto")]
    kind: OffdeskDeckKind,

    /// Markdown deck output path. Defaults to `<source-stem>.marp.md`.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Overwrite the Markdown deck or rendered artifact if it already exists
    #[arg(long)]
    force: bool,

    /// Optional deck title
    #[arg(long)]
    title: Option<String>,

    /// Render the deck with Marp CLI after writing Markdown
    #[arg(long, value_enum)]
    render: Option<OffdeskDeckRenderFormat>,

    /// Marp CLI binary to use with --render
    #[arg(long, default_value = "marp")]
    marp_bin: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum OffdeskDeckKind {
    Auto,
    Closeout,
    Plan,
    Status,
}

impl OffdeskDeckKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Closeout => "closeout",
            Self::Plan => "plan",
            Self::Status => "status",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum OffdeskDeckRenderFormat {
    Html,
    Pdf,
    Pptx,
}

impl OffdeskDeckRenderFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Html => "html",
            Self::Pdf => "pdf",
            Self::Pptx => "pptx",
        }
    }

    fn extension(self) -> &'static str {
        self.as_str()
    }
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
    decision: CloseoutDecisionResolution,

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

impl CloseoutReviewVerdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Revise => "revise",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
enum CloseoutDecisionResolution {
    PreserveInPlace,
}

impl CloseoutDecisionResolution {
    fn as_str(self) -> &'static str {
        match self {
            Self::PreserveInPlace => "preserve_in_place",
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
pub struct DecisionIngestTelegramArgs {
    /// Operator-safe decision request JSON containing decision_record
    #[arg(long)]
    request: PathBuf,

    /// Telegram relay result JSON
    #[arg(long)]
    result: PathBuf,

    /// Override canonical profile directory for producer integrations
    #[arg(long = "profile-dir")]
    profile_dir: Option<PathBuf>,

    /// Actor recording the relay ingestion
    #[arg(long, default_value = "telegram")]
    by: String,

    /// Override execution handoff target
    #[arg(long)]
    target: Option<String>,

    /// Also append a receipt with this result status after resolving
    #[arg(long = "receipt-result-status")]
    receipt_result_status: Option<String>,

    /// Receipt evidence summary line. Repeat for multiple lines.
    #[arg(long = "receipt-evidence")]
    receipt_evidence_summary: Vec<String>,

    /// Remaining review item. Repeat for multiple lines.
    #[arg(long = "remaining-review")]
    remaining_review: Vec<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct DecisionIngestTelegramFeedbackArgs {
    /// Telegram feedback JSON or JSONL file
    #[arg(long)]
    feedback: PathBuf,

    /// Override canonical profile directory for producer integrations
    #[arg(long = "profile-dir")]
    profile_dir: Option<PathBuf>,

    /// Actor recording the inbox item
    #[arg(long, default_value = "telegram")]
    by: String,

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

    /// Edit an entry's claim, instruction, summary, or evidence refs in place
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
pub struct WikiListArgs {
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

    /// Output as JSON
    #[arg(long)]
    json: bool,
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
pub struct WikiShowArgs {
    /// Adaptive wiki entry or candidate id
    id: String,

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
struct RetryTaskLifecycleReport<'a> {
    #[serde(flatten)]
    report: &'a OffdeskTaskLifecycleReport,
    superseded_denied_approvals: usize,
}

#[derive(Serialize)]
struct WikiProposalHandoffPreview {
    proposal_id: String,
    action: AdaptiveWikiReviewProposalAction,
    subject_kind: String,
    subject_id: String,
    status: &'static str,
    command: Option<String>,
    reason: String,
    lifecycle_decision: Option<AdaptiveWikiReviewProposalDecision>,
    lifecycle_stale: bool,
    evidence_refs: Vec<String>,
    required_inputs: Vec<WikiProposalHandoffInput>,
    mutation_options: Vec<WikiProposalHandoffMutationOption>,
}

#[derive(Serialize)]
struct WikiProposalHandoffInput {
    name: &'static str,
    cli_flag: Option<&'static str>,
    required: bool,
    description: &'static str,
}

#[derive(Serialize)]
struct WikiProposalHandoffMutationOption {
    mutation: &'static str,
    command_template: String,
    required_inputs: Vec<&'static str>,
    description: &'static str,
}

#[derive(Serialize)]
struct WikiProposalReceipt {
    generated_at: DateTime<Utc>,
    read_only: bool,
    status: &'static str,
    proposal: WikiProposalReceiptSubject,
    preview_command: String,
    preview_command_sha256: String,
    audit: Option<AdaptiveWikiAuditRecord>,
    event: Option<AdaptiveWikiReviewProposalEventRecord>,
    checks: Vec<WikiProposalReceiptCheck>,
    blockers: Vec<String>,
}

#[derive(Serialize)]
struct WikiProposalReceiptSubject {
    proposal_id: String,
    current: bool,
    action: Option<AdaptiveWikiReviewProposalAction>,
    subject_kind: String,
    subject_id: String,
    lifecycle_decision: Option<AdaptiveWikiReviewProposalDecision>,
    lifecycle_event_id: Option<String>,
    evidence_refs: Vec<String>,
}

#[derive(Serialize)]
struct WikiProposalReceiptCheck {
    name: &'static str,
    passed: bool,
    detail: String,
}

#[derive(Serialize)]
struct WikiProposalReceiptExportReceipt<'a> {
    exported_to: String,
    bytes_written: usize,
    receipt: &'a WikiProposalReceipt,
}

struct WikiProposalReceiptExport {
    path: PathBuf,
    bytes_written: usize,
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

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WikiShowResult {
    Entry {
        entry: AdaptiveWikiHumanEntry,
    },
    Candidate {
        candidate: AdaptiveWikiHumanCandidate,
    },
}

#[derive(Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
enum WikiMutationResult {
    Promote {
        entry: AdaptiveWikiHumanEntry,
        audit: AdaptiveWikiAuditRecord,
        promotion_receipt: Box<AdaptiveWikiPromotionReceipt>,
        promotion_receipt_path: String,
    },
    Reject {
        candidate: AdaptiveWikiHumanCandidate,
        audit: AdaptiveWikiAuditRecord,
    },
    Rescope {
        entry: AdaptiveWikiHumanEntry,
        audit: AdaptiveWikiAuditRecord,
    },
    Edit {
        entry: AdaptiveWikiHumanEntry,
        audit: AdaptiveWikiAuditRecord,
    },
    Retag {
        entry: AdaptiveWikiHumanEntry,
        audit: AdaptiveWikiAuditRecord,
    },
    Deprecate {
        entry: AdaptiveWikiHumanEntry,
        audit: AdaptiveWikiAuditRecord,
    },
    AddCounterexample {
        entry: AdaptiveWikiHumanEntry,
        audit: AdaptiveWikiAuditRecord,
    },
    UpdateRunbook {
        entry: AdaptiveWikiHumanEntry,
        audit: AdaptiveWikiAuditRecord,
    },
    RenewReviewAfter {
        entry: AdaptiveWikiHumanEntry,
        previous_review_after: Option<DateTime<Utc>>,
        audit: AdaptiveWikiAuditRecord,
    },
}

#[derive(Serialize)]
struct WikiRuntimePolicyAckReport {
    generated_at: DateTime<Utc>,
    near_expiry_hours: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    query: Option<AdaptiveWikiQuery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    budget: Option<AdaptiveWikiProjectionBudget>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decision: Option<AdaptiveWikiRuntimePolicyDecision>,
    summary: WikiRuntimePolicyAckReportSummary,
    acknowledgements: Vec<WikiRuntimePolicyAckReportItem>,
}

#[derive(Default, Serialize)]
struct WikiRuntimePolicyAckReportSummary {
    total: usize,
    active: usize,
    expired: usize,
    near_expiry: usize,
    suggested_actions: usize,
    query_applied: usize,
    query_blocked: usize,
    query_stale: usize,
    query_expired: usize,
}

#[derive(Serialize)]
struct WikiRuntimePolicyAckReportItem {
    id: String,
    scope_mode: AdaptiveWikiRuntimePolicyAckScopeMode,
    query: AdaptiveWikiQuery,
    policy: AdaptiveWikiProjectionPolicy,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    minutes_until_expiry: i64,
    status: Vec<String>,
    review_expired_excluded: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    suggested_action: Option<WikiRuntimePolicyAckSuggestedAction>,
}

#[derive(Serialize)]
struct WikiRuntimePolicyAckSuggestedAction {
    kind: String,
    detail: String,
    compare_command_template: String,
    ack_command_template: String,
}

#[derive(Serialize)]
struct WikiReviewAfterReport {
    generated_at: DateTime<Utc>,
    query: AdaptiveWikiQuery,
    near_expiry_hours: i64,
    summary: WikiReviewAfterReportSummary,
    entries: Vec<WikiReviewAfterReportItem>,
}

#[derive(Default, Serialize)]
struct WikiReviewAfterReportSummary {
    scoped_promoted: usize,
    with_review_after: usize,
    missing_review_after: usize,
    expired: usize,
    near_expiry: usize,
    attention: usize,
}

#[derive(Serialize)]
struct WikiReviewAfterReportItem {
    id: String,
    kind: AdaptiveWikiKind,
    scope: AdaptiveWikiScope,
    scope_ref: String,
    review_after: DateTime<Utc>,
    hours_until_review: i64,
    status: String,
    renew_command_template: String,
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

#[derive(Debug, Serialize)]
struct DecisionIngestTelegramReport {
    request_path: String,
    result_path: String,
    ledger_path: String,
    decision_id: String,
    telegram_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    telegram_decision: Option<String>,
    appended_records: Vec<String>,
    receipt_recorded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    skipped_reason: Option<String>,
    record: DecisionRecord,
    validation_issues: Vec<DecisionValidationIssue>,
}

#[derive(Debug, Serialize)]
struct DecisionIngestTelegramFeedbackReport {
    feedback_path: String,
    ledger_path: String,
    decision_id: String,
    appended: bool,
    record: DecisionRecord,
    validation_issues: Vec<DecisionValidationIssue>,
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

#[derive(Serialize)]
struct OffdeskDeckReport {
    schema: &'static str,
    generated_at: DateTime<Utc>,
    source_path: String,
    source_kind: String,
    marp_markdown_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    rendered_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    render_format: Option<String>,
    render_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    render_error: Option<String>,
    source_of_truth: &'static str,
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

#[derive(Default, Serialize)]
struct CloseoutImplementationPacketCoverage {
    packet_count: usize,
    completed: usize,
    deferred: usize,
    missing: usize,
    drifted: usize,
    detail_items: usize,
    detail_items_completed: usize,
    detail_items_deferred: usize,
    detail_items_missing: usize,
    detail_items_drifted: usize,
    items: Vec<CloseoutImplementationPacketCoverageItem>,
}

#[derive(Serialize)]
struct CloseoutImplementationPacketCoverageItem {
    packet_id: String,
    project_key: String,
    goal: String,
    success_state: String,
    outcome: String,
    safe_to_delegate: bool,
    goal_status: &'static str,
    reason: String,
    evidence_refs: Vec<String>,
    required_revisions: Vec<String>,
    drift_signals: Vec<String>,
    missing_decisions: Vec<String>,
    work_slice_count: usize,
    validation_item_count: usize,
    expected_artifact_count: usize,
    detail_source: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail_error: Option<String>,
    work_slices: Vec<CloseoutPacketCoverageDetail>,
    validation_items: Vec<CloseoutPacketCoverageDetail>,
    expected_artifacts: Vec<CloseoutPacketCoverageDetail>,
}

#[derive(Serialize)]
struct CloseoutPacketCoverageDetail {
    category: &'static str,
    label: String,
    status: &'static str,
    reason: String,
    evidence_refs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt_source: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    receipt_role: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    trust_tier: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reported_status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    claim_status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verification_status: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    verification_summary: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    verification_refs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_observation_status: Option<&'static str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    source_refs: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    summary: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    validation_refs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    artifact_refs: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    open_questions: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    drift_signals: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_safe_action: Option<String>,
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

const CLOSEOUT_RETURN_DECISION_LIMIT: usize = 5;
const CLOSEOUT_RETURN_FIRST_READ_LIMIT: usize = 5;
const CLOSEOUT_RETURN_EVIDENCE_LIMIT: usize = 5;
const CLOSEOUT_RETURN_GOVERNANCE_PATH_LIMIT: usize = 3;
const CLOSEOUT_SOURCE_OBSERVATION_BASE_REF: &str = "HEAD";
const CLOSEOUT_SOURCE_OBSERVATION_CHANGED_FILE_LIMIT: usize = 100;
const CLOSEOUT_SOURCE_OBSERVATION_REF_LIMIT: usize = 5;

#[derive(Serialize)]
struct CloseoutReviewRecord {
    reviewed_at: DateTime<Utc>,
    review_id: String,
    closeout_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    closeout_generated_at: Option<DateTime<Utc>>,
    profile: String,
    artifact_dir: String,
    verdict: CloseoutReviewVerdict,
    reviewer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    review_provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    review_file: Option<String>,
    unsafe_operations: Vec<String>,
    missing_evidence: Vec<String>,
    required_first_reads: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    notes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    decision_resolution: Option<CloseoutDecisionResolutionRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    closeout_retirement: Option<CloseoutRetirementRecord>,
    applies_to_task_ids: Vec<String>,
    applies_to_tasks: Vec<CloseoutReviewTaskRef>,
    read_only_project_state: bool,
    applies_file_operations: bool,
    closeout_receipt: CloseoutReceipt,
    artifacts: CloseoutReviewArtifactPaths,
}

#[derive(Serialize)]
struct CloseoutReviewTaskRef {
    project_key: String,
    request_id: String,
    task_id: String,
}

#[derive(Serialize)]
struct CloseoutReviewArtifactPaths {
    closeout_plan_json: String,
    review_record_json: String,
    closeout_receipt_json: String,
    return_package_markdown: String,
}

#[derive(Serialize)]
struct CloseoutReceipt {
    schema: &'static str,
    receipt_id: String,
    closeout_id: String,
    review_id: String,
    generated_at: DateTime<Utc>,
    reviewed_at: DateTime<Utc>,
    verdict: CloseoutReviewVerdict,
    acceptance_status: &'static str,
    accepted_scope: Vec<String>,
    executed_scope: Vec<String>,
    evidence_status: &'static str,
    verification_status: &'static str,
    open_decisions: Vec<CloseoutReceiptDecision>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    resolved_open_decisions: Vec<CloseoutResolvedDecision>,
    missing_evidence: Vec<String>,
    required_first_reads: Vec<String>,
    unsafe_operations: Vec<String>,
    retention_review: &'static str,
    wiki_promotion_state: &'static str,
    stale_task_count: usize,
    next_safe_action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    retirement_reason: Option<String>,
    source_artifacts: CloseoutReceiptArtifacts,
}

#[derive(Clone, Serialize)]
struct CloseoutReceiptDecision {
    kind: String,
    detail: String,
    suggested_command: String,
}

#[derive(Clone, Serialize)]
struct CloseoutResolvedDecision {
    kind: String,
    decision: String,
    reason: String,
    reviewer: String,
    resolved_at: DateTime<Utc>,
    applies_to_decision: CloseoutReceiptDecision,
    does_not_authorize: Vec<String>,
}

#[derive(Serialize)]
struct CloseoutDecisionResolutionRecord {
    kind: String,
    decision: String,
    reason: String,
    reviewer: String,
    resolved_at: DateTime<Utc>,
    source_review_record_json: String,
    source_receipt_id: Option<String>,
    does_not_authorize: Vec<String>,
}

#[derive(Serialize)]
struct CloseoutRetirementRecord {
    reason: String,
    reviewer: String,
    retired_at: DateTime<Utc>,
    source_review_record_json: Option<String>,
    excluded_accepted_tasks: Vec<String>,
    does_not_authorize: Vec<String>,
}

#[derive(Serialize)]
struct CloseoutReceiptArtifacts {
    closeout_plan_json: String,
    closeout_plan_markdown: Option<String>,
    cleanup_manifest_json: Option<String>,
    commercial_review_packet: Option<String>,
    return_package_markdown: String,
    review_record_json: String,
    review_file: Option<String>,
}

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
        OffdeskCommands::Deck(args) => deck(profile, args).await,
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
        DecisionCommands::IngestTelegram(args) => decision_ingest_telegram(profile, args).await,
        DecisionCommands::IngestTelegramFeedback(args) => {
            decision_ingest_telegram_feedback(profile, args).await
        }
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

async fn decision_ingest_telegram(profile: &str, args: DecisionIngestTelegramArgs) -> Result<()> {
    let profile_dir = match args.profile_dir.as_ref() {
        Some(path) => path.to_path_buf(),
        None => get_profile_dir(profile)?,
    };
    let ledger = DecisionLedger::new(&profile_dir);
    let request = read_json_file(&args.request)?;
    let result = read_json_file(&args.result)?;
    let seed_record = decision_record_from_request(&request, &args.request)?;
    let decision_id = seed_record.decision_id.clone();
    let mut appended_records = Vec::new();
    let mut record = if let Some(existing) = ledger.find(&decision_id)? {
        existing
    } else {
        ledger.append(&seed_record)?;
        appended_records.push(seed_record.status.as_str().to_string());
        seed_record
    };

    let telegram_status = json_string_field(&result, "status").unwrap_or_default();
    let telegram_decision = json_string_field(&result, "decision");
    let mut receipt_recorded = false;
    let mut skipped_reason = None;

    if telegram_status == "accepted" {
        let Some(decision) = telegram_decision
            .clone()
            .filter(|value| !value.trim().is_empty())
        else {
            bail!("accepted Telegram result is missing decision");
        };
        if record.status == DecisionStatus::Receipted {
            skipped_reason = Some("decision_already_receipted".to_string());
        } else if !decision_record_has_matching_handoff(&record, &decision) {
            let resolve_args = DecisionResolveArgs {
                decision_id: decision_id.clone(),
                decision,
                note: json_string_field(&result, "reason").unwrap_or_default(),
                by: args.by.clone(),
                target: args.target.clone(),
                json: false,
            };
            record = resolve_decision_record(record, &resolve_args)?;
            ledger.append(&record)?;
            appended_records.push(record.status.as_str().to_string());
        }

        if let Some(result_status) = args
            .receipt_result_status
            .as_deref()
            .map(str::trim)
            .filter(|status| !status.is_empty())
        {
            if record.status == DecisionStatus::Receipted {
                receipt_recorded = true;
            } else {
                let receipt_args = DecisionReceiptArgs {
                    decision_id: decision_id.clone(),
                    by: args.by.clone(),
                    result_status: result_status.to_string(),
                    evidence_summary: args.receipt_evidence_summary.clone(),
                    remaining_review: args.remaining_review.clone(),
                    json: false,
                };
                record = receipt_decision_record(record, &receipt_args)?;
                ledger.append(&record)?;
                appended_records.push(record.status.as_str().to_string());
                receipt_recorded = true;
            }
        }
    } else {
        skipped_reason = Some(format!(
            "telegram_result_status_{}",
            if telegram_status.is_empty() {
                "missing"
            } else {
                telegram_status.as_str()
            }
        ));
    }

    let report = DecisionIngestTelegramReport {
        request_path: args.request.display().to_string(),
        result_path: args.result.display().to_string(),
        ledger_path: ledger.path().display().to_string(),
        decision_id,
        telegram_status,
        telegram_decision,
        appended_records,
        receipt_recorded,
        skipped_reason,
        validation_issues: record.validation_issues(),
        record,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_decision_ingest_telegram_report(&report);
    Ok(())
}

async fn decision_ingest_telegram_feedback(
    profile: &str,
    args: DecisionIngestTelegramFeedbackArgs,
) -> Result<()> {
    let profile_dir = match args.profile_dir.as_ref() {
        Some(path) => path.to_path_buf(),
        None => get_profile_dir(profile)?,
    };
    let ledger = DecisionLedger::new(&profile_dir);
    let feedback = read_json_or_latest_jsonl_file(&args.feedback)?;
    let seed_record = decision_record_from_telegram_feedback(&feedback, &args.feedback, &args.by)?;
    let decision_id = seed_record.decision_id.clone();

    let (record, appended) = if let Some(existing) = ledger.find(&decision_id)? {
        (existing, false)
    } else {
        ledger.append(&seed_record)?;
        (seed_record, true)
    };

    let report = DecisionIngestTelegramFeedbackReport {
        feedback_path: args.feedback.display().to_string(),
        ledger_path: ledger.path().display().to_string(),
        decision_id,
        appended,
        validation_issues: record.validation_issues(),
        record,
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_decision_ingest_telegram_feedback_report(&report);
    Ok(())
}

fn resolve_decision_record(
    mut record: DecisionRecord,
    args: &DecisionResolveArgs,
) -> Result<DecisionRecord> {
    let decision = normalize_decision_choice(&args.decision);
    let note = crate::offdesk::operator_safe_text(args.note.trim());
    if decision_requires_note(&decision) && note.trim().is_empty() {
        bail!("decision `{decision}` requires --note with the bounded direction or blocker");
    }
    let by = crate::offdesk::operator_safe_text(args.by.trim());
    record.updated_at = Utc::now();
    record.trace_refs.push(DecisionTraceRef {
        kind: "decision_resolution".to_string(),
        label: by.clone(),
        reference: format!("choice={decision}"),
    });

    match decision.as_str() {
        "deny" => {
            record.status = DecisionStatus::Denied;
            record.execution_handoff = None;
        }
        "defer" => {
            record.status = DecisionStatus::Deferred;
            record.execution_handoff = None;
        }
        _ => {
            record.status = DecisionStatus::HandoffReady;
            record.execution_handoff =
                Some(build_execution_handoff(&record, &decision, &note, args));
        }
    }
    Ok(record)
}

fn receipt_decision_record(
    mut record: DecisionRecord,
    args: &DecisionReceiptArgs,
) -> Result<DecisionRecord> {
    let Some(handoff) = record.execution_handoff.as_ref() else {
        bail!(
            "decision {} has no execution_handoff to receipt",
            record.decision_id
        );
    };
    let resolved_at = Utc::now();
    let by = crate::offdesk::operator_safe_text(args.by.trim());
    let result_status = crate::offdesk::operator_safe_text(args.result_status.trim());
    let evidence_summary = args
        .evidence_summary
        .iter()
        .map(|line| crate::offdesk::operator_safe_text(line.trim()))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let remaining_review = args
        .remaining_review
        .iter()
        .map(|line| crate::offdesk::operator_safe_text(line.trim()))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let applied_handoff_id = handoff.handoff_id.clone();
    let final_decision = handoff.approved_direction.clone();

    record.updated_at = resolved_at;
    record.status = DecisionStatus::Receipted;
    record.decision_receipt = Some(DecisionReceipt {
        receipt_id: format!("receipt-{}", short_uuid()),
        decision_id: record.decision_id.clone(),
        resolved_by: by.clone(),
        resolved_at,
        final_decision,
        applied_handoff_id: Some(applied_handoff_id),
        authorization_summary: "Receipt closes the decision handoff; it does not authorize runtime mutation, cleanup, provider retargeting, or wiki promotion.".to_string(),
        evidence_summary,
        result_status: if result_status.is_empty() {
            "closed".to_string()
        } else {
            result_status
        },
        remaining_review,
    });
    record.trace_refs.push(DecisionTraceRef {
        kind: "decision_receipt".to_string(),
        label: by,
        reference: record
            .decision_receipt
            .as_ref()
            .map(|receipt| receipt.receipt_id.clone())
            .unwrap_or_default(),
    });
    Ok(record)
}

fn normalize_decision_choice(value: &str) -> String {
    let normalized = value.trim().to_lowercase().replace([' ', '-'], "_");
    match normalized.as_str() {
        "go" | "ok" | "okay" | "yes" | "proceed" => "continue".to_string(),
        "retry" | "redo" => "revise".to_string(),
        "hold" => "block".to_string(),
        "cancel" | "abort" => "stop".to_string(),
        other => other.to_string(),
    }
}

fn decision_requires_note(decision: &str) -> bool {
    matches!(
        decision,
        "revise" | "block" | "custom" | "custom_direction" | "other"
    )
}

fn build_execution_handoff(
    record: &DecisionRecord,
    decision: &str,
    note: &str,
    args: &DecisionResolveArgs,
) -> ExecutionHandoff {
    let mut instructions = vec![format!("Operator selected `{decision}` for this decision.")];
    if !note.trim().is_empty() {
        instructions.push(format!("Operator note: {note}"));
    }
    instructions.push("Before execution, read the decision request, Council review, and approval brief projection.".to_string());

    let non_authorized_actions = record.decision_request.non_authorized_scope.clone();
    let constraints = non_authorized_actions
        .iter()
        .map(|scope| format!("This handoff does not authorize {scope}."))
        .collect::<Vec<_>>();

    ExecutionHandoff {
        handoff_id: format!("handoff-{}", short_uuid()),
        decision_id: record.decision_id.clone(),
        target: args
            .target
            .as_deref()
            .map(crate::offdesk::operator_safe_text)
            .filter(|target| !target.trim().is_empty())
            .unwrap_or_else(|| default_decision_handoff_target(decision).to_string()),
        approved_direction: decision.to_string(),
        approved_scope: record.decision_request.current_scope.clone(),
        instructions,
        constraints,
        verification_required: vec![
            "Record a decision receipt before treating this handoff as accepted.".to_string(),
            "Use separate approvals for runtime mutation, cleanup, provider retargeting, or wiki promotion.".to_string(),
        ],
        non_authorized_actions,
    }
}

fn default_decision_handoff_target(decision: &str) -> &'static str {
    match decision {
        "stop" => "closeout",
        _ => "agent",
    }
}

fn read_json_file(path: &Path) -> Result<Value> {
    serde_json::from_str(
        &fs::read_to_string(path).with_context(|| format!("read JSON {}", path.display()))?,
    )
    .with_context(|| format!("parse JSON {}", path.display()))
}

fn read_json_or_latest_jsonl_file(path: &Path) -> Result<Value> {
    let content =
        fs::read_to_string(path).with_context(|| format!("read JSON {}", path.display()))?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        bail!("JSON file is empty: {}", path.display());
    }
    match serde_json::from_str(trimmed) {
        Ok(value) => Ok(value),
        Err(full_error) => {
            let Some(line) = content
                .lines()
                .rev()
                .map(str::trim)
                .find(|line| !line.is_empty())
            else {
                bail!("JSON file is empty: {}", path.display());
            };
            if line == trimmed {
                Err(full_error).with_context(|| format!("parse JSON {}", path.display()))
            } else {
                serde_json::from_str(line)
                    .with_context(|| format!("parse latest JSONL row {}", path.display()))
            }
        }
    }
}

fn decision_record_from_request(request: &Value, request_path: &Path) -> Result<DecisionRecord> {
    let Some(record) = request.get("decision_record").cloned() else {
        bail!(
            "Telegram request {} does not contain decision_record",
            request_path.display()
        );
    };
    serde_json::from_value(record).with_context(|| {
        format!(
            "parse decision_record from Telegram request {}",
            request_path.display()
        )
    })
}

fn decision_record_from_telegram_feedback(
    feedback: &Value,
    feedback_path: &Path,
    by: &str,
) -> Result<DecisionRecord> {
    let schema = json_string_field(feedback, "schema").unwrap_or_default();
    if schema != "remote_operator_telegram_feedback.v1" {
        bail!(
            "Telegram feedback {} has unsupported schema `{}`",
            feedback_path.display(),
            if schema.is_empty() {
                "missing"
            } else {
                schema.as_str()
            }
        );
    }

    let id_material = serde_json::json!({
        "schema": feedback.get("schema"),
        "profile": feedback.get("profile"),
        "chat_id_hash": feedback.get("chat_id_hash"),
        "user_id_hash": feedback.get("user_id_hash"),
        "message_id": feedback.get("message_id"),
        "feedback_text": feedback.get("feedback_text"),
        "target_chat_id_hash": feedback.get("target_chat_id_hash"),
        "feedback_context": feedback.get("feedback_context"),
    });
    let canonical_feedback =
        serde_json::to_vec(&id_material).context("serialize Telegram feedback for decision id")?;
    let feedback_hash = sha256_hex(&canonical_feedback);
    let hash_prefix = &feedback_hash[..16];
    let decision_id = format!("telegram-feedback-{hash_prefix}");
    let received_at = json_string_field(feedback, "received_at")
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);
    let actor = safe_nonempty(by).unwrap_or_else(|| "telegram".to_string());
    let feedback_text = safe_nonempty(
        json_string_field(feedback, "feedback_text")
            .as_deref()
            .unwrap_or(""),
    )
    .unwrap_or_else(|| "(empty feedback)".to_string());
    let feedback_kind = json_string_field(feedback, "feedback_kind")
        .and_then(|value| safe_nonempty(&value))
        .unwrap_or_else(|| classify_telegram_feedback_kind(&feedback_text).to_string());
    let is_planning_request = feedback_kind == "planning_request";
    let feedback_excerpt = truncate_chars(&feedback_text, 240);
    let project_key = feedback_context_string(feedback, "project_key")
        .or_else(|| json_string_field(feedback, "profile").and_then(|value| safe_nonempty(&value)))
        .unwrap_or_else(|| "remote-operator-feedback".to_string());
    let message_id = feedback_message_id(feedback);
    let request_id = feedback_context_string(feedback, "request_id")
        .or_else(|| {
            message_id
                .as_ref()
                .map(|id| format!("telegram-message-{id}"))
        })
        .unwrap_or_else(|| format!("telegram-feedback-{hash_prefix}"));
    let task_id = feedback_context_string(feedback, "task_id")
        .or_else(|| feedback_context_string(feedback, "focus_ref"))
        .unwrap_or_else(|| {
            if is_planning_request {
                "telegram-plan-request".to_string()
            } else {
                "telegram-feedback".to_string()
            }
        });
    let focus_kind = feedback_context_string(feedback, "focus_kind");
    let context_kind = feedback_context_string(feedback, "context_kind");
    let focus_ref = feedback_context_string(feedback, "focus_ref");
    let context_label = feedback_context_string(feedback, "focus_label")
        .or_else(|| focus_ref.clone())
        .or_else(|| context_kind.clone());
    let materiality = if is_planning_request {
        DecisionMateriality::Medium
    } else {
        feedback_materiality(context_kind.as_deref(), focus_kind.as_deref())
    };

    let mut evidence_refs = vec![DecisionTraceRef {
        kind: "telegram_feedback".to_string(),
        label: "feedback_file".to_string(),
        reference: feedback_path.display().to_string(),
    }];
    if let Some(id) = message_id.as_deref() {
        evidence_refs.push(DecisionTraceRef {
            kind: "telegram_message".to_string(),
            label: "message_id".to_string(),
            reference: id.to_string(),
        });
    }
    if let Some(focus) = focus_ref.as_deref() {
        evidence_refs.push(DecisionTraceRef {
            kind: "telegram_context".to_string(),
            label: focus_kind.clone().unwrap_or_else(|| "focus".to_string()),
            reference: focus.to_string(),
        });
    }

    let mut why_now = vec![
        if is_planning_request {
            "The remote operator sent a Telegram planning request.".to_string()
        } else {
            "The remote operator sent freeform Telegram feedback.".to_string()
        },
        if is_planning_request {
            "Telegram planning requests are captured for Plan Mode review; they do not start autonomous work by themselves.".to_string()
        } else {
            "Freeform feedback is review input only; it does not authorize runtime mutation or approval resolution.".to_string()
        },
    ];
    if let Some(label) = context_label.as_deref() {
        why_now.push(format!("Referenced context: {label}."));
    }

    let non_authorized_scope = vec![
        "runtime mutation".to_string(),
        "approval resolution".to_string(),
        "background dispatch".to_string(),
        "provider retargeting".to_string(),
        "cleanup or deletion".to_string(),
        "git commit or push".to_string(),
    ];

    let options = vec![
        DecisionOption {
            id: if is_planning_request {
                "plan".to_string()
            } else {
                "revise".to_string()
            },
            label: if is_planning_request {
                "Create plan candidate".to_string()
            } else {
                "Revise next step".to_string()
            },
            description: if is_planning_request {
                "Turn this Telegram request into a bounded Offdesk planning candidate for local review."
                    .to_string()
            } else {
                "Use this feedback to revise the referenced plan, approval review, or handoff direction."
                    .to_string()
            },
            impact: Some(if is_planning_request {
                "Creates a handoff-ready decision for plan drafting; execution still needs normal approval gates."
                        .to_string()
            } else {
                "Creates a handoff-ready decision that still needs an explicit receipt after review."
                        .to_string()
            }),
            natural_input_prompt: Some(if is_planning_request {
                "Describe the project, goal, timebox, and constraints for the plan candidate."
                    .to_string()
            } else {
                "Describe the bounded revision to make.".to_string()
            }),
        },
        DecisionOption {
            id: "defer".to_string(),
            label: "Keep open".to_string(),
            description: "Leave the feedback in the decision inbox for later review.".to_string(),
            impact: Some("No runtime or plan state changes are authorized.".to_string()),
            natural_input_prompt: Some("State what evidence or timing is missing.".to_string()),
        },
        DecisionOption {
            id: "deny".to_string(),
            label: "Not actionable".to_string(),
            description: "Close the feedback as reviewed but not actionable.".to_string(),
            impact: Some("The inbox item is denied without an execution handoff.".to_string()),
            natural_input_prompt: Some(
                "State why the feedback does not change the current direction.".to_string(),
            ),
        },
    ];
    let approval_options = options
        .iter()
        .map(|option| ApprovalBriefOption {
            id: option.id.clone(),
            label: option.label.clone(),
            description: option.description.clone(),
            natural_input_prompt: option.natural_input_prompt.clone(),
        })
        .collect::<Vec<_>>();
    let mut decision_impacts = HashMap::new();
    decision_impacts.insert(
        if is_planning_request {
            "plan".to_string()
        } else {
            "revise".to_string()
        },
        if is_planning_request {
            "Reviewers may create a bounded plan candidate; execution still needs normal plan review, launch prep, and gate approval.".to_string()
        } else {
            "Reviewers may revise the bounded plan or handoff direction; execution still needs the normal handoff and receipt.".to_string()
        },
    );
    decision_impacts.insert(
        "defer".to_string(),
        "The feedback remains visible in the decision inbox with no state mutation.".to_string(),
    );
    decision_impacts.insert(
        "deny".to_string(),
        "The feedback is marked reviewed and not actionable.".to_string(),
    );
    let mut approval_context = HashMap::new();
    if let Some(value) = context_kind.as_deref() {
        approval_context.insert("context_kind".to_string(), value.to_string());
    }
    if let Some(value) = focus_kind.as_deref() {
        approval_context.insert("focus_kind".to_string(), value.to_string());
    }
    if let Some(value) = focus_ref.as_deref() {
        approval_context.insert("focus_ref".to_string(), value.to_string());
    }

    // The subject is what the operator sees in decision lists; it must carry
    // the feedback itself, not the interaction-context label active at send
    // time (which produced rows like "Telegram feedback: 처리할 항목 없음").
    let subject_excerpt = truncate_chars(&feedback_text, 60);
    let subject = if is_planning_request {
        format!("Telegram planning request: {subject_excerpt}")
    } else {
        format!("Telegram feedback: {subject_excerpt}")
    };
    let current_scope = if is_planning_request {
        "Review this Telegram planning request and, if appropriate, turn it into a bounded Offdesk plan candidate. This decision does not execute work by itself.".to_string()
    } else {
        "Review and classify this feedback for the referenced Offdesk context only. This decision does not execute work by itself.".to_string()
    };
    let source_surface = if is_planning_request {
        "telegram.remote_operator.plan_request"
    } else {
        "telegram.remote_operator.feedback"
    };

    Ok(DecisionRecord {
        schema: DECISION_RECORD_SCHEMA.to_string(),
        decision_id,
        project_key,
        request_id,
        task_id,
        raised_by: DecisionRaisedBy::Operator,
        source_surface: source_surface.to_string(),
        materiality,
        status: DecisionStatus::UserPending,
        created_at: received_at,
        updated_at: received_at,
        decision_request: DecisionRequest {
            kind: if is_planning_request {
                "telegram_operator_plan_request".to_string()
            } else {
                "telegram_operator_feedback".to_string()
            },
            summary: if is_planning_request {
                format!("Telegram planning request: {feedback_excerpt}")
            } else {
                format!("Telegram feedback: {feedback_excerpt}")
            },
            decision_needed: if is_planning_request {
                "Decide whether to create a bounded Offdesk plan candidate from this Telegram request."
                    .to_string()
            } else {
                "Decide whether the feedback changes the referenced plan, approval review, or next Offdesk handoff."
                    .to_string()
            },
            why_now,
            current_scope: current_scope.clone(),
            non_authorized_scope: non_authorized_scope.clone(),
            options,
            evidence_refs: evidence_refs.clone(),
            trace_refs: evidence_refs.clone(),
        },
        council_review: None,
        judgment_route: Some(JudgmentRoute {
            schema: JUDGMENT_ROUTE_SCHEMA.to_string(),
            evaluator: JudgmentEvaluator::DeterministicGate,
            reason:
                if is_planning_request {
                    "Telegram planning text is captured as a planning request, not as runtime authority."
                } else {
                    "Telegram freeform text is operator feedback, so the adapter may only promote it into a reviewable decision inbox item."
                }
                    .to_string(),
            policy_basis: vec![
                "Remote operator transport is read-only.".to_string(),
                if is_planning_request {
                    "Telegram planning requests require local Plan Mode review before any work starts."
                        .to_string()
                } else {
                    "Freeform Telegram text is not an approval or execution command.".to_string()
                },
            ],
            evidence_refs: evidence_refs.clone(),
            selected_by: actor.clone(),
            selected_at: received_at,
            default_if_no_reply: Some("defer".to_string()),
        }),
        route: Some(DecisionRoute {
            materiality,
            target: DecisionRouteTarget::User,
            reason:
                if is_planning_request {
                    "Human review is required before a Telegram planning request becomes a plan candidate."
                } else {
                    "Human review is required before feedback can change a plan, approval, or workload direction."
                }
                    .to_string(),
            policy_basis: vec![
                if is_planning_request {
                    "Planning requests are captured as intent, not authority.".to_string()
                } else {
                    "Feedback is captured as input, not authority.".to_string()
                },
                "Existing decision resolve/receipt commands must close the loop.".to_string(),
            ],
            default_if_no_reply: Some("defer".to_string()),
            expires_at: None,
        }),
        approval_brief: Some(ApprovalBrief {
            schema: "approval_brief.v1".to_string(),
            source: Some(source_surface.to_string()),
            recommendation: if is_planning_request {
                "plan".to_string()
            } else {
                "revise".to_string()
            },
            subject,
            summary_lines: vec![
                if is_planning_request {
                    format!("Planning request: {feedback_excerpt}")
                } else {
                    format!("Feedback: {feedback_excerpt}")
                },
                if is_planning_request {
                    "This request was captured for plan drafting only; no work has started."
                        .to_string()
                } else {
                    "This message was promoted to the decision inbox for review only.".to_string()
                },
            ],
            judgment_route_summary: Some(
                if is_planning_request {
                    "판단 경로: Telegram planning request - deterministic promotion to planning inbox, no runtime authority.".to_string()
                } else {
                    "판단 경로: Telegram freeform feedback - deterministic promotion to review inbox, no runtime authority.".to_string()
                },
            ),
            evidence_sufficiency: Some(
                if is_planning_request {
                    "The request text is captured; plan creation and execution still need explicit local review."
                        .to_string()
                } else {
                    "The feedback text and last Telegram interaction context are captured; further action needs explicit review."
                        .to_string()
                },
            ),
            default_if_no_reply: Some("defer".to_string()),
            scope: current_scope,
            question: if is_planning_request {
                "Should this Telegram request become a bounded Offdesk plan candidate?".to_string()
            } else {
                "How should this Telegram feedback be handled?".to_string()
            },
            options: approval_options,
            why_recommendation: vec![
                if is_planning_request {
                    "The message explicitly asks whether autonomous work can be planned."
                        .to_string()
                } else {
                    "Freeform feedback often indicates a needed plan or review adjustment."
                        .to_string()
                },
                if is_planning_request {
                    "The safest next step is a bounded plan candidate, not immediate execution."
                        .to_string()
                } else {
                    "The safest default is to revise only after a bounded review decision."
                        .to_string()
                },
            ],
            evidence: evidence_refs
                .iter()
                .map(|reference| format!("{}: {}", reference.label, reference.reference))
                .collect(),
            decision_impacts,
            reply_examples: vec![
                if is_planning_request {
                    "plan: draft a bounded plan for the requested project and timebox".to_string()
                } else {
                    "revise: tighten the next plan around the missing mobile UX evidence"
                        .to_string()
                },
                "defer: wait until the morning review".to_string(),
                "deny: no change needed because this is already covered".to_string(),
            ],
            context: approval_context,
        }),
        execution_handoff: None,
        decision_receipt: None,
        trace_refs: evidence_refs,
    })
}

fn json_string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
}

fn feedback_context_string(feedback: &Value, field: &str) -> Option<String> {
    feedback
        .get("feedback_context")
        .and_then(Value::as_object)
        .and_then(|context| context.get(field))
        .and_then(Value::as_str)
        .and_then(safe_nonempty)
}

fn feedback_message_id(feedback: &Value) -> Option<String> {
    match feedback.get("message_id") {
        Some(Value::Number(value)) => Some(value.to_string()),
        Some(Value::String(value)) => safe_nonempty(value),
        _ => None,
    }
}

fn classify_telegram_feedback_kind(text: &str) -> &'static str {
    let normalized = text.trim().to_lowercase();
    if [
        "자율주행",
        "계획",
        "plan",
        "offdesk",
        "진행",
        "처리",
        "검토해볼까",
        "시작",
        "맡기",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        "planning_request"
    } else {
        "freeform_feedback"
    }
}

fn feedback_materiality(
    context_kind: Option<&str>,
    focus_kind: Option<&str>,
) -> DecisionMateriality {
    let context_kind = context_kind.unwrap_or_default();
    let focus_kind = focus_kind.unwrap_or_default();
    if matches!(focus_kind, "approval" | "plan" | "decision")
        || context_kind.contains("attention")
        || context_kind.contains("pending")
    {
        DecisionMateriality::Medium
    } else {
        DecisionMateriality::Low
    }
}

fn safe_nonempty(value: &str) -> Option<String> {
    let safe = operator_safe_text(value.trim());
    if safe.trim().is_empty() {
        None
    } else {
        Some(safe)
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...<truncated>")
    } else {
        truncated
    }
}

fn decision_record_has_matching_handoff(record: &DecisionRecord, decision: &str) -> bool {
    record
        .execution_handoff
        .as_ref()
        .map(|handoff| handoff.approved_direction == normalize_decision_choice(decision))
        .unwrap_or(false)
}

fn print_decision_ingest_telegram_report(report: &DecisionIngestTelegramReport) {
    println!("Decision: {}", report.decision_id);
    println!("Telegram status: {}", report.telegram_status);
    if let Some(decision) = report.telegram_decision.as_deref() {
        println!("Telegram decision: {}", decision);
    }
    println!("Ledger: {}", report.ledger_path);
    if report.appended_records.is_empty() {
        println!("Appended: none");
    } else {
        println!("Appended: {}", report.appended_records.join(", "));
    }
    if report.receipt_recorded {
        println!("Receipt: recorded");
    }
    if let Some(reason) = report.skipped_reason.as_deref() {
        println!("Skipped: {}", reason);
    }
    if !report.validation_issues.is_empty() {
        println!("Validation issues: {}", report.validation_issues.len());
    }
}

fn print_decision_ingest_telegram_feedback_report(report: &DecisionIngestTelegramFeedbackReport) {
    println!("Decision: {}", report.decision_id);
    println!("Feedback: {}", report.feedback_path);
    println!("Ledger: {}", report.ledger_path);
    println!("Appended: {}", if report.appended { "yes" } else { "no" });
    println!("Status: {}", report.record.status.as_str());
    if !report.validation_issues.is_empty() {
        println!("Validation issues: {}", report.validation_issues.len());
    }
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

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&events)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    if events.is_empty() {
        println!("No adaptive wiki proposal lifecycle events found.");
        return Ok(());
    }
    for event in events {
        println!(
            "{} {:?} proposal={} action={} subject={}:{} by={} {}",
            event.id,
            event.decision,
            event.proposal_id,
            event
                .proposal_action
                .map(|action| format!("{action:?}"))
                .unwrap_or_else(|| "-".to_string()),
            empty_dash(&event.subject_kind),
            empty_dash(&event.subject_id),
            empty_dash(&event.actor),
            crate::offdesk::operator_safe_text(&event.reason)
        );
    }
    Ok(())
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

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&event)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    println!(
        "Recorded adaptive wiki proposal event {} for {} ({:?})",
        event.id, event.proposal_id, event.decision
    );
    Ok(())
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

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&event)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    println!(
        "Recorded adaptive wiki proposal event {} for {} ({:?})",
        event.id, event.proposal_id, event.decision
    );
    Ok(())
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

async fn wiki_proposal_handoff(profile: &str, args: WikiProposalHandoffArgs) -> Result<()> {
    require_non_empty_arg("proposal_id", &args.proposal_id)?;
    let report = wiki_store(profile)?.generate_review_report_filtered(
        true,
        Utc::now(),
        AdaptiveWikiReviewQueueFilter::All,
    )?;
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
    let preview = wiki_proposal_handoff_preview(proposal, &args);

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&preview)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    print_wiki_proposal_handoff(&preview);
    Ok(())
}

async fn wiki_proposal_receipt(profile: &str, args: WikiProposalReceiptArgs) -> Result<()> {
    let proposal_id = require_non_empty_arg("proposal_id", &args.proposal_id)?;
    let audit_id = require_non_empty_arg("--audit-id", &args.audit_id)?;
    let event_id = require_non_empty_arg("--event-id", &args.event_id)?;
    let command = require_non_empty_arg("--command", &args.command)?;
    let now = Utc::now();
    let store = wiki_store(profile)?;
    let report =
        store.generate_review_report_filtered(true, now, AdaptiveWikiReviewQueueFilter::All)?;
    let audits = store.load_audit_records()?;
    let events = store.load_review_proposal_events()?;
    let audit = audits.into_iter().find(|audit| audit.id == audit_id);
    let event = events.into_iter().find(|event| event.id == event_id);
    let current_proposal = report
        .proposals
        .iter()
        .find(|proposal| proposal.id == proposal_id);
    let subject = wiki_proposal_receipt_subject(proposal_id, current_proposal, event.as_ref());
    let safe_command = crate::offdesk::operator_safe_text(command);

    let mut checks = Vec::new();
    checks.push(wiki_proposal_receipt_check(
        "preview_command_supplied",
        !safe_command.is_empty(),
        "preview command is present",
    ));
    checks.push(wiki_proposal_receipt_check(
        "audit_found",
        audit.is_some(),
        audit.as_ref().map_or_else(
            || format!("audit id {audit_id} was not found"),
            |audit| format!("found {}", audit_receipt_summary(audit)),
        ),
    ));
    checks.push(wiki_proposal_receipt_check(
        "event_found",
        event.is_some(),
        event.as_ref().map_or_else(
            || format!("event id {event_id} was not found"),
            |event| format!("found {}", event_receipt_summary(event)),
        ),
    ));
    let event_matches = event
        .as_ref()
        .is_some_and(|event| event_matches_receipt_subject(&subject, event));
    checks.push(wiki_proposal_receipt_check(
        "event_matches_proposal",
        event_matches,
        event.as_ref().map_or_else(
            || "event metadata unavailable because event was not found".to_string(),
            |event| {
                receipt_match_detail(
                    event_matches,
                    "event",
                    event_receipt_summary(event),
                    receipt_subject_summary(&subject),
                )
            },
        ),
    ));
    let audit_matches = audit
        .as_ref()
        .is_some_and(|audit| audit_matches_receipt_subject(&subject, audit));
    checks.push(wiki_proposal_receipt_check(
        "audit_matches_proposal",
        audit_matches,
        audit.as_ref().map_or_else(
            || "audit metadata unavailable because audit was not found".to_string(),
            |audit| {
                receipt_match_detail(
                    audit_matches,
                    "audit",
                    audit_receipt_summary(audit),
                    receipt_subject_summary(&subject),
                )
            },
        ),
    ));
    let audit_event_aligned = audit
        .as_ref()
        .zip(event.as_ref())
        .is_some_and(|(audit, event)| audit_event_targets_align(audit, event, &subject));
    checks.push(wiki_proposal_receipt_check(
        "audit_event_target_alignment",
        audit_event_aligned,
        audit.as_ref().zip(event.as_ref()).map_or_else(
            || "audit/event alignment unavailable because audit or event was not found".to_string(),
            |(audit, event)| {
                receipt_match_detail(
                    audit_event_aligned,
                    "audit/event",
                    audit_receipt_summary(audit),
                    event_receipt_summary(event),
                )
            },
        ),
    ));

    let blockers = checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| check.detail.clone())
        .collect::<Vec<_>>();
    let receipt = WikiProposalReceipt {
        generated_at: now,
        read_only: true,
        status: if blockers.is_empty() {
            "linked"
        } else {
            "incomplete"
        },
        proposal: subject,
        preview_command_sha256: sha256_hex(safe_command.as_bytes()),
        preview_command: safe_command,
        audit,
        event,
        checks,
        blockers,
    };
    let export = if args.export || args.output.is_some() {
        Some(write_wiki_proposal_receipt_export(
            profile,
            &receipt,
            args.output.as_ref(),
        )?)
    } else {
        None
    };

    if args.json {
        let value = if let Some(export) = export.as_ref() {
            operator_safe_json_value(serde_json::to_value(WikiProposalReceiptExportReceipt {
                exported_to: export.path.to_string_lossy().to_string(),
                bytes_written: export.bytes_written,
                receipt: &receipt,
            })?)
        } else {
            operator_safe_json_value(serde_json::to_value(&receipt)?)
        };
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    print_wiki_proposal_receipt(&receipt);
    if let Some(export) = export.as_ref() {
        println!(
            "  exported_to: {}",
            crate::offdesk::operator_safe_text(export.path.to_string_lossy().as_ref())
        );
        println!("  bytes_written: {}", export.bytes_written);
    }
    Ok(())
}

fn wiki_proposal_receipt_subject(
    proposal_id: &str,
    current_proposal: Option<&AdaptiveWikiReviewProposal>,
    event: Option<&AdaptiveWikiReviewProposalEventRecord>,
) -> WikiProposalReceiptSubject {
    if let Some(proposal) = current_proposal {
        return WikiProposalReceiptSubject {
            proposal_id: crate::offdesk::operator_safe_text(proposal_id),
            current: true,
            action: Some(proposal.action),
            subject_kind: crate::offdesk::operator_safe_text(&proposal.subject_kind),
            subject_id: crate::offdesk::operator_safe_text(&proposal.subject_id),
            lifecycle_decision: proposal
                .lifecycle
                .as_ref()
                .map(|lifecycle| lifecycle.decision),
            lifecycle_event_id: proposal
                .lifecycle
                .as_ref()
                .map(|lifecycle| crate::offdesk::operator_safe_text(&lifecycle.latest_event_id)),
            evidence_refs: proposal
                .evidence_refs
                .iter()
                .map(|value| crate::offdesk::operator_safe_text(value))
                .collect(),
        };
    }

    if let Some(event) = event.filter(|event| event.proposal_id == proposal_id) {
        return WikiProposalReceiptSubject {
            proposal_id: crate::offdesk::operator_safe_text(proposal_id),
            current: false,
            action: event.proposal_action,
            subject_kind: crate::offdesk::operator_safe_text(&event.subject_kind),
            subject_id: crate::offdesk::operator_safe_text(&event.subject_id),
            lifecycle_decision: Some(event.decision),
            lifecycle_event_id: Some(crate::offdesk::operator_safe_text(&event.id)),
            evidence_refs: event
                .evidence_refs
                .iter()
                .map(|value| crate::offdesk::operator_safe_text(value))
                .collect(),
        };
    }

    WikiProposalReceiptSubject {
        proposal_id: crate::offdesk::operator_safe_text(proposal_id),
        current: false,
        action: None,
        subject_kind: String::new(),
        subject_id: String::new(),
        lifecycle_decision: None,
        lifecycle_event_id: None,
        evidence_refs: Vec::new(),
    }
}

fn wiki_proposal_receipt_check(
    name: &'static str,
    passed: bool,
    detail: impl Into<String>,
) -> WikiProposalReceiptCheck {
    WikiProposalReceiptCheck {
        name,
        passed,
        detail: crate::offdesk::operator_safe_text(&detail.into()),
    }
}

fn receipt_match_detail(passed: bool, label: &str, actual: String, expected: String) -> String {
    if passed {
        format!("{label} matches {expected}")
    } else {
        format!("{label} mismatch: actual {actual}; expected {expected}")
    }
}

fn receipt_subject_summary(subject: &WikiProposalReceiptSubject) -> String {
    format!(
        "proposal={} action={} subject={}:{}",
        empty_dash(&subject.proposal_id),
        subject
            .action
            .map(|action| format!("{action:?}"))
            .unwrap_or_else(|| "-".to_string()),
        empty_dash(&subject.subject_kind),
        empty_dash(&subject.subject_id)
    )
}

fn event_receipt_summary(event: &AdaptiveWikiReviewProposalEventRecord) -> String {
    format!(
        "event={} decision={:?} proposal={} action={} subject={}:{}",
        event.id,
        event.decision,
        empty_dash(&event.proposal_id),
        event
            .proposal_action
            .map(|action| format!("{action:?}"))
            .unwrap_or_else(|| "-".to_string()),
        empty_dash(&event.subject_kind),
        empty_dash(&event.subject_id)
    )
}

fn audit_receipt_summary(audit: &AdaptiveWikiAuditRecord) -> String {
    format!(
        "audit={} action={:?} subject={} candidate={} entry={}",
        audit.id,
        audit.action,
        empty_dash(&audit.subject_id),
        audit.candidate_id.as_deref().unwrap_or("-"),
        audit.entry_id.as_deref().unwrap_or("-")
    )
}

fn event_matches_receipt_subject(
    subject: &WikiProposalReceiptSubject,
    event: &AdaptiveWikiReviewProposalEventRecord,
) -> bool {
    if event.proposal_id != subject.proposal_id
        || event.decision == AdaptiveWikiReviewProposalDecision::Unknown
    {
        return false;
    }
    if let (Some(subject_action), Some(event_action)) = (subject.action, event.proposal_action) {
        if subject_action != event_action {
            return false;
        }
    }
    if !subject.subject_kind.is_empty()
        && !event.subject_kind.is_empty()
        && subject.subject_kind != event.subject_kind
    {
        return false;
    }
    if !subject.subject_id.is_empty()
        && !event.subject_id.is_empty()
        && subject.subject_id != event.subject_id
    {
        return false;
    }
    true
}

fn audit_matches_receipt_subject(
    subject: &WikiProposalReceiptSubject,
    audit: &AdaptiveWikiAuditRecord,
) -> bool {
    let Some(action) = subject.action else {
        return false;
    };
    if subject.subject_id.is_empty() || subject.subject_kind.is_empty() {
        return false;
    }
    match (action, subject.subject_kind.as_str()) {
        (AdaptiveWikiReviewProposalAction::Promote, "candidate") => {
            audit.action == AdaptiveWikiAuditAction::Promote
                && audit_targets_id(audit, &subject.subject_id)
        }
        (AdaptiveWikiReviewProposalAction::Reject, "candidate") => {
            audit.action == AdaptiveWikiAuditAction::Reject
                && audit_targets_id(audit, &subject.subject_id)
        }
        (AdaptiveWikiReviewProposalAction::Rescope, "entry") => {
            audit.action == AdaptiveWikiAuditAction::Rescope
                && audit_targets_id(audit, &subject.subject_id)
        }
        (AdaptiveWikiReviewProposalAction::Deprecate, "entry") => {
            audit.action == AdaptiveWikiAuditAction::Deprecate
                && audit_targets_id(audit, &subject.subject_id)
        }
        (AdaptiveWikiReviewProposalAction::AddCounterexample, "entry") => {
            audit.action == AdaptiveWikiAuditAction::AddCounterexample
                && audit_targets_id(audit, &subject.subject_id)
        }
        (AdaptiveWikiReviewProposalAction::RenewReview, "entry") => {
            matches!(
                audit.action,
                AdaptiveWikiAuditAction::RenewReviewAfter
                    | AdaptiveWikiAuditAction::Rescope
                    | AdaptiveWikiAuditAction::Deprecate
                    | AdaptiveWikiAuditAction::AddCounterexample
            ) && audit_targets_id(audit, &subject.subject_id)
        }
        (AdaptiveWikiReviewProposalAction::Split, "entry") => {
            matches!(
                audit.action,
                AdaptiveWikiAuditAction::Rescope | AdaptiveWikiAuditAction::AddCounterexample
            ) && audit_targets_id(audit, &subject.subject_id)
        }
        (AdaptiveWikiReviewProposalAction::Merge, "entry") => {
            audit.action == AdaptiveWikiAuditAction::Deprecate
                && (audit_targets_id(audit, &subject.subject_id) || {
                    let target = audit_primary_target_id(audit);
                    let target_ref = format!("entry:{target}");
                    !target.is_empty()
                        && subject
                            .evidence_refs
                            .iter()
                            .any(|evidence_ref| evidence_ref == &target_ref)
                })
        }
        _ => false,
    }
}

fn audit_event_targets_align(
    audit: &AdaptiveWikiAuditRecord,
    event: &AdaptiveWikiReviewProposalEventRecord,
    subject: &WikiProposalReceiptSubject,
) -> bool {
    if !event.subject_id.is_empty() && audit_targets_id(audit, &event.subject_id) {
        return true;
    }
    if event.proposal_action == Some(AdaptiveWikiReviewProposalAction::Merge)
        && audit.action == AdaptiveWikiAuditAction::Deprecate
    {
        let target = audit_primary_target_id(audit);
        let target_ref = format!("entry:{target}");
        return !target.is_empty()
            && event
                .evidence_refs
                .iter()
                .chain(subject.evidence_refs.iter())
                .any(|evidence_ref| evidence_ref == &target_ref);
    }
    false
}

fn audit_targets_id(audit: &AdaptiveWikiAuditRecord, id: &str) -> bool {
    audit.subject_id == id
        || audit.candidate_id.as_deref() == Some(id)
        || audit.entry_id.as_deref() == Some(id)
}

fn audit_primary_target_id(audit: &AdaptiveWikiAuditRecord) -> &str {
    audit
        .entry_id
        .as_deref()
        .or(audit.candidate_id.as_deref())
        .unwrap_or(audit.subject_id.as_str())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn write_wiki_proposal_receipt_export(
    profile: &str,
    receipt: &WikiProposalReceipt,
    output: Option<&PathBuf>,
) -> Result<WikiProposalReceiptExport> {
    let bytes = serde_json::to_vec_pretty(receipt)?;

    if let Some(path) = output {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).with_context(|| {
                format!(
                    "create adaptive wiki proposal receipt export directory {}",
                    parent.display()
                )
            })?;
        }
        let bytes_written = write_new_file(path, &bytes).with_context(|| {
            format!(
                "write adaptive wiki proposal receipt export {}",
                path.display()
            )
        })?;
        return Ok(WikiProposalReceiptExport {
            path: path.clone(),
            bytes_written,
        });
    }

    let export_dir = read_only_profile_dir(profile)?.join("adaptive_wiki_proposal_receipts");
    fs::create_dir_all(&export_dir).with_context(|| {
        format!(
            "create adaptive wiki proposal receipt export directory {}",
            export_dir.display()
        )
    })?;
    let timestamp = receipt.generated_at.format("%Y%m%dT%H%M%SZ");
    let proposal_id = receipt.proposal.proposal_id.replace(['/', '\\', ':'], "_");
    for attempt in 0..1000 {
        let filename = if attempt == 0 {
            format!("adaptive_wiki_proposal_receipt_{timestamp}_{proposal_id}.json")
        } else {
            format!("adaptive_wiki_proposal_receipt_{timestamp}_{proposal_id}_{attempt:03}.json")
        };
        let path = export_dir.join(filename);
        match write_new_file(&path, &bytes) {
            Ok(bytes_written) => {
                return Ok(WikiProposalReceiptExport {
                    path,
                    bytes_written,
                })
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "write adaptive wiki proposal receipt export {}",
                        path.display()
                    )
                });
            }
        }
    }

    bail!(
        "could not allocate adaptive wiki proposal receipt export path in {}",
        export_dir.display()
    )
}

fn wiki_proposal_handoff_preview(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> WikiProposalHandoffPreview {
    let (required_inputs, mutation_options) = manual_handoff_contract(proposal);
    let lifecycle_decision = proposal
        .lifecycle
        .as_ref()
        .map(|lifecycle| lifecycle.decision);
    let lifecycle_stale = proposal
        .lifecycle
        .as_ref()
        .is_some_and(|lifecycle| lifecycle.stale);
    if proposal_has_non_stale_decision(proposal) {
        return WikiProposalHandoffPreview {
            proposal_id: proposal.id.clone(),
            action: proposal.action,
            subject_kind: proposal.subject_kind.clone(),
            subject_id: proposal.subject_id.clone(),
            status: "blocked_by_decision",
            command: None,
            reason: "The proposal already has a non-stale lifecycle decision.".to_string(),
            lifecycle_decision,
            lifecycle_stale,
            evidence_refs: proposal.evidence_refs.clone(),
            required_inputs: Vec::new(),
            mutation_options: Vec::new(),
        };
    }

    if let Some(parameterized) = parameterized_handoff_preview(proposal, args) {
        match parameterized {
            ParameterizedHandoffPreview::Ready { command, reason } => {
                return WikiProposalHandoffPreview {
                    proposal_id: proposal.id.clone(),
                    action: proposal.action,
                    subject_kind: proposal.subject_kind.clone(),
                    subject_id: proposal.subject_id.clone(),
                    status: "ready",
                    command: Some(command),
                    reason,
                    lifecycle_decision,
                    lifecycle_stale,
                    evidence_refs: proposal.evidence_refs.clone(),
                    required_inputs: Vec::new(),
                    mutation_options: Vec::new(),
                };
            }
            ParameterizedHandoffPreview::ManualRequired { reason } => {
                return WikiProposalHandoffPreview {
                    proposal_id: proposal.id.clone(),
                    action: proposal.action,
                    subject_kind: proposal.subject_kind.clone(),
                    subject_id: proposal.subject_id.clone(),
                    status: "manual_required",
                    command: None,
                    reason,
                    lifecycle_decision,
                    lifecycle_stale,
                    evidence_refs: proposal.evidence_refs.clone(),
                    required_inputs,
                    mutation_options,
                };
            }
        }
    }

    if let Some(command) = proposal.suggested_command.as_deref() {
        return WikiProposalHandoffPreview {
            proposal_id: proposal.id.clone(),
            action: proposal.action,
            subject_kind: proposal.subject_kind.clone(),
            subject_id: proposal.subject_id.clone(),
            status: "ready",
            command: Some(crate::offdesk::operator_safe_text(command)),
            reason: "The proposal already includes an exact governed mutation command.".to_string(),
            lifecycle_decision,
            lifecycle_stale,
            evidence_refs: proposal.evidence_refs.clone(),
            required_inputs: Vec::new(),
            mutation_options: Vec::new(),
        };
    }

    if let Some(command) = fallback_proposal_handoff_command(proposal) {
        return WikiProposalHandoffPreview {
            proposal_id: proposal.id.clone(),
            action: proposal.action,
            subject_kind: proposal.subject_kind.clone(),
            subject_id: proposal.subject_id.clone(),
            status: "ready",
            command: Some(command),
            reason: "An exact governed mutation command can be derived from the proposal subject."
                .to_string(),
            lifecycle_decision,
            lifecycle_stale,
            evidence_refs: proposal.evidence_refs.clone(),
            required_inputs: Vec::new(),
            mutation_options: Vec::new(),
        };
    }

    WikiProposalHandoffPreview {
        proposal_id: proposal.id.clone(),
        action: proposal.action,
        subject_kind: proposal.subject_kind.clone(),
        subject_id: proposal.subject_id.clone(),
        status: "manual_required",
        command: None,
        reason: manual_handoff_reason(proposal).to_string(),
        lifecycle_decision,
        lifecycle_stale,
        evidence_refs: proposal.evidence_refs.clone(),
        required_inputs,
        mutation_options,
    }
}

enum ParameterizedHandoffPreview {
    Ready { command: String, reason: String },
    ManualRequired { reason: String },
}

fn parameterized_handoff_preview(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> Option<ParameterizedHandoffPreview> {
    let mutation = args.mutation?;
    Some(match mutation {
        WikiProposalHandoffMutation::Rescope => parameterized_rescope_handoff(proposal, args),
        WikiProposalHandoffMutation::Deprecate => parameterized_deprecate_handoff(proposal, args),
        WikiProposalHandoffMutation::AddCounterexample => {
            parameterized_counterexample_handoff(proposal, args)
        }
        WikiProposalHandoffMutation::DeprecateDuplicate => {
            parameterized_deprecate_duplicate_handoff(proposal, args)
        }
        WikiProposalHandoffMutation::Split => parameterized_split_handoff(proposal),
    })
}

fn parameterized_rescope_handoff(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> ParameterizedHandoffPreview {
    if proposal.subject_kind != "entry"
        || !matches!(
            proposal.action,
            AdaptiveWikiReviewProposalAction::Rescope
                | AdaptiveWikiReviewProposalAction::RenewReview
                | AdaptiveWikiReviewProposalAction::Split
        )
    {
        return manual_handoff_missing(
            "--mutation rescope is only supported for entry rescope, renew-review, or split proposals.",
        );
    }
    let Some(scope) = args.scope else {
        return manual_handoff_missing("--mutation rescope requires --scope.");
    };
    let Some(scope_ref) = handoff_arg_value(args.scope_ref.as_deref()) else {
        return manual_handoff_missing("--mutation rescope requires --scope-ref.");
    };
    let mut command = format!(
        "forager offdesk wiki rescope {} --scope {} --scope-ref {}",
        handoff_subject_arg(&proposal.subject_id),
        adaptive_wiki_scope_arg(scope),
        shell_quote_arg(&scope_ref)
    );
    if let Some(reason) = handoff_arg_value(args.reason.as_deref()) {
        command.push_str(" --reason ");
        command.push_str(&shell_quote_arg(&reason));
    }
    ParameterizedHandoffPreview::Ready {
        command,
        reason: "Operator supplied enough inputs for an exact rescope mutation preview."
            .to_string(),
    }
}

fn parameterized_deprecate_handoff(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> ParameterizedHandoffPreview {
    let supported_standard = proposal.subject_kind == "entry"
        && matches!(
            proposal.action,
            AdaptiveWikiReviewProposalAction::Deprecate
                | AdaptiveWikiReviewProposalAction::RenewReview
        );
    let supported_conflict = proposal_is_projection_conflict(proposal);
    if !supported_standard && !supported_conflict {
        return manual_handoff_missing(
            "--mutation deprecate is only supported for entry deprecate, renew-review, or projection-conflict split proposals.",
        );
    }
    let Some(reason) = handoff_arg_value(args.reason.as_deref()) else {
        return manual_handoff_missing("--mutation deprecate requires --reason.");
    };
    let target_entry_id = if supported_conflict {
        match handoff_arg_value(args.deprecated_entry_id.as_deref()) {
            Some(deprecated_entry_id) => {
                if !projection_conflict_entry_ids(proposal)
                    .iter()
                    .any(|entry_id| entry_id == &deprecated_entry_id)
                {
                    return manual_handoff_missing(
                        "--mutation deprecate for conflict proposals requires --deprecated-entry-id to match the proposal subject or a conflicting entry evidence ref.",
                    );
                }
                deprecated_entry_id
            }
            None => proposal.subject_id.clone(),
        }
    } else {
        proposal.subject_id.clone()
    };
    ParameterizedHandoffPreview::Ready {
        command: deprecate_command(&target_entry_id, &reason),
        reason: "Operator supplied enough inputs for an exact deprecate mutation preview."
            .to_string(),
    }
}

fn parameterized_counterexample_handoff(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> ParameterizedHandoffPreview {
    if proposal.subject_kind != "entry"
        || !matches!(
            proposal.action,
            AdaptiveWikiReviewProposalAction::AddCounterexample
                | AdaptiveWikiReviewProposalAction::RenewReview
                | AdaptiveWikiReviewProposalAction::Split
        )
    {
        return manual_handoff_missing(
            "--mutation add-counterexample is only supported for entry counterexample, renew-review, or split proposals.",
        );
    }
    let Some(evidence_ref) = handoff_arg_value(args.evidence_ref.as_deref()) else {
        return manual_handoff_missing("--mutation add-counterexample requires --evidence-ref.");
    };
    let Some(reason) = handoff_arg_value(args.reason.as_deref()) else {
        return manual_handoff_missing("--mutation add-counterexample requires --reason.");
    };
    ParameterizedHandoffPreview::Ready {
        command: counterexample_command(&proposal.subject_id, &evidence_ref, &reason),
        reason: "Operator supplied enough inputs for an exact add-counterexample mutation preview."
            .to_string(),
    }
}

fn parameterized_deprecate_duplicate_handoff(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> ParameterizedHandoffPreview {
    if proposal.subject_kind != "entry"
        || proposal.action != AdaptiveWikiReviewProposalAction::Merge
    {
        return manual_handoff_missing(
            "--mutation deprecate-duplicate is only supported for entry merge proposals.",
        );
    }
    let Some(deprecated_entry_id) = handoff_arg_value(args.deprecated_entry_id.as_deref()) else {
        return manual_handoff_missing(
            "--mutation deprecate-duplicate requires --deprecated-entry-id.",
        );
    };
    let Some(reason) = handoff_arg_value(args.reason.as_deref()) else {
        return manual_handoff_missing("--mutation deprecate-duplicate requires --reason.");
    };
    ParameterizedHandoffPreview::Ready {
        command: deprecate_command(&deprecated_entry_id, &reason),
        reason:
            "Operator supplied enough inputs for an exact duplicate deprecate mutation preview."
                .to_string(),
    }
}

fn parameterized_split_handoff(
    proposal: &AdaptiveWikiReviewProposal,
) -> ParameterizedHandoffPreview {
    if proposal.subject_kind != "entry"
        || proposal.action != AdaptiveWikiReviewProposalAction::Split
    {
        return manual_handoff_missing(
            "--mutation split is only supported for entry split proposals.",
        );
    }
    if proposal_is_projection_conflict(proposal) {
        return manual_handoff_missing(
            "Projection-conflict splits require one or more governed mutations; preview rescope, deprecate, or add-counterexample paths and then link the executed mutation with a proposal receipt.",
        );
    }
    manual_handoff_missing(
        "Split proposals require manual scope design before a governed mutation command is exact.",
    )
}

fn manual_handoff_missing(reason: &str) -> ParameterizedHandoffPreview {
    ParameterizedHandoffPreview::ManualRequired {
        reason: reason.to_string(),
    }
}

fn fallback_proposal_handoff_command(proposal: &AdaptiveWikiReviewProposal) -> Option<String> {
    let reason = format!("curator review: {}", proposal.title);
    let subject_id = crate::offdesk::operator_safe_text(&proposal.subject_id);
    let reason = crate::offdesk::operator_safe_text(&reason);
    match (proposal.action, proposal.subject_kind.as_str()) {
        (AdaptiveWikiReviewProposalAction::Reject, "candidate") => Some(format!(
            "forager offdesk wiki reject {} --reason {}",
            shell_quote_arg(&subject_id),
            shell_quote_arg(&reason)
        )),
        (AdaptiveWikiReviewProposalAction::Deprecate, "entry") => {
            Some(deprecate_command(&subject_id, &reason))
        }
        _ => None,
    }
}

fn manual_handoff_contract(
    proposal: &AdaptiveWikiReviewProposal,
) -> (
    Vec<WikiProposalHandoffInput>,
    Vec<WikiProposalHandoffMutationOption>,
) {
    if proposal_is_projection_conflict(proposal) {
        return projection_conflict_handoff_contract(proposal);
    }
    match (proposal.action, proposal.subject_kind.as_str()) {
        (AdaptiveWikiReviewProposalAction::Rescope, "entry") => (
            vec![
                handoff_input(
                    "scope",
                    Some("--scope"),
                    true,
                    "New entry scope: session, project, artifact_kind, or user_global.",
                ),
                handoff_input(
                    "scope_ref",
                    Some("--scope-ref"),
                    true,
                    "Scope reference for the selected scope.",
                ),
                handoff_input(
                    "reason",
                    Some("--reason"),
                    false,
                    "Operator rationale to preserve in the mutation audit.",
                ),
            ],
            vec![handoff_option(
                "rescope",
                rescope_command_template(&proposal.subject_id),
                vec!["scope", "scope_ref"],
                "Narrow or widen the promoted entry after reviewing correction evidence.",
            )],
        ),
        (AdaptiveWikiReviewProposalAction::RenewReview, "entry") => (
            vec![
                handoff_input(
                    "mutation",
                    None,
                    true,
                    "Operator-selected mutation path: renew_review_after, rescope, deprecate, or add_counterexample.",
                ),
                handoff_input(
                    "review_after",
                    Some("--review-after"),
                    false,
                    "Required when mutation is renew_review_after.",
                ),
                handoff_input(
                    "scope",
                    Some("--scope"),
                    false,
                    "Required when mutation is rescope.",
                ),
                handoff_input(
                    "scope_ref",
                    Some("--scope-ref"),
                    false,
                    "Required when mutation is rescope.",
                ),
                handoff_input(
                    "evidence_ref",
                    Some("--evidence-ref"),
                    false,
                    "Required when mutation is add_counterexample.",
                ),
                handoff_input(
                    "reason",
                    Some("--reason"),
                    true,
                    "Operator rationale to preserve in the mutation audit.",
                ),
            ],
            vec![
                handoff_option(
                    "renew_review_after",
                    renew_review_after_command_template(&proposal.subject_id),
                    vec!["mutation", "review_after", "reason"],
                    "Keep the entry unchanged and move its next review timestamp.",
                ),
                handoff_option(
                    "rescope",
                    rescope_command_template(&proposal.subject_id),
                    vec!["mutation", "scope", "scope_ref"],
                    "Keep the entry promoted but adjust where it applies.",
                ),
                handoff_option(
                    "deprecate",
                    deprecate_command_template(&proposal.subject_id),
                    vec!["mutation", "reason"],
                    "Retire the entry when review finds it should no longer project.",
                ),
                handoff_option(
                    "add_counterexample",
                    counterexample_command_template(&proposal.subject_id),
                    vec!["mutation", "evidence_ref", "reason"],
                    "Keep the entry but attach limiting evidence for future review.",
                ),
            ],
        ),
        (AdaptiveWikiReviewProposalAction::Split, "entry") => (
            vec![
                handoff_input(
                    "mutation",
                    None,
                    true,
                    "Operator-selected mutation path after designing the narrower split.",
                ),
                handoff_input(
                    "scope",
                    Some("--scope"),
                    false,
                    "Required when the split is represented as a rescope of the current entry.",
                ),
                handoff_input(
                    "scope_ref",
                    Some("--scope-ref"),
                    false,
                    "Required when the split is represented as a rescope of the current entry.",
                ),
                handoff_input(
                    "evidence_ref",
                    Some("--evidence-ref"),
                    false,
                    "Required when the split is preserved as counterexample evidence.",
                ),
                handoff_input(
                    "reason",
                    Some("--reason"),
                    true,
                    "Operator rationale to preserve in the mutation audit.",
                ),
            ],
            vec![
                handoff_option(
                    "rescope",
                    rescope_command_template(&proposal.subject_id),
                    vec!["mutation", "scope", "scope_ref"],
                    "Represent the split by narrowing the current entry scope.",
                ),
                handoff_option(
                    "add_counterexample",
                    counterexample_command_template(&proposal.subject_id),
                    vec!["mutation", "evidence_ref", "reason"],
                    "Represent the split pressure as limiting evidence before creating variants.",
                ),
            ],
        ),
        (AdaptiveWikiReviewProposalAction::Merge, "entry") => (
            vec![
                handoff_input(
                    "survivor_entry_id",
                    None,
                    true,
                    "Entry that should remain promoted after duplicate review.",
                ),
                handoff_input(
                    "deprecated_entry_id",
                    None,
                    true,
                    "Duplicate entry to retire with an audited deprecate mutation.",
                ),
                handoff_input(
                    "reason",
                    Some("--reason"),
                    true,
                    "Operator rationale to preserve in the mutation audit.",
                ),
            ],
            vec![handoff_option(
                "deprecate_duplicate",
                "forager offdesk wiki deprecate <deprecated-entry-id> --reason <reason>"
                    .to_string(),
                vec!["survivor_entry_id", "deprecated_entry_id", "reason"],
                "The current mutation surface represents merge cleanup by deprecating duplicates.",
            )],
        ),
        (AdaptiveWikiReviewProposalAction::AddCounterexample, "entry") => (
            vec![
                handoff_input(
                    "evidence_ref",
                    Some("--evidence-ref"),
                    true,
                    "Evidence ref that contradicts or limits the entry.",
                ),
                handoff_input(
                    "reason",
                    Some("--reason"),
                    true,
                    "Operator rationale to preserve in the mutation audit.",
                ),
            ],
            vec![handoff_option(
                "add_counterexample",
                counterexample_command_template(&proposal.subject_id),
                vec!["evidence_ref", "reason"],
                "Attach limiting evidence to the promoted entry.",
            )],
        ),
        (AdaptiveWikiReviewProposalAction::AddCounterexample, "candidate") => (
            vec![
                handoff_input(
                    "candidate_evidence_source",
                    None,
                    true,
                    "Audit or source evidence to attach to the candidate.",
                ),
                handoff_input(
                    "mutation_path",
                    None,
                    true,
                    "Operator choice for re-recording, promoting, or rejecting the candidate.",
                ),
            ],
            Vec::new(),
        ),
        _ => (Vec::new(), Vec::new()),
    }
}

fn projection_conflict_handoff_contract(
    proposal: &AdaptiveWikiReviewProposal,
) -> (
    Vec<WikiProposalHandoffInput>,
    Vec<WikiProposalHandoffMutationOption>,
) {
    (
        vec![
            handoff_input(
                "mutation",
                None,
                true,
                "Operator-selected conflict path: rescope, deprecate, split, or add_counterexample.",
            ),
            handoff_input(
                "scope",
                Some("--scope"),
                false,
                "Required when mutation is rescope.",
            ),
            handoff_input(
                "scope_ref",
                Some("--scope-ref"),
                false,
                "Required when mutation is rescope.",
            ),
            handoff_input(
                "deprecated_entry_id",
                Some("--deprecated-entry-id"),
                false,
                "Optional when mutation is deprecate; defaults to the proposal subject and may target a conflicting entry evidence ref.",
            ),
            handoff_input(
                "evidence_ref",
                Some("--evidence-ref"),
                false,
                "Required when mutation is add_counterexample.",
            ),
            handoff_input(
                "reason",
                Some("--reason"),
                true,
                "Operator rationale to preserve in the mutation audit.",
            ),
        ],
        vec![
            handoff_option(
                "rescope",
                rescope_command_template(&proposal.subject_id),
                vec!["mutation", "scope", "scope_ref"],
                "Keep the entry promoted but narrow or widen where this side of the conflict applies.",
            ),
            handoff_option(
                "deprecate",
                deprecate_command_template(&proposal.subject_id),
                vec!["mutation", "reason"],
                "Retire the proposal subject, or pass --deprecated-entry-id for a conflicting entry evidence ref.",
            ),
            handoff_option(
                "add_counterexample",
                counterexample_command_template(&proposal.subject_id),
                vec!["mutation", "evidence_ref", "reason"],
                "Keep the entry but attach limiting evidence that explains the conflict.",
            ),
            handoff_option(
                "split",
                "manual: combine rescope, deprecate, and/or add-counterexample mutations, then record a proposal receipt".to_string(),
                vec!["mutation", "reason"],
                "Use when resolving the conflict needs multiple governed wiki mutations instead of one exact command.",
            ),
        ],
    )
}

fn proposal_is_projection_conflict(proposal: &AdaptiveWikiReviewProposal) -> bool {
    proposal.action == AdaptiveWikiReviewProposalAction::Split
        && proposal.subject_kind == "entry"
        && (proposal.title == "Resolve conflicting promoted entries"
            || proposal
                .evidence_refs
                .iter()
                .any(|value| value == "projection:conflict"))
}

fn projection_conflict_entry_ids(proposal: &AdaptiveWikiReviewProposal) -> Vec<String> {
    let mut ids = vec![proposal.subject_id.clone()];
    for evidence_ref in &proposal.evidence_refs {
        let Some(entry_id) = evidence_ref.strip_prefix("entry:") else {
            continue;
        };
        let entry_id = crate::offdesk::operator_safe_text(entry_id);
        if !entry_id.is_empty() && !ids.iter().any(|existing| existing == &entry_id) {
            ids.push(entry_id);
        }
    }
    ids
}

fn handoff_input(
    name: &'static str,
    cli_flag: Option<&'static str>,
    required: bool,
    description: &'static str,
) -> WikiProposalHandoffInput {
    WikiProposalHandoffInput {
        name,
        cli_flag,
        required,
        description,
    }
}

fn handoff_option(
    mutation: &'static str,
    command_template: String,
    required_inputs: Vec<&'static str>,
    description: &'static str,
) -> WikiProposalHandoffMutationOption {
    WikiProposalHandoffMutationOption {
        mutation,
        command_template,
        required_inputs,
        description,
    }
}

fn rescope_command_template(entry_id: &str) -> String {
    format!(
        "forager offdesk wiki rescope {} --scope <scope> --scope-ref <scope-ref> --reason <reason>",
        handoff_subject_arg(entry_id)
    )
}

fn deprecate_command_template(entry_id: &str) -> String {
    format!(
        "forager offdesk wiki deprecate {} --reason <reason>",
        handoff_subject_arg(entry_id)
    )
}

fn counterexample_command_template(entry_id: &str) -> String {
    format!(
        "forager offdesk wiki add-counterexample {} --evidence-ref <evidence-ref> --reason <reason>",
        handoff_subject_arg(entry_id)
    )
}

fn renew_review_after_command_template(entry_id: &str) -> String {
    format!(
        "forager offdesk wiki renew-review-after {} --review-after <rfc3339> --reason <reason>",
        handoff_subject_arg(entry_id)
    )
}

fn deprecate_command(entry_id: &str, reason: &str) -> String {
    format!(
        "forager offdesk wiki deprecate {} --reason {}",
        handoff_subject_arg(entry_id),
        shell_quote_arg(&crate::offdesk::operator_safe_text(reason))
    )
}

fn counterexample_command(entry_id: &str, evidence_ref: &str, reason: &str) -> String {
    format!(
        "forager offdesk wiki add-counterexample {} --evidence-ref {} --reason {}",
        handoff_subject_arg(entry_id),
        shell_quote_arg(&crate::offdesk::operator_safe_text(evidence_ref)),
        shell_quote_arg(&crate::offdesk::operator_safe_text(reason))
    )
}

fn handoff_subject_arg(subject_id: &str) -> String {
    shell_quote_arg(&crate::offdesk::operator_safe_text(subject_id))
}

fn handoff_arg_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(crate::offdesk::operator_safe_text)
}

fn adaptive_wiki_scope_arg(scope: AdaptiveWikiScope) -> &'static str {
    match scope {
        AdaptiveWikiScope::Session => "session",
        AdaptiveWikiScope::Project => "project",
        AdaptiveWikiScope::ArtifactKind => "artifact_kind",
        AdaptiveWikiScope::UserGlobal => "user_global",
    }
}

fn manual_handoff_reason(proposal: &AdaptiveWikiReviewProposal) -> &'static str {
    if proposal_is_projection_conflict(proposal) {
        return "Projection-conflict proposals require choosing whether to rescope, deprecate one side, preserve counterexample evidence, or split with multiple governed mutations.";
    }
    match proposal.action {
        AdaptiveWikiReviewProposalAction::Rescope => {
            "Rescope proposals require an operator-selected --scope and --scope-ref."
        }
        AdaptiveWikiReviewProposalAction::RenewReview => {
            "Renew-review proposals require choosing whether to renew, rescope, deprecate, or add evidence."
        }
        AdaptiveWikiReviewProposalAction::Split => {
            "Split proposals require manual scope design before a governed mutation command is exact."
        }
        AdaptiveWikiReviewProposalAction::Merge => {
            "Merge proposals require choosing the surviving entry and migration plan."
        }
        AdaptiveWikiReviewProposalAction::AddCounterexample => {
            "Counterexample proposals require a specific evidence ref and target mutation choice."
        }
        AdaptiveWikiReviewProposalAction::Promote
        | AdaptiveWikiReviewProposalAction::Reject
        | AdaptiveWikiReviewProposalAction::Deprecate => {
            "This proposal does not contain enough information for an exact governed mutation command."
        }
    }
}

async fn wiki_corrections(profile: &str, args: JsonArgs) -> Result<()> {
    let corrections = wiki_store(profile)?.load_correction_records()?;

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&corrections)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    if corrections.is_empty() {
        println!("No adaptive wiki correction records found.");
        return Ok(());
    }
    for correction in corrections {
        println!(
            "{} {:?} task={} request={} entry={} {}",
            correction.id,
            correction.correction_kind,
            correction.task_id.as_deref().unwrap_or("-"),
            correction.request_id.as_deref().unwrap_or("-"),
            correction.entry_id.as_deref().unwrap_or("-"),
            crate::offdesk::operator_safe_text(&correction.summary)
        );
    }
    Ok(())
}

async fn wiki_candidates(profile: &str, args: WikiListArgs) -> Result<()> {
    let projection = wiki_store(profile)?.human_projection(&wiki_query(
        &args.session_id,
        &args.project_key,
        &args.artifact_kind,
        args.agent_mode,
    ))?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&projection.candidates)?);
        return Ok(());
    }

    if projection.candidates.is_empty() {
        println!("No adaptive wiki candidates found.");
        return Ok(());
    }

    print_wiki_candidates(&projection.candidates);
    Ok(())
}

async fn wiki_entries(profile: &str, args: WikiListArgs) -> Result<()> {
    let projection = wiki_store(profile)?.human_projection(&wiki_query(
        &args.session_id,
        &args.project_key,
        &args.artifact_kind,
        args.agent_mode,
    ))?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&projection.entries)?);
        return Ok(());
    }

    if projection.entries.is_empty() {
        println!("No adaptive wiki entries found.");
        return Ok(());
    }

    print_wiki_entries(&projection.entries);
    Ok(())
}

async fn wiki_show(profile: &str, args: WikiShowArgs) -> Result<()> {
    let projection = wiki_store(profile)?.human_projection(&AdaptiveWikiQuery::default())?;
    let result = projection
        .entries
        .into_iter()
        .find(|entry| entry.id == args.id)
        .map(|entry| WikiShowResult::Entry { entry })
        .or_else(|| {
            projection
                .candidates
                .into_iter()
                .find(|candidate| candidate.id == args.id)
                .map(|candidate| WikiShowResult::Candidate { candidate })
        });

    let Some(result) = result else {
        bail!("Adaptive wiki entry or candidate not found: {}", args.id);
    };

    if args.json {
        println!("{}", serde_json::to_string_pretty(&result)?);
        return Ok(());
    }

    print_wiki_show(&result);
    Ok(())
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
    let by_id: std::collections::HashMap<&str, &AdaptiveWikiEntry> = entries_state
        .entries
        .iter()
        .map(|entry| (entry.id.as_str(), entry))
        .collect();
    let now = Utc::now();
    let scope_label = args
        .project_key
        .clone()
        .unwrap_or_else(|| "user_global".to_string());

    let mut items: Vec<serde_json::Value> = Vec::new();
    let mut lines: Vec<String> = Vec::new();
    lines.push(
        "<!-- generated by `forager offdesk wiki brief`; regenerate, do not edit -->".to_string(),
    );
    lines.push(format!("# Wiki Brief: {profile} / {scope_label}"));
    lines.push(String::new());
    lines.push(
        "Distilled, reviewed operating knowledge. **Advisory context, not gospel.**".to_string(),
    );
    lines.push(
        "Trust signals per line: `[confidence . evidence N . age Nd]`; `STALE` means the review \
         window passed, so treat that line as a hypothesis. If an entry conflicts with what you \
         observe (paths moved, decisions changed, results superseded), do NOT silently obey or \
         silently ignore it: verify against current state, then record the outcome with \
         `forager offdesk wiki add-counterexample <id>` or `deprecate <id>`."
            .to_string(),
    );
    lines.push(String::new());

    let mut current_kind: Option<AdaptiveWikiKind> = None;
    let mut sorted = report.selected.clone();
    sorted.sort_by_key(|item| format!("{:?}", item.kind));
    for item in &sorted {
        let entry = by_id.get(item.id.as_str());
        let age_days = entry
            .map(|e| (now - e.updated_at).num_days().max(0))
            .unwrap_or(0);
        let stale = entry
            .and_then(|e| e.review_after)
            .is_some_and(|review_after| review_after < now);
        let claim = entry.map(|e| e.claim.as_str()).unwrap_or("");
        let text = if item.instruction.trim().is_empty() {
            claim
        } else {
            &item.instruction
        };
        if current_kind != Some(item.kind) {
            current_kind = Some(item.kind);
            lines.push(format!("## {}", wiki_kind_label(item.kind)));
        }
        let stale_marker = if stale { " **STALE**" } else { "" };
        lines.push(format!(
            "- {}{} `[{:?} . ev {} . age {}d]` ({})",
            operator_safe_text(text),
            stale_marker,
            item.confidence,
            item.evidence_count,
            age_days,
            item.id
        ));
        items.push(serde_json::json!({
            "id": item.id,
            "kind": format!("{:?}", item.kind),
            "text": operator_safe_text(text),
            "confidence": format!("{:?}", item.confidence),
            "evidence_count": item.evidence_count,
            "age_days": age_days,
            "stale": stale,
        }));
    }
    if sorted.is_empty() {
        lines.push("(no promoted in-scope entries yet)".to_string());
    }
    lines.push(String::new());
    let body = lines.join("\n");

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "schema": "wiki_brief.v1",
                "profile": profile,
                "scope": scope_label,
                "generated_at": now,
                "entries": items,
            }))?
        );
        return Ok(());
    }
    if let Some(out) = &args.out {
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(out, format!("{body}\n"))?;
        println!(
            "Wrote wiki brief ({} entries) to {}",
            sorted.len(),
            out.display()
        );
    } else {
        println!("{body}");
    }
    Ok(())
}

fn wiki_kind_label(kind: AdaptiveWikiKind) -> &'static str {
    match kind {
        AdaptiveWikiKind::Preference => "Preferences",
        AdaptiveWikiKind::Procedure => "Procedures",
        AdaptiveWikiKind::FailurePattern => "Failure patterns",
        AdaptiveWikiKind::PolicyRule => "Policy rules",
        AdaptiveWikiKind::Fact => "Facts",
    }
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
        if args.json {
            println!("{}", serde_json::to_string_pretty(&comparison)?);
            return Ok(());
        }
        print_wiki_projection_comparison_report(&comparison);
        return Ok(());
    }
    let policy = wiki_projection_policy(&args);
    let report = wiki_store(profile)?.ai_projection_report_with_policy(&query, budget, policy)?;

    if args.json {
        if args.report {
            println!("{}", serde_json::to_string_pretty(&report)?);
        } else {
            println!("{}", serde_json::to_string_pretty(&report.selected)?);
        }
        return Ok(());
    }

    if args.report {
        print_wiki_projection_report(&report);
        return Ok(());
    }

    if report.selected.is_empty() {
        println!("No adaptive wiki projection entries found.");
        return Ok(());
    }

    println!(
        "{:<44} {:<16} {:<16} {:<18} INSTRUCTION",
        "ID", "SCOPE", "ACTIVATION", "AGENT_MODES"
    );
    for entry in report.selected {
        println!(
            "{:<44} {:<16} {:<16} {:<18} {}",
            entry.id,
            format!("{:?}", entry.scope).to_lowercase(),
            format!("{:?}", entry.activation_mode).to_lowercase(),
            adaptive_wiki_agent_modes_label(&entry.agent_modes),
            entry.instruction
        );
    }
    Ok(())
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

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&acknowledgements)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    if acknowledgements.is_empty() {
        println!("No adaptive wiki runtime policy acknowledgements found.");
        return Ok(());
    }

    print_wiki_runtime_policy_acknowledgements(&acknowledgements);
    Ok(())
}

async fn wiki_runtime_policy_ack_report(
    profile: &str,
    args: WikiRuntimePolicyAckReportArgs,
) -> Result<()> {
    let now = Utc::now();
    let near_expiry_hours = args.near_expiry_hours.max(1);
    let near_expiry_window = Duration::hours(near_expiry_hours);
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
        near_expiry_window,
        now,
    );

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&report)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    print_wiki_runtime_policy_ack_report(&report);
    Ok(())
}

async fn wiki_review_after_report(profile: &str, args: WikiReviewAfterReportArgs) -> Result<()> {
    let now = Utc::now();
    let near_expiry_hours = args.near_expiry_hours.max(1);
    let near_expiry_window = Duration::hours(near_expiry_hours);
    let query = wiki_query(
        &args.session_id,
        &args.project_key,
        &args.artifact_kind,
        args.agent_mode,
    );
    let projection = wiki_store(profile)?.human_projection(&query)?;
    let report = build_review_after_report(
        projection.entries,
        query,
        near_expiry_hours,
        near_expiry_window,
        now,
    );

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&report)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    print_wiki_review_after_report(&report);
    Ok(())
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

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&acknowledgement)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    println!(
        "Recorded adaptive wiki runtime policy acknowledgement {}",
        acknowledgement.id
    );
    print_wiki_runtime_policy_acknowledgement(&acknowledgement);
    Ok(())
}

fn build_review_after_report(
    entries: Vec<AdaptiveWikiHumanEntry>,
    query: AdaptiveWikiQuery,
    near_expiry_hours: i64,
    near_expiry_window: Duration,
    now: DateTime<Utc>,
) -> WikiReviewAfterReport {
    let mut summary = WikiReviewAfterReportSummary::default();
    let mut attention = Vec::new();
    for entry in entries
        .into_iter()
        .filter(|entry| entry.status == crate::offdesk::AdaptiveWikiStatus::Promoted)
    {
        summary.scoped_promoted += 1;
        let Some(review_after) = entry.review_after else {
            summary.missing_review_after += 1;
            continue;
        };
        summary.with_review_after += 1;
        if review_after <= now {
            summary.expired += 1;
            attention.push(review_after_report_item(
                entry,
                review_after,
                "expired",
                now,
            ));
        } else if review_after <= now + near_expiry_window {
            summary.near_expiry += 1;
            attention.push(review_after_report_item(
                entry,
                review_after,
                "near_expiry",
                now,
            ));
        }
    }
    summary.attention = attention.len();
    attention.sort_by_key(|entry| (review_after_status_order(&entry.status), entry.review_after));
    WikiReviewAfterReport {
        generated_at: now,
        query,
        near_expiry_hours,
        summary,
        entries: attention,
    }
}

fn review_after_report_item(
    entry: AdaptiveWikiHumanEntry,
    review_after: DateTime<Utc>,
    status: &str,
    now: DateTime<Utc>,
) -> WikiReviewAfterReportItem {
    WikiReviewAfterReportItem {
        renew_command_template: renew_review_after_command_template(&entry.id),
        id: entry.id,
        kind: entry.kind,
        scope: entry.scope,
        scope_ref: entry.scope_ref,
        review_after,
        hours_until_review: review_after.signed_duration_since(now).num_hours(),
        status: status.to_string(),
    }
}

fn review_after_status_order(status: &str) -> u8 {
    match status {
        "expired" => 0,
        "near_expiry" => 1,
        _ => 2,
    }
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

fn build_runtime_policy_ack_report(
    acknowledgements: Vec<AdaptiveWikiRuntimePolicyAcknowledgement>,
    query: Option<AdaptiveWikiQuery>,
    budget: Option<AdaptiveWikiProjectionBudget>,
    decision: Option<AdaptiveWikiRuntimePolicyDecision>,
    near_expiry_hours: i64,
    near_expiry_window: Duration,
    now: DateTime<Utc>,
) -> WikiRuntimePolicyAckReport {
    let decision_ack_id = decision
        .as_ref()
        .and_then(|decision| decision.acknowledgement_id.as_deref());
    let decision_status = decision.as_ref().map(|decision| decision.status);
    let query_ref = query.as_ref();
    let budget_ref = budget.as_ref();
    let mut summary = WikiRuntimePolicyAckReportSummary {
        total: acknowledgements.len(),
        ..WikiRuntimePolicyAckReportSummary::default()
    };
    let acknowledgements = acknowledgements
        .into_iter()
        .map(|acknowledgement| {
            let mut status = Vec::new();
            let expired = acknowledgement.expires_at <= now;
            let near_expiry =
                !expired && acknowledgement.expires_at <= now + near_expiry_window;
            if expired {
                summary.expired += 1;
                status.push("expired".to_string());
            } else {
                summary.active += 1;
                status.push("active".to_string());
            }
            if near_expiry {
                summary.near_expiry += 1;
                status.push("near_expiry".to_string());
            }
            let mut query_status = None;
            if decision_ack_id == Some(acknowledgement.id.as_str()) {
                match decision_status {
                    Some(AdaptiveWikiRuntimePolicyDecisionStatus::AppliedAcknowledged)
                    | Some(
                        AdaptiveWikiRuntimePolicyDecisionStatus::AppliedProjectArtifactAcknowledged,
                    ) => {
                        summary.query_applied += 1;
                        status.push("query_applied".to_string());
                    }
                    Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedScopeModeBlocked) => {
                        summary.query_blocked += 1;
                        status.push("query_blocked_by_session_scope".to_string());
                        query_status = decision_status;
                    }
                    Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedStaleAcknowledgement) => {
                        summary.query_stale += 1;
                        status.push("query_stale_comparison".to_string());
                        query_status = decision_status;
                    }
                    Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedExpiredAcknowledgement) => {
                        summary.query_expired += 1;
                        status.push("query_expired_acknowledgement".to_string());
                        query_status = decision_status;
                    }
                    _ => {}
                }
            }
            let suggested_action = runtime_policy_ack_suggested_action(
                &acknowledgement,
                expired,
                near_expiry,
                query_status,
                query_ref,
                budget_ref,
            );
            if suggested_action.is_some() {
                summary.suggested_actions += 1;
            }
            WikiRuntimePolicyAckReportItem {
                id: acknowledgement.id,
                scope_mode: acknowledgement.scope_mode,
                query: acknowledgement.query,
                policy: acknowledgement.policy,
                created_at: acknowledgement.created_at,
                expires_at: acknowledgement.expires_at,
                minutes_until_expiry: acknowledgement
                    .expires_at
                    .signed_duration_since(now)
                    .num_minutes(),
                status,
                review_expired_excluded: acknowledgement.review_expired_excluded,
                suggested_action,
            }
        })
        .collect();

    WikiRuntimePolicyAckReport {
        generated_at: now,
        near_expiry_hours,
        query,
        budget,
        decision,
        summary,
        acknowledgements,
    }
}

fn runtime_policy_ack_suggested_action(
    acknowledgement: &AdaptiveWikiRuntimePolicyAcknowledgement,
    expired: bool,
    near_expiry: bool,
    query_status: Option<AdaptiveWikiRuntimePolicyDecisionStatus>,
    report_query: Option<&AdaptiveWikiQuery>,
    report_budget: Option<&AdaptiveWikiProjectionBudget>,
) -> Option<WikiRuntimePolicyAckSuggestedAction> {
    match query_status {
        Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedScopeModeBlocked) => {
            let query = report_query.unwrap_or(&acknowledgement.query);
            let budget = report_budget.unwrap_or(&acknowledgement.budget);
            Some(WikiRuntimePolicyAckSuggestedAction {
                kind: "record_exact_query_acknowledgement".to_string(),
                detail: "Project/artifact acknowledgement cannot apply while session-scoped projection entries are present; review the exact query comparison and append a new exact-query acknowledgement.".to_string(),
                compare_command_template: runtime_policy_compare_command_template(query, budget),
                ack_command_template: runtime_policy_ack_command_template(
                    query,
                    budget,
                    AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery,
                ),
            })
        }
        Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedStaleAcknowledgement) => {
            Some(WikiRuntimePolicyAckSuggestedAction {
                kind: "recompare_and_append_acknowledgement".to_string(),
                detail: "The current strict runtime comparison no longer matches this acknowledgement hash; review the comparison again and append a new acknowledgement.".to_string(),
                compare_command_template: runtime_policy_compare_command_template(
                    &acknowledgement.query,
                    &acknowledgement.budget,
                ),
                ack_command_template: runtime_policy_ack_command_template(
                    &acknowledgement.query,
                    &acknowledgement.budget,
                    acknowledgement.scope_mode,
                ),
            })
        }
        Some(AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedExpiredAcknowledgement) => {
            Some(WikiRuntimePolicyAckSuggestedAction {
                kind: "recompare_and_append_acknowledgement".to_string(),
                detail: "The matching strict runtime acknowledgement is expired; review the comparison again and append a new acknowledgement instead of extending the old record.".to_string(),
                compare_command_template: runtime_policy_compare_command_template(
                    &acknowledgement.query,
                    &acknowledgement.budget,
                ),
                ack_command_template: runtime_policy_ack_command_template(
                    &acknowledgement.query,
                    &acknowledgement.budget,
                    acknowledgement.scope_mode,
                ),
            })
        }
        _ if expired => Some(WikiRuntimePolicyAckSuggestedAction {
            kind: "recompare_and_append_acknowledgement".to_string(),
            detail: "This acknowledgement is expired; review the comparison again and append a new acknowledgement instead of extending the old record.".to_string(),
            compare_command_template: runtime_policy_compare_command_template(
                &acknowledgement.query,
                &acknowledgement.budget,
            ),
            ack_command_template: runtime_policy_ack_command_template(
                &acknowledgement.query,
                &acknowledgement.budget,
                acknowledgement.scope_mode,
            ),
        }),
        _ if near_expiry => Some(WikiRuntimePolicyAckSuggestedAction {
            kind: "review_before_expiry".to_string(),
            detail: "This acknowledgement is near expiry; review the comparison before it expires and append a new acknowledgement if strict runtime should continue.".to_string(),
            compare_command_template: runtime_policy_compare_command_template(
                &acknowledgement.query,
                &acknowledgement.budget,
            ),
            ack_command_template: runtime_policy_ack_command_template(
                &acknowledgement.query,
                &acknowledgement.budget,
                acknowledgement.scope_mode,
            ),
        }),
        _ => None,
    }
}

fn runtime_policy_compare_command_template(
    query: &AdaptiveWikiQuery,
    budget: &AdaptiveWikiProjectionBudget,
) -> String {
    let mut parts = vec![
        "forager".to_string(),
        "offdesk".to_string(),
        "wiki".to_string(),
        "projection".to_string(),
    ];
    append_runtime_policy_query_args(&mut parts, query);
    if query.agent_mode_filter == AdaptiveWikiAgentModeFilter::SharedWhenUnspecified {
        parts.push("--runtime-agent-mode-default".to_string());
    }
    append_runtime_policy_budget_args(&mut parts, budget);
    parts.push("--compare-review-expired-policy".to_string());
    parts.push("--json".to_string());
    parts.join(" ")
}

fn runtime_policy_ack_command_template(
    query: &AdaptiveWikiQuery,
    budget: &AdaptiveWikiProjectionBudget,
    scope_mode: AdaptiveWikiRuntimePolicyAckScopeMode,
) -> String {
    let mut parts = vec![
        "forager".to_string(),
        "offdesk".to_string(),
        "wiki".to_string(),
        "ack-runtime-policy".to_string(),
    ];
    if scope_mode != AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery {
        parts.push("--scope-mode".to_string());
        parts.push(shell_quote_arg(runtime_ack_scope_mode_cli_value(
            scope_mode,
        )));
    }
    append_runtime_policy_query_args(&mut parts, query);
    append_runtime_policy_budget_args(&mut parts, budget);
    parts.push("--reason".to_string());
    parts.push("<reason>".to_string());
    parts
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn append_runtime_policy_query_args(parts: &mut Vec<String>, query: &AdaptiveWikiQuery) {
    if let Some(session_id) = query.session_id.as_deref() {
        parts.push("--session-id".to_string());
        parts.push(shell_quote_arg(session_id));
    }
    if let Some(project_key) = query.project_key.as_deref() {
        parts.push("--project-key".to_string());
        parts.push(shell_quote_arg(project_key));
    }
    if let Some(artifact_kind) = query.artifact_kind.as_deref() {
        parts.push("--artifact-kind".to_string());
        parts.push(shell_quote_arg(artifact_kind));
    }
    if let Some(agent_mode) = query.agent_mode {
        parts.push("--agent-mode".to_string());
        parts.push(shell_quote_arg(adaptive_wiki_agent_mode_cli_value(
            agent_mode,
        )));
    }
}

fn append_runtime_policy_budget_args(
    parts: &mut Vec<String>,
    budget: &AdaptiveWikiProjectionBudget,
) {
    let default = AdaptiveWikiProjectionBudget::default();
    if budget.max_entries != default.max_entries {
        parts.push("--max-entries".to_string());
        parts.push(budget.max_entries.to_string());
    }
    if budget.max_context_chars != default.max_context_chars {
        parts.push("--max-context-chars".to_string());
        parts.push(budget.max_context_chars.to_string());
    }
    if budget.max_instruction_chars != default.max_instruction_chars {
        parts.push("--max-instruction-chars".to_string());
        parts.push(budget.max_instruction_chars.to_string());
    }
}

fn runtime_ack_scope_mode_cli_value(mode: AdaptiveWikiRuntimePolicyAckScopeMode) -> &'static str {
    match mode {
        AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery => "exact-query",
        AdaptiveWikiRuntimePolicyAckScopeMode::ProjectArtifact => "project-artifact",
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

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_wiki_lint(&report);
    Ok(())
}

async fn wiki_export_markdown(profile: &str, args: WikiExportMarkdownArgs) -> Result<()> {
    let store = wiki_store(profile)?;
    let output = args
        .output
        .unwrap_or_else(|| store.default_markdown_vault_dir());
    let report = store.export_markdown(&output, args.dry_run, Utc::now())?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_wiki_markdown_export(&report);
    Ok(())
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

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_wiki_graph_report(&report, args.output.as_deref(), args.dry_run, files.len());
    Ok(())
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

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_wiki_review_report(&report);
    Ok(())
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

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_wiki_episode_evaluation_report(&report);
    Ok(())
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

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_wiki_live_episode_trace_report(&report);
    Ok(())
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

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_wiki_correction_recurrence_report(&report);
    Ok(())
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

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_wiki_promotion_chain_report(&report);
    Ok(())
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
    print_wiki_mutation(&result, args.json)
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
    print_wiki_mutation(&result, args.json)
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
    print_wiki_mutation(&result, args.json)
}

async fn wiki_edit(profile: &str, args: WikiEditArgs) -> Result<()> {
    if args.claim.is_none()
        && args.ai_instruction.is_none()
        && args.human_summary.is_none()
        && args.evidence_refs.is_empty()
    {
        bail!(
            "wiki edit needs at least one of --claim, --ai-instruction, --human-summary, or --evidence-ref"
        );
    }
    let now = Utc::now();
    let store = writable_wiki_store(profile)?;
    let entry = store
        .edit_entry(
            &args.entry_id,
            args.claim.as_deref(),
            args.ai_instruction.as_deref(),
            args.human_summary.as_deref(),
            &args.evidence_refs,
            now,
        )?
        .ok_or_else(|| anyhow::anyhow!("Adaptive wiki entry not found: {}", args.entry_id))?;
    let audit = wiki_audit_record(WikiAuditRecordInput {
        action: AdaptiveWikiAuditAction::Edit,
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
    let result = WikiMutationResult::Edit {
        entry: human_entry(entry),
        audit,
    };
    print_wiki_mutation(&result, args.json)
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
    print_wiki_mutation(&result, args.json)
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
    print_wiki_mutation(&result, args.json)
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
    print_wiki_mutation(&result, args.json)
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
    print_wiki_mutation(&result, args.json)
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
    print_wiki_mutation(&result, args.json)
}

async fn cancel_task(profile: &str, args: CancelTaskArgs) -> Result<()> {
    let report =
        task_store(profile)?.cancel_task(&args.task_id, args.reason.as_deref(), Utc::now())?;
    print_lifecycle_report(&report, args.json)
}

async fn pause_dispatch(profile: &str, args: PauseArgs) -> Result<()> {
    let state = OperatorPauseStore::new(get_profile_dir(profile)?).pause(
        args.reason.as_deref(),
        Some(&args.by),
        Utc::now(),
    )?;
    print_operator_pause_state(&state, args.json)
}

async fn unpause_dispatch(profile: &str, args: UnpauseArgs) -> Result<()> {
    let state =
        OperatorPauseStore::new(get_profile_dir(profile)?).resume(Some(&args.by), Utc::now())?;
    print_operator_pause_state(&state, args.json)
}

async fn pause_status(profile: &str, args: JsonArgs) -> Result<()> {
    let state = OperatorPauseStore::new(get_profile_dir(profile)?).load()?;
    print_operator_pause_state(&state, args.json)
}

async fn learning_scan(profile: &str, args: JsonArgs) -> Result<()> {
    let report = scan_and_emit_learning_signals(get_profile_dir(profile)?, Utc::now())?;
    print_learning_scan_report(&report, args.json)
}

fn print_learning_scan_report(report: &LearningScanReport, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    if report.emitted.is_empty() {
        println!(
            "No new learning signals ({} already recorded).",
            report.skipped_already_processed
        );
        return Ok(());
    }
    println!(
        "Emitted {} learning candidate(s) ({} already recorded):",
        report.emitted.len(),
        report.skipped_already_processed
    );
    for signal in &report.emitted {
        println!("  [{}] {}", signal.source.as_str(), signal.claim);
    }
    println!("Candidates are recommendation-only; review with `forager offdesk wiki candidates`.");
    Ok(())
}

fn print_operator_pause_state(state: &OperatorPauseState, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(state)?);
        return Ok(());
    }
    if state.paused {
        println!("Offdesk dispatch is PAUSED; new work is held until resume.");
        if let Some(reason) = state.reason.as_deref() {
            println!("  reason: {reason}");
        }
    } else {
        println!("Offdesk dispatch is active.");
    }
    Ok(())
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
    print_retry_lifecycle_report(
        &report,
        superseded_denied_approvals,
        args.json,
        args.new_approval,
    )
}

async fn resume_task(profile: &str, args: TaskLifecycleArgs) -> Result<()> {
    let report = task_store(profile)?.resume_task(&args.task_id, Utc::now())?;
    print_lifecycle_report(&report, args.json)
}

async fn abandon_task(profile: &str, args: TaskLifecycleArgs) -> Result<()> {
    let report = task_store(profile)?.abandon_task(&args.task_id, Utc::now())?;
    print_lifecycle_report(&report, args.json)
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

    if args.json {
        println!("{}", serde_json::to_string_pretty(&outcomes)?);
        return Ok(());
    }

    if outcomes.is_empty() {
        println!("No matching background runner probes found.");
        return Ok(());
    }

    for outcome in outcomes {
        println!(
            "{} {:?} -> {:?}: {}",
            outcome.probe.ticket_id,
            outcome.probe.runner_kind,
            outcome.decision.phase,
            outcome.decision.evidence
        );
        print_mode_assessment(&outcome.mode_assessment);
        if let Some(observed_at) = outcome.probe.last_observed_at {
            println!("  observed_at: {observed_at}");
        }
        if let Some(tail) = outcome.probe.last_log_tail.as_deref() {
            println!("  tail: {tail}");
        }
        print_next_safe_action(&outcome.next_safe_action);
    }
    Ok(())
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

    if args.json {
        println!("{}", serde_json::to_string_pretty(&states)?);
        return Ok(());
    }

    if states.is_empty() {
        println!("No task resume artifacts found.");
        return Ok(());
    }

    print_resume_states(&states);
    Ok(())
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

    if args.json {
        println!("{}", serde_json::to_string_pretty(&statuses)?);
        return Ok(());
    }

    if statuses.is_empty() {
        println!("No background runner probes found.");
        return Ok(());
    }

    for status in statuses {
        println!(
            "{} {:?} -> {:?}: {}",
            status.probe.ticket_id,
            status.probe.runner_kind,
            status.decision.phase,
            status.decision.evidence
        );
        print_mode_assessment(&status.mode_assessment);
        if let Some(observed_at) = status.probe.last_observed_at {
            println!("  observed_at: {observed_at}");
        }
        if let Some(tail) = status.probe.last_log_tail.as_deref() {
            println!("  tail: {tail}");
        }
    }
    Ok(())
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
        print_background_ack_report(&report, args.json)?;
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
    print_background_ack_report(&report, args.json)?;
    Ok(())
}

fn print_background_ack_report(report: &BackgroundAckReport, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    println!(
        "Acknowledged background recovery {} -> {:?}",
        report.ticket_id, report.status.decision.phase
    );
    println!("  by:     {}", report.acknowledgement.acknowledged_by);
    println!("  reason: {}", report.acknowledgement.reason);
    if !report.linked_task_ids.is_empty() {
        println!("  tasks:  {}", report.linked_task_ids.join(", "));
    }
    println!(
        "  note:   no retry, resume, closeout, cleanup, or accepted-truth action is authorized by this acknowledgement"
    );
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
        println!("{}", serde_json::to_string_pretty(HOSTED_HARNESS_PROFILES)?);
        return Ok(());
    }

    println!("Hosted harness agent profiles");
    println!("Current support target: Codex CLI and Claude Code");
    println!();
    for profile in HOSTED_HARNESS_PROFILES {
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
    let packet = build_harness_prompt_packet(profile, args)?;

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

const OFFDESK_PLAN_REGISTRATION_SCHEMA: &str = "offdesk_plan_registration.v1";
const OFFDESK_PLAN_REQUIRED_DENIALS: [&str; 8] = [
    "enqueue",
    "launch",
    "approval",
    "file movement",
    "archive",
    "delete",
    "wiki promotion",
    "accepted truth",
];

async fn plan(profile: &str, args: PlanArgs) -> Result<()> {
    let registration = build_offdesk_plan_registration(profile, &args)?;
    print_offdesk_plan_registration(&registration, args.json)
}

async fn plans(profile: &str, args: PlansArgs) -> Result<()> {
    let mut items = load_offdesk_plan_registry_items(profile)?;
    items.retain(|item| offdesk_plan_matches_filter(item, &args));
    items.sort_by_key(|item| item.registration.registered_at);
    if args.latest {
        if let Some(latest) = items.pop() {
            items = vec![latest];
        }
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&items)?);
        return Ok(());
    }

    if items.is_empty() {
        println!("No registered Offdesk plans found.");
        return Ok(());
    }

    print_offdesk_plan_registry_items(&items);
    Ok(())
}

async fn plan_show(profile: &str, args: PlanShowArgs) -> Result<()> {
    let items = load_offdesk_plan_registry_items(profile)?;
    let Some(item) = find_offdesk_plan_registry_item(items, &args.plan_ref) else {
        bail!("Registered Offdesk plan not found: {}", args.plan_ref);
    };
    let detail = offdesk_plan_registry_detail(item)?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&detail)?);
        return Ok(());
    }

    print_offdesk_plan_registry_detail(&detail);
    Ok(())
}

async fn plan_review(profile: &str, args: PlanReviewArgs) -> Result<()> {
    let items = load_offdesk_plan_registry_items(profile)?;
    let Some(item) = find_offdesk_plan_registry_item(items, &args.plan_ref) else {
        bail!("Registered Offdesk plan not found: {}", args.plan_ref);
    };
    let record = build_offdesk_plan_review_record(profile, &item, &args)?;
    write_offdesk_plan_review_record(&record)?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&record)?);
        return Ok(());
    }

    print_offdesk_plan_review_record(&record);
    Ok(())
}

async fn plan_launch_prep(profile: &str, args: PlanLaunchPrepArgs) -> Result<()> {
    let items = load_offdesk_plan_registry_items(profile)?;
    let Some(item) = find_offdesk_plan_registry_item(items, &args.plan_ref) else {
        bail!("Registered Offdesk plan not found: {}", args.plan_ref);
    };
    let packet = build_offdesk_plan_launch_prep_packet(profile, &item, &args)?;
    write_offdesk_plan_launch_prep_packet(&packet)?;

    if args.json {
        println!("{}", serde_json::to_string_pretty(&packet)?);
        return Ok(());
    }

    print_offdesk_plan_launch_prep_packet(&packet);
    Ok(())
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
    let payload = remote_operator_status_payload(status);
    let observed_hash = observed_hash_for(&payload)?;
    let card = remote_operator_status_card(&payload, observed_hash);
    let projection = remote_operator_projection(profile, &args.transport, "status", card, payload);
    print_remote_operator_projection(&projection, args.json)
}

async fn remote_operator_pending(profile: &str, args: RemoteOperatorPendingArgs) -> Result<()> {
    let now = Utc::now();
    let mut approvals = approval_ledger(profile)?.load()?;
    if !args.all {
        approvals.retain(|approval| approval.status == ApprovalStatus::Pending);
    }
    approvals.sort_by_key(|approval| approval.created_at);
    let approval_views = pending_approval_operator_views(approvals, now);
    let approvals = approval_views
        .iter()
        .map(remote_operator_approval_summary)
        .collect::<Result<Vec<_>>>()?;
    let payload = RemoteOperatorPendingPayload {
        include_all: args.all,
        approval_count: approvals.len(),
        approvals,
    };
    let observed_hash = observed_hash_for(&payload)?;
    let card = remote_operator_pending_card(&payload, observed_hash);
    let projection = remote_operator_projection(profile, &args.transport, "pending", card, payload);
    print_remote_operator_projection(&projection, args.json)
}

async fn remote_operator_plans(profile: &str, args: RemoteOperatorPlansArgs) -> Result<()> {
    let filters = RemoteOperatorPlanFilters {
        project_key: args
            .project_key
            .clone()
            .map(|value| operator_safe_text(&value)),
        task_id: args.task_id.clone().map(|value| operator_safe_text(&value)),
        profile_key: args
            .profile_key
            .clone()
            .map(|value| operator_safe_text(&value)),
        artifact_kind: args
            .artifact_kind
            .clone()
            .map(|value| operator_safe_text(&value)),
        latest: args.latest,
    };
    let mut items = load_offdesk_plan_registry_items(profile)?;
    items.retain(|item| remote_operator_plan_matches_filter(item, &args));
    items.sort_by_key(|item| item.registration.registered_at);
    if args.latest {
        if let Some(latest) = items.pop() {
            items = vec![latest];
        }
    }
    let plans = items
        .iter()
        .map(remote_operator_plan_summary_from_item)
        .collect::<Result<Vec<_>>>()?;
    let payload = RemoteOperatorPlansPayload {
        filters,
        plan_count: plans.len(),
        plans,
    };
    let observed_hash = observed_hash_for(&payload)?;
    let card = remote_operator_plans_card(&payload, observed_hash);
    let projection = remote_operator_projection(profile, &args.transport, "plans", card, payload);
    print_remote_operator_projection(&projection, args.json)
}

async fn remote_operator_show(profile: &str, args: RemoteOperatorShowArgs) -> Result<()> {
    let items = load_offdesk_plan_registry_items(profile)?;
    let Some(item) = find_offdesk_plan_registry_item(items, &args.plan_ref) else {
        bail!("Registered Offdesk plan not found: {}", args.plan_ref);
    };
    let detail = offdesk_plan_registry_detail(item)?;
    let plan = remote_operator_plan_summary_from_detail(&detail)?;
    let reviews = detail
        .reviews
        .iter()
        .map(remote_operator_plan_review_summary)
        .collect();
    let launch_preps = detail
        .launch_preps
        .iter()
        .map(remote_operator_launch_prep_summary)
        .collect();
    let payload = RemoteOperatorPlanDetailPayload {
        plan,
        reviews,
        launch_preps,
        does_not_authorize: detail
            .registration
            .does_not_authorize
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
    };
    let observed_hash = observed_hash_for(&payload)?;
    let card = remote_operator_show_card(&payload, observed_hash);
    let projection = remote_operator_projection(profile, &args.transport, "show", card, payload);
    print_remote_operator_projection(&projection, args.json)
}

fn build_offdesk_plan_registration(
    profile: &str,
    args: &PlanArgs,
) -> Result<OffdeskPlanRegistration> {
    let source_bytes = fs::read(&args.input)
        .with_context(|| format!("read Offdesk plan artifact {}", args.input.display()))?;
    let source_value: Value = serde_json::from_slice(&source_bytes)
        .with_context(|| format!("parse Offdesk plan artifact {}", args.input.display()))?;
    let summary = validate_offdesk_plan_input(&source_value)?;
    let source_path = fs::canonicalize(&args.input).unwrap_or_else(|_| args.input.clone());
    let registered_at = Utc::now();
    let source_sha256 = {
        let mut hasher = Sha256::new();
        hasher.update(&source_bytes);
        format!("{:x}", hasher.finalize())
    };

    let artifacts = if args.dry_run {
        OffdeskPlanRegistrationArtifacts {
            registry_dir: None,
            registration_json: None,
            copied_source_json: None,
        }
    } else {
        let profile_dir = get_profile_dir(profile)?;
        let registry_dir =
            allocate_offdesk_plan_registry_dir(&profile_dir, registered_at, summary.artifact_kind)?;
        let copied_source = registry_dir.join("source.json");
        write_new_file(&copied_source, &source_bytes).with_context(|| {
            format!("write Offdesk plan source copy {}", copied_source.display())
        })?;
        OffdeskPlanRegistrationArtifacts {
            registry_dir: Some(registry_dir.display().to_string()),
            registration_json: Some(registry_dir.join("registration.json").display().to_string()),
            copied_source_json: Some(copied_source.display().to_string()),
        }
    };

    let registration = OffdeskPlanRegistration {
        schema: OFFDESK_PLAN_REGISTRATION_SCHEMA.to_string(),
        registered_at,
        forager_profile: profile.to_string(),
        source_path: source_path.display().to_string(),
        source_sha256,
        artifact_kind: summary.artifact_kind.to_string(),
        plan_schema: summary.plan_schema,
        profile_key: summary.profile_key,
        profile_name: summary.profile_name,
        project_key: args.project_key.clone(),
        request_id: args.request_id.clone(),
        task_id: args.task_id.clone(),
        ready_for_operator_review: summary.ready_for_operator_review,
        ready_for_launch_preparation: summary.ready_for_launch_preparation,
        ready_for_enqueue: summary.ready_for_enqueue,
        validation_failures: Vec::new(),
        decision: summary.decision,
        consensus: summary.consensus,
        selected_plan_path: summary.selected_plan_path,
        dry_run: args.dry_run,
        artifacts,
        does_not_authorize: offdesk_plan_registration_denials(),
    };

    if let Some(registration_path) = registration.artifacts.registration_json.as_deref() {
        let bytes = serde_json::to_vec_pretty(&registration)?;
        write_new_file(Path::new(registration_path), &bytes)
            .with_context(|| format!("write Offdesk plan registration {}", registration_path))?;
    }

    Ok(registration)
}

fn build_offdesk_plan_review_record(
    profile: &str,
    item: &OffdeskPlanRegistryItem,
    args: &PlanReviewArgs,
) -> Result<OffdeskPlanReviewRecord> {
    if args.reason.trim().is_empty() {
        bail!("Offdesk plan review reason is required");
    }
    if args.decision == OffdeskPlanReviewDecision::Approved && !args.blockers.is_empty() {
        bail!("approved Offdesk plan review cannot include blockers");
    }

    let reviewed_at = Utc::now();
    let registry_dir = offdesk_plan_registry_dir(item)?;
    let review_record_path = allocate_offdesk_plan_review_record_path(&registry_dir, reviewed_at)?;
    let ready_for_launch_preparation_candidate = args.decision
        == OffdeskPlanReviewDecision::Approved
        && item.registration.ready_for_operator_review
        && !item.registration.ready_for_launch_preparation
        && !item.registration.ready_for_enqueue
        && item.registration.validation_failures.is_empty();
    let profile_name = if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    };

    Ok(OffdeskPlanReviewRecord {
        schema: "offdesk_plan_review.v1".to_string(),
        reviewed_at,
        review_id: format!("plan_review_{}", short_uuid()),
        plan_id: item.plan_id.clone(),
        forager_profile: crate::offdesk::operator_safe_text(profile_name),
        registration_path: item.registration_path.clone(),
        source_sha256: item.registration.source_sha256.clone(),
        decision: args.decision,
        reviewer: crate::offdesk::operator_safe_text(args.reviewer.trim()),
        review_provider: args
            .review_provider
            .as_deref()
            .map(|value| crate::offdesk::operator_safe_text(value.trim())),
        review_file: args
            .review_file
            .as_ref()
            .map(|path| crate::offdesk::operator_safe_text(path.to_string_lossy().as_ref())),
        reason: truncate_closeout_text(
            &crate::offdesk::operator_safe_text(args.reason.trim()),
            2000,
        ),
        blockers: safe_text_list(&args.blockers),
        followups: safe_text_list(&args.followups),
        ready_for_launch_preparation_candidate,
        ready_for_enqueue: false,
        read_only_project_state: true,
        applies_file_operations: false,
        artifacts: OffdeskPlanReviewArtifacts {
            registration_json: item.registration_path.clone(),
            copied_source_json: item.registration.artifacts.copied_source_json.clone(),
            review_record_json: review_record_path.display().to_string(),
        },
        does_not_authorize: offdesk_plan_review_denials(),
    })
}

fn write_offdesk_plan_review_record(record: &OffdeskPlanReviewRecord) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(record)?;
    write_new_file(Path::new(&record.artifacts.review_record_json), &bytes)
        .with_context(|| format!("write {}", record.artifacts.review_record_json))?;
    Ok(())
}

fn build_offdesk_plan_launch_prep_packet(
    profile: &str,
    item: &OffdeskPlanRegistryItem,
    args: &PlanLaunchPrepArgs,
) -> Result<OffdeskPlanLaunchPrepPacket> {
    let registry_dir = offdesk_plan_registry_dir(item)?;
    let reviews = load_offdesk_plan_reviews(&registry_dir)?;
    let review = select_offdesk_plan_review(&reviews, args.review_id.as_deref())?;
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
    if review.source_sha256 != item.registration.source_sha256 {
        bail!(
            "Offdesk plan review {} source hash does not match registration",
            review.review_id
        );
    }

    let prepared_at = Utc::now();
    let launch_prep_path = allocate_offdesk_plan_launch_prep_path(&registry_dir, prepared_at)?;
    let mut required_first_reads = vec![
        item.registration_path.clone(),
        review.artifacts.review_record_json.clone(),
    ];
    if let Some(path) = item.registration.artifacts.copied_source_json.as_ref() {
        required_first_reads.push(path.clone());
    }
    if let Some(path) = item.registration.selected_plan_path.as_ref() {
        if !required_first_reads.contains(path) {
            required_first_reads.push(path.clone());
        }
    }
    let profile_name = if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    };

    Ok(OffdeskPlanLaunchPrepPacket {
        schema: "offdesk_plan_launch_prep.v1".to_string(),
        prepared_at,
        prep_id: format!("plan_launch_prep_{}", short_uuid()),
        plan_id: item.plan_id.clone(),
        forager_profile: crate::offdesk::operator_safe_text(profile_name),
        prepared_by: crate::offdesk::operator_safe_text(args.prepared_by.trim()),
        registration_path: item.registration_path.clone(),
        source_path: item.registration.source_path.clone(),
        source_sha256: item.registration.source_sha256.clone(),
        review_id: review.review_id.clone(),
        review_decision: review.decision,
        review_record_json: review.artifacts.review_record_json.clone(),
        artifact_kind: item.registration.artifact_kind.clone(),
        plan_schema: item.registration.plan_schema.clone(),
        profile_key: item.registration.profile_key.clone(),
        project_key: item.registration.project_key.clone(),
        request_id: item.registration.request_id.clone(),
        task_id: item.registration.task_id.clone(),
        selected_plan_path: item.registration.selected_plan_path.clone(),
        required_first_reads,
        launch_preparation_candidate: true,
        ready_for_launch: false,
        ready_for_enqueue: false,
        next_safe_action: "build_execution_brief_then_use_existing_offdesk_gate".to_string(),
        notes: args
            .notes
            .as_deref()
            .map(|value| truncate_closeout_text(&crate::offdesk::operator_safe_text(value), 2000)),
        read_only_project_state: true,
        applies_file_operations: false,
        artifacts: OffdeskPlanLaunchPrepArtifacts {
            registration_json: item.registration_path.clone(),
            copied_source_json: item.registration.artifacts.copied_source_json.clone(),
            review_record_json: review.artifacts.review_record_json.clone(),
            launch_prep_json: launch_prep_path.display().to_string(),
        },
        does_not_authorize: offdesk_plan_launch_prep_denials(),
    })
}

fn write_offdesk_plan_launch_prep_packet(packet: &OffdeskPlanLaunchPrepPacket) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(packet)?;
    write_new_file(Path::new(&packet.artifacts.launch_prep_json), &bytes)
        .with_context(|| format!("write {}", packet.artifacts.launch_prep_json))?;
    Ok(())
}

fn load_offdesk_plan_registry_items(profile: &str) -> Result<Vec<OffdeskPlanRegistryItem>> {
    let registry_dir = read_only_profile_dir(profile)?.join("offdesk_plans");
    if !registry_dir.exists() {
        return Ok(Vec::new());
    }

    let mut items = Vec::new();
    for entry in fs::read_dir(&registry_dir)
        .with_context(|| format!("read Offdesk plan registry {}", registry_dir.display()))?
    {
        let entry = entry.with_context(|| {
            format!(
                "read Offdesk plan registry entry {}",
                registry_dir.display()
            )
        })?;
        let file_type = entry.file_type().with_context(|| {
            format!(
                "read Offdesk plan registry entry type {}",
                entry.path().display()
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let registration_path = entry.path().join("registration.json");
        if !registration_path.exists() {
            continue;
        }
        let registration_bytes = fs::read(&registration_path).with_context(|| {
            format!(
                "read Offdesk plan registration {}",
                registration_path.display()
            )
        })?;
        let registration: OffdeskPlanRegistration = serde_json::from_slice(&registration_bytes)
            .with_context(|| {
                format!(
                    "parse Offdesk plan registration {}",
                    registration_path.display()
                )
            })?;
        let plan_id = entry.file_name().to_string_lossy().to_string();
        let reviews = load_offdesk_plan_reviews(&entry.path())?;
        let latest_review = reviews.last().cloned();
        let review_state = offdesk_plan_review_state(latest_review.as_ref());
        let launch_preps = load_offdesk_plan_launch_preps(&entry.path())?;
        let latest_launch_prep = launch_preps.last().cloned();
        items.push(OffdeskPlanRegistryItem {
            plan_id,
            registration_path: registration_path.display().to_string(),
            registration,
            review_state,
            review_count: reviews.len(),
            latest_review,
            launch_prep_count: launch_preps.len(),
            latest_launch_prep,
        });
    }

    Ok(items)
}

fn offdesk_plan_registry_detail(
    item: OffdeskPlanRegistryItem,
) -> Result<OffdeskPlanRegistryDetail> {
    let registry_dir = offdesk_plan_registry_dir(&item)?;
    let reviews = load_offdesk_plan_reviews(&registry_dir)?;
    let latest_review = reviews.last().cloned();
    let review_state = offdesk_plan_review_state(latest_review.as_ref());
    let launch_preps = load_offdesk_plan_launch_preps(&registry_dir)?;
    let latest_launch_prep = launch_preps.last().cloned();
    Ok(OffdeskPlanRegistryDetail {
        plan_id: item.plan_id,
        registration_path: item.registration_path,
        registration: item.registration,
        review_state,
        review_count: reviews.len(),
        latest_review,
        reviews,
        launch_prep_count: launch_preps.len(),
        latest_launch_prep,
        launch_preps,
    })
}

fn load_offdesk_plan_reviews(registry_dir: &Path) -> Result<Vec<OffdeskPlanReviewRecord>> {
    let mut reviews = Vec::new();
    if !registry_dir.exists() {
        return Ok(reviews);
    }
    for entry in fs::read_dir(registry_dir)
        .with_context(|| format!("read Offdesk plan registry {}", registry_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let filename = entry.file_name().to_string_lossy().to_string();
        if !filename.starts_with("plan_review_") || !filename.ends_with(".json") {
            continue;
        }
        let path = entry.path();
        let review_bytes = fs::read(&path)
            .with_context(|| format!("read Offdesk plan review {}", path.display()))?;
        let review: OffdeskPlanReviewRecord = serde_json::from_slice(&review_bytes)
            .with_context(|| format!("parse Offdesk plan review {}", path.display()))?;
        reviews.push(review);
    }
    reviews.sort_by_key(|review| review.reviewed_at);
    Ok(reviews)
}

fn load_offdesk_plan_launch_preps(registry_dir: &Path) -> Result<Vec<OffdeskPlanLaunchPrepPacket>> {
    let mut packets = Vec::new();
    if !registry_dir.exists() {
        return Ok(packets);
    }
    for entry in fs::read_dir(registry_dir)
        .with_context(|| format!("read Offdesk plan registry {}", registry_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let filename = entry.file_name().to_string_lossy().to_string();
        if !filename.starts_with("launch_prep_") || !filename.ends_with(".json") {
            continue;
        }
        let path = entry.path();
        let packet_bytes = fs::read(&path)
            .with_context(|| format!("read Offdesk plan launch-prep {}", path.display()))?;
        let packet: OffdeskPlanLaunchPrepPacket = serde_json::from_slice(&packet_bytes)
            .with_context(|| format!("parse Offdesk plan launch-prep {}", path.display()))?;
        packets.push(packet);
    }
    packets.sort_by_key(|packet| packet.prepared_at);
    Ok(packets)
}

fn select_offdesk_plan_review<'a>(
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

fn offdesk_plan_registry_dir(item: &OffdeskPlanRegistryItem) -> Result<PathBuf> {
    if let Some(registry_dir) = item.registration.artifacts.registry_dir.as_deref() {
        return Ok(PathBuf::from(registry_dir));
    }
    Path::new(&item.registration_path)
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow::anyhow!("registered Offdesk plan is missing registry directory"))
}

fn offdesk_plan_review_state(
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

fn offdesk_plan_matches_filter(item: &OffdeskPlanRegistryItem, args: &PlansArgs) -> bool {
    if let Some(project_key) = args.project_key.as_deref() {
        if item.registration.project_key.as_deref() != Some(project_key) {
            return false;
        }
    }
    if let Some(task_id) = args.task_id.as_deref() {
        if item.registration.task_id.as_deref() != Some(task_id) {
            return false;
        }
    }
    if let Some(profile_key) = args.profile_key.as_deref() {
        if item.registration.profile_key.as_deref() != Some(profile_key) {
            return false;
        }
    }
    if let Some(artifact_kind) = args.artifact_kind.as_deref() {
        if item.registration.artifact_kind != artifact_kind {
            return false;
        }
    }
    true
}

fn find_offdesk_plan_registry_item(
    items: Vec<OffdeskPlanRegistryItem>,
    plan_ref: &str,
) -> Option<OffdeskPlanRegistryItem> {
    let normalized_ref = normalize_offdesk_plan_ref_path(plan_ref);
    items.into_iter().find(|item| {
        if item.plan_id == plan_ref {
            return true;
        }
        if normalize_offdesk_plan_ref_path(&item.registration_path) == normalized_ref {
            return true;
        }
        for path in [
            item.registration.artifacts.registry_dir.as_deref(),
            item.registration.artifacts.registration_json.as_deref(),
            item.registration.artifacts.copied_source_json.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if normalize_offdesk_plan_ref_path(path) == normalized_ref {
                return true;
            }
        }
        false
    })
}

fn normalize_offdesk_plan_ref_path(path: &str) -> String {
    #[cfg(target_os = "macos")]
    {
        path.strip_prefix("/private").unwrap_or(path).to_owned()
    }
    #[cfg(not(target_os = "macos"))]
    {
        path.to_owned()
    }
}

fn allocate_offdesk_plan_review_record_path(
    registry_dir: &Path,
    reviewed_at: DateTime<Utc>,
) -> Result<PathBuf> {
    fs::create_dir_all(registry_dir)
        .with_context(|| format!("create Offdesk plan registry {}", registry_dir.display()))?;
    let timestamp = reviewed_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let filename = if attempt == 0 {
            format!("plan_review_{timestamp}.json")
        } else {
            format!("plan_review_{timestamp}_{attempt:03}.json")
        };
        let path = registry_dir.join(filename);
        if !path.exists() {
            return Ok(path);
        }
    }

    bail!(
        "could not allocate Offdesk plan review path in {}",
        registry_dir.display()
    )
}

fn allocate_offdesk_plan_launch_prep_path(
    registry_dir: &Path,
    prepared_at: DateTime<Utc>,
) -> Result<PathBuf> {
    fs::create_dir_all(registry_dir)
        .with_context(|| format!("create Offdesk plan registry {}", registry_dir.display()))?;
    let timestamp = prepared_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let filename = if attempt == 0 {
            format!("launch_prep_{timestamp}.json")
        } else {
            format!("launch_prep_{timestamp}_{attempt:03}.json")
        };
        let path = registry_dir.join(filename);
        if !path.exists() {
            return Ok(path);
        }
    }

    bail!(
        "could not allocate Offdesk plan launch-prep path in {}",
        registry_dir.display()
    )
}

fn allocate_offdesk_plan_registry_dir(
    profile_dir: &Path,
    registered_at: DateTime<Utc>,
    artifact_kind: &str,
) -> Result<PathBuf> {
    let base_dir = profile_dir.join("offdesk_plans");
    fs::create_dir_all(&base_dir)
        .with_context(|| format!("create Offdesk plan registry {}", base_dir.display()))?;
    let timestamp = registered_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let name = if attempt == 0 {
            format!("{timestamp}_{artifact_kind}")
        } else {
            format!("{timestamp}_{artifact_kind}_{attempt:03}")
        };
        let path = base_dir.join(name);
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create Offdesk plan registry {}", path.display()))
            }
        }
    }

    bail!(
        "could not allocate Offdesk plan registry path in {}",
        base_dir.display()
    )
}

fn validate_offdesk_plan_input(value: &Value) -> Result<OffdeskPlanInputSummary> {
    let plan_schema = value_string_field(value, "schema").unwrap_or_default();
    match plan_schema.as_str() {
        "offdesk_multiturn_plan.v1" => validate_multiturn_plan_input(value, plan_schema),
        "offdesk_planner_council.v1" => validate_planner_council_input(value, plan_schema),
        "" => bail!("Offdesk plan registration guard failed: schema_missing"),
        other => bail!("Offdesk plan registration guard failed: unsupported_schema:{other}"),
    }
}

fn validate_multiturn_plan_input(
    value: &Value,
    plan_schema: String,
) -> Result<OffdeskPlanInputSummary> {
    let mut failures = Vec::new();
    let decision = value.get("decision").filter(|entry| entry.is_object());
    if decision.is_none() {
        failures.push("decision_missing".to_string());
    }
    let ready_for_operator_review = require_bool_field(
        &mut failures,
        decision,
        "decision",
        "ready_for_operator_review",
        true,
    );
    let ready_for_launch_preparation = require_bool_field(
        &mut failures,
        decision,
        "decision",
        "ready_for_launch_preparation",
        false,
    );
    let ready_for_enqueue = require_bool_field(
        &mut failures,
        decision,
        "decision",
        "ready_for_enqueue",
        false,
    );
    match value.get("execution_sequence").and_then(Value::as_array) {
        Some(items) if !items.is_empty() => {}
        _ => failures.push("execution_sequence_missing".to_string()),
    }
    validate_plan_authority(value, &mut failures);
    fail_plan_registration_if_needed(failures)?;

    Ok(OffdeskPlanInputSummary {
        artifact_kind: "offdesk_multiturn_plan",
        plan_schema,
        profile_key: value_string_field(value, "profile_key"),
        profile_name: value_string_field(value, "profile_name"),
        ready_for_operator_review,
        ready_for_launch_preparation,
        ready_for_enqueue,
        decision: decision.cloned(),
        consensus: None,
        selected_plan_path: None,
    })
}

fn validate_planner_council_input(
    value: &Value,
    plan_schema: String,
) -> Result<OffdeskPlanInputSummary> {
    let mut failures = Vec::new();
    let consensus = value.get("consensus").filter(|entry| entry.is_object());
    if consensus.is_none() {
        failures.push("consensus_missing".to_string());
    }
    let ready_for_operator_review = require_bool_field(
        &mut failures,
        consensus,
        "consensus",
        "ready_for_operator_review",
        true,
    );
    let ready_for_launch_preparation = require_bool_field(
        &mut failures,
        consensus,
        "consensus",
        "ready_for_launch_preparation",
        false,
    );
    let ready_for_enqueue = require_bool_field(
        &mut failures,
        consensus,
        "consensus",
        "ready_for_enqueue",
        false,
    );
    match value.get("validation_failures").and_then(Value::as_array) {
        Some(items) if items.is_empty() => {}
        Some(items) => failures.push(format!("validation_failures_present:{}", items.len())),
        None => failures.push("validation_failures_missing".to_string()),
    }
    fail_plan_registration_if_needed(failures)?;

    Ok(OffdeskPlanInputSummary {
        artifact_kind: "offdesk_planner_council",
        plan_schema,
        profile_key: value_string_field(value, "profile_key"),
        profile_name: value_string_field(value, "profile_name"),
        ready_for_operator_review,
        ready_for_launch_preparation,
        ready_for_enqueue,
        decision: None,
        consensus: consensus.cloned(),
        selected_plan_path: value_string_field(value, "synthesized_plan_path"),
    })
}

fn validate_plan_authority(value: &Value, failures: &mut Vec<String>) {
    let authority = value.get("authority").filter(|entry| entry.is_object());
    if authority.is_none() {
        failures.push("authority_missing".to_string());
    }
    require_bool_field(failures, authority, "authority", "read_only_plan", true);
    let denials = authority
        .and_then(|entry| entry.get("does_not_authorize"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    for required in OFFDESK_PLAN_REQUIRED_DENIALS {
        if !denials.contains(required) {
            failures.push(format!("authority_missing:{required}"));
        }
    }
}

fn require_bool_field(
    failures: &mut Vec<String>,
    parent: Option<&Value>,
    parent_name: &str,
    field: &str,
    expected: bool,
) -> bool {
    match parent
        .and_then(|entry| entry.get(field))
        .and_then(Value::as_bool)
    {
        Some(actual) if actual == expected => actual,
        Some(actual) => {
            failures.push(format!("{parent_name}.{field}_must_be_{expected}"));
            actual
        }
        None => {
            failures.push(format!("{parent_name}.{field}_missing"));
            false
        }
    }
}

fn fail_plan_registration_if_needed(failures: Vec<String>) -> Result<()> {
    if !failures.is_empty() {
        bail!(
            "Offdesk plan registration guard failed: {}",
            failures.join(", ")
        );
    }
    Ok(())
}

fn value_string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn offdesk_plan_registration_denials() -> Vec<String> {
    OFFDESK_PLAN_REQUIRED_DENIALS
        .into_iter()
        .map(ToOwned::to_owned)
        .collect()
}

fn offdesk_plan_review_denials() -> Vec<String> {
    let mut denials = offdesk_plan_registration_denials();
    denials.push("launch preparation without a separate command".to_string());
    denials
}

fn offdesk_plan_launch_prep_denials() -> Vec<String> {
    let mut denials = offdesk_plan_review_denials();
    denials.push("dispatch".to_string());
    denials
}

fn remote_operator_projection<T>(
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

fn remote_operator_status_payload(status: Value) -> RemoteOperatorStatusPayload {
    RemoteOperatorStatusPayload {
        profile: json_string_field(&status, "profile").unwrap_or_else(|| "default".to_string()),
        waiting: json_usize_field(&status, "waiting"),
        running: json_usize_field(&status, "running"),
        idle: json_usize_field(&status, "idle"),
        stopped: json_usize_field(&status, "stopped"),
        error: json_usize_field(&status, "error"),
        total: json_usize_field(&status, "total"),
        resume_pending_fresh: json_usize_field(&status, "resume_pending_fresh"),
        resume_pending_stale: json_usize_field(&status, "resume_pending_stale"),
        pending_approvals: json_usize_field(&status, "pending_approvals"),
        queued_offdesk_tasks: json_usize_field(&status, "queued_offdesk_tasks"),
        active_offdesk_tasks: json_usize_field(&status, "active_offdesk_tasks"),
        offdesk_tasks_pending_approval: json_usize_field(&status, "offdesk_tasks_pending_approval"),
        failed_offdesk_tasks: json_usize_field(&status, "failed_offdesk_tasks"),
        resume_pending_offdesk_tasks: json_usize_field(&status, "resume_pending_offdesk_tasks"),
        cancelled_offdesk_tasks: json_usize_field(&status, "cancelled_offdesk_tasks"),
        stale_background_runs: json_usize_field(&status, "stale_background_runs"),
        failed_background_runs: json_usize_field(&status, "failed_background_runs"),
        closeout_required_offdesk_tasks: json_usize_field(
            &status,
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
) -> Result<RemoteOperatorApprovalSummary> {
    let approval = &view.approval;
    let core = RemoteOperatorApprovalSummaryCore {
        approval_id: operator_safe_text(&approval.approval_id),
        action_id: operator_safe_text(approval.action_id()),
        status: approval.status,
        expired: approval.status == ApprovalStatus::Pending && approval.expires_at < Utc::now(),
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

fn remote_operator_plan_matches_filter(
    item: &OffdeskPlanRegistryItem,
    args: &RemoteOperatorPlansArgs,
) -> bool {
    args.project_key.as_ref().map_or(true, |expected| {
        item.registration.project_key.as_deref() == Some(expected.as_str())
    }) && args.task_id.as_ref().map_or(true, |expected| {
        item.registration.task_id.as_deref() == Some(expected.as_str())
    }) && args.profile_key.as_ref().map_or(true, |expected| {
        item.registration.profile_key.as_deref() == Some(expected.as_str())
    }) && args.artifact_kind.as_ref().map_or(true, |expected| {
        item.registration.artifact_kind == *expected
    })
}

fn remote_operator_plan_summary_from_item(
    item: &OffdeskPlanRegistryItem,
) -> Result<RemoteOperatorPlanSummary> {
    let core = remote_operator_plan_summary_core(
        &item.plan_id,
        &item.registration,
        &item.review_state,
        item.review_count,
        item.latest_review.as_ref(),
        item.launch_prep_count,
        item.latest_launch_prep.as_ref(),
    );
    let observed_hash = observed_hash_for(&core)?;
    Ok(RemoteOperatorPlanSummary {
        core,
        observed_hash,
    })
}

fn remote_operator_plan_summary_from_detail(
    detail: &OffdeskPlanRegistryDetail,
) -> Result<RemoteOperatorPlanSummary> {
    let core = remote_operator_plan_summary_core(
        &detail.plan_id,
        &detail.registration,
        &detail.review_state,
        detail.review_count,
        detail.latest_review.as_ref(),
        detail.launch_prep_count,
        detail.latest_launch_prep.as_ref(),
    );
    let observed_hash = observed_hash_for(&core)?;
    Ok(RemoteOperatorPlanSummary {
        core,
        observed_hash,
    })
}

fn remote_operator_plan_summary_core(
    plan_id: &str,
    registration: &OffdeskPlanRegistration,
    review_state: &OffdeskPlanReviewState,
    review_count: usize,
    latest_review: Option<&OffdeskPlanReviewRecord>,
    launch_prep_count: usize,
    latest_launch_prep: Option<&OffdeskPlanLaunchPrepPacket>,
) -> RemoteOperatorPlanSummaryCore {
    RemoteOperatorPlanSummaryCore {
        plan_id: operator_safe_text(plan_id),
        artifact_kind: operator_safe_text(&registration.artifact_kind),
        plan_schema: operator_safe_text(&registration.plan_schema),
        profile_key: registration.profile_key.as_deref().map(operator_safe_text),
        project_key: registration.project_key.as_deref().map(operator_safe_text),
        request_id: registration.request_id.as_deref().map(operator_safe_text),
        task_id: registration.task_id.as_deref().map(operator_safe_text),
        registered_at: registration.registered_at,
        source_sha256: registration.source_sha256.clone(),
        review_status: operator_safe_text(&review_state.status),
        review_count,
        latest_review_id: latest_review
            .map(|review| operator_safe_text(&review.review_id))
            .or_else(|| {
                review_state
                    .latest_review_id
                    .as_deref()
                    .map(operator_safe_text)
            }),
        launch_prep_count,
        latest_launch_prep_id: latest_launch_prep.map(|packet| operator_safe_text(&packet.prep_id)),
        ready_for_operator_review: registration.ready_for_operator_review,
        launch_preparation_candidate: review_state.ready_for_launch_preparation_candidate,
        ready_for_enqueue: registration.ready_for_enqueue,
        next_safe_action: operator_safe_text(&review_state.next_safe_action),
        remote_actions: vec!["inspect_plan".to_string()],
    }
}

fn remote_operator_plan_review_summary(
    review: &OffdeskPlanReviewRecord,
) -> RemoteOperatorPlanReviewSummary {
    RemoteOperatorPlanReviewSummary {
        review_id: operator_safe_text(&review.review_id),
        reviewed_at: review.reviewed_at,
        decision: review.decision,
        reviewer: operator_safe_text(&review.reviewer),
        ready_for_launch_preparation_candidate: review.ready_for_launch_preparation_candidate,
        ready_for_enqueue: review.ready_for_enqueue,
        blockers: review
            .blockers
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
        followups: review
            .followups
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
    }
}

fn remote_operator_launch_prep_summary(
    packet: &OffdeskPlanLaunchPrepPacket,
) -> RemoteOperatorLaunchPrepSummary {
    RemoteOperatorLaunchPrepSummary {
        prep_id: operator_safe_text(&packet.prep_id),
        prepared_at: packet.prepared_at,
        review_id: operator_safe_text(&packet.review_id),
        launch_preparation_candidate: packet.launch_preparation_candidate,
        ready_for_launch: packet.ready_for_launch,
        ready_for_enqueue: packet.ready_for_enqueue,
        next_safe_action: operator_safe_text(&packet.next_safe_action),
    }
}

fn remote_operator_status_card(
    payload: &RemoteOperatorStatusPayload,
    observed_hash: String,
) -> RemoteOperatorCard {
    let mut detail_lines = Vec::new();
    for action in payload.next_safe_actions.iter().take(3) {
        detail_lines.push(format!("next: {} ({})", action.detail, action.kind));
    }
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
    let mut detail_lines = Vec::new();
    for approval in payload.approvals.iter().take(3) {
        detail_lines.push(format!(
            "{}: {} {}",
            approval.core.approval_id,
            approval.core.action,
            approval_status_label(approval.core.status)
        ));
    }
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

fn remote_operator_plans_card(
    payload: &RemoteOperatorPlansPayload,
    observed_hash: String,
) -> RemoteOperatorCard {
    let mut detail_lines = Vec::new();
    for plan in payload.plans.iter().take(3) {
        detail_lines.push(format!(
            "{}: {} review={}",
            plan.core.plan_id, plan.core.artifact_kind, plan.core.review_status
        ));
    }
    remote_operator_card(
        "Forager Remote Plans",
        vec![
            format!("plans: {}", payload.plan_count),
            format!(
                "filter project: {}",
                payload.filters.project_key.as_deref().unwrap_or("any")
            ),
            "remote plan review requires a registered artifact".to_string(),
        ],
        detail_lines,
        observed_hash,
        vec!["inspect_plans".to_string()],
    )
}

fn remote_operator_show_card(
    payload: &RemoteOperatorPlanDetailPayload,
    observed_hash: String,
) -> RemoteOperatorCard {
    remote_operator_card(
        "Forager Remote Plan Detail",
        vec![
            format!("plan: {}", payload.plan.core.plan_id),
            format!(
                "review: {} / launch-preps: {}",
                payload.plan.core.review_status,
                payload.launch_preps.len()
            ),
            format!("next: {}", payload.plan.core.next_safe_action),
        ],
        vec![
            format!("reviews: {}", payload.reviews.len()),
            "remote launch and mutation remain disabled".to_string(),
        ],
        observed_hash,
        vec!["inspect_plan".to_string()],
    )
}

fn remote_operator_card(
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

fn print_remote_operator_projection<T>(
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

fn observed_hash_for<T>(value: &T) -> Result<String>
where
    T: Serialize,
{
    let bytes = serde_json::to_vec(value)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
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

fn json_usize_field(value: &Value, field: &str) -> usize {
    value
        .get(field)
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or_default()
}

fn print_offdesk_plan_registration(
    registration: &OffdeskPlanRegistration,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(registration)?);
        return Ok(());
    }

    let verb = if registration.dry_run {
        "Validated"
    } else {
        "Registered"
    };
    println!(
        "{verb} Offdesk plan artifact: {} ({})",
        registration.artifact_kind, registration.plan_schema
    );
    println!("  source: {}", registration.source_path);
    println!(
        "  ready_for_operator_review: {}",
        registration.ready_for_operator_review
    );
    println!(
        "  ready_for_launch_preparation: {}",
        registration.ready_for_launch_preparation
    );
    println!("  ready_for_enqueue: {}", registration.ready_for_enqueue);
    if let Some(path) = registration.artifacts.registration_json.as_deref() {
        println!("  registration: {path}");
    }
    println!(
        "  note: registration does not authorize enqueue, launch, approval, file movement, cleanup, or accepted truth"
    );
    Ok(())
}

fn print_offdesk_plan_registry_items(items: &[OffdeskPlanRegistryItem]) {
    println!("Registered Offdesk plans");
    for item in items {
        let registration = &item.registration;
        println!(
            "- {} [{}] plan_review={} launch_candidate={} enqueue={}",
            item.plan_id,
            registration.artifact_kind,
            item.review_state.status,
            item.review_state.ready_for_launch_preparation_candidate,
            registration.ready_for_enqueue
        );
        println!("  next:    {}", item.review_state.next_safe_action);
        if let Some(packet) = item.latest_launch_prep.as_ref() {
            println!("  prep:    {}", packet.prep_id);
        }
        if let Some(project_key) = registration.project_key.as_deref() {
            println!("  project: {project_key}");
        }
        if let Some(task_id) = registration.task_id.as_deref() {
            println!("  task:    {task_id}");
        }
        println!("  source:  {}", registration.source_path);
    }
}

fn print_offdesk_plan_registry_detail(detail: &OffdeskPlanRegistryDetail) {
    let registration = &detail.registration;
    println!("Registered Offdesk plan: {}", detail.plan_id);
    println!("  kind:       {}", registration.artifact_kind);
    println!("  schema:     {}", registration.plan_schema);
    println!("  registered: {}", registration.registered_at);
    println!("  source:     {}", registration.source_path);
    println!("  sha256:     {}", registration.source_sha256);
    if let Some(profile_key) = registration.profile_key.as_deref() {
        println!("  profile:    {profile_key}");
    }
    if let Some(project_key) = registration.project_key.as_deref() {
        println!("  project:    {project_key}");
    }
    if let Some(request_id) = registration.request_id.as_deref() {
        println!("  request:    {request_id}");
    }
    if let Some(task_id) = registration.task_id.as_deref() {
        println!("  task:       {task_id}");
    }
    println!(
        "  ready_for_operator_review: {}",
        registration.ready_for_operator_review
    );
    println!(
        "  ready_for_launch_preparation: {}",
        registration.ready_for_launch_preparation
    );
    println!("  ready_for_enqueue: {}", registration.ready_for_enqueue);
    if let Some(path) = registration.selected_plan_path.as_deref() {
        println!("  selected_plan: {path}");
    }
    println!("  review_state: {}", detail.review_state.status);
    println!(
        "  launch_candidate: {}",
        detail.review_state.ready_for_launch_preparation_candidate
    );
    println!("  next:       {}", detail.review_state.next_safe_action);
    if let Some(review) = detail.latest_review.as_ref() {
        println!("  latest_review: {}", review.review_id);
        println!("  reviewer:   {}", review.reviewer);
        println!("  reason:     {}", review.reason);
    }
    if let Some(packet) = detail.latest_launch_prep.as_ref() {
        println!("  latest_launch_prep: {}", packet.prep_id);
        println!(
            "  launch_prep_file:   {}",
            packet.artifacts.launch_prep_json
        );
    }
    println!("  registration: {}", detail.registration_path);
    println!(
        "  does_not_authorize: {}",
        registration.does_not_authorize.join(", ")
    );
}

fn print_offdesk_plan_review_record(record: &OffdeskPlanReviewRecord) {
    println!("Offdesk plan review");
    println!("  reviewed_at:  {}", record.reviewed_at);
    println!("  review_id:    {}", record.review_id);
    println!("  plan_id:      {}", record.plan_id);
    println!("  decision:     {}", record.decision.as_str());
    println!("  reviewer:     {}", record.reviewer);
    if let Some(provider) = record.review_provider.as_deref() {
        println!("  provider:     {provider}");
    }
    println!("  reason:       {}", record.reason);
    println!(
        "  launch_candidate: {}",
        record.ready_for_launch_preparation_candidate
    );
    println!("  ready_for_enqueue: {}", record.ready_for_enqueue);
    println!("  project file mutations: none");
    println!("Artifacts:");
    println!("  registration: {}", record.artifacts.registration_json);
    println!("  review:       {}", record.artifacts.review_record_json);
    if !record.blockers.is_empty() {
        println!("Blockers:");
        for blocker in &record.blockers {
            println!("  - {blocker}");
        }
    }
    if !record.followups.is_empty() {
        println!("Follow-ups:");
        for followup in &record.followups {
            println!("  - {followup}");
        }
    }
    println!(
        "  note: review does not authorize enqueue, launch, approval, file movement, cleanup, or accepted truth"
    );
}

fn print_offdesk_plan_launch_prep_packet(packet: &OffdeskPlanLaunchPrepPacket) {
    println!("Offdesk plan launch-prep packet");
    println!("  prepared_at:  {}", packet.prepared_at);
    println!("  prep_id:      {}", packet.prep_id);
    println!("  plan_id:      {}", packet.plan_id);
    println!("  review_id:    {}", packet.review_id);
    println!("  prepared_by:  {}", packet.prepared_by);
    println!(
        "  launch_candidate: {}",
        packet.launch_preparation_candidate
    );
    println!("  ready_for_launch: {}", packet.ready_for_launch);
    println!("  ready_for_enqueue: {}", packet.ready_for_enqueue);
    println!("  next:         {}", packet.next_safe_action);
    println!("Artifacts:");
    println!("  registration: {}", packet.artifacts.registration_json);
    println!("  review:       {}", packet.artifacts.review_record_json);
    println!("  launch_prep:  {}", packet.artifacts.launch_prep_json);
    if !packet.required_first_reads.is_empty() {
        println!("Required first reads:");
        for path in &packet.required_first_reads {
            println!("  - {path}");
        }
    }
    println!(
        "  note: launch-prep packet does not authorize enqueue, launch, approval, file movement, cleanup, or accepted truth"
    );
}

fn hosted_harness_profile(id: &str) -> Option<&'static HostedHarnessProfileView> {
    HOSTED_HARNESS_PROFILES
        .iter()
        .find(|profile| profile.id.eq_ignore_ascii_case(id))
}

fn build_harness_prompt_packet(
    profile: &HostedHarnessProfileView,
    args: HarnessPromptArgs,
) -> Result<HostedHarnessPromptPacket> {
    let first_read_total_budget_bytes = args
        .max_first_read_total_bytes
        .unwrap_or(profile.prompt_contract.first_read_total_budget_bytes);
    let first_read_file_budget_bytes = profile.prompt_contract.first_read_file_budget_bytes;
    let first_reads = args
        .first_reads
        .iter()
        .map(|path| {
            let size_bytes = path
                .metadata()
                .ok()
                .filter(|meta| meta.is_file())
                .map(|meta| meta.len());
            HostedHarnessFirstRead {
                path: path.display().to_string(),
                present: path.exists(),
                size_bytes,
                over_file_budget: size_bytes
                    .is_some_and(|size| size > first_read_file_budget_bytes),
            }
        })
        .collect::<Vec<_>>();
    let first_read_total_bytes = first_reads
        .iter()
        .filter_map(|read| read.size_bytes)
        .sum::<u64>();
    let workdir = args.workdir.map(|path| path.display().to_string());
    let result_artifact = args.result_artifact.map(|path| path.display().to_string());
    let output_path = args.output.map(|path| path.display().to_string());
    let mut warnings = Vec::new();
    let mut first_read_budget_warning = false;
    if profile.prompt_contract.first_read_required && first_reads.is_empty() {
        first_read_budget_warning = true;
        warnings.push("no first-read artifacts were provided".to_string());
    }
    let missing_first_reads = first_reads.iter().filter(|read| !read.present).count();
    if missing_first_reads > 0 {
        first_read_budget_warning = true;
        warnings.push(format!(
            "{missing_first_reads} first-read artifact(s) are missing"
        ));
    }
    for read in first_reads.iter().filter(|read| read.over_file_budget) {
        if let Some(size) = read.size_bytes {
            first_read_budget_warning = true;
            warnings.push(format!(
                "first-read artifact {} is {} bytes; profile file budget is {} bytes",
                read.path, size, first_read_file_budget_bytes
            ));
        }
    }
    if first_read_total_bytes > first_read_total_budget_bytes {
        first_read_budget_warning = true;
        warnings.push(format!(
            "first-read artifacts total {} bytes; budget is {} bytes",
            first_read_total_bytes, first_read_total_budget_bytes
        ));
    }
    if args.task.len() > profile.prompt_contract.inline_context_budget_bytes {
        warnings.push(format!(
            "task text is {} bytes; profile inline budget is {} bytes",
            args.task.len(),
            profile.prompt_contract.inline_context_budget_bytes
        ));
    }
    let prompt_markdown = render_harness_prompt_markdown(
        profile,
        &args.task,
        workdir.as_deref(),
        &first_reads,
        result_artifact.as_deref(),
    );

    Ok(HostedHarnessPromptPacket {
        harness_id: profile.id.to_string(),
        display_name: profile.display_name.to_string(),
        support_status: profile.support_status.to_string(),
        prompt_strategy: profile.prompt_contract.strategy.to_string(),
        inline_context_budget_bytes: profile.prompt_contract.inline_context_budget_bytes,
        first_read_file_budget_bytes,
        first_read_total_budget_bytes,
        first_read_required: profile.prompt_contract.first_read_required,
        first_read_total_bytes,
        first_read_budget_status: if first_read_budget_warning {
            "warning"
        } else {
            "ok"
        }
        .to_string(),
        task: args.task,
        workdir,
        first_reads,
        result_artifact,
        output_path,
        warnings,
        prompt_markdown,
    })
}

fn render_harness_prompt_markdown(
    profile: &HostedHarnessProfileView,
    task: &str,
    workdir: Option<&str>,
    first_reads: &[HostedHarnessFirstRead],
    result_artifact: Option<&str>,
) -> String {
    let mut output = String::new();
    output.push_str("# Hosted Harness Start Packet\n\n");
    output.push_str(&format!(
        "- harness: {} (`{}`)\n",
        profile.display_name, profile.id
    ));
    output.push_str(&format!(
        "- strategy: `{}`\n",
        profile.prompt_contract.strategy
    ));
    output.push_str(&format!(
        "- inline_context_budget_bytes: `{}`\n",
        profile.prompt_contract.inline_context_budget_bytes
    ));
    output.push_str(&format!(
        "- first_read_file_budget_bytes: `{}`\n",
        profile.prompt_contract.first_read_file_budget_bytes
    ));
    output.push_str(&format!(
        "- first_read_total_budget_bytes: `{}`\n",
        profile.prompt_contract.first_read_total_budget_bytes
    ));
    if let Some(workdir) = workdir {
        output.push_str(&format!("- workdir: `{workdir}`\n"));
    }
    if let Some(result_artifact) = result_artifact {
        output.push_str(&format!("- result_artifact: `{result_artifact}`\n"));
    }
    output.push_str("\n## Task\n\n");
    output.push_str(task.trim());
    output.push_str("\n\n## Operating Contract\n\n");
    output.push_str("- Use this compact prompt as the instruction surface.\n");
    output.push_str("- Read the first-read artifacts before making a decision.\n");
    output.push_str(
        "- Do not ask the operator to paste full git diffs, raw logs, or scrollback inline.\n",
    );
    output.push_str(
        "- Summarize missing context as explicit missing evidence instead of guessing.\n",
    );
    output
        .push_str("- Write or inspect the declared result artifact before reporting completion.\n");
    output.push_str("\n## First-Read Artifacts\n\n");
    if first_reads.is_empty() {
        output.push_str(
            "- None provided. Ask for a first-read artifact before using large inline context.\n",
        );
    } else {
        for read in first_reads {
            let present = if read.present { "present" } else { "missing" };
            let size = read
                .size_bytes
                .map(|bytes| format!(", {bytes} bytes"))
                .unwrap_or_default();
            let budget = if read.over_file_budget {
                ", over file budget"
            } else {
                ""
            };
            output.push_str(&format!("- `{}` ({present}{size}{budget})\n", read.path));
        }
    }
    output.push_str("\n## Response Contract\n\n");
    output.push_str("- verdict: pass, caution, or fail\n");
    output.push_str("- evidence_read: paths actually read\n");
    output.push_str("- strongest_positive_signal\n");
    output.push_str("- strongest_risk\n");
    output.push_str("- one_next_action\n");
    output
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
        Duration::hours(168),
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
        Duration::hours(6),
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

async fn deck(_profile: &str, args: DeckArgs) -> Result<()> {
    let json = args.json;
    let report = build_deck_report(&args)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    print_deck_report(&report);
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

fn build_deck_report(args: &DeckArgs) -> Result<OffdeskDeckReport> {
    let generated_at = Utc::now();
    let source = read_json_file(&args.source)?;
    let source_kind = detect_offdesk_deck_kind(&source, args.kind);
    let title = args
        .title
        .as_deref()
        .map(operator_safe_text)
        .unwrap_or_else(|| default_offdesk_deck_title(&source, source_kind));
    let markdown_path = args
        .out
        .clone()
        .unwrap_or_else(|| default_offdesk_deck_path(&args.source));
    let markdown =
        render_offdesk_deck_markdown(&source, source_kind, &title, &args.source, generated_at);
    write_deck_artifact(&markdown_path, markdown.as_bytes(), args.force)
        .with_context(|| format!("write Marp deck {}", markdown_path.display()))?;

    let mut rendered_path = None;
    let mut render_format = None;
    let render_status = if let Some(format) = args.render {
        let output_path = offdesk_deck_render_path(&markdown_path, format);
        render_offdesk_deck_with_marp(
            &args.marp_bin,
            &markdown_path,
            &output_path,
            format,
            args.force,
        )?;
        rendered_path = Some(output_path.display().to_string());
        render_format = Some(format.as_str().to_string());
        "rendered".to_string()
    } else {
        "not_requested".to_string()
    };

    Ok(OffdeskDeckReport {
        schema: "offdesk_marp_deck.v1",
        generated_at,
        source_path: args.source.display().to_string(),
        source_kind: source_kind.as_str().to_string(),
        marp_markdown_path: markdown_path.display().to_string(),
        rendered_path,
        render_format,
        render_status,
        render_error: None,
        source_of_truth: "source JSON remains authoritative",
    })
}

fn detect_offdesk_deck_kind(value: &Value, explicit: OffdeskDeckKind) -> OffdeskDeckKind {
    if explicit != OffdeskDeckKind::Auto {
        return explicit;
    }
    if value.get("closeout_id").is_some()
        || value.get("closeout_receipt").is_some()
        || value.pointer("/artifacts/closeout_plan_json").is_some()
    {
        return OffdeskDeckKind::Closeout;
    }
    if value.get("plan_id").is_some()
        || value.get("ready_for_operator_review").is_some()
        || value.get("does_not_authorize").is_some()
        || value.get("launch_prep_id").is_some()
    {
        return OffdeskDeckKind::Plan;
    }
    OffdeskDeckKind::Status
}

fn default_offdesk_deck_title(value: &Value, kind: OffdeskDeckKind) -> String {
    match kind {
        OffdeskDeckKind::Closeout => deck_text_at(value, "/closeout_id")
            .map(|id| format!("Offdesk Closeout {id}"))
            .unwrap_or_else(|| "Offdesk Closeout Review".to_string()),
        OffdeskDeckKind::Plan => deck_text_at(value, "/plan_id")
            .or_else(|| deck_text_at(value, "/launch_prep_id"))
            .map(|id| format!("Offdesk Plan {id}"))
            .unwrap_or_else(|| "Offdesk Plan Review".to_string()),
        OffdeskDeckKind::Status | OffdeskDeckKind::Auto => "Offdesk Status Review".to_string(),
    }
}

fn default_offdesk_deck_path(source_path: &Path) -> PathBuf {
    let parent = source_path.parent().unwrap_or_else(|| Path::new(""));
    let stem = source_path
        .file_stem()
        .and_then(|value| value.to_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("offdesk_artifact");
    parent.join(format!("{stem}.marp.md"))
}

fn write_deck_artifact(path: &Path, bytes: &[u8], force: bool) -> io::Result<usize> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    if force {
        fs::write(path, bytes)?;
        Ok(bytes.len())
    } else {
        write_new_file(path, bytes)
    }
}

fn offdesk_deck_render_path(markdown_path: &Path, format: OffdeskDeckRenderFormat) -> PathBuf {
    let extension = format.extension();
    if let Some(file_name) = markdown_path.file_name().and_then(|value| value.to_str()) {
        if let Some(stem) = file_name.strip_suffix(".marp.md") {
            return markdown_path.with_file_name(format!("{stem}.{extension}"));
        }
    }
    markdown_path.with_extension(extension)
}

fn render_offdesk_deck_with_marp(
    marp_bin: &str,
    markdown_path: &Path,
    output_path: &Path,
    format: OffdeskDeckRenderFormat,
    force: bool,
) -> Result<()> {
    if output_path.exists() && !force {
        bail!(
            "render output already exists: {} (use --force to overwrite)",
            output_path.display()
        );
    }
    if let Some(parent) = output_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("create render output directory {}", parent.display()))?;
    }
    let output = Command::new(marp_bin)
        .arg(markdown_path)
        .arg("--output")
        .arg(output_path)
        .output()
        .with_context(|| {
            format!(
                "run Marp CLI `{}` for {} output",
                operator_safe_text(marp_bin),
                format.as_str()
            )
        })?;
    if !output.status.success() {
        let stdout = operator_safe_text(String::from_utf8_lossy(&output.stdout).trim());
        let stderr = operator_safe_text(String::from_utf8_lossy(&output.stderr).trim());
        bail!(
            "Marp CLI failed for {} output (status: {}, stdout: {}, stderr: {})",
            format.as_str(),
            output.status,
            stdout,
            stderr
        );
    }
    Ok(())
}

fn render_offdesk_deck_markdown(
    value: &Value,
    kind: OffdeskDeckKind,
    title: &str,
    source_path: &Path,
    generated_at: DateTime<Utc>,
) -> String {
    let mut output = String::new();
    output.push_str("---\n");
    output.push_str("marp: true\n");
    output.push_str("theme: default\n");
    output.push_str("paginate: true\n");
    output.push_str("---\n\n");
    output.push_str(&format!("# {}\n\n", deck_escape_markdown(title)));
    output.push_str(&format!(
        "- source_kind: `{}`\n",
        deck_escape_markdown(kind.as_str())
    ));
    output.push_str(&format!(
        "- source_file: `{}`\n",
        deck_escape_markdown(&deck_file_name(source_path))
    ));
    output.push_str(&format!("- generated_at: `{generated_at}`\n"));
    output.push_str("- source_of_truth: source JSON remains authoritative\n");
    output.push_str("- boundary: review surface only; does not approve or execute work\n");

    match kind {
        OffdeskDeckKind::Closeout => render_closeout_deck_slides(&mut output, value),
        OffdeskDeckKind::Plan => render_plan_deck_slides(&mut output, value),
        OffdeskDeckKind::Status | OffdeskDeckKind::Auto => {
            render_status_deck_slides(&mut output, value)
        }
    }

    output
}

fn render_closeout_deck_slides(output: &mut String, value: &Value) {
    output.push_str("\n---\n\n## Closeout State\n\n");
    deck_push_optional(output, "closeout_id", deck_text_at(value, "/closeout_id"));
    deck_push_optional(output, "profile", deck_text_at(value, "/profile"));
    deck_push_optional(
        output,
        "completed_tasks",
        deck_text_at(value, "/summary/completed_tasks"),
    );
    deck_push_optional(
        output,
        "active_or_blocked_tasks",
        deck_text_at(value, "/summary/active_or_blocked_tasks"),
    );
    deck_push_optional(
        output,
        "missing_artifacts",
        deck_text_at(value, "/summary/missing_artifacts"),
    );
    deck_push_optional(
        output,
        "return_package_required",
        deck_text_at(value, "/summary/return_package_required"),
    );
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Review And Decisions\n\n");
    deck_push_count(
        output,
        "open_decisions",
        deck_array_len_at(value, "/open_decisions"),
    );
    deck_push_count(
        output,
        "verification_commands",
        deck_array_len_at(value, "/verification_commands"),
    );
    deck_push_count(
        output,
        "required_first_reads",
        deck_array_len_at(value, "/required_first_reads"),
    );
    deck_push_limited_items(
        output,
        "open decision",
        deck_items_at(value, "/open_decisions", 5),
    );
    deck_push_limited_items(
        output,
        "verification",
        deck_items_at(value, "/verification_commands", 4),
    );
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Evidence Surface\n\n");
    deck_push_optional(
        output,
        "artifact_dir",
        deck_text_at(value, "/artifact_dir").map(deck_file_name_from_text),
    );
    deck_push_limited_items(output, "artifact", deck_artifact_items(value, 8));
    deck_push_limited_items(
        output,
        "first read",
        deck_items_at(value, "/required_first_reads", 5),
    );
    deck_push_empty_if_needed(output);
}

fn render_plan_deck_slides(output: &mut String, value: &Value) {
    output.push_str("\n---\n\n## Plan Identity\n\n");
    for pointer in [
        ("/plan_id", "plan_id"),
        ("/launch_prep_id", "launch_prep_id"),
        ("/project_key", "project_key"),
        ("/request_id", "request_id"),
        ("/task_id", "task_id"),
        ("/review_status", "review_status"),
        ("/ready_for_operator_review", "ready_for_operator_review"),
    ] {
        deck_push_optional(output, pointer.1, deck_text_at(value, pointer.0));
    }
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Operator Boundary\n\n");
    deck_push_optional(
        output,
        "next_safe_action",
        deck_text_at(value, "/next_safe_action"),
    );
    deck_push_limited_items(
        output,
        "does not authorize",
        deck_items_at(value, "/does_not_authorize", 6),
    );
    deck_push_limited_items(
        output,
        "validation failure",
        deck_items_at(value, "/validation_failures", 6),
    );
    deck_push_limited_items(output, "blocker", deck_items_at(value, "/blockers", 6));
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Follow-Up\n\n");
    deck_push_limited_items(output, "follow-up", deck_items_at(value, "/followups", 6));
    deck_push_limited_items(output, "approval", deck_items_at(value, "/approvals", 6));
    deck_push_limited_items(output, "artifact", deck_artifact_items(value, 8));
    deck_push_empty_if_needed(output);
}

fn render_status_deck_slides(output: &mut String, value: &Value) {
    output.push_str("\n---\n\n## Runtime Status\n\n");
    for pointer in [
        ("/status", "status"),
        ("/state", "state"),
        ("/remote_status", "remote_status"),
        ("/agent_runtime_status", "agent_runtime_status"),
        ("/listener_status", "listener_status"),
        ("/model", "model"),
        ("/endpoint", "endpoint"),
    ] {
        deck_push_optional(output, pointer.1, deck_text_at(value, pointer.0));
    }
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Queue And Attention\n\n");
    for pointer in [
        ("/pending_approvals", "pending_approvals"),
        ("/queued_offdesk_tasks", "queued_offdesk_tasks"),
        ("/active_offdesk_tasks", "active_offdesk_tasks"),
        ("/failed_offdesk_tasks", "failed_offdesk_tasks"),
        (
            "/closeout_required_offdesk_tasks",
            "closeout_required_offdesk_tasks",
        ),
    ] {
        deck_push_count(output, pointer.1, deck_array_len_at(value, pointer.0));
    }
    deck_push_limited_items(
        output,
        "pending approval",
        deck_items_at(value, "/pending_approvals", 5),
    );
    deck_push_limited_items(output, "task", deck_items_at(value, "/tasks", 5));
    deck_push_empty_if_needed(output);

    output.push_str("\n---\n\n## Source Keys\n\n");
    deck_push_limited_items(output, "top-level key", deck_top_level_keys(value, 10));
    deck_push_empty_if_needed(output);
}

fn deck_push_optional(output: &mut String, label: &str, value: Option<String>) {
    if let Some(value) = value.filter(|value| !value.trim().is_empty()) {
        output.push_str(&format!(
            "- {}: {}\n",
            deck_escape_markdown(label),
            deck_escape_markdown(&value)
        ));
    }
}

fn deck_push_count(output: &mut String, label: &str, count: Option<usize>) {
    if let Some(count) = count {
        output.push_str(&format!("- {}: {}\n", deck_escape_markdown(label), count));
    }
}

fn deck_push_limited_items(output: &mut String, label: &str, items: Vec<String>) {
    for item in items {
        output.push_str(&format!(
            "- {}: {}\n",
            deck_escape_markdown(label),
            deck_escape_markdown(&item)
        ));
    }
}

fn deck_push_empty_if_needed(output: &mut String) {
    let slide = output.rsplit("\n---\n\n").next().unwrap_or(output.as_str());
    if !slide.lines().any(|line| line.starts_with("- ")) {
        output.push_str("- No matching fields found in this artifact.\n");
    }
}

fn deck_text_at(value: &Value, pointer: &str) -> Option<String> {
    let value = value.pointer(pointer)?;
    deck_value_text(value)
}

fn deck_value_text(value: &Value) -> Option<String> {
    let text = match value {
        Value::String(text) => text.clone(),
        Value::Bool(value) => value.to_string(),
        Value::Number(value) => value.to_string(),
        Value::Null | Value::Array(_) | Value::Object(_) => return None,
    };
    Some(operator_safe_text(text.trim()))
}

fn deck_array_len_at(value: &Value, pointer: &str) -> Option<usize> {
    value.pointer(pointer)?.as_array().map(Vec::len)
}

fn deck_items_at(value: &Value, pointer: &str, limit: usize) -> Vec<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(deck_item_summary)
                .take(limit)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn deck_item_summary(value: &Value) -> Option<String> {
    if let Some(text) = deck_value_text(value) {
        return Some(text);
    }
    let object = value.as_object()?;
    for key in [
        "kind",
        "detail",
        "summary",
        "title",
        "task_id",
        "request_id",
        "project_key",
        "path",
        "command",
        "reason",
        "status",
        "action",
    ] {
        if let Some(text) = object.get(key).and_then(deck_value_text) {
            return Some(format!("{key}: {text}"));
        }
    }
    Some(format!("object with {} field(s)", object.len()))
}

fn deck_artifact_items(value: &Value, limit: usize) -> Vec<String> {
    let mut items = Vec::new();
    if let Some(artifacts) = value.get("artifacts").and_then(Value::as_object) {
        for (key, value) in artifacts.iter().take(limit) {
            if let Some(text) = deck_value_text(value) {
                items.push(format!("{key}: {}", deck_file_name_from_text(text)));
            }
        }
    }
    items
}

fn deck_top_level_keys(value: &Value, limit: usize) -> Vec<String> {
    value
        .as_object()
        .map(|object| {
            object
                .keys()
                .take(limit)
                .map(|key| operator_safe_text(key))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn deck_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(operator_safe_text)
        .unwrap_or_else(|| operator_safe_text(path.to_string_lossy().as_ref()))
}

fn deck_file_name_from_text(path: String) -> String {
    Path::new(&path)
        .file_name()
        .and_then(|value| value.to_str())
        .map(operator_safe_text)
        .unwrap_or(path)
}

fn deck_escape_markdown(text: &str) -> String {
    operator_safe_text(text)
        .replace(['\n', '\r'], " ")
        .replace('|', "\\|")
}

fn build_closeout_decision_record(
    profile: &str,
    args: &CloseoutDecisionArgs,
) -> Result<CloseoutReviewRecord> {
    let profile_dir = get_profile_dir(profile)?;
    let profile_name = if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    };
    let kind = require_non_empty_arg("--kind", args.kind.trim())?;
    let reason = require_non_empty_arg("--reason", args.reason.trim())?;
    if args.decision == CloseoutDecisionResolution::PreserveInPlace && kind != "archive_review" {
        bail!(
            "preserve-in-place closeout decisions are currently supported only for archive_review"
        );
    }

    let artifact_dir = resolve_closeout_artifact_dir_for(
        &profile_dir,
        args.closeout_id.as_deref(),
        args.artifact_dir.as_ref(),
    )?;
    let plan_path = artifact_dir.join("closeout_plan.json");
    let plan: Value = serde_json::from_str(
        &fs::read_to_string(&plan_path)
            .with_context(|| format!("read closeout plan {}", plan_path.display()))?,
    )
    .with_context(|| format!("parse closeout plan {}", plan_path.display()))?;
    let closeout_id = closeout_id_from_plan(&plan)?;
    if let Some(expected) = args.closeout_id.as_deref() {
        let expected = crate::offdesk::operator_safe_text(expected);
        if expected != closeout_id {
            bail!(
                "closeout id mismatch: requested {}, artifact contains {}",
                expected,
                closeout_id
            );
        }
    }

    let (source_review_record_path, _source_reviewed_at, source_review) =
        latest_closeout_review_value(&artifact_dir)?;
    let source_receipt = source_review
        .get("closeout_receipt")
        .ok_or_else(|| anyhow::anyhow!("latest closeout review has no closeout_receipt"))?;
    let source_acceptance_status = source_receipt
        .get("acceptance_status")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    if source_acceptance_status == "accepted" {
        bail!("closeout receipt is already accepted");
    }
    let source_verdict = source_review
        .get("verdict")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    if source_verdict != "approved" || source_acceptance_status != "approved_with_followups" {
        bail!("closeout decisions can only resolve approved closeout receipts with follow-ups");
    }

    let source_open_decisions = receipt_decisions_from_value(source_receipt);
    let mut matched_decisions = Vec::new();
    let mut remaining_decisions = Vec::new();
    for decision in source_open_decisions {
        if decision.kind == kind {
            matched_decisions.push(decision);
        } else {
            remaining_decisions.push(decision);
        }
    }
    if matched_decisions.is_empty() {
        bail!("latest closeout receipt has no open decision of kind {kind}");
    }

    let missing_evidence = receipt_string_list(source_receipt, "missing_evidence");
    let required_first_reads = receipt_string_list(source_receipt, "required_first_reads");
    let unsafe_operations = receipt_string_list(source_receipt, "unsafe_operations");
    let stale_task_count = source_receipt
        .get("stale_task_count")
        .and_then(Value::as_u64)
        .unwrap_or_default() as usize;
    if stale_task_count > 0 {
        bail!("closeout decision resolution cannot accept a stale closeout; regenerate closeout first");
    }
    if closeout_receipt_evidence_status(source_receipt) == "missing" {
        bail!("closeout decision resolution cannot bypass missing evidence status");
    }
    if !missing_evidence.is_empty()
        || !required_first_reads.is_empty()
        || !unsafe_operations.is_empty()
    {
        bail!("closeout decision resolution cannot bypass missing evidence, required reads, or unsafe operations");
    }
    if matches!(
        source_receipt
            .get("wiki_promotion_state")
            .and_then(Value::as_str),
        Some("review_required" | "audit_unavailable")
    ) {
        bail!("closeout decision resolution cannot bypass wiki promotion follow-ups");
    }

    let reviewed_at = Utc::now();
    let review_id = format!("closeout_decision_{}", short_uuid());
    let review_record_path = allocate_closeout_review_record_path(&artifact_dir, reviewed_at)?;
    let receipt_path = allocate_closeout_receipt_path(&artifact_dir, reviewed_at)?;
    let return_package_path = closeout_return_package_path(&artifact_dir, &plan);
    let artifacts = CloseoutReviewArtifactPaths {
        closeout_plan_json: plan_path.display().to_string(),
        review_record_json: review_record_path.display().to_string(),
        closeout_receipt_json: receipt_path.display().to_string(),
        return_package_markdown: return_package_path.display().to_string(),
    };
    let closeout_generated_at = closeout_generated_at_from_plan(&plan);
    let applies_to_tasks = closeout_review_task_refs_from_plan(&plan);
    let applies_to_task_ids = applies_to_tasks
        .iter()
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    let executed_scope = closeout_executed_scope(&applies_to_tasks);
    let accepted_scope = executed_scope.clone();
    let reviewer = crate::offdesk::operator_safe_text(args.reviewer.trim());
    let reason = truncate_closeout_text(&crate::offdesk::operator_safe_text(reason), 2000);
    let does_not_authorize = closeout_decision_does_not_authorize();
    let source_receipt_id = source_receipt
        .get("receipt_id")
        .and_then(Value::as_str)
        .map(crate::offdesk::operator_safe_text);
    let resolved_open_decisions = matched_decisions
        .iter()
        .cloned()
        .map(|decision| CloseoutResolvedDecision {
            kind: kind.to_string(),
            decision: args.decision.as_str().to_string(),
            reason: reason.clone(),
            reviewer: reviewer.clone(),
            resolved_at: reviewed_at,
            applies_to_decision: decision,
            does_not_authorize: does_not_authorize.clone(),
        })
        .collect::<Vec<_>>();
    let decision_resolution = CloseoutDecisionResolutionRecord {
        kind: kind.to_string(),
        decision: args.decision.as_str().to_string(),
        reason: reason.clone(),
        reviewer: reviewer.clone(),
        resolved_at: reviewed_at,
        source_review_record_json: source_review_record_path.display().to_string(),
        source_receipt_id,
        does_not_authorize: does_not_authorize.clone(),
    };
    let retention_review = closeout_retention_status_after_resolution(
        source_receipt,
        args.decision,
        remaining_decisions.is_empty(),
    );
    let has_followups = !remaining_decisions.is_empty()
        || matches!(retention_review, "required")
        || matches!(
            source_receipt
                .get("wiki_promotion_state")
                .and_then(Value::as_str),
            Some("review_required" | "audit_unavailable")
        );
    let acceptance_status = if has_followups {
        "approved_with_followups"
    } else {
        "accepted"
    };
    let verification_status = if has_followups { "pending" } else { "recorded" };
    let next_safe_action = if acceptance_status == "accepted" {
        "Rehydrate Ondesk from the return package and continue under reviewed evidence.".to_string()
    } else {
        closeout_receipt_next_safe_action(
            acceptance_status,
            stale_task_count,
            &remaining_decisions,
            &missing_evidence,
            &required_first_reads,
        )
    };
    let closeout_receipt = CloseoutReceipt {
        schema: "closeout_receipt.v1",
        receipt_id: format!("closeout_receipt_{}", short_uuid()),
        closeout_id: closeout_id.clone(),
        review_id: review_id.clone(),
        generated_at: closeout_generated_at.unwrap_or(reviewed_at),
        reviewed_at,
        verdict: CloseoutReviewVerdict::Approved,
        acceptance_status,
        accepted_scope,
        executed_scope,
        evidence_status: closeout_receipt_evidence_status(source_receipt),
        verification_status,
        open_decisions: remaining_decisions,
        resolved_open_decisions,
        missing_evidence,
        required_first_reads,
        unsafe_operations,
        retention_review,
        wiki_promotion_state: closeout_receipt_wiki_state(source_receipt),
        stale_task_count,
        next_safe_action,
        retirement_reason: None,
        source_artifacts: CloseoutReceiptArtifacts {
            closeout_plan_json: crate::offdesk::operator_safe_text(&artifacts.closeout_plan_json),
            closeout_plan_markdown: closeout_plan_artifact(
                &plan,
                "/artifacts/closeout_plan_markdown",
            ),
            cleanup_manifest_json: closeout_plan_artifact(
                &plan,
                "/artifacts/cleanup_manifest_json",
            ),
            commercial_review_packet: closeout_plan_artifact(
                &plan,
                "/artifacts/commercial_review_packet",
            ),
            return_package_markdown: crate::offdesk::operator_safe_text(
                &artifacts.return_package_markdown,
            ),
            review_record_json: crate::offdesk::operator_safe_text(&artifacts.review_record_json),
            review_file: None,
        },
    };
    let record = CloseoutReviewRecord {
        reviewed_at,
        review_id,
        closeout_id,
        closeout_generated_at,
        profile: crate::offdesk::operator_safe_text(profile_name),
        artifact_dir: artifact_dir.display().to_string(),
        verdict: CloseoutReviewVerdict::Approved,
        reviewer,
        review_provider: Some("operator_decision_resolution".to_string()),
        review_file: None,
        unsafe_operations: Vec::new(),
        missing_evidence: Vec::new(),
        required_first_reads: Vec::new(),
        notes: Some(format!(
            "Resolved closeout decision `{kind}` with `{}`. Reason: {reason}",
            args.decision.as_str()
        )),
        decision_resolution: Some(decision_resolution),
        closeout_retirement: None,
        applies_to_task_ids,
        applies_to_tasks,
        read_only_project_state: true,
        applies_file_operations: false,
        closeout_receipt,
        artifacts,
    };

    write_closeout_review_record(&record)?;
    Ok(record)
}

fn build_closeout_retire_record(
    profile: &str,
    args: &CloseoutRetireArgs,
) -> Result<CloseoutReviewRecord> {
    let profile_dir = get_profile_dir(profile)?;
    let profile_name = if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    };
    let reason = truncate_closeout_text(
        &crate::offdesk::operator_safe_text(require_non_empty_arg("--reason", args.reason.trim())?),
        2000,
    );
    let artifact_dir = resolve_closeout_artifact_dir_for(
        &profile_dir,
        args.closeout_id.as_deref(),
        args.artifact_dir.as_ref(),
    )?;
    let plan_path = artifact_dir.join("closeout_plan.json");
    let plan: Value = serde_json::from_str(
        &fs::read_to_string(&plan_path)
            .with_context(|| format!("read closeout plan {}", plan_path.display()))?,
    )
    .with_context(|| format!("parse closeout plan {}", plan_path.display()))?;
    let closeout_id = closeout_id_from_plan(&plan)?;
    if let Some(expected) = args.closeout_id.as_deref() {
        let expected = crate::offdesk::operator_safe_text(expected);
        if expected != closeout_id {
            bail!(
                "closeout id mismatch: requested {}, artifact contains {}",
                expected,
                closeout_id
            );
        }
    }
    let source_review = latest_closeout_review_value(&artifact_dir).ok();
    let source_receipt = source_review
        .as_ref()
        .and_then(|(_, _, review)| review.get("closeout_receipt"));
    if source_receipt
        .and_then(|receipt| receipt.get("acceptance_status"))
        .and_then(Value::as_str)
        == Some("accepted")
    {
        bail!("accepted closeouts cannot be retired as evidence-incomplete");
    }

    let current_acceptance = latest_closeout_acceptance_by_task(&profile_dir)?;
    let mut excluded_accepted_tasks = Vec::new();
    let applies_to_tasks = closeout_review_task_refs_from_plan(&plan)
        .into_iter()
        .filter(|task| {
            let key = (task.project_key.clone(), task.task_id.clone());
            if current_acceptance.get(&key).map(String::as_str) == Some("accepted") {
                excluded_accepted_tasks.push(format!("{}:{}", task.project_key, task.task_id));
                false
            } else {
                true
            }
        })
        .collect::<Vec<_>>();
    if applies_to_tasks.is_empty() {
        bail!("no non-accepted tasks remain in this closeout to retire");
    }

    let reviewed_at = Utc::now();
    let review_id = format!("closeout_retirement_{}", short_uuid());
    let review_record_path = allocate_closeout_review_record_path(&artifact_dir, reviewed_at)?;
    let receipt_path = allocate_closeout_receipt_path(&artifact_dir, reviewed_at)?;
    let return_package_path = closeout_return_package_path(&artifact_dir, &plan);
    let artifacts = CloseoutReviewArtifactPaths {
        closeout_plan_json: plan_path.display().to_string(),
        review_record_json: review_record_path.display().to_string(),
        closeout_receipt_json: receipt_path.display().to_string(),
        return_package_markdown: return_package_path.display().to_string(),
    };
    let closeout_generated_at = closeout_generated_at_from_plan(&plan);
    let applies_to_task_ids = applies_to_tasks
        .iter()
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    let executed_scope = closeout_executed_scope(&applies_to_tasks);
    let accepted_scope =
        vec!["No accepted scope; historical closeout retired as evidence-incomplete.".to_string()];
    let reviewer = crate::offdesk::operator_safe_text(args.reviewer.trim());
    let does_not_authorize = closeout_retirement_does_not_authorize();
    let source_review_record_json = source_review
        .as_ref()
        .map(|(path, _, _)| path.display().to_string());
    let closeout_retirement = CloseoutRetirementRecord {
        reason: reason.clone(),
        reviewer: reviewer.clone(),
        retired_at: reviewed_at,
        source_review_record_json,
        excluded_accepted_tasks,
        does_not_authorize: does_not_authorize.clone(),
    };
    let closeout_receipt = CloseoutReceipt {
        schema: "closeout_receipt.v1",
        receipt_id: format!("closeout_receipt_{}", short_uuid()),
        closeout_id: closeout_id.clone(),
        review_id: review_id.clone(),
        generated_at: closeout_generated_at.unwrap_or(reviewed_at),
        reviewed_at,
        verdict: CloseoutReviewVerdict::Revise,
        acceptance_status: "retired_incomplete",
        accepted_scope,
        executed_scope,
        evidence_status: source_receipt
            .map(closeout_receipt_evidence_status)
            .unwrap_or("missing"),
        verification_status: "retired",
        open_decisions: Vec::new(),
        resolved_open_decisions: Vec::new(),
        missing_evidence: Vec::new(),
        required_first_reads: Vec::new(),
        unsafe_operations: Vec::new(),
        retention_review: "retired_incomplete",
        wiki_promotion_state: "not_required",
        stale_task_count: 0,
        next_safe_action:
            "No accepted truth is recorded for this retired evidence-incomplete closeout."
                .to_string(),
        retirement_reason: Some(reason.clone()),
        source_artifacts: CloseoutReceiptArtifacts {
            closeout_plan_json: crate::offdesk::operator_safe_text(&artifacts.closeout_plan_json),
            closeout_plan_markdown: closeout_plan_artifact(
                &plan,
                "/artifacts/closeout_plan_markdown",
            ),
            cleanup_manifest_json: closeout_plan_artifact(
                &plan,
                "/artifacts/cleanup_manifest_json",
            ),
            commercial_review_packet: closeout_plan_artifact(
                &plan,
                "/artifacts/commercial_review_packet",
            ),
            return_package_markdown: crate::offdesk::operator_safe_text(
                &artifacts.return_package_markdown,
            ),
            review_record_json: crate::offdesk::operator_safe_text(&artifacts.review_record_json),
            review_file: None,
        },
    };
    let record = CloseoutReviewRecord {
        reviewed_at,
        review_id,
        closeout_id,
        closeout_generated_at,
        profile: crate::offdesk::operator_safe_text(profile_name),
        artifact_dir: artifact_dir.display().to_string(),
        verdict: CloseoutReviewVerdict::Revise,
        reviewer,
        review_provider: Some("operator_closeout_retirement".to_string()),
        review_file: None,
        unsafe_operations: Vec::new(),
        missing_evidence: Vec::new(),
        required_first_reads: Vec::new(),
        notes: Some(format!(
            "Retired evidence-incomplete historical closeout. Reason: {reason}"
        )),
        decision_resolution: None,
        closeout_retirement: Some(closeout_retirement),
        applies_to_task_ids,
        applies_to_tasks,
        read_only_project_state: true,
        applies_file_operations: false,
        closeout_receipt,
        artifacts,
    };

    write_closeout_review_record(&record)?;
    Ok(record)
}

fn closeout_decision_does_not_authorize() -> Vec<String> {
    vec![
        "file movement, archive creation, deletion, cleanup, wiki promotion, provider retargeting, or accepting unrelated closeouts"
            .to_string(),
    ]
}

fn closeout_retirement_does_not_authorize() -> Vec<String> {
    vec![
        "accepted truth, evidence repair, file movement, archive creation, deletion, cleanup, wiki promotion, provider retargeting, or accepting unrelated closeouts"
            .to_string(),
    ]
}

fn closeout_id_from_plan(plan: &Value) -> Result<String> {
    plan.get("closeout_id")
        .and_then(Value::as_str)
        .map(crate::offdesk::operator_safe_text)
        .ok_or_else(|| anyhow::anyhow!("closeout plan is missing closeout_id"))
}

fn closeout_generated_at_from_plan(plan: &Value) -> Option<DateTime<Utc>> {
    plan.get("generated_at")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn closeout_review_task_refs_from_plan(plan: &Value) -> Vec<CloseoutReviewTaskRef> {
    plan.get("tasks")
        .and_then(Value::as_array)
        .map(|tasks| {
            tasks
                .iter()
                .filter_map(|task| {
                    Some(CloseoutReviewTaskRef {
                        project_key: crate::offdesk::operator_safe_text(
                            task.get("project_key")?.as_str()?,
                        ),
                        request_id: crate::offdesk::operator_safe_text(
                            task.get("request_id")?.as_str()?,
                        ),
                        task_id: crate::offdesk::operator_safe_text(task.get("task_id")?.as_str()?),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn closeout_executed_scope(applies_to_tasks: &[CloseoutReviewTaskRef]) -> Vec<String> {
    applies_to_tasks
        .iter()
        .map(|task| {
            format!(
                "{}:{} request={}",
                task.project_key, task.task_id, task.request_id
            )
        })
        .collect()
}

fn latest_closeout_review_value(artifact_dir: &Path) -> Result<(PathBuf, DateTime<Utc>, Value)> {
    let mut reviews = Vec::new();
    for entry in fs::read_dir(artifact_dir)
        .with_context(|| format!("read closeout artifact dir {}", artifact_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !filename.starts_with("closeout_review_") || !filename.ends_with(".json") {
            continue;
        }
        let content = fs::read_to_string(&path)
            .with_context(|| format!("read closeout review {}", path.display()))?;
        let value: Value = serde_json::from_str(&content)
            .with_context(|| format!("parse closeout review {}", path.display()))?;
        let Some(reviewed_at) = value
            .get("reviewed_at")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))
        else {
            continue;
        };
        reviews.push((reviewed_at, path, value));
    }
    reviews.sort_by_key(|(reviewed_at, _, _)| *reviewed_at);
    reviews
        .pop()
        .map(|(reviewed_at, path, value)| (path, reviewed_at, value))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no closeout review record found in {}; run closeout-review first",
                artifact_dir.display()
            )
        })
}

fn latest_closeout_acceptance_by_task(
    profile_dir: &Path,
) -> Result<BTreeMap<(String, String), String>> {
    let closeouts_dir = profile_dir.join("offdesk_closeouts");
    let mut latest = BTreeMap::<(String, String), (DateTime<Utc>, String)>::new();
    let Ok(closeouts) = fs::read_dir(&closeouts_dir) else {
        return Ok(BTreeMap::new());
    };
    for closeout in closeouts {
        let closeout = closeout?;
        if !closeout.file_type()?.is_dir() {
            continue;
        }
        for review in fs::read_dir(closeout.path())? {
            let review = review?;
            let path = review.path();
            let Some(filename) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !filename.starts_with("closeout_review_") || !filename.ends_with(".json") {
                continue;
            }
            let Ok(content) = fs::read_to_string(&path) else {
                continue;
            };
            let Ok(value) = serde_json::from_str::<Value>(&content) else {
                continue;
            };
            let Some(reviewed_at) = value
                .get("reviewed_at")
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))
            else {
                continue;
            };
            let status = value
                .pointer("/closeout_receipt/acceptance_status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            let Some(tasks) = value.get("applies_to_tasks").and_then(Value::as_array) else {
                continue;
            };
            for task in tasks {
                let Some(project_key) = task.get("project_key").and_then(Value::as_str) else {
                    continue;
                };
                let Some(task_id) = task.get("task_id").and_then(Value::as_str) else {
                    continue;
                };
                let key = (project_key.to_string(), task_id.to_string());
                latest
                    .entry(key)
                    .and_modify(|existing| {
                        if reviewed_at > existing.0 {
                            *existing = (reviewed_at, status.clone());
                        }
                    })
                    .or_insert_with(|| (reviewed_at, status.clone()));
            }
        }
    }
    Ok(latest
        .into_iter()
        .map(|(key, (_, status))| (key, status))
        .collect())
}

fn receipt_decisions_from_value(receipt: &Value) -> Vec<CloseoutReceiptDecision> {
    receipt
        .get("open_decisions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(|decision| CloseoutReceiptDecision {
            kind: closeout_plan_string(decision, "kind", "unknown"),
            detail: truncate_closeout_text(&closeout_plan_string(decision, "detail", "-"), 500),
            suggested_command: truncate_closeout_text(
                &closeout_plan_string(decision, "suggested_command", "-"),
                500,
            ),
        })
        .collect()
}

fn receipt_string_list(receipt: &Value, key: &str) -> Vec<String> {
    receipt
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(crate::offdesk::operator_safe_text)
        .collect()
}

fn closeout_retention_status_after_resolution(
    receipt: &Value,
    decision: CloseoutDecisionResolution,
    all_requested_decisions_resolved: bool,
) -> &'static str {
    if all_requested_decisions_resolved && decision == CloseoutDecisionResolution::PreserveInPlace {
        "resolved_preserve_in_place"
    } else {
        match receipt.get("retention_review").and_then(Value::as_str) {
            Some("not_required") => "not_required",
            Some("resolved_preserve_in_place") => "resolved_preserve_in_place",
            _ => "required",
        }
    }
}

fn closeout_receipt_evidence_status(receipt: &Value) -> &'static str {
    match receipt.get("evidence_status").and_then(Value::as_str) {
        Some("missing") => "missing",
        _ => "review_ready",
    }
}

fn closeout_receipt_wiki_state(receipt: &Value) -> &'static str {
    match receipt.get("wiki_promotion_state").and_then(Value::as_str) {
        Some("review_required") => "review_required",
        Some("audit_unavailable") => "audit_unavailable",
        Some("no_candidate") => "no_candidate",
        Some("not_requested") => "not_requested",
        _ => "not_required",
    }
}

fn build_closeout_report(profile: &str, args: &CloseoutArgs) -> Result<OffdeskCloseoutReport> {
    let profile_dir = get_profile_dir(profile)?;
    let profile_name = if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    };
    let generated_at = Utc::now();
    let closeout_id = format!("closeout_{}", short_uuid());
    let artifact_dir = allocate_closeout_artifact_dir(
        &profile_dir,
        args.output.as_ref(),
        generated_at,
        &closeout_id,
    )?;

    let filters = CloseoutFilters {
        project_key: args
            .project_key
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        request_id: args
            .request_id
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        task_id: args
            .task_id
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
    };

    let tasks = OffdeskTaskStore::new(&profile_dir)
        .load()?
        .into_iter()
        .filter(|task| closeout_task_matches(task, args))
        .collect::<Vec<_>>();
    let background_runs = BackgroundRunStore::new(&profile_dir)
        .load()?
        .into_iter()
        .filter(|probe| closeout_probe_matches(probe, args))
        .collect::<Vec<_>>();

    let closeout_tasks = tasks.iter().map(closeout_task_summary).collect::<Vec<_>>();
    let closeout_background_runs = background_runs
        .iter()
        .map(closeout_background_summary)
        .collect::<Vec<_>>();
    let source_observation = closeout_source_observation(
        args,
        &closeout_tasks,
        &closeout_background_runs,
        generated_at,
    );
    let implementation_packet_coverage = closeout_implementation_packet_coverage(
        &closeout_tasks,
        &closeout_background_runs,
        &source_observation,
    );
    let decision_records = closeout_decision_records(&profile_dir, &tasks, &background_runs, args)?;

    let mut file_operations = closeout_file_operations(&tasks, &background_runs);
    file_operations.sort_by(|left, right| {
        (left.path.as_str(), left.operation).cmp(&(right.path.as_str(), right.operation))
    });
    file_operations.dedup_by(|left, right| {
        left.path == right.path && left.operation == right.operation && left.source == right.source
    });

    let mut required_first_reads = file_operations
        .iter()
        .filter(|operation| operation.present && operation.operation == "keep")
        .map(|operation| CloseoutReadRef {
            path: operation.path.clone(),
            reason: operation.reason.clone(),
            present: operation.present,
        })
        .collect::<Vec<_>>();
    let mut decision_sources = BTreeSet::new();
    for decision in &decision_records {
        if decision_sources.insert(decision.source_path.clone()) {
            required_first_reads.push(CloseoutReadRef {
                path: decision.source_path.clone(),
                reason: "Decision ledger used by closeout; review unresolved decisions before accepting the run.".to_string(),
                present: Path::new(&decision.source_path).exists(),
            });
        }
    }
    required_first_reads.truncate(20);

    let git_snapshot = if args.include_git {
        closeout_git_snapshot(args, &tasks)?
    } else {
        None
    };
    let documentation_governance = closeout_documentation_governance(args, &tasks);
    let open_decisions = closeout_open_decisions(
        &tasks,
        &file_operations,
        &decision_records,
        git_snapshot.as_ref(),
        args,
        documentation_governance.as_ref(),
        &implementation_packet_coverage,
    );
    let verification_commands =
        closeout_verification_commands(args, documentation_governance.as_ref());

    let artifacts = CloseoutArtifactPaths {
        closeout_plan_json: artifact_dir
            .join("closeout_plan.json")
            .display()
            .to_string(),
        closeout_plan_markdown: artifact_dir.join("CLOSEOUT_PLAN.md").display().to_string(),
        cleanup_manifest_json: artifact_dir
            .join("cleanup_manifest.json")
            .display()
            .to_string(),
        commercial_review_packet: artifact_dir
            .join("COMMERCIAL_REVIEW_PACKET.md")
            .display()
            .to_string(),
        return_package_markdown: artifact_dir.join("RETURN_PACKAGE.md").display().to_string(),
    };
    let review_contract = CloseoutReviewContract {
        provider: crate::offdesk::operator_safe_text(&args.review_provider),
        required: true,
        applies_to_operations: vec!["archive_candidate", "delete_candidate", "move_candidate"],
        required_verdicts: vec!["approved", "revise", "blocked"],
        decision_schema: serde_json::json!({
            "verdict": "approved|revise|blocked",
            "unsafe_operations": ["operation path or id"],
            "missing_evidence": ["required file, artifact, or command"],
            "required_first_reads": ["paths the next Ondesk harness must read first"],
            "packet_goal_coverage": "completed|deferred|missing|drifted",
            "notes": "short rationale"
        }),
        safety_rules: vec![
            "Never approve delete or move for git-tracked source files without explicit human approval.",
            "Never treat closeout as completion proof; require result and review artifacts.",
            "Archive raw logs before deletion is considered.",
            "Reject plans that touch hidden config, env, mount, symlink, external drive, or system paths without dedicated evidence.",
            "Prefer keep or archive when provenance is uncertain.",
        ],
        packet_path: artifacts.commercial_review_packet.clone(),
    };

    let summary = summarize_closeout(
        &closeout_tasks,
        &closeout_background_runs,
        &file_operations,
        &decision_records,
        &implementation_packet_coverage,
    );

    let report = OffdeskCloseoutReport {
        generated_at,
        closeout_id,
        profile: crate::offdesk::operator_safe_text(profile_name),
        profile_dir: crate::offdesk::operator_safe_text(profile_dir.to_string_lossy().as_ref()),
        artifact_dir: artifact_dir.display().to_string(),
        dry_run: true,
        operator_requested_dry_run: args.dry_run,
        read_only_project_state: true,
        filters,
        summary,
        source_observation,
        implementation_packet_coverage,
        tasks: closeout_tasks,
        background_runs: closeout_background_runs,
        file_operations,
        required_first_reads,
        decision_records,
        open_decisions,
        verification_commands,
        documentation_governance,
        review_contract,
        git_snapshot,
        artifacts,
    };

    write_closeout_artifacts(&report)?;
    Ok(report)
}

fn build_closeout_review_record(
    profile: &str,
    args: &CloseoutReviewArgs,
) -> Result<CloseoutReviewRecord> {
    let profile_dir = get_profile_dir(profile)?;
    let profile_name = if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    };
    let artifact_dir = resolve_closeout_artifact_dir(&profile_dir, args)?;
    let plan_path = artifact_dir.join("closeout_plan.json");
    let plan: Value = serde_json::from_str(&fs::read_to_string(&plan_path).with_context(|| {
        format!(
            "read closeout plan for review record {}",
            plan_path.display()
        )
    })?)
    .with_context(|| format!("parse closeout plan {}", plan_path.display()))?;

    let closeout_id = plan
        .get("closeout_id")
        .and_then(Value::as_str)
        .map(crate::offdesk::operator_safe_text)
        .ok_or_else(|| anyhow::anyhow!("closeout plan is missing closeout_id"))?;
    if let Some(expected) = args.closeout_id.as_deref() {
        let expected = crate::offdesk::operator_safe_text(expected);
        if expected != closeout_id {
            bail!(
                "closeout id mismatch: requested {}, artifact contains {}",
                expected,
                closeout_id
            );
        }
    }

    let closeout_generated_at = plan
        .get("generated_at")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc));
    let applies_to_task_ids = plan
        .get("tasks")
        .and_then(Value::as_array)
        .map(|tasks| {
            tasks
                .iter()
                .filter_map(|task| task.get("task_id").and_then(Value::as_str))
                .map(crate::offdesk::operator_safe_text)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let applies_to_tasks = plan
        .get("tasks")
        .and_then(Value::as_array)
        .map(|tasks| {
            tasks
                .iter()
                .filter_map(|task| {
                    Some(CloseoutReviewTaskRef {
                        project_key: crate::offdesk::operator_safe_text(
                            task.get("project_key")?.as_str()?,
                        ),
                        request_id: crate::offdesk::operator_safe_text(
                            task.get("request_id")?.as_str()?,
                        ),
                        task_id: crate::offdesk::operator_safe_text(task.get("task_id")?.as_str()?),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let reviewed_at = Utc::now();
    let review_id = format!("closeout_review_{}", short_uuid());
    let review_record_path = allocate_closeout_review_record_path(&artifact_dir, reviewed_at)?;
    let receipt_path = allocate_closeout_receipt_path(&artifact_dir, reviewed_at)?;
    let return_package_path = closeout_return_package_path(&artifact_dir, &plan);
    let artifacts = CloseoutReviewArtifactPaths {
        closeout_plan_json: plan_path.display().to_string(),
        review_record_json: review_record_path.display().to_string(),
        closeout_receipt_json: receipt_path.display().to_string(),
        return_package_markdown: return_package_path.display().to_string(),
    };
    let stale_task_count =
        closeout_review_stale_task_count(&profile_dir, &applies_to_tasks, closeout_generated_at);
    let closeout_receipt = build_closeout_receipt(CloseoutReceiptInput {
        plan: &plan,
        args,
        artifacts: &artifacts,
        closeout_id: &closeout_id,
        review_id: &review_id,
        closeout_generated_at,
        reviewed_at,
        applies_to_tasks: &applies_to_tasks,
        stale_task_count,
    });
    let record = CloseoutReviewRecord {
        reviewed_at,
        review_id,
        closeout_id,
        closeout_generated_at,
        profile: crate::offdesk::operator_safe_text(profile_name),
        artifact_dir: artifact_dir.display().to_string(),
        verdict: args.verdict,
        reviewer: crate::offdesk::operator_safe_text(args.reviewer.trim()),
        review_provider: args
            .review_provider
            .as_deref()
            .map(|value| crate::offdesk::operator_safe_text(value.trim())),
        review_file: args
            .review_file
            .as_ref()
            .map(|path| crate::offdesk::operator_safe_text(path.to_string_lossy().as_ref())),
        unsafe_operations: safe_text_list(&args.unsafe_operation),
        missing_evidence: safe_text_list(&args.missing_evidence),
        required_first_reads: safe_text_list(&args.required_first_read),
        notes: args
            .notes
            .as_deref()
            .map(|value| truncate_closeout_text(&crate::offdesk::operator_safe_text(value), 2000)),
        decision_resolution: None,
        closeout_retirement: None,
        applies_to_task_ids,
        applies_to_tasks,
        read_only_project_state: true,
        applies_file_operations: false,
        closeout_receipt,
        artifacts,
    };

    write_closeout_review_record(&record)?;
    Ok(record)
}

fn closeout_return_package_path(artifact_dir: &Path, plan: &Value) -> PathBuf {
    plan.pointer("/artifacts/return_package_markdown")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .unwrap_or_else(|| artifact_dir.join("RETURN_PACKAGE.md"))
}

fn resolve_closeout_artifact_dir(profile_dir: &Path, args: &CloseoutReviewArgs) -> Result<PathBuf> {
    resolve_closeout_artifact_dir_for(
        profile_dir,
        args.closeout_id.as_deref(),
        args.artifact_dir.as_ref(),
    )
}

fn resolve_closeout_artifact_dir_for(
    profile_dir: &Path,
    closeout_id: Option<&str>,
    artifact_dir: Option<&PathBuf>,
) -> Result<PathBuf> {
    if let Some(artifact_dir) = artifact_dir {
        return Ok(artifact_dir.clone());
    }

    let closeouts_dir = profile_dir.join("offdesk_closeouts");
    let entries = fs::read_dir(&closeouts_dir)
        .with_context(|| format!("read closeout artifact root {}", closeouts_dir.display()))?;
    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let artifact_dir = entry.path();
        let plan_path = artifact_dir.join("closeout_plan.json");
        let Ok(content) = fs::read_to_string(&plan_path) else {
            continue;
        };
        let Ok(plan) = serde_json::from_str::<Value>(&content) else {
            continue;
        };
        let plan_closeout_id = plan.get("closeout_id").and_then(Value::as_str);
        if let Some(expected) = closeout_id {
            if plan_closeout_id != Some(expected) {
                continue;
            }
        }
        let generated_at = plan
            .get("generated_at")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        candidates.push((generated_at, artifact_dir));
    }

    candidates.sort_by_key(|(generated_at, _)| *generated_at);
    candidates.pop().map(|(_, path)| path).ok_or_else(|| {
        if let Some(closeout_id) = closeout_id {
            anyhow::anyhow!(
                "no closeout artifact found for closeout_id {}",
                crate::offdesk::operator_safe_text(closeout_id)
            )
        } else {
            anyhow::anyhow!("no closeout artifact found; run `forager offdesk closeout` first")
        }
    })
}

fn allocate_closeout_review_record_path(
    artifact_dir: &Path,
    reviewed_at: DateTime<Utc>,
) -> Result<PathBuf> {
    fs::create_dir_all(artifact_dir)
        .with_context(|| format!("create closeout artifact dir {}", artifact_dir.display()))?;
    let timestamp = reviewed_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let filename = if attempt == 0 {
            format!("closeout_review_{timestamp}.json")
        } else {
            format!("closeout_review_{timestamp}_{attempt:03}.json")
        };
        let path = artifact_dir.join(filename);
        if !path.exists() {
            return Ok(path);
        }
    }

    bail!(
        "could not allocate closeout review record path in {}",
        artifact_dir.display()
    )
}

fn allocate_closeout_receipt_path(
    artifact_dir: &Path,
    reviewed_at: DateTime<Utc>,
) -> Result<PathBuf> {
    fs::create_dir_all(artifact_dir)
        .with_context(|| format!("create closeout artifact dir {}", artifact_dir.display()))?;
    let timestamp = reviewed_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let filename = if attempt == 0 {
            format!("closeout_receipt_{timestamp}.json")
        } else {
            format!("closeout_receipt_{timestamp}_{attempt:03}.json")
        };
        let path = artifact_dir.join(filename);
        if !path.exists() {
            return Ok(path);
        }
    }

    bail!(
        "could not allocate closeout receipt path in {}",
        artifact_dir.display()
    )
}

fn write_closeout_review_record(record: &CloseoutReviewRecord) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(record)?;
    write_new_file(Path::new(&record.artifacts.review_record_json), &bytes)
        .with_context(|| format!("write {}", record.artifacts.review_record_json))?;
    let receipt_bytes = serde_json::to_vec_pretty(&record.closeout_receipt)?;
    write_new_file(
        Path::new(&record.artifacts.closeout_receipt_json),
        &receipt_bytes,
    )
    .with_context(|| format!("write {}", record.artifacts.closeout_receipt_json))?;
    update_return_package_with_closeout_receipt(record)?;
    Ok(())
}

struct CloseoutReceiptInput<'a> {
    plan: &'a Value,
    args: &'a CloseoutReviewArgs,
    artifacts: &'a CloseoutReviewArtifactPaths,
    closeout_id: &'a str,
    review_id: &'a str,
    closeout_generated_at: Option<DateTime<Utc>>,
    reviewed_at: DateTime<Utc>,
    applies_to_tasks: &'a [CloseoutReviewTaskRef],
    stale_task_count: usize,
}

fn build_closeout_receipt(input: CloseoutReceiptInput<'_>) -> CloseoutReceipt {
    let CloseoutReceiptInput {
        plan,
        args,
        artifacts,
        closeout_id,
        review_id,
        closeout_generated_at,
        reviewed_at,
        applies_to_tasks,
        stale_task_count,
    } = input;
    let open_decisions = closeout_receipt_open_decisions(plan);
    let unsafe_operations = material_review_items(&args.unsafe_operation);
    let missing_evidence = material_review_items(&args.missing_evidence);
    let required_first_reads = material_review_items(&args.required_first_read);
    let plan_missing_artifacts = closeout_plan_usize(plan, "/summary/missing_artifacts");
    let retention_review = closeout_receipt_retention_review(plan, &unsafe_operations);
    let wiki_promotion_state = closeout_receipt_wiki_promotion_state(plan);
    let evidence_status = if plan_missing_artifacts > 0 || !missing_evidence.is_empty() {
        "missing"
    } else {
        "review_ready"
    };
    let has_followups = stale_task_count > 0
        || !open_decisions.is_empty()
        || !unsafe_operations.is_empty()
        || !missing_evidence.is_empty()
        || !required_first_reads.is_empty()
        || plan_missing_artifacts > 0
        || retention_review == "required"
        || wiki_promotion_state == "review_required"
        || wiki_promotion_state == "audit_unavailable";
    let verification_status = if has_followups { "pending" } else { "recorded" };
    let acceptance_status = match args.verdict {
        CloseoutReviewVerdict::Approved if has_followups => "approved_with_followups",
        CloseoutReviewVerdict::Approved => "accepted",
        CloseoutReviewVerdict::Revise => "revision_required",
        CloseoutReviewVerdict::Blocked => "blocked",
    };
    let executed_scope = applies_to_tasks
        .iter()
        .map(|task| {
            format!(
                "{}:{} request={}",
                task.project_key, task.task_id, task.request_id
            )
        })
        .collect::<Vec<_>>();
    let accepted_scope = if acceptance_status == "accepted" {
        executed_scope.clone()
    } else {
        vec![
            "No final accepted scope; receipt requires follow-up review before accepted truth."
                .to_string(),
        ]
    };
    let next_safe_action = closeout_receipt_next_safe_action(
        acceptance_status,
        stale_task_count,
        &open_decisions,
        &missing_evidence,
        &required_first_reads,
    );

    CloseoutReceipt {
        schema: "closeout_receipt.v1",
        receipt_id: format!("closeout_receipt_{}", short_uuid()),
        closeout_id: closeout_id.to_string(),
        review_id: review_id.to_string(),
        generated_at: closeout_generated_at.unwrap_or(reviewed_at),
        reviewed_at,
        verdict: args.verdict,
        acceptance_status,
        accepted_scope,
        executed_scope,
        evidence_status,
        verification_status,
        open_decisions,
        resolved_open_decisions: Vec::new(),
        missing_evidence,
        required_first_reads,
        unsafe_operations,
        retention_review,
        wiki_promotion_state,
        stale_task_count,
        next_safe_action,
        retirement_reason: None,
        source_artifacts: CloseoutReceiptArtifacts {
            closeout_plan_json: crate::offdesk::operator_safe_text(&artifacts.closeout_plan_json),
            closeout_plan_markdown: closeout_plan_artifact(
                plan,
                "/artifacts/closeout_plan_markdown",
            ),
            cleanup_manifest_json: closeout_plan_artifact(plan, "/artifacts/cleanup_manifest_json"),
            commercial_review_packet: closeout_plan_artifact(
                plan,
                "/artifacts/commercial_review_packet",
            ),
            return_package_markdown: crate::offdesk::operator_safe_text(
                &artifacts.return_package_markdown,
            ),
            review_record_json: crate::offdesk::operator_safe_text(&artifacts.review_record_json),
            review_file: args
                .review_file
                .as_ref()
                .map(|path| crate::offdesk::operator_safe_text(path.to_string_lossy().as_ref())),
        },
    }
}

fn closeout_review_stale_task_count(
    profile_dir: &Path,
    applies_to_tasks: &[CloseoutReviewTaskRef],
    closeout_generated_at: Option<DateTime<Utc>>,
) -> usize {
    let Some(generated_at) = closeout_generated_at else {
        return 0;
    };
    let targets = applies_to_tasks
        .iter()
        .map(|task| (task.project_key.clone(), task.task_id.clone()))
        .collect::<BTreeSet<_>>();
    if targets.is_empty() {
        return 0;
    }
    let Ok(tasks) = OffdeskTaskStore::new(profile_dir).load() else {
        return 0;
    };
    tasks
        .iter()
        .filter(|task| targets.contains(&(task.project_key.clone(), task.task_id.clone())))
        .filter(|task| task.updated_at > generated_at)
        .count()
}

fn closeout_receipt_open_decisions(plan: &Value) -> Vec<CloseoutReceiptDecision> {
    plan.get("open_decisions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(20)
        .map(|decision| CloseoutReceiptDecision {
            kind: closeout_plan_string(decision, "kind", "unknown"),
            detail: truncate_closeout_text(&closeout_plan_string(decision, "detail", "-"), 500),
            suggested_command: truncate_closeout_text(
                &closeout_plan_string(decision, "suggested_command", "-"),
                500,
            ),
        })
        .collect()
}

fn closeout_receipt_retention_review(plan: &Value, unsafe_operations: &[String]) -> &'static str {
    if !unsafe_operations.is_empty()
        || closeout_plan_usize(plan, "/summary/operations_requiring_commercial_review") > 0
        || closeout_plan_usize(plan, "/summary/operations_requiring_human_approval") > 0
        || closeout_plan_usize(plan, "/summary/archive_candidates") > 0
        || closeout_plan_usize(plan, "/summary/delete_candidates") > 0
    {
        "required"
    } else {
        "not_required"
    }
}

fn closeout_receipt_wiki_promotion_state(plan: &Value) -> &'static str {
    let Some(governance) = plan.get("documentation_governance") else {
        return "not_requested";
    };
    if governance
        .get("error")
        .is_some_and(|value| !value.is_null())
    {
        "audit_unavailable"
    } else if closeout_plan_usize(plan, "/documentation_governance/recommendation_count") > 0 {
        "review_required"
    } else {
        "no_candidate"
    }
}

fn closeout_receipt_next_safe_action(
    acceptance_status: &str,
    stale_task_count: usize,
    open_decisions: &[CloseoutReceiptDecision],
    missing_evidence: &[String],
    required_first_reads: &[String],
) -> String {
    if stale_task_count > 0 {
        return "Regenerate closeout because one or more tasks changed after the closeout plan."
            .to_string();
    }
    if acceptance_status == "accepted" {
        return "Rehydrate Ondesk from the return package and continue under reviewed evidence."
            .to_string();
    }
    if acceptance_status == "blocked" {
        return "Resolve the closeout blocker, then rerun closeout-review.".to_string();
    }
    if acceptance_status == "revision_required" {
        return "Revise the closeout package or evidence and rerun closeout-review.".to_string();
    }
    if !missing_evidence.is_empty() {
        return "Supply the missing evidence and rerun closeout-review.".to_string();
    }
    if !required_first_reads.is_empty() {
        return "Read the required artifacts before treating the result as accepted.".to_string();
    }
    if let Some(decision) = open_decisions.first() {
        return format!(
            "Resolve `{}` before treating the result as accepted.",
            decision.kind
        );
    }
    "Review remaining follow-ups before treating the result as accepted.".to_string()
}

fn closeout_plan_usize(plan: &Value, pointer: &str) -> usize {
    plan.pointer(pointer)
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .unwrap_or_default()
}

fn closeout_plan_artifact(plan: &Value, pointer: &str) -> Option<String> {
    plan.pointer(pointer)
        .and_then(Value::as_str)
        .map(crate::offdesk::operator_safe_text)
}

fn closeout_plan_string(value: &Value, field: &str, fallback: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(crate::offdesk::operator_safe_text)
        .unwrap_or_else(|| fallback.to_string())
}

fn material_review_items(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| crate::offdesk::operator_safe_text(value.trim()))
        .filter(|value| {
            let normalized = value.trim().to_lowercase();
            !normalized.is_empty()
                && normalized != "none"
                && normalized != "n/a"
                && normalized != "na"
                && normalized != "-"
        })
        .collect()
}

fn update_return_package_with_closeout_receipt(record: &CloseoutReviewRecord) -> Result<()> {
    let path = Path::new(&record.artifacts.return_package_markdown);
    let existing =
        fs::read_to_string(path).unwrap_or_else(|_| "# Ondesk Return Package\n\n".to_string());
    let section =
        render_closeout_receipt_return_section(&record.closeout_receipt, &record.artifacts);
    let updated = replace_marked_section(
        &existing,
        CLOSEOUT_RECEIPT_SECTION_START,
        CLOSEOUT_RECEIPT_SECTION_END,
        &section,
    );
    fs::write(path, updated).with_context(|| format!("update {}", path.display()))?;
    Ok(())
}

const CLOSEOUT_RECEIPT_SECTION_START: &str = "<!-- forager:closeout-receipt:start -->";
const CLOSEOUT_RECEIPT_SECTION_END: &str = "<!-- forager:closeout-receipt:end -->";

fn render_closeout_receipt_return_section(
    receipt: &CloseoutReceipt,
    artifacts: &CloseoutReviewArtifactPaths,
) -> String {
    let mut output = String::new();
    output.push_str(CLOSEOUT_RECEIPT_SECTION_START);
    output.push_str("\n## Closeout Receipt\n");
    output.push_str(&format!(
        "- acceptance_status: `{}`\n",
        receipt.acceptance_status
    ));
    output.push_str(&format!("- verdict: `{}`\n", receipt.verdict.as_str()));
    output.push_str(&format!(
        "- evidence_status: `{}` / verification_status: `{}`\n",
        receipt.evidence_status, receipt.verification_status
    ));
    output.push_str(&format!(
        "- open_decisions: {} / missing_evidence: {} / required_first_reads: {} / stale_tasks: {}\n",
        receipt.open_decisions.len(),
        receipt.missing_evidence.len(),
        receipt.required_first_reads.len(),
        receipt.stale_task_count
    ));
    output.push_str(&format!(
        "- retention_review: `{}` / wiki_promotion_state: `{}`\n",
        receipt.retention_review, receipt.wiki_promotion_state
    ));
    output.push_str(&format!(
        "- next_safe_action: {}\n",
        receipt.next_safe_action
    ));
    output.push_str(&format!(
        "- receipt_artifact: `{}`\n",
        artifacts.closeout_receipt_json
    ));
    output.push_str(CLOSEOUT_RECEIPT_SECTION_END);
    output.push_str("\n\n");
    output
}

fn replace_marked_section(existing: &str, start: &str, end: &str, section: &str) -> String {
    if let Some(start_index) = existing.find(start) {
        if let Some(end_offset) = existing[start_index..].find(end) {
            let end_index = start_index + end_offset + end.len();
            let mut output = String::new();
            output.push_str(existing[..start_index].trim_end());
            output.push_str("\n\n");
            output.push_str(section);
            output.push_str(existing[end_index..].trim_start());
            return output;
        }
    }

    if let Some(insert_at) = existing.find("\n## Status\n") {
        let mut output = String::new();
        output.push_str(&existing[..insert_at]);
        output.push_str(section);
        output.push_str(&existing[insert_at + 1..]);
        output
    } else {
        let mut output = String::new();
        output.push_str(existing.trim_end());
        output.push_str("\n\n");
        output.push_str(section);
        output
    }
}

fn safe_text_list(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| crate::offdesk::operator_safe_text(value.trim()))
        .filter(|value| !value.is_empty())
        .collect()
}

fn allocate_closeout_artifact_dir(
    profile_dir: &Path,
    output: Option<&PathBuf>,
    generated_at: DateTime<Utc>,
    closeout_id: &str,
) -> Result<PathBuf> {
    if let Some(output) = output {
        fs::create_dir_all(output)
            .with_context(|| format!("create closeout output directory {}", output.display()))?;
        return Ok(output.clone());
    }

    let base = profile_dir.join("offdesk_closeouts");
    fs::create_dir_all(&base)
        .with_context(|| format!("create closeout artifact root {}", base.display()))?;
    let timestamp = generated_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let dirname = if attempt == 0 {
            format!("{timestamp}_{closeout_id}")
        } else {
            format!("{timestamp}_{closeout_id}_{attempt:03}")
        };
        let path = base.join(dirname);
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create closeout artifact dir {}", path.display()));
            }
        }
    }

    bail!(
        "could not allocate closeout artifact directory in {}",
        base.display()
    )
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

    let mut coverage = CloseoutImplementationPacketCoverage::default();
    for aggregate in packets.into_values() {
        let (goal_status, reason) = closeout_packet_goal_status(&aggregate);
        let details = closeout_packet_detail_coverage(&aggregate, goal_status);
        match goal_status {
            "completed" => coverage.completed += 1,
            "deferred" => coverage.deferred += 1,
            "missing" => coverage.missing += 1,
            "drifted" => coverage.drifted += 1,
            _ => {}
        }
        closeout_count_packet_details(&mut coverage, &details.work_slices);
        closeout_count_packet_details(&mut coverage, &details.validation_items);
        closeout_count_packet_details(&mut coverage, &details.expected_artifacts);
        let summary = aggregate.summary;
        coverage
            .items
            .push(CloseoutImplementationPacketCoverageItem {
                packet_id: summary.packet_id,
                project_key: summary.project_key,
                goal: summary.goal,
                success_state: summary.success_state,
                outcome: summary.outcome,
                safe_to_delegate: summary.safe_to_delegate,
                goal_status,
                reason,
                evidence_refs: aggregate.evidence_refs.into_iter().take(20).collect(),
                required_revisions: summary.required_revisions,
                drift_signals: summary.drift_signals,
                missing_decisions: summary.missing_decisions,
                work_slice_count: summary.work_slice_count,
                validation_item_count: summary.validation_item_count,
                expected_artifact_count: summary.expected_artifact_count,
                detail_source: details.detail_source,
                detail_error: details.detail_error,
                work_slices: details.work_slices,
                validation_items: details.validation_items,
                expected_artifacts: details.expected_artifacts,
            });
    }
    coverage.packet_count = coverage.items.len();
    coverage
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
    packet_status: &'static str,
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
    packet_status: &'static str,
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
                status: packet_status,
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
    let role = receipt.resolved_producer_role();
    let reported_status = receipt.status.as_str();
    let status = closeout_effective_work_slice_status(receipt, role);
    let trust_tier = closeout_receipt_trust_tier(role, receipt.verification_status);
    let summary = crate::offdesk::operator_safe_text(&receipt.summary);
    let mut reason = if summary.is_empty() {
        format!(
            "{} reports `{reported_status}`.",
            closeout_receipt_role_label(role)
        )
    } else {
        format!(
            "{} reports `{reported_status}`: {summary}",
            closeout_receipt_role_label(role)
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

fn closeout_effective_work_slice_status(
    receipt: &WorkSliceExecutionReceipt,
    role: WorkSliceReceiptProducerRole,
) -> &'static str {
    let reported_status = receipt.status.as_str();
    if reported_status != "completed" {
        return reported_status;
    }
    match role {
        WorkSliceReceiptProducerRole::DeterministicVerification
        | WorkSliceReceiptProducerRole::ReviewJudgment => "completed",
        WorkSliceReceiptProducerRole::CloseoutCollector
            if receipt.verification_status.is_independently_verified() =>
        {
            "completed"
        }
        _ => "deferred",
    }
}

fn closeout_receipt_trust_tier(
    role: WorkSliceReceiptProducerRole,
    verification_status: WorkSliceVerificationStatus,
) -> &'static str {
    match role {
        WorkSliceReceiptProducerRole::RunnerObservation => "runtime_observation",
        WorkSliceReceiptProducerRole::WorkerClaim => "worker_claim",
        WorkSliceReceiptProducerRole::DeterministicVerification => "source_verified",
        WorkSliceReceiptProducerRole::ReviewJudgment => "review_judgment",
        WorkSliceReceiptProducerRole::CloseoutCollector
            if verification_status.is_independently_verified() =>
        {
            "closeout_verified"
        }
        WorkSliceReceiptProducerRole::CloseoutCollector => "closeout_observation",
        WorkSliceReceiptProducerRole::LegacyReceipt => "legacy_receipt",
    }
}

fn closeout_receipt_role_label(role: WorkSliceReceiptProducerRole) -> &'static str {
    match role {
        WorkSliceReceiptProducerRole::RunnerObservation => "Runner observation",
        WorkSliceReceiptProducerRole::WorkerClaim => "Worker claim",
        WorkSliceReceiptProducerRole::CloseoutCollector => "Closeout observation",
        WorkSliceReceiptProducerRole::DeterministicVerification => "Deterministic verification",
        WorkSliceReceiptProducerRole::ReviewJudgment => "Review judgment",
        WorkSliceReceiptProducerRole::LegacyReceipt => "Legacy receipt",
    }
}

fn closeout_validation_item_details(
    packet: &ImplementationPacket,
    aggregate: &CloseoutPacketAggregate,
    packet_status: &'static str,
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
    packet_status: &'static str,
) {
    for label in labels {
        let evidence_refs = closeout_packet_matching_refs(aggregate, label);
        let (status, reason) =
            closeout_detail_status_from_match(packet_status, !evidence_refs.is_empty());
        items.push(CloseoutPacketCoverageDetail {
            category,
            label: crate::offdesk::operator_safe_text(label),
            status,
            reason,
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
    packet_status: &'static str,
) -> Vec<CloseoutPacketCoverageDetail> {
    expected_artifacts
        .iter()
        .map(|artifact| {
            let evidence_refs = closeout_packet_matching_refs(aggregate, artifact);
            let (status, reason) =
                closeout_detail_status_from_match(packet_status, !evidence_refs.is_empty());
            CloseoutPacketCoverageDetail {
                category: "expected_artifact",
                label: crate::offdesk::operator_safe_text(artifact),
                status,
                reason,
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

fn closeout_detail_status_from_match(
    packet_status: &'static str,
    has_match: bool,
) -> (&'static str, String) {
    if matches!(packet_status, "deferred" | "missing" | "drifted") {
        return (
            packet_status,
            "Packet-level status prevents item-level acceptance.".to_string(),
        );
    }
    if has_match {
        (
            "completed",
            "Closeout evidence matched this packet item.".to_string(),
        )
    } else {
        (
            "missing",
            "No closeout artifact or evidence ref matched this packet item.".to_string(),
        )
    }
}

fn closeout_summary_only_details(
    category: &'static str,
    count: usize,
    packet_status: &'static str,
    aggregate: &CloseoutPacketAggregate,
) -> Vec<CloseoutPacketCoverageDetail> {
    (0..count)
        .map(|index| CloseoutPacketCoverageDetail {
            category,
            label: format!("{category}_{}", index + 1),
            status: packet_status,
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

fn closeout_count_packet_details(
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

fn closeout_packet_goal_status(aggregate: &CloseoutPacketAggregate) -> (&'static str, String) {
    let summary = &aggregate.summary;
    if !summary.safe_to_delegate
        || !summary.outcome.eq_ignore_ascii_case("pass")
        || !summary.required_revisions.is_empty()
        || !summary.drift_signals.is_empty()
        || !summary.missing_decisions.is_empty()
    {
        return (
            "drifted",
            "Implementation packet alignment was not clean; revise the packet or resolve listed drift before accepting the run.".to_string(),
        );
    }
    if aggregate.has_failed_evidence {
        return (
            "drifted",
            "Execution evidence shows failed, cancelled, stale, or reconstructable work for this packet.".to_string(),
        );
    }
    if aggregate.has_active_evidence {
        return (
            "deferred",
            "Execution is still queued, running, pending approval, or waiting for resume."
                .to_string(),
        );
    }
    if aggregate.has_completed_evidence {
        return (
            "completed",
            "Execution evidence exists for this packet; acceptance still depends on closeout review and first-read verification.".to_string(),
        );
    }
    (
        "missing",
        "The packet is linked to closeout, but no task or background completion evidence was found.".to_string(),
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

fn write_closeout_artifacts(report: &OffdeskCloseoutReport) -> Result<()> {
    let plan_json = serde_json::to_vec_pretty(report)?;
    write_new_file(Path::new(&report.artifacts.closeout_plan_json), &plan_json)
        .with_context(|| format!("write {}", report.artifacts.closeout_plan_json))?;

    let manifest_json = serde_json::to_vec_pretty(&report.file_operations)?;
    write_new_file(
        Path::new(&report.artifacts.cleanup_manifest_json),
        &manifest_json,
    )
    .with_context(|| format!("write {}", report.artifacts.cleanup_manifest_json))?;

    write_new_file(
        Path::new(&report.artifacts.closeout_plan_markdown),
        render_closeout_plan_markdown(report).as_bytes(),
    )
    .with_context(|| format!("write {}", report.artifacts.closeout_plan_markdown))?;

    write_new_file(
        Path::new(&report.artifacts.return_package_markdown),
        render_closeout_return_package(report).as_bytes(),
    )
    .with_context(|| format!("write {}", report.artifacts.return_package_markdown))?;

    write_new_file(
        Path::new(&report.artifacts.commercial_review_packet),
        render_commercial_review_packet(report).as_bytes(),
    )
    .with_context(|| format!("write {}", report.artifacts.commercial_review_packet))?;
    Ok(())
}

fn render_closeout_plan_markdown(report: &OffdeskCloseoutReport) -> String {
    let mut output = String::new();
    output.push_str("# Offdesk Closeout Plan\n\n");
    output.push_str(&format!("- closeout_id: {}\n", report.closeout_id));
    output.push_str(&format!("- generated_at: {}\n", report.generated_at));
    output.push_str(&format!("- profile: {}\n", report.profile));
    output.push_str("- dry_run: true\n");
    output.push_str("- project file mutations: none\n\n");
    output.push_str("## Summary\n");
    output.push_str(&format!(
        "- tasks: {} scanned, {} completed, {} active_or_blocked\n",
        report.summary.tasks_scanned,
        report.summary.completed_tasks,
        report.summary.active_or_blocked_tasks
    ));
    output.push_str(&format!(
        "- file operations: {} keep, {} archive candidates, {} delete candidates\n",
        report.summary.keep_operations,
        report.summary.archive_candidates,
        report.summary.delete_candidates
    ));
    output.push_str(&format!(
        "- commercial review required: {}\n",
        report.summary.operations_requiring_commercial_review
    ));
    output.push_str(&format!(
        "- decision records: {} scanned, {} open, {} invalid\n\n",
        report.summary.decision_records_scanned,
        report.summary.open_decision_records,
        report.summary.invalid_decision_records
    ));
    render_implementation_packet_coverage_markdown(
        &mut output,
        &report.implementation_packet_coverage,
    );
    output.push_str("## File Operations\n");
    if report.file_operations.is_empty() {
        output.push_str("- No file operations proposed.\n");
    } else {
        for operation in &report.file_operations {
            output.push_str(&format!(
                "- {} `{}` risk={} present={} review={} approval={}\n  - reason: {}\n",
                operation.operation,
                operation.path,
                operation.risk,
                operation.present,
                operation.requires_commercial_review,
                operation.requires_human_approval,
                operation.reason
            ));
        }
    }
    output.push_str("\n## Open Decisions\n");
    if report.open_decisions.is_empty() {
        output.push_str("- None recorded.\n");
    } else {
        for decision in &report.open_decisions {
            output.push_str(&format!(
                "- {}: {}\n  - command: `{}`\n",
                decision.kind, decision.detail, decision.suggested_command
            ));
        }
    }
    output.push_str("\n## Documentation Governance\n");
    render_documentation_governance_markdown(&mut output, report.documentation_governance.as_ref());
    output
}

fn render_closeout_return_package(report: &OffdeskCloseoutReport) -> String {
    let mut output = String::new();
    output.push_str("# Ondesk Return Package\n\n");
    output.push_str("Use this package to rehydrate a fresh Ondesk harness after Offdesk work.\n\n");
    render_return_status(&mut output, report);
    render_return_source_observation(&mut output, report);
    render_implementation_packet_coverage_markdown(
        &mut output,
        &report.implementation_packet_coverage,
    );
    render_return_decisions(&mut output, report);
    output.push_str("## Required First Reads\n");
    let first_reads = prioritized_closeout_first_reads(&report.required_first_reads);
    if first_reads.is_empty() {
        output.push_str(
            "- No present result artifacts were found. Start with `closeout_plan.json`.\n",
        );
    } else {
        for read in first_reads.iter().take(CLOSEOUT_RETURN_FIRST_READ_LIMIT) {
            output.push_str(&format!(
                "- {}: `{}`\n  - why: {}\n",
                closeout_read_label(read),
                read.path,
                read.reason
            ));
        }
        if first_reads.len() > CLOSEOUT_RETURN_FIRST_READ_LIMIT {
            output.push_str(&format!(
                "- ... {} more first-read candidate(s) are listed in `closeout_plan.json`.\n",
                first_reads.len() - CLOSEOUT_RETURN_FIRST_READ_LIMIT
            ));
        }
    }
    render_return_change_summary(&mut output, report);
    render_return_evidence(&mut output, report);
    output.push_str("\n## Documentation Governance Recommendations\n");
    render_documentation_governance_return_markdown(
        &mut output,
        report.documentation_governance.as_ref(),
    );
    render_return_next_safe_action(&mut output, report);
    output.push_str("\n## Verification Commands\n");
    for command in &report.verification_commands {
        output.push_str(&format!("- `{command}`\n"));
    }
    output.push_str("\n## Context Policy\n");
    output.push_str("- Treat Offdesk results as evidence, not final truth.\n");
    output.push_str("- Re-read listed artifacts before continuing work.\n");
    output.push_str("- Do not delete or move files until commercial review and human approval are both recorded.\n");
    output
}

fn render_return_status(output: &mut String, report: &OffdeskCloseoutReport) {
    output.push_str("## Status\n");
    let state =
        if report.summary.active_or_blocked_tasks > 0 || report.summary.missing_artifacts > 0 {
            "blocked"
        } else if report.open_decisions.is_empty() {
            "evidence_ready"
        } else {
            "review_required"
        };
    output.push_str(&format!("- state: `{state}`\n"));
    output.push_str(&format!(
        "- tasks: {} completed / {} scanned; {} active_or_blocked\n",
        report.summary.completed_tasks,
        report.summary.tasks_scanned,
        report.summary.active_or_blocked_tasks
    ));
    output.push_str(&format!(
        "- file review: {} keep, {} archive candidates, {} delete candidates, {} missing artifacts\n",
        report.summary.keep_operations,
        report.summary.archive_candidates,
        report.summary.delete_candidates,
        report.summary.missing_artifacts
    ));
    if report.summary.implementation_packets_scanned > 0 {
        output.push_str(&format!(
            "- implementation packets: {} scanned; {} completed, {} deferred, {} missing, {} drifted\n",
            report.summary.implementation_packets_scanned,
            report.summary.packet_goals_completed,
            report.summary.packet_goals_deferred,
            report.summary.packet_goals_missing,
            report.summary.packet_goals_drifted
        ));
        if report.summary.packet_detail_items > 0 {
            output.push_str(&format!(
                "- packet detail items: {} completed, {} deferred, {} missing, {} drifted / {} total\n",
                report.summary.packet_detail_items_completed,
                report.summary.packet_detail_items_deferred,
                report.summary.packet_detail_items_missing,
                report.summary.packet_detail_items_drifted,
                report.summary.packet_detail_items
            ));
        }
    }
    if let Some(governance) = &report.documentation_governance {
        if governance.error.is_some() {
            output.push_str("- documentation governance: audit unavailable\n");
        } else {
            output.push_str(&format!(
                "- documentation governance: {} recommendation(s)\n",
                governance.recommendation_count
            ));
        }
    }
    output.push_str(&format!(
        "- source observation: `{}`; {} changed file(s)\n",
        report.source_observation.status, report.source_observation.changed_file_count
    ));
    output.push('\n');
}

fn render_return_source_observation(output: &mut String, report: &OffdeskCloseoutReport) {
    let observation = &report.source_observation;
    output.push_str("## Source Observation\n");
    output.push_str(&format!(
        "- status: `{}` from `{}` against `{}`\n",
        observation.status, observation.source_kind, observation.base_ref
    ));
    if let Some(workdir) = observation.workdir.as_deref() {
        output.push_str(&format!("- workdir: `{workdir}`\n"));
    }
    if !observation.available {
        if !observation.warnings.is_empty() {
            for warning in observation.warnings.iter().take(3) {
                output.push_str(&format!(
                    "- warning: {}\n",
                    truncate_closeout_text(warning, 180)
                ));
            }
        }
        output.push('\n');
        return;
    }
    if observation.changed_files.is_empty() {
        output.push_str("- changed files: none observed in the worktree.\n\n");
        return;
    }
    output.push_str(&format!(
        "- changed files: {} observed",
        observation.changed_file_count
    ));
    if observation.changed_files_truncated {
        output.push_str(" (truncated in closeout_plan.json)");
    }
    output.push('\n');
    for file in observation
        .changed_files
        .iter()
        .take(CLOSEOUT_RETURN_EVIDENCE_LIMIT)
    {
        output.push_str(&format!(
            "  - [{}] `{}` (+{} -{})\n",
            file.status, file.path, file.additions, file.deletions
        ));
    }
    if observation.changed_files.len() > CLOSEOUT_RETURN_EVIDENCE_LIMIT {
        output.push_str(&format!(
            "  - ... {} more changed file(s) are listed in `closeout_plan.json`.\n",
            observation.changed_files.len() - CLOSEOUT_RETURN_EVIDENCE_LIMIT
        ));
    }
    output.push('\n');
}

fn render_return_decisions(output: &mut String, report: &OffdeskCloseoutReport) {
    output.push_str("## Decision Needed\n");
    if report.open_decisions.is_empty() {
        output.push_str("- No open decision recorded. Start with the first reads and verification commands.\n\n");
        return;
    }
    for decision in report
        .open_decisions
        .iter()
        .take(CLOSEOUT_RETURN_DECISION_LIMIT)
    {
        output.push_str(&format!(
            "- {}: {}\n  - next: `{}`\n",
            decision.kind, decision.detail, decision.suggested_command
        ));
    }
    if report.open_decisions.len() > CLOSEOUT_RETURN_DECISION_LIMIT {
        output.push_str(&format!(
            "- ... {} more decision(s) are listed in `closeout_plan.json`.\n",
            report.open_decisions.len() - CLOSEOUT_RETURN_DECISION_LIMIT
        ));
    }
    output.push('\n');
}

fn render_implementation_packet_coverage_markdown(
    output: &mut String,
    coverage: &CloseoutImplementationPacketCoverage,
) {
    output.push_str("## Implementation Packet Coverage\n");
    if coverage.packet_count == 0 {
        output.push_str("- No implementation packet was linked to the matched closeout work.\n\n");
        return;
    }
    output.push_str(&format!(
        "- packets: {} scanned; {} completed, {} deferred, {} missing, {} drifted\n",
        coverage.packet_count,
        coverage.completed,
        coverage.deferred,
        coverage.missing,
        coverage.drifted
    ));
    if coverage.detail_items > 0 {
        output.push_str(&format!(
            "- detail items: {} completed, {} deferred, {} missing, {} drifted / {} total\n",
            coverage.detail_items_completed,
            coverage.detail_items_deferred,
            coverage.detail_items_missing,
            coverage.detail_items_drifted,
            coverage.detail_items
        ));
    }
    for item in coverage.items.iter().take(CLOSEOUT_RETURN_DECISION_LIMIT) {
        output.push_str(&format!(
            "- {}: status=`{}` safe_to_delegate={} outcome=`{}`\n",
            item.packet_id, item.goal_status, item.safe_to_delegate, item.outcome
        ));
        output.push_str(&format!("  - goal: {}\n", item.goal));
        output.push_str(&format!("  - success_state: {}\n", item.success_state));
        output.push_str(&format!("  - reason: {}\n", item.reason));
        output.push_str(&format!("  - detail_source: `{}`\n", item.detail_source));
        if let Some(error) = item.detail_error.as_deref() {
            output.push_str(&format!("  - detail_error: {}\n", error));
        }
        render_packet_detail_group(output, "work_slices", &item.work_slices);
        render_packet_detail_group(output, "validation_items", &item.validation_items);
        render_packet_detail_group(output, "expected_artifacts", &item.expected_artifacts);
        if !item.evidence_refs.is_empty() {
            output.push_str("  - evidence:");
            for evidence in item.evidence_refs.iter().take(5) {
                output.push_str(&format!(" `{evidence}`"));
            }
            if item.evidence_refs.len() > 5 {
                output.push_str(&format!(" (+{} more)", item.evidence_refs.len() - 5));
            }
            output.push('\n');
        }
        if !item.required_revisions.is_empty() {
            output.push_str("  - required_revisions:");
            for revision in item.required_revisions.iter().take(3) {
                output.push_str(&format!(" {}", truncate_closeout_text(revision, 120)));
            }
            if item.required_revisions.len() > 3 {
                output.push_str(&format!(" (+{} more)", item.required_revisions.len() - 3));
            }
            output.push('\n');
        }
        if !item.drift_signals.is_empty() {
            output.push_str("  - drift_signals:");
            for signal in item.drift_signals.iter().take(3) {
                output.push_str(&format!(" {}", truncate_closeout_text(signal, 120)));
            }
            if item.drift_signals.len() > 3 {
                output.push_str(&format!(" (+{} more)", item.drift_signals.len() - 3));
            }
            output.push('\n');
        }
        if !item.missing_decisions.is_empty() {
            output.push_str("  - missing_decisions:");
            for decision in item.missing_decisions.iter().take(3) {
                output.push_str(&format!(" {}", truncate_closeout_text(decision, 120)));
            }
            if item.missing_decisions.len() > 3 {
                output.push_str(&format!(" (+{} more)", item.missing_decisions.len() - 3));
            }
            output.push('\n');
        }
    }
    if coverage.items.len() > CLOSEOUT_RETURN_DECISION_LIMIT {
        output.push_str(&format!(
            "- ... {} more packet coverage item(s) are listed in `closeout_plan.json`.\n",
            coverage.items.len() - CLOSEOUT_RETURN_DECISION_LIMIT
        ));
    }
    output.push('\n');
}

fn render_packet_detail_group(
    output: &mut String,
    title: &str,
    details: &[CloseoutPacketCoverageDetail],
) {
    if details.is_empty() {
        return;
    }
    let attention = details
        .iter()
        .filter(|detail| detail.status != "completed")
        .collect::<Vec<_>>();
    let shown = if attention.is_empty() {
        details.iter().take(3).collect::<Vec<_>>()
    } else {
        attention.into_iter().take(3).collect::<Vec<_>>()
    };
    output.push_str(&format!("  - {title}:"));
    for detail in shown {
        output.push_str(&format!(
            " [{}] {}",
            detail.status,
            truncate_closeout_text(&detail.label, 80)
        ));
        if let Some(claim_status) = detail.claim_status {
            output.push_str(&format!(" (claim: {claim_status})"));
        } else if let Some(reported_status) = detail.reported_status {
            output.push_str(&format!(" (reported: {reported_status})"));
        }
        if let Some(trust_tier) = detail.trust_tier {
            output.push_str(&format!(" (trust: {trust_tier})"));
        }
        if let Some(source_status) = detail.source_observation_status {
            output.push_str(&format!(" (source: {source_status})"));
        }
        if detail.status != "completed" {
            if let Some(next) = detail.next_safe_action.as_deref() {
                output.push_str(&format!(" (next: {})", truncate_closeout_text(next, 100)));
            } else if let Some(summary) = detail.summary.as_deref() {
                output.push_str(&format!(
                    " (summary: {})",
                    truncate_closeout_text(summary, 100)
                ));
            }
        }
        if !detail.evidence_refs.is_empty() {
            output.push_str(" (evidence:");
            for evidence in detail.evidence_refs.iter().take(2) {
                output.push_str(&format!(" `{evidence}`"));
            }
            output.push(')');
        }
        if !detail.source_refs.is_empty() {
            output.push_str(" (source_refs:");
            for source_ref in detail.source_refs.iter().take(2) {
                output.push_str(&format!(" `{source_ref}`"));
            }
            output.push(')');
        }
    }
    if details.len() > 3 {
        output.push_str(&format!(" (+{} more)", details.len() - 3));
    }
    output.push('\n');
}

fn render_return_change_summary(output: &mut String, report: &OffdeskCloseoutReport) {
    output.push_str("\n## What Changed\n");
    output.push_str("- Closeout generated review artifacts only; project files were not moved, deleted, or archived.\n");
    output.push_str(&format!(
        "- Review packet: `{}`\n",
        report.artifacts.commercial_review_packet
    ));
    output.push_str(&format!(
        "- Full machine plan: `{}`\n",
        report.artifacts.closeout_plan_json
    ));
    output.push_str(&format!(
        "- Cleanup manifest: `{}`\n",
        report.artifacts.cleanup_manifest_json
    ));
}

fn render_return_evidence(output: &mut String, report: &OffdeskCloseoutReport) {
    output.push_str("\n## Evidence\n");
    render_return_evidence_group(output, report, "keep", "Kept review evidence");
    render_return_evidence_group(
        output,
        report,
        "archive_candidate",
        "Archive review candidates",
    );
    render_return_evidence_group(
        output,
        report,
        "delete_candidate",
        "Delete review candidates",
    );
}

fn render_return_evidence_group(
    output: &mut String,
    report: &OffdeskCloseoutReport,
    operation: &str,
    title: &str,
) {
    let mut seen_paths = BTreeSet::new();
    let operations = report
        .file_operations
        .iter()
        .filter(|item| item.operation == operation)
        .filter(|item| seen_paths.insert(item.path.clone()))
        .collect::<Vec<_>>();
    output.push_str(&format!("\n### {title}\n"));
    if operations.is_empty() {
        output.push_str("- None.\n");
        return;
    }
    for item in operations.iter().take(CLOSEOUT_RETURN_EVIDENCE_LIMIT) {
        output.push_str(&format!(
            "- {}: `{}`\n  - purpose: {}\n  - present: {} / risk: {} / review_required: {}\n",
            closeout_operation_label(item),
            item.path,
            item.reason,
            item.present,
            item.risk,
            item.requires_commercial_review || item.requires_human_approval
        ));
    }
    if operations.len() > CLOSEOUT_RETURN_EVIDENCE_LIMIT {
        output.push_str(&format!(
            "- ... {} more `{operation}` item(s) are listed in `cleanup_manifest.json`.\n",
            operations.len() - CLOSEOUT_RETURN_EVIDENCE_LIMIT
        ));
    }
}

fn render_documentation_governance_return_markdown(
    output: &mut String,
    governance: Option<&CloseoutDocumentationGovernance>,
) {
    let Some(governance) = governance else {
        output.push_str("- No project workdir was available for documentation governance audit.\n");
        return;
    };
    output.push_str(&format!(
        "- audit source: `{}` profile for `{}`\n",
        governance.audit_profile, governance.workdir
    ));
    output.push_str(&format!("- full audit command: `{}`\n", governance.command));
    if let Some(error) = &governance.error {
        output.push_str(&format!("- audit unavailable: {}\n", error));
        return;
    }
    if governance.recommendations.is_empty() {
        output.push_str("- No documentation governance recommendations.\n");
        return;
    }
    for recommendation in &governance.recommendations {
        output.push_str(&format!(
            "- {}: {} (`{}`)\n  - action: {}\n",
            recommendation.priority,
            recommendation.title,
            recommendation.kind,
            recommendation.suggested_action
        ));
        if !recommendation.paths.is_empty() {
            output.push_str("  - focus:");
            for path in recommendation
                .paths
                .iter()
                .take(CLOSEOUT_RETURN_GOVERNANCE_PATH_LIMIT)
            {
                output.push_str(&format!(" `{path}`"));
            }
            if recommendation.paths.len() > CLOSEOUT_RETURN_GOVERNANCE_PATH_LIMIT {
                output.push_str(&format!(
                    " (+{} more)",
                    recommendation.paths.len() - CLOSEOUT_RETURN_GOVERNANCE_PATH_LIMIT
                ));
            }
            output.push('\n');
        }
    }
}

fn render_return_next_safe_action(output: &mut String, report: &OffdeskCloseoutReport) {
    output.push_str("\n## Next Safe Action\n");
    if let Some(decision) = report.open_decisions.first() {
        output.push_str(&format!(
            "- Resolve `{}` first: {}\n",
            decision.kind, decision.detail
        ));
        output.push_str(&format!(
            "- Suggested command/review: `{}`\n",
            decision.suggested_command
        ));
        return;
    }
    if let Some(command) = report
        .verification_commands
        .iter()
        .find(|command| command.contains("forager ondesk prompt-package"))
    {
        output.push_str(&format!("- Rehydrate Ondesk with `{command}`.\n"));
        return;
    }
    output.push_str("- Run the verification commands below before continuing work.\n");
}

fn prioritized_closeout_first_reads(reads: &[CloseoutReadRef]) -> Vec<&CloseoutReadRef> {
    let mut seen = BTreeSet::new();
    let mut prioritized = reads
        .iter()
        .filter(|read| read.present)
        .filter(|read| seen.insert(read.path.clone()))
        .collect::<Vec<_>>();
    prioritized.sort_by(|left, right| {
        (closeout_read_priority(left), left.path.as_str())
            .cmp(&(closeout_read_priority(right), right.path.as_str()))
    });
    prioritized
}

fn closeout_read_priority(read: &CloseoutReadRef) -> u8 {
    if read.reason.contains("Result artifacts")
        || read.reason.contains("Background result artifacts")
    {
        0
    } else if read.reason.contains("Declared task artifacts") {
        1
    } else {
        2
    }
}

fn closeout_read_label(read: &CloseoutReadRef) -> &'static str {
    if read.reason.contains("Result artifacts")
        || read.reason.contains("Background result artifacts")
    {
        "Result evidence"
    } else if read.reason.contains("Declared task artifacts") {
        "Declared artifact"
    } else {
        "Review evidence"
    }
}

fn closeout_operation_label(operation: &CloseoutFileOperation) -> &'static str {
    if operation.source.contains("result_artifact") {
        "result artifact"
    } else if operation.source.contains("log_artifact") {
        "runtime log"
    } else if operation.source.contains("artifact_ref") {
        "declared artifact"
    } else {
        "artifact"
    }
}

fn render_documentation_governance_markdown(
    output: &mut String,
    governance: Option<&CloseoutDocumentationGovernance>,
) {
    let Some(governance) = governance else {
        output.push_str("- No project workdir was available for documentation governance audit.\n");
        return;
    };
    output.push_str(&format!("- workdir: `{}`\n", governance.workdir));
    output.push_str(&format!("- audit: `{}`\n", governance.command));
    if let Some(error) = &governance.error {
        output.push_str(&format!("- audit_error: {}\n", error));
        return;
    }
    if governance.recommendations.is_empty() {
        output.push_str("- No documentation governance recommendations.\n");
        return;
    }
    for recommendation in &governance.recommendations {
        output.push_str(&format!(
            "- {} `{}`: {}\n",
            recommendation.priority, recommendation.kind, recommendation.title
        ));
        output.push_str(&format!(
            "  - action: {}\n",
            recommendation.suggested_action
        ));
        if !recommendation.paths.is_empty() {
            output.push_str("  - focus paths:\n");
            for path in &recommendation.paths {
                output.push_str(&format!("    - `{path}`\n"));
            }
        }
    }
}

fn render_commercial_review_packet(report: &OffdeskCloseoutReport) -> String {
    let mut output = String::new();
    output.push_str("# Commercial Model Closeout Review Packet\n\n");
    output.push_str(
        "Review the proposed closeout plan for file movement, archive, and deletion risk.\n",
    );
    output
        .push_str("Do not execute shell commands. Return only a review verdict and rationale.\n\n");
    output.push_str("## Required Verdict Schema\n");
    output.push_str("```json\n");
    output.push_str(
        "{\n  \"verdict\": \"approved|revise|blocked\",\n  \"unsafe_operations\": [],\n  \"missing_evidence\": [],\n  \"required_first_reads\": [],\n  \"packet_goal_coverage\": \"completed|deferred|missing|drifted\",\n  \"notes\": \"\"\n}\n",
    );
    output.push_str("```\n\n");
    output.push_str("## Safety Rules\n");
    for rule in &report.review_contract.safety_rules {
        output.push_str(&format!("- {rule}\n"));
    }
    output.push('\n');
    render_implementation_packet_coverage_markdown(
        &mut output,
        &report.implementation_packet_coverage,
    );
    output.push_str("\n## Candidate Operations\n");
    if report.file_operations.is_empty() {
        output.push_str("- No file operations proposed.\n");
    } else {
        for operation in &report.file_operations {
            output.push_str(&format!(
                "- operation: {}\n  path: `{}`\n  destination: `{}`\n  risk: {}\n  present: {}\n  reason: {}\n  evidence: {}\n",
                operation.operation,
                operation.path,
                operation.destination.as_deref().unwrap_or("-"),
                operation.risk,
                operation.present,
                operation.reason,
                operation.evidence_refs.join(", ")
            ));
        }
    }
    output.push_str("\n## Open Decisions\n");
    for decision in &report.open_decisions {
        output.push_str(&format!("- {}: {}\n", decision.kind, decision.detail));
    }
    output
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
        Duration::hours(wiki_review_near_expiry_hours),
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
        Duration::hours(wiki_runtime_ack_near_expiry_hours),
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

fn parse_rfc3339(value: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    Ok(Some(
        DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc),
    ))
}

fn parse_rfc3339_datetime(value: &str) -> std::result::Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|err| format!("timestamp must be RFC3339: {err}"))
}

fn parse_background_runner_kind(value: &str) -> std::result::Result<BackgroundRunnerKind, String> {
    value.parse()
}

fn parse_maintenance_action_kind(
    value: &str,
) -> std::result::Result<MaintenanceActionKind, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "runtime_recovery" | "runtime-recovery" | "recovery" => {
            Ok(MaintenanceActionKind::RuntimeRecovery)
        }
        "wiki_runtime_ack" | "wiki-runtime-ack" | "runtime_ack" | "runtime-ack" => {
            Ok(MaintenanceActionKind::WikiRuntimeAck)
        }
        "wiki_review_after" | "wiki-review-after" | "review_after" | "review-after" => {
            Ok(MaintenanceActionKind::WikiReviewAfter)
        }
        "wiki_mutation" | "wiki-mutation" | "wiki" => Ok(MaintenanceActionKind::WikiMutation),
        "provider_capacity" | "provider-capacity" | "capacity" => {
            Ok(MaintenanceActionKind::ProviderCapacity)
        }
        "artifact_cleanup" | "artifact-cleanup" | "cleanup" => {
            Ok(MaintenanceActionKind::ArtifactCleanup)
        }
        "service_restart" | "service-restart" | "restart" => {
            Ok(MaintenanceActionKind::ServiceRestart)
        }
        "system_change" | "system-change" | "system" => Ok(MaintenanceActionKind::SystemChange),
        _ => Err(
            "maintenance kind must be one of runtime_recovery, wiki_runtime_ack, wiki_review_after, wiki_mutation, provider_capacity, artifact_cleanup, service_restart, system_change"
                .to_string(),
        ),
    }
}

fn parse_risk_level(value: &str) -> std::result::Result<RiskLevel, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "safe" => Ok(RiskLevel::Safe),
        "runtime_mutation" | "runtime-mutation" | "runtime" => Ok(RiskLevel::RuntimeMutation),
        "canonical_mutation" | "canonical-mutation" | "canonical" => {
            Ok(RiskLevel::CanonicalMutation)
        }
        "destructive" | "delete" | "cleanup" => Ok(RiskLevel::Destructive),
        "external_side_effect" | "external-side-effect" | "external" => {
            Ok(RiskLevel::ExternalSideEffect)
        }
        _ => Err(
            "risk must be one of safe, runtime_mutation, canonical_mutation, destructive, external_side_effect"
                .to_string(),
        ),
    }
}

fn parse_artifact_ref(value: &str) -> std::result::Result<CapabilityArtifactRef, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("artifact reference must not be empty".to_string());
    }
    if let Some((artifact_id, path)) = trimmed.split_once('=') {
        let artifact_id = artifact_id.trim();
        let path = path.trim();
        if artifact_id.is_empty() || path.is_empty() {
            return Err("artifact reference must use ARTIFACT_ID=PATH".to_string());
        }
        Ok(CapabilityArtifactRef::new(
            artifact_id.to_string(),
            Some(path.to_string()),
        ))
    } else {
        Ok(CapabilityArtifactRef::new(
            trimmed.to_string(),
            None::<String>,
        ))
    }
}

fn parse_adaptive_wiki_scope(value: &str) -> std::result::Result<AdaptiveWikiScope, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "session" => Ok(AdaptiveWikiScope::Session),
        "artifact_kind" | "artifact-kind" | "artifact" => Ok(AdaptiveWikiScope::ArtifactKind),
        "project" => Ok(AdaptiveWikiScope::Project),
        "user_global" | "user-global" | "global" => Ok(AdaptiveWikiScope::UserGlobal),
        _ => Err("scope must be one of session, artifact_kind, project, user_global".to_string()),
    }
}

fn parse_adaptive_wiki_kind(value: &str) -> std::result::Result<AdaptiveWikiKind, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "preference" | "pref" => Ok(AdaptiveWikiKind::Preference),
        "procedure" | "proc" => Ok(AdaptiveWikiKind::Procedure),
        "failure_pattern" | "failure-pattern" | "failure" | "fail" => {
            Ok(AdaptiveWikiKind::FailurePattern)
        }
        "policy_rule" | "policy-rule" | "policy" => Ok(AdaptiveWikiKind::PolicyRule),
        "fact" => Ok(AdaptiveWikiKind::Fact),
        _ => Err(
            "kind must be one of preference, procedure, failure_pattern, policy_rule, fact"
                .to_string(),
        ),
    }
}

fn parse_adaptive_wiki_confidence(
    value: &str,
) -> std::result::Result<AdaptiveWikiConfidence, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "explicit" => Ok(AdaptiveWikiConfidence::Explicit),
        "repeated" => Ok(AdaptiveWikiConfidence::Repeated),
        "inferred" => Ok(AdaptiveWikiConfidence::Inferred),
        _ => Err("confidence must be one of explicit, repeated, inferred".to_string()),
    }
}

fn parse_adaptive_wiki_signal_kind(
    value: &str,
) -> std::result::Result<AdaptiveWikiSignalKind, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "operator_correction" | "operator-correction" | "correction" => {
            Ok(AdaptiveWikiSignalKind::OperatorCorrection)
        }
        "explicit_preference" | "explicit-preference" | "preference" => {
            Ok(AdaptiveWikiSignalKind::ExplicitPreference)
        }
        "imported_doc" | "imported-doc" | "doc" => Ok(AdaptiveWikiSignalKind::ImportedDoc),
        "repeated_failure" | "repeated-failure" => Ok(AdaptiveWikiSignalKind::RepeatedFailure),
        _ => Err(
            "signal kind must be one of operator_correction, explicit_preference, imported_doc, repeated_failure"
                .to_string(),
        ),
    }
}

fn parse_adaptive_wiki_origin(value: &str) -> std::result::Result<AdaptiveWikiOrigin, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "operator_explicit" | "operator-explicit" | "operator" => {
            Ok(AdaptiveWikiOrigin::OperatorExplicit)
        }
        "background_review" | "background-review" | "background" => {
            Ok(AdaptiveWikiOrigin::BackgroundReview)
        }
        "imported" => Ok(AdaptiveWikiOrigin::Imported),
        _ => {
            Err("origin must be one of operator_explicit, background_review, imported".to_string())
        }
    }
}

fn parse_adaptive_wiki_runtime_policy_ack_scope_mode(
    value: &str,
) -> std::result::Result<AdaptiveWikiRuntimePolicyAckScopeMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "exact_query" | "exact-query" | "exact" => {
            Ok(AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery)
        }
        "project_artifact" | "project-artifact" => {
            Ok(AdaptiveWikiRuntimePolicyAckScopeMode::ProjectArtifact)
        }
        _ => Err("scope mode must be one of exact_query, project_artifact".to_string()),
    }
}

fn parse_adaptive_wiki_activation_mode(
    value: &str,
) -> std::result::Result<AdaptiveWikiActivationMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "context_only" | "context-only" => Ok(AdaptiveWikiActivationMode::ContextOnly),
        "confirm" => Ok(AdaptiveWikiActivationMode::Confirm),
        "auto_apply" | "auto-apply" => Ok(AdaptiveWikiActivationMode::AutoApply),
        _ => Err("activation mode must be one of context_only, confirm, auto_apply".to_string()),
    }
}

fn parse_adaptive_wiki_agent_mode(
    value: &str,
) -> std::result::Result<AdaptiveWikiAgentMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "planning" | "plan" | "planner" => Ok(AdaptiveWikiAgentMode::Planning),
        "code_development" | "code-development" | "code" | "coding" | "development" => {
            Ok(AdaptiveWikiAgentMode::Development)
        }
        "analysis" | "analyze" | "analyse" | "diagnostics" | "diagnostic" => {
            Ok(AdaptiveWikiAgentMode::Analysis)
        }
        "research_writing" | "research-writing" | "research" | "writing" | "editing" => {
            Ok(AdaptiveWikiAgentMode::Writing)
        }
        "critique" | "critic" => Ok(AdaptiveWikiAgentMode::Critique),
        "review" | "reviewer" => Ok(AdaptiveWikiAgentMode::Review),
        "maintenance" | "maintain" | "maintainer" | "ops" | "health" => {
            Ok(AdaptiveWikiAgentMode::Maintenance)
        }
        _ => Err(
            "agent mode must be one of planning, development, analysis, writing, critique, review, maintenance".to_string(),
        ),
    }
}

fn parse_adaptive_wiki_review_action(
    value: &str,
) -> std::result::Result<AdaptiveWikiReviewProposalAction, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "promote" => Ok(AdaptiveWikiReviewProposalAction::Promote),
        "reject" => Ok(AdaptiveWikiReviewProposalAction::Reject),
        "rescope" => Ok(AdaptiveWikiReviewProposalAction::Rescope),
        "deprecate" => Ok(AdaptiveWikiReviewProposalAction::Deprecate),
        "add_counterexample" | "add-counterexample" => {
            Ok(AdaptiveWikiReviewProposalAction::AddCounterexample)
        }
        "renew_review" | "renew-review" => Ok(AdaptiveWikiReviewProposalAction::RenewReview),
        "split" => Ok(AdaptiveWikiReviewProposalAction::Split),
        "merge" => Ok(AdaptiveWikiReviewProposalAction::Merge),
        _ => Err("proposal action must be one of promote, reject, rescope, deprecate, add_counterexample, renew_review, split, merge".to_string()),
    }
}

fn parse_wiki_proposal_handoff_mutation(
    value: &str,
) -> std::result::Result<WikiProposalHandoffMutation, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "rescope" => Ok(WikiProposalHandoffMutation::Rescope),
        "deprecate" => Ok(WikiProposalHandoffMutation::Deprecate),
        "add_counterexample" | "add-counterexample" => {
            Ok(WikiProposalHandoffMutation::AddCounterexample)
        }
        "deprecate_duplicate" | "deprecate-duplicate" => {
            Ok(WikiProposalHandoffMutation::DeprecateDuplicate)
        }
        "split" => Ok(WikiProposalHandoffMutation::Split),
        _ => Err(
            "mutation must be one of rescope, deprecate, add_counterexample, deprecate_duplicate, split"
                .to_string(),
        ),
    }
}

fn parse_adaptive_wiki_proposal_decision(
    value: &str,
) -> std::result::Result<AdaptiveWikiReviewProposalDecision, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "accepted" | "accept" => Ok(AdaptiveWikiReviewProposalDecision::Accepted),
        "rejected" | "reject" => Ok(AdaptiveWikiReviewProposalDecision::Rejected),
        "superseded" | "supersede" => Ok(AdaptiveWikiReviewProposalDecision::Superseded),
        _ => Err("proposal decision must be one of accepted, rejected, superseded".to_string()),
    }
}

fn empty_dash(value: &str) -> &str {
    if value.trim().is_empty() {
        "-"
    } else {
        value
    }
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

fn print_wiki_entries(entries: &[AdaptiveWikiHumanEntry]) {
    println!(
        "{:<44} {:<12} {:<14} {:<16} {:<18} CLAIM",
        "ID", "STATUS", "SCOPE", "ACTIVATION", "AGENT_MODES"
    );
    for entry in entries {
        println!(
            "{:<44} {:<12} {:<14} {:<16} {:<18} {}",
            entry.id,
            format!("{:?}", entry.status).to_lowercase(),
            wiki_scope_label(entry.scope, &entry.scope_ref),
            format!("{:?}", entry.activation_mode).to_lowercase(),
            adaptive_wiki_agent_modes_label(&entry.agent_modes),
            entry.claim
        );
        if !entry.human_summary.trim().is_empty() {
            println!("  summary: {}", entry.human_summary);
        }
        if !entry.evidence_refs.is_empty() {
            println!("  evidence: {}", entry.evidence_refs.join(", "));
        }
        if !entry.support_refs.is_empty() {
            println!("  support: {}", entry.support_refs.join(", "));
        }
        if !entry.capability_ids.is_empty() {
            println!("  capabilities: {}", entry.capability_ids.join(", "));
        }
        if !entry.required_artifact_kinds.is_empty() {
            println!("  artifacts: {}", entry.required_artifact_kinds.join(", "));
        }
    }
}

fn print_wiki_candidates(candidates: &[AdaptiveWikiHumanCandidate]) {
    println!(
        "{:<44} {:<14} {:<18} {:<18} {:<8} CLAIM",
        "ID", "SCOPE", "SIGNAL", "AGENT_MODES", "HITS"
    );
    for candidate in candidates {
        println!(
            "{:<44} {:<14} {:<18} {:<18} {:<8} {}",
            candidate.id,
            wiki_scope_label(candidate.scope, &candidate.scope_ref),
            format!("{:?}", candidate.signal_kind).to_lowercase(),
            adaptive_wiki_agent_modes_label(&candidate.agent_modes),
            candidate.occurrence_count,
            candidate.claim
        );
        if !candidate.review_reason.trim().is_empty() {
            println!("  review: {}", candidate.review_reason);
        }
        if !candidate.source_refs.is_empty() {
            println!("  sources: {}", candidate.source_refs.join(", "));
        } else if !candidate.evidence_refs.is_empty() {
            println!("  evidence: {}", candidate.evidence_refs.join(", "));
        }
    }
}

fn print_wiki_show(result: &WikiShowResult) {
    match result {
        WikiShowResult::Entry { entry } => {
            println!("Adaptive wiki entry {}", entry.id);
            println!("  status:     {:?}", entry.status);
            println!("  kind:       {:?}", entry.kind);
            println!(
                "  scope:      {}",
                wiki_scope_label(entry.scope, &entry.scope_ref)
            );
            println!("  activation: {:?}", entry.activation_mode);
            println!(
                "  agent_modes: {}",
                adaptive_wiki_agent_modes_label(&entry.agent_modes)
            );
            println!("  confidence: {:?}", entry.confidence);
            println!("  claim:      {}", entry.claim);
            if !entry.human_summary.trim().is_empty() {
                println!("  summary:    {}", entry.human_summary);
            }
            if !entry.evidence_refs.is_empty() {
                println!("  evidence:   {}", entry.evidence_refs.join(", "));
            }
            if !entry.support_refs.is_empty() {
                println!("  support:    {}", entry.support_refs.join(", "));
            }
            if !entry.capability_ids.is_empty() {
                println!("  capabilities: {}", entry.capability_ids.join(", "));
            }
            if !entry.required_artifact_kinds.is_empty() {
                println!("  artifacts:  {}", entry.required_artifact_kinds.join(", "));
            }
        }
        WikiShowResult::Candidate { candidate } => {
            println!("Adaptive wiki candidate {}", candidate.id);
            println!("  kind:       {:?}", candidate.kind);
            println!(
                "  scope:      {}",
                wiki_scope_label(candidate.scope, &candidate.scope_ref)
            );
            println!("  signal:     {:?}", candidate.signal_kind);
            println!("  origin:     {:?}", candidate.origin);
            println!(
                "  agent_modes: {}",
                adaptive_wiki_agent_modes_label(&candidate.agent_modes)
            );
            println!("  hits:       {}", candidate.occurrence_count);
            println!("  confidence: {:?}", candidate.confidence);
            println!("  claim:      {}", candidate.claim);
            if !candidate.human_summary.trim().is_empty() {
                println!("  summary:    {}", candidate.human_summary);
            }
            if !candidate.review_reason.trim().is_empty() {
                println!("  review:     {}", candidate.review_reason);
            }
            if !candidate.source_refs.is_empty() {
                println!("  sources:    {}", candidate.source_refs.join(", "));
            }
        }
    }
}

fn print_wiki_mutation(result: &WikiMutationResult, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(result)?);
        return Ok(());
    }

    match result {
        WikiMutationResult::Promote {
            entry,
            audit,
            promotion_receipt,
            promotion_receipt_path,
        } => {
            println!("Promoted adaptive wiki candidate to entry {}", entry.id);
            println!(
                "  scope: {}",
                wiki_scope_label(entry.scope, &entry.scope_ref)
            );
            println!("  activation:  {:?}", entry.activation_mode);
            println!(
                "  agent_modes: {}",
                adaptive_wiki_agent_modes_label(&entry.agent_modes)
            );
            println!("  audit: {}", audit.id);
            println!("  receipt: {}", promotion_receipt.receipt_id);
            println!("  receipt_path: {promotion_receipt_path}");
        }
        WikiMutationResult::Reject { candidate, audit } => {
            println!("Rejected adaptive wiki candidate {}", candidate.id);
            println!("  reason: {}", audit.reason);
            println!("  audit:  {}", audit.id);
        }
        WikiMutationResult::Rescope { entry, audit } => {
            println!("Rescoped adaptive wiki entry {}", entry.id);
            println!(
                "  scope: {}",
                wiki_scope_label(entry.scope, &entry.scope_ref)
            );
            println!("  audit: {}", audit.id);
        }
        WikiMutationResult::Edit { entry, audit } => {
            println!("Edited adaptive wiki entry {}", entry.id);
            println!("  claim: {}", entry.claim);
            if !entry.evidence_refs.is_empty() {
                println!("  evidence: {}", entry.evidence_refs.join(", "));
            }
            println!("  audit: {}", audit.id);
        }
        WikiMutationResult::Retag { entry, audit } => {
            println!("Retagged adaptive wiki entry {}", entry.id);
            if !entry.core_tags.is_empty() {
                println!("  core tags: {}", entry.core_tags.join(", "));
            }
            if !entry.proposed_tags.is_empty() {
                println!("  proposed tags: {}", entry.proposed_tags.join(", "));
            }
            println!("  audit: {}", audit.id);
        }
        WikiMutationResult::Deprecate { entry, audit } => {
            println!("Deprecated adaptive wiki entry {}", entry.id);
            println!("  reason: {}", audit.reason);
            println!("  audit:  {}", audit.id);
        }
        WikiMutationResult::AddCounterexample { entry, audit } => {
            println!("Added adaptive wiki counterexample to {}", entry.id);
            if let Some(evidence_ref) = audit.evidence_ref.as_deref() {
                println!("  evidence: {evidence_ref}");
            }
            println!("  audit:    {}", audit.id);
        }
        WikiMutationResult::UpdateRunbook { entry, audit } => {
            println!("Updated adaptive wiki runbook {}", entry.id);
            if !entry.support_refs.is_empty() {
                println!("  support: {}", entry.support_refs.join(", "));
            }
            if !entry.capability_ids.is_empty() {
                println!("  capabilities: {}", entry.capability_ids.join(", "));
            }
            if !entry.required_artifact_kinds.is_empty() {
                println!("  artifacts: {}", entry.required_artifact_kinds.join(", "));
            }
            println!("  audit:   {}", audit.id);
        }
        WikiMutationResult::RenewReviewAfter {
            entry,
            previous_review_after,
            audit,
        } => {
            println!("Renewed adaptive wiki review_after {}", entry.id);
            println!(
                "  previous: {}",
                previous_review_after
                    .as_ref()
                    .map(DateTime::<Utc>::to_rfc3339)
                    .unwrap_or_else(|| "-".to_string())
            );
            println!(
                "  review_after: {}",
                entry
                    .review_after
                    .as_ref()
                    .map(DateTime::<Utc>::to_rfc3339)
                    .unwrap_or_else(|| "-".to_string())
            );
            println!("  audit: {}", audit.id);
        }
    }
    Ok(())
}

fn print_wiki_proposal_handoff(preview: &WikiProposalHandoffPreview) {
    println!(
        "Adaptive wiki proposal handoff {}: {}",
        preview.proposal_id, preview.status
    );
    println!(
        "  proposal: {:?} {} {}",
        preview.action, preview.subject_kind, preview.subject_id
    );
    println!("  reason: {}", preview.reason);
    if let Some(decision) = preview.lifecycle_decision {
        println!(
            "  lifecycle: {:?}{}",
            decision,
            if preview.lifecycle_stale {
                " stale"
            } else {
                ""
            }
        );
    }
    if let Some(command) = preview.command.as_deref() {
        println!("  command: {command}");
    }
    if !preview.required_inputs.is_empty() {
        println!("  required inputs:");
        for input in &preview.required_inputs {
            let flag = input.cli_flag.unwrap_or(input.name);
            let required = if input.required {
                "required"
            } else {
                "conditional"
            };
            println!("    {flag} ({required}): {}", input.description);
        }
    }
    if !preview.mutation_options.is_empty() {
        println!("  mutation options:");
        for option in &preview.mutation_options {
            println!("    {}: {}", option.mutation, option.command_template);
            println!("      {}", option.description);
        }
    }
    if !preview.evidence_refs.is_empty() {
        println!("  evidence: {}", preview.evidence_refs.join(", "));
    }
}

fn print_wiki_proposal_receipt(receipt: &WikiProposalReceipt) {
    println!(
        "Adaptive wiki proposal receipt {}: {}",
        receipt.proposal.proposal_id, receipt.status
    );
    println!(
        "  proposal: {} {} current={}",
        empty_dash(&receipt.proposal.subject_kind),
        empty_dash(&receipt.proposal.subject_id),
        receipt.proposal.current
    );
    if let Some(action) = receipt.proposal.action {
        println!("  action: {:?}", action);
    }
    if let Some(decision) = receipt.proposal.lifecycle_decision {
        println!("  lifecycle: {:?}", decision);
    }
    if let Some(event_id) = receipt.proposal.lifecycle_event_id.as_deref() {
        println!("  event: {event_id}");
    }
    if let Some(audit) = receipt.audit.as_ref() {
        println!("  audit: {}", audit.id);
    }
    println!("  command_sha256: {}", receipt.preview_command_sha256);
    println!("  command: {}", receipt.preview_command);
    println!("  checks:");
    for check in &receipt.checks {
        println!(
            "    {}: {} ({})",
            check.name,
            if check.passed { "pass" } else { "fail" },
            check.detail
        );
    }
    if !receipt.blockers.is_empty() {
        println!("  blockers:");
        for blocker in &receipt.blockers {
            println!("    - {blocker}");
        }
    }
    if !receipt.proposal.evidence_refs.is_empty() {
        println!("  evidence: {}", receipt.proposal.evidence_refs.join(", "));
    }
}

fn print_wiki_lint(report: &AdaptiveWikiLintReport) {
    println!(
        "Adaptive wiki lint: {} errors, {} warnings, {} info ({} entries, {} candidates)",
        report.summary.errors,
        report.summary.warnings,
        report.summary.info,
        report.summary.entries_checked,
        report.summary.candidates_checked
    );
    for issue in &report.issues {
        println!(
            "  - {:?} {} {}: {}",
            issue.severity, issue.subject_kind, issue.subject_id, issue.message
        );
    }
}

fn print_wiki_projection_report(report: &AdaptiveWikiProjectionReport) {
    println!(
        "Adaptive wiki projection: {} selected, {} rejected, {} conflicts, {} review-expired ({} matching promoted entries)",
        report.summary.selected,
        report.summary.rejected,
        report.summary.conflicts,
        report.summary.review_expired_projected,
        report.summary.promoted_scope_matches
    );
    println!(
        "  budget: entries={} context_chars={} instruction_chars={}",
        report.budget.max_entries,
        report.budget.max_context_chars,
        report.budget.max_instruction_chars
    );
    println!(
        "  policy: review_expired={:?}",
        report.policy.review_expired
    );
    println!(
        "  estimated_context_chars: {}",
        report.summary.estimated_context_chars
    );
    if report.summary.instructions_truncated > 0 {
        println!(
            "  instructions_truncated: {}",
            report.summary.instructions_truncated
        );
    }
    if !report.selected.is_empty() {
        println!("  selected:");
        for entry in &report.selected {
            println!(
                "    {} {:?} {:?}:{} {:?} agent_modes={} evidence={}",
                entry.id,
                entry.kind,
                entry.scope,
                entry.scope_ref,
                entry.confidence,
                adaptive_wiki_agent_modes_label(&entry.agent_modes),
                entry.evidence_count
            );
        }
    }
    if !report.rejected.is_empty() {
        println!("  rejected:");
        for rejection in &report.rejected {
            println!(
                "    {} {:?}: {}",
                rejection.entry_id, rejection.reason, rejection.detail
            );
        }
    }
    if !report.conflicts.is_empty() {
        println!("  conflicts:");
        for conflict in &report.conflicts {
            println!(
                "    {} <-> {} {}: {}",
                conflict.entry_id,
                conflict.conflicting_entry_id,
                conflict.signature,
                conflict.detail
            );
        }
    }
    if !report.review_expired.is_empty() {
        println!("  review_expired:");
        for warning in &report.review_expired {
            println!(
                "    {} {:?}: review_after={} {}",
                warning.entry_id, warning.scope, warning.review_after, warning.detail
            );
        }
    }
}

fn print_wiki_projection_comparison_report(report: &AdaptiveWikiProjectionComparisonReport) {
    println!("Adaptive wiki projection review-expired policy comparison");
    println!(
        "  budget: entries={} context_chars={} instruction_chars={}",
        report.budget.max_entries,
        report.budget.max_context_chars,
        report.budget.max_instruction_chars
    );
    println!(
        "  warn:   selected={} rejected={} review_expired_projected={} context_chars={}",
        report.summary.warn_selected,
        report.summary.warn_rejected,
        report.warn.summary.review_expired_projected,
        report.summary.warn_estimated_context_chars
    );
    println!(
        "  strict: selected={} rejected={} review_expired_projected={} context_chars={}",
        report.summary.strict_selected,
        report.summary.strict_rejected,
        report.strict.summary.review_expired_projected,
        report.summary.strict_estimated_context_chars
    );
    if !report.summary.selected_only_in_warn.is_empty() {
        println!(
            "  selected only in warn: {}",
            report.summary.selected_only_in_warn.join(", ")
        );
    }
    if !report.summary.selected_only_in_strict.is_empty() {
        println!(
            "  selected only in strict: {}",
            report.summary.selected_only_in_strict.join(", ")
        );
    }
    if !report.summary.review_expired_excluded.is_empty() {
        println!(
            "  review_expired_excluded: {}",
            report.summary.review_expired_excluded.join(", ")
        );
    }
}

fn print_wiki_runtime_policy_acknowledgements(
    acknowledgements: &[AdaptiveWikiRuntimePolicyAcknowledgement],
) {
    println!(
        "{:<48} {:<18} {:<22} {:<22} POLICY",
        "ID", "SCOPE_MODE", "CREATED_AT", "EXPIRES_AT"
    );
    for acknowledgement in acknowledgements {
        println!(
            "{:<48} {:<18} {:<22} {:<22} review_expired={:?}",
            acknowledgement.id,
            runtime_ack_scope_mode_label(acknowledgement.scope_mode),
            acknowledgement.created_at,
            acknowledgement.expires_at,
            acknowledgement.policy.review_expired
        );
    }
}

fn print_wiki_runtime_policy_acknowledgement(
    acknowledgement: &AdaptiveWikiRuntimePolicyAcknowledgement,
) {
    println!(
        "  policy: review_expired={:?}",
        acknowledgement.policy.review_expired
    );
    println!(
        "  scope_mode: {}",
        runtime_ack_scope_mode_label(acknowledgement.scope_mode)
    );
    println!("  comparison_hash: {}", acknowledgement.comparison_hash);
    println!("  expires_at: {}", acknowledgement.expires_at);
    if !acknowledgement.review_expired_excluded.is_empty() {
        println!(
            "  review_expired_excluded: {}",
            acknowledgement.review_expired_excluded.join(", ")
        );
    }
    if !acknowledgement.selected_only_in_warn.is_empty() {
        println!(
            "  selected_only_in_warn: {}",
            acknowledgement.selected_only_in_warn.join(", ")
        );
    }
    if !acknowledgement.selected_only_in_strict.is_empty() {
        println!(
            "  selected_only_in_strict: {}",
            acknowledgement.selected_only_in_strict.join(", ")
        );
    }
    if !acknowledgement.reason.trim().is_empty() {
        println!("  reason: {}", acknowledgement.reason);
    }
}

fn print_wiki_runtime_policy_ack_report(report: &WikiRuntimePolicyAckReport) {
    println!("Adaptive wiki runtime policy acknowledgement report");
    println!(
        "  total: {}  active: {}  near_expiry: {}  expired: {}",
        report.summary.total,
        report.summary.active,
        report.summary.near_expiry,
        report.summary.expired
    );
    if let Some(decision) = report.decision.as_ref() {
        println!(
            "  query_decision: {:?} ack={} reason={}",
            decision.status,
            decision.acknowledgement_id.as_deref().unwrap_or("-"),
            decision.reason
        );
    }
    if report.acknowledgements.is_empty() {
        println!("No adaptive wiki runtime policy acknowledgements found.");
        return;
    }
    println!(
        "{:<48} {:<18} {:<34} {:<22} STATUS",
        "ID", "SCOPE_MODE", "QUERY", "EXPIRES_AT"
    );
    for acknowledgement in &report.acknowledgements {
        println!(
            "{:<48} {:<18} {:<34} {:<22} {}",
            acknowledgement.id,
            runtime_ack_scope_mode_label(acknowledgement.scope_mode),
            runtime_ack_query_label(&acknowledgement.query),
            acknowledgement.expires_at,
            acknowledgement.status.join(",")
        );
        if let Some(action) = acknowledgement.suggested_action.as_ref() {
            println!("  suggested_action: {}", action.kind);
            println!("    {}", action.detail);
            println!("    compare: {}", action.compare_command_template);
            println!("    ack:     {}", action.ack_command_template);
        }
    }
}

fn print_wiki_review_after_report(report: &WikiReviewAfterReport) {
    println!("Adaptive wiki review_after attention report");
    println!(
        "  scoped_promoted: {}  with_review_after: {}  missing_review_after: {}",
        report.summary.scoped_promoted,
        report.summary.with_review_after,
        report.summary.missing_review_after
    );
    println!(
        "  expired: {}  near_expiry: {}  attention: {}",
        report.summary.expired, report.summary.near_expiry, report.summary.attention
    );
    if report.entries.is_empty() {
        println!("No promoted adaptive wiki entries need review_after attention.");
        return;
    }
    println!(
        "{:<40} {:<12} {:<28} {:<14} SCOPE",
        "ID", "STATUS", "REVIEW_AFTER", "HOURS_LEFT"
    );
    for entry in &report.entries {
        println!(
            "{:<40} {:<12} {:<28} {:<14} {}",
            entry.id,
            entry.status,
            entry.review_after,
            entry.hours_until_review,
            wiki_scope_label(entry.scope, &entry.scope_ref)
        );
    }
}

fn runtime_ack_scope_mode_label(mode: AdaptiveWikiRuntimePolicyAckScopeMode) -> &'static str {
    match mode {
        AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery => "exact_query",
        AdaptiveWikiRuntimePolicyAckScopeMode::ProjectArtifact => "project_artifact",
    }
}

fn runtime_ack_query_label(query: &AdaptiveWikiQuery) -> String {
    let session = query.session_id.as_deref().unwrap_or("-");
    let project = query.project_key.as_deref().unwrap_or("-");
    let artifact = query.artifact_kind.as_deref().unwrap_or("-");
    let agent_mode = query
        .agent_mode
        .map(adaptive_wiki_agent_mode_cli_value)
        .unwrap_or("-");
    format!("s:{session} p:{project} a:{artifact} m:{agent_mode}")
}

fn print_wiki_markdown_export(report: &AdaptiveWikiMarkdownExportReport) {
    let action = if report.dry_run { "planned" } else { "wrote" };
    println!(
        "Adaptive wiki markdown export {} {} files to {}",
        action, report.summary.files_planned, report.output_dir
    );
    println!(
        "  status: {:?}  reexport_recommended={}",
        report.projection_status.state, report.projection_status.reexport_recommended
    );
    println!(
        "  entries: {}  candidates: {}",
        report.summary.entries_exported, report.summary.candidates_exported
    );
    for file in &report.files {
        println!(
            "  - {} ({} bytes, sha256:{})",
            file.path, file.bytes, file.sha256
        );
    }
}

fn print_wiki_graph_report(
    report: &AdaptiveWikiGraphReport,
    output: Option<&Path>,
    dry_run: bool,
    files: usize,
) {
    println!(
        "Adaptive wiki tag graph: {} nodes, {} edges, {} review issues",
        report.nodes.len(),
        report.edges.len(),
        report.review_issues.len()
    );
    println!(
        "  entries: {}  candidates: {}  tag_nodes: {}",
        report.summary.entries, report.summary.candidates, report.summary.tag_nodes
    );
    println!(
        "  tag_edges: derived_core={} core={} proposed={}",
        report.summary.derived_core_tag_edges,
        report.summary.core_tag_edges,
        report.summary.proposed_tag_edges
    );
    if let Some(output) = output {
        let action = if dry_run { "planned" } else { "wrote" };
        println!(
            "  export: {} {} files to {}",
            action,
            files,
            output.display()
        );
    }
    for issue in report.review_issues.iter().take(8) {
        println!(
            "  - {:?} {}:{} #{} {}",
            issue.severity, issue.subject_kind, issue.subject_id, issue.tag, issue.code
        );
    }
}

fn print_wiki_review_report(report: &AdaptiveWikiReviewReport) {
    let action = if report.dry_run { "planned" } else { "wrote" };
    println!(
        "Adaptive wiki review report {} {} proposals ({} open, {} filtered out) at {}",
        action,
        report.summary.proposals,
        report.summary.open_proposals,
        report.summary.filtered_out_proposals,
        report.report_dir
    );
    println!(
        "  checked: {} entries, {} candidates, {} usage records, {} audit records, {} correction records, {} review events",
        report.summary.entries_checked,
        report.summary.candidates_checked,
        report.summary.usage_records_checked,
        report.summary.audit_records_checked,
        report.summary.correction_records_checked,
        report.summary.review_events_checked
    );
    println!(
        "  lint: {} errors, {} warnings, {} info",
        report.summary.lint_errors, report.summary.lint_warnings, report.summary.lint_info
    );
    println!(
        "  lifecycle: {} with events, {} accepted, {} rejected, {} superseded",
        report.summary.proposals_with_events,
        report.summary.accepted_proposals,
        report.summary.rejected_proposals,
        report.summary.superseded_proposals
    );
    println!(
        "  promotion receipts: {} checked, {} invalid files, {} promoted entries covered, {} missing receipts",
        report.summary.promotion_receipts_checked,
        report.summary.promotion_receipt_files_invalid,
        report.summary.promoted_entries_with_promotion_receipt,
        report.summary.promoted_entries_missing_promotion_receipt
    );
    if report.summary.stale_decision_proposals > 0 {
        println!(
            "  stale decisions: {} need renewed review",
            report.summary.stale_decision_proposals
        );
    }
    for proposal in &report.proposals {
        let lifecycle = proposal
            .lifecycle
            .as_ref()
            .map(|lifecycle| {
                let stale = if lifecycle.stale { ", stale" } else { "" };
                format!(
                    "{:?} by {}{}",
                    lifecycle.decision,
                    empty_dash(&lifecycle.actor),
                    stale
                )
            })
            .unwrap_or_else(|| "Open".to_string());
        println!(
            "  - {:?} {} {} ({:?}, {}): {}",
            proposal.action,
            proposal.subject_kind,
            proposal.subject_id,
            proposal.risk,
            lifecycle,
            proposal.title
        );
        if let Some(lifecycle) = proposal.lifecycle.as_ref() {
            println!(
                "    lifecycle: event={} at={} reason={}",
                lifecycle.latest_event_id,
                lifecycle.decided_at.to_rfc3339(),
                empty_dash(&lifecycle.reason)
            );
            if !lifecycle.stale_evidence_refs.is_empty() {
                println!(
                    "    stale evidence: {}",
                    lifecycle.stale_evidence_refs.join(", ")
                );
            }
        }
        if let Some(command) = proposal.suggested_command.as_deref() {
            println!("    command: {command}");
        }
        println!("    evidence: {}", proposal.evidence_refs.join(", "));
    }
}

fn print_wiki_episode_evaluation_report(report: &AdaptiveWikiEpisodeEvaluationReport) {
    let action = if report.dry_run { "planned" } else { "wrote" };
    let status = if report.passed { "passed" } else { "failed" };
    println!(
        "Adaptive wiki episode evaluation {action} at {} ({status})",
        report.report_dir
    );
    println!("  target: {}", report.target_entry_id);
    println!(
        "  in-scope: {} entries  out-of-scope: {} entries",
        report.summary.in_scope_projection_count, report.summary.out_of_scope_projection_count
    );
    println!(
        "  checks: target_in_scope={} target_out_of_scope={} scope_leakage={} review_expired_projected={} deprecated_projected={} projected_without_evidence={}",
        report.summary.target_entry_in_scope,
        report.summary.target_entry_out_of_scope,
        report.summary.scope_leakage_count,
        report.summary.review_expired_entry_projected,
        report.summary.deprecated_entry_projected,
        report.summary.projected_without_evidence
    );
    if report.failures.is_empty() {
        println!("  failures: none");
    } else {
        println!("  failures:");
        for failure in &report.failures {
            println!("    - {failure}");
        }
    }
    if !report.in_scope_projection.is_empty() {
        println!(
            "  in-scope ids: {}",
            report
                .in_scope_projection
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    if !report.out_of_scope_projection.is_empty() {
        println!(
            "  out-of-scope ids: {}",
            report
                .out_of_scope_projection
                .iter()
                .map(|entry| entry.id.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
}

fn print_wiki_live_episode_trace_report(report: &AdaptiveWikiLiveEpisodeTraceReport) {
    let action = if report.dry_run { "planned" } else { "wrote" };
    println!(
        "Adaptive wiki live episode trace {action} {} events at {}",
        report.summary.events, report.report_dir
    );
    println!(
        "  tasks: {}  runtime usage: {}  projections: {}  candidates: {}  corrections: {}",
        report.summary.task_events,
        report.summary.runtime_usage_events,
        report.summary.projection_events,
        report.summary.candidate_events,
        report.summary.correction_events
    );
    println!(
        "  promotions: {}  completed: {}  failed: {}  resume pending: {}  rollbacks: {}",
        report.summary.promotion_events,
        report.summary.completion_events,
        report.summary.failure_events,
        report.summary.resume_pending_events,
        report.summary.rollback_events
    );
    if report.summary.usage_without_task > 0 {
        println!(
            "  usage without matching task: {}",
            report.summary.usage_without_task
        );
    }
    for event in &report.events {
        println!(
            "  - {:?} {} task={} request={} entries={} {}",
            event.kind,
            event.occurred_at.to_rfc3339(),
            event.task_id.as_deref().unwrap_or("-"),
            event.request_id.as_deref().unwrap_or("-"),
            event.entry_ids.join(","),
            event.summary
        );
    }
}

fn print_wiki_correction_recurrence_report(report: &AdaptiveWikiCorrectionRecurrenceReport) {
    let action = if report.dry_run { "planned" } else { "wrote" };
    println!(
        "Adaptive wiki correction recurrence {action} at {} ({:?})",
        report.report_dir, report.assessment
    );
    println!("  entry: {}", report.entry_id);
    if let Some(scope) = &report.scope {
        println!(
            "  scope: {}",
            wiki_scope_label(scope.scope, &scope.scope_ref)
        );
    }
    if let Some(promotion_at) = report.promotion_at {
        println!("  promotion: {}", promotion_at.to_rfc3339());
    }
    println!(
        "  corrections: pre={} post={} delta={}",
        report.summary.pre_promotion_correction_events,
        report.summary.post_promotion_correction_events,
        report.summary.recurrence_delta
    );
    println!(
        "  post usage={} failures={} counterexamples={} recurrence_per_1000={}",
        report.summary.post_promotion_usage_events,
        report.summary.post_promotion_failure_events,
        report.summary.post_promotion_counterexample_events,
        report.summary.post_promotion_recurrence_per_1000
    );
    if !report.failures.is_empty() {
        println!("  failures:");
        for failure in &report.failures {
            println!("    - {failure}");
        }
    }
}

fn print_wiki_promotion_chain_report(report: &AdaptiveWikiPromotionEvidenceChainReport) {
    let action = if report.dry_run { "planned" } else { "wrote" };
    println!(
        "Adaptive wiki promotion evidence chain {action} at {}",
        report.report_dir
    );
    println!("  entry: {}", report.entry_id);
    println!(
        "  promotion audit={} candidate snapshot={} entry snapshot={} current entry={}",
        report.summary.promotion_audit_found,
        report.summary.candidate_snapshot_present,
        report.summary.entry_snapshot_present,
        report.summary.current_entry_present
    );
    println!(
        "  usage records={} related audits={} failures={}",
        report.summary.usage_records, report.summary.related_audit_records, report.summary.failures
    );
    if let Some(audit) = &report.promotion_audit {
        println!(
            "  promoted at {} candidate={} actor={}",
            audit.created_at.to_rfc3339(),
            audit.candidate_id.as_deref().unwrap_or("-"),
            audit.actor
        );
    }
    if !report.failures.is_empty() {
        println!("  failures:");
        for failure in &report.failures {
            println!("    - {failure}");
        }
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

fn print_deck_report(report: &OffdeskDeckReport) {
    println!("Offdesk Marp deck");
    println!("  generated_at: {}", report.generated_at);
    println!("  source_kind:  {}", report.source_kind);
    println!(
        "  source:       {}",
        operator_safe_report(&report.source_path).text
    );
    println!(
        "  markdown:     {}",
        operator_safe_report(&report.marp_markdown_path).text
    );
    println!("  render:       {}", report.render_status);
    if let Some(path) = report.rendered_path.as_deref() {
        println!("  rendered:     {}", operator_safe_report(path).text);
    }
    println!("  authority:    {}", report.source_of_truth);
}

fn print_resume_states(states: &[TaskResumeState]) {
    let now = Utc::now();
    println!(
        "{:<24} {:<16} {:<8} {:<18} NEXT STEP",
        "TASK", "STATUS", "FRESH", "RUNNER"
    );
    for state in states {
        let fresh = if state.status == ResumeStatus::ResumePending {
            if state.is_fresh_at(now) {
                "fresh"
            } else {
                "stale"
            }
        } else {
            "-"
        };
        println!(
            "{:<24} {:<16} {:<8} {:<18} {}",
            state.task_id,
            format!("{:?}", state.status).to_lowercase(),
            fresh,
            state.runner_target,
            state.next_safe_resume_step
        );
        println!("  resume_id: {}", state.resume_id());
        for evidence in state.evidence.iter().take(3) {
            let present = evidence
                .present
                .map(|present| if present { "present" } else { "missing" });
            match (evidence.path.as_deref(), present) {
                (Some(path), Some(present)) => {
                    println!(
                        "  evidence: {}: {} ({present}, {path})",
                        evidence.kind, evidence.summary
                    );
                }
                (Some(path), None) => {
                    println!(
                        "  evidence: {}: {} ({path})",
                        evidence.kind, evidence.summary
                    );
                }
                _ => println!("  evidence: {}: {}", evidence.kind, evidence.summary),
            }
        }
        if state.evidence.len() > 3 {
            println!("  evidence: +{} more", state.evidence.len() - 3);
        }
        if let Some(tail) = state.last_log_tail.as_deref() {
            println!("  tail: {tail}");
        }
    }
}

fn print_lifecycle_report(report: &OffdeskTaskLifecycleReport, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }

    println!(
        "{} offdesk task {}: {} -> {} ({})",
        if report.changed {
            "Updated"
        } else {
            "Unchanged"
        },
        report.task.task_id,
        status_label(report.previous_status),
        status_label(report.status),
        report.message
    );
    if let Some(ticket_id) = report.task.background_ticket_id.as_deref() {
        println!("  ticket: {}", ticket_id);
    }
    if !report.task.reason.trim().is_empty() {
        println!("  reason: {}", report.task.reason);
    }
    if let Some(error) = report.task.last_error.as_deref() {
        println!("  error:  {}", error);
    }
    Ok(())
}

fn print_retry_lifecycle_report(
    report: &OffdeskTaskLifecycleReport,
    superseded_denied_approvals: usize,
    json: bool,
    include_denied_reset: bool,
) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&RetryTaskLifecycleReport {
                report,
                superseded_denied_approvals,
            })?
        );
        return Ok(());
    }

    print_lifecycle_report(report, false)?;
    if include_denied_reset {
        println!(
            "  superseded denied approvals: {}",
            superseded_denied_approvals
        );
    }
    Ok(())
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
            status_label(task.status),
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

fn status_label(status: OffdeskTaskStatus) -> String {
    match status {
        OffdeskTaskStatus::Queued => "queued",
        OffdeskTaskStatus::PendingApproval => "pending-approval",
        OffdeskTaskStatus::Launched => "launched",
        OffdeskTaskStatus::Running => "running",
        OffdeskTaskStatus::Completed => "completed",
        OffdeskTaskStatus::Failed => "failed",
        OffdeskTaskStatus::ResumePending => "resume-pending",
        OffdeskTaskStatus::Cancelled => "cancelled",
    }
    .to_string()
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
