//! Workstation dashboard surface for the Web UI control plane.

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use super::status::current_status_json_value;
use crate::offdesk::{
    load_offdesk_status_summary, operator_safe_text, DecisionLedger, DecisionRecord,
    DecisionStatus, OffdeskStatusSummary, OffdeskTask, OffdeskTaskStatus, OffdeskTaskStore,
    SchedulerGateStatus,
};
use crate::session::get_profile_dir;

const WORKSTATION_SURFACE_SCHEMA: &str = "workstation_surface.v1";
const DECISION_INBOX_SURFACE_SCHEMA: &str = "decision_inbox_surface.v1";
const CHAT_CONTEXT_SURFACE_SCHEMA: &str = "chat_context_surface.v1";
const ACTION_ENVELOPE_SCHEMA: &str = "action_envelope.v1";
const ACTION_ENVELOPE_RECEIPTS_FILE: &str = "action_envelope_receipts.jsonl";
const DECISION_ACTION_EXECUTIONS_FILE: &str = "decision_action_executions.jsonl";
const RUNTIME_DISPATCH_SURFACE_SCHEMA: &str = "runtime_dispatch_surface.v1";
const ACCEPTED_TRUTH_RECOVERY_SURFACE_SCHEMA: &str = "accepted_truth_recovery_surface.v1";
const ACCEPTED_TRUTH_RECOVERY_ACTION_ENVELOPE_SCHEMA: &str =
    "accepted_truth_recovery_action_envelope.v1";
const ACCEPTED_TRUTH_RECOVERY_ACTION_RECEIPT_SCHEMA: &str =
    "accepted_truth_recovery_action_receipt.v1";
const DECISION_ACTION_CLOSEOUTS_FILE: &str = "decision_action_closeouts.jsonl";
const RUNTIME_DISPATCH_PREFLIGHTS_FILE: &str = "runtime_dispatch_preflights.jsonl";
const RUNTIME_DISPATCH_RECEIPTS_FILE: &str = "runtime_dispatch_receipts.jsonl";
const ACCEPTED_TRUTH_RECOVERY_ACTION_RECEIPTS_FILE: &str =
    "accepted_truth_recovery_action_receipts.jsonl";
const MAX_PROJECT_ROWS: usize = 12;
const MAX_DECISION_ITEMS: usize = 8;
const MAX_RUNTIME_DISPATCH_ITEMS: usize = 6;
const MAX_ACCEPTED_TRUTH_RECOVERY_ITEMS: usize = 6;
const MAX_DECISION_EVIDENCE_REFS: usize = 6;
const MAX_ACTION_ENVELOPES_PER_DECISION: usize = 3;
const MAX_TASK_RECEIPT_LINKS: usize = 4;
const MAX_ACTION_RECEIPT_FAILED_CHECKS: usize = 4;
const MAX_ACTION_EXECUTION_FAILED_CHECKS: usize = 4;
const TELEGRAM_LOOP_STATUS_MAX_AGE_SECONDS: i64 = 180;

#[derive(Args)]
pub struct WorkstationSurfaceArgs {
    /// Emit compact JSON. Without this flag, a human summary is printed.
    #[arg(long)]
    pub json: bool,
}

#[derive(Debug, Serialize)]
struct WorkstationSurface {
    schema: &'static str,
    generated_at: DateTime<Utc>,
    workstation_id: String,
    profile: String,
    source_label: &'static str,
    workspace_roots: Vec<String>,
    health: Vec<HealthItem>,
    capacity: BTreeMap<String, usize>,
    attention_counts: BTreeMap<String, usize>,
    top_attention: TopAttention,
    next_safe_actions: Vec<DashboardAction>,
    projects: Vec<ProjectRow>,
    decision_inbox: DecisionInboxSurface,
    runtime_dispatch: RuntimeDispatchSurface,
    accepted_truth_recovery: AcceptedTruthRecoverySurface,
    chat_context: ChatContextSurface,
    decisions: Vec<DecisionItem>,
    graph_focus: GraphFocus,
    stale_state: StaleState,
    redaction: Redaction,
    source_refs: SourceRefs,
}

#[derive(Debug, Serialize)]
struct HealthItem {
    id: &'static str,
    label: &'static str,
    status: &'static str,
    summary: String,
}

#[derive(Debug, Serialize)]
struct TopAttention {
    kind: String,
    title: String,
    severity: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_key: Option<String>,
    summary: String,
    action_label: String,
}

#[derive(Debug, Serialize)]
struct DashboardAction {
    kind: String,
    label: String,
    reason: String,
    command: String,
    requires_operator_review: bool,
}

#[derive(Debug, Serialize)]
struct ProjectRow {
    project_key: String,
    display_name: String,
    severity: &'static str,
    plan: String,
    runtime: String,
    decisions: usize,
    closeout: String,
    truth: String,
    last_activity: String,
    task_items: Vec<ProjectTaskItem>,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectTaskItem {
    kind: &'static str,
    task_id: String,
    request_id: String,
    title: String,
    status: String,
    capability_id: String,
    runner_kind: &'static str,
    summary: String,
    reference: String,
    command: String,
    updated_at: DateTime<Utc>,
    next_safe_action_kind: String,
    requires_operator_review: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    inspection_items: Vec<ProjectTaskInspectionItem>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    artifact_refs: Vec<ProjectTaskArtifactRef>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    receipt_links: Vec<ProjectTaskReceiptLink>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    log_artifact_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_artifact_path: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectTaskInspectionItem {
    label: &'static str,
    value: String,
    tone: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectTaskArtifactRef {
    artifact_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    present: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectTaskReceiptLink {
    source: &'static str,
    schema: String,
    record_id: String,
    result_status: String,
    recorded_at: DateTime<Utc>,
    summary: String,
}

#[derive(Debug, Clone, Serialize)]
struct DecisionInboxSurface {
    schema: &'static str,
    status: &'static str,
    open_count: usize,
    visible_count: usize,
    summary: String,
    sort_order: Vec<&'static str>,
    items: Vec<DecisionItem>,
    empty_state: DecisionEmptyState,
    action_model: DecisionActionModel,
    source_refs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
struct DecisionEmptyState {
    title: &'static str,
    summary: &'static str,
    cli_fallback: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct DecisionActionModel {
    mode: &'static str,
    max_primary_actions: usize,
    direct_input_allowed: bool,
    mutation_policy: &'static str,
    receipt_policy: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct DecisionItem {
    decision_id: String,
    kind: String,
    severity: &'static str,
    status: String,
    materiality: String,
    raised_by: String,
    project_key: String,
    title: String,
    what_changed: String,
    why_now: String,
    risk: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    evidence_refs: Vec<DecisionEvidenceRef>,
    allowed_actions: Vec<String>,
    action_envelopes: Vec<ActionEnvelopePreview>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    recommendation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_if_no_reply: Option<String>,
    authorization_boundary: String,
    stale_guard: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt_ref: Option<String>,
    cli_fallback: String,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct DecisionEvidenceRef {
    kind: String,
    label: String,
    reference: String,
}

#[derive(Debug, Clone, Serialize)]
struct ActionEnvelopePreview {
    schema: &'static str,
    action_id: String,
    action_kind: String,
    profile: String,
    project_key: String,
    target_ref: ActionTargetRef,
    observed_hash: String,
    nonce: String,
    ttl: &'static str,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    idempotency_key: String,
    preview: String,
    allowed_command: String,
    forbidden_effects: Vec<&'static str>,
    expected_receipt_schema: &'static str,
    requires_confirmation: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    confirmation_phrase: Option<String>,
    stale_rejection_reason: String,
    receipt_history_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_receipt: Option<ActionEnvelopeReceiptSummary>,
    execution_history_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_execution: Option<DecisionActionExecutionSummary>,
}

#[derive(Debug, Clone, Serialize)]
struct ActionTargetRef {
    kind: &'static str,
    decision_id: String,
    status: String,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct ActionEnvelopeReceiptSummary {
    schema: String,
    receipt_id: String,
    processed_at: DateTime<Utc>,
    result_status: String,
    stale: bool,
    reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    current_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    failed_checks: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct StoredActionEnvelopeReceipt {
    schema: String,
    receipt_id: String,
    action_id: String,
    idempotency_key: String,
    processed_at: DateTime<Utc>,
    result_status: String,
    stale: bool,
    reason: String,
    #[serde(default)]
    current_hash: Option<String>,
    #[serde(default)]
    checks: Vec<StoredActionEnvelopeCheck>,
}

#[derive(Debug, Clone, Deserialize)]
struct StoredActionEnvelopeCheck {
    name: String,
    status: String,
}

#[derive(Debug, Clone, Serialize)]
struct DecisionActionExecutionSummary {
    schema: String,
    execution_id: String,
    preflight_id: String,
    executed_at: DateTime<Utc>,
    result_status: String,
    decision: String,
    decision_appended: bool,
    mutation_allowed_by_this_command: bool,
    reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    handoff_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    closeout_command: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    failed_checks: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RuntimeDispatchSurface {
    schema: &'static str,
    status: &'static str,
    candidate_count: usize,
    visible_count: usize,
    summary: String,
    items: Vec<RuntimeDispatchItem>,
    empty_state: RuntimeDispatchEmptyState,
    source_refs: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct RuntimeDispatchEmptyState {
    title: &'static str,
    summary: &'static str,
    cli_fallback: &'static str,
}

#[derive(Debug, Serialize)]
struct RuntimeDispatchItem {
    project_key: String,
    decision_id: String,
    title: String,
    stage: &'static str,
    severity: &'static str,
    closeout_id: String,
    execution_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    receipt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    handoff_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_preflight: Option<RuntimeDispatchPreflightSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_receipt: Option<RuntimeDispatchReceiptSummary>,
    preflight_command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    dispatch_command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tick_command: Option<String>,
    boundary: String,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeDispatchPreflightSummary {
    schema: String,
    preflight_id: String,
    processed_at: DateTime<Utc>,
    result_status: String,
    reason: String,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeDispatchReceiptSummary {
    schema: String,
    receipt_id: String,
    preflight_id: String,
    recorded_at: DateTime<Utc>,
    result_status: String,
    task_id: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct AcceptedTruthRecoverySurface {
    schema: &'static str,
    status: &'static str,
    candidate_count: usize,
    visible_count: usize,
    summary: String,
    items: Vec<AcceptedTruthRecoveryItem>,
    empty_state: AcceptedTruthRecoveryEmptyState,
    source_refs: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct AcceptedTruthRecoveryEmptyState {
    title: &'static str,
    summary: &'static str,
    cli_fallback: &'static str,
}

#[derive(Debug, Serialize)]
struct AcceptedTruthRecoveryItem {
    project_key: String,
    closeout_id: String,
    review_id: String,
    receipt_id: String,
    acceptance_status: String,
    verification_status: String,
    stage: &'static str,
    severity: &'static str,
    open_decision_count: usize,
    open_decision_kinds: Vec<String>,
    evidence_status: String,
    retention_review: String,
    wiki_promotion_state: String,
    stale_task_count: usize,
    next_safe_action: String,
    artifact_dir: String,
    reviewed_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    resolve_command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retire_command: Option<String>,
    action_envelopes: Vec<AcceptedTruthRecoveryActionEnvelopePreview>,
    boundary: String,
}

#[derive(Debug, Serialize)]
struct AcceptedTruthRecoveryActionEnvelopePreview {
    schema: &'static str,
    action_id: String,
    action_kind: String,
    profile: String,
    project_key: String,
    target_ref: AcceptedTruthRecoveryTargetRef,
    observed_hash: String,
    nonce: String,
    ttl: &'static str,
    issued_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
    idempotency_key: String,
    preview: String,
    allowed_command: String,
    forbidden_effects: Vec<&'static str>,
    expected_receipt_schema: &'static str,
    requires_confirmation: bool,
    confirmation_phrase: String,
    stale_rejection_reason: String,
    receipt_history_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    latest_receipt: Option<AcceptedTruthRecoveryActionReceiptSummary>,
}

#[derive(Debug, Serialize, Clone)]
struct AcceptedTruthRecoveryTargetRef {
    kind: &'static str,
    closeout_id: String,
    review_id: String,
    receipt_id: String,
    acceptance_status: String,
    reviewed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
struct AcceptedTruthRecoveryActionReceiptSummary {
    schema: String,
    receipt_id: String,
    processed_at: DateTime<Utc>,
    result_status: String,
    stale: bool,
    reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    current_hash: Option<String>,
}

#[derive(Debug, Serialize)]
struct ChatContextSurface {
    schema: &'static str,
    status: &'static str,
    mode: &'static str,
    summary: String,
    action_policy: &'static str,
    scopes: Vec<ChatContextScope>,
    source_refs: BTreeMap<String, String>,
}

#[derive(Debug, Serialize)]
struct ChatContextScope {
    scope_id: String,
    label: String,
    kind: &'static str,
    answer: String,
    confidence: &'static str,
    citations: Vec<ChatCitation>,
    inference_notes: Vec<String>,
    suggested_actions: Vec<ChatSuggestedAction>,
    prompts: Vec<ChatPrompt>,
}

#[derive(Debug, Serialize)]
struct ChatCitation {
    label: String,
    reference: String,
    trust: &'static str,
}

#[derive(Debug, Serialize)]
struct ChatSuggestedAction {
    label: String,
    kind: &'static str,
    boundary: &'static str,
    target_ref: String,
    command: String,
}

#[derive(Debug, Serialize)]
struct ChatPrompt {
    label: String,
    prompt: String,
}

#[derive(Debug, Clone, Deserialize)]
struct StoredAcceptedTruthRecoveryActionReceipt {
    schema: String,
    receipt_id: String,
    action_id: String,
    idempotency_key: String,
    processed_at: DateTime<Utc>,
    result_status: String,
    stale: bool,
    reason: String,
    #[serde(default)]
    current_hash: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct StoredDecisionActionExecution {
    schema: String,
    execution_id: String,
    preflight_id: String,
    action_kind: String,
    project_key: String,
    decision_id: String,
    executed_at: DateTime<Utc>,
    result_status: String,
    decision: String,
    decision_appended: bool,
    mutation_allowed_by_this_command: bool,
    reason: String,
    #[serde(default)]
    handoff_id: Option<String>,
    #[serde(default)]
    checks: Vec<StoredActionEnvelopeCheck>,
}

#[derive(Debug, Clone, Deserialize)]
struct StoredDecisionActionCloseout {
    schema: String,
    closeout_id: String,
    execution_id: String,
    decision: String,
    project_key: String,
    decision_id: String,
    recorded_at: DateTime<Utc>,
    result_status: String,
    mutation_allowed_by_this_command: bool,
    decision_appended: bool,
    #[serde(default)]
    receipt_id: Option<String>,
    #[serde(default)]
    handoff_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct StoredRuntimeDispatchPreflight {
    schema: String,
    preflight_id: String,
    source_closeout_id: String,
    processed_at: DateTime<Utc>,
    result_status: String,
    reason: String,
}

#[derive(Debug, Clone, Deserialize)]
struct StoredRuntimeDispatchReceipt {
    schema: String,
    receipt_id: String,
    preflight_id: String,
    source_closeout_id: String,
    task_id: String,
    recorded_at: DateTime<Utc>,
    result_status: String,
    reason: String,
}

#[derive(Debug, Serialize)]
struct GraphFocus {
    title: &'static str,
    nodes: Vec<GraphNode>,
    edges: Vec<GraphEdge>,
}

#[derive(Debug, Serialize)]
struct GraphNode {
    id: &'static str,
    label: &'static str,
    kind: &'static str,
}

#[derive(Debug, Serialize)]
struct GraphEdge {
    from: &'static str,
    to: &'static str,
    label: &'static str,
}

#[derive(Debug, Serialize)]
struct StaleState {
    status: &'static str,
    last_seen_at: DateTime<Utc>,
    summary: String,
}

#[derive(Debug, Serialize)]
struct Redaction {
    operator_safe: bool,
    path_policy: &'static str,
}

#[derive(Debug, Serialize)]
struct SourceRefs {
    status_json: &'static str,
    offdesk_status_summary: &'static str,
    task_store: &'static str,
    decision_ledger: &'static str,
    action_envelope_receipts: &'static str,
    decision_action_executions: &'static str,
    decision_action_closeouts: &'static str,
    runtime_dispatch_preflights: &'static str,
    runtime_dispatch_receipts: &'static str,
    accepted_truth_recovery_action_receipts: &'static str,
    telegram_loop_status: &'static str,
}

#[derive(Default)]
struct ProjectAccumulator {
    project_key: String,
    queued: usize,
    pending_approval: usize,
    active: usize,
    completed: usize,
    failed: usize,
    resume_pending: usize,
    cancelled: usize,
    open_decisions: usize,
    last_activity: Option<DateTime<Utc>>,
    task_items: Vec<ProjectTaskItem>,
}

struct TaskReceiptContext<'a> {
    profile: &'a str,
    generated_at: DateTime<Utc>,
    decisions: &'a [DecisionRecord],
    action_receipts: &'a [StoredActionEnvelopeReceipt],
    action_executions: &'a [StoredDecisionActionExecution],
    action_closeouts: &'a [StoredDecisionActionCloseout],
    runtime_receipts: &'a [StoredRuntimeDispatchReceipt],
}

pub async fn run(profile: &str, args: WorkstationSurfaceArgs) -> Result<()> {
    let surface = build_workstation_surface(profile)?;
    if args.json {
        println!("{}", serde_json::to_string(&surface)?);
    } else {
        let value = serde_json::to_value(&surface)?;
        print!("{}", human_summary_from_value(&value));
    }
    Ok(())
}

fn build_workstation_surface(profile: &str) -> Result<WorkstationSurface> {
    let generated_at = Utc::now();
    let profile_dir = get_profile_dir(profile)?;
    let status_json = current_status_json_value(profile)?;
    let offdesk_summary = load_offdesk_status_summary(&profile_dir, generated_at)
        .unwrap_or_else(|_| OffdeskStatusSummary::default());
    let tasks = OffdeskTaskStore::new(&profile_dir)
        .load()
        .unwrap_or_default();
    let decisions = DecisionLedger::new(&profile_dir).load().unwrap_or_default();
    let latest_decisions = latest_decision_records(&decisions);
    let action_receipts = load_action_envelope_receipts(&profile_dir).unwrap_or_default();
    let action_executions = load_decision_action_executions(&profile_dir).unwrap_or_default();
    let action_closeouts = load_decision_action_closeouts(&profile_dir).unwrap_or_default();
    let runtime_preflights = load_runtime_dispatch_preflights(&profile_dir).unwrap_or_default();
    let runtime_receipts = load_runtime_dispatch_receipts(&profile_dir).unwrap_or_default();
    let recovery_action_receipts =
        load_accepted_truth_recovery_action_receipts(&profile_dir).unwrap_or_default();
    let open_decisions = latest_decisions
        .iter()
        .filter(|record| decision_is_open(record.status))
        .collect::<Vec<_>>();
    let next_safe_actions = dashboard_actions_from_status(&status_json);
    let decision_items = decision_items(
        &open_decisions,
        profile,
        generated_at,
        &action_receipts,
        &action_executions,
    );
    let top_attention = build_top_attention(
        &status_json,
        &offdesk_summary,
        open_decisions.len(),
        next_safe_actions.first(),
    );
    let runtime_dispatch = runtime_dispatch_surface(
        &latest_decisions,
        &action_closeouts,
        &runtime_preflights,
        &runtime_receipts,
    );
    let accepted_truth_recovery = accepted_truth_recovery_surface(
        &profile_dir,
        profile,
        generated_at,
        &recovery_action_receipts,
    )
    .unwrap_or_else(|_| accepted_truth_recovery_surface_from_items(Vec::new()));
    let projects = project_rows(
        &tasks,
        &latest_decisions,
        &TaskReceiptContext {
            profile,
            generated_at,
            decisions: &latest_decisions,
            action_receipts: &action_receipts,
            action_executions: &action_executions,
            action_closeouts: &action_closeouts,
            runtime_receipts: &runtime_receipts,
        },
    );
    let decision_inbox = decision_inbox_surface(decision_items.clone(), open_decisions.len());
    let chat_context = chat_context_surface(
        &top_attention,
        &decision_inbox,
        &runtime_dispatch,
        &accepted_truth_recovery,
        &projects,
    );

    Ok(WorkstationSurface {
        schema: WORKSTATION_SURFACE_SCHEMA,
        generated_at,
        workstation_id: workstation_id(),
        profile: operator_safe_text(profile),
        source_label: "Live workstation_surface.v1",
        workspace_roots: workspace_roots(&tasks),
        health: health_items(&profile_dir, &status_json, &offdesk_summary, generated_at),
        capacity: capacity_counts(&status_json),
        attention_counts: attention_counts(&status_json, open_decisions.len()),
        top_attention,
        next_safe_actions: ensure_dashboard_actions(next_safe_actions),
        projects,
        decision_inbox,
        runtime_dispatch,
        accepted_truth_recovery,
        chat_context,
        decisions: decision_items,
        graph_focus: graph_focus(),
        stale_state: StaleState {
            status: "fresh",
            last_seen_at: generated_at,
            summary: "Surface was generated from current local Forager state.".to_string(),
        },
        redaction: Redaction {
            operator_safe: true,
            path_policy: "summary_first",
        },
        source_refs: SourceRefs {
            status_json: "forager status --json",
            offdesk_status_summary: "load_offdesk_status_summary",
            task_store: "offdesk_tasks.json",
            decision_ledger: "offdesk_decisions.jsonl",
            action_envelope_receipts: "action_envelope_receipts.jsonl",
            decision_action_executions: "decision_action_executions.jsonl",
            decision_action_closeouts: "decision_action_closeouts.jsonl",
            runtime_dispatch_preflights: "runtime_dispatch_preflights.jsonl",
            runtime_dispatch_receipts: "runtime_dispatch_receipts.jsonl",
            accepted_truth_recovery_action_receipts:
                "accepted_truth_recovery_action_receipts.jsonl",
            telegram_loop_status: "~/.cache/forager/remote_operator_telegram_loop.json",
        },
    })
}

pub(crate) fn accepted_truth_recovery_surface_value(profile: &str) -> Result<Value> {
    let profile_dir = get_profile_dir(profile)?;
    let action_receipts =
        load_accepted_truth_recovery_action_receipts(&profile_dir).unwrap_or_default();
    let surface =
        accepted_truth_recovery_surface(&profile_dir, profile, Utc::now(), &action_receipts)?;
    Ok(serde_json::to_value(surface)?)
}

fn latest_decision_records(records: &[DecisionRecord]) -> Vec<DecisionRecord> {
    let mut by_decision_id = BTreeMap::<String, DecisionRecord>::new();
    for record in records {
        by_decision_id.insert(record.decision_id.clone(), record.clone());
    }
    by_decision_id.into_values().collect()
}

fn human_summary_from_value(surface: &Value) -> String {
    let mut output = String::new();
    output.push_str("Workstation Surface\n");
    push_summary_line(&mut output, "profile", text_at(surface, "/profile"));
    push_summary_line(
        &mut output,
        "workstation",
        text_at(surface, "/workstation_id"),
    );
    let severity = text_at(surface, "/top_attention/severity").unwrap_or("unknown");
    let title = text_at(surface, "/top_attention/title").unwrap_or("No top attention");
    output.push_str(&format!("  top attention: {title} ({severity})\n"));
    if let Some(summary) = text_at(surface, "/top_attention/summary") {
        output.push_str(&format!("  summary: {summary}\n"));
    }
    output.push_str(&format!(
        "  pending decisions: {}\n",
        number_at(surface, "/attention_counts/pending_decisions").unwrap_or_default()
    ));
    output.push_str(&format!(
        "  failed tasks: {}\n",
        number_at(surface, "/attention_counts/failed_tasks").unwrap_or_default()
    ));
    output.push_str(&format!(
        "  closeout required: {}\n",
        number_at(surface, "/attention_counts/closeout_required").unwrap_or_default()
    ));
    output.push_str(&format!(
        "  runtime handoffs: {}\n",
        number_at(surface, "/runtime_dispatch/candidate_count").unwrap_or_default()
    ));
    output.push_str(&format!(
        "  truth recovery: {}\n",
        number_at(surface, "/accepted_truth_recovery/candidate_count").unwrap_or_default()
    ));
    if let Some(command) = text_at(surface, "/next_safe_actions/0/command") {
        if !command.is_empty() {
            output.push_str(&format!("  next command: {command}\n"));
        }
    }
    output.push_str("  refs: use --json for full dashboard read model\n");
    output
}

fn build_top_attention(
    status_json: &Value,
    summary: &OffdeskStatusSummary,
    open_decisions: usize,
    next_action: Option<&DashboardAction>,
) -> TopAttention {
    if value_usize(status_json, "failed_offdesk_tasks") > 0 || summary.background_failed > 0 {
        return TopAttention {
            kind: "runtime_failure".to_string(),
            title: "Runtime failure needs review".to_string(),
            severity: "critical",
            project_key: None,
            summary: "A task or background runner failed. Inspect evidence before retrying."
                .to_string(),
            action_label: "Open recovery evidence".to_string(),
        };
    }
    if open_decisions > 0 {
        return TopAttention {
            kind: "decision_inbox".to_string(),
            title: "Decision inbox has open items".to_string(),
            severity: "attention",
            project_key: None,
            summary: format!("{open_decisions} decision item(s) require operator review."),
            action_label: "Open decision inbox".to_string(),
        };
    }
    if summary.closeout_required > 0 {
        return TopAttention {
            kind: "closeout_required".to_string(),
            title: "Closeout review required".to_string(),
            severity: "attention",
            project_key: None,
            summary: format!(
                "{} completed task(s) still need closeout or closeout review.",
                summary.closeout_required
            ),
            action_label: "Open review".to_string(),
        };
    }
    if let Some(action) = next_action {
        return TopAttention {
            kind: action.kind.clone(),
            title: label_from_kind(&action.kind),
            severity: if action.requires_operator_review {
                "attention"
            } else {
                "info"
            },
            project_key: None,
            summary: action.reason.clone(),
            action_label: "Open review".to_string(),
        };
    }
    if value_usize(status_json, "running") + value_usize(status_json, "active_offdesk_tasks") > 0 {
        return TopAttention {
            kind: "active_work".to_string(),
            title: "Work is active".to_string(),
            severity: "info",
            project_key: None,
            summary: "Forager has active work, with no blocking operator decision visible."
                .to_string(),
            action_label: "Inspect active work".to_string(),
        };
    }
    TopAttention {
        kind: "clear".to_string(),
        title: "No blocking attention item".to_string(),
        severity: "ok",
        project_key: None,
        summary: "No blocking Forager action is currently visible.".to_string(),
        action_label: "Open review".to_string(),
    }
}

fn health_items(
    profile_dir: &Path,
    status_json: &Value,
    summary: &OffdeskStatusSummary,
    now: DateTime<Utc>,
) -> Vec<HealthItem> {
    vec![
        telegram_health(now),
        HealthItem {
            id: "runtime_store",
            label: "Runtime store",
            status: if profile_dir.exists() {
                "ok"
            } else {
                "unknown"
            },
            summary: format!(
                "Profile store is readable; {} queued, {} active, {} failed Offdesk task(s).",
                value_usize(status_json, "queued_offdesk_tasks"),
                value_usize(status_json, "active_offdesk_tasks")
                    + value_usize(status_json, "offdesk_tasks_pending_approval"),
                value_usize(status_json, "failed_offdesk_tasks")
            ),
        },
        HealthItem {
            id: "closeout",
            label: "Closeout state",
            status: if summary.closeout_required > 0 {
                "attention"
            } else {
                "ok"
            },
            summary: format!(
                "{} required, {} accepted, {} approved with follow-ups.",
                summary.closeout_required,
                summary.closeout_state.accepted,
                summary.closeout_state.approved_with_followups
            ),
        },
        HealthItem {
            id: "local_llm",
            label: "Local LLM",
            status: "unknown",
            summary: "The dashboard surface does not actively probe local model endpoints yet."
                .to_string(),
        },
    ]
}

fn telegram_health(now: DateTime<Utc>) -> HealthItem {
    let Some(path) = telegram_loop_status_path() else {
        return HealthItem {
            id: "telegram",
            label: "Telegram operator",
            status: "unknown",
            summary: "Home cache path is unavailable; listener status cannot be checked."
                .to_string(),
        };
    };
    if !path.exists() {
        return HealthItem {
            id: "telegram",
            label: "Telegram operator",
            status: "unknown",
            summary: "No listener loop-status file is present.".to_string(),
        };
    }
    let Ok(value) = fs::read_to_string(&path)
        .ok()
        .and_then(|content| serde_json::from_str::<Value>(&content).ok())
        .ok_or(())
    else {
        return HealthItem {
            id: "telegram",
            label: "Telegram operator",
            status: "attention",
            summary: "Listener loop-status file is present but unreadable.".to_string(),
        };
    };
    let listener_status = text_field(&value, "status").unwrap_or_else(|| "unknown".to_string());
    let last_poll_at = value
        .pointer("/last_result/generated_at")
        .and_then(Value::as_str)
        .or_else(|| value.get("generated_at").and_then(Value::as_str))
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|value| value.with_timezone(&Utc));
    let stale = last_poll_at
        .map(|seen| (now - seen).num_seconds() > TELEGRAM_LOOP_STATUS_MAX_AGE_SECONDS)
        .unwrap_or(true);
    let polling = matches!(listener_status.as_str(), "polling" | "max_polls_reached");
    let status = if polling && !stale {
        "ok"
    } else if polling {
        "attention"
    } else {
        "blocked"
    };
    HealthItem {
        id: "telegram",
        label: "Telegram operator",
        status,
        summary: format!("Listener status is {listener_status}; stale={stale}."),
    }
}

fn capacity_counts(status_json: &Value) -> BTreeMap<String, usize> {
    BTreeMap::from([
        (
            "active_runners".to_string(),
            value_usize(status_json, "running") + value_usize(status_json, "active_offdesk_tasks"),
        ),
        (
            "queued_tasks".to_string(),
            value_usize(status_json, "queued_offdesk_tasks"),
        ),
        ("provider_deferred".to_string(), 0),
        ("budget_warnings".to_string(), 0),
    ])
}

fn attention_counts(status_json: &Value, open_decisions: usize) -> BTreeMap<String, usize> {
    BTreeMap::from([
        (
            "pending_approvals".to_string(),
            value_usize(status_json, "pending_approvals"),
        ),
        ("pending_decisions".to_string(), open_decisions),
        (
            "blocked_tasks".to_string(),
            value_usize(status_json, "resume_pending_offdesk_tasks")
                + value_usize(status_json, "stale_background_runs"),
        ),
        (
            "failed_tasks".to_string(),
            value_usize(status_json, "failed_offdesk_tasks")
                + value_usize(status_json, "failed_background_runs"),
        ),
        (
            "closeout_required".to_string(),
            value_usize(status_json, "closeout_required_offdesk_tasks"),
        ),
        (
            "accepted_truth_missing".to_string(),
            value_usize(status_json, "closeout_required_offdesk_tasks"),
        ),
        ("followup_decisions_required".to_string(), open_decisions),
    ])
}

fn dashboard_actions_from_status(status_json: &Value) -> Vec<DashboardAction> {
    status_json
        .get("offdesk_next_safe_actions")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|actions| actions.iter())
        .map(|action| {
            let kind = text_field(action, "kind").unwrap_or_else(|| "review".to_string());
            let reason = text_field(action, "detail")
                .map(|value| operator_safe_text(&value))
                .unwrap_or_else(|| "Inspect current state before taking action.".to_string());
            let command = action
                .get("commands")
                .and_then(Value::as_array)
                .and_then(|commands| commands.first())
                .and_then(Value::as_str)
                .map(operator_safe_text)
                .unwrap_or_default();
            DashboardAction {
                label: label_from_kind(&kind),
                kind,
                reason,
                command,
                requires_operator_review: action
                    .get("requires_operator_review")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            }
        })
        .collect()
}

fn ensure_dashboard_actions(actions: Vec<DashboardAction>) -> Vec<DashboardAction> {
    if actions.is_empty() {
        vec![DashboardAction {
            kind: "review_dashboard".to_string(),
            label: "Review dashboard".to_string(),
            reason: "No blocking next safe action was reported; inspect the dashboard before starting new work.".to_string(),
            command: "forager status --json".to_string(),
            requires_operator_review: false,
        }]
    } else {
        actions
    }
}

fn project_rows(
    tasks: &[OffdeskTask],
    decisions: &[DecisionRecord],
    receipt_context: &TaskReceiptContext<'_>,
) -> Vec<ProjectRow> {
    let mut projects = BTreeMap::<String, ProjectAccumulator>::new();
    for task in tasks {
        let key = operator_safe_text(&task.project_key);
        let project = projects
            .entry(key.clone())
            .or_insert_with(|| ProjectAccumulator {
                project_key: key,
                ..ProjectAccumulator::default()
            });
        match task.status {
            OffdeskTaskStatus::Queued => project.queued += 1,
            OffdeskTaskStatus::PendingApproval => project.pending_approval += 1,
            OffdeskTaskStatus::Launched | OffdeskTaskStatus::Running => project.active += 1,
            OffdeskTaskStatus::Completed => project.completed += 1,
            OffdeskTaskStatus::Failed => project.failed += 1,
            OffdeskTaskStatus::ResumePending => project.resume_pending += 1,
            OffdeskTaskStatus::Cancelled => project.cancelled += 1,
        }
        let receipt_links = project_task_receipt_links(task, receipt_context);
        project
            .task_items
            .push(project_task_item(task, receipt_links));
        update_latest(&mut project.last_activity, task.updated_at);
    }
    for record in decisions
        .iter()
        .filter(|record| decision_is_open(record.status))
    {
        let key = operator_safe_text(&record.project_key);
        let project = projects
            .entry(key.clone())
            .or_insert_with(|| ProjectAccumulator {
                project_key: key,
                ..ProjectAccumulator::default()
            });
        project.open_decisions += 1;
        update_latest(&mut project.last_activity, record.updated_at);
    }

    let mut rows = projects
        .into_values()
        .map(project_row)
        .collect::<Vec<ProjectRow>>();
    rows.sort_by_key(|row| {
        (
            severity_rank(row.severity),
            std::cmp::Reverse(row.decisions),
            row.project_key.clone(),
        )
    });
    rows.truncate(MAX_PROJECT_ROWS);
    rows
}

fn project_row(project: ProjectAccumulator) -> ProjectRow {
    let mut task_items = project.task_items;
    task_items.sort_by_key(|item| {
        (
            project_task_status_rank(&item.status),
            std::cmp::Reverse(item.updated_at),
            item.task_id.clone(),
        )
    });
    task_items.truncate(6);

    let severity = if project.failed > 0 {
        "critical"
    } else if project.resume_pending > 0 {
        "blocked"
    } else if project.open_decisions > 0 || project.pending_approval > 0 {
        "attention"
    } else if project.active > 0 || project.queued > 0 {
        "info"
    } else {
        "ok"
    };
    let runtime = if project.failed > 0 {
        "failed"
    } else if project.resume_pending > 0 {
        "resume_pending"
    } else if project.active > 0 {
        "running"
    } else if project.pending_approval > 0 {
        "pending_approval"
    } else if project.queued > 0 {
        "queued"
    } else if project.completed > 0 {
        "completed"
    } else {
        "no_runtime"
    };
    ProjectRow {
        display_name: project.project_key.clone(),
        project_key: project.project_key,
        severity,
        plan: if project.open_decisions > 0 {
            "needs_decision".to_string()
        } else if project.queued + project.active + project.pending_approval > 0 {
            "in_flight".to_string()
        } else {
            "observed".to_string()
        },
        runtime: runtime.to_string(),
        decisions: project.open_decisions,
        closeout: if project.completed > 0 {
            "review_required_if_no_receipt".to_string()
        } else {
            "not_ready".to_string()
        },
        truth: if project.completed > 0 {
            "unknown".to_string()
        } else {
            "not_applicable".to_string()
        },
        last_activity: project
            .last_activity
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string()),
        task_items,
    }
}

fn project_task_item(
    task: &OffdeskTask,
    receipt_links: Vec<ProjectTaskReceiptLink>,
) -> ProjectTaskItem {
    let view = task.operator_view();
    let runner_kind = background_runner_kind_label(task.runner_kind);
    let command = view
        .next_safe_action
        .commands
        .first()
        .cloned()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| view.command.clone());
    let title = if !view.preview.is_empty() {
        view.preview.clone()
    } else if !view.reason.is_empty() {
        view.reason.clone()
    } else {
        format!("{} task", view.capability_id)
    };
    let summary = if !view.reason.is_empty() {
        view.reason.clone()
    } else {
        view.next_safe_action.detail.clone()
    };
    let artifact_count = view.artifact_refs.len();
    let present_artifact_count = view
        .artifact_refs
        .iter()
        .filter(|artifact| artifact.present)
        .count();
    let inspection_items =
        project_task_inspection_items(&view, runner_kind, artifact_count, present_artifact_count);
    let artifact_refs: Vec<ProjectTaskArtifactRef> = view
        .artifact_refs
        .into_iter()
        .map(|artifact| ProjectTaskArtifactRef {
            artifact_id: operator_safe_text(&artifact.artifact_id),
            path: artifact.path.map(|path| operator_safe_text(&path)),
            present: artifact.present,
        })
        .collect();

    ProjectTaskItem {
        kind: project_task_kind(task.status),
        task_id: view.task_id.clone(),
        request_id: view.request_id,
        title,
        status: offdesk_task_status_label(task.status).to_string(),
        capability_id: view.capability_id,
        runner_kind,
        summary,
        reference: format!("offdesk_tasks.json#{}", view.task_id),
        command,
        updated_at: view.updated_at,
        next_safe_action_kind: view.next_safe_action.kind,
        requires_operator_review: view.next_safe_action.requires_operator_review,
        inspection_items,
        artifact_refs,
        receipt_links,
        log_artifact_path: view.log_artifact_path.map(|path| operator_safe_text(&path)),
        result_artifact_path: view
            .result_artifact_path
            .map(|path| operator_safe_text(&path)),
    }
}

fn project_task_receipt_links(
    task: &OffdeskTask,
    context: &TaskReceiptContext<'_>,
) -> Vec<ProjectTaskReceiptLink> {
    let mut links = Vec::new();
    let mut seen = BTreeSet::new();
    for record in context
        .decisions
        .iter()
        .filter(|record| record.project_key == task.project_key && record.task_id == task.task_id)
    {
        for action in decision_action_envelopes(
            record,
            context.profile,
            &decision_allowed_actions(record),
            context.generated_at,
            context.action_receipts,
            context.action_executions,
        ) {
            if let Some(receipt) = action.latest_receipt {
                push_project_task_receipt_link(
                    &mut links,
                    &mut seen,
                    ProjectTaskReceiptLink {
                        source: "action_envelope",
                        schema: receipt.schema,
                        record_id: receipt.receipt_id,
                        result_status: receipt.result_status,
                        recorded_at: receipt.processed_at,
                        summary: receipt.reason,
                    },
                );
            }
            if let Some(execution) = action.latest_execution {
                push_project_task_receipt_link(
                    &mut links,
                    &mut seen,
                    ProjectTaskReceiptLink {
                        source: "decision_execution",
                        schema: execution.schema,
                        record_id: execution.execution_id,
                        result_status: execution.result_status,
                        recorded_at: execution.executed_at,
                        summary: execution.reason,
                    },
                );
            }
        }
        if let Some(closeout) = latest_matching_closeout(record, context.action_closeouts) {
            push_project_task_receipt_link(
                &mut links,
                &mut seen,
                ProjectTaskReceiptLink {
                    source: "decision_closeout",
                    schema: operator_safe_text(&closeout.schema),
                    record_id: operator_safe_text(&closeout.closeout_id),
                    result_status: operator_safe_text(&closeout.result_status),
                    recorded_at: closeout.recorded_at,
                    summary: format!(
                        "{} decision action closeout for {}",
                        operator_safe_text(&closeout.decision),
                        operator_safe_text(&closeout.decision_id)
                    ),
                },
            );
        }
    }
    for receipt in context
        .runtime_receipts
        .iter()
        .filter(|receipt| receipt.task_id == task.task_id)
    {
        push_project_task_receipt_link(
            &mut links,
            &mut seen,
            ProjectTaskReceiptLink {
                source: "runtime_dispatch",
                schema: operator_safe_text(&receipt.schema),
                record_id: operator_safe_text(&receipt.receipt_id),
                result_status: operator_safe_text(&receipt.result_status),
                recorded_at: receipt.recorded_at,
                summary: operator_safe_text(&receipt.reason),
            },
        );
    }
    links.sort_by_key(|link| {
        (
            std::cmp::Reverse(link.recorded_at),
            link.source,
            link.record_id.clone(),
        )
    });
    links.truncate(MAX_TASK_RECEIPT_LINKS);
    links
}

fn push_project_task_receipt_link(
    links: &mut Vec<ProjectTaskReceiptLink>,
    seen: &mut BTreeSet<String>,
    link: ProjectTaskReceiptLink,
) {
    let key = format!("{}:{}:{}", link.source, link.schema, link.record_id);
    if seen.insert(key) {
        links.push(link);
    }
}

fn chat_context_surface(
    top_attention: &TopAttention,
    decision_inbox: &DecisionInboxSurface,
    runtime_dispatch: &RuntimeDispatchSurface,
    accepted_truth_recovery: &AcceptedTruthRecoverySurface,
    projects: &[ProjectRow],
) -> ChatContextSurface {
    let status = if decision_inbox.open_count > 0
        || runtime_dispatch.candidate_count > 0
        || accepted_truth_recovery.candidate_count > 0
    {
        "attention"
    } else {
        "clear"
    };
    let mut scopes = vec![chat_overview_scope(
        top_attention,
        decision_inbox,
        runtime_dispatch,
        accepted_truth_recovery,
    )];
    scopes.extend(decision_inbox.items.iter().map(chat_decision_scope));
    scopes.extend(projects.iter().map(chat_project_scope));

    ChatContextSurface {
        schema: CHAT_CONTEXT_SURFACE_SCHEMA,
        status,
        mode: "read_only_cited_answer",
        summary: format!(
            "{} assistant scope(s) are available; answers cite state or receipt refs and cannot execute actions.",
            scopes.len()
        ),
        action_policy: "Suggested actions are review cards only. Execution still requires action envelopes, preflight checks, and receipts.",
        scopes,
        source_refs: BTreeMap::from([
            (
                "overview".to_string(),
                "workstation_surface.v1#top_attention".to_string(),
            ),
            (
                "decision_inbox".to_string(),
                DECISION_INBOX_SURFACE_SCHEMA.to_string(),
            ),
            (
                "decision_scope".to_string(),
                "chat_context_surface.v1#decision:{decision_id}".to_string(),
            ),
            (
                "runtime_dispatch".to_string(),
                RUNTIME_DISPATCH_SURFACE_SCHEMA.to_string(),
            ),
            (
                "accepted_truth_recovery".to_string(),
                ACCEPTED_TRUTH_RECOVERY_SURFACE_SCHEMA.to_string(),
            ),
        ]),
    }
}

fn chat_overview_scope(
    top_attention: &TopAttention,
    decision_inbox: &DecisionInboxSurface,
    runtime_dispatch: &RuntimeDispatchSurface,
    accepted_truth_recovery: &AcceptedTruthRecoverySurface,
) -> ChatContextScope {
    let answer = if decision_inbox.open_count > 0 {
        format!(
            "{} open decision item(s) need operator attention. Start with \"{}\". This assistant can explain and draft a safe note, but execution still needs action envelopes and receipts.",
            decision_inbox.open_count, top_attention.title
        )
    } else if accepted_truth_recovery.candidate_count > 0 {
        format!(
            "{} accepted-truth recovery item(s) are visible. Review closeout evidence and follow-up state before treating any output as accepted truth.",
            accepted_truth_recovery.candidate_count
        )
    } else if runtime_dispatch.candidate_count > 0 {
        format!(
            "{} runtime handoff candidate(s) are visible. Distinguish queueing, launch, and monitoring before proposing a runtime action.",
            runtime_dispatch.candidate_count
        )
    } else {
        format!(
            "No blocking assistant item is visible. Current top attention is \"{}\"; summarize health and capacity before starting new work.",
            top_attention.title
        )
    };
    let mut citations = vec![
        ChatCitation {
            label: "Top attention".to_string(),
            reference: "workstation_surface.v1#top_attention".to_string(),
            trust: "state-ref",
        },
        ChatCitation {
            label: "Decision inbox".to_string(),
            reference: decision_inbox.schema.to_string(),
            trust: "state-ref",
        },
        ChatCitation {
            label: "Capacity".to_string(),
            reference: "workstation_surface.v1#capacity".to_string(),
            trust: "state-ref",
        },
    ];
    if let Some(decision) = decision_inbox.items.first() {
        citations.push(ChatCitation {
            label: "Leading decision".to_string(),
            reference: format!("decision:{}", decision.decision_id),
            trust: if decision.receipt_ref.is_some() {
                "receipt-backed"
            } else {
                "state-ref"
            },
        });
        if let Some(receipt) = decision
            .action_envelopes
            .iter()
            .find_map(|action| action.latest_receipt.as_ref())
        {
            citations.push(ChatCitation {
                label: "Latest action receipt".to_string(),
                reference: receipt.receipt_id.clone(),
                trust: "receipt-backed",
            });
        }
    }

    ChatContextScope {
        scope_id: "overview".to_string(),
        label: "Workstation".to_string(),
        kind: "overview",
        answer,
        confidence: "surface-derived",
        citations,
        inference_notes: vec![
            "This answer is deterministic surface synthesis, not an external model call."
                .to_string(),
        ],
        suggested_actions: vec![chat_overview_action(
            decision_inbox,
            runtime_dispatch,
            accepted_truth_recovery,
        )],
        prompts: vec![
            ChatPrompt {
                label: "What needs attention?".to_string(),
                prompt:
                    "Summarize the current Forager workstation state from workstation_surface.v1. Cite top_attention, decision_inbox, capacity, and health refs; mark any inference."
                        .to_string(),
            },
            ChatPrompt {
                label: "Explain top item".to_string(),
                prompt: format!(
                    "Explain why \"{}\" needs attention. Include the risk, next safe action, and which receipt or state refs must be checked before action.",
                    top_attention.title
                ),
            },
        ],
    }
}

fn chat_overview_action(
    decision_inbox: &DecisionInboxSurface,
    runtime_dispatch: &RuntimeDispatchSurface,
    accepted_truth_recovery: &AcceptedTruthRecoverySurface,
) -> ChatSuggestedAction {
    if decision_inbox.open_count > 0 {
        return ChatSuggestedAction {
            label: "Open decision inbox".to_string(),
            kind: "open_surface",
            boundary: "Proposal only; no decision is applied from the assistant panel.",
            target_ref: DECISION_INBOX_SURFACE_SCHEMA.to_string(),
            command: "forager offdesk decisions --json".to_string(),
        };
    }
    if accepted_truth_recovery.candidate_count > 0 {
        return ChatSuggestedAction {
            label: "Review truth recovery".to_string(),
            kind: "open_surface",
            boundary:
                "Proposal only; accepted truth requires the recovery action envelope and receipt.",
            target_ref: ACCEPTED_TRUTH_RECOVERY_SURFACE_SCHEMA.to_string(),
            command: "forager ondesk review-surface --json".to_string(),
        };
    }
    if runtime_dispatch.candidate_count > 0 {
        return ChatSuggestedAction {
            label: "Inspect runtime handoffs".to_string(),
            kind: "open_surface",
            boundary: "Proposal only; runtime launch remains outside the assistant panel.",
            target_ref: RUNTIME_DISPATCH_SURFACE_SCHEMA.to_string(),
            command: "forager ondesk workstation-surface --json".to_string(),
        };
    }
    ChatSuggestedAction {
        label: "Review dashboard".to_string(),
        kind: "open_surface",
        boundary: "Proposal only; inspect current state before starting new work.",
        target_ref: "workstation_surface.v1".to_string(),
        command: "forager status --json".to_string(),
    }
}

fn chat_decision_scope(decision: &DecisionItem) -> ChatContextScope {
    let latest_receipt = decision
        .action_envelopes
        .iter()
        .find_map(|action| action.latest_receipt.as_ref());
    let latest_execution = decision
        .action_envelopes
        .iter()
        .find_map(|action| action.latest_execution.as_ref());
    let answer = if latest_receipt.is_some_and(|receipt| receipt.stale) {
        format!(
            "\"{}\" should not advance from the current receipt. The latest action receipt is stale, so refresh the decision state and regenerate the envelope before any execution path.",
            decision.title
        )
    } else if let Some(execution) = latest_execution {
        format!(
            "\"{}\" has latest execution status {}. Verify the execution receipt, closeout or handoff refs, and stale guard before treating the decision as resolved.",
            decision.title, execution.result_status
        )
    } else if let Some(receipt) = latest_receipt {
        format!(
            "\"{}\" has a non-stale action receipt with status {}. It can be discussed as validated preview state, but execution still requires the preflight and receipt path outside this assistant panel.",
            decision.title, receipt.result_status
        )
    } else {
        format!(
            "\"{}\" is still awaiting operator judgment. Explain risk and options from the decision record, then validate an action envelope before any state change.",
            decision.title
        )
    };
    let mut citations = vec![
        ChatCitation {
            label: "Decision".to_string(),
            reference: format!("decision:{}", decision.decision_id),
            trust: "state-ref",
        },
        ChatCitation {
            label: "Decision inbox".to_string(),
            reference: DECISION_INBOX_SURFACE_SCHEMA.to_string(),
            trust: "state-ref",
        },
    ];
    citations.extend(
        decision
            .evidence_refs
            .iter()
            .take(2)
            .map(|evidence| ChatCitation {
                label: format!("Evidence: {}", evidence.label),
                reference: evidence.reference.clone(),
                trust: "state-ref",
            }),
    );
    if let Some(receipt_ref) = &decision.receipt_ref {
        citations.push(ChatCitation {
            label: "Decision receipt".to_string(),
            reference: receipt_ref.clone(),
            trust: "receipt-backed",
        });
    }
    if let Some(receipt) = latest_receipt {
        citations.push(ChatCitation {
            label: "Latest action receipt".to_string(),
            reference: receipt.receipt_id.clone(),
            trust: "receipt-backed",
        });
    }
    if let Some(execution) = latest_execution {
        citations.push(ChatCitation {
            label: "Latest execution".to_string(),
            reference: execution.execution_id.clone(),
            trust: "receipt-backed",
        });
    }

    ChatContextScope {
        scope_id: format!("decision:{}", decision.decision_id),
        label: decision.title.clone(),
        kind: "decision",
        answer,
        confidence: "surface-derived",
        citations,
        inference_notes: vec![
            "Decision answer is synthesized from the decision record, action envelope previews, receipts, and execution summaries only."
                .to_string(),
        ],
        suggested_actions: vec![chat_decision_action(
            decision,
            latest_receipt,
            latest_execution,
        )],
        prompts: vec![
            ChatPrompt {
                label: "Can this advance?".to_string(),
                prompt: format!(
                    "Explain whether \"{}\" can advance. Cite the decision record, stale guard, latest receipt or execution, and mark any inference.",
                    decision.title
                ),
            },
            ChatPrompt {
                label: "Explain risk".to_string(),
                prompt: format!(
                    "Explain the risk and safe next step for decision {} without executing any action.",
                    decision.decision_id
                ),
            },
        ],
    }
}

fn chat_decision_action(
    decision: &DecisionItem,
    latest_receipt: Option<&ActionEnvelopeReceiptSummary>,
    latest_execution: Option<&DecisionActionExecutionSummary>,
) -> ChatSuggestedAction {
    if latest_receipt.is_some_and(|receipt| receipt.stale) {
        return ChatSuggestedAction {
            label: "Refresh decision envelope".to_string(),
            kind: "inspect_state",
            boundary: "Proposal only; stale receipts must be rejected before any execution path.",
            target_ref: format!("decision:{}", decision.decision_id),
            command: decision.cli_fallback.clone(),
        };
    }
    if let Some(execution) = latest_execution {
        return ChatSuggestedAction {
            label: "Review execution receipt".to_string(),
            kind: "inspect_receipt",
            boundary:
                "Proposal only; execution result must be reviewed before closeout or runtime handoff.",
            target_ref: execution.execution_id.clone(),
            command: execution
                .closeout_command
                .clone()
                .unwrap_or_else(|| decision.cli_fallback.clone()),
        };
    }
    if let Some(receipt) = latest_receipt {
        return ChatSuggestedAction {
            label: "Run action preflight".to_string(),
            kind: "inspect_receipt",
            boundary: "Proposal only; preflight and execution remain outside the assistant panel.",
            target_ref: receipt.receipt_id.clone(),
            command: format!(
                "forager ondesk action-preflight --receipt-id {} --json",
                receipt.receipt_id
            ),
        };
    }
    ChatSuggestedAction {
        label: "Validate action envelope".to_string(),
        kind: "inspect_state",
        boundary:
            "Proposal only; validation must produce an action_envelope_receipt.v1 before execution.",
        target_ref: format!("decision:{}", decision.decision_id),
        command: decision
            .action_envelopes
            .first()
            .map(|envelope| envelope.allowed_command.clone())
            .unwrap_or_else(|| decision.cli_fallback.clone()),
    }
}

fn chat_project_scope(project: &ProjectRow) -> ChatContextScope {
    let task_count = project.task_items.len();
    let leading_task = project
        .task_items
        .first()
        .map(|task| format!(" Leading task: {}.", task.title))
        .unwrap_or_default();
    let answer = format!(
        "{} is {}: plan {}, runtime {}, closeout {}, truth {}. {} open decision(s) and {} task row(s) are visible.{} Treat this as read-only context until an action envelope or receipt is reviewed.",
        project.display_name,
        project.severity,
        project.plan,
        project.runtime,
        project.closeout,
        project.truth,
        project.decisions,
        task_count,
        leading_task
    );
    let mut citations = vec![ChatCitation {
        label: "Project state".to_string(),
        reference: format!("workstation_surface.v1#project:{}", project.project_key),
        trust: "state-ref",
    }];
    if let Some(task) = project.task_items.first() {
        citations.push(ChatCitation {
            label: "Leading task".to_string(),
            reference: task.reference.clone(),
            trust: "state-ref",
        });
        if let Some(receipt) = task.receipt_links.first() {
            citations.push(ChatCitation {
                label: "Latest task receipt".to_string(),
                reference: receipt.record_id.clone(),
                trust: "receipt-backed",
            });
        }
    }

    ChatContextScope {
        scope_id: format!("project:{}", project.project_key),
        label: project.display_name.clone(),
        kind: "project",
        answer,
        confidence: "surface-derived",
        citations,
        inference_notes: vec![
            "Project answer is synthesized from project row, task drawer, and receipt links only."
                .to_string(),
        ],
        suggested_actions: vec![chat_project_action(project)],
        prompts: vec![
            ChatPrompt {
                label: if project.decisions > 0 {
                    "Can this advance?".to_string()
                } else {
                    "Summarize state".to_string()
                },
                prompt: format!(
                    "Summarize {} from workstation_surface.v1 and cite project state, task refs, and receipt links before proposing any action.",
                    project.display_name
                ),
            },
            ChatPrompt {
                label: "Show provenance".to_string(),
                prompt: format!("Trace the scoped provenance path for {}.", project.display_name),
            },
        ],
    }
}

fn chat_project_action(project: &ProjectRow) -> ChatSuggestedAction {
    if project.decisions > 0 {
        return ChatSuggestedAction {
            label: "Review project decision".to_string(),
            kind: "open_surface",
            boundary: "Proposal only; decision changes require an action envelope and receipt.",
            target_ref: format!("workstation_surface.v1#project:{}", project.project_key),
            command: "forager offdesk decisions --json".to_string(),
        };
    }
    if let Some(task) = project.task_items.first() {
        return ChatSuggestedAction {
            label: "Inspect task drawer".to_string(),
            kind: "inspect_state",
            boundary:
                "Proposal only; task execution remains controlled by Offdesk commands and receipts.",
            target_ref: task.reference.clone(),
            command: task.command.clone(),
        };
    }
    ChatSuggestedAction {
        label: "Review project state".to_string(),
        kind: "inspect_state",
        boundary: "Proposal only; no mutation is authorized from this assistant panel.",
        target_ref: format!("workstation_surface.v1#project:{}", project.project_key),
        command: "forager status --json".to_string(),
    }
}

fn project_task_inspection_items(
    view: &crate::offdesk::OffdeskTaskView,
    runner_kind: &'static str,
    artifact_count: usize,
    present_artifact_count: usize,
) -> Vec<ProjectTaskInspectionItem> {
    let mut items = vec![
        ProjectTaskInspectionItem {
            label: "Runner",
            value: format!("{runner_kind} / {}", view.capability_id),
            tone: "neutral",
        },
        ProjectTaskInspectionItem {
            label: "Ticket",
            value: view
                .background_ticket_id
                .as_deref()
                .map(operator_safe_text)
                .unwrap_or_else(|| "not launched".to_string()),
            tone: if view.background_ticket_id.is_some() {
                "neutral"
            } else {
                "muted"
            },
        },
        ProjectTaskInspectionItem {
            label: "Attempts",
            value: view.attempt_count.to_string(),
            tone: if view.attempt_count > 0 {
                "attention"
            } else {
                "muted"
            },
        },
        ProjectTaskInspectionItem {
            label: "Gate",
            value: view
                .last_gate_status
                .map(scheduler_gate_status_label)
                .unwrap_or("not gated")
                .to_string(),
            tone: view
                .last_gate_status
                .map(scheduler_gate_status_tone)
                .unwrap_or("muted"),
        },
        ProjectTaskInspectionItem {
            label: "Artifacts",
            value: project_task_artifact_summary(
                artifact_count,
                present_artifact_count,
                view.log_artifact_path.is_some(),
                view.result_artifact_path.is_some(),
            ),
            tone: project_task_artifact_tone(
                artifact_count,
                present_artifact_count,
                view.result_artifact_path.is_some(),
            ),
        },
        ProjectTaskInspectionItem {
            label: "Mode",
            value: format!(
                "{} / {}",
                view.mode_assessment.mode_verdict.label(),
                view.mode_assessment.mode_risk.label()
            ),
            tone: if view.mode_assessment.review_stage_required {
                "attention"
            } else if view.mode_assessment.mode_risk.label() == "none" {
                "success"
            } else {
                "neutral"
            },
        },
    ];

    if let Some(not_before) = view.not_before {
        items.push(ProjectTaskInspectionItem {
            label: "Wait until",
            value: not_before.to_rfc3339(),
            tone: "attention",
        });
    }
    if let Some(provider_id) = &view.provider_id {
        let model = view.model.as_deref().unwrap_or("model unspecified");
        items.push(ProjectTaskInspectionItem {
            label: "Provider",
            value: format!(
                "{} / {}",
                operator_safe_text(provider_id),
                operator_safe_text(model)
            ),
            tone: "neutral",
        });
    }
    if let Some(last_error) = &view.last_error {
        items.push(ProjectTaskInspectionItem {
            label: "Error",
            value: operator_safe_text(last_error),
            tone: "danger",
        });
    }

    items
}

fn project_task_artifact_summary(
    artifact_count: usize,
    present_artifact_count: usize,
    has_log_artifact: bool,
    has_result_artifact: bool,
) -> String {
    let log = if has_log_artifact {
        "log ready"
    } else {
        "log missing"
    };
    let result = if has_result_artifact {
        "result ready"
    } else {
        "result missing"
    };
    format!("{present_artifact_count}/{artifact_count} refs; {log}; {result}")
}

fn project_task_artifact_tone(
    artifact_count: usize,
    present_artifact_count: usize,
    has_result_artifact: bool,
) -> &'static str {
    if has_result_artifact || (artifact_count > 0 && present_artifact_count == artifact_count) {
        "success"
    } else if artifact_count == 0 {
        "muted"
    } else {
        "attention"
    }
}

fn project_task_kind(status: OffdeskTaskStatus) -> &'static str {
    match status {
        OffdeskTaskStatus::PendingApproval => "Approval task",
        OffdeskTaskStatus::Failed | OffdeskTaskStatus::ResumePending => "Recovery task",
        OffdeskTaskStatus::Launched | OffdeskTaskStatus::Running => "Runtime task",
        OffdeskTaskStatus::Completed => "Closeout task",
        OffdeskTaskStatus::Cancelled => "Cancelled task",
        OffdeskTaskStatus::Queued => "Queued task",
    }
}

fn offdesk_task_status_label(status: OffdeskTaskStatus) -> &'static str {
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

fn project_task_status_rank(status: &str) -> u8 {
    match status {
        "failed" | "resume_pending" => 0,
        "pending_approval" => 1,
        "running" | "launched" => 2,
        "queued" => 3,
        "completed" => 4,
        "cancelled" => 5,
        _ => 6,
    }
}

fn scheduler_gate_status_label(status: SchedulerGateStatus) -> &'static str {
    match status {
        SchedulerGateStatus::Proceed => "proceed",
        SchedulerGateStatus::PendingApproval => "pending_approval",
        SchedulerGateStatus::Denied => "denied",
        SchedulerGateStatus::Blocked => "blocked",
    }
}

fn scheduler_gate_status_tone(status: SchedulerGateStatus) -> &'static str {
    match status {
        SchedulerGateStatus::Proceed => "success",
        SchedulerGateStatus::PendingApproval => "attention",
        SchedulerGateStatus::Denied | SchedulerGateStatus::Blocked => "danger",
    }
}

fn background_runner_kind_label(kind: crate::offdesk::BackgroundRunnerKind) -> &'static str {
    match kind {
        crate::offdesk::BackgroundRunnerKind::LocalTmux => "local_tmux",
        crate::offdesk::BackgroundRunnerKind::LocalBackground => "local_background",
        crate::offdesk::BackgroundRunnerKind::GithubRunner => "github_runner",
        crate::offdesk::BackgroundRunnerKind::RemoteWorker => "remote_worker",
    }
}

fn decision_inbox_surface(items: Vec<DecisionItem>, open_count: usize) -> DecisionInboxSurface {
    let visible_count = items.len();
    DecisionInboxSurface {
        schema: DECISION_INBOX_SURFACE_SCHEMA,
        status: if open_count > 0 { "attention" } else { "clear" },
        open_count,
        visible_count,
        summary: if open_count == 0 {
            "No open human decision records are currently visible.".to_string()
        } else if open_count > visible_count {
            format!("{visible_count} of {open_count} open decision records are visible by urgency.")
        } else {
            format!("{open_count} open decision record(s) require operator review.")
        },
        sort_order: vec!["severity", "updated_at_desc", "project_key"],
        items,
        empty_state: DecisionEmptyState {
            title: "No open decisions",
            summary: "The current workstation surface does not report a human decision item.",
            cli_fallback: "forager offdesk decisions --json",
        },
        action_model: DecisionActionModel {
            mode: "read_only_preview",
            max_primary_actions: 3,
            direct_input_allowed: true,
            mutation_policy: "No decision action mutates project files, runtime state, accepted truth, or approval ledgers from this surface.",
            receipt_policy: "Decision action attempts must validate an action envelope and surface an action envelope receipt before any mutation-capable continuation.",
        },
        source_refs: BTreeMap::from([
            (
                "decision_ledger".to_string(),
                "offdesk_decisions.jsonl".to_string(),
            ),
            (
                "cli_fallback".to_string(),
                "forager offdesk decisions --json".to_string(),
            ),
        ]),
    }
}

fn runtime_dispatch_surface(
    decisions: &[DecisionRecord],
    closeouts: &[StoredDecisionActionCloseout],
    preflights: &[StoredRuntimeDispatchPreflight],
    receipts: &[StoredRuntimeDispatchReceipt],
) -> RuntimeDispatchSurface {
    let mut items = decisions
        .iter()
        .filter(|record| record.status == DecisionStatus::Receipted)
        .filter_map(|record| runtime_dispatch_item(record, closeouts, preflights, receipts))
        .collect::<Vec<_>>();
    items.sort_by_key(|item| {
        (
            severity_rank(item.severity),
            runtime_dispatch_stage_rank(item.stage),
            item.project_key.clone(),
            item.decision_id.clone(),
        )
    });
    let candidate_count = items.len();
    let queued_count = items.iter().filter(|item| item.stage == "queued").count();
    let ready_count = items
        .iter()
        .filter(|item| item.stage == "ready_to_queue")
        .count();
    let blocked_count = items
        .iter()
        .filter(|item| item.stage.ends_with("_blocked"))
        .count();
    items.truncate(MAX_RUNTIME_DISPATCH_ITEMS);
    let visible_count = items.len();
    let status = if blocked_count > 0 {
        "blocked"
    } else if ready_count > 0 {
        "attention"
    } else if queued_count > 0 {
        "info"
    } else {
        "clear"
    };
    let summary = if candidate_count == 0 {
        "No post-closeout runtime handoff candidates are visible.".to_string()
    } else {
        format!(
            "{candidate_count} post-closeout runtime handoff candidate(s): {ready_count} ready to queue, {queued_count} queued, {blocked_count} blocked."
        )
    };

    RuntimeDispatchSurface {
        schema: RUNTIME_DISPATCH_SURFACE_SCHEMA,
        status,
        candidate_count,
        visible_count,
        summary,
        items,
        empty_state: RuntimeDispatchEmptyState {
            title: "No runtime handoff",
            summary: "Receipted decision-action closeouts are not waiting for runtime dispatch.",
            cli_fallback: "forager ondesk workstation-surface --json",
        },
        source_refs: BTreeMap::from([
            (
                "decision_action_closeouts".to_string(),
                DECISION_ACTION_CLOSEOUTS_FILE.to_string(),
            ),
            (
                "runtime_dispatch_preflights".to_string(),
                RUNTIME_DISPATCH_PREFLIGHTS_FILE.to_string(),
            ),
            (
                "runtime_dispatch_receipts".to_string(),
                RUNTIME_DISPATCH_RECEIPTS_FILE.to_string(),
            ),
        ]),
    }
}

fn runtime_dispatch_item(
    record: &DecisionRecord,
    closeouts: &[StoredDecisionActionCloseout],
    preflights: &[StoredRuntimeDispatchPreflight],
    receipts: &[StoredRuntimeDispatchReceipt],
) -> Option<RuntimeDispatchItem> {
    let closeout = latest_matching_closeout(record, closeouts)?;
    let latest_preflight = latest_runtime_preflight(&closeout.closeout_id, preflights);
    let latest_receipt = latest_runtime_receipt(
        &closeout.closeout_id,
        latest_preflight.map(|preflight| preflight.preflight_id.as_str()),
        receipts,
    );
    let stage = runtime_dispatch_stage(latest_preflight, latest_receipt);
    let severity = runtime_dispatch_severity(stage);
    let latest_preflight_summary = latest_preflight.map(runtime_dispatch_preflight_summary);
    let latest_receipt_summary = latest_receipt.map(runtime_dispatch_receipt_summary);
    let dispatch_command = latest_preflight
        .filter(|preflight| preflight.result_status == "ready_for_runtime_dispatch")
        .map(|preflight| {
            format!(
                "forager ondesk runtime-dispatch --preflight-id {} --runner local-background --cmd <command> --json",
                operator_safe_text(&preflight.preflight_id)
            )
        });
    let tick_command = latest_receipt
        .filter(|receipt| receipt.result_status == "queued" && !receipt.task_id.trim().is_empty())
        .map(|receipt| {
            format!(
                "forager offdesk tick --task-id {}",
                operator_safe_text(&receipt.task_id)
            )
        });

    Some(RuntimeDispatchItem {
        project_key: operator_safe_text(&record.project_key),
        decision_id: operator_safe_text(&record.decision_id),
        title: decision_title(record),
        stage,
        severity,
        closeout_id: operator_safe_text(&closeout.closeout_id),
        execution_id: operator_safe_text(&closeout.execution_id),
        receipt_id: closeout
            .receipt_id
            .as_ref()
            .map(|value| operator_safe_text(value)),
        handoff_id: closeout
            .handoff_id
            .as_ref()
            .map(|value| operator_safe_text(value)),
        latest_preflight: latest_preflight_summary,
        latest_receipt: latest_receipt_summary,
        preflight_command: format!(
            "forager ondesk runtime-preflight --closeout-id {} --json",
            operator_safe_text(&closeout.closeout_id)
        ),
        dispatch_command,
        tick_command,
        boundary: "Post-closeout handoff only: this surface does not queue or launch runtime work. Queueing requires runtime-dispatch; launching still goes through offdesk tick."
            .to_string(),
    })
}

fn latest_matching_closeout<'a>(
    record: &DecisionRecord,
    closeouts: &'a [StoredDecisionActionCloseout],
) -> Option<&'a StoredDecisionActionCloseout> {
    closeouts
        .iter()
        .enumerate()
        .filter(|(_, closeout)| closeout_matches_record(record, closeout))
        .max_by_key(|(index, closeout)| (closeout.recorded_at, *index))
        .map(|(_, closeout)| closeout)
}

fn closeout_matches_record(
    record: &DecisionRecord,
    closeout: &StoredDecisionActionCloseout,
) -> bool {
    if closeout.schema != "decision_action_closeout.v1"
        || closeout.result_status != "receipted"
        || !closeout.mutation_allowed_by_this_command
        || !closeout.decision_appended
        || closeout.project_key != record.project_key
        || closeout.decision_id != record.decision_id
    {
        return false;
    }
    let Some(receipt) = record.decision_receipt.as_ref() else {
        return false;
    };
    let receipt_matches = closeout
        .receipt_id
        .as_deref()
        .is_some_and(|receipt_id| receipt_id == receipt.receipt_id);
    let handoff_matches = closeout.handoff_id.as_deref() == receipt.applied_handoff_id.as_deref();
    receipt_matches && handoff_matches && receipt.final_decision == closeout.decision
}

fn latest_runtime_preflight<'a>(
    closeout_id: &str,
    preflights: &'a [StoredRuntimeDispatchPreflight],
) -> Option<&'a StoredRuntimeDispatchPreflight> {
    preflights
        .iter()
        .enumerate()
        .filter(|(_, preflight)| preflight.source_closeout_id == closeout_id)
        .max_by_key(|(index, preflight)| (preflight.processed_at, *index))
        .map(|(_, preflight)| preflight)
}

fn latest_runtime_receipt<'a>(
    closeout_id: &str,
    preflight_id: Option<&str>,
    receipts: &'a [StoredRuntimeDispatchReceipt],
) -> Option<&'a StoredRuntimeDispatchReceipt> {
    receipts
        .iter()
        .enumerate()
        .filter(|(_, receipt)| {
            receipt.source_closeout_id == closeout_id
                || preflight_id.is_some_and(|preflight_id| receipt.preflight_id == preflight_id)
        })
        .max_by_key(|(index, receipt)| (receipt.recorded_at, *index))
        .map(|(_, receipt)| receipt)
}

fn runtime_dispatch_stage(
    preflight: Option<&StoredRuntimeDispatchPreflight>,
    receipt: Option<&StoredRuntimeDispatchReceipt>,
) -> &'static str {
    if let Some(receipt) = receipt {
        return match receipt.result_status.as_str() {
            "queued" => "queued",
            "blocked" => "dispatch_blocked",
            _ => "dispatch_unknown",
        };
    }
    if let Some(preflight) = preflight {
        return match preflight.result_status.as_str() {
            "ready_for_runtime_dispatch" => "ready_to_queue",
            "blocked" => "preflight_blocked",
            _ => "preflight_unknown",
        };
    }
    "needs_preflight"
}

fn runtime_dispatch_severity(stage: &str) -> &'static str {
    match stage {
        "queued" => "ok",
        "ready_to_queue" | "needs_preflight" => "attention",
        "dispatch_blocked" | "preflight_blocked" | "dispatch_unknown" | "preflight_unknown" => {
            "blocked"
        }
        _ => "info",
    }
}

fn runtime_dispatch_stage_rank(stage: &str) -> u8 {
    match stage {
        "dispatch_blocked" | "preflight_blocked" => 0,
        "ready_to_queue" => 1,
        "needs_preflight" => 2,
        "queued" => 3,
        _ => 4,
    }
}

fn runtime_dispatch_preflight_summary(
    preflight: &StoredRuntimeDispatchPreflight,
) -> RuntimeDispatchPreflightSummary {
    RuntimeDispatchPreflightSummary {
        schema: operator_safe_text(&preflight.schema),
        preflight_id: operator_safe_text(&preflight.preflight_id),
        processed_at: preflight.processed_at,
        result_status: operator_safe_text(&preflight.result_status),
        reason: operator_safe_text(&preflight.reason),
    }
}

fn runtime_dispatch_receipt_summary(
    receipt: &StoredRuntimeDispatchReceipt,
) -> RuntimeDispatchReceiptSummary {
    RuntimeDispatchReceiptSummary {
        schema: operator_safe_text(&receipt.schema),
        receipt_id: operator_safe_text(&receipt.receipt_id),
        preflight_id: operator_safe_text(&receipt.preflight_id),
        recorded_at: receipt.recorded_at,
        result_status: operator_safe_text(&receipt.result_status),
        task_id: operator_safe_text(&receipt.task_id),
        reason: operator_safe_text(&receipt.reason),
    }
}

fn accepted_truth_recovery_surface(
    profile_dir: &Path,
    profile: &str,
    generated_at: DateTime<Utc>,
    action_receipts: &[StoredAcceptedTruthRecoveryActionReceipt],
) -> Result<AcceptedTruthRecoverySurface> {
    let closeouts_dir = profile_dir.join("offdesk_closeouts");
    if !closeouts_dir.exists() {
        return Ok(accepted_truth_recovery_surface_from_items(Vec::new()));
    }

    let mut items = Vec::new();
    for entry in fs::read_dir(&closeouts_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        if let Some(item) = accepted_truth_recovery_item_from_dir(
            &entry.path(),
            profile,
            generated_at,
            action_receipts,
        )? {
            items.push(item);
        }
    }
    Ok(accepted_truth_recovery_surface_from_items(items))
}

fn accepted_truth_recovery_surface_from_items(
    mut items: Vec<AcceptedTruthRecoveryItem>,
) -> AcceptedTruthRecoverySurface {
    items.sort_by_key(|item| {
        (
            severity_rank(item.severity),
            accepted_truth_recovery_stage_rank(item.stage),
            std::cmp::Reverse(item.reviewed_at),
            item.project_key.clone(),
        )
    });
    let candidate_count = items.len();
    let followup_count = items
        .iter()
        .filter(|item| item.stage == "followup_required")
        .count();
    let blocked_count = items
        .iter()
        .filter(|item| item.stage == "blocked_or_revision")
        .count();
    let retired_count = items
        .iter()
        .filter(|item| item.stage == "retired_incomplete")
        .count();
    items.truncate(MAX_ACCEPTED_TRUTH_RECOVERY_ITEMS);
    let visible_count = items.len();
    let status = if blocked_count > 0 {
        "blocked"
    } else if followup_count > 0 {
        "attention"
    } else if retired_count > 0 {
        "info"
    } else {
        "clear"
    };
    let summary = if candidate_count == 0 {
        "No closeout receipt currently blocks accepted-truth status.".to_string()
    } else {
        format!(
            "{candidate_count} accepted-truth recovery candidate(s): {followup_count} with follow-ups, {blocked_count} blocked/revision, {retired_count} retired incomplete."
        )
    };

    AcceptedTruthRecoverySurface {
        schema: ACCEPTED_TRUTH_RECOVERY_SURFACE_SCHEMA,
        status,
        candidate_count,
        visible_count,
        summary,
        items,
        empty_state: AcceptedTruthRecoveryEmptyState {
            title: "Accepted truth clear",
            summary: "No latest closeout receipt currently needs accepted-truth recovery.",
            cli_fallback: "forager ondesk review-surface --json",
        },
        source_refs: BTreeMap::from([
            (
                "closeout_reviews".to_string(),
                "offdesk_closeouts/*/closeout_review_*.json".to_string(),
            ),
            (
                "closeout_plans".to_string(),
                "offdesk_closeouts/*/closeout_plan.json".to_string(),
            ),
            (
                "cli_fallback".to_string(),
                "forager ondesk review-surface --json".to_string(),
            ),
            (
                "action_receipts".to_string(),
                ACCEPTED_TRUTH_RECOVERY_ACTION_RECEIPTS_FILE.to_string(),
            ),
        ]),
    }
}

fn accepted_truth_recovery_item_from_dir(
    artifact_dir: &Path,
    profile: &str,
    generated_at: DateTime<Utc>,
    action_receipts: &[StoredAcceptedTruthRecoveryActionReceipt],
) -> Result<Option<AcceptedTruthRecoveryItem>> {
    let plan_path = artifact_dir.join("closeout_plan.json");
    let Ok(plan_content) = fs::read_to_string(&plan_path) else {
        return Ok(None);
    };
    let Ok(plan) = serde_json::from_str::<Value>(&plan_content) else {
        return Ok(None);
    };
    let Some((reviewed_at, review_path, review)) = latest_closeout_review_value(artifact_dir)?
    else {
        return Ok(None);
    };
    let receipt = review.get("closeout_receipt");
    let acceptance_status = receipt
        .and_then(|receipt| receipt.get("acceptance_status"))
        .and_then(Value::as_str)
        .map(operator_safe_text)
        .unwrap_or_else(|| "receipt_missing".to_string());
    if acceptance_status == "accepted" {
        return Ok(None);
    }

    let closeout_id = plan
        .get("closeout_id")
        .and_then(Value::as_str)
        .or_else(|| review.get("closeout_id").and_then(Value::as_str))
        .map(operator_safe_text)
        .unwrap_or_else(|| artifact_dir_name(artifact_dir));
    let review_id = review
        .get("review_id")
        .and_then(Value::as_str)
        .map(operator_safe_text)
        .unwrap_or_else(|| artifact_file_name(&review_path));
    let receipt_id = receipt
        .and_then(|receipt| receipt.get("receipt_id"))
        .and_then(Value::as_str)
        .map(operator_safe_text)
        .unwrap_or_default();
    let verification_status = receipt
        .and_then(|receipt| receipt.get("verification_status"))
        .and_then(Value::as_str)
        .map(operator_safe_text)
        .unwrap_or_else(|| "unknown".to_string());
    let open_decision_kinds = receipt
        .map(closeout_open_decision_kinds)
        .unwrap_or_default();
    let open_decision_count = receipt
        .and_then(|receipt| receipt.get("open_decisions"))
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or_default();
    let stage = accepted_truth_recovery_stage(&acceptance_status);
    let severity = accepted_truth_recovery_severity(stage);
    let evidence_status = receipt_text_field(receipt, "evidence_status", "unknown");
    let retention_review = receipt_text_field(receipt, "retention_review", "unknown");
    let wiki_promotion_state = receipt_text_field(receipt, "wiki_promotion_state", "unknown");
    let stale_task_count = receipt
        .and_then(|receipt| receipt.get("stale_task_count"))
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_default();
    let next_safe_action = receipt
        .and_then(|receipt| receipt.get("next_safe_action"))
        .and_then(Value::as_str)
        .map(operator_safe_text)
        .unwrap_or_else(|| "Review the latest closeout record before accepting truth.".to_string());
    let project_key = accepted_truth_recovery_project_key(&plan, &review);
    let resolve_command = open_decision_kinds.first().map(|kind| {
        format!(
            "forager offdesk closeout-decision --closeout-id {} --kind {} --decision preserve-in-place --reason <reason> --json",
            closeout_id, kind
        )
    });
    let retire_command = (acceptance_status != "retired_incomplete").then(|| {
        format!(
            "forager offdesk closeout-retire --closeout-id {closeout_id} --reason <reason> --json"
        )
    });
    let target_ref = AcceptedTruthRecoveryTargetRef {
        kind: "accepted_truth_recovery.v1",
        closeout_id: closeout_id.clone(),
        review_id: review_id.clone(),
        receipt_id: receipt_id.clone(),
        acceptance_status: acceptance_status.clone(),
        reviewed_at,
    };
    let action_context = AcceptedTruthRecoveryActionContext {
        profile,
        project_key: &project_key,
        target_ref: &target_ref,
        verification_status: &verification_status,
        open_decision_kinds: &open_decision_kinds,
        evidence_status: &evidence_status,
        retention_review: &retention_review,
        wiki_promotion_state: &wiki_promotion_state,
        stale_task_count,
        generated_at,
        action_receipts,
    };
    let action_envelopes = accepted_truth_recovery_action_envelopes(
        &action_context,
        resolve_command.as_deref(),
        retire_command.as_deref(),
    );

    Ok(Some(AcceptedTruthRecoveryItem {
        project_key,
        closeout_id,
        review_id,
        receipt_id,
        acceptance_status,
        verification_status,
        stage,
        severity,
        open_decision_count,
        open_decision_kinds,
        evidence_status,
        retention_review,
        wiki_promotion_state,
        stale_task_count,
        next_safe_action,
        artifact_dir: operator_safe_text(artifact_dir.to_string_lossy().as_ref()),
        reviewed_at,
        resolve_command,
        retire_command,
        action_envelopes,
        boundary: "Read-only accepted-truth recovery: this surface does not resolve follow-ups, retire closeouts, move files, promote wiki state, or record accepted truth."
            .to_string(),
    }))
}

struct AcceptedTruthRecoveryActionContext<'a> {
    profile: &'a str,
    project_key: &'a str,
    target_ref: &'a AcceptedTruthRecoveryTargetRef,
    verification_status: &'a str,
    open_decision_kinds: &'a [String],
    evidence_status: &'a str,
    retention_review: &'a str,
    wiki_promotion_state: &'a str,
    stale_task_count: usize,
    generated_at: DateTime<Utc>,
    action_receipts: &'a [StoredAcceptedTruthRecoveryActionReceipt],
}

fn accepted_truth_recovery_action_envelopes(
    context: &AcceptedTruthRecoveryActionContext<'_>,
    resolve_command: Option<&str>,
    retire_command: Option<&str>,
) -> Vec<AcceptedTruthRecoveryActionEnvelopePreview> {
    let mut actions = Vec::new();
    if let Some(command) = resolve_command {
        actions.push(("resolve_followup", command));
    }
    if let Some(command) = retire_command {
        actions.push(("retire_closeout", command));
    }

    actions
        .into_iter()
        .take(2)
        .map(|(action_kind, command)| {
            accepted_truth_recovery_action_envelope_preview(context, action_kind, command)
        })
        .collect()
}

fn accepted_truth_recovery_action_envelope_preview(
    context: &AcceptedTruthRecoveryActionContext<'_>,
    action_kind: &str,
    allowed_command: &str,
) -> AcceptedTruthRecoveryActionEnvelopePreview {
    let observed_hash = accepted_truth_recovery_observed_hash(context, action_kind);
    let hash_prefix = hash_prefix(&observed_hash, 16);
    let action_id = format!(
        "truth_action_{}_{}_{}",
        action_kind_slug(&context.target_ref.closeout_id),
        action_kind,
        hash_prefix
    );
    let idempotency_key = format!(
        "accepted_truth_recovery:{}:{}:{}:{}:{}",
        operator_safe_text(&context.target_ref.closeout_id),
        operator_safe_text(&context.target_ref.review_id),
        operator_safe_text(&context.target_ref.receipt_id),
        action_kind,
        hash_prefix
    );
    let (latest_receipt, receipt_history_count) = accepted_truth_recovery_action_latest_receipt(
        &action_id,
        &idempotency_key,
        context.action_receipts,
    );

    AcceptedTruthRecoveryActionEnvelopePreview {
        schema: ACCEPTED_TRUTH_RECOVERY_ACTION_ENVELOPE_SCHEMA,
        action_id,
        action_kind: action_kind.to_string(),
        profile: operator_safe_text(context.profile),
        project_key: operator_safe_text(context.project_key),
        target_ref: context.target_ref.clone(),
        observed_hash: observed_hash.clone(),
        nonce: format!("truth_preview_{hash_prefix}"),
        ttl: "PT10M",
        issued_at: context.generated_at,
        expires_at: context.generated_at + Duration::minutes(10),
        idempotency_key,
        preview: format!(
            "Preview only: validate {action_kind} fallback for closeout {}; this does not execute the fallback.",
            operator_safe_text(&context.target_ref.closeout_id)
        ),
        allowed_command: operator_safe_text(allowed_command),
        forbidden_effects: vec![
            "project_file_mutation",
            "runtime_dispatch",
            "approval_ledger_mutation",
            "accepted_truth_mutation",
            "arbitrary_shell",
            "wiki_promotion",
            "file_movement",
        ],
        expected_receipt_schema: ACCEPTED_TRUTH_RECOVERY_ACTION_RECEIPT_SCHEMA,
        requires_confirmation: true,
        confirmation_phrase: format!(
            "confirm {} {}",
            action_kind,
            action_kind_slug(&context.target_ref.closeout_id)
        ),
        stale_rejection_reason:
            "Reject if the closeout review, receipt state, or selected recovery fallback no longer matches observed_hash."
                .to_string(),
        receipt_history_count,
        latest_receipt,
    }
}

fn accepted_truth_recovery_observed_hash(
    context: &AcceptedTruthRecoveryActionContext<'_>,
    action_kind: &str,
) -> String {
    accepted_truth_recovery_observed_hash_parts(
        &context.target_ref.closeout_id,
        &context.target_ref.review_id,
        &context.target_ref.receipt_id,
        &context.target_ref.acceptance_status,
        &context.target_ref.reviewed_at.to_rfc3339(),
        context.verification_status,
        context.open_decision_kinds,
        context.evidence_status,
        context.retention_review,
        context.wiki_promotion_state,
        context.stale_task_count,
        action_kind,
    )
}

pub(crate) fn accepted_truth_recovery_observed_hash_from_value(
    item: &Value,
    action_kind: &str,
) -> String {
    let reviewed_at = item
        .get("reviewed_at")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc).to_rfc3339())
        .or_else(|| {
            item.get("reviewed_at")
                .and_then(Value::as_str)
                .map(operator_safe_text)
        })
        .unwrap_or_default();
    let open_decision_kinds = item
        .get("open_decision_kinds")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|items| items.iter())
        .filter_map(Value::as_str)
        .map(operator_safe_text)
        .filter(|value| !value.trim().is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    accepted_truth_recovery_observed_hash_parts(
        item.get("closeout_id")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        item.get("review_id")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        item.get("receipt_id")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        item.get("acceptance_status")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        &reviewed_at,
        item.get("verification_status")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        &open_decision_kinds,
        item.get("evidence_status")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        item.get("retention_review")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        item.get("wiki_promotion_state")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        item.get("stale_task_count")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or_default(),
        action_kind,
    )
}

#[allow(clippy::too_many_arguments)]
fn accepted_truth_recovery_observed_hash_parts(
    closeout_id: &str,
    review_id: &str,
    receipt_id: &str,
    acceptance_status: &str,
    reviewed_at: &str,
    verification_status: &str,
    open_decision_kinds: &[String],
    evidence_status: &str,
    retention_review: &str,
    wiki_promotion_state: &str,
    stale_task_count: usize,
    action_kind: &str,
) -> String {
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        operator_safe_text(closeout_id),
        operator_safe_text(review_id),
        operator_safe_text(receipt_id),
        operator_safe_text(acceptance_status),
        operator_safe_text(reviewed_at),
        operator_safe_text(verification_status),
        open_decision_kinds
            .iter()
            .map(|value| operator_safe_text(value))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
            .join(","),
        operator_safe_text(evidence_status),
        operator_safe_text(retention_review),
        operator_safe_text(wiki_promotion_state),
        stale_task_count,
        operator_safe_text(action_kind)
    );
    sha256_hex(canonical.as_bytes())
}

fn accepted_truth_recovery_action_latest_receipt(
    action_id: &str,
    idempotency_key: &str,
    receipts: &[StoredAcceptedTruthRecoveryActionReceipt],
) -> (Option<AcceptedTruthRecoveryActionReceiptSummary>, usize) {
    let matching = receipts
        .iter()
        .enumerate()
        .filter(|receipt| {
            receipt.1.action_id == action_id || receipt.1.idempotency_key == idempotency_key
        })
        .collect::<Vec<_>>();
    let receipt_history_count = matching.len();
    let latest = matching
        .into_iter()
        .max_by_key(|(index, receipt)| (receipt.processed_at, *index))
        .map(|(_, receipt)| accepted_truth_recovery_action_receipt_summary(receipt));
    (latest, receipt_history_count)
}

fn accepted_truth_recovery_action_receipt_summary(
    receipt: &StoredAcceptedTruthRecoveryActionReceipt,
) -> AcceptedTruthRecoveryActionReceiptSummary {
    AcceptedTruthRecoveryActionReceiptSummary {
        schema: operator_safe_text(&receipt.schema),
        receipt_id: operator_safe_text(&receipt.receipt_id),
        processed_at: receipt.processed_at,
        result_status: operator_safe_text(&receipt.result_status),
        stale: receipt.stale,
        reason: operator_safe_text(&receipt.reason),
        current_hash: receipt
            .current_hash
            .as_ref()
            .map(|value| operator_safe_text(value)),
    }
}

fn latest_closeout_review_value(
    artifact_dir: &Path,
) -> Result<Option<(DateTime<Utc>, PathBuf, Value)>> {
    let mut reviews = Vec::new();
    for entry in fs::read_dir(artifact_dir)? {
        let entry = entry?;
        let path = entry.path();
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
        reviews.push((reviewed_at, path, value));
    }
    reviews.sort_by_key(|(reviewed_at, _, _)| *reviewed_at);
    Ok(reviews.pop())
}

fn accepted_truth_recovery_stage(acceptance_status: &str) -> &'static str {
    match acceptance_status {
        "approved_with_followups" => "followup_required",
        "revision_required" | "blocked" => "blocked_or_revision",
        "retired_incomplete" => "retired_incomplete",
        "receipt_missing" => "receipt_missing",
        _ => "needs_review",
    }
}

fn accepted_truth_recovery_severity(stage: &str) -> &'static str {
    match stage {
        "followup_required" | "receipt_missing" | "needs_review" => "attention",
        "blocked_or_revision" => "blocked",
        "retired_incomplete" => "info",
        _ => "info",
    }
}

fn accepted_truth_recovery_stage_rank(stage: &str) -> u8 {
    match stage {
        "blocked_or_revision" => 0,
        "followup_required" => 1,
        "receipt_missing" | "needs_review" => 2,
        "retired_incomplete" => 3,
        _ => 4,
    }
}

fn closeout_open_decision_kinds(receipt: &Value) -> Vec<String> {
    receipt
        .get("open_decisions")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|items| items.iter())
        .filter_map(|item| item.get("kind").and_then(Value::as_str))
        .map(operator_safe_text)
        .filter(|value| !value.trim().is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn accepted_truth_recovery_project_key(plan: &Value, review: &Value) -> String {
    let mut keys = BTreeSet::new();
    for item in plan
        .get("tasks")
        .and_then(Value::as_array)
        .into_iter()
        .flat_map(|items| items.iter())
        .chain(
            review
                .get("applies_to_tasks")
                .and_then(Value::as_array)
                .into_iter()
                .flat_map(|items| items.iter()),
        )
    {
        if let Some(key) = item.get("project_key").and_then(Value::as_str) {
            let key = operator_safe_text(key);
            if !key.trim().is_empty() {
                keys.insert(key);
            }
        }
    }
    match keys.len() {
        0 => "unknown".to_string(),
        1 => keys
            .into_iter()
            .next()
            .unwrap_or_else(|| "unknown".to_string()),
        _ => "multi".to_string(),
    }
}

fn receipt_text_field(receipt: Option<&Value>, key: &str, fallback: &str) -> String {
    receipt
        .and_then(|receipt| receipt.get(key))
        .and_then(Value::as_str)
        .map(operator_safe_text)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn artifact_dir_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(operator_safe_text)
        .unwrap_or_else(|| operator_safe_text(path.to_string_lossy().as_ref()))
}

fn artifact_file_name(path: &Path) -> String {
    path.file_name()
        .and_then(|value| value.to_str())
        .map(operator_safe_text)
        .unwrap_or_else(|| operator_safe_text(path.to_string_lossy().as_ref()))
}

fn decision_title(record: &DecisionRecord) -> String {
    record
        .approval_brief
        .as_ref()
        .map(|brief| operator_safe_text(&brief.subject))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| operator_safe_text(&record.decision_request.summary))
}

fn decision_items(
    decisions: &[&DecisionRecord],
    profile: &str,
    generated_at: DateTime<Utc>,
    action_receipts: &[StoredActionEnvelopeReceipt],
    action_executions: &[StoredDecisionActionExecution],
) -> Vec<DecisionItem> {
    let mut decisions = decisions.to_vec();
    decisions.sort_by_key(|record| {
        (
            severity_rank(decision_severity(record)),
            std::cmp::Reverse(record.updated_at),
            operator_safe_text(&record.project_key),
        )
    });
    decisions
        .into_iter()
        .take(MAX_DECISION_ITEMS)
        .map(|record| {
            decision_item(
                record,
                profile,
                generated_at,
                action_receipts,
                action_executions,
            )
        })
        .collect()
}

fn decision_item(
    record: &DecisionRecord,
    profile: &str,
    generated_at: DateTime<Utc>,
    action_receipts: &[StoredActionEnvelopeReceipt],
    action_executions: &[StoredDecisionActionExecution],
) -> DecisionItem {
    let approval_brief = record.approval_brief.as_ref();
    let route = record.route.as_ref();
    let council_review = record.council_review.as_ref();
    let allowed_actions = decision_allowed_actions(record);
    DecisionItem {
        decision_id: operator_safe_text(&record.decision_id),
        kind: operator_safe_text(&record.decision_request.kind),
        severity: decision_severity(record),
        status: record.status.as_str().to_string(),
        materiality: record.materiality.as_str().to_string(),
        raised_by: record.raised_by.as_str().to_string(),
        project_key: operator_safe_text(&record.project_key),
        title: approval_brief
            .map(|brief| operator_safe_text(&brief.subject))
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| operator_safe_text(&record.decision_request.summary)),
        what_changed: decision_what_changed(record),
        why_now: decision_why_now(record),
        risk: decision_risk(record),
        evidence_refs: decision_evidence_refs(record),
        action_envelopes: decision_action_envelopes(
            record,
            profile,
            &allowed_actions,
            generated_at,
            action_receipts,
            action_executions,
        ),
        allowed_actions,
        recommendation: approval_brief
            .map(|brief| operator_safe_text(&brief.recommendation))
            .or_else(|| council_review.map(|review| operator_safe_text(&review.recommendation)))
            .filter(|value| !value.trim().is_empty()),
        default_if_no_reply: approval_brief
            .and_then(|brief| brief.default_if_no_reply.as_ref())
            .map(|value| operator_safe_text(value))
            .or_else(|| {
                route
                    .and_then(|route| route.default_if_no_reply.as_ref())
                    .map(|value| operator_safe_text(value))
            }),
        authorization_boundary: decision_authorization_boundary(record),
        stale_guard: decision_stale_guard(record),
        receipt_ref: record
            .decision_receipt
            .as_ref()
            .map(|receipt| operator_safe_text(&receipt.receipt_id)),
        cli_fallback: "forager offdesk decisions --json".to_string(),
        updated_at: record.updated_at,
    }
}

fn decision_action_envelopes(
    record: &DecisionRecord,
    profile: &str,
    allowed_actions: &[String],
    generated_at: DateTime<Utc>,
    action_receipts: &[StoredActionEnvelopeReceipt],
    action_executions: &[StoredDecisionActionExecution],
) -> Vec<ActionEnvelopePreview> {
    let actions = if allowed_actions.is_empty() {
        vec!["Review decision".to_string()]
    } else {
        allowed_actions.to_vec()
    };
    actions
        .iter()
        .take(MAX_ACTION_ENVELOPES_PER_DECISION)
        .map(|action| {
            action_envelope_preview(
                record,
                profile,
                action,
                generated_at,
                action_receipts,
                action_executions,
            )
        })
        .collect()
}

fn action_envelope_preview(
    record: &DecisionRecord,
    profile: &str,
    action_label: &str,
    generated_at: DateTime<Utc>,
    action_receipts: &[StoredActionEnvelopeReceipt],
    action_executions: &[StoredDecisionActionExecution],
) -> ActionEnvelopePreview {
    let action_kind = action_kind_slug(action_label);
    let observed_hash = action_observed_hash(record, &action_kind);
    let hash_prefix = observed_hash
        .strip_prefix("sha256:")
        .unwrap_or(&observed_hash)
        .chars()
        .take(16)
        .collect::<String>();
    let action_id = format!(
        "action_{}_{}_{}",
        action_kind_slug(&record.decision_id),
        action_kind,
        hash_prefix
    );
    let idempotency_key = format!(
        "decision:{}:{}:{}",
        operator_safe_text(&record.decision_id),
        action_kind,
        hash_prefix
    );
    let (latest_receipt, receipt_history_count) =
        action_envelope_latest_receipt(&action_id, &idempotency_key, action_receipts);
    let (latest_execution, execution_history_count) =
        decision_action_latest_execution(record, &action_kind, action_executions);
    let requires_confirmation = action_requires_confirmation(record, action_label);
    ActionEnvelopePreview {
        schema: ACTION_ENVELOPE_SCHEMA,
        action_id,
        action_kind: action_kind.clone(),
        profile: operator_safe_text(profile),
        project_key: operator_safe_text(&record.project_key),
        target_ref: ActionTargetRef {
            kind: "decision_record.v1",
            decision_id: operator_safe_text(&record.decision_id),
            status: record.status.as_str().to_string(),
            updated_at: record.updated_at,
        },
        observed_hash: observed_hash.clone(),
        nonce: format!("preview_{hash_prefix}"),
        ttl: "PT10M",
        issued_at: generated_at,
        expires_at: generated_at + Duration::minutes(10),
        idempotency_key,
        preview: format!(
            "Preview only: {} for decision {}.",
            operator_safe_text(action_label),
            operator_safe_text(&record.decision_id)
        ),
        allowed_command: format!(
            "forager offdesk decision show --json {}",
            operator_safe_text(&record.decision_id)
        ),
        forbidden_effects: vec![
            "project_file_mutation",
            "runtime_dispatch",
            "approval_ledger_mutation",
            "accepted_truth_mutation",
            "arbitrary_shell",
        ],
        expected_receipt_schema: "action_envelope_receipt.v1",
        requires_confirmation,
        confirmation_phrase: requires_confirmation.then(|| {
            format!(
                "confirm {}",
                action_kind_slug(&format!("{} {}", record.decision_id, action_label))
            )
        }),
        stale_rejection_reason:
            "Reject if the decision record status, updated_at, project, or selected action no longer matches observed_hash."
                .to_string(),
        receipt_history_count,
        latest_receipt,
        execution_history_count,
        latest_execution,
    }
}

fn load_action_envelope_receipts(profile_dir: &Path) -> Result<Vec<StoredActionEnvelopeReceipt>> {
    let path = profile_dir.join(ACTION_ENVELOPE_RECEIPTS_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(&path)?;
    let receipts = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str::<StoredActionEnvelopeReceipt>(line).ok())
        .collect();
    Ok(receipts)
}

fn action_envelope_latest_receipt(
    action_id: &str,
    idempotency_key: &str,
    receipts: &[StoredActionEnvelopeReceipt],
) -> (Option<ActionEnvelopeReceiptSummary>, usize) {
    let matching = receipts
        .iter()
        .enumerate()
        .filter(|receipt| {
            receipt.1.action_id == action_id || receipt.1.idempotency_key == idempotency_key
        })
        .collect::<Vec<_>>();
    let receipt_history_count = matching.len();
    let latest = matching
        .into_iter()
        .max_by_key(|(index, receipt)| (receipt.processed_at, *index))
        .map(|(_, receipt)| action_envelope_receipt_summary(receipt));
    (latest, receipt_history_count)
}

fn action_envelope_receipt_summary(
    receipt: &StoredActionEnvelopeReceipt,
) -> ActionEnvelopeReceiptSummary {
    let mut failed_checks = receipt
        .checks
        .iter()
        .filter(|check| check.status == "failed")
        .map(|check| operator_safe_text(&check.name))
        .filter(|check| !check.trim().is_empty())
        .collect::<Vec<_>>();
    failed_checks.truncate(MAX_ACTION_RECEIPT_FAILED_CHECKS);
    ActionEnvelopeReceiptSummary {
        schema: operator_safe_text(&receipt.schema),
        receipt_id: operator_safe_text(&receipt.receipt_id),
        processed_at: receipt.processed_at,
        result_status: operator_safe_text(&receipt.result_status),
        stale: receipt.stale,
        reason: operator_safe_text(&receipt.reason),
        current_hash: receipt
            .current_hash
            .as_ref()
            .map(|value| operator_safe_text(value)),
        failed_checks,
    }
}

fn load_accepted_truth_recovery_action_receipts(
    profile_dir: &Path,
) -> Result<Vec<StoredAcceptedTruthRecoveryActionReceipt>> {
    let path = profile_dir.join(ACCEPTED_TRUTH_RECOVERY_ACTION_RECEIPTS_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(&path)?;
    let receipts = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| {
            serde_json::from_str::<StoredAcceptedTruthRecoveryActionReceipt>(line).ok()
        })
        .collect();
    Ok(receipts)
}

fn load_decision_action_executions(
    profile_dir: &Path,
) -> Result<Vec<StoredDecisionActionExecution>> {
    let path = profile_dir.join(DECISION_ACTION_EXECUTIONS_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(&path)?;
    let executions = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str::<StoredDecisionActionExecution>(line).ok())
        .collect();
    Ok(executions)
}

fn load_decision_action_closeouts(profile_dir: &Path) -> Result<Vec<StoredDecisionActionCloseout>> {
    let path = profile_dir.join(DECISION_ACTION_CLOSEOUTS_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(&path)?;
    let closeouts = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str::<StoredDecisionActionCloseout>(line).ok())
        .collect();
    Ok(closeouts)
}

fn load_runtime_dispatch_preflights(
    profile_dir: &Path,
) -> Result<Vec<StoredRuntimeDispatchPreflight>> {
    let path = profile_dir.join(RUNTIME_DISPATCH_PREFLIGHTS_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(&path)?;
    let preflights = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str::<StoredRuntimeDispatchPreflight>(line).ok())
        .collect();
    Ok(preflights)
}

fn load_runtime_dispatch_receipts(profile_dir: &Path) -> Result<Vec<StoredRuntimeDispatchReceipt>> {
    let path = profile_dir.join(RUNTIME_DISPATCH_RECEIPTS_FILE);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(&path)?;
    let receipts = content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| serde_json::from_str::<StoredRuntimeDispatchReceipt>(line).ok())
        .collect();
    Ok(receipts)
}

fn decision_action_latest_execution(
    record: &DecisionRecord,
    action_kind: &str,
    executions: &[StoredDecisionActionExecution],
) -> (Option<DecisionActionExecutionSummary>, usize) {
    let matching = executions
        .iter()
        .enumerate()
        .filter(|(_, execution)| {
            execution.decision_id == record.decision_id
                && execution.project_key == record.project_key
                && execution.action_kind == action_kind
        })
        .collect::<Vec<_>>();
    let execution_history_count = matching.len();
    let latest = matching
        .into_iter()
        .max_by_key(|(index, execution)| (execution.executed_at, *index))
        .map(|(_, execution)| decision_action_execution_summary(execution));
    (latest, execution_history_count)
}

fn decision_action_execution_summary(
    execution: &StoredDecisionActionExecution,
) -> DecisionActionExecutionSummary {
    let mut failed_checks = execution
        .checks
        .iter()
        .filter(|check| check.status == "failed")
        .map(|check| operator_safe_text(&check.name))
        .filter(|check| !check.trim().is_empty())
        .collect::<Vec<_>>();
    failed_checks.truncate(MAX_ACTION_EXECUTION_FAILED_CHECKS);
    DecisionActionExecutionSummary {
        schema: operator_safe_text(&execution.schema),
        execution_id: operator_safe_text(&execution.execution_id),
        preflight_id: operator_safe_text(&execution.preflight_id),
        executed_at: execution.executed_at,
        result_status: operator_safe_text(&execution.result_status),
        decision: operator_safe_text(&execution.decision),
        decision_appended: execution.decision_appended,
        mutation_allowed_by_this_command: execution.mutation_allowed_by_this_command,
        reason: operator_safe_text(&execution.reason),
        handoff_id: execution
            .handoff_id
            .as_ref()
            .map(|value| operator_safe_text(value)),
        closeout_command: (execution.result_status == "applied" && execution.handoff_id.is_some())
            .then(|| {
                format!(
                    "forager ondesk action-closeout --execution-id {} --result-status closed --evidence <summary> --json",
                    operator_safe_text(&execution.execution_id)
                )
            }),
        failed_checks,
    }
}

fn decision_what_changed(record: &DecisionRecord) -> String {
    if let Some(brief) = record.approval_brief.as_ref() {
        if !brief.summary_lines.is_empty() {
            return brief
                .summary_lines
                .iter()
                .take(2)
                .map(|line| operator_safe_text(line))
                .collect::<Vec<_>>()
                .join(" ");
        }
    }
    operator_safe_text(&record.decision_request.summary)
}

fn decision_why_now(record: &DecisionRecord) -> String {
    if record.decision_request.why_now.is_empty() {
        operator_safe_text(&record.decision_request.decision_needed)
    } else {
        record
            .decision_request
            .why_now
            .iter()
            .map(|value| operator_safe_text(value))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

fn decision_risk(record: &DecisionRecord) -> String {
    if let Some(review) = record.council_review.as_ref() {
        if !review.risk_notes.is_empty() {
            return review
                .risk_notes
                .iter()
                .take(2)
                .map(|note| operator_safe_text(note))
                .collect::<Vec<_>>()
                .join(" ");
        }
    }
    if let Some(route) = record.route.as_ref() {
        if !route.reason.trim().is_empty() {
            return operator_safe_text(&route.reason);
        }
    }
    format!(
        "Materiality is {}; review before changing scope, runtime, provider, or accepted-truth state.",
        record.materiality.as_str()
    )
}

fn decision_evidence_refs(record: &DecisionRecord) -> Vec<DecisionEvidenceRef> {
    let mut refs = Vec::new();
    for item in record
        .decision_request
        .evidence_refs
        .iter()
        .chain(record.decision_request.trace_refs.iter())
        .chain(record.trace_refs.iter())
    {
        refs.push(DecisionEvidenceRef {
            kind: operator_safe_text(&item.kind),
            label: operator_safe_text(&item.label),
            reference: operator_safe_text(&item.reference),
        });
        if refs.len() >= MAX_DECISION_EVIDENCE_REFS {
            return refs;
        }
    }
    if let Some(route) = record.judgment_route.as_ref() {
        for item in &route.evidence_refs {
            refs.push(DecisionEvidenceRef {
                kind: operator_safe_text(&item.kind),
                label: operator_safe_text(&item.label),
                reference: operator_safe_text(&item.reference),
            });
            if refs.len() >= MAX_DECISION_EVIDENCE_REFS {
                return refs;
            }
        }
    }
    refs
}

fn decision_allowed_actions(record: &DecisionRecord) -> Vec<String> {
    // Envelopes must carry the option ID (the executor's action vocabulary:
    // continue/revise/block/stop/deny/defer/plan), not the display label.
    // Slugged labels like "revise_next_step" pass envelope validation but
    // are rejected by `ondesk action-decision`, so the operator could never
    // complete the chain.
    if let Some(brief) = record.approval_brief.as_ref() {
        if !brief.options.is_empty() {
            return brief
                .options
                .iter()
                .map(|option| decision_option_action(&option.id, &option.label))
                .collect();
        }
    }
    if record.decision_request.options.is_empty() {
        vec!["Review decision".to_string()]
    } else {
        record
            .decision_request
            .options
            .iter()
            .map(|option| decision_option_action(&option.id, &option.label))
            .collect()
    }
}

fn decision_option_action(id: &str, label: &str) -> String {
    let id = operator_safe_text(id);
    if id.trim().is_empty() {
        operator_safe_text(label)
    } else {
        id
    }
}

fn decision_authorization_boundary(record: &DecisionRecord) -> String {
    let mut boundary = operator_safe_text(&record.decision_request.current_scope);
    if boundary.trim().is_empty() {
        boundary = "Decision is scoped to the referenced request only.".to_string();
    }
    if !record.decision_request.non_authorized_scope.is_empty() {
        let denied = record
            .decision_request
            .non_authorized_scope
            .iter()
            .map(|item| operator_safe_text(item))
            .collect::<Vec<_>>()
            .join("; ");
        boundary = format!("{boundary} Not authorized: {denied}.");
    }
    boundary
}

fn decision_stale_guard(record: &DecisionRecord) -> String {
    if let Some(expires_at) = record
        .route
        .as_ref()
        .and_then(|route| route.expires_at.as_ref())
    {
        return format!(
            "Treat this decision as stale after {}.",
            expires_at.to_rfc3339()
        );
    }
    format!(
        "Verify ledger state is still current after {}; do not execute from this read-only surface.",
        record.updated_at.to_rfc3339()
    )
}

fn action_requires_confirmation(record: &DecisionRecord, action_label: &str) -> bool {
    let action = action_label.to_ascii_lowercase();
    let kind = record.decision_request.kind.to_ascii_lowercase();
    let high_materiality = matches!(
        record.materiality,
        crate::offdesk::DecisionMateriality::High
    );
    high_materiality
        || kind.contains("accepted_truth")
        || kind.contains("closeout")
        || action.contains("accept")
        || action.contains("approve")
        || action.contains("delete")
        || action.contains("retire")
        || action.contains("truth")
}

fn action_observed_hash(record: &DecisionRecord, action_kind: &str) -> String {
    let canonical = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}",
        record.decision_id,
        record.project_key,
        record.status.as_str(),
        record.materiality.as_str(),
        record.updated_at.to_rfc3339(),
        record.decision_request.kind,
        action_kind
    );
    sha256_hex(canonical.as_bytes())
}

fn action_kind_slug(value: &str) -> String {
    let mut output = String::new();
    let mut last_was_separator = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            output.push(ch.to_ascii_lowercase());
            last_was_separator = false;
        } else if !last_was_separator && !output.is_empty() {
            output.push('_');
            last_was_separator = true;
        }
    }
    while output.ends_with('_') {
        output.pop();
    }
    if output.is_empty() {
        "review_decision".to_string()
    } else {
        output
    }
}

fn hash_prefix(hash: &str, len: usize) -> String {
    hash.strip_prefix("sha256:")
        .unwrap_or(hash)
        .chars()
        .take(len)
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

fn graph_focus() -> GraphFocus {
    GraphFocus {
        title: "Selected provenance path",
        nodes: vec![
            GraphNode {
                id: "status",
                label: "Status",
                kind: "source",
            },
            GraphNode {
                id: "attention",
                label: "Attention",
                kind: "summary",
            },
            GraphNode {
                id: "decision",
                label: "Decision",
                kind: "decision",
            },
            GraphNode {
                id: "action",
                label: "Next safe action",
                kind: "action",
            },
            GraphNode {
                id: "receipt",
                label: "Receipt",
                kind: "receipt",
            },
        ],
        edges: vec![
            GraphEdge {
                from: "status",
                to: "attention",
                label: "projects",
            },
            GraphEdge {
                from: "attention",
                to: "decision",
                label: "prioritizes",
            },
            GraphEdge {
                from: "decision",
                to: "action",
                label: "permits",
            },
            GraphEdge {
                from: "action",
                to: "receipt",
                label: "must produce",
            },
        ],
    }
}

fn workspace_roots(tasks: &[OffdeskTask]) -> Vec<String> {
    let mut roots = BTreeSet::new();
    for task in tasks {
        if !task.workdir.trim().is_empty() {
            roots.insert(operator_safe_text(&task.workdir));
        }
    }
    roots.into_iter().take(12).collect()
}

fn decision_is_open(status: DecisionStatus) -> bool {
    matches!(
        status,
        DecisionStatus::Draft
            | DecisionStatus::CouncilReview
            | DecisionStatus::UserPending
            | DecisionStatus::Deferred
            | DecisionStatus::HandoffReady
    )
}

fn decision_severity(record: &DecisionRecord) -> &'static str {
    match record.status {
        DecisionStatus::UserPending | DecisionStatus::HandoffReady => "attention",
        DecisionStatus::CouncilReview | DecisionStatus::Deferred => "blocked",
        DecisionStatus::Draft => "info",
        _ => "ok",
    }
}

fn update_latest(target: &mut Option<DateTime<Utc>>, candidate: DateTime<Utc>) {
    if target.map_or(true, |current| candidate > current) {
        *target = Some(candidate);
    }
}

fn severity_rank(severity: &str) -> u8 {
    match severity {
        "critical" => 0,
        "blocked" => 1,
        "attention" => 2,
        "info" => 3,
        "ok" => 4,
        _ => 5,
    }
}

fn label_from_kind(kind: &str) -> String {
    kind.split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().chain(chars).collect::<String>(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn workstation_id() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .map(|value| operator_safe_text(&value))
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "workstation-default".to_string())
}

fn telegram_loop_status_path() -> Option<PathBuf> {
    dirs::home_dir().map(|home| {
        home.join(".cache")
            .join("forager")
            .join("remote_operator_telegram_loop.json")
    })
}

fn value_usize(value: &Value, key: &str) -> usize {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or_default()
}

fn number_at(value: &Value, pointer: &str) -> Option<usize> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .and_then(|value| usize::try_from(value).ok())
}

fn text_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn text_at<'a>(value: &'a Value, pointer: &str) -> Option<&'a str> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn push_summary_line(output: &mut String, label: &str, value: Option<&str>) {
    if let Some(value) = value {
        output.push_str(&format!("  {label}: {value}\n"));
    }
}
