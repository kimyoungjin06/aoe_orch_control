//! One-shot offdesk control loop.

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use super::adaptive_wiki::{
    build_usage_records_with_policy, AdaptiveWikiStore, AdaptiveWikiUsageContext,
};
use super::approval::{
    ActionApprovalMetadata, ActionApprovalRequest, ApprovalLedger, ApprovalLedgerSession,
    ApprovalStatus, ProviderFallbackApprovalMetadata, RiskLevel,
};
use super::background::{BackgroundProbe, BackgroundRunStore, BackgroundRunnerPhase};
use super::redaction::operator_safe_text;
use super::resume::{ResumeEvidence, ResumePendingInput, TaskResumeState, TaskResumeStore};
use super::runner::{
    launch_background_command_with_gate_outcome, poll_background_runs, BackgroundLaunchOutcome,
    BackgroundLaunchRequest, BackgroundPollOutcome, LocalCommandLaunchSpec,
};
use super::scheduler::{
    is_provider_capacity_block, SchedulerGate, SchedulerGateRequest, SchedulerGateStatus,
};
use super::task_queue::{
    count_tasks, status_next_safe_actions_from_summary, tick_next_safe_actions_from_report,
    OffdeskCloseoutStateSummary, OffdeskNextSafeAction, OffdeskStatusNextSafeActionInput,
    OffdeskTask, OffdeskTaskCounts, OffdeskTaskStatus, OffdeskTaskStore, OffdeskTickReportInput,
};
use super::tick_lock::OffdeskTickLockGuard;
use super::{
    recommend_provider_fallback, ProviderCapacityStore, ProviderFallbackCandidate,
    ProviderFallbackRecommendation,
};

const PROVIDER_FALLBACK_ACTION: &str = "dispatch.provider_fallback";
const PROVIDER_FALLBACK_CANDIDATE_LIMIT: usize = 3;

#[derive(Debug, Clone)]
pub struct OffdeskTickOptions {
    pub limit: usize,
    pub now: DateTime<Utc>,
    pub notification_cooldown: Option<Duration>,
    pub lock_stale_after: Duration,
    pub project_key: Option<String>,
    pub task_id: Option<String>,
}

impl OffdeskTickOptions {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            limit: 10,
            now,
            notification_cooldown: None,
            lock_stale_after: Duration::minutes(30),
            project_key: None,
            task_id: None,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct OffdeskTickReport {
    pub expired_approvals: usize,
    pub polled_background: usize,
    pub launched: usize,
    pub pending_approval: usize,
    pub completed: usize,
    pub failed: usize,
    pub resume_pending: usize,
    pub provider_deferred: usize,
    pub provider_retargeted: usize,
    pub skipped: usize,
    pub stale_lock_replaced: bool,
    /// True when a global operator pause held back new dispatch this tick.
    #[serde(default)]
    pub dispatch_paused: bool,
    /// Number of dispatch-ready tasks held back by the operator pause.
    #[serde(default)]
    pub held: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pause_reason: Option<String>,
    /// Adaptive-wiki learning candidates emitted from this tick's observed
    /// denials, failures, and resume-recovery rows (recommendation-only).
    #[serde(default)]
    pub learning_signals_emitted: usize,
    pub updated_task_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub next_safe_actions: Vec<OffdeskNextSafeAction>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct OffdeskStatusSummary {
    pub pending_approvals: usize,
    pub tasks: OffdeskTaskCounts,
    pub background_active: usize,
    pub background_stale: usize,
    pub background_failed: usize,
    pub closeout_required: usize,
    pub closeout_state: OffdeskCloseoutStateSummary,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub next_safe_actions: Vec<OffdeskNextSafeAction>,
}

pub fn run_offdesk_tick(
    profile_dir: impl AsRef<Path>,
    options: OffdeskTickOptions,
) -> Result<OffdeskTickReport> {
    let profile_dir = profile_dir.as_ref();
    let tick_lock =
        OffdeskTickLockGuard::acquire(profile_dir, options.now, options.lock_stale_after)?;
    let approval_ledger = ApprovalLedger::new(profile_dir);
    let background_store = BackgroundRunStore::new(profile_dir);
    let task_store = OffdeskTaskStore::new(profile_dir);
    let resume_store = TaskResumeStore::new(profile_dir);
    let provider_capacity_store = ProviderCapacityStore::new(profile_dir);
    let adaptive_wiki_store = AdaptiveWikiStore::new(profile_dir);
    let gate = SchedulerGate::with_provider_capacity(
        approval_ledger.clone(),
        provider_capacity_store.clone(),
    )
    .with_adaptive_wiki(adaptive_wiki_store.clone());
    let (mut approval_session, expired) = approval_ledger.begin_session(options.now)?;
    let mut tasks = task_store.load()?;
    let scoped_ticket_id = task_scoped_background_ticket_id(&tasks, &options);
    let background_outcomes = match scoped_ticket_id {
        ScopedBackgroundTicket::All => poll_background_runs(
            &background_store,
            None,
            options.now,
            options.notification_cooldown,
        )?,
        ScopedBackgroundTicket::One(ticket_id) => poll_background_runs(
            &background_store,
            Some(&ticket_id),
            options.now,
            options.notification_cooldown,
        )?,
        ScopedBackgroundTicket::None => Vec::new(),
    };
    let background_by_ticket = background_outcomes
        .iter()
        .map(|outcome| (outcome.probe.ticket_id.clone(), outcome))
        .collect::<HashMap<_, _>>();

    let operator_pause =
        crate::offdesk::operator_pause::OperatorPauseStore::new(profile_dir).load()?;

    let mut report = OffdeskTickReport {
        expired_approvals: expired.len(),
        polled_background: background_outcomes.len(),
        stale_lock_replaced: tick_lock.stale_metadata_replaced(),
        dispatch_paused: operator_pause.paused,
        pause_reason: operator_pause.reason.clone(),
        ..OffdeskTickReport::default()
    };

    for task in tasks.iter_mut() {
        if !task_matches_tick_filter(task, &options) {
            continue;
        }
        apply_background_outcome(
            task,
            &background_by_ticket,
            &resume_store,
            options.now,
            &mut report,
        )?;
    }
    apply_approved_provider_fallbacks(
        &mut tasks,
        &mut approval_session,
        &provider_capacity_store,
        options.now,
        &mut report,
        options.project_key.as_deref(),
        options.task_id.as_deref(),
    )?;

    let mut dispatched = 0usize;
    for task in tasks.iter_mut() {
        if !task_matches_tick_filter(task, &options) {
            continue;
        }
        // A global operator pause halts all new dispatch: dispatch-ready tasks
        // stay queued and are reported as held. Background polling above still
        // runs, so existing work keeps being monitored.
        if operator_pause.paused {
            if task.can_dispatch_at(options.now) {
                report.held += 1;
            }
            continue;
        }
        if dispatched >= options.limit {
            if task.can_dispatch_at(options.now) {
                report.skipped += 1;
            }
            continue;
        }
        if !task.can_dispatch_at(options.now) {
            continue;
        }

        let before = task.status;
        if let Err(error) = dispatch_task(
            task,
            &gate,
            &mut approval_session,
            &background_store,
            &adaptive_wiki_store,
            options.now,
            &mut report,
        ) {
            approval_session.flush()?;
            return Err(error);
        }
        if task.status != before || matches!(task.status, OffdeskTaskStatus::PendingApproval) {
            dispatched += 1;
        }
    }

    approval_session.flush()?;
    refresh_tick_next_safe_actions(&mut report);
    task_store.save(&tasks)?;

    // Event-driven learning: translate this tick's observed denials, failures,
    // and resume-recovery rows into adaptive-wiki candidates (recommendation-only;
    // never auto-applied). A durable cursor emits each event once. This must never
    // fail the tick, so its error is logged and swallowed.
    match crate::offdesk::learning_signals::scan_and_emit_learning_signals(profile_dir, options.now)
    {
        Ok(scan) => report.learning_signals_emitted = scan.emitted_count(),
        Err(error) => tracing::warn!(error = %error, "learning signal scan failed"),
    }

    Ok(report)
}

enum ScopedBackgroundTicket {
    All,
    One(String),
    None,
}

fn task_matches_tick_filter(task: &OffdeskTask, options: &OffdeskTickOptions) -> bool {
    if let Some(project_key) = options.project_key.as_deref() {
        if task.project_key != project_key {
            return false;
        }
    }
    if let Some(task_id) = options.task_id.as_deref() {
        if task.task_id != task_id {
            return false;
        }
    }
    true
}

fn task_scoped_background_ticket_id(
    tasks: &[OffdeskTask],
    options: &OffdeskTickOptions,
) -> ScopedBackgroundTicket {
    if options.project_key.is_none() && options.task_id.is_none() {
        return ScopedBackgroundTicket::All;
    }
    let Some(task_id) = options.task_id.as_deref() else {
        return ScopedBackgroundTicket::All;
    };
    tasks
        .iter()
        .find(|task| task.task_id == task_id && task_matches_tick_filter(task, options))
        .and_then(|task| task.background_ticket_id.clone())
        .map(ScopedBackgroundTicket::One)
        .unwrap_or(ScopedBackgroundTicket::None)
}

pub fn reconcile_tasks_with_background_outcomes(
    profile_dir: impl AsRef<Path>,
    outcomes: &[BackgroundPollOutcome],
    now: DateTime<Utc>,
) -> Result<OffdeskTickReport> {
    let profile_dir = profile_dir.as_ref();
    let task_store = OffdeskTaskStore::new(profile_dir);
    let resume_store = TaskResumeStore::new(profile_dir);
    let background_by_ticket = outcomes
        .iter()
        .map(|outcome| (outcome.probe.ticket_id.clone(), outcome))
        .collect::<HashMap<_, _>>();

    let mut report = OffdeskTickReport {
        polled_background: outcomes.len(),
        ..OffdeskTickReport::default()
    };
    let mut tasks = task_store.load()?;
    for task in tasks.iter_mut() {
        apply_background_outcome(task, &background_by_ticket, &resume_store, now, &mut report)?;
    }
    refresh_tick_next_safe_actions(&mut report);
    if !report.updated_task_ids.is_empty() {
        task_store.save(&tasks)?;
    }
    Ok(report)
}

fn refresh_tick_next_safe_actions(report: &mut OffdeskTickReport) {
    report.next_safe_actions = tick_next_safe_actions_from_report(&OffdeskTickReportInput {
        expired_approvals: report.expired_approvals,
        polled_background: report.polled_background,
        launched: report.launched,
        pending_approval: report.pending_approval,
        completed: report.completed,
        failed: report.failed,
        resume_pending: report.resume_pending,
        provider_deferred: report.provider_deferred,
        skipped: report.skipped,
    });
}

pub fn load_offdesk_status_summary(
    profile_dir: impl AsRef<Path>,
    now: DateTime<Utc>,
) -> Result<OffdeskStatusSummary> {
    let profile_dir = profile_dir.as_ref();
    let approvals = ApprovalLedger::new(profile_dir).load()?;
    let tasks = OffdeskTaskStore::new(profile_dir).load()?;
    let backgrounds = BackgroundRunStore::new(profile_dir).load()?;

    let pending_approvals = approvals
        .iter()
        .filter(|approval| approval.status == ApprovalStatus::Pending && approval.expires_at >= now)
        .count();
    let closeout_state = summarize_closeout_state(profile_dir, &tasks);
    let mut summary = OffdeskStatusSummary {
        pending_approvals,
        tasks: count_tasks(&tasks),
        closeout_required: closeout_state.required_count(),
        closeout_state,
        ..OffdeskStatusSummary::default()
    };

    for probe in backgrounds {
        match probe.phase {
            BackgroundRunnerPhase::Failed => summary.background_failed += 1,
            BackgroundRunnerPhase::StaleNoAck
            | BackgroundRunnerPhase::StaleLostCallback
            | BackgroundRunnerPhase::Reconstructable => summary.background_stale += 1,
            BackgroundRunnerPhase::Completed
            | BackgroundRunnerPhase::ResultReceived
            | BackgroundRunnerPhase::RecoveryAcknowledged => {}
            BackgroundRunnerPhase::Launched
            | BackgroundRunnerPhase::HandoffEmitted
            | BackgroundRunnerPhase::PickupAcknowledged => summary.background_active += 1,
        }
    }
    summary.next_safe_actions =
        status_next_safe_actions_from_summary(&OffdeskStatusNextSafeActionInput {
            pending_approvals: summary.pending_approvals,
            tasks: summary.tasks.clone(),
            background_active: summary.background_active,
            background_stale: summary.background_stale,
            background_failed: summary.background_failed,
            closeout_required: summary.closeout_required,
            closeout_state: summary.closeout_state.clone(),
        });

    Ok(summary)
}

fn summarize_closeout_state(
    profile_dir: &Path,
    tasks: &[OffdeskTask],
) -> OffdeskCloseoutStateSummary {
    let closeouts = latest_closeout_generated_at_by_task(profile_dir);
    let reviews = latest_closeout_review_by_task(profile_dir);
    let mut summary = OffdeskCloseoutStateSummary::default();

    for task in tasks
        .iter()
        .filter(|task| task.status == OffdeskTaskStatus::Completed)
    {
        let key = (task.project_key.clone(), task.task_id.clone());
        let closeout_generated_at = closeouts.get(&key).copied();
        let review = reviews.get(&key);
        if let Some(review) = review.filter(|review| review.reviewed_at > task.updated_at) {
            apply_closeout_review_snapshot(&mut summary, review);
        } else if closeout_generated_at.is_some_and(|generated_at| generated_at > task.updated_at) {
            summary.pending_review += 1;
        } else if review.is_some() {
            summary.stale_review += 1;
        } else if closeout_generated_at.is_some() {
            summary.stale_closeout += 1;
        } else {
            summary.missing_closeout += 1;
        }
    }

    summary
}

fn apply_closeout_review_snapshot(
    summary: &mut OffdeskCloseoutStateSummary,
    review: &CloseoutReviewSnapshot,
) {
    match review.receipt_acceptance_status.as_deref() {
        Some("accepted") => {
            summary.accepted += 1;
            summary.approved += 1;
        }
        Some("approved_with_followups") => {
            summary.approved_with_followups += 1;
        }
        Some("revision_required" | "blocked") => {
            summary.revision_required += 1;
        }
        Some("retired_incomplete") => {
            summary.retired_incomplete += 1;
        }
        Some(_) => {
            summary.revision_required += 1;
        }
        None if review.verdict == "approved" => {
            summary.approved += 1;
        }
        None => {
            summary.revision_required += 1;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CloseoutReviewSnapshot {
    reviewed_at: DateTime<Utc>,
    verdict: String,
    receipt_acceptance_status: Option<String>,
}

fn latest_closeout_generated_at_by_task(
    profile_dir: &Path,
) -> HashMap<(String, String), DateTime<Utc>> {
    let closeouts_dir = profile_dir.join("offdesk_closeouts");
    let Ok(entries) = fs::read_dir(closeouts_dir) else {
        return HashMap::new();
    };
    let mut closeouts = HashMap::new();
    for (project_key, task_id, generated_at) in entries
        .filter_map(Result::ok)
        .filter_map(|entry| {
            let path = entry.path().join("closeout_plan.json");
            let content = fs::read_to_string(path).ok()?;
            let value: Value = serde_json::from_str(&content).ok()?;
            let generated_at = value
                .get("generated_at")
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))?;
            let tasks = value.get("tasks")?.as_array()?;
            Some(
                tasks
                    .iter()
                    .filter_map(move |task| {
                        Some((
                            task.get("project_key")?.as_str()?.to_string(),
                            task.get("task_id")?.as_str()?.to_string(),
                            generated_at,
                        ))
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .flatten()
    {
        closeouts
            .entry((project_key, task_id))
            .and_modify(|existing| {
                if generated_at > *existing {
                    *existing = generated_at;
                }
            })
            .or_insert(generated_at);
    }
    closeouts
}

fn latest_closeout_review_by_task(
    profile_dir: &Path,
) -> HashMap<(String, String), CloseoutReviewSnapshot> {
    let closeouts_dir = profile_dir.join("offdesk_closeouts");
    let Ok(entries) = fs::read_dir(closeouts_dir) else {
        return HashMap::new();
    };
    let mut reviews = HashMap::new();
    for (project_key, task_id, snapshot) in entries
        .filter_map(Result::ok)
        .flat_map(|entry| {
            fs::read_dir(entry.path())
                .ok()
                .into_iter()
                .flat_map(|entries| entries.filter_map(Result::ok))
        })
        .filter_map(|entry| {
            let path = entry.path();
            let filename = path.file_name()?.to_str()?;
            if !filename.starts_with("closeout_review_") || !filename.ends_with(".json") {
                return None;
            }
            let content = fs::read_to_string(path).ok()?;
            let value: Value = serde_json::from_str(&content).ok()?;
            let reviewed_at = value
                .get("reviewed_at")
                .and_then(Value::as_str)
                .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))?;
            let verdict = value.get("verdict")?.as_str()?.to_string();
            let receipt_acceptance_status = value
                .pointer("/closeout_receipt/acceptance_status")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned);
            let tasks = value.get("applies_to_tasks")?.as_array()?;
            Some(
                tasks
                    .iter()
                    .filter_map(move |task| {
                        Some((
                            task.get("project_key")?.as_str()?.to_string(),
                            task.get("task_id")?.as_str()?.to_string(),
                            CloseoutReviewSnapshot {
                                reviewed_at,
                                verdict: verdict.clone(),
                                receipt_acceptance_status: receipt_acceptance_status.clone(),
                            },
                        ))
                    })
                    .collect::<Vec<_>>(),
            )
        })
        .flatten()
    {
        reviews
            .entry((project_key, task_id))
            .and_modify(|existing: &mut CloseoutReviewSnapshot| {
                if snapshot.reviewed_at > existing.reviewed_at {
                    *existing = snapshot.clone();
                }
            })
            .or_insert(snapshot);
    }
    reviews
}

fn apply_background_outcome(
    task: &mut OffdeskTask,
    background_by_ticket: &HashMap<String, &BackgroundPollOutcome>,
    resume_store: &TaskResumeStore,
    now: DateTime<Utc>,
    report: &mut OffdeskTickReport,
) -> Result<()> {
    if !matches!(
        task.status,
        OffdeskTaskStatus::Launched | OffdeskTaskStatus::Running | OffdeskTaskStatus::ResumePending
    ) {
        return Ok(());
    }

    let Some(ticket_id) = task.background_ticket_id.as_deref() else {
        return Ok(());
    };
    let Some(outcome) = background_by_ticket.get(ticket_id) else {
        return Ok(());
    };

    match outcome.decision.phase {
        BackgroundRunnerPhase::Completed | BackgroundRunnerPhase::ResultReceived => {
            if task.status != OffdeskTaskStatus::Completed {
                task.status = OffdeskTaskStatus::Completed;
                task.last_error = None;
                task.updated_at = now;
                resume_store.clear_after_recovery(&task.project_key, &task.task_id, now)?;
                report.completed += 1;
                report.updated_task_ids.push(task.task_id.clone());
            }
        }
        BackgroundRunnerPhase::Failed
        | BackgroundRunnerPhase::StaleNoAck
        | BackgroundRunnerPhase::StaleLostCallback
        | BackgroundRunnerPhase::Reconstructable => {
            if task.status != OffdeskTaskStatus::ResumePending {
                let previous_status = task.status;
                task.status = OffdeskTaskStatus::ResumePending;
                task.last_error = Some(outcome.decision.evidence.clone());
                task.updated_at = now;
                write_resume_state(task, previous_status, outcome, resume_store, now)?;
                report.resume_pending += 1;
                report.updated_task_ids.push(task.task_id.clone());
            }
        }
        BackgroundRunnerPhase::RecoveryAcknowledged => {}
        BackgroundRunnerPhase::Launched
        | BackgroundRunnerPhase::HandoffEmitted
        | BackgroundRunnerPhase::PickupAcknowledged => {
            if task.status != OffdeskTaskStatus::Running {
                task.status = OffdeskTaskStatus::Running;
                task.updated_at = now;
                report.updated_task_ids.push(task.task_id.clone());
            }
        }
    }

    Ok(())
}

fn apply_approved_provider_fallbacks(
    tasks: &mut [OffdeskTask],
    approvals: &mut ApprovalLedgerSession,
    provider_capacity_store: &ProviderCapacityStore,
    now: DateTime<Utc>,
    report: &mut OffdeskTickReport,
    project_key: Option<&str>,
    task_id: Option<&str>,
) -> Result<()> {
    for approval in approvals.approved_provider_fallbacks(now) {
        if approval.action != PROVIDER_FALLBACK_ACTION {
            continue;
        }
        let Some(metadata) = approval
            .metadata
            .as_ref()
            .and_then(ActionApprovalMetadata::as_provider_fallback)
        else {
            continue;
        };
        let recommendation = recommend_provider_fallback(
            provider_capacity_store,
            &metadata.current_provider_id,
            metadata.current_model.as_deref(),
            "approved provider fallback revalidation",
            &metadata.runner_role,
            now,
        )?;
        let Some(candidate) =
            select_approved_provider_fallback_candidate(metadata, &recommendation)
        else {
            continue;
        };

        let mut applied = 0usize;
        for task in tasks.iter_mut().filter(|task| {
            project_key.is_none_or(|expected| task.project_key == expected)
                && task_id.is_none_or(|expected| task.task_id == expected)
                && provider_fallback_task_matches_scope(task, &approval, metadata)
        }) {
            task.provider_id = Some(candidate.provider_id.clone());
            task.model = candidate.model.clone();
            task.not_before = None;
            task.last_provider_fallback = Some(recommendation.clone());
            task.updated_at = now;
            applied += 1;
            report.updated_task_ids.push(task.task_id.clone());
        }

        if applied > 0 {
            report.provider_retargeted += applied;
            approvals.supersede_approval(&approval.approval_id, "provider_fallback_applied");
        }
    }

    Ok(())
}

fn select_approved_provider_fallback_candidate(
    metadata: &ProviderFallbackApprovalMetadata,
    recommendation: &ProviderFallbackRecommendation,
) -> Option<ProviderFallbackCandidate> {
    metadata.candidates.iter().find_map(|approved| {
        recommendation
            .candidates
            .iter()
            .find(|candidate| {
                candidate.recommended
                    && candidate.provider_id == approved.provider_id
                    && candidate.model == approved.model
            })
            .cloned()
    })
}

fn provider_fallback_task_matches_scope(
    task: &OffdeskTask,
    approval: &super::approval::PendingActionApproval,
    metadata: &ProviderFallbackApprovalMetadata,
) -> bool {
    matches!(
        task.status,
        OffdeskTaskStatus::Queued | OffdeskTaskStatus::PendingApproval
    ) && task.project_key == approval.project_key
        && task.request_id == approval.request_id
        && task.provider_id.as_deref() == Some(metadata.current_provider_id.as_str())
        && task.model.as_deref() == metadata.current_model.as_deref()
}

fn ensure_provider_fallback_approval(
    task: &OffdeskTask,
    fallback: Option<&ProviderFallbackRecommendation>,
    approvals: &mut ApprovalLedgerSession,
    now: DateTime<Utc>,
) -> Result<()> {
    let Some(fallback) = fallback else {
        return Ok(());
    };
    let Some(metadata) = ActionApprovalMetadata::provider_fallback_from_recommendation(
        fallback,
        "worker",
        PROVIDER_FALLBACK_CANDIDATE_LIMIT,
    ) else {
        return Ok(());
    };

    let mut request = ActionApprovalRequest::new(
        &task.project_key,
        &task.request_id,
        &task.task_id,
        PROVIDER_FALLBACK_ACTION,
        RiskLevel::RuntimeMutation,
    );
    request.mutation_class = Some(PROVIDER_FALLBACK_ACTION.to_string());
    request.preview = format!(
        "Retarget provider/model for request {} from {} {} using an approved fallback candidate",
        task.request_id,
        fallback.current_provider_id,
        fallback.current_model.as_deref().unwrap_or("-")
    );
    request.reason =
        "provider capacity cooldown active; provider/model fallback needs operator approval"
            .to_string();
    request.source_surface = "offdesk.tick".to_string();
    request.metadata = Some(metadata);

    approvals.ensure_pending_without_consuming_grant(request, now)?;
    Ok(())
}

fn dispatch_task(
    task: &mut OffdeskTask,
    gate: &SchedulerGate,
    approvals: &mut ApprovalLedgerSession,
    background_store: &BackgroundRunStore,
    adaptive_wiki_store: &AdaptiveWikiStore,
    now: DateTime<Utc>,
    report: &mut OffdeskTickReport,
) -> Result<()> {
    let mut gate_request = SchedulerGateRequest::new(
        task.capability_id.clone(),
        task.project_key.clone(),
        task.request_id.clone(),
        task.task_id.clone(),
    );
    gate_request.mutation_class = task
        .mutation_class
        .clone()
        .or_else(|| Some(task.capability_id.clone()));
    gate_request.artifact_refs = task.artifact_refs.clone();
    gate_request.preview = if task.preview.trim().is_empty() {
        task.command.clone()
    } else {
        task.preview.clone()
    };
    gate_request.reason = if task.reason.trim().is_empty() {
        "offdesk task requires operator approval".to_string()
    } else {
        task.reason.clone()
    };
    gate_request.source_surface = "offdesk.tick".to_string();
    gate_request.provider_id = task.provider_id.clone();
    gate_request.model = task.model.clone();
    gate_request.artifact_kind = task.artifact_kind.clone();
    gate_request.agent_mode = task.agent_mode;

    let mut launch_request = BackgroundLaunchRequest::new(gate_request, task.runner_kind);
    launch_request.ticket_id = task.background_ticket_id.clone();
    launch_request.launch_spec_summary = Some(task.command.clone());
    launch_request.implementation_packet = task.implementation_packet.clone();
    launch_request.runtime_handle_alive = true;

    let mut command_spec = LocalCommandLaunchSpec::new(&task.command, PathBuf::from(&task.workdir));
    command_spec.log_artifact_path = task.log_artifact_path.as_ref().map(PathBuf::from);
    command_spec.result_artifact_path = task.result_artifact_path.as_ref().map(PathBuf::from);

    let gate_outcome = gate.evaluate_with_session(
        launch_request.gate_request.clone(),
        task.execution_brief.as_ref(),
        now,
        approvals,
    )?;
    let outcome = launch_background_command_with_gate_outcome(
        background_store,
        launch_request,
        gate_outcome,
        now,
        command_spec,
    )?;
    task.last_gate_status = Some(outcome.gate.status);
    match outcome.gate.status {
        SchedulerGateStatus::Proceed => {
            if let Some(probe) = outcome.probe.as_ref() {
                task.status = OffdeskTaskStatus::Launched;
                task.background_ticket_id = Some(probe.ticket_id.clone());
                task.attempt_count += 1;
                task.last_error = None;
                task.last_provider_fallback = None;
                task.last_adaptive_wiki_entry_ids = probe.adaptive_wiki_entry_ids.clone();
                append_adaptive_wiki_usage_for_task(task, &outcome, adaptive_wiki_store, now)?;
                task.updated_at = now;
                report.launched += 1;
                report.updated_task_ids.push(task.task_id.clone());
            }
        }
        SchedulerGateStatus::PendingApproval => {
            task.status = OffdeskTaskStatus::PendingApproval;
            task.last_provider_fallback = None;
            task.last_adaptive_wiki_entry_ids.clear();
            task.updated_at = now;
            report.pending_approval += 1;
            report.updated_task_ids.push(task.task_id.clone());
        }
        SchedulerGateStatus::Denied | SchedulerGateStatus::Blocked => {
            if is_provider_capacity_block(&outcome.gate) {
                let fallback = outcome.gate.provider_fallback.clone();
                task.status = OffdeskTaskStatus::Queued;
                task.not_before = outcome.gate.retry_at;
                task.last_error = Some(outcome.gate.reason);
                task.last_provider_fallback = fallback.clone();
                task.last_adaptive_wiki_entry_ids.clear();
                task.updated_at = now;
                ensure_provider_fallback_approval(task, fallback.as_ref(), approvals, now)?;
                report.provider_deferred += 1;
                report.updated_task_ids.push(task.task_id.clone());
            } else {
                task.status = OffdeskTaskStatus::Failed;
                task.last_error = Some(outcome.gate.reason);
                task.last_provider_fallback = None;
                task.last_adaptive_wiki_entry_ids.clear();
                task.updated_at = now;
                report.failed += 1;
                report.updated_task_ids.push(task.task_id.clone());
            }
        }
    }

    Ok(())
}

fn append_adaptive_wiki_usage_for_task(
    task: &OffdeskTask,
    outcome: &BackgroundLaunchOutcome,
    adaptive_wiki_store: &AdaptiveWikiStore,
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
            task_id: &task.task_id,
            request_id: &task.request_id,
            project_key: &task.project_key,
            artifact_kind: task.artifact_kind.as_deref(),
            agent_mode: task.agent_mode,
            projection_kind: "runtime_probe",
            projection_policy: Some(outcome.gate.adaptive_wiki_runtime_policy),
            now,
        },
    );
    adaptive_wiki_store.append_usage_records(&records)?;
    Ok(())
}

fn write_resume_state(
    task: &OffdeskTask,
    previous_status: OffdeskTaskStatus,
    outcome: &BackgroundPollOutcome,
    resume_store: &TaskResumeStore,
    now: DateTime<Utc>,
) -> Result<()> {
    let decision = &outcome.decision;
    let mut state = TaskResumeState::mark_pending(ResumePendingInput {
        task_id: task.task_id.clone(),
        request_id: task.request_id.clone(),
        project_key: task.project_key.clone(),
        phase: format!("{:?}", decision.phase).to_lowercase(),
        runner_target: format!("{:?}", task.runner_kind).to_lowercase(),
        interruption_reason: decision.evidence.clone(),
        interrupted_at: now,
        fresh_until: now + Duration::minutes(30),
    });
    state.background_ticket_id = task.background_ticket_id.clone();
    state.last_task_status = Some(format!("{:?}", previous_status).to_lowercase());
    state.attempt_count = task.attempt_count;
    state.last_durable_action = Some("background runner poll".to_string());
    state.last_evidence_artifacts = outcome
        .probe
        .log_artifact_path
        .iter()
        .chain(outcome.probe.result_artifact_path.iter())
        .cloned()
        .collect();
    state.last_log_tail = decision.last_log_tail.as_deref().map(operator_safe_text);
    state.evidence = resume_evidence_from_background(&outcome.probe, decision, now);
    state.next_safe_resume_step =
        "inspect background result sidecar and logs before retrying".to_string();
    resume_store.mark_resume_pending(state)
}

fn resume_evidence_from_background(
    probe: &BackgroundProbe,
    decision: &super::background::BackgroundRecoveryDecision,
    now: DateTime<Utc>,
) -> Vec<ResumeEvidence> {
    let mut evidence = vec![ResumeEvidence::new(
        "background_probe",
        operator_safe_text(&format!(
            "{}: {}",
            format!("{:?}", decision.phase).to_lowercase(),
            decision.evidence
        )),
        now,
    )];

    if let Some(path) = probe.log_artifact_path.as_deref() {
        evidence.push(ResumeEvidence::artifact(
            "log_artifact",
            operator_safe_text(path),
            probe.log_artifact_present,
            now,
        ));
    }
    if let Some(path) = probe.result_artifact_path.as_deref() {
        evidence.push(ResumeEvidence::artifact(
            "result_artifact",
            operator_safe_text(path),
            probe.result_artifact_present,
            now,
        ));
    }
    if let Some(tail) = decision.last_log_tail.as_deref() {
        evidence.push(ResumeEvidence::new(
            "log_tail",
            operator_safe_text(tail),
            now,
        ));
    }

    evidence
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offdesk::{
        ApprovalMode, ApprovalScope, BackgroundProbe, BackgroundRunnerKind, ExecutionBrief,
        OffdeskTaskInput, PendingActionApproval, ProviderCapacityState, ProviderCapacityStatus,
        ProviderErrorReason, ProviderFallbackApplyScope, ProviderFallbackAuthStatus,
        ProviderFallbackSource,
    };
    use serial_test::serial;
    use tempfile::tempdir;

    struct EnvGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl EnvGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(previous) = self.previous.as_ref() {
                std::env::set_var(self.key, previous);
            } else {
                std::env::remove_var(self.key);
            }
        }
    }

    fn task_input(now: DateTime<Utc>, command: &str) -> OffdeskTaskInput {
        OffdeskTaskInput {
            task_id: Some("task".to_string()),
            request_id: "request".to_string(),
            project_key: "project".to_string(),
            capability_id: "dispatch.runtime".to_string(),
            runner_kind: BackgroundRunnerKind::LocalBackground,
            command: command.to_string(),
            workdir: "/tmp".to_string(),
            execution_brief: Some(ExecutionBrief {
                request_id: "request".to_string(),
                task_id: "task".to_string(),
                project_key: "project".to_string(),
                approved: true,
                allowed_runtime_mutations: vec!["dispatch.runtime".to_string()],
                allowed_canonical_mutations: vec![],
                fresh_until: Some(now + Duration::minutes(10)),
            }),
            not_before: None,
            mutation_class: None,
            artifact_refs: Vec::new(),
            implementation_packet: None,
            artifact_kind: None,
            agent_mode: None,
            provider_id: None,
            model: None,
            preview: String::new(),
            reason: String::new(),
            log_artifact_path: None,
            result_artifact_path: None,
        }
    }

    fn completed_task(task_id: &str, updated_at: DateTime<Utc>) -> OffdeskTask {
        let mut input = task_input(updated_at, "true");
        input.task_id = Some(task_id.to_string());
        let mut task = OffdeskTask::new(input, updated_at);
        task.status = OffdeskTaskStatus::Completed;
        task
    }

    fn write_closeout_plan(
        profile_dir: &Path,
        dir_name: &str,
        task_id: &str,
        generated_at: DateTime<Utc>,
    ) -> Result<()> {
        let dir = profile_dir.join("offdesk_closeouts").join(dir_name);
        fs::create_dir_all(&dir)?;
        fs::write(
            dir.join("closeout_plan.json"),
            serde_json::to_string_pretty(&serde_json::json!({
                "generated_at": generated_at,
                "closeout_id": format!("closeout_{task_id}"),
                "tasks": [
                    {
                        "project_key": "project",
                        "request_id": "request",
                        "task_id": task_id
                    }
                ]
            }))?,
        )?;
        Ok(())
    }

    fn write_closeout_review(
        profile_dir: &Path,
        dir_name: &str,
        task_id: &str,
        reviewed_at: DateTime<Utc>,
        verdict: &str,
        receipt_status: Option<&str>,
    ) -> Result<()> {
        let dir = profile_dir.join("offdesk_closeouts").join(dir_name);
        fs::create_dir_all(&dir)?;
        let mut review = serde_json::json!({
            "reviewed_at": reviewed_at,
            "verdict": verdict,
            "applies_to_tasks": [
                {
                    "project_key": "project",
                    "request_id": "request",
                    "task_id": task_id
                }
            ]
        });
        if let Some(status) = receipt_status {
            review["closeout_receipt"] = serde_json::json!({
                "schema": "closeout_receipt.v1",
                "acceptance_status": status
            });
        }
        fs::write(
            dir.join(format!(
                "closeout_review_{}.json",
                reviewed_at.format("%Y%m%dT%H%M%SZ")
            )),
            serde_json::to_string_pretty(&review)?,
        )?;
        Ok(())
    }

    fn fallback_candidate() -> ProviderFallbackCandidate {
        ProviderFallbackCandidate {
            provider_id: "openai".to_string(),
            model: Some("gpt-4.1-mini".to_string()),
            source: ProviderFallbackSource::SameProviderModel,
            auth_status: ProviderFallbackAuthStatus::Available,
            capacity_status: ProviderCapacityStatus::Available,
            recommended: true,
            reason: "same provider fallback model".to_string(),
        }
    }

    fn approved_provider_fallback(now: DateTime<Utc>) -> PendingActionApproval {
        PendingActionApproval {
            approval_id: "approval_provider_fallback".to_string(),
            action_id: "action_provider_fallback".to_string(),
            status: ApprovalStatus::Approved,
            scope: ApprovalScope::Once,
            project_key: "project".to_string(),
            request_id: "request".to_string(),
            task_id: "task".to_string(),
            action: PROVIDER_FALLBACK_ACTION.to_string(),
            risk_level: RiskLevel::RuntimeMutation,
            approval_mode: ApprovalMode::OperatorRequired,
            preview: String::new(),
            reason: String::new(),
            created_at: now,
            expires_at: now + Duration::minutes(10),
            resolved_at: Some(now),
            resolved_by: Some("operator".to_string()),
            source_surface: "test".to_string(),
            metadata: Some(ActionApprovalMetadata::ProviderFallback(Box::new(
                ProviderFallbackApprovalMetadata {
                    current_provider_id: "openai".to_string(),
                    current_model: Some("gpt-4.1".to_string()),
                    runner_role: "worker".to_string(),
                    generated_at: now,
                    candidate_limit: 3,
                    candidates: vec![fallback_candidate()],
                    apply_scope: ProviderFallbackApplyScope::RequestMatchingProviderModel,
                    approval_brief: None,
                },
            ))),
        }
    }

    #[test]
    fn closeout_state_distinguishes_missing_pending_revision_stale_and_approved() -> Result<()> {
        let temp = tempdir()?;
        let now = Utc::now();
        let older = now - Duration::minutes(10);
        let newer = now + Duration::minutes(10);
        let tasks = vec![
            completed_task("missing", now),
            completed_task("pending", now),
            completed_task("revision", now),
            completed_task("legacy-approved", now),
            completed_task("accepted", now),
            completed_task("followup", now),
            completed_task("stale-package", now),
            completed_task("stale-review", now),
        ];

        write_closeout_plan(temp.path(), "pending", "pending", newer)?;
        write_closeout_review(temp.path(), "revision", "revision", newer, "revise", None)?;
        write_closeout_review(
            temp.path(),
            "legacy-approved",
            "legacy-approved",
            newer,
            "approved",
            None,
        )?;
        write_closeout_review(
            temp.path(),
            "accepted",
            "accepted",
            newer,
            "approved",
            Some("accepted"),
        )?;
        write_closeout_review(
            temp.path(),
            "followup",
            "followup",
            newer,
            "approved",
            Some("approved_with_followups"),
        )?;
        write_closeout_plan(temp.path(), "stale-package", "stale-package", older)?;
        write_closeout_review(
            temp.path(),
            "stale-review",
            "stale-review",
            older,
            "approved",
            Some("accepted"),
        )?;

        let summary = summarize_closeout_state(temp.path(), &tasks);

        assert_eq!(summary.missing_closeout, 1);
        assert_eq!(summary.pending_review, 1);
        assert_eq!(summary.revision_required, 1);
        assert_eq!(summary.approved_with_followups, 1);
        assert_eq!(summary.accepted, 1);
        assert_eq!(summary.approved, 2);
        assert_eq!(summary.stale_closeout, 1);
        assert_eq!(summary.stale_review, 1);
        assert_eq!(summary.required_count(), 6);
        Ok(())
    }

    #[test]
    fn tick_marks_stale_background_as_resume_pending() -> Result<()> {
        let temp = tempdir()?;
        let now = Utc::now();
        let store = OffdeskTaskStore::new(temp.path());
        let mut task = OffdeskTask::new(task_input(now, "true"), now);
        task.status = OffdeskTaskStatus::Running;
        task.background_ticket_id = Some("ticket".to_string());
        store.enqueue(task)?;

        let mut probe = BackgroundProbe::new("ticket", BackgroundRunnerKind::LocalBackground);
        probe.runtime_handle_alive = false;
        BackgroundRunStore::new(temp.path()).upsert(probe)?;

        let report = run_offdesk_tick(temp.path(), OffdeskTickOptions::new(now))?;

        assert_eq!(report.resume_pending, 1);
        assert_eq!(store.load()?[0].status, OffdeskTaskStatus::ResumePending);
        assert_eq!(
            TaskResumeStore::new(temp.path()).load()?[0].status,
            crate::offdesk::ResumeStatus::ResumePending
        );
        Ok(())
    }

    #[test]
    fn tick_defers_provider_capacity_block_without_failing_task() -> Result<()> {
        let temp = tempdir()?;
        let now = Utc::now();
        let retry_at = now + Duration::minutes(3);
        ProviderCapacityStore::new(temp.path()).upsert(ProviderCapacityState {
            provider_id: "openai".to_string(),
            model: Some("gpt-4.1".to_string()),
            status: ProviderCapacityStatus::CoolingDown,
            reason: ProviderErrorReason::RateLimit,
            cooldown_until: Some(retry_at),
            last_error_summary: Some("rate limit".to_string()),
            updated_at: now,
        })?;
        ProviderCapacityStore::new(temp.path()).upsert(ProviderCapacityState {
            provider_id: "openai".to_string(),
            model: Some("gpt-4.1-mini".to_string()),
            status: ProviderCapacityStatus::Blocked,
            reason: ProviderErrorReason::RateLimit,
            cooldown_until: None,
            last_error_summary: Some("blocked".to_string()),
            updated_at: now,
        })?;
        ProviderCapacityStore::new(temp.path()).upsert(ProviderCapacityState {
            provider_id: "anthropic".to_string(),
            model: None,
            status: ProviderCapacityStatus::Blocked,
            reason: ProviderErrorReason::RateLimit,
            cooldown_until: None,
            last_error_summary: Some("blocked".to_string()),
            updated_at: now,
        })?;

        let store = OffdeskTaskStore::new(temp.path());
        let mut task = OffdeskTask::new(task_input(now, "true"), now);
        task.provider_id = Some("openai".to_string());
        task.model = Some("gpt-4.1".to_string());
        store.enqueue(task)?;

        let report = run_offdesk_tick(temp.path(), OffdeskTickOptions::new(now))?;
        let task = store.load()?.remove(0);

        assert_eq!(report.provider_deferred, 1);
        assert_eq!(report.failed, 0);
        assert_eq!(report.pending_approval, 0);
        assert_eq!(task.status, OffdeskTaskStatus::Queued);
        assert_eq!(task.not_before, Some(retry_at));
        assert_eq!(task.last_gate_status, Some(SchedulerGateStatus::Blocked));
        let fallback = task.last_provider_fallback.as_ref().expect("fallback");
        assert_eq!(fallback.current_provider_id, "openai");
        assert_eq!(fallback.current_model.as_deref(), Some("gpt-4.1"));
        assert_eq!(ApprovalLedger::new(temp.path()).load()?.len(), 0);
        assert!(BackgroundRunStore::new(temp.path()).load()?.is_empty());
        Ok(())
    }

    #[test]
    #[serial]
    fn tick_operator_pause_holds_new_dispatch_then_resumes() -> Result<()> {
        let temp = tempdir()?;
        let now = Utc::now();
        let store = OffdeskTaskStore::new(temp.path());
        store.enqueue(OffdeskTask::new(task_input(now, "true"), now))?;

        // A global operator pause holds new dispatch: the task stays queued.
        let pause = crate::offdesk::operator_pause::OperatorPauseStore::new(temp.path());
        pause.pause(Some("emergency"), Some("test"), now)?;
        let report = run_offdesk_tick(temp.path(), OffdeskTickOptions::new(now))?;
        assert!(report.dispatch_paused);
        assert_eq!(report.held, 1);
        assert_eq!(report.launched, 0);
        assert_eq!(report.pause_reason.as_deref(), Some("emergency"));
        assert_eq!(store.load()?.remove(0).status, OffdeskTaskStatus::Queued);
        assert!(BackgroundRunStore::new(temp.path()).load()?.is_empty());

        // Resuming releases dispatch; the task launches on the next tick.
        pause.resume(Some("test"), now)?;
        let report = run_offdesk_tick(temp.path(), OffdeskTickOptions::new(now))?;
        assert!(!report.dispatch_paused);
        assert_eq!(report.held, 0);
        assert_eq!(report.launched, 1);
        assert_eq!(store.load()?.remove(0).status, OffdeskTaskStatus::Launched);
        Ok(())
    }

    #[test]
    #[serial]
    fn approved_provider_fallback_retargets_only_provider_fields_before_dispatch() -> Result<()> {
        let _openai_auth = EnvGuard::set("OPENAI_API_KEY", "sk-test-provider-fallback");
        let temp = tempdir()?;
        let now = Utc::now();
        let retry_at = now + Duration::minutes(3);
        ProviderCapacityStore::new(temp.path()).upsert(ProviderCapacityState {
            provider_id: "openai".to_string(),
            model: Some("gpt-4.1".to_string()),
            status: ProviderCapacityStatus::CoolingDown,
            reason: ProviderErrorReason::RateLimit,
            cooldown_until: Some(retry_at),
            last_error_summary: Some("rate limit".to_string()),
            updated_at: now,
        })?;
        let store = OffdeskTaskStore::new(temp.path());
        let mut task = OffdeskTask::new(task_input(now, "true"), now);
        task.provider_id = Some("openai".to_string());
        task.model = Some("gpt-4.1".to_string());
        task.not_before = Some(retry_at);
        task.last_error = Some("provider capacity cooldown active".to_string());
        task.last_gate_status = Some(SchedulerGateStatus::Blocked);
        store.enqueue(task)?;
        ApprovalLedger::new(temp.path()).save(&[approved_provider_fallback(now)])?;

        let mut options = OffdeskTickOptions::new(now);
        options.limit = 0;
        let report = run_offdesk_tick(temp.path(), options)?;
        let task = store.load()?.remove(0);
        let approvals = ApprovalLedger::new(temp.path()).load()?;

        assert_eq!(report.provider_retargeted, 1);
        assert_eq!(report.launched, 0);
        assert_eq!(task.provider_id.as_deref(), Some("openai"));
        assert_eq!(task.model.as_deref(), Some("gpt-4.1-mini"));
        assert!(task.not_before.is_none());
        assert_eq!(
            task.last_error.as_deref(),
            Some("provider capacity cooldown active")
        );
        assert_eq!(task.last_gate_status, Some(SchedulerGateStatus::Blocked));
        assert_eq!(task.command, "true");
        assert_eq!(task.workdir, "/tmp");
        assert!(task.last_provider_fallback.is_some());
        assert_eq!(approvals[0].status, ApprovalStatus::Superseded);
        Ok(())
    }

    #[test]
    fn approved_provider_fallback_keeps_approval_when_revalidation_has_no_valid_candidate(
    ) -> Result<()> {
        let temp = tempdir()?;
        let now = Utc::now();
        let retry_at = now + Duration::minutes(3);
        let capacity_store = ProviderCapacityStore::new(temp.path());
        capacity_store.upsert(ProviderCapacityState {
            provider_id: "openai".to_string(),
            model: Some("gpt-4.1".to_string()),
            status: ProviderCapacityStatus::CoolingDown,
            reason: ProviderErrorReason::RateLimit,
            cooldown_until: Some(retry_at),
            last_error_summary: Some("rate limit".to_string()),
            updated_at: now,
        })?;
        capacity_store.upsert(ProviderCapacityState {
            provider_id: "openai".to_string(),
            model: Some("gpt-4.1-mini".to_string()),
            status: ProviderCapacityStatus::Blocked,
            reason: ProviderErrorReason::RateLimit,
            cooldown_until: None,
            last_error_summary: Some("blocked".to_string()),
            updated_at: now,
        })?;
        let store = OffdeskTaskStore::new(temp.path());
        let mut task = OffdeskTask::new(task_input(now, "true"), now);
        task.provider_id = Some("openai".to_string());
        task.model = Some("gpt-4.1".to_string());
        task.not_before = Some(retry_at);
        task.last_error = Some("provider capacity cooldown active".to_string());
        task.last_gate_status = Some(SchedulerGateStatus::Blocked);
        store.enqueue(task)?;
        ApprovalLedger::new(temp.path()).save(&[approved_provider_fallback(now)])?;

        let report = run_offdesk_tick(temp.path(), OffdeskTickOptions::new(now))?;
        let task = store.load()?.remove(0);
        let approvals = ApprovalLedger::new(temp.path()).load()?;

        assert_eq!(report.provider_retargeted, 0);
        assert_eq!(report.launched, 0);
        assert_eq!(task.provider_id.as_deref(), Some("openai"));
        assert_eq!(task.model.as_deref(), Some("gpt-4.1"));
        assert_eq!(task.not_before, Some(retry_at));
        assert_eq!(
            task.last_error.as_deref(),
            Some("provider capacity cooldown active")
        );
        assert_eq!(task.last_gate_status, Some(SchedulerGateStatus::Blocked));
        assert_eq!(task.command, "true");
        assert_eq!(task.workdir, "/tmp");
        assert_eq!(approvals[0].status, ApprovalStatus::Approved);
        Ok(())
    }
}
