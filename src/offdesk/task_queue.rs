//! Durable offdesk task queue.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use super::adaptive_wiki::AdaptiveWikiAgentMode;
use super::approval::{ApprovalStatus, ExecutionBrief, PendingActionApproval};
use super::background::{
    BackgroundProbe, BackgroundRecoveryDecision, BackgroundRunnerKind, BackgroundRunnerPhase,
};
use super::capability::CapabilityArtifactRef;
use super::implementation_packet::{
    operator_safe_implementation_packet_summary, ImplementationPacketSummary,
};
use super::mode_contract::{assess_offdesk_mode, OffdeskModeAssessment, OffdeskModeLifecycle};
use super::provider::{ProviderFallbackCandidate, ProviderFallbackRecommendation};
use super::redaction::operator_safe_text;
use super::resume::TaskResumeStore;
use super::scheduler::SchedulerGateStatus;

const TASKS_FILE: &str = "offdesk_tasks.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OffdeskTaskStatus {
    Queued,
    PendingApproval,
    Launched,
    Running,
    Completed,
    Failed,
    ResumePending,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffdeskTask {
    pub task_id: String,
    pub request_id: String,
    pub project_key: String,
    pub status: OffdeskTaskStatus,
    pub capability_id: String,
    pub runner_kind: BackgroundRunnerKind,
    pub command: String,
    pub workdir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution_brief: Option<ExecutionBrief>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_ticket_id: Option<String>,
    #[serde(default)]
    pub attempt_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_gate_status: Option<SchedulerGateStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_class: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<CapabilityArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementation_packet: Option<ImplementationPacketSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_mode: Option<AdaptiveWikiAgentMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_provider_fallback: Option<ProviderFallbackRecommendation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub last_adaptive_wiki_entry_ids: Vec<String>,
    #[serde(default)]
    pub preview: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_artifact_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_artifact_path: Option<String>,
}

impl OffdeskTask {
    pub fn new(input: OffdeskTaskInput, now: DateTime<Utc>) -> Self {
        let task_id = input
            .task_id
            .unwrap_or_else(|| format!("task_{}", Uuid::new_v4()));
        Self {
            task_id,
            request_id: input.request_id,
            project_key: input.project_key,
            status: OffdeskTaskStatus::Queued,
            capability_id: input.capability_id,
            runner_kind: input.runner_kind,
            command: input.command,
            workdir: input.workdir,
            execution_brief: input.execution_brief,
            background_ticket_id: None,
            attempt_count: 0,
            last_gate_status: None,
            last_error: None,
            created_at: now,
            updated_at: now,
            not_before: input.not_before,
            mutation_class: input.mutation_class,
            artifact_refs: input.artifact_refs,
            implementation_packet: input.implementation_packet,
            artifact_kind: input.artifact_kind,
            agent_mode: input.agent_mode,
            provider_id: input.provider_id,
            model: input.model,
            last_provider_fallback: None,
            last_adaptive_wiki_entry_ids: Vec::new(),
            preview: input.preview,
            reason: input.reason,
            log_artifact_path: input.log_artifact_path,
            result_artifact_path: input.result_artifact_path,
        }
    }

    pub fn is_due_at(&self, now: DateTime<Utc>) -> bool {
        self.not_before.is_none_or(|not_before| not_before <= now)
    }

    pub fn can_dispatch_at(&self, now: DateTime<Utc>) -> bool {
        matches!(
            self.status,
            OffdeskTaskStatus::Queued | OffdeskTaskStatus::PendingApproval
        ) && self.is_due_at(now)
    }

    pub fn operator_view(&self) -> OffdeskTaskView {
        let mode_assessment = assess_offdesk_mode(
            self.agent_mode,
            task_mode_lifecycle(self.status, self.result_artifact_path.as_deref()),
        );
        let next_safe_action = next_safe_action_for_task(
            &self.task_id,
            &self.project_key,
            self.background_ticket_id.as_deref(),
            self.status,
            mode_assessment.review_stage_required,
        );
        OffdeskTaskView {
            task_id: self.task_id.clone(),
            request_id: self.request_id.clone(),
            project_key: self.project_key.clone(),
            status: self.status,
            capability_id: self.capability_id.clone(),
            runner_kind: self.runner_kind,
            command: operator_safe_text(&self.command),
            workdir: self.workdir.clone(),
            background_ticket_id: self.background_ticket_id.clone(),
            attempt_count: self.attempt_count,
            last_gate_status: self.last_gate_status,
            last_error: self.last_error.as_deref().map(operator_safe_text),
            created_at: self.created_at,
            updated_at: self.updated_at,
            not_before: self.not_before,
            mutation_class: self.mutation_class.clone(),
            artifact_refs: self
                .artifact_refs
                .iter()
                .map(operator_safe_artifact_ref)
                .collect(),
            implementation_packet: self
                .implementation_packet
                .as_ref()
                .map(operator_safe_implementation_packet_summary),
            artifact_kind: self.artifact_kind.as_deref().map(operator_safe_text),
            agent_mode: self.agent_mode,
            mode_assessment,
            next_safe_action,
            provider_id: self.provider_id.as_deref().map(operator_safe_text),
            model: self.model.as_deref().map(operator_safe_text),
            last_provider_fallback: self
                .last_provider_fallback
                .as_ref()
                .map(operator_safe_provider_fallback),
            last_adaptive_wiki_entry_ids: self
                .last_adaptive_wiki_entry_ids
                .iter()
                .map(|entry_id| operator_safe_text(entry_id))
                .collect(),
            preview: operator_safe_text(&self.preview),
            reason: operator_safe_text(&self.reason),
            log_artifact_path: self.log_artifact_path.clone(),
            result_artifact_path: self.result_artifact_path.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffdeskTaskInput {
    pub task_id: Option<String>,
    pub request_id: String,
    pub project_key: String,
    pub capability_id: String,
    pub runner_kind: BackgroundRunnerKind,
    pub command: String,
    pub workdir: String,
    pub execution_brief: Option<ExecutionBrief>,
    pub not_before: Option<DateTime<Utc>>,
    pub mutation_class: Option<String>,
    pub artifact_refs: Vec<CapabilityArtifactRef>,
    pub implementation_packet: Option<ImplementationPacketSummary>,
    pub artifact_kind: Option<String>,
    pub agent_mode: Option<AdaptiveWikiAgentMode>,
    pub provider_id: Option<String>,
    pub model: Option<String>,
    pub preview: String,
    pub reason: String,
    pub log_artifact_path: Option<String>,
    pub result_artifact_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OffdeskTaskView {
    pub task_id: String,
    pub request_id: String,
    pub project_key: String,
    pub status: OffdeskTaskStatus,
    pub capability_id: String,
    pub runner_kind: BackgroundRunnerKind,
    pub command: String,
    pub workdir: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub background_ticket_id: Option<String>,
    pub attempt_count: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_gate_status: Option<SchedulerGateStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_before: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_class: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<CapabilityArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub implementation_packet: Option<ImplementationPacketSummary>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_mode: Option<AdaptiveWikiAgentMode>,
    #[serde(flatten)]
    pub mode_assessment: OffdeskModeAssessment,
    pub next_safe_action: OffdeskTaskNextSafeAction,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_provider_fallback: Option<ProviderFallbackRecommendation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub last_adaptive_wiki_entry_ids: Vec<String>,
    pub preview: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_artifact_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_artifact_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OffdeskNextSafeAction {
    pub kind: String,
    pub detail: String,
    pub scope: String,
    pub commands: Vec<String>,
    pub requires_operator_review: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub does_not_authorize: Vec<String>,
}

pub type OffdeskTaskNextSafeAction = OffdeskNextSafeAction;

impl OffdeskNextSafeAction {
    pub fn new(
        kind: impl Into<String>,
        detail: impl Into<String>,
        commands: Vec<String>,
        requires_operator_review: bool,
    ) -> Self {
        Self {
            kind: kind.into(),
            detail: detail.into(),
            scope: "operator_next_step".to_string(),
            commands,
            requires_operator_review,
            does_not_authorize: vec![
                "unrelated cleanup, file movement, wiki promotion, provider retargeting, or accepting Offdesk output without separate review"
                    .to_string(),
            ],
        }
    }
}

fn next_safe_action_priority(kind: &str) -> u8 {
    match kind {
        "approval_pending" | "approval_expired" | "approval_denied" => 10,
        "recovery_required" | "resume_review_required" | "result_artifact_missing" => 20,
        "review_required" | "closeout_check" => 30,
        "provider_attention" => 40,
        "runtime_monitoring" => 50,
        "dispatch_pending" => 60,
        "approval_resolved" | "cancelled" => 70,
        _ => 80,
    }
}

fn order_next_safe_actions_by_priority(actions: &mut [OffdeskNextSafeAction]) {
    actions.sort_by_key(|action| next_safe_action_priority(&action.kind));
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OffdeskPendingApprovalView {
    #[serde(flatten)]
    pub approval: PendingActionApproval,
    pub next_safe_action: OffdeskNextSafeAction,
}

fn task_mode_lifecycle(
    status: OffdeskTaskStatus,
    result_artifact_path: Option<&str>,
) -> OffdeskModeLifecycle {
    match status {
        OffdeskTaskStatus::Queued | OffdeskTaskStatus::PendingApproval => {
            OffdeskModeLifecycle::Pending
        }
        OffdeskTaskStatus::Launched | OffdeskTaskStatus::Running => OffdeskModeLifecycle::Running,
        OffdeskTaskStatus::Completed if result_artifact_path.is_some() => {
            OffdeskModeLifecycle::CompletedWithResult
        }
        OffdeskTaskStatus::Completed => OffdeskModeLifecycle::CompletedWithoutResult,
        OffdeskTaskStatus::Failed | OffdeskTaskStatus::ResumePending => {
            OffdeskModeLifecycle::Blocked
        }
        OffdeskTaskStatus::Cancelled => OffdeskModeLifecycle::Cancelled,
    }
}

fn next_safe_action_for_task(
    task_id: &str,
    project_key: &str,
    background_ticket_id: Option<&str>,
    status: OffdeskTaskStatus,
    review_stage_required: bool,
) -> OffdeskTaskNextSafeAction {
    let command_task_id = operator_safe_text(task_id);
    let command_project_key = operator_safe_text(project_key);
    let poll_target = background_ticket_id
        .map(operator_safe_text)
        .unwrap_or_else(|| command_task_id.clone());

    match status {
        OffdeskTaskStatus::Queued => OffdeskNextSafeAction::new(
            "dispatch_pending",
            "Task is queued; run one tick when ready or cancel only if the work should stop.",
            vec![
                "forager offdesk tick".to_string(),
                format!("forager offdesk cancel-task {command_task_id}"),
            ],
            false,
        ),
        OffdeskTaskStatus::PendingApproval => OffdeskNextSafeAction::new(
            "approval_pending",
            "Task is waiting for operator approval before launch.",
            vec![
                "forager offdesk pending".to_string(),
                format!("forager offdesk cancel-task {command_task_id}"),
            ],
            true,
        ),
        OffdeskTaskStatus::Launched | OffdeskTaskStatus::Running => OffdeskNextSafeAction::new(
            "runtime_monitoring",
            "Background work is active or recently launched; poll before judging completion.",
            vec![
                format!("forager offdesk poll {poll_target}"),
                format!("forager offdesk cancel-task {command_task_id}"),
            ],
            false,
        ),
        OffdeskTaskStatus::Failed => OffdeskNextSafeAction::new(
            "recovery_required",
            "Task failed; inspect the failure, then retry with the existing approval scope or request a new approval.",
            vec![
                format!("forager offdesk retry-task {command_task_id}"),
                format!("forager offdesk retry-task {command_task_id} --new-approval"),
            ],
            true,
        ),
        OffdeskTaskStatus::ResumePending => OffdeskNextSafeAction::new(
            "resume_review_required",
            "Task needs recovery review before the harness resumes or abandons it.",
            vec![
                format!("forager offdesk resume-task {command_task_id}"),
                format!("forager offdesk retry-task {command_task_id}"),
                format!("forager offdesk abandon-task {command_task_id}"),
            ],
            true,
        ),
        OffdeskTaskStatus::Completed if review_stage_required => OffdeskNextSafeAction::new(
            "review_required",
            "Completed Offdesk output still needs closeout and Ondesk review before it is treated as accepted.",
            vec![
                format!(
                    "forager offdesk closeout --project-key {command_project_key} --task-id {command_task_id}"
                ),
                format!("forager ondesk prompt-package --project-key {command_project_key}"),
            ],
            true,
        ),
        OffdeskTaskStatus::Completed => OffdeskNextSafeAction::new(
            "closeout_check",
            "Dispatch is terminal; verify closeout before treating Offdesk output as accepted.",
            vec![format!(
                "forager offdesk closeout --project-key {command_project_key} --task-id {command_task_id}"
            )],
            true,
        ),
        OffdeskTaskStatus::Cancelled => OffdeskNextSafeAction::new(
            "cancelled",
            "Task is cancelled; no dispatch action is needed.",
            Vec::new(),
            false,
        ),
    }
}

pub fn next_safe_action_for_background_poll(
    probe: &BackgroundProbe,
    decision: &BackgroundRecoveryDecision,
    review_stage_required: bool,
) -> OffdeskNextSafeAction {
    let ticket_id = operator_safe_text(&probe.ticket_id);
    let task_id = probe.task_id.as_deref().map(operator_safe_text);
    let project_key = probe.project_key.as_deref().map(operator_safe_text);

    match decision.phase {
        BackgroundRunnerPhase::Completed | BackgroundRunnerPhase::ResultReceived
            if !probe.result_artifact_present =>
        {
            OffdeskNextSafeAction::new(
                "result_artifact_missing",
                "Background runner reached a terminal phase, but the result artifact is missing; inspect logs before closeout.",
                vec![format!("forager offdesk poll {ticket_id}"), "forager offdesk background".to_string()],
                true,
            )
        }
        BackgroundRunnerPhase::Completed | BackgroundRunnerPhase::ResultReceived
            if review_stage_required =>
        {
            OffdeskNextSafeAction::new(
                "review_required",
                "Background result is present; close out the matched Offdesk work and return through Ondesk review before trusting output.",
                closeout_and_ondesk_commands(project_key.as_deref(), task_id.as_deref()),
                true,
            )
        }
        BackgroundRunnerPhase::Completed | BackgroundRunnerPhase::ResultReceived => {
            OffdeskNextSafeAction::new(
                "closeout_check",
                "Background dispatch is terminal; verify closeout before treating output as accepted.",
                closeout_commands(project_key.as_deref(), task_id.as_deref()),
                true,
            )
        }
        BackgroundRunnerPhase::Failed
        | BackgroundRunnerPhase::StaleNoAck
        | BackgroundRunnerPhase::StaleLostCallback
        | BackgroundRunnerPhase::Reconstructable => {
            let commands = task_id.as_deref().map_or_else(
                || vec![format!("forager offdesk poll {ticket_id}"), "forager offdesk background".to_string()],
                |task_id| {
                    vec![
                        format!("forager offdesk resume-task {task_id}"),
                        format!("forager offdesk retry-task {task_id}"),
                        format!("forager offdesk abandon-task {task_id}"),
                    ]
                },
            );
            OffdeskNextSafeAction::new(
                "resume_review_required",
                "Background runner needs recovery review before the harness resumes, retries, or abandons it.",
                commands,
                true,
            )
        }
        BackgroundRunnerPhase::RecoveryAcknowledged => OffdeskNextSafeAction::new(
            "recovery_acknowledged",
            "Background recovery was acknowledged by an operator; no retry, resume, closeout, or accepted-truth action is authorized from this probe.",
            Vec::new(),
            false,
        ),
        BackgroundRunnerPhase::Launched
        | BackgroundRunnerPhase::HandoffEmitted
        | BackgroundRunnerPhase::PickupAcknowledged => OffdeskNextSafeAction::new(
            "runtime_monitoring",
            "Background work is still active or awaiting a result; poll before judging completion.",
            vec![format!("forager offdesk poll {ticket_id}")],
            false,
        ),
    }
}

pub fn pending_approval_operator_views(
    approvals: Vec<PendingActionApproval>,
    now: DateTime<Utc>,
) -> Vec<OffdeskPendingApprovalView> {
    approvals
        .into_iter()
        .map(|approval| pending_approval_operator_view(approval, now))
        .collect()
}

pub fn pending_approval_operator_view(
    approval: PendingActionApproval,
    now: DateTime<Utc>,
) -> OffdeskPendingApprovalView {
    let next_safe_action = next_safe_action_for_pending_approval(&approval, now);
    OffdeskPendingApprovalView {
        approval,
        next_safe_action,
    }
}

pub fn next_safe_action_for_pending_approval(
    approval: &PendingActionApproval,
    now: DateTime<Utc>,
) -> OffdeskNextSafeAction {
    let approval_id = operator_safe_text(&approval.approval_id);
    let task_id = operator_safe_text(&approval.task_id);
    match approval.status {
        ApprovalStatus::Pending if approval.expires_at >= now => OffdeskNextSafeAction::new(
            "approval_pending",
            format!(
                "Approval for `{}` needs an operator decision; approve only the bounded action shown in this approval.",
                operator_safe_text(&approval.action)
            ),
            vec![
                format!("forager offdesk ok {approval_id}"),
                format!("forager offdesk cancel {approval_id}"),
                "forager offdesk pending".to_string(),
            ],
            true,
        ),
        ApprovalStatus::Pending => OffdeskNextSafeAction::new(
            "approval_expired",
            "Approval is past its TTL; expire it, then re-run the gated action if it is still needed.",
            vec![
                "forager offdesk pending --all".to_string(),
                "forager offdesk tick".to_string(),
            ],
            true,
        ),
        ApprovalStatus::Denied => OffdeskNextSafeAction::new(
            "approval_denied",
            "Approval was denied; retry only after revising the task or requesting a new approval.",
            vec![
                format!("forager offdesk retry-task {task_id} --new-approval"),
                "forager offdesk tasks".to_string(),
            ],
            true,
        ),
        ApprovalStatus::Approved | ApprovalStatus::Superseded => OffdeskNextSafeAction::new(
            "approval_resolved",
            "Approval is resolved; run a tick or inspect tasks to see the resulting dispatch state.",
            vec![
                "forager offdesk tick".to_string(),
                "forager offdesk tasks".to_string(),
            ],
            false,
        ),
        ApprovalStatus::Expired => OffdeskNextSafeAction::new(
            "approval_expired",
            "Approval expired; re-run the gated task path if this work is still wanted.",
            vec![
                "forager offdesk tasks".to_string(),
                format!("forager offdesk retry-task {task_id} --new-approval"),
            ],
            true,
        ),
    }
}

pub fn tick_next_safe_actions_from_report(
    report: &OffdeskTickReportInput,
) -> Vec<OffdeskNextSafeAction> {
    let mut actions = Vec::new();
    if report.pending_approval > 0 || report.expired_approvals > 0 {
        actions.push(OffdeskNextSafeAction::new(
            "approval_pending",
            "One or more Offdesk approvals need operator attention before launch can proceed.",
            vec!["forager offdesk pending".to_string()],
            true,
        ));
    }
    if report.resume_pending > 0 || report.failed > 0 {
        actions.push(OffdeskNextSafeAction::new(
            "recovery_required",
            "One or more Offdesk tasks need recovery review before retry, resume, or abandon.",
            vec![
                "forager offdesk resume".to_string(),
                "forager offdesk tasks --status resume-pending".to_string(),
                "forager offdesk tasks --status failed".to_string(),
            ],
            true,
        ));
    }
    if report.completed > 0 {
        actions.push(OffdeskNextSafeAction::new(
            "review_required",
            "One or more Offdesk tasks completed; run closeout before returning through Ondesk review.",
            vec!["forager offdesk closeout".to_string()],
            true,
        ));
    }
    if report.launched > 0
        || (report.polled_background > 0
            && report.completed == 0
            && report.resume_pending == 0
            && report.failed == 0)
    {
        actions.push(OffdeskNextSafeAction::new(
            "runtime_monitoring",
            "Background runners were launched or polled; continue polling until a result or recovery state is clear.",
            vec!["forager offdesk poll".to_string()],
            false,
        ));
    }
    if report.provider_deferred > 0 {
        actions.push(OffdeskNextSafeAction::new(
            "provider_attention",
            "Provider capacity deferred one or more tasks; inspect fallback recommendations before retargeting.",
            vec![
                "forager offdesk provider-capacity".to_string(),
                "forager offdesk provider-fallback".to_string(),
            ],
            true,
        ));
    }
    if report.skipped > 0 {
        actions.push(OffdeskNextSafeAction::new(
            "dispatch_pending",
            "The tick limit left due tasks queued; run another tick when ready.",
            vec!["forager offdesk tick".to_string()],
            false,
        ));
    }
    order_next_safe_actions_by_priority(&mut actions);
    actions
}

pub fn status_next_safe_actions_from_summary(
    summary: &OffdeskStatusNextSafeActionInput,
) -> Vec<OffdeskNextSafeAction> {
    let mut actions = Vec::new();
    if summary.pending_approvals > 0 || summary.tasks.pending_approval > 0 {
        actions.push(OffdeskNextSafeAction::new(
            "approval_pending",
            "Offdesk approvals are blocking dispatch; review the exact bounded action before approve/deny.",
            vec!["forager offdesk pending".to_string()],
            true,
        ));
    }
    if summary.tasks.failed > 0
        || summary.tasks.resume_pending > 0
        || summary.background_stale > 0
        || summary.background_failed > 0
    {
        actions.push(OffdeskNextSafeAction::new(
            "recovery_required",
            "One or more Offdesk tasks or background runners need recovery review before retry, resume, or abandon.",
            vec![
                "forager offdesk resume".to_string(),
                "forager offdesk tasks --status resume-pending".to_string(),
                "forager offdesk tasks --status failed".to_string(),
                "forager offdesk poll".to_string(),
            ],
            true,
        ));
    }
    if summary.closeout_required > 0 {
        let closeout_without_fresh_package = summary.closeout_state.missing_closeout
            + summary.closeout_state.stale_closeout
            + summary.closeout_state.stale_review;
        if closeout_without_fresh_package > 0 {
            actions.push(OffdeskNextSafeAction::new(
                "review_required",
                "Completed Offdesk output needs a fresh closeout package before Ondesk review.",
                vec![
                    "forager offdesk closeout".to_string(),
                    "forager ondesk prompt-package".to_string(),
                ],
                true,
            ));
        } else if summary.closeout_state.pending_review > 0 {
            actions.push(OffdeskNextSafeAction::new(
                "review_required",
                "Closeout package exists; review COMMERCIAL_REVIEW_PACKET.md and record a closeout-review verdict.",
                vec![
                    "forager offdesk closeout-review --verdict approved|revise|blocked".to_string(),
                    "forager ondesk prompt-package".to_string(),
                ],
                true,
            ));
        } else if summary.closeout_state.revision_required > 0 {
            let detail = if summary.closeout_state.approved_with_followups > 0 {
                format!(
                    "{} closeout review(s) returned revise/blocked; {} approved receipt(s) still have receipt follow-ups. Address the revise/blocked review notes before treating any output as accepted.",
                    summary.closeout_state.revision_required,
                    summary.closeout_state.approved_with_followups
                )
            } else {
                format!(
                    "{} closeout review(s) returned revise/blocked; address the review notes before treating output as accepted.",
                    summary.closeout_state.revision_required
                )
            };
            actions.push(OffdeskNextSafeAction::new(
                "review_required",
                detail,
                vec![
                    "forager ondesk prompt-package".to_string(),
                    "forager offdesk closeout".to_string(),
                ],
                true,
            ));
        } else if summary.closeout_state.approved_with_followups > 0 {
            actions.push(OffdeskNextSafeAction::new(
                "review_required",
                "Closeout review is approved, but receipt follow-ups remain before the output is accepted truth.",
                vec![
                    "forager ondesk prompt-package".to_string(),
                    "Review the latest closeout receipt and RETURN_PACKAGE.md".to_string(),
                ],
                true,
            ));
        }
    }
    if summary.background_active > 0 || summary.tasks.active > 0 {
        actions.push(OffdeskNextSafeAction::new(
            "runtime_monitoring",
            "Background work is active; poll before judging completion.",
            vec!["forager offdesk poll".to_string()],
            false,
        ));
    }
    if summary.tasks.queued > 0 {
        actions.push(OffdeskNextSafeAction::new(
            "dispatch_pending",
            "Queued Offdesk work remains; run a tick when ready to dispatch the next due task.",
            vec!["forager offdesk tick".to_string()],
            false,
        ));
    }
    order_next_safe_actions_by_priority(&mut actions);
    actions
}

pub fn ensure_resume_review_next_safe_action(actions: &mut Vec<OffdeskNextSafeAction>) {
    if actions.iter().any(|action| {
        matches!(
            action.kind.as_str(),
            "recovery_required" | "resume_review_required"
        )
    }) {
        order_next_safe_actions_by_priority(actions);
        return;
    }
    let action = OffdeskNextSafeAction::new(
        "resume_review_required",
        "Resume records are waiting; inspect the resume evidence before continuing Offdesk work.",
        vec!["forager offdesk resume".to_string()],
        true,
    );
    actions.push(action);
    order_next_safe_actions_by_priority(actions);
}

#[derive(Debug, Clone, Copy)]
pub struct OffdeskTickReportInput {
    pub expired_approvals: usize,
    pub polled_background: usize,
    pub launched: usize,
    pub pending_approval: usize,
    pub completed: usize,
    pub failed: usize,
    pub resume_pending: usize,
    pub provider_deferred: usize,
    pub skipped: usize,
}

#[derive(Debug, Clone)]
pub struct OffdeskStatusNextSafeActionInput {
    pub pending_approvals: usize,
    pub tasks: OffdeskTaskCounts,
    pub background_active: usize,
    pub background_stale: usize,
    pub background_failed: usize,
    pub closeout_required: usize,
    pub closeout_state: OffdeskCloseoutStateSummary,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct OffdeskCloseoutStateSummary {
    pub missing_closeout: usize,
    pub pending_review: usize,
    pub revision_required: usize,
    pub approved_with_followups: usize,
    pub stale_closeout: usize,
    pub stale_review: usize,
    pub accepted: usize,
    pub approved: usize,
    pub retired_incomplete: usize,
}

impl OffdeskCloseoutStateSummary {
    pub fn required_count(&self) -> usize {
        self.missing_closeout
            + self.pending_review
            + self.revision_required
            + self.approved_with_followups
            + self.stale_closeout
            + self.stale_review
    }
}

fn closeout_and_ondesk_commands(project_key: Option<&str>, task_id: Option<&str>) -> Vec<String> {
    let mut commands = closeout_commands(project_key, task_id);
    if let Some(project_key) = project_key {
        commands.push(format!(
            "forager ondesk prompt-package --project-key {project_key}"
        ));
    }
    commands
}

fn closeout_commands(project_key: Option<&str>, task_id: Option<&str>) -> Vec<String> {
    match (project_key, task_id) {
        (Some(project_key), Some(task_id)) => vec![format!(
            "forager offdesk closeout --project-key {project_key} --task-id {task_id}"
        )],
        (Some(project_key), None) => {
            vec![format!(
                "forager offdesk closeout --project-key {project_key}"
            )]
        }
        (None, Some(task_id)) => vec![format!("forager offdesk closeout --task-id {task_id}")],
        (None, None) => vec!["forager offdesk closeout".to_string()],
    }
}

fn operator_safe_artifact_ref(artifact_ref: &CapabilityArtifactRef) -> CapabilityArtifactRef {
    CapabilityArtifactRef {
        artifact_id: operator_safe_text(&artifact_ref.artifact_id),
        path: artifact_ref.path.as_deref().map(operator_safe_text),
        present: artifact_ref.present,
    }
}

fn operator_safe_provider_fallback(
    recommendation: &ProviderFallbackRecommendation,
) -> ProviderFallbackRecommendation {
    ProviderFallbackRecommendation {
        current_provider_id: operator_safe_text(&recommendation.current_provider_id),
        current_model: recommendation
            .current_model
            .as_deref()
            .map(operator_safe_text),
        trigger_reason: operator_safe_text(&recommendation.trigger_reason),
        generated_at: recommendation.generated_at,
        candidates: recommendation
            .candidates
            .iter()
            .map(operator_safe_provider_fallback_candidate)
            .collect(),
    }
}

fn operator_safe_provider_fallback_candidate(
    candidate: &ProviderFallbackCandidate,
) -> ProviderFallbackCandidate {
    ProviderFallbackCandidate {
        provider_id: operator_safe_text(&candidate.provider_id),
        model: candidate.model.as_deref().map(operator_safe_text),
        source: candidate.source,
        auth_status: candidate.auth_status,
        capacity_status: candidate.capacity_status,
        recommended: candidate.recommended,
        reason: operator_safe_text(&candidate.reason),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OffdeskTaskLifecycleAction {
    Cancel,
    Retry,
    Resume,
    Abandon,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OffdeskTaskLifecycleReport {
    pub task: OffdeskTaskView,
    pub action: OffdeskTaskLifecycleAction,
    pub changed: bool,
    pub previous_status: OffdeskTaskStatus,
    pub status: OffdeskTaskStatus,
    pub message: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct OffdeskTaskCounts {
    pub queued: usize,
    pub pending_approval: usize,
    pub active: usize,
    pub completed: usize,
    pub failed: usize,
    pub resume_pending: usize,
    pub cancelled: usize,
}

#[derive(Debug, Clone)]
pub struct OffdeskTaskStore {
    root: PathBuf,
}

impl OffdeskTaskStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn path(&self) -> PathBuf {
        self.root.join(TASKS_FILE)
    }

    pub fn load(&self) -> Result<Vec<OffdeskTask>> {
        read_tasks(&self.path())
    }

    pub fn save(&self, tasks: &[OffdeskTask]) -> Result<()> {
        write_tasks(&self.path(), tasks)
    }

    pub fn enqueue(&self, task: OffdeskTask) -> Result<()> {
        let mut tasks = self.load()?;
        if let Some(existing) = tasks
            .iter_mut()
            .find(|existing| existing.task_id == task.task_id)
        {
            *existing = task;
        } else {
            tasks.push(task);
        }
        self.save(&tasks)
    }

    pub fn counts(&self) -> Result<OffdeskTaskCounts> {
        Ok(count_tasks(&self.load()?))
    }

    pub fn cancel_task(
        &self,
        task_id: &str,
        reason: Option<&str>,
        now: DateTime<Utc>,
    ) -> Result<OffdeskTaskLifecycleReport> {
        let mut tasks = self.load()?;
        let index = find_task_index(&tasks, task_id)?;
        let previous_status = tasks[index].status;

        let changed = match previous_status {
            OffdeskTaskStatus::Cancelled => false,
            OffdeskTaskStatus::Completed => false,
            _ => {
                let task = &mut tasks[index];
                task.status = OffdeskTaskStatus::Cancelled;
                task.updated_at = now;
                if let Some(reason) = reason.map(str::trim).filter(|reason| !reason.is_empty()) {
                    task.reason = reason.to_string();
                }
                true
            }
        };

        if changed {
            self.save(&tasks)?;
            let task = &tasks[index];
            TaskResumeStore::new(&self.root).cancel(&task.project_key, &task.task_id, now)?;
        }

        let message = match (previous_status, changed) {
            (OffdeskTaskStatus::Cancelled, false) => "task is already cancelled",
            (OffdeskTaskStatus::Completed, false) => "completed tasks cannot be cancelled",
            _ => "task marked cancelled; background runner was left untouched",
        };
        Ok(lifecycle_report(
            &tasks[index],
            OffdeskTaskLifecycleAction::Cancel,
            changed,
            previous_status,
            message,
        ))
    }

    pub fn retry_task(
        &self,
        task_id: &str,
        now: DateTime<Utc>,
    ) -> Result<OffdeskTaskLifecycleReport> {
        let mut tasks = self.load()?;
        let index = find_task_index(&tasks, task_id)?;
        let previous_status = tasks[index].status;

        let changed = match previous_status {
            OffdeskTaskStatus::Failed
            | OffdeskTaskStatus::ResumePending
            | OffdeskTaskStatus::Cancelled => {
                requeue_task(&mut tasks[index], now);
                true
            }
            _ => false,
        };

        if changed {
            self.save(&tasks)?;
            if previous_status == OffdeskTaskStatus::ResumePending {
                let task = &tasks[index];
                TaskResumeStore::new(&self.root).clear_after_recovery(
                    &task.project_key,
                    &task.task_id,
                    now,
                )?;
            }
        }

        let message = match (previous_status, changed) {
            (_, true) => "task requeued; next tick will create a fresh background ticket",
            (OffdeskTaskStatus::Queued, false) => "task is already queued",
            (OffdeskTaskStatus::Completed, false) => "completed tasks cannot be retried",
            (OffdeskTaskStatus::PendingApproval, false) => {
                "task is pending approval; resolve or cancel the approval path first"
            }
            (OffdeskTaskStatus::Launched | OffdeskTaskStatus::Running, false) => {
                "active tasks cannot be retried without cancellation or recovery"
            }
            _ => "task cannot be retried from its current status",
        };
        Ok(lifecycle_report(
            &tasks[index],
            OffdeskTaskLifecycleAction::Retry,
            changed,
            previous_status,
            message,
        ))
    }

    pub fn resume_task(
        &self,
        task_id: &str,
        now: DateTime<Utc>,
    ) -> Result<OffdeskTaskLifecycleReport> {
        let mut tasks = self.load()?;
        let index = find_task_index(&tasks, task_id)?;
        let previous_status = tasks[index].status;

        let changed = if previous_status == OffdeskTaskStatus::ResumePending {
            requeue_task(&mut tasks[index], now);
            true
        } else {
            false
        };

        if changed {
            self.save(&tasks)?;
            let task = &tasks[index];
            TaskResumeStore::new(&self.root).clear_after_recovery(
                &task.project_key,
                &task.task_id,
                now,
            )?;
        }

        let message = if changed {
            "resume accepted; task requeued and resume artifact marked resumed"
        } else if previous_status == OffdeskTaskStatus::Queued {
            "task is already queued"
        } else {
            "only resume_pending tasks can be resumed"
        };
        Ok(lifecycle_report(
            &tasks[index],
            OffdeskTaskLifecycleAction::Resume,
            changed,
            previous_status,
            message,
        ))
    }

    pub fn abandon_task(
        &self,
        task_id: &str,
        now: DateTime<Utc>,
    ) -> Result<OffdeskTaskLifecycleReport> {
        let mut tasks = self.load()?;
        let index = find_task_index(&tasks, task_id)?;
        let previous_status = tasks[index].status;

        let changed = match previous_status {
            OffdeskTaskStatus::ResumePending | OffdeskTaskStatus::Failed => {
                let task = &mut tasks[index];
                task.status = OffdeskTaskStatus::Cancelled;
                task.updated_at = now;
                true
            }
            _ => false,
        };

        if changed {
            self.save(&tasks)?;
            let task = &tasks[index];
            TaskResumeStore::new(&self.root).abandon(&task.project_key, &task.task_id, now)?;
        }

        let message = match (previous_status, changed) {
            (_, true) => "task abandoned and marked cancelled",
            (OffdeskTaskStatus::Cancelled, false) => "task is already cancelled",
            (OffdeskTaskStatus::Completed, false) => "completed tasks cannot be abandoned",
            _ => "only failed or resume_pending tasks can be abandoned",
        };
        Ok(lifecycle_report(
            &tasks[index],
            OffdeskTaskLifecycleAction::Abandon,
            changed,
            previous_status,
            message,
        ))
    }
}

pub fn count_tasks(tasks: &[OffdeskTask]) -> OffdeskTaskCounts {
    let mut counts = OffdeskTaskCounts::default();
    for task in tasks {
        match task.status {
            OffdeskTaskStatus::Queued => counts.queued += 1,
            OffdeskTaskStatus::PendingApproval => counts.pending_approval += 1,
            OffdeskTaskStatus::Launched | OffdeskTaskStatus::Running => counts.active += 1,
            OffdeskTaskStatus::Completed => counts.completed += 1,
            OffdeskTaskStatus::Failed => counts.failed += 1,
            OffdeskTaskStatus::ResumePending => counts.resume_pending += 1,
            OffdeskTaskStatus::Cancelled => counts.cancelled += 1,
        }
    }
    counts
}

fn read_tasks(path: &Path) -> Result<Vec<OffdeskTask>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(path)?;
    if content.trim().is_empty() {
        return Ok(Vec::new());
    }
    Ok(serde_json::from_str(&content)?)
}

fn write_tasks(path: &Path, tasks: &[OffdeskTask]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(tasks)?)?;
    Ok(())
}

fn find_task_index(tasks: &[OffdeskTask], task_id: &str) -> Result<usize> {
    tasks
        .iter()
        .position(|task| task.task_id == task_id)
        .ok_or_else(|| anyhow::anyhow!("offdesk task not found: {task_id}"))
}

fn requeue_task(task: &mut OffdeskTask, now: DateTime<Utc>) {
    task.status = OffdeskTaskStatus::Queued;
    task.background_ticket_id = None;
    task.last_gate_status = None;
    task.last_error = None;
    task.last_provider_fallback = None;
    task.updated_at = now;
}

fn lifecycle_report(
    task: &OffdeskTask,
    action: OffdeskTaskLifecycleAction,
    changed: bool,
    previous_status: OffdeskTaskStatus,
    message: &str,
) -> OffdeskTaskLifecycleReport {
    OffdeskTaskLifecycleReport {
        task: task.operator_view(),
        action,
        changed,
        previous_status,
        status: task.status,
        message: operator_safe_text(message),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offdesk::{
        OffdeskModeRisk, OffdeskModeVerdict, ResumePendingInput, ResumeStatus, TaskResumeState,
        TaskResumeStore,
    };
    use chrono::Duration;
    use tempfile::tempdir;

    fn input() -> OffdeskTaskInput {
        OffdeskTaskInput {
            task_id: Some("task".to_string()),
            request_id: "request".to_string(),
            project_key: "project".to_string(),
            capability_id: "dispatch.runtime".to_string(),
            runner_kind: BackgroundRunnerKind::LocalBackground,
            command: "token=sk-secretsecretsecretsecret".to_string(),
            workdir: "/tmp".to_string(),
            execution_brief: None,
            not_before: None,
            mutation_class: None,
            artifact_refs: Vec::new(),
            implementation_packet: None,
            artifact_kind: None,
            agent_mode: None,
            provider_id: None,
            model: None,
            preview: "preview".to_string(),
            reason: "reason".to_string(),
            log_artifact_path: None,
            result_artifact_path: None,
        }
    }

    #[test]
    fn queue_roundtrips_task() -> Result<()> {
        let temp = tempdir()?;
        let store = OffdeskTaskStore::new(temp.path());
        let task = OffdeskTask::new(input(), Utc::now());

        store.enqueue(task.clone())?;

        assert_eq!(store.load()?, vec![task]);
        Ok(())
    }

    #[test]
    fn legacy_task_without_optional_dispatch_fields_still_loads() -> Result<()> {
        let temp = tempdir()?;
        let store = OffdeskTaskStore::new(temp.path());
        let now = Utc::now();
        fs::create_dir_all(temp.path())?;
        fs::write(
            store.path(),
            serde_json::to_string_pretty(&serde_json::json!([
                {
                    "task_id": "task",
                    "request_id": "request",
                    "project_key": "project",
                    "status": "queued",
                    "capability_id": "dispatch.runtime",
                    "runner_kind": "local_background",
                    "command": "true",
                    "workdir": "/tmp",
                    "created_at": now,
                    "updated_at": now
                }
            ]))?,
        )?;

        let task = store.load()?.remove(0);

        assert_eq!(task.attempt_count, 0);
        assert!(task.execution_brief.is_none());
        assert!(task.background_ticket_id.is_none());
        assert!(task.last_gate_status.is_none());
        assert!(task.last_error.is_none());
        assert!(task.not_before.is_none());
        assert!(task.mutation_class.is_none());
        assert!(task.artifact_refs.is_empty());
        assert!(task.provider_id.is_none());
        assert!(task.model.is_none());
        assert!(task.last_provider_fallback.is_none());
        assert!(task.preview.is_empty());
        assert!(task.reason.is_empty());
        assert!(task.log_artifact_path.is_none());
        assert!(task.result_artifact_path.is_none());
        Ok(())
    }

    #[test]
    fn operator_view_redacts_command() {
        let task = OffdeskTask::new(input(), Utc::now());
        let view = task.operator_view();

        assert!(!view.command.contains("sk-secret"));
    }

    #[test]
    fn operator_view_surfaces_mode_assessment_without_persisting_new_fields() {
        let mut input = input();
        input.agent_mode = Some(AdaptiveWikiAgentMode::Development);
        input.result_artifact_path = Some("/tmp/result.json".to_string());
        let mut task = OffdeskTask::new(input, Utc::now());
        task.status = OffdeskTaskStatus::Completed;

        let view = task.operator_view();

        assert_eq!(
            view.mode_assessment.mode_verdict,
            OffdeskModeVerdict::EvidenceReady
        );
        assert_eq!(
            view.mode_assessment.mode_risk,
            OffdeskModeRisk::OperatorReviewRequired
        );
        assert!(view.mode_assessment.review_stage_required);
        assert_eq!(view.next_safe_action.kind, "review_required");
        assert!(view.next_safe_action.requires_operator_review);
        assert!(view
            .next_safe_action
            .commands
            .iter()
            .any(|command| command.contains("forager offdesk closeout")));
    }

    fn resume_state(now: DateTime<Utc>) -> TaskResumeState {
        TaskResumeState::mark_pending(ResumePendingInput {
            task_id: "task".to_string(),
            request_id: "request".to_string(),
            project_key: "project".to_string(),
            phase: "background".to_string(),
            runner_target: "local_background".to_string(),
            interruption_reason: "restart".to_string(),
            interrupted_at: now,
            fresh_until: now + Duration::minutes(10),
        })
    }

    fn action_kinds(actions: &[OffdeskNextSafeAction]) -> Vec<&str> {
        actions.iter().map(|action| action.kind.as_str()).collect()
    }

    #[test]
    fn status_next_safe_actions_use_operator_priority_contract() {
        let actions = status_next_safe_actions_from_summary(&OffdeskStatusNextSafeActionInput {
            pending_approvals: 1,
            tasks: OffdeskTaskCounts {
                queued: 1,
                pending_approval: 1,
                active: 1,
                completed: 1,
                failed: 1,
                resume_pending: 1,
                cancelled: 1,
            },
            background_active: 1,
            background_stale: 1,
            background_failed: 1,
            closeout_required: 1,
            closeout_state: OffdeskCloseoutStateSummary {
                missing_closeout: 1,
                ..OffdeskCloseoutStateSummary::default()
            },
        });

        assert_eq!(
            action_kinds(&actions),
            vec![
                "approval_pending",
                "recovery_required",
                "review_required",
                "runtime_monitoring",
                "dispatch_pending",
            ]
        );
    }

    #[test]
    fn status_next_safe_actions_point_pending_closeouts_to_review_verdict() {
        let actions = status_next_safe_actions_from_summary(&OffdeskStatusNextSafeActionInput {
            pending_approvals: 0,
            tasks: OffdeskTaskCounts::default(),
            background_active: 0,
            background_stale: 0,
            background_failed: 0,
            closeout_required: 1,
            closeout_state: OffdeskCloseoutStateSummary {
                pending_review: 1,
                ..OffdeskCloseoutStateSummary::default()
            },
        });

        assert_eq!(action_kinds(&actions), vec!["review_required"]);
        assert!(actions[0].detail.contains("COMMERCIAL_REVIEW_PACKET.md"));
        assert_eq!(
            actions[0].commands[0],
            "forager offdesk closeout-review --verdict approved|revise|blocked"
        );
    }

    #[test]
    fn status_next_safe_actions_keep_receipt_followups_visible() {
        let actions = status_next_safe_actions_from_summary(&OffdeskStatusNextSafeActionInput {
            pending_approvals: 0,
            tasks: OffdeskTaskCounts::default(),
            background_active: 0,
            background_stale: 0,
            background_failed: 0,
            closeout_required: 1,
            closeout_state: OffdeskCloseoutStateSummary {
                approved_with_followups: 1,
                ..OffdeskCloseoutStateSummary::default()
            },
        });

        assert_eq!(action_kinds(&actions), vec!["review_required"]);
        assert!(actions[0].detail.contains("receipt follow-ups"));
        assert_eq!(actions[0].commands[0], "forager ondesk prompt-package");
    }

    #[test]
    fn status_next_safe_actions_prioritize_revision_required_over_receipt_followups() {
        let actions = status_next_safe_actions_from_summary(&OffdeskStatusNextSafeActionInput {
            pending_approvals: 0,
            tasks: OffdeskTaskCounts::default(),
            background_active: 0,
            background_stale: 0,
            background_failed: 0,
            closeout_required: 13,
            closeout_state: OffdeskCloseoutStateSummary {
                revision_required: 12,
                approved_with_followups: 1,
                ..OffdeskCloseoutStateSummary::default()
            },
        });

        assert_eq!(action_kinds(&actions), vec!["review_required"]);
        assert!(actions[0].detail.contains("revise/blocked"));
        assert!(actions[0].detail.contains("receipt follow-ups"));
        assert_eq!(actions[0].commands[0], "forager ondesk prompt-package");
    }

    #[test]
    fn tick_next_safe_actions_use_operator_priority_contract() {
        let actions = tick_next_safe_actions_from_report(&OffdeskTickReportInput {
            expired_approvals: 1,
            polled_background: 1,
            launched: 1,
            pending_approval: 1,
            completed: 1,
            failed: 1,
            resume_pending: 1,
            provider_deferred: 1,
            skipped: 1,
        });

        assert_eq!(
            action_kinds(&actions),
            vec![
                "approval_pending",
                "recovery_required",
                "review_required",
                "provider_attention",
                "runtime_monitoring",
                "dispatch_pending",
            ]
        );
    }

    #[test]
    fn resume_store_next_safe_action_is_ordered_after_approvals() {
        let mut actions = vec![
            OffdeskNextSafeAction::new(
                "runtime_monitoring",
                "poll",
                vec!["forager offdesk poll".to_string()],
                false,
            ),
            OffdeskNextSafeAction::new(
                "review_required",
                "closeout",
                vec!["forager offdesk closeout".to_string()],
                true,
            ),
            OffdeskNextSafeAction::new(
                "approval_expired",
                "approval expired",
                vec!["forager offdesk pending --all".to_string()],
                true,
            ),
            OffdeskNextSafeAction::new(
                "dispatch_pending",
                "tick",
                vec!["forager offdesk tick".to_string()],
                false,
            ),
        ];

        ensure_resume_review_next_safe_action(&mut actions);

        assert_eq!(
            action_kinds(&actions),
            vec![
                "approval_expired",
                "resume_review_required",
                "review_required",
                "runtime_monitoring",
                "dispatch_pending",
            ]
        );
    }

    #[test]
    fn cancel_queued_task_marks_cancelled() -> Result<()> {
        let temp = tempdir()?;
        let store = OffdeskTaskStore::new(temp.path());
        let now = Utc::now();
        store.enqueue(OffdeskTask::new(input(), now))?;

        let report =
            store.cancel_task("task", Some("operator stop"), now + Duration::seconds(1))?;

        assert!(report.changed);
        assert_eq!(report.previous_status, OffdeskTaskStatus::Queued);
        assert_eq!(report.status, OffdeskTaskStatus::Cancelled);
        assert_eq!(store.load()?[0].status, OffdeskTaskStatus::Cancelled);
        Ok(())
    }

    #[test]
    fn cancel_running_task_preserves_background_ticket() -> Result<()> {
        let temp = tempdir()?;
        let store = OffdeskTaskStore::new(temp.path());
        let now = Utc::now();
        let mut task = OffdeskTask::new(input(), now);
        task.status = OffdeskTaskStatus::Running;
        task.background_ticket_id = Some("ticket".to_string());
        store.enqueue(task)?;

        let report = store.cancel_task("task", None, now + Duration::seconds(1))?;
        let updated = store.load()?.remove(0);

        assert!(report.changed);
        assert_eq!(updated.status, OffdeskTaskStatus::Cancelled);
        assert_eq!(updated.background_ticket_id.as_deref(), Some("ticket"));
        Ok(())
    }

    #[test]
    fn retry_failed_task_requeues_and_clears_dispatch_state() -> Result<()> {
        let temp = tempdir()?;
        let store = OffdeskTaskStore::new(temp.path());
        let now = Utc::now();
        let mut task = OffdeskTask::new(input(), now);
        task.status = OffdeskTaskStatus::Failed;
        task.background_ticket_id = Some("ticket".to_string());
        task.attempt_count = 2;
        task.last_gate_status = Some(SchedulerGateStatus::Denied);
        task.last_error = Some("denied".to_string());
        task.last_provider_fallback = Some(ProviderFallbackRecommendation {
            current_provider_id: "openai".to_string(),
            current_model: Some("gpt-4.1".to_string()),
            trigger_reason: "provider capacity cooldown active".to_string(),
            generated_at: now,
            candidates: Vec::new(),
        });
        store.enqueue(task)?;

        let report = store.retry_task("task", now + Duration::seconds(1))?;
        let updated = store.load()?.remove(0);

        assert!(report.changed);
        assert_eq!(updated.status, OffdeskTaskStatus::Queued);
        assert_eq!(updated.attempt_count, 2);
        assert!(updated.background_ticket_id.is_none());
        assert!(updated.last_gate_status.is_none());
        assert!(updated.last_error.is_none());
        assert!(updated.last_provider_fallback.is_none());
        Ok(())
    }

    #[test]
    fn resume_pending_resume_requeues_and_marks_resume_artifact_resumed() -> Result<()> {
        let temp = tempdir()?;
        let store = OffdeskTaskStore::new(temp.path());
        let resume_store = TaskResumeStore::new(temp.path());
        let now = Utc::now();
        let mut task = OffdeskTask::new(input(), now);
        task.status = OffdeskTaskStatus::ResumePending;
        task.background_ticket_id = Some("ticket".to_string());
        task.last_error = Some("stale".to_string());
        store.enqueue(task)?;
        resume_store.mark_resume_pending(resume_state(now))?;

        let report = store.resume_task("task", now + Duration::seconds(1))?;
        let updated = store.load()?.remove(0);
        let resume = resume_store.load()?.remove(0);

        assert!(report.changed);
        assert_eq!(updated.status, OffdeskTaskStatus::Queued);
        assert!(updated.background_ticket_id.is_none());
        assert!(updated.last_error.is_none());
        assert_eq!(resume.status, ResumeStatus::Resumed);
        Ok(())
    }

    #[test]
    fn resume_pending_abandon_cancels_task_and_marks_resume_artifact_abandoned() -> Result<()> {
        let temp = tempdir()?;
        let store = OffdeskTaskStore::new(temp.path());
        let resume_store = TaskResumeStore::new(temp.path());
        let now = Utc::now();
        let mut task = OffdeskTask::new(input(), now);
        task.status = OffdeskTaskStatus::ResumePending;
        store.enqueue(task)?;
        resume_store.mark_resume_pending(resume_state(now))?;

        let report = store.abandon_task("task", now + Duration::seconds(1))?;
        let updated = store.load()?.remove(0);
        let resume = resume_store.load()?.remove(0);

        assert!(report.changed);
        assert_eq!(updated.status, OffdeskTaskStatus::Cancelled);
        assert_eq!(resume.status, ResumeStatus::Abandoned);
        Ok(())
    }

    #[test]
    fn completed_task_lifecycle_commands_are_no_ops() -> Result<()> {
        let temp = tempdir()?;
        let store = OffdeskTaskStore::new(temp.path());
        let now = Utc::now();
        let mut task = OffdeskTask::new(input(), now);
        task.status = OffdeskTaskStatus::Completed;
        store.enqueue(task)?;

        let cancel = store.cancel_task("task", None, now + Duration::seconds(1))?;
        let retry = store.retry_task("task", now + Duration::seconds(2))?;
        let updated = store.load()?.remove(0);

        assert!(!cancel.changed);
        assert!(!retry.changed);
        assert_eq!(updated.status, OffdeskTaskStatus::Completed);
        Ok(())
    }
}
