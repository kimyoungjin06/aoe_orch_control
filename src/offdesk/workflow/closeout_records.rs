//! Typed closeout review, decision-resolution, and retirement records.

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::offdesk::operator_safe_text;

use super::closeout_receipt::normalize_closeout_review_items;
use super::{
    build_closeout_receipt, closeout_receipt_decisions_from_value,
    closeout_receipt_evidence_status, closeout_receipt_next_safe_action,
    closeout_receipt_string_list, closeout_receipt_wiki_state,
    closeout_retention_status_after_preserve_in_place, CloseoutReceipt,
    CloseoutReceiptArtifactPathsInput, CloseoutReceiptArtifacts, CloseoutReceiptBuildInput,
    CloseoutReceiptTaskRef, CloseoutResolvedDecision, CloseoutVerdict,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseoutDecisionResolution {
    PreserveInPlace,
}

impl CloseoutDecisionResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreserveInPlace => "preserve_in_place",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseoutReviewRecord {
    pub reviewed_at: DateTime<Utc>,
    pub review_id: String,
    pub closeout_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closeout_generated_at: Option<DateTime<Utc>>,
    pub profile: String,
    pub artifact_dir: String,
    pub verdict: CloseoutVerdict,
    pub reviewer: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review_provider: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub review_file: Option<String>,
    pub unsafe_operations: Vec<String>,
    pub missing_evidence: Vec<String>,
    pub required_first_reads: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub notes: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision_resolution: Option<CloseoutDecisionResolutionRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub closeout_retirement: Option<CloseoutRetirementRecord>,
    pub applies_to_task_ids: Vec<String>,
    pub applies_to_tasks: Vec<CloseoutReceiptTaskRef>,
    pub read_only_project_state: bool,
    pub applies_file_operations: bool,
    pub closeout_receipt: CloseoutReceipt,
    pub artifacts: CloseoutReviewArtifactPaths,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseoutReviewArtifactPaths {
    pub closeout_plan_json: String,
    pub review_record_json: String,
    pub closeout_receipt_json: String,
    pub return_package_markdown: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseoutDecisionResolutionRecord {
    pub kind: String,
    pub decision: String,
    pub reason: String,
    pub reviewer: String,
    pub resolved_at: DateTime<Utc>,
    pub source_review_record_json: String,
    pub source_receipt_id: Option<String>,
    pub does_not_authorize: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseoutRetirementRecord {
    pub reason: String,
    pub reviewer: String,
    pub retired_at: DateTime<Utc>,
    pub source_review_record_json: Option<String>,
    pub excluded_accepted_tasks: Vec<String>,
    pub does_not_authorize: Vec<String>,
}

pub struct CloseoutDecisionRecordBuildInput<'a> {
    pub plan: &'a Value,
    pub source_review: &'a Value,
    pub source_review_record_json: &'a str,
    pub profile: &'a str,
    pub artifact_dir: &'a str,
    pub artifacts: CloseoutReviewArtifactPaths,
    pub closeout_id: &'a str,
    pub closeout_generated_at: Option<DateTime<Utc>>,
    pub applies_to_tasks: Vec<CloseoutReceiptTaskRef>,
    pub kind: &'a str,
    pub decision: CloseoutDecisionResolution,
    pub reviewer: &'a str,
    pub reason: &'a str,
    pub reviewed_at: DateTime<Utc>,
    pub review_id: &'a str,
}

pub struct CloseoutRetirementRecordBuildInput<'a> {
    pub plan: &'a Value,
    pub source_review: Option<&'a Value>,
    pub source_review_record_json: Option<String>,
    pub profile: &'a str,
    pub artifact_dir: &'a str,
    pub artifacts: CloseoutReviewArtifactPaths,
    pub closeout_id: &'a str,
    pub closeout_generated_at: Option<DateTime<Utc>>,
    pub applies_to_tasks: Vec<CloseoutReceiptTaskRef>,
    pub excluded_accepted_tasks: Vec<String>,
    pub reviewer: &'a str,
    pub reason: &'a str,
    pub reviewed_at: DateTime<Utc>,
    pub review_id: &'a str,
}

pub struct CloseoutReviewRecordBuildInput<'a> {
    pub plan: &'a Value,
    pub profile: &'a str,
    pub artifact_dir: &'a str,
    pub artifacts: CloseoutReviewArtifactPaths,
    pub closeout_id: &'a str,
    pub closeout_generated_at: Option<DateTime<Utc>>,
    pub applies_to_tasks: Vec<CloseoutReceiptTaskRef>,
    pub verdict: CloseoutVerdict,
    pub reviewer: &'a str,
    pub review_provider: Option<&'a str>,
    pub review_file: Option<String>,
    pub unsafe_operations: &'a [String],
    pub missing_evidence: &'a [String],
    pub required_first_reads: &'a [String],
    pub notes: Option<&'a str>,
    pub stale_task_count: usize,
    pub reviewed_at: DateTime<Utc>,
    pub review_id: &'a str,
}

pub fn build_closeout_review_record(
    input: CloseoutReviewRecordBuildInput<'_>,
) -> CloseoutReviewRecord {
    let CloseoutReviewRecordBuildInput {
        plan,
        profile,
        artifact_dir,
        artifacts,
        closeout_id,
        closeout_generated_at,
        applies_to_tasks,
        verdict,
        reviewer,
        review_provider,
        review_file,
        unsafe_operations,
        missing_evidence,
        required_first_reads,
        notes,
        stale_task_count,
        reviewed_at,
        review_id,
    } = input;
    let review_file = review_file.map(|path| operator_safe_text(&path));
    let closeout_receipt = build_closeout_receipt(CloseoutReceiptBuildInput {
        plan,
        verdict,
        unsafe_operations,
        missing_evidence,
        required_first_reads,
        artifacts: CloseoutReceiptArtifactPathsInput {
            closeout_plan_json: artifacts.closeout_plan_json.clone(),
            return_package_markdown: artifacts.return_package_markdown.clone(),
            review_record_json: artifacts.review_record_json.clone(),
            review_file: review_file.clone(),
        },
        closeout_id,
        review_id,
        closeout_generated_at,
        reviewed_at,
        applies_to_tasks: &applies_to_tasks,
        stale_task_count,
    });
    let applies_to_task_ids = applies_to_tasks
        .iter()
        .map(|task| task.task_id.clone())
        .collect();

    CloseoutReviewRecord {
        reviewed_at,
        review_id: review_id.to_string(),
        closeout_id: closeout_id.to_string(),
        closeout_generated_at,
        profile: operator_safe_text(profile),
        artifact_dir: operator_safe_text(artifact_dir),
        verdict,
        reviewer: operator_safe_text(reviewer.trim()),
        review_provider: review_provider.map(|value| operator_safe_text(value.trim())),
        review_file,
        unsafe_operations: normalize_closeout_review_items(unsafe_operations),
        missing_evidence: normalize_closeout_review_items(missing_evidence),
        required_first_reads: normalize_closeout_review_items(required_first_reads),
        notes: notes.map(|value| truncate_closeout_text(&operator_safe_text(value), 2000)),
        decision_resolution: None,
        closeout_retirement: None,
        applies_to_task_ids,
        applies_to_tasks,
        read_only_project_state: true,
        applies_file_operations: false,
        closeout_receipt,
        artifacts,
    }
}

pub fn build_closeout_decision_record(
    input: CloseoutDecisionRecordBuildInput<'_>,
) -> Result<CloseoutReviewRecord> {
    let CloseoutDecisionRecordBuildInput {
        plan,
        source_review,
        source_review_record_json,
        profile,
        artifact_dir,
        artifacts,
        closeout_id,
        closeout_generated_at,
        applies_to_tasks,
        kind,
        decision,
        reviewer,
        reason,
        reviewed_at,
        review_id,
    } = input;
    let kind = operator_safe_text(kind.trim());
    let reason = truncate_closeout_text(&operator_safe_text(reason.trim()), 2000);
    let reviewer = operator_safe_text(reviewer.trim());
    if kind.is_empty() {
        bail!("closeout decision kind must not be empty");
    }
    if reason.is_empty() {
        bail!("closeout decision reason must not be empty");
    }
    if decision == CloseoutDecisionResolution::PreserveInPlace && kind != "archive_review" {
        bail!(
            "preserve-in-place closeout decisions are currently supported only for archive_review"
        );
    }

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

    let mut matched_decisions = Vec::new();
    let mut remaining_decisions = Vec::new();
    for open_decision in closeout_receipt_decisions_from_value(source_receipt) {
        if open_decision.kind == kind {
            matched_decisions.push(open_decision);
        } else {
            remaining_decisions.push(open_decision);
        }
    }
    if matched_decisions.is_empty() {
        bail!("latest closeout receipt has no open decision of kind {kind}");
    }

    let missing_evidence = closeout_receipt_string_list(source_receipt, "missing_evidence");
    let required_first_reads = closeout_receipt_string_list(source_receipt, "required_first_reads");
    let unsafe_operations = closeout_receipt_string_list(source_receipt, "unsafe_operations");
    let stale_task_count = source_receipt
        .get("stale_task_count")
        .and_then(Value::as_u64)
        .unwrap_or_default() as usize;
    if stale_task_count > 0 {
        bail!(
            "closeout decision resolution cannot accept a stale closeout; regenerate closeout first"
        );
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
    let wiki_promotion_state = closeout_receipt_wiki_state(source_receipt);
    if matches!(
        wiki_promotion_state,
        "review_required" | "audit_unavailable"
    ) {
        bail!("closeout decision resolution cannot bypass wiki promotion follow-ups");
    }

    let applies_to_task_ids = applies_to_tasks
        .iter()
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    let executed_scope = closeout_executed_scope(&applies_to_tasks);
    let does_not_authorize = closeout_decision_does_not_authorize();
    let resolved_open_decisions = matched_decisions
        .into_iter()
        .map(|open_decision| CloseoutResolvedDecision {
            kind: kind.clone(),
            decision: decision.as_str().to_string(),
            reason: reason.clone(),
            reviewer: reviewer.clone(),
            resolved_at: reviewed_at,
            applies_to_decision: open_decision,
            does_not_authorize: does_not_authorize.clone(),
        })
        .collect::<Vec<_>>();
    let decision_resolution = CloseoutDecisionResolutionRecord {
        kind: kind.clone(),
        decision: decision.as_str().to_string(),
        reason: reason.clone(),
        reviewer: reviewer.clone(),
        resolved_at: reviewed_at,
        source_review_record_json: operator_safe_text(source_review_record_json),
        source_receipt_id: source_receipt
            .get("receipt_id")
            .and_then(Value::as_str)
            .map(operator_safe_text),
        does_not_authorize,
    };
    let retention_review = closeout_retention_status_after_preserve_in_place(
        source_receipt,
        remaining_decisions.is_empty(),
    );
    let has_followups = !remaining_decisions.is_empty() || retention_review == "required";
    let acceptance_status = if has_followups {
        "approved_with_followups"
    } else {
        "accepted"
    };
    let verification_status = if has_followups { "pending" } else { "recorded" };
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
        &remaining_decisions,
        &missing_evidence,
        &required_first_reads,
    );
    let closeout_receipt = CloseoutReceipt {
        schema: "closeout_receipt.v1",
        receipt_id: format!("closeout_receipt_{}", short_uuid()),
        closeout_id: closeout_id.to_string(),
        review_id: review_id.to_string(),
        generated_at: closeout_generated_at.unwrap_or(reviewed_at),
        reviewed_at,
        verdict: CloseoutVerdict::Approved,
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
        wiki_promotion_state,
        stale_task_count,
        next_safe_action,
        retirement_reason: None,
        source_artifacts: closeout_source_artifacts(plan, &artifacts),
    };

    Ok(CloseoutReviewRecord {
        reviewed_at,
        review_id: review_id.to_string(),
        closeout_id: closeout_id.to_string(),
        closeout_generated_at,
        profile: operator_safe_text(profile),
        artifact_dir: operator_safe_text(artifact_dir),
        verdict: CloseoutVerdict::Approved,
        reviewer,
        review_provider: Some("operator_decision_resolution".to_string()),
        review_file: None,
        unsafe_operations: Vec::new(),
        missing_evidence: Vec::new(),
        required_first_reads: Vec::new(),
        notes: Some(format!(
            "Resolved closeout decision `{kind}` with `{}`. Reason: {reason}",
            decision.as_str()
        )),
        decision_resolution: Some(decision_resolution),
        closeout_retirement: None,
        applies_to_task_ids,
        applies_to_tasks,
        read_only_project_state: true,
        applies_file_operations: false,
        closeout_receipt,
        artifacts,
    })
}

pub fn build_closeout_retirement_record(
    input: CloseoutRetirementRecordBuildInput<'_>,
) -> Result<CloseoutReviewRecord> {
    let CloseoutRetirementRecordBuildInput {
        plan,
        source_review,
        source_review_record_json,
        profile,
        artifact_dir,
        artifacts,
        closeout_id,
        closeout_generated_at,
        applies_to_tasks,
        excluded_accepted_tasks,
        reviewer,
        reason,
        reviewed_at,
        review_id,
    } = input;
    let reason = truncate_closeout_text(&operator_safe_text(reason.trim()), 2000);
    let reviewer = operator_safe_text(reviewer.trim());
    if reason.is_empty() {
        bail!("closeout retirement reason must not be empty");
    }
    let source_receipt = source_review.and_then(|review| review.get("closeout_receipt"));
    if source_receipt
        .and_then(|receipt| receipt.get("acceptance_status"))
        .and_then(Value::as_str)
        == Some("accepted")
    {
        bail!("accepted closeouts cannot be retired as evidence-incomplete");
    }
    if applies_to_tasks.is_empty() {
        bail!("no non-accepted tasks remain in this closeout to retire");
    }

    let applies_to_task_ids = applies_to_tasks
        .iter()
        .map(|task| task.task_id.clone())
        .collect::<Vec<_>>();
    let executed_scope = closeout_executed_scope(&applies_to_tasks);
    let does_not_authorize = closeout_retirement_does_not_authorize();
    let closeout_retirement = CloseoutRetirementRecord {
        reason: reason.clone(),
        reviewer: reviewer.clone(),
        retired_at: reviewed_at,
        source_review_record_json,
        excluded_accepted_tasks,
        does_not_authorize,
    };
    let closeout_receipt = CloseoutReceipt {
        schema: "closeout_receipt.v1",
        receipt_id: format!("closeout_receipt_{}", short_uuid()),
        closeout_id: closeout_id.to_string(),
        review_id: review_id.to_string(),
        generated_at: closeout_generated_at.unwrap_or(reviewed_at),
        reviewed_at,
        verdict: CloseoutVerdict::Revise,
        acceptance_status: "retired_incomplete",
        accepted_scope: vec![
            "No accepted scope; historical closeout retired as evidence-incomplete.".to_string(),
        ],
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
        source_artifacts: closeout_source_artifacts(plan, &artifacts),
    };

    Ok(CloseoutReviewRecord {
        reviewed_at,
        review_id: review_id.to_string(),
        closeout_id: closeout_id.to_string(),
        closeout_generated_at,
        profile: operator_safe_text(profile),
        artifact_dir: operator_safe_text(artifact_dir),
        verdict: CloseoutVerdict::Revise,
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
    })
}

fn closeout_source_artifacts(
    plan: &Value,
    artifacts: &CloseoutReviewArtifactPaths,
) -> CloseoutReceiptArtifacts {
    CloseoutReceiptArtifacts {
        closeout_plan_json: operator_safe_text(&artifacts.closeout_plan_json),
        closeout_plan_markdown: closeout_plan_artifact(plan, "/artifacts/closeout_plan_markdown"),
        cleanup_manifest_json: closeout_plan_artifact(plan, "/artifacts/cleanup_manifest_json"),
        commercial_review_packet: closeout_plan_artifact(
            plan,
            "/artifacts/commercial_review_packet",
        ),
        return_package_markdown: operator_safe_text(&artifacts.return_package_markdown),
        review_record_json: operator_safe_text(&artifacts.review_record_json),
        review_file: None,
    }
}

fn closeout_plan_artifact(plan: &Value, pointer: &str) -> Option<String> {
    plan.pointer(pointer)
        .and_then(Value::as_str)
        .map(operator_safe_text)
}

fn closeout_executed_scope(applies_to_tasks: &[CloseoutReceiptTaskRef]) -> Vec<String> {
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

fn short_uuid() -> String {
    Uuid::new_v4().to_string()[..8].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn task(task_id: &str) -> CloseoutReceiptTaskRef {
        CloseoutReceiptTaskRef {
            project_key: "project".to_string(),
            request_id: "request".to_string(),
            task_id: task_id.to_string(),
        }
    }

    fn artifacts() -> CloseoutReviewArtifactPaths {
        CloseoutReviewArtifactPaths {
            closeout_plan_json: "closeout_plan.json".to_string(),
            review_record_json: "review.json".to_string(),
            closeout_receipt_json: "receipt.json".to_string(),
            return_package_markdown: "RETURN_PACKAGE.md".to_string(),
        }
    }

    fn source_review(wiki_state: &str) -> Value {
        json!({
            "verdict": "approved",
            "closeout_receipt": {
                "receipt_id": "receipt-source",
                "acceptance_status": "approved_with_followups",
                "evidence_status": "review_ready",
                "verification_status": "pending",
                "open_decisions": [{
                    "kind": "archive_review",
                    "detail": "Review archive retention.",
                    "suggested_command": "forager offdesk closeout-decision"
                }],
                "missing_evidence": [],
                "required_first_reads": [],
                "unsafe_operations": [],
                "retention_review": "required",
                "wiki_promotion_state": wiki_state,
                "stale_task_count": 0
            }
        })
    }

    #[test]
    fn clean_approved_review_builds_accepted_record_and_receipt_together() {
        let reviewed_at = Utc::now();
        let record = build_closeout_review_record(CloseoutReviewRecordBuildInput {
            plan: &json!({"summary": {}}),
            profile: "default",
            artifact_dir: "closeout",
            artifacts: artifacts(),
            closeout_id: "closeout-test",
            closeout_generated_at: None,
            applies_to_tasks: vec![task("task-1")],
            verdict: CloseoutVerdict::Approved,
            reviewer: " operator ",
            review_provider: Some(" commercial "),
            review_file: Some("review-output.md".to_string()),
            unsafe_operations: &[],
            missing_evidence: &[],
            required_first_reads: &[],
            notes: Some("clean review"),
            stale_task_count: 0,
            reviewed_at,
            review_id: "review-test",
        });

        assert_eq!(record.verdict, CloseoutVerdict::Approved);
        assert_eq!(record.reviewer, "operator");
        assert_eq!(record.review_provider.as_deref(), Some("commercial"));
        assert_eq!(record.applies_to_task_ids, ["task-1"]);
        assert_eq!(record.closeout_receipt.acceptance_status, "accepted");
        assert_eq!(
            record
                .closeout_receipt
                .source_artifacts
                .review_file
                .as_deref(),
            Some("review-output.md")
        );
    }

    #[test]
    fn review_evidence_lists_and_receipt_followups_share_one_input() {
        let missing_evidence = vec![
            " ".to_string(),
            "none".to_string(),
            "result hash".to_string(),
        ];
        let required_first_reads = vec!["N/A".to_string(), " evidence.md ".to_string()];
        let record = build_closeout_review_record(CloseoutReviewRecordBuildInput {
            plan: &json!({"summary": {}}),
            profile: "default",
            artifact_dir: "closeout",
            artifacts: artifacts(),
            closeout_id: "closeout-test",
            closeout_generated_at: None,
            applies_to_tasks: vec![task("task-1")],
            verdict: CloseoutVerdict::Approved,
            reviewer: "operator",
            review_provider: None,
            review_file: None,
            unsafe_operations: &[],
            missing_evidence: &missing_evidence,
            required_first_reads: &required_first_reads,
            notes: None,
            stale_task_count: 0,
            reviewed_at: Utc::now(),
            review_id: "review-test",
        });

        assert_eq!(record.missing_evidence, ["result hash"]);
        assert_eq!(record.required_first_reads, ["evidence.md"]);
        assert_eq!(record.closeout_receipt.evidence_status, "missing");
        assert_eq!(
            record.closeout_receipt.acceptance_status,
            "approved_with_followups"
        );
        assert_eq!(record.closeout_receipt.missing_evidence, ["result hash"]);
        assert_eq!(
            record.closeout_receipt.required_first_reads,
            ["evidence.md"]
        );
    }

    #[test]
    fn stale_review_never_opens_accepted_scope() {
        let record = build_closeout_review_record(CloseoutReviewRecordBuildInput {
            plan: &json!({"summary": {}}),
            profile: "default",
            artifact_dir: "closeout",
            artifacts: artifacts(),
            closeout_id: "closeout-test",
            closeout_generated_at: None,
            applies_to_tasks: vec![task("task-1")],
            verdict: CloseoutVerdict::Approved,
            reviewer: "operator",
            review_provider: None,
            review_file: None,
            unsafe_operations: &[],
            missing_evidence: &[],
            required_first_reads: &[],
            notes: None,
            stale_task_count: 1,
            reviewed_at: Utc::now(),
            review_id: "review-test",
        });

        assert_eq!(
            record.closeout_receipt.acceptance_status,
            "approved_with_followups"
        );
        assert_ne!(
            record.closeout_receipt.accepted_scope,
            record.closeout_receipt.executed_scope
        );
    }

    #[test]
    fn decision_resolution_accepts_when_the_only_followup_is_resolved() {
        let source = source_review("no_candidate");
        let reviewed_at = Utc::now();
        let record = build_closeout_decision_record(CloseoutDecisionRecordBuildInput {
            plan: &json!({"summary": {}}),
            source_review: &source,
            source_review_record_json: "source_review.json",
            profile: "default",
            artifact_dir: "closeout",
            artifacts: artifacts(),
            closeout_id: "closeout-test",
            closeout_generated_at: None,
            applies_to_tasks: vec![task("task-1")],
            kind: "archive_review",
            decision: CloseoutDecisionResolution::PreserveInPlace,
            reviewer: "operator",
            reason: "Keep the artifact where it is.",
            reviewed_at,
            review_id: "decision-test",
        })
        .unwrap();

        assert_eq!(record.closeout_receipt.acceptance_status, "accepted");
        assert_eq!(record.closeout_receipt.verification_status, "recorded");
        assert!(record.closeout_receipt.open_decisions.is_empty());
        assert_eq!(record.closeout_receipt.resolved_open_decisions.len(), 1);
        assert_eq!(
            record.closeout_receipt.accepted_scope,
            record.closeout_receipt.executed_scope
        );
    }

    #[test]
    fn unknown_wiki_state_blocks_decision_resolution() {
        let source = source_review("unexpected");
        let error = build_closeout_decision_record(CloseoutDecisionRecordBuildInput {
            plan: &json!({"summary": {}}),
            source_review: &source,
            source_review_record_json: "source_review.json",
            profile: "default",
            artifact_dir: "closeout",
            artifacts: artifacts(),
            closeout_id: "closeout-test",
            closeout_generated_at: None,
            applies_to_tasks: vec![task("task-1")],
            kind: "archive_review",
            decision: CloseoutDecisionResolution::PreserveInPlace,
            reviewer: "operator",
            reason: "Keep the artifact where it is.",
            reviewed_at: Utc::now(),
            review_id: "decision-test",
        })
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("cannot bypass wiki promotion follow-ups"));
    }

    #[test]
    fn remaining_decision_keeps_accepted_scope_closed() {
        let mut source = source_review("no_candidate");
        source["closeout_receipt"]["open_decisions"] = json!([
            {
                "kind": "archive_review",
                "detail": "Review archive retention.",
                "suggested_command": "forager offdesk closeout-decision"
            },
            {
                "kind": "operator_choice",
                "detail": "Choose the next operator action.",
                "suggested_command": "forager offdesk closeout-decision"
            }
        ]);
        let record = build_closeout_decision_record(CloseoutDecisionRecordBuildInput {
            plan: &json!({"summary": {}}),
            source_review: &source,
            source_review_record_json: "source_review.json",
            profile: "default",
            artifact_dir: "closeout",
            artifacts: artifacts(),
            closeout_id: "closeout-test",
            closeout_generated_at: None,
            applies_to_tasks: vec![task("task-1")],
            kind: "archive_review",
            decision: CloseoutDecisionResolution::PreserveInPlace,
            reviewer: "operator",
            reason: "Keep the artifact where it is.",
            reviewed_at: Utc::now(),
            review_id: "decision-test",
        })
        .unwrap();

        assert_eq!(
            record.closeout_receipt.acceptance_status,
            "approved_with_followups"
        );
        assert_eq!(record.closeout_receipt.open_decisions.len(), 1);
        assert_ne!(
            record.closeout_receipt.accepted_scope,
            record.closeout_receipt.executed_scope
        );
    }

    #[test]
    fn retirement_records_no_accepted_truth() {
        let record = build_closeout_retirement_record(CloseoutRetirementRecordBuildInput {
            plan: &json!({"summary": {}}),
            source_review: None,
            source_review_record_json: None,
            profile: "default",
            artifact_dir: "closeout",
            artifacts: artifacts(),
            closeout_id: "closeout-test",
            closeout_generated_at: None,
            applies_to_tasks: vec![task("task-1")],
            excluded_accepted_tasks: vec!["project:task-accepted".to_string()],
            reviewer: "operator",
            reason: "Historical evidence is incomplete.",
            reviewed_at: Utc::now(),
            review_id: "retirement-test",
        })
        .unwrap();

        assert_eq!(
            record.closeout_receipt.acceptance_status,
            "retired_incomplete"
        );
        assert_eq!(record.closeout_receipt.evidence_status, "missing");
        assert_eq!(record.closeout_receipt.accepted_scope.len(), 1);
        assert!(record.closeout_retirement.is_some());
    }

    #[test]
    fn accepted_closeout_cannot_be_retired() {
        let source = json!({
            "closeout_receipt": {"acceptance_status": "accepted"}
        });
        let error = build_closeout_retirement_record(CloseoutRetirementRecordBuildInput {
            plan: &json!({"summary": {}}),
            source_review: Some(&source),
            source_review_record_json: Some("source_review.json".to_string()),
            profile: "default",
            artifact_dir: "closeout",
            artifacts: artifacts(),
            closeout_id: "closeout-test",
            closeout_generated_at: None,
            applies_to_tasks: vec![task("task-1")],
            excluded_accepted_tasks: Vec::new(),
            reviewer: "operator",
            reason: "Historical evidence is incomplete.",
            reviewed_at: Utc::now(),
            review_id: "retirement-test",
        })
        .unwrap_err();

        assert!(error
            .to_string()
            .contains("accepted closeouts cannot be retired"));
    }
}
