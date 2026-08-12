//! Closeout receipt construction and accepted-truth boundary calculation.

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::offdesk::operator_safe_text;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CloseoutVerdict {
    Approved,
    Revise,
    Blocked,
}

impl CloseoutVerdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Revise => "revise",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseoutReceipt {
    pub schema: &'static str,
    pub receipt_id: String,
    pub closeout_id: String,
    pub review_id: String,
    pub generated_at: DateTime<Utc>,
    pub reviewed_at: DateTime<Utc>,
    pub verdict: CloseoutVerdict,
    pub acceptance_status: &'static str,
    pub accepted_scope: Vec<String>,
    pub executed_scope: Vec<String>,
    pub evidence_status: &'static str,
    pub verification_status: &'static str,
    pub open_decisions: Vec<CloseoutReceiptDecision>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub resolved_open_decisions: Vec<CloseoutResolvedDecision>,
    pub missing_evidence: Vec<String>,
    pub required_first_reads: Vec<String>,
    pub unsafe_operations: Vec<String>,
    pub retention_review: &'static str,
    pub wiki_promotion_state: &'static str,
    pub stale_task_count: usize,
    pub next_safe_action: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retirement_reason: Option<String>,
    pub source_artifacts: CloseoutReceiptArtifacts,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseoutReceiptDecision {
    pub kind: String,
    pub detail: String,
    pub suggested_command: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseoutResolvedDecision {
    pub kind: String,
    pub decision: String,
    pub reason: String,
    pub reviewer: String,
    pub resolved_at: DateTime<Utc>,
    pub applies_to_decision: CloseoutReceiptDecision,
    pub does_not_authorize: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseoutReceiptArtifacts {
    pub closeout_plan_json: String,
    pub closeout_plan_markdown: Option<String>,
    pub cleanup_manifest_json: Option<String>,
    pub commercial_review_packet: Option<String>,
    pub return_package_markdown: String,
    pub review_record_json: String,
    pub review_file: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CloseoutReceiptTaskRef {
    pub project_key: String,
    pub request_id: String,
    pub task_id: String,
}

pub struct CloseoutReceiptArtifactPathsInput {
    pub closeout_plan_json: String,
    pub return_package_markdown: String,
    pub review_record_json: String,
    pub review_file: Option<String>,
}

pub struct CloseoutReceiptBuildInput<'a> {
    pub plan: &'a Value,
    pub verdict: CloseoutVerdict,
    pub unsafe_operations: &'a [String],
    pub missing_evidence: &'a [String],
    pub required_first_reads: &'a [String],
    pub artifacts: CloseoutReceiptArtifactPathsInput,
    pub closeout_id: &'a str,
    pub review_id: &'a str,
    pub closeout_generated_at: Option<DateTime<Utc>>,
    pub reviewed_at: DateTime<Utc>,
    pub applies_to_tasks: &'a [CloseoutReceiptTaskRef],
    pub stale_task_count: usize,
}

pub fn build_closeout_receipt(input: CloseoutReceiptBuildInput<'_>) -> CloseoutReceipt {
    let CloseoutReceiptBuildInput {
        plan,
        verdict,
        unsafe_operations,
        missing_evidence,
        required_first_reads,
        artifacts,
        closeout_id,
        review_id,
        closeout_generated_at,
        reviewed_at,
        applies_to_tasks,
        stale_task_count,
    } = input;
    let open_decisions = closeout_receipt_open_decisions(plan);
    let unsafe_operations = normalize_closeout_review_items(unsafe_operations);
    let missing_evidence = normalize_closeout_review_items(missing_evidence);
    let required_first_reads = normalize_closeout_review_items(required_first_reads);
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
    let acceptance_status = match verdict {
        CloseoutVerdict::Approved if has_followups => "approved_with_followups",
        CloseoutVerdict::Approved => "accepted",
        CloseoutVerdict::Revise => "revision_required",
        CloseoutVerdict::Blocked => "blocked",
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
        verdict,
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
            closeout_plan_json: operator_safe_text(&artifacts.closeout_plan_json),
            closeout_plan_markdown: closeout_plan_artifact(
                plan,
                "/artifacts/closeout_plan_markdown",
            ),
            cleanup_manifest_json: closeout_plan_artifact(plan, "/artifacts/cleanup_manifest_json"),
            commercial_review_packet: closeout_plan_artifact(
                plan,
                "/artifacts/commercial_review_packet",
            ),
            return_package_markdown: operator_safe_text(&artifacts.return_package_markdown),
            review_record_json: operator_safe_text(&artifacts.review_record_json),
            review_file: artifacts.review_file.map(|path| operator_safe_text(&path)),
        },
    }
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

pub fn closeout_receipt_next_safe_action(
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

pub fn closeout_receipt_decisions_from_value(receipt: &Value) -> Vec<CloseoutReceiptDecision> {
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

pub fn closeout_receipt_string_list(receipt: &Value, key: &str) -> Vec<String> {
    receipt
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(operator_safe_text)
        .collect()
}

pub fn closeout_retention_status_after_preserve_in_place(
    receipt: &Value,
    all_requested_decisions_resolved: bool,
) -> &'static str {
    if all_requested_decisions_resolved {
        "resolved_preserve_in_place"
    } else {
        match receipt.get("retention_review").and_then(Value::as_str) {
            Some("not_required") => "not_required",
            Some("resolved_preserve_in_place") => "resolved_preserve_in_place",
            _ => "required",
        }
    }
}

pub fn closeout_receipt_evidence_status(receipt: &Value) -> &'static str {
    match receipt.get("evidence_status").and_then(Value::as_str) {
        Some("review_ready") => "review_ready",
        _ => "missing",
    }
}

pub fn closeout_receipt_wiki_state(receipt: &Value) -> &'static str {
    match receipt.get("wiki_promotion_state").and_then(Value::as_str) {
        Some("review_required") => "review_required",
        Some("audit_unavailable") => "audit_unavailable",
        Some("no_candidate") => "no_candidate",
        Some("not_requested") => "not_requested",
        Some("not_required") => "not_required",
        _ => "audit_unavailable",
    }
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
        .map(operator_safe_text)
}

fn closeout_plan_string(value: &Value, field: &str, fallback: &str) -> String {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(operator_safe_text)
        .unwrap_or_else(|| fallback.to_string())
}

pub(super) fn normalize_closeout_review_items(values: &[String]) -> Vec<String> {
    values
        .iter()
        .map(|value| operator_safe_text(value.trim()))
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

fn truncate_closeout_text(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn short_uuid() -> String {
    Uuid::new_v4().to_string()[..8].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn build(verdict: CloseoutVerdict, plan: &Value) -> CloseoutReceipt {
        build_closeout_receipt(CloseoutReceiptBuildInput {
            plan,
            verdict,
            unsafe_operations: &[],
            missing_evidence: &[],
            required_first_reads: &[],
            artifacts: CloseoutReceiptArtifactPathsInput {
                closeout_plan_json: "closeout_plan.json".to_string(),
                return_package_markdown: "RETURN_PACKAGE.md".to_string(),
                review_record_json: "review.json".to_string(),
                review_file: None,
            },
            closeout_id: "closeout-test",
            review_id: "review-test",
            closeout_generated_at: None,
            reviewed_at: Utc::now(),
            applies_to_tasks: &[CloseoutReceiptTaskRef {
                project_key: "project".to_string(),
                request_id: "request".to_string(),
                task_id: "task".to_string(),
            }],
            stale_task_count: 0,
        })
    }

    #[test]
    fn approved_without_followups_is_accepted_truth() {
        let receipt = build(CloseoutVerdict::Approved, &json!({"summary": {}}));
        assert_eq!(receipt.acceptance_status, "accepted");
        assert_eq!(receipt.verification_status, "recorded");
        assert_eq!(receipt.accepted_scope, receipt.executed_scope);
    }

    #[test]
    fn open_decision_keeps_approved_result_pending() {
        let receipt = build(
            CloseoutVerdict::Approved,
            &json!({
                "summary": {},
                "open_decisions": [{
                    "kind": "archive_review",
                    "detail": "Review archive retention.",
                    "suggested_command": "forager offdesk closeout-decision"
                }]
            }),
        );
        assert_eq!(receipt.acceptance_status, "approved_with_followups");
        assert_eq!(receipt.open_decisions.len(), 1);
        assert!(receipt.next_safe_action.contains("archive_review"));
    }

    #[test]
    fn revision_never_records_accepted_scope() {
        let receipt = build(CloseoutVerdict::Revise, &json!({"summary": {}}));
        assert_eq!(receipt.acceptance_status, "revision_required");
        assert_ne!(receipt.accepted_scope, receipt.executed_scope);
    }

    #[test]
    fn stored_receipt_helpers_keep_unknown_values_fail_closed() {
        let stored = json!({
            "retention_review": "unexpected",
            "evidence_status": "unexpected",
            "wiki_promotion_state": "unexpected",
            "open_decisions": [{"kind": "archive_review"}],
            "missing_evidence": ["artifact", 3]
        });

        assert_eq!(
            closeout_retention_status_after_preserve_in_place(&stored, false),
            "required"
        );
        assert_eq!(closeout_receipt_evidence_status(&stored), "missing");
        assert_eq!(closeout_receipt_wiki_state(&stored), "audit_unavailable");
        assert_eq!(closeout_receipt_decisions_from_value(&stored).len(), 1);
        assert_eq!(
            closeout_receipt_string_list(&stored, "missing_evidence"),
            ["artifact"]
        );
    }
}
