//! Canonical decision-ledger state transitions.

use anyhow::{bail, Result};
use chrono::Utc;
use uuid::Uuid;

use crate::offdesk::{
    operator_safe_text, DecisionReceipt, DecisionRecord, DecisionStatus, DecisionTraceRef,
    ExecutionHandoff,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionResolutionInput {
    pub decision: String,
    pub note: String,
    pub by: String,
    pub target: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionReceiptInput {
    pub by: String,
    pub result_status: String,
    pub evidence_summary: Vec<String>,
    pub remaining_review: Vec<String>,
}

pub fn resolve_decision_record(
    mut record: DecisionRecord,
    input: &DecisionResolutionInput,
) -> Result<DecisionRecord> {
    let decision = normalize_decision_choice(&input.decision);
    let note = operator_safe_text(input.note.trim());
    if decision_requires_note(&decision) && note.trim().is_empty() {
        bail!("decision `{decision}` requires --note with the bounded direction or blocker");
    }
    let by = operator_safe_text(input.by.trim());
    record.updated_at = Utc::now();
    record.trace_refs.push(DecisionTraceRef {
        kind: "decision_resolution".to_string(),
        label: by,
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
                Some(build_execution_handoff(&record, &decision, &note, input));
        }
    }
    Ok(record)
}

pub fn receipt_decision_record(
    mut record: DecisionRecord,
    input: &DecisionReceiptInput,
) -> Result<DecisionRecord> {
    let Some(handoff) = record.execution_handoff.as_ref() else {
        bail!(
            "decision {} has no execution_handoff to receipt",
            record.decision_id
        );
    };
    let resolved_at = Utc::now();
    let by = operator_safe_text(input.by.trim());
    let result_status = operator_safe_text(input.result_status.trim());
    let evidence_summary = input
        .evidence_summary
        .iter()
        .map(|line| operator_safe_text(line.trim()))
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    let remaining_review = input
        .remaining_review
        .iter()
        .map(|line| operator_safe_text(line.trim()))
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

pub fn normalize_decision_choice(value: &str) -> String {
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
    input: &DecisionResolutionInput,
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
        target: input
            .target
            .as_deref()
            .map(operator_safe_text)
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

fn short_uuid() -> String {
    Uuid::new_v4().to_string()[..8].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offdesk::{
        DecisionMateriality, DecisionRaisedBy, DecisionRequest, DECISION_RECORD_SCHEMA,
    };

    fn sample_record() -> DecisionRecord {
        let now = Utc::now();
        DecisionRecord {
            schema: DECISION_RECORD_SCHEMA.to_string(),
            decision_id: "decision-test".to_string(),
            project_key: "project".to_string(),
            request_id: "request".to_string(),
            task_id: "task".to_string(),
            raised_by: DecisionRaisedBy::Operator,
            source_surface: "test".to_string(),
            materiality: DecisionMateriality::High,
            status: DecisionStatus::UserPending,
            created_at: now,
            updated_at: now,
            decision_request: DecisionRequest {
                kind: "direction".to_string(),
                summary: "Choose the next direction.".to_string(),
                decision_needed: "Continue or stop.".to_string(),
                why_now: Vec::new(),
                current_scope: "Current task only.".to_string(),
                non_authorized_scope: vec!["runtime mutation".to_string()],
                options: Vec::new(),
                evidence_refs: Vec::new(),
                trace_refs: Vec::new(),
            },
            council_review: None,
            judgment_route: None,
            route: None,
            approval_brief: None,
            execution_handoff: None,
            decision_receipt: None,
            trace_refs: Vec::new(),
        }
    }

    fn resolution_input(decision: &str, note: &str) -> DecisionResolutionInput {
        DecisionResolutionInput {
            decision: decision.to_string(),
            note: note.to_string(),
            by: "operator".to_string(),
            target: None,
        }
    }

    #[test]
    fn normalizes_common_operator_choices() {
        assert_eq!(normalize_decision_choice(" proceed "), "continue");
        assert_eq!(normalize_decision_choice("RETRY"), "revise");
        assert_eq!(normalize_decision_choice("cancel"), "stop");
    }

    #[test]
    fn bounded_direction_requires_a_note() {
        let error = resolve_decision_record(sample_record(), &resolution_input("revise", ""))
            .expect_err("revise without a note must fail closed");
        assert!(error.to_string().contains("requires --note"));
    }

    #[test]
    fn resolution_and_receipt_preserve_authority_boundary() -> Result<()> {
        let resolved = resolve_decision_record(
            sample_record(),
            &resolution_input("continue", "Stay within the current task."),
        )?;
        assert_eq!(resolved.status, DecisionStatus::HandoffReady);
        let handoff = resolved
            .execution_handoff
            .as_ref()
            .expect("continue creates a handoff");
        assert_eq!(handoff.target, "agent");
        assert_eq!(handoff.non_authorized_actions, ["runtime mutation"]);

        let receipted = receipt_decision_record(
            resolved,
            &DecisionReceiptInput {
                by: "operator".to_string(),
                result_status: "completed".to_string(),
                evidence_summary: vec!["artifact verified".to_string()],
                remaining_review: vec!["runtime remains gated".to_string()],
            },
        )?;
        assert_eq!(receipted.status, DecisionStatus::Receipted);
        let receipt = receipted
            .decision_receipt
            .expect("receipted state carries a receipt");
        assert_eq!(receipt.result_status, "completed");
        assert!(receipt.authorization_summary.contains("does not authorize"));
        assert_eq!(receipt.remaining_review, ["runtime remains gated"]);
        Ok(())
    }
}
