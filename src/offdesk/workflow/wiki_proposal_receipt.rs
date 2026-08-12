//! Adaptive-wiki proposal receipt construction.

use chrono::{DateTime, Utc};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::offdesk::{
    operator_safe_text, AdaptiveWikiAuditAction, AdaptiveWikiAuditRecord,
    AdaptiveWikiReviewProposal, AdaptiveWikiReviewProposalAction,
    AdaptiveWikiReviewProposalDecision, AdaptiveWikiReviewProposalEventRecord,
};

pub struct AdaptiveWikiProposalReceiptInput<'a> {
    pub proposal_id: &'a str,
    pub audit_id: &'a str,
    pub event_id: &'a str,
    pub preview_command: &'a str,
    pub current_proposal: Option<&'a AdaptiveWikiReviewProposal>,
    pub audit: Option<AdaptiveWikiAuditRecord>,
    pub event: Option<AdaptiveWikiReviewProposalEventRecord>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveWikiProposalReceipt {
    pub generated_at: DateTime<Utc>,
    pub read_only: bool,
    pub status: &'static str,
    pub proposal: AdaptiveWikiProposalReceiptSubject,
    pub preview_command: String,
    pub preview_command_sha256: String,
    pub audit: Option<AdaptiveWikiAuditRecord>,
    pub event: Option<AdaptiveWikiReviewProposalEventRecord>,
    pub checks: Vec<AdaptiveWikiProposalReceiptCheck>,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveWikiProposalReceiptSubject {
    pub proposal_id: String,
    pub current: bool,
    pub action: Option<AdaptiveWikiReviewProposalAction>,
    pub subject_kind: String,
    pub subject_id: String,
    pub lifecycle_decision: Option<AdaptiveWikiReviewProposalDecision>,
    pub lifecycle_event_id: Option<String>,
    pub evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AdaptiveWikiProposalReceiptCheck {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

pub fn build_adaptive_wiki_proposal_receipt(
    input: AdaptiveWikiProposalReceiptInput<'_>,
) -> AdaptiveWikiProposalReceipt {
    let AdaptiveWikiProposalReceiptInput {
        proposal_id,
        audit_id,
        event_id,
        preview_command,
        current_proposal,
        audit,
        event,
        generated_at,
    } = input;
    let subject = receipt_subject(proposal_id, current_proposal, event.as_ref());
    let safe_command = operator_safe_text(preview_command);

    let mut checks = Vec::new();
    checks.push(receipt_check(
        "preview_command_supplied",
        !safe_command.is_empty(),
        "preview command is present",
    ));
    checks.push(receipt_check(
        "audit_found",
        audit.is_some(),
        audit.as_ref().map_or_else(
            || format!("audit id {audit_id} was not found"),
            |audit| format!("found {}", audit_summary(audit)),
        ),
    ));
    checks.push(receipt_check(
        "event_found",
        event.is_some(),
        event.as_ref().map_or_else(
            || format!("event id {event_id} was not found"),
            |event| format!("found {}", event_summary(event)),
        ),
    ));
    let event_matches = event
        .as_ref()
        .is_some_and(|event| event_matches_subject(&subject, event));
    checks.push(receipt_check(
        "event_matches_proposal",
        event_matches,
        event.as_ref().map_or_else(
            || "event metadata unavailable because event was not found".to_string(),
            |event| {
                match_detail(
                    event_matches,
                    "event",
                    event_summary(event),
                    subject_summary(&subject),
                )
            },
        ),
    ));
    let audit_matches = audit
        .as_ref()
        .is_some_and(|audit| audit_matches_subject(&subject, audit));
    checks.push(receipt_check(
        "audit_matches_proposal",
        audit_matches,
        audit.as_ref().map_or_else(
            || "audit metadata unavailable because audit was not found".to_string(),
            |audit| {
                match_detail(
                    audit_matches,
                    "audit",
                    audit_summary(audit),
                    subject_summary(&subject),
                )
            },
        ),
    ));
    let audit_event_aligned = audit
        .as_ref()
        .zip(event.as_ref())
        .is_some_and(|(audit, event)| audit_event_targets_align(audit, event, &subject));
    checks.push(receipt_check(
        "audit_event_target_alignment",
        audit_event_aligned,
        audit.as_ref().zip(event.as_ref()).map_or_else(
            || "audit/event alignment unavailable because audit or event was not found".to_string(),
            |(audit, event)| {
                match_detail(
                    audit_event_aligned,
                    "audit/event",
                    audit_summary(audit),
                    event_summary(event),
                )
            },
        ),
    ));

    let blockers = checks
        .iter()
        .filter(|check| !check.passed)
        .map(|check| check.detail.clone())
        .collect::<Vec<_>>();
    AdaptiveWikiProposalReceipt {
        generated_at,
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
    }
}

fn receipt_subject(
    proposal_id: &str,
    current_proposal: Option<&AdaptiveWikiReviewProposal>,
    event: Option<&AdaptiveWikiReviewProposalEventRecord>,
) -> AdaptiveWikiProposalReceiptSubject {
    if let Some(proposal) = current_proposal {
        return AdaptiveWikiProposalReceiptSubject {
            proposal_id: operator_safe_text(proposal_id),
            current: true,
            action: Some(proposal.action),
            subject_kind: operator_safe_text(&proposal.subject_kind),
            subject_id: operator_safe_text(&proposal.subject_id),
            lifecycle_decision: proposal
                .lifecycle
                .as_ref()
                .map(|lifecycle| lifecycle.decision),
            lifecycle_event_id: proposal
                .lifecycle
                .as_ref()
                .map(|lifecycle| operator_safe_text(&lifecycle.latest_event_id)),
            evidence_refs: proposal
                .evidence_refs
                .iter()
                .map(|value| operator_safe_text(value))
                .collect(),
        };
    }

    if let Some(event) = event.filter(|event| event.proposal_id == proposal_id) {
        return AdaptiveWikiProposalReceiptSubject {
            proposal_id: operator_safe_text(proposal_id),
            current: false,
            action: event.proposal_action,
            subject_kind: operator_safe_text(&event.subject_kind),
            subject_id: operator_safe_text(&event.subject_id),
            lifecycle_decision: Some(event.decision),
            lifecycle_event_id: Some(operator_safe_text(&event.id)),
            evidence_refs: event
                .evidence_refs
                .iter()
                .map(|value| operator_safe_text(value))
                .collect(),
        };
    }

    AdaptiveWikiProposalReceiptSubject {
        proposal_id: operator_safe_text(proposal_id),
        current: false,
        action: None,
        subject_kind: String::new(),
        subject_id: String::new(),
        lifecycle_decision: None,
        lifecycle_event_id: None,
        evidence_refs: Vec::new(),
    }
}

fn receipt_check(
    name: &'static str,
    passed: bool,
    detail: impl Into<String>,
) -> AdaptiveWikiProposalReceiptCheck {
    AdaptiveWikiProposalReceiptCheck {
        name,
        passed,
        detail: operator_safe_text(&detail.into()),
    }
}

fn match_detail(passed: bool, label: &str, actual: String, expected: String) -> String {
    if passed {
        format!("{label} matches {expected}")
    } else {
        format!("{label} mismatch: actual {actual}; expected {expected}")
    }
}

fn subject_summary(subject: &AdaptiveWikiProposalReceiptSubject) -> String {
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

fn event_summary(event: &AdaptiveWikiReviewProposalEventRecord) -> String {
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

fn audit_summary(audit: &AdaptiveWikiAuditRecord) -> String {
    format!(
        "audit={} action={:?} subject={} candidate={} entry={}",
        audit.id,
        audit.action,
        empty_dash(&audit.subject_id),
        audit.candidate_id.as_deref().unwrap_or("-"),
        audit.entry_id.as_deref().unwrap_or("-")
    )
}

fn event_matches_subject(
    subject: &AdaptiveWikiProposalReceiptSubject,
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

fn audit_matches_subject(
    subject: &AdaptiveWikiProposalReceiptSubject,
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
    subject: &AdaptiveWikiProposalReceiptSubject,
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

fn empty_dash(value: &str) -> &str {
    if value.trim().is_empty() {
        "-"
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_audit_and_event_fail_closed() {
        let receipt = build_adaptive_wiki_proposal_receipt(AdaptiveWikiProposalReceiptInput {
            proposal_id: "proposal-test",
            audit_id: "audit-test",
            event_id: "event-test",
            preview_command: "forager offdesk wiki promote candidate-test",
            current_proposal: None,
            audit: None,
            event: None,
            generated_at: Utc::now(),
        });

        assert_eq!(receipt.status, "incomplete");
        assert!(!receipt.blockers.is_empty());
        assert_eq!(receipt.preview_command_sha256.len(), 64);
        assert!(receipt.read_only);
    }
}
