//! Adaptive-wiki mutation result envelopes and operator presentation.
//!
//! Command handlers retain all validation, canonical writes, audit appends, and
//! receipt creation. This module only serializes and renders their completed
//! results.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;

use super::{adaptive_wiki_agent_modes_label, wiki_scope_label};
use crate::offdesk::{
    AdaptiveWikiAuditRecord, AdaptiveWikiHumanCandidate, AdaptiveWikiHumanEntry,
    AdaptiveWikiPromotionReceipt,
};

#[derive(Serialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub(super) enum WikiMutationResult {
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

pub(super) fn present_wiki_mutation(result: &WikiMutationResult, json: bool) -> Result<()> {
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
            println!("  kind: {:?}", entry.kind);
            println!(
                "  agent_modes: {}",
                adaptive_wiki_agent_modes_label(&entry.agent_modes)
            );
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
