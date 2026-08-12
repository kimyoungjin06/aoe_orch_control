//! CLI adapter and file export for adaptive-wiki proposal receipts.

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::Serialize;
use std::{fs, io, path::PathBuf};

use super::{
    operator_safe_json_value, read_only_profile_dir, require_non_empty_arg, wiki_store,
    write_new_file, WikiProposalReceiptArgs,
};
use crate::offdesk::{
    build_adaptive_wiki_proposal_receipt, AdaptiveWikiProposalReceipt,
    AdaptiveWikiProposalReceiptInput, AdaptiveWikiReviewQueueFilter,
};

#[derive(Serialize)]
struct WikiProposalReceiptExportReceipt<'a> {
    exported_to: String,
    bytes_written: usize,
    receipt: &'a AdaptiveWikiProposalReceipt,
}

struct WikiProposalReceiptExport {
    path: PathBuf,
    bytes_written: usize,
}

pub(super) async fn wiki_proposal_receipt(
    profile: &str,
    args: WikiProposalReceiptArgs,
) -> Result<()> {
    let proposal_id = require_non_empty_arg("proposal_id", &args.proposal_id)?;
    let audit_id = require_non_empty_arg("--audit-id", &args.audit_id)?;
    let event_id = require_non_empty_arg("--event-id", &args.event_id)?;
    let command = require_non_empty_arg("--command", &args.command)?;
    let now = Utc::now();
    let store = wiki_store(profile)?;
    let report =
        store.generate_review_report_filtered(true, now, AdaptiveWikiReviewQueueFilter::All)?;
    let audit = store
        .load_audit_records()?
        .into_iter()
        .find(|audit| audit.id == audit_id);
    let event = store
        .load_review_proposal_events()?
        .into_iter()
        .find(|event| event.id == event_id);
    let current_proposal = report
        .proposals
        .iter()
        .find(|proposal| proposal.id == proposal_id);
    let receipt = build_adaptive_wiki_proposal_receipt(AdaptiveWikiProposalReceiptInput {
        proposal_id,
        audit_id,
        event_id,
        preview_command: command,
        current_proposal,
        audit,
        event,
        generated_at: now,
    });
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

fn print_wiki_proposal_receipt(receipt: &AdaptiveWikiProposalReceipt) {
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

fn write_wiki_proposal_receipt_export(
    profile: &str,
    receipt: &AdaptiveWikiProposalReceipt,
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

fn empty_dash(value: &str) -> &str {
    if value.trim().is_empty() {
        "-"
    } else {
        value
    }
}
