//! Pure Markdown renderers for closeout artifacts.

use std::collections::BTreeSet;

use super::{
    truncate_closeout_text, CloseoutDocumentationGovernance, CloseoutFileOperation,
    CloseoutImplementationPacketCoverage, CloseoutPacketCoverageDetail, CloseoutReadRef,
    OffdeskCloseoutReport,
};

const CLOSEOUT_RETURN_DECISION_LIMIT: usize = 5;
const CLOSEOUT_RETURN_FIRST_READ_LIMIT: usize = 5;
const CLOSEOUT_RETURN_EVIDENCE_LIMIT: usize = 5;
const CLOSEOUT_RETURN_GOVERNANCE_PATH_LIMIT: usize = 3;
pub(super) fn render_closeout_plan_markdown(report: &OffdeskCloseoutReport) -> String {
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

pub(super) fn render_closeout_return_package(report: &OffdeskCloseoutReport) -> String {
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

pub(super) fn render_commercial_review_packet(report: &OffdeskCloseoutReport) -> String {
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
