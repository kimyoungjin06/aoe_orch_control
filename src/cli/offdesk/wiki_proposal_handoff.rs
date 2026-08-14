//! Read-only CLI adapter for adaptive-wiki proposal handoff previews.
//!
//! This module selects and renders governed mutation commands, but it never
//! executes those commands or mutates adaptive-wiki state.

use anyhow::Result;
use chrono::Utc;
use clap::Args;
use serde::Serialize;

use super::{
    operator_safe_json_value, parse_adaptive_wiki_scope, proposal_has_non_stale_decision,
    require_non_empty_arg, shell_quote_arg, wiki_store,
};
use crate::offdesk::{
    AdaptiveWikiReviewProposal, AdaptiveWikiReviewProposalAction,
    AdaptiveWikiReviewProposalDecision, AdaptiveWikiReviewQueueFilter, AdaptiveWikiScope,
};

#[derive(Args)]
pub struct WikiProposalHandoffArgs {
    /// Current curator review proposal id
    proposal_id: String,

    /// Operator-selected mutation path to preview when the proposal is manual
    #[arg(long, value_parser = parse_wiki_proposal_handoff_mutation)]
    mutation: Option<WikiProposalHandoffMutation>,

    /// Scope for a parameterized rescope handoff
    #[arg(long, value_parser = parse_adaptive_wiki_scope)]
    scope: Option<AdaptiveWikiScope>,

    /// Scope reference for a parameterized rescope handoff
    #[arg(long)]
    scope_ref: Option<String>,

    /// Evidence ref for a parameterized counterexample handoff
    #[arg(long = "evidence-ref")]
    evidence_ref: Option<String>,

    /// Entry to deprecate for a parameterized merge cleanup or conflict handoff
    #[arg(long = "deprecated-entry-id")]
    deprecated_entry_id: Option<String>,

    /// Operator rationale to include in the previewed mutation command
    #[arg(long)]
    reason: Option<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WikiProposalHandoffMutation {
    Rescope,
    Deprecate,
    AddCounterexample,
    DeprecateDuplicate,
    Split,
}

fn parse_wiki_proposal_handoff_mutation(
    value: &str,
) -> std::result::Result<WikiProposalHandoffMutation, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "rescope" => Ok(WikiProposalHandoffMutation::Rescope),
        "deprecate" => Ok(WikiProposalHandoffMutation::Deprecate),
        "add_counterexample" | "add-counterexample" => {
            Ok(WikiProposalHandoffMutation::AddCounterexample)
        }
        "deprecate_duplicate" | "deprecate-duplicate" => {
            Ok(WikiProposalHandoffMutation::DeprecateDuplicate)
        }
        "split" => Ok(WikiProposalHandoffMutation::Split),
        _ => Err(
            "mutation must be one of rescope, deprecate, add_counterexample, deprecate_duplicate, split"
                .to_string(),
        ),
    }
}

#[derive(Serialize)]
struct WikiProposalHandoffPreview {
    proposal_id: String,
    action: AdaptiveWikiReviewProposalAction,
    subject_kind: String,
    subject_id: String,
    status: &'static str,
    command: Option<String>,
    reason: String,
    lifecycle_decision: Option<AdaptiveWikiReviewProposalDecision>,
    lifecycle_stale: bool,
    evidence_refs: Vec<String>,
    required_inputs: Vec<WikiProposalHandoffInput>,
    mutation_options: Vec<WikiProposalHandoffMutationOption>,
}

#[derive(Serialize)]
struct WikiProposalHandoffInput {
    name: &'static str,
    cli_flag: Option<&'static str>,
    required: bool,
    description: &'static str,
}

#[derive(Serialize)]
struct WikiProposalHandoffMutationOption {
    mutation: &'static str,
    command_template: String,
    required_inputs: Vec<&'static str>,
    description: &'static str,
}

pub(super) async fn wiki_proposal_handoff(
    profile: &str,
    args: WikiProposalHandoffArgs,
) -> Result<()> {
    require_non_empty_arg("proposal_id", &args.proposal_id)?;
    let report = wiki_store(profile)?.generate_review_report_filtered(
        true,
        Utc::now(),
        AdaptiveWikiReviewQueueFilter::All,
    )?;
    let proposal = report
        .proposals
        .iter()
        .find(|proposal| proposal.id == args.proposal_id)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Adaptive wiki review proposal not found: {}",
                args.proposal_id
            )
        })?;
    let preview = wiki_proposal_handoff_preview(proposal, &args);

    if args.json {
        let value = operator_safe_json_value(serde_json::to_value(&preview)?);
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    print_wiki_proposal_handoff(&preview);
    Ok(())
}

fn wiki_proposal_handoff_preview(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> WikiProposalHandoffPreview {
    let (required_inputs, mutation_options) = manual_handoff_contract(proposal);
    let lifecycle_decision = proposal
        .lifecycle
        .as_ref()
        .map(|lifecycle| lifecycle.decision);
    let lifecycle_stale = proposal
        .lifecycle
        .as_ref()
        .is_some_and(|lifecycle| lifecycle.stale);
    if proposal_has_non_stale_decision(proposal) {
        return WikiProposalHandoffPreview {
            proposal_id: proposal.id.clone(),
            action: proposal.action,
            subject_kind: proposal.subject_kind.clone(),
            subject_id: proposal.subject_id.clone(),
            status: "blocked_by_decision",
            command: None,
            reason: "The proposal already has a non-stale lifecycle decision.".to_string(),
            lifecycle_decision,
            lifecycle_stale,
            evidence_refs: proposal.evidence_refs.clone(),
            required_inputs: Vec::new(),
            mutation_options: Vec::new(),
        };
    }

    if let Some(parameterized) = parameterized_handoff_preview(proposal, args) {
        match parameterized {
            ParameterizedHandoffPreview::Ready { command, reason } => {
                return WikiProposalHandoffPreview {
                    proposal_id: proposal.id.clone(),
                    action: proposal.action,
                    subject_kind: proposal.subject_kind.clone(),
                    subject_id: proposal.subject_id.clone(),
                    status: "ready",
                    command: Some(command),
                    reason,
                    lifecycle_decision,
                    lifecycle_stale,
                    evidence_refs: proposal.evidence_refs.clone(),
                    required_inputs: Vec::new(),
                    mutation_options: Vec::new(),
                };
            }
            ParameterizedHandoffPreview::ManualRequired { reason } => {
                return WikiProposalHandoffPreview {
                    proposal_id: proposal.id.clone(),
                    action: proposal.action,
                    subject_kind: proposal.subject_kind.clone(),
                    subject_id: proposal.subject_id.clone(),
                    status: "manual_required",
                    command: None,
                    reason,
                    lifecycle_decision,
                    lifecycle_stale,
                    evidence_refs: proposal.evidence_refs.clone(),
                    required_inputs,
                    mutation_options,
                };
            }
        }
    }

    if let Some(command) = proposal.suggested_command.as_deref() {
        return WikiProposalHandoffPreview {
            proposal_id: proposal.id.clone(),
            action: proposal.action,
            subject_kind: proposal.subject_kind.clone(),
            subject_id: proposal.subject_id.clone(),
            status: "ready",
            command: Some(crate::offdesk::operator_safe_text(command)),
            reason: "The proposal already includes an exact governed mutation command.".to_string(),
            lifecycle_decision,
            lifecycle_stale,
            evidence_refs: proposal.evidence_refs.clone(),
            required_inputs: Vec::new(),
            mutation_options: Vec::new(),
        };
    }

    if let Some(command) = fallback_proposal_handoff_command(proposal) {
        return WikiProposalHandoffPreview {
            proposal_id: proposal.id.clone(),
            action: proposal.action,
            subject_kind: proposal.subject_kind.clone(),
            subject_id: proposal.subject_id.clone(),
            status: "ready",
            command: Some(command),
            reason: "An exact governed mutation command can be derived from the proposal subject."
                .to_string(),
            lifecycle_decision,
            lifecycle_stale,
            evidence_refs: proposal.evidence_refs.clone(),
            required_inputs: Vec::new(),
            mutation_options: Vec::new(),
        };
    }

    WikiProposalHandoffPreview {
        proposal_id: proposal.id.clone(),
        action: proposal.action,
        subject_kind: proposal.subject_kind.clone(),
        subject_id: proposal.subject_id.clone(),
        status: "manual_required",
        command: None,
        reason: manual_handoff_reason(proposal).to_string(),
        lifecycle_decision,
        lifecycle_stale,
        evidence_refs: proposal.evidence_refs.clone(),
        required_inputs,
        mutation_options,
    }
}

enum ParameterizedHandoffPreview {
    Ready { command: String, reason: String },
    ManualRequired { reason: String },
}

fn parameterized_handoff_preview(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> Option<ParameterizedHandoffPreview> {
    let mutation = args.mutation?;
    Some(match mutation {
        WikiProposalHandoffMutation::Rescope => parameterized_rescope_handoff(proposal, args),
        WikiProposalHandoffMutation::Deprecate => parameterized_deprecate_handoff(proposal, args),
        WikiProposalHandoffMutation::AddCounterexample => {
            parameterized_counterexample_handoff(proposal, args)
        }
        WikiProposalHandoffMutation::DeprecateDuplicate => {
            parameterized_deprecate_duplicate_handoff(proposal, args)
        }
        WikiProposalHandoffMutation::Split => parameterized_split_handoff(proposal),
    })
}

fn parameterized_rescope_handoff(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> ParameterizedHandoffPreview {
    if proposal.subject_kind != "entry"
        || !matches!(
            proposal.action,
            AdaptiveWikiReviewProposalAction::Rescope
                | AdaptiveWikiReviewProposalAction::RenewReview
                | AdaptiveWikiReviewProposalAction::Split
        )
    {
        return manual_handoff_missing(
            "--mutation rescope is only supported for entry rescope, renew-review, or split proposals.",
        );
    }
    let Some(scope) = args.scope else {
        return manual_handoff_missing("--mutation rescope requires --scope.");
    };
    let Some(scope_ref) = handoff_arg_value(args.scope_ref.as_deref()) else {
        return manual_handoff_missing("--mutation rescope requires --scope-ref.");
    };
    let mut command = format!(
        "forager offdesk wiki rescope {} --scope {} --scope-ref {}",
        handoff_subject_arg(&proposal.subject_id),
        adaptive_wiki_scope_arg(scope),
        shell_quote_arg(&scope_ref)
    );
    if let Some(reason) = handoff_arg_value(args.reason.as_deref()) {
        command.push_str(" --reason ");
        command.push_str(&shell_quote_arg(&reason));
    }
    ParameterizedHandoffPreview::Ready {
        command,
        reason: "Operator supplied enough inputs for an exact rescope mutation preview."
            .to_string(),
    }
}

fn parameterized_deprecate_handoff(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> ParameterizedHandoffPreview {
    let supported_standard = proposal.subject_kind == "entry"
        && matches!(
            proposal.action,
            AdaptiveWikiReviewProposalAction::Deprecate
                | AdaptiveWikiReviewProposalAction::RenewReview
        );
    let supported_conflict = proposal_is_projection_conflict(proposal);
    if !supported_standard && !supported_conflict {
        return manual_handoff_missing(
            "--mutation deprecate is only supported for entry deprecate, renew-review, or projection-conflict split proposals.",
        );
    }
    let Some(reason) = handoff_arg_value(args.reason.as_deref()) else {
        return manual_handoff_missing("--mutation deprecate requires --reason.");
    };
    let target_entry_id = if supported_conflict {
        match handoff_arg_value(args.deprecated_entry_id.as_deref()) {
            Some(deprecated_entry_id) => {
                if !projection_conflict_entry_ids(proposal)
                    .iter()
                    .any(|entry_id| entry_id == &deprecated_entry_id)
                {
                    return manual_handoff_missing(
                        "--mutation deprecate for conflict proposals requires --deprecated-entry-id to match the proposal subject or a conflicting entry evidence ref.",
                    );
                }
                deprecated_entry_id
            }
            None => proposal.subject_id.clone(),
        }
    } else {
        proposal.subject_id.clone()
    };
    ParameterizedHandoffPreview::Ready {
        command: deprecate_command(&target_entry_id, &reason),
        reason: "Operator supplied enough inputs for an exact deprecate mutation preview."
            .to_string(),
    }
}

fn parameterized_counterexample_handoff(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> ParameterizedHandoffPreview {
    if proposal.subject_kind != "entry"
        || !matches!(
            proposal.action,
            AdaptiveWikiReviewProposalAction::AddCounterexample
                | AdaptiveWikiReviewProposalAction::RenewReview
                | AdaptiveWikiReviewProposalAction::Split
        )
    {
        return manual_handoff_missing(
            "--mutation add-counterexample is only supported for entry counterexample, renew-review, or split proposals.",
        );
    }
    let Some(evidence_ref) = handoff_arg_value(args.evidence_ref.as_deref()) else {
        return manual_handoff_missing("--mutation add-counterexample requires --evidence-ref.");
    };
    let Some(reason) = handoff_arg_value(args.reason.as_deref()) else {
        return manual_handoff_missing("--mutation add-counterexample requires --reason.");
    };
    ParameterizedHandoffPreview::Ready {
        command: counterexample_command(&proposal.subject_id, &evidence_ref, &reason),
        reason: "Operator supplied enough inputs for an exact add-counterexample mutation preview."
            .to_string(),
    }
}

fn parameterized_deprecate_duplicate_handoff(
    proposal: &AdaptiveWikiReviewProposal,
    args: &WikiProposalHandoffArgs,
) -> ParameterizedHandoffPreview {
    if proposal.subject_kind != "entry"
        || proposal.action != AdaptiveWikiReviewProposalAction::Merge
    {
        return manual_handoff_missing(
            "--mutation deprecate-duplicate is only supported for entry merge proposals.",
        );
    }
    let Some(deprecated_entry_id) = handoff_arg_value(args.deprecated_entry_id.as_deref()) else {
        return manual_handoff_missing(
            "--mutation deprecate-duplicate requires --deprecated-entry-id.",
        );
    };
    let Some(reason) = handoff_arg_value(args.reason.as_deref()) else {
        return manual_handoff_missing("--mutation deprecate-duplicate requires --reason.");
    };
    ParameterizedHandoffPreview::Ready {
        command: deprecate_command(&deprecated_entry_id, &reason),
        reason:
            "Operator supplied enough inputs for an exact duplicate deprecate mutation preview."
                .to_string(),
    }
}

fn parameterized_split_handoff(
    proposal: &AdaptiveWikiReviewProposal,
) -> ParameterizedHandoffPreview {
    if proposal.subject_kind != "entry"
        || proposal.action != AdaptiveWikiReviewProposalAction::Split
    {
        return manual_handoff_missing(
            "--mutation split is only supported for entry split proposals.",
        );
    }
    if proposal_is_projection_conflict(proposal) {
        return manual_handoff_missing(
            "Projection-conflict splits require one or more governed mutations; preview rescope, deprecate, or add-counterexample paths and then link the executed mutation with a proposal receipt.",
        );
    }
    manual_handoff_missing(
        "Split proposals require manual scope design before a governed mutation command is exact.",
    )
}

fn manual_handoff_missing(reason: &str) -> ParameterizedHandoffPreview {
    ParameterizedHandoffPreview::ManualRequired {
        reason: reason.to_string(),
    }
}

fn fallback_proposal_handoff_command(proposal: &AdaptiveWikiReviewProposal) -> Option<String> {
    let reason = format!("curator review: {}", proposal.title);
    let subject_id = crate::offdesk::operator_safe_text(&proposal.subject_id);
    let reason = crate::offdesk::operator_safe_text(&reason);
    match (proposal.action, proposal.subject_kind.as_str()) {
        (AdaptiveWikiReviewProposalAction::Reject, "candidate") => Some(format!(
            "forager offdesk wiki reject {} --reason {}",
            shell_quote_arg(&subject_id),
            shell_quote_arg(&reason)
        )),
        (AdaptiveWikiReviewProposalAction::Deprecate, "entry") => {
            Some(deprecate_command(&subject_id, &reason))
        }
        _ => None,
    }
}

fn manual_handoff_contract(
    proposal: &AdaptiveWikiReviewProposal,
) -> (
    Vec<WikiProposalHandoffInput>,
    Vec<WikiProposalHandoffMutationOption>,
) {
    if proposal_is_projection_conflict(proposal) {
        return projection_conflict_handoff_contract(proposal);
    }
    match (proposal.action, proposal.subject_kind.as_str()) {
        (AdaptiveWikiReviewProposalAction::Rescope, "entry") => (
            vec![
                handoff_input(
                    "scope",
                    Some("--scope"),
                    true,
                    "New entry scope: session, project, artifact_kind, or user_global.",
                ),
                handoff_input(
                    "scope_ref",
                    Some("--scope-ref"),
                    true,
                    "Scope reference for the selected scope.",
                ),
                handoff_input(
                    "reason",
                    Some("--reason"),
                    false,
                    "Operator rationale to preserve in the mutation audit.",
                ),
            ],
            vec![handoff_option(
                "rescope",
                rescope_command_template(&proposal.subject_id),
                vec!["scope", "scope_ref"],
                "Narrow or widen the promoted entry after reviewing correction evidence.",
            )],
        ),
        (AdaptiveWikiReviewProposalAction::RenewReview, "entry") => (
            vec![
                handoff_input(
                    "mutation",
                    None,
                    true,
                    "Operator-selected mutation path: renew_review_after, rescope, deprecate, or add_counterexample.",
                ),
                handoff_input(
                    "review_after",
                    Some("--review-after"),
                    false,
                    "Required when mutation is renew_review_after.",
                ),
                handoff_input(
                    "scope",
                    Some("--scope"),
                    false,
                    "Required when mutation is rescope.",
                ),
                handoff_input(
                    "scope_ref",
                    Some("--scope-ref"),
                    false,
                    "Required when mutation is rescope.",
                ),
                handoff_input(
                    "evidence_ref",
                    Some("--evidence-ref"),
                    false,
                    "Required when mutation is add_counterexample.",
                ),
                handoff_input(
                    "reason",
                    Some("--reason"),
                    true,
                    "Operator rationale to preserve in the mutation audit.",
                ),
            ],
            vec![
                handoff_option(
                    "renew_review_after",
                    renew_review_after_command_template(&proposal.subject_id),
                    vec!["mutation", "review_after", "reason"],
                    "Keep the entry unchanged and move its next review timestamp.",
                ),
                handoff_option(
                    "rescope",
                    rescope_command_template(&proposal.subject_id),
                    vec!["mutation", "scope", "scope_ref"],
                    "Keep the entry promoted but adjust where it applies.",
                ),
                handoff_option(
                    "deprecate",
                    deprecate_command_template(&proposal.subject_id),
                    vec!["mutation", "reason"],
                    "Retire the entry when review finds it should no longer project.",
                ),
                handoff_option(
                    "add_counterexample",
                    counterexample_command_template(&proposal.subject_id),
                    vec!["mutation", "evidence_ref", "reason"],
                    "Keep the entry but attach limiting evidence for future review.",
                ),
            ],
        ),
        (AdaptiveWikiReviewProposalAction::Split, "entry") => (
            vec![
                handoff_input(
                    "mutation",
                    None,
                    true,
                    "Operator-selected mutation path after designing the narrower split.",
                ),
                handoff_input(
                    "scope",
                    Some("--scope"),
                    false,
                    "Required when the split is represented as a rescope of the current entry.",
                ),
                handoff_input(
                    "scope_ref",
                    Some("--scope-ref"),
                    false,
                    "Required when the split is represented as a rescope of the current entry.",
                ),
                handoff_input(
                    "evidence_ref",
                    Some("--evidence-ref"),
                    false,
                    "Required when the split is preserved as counterexample evidence.",
                ),
                handoff_input(
                    "reason",
                    Some("--reason"),
                    true,
                    "Operator rationale to preserve in the mutation audit.",
                ),
            ],
            vec![
                handoff_option(
                    "rescope",
                    rescope_command_template(&proposal.subject_id),
                    vec!["mutation", "scope", "scope_ref"],
                    "Represent the split by narrowing the current entry scope.",
                ),
                handoff_option(
                    "add_counterexample",
                    counterexample_command_template(&proposal.subject_id),
                    vec!["mutation", "evidence_ref", "reason"],
                    "Represent the split pressure as limiting evidence before creating variants.",
                ),
            ],
        ),
        (AdaptiveWikiReviewProposalAction::Merge, "entry") => (
            vec![
                handoff_input(
                    "survivor_entry_id",
                    None,
                    true,
                    "Entry that should remain promoted after duplicate review.",
                ),
                handoff_input(
                    "deprecated_entry_id",
                    None,
                    true,
                    "Duplicate entry to retire with an audited deprecate mutation.",
                ),
                handoff_input(
                    "reason",
                    Some("--reason"),
                    true,
                    "Operator rationale to preserve in the mutation audit.",
                ),
            ],
            vec![handoff_option(
                "deprecate_duplicate",
                "forager offdesk wiki deprecate <deprecated-entry-id> --reason <reason>"
                    .to_string(),
                vec!["survivor_entry_id", "deprecated_entry_id", "reason"],
                "The current mutation surface represents merge cleanup by deprecating duplicates.",
            )],
        ),
        (AdaptiveWikiReviewProposalAction::AddCounterexample, "entry") => (
            vec![
                handoff_input(
                    "evidence_ref",
                    Some("--evidence-ref"),
                    true,
                    "Evidence ref that contradicts or limits the entry.",
                ),
                handoff_input(
                    "reason",
                    Some("--reason"),
                    true,
                    "Operator rationale to preserve in the mutation audit.",
                ),
            ],
            vec![handoff_option(
                "add_counterexample",
                counterexample_command_template(&proposal.subject_id),
                vec!["evidence_ref", "reason"],
                "Attach limiting evidence to the promoted entry.",
            )],
        ),
        (AdaptiveWikiReviewProposalAction::AddCounterexample, "candidate") => (
            vec![
                handoff_input(
                    "candidate_evidence_source",
                    None,
                    true,
                    "Audit or source evidence to attach to the candidate.",
                ),
                handoff_input(
                    "mutation_path",
                    None,
                    true,
                    "Operator choice for re-recording, promoting, or rejecting the candidate.",
                ),
            ],
            Vec::new(),
        ),
        _ => (Vec::new(), Vec::new()),
    }
}

fn projection_conflict_handoff_contract(
    proposal: &AdaptiveWikiReviewProposal,
) -> (
    Vec<WikiProposalHandoffInput>,
    Vec<WikiProposalHandoffMutationOption>,
) {
    (
        vec![
            handoff_input(
                "mutation",
                None,
                true,
                "Operator-selected conflict path: rescope, deprecate, split, or add_counterexample.",
            ),
            handoff_input(
                "scope",
                Some("--scope"),
                false,
                "Required when mutation is rescope.",
            ),
            handoff_input(
                "scope_ref",
                Some("--scope-ref"),
                false,
                "Required when mutation is rescope.",
            ),
            handoff_input(
                "deprecated_entry_id",
                Some("--deprecated-entry-id"),
                false,
                "Optional when mutation is deprecate; defaults to the proposal subject and may target a conflicting entry evidence ref.",
            ),
            handoff_input(
                "evidence_ref",
                Some("--evidence-ref"),
                false,
                "Required when mutation is add_counterexample.",
            ),
            handoff_input(
                "reason",
                Some("--reason"),
                true,
                "Operator rationale to preserve in the mutation audit.",
            ),
        ],
        vec![
            handoff_option(
                "rescope",
                rescope_command_template(&proposal.subject_id),
                vec!["mutation", "scope", "scope_ref"],
                "Keep the entry promoted but narrow or widen where this side of the conflict applies.",
            ),
            handoff_option(
                "deprecate",
                deprecate_command_template(&proposal.subject_id),
                vec!["mutation", "reason"],
                "Retire the proposal subject, or pass --deprecated-entry-id for a conflicting entry evidence ref.",
            ),
            handoff_option(
                "add_counterexample",
                counterexample_command_template(&proposal.subject_id),
                vec!["mutation", "evidence_ref", "reason"],
                "Keep the entry but attach limiting evidence that explains the conflict.",
            ),
            handoff_option(
                "split",
                "manual: combine rescope, deprecate, and/or add-counterexample mutations, then record a proposal receipt".to_string(),
                vec!["mutation", "reason"],
                "Use when resolving the conflict needs multiple governed wiki mutations instead of one exact command.",
            ),
        ],
    )
}

fn proposal_is_projection_conflict(proposal: &AdaptiveWikiReviewProposal) -> bool {
    proposal.action == AdaptiveWikiReviewProposalAction::Split
        && proposal.subject_kind == "entry"
        && (proposal.title == "Resolve conflicting promoted entries"
            || proposal
                .evidence_refs
                .iter()
                .any(|value| value == "projection:conflict"))
}

fn projection_conflict_entry_ids(proposal: &AdaptiveWikiReviewProposal) -> Vec<String> {
    let mut ids = vec![proposal.subject_id.clone()];
    for evidence_ref in &proposal.evidence_refs {
        let Some(entry_id) = evidence_ref.strip_prefix("entry:") else {
            continue;
        };
        let entry_id = crate::offdesk::operator_safe_text(entry_id);
        if !entry_id.is_empty() && !ids.iter().any(|existing| existing == &entry_id) {
            ids.push(entry_id);
        }
    }
    ids
}

fn handoff_input(
    name: &'static str,
    cli_flag: Option<&'static str>,
    required: bool,
    description: &'static str,
) -> WikiProposalHandoffInput {
    WikiProposalHandoffInput {
        name,
        cli_flag,
        required,
        description,
    }
}

fn handoff_option(
    mutation: &'static str,
    command_template: String,
    required_inputs: Vec<&'static str>,
    description: &'static str,
) -> WikiProposalHandoffMutationOption {
    WikiProposalHandoffMutationOption {
        mutation,
        command_template,
        required_inputs,
        description,
    }
}

fn rescope_command_template(entry_id: &str) -> String {
    format!(
        "forager offdesk wiki rescope {} --scope <scope> --scope-ref <scope-ref> --reason <reason>",
        handoff_subject_arg(entry_id)
    )
}

fn deprecate_command_template(entry_id: &str) -> String {
    format!(
        "forager offdesk wiki deprecate {} --reason <reason>",
        handoff_subject_arg(entry_id)
    )
}

fn counterexample_command_template(entry_id: &str) -> String {
    format!(
        "forager offdesk wiki add-counterexample {} --evidence-ref <evidence-ref> --reason <reason>",
        handoff_subject_arg(entry_id)
    )
}

pub(super) fn renew_review_after_command_template(entry_id: &str) -> String {
    format!(
        "forager offdesk wiki renew-review-after {} --review-after <rfc3339> --reason <reason>",
        handoff_subject_arg(entry_id)
    )
}

fn deprecate_command(entry_id: &str, reason: &str) -> String {
    format!(
        "forager offdesk wiki deprecate {} --reason {}",
        handoff_subject_arg(entry_id),
        shell_quote_arg(&crate::offdesk::operator_safe_text(reason))
    )
}

fn counterexample_command(entry_id: &str, evidence_ref: &str, reason: &str) -> String {
    format!(
        "forager offdesk wiki add-counterexample {} --evidence-ref {} --reason {}",
        handoff_subject_arg(entry_id),
        shell_quote_arg(&crate::offdesk::operator_safe_text(evidence_ref)),
        shell_quote_arg(&crate::offdesk::operator_safe_text(reason))
    )
}

fn handoff_subject_arg(subject_id: &str) -> String {
    shell_quote_arg(&crate::offdesk::operator_safe_text(subject_id))
}

fn handoff_arg_value(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(crate::offdesk::operator_safe_text)
}

fn adaptive_wiki_scope_arg(scope: AdaptiveWikiScope) -> &'static str {
    match scope {
        AdaptiveWikiScope::Session => "session",
        AdaptiveWikiScope::Project => "project",
        AdaptiveWikiScope::ArtifactKind => "artifact_kind",
        AdaptiveWikiScope::UserGlobal => "user_global",
    }
}

fn manual_handoff_reason(proposal: &AdaptiveWikiReviewProposal) -> &'static str {
    if proposal_is_projection_conflict(proposal) {
        return "Projection-conflict proposals require choosing whether to rescope, deprecate one side, preserve counterexample evidence, or split with multiple governed mutations.";
    }
    match proposal.action {
        AdaptiveWikiReviewProposalAction::Rescope => {
            "Rescope proposals require an operator-selected --scope and --scope-ref."
        }
        AdaptiveWikiReviewProposalAction::RenewReview => {
            "Renew-review proposals require choosing whether to renew, rescope, deprecate, or add evidence."
        }
        AdaptiveWikiReviewProposalAction::Split => {
            "Split proposals require manual scope design before a governed mutation command is exact."
        }
        AdaptiveWikiReviewProposalAction::Merge => {
            "Merge proposals require choosing the surviving entry and migration plan."
        }
        AdaptiveWikiReviewProposalAction::AddCounterexample => {
            "Counterexample proposals require a specific evidence ref and target mutation choice."
        }
        AdaptiveWikiReviewProposalAction::Promote
        | AdaptiveWikiReviewProposalAction::Reject
        | AdaptiveWikiReviewProposalAction::Deprecate => {
            "This proposal does not contain enough information for an exact governed mutation command."
        }
    }
}

fn print_wiki_proposal_handoff(preview: &WikiProposalHandoffPreview) {
    println!(
        "Adaptive wiki proposal handoff {}: {}",
        preview.proposal_id, preview.status
    );
    println!(
        "  proposal: {:?} {} {}",
        preview.action, preview.subject_kind, preview.subject_id
    );
    println!("  reason: {}", preview.reason);
    if let Some(decision) = preview.lifecycle_decision {
        println!(
            "  lifecycle: {:?}{}",
            decision,
            if preview.lifecycle_stale {
                " stale"
            } else {
                ""
            }
        );
    }
    if let Some(command) = preview.command.as_deref() {
        println!("  command: {command}");
    }
    if !preview.required_inputs.is_empty() {
        println!("  required inputs:");
        for input in &preview.required_inputs {
            let flag = input.cli_flag.unwrap_or(input.name);
            let required = if input.required {
                "required"
            } else {
                "conditional"
            };
            println!("    {flag} ({required}): {}", input.description);
        }
    }
    if !preview.mutation_options.is_empty() {
        println!("  mutation options:");
        for option in &preview.mutation_options {
            println!("    {}: {}", option.mutation, option.command_template);
            println!("      {}", option.description);
        }
    }
    if !preview.evidence_refs.is_empty() {
        println!("  evidence: {}", preview.evidence_refs.join(", "));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn handoff_mutation_parser_accepts_cli_aliases_and_rejects_unknown_values() {
        assert_eq!(
            parse_wiki_proposal_handoff_mutation("add-counterexample"),
            Ok(WikiProposalHandoffMutation::AddCounterexample)
        );
        assert_eq!(
            parse_wiki_proposal_handoff_mutation("deprecate_duplicate"),
            Ok(WikiProposalHandoffMutation::DeprecateDuplicate)
        );
        assert!(parse_wiki_proposal_handoff_mutation("execute").is_err());
    }

    #[test]
    fn mutation_commands_quote_operator_values() {
        assert_eq!(
            deprecate_command("entry'id", "operator's reason"),
            "forager offdesk wiki deprecate 'entry'\\''id' --reason 'operator'\\''s reason'"
        );
        assert_eq!(
            adaptive_wiki_scope_arg(AdaptiveWikiScope::ArtifactKind),
            "artifact_kind"
        );
    }
}
