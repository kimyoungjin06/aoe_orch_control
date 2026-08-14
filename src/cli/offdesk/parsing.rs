//! Value parsers shared by the Offdesk command arguments and handlers.

use super::*;

pub(super) fn parse_rfc3339(value: Option<&str>) -> Result<Option<DateTime<Utc>>> {
    let Some(value) = value else {
        return Ok(None);
    };
    Ok(Some(
        DateTime::parse_from_rfc3339(value)?.with_timezone(&Utc),
    ))
}

pub(super) fn parse_rfc3339_datetime(value: &str) -> std::result::Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&Utc))
        .map_err(|err| format!("timestamp must be RFC3339: {err}"))
}

pub(super) fn parse_background_runner_kind(
    value: &str,
) -> std::result::Result<BackgroundRunnerKind, String> {
    value.parse()
}

pub(super) fn parse_maintenance_action_kind(
    value: &str,
) -> std::result::Result<MaintenanceActionKind, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "runtime_recovery" | "runtime-recovery" | "recovery" => {
            Ok(MaintenanceActionKind::RuntimeRecovery)
        }
        "wiki_runtime_ack" | "wiki-runtime-ack" | "runtime_ack" | "runtime-ack" => {
            Ok(MaintenanceActionKind::WikiRuntimeAck)
        }
        "wiki_review_after" | "wiki-review-after" | "review_after" | "review-after" => {
            Ok(MaintenanceActionKind::WikiReviewAfter)
        }
        "wiki_mutation" | "wiki-mutation" | "wiki" => Ok(MaintenanceActionKind::WikiMutation),
        "provider_capacity" | "provider-capacity" | "capacity" => {
            Ok(MaintenanceActionKind::ProviderCapacity)
        }
        "artifact_cleanup" | "artifact-cleanup" | "cleanup" => {
            Ok(MaintenanceActionKind::ArtifactCleanup)
        }
        "service_restart" | "service-restart" | "restart" => {
            Ok(MaintenanceActionKind::ServiceRestart)
        }
        "system_change" | "system-change" | "system" => Ok(MaintenanceActionKind::SystemChange),
        _ => Err(
            "maintenance kind must be one of runtime_recovery, wiki_runtime_ack, wiki_review_after, wiki_mutation, provider_capacity, artifact_cleanup, service_restart, system_change"
                .to_string(),
        ),
    }
}

pub(super) fn parse_risk_level(value: &str) -> std::result::Result<RiskLevel, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "safe" => Ok(RiskLevel::Safe),
        "runtime_mutation" | "runtime-mutation" | "runtime" => Ok(RiskLevel::RuntimeMutation),
        "canonical_mutation" | "canonical-mutation" | "canonical" => {
            Ok(RiskLevel::CanonicalMutation)
        }
        "destructive" | "delete" | "cleanup" => Ok(RiskLevel::Destructive),
        "external_side_effect" | "external-side-effect" | "external" => {
            Ok(RiskLevel::ExternalSideEffect)
        }
        _ => Err(
            "risk must be one of safe, runtime_mutation, canonical_mutation, destructive, external_side_effect"
                .to_string(),
        ),
    }
}

pub(super) fn parse_artifact_ref(
    value: &str,
) -> std::result::Result<CapabilityArtifactRef, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("artifact reference must not be empty".to_string());
    }
    if let Some((artifact_id, path)) = trimmed.split_once('=') {
        let artifact_id = artifact_id.trim();
        let path = path.trim();
        if artifact_id.is_empty() || path.is_empty() {
            return Err("artifact reference must use ARTIFACT_ID=PATH".to_string());
        }
        Ok(CapabilityArtifactRef::new(
            artifact_id.to_string(),
            Some(path.to_string()),
        ))
    } else {
        Ok(CapabilityArtifactRef::new(
            trimmed.to_string(),
            None::<String>,
        ))
    }
}

pub(super) fn parse_adaptive_wiki_scope(
    value: &str,
) -> std::result::Result<AdaptiveWikiScope, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "session" => Ok(AdaptiveWikiScope::Session),
        "artifact_kind" | "artifact-kind" | "artifact" => Ok(AdaptiveWikiScope::ArtifactKind),
        "project" => Ok(AdaptiveWikiScope::Project),
        "user_global" | "user-global" | "global" => Ok(AdaptiveWikiScope::UserGlobal),
        _ => Err("scope must be one of session, artifact_kind, project, user_global".to_string()),
    }
}

pub(super) fn parse_adaptive_wiki_kind(
    value: &str,
) -> std::result::Result<AdaptiveWikiKind, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "preference" | "pref" => Ok(AdaptiveWikiKind::Preference),
        "procedure" | "proc" => Ok(AdaptiveWikiKind::Procedure),
        "failure_pattern" | "failure-pattern" | "failure" | "fail" => {
            Ok(AdaptiveWikiKind::FailurePattern)
        }
        "policy_rule" | "policy-rule" | "policy" => Ok(AdaptiveWikiKind::PolicyRule),
        "fact" => Ok(AdaptiveWikiKind::Fact),
        _ => Err(
            "kind must be one of preference, procedure, failure_pattern, policy_rule, fact"
                .to_string(),
        ),
    }
}

pub(super) fn parse_adaptive_wiki_confidence(
    value: &str,
) -> std::result::Result<AdaptiveWikiConfidence, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "explicit" => Ok(AdaptiveWikiConfidence::Explicit),
        "repeated" => Ok(AdaptiveWikiConfidence::Repeated),
        "inferred" => Ok(AdaptiveWikiConfidence::Inferred),
        _ => Err("confidence must be one of explicit, repeated, inferred".to_string()),
    }
}

pub(super) fn parse_adaptive_wiki_signal_kind(
    value: &str,
) -> std::result::Result<AdaptiveWikiSignalKind, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "operator_correction" | "operator-correction" | "correction" => {
            Ok(AdaptiveWikiSignalKind::OperatorCorrection)
        }
        "explicit_preference" | "explicit-preference" | "preference" => {
            Ok(AdaptiveWikiSignalKind::ExplicitPreference)
        }
        "imported_doc" | "imported-doc" | "doc" => Ok(AdaptiveWikiSignalKind::ImportedDoc),
        "repeated_failure" | "repeated-failure" => Ok(AdaptiveWikiSignalKind::RepeatedFailure),
        _ => Err(
            "signal kind must be one of operator_correction, explicit_preference, imported_doc, repeated_failure"
                .to_string(),
        ),
    }
}

pub(super) fn parse_adaptive_wiki_origin(
    value: &str,
) -> std::result::Result<AdaptiveWikiOrigin, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "operator_explicit" | "operator-explicit" | "operator" => {
            Ok(AdaptiveWikiOrigin::OperatorExplicit)
        }
        "background_review" | "background-review" | "background" => {
            Ok(AdaptiveWikiOrigin::BackgroundReview)
        }
        "imported" => Ok(AdaptiveWikiOrigin::Imported),
        _ => {
            Err("origin must be one of operator_explicit, background_review, imported".to_string())
        }
    }
}

pub(super) fn parse_adaptive_wiki_runtime_policy_ack_scope_mode(
    value: &str,
) -> std::result::Result<AdaptiveWikiRuntimePolicyAckScopeMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "exact_query" | "exact-query" | "exact" => {
            Ok(AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery)
        }
        "project_artifact" | "project-artifact" => {
            Ok(AdaptiveWikiRuntimePolicyAckScopeMode::ProjectArtifact)
        }
        _ => Err("scope mode must be one of exact_query, project_artifact".to_string()),
    }
}

pub(super) fn parse_adaptive_wiki_activation_mode(
    value: &str,
) -> std::result::Result<AdaptiveWikiActivationMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "context_only" | "context-only" => Ok(AdaptiveWikiActivationMode::ContextOnly),
        "confirm" => Ok(AdaptiveWikiActivationMode::Confirm),
        "auto_apply" | "auto-apply" => Ok(AdaptiveWikiActivationMode::AutoApply),
        _ => Err("activation mode must be one of context_only, confirm, auto_apply".to_string()),
    }
}

pub(super) fn parse_adaptive_wiki_agent_mode(
    value: &str,
) -> std::result::Result<AdaptiveWikiAgentMode, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "planning" | "plan" | "planner" => Ok(AdaptiveWikiAgentMode::Planning),
        "code_development" | "code-development" | "code" | "coding" | "development" => {
            Ok(AdaptiveWikiAgentMode::Development)
        }
        "analysis" | "analyze" | "analyse" | "diagnostics" | "diagnostic" => {
            Ok(AdaptiveWikiAgentMode::Analysis)
        }
        "research_writing" | "research-writing" | "research" | "writing" | "editing" => {
            Ok(AdaptiveWikiAgentMode::Writing)
        }
        "critique" | "critic" => Ok(AdaptiveWikiAgentMode::Critique),
        "review" | "reviewer" => Ok(AdaptiveWikiAgentMode::Review),
        "maintenance" | "maintain" | "maintainer" | "ops" | "health" => {
            Ok(AdaptiveWikiAgentMode::Maintenance)
        }
        _ => Err(
            "agent mode must be one of planning, development, analysis, writing, critique, review, maintenance".to_string(),
        ),
    }
}

pub(super) fn parse_adaptive_wiki_review_action(
    value: &str,
) -> std::result::Result<AdaptiveWikiReviewProposalAction, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "promote" => Ok(AdaptiveWikiReviewProposalAction::Promote),
        "reject" => Ok(AdaptiveWikiReviewProposalAction::Reject),
        "rescope" => Ok(AdaptiveWikiReviewProposalAction::Rescope),
        "deprecate" => Ok(AdaptiveWikiReviewProposalAction::Deprecate),
        "add_counterexample" | "add-counterexample" => {
            Ok(AdaptiveWikiReviewProposalAction::AddCounterexample)
        }
        "renew_review" | "renew-review" => Ok(AdaptiveWikiReviewProposalAction::RenewReview),
        "split" => Ok(AdaptiveWikiReviewProposalAction::Split),
        "merge" => Ok(AdaptiveWikiReviewProposalAction::Merge),
        _ => Err("proposal action must be one of promote, reject, rescope, deprecate, add_counterexample, renew_review, split, merge".to_string()),
    }
}

pub(super) fn parse_adaptive_wiki_proposal_decision(
    value: &str,
) -> std::result::Result<AdaptiveWikiReviewProposalDecision, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "accepted" | "accept" => Ok(AdaptiveWikiReviewProposalDecision::Accepted),
        "rejected" | "reject" => Ok(AdaptiveWikiReviewProposalDecision::Rejected),
        "superseded" | "supersede" => Ok(AdaptiveWikiReviewProposalDecision::Superseded),
        _ => Err("proposal decision must be one of accepted, rejected, superseded".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maintenance_aliases_map_to_the_same_kind() {
        assert_eq!(
            parse_maintenance_action_kind("runtime-recovery").unwrap(),
            MaintenanceActionKind::RuntimeRecovery
        );
        assert_eq!(
            parse_maintenance_action_kind("recovery").unwrap(),
            MaintenanceActionKind::RuntimeRecovery
        );
    }

    #[test]
    fn adaptive_wiki_aliases_are_normalized() {
        assert_eq!(
            parse_adaptive_wiki_scope("artifact-kind").unwrap(),
            AdaptiveWikiScope::ArtifactKind
        );
        assert_eq!(
            parse_adaptive_wiki_agent_mode("coding").unwrap(),
            AdaptiveWikiAgentMode::Development
        );
        assert!(parse_adaptive_wiki_kind("unknown").is_err());
    }

    #[test]
    fn artifact_reference_rejects_missing_sides() {
        assert!(parse_artifact_ref("artifact=/tmp/result.json").is_ok());
        assert!(parse_artifact_ref("artifact=").is_err());
        assert!(parse_artifact_ref(" =/tmp/result.json").is_err());
    }
}
