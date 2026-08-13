//! Typed Offdesk plan-registration validation and record construction.
//!
//! CLI adapters read and hash source files, allocate registry paths, copy the
//! source artifact, and persist registrations. This module owns plan policy.

use std::collections::BTreeSet;

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const OFFDESK_PLAN_REGISTRATION_SCHEMA: &str = "offdesk_plan_registration.v1";
pub const OFFDESK_PLAN_REQUIRED_DENIALS: [&str; 8] = [
    "enqueue",
    "launch",
    "approval",
    "file movement",
    "archive",
    "delete",
    "wiki promotion",
    "accepted truth",
];

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OffdeskPlanRegistration {
    pub schema: String,
    pub registered_at: DateTime<Utc>,
    pub forager_profile: String,
    pub source_path: String,
    pub source_sha256: String,
    pub artifact_kind: String,
    pub plan_schema: String,
    pub profile_key: Option<String>,
    pub profile_name: Option<String>,
    pub project_key: Option<String>,
    pub request_id: Option<String>,
    pub task_id: Option<String>,
    pub ready_for_operator_review: bool,
    pub ready_for_launch_preparation: bool,
    pub ready_for_enqueue: bool,
    pub validation_failures: Vec<String>,
    pub decision: Option<Value>,
    pub consensus: Option<Value>,
    pub selected_plan_path: Option<String>,
    pub dry_run: bool,
    pub artifacts: OffdeskPlanRegistrationArtifacts,
    pub does_not_authorize: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OffdeskPlanRegistrationArtifacts {
    pub registry_dir: Option<String>,
    pub registration_json: Option<String>,
    pub copied_source_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OffdeskPlanInputSummary {
    pub artifact_kind: &'static str,
    pub plan_schema: String,
    pub profile_key: Option<String>,
    pub profile_name: Option<String>,
    pub ready_for_operator_review: bool,
    pub ready_for_launch_preparation: bool,
    pub ready_for_enqueue: bool,
    pub decision: Option<Value>,
    pub consensus: Option<Value>,
    pub selected_plan_path: Option<String>,
}

pub struct OffdeskPlanRegistrationBuildInput<'a> {
    pub registered_at: DateTime<Utc>,
    pub forager_profile: &'a str,
    pub source_path: &'a str,
    pub source_sha256: &'a str,
    pub summary: OffdeskPlanInputSummary,
    pub project_key: Option<&'a str>,
    pub request_id: Option<&'a str>,
    pub task_id: Option<&'a str>,
    pub dry_run: bool,
    pub artifacts: OffdeskPlanRegistrationArtifacts,
}

pub fn validate_offdesk_plan_input(value: &Value) -> Result<OffdeskPlanInputSummary> {
    let plan_schema = value_string_field(value, "schema").unwrap_or_default();
    match plan_schema.as_str() {
        "offdesk_multiturn_plan.v1" => validate_multiturn_plan_input(value, plan_schema),
        "offdesk_planner_council.v1" => validate_planner_council_input(value, plan_schema),
        "" => bail!("Offdesk plan registration guard failed: schema_missing"),
        other => bail!("Offdesk plan registration guard failed: unsupported_schema:{other}"),
    }
}

pub fn build_offdesk_plan_registration(
    input: OffdeskPlanRegistrationBuildInput<'_>,
) -> OffdeskPlanRegistration {
    let summary = input.summary;
    OffdeskPlanRegistration {
        schema: OFFDESK_PLAN_REGISTRATION_SCHEMA.to_string(),
        registered_at: input.registered_at,
        forager_profile: input.forager_profile.to_string(),
        source_path: input.source_path.to_string(),
        source_sha256: input.source_sha256.to_string(),
        artifact_kind: summary.artifact_kind.to_string(),
        plan_schema: summary.plan_schema,
        profile_key: summary.profile_key,
        profile_name: summary.profile_name,
        project_key: input.project_key.map(ToOwned::to_owned),
        request_id: input.request_id.map(ToOwned::to_owned),
        task_id: input.task_id.map(ToOwned::to_owned),
        ready_for_operator_review: summary.ready_for_operator_review,
        ready_for_launch_preparation: summary.ready_for_launch_preparation,
        ready_for_enqueue: summary.ready_for_enqueue,
        validation_failures: Vec::new(),
        decision: summary.decision,
        consensus: summary.consensus,
        selected_plan_path: summary.selected_plan_path,
        dry_run: input.dry_run,
        artifacts: input.artifacts,
        does_not_authorize: offdesk_plan_registration_denials(),
    }
}

pub fn offdesk_plan_registration_denials() -> Vec<String> {
    OFFDESK_PLAN_REQUIRED_DENIALS
        .iter()
        .map(|denial| (*denial).to_string())
        .collect()
}

fn validate_multiturn_plan_input(
    value: &Value,
    plan_schema: String,
) -> Result<OffdeskPlanInputSummary> {
    let mut failures = Vec::new();
    let decision = value.get("decision").filter(|entry| entry.is_object());
    if decision.is_none() {
        failures.push("decision_missing".to_string());
    }
    let ready_for_operator_review = require_bool_field(
        &mut failures,
        decision,
        "decision",
        "ready_for_operator_review",
        true,
    );
    let ready_for_launch_preparation = require_bool_field(
        &mut failures,
        decision,
        "decision",
        "ready_for_launch_preparation",
        false,
    );
    let ready_for_enqueue = require_bool_field(
        &mut failures,
        decision,
        "decision",
        "ready_for_enqueue",
        false,
    );
    match value.get("execution_sequence").and_then(Value::as_array) {
        Some(items) if !items.is_empty() => {}
        _ => failures.push("execution_sequence_missing".to_string()),
    }
    validate_plan_authority(value, &mut failures);
    fail_plan_registration_if_needed(failures)?;

    Ok(OffdeskPlanInputSummary {
        artifact_kind: "offdesk_multiturn_plan",
        plan_schema,
        profile_key: value_string_field(value, "profile_key"),
        profile_name: value_string_field(value, "profile_name"),
        ready_for_operator_review,
        ready_for_launch_preparation,
        ready_for_enqueue,
        decision: decision.cloned(),
        consensus: None,
        selected_plan_path: None,
    })
}

fn validate_planner_council_input(
    value: &Value,
    plan_schema: String,
) -> Result<OffdeskPlanInputSummary> {
    let mut failures = Vec::new();
    let consensus = value.get("consensus").filter(|entry| entry.is_object());
    if consensus.is_none() {
        failures.push("consensus_missing".to_string());
    }
    let ready_for_operator_review = require_bool_field(
        &mut failures,
        consensus,
        "consensus",
        "ready_for_operator_review",
        true,
    );
    let ready_for_launch_preparation = require_bool_field(
        &mut failures,
        consensus,
        "consensus",
        "ready_for_launch_preparation",
        false,
    );
    let ready_for_enqueue = require_bool_field(
        &mut failures,
        consensus,
        "consensus",
        "ready_for_enqueue",
        false,
    );
    match value.get("validation_failures").and_then(Value::as_array) {
        Some(items) if items.is_empty() => {}
        Some(items) => failures.push(format!("validation_failures_present:{}", items.len())),
        None => failures.push("validation_failures_missing".to_string()),
    }
    fail_plan_registration_if_needed(failures)?;

    Ok(OffdeskPlanInputSummary {
        artifact_kind: "offdesk_planner_council",
        plan_schema,
        profile_key: value_string_field(value, "profile_key"),
        profile_name: value_string_field(value, "profile_name"),
        ready_for_operator_review,
        ready_for_launch_preparation,
        ready_for_enqueue,
        decision: None,
        consensus: consensus.cloned(),
        selected_plan_path: value_string_field(value, "synthesized_plan_path"),
    })
}

fn validate_plan_authority(value: &Value, failures: &mut Vec<String>) {
    let authority = value.get("authority").filter(|entry| entry.is_object());
    if authority.is_none() {
        failures.push("authority_missing".to_string());
    }
    require_bool_field(failures, authority, "authority", "read_only_plan", true);
    let denials = authority
        .and_then(|entry| entry.get("does_not_authorize"))
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(ToOwned::to_owned)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
    for required in OFFDESK_PLAN_REQUIRED_DENIALS {
        if !denials.contains(required) {
            failures.push(format!("authority_missing:{required}"));
        }
    }
}

fn require_bool_field(
    failures: &mut Vec<String>,
    parent: Option<&Value>,
    parent_name: &str,
    field: &str,
    expected: bool,
) -> bool {
    match parent
        .and_then(|entry| entry.get(field))
        .and_then(Value::as_bool)
    {
        Some(actual) if actual == expected => actual,
        Some(actual) => {
            failures.push(format!("{parent_name}.{field}_must_be_{expected}"));
            actual
        }
        None => {
            failures.push(format!("{parent_name}.{field}_missing"));
            false
        }
    }
}

fn fail_plan_registration_if_needed(failures: Vec<String>) -> Result<()> {
    if !failures.is_empty() {
        bail!(
            "Offdesk plan registration guard failed: {}",
            failures.join(", ")
        );
    }
    Ok(())
}

fn value_string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn authority() -> Value {
        json!({
            "read_only_plan": true,
            "does_not_authorize": OFFDESK_PLAN_REQUIRED_DENIALS
        })
    }

    fn multiturn_plan() -> Value {
        json!({
            "schema": "offdesk_multiturn_plan.v1",
            "profile_key": "generic",
            "profile_name": "Generic Offdesk Planning",
            "decision": {
                "ready_for_operator_review": true,
                "ready_for_launch_preparation": false,
                "ready_for_enqueue": false,
                "reason": "Operator review is required."
            },
            "execution_sequence": [{"id": "phase_1"}],
            "authority": authority()
        })
    }

    #[test]
    fn validates_multiturn_plan_and_builds_registration() {
        let summary = validate_offdesk_plan_input(&multiturn_plan()).expect("valid plan");
        assert_eq!(summary.artifact_kind, "offdesk_multiturn_plan");
        assert_eq!(summary.profile_key.as_deref(), Some("generic"));
        assert!(summary.ready_for_operator_review);
        assert!(!summary.ready_for_launch_preparation);
        assert!(!summary.ready_for_enqueue);
        assert!(summary.decision.is_some());

        let registration = build_offdesk_plan_registration(OffdeskPlanRegistrationBuildInput {
            registered_at: "2026-08-13T03:00:00Z".parse().expect("valid timestamp"),
            forager_profile: "forager-ops",
            source_path: "/workspace/OVERNIGHT_PLAN.json",
            source_sha256: "abc123",
            summary,
            project_key: Some("project"),
            request_id: Some("request"),
            task_id: Some("task"),
            dry_run: false,
            artifacts: OffdeskPlanRegistrationArtifacts {
                registry_dir: Some("/tmp/plan".to_string()),
                registration_json: Some("/tmp/plan/registration.json".to_string()),
                copied_source_json: Some("/tmp/plan/source.json".to_string()),
            },
        });

        assert_eq!(registration.schema, OFFDESK_PLAN_REGISTRATION_SCHEMA);
        assert_eq!(registration.source_sha256, "abc123");
        assert_eq!(registration.project_key.as_deref(), Some("project"));
        assert!(registration.validation_failures.is_empty());
        assert!(!registration.dry_run);
        assert_eq!(
            registration.does_not_authorize,
            offdesk_plan_registration_denials()
        );
    }

    #[test]
    fn validates_planner_council_summary() {
        let value = json!({
            "schema": "offdesk_planner_council.v1",
            "profile_key": "generic",
            "consensus": {
                "ready_for_operator_review": true,
                "ready_for_launch_preparation": false,
                "ready_for_enqueue": false,
                "selected_planner": "planner_a"
            },
            "validation_failures": [],
            "synthesized_plan_path": "/tmp/SYNTHESIZED_PLAN.json"
        });

        let summary = validate_offdesk_plan_input(&value).expect("valid council");

        assert_eq!(summary.artifact_kind, "offdesk_planner_council");
        assert!(summary.decision.is_none());
        assert_eq!(
            summary.consensus.expect("consensus")["selected_planner"],
            "planner_a"
        );
        assert_eq!(
            summary.selected_plan_path.as_deref(),
            Some("/tmp/SYNTHESIZED_PLAN.json")
        );
    }

    #[test]
    fn rejects_missing_and_unknown_schema() {
        assert_eq!(
            validate_offdesk_plan_input(&json!({}))
                .expect_err("schema is required")
                .to_string(),
            "Offdesk plan registration guard failed: schema_missing"
        );
        assert_eq!(
            validate_offdesk_plan_input(&json!({"schema": "offdesk_future_plan.v1"}))
                .expect_err("unknown schema must fail")
                .to_string(),
            "Offdesk plan registration guard failed: unsupported_schema:offdesk_future_plan.v1"
        );
    }

    #[test]
    fn multiturn_validation_reports_ordered_fail_closed_reasons() {
        let error = validate_offdesk_plan_input(&json!({
            "schema": "offdesk_multiturn_plan.v1"
        }))
        .expect_err("incomplete plan must fail")
        .to_string();

        assert_eq!(
            error,
            "Offdesk plan registration guard failed: decision_missing, decision.ready_for_operator_review_missing, decision.ready_for_launch_preparation_missing, decision.ready_for_enqueue_missing, execution_sequence_missing, authority_missing, authority.read_only_plan_missing, authority_missing:enqueue, authority_missing:launch, authority_missing:approval, authority_missing:file movement, authority_missing:archive, authority_missing:delete, authority_missing:wiki promotion, authority_missing:accepted truth"
        );
    }

    #[test]
    fn planner_council_rejects_validation_failures() {
        let error = validate_offdesk_plan_input(&json!({
            "schema": "offdesk_planner_council.v1",
            "consensus": {
                "ready_for_operator_review": true,
                "ready_for_launch_preparation": false,
                "ready_for_enqueue": false
            },
            "validation_failures": ["planner_timeout", "missing_vote"]
        }))
        .expect_err("failed council must not register")
        .to_string();

        assert_eq!(
            error,
            "Offdesk plan registration guard failed: validation_failures_present:2"
        );
    }

    #[test]
    fn dry_run_registration_has_no_artifact_paths() {
        let summary = validate_offdesk_plan_input(&multiturn_plan()).expect("valid plan");
        let registration = build_offdesk_plan_registration(OffdeskPlanRegistrationBuildInput {
            registered_at: "2026-08-13T03:00:00Z".parse().expect("valid timestamp"),
            forager_profile: "default",
            source_path: "/workspace/OVERNIGHT_PLAN.json",
            source_sha256: "abc123",
            summary,
            project_key: None,
            request_id: None,
            task_id: None,
            dry_run: true,
            artifacts: OffdeskPlanRegistrationArtifacts {
                registry_dir: None,
                registration_json: None,
                copied_source_json: None,
            },
        });

        assert!(registration.dry_run);
        assert!(registration.artifacts.registry_dir.is_none());
        assert!(registration.artifacts.registration_json.is_none());
        assert!(registration.artifacts.copied_source_json.is_none());
    }
}
