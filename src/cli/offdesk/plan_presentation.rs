//! CLI presentation adapter for Offdesk plan records and projections.
//!
//! Query, workflow, and storage adapters provide typed records. This module
//! owns their terminal output and plan-specific Remote Operator projections.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;

use super::{
    observed_hash_for, print_remote_operator_projection, remote_operator_card,
    remote_operator_projection, RemoteOperatorCard, RemoteOperatorPlansArgs,
    RemoteOperatorShowArgs,
};
use crate::offdesk::{
    operator_safe_text, OffdeskPlanLaunchPrepPacket, OffdeskPlanRegistration,
    OffdeskPlanRegistryDetail, OffdeskPlanRegistryItem, OffdeskPlanReviewDecision,
    OffdeskPlanReviewRecord, OffdeskPlanReviewState,
};

#[derive(Serialize)]
struct RemoteOperatorPlansPayload {
    filters: RemoteOperatorPlanFilters,
    plan_count: usize,
    plans: Vec<RemoteOperatorPlanSummary>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorPlanFilters {
    project_key: Option<String>,
    task_id: Option<String>,
    profile_key: Option<String>,
    artifact_kind: Option<String>,
    latest: bool,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorPlanSummaryCore {
    plan_id: String,
    artifact_kind: String,
    plan_schema: String,
    profile_key: Option<String>,
    project_key: Option<String>,
    request_id: Option<String>,
    task_id: Option<String>,
    registered_at: DateTime<Utc>,
    source_sha256: String,
    review_status: String,
    review_count: usize,
    latest_review_id: Option<String>,
    launch_prep_count: usize,
    latest_launch_prep_id: Option<String>,
    ready_for_operator_review: bool,
    launch_preparation_candidate: bool,
    ready_for_enqueue: bool,
    next_safe_action: String,
    remote_actions: Vec<String>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorPlanSummary {
    #[serde(flatten)]
    core: RemoteOperatorPlanSummaryCore,
    observed_hash: String,
}

#[derive(Serialize)]
struct RemoteOperatorPlanDetailPayload {
    plan: RemoteOperatorPlanSummary,
    reviews: Vec<RemoteOperatorPlanReviewSummary>,
    launch_preps: Vec<RemoteOperatorLaunchPrepSummary>,
    does_not_authorize: Vec<String>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorPlanReviewSummary {
    review_id: String,
    reviewed_at: DateTime<Utc>,
    decision: OffdeskPlanReviewDecision,
    reviewer: String,
    ready_for_launch_preparation_candidate: bool,
    ready_for_enqueue: bool,
    blockers: Vec<String>,
    followups: Vec<String>,
}

#[derive(Clone, Serialize)]
struct RemoteOperatorLaunchPrepSummary {
    prep_id: String,
    prepared_at: DateTime<Utc>,
    review_id: String,
    launch_preparation_candidate: bool,
    ready_for_launch: bool,
    ready_for_enqueue: bool,
    next_safe_action: String,
}

pub(super) fn present_offdesk_plan_registration(
    registration: &OffdeskPlanRegistration,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(registration)?);
        return Ok(());
    }

    let verb = if registration.dry_run {
        "Validated"
    } else {
        "Registered"
    };
    println!(
        "{verb} Offdesk plan artifact: {} ({})",
        registration.artifact_kind, registration.plan_schema
    );
    println!("  source: {}", registration.source_path);
    println!(
        "  ready_for_operator_review: {}",
        registration.ready_for_operator_review
    );
    println!(
        "  ready_for_launch_preparation: {}",
        registration.ready_for_launch_preparation
    );
    println!("  ready_for_enqueue: {}", registration.ready_for_enqueue);
    if let Some(path) = registration.artifacts.registration_json.as_deref() {
        println!("  registration: {path}");
    }
    println!(
        "  note: registration does not authorize enqueue, launch, approval, file movement, cleanup, or accepted truth"
    );
    Ok(())
}

pub(super) fn present_offdesk_plan_registry_items(
    items: &[OffdeskPlanRegistryItem],
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(items)?);
        return Ok(());
    }
    if items.is_empty() {
        println!("No registered Offdesk plans found.");
        return Ok(());
    }

    println!("Registered Offdesk plans");
    for item in items {
        let registration = &item.registration;
        println!(
            "- {} [{}] plan_review={} launch_candidate={} enqueue={}",
            item.plan_id,
            registration.artifact_kind,
            item.review_state.status,
            item.review_state.ready_for_launch_preparation_candidate,
            registration.ready_for_enqueue
        );
        println!("  next:    {}", item.review_state.next_safe_action);
        if let Some(packet) = item.latest_launch_prep.as_ref() {
            println!("  prep:    {}", packet.prep_id);
        }
        if let Some(project_key) = registration.project_key.as_deref() {
            println!("  project: {project_key}");
        }
        if let Some(task_id) = registration.task_id.as_deref() {
            println!("  task:    {task_id}");
        }
        println!("  source:  {}", registration.source_path);
    }
    Ok(())
}

pub(super) fn present_offdesk_plan_registry_detail(
    detail: &OffdeskPlanRegistryDetail,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(detail)?);
        return Ok(());
    }

    let registration = &detail.registration;
    println!("Registered Offdesk plan: {}", detail.plan_id);
    println!("  kind:       {}", registration.artifact_kind);
    println!("  schema:     {}", registration.plan_schema);
    println!("  registered: {}", registration.registered_at);
    println!("  source:     {}", registration.source_path);
    println!("  sha256:     {}", registration.source_sha256);
    if let Some(profile_key) = registration.profile_key.as_deref() {
        println!("  profile:    {profile_key}");
    }
    if let Some(project_key) = registration.project_key.as_deref() {
        println!("  project:    {project_key}");
    }
    if let Some(request_id) = registration.request_id.as_deref() {
        println!("  request:    {request_id}");
    }
    if let Some(task_id) = registration.task_id.as_deref() {
        println!("  task:       {task_id}");
    }
    println!(
        "  ready_for_operator_review: {}",
        registration.ready_for_operator_review
    );
    println!(
        "  ready_for_launch_preparation: {}",
        registration.ready_for_launch_preparation
    );
    println!("  ready_for_enqueue: {}", registration.ready_for_enqueue);
    if let Some(path) = registration.selected_plan_path.as_deref() {
        println!("  selected_plan: {path}");
    }
    println!("  review_state: {}", detail.review_state.status);
    println!(
        "  launch_candidate: {}",
        detail.review_state.ready_for_launch_preparation_candidate
    );
    println!("  next:       {}", detail.review_state.next_safe_action);
    if let Some(review) = detail.latest_review.as_ref() {
        println!("  latest_review: {}", review.review_id);
        println!("  reviewer:   {}", review.reviewer);
        println!("  reason:     {}", review.reason);
    }
    if let Some(packet) = detail.latest_launch_prep.as_ref() {
        println!("  latest_launch_prep: {}", packet.prep_id);
        println!(
            "  launch_prep_file:   {}",
            packet.artifacts.launch_prep_json
        );
    }
    println!("  registration: {}", detail.registration_path);
    println!(
        "  does_not_authorize: {}",
        registration.does_not_authorize.join(", ")
    );
    Ok(())
}

pub(super) fn present_offdesk_plan_review_record(
    record: &OffdeskPlanReviewRecord,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(record)?);
        return Ok(());
    }

    println!("Offdesk plan review");
    println!("  reviewed_at:  {}", record.reviewed_at);
    println!("  review_id:    {}", record.review_id);
    println!("  plan_id:      {}", record.plan_id);
    println!("  decision:     {}", record.decision.as_str());
    println!("  reviewer:     {}", record.reviewer);
    if let Some(provider) = record.review_provider.as_deref() {
        println!("  provider:     {provider}");
    }
    println!("  reason:       {}", record.reason);
    println!(
        "  launch_candidate: {}",
        record.ready_for_launch_preparation_candidate
    );
    println!("  ready_for_enqueue: {}", record.ready_for_enqueue);
    println!("  project file mutations: none");
    println!("Artifacts:");
    println!("  registration: {}", record.artifacts.registration_json);
    println!("  review:       {}", record.artifacts.review_record_json);
    if !record.blockers.is_empty() {
        println!("Blockers:");
        for blocker in &record.blockers {
            println!("  - {blocker}");
        }
    }
    if !record.followups.is_empty() {
        println!("Follow-ups:");
        for followup in &record.followups {
            println!("  - {followup}");
        }
    }
    println!(
        "  note: review does not authorize enqueue, launch, approval, file movement, cleanup, or accepted truth"
    );
    Ok(())
}

pub(super) fn present_offdesk_plan_launch_prep_packet(
    packet: &OffdeskPlanLaunchPrepPacket,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(packet)?);
        return Ok(());
    }

    println!("Offdesk plan launch-prep packet");
    println!("  prepared_at:  {}", packet.prepared_at);
    println!("  prep_id:      {}", packet.prep_id);
    println!("  plan_id:      {}", packet.plan_id);
    println!("  review_id:    {}", packet.review_id);
    println!("  prepared_by:  {}", packet.prepared_by);
    println!(
        "  launch_candidate: {}",
        packet.launch_preparation_candidate
    );
    println!("  ready_for_launch: {}", packet.ready_for_launch);
    println!("  ready_for_enqueue: {}", packet.ready_for_enqueue);
    println!("  next:         {}", packet.next_safe_action);
    println!("Artifacts:");
    println!("  registration: {}", packet.artifacts.registration_json);
    println!("  review:       {}", packet.artifacts.review_record_json);
    println!("  launch_prep:  {}", packet.artifacts.launch_prep_json);
    if !packet.required_first_reads.is_empty() {
        println!("Required first reads:");
        for path in &packet.required_first_reads {
            println!("  - {path}");
        }
    }
    println!(
        "  note: launch-prep packet does not authorize enqueue, launch, approval, file movement, cleanup, or accepted truth"
    );
    Ok(())
}

pub(super) fn present_remote_operator_plans(
    profile: &str,
    args: &RemoteOperatorPlansArgs,
    items: &[OffdeskPlanRegistryItem],
) -> Result<()> {
    let payload = build_remote_operator_plans_payload(args, items)?;
    let observed_hash = observed_hash_for(&payload)?;
    let card = remote_operator_plans_card(&payload, observed_hash);
    let projection = remote_operator_projection(profile, &args.transport, "plans", card, payload);
    print_remote_operator_projection(&projection, args.json)
}

pub(super) fn present_remote_operator_plan_detail(
    profile: &str,
    args: &RemoteOperatorShowArgs,
    detail: &OffdeskPlanRegistryDetail,
) -> Result<()> {
    let payload = build_remote_operator_plan_detail_payload(detail)?;
    let observed_hash = observed_hash_for(&payload)?;
    let card = remote_operator_show_card(&payload, observed_hash);
    let projection = remote_operator_projection(profile, &args.transport, "show", card, payload);
    print_remote_operator_projection(&projection, args.json)
}

fn build_remote_operator_plans_payload(
    args: &RemoteOperatorPlansArgs,
    items: &[OffdeskPlanRegistryItem],
) -> Result<RemoteOperatorPlansPayload> {
    let plans = items
        .iter()
        .map(remote_operator_plan_summary_from_item)
        .collect::<Result<Vec<_>>>()?;
    Ok(RemoteOperatorPlansPayload {
        filters: RemoteOperatorPlanFilters {
            project_key: safe_optional(args.project_key.as_deref()),
            task_id: safe_optional(args.task_id.as_deref()),
            profile_key: safe_optional(args.profile_key.as_deref()),
            artifact_kind: safe_optional(args.artifact_kind.as_deref()),
            latest: args.latest,
        },
        plan_count: plans.len(),
        plans,
    })
}

fn build_remote_operator_plan_detail_payload(
    detail: &OffdeskPlanRegistryDetail,
) -> Result<RemoteOperatorPlanDetailPayload> {
    Ok(RemoteOperatorPlanDetailPayload {
        plan: remote_operator_plan_summary_from_detail(detail)?,
        reviews: detail
            .reviews
            .iter()
            .map(remote_operator_plan_review_summary)
            .collect(),
        launch_preps: detail
            .launch_preps
            .iter()
            .map(remote_operator_launch_prep_summary)
            .collect(),
        does_not_authorize: detail
            .registration
            .does_not_authorize
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
    })
}

fn remote_operator_plan_summary_from_item(
    item: &OffdeskPlanRegistryItem,
) -> Result<RemoteOperatorPlanSummary> {
    remote_operator_plan_summary(
        &item.plan_id,
        &item.registration,
        &item.review_state,
        item.review_count,
        item.latest_review.as_ref(),
        item.launch_prep_count,
        item.latest_launch_prep.as_ref(),
    )
}

fn remote_operator_plan_summary_from_detail(
    detail: &OffdeskPlanRegistryDetail,
) -> Result<RemoteOperatorPlanSummary> {
    remote_operator_plan_summary(
        &detail.plan_id,
        &detail.registration,
        &detail.review_state,
        detail.review_count,
        detail.latest_review.as_ref(),
        detail.launch_prep_count,
        detail.latest_launch_prep.as_ref(),
    )
}

fn remote_operator_plan_summary(
    plan_id: &str,
    registration: &OffdeskPlanRegistration,
    review_state: &OffdeskPlanReviewState,
    review_count: usize,
    latest_review: Option<&OffdeskPlanReviewRecord>,
    launch_prep_count: usize,
    latest_launch_prep: Option<&OffdeskPlanLaunchPrepPacket>,
) -> Result<RemoteOperatorPlanSummary> {
    let core = RemoteOperatorPlanSummaryCore {
        plan_id: operator_safe_text(plan_id),
        artifact_kind: operator_safe_text(&registration.artifact_kind),
        plan_schema: operator_safe_text(&registration.plan_schema),
        profile_key: safe_optional(registration.profile_key.as_deref()),
        project_key: safe_optional(registration.project_key.as_deref()),
        request_id: safe_optional(registration.request_id.as_deref()),
        task_id: safe_optional(registration.task_id.as_deref()),
        registered_at: registration.registered_at,
        source_sha256: registration.source_sha256.clone(),
        review_status: operator_safe_text(&review_state.status),
        review_count,
        latest_review_id: latest_review
            .map(|review| operator_safe_text(&review.review_id))
            .or_else(|| safe_optional(review_state.latest_review_id.as_deref())),
        launch_prep_count,
        latest_launch_prep_id: latest_launch_prep.map(|packet| operator_safe_text(&packet.prep_id)),
        ready_for_operator_review: registration.ready_for_operator_review,
        launch_preparation_candidate: review_state.ready_for_launch_preparation_candidate,
        ready_for_enqueue: registration.ready_for_enqueue,
        next_safe_action: operator_safe_text(&review_state.next_safe_action),
        remote_actions: vec!["inspect_plan".to_string()],
    };
    let observed_hash = observed_hash_for(&core)?;
    Ok(RemoteOperatorPlanSummary {
        core,
        observed_hash,
    })
}

fn remote_operator_plan_review_summary(
    review: &OffdeskPlanReviewRecord,
) -> RemoteOperatorPlanReviewSummary {
    RemoteOperatorPlanReviewSummary {
        review_id: operator_safe_text(&review.review_id),
        reviewed_at: review.reviewed_at,
        decision: review.decision,
        reviewer: operator_safe_text(&review.reviewer),
        ready_for_launch_preparation_candidate: review.ready_for_launch_preparation_candidate,
        ready_for_enqueue: review.ready_for_enqueue,
        blockers: review
            .blockers
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
        followups: review
            .followups
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
    }
}

fn remote_operator_launch_prep_summary(
    packet: &OffdeskPlanLaunchPrepPacket,
) -> RemoteOperatorLaunchPrepSummary {
    RemoteOperatorLaunchPrepSummary {
        prep_id: operator_safe_text(&packet.prep_id),
        prepared_at: packet.prepared_at,
        review_id: operator_safe_text(&packet.review_id),
        launch_preparation_candidate: packet.launch_preparation_candidate,
        ready_for_launch: packet.ready_for_launch,
        ready_for_enqueue: packet.ready_for_enqueue,
        next_safe_action: operator_safe_text(&packet.next_safe_action),
    }
}

fn remote_operator_plans_card(
    payload: &RemoteOperatorPlansPayload,
    observed_hash: String,
) -> RemoteOperatorCard {
    let detail_lines = payload
        .plans
        .iter()
        .take(3)
        .map(|plan| {
            format!(
                "{}: {} review={}",
                plan.core.plan_id, plan.core.artifact_kind, plan.core.review_status
            )
        })
        .collect();
    remote_operator_card(
        "Forager Remote Plans",
        vec![
            format!("plans: {}", payload.plan_count),
            format!(
                "filter project: {}",
                payload.filters.project_key.as_deref().unwrap_or("any")
            ),
            "remote plan review requires a registered artifact".to_string(),
        ],
        detail_lines,
        observed_hash,
        vec!["inspect_plans".to_string()],
    )
}

fn remote_operator_show_card(
    payload: &RemoteOperatorPlanDetailPayload,
    observed_hash: String,
) -> RemoteOperatorCard {
    remote_operator_card(
        "Forager Remote Plan Detail",
        vec![
            format!("plan: {}", payload.plan.core.plan_id),
            format!(
                "review: {} / launch-preps: {}",
                payload.plan.core.review_status,
                payload.launch_preps.len()
            ),
            format!("next: {}", payload.plan.core.next_safe_action),
        ],
        vec![
            format!("reviews: {}", payload.reviews.len()),
            "remote launch and mutation remain disabled".to_string(),
        ],
        observed_hash,
        vec!["inspect_plan".to_string()],
    )
}

fn safe_optional(value: Option<&str>) -> Option<String> {
    value.map(operator_safe_text)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn safe_optional_redacts_plan_filter_secrets() {
        let safe = safe_optional(Some("project token=sk-secretsecretsecretsecret"));

        assert_eq!(safe.as_deref(), Some("project token=[REDACTED]"));
    }

    #[test]
    fn plan_cards_keep_remote_mutation_disabled() {
        let payload = RemoteOperatorPlansPayload {
            filters: RemoteOperatorPlanFilters {
                project_key: None,
                task_id: None,
                profile_key: None,
                artifact_kind: None,
                latest: false,
            },
            plan_count: 0,
            plans: Vec::new(),
        };
        let card = remote_operator_plans_card(&payload, "sha256:test".to_string());

        assert_eq!(card.remote_actions, vec!["inspect_plans"]);
        assert!(card
            .disabled_remote_actions
            .iter()
            .any(|action| action == "dispatch"));
    }
}
