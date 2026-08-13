//! CLI command adapter for Offdesk plan registration, review, and launch prep.
//!
//! This module coordinates the registry storage adapter with typed workflow
//! policy. It does not own validation rules or record serialization.

use anyhow::{bail, Result};
use chrono::Utc;
use uuid::Uuid;

use super::plan_registry::{
    allocate_offdesk_plan_launch_prep_path, allocate_offdesk_plan_review_record_path,
    find_offdesk_plan_registry_item, load_offdesk_plan_registry_items, load_offdesk_plan_reviews,
    offdesk_plan_registry_dir, persist_offdesk_plan_source_copy, read_offdesk_plan_source,
    write_offdesk_plan_launch_prep_packet, write_offdesk_plan_registration,
    write_offdesk_plan_review_record,
};
use super::{PlanArgs, PlanLaunchPrepArgs, PlanReviewArgs};
use crate::offdesk::{
    OffdeskPlanLaunchPrepBuildInput, OffdeskPlanLaunchPrepPacket, OffdeskPlanRegistration,
    OffdeskPlanRegistrationBuildInput, OffdeskPlanRegistryItem, OffdeskPlanReviewBuildInput,
    OffdeskPlanReviewRecord,
};
use crate::session::{get_profile_dir, DEFAULT_PROFILE};

pub(super) fn register_offdesk_plan(
    profile: &str,
    args: &PlanArgs,
) -> Result<OffdeskPlanRegistration> {
    let source = read_offdesk_plan_source(&args.input)?;
    let summary = crate::offdesk::validate_offdesk_plan_input(&source.value)?;
    let registered_at = Utc::now();
    let profile_dir = if args.dry_run {
        None
    } else {
        Some(get_profile_dir(profile)?)
    };
    let artifacts = persist_offdesk_plan_source_copy(
        profile_dir.as_deref(),
        registered_at,
        summary.artifact_kind,
        &source,
    )?;

    let registration =
        crate::offdesk::build_offdesk_plan_registration(OffdeskPlanRegistrationBuildInput {
            registered_at,
            forager_profile: profile,
            source_path: &source.source_path,
            source_sha256: &source.source_sha256,
            summary,
            project_key: args.project_key.as_deref(),
            request_id: args.request_id.as_deref(),
            task_id: args.task_id.as_deref(),
            dry_run: args.dry_run,
            artifacts,
        });

    write_offdesk_plan_registration(&registration)?;
    Ok(registration)
}

pub(super) fn record_offdesk_plan_review(
    profile: &str,
    args: &PlanReviewArgs,
) -> Result<OffdeskPlanReviewRecord> {
    let items = load_offdesk_plan_registry_items(profile)?;
    let Some(item) = find_offdesk_plan_registry_item(items, &args.plan_ref) else {
        bail!("Registered Offdesk plan not found: {}", args.plan_ref);
    };
    let record = build_offdesk_plan_review_record(profile, &item, args)?;
    write_offdesk_plan_review_record(&record)?;
    Ok(record)
}

pub(super) fn prepare_offdesk_plan_launch(
    profile: &str,
    args: &PlanLaunchPrepArgs,
) -> Result<OffdeskPlanLaunchPrepPacket> {
    let items = load_offdesk_plan_registry_items(profile)?;
    let Some(item) = find_offdesk_plan_registry_item(items, &args.plan_ref) else {
        bail!("Registered Offdesk plan not found: {}", args.plan_ref);
    };
    let packet = build_offdesk_plan_launch_prep_packet(profile, &item, args)?;
    write_offdesk_plan_launch_prep_packet(&packet)?;
    Ok(packet)
}

fn build_offdesk_plan_review_record(
    profile: &str,
    item: &OffdeskPlanRegistryItem,
    args: &PlanReviewArgs,
) -> Result<OffdeskPlanReviewRecord> {
    crate::offdesk::validate_offdesk_plan_review_input(
        args.decision,
        &args.reason,
        &args.blockers,
    )?;

    let reviewed_at = Utc::now();
    let registry_dir = offdesk_plan_registry_dir(item)?;
    let review_record_path = allocate_offdesk_plan_review_record_path(&registry_dir, reviewed_at)?;
    let profile_name = profile_name(profile);
    let review_file = args
        .review_file
        .as_ref()
        .map(|path| path.to_string_lossy().into_owned());
    let review_record_json = review_record_path.display().to_string();
    let review_id = format!("plan_review_{}", short_uuid());

    crate::offdesk::build_offdesk_plan_review_record(OffdeskPlanReviewBuildInput {
        reviewed_at,
        review_id: &review_id,
        plan_id: &item.plan_id,
        forager_profile: profile_name,
        registration_path: &item.registration_path,
        source_sha256: &item.registration.source_sha256,
        decision: args.decision,
        reviewer: &args.reviewer,
        review_provider: args.review_provider.as_deref(),
        review_file,
        reason: &args.reason,
        blockers: &args.blockers,
        followups: &args.followups,
        registration_ready_for_operator_review: item.registration.ready_for_operator_review,
        registration_ready_for_launch_preparation: item.registration.ready_for_launch_preparation,
        registration_ready_for_enqueue: item.registration.ready_for_enqueue,
        registration_validation_failures: &item.registration.validation_failures,
        copied_source_json: item.registration.artifacts.copied_source_json.as_deref(),
        review_record_json: &review_record_json,
    })
}

fn build_offdesk_plan_launch_prep_packet(
    profile: &str,
    item: &OffdeskPlanRegistryItem,
    args: &PlanLaunchPrepArgs,
) -> Result<OffdeskPlanLaunchPrepPacket> {
    let registry_dir = offdesk_plan_registry_dir(item)?;
    let reviews = load_offdesk_plan_reviews(&registry_dir)?;
    let review = crate::offdesk::select_offdesk_plan_review(&reviews, args.review_id.as_deref())?;
    crate::offdesk::validate_offdesk_plan_launch_prep(review, &item.registration.source_sha256)?;

    let prepared_at = Utc::now();
    let launch_prep_path = allocate_offdesk_plan_launch_prep_path(&registry_dir, prepared_at)?;
    let prep_id = format!("plan_launch_prep_{}", short_uuid());
    let launch_prep_json = launch_prep_path.display().to_string();

    crate::offdesk::build_offdesk_plan_launch_prep_packet(OffdeskPlanLaunchPrepBuildInput {
        prepared_at,
        prep_id: &prep_id,
        plan_id: &item.plan_id,
        forager_profile: profile_name(profile),
        prepared_by: &args.prepared_by,
        registration_path: &item.registration_path,
        source_path: &item.registration.source_path,
        source_sha256: &item.registration.source_sha256,
        review,
        artifact_kind: &item.registration.artifact_kind,
        plan_schema: &item.registration.plan_schema,
        profile_key: item.registration.profile_key.as_deref(),
        project_key: item.registration.project_key.as_deref(),
        request_id: item.registration.request_id.as_deref(),
        task_id: item.registration.task_id.as_deref(),
        selected_plan_path: item.registration.selected_plan_path.as_deref(),
        copied_source_json: item.registration.artifacts.copied_source_json.as_deref(),
        notes: args.notes.as_deref(),
        launch_prep_json: &launch_prep_json,
    })
}

fn profile_name(profile: &str) -> &str {
    if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    }
}

fn short_uuid() -> String {
    Uuid::new_v4().to_string()[..8].to_string()
}
