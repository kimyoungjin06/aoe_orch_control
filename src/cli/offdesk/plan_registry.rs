//! CLI storage adapter for the Offdesk plan registry.
//!
//! Reads fail closed, and every record write uses append-only path allocation
//! plus create-new persistence.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::{read_only_profile_dir, write_new_file};
use crate::offdesk::{
    build_offdesk_plan_registry_detail, build_offdesk_plan_registry_item,
    OffdeskPlanLaunchPrepPacket, OffdeskPlanRegistration, OffdeskPlanRegistrationArtifacts,
    OffdeskPlanRegistryDetail, OffdeskPlanRegistryItem, OffdeskPlanReviewRecord,
};

pub(super) struct OffdeskPlanSource {
    pub value: Value,
    pub source_path: String,
    pub source_sha256: String,
    bytes: Vec<u8>,
}

pub(super) fn read_offdesk_plan_source(input: &Path) -> Result<OffdeskPlanSource> {
    let bytes = fs::read(input)
        .with_context(|| format!("read Offdesk plan artifact {}", input.display()))?;
    let value = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse Offdesk plan artifact {}", input.display()))?;
    let source_path = fs::canonicalize(input)
        .unwrap_or_else(|_| input.to_path_buf())
        .display()
        .to_string();
    let source_sha256 = {
        let mut hasher = Sha256::new();
        hasher.update(&bytes);
        format!("{:x}", hasher.finalize())
    };
    Ok(OffdeskPlanSource {
        value,
        source_path,
        source_sha256,
        bytes,
    })
}

pub(super) fn persist_offdesk_plan_source_copy(
    profile_dir: Option<&Path>,
    registered_at: DateTime<Utc>,
    artifact_kind: &str,
    source: &OffdeskPlanSource,
) -> Result<OffdeskPlanRegistrationArtifacts> {
    let Some(profile_dir) = profile_dir else {
        return Ok(OffdeskPlanRegistrationArtifacts {
            registry_dir: None,
            registration_json: None,
            copied_source_json: None,
        });
    };

    let registry_dir =
        allocate_offdesk_plan_registry_dir(profile_dir, registered_at, artifact_kind)?;
    let copied_source = registry_dir.join("source.json");
    write_new_file(&copied_source, &source.bytes)
        .with_context(|| format!("write Offdesk plan source copy {}", copied_source.display()))?;
    Ok(OffdeskPlanRegistrationArtifacts {
        registry_dir: Some(registry_dir.display().to_string()),
        registration_json: Some(registry_dir.join("registration.json").display().to_string()),
        copied_source_json: Some(copied_source.display().to_string()),
    })
}

pub(super) fn allocate_offdesk_plan_registry_dir(
    profile_dir: &Path,
    registered_at: DateTime<Utc>,
    artifact_kind: &str,
) -> Result<PathBuf> {
    let base_dir = profile_dir.join("offdesk_plans");
    fs::create_dir_all(&base_dir)
        .with_context(|| format!("create Offdesk plan registry {}", base_dir.display()))?;
    let timestamp = registered_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let name = if attempt == 0 {
            format!("{timestamp}_{artifact_kind}")
        } else {
            format!("{timestamp}_{artifact_kind}_{attempt:03}")
        };
        let path = base_dir.join(name);
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create Offdesk plan registry {}", path.display()))
            }
        }
    }

    bail!(
        "could not allocate Offdesk plan registry path in {}",
        base_dir.display()
    )
}

pub(super) fn allocate_offdesk_plan_review_record_path(
    registry_dir: &Path,
    reviewed_at: DateTime<Utc>,
) -> Result<PathBuf> {
    allocate_offdesk_plan_record_path(registry_dir, reviewed_at, "plan_review", "review")
}

pub(super) fn allocate_offdesk_plan_launch_prep_path(
    registry_dir: &Path,
    prepared_at: DateTime<Utc>,
) -> Result<PathBuf> {
    allocate_offdesk_plan_record_path(registry_dir, prepared_at, "launch_prep", "launch-prep")
}

fn allocate_offdesk_plan_record_path(
    registry_dir: &Path,
    recorded_at: DateTime<Utc>,
    filename_prefix: &str,
    error_kind: &str,
) -> Result<PathBuf> {
    fs::create_dir_all(registry_dir)
        .with_context(|| format!("create Offdesk plan registry {}", registry_dir.display()))?;
    let timestamp = recorded_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let filename = if attempt == 0 {
            format!("{filename_prefix}_{timestamp}.json")
        } else {
            format!("{filename_prefix}_{timestamp}_{attempt:03}.json")
        };
        let path = registry_dir.join(filename);
        if !path.exists() {
            return Ok(path);
        }
    }

    bail!(
        "could not allocate Offdesk plan {error_kind} path in {}",
        registry_dir.display()
    )
}

pub(super) fn write_offdesk_plan_registration(
    registration: &OffdeskPlanRegistration,
) -> Result<()> {
    let Some(registration_path) = registration.artifacts.registration_json.as_deref() else {
        return Ok(());
    };
    let bytes = serde_json::to_vec_pretty(registration)?;
    write_new_file(Path::new(registration_path), &bytes)
        .with_context(|| format!("write Offdesk plan registration {}", registration_path))?;
    Ok(())
}

pub(super) fn write_offdesk_plan_review_record(record: &OffdeskPlanReviewRecord) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(record)?;
    write_new_file(Path::new(&record.artifacts.review_record_json), &bytes)
        .with_context(|| format!("write {}", record.artifacts.review_record_json))?;
    Ok(())
}

pub(super) fn write_offdesk_plan_launch_prep_packet(
    packet: &OffdeskPlanLaunchPrepPacket,
) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(packet)?;
    write_new_file(Path::new(&packet.artifacts.launch_prep_json), &bytes)
        .with_context(|| format!("write {}", packet.artifacts.launch_prep_json))?;
    Ok(())
}

pub(super) fn load_offdesk_plan_registry_items(
    profile: &str,
) -> Result<Vec<OffdeskPlanRegistryItem>> {
    let registry_dir = read_only_profile_dir(profile)?.join("offdesk_plans");
    load_offdesk_plan_registry_items_from_dir(&registry_dir)
}

fn load_offdesk_plan_registry_items_from_dir(
    registry_dir: &Path,
) -> Result<Vec<OffdeskPlanRegistryItem>> {
    if !registry_dir.exists() {
        return Ok(Vec::new());
    }

    let mut items = Vec::new();
    for entry in fs::read_dir(registry_dir)
        .with_context(|| format!("read Offdesk plan registry {}", registry_dir.display()))?
    {
        let entry = entry.with_context(|| {
            format!(
                "read Offdesk plan registry entry {}",
                registry_dir.display()
            )
        })?;
        let file_type = entry.file_type().with_context(|| {
            format!(
                "read Offdesk plan registry entry type {}",
                entry.path().display()
            )
        })?;
        if !file_type.is_dir() {
            continue;
        }
        let registration_path = entry.path().join("registration.json");
        if !registration_path.exists() {
            continue;
        }
        let registration_bytes = fs::read(&registration_path).with_context(|| {
            format!(
                "read Offdesk plan registration {}",
                registration_path.display()
            )
        })?;
        let registration: OffdeskPlanRegistration = serde_json::from_slice(&registration_bytes)
            .with_context(|| {
                format!(
                    "parse Offdesk plan registration {}",
                    registration_path.display()
                )
            })?;
        let plan_id = entry.file_name().to_string_lossy().to_string();
        let reviews = load_offdesk_plan_reviews(&entry.path())?;
        let launch_preps = load_offdesk_plan_launch_preps(&entry.path())?;
        items.push(build_offdesk_plan_registry_item(
            plan_id,
            registration_path.display().to_string(),
            registration,
            &reviews,
            &launch_preps,
        ));
    }

    Ok(items)
}

pub(super) fn load_offdesk_plan_registry_detail(
    item: OffdeskPlanRegistryItem,
) -> Result<OffdeskPlanRegistryDetail> {
    let registry_dir = offdesk_plan_registry_dir(&item)?;
    let reviews = load_offdesk_plan_reviews(&registry_dir)?;
    let launch_preps = load_offdesk_plan_launch_preps(&registry_dir)?;
    Ok(build_offdesk_plan_registry_detail(
        item,
        reviews,
        launch_preps,
    ))
}

pub(super) fn load_offdesk_plan_reviews(
    registry_dir: &Path,
) -> Result<Vec<OffdeskPlanReviewRecord>> {
    let mut reviews = Vec::new();
    if !registry_dir.exists() {
        return Ok(reviews);
    }
    for entry in fs::read_dir(registry_dir)
        .with_context(|| format!("read Offdesk plan registry {}", registry_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let filename = entry.file_name().to_string_lossy().to_string();
        if !filename.starts_with("plan_review_") || !filename.ends_with(".json") {
            continue;
        }
        let path = entry.path();
        let review_bytes = fs::read(&path)
            .with_context(|| format!("read Offdesk plan review {}", path.display()))?;
        let review: OffdeskPlanReviewRecord = serde_json::from_slice(&review_bytes)
            .with_context(|| format!("parse Offdesk plan review {}", path.display()))?;
        reviews.push(review);
    }
    reviews.sort_by_key(|review| review.reviewed_at);
    Ok(reviews)
}

fn load_offdesk_plan_launch_preps(registry_dir: &Path) -> Result<Vec<OffdeskPlanLaunchPrepPacket>> {
    let mut packets = Vec::new();
    if !registry_dir.exists() {
        return Ok(packets);
    }
    for entry in fs::read_dir(registry_dir)
        .with_context(|| format!("read Offdesk plan registry {}", registry_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let filename = entry.file_name().to_string_lossy().to_string();
        if !filename.starts_with("launch_prep_") || !filename.ends_with(".json") {
            continue;
        }
        let path = entry.path();
        let packet_bytes = fs::read(&path)
            .with_context(|| format!("read Offdesk plan launch-prep {}", path.display()))?;
        let packet: OffdeskPlanLaunchPrepPacket = serde_json::from_slice(&packet_bytes)
            .with_context(|| format!("parse Offdesk plan launch-prep {}", path.display()))?;
        packets.push(packet);
    }
    packets.sort_by_key(|packet| packet.prepared_at);
    Ok(packets)
}

pub(super) fn offdesk_plan_registry_dir(item: &OffdeskPlanRegistryItem) -> Result<PathBuf> {
    if let Some(registry_dir) = item.registration.artifacts.registry_dir.as_deref() {
        return Ok(PathBuf::from(registry_dir));
    }
    Path::new(&item.registration_path)
        .parent()
        .map(Path::to_path_buf)
        .ok_or_else(|| anyhow::anyhow!("registered Offdesk plan is missing registry directory"))
}

pub(super) fn find_offdesk_plan_registry_item(
    items: Vec<OffdeskPlanRegistryItem>,
    plan_ref: &str,
) -> Option<OffdeskPlanRegistryItem> {
    let normalized_ref = normalize_offdesk_plan_ref_path(plan_ref);
    items.into_iter().find(|item| {
        if item.plan_id == plan_ref {
            return true;
        }
        if normalize_offdesk_plan_ref_path(&item.registration_path) == normalized_ref {
            return true;
        }
        for path in [
            item.registration.artifacts.registry_dir.as_deref(),
            item.registration.artifacts.registration_json.as_deref(),
            item.registration.artifacts.copied_source_json.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if normalize_offdesk_plan_ref_path(path) == normalized_ref {
                return true;
            }
        }
        false
    })
}

fn normalize_offdesk_plan_ref_path(path: &str) -> String {
    #[cfg(target_os = "macos")]
    {
        path.strip_prefix("/private").unwrap_or(path).to_owned()
    }
    #[cfg(not(target_os = "macos"))]
    {
        path.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::offdesk::{
        build_offdesk_plan_registry_item, offdesk_plan_registration_denials,
        OffdeskPlanRegistrationArtifacts, OFFDESK_PLAN_REGISTRATION_SCHEMA,
    };
    use serde_json::json;
    use tempfile::tempdir;

    fn registration(registry_dir: Option<String>) -> OffdeskPlanRegistration {
        OffdeskPlanRegistration {
            schema: OFFDESK_PLAN_REGISTRATION_SCHEMA.to_string(),
            registered_at: "2026-08-13T01:00:00Z".parse().expect("valid timestamp"),
            forager_profile: "forager-ops".to_string(),
            source_path: "/workspace/OVERNIGHT_PLAN.json".to_string(),
            source_sha256: "abc123".to_string(),
            artifact_kind: "offdesk_multiturn_plan".to_string(),
            plan_schema: "offdesk_multiturn_plan.v1".to_string(),
            profile_key: Some("generic".to_string()),
            profile_name: None,
            project_key: Some("project".to_string()),
            request_id: Some("request".to_string()),
            task_id: Some("task".to_string()),
            ready_for_operator_review: true,
            ready_for_launch_preparation: false,
            ready_for_enqueue: false,
            validation_failures: Vec::new(),
            decision: None,
            consensus: None,
            selected_plan_path: None,
            dry_run: false,
            artifacts: OffdeskPlanRegistrationArtifacts {
                registration_json: registry_dir.as_ref().map(|dir| {
                    Path::new(dir)
                        .join("registration.json")
                        .display()
                        .to_string()
                }),
                copied_source_json: registry_dir
                    .as_ref()
                    .map(|dir| Path::new(dir).join("source.json").display().to_string()),
                registry_dir,
            },
            does_not_authorize: offdesk_plan_registration_denials(),
        }
    }

    fn item(registration_path: String, registry_dir: Option<String>) -> OffdeskPlanRegistryItem {
        build_offdesk_plan_registry_item(
            "plan_123".to_string(),
            registration_path,
            registration(registry_dir),
            &[],
            &[],
        )
    }

    #[test]
    fn source_ingestion_observes_canonical_path_hash_and_json() {
        let temp = tempdir().expect("temp dir");
        let source_path = temp.path().join("plan.json");
        fs::write(&source_path, b"{}").expect("write source");

        let source = read_offdesk_plan_source(&source_path).expect("read plan source");

        assert_eq!(source.value, json!({}));
        assert_eq!(
            source.source_path,
            fs::canonicalize(&source_path)
                .expect("canonical source")
                .display()
                .to_string()
        );
        assert_eq!(
            source.source_sha256,
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        );

        fs::write(&source_path, b"{").expect("write invalid source");
        let error = read_offdesk_plan_source(&source_path)
            .err()
            .expect("invalid JSON must fail");
        assert!(error.to_string().contains("parse Offdesk plan artifact"));
    }

    #[test]
    fn source_copy_is_write_free_for_dry_run_and_preserves_exact_bytes() {
        let temp = tempdir().expect("temp dir");
        let source_path = temp.path().join("plan.json");
        let source_bytes = b"{\n  \"schema\": \"offdesk_multiturn_plan.v1\"\n}\n";
        fs::write(&source_path, source_bytes).expect("write source");
        let source = read_offdesk_plan_source(&source_path).expect("read source");
        let registered_at: DateTime<Utc> = "2026-08-13T01:02:03Z".parse().expect("valid timestamp");

        let dry_run = persist_offdesk_plan_source_copy(
            None,
            registered_at,
            "offdesk_multiturn_plan",
            &source,
        )
        .expect("dry-run source persistence");
        assert!(dry_run.registry_dir.is_none());
        assert!(!temp.path().join("offdesk_plans").exists());

        let stored = persist_offdesk_plan_source_copy(
            Some(temp.path()),
            registered_at,
            "offdesk_multiturn_plan",
            &source,
        )
        .expect("persist source copy");
        let copied_source = stored
            .copied_source_json
            .as_deref()
            .expect("copied source path");
        assert_eq!(
            fs::read(copied_source).expect("read source copy"),
            source_bytes
        );
        assert!(stored
            .registration_json
            .as_deref()
            .expect("registration path")
            .ends_with("/registration.json"));
    }

    #[test]
    fn append_only_allocators_suffix_collisions() {
        let temp = tempdir().expect("temp dir");
        let recorded_at: DateTime<Utc> = "2026-08-13T01:02:03Z".parse().expect("valid timestamp");

        let first_registry =
            allocate_offdesk_plan_registry_dir(temp.path(), recorded_at, "offdesk_multiturn_plan")
                .expect("first registry dir");
        let second_registry =
            allocate_offdesk_plan_registry_dir(temp.path(), recorded_at, "offdesk_multiturn_plan")
                .expect("second registry dir");
        assert_eq!(
            first_registry.file_name().and_then(|value| value.to_str()),
            Some("20260813T010203Z_offdesk_multiturn_plan")
        );
        assert_eq!(
            second_registry.file_name().and_then(|value| value.to_str()),
            Some("20260813T010203Z_offdesk_multiturn_plan_001")
        );

        let first_review = allocate_offdesk_plan_review_record_path(&first_registry, recorded_at)
            .expect("first review path");
        fs::write(&first_review, b"{}").expect("reserve first review path");
        let second_review = allocate_offdesk_plan_review_record_path(&first_registry, recorded_at)
            .expect("second review path");
        assert_eq!(
            first_review.file_name().and_then(|value| value.to_str()),
            Some("plan_review_20260813T010203Z.json")
        );
        assert_eq!(
            second_review.file_name().and_then(|value| value.to_str()),
            Some("plan_review_20260813T010203Z_001.json")
        );

        let first_prep = allocate_offdesk_plan_launch_prep_path(&first_registry, recorded_at)
            .expect("first launch-prep path");
        fs::write(&first_prep, b"{}").expect("reserve first launch-prep path");
        let second_prep = allocate_offdesk_plan_launch_prep_path(&first_registry, recorded_at)
            .expect("second launch-prep path");
        assert_eq!(
            first_prep.file_name().and_then(|value| value.to_str()),
            Some("launch_prep_20260813T010203Z.json")
        );
        assert_eq!(
            second_prep.file_name().and_then(|value| value.to_str()),
            Some("launch_prep_20260813T010203Z_001.json")
        );
    }

    #[test]
    fn registration_persistence_skips_dry_run_and_refuses_overwrite() {
        let dry_run = registration(None);
        write_offdesk_plan_registration(&dry_run).expect("dry-run persistence is a no-op");

        let temp = tempdir().expect("temp dir");
        let plan_dir = temp.path().join("plan_123");
        fs::create_dir_all(&plan_dir).expect("create plan dir");
        let stored = registration(Some(plan_dir.display().to_string()));
        let registration_path = plan_dir.join("registration.json");

        write_offdesk_plan_registration(&stored).expect("write registration");
        let original = fs::read(&registration_path).expect("read registration");
        assert_eq!(
            serde_json::from_slice::<OffdeskPlanRegistration>(&original)
                .expect("parse stored registration")
                .source_sha256,
            "abc123"
        );

        let error = write_offdesk_plan_registration(&stored)
            .expect_err("append-only registration must not overwrite");
        assert!(error
            .to_string()
            .contains("write Offdesk plan registration"));
        assert_eq!(
            fs::read(registration_path).expect("read preserved registration"),
            original
        );
    }

    #[test]
    fn missing_registry_is_empty_and_valid_registration_loads() {
        let temp = tempdir().expect("temp dir");
        let missing = temp.path().join("missing");
        assert!(load_offdesk_plan_registry_items_from_dir(&missing)
            .expect("missing registry")
            .is_empty());

        let registry = temp.path().join("offdesk_plans");
        let plan_dir = registry.join("plan_123");
        fs::create_dir_all(&plan_dir).expect("create plan dir");
        fs::write(
            plan_dir.join("registration.json"),
            serde_json::to_vec_pretty(&registration(Some(plan_dir.display().to_string())))
                .expect("serialize registration"),
        )
        .expect("write registration");

        let items =
            load_offdesk_plan_registry_items_from_dir(&registry).expect("load plan registry");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].plan_id, "plan_123");
        assert_eq!(items[0].review_state.status, "unreviewed");
    }

    #[test]
    fn corrupt_registry_records_fail_closed() {
        let temp = tempdir().expect("temp dir");
        let registry = temp.path().join("offdesk_plans");
        let plan_dir = registry.join("plan_corrupt");
        fs::create_dir_all(&plan_dir).expect("create plan dir");
        let registration_path = plan_dir.join("registration.json");
        fs::write(&registration_path, b"{").expect("write corrupt registration");

        let error = load_offdesk_plan_registry_items_from_dir(&registry)
            .expect_err("corrupt registration must fail");
        assert!(error
            .to_string()
            .contains("parse Offdesk plan registration"));

        fs::write(
            &registration_path,
            serde_json::to_vec_pretty(&registration(Some(plan_dir.display().to_string())))
                .expect("serialize registration"),
        )
        .expect("replace registration");
        let review_path = plan_dir.join("plan_review_corrupt.json");
        fs::write(&review_path, b"{").expect("write corrupt review");

        let error = load_offdesk_plan_registry_items_from_dir(&registry)
            .expect_err("corrupt review must fail");
        assert!(error.to_string().contains("parse Offdesk plan review"));

        fs::remove_file(review_path).expect("remove corrupt review");
        fs::write(plan_dir.join("launch_prep_corrupt.json"), b"{")
            .expect("write corrupt launch prep");

        let error = load_offdesk_plan_registry_items_from_dir(&registry)
            .expect_err("corrupt launch prep must fail");
        assert!(error.to_string().contains("parse Offdesk plan launch-prep"));
    }

    #[test]
    fn history_records_are_ordered_by_record_timestamp() {
        let temp = tempdir().expect("temp dir");
        let plan_dir = temp.path().join("plan_123");
        fs::create_dir_all(&plan_dir).expect("create plan dir");

        for (filename, review_id, reviewed_at) in [
            (
                "plan_review_a_later.json",
                "review_later",
                "2026-08-13T03:00:00Z",
            ),
            (
                "plan_review_z_earlier.json",
                "review_earlier",
                "2026-08-13T02:00:00Z",
            ),
        ] {
            let value = json!({
                "schema": "offdesk_plan_review.v1",
                "reviewed_at": reviewed_at,
                "review_id": review_id,
                "plan_id": "plan_123",
                "forager_profile": "forager-ops",
                "registration_path": "/tmp/plan_123/registration.json",
                "source_sha256": "abc123",
                "decision": "approved",
                "reviewer": "operator",
                "review_provider": null,
                "review_file": null,
                "reason": "reviewed",
                "blockers": [],
                "followups": [],
                "ready_for_launch_preparation_candidate": true,
                "ready_for_enqueue": false,
                "read_only_project_state": true,
                "applies_file_operations": false,
                "artifacts": {
                    "registration_json": "/tmp/plan_123/registration.json",
                    "copied_source_json": "/tmp/plan_123/source.json",
                    "review_record_json": format!("/tmp/plan_123/{filename}")
                },
                "does_not_authorize": []
            });
            fs::write(
                plan_dir.join(filename),
                serde_json::to_vec_pretty(&value).expect("serialize review"),
            )
            .expect("write review");
        }

        for (filename, prep_id, prepared_at) in [
            (
                "launch_prep_a_later.json",
                "prep_later",
                "2026-08-13T05:00:00Z",
            ),
            (
                "launch_prep_z_earlier.json",
                "prep_earlier",
                "2026-08-13T04:00:00Z",
            ),
        ] {
            let value = json!({
                "schema": "offdesk_plan_launch_prep.v1",
                "prepared_at": prepared_at,
                "prep_id": prep_id,
                "plan_id": "plan_123",
                "forager_profile": "forager-ops",
                "prepared_by": "operator",
                "registration_path": "/tmp/plan_123/registration.json",
                "source_path": "/workspace/OVERNIGHT_PLAN.json",
                "source_sha256": "abc123",
                "review_id": "review_later",
                "review_decision": "approved",
                "review_record_json": "/tmp/plan_123/plan_review_a_later.json",
                "artifact_kind": "offdesk_multiturn_plan",
                "plan_schema": "offdesk_multiturn_plan.v1",
                "profile_key": "generic",
                "project_key": "project",
                "request_id": "request",
                "task_id": "task",
                "selected_plan_path": null,
                "required_first_reads": [],
                "launch_preparation_candidate": true,
                "ready_for_launch": false,
                "ready_for_enqueue": false,
                "next_safe_action": "build_execution_brief_then_use_existing_offdesk_gate",
                "notes": null,
                "read_only_project_state": true,
                "applies_file_operations": false,
                "artifacts": {
                    "registration_json": "/tmp/plan_123/registration.json",
                    "copied_source_json": "/tmp/plan_123/source.json",
                    "review_record_json": "/tmp/plan_123/plan_review_a_later.json",
                    "launch_prep_json": format!("/tmp/plan_123/{filename}")
                },
                "does_not_authorize": []
            });
            fs::write(
                plan_dir.join(filename),
                serde_json::to_vec_pretty(&value).expect("serialize launch prep"),
            )
            .expect("write launch prep");
        }

        let reviews = load_offdesk_plan_reviews(&plan_dir).expect("load reviews");
        assert_eq!(
            reviews
                .iter()
                .map(|review| review.review_id.as_str())
                .collect::<Vec<_>>(),
            vec!["review_earlier", "review_later"]
        );

        let launch_preps = load_offdesk_plan_launch_preps(&plan_dir).expect("load launch preps");
        assert_eq!(
            launch_preps
                .iter()
                .map(|packet| packet.prep_id.as_str())
                .collect::<Vec<_>>(),
            vec!["prep_earlier", "prep_later"]
        );
    }

    #[test]
    fn registry_dir_prefers_artifact_and_falls_back_to_registration_parent() {
        let explicit = item(
            "/fallback/registration.json".to_string(),
            Some("/explicit/plan_123".to_string()),
        );
        assert_eq!(
            offdesk_plan_registry_dir(&explicit).expect("explicit registry dir"),
            PathBuf::from("/explicit/plan_123")
        );

        let fallback = item("/fallback/registration.json".to_string(), None);
        assert_eq!(
            offdesk_plan_registry_dir(&fallback).expect("fallback registry dir"),
            PathBuf::from("/fallback")
        );
    }

    #[test]
    fn plan_reference_matches_id_and_all_registered_artifact_paths() {
        let registry_dir = "/tmp/plan_123".to_string();
        let registration_path = "/tmp/plan_123/registration.json".to_string();
        let plan = item(registration_path.clone(), Some(registry_dir.clone()));

        for plan_ref in [
            "plan_123",
            registry_dir.as_str(),
            registration_path.as_str(),
            "/tmp/plan_123/source.json",
        ] {
            assert!(find_offdesk_plan_registry_item(vec![plan.clone()], plan_ref).is_some());
        }
        assert!(find_offdesk_plan_registry_item(vec![plan], "/tmp/other.json").is_none());
    }
}
