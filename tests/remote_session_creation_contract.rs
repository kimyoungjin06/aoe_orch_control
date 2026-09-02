use anyhow::Result;
use forager::offdesk::{
    load_remote_session_policy_for_inspection, remote_session_cleanup_receipt_sha256,
    remote_session_identity_sha256, remote_session_launch_profile_sha256,
    remote_session_policy_harness_launch_sha256, remote_session_policy_launch_command,
    remote_session_policy_sha256, remote_session_preview_sha256, remote_session_receipt_sha256,
    remote_session_request_sha256, remote_session_reservation_sha256,
    resolve_loaded_remote_session_policy_target, resolve_remote_session_policy_target,
    validate_remote_session_contract_chain, validate_remote_session_duplicate_replay,
    validate_remote_session_reservation_transition, RemoteSessionCleanupReceiptV1,
    RemoteSessionCleanupWorktreeOutcomeV1, RemoteSessionLaunchPreviewV1,
    RemoteSessionLaunchReceiptV1, RemoteSessionLaunchRequestV1, RemoteSessionLaunchReservationV1,
    RemoteSessionLaunchResultV1, RemoteSessionPolicyV1, RemoteSessionReservationStateV1,
    RemoteSessionUtcTimestamp, REMOTE_SESSION_CLEANUP_RECEIPT_SCHEMA,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use tempfile::tempdir;

const POLICY_JSON: &str =
    include_str!("fixtures/remote_session_creation/remote_session_policy.v1.canonical.json");
const PREVIEW_JSON: &str = include_str!(
    "fixtures/remote_session_creation/remote_session_launch_preview.v1.canonical.json"
);
const REQUEST_JSON: &str = include_str!(
    "fixtures/remote_session_creation/remote_session_launch_request.v1.canonical.json"
);
const RESERVATION_JSON: &str = include_str!(
    "fixtures/remote_session_creation/remote_session_launch_reservation.v1.canonical.json"
);
const RECEIPT_JSON: &str = include_str!(
    "fixtures/remote_session_creation/remote_session_launch_receipt.v1.canonical.json"
);
const CLEANUP_RECEIPT_JSON: &str = include_str!(
    "fixtures/remote_session_creation/remote_session_cleanup_receipt.v1.canonical.json"
);
const HASHES_JSON: &str = include_str!("fixtures/remote_session_creation/hashes.json");

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractHashes {
    schema: String,
    policy_sha256: String,
    launch_profile_sha256: String,
    policy_harness_launch_sha256: String,
    preview_sha256: String,
    request_sha256: String,
    reservation_sha256: String,
    cleanup_receipt_sha256: String,
    remote_session_identity_sha256: String,
    receipt_sha256: String,
}

fn canonical_fixture(value: &str) -> &str {
    value.strip_suffix('\n').unwrap_or(value)
}

fn fixtures() -> Result<(
    RemoteSessionPolicyV1,
    RemoteSessionLaunchPreviewV1,
    RemoteSessionLaunchRequestV1,
    RemoteSessionLaunchReservationV1,
    RemoteSessionLaunchReceiptV1,
    ContractHashes,
)> {
    Ok((
        serde_json::from_str(POLICY_JSON)?,
        serde_json::from_str(PREVIEW_JSON)?,
        serde_json::from_str(REQUEST_JSON)?,
        serde_json::from_str(RESERVATION_JSON)?,
        serde_json::from_str(RECEIPT_JSON)?,
        serde_json::from_str(HASHES_JSON)?,
    ))
}

fn cleanup_fixture() -> Result<RemoteSessionCleanupReceiptV1> {
    Ok(serde_json::from_str(CLEANUP_RECEIPT_JSON)?)
}

fn rebind_live_identity(receipt: &mut RemoteSessionLaunchReceiptV1) -> Result<()> {
    let identity = receipt
        .remote_session_identity
        .as_mut()
        .expect("test receipt live identity");
    let mut value = serde_json::to_value(&*identity)?;
    value
        .as_object_mut()
        .expect("identity object")
        .remove("binding_sha256");
    identity.binding_sha256 = format!("{:x}", Sha256::digest(serde_json::to_vec(&value)?));
    receipt.remote_session_identity_sha256 = Some(remote_session_identity_sha256(identity)?);
    Ok(())
}

fn rebind_contract_chain(
    policy: &RemoteSessionPolicyV1,
    preview: &mut RemoteSessionLaunchPreviewV1,
    request: &mut RemoteSessionLaunchRequestV1,
    reservation: &mut RemoteSessionLaunchReservationV1,
    receipt: &mut RemoteSessionLaunchReceiptV1,
) -> Result<()> {
    preview.policy_sha256 = remote_session_policy_sha256(policy)?;
    preview.launch_profile_sha256 =
        remote_session_launch_profile_sha256(&policy.launch_profiles[0])?;
    preview.harness_launch_sha256 =
        remote_session_policy_harness_launch_sha256(&policy.launch_profiles[0])?;
    rebind_downstream_contract(preview, request, reservation, receipt)
}

fn rebind_downstream_contract(
    preview: &RemoteSessionLaunchPreviewV1,
    request: &mut RemoteSessionLaunchRequestV1,
    reservation: &mut RemoteSessionLaunchReservationV1,
    receipt: &mut RemoteSessionLaunchReceiptV1,
) -> Result<()> {
    request.preview = preview.clone();
    request.confirmation.preview_id = preview.preview_id.clone();
    request.confirmation.preview_sha256 = remote_session_preview_sha256(preview)?;
    request.confirmation.operator_identity_sha256 = preview.operator_identity_sha256.clone();
    request.confirmation.control_generation_sha256 = preview.control_generation_sha256.clone();
    request.confirmation.observed_state_sha256 = preview.observed_state_sha256.clone();
    let request_sha256 = remote_session_request_sha256(request)?;
    reservation.request_id = request.request_id.clone();
    reservation.request_sha256 = request_sha256.clone();
    reservation.policy_id = preview.policy_id.clone();
    reservation.policy_sha256 = preview.policy_sha256.clone();
    reservation.root_id = preview.root_id.clone();
    reservation.project_key = preview.project_key.clone();
    reservation.project_root_identity_sha256 = preview.initial_project_root_identity_sha256.clone();
    reservation.launch_profile_id = preview.launch_profile_id.clone();
    reservation.launch_profile_sha256 = preview.launch_profile_sha256.clone();
    receipt.request_id = request.request_id.clone();
    receipt.request_sha256 = request_sha256;
    if receipt.reservation_id.is_some() {
        receipt.reservation_id = Some(reservation.reservation_id.clone());
        receipt.reservation_sha256 = Some(remote_session_reservation_sha256(reservation)?);
    }
    if receipt.remote_session_identity.is_some() {
        rebind_live_identity(receipt)?;
    }
    Ok(())
}

#[test]
fn remote_session_contract_fixtures_are_canonical_and_hash_bound() -> Result<()> {
    let (policy, preview, request, reservation, receipt, hashes) = fixtures()?;
    let cleanup_receipt = cleanup_fixture()?;
    assert_eq!(hashes.schema, "remote_session_contract_hashes.v1");

    policy.validate()?;
    preview.validate()?;
    request.validate()?;
    reservation.validate()?;
    receipt.validate()?;
    cleanup_receipt.validate()?;
    validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )?;

    assert_eq!(
        serde_json::to_string(&policy)?,
        canonical_fixture(POLICY_JSON)
    );
    assert_eq!(
        serde_json::to_string(&preview)?,
        canonical_fixture(PREVIEW_JSON)
    );
    assert_eq!(
        serde_json::to_string(&request)?,
        canonical_fixture(REQUEST_JSON)
    );
    assert_eq!(
        serde_json::to_string(&reservation)?,
        canonical_fixture(RESERVATION_JSON)
    );
    assert_eq!(
        serde_json::to_string(&receipt)?,
        canonical_fixture(RECEIPT_JSON)
    );
    assert_eq!(
        serde_json::to_string(&cleanup_receipt)?,
        canonical_fixture(CLEANUP_RECEIPT_JSON)
    );

    assert_eq!(remote_session_policy_sha256(&policy)?, hashes.policy_sha256);
    assert_eq!(
        remote_session_launch_profile_sha256(&policy.launch_profiles[0])?,
        hashes.launch_profile_sha256
    );
    assert_eq!(
        remote_session_policy_harness_launch_sha256(&policy.launch_profiles[0])?,
        hashes.policy_harness_launch_sha256
    );
    assert_eq!(
        remote_session_policy_launch_command(&policy.launch_profiles[0])?,
        "'/usr/bin/codex' '--no-alt-screen'"
    );
    assert_eq!(
        remote_session_preview_sha256(&preview)?,
        hashes.preview_sha256
    );
    assert_eq!(
        remote_session_request_sha256(&request)?,
        hashes.request_sha256
    );
    assert_eq!(
        remote_session_reservation_sha256(&reservation)?,
        hashes.reservation_sha256
    );
    assert_eq!(
        remote_session_identity_sha256(
            receipt
                .remote_session_identity
                .as_ref()
                .expect("fixture session identity"),
        )?,
        hashes.remote_session_identity_sha256
    );
    assert_eq!(
        remote_session_receipt_sha256(&receipt)?,
        hashes.receipt_sha256
    );
    assert_eq!(
        remote_session_cleanup_receipt_sha256(&cleanup_receipt)?,
        hashes.cleanup_receipt_sha256
    );
    Ok(())
}

#[test]
fn remote_session_contracts_reject_ambiguous_or_unsafe_inputs() -> Result<()> {
    let duplicate = POLICY_JSON.replacen(
        "\"schema\":",
        "\"schema\":\"remote_session_policy.v1\",\"schema\":",
        1,
    );
    assert!(serde_json::from_str::<RemoteSessionPolicyV1>(&duplicate).is_err());

    let unknown_enum = POLICY_JSON.replacen(
        "\"harness\":\"codex\"",
        "\"harness\":\"unreviewed_harness\"",
        1,
    );
    assert!(serde_json::from_str::<RemoteSessionPolicyV1>(&unknown_enum).is_err());

    let unknown_field = POLICY_JSON.replacen(
        "\"enabled\":true,",
        "\"enabled\":true,\"allow_arbitrary_command\":true,",
        1,
    );
    assert!(serde_json::from_str::<RemoteSessionPolicyV1>(&unknown_field).is_err());

    let unsafe_argument = POLICY_JSON.replacen("--no-alt-screen", "unsafe\\u000aargument", 1);
    let unsafe_policy: RemoteSessionPolicyV1 = serde_json::from_str(&unsafe_argument)?;
    assert!(unsafe_policy.validate().is_err());

    let secret_policy_id = POLICY_JSON.replacen(
        "policy_forager_local",
        "ghp_abcdefghijklmnopqrstuvwxyz123456",
        1,
    );
    let secret_policy: RemoteSessionPolicyV1 = serde_json::from_str(&secret_policy_id)?;
    assert!(secret_policy.validate().is_err());

    let noncanonical_time =
        PREVIEW_JSON.replacen("2026-09-01T00:00:00Z", "2026-09-01T00:00:00+00:00", 1);
    assert!(serde_json::from_str::<RemoteSessionLaunchPreviewV1>(&noncanonical_time).is_err());

    let (_, mut preview, mut request, reservation, receipt, _) = fixtures()?;
    preview.operator_identity_sha256 = preview.operator_identity_sha256.to_uppercase();
    assert!(preview.validate().is_err());

    request.preview.root_id = "different_root".to_string();
    assert!(request.validate().is_err());

    let mut bad_receipt = receipt.clone();
    bad_receipt.remote_session_identity_sha256 = Some("0".repeat(64));
    assert!(bad_receipt.validate().is_err());

    let mut before = reservation.clone();
    before.sequence = 3;
    before.state = RemoteSessionReservationStateV1::Reserved;
    before.updated_at = before.created_at.clone();
    assert!(validate_remote_session_reservation_transition(&before, &reservation).is_err());

    let mut invalid_capacity = request.preview.capacity_observation.clone();
    invalid_capacity.active_for_root = 1;
    invalid_capacity.capacity_available = false;
    assert!(invalid_capacity.validate().is_err());
    Ok(())
}

#[test]
fn remote_session_contract_chain_rejects_policy_or_request_drift() -> Result<()> {
    let (mut policy, preview, request, reservation, receipt, _) = fixtures()?;
    policy.allowed_roots[0].canonical_path = "/workspace/replaced-forager".to_string();
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());

    let (policy, preview, request, reservation, mut receipt, _) = fixtures()?;
    receipt.request_id = "request_remote_other".to_string();
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());
    Ok(())
}

#[test]
fn remote_session_chain_rejects_expired_reservations_and_policy_ttl_drift() -> Result<()> {
    let (policy, mut preview, mut request, mut reservation, mut receipt, _) = fixtures()?;
    preview.expires_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:06:00Z")?;
    rebind_contract_chain(
        &policy,
        &mut preview,
        &mut request,
        &mut reservation,
        &mut receipt,
    )?;
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());

    let (policy, mut preview, mut request, mut reservation, mut receipt, _) = fixtures()?;
    reservation.created_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:05:01Z")?;
    reservation.updated_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:05:02Z")?;
    reservation.capacity_observation.observed_at = reservation.created_at.clone();
    receipt.created_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:05:03Z")?;
    rebind_contract_chain(
        &policy,
        &mut preview,
        &mut request,
        &mut reservation,
        &mut receipt,
    )?;
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());

    let (policy, mut preview, mut request, mut reservation, mut receipt, _) = fixtures()?;
    reservation.expires_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:10:01Z")?;
    rebind_contract_chain(
        &policy,
        &mut preview,
        &mut request,
        &mut reservation,
        &mut receipt,
    )?;
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());

    let (policy, mut preview, mut request, mut reservation, mut receipt, _) = fixtures()?;
    receipt.created_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:10:01Z")?;
    rebind_contract_chain(
        &policy,
        &mut preview,
        &mut request,
        &mut reservation,
        &mut receipt,
    )?;
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());
    Ok(())
}

#[test]
fn resumed_remote_session_binds_the_exact_preview_session() -> Result<()> {
    let (policy, mut preview, mut request, mut reservation, mut receipt, _) = fixtures()?;
    preview.resolved_disposition = forager::offdesk::RemoteSessionResolvedDispositionV1::Resume;
    preview.matching_session_id = Some("session_remote_other".to_string());
    preview.matching_worktree_identity_sha256 = reservation.worktree_identity_sha256.clone();
    receipt.result = RemoteSessionLaunchResultV1::Resumed;
    rebind_contract_chain(
        &policy,
        &mut preview,
        &mut request,
        &mut reservation,
        &mut receipt,
    )?;
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());

    reservation.state = RemoteSessionReservationStateV1::RecoveryRequired;
    receipt.result = RemoteSessionLaunchResultV1::RecoveryRequired;
    receipt.blocking_reasons = vec!["runtime_identity_conflict".to_string()];
    rebind_downstream_contract(&preview, &mut request, &mut reservation, &mut receipt)?;
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());

    preview.matching_session_id = Some(reservation.intended_session_id.clone());
    reservation.state = RemoteSessionReservationStateV1::Completed;
    receipt.result = RemoteSessionLaunchResultV1::Resumed;
    receipt.blocking_reasons.clear();
    rebind_contract_chain(
        &policy,
        &mut preview,
        &mut request,
        &mut reservation,
        &mut receipt,
    )?;
    validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )?;
    Ok(())
}

#[test]
fn remote_session_chain_binds_success_and_recovery_live_identity() -> Result<()> {
    let (policy, mut preview, mut request, mut reservation, mut receipt, _) = fixtures()?;
    receipt
        .remote_session_identity
        .as_mut()
        .expect("fixture identity")
        .harness_launch_sha256 = "9".repeat(64);
    rebind_contract_chain(
        &policy,
        &mut preview,
        &mut request,
        &mut reservation,
        &mut receipt,
    )?;
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());

    let (policy, mut preview, mut request, mut reservation, mut receipt, _) = fixtures()?;
    reservation.state = RemoteSessionReservationStateV1::RecoveryRequired;
    receipt.result = RemoteSessionLaunchResultV1::RecoveryRequired;
    receipt.blocking_reasons = vec!["runtime_identity_conflict".to_string()];
    rebind_contract_chain(
        &policy,
        &mut preview,
        &mut request,
        &mut reservation,
        &mut receipt,
    )?;
    validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )?;

    receipt
        .remote_session_identity
        .as_mut()
        .expect("fixture identity")
        .machine_identity_sha256 = "9".repeat(64);
    rebind_live_identity(&mut receipt)?;
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());

    let (policy, mut preview, mut request, mut reservation, mut receipt, _) = fixtures()?;
    preview.harness_launch_sha256 = "9".repeat(64);
    receipt
        .remote_session_identity
        .as_mut()
        .expect("fixture identity")
        .harness_launch_sha256 = "9".repeat(64);
    rebind_downstream_contract(&preview, &mut request, &mut reservation, &mut receipt)?;
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());
    Ok(())
}

#[test]
fn failed_clean_requires_exact_typed_cleanup_receipt() -> Result<()> {
    let (policy, mut preview, mut request, mut reservation, mut receipt, _) = fixtures()?;
    reservation.state = RemoteSessionReservationStateV1::Released;
    reservation.release_origin = Some(RemoteSessionReservationStateV1::RecoveryRequired);
    receipt.created_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:00:12Z")?;
    receipt.result = RemoteSessionLaunchResultV1::FailedClean;
    receipt.session_id = None;
    receipt.remote_session_identity = None;
    receipt.remote_session_identity_sha256 = None;
    receipt.cleanup_receipt_id = Some("cleanup_remote_001".to_string());
    rebind_contract_chain(
        &policy,
        &mut preview,
        &mut request,
        &mut reservation,
        &mut receipt,
    )?;

    let cleanup = cleanup_fixture()?;
    assert_eq!(cleanup.schema, REMOTE_SESSION_CLEANUP_RECEIPT_SCHEMA);
    assert_eq!(
        cleanup.worktree_outcome,
        RemoteSessionCleanupWorktreeOutcomeV1::RemovedPristine
    );
    assert_eq!(
        cleanup.request_sha256,
        remote_session_request_sha256(&request)?
    );
    assert_eq!(
        cleanup.reservation_sha256,
        remote_session_reservation_sha256(&reservation)?
    );
    receipt.cleanup_receipt_sha256 = Some(remote_session_cleanup_receipt_sha256(&cleanup)?);
    validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        Some(&cleanup),
    )?;

    let mut self_referential = receipt.clone();
    self_referential.cleanup_receipt_id = Some(self_referential.receipt_id.clone());
    assert!(self_referential.validate().is_err());

    let mut arbitrary_hash = receipt.clone();
    arbitrary_hash.cleanup_receipt_sha256 = Some("8".repeat(64));
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &arbitrary_hash,
        None,
        Some(&cleanup),
    )
    .is_err());
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&reservation),
        &receipt,
        None,
        None,
    )
    .is_err());

    let mut post_use_reservation = reservation.clone();
    post_use_reservation.release_origin = Some(RemoteSessionReservationStateV1::Completed);
    let mut post_use_receipt = receipt.clone();
    post_use_receipt.reservation_sha256 =
        Some(remote_session_reservation_sha256(&post_use_reservation)?);
    let mut post_use_cleanup = cleanup.clone();
    post_use_cleanup.reservation_sha256 = remote_session_reservation_sha256(&post_use_reservation)?;
    post_use_receipt.cleanup_receipt_sha256 =
        Some(remote_session_cleanup_receipt_sha256(&post_use_cleanup)?);
    assert!(validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        Some(&post_use_reservation),
        &post_use_receipt,
        None,
        Some(&post_use_cleanup),
    )
    .is_err());
    Ok(())
}

#[test]
fn duplicate_remote_session_receipt_requires_exact_non_self_original() -> Result<()> {
    let (policy, preview, request, _reservation, original, _) = fixtures()?;
    let mut replay = original.clone();
    replay.receipt_id = "receipt_remote_replay".to_string();
    replay.created_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:00:12Z")?;
    replay.result = RemoteSessionLaunchResultV1::DuplicateReplay;
    replay.reservation_id = None;
    replay.reservation_sha256 = None;
    replay.session_id = None;
    replay.remote_session_identity = None;
    replay.remote_session_identity_sha256 = None;
    replay.original_receipt_id = Some(original.receipt_id.clone());
    replay.original_receipt_sha256 = Some(remote_session_receipt_sha256(&original)?);

    validate_remote_session_duplicate_replay(&original, &replay)?;
    validate_remote_session_contract_chain(
        &policy,
        &preview,
        &request,
        None,
        &replay,
        Some(&original),
        None,
    )?;

    let mut self_referential = replay.clone();
    self_referential.original_receipt_id = Some(self_referential.receipt_id.clone());
    assert!(self_referential.validate().is_err());
    assert!(validate_remote_session_contract_chain(
        &policy, &preview, &request, None, &replay, None, None,
    )
    .is_err());
    Ok(())
}

#[test]
fn remote_session_reservation_transition_graph_is_exact() -> Result<()> {
    use RemoteSessionReservationStateV1 as State;

    let (_, _, _, fixture, _, _) = fixtures()?;
    let states = [
        State::Reserved,
        State::SessionRowCommitted,
        State::TmuxStarted,
        State::IdentityBound,
        State::Completed,
        State::RecoveryRequired,
        State::Released,
    ];
    let allowed = [
        (State::Reserved, State::SessionRowCommitted),
        (State::Reserved, State::RecoveryRequired),
        (State::Reserved, State::Released),
        (State::SessionRowCommitted, State::TmuxStarted),
        (State::SessionRowCommitted, State::RecoveryRequired),
        (State::TmuxStarted, State::IdentityBound),
        (State::TmuxStarted, State::RecoveryRequired),
        (State::IdentityBound, State::Completed),
        (State::IdentityBound, State::RecoveryRequired),
        (State::Completed, State::Released),
        (State::RecoveryRequired, State::Released),
    ];

    for before_state in states {
        for after_state in states {
            let mut before = fixture.clone();
            before.sequence = 1;
            before.state = before_state;
            before.updated_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:00:07Z")?;
            let mut after = before.clone();
            after.sequence = 2;
            after.state = after_state;
            after.release_origin = (after_state == State::Released).then_some(before_state);
            after.updated_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:00:08Z")?;

            let accepted = validate_remote_session_reservation_transition(&before, &after).is_ok();
            assert_eq!(
                accepted,
                allowed.contains(&(before_state, after_state)),
                "unexpected transition result for {before_state:?} -> {after_state:?}"
            );
        }
    }

    let mut before = fixture.clone();
    before.sequence = 1;
    before.state = State::IdentityBound;
    before.updated_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:00:07Z")?;
    let mut changed_identity = before.clone();
    changed_identity.sequence = 2;
    changed_identity.state = State::Completed;
    changed_identity.updated_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:00:08Z")?;
    changed_identity.intended_session_id = "session_remote_replaced".to_string();
    assert!(validate_remote_session_reservation_transition(&before, &changed_identity).is_err());

    let mut before = fixture.clone();
    before.sequence = 1;
    before.state = State::Reserved;
    before.updated_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:00:07Z")?;
    before.worktree_identity_sha256 = None;
    let mut after = before.clone();
    after.sequence = 2;
    after.state = State::SessionRowCommitted;
    after.updated_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:00:08Z")?;
    after.worktree_identity_sha256 = Some("f".repeat(64));
    validate_remote_session_reservation_transition(&before, &after)?;

    let mut equal_time = after.clone();
    equal_time.sequence = 3;
    equal_time.state = State::TmuxStarted;
    assert!(validate_remote_session_reservation_transition(&after, &equal_time).is_err());

    let mut overflow = before.clone();
    overflow.sequence = u64::MAX;
    let mut overflow_after = overflow.clone();
    overflow_after.state = State::RecoveryRequired;
    overflow_after.updated_at = RemoteSessionUtcTimestamp::parse("2026-09-01T00:00:08Z")?;
    assert!(validate_remote_session_reservation_transition(&overflow, &overflow_after).is_err());
    Ok(())
}

fn forager_command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_forager"));
    command.env("HOME", home);
    command.env_remove("FORAGER_PROFILE");
    command.env_remove("AGENT_OF_EMPIRES_PROFILE");
    #[cfg(target_os = "linux")]
    command.env("XDG_CONFIG_HOME", home.join(".config"));
    command
}

fn app_dir(home: &Path) -> PathBuf {
    #[cfg(target_os = "linux")]
    {
        home.join(".config").join("forager")
    }
    #[cfg(not(target_os = "linux"))]
    {
        home.join(".forager")
    }
}

fn write_owner_only_policy(path: &Path) -> Result<()> {
    fs::write(path, POLICY_JSON)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn owner_only_policy_parent(base: &Path) -> Result<PathBuf> {
    let base = fs::canonicalize(base)?;
    let parent = base.join("policy-parent");
    fs::create_dir(&parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700))?;
    }
    Ok(parent)
}

fn inspect_policy(home: &Path, policy: &Path) -> Result<Output> {
    Ok(forager_command(home)
        .args([
            "-p",
            "untouched",
            "offdesk",
            "remote-session",
            "policy-inspect",
            "--policy",
        ])
        .arg(policy)
        .arg("--json")
        .output()?)
}

fn inspect_policy_human(home: &Path, policy: &Path) -> Result<Output> {
    Ok(forager_command(home)
        .args([
            "-p",
            "untouched",
            "offdesk",
            "remote-session",
            "policy-inspect",
            "--policy",
        ])
        .arg(policy)
        .output()?)
}

fn native_test_executable(success: bool) -> &'static Path {
    if success {
        Path::new("/usr/bin/true")
    } else {
        Path::new("/usr/bin/false")
    }
}

fn write_executable(path: &Path, source: &Path) -> Result<()> {
    fs::copy(source, path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn write_resolution_policy(policy: &Path, root: &Path, executable: &Path) -> Result<()> {
    let mut value: serde_json::Value = serde_json::from_str(POLICY_JSON)?;
    value["allowed_roots"][0]["canonical_path"] =
        serde_json::Value::String(root.to_string_lossy().into_owned());
    value["launch_profiles"][0]["executable_path"] =
        serde_json::Value::String(executable.to_string_lossy().into_owned());
    value["launch_profiles"][0]["fixed_argv"][0] =
        serde_json::Value::String(executable.to_string_lossy().into_owned());
    fs::write(policy, serde_json::to_vec(&value)?)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(policy, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn resolution_fixture(base: &Path) -> Result<(PathBuf, PathBuf, PathBuf)> {
    fs::create_dir_all(base)?;
    let base = fs::canonicalize(base)?;
    let installation = base.join("installation");
    let root = installation.join("projects").join("forager");
    let bin = installation.join("bin");
    fs::create_dir_all(&root)?;
    fs::create_dir(&bin)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for directory in [
            &installation,
            root.parent().expect("projects parent"),
            &root,
            &bin,
        ] {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
        }
    }
    let executable = bin.join("codex-fixed");
    write_executable(&executable, native_test_executable(true))?;
    let policy = owner_only_policy_parent(&base)?.join("remote-session-policy.json");
    write_resolution_policy(&policy, &root, &executable)?;
    Ok((policy, installation, executable))
}

fn resolve_policy(home: &Path, policy: &Path, installation_boundary: &Path) -> Result<Output> {
    Ok(forager_command(home)
        .args([
            "-p",
            "untouched",
            "offdesk",
            "remote-session",
            "policy-resolve",
            "--policy",
        ])
        .arg(policy)
        .arg("--installation-boundary")
        .arg(installation_boundary)
        .args([
            "--root-id",
            "forager",
            "--launch-profile-id",
            "codex_managed",
            "--json",
        ])
        .output()?)
}

fn inspect_policy_without_profile_with_binary(
    binary: &Path,
    home: &Path,
    policy: &Path,
) -> Result<Output> {
    let mut command = Command::new(binary);
    command.env("HOME", home);
    command.env_remove("FORAGER_PROFILE");
    command.env_remove("AGENT_OF_EMPIRES_PROFILE");
    #[cfg(target_os = "linux")]
    command.env("XDG_CONFIG_HOME", home.join(".config"));
    Ok(command
        .args(["offdesk", "remote-session", "policy-inspect", "--policy"])
        .arg(policy)
        .arg("--json")
        .output()?)
}

#[test]
fn remote_session_policy_inspect_is_migration_free_and_redacted() -> Result<()> {
    let temp = tempdir()?;
    let home = temp.path().join("empty-home");
    fs::create_dir(&home)?;
    let policy = owner_only_policy_parent(temp.path())?.join("remote-session-policy.json");
    write_owner_only_policy(&policy)?;
    let before = fs::read(&policy)?;

    let output = inspect_policy(&home, &policy)?;
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    let report: serde_json::Value = serde_json::from_str(&stdout)?;
    assert_eq!(report["schema"], "remote_session_policy_inspection.v1");
    assert_eq!(report["read_only"], true);
    assert_eq!(report["profile_state_read"], false);
    assert_eq!(report["root_resolution_authorized"], false);
    assert_eq!(report["executable_resolution_authorized"], false);
    assert_eq!(report["request_creation_authorized"], false);
    assert_eq!(report["launch_authorized"], false);
    for private_value in [
        "/workspace/forager",
        "/usr/bin/codex",
        "--no-alt-screen",
        "xterm-256color",
    ] {
        assert!(!stdout.contains(private_value), "leaked {private_value}");
    }

    let human = inspect_policy_human(&home, &policy)?;
    assert!(
        human.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&human.stderr)
    );
    let human_stdout = String::from_utf8(human.stdout)?;
    assert!(human_stdout.contains("Request creation or launch: not authorized"));
    for private_value in [
        "/workspace/forager",
        "/usr/bin/codex",
        "--no-alt-screen",
        "xterm-256color",
    ] {
        assert!(
            !human_stdout.contains(private_value),
            "human output leaked {private_value}"
        );
    }
    assert_eq!(fs::read(&policy)?, before);
    assert!(!app_dir(&home).exists());
    Ok(())
}

#[test]
fn remote_session_policy_inspect_does_not_read_default_profile_config() -> Result<()> {
    let temp = tempdir()?;
    let home = temp.path().join("home-with-hostile-config");
    fs::create_dir_all(app_dir(&home))?;
    let config = app_dir(&home).join("config.toml");
    let config_bytes = b"default_profile = \"../state-dependent\"\n";
    fs::write(&config, config_bytes)?;
    let policy = owner_only_policy_parent(temp.path())?.join("policy.json");
    write_owner_only_policy(&policy)?;

    for binary in [
        Path::new(env!("CARGO_BIN_EXE_forager")),
        Path::new(env!("CARGO_BIN_EXE_aoe")),
    ] {
        let output = inspect_policy_without_profile_with_binary(binary, &home, &policy)?;
        assert!(
            output.status.success(),
            "{} stderr: {}",
            binary.display(),
            String::from_utf8_lossy(&output.stderr)
        );
        let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        assert_eq!(report["profile_state_read"], false);
    }

    assert_eq!(fs::read(config)?, config_bytes);
    assert!(!app_dir(&home).join(".schema_version").exists());
    assert!(!app_dir(&home).join("profiles").exists());
    Ok(())
}

#[test]
fn remote_session_policy_inspect_rejects_secret_shaped_identifiers_without_leaking() -> Result<()> {
    let temp = tempdir()?;
    let home = temp.path().join("empty-home");
    fs::create_dir(&home)?;
    let policy = owner_only_policy_parent(temp.path())?.join("secret-policy.json");
    let secret = "ghp_abcdefghijklmnopqrstuvwxyz123456";
    let bytes = POLICY_JSON.replacen("policy_forager_local", secret, 1);
    fs::write(&policy, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&policy, fs::Permissions::from_mode(0o600))?;
    }

    for output in [
        inspect_policy(&home, &policy)?,
        inspect_policy_human(&home, &policy)?,
    ] {
        assert!(!output.status.success());
        let rendered = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!rendered.contains(secret));
    }
    assert!(!app_dir(&home).exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn remote_session_policy_inspect_rejects_symlink_hardlink_and_public_mode() -> Result<()> {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let temp = tempdir()?;
    let home = temp.path().join("empty-home");
    fs::create_dir(&home)?;
    let policy_parent = owner_only_policy_parent(temp.path())?;
    let policy = policy_parent.join("policy.json");
    write_owner_only_policy(&policy)?;

    let symlink_policy = policy_parent.join("policy-link.json");
    symlink(&policy, &symlink_policy)?;
    assert!(!inspect_policy(&home, &symlink_policy)?.status.success());

    let hardlink_policy = policy_parent.join("policy-hardlink.json");
    fs::hard_link(&policy, &hardlink_policy)?;
    assert!(!inspect_policy(&home, &policy)?.status.success());
    fs::remove_file(&hardlink_policy)?;

    fs::set_permissions(&policy, fs::Permissions::from_mode(0o644))?;
    assert!(!inspect_policy(&home, &policy)?.status.success());

    let writable_parent = temp.path().join("writable-parent");
    fs::create_dir(&writable_parent)?;
    fs::set_permissions(&writable_parent, fs::Permissions::from_mode(0o777))?;
    let policy = writable_parent.join("policy.json");
    write_owner_only_policy(&policy)?;
    assert!(!inspect_policy(&home, &policy)?.status.success());
    assert!(!app_dir(&home).exists());
    Ok(())
}

#[test]
fn remote_session_policy_validation_rejects_every_unsafe_resolution_shape() -> Result<()> {
    let (policy, _, _, _, _, _) = fixtures()?;

    let mut duplicate_root = policy.clone();
    duplicate_root
        .allowed_roots
        .push(policy.allowed_roots[0].clone());
    assert!(duplicate_root.validate().is_err());

    let mut duplicate_profile = policy.clone();
    duplicate_profile
        .launch_profiles
        .push(policy.launch_profiles[0].clone());
    assert!(duplicate_profile.validate().is_err());

    let mut unknown_profile = policy.clone();
    unknown_profile.allowed_roots[0].allowed_launch_profile_ids = vec!["missing".to_string()];
    assert!(unknown_profile.validate().is_err());

    let mut empty_argv = policy.clone();
    empty_argv.launch_profiles[0].fixed_argv.clear();
    assert!(empty_argv.validate().is_err());

    let mut relative_executable = policy.clone();
    relative_executable.launch_profiles[0].executable_path = "codex".to_string();
    relative_executable.launch_profiles[0].fixed_argv[0] = "codex".to_string();
    assert!(relative_executable.validate().is_err());

    let mut noncanonical_root = policy.clone();
    noncanonical_root.allowed_roots[0].canonical_path = "/workspace//forager".to_string();
    assert!(noncanonical_root.validate().is_err());

    let mut uncontrolled_environment = policy.clone();
    uncontrolled_environment.launch_profiles[0].fixed_environment[0].key = "LD_PRELOAD".to_string();
    assert!(uncontrolled_environment.validate().is_err());

    let mut zero_capacity = policy;
    zero_capacity.capacity.max_remote_active_global = 0;
    assert!(zero_capacity.validate().is_err());
    Ok(())
}

#[test]
fn remote_session_policy_resolve_is_read_only_exact_and_redacted() -> Result<()> {
    let temp = tempdir()?;
    let home = temp.path().join("empty-home");
    fs::create_dir(&home)?;
    let (policy, installation, executable) = resolution_fixture(temp.path())?;
    let root = installation.join("projects").join("forager");

    let output = resolve_policy(&home, &policy, &installation)?;
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    let report: serde_json::Value = serde_json::from_str(&stdout)?;
    assert_eq!(report["schema"], "remote_session_policy_resolution.v1");
    assert_eq!(report["read_only"], true);
    assert_eq!(report["profile_state_read"], false);
    assert_eq!(report["project_registry_read"], false);
    assert_eq!(report["exact_root_resolved"], true);
    assert_eq!(report["exact_executable_resolved"], true);
    assert_eq!(report["request_creation_authorized"], false);
    assert_eq!(report["launch_authorized"], false);
    assert_eq!(report["executable_symlink_hops"], 0);
    assert_eq!(report["executable_local_filesystem_required"], true);
    assert_eq!(report["executable_byte_budget"], 512 * 1024 * 1024);
    assert_eq!(report["executable_hash_budget_millis"], 30_000);
    #[cfg(target_os = "linux")]
    {
        assert_eq!(report["executable_runtime_loader_present"], true);
        assert!(report["executable_runtime_loader_identity_sha256"].is_string());
        assert!(report["executable_runtime_loader_content_sha256"].is_string());
        assert!(report["executable_runtime_loader_size_bytes"].is_number());
        assert_eq!(report["executable_chain_byte_budget"], 1024 * 1024 * 1024);
        assert_eq!(report["executable_chain_hash_budget_millis"], 60_000);
    }
    assert_eq!(report["argv_count"], 2);
    assert_eq!(report["environment_keys"], serde_json::json!(["TERM"]));
    for private_value in [
        root.to_string_lossy().as_ref(),
        executable.to_string_lossy().as_ref(),
        "--no-alt-screen",
        "xterm-256color",
    ] {
        assert!(!stdout.contains(private_value), "leaked {private_value}");
    }
    assert!(!app_dir(&home).exists());
    Ok(())
}

#[cfg(unix)]
#[test]
fn remote_session_policy_resolve_binds_allowed_executable_symlink_chain() -> Result<()> {
    use std::os::unix::fs::symlink;

    let temp = tempdir()?;
    let home = temp.path().join("empty-home");
    fs::create_dir(&home)?;
    let (policy, installation, executable) = resolution_fixture(temp.path())?;
    let alias = executable.with_file_name("codex-link");
    symlink(executable.file_name().expect("executable name"), &alias)?;
    let root = installation.join("projects").join("forager");
    write_resolution_policy(&policy, &root, &alias)?;

    let output = resolve_policy(&home, &policy, &installation)?;
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["executable_symlink_hops"], 1);
    assert_ne!(
        report["executable_policy_path_sha256"],
        report["executable_resolved_path_sha256"]
    );

    let resolved =
        resolve_remote_session_policy_target(&policy, &installation, "forager", "codex_managed")?;
    fs::remove_file(&alias)?;
    let replacement = executable.with_file_name("codex-replacement");
    write_executable(&replacement, native_test_executable(false))?;
    symlink(replacement.file_name().expect("replacement name"), &alias)?;
    assert!(resolved.ensure_active().is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn remote_session_policy_resolve_rejects_hardlinked_executable() -> Result<()> {
    let temp = tempdir()?;
    let (policy, installation, executable) = resolution_fixture(temp.path())?;
    fs::hard_link(&executable, executable.with_file_name("codex-hardlink"))?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
fn remote_session_policy_resolution_binds_runtime_loader_identity() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir()?;
    let (policy, installation, executable) = resolution_fixture(temp.path())?;
    let mut executable_bytes = fs::read(native_test_executable(true))?;
    let program_offset = u64::from_le_bytes(
        executable_bytes[32..40]
            .try_into()
            .expect("ELF program offset"),
    ) as usize;
    let program_size = u16::from_le_bytes(
        executable_bytes[54..56]
            .try_into()
            .expect("ELF program size"),
    ) as usize;
    let program_count = u16::from_le_bytes(
        executable_bytes[56..58]
            .try_into()
            .expect("ELF program count"),
    ) as usize;
    let interpreter_program = (0..program_count)
        .map(|index| program_offset + (index * program_size))
        .find(|program| {
            u32::from_le_bytes(
                executable_bytes[*program..*program + 4]
                    .try_into()
                    .expect("ELF program type"),
            ) == 3
        })
        .ok_or_else(|| anyhow::anyhow!("native fixture has no PT_INTERP"))?;
    let original_interpreter_offset = u64::from_le_bytes(
        executable_bytes[interpreter_program + 8..interpreter_program + 16]
            .try_into()
            .expect("ELF interpreter offset"),
    ) as usize;
    let original_interpreter_size = u64::from_le_bytes(
        executable_bytes[interpreter_program + 32..interpreter_program + 40]
            .try_into()
            .expect("ELF interpreter size"),
    ) as usize;
    let original_interpreter = std::str::from_utf8(
        &executable_bytes[original_interpreter_offset
            ..original_interpreter_offset + original_interpreter_size - 1],
    )?
    .to_string();

    let runtime_loader = installation.join("bin").join("runtime-loader");
    write_executable(&runtime_loader, Path::new(&original_interpreter))?;
    let runtime_loader_text = runtime_loader
        .to_str()
        .ok_or_else(|| anyhow::anyhow!("runtime loader path is not UTF-8"))?;
    let replacement_offset = executable_bytes.len() as u64;
    let replacement_size = (runtime_loader_text.len() + 1) as u64;
    executable_bytes.extend_from_slice(runtime_loader_text.as_bytes());
    executable_bytes.push(0);
    executable_bytes[interpreter_program + 8..interpreter_program + 16]
        .copy_from_slice(&replacement_offset.to_le_bytes());
    executable_bytes[interpreter_program + 32..interpreter_program + 40]
        .copy_from_slice(&replacement_size.to_le_bytes());
    executable_bytes[interpreter_program + 40..interpreter_program + 48]
        .copy_from_slice(&replacement_size.to_le_bytes());
    let prepared_executable = executable.with_file_name("codex-fixed-prepared");
    fs::write(&prepared_executable, executable_bytes)?;
    fs::set_permissions(&prepared_executable, fs::Permissions::from_mode(0o700))?;
    fs::rename(&prepared_executable, &executable)?;

    let mut status = None;
    for _ in 0..20 {
        match Command::new(&executable).status() {
            Ok(observed) => {
                status = Some(observed);
                break;
            }
            Err(error) if error.raw_os_error() == Some(nix::libc::ETXTBSY) => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err(error) => return Err(error.into()),
        }
    }
    assert!(status
        .ok_or_else(|| anyhow::anyhow!("prepared executable remained busy"))?
        .success());
    let resolved =
        resolve_remote_session_policy_target(&policy, &installation, "forager", "codex_managed")?;
    assert!(resolved.report().executable_runtime_loader_present);

    let previous_loader = runtime_loader.with_file_name("runtime-loader-old");
    fs::rename(&runtime_loader, &previous_loader)?;
    write_executable(&runtime_loader, Path::new(&original_interpreter))?;
    assert!(resolved.ensure_active().is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn remote_session_policy_resolve_rejects_unsafe_intermediate_and_executable_modes() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir()?;
    let (policy, installation, executable) = resolution_fixture(temp.path())?;
    let projects = installation.join("projects");
    let bin = installation.join("bin");

    fs::set_permissions(&projects, fs::Permissions::from_mode(0o777))?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());
    fs::set_permissions(&projects, fs::Permissions::from_mode(0o700))?;

    fs::set_permissions(&bin, fs::Permissions::from_mode(0o777))?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());
    fs::set_permissions(&bin, fs::Permissions::from_mode(0o700))?;

    fs::set_permissions(&executable, fs::Permissions::from_mode(0o401))?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());

    fs::set_permissions(&executable, fs::Permissions::from_mode(0o700))?;
    fs::write(&executable, b"#!/usr/bin/env node\n")?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());

    fs::write(&executable, b"\x7fELF")?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());

    #[cfg(target_os = "linux")]
    {
        write_executable(&executable, native_test_executable(true))?;
        let mut wrong_architecture = fs::read(&executable)?;
        wrong_architecture[18..20].copy_from_slice(&0u16.to_le_bytes());
        fs::write(&executable, wrong_architecture)?;
        assert!(resolve_remote_session_policy_target(
            &policy,
            &installation,
            "forager",
            "codex_managed"
        )
        .is_err());

        write_executable(&executable, native_test_executable(true))?;
        let mut invalid_program_table = fs::read(&executable)?;
        invalid_program_table[32..40].copy_from_slice(&u64::MAX.to_le_bytes());
        fs::write(&executable, invalid_program_table)?;
        assert!(resolve_remote_session_policy_target(
            &policy,
            &installation,
            "forager",
            "codex_managed"
        )
        .is_err());

        write_executable(&executable, native_test_executable(true))?;
        let mut invalid_load_segment = fs::read(&executable)?;
        let program_offset = u64::from_le_bytes(
            invalid_load_segment[32..40]
                .try_into()
                .expect("ELF program offset"),
        ) as usize;
        let program_size = u16::from_le_bytes(
            invalid_load_segment[54..56]
                .try_into()
                .expect("ELF program size"),
        ) as usize;
        let program_count = u16::from_le_bytes(
            invalid_load_segment[56..58]
                .try_into()
                .expect("ELF program count"),
        ) as usize;
        let executable_load = (0..program_count)
            .map(|index| program_offset + (index * program_size))
            .find(|program| {
                u32::from_le_bytes(
                    invalid_load_segment[*program..*program + 4]
                        .try_into()
                        .expect("ELF program type"),
                ) == 1
                    && u32::from_le_bytes(
                        invalid_load_segment[*program + 4..*program + 8]
                            .try_into()
                            .expect("ELF program flags"),
                    ) & 1
                        != 0
            })
            .ok_or_else(|| anyhow::anyhow!("native fixture has no executable PT_LOAD"))?;
        let memory_size = u64::from_le_bytes(
            invalid_load_segment[executable_load + 40..executable_load + 48]
                .try_into()
                .expect("ELF load memory size"),
        );
        invalid_load_segment[executable_load + 32..executable_load + 40]
            .copy_from_slice(&(memory_size + 1).to_le_bytes());
        fs::write(&executable, invalid_load_segment)?;
        assert!(resolve_remote_session_policy_target(
            &policy,
            &installation,
            "forager",
            "codex_managed"
        )
        .is_err());

        write_executable(&executable, native_test_executable(true))?;
        let mut invalid_alignment = fs::read(&executable)?;
        invalid_alignment[executable_load + 48..executable_load + 56]
            .copy_from_slice(&3u64.to_le_bytes());
        fs::write(&executable, invalid_alignment)?;
        assert!(resolve_remote_session_policy_target(
            &policy,
            &installation,
            "forager",
            "codex_managed"
        )
        .is_err());

        write_executable(&executable, native_test_executable(true))?;
        let mut missing_interpreter = fs::read(&executable)?;
        let program_offset = u64::from_le_bytes(
            missing_interpreter[32..40]
                .try_into()
                .expect("ELF program offset"),
        ) as usize;
        let program_size = u16::from_le_bytes(
            missing_interpreter[54..56]
                .try_into()
                .expect("ELF program size"),
        ) as usize;
        let program_count = u16::from_le_bytes(
            missing_interpreter[56..58]
                .try_into()
                .expect("ELF program count"),
        ) as usize;
        let mut interpreter_replaced = false;
        for index in 0..program_count {
            let program = program_offset + (index * program_size);
            let program_type = u32::from_le_bytes(
                missing_interpreter[program..program + 4]
                    .try_into()
                    .expect("ELF program type"),
            );
            if program_type != 3 {
                continue;
            }
            let interpreter_offset = u64::from_le_bytes(
                missing_interpreter[program + 8..program + 16]
                    .try_into()
                    .expect("ELF interpreter offset"),
            ) as usize;
            let interpreter_size = u64::from_le_bytes(
                missing_interpreter[program + 32..program + 40]
                    .try_into()
                    .expect("ELF interpreter size"),
            ) as usize;
            let interpreter =
                &mut missing_interpreter[interpreter_offset..interpreter_offset + interpreter_size];
            interpreter.fill(b'x');
            interpreter[0] = b'/';
            interpreter[interpreter_size - 1] = 0;
            interpreter_replaced = true;
            break;
        }
        assert!(interpreter_replaced, "native fixture has no PT_INTERP");
        fs::write(&executable, missing_interpreter)?;
        assert!(resolve_remote_session_policy_target(
            &policy,
            &installation,
            "forager",
            "codex_managed"
        )
        .is_err());
    }

    let oversized = (512 * 1024 * 1024) + 1;
    fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&executable)?
        .set_len(oversized)?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn remote_session_policy_resolve_rejects_umbrella_outside_symlink_and_non_directory_roots(
) -> Result<()> {
    use std::os::unix::fs::{symlink, PermissionsExt};

    let temp = tempdir()?;
    let (policy, installation, executable) = resolution_fixture(temp.path())?;

    let mut disabled: serde_json::Value = serde_json::from_slice(&fs::read(&policy)?)?;
    disabled["enabled"] = serde_json::Value::Bool(false);
    fs::write(&policy, serde_json::to_vec(&disabled)?)?;
    fs::set_permissions(&policy, fs::Permissions::from_mode(0o600))?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());

    write_resolution_policy(&policy, &installation, &executable)?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());

    let outside = temp.path().join("outside");
    fs::create_dir(&outside)?;
    fs::set_permissions(&outside, fs::Permissions::from_mode(0o700))?;
    write_resolution_policy(&policy, &outside, &executable)?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());

    let root = installation.join("projects").join("forager");
    let root_link = installation.join("projects").join("forager-link");
    symlink(&root, &root_link)?;
    write_resolution_policy(&policy, &root_link, &executable)?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());

    let root_file = installation.join("projects").join("not-a-directory");
    fs::write(&root_file, b"not a directory")?;
    write_resolution_policy(&policy, &root_file, &executable)?;
    assert!(resolve_remote_session_policy_target(
        &policy,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());
    Ok(())
}

#[cfg(unix)]
#[test]
fn remote_session_policy_resolution_revalidates_policy_and_root_identity() -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let temp = tempdir()?;
    let (policy, installation, _) = resolution_fixture(temp.path())?;
    let root = installation.join("projects").join("forager");
    let resolved =
        resolve_remote_session_policy_target(&policy, &installation, "forager", "codex_managed")?;
    let old_root = installation.join("projects").join("forager-old");
    fs::rename(&root, &old_root)?;
    fs::create_dir(&root)?;
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700))?;
    assert!(resolved.ensure_active().is_err());

    let (policy, installation, _) = resolution_fixture(&temp.path().join("second"))?;
    let loaded = load_remote_session_policy_for_inspection(&policy)?;
    let original = fs::read(&policy)?;
    let replaced = policy.with_file_name("policy-old.json");
    fs::rename(&policy, &replaced)?;
    fs::write(&policy, original)?;
    fs::set_permissions(&policy, fs::Permissions::from_mode(0o600))?;
    assert!(resolve_loaded_remote_session_policy_target(
        loaded,
        &installation,
        "forager",
        "codex_managed"
    )
    .is_err());

    let third = temp.path().join("third");
    let (policy, installation, executable) = resolution_fixture(&third)?;
    let resolved =
        resolve_remote_session_policy_target(&policy, &installation, "forager", "codex_managed")?;
    let bin = executable.parent().expect("executable parent");
    let old_bin = installation.join("bin-old");
    fs::rename(bin, &old_bin)?;
    fs::create_dir(bin)?;
    fs::set_permissions(bin, fs::Permissions::from_mode(0o700))?;
    write_executable(&executable, native_test_executable(true))?;
    assert!(resolved.ensure_active().is_err());
    Ok(())
}

#[test]
fn remote_session_policy_inspect_rejects_oversized_source() -> Result<()> {
    let temp = tempdir()?;
    let home = temp.path().join("empty-home");
    fs::create_dir(&home)?;
    let policy = owner_only_policy_parent(temp.path())?.join("oversized-policy.json");
    fs::write(&policy, vec![b' '; (64 * 1024) + 1])?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&policy, fs::Permissions::from_mode(0o600))?;
    }
    assert!(!inspect_policy(&home, &policy)?.status.success());
    assert!(!app_dir(&home).exists());
    Ok(())
}
