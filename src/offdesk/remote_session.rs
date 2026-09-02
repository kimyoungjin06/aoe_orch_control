//! Strict contracts for approval-gated remote interactive-session creation.
//!
//! This module is intentionally mutation-free. S2-A freezes policy-derived
//! previews, confirmed requests, launch reservations, and terminal receipts.
//! S2-B adds descriptor-bound policy, root, and executable resolution. Session
//! creation, tmux launch, and cleanup belong to later reviewed slices.

use anyhow::{bail, ensure, Context, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::collections::{BTreeSet, VecDeque};
use std::ffi::OsString;
use std::fmt;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant};

use crate::session::{RemoteSessionIdentityV1, RemoteSessionWorktreeKindV1};

pub const REMOTE_SESSION_POLICY_SCHEMA: &str = "remote_session_policy.v1";
pub const REMOTE_SESSION_POLICY_INSPECTION_SCHEMA: &str = "remote_session_policy_inspection.v1";
pub const REMOTE_SESSION_POLICY_RESOLUTION_SCHEMA: &str = "remote_session_policy_resolution.v1";
pub const REMOTE_SESSION_LAUNCH_PREVIEW_SCHEMA: &str = "remote_session_launch_preview.v1";
pub const REMOTE_SESSION_LAUNCH_CONFIRMATION_SCHEMA: &str = "remote_session_launch_confirmation.v1";
pub const REMOTE_SESSION_LAUNCH_REQUEST_SCHEMA: &str = "remote_session_launch_request.v1";
pub const REMOTE_SESSION_LAUNCH_RESERVATION_SCHEMA: &str = "remote_session_launch_reservation.v1";
pub const REMOTE_SESSION_LAUNCH_RECEIPT_SCHEMA: &str = "remote_session_launch_receipt.v1";
pub const REMOTE_SESSION_CLEANUP_RECEIPT_SCHEMA: &str = "remote_session_cleanup_receipt.v1";

pub const REMOTE_SESSION_POLICY_MAX_BYTES: u64 = 64 * 1024;
const REMOTE_SESSION_EXECUTABLE_MAX_BYTES: u64 = 512 * 1024 * 1024;
const REMOTE_SESSION_EXECUTABLE_HASH_MAX_MILLIS: u64 = 30_000;
const MAX_EXECUTABLE_SYMLINK_HOPS: usize = 16;
const MAX_EXECUTABLE_PATH_COMPONENTS: usize = 256;
const MAX_ID_CHARS: usize = 128;
const MAX_PATH_CHARS: usize = 4096;
const MAX_ARG_CHARS: usize = 4096;
const MAX_ARG_COUNT: usize = 64;
const MAX_ENVIRONMENT_ENTRIES: usize = 32;
const MAX_ENVIRONMENT_VALUE_CHARS: usize = 4096;
const MAX_BLOCKING_REASONS: usize = 16;
const MAX_POLICY_ROOTS: usize = 64;
const MAX_LAUNCH_PROFILES: usize = 64;
const MIN_REQUEST_TTL_SECONDS: u32 = 30;
const MAX_REQUEST_TTL_SECONDS: u32 = 3600;
const MAX_REMOTE_CAPACITY: u16 = 64;

const POLICY_HASH_DOMAIN: &[u8] = b"forager.remote-session.policy.v1\0";
const LAUNCH_PROFILE_HASH_DOMAIN: &[u8] = b"forager.remote-session.launch-profile.v1\0";
const POLICY_HARNESS_LAUNCH_HASH_DOMAIN: &[u8] =
    b"forager.remote-session.policy-harness-launch.v1\0";
const PREVIEW_HASH_DOMAIN: &[u8] = b"forager.remote-session.preview.v1\0";
const REQUEST_HASH_DOMAIN: &[u8] = b"forager.remote-session.request.v1\0";
const RESERVATION_HASH_DOMAIN: &[u8] = b"forager.remote-session.reservation.v1\0";
const RECEIPT_HASH_DOMAIN: &[u8] = b"forager.remote-session.receipt.v1\0";
const CLEANUP_RECEIPT_HASH_DOMAIN: &[u8] = b"forager.remote-session.cleanup-receipt.v1\0";
const SESSION_IDENTITY_HASH_DOMAIN: &[u8] = b"forager.remote-session.identity-reference.v1\0";
const POLICY_PATH_HASH_DOMAIN: &[u8] = b"forager.remote-session.policy-path.v1\0";
const PROJECT_ROOT_HASH_DOMAIN: &[u8] = b"forager.remote-session.project-root.v1\0";
const EXECUTABLE_IDENTITY_HASH_DOMAIN: &[u8] = b"forager.remote-session.executable-identity.v1\0";
const EXECUTABLE_SYMLINK_CHAIN_HASH_DOMAIN: &[u8] =
    b"forager.remote-session.executable-symlink-chain.v1\0";
const EXECUTABLE_DIRECTORY_CHAIN_HASH_DOMAIN: &[u8] =
    b"forager.remote-session.executable-directory-chain.v1\0";

const ALLOWED_ENVIRONMENT_KEYS: &[&str] = &[
    "CLAUDE_CONFIG_DIR",
    "CODEX_HOME",
    "COLORTERM",
    "NO_COLOR",
    "TERM",
];

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RemoteSessionUtcTimestamp(DateTime<Utc>);

impl RemoteSessionUtcTimestamp {
    pub fn parse(value: &str) -> Result<Self> {
        let parsed = DateTime::parse_from_rfc3339(value)
            .with_context(|| format!("invalid remote session UTC timestamp: {value}"))?
            .with_timezone(&Utc);
        let canonical = parsed.to_rfc3339_opts(SecondsFormat::AutoSi, true);
        ensure!(
            value == canonical,
            "remote session timestamp must use canonical UTC precision with a Z suffix"
        );
        Ok(Self(parsed))
    }

    pub fn from_datetime(value: DateTime<Utc>) -> Self {
        let canonical = value.to_rfc3339_opts(SecondsFormat::AutoSi, true);
        Self::parse(&canonical).expect("UTC DateTime has a canonical representation")
    }

    pub fn as_datetime(&self) -> &DateTime<Utc> {
        &self.0
    }

    pub fn as_str(&self) -> String {
        self.0.to_rfc3339_opts(SecondsFormat::AutoSi, true)
    }
}

impl Serialize for RemoteSessionUtcTimestamp {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_str())
    }
}

impl<'de> Deserialize<'de> for RemoteSessionUtcTimestamp {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::parse(&value).map_err(D::Error::custom)
    }
}

impl fmt::Display for RemoteSessionUtcTimestamp {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSessionKindV1 {
    Agent,
    Orchestrator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSessionHarnessV1 {
    Claude,
    Codex,
    Gemini,
    Opencode,
    Generic,
}

impl RemoteSessionHarnessV1 {
    fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
            Self::Gemini => "gemini",
            Self::Opencode => "opencode",
            Self::Generic => "generic",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSessionWorktreePolicyV1 {
    ManagedWorktreeRequired,
    DirectExistingRoot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSessionHookPolicyV1 {
    PretrustedOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSessionRequestedDispositionV1 {
    Auto,
    New,
    Resume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSessionResolvedDispositionV1 {
    New,
    Resume,
    AlreadyRunning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSessionReservationStateV1 {
    Reserved,
    SessionRowCommitted,
    TmuxStarted,
    IdentityBound,
    Completed,
    RecoveryRequired,
    Released,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSessionLaunchResultV1 {
    Created,
    Resumed,
    AlreadyRunning,
    DuplicateReplay,
    HeldCapacity,
    HeldPause,
    BlockedPolicy,
    BlockedStaleRoot,
    BlockedHookTrust,
    BlockedConflict,
    RecoveryRequired,
    FailedClean,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSessionCleanupWorktreeOutcomeV1 {
    NotCreated,
    RemovedPristine,
    PreservedExistingWorktree,
    PreservedDirectRoot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionEnvironmentEntryV1 {
    pub key: String,
    pub value: String,
}

impl RemoteSessionEnvironmentEntryV1 {
    fn validate(&self) -> Result<()> {
        ensure!(
            ALLOWED_ENVIRONMENT_KEYS.contains(&self.key.as_str()),
            "remote session environment key is not allowlisted"
        );
        validate_safe_text(
            &self.value,
            MAX_ENVIRONMENT_VALUE_CHARS,
            "remote session environment value",
            true,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionAllowedRootV1 {
    pub root_id: String,
    pub canonical_path: String,
    pub project_key: String,
    pub allowed_launch_profile_ids: Vec<String>,
}

impl RemoteSessionAllowedRootV1 {
    fn validate(&self) -> Result<()> {
        validate_identifier(&self.root_id, "remote session root_id")?;
        validate_identifier(&self.project_key, "remote session project_key")?;
        validate_absolute_path(&self.canonical_path, "remote session canonical root")?;
        ensure!(
            !self.allowed_launch_profile_ids.is_empty(),
            "remote session root must allow at least one launch profile"
        );
        ensure!(
            self.allowed_launch_profile_ids.len() <= MAX_LAUNCH_PROFILES,
            "remote session root exceeds its launch profile budget"
        );
        validate_unique_identifiers(
            &self.allowed_launch_profile_ids,
            "remote session root launch profile",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionLaunchProfileV1 {
    pub launch_profile_id: String,
    pub session_kind: RemoteSessionKindV1,
    pub harness: RemoteSessionHarnessV1,
    pub executable_path: String,
    pub fixed_argv: Vec<String>,
    pub fixed_environment: Vec<RemoteSessionEnvironmentEntryV1>,
    pub yolo_mode: bool,
    pub automatic_orchestrator: bool,
    pub worktree_policy: RemoteSessionWorktreePolicyV1,
    pub hook_policy: RemoteSessionHookPolicyV1,
}

impl RemoteSessionLaunchProfileV1 {
    pub fn validate(&self) -> Result<()> {
        validate_identifier(&self.launch_profile_id, "remote session launch_profile_id")?;
        validate_absolute_path(&self.executable_path, "remote session executable")?;
        ensure!(
            !self.fixed_argv.is_empty() && self.fixed_argv.len() <= MAX_ARG_COUNT,
            "remote session fixed argv count is invalid"
        );
        for argument in &self.fixed_argv {
            validate_safe_text(
                argument,
                MAX_ARG_CHARS,
                "remote session fixed argument",
                false,
            )?;
        }
        ensure!(
            self.fixed_argv[0] == self.executable_path,
            "remote session argv[0] must equal executable_path"
        );
        ensure!(
            self.fixed_environment.len() <= MAX_ENVIRONMENT_ENTRIES,
            "remote session fixed environment exceeds its entry budget"
        );
        let mut environment_keys = BTreeSet::new();
        for entry in &self.fixed_environment {
            entry.validate()?;
            ensure!(
                environment_keys.insert(entry.key.as_str()),
                "remote session fixed environment contains duplicate keys"
            );
        }
        ensure!(
            !self.automatic_orchestrator,
            "remote session v1 forbids implicit automatic Orchestrator children"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionCapacityPolicyV1 {
    pub max_remote_active_global: u16,
    pub max_remote_active_per_root: u16,
}

impl RemoteSessionCapacityPolicyV1 {
    fn validate(&self) -> Result<()> {
        ensure!(
            (1..=MAX_REMOTE_CAPACITY).contains(&self.max_remote_active_global),
            "remote session global capacity is invalid"
        );
        ensure!(
            (1..=self.max_remote_active_global).contains(&self.max_remote_active_per_root),
            "remote session per-root capacity is invalid"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionPolicyV1 {
    pub schema: String,
    pub enabled: bool,
    pub policy_id: String,
    pub allowed_roots: Vec<RemoteSessionAllowedRootV1>,
    pub launch_profiles: Vec<RemoteSessionLaunchProfileV1>,
    pub capacity: RemoteSessionCapacityPolicyV1,
    pub request_ttl_seconds: u32,
}

impl RemoteSessionPolicyV1 {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == REMOTE_SESSION_POLICY_SCHEMA,
            "remote session policy schema is invalid"
        );
        validate_identifier(&self.policy_id, "remote session policy_id")?;
        self.capacity.validate()?;
        ensure!(
            (MIN_REQUEST_TTL_SECONDS..=MAX_REQUEST_TTL_SECONDS).contains(&self.request_ttl_seconds),
            "remote session request TTL is outside the allowed range"
        );
        if self.enabled {
            ensure!(
                !self.allowed_roots.is_empty() && !self.launch_profiles.is_empty(),
                "enabled remote session policy requires roots and launch profiles"
            );
        }
        ensure!(
            self.allowed_roots.len() <= MAX_POLICY_ROOTS
                && self.launch_profiles.len() <= MAX_LAUNCH_PROFILES,
            "remote session policy exceeds its collection budget"
        );

        let mut root_ids = BTreeSet::new();
        let mut root_paths = BTreeSet::new();
        for root in &self.allowed_roots {
            root.validate()?;
            ensure!(
                root_ids.insert(root.root_id.as_str()),
                "remote session policy contains duplicate root IDs"
            );
            ensure!(
                root_paths.insert(root.canonical_path.as_str()),
                "remote session policy contains duplicate canonical roots"
            );
        }

        let mut launch_profile_ids = BTreeSet::new();
        for profile in &self.launch_profiles {
            profile.validate()?;
            ensure!(
                launch_profile_ids.insert(profile.launch_profile_id.as_str()),
                "remote session policy contains duplicate launch profile IDs"
            );
        }
        for root in &self.allowed_roots {
            for profile_id in &root.allowed_launch_profile_ids {
                ensure!(
                    launch_profile_ids.contains(profile_id.as_str()),
                    "remote session root references an unknown launch profile"
                );
            }
        }
        Ok(())
    }

    pub fn allowed_root(&self, root_id: &str) -> Result<&RemoteSessionAllowedRootV1> {
        unique_item(
            self.allowed_roots
                .iter()
                .filter(|root| root.root_id == root_id),
            "remote session root_id does not resolve exactly once",
        )
    }

    pub fn launch_profile(&self, profile_id: &str) -> Result<&RemoteSessionLaunchProfileV1> {
        unique_item(
            self.launch_profiles
                .iter()
                .filter(|profile| profile.launch_profile_id == profile_id),
            "remote session launch_profile_id does not resolve exactly once",
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionCapacityObservationV1 {
    pub observed_at: RemoteSessionUtcTimestamp,
    pub max_remote_active_global: u16,
    pub max_remote_active_per_root: u16,
    pub active_global: u16,
    pub active_for_root: u16,
    pub reserved_global: u16,
    pub reserved_for_root: u16,
    pub capacity_available: bool,
}

impl RemoteSessionCapacityObservationV1 {
    pub fn validate(&self) -> Result<()> {
        let active_and_reserved_global = self
            .active_global
            .checked_add(self.reserved_global)
            .context("remote session global capacity observation overflow")?;
        let active_and_reserved_root = self
            .active_for_root
            .checked_add(self.reserved_for_root)
            .context("remote session root capacity observation overflow")?;
        ensure!(
            self.max_remote_active_global > 0
                && self.max_remote_active_per_root > 0
                && self.max_remote_active_global <= MAX_REMOTE_CAPACITY
                && self.max_remote_active_per_root <= self.max_remote_active_global
                && self.active_for_root <= self.active_global
                && self.reserved_for_root <= self.reserved_global,
            "remote session capacity observation has invalid limits"
        );
        let expected_available = active_and_reserved_global < self.max_remote_active_global
            && active_and_reserved_root < self.max_remote_active_per_root;
        ensure!(
            self.capacity_available == expected_available,
            "remote session capacity availability contradicts its counts"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionLaunchPreviewV1 {
    pub schema: String,
    pub preview_id: String,
    pub created_at: RemoteSessionUtcTimestamp,
    pub expires_at: RemoteSessionUtcTimestamp,
    pub profile: String,
    pub operator_identity_sha256: String,
    pub machine_identity_sha256: String,
    pub control_generation_sha256: String,
    pub policy_id: String,
    pub policy_sha256: String,
    pub root_id: String,
    pub project_key: String,
    pub initial_project_root_identity_sha256: String,
    pub launch_profile_id: String,
    pub launch_profile_sha256: String,
    pub session_kind: RemoteSessionKindV1,
    pub harness: RemoteSessionHarnessV1,
    pub harness_launch_sha256: String,
    pub yolo_mode: bool,
    pub worktree_policy: RemoteSessionWorktreePolicyV1,
    pub requested_disposition: RemoteSessionRequestedDispositionV1,
    pub resolved_disposition: RemoteSessionResolvedDispositionV1,
    pub matching_session_id: Option<String>,
    pub matching_worktree_identity_sha256: Option<String>,
    pub capacity_observation: RemoteSessionCapacityObservationV1,
    pub observed_state_sha256: String,
}

impl RemoteSessionLaunchPreviewV1 {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == REMOTE_SESSION_LAUNCH_PREVIEW_SCHEMA,
            "remote session preview schema is invalid"
        );
        for (label, value) in [
            ("preview_id", self.preview_id.as_str()),
            ("profile", self.profile.as_str()),
            ("policy_id", self.policy_id.as_str()),
            ("root_id", self.root_id.as_str()),
            ("project_key", self.project_key.as_str()),
            ("launch_profile_id", self.launch_profile_id.as_str()),
        ] {
            validate_identifier(value, label)?;
        }
        for (label, value) in [
            ("operator_identity_sha256", &self.operator_identity_sha256),
            ("machine_identity_sha256", &self.machine_identity_sha256),
            ("control_generation_sha256", &self.control_generation_sha256),
            ("policy_sha256", &self.policy_sha256),
            (
                "initial_project_root_identity_sha256",
                &self.initial_project_root_identity_sha256,
            ),
            ("launch_profile_sha256", &self.launch_profile_sha256),
            ("harness_launch_sha256", &self.harness_launch_sha256),
            ("observed_state_sha256", &self.observed_state_sha256),
        ] {
            validate_sha256(value, label)?;
        }
        ensure!(
            self.created_at.as_datetime() < self.expires_at.as_datetime(),
            "remote session preview must expire after creation"
        );
        self.capacity_observation.validate()?;
        ensure!(
            self.capacity_observation.observed_at.as_datetime() >= self.created_at.as_datetime()
                && self.capacity_observation.observed_at.as_datetime()
                    <= self.expires_at.as_datetime(),
            "remote session capacity observation is outside the preview window"
        );
        validate_disposition(
            self.requested_disposition,
            self.resolved_disposition,
            self.matching_session_id.as_deref(),
            self.matching_worktree_identity_sha256.as_deref(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionLaunchConfirmationV1 {
    pub schema: String,
    pub confirmation_id: String,
    pub confirmed_at: RemoteSessionUtcTimestamp,
    pub preview_id: String,
    pub preview_sha256: String,
    pub operator_identity_sha256: String,
    pub control_generation_sha256: String,
    pub observed_state_sha256: String,
}

impl RemoteSessionLaunchConfirmationV1 {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == REMOTE_SESSION_LAUNCH_CONFIRMATION_SCHEMA,
            "remote session confirmation schema is invalid"
        );
        validate_identifier(&self.confirmation_id, "remote session confirmation_id")?;
        validate_identifier(&self.preview_id, "remote session confirmation preview_id")?;
        for (label, value) in [
            ("preview_sha256", &self.preview_sha256),
            ("operator_identity_sha256", &self.operator_identity_sha256),
            ("control_generation_sha256", &self.control_generation_sha256),
            ("observed_state_sha256", &self.observed_state_sha256),
        ] {
            validate_sha256(value, label)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionLaunchRequestV1 {
    pub schema: String,
    pub request_id: String,
    pub application_id: String,
    pub idempotency_key_sha256: String,
    pub created_at: RemoteSessionUtcTimestamp,
    pub expires_at: RemoteSessionUtcTimestamp,
    pub preview: RemoteSessionLaunchPreviewV1,
    pub confirmation: RemoteSessionLaunchConfirmationV1,
}

impl RemoteSessionLaunchRequestV1 {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == REMOTE_SESSION_LAUNCH_REQUEST_SCHEMA,
            "remote session request schema is invalid"
        );
        validate_identifier(&self.request_id, "remote session request_id")?;
        validate_identifier(&self.application_id, "remote session application_id")?;
        validate_sha256(
            &self.idempotency_key_sha256,
            "remote session idempotency_key_sha256",
        )?;
        self.preview.validate()?;
        self.confirmation.validate()?;
        ensure!(
            self.created_at.as_datetime() >= self.confirmation.confirmed_at.as_datetime()
                && self.created_at.as_datetime() < self.expires_at.as_datetime()
                && self.expires_at.as_datetime() <= self.preview.expires_at.as_datetime(),
            "remote session request timestamps are inconsistent"
        );
        ensure!(
            self.confirmation.confirmed_at.as_datetime() >= self.preview.created_at.as_datetime()
                && self.confirmation.confirmed_at.as_datetime()
                    <= self.preview.expires_at.as_datetime(),
            "remote session confirmation is outside the preview window"
        );
        ensure!(
            self.confirmation.preview_id == self.preview.preview_id
                && self.confirmation.preview_sha256
                    == remote_session_preview_sha256(&self.preview)?
                && self.confirmation.operator_identity_sha256
                    == self.preview.operator_identity_sha256
                && self.confirmation.control_generation_sha256
                    == self.preview.control_generation_sha256
                && self.confirmation.observed_state_sha256 == self.preview.observed_state_sha256,
            "remote session confirmation does not bind the exact preview"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionLaunchReservationV1 {
    pub schema: String,
    pub reservation_id: String,
    pub request_id: String,
    pub request_sha256: String,
    pub attempt_number: u32,
    pub sequence: u64,
    pub state: RemoteSessionReservationStateV1,
    pub release_origin: Option<RemoteSessionReservationStateV1>,
    pub created_at: RemoteSessionUtcTimestamp,
    pub updated_at: RemoteSessionUtcTimestamp,
    pub expires_at: RemoteSessionUtcTimestamp,
    pub policy_id: String,
    pub policy_sha256: String,
    pub root_id: String,
    pub project_key: String,
    pub project_root_identity_sha256: String,
    pub launch_profile_id: String,
    pub launch_profile_sha256: String,
    pub intended_session_id: String,
    pub intended_worktree_path: String,
    pub worktree_identity_sha256: Option<String>,
    pub intended_branch: Option<String>,
    pub worktree_policy: RemoteSessionWorktreePolicyV1,
    pub capacity_observation: RemoteSessionCapacityObservationV1,
}

impl RemoteSessionLaunchReservationV1 {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == REMOTE_SESSION_LAUNCH_RESERVATION_SCHEMA,
            "remote session reservation schema is invalid"
        );
        for (label, value) in [
            ("reservation_id", self.reservation_id.as_str()),
            ("request_id", self.request_id.as_str()),
            ("policy_id", self.policy_id.as_str()),
            ("root_id", self.root_id.as_str()),
            ("project_key", self.project_key.as_str()),
            ("launch_profile_id", self.launch_profile_id.as_str()),
            ("intended_session_id", self.intended_session_id.as_str()),
        ] {
            validate_identifier(value, label)?;
        }
        for (label, value) in [
            ("request_sha256", &self.request_sha256),
            ("policy_sha256", &self.policy_sha256),
            (
                "project_root_identity_sha256",
                &self.project_root_identity_sha256,
            ),
            ("launch_profile_sha256", &self.launch_profile_sha256),
        ] {
            validate_sha256(value, label)?;
        }
        if let Some(value) = &self.worktree_identity_sha256 {
            validate_sha256(value, "remote session worktree_identity_sha256")?;
        }
        if matches!(
            self.state,
            RemoteSessionReservationStateV1::SessionRowCommitted
                | RemoteSessionReservationStateV1::TmuxStarted
                | RemoteSessionReservationStateV1::IdentityBound
                | RemoteSessionReservationStateV1::Completed
        ) {
            ensure!(
                self.worktree_identity_sha256.is_some(),
                "remote session reservation state requires a bound worktree identity"
            );
        }
        match self.state {
            RemoteSessionReservationStateV1::Released => ensure!(
                matches!(
                    self.release_origin,
                    Some(
                        RemoteSessionReservationStateV1::Reserved
                            | RemoteSessionReservationStateV1::RecoveryRequired
                            | RemoteSessionReservationStateV1::Completed
                    )
                ),
                "released remote session reservation has no valid origin state"
            ),
            _ => ensure!(
                self.release_origin.is_none(),
                "non-released remote session reservation carries a release origin"
            ),
        }
        ensure!(
            self.attempt_number > 0 && self.sequence > 0,
            "remote session reservation counters must be positive"
        );
        ensure!(
            self.created_at.as_datetime() <= self.updated_at.as_datetime()
                && self.updated_at.as_datetime() < self.expires_at.as_datetime(),
            "remote session reservation timestamps are inconsistent"
        );
        self.capacity_observation.validate()?;
        ensure!(
            self.capacity_observation.capacity_available,
            "remote session reservation requires available observed capacity"
        );
        ensure!(
            self.capacity_observation.observed_at.as_datetime() >= self.created_at.as_datetime()
                && self.capacity_observation.observed_at.as_datetime()
                    <= self.updated_at.as_datetime(),
            "remote session capacity observation is outside the reservation window"
        );
        validate_absolute_path(
            &self.intended_worktree_path,
            "remote session intended worktree",
        )?;
        match self.worktree_policy {
            RemoteSessionWorktreePolicyV1::ManagedWorktreeRequired => {
                let branch = self
                    .intended_branch
                    .as_deref()
                    .context("managed remote session reservation is missing its branch")?;
                validate_safe_text(
                    branch,
                    MAX_ID_CHARS,
                    "remote session intended branch",
                    false,
                )?;
            }
            RemoteSessionWorktreePolicyV1::DirectExistingRoot => ensure!(
                self.intended_branch.is_none(),
                "direct-root remote session reservation cannot carry a branch"
            ),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionCleanupReceiptV1 {
    pub schema: String,
    pub cleanup_receipt_id: String,
    pub created_at: RemoteSessionUtcTimestamp,
    pub request_id: String,
    pub request_sha256: String,
    pub reservation_id: String,
    pub reservation_sha256: String,
    pub intended_session_id: String,
    pub session_row_absent: bool,
    pub transport_session_absent: bool,
    pub worktree_outcome: RemoteSessionCleanupWorktreeOutcomeV1,
    pub cleanup_observation_sha256: String,
}

impl RemoteSessionCleanupReceiptV1 {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == REMOTE_SESSION_CLEANUP_RECEIPT_SCHEMA,
            "remote session cleanup receipt schema is invalid"
        );
        for (label, value) in [
            ("cleanup_receipt_id", self.cleanup_receipt_id.as_str()),
            ("request_id", self.request_id.as_str()),
            ("reservation_id", self.reservation_id.as_str()),
            ("intended_session_id", self.intended_session_id.as_str()),
        ] {
            validate_identifier(value, label)?;
        }
        for (label, value) in [
            ("request_sha256", &self.request_sha256),
            ("reservation_sha256", &self.reservation_sha256),
            (
                "cleanup_observation_sha256",
                &self.cleanup_observation_sha256,
            ),
        ] {
            validate_sha256(value, label)?;
        }
        ensure!(
            self.session_row_absent && self.transport_session_absent,
            "remote session cleanup receipt does not prove an absent session row and transport"
        );
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteSessionLaunchReceiptV1 {
    pub schema: String,
    pub receipt_id: String,
    pub created_at: RemoteSessionUtcTimestamp,
    pub result: RemoteSessionLaunchResultV1,
    pub request_id: String,
    pub request_sha256: String,
    pub reservation_id: Option<String>,
    pub reservation_sha256: Option<String>,
    pub session_id: Option<String>,
    pub remote_session_identity: Option<RemoteSessionIdentityV1>,
    pub remote_session_identity_sha256: Option<String>,
    pub original_receipt_id: Option<String>,
    pub original_receipt_sha256: Option<String>,
    pub blocking_reasons: Vec<String>,
    pub cleanup_receipt_id: Option<String>,
    pub cleanup_receipt_sha256: Option<String>,
}

impl RemoteSessionLaunchReceiptV1 {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.schema == REMOTE_SESSION_LAUNCH_RECEIPT_SCHEMA,
            "remote session receipt schema is invalid"
        );
        validate_identifier(&self.receipt_id, "remote session receipt_id")?;
        validate_identifier(&self.request_id, "remote session receipt request_id")?;
        validate_sha256(
            &self.request_sha256,
            "remote session receipt request_sha256",
        )?;
        validate_optional_identifier(
            self.reservation_id.as_deref(),
            "remote session receipt reservation_id",
        )?;
        if let Some(value) = &self.reservation_sha256 {
            validate_sha256(value, "remote session receipt reservation_sha256")?;
        }
        validate_optional_identifier(
            self.session_id.as_deref(),
            "remote session receipt session_id",
        )?;
        validate_optional_identifier(
            self.original_receipt_id.as_deref(),
            "remote session original_receipt_id",
        )?;
        if let Some(value) = &self.original_receipt_sha256 {
            validate_sha256(value, "remote session original_receipt_sha256")?;
        }
        validate_optional_identifier(
            self.cleanup_receipt_id.as_deref(),
            "remote session cleanup_receipt_id",
        )?;
        if let Some(value) = &self.cleanup_receipt_sha256 {
            validate_sha256(value, "remote session cleanup_receipt_sha256")?;
        }
        ensure!(
            self.reservation_id.is_some() == self.reservation_sha256.is_some(),
            "remote session receipt reservation identity is partial"
        );
        ensure!(
            self.blocking_reasons.len() <= MAX_BLOCKING_REASONS,
            "remote session receipt has too many blocking reasons"
        );
        for reason in &self.blocking_reasons {
            validate_identifier(reason, "remote session blocking reason")?;
        }
        if let Some(value) = &self.remote_session_identity_sha256 {
            validate_sha256(value, "remote session identity reference SHA-256")?;
        }
        if let Some(identity) = &self.remote_session_identity {
            validate_canonical_remote_session_identity(identity)?;
            let identity_sha256 = remote_session_identity_sha256(identity)?;
            ensure!(
                self.remote_session_identity_sha256.as_deref() == Some(identity_sha256.as_str()),
                "remote session receipt identity reference hash is invalid"
            );
            ensure!(
                self.session_id.as_deref() == Some(identity.session_id.as_str()),
                "remote session receipt session ID does not match its identity"
            );
        }

        let has_session = self.session_id.is_some()
            && self.remote_session_identity.is_some()
            && self.remote_session_identity_sha256.is_some();
        ensure!(
            self.session_id.is_some() == self.remote_session_identity.is_some()
                && self.remote_session_identity.is_some()
                    == self.remote_session_identity_sha256.is_some(),
            "remote session receipt live identity is partial"
        );
        ensure!(
            self.original_receipt_id.is_some() == self.original_receipt_sha256.is_some(),
            "remote session original receipt identity is partial"
        );
        ensure!(
            self.cleanup_receipt_id.is_some() == self.cleanup_receipt_sha256.is_some(),
            "remote session cleanup receipt identity is partial"
        );
        ensure!(
            self.cleanup_receipt_id.as_deref() != Some(self.receipt_id.as_str()),
            "remote session receipt cannot reference itself as cleanup evidence"
        );
        match self.result {
            RemoteSessionLaunchResultV1::Created | RemoteSessionLaunchResultV1::Resumed => {
                ensure!(
                    self.reservation_id.is_some()
                        && has_session
                        && self.original_receipt_id.is_none()
                        && self.original_receipt_sha256.is_none()
                        && self.blocking_reasons.is_empty()
                        && self.cleanup_receipt_id.is_none(),
                    "successful remote session receipt has inconsistent evidence"
                );
            }
            RemoteSessionLaunchResultV1::AlreadyRunning => ensure!(
                self.reservation_id.is_none()
                    && has_session
                    && self.original_receipt_id.is_none()
                    && self.original_receipt_sha256.is_none()
                    && self.blocking_reasons.is_empty()
                    && self.cleanup_receipt_id.is_none(),
                "already-running remote session receipt has inconsistent evidence"
            ),
            RemoteSessionLaunchResultV1::DuplicateReplay => ensure!(
                self.reservation_id.is_none()
                    && !has_session
                    && self.original_receipt_id.is_some()
                    && self.original_receipt_sha256.is_some()
                    && self.original_receipt_id.as_deref() != Some(self.receipt_id.as_str())
                    && self.blocking_reasons.is_empty()
                    && self.cleanup_receipt_id.is_none(),
                "duplicate remote session receipt has inconsistent evidence"
            ),
            RemoteSessionLaunchResultV1::HeldCapacity
            | RemoteSessionLaunchResultV1::HeldPause
            | RemoteSessionLaunchResultV1::BlockedPolicy
            | RemoteSessionLaunchResultV1::BlockedStaleRoot
            | RemoteSessionLaunchResultV1::BlockedHookTrust
            | RemoteSessionLaunchResultV1::BlockedConflict => ensure!(
                self.reservation_id.is_none()
                    && !has_session
                    && self.original_receipt_id.is_none()
                    && self.original_receipt_sha256.is_none()
                    && !self.blocking_reasons.is_empty()
                    && self.cleanup_receipt_id.is_none(),
                "held or blocked remote session receipt has inconsistent evidence"
            ),
            RemoteSessionLaunchResultV1::RecoveryRequired => ensure!(
                self.reservation_id.is_some()
                    && self.original_receipt_id.is_none()
                    && self.original_receipt_sha256.is_none()
                    && !self.blocking_reasons.is_empty()
                    && self.cleanup_receipt_id.is_none(),
                "remote session recovery receipt has inconsistent evidence"
            ),
            RemoteSessionLaunchResultV1::FailedClean => ensure!(
                self.reservation_id.is_some()
                    && !has_session
                    && self.original_receipt_id.is_none()
                    && self.original_receipt_sha256.is_none()
                    && self.blocking_reasons.is_empty()
                    && self.cleanup_receipt_id.is_some()
                    && self.cleanup_receipt_sha256.is_some(),
                "clean remote session failure receipt has inconsistent evidence"
            ),
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RemoteSessionPolicyRootInspectionV1 {
    pub root_id: String,
    pub project_key: String,
    pub canonical_path_sha256: String,
    pub allowed_launch_profile_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RemoteSessionPolicyLaunchProfileInspectionV1 {
    pub launch_profile_id: String,
    pub session_kind: RemoteSessionKindV1,
    pub harness: RemoteSessionHarnessV1,
    pub executable_path_sha256: String,
    pub argv_count: usize,
    pub environment_keys: Vec<String>,
    pub yolo_mode: bool,
    pub automatic_orchestrator: bool,
    pub worktree_policy: RemoteSessionWorktreePolicyV1,
    pub hook_policy: RemoteSessionHookPolicyV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RemoteSessionPolicyInspectionV1 {
    pub schema: String,
    pub policy_schema: String,
    pub policy_id: String,
    pub policy_sha256: String,
    pub source_file_sha256: String,
    pub source_size_bytes: u64,
    pub enabled: bool,
    pub roots: Vec<RemoteSessionPolicyRootInspectionV1>,
    pub launch_profiles: Vec<RemoteSessionPolicyLaunchProfileInspectionV1>,
    pub capacity: RemoteSessionCapacityPolicyV1,
    pub request_ttl_seconds: u32,
    pub read_only: bool,
    pub profile_state_read: bool,
    pub root_resolution_authorized: bool,
    pub executable_resolution_authorized: bool,
    pub request_creation_authorized: bool,
    pub launch_authorized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RemoteSessionPolicyResolutionV1 {
    pub schema: String,
    pub policy_schema: String,
    pub policy_id: String,
    pub policy_sha256: String,
    pub source_file_sha256: String,
    pub root_id: String,
    pub project_key: String,
    pub installation_boundary_path_sha256: String,
    pub canonical_root_path_sha256: String,
    pub project_root_identity_sha256: String,
    pub launch_profile_id: String,
    pub launch_profile_sha256: String,
    pub harness: RemoteSessionHarnessV1,
    pub harness_launch_sha256: String,
    pub executable_policy_path_sha256: String,
    pub executable_resolved_path_sha256: String,
    pub executable_identity_sha256: String,
    pub executable_content_sha256: String,
    pub executable_runtime_loader_present: bool,
    pub executable_runtime_loader_identity_sha256: Option<String>,
    pub executable_runtime_loader_content_sha256: Option<String>,
    pub executable_runtime_loader_size_bytes: Option<u64>,
    pub executable_size_bytes: u64,
    pub executable_byte_budget: u64,
    pub executable_hash_budget_millis: u64,
    pub executable_chain_byte_budget: u64,
    pub executable_chain_hash_budget_millis: u64,
    pub executable_local_filesystem_required: bool,
    pub executable_directory_count: usize,
    pub executable_directory_chain_sha256: String,
    pub executable_symlink_hops: usize,
    pub executable_symlink_chain_sha256: String,
    pub argv_count: usize,
    pub environment_keys: Vec<String>,
    pub yolo_mode: bool,
    pub automatic_orchestrator: bool,
    pub worktree_policy: RemoteSessionWorktreePolicyV1,
    pub hook_policy: RemoteSessionHookPolicyV1,
    pub read_only: bool,
    pub profile_state_read: bool,
    pub project_registry_read: bool,
    pub exact_root_resolved: bool,
    pub exact_executable_resolved: bool,
    pub request_creation_authorized: bool,
    pub launch_authorized: bool,
}

#[derive(Debug)]
pub struct LoadedRemoteSessionPolicyV1 {
    pub policy: RemoteSessionPolicyV1,
    pub policy_sha256: String,
    pub source_file_sha256: String,
    pub source_size_bytes: u64,
    source_path: PathBuf,
    source_parent: File,
    source_file: File,
    source_parent_identity: (u64, u64),
    source_file_identity: (u64, u64),
    source_file_snapshot: crate::process::FilesystemObjectSnapshot,
}

#[derive(Debug)]
struct RemoteSessionRootBindingV1 {
    installation_boundary_path: PathBuf,
    installation_boundary: File,
    installation_boundary_identity: (u64, u64),
    canonical_root_path: PathBuf,
    project_root: File,
    project_root_identity: (u64, u64),
    components: Vec<RemoteSessionDirectoryBindingV1>,
}

#[derive(Debug)]
struct RemoteSessionDirectoryBindingV1 {
    path: PathBuf,
    descriptor: File,
    identity: (u64, u64),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RemoteSessionExecutableSymlinkBindingV1 {
    path: PathBuf,
    target: PathBuf,
    identity: (u64, u64),
    snapshot: crate::process::FilesystemObjectSnapshot,
}

#[derive(Debug)]
struct RemoteSessionExecutableBindingV1 {
    policy_path: PathBuf,
    resolved_path: PathBuf,
    trusted_boundary_path: PathBuf,
    trusted_boundary_identity: (u64, u64),
    executable: File,
    identity: (u64, u64),
    snapshot: crate::process::FilesystemObjectSnapshot,
    content_sha256: String,
    directories: Vec<RemoteSessionDirectoryBindingV1>,
    symlinks: Vec<RemoteSessionExecutableSymlinkBindingV1>,
    runtime_loader: Option<Box<RemoteSessionExecutableBindingV1>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeExecutableRole {
    Program,
    RuntimeLoader,
}

#[derive(Debug)]
pub struct ResolvedRemoteSessionPolicyV1 {
    loaded: LoadedRemoteSessionPolicyV1,
    root: RemoteSessionRootBindingV1,
    executable: RemoteSessionExecutableBindingV1,
    report: RemoteSessionPolicyResolutionV1,
}

impl LoadedRemoteSessionPolicyV1 {
    pub fn inspection(&self) -> RemoteSessionPolicyInspectionV1 {
        RemoteSessionPolicyInspectionV1 {
            schema: REMOTE_SESSION_POLICY_INSPECTION_SCHEMA.to_string(),
            policy_schema: self.policy.schema.clone(),
            policy_id: self.policy.policy_id.clone(),
            policy_sha256: self.policy_sha256.clone(),
            source_file_sha256: self.source_file_sha256.clone(),
            source_size_bytes: self.source_size_bytes,
            enabled: self.policy.enabled,
            roots: self
                .policy
                .allowed_roots
                .iter()
                .map(|root| RemoteSessionPolicyRootInspectionV1 {
                    root_id: root.root_id.clone(),
                    project_key: root.project_key.clone(),
                    canonical_path_sha256: hash_bytes(
                        POLICY_PATH_HASH_DOMAIN,
                        root.canonical_path.as_bytes(),
                    ),
                    allowed_launch_profile_ids: root.allowed_launch_profile_ids.clone(),
                })
                .collect(),
            launch_profiles: self
                .policy
                .launch_profiles
                .iter()
                .map(|profile| RemoteSessionPolicyLaunchProfileInspectionV1 {
                    launch_profile_id: profile.launch_profile_id.clone(),
                    session_kind: profile.session_kind,
                    harness: profile.harness,
                    executable_path_sha256: hash_bytes(
                        POLICY_PATH_HASH_DOMAIN,
                        profile.executable_path.as_bytes(),
                    ),
                    argv_count: profile.fixed_argv.len(),
                    environment_keys: profile
                        .fixed_environment
                        .iter()
                        .map(|entry| entry.key.clone())
                        .collect(),
                    yolo_mode: profile.yolo_mode,
                    automatic_orchestrator: profile.automatic_orchestrator,
                    worktree_policy: profile.worktree_policy,
                    hook_policy: profile.hook_policy,
                })
                .collect(),
            capacity: self.policy.capacity.clone(),
            request_ttl_seconds: self.policy.request_ttl_seconds,
            read_only: true,
            profile_state_read: false,
            root_resolution_authorized: false,
            executable_resolution_authorized: false,
            request_creation_authorized: false,
            launch_authorized: false,
        }
    }

    pub fn ensure_source_active(&self) -> Result<()> {
        let parent = self
            .source_path
            .parent()
            .context("remote session policy source has no parent")?;
        let name = self
            .source_path
            .file_name()
            .map(Path::new)
            .context("remote session policy source has no file name")?;
        let held_parent_metadata = self.source_parent.metadata()?;
        let held_file_metadata = self.source_file.metadata()?;
        let reopened_parent = crate::process::open_bounded_root(parent)
            .context("reopen remote session policy parent")?;
        let reopened_file = crate::process::open_bounded_regular_file(&reopened_parent, name)
            .context("reopen remote session policy source")?;
        let active_parent = std::fs::symlink_metadata(parent)?;
        let active_file = std::fs::symlink_metadata(&self.source_path)?;
        ensure!(
            crate::process::filesystem_file_identity(&held_parent_metadata)
                == Some(self.source_parent_identity)
                && crate::process::filesystem_file_identity(&reopened_parent.metadata()?)
                    == Some(self.source_parent_identity)
                && crate::process::filesystem_file_identity(&active_parent)
                    == Some(self.source_parent_identity)
                && crate::process::filesystem_file_identity(&held_file_metadata)
                    == Some(self.source_file_identity)
                && crate::process::filesystem_file_identity(&reopened_file.metadata()?)
                    == Some(self.source_file_identity)
                && crate::process::filesystem_file_identity(&active_file)
                    == Some(self.source_file_identity)
                && crate::process::FilesystemObjectSnapshot::from_metadata(&held_file_metadata)
                    == self.source_file_snapshot
                && crate::process::FilesystemObjectSnapshot::from_metadata(
                    &reopened_file.metadata()?
                ) == self.source_file_snapshot,
            "remote session policy source is no longer the exact validated snapshot"
        );
        ensure_policy_parent_authority(&held_parent_metadata)?;
        ensure_policy_parent_authority(&reopened_parent.metadata()?)?;
        ensure_policy_parent_authority(&active_parent)?;
        ensure_policy_file_authority(&held_file_metadata)?;
        ensure_policy_file_authority(&reopened_file.metadata()?)?;
        ensure_policy_file_authority(&active_file)
    }
}

impl ResolvedRemoteSessionPolicyV1 {
    pub fn report(&self) -> &RemoteSessionPolicyResolutionV1 {
        &self.report
    }

    pub fn ensure_active(&self) -> Result<()> {
        self.loaded.ensure_source_active()?;
        self.root.ensure_active()?;
        self.executable.ensure_active()
    }
}

impl RemoteSessionRootBindingV1 {
    fn ensure_active(&self) -> Result<()> {
        let held_boundary = self.installation_boundary.metadata()?;
        let held_root = self.project_root.metadata()?;
        let reopened_boundary = crate::process::open_bounded_root(&self.installation_boundary_path)
            .context("reopen remote session installation boundary")?;
        let mut current = reopened_boundary.try_clone()?;
        for binding in &self.components {
            let name = binding
                .path
                .file_name()
                .map(Path::new)
                .context("remote session root component has no name")?;
            let reopened = crate::process::open_bounded_subdirectory(&current, name)
                .context("reopen exact remote session root component")?;
            let held = binding.descriptor.metadata()?;
            let active = std::fs::symlink_metadata(&binding.path)?;
            ensure!(
                crate::process::filesystem_file_identity(&held) == Some(binding.identity)
                    && crate::process::filesystem_file_identity(&reopened.metadata()?)
                        == Some(binding.identity)
                    && crate::process::filesystem_file_identity(&active) == Some(binding.identity),
                "remote session root component is no longer exact"
            );
            ensure_remote_session_directory_authority(&held, "remote session root component")?;
            ensure_remote_session_directory_authority(&active, "remote session root component")?;
            current = reopened;
        }
        let reopened_root = current;
        let active_boundary = std::fs::symlink_metadata(&self.installation_boundary_path)?;
        let active_root = std::fs::symlink_metadata(&self.canonical_root_path)?;
        ensure!(
            crate::process::filesystem_file_identity(&held_boundary)
                == Some(self.installation_boundary_identity)
                && crate::process::filesystem_file_identity(&reopened_boundary.metadata()?)
                    == Some(self.installation_boundary_identity)
                && crate::process::filesystem_file_identity(&active_boundary)
                    == Some(self.installation_boundary_identity)
                && crate::process::filesystem_file_identity(&held_root)
                    == Some(self.project_root_identity)
                && crate::process::filesystem_file_identity(&reopened_root.metadata()?)
                    == Some(self.project_root_identity)
                && crate::process::filesystem_file_identity(&active_root)
                    == Some(self.project_root_identity),
            "remote session project root is no longer the exact resolved directory"
        );
        ensure_remote_session_directory_authority(
            &held_boundary,
            "remote session installation boundary",
        )?;
        ensure_remote_session_directory_authority(
            &active_boundary,
            "remote session installation boundary",
        )?;
        ensure_remote_session_directory_authority(&held_root, "remote session project root")?;
        ensure_remote_session_directory_authority(&active_root, "remote session project root")
    }
}

impl RemoteSessionExecutableBindingV1 {
    fn ensure_active(&self) -> Result<()> {
        self.ensure_held_chain_active()?;
        let trusted_boundary = crate::process::open_bounded_root(&self.trusted_boundary_path)
            .context("reopen executable trust boundary")?;
        let active_boundary = std::fs::symlink_metadata(&self.trusted_boundary_path)?;
        ensure!(
            crate::process::filesystem_file_identity(&trusted_boundary.metadata()?)
                == Some(self.trusted_boundary_identity)
                && crate::process::filesystem_file_identity(&active_boundary)
                    == Some(self.trusted_boundary_identity),
            "remote session executable trust boundary changed"
        );
        let reopened = open_remote_session_executable(
            &self.policy_path,
            &self.trusted_boundary_path,
            &trusted_boundary,
            self.trusted_boundary_identity,
        )?;
        ensure!(
            executable_bindings_match(&reopened, self),
            "remote session executable or allowed symlink chain changed after resolution"
        );
        Ok(())
    }

    fn ensure_held_chain_active(&self) -> Result<()> {
        let held = self.executable.metadata()?;
        ensure!(
            crate::process::filesystem_file_identity(&held) == Some(self.identity)
                && crate::process::FilesystemObjectSnapshot::from_metadata(&held) == self.snapshot,
            "remote session executable descriptor changed after resolution"
        );
        ensure_remote_session_executable_authority(&held)?;
        ensure_executable_directory_bindings_active(&self.directories)?;
        ensure_executable_symlink_bindings_active(&self.symlinks)?;
        if let Some(runtime_loader) = &self.runtime_loader {
            runtime_loader.ensure_held_chain_active()?;
        }
        Ok(())
    }
}

pub fn resolve_remote_session_policy_target(
    policy_path: &Path,
    installation_boundary: &Path,
    root_id: &str,
    launch_profile_id: &str,
) -> Result<ResolvedRemoteSessionPolicyV1> {
    let loaded = load_remote_session_policy_for_inspection(policy_path)?;
    resolve_loaded_remote_session_policy_target(
        loaded,
        installation_boundary,
        root_id,
        launch_profile_id,
    )
}

pub fn resolve_loaded_remote_session_policy_target(
    loaded: LoadedRemoteSessionPolicyV1,
    installation_boundary: &Path,
    root_id: &str,
    launch_profile_id: &str,
) -> Result<ResolvedRemoteSessionPolicyV1> {
    loaded.ensure_source_active()?;
    ensure!(
        loaded.policy.enabled,
        "remote session policy is disabled and cannot resolve a launch target"
    );
    validate_identifier(root_id, "remote session root selector")?;
    validate_identifier(launch_profile_id, "remote session launch profile selector")?;
    let allowed_root = loaded.policy.allowed_root(root_id)?.clone();
    let launch_profile = loaded.policy.launch_profile(launch_profile_id)?.clone();
    ensure!(
        allowed_root
            .allowed_launch_profile_ids
            .iter()
            .filter(|candidate| candidate.as_str() == launch_profile_id)
            .count()
            == 1,
        "remote session launch profile is not authorized for the exact root"
    );

    let root = open_remote_session_root(&allowed_root, installation_boundary)?;
    let executable = open_remote_session_executable(
        Path::new(&launch_profile.executable_path),
        &root.installation_boundary_path,
        &root.installation_boundary,
        root.installation_boundary_identity,
    )?;
    loaded.ensure_source_active()?;
    root.ensure_active()?;
    executable.ensure_active()?;

    let report = RemoteSessionPolicyResolutionV1 {
        schema: REMOTE_SESSION_POLICY_RESOLUTION_SCHEMA.to_string(),
        policy_schema: loaded.policy.schema.clone(),
        policy_id: loaded.policy.policy_id.clone(),
        policy_sha256: loaded.policy_sha256.clone(),
        source_file_sha256: loaded.source_file_sha256.clone(),
        root_id: allowed_root.root_id.clone(),
        project_key: allowed_root.project_key.clone(),
        installation_boundary_path_sha256: hash_path_for_report(installation_boundary)?,
        canonical_root_path_sha256: hash_path_for_report(&root.canonical_root_path)?,
        project_root_identity_sha256: remote_session_root_identity_sha256(
            &root.canonical_root_path,
            root.project_root_identity,
        )?,
        launch_profile_id: launch_profile.launch_profile_id.clone(),
        launch_profile_sha256: remote_session_launch_profile_sha256(&launch_profile)?,
        harness: launch_profile.harness,
        harness_launch_sha256: remote_session_policy_harness_launch_sha256(&launch_profile)?,
        executable_policy_path_sha256: hash_path_for_report(&executable.policy_path)?,
        executable_resolved_path_sha256: hash_path_for_report(&executable.resolved_path)?,
        executable_identity_sha256: remote_session_executable_identity_sha256(&executable)?,
        executable_content_sha256: executable.content_sha256.clone(),
        executable_runtime_loader_present: executable.runtime_loader.is_some(),
        executable_runtime_loader_identity_sha256: executable
            .runtime_loader
            .as_deref()
            .map(remote_session_executable_identity_sha256)
            .transpose()?,
        executable_runtime_loader_content_sha256: executable
            .runtime_loader
            .as_ref()
            .map(|runtime_loader| runtime_loader.content_sha256.clone()),
        executable_runtime_loader_size_bytes: executable
            .runtime_loader
            .as_ref()
            .map(|runtime_loader| {
                runtime_loader
                    .executable
                    .metadata()
                    .map(|metadata| metadata.len())
            })
            .transpose()?,
        executable_size_bytes: executable.executable.metadata()?.len(),
        executable_byte_budget: REMOTE_SESSION_EXECUTABLE_MAX_BYTES,
        executable_hash_budget_millis: REMOTE_SESSION_EXECUTABLE_HASH_MAX_MILLIS,
        executable_chain_byte_budget: REMOTE_SESSION_EXECUTABLE_MAX_BYTES
            * if executable.runtime_loader.is_some() {
                2
            } else {
                1
            },
        executable_chain_hash_budget_millis: REMOTE_SESSION_EXECUTABLE_HASH_MAX_MILLIS
            * if executable.runtime_loader.is_some() {
                2
            } else {
                1
            },
        executable_local_filesystem_required: true,
        executable_directory_count: executable.directories.len(),
        executable_directory_chain_sha256: remote_session_executable_directory_chain_sha256(
            &executable.directories,
        )?,
        executable_symlink_hops: executable.symlinks.len(),
        executable_symlink_chain_sha256: remote_session_executable_symlink_chain_sha256(
            &executable.symlinks,
        )?,
        argv_count: launch_profile.fixed_argv.len(),
        environment_keys: launch_profile
            .fixed_environment
            .iter()
            .map(|entry| entry.key.clone())
            .collect(),
        yolo_mode: launch_profile.yolo_mode,
        automatic_orchestrator: launch_profile.automatic_orchestrator,
        worktree_policy: launch_profile.worktree_policy,
        hook_policy: launch_profile.hook_policy,
        read_only: true,
        profile_state_read: false,
        project_registry_read: false,
        exact_root_resolved: true,
        exact_executable_resolved: true,
        request_creation_authorized: false,
        launch_authorized: false,
    };
    Ok(ResolvedRemoteSessionPolicyV1 {
        loaded,
        root,
        executable,
        report,
    })
}

fn open_remote_session_root(
    allowed_root: &RemoteSessionAllowedRootV1,
    installation_boundary: &Path,
) -> Result<RemoteSessionRootBindingV1> {
    let boundary_text = installation_boundary
        .to_str()
        .context("remote session installation boundary is not UTF-8")?;
    validate_absolute_path(boundary_text, "remote session installation boundary")?;
    let canonical_root_path = PathBuf::from(&allowed_root.canonical_path);
    let root_relative_path = canonical_root_path
        .strip_prefix(installation_boundary)
        .context("remote session root is outside the installation boundary")?
        .to_path_buf();
    ensure!(
        !root_relative_path.as_os_str().is_empty()
            && root_relative_path
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
        "remote session root cannot be the workspace umbrella directory"
    );

    let installation_boundary = crate::process::open_bounded_root(installation_boundary)
        .context("open remote session installation boundary")?;
    let boundary_metadata = installation_boundary.metadata()?;
    ensure_remote_session_directory_authority(
        &boundary_metadata,
        "remote session installation boundary",
    )?;
    let installation_boundary_identity =
        crate::process::filesystem_file_identity(&boundary_metadata)
            .context("remote session installation boundary has no filesystem identity")?;
    let mut current = installation_boundary.try_clone()?;
    let mut current_path = PathBuf::from(boundary_text);
    let mut components = Vec::new();
    for component in root_relative_path.components() {
        let Component::Normal(name) = component else {
            bail!("remote session root contains a non-canonical component");
        };
        current_path.push(name);
        let opened = crate::process::open_bounded_subdirectory(&current, Path::new(name))
            .context("open exact remote session root component")?;
        let metadata = opened.metadata()?;
        ensure_remote_session_directory_authority(&metadata, "remote session root component")?;
        let identity = crate::process::filesystem_file_identity(&metadata)
            .context("remote session root component has no filesystem identity")?;
        let active = std::fs::symlink_metadata(&current_path)?;
        ensure!(
            crate::process::filesystem_file_identity(&active) == Some(identity),
            "remote session root component is not the exact active directory"
        );
        ensure_remote_session_directory_authority(&active, "remote session root component")?;
        components.push(RemoteSessionDirectoryBindingV1 {
            path: current_path.clone(),
            descriptor: opened.try_clone()?,
            identity,
        });
        current = opened;
    }
    let project_root = current;
    let project_root_metadata = project_root.metadata()?;
    ensure_remote_session_directory_authority(
        &project_root_metadata,
        "remote session project root",
    )?;
    let project_root_identity = crate::process::filesystem_file_identity(&project_root_metadata)
        .context("remote session project root has no filesystem identity")?;
    let active_boundary = std::fs::symlink_metadata(boundary_text)?;
    let active_root = std::fs::symlink_metadata(&canonical_root_path)?;
    ensure!(
        crate::process::filesystem_file_identity(&active_boundary)
            == Some(installation_boundary_identity)
            && crate::process::filesystem_file_identity(&active_root)
                == Some(project_root_identity),
        "remote session root is not the exact active directory"
    );

    let binding = RemoteSessionRootBindingV1 {
        installation_boundary_path: PathBuf::from(boundary_text),
        installation_boundary,
        installation_boundary_identity,
        canonical_root_path,
        project_root,
        project_root_identity,
        components,
    };
    binding.ensure_active()?;
    Ok(binding)
}

fn open_remote_session_executable(
    path: &Path,
    trusted_boundary_path: &Path,
    trusted_boundary: &File,
    trusted_boundary_identity: (u64, u64),
) -> Result<RemoteSessionExecutableBindingV1> {
    open_remote_session_executable_with_runtime_loader(
        path,
        trusted_boundary_path,
        trusted_boundary,
        trusted_boundary_identity,
        NativeExecutableRole::Program,
    )
}

fn open_remote_session_executable_with_runtime_loader(
    path: &Path,
    trusted_boundary_path: &Path,
    trusted_boundary: &File,
    trusted_boundary_identity: (u64, u64),
    role: NativeExecutableRole,
) -> Result<RemoteSessionExecutableBindingV1> {
    let path_text = path
        .to_str()
        .context("remote session executable path is not UTF-8")?;
    validate_absolute_path(path_text, "remote session executable")?;
    ensure!(
        !is_virtual_executable_path(path),
        "remote session executable cannot use a virtual filesystem path"
    );
    let (resolved_path, parent_descriptor, directories, symlinks) =
        resolve_remote_session_executable_path(
            path,
            trusted_boundary_path,
            trusted_boundary,
            trusted_boundary_identity,
        )?;
    ensure!(
        !is_virtual_executable_path(&resolved_path),
        "remote session executable resolved into a virtual filesystem"
    );
    let name = resolved_path
        .file_name()
        .map(Path::new)
        .context("remote session executable has no file name")?;
    ensure_supported_local_remote_session_filesystem(&parent_descriptor)?;
    let mut executable = crate::process::open_bounded_regular_file(&parent_descriptor, name)
        .context("open resolved remote session executable")?;
    let before = executable.metadata()?;
    ensure_remote_session_executable_authority(&before)?;
    ensure_supported_local_remote_session_filesystem(&executable)?;
    ensure!(
        before.len() <= REMOTE_SESSION_EXECUTABLE_MAX_BYTES,
        "remote session executable exceeds its byte budget"
    );
    let started_at = Instant::now();
    let hash_budget = Duration::from_millis(REMOTE_SESSION_EXECUTABLE_HASH_MAX_MILLIS);
    let runtime_loader_path = validate_native_remote_session_executable(
        &mut executable,
        before.len(),
        started_at,
        hash_budget,
        role,
    )?;
    ensure!(
        role == NativeExecutableRole::Program || runtime_loader_path.is_none(),
        "remote session runtime loader cannot delegate to another runtime loader"
    );
    let identity = crate::process::filesystem_file_identity(&before)
        .context("remote session executable has no filesystem identity")?;
    let mut hasher = Sha256::new();
    let mut bytes_read = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        ensure!(
            started_at.elapsed() <= hash_budget,
            "remote session executable exceeded its hash time budget"
        );
        let remaining = (REMOTE_SESSION_EXECUTABLE_MAX_BYTES + 1)
            .saturating_sub(bytes_read)
            .min(buffer.len() as u64) as usize;
        if remaining == 0 {
            break;
        }
        let read = executable.read(&mut buffer[..remaining])?;
        ensure!(
            started_at.elapsed() <= hash_budget,
            "remote session executable exceeded its hash time budget"
        );
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes_read = bytes_read
            .checked_add(read as u64)
            .context("remote session executable byte count overflow")?;
    }
    ensure!(
        bytes_read <= REMOTE_SESSION_EXECUTABLE_MAX_BYTES,
        "remote session executable grew beyond its byte budget"
    );
    let after = executable.metadata()?;
    let snapshot = crate::process::FilesystemObjectSnapshot::from_metadata(&before);
    ensure!(
        snapshot == crate::process::FilesystemObjectSnapshot::from_metadata(&after),
        "remote session executable changed while hashing"
    );
    let content_sha256 = format!("{:x}", hasher.finalize());
    let runtime_loader = runtime_loader_path
        .map(|runtime_loader_path| {
            ensure!(
                runtime_loader_path != path,
                "remote session executable cannot be its own runtime loader"
            );
            open_remote_session_executable_with_runtime_loader(
                &runtime_loader_path,
                trusted_boundary_path,
                trusted_boundary,
                trusted_boundary_identity,
                NativeExecutableRole::RuntimeLoader,
            )
            .map(Box::new)
        })
        .transpose()?;
    let (final_resolved_path, final_parent, final_directories, final_symlinks) =
        resolve_remote_session_executable_path(
            path,
            trusted_boundary_path,
            trusted_boundary,
            trusted_boundary_identity,
        )?;
    let reopened = crate::process::open_bounded_regular_file(&final_parent, name)
        .context("reopen resolved remote session executable")?;
    let active_resolved = std::fs::symlink_metadata(&resolved_path)?;
    let active_policy = std::fs::metadata(path)?;
    ensure!(
        crate::process::filesystem_file_identity(&reopened.metadata()?) == Some(identity)
            && crate::process::filesystem_file_identity(&active_resolved) == Some(identity)
            && crate::process::filesystem_file_identity(&active_policy) == Some(identity)
            && crate::process::FilesystemObjectSnapshot::from_metadata(&reopened.metadata()?)
                == snapshot,
        "remote session executable is not the exact active file"
    );
    ensure_remote_session_executable_authority(&reopened.metadata()?)?;
    ensure_executable_directory_bindings_active(&directories)?;
    ensure_executable_symlink_bindings_active(&symlinks)?;
    ensure!(
        final_resolved_path == resolved_path
            && executable_directory_bindings_match(&final_directories, &directories)
            && final_symlinks == symlinks,
        "remote session executable symlink chain changed while resolving"
    );
    executable.seek(SeekFrom::Start(0))?;
    Ok(RemoteSessionExecutableBindingV1 {
        policy_path: path.to_path_buf(),
        resolved_path,
        trusted_boundary_path: trusted_boundary_path.to_path_buf(),
        trusted_boundary_identity,
        executable,
        identity,
        snapshot,
        content_sha256,
        directories,
        symlinks,
        runtime_loader,
    })
}

fn resolve_remote_session_executable_path(
    path: &Path,
    trusted_boundary_path: &Path,
    trusted_boundary: &File,
    trusted_boundary_identity: (u64, u64),
) -> Result<(
    PathBuf,
    File,
    Vec<RemoteSessionDirectoryBindingV1>,
    Vec<RemoteSessionExecutableSymlinkBindingV1>,
)> {
    let (mut resolved, mut current_directory, mut pending, initial_directory) =
        start_remote_session_executable_walk(
            path,
            trusted_boundary_path,
            trusted_boundary,
            trusted_boundary_identity,
        )?;
    let mut directories = vec![initial_directory];
    let mut symlinks = Vec::new();
    let mut seen_symlinks = BTreeSet::new();
    let mut components_observed = 0usize;
    while let Some(component) = pending.pop_front() {
        components_observed = components_observed
            .checked_add(1)
            .context("remote session executable path component budget overflow")?;
        ensure!(
            components_observed <= MAX_EXECUTABLE_PATH_COMPONENTS,
            "remote session executable exceeds its path component budget"
        );
        let candidate = resolved.join(&component);
        let before = std::fs::symlink_metadata(&candidate)
            .context("inspect remote session executable path component")?;
        if before.file_type().is_symlink() {
            ensure!(
                symlinks.len() < MAX_EXECUTABLE_SYMLINK_HOPS,
                "remote session executable exceeds its symlink budget"
            );
            ensure_remote_session_executable_symlink_authority(&before)?;
            let identity = crate::process::filesystem_file_identity(&before)
                .context("remote session executable symlink has no filesystem identity")?;
            ensure!(
                seen_symlinks.insert(identity),
                "remote session executable symlink chain contains a cycle"
            );
            let target =
                std::fs::read_link(&candidate).context("read remote session executable symlink")?;
            ensure!(
                target.to_str().is_some(),
                "remote session executable symlink target is not UTF-8"
            );
            let after = std::fs::symlink_metadata(&candidate)?;
            let snapshot = crate::process::FilesystemObjectSnapshot::from_metadata(&before);
            ensure!(
                snapshot == crate::process::FilesystemObjectSnapshot::from_metadata(&after)
                    && crate::process::filesystem_file_identity(&after) == Some(identity)
                    && std::fs::read_link(&candidate)? == target,
                "remote session executable symlink changed while resolving"
            );
            symlinks.push(RemoteSessionExecutableSymlinkBindingV1 {
                path: candidate,
                target: target.clone(),
                identity,
                snapshot,
            });
            let mut redirected = if target.is_absolute() {
                target
            } else {
                resolved.join(target)
            };
            for remaining in &pending {
                redirected.push(remaining);
            }
            let (next_resolved, next_directory, next_pending, directory_binding) =
                start_remote_session_executable_walk(
                    &redirected,
                    trusted_boundary_path,
                    trusted_boundary,
                    trusted_boundary_identity,
                )?;
            resolved = next_resolved;
            current_directory = next_directory;
            pending = next_pending;
            directories.push(directory_binding);
            continue;
        }
        if !pending.is_empty() {
            ensure!(
                before.is_dir(),
                "remote session executable path contains a non-directory component"
            );
            let opened = crate::process::open_bounded_subdirectory(
                &current_directory,
                Path::new(&component),
            )
            .context("open remote session executable directory component")?;
            ensure_supported_local_remote_session_filesystem(&opened)?;
            let metadata = opened.metadata()?;
            ensure_remote_session_executable_directory_authority(&metadata)?;
            let identity = crate::process::filesystem_file_identity(&metadata)
                .context("remote session executable directory has no filesystem identity")?;
            ensure!(
                crate::process::filesystem_file_identity(&before) == Some(identity),
                "remote session executable directory is not the exact active component"
            );
            directories.push(RemoteSessionDirectoryBindingV1 {
                path: candidate.clone(),
                descriptor: opened.try_clone()?,
                identity,
            });
            current_directory = opened;
        } else {
            ensure!(
                before.is_file(),
                "remote session executable must resolve to a regular file"
            );
        }
        resolved.push(component);
    }
    ensure!(
        resolved != Path::new("/"),
        "remote session executable resolved to filesystem root"
    );
    Ok((resolved, current_directory, directories, symlinks))
}

fn start_remote_session_executable_walk(
    path: &Path,
    trusted_boundary_path: &Path,
    trusted_boundary: &File,
    trusted_boundary_identity: (u64, u64),
) -> Result<(
    PathBuf,
    File,
    VecDeque<OsString>,
    RemoteSessionDirectoryBindingV1,
)> {
    let normalized = canonical_absolute_path(path)?;
    if let Ok(relative) = normalized.strip_prefix(trusted_boundary_path) {
        ensure!(
            !relative.as_os_str().is_empty(),
            "remote session executable cannot be the trust boundary directory"
        );
        let metadata = trusted_boundary.metadata()?;
        ensure!(
            crate::process::filesystem_file_identity(&metadata) == Some(trusted_boundary_identity),
            "remote session executable trust boundary descriptor changed"
        );
        ensure_remote_session_directory_authority(
            &metadata,
            "remote session executable trust boundary",
        )?;
        ensure_supported_local_remote_session_filesystem(trusted_boundary)?;
        return Ok((
            trusted_boundary_path.to_path_buf(),
            trusted_boundary.try_clone()?,
            relative
                .components()
                .filter_map(|component| match component {
                    Component::Normal(name) => Some(name.to_os_string()),
                    _ => None,
                })
                .collect(),
            RemoteSessionDirectoryBindingV1 {
                path: trusted_boundary_path.to_path_buf(),
                descriptor: trusted_boundary.try_clone()?,
                identity: trusted_boundary_identity,
            },
        ));
    }

    let filesystem_root = crate::process::open_bounded_root(Path::new("/"))
        .context("open executable filesystem trust root")?;
    ensure_supported_local_remote_session_filesystem(&filesystem_root)?;
    let metadata = filesystem_root.metadata()?;
    ensure_remote_session_executable_directory_authority(&metadata)?;
    let identity = crate::process::filesystem_file_identity(&metadata)
        .context("executable filesystem root has no identity")?;
    Ok((
        PathBuf::from("/"),
        filesystem_root.try_clone()?,
        canonical_absolute_components(&normalized)?,
        RemoteSessionDirectoryBindingV1 {
            path: PathBuf::from("/"),
            descriptor: filesystem_root,
            identity,
        },
    ))
}

fn canonical_absolute_path(path: &Path) -> Result<PathBuf> {
    let components = canonical_absolute_components(path)?;
    let mut normalized = PathBuf::from("/");
    for component in components {
        normalized.push(component);
    }
    Ok(normalized)
}

fn canonical_absolute_components(path: &Path) -> Result<VecDeque<OsString>> {
    ensure!(
        path.is_absolute(),
        "remote session executable path must be absolute"
    );
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                ensure!(
                    components.pop().is_some(),
                    "remote session executable path escapes filesystem root"
                );
            }
            Component::Normal(name) => components.push(name.to_os_string()),
            Component::Prefix(_) => {
                bail!("remote session executable path prefix is unsupported")
            }
        }
    }
    ensure!(
        !components.is_empty(),
        "remote session executable path cannot be filesystem root"
    );
    Ok(components.into())
}

fn ensure_executable_symlink_bindings_active(
    symlinks: &[RemoteSessionExecutableSymlinkBindingV1],
) -> Result<()> {
    for binding in symlinks {
        let metadata = std::fs::symlink_metadata(&binding.path)?;
        ensure_remote_session_executable_symlink_authority(&metadata)?;
        ensure!(
            crate::process::filesystem_file_identity(&metadata) == Some(binding.identity)
                && crate::process::FilesystemObjectSnapshot::from_metadata(&metadata)
                    == binding.snapshot
                && std::fs::read_link(&binding.path)? == binding.target,
            "remote session executable symlink chain is no longer exact"
        );
    }
    Ok(())
}

fn ensure_executable_directory_bindings_active(
    directories: &[RemoteSessionDirectoryBindingV1],
) -> Result<()> {
    for binding in directories {
        let held = binding.descriptor.metadata()?;
        let active = std::fs::symlink_metadata(&binding.path)?;
        ensure_remote_session_executable_directory_authority(&held)?;
        ensure_remote_session_executable_directory_authority(&active)?;
        ensure!(
            crate::process::filesystem_file_identity(&held) == Some(binding.identity)
                && crate::process::filesystem_file_identity(&active) == Some(binding.identity),
            "remote session executable directory chain is no longer exact"
        );
    }
    Ok(())
}

fn executable_directory_bindings_match(
    left: &[RemoteSessionDirectoryBindingV1],
    right: &[RemoteSessionDirectoryBindingV1],
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.path == right.path && left.identity == right.identity)
}

fn executable_bindings_match(
    left: &RemoteSessionExecutableBindingV1,
    right: &RemoteSessionExecutableBindingV1,
) -> bool {
    left.policy_path == right.policy_path
        && left.resolved_path == right.resolved_path
        && left.trusted_boundary_path == right.trusted_boundary_path
        && left.trusted_boundary_identity == right.trusted_boundary_identity
        && left.identity == right.identity
        && left.snapshot == right.snapshot
        && left.content_sha256 == right.content_sha256
        && executable_directory_bindings_match(&left.directories, &right.directories)
        && left.symlinks == right.symlinks
        && match (&left.runtime_loader, &right.runtime_loader) {
            (Some(left), Some(right)) => executable_bindings_match(left, right),
            (None, None) => true,
            _ => false,
        }
}

fn remote_session_root_identity_sha256(path: &Path, identity: (u64, u64)) -> Result<String> {
    let path = path
        .to_str()
        .context("remote session project root path is not UTF-8")?;
    let device = identity.0.to_string();
    let inode = identity.1.to_string();
    Ok(hash_length_prefixed_parts(
        PROJECT_ROOT_HASH_DOMAIN,
        &[path, device.as_str(), inode.as_str()],
    ))
}

fn remote_session_executable_identity_sha256(
    executable: &RemoteSessionExecutableBindingV1,
) -> Result<String> {
    #[derive(Serialize)]
    struct ExecutableIdentity<'a> {
        resolved_path: &'a str,
        device: u64,
        inode: u64,
        content_sha256: &'a str,
        runtime_loader_identity_sha256: Option<&'a str>,
    }
    let resolved_path = executable
        .resolved_path
        .to_str()
        .context("resolved remote session executable path is not UTF-8")?;
    let runtime_loader_identity_sha256 = executable
        .runtime_loader
        .as_deref()
        .map(remote_session_executable_identity_sha256)
        .transpose()?;
    hash_serializable(
        EXECUTABLE_IDENTITY_HASH_DOMAIN,
        &ExecutableIdentity {
            resolved_path,
            device: executable.identity.0,
            inode: executable.identity.1,
            content_sha256: &executable.content_sha256,
            runtime_loader_identity_sha256: runtime_loader_identity_sha256.as_deref(),
        },
    )
}

fn remote_session_executable_symlink_chain_sha256(
    symlinks: &[RemoteSessionExecutableSymlinkBindingV1],
) -> Result<String> {
    #[derive(Serialize)]
    struct SymlinkIdentity<'a> {
        path: &'a str,
        target: &'a str,
        device: u64,
        inode: u64,
    }
    let mut identities = Vec::with_capacity(symlinks.len());
    for symlink in symlinks {
        identities.push(SymlinkIdentity {
            path: symlink
                .path
                .to_str()
                .context("remote session executable symlink path is not UTF-8")?,
            target: symlink
                .target
                .to_str()
                .context("remote session executable symlink target is not UTF-8")?,
            device: symlink.identity.0,
            inode: symlink.identity.1,
        });
    }
    hash_serializable(EXECUTABLE_SYMLINK_CHAIN_HASH_DOMAIN, &identities)
}

fn remote_session_executable_directory_chain_sha256(
    directories: &[RemoteSessionDirectoryBindingV1],
) -> Result<String> {
    #[derive(Serialize)]
    struct DirectoryIdentity<'a> {
        path: &'a str,
        device: u64,
        inode: u64,
    }
    let mut identities = Vec::with_capacity(directories.len());
    for directory in directories {
        identities.push(DirectoryIdentity {
            path: directory
                .path
                .to_str()
                .context("remote session executable directory path is not UTF-8")?,
            device: directory.identity.0,
            inode: directory.identity.1,
        });
    }
    hash_serializable(EXECUTABLE_DIRECTORY_CHAIN_HASH_DOMAIN, &identities)
}

fn hash_path_for_report(path: &Path) -> Result<String> {
    let path = path
        .to_str()
        .context("remote session resolved path is not UTF-8")?;
    Ok(hash_bytes(POLICY_PATH_HASH_DOMAIN, path.as_bytes()))
}

fn hash_length_prefixed_parts(domain: &[u8], parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    for part in parts {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn is_virtual_executable_path(path: &Path) -> bool {
    [Path::new("/proc"), Path::new("/sys"), Path::new("/dev")]
        .iter()
        .any(|root| path == *root || path.starts_with(root))
}

fn validate_native_remote_session_executable(
    executable: &mut File,
    size_bytes: u64,
    started_at: Instant,
    budget: Duration,
    role: NativeExecutableRole,
) -> Result<Option<PathBuf>> {
    #[cfg(target_os = "linux")]
    let runtime_loader =
        validate_linux_elf_executable(executable, size_bytes, started_at, budget, role)?;

    #[cfg(target_os = "macos")]
    let runtime_loader = validate_macos_macho_executable(
        executable,
        size_bytes,
        started_at,
        budget,
        native_macos_cpu_type(),
        role,
    )?;

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    bail!("native remote session executable validation is unsupported on this platform");

    executable.seek(SeekFrom::Start(0))?;
    Ok(runtime_loader)
}

#[cfg(target_os = "linux")]
fn validate_linux_elf_executable(
    executable: &mut File,
    size_bytes: u64,
    started_at: Instant,
    budget: Duration,
    _role: NativeExecutableRole,
) -> Result<Option<PathBuf>> {
    const ELF64_HEADER_BYTES: usize = 64;
    const ELF64_PROGRAM_HEADER_BYTES: u16 = 56;
    const PT_LOAD: u32 = 1;
    const PT_INTERP: u32 = 3;
    const PF_X: u32 = 1;

    ensure!(
        size_bytes >= ELF64_HEADER_BYTES as u64,
        "remote session ELF executable is truncated"
    );
    let mut header = [0u8; ELF64_HEADER_BYTES];
    read_exact_at_with_budget(executable, 0, &mut header, started_at, budget)?;
    ensure!(
        &header[..4] == b"\x7fELF" && header[4] == 2 && header[5] == 1 && header[6] == 1,
        "remote session executable is not a supported 64-bit little-endian ELF"
    );
    let executable_type = little_u16(&header, 16)?;
    ensure!(
        matches!(executable_type, 2 | 3),
        "remote session ELF is not an executable or position-independent executable"
    );
    let machine = little_u16(&header, 18)?;
    ensure!(
        machine == native_linux_elf_machine(),
        "remote session ELF architecture does not match the host"
    );
    ensure!(
        little_u32(&header, 20)? == 1,
        "remote session ELF version is invalid"
    );
    let entry = little_u64(&header, 24)?;
    let program_offset = little_u64(&header, 32)?;
    let header_size = little_u16(&header, 52)?;
    let program_entry_size = little_u16(&header, 54)?;
    let program_count = little_u16(&header, 56)?;
    ensure!(
        header_size == ELF64_HEADER_BYTES as u16
            && program_entry_size == ELF64_PROGRAM_HEADER_BYTES
            && (1..=1024).contains(&program_count)
            && program_offset >= header_size as u64,
        "remote session ELF header or program-table shape is invalid"
    );
    let program_bytes = u64::from(program_entry_size)
        .checked_mul(u64::from(program_count))
        .context("remote session ELF program-table size overflow")?;
    let program_end = program_offset
        .checked_add(program_bytes)
        .context("remote session ELF program-table offset overflow")?;
    ensure!(
        program_end <= size_bytes,
        "remote session ELF program table exceeds the file"
    );

    let mut executable_entry_segment = false;
    let mut runtime_loader = None;
    for index in 0..program_count {
        let offset = program_offset
            .checked_add(u64::from(index) * u64::from(program_entry_size))
            .context("remote session ELF program-header offset overflow")?;
        let mut program = [0u8; ELF64_PROGRAM_HEADER_BYTES as usize];
        read_exact_at_with_budget(executable, offset, &mut program, started_at, budget)?;
        let program_type = little_u32(&program, 0)?;
        let flags = little_u32(&program, 4)?;
        let file_offset = little_u64(&program, 8)?;
        let virtual_address = little_u64(&program, 16)?;
        let file_size = little_u64(&program, 32)?;
        let memory_size = little_u64(&program, 40)?;
        let alignment = little_u64(&program, 48)?;
        let file_end = file_offset
            .checked_add(file_size)
            .context("remote session ELF segment range overflow")?;
        ensure!(
            file_end <= size_bytes,
            "remote session ELF segment exceeds the file"
        );
        if program_type == PT_INTERP {
            ensure!(
                runtime_loader.is_none(),
                "remote session ELF has multiple runtime interpreters"
            );
            ensure!(
                (2..=4096).contains(&file_size),
                "remote session ELF interpreter record is invalid"
            );
            let mut interpreter = vec![0u8; file_size as usize];
            read_exact_at_with_budget(
                executable,
                file_offset,
                &mut interpreter,
                started_at,
                budget,
            )?;
            ensure!(
                interpreter.first() == Some(&b'/')
                    && interpreter.last() == Some(&0)
                    && !interpreter[..interpreter.len() - 1]
                        .iter()
                        .any(|byte| byte.is_ascii_whitespace() || *byte == 0),
                "remote session ELF interpreter must be one absolute path"
            );
            let interpreter = std::str::from_utf8(&interpreter[..interpreter.len() - 1])
                .context("remote session ELF interpreter path is not UTF-8")?;
            runtime_loader = Some(PathBuf::from(interpreter));
        }
        if program_type == PT_LOAD {
            ensure!(
                file_size <= memory_size,
                "remote session ELF load segment file size exceeds memory size"
            );
            ensure!(
                alignment <= 1
                    || (alignment.is_power_of_two()
                        && file_offset % alignment == virtual_address % alignment),
                "remote session ELF load segment alignment is invalid"
            );
            let memory_end = virtual_address
                .checked_add(memory_size)
                .context("remote session ELF virtual segment range overflow")?;
            let file_backed_end = virtual_address
                .checked_add(file_size)
                .context("remote session ELF file-backed segment range overflow")?;
            if flags & PF_X != 0
                && file_size > 0
                && (virtual_address..memory_end).contains(&entry)
                && (virtual_address..file_backed_end).contains(&entry)
            {
                executable_entry_segment = true;
            }
        }
    }
    ensure!(
        entry != 0 && executable_entry_segment,
        "remote session ELF entrypoint is not inside an executable load segment"
    );
    Ok(runtime_loader)
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn native_linux_elf_machine() -> u16 {
    62
}

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
fn native_linux_elf_machine() -> u16 {
    183
}

#[cfg(all(
    target_os = "linux",
    not(any(target_arch = "x86_64", target_arch = "aarch64"))
))]
fn native_linux_elf_machine() -> u16 {
    0
}

#[cfg(any(target_os = "macos", test))]
fn validate_macos_macho_executable(
    executable: &mut File,
    size_bytes: u64,
    started_at: Instant,
    budget: Duration,
    expected_cpu_type: u32,
    role: NativeExecutableRole,
) -> Result<Option<PathBuf>> {
    const MACHO64_HEADER_BYTES: usize = 32;
    const LC_SEGMENT_64: u32 = 0x19;
    const LC_UNIXTHREAD: u32 = 0x5;
    const LC_LOAD_DYLINKER: u32 = 0xe;
    const LC_MAIN: u32 = 0x8000_0028;
    const VM_PROT_EXECUTE: u32 = 0x4;

    ensure!(
        size_bytes >= MACHO64_HEADER_BYTES as u64,
        "remote session Mach-O executable is truncated"
    );
    let mut header = [0u8; MACHO64_HEADER_BYTES];
    read_exact_at_with_budget(executable, 0, &mut header, started_at, budget)?;
    ensure!(
        header[..4] == [0xcf, 0xfa, 0xed, 0xfe],
        "remote session executable is not a supported thin 64-bit little-endian Mach-O"
    );
    ensure!(
        little_u32(&header, 4)? == expected_cpu_type,
        "remote session Mach-O architecture does not match the host"
    );
    let expected_file_type = match role {
        NativeExecutableRole::Program => 2,
        NativeExecutableRole::RuntimeLoader => 7,
    };
    ensure!(
        little_u32(&header, 12)? == expected_file_type,
        "remote session Mach-O file type does not match its executable role"
    );
    let command_count = little_u32(&header, 16)?;
    let command_bytes = little_u32(&header, 20)?;
    ensure!(
        (1..=4096).contains(&command_count) && command_bytes >= 8,
        "remote session Mach-O load-command shape is invalid"
    );
    let commands_end = (MACHO64_HEADER_BYTES as u64)
        .checked_add(u64::from(command_bytes))
        .context("remote session Mach-O load-command size overflow")?;
    ensure!(
        commands_end <= size_bytes,
        "remote session Mach-O load commands exceed the file"
    );

    let mut offset = MACHO64_HEADER_BYTES as u64;
    let mut executable_file_segments = Vec::new();
    let mut executable_virtual_segments = Vec::new();
    let mut main_entry_offset = None;
    let mut thread_entry_address = None;
    let mut runtime_loader = None;
    for _ in 0..command_count {
        let mut command_header = [0u8; 8];
        read_exact_at_with_budget(executable, offset, &mut command_header, started_at, budget)?;
        let command = little_u32(&command_header, 0)?;
        let command_size = little_u32(&command_header, 4)?;
        ensure!(
            command_size >= 8 && command_size % 8 == 0,
            "remote session Mach-O load command size or alignment is invalid"
        );
        let next = offset
            .checked_add(u64::from(command_size))
            .context("remote session Mach-O load-command offset overflow")?;
        ensure!(
            next <= commands_end,
            "remote session Mach-O load command exceeds the table"
        );
        if command == LC_SEGMENT_64 {
            ensure!(
                command_size >= 72,
                "remote session Mach-O segment command is truncated"
            );
            let mut segment = [0u8; 72];
            read_exact_at_with_budget(executable, offset, &mut segment, started_at, budget)?;
            let virtual_address = little_u64(&segment, 24)?;
            let virtual_size = little_u64(&segment, 32)?;
            let file_offset = little_u64(&segment, 40)?;
            let file_size = little_u64(&segment, 48)?;
            let init_protection = little_u32(&segment, 60)?;
            let section_count = little_u32(&segment, 64)?;
            let expected_command_size = 72u64
                .checked_add(
                    u64::from(section_count)
                        .checked_mul(80)
                        .context("remote session Mach-O section-table size overflow")?,
                )
                .context("remote session Mach-O segment-command size overflow")?;
            ensure!(
                u64::from(command_size) == expected_command_size,
                "remote session Mach-O segment sections do not match command size"
            );
            ensure!(
                file_size <= virtual_size,
                "remote session Mach-O segment file size exceeds virtual size"
            );
            let file_end = file_offset
                .checked_add(file_size)
                .context("remote session Mach-O file segment range overflow")?;
            let virtual_end = virtual_address
                .checked_add(virtual_size)
                .context("remote session Mach-O virtual segment range overflow")?;
            ensure!(
                file_end <= size_bytes,
                "remote session Mach-O segment exceeds the file"
            );
            if init_protection & VM_PROT_EXECUTE != 0 && file_size > 0 {
                executable_file_segments.push((file_offset, file_end));
                executable_virtual_segments.push((virtual_address, virtual_end));
            }
        } else if command == LC_MAIN {
            ensure!(
                role == NativeExecutableRole::Program && main_entry_offset.is_none(),
                "remote session Mach-O has multiple main entry commands"
            );
            ensure!(
                command_size == 24,
                "remote session Mach-O main command has an invalid size"
            );
            let mut main = [0u8; 24];
            read_exact_at_with_budget(executable, offset, &mut main, started_at, budget)?;
            main_entry_offset = Some(little_u64(&main, 8)?);
        } else if command == LC_UNIXTHREAD {
            ensure!(
                role == NativeExecutableRole::RuntimeLoader && thread_entry_address.is_none(),
                "remote session Mach-O thread entry is not valid for this executable role"
            );
            ensure!(
                command_size <= 4096,
                "remote session Mach-O thread entry command exceeds its budget"
            );
            let mut thread = vec![0u8; command_size as usize];
            read_exact_at_with_budget(executable, offset, &mut thread, started_at, budget)?;
            thread_entry_address = Some(macho_unixthread_program_counter(
                &thread,
                expected_cpu_type,
            )?);
        } else if command == LC_LOAD_DYLINKER {
            ensure!(
                runtime_loader.is_none(),
                "remote session Mach-O has multiple runtime loaders"
            );
            ensure!(
                (12..=4096).contains(&command_size),
                "remote session Mach-O runtime-loader command is invalid"
            );
            let mut dylinker = vec![0u8; command_size as usize];
            read_exact_at_with_budget(executable, offset, &mut dylinker, started_at, budget)?;
            let name_offset = little_u32(&dylinker, 8)? as usize;
            ensure!(
                (12..dylinker.len()).contains(&name_offset),
                "remote session Mach-O runtime-loader name offset is invalid"
            );
            let encoded = &dylinker[name_offset..];
            let nul = encoded
                .iter()
                .position(|byte| *byte == 0)
                .context("remote session Mach-O runtime-loader path is not terminated")?;
            let encoded = &encoded[..nul];
            ensure!(
                encoded.first() == Some(&b'/')
                    && !encoded
                        .iter()
                        .any(|byte| byte.is_ascii_whitespace() || *byte == 0),
                "remote session Mach-O runtime loader must be one absolute path"
            );
            let runtime_loader_path = std::str::from_utf8(encoded)
                .context("remote session Mach-O runtime-loader path is not UTF-8")?;
            runtime_loader = Some(PathBuf::from(runtime_loader_path));
        }
        offset = next;
    }
    ensure!(
        offset == commands_end,
        "remote session Mach-O load-command table is incomplete"
    );
    match role {
        NativeExecutableRole::Program => {
            let entry_offset =
                main_entry_offset.context("remote session Mach-O has no main entry command")?;
            ensure!(
                executable_file_segments
                    .iter()
                    .any(|(start, end)| (*start..*end).contains(&entry_offset)),
                "remote session Mach-O entrypoint is not inside a file-backed executable segment"
            );
        }
        NativeExecutableRole::RuntimeLoader => {
            let entry_address = thread_entry_address
                .context("remote session Mach-O runtime loader has no thread entry command")?;
            ensure!(
                executable_virtual_segments
                    .iter()
                    .any(|(start, end)| (*start..*end).contains(&entry_address)),
                "remote session Mach-O runtime-loader entry is not inside an executable segment"
            );
        }
    }
    Ok(runtime_loader)
}

#[cfg(any(target_os = "macos", test))]
fn macho_unixthread_program_counter(command: &[u8], expected_cpu_type: u32) -> Result<u64> {
    ensure!(
        command.len() >= 16,
        "remote session Mach-O thread command is truncated"
    );
    let flavor = little_u32(command, 8)?;
    let count = little_u32(command, 12)?;
    let (expected_flavor, expected_count, program_counter_offset) = match expected_cpu_type {
        0x0100_0007 => (4, 42, 144usize),
        0x0100_000c => (6, 68, 272usize),
        _ => bail!("remote session Mach-O thread architecture is unsupported"),
    };
    let expected_size = 16usize
        .checked_add(
            (count as usize)
                .checked_mul(4)
                .context("remote session Mach-O thread-state size overflow")?,
        )
        .context("remote session Mach-O thread-command size overflow")?;
    ensure!(
        flavor == expected_flavor && count == expected_count && command.len() == expected_size,
        "remote session Mach-O thread state does not match the host architecture"
    );
    little_u64(command, program_counter_offset)
}

#[cfg(all(target_os = "macos", target_arch = "x86_64"))]
fn native_macos_cpu_type() -> u32 {
    0x0100_0007
}

#[cfg(all(target_os = "macos", target_arch = "aarch64"))]
fn native_macos_cpu_type() -> u32 {
    0x0100_000c
}

#[cfg(all(
    target_os = "macos",
    not(any(target_arch = "x86_64", target_arch = "aarch64"))
))]
fn native_macos_cpu_type() -> u32 {
    0
}

fn read_exact_at_with_budget(
    file: &mut File,
    offset: u64,
    buffer: &mut [u8],
    started_at: Instant,
    budget: Duration,
) -> Result<()> {
    ensure!(
        started_at.elapsed() <= budget,
        "remote session executable exceeded its validation time budget"
    );
    file.seek(SeekFrom::Start(offset))?;
    let mut filled = 0usize;
    while filled < buffer.len() {
        ensure!(
            started_at.elapsed() <= budget,
            "remote session executable exceeded its validation time budget"
        );
        let read = file.read(&mut buffer[filled..])?;
        ensure!(
            started_at.elapsed() <= budget,
            "remote session executable exceeded its validation time budget"
        );
        ensure!(read > 0, "remote session executable is truncated");
        filled = filled
            .checked_add(read)
            .context("remote session executable validation byte count overflow")?;
    }
    Ok(())
}

fn little_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let value: [u8; 2] = bytes
        .get(offset..offset + 2)
        .context("native executable u16 field is truncated")?
        .try_into()
        .expect("validated u16 slice length");
    Ok(u16::from_le_bytes(value))
}

fn little_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let value: [u8; 4] = bytes
        .get(offset..offset + 4)
        .context("native executable u32 field is truncated")?
        .try_into()
        .expect("validated u32 slice length");
    Ok(u32::from_le_bytes(value))
}

fn little_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let value: [u8; 8] = bytes
        .get(offset..offset + 8)
        .context("native executable u64 field is truncated")?
        .try_into()
        .expect("validated u64 slice length");
    Ok(u64::from_le_bytes(value))
}

fn ensure_supported_local_remote_session_filesystem(file: &File) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        let filesystem_type = nix::sys::statfs::fstatfs(file)?.filesystem_type().0 as u64;
        ensure!(
            linux_remote_session_filesystem_is_supported_local(filesystem_type),
            "remote session executable must be on a supported local filesystem"
        );
    }

    #[cfg(target_os = "macos")]
    {
        let filesystem = nix::sys::statfs::fstatfs(file)?;
        let name = filesystem.filesystem_type_name().to_ascii_lowercase();
        ensure!(
            matches!(name.as_str(), "apfs" | "hfs"),
            "remote session executable must be on a supported local filesystem"
        );
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    bail!("local executable filesystem validation is unsupported on this platform");

    Ok(())
}

#[cfg(target_os = "linux")]
fn linux_remote_session_filesystem_is_supported_local(filesystem_type: u64) -> bool {
    matches!(
        filesystem_type,
        0x0000_ef53 // ext2, ext3, ext4
            | 0x9123_683e // btrfs
            | 0x5846_5342 // xfs
            | 0x0102_1994 // tmpfs
            | 0x794c_7630 // overlayfs
            | 0xf2f5_2010 // f2fs
            | 0x7371_7368 // squashfs
            | 0x2fc1_2fc1 // zfs
    )
}

pub fn parse_remote_session_policy_strict(bytes: &[u8]) -> Result<RemoteSessionPolicyV1> {
    let policy: RemoteSessionPolicyV1 =
        serde_json::from_slice(bytes).context("parse strict remote session policy JSON")?;
    policy.validate()?;
    Ok(policy)
}

pub fn load_remote_session_policy_for_inspection(
    path: &Path,
) -> Result<LoadedRemoteSessionPolicyV1> {
    let absolute = normalize_absolute_path(path)?;
    let parent = absolute
        .parent()
        .context("remote session policy path has no parent")?;
    let name = absolute
        .file_name()
        .map(Path::new)
        .context("remote session policy path has no file name")?;
    let root = crate::process::open_bounded_root(parent)
        .with_context(|| format!("open remote session policy parent {}", parent.display()))?;
    let root_metadata = root.metadata()?;
    ensure_policy_parent_authority(&root_metadata)?;
    let root_identity = crate::process::filesystem_file_identity(&root_metadata)
        .context("remote session policy parent has no filesystem identity")?;
    let active_parent = std::fs::symlink_metadata(parent)?;
    ensure!(
        crate::process::filesystem_file_identity(&active_parent) == Some(root_identity),
        "remote session policy parent is not the exact active directory"
    );
    ensure_policy_parent_authority(&active_parent)?;
    let mut file = crate::process::open_bounded_regular_file(&root, name)
        .with_context(|| format!("open remote session policy {}", absolute.display()))?;
    let before = file.metadata()?;
    let source_file_snapshot = crate::process::FilesystemObjectSnapshot::from_metadata(&before);
    ensure_policy_file_authority(&before)?;
    ensure!(
        before.len() <= REMOTE_SESSION_POLICY_MAX_BYTES,
        "remote session policy exceeds its byte budget"
    );
    let file_identity = crate::process::filesystem_file_identity(&before)
        .context("remote session policy has no filesystem identity")?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(REMOTE_SESSION_POLICY_MAX_BYTES + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= REMOTE_SESSION_POLICY_MAX_BYTES,
        "remote session policy grew beyond its byte budget"
    );
    let after = file.metadata()?;
    let reopened = crate::process::open_bounded_regular_file(&root, name)
        .context("reopen remote session policy")?;
    let reopened_parent =
        crate::process::open_bounded_root(parent).context("reopen remote session policy parent")?;
    let reopened_parent_metadata = reopened_parent.metadata()?;
    let active_parent = std::fs::symlink_metadata(parent)?;
    let active = std::fs::symlink_metadata(&absolute)?;
    ensure!(
        crate::process::FilesystemObjectSnapshot::from_metadata(&before)
            == crate::process::FilesystemObjectSnapshot::from_metadata(&after)
            && crate::process::filesystem_file_identity(&reopened.metadata()?)
                == Some(file_identity)
            && crate::process::filesystem_file_identity(&active) == Some(file_identity)
            && crate::process::filesystem_file_identity(&root.metadata()?) == Some(root_identity)
            && crate::process::filesystem_file_identity(&reopened_parent_metadata)
                == Some(root_identity)
            && crate::process::filesystem_file_identity(&active_parent) == Some(root_identity),
        "remote session policy changed while reading"
    );
    ensure_policy_parent_authority(&root.metadata()?)?;
    ensure_policy_parent_authority(&reopened_parent_metadata)?;
    ensure_policy_parent_authority(&active_parent)?;
    ensure_policy_file_authority(&reopened.metadata()?)?;

    let policy = parse_remote_session_policy_strict(&bytes)?;
    let policy_sha256 = remote_session_policy_sha256(&policy)?;
    let final_file = crate::process::open_bounded_regular_file(&root, name)
        .context("final reopen of remote session policy")?;
    let final_file_metadata = final_file.metadata()?;
    let final_parent = crate::process::open_bounded_root(parent)
        .context("final reopen of remote session policy parent")?;
    let final_parent_metadata = final_parent.metadata()?;
    let final_active_parent = std::fs::symlink_metadata(parent)?;
    let final_active = std::fs::symlink_metadata(&absolute)?;
    ensure!(
        crate::process::filesystem_file_identity(&file.metadata()?) == Some(file_identity)
            && crate::process::filesystem_file_identity(&final_file_metadata)
                == Some(file_identity)
            && crate::process::filesystem_file_identity(&final_active) == Some(file_identity)
            && crate::process::FilesystemObjectSnapshot::from_metadata(&final_file_metadata)
                == crate::process::FilesystemObjectSnapshot::from_metadata(&before)
            && crate::process::filesystem_file_identity(&root.metadata()?) == Some(root_identity)
            && crate::process::filesystem_file_identity(&final_parent_metadata)
                == Some(root_identity)
            && crate::process::filesystem_file_identity(&final_active_parent)
                == Some(root_identity),
        "remote session policy changed after validation"
    );
    ensure_policy_file_authority(&final_file_metadata)?;
    ensure_policy_parent_authority(&root.metadata()?)?;
    ensure_policy_parent_authority(&final_parent_metadata)?;
    ensure_policy_parent_authority(&final_active_parent)?;
    Ok(LoadedRemoteSessionPolicyV1 {
        policy_sha256,
        source_file_sha256: hash_bytes(&[], &bytes),
        source_size_bytes: bytes.len() as u64,
        policy,
        source_path: absolute,
        source_parent: root,
        source_file: file,
        source_parent_identity: root_identity,
        source_file_identity: file_identity,
        source_file_snapshot,
    })
}

pub fn remote_session_policy_sha256(policy: &RemoteSessionPolicyV1) -> Result<String> {
    policy.validate()?;
    hash_serializable(POLICY_HASH_DOMAIN, policy)
}

pub fn remote_session_launch_profile_sha256(
    profile: &RemoteSessionLaunchProfileV1,
) -> Result<String> {
    profile.validate()?;
    hash_serializable(LAUNCH_PROFILE_HASH_DOMAIN, profile)
}

pub fn remote_session_policy_harness_launch_sha256(
    profile: &RemoteSessionLaunchProfileV1,
) -> Result<String> {
    profile.validate()?;
    #[derive(Serialize)]
    struct PolicyHarnessLaunchMaterial<'a> {
        harness: RemoteSessionHarnessV1,
        executable_path: &'a str,
        fixed_argv: &'a [String],
        fixed_environment: &'a [RemoteSessionEnvironmentEntryV1],
        yolo_mode: bool,
    }

    hash_serializable(
        POLICY_HARNESS_LAUNCH_HASH_DOMAIN,
        &PolicyHarnessLaunchMaterial {
            harness: profile.harness,
            executable_path: &profile.executable_path,
            fixed_argv: &profile.fixed_argv,
            fixed_environment: &profile.fixed_environment,
            yolo_mode: profile.yolo_mode,
        },
    )
}

pub fn remote_session_policy_launch_command(
    profile: &RemoteSessionLaunchProfileV1,
) -> Result<String> {
    profile.validate()?;
    Ok(profile
        .fixed_argv
        .iter()
        .map(|argument| shell_quote_remote_session_argument(argument))
        .collect::<Vec<_>>()
        .join(" "))
}

pub fn remote_session_preview_sha256(preview: &RemoteSessionLaunchPreviewV1) -> Result<String> {
    preview.validate()?;
    hash_serializable(PREVIEW_HASH_DOMAIN, preview)
}

pub fn remote_session_request_sha256(request: &RemoteSessionLaunchRequestV1) -> Result<String> {
    request.validate()?;
    hash_serializable(REQUEST_HASH_DOMAIN, request)
}

pub fn remote_session_reservation_sha256(
    reservation: &RemoteSessionLaunchReservationV1,
) -> Result<String> {
    reservation.validate()?;
    hash_serializable(RESERVATION_HASH_DOMAIN, reservation)
}

pub fn remote_session_receipt_sha256(receipt: &RemoteSessionLaunchReceiptV1) -> Result<String> {
    receipt.validate()?;
    hash_serializable(RECEIPT_HASH_DOMAIN, receipt)
}

pub fn remote_session_cleanup_receipt_sha256(
    receipt: &RemoteSessionCleanupReceiptV1,
) -> Result<String> {
    receipt.validate()?;
    hash_serializable(CLEANUP_RECEIPT_HASH_DOMAIN, receipt)
}

pub fn remote_session_identity_sha256(identity: &RemoteSessionIdentityV1) -> Result<String> {
    validate_canonical_remote_session_identity(identity)?;
    hash_serializable(SESSION_IDENTITY_HASH_DOMAIN, identity)
}

pub fn validate_remote_session_reservation_transition(
    before: &RemoteSessionLaunchReservationV1,
    after: &RemoteSessionLaunchReservationV1,
) -> Result<()> {
    before.validate()?;
    after.validate()?;
    let expected_sequence = before
        .sequence
        .checked_add(1)
        .context("remote session reservation sequence overflow")?;
    ensure!(
        after.sequence == expected_sequence
            && after.updated_at.as_datetime() > before.updated_at.as_datetime(),
        "remote session reservation transition sequence or time is invalid"
    );
    let mut before_identity = serde_json::to_value(before)?;
    let mut after_identity = serde_json::to_value(after)?;
    for value in [&mut before_identity, &mut after_identity] {
        let object = value
            .as_object_mut()
            .context("serialize remote session reservation identity")?;
        object.remove("sequence");
        object.remove("state");
        object.remove("updated_at");
        object.remove("worktree_identity_sha256");
        object.remove("release_origin");
    }
    ensure!(
        before_identity == after_identity,
        "remote session reservation immutable identity changed"
    );
    let allowed = matches!(
        (before.state, after.state),
        (
            RemoteSessionReservationStateV1::Reserved,
            RemoteSessionReservationStateV1::SessionRowCommitted
                | RemoteSessionReservationStateV1::RecoveryRequired
                | RemoteSessionReservationStateV1::Released
        ) | (
            RemoteSessionReservationStateV1::SessionRowCommitted,
            RemoteSessionReservationStateV1::TmuxStarted
                | RemoteSessionReservationStateV1::RecoveryRequired
        ) | (
            RemoteSessionReservationStateV1::TmuxStarted,
            RemoteSessionReservationStateV1::IdentityBound
                | RemoteSessionReservationStateV1::RecoveryRequired
        ) | (
            RemoteSessionReservationStateV1::IdentityBound,
            RemoteSessionReservationStateV1::Completed
                | RemoteSessionReservationStateV1::RecoveryRequired
        ) | (
            RemoteSessionReservationStateV1::RecoveryRequired,
            RemoteSessionReservationStateV1::Released
        ) | (
            RemoteSessionReservationStateV1::Completed,
            RemoteSessionReservationStateV1::Released
        )
    );
    ensure!(allowed, "remote session reservation transition is invalid");
    ensure!(
        after.release_origin
            == (after.state == RemoteSessionReservationStateV1::Released).then_some(before.state),
        "remote session reservation release origin does not match its transition"
    );
    match (
        before.worktree_identity_sha256.as_deref(),
        after.worktree_identity_sha256.as_deref(),
    ) {
        (Some(before), Some(after)) => ensure!(
            before == after,
            "remote session reservation worktree identity changed"
        ),
        (Some(_), None) => bail!("remote session reservation lost its worktree identity"),
        (None, Some(_)) => ensure!(
            before.state == RemoteSessionReservationStateV1::Reserved
                && matches!(
                    after.state,
                    RemoteSessionReservationStateV1::SessionRowCommitted
                        | RemoteSessionReservationStateV1::RecoveryRequired
                ),
            "remote session reservation bound its worktree identity at an invalid transition"
        ),
        (None, None) => {}
    }
    Ok(())
}

pub fn validate_remote_session_contract_chain(
    policy: &RemoteSessionPolicyV1,
    preview: &RemoteSessionLaunchPreviewV1,
    request: &RemoteSessionLaunchRequestV1,
    reservation: Option<&RemoteSessionLaunchReservationV1>,
    receipt: &RemoteSessionLaunchReceiptV1,
    original_receipt: Option<&RemoteSessionLaunchReceiptV1>,
    cleanup_receipt: Option<&RemoteSessionCleanupReceiptV1>,
) -> Result<()> {
    policy.validate()?;
    preview.validate()?;
    request.validate()?;
    receipt.validate()?;
    ensure!(
        policy.enabled,
        "disabled remote session policy cannot authorize a preview"
    );
    ensure!(
        request.preview == *preview,
        "remote session request does not contain the supplied preview"
    );
    let preview_duration = preview
        .expires_at
        .as_datetime()
        .signed_duration_since(preview.created_at.as_datetime());
    ensure!(
        preview_duration == chrono::Duration::seconds(i64::from(policy.request_ttl_seconds)),
        "remote session preview expiry does not match policy TTL"
    );
    let root = policy.allowed_root(&preview.root_id)?;
    let launch_profile = policy.launch_profile(&preview.launch_profile_id)?;
    ensure!(
        policy.policy_id == preview.policy_id
            && root.project_key == preview.project_key
            && root
                .allowed_launch_profile_ids
                .contains(&preview.launch_profile_id)
            && launch_profile.session_kind == preview.session_kind
            && launch_profile.harness == preview.harness
            && launch_profile.yolo_mode == preview.yolo_mode
            && launch_profile.worktree_policy == preview.worktree_policy
            && policy.capacity.max_remote_active_global
                == preview.capacity_observation.max_remote_active_global
            && policy.capacity.max_remote_active_per_root
                == preview.capacity_observation.max_remote_active_per_root
            && remote_session_policy_sha256(policy)? == preview.policy_sha256
            && remote_session_launch_profile_sha256(launch_profile)?
                == preview.launch_profile_sha256
            && remote_session_policy_harness_launch_sha256(launch_profile)?
                == preview.harness_launch_sha256,
        "remote session preview is not exactly derived from policy"
    );
    let request_sha256 = remote_session_request_sha256(request)?;
    ensure!(
        receipt.request_id == request.request_id && receipt.request_sha256 == request_sha256,
        "remote session receipt does not bind the exact request"
    );
    ensure!(
        receipt.created_at.as_datetime() >= request.created_at.as_datetime(),
        "remote session receipt predates its request"
    );

    match (reservation, receipt.reservation_id.as_deref()) {
        (Some(reservation), Some(receipt_reservation_id)) => {
            reservation.validate()?;
            let reservation_sha256 = remote_session_reservation_sha256(reservation)?;
            ensure!(
                reservation.reservation_id == receipt_reservation_id
                    && receipt.reservation_sha256.as_deref() == Some(reservation_sha256.as_str())
                    && reservation.request_id == request.request_id
                    && reservation.request_sha256 == request_sha256
                    && reservation.policy_id == preview.policy_id
                    && reservation.policy_sha256 == preview.policy_sha256
                    && reservation.root_id == preview.root_id
                    && reservation.project_key == preview.project_key
                    && reservation.project_root_identity_sha256
                        == preview.initial_project_root_identity_sha256
                    && reservation.launch_profile_id == preview.launch_profile_id
                    && reservation.launch_profile_sha256 == preview.launch_profile_sha256
                    && reservation.worktree_policy == preview.worktree_policy
                    && reservation.capacity_observation.max_remote_active_global
                        == policy.capacity.max_remote_active_global
                    && reservation.capacity_observation.max_remote_active_per_root
                        == policy.capacity.max_remote_active_per_root
                    && reservation.created_at.as_datetime() >= request.created_at.as_datetime()
                    && reservation.created_at.as_datetime() < request.expires_at.as_datetime()
                    && reservation.expires_at.as_datetime()
                        == &request
                            .expires_at
                            .as_datetime()
                            .checked_add_signed(chrono::Duration::seconds(i64::from(
                                policy.request_ttl_seconds,
                            )))
                            .context("remote session reservation expiry overflow")?,
                "remote session reservation does not bind the exact request and policy"
            );
            if preview.resolved_disposition == RemoteSessionResolvedDispositionV1::Resume {
                ensure!(
                    preview.matching_session_id.as_deref()
                        == Some(reservation.intended_session_id.as_str())
                        && preview.matching_worktree_identity_sha256.as_deref()
                            == reservation.worktree_identity_sha256.as_deref(),
                    "remote session reservation does not bind the exact resume match"
                );
            }
            ensure!(
                receipt.created_at.as_datetime() >= reservation.updated_at.as_datetime(),
                "remote session receipt predates its reservation state"
            );
        }
        (None, None) => {}
        _ => bail!("remote session receipt and supplied reservation disagree"),
    }

    match (receipt.result, original_receipt) {
        (RemoteSessionLaunchResultV1::DuplicateReplay, Some(original)) => {
            validate_remote_session_duplicate_replay(original, receipt)?;
        }
        (RemoteSessionLaunchResultV1::DuplicateReplay, None) => {
            bail!("duplicate remote session receipt has no supplied original receipt")
        }
        (_, Some(_)) => bail!("non-duplicate remote session receipt supplied replay evidence"),
        (_, None) => {}
    }
    match (receipt.result, cleanup_receipt) {
        (RemoteSessionLaunchResultV1::FailedClean, Some(_)) => {}
        (RemoteSessionLaunchResultV1::FailedClean, None) => {
            bail!("clean failure receipt has no supplied cleanup receipt")
        }
        (_, Some(_)) => bail!("non-clean-failure receipt supplied cleanup evidence"),
        (_, None) => {}
    }

    match receipt.result {
        RemoteSessionLaunchResultV1::Created => ensure!(
            preview.resolved_disposition == RemoteSessionResolvedDispositionV1::New,
            "created remote session receipt contradicts preview disposition"
        ),
        RemoteSessionLaunchResultV1::Resumed => ensure!(
            preview.resolved_disposition == RemoteSessionResolvedDispositionV1::Resume,
            "resumed remote session receipt contradicts preview disposition"
        ),
        RemoteSessionLaunchResultV1::AlreadyRunning => ensure!(
            preview.resolved_disposition == RemoteSessionResolvedDispositionV1::AlreadyRunning,
            "already-running receipt contradicts preview disposition"
        ),
        _ => {}
    }

    if matches!(
        receipt.result,
        RemoteSessionLaunchResultV1::Created | RemoteSessionLaunchResultV1::Resumed
    ) {
        let reservation =
            reservation.context("successful remote session receipt has no reservation")?;
        ensure!(
            matches!(
                reservation.state,
                RemoteSessionReservationStateV1::Completed
            ),
            "successful remote session receipt requires a completed reservation"
        );
        validate_receipt_session_identity(preview, reservation, receipt)?;
        ensure!(
            receipt.created_at.as_datetime() < reservation.expires_at.as_datetime(),
            "successful remote session receipt was issued after reservation expiry"
        );
        if receipt.result == RemoteSessionLaunchResultV1::Resumed {
            ensure!(
                preview.matching_session_id.as_deref()
                    == Some(reservation.intended_session_id.as_str()),
                "resumed remote session receipt does not bind the preview session"
            );
        }
    } else if receipt.result == RemoteSessionLaunchResultV1::AlreadyRunning {
        let identity = receipt
            .remote_session_identity
            .as_ref()
            .context("already-running receipt has no session identity")?;
        ensure!(
            receipt.session_id.as_deref() == Some(identity.session_id.as_str())
                && preview.matching_session_id.as_deref() == Some(identity.session_id.as_str()),
            "already-running receipt does not bind the preview session"
        );
        validate_identity_against_preview(preview, identity)?;
    } else if receipt.result == RemoteSessionLaunchResultV1::RecoveryRequired {
        let reservation = reservation.context("recovery receipt has no reservation")?;
        ensure!(
            reservation.state == RemoteSessionReservationStateV1::RecoveryRequired,
            "recovery receipt requires a recovery-required reservation"
        );
        if receipt.remote_session_identity.is_some() {
            validate_receipt_session_identity(preview, reservation, receipt)?;
        }
    } else if receipt.result == RemoteSessionLaunchResultV1::FailedClean {
        let reservation = reservation.context("clean failure receipt has no reservation")?;
        let cleanup_receipt = cleanup_receipt.expect("cleanup receipt presence checked above");
        cleanup_receipt.validate()?;
        let reservation_sha256 = remote_session_reservation_sha256(reservation)?;
        let cleanup_receipt_sha256 = remote_session_cleanup_receipt_sha256(cleanup_receipt)?;
        ensure!(
            reservation.state == RemoteSessionReservationStateV1::Released
                && matches!(
                    reservation.release_origin,
                    Some(
                        RemoteSessionReservationStateV1::Reserved
                            | RemoteSessionReservationStateV1::RecoveryRequired
                    )
                )
                && cleanup_receipt.cleanup_receipt_id != receipt.receipt_id
                && receipt.cleanup_receipt_id.as_deref()
                    == Some(cleanup_receipt.cleanup_receipt_id.as_str())
                && receipt.cleanup_receipt_sha256.as_deref()
                    == Some(cleanup_receipt_sha256.as_str())
                && cleanup_receipt.request_id == request.request_id
                && cleanup_receipt.request_sha256 == request_sha256
                && cleanup_receipt.reservation_id == reservation.reservation_id
                && cleanup_receipt.reservation_sha256 == reservation_sha256
                && cleanup_receipt.intended_session_id == reservation.intended_session_id
                && cleanup_receipt.created_at.as_datetime() >= reservation.updated_at.as_datetime()
                && receipt.created_at.as_datetime() >= cleanup_receipt.created_at.as_datetime(),
            "clean failure receipt does not bind exact released cleanup evidence"
        );
        let worktree_outcome_matches = match reservation.worktree_policy {
            RemoteSessionWorktreePolicyV1::ManagedWorktreeRequired => {
                if preview.resolved_disposition == RemoteSessionResolvedDispositionV1::Resume {
                    cleanup_receipt.worktree_outcome
                        == RemoteSessionCleanupWorktreeOutcomeV1::PreservedExistingWorktree
                } else if reservation.worktree_identity_sha256.is_some() {
                    cleanup_receipt.worktree_outcome
                        == RemoteSessionCleanupWorktreeOutcomeV1::RemovedPristine
                } else {
                    cleanup_receipt.worktree_outcome
                        == RemoteSessionCleanupWorktreeOutcomeV1::NotCreated
                }
            }
            RemoteSessionWorktreePolicyV1::DirectExistingRoot => {
                cleanup_receipt.worktree_outcome
                    == RemoteSessionCleanupWorktreeOutcomeV1::PreservedDirectRoot
            }
        };
        ensure!(
            worktree_outcome_matches,
            "remote session cleanup worktree outcome contradicts its reservation"
        );
    }
    Ok(())
}

pub fn validate_remote_session_duplicate_replay(
    original: &RemoteSessionLaunchReceiptV1,
    replay: &RemoteSessionLaunchReceiptV1,
) -> Result<()> {
    original.validate()?;
    replay.validate()?;
    let original_sha256 = remote_session_receipt_sha256(original)?;
    ensure!(
        replay.result == RemoteSessionLaunchResultV1::DuplicateReplay
            && original.result != RemoteSessionLaunchResultV1::DuplicateReplay
            && replay.receipt_id != original.receipt_id
            && replay.original_receipt_id.as_deref() == Some(original.receipt_id.as_str())
            && replay.original_receipt_sha256.as_deref() == Some(original_sha256.as_str())
            && replay.request_id == original.request_id
            && replay.request_sha256 == original.request_sha256
            && replay.created_at.as_datetime() >= original.created_at.as_datetime(),
        "duplicate remote session receipt does not bind its exact original receipt"
    );
    Ok(())
}

fn validate_receipt_session_identity(
    preview: &RemoteSessionLaunchPreviewV1,
    reservation: &RemoteSessionLaunchReservationV1,
    receipt: &RemoteSessionLaunchReceiptV1,
) -> Result<()> {
    let identity = receipt
        .remote_session_identity
        .as_ref()
        .context("successful remote session receipt has no identity")?;
    ensure!(
        receipt.session_id.as_deref() == Some(identity.session_id.as_str())
            && identity.session_id == reservation.intended_session_id
            && reservation.worktree_identity_sha256.as_deref()
                == Some(identity.worktree_identity_sha256.as_str()),
        "remote session receipt, reservation, and live session IDs differ"
    );
    validate_identity_against_preview(preview, identity)
}

fn validate_identity_against_preview(
    preview: &RemoteSessionLaunchPreviewV1,
    identity: &RemoteSessionIdentityV1,
) -> Result<()> {
    let expected_worktree_kind = match preview.worktree_policy {
        RemoteSessionWorktreePolicyV1::ManagedWorktreeRequired => {
            RemoteSessionWorktreeKindV1::ManagedWorktree
        }
        RemoteSessionWorktreePolicyV1::DirectExistingRoot => {
            RemoteSessionWorktreeKindV1::DirectProjectRoot
        }
    };
    ensure!(
        identity.machine_identity_sha256 == preview.machine_identity_sha256
            && identity.project_key.as_deref() == Some(preview.project_key.as_str())
            && identity.project_root_identity_sha256
                == preview.initial_project_root_identity_sha256
            && identity.worktree_kind == expected_worktree_kind
            && identity.harness_kind == preview.harness.as_str()
            && identity.harness_launch_sha256 == preview.harness_launch_sha256
            && preview
                .matching_worktree_identity_sha256
                .as_deref()
                .is_none_or(|expected| expected == identity.worktree_identity_sha256),
        "remote session live identity does not match its preview"
    );
    Ok(())
}

fn validate_canonical_remote_session_identity(identity: &RemoteSessionIdentityV1) -> Result<()> {
    identity.validate()?;
    for (label, value) in [
        ("machine_identity_sha256", &identity.machine_identity_sha256),
        (
            "project_root_identity_sha256",
            &identity.project_root_identity_sha256,
        ),
        (
            "worktree_identity_sha256",
            &identity.worktree_identity_sha256,
        ),
        ("harness_launch_sha256", &identity.harness_launch_sha256),
        (
            "session_record_identity_sha256",
            &identity.session_record_identity_sha256,
        ),
        (
            "transport_session_identity_sha256",
            &identity.transport_session_identity_sha256,
        ),
        ("pane_identity_sha256", &identity.pane_identity_sha256),
        ("process_identity_sha256", &identity.process_identity_sha256),
        ("binding_sha256", &identity.binding_sha256),
    ] {
        validate_sha256(value, label)?;
    }
    Ok(())
}

fn validate_disposition(
    requested: RemoteSessionRequestedDispositionV1,
    resolved: RemoteSessionResolvedDispositionV1,
    matching_session_id: Option<&str>,
    matching_worktree_identity_sha256: Option<&str>,
) -> Result<()> {
    if let Some(session_id) = matching_session_id {
        validate_identifier(session_id, "remote session matching_session_id")?;
    }
    if let Some(identity) = matching_worktree_identity_sha256 {
        validate_sha256(identity, "remote session matching worktree identity")?;
    }
    ensure!(
        matching_session_id.is_some() == matching_worktree_identity_sha256.is_some(),
        "remote session matching session identity is partial"
    );
    let valid = matches!(
        (requested, resolved, matching_session_id.is_some()),
        (
            RemoteSessionRequestedDispositionV1::New,
            RemoteSessionResolvedDispositionV1::New,
            false,
        ) | (
            RemoteSessionRequestedDispositionV1::Resume,
            RemoteSessionResolvedDispositionV1::Resume,
            true,
        ) | (
            RemoteSessionRequestedDispositionV1::Auto,
            RemoteSessionResolvedDispositionV1::New,
            false,
        ) | (
            RemoteSessionRequestedDispositionV1::Auto,
            RemoteSessionResolvedDispositionV1::Resume,
            true,
        ) | (
            RemoteSessionRequestedDispositionV1::Auto,
            RemoteSessionResolvedDispositionV1::AlreadyRunning,
            true,
        )
    );
    ensure!(
        valid,
        "remote session requested and resolved dispositions conflict"
    );
    Ok(())
}

fn validate_identifier(value: &str, label: &str) -> Result<()> {
    let bytes = value.as_bytes();
    ensure!(
        !value.is_empty()
            && value.len() <= MAX_ID_CHARS
            && value == value.trim()
            && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
            && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
            && !value.contains("..")
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
            && crate::offdesk::operator_safe_text(value) == value,
        "{label} is not a canonical identifier"
    );
    Ok(())
}

fn shell_quote_remote_session_argument(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn validate_unique_identifiers(values: &[String], label: &str) -> Result<()> {
    let mut seen = BTreeSet::new();
    for value in values {
        validate_identifier(value, label)?;
        ensure!(seen.insert(value.as_str()), "{label} contains duplicates");
    }
    Ok(())
}

fn validate_optional_identifier(value: Option<&str>, label: &str) -> Result<()> {
    if let Some(value) = value {
        validate_identifier(value, label)?;
    }
    Ok(())
}

fn validate_safe_text(value: &str, max_chars: usize, label: &str, allow_empty: bool) -> Result<()> {
    ensure!(
        (allow_empty || !value.is_empty())
            && value.chars().count() <= max_chars
            && !value.chars().any(char::is_control),
        "{label} is unsafe or exceeds its character budget"
    );
    Ok(())
}

fn validate_absolute_path(value: &str, label: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= MAX_PATH_CHARS
            && value == value.trim()
            && !value.chars().any(char::is_control),
        "{label} is empty or exceeds its character budget"
    );
    let path = Path::new(value);
    ensure!(
        path.is_absolute() && path != Path::new("/"),
        "{label} must be a bounded absolute path"
    );
    ensure!(
        path.components()
            .all(|component| matches!(component, Component::RootDir | Component::Normal(_))),
        "{label} contains non-canonical path components"
    );
    let mut canonical = PathBuf::from("/");
    for component in path.components() {
        if let Component::Normal(name) = component {
            canonical.push(name);
        }
    }
    ensure!(
        canonical.to_str() == Some(value),
        "{label} must use one canonical absolute spelling"
    );
    if let Some(home) = dirs::home_dir() {
        ensure!(path != home, "{label} cannot be the home directory");
    }
    Ok(())
}

fn validate_sha256(value: &str, label: &str) -> Result<()> {
    ensure!(
        value.len() == 64
            && value
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "{label} must be 64 lowercase hexadecimal characters"
    );
    Ok(())
}

fn unique_item<'a, T>(items: impl Iterator<Item = &'a T>, error: &str) -> Result<&'a T> {
    let matches = items.collect::<Vec<_>>();
    match matches.as_slice() {
        [item] => Ok(*item),
        _ => bail!("{error}"),
    }
}

fn hash_serializable(domain: &[u8], value: &impl Serialize) -> Result<String> {
    Ok(hash_bytes(domain, &serde_json::to_vec(value)?))
}

fn hash_bytes(domain: &[u8], bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn normalize_absolute_path(path: &Path) -> Result<PathBuf> {
    let candidate = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::from("/");
    for component in candidate.components() {
        match component {
            Component::RootDir => normalized = PathBuf::from("/"),
            Component::CurDir => {}
            Component::ParentDir => {
                ensure!(
                    normalized.pop(),
                    "remote session policy path escapes filesystem root"
                );
            }
            Component::Normal(name) => normalized.push(name),
            Component::Prefix(_) => bail!("remote session policy path prefix is unsupported"),
        }
    }
    ensure!(
        normalized != Path::new("/"),
        "remote session policy path cannot be filesystem root"
    );
    Ok(normalized)
}

fn ensure_policy_file_authority(metadata: &std::fs::Metadata) -> Result<()> {
    ensure!(
        metadata.is_file(),
        "remote session policy must be a regular file"
    );
    #[cfg(unix)]
    {
        ensure!(
            metadata.nlink() == 1,
            "remote session policy must have exactly one filesystem link"
        );
        ensure!(
            metadata.mode() & 0o077 == 0,
            "remote session policy must be owner-only"
        );
        ensure!(
            metadata.uid() == nix::unistd::Uid::current().as_raw(),
            "remote session policy must be owned by the current user"
        );
    }
    Ok(())
}

fn ensure_policy_parent_authority(metadata: &std::fs::Metadata) -> Result<()> {
    ensure!(
        metadata.is_dir(),
        "remote session policy parent must be a directory"
    );
    #[cfg(unix)]
    {
        ensure!(
            metadata.mode() & 0o022 == 0,
            "remote session policy parent must not be group or world writable (mode {:o})",
            metadata.mode() & 0o777
        );
        ensure!(
            metadata.uid() == nix::unistd::Uid::current().as_raw(),
            "remote session policy parent must be owned by the current user"
        );
    }
    Ok(())
}

fn ensure_remote_session_directory_authority(
    metadata: &std::fs::Metadata,
    label: &str,
) -> Result<()> {
    ensure!(metadata.is_dir(), "{label} must be a directory");
    #[cfg(unix)]
    {
        ensure!(
            metadata.uid() == nix::unistd::Uid::current().as_raw(),
            "{label} must be owned by the current user"
        );
        ensure!(
            metadata.mode() & 0o022 == 0,
            "{label} must not be group or world writable"
        );
    }
    Ok(())
}

fn ensure_remote_session_executable_authority(metadata: &std::fs::Metadata) -> Result<()> {
    ensure!(
        metadata.is_file(),
        "remote session executable must be a regular file"
    );
    #[cfg(unix)]
    {
        let current = nix::unistd::Uid::current().as_raw();
        ensure!(
            metadata.uid() == current || metadata.uid() == 0,
            "remote session executable must be owned by the current user or root"
        );
        ensure!(
            metadata.nlink() == 1,
            "remote session executable must have exactly one filesystem link"
        );
        ensure!(
            metadata.mode() & 0o022 == 0,
            "remote session executable must not be group or world writable"
        );
        let executable_for_current_user = if metadata.uid() == current {
            metadata.mode() & 0o100 != 0
        } else {
            metadata.mode() & 0o001 != 0
        };
        ensure!(
            executable_for_current_user,
            "remote session executable is not executable by the current user"
        );
    }
    Ok(())
}

fn ensure_remote_session_executable_directory_authority(
    metadata: &std::fs::Metadata,
) -> Result<()> {
    ensure!(
        metadata.is_dir(),
        "remote session executable path component must be a directory"
    );
    #[cfg(unix)]
    {
        let current = nix::unistd::Uid::current().as_raw();
        ensure!(
            metadata.uid() == current || metadata.uid() == 0,
            "remote session executable directory must be owned by the current user or root"
        );
        ensure!(
            metadata.mode() & 0o022 == 0,
            "remote session executable directory must not be group or world writable"
        );
    }
    Ok(())
}

fn ensure_remote_session_executable_symlink_authority(metadata: &std::fs::Metadata) -> Result<()> {
    ensure!(
        metadata.file_type().is_symlink(),
        "remote session executable chain entry must be a symlink"
    );
    #[cfg(unix)]
    {
        let current = nix::unistd::Uid::current().as_raw();
        ensure!(
            metadata.uid() == current || metadata.uid() == 0,
            "remote session executable symlink must be owned by the current user or root"
        );
        ensure!(
            metadata.nlink() == 1,
            "remote session executable symlink must have exactly one filesystem link"
        );
    }
    Ok(())
}

#[cfg(test)]
mod s2b_tests {
    use super::*;

    #[test]
    fn project_root_identity_encoding_is_prefix_free() -> Result<()> {
        assert_eq!(
            format!("{}{}{}", "/p", 1, 123),
            format!("{}{}{}", "/p1", 1, 23)
        );
        assert_ne!(
            remote_session_root_identity_sha256(Path::new("/p"), (1, 123))?,
            remote_session_root_identity_sha256(Path::new("/p1"), (1, 23))?
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn executable_filesystem_classification_is_fail_closed() {
        for local in [
            0x0000_ef53,
            0x9123_683e,
            0x5846_5342,
            0x0102_1994,
            0x794c_7630,
            0xf2f5_2010,
            0x7371_7368,
            0x2fc1_2fc1,
        ] {
            assert!(linux_remote_session_filesystem_is_supported_local(local));
        }
        for remote_or_unknown in [
            0x0000_6969, // nfs
            0x0000_517b, // legacy smbfs
            0xff53_4d42, // cifs
            0xfe53_4d42, // smb2
            0x00c3_6464, // ceph
            0x0102_1997, // 9p
            0x6573_5546, // fuse
            0xdead_beef, // unknown
        ] {
            assert!(!linux_remote_session_filesystem_is_supported_local(
                remote_or_unknown
            ));
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macho_program_and_runtime_loader_chain_revalidates_with_exact_roles() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let boundary = tempfile::tempdir()?;
        let boundary_path = std::fs::canonicalize(boundary.path())?;
        std::fs::set_permissions(&boundary_path, std::fs::Permissions::from_mode(0o700))?;
        let current_executable = std::env::current_exe()?;
        let copied_executable = boundary_path.join("forager-s2b-macho-program");
        std::fs::copy(current_executable, &copied_executable)?;
        std::fs::set_permissions(&copied_executable, std::fs::Permissions::from_mode(0o700))?;
        let boundary_descriptor = crate::process::open_bounded_root(&boundary_path)?;
        let boundary_identity =
            crate::process::filesystem_file_identity(&boundary_descriptor.metadata()?)
                .context("test boundary has no filesystem identity")?;
        let binding = open_remote_session_executable(
            &copied_executable,
            &boundary_path,
            &boundary_descriptor,
            boundary_identity,
        )?;

        ensure!(
            binding.runtime_loader.is_some(),
            "test Mach-O program has no runtime loader"
        );
        binding.ensure_active()?;
        binding.ensure_active()?;
        Ok(())
    }

    #[test]
    fn macho_entrypoint_requires_role_structure_and_aligned_commands() -> Result<()> {
        fn fixture(
            role: NativeExecutableRole,
            entry_command: u32,
            entry_offset: u64,
            executable_bytes: u64,
        ) -> Vec<u8> {
            const HEADER_BYTES: usize = 32;
            const SEGMENT_BYTES: usize = 72;
            let entry_bytes = if entry_command == 0x8000_0028 {
                24usize
            } else {
                184usize
            };
            let mut bytes = vec![0u8; 320];
            bytes[..4].copy_from_slice(&[0xcf, 0xfa, 0xed, 0xfe]);
            bytes[4..8].copy_from_slice(&0x0100_0007u32.to_le_bytes());
            let file_type = match role {
                NativeExecutableRole::Program => 2u32,
                NativeExecutableRole::RuntimeLoader => 7u32,
            };
            bytes[12..16].copy_from_slice(&file_type.to_le_bytes());
            bytes[16..20].copy_from_slice(&2u32.to_le_bytes());
            bytes[20..24].copy_from_slice(&((SEGMENT_BYTES + entry_bytes) as u32).to_le_bytes());

            let segment = HEADER_BYTES;
            bytes[segment..segment + 4].copy_from_slice(&0x19u32.to_le_bytes());
            bytes[segment + 4..segment + 8].copy_from_slice(&(SEGMENT_BYTES as u32).to_le_bytes());
            bytes[segment + 32..segment + 40].copy_from_slice(&executable_bytes.to_le_bytes());
            bytes[segment + 40..segment + 48].copy_from_slice(&0u64.to_le_bytes());
            bytes[segment + 48..segment + 56].copy_from_slice(&executable_bytes.to_le_bytes());
            bytes[segment + 60..segment + 64].copy_from_slice(&4u32.to_le_bytes());

            let entry = HEADER_BYTES + SEGMENT_BYTES;
            bytes[entry..entry + 4].copy_from_slice(&entry_command.to_le_bytes());
            bytes[entry + 4..entry + 8].copy_from_slice(&(entry_bytes as u32).to_le_bytes());
            if entry_bytes == 24 {
                bytes[entry + 8..entry + 16].copy_from_slice(&entry_offset.to_le_bytes());
            } else {
                bytes[entry + 8..entry + 12].copy_from_slice(&4u32.to_le_bytes());
                bytes[entry + 12..entry + 16].copy_from_slice(&42u32.to_le_bytes());
                bytes[entry + 144..entry + 152].copy_from_slice(&entry_offset.to_le_bytes());
            }
            bytes
        }

        fn validate(bytes: &[u8], role: NativeExecutableRole) -> Result<Option<PathBuf>> {
            use std::io::Write;

            let mut file = tempfile::tempfile()?;
            file.write_all(bytes)?;
            validate_macos_macho_executable(
                &mut file,
                bytes.len() as u64,
                Instant::now(),
                Duration::from_secs(1),
                0x0100_0007,
                role,
            )
        }

        assert!(validate(
            &fixture(NativeExecutableRole::Program, 0x5, 128, 320),
            NativeExecutableRole::Program,
        )
        .is_err());
        assert!(validate(
            &fixture(NativeExecutableRole::Program, 0x8000_0028, 128, 64,),
            NativeExecutableRole::Program,
        )
        .is_err());
        assert_eq!(
            validate(
                &fixture(NativeExecutableRole::Program, 0x8000_0028, 128, 320,),
                NativeExecutableRole::Program,
            )?,
            None
        );
        assert_eq!(
            validate(
                &fixture(NativeExecutableRole::RuntimeLoader, 0x5, 128, 320),
                NativeExecutableRole::RuntimeLoader,
            )?,
            None
        );

        let mut misaligned = fixture(NativeExecutableRole::Program, 0x8000_0028, 128, 320);
        misaligned[20..24].copy_from_slice(&97u32.to_le_bytes());
        misaligned[108..112].copy_from_slice(&25u32.to_le_bytes());
        assert!(validate(&misaligned, NativeExecutableRole::Program).is_err());

        let mut mismatched_sections = fixture(NativeExecutableRole::Program, 0x8000_0028, 128, 320);
        mismatched_sections[96..100].copy_from_slice(&1u32.to_le_bytes());
        assert!(validate(&mismatched_sections, NativeExecutableRole::Program).is_err());
        Ok(())
    }
}
