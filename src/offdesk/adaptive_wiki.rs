//! Adaptive wiki records and projections for Offdesk learning.
//!
//! The canonical wiki state is shared, but the AI and human surfaces are
//! intentionally separate. AI projections are compact and redacted. Human
//! projections keep governance context such as evidence refs and review state.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use super::background::{BackgroundProbe, BackgroundRunnerPhase};
use super::redaction::operator_safe_text;
use super::resume::{ResumeStatus, TaskResumeState};
use super::task_queue::{OffdeskTask, OffdeskTaskStatus};

const ADAPTIVE_WIKI_VERSION: &str = "2026-05-14.v0";
const ADAPTIVE_WIKI_ENTRIES_FILE: &str = "adaptive_wiki_entries.json";
const ADAPTIVE_WIKI_CANDIDATES_FILE: &str = "adaptive_wiki_candidates.json";
const ADAPTIVE_WIKI_LOCK_FILE: &str = "adaptive_wiki.lock";
const ADAPTIVE_WIKI_AUDIT_FILE: &str = "adaptive_wiki_audit.jsonl";
const ADAPTIVE_WIKI_USAGE_FILE: &str = "adaptive_wiki_usage.jsonl";
const ADAPTIVE_WIKI_CORRECTIONS_FILE: &str = "adaptive_wiki_corrections.jsonl";
const ADAPTIVE_WIKI_REVIEW_EVENTS_FILE: &str = "adaptive_wiki_review_events.jsonl";
const ADAPTIVE_WIKI_RUNTIME_POLICY_ACKS_FILE: &str =
    "adaptive_wiki_runtime_policy_acknowledgements.jsonl";
const ADAPTIVE_WIKI_MARKDOWN_VAULT_DIR: &str = "wiki-vault";
const ADAPTIVE_WIKI_REVIEW_REPORTS_DIR: &str = "adaptive_wiki_review_reports";
const ADAPTIVE_WIKI_EPISODE_REPORTS_DIR: &str = "adaptive_wiki_episode_reports";
const ADAPTIVE_WIKI_EPISODE_TRACES_DIR: &str = "adaptive_wiki_episode_traces";
const ADAPTIVE_WIKI_RECURRENCE_REPORTS_DIR: &str = "adaptive_wiki_recurrence_reports";
const ADAPTIVE_WIKI_PROMOTION_CHAINS_DIR: &str = "adaptive_wiki_promotion_chains";
const ADAPTIVE_WIKI_PROMOTION_RECEIPTS_DIR: &str = "adaptive_wiki_promotion_receipts";
const ADAPTIVE_WIKI_PROMOTION_RECEIPT_SCHEMA: &str = "adaptive_wiki_promotion_receipt.v1";
const STALE_CANDIDATE_DAYS: i64 = 30;
const DEFAULT_PROJECTION_MAX_ENTRIES: usize = 8;
const DEFAULT_PROJECTION_MAX_CONTEXT_CHARS: usize = 4_000;
const DEFAULT_PROJECTION_MAX_INSTRUCTION_CHARS: usize = 500;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiKind {
    Preference,
    Procedure,
    FailurePattern,
    PolicyRule,
    #[default]
    Fact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiScope {
    Session,
    ArtifactKind,
    Project,
    #[default]
    UserGlobal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiStatus {
    #[default]
    Candidate,
    Promoted,
    Deprecated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiActivationMode {
    ContextOnly,
    #[default]
    Confirm,
    AutoApply,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiAgentMode {
    Planning,
    #[serde(
        alias = "code_development",
        alias = "code-development",
        alias = "code",
        alias = "coding"
    )]
    Development,
    Analysis,
    #[serde(
        alias = "research_writing",
        alias = "research-writing",
        alias = "research",
        alias = "editing"
    )]
    Writing,
    #[serde(alias = "critic")]
    Critique,
    Review,
    Maintenance,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiAgentModeFilter {
    #[default]
    AllWhenUnspecified,
    SharedWhenUnspecified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiConfidence {
    #[default]
    Explicit,
    Repeated,
    Inferred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiSignalKind {
    OperatorCorrection,
    ApprovalDenial,
    Rollback,
    RepeatedFailure,
    ManualPatch,
    ExplicitPreference,
    ImportedDoc,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiOrigin {
    OperatorExplicit,
    RuntimeObserved,
    BackgroundReview,
    Imported,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiCorrectionKind {
    OperatorCorrection,
    Counterexample,
    FailureRecurrence,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiScopeSuggestion {
    #[serde(default)]
    pub scope: AdaptiveWikiScope,
    #[serde(default = "default_scope_ref")]
    pub scope_ref: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiAuditAction {
    Promote,
    Reject,
    Rescope,
    Deprecate,
    AddCounterexample,
    UpdateRunbook,
    RenewReviewAfter,
    Edit,
    Retag,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiAuditRecord {
    pub id: String,
    pub action: AdaptiveWikiAuditAction,
    pub subject_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<String>,
    pub actor: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub before_scope: Option<AdaptiveWikiScopeSuggestion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after_scope: Option<AdaptiveWikiScopeSuggestion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activation_mode: Option<AdaptiveWikiActivationMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_snapshot: Option<AdaptiveWikiHumanCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_snapshot: Option<AdaptiveWikiHumanEntry>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiPromotionReceipt {
    pub schema: String,
    pub receipt_id: String,
    pub generated_at: DateTime<Utc>,
    pub status: String,
    pub read_only_review_artifact: bool,
    pub candidate_id: String,
    pub entry_id: String,
    pub audit_id: String,
    pub actor: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    pub activation_mode: AdaptiveWikiActivationMode,
    pub before_scope: AdaptiveWikiScopeSuggestion,
    pub after_scope: AdaptiveWikiScopeSuggestion,
    pub candidate_snapshot: AdaptiveWikiHumanCandidate,
    pub entry_snapshot: AdaptiveWikiHumanEntry,
    pub authority: AdaptiveWikiPromotionReceiptAuthority,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiPromotionReceiptAuthority {
    pub canonical_mutation_recorded: bool,
    pub does_not_authorize: Vec<String>,
}

impl AdaptiveWikiPromotionReceipt {
    pub fn schema_name() -> &'static str {
        ADAPTIVE_WIKI_PROMOTION_RECEIPT_SCHEMA
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiUsageRecord {
    pub id: String,
    pub entry_id: String,
    pub task_id: String,
    pub request_id: String,
    pub project_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_mode: Option<AdaptiveWikiAgentMode>,
    pub projection_kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projection_policy: Option<AdaptiveWikiProjectionPolicy>,
    pub activation_mode: AdaptiveWikiActivationMode,
    pub created_at: DateTime<Utc>,
}

pub struct AdaptiveWikiUsageContext<'a> {
    pub task_id: &'a str,
    pub request_id: &'a str,
    pub project_key: &'a str,
    pub artifact_kind: Option<&'a str>,
    pub agent_mode: Option<AdaptiveWikiAgentMode>,
    pub projection_kind: &'a str,
    pub projection_policy: Option<AdaptiveWikiProjectionPolicy>,
    pub now: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiCorrectionRecord {
    pub id: String,
    #[serde(default)]
    pub correction_kind: AdaptiveWikiCorrectionKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_kind: Option<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<String>,
    #[serde(default = "default_timestamp")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiRuntimeProjection {
    pub entry_ids: Vec<String>,
    pub context: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiEntry {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub kind: AdaptiveWikiKind,
    #[serde(default)]
    pub scope: AdaptiveWikiScope,
    #[serde(default = "default_scope_ref")]
    pub scope_ref: String,
    #[serde(default)]
    pub status: AdaptiveWikiStatus,
    #[serde(default)]
    pub activation_mode: AdaptiveWikiActivationMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_modes: Vec<AdaptiveWikiAgentMode>,
    #[serde(default)]
    pub claim: String,
    #[serde(default)]
    pub ai_instruction: String,
    #[serde(default)]
    pub human_summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub counterexamples: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub support_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub capability_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_artifact_kinds: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub core_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proposed_tags: Vec<String>,
    #[serde(default)]
    pub confidence: AdaptiveWikiConfidence,
    #[serde(default = "default_timestamp")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_timestamp")]
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AdaptiveWikiEntryEdit {
    pub kind: Option<AdaptiveWikiKind>,
    pub agent_modes: Option<Vec<AdaptiveWikiAgentMode>>,
    pub claim: Option<String>,
    pub ai_instruction: Option<String>,
    pub human_summary: Option<String>,
    pub add_evidence_refs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiCandidate {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub kind: AdaptiveWikiKind,
    #[serde(default)]
    pub scope: AdaptiveWikiScope,
    #[serde(default = "default_scope_ref")]
    pub scope_ref: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_modes: Vec<AdaptiveWikiAgentMode>,
    #[serde(default)]
    pub claim: String,
    #[serde(default)]
    pub suggested_ai_instruction: String,
    #[serde(default)]
    pub human_summary: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub signal_kind: AdaptiveWikiSignalKind,
    #[serde(default)]
    pub origin: AdaptiveWikiOrigin,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_hashes: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub core_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub proposed_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_scope: Option<AdaptiveWikiScopeSuggestion>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub review_reason: String,
    #[serde(default = "default_occurrence_count")]
    pub occurrence_count: u32,
    #[serde(default)]
    pub confidence: AdaptiveWikiConfidence,
    #[serde(default = "default_timestamp")]
    pub created_at: DateTime<Utc>,
    #[serde(default = "default_timestamp")]
    pub updated_at: DateTime<Utc>,
    #[serde(default = "default_timestamp")]
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiEntryState {
    #[serde(default = "default_version")]
    pub version: String,
    #[serde(default)]
    pub entries: Vec<AdaptiveWikiEntry>,
}

impl Default for AdaptiveWikiEntryState {
    fn default() -> Self {
        Self {
            version: default_version(),
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiCandidateState {
    #[serde(default = "default_version")]
    pub version: String,
    #[serde(default)]
    pub candidates: Vec<AdaptiveWikiCandidate>,
}

impl Default for AdaptiveWikiCandidateState {
    fn default() -> Self {
        Self {
            version: default_version(),
            candidates: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdaptiveWikiCandidateInput {
    pub kind: AdaptiveWikiKind,
    pub scope: AdaptiveWikiScope,
    pub scope_ref: String,
    pub claim: String,
    pub suggested_ai_instruction: String,
    pub human_summary: String,
    pub evidence_ref: Option<String>,
    pub signal_kind: AdaptiveWikiSignalKind,
    pub origin: AdaptiveWikiOrigin,
    pub source_refs: Vec<String>,
    pub source_hashes: Vec<String>,
    pub suggested_scope: Option<AdaptiveWikiScopeSuggestion>,
    pub agent_modes: Vec<AdaptiveWikiAgentMode>,
    pub core_tags: Vec<String>,
    pub proposed_tags: Vec<String>,
    pub review_reason: String,
    pub confidence: AdaptiveWikiConfidence,
}

impl Default for AdaptiveWikiCandidateInput {
    fn default() -> Self {
        Self {
            kind: AdaptiveWikiKind::default(),
            scope: AdaptiveWikiScope::default(),
            scope_ref: default_scope_ref(),
            claim: String::new(),
            suggested_ai_instruction: String::new(),
            human_summary: String::new(),
            evidence_ref: None,
            signal_kind: AdaptiveWikiSignalKind::default(),
            origin: AdaptiveWikiOrigin::default(),
            source_refs: Vec::new(),
            source_hashes: Vec::new(),
            suggested_scope: None,
            agent_modes: Vec::new(),
            core_tags: Vec::new(),
            proposed_tags: Vec::new(),
            review_reason: String::new(),
            confidence: AdaptiveWikiConfidence::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiQuery {
    pub session_id: Option<String>,
    pub project_key: Option<String>,
    pub artifact_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_mode: Option<AdaptiveWikiAgentMode>,
    #[serde(default, skip_serializing_if = "is_default_agent_mode_filter")]
    pub agent_mode_filter: AdaptiveWikiAgentModeFilter,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiAiProjection {
    pub id: String,
    pub kind: AdaptiveWikiKind,
    pub scope: AdaptiveWikiScope,
    pub scope_ref: String,
    pub activation_mode: AdaptiveWikiActivationMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_modes: Vec<AdaptiveWikiAgentMode>,
    pub instruction: String,
    pub confidence: AdaptiveWikiConfidence,
    pub evidence_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiProjectionBudget {
    pub max_entries: usize,
    pub max_context_chars: usize,
    pub max_instruction_chars: usize,
}

impl Default for AdaptiveWikiProjectionBudget {
    fn default() -> Self {
        Self {
            max_entries: DEFAULT_PROJECTION_MAX_ENTRIES,
            max_context_chars: DEFAULT_PROJECTION_MAX_CONTEXT_CHARS,
            max_instruction_chars: DEFAULT_PROJECTION_MAX_INSTRUCTION_CHARS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiProjectionReport {
    pub query: AdaptiveWikiQuery,
    pub budget: AdaptiveWikiProjectionBudget,
    #[serde(default)]
    pub policy: AdaptiveWikiProjectionPolicy,
    pub summary: AdaptiveWikiProjectionSummary,
    pub selected: Vec<AdaptiveWikiAiProjection>,
    pub rejected: Vec<AdaptiveWikiProjectionRejection>,
    #[serde(default)]
    pub conflicts: Vec<AdaptiveWikiProjectionConflict>,
    #[serde(default)]
    pub review_expired: Vec<AdaptiveWikiProjectionReviewExpired>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiProjectionComparisonReport {
    pub query: AdaptiveWikiQuery,
    pub budget: AdaptiveWikiProjectionBudget,
    pub summary: AdaptiveWikiProjectionComparisonSummary,
    pub warn: AdaptiveWikiProjectionReport,
    pub strict: AdaptiveWikiProjectionReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiProjectionComparisonSummary {
    pub warn_selected: usize,
    pub strict_selected: usize,
    pub warn_rejected: usize,
    pub strict_rejected: usize,
    pub warn_estimated_context_chars: usize,
    pub strict_estimated_context_chars: usize,
    pub selected_only_in_warn: Vec<String>,
    pub selected_only_in_strict: Vec<String>,
    pub review_expired_excluded: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiRuntimePolicyAcknowledgement {
    pub id: String,
    #[serde(default)]
    pub scope_mode: AdaptiveWikiRuntimePolicyAckScopeMode,
    pub query: AdaptiveWikiQuery,
    pub budget: AdaptiveWikiProjectionBudget,
    pub policy: AdaptiveWikiProjectionPolicy,
    pub comparison_hash: String,
    pub selected_only_in_warn: Vec<String>,
    pub selected_only_in_strict: Vec<String>,
    pub review_expired_excluded: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiRuntimeProjectionResolution {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<AdaptiveWikiProjectionReport>,
    pub decision: AdaptiveWikiRuntimePolicyDecision,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiRuntimePolicyDecision {
    pub requested_policy: AdaptiveWikiProjectionPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_policy: Option<AdaptiveWikiProjectionPolicy>,
    pub status: AdaptiveWikiRuntimePolicyDecisionStatus,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledgement_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledgement_scope_mode: Option<AdaptiveWikiRuntimePolicyAckScopeMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparison_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiRuntimePolicyDecisionStatus {
    DefaultWarn,
    AppliedAcknowledged,
    AppliedProjectArtifactAcknowledged,
    StrictRequestedMissingAcknowledgement,
    StrictRequestedExpiredAcknowledgement,
    StrictRequestedStaleAcknowledgement,
    StrictRequestedScopeModeBlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiRuntimePolicyAckScopeMode {
    #[default]
    ExactQuery,
    ProjectArtifact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiProjectionPolicy {
    #[serde(default)]
    pub review_expired: AdaptiveWikiProjectionReviewExpiredPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiProjectionReviewExpiredPolicy {
    #[default]
    Warn,
    Exclude,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiProjectionSummary {
    pub entries_checked: usize,
    pub promoted_scope_matches: usize,
    pub selected: usize,
    pub rejected: usize,
    #[serde(default)]
    pub conflicts: usize,
    #[serde(default)]
    pub review_expired_projected: usize,
    pub instructions_truncated: usize,
    pub estimated_context_chars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiProjectionRejection {
    pub entry_id: String,
    pub kind: AdaptiveWikiKind,
    pub scope: AdaptiveWikiScope,
    pub scope_ref: String,
    pub reason: AdaptiveWikiProjectionRejectionReason,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiProjectionRejectionReason {
    EmptyInstruction,
    BudgetMaxEntries,
    BudgetMaxContextChars,
    ReviewExpiredExcluded,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiProjectionReviewExpired {
    pub entry_id: String,
    pub kind: AdaptiveWikiKind,
    pub scope: AdaptiveWikiScope,
    pub scope_ref: String,
    pub review_after: DateTime<Utc>,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiProjectionConflict {
    pub entry_id: String,
    pub conflicting_entry_id: String,
    pub kind: AdaptiveWikiKind,
    pub scope: AdaptiveWikiScope,
    pub scope_ref: String,
    pub signature: String,
    pub entry_polarity: AdaptiveWikiProjectionConflictPolarity,
    pub conflicting_polarity: AdaptiveWikiProjectionConflictPolarity,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiProjectionConflictPolarity {
    Positive,
    Negative,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectionConflictSignature {
    polarity: AdaptiveWikiProjectionConflictPolarity,
    target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProjectionConflictCandidate {
    entry_id: String,
    kind: AdaptiveWikiKind,
    scope: AdaptiveWikiScope,
    scope_ref: String,
    signature: ProjectionConflictSignature,
}

struct AdaptiveWikiProjectionCandidate {
    projection: AdaptiveWikiAiProjection,
    updated_at: DateTime<Utc>,
    review_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiHumanEntry {
    pub id: String,
    pub kind: AdaptiveWikiKind,
    pub scope: AdaptiveWikiScope,
    pub scope_ref: String,
    pub status: AdaptiveWikiStatus,
    pub activation_mode: AdaptiveWikiActivationMode,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_modes: Vec<AdaptiveWikiAgentMode>,
    pub claim: String,
    pub human_summary: String,
    pub evidence_refs: Vec<String>,
    pub counterexamples: Vec<String>,
    #[serde(default)]
    pub contested: bool,
    #[serde(default)]
    pub support_refs: Vec<String>,
    #[serde(default)]
    pub capability_ids: Vec<String>,
    #[serde(default)]
    pub required_artifact_kinds: Vec<String>,
    #[serde(default)]
    pub core_tags: Vec<String>,
    #[serde(default)]
    pub proposed_tags: Vec<String>,
    pub confidence: AdaptiveWikiConfidence,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub review_after: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiHumanCandidate {
    pub id: String,
    pub kind: AdaptiveWikiKind,
    pub scope: AdaptiveWikiScope,
    pub scope_ref: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub agent_modes: Vec<AdaptiveWikiAgentMode>,
    pub claim: String,
    pub human_summary: String,
    pub evidence_refs: Vec<String>,
    pub signal_kind: AdaptiveWikiSignalKind,
    pub origin: AdaptiveWikiOrigin,
    pub source_refs: Vec<String>,
    pub source_hashes: Vec<String>,
    #[serde(default)]
    pub core_tags: Vec<String>,
    #[serde(default)]
    pub proposed_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_scope: Option<AdaptiveWikiScopeSuggestion>,
    pub review_reason: String,
    pub occurrence_count: u32,
    pub confidence: AdaptiveWikiConfidence,
    pub updated_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiHumanProjection {
    pub entries: Vec<AdaptiveWikiHumanEntry>,
    pub candidates: Vec<AdaptiveWikiHumanCandidate>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiLintSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiLintIssue {
    pub severity: AdaptiveWikiLintSeverity,
    pub subject_kind: String,
    pub subject_id: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiLintSummary {
    pub entries_checked: usize,
    pub candidates_checked: usize,
    pub errors: usize,
    pub warnings: usize,
    pub info: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiLintReport {
    pub generated_at: DateTime<Utc>,
    pub summary: AdaptiveWikiLintSummary,
    pub issues: Vec<AdaptiveWikiLintIssue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiMarkdownExportFile {
    pub path: String,
    pub bytes: usize,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiMarkdownExportSummary {
    pub entries_exported: usize,
    pub candidates_exported: usize,
    pub files_planned: usize,
    pub files_written: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiMarkdownProjectionState {
    EmptyCanonical,
    Missing,
    Stale,
    Fresh,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiMarkdownProjectionStatus {
    pub profile_dir: String,
    pub entries_path: String,
    pub candidates_path: String,
    pub vault_index_path: String,
    pub entries_present: bool,
    pub candidates_present: bool,
    pub vault_index_present: bool,
    pub entries_modified_at: Option<DateTime<Utc>>,
    pub candidates_modified_at: Option<DateTime<Utc>>,
    pub vault_index_modified_at: Option<DateTime<Utc>>,
    pub canonical_modified_at: Option<DateTime<Utc>>,
    pub state: AdaptiveWikiMarkdownProjectionState,
    pub reexport_recommended: bool,
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiMarkdownExportReport {
    pub generated_at: DateTime<Utc>,
    pub output_dir: String,
    pub dry_run: bool,
    pub projection_status: AdaptiveWikiMarkdownProjectionStatus,
    pub summary: AdaptiveWikiMarkdownExportSummary,
    pub files: Vec<AdaptiveWikiMarkdownExportFile>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiTagClass {
    DerivedCore,
    Core,
    Proposed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiTagRegistryRule {
    pub prefix: String,
    pub description: String,
    pub lifecycle: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiGraphNode {
    pub id: String,
    pub node_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub wiki_id: String,
    pub label: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag_class: Option<AdaptiveWikiTagClass>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiGraphEdge {
    pub source: String,
    pub target: String,
    pub relationship: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag_class: Option<AdaptiveWikiTagClass>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiTagReviewIssue {
    pub severity: AdaptiveWikiLintSeverity,
    pub subject_kind: String,
    pub subject_id: String,
    pub tag: String,
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiGraphSummary {
    pub entries: usize,
    pub candidates: usize,
    pub tag_nodes: usize,
    pub core_tag_edges: usize,
    pub proposed_tag_edges: usize,
    pub derived_core_tag_edges: usize,
    pub review_issues: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiGraphReport {
    pub generated_at: DateTime<Utc>,
    pub summary: AdaptiveWikiGraphSummary,
    pub registry: Vec<AdaptiveWikiTagRegistryRule>,
    pub nodes: Vec<AdaptiveWikiGraphNode>,
    pub edges: Vec<AdaptiveWikiGraphEdge>,
    pub review_issues: Vec<AdaptiveWikiTagReviewIssue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiReviewProposalAction {
    Promote,
    Reject,
    Rescope,
    Deprecate,
    AddCounterexample,
    RenewReview,
    Split,
    Merge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiReviewRisk {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiReviewProposalDecision {
    Accepted,
    Rejected,
    Superseded,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiReviewQueueFilter {
    #[default]
    All,
    Active,
    Decided,
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiReviewProposal {
    pub id: String,
    pub action: AdaptiveWikiReviewProposalAction,
    pub subject_kind: String,
    pub subject_id: String,
    pub title: String,
    pub rationale: String,
    pub evidence_refs: Vec<String>,
    pub risk: AdaptiveWikiReviewRisk,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suggested_command: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<AdaptiveWikiReviewProposalLifecycle>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiReviewProposalLifecycle {
    pub latest_event_id: String,
    #[serde(default)]
    pub decision: AdaptiveWikiReviewProposalDecision,
    #[serde(default)]
    pub stale: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stale_evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    #[serde(default = "default_timestamp")]
    pub decided_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiReviewProposalEventRecord {
    pub id: String,
    pub proposal_id: String,
    #[serde(default)]
    pub decision: AdaptiveWikiReviewProposalDecision,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposal_action: Option<AdaptiveWikiReviewProposalAction>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub subject_kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub subject_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub actor: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supersedes: Option<String>,
    #[serde(default = "default_timestamp")]
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiReviewReportSummary {
    pub entries_checked: usize,
    pub candidates_checked: usize,
    pub usage_records_checked: usize,
    pub audit_records_checked: usize,
    pub correction_records_checked: usize,
    #[serde(default)]
    pub promotion_receipts_checked: usize,
    #[serde(default)]
    pub promotion_receipt_files_invalid: usize,
    #[serde(default)]
    pub promoted_entries_with_promotion_receipt: usize,
    #[serde(default)]
    pub promoted_entries_missing_promotion_receipt: usize,
    pub review_events_checked: usize,
    #[serde(default)]
    pub proposals_with_events: usize,
    #[serde(default)]
    pub open_proposals: usize,
    #[serde(default)]
    pub accepted_proposals: usize,
    #[serde(default)]
    pub rejected_proposals: usize,
    #[serde(default)]
    pub superseded_proposals: usize,
    #[serde(default)]
    pub stale_decision_proposals: usize,
    #[serde(default)]
    pub filtered_out_proposals: usize,
    pub lint_errors: usize,
    pub lint_warnings: usize,
    pub lint_info: usize,
    pub proposals: usize,
    pub files_written: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiReviewReport {
    pub generated_at: DateTime<Utc>,
    pub dry_run: bool,
    pub report_dir: String,
    pub summary: AdaptiveWikiReviewReportSummary,
    pub proposals: Vec<AdaptiveWikiReviewProposal>,
    pub lint: AdaptiveWikiLintReport,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiEpisodeEvaluationSummary {
    pub entries_checked: usize,
    pub candidates_checked: usize,
    pub in_scope_projection_count: usize,
    pub out_of_scope_projection_count: usize,
    pub target_entry_in_scope: bool,
    pub target_entry_out_of_scope: bool,
    pub deprecated_entry_projected: bool,
    pub review_expired_entry_projected: bool,
    pub projected_without_evidence: usize,
    pub scope_leakage_count: usize,
    pub failures: usize,
    pub files_written: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiEpisodeTraceStep {
    pub label: String,
    pub detail: String,
    pub entry_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiEpisodeEvaluationReport {
    pub generated_at: DateTime<Utc>,
    pub dry_run: bool,
    pub report_dir: String,
    pub target_entry_id: String,
    pub in_scope_query: AdaptiveWikiQuery,
    pub out_of_scope_query: AdaptiveWikiQuery,
    pub passed: bool,
    pub summary: AdaptiveWikiEpisodeEvaluationSummary,
    pub failures: Vec<String>,
    pub in_scope_projection: Vec<AdaptiveWikiAiProjection>,
    pub out_of_scope_projection: Vec<AdaptiveWikiAiProjection>,
    pub deprecated_projected_entry_ids: Vec<String>,
    pub review_expired_projected_entry_ids: Vec<String>,
    pub projected_without_evidence_entry_ids: Vec<String>,
    pub trace: Vec<AdaptiveWikiEpisodeTraceStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiLiveEpisodeFilter {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiLiveEpisodeEventKind {
    TaskEnqueued,
    ProjectionAttached,
    RuntimeUsageRecorded,
    OperatorCorrectionObserved,
    CandidateRecorded,
    EntryPromoted,
    CounterexampleRecorded,
    EntryDeprecated,
    TaskCompleted,
    TaskFailed,
    ResumePending,
    RollbackObserved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiLiveEpisodeEvent {
    pub id: String,
    pub kind: AdaptiveWikiLiveEpisodeEventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entry_ids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_refs: Vec<String>,
    pub summary: String,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiLiveEpisodeSummary {
    pub events: usize,
    pub task_events: usize,
    pub runtime_usage_events: usize,
    pub projection_events: usize,
    pub candidate_events: usize,
    pub correction_events: usize,
    pub promotion_events: usize,
    pub counterexample_events: usize,
    pub completion_events: usize,
    pub failure_events: usize,
    pub resume_pending_events: usize,
    pub rollback_events: usize,
    pub usage_without_task: usize,
    pub files_written: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiLiveEpisodeTraceReport {
    pub generated_at: DateTime<Utc>,
    pub dry_run: bool,
    pub report_dir: String,
    pub filter: AdaptiveWikiLiveEpisodeFilter,
    pub summary: AdaptiveWikiLiveEpisodeSummary,
    pub events: Vec<AdaptiveWikiLiveEpisodeEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdaptiveWikiCorrectionRecurrenceAssessment {
    InsufficientEvidence,
    NoRecurrenceObserved,
    RecurrenceObserved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiCorrectionRecurrenceSummary {
    pub entries_checked: usize,
    pub candidates_checked: usize,
    pub usage_records_checked: usize,
    pub correction_records_checked: usize,
    pub scoped_events: usize,
    pub pre_promotion_correction_events: usize,
    pub post_promotion_correction_events: usize,
    pub pre_promotion_failure_events: usize,
    pub post_promotion_failure_events: usize,
    pub post_promotion_counterexample_events: usize,
    pub post_promotion_usage_events: usize,
    pub recurrence_delta: i64,
    pub post_promotion_recurrence_per_1000: u32,
    pub files_written: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiCorrectionRecurrenceReport {
    pub generated_at: DateTime<Utc>,
    pub dry_run: bool,
    pub report_dir: String,
    pub entry_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<AdaptiveWikiScopeSuggestion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_at: Option<DateTime<Utc>>,
    pub assessment: AdaptiveWikiCorrectionRecurrenceAssessment,
    pub summary: AdaptiveWikiCorrectionRecurrenceSummary,
    pub pre_promotion_events: Vec<AdaptiveWikiLiveEpisodeEvent>,
    pub post_promotion_events: Vec<AdaptiveWikiLiveEpisodeEvent>,
    pub usage_events: Vec<AdaptiveWikiLiveEpisodeEvent>,
    pub failures: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdaptiveWikiPromotionEvidenceChainSummary {
    pub entries_checked: usize,
    pub candidates_checked: usize,
    pub usage_records_checked: usize,
    pub audit_records_checked: usize,
    pub promotion_audit_found: bool,
    pub candidate_snapshot_present: bool,
    pub entry_snapshot_present: bool,
    pub current_entry_present: bool,
    pub usage_records: usize,
    pub related_audit_records: usize,
    pub failures: usize,
    pub files_written: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdaptiveWikiPromotionEvidenceChainReport {
    pub generated_at: DateTime<Utc>,
    pub dry_run: bool,
    pub report_dir: String,
    pub entry_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub promotion_audit: Option<AdaptiveWikiAuditRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_snapshot: Option<AdaptiveWikiHumanCandidate>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entry_snapshot: Option<AdaptiveWikiHumanEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_entry: Option<AdaptiveWikiHumanEntry>,
    pub usage_records: Vec<AdaptiveWikiUsageRecord>,
    pub related_audit_records: Vec<AdaptiveWikiAuditRecord>,
    pub failures: Vec<String>,
    pub summary: AdaptiveWikiPromotionEvidenceChainSummary,
}

#[derive(Debug, Clone)]
pub struct AdaptiveWikiStore {
    root: PathBuf,
}

struct AdaptiveWikiLockGuard {
    file: File,
}

impl Drop for AdaptiveWikiLockGuard {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
    }
}

impl AdaptiveWikiStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn entries_path(&self) -> PathBuf {
        self.root.join(ADAPTIVE_WIKI_ENTRIES_FILE)
    }

    pub fn candidates_path(&self) -> PathBuf {
        self.root.join(ADAPTIVE_WIKI_CANDIDATES_FILE)
    }

    pub fn audit_path(&self) -> PathBuf {
        self.root.join(ADAPTIVE_WIKI_AUDIT_FILE)
    }

    pub fn usage_path(&self) -> PathBuf {
        self.root.join(ADAPTIVE_WIKI_USAGE_FILE)
    }

    pub fn corrections_path(&self) -> PathBuf {
        self.root.join(ADAPTIVE_WIKI_CORRECTIONS_FILE)
    }

    pub fn review_events_path(&self) -> PathBuf {
        self.root.join(ADAPTIVE_WIKI_REVIEW_EVENTS_FILE)
    }

    pub fn runtime_policy_acknowledgements_path(&self) -> PathBuf {
        self.root.join(ADAPTIVE_WIKI_RUNTIME_POLICY_ACKS_FILE)
    }

    pub fn promotion_receipts_dir(&self) -> PathBuf {
        self.root.join(ADAPTIVE_WIKI_PROMOTION_RECEIPTS_DIR)
    }

    pub fn default_markdown_vault_dir(&self) -> PathBuf {
        self.root.join(ADAPTIVE_WIKI_MARKDOWN_VAULT_DIR)
    }

    fn lock_exclusive(&self) -> Result<AdaptiveWikiLockGuard> {
        fs::create_dir_all(&self.root)?;
        let path = self.root.join(ADAPTIVE_WIKI_LOCK_FILE);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .with_context(|| format!("failed to open adaptive wiki lock {}", path.display()))?;
        file.lock_exclusive()
            .with_context(|| format!("failed to lock adaptive wiki state {}", path.display()))?;
        Ok(AdaptiveWikiLockGuard { file })
    }

    pub fn load_entries(&self) -> Result<AdaptiveWikiEntryState> {
        read_entry_state(&self.entries_path())
    }

    pub fn save_entries(&self, state: &AdaptiveWikiEntryState) -> Result<()> {
        let _lock = self.lock_exclusive()?;
        write_json_state(&self.entries_path(), state)
    }

    pub fn load_candidates(&self) -> Result<AdaptiveWikiCandidateState> {
        read_candidate_state(&self.candidates_path())
    }

    pub fn save_candidates(&self, state: &AdaptiveWikiCandidateState) -> Result<()> {
        let _lock = self.lock_exclusive()?;
        write_json_state(&self.candidates_path(), state)
    }

    pub fn record_candidate(
        &self,
        input: AdaptiveWikiCandidateInput,
        now: DateTime<Utc>,
    ) -> Result<AdaptiveWikiCandidate> {
        let _lock = self.lock_exclusive()?;
        let mut state = read_candidate_state(&self.candidates_path())?;
        let normalized_scope_ref = normalize_scope_ref(input.scope, &input.scope_ref);
        let claim_key = normalize_key(&input.claim);

        let candidate = if let Some(existing) = state.candidates.iter_mut().find(|candidate| {
            candidate.kind == input.kind
                && candidate.scope == input.scope
                && candidate.scope_ref == normalized_scope_ref
                && normalize_key(&candidate.claim) == claim_key
        }) {
            existing.occurrence_count = existing.occurrence_count.saturating_add(1).max(1);
            existing.suggested_ai_instruction = prefer_new_text(
                &existing.suggested_ai_instruction,
                &input.suggested_ai_instruction,
            );
            existing.human_summary = prefer_new_text(&existing.human_summary, &input.human_summary);
            existing.confidence = input.confidence;
            existing.updated_at = now;
            existing.last_seen_at = now;
            existing.signal_kind = input.signal_kind;
            existing.origin = input.origin;
            existing.suggested_scope = input.suggested_scope;
            push_unique_modes(&mut existing.agent_modes, input.agent_modes.iter());
            existing.review_reason = prefer_new_text(&existing.review_reason, &input.review_reason);
            push_unique(&mut existing.evidence_refs, input.evidence_ref.as_deref());
            push_unique_many(&mut existing.source_refs, input.source_refs.iter());
            push_unique_many(&mut existing.source_hashes, input.source_hashes.iter());
            push_unique_many(&mut existing.core_tags, clean_tags(input.core_tags).iter());
            push_unique_many(
                &mut existing.proposed_tags,
                clean_tags(input.proposed_tags).iter(),
            );
            existing.clone()
        } else {
            let candidate = AdaptiveWikiCandidate {
                id: format!("wiki_candidate_{}", Uuid::new_v4()),
                kind: input.kind,
                scope: input.scope,
                scope_ref: normalized_scope_ref,
                agent_modes: clean_agent_modes(input.agent_modes),
                claim: input.claim.trim().to_string(),
                suggested_ai_instruction: input.suggested_ai_instruction.trim().to_string(),
                human_summary: input.human_summary.trim().to_string(),
                evidence_refs: input
                    .evidence_ref
                    .as_deref()
                    .map(clean_ref)
                    .into_iter()
                    .filter(|value| !value.is_empty())
                    .collect(),
                signal_kind: input.signal_kind,
                origin: input.origin,
                source_refs: clean_refs(input.source_refs),
                source_hashes: clean_refs(input.source_hashes),
                core_tags: clean_tags(input.core_tags),
                proposed_tags: clean_tags(input.proposed_tags),
                suggested_scope: input.suggested_scope,
                review_reason: input.review_reason.trim().to_string(),
                occurrence_count: default_occurrence_count(),
                confidence: input.confidence,
                created_at: now,
                updated_at: now,
                last_seen_at: now,
            };
            state.candidates.push(candidate.clone());
            candidate
        };

        write_json_state(&self.candidates_path(), &state)?;
        if candidate.signal_kind == AdaptiveWikiSignalKind::OperatorCorrection {
            append_jsonl(
                &self.corrections_path(),
                &correction_record_from_candidate(&candidate, now),
            )?;
        }
        Ok(candidate)
    }

    pub fn promote_candidate(
        &self,
        candidate_id: &str,
        activation_mode: AdaptiveWikiActivationMode,
        now: DateTime<Utc>,
    ) -> Result<Option<AdaptiveWikiEntry>> {
        self.promote_candidate_scoped(candidate_id, activation_mode, None, now)
    }

    pub fn promote_candidate_scoped(
        &self,
        candidate_id: &str,
        activation_mode: AdaptiveWikiActivationMode,
        scope_override: Option<AdaptiveWikiScopeSuggestion>,
        now: DateTime<Utc>,
    ) -> Result<Option<AdaptiveWikiEntry>> {
        self.promote_candidate_scoped_with_agent_modes(
            candidate_id,
            activation_mode,
            scope_override,
            Vec::new(),
            now,
        )
    }

    pub fn promote_candidate_scoped_with_agent_modes(
        &self,
        candidate_id: &str,
        activation_mode: AdaptiveWikiActivationMode,
        scope_override: Option<AdaptiveWikiScopeSuggestion>,
        agent_modes: Vec<AdaptiveWikiAgentMode>,
        now: DateTime<Utc>,
    ) -> Result<Option<AdaptiveWikiEntry>> {
        let _lock = self.lock_exclusive()?;
        let mut candidate_state = read_candidate_state(&self.candidates_path())?;
        let Some(index) = candidate_state
            .candidates
            .iter()
            .position(|candidate| candidate.id == candidate_id)
        else {
            return Ok(None);
        };
        let candidate = candidate_state.candidates.remove(index);
        let scope = scope_override
            .as_ref()
            .map(|scope| scope.scope)
            .unwrap_or(candidate.scope);
        let scope_ref = scope_override
            .as_ref()
            .map(|scope| normalize_scope_ref(scope.scope, &scope.scope_ref))
            .unwrap_or(candidate.scope_ref);
        let agent_modes = if agent_modes.is_empty() {
            candidate.agent_modes.clone()
        } else {
            agent_modes
        };
        let entry = AdaptiveWikiEntry {
            id: format!("wiki_entry_{}", Uuid::new_v4()),
            kind: candidate.kind,
            scope,
            scope_ref,
            status: AdaptiveWikiStatus::Promoted,
            activation_mode,
            agent_modes: clean_agent_modes(agent_modes),
            claim: candidate.claim.clone(),
            ai_instruction: fallback_text(&candidate.suggested_ai_instruction, &candidate.claim),
            human_summary: fallback_text(&candidate.human_summary, &candidate.claim),
            evidence_refs: candidate.evidence_refs,
            counterexamples: Vec::new(),
            support_refs: Vec::new(),
            capability_ids: Vec::new(),
            required_artifact_kinds: Vec::new(),
            core_tags: candidate.core_tags,
            proposed_tags: candidate.proposed_tags,
            confidence: candidate.confidence,
            created_at: now,
            updated_at: now,
            review_after: None,
        };

        let mut entry_state = read_entry_state(&self.entries_path())?;
        entry_state.entries.push(entry.clone());
        write_json_state(&self.entries_path(), &entry_state)?;
        write_json_state(&self.candidates_path(), &candidate_state)?;
        Ok(Some(entry))
    }

    pub fn reject_candidate(&self, candidate_id: &str) -> Result<Option<AdaptiveWikiCandidate>> {
        let _lock = self.lock_exclusive()?;
        let mut state = read_candidate_state(&self.candidates_path())?;
        let Some(index) = state
            .candidates
            .iter()
            .position(|candidate| candidate.id == candidate_id)
        else {
            return Ok(None);
        };
        let candidate = state.candidates.remove(index);
        write_json_state(&self.candidates_path(), &state)?;
        Ok(Some(candidate))
    }

    pub fn rescope_entry(
        &self,
        entry_id: &str,
        scope: AdaptiveWikiScope,
        scope_ref: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<AdaptiveWikiEntry>> {
        let _lock = self.lock_exclusive()?;
        let mut state = read_entry_state(&self.entries_path())?;
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == entry_id) else {
            return Ok(None);
        };
        entry.scope = scope;
        entry.scope_ref = normalize_scope_ref(scope, scope_ref);
        entry.updated_at = now;
        let entry = entry.clone();
        write_json_state(&self.entries_path(), &state)?;
        Ok(Some(entry))
    }

    pub fn deprecate_entry(
        &self,
        entry_id: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<AdaptiveWikiEntry>> {
        let _lock = self.lock_exclusive()?;
        let mut state = read_entry_state(&self.entries_path())?;
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == entry_id) else {
            return Ok(None);
        };
        entry.status = AdaptiveWikiStatus::Deprecated;
        entry.updated_at = now;
        let entry = entry.clone();
        write_json_state(&self.entries_path(), &state)?;
        Ok(Some(entry))
    }

    pub fn add_counterexample(
        &self,
        entry_id: &str,
        evidence_ref: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<AdaptiveWikiEntry>> {
        let _lock = self.lock_exclusive()?;
        let mut state = read_entry_state(&self.entries_path())?;
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == entry_id) else {
            return Ok(None);
        };
        push_unique(&mut entry.counterexamples, Some(evidence_ref));
        entry.updated_at = now;
        let entry = entry.clone();
        write_json_state(&self.entries_path(), &state)?;
        Ok(Some(entry))
    }

    pub fn update_runbook_refs(
        &self,
        entry_id: &str,
        support_refs: &[String],
        capability_ids: &[String],
        required_artifact_kinds: &[String],
        now: DateTime<Utc>,
    ) -> Result<Option<AdaptiveWikiEntry>> {
        let _lock = self.lock_exclusive()?;
        let mut state = read_entry_state(&self.entries_path())?;
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == entry_id) else {
            return Ok(None);
        };
        push_unique_many(&mut entry.support_refs, support_refs.iter());
        push_unique_many(&mut entry.capability_ids, capability_ids.iter());
        push_unique_many(
            &mut entry.required_artifact_kinds,
            required_artifact_kinds.iter(),
        );
        entry.updated_at = now;
        let entry = entry.clone();
        write_json_state(&self.entries_path(), &state)?;
        Ok(Some(entry))
    }

    pub fn renew_review_after(
        &self,
        entry_id: &str,
        review_after: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<Option<AdaptiveWikiEntry>> {
        let _lock = self.lock_exclusive()?;
        let mut state = read_entry_state(&self.entries_path())?;
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == entry_id) else {
            return Ok(None);
        };
        entry.review_after = Some(review_after);
        entry.updated_at = now;
        let entry = entry.clone();
        write_json_state(&self.entries_path(), &state)?;
        Ok(Some(entry))
    }

    /// Edit an entry's classification, mode scope, text, and/or evidence refs.
    /// Only the provided fields change, so curator verdicts apply in place
    /// without reject + re-record.
    pub fn edit_entry(
        &self,
        entry_id: &str,
        edit: AdaptiveWikiEntryEdit,
        now: DateTime<Utc>,
    ) -> Result<Option<AdaptiveWikiEntry>> {
        let _lock = self.lock_exclusive()?;
        let mut state = read_entry_state(&self.entries_path())?;
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == entry_id) else {
            return Ok(None);
        };
        if let Some(kind) = edit.kind {
            entry.kind = kind;
        }
        if let Some(agent_modes) = edit.agent_modes {
            entry.agent_modes = clean_agent_modes(agent_modes);
        }
        if let Some(claim) = edit.claim {
            let claim = claim.trim();
            if !claim.is_empty() {
                entry.claim = claim.to_string();
            }
        }
        if let Some(ai_instruction) = edit.ai_instruction {
            entry.ai_instruction = ai_instruction.trim().to_string();
        }
        if let Some(human_summary) = edit.human_summary {
            entry.human_summary = human_summary.trim().to_string();
        }
        push_unique_many(
            &mut entry.evidence_refs,
            clean_refs(edit.add_evidence_refs).iter(),
        );
        entry.updated_at = now;
        let entry = entry.clone();
        write_json_state(&self.entries_path(), &state)?;
        Ok(Some(entry))
    }

    /// Add controlled/proposed tags to an entry (e.g. a reviewer's `facet/*` or
    /// `domain/*` classification), without changing any other field.
    pub fn add_entry_tags(
        &self,
        entry_id: &str,
        core_tags: &[String],
        proposed_tags: &[String],
        now: DateTime<Utc>,
    ) -> Result<Option<AdaptiveWikiEntry>> {
        let _lock = self.lock_exclusive()?;
        let mut state = read_entry_state(&self.entries_path())?;
        let Some(entry) = state.entries.iter_mut().find(|entry| entry.id == entry_id) else {
            return Ok(None);
        };
        push_unique_many(&mut entry.core_tags, clean_tags(core_tags.to_vec()).iter());
        push_unique_many(
            &mut entry.proposed_tags,
            clean_tags(proposed_tags.to_vec()).iter(),
        );
        entry.updated_at = now;
        let entry = entry.clone();
        write_json_state(&self.entries_path(), &state)?;
        Ok(Some(entry))
    }

    pub fn append_audit(&self, record: &AdaptiveWikiAuditRecord) -> Result<()> {
        let _lock = self.lock_exclusive()?;
        append_jsonl(&self.audit_path(), record)
    }

    pub fn load_audit_records(&self) -> Result<Vec<AdaptiveWikiAuditRecord>> {
        read_jsonl(&self.audit_path())
    }

    pub fn write_promotion_receipt(
        &self,
        receipt: &AdaptiveWikiPromotionReceipt,
    ) -> Result<PathBuf> {
        let _lock = self.lock_exclusive()?;
        let output_dir = self.promotion_receipts_dir();
        fs::create_dir_all(&output_dir)?;
        let path = output_dir.join(promotion_receipt_filename(receipt));
        write_json_state(&path, receipt)?;
        Ok(path)
    }

    fn load_promotion_receipt_report(&self) -> Result<AdaptiveWikiPromotionReceiptLoadReport> {
        let dir = self.promotion_receipts_dir();
        if !dir.exists() {
            return Ok(AdaptiveWikiPromotionReceiptLoadReport::default());
        }
        let mut receipts = Vec::new();
        let mut invalid_files = 0;
        for entry in fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file()
                || path
                    .extension()
                    .and_then(|value| value.to_str())
                    .map(|extension| extension != "json")
                    .unwrap_or(true)
            {
                continue;
            }
            let Ok(content) = fs::read_to_string(&path) else {
                invalid_files += 1;
                continue;
            };
            let Ok(receipt) = serde_json::from_str::<AdaptiveWikiPromotionReceipt>(&content) else {
                invalid_files += 1;
                continue;
            };
            receipts.push(receipt);
        }
        receipts.sort_by_key(|receipt| receipt.generated_at);
        Ok(AdaptiveWikiPromotionReceiptLoadReport {
            receipts,
            invalid_files,
        })
    }

    pub fn load_promotion_receipts(&self) -> Result<Vec<AdaptiveWikiPromotionReceipt>> {
        Ok(self.load_promotion_receipt_report()?.receipts)
    }

    pub fn invalid_promotion_receipt_files(&self) -> Result<usize> {
        Ok(self.load_promotion_receipt_report()?.invalid_files)
    }

    pub fn append_usage_records(&self, records: &[AdaptiveWikiUsageRecord]) -> Result<()> {
        let _lock = self.lock_exclusive()?;
        for record in records {
            append_jsonl(&self.usage_path(), record)?;
        }
        Ok(())
    }

    pub fn load_usage_records(&self) -> Result<Vec<AdaptiveWikiUsageRecord>> {
        read_jsonl(&self.usage_path())
    }

    pub fn append_correction_record(&self, record: &AdaptiveWikiCorrectionRecord) -> Result<()> {
        let _lock = self.lock_exclusive()?;
        append_jsonl(&self.corrections_path(), record)
    }

    pub fn load_correction_records(&self) -> Result<Vec<AdaptiveWikiCorrectionRecord>> {
        read_jsonl(&self.corrections_path())
    }

    pub fn append_review_proposal_event(
        &self,
        record: &AdaptiveWikiReviewProposalEventRecord,
    ) -> Result<()> {
        let _lock = self.lock_exclusive()?;
        append_jsonl(&self.review_events_path(), record)
    }

    pub fn load_review_proposal_events(
        &self,
    ) -> Result<Vec<AdaptiveWikiReviewProposalEventRecord>> {
        read_jsonl(&self.review_events_path())
    }

    pub fn append_runtime_policy_acknowledgement(
        &self,
        record: &AdaptiveWikiRuntimePolicyAcknowledgement,
    ) -> Result<()> {
        let _lock = self.lock_exclusive()?;
        append_jsonl(&self.runtime_policy_acknowledgements_path(), record)
    }

    pub fn load_runtime_policy_acknowledgements(
        &self,
    ) -> Result<Vec<AdaptiveWikiRuntimePolicyAcknowledgement>> {
        read_jsonl(&self.runtime_policy_acknowledgements_path())
    }

    pub fn ai_projection(
        &self,
        query: &AdaptiveWikiQuery,
    ) -> Result<Vec<AdaptiveWikiAiProjection>> {
        Ok(build_ai_projection(&self.load_entries()?.entries, query))
    }

    pub fn ai_projection_report(
        &self,
        query: &AdaptiveWikiQuery,
        budget: AdaptiveWikiProjectionBudget,
    ) -> Result<AdaptiveWikiProjectionReport> {
        self.ai_projection_report_with_policy(
            query,
            budget,
            AdaptiveWikiProjectionPolicy::default(),
        )
    }

    pub fn ai_projection_report_with_policy(
        &self,
        query: &AdaptiveWikiQuery,
        budget: AdaptiveWikiProjectionBudget,
        policy: AdaptiveWikiProjectionPolicy,
    ) -> Result<AdaptiveWikiProjectionReport> {
        Ok(build_ai_projection_report_with_policy(
            &self.load_entries()?.entries,
            query,
            budget,
            policy,
        ))
    }

    pub fn ai_projection_review_expired_policy_comparison(
        &self,
        query: &AdaptiveWikiQuery,
        budget: AdaptiveWikiProjectionBudget,
    ) -> Result<AdaptiveWikiProjectionComparisonReport> {
        Ok(build_ai_projection_review_expired_policy_comparison(
            &self.load_entries()?.entries,
            query,
            budget,
        ))
    }

    pub fn acknowledge_runtime_strict_review_expired_policy(
        &self,
        query: &AdaptiveWikiQuery,
        budget: AdaptiveWikiProjectionBudget,
        scope_mode: AdaptiveWikiRuntimePolicyAckScopeMode,
        ttl: Duration,
        reason: &str,
        now: DateTime<Utc>,
    ) -> Result<AdaptiveWikiRuntimePolicyAcknowledgement> {
        let policy = AdaptiveWikiProjectionPolicy {
            review_expired: AdaptiveWikiProjectionReviewExpiredPolicy::Exclude,
        };
        let comparison =
            self.ai_projection_review_expired_policy_comparison(query, budget.clone())?;
        let comparison_hash = runtime_policy_comparison_hash(&comparison, policy)?;
        let record = AdaptiveWikiRuntimePolicyAcknowledgement {
            id: format!("wiki_runtime_policy_ack_{}", Uuid::new_v4()),
            scope_mode,
            query: query.clone(),
            budget,
            policy,
            comparison_hash,
            selected_only_in_warn: comparison.summary.selected_only_in_warn,
            selected_only_in_strict: comparison.summary.selected_only_in_strict,
            review_expired_excluded: comparison.summary.review_expired_excluded,
            reason: operator_safe_text(reason),
            created_at: now,
            expires_at: now + ttl,
        };
        self.append_runtime_policy_acknowledgement(&record)?;
        Ok(record)
    }

    pub fn runtime_projection_with_policy_acknowledgement(
        &self,
        query: &AdaptiveWikiQuery,
        budget: AdaptiveWikiProjectionBudget,
        requested_policy: AdaptiveWikiProjectionPolicy,
        now: DateTime<Utc>,
    ) -> Result<AdaptiveWikiRuntimeProjectionResolution> {
        if requested_policy.review_expired == AdaptiveWikiProjectionReviewExpiredPolicy::Warn {
            let report = self.ai_projection_report(query, budget)?;
            return Ok(AdaptiveWikiRuntimeProjectionResolution {
                report: Some(report),
                decision: AdaptiveWikiRuntimePolicyDecision {
                    requested_policy,
                    applied_policy: Some(requested_policy),
                    status: AdaptiveWikiRuntimePolicyDecisionStatus::DefaultWarn,
                    reason: "default warn runtime projection policy applied".to_string(),
                    acknowledgement_id: None,
                    comparison_hash: None,
                    expires_at: None,
                    acknowledgement_scope_mode: None,
                },
            });
        }

        if requested_policy.review_expired != AdaptiveWikiProjectionReviewExpiredPolicy::Exclude {
            bail!("unsupported adaptive wiki runtime projection policy");
        }

        let comparison =
            self.ai_projection_review_expired_policy_comparison(query, budget.clone())?;
        let comparison_hash = runtime_policy_comparison_hash(&comparison, requested_policy)?;
        let broader_query = project_artifact_runtime_ack_query(query);
        let broader_comparison = broader_query
            .as_ref()
            .map(|query| self.ai_projection_review_expired_policy_comparison(query, budget.clone()))
            .transpose()?;
        let broader_comparison_hash = broader_comparison
            .as_ref()
            .map(|comparison| runtime_policy_comparison_hash(comparison, requested_policy))
            .transpose()?;
        let has_session_specific_projection = comparison_has_session_scope(&comparison);
        let acknowledgements = self.load_runtime_policy_acknowledgements()?;
        let mut expired_match = None;
        let mut stale_match = None;
        let mut scope_blocked_match = None;

        for acknowledgement in acknowledgements.iter().rev() {
            if acknowledgement.budget != budget || acknowledgement.policy != requested_policy {
                continue;
            }

            match acknowledgement.scope_mode {
                AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery => {
                    if acknowledgement.query != *query {
                        continue;
                    }
                    if acknowledgement.comparison_hash == comparison_hash {
                        if acknowledgement.expires_at > now {
                            return Ok(AdaptiveWikiRuntimeProjectionResolution {
                                report: Some(comparison.strict),
                                decision: AdaptiveWikiRuntimePolicyDecision {
                                    requested_policy,
                                    applied_policy: Some(requested_policy),
                                    status:
                                        AdaptiveWikiRuntimePolicyDecisionStatus::AppliedAcknowledged,
                                    reason:
                                        "strict review-expired runtime projection policy acknowledged"
                                            .to_string(),
                                    acknowledgement_id: Some(acknowledgement.id.clone()),
                                    acknowledgement_scope_mode: Some(acknowledgement.scope_mode),
                                    comparison_hash: Some(comparison_hash),
                                    expires_at: Some(acknowledgement.expires_at),
                                },
                            });
                        }
                        expired_match.get_or_insert_with(|| acknowledgement.clone());
                        continue;
                    }
                    if acknowledgement.expires_at > now {
                        stale_match.get_or_insert_with(|| acknowledgement.clone());
                    }
                }
                AdaptiveWikiRuntimePolicyAckScopeMode::ProjectArtifact => {
                    let Some(broader_query) = broader_query.as_ref() else {
                        continue;
                    };
                    if acknowledgement.query != *broader_query {
                        continue;
                    }
                    let Some(broader_hash) = broader_comparison_hash.as_ref() else {
                        continue;
                    };
                    if acknowledgement.comparison_hash == *broader_hash {
                        if acknowledgement.expires_at > now {
                            if has_session_specific_projection {
                                scope_blocked_match.get_or_insert_with(|| acknowledgement.clone());
                                continue;
                            }
                            let broader_comparison = broader_comparison
                                .as_ref()
                                .expect("broader comparison built with broader query");
                            return Ok(AdaptiveWikiRuntimeProjectionResolution {
                                report: Some(broader_comparison.strict.clone()),
                                decision: AdaptiveWikiRuntimePolicyDecision {
                                    requested_policy,
                                    applied_policy: Some(requested_policy),
                                    status: AdaptiveWikiRuntimePolicyDecisionStatus::AppliedProjectArtifactAcknowledged,
                                    reason: "strict review-expired runtime projection policy acknowledged for project/artifact scope"
                                        .to_string(),
                                    acknowledgement_id: Some(acknowledgement.id.clone()),
                                    acknowledgement_scope_mode: Some(acknowledgement.scope_mode),
                                    comparison_hash: Some(broader_hash.clone()),
                                    expires_at: Some(acknowledgement.expires_at),
                                },
                            });
                        }
                        expired_match.get_or_insert_with(|| acknowledgement.clone());
                        continue;
                    }
                    if acknowledgement.expires_at > now {
                        stale_match.get_or_insert_with(|| acknowledgement.clone());
                    }
                }
            }
        }

        let (status, reason, acknowledgement_id, acknowledgement_scope_mode, expires_at) =
            if let Some(acknowledgement) = scope_blocked_match {
                (
                    AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedScopeModeBlocked,
                    "project/artifact runtime policy acknowledgement cannot apply while session-scoped projection entries are present",
                    Some(acknowledgement.id),
                    Some(acknowledgement.scope_mode),
                    Some(acknowledgement.expires_at),
                )
            } else if let Some(acknowledgement) = stale_match {
                (
                    AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedStaleAcknowledgement,
                    "strict review-expired runtime policy acknowledgement does not match the current comparison hash",
                    Some(acknowledgement.id),
                    Some(acknowledgement.scope_mode),
                    Some(acknowledgement.expires_at),
                )
            } else if let Some(acknowledgement) = expired_match {
                (
                    AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedExpiredAcknowledgement,
                    "strict review-expired runtime policy acknowledgement is expired",
                    Some(acknowledgement.id),
                    Some(acknowledgement.scope_mode),
                    Some(acknowledgement.expires_at),
                )
            } else {
                (
                    AdaptiveWikiRuntimePolicyDecisionStatus::StrictRequestedMissingAcknowledgement,
                    "strict review-expired runtime policy requested without acknowledgement",
                    None,
                    None,
                    None,
                )
            };

        Ok(AdaptiveWikiRuntimeProjectionResolution {
            report: None,
            decision: AdaptiveWikiRuntimePolicyDecision {
                requested_policy,
                applied_policy: None,
                status,
                reason: reason.to_string(),
                acknowledgement_id,
                acknowledgement_scope_mode,
                comparison_hash: Some(comparison_hash),
                expires_at,
            },
        })
    }

    pub fn human_projection(
        &self,
        query: &AdaptiveWikiQuery,
    ) -> Result<AdaptiveWikiHumanProjection> {
        Ok(build_human_projection(
            &self.load_entries()?.entries,
            &self.load_candidates()?.candidates,
            query,
        ))
    }

    pub fn lint(&self, now: DateTime<Utc>) -> Result<AdaptiveWikiLintReport> {
        Ok(build_lint_report(
            &self.load_entries()?.entries,
            &self.load_candidates()?.candidates,
            now,
        ))
    }

    pub fn export_markdown(
        &self,
        output_dir: &Path,
        dry_run: bool,
        now: DateTime<Utc>,
    ) -> Result<AdaptiveWikiMarkdownExportReport> {
        let entries = self.load_entries()?.entries;
        let candidates = self.load_candidates()?.candidates;
        let files = build_markdown_export_files(&entries, &candidates, now);
        if !dry_run {
            write_markdown_export(output_dir, &files)?;
        }
        let projection_status = self.markdown_projection_status(output_dir);
        Ok(markdown_export_report(
            output_dir,
            dry_run,
            now,
            projection_status,
            entries.len(),
            candidates.len(),
            &files,
        ))
    }

    pub fn markdown_projection_status(
        &self,
        output_dir: &Path,
    ) -> AdaptiveWikiMarkdownProjectionStatus {
        markdown_projection_status(
            &self.root,
            &self.entries_path(),
            &self.candidates_path(),
            &output_dir.join("index.md"),
        )
    }

    pub fn graph_report(&self, now: DateTime<Utc>) -> Result<AdaptiveWikiGraphReport> {
        Ok(build_graph_report(
            &self.load_entries()?.entries,
            &self.load_candidates()?.candidates,
            now,
        ))
    }

    pub fn generate_review_report(
        &self,
        dry_run: bool,
        now: DateTime<Utc>,
    ) -> Result<AdaptiveWikiReviewReport> {
        self.generate_review_report_filtered(dry_run, now, AdaptiveWikiReviewQueueFilter::All)
    }

    pub fn generate_review_report_filtered(
        &self,
        dry_run: bool,
        now: DateTime<Utc>,
        queue_filter: AdaptiveWikiReviewQueueFilter,
    ) -> Result<AdaptiveWikiReviewReport> {
        let entries = self.load_entries()?.entries;
        let candidates = self.load_candidates()?.candidates;
        let usage_records = self.load_usage_records()?;
        let audit_records = self.load_audit_records()?;
        let correction_records = self.load_correction_records()?;
        let promotion_receipt_report = self.load_promotion_receipt_report()?;
        let promotion_receipts = promotion_receipt_report.receipts;
        let review_events = self.load_review_proposal_events()?;
        let lint = build_lint_report(&entries, &candidates, now);
        let promotion_receipt_coverage = promotion_receipt_coverage(&entries, &promotion_receipts);
        let report_dir = self.review_report_dir(now);
        let mut proposals = build_review_proposals(
            &entries,
            &candidates,
            &usage_records,
            &audit_records,
            &correction_records,
            &lint,
            now,
        );
        let lifecycle_context = AdaptiveWikiReviewProposalLifecycleContext {
            entries: &entries,
            candidates: &candidates,
            usage_records: &usage_records,
            audit_records: &audit_records,
            correction_records: &correction_records,
        };
        attach_review_proposal_lifecycle(&mut proposals, &review_events, &lifecycle_context);
        let unfiltered_proposals = proposals.len();
        proposals.retain(|proposal| review_proposal_matches_queue_filter(proposal, queue_filter));
        let proposal_lifecycle_summary = review_proposal_lifecycle_summary(&proposals);
        let mut report = AdaptiveWikiReviewReport {
            generated_at: now,
            dry_run,
            report_dir: report_dir.display().to_string(),
            summary: AdaptiveWikiReviewReportSummary {
                entries_checked: entries.len(),
                candidates_checked: candidates.len(),
                usage_records_checked: usage_records.len(),
                audit_records_checked: audit_records.len(),
                correction_records_checked: correction_records.len(),
                promotion_receipts_checked: promotion_receipts.len(),
                promotion_receipt_files_invalid: promotion_receipt_report.invalid_files,
                promoted_entries_with_promotion_receipt: promotion_receipt_coverage.with_receipt,
                promoted_entries_missing_promotion_receipt: promotion_receipt_coverage
                    .missing_receipt,
                review_events_checked: review_events.len(),
                proposals_with_events: proposal_lifecycle_summary.proposals_with_events,
                open_proposals: proposal_lifecycle_summary.open_proposals,
                accepted_proposals: proposal_lifecycle_summary.accepted_proposals,
                rejected_proposals: proposal_lifecycle_summary.rejected_proposals,
                superseded_proposals: proposal_lifecycle_summary.superseded_proposals,
                stale_decision_proposals: proposal_lifecycle_summary.stale_decision_proposals,
                filtered_out_proposals: unfiltered_proposals.saturating_sub(proposals.len()),
                lint_errors: lint.summary.errors,
                lint_warnings: lint.summary.warnings,
                lint_info: lint.summary.info,
                proposals: proposals.len(),
                files_written: 0,
            },
            proposals,
            lint,
        };
        if !dry_run {
            write_review_report(&report_dir, &report)?;
            report.summary.files_written = 2;
        }
        Ok(report)
    }

    pub fn generate_episode_evaluation_report(
        &self,
        target_entry_id: &str,
        in_scope_query: AdaptiveWikiQuery,
        out_of_scope_query: AdaptiveWikiQuery,
        dry_run: bool,
        now: DateTime<Utc>,
    ) -> Result<AdaptiveWikiEpisodeEvaluationReport> {
        let entries = self.load_entries()?.entries;
        let candidates = self.load_candidates()?.candidates;
        let report_dir = self.episode_report_dir(now);
        let in_scope_projection = build_ai_projection(&entries, &in_scope_query);
        let out_of_scope_projection = build_ai_projection(&entries, &out_of_scope_query);
        let projected_without_evidence_entry_ids = projected_entries_without_evidence(
            &entries,
            &in_scope_projection,
            &out_of_scope_projection,
        );
        let deprecated_projected_entry_ids = projected_entries_with_status(
            &entries,
            &in_scope_projection,
            &out_of_scope_projection,
            AdaptiveWikiStatus::Deprecated,
        );
        let review_expired_projected_entry_ids = projected_review_expired_entries(
            &entries,
            &in_scope_projection,
            &out_of_scope_projection,
            now,
        );

        let target_entry_id = operator_safe_text(target_entry_id.trim());
        let target_entry = entries.iter().find(|entry| entry.id == target_entry_id);
        let target_entry_in_scope =
            projection_contains_entry(&in_scope_projection, &target_entry_id);
        let target_entry_out_of_scope =
            projection_contains_entry(&out_of_scope_projection, &target_entry_id);
        let mut failures = Vec::new();
        if target_entry.is_none() {
            failures.push(format!("target entry `{target_entry_id}` was not found"));
        } else {
            if !target_entry_in_scope {
                failures.push(format!(
                    "target entry `{target_entry_id}` was not projected for the in-scope query"
                ));
            }
            if target_entry_out_of_scope {
                failures.push(format!(
                    "target entry `{target_entry_id}` leaked into the out-of-scope query"
                ));
            }
            if target_entry.is_some_and(|entry| entry.evidence_refs.is_empty()) {
                failures.push(format!(
                    "target entry `{target_entry_id}` has no evidence refs"
                ));
            }
        }
        if !deprecated_projected_entry_ids.is_empty() {
            failures.push(format!(
                "deprecated entries were projected: {}",
                deprecated_projected_entry_ids.join(", ")
            ));
        }
        if !review_expired_projected_entry_ids.is_empty() {
            failures.push(format!(
                "review-expired entries were projected: {}",
                review_expired_projected_entry_ids.join(", ")
            ));
        }
        if !projected_without_evidence_entry_ids.is_empty() {
            failures.push(format!(
                "projected entries without evidence refs: {}",
                projected_without_evidence_entry_ids.join(", ")
            ));
        }

        let mut report = AdaptiveWikiEpisodeEvaluationReport {
            generated_at: now,
            dry_run,
            report_dir: report_dir.display().to_string(),
            target_entry_id,
            in_scope_query,
            out_of_scope_query,
            passed: failures.is_empty(),
            summary: AdaptiveWikiEpisodeEvaluationSummary {
                entries_checked: entries.len(),
                candidates_checked: candidates.len(),
                in_scope_projection_count: in_scope_projection.len(),
                out_of_scope_projection_count: out_of_scope_projection.len(),
                target_entry_in_scope,
                target_entry_out_of_scope,
                deprecated_entry_projected: !deprecated_projected_entry_ids.is_empty(),
                review_expired_entry_projected: !review_expired_projected_entry_ids.is_empty(),
                projected_without_evidence: projected_without_evidence_entry_ids.len(),
                scope_leakage_count: usize::from(target_entry_out_of_scope),
                failures: failures.len(),
                files_written: 0,
            },
            failures,
            in_scope_projection,
            out_of_scope_projection,
            deprecated_projected_entry_ids,
            review_expired_projected_entry_ids,
            projected_without_evidence_entry_ids,
            trace: Vec::new(),
        };
        report.trace = build_episode_trace(&report);
        if !dry_run {
            report.summary.files_written = 2;
            write_episode_evaluation_report(&report_dir, &report)?;
        }
        Ok(report)
    }

    pub fn generate_live_episode_trace_report(
        &self,
        tasks: &[OffdeskTask],
        probes: &[BackgroundProbe],
        resume_states: &[TaskResumeState],
        filter: AdaptiveWikiLiveEpisodeFilter,
        dry_run: bool,
        now: DateTime<Utc>,
    ) -> Result<AdaptiveWikiLiveEpisodeTraceReport> {
        let entries = self.load_entries()?.entries;
        let candidates = self.load_candidates()?.candidates;
        let usage_records = self.load_usage_records()?;
        let audit_records = self.load_audit_records()?;
        let correction_records = self.load_correction_records()?;
        let report_dir = self.live_episode_trace_dir(now);
        let events = build_live_episode_events(
            &entries,
            &candidates,
            &usage_records,
            &audit_records,
            &correction_records,
            tasks,
            probes,
            resume_states,
            &filter,
            now,
        );
        let safe_filter = operator_safe_live_episode_filter(&filter);
        let mut report = AdaptiveWikiLiveEpisodeTraceReport {
            generated_at: now,
            dry_run,
            report_dir: report_dir.display().to_string(),
            filter: safe_filter,
            summary: live_episode_summary(&events, tasks, &usage_records),
            events,
        };
        if !dry_run {
            write_live_episode_trace_report(&report_dir, &report)?;
            report.summary.files_written = 3;
        }
        Ok(report)
    }

    pub fn generate_correction_recurrence_report(
        &self,
        tasks: &[OffdeskTask],
        probes: &[BackgroundProbe],
        resume_states: &[TaskResumeState],
        entry_id: &str,
        dry_run: bool,
        now: DateTime<Utc>,
    ) -> Result<AdaptiveWikiCorrectionRecurrenceReport> {
        let entries = self.load_entries()?.entries;
        let candidates = self.load_candidates()?.candidates;
        let usage_records = self.load_usage_records()?;
        let audit_records = self.load_audit_records()?;
        let correction_records = self.load_correction_records()?;
        let report_dir = self.correction_recurrence_report_dir(now);
        let entry_id = operator_safe_text(entry_id.trim());
        let entry = entries.iter().find(|entry| entry.id == entry_id);
        let scope = entry.map(|entry| AdaptiveWikiScopeSuggestion {
            scope: entry.scope,
            scope_ref: operator_safe_text(&entry.scope_ref),
        });
        let promotion_boundary = entry.and_then(|entry| {
            promotion_time_for_entry(entry, &audit_records).or(Some(entry.created_at))
        });
        let mut failures = Vec::new();
        if entry.is_none() {
            failures.push(format!("entry `{entry_id}` was not found"));
        }
        if promotion_boundary.is_none() {
            failures.push(format!(
                "entry `{entry_id}` has no promotion boundary to evaluate"
            ));
        }

        let filter = entry.map(live_filter_for_entry_scope).unwrap_or_default();
        let scoped_events = build_live_episode_events(
            &entries,
            &candidates,
            &usage_records,
            &audit_records,
            &correction_records,
            tasks,
            probes,
            resume_states,
            &filter,
            now,
        );
        let promotion_at = promotion_boundary.unwrap_or(now);
        let pre_promotion_events: Vec<_> = scoped_events
            .iter()
            .filter(|event| event.occurred_at < promotion_at)
            .filter(|event| is_correction_recurrence_event(event, &entry_id))
            .cloned()
            .collect();
        let post_promotion_events: Vec<_> = scoped_events
            .iter()
            .filter(|event| event.occurred_at >= promotion_at)
            .filter(|event| is_correction_recurrence_event(event, &entry_id))
            .cloned()
            .collect();
        let usage_events: Vec<_> = scoped_events
            .iter()
            .filter(|event| event.kind == AdaptiveWikiLiveEpisodeEventKind::RuntimeUsageRecorded)
            .filter(|event| event.entry_ids.iter().any(|id| id == &entry_id))
            .cloned()
            .collect();
        let pre_promotion_failure_events = pre_promotion_events
            .iter()
            .filter(|event| event.kind == AdaptiveWikiLiveEpisodeEventKind::TaskFailed)
            .count();
        let post_promotion_failure_events = post_promotion_events
            .iter()
            .filter(|event| event.kind == AdaptiveWikiLiveEpisodeEventKind::TaskFailed)
            .count();
        let post_promotion_counterexample_events = post_promotion_events
            .iter()
            .filter(|event| event.kind == AdaptiveWikiLiveEpisodeEventKind::CounterexampleRecorded)
            .count();
        let post_promotion_correction_events = post_promotion_events
            .iter()
            .filter(|event| {
                matches!(
                    event.kind,
                    AdaptiveWikiLiveEpisodeEventKind::OperatorCorrectionObserved
                        | AdaptiveWikiLiveEpisodeEventKind::CounterexampleRecorded
                )
            })
            .count();
        let pre_promotion_correction_events = pre_promotion_events
            .iter()
            .filter(|event| {
                event.kind == AdaptiveWikiLiveEpisodeEventKind::OperatorCorrectionObserved
            })
            .count();
        let post_promotion_usage_events = usage_events
            .iter()
            .filter(|event| event.occurred_at >= promotion_at)
            .count();
        let recurrence_numerator = post_promotion_correction_events + post_promotion_failure_events;
        let post_promotion_recurrence_per_1000 = recurrence_numerator
            .saturating_mul(1000)
            .checked_div(post_promotion_usage_events)
            .map(|value| u32::try_from(value).unwrap_or(u32::MAX))
            .unwrap_or(0);
        let assessment = if entry.is_none() || post_promotion_usage_events == 0 {
            AdaptiveWikiCorrectionRecurrenceAssessment::InsufficientEvidence
        } else if recurrence_numerator == 0 {
            AdaptiveWikiCorrectionRecurrenceAssessment::NoRecurrenceObserved
        } else {
            AdaptiveWikiCorrectionRecurrenceAssessment::RecurrenceObserved
        };
        let mut report = AdaptiveWikiCorrectionRecurrenceReport {
            generated_at: now,
            dry_run,
            report_dir: report_dir.display().to_string(),
            entry_id,
            scope,
            promotion_at: promotion_boundary,
            assessment,
            summary: AdaptiveWikiCorrectionRecurrenceSummary {
                entries_checked: entries.len(),
                candidates_checked: candidates.len(),
                usage_records_checked: usage_records.len(),
                correction_records_checked: correction_records.len(),
                scoped_events: scoped_events.len(),
                pre_promotion_correction_events,
                post_promotion_correction_events,
                pre_promotion_failure_events,
                post_promotion_failure_events,
                post_promotion_counterexample_events,
                post_promotion_usage_events,
                recurrence_delta: post_promotion_correction_events as i64
                    - pre_promotion_correction_events as i64,
                post_promotion_recurrence_per_1000,
                files_written: 0,
            },
            pre_promotion_events,
            post_promotion_events,
            usage_events,
            failures,
        };
        if !dry_run {
            write_correction_recurrence_report(&report_dir, &report)?;
            report.summary.files_written = 3;
        }
        Ok(report)
    }

    pub fn generate_promotion_evidence_chain_report(
        &self,
        entry_id: &str,
        dry_run: bool,
        now: DateTime<Utc>,
    ) -> Result<AdaptiveWikiPromotionEvidenceChainReport> {
        let entries = self.load_entries()?.entries;
        let candidates = self.load_candidates()?.candidates;
        let usage_records = self.load_usage_records()?;
        let audit_records = self.load_audit_records()?;
        let report_dir = self.promotion_evidence_chain_report_dir(now);
        let raw_entry_id = entry_id.trim();
        let entry_id = operator_safe_text(raw_entry_id);
        let current_entry = entries
            .iter()
            .find(|entry| entry.id == raw_entry_id)
            .map(human_entry_snapshot);
        let promotion_audit = promotion_audit_for_entry_id(raw_entry_id, &audit_records)
            .map(operator_safe_audit_record);
        let candidate_snapshot = promotion_audit
            .as_ref()
            .and_then(|audit| audit.candidate_snapshot.clone());
        let entry_snapshot = promotion_audit
            .as_ref()
            .and_then(|audit| audit.entry_snapshot.clone());
        let usage_records_for_entry: Vec<_> = usage_records
            .iter()
            .filter(|usage| usage.entry_id == raw_entry_id)
            .map(operator_safe_usage_record)
            .collect();
        let related_audit_records: Vec<_> = audit_records
            .iter()
            .filter(|audit| {
                audit.entry_id.as_deref() == Some(raw_entry_id) || audit.subject_id == raw_entry_id
            })
            .map(operator_safe_audit_record)
            .collect();

        let mut failures = Vec::new();
        if current_entry.is_none() {
            failures.push(format!("entry `{entry_id}` was not found"));
        }
        if promotion_audit.is_none() {
            failures.push(format!("entry `{entry_id}` has no promotion audit record"));
        }
        if promotion_audit.is_some() && candidate_snapshot.is_none() {
            failures.push(format!(
                "promotion audit for entry `{entry_id}` has no candidate snapshot"
            ));
        }
        if promotion_audit.is_some() && entry_snapshot.is_none() {
            failures.push(format!(
                "promotion audit for entry `{entry_id}` has no entry snapshot"
            ));
        }

        let mut report = AdaptiveWikiPromotionEvidenceChainReport {
            generated_at: now,
            dry_run,
            report_dir: report_dir.display().to_string(),
            entry_id,
            promotion_audit,
            candidate_snapshot,
            entry_snapshot,
            current_entry,
            usage_records: usage_records_for_entry,
            related_audit_records,
            failures,
            summary: AdaptiveWikiPromotionEvidenceChainSummary {
                entries_checked: entries.len(),
                candidates_checked: candidates.len(),
                usage_records_checked: usage_records.len(),
                audit_records_checked: audit_records.len(),
                ..AdaptiveWikiPromotionEvidenceChainSummary::default()
            },
        };
        report.summary.promotion_audit_found = report.promotion_audit.is_some();
        report.summary.candidate_snapshot_present = report.candidate_snapshot.is_some();
        report.summary.entry_snapshot_present = report.entry_snapshot.is_some();
        report.summary.current_entry_present = report.current_entry.is_some();
        report.summary.usage_records = report.usage_records.len();
        report.summary.related_audit_records = report.related_audit_records.len();
        report.summary.failures = report.failures.len();
        if !dry_run {
            report.summary.files_written = 3;
            write_promotion_evidence_chain_report(&report_dir, &report)?;
        }
        Ok(report)
    }

    fn review_report_dir(&self, now: DateTime<Utc>) -> PathBuf {
        self.root
            .join(ADAPTIVE_WIKI_REVIEW_REPORTS_DIR)
            .join(format!("{}", now.format("%Y%m%dT%H%M%SZ")))
    }

    fn episode_report_dir(&self, now: DateTime<Utc>) -> PathBuf {
        self.root
            .join(ADAPTIVE_WIKI_EPISODE_REPORTS_DIR)
            .join(format!("{}", now.format("%Y%m%dT%H%M%SZ")))
    }

    fn live_episode_trace_dir(&self, now: DateTime<Utc>) -> PathBuf {
        self.root
            .join(ADAPTIVE_WIKI_EPISODE_TRACES_DIR)
            .join(format!("{}", now.format("%Y%m%dT%H%M%SZ")))
    }

    fn correction_recurrence_report_dir(&self, now: DateTime<Utc>) -> PathBuf {
        self.root
            .join(ADAPTIVE_WIKI_RECURRENCE_REPORTS_DIR)
            .join(format!("{}", now.format("%Y%m%dT%H%M%SZ")))
    }

    fn promotion_evidence_chain_report_dir(&self, now: DateTime<Utc>) -> PathBuf {
        self.root
            .join(ADAPTIVE_WIKI_PROMOTION_CHAINS_DIR)
            .join(format!("{}", now.format("%Y%m%dT%H%M%SZ")))
    }
}

pub fn build_ai_projection(
    entries: &[AdaptiveWikiEntry],
    query: &AdaptiveWikiQuery,
) -> Vec<AdaptiveWikiAiProjection> {
    build_ai_projection_report(entries, query, AdaptiveWikiProjectionBudget::default()).selected
}

pub fn build_ai_projection_report(
    entries: &[AdaptiveWikiEntry],
    query: &AdaptiveWikiQuery,
    budget: AdaptiveWikiProjectionBudget,
) -> AdaptiveWikiProjectionReport {
    build_ai_projection_report_with_policy(
        entries,
        query,
        budget,
        AdaptiveWikiProjectionPolicy::default(),
    )
}

pub fn build_ai_projection_report_with_policy(
    entries: &[AdaptiveWikiEntry],
    query: &AdaptiveWikiQuery,
    budget: AdaptiveWikiProjectionBudget,
    policy: AdaptiveWikiProjectionPolicy,
) -> AdaptiveWikiProjectionReport {
    let mut candidates = Vec::new();
    let mut rejected = Vec::new();
    let mut review_expired = Vec::new();
    let now = Utc::now();
    let mut summary = AdaptiveWikiProjectionSummary {
        entries_checked: entries.len(),
        ..AdaptiveWikiProjectionSummary::default()
    };
    let conflicts =
        detect_projection_conflicts(&projection_conflict_candidates(entries, Some(query)));

    for entry in entries
        .iter()
        .filter(|entry| entry.status == AdaptiveWikiStatus::Promoted)
        .filter(|entry| entry_matches_query(entry, query))
    {
        summary.promoted_scope_matches += 1;
        if let Some(review_after) = entry
            .review_after
            .filter(|review_after| review_expired_is_past(*review_after, now))
        {
            if policy.review_expired == AdaptiveWikiProjectionReviewExpiredPolicy::Exclude {
                rejected.push(projection_rejection(
                    entry,
                    AdaptiveWikiProjectionRejectionReason::ReviewExpiredExcluded,
                    format!(
                        "entry review_after {} is expired under strict projection policy",
                        review_after.to_rfc3339()
                    ),
                ));
                continue;
            }
        }
        let raw_instruction =
            operator_safe_text(&fallback_text(&entry.ai_instruction, &entry.claim));
        if raw_instruction.trim().is_empty() {
            rejected.push(projection_rejection(
                entry,
                AdaptiveWikiProjectionRejectionReason::EmptyInstruction,
                "entry has no AI instruction or claim to project",
            ));
            continue;
        }
        let (instruction, truncated) =
            projection_instruction_with_budget(&raw_instruction, budget.max_instruction_chars);
        if truncated {
            summary.instructions_truncated += 1;
        }
        candidates.push(AdaptiveWikiProjectionCandidate {
            projection: AdaptiveWikiAiProjection {
                id: entry.id.clone(),
                kind: entry.kind,
                scope: entry.scope,
                scope_ref: entry.scope_ref.clone(),
                activation_mode: entry.activation_mode,
                agent_modes: entry.agent_modes.clone(),
                instruction,
                confidence: entry.confidence,
                evidence_count: entry.evidence_refs.len(),
            },
            updated_at: entry.updated_at,
            review_after: entry.review_after,
        });
    }

    candidates.sort_by(projection_order);
    let mut selected = Vec::new();
    let mut estimated_context_chars = 0usize;
    for candidate in candidates {
        let projection = candidate.projection;
        let estimated_entry_chars = estimate_projection_context_chars(&projection);
        if selected.len() >= budget.max_entries {
            rejected.push(projection_projection_rejection(
                &projection,
                AdaptiveWikiProjectionRejectionReason::BudgetMaxEntries,
                format!(
                    "projection budget selected at most {} entries",
                    budget.max_entries
                ),
            ));
            continue;
        }
        if estimated_context_chars.saturating_add(estimated_entry_chars) > budget.max_context_chars
            && !selected.is_empty()
        {
            rejected.push(projection_projection_rejection(
                &projection,
                AdaptiveWikiProjectionRejectionReason::BudgetMaxContextChars,
                format!(
                    "estimated context chars would exceed {}",
                    budget.max_context_chars
                ),
            ));
            continue;
        }
        estimated_context_chars = estimated_context_chars.saturating_add(estimated_entry_chars);
        if let Some(review_after) = candidate
            .review_after
            .filter(|review_after| review_expired_is_past(*review_after, now))
        {
            review_expired.push(projection_review_expired(&projection, review_after));
        }
        selected.push(projection);
    }

    summary.selected = selected.len();
    summary.rejected = rejected.len();
    summary.conflicts = conflicts.len();
    summary.review_expired_projected = review_expired.len();
    summary.estimated_context_chars = estimated_context_chars;
    AdaptiveWikiProjectionReport {
        query: query.clone(),
        budget,
        policy,
        summary,
        selected,
        rejected,
        conflicts,
        review_expired,
    }
}

pub fn build_ai_projection_review_expired_policy_comparison(
    entries: &[AdaptiveWikiEntry],
    query: &AdaptiveWikiQuery,
    budget: AdaptiveWikiProjectionBudget,
) -> AdaptiveWikiProjectionComparisonReport {
    let warn = build_ai_projection_report_with_policy(
        entries,
        query,
        budget.clone(),
        AdaptiveWikiProjectionPolicy {
            review_expired: AdaptiveWikiProjectionReviewExpiredPolicy::Warn,
        },
    );
    let strict = build_ai_projection_report_with_policy(
        entries,
        query,
        budget.clone(),
        AdaptiveWikiProjectionPolicy {
            review_expired: AdaptiveWikiProjectionReviewExpiredPolicy::Exclude,
        },
    );
    let summary = projection_comparison_summary(&warn, &strict);
    AdaptiveWikiProjectionComparisonReport {
        query: query.clone(),
        budget,
        summary,
        warn,
        strict,
    }
}

fn projection_comparison_summary(
    warn: &AdaptiveWikiProjectionReport,
    strict: &AdaptiveWikiProjectionReport,
) -> AdaptiveWikiProjectionComparisonSummary {
    AdaptiveWikiProjectionComparisonSummary {
        warn_selected: warn.summary.selected,
        strict_selected: strict.summary.selected,
        warn_rejected: warn.summary.rejected,
        strict_rejected: strict.summary.rejected,
        warn_estimated_context_chars: warn.summary.estimated_context_chars,
        strict_estimated_context_chars: strict.summary.estimated_context_chars,
        selected_only_in_warn: projection_id_difference(&warn.selected, &strict.selected),
        selected_only_in_strict: projection_id_difference(&strict.selected, &warn.selected),
        review_expired_excluded: projection_rejection_ids(
            &strict.rejected,
            AdaptiveWikiProjectionRejectionReason::ReviewExpiredExcluded,
        ),
    }
}

fn runtime_policy_comparison_hash(
    comparison: &AdaptiveWikiProjectionComparisonReport,
    policy: AdaptiveWikiProjectionPolicy,
) -> Result<String> {
    #[derive(Serialize)]
    struct RuntimePolicyComparisonFingerprint<'a> {
        query: &'a AdaptiveWikiQuery,
        budget: &'a AdaptiveWikiProjectionBudget,
        policy: AdaptiveWikiProjectionPolicy,
        warn_selected: Vec<&'a str>,
        strict_selected: Vec<&'a str>,
        selected_only_in_warn: &'a [String],
        selected_only_in_strict: &'a [String],
        review_expired_excluded: &'a [String],
    }

    let fingerprint = RuntimePolicyComparisonFingerprint {
        query: &comparison.query,
        budget: &comparison.budget,
        policy,
        warn_selected: comparison
            .warn
            .selected
            .iter()
            .map(|entry| entry.id.as_str())
            .collect(),
        strict_selected: comparison
            .strict
            .selected
            .iter()
            .map(|entry| entry.id.as_str())
            .collect(),
        selected_only_in_warn: &comparison.summary.selected_only_in_warn,
        selected_only_in_strict: &comparison.summary.selected_only_in_strict,
        review_expired_excluded: &comparison.summary.review_expired_excluded,
    };
    Ok(sha256_hex(&serde_json::to_vec(&fingerprint)?))
}

fn project_artifact_runtime_ack_query(query: &AdaptiveWikiQuery) -> Option<AdaptiveWikiQuery> {
    Some(AdaptiveWikiQuery {
        session_id: None,
        project_key: Some(query.project_key.as_ref()?.clone()),
        artifact_kind: Some(query.artifact_kind.as_ref()?.clone()),
        agent_mode: query.agent_mode,
        agent_mode_filter: query.agent_mode_filter,
    })
}

fn comparison_has_session_scope(comparison: &AdaptiveWikiProjectionComparisonReport) -> bool {
    projection_report_has_session_scope(&comparison.warn)
        || projection_report_has_session_scope(&comparison.strict)
}

fn projection_report_has_session_scope(report: &AdaptiveWikiProjectionReport) -> bool {
    report
        .selected
        .iter()
        .any(|entry| entry.scope == AdaptiveWikiScope::Session)
        || report
            .rejected
            .iter()
            .any(|entry| entry.scope == AdaptiveWikiScope::Session)
        || report
            .review_expired
            .iter()
            .any(|entry| entry.scope == AdaptiveWikiScope::Session)
}

fn projection_id_difference(
    left: &[AdaptiveWikiAiProjection],
    right: &[AdaptiveWikiAiProjection],
) -> Vec<String> {
    let right_ids: BTreeSet<_> = right.iter().map(|entry| entry.id.as_str()).collect();
    left.iter()
        .filter(|entry| !right_ids.contains(entry.id.as_str()))
        .map(|entry| operator_safe_text(&entry.id))
        .collect()
}

fn projection_rejection_ids(
    rejections: &[AdaptiveWikiProjectionRejection],
    reason: AdaptiveWikiProjectionRejectionReason,
) -> Vec<String> {
    let mut ids: Vec<_> = rejections
        .iter()
        .filter(|rejection| rejection.reason == reason)
        .map(|rejection| operator_safe_text(&rejection.entry_id))
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn projection_rejection(
    entry: &AdaptiveWikiEntry,
    reason: AdaptiveWikiProjectionRejectionReason,
    detail: impl Into<String>,
) -> AdaptiveWikiProjectionRejection {
    AdaptiveWikiProjectionRejection {
        entry_id: operator_safe_text(&entry.id),
        kind: entry.kind,
        scope: entry.scope,
        scope_ref: operator_safe_text(&entry.scope_ref),
        reason,
        detail: operator_safe_text(&detail.into()),
    }
}

fn projection_projection_rejection(
    projection: &AdaptiveWikiAiProjection,
    reason: AdaptiveWikiProjectionRejectionReason,
    detail: impl Into<String>,
) -> AdaptiveWikiProjectionRejection {
    AdaptiveWikiProjectionRejection {
        entry_id: operator_safe_text(&projection.id),
        kind: projection.kind,
        scope: projection.scope,
        scope_ref: operator_safe_text(&projection.scope_ref),
        reason,
        detail: operator_safe_text(&detail.into()),
    }
}

fn projection_review_expired(
    projection: &AdaptiveWikiAiProjection,
    review_after: DateTime<Utc>,
) -> AdaptiveWikiProjectionReviewExpired {
    AdaptiveWikiProjectionReviewExpired {
        entry_id: operator_safe_text(&projection.id),
        kind: projection.kind,
        scope: projection.scope,
        scope_ref: operator_safe_text(&projection.scope_ref),
        review_after,
        detail: "entry is past review_after but remains selected under the default warn policy"
            .to_string(),
    }
}

fn review_expired_is_past(review_after: DateTime<Utc>, now: DateTime<Utc>) -> bool {
    review_after <= now
}

fn projection_conflict_candidates(
    entries: &[AdaptiveWikiEntry],
    query: Option<&AdaptiveWikiQuery>,
) -> Vec<ProjectionConflictCandidate> {
    entries
        .iter()
        .filter(|entry| entry.status == AdaptiveWikiStatus::Promoted)
        .filter(|entry| {
            query
                .map(|query| entry_matches_query(entry, query))
                .unwrap_or(true)
        })
        .filter_map(|entry| {
            let instruction =
                operator_safe_text(&fallback_text(&entry.ai_instruction, &entry.claim));
            let signature = projection_conflict_signature(&instruction)?;
            Some(ProjectionConflictCandidate {
                entry_id: operator_safe_text(&entry.id),
                kind: entry.kind,
                scope: entry.scope,
                scope_ref: operator_safe_text(&entry.scope_ref),
                signature,
            })
        })
        .collect()
}

fn detect_projection_conflicts(
    candidates: &[ProjectionConflictCandidate],
) -> Vec<AdaptiveWikiProjectionConflict> {
    let mut conflicts = Vec::new();
    for (index, entry) in candidates.iter().enumerate() {
        for other in candidates.iter().skip(index + 1) {
            if entry.kind != other.kind
                || entry.scope != other.scope
                || entry.scope_ref != other.scope_ref
                || entry.signature.target != other.signature.target
                || entry.signature.polarity == other.signature.polarity
            {
                continue;
            }
            conflicts.push(AdaptiveWikiProjectionConflict {
                entry_id: entry.entry_id.clone(),
                conflicting_entry_id: other.entry_id.clone(),
                kind: entry.kind,
                scope: entry.scope,
                scope_ref: entry.scope_ref.clone(),
                signature: entry.signature.target.clone(),
                entry_polarity: entry.signature.polarity,
                conflicting_polarity: other.signature.polarity,
                detail: operator_safe_text(&format!(
                    "Promoted entries share kind/scope and opposite projection polarity for `{}`.",
                    entry.signature.target
                )),
            });
        }
    }
    conflicts
}

fn projection_conflict_signature(instruction: &str) -> Option<ProjectionConflictSignature> {
    let normalized = normalize_conflict_text(instruction);
    if normalized.is_empty() {
        return None;
    }
    let (polarity, target) = if let Some(target) = strip_first_prefix(
        &normalized,
        &[
            "do not ",
            "must not ",
            "should not ",
            "never ",
            "avoid ",
            "cannot ",
            "can not ",
        ],
    ) {
        (
            AdaptiveWikiProjectionConflictPolarity::Negative,
            strip_projection_action_prefix(target),
        )
    } else {
        let target = strip_first_prefix(
            &normalized,
            &[
                "always ", "must ", "should ", "use ", "prefer ", "allow ", "include ", "keep ",
            ],
        )?;
        (
            AdaptiveWikiProjectionConflictPolarity::Positive,
            strip_projection_action_prefix(target),
        )
    };
    let target = target.trim();
    if target.split_whitespace().count() < 2 || target.len() < 8 {
        return None;
    }
    Some(ProjectionConflictSignature {
        polarity,
        target: target.to_string(),
    })
}

fn strip_projection_action_prefix(value: &str) -> &str {
    strip_first_prefix(
        value.trim(),
        &["use ", "prefer ", "allow ", "include ", "keep "],
    )
    .unwrap_or(value.trim())
}

fn strip_first_prefix<'a>(value: &'a str, prefixes: &[&str]) -> Option<&'a str> {
    prefixes
        .iter()
        .find_map(|prefix| value.strip_prefix(prefix))
}

fn normalize_conflict_text(value: &str) -> String {
    let value = value.replace("don't", "do not");
    let mut normalized = String::new();
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push(' ');
        }
    }
    normalized.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn projection_instruction_with_budget(input: &str, max_chars: usize) -> (String, bool) {
    if max_chars == 0 || input.chars().count() <= max_chars {
        return (input.to_string(), false);
    }
    if max_chars <= 3 {
        return (input.chars().take(max_chars).collect(), true);
    }
    let mut output: String = input.chars().take(max_chars - 3).collect();
    output.push_str("...");
    (output, true)
}

fn estimate_projection_context_chars(projection: &AdaptiveWikiAiProjection) -> usize {
    120 + projection.id.len() + projection.scope_ref.len() + projection.instruction.len()
}

fn projection_order(
    left: &AdaptiveWikiProjectionCandidate,
    right: &AdaptiveWikiProjectionCandidate,
) -> Ordering {
    let left_projection = &left.projection;
    let right_projection = &right.projection;
    scope_specificity(left_projection.scope)
        .cmp(&scope_specificity(right_projection.scope))
        .then_with(|| {
            confidence_order(left_projection.confidence)
                .cmp(&confidence_order(right_projection.confidence))
        })
        .then_with(|| {
            right_projection
                .evidence_count
                .cmp(&left_projection.evidence_count)
        })
        .then_with(|| right.updated_at.cmp(&left.updated_at))
        .then_with(|| {
            activation_order(left_projection.activation_mode)
                .cmp(&activation_order(right_projection.activation_mode))
        })
        .then_with(|| left_projection.id.cmp(&right_projection.id))
}

pub fn build_runtime_projection(
    entries: &[AdaptiveWikiAiProjection],
) -> Option<AdaptiveWikiRuntimeProjection> {
    if entries.is_empty() {
        return None;
    }

    let mut context = String::from(
        "<adaptive-wiki-context>\n\
The following entries are promoted, scope-matching adaptive wiki context.\n\
They are informational and must not override approval, command, workdir, provider, model, or launch-spec safety rails.\n",
    );
    for entry in entries {
        context.push_str(&format!(
            "\n- [{}] kind={:?} scope={:?}:{} activation={:?} agent_modes={} confidence={:?} evidence_count={}\n  {}\n",
            operator_safe_text(&entry.id),
            entry.kind,
            entry.scope,
            operator_safe_text(&entry.scope_ref),
            entry.activation_mode,
            agent_modes_label(&entry.agent_modes),
            entry.confidence,
            entry.evidence_count,
            operator_safe_text(&entry.instruction)
        ));
    }
    context.push_str("</adaptive-wiki-context>");

    Some(AdaptiveWikiRuntimeProjection {
        entry_ids: entries
            .iter()
            .map(|entry| operator_safe_text(&entry.id))
            .collect(),
        context,
    })
}

pub fn build_usage_records(
    entries: &[AdaptiveWikiAiProjection],
    task_id: &str,
    request_id: &str,
    project_key: &str,
    artifact_kind: Option<&str>,
    projection_kind: &str,
    now: DateTime<Utc>,
) -> Vec<AdaptiveWikiUsageRecord> {
    build_usage_records_with_policy(
        entries,
        AdaptiveWikiUsageContext {
            task_id,
            request_id,
            project_key,
            artifact_kind,
            agent_mode: None,
            projection_kind,
            projection_policy: None,
            now,
        },
    )
}

pub fn build_usage_records_with_policy(
    entries: &[AdaptiveWikiAiProjection],
    context: AdaptiveWikiUsageContext<'_>,
) -> Vec<AdaptiveWikiUsageRecord> {
    entries
        .iter()
        .map(|entry| AdaptiveWikiUsageRecord {
            id: format!("wiki_usage_{}", Uuid::new_v4()),
            entry_id: operator_safe_text(&entry.id),
            task_id: operator_safe_text(context.task_id),
            request_id: operator_safe_text(context.request_id),
            project_key: operator_safe_text(context.project_key),
            artifact_kind: context.artifact_kind.map(operator_safe_text),
            agent_mode: context.agent_mode,
            projection_kind: operator_safe_text(context.projection_kind),
            projection_policy: context.projection_policy,
            activation_mode: entry.activation_mode,
            created_at: context.now,
        })
        .collect()
}

pub fn build_human_projection(
    entries: &[AdaptiveWikiEntry],
    candidates: &[AdaptiveWikiCandidate],
    query: &AdaptiveWikiQuery,
) -> AdaptiveWikiHumanProjection {
    let mut entries: Vec<_> = entries
        .iter()
        .filter(|entry| is_unfiltered_query(query) || entry_matches_query(entry, query))
        .map(|entry| AdaptiveWikiHumanEntry {
            id: entry.id.clone(),
            kind: entry.kind,
            scope: entry.scope,
            scope_ref: operator_safe_text(&entry.scope_ref),
            status: entry.status,
            activation_mode: entry.activation_mode,
            agent_modes: entry.agent_modes.clone(),
            claim: operator_safe_text(&entry.claim),
            human_summary: operator_safe_text(&entry.human_summary),
            evidence_refs: entry
                .evidence_refs
                .iter()
                .map(|value| operator_safe_text(value))
                .collect(),
            counterexamples: entry
                .counterexamples
                .iter()
                .map(|value| operator_safe_text(value))
                .collect(),
            contested: !entry.counterexamples.is_empty(),
            support_refs: entry
                .support_refs
                .iter()
                .map(|value| operator_safe_text(value))
                .collect(),
            capability_ids: entry
                .capability_ids
                .iter()
                .map(|value| operator_safe_text(value))
                .collect(),
            required_artifact_kinds: entry
                .required_artifact_kinds
                .iter()
                .map(|value| operator_safe_text(value))
                .collect(),
            core_tags: clean_tags(entry.core_tags.clone()),
            proposed_tags: clean_tags(entry.proposed_tags.clone()),
            confidence: entry.confidence,
            updated_at: entry.updated_at,
            review_after: entry.review_after,
        })
        .collect();
    entries.sort_by_key(|entry| {
        (
            scope_specificity(entry.scope),
            status_order(entry.status),
            entry.id.clone(),
        )
    });

    let mut candidates: Vec<_> = candidates
        .iter()
        .filter(|candidate| is_unfiltered_query(query) || candidate_matches_query(candidate, query))
        .map(|candidate| AdaptiveWikiHumanCandidate {
            id: candidate.id.clone(),
            kind: candidate.kind,
            scope: candidate.scope,
            scope_ref: operator_safe_text(&candidate.scope_ref),
            agent_modes: candidate.agent_modes.clone(),
            claim: operator_safe_text(&candidate.claim),
            human_summary: operator_safe_text(&candidate.human_summary),
            evidence_refs: candidate
                .evidence_refs
                .iter()
                .map(|value| operator_safe_text(value))
                .collect(),
            signal_kind: candidate.signal_kind,
            origin: candidate.origin,
            source_refs: candidate
                .source_refs
                .iter()
                .map(|value| operator_safe_text(value))
                .collect(),
            source_hashes: candidate
                .source_hashes
                .iter()
                .map(|value| operator_safe_text(value))
                .collect(),
            core_tags: clean_tags(candidate.core_tags.clone()),
            proposed_tags: clean_tags(candidate.proposed_tags.clone()),
            suggested_scope: candidate
                .suggested_scope
                .as_ref()
                .map(operator_safe_scope_suggestion),
            review_reason: operator_safe_text(&candidate.review_reason),
            occurrence_count: candidate.occurrence_count,
            confidence: candidate.confidence,
            updated_at: candidate.updated_at,
            last_seen_at: candidate.last_seen_at,
        })
        .collect();
    candidates.sort_by_key(|candidate| {
        (
            scope_specificity(candidate.scope),
            std::cmp::Reverse(candidate.occurrence_count),
            candidate.id.clone(),
        )
    });

    AdaptiveWikiHumanProjection {
        entries,
        candidates,
    }
}

pub fn build_lint_report(
    entries: &[AdaptiveWikiEntry],
    candidates: &[AdaptiveWikiCandidate],
    now: DateTime<Utc>,
) -> AdaptiveWikiLintReport {
    let mut issues = Vec::new();

    for entry in entries {
        let subject_id = fallback_subject_id(&entry.id);
        if entry.id.trim().is_empty() {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Error,
                "entry",
                &subject_id,
                "missing_id",
                "Entry is missing an id.",
            ));
        }
        if entry.status == AdaptiveWikiStatus::Promoted && entry.evidence_refs.is_empty() {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Warning,
                "entry",
                &subject_id,
                "promoted_without_evidence",
                "Promoted entry has no evidence refs.",
            ));
        }
        if entry.status == AdaptiveWikiStatus::Promoted
            && fallback_text(&entry.ai_instruction, &entry.claim)
                .trim()
                .is_empty()
        {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Error,
                "entry",
                &subject_id,
                "empty_runtime_instruction",
                "Promoted entry has no AI instruction or claim for projection.",
            ));
        }
        if entry
            .review_after
            .as_ref()
            .is_some_and(|review_after| *review_after <= now)
            && entry.status == AdaptiveWikiStatus::Promoted
        {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Warning,
                "entry",
                &subject_id,
                "review_expired",
                "Promoted entry is past review_after.",
            ));
        }
        if entry.status == AdaptiveWikiStatus::Promoted
            && entry.confidence == AdaptiveWikiConfidence::Inferred
        {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Warning,
                "entry",
                &subject_id,
                "promoted_low_confidence",
                "Promoted entry is inferred confidence and should be reviewed.",
            ));
        }
        if entry.status == AdaptiveWikiStatus::Promoted && !entry.counterexamples.is_empty() {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Warning,
                "entry",
                &subject_id,
                "contested_entry",
                "Promoted entry has counterexamples and should be reviewed.",
            ));
        }
        let has_runbook_links = !entry.support_refs.is_empty()
            || !entry.capability_ids.is_empty()
            || !entry.required_artifact_kinds.is_empty();
        if entry.kind != AdaptiveWikiKind::Procedure && has_runbook_links {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Warning,
                "entry",
                &subject_id,
                "runbook_links_on_non_procedure",
                "Runbook support refs are only valid on procedure entries.",
            ));
        }
        if entry.kind == AdaptiveWikiKind::Procedure
            && entry.status == AdaptiveWikiStatus::Promoted
            && !entry.required_artifact_kinds.is_empty()
            && entry.capability_ids.is_empty()
        {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Warning,
                "entry",
                &subject_id,
                "procedure_artifact_without_capability",
                "Procedure entry declares artifact kinds but no capability ids.",
            ));
        }
        if entry.kind == AdaptiveWikiKind::Procedure
            && entry.status == AdaptiveWikiStatus::Promoted
            && !has_runbook_links
        {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Info,
                "entry",
                &subject_id,
                "procedure_without_runbook_links",
                "Procedure entry has no support refs, capability ids, or required artifact kinds.",
            ));
        }
    }

    for candidate in candidates {
        let subject_id = fallback_subject_id(&candidate.id);
        if candidate.id.trim().is_empty() {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Error,
                "candidate",
                &subject_id,
                "missing_id",
                "Candidate is missing an id.",
            ));
        }
        if candidate.claim.trim().is_empty() {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Error,
                "candidate",
                &subject_id,
                "empty_claim",
                "Candidate has no claim.",
            ));
        }
        if candidate.evidence_refs.is_empty() && candidate.source_refs.is_empty() {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Warning,
                "candidate",
                &subject_id,
                "candidate_without_source",
                "Candidate has no evidence or source refs.",
            ));
        }
        if candidate.occurrence_count == 0 {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Warning,
                "candidate",
                &subject_id,
                "zero_occurrence_count",
                "Candidate occurrence_count is zero; legacy/default repair should set it to one on next merge.",
            ));
        }
        if candidate.signal_kind == AdaptiveWikiSignalKind::Unknown {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Info,
                "candidate",
                &subject_id,
                "unknown_signal_kind",
                "Candidate has no signal_kind; this is expected for legacy rows.",
            ));
        }
        if candidate.last_seen_at + Duration::days(STALE_CANDIDATE_DAYS) <= now {
            issues.push(lint_issue(
                AdaptiveWikiLintSeverity::Info,
                "candidate",
                &subject_id,
                "stale_candidate",
                "Candidate has not been seen recently and should be promoted, rejected, or refreshed.",
            ));
        }
    }

    let summary = lint_summary(entries.len(), candidates.len(), &issues);
    AdaptiveWikiLintReport {
        generated_at: now,
        summary,
        issues,
    }
}

pub fn build_graph_report(
    entries: &[AdaptiveWikiEntry],
    candidates: &[AdaptiveWikiCandidate],
    now: DateTime<Utc>,
) -> AdaptiveWikiGraphReport {
    let mut nodes = Vec::new();
    let mut graph = AdaptiveWikiGraphDraft::default();

    for entry in entries {
        let source = format!("entry:{}", fallback_subject_id(&entry.id));
        nodes.push(AdaptiveWikiGraphNode {
            id: source.clone(),
            node_type: "entry".to_string(),
            wiki_id: operator_safe_text(&entry.id),
            label: operator_safe_text(&fallback_text(&entry.human_summary, &entry.claim)),
            tag: None,
            tag_class: None,
        });

        add_tag_edges(
            TagSubject::new("entry", &entry.id),
            &source,
            &derived_entry_tags(entry),
            AdaptiveWikiTagClass::DerivedCore,
            &mut graph,
        );
        add_tag_edges(
            TagSubject::new("entry", &entry.id),
            &source,
            &clean_tags(entry.core_tags.clone()),
            AdaptiveWikiTagClass::Core,
            &mut graph,
        );
        add_tag_edges(
            TagSubject::new("entry", &entry.id),
            &source,
            &clean_tags(entry.proposed_tags.clone()),
            AdaptiveWikiTagClass::Proposed,
            &mut graph,
        );
    }

    for candidate in candidates {
        let source = format!("candidate:{}", fallback_subject_id(&candidate.id));
        nodes.push(AdaptiveWikiGraphNode {
            id: source.clone(),
            node_type: "candidate".to_string(),
            wiki_id: operator_safe_text(&candidate.id),
            label: operator_safe_text(&fallback_text(&candidate.human_summary, &candidate.claim)),
            tag: None,
            tag_class: None,
        });

        add_tag_edges(
            TagSubject::new("candidate", &candidate.id),
            &source,
            &derived_candidate_tags(candidate),
            AdaptiveWikiTagClass::DerivedCore,
            &mut graph,
        );
        add_tag_edges(
            TagSubject::new("candidate", &candidate.id),
            &source,
            &clean_tags(candidate.core_tags.clone()),
            AdaptiveWikiTagClass::Core,
            &mut graph,
        );
        add_tag_edges(
            TagSubject::new("candidate", &candidate.id),
            &source,
            &clean_tags(candidate.proposed_tags.clone()),
            AdaptiveWikiTagClass::Proposed,
            &mut graph,
        );
    }

    for (tag, tag_class) in graph.tag_nodes {
        nodes.push(AdaptiveWikiGraphNode {
            id: tag_node_id(&tag),
            node_type: "tag".to_string(),
            wiki_id: String::new(),
            label: format!("#{tag}"),
            tag: Some(tag),
            tag_class: Some(tag_class),
        });
    }

    nodes.sort_by(|left, right| left.id.cmp(&right.id));
    let mut edges = graph.edges;
    edges.sort_by(|left, right| {
        left.source
            .cmp(&right.source)
            .then_with(|| left.relationship.cmp(&right.relationship))
            .then_with(|| left.target.cmp(&right.target))
    });
    let mut review_issues = graph.review_issues;
    review_issues.sort_by(|left, right| {
        left.subject_kind
            .cmp(&right.subject_kind)
            .then_with(|| left.subject_id.cmp(&right.subject_id))
            .then_with(|| left.tag.cmp(&right.tag))
            .then_with(|| left.code.cmp(&right.code))
    });

    let summary = AdaptiveWikiGraphSummary {
        entries: entries.len(),
        candidates: candidates.len(),
        tag_nodes: nodes.iter().filter(|node| node.node_type == "tag").count(),
        core_tag_edges: edges
            .iter()
            .filter(|edge| edge.tag_class == Some(AdaptiveWikiTagClass::Core))
            .count(),
        proposed_tag_edges: edges
            .iter()
            .filter(|edge| edge.tag_class == Some(AdaptiveWikiTagClass::Proposed))
            .count(),
        derived_core_tag_edges: edges
            .iter()
            .filter(|edge| edge.tag_class == Some(AdaptiveWikiTagClass::DerivedCore))
            .count(),
        review_issues: review_issues.len(),
    };

    AdaptiveWikiGraphReport {
        generated_at: now,
        summary,
        registry: tag_registry_rules(),
        nodes,
        edges,
        review_issues,
    }
}

pub fn build_graph_export_files(report: &AdaptiveWikiGraphReport) -> Result<Vec<(String, String)>> {
    Ok(vec![
        (
            "graph.json".to_string(),
            serde_json::to_string_pretty(report)?,
        ),
        ("graph.md".to_string(), graph_markdown(report)),
    ])
}

#[derive(Default)]
struct AdaptiveWikiGraphDraft {
    tag_nodes: BTreeMap<String, AdaptiveWikiTagClass>,
    edges: Vec<AdaptiveWikiGraphEdge>,
    review_issues: Vec<AdaptiveWikiTagReviewIssue>,
}

struct TagSubject<'a> {
    kind: &'a str,
    id: &'a str,
}

impl<'a> TagSubject<'a> {
    fn new(kind: &'a str, id: &'a str) -> Self {
        Self { kind, id }
    }
}

fn tag_registry_rules() -> Vec<AdaptiveWikiTagRegistryRule> {
    [
        ("project/", "Project or repository key.", "core"),
        ("agent/", "Agent mode or role lens.", "core"),
        ("mode/", "Execution mode such as offdesk.", "core"),
        (
            "artifact/",
            "Artifact class such as report or evidence.",
            "core",
        ),
        ("evidence/", "Evidence class or gate name.", "core"),
        ("risk/", "Operator or runtime risk class.", "core"),
        ("status/", "Lifecycle or reportability status.", "core"),
        (
            "capability/",
            "Capability id used by Offdesk rails.",
            "core",
        ),
        ("kind/", "Adaptive wiki entry kind.", "derived_core"),
        ("scope/", "Adaptive wiki scope kind.", "derived_core"),
        (
            "confidence/",
            "Adaptive wiki confidence class.",
            "derived_core",
        ),
        ("signal/", "Candidate signal kind.", "derived_core"),
    ]
    .into_iter()
    .map(
        |(prefix, description, lifecycle)| AdaptiveWikiTagRegistryRule {
            prefix: prefix.to_string(),
            description: description.to_string(),
            lifecycle: lifecycle.to_string(),
        },
    )
    .collect()
}

fn core_tag_prefixes() -> &'static [&'static str] {
    &[
        "project/",
        "agent/",
        "mode/",
        "artifact/",
        "evidence/",
        "risk/",
        "status/",
        "capability/",
        "kind/",
        "scope/",
        "confidence/",
        "signal/",
    ]
}

fn derived_entry_tags(entry: &AdaptiveWikiEntry) -> Vec<String> {
    let mut tags = vec![
        format!("kind/{}", kind_label(entry.kind)),
        format!("scope/{}", scope_label(entry.scope)),
        format!("status/{}", status_label(entry.status)),
        format!("confidence/{}", confidence_label(entry.confidence)),
    ];
    match entry.scope {
        AdaptiveWikiScope::Project => push_tag_component(&mut tags, "project", &entry.scope_ref),
        AdaptiveWikiScope::ArtifactKind => {
            push_tag_component(&mut tags, "artifact", &entry.scope_ref)
        }
        AdaptiveWikiScope::Session | AdaptiveWikiScope::UserGlobal => {}
    }
    if entry.agent_modes.is_empty() {
        tags.push("agent/shared".to_string());
    } else {
        tags.extend(
            entry
                .agent_modes
                .iter()
                .map(|mode| format!("agent/{}", agent_mode_label(*mode))),
        );
    }
    for capability_id in &entry.capability_ids {
        push_tag_component(&mut tags, "capability", capability_id);
    }
    for artifact_kind in &entry.required_artifact_kinds {
        push_tag_component(&mut tags, "artifact", artifact_kind);
    }
    clean_tags(tags)
}

fn derived_candidate_tags(candidate: &AdaptiveWikiCandidate) -> Vec<String> {
    let mut tags = vec![
        "status/candidate".to_string(),
        format!("kind/{}", kind_label(candidate.kind)),
        format!("scope/{}", scope_label(candidate.scope)),
        format!("confidence/{}", confidence_label(candidate.confidence)),
        format!("signal/{}", signal_label(candidate.signal_kind)),
    ];
    match candidate.scope {
        AdaptiveWikiScope::Project => {
            push_tag_component(&mut tags, "project", &candidate.scope_ref)
        }
        AdaptiveWikiScope::ArtifactKind => {
            push_tag_component(&mut tags, "artifact", &candidate.scope_ref)
        }
        AdaptiveWikiScope::Session | AdaptiveWikiScope::UserGlobal => {}
    }
    if candidate.agent_modes.is_empty() {
        tags.push("agent/shared".to_string());
    } else {
        tags.extend(
            candidate
                .agent_modes
                .iter()
                .map(|mode| format!("agent/{}", agent_mode_label(*mode))),
        );
    }
    clean_tags(tags)
}

fn push_tag_component(tags: &mut Vec<String>, prefix: &str, value: &str) {
    if let Some(component) = tag_component(value) {
        tags.push(format!("{prefix}/{component}"));
    }
}

fn add_tag_edges(
    subject: TagSubject<'_>,
    source: &str,
    tags: &[String],
    tag_class: AdaptiveWikiTagClass,
    graph: &mut AdaptiveWikiGraphDraft,
) {
    let mut seen = BTreeSet::new();
    for tag in tags {
        let tag = clean_tag(tag);
        if tag.is_empty() || !seen.insert(tag.clone()) {
            continue;
        }
        review_tag(
            subject.kind,
            subject.id,
            &tag,
            tag_class,
            &mut graph.review_issues,
        );
        if !tag_has_valid_syntax(&tag) {
            continue;
        }
        set_tag_node_class(&mut graph.tag_nodes, &tag, tag_class);
        graph.edges.push(AdaptiveWikiGraphEdge {
            source: source.to_string(),
            target: tag_node_id(&tag),
            relationship: match tag_class {
                AdaptiveWikiTagClass::DerivedCore => "has_derived_core_tag",
                AdaptiveWikiTagClass::Core => "has_core_tag",
                AdaptiveWikiTagClass::Proposed => "has_proposed_tag",
            }
            .to_string(),
            tag_class: Some(tag_class),
        });
    }
}

fn set_tag_node_class(
    tag_nodes: &mut BTreeMap<String, AdaptiveWikiTagClass>,
    tag: &str,
    tag_class: AdaptiveWikiTagClass,
) {
    match tag_nodes.get(tag).copied() {
        Some(existing) if tag_class_rank(existing) >= tag_class_rank(tag_class) => {}
        _ => {
            tag_nodes.insert(tag.to_string(), tag_class);
        }
    }
}

fn tag_class_rank(tag_class: AdaptiveWikiTagClass) -> u8 {
    match tag_class {
        AdaptiveWikiTagClass::DerivedCore => 0,
        AdaptiveWikiTagClass::Proposed => 1,
        AdaptiveWikiTagClass::Core => 2,
    }
}

fn review_tag(
    subject_kind: &str,
    subject_id: &str,
    tag: &str,
    tag_class: AdaptiveWikiTagClass,
    issues: &mut Vec<AdaptiveWikiTagReviewIssue>,
) {
    if !tag_has_valid_syntax(tag) {
        issues.push(tag_review_issue(
            AdaptiveWikiLintSeverity::Error,
            subject_kind,
            subject_id,
            tag,
            "invalid_tag_syntax",
            "Tag must use lowercase ASCII letters, numbers, slash, dot, dash, or underscore.",
        ));
        return;
    }
    if !tag.contains('/') {
        issues.push(tag_review_issue(
            AdaptiveWikiLintSeverity::Info,
            subject_kind,
            subject_id,
            tag,
            "unscoped_tag",
            "Tag has no prefix; prefer controlled prefixes such as project/, agent/, or evidence/.",
        ));
    }
    if tag.len() > 80 {
        issues.push(tag_review_issue(
            AdaptiveWikiLintSeverity::Info,
            subject_kind,
            subject_id,
            tag,
            "long_tag",
            "Tag is long; consider a shorter reusable tag and keep details in the wiki claim.",
        ));
    }
    if tag_is_overbroad(tag) {
        issues.push(tag_review_issue(
            AdaptiveWikiLintSeverity::Warning,
            subject_kind,
            subject_id,
            tag,
            "overbroad_tag",
            "Tag is likely too broad to improve retrieval, routing, or review.",
        ));
    }
    let has_core_prefix = core_tag_prefixes()
        .iter()
        .any(|prefix| tag.starts_with(prefix) && tag.len() > prefix.len());
    match tag_class {
        AdaptiveWikiTagClass::Core if !has_core_prefix => issues.push(tag_review_issue(
            AdaptiveWikiLintSeverity::Warning,
            subject_kind,
            subject_id,
            tag,
            "unknown_core_tag_prefix",
            "Core tag does not match the controlled registry prefixes.",
        )),
        AdaptiveWikiTagClass::Proposed if has_core_prefix => issues.push(tag_review_issue(
            AdaptiveWikiLintSeverity::Info,
            subject_kind,
            subject_id,
            tag,
            "proposed_tag_matches_core_prefix",
            "Proposed tag uses a controlled prefix; reviewer should promote it to core or normalize to an existing core tag.",
        )),
        AdaptiveWikiTagClass::DerivedCore | AdaptiveWikiTagClass::Core | AdaptiveWikiTagClass::Proposed => {}
    }
}

fn tag_review_issue(
    severity: AdaptiveWikiLintSeverity,
    subject_kind: &str,
    subject_id: &str,
    tag: &str,
    code: &str,
    message: &str,
) -> AdaptiveWikiTagReviewIssue {
    AdaptiveWikiTagReviewIssue {
        severity,
        subject_kind: subject_kind.to_string(),
        subject_id: fallback_subject_id(subject_id),
        tag: tag.to_string(),
        code: code.to_string(),
        message: message.to_string(),
    }
}

fn tag_has_valid_syntax(tag: &str) -> bool {
    !tag.is_empty()
        && !tag.starts_with('/')
        && !tag.ends_with('/')
        && !tag.contains("//")
        && tag.chars().all(|ch| {
            ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '/' | '_' | '-' | '.')
        })
}

fn tag_is_overbroad(tag: &str) -> bool {
    let last = tag.rsplit('/').next().unwrap_or(tag);
    matches!(
        last,
        "important" | "misc" | "todo" | "review" | "research" | "wiki" | "note" | "general"
    )
}

fn tag_node_id(tag: &str) -> String {
    format!("tag:{tag}")
}

fn tag_component(value: &str) -> Option<String> {
    let mut component = String::new();
    let mut previous_dash = false;
    for ch in operator_safe_text(value)
        .trim()
        .to_ascii_lowercase()
        .chars()
    {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '-' | '.') {
            component.push(ch);
            previous_dash = false;
        } else if !previous_dash {
            component.push('-');
            previous_dash = true;
        }
    }
    let component = component.trim_matches('-').to_string();
    (!component.is_empty()).then_some(component)
}

fn graph_markdown(report: &AdaptiveWikiGraphReport) -> String {
    let mut content = String::new();
    content.push_str("# Adaptive Wiki Tag Graph\n\n");
    content.push_str(&format!(
        "Generated: `{}`\n\n",
        report.generated_at.to_rfc3339()
    ));
    content.push_str("## Summary\n\n");
    content.push_str(&format!(
        "- Entries: `{}`\n- Candidates: `{}`\n- Tag nodes: `{}`\n- Derived core tag edges: `{}`\n- Core tag edges: `{}`\n- Proposed tag edges: `{}`\n- Review issues: `{}`\n",
        report.summary.entries,
        report.summary.candidates,
        report.summary.tag_nodes,
        report.summary.derived_core_tag_edges,
        report.summary.core_tag_edges,
        report.summary.proposed_tag_edges,
        report.summary.review_issues
    ));

    content.push_str("\n## Registry\n\n");
    content.push_str("| Prefix | Lifecycle | Description |\n");
    content.push_str("| --- | --- | --- |\n");
    for rule in &report.registry {
        content.push_str(&format!(
            "| `{}` | `{}` | {} |\n",
            table_text(&rule.prefix),
            table_text(&rule.lifecycle),
            table_text(&rule.description)
        ));
    }

    content.push_str("\n## Review Issues\n\n");
    if report.review_issues.is_empty() {
        content.push_str("_No tag review issues._\n");
    } else {
        content.push_str("| Severity | Subject | Tag | Code | Message |\n");
        content.push_str("| --- | --- | --- | --- | --- |\n");
        for issue in &report.review_issues {
            content.push_str(&format!(
                "| `{:?}` | `{}`:`{}` | `#{}` | `{}` | {} |\n",
                issue.severity,
                table_text(&issue.subject_kind),
                table_text(&issue.subject_id),
                table_text(&issue.tag),
                table_text(&issue.code),
                table_text(&issue.message)
            ));
        }
    }

    content.push_str("\n## Edges\n\n");
    content.push_str("| Source | Relationship | Tag |\n");
    content.push_str("| --- | --- | --- |\n");
    for edge in &report.edges {
        content.push_str(&format!(
            "| `{}` | `{}` | `{}` |\n",
            table_text(&edge.source),
            table_text(&edge.relationship),
            table_text(edge.target.strip_prefix("tag:").unwrap_or(&edge.target))
        ));
    }
    content
}

pub fn build_markdown_export_files(
    entries: &[AdaptiveWikiEntry],
    candidates: &[AdaptiveWikiCandidate],
    now: DateTime<Utc>,
) -> Vec<(String, String)> {
    let mut files = Vec::new();
    files.push(("SCHEMA.md".to_string(), markdown_schema()));
    files.push((
        "index.md".to_string(),
        markdown_index(entries, candidates, now),
    ));
    files.push((
        "log.md".to_string(),
        markdown_export_log(entries, candidates, now),
    ));

    let mut entries = entries.to_vec();
    entries.sort_by_key(|entry| {
        (
            kind_label(entry.kind).to_string(),
            status_order(entry.status),
            entry.id.clone(),
        )
    });
    for entry in entries {
        files.push((
            format!(
                "entries/{}/{}.md",
                kind_dir(entry.kind),
                markdown_slug(&entry.id, "entry")
            ),
            markdown_entry_page(&entry),
        ));
    }

    let mut candidates = candidates.to_vec();
    candidates.sort_by_key(|candidate| {
        (
            kind_label(candidate.kind).to_string(),
            std::cmp::Reverse(candidate.occurrence_count),
            candidate.id.clone(),
        )
    });
    for candidate in candidates {
        files.push((
            format!(
                "candidates/{}.md",
                markdown_slug(&candidate.id, "candidate")
            ),
            markdown_candidate_page(&candidate),
        ));
    }

    files
}

fn write_markdown_export(output_dir: &Path, files: &[(String, String)]) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    for relative_dir in [
        "entries/preference",
        "entries/procedure",
        "entries/failure-pattern",
        "entries/policy-rule",
        "entries/fact",
        "candidates",
        "raw/audits",
        "raw/diffs",
        "raw/docs",
        "support/references",
        "support/templates",
        "support/scripts",
    ] {
        fs::create_dir_all(output_dir.join(relative_dir))?;
    }
    for (relative_path, content) in files {
        let path = output_dir.join(relative_path);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, content)?;
    }
    Ok(())
}

fn markdown_export_report(
    output_dir: &Path,
    dry_run: bool,
    now: DateTime<Utc>,
    projection_status: AdaptiveWikiMarkdownProjectionStatus,
    entries_exported: usize,
    candidates_exported: usize,
    files: &[(String, String)],
) -> AdaptiveWikiMarkdownExportReport {
    AdaptiveWikiMarkdownExportReport {
        generated_at: now,
        output_dir: output_dir.display().to_string(),
        dry_run,
        projection_status,
        summary: AdaptiveWikiMarkdownExportSummary {
            entries_exported,
            candidates_exported,
            files_planned: files.len(),
            files_written: if dry_run { 0 } else { files.len() },
        },
        files: files
            .iter()
            .map(|(path, content)| AdaptiveWikiMarkdownExportFile {
                path: path.clone(),
                bytes: content.len(),
                sha256: sha256_hex(content.as_bytes()),
            })
            .collect(),
    }
}

fn markdown_projection_status(
    profile_dir: &Path,
    entries_path: &Path,
    candidates_path: &Path,
    vault_index_path: &Path,
) -> AdaptiveWikiMarkdownProjectionStatus {
    let entries_modified_at = file_modified_at(entries_path);
    let candidates_modified_at = file_modified_at(candidates_path);
    let vault_index_modified_at = file_modified_at(vault_index_path);
    let canonical_modified_at = [entries_modified_at, candidates_modified_at]
        .into_iter()
        .flatten()
        .max();

    let (state, reason) = match (canonical_modified_at, vault_index_modified_at) {
        (None, _) => (
            AdaptiveWikiMarkdownProjectionState::EmptyCanonical,
            "No canonical adaptive wiki entries or candidates were found.".to_string(),
        ),
        (Some(_), None) => (
            AdaptiveWikiMarkdownProjectionState::Missing,
            "Canonical adaptive wiki state exists without a markdown vault index.".to_string(),
        ),
        (Some(canonical), Some(index)) if canonical > index => (
            AdaptiveWikiMarkdownProjectionState::Stale,
            "Canonical adaptive wiki state is newer than the markdown vault index.".to_string(),
        ),
        (Some(_), Some(_)) => (
            AdaptiveWikiMarkdownProjectionState::Fresh,
            "Markdown vault index is current with canonical adaptive wiki state.".to_string(),
        ),
    };

    AdaptiveWikiMarkdownProjectionStatus {
        profile_dir: profile_dir.display().to_string(),
        entries_path: entries_path.display().to_string(),
        candidates_path: candidates_path.display().to_string(),
        vault_index_path: vault_index_path.display().to_string(),
        entries_present: entries_path.exists(),
        candidates_present: candidates_path.exists(),
        vault_index_present: vault_index_path.exists(),
        entries_modified_at,
        candidates_modified_at,
        vault_index_modified_at,
        canonical_modified_at,
        state,
        reexport_recommended: matches!(
            state,
            AdaptiveWikiMarkdownProjectionState::Missing
                | AdaptiveWikiMarkdownProjectionState::Stale
        ),
        reason,
    }
}

fn file_modified_at(path: &Path) -> Option<DateTime<Utc>> {
    fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .map(DateTime::<Utc>::from)
}

pub fn build_review_proposals(
    entries: &[AdaptiveWikiEntry],
    candidates: &[AdaptiveWikiCandidate],
    usage_records: &[AdaptiveWikiUsageRecord],
    audit_records: &[AdaptiveWikiAuditRecord],
    correction_records: &[AdaptiveWikiCorrectionRecord],
    _lint: &AdaptiveWikiLintReport,
    now: DateTime<Utc>,
) -> Vec<AdaptiveWikiReviewProposal> {
    let mut proposals = Vec::new();

    for candidate in candidates {
        if candidate.claim.trim().is_empty() {
            push_review_proposal(
                &mut proposals,
                AdaptiveWikiReviewProposalInput {
                    action: AdaptiveWikiReviewProposalAction::Reject,
                    subject_kind: "candidate",
                    subject_id: &candidate.id,
                    title: "Reject candidate without a claim",
                    rationale:
                        "The candidate has no claim, so promotion would create ambiguous knowledge.",
                    evidence_refs: candidate_evidence_refs(candidate, &["lint:empty_claim"]),
                    risk: AdaptiveWikiReviewRisk::Low,
                    suggested_command: Some(format!(
                        "forager offdesk wiki reject {} --reason \"curator review: empty claim\"",
                        candidate.id
                    )),
                },
            );
            continue;
        }

        if candidate.occurrence_count >= 2
            && (!candidate.evidence_refs.is_empty() || !candidate.source_refs.is_empty())
            && matches!(
                candidate.confidence,
                AdaptiveWikiConfidence::Explicit | AdaptiveWikiConfidence::Repeated
            )
        {
            push_review_proposal(
                &mut proposals,
                AdaptiveWikiReviewProposalInput {
                    action: AdaptiveWikiReviewProposalAction::Promote,
                    subject_kind: "candidate",
                    subject_id: &candidate.id,
                    title: "Promote repeated candidate",
                    rationale: "The candidate has repeated evidence and explicit or repeated confidence.",
                    evidence_refs: candidate_evidence_refs(candidate, &["lint:promotion_candidate"]),
                    risk: AdaptiveWikiReviewRisk::Medium,
                    suggested_command: Some(format!(
                        "forager offdesk wiki promote {} --scope {} --scope-ref {} --activation-mode confirm",
                        candidate.id,
                        scope_label(candidate.scope),
                        candidate.scope_ref
                    )),
                },
            );
        }

        if candidate.last_seen_at + Duration::days(STALE_CANDIDATE_DAYS) <= now {
            push_review_proposal(
                &mut proposals,
                AdaptiveWikiReviewProposalInput {
                    action: AdaptiveWikiReviewProposalAction::Reject,
                    subject_kind: "candidate",
                    subject_id: &candidate.id,
                    title: "Reject or refresh stale candidate",
                    rationale: "The candidate has not been seen recently and remains unpromoted.",
                    evidence_refs: candidate_evidence_refs(candidate, &["lint:stale_candidate"]),
                    risk: AdaptiveWikiReviewRisk::Low,
                    suggested_command: Some(format!(
                        "forager offdesk wiki reject {} --reason \"curator review: stale candidate\"",
                        candidate.id
                    )),
                },
            );
        }
    }

    for entry in entries {
        if entry.status != AdaptiveWikiStatus::Promoted {
            continue;
        }

        if entry
            .review_after
            .as_ref()
            .is_some_and(|review_after| *review_after <= now)
        {
            push_review_proposal(
                &mut proposals,
                AdaptiveWikiReviewProposalInput {
                    action: AdaptiveWikiReviewProposalAction::RenewReview,
                    subject_kind: "entry",
                    subject_id: &entry.id,
                    title: "Renew or revise expired review window",
                    rationale: "The entry is past review_after and should be explicitly renewed or changed.",
                    evidence_refs: entry_evidence_refs(entry, &["lint:review_expired"]),
                    risk: AdaptiveWikiReviewRisk::Medium,
                    suggested_command: None,
                },
            );
        }

        if entry.confidence == AdaptiveWikiConfidence::Inferred {
            push_review_proposal(
                &mut proposals,
                AdaptiveWikiReviewProposalInput {
                    action: AdaptiveWikiReviewProposalAction::RenewReview,
                    subject_kind: "entry",
                    subject_id: &entry.id,
                    title: "Review inferred promoted entry",
                    rationale:
                        "Promoted inferred knowledge should be confirmed, rescoped, or deprecated.",
                    evidence_refs: entry_evidence_refs(entry, &["lint:promoted_low_confidence"]),
                    risk: AdaptiveWikiReviewRisk::Medium,
                    suggested_command: None,
                },
            );
        }

        if entry.evidence_refs.is_empty() {
            push_review_proposal(
                &mut proposals,
                AdaptiveWikiReviewProposalInput {
                    action: AdaptiveWikiReviewProposalAction::Deprecate,
                    subject_kind: "entry",
                    subject_id: &entry.id,
                    title: "Deprecate promoted entry without evidence",
                    rationale: "The entry affects projection but has no evidence refs.",
                    evidence_refs: entry_evidence_refs(entry, &["lint:promoted_without_evidence"]),
                    risk: AdaptiveWikiReviewRisk::High,
                    suggested_command: Some(format!(
                        "forager offdesk wiki deprecate {} --reason \"curator review: missing evidence\"",
                        entry.id
                    )),
                },
            );
        }

        if fallback_text(&entry.ai_instruction, &entry.claim)
            .trim()
            .is_empty()
        {
            push_review_proposal(
                &mut proposals,
                AdaptiveWikiReviewProposalInput {
                    action: AdaptiveWikiReviewProposalAction::Deprecate,
                    subject_kind: "entry",
                    subject_id: &entry.id,
                    title: "Deprecate entry without runtime instruction",
                    rationale: "The entry cannot produce a safe AI projection.",
                    evidence_refs: entry_evidence_refs(entry, &["lint:empty_runtime_instruction"]),
                    risk: AdaptiveWikiReviewRisk::High,
                    suggested_command: Some(format!(
                        "forager offdesk wiki deprecate {} --reason \"curator review: empty runtime instruction\"",
                        entry.id
                    )),
                },
            );
        }

        if !entry.counterexamples.is_empty() {
            push_review_proposal(
                &mut proposals,
                AdaptiveWikiReviewProposalInput {
                    action: AdaptiveWikiReviewProposalAction::Split,
                    subject_kind: "entry",
                    subject_id: &entry.id,
                    title: "Split contested entry",
                    rationale: "Counterexamples indicate this entry may need narrower scope or separate variants.",
                    evidence_refs: entry_evidence_refs(entry, &["lint:contested_entry"]),
                    risk: AdaptiveWikiReviewRisk::Medium,
                    suggested_command: None,
                },
            );
        }
    }

    add_merge_proposals(&mut proposals, entries);
    add_projection_conflict_proposals(&mut proposals, entries);
    add_usage_proposals(&mut proposals, entries, usage_records);
    add_audit_evidence_refresh(&mut proposals, candidates, audit_records);
    add_correction_recurrence_proposals(&mut proposals, entries, audit_records, correction_records);
    add_promotion_chain_proposals(&mut proposals, entries, usage_records, audit_records);

    proposals.sort_by_key(|proposal| {
        (
            action_order(proposal.action),
            proposal.subject_kind.clone(),
            proposal.subject_id.clone(),
            proposal.id.clone(),
        )
    });
    proposals
}

fn write_review_report(output_dir: &Path, report: &AdaptiveWikiReviewReport) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    fs::write(
        output_dir.join("report.json"),
        serde_json::to_string_pretty(report)?,
    )?;
    fs::write(output_dir.join("REPORT.md"), markdown_review_report(report))?;
    Ok(())
}

fn markdown_review_report(report: &AdaptiveWikiReviewReport) -> String {
    let mut content = String::new();
    content.push_str("# Adaptive Wiki Review Report\n\n");
    content.push_str(&format!(
        "Generated: `{}`\n\n",
        report.generated_at.to_rfc3339()
    ));
    content.push_str(&format!(
        "Dry run: `{}`\n\nReport dir: `{}`\n\n",
        report.dry_run,
        table_text(&report.report_dir)
    ));
    content.push_str("## Summary\n\n");
    content.push_str(&format!(
        "- Entries checked: `{}`\n- Candidates checked: `{}`\n- Usage records checked: `{}`\n- Audit records checked: `{}`\n- Correction records checked: `{}`\n- Promotion receipts checked: `{}`\n- Invalid promotion receipt files: `{}`\n- Promoted entries with promotion receipt: `{}`\n- Promoted entries missing promotion receipt: `{}`\n- Review events checked: `{}`\n- Proposals: `{}`\n- Filtered out proposals: `{}`\n- Open proposals: `{}`\n- Proposals with events: `{}`\n- Accepted proposals: `{}`\n- Rejected proposals: `{}`\n- Superseded proposals: `{}`\n- Stale decision proposals: `{}`\n- Lint errors: `{}`\n- Lint warnings: `{}`\n- Lint info: `{}`\n\n",
        report.summary.entries_checked,
        report.summary.candidates_checked,
        report.summary.usage_records_checked,
        report.summary.audit_records_checked,
        report.summary.correction_records_checked,
        report.summary.promotion_receipts_checked,
        report.summary.promotion_receipt_files_invalid,
        report.summary.promoted_entries_with_promotion_receipt,
        report.summary.promoted_entries_missing_promotion_receipt,
        report.summary.review_events_checked,
        report.summary.proposals,
        report.summary.filtered_out_proposals,
        report.summary.open_proposals,
        report.summary.proposals_with_events,
        report.summary.accepted_proposals,
        report.summary.rejected_proposals,
        report.summary.superseded_proposals,
        report.summary.stale_decision_proposals,
        report.summary.lint_errors,
        report.summary.lint_warnings,
        report.summary.lint_info
    ));
    content.push_str("## Proposals\n\n");
    if report.proposals.is_empty() {
        content.push_str("_No proposals._\n");
        return content;
    }
    for proposal in &report.proposals {
        content.push_str(&format!(
            "### {} `{}`\n\n",
            review_action_label(proposal.action),
            table_text(&proposal.subject_id)
        ));
        content.push_str(&format!(
            "- ID: `{}`\n- Subject: `{}` `{}`\n- Risk: `{}`\n- Title: {}\n- Rationale: {}\n",
            table_text(&proposal.id),
            table_text(&proposal.subject_kind),
            table_text(&proposal.subject_id),
            review_risk_label(proposal.risk),
            table_text(&proposal.title),
            table_text(&proposal.rationale)
        ));
        if let Some(lifecycle) = proposal.lifecycle.as_ref() {
            let stale_label = if lifecycle.stale { " stale" } else { "" };
            content.push_str(&format!(
                "- Lifecycle: `{}{}` via `{}` by `{}` at `{}`\n",
                review_decision_label(lifecycle.decision),
                stale_label,
                table_text(&lifecycle.latest_event_id),
                table_text(&lifecycle.actor),
                lifecycle.decided_at.to_rfc3339()
            ));
            if !lifecycle.reason.is_empty() {
                content.push_str(&format!(
                    "- Decision reason: {}\n",
                    table_text(&lifecycle.reason)
                ));
            }
            if !lifecycle.stale_evidence_refs.is_empty() {
                content.push_str("- Stale evidence:\n");
                for evidence_ref in &lifecycle.stale_evidence_refs {
                    content.push_str(&format!("  - `{}`\n", table_text(evidence_ref)));
                }
            }
        } else {
            content.push_str("- Lifecycle: `open`\n");
        }
        if let Some(command) = proposal.suggested_command.as_deref() {
            content.push_str(&format!("- Suggested command: `{}`\n", table_text(command)));
        }
        content.push_str("- Evidence:\n");
        for evidence_ref in &proposal.evidence_refs {
            content.push_str(&format!("  - `{}`\n", table_text(evidence_ref)));
        }
        content.push('\n');
    }
    content
}

fn projected_entries_without_evidence(
    entries: &[AdaptiveWikiEntry],
    in_scope_projection: &[AdaptiveWikiAiProjection],
    out_of_scope_projection: &[AdaptiveWikiAiProjection],
) -> Vec<String> {
    let mut ids: Vec<_> = entries
        .iter()
        .filter(|entry| entry.evidence_refs.is_empty())
        .filter(|entry| {
            projection_contains_entry(in_scope_projection, &entry.id)
                || projection_contains_entry(out_of_scope_projection, &entry.id)
        })
        .map(|entry| operator_safe_text(&entry.id))
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn projected_entries_with_status(
    entries: &[AdaptiveWikiEntry],
    in_scope_projection: &[AdaptiveWikiAiProjection],
    out_of_scope_projection: &[AdaptiveWikiAiProjection],
    status: AdaptiveWikiStatus,
) -> Vec<String> {
    let mut ids: Vec<_> = entries
        .iter()
        .filter(|entry| entry.status == status)
        .filter(|entry| {
            projection_contains_entry(in_scope_projection, &entry.id)
                || projection_contains_entry(out_of_scope_projection, &entry.id)
        })
        .map(|entry| operator_safe_text(&entry.id))
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn projected_review_expired_entries(
    entries: &[AdaptiveWikiEntry],
    in_scope_projection: &[AdaptiveWikiAiProjection],
    out_of_scope_projection: &[AdaptiveWikiAiProjection],
    now: DateTime<Utc>,
) -> Vec<String> {
    let mut ids: Vec<_> = entries
        .iter()
        .filter(|entry| entry.status == AdaptiveWikiStatus::Promoted)
        .filter(|entry| {
            entry
                .review_after
                .as_ref()
                .is_some_and(|review_after| *review_after <= now)
        })
        .filter(|entry| {
            projection_contains_entry(in_scope_projection, &entry.id)
                || projection_contains_entry(out_of_scope_projection, &entry.id)
        })
        .map(|entry| operator_safe_text(&entry.id))
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn projection_contains_entry(projection: &[AdaptiveWikiAiProjection], entry_id: &str) -> bool {
    projection.iter().any(|entry| entry.id == entry_id)
}

fn build_episode_trace(
    report: &AdaptiveWikiEpisodeEvaluationReport,
) -> Vec<AdaptiveWikiEpisodeTraceStep> {
    vec![
        AdaptiveWikiEpisodeTraceStep {
            label: "in_scope_projection".to_string(),
            detail: format_episode_query(&report.in_scope_query),
            entry_ids: report
                .in_scope_projection
                .iter()
                .map(|entry| operator_safe_text(&entry.id))
                .collect(),
        },
        AdaptiveWikiEpisodeTraceStep {
            label: "out_of_scope_projection".to_string(),
            detail: format_episode_query(&report.out_of_scope_query),
            entry_ids: report
                .out_of_scope_projection
                .iter()
                .map(|entry| operator_safe_text(&entry.id))
                .collect(),
        },
        AdaptiveWikiEpisodeTraceStep {
            label: "episode_checks".to_string(),
            detail: format!(
                "passed={} failures={} scope_leakage_count={}",
                report.passed, report.summary.failures, report.summary.scope_leakage_count
            ),
            entry_ids: std::iter::once(report.target_entry_id.clone())
                .chain(report.deprecated_projected_entry_ids.iter().cloned())
                .chain(report.review_expired_projected_entry_ids.iter().cloned())
                .chain(report.projected_without_evidence_entry_ids.iter().cloned())
                .collect(),
        },
    ]
}

fn write_episode_evaluation_report(
    output_dir: &Path,
    report: &AdaptiveWikiEpisodeEvaluationReport,
) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    fs::write(
        output_dir.join("episode.json"),
        serde_json::to_string_pretty(report)?,
    )?;
    fs::write(
        output_dir.join("EPISODE.md"),
        markdown_episode_evaluation_report(report),
    )?;
    Ok(())
}

fn markdown_episode_evaluation_report(report: &AdaptiveWikiEpisodeEvaluationReport) -> String {
    let mut content = String::new();
    content.push_str("# Adaptive Wiki Episode Evaluation\n\n");
    content.push_str(&format!(
        "Generated: `{}`\n\n",
        report.generated_at.to_rfc3339()
    ));
    content.push_str(&format!(
        "Dry run: `{}`\n\nReport dir: `{}`\n\n",
        report.dry_run,
        table_text(&report.report_dir)
    ));
    content.push_str("## Summary\n\n");
    content.push_str(&format!(
        "- Passed: `{}`\n- Target entry: `{}`\n- Entries checked: `{}`\n- Candidates checked: `{}`\n- In-scope projections: `{}`\n- Out-of-scope projections: `{}`\n- Scope leakage count: `{}`\n- Review-expired projected: `{}`\n- Deprecated projected: `{}`\n- Projected without evidence: `{}`\n\n",
        report.passed,
        table_text(&report.target_entry_id),
        report.summary.entries_checked,
        report.summary.candidates_checked,
        report.summary.in_scope_projection_count,
        report.summary.out_of_scope_projection_count,
        report.summary.scope_leakage_count,
        report.summary.review_expired_entry_projected,
        report.summary.deprecated_entry_projected,
        report.summary.projected_without_evidence
    ));
    content.push_str("## Queries\n\n");
    content.push_str(&format!(
        "- In scope: `{}`\n- Out of scope: `{}`\n\n",
        table_text(&format_episode_query(&report.in_scope_query)),
        table_text(&format_episode_query(&report.out_of_scope_query))
    ));
    content.push_str("## Failures\n\n");
    if report.failures.is_empty() {
        content.push_str("_No failures._\n\n");
    } else {
        for failure in &report.failures {
            content.push_str(&format!("- {}\n", table_text(failure)));
        }
        content.push('\n');
    }
    content.push_str("## Projection Trace\n\n");
    content.push_str("### In Scope\n\n");
    markdown_episode_projection(&mut content, &report.in_scope_projection);
    content.push_str("\n### Out Of Scope\n\n");
    markdown_episode_projection(&mut content, &report.out_of_scope_projection);
    content
}

fn markdown_episode_projection(content: &mut String, projection: &[AdaptiveWikiAiProjection]) {
    if projection.is_empty() {
        content.push_str("_No entries projected._\n");
        return;
    }
    for entry in projection {
        content.push_str(&format!(
            "- `{}` kind=`{}` scope=`{}:{}` mode=`{}` evidence_count=`{}`\n",
            table_text(&entry.id),
            kind_label(entry.kind),
            scope_label(entry.scope),
            table_text(&entry.scope_ref),
            activation_label(entry.activation_mode),
            entry.evidence_count
        ));
    }
}

fn format_episode_query(query: &AdaptiveWikiQuery) -> String {
    let mut parts = Vec::new();
    if let Some(session_id) = query.session_id.as_deref() {
        parts.push(format!("session_id={}", operator_safe_text(session_id)));
    }
    if let Some(project_key) = query.project_key.as_deref() {
        parts.push(format!("project_key={}", operator_safe_text(project_key)));
    }
    if let Some(artifact_kind) = query.artifact_kind.as_deref() {
        parts.push(format!(
            "artifact_kind={}",
            operator_safe_text(artifact_kind)
        ));
    }
    if let Some(agent_mode) = query.agent_mode {
        parts.push(format!("agent_mode={}", agent_mode_label(agent_mode)));
    }
    if parts.is_empty() {
        "unfiltered".to_string()
    } else {
        parts.join(" ")
    }
}

#[allow(clippy::too_many_arguments)]
fn build_live_episode_events(
    entries: &[AdaptiveWikiEntry],
    candidates: &[AdaptiveWikiCandidate],
    usage_records: &[AdaptiveWikiUsageRecord],
    audit_records: &[AdaptiveWikiAuditRecord],
    correction_records: &[AdaptiveWikiCorrectionRecord],
    tasks: &[OffdeskTask],
    probes: &[BackgroundProbe],
    resume_states: &[TaskResumeState],
    filter: &AdaptiveWikiLiveEpisodeFilter,
    now: DateTime<Utc>,
) -> Vec<AdaptiveWikiLiveEpisodeEvent> {
    let mut events = Vec::new();

    for task in tasks
        .iter()
        .filter(|task| live_filter_matches_task(task, filter))
    {
        events.push(AdaptiveWikiLiveEpisodeEvent {
            id: live_event_id(
                AdaptiveWikiLiveEpisodeEventKind::TaskEnqueued,
                "task",
                &task.task_id,
            ),
            kind: AdaptiveWikiLiveEpisodeEventKind::TaskEnqueued,
            task_id: Some(operator_safe_text(&task.task_id)),
            request_id: Some(operator_safe_text(&task.request_id)),
            project_key: Some(operator_safe_text(&task.project_key)),
            artifact_kind: task.artifact_kind.as_deref().map(operator_safe_text),
            entry_ids: safe_refs(task.last_adaptive_wiki_entry_ids.iter()),
            candidate_id: None,
            evidence_refs: task
                .log_artifact_path
                .as_ref()
                .into_iter()
                .chain(task.result_artifact_path.as_ref())
                .map(|value| operator_safe_text(value))
                .collect(),
            summary: operator_safe_text(&format!(
                "task status={} capability={} attempts={}",
                task_status_label(task.status),
                task.capability_id,
                task.attempt_count
            )),
            occurred_at: task.created_at,
        });
        if !task.last_adaptive_wiki_entry_ids.is_empty() {
            events.push(AdaptiveWikiLiveEpisodeEvent {
                id: live_event_id(
                    AdaptiveWikiLiveEpisodeEventKind::ProjectionAttached,
                    "task",
                    &task.task_id,
                ),
                kind: AdaptiveWikiLiveEpisodeEventKind::ProjectionAttached,
                task_id: Some(operator_safe_text(&task.task_id)),
                request_id: Some(operator_safe_text(&task.request_id)),
                project_key: Some(operator_safe_text(&task.project_key)),
                artifact_kind: task.artifact_kind.as_deref().map(operator_safe_text),
                entry_ids: safe_refs(task.last_adaptive_wiki_entry_ids.iter()),
                candidate_id: None,
                evidence_refs: Vec::new(),
                summary: "task recorded adaptive wiki entry ids".to_string(),
                occurred_at: task.updated_at,
            });
        }
        if matches!(
            task.status,
            OffdeskTaskStatus::Completed
                | OffdeskTaskStatus::Failed
                | OffdeskTaskStatus::ResumePending
        ) {
            let kind = match task.status {
                OffdeskTaskStatus::Completed => AdaptiveWikiLiveEpisodeEventKind::TaskCompleted,
                OffdeskTaskStatus::Failed => AdaptiveWikiLiveEpisodeEventKind::TaskFailed,
                OffdeskTaskStatus::ResumePending => AdaptiveWikiLiveEpisodeEventKind::ResumePending,
                _ => unreachable!("filtered terminal-like task status"),
            };
            events.push(AdaptiveWikiLiveEpisodeEvent {
                id: live_event_id(kind, "task", &task.task_id),
                kind,
                task_id: Some(operator_safe_text(&task.task_id)),
                request_id: Some(operator_safe_text(&task.request_id)),
                project_key: Some(operator_safe_text(&task.project_key)),
                artifact_kind: task.artifact_kind.as_deref().map(operator_safe_text),
                entry_ids: safe_refs(task.last_adaptive_wiki_entry_ids.iter()),
                candidate_id: None,
                evidence_refs: task
                    .last_error
                    .as_ref()
                    .map(|value| vec![operator_safe_text(value)])
                    .unwrap_or_default(),
                summary: operator_safe_text(&format!(
                    "task reached {}",
                    task_status_label(task.status)
                )),
                occurred_at: task.updated_at,
            });
        }
    }

    for usage in usage_records
        .iter()
        .filter(|usage| live_filter_matches_usage(usage, filter))
    {
        events.push(AdaptiveWikiLiveEpisodeEvent {
            id: live_event_id(
                AdaptiveWikiLiveEpisodeEventKind::RuntimeUsageRecorded,
                "usage",
                &usage.id,
            ),
            kind: AdaptiveWikiLiveEpisodeEventKind::RuntimeUsageRecorded,
            task_id: Some(operator_safe_text(&usage.task_id)),
            request_id: Some(operator_safe_text(&usage.request_id)),
            project_key: Some(operator_safe_text(&usage.project_key)),
            artifact_kind: usage.artifact_kind.as_deref().map(operator_safe_text),
            entry_ids: vec![operator_safe_text(&usage.entry_id)],
            candidate_id: None,
            evidence_refs: Vec::new(),
            summary: operator_safe_text(&format!(
                "runtime projection kind={} mode={}",
                usage.projection_kind,
                activation_label(usage.activation_mode)
            )),
            occurred_at: usage.created_at,
        });
    }

    for correction in correction_records
        .iter()
        .filter(|correction| live_filter_matches_correction(correction, filter))
    {
        let kind = live_event_kind_for_correction(correction.correction_kind);
        events.push(AdaptiveWikiLiveEpisodeEvent {
            id: live_event_id(kind, "correction", &correction.id),
            kind,
            task_id: correction.task_id.as_deref().map(operator_safe_text),
            request_id: correction.request_id.as_deref().map(operator_safe_text),
            project_key: correction.project_key.as_deref().map(operator_safe_text),
            artifact_kind: correction.artifact_kind.as_deref().map(operator_safe_text),
            entry_ids: correction
                .entry_id
                .as_ref()
                .map(|entry_id| vec![operator_safe_text(entry_id)])
                .unwrap_or_default(),
            candidate_id: correction.candidate_id.as_deref().map(operator_safe_text),
            evidence_refs: safe_refs(
                correction
                    .evidence_refs
                    .iter()
                    .chain(correction.source_refs.iter()),
            ),
            summary: operator_safe_text(&format!(
                "correction kind={} {}",
                correction_kind_label(correction.correction_kind),
                correction.summary
            )),
            occurred_at: correction.created_at,
        });
    }

    for probe in probes
        .iter()
        .filter(|probe| live_filter_matches_probe(probe, filter))
    {
        if !probe.adaptive_wiki_entry_ids.is_empty() {
            events.push(AdaptiveWikiLiveEpisodeEvent {
                id: live_event_id(
                    AdaptiveWikiLiveEpisodeEventKind::ProjectionAttached,
                    "probe",
                    &probe.ticket_id,
                ),
                kind: AdaptiveWikiLiveEpisodeEventKind::ProjectionAttached,
                task_id: probe.task_id.as_deref().map(operator_safe_text),
                request_id: probe.request_id.as_deref().map(operator_safe_text),
                project_key: probe.project_key.as_deref().map(operator_safe_text),
                artifact_kind: None,
                entry_ids: safe_refs(probe.adaptive_wiki_entry_ids.iter()),
                candidate_id: None,
                evidence_refs: Vec::new(),
                summary: operator_safe_text(&format!(
                    "background probe phase={} runner={}",
                    background_phase_label(probe.phase),
                    background_runner_label(probe.runner_kind)
                )),
                occurred_at: probe
                    .handoff_emitted_at
                    .or(probe.last_observed_at)
                    .unwrap_or(now),
            });
        }
        if matches!(
            probe.phase,
            BackgroundRunnerPhase::Completed
                | BackgroundRunnerPhase::Failed
                | BackgroundRunnerPhase::StaleNoAck
                | BackgroundRunnerPhase::StaleLostCallback
                | BackgroundRunnerPhase::Reconstructable
                | BackgroundRunnerPhase::RecoveryAcknowledged
        ) {
            let kind = if probe.phase == BackgroundRunnerPhase::Completed {
                AdaptiveWikiLiveEpisodeEventKind::TaskCompleted
            } else {
                AdaptiveWikiLiveEpisodeEventKind::TaskFailed
            };
            events.push(AdaptiveWikiLiveEpisodeEvent {
                id: live_event_id(kind, "probe", &probe.ticket_id),
                kind,
                task_id: probe.task_id.as_deref().map(operator_safe_text),
                request_id: probe.request_id.as_deref().map(operator_safe_text),
                project_key: probe.project_key.as_deref().map(operator_safe_text),
                artifact_kind: None,
                entry_ids: safe_refs(probe.adaptive_wiki_entry_ids.iter()),
                candidate_id: None,
                evidence_refs: probe
                    .last_recovery_evidence
                    .as_ref()
                    .map(|value| vec![operator_safe_text(value)])
                    .unwrap_or_default(),
                summary: operator_safe_text(&format!(
                    "background probe reached {}",
                    background_phase_label(probe.phase)
                )),
                occurred_at: probe.last_observed_at.unwrap_or(now),
            });
        }
    }

    for candidate in candidates
        .iter()
        .filter(|candidate| live_filter_matches_candidate(candidate, filter))
    {
        let evidence_refs = live_candidate_evidence_refs(candidate);
        events.push(AdaptiveWikiLiveEpisodeEvent {
            id: live_event_id(
                AdaptiveWikiLiveEpisodeEventKind::CandidateRecorded,
                "candidate",
                &candidate.id,
            ),
            kind: AdaptiveWikiLiveEpisodeEventKind::CandidateRecorded,
            task_id: None,
            request_id: None,
            project_key: None,
            artifact_kind: None,
            entry_ids: Vec::new(),
            candidate_id: Some(operator_safe_text(&candidate.id)),
            evidence_refs: evidence_refs.clone(),
            summary: operator_safe_text(&format!(
                "candidate signal={} origin={} occurrences={}",
                signal_label(candidate.signal_kind),
                origin_label(candidate.origin),
                candidate.occurrence_count
            )),
            occurred_at: candidate.created_at,
        });
        if candidate.signal_kind == AdaptiveWikiSignalKind::OperatorCorrection
            && !correction_records
                .iter()
                .any(|correction| correction.candidate_id.as_deref() == Some(candidate.id.as_str()))
        {
            events.push(AdaptiveWikiLiveEpisodeEvent {
                id: live_event_id(
                    AdaptiveWikiLiveEpisodeEventKind::OperatorCorrectionObserved,
                    "candidate",
                    &candidate.id,
                ),
                kind: AdaptiveWikiLiveEpisodeEventKind::OperatorCorrectionObserved,
                task_id: None,
                request_id: None,
                project_key: None,
                artifact_kind: None,
                entry_ids: Vec::new(),
                candidate_id: Some(operator_safe_text(&candidate.id)),
                evidence_refs: evidence_refs.clone(),
                summary: "operator correction produced adaptive wiki evidence".to_string(),
                occurred_at: candidate.last_seen_at,
            });
        }
        if candidate.signal_kind == AdaptiveWikiSignalKind::Rollback {
            events.push(AdaptiveWikiLiveEpisodeEvent {
                id: live_event_id(
                    AdaptiveWikiLiveEpisodeEventKind::RollbackObserved,
                    "candidate",
                    &candidate.id,
                ),
                kind: AdaptiveWikiLiveEpisodeEventKind::RollbackObserved,
                task_id: None,
                request_id: None,
                project_key: None,
                artifact_kind: None,
                entry_ids: Vec::new(),
                candidate_id: Some(operator_safe_text(&candidate.id)),
                evidence_refs,
                summary: "rollback evidence produced adaptive wiki candidate".to_string(),
                occurred_at: candidate.last_seen_at,
            });
        }
    }

    for audit in audit_records
        .iter()
        .filter(|audit| live_filter_matches_audit(audit, entries, candidates, filter))
    {
        let Some(kind) = live_event_kind_for_audit(audit.action) else {
            continue;
        };
        events.push(AdaptiveWikiLiveEpisodeEvent {
            id: live_event_id(kind, "audit", &audit.id),
            kind,
            task_id: None,
            request_id: None,
            project_key: None,
            artifact_kind: None,
            entry_ids: audit
                .entry_id
                .as_ref()
                .into_iter()
                .chain(if audit.entry_id.is_none() {
                    Some(&audit.subject_id)
                } else {
                    None
                })
                .map(|value| operator_safe_text(value))
                .collect(),
            candidate_id: audit.candidate_id.as_deref().map(operator_safe_text),
            evidence_refs: audit
                .evidence_ref
                .as_ref()
                .map(|value| vec![operator_safe_text(value)])
                .unwrap_or_default(),
            summary: operator_safe_text(&format!(
                "audit action={} actor={}",
                audit_action_label(audit.action),
                audit.actor
            )),
            occurred_at: audit.created_at,
        });
    }

    for resume in resume_states
        .iter()
        .filter(|resume| live_filter_matches_resume(resume, filter))
    {
        if resume.status == ResumeStatus::ResumePending {
            events.push(AdaptiveWikiLiveEpisodeEvent {
                id: live_event_id(
                    AdaptiveWikiLiveEpisodeEventKind::ResumePending,
                    "resume",
                    &resume.resume_id(),
                ),
                kind: AdaptiveWikiLiveEpisodeEventKind::ResumePending,
                task_id: Some(operator_safe_text(&resume.task_id)),
                request_id: Some(operator_safe_text(&resume.request_id)),
                project_key: Some(operator_safe_text(&resume.project_key)),
                artifact_kind: None,
                entry_ids: Vec::new(),
                candidate_id: None,
                evidence_refs: resume
                    .last_evidence_artifacts
                    .iter()
                    .map(|value| operator_safe_text(value))
                    .collect(),
                summary: operator_safe_text(&format!(
                    "resume pending phase={} reason={}",
                    resume.phase,
                    resume.interruption_reason.clone().unwrap_or_default()
                )),
                occurred_at: resume.interrupted_at.unwrap_or(now),
            });
        }
    }

    events.sort_by_key(|event| {
        (
            event.occurred_at,
            live_event_kind_label(event.kind).to_string(),
            event.id.clone(),
        )
    });
    events.dedup_by(|left, right| left.id == right.id);
    events
}

fn live_episode_summary(
    events: &[AdaptiveWikiLiveEpisodeEvent],
    tasks: &[OffdeskTask],
    usage_records: &[AdaptiveWikiUsageRecord],
) -> AdaptiveWikiLiveEpisodeSummary {
    let task_ids: std::collections::HashSet<_> =
        tasks.iter().map(|task| task.task_id.as_str()).collect();
    let mut summary = AdaptiveWikiLiveEpisodeSummary {
        events: events.len(),
        usage_without_task: usage_records
            .iter()
            .filter(|usage| !task_ids.contains(usage.task_id.as_str()))
            .count(),
        ..AdaptiveWikiLiveEpisodeSummary::default()
    };
    for event in events {
        match event.kind {
            AdaptiveWikiLiveEpisodeEventKind::TaskEnqueued => summary.task_events += 1,
            AdaptiveWikiLiveEpisodeEventKind::ProjectionAttached => summary.projection_events += 1,
            AdaptiveWikiLiveEpisodeEventKind::RuntimeUsageRecorded => {
                summary.runtime_usage_events += 1
            }
            AdaptiveWikiLiveEpisodeEventKind::OperatorCorrectionObserved => {
                summary.correction_events += 1
            }
            AdaptiveWikiLiveEpisodeEventKind::CandidateRecorded => summary.candidate_events += 1,
            AdaptiveWikiLiveEpisodeEventKind::EntryPromoted => summary.promotion_events += 1,
            AdaptiveWikiLiveEpisodeEventKind::CounterexampleRecorded => {
                summary.counterexample_events += 1
            }
            AdaptiveWikiLiveEpisodeEventKind::EntryDeprecated => {}
            AdaptiveWikiLiveEpisodeEventKind::TaskCompleted => summary.completion_events += 1,
            AdaptiveWikiLiveEpisodeEventKind::TaskFailed => summary.failure_events += 1,
            AdaptiveWikiLiveEpisodeEventKind::ResumePending => summary.resume_pending_events += 1,
            AdaptiveWikiLiveEpisodeEventKind::RollbackObserved => summary.rollback_events += 1,
        }
    }
    summary
}

fn write_live_episode_trace_report(
    output_dir: &Path,
    report: &AdaptiveWikiLiveEpisodeTraceReport,
) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    fs::write(
        output_dir.join("report.json"),
        serde_json::to_string_pretty(report)?,
    )?;
    let mut trace = String::new();
    for event in &report.events {
        trace.push_str(&serde_json::to_string(event)?);
        trace.push('\n');
    }
    fs::write(output_dir.join("trace.jsonl"), trace)?;
    fs::write(
        output_dir.join("REPORT.md"),
        markdown_live_episode_report(report),
    )?;
    Ok(())
}

fn markdown_live_episode_report(report: &AdaptiveWikiLiveEpisodeTraceReport) -> String {
    let mut content = String::new();
    content.push_str("# Adaptive Wiki Live Episode Trace\n\n");
    content.push_str(&format!(
        "Generated: `{}`\n\nDry run: `{}`\n\nReport dir: `{}`\n\n",
        report.generated_at.to_rfc3339(),
        report.dry_run,
        table_text(&report.report_dir)
    ));
    content.push_str("## Filter\n\n");
    content.push_str(&format!(
        "- Request: `{}`\n- Task: `{}`\n- Project: `{}`\n- Artifact kind: `{}`\n- Entry: `{}`\n\n",
        optional_text(report.filter.request_id.as_deref()),
        optional_text(report.filter.task_id.as_deref()),
        optional_text(report.filter.project_key.as_deref()),
        optional_text(report.filter.artifact_kind.as_deref()),
        optional_text(report.filter.entry_id.as_deref())
    ));
    content.push_str("## Summary\n\n");
    content.push_str(&format!(
        "- Events: `{}`\n- Task events: `{}`\n- Runtime usage events: `{}`\n- Projection events: `{}`\n- Candidate events: `{}`\n- Correction events: `{}`\n- Promotion events: `{}`\n- Completion events: `{}`\n- Failure events: `{}`\n- Resume pending events: `{}`\n- Rollback events: `{}`\n- Usage without task: `{}`\n\n",
        report.summary.events,
        report.summary.task_events,
        report.summary.runtime_usage_events,
        report.summary.projection_events,
        report.summary.candidate_events,
        report.summary.correction_events,
        report.summary.promotion_events,
        report.summary.completion_events,
        report.summary.failure_events,
        report.summary.resume_pending_events,
        report.summary.rollback_events,
        report.summary.usage_without_task
    ));
    content.push_str("## Events\n\n");
    if report.events.is_empty() {
        content.push_str("_No episode events matched the filter._\n");
        return content;
    }
    for event in &report.events {
        content.push_str(&format!(
            "- `{}` `{}` task=`{}` request=`{}` entries=`{}` summary={}\n",
            event.occurred_at.to_rfc3339(),
            live_event_kind_label(event.kind),
            optional_text(event.task_id.as_deref()),
            optional_text(event.request_id.as_deref()),
            table_text(&event.entry_ids.join(",")),
            table_text(&event.summary)
        ));
    }
    content
}

fn write_promotion_evidence_chain_report(
    output_dir: &Path,
    report: &AdaptiveWikiPromotionEvidenceChainReport,
) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    fs::write(
        output_dir.join("report.json"),
        serde_json::to_string_pretty(report)?,
    )?;
    let mut chain = String::new();
    if let Some(audit) = &report.promotion_audit {
        push_chain_record(&mut chain, "promotion_audit", audit)?;
    }
    if let Some(candidate) = &report.candidate_snapshot {
        push_chain_record(&mut chain, "candidate_snapshot", candidate)?;
    }
    if let Some(entry) = &report.entry_snapshot {
        push_chain_record(&mut chain, "entry_snapshot", entry)?;
    }
    if let Some(entry) = &report.current_entry {
        push_chain_record(&mut chain, "current_entry", entry)?;
    }
    for usage in &report.usage_records {
        push_chain_record(&mut chain, "usage", usage)?;
    }
    for audit in &report.related_audit_records {
        push_chain_record(&mut chain, "related_audit", audit)?;
    }
    fs::write(output_dir.join("chain.jsonl"), chain)?;
    fs::write(
        output_dir.join("REPORT.md"),
        markdown_promotion_evidence_chain_report(report),
    )?;
    Ok(())
}

fn push_chain_record<T: Serialize>(chain: &mut String, kind: &str, record: &T) -> Result<()> {
    chain.push_str(&serde_json::to_string(&serde_json::json!({
        "kind": kind,
        "record": record
    }))?);
    chain.push('\n');
    Ok(())
}

fn markdown_promotion_evidence_chain_report(
    report: &AdaptiveWikiPromotionEvidenceChainReport,
) -> String {
    let mut content = String::new();
    content.push_str("# Adaptive Wiki Promotion Evidence Chain\n\n");
    content.push_str(&format!(
        "Generated: `{}`\n\nDry run: `{}`\n\nReport dir: `{}`\n\n",
        report.generated_at.to_rfc3339(),
        report.dry_run,
        table_text(&report.report_dir)
    ));
    content.push_str("## Summary\n\n");
    content.push_str(&format!(
        "- Entry: `{}`\n- Promotion audit found: `{}`\n- Candidate snapshot: `{}`\n- Entry snapshot: `{}`\n- Current entry: `{}`\n- Usage records: `{}`\n- Related audit records: `{}`\n- Failures: `{}`\n\n",
        table_text(&report.entry_id),
        report.summary.promotion_audit_found,
        report.summary.candidate_snapshot_present,
        report.summary.entry_snapshot_present,
        report.summary.current_entry_present,
        report.summary.usage_records,
        report.summary.related_audit_records,
        report.summary.failures
    ));
    content.push_str("## Failures\n\n");
    if report.failures.is_empty() {
        content.push_str("_No report failures._\n\n");
    } else {
        for failure in &report.failures {
            content.push_str(&format!("- {}\n", table_text(failure)));
        }
        content.push('\n');
    }
    content.push_str("## Promotion Audit\n\n");
    if let Some(audit) = &report.promotion_audit {
        content.push_str(&format!(
            "- `{}` action=`{}` candidate=`{}` entry=`{}` actor=`{}` reason={}\n\n",
            audit.created_at.to_rfc3339(),
            audit_action_label(audit.action),
            optional_text(audit.candidate_id.as_deref()),
            optional_text(audit.entry_id.as_deref()),
            table_text(&audit.actor),
            table_text(&audit.reason)
        ));
    } else {
        content.push_str("_No promotion audit found._\n\n");
    }
    content.push_str("## Snapshots\n\n");
    content.push_str(&format!(
        "- Candidate snapshot: `{}`\n- Entry snapshot: `{}`\n- Current entry: `{}`\n\n",
        report
            .candidate_snapshot
            .as_ref()
            .map(|candidate| table_text(&candidate.id))
            .unwrap_or_else(|| "-".to_string()),
        report
            .entry_snapshot
            .as_ref()
            .map(|entry| table_text(&entry.id))
            .unwrap_or_else(|| "-".to_string()),
        report
            .current_entry
            .as_ref()
            .map(|entry| format!(
                "{} {}:{} {:?}",
                table_text(&entry.id),
                scope_label(entry.scope),
                table_text(&entry.scope_ref),
                entry.status
            ))
            .unwrap_or_else(|| "-".to_string())
    ));
    content.push_str("## Usage Records\n\n");
    if report.usage_records.is_empty() {
        content.push_str("_No usage records for this entry._\n\n");
    } else {
        for usage in &report.usage_records {
            content.push_str(&format!(
                "- `{}` task=`{}` request=`{}` project=`{}` artifact=`{}` projection=`{}`\n",
                usage.created_at.to_rfc3339(),
                table_text(&usage.task_id),
                table_text(&usage.request_id),
                table_text(&usage.project_key),
                optional_text(usage.artifact_kind.as_deref()),
                table_text(&usage.projection_kind)
            ));
        }
        content.push('\n');
    }
    content.push_str("## Related Audit Records\n\n");
    if report.related_audit_records.is_empty() {
        content.push_str("_No related audit records._\n");
    } else {
        for audit in &report.related_audit_records {
            content.push_str(&format!(
                "- `{}` `{}` subject=`{}` candidate=`{}` entry=`{}` reason={}\n",
                audit.created_at.to_rfc3339(),
                audit_action_label(audit.action),
                table_text(&audit.subject_id),
                optional_text(audit.candidate_id.as_deref()),
                optional_text(audit.entry_id.as_deref()),
                table_text(&audit.reason)
            ));
        }
    }
    content
}

fn promotion_audit_for_entry_id<'a>(
    entry_id: &str,
    audit_records: &'a [AdaptiveWikiAuditRecord],
) -> Option<&'a AdaptiveWikiAuditRecord> {
    audit_records
        .iter()
        .filter(|audit| audit.action == AdaptiveWikiAuditAction::Promote)
        .filter(|audit| audit.entry_id.as_deref() == Some(entry_id) || audit.subject_id == entry_id)
        .min_by_key(|audit| audit.created_at)
}

fn promotion_time_for_entry(
    entry: &AdaptiveWikiEntry,
    audit_records: &[AdaptiveWikiAuditRecord],
) -> Option<DateTime<Utc>> {
    audit_records
        .iter()
        .filter(|audit| audit.action == AdaptiveWikiAuditAction::Promote)
        .filter(|audit| {
            audit.entry_id.as_deref() == Some(entry.id.as_str()) || audit.subject_id == entry.id
        })
        .map(|audit| audit.created_at)
        .min()
}

fn live_filter_for_entry_scope(entry: &AdaptiveWikiEntry) -> AdaptiveWikiLiveEpisodeFilter {
    match entry.scope {
        AdaptiveWikiScope::Session => AdaptiveWikiLiveEpisodeFilter {
            request_id: if entry.scope_ref == "*" {
                None
            } else {
                Some(entry.scope_ref.clone())
            },
            ..AdaptiveWikiLiveEpisodeFilter::default()
        },
        AdaptiveWikiScope::Project => AdaptiveWikiLiveEpisodeFilter {
            project_key: if entry.scope_ref == "*" {
                None
            } else {
                Some(entry.scope_ref.clone())
            },
            ..AdaptiveWikiLiveEpisodeFilter::default()
        },
        AdaptiveWikiScope::ArtifactKind => AdaptiveWikiLiveEpisodeFilter {
            artifact_kind: if entry.scope_ref == "*" {
                None
            } else {
                Some(entry.scope_ref.clone())
            },
            ..AdaptiveWikiLiveEpisodeFilter::default()
        },
        AdaptiveWikiScope::UserGlobal => AdaptiveWikiLiveEpisodeFilter::default(),
    }
}

fn is_correction_recurrence_event(event: &AdaptiveWikiLiveEpisodeEvent, entry_id: &str) -> bool {
    match event.kind {
        AdaptiveWikiLiveEpisodeEventKind::OperatorCorrectionObserved => true,
        AdaptiveWikiLiveEpisodeEventKind::CounterexampleRecorded => {
            event.entry_ids.iter().any(|id| id == entry_id)
        }
        AdaptiveWikiLiveEpisodeEventKind::TaskFailed
        | AdaptiveWikiLiveEpisodeEventKind::ResumePending => {
            event.entry_ids.iter().any(|id| id == entry_id)
        }
        _ => false,
    }
}

fn write_correction_recurrence_report(
    output_dir: &Path,
    report: &AdaptiveWikiCorrectionRecurrenceReport,
) -> Result<()> {
    fs::create_dir_all(output_dir)?;
    fs::write(
        output_dir.join("report.json"),
        serde_json::to_string_pretty(report)?,
    )?;
    let mut trace = String::new();
    for event in report
        .pre_promotion_events
        .iter()
        .chain(report.post_promotion_events.iter())
        .chain(report.usage_events.iter())
    {
        trace.push_str(&serde_json::to_string(event)?);
        trace.push('\n');
    }
    fs::write(output_dir.join("recurrence.jsonl"), trace)?;
    fs::write(
        output_dir.join("REPORT.md"),
        markdown_correction_recurrence_report(report),
    )?;
    Ok(())
}

fn markdown_correction_recurrence_report(
    report: &AdaptiveWikiCorrectionRecurrenceReport,
) -> String {
    let mut content = String::new();
    content.push_str("# Adaptive Wiki Correction Recurrence\n\n");
    content.push_str(&format!(
        "Generated: `{}`\n\nDry run: `{}`\n\nReport dir: `{}`\n\n",
        report.generated_at.to_rfc3339(),
        report.dry_run,
        table_text(&report.report_dir)
    ));
    content.push_str("## Summary\n\n");
    content.push_str(&format!(
        "- Entry: `{}`\n- Scope: `{}`\n- Promotion at: `{}`\n- Assessment: `{}`\n- Correction records checked: `{}`\n- Scoped events: `{}`\n- Pre-promotion corrections: `{}`\n- Post-promotion corrections: `{}`\n- Post-promotion counterexamples: `{}`\n- Pre-promotion failures: `{}`\n- Post-promotion failures: `{}`\n- Post-promotion usages: `{}`\n- Recurrence per 1000 usages: `{}`\n- Recurrence delta: `{}`\n\n",
        table_text(&report.entry_id),
        table_text(
            &report
                .scope
                .as_ref()
                .map(|scope| format!("{}:{}", scope_label(scope.scope), scope.scope_ref))
                .unwrap_or_else(|| "-".to_string())
        ),
        report
            .promotion_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "-".to_string()),
        correction_assessment_label(report.assessment),
        report.summary.correction_records_checked,
        report.summary.scoped_events,
        report.summary.pre_promotion_correction_events,
        report.summary.post_promotion_correction_events,
        report.summary.post_promotion_counterexample_events,
        report.summary.pre_promotion_failure_events,
        report.summary.post_promotion_failure_events,
        report.summary.post_promotion_usage_events,
        report.summary.post_promotion_recurrence_per_1000,
        report.summary.recurrence_delta
    ));
    content.push_str("## Failures\n\n");
    if report.failures.is_empty() {
        content.push_str("_No report failures._\n\n");
    } else {
        for failure in &report.failures {
            content.push_str(&format!("- {}\n", table_text(failure)));
        }
        content.push('\n');
    }
    content.push_str("## Pre-Promotion Events\n\n");
    markdown_live_event_list(&mut content, &report.pre_promotion_events);
    content.push_str("\n## Post-Promotion Events\n\n");
    markdown_live_event_list(&mut content, &report.post_promotion_events);
    content.push_str("\n## Usage Events\n\n");
    markdown_live_event_list(&mut content, &report.usage_events);
    content
}

fn markdown_live_event_list(content: &mut String, events: &[AdaptiveWikiLiveEpisodeEvent]) {
    if events.is_empty() {
        content.push_str("_No matching events._\n");
        return;
    }
    for event in events {
        content.push_str(&format!(
            "- `{}` `{}` task=`{}` request=`{}` entries=`{}` summary={}\n",
            event.occurred_at.to_rfc3339(),
            live_event_kind_label(event.kind),
            optional_text(event.task_id.as_deref()),
            optional_text(event.request_id.as_deref()),
            table_text(&event.entry_ids.join(",")),
            table_text(&event.summary)
        ));
    }
}

fn correction_assessment_label(
    assessment: AdaptiveWikiCorrectionRecurrenceAssessment,
) -> &'static str {
    match assessment {
        AdaptiveWikiCorrectionRecurrenceAssessment::InsufficientEvidence => "insufficient_evidence",
        AdaptiveWikiCorrectionRecurrenceAssessment::NoRecurrenceObserved => {
            "no_recurrence_observed"
        }
        AdaptiveWikiCorrectionRecurrenceAssessment::RecurrenceObserved => "recurrence_observed",
    }
}

fn operator_safe_live_episode_filter(
    filter: &AdaptiveWikiLiveEpisodeFilter,
) -> AdaptiveWikiLiveEpisodeFilter {
    AdaptiveWikiLiveEpisodeFilter {
        request_id: filter.request_id.as_deref().map(operator_safe_text),
        task_id: filter.task_id.as_deref().map(operator_safe_text),
        project_key: filter.project_key.as_deref().map(operator_safe_text),
        artifact_kind: filter.artifact_kind.as_deref().map(operator_safe_text),
        entry_id: filter.entry_id.as_deref().map(operator_safe_text),
    }
}

fn operator_safe_scope_suggestion(
    suggestion: &AdaptiveWikiScopeSuggestion,
) -> AdaptiveWikiScopeSuggestion {
    AdaptiveWikiScopeSuggestion {
        scope: suggestion.scope,
        scope_ref: operator_safe_text(&suggestion.scope_ref),
    }
}

fn human_entry_snapshot(entry: &AdaptiveWikiEntry) -> AdaptiveWikiHumanEntry {
    build_human_projection(
        std::slice::from_ref(entry),
        &[],
        &AdaptiveWikiQuery::default(),
    )
    .entries
    .into_iter()
    .next()
    .expect("one human entry projection")
}

fn operator_safe_human_entry(entry: &AdaptiveWikiHumanEntry) -> AdaptiveWikiHumanEntry {
    AdaptiveWikiHumanEntry {
        id: operator_safe_text(&entry.id),
        kind: entry.kind,
        scope: entry.scope,
        scope_ref: operator_safe_text(&entry.scope_ref),
        status: entry.status,
        activation_mode: entry.activation_mode,
        agent_modes: entry.agent_modes.clone(),
        claim: operator_safe_text(&entry.claim),
        human_summary: operator_safe_text(&entry.human_summary),
        evidence_refs: entry
            .evidence_refs
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
        counterexamples: entry
            .counterexamples
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
        contested: entry.contested,
        support_refs: entry
            .support_refs
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
        capability_ids: entry
            .capability_ids
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
        required_artifact_kinds: entry
            .required_artifact_kinds
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
        core_tags: clean_tags(entry.core_tags.clone()),
        proposed_tags: clean_tags(entry.proposed_tags.clone()),
        confidence: entry.confidence,
        updated_at: entry.updated_at,
        review_after: entry.review_after,
    }
}

fn operator_safe_human_candidate(
    candidate: &AdaptiveWikiHumanCandidate,
) -> AdaptiveWikiHumanCandidate {
    AdaptiveWikiHumanCandidate {
        id: operator_safe_text(&candidate.id),
        kind: candidate.kind,
        scope: candidate.scope,
        scope_ref: operator_safe_text(&candidate.scope_ref),
        agent_modes: candidate.agent_modes.clone(),
        claim: operator_safe_text(&candidate.claim),
        human_summary: operator_safe_text(&candidate.human_summary),
        evidence_refs: candidate
            .evidence_refs
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
        signal_kind: candidate.signal_kind,
        origin: candidate.origin,
        source_refs: candidate
            .source_refs
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
        source_hashes: candidate
            .source_hashes
            .iter()
            .map(|value| operator_safe_text(value))
            .collect(),
        core_tags: clean_tags(candidate.core_tags.clone()),
        proposed_tags: clean_tags(candidate.proposed_tags.clone()),
        suggested_scope: candidate
            .suggested_scope
            .as_ref()
            .map(operator_safe_scope_suggestion),
        review_reason: operator_safe_text(&candidate.review_reason),
        occurrence_count: candidate.occurrence_count,
        confidence: candidate.confidence,
        updated_at: candidate.updated_at,
        last_seen_at: candidate.last_seen_at,
    }
}

fn operator_safe_audit_record(audit: &AdaptiveWikiAuditRecord) -> AdaptiveWikiAuditRecord {
    AdaptiveWikiAuditRecord {
        id: operator_safe_text(&audit.id),
        action: audit.action,
        subject_id: operator_safe_text(&audit.subject_id),
        candidate_id: audit.candidate_id.as_deref().map(operator_safe_text),
        entry_id: audit.entry_id.as_deref().map(operator_safe_text),
        actor: operator_safe_text(&audit.actor),
        reason: operator_safe_text(&audit.reason),
        evidence_ref: audit.evidence_ref.as_deref().map(operator_safe_text),
        before_scope: audit
            .before_scope
            .as_ref()
            .map(operator_safe_scope_suggestion),
        after_scope: audit
            .after_scope
            .as_ref()
            .map(operator_safe_scope_suggestion),
        activation_mode: audit.activation_mode,
        candidate_snapshot: audit
            .candidate_snapshot
            .as_ref()
            .map(operator_safe_human_candidate),
        entry_snapshot: audit.entry_snapshot.as_ref().map(operator_safe_human_entry),
        created_at: audit.created_at,
    }
}

fn operator_safe_usage_record(usage: &AdaptiveWikiUsageRecord) -> AdaptiveWikiUsageRecord {
    AdaptiveWikiUsageRecord {
        id: operator_safe_text(&usage.id),
        entry_id: operator_safe_text(&usage.entry_id),
        task_id: operator_safe_text(&usage.task_id),
        request_id: operator_safe_text(&usage.request_id),
        project_key: operator_safe_text(&usage.project_key),
        artifact_kind: usage.artifact_kind.as_deref().map(operator_safe_text),
        agent_mode: usage.agent_mode,
        projection_kind: operator_safe_text(&usage.projection_kind),
        projection_policy: usage.projection_policy,
        activation_mode: usage.activation_mode,
        created_at: usage.created_at,
    }
}

fn live_filter_matches_task(task: &OffdeskTask, filter: &AdaptiveWikiLiveEpisodeFilter) -> bool {
    matches_optional_filter(filter.task_id.as_deref(), &task.task_id)
        && matches_optional_filter(filter.request_id.as_deref(), &task.request_id)
        && matches_optional_filter(filter.project_key.as_deref(), &task.project_key)
        && matches_optional_filter(
            filter.artifact_kind.as_deref(),
            task.artifact_kind.as_deref().unwrap_or(""),
        )
        && filter
            .entry_id
            .as_deref()
            .map(|entry_id| {
                task.last_adaptive_wiki_entry_ids
                    .iter()
                    .any(|id| id == entry_id)
            })
            .unwrap_or(true)
}

fn live_filter_matches_usage(
    usage: &AdaptiveWikiUsageRecord,
    filter: &AdaptiveWikiLiveEpisodeFilter,
) -> bool {
    matches_optional_filter(filter.task_id.as_deref(), &usage.task_id)
        && matches_optional_filter(filter.request_id.as_deref(), &usage.request_id)
        && matches_optional_filter(filter.project_key.as_deref(), &usage.project_key)
        && matches_optional_filter(
            filter.artifact_kind.as_deref(),
            usage.artifact_kind.as_deref().unwrap_or(""),
        )
        && matches_optional_filter(filter.entry_id.as_deref(), &usage.entry_id)
}

fn live_filter_matches_correction(
    correction: &AdaptiveWikiCorrectionRecord,
    filter: &AdaptiveWikiLiveEpisodeFilter,
) -> bool {
    let refs: Vec<_> = correction
        .evidence_refs
        .iter()
        .chain(correction.source_refs.iter())
        .collect();
    let task_matches = filter
        .task_id
        .as_deref()
        .map(|needle| {
            correction
                .task_id
                .as_deref()
                .map(|value| value.trim() == needle.trim())
                .unwrap_or_else(|| refs.iter().any(|value| value.contains(needle)))
        })
        .unwrap_or(true);
    let request_matches = filter
        .request_id
        .as_deref()
        .map(|needle| {
            correction
                .request_id
                .as_deref()
                .map(|value| value.trim() == needle.trim())
                .unwrap_or_else(|| refs.iter().any(|value| value.contains(needle)))
        })
        .unwrap_or(true);

    task_matches
        && request_matches
        && matches_optional_filter(
            filter.project_key.as_deref(),
            correction.project_key.as_deref().unwrap_or(""),
        )
        && matches_optional_filter(
            filter.artifact_kind.as_deref(),
            correction.artifact_kind.as_deref().unwrap_or(""),
        )
        && matches_optional_filter(
            filter.entry_id.as_deref(),
            correction.entry_id.as_deref().unwrap_or(""),
        )
}

fn live_filter_matches_probe(
    probe: &BackgroundProbe,
    filter: &AdaptiveWikiLiveEpisodeFilter,
) -> bool {
    matches_optional_filter(
        filter.task_id.as_deref(),
        probe.task_id.as_deref().unwrap_or(""),
    ) && matches_optional_filter(
        filter.request_id.as_deref(),
        probe.request_id.as_deref().unwrap_or(""),
    ) && matches_optional_filter(
        filter.project_key.as_deref(),
        probe.project_key.as_deref().unwrap_or(""),
    ) && filter.artifact_kind.is_none()
        && filter
            .entry_id
            .as_deref()
            .map(|entry_id| {
                probe
                    .adaptive_wiki_entry_ids
                    .iter()
                    .any(|id| id == entry_id)
            })
            .unwrap_or(true)
}

fn live_filter_matches_candidate(
    candidate: &AdaptiveWikiCandidate,
    filter: &AdaptiveWikiLiveEpisodeFilter,
) -> bool {
    if filter.entry_id.is_some() {
        return false;
    }
    let query = AdaptiveWikiQuery {
        session_id: None,
        project_key: filter.project_key.clone(),
        artifact_kind: filter.artifact_kind.clone(),
        agent_mode: None,
        agent_mode_filter: AdaptiveWikiAgentModeFilter::AllWhenUnspecified,
    };
    let scope_matches = is_unfiltered_query(&query) || candidate_matches_query(candidate, &query);
    scope_matches
        && refs_match_filter(
            candidate
                .evidence_refs
                .iter()
                .chain(candidate.source_refs.iter()),
            filter,
        )
}

fn live_filter_matches_audit(
    audit: &AdaptiveWikiAuditRecord,
    entries: &[AdaptiveWikiEntry],
    candidates: &[AdaptiveWikiCandidate],
    filter: &AdaptiveWikiLiveEpisodeFilter,
) -> bool {
    let entry_matches = audit.entry_id.as_ref().is_some_and(|entry_id| {
        filter
            .entry_id
            .as_deref()
            .map(|filter_entry_id| filter_entry_id == entry_id)
            .unwrap_or(true)
            && entries
                .iter()
                .find(|entry| entry.id == *entry_id)
                .map(|entry| live_filter_matches_scope(entry.scope, &entry.scope_ref, filter))
                .unwrap_or(true)
    });
    let candidate_matches = audit.candidate_id.as_ref().is_some_and(|candidate_id| {
        filter.entry_id.is_none()
            && candidates
                .iter()
                .find(|candidate| candidate.id == *candidate_id)
                .map(|candidate| {
                    live_filter_matches_scope(candidate.scope, &candidate.scope_ref, filter)
                })
                .unwrap_or(true)
    });
    let subject_matches = filter
        .entry_id
        .as_deref()
        .map(|entry_id| audit.subject_id == entry_id)
        .unwrap_or(false);
    (entry_matches || candidate_matches || subject_matches || filter_is_unscoped(filter))
        && refs_match_filter(audit.evidence_ref.iter(), filter)
}

fn live_filter_matches_resume(
    resume: &TaskResumeState,
    filter: &AdaptiveWikiLiveEpisodeFilter,
) -> bool {
    if filter.entry_id.is_some() || filter.artifact_kind.is_some() {
        return false;
    }
    matches_optional_filter(filter.task_id.as_deref(), &resume.task_id)
        && matches_optional_filter(filter.request_id.as_deref(), &resume.request_id)
        && matches_optional_filter(filter.project_key.as_deref(), &resume.project_key)
}

fn live_filter_matches_scope(
    scope: AdaptiveWikiScope,
    scope_ref: &str,
    filter: &AdaptiveWikiLiveEpisodeFilter,
) -> bool {
    matches_query(
        scope,
        scope_ref,
        &AdaptiveWikiQuery {
            session_id: None,
            project_key: filter.project_key.clone(),
            artifact_kind: filter.artifact_kind.clone(),
            agent_mode: None,
            agent_mode_filter: AdaptiveWikiAgentModeFilter::AllWhenUnspecified,
        },
    )
}

fn refs_match_filter<'a>(
    refs: impl Iterator<Item = &'a String>,
    filter: &AdaptiveWikiLiveEpisodeFilter,
) -> bool {
    let refs: Vec<_> = refs.collect();
    let request_matches = filter
        .request_id
        .as_deref()
        .map(|needle| refs.iter().any(|value| value.contains(needle)))
        .unwrap_or(true);
    let task_matches = filter
        .task_id
        .as_deref()
        .map(|needle| refs.iter().any(|value| value.contains(needle)))
        .unwrap_or(true);
    request_matches && task_matches
}

fn filter_is_unscoped(filter: &AdaptiveWikiLiveEpisodeFilter) -> bool {
    filter.request_id.is_none()
        && filter.task_id.is_none()
        && filter.project_key.is_none()
        && filter.artifact_kind.is_none()
        && filter.entry_id.is_none()
}

fn matches_optional_filter(filter: Option<&str>, value: &str) -> bool {
    filter
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|filter| value.trim() == filter)
        .unwrap_or(true)
}

fn live_event_kind_for_audit(
    action: AdaptiveWikiAuditAction,
) -> Option<AdaptiveWikiLiveEpisodeEventKind> {
    match action {
        AdaptiveWikiAuditAction::Promote => Some(AdaptiveWikiLiveEpisodeEventKind::EntryPromoted),
        AdaptiveWikiAuditAction::Deprecate => {
            Some(AdaptiveWikiLiveEpisodeEventKind::EntryDeprecated)
        }
        AdaptiveWikiAuditAction::AddCounterexample => {
            Some(AdaptiveWikiLiveEpisodeEventKind::CounterexampleRecorded)
        }
        AdaptiveWikiAuditAction::Reject
        | AdaptiveWikiAuditAction::Rescope
        | AdaptiveWikiAuditAction::UpdateRunbook
        | AdaptiveWikiAuditAction::RenewReviewAfter
        | AdaptiveWikiAuditAction::Edit
        | AdaptiveWikiAuditAction::Retag => None,
    }
}

fn live_event_kind_for_correction(
    kind: AdaptiveWikiCorrectionKind,
) -> AdaptiveWikiLiveEpisodeEventKind {
    match kind {
        AdaptiveWikiCorrectionKind::Counterexample => {
            AdaptiveWikiLiveEpisodeEventKind::CounterexampleRecorded
        }
        AdaptiveWikiCorrectionKind::OperatorCorrection
        | AdaptiveWikiCorrectionKind::FailureRecurrence
        | AdaptiveWikiCorrectionKind::Unknown => {
            AdaptiveWikiLiveEpisodeEventKind::OperatorCorrectionObserved
        }
    }
}

fn live_event_id(
    kind: AdaptiveWikiLiveEpisodeEventKind,
    source_kind: &str,
    source_id: &str,
) -> String {
    format!(
        "wiki_episode_{}_{}_{}",
        live_event_kind_label(kind),
        markdown_slug(source_kind, "source"),
        markdown_slug(source_id, "source")
    )
}

fn live_candidate_evidence_refs(candidate: &AdaptiveWikiCandidate) -> Vec<String> {
    let mut refs = safe_refs(candidate.evidence_refs.iter());
    for value in safe_refs(candidate.source_refs.iter()) {
        push_unique(&mut refs, Some(&value));
    }
    refs
}

fn safe_refs<'a>(values: impl Iterator<Item = &'a String>) -> Vec<String> {
    values.map(|value| operator_safe_text(value)).collect()
}

fn optional_text(value: Option<&str>) -> String {
    value
        .map(table_text)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "-".to_string())
}

struct AdaptiveWikiReviewProposalInput<'a> {
    action: AdaptiveWikiReviewProposalAction,
    subject_kind: &'a str,
    subject_id: &'a str,
    title: &'a str,
    rationale: &'a str,
    evidence_refs: Vec<String>,
    risk: AdaptiveWikiReviewRisk,
    suggested_command: Option<String>,
}

fn push_review_proposal(
    proposals: &mut Vec<AdaptiveWikiReviewProposal>,
    input: AdaptiveWikiReviewProposalInput<'_>,
) {
    let subject_id = fallback_subject_id(input.subject_id);
    let action = review_action_label(input.action);
    let id = format!(
        "wiki_review_{}_{}_{}",
        action,
        input.subject_kind,
        markdown_slug(&subject_id, "subject")
    );
    if proposals.iter().any(|proposal| proposal.id == id) {
        return;
    }
    proposals.push(AdaptiveWikiReviewProposal {
        id,
        action: input.action,
        subject_kind: operator_safe_text(input.subject_kind),
        subject_id: operator_safe_text(&subject_id),
        title: operator_safe_text(input.title),
        rationale: operator_safe_text(input.rationale),
        evidence_refs: ensure_review_evidence(&subject_id, input.evidence_refs),
        risk: input.risk,
        suggested_command: input
            .suggested_command
            .map(|command| operator_safe_text(&command)),
        lifecycle: None,
    });
}

#[derive(Debug, Clone, Copy, Default)]
struct AdaptiveWikiReviewProposalLifecycleSummary {
    proposals_with_events: usize,
    open_proposals: usize,
    accepted_proposals: usize,
    rejected_proposals: usize,
    superseded_proposals: usize,
    stale_decision_proposals: usize,
}

#[derive(Debug, Clone, Default)]
struct AdaptiveWikiPromotionReceiptLoadReport {
    receipts: Vec<AdaptiveWikiPromotionReceipt>,
    invalid_files: usize,
}

#[derive(Debug, Clone, Copy, Default)]
struct AdaptiveWikiPromotionReceiptCoverage {
    with_receipt: usize,
    missing_receipt: usize,
}

fn promotion_receipt_coverage(
    entries: &[AdaptiveWikiEntry],
    receipts: &[AdaptiveWikiPromotionReceipt],
) -> AdaptiveWikiPromotionReceiptCoverage {
    let receipted_entry_ids = receipts
        .iter()
        .filter(|receipt| receipt.status == "promoted")
        .map(|receipt| receipt.entry_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut coverage = AdaptiveWikiPromotionReceiptCoverage::default();
    for entry in entries
        .iter()
        .filter(|entry| entry.status == AdaptiveWikiStatus::Promoted)
    {
        if receipted_entry_ids.contains(entry.id.as_str()) {
            coverage.with_receipt += 1;
        } else {
            coverage.missing_receipt += 1;
        }
    }
    coverage
}

fn promotion_receipt_filename(receipt: &AdaptiveWikiPromotionReceipt) -> String {
    let timestamp = receipt.generated_at.format("%Y%m%dT%H%M%SZ");
    format!(
        "adaptive_wiki_promotion_receipt_{}_{}_{}.json",
        timestamp,
        markdown_slug(&receipt.entry_id, "entry"),
        markdown_slug(&receipt.receipt_id, "receipt")
    )
}

struct AdaptiveWikiReviewProposalLifecycleContext<'a> {
    entries: &'a [AdaptiveWikiEntry],
    candidates: &'a [AdaptiveWikiCandidate],
    usage_records: &'a [AdaptiveWikiUsageRecord],
    audit_records: &'a [AdaptiveWikiAuditRecord],
    correction_records: &'a [AdaptiveWikiCorrectionRecord],
}

fn attach_review_proposal_lifecycle(
    proposals: &mut [AdaptiveWikiReviewProposal],
    events: &[AdaptiveWikiReviewProposalEventRecord],
    context: &AdaptiveWikiReviewProposalLifecycleContext<'_>,
) {
    let mut latest_by_proposal: BTreeMap<&str, &AdaptiveWikiReviewProposalEventRecord> =
        BTreeMap::new();
    for event in events {
        latest_by_proposal
            .entry(event.proposal_id.as_str())
            .and_modify(|existing| {
                if review_event_is_newer(event, existing) {
                    *existing = event;
                }
            })
            .or_insert(event);
    }

    for proposal in proposals {
        if let Some(event) = latest_by_proposal.get(proposal.id.as_str()) {
            let stale_evidence_refs = stale_review_evidence_refs(proposal, event, context);
            proposal.lifecycle = Some(AdaptiveWikiReviewProposalLifecycle {
                latest_event_id: operator_safe_text(&event.id),
                decision: event.decision,
                stale: !stale_evidence_refs.is_empty(),
                actor: operator_safe_text(&event.actor),
                reason: operator_safe_text(&event.reason),
                evidence_refs: event
                    .evidence_refs
                    .iter()
                    .map(|value| operator_safe_text(value))
                    .filter(|value| !value.is_empty())
                    .collect(),
                stale_evidence_refs,
                supersedes: event.supersedes.as_deref().map(operator_safe_text),
                decided_at: event.created_at,
            });
        }
    }
}

fn stale_review_evidence_refs(
    proposal: &AdaptiveWikiReviewProposal,
    event: &AdaptiveWikiReviewProposalEventRecord,
    context: &AdaptiveWikiReviewProposalLifecycleContext<'_>,
) -> Vec<String> {
    if event.decision == AdaptiveWikiReviewProposalDecision::Unknown {
        return Vec::new();
    }

    let mut refs = Vec::new();
    if subject_updated_after_decision(proposal, event.created_at, context) {
        refs.push(format!("{}:{}", proposal.subject_kind, proposal.subject_id));
    }

    for evidence_ref in &proposal.evidence_refs {
        if evidence_ref_timestamp(evidence_ref, context)
            .is_some_and(|timestamp| timestamp > event.created_at)
        {
            refs.push(evidence_ref.clone());
        }
    }

    clean_refs(refs)
        .into_iter()
        .map(|value| operator_safe_text(&value))
        .collect()
}

fn subject_updated_after_decision(
    proposal: &AdaptiveWikiReviewProposal,
    decided_at: DateTime<Utc>,
    context: &AdaptiveWikiReviewProposalLifecycleContext<'_>,
) -> bool {
    match proposal.subject_kind.as_str() {
        "candidate" => context
            .candidates
            .iter()
            .find(|candidate| candidate.id == proposal.subject_id)
            .map(candidate_review_timestamp)
            .is_some_and(|timestamp| timestamp > decided_at),
        "entry" => context
            .entries
            .iter()
            .find(|entry| entry.id == proposal.subject_id)
            .is_some_and(|entry| entry.updated_at > decided_at),
        _ => false,
    }
}

fn evidence_ref_timestamp(
    evidence_ref: &str,
    context: &AdaptiveWikiReviewProposalLifecycleContext<'_>,
) -> Option<DateTime<Utc>> {
    let (kind, id) = evidence_ref.split_once(':')?;
    match kind {
        "audit" => context
            .audit_records
            .iter()
            .find(|record| record.id == id)
            .map(|record| record.created_at),
        "candidate" => context
            .candidates
            .iter()
            .find(|candidate| candidate.id == id)
            .map(candidate_review_timestamp),
        "correction" => context
            .correction_records
            .iter()
            .find(|record| record.id == id)
            .map(|record| record.created_at),
        "entry" => context
            .entries
            .iter()
            .find(|entry| entry.id == id)
            .map(|entry| entry.updated_at),
        "usage" => context
            .usage_records
            .iter()
            .find(|record| record.id == id)
            .map(|record| record.created_at),
        _ => None,
    }
}

fn candidate_review_timestamp(candidate: &AdaptiveWikiCandidate) -> DateTime<Utc> {
    candidate.updated_at.max(candidate.last_seen_at)
}

fn review_event_is_newer(
    candidate: &AdaptiveWikiReviewProposalEventRecord,
    existing: &AdaptiveWikiReviewProposalEventRecord,
) -> bool {
    candidate.created_at > existing.created_at
        || (candidate.created_at == existing.created_at
            && candidate.id.as_str() > existing.id.as_str())
}

fn review_proposal_lifecycle_summary(
    proposals: &[AdaptiveWikiReviewProposal],
) -> AdaptiveWikiReviewProposalLifecycleSummary {
    let mut summary = AdaptiveWikiReviewProposalLifecycleSummary::default();
    for proposal in proposals {
        if proposal
            .lifecycle
            .as_ref()
            .is_some_and(|lifecycle| lifecycle.stale)
        {
            summary.stale_decision_proposals += 1;
            summary.open_proposals += 1;
        }
        match proposal
            .lifecycle
            .as_ref()
            .map(|lifecycle| lifecycle.decision)
        {
            Some(AdaptiveWikiReviewProposalDecision::Accepted) => {
                summary.proposals_with_events += 1;
                summary.accepted_proposals += 1;
            }
            Some(AdaptiveWikiReviewProposalDecision::Rejected) => {
                summary.proposals_with_events += 1;
                summary.rejected_proposals += 1;
            }
            Some(AdaptiveWikiReviewProposalDecision::Superseded) => {
                summary.proposals_with_events += 1;
                summary.superseded_proposals += 1;
            }
            Some(AdaptiveWikiReviewProposalDecision::Unknown) => {
                summary.proposals_with_events += 1;
                summary.open_proposals += 1;
            }
            None => {
                summary.open_proposals += 1;
            }
        }
    }
    summary
}

fn review_proposal_matches_queue_filter(
    proposal: &AdaptiveWikiReviewProposal,
    filter: AdaptiveWikiReviewQueueFilter,
) -> bool {
    match filter {
        AdaptiveWikiReviewQueueFilter::All => true,
        AdaptiveWikiReviewQueueFilter::Active => review_proposal_is_active(proposal),
        AdaptiveWikiReviewQueueFilter::Decided => review_proposal_is_decided(proposal),
        AdaptiveWikiReviewQueueFilter::Stale => proposal
            .lifecycle
            .as_ref()
            .is_some_and(|lifecycle| lifecycle.stale),
    }
}

fn review_proposal_is_active(proposal: &AdaptiveWikiReviewProposal) -> bool {
    match proposal.lifecycle.as_ref() {
        Some(lifecycle) => {
            lifecycle.stale || lifecycle.decision == AdaptiveWikiReviewProposalDecision::Unknown
        }
        None => true,
    }
}

fn review_proposal_is_decided(proposal: &AdaptiveWikiReviewProposal) -> bool {
    proposal.lifecycle.as_ref().is_some_and(|lifecycle| {
        !lifecycle.stale && lifecycle.decision != AdaptiveWikiReviewProposalDecision::Unknown
    })
}

fn candidate_evidence_refs(candidate: &AdaptiveWikiCandidate, fallback: &[&str]) -> Vec<String> {
    let mut refs = Vec::new();
    push_unique_many(&mut refs, candidate.evidence_refs.iter());
    push_unique_many(&mut refs, candidate.source_refs.iter());
    for value in fallback {
        push_unique(&mut refs, Some(value));
    }
    refs
}

fn entry_evidence_refs(entry: &AdaptiveWikiEntry, fallback: &[&str]) -> Vec<String> {
    let mut refs = Vec::new();
    push_unique_many(&mut refs, entry.evidence_refs.iter());
    push_unique_many(&mut refs, entry.counterexamples.iter());
    for value in fallback {
        push_unique(&mut refs, Some(value));
    }
    refs
}

fn ensure_review_evidence(subject_id: &str, refs: Vec<String>) -> Vec<String> {
    let mut cleaned = clean_refs(refs);
    if cleaned.is_empty() {
        cleaned.push(format!("wiki:{}", subject_id));
    }
    cleaned
        .into_iter()
        .map(|value| operator_safe_text(&value))
        .collect()
}

fn add_merge_proposals(
    proposals: &mut Vec<AdaptiveWikiReviewProposal>,
    entries: &[AdaptiveWikiEntry],
) {
    for (index, entry) in entries.iter().enumerate() {
        if entry.status != AdaptiveWikiStatus::Promoted {
            continue;
        }
        let duplicates: Vec<_> = entries
            .iter()
            .skip(index + 1)
            .filter(|candidate| {
                candidate.status == AdaptiveWikiStatus::Promoted
                    && candidate.kind == entry.kind
                    && candidate.scope == entry.scope
                    && candidate.scope_ref == entry.scope_ref
                    && normalize_key(&candidate.claim) == normalize_key(&entry.claim)
            })
            .collect();
        if duplicates.is_empty() {
            continue;
        }
        let mut evidence = entry_evidence_refs(entry, &["lint:duplicate_entry"]);
        for duplicate in duplicates {
            push_unique(&mut evidence, Some(&format!("entry:{}", duplicate.id)));
        }
        push_review_proposal(
            proposals,
            AdaptiveWikiReviewProposalInput {
                action: AdaptiveWikiReviewProposalAction::Merge,
                subject_kind: "entry",
                subject_id: &entry.id,
                title: "Merge duplicate promoted entries",
                rationale: "Multiple promoted entries share the same kind, scope, and claim.",
                evidence_refs: evidence,
                risk: AdaptiveWikiReviewRisk::Medium,
                suggested_command: None,
            },
        );
    }
}

fn add_projection_conflict_proposals(
    proposals: &mut Vec<AdaptiveWikiReviewProposal>,
    entries: &[AdaptiveWikiEntry],
) {
    let conflict_candidates = projection_conflict_candidates(entries, None);
    let conflicts = detect_projection_conflicts(&conflict_candidates);
    for conflict in conflicts {
        let Some(entry) = entries.iter().find(|entry| entry.id == conflict.entry_id) else {
            continue;
        };
        let mut evidence = entry_evidence_refs(entry, &["projection:conflict"]);
        push_unique(
            &mut evidence,
            Some(&format!("entry:{}", conflict.conflicting_entry_id)),
        );
        push_unique(
            &mut evidence,
            Some(&format!("projection:{}", conflict.signature)),
        );
        push_review_proposal(
            proposals,
            AdaptiveWikiReviewProposalInput {
                action: AdaptiveWikiReviewProposalAction::Split,
                subject_kind: "entry",
                subject_id: &conflict.entry_id,
                title: "Resolve conflicting promoted entries",
                rationale: "Two promoted entries share kind and scope but project opposite instruction polarity, so an operator should rescope, split, deprecate, or add counterexample evidence before relying on them together.",
                evidence_refs: evidence,
                risk: AdaptiveWikiReviewRisk::High,
                suggested_command: None,
            },
        );
    }
}

fn add_usage_proposals(
    proposals: &mut Vec<AdaptiveWikiReviewProposal>,
    entries: &[AdaptiveWikiEntry],
    usage_records: &[AdaptiveWikiUsageRecord],
) {
    for entry in entries
        .iter()
        .filter(|entry| entry.status == AdaptiveWikiStatus::Deprecated)
    {
        if !usage_records
            .iter()
            .any(|record| record.entry_id == entry.id && record.created_at >= entry.updated_at)
        {
            continue;
        }
        push_review_proposal(
            proposals,
            AdaptiveWikiReviewProposalInput {
                action: AdaptiveWikiReviewProposalAction::Deprecate,
                subject_kind: "entry",
                subject_id: &entry.id,
                title: "Investigate deprecated entry usage",
                rationale: "A deprecated entry appears in usage records after deprecation.",
                evidence_refs: entry_evidence_refs(entry, &["usage:deprecated_entry"]),
                risk: AdaptiveWikiReviewRisk::High,
                suggested_command: None,
            },
        );
    }
}

fn add_audit_evidence_refresh(
    proposals: &mut Vec<AdaptiveWikiReviewProposal>,
    candidates: &[AdaptiveWikiCandidate],
    audit_records: &[AdaptiveWikiAuditRecord],
) {
    for candidate in candidates.iter().filter(|candidate| {
        candidate.signal_kind == AdaptiveWikiSignalKind::ApprovalDenial
            && candidate.evidence_refs.is_empty()
            && !audit_records.is_empty()
    }) {
        push_review_proposal(
            proposals,
            AdaptiveWikiReviewProposalInput {
                action: AdaptiveWikiReviewProposalAction::AddCounterexample,
                subject_kind: "candidate",
                subject_id: &candidate.id,
                title: "Attach missing audit evidence",
                rationale: "The candidate came from approval denial but has no evidence refs.",
                evidence_refs: candidate_evidence_refs(candidate, &["audit:available"]),
                risk: AdaptiveWikiReviewRisk::Low,
                suggested_command: None,
            },
        );
    }
}

fn add_correction_recurrence_proposals(
    proposals: &mut Vec<AdaptiveWikiReviewProposal>,
    entries: &[AdaptiveWikiEntry],
    audit_records: &[AdaptiveWikiAuditRecord],
    correction_records: &[AdaptiveWikiCorrectionRecord],
) {
    for entry in entries
        .iter()
        .filter(|entry| entry.status == AdaptiveWikiStatus::Promoted)
    {
        let promotion_at =
            promotion_time_for_entry(entry, audit_records).unwrap_or(entry.created_at);
        let recurring_corrections: Vec<_> = correction_records
            .iter()
            .filter(|correction| correction.created_at >= promotion_at)
            .filter(|correction| correction_record_targets_entry(correction, entry))
            .collect();
        if recurring_corrections.is_empty() {
            continue;
        }

        let mut evidence = entry_evidence_refs(entry, &["recurrence:post_promotion_correction"]);
        for correction in recurring_corrections {
            push_unique(
                &mut evidence,
                Some(&format!("correction:{}", correction.id)),
            );
            push_unique_many(&mut evidence, correction.evidence_refs.iter());
            push_unique_many(&mut evidence, correction.source_refs.iter());
        }
        push_review_proposal(
            proposals,
            AdaptiveWikiReviewProposalInput {
                action: AdaptiveWikiReviewProposalAction::Rescope,
                subject_kind: "entry",
                subject_id: &entry.id,
                title: "Rescope entry with recurring corrections",
                rationale: "Correction evidence still appears after this entry was promoted, so the entry may need a narrower scope, clearer instruction, or counterexample.",
                evidence_refs: evidence,
                risk: AdaptiveWikiReviewRisk::High,
                suggested_command: None,
            },
        );
    }
}

fn correction_record_targets_entry(
    correction: &AdaptiveWikiCorrectionRecord,
    entry: &AdaptiveWikiEntry,
) -> bool {
    correction.entry_id.as_deref() == Some(entry.id.as_str())
}

fn add_promotion_chain_proposals(
    proposals: &mut Vec<AdaptiveWikiReviewProposal>,
    entries: &[AdaptiveWikiEntry],
    usage_records: &[AdaptiveWikiUsageRecord],
    audit_records: &[AdaptiveWikiAuditRecord],
) {
    for entry in entries
        .iter()
        .filter(|entry| entry.status == AdaptiveWikiStatus::Promoted)
    {
        let promotion_audit = promotion_audit_for_entry_id(&entry.id, audit_records);
        let missing_promotion_audit = promotion_audit.is_none();
        let missing_candidate_snapshot =
            promotion_audit.is_some_and(|audit| audit.candidate_snapshot.is_none());
        let missing_entry_snapshot =
            promotion_audit.is_some_and(|audit| audit.entry_snapshot.is_none());
        if !(missing_promotion_audit || missing_candidate_snapshot || missing_entry_snapshot) {
            continue;
        }

        let used_after_promotion = usage_records
            .iter()
            .any(|usage| usage.entry_id == entry.id && usage.created_at >= entry.created_at);
        let mut evidence = entry_evidence_refs(entry, &["promotion_chain:incomplete"]);
        if missing_promotion_audit {
            push_unique(&mut evidence, Some("audit:missing_promotion_audit"));
        }
        if missing_candidate_snapshot {
            push_unique(&mut evidence, Some("audit:missing_candidate_snapshot"));
        }
        if missing_entry_snapshot {
            push_unique(&mut evidence, Some("audit:missing_entry_snapshot"));
        }
        for usage in usage_records
            .iter()
            .filter(|usage| usage.entry_id == entry.id)
        {
            push_unique(&mut evidence, Some(&format!("usage:{}", usage.id)));
        }
        push_review_proposal(
            proposals,
            AdaptiveWikiReviewProposalInput {
                action: AdaptiveWikiReviewProposalAction::RenewReview,
                subject_kind: "entry",
                subject_id: &entry.id,
                title: "Review incomplete promotion evidence chain",
                rationale: "Promotion evidence is incomplete, so future audits cannot fully replay what was approved.",
                evidence_refs: evidence,
                risk: if used_after_promotion {
                    AdaptiveWikiReviewRisk::High
                } else {
                    AdaptiveWikiReviewRisk::Medium
                },
                suggested_command: None,
            },
        );
    }
}

fn action_order(action: AdaptiveWikiReviewProposalAction) -> u8 {
    match action {
        AdaptiveWikiReviewProposalAction::Promote => 0,
        AdaptiveWikiReviewProposalAction::RenewReview => 1,
        AdaptiveWikiReviewProposalAction::Rescope => 2,
        AdaptiveWikiReviewProposalAction::Split => 3,
        AdaptiveWikiReviewProposalAction::Merge => 4,
        AdaptiveWikiReviewProposalAction::AddCounterexample => 5,
        AdaptiveWikiReviewProposalAction::Deprecate => 6,
        AdaptiveWikiReviewProposalAction::Reject => 7,
    }
}

fn review_action_label(action: AdaptiveWikiReviewProposalAction) -> &'static str {
    match action {
        AdaptiveWikiReviewProposalAction::Promote => "promote",
        AdaptiveWikiReviewProposalAction::Reject => "reject",
        AdaptiveWikiReviewProposalAction::Rescope => "rescope",
        AdaptiveWikiReviewProposalAction::Deprecate => "deprecate",
        AdaptiveWikiReviewProposalAction::AddCounterexample => "add_counterexample",
        AdaptiveWikiReviewProposalAction::RenewReview => "renew_review",
        AdaptiveWikiReviewProposalAction::Split => "split",
        AdaptiveWikiReviewProposalAction::Merge => "merge",
    }
}

fn review_risk_label(risk: AdaptiveWikiReviewRisk) -> &'static str {
    match risk {
        AdaptiveWikiReviewRisk::Low => "low",
        AdaptiveWikiReviewRisk::Medium => "medium",
        AdaptiveWikiReviewRisk::High => "high",
    }
}

fn review_decision_label(decision: AdaptiveWikiReviewProposalDecision) -> &'static str {
    match decision {
        AdaptiveWikiReviewProposalDecision::Accepted => "accepted",
        AdaptiveWikiReviewProposalDecision::Rejected => "rejected",
        AdaptiveWikiReviewProposalDecision::Superseded => "superseded",
        AdaptiveWikiReviewProposalDecision::Unknown => "unknown",
    }
}

fn markdown_schema() -> String {
    [
        "# Adaptive Wiki Schema",
        "",
        "Canonical state lives in `adaptive_wiki_entries.json` and `adaptive_wiki_candidates.json`.",
        "This markdown vault is a one-way human projection. Edit through `forager offdesk wiki` commands.",
        "",
        "## Kinds",
        "",
        "- `preference`: durable operator preference.",
        "- `procedure`: reusable runbook or workflow rule.",
        "- `failure_pattern`: repeated failure or correction pattern.",
        "- `policy_rule`: safety or approval behavior rule.",
        "- `fact`: durable factual context.",
        "",
        "## Status",
        "",
        "- `promoted`: eligible for scoped AI projection.",
        "- `deprecated`: retained for audit but excluded from AI projection.",
        "- `candidate`: review item that does not affect runtime behavior.",
        "",
        "## Scope",
        "",
        "- `session`: applies only to one request/session id.",
        "- `project`: applies to a project key.",
        "- `artifact_kind`: applies to an artifact class.",
        "- `user_global`: applies across projects.",
        "",
        "## Agent Modes",
        "",
        "- `planning`: scoped plans, evidence gates, risks, and review handoffs.",
        "- `development`: implementation, debugging, tests, and code review preparation.",
        "- `analysis`: log, metric, experiment, and system-state interpretation.",
        "- `writing`: research synthesis, prose drafting, editing, and report work.",
        "- `critique`: skeptical review, validation, risk finding, and quality checks.",
        "- `review`: read-only checkpoint review before execution, approval, or handoff.",
        "- `maintenance`: read-only wiki, task, repo, model, and machine health inspection.",
        "- Legacy `code_development` loads as `development`; legacy `research_writing` loads as `writing`.",
        "- Missing `agent_modes` means the entry is shared across all modes.",
        "",
        "## Invariants",
        "",
        "- Candidates never change runtime behavior by themselves.",
        "- Runtime projection must not change command, workdir, provider, model, launch spec, or approval decisions.",
        "- Human pages are sanitized and should not contain secrets.",
        "- Procedure support refs are human/export material and are not part of compact AI projection.",
        "",
    ]
    .join("\n")
}

fn markdown_index(
    entries: &[AdaptiveWikiEntry],
    candidates: &[AdaptiveWikiCandidate],
    now: DateTime<Utc>,
) -> String {
    let mut content = String::new();
    content.push_str("# Adaptive Wiki Index\n\n");
    content.push_str(&format!("Generated: `{}`\n\n", now.to_rfc3339()));
    content.push_str("## Entries\n\n");
    content.push_str("| ID | Status | Kind | Scope | Activation | Agent Modes | Confidence | Contested | Claim |\n");
    content.push_str("| --- | --- | --- | --- | --- | --- | --- | --- | --- |\n");
    let mut entries = entries.to_vec();
    entries.sort_by_key(|entry| {
        (
            kind_label(entry.kind).to_string(),
            status_order(entry.status),
            entry.id.clone(),
        )
    });
    let entries_empty = entries.is_empty();
    for entry in &entries {
        let entry_path = format!(
            "entries/{}/{}.md",
            kind_dir(entry.kind),
            markdown_slug(&entry.id, "entry")
        );
        content.push_str(&format!(
            "| [{}]({}) | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | {} |\n",
            table_text(&entry.id),
            entry_path,
            status_label(entry.status),
            kind_label(entry.kind),
            table_text(&format!("{}:{}", scope_label(entry.scope), entry.scope_ref)),
            activation_label(entry.activation_mode),
            table_text(&agent_modes_label(&entry.agent_modes)),
            confidence_label(entry.confidence),
            !entry.counterexamples.is_empty(),
            table_text(&fallback_text(&entry.human_summary, &entry.claim))
        ));
    }
    if entries_empty {
        content.push_str("| _none_ | | | | | | | | |\n");
    }

    content.push_str("\n## Candidates\n\n");
    content.push_str("| ID | Kind | Scope | Signal | Hits | Confidence | Claim |\n");
    content.push_str("| --- | --- | --- | --- | --- | --- | --- |\n");
    let mut candidates = candidates.to_vec();
    candidates.sort_by_key(|candidate| {
        (
            kind_label(candidate.kind).to_string(),
            std::cmp::Reverse(candidate.occurrence_count),
            candidate.id.clone(),
        )
    });
    let candidates_empty = candidates.is_empty();
    for candidate in &candidates {
        let candidate_path = format!(
            "candidates/{}.md",
            markdown_slug(&candidate.id, "candidate")
        );
        content.push_str(&format!(
            "| [{}]({}) | `{}` | `{}` | `{}` | `{}` | `{}` | {} |\n",
            table_text(&candidate.id),
            candidate_path,
            kind_label(candidate.kind),
            table_text(&format!(
                "{}:{}",
                scope_label(candidate.scope),
                candidate.scope_ref
            )),
            signal_label(candidate.signal_kind),
            candidate.occurrence_count,
            confidence_label(candidate.confidence),
            table_text(&fallback_text(&candidate.human_summary, &candidate.claim))
        ));
    }
    if candidates_empty {
        content.push_str("| _none_ | | | | | | |\n");
    }
    content
}

fn markdown_export_log(
    entries: &[AdaptiveWikiEntry],
    candidates: &[AdaptiveWikiCandidate],
    now: DateTime<Utc>,
) -> String {
    format!(
        "# Adaptive Wiki Export Log\n\n- Generated at: `{}`\n- Entries exported: `{}`\n- Candidates exported: `{}`\n- Source of truth: canonical JSON store\n- Mutation policy: use `forager offdesk wiki` review commands\n",
        now.to_rfc3339(),
        entries.len(),
        candidates.len()
    )
}

fn markdown_entry_page(entry: &AdaptiveWikiEntry) -> String {
    let mut content = String::new();
    content.push_str("---\n");
    push_frontmatter_scalar(&mut content, "id", &entry.id);
    push_frontmatter_scalar(&mut content, "kind", kind_label(entry.kind));
    push_frontmatter_scalar(&mut content, "status", status_label(entry.status));
    push_frontmatter_scalar(&mut content, "scope", scope_label(entry.scope));
    push_frontmatter_scalar(&mut content, "scope_ref", &entry.scope_ref);
    push_frontmatter_scalar(
        &mut content,
        "activation_mode",
        activation_label(entry.activation_mode),
    );
    push_frontmatter_list(
        &mut content,
        "agent_modes",
        &agent_mode_values(&entry.agent_modes),
    );
    push_frontmatter_scalar(
        &mut content,
        "confidence",
        confidence_label(entry.confidence),
    );
    push_frontmatter_scalar(&mut content, "updated_at", &entry.updated_at.to_rfc3339());
    if let Some(review_after) = entry.review_after.as_ref() {
        push_frontmatter_scalar(&mut content, "review_after", &review_after.to_rfc3339());
    }
    push_frontmatter_bool(&mut content, "contested", !entry.counterexamples.is_empty());
    push_frontmatter_list(&mut content, "evidence_refs", &entry.evidence_refs);
    push_frontmatter_list(&mut content, "counterexamples", &entry.counterexamples);
    push_frontmatter_list(&mut content, "support_refs", &entry.support_refs);
    push_frontmatter_list(&mut content, "capability_ids", &entry.capability_ids);
    push_frontmatter_list(
        &mut content,
        "required_artifact_kinds",
        &entry.required_artifact_kinds,
    );
    push_frontmatter_list(
        &mut content,
        "core_tags",
        &clean_tags(entry.core_tags.clone()),
    );
    push_frontmatter_list(
        &mut content,
        "proposed_tags",
        &clean_tags(entry.proposed_tags.clone()),
    );
    content.push_str("---\n\n");

    content.push_str(&format!(
        "# {}\n\n",
        markdown_heading(&entry.claim, &entry.id)
    ));
    content.push_str("## Summary\n\n");
    content.push_str(&paragraph_or_placeholder(&entry.human_summary));
    content.push_str("\n\n## AI Projection Note\n\n");
    content.push_str(&paragraph_or_placeholder(&fallback_text(
        &entry.ai_instruction,
        &entry.claim,
    )));
    content.push_str("\n\n## Evidence\n\n");
    push_markdown_list(
        &mut content,
        &entry.evidence_refs,
        "_No evidence refs recorded._",
    );
    content.push_str("\n## Counterexamples\n\n");
    push_markdown_list(
        &mut content,
        &entry.counterexamples,
        "_No counterexamples recorded._",
    );
    if entry.kind == AdaptiveWikiKind::Procedure
        || !entry.support_refs.is_empty()
        || !entry.capability_ids.is_empty()
        || !entry.required_artifact_kinds.is_empty()
    {
        content.push_str("\n## Runbook Support\n\n");
        content.push_str("### Support Refs\n\n");
        push_markdown_list(
            &mut content,
            &entry.support_refs,
            "_No support refs recorded._",
        );
        content.push_str("### Capability IDs\n\n");
        push_markdown_list(
            &mut content,
            &entry.capability_ids,
            "_No capability ids recorded._",
        );
        content.push_str("### Required Artifact Kinds\n\n");
        push_markdown_list(
            &mut content,
            &entry.required_artifact_kinds,
            "_No required artifact kinds recorded._",
        );
    }
    content.push_str("\n## Governance\n\n");
    content.push_str(&format!(
        "- Status: `{}`\n- Scope: `{}`\n- Activation mode: `{}`\n- Agent modes: `{}`\n- Core tags: `{}`\n- Proposed tags: `{}`\n- Confidence: `{}`\n- Contested: `{}`\n",
        status_label(entry.status),
        table_text(&format!("{}:{}", scope_label(entry.scope), entry.scope_ref)),
        activation_label(entry.activation_mode),
        table_text(&agent_modes_label(&entry.agent_modes)),
        table_text(&tag_list_label(&entry.core_tags)),
        table_text(&tag_list_label(&entry.proposed_tags)),
        confidence_label(entry.confidence),
        !entry.counterexamples.is_empty()
    ));
    if let Some(review_after) = entry.review_after.as_ref() {
        content.push_str(&format!(
            "- Review after: `{}`\n",
            review_after.to_rfc3339()
        ));
    }
    content
}

fn markdown_candidate_page(candidate: &AdaptiveWikiCandidate) -> String {
    let mut content = String::new();
    content.push_str("---\n");
    push_frontmatter_scalar(&mut content, "id", &candidate.id);
    push_frontmatter_scalar(&mut content, "kind", kind_label(candidate.kind));
    push_frontmatter_scalar(&mut content, "scope", scope_label(candidate.scope));
    push_frontmatter_scalar(&mut content, "scope_ref", &candidate.scope_ref);
    push_frontmatter_list(
        &mut content,
        "agent_modes",
        &agent_mode_values(&candidate.agent_modes),
    );
    push_frontmatter_scalar(
        &mut content,
        "signal_kind",
        signal_label(candidate.signal_kind),
    );
    push_frontmatter_scalar(&mut content, "origin", origin_label(candidate.origin));
    push_frontmatter_scalar(
        &mut content,
        "confidence",
        confidence_label(candidate.confidence),
    );
    push_frontmatter_scalar(
        &mut content,
        "occurrence_count",
        &candidate.occurrence_count.to_string(),
    );
    push_frontmatter_scalar(
        &mut content,
        "last_seen_at",
        &candidate.last_seen_at.to_rfc3339(),
    );
    push_frontmatter_list(&mut content, "evidence_refs", &candidate.evidence_refs);
    push_frontmatter_list(&mut content, "source_refs", &candidate.source_refs);
    push_frontmatter_list(&mut content, "source_hashes", &candidate.source_hashes);
    push_frontmatter_list(
        &mut content,
        "core_tags",
        &clean_tags(candidate.core_tags.clone()),
    );
    push_frontmatter_list(
        &mut content,
        "proposed_tags",
        &clean_tags(candidate.proposed_tags.clone()),
    );
    content.push_str("---\n\n");

    content.push_str(&format!(
        "# {}\n\n",
        markdown_heading(&candidate.claim, &candidate.id)
    ));
    content.push_str("## Summary\n\n");
    content.push_str(&paragraph_or_placeholder(&candidate.human_summary));
    content.push_str("\n\n## Suggested AI Instruction\n\n");
    content.push_str(&paragraph_or_placeholder(
        &candidate.suggested_ai_instruction,
    ));
    content.push_str("\n\n## Review Reason\n\n");
    content.push_str(&paragraph_or_placeholder(&candidate.review_reason));
    content.push_str("\n\n## Evidence\n\n");
    push_markdown_list(
        &mut content,
        &candidate.evidence_refs,
        "_No evidence refs recorded._",
    );
    content.push_str("\n## Sources\n\n");
    push_markdown_list(
        &mut content,
        &candidate.source_refs,
        "_No source refs recorded._",
    );
    content.push_str("\n## Governance\n\n");
    content.push_str(&format!(
        "- Scope: `{}`\n- Signal: `{}`\n- Origin: `{}`\n- Hits: `{}`\n- Core tags: `{}`\n- Proposed tags: `{}`\n- Confidence: `{}`\n",
        table_text(&format!(
            "{}:{}",
            scope_label(candidate.scope),
            candidate.scope_ref
        )),
        signal_label(candidate.signal_kind),
        origin_label(candidate.origin),
        candidate.occurrence_count,
        table_text(&tag_list_label(&candidate.core_tags)),
        table_text(&tag_list_label(&candidate.proposed_tags)),
        confidence_label(candidate.confidence)
    ));
    if let Some(scope) = candidate.suggested_scope.as_ref() {
        content.push_str(&format!(
            "- Suggested scope: `{}`\n",
            table_text(&format!("{}:{}", scope_label(scope.scope), scope.scope_ref))
        ));
    }
    content
}

fn push_frontmatter_scalar(content: &mut String, key: &str, value: &str) {
    content.push_str(key);
    content.push_str(": ");
    content.push_str(&serde_json::to_string(&operator_safe_text(value)).expect("string scalar"));
    content.push('\n');
}

fn push_frontmatter_bool(content: &mut String, key: &str, value: bool) {
    content.push_str(key);
    content.push_str(": ");
    content.push_str(if value { "true" } else { "false" });
    content.push('\n');
}

fn push_frontmatter_list(content: &mut String, key: &str, values: &[String]) {
    content.push_str(key);
    if values.is_empty() {
        content.push_str(": []\n");
        return;
    }
    content.push_str(":\n");
    for value in values {
        content.push_str("  - ");
        content.push_str(&serde_json::to_string(&operator_safe_text(value)).expect("list scalar"));
        content.push('\n');
    }
}

fn push_markdown_list(content: &mut String, values: &[String], empty: &str) {
    if values.is_empty() {
        content.push_str(empty);
        content.push_str("\n\n");
        return;
    }
    for value in values {
        content.push_str("- ");
        content.push_str(&operator_safe_text(value));
        content.push('\n');
    }
    content.push('\n');
}

fn paragraph_or_placeholder(value: &str) -> String {
    let sanitized = operator_safe_text(value);
    if sanitized.trim().is_empty() {
        "_Not recorded._".to_string()
    } else {
        sanitized
    }
}

fn markdown_heading(value: &str, fallback: &str) -> String {
    let value = operator_safe_text(value);
    if value.trim().is_empty() {
        operator_safe_text(fallback)
    } else {
        value.replace('\n', " ")
    }
}

fn table_text(value: &str) -> String {
    operator_safe_text(value)
        .replace('|', "\\|")
        .replace('\n', " ")
}

fn markdown_slug(value: &str, fallback: &str) -> String {
    let mut slug = String::new();
    let mut previous_separator = false;
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            previous_separator = false;
        } else if !previous_separator {
            slug.push('-');
            previous_separator = true;
        }
    }
    let slug = slug.trim_matches('-');
    if slug.is_empty() {
        fallback.to_string()
    } else {
        slug.to_string()
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn kind_dir(kind: AdaptiveWikiKind) -> &'static str {
    match kind {
        AdaptiveWikiKind::Preference => "preference",
        AdaptiveWikiKind::Procedure => "procedure",
        AdaptiveWikiKind::FailurePattern => "failure-pattern",
        AdaptiveWikiKind::PolicyRule => "policy-rule",
        AdaptiveWikiKind::Fact => "fact",
    }
}

fn kind_label(kind: AdaptiveWikiKind) -> &'static str {
    match kind {
        AdaptiveWikiKind::Preference => "preference",
        AdaptiveWikiKind::Procedure => "procedure",
        AdaptiveWikiKind::FailurePattern => "failure_pattern",
        AdaptiveWikiKind::PolicyRule => "policy_rule",
        AdaptiveWikiKind::Fact => "fact",
    }
}

fn scope_label(scope: AdaptiveWikiScope) -> &'static str {
    match scope {
        AdaptiveWikiScope::Session => "session",
        AdaptiveWikiScope::ArtifactKind => "artifact_kind",
        AdaptiveWikiScope::Project => "project",
        AdaptiveWikiScope::UserGlobal => "user_global",
    }
}

fn status_label(status: AdaptiveWikiStatus) -> &'static str {
    match status {
        AdaptiveWikiStatus::Candidate => "candidate",
        AdaptiveWikiStatus::Promoted => "promoted",
        AdaptiveWikiStatus::Deprecated => "deprecated",
    }
}

fn activation_label(mode: AdaptiveWikiActivationMode) -> &'static str {
    match mode {
        AdaptiveWikiActivationMode::ContextOnly => "context_only",
        AdaptiveWikiActivationMode::Confirm => "confirm",
        AdaptiveWikiActivationMode::AutoApply => "auto_apply",
    }
}

fn agent_mode_label(mode: AdaptiveWikiAgentMode) -> &'static str {
    match mode {
        AdaptiveWikiAgentMode::Planning => "planning",
        AdaptiveWikiAgentMode::Development => "development",
        AdaptiveWikiAgentMode::Analysis => "analysis",
        AdaptiveWikiAgentMode::Writing => "writing",
        AdaptiveWikiAgentMode::Critique => "critique",
        AdaptiveWikiAgentMode::Review => "review",
        AdaptiveWikiAgentMode::Maintenance => "maintenance",
    }
}

fn agent_modes_label(modes: &[AdaptiveWikiAgentMode]) -> String {
    if modes.is_empty() {
        "all".to_string()
    } else {
        modes
            .iter()
            .map(|mode| agent_mode_label(*mode))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn agent_mode_values(modes: &[AdaptiveWikiAgentMode]) -> Vec<String> {
    modes
        .iter()
        .map(|mode| agent_mode_label(*mode).to_string())
        .collect()
}

fn tag_list_label(tags: &[String]) -> String {
    let tags = clean_tags(tags.to_vec());
    if tags.is_empty() {
        "-".to_string()
    } else {
        tags.into_iter()
            .map(|tag| format!("#{tag}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

fn confidence_label(confidence: AdaptiveWikiConfidence) -> &'static str {
    match confidence {
        AdaptiveWikiConfidence::Explicit => "explicit",
        AdaptiveWikiConfidence::Repeated => "repeated",
        AdaptiveWikiConfidence::Inferred => "inferred",
    }
}

fn signal_label(signal: AdaptiveWikiSignalKind) -> &'static str {
    match signal {
        AdaptiveWikiSignalKind::OperatorCorrection => "operator_correction",
        AdaptiveWikiSignalKind::ApprovalDenial => "approval_denial",
        AdaptiveWikiSignalKind::Rollback => "rollback",
        AdaptiveWikiSignalKind::RepeatedFailure => "repeated_failure",
        AdaptiveWikiSignalKind::ManualPatch => "manual_patch",
        AdaptiveWikiSignalKind::ExplicitPreference => "explicit_preference",
        AdaptiveWikiSignalKind::ImportedDoc => "imported_doc",
        AdaptiveWikiSignalKind::Unknown => "unknown",
    }
}

fn correction_kind_label(kind: AdaptiveWikiCorrectionKind) -> &'static str {
    match kind {
        AdaptiveWikiCorrectionKind::OperatorCorrection => "operator_correction",
        AdaptiveWikiCorrectionKind::Counterexample => "counterexample",
        AdaptiveWikiCorrectionKind::FailureRecurrence => "failure_recurrence",
        AdaptiveWikiCorrectionKind::Unknown => "unknown",
    }
}

fn origin_label(origin: AdaptiveWikiOrigin) -> &'static str {
    match origin {
        AdaptiveWikiOrigin::OperatorExplicit => "operator_explicit",
        AdaptiveWikiOrigin::RuntimeObserved => "runtime_observed",
        AdaptiveWikiOrigin::BackgroundReview => "background_review",
        AdaptiveWikiOrigin::Imported => "imported",
        AdaptiveWikiOrigin::Unknown => "unknown",
    }
}

fn audit_action_label(action: AdaptiveWikiAuditAction) -> &'static str {
    match action {
        AdaptiveWikiAuditAction::Promote => "promote",
        AdaptiveWikiAuditAction::Reject => "reject",
        AdaptiveWikiAuditAction::Rescope => "rescope",
        AdaptiveWikiAuditAction::Deprecate => "deprecate",
        AdaptiveWikiAuditAction::AddCounterexample => "add_counterexample",
        AdaptiveWikiAuditAction::UpdateRunbook => "update_runbook",
        AdaptiveWikiAuditAction::RenewReviewAfter => "renew_review_after",
        AdaptiveWikiAuditAction::Edit => "edit",
        AdaptiveWikiAuditAction::Retag => "retag",
    }
}

fn live_event_kind_label(kind: AdaptiveWikiLiveEpisodeEventKind) -> &'static str {
    match kind {
        AdaptiveWikiLiveEpisodeEventKind::TaskEnqueued => "task_enqueued",
        AdaptiveWikiLiveEpisodeEventKind::ProjectionAttached => "projection_attached",
        AdaptiveWikiLiveEpisodeEventKind::RuntimeUsageRecorded => "runtime_usage_recorded",
        AdaptiveWikiLiveEpisodeEventKind::OperatorCorrectionObserved => {
            "operator_correction_observed"
        }
        AdaptiveWikiLiveEpisodeEventKind::CandidateRecorded => "candidate_recorded",
        AdaptiveWikiLiveEpisodeEventKind::EntryPromoted => "entry_promoted",
        AdaptiveWikiLiveEpisodeEventKind::CounterexampleRecorded => "counterexample_recorded",
        AdaptiveWikiLiveEpisodeEventKind::EntryDeprecated => "entry_deprecated",
        AdaptiveWikiLiveEpisodeEventKind::TaskCompleted => "task_completed",
        AdaptiveWikiLiveEpisodeEventKind::TaskFailed => "task_failed",
        AdaptiveWikiLiveEpisodeEventKind::ResumePending => "resume_pending",
        AdaptiveWikiLiveEpisodeEventKind::RollbackObserved => "rollback_observed",
    }
}

fn task_status_label(status: OffdeskTaskStatus) -> &'static str {
    match status {
        OffdeskTaskStatus::Queued => "queued",
        OffdeskTaskStatus::PendingApproval => "pending_approval",
        OffdeskTaskStatus::Launched => "launched",
        OffdeskTaskStatus::Running => "running",
        OffdeskTaskStatus::Completed => "completed",
        OffdeskTaskStatus::Failed => "failed",
        OffdeskTaskStatus::ResumePending => "resume_pending",
        OffdeskTaskStatus::Cancelled => "cancelled",
    }
}

fn background_runner_label(runner: super::background::BackgroundRunnerKind) -> &'static str {
    match runner {
        super::background::BackgroundRunnerKind::LocalTmux => "local_tmux",
        super::background::BackgroundRunnerKind::LocalBackground => "local_background",
        super::background::BackgroundRunnerKind::GithubRunner => "github_runner",
        super::background::BackgroundRunnerKind::RemoteWorker => "remote_worker",
    }
}

fn background_phase_label(phase: BackgroundRunnerPhase) -> &'static str {
    match phase {
        BackgroundRunnerPhase::Launched => "launched",
        BackgroundRunnerPhase::HandoffEmitted => "handoff_emitted",
        BackgroundRunnerPhase::PickupAcknowledged => "pickup_acknowledged",
        BackgroundRunnerPhase::ResultReceived => "result_received",
        BackgroundRunnerPhase::Completed => "completed",
        BackgroundRunnerPhase::Failed => "failed",
        BackgroundRunnerPhase::StaleNoAck => "stale_no_ack",
        BackgroundRunnerPhase::StaleLostCallback => "stale_lost_callback",
        BackgroundRunnerPhase::Reconstructable => "reconstructable",
        BackgroundRunnerPhase::RecoveryAcknowledged => "recovery_acknowledged",
    }
}

fn read_entry_state(path: &Path) -> Result<AdaptiveWikiEntryState> {
    if !path.exists() {
        return Ok(AdaptiveWikiEntryState::default());
    }
    let content = fs::read_to_string(path)?;
    if content.trim().is_empty() {
        return Ok(AdaptiveWikiEntryState::default());
    }
    Ok(serde_json::from_str(&content)?)
}

fn read_candidate_state(path: &Path) -> Result<AdaptiveWikiCandidateState> {
    if !path.exists() {
        return Ok(AdaptiveWikiCandidateState::default());
    }
    let content = fs::read_to_string(path)?;
    if content.trim().is_empty() {
        return Ok(AdaptiveWikiCandidateState::default());
    }
    Ok(serde_json::from_str(&content)?)
}

fn write_json_state<T: Serialize>(path: &Path, state: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let serialized = serde_json::to_string_pretty(state)? + "\n";
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("adaptive_wiki_state");
    let temporary = path.with_file_name(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(serialized.as_bytes())?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn append_jsonl<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{}", serde_json::to_string(value)?)?;
    file.sync_data()?;
    Ok(())
}

fn read_jsonl<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(path)?;
    let mut records = Vec::new();
    for line in content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
    {
        records.push(serde_json::from_str(line)?);
    }
    Ok(records)
}

fn default_version() -> String {
    ADAPTIVE_WIKI_VERSION.to_string()
}

fn default_scope_ref() -> String {
    "*".to_string()
}

fn is_default_agent_mode_filter(filter: &AdaptiveWikiAgentModeFilter) -> bool {
    *filter == AdaptiveWikiAgentModeFilter::AllWhenUnspecified
}

fn default_occurrence_count() -> u32 {
    1
}

fn default_timestamp() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0).expect("unix epoch is valid")
}

fn normalize_scope_ref(scope: AdaptiveWikiScope, value: &str) -> String {
    let trimmed = value.trim();
    match scope {
        AdaptiveWikiScope::UserGlobal => "*".to_string(),
        AdaptiveWikiScope::Session => {
            if trimmed.is_empty() {
                "-".to_string()
            } else {
                trimmed.to_string()
            }
        }
        AdaptiveWikiScope::ArtifactKind | AdaptiveWikiScope::Project => {
            if trimmed.is_empty() {
                "*".to_string()
            } else {
                trimmed.to_string()
            }
        }
    }
}

fn normalize_key(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn prefer_new_text(existing: &str, candidate: &str) -> String {
    let trimmed = candidate.trim();
    if trimmed.is_empty() {
        existing.trim().to_string()
    } else {
        trimmed.to_string()
    }
}

fn fallback_text(primary: &str, fallback: &str) -> String {
    let primary = primary.trim();
    if primary.is_empty() {
        fallback.trim().to_string()
    } else {
        primary.to_string()
    }
}

fn correction_record_from_candidate(
    candidate: &AdaptiveWikiCandidate,
    now: DateTime<Utc>,
) -> AdaptiveWikiCorrectionRecord {
    let evidence_refs = safe_refs(candidate.evidence_refs.iter());
    let source_refs = safe_refs(candidate.source_refs.iter());
    let refs: Vec<_> = evidence_refs
        .iter()
        .chain(source_refs.iter())
        .cloned()
        .collect();
    AdaptiveWikiCorrectionRecord {
        id: format!("wiki_correction_{}", Uuid::new_v4()),
        correction_kind: AdaptiveWikiCorrectionKind::OperatorCorrection,
        candidate_id: Some(operator_safe_text(&candidate.id)),
        entry_id: None,
        task_id: first_ref_value(&refs, "task:").map(|value| operator_safe_text(&value)),
        request_id: first_ref_value(&refs, "request:").map(|value| operator_safe_text(&value)),
        project_key: match candidate.scope {
            AdaptiveWikiScope::Project => Some(operator_safe_text(&candidate.scope_ref)),
            _ => None,
        },
        artifact_kind: match candidate.scope {
            AdaptiveWikiScope::ArtifactKind => Some(operator_safe_text(&candidate.scope_ref)),
            _ => None,
        },
        summary: operator_safe_text(&fallback_text(&candidate.human_summary, &candidate.claim)),
        evidence_refs,
        source_refs,
        created_at: now,
    }
}

fn first_ref_value(refs: &[String], prefix: &str) -> Option<String> {
    refs.iter().find_map(|value| {
        value
            .strip_prefix(prefix)
            .map(|suffix| suffix.split(['?', '#']).next().unwrap_or(suffix).trim())
            .filter(|suffix| !suffix.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn clean_ref(value: &str) -> String {
    value.trim().to_string()
}

fn clean_refs(values: Vec<String>) -> Vec<String> {
    let mut cleaned = Vec::new();
    push_unique_many(&mut cleaned, values.iter());
    cleaned
}

fn clean_tag(value: &str) -> String {
    value
        .trim()
        .trim_start_matches('#')
        .trim()
        .to_ascii_lowercase()
}

fn clean_tags(values: Vec<String>) -> Vec<String> {
    let mut cleaned = Vec::new();
    for value in values {
        let tag = clean_tag(&value);
        if !tag.is_empty() && !cleaned.iter().any(|existing| existing == &tag) {
            cleaned.push(tag);
        }
    }
    cleaned
}

fn clean_agent_modes(values: Vec<AdaptiveWikiAgentMode>) -> Vec<AdaptiveWikiAgentMode> {
    let mut cleaned = values;
    cleaned.sort();
    cleaned.dedup();
    cleaned
}

fn push_unique(values: &mut Vec<String>, maybe_value: Option<&str>) {
    let Some(value) = maybe_value.map(clean_ref).filter(|value| !value.is_empty()) else {
        return;
    };
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn push_unique_many<'a>(values: &mut Vec<String>, candidates: impl Iterator<Item = &'a String>) {
    for value in candidates {
        push_unique(values, Some(value));
    }
}

fn push_unique_modes<'a>(
    values: &mut Vec<AdaptiveWikiAgentMode>,
    candidates: impl Iterator<Item = &'a AdaptiveWikiAgentMode>,
) {
    for candidate in candidates {
        if !values.contains(candidate) {
            values.push(*candidate);
        }
    }
    values.sort();
}

fn fallback_subject_id(id: &str) -> String {
    let id = id.trim();
    if id.is_empty() {
        "(missing)".to_string()
    } else {
        id.to_string()
    }
}

fn lint_issue(
    severity: AdaptiveWikiLintSeverity,
    subject_kind: &str,
    subject_id: &str,
    code: &str,
    message: &str,
) -> AdaptiveWikiLintIssue {
    AdaptiveWikiLintIssue {
        severity,
        subject_kind: subject_kind.to_string(),
        subject_id: operator_safe_text(subject_id),
        code: code.to_string(),
        message: operator_safe_text(message),
    }
}

fn lint_summary(
    entries_checked: usize,
    candidates_checked: usize,
    issues: &[AdaptiveWikiLintIssue],
) -> AdaptiveWikiLintSummary {
    let mut summary = AdaptiveWikiLintSummary {
        entries_checked,
        candidates_checked,
        ..AdaptiveWikiLintSummary::default()
    };
    for issue in issues {
        match issue.severity {
            AdaptiveWikiLintSeverity::Error => summary.errors += 1,
            AdaptiveWikiLintSeverity::Warning => summary.warnings += 1,
            AdaptiveWikiLintSeverity::Info => summary.info += 1,
        }
    }
    summary
}

fn matches_query(scope: AdaptiveWikiScope, scope_ref: &str, query: &AdaptiveWikiQuery) -> bool {
    match scope {
        AdaptiveWikiScope::UserGlobal => true,
        AdaptiveWikiScope::Project => matches_optional_ref(query.project_key.as_deref(), scope_ref),
        AdaptiveWikiScope::ArtifactKind => {
            matches_optional_ref(query.artifact_kind.as_deref(), scope_ref)
        }
        AdaptiveWikiScope::Session => matches_optional_ref(query.session_id.as_deref(), scope_ref),
    }
}

fn is_unfiltered_query(query: &AdaptiveWikiQuery) -> bool {
    query.session_id.is_none()
        && query.project_key.is_none()
        && query.artifact_kind.is_none()
        && query.agent_mode.is_none()
        && query.agent_mode_filter == AdaptiveWikiAgentModeFilter::AllWhenUnspecified
}

fn entry_matches_query(entry: &AdaptiveWikiEntry, query: &AdaptiveWikiQuery) -> bool {
    matches_query(entry.scope, &entry.scope_ref, query)
        && matches_agent_mode(&entry.agent_modes, query)
}

fn candidate_matches_query(candidate: &AdaptiveWikiCandidate, query: &AdaptiveWikiQuery) -> bool {
    matches_query(candidate.scope, &candidate.scope_ref, query)
        && matches_agent_mode(&candidate.agent_modes, query)
}

fn matches_agent_mode(entry_modes: &[AdaptiveWikiAgentMode], query: &AdaptiveWikiQuery) -> bool {
    if let Some(query_mode) = query.agent_mode {
        return entry_modes.is_empty() || entry_modes.contains(&query_mode);
    }
    match query.agent_mode_filter {
        AdaptiveWikiAgentModeFilter::AllWhenUnspecified => true,
        AdaptiveWikiAgentModeFilter::SharedWhenUnspecified => entry_modes.is_empty(),
    }
}

fn matches_optional_ref(candidate: Option<&str>, scope_ref: &str) -> bool {
    let scope_ref = scope_ref.trim();
    if scope_ref == "*" {
        return true;
    }
    let Some(candidate) = candidate.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    candidate == scope_ref
}

fn scope_specificity(scope: AdaptiveWikiScope) -> u8 {
    match scope {
        AdaptiveWikiScope::Session => 0,
        AdaptiveWikiScope::Project => 1,
        AdaptiveWikiScope::ArtifactKind => 2,
        AdaptiveWikiScope::UserGlobal => 3,
    }
}

fn activation_order(mode: AdaptiveWikiActivationMode) -> u8 {
    match mode {
        AdaptiveWikiActivationMode::AutoApply => 0,
        AdaptiveWikiActivationMode::Confirm => 1,
        AdaptiveWikiActivationMode::ContextOnly => 2,
    }
}

fn confidence_order(confidence: AdaptiveWikiConfidence) -> u8 {
    match confidence {
        AdaptiveWikiConfidence::Explicit => 0,
        AdaptiveWikiConfidence::Repeated => 1,
        AdaptiveWikiConfidence::Inferred => 2,
    }
}

fn status_order(status: AdaptiveWikiStatus) -> u8 {
    match status {
        AdaptiveWikiStatus::Promoted => 0,
        AdaptiveWikiStatus::Candidate => 1,
        AdaptiveWikiStatus::Deprecated => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::tempdir;

    fn now() -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(1_715_734_400, 0).expect("valid timestamp")
    }

    fn promoted_entry(
        id: &str,
        scope: AdaptiveWikiScope,
        scope_ref: &str,
        instruction: &str,
    ) -> AdaptiveWikiEntry {
        AdaptiveWikiEntry {
            id: id.to_string(),
            kind: AdaptiveWikiKind::Procedure,
            scope,
            scope_ref: scope_ref.to_string(),
            status: AdaptiveWikiStatus::Promoted,
            activation_mode: AdaptiveWikiActivationMode::Confirm,
            agent_modes: Vec::new(),
            claim: "preserve evidence sections".to_string(),
            ai_instruction: instruction.to_string(),
            human_summary: "Operator wants evidence sections preserved".to_string(),
            evidence_refs: vec!["audit:one".to_string()],
            counterexamples: Vec::new(),
            support_refs: Vec::new(),
            capability_ids: Vec::new(),
            required_artifact_kinds: Vec::new(),
            core_tags: Vec::new(),
            proposed_tags: Vec::new(),
            confidence: AdaptiveWikiConfidence::Repeated,
            created_at: now(),
            updated_at: now(),
            review_after: None,
        }
    }

    #[test]
    fn legacy_runtime_policy_ack_json_defaults_to_exact_query_scope() -> Result<()> {
        let acknowledgement: AdaptiveWikiRuntimePolicyAcknowledgement =
            serde_json::from_value(json!({
                "id": "wiki_runtime_policy_ack_legacy",
                "query": {
                    "session_id": "request",
                    "project_key": "project",
                    "artifact_kind": "report"
                },
                "budget": {
                    "max_entries": 8,
                    "max_context_chars": 4000,
                    "max_instruction_chars": 500
                },
                "policy": {
                    "review_expired": "exclude"
                },
                "comparison_hash": "abc123",
                "selected_only_in_warn": [],
                "selected_only_in_strict": [],
                "review_expired_excluded": [],
                "created_at": now(),
                "expires_at": now() + Duration::hours(1)
            }))?;

        assert_eq!(
            acknowledgement.scope_mode,
            AdaptiveWikiRuntimePolicyAckScopeMode::ExactQuery
        );
        Ok(())
    }

    #[test]
    fn ai_projection_uses_only_promoted_entries_and_redacts_instruction() {
        let mut entries = vec![
            promoted_entry(
                "project_rule",
                AdaptiveWikiScope::Project,
                "project-a",
                "Use section split. token=sk-secretsecretsecretsecret",
            ),
            promoted_entry(
                "other_project",
                AdaptiveWikiScope::Project,
                "project-b",
                "Do not include this.",
            ),
        ];
        entries.push(AdaptiveWikiEntry {
            status: AdaptiveWikiStatus::Candidate,
            id: "candidate".to_string(),
            ai_instruction: "Do not expose candidates".to_string(),
            ..promoted_entry(
                "candidate_base",
                AdaptiveWikiScope::UserGlobal,
                "*",
                "candidate",
            )
        });

        let projection = build_ai_projection(
            &entries,
            &AdaptiveWikiQuery {
                project_key: Some("project-a".to_string()),
                ..AdaptiveWikiQuery::default()
            },
        );

        assert_eq!(projection.len(), 1);
        assert_eq!(projection[0].id, "project_rule");
        assert!(projection[0].instruction.contains("REDACTED"));
        assert!(!projection[0].instruction.contains("sk-secret"));
    }

    #[test]
    fn ai_projection_filters_agent_mode_entries_and_keeps_universal_entries() {
        let universal = promoted_entry(
            "universal_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "Shared guidance applies to every agent mode.",
        );
        let mut code = promoted_entry(
            "code_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "Code development guidance.",
        );
        code.agent_modes = vec![AdaptiveWikiAgentMode::Development];
        let mut critique = promoted_entry(
            "critique_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "Critique-only guidance.",
        );
        critique.agent_modes = vec![AdaptiveWikiAgentMode::Critique];

        let projection = build_ai_projection(
            &[universal, code, critique],
            &AdaptiveWikiQuery {
                project_key: Some("project-a".to_string()),
                agent_mode: Some(AdaptiveWikiAgentMode::Development),
                ..AdaptiveWikiQuery::default()
            },
        );

        let ids: Vec<&str> = projection.iter().map(|entry| entry.id.as_str()).collect();
        assert_eq!(ids.len(), 2);
        assert!(ids.contains(&"universal_rule"));
        assert!(ids.contains(&"code_rule"));
        assert!(!ids.contains(&"critique_rule"));
    }

    #[test]
    fn ai_projection_shared_when_unspecified_policy_keeps_mode_specific_entries_out() {
        let universal = promoted_entry(
            "universal_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "Shared guidance applies without a mode.",
        );
        let mut code = promoted_entry(
            "code_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "Code development guidance.",
        );
        code.agent_modes = vec![AdaptiveWikiAgentMode::Development];

        let projection = build_ai_projection(
            &[universal, code],
            &AdaptiveWikiQuery {
                project_key: Some("project-a".to_string()),
                agent_mode_filter: AdaptiveWikiAgentModeFilter::SharedWhenUnspecified,
                ..AdaptiveWikiQuery::default()
            },
        );

        assert_eq!(projection.len(), 1);
        assert_eq!(projection[0].id, "universal_rule");
    }

    #[test]
    fn legacy_agent_mode_values_load_as_canonical_modes() -> Result<()> {
        let state: AdaptiveWikiEntryState = serde_json::from_value(json!({
            "entries": [
                {
                    "id": "legacy_modes",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project-a",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "agent_modes": ["code_development", "research_writing", "review"],
                    "claim": "Legacy mode values stay loadable.",
                    "ai_instruction": "Use canonical mode labels after load.",
                    "evidence_refs": ["task:legacy"],
                    "confidence": "explicit"
                }
            ]
        }))?;

        assert_eq!(
            state.entries[0].agent_modes,
            vec![
                AdaptiveWikiAgentMode::Development,
                AdaptiveWikiAgentMode::Writing,
                AdaptiveWikiAgentMode::Review,
            ]
        );
        assert_eq!(
            serde_json::to_value(&state.entries[0].agent_modes)?,
            json!(["development", "writing", "review"])
        );
        Ok(())
    }

    #[test]
    fn ai_projection_report_applies_budget_priority_and_rejections() {
        let mut session = promoted_entry(
            "session_rule",
            AdaptiveWikiScope::Session,
            "request-a",
            "Use the very specific session correction before broader guidance.",
        );
        session.confidence = AdaptiveWikiConfidence::Inferred;
        let mut project = promoted_entry(
            "project_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "Use project guidance.",
        );
        project.confidence = AdaptiveWikiConfidence::Explicit;
        let mut empty = promoted_entry("empty_rule", AdaptiveWikiScope::Project, "project-a", "");
        empty.claim.clear();

        let report = build_ai_projection_report(
            &[project, session, empty],
            &AdaptiveWikiQuery {
                session_id: Some("request-a".to_string()),
                project_key: Some("project-a".to_string()),
                ..AdaptiveWikiQuery::default()
            },
            AdaptiveWikiProjectionBudget {
                max_entries: 1,
                max_context_chars: 4_000,
                max_instruction_chars: 24,
            },
        );

        assert_eq!(report.summary.entries_checked, 3);
        assert_eq!(report.summary.promoted_scope_matches, 3);
        assert_eq!(report.summary.selected, 1);
        assert_eq!(report.summary.rejected, 2);
        assert_eq!(report.summary.instructions_truncated, 1);
        assert_eq!(report.selected[0].id, "session_rule");
        assert!(report.selected[0].instruction.ends_with("..."));
        assert!(report.rejected.iter().any(|rejection| {
            rejection.entry_id == "empty_rule"
                && rejection.reason == AdaptiveWikiProjectionRejectionReason::EmptyInstruction
        }));
        assert!(report.rejected.iter().any(|rejection| {
            rejection.entry_id == "project_rule"
                && rejection.reason == AdaptiveWikiProjectionRejectionReason::BudgetMaxEntries
        }));
    }

    #[test]
    fn ai_projection_report_flags_conflicts_and_review_proposes_resolution() {
        let entries = vec![
            promoted_entry(
                "allow_tables",
                AdaptiveWikiScope::Project,
                "project-a",
                "Use markdown tables for report evidence",
            ),
            promoted_entry(
                "block_tables",
                AdaptiveWikiScope::Project,
                "project-a",
                "Do not use markdown tables for report evidence",
            ),
            promoted_entry(
                "other_project",
                AdaptiveWikiScope::Project,
                "project-b",
                "Do not use markdown tables for report evidence",
            ),
        ];

        let query = AdaptiveWikiQuery {
            project_key: Some("project-a".to_string()),
            ..AdaptiveWikiQuery::default()
        };
        let report =
            build_ai_projection_report(&entries, &query, AdaptiveWikiProjectionBudget::default());

        assert_eq!(report.summary.selected, 2);
        assert_eq!(report.summary.conflicts, 1);
        assert_eq!(report.conflicts.len(), 1);
        assert_eq!(report.conflicts[0].entry_id, "allow_tables");
        assert_eq!(report.conflicts[0].conflicting_entry_id, "block_tables");
        assert_eq!(
            report.conflicts[0].signature,
            "markdown tables for report evidence"
        );

        let lint = build_lint_report(&entries, &[], now());
        let proposals = build_review_proposals(&entries, &[], &[], &[], &[], &lint, now());
        let proposal = proposals
            .iter()
            .find(|proposal| {
                proposal.action == AdaptiveWikiReviewProposalAction::Split
                    && proposal.subject_id == "allow_tables"
                    && proposal.title == "Resolve conflicting promoted entries"
            })
            .expect("conflict resolution proposal");

        assert_eq!(proposal.risk, AdaptiveWikiReviewRisk::High);
        assert!(proposal
            .evidence_refs
            .iter()
            .any(|value| value == "entry:block_tables"));
        assert!(proposal
            .evidence_refs
            .iter()
            .any(|value| value == "projection:markdown tables for report evidence"));
        assert!(proposal.suggested_command.is_none());
    }

    #[test]
    fn ai_projection_report_warns_review_expired_without_excluding() {
        let mut expired = promoted_entry(
            "expired_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "Keep using this rule until an operator reviews it.",
        );
        expired.review_after = Some(now());

        let report = build_ai_projection_report(
            &[expired],
            &AdaptiveWikiQuery {
                project_key: Some("project-a".to_string()),
                ..AdaptiveWikiQuery::default()
            },
            AdaptiveWikiProjectionBudget::default(),
        );

        assert_eq!(report.summary.selected, 1);
        assert_eq!(report.summary.review_expired_projected, 1);
        assert_eq!(report.selected[0].id, "expired_rule");
        assert_eq!(report.review_expired.len(), 1);
        assert_eq!(report.review_expired[0].entry_id, "expired_rule");
        assert_eq!(report.review_expired[0].review_after, now());
        assert!(report.review_expired[0]
            .detail
            .contains("default warn policy"));
    }

    #[test]
    fn ai_projection_report_can_exclude_review_expired_by_policy() {
        let mut expired = promoted_entry(
            "expired_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "This expired rule should not project under strict policy.",
        );
        expired.review_after = Some(now());
        let active = promoted_entry(
            "active_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "This active rule should still project.",
        );

        let report = build_ai_projection_report_with_policy(
            &[expired, active],
            &AdaptiveWikiQuery {
                project_key: Some("project-a".to_string()),
                ..AdaptiveWikiQuery::default()
            },
            AdaptiveWikiProjectionBudget::default(),
            AdaptiveWikiProjectionPolicy {
                review_expired: AdaptiveWikiProjectionReviewExpiredPolicy::Exclude,
            },
        );

        assert_eq!(
            report.policy.review_expired,
            AdaptiveWikiProjectionReviewExpiredPolicy::Exclude
        );
        assert_eq!(report.summary.promoted_scope_matches, 2);
        assert_eq!(report.summary.selected, 1);
        assert_eq!(report.summary.rejected, 1);
        assert_eq!(report.summary.review_expired_projected, 0);
        assert!(report.review_expired.is_empty());
        assert_eq!(report.selected[0].id, "active_rule");
        assert!(report.rejected.iter().any(|rejection| {
            rejection.entry_id == "expired_rule"
                && rejection.reason == AdaptiveWikiProjectionRejectionReason::ReviewExpiredExcluded
        }));
    }

    #[test]
    fn ai_projection_policy_comparison_reports_warn_and_strict_delta() {
        let mut expired = promoted_entry(
            "aaa_expired_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "This expired rule wins the default budget.",
        );
        expired.review_after = Some(now());
        let active = promoted_entry(
            "zzz_active_rule",
            AdaptiveWikiScope::Project,
            "project-a",
            "This active rule replaces the expired rule under strict policy.",
        );

        let report = build_ai_projection_review_expired_policy_comparison(
            &[expired, active],
            &AdaptiveWikiQuery {
                project_key: Some("project-a".to_string()),
                ..AdaptiveWikiQuery::default()
            },
            AdaptiveWikiProjectionBudget {
                max_entries: 1,
                max_context_chars: 4_000,
                max_instruction_chars: 500,
            },
        );

        assert_eq!(report.warn.selected[0].id, "aaa_expired_rule");
        assert_eq!(report.strict.selected[0].id, "zzz_active_rule");
        assert_eq!(
            report.summary.selected_only_in_warn,
            vec!["aaa_expired_rule"]
        );
        assert_eq!(
            report.summary.selected_only_in_strict,
            vec!["zzz_active_rule"]
        );
        assert_eq!(
            report.summary.review_expired_excluded,
            vec!["aaa_expired_rule"]
        );
    }

    #[test]
    fn human_projection_keeps_governance_fields_separate_from_ai_projection() {
        let entry = AdaptiveWikiEntry {
            human_summary: "Human note with password=supersecret".to_string(),
            evidence_refs: vec!["task:one?token=secret".to_string()],
            counterexamples: vec!["diff:two".to_string()],
            ..promoted_entry(
                "entry",
                AdaptiveWikiScope::ArtifactKind,
                "report",
                "Keep evidence separate.",
            )
        };
        let candidate = AdaptiveWikiCandidate {
            id: "cand".to_string(),
            kind: AdaptiveWikiKind::FailurePattern,
            scope: AdaptiveWikiScope::ArtifactKind,
            scope_ref: "report".to_string(),
            agent_modes: Vec::new(),
            claim: "same correction repeated".to_string(),
            suggested_ai_instruction: "Confirm before rewriting.".to_string(),
            human_summary: "Candidate has api_key=abc123456789".to_string(),
            evidence_refs: vec!["audit:three".to_string()],
            signal_kind: AdaptiveWikiSignalKind::OperatorCorrection,
            origin: AdaptiveWikiOrigin::OperatorExplicit,
            source_refs: vec!["task:three?token=secret".to_string()],
            source_hashes: vec!["sha256:abc".to_string()],
            core_tags: vec!["artifact/report".to_string()],
            proposed_tags: vec!["method/baseline-first".to_string()],
            suggested_scope: Some(AdaptiveWikiScopeSuggestion {
                scope: AdaptiveWikiScope::ArtifactKind,
                scope_ref: "report".to_string(),
            }),
            review_reason: "Human review has token=secret".to_string(),
            occurrence_count: 3,
            confidence: AdaptiveWikiConfidence::Repeated,
            created_at: now(),
            updated_at: now(),
            last_seen_at: now(),
        };

        let projection = build_human_projection(
            &[entry],
            &[candidate],
            &AdaptiveWikiQuery {
                artifact_kind: Some("report".to_string()),
                ..AdaptiveWikiQuery::default()
            },
        );

        assert_eq!(projection.entries.len(), 1);
        assert_eq!(projection.candidates.len(), 1);
        assert!(projection.entries[0].human_summary.contains("[REDACTED]"));
        assert!(projection.entries[0].evidence_refs[0].contains("[REDACTED]"));
        assert_eq!(projection.entries[0].counterexamples[0], "diff:two");
        assert!(projection.candidates[0]
            .human_summary
            .contains("[REDACTED]"));
        assert!(projection.candidates[0].source_refs[0].contains("[REDACTED]"));
        assert!(projection.candidates[0]
            .review_reason
            .contains("[REDACTED]"));
    }

    #[test]
    fn graph_report_uses_core_tags_as_edges_and_keeps_proposed_tags_reviewable() {
        let entry = AdaptiveWikiEntry {
            core_tags: vec!["#project/project-a".to_string()],
            proposed_tags: vec!["project/project-a".to_string()],
            capability_ids: vec!["dispatch.runtime".to_string()],
            ..promoted_entry(
                "entry",
                AdaptiveWikiScope::Project,
                "project-a",
                "Use project runbook.",
            )
        };
        let candidate = AdaptiveWikiCandidate {
            id: "candidate".to_string(),
            kind: AdaptiveWikiKind::FailurePattern,
            scope: AdaptiveWikiScope::ArtifactKind,
            scope_ref: "report".to_string(),
            agent_modes: Vec::new(),
            claim: "same correction repeated".to_string(),
            suggested_ai_instruction: "Confirm before rewriting.".to_string(),
            human_summary: "Candidate summary".to_string(),
            evidence_refs: vec!["audit:three".to_string()],
            signal_kind: AdaptiveWikiSignalKind::OperatorCorrection,
            origin: AdaptiveWikiOrigin::OperatorExplicit,
            source_refs: Vec::new(),
            source_hashes: Vec::new(),
            core_tags: Vec::new(),
            proposed_tags: vec!["important".to_string()],
            suggested_scope: None,
            review_reason: "Review candidate".to_string(),
            occurrence_count: 3,
            confidence: AdaptiveWikiConfidence::Repeated,
            created_at: now(),
            updated_at: now(),
            last_seen_at: now(),
        };

        let report = build_graph_report(&[entry], &[candidate], now());

        assert_eq!(report.summary.entries, 1);
        assert_eq!(report.summary.candidates, 1);
        assert!(report
            .nodes
            .iter()
            .any(|node| node.id == "tag:project/project-a"));
        assert!(report.edges.iter().any(|edge| {
            edge.relationship == "has_core_tag" && edge.target == "tag:project/project-a"
        }));
        assert!(report.edges.iter().any(|edge| {
            edge.relationship == "has_proposed_tag" && edge.target == "tag:project/project-a"
        }));
        assert!(report.review_issues.iter().any(|issue| {
            issue.code == "proposed_tag_matches_core_prefix" && issue.tag == "project/project-a"
        }));
        assert!(report
            .review_issues
            .iter()
            .any(|issue| issue.code == "unscoped_tag" && issue.tag == "important"));
    }

    #[test]
    fn candidate_recording_merges_repeated_claims_and_evidence_refs() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        let secret = "sk-secretsecretsecretsecret";
        let input = AdaptiveWikiCandidateInput {
            kind: AdaptiveWikiKind::Preference,
            scope: AdaptiveWikiScope::Project,
            scope_ref: "project-a".to_string(),
            claim: "Keep recommendations above details".to_string(),
            suggested_ai_instruction: "Lead with recommendation.".to_string(),
            human_summary: format!(
                "User repeatedly asked to frontload recommendations token={secret}."
            ),
            evidence_ref: Some("task:one".to_string()),
            signal_kind: AdaptiveWikiSignalKind::OperatorCorrection,
            origin: AdaptiveWikiOrigin::OperatorExplicit,
            source_refs: vec![
                "request:request-one".to_string(),
                format!("approval:one?token={secret}"),
            ],
            source_hashes: Vec::new(),
            suggested_scope: Some(AdaptiveWikiScopeSuggestion {
                scope: AdaptiveWikiScope::Project,
                scope_ref: "project-a".to_string(),
            }),
            agent_modes: vec![AdaptiveWikiAgentMode::Development],
            core_tags: vec!["project/project-a".to_string()],
            proposed_tags: vec!["method/frontload-recommendations".to_string()],
            review_reason: "Repeated correction".to_string(),
            confidence: AdaptiveWikiConfidence::Repeated,
        };

        let first = store.record_candidate(input.clone(), now())?;
        let second = store.record_candidate(
            AdaptiveWikiCandidateInput {
                evidence_ref: Some("task:two".to_string()),
                agent_modes: vec![AdaptiveWikiAgentMode::Critique],
                ..input
            },
            now(),
        )?;

        assert_eq!(first.id, second.id);
        assert_eq!(second.occurrence_count, 2);
        assert_eq!(second.evidence_refs, vec!["task:one", "task:two"]);
        assert_eq!(
            second.agent_modes,
            vec![
                AdaptiveWikiAgentMode::Development,
                AdaptiveWikiAgentMode::Critique
            ]
        );
        assert_eq!(
            second.source_refs,
            vec![
                "request:request-one".to_string(),
                format!("approval:one?token={secret}")
            ]
        );
        assert_eq!(
            second.signal_kind,
            AdaptiveWikiSignalKind::OperatorCorrection
        );
        let corrections = store.load_correction_records()?;
        assert_eq!(corrections.len(), 2);
        assert_eq!(
            corrections[0].correction_kind,
            AdaptiveWikiCorrectionKind::OperatorCorrection
        );
        assert_eq!(
            corrections[0].candidate_id.as_deref(),
            Some(first.id.as_str())
        );
        assert_eq!(corrections[0].task_id.as_deref(), Some("one"));
        assert_eq!(corrections[0].request_id.as_deref(), Some("request-one"));
        assert_eq!(corrections[0].project_key.as_deref(), Some("project-a"));
        assert_eq!(
            corrections[1].candidate_id.as_deref(),
            Some(second.id.as_str())
        );
        assert_eq!(corrections[1].task_id.as_deref(), Some("one"));
        let serialized = serde_json::to_string(&corrections)?;
        assert!(!serialized.contains(secret));
        assert!(serialized.contains("[REDACTED]"));
        Ok(())
    }

    #[test]
    fn concurrent_candidate_writers_preserve_every_occurrence() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        let writers = 16;
        let barrier = Arc::new(Barrier::new(writers));
        let mut handles = Vec::new();
        for index in 0..writers {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || -> Result<()> {
                barrier.wait();
                store.record_candidate(
                    AdaptiveWikiCandidateInput {
                        kind: AdaptiveWikiKind::Preference,
                        scope: AdaptiveWikiScope::Project,
                        scope_ref: "forager".to_string(),
                        claim: "Serialize adaptive wiki candidate updates".to_string(),
                        suggested_ai_instruction: "Use the shared wiki writer lock.".to_string(),
                        human_summary: "Concurrent writer regression test".to_string(),
                        evidence_ref: Some(format!("test:writer:{index}")),
                        signal_kind: AdaptiveWikiSignalKind::Unknown,
                        origin: AdaptiveWikiOrigin::RuntimeObserved,
                        source_refs: Vec::new(),
                        source_hashes: Vec::new(),
                        suggested_scope: None,
                        agent_modes: Vec::new(),
                        core_tags: Vec::new(),
                        proposed_tags: Vec::new(),
                        review_reason: "Verify serialization".to_string(),
                        confidence: AdaptiveWikiConfidence::Repeated,
                    },
                    now(),
                )?;
                Ok(())
            }));
        }
        for handle in handles {
            handle.join().expect("candidate writer thread panicked")?;
        }

        let state = store.load_candidates()?;
        assert_eq!(state.candidates.len(), 1);
        assert_eq!(state.candidates[0].occurrence_count, writers as u32);
        assert_eq!(state.candidates[0].evidence_refs.len(), writers);
        assert!(!fs::read_dir(temp.path())?.any(|entry| {
            entry
                .ok()
                .and_then(|entry| entry.file_name().into_string().ok())
                .is_some_and(|name| name.ends_with(".tmp"))
        }));
        Ok(())
    }

    #[test]
    fn legacy_correction_json_loads_with_defaults() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        fs::write(
            store.corrections_path(),
            serde_json::to_string(&json!({
                "id": "legacy_correction"
            }))?,
        )?;

        let corrections = store.load_correction_records()?;

        assert_eq!(corrections.len(), 1);
        assert_eq!(corrections[0].id, "legacy_correction");
        assert_eq!(
            corrections[0].correction_kind,
            AdaptiveWikiCorrectionKind::Unknown
        );
        assert!(corrections[0].summary.is_empty());
        assert!(corrections[0].evidence_refs.is_empty());
        assert!(corrections[0].source_refs.is_empty());
        Ok(())
    }

    #[test]
    fn review_proposal_events_roundtrip_and_legacy_defaults() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        store.append_review_proposal_event(&AdaptiveWikiReviewProposalEventRecord {
            id: "wiki_review_event_one".to_string(),
            proposal_id: "wiki_review_rescope_entry_wiki-entry".to_string(),
            decision: AdaptiveWikiReviewProposalDecision::Accepted,
            proposal_action: Some(AdaptiveWikiReviewProposalAction::Rescope),
            subject_kind: "entry".to_string(),
            subject_id: "wiki_entry".to_string(),
            actor: "cli".to_string(),
            reason: "Operator accepted rescope proposal".to_string(),
            evidence_refs: vec!["review:report".to_string()],
            supersedes: None,
            created_at: now(),
        })?;
        fs::write(
            store.review_events_path(),
            format!(
                "{}\n{}\n",
                fs::read_to_string(store.review_events_path())?,
                serde_json::to_string(&json!({
                    "id": "legacy_review_event",
                    "proposal_id": "legacy_proposal"
                }))?
            ),
        )?;

        let events = store.load_review_proposal_events()?;

        assert_eq!(events.len(), 2);
        assert_eq!(
            events[0].decision,
            AdaptiveWikiReviewProposalDecision::Accepted
        );
        assert_eq!(
            events[0].proposal_action,
            Some(AdaptiveWikiReviewProposalAction::Rescope)
        );
        assert_eq!(
            events[1].decision,
            AdaptiveWikiReviewProposalDecision::Unknown
        );
        assert!(events[1].subject_kind.is_empty());
        assert!(events[1].evidence_refs.is_empty());
        Ok(())
    }

    #[test]
    fn review_report_annotates_proposals_with_latest_lifecycle_event() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        let reviewed_at = now();
        let secret = "sk-secretsecretsecretsecret";
        fs::write(
            store.candidates_path(),
            serde_json::to_string_pretty(&AdaptiveWikiCandidateState {
                version: default_version(),
                candidates: vec![AdaptiveWikiCandidate {
                    id: "wiki_candidate".to_string(),
                    kind: AdaptiveWikiKind::FailurePattern,
                    scope: AdaptiveWikiScope::Project,
                    scope_ref: "project".to_string(),
                    agent_modes: Vec::new(),
                    claim: "Repeated correction should become durable wiki guidance".to_string(),
                    suggested_ai_instruction: "Review repeated corrections before repeating them."
                        .to_string(),
                    human_summary: "Repeated correction candidate.".to_string(),
                    evidence_refs: vec!["task:wiki_candidate".to_string()],
                    signal_kind: AdaptiveWikiSignalKind::OperatorCorrection,
                    origin: AdaptiveWikiOrigin::RuntimeObserved,
                    source_refs: Vec::new(),
                    source_hashes: Vec::new(),
                    core_tags: vec!["project/project".to_string()],
                    proposed_tags: Vec::new(),
                    suggested_scope: None,
                    review_reason: String::new(),
                    occurrence_count: 2,
                    confidence: AdaptiveWikiConfidence::Repeated,
                    created_at: reviewed_at,
                    updated_at: reviewed_at,
                    last_seen_at: reviewed_at,
                }],
            })?,
        )?;
        store.append_review_proposal_event(&AdaptiveWikiReviewProposalEventRecord {
            id: "wiki_review_event_old".to_string(),
            proposal_id: "wiki_review_promote_candidate_wiki-candidate".to_string(),
            decision: AdaptiveWikiReviewProposalDecision::Rejected,
            proposal_action: Some(AdaptiveWikiReviewProposalAction::Promote),
            subject_kind: "candidate".to_string(),
            subject_id: "wiki_candidate".to_string(),
            actor: "cli".to_string(),
            reason: "Older decision".to_string(),
            evidence_refs: vec!["review:old".to_string()],
            supersedes: None,
            created_at: reviewed_at - Duration::minutes(5),
        })?;
        store.append_review_proposal_event(&AdaptiveWikiReviewProposalEventRecord {
            id: "wiki_review_event_new".to_string(),
            proposal_id: "wiki_review_promote_candidate_wiki-candidate".to_string(),
            decision: AdaptiveWikiReviewProposalDecision::Accepted,
            proposal_action: Some(AdaptiveWikiReviewProposalAction::Promote),
            subject_kind: "candidate".to_string(),
            subject_id: "wiki_candidate".to_string(),
            actor: "operator".to_string(),
            reason: format!("Accepted after review token={secret}"),
            evidence_refs: vec![format!("review:latest?token={secret}")],
            supersedes: Some("wiki_review_event_old".to_string()),
            created_at: reviewed_at,
        })?;

        let report = store.generate_review_report(true, reviewed_at)?;

        assert_eq!(report.summary.proposals, 1);
        assert_eq!(report.summary.review_events_checked, 2);
        assert_eq!(report.summary.proposals_with_events, 1);
        assert_eq!(report.summary.open_proposals, 0);
        assert_eq!(report.summary.accepted_proposals, 1);
        assert_eq!(report.summary.rejected_proposals, 0);
        assert_eq!(report.summary.stale_decision_proposals, 0);
        let lifecycle = report.proposals[0]
            .lifecycle
            .as_ref()
            .expect("proposal lifecycle");
        assert_eq!(lifecycle.latest_event_id, "wiki_review_event_new");
        assert_eq!(
            lifecycle.decision,
            AdaptiveWikiReviewProposalDecision::Accepted
        );
        assert!(!lifecycle.stale);
        assert!(lifecycle.reason.contains("[REDACTED]"));
        assert!(!serde_json::to_string(&report)?.contains(secret));
        Ok(())
    }

    #[test]
    fn review_report_marks_lifecycle_decision_stale_after_new_subject_evidence() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        let decided_at = now();
        let refreshed_at = decided_at + Duration::minutes(10);
        fs::write(
            store.candidates_path(),
            serde_json::to_string_pretty(&AdaptiveWikiCandidateState {
                version: default_version(),
                candidates: vec![AdaptiveWikiCandidate {
                    id: "wiki_candidate".to_string(),
                    kind: AdaptiveWikiKind::FailurePattern,
                    scope: AdaptiveWikiScope::Project,
                    scope_ref: "project".to_string(),
                    agent_modes: Vec::new(),
                    claim: "Repeated correction should become durable wiki guidance".to_string(),
                    suggested_ai_instruction: "Review repeated corrections before repeating them."
                        .to_string(),
                    human_summary: "Repeated correction candidate.".to_string(),
                    evidence_refs: vec!["task:wiki_candidate".to_string()],
                    signal_kind: AdaptiveWikiSignalKind::OperatorCorrection,
                    origin: AdaptiveWikiOrigin::RuntimeObserved,
                    source_refs: Vec::new(),
                    source_hashes: Vec::new(),
                    core_tags: vec!["project/project".to_string()],
                    proposed_tags: Vec::new(),
                    suggested_scope: None,
                    review_reason: String::new(),
                    occurrence_count: 2,
                    confidence: AdaptiveWikiConfidence::Repeated,
                    created_at: decided_at - Duration::minutes(30),
                    updated_at: refreshed_at,
                    last_seen_at: refreshed_at,
                }],
            })?,
        )?;
        store.append_review_proposal_event(&AdaptiveWikiReviewProposalEventRecord {
            id: "wiki_review_event_accepted".to_string(),
            proposal_id: "wiki_review_promote_candidate_wiki-candidate".to_string(),
            decision: AdaptiveWikiReviewProposalDecision::Accepted,
            proposal_action: Some(AdaptiveWikiReviewProposalAction::Promote),
            subject_kind: "candidate".to_string(),
            subject_id: "wiki_candidate".to_string(),
            actor: "operator".to_string(),
            reason: "Accepted before the candidate changed".to_string(),
            evidence_refs: vec!["review:accepted".to_string()],
            supersedes: None,
            created_at: decided_at,
        })?;

        let report = store.generate_review_report(true, refreshed_at)?;

        assert_eq!(report.summary.proposals, 1);
        assert_eq!(report.summary.proposals_with_events, 1);
        assert_eq!(report.summary.accepted_proposals, 1);
        assert_eq!(report.summary.open_proposals, 1);
        assert_eq!(report.summary.stale_decision_proposals, 1);
        let lifecycle = report.proposals[0]
            .lifecycle
            .as_ref()
            .expect("proposal lifecycle");
        assert_eq!(
            lifecycle.decision,
            AdaptiveWikiReviewProposalDecision::Accepted
        );
        assert!(lifecycle.stale);
        assert_eq!(
            lifecycle.stale_evidence_refs,
            vec!["candidate:wiki_candidate".to_string()]
        );
        Ok(())
    }

    #[test]
    fn promotion_moves_candidate_to_promoted_entry() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        let candidate = store.record_candidate(
            AdaptiveWikiCandidateInput {
                kind: AdaptiveWikiKind::FailurePattern,
                scope: AdaptiveWikiScope::ArtifactKind,
                scope_ref: "report".to_string(),
                claim: "Evidence and recommendations were merged".to_string(),
                suggested_ai_instruction: "Confirm before merging evidence and recommendations."
                    .to_string(),
                human_summary: "Repeated correction on report section boundaries.".to_string(),
                evidence_ref: Some("audit:one".to_string()),
                signal_kind: AdaptiveWikiSignalKind::RepeatedFailure,
                origin: AdaptiveWikiOrigin::RuntimeObserved,
                source_refs: vec!["task:one".to_string()],
                source_hashes: Vec::new(),
                suggested_scope: None,
                agent_modes: vec![AdaptiveWikiAgentMode::Writing],
                core_tags: vec!["artifact/report".to_string()],
                proposed_tags: vec!["failure/merged-evidence-recommendations".to_string()],
                review_reason: "Repeated failure".to_string(),
                confidence: AdaptiveWikiConfidence::Repeated,
            },
            now(),
        )?;

        let entry = store
            .promote_candidate(&candidate.id, AdaptiveWikiActivationMode::Confirm, now())?
            .expect("candidate promoted");

        assert_eq!(entry.status, AdaptiveWikiStatus::Promoted);
        assert_eq!(entry.evidence_refs, vec!["audit:one"]);
        assert_eq!(entry.agent_modes, vec![AdaptiveWikiAgentMode::Writing]);
        assert_eq!(entry.core_tags, vec!["artifact/report"]);
        assert_eq!(
            entry.proposed_tags,
            vec!["failure/merged-evidence-recommendations"]
        );
        assert!(store.load_candidates()?.candidates.is_empty());
        assert_eq!(
            store
                .ai_projection(&AdaptiveWikiQuery {
                    artifact_kind: Some("report".to_string()),
                    ..AdaptiveWikiQuery::default()
                })?
                .len(),
            1
        );
        Ok(())
    }

    #[test]
    fn review_mutations_update_wiki_state_and_append_audit() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        let promoted = store.record_candidate(
            AdaptiveWikiCandidateInput {
                kind: AdaptiveWikiKind::PolicyRule,
                scope: AdaptiveWikiScope::Project,
                scope_ref: "project-a".to_string(),
                claim: "Ask before dispatch retry".to_string(),
                suggested_ai_instruction: "Ask the operator before retrying dispatch.".to_string(),
                human_summary: "Captured from denial.".to_string(),
                evidence_ref: Some("approval:one".to_string()),
                signal_kind: AdaptiveWikiSignalKind::ApprovalDenial,
                origin: AdaptiveWikiOrigin::OperatorExplicit,
                source_refs: vec!["approval:one".to_string()],
                source_hashes: Vec::new(),
                suggested_scope: None,
                agent_modes: Vec::new(),
                core_tags: vec!["project/project-a".to_string()],
                proposed_tags: Vec::new(),
                review_reason: "Operator denied retry.".to_string(),
                confidence: AdaptiveWikiConfidence::Explicit,
            },
            now(),
        )?;
        let rejected = store.record_candidate(
            AdaptiveWikiCandidateInput {
                claim: "Reject this candidate".to_string(),
                evidence_ref: Some("task:reject".to_string()),
                ..AdaptiveWikiCandidateInput::default()
            },
            now(),
        )?;

        let entry = store
            .promote_candidate_scoped(
                &promoted.id,
                AdaptiveWikiActivationMode::ContextOnly,
                Some(AdaptiveWikiScopeSuggestion {
                    scope: AdaptiveWikiScope::ArtifactKind,
                    scope_ref: "report".to_string(),
                }),
                now(),
            )?
            .expect("candidate promoted");

        assert_eq!(entry.scope, AdaptiveWikiScope::ArtifactKind);
        assert_eq!(entry.scope_ref, "report");
        assert_eq!(
            entry.activation_mode,
            AdaptiveWikiActivationMode::ContextOnly
        );
        assert_eq!(
            store.reject_candidate(&rejected.id)?.expect("rejected").id,
            rejected.id
        );
        assert!(store.load_candidates()?.candidates.is_empty());

        let rescoped = store
            .rescope_entry(&entry.id, AdaptiveWikiScope::Project, "project-b", now())?
            .expect("entry rescoped");
        assert_eq!(rescoped.scope, AdaptiveWikiScope::Project);
        assert_eq!(rescoped.scope_ref, "project-b");

        let with_counterexample = store
            .add_counterexample(&entry.id, "audit:counterexample", now())?
            .expect("counterexample added");
        assert_eq!(
            with_counterexample.counterexamples,
            vec!["audit:counterexample"]
        );

        let deprecated = store
            .deprecate_entry(&entry.id, now())?
            .expect("entry deprecated");
        assert_eq!(deprecated.status, AdaptiveWikiStatus::Deprecated);
        assert!(store
            .ai_projection(&AdaptiveWikiQuery {
                project_key: Some("project-b".to_string()),
                ..AdaptiveWikiQuery::default()
            })?
            .is_empty());

        store.append_audit(&AdaptiveWikiAuditRecord {
            id: "audit_one".to_string(),
            action: AdaptiveWikiAuditAction::Deprecate,
            subject_id: entry.id,
            candidate_id: None,
            entry_id: Some(deprecated.id),
            actor: "cli".to_string(),
            reason: "Superseded by newer rule".to_string(),
            evidence_ref: None,
            before_scope: None,
            after_scope: None,
            activation_mode: None,
            candidate_snapshot: None,
            entry_snapshot: None,
            created_at: now(),
        })?;
        let audit = fs::read_to_string(store.audit_path())?;
        assert!(audit.contains("\"action\":\"deprecate\""));
        assert!(audit.contains("\"reason\":\"Superseded by newer rule\""));
        Ok(())
    }

    #[test]
    fn edit_entry_and_add_tags_update_in_place() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        let candidate = store.record_candidate(
            AdaptiveWikiCandidateInput {
                kind: AdaptiveWikiKind::PolicyRule,
                scope: AdaptiveWikiScope::Project,
                scope_ref: "project-a".to_string(),
                claim: "A verbose original claim that should be compressed".to_string(),
                suggested_ai_instruction: "old instruction".to_string(),
                evidence_ref: Some("doc:one".to_string()),
                core_tags: vec!["project/project-a".to_string()],
                ..AdaptiveWikiCandidateInput::default()
            },
            now(),
        )?;
        let entry = store
            .promote_candidate(
                &candidate.id,
                AdaptiveWikiActivationMode::ContextOnly,
                now(),
            )?
            .expect("candidate promoted");

        // edit compresses the claim/instruction and adds an evidence ref in place
        let edited = store
            .edit_entry(
                &entry.id,
                AdaptiveWikiEntryEdit {
                    kind: Some(AdaptiveWikiKind::Procedure),
                    agent_modes: Some(vec![
                        AdaptiveWikiAgentMode::Development,
                        AdaptiveWikiAgentMode::Review,
                    ]),
                    claim: Some("Tight compressed claim.".to_string()),
                    ai_instruction: Some("Do the tight thing.".to_string()),
                    add_evidence_refs: vec!["doc:two".to_string()],
                    ..AdaptiveWikiEntryEdit::default()
                },
                now(),
            )?
            .expect("entry edited");
        assert_eq!(edited.claim, "Tight compressed claim.");
        assert_eq!(edited.ai_instruction, "Do the tight thing.");
        assert_eq!(edited.evidence_refs, vec!["doc:one", "doc:two"]);
        assert_eq!(edited.kind, AdaptiveWikiKind::Procedure);
        assert_eq!(
            edited.agent_modes,
            vec![
                AdaptiveWikiAgentMode::Development,
                AdaptiveWikiAgentMode::Review,
            ]
        );

        let shared = store
            .edit_entry(
                &entry.id,
                AdaptiveWikiEntryEdit {
                    agent_modes: Some(Vec::new()),
                    ..AdaptiveWikiEntryEdit::default()
                },
                now(),
            )?
            .expect("entry mode scope cleared");
        assert!(shared.agent_modes.is_empty());

        // add-tag appends controlled tags without touching other fields
        let retagged = store
            .add_entry_tags(
                &entry.id,
                &[
                    "facet/research".to_string(),
                    "project/project-a".to_string(),
                ],
                &["method/baseline-first".to_string()],
                now(),
            )?
            .expect("entry retagged");
        // existing project tag is not duplicated; new facet tag is added
        assert_eq!(
            retagged.core_tags,
            vec!["project/project-a", "facet/research"]
        );
        assert_eq!(retagged.proposed_tags, vec!["method/baseline-first"]);
        // edit is preserved after retag
        assert_eq!(retagged.claim, "Tight compressed claim.");

        // unknown entry id returns None rather than erroring
        assert!(store
            .edit_entry(
                "missing",
                AdaptiveWikiEntryEdit {
                    claim: Some("x".to_string()),
                    ..AdaptiveWikiEntryEdit::default()
                },
                now(),
            )?
            .is_none());
        Ok(())
    }

    #[test]
    fn runtime_projection_is_fenced_redacted_and_usage_records_roundtrip() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        let projection = build_ai_projection(
            &[AdaptiveWikiEntry {
                id: "wiki_entry_runtime".to_string(),
                ai_instruction: "Keep evidence separate token=sk-secretsecretsecretsecret"
                    .to_string(),
                ..promoted_entry("base", AdaptiveWikiScope::Project, "project-a", "fallback")
            }],
            &AdaptiveWikiQuery {
                project_key: Some("project-a".to_string()),
                ..AdaptiveWikiQuery::default()
            },
        );

        let runtime = build_runtime_projection(&projection).expect("runtime context");
        assert_eq!(runtime.entry_ids, vec!["wiki_entry_runtime"]);
        assert!(runtime.context.contains("<adaptive-wiki-context>"));
        assert!(runtime.context.contains("wiki_entry_runtime"));
        assert!(runtime.context.contains("[REDACTED]"));
        assert!(!runtime.context.contains("sk-secret"));

        let records = build_usage_records(
            &projection,
            "task",
            "request",
            "project-a",
            Some("report"),
            "runtime_probe",
            now(),
        );
        store.append_usage_records(&records)?;
        let loaded = store.load_usage_records()?;
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].entry_id, "wiki_entry_runtime");
        assert_eq!(loaded[0].artifact_kind.as_deref(), Some("report"));
        assert_eq!(loaded[0].projection_kind, "runtime_probe");
        Ok(())
    }

    #[test]
    fn legacy_candidate_json_loads_with_defaults() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        fs::write(
            store.candidates_path(),
            serde_json::to_string_pretty(&json!({
                "candidates": [
                    {
                        "id": "legacy_candidate",
                        "claim": "Legacy candidate"
                    }
                ]
            }))?,
        )?;

        let state = store.load_candidates()?;

        assert_eq!(state.version, ADAPTIVE_WIKI_VERSION);
        assert_eq!(
            state.candidates[0].signal_kind,
            AdaptiveWikiSignalKind::Unknown
        );
        assert_eq!(state.candidates[0].origin, AdaptiveWikiOrigin::Unknown);
        assert!(state.candidates[0].source_refs.is_empty());
        assert!(state.candidates[0].suggested_scope.is_none());
        assert_eq!(state.candidates[0].occurrence_count, 1);
        Ok(())
    }

    #[test]
    fn lint_flags_unsafe_or_incomplete_wiki_rows() {
        let report = build_lint_report(
            &[AdaptiveWikiEntry {
                evidence_refs: Vec::new(),
                counterexamples: vec!["audit:counterexample".to_string()],
                confidence: AdaptiveWikiConfidence::Inferred,
                review_after: Some(now()),
                ..promoted_entry(
                    "promoted_without_evidence",
                    AdaptiveWikiScope::Project,
                    "project-a",
                    "",
                )
            }],
            &[AdaptiveWikiCandidate {
                id: "candidate_without_source".to_string(),
                kind: AdaptiveWikiKind::Fact,
                scope: AdaptiveWikiScope::Project,
                scope_ref: "project-a".to_string(),
                agent_modes: Vec::new(),
                claim: "Needs source".to_string(),
                suggested_ai_instruction: String::new(),
                human_summary: String::new(),
                evidence_refs: Vec::new(),
                signal_kind: AdaptiveWikiSignalKind::Unknown,
                origin: AdaptiveWikiOrigin::Unknown,
                source_refs: Vec::new(),
                source_hashes: Vec::new(),
                core_tags: Vec::new(),
                proposed_tags: Vec::new(),
                suggested_scope: None,
                review_reason: String::new(),
                occurrence_count: 1,
                confidence: AdaptiveWikiConfidence::Inferred,
                created_at: now(),
                updated_at: now(),
                last_seen_at: now() - Duration::days(STALE_CANDIDATE_DAYS + 1),
            }],
            now(),
        );

        let codes: Vec<_> = report
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect();
        assert!(codes.contains(&"promoted_without_evidence"));
        assert!(codes.contains(&"review_expired"));
        assert!(codes.contains(&"promoted_low_confidence"));
        assert!(codes.contains(&"contested_entry"));
        assert!(codes.contains(&"candidate_without_source"));
        assert!(codes.contains(&"unknown_signal_kind"));
        assert!(codes.contains(&"stale_candidate"));
    }

    #[test]
    fn markdown_export_is_deterministic_sanitized_and_writes_vault() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path().join("profile"));
        let exported_at = now();
        store.save_entries(&AdaptiveWikiEntryState {
            version: ADAPTIVE_WIKI_VERSION.to_string(),
            entries: vec![AdaptiveWikiEntry {
                id: "wiki_project_entry".to_string(),
                kind: AdaptiveWikiKind::Procedure,
                scope: AdaptiveWikiScope::Project,
                scope_ref: "project-a".to_string(),
                status: AdaptiveWikiStatus::Promoted,
                activation_mode: AdaptiveWikiActivationMode::Confirm,
                agent_modes: Vec::new(),
                claim: "Keep evidence separate".to_string(),
                ai_instruction: "Keep evidence separate token=sk-secretsecretsecretsecret"
                    .to_string(),
                human_summary: "Human summary token=sk-secretsecretsecretsecret".to_string(),
                evidence_refs: vec!["task:one?token=sk-secretsecretsecretsecret".to_string()],
                counterexamples: vec!["audit:counterexample".to_string()],
                support_refs: Vec::new(),
                capability_ids: Vec::new(),
                required_artifact_kinds: Vec::new(),
                core_tags: vec!["project/project-a".to_string()],
                proposed_tags: vec!["method/evidence-separation".to_string()],
                confidence: AdaptiveWikiConfidence::Repeated,
                created_at: exported_at,
                updated_at: exported_at,
                review_after: Some(exported_at),
            }],
        })?;
        store.save_candidates(&AdaptiveWikiCandidateState {
            version: ADAPTIVE_WIKI_VERSION.to_string(),
            candidates: vec![AdaptiveWikiCandidate {
                id: "wiki_candidate_one".to_string(),
                kind: AdaptiveWikiKind::PolicyRule,
                scope: AdaptiveWikiScope::Project,
                scope_ref: "project-a".to_string(),
                agent_modes: Vec::new(),
                claim: "Ask before retry".to_string(),
                suggested_ai_instruction: "Ask before retry token=sk-secretsecretsecretsecret"
                    .to_string(),
                human_summary: "Candidate summary".to_string(),
                evidence_refs: vec!["approval:one".to_string()],
                signal_kind: AdaptiveWikiSignalKind::ApprovalDenial,
                origin: AdaptiveWikiOrigin::OperatorExplicit,
                source_refs: vec!["approval:one?token=sk-secretsecretsecretsecret".to_string()],
                source_hashes: vec!["sha256:abc".to_string()],
                core_tags: vec!["project/project-a".to_string()],
                proposed_tags: Vec::new(),
                suggested_scope: None,
                review_reason: "Review candidate".to_string(),
                occurrence_count: 2,
                confidence: AdaptiveWikiConfidence::Explicit,
                created_at: exported_at,
                updated_at: exported_at,
                last_seen_at: exported_at,
            }],
        })?;

        let output_dir = temp.path().join("vault");
        let dry_run = store.export_markdown(&output_dir, true, exported_at)?;
        assert_eq!(dry_run.summary.files_written, 0);
        assert_eq!(dry_run.summary.entries_exported, 1);
        assert_eq!(dry_run.summary.candidates_exported, 1);
        assert!(dry_run
            .files
            .iter()
            .any(|file| file.path == "entries/procedure/wiki-project-entry.md"));
        assert!(!output_dir.exists());

        let written = store.export_markdown(&output_dir, false, exported_at)?;
        assert_eq!(written.summary.files_planned, dry_run.summary.files_planned);
        assert_eq!(
            written
                .files
                .iter()
                .map(|file| &file.sha256)
                .collect::<Vec<_>>(),
            dry_run
                .files
                .iter()
                .map(|file| &file.sha256)
                .collect::<Vec<_>>()
        );
        let index = fs::read_to_string(output_dir.join("index.md"))?;
        assert!(index.contains("wiki_project_entry"));
        assert!(index.contains("true"));
        assert!(!index.contains("sk-secret"));
        let entry = fs::read_to_string(output_dir.join("entries/procedure/wiki-project-entry.md"))?;
        assert!(entry.contains("contested: true"));
        assert!(entry.contains("[REDACTED]"));
        assert!(!entry.contains("sk-secret"));
        assert!(output_dir.join("raw/audits").is_dir());
        Ok(())
    }

    #[test]
    fn procedure_runbook_refs_are_human_export_only() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path().join("profile"));
        let entry = promoted_entry(
            "wiki_procedure_entry",
            AdaptiveWikiScope::Project,
            "project-a",
            "Use the approved report runbook.",
        );
        store.save_entries(&AdaptiveWikiEntryState {
            version: ADAPTIVE_WIKI_VERSION.to_string(),
            entries: vec![entry.clone()],
        })?;

        let updated = store
            .update_runbook_refs(
                &entry.id,
                &["references/report-runbook.md?token=sk-secretsecretsecretsecret".to_string()],
                &["capability.syncback".to_string()],
                &["report".to_string()],
                now(),
            )?
            .expect("runbook refs updated");

        assert_eq!(updated.kind, AdaptiveWikiKind::Procedure);
        assert_eq!(updated.capability_ids, vec!["capability.syncback"]);
        assert_eq!(updated.required_artifact_kinds, vec!["report"]);

        let ai_projection = store.ai_projection(&AdaptiveWikiQuery {
            project_key: Some("project-a".to_string()),
            artifact_kind: Some("report".to_string()),
            ..AdaptiveWikiQuery::default()
        })?;
        let ai_json = serde_json::to_string(&ai_projection)?;
        assert_eq!(ai_projection.len(), 1);
        assert!(!ai_json.contains("support_refs"));
        assert!(!ai_json.contains("capability.syncback"));
        assert!(!ai_json.contains("report-runbook"));

        let human_projection = store.human_projection(&AdaptiveWikiQuery::default())?;
        assert!(human_projection.entries[0].support_refs[0].contains("[REDACTED]"));
        assert_eq!(
            human_projection.entries[0].capability_ids,
            vec!["capability.syncback"]
        );

        let lint = store.lint(now())?;
        let codes: Vec<_> = lint
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect();
        assert!(!codes.contains(&"procedure_without_runbook_links"));
        assert!(!codes.contains(&"procedure_artifact_without_capability"));

        let output_dir = temp.path().join("vault");
        store.export_markdown(&output_dir, false, now())?;
        let page =
            fs::read_to_string(output_dir.join("entries/procedure/wiki-procedure-entry.md"))?;
        assert!(page.contains("## Runbook Support"));
        assert!(page.contains("capability.syncback"));
        assert!(page.contains("report"));
        assert!(page.contains("[REDACTED]"));
        assert!(!page.contains("sk-secret"));
        assert!(output_dir.join("support/references").is_dir());
        assert!(output_dir.join("support/templates").is_dir());
        assert!(output_dir.join("support/scripts").is_dir());
        Ok(())
    }

    #[test]
    fn review_report_is_recommendation_only_redacted_and_writes_report_files() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path().join("profile"));
        let reviewed_at = now();
        let secret = "sk-secretsecretsecretsecret";
        store.save_entries(&AdaptiveWikiEntryState {
            version: ADAPTIVE_WIKI_VERSION.to_string(),
            entries: vec![AdaptiveWikiEntry {
                id: "wiki_entry_review".to_string(),
                kind: AdaptiveWikiKind::PolicyRule,
                scope: AdaptiveWikiScope::Project,
                scope_ref: "project-a".to_string(),
                status: AdaptiveWikiStatus::Promoted,
                activation_mode: AdaptiveWikiActivationMode::Confirm,
                agent_modes: Vec::new(),
                claim: "Review this policy".to_string(),
                ai_instruction: "Review this policy".to_string(),
                human_summary: "Needs periodic review".to_string(),
                evidence_refs: Vec::new(),
                counterexamples: vec![format!("audit:counterexample?token={secret}")],
                support_refs: Vec::new(),
                capability_ids: Vec::new(),
                required_artifact_kinds: Vec::new(),
                core_tags: vec!["project/project-a".to_string()],
                proposed_tags: vec!["status/review".to_string()],
                confidence: AdaptiveWikiConfidence::Inferred,
                created_at: reviewed_at,
                updated_at: reviewed_at,
                review_after: Some(reviewed_at),
            }],
        })?;
        store.save_candidates(&AdaptiveWikiCandidateState {
            version: ADAPTIVE_WIKI_VERSION.to_string(),
            candidates: vec![AdaptiveWikiCandidate {
                id: "wiki_candidate_review".to_string(),
                kind: AdaptiveWikiKind::Procedure,
                scope: AdaptiveWikiScope::Project,
                scope_ref: "project-a".to_string(),
                agent_modes: Vec::new(),
                claim: "Promote repeated candidate".to_string(),
                suggested_ai_instruction: "Use reviewed flow".to_string(),
                human_summary: "Repeated review candidate".to_string(),
                evidence_refs: vec![format!("task:one?token={secret}")],
                signal_kind: AdaptiveWikiSignalKind::OperatorCorrection,
                origin: AdaptiveWikiOrigin::OperatorExplicit,
                source_refs: vec!["task:one".to_string()],
                source_hashes: Vec::new(),
                core_tags: vec!["project/project-a".to_string()],
                proposed_tags: vec!["method/repeated-evidence".to_string()],
                suggested_scope: None,
                review_reason: "Repeated evidence".to_string(),
                occurrence_count: 2,
                confidence: AdaptiveWikiConfidence::Repeated,
                created_at: reviewed_at,
                updated_at: reviewed_at,
                last_seen_at: reviewed_at,
            }],
        })?;
        let entries_before = fs::read_to_string(store.entries_path())?;
        let candidates_before = fs::read_to_string(store.candidates_path())?;

        let dry_run = store.generate_review_report(true, reviewed_at)?;
        assert_eq!(dry_run.summary.files_written, 0);
        assert!(dry_run.summary.proposals >= 4);
        assert!(dry_run
            .proposals
            .iter()
            .any(|proposal| proposal.action == AdaptiveWikiReviewProposalAction::Promote));
        assert!(dry_run
            .proposals
            .iter()
            .any(|proposal| proposal.action == AdaptiveWikiReviewProposalAction::Split));
        assert!(!serde_json::to_string(&dry_run)?.contains(secret));
        assert_eq!(fs::read_to_string(store.entries_path())?, entries_before);
        assert_eq!(
            fs::read_to_string(store.candidates_path())?,
            candidates_before
        );
        assert!(!temp
            .path()
            .join("profile")
            .join(ADAPTIVE_WIKI_REVIEW_REPORTS_DIR)
            .exists());

        let written = store.generate_review_report(false, reviewed_at)?;
        assert_eq!(written.summary.files_written, 2);
        let report_dir = std::path::PathBuf::from(&written.report_dir);
        assert!(report_dir.starts_with(
            temp.path()
                .join("profile")
                .join(ADAPTIVE_WIKI_REVIEW_REPORTS_DIR)
        ));
        assert!(report_dir.join("report.json").is_file());
        assert!(report_dir.join("REPORT.md").is_file());
        let report_md = fs::read_to_string(report_dir.join("REPORT.md"))?;
        assert!(report_md.contains("Adaptive Wiki Review Report"));
        assert!(report_md.contains("wiki_candidate_review"));
        assert!(report_md.contains("[REDACTED]"));
        assert!(!report_md.contains(secret));
        assert_eq!(fs::read_to_string(store.entries_path())?, entries_before);
        assert_eq!(
            fs::read_to_string(store.candidates_path())?,
            candidates_before
        );
        Ok(())
    }

    #[test]
    fn review_report_uses_evidence_graph_without_mutation() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path().join("profile"));
        let reviewed_at = now();
        let promoted_at = reviewed_at - Duration::hours(2);
        let after_promotion = reviewed_at + Duration::hours(1);
        let secret = "sk-secretsecretsecretsecret";
        let mut recurring_entry = promoted_entry(
            "wiki_entry_recurring",
            AdaptiveWikiScope::Project,
            "project-a",
            "Use the promoted flow.",
        );
        recurring_entry.created_at = promoted_at;
        recurring_entry.updated_at = promoted_at;
        let mut chain_entry = promoted_entry(
            "wiki_entry_chain",
            AdaptiveWikiScope::Project,
            "project-b",
            "Use the promotion chain flow.",
        );
        chain_entry.created_at = promoted_at;
        chain_entry.updated_at = promoted_at;
        store.save_entries(&AdaptiveWikiEntryState {
            version: ADAPTIVE_WIKI_VERSION.to_string(),
            entries: vec![recurring_entry.clone(), chain_entry.clone()],
        })?;
        store.save_candidates(&AdaptiveWikiCandidateState {
            version: ADAPTIVE_WIKI_VERSION.to_string(),
            candidates: Vec::new(),
        })?;
        store.append_correction_record(&AdaptiveWikiCorrectionRecord {
            id: "wiki_corr_recurring".to_string(),
            correction_kind: AdaptiveWikiCorrectionKind::OperatorCorrection,
            candidate_id: None,
            entry_id: Some(recurring_entry.id.clone()),
            task_id: Some("task_recurring".to_string()),
            request_id: Some("request_recurring".to_string()),
            project_key: Some("project-a".to_string()),
            artifact_kind: Some("report".to_string()),
            summary: format!("Correction after promotion token={secret}"),
            evidence_refs: vec![format!("task:task_recurring?token={secret}")],
            source_refs: vec!["request:request_recurring".to_string()],
            created_at: after_promotion,
        })?;
        store.append_usage_records(&[AdaptiveWikiUsageRecord {
            id: "wiki_usage_chain".to_string(),
            entry_id: chain_entry.id.clone(),
            task_id: "task_chain".to_string(),
            request_id: "request_chain".to_string(),
            project_key: "project-b".to_string(),
            artifact_kind: Some("report".to_string()),
            agent_mode: None,
            projection_kind: "runtime_probe".to_string(),
            projection_policy: None,
            activation_mode: AdaptiveWikiActivationMode::Confirm,
            created_at: after_promotion,
        }])?;

        let entries_before = fs::read_to_string(store.entries_path())?;
        let candidates_before = fs::read_to_string(store.candidates_path())?;
        let corrections_before = fs::read_to_string(store.corrections_path())?;
        let usage_before = fs::read_to_string(store.usage_path())?;

        let report = store.generate_review_report(true, reviewed_at)?;

        assert_eq!(report.summary.files_written, 0);
        assert_eq!(report.summary.correction_records_checked, 1);
        assert!(report.proposals.iter().any(|proposal| {
            proposal.action == AdaptiveWikiReviewProposalAction::Rescope
                && proposal.subject_id == recurring_entry.id.as_str()
                && proposal
                    .evidence_refs
                    .iter()
                    .any(|value| value == "correction:wiki_corr_recurring")
        }));
        assert!(report.proposals.iter().any(|proposal| {
            proposal.action == AdaptiveWikiReviewProposalAction::RenewReview
                && proposal.subject_id == chain_entry.id.as_str()
                && proposal.title == "Review incomplete promotion evidence chain"
                && proposal.risk == AdaptiveWikiReviewRisk::High
                && proposal
                    .evidence_refs
                    .iter()
                    .any(|value| value == "audit:missing_promotion_audit")
        }));
        assert!(!serde_json::to_string(&report)?.contains(secret));
        assert!(!temp
            .path()
            .join("profile")
            .join(ADAPTIVE_WIKI_REVIEW_REPORTS_DIR)
            .exists());
        assert_eq!(fs::read_to_string(store.entries_path())?, entries_before);
        assert_eq!(
            fs::read_to_string(store.candidates_path())?,
            candidates_before
        );
        assert_eq!(
            fs::read_to_string(store.corrections_path())?,
            corrections_before
        );
        assert_eq!(fs::read_to_string(store.usage_path())?, usage_before);
        Ok(())
    }

    #[test]
    fn episode_evaluation_checks_scope_stale_status_and_evidence() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path().join("profile"));
        let evaluated_at = now();
        store.save_entries(&AdaptiveWikiEntryState {
            version: ADAPTIVE_WIKI_VERSION.to_string(),
            entries: vec![
                promoted_entry(
                    "wiki_entry_project",
                    AdaptiveWikiScope::Project,
                    "project-a",
                    "Use the project-specific runbook.",
                ),
                AdaptiveWikiEntry {
                    id: "wiki_entry_deprecated".to_string(),
                    status: AdaptiveWikiStatus::Deprecated,
                    ..promoted_entry(
                        "deprecated_base",
                        AdaptiveWikiScope::Project,
                        "project-a",
                        "Do not project deprecated entries.",
                    )
                },
                AdaptiveWikiEntry {
                    id: "wiki_entry_review_expired".to_string(),
                    review_after: Some(evaluated_at),
                    ..promoted_entry(
                        "review_expired_base",
                        AdaptiveWikiScope::Project,
                        "project-a",
                        "Expired review entry should be reported.",
                    )
                },
                AdaptiveWikiEntry {
                    id: "wiki_entry_no_evidence".to_string(),
                    evidence_refs: Vec::new(),
                    ..promoted_entry(
                        "no_evidence_base",
                        AdaptiveWikiScope::Project,
                        "project-a",
                        "Missing evidence should be reported.",
                    )
                },
                promoted_entry(
                    "wiki_entry_other_project",
                    AdaptiveWikiScope::Project,
                    "project-b",
                    "Other project only.",
                ),
            ],
        })?;

        let dry_run = store.generate_episode_evaluation_report(
            "wiki_entry_project",
            AdaptiveWikiQuery {
                project_key: Some("project-a".to_string()),
                ..AdaptiveWikiQuery::default()
            },
            AdaptiveWikiQuery {
                project_key: Some("project-b".to_string()),
                ..AdaptiveWikiQuery::default()
            },
            true,
            evaluated_at,
        )?;
        assert_eq!(dry_run.summary.files_written, 0);
        assert!(dry_run.summary.target_entry_in_scope);
        assert!(!dry_run.summary.target_entry_out_of_scope);
        assert!(!dry_run.summary.deprecated_entry_projected);
        assert!(dry_run.summary.review_expired_entry_projected);
        assert_eq!(
            dry_run.projected_without_evidence_entry_ids,
            vec!["wiki_entry_no_evidence"]
        );
        assert_eq!(
            dry_run.review_expired_projected_entry_ids,
            vec!["wiki_entry_review_expired"]
        );
        assert!(!dry_run.passed);
        assert!(!temp
            .path()
            .join("profile")
            .join(ADAPTIVE_WIKI_EPISODE_REPORTS_DIR)
            .exists());

        let written = store.generate_episode_evaluation_report(
            "wiki_entry_project",
            AdaptiveWikiQuery {
                project_key: Some("project-a".to_string()),
                ..AdaptiveWikiQuery::default()
            },
            AdaptiveWikiQuery {
                project_key: Some("project-b".to_string()),
                ..AdaptiveWikiQuery::default()
            },
            false,
            evaluated_at,
        )?;
        assert_eq!(written.summary.files_written, 2);
        let report_dir = std::path::PathBuf::from(&written.report_dir);
        assert!(report_dir.starts_with(
            temp.path()
                .join("profile")
                .join(ADAPTIVE_WIKI_EPISODE_REPORTS_DIR)
        ));
        assert!(report_dir.join("episode.json").is_file());
        assert!(report_dir.join("EPISODE.md").is_file());
        let report_md = fs::read_to_string(report_dir.join("EPISODE.md"))?;
        assert!(report_md.contains("Adaptive Wiki Episode Evaluation"));
        assert!(report_md.contains("wiki_entry_project"));
        assert!(report_md.contains("review-expired entries were projected"));
        Ok(())
    }

    #[test]
    fn legacy_entry_json_loads_with_defaults() -> Result<()> {
        let temp = tempdir()?;
        let store = AdaptiveWikiStore::new(temp.path());
        fs::write(
            store.entries_path(),
            serde_json::to_string_pretty(&json!({
                "entries": [
                    {
                        "id": "legacy",
                        "claim": "Stable fact"
                    }
                ]
            }))?,
        )?;

        let state = store.load_entries()?;

        assert_eq!(state.version, ADAPTIVE_WIKI_VERSION);
        assert_eq!(state.entries[0].kind, AdaptiveWikiKind::Fact);
        assert_eq!(state.entries[0].scope, AdaptiveWikiScope::UserGlobal);
        assert_eq!(state.entries[0].scope_ref, "*");
        assert_eq!(state.entries[0].status, AdaptiveWikiStatus::Candidate);
        assert_eq!(
            state.entries[0].activation_mode,
            AdaptiveWikiActivationMode::Confirm
        );
        Ok(())
    }
}
