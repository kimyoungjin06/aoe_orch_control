//! Telegram decision-ingest command adapter.
//!
//! This module parses relay artifacts, promotes freeform feedback into a
//! review-only decision record, and appends confirmed structured decision
//! transitions to the canonical ledger. Freeform Telegram input never
//! authorizes runtime mutation, background dispatch, approval resolution, or
//! other execution by itself.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use clap::Args;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::offdesk::{
    normalize_decision_choice, operator_safe_text,
    receipt_decision_record as transition_receipt_decision_record,
    resolve_decision_record as transition_resolve_decision_record, ApprovalBrief,
    ApprovalBriefOption, DecisionLedger, DecisionMateriality, DecisionOption, DecisionRaisedBy,
    DecisionReceiptInput, DecisionRecord, DecisionRequest, DecisionResolutionInput, DecisionRoute,
    DecisionRouteTarget, DecisionStatus, DecisionTraceRef, DecisionValidationIssue,
    JudgmentEvaluator, JudgmentRoute, DECISION_RECORD_SCHEMA, JUDGMENT_ROUTE_SCHEMA,
};
use crate::session::get_profile_dir;

#[derive(Args)]
pub struct DecisionIngestTelegramArgs {
    /// Operator-safe decision request JSON containing decision_record
    #[arg(long)]
    request: PathBuf,

    /// Telegram relay result JSON
    #[arg(long)]
    result: PathBuf,

    /// Override canonical profile directory for producer integrations
    #[arg(long = "profile-dir")]
    profile_dir: Option<PathBuf>,

    /// Actor recording the relay ingestion
    #[arg(long, default_value = "telegram")]
    by: String,

    /// Override execution handoff target
    #[arg(long)]
    target: Option<String>,

    /// Also append a receipt with this result status after resolving
    #[arg(long = "receipt-result-status")]
    receipt_result_status: Option<String>,

    /// Receipt evidence summary line. Repeat for multiple lines.
    #[arg(long = "receipt-evidence")]
    receipt_evidence_summary: Vec<String>,

    /// Remaining review item. Repeat for multiple lines.
    #[arg(long = "remaining-review")]
    remaining_review: Vec<String>,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Args)]
pub struct DecisionIngestTelegramFeedbackArgs {
    /// Telegram feedback JSON or JSONL file
    #[arg(long)]
    feedback: PathBuf,

    /// Override canonical profile directory for producer integrations
    #[arg(long = "profile-dir")]
    profile_dir: Option<PathBuf>,

    /// Actor recording the inbox item
    #[arg(long, default_value = "telegram")]
    by: String,

    /// Output as JSON
    #[arg(long)]
    json: bool,
}

#[derive(Debug)]
struct TelegramDecisionResult {
    status: String,
    decision: Option<String>,
    reason: String,
}

#[derive(Debug, Serialize)]
struct DecisionIngestTelegramReport {
    request_path: String,
    result_path: String,
    ledger_path: String,
    decision_id: String,
    telegram_status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    telegram_decision: Option<String>,
    appended_records: Vec<String>,
    receipt_recorded: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    skipped_reason: Option<String>,
    record: DecisionRecord,
    validation_issues: Vec<DecisionValidationIssue>,
}

#[derive(Debug, Serialize)]
struct DecisionIngestTelegramFeedbackReport {
    feedback_path: String,
    ledger_path: String,
    decision_id: String,
    appended: bool,
    record: DecisionRecord,
    validation_issues: Vec<DecisionValidationIssue>,
}

pub(super) fn ingest_telegram_decision(
    profile: &str,
    args: DecisionIngestTelegramArgs,
) -> Result<()> {
    let profile_dir = match args.profile_dir.as_ref() {
        Some(path) => path.to_path_buf(),
        None => get_profile_dir(profile)?,
    };
    let ledger = DecisionLedger::new(&profile_dir);
    let request = read_json_file(&args.request)?;
    let result = parse_telegram_decision_result(&read_json_file(&args.result)?);
    let seed_record = decision_record_from_request(&request, &args.request)?;
    let decision_id = seed_record.decision_id.clone();
    let mut appended_records = Vec::new();
    let mut record = if let Some(existing) = ledger.find(&decision_id)? {
        existing
    } else {
        ledger.append(&seed_record)?;
        appended_records.push(seed_record.status.as_str().to_string());
        seed_record
    };

    let mut receipt_recorded = false;
    let mut skipped_reason = None;

    if result.status == "accepted" {
        let Some(decision) = result.decision.clone() else {
            bail!("accepted Telegram result is missing decision");
        };
        if record.status == DecisionStatus::Receipted {
            skipped_reason = Some("decision_already_receipted".to_string());
        } else if !decision_record_has_matching_handoff(&record, &decision) {
            record = transition_resolve_decision_record(
                record,
                &DecisionResolutionInput {
                    decision,
                    note: result.reason.clone(),
                    by: args.by.clone(),
                    target: args.target.clone(),
                },
            )?;
            ledger.append(&record)?;
            appended_records.push(record.status.as_str().to_string());
        }

        if let Some(result_status) = args
            .receipt_result_status
            .as_deref()
            .map(str::trim)
            .filter(|status| !status.is_empty())
        {
            if record.status == DecisionStatus::Receipted {
                receipt_recorded = true;
            } else {
                record = transition_receipt_decision_record(
                    record,
                    &DecisionReceiptInput {
                        by: args.by.clone(),
                        result_status: result_status.to_string(),
                        evidence_summary: args.receipt_evidence_summary.clone(),
                        remaining_review: args.remaining_review.clone(),
                    },
                )?;
                ledger.append(&record)?;
                appended_records.push(record.status.as_str().to_string());
                receipt_recorded = true;
            }
        }
    } else {
        skipped_reason = Some(format!(
            "telegram_result_status_{}",
            if result.status.is_empty() {
                "missing"
            } else {
                result.status.as_str()
            }
        ));
    }

    let report = DecisionIngestTelegramReport {
        request_path: args.request.display().to_string(),
        result_path: args.result.display().to_string(),
        ledger_path: ledger.path().display().to_string(),
        decision_id,
        telegram_status: result.status,
        telegram_decision: result.decision,
        appended_records,
        receipt_recorded,
        skipped_reason,
        validation_issues: record.validation_issues(),
        record,
    };

    present_telegram_decision_report(&report, args.json)
}

pub(super) fn ingest_telegram_feedback(
    profile: &str,
    args: DecisionIngestTelegramFeedbackArgs,
) -> Result<()> {
    let profile_dir = match args.profile_dir.as_ref() {
        Some(path) => path.to_path_buf(),
        None => get_profile_dir(profile)?,
    };
    let ledger = DecisionLedger::new(&profile_dir);
    let feedback = read_json_or_latest_jsonl_file(&args.feedback)?;
    let seed_record = decision_record_from_telegram_feedback(&feedback, &args.feedback, &args.by)?;
    let decision_id = seed_record.decision_id.clone();

    let (record, appended) = if let Some(existing) = ledger.find(&decision_id)? {
        (existing, false)
    } else {
        ledger.append(&seed_record)?;
        (seed_record, true)
    };

    let report = DecisionIngestTelegramFeedbackReport {
        feedback_path: args.feedback.display().to_string(),
        ledger_path: ledger.path().display().to_string(),
        decision_id,
        appended,
        validation_issues: record.validation_issues(),
        record,
    };

    present_telegram_feedback_report(&report, args.json)
}

fn read_json_file(path: &Path) -> Result<Value> {
    serde_json::from_str(
        &fs::read_to_string(path).with_context(|| format!("read JSON {}", path.display()))?,
    )
    .with_context(|| format!("parse JSON {}", path.display()))
}

fn read_json_or_latest_jsonl_file(path: &Path) -> Result<Value> {
    let content =
        fs::read_to_string(path).with_context(|| format!("read JSON {}", path.display()))?;
    let trimmed = content.trim();
    if trimmed.is_empty() {
        bail!("JSON file is empty: {}", path.display());
    }
    match serde_json::from_str(trimmed) {
        Ok(value) => Ok(value),
        Err(full_error) => {
            let Some(line) = content
                .lines()
                .rev()
                .map(str::trim)
                .find(|line| !line.is_empty())
            else {
                bail!("JSON file is empty: {}", path.display());
            };
            if line == trimmed {
                Err(full_error).with_context(|| format!("parse JSON {}", path.display()))
            } else {
                serde_json::from_str(line)
                    .with_context(|| format!("parse latest JSONL row {}", path.display()))
            }
        }
    }
}

fn decision_record_from_request(request: &Value, request_path: &Path) -> Result<DecisionRecord> {
    let Some(record) = request.get("decision_record").cloned() else {
        bail!(
            "Telegram request {} does not contain decision_record",
            request_path.display()
        );
    };
    serde_json::from_value(record).with_context(|| {
        format!(
            "parse decision_record from Telegram request {}",
            request_path.display()
        )
    })
}

fn parse_telegram_decision_result(result: &Value) -> TelegramDecisionResult {
    TelegramDecisionResult {
        status: json_string_field(result, "status").unwrap_or_default(),
        decision: json_string_field(result, "decision"),
        reason: json_string_field(result, "reason").unwrap_or_default(),
    }
}

fn decision_record_from_telegram_feedback(
    feedback: &Value,
    feedback_path: &Path,
    by: &str,
) -> Result<DecisionRecord> {
    let schema = json_string_field(feedback, "schema").unwrap_or_default();
    if schema != "remote_operator_telegram_feedback.v1" {
        bail!(
            "Telegram feedback {} has unsupported schema `{}`",
            feedback_path.display(),
            if schema.is_empty() {
                "missing"
            } else {
                schema.as_str()
            }
        );
    }

    let id_material = serde_json::json!({
        "schema": feedback.get("schema"),
        "profile": feedback.get("profile"),
        "chat_id_hash": feedback.get("chat_id_hash"),
        "user_id_hash": feedback.get("user_id_hash"),
        "message_id": feedback.get("message_id"),
        "feedback_text": feedback.get("feedback_text"),
        "target_chat_id_hash": feedback.get("target_chat_id_hash"),
        "feedback_context": feedback.get("feedback_context"),
    });
    let canonical_feedback =
        serde_json::to_vec(&id_material).context("serialize Telegram feedback for decision id")?;
    let feedback_hash = sha256_hex(&canonical_feedback);
    let hash_prefix = &feedback_hash[..16];
    let decision_id = format!("telegram-feedback-{hash_prefix}");
    let received_at = json_string_field(feedback, "received_at")
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);
    let actor = safe_nonempty(by).unwrap_or_else(|| "telegram".to_string());
    let feedback_text = safe_nonempty(
        json_string_field(feedback, "feedback_text")
            .as_deref()
            .unwrap_or(""),
    )
    .unwrap_or_else(|| "(empty feedback)".to_string());
    let feedback_kind = json_string_field(feedback, "feedback_kind")
        .and_then(|value| safe_nonempty(&value))
        .unwrap_or_else(|| classify_telegram_feedback_kind(&feedback_text).to_string());
    let is_planning_request = feedback_kind == "planning_request";
    let feedback_excerpt = truncate_chars(&feedback_text, 240);
    let project_key = feedback_context_string(feedback, "project_key")
        .or_else(|| json_string_field(feedback, "profile").and_then(|value| safe_nonempty(&value)))
        .unwrap_or_else(|| "remote-operator-feedback".to_string());
    let message_id = feedback_message_id(feedback);
    let request_id = feedback_context_string(feedback, "request_id")
        .or_else(|| {
            message_id
                .as_ref()
                .map(|id| format!("telegram-message-{id}"))
        })
        .unwrap_or_else(|| format!("telegram-feedback-{hash_prefix}"));
    let task_id = feedback_context_string(feedback, "task_id")
        .or_else(|| feedback_context_string(feedback, "focus_ref"))
        .unwrap_or_else(|| {
            if is_planning_request {
                "telegram-plan-request".to_string()
            } else {
                "telegram-feedback".to_string()
            }
        });
    let focus_kind = feedback_context_string(feedback, "focus_kind");
    let context_kind = feedback_context_string(feedback, "context_kind");
    let focus_ref = feedback_context_string(feedback, "focus_ref");
    let context_label = feedback_context_string(feedback, "focus_label")
        .or_else(|| focus_ref.clone())
        .or_else(|| context_kind.clone());
    let materiality = if is_planning_request {
        DecisionMateriality::Medium
    } else {
        feedback_materiality(context_kind.as_deref(), focus_kind.as_deref())
    };

    let mut evidence_refs = vec![DecisionTraceRef {
        kind: "telegram_feedback".to_string(),
        label: "feedback_file".to_string(),
        reference: feedback_path.display().to_string(),
    }];
    if let Some(id) = message_id.as_deref() {
        evidence_refs.push(DecisionTraceRef {
            kind: "telegram_message".to_string(),
            label: "message_id".to_string(),
            reference: id.to_string(),
        });
    }
    if let Some(focus) = focus_ref.as_deref() {
        evidence_refs.push(DecisionTraceRef {
            kind: "telegram_context".to_string(),
            label: focus_kind.clone().unwrap_or_else(|| "focus".to_string()),
            reference: focus.to_string(),
        });
    }

    let mut why_now = vec![
        if is_planning_request {
            "The remote operator sent a Telegram planning request.".to_string()
        } else {
            "The remote operator sent freeform Telegram feedback.".to_string()
        },
        if is_planning_request {
            "Telegram planning requests are captured for Plan Mode review; they do not start autonomous work by themselves.".to_string()
        } else {
            "Freeform feedback is review input only; it does not authorize runtime mutation or approval resolution.".to_string()
        },
    ];
    if let Some(label) = context_label.as_deref() {
        why_now.push(format!("Referenced context: {label}."));
    }

    let non_authorized_scope = vec![
        "runtime mutation".to_string(),
        "approval resolution".to_string(),
        "background dispatch".to_string(),
        "provider retargeting".to_string(),
        "cleanup or deletion".to_string(),
        "git commit or push".to_string(),
    ];

    let options = vec![
        DecisionOption {
            id: if is_planning_request {
                "plan".to_string()
            } else {
                "revise".to_string()
            },
            label: if is_planning_request {
                "Create plan candidate".to_string()
            } else {
                "Revise next step".to_string()
            },
            description: if is_planning_request {
                "Turn this Telegram request into a bounded Offdesk planning candidate for local review."
                    .to_string()
            } else {
                "Use this feedback to revise the referenced plan, approval review, or handoff direction."
                    .to_string()
            },
            impact: Some(if is_planning_request {
                "Creates a handoff-ready decision for plan drafting; execution still needs normal approval gates."
                        .to_string()
            } else {
                "Creates a handoff-ready decision that still needs an explicit receipt after review."
                        .to_string()
            }),
            natural_input_prompt: Some(if is_planning_request {
                "Describe the project, goal, timebox, and constraints for the plan candidate."
                    .to_string()
            } else {
                "Describe the bounded revision to make.".to_string()
            }),
        },
        DecisionOption {
            id: "defer".to_string(),
            label: "Keep open".to_string(),
            description: "Leave the feedback in the decision inbox for later review.".to_string(),
            impact: Some("No runtime or plan state changes are authorized.".to_string()),
            natural_input_prompt: Some("State what evidence or timing is missing.".to_string()),
        },
        DecisionOption {
            id: "deny".to_string(),
            label: "Not actionable".to_string(),
            description: "Close the feedback as reviewed but not actionable.".to_string(),
            impact: Some("The inbox item is denied without an execution handoff.".to_string()),
            natural_input_prompt: Some(
                "State why the feedback does not change the current direction.".to_string(),
            ),
        },
    ];
    let approval_options = options
        .iter()
        .map(|option| ApprovalBriefOption {
            id: option.id.clone(),
            label: option.label.clone(),
            description: option.description.clone(),
            natural_input_prompt: option.natural_input_prompt.clone(),
        })
        .collect::<Vec<_>>();
    let mut decision_impacts = HashMap::new();
    decision_impacts.insert(
        if is_planning_request {
            "plan".to_string()
        } else {
            "revise".to_string()
        },
        if is_planning_request {
            "Reviewers may create a bounded plan candidate; execution still needs normal plan review, launch prep, and gate approval.".to_string()
        } else {
            "Reviewers may revise the bounded plan or handoff direction; execution still needs the normal handoff and receipt.".to_string()
        },
    );
    decision_impacts.insert(
        "defer".to_string(),
        "The feedback remains visible in the decision inbox with no state mutation.".to_string(),
    );
    decision_impacts.insert(
        "deny".to_string(),
        "The feedback is marked reviewed and not actionable.".to_string(),
    );
    let mut approval_context = HashMap::new();
    if let Some(value) = context_kind.as_deref() {
        approval_context.insert("context_kind".to_string(), value.to_string());
    }
    if let Some(value) = focus_kind.as_deref() {
        approval_context.insert("focus_kind".to_string(), value.to_string());
    }
    if let Some(value) = focus_ref.as_deref() {
        approval_context.insert("focus_ref".to_string(), value.to_string());
    }

    // Decision lists must show the feedback itself, not the interaction label
    // that happened to be active when the message was sent.
    let subject_excerpt = truncate_chars(&feedback_text, 60);
    let subject = if is_planning_request {
        format!("Telegram planning request: {subject_excerpt}")
    } else {
        format!("Telegram feedback: {subject_excerpt}")
    };
    let current_scope = if is_planning_request {
        "Review this Telegram planning request and, if appropriate, turn it into a bounded Offdesk plan candidate. This decision does not execute work by itself.".to_string()
    } else {
        "Review and classify this feedback for the referenced Offdesk context only. This decision does not execute work by itself.".to_string()
    };
    let source_surface = if is_planning_request {
        "telegram.remote_operator.plan_request"
    } else {
        "telegram.remote_operator.feedback"
    };

    Ok(DecisionRecord {
        schema: DECISION_RECORD_SCHEMA.to_string(),
        decision_id,
        project_key,
        request_id,
        task_id,
        raised_by: DecisionRaisedBy::Operator,
        source_surface: source_surface.to_string(),
        materiality,
        status: DecisionStatus::UserPending,
        created_at: received_at,
        updated_at: received_at,
        decision_request: DecisionRequest {
            kind: if is_planning_request {
                "telegram_operator_plan_request".to_string()
            } else {
                "telegram_operator_feedback".to_string()
            },
            summary: if is_planning_request {
                format!("Telegram planning request: {feedback_excerpt}")
            } else {
                format!("Telegram feedback: {feedback_excerpt}")
            },
            decision_needed: if is_planning_request {
                "Decide whether to create a bounded Offdesk plan candidate from this Telegram request."
                    .to_string()
            } else {
                "Decide whether the feedback changes the referenced plan, approval review, or next Offdesk handoff."
                    .to_string()
            },
            why_now,
            current_scope: current_scope.clone(),
            non_authorized_scope: non_authorized_scope.clone(),
            options,
            evidence_refs: evidence_refs.clone(),
            trace_refs: evidence_refs.clone(),
        },
        council_review: None,
        judgment_route: Some(JudgmentRoute {
            schema: JUDGMENT_ROUTE_SCHEMA.to_string(),
            evaluator: JudgmentEvaluator::DeterministicGate,
            reason: if is_planning_request {
                "Telegram planning text is captured as a planning request, not as runtime authority."
            } else {
                "Telegram freeform text is operator feedback, so the adapter may only promote it into a reviewable decision inbox item."
            }
            .to_string(),
            policy_basis: vec![
                "Remote operator transport is read-only.".to_string(),
                if is_planning_request {
                    "Telegram planning requests require local Plan Mode review before any work starts."
                        .to_string()
                } else {
                    "Freeform Telegram text is not an approval or execution command.".to_string()
                },
            ],
            evidence_refs: evidence_refs.clone(),
            selected_by: actor.clone(),
            selected_at: received_at,
            default_if_no_reply: Some("defer".to_string()),
        }),
        route: Some(DecisionRoute {
            materiality,
            target: DecisionRouteTarget::User,
            reason: if is_planning_request {
                "Human review is required before a Telegram planning request becomes a plan candidate."
            } else {
                "Human review is required before feedback can change a plan, approval, or workload direction."
            }
            .to_string(),
            policy_basis: vec![
                if is_planning_request {
                    "Planning requests are captured as intent, not authority.".to_string()
                } else {
                    "Feedback is captured as input, not authority.".to_string()
                },
                "Existing decision resolve/receipt commands must close the loop.".to_string(),
            ],
            default_if_no_reply: Some("defer".to_string()),
            expires_at: None,
        }),
        approval_brief: Some(ApprovalBrief {
            schema: "approval_brief.v1".to_string(),
            source: Some(source_surface.to_string()),
            recommendation: if is_planning_request {
                "plan".to_string()
            } else {
                "revise".to_string()
            },
            subject,
            summary_lines: vec![
                if is_planning_request {
                    format!("Planning request: {feedback_excerpt}")
                } else {
                    format!("Feedback: {feedback_excerpt}")
                },
                if is_planning_request {
                    "This request was captured for plan drafting only; no work has started."
                        .to_string()
                } else {
                    "This message was promoted to the decision inbox for review only.".to_string()
                },
            ],
            judgment_route_summary: Some(
                if is_planning_request {
                    "판단 경로: Telegram planning request - deterministic promotion to planning inbox, no runtime authority.".to_string()
                } else {
                    "판단 경로: Telegram freeform feedback - deterministic promotion to review inbox, no runtime authority.".to_string()
                },
            ),
            evidence_sufficiency: Some(
                if is_planning_request {
                    "The request text is captured; plan creation and execution still need explicit local review."
                        .to_string()
                } else {
                    "The feedback text and last Telegram interaction context are captured; further action needs explicit review."
                        .to_string()
                },
            ),
            default_if_no_reply: Some("defer".to_string()),
            scope: current_scope,
            question: if is_planning_request {
                "Should this Telegram request become a bounded Offdesk plan candidate?".to_string()
            } else {
                "How should this Telegram feedback be handled?".to_string()
            },
            options: approval_options,
            why_recommendation: vec![
                if is_planning_request {
                    "The message explicitly asks whether autonomous work can be planned.".to_string()
                } else {
                    "Freeform feedback often indicates a needed plan or review adjustment."
                        .to_string()
                },
                if is_planning_request {
                    "The safest next step is a bounded plan candidate, not immediate execution."
                        .to_string()
                } else {
                    "The safest default is to revise only after a bounded review decision."
                        .to_string()
                },
            ],
            evidence: evidence_refs
                .iter()
                .map(|reference| format!("{}: {}", reference.label, reference.reference))
                .collect(),
            decision_impacts,
            reply_examples: vec![
                if is_planning_request {
                    "plan: draft a bounded plan for the requested project and timebox".to_string()
                } else {
                    "revise: tighten the next plan around the missing mobile UX evidence"
                        .to_string()
                },
                "defer: wait until the morning review".to_string(),
                "deny: no change needed because this is already covered".to_string(),
            ],
            context: approval_context,
        }),
        execution_handoff: None,
        decision_receipt: None,
        trace_refs: evidence_refs,
    })
}

fn json_string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToOwned::to_owned)
}

fn feedback_context_string(feedback: &Value, field: &str) -> Option<String> {
    feedback
        .get("feedback_context")
        .and_then(Value::as_object)
        .and_then(|context| context.get(field))
        .and_then(Value::as_str)
        .and_then(safe_nonempty)
}

fn feedback_message_id(feedback: &Value) -> Option<String> {
    match feedback.get("message_id") {
        Some(Value::Number(value)) => Some(value.to_string()),
        Some(Value::String(value)) => safe_nonempty(value),
        _ => None,
    }
}

fn classify_telegram_feedback_kind(text: &str) -> &'static str {
    let normalized = text.trim().to_lowercase();
    if [
        "자율주행",
        "계획",
        "plan",
        "offdesk",
        "진행",
        "처리",
        "검토해볼까",
        "시작",
        "맡기",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        "planning_request"
    } else {
        "freeform_feedback"
    }
}

fn feedback_materiality(
    context_kind: Option<&str>,
    focus_kind: Option<&str>,
) -> DecisionMateriality {
    let context_kind = context_kind.unwrap_or_default();
    let focus_kind = focus_kind.unwrap_or_default();
    if matches!(focus_kind, "approval" | "plan" | "decision")
        || context_kind.contains("attention")
        || context_kind.contains("pending")
    {
        DecisionMateriality::Medium
    } else {
        DecisionMateriality::Low
    }
}

fn safe_nonempty(value: &str) -> Option<String> {
    let safe = operator_safe_text(value.trim());
    if safe.trim().is_empty() {
        None
    } else {
        Some(safe)
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...<truncated>")
    } else {
        truncated
    }
}

fn decision_record_has_matching_handoff(record: &DecisionRecord, decision: &str) -> bool {
    record
        .execution_handoff
        .as_ref()
        .map(|handoff| handoff.approved_direction == normalize_decision_choice(decision))
        .unwrap_or(false)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn present_telegram_decision_report(
    report: &DecisionIngestTelegramReport,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }

    println!("Decision: {}", report.decision_id);
    println!("Telegram status: {}", report.telegram_status);
    if let Some(decision) = report.telegram_decision.as_deref() {
        println!("Telegram decision: {}", decision);
    }
    println!("Ledger: {}", report.ledger_path);
    if report.appended_records.is_empty() {
        println!("Appended: none");
    } else {
        println!("Appended: {}", report.appended_records.join(", "));
    }
    if report.receipt_recorded {
        println!("Receipt: recorded");
    }
    if let Some(reason) = report.skipped_reason.as_deref() {
        println!("Skipped: {}", reason);
    }
    if !report.validation_issues.is_empty() {
        println!("Validation issues: {}", report.validation_issues.len());
    }
    Ok(())
}

fn present_telegram_feedback_report(
    report: &DecisionIngestTelegramFeedbackReport,
    json: bool,
) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }

    println!("Decision: {}", report.decision_id);
    println!("Feedback: {}", report.feedback_path);
    println!("Ledger: {}", report.ledger_path);
    println!("Appended: {}", if report.appended { "yes" } else { "no" });
    println!("Status: {}", report.record.status.as_str());
    if !report.validation_issues.is_empty() {
        println!("Validation issues: {}", report.validation_issues.len());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn feedback_classification_is_bounded_to_planning_markers() {
        assert_eq!(
            classify_telegram_feedback_kind("다음 작업을 계획하고 진행해볼까"),
            "planning_request"
        );
        assert_eq!(
            classify_telegram_feedback_kind("요약 문구가 너무 길어요"),
            "freeform_feedback"
        );
    }

    #[test]
    fn decision_result_parser_trims_operator_fields() {
        let result = parse_telegram_decision_result(&json!({
            "status": " accepted ",
            "decision": " revise ",
            "reason": " narrow the scope "
        }));

        assert_eq!(result.status, "accepted");
        assert_eq!(result.decision.as_deref(), Some("revise"));
        assert_eq!(result.reason, "narrow the scope");
    }

    #[test]
    fn jsonl_reader_selects_latest_nonempty_row() -> Result<()> {
        let temp = tempdir()?;
        let path = temp.path().join("feedback.jsonl");
        fs::write(&path, "{\"message_id\":1}\n\n{\"message_id\":2}\n")?;

        let value = read_json_or_latest_jsonl_file(&path)?;

        assert_eq!(value["message_id"], 2);
        Ok(())
    }

    #[test]
    fn feedback_record_is_review_only_and_stable_across_receive_time() -> Result<()> {
        let path = Path::new("feedback.json");
        let base = json!({
            "schema": "remote_operator_telegram_feedback.v1",
            "received_at": "2026-08-14T00:00:00Z",
            "profile": "default",
            "chat_id_hash": "sha256:chat",
            "user_id_hash": "sha256:user",
            "message_id": 42,
            "feedback_text": "다음 작업을 계획해보자",
            "target_chat_id_hash": "sha256:chat",
            "feedback_context": {
                "context_kind": "status_clear",
                "focus_kind": "none"
            }
        });
        let mut replay = base.clone();
        replay["received_at"] = json!("2026-08-14T00:01:00Z");

        let record = decision_record_from_telegram_feedback(&base, path, "telegram")?;
        let replay_record = decision_record_from_telegram_feedback(&replay, path, "telegram")?;

        assert_eq!(record.decision_id, replay_record.decision_id);
        assert_eq!(record.status, DecisionStatus::UserPending);
        assert_eq!(
            record.source_surface,
            "telegram.remote_operator.plan_request"
        );
        assert!(record.execution_handoff.is_none());
        assert!(record.decision_receipt.is_none());
        assert!(record
            .decision_request
            .non_authorized_scope
            .iter()
            .any(|scope| scope == "background dispatch"));
        Ok(())
    }
}
