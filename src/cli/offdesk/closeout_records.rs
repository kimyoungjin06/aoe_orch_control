//! CLI I/O adapter for closeout review, decision, and retirement records.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

use super::{
    require_non_empty_arg, write_new_file, CloseoutDecisionArgs, CloseoutRetireArgs,
    CloseoutReviewArgs,
};
use crate::offdesk::{
    build_closeout_decision_record as workflow_build_closeout_decision_record,
    build_closeout_retirement_record as workflow_build_closeout_retirement_record,
    build_closeout_review_record as workflow_build_closeout_review_record,
    CloseoutDecisionRecordBuildInput, CloseoutReceipt, CloseoutReceiptTaskRef,
    CloseoutRetirementRecordBuildInput, CloseoutReviewArtifactPaths, CloseoutReviewRecord,
    CloseoutReviewRecordBuildInput, OffdeskTaskStore,
};
use crate::session::{get_profile_dir, DEFAULT_PROFILE};

pub(super) fn build_closeout_decision_record(
    profile: &str,
    args: &CloseoutDecisionArgs,
) -> Result<CloseoutReviewRecord> {
    let profile_dir = get_profile_dir(profile)?;
    let profile_name = profile_name(profile);
    let kind = require_non_empty_arg("--kind", args.kind.trim())?;
    let reason = require_non_empty_arg("--reason", args.reason.trim())?;
    let artifact_dir = resolve_closeout_artifact_dir_for(
        &profile_dir,
        args.closeout_id.as_deref(),
        args.artifact_dir.as_ref(),
    )?;
    let plan_path = artifact_dir.join("closeout_plan.json");
    let plan = read_closeout_plan(&plan_path, "read closeout plan")?;
    let closeout_id = closeout_id_from_plan(&plan)?;
    ensure_closeout_id_matches(args.closeout_id.as_deref(), &closeout_id)?;

    let (source_review_record_path, _source_reviewed_at, source_review) =
        latest_closeout_review_value(&artifact_dir)?;
    let reviewed_at = Utc::now();
    let review_id = format!("closeout_decision_{}", short_uuid());
    let artifacts =
        allocate_closeout_review_artifacts(&artifact_dir, &plan_path, &plan, reviewed_at)?;
    let closeout_generated_at = closeout_generated_at_from_plan(&plan);
    let applies_to_tasks = closeout_review_task_refs_from_plan(&plan);
    let source_review_record_json = source_review_record_path.display().to_string();
    let artifact_dir_text = artifact_dir.display().to_string();
    let record = workflow_build_closeout_decision_record(CloseoutDecisionRecordBuildInput {
        plan: &plan,
        source_review: &source_review,
        source_review_record_json: &source_review_record_json,
        profile: profile_name,
        artifact_dir: &artifact_dir_text,
        artifacts,
        closeout_id: &closeout_id,
        closeout_generated_at,
        applies_to_tasks,
        kind,
        decision: args.decision.into(),
        reviewer: &args.reviewer,
        reason,
        reviewed_at,
        review_id: &review_id,
    })?;

    write_closeout_review_record(&record)?;
    Ok(record)
}

pub(super) fn build_closeout_retire_record(
    profile: &str,
    args: &CloseoutRetireArgs,
) -> Result<CloseoutReviewRecord> {
    let profile_dir = get_profile_dir(profile)?;
    let profile_name = profile_name(profile);
    let reason = require_non_empty_arg("--reason", args.reason.trim())?;
    let artifact_dir = resolve_closeout_artifact_dir_for(
        &profile_dir,
        args.closeout_id.as_deref(),
        args.artifact_dir.as_ref(),
    )?;
    let plan_path = artifact_dir.join("closeout_plan.json");
    let plan = read_closeout_plan(&plan_path, "read closeout plan")?;
    let closeout_id = closeout_id_from_plan(&plan)?;
    ensure_closeout_id_matches(args.closeout_id.as_deref(), &closeout_id)?;

    let source_review = latest_closeout_review_value_optional(&artifact_dir)?;
    let current_acceptance = latest_closeout_acceptance_by_task(&profile_dir)?;
    let mut excluded_accepted_tasks = Vec::new();
    let applies_to_tasks = closeout_review_task_refs_from_plan(&plan)
        .into_iter()
        .filter(|task| {
            let key = (task.project_key.clone(), task.task_id.clone());
            if current_acceptance.get(&key).map(String::as_str) == Some("accepted") {
                excluded_accepted_tasks.push(format!("{}:{}", task.project_key, task.task_id));
                false
            } else {
                true
            }
        })
        .collect::<Vec<_>>();
    let reviewed_at = Utc::now();
    let review_id = format!("closeout_retirement_{}", short_uuid());
    let artifacts =
        allocate_closeout_review_artifacts(&artifact_dir, &plan_path, &plan, reviewed_at)?;
    let closeout_generated_at = closeout_generated_at_from_plan(&plan);
    let source_review_record_json = source_review
        .as_ref()
        .map(|(path, _, _)| path.display().to_string());
    let artifact_dir_text = artifact_dir.display().to_string();
    let record = workflow_build_closeout_retirement_record(CloseoutRetirementRecordBuildInput {
        plan: &plan,
        source_review: source_review.as_ref().map(|(_, _, review)| review),
        source_review_record_json,
        profile: profile_name,
        artifact_dir: &artifact_dir_text,
        artifacts,
        closeout_id: &closeout_id,
        closeout_generated_at,
        applies_to_tasks,
        excluded_accepted_tasks,
        reviewer: &args.reviewer,
        reason,
        reviewed_at,
        review_id: &review_id,
    })?;

    write_closeout_review_record(&record)?;
    Ok(record)
}

pub(super) fn build_closeout_review_record(
    profile: &str,
    args: &CloseoutReviewArgs,
) -> Result<CloseoutReviewRecord> {
    let profile_dir = get_profile_dir(profile)?;
    let profile_name = profile_name(profile);
    let artifact_dir = resolve_closeout_artifact_dir_for(
        &profile_dir,
        args.closeout_id.as_deref(),
        args.artifact_dir.as_ref(),
    )?;
    let plan_path = artifact_dir.join("closeout_plan.json");
    let plan = read_closeout_plan(&plan_path, "read closeout plan for review record")?;
    let closeout_id = closeout_id_from_plan(&plan)?;
    ensure_closeout_id_matches(args.closeout_id.as_deref(), &closeout_id)?;

    let closeout_generated_at = closeout_generated_at_from_plan(&plan);
    let applies_to_tasks = closeout_review_task_refs_from_plan(&plan);
    let reviewed_at = Utc::now();
    let review_id = format!("closeout_review_{}", short_uuid());
    let artifacts =
        allocate_closeout_review_artifacts(&artifact_dir, &plan_path, &plan, reviewed_at)?;
    let stale_task_count =
        closeout_review_stale_task_count(&profile_dir, &applies_to_tasks, closeout_generated_at);
    let review_file = args
        .review_file
        .as_ref()
        .map(|path| path.to_string_lossy().to_string());
    let artifact_dir_text = artifact_dir.display().to_string();
    let record = workflow_build_closeout_review_record(CloseoutReviewRecordBuildInput {
        plan: &plan,
        profile: profile_name,
        artifact_dir: &artifact_dir_text,
        artifacts,
        closeout_id: &closeout_id,
        closeout_generated_at,
        applies_to_tasks,
        verdict: args.verdict.into(),
        reviewer: &args.reviewer,
        review_provider: args.review_provider.as_deref(),
        review_file,
        unsafe_operations: &args.unsafe_operation,
        missing_evidence: &args.missing_evidence,
        required_first_reads: &args.required_first_read,
        notes: args.notes.as_deref(),
        stale_task_count,
        reviewed_at,
        review_id: &review_id,
    });

    write_closeout_review_record(&record)?;
    Ok(record)
}

fn profile_name(profile: &str) -> &str {
    if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    }
}

fn read_closeout_plan(path: &Path, operation: &str) -> Result<Value> {
    let content =
        fs::read_to_string(path).with_context(|| format!("{operation} {}", path.display()))?;
    serde_json::from_str(&content)
        .with_context(|| format!("parse closeout plan {}", path.display()))
}

fn closeout_id_from_plan(plan: &Value) -> Result<String> {
    plan.get("closeout_id")
        .and_then(Value::as_str)
        .map(crate::offdesk::operator_safe_text)
        .ok_or_else(|| anyhow::anyhow!("closeout plan is missing closeout_id"))
}

fn ensure_closeout_id_matches(expected: Option<&str>, actual: &str) -> Result<()> {
    let Some(expected) = expected else {
        return Ok(());
    };
    let expected = crate::offdesk::operator_safe_text(expected);
    if expected != actual {
        bail!(
            "closeout id mismatch: requested {}, artifact contains {}",
            expected,
            actual
        );
    }
    Ok(())
}

fn closeout_generated_at_from_plan(plan: &Value) -> Option<DateTime<Utc>> {
    plan.get("generated_at")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn closeout_review_task_refs_from_plan(plan: &Value) -> Vec<CloseoutReceiptTaskRef> {
    plan.get("tasks")
        .and_then(Value::as_array)
        .map(|tasks| {
            tasks
                .iter()
                .filter_map(|task| {
                    Some(CloseoutReceiptTaskRef {
                        project_key: crate::offdesk::operator_safe_text(
                            task.get("project_key")?.as_str()?,
                        ),
                        request_id: crate::offdesk::operator_safe_text(
                            task.get("request_id")?.as_str()?,
                        ),
                        task_id: crate::offdesk::operator_safe_text(task.get("task_id")?.as_str()?),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn latest_closeout_review_value(artifact_dir: &Path) -> Result<(PathBuf, DateTime<Utc>, Value)> {
    latest_closeout_review_value_optional(artifact_dir)?.ok_or_else(|| {
        anyhow::anyhow!(
            "no closeout review record found in {}; run closeout-review first",
            artifact_dir.display()
        )
    })
}

fn latest_closeout_review_value_optional(
    artifact_dir: &Path,
) -> Result<Option<(PathBuf, DateTime<Utc>, Value)>> {
    let mut reviews = Vec::new();
    for entry in fs::read_dir(artifact_dir)
        .with_context(|| format!("read closeout artifact dir {}", artifact_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !is_closeout_review_path(&path) {
            continue;
        }
        let content = fs::read_to_string(&path)
            .with_context(|| format!("read closeout review {}", path.display()))?;
        let value: Value = serde_json::from_str(&content)
            .with_context(|| format!("parse closeout review {}", path.display()))?;
        let Some(reviewed_at) = closeout_reviewed_at(&value) else {
            continue;
        };
        reviews.push((reviewed_at, path, value));
    }
    reviews.sort_by_key(|(reviewed_at, _, _)| *reviewed_at);
    Ok(reviews
        .pop()
        .map(|(reviewed_at, path, value)| (path, reviewed_at, value)))
}

fn latest_closeout_acceptance_by_task(
    profile_dir: &Path,
) -> Result<BTreeMap<(String, String), String>> {
    let closeouts_dir = profile_dir.join("offdesk_closeouts");
    let mut latest = BTreeMap::<(String, String), (DateTime<Utc>, String)>::new();
    let closeouts = match fs::read_dir(&closeouts_dir) {
        Ok(closeouts) => closeouts,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new());
        }
        Err(error) => {
            return Err(error).with_context(|| {
                format!("read closeout artifact root {}", closeouts_dir.display())
            });
        }
    };
    for closeout in closeouts {
        let closeout = closeout?;
        if !closeout.file_type()?.is_dir() {
            continue;
        }
        for review in fs::read_dir(closeout.path())? {
            let review = review?;
            let path = review.path();
            if !is_closeout_review_path(&path) {
                continue;
            }
            let content = fs::read_to_string(&path)
                .with_context(|| format!("read closeout review {}", path.display()))?;
            let value: Value = serde_json::from_str(&content)
                .with_context(|| format!("parse closeout review {}", path.display()))?;
            let Some(reviewed_at) = closeout_reviewed_at(&value) else {
                continue;
            };
            let status = value
                .pointer("/closeout_receipt/acceptance_status")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            let Some(tasks) = value.get("applies_to_tasks").and_then(Value::as_array) else {
                continue;
            };
            for task in tasks {
                let Some(project_key) = task.get("project_key").and_then(Value::as_str) else {
                    continue;
                };
                let Some(task_id) = task.get("task_id").and_then(Value::as_str) else {
                    continue;
                };
                let key = (project_key.to_string(), task_id.to_string());
                latest
                    .entry(key)
                    .and_modify(|existing| {
                        if reviewed_at > existing.0 {
                            *existing = (reviewed_at, status.clone());
                        }
                    })
                    .or_insert_with(|| (reviewed_at, status.clone()));
            }
        }
    }
    Ok(latest
        .into_iter()
        .map(|(key, (_, status))| (key, status))
        .collect())
}

fn is_closeout_review_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|filename| {
            filename.starts_with("closeout_review_") && filename.ends_with(".json")
        })
}

fn closeout_reviewed_at(value: &Value) -> Option<DateTime<Utc>> {
    value
        .get("reviewed_at")
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn resolve_closeout_artifact_dir_for(
    profile_dir: &Path,
    closeout_id: Option<&str>,
    artifact_dir: Option<&PathBuf>,
) -> Result<PathBuf> {
    if let Some(artifact_dir) = artifact_dir {
        return Ok(artifact_dir.clone());
    }

    let closeouts_dir = profile_dir.join("offdesk_closeouts");
    let entries = fs::read_dir(&closeouts_dir)
        .with_context(|| format!("read closeout artifact root {}", closeouts_dir.display()))?;
    let mut candidates = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let artifact_dir = entry.path();
        let plan_path = artifact_dir.join("closeout_plan.json");
        let Ok(content) = fs::read_to_string(&plan_path) else {
            continue;
        };
        let Ok(plan) = serde_json::from_str::<Value>(&content) else {
            continue;
        };
        let plan_closeout_id = plan.get("closeout_id").and_then(Value::as_str);
        if let Some(expected) = closeout_id {
            if plan_closeout_id != Some(expected) {
                continue;
            }
        }
        let generated_at =
            closeout_generated_at_from_plan(&plan).unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        candidates.push((generated_at, artifact_dir));
    }

    candidates.sort_by_key(|(generated_at, _)| *generated_at);
    candidates.pop().map(|(_, path)| path).ok_or_else(|| {
        if let Some(closeout_id) = closeout_id {
            anyhow::anyhow!(
                "no closeout artifact found for closeout_id {}",
                crate::offdesk::operator_safe_text(closeout_id)
            )
        } else {
            anyhow::anyhow!("no closeout artifact found; run `forager offdesk closeout` first")
        }
    })
}

fn allocate_closeout_review_artifacts(
    artifact_dir: &Path,
    plan_path: &Path,
    plan: &Value,
    reviewed_at: DateTime<Utc>,
) -> Result<CloseoutReviewArtifactPaths> {
    let review_record_path = allocate_closeout_review_record_path(artifact_dir, reviewed_at)?;
    let receipt_path = allocate_closeout_receipt_path(artifact_dir, reviewed_at)?;
    let return_package_path = closeout_return_package_path(artifact_dir, plan);
    Ok(CloseoutReviewArtifactPaths {
        closeout_plan_json: plan_path.display().to_string(),
        review_record_json: review_record_path.display().to_string(),
        closeout_receipt_json: receipt_path.display().to_string(),
        return_package_markdown: return_package_path.display().to_string(),
    })
}

fn closeout_return_package_path(artifact_dir: &Path, plan: &Value) -> PathBuf {
    plan.pointer("/artifacts/return_package_markdown")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .unwrap_or_else(|| artifact_dir.join("RETURN_PACKAGE.md"))
}

fn allocate_closeout_review_record_path(
    artifact_dir: &Path,
    reviewed_at: DateTime<Utc>,
) -> Result<PathBuf> {
    allocate_timestamped_path(
        artifact_dir,
        reviewed_at,
        "closeout_review",
        "review record",
    )
}

fn allocate_closeout_receipt_path(
    artifact_dir: &Path,
    reviewed_at: DateTime<Utc>,
) -> Result<PathBuf> {
    allocate_timestamped_path(artifact_dir, reviewed_at, "closeout_receipt", "receipt")
}

fn allocate_timestamped_path(
    artifact_dir: &Path,
    generated_at: DateTime<Utc>,
    stem: &str,
    artifact_kind: &str,
) -> Result<PathBuf> {
    fs::create_dir_all(artifact_dir)
        .with_context(|| format!("create closeout artifact dir {}", artifact_dir.display()))?;
    let timestamp = generated_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let filename = if attempt == 0 {
            format!("{stem}_{timestamp}.json")
        } else {
            format!("{stem}_{timestamp}_{attempt:03}.json")
        };
        let path = artifact_dir.join(filename);
        if !path.exists() {
            return Ok(path);
        }
    }

    bail!(
        "could not allocate closeout {artifact_kind} path in {}",
        artifact_dir.display()
    )
}

fn write_closeout_review_record(record: &CloseoutReviewRecord) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(record)?;
    write_new_file(Path::new(&record.artifacts.review_record_json), &bytes)
        .with_context(|| format!("write {}", record.artifacts.review_record_json))?;
    let receipt_bytes = serde_json::to_vec_pretty(&record.closeout_receipt)?;
    write_new_file(
        Path::new(&record.artifacts.closeout_receipt_json),
        &receipt_bytes,
    )
    .with_context(|| format!("write {}", record.artifacts.closeout_receipt_json))?;
    update_return_package_with_closeout_receipt(record)?;
    Ok(())
}

fn closeout_review_stale_task_count(
    profile_dir: &Path,
    applies_to_tasks: &[CloseoutReceiptTaskRef],
    closeout_generated_at: Option<DateTime<Utc>>,
) -> usize {
    let Some(generated_at) = closeout_generated_at else {
        return 0;
    };
    let targets = applies_to_tasks
        .iter()
        .map(|task| (task.project_key.clone(), task.task_id.clone()))
        .collect::<BTreeSet<_>>();
    if targets.is_empty() {
        return 0;
    }
    let Ok(tasks) = OffdeskTaskStore::new(profile_dir).load() else {
        return 0;
    };
    tasks
        .iter()
        .filter(|task| targets.contains(&(task.project_key.clone(), task.task_id.clone())))
        .filter(|task| task.updated_at > generated_at)
        .count()
}

fn update_return_package_with_closeout_receipt(record: &CloseoutReviewRecord) -> Result<()> {
    let path = Path::new(&record.artifacts.return_package_markdown);
    let existing =
        fs::read_to_string(path).unwrap_or_else(|_| "# Ondesk Return Package\n\n".to_string());
    let section =
        render_closeout_receipt_return_section(&record.closeout_receipt, &record.artifacts);
    let updated = replace_marked_section(
        &existing,
        CLOSEOUT_RECEIPT_SECTION_START,
        CLOSEOUT_RECEIPT_SECTION_END,
        &section,
    );
    fs::write(path, updated).with_context(|| format!("update {}", path.display()))?;
    Ok(())
}

const CLOSEOUT_RECEIPT_SECTION_START: &str = "<!-- forager:closeout-receipt:start -->";
const CLOSEOUT_RECEIPT_SECTION_END: &str = "<!-- forager:closeout-receipt:end -->";

fn render_closeout_receipt_return_section(
    receipt: &CloseoutReceipt,
    artifacts: &CloseoutReviewArtifactPaths,
) -> String {
    let mut output = String::new();
    output.push_str(CLOSEOUT_RECEIPT_SECTION_START);
    output.push_str("\n## Closeout Receipt\n");
    output.push_str(&format!(
        "- acceptance_status: `{}`\n",
        receipt.acceptance_status
    ));
    output.push_str(&format!("- verdict: `{}`\n", receipt.verdict.as_str()));
    output.push_str(&format!(
        "- evidence_status: `{}` / verification_status: `{}`\n",
        receipt.evidence_status, receipt.verification_status
    ));
    output.push_str(&format!(
        "- open_decisions: {} / missing_evidence: {} / required_first_reads: {} / stale_tasks: {}\n",
        receipt.open_decisions.len(),
        receipt.missing_evidence.len(),
        receipt.required_first_reads.len(),
        receipt.stale_task_count
    ));
    output.push_str(&format!(
        "- retention_review: `{}` / wiki_promotion_state: `{}`\n",
        receipt.retention_review, receipt.wiki_promotion_state
    ));
    output.push_str(&format!(
        "- next_safe_action: {}\n",
        receipt.next_safe_action
    ));
    output.push_str(&format!(
        "- receipt_artifact: `{}`\n",
        artifacts.closeout_receipt_json
    ));
    output.push_str(CLOSEOUT_RECEIPT_SECTION_END);
    output.push_str("\n\n");
    output
}

fn replace_marked_section(existing: &str, start: &str, end: &str, section: &str) -> String {
    if let Some(start_index) = existing.find(start) {
        if let Some(end_offset) = existing[start_index..].find(end) {
            let end_index = start_index + end_offset + end.len();
            let mut output = String::new();
            output.push_str(existing[..start_index].trim_end());
            output.push_str("\n\n");
            output.push_str(section);
            output.push_str(existing[end_index..].trim_start());
            return output;
        }
    }

    if let Some(insert_at) = existing.find("\n## Status\n") {
        let mut output = String::new();
        output.push_str(&existing[..insert_at]);
        output.push_str(section);
        output.push_str(&existing[insert_at + 1..]);
        output
    } else {
        let mut output = String::new();
        output.push_str(existing.trim_end());
        output.push_str("\n\n");
        output.push_str(section);
        output
    }
}

fn short_uuid() -> String {
    Uuid::new_v4().to_string()[..8].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    #[test]
    fn marked_section_replacement_is_idempotent() {
        let original = "# Return\n\n## Status\nReady\n";
        let first = replace_marked_section(
            original,
            CLOSEOUT_RECEIPT_SECTION_START,
            CLOSEOUT_RECEIPT_SECTION_END,
            "<!-- forager:closeout-receipt:start -->\nfirst\n<!-- forager:closeout-receipt:end -->\n\n",
        );
        let second = replace_marked_section(
            &first,
            CLOSEOUT_RECEIPT_SECTION_START,
            CLOSEOUT_RECEIPT_SECTION_END,
            "<!-- forager:closeout-receipt:start -->\nsecond\n<!-- forager:closeout-receipt:end -->\n\n",
        );

        assert_eq!(second.matches(CLOSEOUT_RECEIPT_SECTION_START).count(), 1);
        assert!(second.contains("second"));
        assert!(!second.contains("first"));
        assert!(second.find(CLOSEOUT_RECEIPT_SECTION_START) < second.find("## Status"));
    }

    #[test]
    fn latest_review_selection_uses_reviewed_at() -> Result<()> {
        let temp = tempdir()?;
        fs::write(
            temp.path().join("closeout_review_z.json"),
            serde_json::to_vec(&json!({"reviewed_at": "2026-08-12T01:00:00Z"}))?,
        )?;
        fs::write(
            temp.path().join("closeout_review_a.json"),
            serde_json::to_vec(&json!({"reviewed_at": "2026-08-12T02:00:00Z"}))?,
        )?;

        let (path, _, _) = latest_closeout_review_value(temp.path())?;
        assert_eq!(
            path.file_name().and_then(|name| name.to_str()),
            Some("closeout_review_a.json")
        );
        Ok(())
    }

    #[test]
    fn optional_latest_review_distinguishes_missing_from_corrupt() -> Result<()> {
        let temp = tempdir()?;
        assert!(latest_closeout_review_value_optional(temp.path())?.is_none());

        fs::write(temp.path().join("closeout_review_bad.json"), "{")?;
        let error = latest_closeout_review_value_optional(temp.path()).unwrap_err();
        assert!(error.to_string().contains("parse closeout review"));
        Ok(())
    }

    #[test]
    fn acceptance_scan_fails_closed_on_corrupt_review() -> Result<()> {
        let temp = tempdir()?;
        let closeout = temp.path().join("offdesk_closeouts").join("closeout");
        fs::create_dir_all(&closeout)?;
        fs::write(closeout.join("closeout_review_bad.json"), "{")?;

        let error = latest_closeout_acceptance_by_task(temp.path()).unwrap_err();
        assert!(error.to_string().contains("parse closeout review"));
        Ok(())
    }
}
