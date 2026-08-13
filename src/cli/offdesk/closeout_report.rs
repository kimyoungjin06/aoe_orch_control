//! CLI I/O adapter for closeout report assembly and initial artifacts.

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use super::{
    closeout_background_summary, closeout_decision_records, closeout_documentation_governance,
    closeout_file_operations, closeout_git_snapshot, closeout_implementation_packet_coverage,
    closeout_open_decisions, closeout_probe_matches, closeout_source_observation,
    closeout_task_matches, closeout_task_summary, closeout_verification_commands,
    render_closeout_plan_markdown, render_closeout_return_package, render_commercial_review_packet,
    short_uuid, summarize_closeout, write_new_file, CloseoutArgs, CloseoutArtifactPaths,
    CloseoutFilters, CloseoutReadRef, CloseoutReviewContract, OffdeskCloseoutReport,
};
use crate::offdesk::{BackgroundRunStore, OffdeskTaskStore};
use crate::session::{get_profile_dir, DEFAULT_PROFILE};

pub(super) fn build_closeout_report(
    profile: &str,
    args: &CloseoutArgs,
) -> Result<OffdeskCloseoutReport> {
    let profile_dir = get_profile_dir(profile)?;
    let profile_name = if profile.is_empty() {
        DEFAULT_PROFILE
    } else {
        profile
    };
    let generated_at = Utc::now();
    let closeout_id = format!("closeout_{}", short_uuid());

    let filters = CloseoutFilters {
        project_key: args
            .project_key
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        request_id: args
            .request_id
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
        task_id: args
            .task_id
            .as_deref()
            .map(crate::offdesk::operator_safe_text),
    };

    let tasks = OffdeskTaskStore::new(&profile_dir)
        .load()?
        .into_iter()
        .filter(|task| closeout_task_matches(task, args))
        .collect::<Vec<_>>();
    let background_runs = BackgroundRunStore::new(&profile_dir)
        .load()?
        .into_iter()
        .filter(|probe| closeout_probe_matches(probe, args))
        .collect::<Vec<_>>();

    let closeout_tasks = tasks.iter().map(closeout_task_summary).collect::<Vec<_>>();
    let closeout_background_runs = background_runs
        .iter()
        .map(closeout_background_summary)
        .collect::<Vec<_>>();
    let source_observation = closeout_source_observation(
        args,
        &closeout_tasks,
        &closeout_background_runs,
        generated_at,
    );
    let implementation_packet_coverage = closeout_implementation_packet_coverage(
        &closeout_tasks,
        &closeout_background_runs,
        &source_observation,
    );
    let decision_records = closeout_decision_records(&profile_dir, &tasks, &background_runs, args)?;

    let mut file_operations = closeout_file_operations(&tasks, &background_runs);
    file_operations.sort_by(|left, right| {
        (left.path.as_str(), left.operation).cmp(&(right.path.as_str(), right.operation))
    });
    file_operations.dedup_by(|left, right| {
        left.path == right.path && left.operation == right.operation && left.source == right.source
    });

    let mut required_first_reads = file_operations
        .iter()
        .filter(|operation| operation.present && operation.operation == "keep")
        .map(|operation| CloseoutReadRef {
            path: operation.path.clone(),
            reason: operation.reason.clone(),
            present: operation.present,
        })
        .collect::<Vec<_>>();
    let mut decision_sources = BTreeSet::new();
    for decision in &decision_records {
        if decision_sources.insert(decision.source_path.clone()) {
            required_first_reads.push(CloseoutReadRef {
                path: decision.source_path.clone(),
                reason: "Decision ledger used by closeout; review unresolved decisions before accepting the run.".to_string(),
                present: Path::new(&decision.source_path).exists(),
            });
        }
    }
    required_first_reads.truncate(20);

    let git_snapshot = if args.include_git {
        closeout_git_snapshot(args, &tasks)?
    } else {
        None
    };
    let documentation_governance = closeout_documentation_governance(args, &tasks);
    let open_decisions = closeout_open_decisions(
        &tasks,
        &file_operations,
        &decision_records,
        git_snapshot.as_ref(),
        args,
        documentation_governance.as_ref(),
        &implementation_packet_coverage,
    );
    let verification_commands =
        closeout_verification_commands(args, documentation_governance.as_ref());

    // Allocate only after all fallible source reads complete so failed assembly
    // does not leave an empty default closeout directory behind.
    let artifact_dir = allocate_closeout_artifact_dir(
        &profile_dir,
        args.output.as_ref(),
        generated_at,
        &closeout_id,
    )?;
    let artifacts = CloseoutArtifactPaths {
        closeout_plan_json: artifact_dir
            .join("closeout_plan.json")
            .display()
            .to_string(),
        closeout_plan_markdown: artifact_dir.join("CLOSEOUT_PLAN.md").display().to_string(),
        cleanup_manifest_json: artifact_dir
            .join("cleanup_manifest.json")
            .display()
            .to_string(),
        commercial_review_packet: artifact_dir
            .join("COMMERCIAL_REVIEW_PACKET.md")
            .display()
            .to_string(),
        return_package_markdown: artifact_dir.join("RETURN_PACKAGE.md").display().to_string(),
    };
    let review_contract = CloseoutReviewContract {
        provider: crate::offdesk::operator_safe_text(&args.review_provider),
        required: true,
        applies_to_operations: vec!["archive_candidate", "delete_candidate", "move_candidate"],
        required_verdicts: vec!["approved", "revise", "blocked"],
        decision_schema: serde_json::json!({
            "verdict": "approved|revise|blocked",
            "unsafe_operations": ["operation path or id"],
            "missing_evidence": ["required file, artifact, or command"],
            "required_first_reads": ["paths the next Ondesk harness must read first"],
            "packet_goal_coverage": "completed|deferred|missing|drifted",
            "notes": "short rationale"
        }),
        safety_rules: vec![
            "Never approve delete or move for git-tracked source files without explicit human approval.",
            "Never treat closeout as completion proof; require result and review artifacts.",
            "Archive raw logs before deletion is considered.",
            "Reject plans that touch hidden config, env, mount, symlink, external drive, or system paths without dedicated evidence.",
            "Prefer keep or archive when provenance is uncertain.",
        ],
        packet_path: artifacts.commercial_review_packet.clone(),
    };

    let summary = summarize_closeout(
        &closeout_tasks,
        &closeout_background_runs,
        &file_operations,
        &decision_records,
        &implementation_packet_coverage,
    );

    let report = OffdeskCloseoutReport {
        generated_at,
        closeout_id,
        profile: crate::offdesk::operator_safe_text(profile_name),
        profile_dir: crate::offdesk::operator_safe_text(profile_dir.to_string_lossy().as_ref()),
        artifact_dir: artifact_dir.display().to_string(),
        dry_run: true,
        operator_requested_dry_run: args.dry_run,
        read_only_project_state: true,
        filters,
        summary,
        source_observation,
        implementation_packet_coverage,
        tasks: closeout_tasks,
        background_runs: closeout_background_runs,
        file_operations,
        required_first_reads,
        decision_records,
        open_decisions,
        verification_commands,
        documentation_governance,
        review_contract,
        git_snapshot,
        artifacts,
    };

    write_closeout_artifacts(&report)?;
    Ok(report)
}

fn allocate_closeout_artifact_dir(
    profile_dir: &Path,
    output: Option<&PathBuf>,
    generated_at: DateTime<Utc>,
    closeout_id: &str,
) -> Result<PathBuf> {
    if let Some(output) = output {
        fs::create_dir_all(output)
            .with_context(|| format!("create closeout output directory {}", output.display()))?;
        return Ok(output.clone());
    }

    let base = profile_dir.join("offdesk_closeouts");
    fs::create_dir_all(&base)
        .with_context(|| format!("create closeout artifact root {}", base.display()))?;
    let timestamp = generated_at.format("%Y%m%dT%H%M%SZ");
    for attempt in 0..1000 {
        let dirname = if attempt == 0 {
            format!("{timestamp}_{closeout_id}")
        } else {
            format!("{timestamp}_{closeout_id}_{attempt:03}")
        };
        let path = base.join(dirname);
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create closeout artifact dir {}", path.display()));
            }
        }
    }

    bail!(
        "could not allocate closeout artifact directory in {}",
        base.display()
    )
}

fn write_closeout_artifacts(report: &OffdeskCloseoutReport) -> Result<()> {
    let files = vec![
        (
            PathBuf::from(&report.artifacts.closeout_plan_json),
            serde_json::to_vec_pretty(report)?,
        ),
        (
            PathBuf::from(&report.artifacts.cleanup_manifest_json),
            serde_json::to_vec_pretty(&report.file_operations)?,
        ),
        (
            PathBuf::from(&report.artifacts.closeout_plan_markdown),
            render_closeout_plan_markdown(report).into_bytes(),
        ),
        (
            PathBuf::from(&report.artifacts.return_package_markdown),
            render_closeout_return_package(report).into_bytes(),
        ),
        (
            PathBuf::from(&report.artifacts.commercial_review_packet),
            render_commercial_review_packet(report).into_bytes(),
        ),
    ];
    write_closeout_artifact_files(&files)
}

fn write_closeout_artifact_files(files: &[(PathBuf, Vec<u8>)]) -> Result<()> {
    for (path, _) in files {
        if path
            .try_exists()
            .with_context(|| format!("inspect closeout artifact target {}", path.display()))?
        {
            bail!(
                "closeout artifact target already exists: {}",
                path.display()
            );
        }
    }

    for (path, bytes) in files {
        write_new_file(path, bytes).with_context(|| format!("write {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn artifact_collision_is_detected_before_any_write() -> Result<()> {
        let temp = tempdir()?;
        let first = temp.path().join("closeout_plan.json");
        let collision = temp.path().join("CLOSEOUT_PLAN.md");
        let last = temp.path().join("RETURN_PACKAGE.md");
        fs::write(&collision, "existing")?;

        let error = write_closeout_artifact_files(&[
            (first.clone(), b"plan".to_vec()),
            (collision.clone(), b"markdown".to_vec()),
            (last.clone(), b"return".to_vec()),
        ])
        .expect_err("existing target must fail closed");

        assert!(error
            .to_string()
            .contains("closeout artifact target already exists"));
        assert!(!first.exists());
        assert_eq!(fs::read_to_string(collision)?, "existing");
        assert!(!last.exists());
        Ok(())
    }
}
