use anyhow::Result;
use chrono::Utc;
use serde_json::{json, Value};
use serial_test::serial;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::tempdir;

fn forager_command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_forager"));
    command.env("HOME", home);
    command.env_remove("FORAGER_PROFILE");
    command.env_remove("AGENT_OF_EMPIRES_PROFILE");
    command.env_remove("FORAGER_DEBUG");
    command.env_remove("AGENT_OF_EMPIRES_DEBUG");
    #[cfg(target_os = "linux")]
    command.env("XDG_CONFIG_HOME", home.join(".config"));
    command
}

fn profile_dir(home: &Path) -> PathBuf {
    #[cfg(target_os = "linux")]
    {
        home.join(".config")
            .join("forager")
            .join("profiles")
            .join("default")
    }
    #[cfg(not(target_os = "linux"))]
    {
        home.join(".forager").join("profiles").join("default")
    }
}

fn initialize_git_repo_with_initial_commit(repo_path: &Path, paths: &[&str]) -> Result<()> {
    let repo = git2::Repository::init(repo_path)?;
    let sig = git2::Signature::now("Test", "test@example.com")?;
    let mut index = repo.index()?;
    for path in paths {
        index.add_path(Path::new(path))?;
    }
    index.write()?;
    let tree_id = index.write_tree()?;
    let tree = repo.find_tree(tree_id)?;
    repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])?;
    Ok(())
}

#[test]
#[serial]
fn offdesk_closeout_output_collision_writes_no_partial_artifacts() -> Result<()> {
    let temp = tempdir()?;
    fs::create_dir_all(profile_dir(temp.path()))?;
    let output_dir = temp.path().join("closeout-output");
    fs::create_dir_all(&output_dir)?;
    let collision = output_dir.join("CLOSEOUT_PLAN.md");
    fs::write(&collision, "existing plan")?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout",
            "--output",
            output_dir.to_str().expect("utf-8 output directory"),
            "--dry-run",
            "--json",
        ])
        .output()?;

    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("closeout artifact target already exists")
    );
    assert_eq!(fs::read_to_string(collision)?, "existing plan");
    for name in [
        "closeout_plan.json",
        "cleanup_manifest.json",
        "COMMERCIAL_REVIEW_PACKET.md",
        "RETURN_PACKAGE.md",
    ] {
        assert!(
            !output_dir.join(name).exists(),
            "unexpected artifact: {name}"
        );
    }
    Ok(())
}

#[test]
#[serial]
fn offdesk_closeout_writes_dry_run_review_packet_and_return_package() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let artifact_dir = temp.path().join("run-artifacts");
    fs::create_dir_all(&artifact_dir)?;
    let result_path = artifact_dir.join("result.json");
    let log_path = artifact_dir.join("runner.log");
    let report_path = artifact_dir.join("REPORT.md");
    fs::write(&result_path, "{\"status\":\"ok\"}")?;
    fs::write(&log_path, "runner log token=sk-secretsecretsecretsecret")?;
    fs::write(&report_path, "# Report\n")?;
    let mut artifact_refs = vec![json!({
        "artifact_id": "report",
        "path": report_path.to_str().expect("utf-8 report path"),
        "present": true
    })];
    for index in 0..8 {
        let extra_path = artifact_dir.join(format!("extra-report-{index}.md"));
        fs::write(&extra_path, format!("# Extra Report {index}\n"))?;
        artifact_refs.push(json!({
            "artifact_id": format!("extra-report-{index}"),
            "path": extra_path.to_str().expect("utf-8 extra artifact path"),
            "present": true
        }));
    }
    fs::write(temp.path().join("README.md"), "# Project\n")?;
    fs::write(
        temp.path().join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(temp.path().join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        temp.path().join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: project overview.\n",
    )?;
    for index in 0..3 {
        let path = temp.path().join(format!("outputs/doc-report-{index}.html"));
        fs::create_dir_all(path.parent().expect("output parent"))?;
        fs::write(path, "x".repeat((index + 1) * 10))?;
    }

    let now = Utc::now();
    let packet_dir = artifact_dir
        .join("implementation_packets")
        .join("packet-closeout-test");
    fs::create_dir_all(&packet_dir)?;
    let packet_path = packet_dir.join("IMPLEMENTATION_PACKET.json");
    let alignment_path = packet_dir.join("RECURSIVE_ALIGNMENT_REVIEW.json");
    let packet_markdown_path = packet_dir.join("IMPLEMENTATION_PACKET.md");
    let packet_record = json!({
        "schema": "implementation_packet.v1",
        "packet_id": "packet-closeout-test",
        "created_at": now,
        "project_key": "project",
        "project_root": temp.path().to_str().expect("utf-8 temp path"),
        "source_intent": {
            "user_goal": "Closeout must compare actual output against implementation intent.",
            "why_now": "Packet-aware closeout needs itemized evidence before Ondesk return.",
            "success_state": "Closeout reports completed, deferred, missing, and drifted packet goals."
        },
        "alignment": {
            "north_star_fit": "Keeps evidence, choices, and continuity visible.",
            "brand_fit": "Keeps Forager as the local meta-harness.",
            "product_boundary": "Closeout reports evidence; it does not accept truth.",
            "anti_drift_notes": []
        },
        "scope": {
            "included": ["packet coverage closeout"],
            "excluded": ["runtime approval", "cleanup application"],
            "allowed_files": [],
            "mutation_boundary": "closeout artifacts only",
            "non_authorized_actions": ["delete", "archive", "accepted truth"]
        },
        "capability_mapping": [{
            "capability_id": "FD-016",
            "reason": "Implementation packet closeout comparison."
        }],
        "design": {
            "approach": "Compare packet intent against task evidence.",
            "work_slices": ["coverage model", "coverage rendering"],
            "interfaces": [],
            "data_contracts": ["implementation_packet_coverage"],
            "compatibility_notes": []
        },
        "execution": {
            "preferred_worker": "hosted_harness",
            "worker_requirements": [],
            "commands": [],
            "stop_conditions": ["missing result artifact"],
            "rollback_or_recovery": []
        },
        "validation": {
            "tests": ["result.json"],
            "smoke_checks": ["runner.log"],
            "manual_review": [],
            "evidence_required": ["REPORT.md"]
        },
        "closeout": {
            "expected_artifacts": ["result.json", "runner.log", "REPORT.md"],
            "accepted_truth_rule": "Execution evidence is not accepted truth.",
            "handoff_summary_requirements": []
        },
        "recursive_alignment_review": {
            "schema": "recursive_alignment_review.v1",
            "reviewer": "deterministic_gate",
            "outcome": "pass",
            "checks": {
                "original_goal_coverage": "complete",
                "north_star_alignment": "acceptable",
                "brand_alignment": "acceptable",
                "scope_balance": "right_sized",
                "capability_coverage": "complete",
                "evidence_sufficiency": "sufficient",
                "completion_definition": "testable"
            },
            "drift_signals": [],
            "missing_decisions": [],
            "required_revisions": [],
            "safe_to_delegate": true
        }
    });
    fs::write(&packet_path, serde_json::to_string_pretty(&packet_record)?)?;
    fs::write(&alignment_path, "{}")?;
    fs::write(&packet_markdown_path, "# Implementation Packet\n")?;
    let packet_summary = json!({
        "packet_id": "packet-closeout-test",
        "created_at": now,
        "project_key": "project",
        "artifact_dir": packet_dir.to_str().expect("utf-8 packet dir"),
        "packet_path": packet_path.to_str().expect("utf-8 packet path"),
        "alignment_review_path": alignment_path.to_str().expect("utf-8 alignment path"),
        "markdown_path": packet_markdown_path.to_str().expect("utf-8 packet markdown path"),
        "goal": "Closeout must compare actual output against implementation intent.",
        "success_state": "Closeout reports completed, deferred, missing, and drifted packet goals.",
        "preferred_worker": "hosted_harness",
        "safe_to_delegate": true,
        "outcome": "pass",
        "required_revisions": [],
        "drift_signals": [],
        "missing_decisions": [],
        "work_slice_count": 2,
        "capability_mapping_count": 1,
        "validation_item_count": 3,
        "stop_condition_count": 1,
        "expected_artifact_count": 3
    });
    fs::write(
        artifact_dir.join("offdesk_decisions.jsonl"),
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "schema": "decision_record.v1",
                "decision_id": "decision-closeout",
                "project_key": "project",
                "request_id": "request-closeout",
                "task_id": "task-closeout",
                "raised_by": "council",
                "source_surface": "offdesk.council",
                "materiality": "high",
                "status": "user_pending",
                "created_at": now,
                "updated_at": now,
                "decision_request": {
                    "kind": "episode_council_continuation",
                    "summary": "Council recommends revising before accepting the run.",
                    "decision_needed": "Choose whether to revise or close out.",
                    "current_scope": "Next episode continuation only.",
                    "non_authorized_scope": [
                        "runtime dispatch",
                        "cleanup",
                        "provider retargeting",
                        "wiki promotion"
                    ]
                },
                "route": {
                    "materiality": "high",
                    "target": "user",
                    "reason": "Council returned a non-continue decision.",
                    "default_if_no_reply": "defer"
                },
                "approval_brief": {
                    "schema": "approval_brief.v1",
                    "source": "offdesk_twinpaper_autonomy_workload",
                    "recommendation": "revise",
                    "subject": "Council continuation decision",
                    "summary_lines": ["Council recommends revising before accepting the run."],
                    "scope": "Next episode continuation only; does not approve cleanup or provider retargeting.",
                    "question": "How should closeout treat this decision?"
                }
            }))?
        ),
    )?;
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task-closeout",
                "request_id": "request-closeout",
                "project_key": "project",
                "status": "completed",
                "capability_id": "inspect.status",
                "runner_kind": "local_tmux",
                "command": "echo token=sk-secretsecretsecretsecret",
                "workdir": temp.path().to_str().expect("utf-8 temp path"),
                "background_ticket_id": "ticket-closeout",
                "attempt_count": 1,
                "created_at": now,
                "updated_at": now,
                "artifact_refs": artifact_refs,
                "preview": "preview token=sk-secretsecretsecretsecret",
                "reason": "closeout test",
                "implementation_packet": packet_summary.clone(),
                "log_artifact_path": log_path.to_str().expect("utf-8 log path"),
                "result_artifact_path": result_path.to_str().expect("utf-8 result path")
            }
        ]))?,
    )?;
    fs::write(
        profile_dir.join("background_runs.json"),
        serde_json::to_string_pretty(&json!([
            {
                "ticket_id": "ticket-closeout",
                "project_key": "project",
                "request_id": "request-closeout",
                "task_id": "task-closeout",
                "runner_kind": "local_tmux",
                "phase": "completed",
                "working_dir": temp.path().to_str().expect("utf-8 temp path"),
                "log_artifact_path": log_path.to_str().expect("utf-8 log path"),
                "result_artifact_path": result_path.to_str().expect("utf-8 result path"),
                "implementation_packet": packet_summary.clone(),
                "runtime_handle_alive": false,
                "log_artifact_present": true,
                "result_artifact_present": true
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout",
            "--project-key",
            "project",
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["dry_run"], true);
    assert_eq!(report["read_only_project_state"], true);
    assert_eq!(report["review_contract"]["required"], true);
    assert_eq!(report["summary"]["delete_candidates"], 0);
    assert!(report["summary"]["archive_candidates"].as_u64().unwrap() >= 1);
    assert!(
        report["documentation_governance"]["recommendation_count"]
            .as_u64()
            .expect("documentation governance recommendation count")
            >= 1
    );
    assert!(report["documentation_governance"]["recommendations"]
        .as_array()
        .expect("documentation governance recommendations")
        .iter()
        .any(|recommendation| recommendation["kind"] == "review_human_output_candidates"));
    assert!(report["verification_commands"]
        .as_array()
        .unwrap()
        .iter()
        .any(|command| command
            .as_str()
            .unwrap()
            .contains("forager project audit-docs")));
    assert!(report["open_decisions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|decision| decision["kind"] == "documentation_governance_review"));
    assert_eq!(report["summary"]["decision_records_scanned"], 1);
    assert_eq!(report["summary"]["open_decision_records"], 1);
    assert_eq!(report["summary"]["implementation_packets_scanned"], 1);
    assert_eq!(report["summary"]["packet_goals_completed"], 1);
    assert_eq!(report["summary"]["packet_goals_deferred"], 0);
    assert_eq!(report["summary"]["packet_goals_missing"], 0);
    assert_eq!(report["summary"]["packet_goals_drifted"], 0);
    assert_eq!(report["summary"]["packet_detail_items"], 8);
    assert_eq!(report["summary"]["packet_detail_items_completed"], 8);
    assert_eq!(report["summary"]["packet_detail_items_deferred"], 0);
    assert_eq!(report["summary"]["packet_detail_items_missing"], 0);
    assert_eq!(report["summary"]["packet_detail_items_drifted"], 0);
    assert_eq!(report["implementation_packet_coverage"]["packet_count"], 1);
    assert_eq!(report["implementation_packet_coverage"]["completed"], 1);
    assert_eq!(report["implementation_packet_coverage"]["detail_items"], 8);
    assert_eq!(
        report["implementation_packet_coverage"]["detail_items_completed"],
        8
    );
    assert_eq!(
        report["implementation_packet_coverage"]["items"][0]["packet_id"],
        "packet-closeout-test"
    );
    assert_eq!(
        report["implementation_packet_coverage"]["items"][0]["goal_status"],
        "completed"
    );
    assert!(
        report["implementation_packet_coverage"]["items"][0]["reason"]
            .as_str()
            .expect("packet coverage reason")
            .contains("acceptance still depends on closeout review")
    );
    assert_eq!(
        report["implementation_packet_coverage"]["items"][0]["detail_source"],
        "implementation_packet"
    );
    assert_eq!(
        report["implementation_packet_coverage"]["items"][0]["work_slices"]
            .as_array()
            .expect("work slices")
            .len(),
        2
    );
    assert_eq!(
        report["implementation_packet_coverage"]["items"][0]["validation_items"]
            .as_array()
            .expect("validation items")
            .len(),
        3
    );
    assert!(
        report["implementation_packet_coverage"]["items"][0]["validation_items"]
            .as_array()
            .expect("validation items")
            .iter()
            .all(|item| item["status"] == "completed")
    );
    assert_eq!(
        report["implementation_packet_coverage"]["items"][0]["expected_artifacts"]
            .as_array()
            .expect("expected artifacts")
            .len(),
        3
    );
    assert!(
        report["implementation_packet_coverage"]["items"][0]["expected_artifacts"]
            .as_array()
            .expect("expected artifacts")
            .iter()
            .all(|item| item["status"] == "completed")
    );
    assert!(report["decision_records"]
        .as_array()
        .expect("decision records")
        .iter()
        .any(|decision| decision["record"]["decision_id"] == "decision-closeout"));
    assert!(report["open_decisions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|decision| decision["kind"] == "decision_record_review"));
    assert!(report["required_first_reads"]
        .as_array()
        .expect("first reads")
        .iter()
        .any(|read| read["path"]
            .as_str()
            .unwrap_or_default()
            .ends_with("offdesk_decisions.jsonl")));

    let plan_path = PathBuf::from(
        report["artifacts"]["closeout_plan_json"]
            .as_str()
            .expect("plan path"),
    );
    let plan_markdown_path = PathBuf::from(
        report["artifacts"]["closeout_plan_markdown"]
            .as_str()
            .expect("plan markdown path"),
    );
    let manifest_path = PathBuf::from(
        report["artifacts"]["cleanup_manifest_json"]
            .as_str()
            .expect("manifest path"),
    );
    let review_packet_path = PathBuf::from(
        report["artifacts"]["commercial_review_packet"]
            .as_str()
            .expect("review packet path"),
    );
    let return_package_path = PathBuf::from(
        report["artifacts"]["return_package_markdown"]
            .as_str()
            .expect("return package path"),
    );
    assert!(plan_path.exists());
    assert!(plan_markdown_path.exists());
    assert!(manifest_path.exists());
    assert!(review_packet_path.exists());
    assert!(return_package_path.exists());

    let plan = fs::read_to_string(plan_path)?;
    assert!(plan.contains("[REDACTED]"));
    assert!(!plan.contains("sk-secretsecretsecretsecret"));
    let plan_markdown = fs::read_to_string(plan_markdown_path)?;
    assert!(plan_markdown.contains("Implementation Packet Coverage"));
    assert!(plan_markdown.contains("packet-closeout-test"));
    assert!(plan_markdown.contains("detail items: 8 completed"));
    assert!(plan_markdown.contains("validation_items:"));
    assert!(plan_markdown.contains("expected_artifacts:"));
    assert!(plan_markdown
        .contains("Closeout must compare actual output against implementation intent."));

    let manifest: Value = serde_json::from_str(&fs::read_to_string(manifest_path)?)?;
    let operations = manifest.as_array().expect("manifest array");
    assert!(operations
        .iter()
        .any(|operation| operation["operation"] == "archive_candidate"
            && operation["requires_commercial_review"] == true
            && operation["requires_human_approval"] == true));
    assert!(!operations
        .iter()
        .any(|operation| operation["operation"] == "delete_candidate"));

    let review_packet = fs::read_to_string(review_packet_path)?;
    assert!(review_packet.contains("Commercial Model Closeout Review Packet"));
    assert!(review_packet.contains("\"verdict\": \"approved|revise|blocked\""));
    assert!(
        review_packet.contains("\"packet_goal_coverage\": \"completed|deferred|missing|drifted\"")
    );
    assert!(review_packet.contains("Implementation Packet Coverage"));

    let return_package = fs::read_to_string(&return_package_path)?;
    assert!(return_package.contains("Ondesk Return Package"));
    assert!(return_package.contains("## Status"));
    assert!(return_package.contains("implementation packets: 1 scanned; 1 completed"));
    assert!(return_package.contains("packet detail items: 8 completed"));
    assert!(return_package.contains("## Implementation Packet Coverage"));
    assert!(return_package.contains("packet-closeout-test"));
    assert!(return_package.contains("detail_source: `implementation_packet`"));
    assert!(return_package.contains("acceptance still depends on closeout review"));
    assert!(return_package.contains("## Decision Needed"));
    assert!(return_package.contains("Required First Reads"));
    assert!(return_package.contains("Result evidence"));
    assert!(
        return_package.contains("more first-read candidate(s) are listed in `closeout_plan.json`.")
    );
    assert!(return_package.contains("## What Changed"));
    assert!(return_package.contains("## Evidence"));
    assert!(return_package.contains("Kept review evidence"));
    assert!(
        return_package.contains("... 5 more `keep` item(s) are listed in `cleanup_manifest.json`.")
    );
    assert!(return_package.contains("Documentation Governance Recommendations"));
    assert!(return_package.contains("review_human_output_candidates"));
    assert!(return_package.contains("RETENTION_REVIEW.md"));
    assert!(return_package.contains("outputs/doc-report-2.html"));
    assert!(return_package.contains("## Next Safe Action"));
    assert!(return_package.contains("documentation_governance_review"));
    assert!(!return_package.contains("extra-report-7.md"));
    assert!(result_path.exists());
    assert!(log_path.exists());

    let review_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout-review",
            "--closeout-id",
            report["closeout_id"].as_str().expect("closeout id"),
            "--verdict",
            "approved",
            "--reviewer",
            "gpt-5.5",
            "--review-provider",
            "gpt-5.5",
            "--review-file",
            report["artifacts"]["commercial_review_packet"]
                .as_str()
                .expect("review packet"),
            "--unsafe-operation",
            "none",
            "--required-first-read",
            result_path.to_str().expect("utf-8 result path"),
            "--notes",
            "approved after review token=sk-secretsecretsecretsecret",
            "--json",
        ])
        .output()?;
    assert!(
        review_output.status.success(),
        "{}",
        String::from_utf8_lossy(&review_output.stderr)
    );
    let review: Value = serde_json::from_slice(&review_output.stdout)?;
    assert_eq!(review["verdict"], "approved");
    assert_eq!(review["closeout_receipt"]["schema"], "closeout_receipt.v1");
    assert_eq!(
        review["closeout_receipt"]["acceptance_status"],
        "approved_with_followups"
    );
    assert_eq!(
        review["closeout_receipt"]["evidence_status"],
        "review_ready"
    );
    assert_eq!(review["closeout_receipt"]["verification_status"], "pending");
    assert!(review["closeout_receipt"]["open_decisions"]
        .as_array()
        .expect("receipt open decisions")
        .iter()
        .any(|decision| decision["kind"] == "decision_record_review"));
    assert_eq!(review["read_only_project_state"], true);
    assert_eq!(review["applies_file_operations"], false);
    assert_eq!(review["closeout_id"], report["closeout_id"]);
    assert_eq!(review["applies_to_task_ids"][0], "task-closeout");
    assert_eq!(review["applies_to_tasks"][0]["project_key"], "project");
    assert_eq!(review["applies_to_tasks"][0]["task_id"], "task-closeout");
    assert!(review["notes"].as_str().unwrap().contains("[REDACTED]"));
    assert!(!review["notes"]
        .as_str()
        .unwrap()
        .contains("sk-secretsecretsecretsecret"));
    let review_record_path = PathBuf::from(
        review["artifacts"]["review_record_json"]
            .as_str()
            .expect("review record path"),
    );
    let receipt_path = PathBuf::from(
        review["artifacts"]["closeout_receipt_json"]
            .as_str()
            .expect("receipt path"),
    );
    assert!(review_record_path.exists());
    assert!(receipt_path.exists());
    let receipt: Value = serde_json::from_str(&fs::read_to_string(receipt_path)?)?;
    assert_eq!(receipt, review["closeout_receipt"]);
    let updated_return_package = fs::read_to_string(&return_package_path)?;
    assert!(updated_return_package.contains("## Closeout Receipt"));
    assert!(updated_return_package.contains("acceptance_status: `approved_with_followups`"));
    assert!(updated_return_package.contains("next_safe_action"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_closeout_review_receipt_distinguishes_acceptance_from_revision() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let project = temp.path().join("project");
    fs::create_dir_all(&project)?;
    fs::write(project.join("README.md"), "# Project\n")?;
    fs::write(
        project.join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(project.join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        project.join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: project overview.\n",
    )?;
    let result_path = project.join("result.json");
    fs::write(&result_path, "{\"status\":\"ok\"}")?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task-accepted",
                "request_id": "request-accepted",
                "project_key": "accepted",
                "status": "completed",
                "capability_id": "inspect.status",
                "runner_kind": "local_tmux",
                "command": "true",
                "workdir": project.to_str().expect("utf-8 project path"),
                "attempt_count": 1,
                "created_at": now,
                "updated_at": now,
                "preview": "accepted closeout",
                "reason": "closeout receipt test",
                "result_artifact_path": result_path.to_str().expect("utf-8 result path")
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout",
            "--project-key",
            "accepted",
            "--task-id",
            "task-accepted",
            "--workdir",
            project.to_str().expect("utf-8 project path"),
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert!(report["open_decisions"]
        .as_array()
        .expect("open decisions")
        .is_empty());

    let revise_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout-review",
            "--closeout-id",
            report["closeout_id"].as_str().expect("closeout id"),
            "--verdict",
            "revise",
            "--reviewer",
            "operator",
            "--notes",
            "revise before acceptance",
            "--json",
        ])
        .output()?;
    assert!(
        revise_output.status.success(),
        "{}",
        String::from_utf8_lossy(&revise_output.stderr)
    );
    let revise: Value = serde_json::from_slice(&revise_output.stdout)?;
    assert_eq!(
        revise["closeout_receipt"]["acceptance_status"],
        "revision_required"
    );

    let approved_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout-review",
            "--closeout-id",
            report["closeout_id"].as_str().expect("closeout id"),
            "--verdict",
            "approved",
            "--reviewer",
            "operator",
            "--notes",
            "accepted after clean review",
            "--json",
        ])
        .output()?;
    assert!(
        approved_output.status.success(),
        "{}",
        String::from_utf8_lossy(&approved_output.stderr)
    );
    let approved: Value = serde_json::from_slice(&approved_output.stdout)?;
    assert_eq!(
        approved["closeout_receipt"]["acceptance_status"],
        "accepted"
    );
    assert_eq!(
        approved["closeout_receipt"]["verification_status"],
        "recorded"
    );
    let return_package_path = PathBuf::from(
        report["artifacts"]["return_package_markdown"]
            .as_str()
            .expect("return package path"),
    );
    let return_package = fs::read_to_string(return_package_path)?;
    assert!(return_package.contains("acceptance_status: `accepted`"));
    assert!(!return_package.contains("acceptance_status: `revision_required`"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_closeout_decision_resolves_archive_review_without_file_mutation() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let project = temp.path().join("project");
    fs::create_dir_all(&project)?;
    fs::write(project.join("README.md"), "# Project\n")?;
    fs::write(
        project.join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(project.join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        project.join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: project overview.\n",
    )?;
    let artifact_dir = temp.path().join("run-artifacts");
    fs::create_dir_all(&artifact_dir)?;
    let result_path = artifact_dir.join("result.json");
    let log_path = artifact_dir.join("offdesk-runner.log");
    fs::write(&result_path, "{\"status\":\"ok\"}")?;
    fs::write(&log_path, "runner log")?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task-archive-followup",
                "request_id": "request-archive-followup",
                "project_key": "archive-followup",
                "status": "completed",
                "capability_id": "inspect.status",
                "runner_kind": "local_tmux",
                "command": "true",
                "workdir": project.to_str().expect("utf-8 project path"),
                "attempt_count": 1,
                "created_at": now,
                "updated_at": now,
                "preview": "archive followup closeout",
                "reason": "closeout decision test",
                "result_artifact_path": result_path.to_str().expect("utf-8 result path"),
                "log_artifact_path": log_path.to_str().expect("utf-8 log path")
            }
        ]))?,
    )?;

    let closeout_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout",
            "--project-key",
            "archive-followup",
            "--task-id",
            "task-archive-followup",
            "--workdir",
            project.to_str().expect("utf-8 project path"),
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(
        closeout_output.status.success(),
        "{}",
        String::from_utf8_lossy(&closeout_output.stderr)
    );
    let closeout: Value = serde_json::from_slice(&closeout_output.stdout)?;
    assert!(closeout["open_decisions"]
        .as_array()
        .expect("open decisions")
        .iter()
        .any(|decision| decision["kind"] == "archive_review"));

    let review_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout-review",
            "--closeout-id",
            closeout["closeout_id"].as_str().expect("closeout id"),
            "--verdict",
            "approved",
            "--reviewer",
            "operator",
            "--notes",
            "approved but archive decision remains",
            "--json",
        ])
        .output()?;
    assert!(
        review_output.status.success(),
        "{}",
        String::from_utf8_lossy(&review_output.stderr)
    );
    let review: Value = serde_json::from_slice(&review_output.stdout)?;
    assert_eq!(
        review["closeout_receipt"]["acceptance_status"],
        "approved_with_followups"
    );
    assert_eq!(review["closeout_receipt"]["retention_review"], "required");
    assert_eq!(
        review["closeout_receipt"]["open_decisions"][0]["kind"],
        "archive_review"
    );

    let decision_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout-decision",
            "--closeout-id",
            closeout["closeout_id"].as_str().expect("closeout id"),
            "--kind",
            "archive_review",
            "--decision",
            "preserve-in-place",
            "--reviewer",
            "operator",
            "--reason",
            "Preserve the log in place as provenance; no archive or deletion is authorized.",
            "--json",
        ])
        .output()?;
    assert!(
        decision_output.status.success(),
        "{}",
        String::from_utf8_lossy(&decision_output.stderr)
    );
    let decision: Value = serde_json::from_slice(&decision_output.stdout)?;
    assert_eq!(
        decision["closeout_receipt"]["acceptance_status"],
        "accepted"
    );
    assert_eq!(
        decision["closeout_receipt"]["verification_status"],
        "recorded"
    );
    assert_eq!(
        decision["closeout_receipt"]["retention_review"],
        "resolved_preserve_in_place"
    );
    assert!(decision["closeout_receipt"]["open_decisions"]
        .as_array()
        .expect("open decisions")
        .is_empty());
    assert_eq!(
        decision["closeout_receipt"]["resolved_open_decisions"][0]["kind"],
        "archive_review"
    );
    assert_eq!(
        decision["closeout_receipt"]["resolved_open_decisions"][0]["decision"],
        "preserve_in_place"
    );
    assert_eq!(decision["applies_file_operations"], false);
    assert!(log_path.exists());
    let return_package_path = PathBuf::from(
        closeout["artifacts"]["return_package_markdown"]
            .as_str()
            .expect("return package path"),
    );
    let return_package = fs::read_to_string(return_package_path)?;
    assert!(return_package.contains("acceptance_status: `accepted`"));

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(
        status_output.status.success(),
        "{}",
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status: Value = serde_json::from_slice(&status_output.stdout)?;
    assert_eq!(status["closeout_state"]["accepted"], 1);
    assert_eq!(status["closeout_state"]["approved_with_followups"], 0);
    Ok(())
}

#[test]
#[serial]
fn offdesk_closeout_decision_rejects_revision_required_receipts() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let project = temp.path().join("project");
    fs::create_dir_all(&project)?;
    fs::write(project.join("README.md"), "# Project\n")?;
    fs::write(
        project.join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(project.join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        project.join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: project overview.\n",
    )?;
    let artifact_dir = temp.path().join("run-artifacts");
    fs::create_dir_all(&artifact_dir)?;
    let result_path = artifact_dir.join("result.json");
    let log_path = artifact_dir.join("offdesk-runner.log");
    fs::write(&result_path, "{\"status\":\"ok\"}")?;
    fs::write(&log_path, "runner log")?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task-revise-archive-followup",
                "request_id": "request-revise-archive-followup",
                "project_key": "revise-archive-followup",
                "status": "completed",
                "capability_id": "inspect.status",
                "runner_kind": "local_tmux",
                "command": "true",
                "workdir": project.to_str().expect("utf-8 project path"),
                "attempt_count": 1,
                "created_at": now,
                "updated_at": now,
                "preview": "revise archive followup closeout",
                "reason": "closeout decision rejection test",
                "result_artifact_path": result_path.to_str().expect("utf-8 result path"),
                "log_artifact_path": log_path.to_str().expect("utf-8 log path")
            }
        ]))?,
    )?;

    let closeout_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout",
            "--project-key",
            "revise-archive-followup",
            "--task-id",
            "task-revise-archive-followup",
            "--workdir",
            project.to_str().expect("utf-8 project path"),
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(
        closeout_output.status.success(),
        "{}",
        String::from_utf8_lossy(&closeout_output.stderr)
    );
    let closeout: Value = serde_json::from_slice(&closeout_output.stdout)?;
    let review_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout-review",
            "--closeout-id",
            closeout["closeout_id"].as_str().expect("closeout id"),
            "--verdict",
            "revise",
            "--reviewer",
            "operator",
            "--notes",
            "revise before any decision resolution",
            "--json",
        ])
        .output()?;
    assert!(
        review_output.status.success(),
        "{}",
        String::from_utf8_lossy(&review_output.stderr)
    );

    let decision_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout-decision",
            "--closeout-id",
            closeout["closeout_id"].as_str().expect("closeout id"),
            "--kind",
            "archive_review",
            "--decision",
            "preserve-in-place",
            "--reason",
            "This should not bypass revision-required closeout state.",
            "--json",
        ])
        .output()?;
    assert!(!decision_output.status.success());
    assert!(String::from_utf8_lossy(&decision_output.stderr)
        .contains("can only resolve approved closeout receipts with follow-ups"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_closeout_retire_quarantines_historical_revision_without_overwriting_accepted(
) -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let project = temp.path().join("project");
    fs::create_dir_all(&project)?;
    fs::write(project.join("README.md"), "# Project\n")?;
    fs::write(
        project.join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(project.join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        project.join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: project overview.\n",
    )?;
    let artifact_dir = temp.path().join("run-artifacts");
    fs::create_dir_all(&artifact_dir)?;
    let result_a = artifact_dir.join("result-a.json");
    let result_b = artifact_dir.join("result-b.json");
    fs::write(&result_a, "{\"status\":\"a\"}")?;
    fs::write(&result_b, "{\"status\":\"b\"}")?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task-retire-a",
                "request_id": "request-retire-a",
                "project_key": "retire",
                "status": "completed",
                "capability_id": "inspect.status",
                "runner_kind": "local_tmux",
                "command": "true",
                "workdir": project.to_str().expect("utf-8 project path"),
                "attempt_count": 1,
                "created_at": now,
                "updated_at": now,
                "preview": "historical closeout a",
                "reason": "closeout retire test",
                "result_artifact_path": result_a.to_str().expect("utf-8 result path")
            },
            {
                "task_id": "task-retire-b",
                "request_id": "request-retire-b",
                "project_key": "retire",
                "status": "completed",
                "capability_id": "inspect.status",
                "runner_kind": "local_tmux",
                "command": "true",
                "workdir": project.to_str().expect("utf-8 project path"),
                "attempt_count": 1,
                "created_at": now,
                "updated_at": now,
                "preview": "historical closeout b",
                "reason": "closeout retire test",
                "result_artifact_path": result_b.to_str().expect("utf-8 result path")
            }
        ]))?,
    )?;

    let historical_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout",
            "--project-key",
            "retire",
            "--workdir",
            project.to_str().expect("utf-8 project path"),
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(
        historical_output.status.success(),
        "{}",
        String::from_utf8_lossy(&historical_output.stderr)
    );
    let historical: Value = serde_json::from_slice(&historical_output.stdout)?;
    let historical_review = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout-review",
            "--closeout-id",
            historical["closeout_id"].as_str().expect("closeout id"),
            "--verdict",
            "revise",
            "--reviewer",
            "operator",
            "--notes",
            "historical evidence is incomplete",
            "--json",
        ])
        .output()?;
    assert!(
        historical_review.status.success(),
        "{}",
        String::from_utf8_lossy(&historical_review.stderr)
    );

    let accepted_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout",
            "--project-key",
            "retire",
            "--task-id",
            "task-retire-b",
            "--workdir",
            project.to_str().expect("utf-8 project path"),
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(
        accepted_output.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted_output.stderr)
    );
    let accepted_closeout: Value = serde_json::from_slice(&accepted_output.stdout)?;
    let accepted_review = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout-review",
            "--closeout-id",
            accepted_closeout["closeout_id"]
                .as_str()
                .expect("closeout id"),
            "--verdict",
            "approved",
            "--reviewer",
            "operator",
            "--notes",
            "accepted current evidence",
            "--json",
        ])
        .output()?;
    assert!(
        accepted_review.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted_review.stderr)
    );

    let retire_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout-retire",
            "--closeout-id",
            historical["closeout_id"].as_str().expect("closeout id"),
            "--reviewer",
            "operator",
            "--reason",
            "Historical closeout is evidence-incomplete and must not be accepted.",
            "--json",
        ])
        .output()?;
    assert!(
        retire_output.status.success(),
        "{}",
        String::from_utf8_lossy(&retire_output.stderr)
    );
    let retirement: Value = serde_json::from_slice(&retire_output.stdout)?;
    assert_eq!(
        retirement["closeout_receipt"]["acceptance_status"],
        "retired_incomplete"
    );
    assert_eq!(
        retirement["closeout_receipt"]["verification_status"],
        "retired"
    );
    assert_eq!(
        retirement["applies_to_task_ids"]
            .as_array()
            .expect("applies to task ids")
            .len(),
        1
    );
    assert_eq!(retirement["applies_to_task_ids"][0], "task-retire-a");
    assert_eq!(
        retirement["closeout_retirement"]["excluded_accepted_tasks"][0],
        "retire:task-retire-b"
    );

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(
        status_output.status.success(),
        "{}",
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status: Value = serde_json::from_slice(&status_output.stdout)?;
    assert_eq!(status["closeout_required_offdesk_tasks"], 0);
    assert_eq!(status["closeout_state"]["accepted"], 1);
    assert_eq!(status["closeout_state"]["retired_incomplete"], 1);
    assert_eq!(status["closeout_state"]["revision_required"], 0);
    Ok(())
}

#[test]
#[serial]
fn offdesk_closeout_flags_missing_packet_detail_evidence() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    fs::write(temp.path().join("README.md"), "# Project\n")?;
    fs::write(
        temp.path().join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(temp.path().join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        temp.path().join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: project overview.\n",
    )?;

    let artifact_dir = temp.path().join("run-artifacts");
    fs::create_dir_all(&artifact_dir)?;
    let result_path = artifact_dir.join("result.json");
    fs::write(&result_path, "{\"status\":\"ok\"}")?;

    let now = Utc::now();
    let packet_dir = artifact_dir
        .join("implementation_packets")
        .join("packet-missing-detail");
    fs::create_dir_all(&packet_dir)?;
    let packet_path = packet_dir.join("IMPLEMENTATION_PACKET.json");
    let packet_record = json!({
        "schema": "implementation_packet.v1",
        "packet_id": "packet-missing-detail",
        "created_at": now,
        "project_key": "project",
        "project_root": temp.path().to_str().expect("utf-8 temp path"),
        "source_intent": {
            "user_goal": "Closeout should flag missing validation evidence.",
            "why_now": "Acceptance must not hide missing packet detail.",
            "success_state": "Missing validation and expected artifacts are visible."
        },
        "alignment": {
            "north_star_fit": "Evidence remains inspectable.",
            "brand_fit": "Forager keeps supervision separate from acceptance.",
            "product_boundary": "Closeout reports missing evidence only.",
            "anti_drift_notes": []
        },
        "scope": {
            "included": ["missing evidence detection"],
            "excluded": ["accepted truth"],
            "allowed_files": [],
            "mutation_boundary": "closeout artifacts only",
            "non_authorized_actions": ["cleanup"]
        },
        "capability_mapping": [{
            "capability_id": "FD-016",
            "reason": "Packet closeout comparison."
        }],
        "design": {
            "approach": "Detect unmatched validation evidence.",
            "work_slices": ["missing detail signal"],
            "interfaces": [],
            "data_contracts": ["implementation_packet_coverage"],
            "compatibility_notes": []
        },
        "execution": {
            "preferred_worker": "hosted_harness",
            "worker_requirements": [],
            "commands": [],
            "stop_conditions": ["missing validation artifact"],
            "rollback_or_recovery": []
        },
        "validation": {
            "tests": ["missing-validation.txt"],
            "smoke_checks": [],
            "manual_review": [],
            "evidence_required": []
        },
        "closeout": {
            "expected_artifacts": ["missing-validation.txt"],
            "accepted_truth_rule": "Execution evidence is not accepted truth.",
            "handoff_summary_requirements": []
        },
        "recursive_alignment_review": {
            "schema": "recursive_alignment_review.v1",
            "reviewer": "deterministic_gate",
            "outcome": "pass",
            "checks": {
                "original_goal_coverage": "complete",
                "north_star_alignment": "acceptable",
                "brand_alignment": "acceptable",
                "scope_balance": "right_sized",
                "capability_coverage": "complete",
                "evidence_sufficiency": "sufficient",
                "completion_definition": "testable"
            },
            "drift_signals": [],
            "missing_decisions": [],
            "required_revisions": [],
            "safe_to_delegate": true
        }
    });
    fs::write(&packet_path, serde_json::to_string_pretty(&packet_record)?)?;
    let alignment_path = packet_dir.join("RECURSIVE_ALIGNMENT_REVIEW.json");
    let packet_markdown_path = packet_dir.join("IMPLEMENTATION_PACKET.md");
    fs::write(&alignment_path, "{}")?;
    fs::write(&packet_markdown_path, "# Implementation Packet\n")?;
    let packet_summary = json!({
        "packet_id": "packet-missing-detail",
        "created_at": now,
        "project_key": "project",
        "artifact_dir": packet_dir.to_str().expect("utf-8 packet dir"),
        "packet_path": packet_path.to_str().expect("utf-8 packet path"),
        "alignment_review_path": alignment_path.to_str().expect("utf-8 alignment path"),
        "markdown_path": packet_markdown_path.to_str().expect("utf-8 packet markdown path"),
        "goal": "Closeout should flag missing validation evidence.",
        "success_state": "Missing validation and expected artifacts are visible.",
        "preferred_worker": "hosted_harness",
        "safe_to_delegate": true,
        "outcome": "pass",
        "required_revisions": [],
        "drift_signals": [],
        "missing_decisions": [],
        "work_slice_count": 1,
        "capability_mapping_count": 1,
        "validation_item_count": 1,
        "stop_condition_count": 1,
        "expected_artifact_count": 1
    });
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task-missing-detail",
                "request_id": "request-missing-detail",
                "project_key": "project",
                "status": "completed",
                "capability_id": "inspect.status",
                "runner_kind": "local_tmux",
                "command": "true",
                "workdir": temp.path().to_str().expect("utf-8 temp path"),
                "attempt_count": 1,
                "created_at": now,
                "updated_at": now,
                "preview": "missing detail closeout",
                "reason": "closeout detail test",
                "implementation_packet": packet_summary,
                "result_artifact_path": result_path.to_str().expect("utf-8 result path")
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout",
            "--project-key",
            "project",
            "--task-id",
            "task-missing-detail",
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["summary"]["packet_goals_completed"], 1);
    assert_eq!(report["summary"]["packet_detail_items_missing"], 2);
    assert!(report["open_decisions"]
        .as_array()
        .expect("open decisions")
        .iter()
        .any(|decision| decision["kind"] == "implementation_packet_coverage_review"));
    assert!(
        report["implementation_packet_coverage"]["items"][0]["validation_items"]
            .as_array()
            .expect("validation items")
            .iter()
            .any(|item| item["label"] == "missing-validation.txt" && item["status"] == "missing")
    );
    assert!(
        report["implementation_packet_coverage"]["items"][0]["expected_artifacts"]
            .as_array()
            .expect("expected artifacts")
            .iter()
            .any(|item| item["label"] == "missing-validation.txt" && item["status"] == "missing")
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_closeout_uses_work_slice_execution_receipts() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    fs::write(temp.path().join("README.md"), "# Project\n")?;
    fs::write(
        temp.path().join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(temp.path().join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        temp.path().join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: project overview.\n",
    )?;
    fs::write(temp.path().join(".gitignore"), ".config/\nrun-artifacts/\n")?;
    initialize_git_repo_with_initial_commit(
        temp.path(),
        &[
            ".gitignore",
            "README.md",
            "PROJECT_STATE.md",
            "DECISIONS.md",
            "DELIVERABLES.md",
        ],
    )?;
    fs::write(
        temp.path().join("README.md"),
        "# Project\n\nSource observation changed this file.\n",
    )?;

    let artifact_dir = temp.path().join("run-artifacts");
    fs::create_dir_all(&artifact_dir)?;
    let result_path = artifact_dir.join("result.json");
    fs::write(&result_path, "{\"status\":\"ok\"}")?;

    let now = Utc::now();
    let packet_dir = artifact_dir
        .join("implementation_packets")
        .join("packet-slice-receipts");
    fs::create_dir_all(&packet_dir)?;
    let packet_path = packet_dir.join("IMPLEMENTATION_PACKET.json");
    let alignment_path = packet_dir.join("RECURSIVE_ALIGNMENT_REVIEW.json");
    let packet_markdown_path = packet_dir.join("IMPLEMENTATION_PACKET.md");
    let packet_record = json!({
        "schema": "implementation_packet.v1",
        "packet_id": "packet-slice-receipts",
        "created_at": now,
        "project_key": "project",
        "project_root": temp.path().to_str().expect("utf-8 temp path"),
        "source_intent": {
            "user_goal": "Closeout should judge each work slice from execution receipts.",
            "why_now": "Packet-level completion hides slice-level drift.",
            "success_state": "Ondesk can see which slice drifted or only has a worker claim before accepting the run."
        },
        "alignment": {
            "north_star_fit": "Slice receipts keep evidence, choices, and continuity itemized.",
            "brand_fit": "Forager remains the local supervisor rather than trusting worker prose.",
            "product_boundary": "Receipts report execution only; closeout acceptance remains separate.",
            "anti_drift_notes": []
        },
        "scope": {
            "included": ["slice receipt coverage"],
            "excluded": ["accepted truth"],
            "allowed_files": [],
            "mutation_boundary": "closeout artifacts only",
            "non_authorized_actions": ["cleanup"]
        },
        "capability_mapping": [{
            "capability_id": "FD-016",
            "reason": "Work-slice execution receipts."
        }],
        "design": {
            "approach": "Use slice receipts before packet-level inherited status.",
            "work_slices": ["receipt completed slice", "receipt drifted slice", "receipt worker claim slice"],
            "interfaces": [],
            "data_contracts": ["work_slice_execution_receipt.v1"],
            "compatibility_notes": []
        },
        "execution": {
            "preferred_worker": "hosted_harness",
            "worker_requirements": [],
            "commands": [],
            "stop_conditions": ["missing slice receipt"],
            "rollback_or_recovery": []
        },
        "validation": {
            "tests": [],
            "smoke_checks": [],
            "manual_review": [],
            "evidence_required": []
        },
        "closeout": {
            "expected_artifacts": [],
            "accepted_truth_rule": "Execution evidence is not accepted truth.",
            "handoff_summary_requirements": []
        },
        "recursive_alignment_review": {
            "schema": "recursive_alignment_review.v1",
            "reviewer": "deterministic_gate",
            "outcome": "pass",
            "checks": {
                "original_goal_coverage": "complete",
                "north_star_alignment": "acceptable",
                "brand_alignment": "acceptable",
                "scope_balance": "right_sized",
                "capability_coverage": "complete",
                "evidence_sufficiency": "sufficient",
                "completion_definition": "testable"
            },
            "drift_signals": [],
            "missing_decisions": [],
            "required_revisions": [],
            "safe_to_delegate": true
        }
    });
    fs::write(&packet_path, serde_json::to_string_pretty(&packet_record)?)?;
    fs::write(&alignment_path, "{}")?;
    fs::write(&packet_markdown_path, "# Implementation Packet\n")?;

    let receipts = [
        json!({
            "schema": "work_slice_execution_receipt.v1",
            "packet_id": "packet-slice-receipts",
            "project_key": "project",
            "task_id": "task-slice-receipts",
            "generated_at": now,
            "producer": "deterministic_gate",
            "producer_role": "deterministic_verification",
            "slice_index": 0,
            "slice_label": "receipt completed slice",
            "status": "completed",
            "verification_status": "validation_passed",
            "verification_summary": "A deterministic validation log confirms this slice.",
            "verification_refs": ["slice-complete-validation.log"],
            "summary": "The completed slice produced the expected state.",
            "evidence_refs": ["slice-complete-result.json"],
            "validation_refs": [],
            "artifact_refs": ["slice-complete-result.json"],
            "open_questions": [],
            "drift_signals": [],
            "next_safe_action": "Review the closeout package."
        }),
        json!({
            "schema": "work_slice_execution_receipt.v1",
            "packet_id": "packet-slice-receipts",
            "project_key": "project",
            "task_id": "task-slice-receipts",
            "generated_at": now,
            "producer": "worker",
            "producer_role": "worker_claim",
            "slice_index": 1,
            "slice_label": "receipt drifted slice",
            "status": "drifted",
            "claim_status": "drifted",
            "summary": "The worker changed the slice boundary before collecting evidence.",
            "evidence_refs": ["slice-drift-log.txt"],
            "validation_refs": [],
            "artifact_refs": [],
            "open_questions": ["Confirm whether the new slice boundary is acceptable."],
            "drift_signals": ["slice_boundary_changed_without_packet_update"],
            "next_safe_action": "Revise the packet or rerun this slice before accepting truth."
        }),
        json!({
            "schema": "work_slice_execution_receipt.v1",
            "packet_id": "packet-slice-receipts",
            "project_key": "project",
            "task_id": "task-slice-receipts",
            "generated_at": now,
            "producer": "worker",
            "producer_role": "worker_claim",
            "slice_index": 2,
            "slice_label": "receipt worker claim slice",
            "status": "completed",
            "claim_status": "completed",
            "verification_status": "unverified",
            "summary": "The worker says this slice is complete, but no independent verification is attached.",
            "evidence_refs": ["worker-report.md"],
            "validation_refs": [],
            "artifact_refs": [],
            "open_questions": ["Reconcile the worker report against diff, tests, and artifacts."],
            "drift_signals": [],
            "next_safe_action": "Verify source diff, tests, and artifacts before accepting this completed claim."
        }),
    ];
    fs::write(
        artifact_dir.join("work_slice_receipts.jsonl"),
        receipts
            .iter()
            .map(serde_json::to_string)
            .collect::<std::result::Result<Vec<_>, _>>()?
            .join("\n")
            + "\n",
    )?;

    let packet_summary = json!({
        "packet_id": "packet-slice-receipts",
        "created_at": now,
        "project_key": "project",
        "artifact_dir": packet_dir.to_str().expect("utf-8 packet dir"),
        "packet_path": packet_path.to_str().expect("utf-8 packet path"),
        "alignment_review_path": alignment_path.to_str().expect("utf-8 alignment path"),
        "markdown_path": packet_markdown_path.to_str().expect("utf-8 packet markdown path"),
        "goal": "Closeout should judge each work slice from execution receipts.",
        "success_state": "Ondesk can see which slice drifted or only has a worker claim before accepting the run.",
        "preferred_worker": "hosted_harness",
        "safe_to_delegate": true,
        "outcome": "pass",
        "required_revisions": [],
        "drift_signals": [],
        "missing_decisions": [],
        "work_slice_count": 3,
        "capability_mapping_count": 1,
        "validation_item_count": 0,
        "stop_condition_count": 1,
        "expected_artifact_count": 0
    });
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task-slice-receipts",
                "request_id": "request-slice-receipts",
                "project_key": "project",
                "status": "completed",
                "capability_id": "inspect.status",
                "runner_kind": "local_tmux",
                "command": "true",
                "workdir": temp.path().to_str().expect("utf-8 temp path"),
                "attempt_count": 1,
                "created_at": now,
                "updated_at": now,
                "preview": "slice receipt closeout",
                "reason": "slice receipt test",
                "implementation_packet": packet_summary,
                "result_artifact_path": result_path.to_str().expect("utf-8 result path")
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout",
            "--project-key",
            "project",
            "--task-id",
            "task-slice-receipts",
            "--dry-run",
            "--include-git",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["summary"]["packet_goals_completed"], 1);
    assert_eq!(report["summary"]["packet_detail_items"], 3);
    assert_eq!(report["summary"]["packet_detail_items_completed"], 1);
    assert_eq!(report["summary"]["packet_detail_items_deferred"], 1);
    assert_eq!(report["summary"]["packet_detail_items_drifted"], 1);
    assert!(report["open_decisions"]
        .as_array()
        .expect("open decisions")
        .iter()
        .any(|decision| decision["kind"] == "implementation_packet_coverage_review"));
    assert_eq!(
        report["source_observation"]["schema"],
        "source_observation.v1"
    );
    assert_eq!(report["source_observation"]["status"], "observed");
    assert!(report["source_observation"]["changed_files"]
        .as_array()
        .expect("changed files")
        .iter()
        .any(|file| file["path"] == "README.md" && file["status"] == "modified"));
    assert_eq!(
        report["implementation_packet_coverage"]["items"][0]["detail_source"],
        "implementation_packet_and_work_slice_receipts"
    );
    let work_slices = report["implementation_packet_coverage"]["items"][0]["work_slices"]
        .as_array()
        .expect("work slices");
    assert!(work_slices.iter().any(|item| {
        item["label"] == "receipt completed slice"
            && item["status"] == "completed"
            && item["trust_tier"] == "source_verified"
            && item["verification_status"] == "validation_passed"
            && item["receipt_source"]
                .as_str()
                .unwrap_or_default()
                .ends_with("work_slice_receipts.jsonl")
    }));
    assert!(work_slices.iter().any(|item| {
        item["label"] == "receipt drifted slice"
            && item["status"] == "drifted"
            && item["next_safe_action"]
                == "Revise the packet or rerun this slice before accepting truth."
            && item["drift_signals"]
                .as_array()
                .expect("drift signals")
                .iter()
                .any(|signal| signal == "slice_boundary_changed_without_packet_update")
    }));
    assert!(work_slices.iter().any(|item| {
        item["label"] == "receipt worker claim slice"
            && item["status"] == "deferred"
            && item["reported_status"] == "completed"
            && item["claim_status"] == "completed"
            && item["trust_tier"] == "worker_claim"
            && item["source_observation_status"] == "observed"
            && item["source_refs"]
                .as_array()
                .expect("source refs")
                .iter()
                .any(|source_ref| source_ref == "source:git:modified:README.md")
            && item["reason"]
                .as_str()
                .unwrap_or_default()
                .contains("independent source or review verification")
    }));

    let return_package_path = PathBuf::from(
        report["artifacts"]["return_package_markdown"]
            .as_str()
            .expect("return package path"),
    );
    let return_package = fs::read_to_string(return_package_path)?;
    assert!(return_package.contains("work_slices:"));
    assert!(return_package.contains("[drifted] receipt drifted slice"));
    assert!(return_package.contains("[deferred] receipt worker claim slice"));
    assert!(return_package.contains("claim: completed"));
    assert!(return_package.contains("trust: worker_claim"));
    assert!(return_package.contains("Source Observation"));
    assert!(return_package.contains("[modified] `README.md`"));
    assert!(return_package.contains("source: observed"));
    assert!(
        return_package.contains("Revise the packet or rerun this slice before accepting truth.")
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_closeout_prefers_prepared_repo_for_documentation_governance() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;

    let harness_dir = temp.path().join("harness");
    let run_dir = harness_dir.join("target").join("offdesk-run");
    fs::create_dir_all(&run_dir)?;
    fs::write(harness_dir.join("README.md"), "# Harness\n")?;
    fs::write(
        harness_dir.join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(harness_dir.join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        harness_dir.join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: harness overview.\n",
    )?;

    let target_repo = temp.path().join("target-repo");
    fs::create_dir_all(target_repo.join("outputs"))?;
    fs::write(target_repo.join("README.md"), "# Target Repo\n")?;
    fs::write(
        target_repo.join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(target_repo.join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        target_repo.join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: target overview.\n",
    )?;
    fs::write(
        target_repo.join("outputs").join("target-report.html"),
        "<h1>target</h1>",
    )?;

    let result_path = run_dir.join("result.json");
    let log_path = run_dir.join("runner.log");
    fs::write(&result_path, "{\"status\":\"ok\"}")?;
    fs::write(&log_path, "runner log")?;
    fs::write(
        run_dir.join("prepared_task.json"),
        serde_json::to_string_pretty(&json!({
            "project_key": "target",
            "repo": target_repo.to_str().expect("utf-8 target repo"),
            "out_dir": run_dir.to_str().expect("utf-8 run dir")
        }))?,
    )?;

    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task-target",
                "request_id": "request-target",
                "project_key": "target",
                "status": "completed",
                "capability_id": "dispatch.runtime",
                "runner_kind": "local_tmux",
                "command": "bash run_workload.sh",
                "workdir": harness_dir.to_str().expect("utf-8 harness path"),
                "background_ticket_id": "ticket-target",
                "attempt_count": 1,
                "created_at": now,
                "updated_at": now,
                "log_artifact_path": log_path.to_str().expect("utf-8 log path"),
                "result_artifact_path": result_path.to_str().expect("utf-8 result path")
            }
        ]))?,
    )?;
    fs::write(
        profile_dir.join("background_runs.json"),
        serde_json::to_string_pretty(&json!([
            {
                "ticket_id": "ticket-target",
                "project_key": "target",
                "request_id": "request-target",
                "task_id": "task-target",
                "runner_kind": "local_tmux",
                "phase": "completed",
                "working_dir": harness_dir.to_str().expect("utf-8 harness path"),
                "log_artifact_path": log_path.to_str().expect("utf-8 log path"),
                "result_artifact_path": result_path.to_str().expect("utf-8 result path"),
                "runtime_handle_alive": false,
                "log_artifact_present": true,
                "result_artifact_present": true
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .current_dir(&harness_dir)
        .args([
            "offdesk",
            "closeout",
            "--project-key",
            "target",
            "--task-id",
            "task-target",
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        report["documentation_governance"]["workdir"],
        target_repo.to_string_lossy().as_ref()
    );
    assert!(report["documentation_governance"]["recommendations"]
        .as_array()
        .expect("documentation governance recommendations")
        .iter()
        .any(|recommendation| recommendation["kind"] == "review_human_output_candidates"));

    let return_package_path = PathBuf::from(
        report["artifacts"]["return_package_markdown"]
            .as_str()
            .expect("return package path"),
    );
    let return_package = fs::read_to_string(return_package_path)?;
    assert!(return_package.contains("target-report.html"));
    assert!(return_package.contains(target_repo.to_str().expect("utf-8 target repo")));
    assert!(!return_package.contains("harness overview"));
    Ok(())
}
