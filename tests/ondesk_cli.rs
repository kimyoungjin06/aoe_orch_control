use anyhow::Result;
use chrono::{Duration, Utc};
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

fn offdesk_task_fixture(
    task_id: &str,
    status: &str,
    now: chrono::DateTime<Utc>,
) -> serde_json::Value {
    json!({
        "task_id": task_id,
        "request_id": format!("request-{task_id}"),
        "project_key": "project",
        "status": status,
        "capability_id": "dispatch.runtime",
        "runner_kind": "local_background",
        "command": "true",
        "workdir": "/tmp",
        "created_at": now,
        "updated_at": now
    })
}

#[test]
#[serial]
fn ondesk_note_json_redacts_and_persists_note() -> Result<()> {
    let temp = tempdir()?;
    let output = forager_command(temp.path())
        .args([
            "ondesk",
            "note",
            "--project-key",
            "twinpaper",
            "--mode",
            "writing",
            "--text",
            "draft note token=sk-secretsecretsecretsecret",
            "--json",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(json["project_key"], "twinpaper");
    assert_eq!(json["mode"], "writing");

    let notes = fs::read_to_string(profile_dir(temp.path()).join("ondesk_notes.jsonl"))?;
    assert!(notes.contains("twinpaper"));
    assert!(notes.contains("writing"));
    assert!(notes.contains("[REDACTED]"));
    assert!(!notes.contains("sk-secretsecretsecretsecret"));
    Ok(())
}

#[test]
#[serial]
fn ondesk_prompt_package_uses_recent_redacted_notes() -> Result<()> {
    let temp = tempdir()?;
    let note_output = forager_command(temp.path())
        .args([
            "ondesk",
            "note",
            "--project-key",
            "research",
            "--mode",
            "analysis",
            "--text",
            "compare evidence before claim token=sk-secretsecretsecretsecret",
        ])
        .output()?;
    assert!(
        note_output.status.success(),
        "{}",
        String::from_utf8_lossy(&note_output.stderr)
    );

    let output = forager_command(temp.path())
        .args([
            "ondesk",
            "prompt-package",
            "--project-key",
            "research",
            "--mode",
            "analysis",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(json["project_key"], "research");
    assert_eq!(json["note_count"], 1);
    let content = json["content"].as_str().expect("content string");
    assert!(content.contains("compare evidence before claim"));
    assert!(content.contains("Instructions For The Next Harness"));
    assert!(content.contains("[REDACTED]"));
    assert!(!content.contains("sk-secretsecretsecretsecret"));
    Ok(())
}

#[test]
#[serial]
fn ondesk_review_surface_json_agrees_with_status_next_safe_action() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            offdesk_task_fixture("approval-task", "pending_approval", now),
            offdesk_task_fixture("completed-task", "completed", now),
            offdesk_task_fixture("running-task", "running", now)
        ]))?,
    )?;

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(
        status_output.status.success(),
        "{}",
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status_json: Value = serde_json::from_slice(&status_output.stdout)?;

    let surface_output = forager_command(temp.path())
        .args([
            "ondesk",
            "review-surface",
            "--project-key",
            "project",
            "--json",
        ])
        .output()?;
    assert!(
        surface_output.status.success(),
        "{}",
        String::from_utf8_lossy(&surface_output.stderr)
    );
    let surface: Value = serde_json::from_slice(&surface_output.stdout)?;

    assert_eq!(surface["schema"], "review_surface.v1");
    assert_eq!(surface["project_key"], "project");
    assert_eq!(surface["status"]["label"], "needs_review");
    assert_eq!(
        surface["next_safe_actions"][0],
        status_json["offdesk_next_safe_actions"][0]
    );
    assert_eq!(surface["sources"]["status_json"], "forager status --json");
    assert_eq!(surface["sources"]["artifact_index"], "artifact_index.v1");
    assert_eq!(
        surface["sources"]["artifact_retention_review"],
        "artifact_retention_review.v1"
    );
    assert_eq!(surface["artifacts"]["index"]["schema"], "artifact_index.v1");
    assert_eq!(
        surface["artifacts"]["retention_review"]["schema"],
        "artifact_retention_review.v1"
    );
    Ok(())
}

#[test]
#[serial]
fn ondesk_workstation_surface_json_projects_current_status_into_dashboard() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let mut approval_task = offdesk_task_fixture("approval-task", "pending_approval", now);
    approval_task["attempt_count"] = json!(1);
    approval_task["last_gate_status"] = json!("pending_approval");
    approval_task["artifact_refs"] = json!([
        {
            "artifact_id": "task-log",
            "path": "artifacts/approval-task.log",
            "present": true
        },
        {
            "artifact_id": "task-result",
            "path": "artifacts/approval-task-result.json",
            "present": false
        }
    ]);
    approval_task["log_artifact_path"] = json!("artifacts/approval-task.log");
    approval_task["provider_id"] = json!("local-llm");
    approval_task["model"] = json!("qwen-coder");
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            approval_task,
            offdesk_task_fixture("completed-task", "completed", now)
        ]))?,
    )?;
    fs::write(
        profile_dir.join("runtime_dispatch_receipts.jsonl"),
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "schema": "runtime_dispatch_receipt.v1",
                "receipt_id": "runtime-receipt-approval-task",
                "preflight_id": "runtime-preflight-approval-task",
                "source_closeout_id": "closeout-approval-task",
                "task_id": "approval-task",
                "recorded_at": now + Duration::minutes(2),
                "result_status": "queued",
                "reason": "Runtime dispatch queued approval-task."
            }))?
        ),
    )?;
    let closeout_dir = profile_dir.join("offdesk_closeouts").join("completed-task");
    fs::create_dir_all(&closeout_dir)?;
    fs::write(
        closeout_dir.join("closeout_plan.json"),
        serde_json::to_string_pretty(&json!({
            "schema": "closeout_plan.v1",
            "closeout_id": "closeout-completed-task",
            "generated_at": now,
            "tasks": [{
                "project_key": "project",
                "request_id": "request-completed-task",
                "task_id": "completed-task"
            }]
        }))?,
    )?;
    fs::write(
        closeout_dir.join("closeout_review_20260618T000000Z.json"),
        serde_json::to_string_pretty(&json!({
            "schema": "closeout_review.v1",
            "reviewed_at": now + Duration::minutes(1),
            "review_id": "review-followup",
            "closeout_id": "closeout-completed-task",
            "verdict": "approved",
            "applies_to_tasks": [{
                "project_key": "project",
                "request_id": "request-completed-task",
                "task_id": "completed-task"
            }],
            "closeout_receipt": {
                "schema": "closeout_receipt.v1",
                "receipt_id": "receipt-followup",
                "closeout_id": "closeout-completed-task",
                "acceptance_status": "approved_with_followups",
                "verification_status": "pending",
                "evidence_status": "present",
                "retention_review": "required",
                "wiki_promotion_state": "not_required",
                "stale_task_count": 0,
                "next_safe_action": "Resolve archive review before accepting truth.",
                "open_decisions": [{
                    "kind": "archive_review",
                    "summary": "Archive decision is still open."
                }]
            }
        }))?,
    )?;
    let pending_decision = json!({
        "schema": "decision_record.v1",
        "decision_id": "decision-user",
        "project_key": "project",
        "request_id": "request",
        "task_id": "approval-task",
        "raised_by": "agent",
        "source_surface": "offdesk.council",
        "materiality": "high",
        "status": "user_pending",
        "created_at": now,
        "updated_at": now,
        "decision_request": {
            "kind": "council_escalation",
            "summary": "Council recommends revising the next episode.",
            "decision_needed": "Choose whether to continue, revise, block, or stop.",
            "why_now": ["Council did not return continue."],
            "current_scope": "Next episode only.",
            "non_authorized_scope": [
                "provider retargeting",
                "cleanup",
                "wiki promotion"
            ],
            "options": [
                {
                    "id": "revise",
                    "label": "Revise",
                    "description": "Ask the agent to revise the plan."
                },
                {
                    "id": "block",
                    "label": "Block",
                    "description": "Keep the run blocked."
                }
            ]
        },
        "route": {
            "materiality": "high",
            "target": "user",
            "reason": "The next episode direction changes.",
            "default_if_no_reply": "defer"
        },
        "approval_brief": {
            "schema": "approval_brief.v1",
            "recommendation": "revise",
            "subject": "council continuation decision",
            "summary_lines": ["Council recommends revising before continuing."],
            "scope": "Only approves the next episode direction.",
            "question": "How should the run proceed?"
        }
    });
    fs::write(
        profile_dir.join("offdesk_decisions.jsonl"),
        format!("{}\n", serde_json::to_string(&pending_decision)?),
    )?;

    let output = forager_command(temp.path())
        .args(["ondesk", "workstation-surface", "--json"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let surface: Value = serde_json::from_slice(&output.stdout)?;

    assert_eq!(surface["schema"], "workstation_surface.v1");
    assert_eq!(surface["source_label"], "Live workstation_surface.v1");
    assert_eq!(surface["redaction"]["operator_safe"], true);
    assert_eq!(
        surface["source_refs"]["status_json"],
        "forager status --json"
    );
    assert_eq!(surface["attention_counts"]["pending_decisions"], 1);
    assert_eq!(surface["attention_counts"]["closeout_required"], 1);
    assert_eq!(surface["top_attention"]["kind"], "decision_inbox");
    assert_eq!(surface["projects"][0]["project_key"], "project");
    assert_eq!(surface["projects"][0]["decisions"], 1);
    assert_eq!(
        surface["projects"][0]["task_items"][0]["task_id"],
        "approval-task"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["kind"],
        "Approval task"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["status"],
        "pending_approval"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["reference"],
        "offdesk_tasks.json#approval-task"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["command"],
        "forager offdesk pending"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["next_safe_action_kind"],
        "approval_pending"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["requires_operator_review"],
        true
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["inspection_items"][0]["label"],
        "Runner"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["inspection_items"][0]["value"],
        "local_background / dispatch.runtime"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["inspection_items"][2]["label"],
        "Attempts"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["inspection_items"][2]["value"],
        "1"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["inspection_items"][3]["value"],
        "pending_approval"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["inspection_items"][4]["value"],
        "1/2 refs; log ready; result missing"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["inspection_items"][6]["value"],
        "local-llm / qwen-coder"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["receipt_links"][0]["source"],
        "runtime_dispatch"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["receipt_links"][0]["record_id"],
        "runtime-receipt-approval-task"
    );
    assert_eq!(
        surface["projects"][0]["task_items"][0]["receipt_links"][0]["result_status"],
        "queued"
    );
    assert_eq!(surface["chat_context"]["schema"], "chat_context_surface.v1");
    assert_eq!(surface["chat_context"]["mode"], "read_only_cited_answer");
    assert_eq!(surface["chat_context"]["scopes"][0]["scope_id"], "overview");
    assert!(surface["chat_context"]["scopes"][0]["answer"]
        .as_str()
        .expect("overview answer")
        .contains("open decision"));
    assert_eq!(
        surface["chat_context"]["scopes"][0]["suggested_actions"][0]["label"],
        "Open decision inbox"
    );
    let chat_scopes = surface["chat_context"]["scopes"]
        .as_array()
        .expect("chat scopes");
    let decision_scope = chat_scopes
        .iter()
        .find(|scope| scope["scope_id"] == "decision:decision-user")
        .expect("decision chat scope");
    assert!(decision_scope["answer"]
        .as_str()
        .expect("decision answer")
        .contains("awaiting operator judgment"));
    assert_eq!(
        decision_scope["suggested_actions"][0]["label"],
        "Validate action envelope"
    );
    let project_scope = chat_scopes
        .iter()
        .find(|scope| scope["scope_id"] == "project:project")
        .expect("project chat scope");
    assert!(project_scope["citations"]
        .as_array()
        .expect("project citations")
        .iter()
        .any(
            |citation| citation["reference"] == "runtime-receipt-approval-task"
                && citation["trust"] == "receipt-backed"
        ));
    assert_eq!(
        surface["decision_inbox"]["schema"],
        "decision_inbox_surface.v1"
    );
    assert_eq!(surface["decision_inbox"]["open_count"], 1);
    assert_eq!(surface["decision_inbox"]["visible_count"], 1);
    assert_eq!(
        surface["decision_inbox"]["action_model"]["mode"],
        "read_only_preview"
    );
    assert_eq!(
        surface["decision_inbox"]["action_model"]["direct_input_allowed"],
        true
    );
    assert_eq!(
        surface["decision_inbox"]["items"][0]["decision_id"],
        "decision-user"
    );
    assert_eq!(
        surface["decision_inbox"]["items"][0]["what_changed"],
        "Council recommends revising before continuing."
    );
    assert!(
        surface["decision_inbox"]["items"][0]["authorization_boundary"]
            .as_str()
            .expect("boundary")
            .contains("Not authorized")
    );
    assert_eq!(
        surface["decision_inbox"]["items"][0]["cli_fallback"],
        "forager offdesk decisions --json"
    );
    assert_eq!(
        surface["decision_inbox"]["items"][0]["action_envelopes"][0]["schema"],
        "action_envelope.v1"
    );
    assert_eq!(
        surface["decision_inbox"]["items"][0]["action_envelopes"][0]["target_ref"]["decision_id"],
        "decision-user"
    );
    assert_eq!(
        surface["decision_inbox"]["items"][0]["action_envelopes"][0]["allowed_command"],
        "forager offdesk decision show --json decision-user"
    );
    assert!(
        surface["decision_inbox"]["items"][0]["action_envelopes"][0]["observed_hash"]
            .as_str()
            .expect("observed hash")
            .starts_with("sha256:")
    );
    assert_eq!(
        surface["decision_inbox"]["items"][0]["action_envelopes"][0]["expected_receipt_schema"],
        "action_envelope_receipt.v1"
    );
    assert!(
        surface["decision_inbox"]["items"][0]["action_envelopes"][0]["forbidden_effects"]
            .as_array()
            .expect("forbidden effects")
            .contains(&json!("arbitrary_shell"))
    );
    assert_eq!(
        surface["accepted_truth_recovery"]["schema"],
        "accepted_truth_recovery_surface.v1"
    );
    assert_eq!(surface["accepted_truth_recovery"]["candidate_count"], 1);
    let truth_item = &surface["accepted_truth_recovery"]["items"][0];
    assert_eq!(truth_item["stage"], "followup_required");
    assert_eq!(truth_item["acceptance_status"], "approved_with_followups");
    assert_eq!(truth_item["open_decision_kinds"], json!(["archive_review"]));
    assert_eq!(
        truth_item["resolve_command"],
        "forager offdesk closeout-decision --closeout-id closeout-completed-task --kind archive_review --decision preserve-in-place --reason <reason> --json"
    );
    assert_eq!(
        truth_item["retire_command"],
        "forager offdesk closeout-retire --closeout-id closeout-completed-task --reason <reason> --json"
    );
    assert_eq!(
        truth_item["action_envelopes"][0]["schema"],
        "accepted_truth_recovery_action_envelope.v1"
    );
    assert_eq!(
        truth_item["action_envelopes"][0]["action_kind"],
        "resolve_followup"
    );
    assert_eq!(
        truth_item["action_envelopes"][0]["target_ref"]["closeout_id"],
        "closeout-completed-task"
    );
    assert_eq!(
        truth_item["action_envelopes"][0]["expected_receipt_schema"],
        "accepted_truth_recovery_action_receipt.v1"
    );
    assert!(truth_item["action_envelopes"][0]["forbidden_effects"]
        .as_array()
        .expect("recovery forbidden effects")
        .contains(&json!("wiki_promotion")));
    assert_eq!(surface["decisions"][0]["decision_id"], "decision-user");
    // allowed_actions carry option IDs (the executor's action vocabulary),
    // not display labels; slugged labels were rejected by action-decision.
    assert_eq!(
        surface["decisions"][0]["allowed_actions"],
        json!(["revise", "block"])
    );
    assert_eq!(surface["graph_focus"]["title"], "Selected provenance path");
    Ok(())
}

#[test]
#[serial]
fn ondesk_action_envelope_records_valid_and_stale_receipts() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let pending_decision = json!({
        "schema": "decision_record.v1",
        "decision_id": "decision-user",
        "project_key": "project",
        "request_id": "request",
        "task_id": "approval-task",
        "raised_by": "agent",
        "source_surface": "offdesk.council",
        "materiality": "high",
        "status": "user_pending",
        "created_at": now,
        "updated_at": now,
        "decision_request": {
            "kind": "council_escalation",
            "summary": "Council recommends revising the next episode.",
            "decision_needed": "Choose whether to continue, revise, block, or stop.",
            "why_now": ["Council did not return continue."],
            "current_scope": "Next episode only.",
            "non_authorized_scope": [
                "provider retargeting",
                "cleanup",
                "wiki promotion"
            ],
            "options": [
                {
                    "id": "revise",
                    "label": "Revise",
                    "description": "Ask the agent to revise the plan."
                },
                {
                    "id": "block",
                    "label": "Block",
                    "description": "Keep the run blocked."
                }
            ]
        },
        "route": {
            "materiality": "high",
            "target": "user",
            "reason": "The next episode direction changes.",
            "default_if_no_reply": "defer"
        },
        "approval_brief": {
            "schema": "approval_brief.v1",
            "recommendation": "revise",
            "subject": "council continuation decision",
            "summary_lines": ["Council recommends revising before continuing."],
            "scope": "Only approves the next episode direction.",
            "question": "How should the run proceed?"
        }
    });
    fs::write(
        profile_dir.join("offdesk_decisions.jsonl"),
        format!("{}\n", serde_json::to_string(&pending_decision)?),
    )?;

    let surface_output = forager_command(temp.path())
        .args(["ondesk", "workstation-surface", "--json"])
        .output()?;
    assert!(
        surface_output.status.success(),
        "{}",
        String::from_utf8_lossy(&surface_output.stderr)
    );
    let surface: Value = serde_json::from_slice(&surface_output.stdout)?;
    let envelope = surface["decision_inbox"]["items"][0]["action_envelopes"][0].clone();
    assert_eq!(envelope["schema"], "action_envelope.v1");
    assert!(envelope["issued_at"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));
    assert!(envelope["expires_at"]
        .as_str()
        .is_some_and(|value| !value.is_empty()));

    let envelope_path = temp.path().join("action-envelope.json");
    fs::write(&envelope_path, serde_json::to_string_pretty(&envelope)?)?;
    let valid_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-envelope",
            "--envelope",
            envelope_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(
        valid_output.status.success(),
        "{}",
        String::from_utf8_lossy(&valid_output.stderr)
    );
    let valid: Value = serde_json::from_slice(&valid_output.stdout)?;
    assert_eq!(valid["receipt"]["schema"], "action_envelope_receipt.v1");
    assert_eq!(valid["receipt"]["result_status"], "validated_preview");
    assert_eq!(valid["receipt"]["stale"], false);
    assert_eq!(valid["receipt_appended"], true);
    let valid_receipt_id = valid["receipt"]["receipt_id"]
        .as_str()
        .expect("valid receipt id")
        .to_string();
    let receipt_path = profile_dir.join("action_envelope_receipts.jsonl");
    assert_eq!(fs::read_to_string(&receipt_path)?.lines().count(), 1);

    let ready_preflight_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-preflight",
            "--receipt-id",
            valid_receipt_id.as_str(),
            "--json",
        ])
        .output()?;
    assert!(
        ready_preflight_output.status.success(),
        "{}",
        String::from_utf8_lossy(&ready_preflight_output.stderr)
    );
    let ready_preflight: Value = serde_json::from_slice(&ready_preflight_output.stdout)?;
    assert_eq!(
        ready_preflight["preflight"]["schema"],
        "action_execution_preflight.v1"
    );
    assert_eq!(
        ready_preflight["preflight"]["result_status"],
        "ready_for_executor"
    );
    assert_eq!(
        ready_preflight["preflight"]["mutation_allowed_by_this_command"],
        false
    );
    assert_eq!(ready_preflight["preflight_appended"], true);
    let preflight_path = profile_dir.join("action_execution_preflights.jsonl");
    assert_eq!(fs::read_to_string(&preflight_path)?.lines().count(), 1);

    let duplicate_ready_preflight_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-preflight",
            "--receipt-id",
            valid_receipt_id.as_str(),
            "--json",
        ])
        .output()?;
    assert!(
        duplicate_ready_preflight_output.status.success(),
        "{}",
        String::from_utf8_lossy(&duplicate_ready_preflight_output.stderr)
    );
    let duplicate_ready_preflight: Value =
        serde_json::from_slice(&duplicate_ready_preflight_output.stdout)?;
    assert_eq!(duplicate_ready_preflight["preflight_appended"], false);
    assert_eq!(fs::read_to_string(&preflight_path)?.lines().count(), 1);

    let surfaced_valid_output = forager_command(temp.path())
        .args(["ondesk", "workstation-surface", "--json"])
        .output()?;
    assert!(
        surfaced_valid_output.status.success(),
        "{}",
        String::from_utf8_lossy(&surfaced_valid_output.stderr)
    );
    let surfaced_valid: Value = serde_json::from_slice(&surfaced_valid_output.stdout)?;
    let surfaced_valid_envelope =
        &surfaced_valid["decision_inbox"]["items"][0]["action_envelopes"][0];
    assert_eq!(surfaced_valid_envelope["receipt_history_count"], 1);
    assert_eq!(
        surfaced_valid_envelope["latest_receipt"]["schema"],
        "action_envelope_receipt.v1"
    );
    assert_eq!(
        surfaced_valid_envelope["latest_receipt"]["result_status"],
        "validated_preview"
    );
    assert_eq!(surfaced_valid_envelope["latest_receipt"]["stale"], false);

    let mut stale_envelope = envelope.clone();
    stale_envelope["observed_hash"] = json!("sha256:stale");
    let stale_path = temp.path().join("stale-action-envelope.json");
    fs::write(&stale_path, serde_json::to_string_pretty(&stale_envelope)?)?;
    let stale_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-envelope",
            "--envelope",
            stale_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(
        stale_output.status.success(),
        "{}",
        String::from_utf8_lossy(&stale_output.stderr)
    );
    let stale: Value = serde_json::from_slice(&stale_output.stdout)?;
    assert_eq!(stale["receipt"]["result_status"], "rejected");
    assert_eq!(stale["receipt"]["stale"], true);
    let stale_receipt_id = stale["receipt"]["receipt_id"]
        .as_str()
        .expect("stale receipt id")
        .to_string();
    assert!(stale["receipt"]["reason"]
        .as_str()
        .expect("stale reason")
        .contains("observed_hash"));
    assert_eq!(stale["receipt_appended"], true);
    assert_eq!(fs::read_to_string(&receipt_path)?.lines().count(), 2);

    let surfaced_stale_output = forager_command(temp.path())
        .args(["ondesk", "workstation-surface", "--json"])
        .output()?;
    assert!(
        surfaced_stale_output.status.success(),
        "{}",
        String::from_utf8_lossy(&surfaced_stale_output.stderr)
    );
    let surfaced_stale: Value = serde_json::from_slice(&surfaced_stale_output.stdout)?;
    let surfaced_stale_envelope =
        &surfaced_stale["decision_inbox"]["items"][0]["action_envelopes"][0];
    assert_eq!(surfaced_stale_envelope["receipt_history_count"], 2);
    assert_eq!(
        surfaced_stale_envelope["latest_receipt"]["result_status"],
        "rejected"
    );
    assert_eq!(surfaced_stale_envelope["latest_receipt"]["stale"], true);
    assert!(surfaced_stale_envelope["latest_receipt"]["failed_checks"]
        .as_array()
        .expect("failed checks")
        .contains(&json!("observed_hash")));

    let old_valid_preflight_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-preflight",
            "--receipt-id",
            valid_receipt_id.as_str(),
            "--json",
        ])
        .output()?;
    assert!(
        old_valid_preflight_output.status.success(),
        "{}",
        String::from_utf8_lossy(&old_valid_preflight_output.stderr)
    );
    let old_valid_preflight: Value = serde_json::from_slice(&old_valid_preflight_output.stdout)?;
    assert_eq!(old_valid_preflight["preflight"]["result_status"], "blocked");
    assert!(old_valid_preflight["preflight"]["reason"]
        .as_str()
        .expect("old valid preflight reason")
        .contains("latest_receipt"));
    assert_eq!(old_valid_preflight["preflight_appended"], true);
    assert_eq!(fs::read_to_string(&preflight_path)?.lines().count(), 2);

    let stale_preflight_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-preflight",
            "--receipt-id",
            stale_receipt_id.as_str(),
            "--json",
        ])
        .output()?;
    assert!(
        stale_preflight_output.status.success(),
        "{}",
        String::from_utf8_lossy(&stale_preflight_output.stderr)
    );
    let stale_preflight: Value = serde_json::from_slice(&stale_preflight_output.stdout)?;
    assert_eq!(stale_preflight["preflight"]["result_status"], "blocked");
    assert!(stale_preflight["preflight"]["reason"]
        .as_str()
        .expect("stale preflight reason")
        .contains("source_not_stale"));
    assert_eq!(stale_preflight["preflight_appended"], true);
    assert_eq!(fs::read_to_string(&preflight_path)?.lines().count(), 3);

    let duplicate_stale_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-envelope",
            "--envelope",
            stale_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(
        duplicate_stale_output.status.success(),
        "{}",
        String::from_utf8_lossy(&duplicate_stale_output.stderr)
    );
    let duplicate_stale: Value = serde_json::from_slice(&duplicate_stale_output.stdout)?;
    assert_eq!(duplicate_stale["receipt"]["result_status"], "rejected");
    assert_eq!(duplicate_stale["receipt_appended"], false);
    assert_eq!(fs::read_to_string(&receipt_path)?.lines().count(), 2);
    Ok(())
}

#[test]
#[serial]
fn ondesk_accepted_truth_recovery_envelope_records_valid_and_stale_receipts() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let closeout_dir = profile_dir.join("offdesk_closeouts").join("completed-task");
    fs::create_dir_all(&closeout_dir)?;
    fs::write(
        closeout_dir.join("closeout_plan.json"),
        serde_json::to_string_pretty(&json!({
            "schema": "closeout_plan.v1",
            "closeout_id": "closeout-completed-task",
            "generated_at": now,
            "tasks": [{
                "project_key": "project",
                "request_id": "request-completed-task",
                "task_id": "completed-task"
            }]
        }))?,
    )?;
    fs::write(
        closeout_dir.join("closeout_review_20260618T000000Z.json"),
        serde_json::to_string_pretty(&json!({
            "schema": "closeout_review.v1",
            "reviewed_at": now + Duration::minutes(1),
            "review_id": "review-followup",
            "closeout_id": "closeout-completed-task",
            "verdict": "approved",
            "applies_to_tasks": [{
                "project_key": "project",
                "request_id": "request-completed-task",
                "task_id": "completed-task"
            }],
            "closeout_receipt": {
                "schema": "closeout_receipt.v1",
                "receipt_id": "receipt-followup",
                "closeout_id": "closeout-completed-task",
                "acceptance_status": "approved_with_followups",
                "verification_status": "pending",
                "evidence_status": "present",
                "retention_review": "required",
                "wiki_promotion_state": "not_required",
                "stale_task_count": 0,
                "next_safe_action": "Resolve archive review before accepting truth.",
                "open_decisions": [{
                    "kind": "archive_review",
                    "summary": "Archive decision is still open."
                }]
            }
        }))?,
    )?;

    let surface_output = forager_command(temp.path())
        .args(["ondesk", "workstation-surface", "--json"])
        .output()?;
    assert!(
        surface_output.status.success(),
        "{}",
        String::from_utf8_lossy(&surface_output.stderr)
    );
    let surface: Value = serde_json::from_slice(&surface_output.stdout)?;
    let envelope = surface["accepted_truth_recovery"]["items"][0]["action_envelopes"][0].clone();
    assert_eq!(
        envelope["schema"],
        "accepted_truth_recovery_action_envelope.v1"
    );
    assert_eq!(envelope["action_kind"], "resolve_followup");
    assert_eq!(
        envelope["allowed_command"],
        "forager offdesk closeout-decision --closeout-id closeout-completed-task --kind archive_review --decision preserve-in-place --reason <reason> --json"
    );

    let envelope_path = temp.path().join("truth-recovery-envelope.json");
    fs::write(&envelope_path, serde_json::to_string_pretty(&envelope)?)?;
    let valid_output = forager_command(temp.path())
        .args([
            "ondesk",
            "accepted-truth-recovery-envelope",
            "--envelope",
            envelope_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(
        valid_output.status.success(),
        "{}",
        String::from_utf8_lossy(&valid_output.stderr)
    );
    let valid: Value = serde_json::from_slice(&valid_output.stdout)?;
    assert_eq!(
        valid["receipt"]["schema"],
        "accepted_truth_recovery_action_receipt.v1"
    );
    assert_eq!(valid["receipt"]["result_status"], "validated_preview");
    assert_eq!(valid["receipt"]["stale"], false);
    assert_eq!(valid["receipt_appended"], true);
    let receipt_path = profile_dir.join("accepted_truth_recovery_action_receipts.jsonl");
    assert_eq!(fs::read_to_string(&receipt_path)?.lines().count(), 1);

    let surfaced_valid_output = forager_command(temp.path())
        .args(["ondesk", "workstation-surface", "--json"])
        .output()?;
    assert!(
        surfaced_valid_output.status.success(),
        "{}",
        String::from_utf8_lossy(&surfaced_valid_output.stderr)
    );
    let surfaced_valid: Value = serde_json::from_slice(&surfaced_valid_output.stdout)?;
    let surfaced_valid_envelope =
        &surfaced_valid["accepted_truth_recovery"]["items"][0]["action_envelopes"][0];
    assert_eq!(surfaced_valid_envelope["receipt_history_count"], 1);
    assert_eq!(
        surfaced_valid_envelope["latest_receipt"]["schema"],
        "accepted_truth_recovery_action_receipt.v1"
    );
    assert_eq!(
        surfaced_valid_envelope["latest_receipt"]["result_status"],
        "validated_preview"
    );

    let mut stale_envelope = envelope.clone();
    stale_envelope["observed_hash"] = json!("sha256:stale");
    let stale_path = temp.path().join("stale-truth-recovery-envelope.json");
    fs::write(&stale_path, serde_json::to_string_pretty(&stale_envelope)?)?;
    let stale_output = forager_command(temp.path())
        .args([
            "ondesk",
            "accepted-truth-recovery-envelope",
            "--envelope",
            stale_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(
        stale_output.status.success(),
        "{}",
        String::from_utf8_lossy(&stale_output.stderr)
    );
    let stale: Value = serde_json::from_slice(&stale_output.stdout)?;
    assert_eq!(stale["receipt"]["result_status"], "rejected");
    assert_eq!(stale["receipt"]["stale"], true);
    assert!(stale["receipt"]["reason"]
        .as_str()
        .expect("stale reason")
        .contains("observed_hash"));
    assert_eq!(stale["receipt_appended"], true);
    assert_eq!(fs::read_to_string(&receipt_path)?.lines().count(), 2);

    let duplicate_stale_output = forager_command(temp.path())
        .args([
            "ondesk",
            "accepted-truth-recovery-envelope",
            "--envelope",
            stale_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(
        duplicate_stale_output.status.success(),
        "{}",
        String::from_utf8_lossy(&duplicate_stale_output.stderr)
    );
    let duplicate_stale: Value = serde_json::from_slice(&duplicate_stale_output.stdout)?;
    assert_eq!(duplicate_stale["receipt"]["result_status"], "rejected");
    assert_eq!(duplicate_stale["receipt_appended"], false);
    assert_eq!(fs::read_to_string(&receipt_path)?.lines().count(), 2);
    Ok(())
}

#[test]
#[serial]
fn ondesk_action_decision_requires_ready_preflight_and_is_idempotent() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let pending_decision = json!({
        "schema": "decision_record.v1",
        "decision_id": "decision-user",
        "project_key": "project",
        "request_id": "request",
        "task_id": "approval-task",
        "raised_by": "agent",
        "source_surface": "offdesk.council",
        "materiality": "high",
        "status": "user_pending",
        "created_at": now,
        "updated_at": now,
        "decision_request": {
            "kind": "council_escalation",
            "summary": "Council recommends revising the next episode.",
            "decision_needed": "Choose whether to continue, revise, block, or stop.",
            "why_now": ["Council did not return continue."],
            "current_scope": "Next episode only.",
            "non_authorized_scope": ["provider retargeting", "cleanup", "wiki promotion"],
            "options": [
                {"id": "revise", "label": "Revise", "description": "Ask the agent to revise the plan."},
                {"id": "block", "label": "Block", "description": "Keep the run blocked."}
            ]
        },
        "route": {
            "materiality": "high",
            "target": "user",
            "reason": "The next episode direction changes.",
            "default_if_no_reply": "defer"
        },
        "approval_brief": {
            "schema": "approval_brief.v1",
            "recommendation": "revise",
            "subject": "council continuation decision",
            "summary_lines": ["Council recommends revising before continuing."],
            "scope": "Only approves the next episode direction.",
            "question": "How should the run proceed?"
        }
    });
    fs::write(
        profile_dir.join("offdesk_decisions.jsonl"),
        format!("{}\n", serde_json::to_string(&pending_decision)?),
    )?;

    let surface_output = forager_command(temp.path())
        .args(["ondesk", "workstation-surface", "--json"])
        .output()?;
    assert!(
        surface_output.status.success(),
        "{}",
        String::from_utf8_lossy(&surface_output.stderr)
    );
    let surface: Value = serde_json::from_slice(&surface_output.stdout)?;
    let envelope = surface["decision_inbox"]["items"][0]["action_envelopes"][0].clone();
    assert_eq!(envelope["action_kind"], "revise");
    let envelope_path = temp.path().join("action-envelope.json");
    fs::write(&envelope_path, serde_json::to_string_pretty(&envelope)?)?;

    let valid_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-envelope",
            "--envelope",
            envelope_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(
        valid_output.status.success(),
        "{}",
        String::from_utf8_lossy(&valid_output.stderr)
    );
    let valid: Value = serde_json::from_slice(&valid_output.stdout)?;
    let valid_receipt_id = valid["receipt"]["receipt_id"]
        .as_str()
        .expect("valid receipt id");

    let preflight_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-preflight",
            "--receipt-id",
            valid_receipt_id,
            "--json",
        ])
        .output()?;
    assert!(
        preflight_output.status.success(),
        "{}",
        String::from_utf8_lossy(&preflight_output.stderr)
    );
    let preflight: Value = serde_json::from_slice(&preflight_output.stdout)?;
    assert_eq!(
        preflight["preflight"]["result_status"],
        "ready_for_executor"
    );
    let preflight_id = preflight["preflight"]["preflight_id"]
        .as_str()
        .expect("preflight id");

    let blocked_without_note = forager_command(temp.path())
        .args([
            "ondesk",
            "action-decision",
            "--preflight-id",
            preflight_id,
            "--json",
        ])
        .output()?;
    assert!(
        blocked_without_note.status.success(),
        "{}",
        String::from_utf8_lossy(&blocked_without_note.stderr)
    );
    let blocked: Value = serde_json::from_slice(&blocked_without_note.stdout)?;
    assert_eq!(blocked["execution"]["result_status"], "blocked");
    assert_eq!(blocked["decision_appended"], false);
    let blocked_execution_id = blocked["execution"]["execution_id"]
        .as_str()
        .expect("blocked execution id");

    let blocked_closeout_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-closeout",
            "--execution-id",
            blocked_execution_id,
            "--json",
        ])
        .output()?;
    assert!(
        blocked_closeout_output.status.success(),
        "{}",
        String::from_utf8_lossy(&blocked_closeout_output.stderr)
    );
    let blocked_closeout: Value = serde_json::from_slice(&blocked_closeout_output.stdout)?;
    assert_eq!(blocked_closeout["closeout"]["result_status"], "blocked");
    assert_eq!(blocked_closeout["decision_appended"], false);
    assert_eq!(blocked_closeout["closeout_appended"], true);
    assert_eq!(
        fs::read_to_string(profile_dir.join("decision_action_closeouts.jsonl"))?
            .lines()
            .count(),
        1
    );

    let execution_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-decision",
            "--preflight-id",
            preflight_id,
            "--note",
            "Revise the next episode before continuing.",
            "--json",
        ])
        .output()?;
    assert!(
        execution_output.status.success(),
        "{}",
        String::from_utf8_lossy(&execution_output.stderr)
    );
    let execution: Value = serde_json::from_slice(&execution_output.stdout)?;
    assert_eq!(
        execution["execution"]["schema"],
        "decision_action_execution.v1"
    );
    assert_eq!(execution["execution"]["result_status"], "applied");
    assert_eq!(execution["execution"]["decision"], "revise");
    assert_eq!(
        execution["execution"]["mutation_allowed_by_this_command"],
        true
    );
    assert_eq!(execution["decision_appended"], true);
    assert_eq!(execution["execution_appended"], true);
    assert_eq!(execution["updated_record"]["status"], "handoff_ready");
    assert_eq!(
        execution["updated_record"]["execution_handoff"]["approved_direction"],
        "revise"
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("offdesk_decisions.jsonl"))?
            .lines()
            .count(),
        2
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("decision_action_executions.jsonl"))?
            .lines()
            .count(),
        2
    );

    let surfaced_execution_output = forager_command(temp.path())
        .args(["ondesk", "workstation-surface", "--json"])
        .output()?;
    assert!(
        surfaced_execution_output.status.success(),
        "{}",
        String::from_utf8_lossy(&surfaced_execution_output.stderr)
    );
    let surfaced_execution: Value = serde_json::from_slice(&surfaced_execution_output.stdout)?;
    let surfaced_action = &surfaced_execution["decision_inbox"]["items"][0]["action_envelopes"][0];
    assert_eq!(surfaced_action["execution_history_count"], 2);
    assert_eq!(
        surfaced_action["latest_execution"]["schema"],
        "decision_action_execution.v1"
    );
    assert_eq!(
        surfaced_action["latest_execution"]["result_status"],
        "applied"
    );
    assert_eq!(surfaced_action["latest_execution"]["decision"], "revise");
    assert_eq!(
        surfaced_action["latest_execution"]["decision_appended"],
        true
    );
    assert_eq!(
        surfaced_action["latest_execution"]["handoff_id"],
        execution["execution"]["handoff_id"]
    );
    assert!(surfaced_action["latest_execution"]["closeout_command"]
        .as_str()
        .expect("closeout command")
        .contains("forager ondesk action-closeout --execution-id"));

    let duplicate_execution_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-decision",
            "--preflight-id",
            preflight_id,
            "--note",
            "Revise the next episode before continuing.",
            "--json",
        ])
        .output()?;
    assert!(
        duplicate_execution_output.status.success(),
        "{}",
        String::from_utf8_lossy(&duplicate_execution_output.stderr)
    );
    let duplicate: Value = serde_json::from_slice(&duplicate_execution_output.stdout)?;
    assert_eq!(duplicate["execution"]["result_status"], "applied");
    assert_eq!(duplicate["decision_appended"], false);
    assert_eq!(duplicate["execution_appended"], false);
    assert_eq!(
        fs::read_to_string(profile_dir.join("offdesk_decisions.jsonl"))?
            .lines()
            .count(),
        2
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("decision_action_executions.jsonl"))?
            .lines()
            .count(),
        2
    );

    let execution_id = execution["execution"]["execution_id"]
        .as_str()
        .expect("execution id");
    let closeout_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-closeout",
            "--execution-id",
            execution_id,
            "--result-status",
            "accepted",
            "--evidence",
            "Operator reviewed the handoff before closing the decision.",
            "--remaining-review",
            "Runtime dispatch still requires a separate executor.",
            "--json",
        ])
        .output()?;
    assert!(
        closeout_output.status.success(),
        "{}",
        String::from_utf8_lossy(&closeout_output.stderr)
    );
    let closeout: Value = serde_json::from_slice(&closeout_output.stdout)?;
    assert_eq!(
        closeout["closeout"]["schema"],
        "decision_action_closeout.v1"
    );
    assert_eq!(closeout["closeout"]["result_status"], "receipted");
    assert_eq!(closeout["closeout"]["receipt_result_status"], "accepted");
    assert_eq!(closeout["closeout_appended"], true);
    assert_eq!(closeout["decision_appended"], true);
    assert_eq!(closeout["updated_record"]["status"], "receipted");
    assert_eq!(
        closeout["updated_record"]["decision_receipt"]["applied_handoff_id"],
        execution["execution"]["handoff_id"]
    );
    assert_eq!(
        closeout["updated_record"]["decision_receipt"]["result_status"],
        "accepted"
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("offdesk_decisions.jsonl"))?
            .lines()
            .count(),
        3
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("decision_action_closeouts.jsonl"))?
            .lines()
            .count(),
        2
    );

    let duplicate_closeout_output = forager_command(temp.path())
        .args([
            "ondesk",
            "action-closeout",
            "--execution-id",
            execution_id,
            "--result-status",
            "accepted",
            "--json",
        ])
        .output()?;
    assert!(
        duplicate_closeout_output.status.success(),
        "{}",
        String::from_utf8_lossy(&duplicate_closeout_output.stderr)
    );
    let duplicate_closeout: Value = serde_json::from_slice(&duplicate_closeout_output.stdout)?;
    assert_eq!(duplicate_closeout["closeout"]["result_status"], "receipted");
    assert_eq!(duplicate_closeout["closeout_appended"], false);
    assert_eq!(duplicate_closeout["decision_appended"], false);
    assert_eq!(
        fs::read_to_string(profile_dir.join("offdesk_decisions.jsonl"))?
            .lines()
            .count(),
        3
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("decision_action_closeouts.jsonl"))?
            .lines()
            .count(),
        2
    );

    let closeout_id = closeout["closeout"]["closeout_id"]
        .as_str()
        .expect("closeout id");
    let runtime_preflight_output = forager_command(temp.path())
        .args([
            "ondesk",
            "runtime-preflight",
            "--closeout-id",
            closeout_id,
            "--json",
        ])
        .output()?;
    assert!(
        runtime_preflight_output.status.success(),
        "{}",
        String::from_utf8_lossy(&runtime_preflight_output.stderr)
    );
    let runtime_preflight: Value = serde_json::from_slice(&runtime_preflight_output.stdout)?;
    assert_eq!(
        runtime_preflight["preflight"]["schema"],
        "runtime_dispatch_preflight.v1"
    );
    assert_eq!(
        runtime_preflight["preflight"]["result_status"],
        "ready_for_runtime_dispatch"
    );
    assert_eq!(
        runtime_preflight["preflight"]["mutation_allowed_by_this_command"],
        false
    );
    assert_eq!(runtime_preflight["preflight_appended"], true);
    let runtime_preflight_id = runtime_preflight["preflight"]["preflight_id"]
        .as_str()
        .expect("runtime preflight id");

    let runtime_dispatch_output = forager_command(temp.path())
        .args([
            "ondesk",
            "runtime-dispatch",
            "--preflight-id",
            runtime_preflight_id,
            "--runner",
            "local-background",
            "--cmd",
            "true",
            "--workdir",
            temp.path().to_str().expect("utf-8 temp path"),
            "--task-id",
            "runtime-task",
            "--json",
        ])
        .output()?;
    assert!(
        runtime_dispatch_output.status.success(),
        "{}",
        String::from_utf8_lossy(&runtime_dispatch_output.stderr)
    );
    let runtime_dispatch: Value = serde_json::from_slice(&runtime_dispatch_output.stdout)?;
    assert_eq!(
        runtime_dispatch["receipt"]["schema"],
        "runtime_dispatch_receipt.v1"
    );
    assert_eq!(runtime_dispatch["receipt"]["result_status"], "queued");
    assert_eq!(runtime_dispatch["receipt"]["task_id"], "runtime-task");
    assert_eq!(
        runtime_dispatch["receipt"]["mutation_allowed_by_this_command"],
        true
    );
    assert_eq!(runtime_dispatch["receipt_appended"], true);
    assert_eq!(runtime_dispatch["task_enqueued"], true);
    assert_eq!(runtime_dispatch["task"]["status"], "queued");
    assert_eq!(
        runtime_dispatch["task"]["capability_id"],
        "dispatch.runtime"
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("runtime_dispatch_preflights.jsonl"))?
            .lines()
            .count(),
        1
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("runtime_dispatch_receipts.jsonl"))?
            .lines()
            .count(),
        1
    );
    let tasks: Value =
        serde_json::from_str(&fs::read_to_string(profile_dir.join("offdesk_tasks.json"))?)?;
    assert_eq!(tasks.as_array().expect("tasks").len(), 1);
    assert_eq!(tasks[0]["task_id"], "runtime-task");
    assert_eq!(tasks[0]["status"], "queued");

    let duplicate_runtime_dispatch_output = forager_command(temp.path())
        .args([
            "ondesk",
            "runtime-dispatch",
            "--preflight-id",
            runtime_preflight_id,
            "--runner",
            "local-background",
            "--cmd",
            "true",
            "--workdir",
            temp.path().to_str().expect("utf-8 temp path"),
            "--task-id",
            "runtime-task",
            "--json",
        ])
        .output()?;
    assert!(
        duplicate_runtime_dispatch_output.status.success(),
        "{}",
        String::from_utf8_lossy(&duplicate_runtime_dispatch_output.stderr)
    );
    let duplicate_runtime_dispatch: Value =
        serde_json::from_slice(&duplicate_runtime_dispatch_output.stdout)?;
    assert_eq!(
        duplicate_runtime_dispatch["receipt"]["result_status"],
        "queued"
    );
    assert_eq!(duplicate_runtime_dispatch["receipt_appended"], false);
    assert_eq!(duplicate_runtime_dispatch["task_enqueued"], false);
    assert_eq!(
        fs::read_to_string(profile_dir.join("runtime_dispatch_receipts.jsonl"))?
            .lines()
            .count(),
        1
    );

    let closed_surface_output = forager_command(temp.path())
        .args(["ondesk", "workstation-surface", "--json"])
        .output()?;
    assert!(
        closed_surface_output.status.success(),
        "{}",
        String::from_utf8_lossy(&closed_surface_output.stderr)
    );
    let closed_surface: Value = serde_json::from_slice(&closed_surface_output.stdout)?;
    assert_eq!(closed_surface["decision_inbox"]["open_count"], 0);
    assert_eq!(
        closed_surface["decision_inbox"]["items"]
            .as_array()
            .expect("closed items")
            .len(),
        0
    );
    assert_eq!(
        closed_surface["runtime_dispatch"]["schema"],
        "runtime_dispatch_surface.v1"
    );
    assert_eq!(closed_surface["runtime_dispatch"]["candidate_count"], 1);
    assert_eq!(closed_surface["runtime_dispatch"]["visible_count"], 1);
    let runtime_item = &closed_surface["runtime_dispatch"]["items"][0];
    assert_eq!(runtime_item["stage"], "queued");
    assert_eq!(runtime_item["severity"], "ok");
    assert_eq!(runtime_item["closeout_id"], closeout_id);
    assert_eq!(
        runtime_item["latest_preflight"]["preflight_id"],
        runtime_preflight_id
    );
    assert_eq!(runtime_item["latest_receipt"]["task_id"], "runtime-task");
    assert_eq!(
        runtime_item["preflight_command"],
        format!("forager ondesk runtime-preflight --closeout-id {closeout_id} --json")
    );
    assert_eq!(
        runtime_item["tick_command"],
        "forager offdesk tick --task-id runtime-task"
    );
    assert_eq!(
        closed_surface["projects"][0]["task_items"][0]["receipt_links"][0]["source"],
        "runtime_dispatch"
    );
    assert_eq!(
        closed_surface["projects"][0]["task_items"][0]["receipt_links"][0]["record_id"],
        runtime_dispatch["receipt"]["receipt_id"]
    );
    assert_eq!(
        closed_surface["projects"][0]["task_items"][0]["receipt_links"][0]["result_status"],
        "queued"
    );
    Ok(())
}

#[test]
#[serial]
fn ondesk_review_surface_keeps_accepted_truth_when_latest_closeout_is_retired() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let task_updated = now - Duration::minutes(10);
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            offdesk_task_fixture("accepted-task", "completed", task_updated),
            offdesk_task_fixture("retired-task", "completed", task_updated)
        ]))?,
    )?;

    let accepted_dir = profile_dir
        .join("offdesk_closeouts")
        .join("20260601T000000Z_closeout_accepted");
    fs::create_dir_all(&accepted_dir)?;
    let accepted_receipt_path = accepted_dir.join("closeout_receipt_20260601T000100Z.json");
    let accepted_log_path = accepted_dir.join("accepted-task.log");
    let accepted_commercial_review_path = accepted_dir.join("COMMERCIAL_REVIEW_PACKET.md");
    let accepted_cleanup_manifest_path = accepted_dir.join("cleanup_manifest.json");
    fs::write(
        accepted_dir.join("RETURN_PACKAGE.md"),
        "# Accepted return\n",
    )?;
    fs::write(&accepted_log_path, "accepted log\n")?;
    fs::write(&accepted_commercial_review_path, "# Commercial review\n")?;
    fs::write(&accepted_cleanup_manifest_path, "{}\n")?;
    fs::write(
        accepted_dir.join("closeout_plan.json"),
        serde_json::to_string_pretty(&json!({
            "closeout_id": "closeout_accepted",
            "generated_at": now,
            "filters": {"project_key": "project"},
            "tasks": [{
                "project_key": "project",
                "request_id": "request-accepted-task",
                "task_id": "accepted-task",
                "log_artifact_path": accepted_log_path
            }],
            "artifacts": {
                "return_package_markdown": accepted_dir.join("RETURN_PACKAGE.md"),
                "commercial_review_packet": accepted_commercial_review_path,
                "cleanup_manifest_json": accepted_cleanup_manifest_path
            }
        }))?,
    )?;
    let accepted_receipt = json!({
        "schema": "closeout_receipt.v1",
        "receipt_id": "receipt-accepted",
        "acceptance_status": "accepted",
        "verification_status": "recorded",
        "retention_review": "resolved_preserve_in_place",
        "accepted_scope": ["project:accepted-task"],
        "open_decisions": [],
        "next_safe_action": "Continue with accepted evidence."
    });
    fs::write(
        &accepted_receipt_path,
        serde_json::to_string_pretty(&accepted_receipt)?,
    )?;
    fs::write(
        accepted_dir.join("closeout_review_20260601T000100Z.json"),
        serde_json::to_string_pretty(&json!({
            "reviewed_at": now,
            "verdict": "approved",
            "applies_to_tasks": [{
                "project_key": "project",
                "request_id": "request-accepted-task",
                "task_id": "accepted-task"
            }],
            "closeout_receipt": accepted_receipt,
            "artifacts": {
                "closeout_receipt_json": accepted_receipt_path
            }
        }))?,
    )?;

    let retired_dir = profile_dir
        .join("offdesk_closeouts")
        .join("20260601T000200Z_closeout_retired");
    fs::create_dir_all(&retired_dir)?;
    let retired_log_path = retired_dir.join("retired-task.log");
    fs::write(retired_dir.join("RETURN_PACKAGE.md"), "# Retired return\n")?;
    fs::write(&retired_log_path, "retired log\n")?;
    fs::write(
        retired_dir.join("closeout_plan.json"),
        serde_json::to_string_pretty(&json!({
            "closeout_id": "closeout_retired",
            "generated_at": now + Duration::seconds(1),
            "filters": {"project_key": "project"},
            "tasks": [{
                "project_key": "project",
                "request_id": "request-retired-task",
                "task_id": "retired-task",
                "log_artifact_path": retired_log_path
            }],
            "artifacts": {
                "return_package_markdown": retired_dir.join("RETURN_PACKAGE.md")
            }
        }))?,
    )?;
    fs::write(
        retired_dir.join("closeout_review_20260601T000300Z.json"),
        serde_json::to_string_pretty(&json!({
            "reviewed_at": now + Duration::seconds(2),
            "verdict": "revise",
            "applies_to_tasks": [{
                "project_key": "project",
                "request_id": "request-retired-task",
                "task_id": "retired-task"
            }],
            "closeout_receipt": {
                "schema": "closeout_receipt.v1",
                "receipt_id": "receipt-retired",
                "acceptance_status": "retired_incomplete",
                "verification_status": "retired",
                "open_decisions": [],
                "next_safe_action": "No accepted truth is recorded for this retired evidence-incomplete closeout."
            }
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "ondesk",
            "review-surface",
            "--project-key",
            "project",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let surface: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(surface["status"]["label"], "clear");
    assert_eq!(surface["accepted_truth"]["status"], "accepted");
    assert_eq!(surface["accepted_truth"]["source"], "closeout_receipt.v1");
    assert_eq!(
        surface["accepted_truth"]["receipt_acceptance_status"],
        "accepted"
    );
    assert_eq!(
        surface["accepted_truth"]["accepted_closeout_id"],
        "closeout_accepted"
    );
    assert_eq!(
        surface["accepted_truth"]["accepted_receipt_id"],
        "receipt-accepted"
    );
    assert_eq!(
        surface["accepted_truth"]["accepted_receipt_path"],
        accepted_receipt_path.to_string_lossy().as_ref()
    );
    assert_eq!(
        surface["accepted_truth"]["accepted_scope"][0],
        "project:accepted-task"
    );
    assert_eq!(
        surface["closeout"]["execution_status"],
        "retired_incomplete"
    );
    assert_eq!(surface["closeout"]["review_status"], "retired_incomplete");
    assert_eq!(surface["closeout"]["summary"]["accepted"], 1);
    assert_eq!(surface["closeout"]["summary"]["retired_incomplete"], 1);
    let retention_summary = &surface["artifacts"]["retention_review"]["summary"];
    assert_eq!(
        retention_summary["by_scope"]["active_accepted"]["action_required_entries"],
        0
    );
    assert!(
        retention_summary["by_scope"]["active_accepted"]["keep_entries"]
            .as_u64()
            .unwrap_or_default()
            >= 1
    );
    assert_eq!(
        retention_summary["by_scope"]["retired_historical"]["action_required_entries"],
        1
    );
    let retention_actions = surface["artifacts"]["retention_review"]["action_required"]
        .as_array()
        .expect("retention action projection");
    assert!(!retention_actions
        .iter()
        .any(|item| item["retention_scope"] == "active_accepted"));
    assert!(retention_actions
        .iter()
        .any(|item| item["retention_scope"] == "retired_historical"));
    Ok(())
}

#[test]
#[serial]
fn ondesk_review_surface_default_output_is_human_summary() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([offdesk_task_fixture(
            "approval-task",
            "pending_approval",
            now
        )]))?,
    )?;

    let output = forager_command(temp.path())
        .args(["ondesk", "review-surface", "--project-key", "project"])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout)?;
    assert!(stdout.contains("Morning Review Surface"));
    assert!(stdout.contains("status: needs_review"));
    assert!(stdout.contains("accepted truth:"));
    assert!(stdout.contains("next safe actions:"));
    assert!(stdout.contains("refs: use --json for audit paths"));
    assert!(!stdout.contains(profile_dir.to_string_lossy().as_ref()));
    Ok(())
}

#[test]
#[serial]
fn ondesk_review_surface_summarizes_closeout_receipt_before_paths() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let task_updated = now - Duration::minutes(10);
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([offdesk_task_fixture(
            "completed-task",
            "completed",
            task_updated
        )]))?,
    )?;

    let closeout_dir = profile_dir
        .join("offdesk_closeouts")
        .join("20260601T000000Z_closeout_project");
    fs::create_dir_all(&closeout_dir)?;
    let return_package_path = closeout_dir.join("RETURN_PACKAGE.md");
    let receipt_path = closeout_dir.join("closeout_receipt_20260601T000000Z.json");
    fs::write(&return_package_path, "# Ondesk Return Package\n")?;
    fs::write(
        closeout_dir.join("closeout_plan.json"),
        serde_json::to_string_pretty(&json!({
            "closeout_id": "closeout_project",
            "generated_at": now,
            "filters": {
                "project_key": "project"
            },
            "tasks": [
                {
                    "project_key": "project",
                    "request_id": "request-completed-task",
                    "task_id": "completed-task"
                }
            ],
            "source_observation": {
                "schema": "source_observation.v1",
                "status": "observed",
                "source_kind": "git_worktree",
                "enabled": true,
                "available": true,
                "workdir": temp.path().join("project"),
                "base_ref": "HEAD",
                "changed_file_count": 1,
                "changed_files_truncated": false,
                "changed_files": [
                    {
                        "path": "README.md",
                        "status": "modified",
                        "additions": 2,
                        "deletions": 0
                    }
                ],
                "artifact_refs": [return_package_path],
                "warnings": []
            },
            "implementation_packet_coverage": {
                "packet_count": 1,
                "completed": 1,
                "deferred": 0,
                "missing": 0,
                "drifted": 0,
                "detail_items": 3,
                "detail_items_completed": 1,
                "detail_items_deferred": 0,
                "detail_items_missing": 1,
                "detail_items_drifted": 1,
                "items": [
                    {
                        "packet_id": "packet-closeout-project",
                        "project_key": "project",
                        "goal": "Return packet coverage to Ondesk.",
                        "success_state": "Missing packet evidence remains visible.",
                        "goal_status": "completed",
                        "reason": "Execution evidence exists; closeout review still applies.",
                        "detail_source": "implementation_packet",
                        "work_slices": [
                            {
                                "category": "work_slice",
                                "label": "receipt drifted slice",
                                "status": "drifted",
                                "reason": "Work-slice execution receipt reports drifted.",
                                "evidence_refs": ["slice-drift-log.txt"],
                                "source_observation_status": "observed",
                                "source_refs": ["source:git:modified:README.md"],
                                "summary": "The worker changed the slice boundary.",
                                "next_safe_action": "Revise the packet before accepting truth."
                            }
                        ],
                        "validation_items": [
                            {
                                "category": "validation_test",
                                "label": "missing-validation.txt",
                                "status": "missing",
                                "reason": "No closeout artifact matched this item.",
                                "evidence_refs": []
                            }
                        ],
                        "expected_artifacts": [
                            {
                                "category": "expected_artifact",
                                "label": "result.json",
                                "status": "completed",
                                "reason": "Closeout evidence matched this item.",
                                "evidence_refs": ["task:completed-task:result:result.json"]
                            }
                        ]
                    }
                ]
            },
            "artifacts": {
                "return_package_markdown": return_package_path
            }
        }))?,
    )?;
    let receipt = json!({
        "schema": "closeout_receipt.v1",
        "receipt_id": "receipt-followup",
        "closeout_id": "closeout_project",
        "review_id": "review-followup",
        "acceptance_status": "approved_with_followups",
        "verification_status": "pending",
        "open_decisions": [
            {
                "kind": "operator_followup",
                "detail": "Confirm whether to promote the wiki lesson.",
                "suggested_command": "forager offdesk wiki review"
            }
        ],
        "missing_evidence": ["verification screenshot"],
        "required_first_reads": [],
        "unsafe_operations": [],
        "retention_review": "not_required",
        "wiki_promotion_state": "not_required",
        "stale_task_count": 0,
        "next_safe_action": "Review missing evidence before acceptance."
    });
    fs::write(&receipt_path, serde_json::to_string_pretty(&receipt)?)?;
    fs::write(
        closeout_dir.join("closeout_review_20260601T000000Z.json"),
        serde_json::to_string_pretty(&json!({
            "reviewed_at": now,
            "verdict": "approved",
            "applies_to_tasks": [
                {
                    "project_key": "project",
                    "request_id": "request-completed-task",
                    "task_id": "completed-task"
                }
            ],
            "closeout_receipt": receipt,
            "artifacts": {
                "closeout_receipt_json": receipt_path
            }
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "ondesk",
            "review-surface",
            "--project-key",
            "project",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let surface: Value = serde_json::from_slice(&output.stdout)?;

    assert_eq!(surface["accepted_truth"]["status"], "pending");
    assert_eq!(
        surface["accepted_truth"]["receipt_acceptance_status"],
        "approved_with_followups"
    );
    assert_eq!(
        surface["accepted_truth"]["reason"],
        "Review missing evidence before acceptance."
    );
    assert_eq!(surface["closeout"]["latest_receipt_id"], "receipt-followup");
    assert_eq!(
        surface["closeout"]["review_status"],
        "approved_with_followups"
    );
    assert_eq!(surface["closeout"]["receipt_open_decisions"], 1);
    assert_eq!(
        surface["closeout"]["implementation_packet_coverage"]["packet_count"],
        1
    );
    assert_eq!(
        surface["closeout"]["implementation_packet_coverage"]["detail_items_missing"],
        1
    );
    assert_eq!(
        surface["closeout"]["source_observation"]["status"],
        "observed"
    );
    assert_eq!(
        surface["closeout"]["source_observation"]["interpretation"],
        "source observation is read-only evidence context, not accepted truth or slice verification"
    );
    assert!(surface["closeout"]["source_observation"]["changed_files"]
        .as_array()
        .expect("source changed files")
        .iter()
        .any(|file| file["path"] == "README.md" && file["status"] == "modified"));
    assert!(
        surface["closeout"]["implementation_packet_coverage"]["items"][0]["validation_items"]
            .as_array()
            .expect("coverage validation items")
            .iter()
            .any(|item| item["label"] == "missing-validation.txt" && item["status"] == "missing")
    );
    assert_eq!(
        surface["closeout"]["implementation_packet_coverage"]["items"][0]["work_slices"][0]
            ["source_observation_status"],
        "observed"
    );
    assert!(surface["closeout"]["unresolved_risks"]
        .as_array()
        .expect("unresolved risks")
        .iter()
        .any(|risk| risk
            .as_str()
            .expect("risk string")
            .contains("missing evidence")));

    let summaries = surface["artifacts"]["summary"]
        .as_array()
        .expect("artifact summaries");
    assert!(summaries
        .iter()
        .any(|summary| summary["label"] == "Closeout receipt"
            && summary["retention_class"] == "acceptance"));
    for summary in summaries {
        assert!(
            !summary
                .to_string()
                .contains(closeout_dir.to_string_lossy().as_ref()),
            "artifact summary should explain meaning before exposing paths: {summary}"
        );
    }
    assert!(surface["artifacts"]["refs"]
        .as_array()
        .expect("artifact refs")
        .iter()
        .any(|reference| reference["id"] == "closeout_receipt" && reference["present"] == true));
    let artifact_index = &surface["artifacts"]["index"];
    assert_eq!(artifact_index["schema"], "artifact_index.v1");
    assert!(
        artifact_index["summary"]["total_entries"]
            .as_u64()
            .unwrap_or_default()
            >= 3
    );
    assert!(artifact_index["entries"]
        .as_array()
        .expect("artifact index entries")
        .iter()
        .any(|entry| entry["kind"] == "closeout_receipt"
            && entry["label"] == "Closeout receipt"
            && entry["present"] == true));
    let retention_review = &surface["artifacts"]["retention_review"];
    assert_eq!(retention_review["schema"], "artifact_retention_review.v1");
    assert!(
        retention_review["summary"]["total_entries"]
            .as_u64()
            .unwrap_or_default()
            >= 3
    );
    Ok(())
}

#[test]
#[serial]
fn ondesk_prompt_package_includes_latest_offdesk_return_package() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    let closeout_dir = profile_dir
        .join("offdesk_closeouts")
        .join("20260521T000000Z_closeout_test");
    fs::create_dir_all(&closeout_dir)?;
    let return_package_path = closeout_dir.join("RETURN_PACKAGE.md");
    fs::write(
        &return_package_path,
        "# Ondesk Return Package\n\nNight result summary token=sk-secretsecretsecretsecret\n",
    )?;
    let generated_at = Utc::now();
    fs::write(
        closeout_dir.join("closeout_plan.json"),
        serde_json::to_string_pretty(&json!({
            "closeout_id": "closeout_test",
            "generated_at": generated_at,
            "filters": {
                "project_key": "twinpaper"
            },
            "tasks": [
                {
                    "project_key": "twinpaper",
                    "request_id": "request",
                    "task_id": "task"
                }
            ],
            "source_observation": {
                "schema": "source_observation.v1",
                "status": "observed",
                "source_kind": "git_worktree",
                "enabled": true,
                "available": true,
                "workdir": temp.path().join("twinpaper"),
                "base_ref": "HEAD",
                "changed_file_count": 2,
                "changed_files_truncated": false,
                "changed_files": [
                    {
                        "path": "src/module03.rs",
                        "status": "modified",
                        "additions": 8,
                        "deletions": 1
                    },
                    {
                        "path": "docs/module03.md",
                        "status": "modified",
                        "additions": 3,
                        "deletions": 0
                    }
                ],
                "artifact_refs": [return_package_path],
                "warnings": []
            },
            "implementation_packet_coverage": {
                "packet_count": 1,
                "completed": 1,
                "deferred": 0,
                "missing": 0,
                "drifted": 0,
                "detail_items": 3,
                "detail_items_completed": 1,
                "detail_items_deferred": 0,
                "detail_items_missing": 1,
                "detail_items_drifted": 1,
                "items": [
                    {
                        "packet_id": "packet-twinpaper",
                        "project_key": "twinpaper",
                        "goal": "Return closeout packet coverage to Ondesk.",
                        "success_state": "Ondesk prompt shows missing packet details.",
                        "goal_status": "completed",
                        "reason": "Execution evidence exists; closeout review still applies.",
                        "detail_source": "implementation_packet",
                        "work_slices": [
                            {
                                "category": "work_slice",
                                "label": "receipt drifted slice",
                                "status": "drifted",
                                "reason": "Work-slice execution receipt reports drifted.",
                                "evidence_refs": ["slice-drift-log.txt"],
                                "source_observation_status": "observed",
                                "source_refs": ["source:git:modified:src/module03.rs"],
                                "summary": "The worker changed the slice boundary.",
                                "next_safe_action": "Revise the packet before accepting truth."
                            }
                        ],
                        "validation_items": [
                            {
                                "category": "validation_test",
                                "label": "missing-validation.txt",
                                "status": "missing",
                                "reason": "No closeout artifact matched this item.",
                                "evidence_refs": []
                            }
                        ],
                        "expected_artifacts": [
                            {
                                "category": "expected_artifact",
                                "label": "result.json",
                                "status": "completed",
                                "reason": "Closeout evidence matched this item.",
                                "evidence_refs": ["task:task:result:result.json"]
                            }
                        ]
                    }
                ]
            },
            "artifacts": {
                "return_package_markdown": return_package_path
            }
        }))?,
    )?;
    fs::write(
        closeout_dir.join("closeout_review_20260521T000000Z.json"),
        serde_json::to_string_pretty(&json!({
            "reviewed_at": generated_at,
            "verdict": "approved",
            "closeout_receipt": {
                "schema": "closeout_receipt.v1",
                "acceptance_status": "approved_with_followups",
                "open_decisions": [
                    {
                        "kind": "archive_review",
                        "detail": "Confirm whether to archive generated validation artifacts.",
                        "suggested_command": "forager project retention-review"
                    }
                ]
            },
            "artifacts": {
                "closeout_receipt_json": closeout_dir.join("closeout_receipt_20260521T000000Z.json")
            }
        }))?,
    )?;
    let decision_record = json!({
        "schema": "decision_record.v1",
        "decision_id": "decision-council-route",
        "project_key": "twinpaper",
        "request_id": "request-council-route",
        "task_id": "task-council-route",
        "raised_by": "council",
        "source_surface": "offdesk.council",
        "materiality": "medium",
        "status": "auto_resolved",
        "created_at": generated_at,
        "updated_at": generated_at,
        "decision_request": {
            "kind": "episode_council_continuation",
            "summary": "Council selected the next safe continuation path.",
            "decision_needed": "Record why this route was not escalated further.",
            "current_scope": "Next episode continuation only.",
            "non_authorized_scope": ["cleanup", "provider retargeting"]
        },
        "judgment_route": {
            "schema": "judgment_route.v1",
            "evaluator": "council",
            "reason": "Council compared reviewer outputs and found the decision stayed inside the approved scope.",
            "policy_basis": ["read-only council checkpoint"],
            "evidence_refs": [],
            "selected_by": "offdesk.council",
            "selected_at": generated_at,
            "default_if_no_reply": "continue"
        }
    });
    fs::write(
        profile_dir.join("offdesk_decisions.jsonl"),
        format!("{}\n", serde_json::to_string(&decision_record)?),
    )?;

    let output = forager_command(temp.path())
        .args([
            "ondesk",
            "prompt-package",
            "--project-key",
            "twinpaper",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(json["review_surface"]["schema"], "review_surface.v1");
    assert_eq!(
        json["review_surface"]["accepted_truth"]["receipt_acceptance_status"],
        "approved_with_followups"
    );
    assert_eq!(
        json["review_surface"]["closeout"]["source_observation"]["status"],
        "observed"
    );
    assert_eq!(
        json["review_surface"]["closeout"]["receipt_open_decisions"],
        1
    );
    assert_eq!(json["latest_closeout"]["closeout_id"], "closeout_test");
    assert_eq!(json["latest_closeout"]["review_verdict"], "approved");
    assert_eq!(
        json["latest_closeout"]["receipt_status"],
        "approved_with_followups"
    );
    assert_eq!(
        json["documentation_governance"]["source"],
        "latest_closeout_return_package"
    );
    let content = json["content"].as_str().expect("content string");
    assert!(content.contains("Morning Review Surface"));
    assert!(content.contains("accepted_truth: pending via closeout_receipt.v1"));
    assert!(content.contains("receipt_acceptance_status: approved_with_followups"));
    assert!(content.contains("open_decisions: 0 judgment-route, 1 closeout-receipt"));
    assert!(content.contains("source_observation:"));
    assert!(content.contains("status: observed from git_worktree against HEAD"));
    assert!(content.contains("interpretation: read-only source context"));
    assert!(content.contains("[modified] src/module03.rs (+8 -1)"));
    assert!(content.contains("closeout_implementation_packet_coverage:"));
    assert!(content.contains("detail_items: 1 completed, 0 deferred, 1 missing, 1 drifted"));
    assert!(content.contains("packet packet-twinpaper: completed"));
    assert!(content.contains(
        "work_slices: [drifted] receipt drifted slice (source: observed) (next: Revise the packet before accepting truth.)"
    ));
    assert!(content.contains("validation_items: [missing] missing-validation.txt"));
    assert!(content.contains("judgment_routes:"));
    assert!(content.contains("decision-council-route: council"));
    assert!(content.contains("Council compared reviewer outputs"));
    assert!(content.contains("artifact_index:"));
    assert!(content.contains("retention_review:"));
    assert!(content.contains("artifact_summaries"));
    assert!(content.contains("Documentation Governance Source"));
    assert!(content.contains("source: `latest_closeout_return_package`"));
    assert!(content.contains("Latest Offdesk Return Package"));
    assert!(content.contains("Night result summary"));
    assert!(content.contains("review_verdict: approved"));
    assert!(content.contains("closeout_receipt_status: approved_with_followups"));
    assert!(content.contains("closeout_acceptance: not accepted truth"));
    assert!(content.contains("review receipt follow-ups"));
    assert!(content.contains("[REDACTED]"));
    assert!(!content.contains("sk-secretsecretsecretsecret"));
    Ok(())
}

#[test]
#[serial]
fn ondesk_prompt_package_can_include_fresh_documentation_audit_without_closeout() -> Result<()> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    fs::create_dir_all(project.join("outputs"))?;
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
    fs::write(
        project.join("outputs").join("unpromoted-report.html"),
        "<h1>report</h1>",
    )?;

    let output = forager_command(temp.path())
        .current_dir(&project)
        .args([
            "ondesk",
            "prompt-package",
            "--project-key",
            "project",
            "--include-doc-audit",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        json["documentation_governance"]["source"],
        "fresh_project_audit"
    );
    assert_eq!(
        json["documentation_governance"]["requested_fresh_audit"],
        true
    );
    assert!(
        json["documentation_governance"]["recommendation_count"]
            .as_u64()
            .expect("recommendation count")
            >= 1
    );
    assert_eq!(
        json["documentation_governance"]["recommendations"][0]["kind"],
        "review_human_output_candidates"
    );
    let content = json["content"].as_str().expect("content string");
    assert!(content.contains("Documentation Governance Source"));
    assert!(content.contains("source: `fresh_project_audit`"));
    assert!(content.contains("review_human_output_candidates"));
    assert!(content.contains("outputs/unpromoted-report.html"));
    Ok(())
}

#[test]
#[serial]
fn ondesk_prompt_package_prefers_closeout_workdir_for_fresh_documentation_audit() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    let closeout_dir = profile_dir
        .join("offdesk_closeouts")
        .join("20260522T000000Z_closeout_twinpaper");
    fs::create_dir_all(&closeout_dir)?;

    let target_project = temp.path().join("twinpaper_project");
    fs::create_dir_all(target_project.join("outputs"))?;
    fs::write(target_project.join("README.md"), "# TwinPaper\n")?;
    fs::write(
        target_project.join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(target_project.join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        target_project.join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: project overview.\n",
    )?;
    fs::write(
        target_project.join("outputs").join("target-report.html"),
        "<h1>target</h1>",
    )?;

    let harness_cwd = temp.path().join("harness_cwd");
    fs::create_dir_all(harness_cwd.join("outputs"))?;
    fs::write(harness_cwd.join("README.md"), "# Harness\n")?;
    fs::write(
        harness_cwd.join("PROJECT_STATE.md"),
        format!("# Project State\n\nUpdated: {}\n", Utc::now().date_naive()),
    )?;
    fs::write(harness_cwd.join("DECISIONS.md"), "# Decisions\n")?;
    fs::write(
        harness_cwd.join("DELIVERABLES.md"),
        "# Deliverables\n\n- `README.md`: project overview.\n",
    )?;
    fs::write(
        harness_cwd.join("outputs").join("harness-report.html"),
        "<h1>harness</h1>",
    )?;

    let return_package_path = closeout_dir.join("RETURN_PACKAGE.md");
    fs::write(&return_package_path, "# Ondesk Return Package\n")?;
    fs::write(
        closeout_dir.join("closeout_plan.json"),
        serde_json::to_string_pretty(&json!({
            "closeout_id": "closeout_twinpaper",
            "generated_at": Utc::now(),
            "filters": {
                "project_key": "twinpaper"
            },
            "documentation_governance": {
                "workdir": target_project.to_str().expect("utf-8 target path"),
                "audit_profile": "standard"
            },
            "tasks": [
                {
                    "project_key": "twinpaper",
                    "request_id": "request",
                    "task_id": "task",
                    "workdir": harness_cwd.to_str().expect("utf-8 harness path")
                }
            ],
            "artifacts": {
                "return_package_markdown": return_package_path
            }
        }))?,
    )?;

    let output = forager_command(temp.path())
        .current_dir(&harness_cwd)
        .args([
            "ondesk",
            "prompt-package",
            "--project-key",
            "twinpaper",
            "--include-doc-audit",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        json["documentation_governance"]["project_path"],
        target_project.to_string_lossy().as_ref()
    );
    assert_eq!(
        json["documentation_governance"]["source"],
        "fresh_project_audit"
    );
    let content = json["content"].as_str().expect("content string");
    assert!(content.contains("target-report.html"));
    assert!(!content.contains("harness-report.html"));
    Ok(())
}

#[test]
#[serial]
fn ondesk_prompt_package_includes_latest_project_initialization() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    let init_dir = profile_dir
        .join("project_initializations")
        .join("20260521T000000Z_twinpaper");
    fs::create_dir_all(&init_dir)?;
    let generated_at = Utc::now();
    let start_package_path = init_dir.join("ONDESK_START_PACKAGE.md");
    let ready_check_path = init_dir.join("OFFDESK_READY_CHECK.json");
    let module_preflight_path = init_dir.join("MODULE_OPERATION_PREFLIGHT.json");
    fs::write(
        &start_package_path,
        "# Ondesk Start Package\n\nRead Module03 first token=sk-secretsecretsecretsecret\n",
    )?;
    fs::write(
        &ready_check_path,
        serde_json::to_string_pretty(&json!({
            "ready_for_ondesk_start": true,
            "ready_for_offdesk_runtime": false,
            "requires_operator_review": true
        }))?,
    )?;
    fs::write(
        &module_preflight_path,
        serde_json::to_string_pretty(&json!({
            "kind": "forager_module_operation_preflight",
            "ready_for_offdesk_runtime": false,
            "blockers": [
                "operator_review_required_before_runtime_enqueue",
                "module_operation_profile_requires_review"
            ],
            "operation_targets": [
                {
                    "scope_ref": "module03_regspec_machine",
                    "readiness_level": "known_profile_builder_available",
                    "recognized_profile_kind": "twinpaper_module03_regspec_machine",
                    "profile_builder_available": true,
                    "evidence_bundle_builder_available": true,
                    "evidence_review_builder_available": true,
                    "blockers": ["evidence_bundle_requires_review"],
                    "recommended_commands": [
                        {
                            "purpose": "build_evidence_bundle",
                            "command": "scripts/build_bundle.py --token sk-secretsecretsecretsecret"
                        },
                        {
                            "purpose": "review_evidence_bundle",
                            "command": "scripts/review_bundle.py"
                        }
                    ]
                }
            ]
        }))?,
    )?;
    fs::write(
        init_dir.join("PROJECT_OPERATION_PROFILE.json"),
        serde_json::to_string_pretty(&json!({
            "kind": "forager_project_operation_profile",
            "id": "project-init-test",
            "generated_at": generated_at,
            "project_key": "twinpaper",
            "scope_model": {
                "operation_targets": [
                    {
                        "scope_ref": "module03_regspec_machine",
                        "role": "module_operation_target"
                    }
                ]
            },
            "ondesk_start_package_path": start_package_path,
            "offdesk_ready_check_path": ready_check_path,
            "module_operation_preflight_path": module_preflight_path
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "ondesk",
            "prompt-package",
            "--project-key",
            "twinpaper",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        json["latest_project_initialization"]["initialization_id"],
        "project-init-test"
    );
    assert_eq!(
        json["latest_project_initialization"]["operation_targets"][0],
        "module03_regspec_machine"
    );
    assert_eq!(
        json["latest_project_initialization"]["ready_for_ondesk_start"],
        true
    );
    assert_eq!(
        json["latest_project_initialization"]["ready_for_offdesk_runtime"],
        false
    );
    assert_eq!(
        json["latest_project_initialization"]["module_operation_preflight"]
            ["ready_for_offdesk_runtime"],
        false
    );
    assert_eq!(
        json["latest_project_initialization"]["module_operation_preflight"]["blocker_count"],
        2
    );
    assert_eq!(
        json["latest_project_initialization"]["module_operation_preflight"]["operation_targets"][0]
            ["readiness_level"],
        "known_profile_builder_available"
    );
    assert_eq!(
        json["latest_project_initialization"]["module_operation_preflight"]["operation_targets"][0]
            ["recognized_profile_kind"],
        "twinpaper_module03_regspec_machine"
    );
    assert_eq!(
        json["latest_project_initialization"]["module_operation_preflight"]["operation_targets"][0]
            ["recommended_command_purposes"][0],
        "build_evidence_bundle"
    );
    let content = json["content"].as_str().expect("content string");
    assert!(content.contains("Latest Project Initialization"));
    assert!(content.contains("Latest Module Operation Preflight"));
    assert!(content.contains("module03_regspec_machine"));
    assert!(content.contains("known_profile_builder_available"));
    assert!(content.contains("build_evidence_bundle"));
    assert!(content.contains("Read Module03 first"));
    assert!(content.contains("[REDACTED]"));
    assert!(!content.contains("sk-secretsecretsecretsecret"));
    assert!(!content.contains("scripts/build_bundle.py"));
    Ok(())
}

#[test]
#[serial]
fn ondesk_prompt_package_includes_latest_implementation_packet() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    let packet_dir = profile_dir
        .join("implementation_packets")
        .join("20260603T000000Z_twinpaper");
    fs::create_dir_all(&packet_dir)?;
    let generated_at = Utc::now();
    let packet = json!({
        "schema": "implementation_packet.v1",
        "packet_id": "implementation-packet-test",
        "created_at": generated_at,
        "project_key": "twinpaper",
        "project_root": temp.path().display().to_string(),
        "source_intent": {
            "user_goal": "Project the implementation packet into Ondesk surfaces.",
            "why_now": "The packet is not useful until a returning operator can see it.",
            "success_state": "The prompt package explains delegation readiness without opening raw JSON."
        },
        "alignment": {
            "north_star_fit": "Keeps implementation anchored to the user's original intent.",
            "brand_fit": "Supports design-first harness work.",
            "product_boundary": "Read-only projection; no runtime authority is granted.",
            "anti_drift_notes": ["Do not treat execution as accepted truth."]
        },
        "scope": {
            "included": ["review_surface projection", "ondesk prompt rendering"],
            "excluded": ["runtime launch authority"],
            "allowed_files": ["src/cli/review_surface.rs", "src/cli/ondesk.rs"],
            "mutation_boundary": "Only prompt and review-surface summary fields are changed.",
            "non_authorized_actions": ["Do not enqueue offdesk work."]
        },
        "capability_mapping": [
            {
                "capability_id": "FD-016",
                "reason": "Ondesk handoff should surface planning state."
            }
        ],
        "design": {
            "approach": "Summarize the latest packet by project key.",
            "work_slices": ["scan packet directory", "render prompt section"],
            "interfaces": ["review_surface.v1", "ondesk prompt-package"],
            "data_contracts": ["implementation_packet.v1"],
            "compatibility_notes": ["Missing packet means the field is omitted."]
        },
        "execution": {
            "preferred_worker": "hosted_harness",
            "worker_requirements": ["read-only packet projection"],
            "commands": ["cargo test --test ondesk_cli ondesk_prompt_package_includes_latest_implementation_packet"],
            "stop_conditions": ["implementation packet is not parseable"],
            "rollback_or_recovery": ["remove the optional prompt section"]
        },
        "validation": {
            "tests": ["cargo test --test ondesk_cli ondesk_prompt_package_includes_latest_implementation_packet"],
            "smoke_checks": ["forager ondesk prompt-package --project-key twinpaper --json"],
            "manual_review": ["confirm prompt content carries user-facing decision context"],
            "evidence_required": ["prompt package JSON contains implementation_packet"]
        },
        "closeout": {
            "expected_artifacts": ["IMPLEMENTATION_PACKET.json", "RECURSIVE_ALIGNMENT_REVIEW.json", "IMPLEMENTATION_PACKET.md"],
            "accepted_truth_rule": "Execution completion is not acceptance.",
            "handoff_summary_requirements": ["state safe_to_delegate and required revisions"]
        },
        "recursive_alignment_review": {
            "schema": "recursive_alignment_review.v1",
            "reviewer": "deterministic_gate",
            "outcome": "pass",
            "checks": {
                "original_goal_coverage": "complete",
                "north_star_alignment": "complete",
                "brand_alignment": "complete",
                "scope_balance": "complete",
                "capability_coverage": "complete",
                "evidence_sufficiency": "complete",
                "completion_definition": "complete"
            },
            "drift_signals": [],
            "missing_decisions": [],
            "required_revisions": [],
            "safe_to_delegate": true
        }
    });
    fs::write(
        packet_dir.join("IMPLEMENTATION_PACKET.json"),
        serde_json::to_string_pretty(&packet)?,
    )?;
    fs::write(
        packet_dir.join("RECURSIVE_ALIGNMENT_REVIEW.json"),
        serde_json::to_string_pretty(&packet["recursive_alignment_review"])?,
    )?;
    fs::write(
        packet_dir.join("IMPLEMENTATION_PACKET.md"),
        "# Implementation Packet\n\nProject the implementation packet into Ondesk surfaces.\n",
    )?;

    let output = forager_command(temp.path())
        .args([
            "ondesk",
            "prompt-package",
            "--project-key",
            "twinpaper",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        json["review_surface"]["implementation_packet"]["packet_id"],
        "implementation-packet-test"
    );
    assert_eq!(
        json["review_surface"]["implementation_packet"]["outcome"],
        "pass"
    );
    assert_eq!(
        json["review_surface"]["implementation_packet"]["safe_to_delegate"],
        true
    );
    assert_eq!(
        json["review_surface"]["implementation_packet"]["work_slice_count"],
        2
    );
    assert_eq!(
        json["review_surface"]["sources"]["implementation_packet"],
        "implementation_packet.v1"
    );
    let content = json["content"].as_str().expect("content string");
    assert!(content.contains("implementation_packet:"));
    assert!(content.contains("implementation-packet-test"));
    assert!(content.contains("safe_to_delegate=true"));
    assert!(content.contains("worker=hosted_harness"));
    assert!(content.contains("Project the implementation packet into Ondesk surfaces."));
    Ok(())
}

#[test]
#[serial]
fn ondesk_capture_writes_artifacts_without_requiring_running_tmux() -> Result<()> {
    let temp = tempdir()?;
    let project = temp.path().join("project");
    fs::create_dir_all(&project)?;

    let add_output = forager_command(temp.path())
        .args([
            "add",
            project.to_str().expect("utf-8 project path"),
            "--title",
            "codex-harness",
            "--cmd",
            "codex",
        ])
        .output()?;
    assert!(
        add_output.status.success(),
        "{}",
        String::from_utf8_lossy(&add_output.stderr)
    );

    let capture_output = forager_command(temp.path())
        .args([
            "ondesk",
            "capture",
            "codex-harness",
            "--project-key",
            "project",
            "--lines",
            "50",
            "--json",
        ])
        .output()?;
    assert!(
        capture_output.status.success(),
        "{}",
        String::from_utf8_lossy(&capture_output.stderr)
    );
    let json: Value = serde_json::from_slice(&capture_output.stdout)?;
    assert_eq!(json["project_key"], "project");
    assert_eq!(json["session_running"], false);

    let capture_path = PathBuf::from(json["capture_path"].as_str().expect("capture path"));
    let prompt_path = PathBuf::from(
        json["prompt_package_path"]
            .as_str()
            .expect("prompt package path"),
    );
    assert!(capture_path.exists());
    assert!(prompt_path.exists());

    let capture: Value = serde_json::from_str(&fs::read_to_string(capture_path)?)?;
    assert_eq!(capture["session"]["title"], "codex-harness");
    assert_eq!(capture["scrollback"], "");

    let prompt = fs::read_to_string(prompt_path)?;
    assert!(prompt.contains("Captured Harness Scrollback"));
    assert!(prompt.contains("No live tmux scrollback was available"));
    Ok(())
}
