use anyhow::Result;
use chrono::{Duration, Utc};
use fs2::FileExt;
use serde_json::json;
use serial_test::serial;
use sha2::{Digest, Sha256};
use std::fs;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::Duration as StdDuration;
use tempfile::tempdir;

fn legacy_aoe_command(home: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_aoe"));
    command.env("HOME", home);
    command.env_remove("FORAGER_PROFILE");
    command.env_remove("AGENT_OF_EMPIRES_PROFILE");
    command.env_remove("FORAGER_DEBUG");
    command.env_remove("AGENT_OF_EMPIRES_DEBUG");
    #[cfg(target_os = "linux")]
    command.env("XDG_CONFIG_HOME", home.join(".config"));
    command
}

fn forager_command(home: &std::path::Path) -> Command {
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

fn app_dir(home: &std::path::Path) -> std::path::PathBuf {
    #[cfg(target_os = "linux")]
    {
        home.join(".config").join("forager")
    }
    #[cfg(not(target_os = "linux"))]
    {
        home.join(".forager")
    }
}

fn legacy_app_dir(home: &std::path::Path) -> std::path::PathBuf {
    #[cfg(target_os = "linux")]
    {
        home.join(".config").join("agent-of-empires")
    }
    #[cfg(not(target_os = "linux"))]
    {
        home.join(".agent-of-empires")
    }
}

fn normalize_test_path(path: &str) -> String {
    #[cfg(target_os = "macos")]
    {
        path.strip_prefix("/private").unwrap_or(path).to_owned()
    }
    #[cfg(not(target_os = "macos"))]
    {
        path.to_owned()
    }
}

fn expected_path(path: &Path) -> String {
    normalize_test_path(&path.display().to_string())
}

fn reported_path(value: &serde_json::Value) -> String {
    normalize_test_path(value.as_str().expect("path string"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

fn profile_dir(home: &std::path::Path) -> std::path::PathBuf {
    profile_dir_for(home, "default")
}

fn profile_dir_for(home: &std::path::Path, profile: &str) -> std::path::PathBuf {
    #[cfg(target_os = "linux")]
    {
        home.join(".config")
            .join("forager")
            .join("profiles")
            .join(profile)
    }
    #[cfg(not(target_os = "linux"))]
    {
        home.join(".forager").join("profiles").join(profile)
    }
}

fn write_implementation_packet_fixture(
    home: &std::path::Path,
    project_key: &str,
    packet_id: &str,
) -> Result<std::path::PathBuf> {
    let packet_dir = profile_dir(home)
        .join("implementation_packets")
        .join(format!("20260603T000000Z_{project_key}"));
    fs::create_dir_all(&packet_dir)?;
    let packet = json!({
        "schema": "implementation_packet.v1",
        "packet_id": packet_id,
        "created_at": Utc::now(),
        "project_key": project_key,
        "project_root": home.display().to_string(),
        "source_intent": {
            "user_goal": "Bind delegated Offdesk execution to the original design intent.",
            "why_now": "The queued task should carry its design packet into launch records.",
            "success_state": "Task JSON and background records expose packet readiness without raw path-first output."
        },
        "alignment": {
            "north_star_fit": "Keeps delegated work tied to the project direction.",
            "brand_fit": "Supports design-first harness execution.",
            "product_boundary": "Metadata only; no runtime authority is granted.",
            "anti_drift_notes": ["Do not treat packet presence as execution approval."]
        },
        "scope": {
            "included": ["task metadata", "background probe metadata"],
            "excluded": ["approval bypass"],
            "allowed_files": ["src/offdesk/task_queue.rs", "src/offdesk/background.rs"],
            "mutation_boundary": "Attach optional summary only.",
            "non_authorized_actions": ["Do not launch without existing gate behavior."]
        },
        "capability_mapping": [
            {
                "capability_id": "FD-016",
                "reason": "Substantial delegated work needs an implementation packet."
            }
        ],
        "design": {
            "approach": "Attach latest project packet to queued task records.",
            "work_slices": ["resolve latest packet", "attach artifact refs"],
            "interfaces": ["offdesk task JSON", "background probe JSON"],
            "data_contracts": ["implementation_packet.v1"],
            "compatibility_notes": ["Missing packet leaves the field absent."]
        },
        "execution": {
            "preferred_worker": "hosted_harness",
            "worker_requirements": ["packet is metadata only"],
            "commands": ["forager offdesk enqueue ... --json"],
            "stop_conditions": ["packet project key mismatch"],
            "rollback_or_recovery": ["omit implementation_packet field"]
        },
        "validation": {
            "tests": ["cargo test --test offdesk_cli offdesk_enqueue_tasks_json_redacts_command"],
            "smoke_checks": ["forager offdesk tasks --json"],
            "manual_review": ["confirm no raw JSON path is primary surface"],
            "evidence_required": ["task artifact refs include implementation packet"]
        },
        "closeout": {
            "expected_artifacts": ["IMPLEMENTATION_PACKET.json", "RECURSIVE_ALIGNMENT_REVIEW.json", "IMPLEMENTATION_PACKET.md"],
            "accepted_truth_rule": "Execution completion is not acceptance.",
            "handoff_summary_requirements": ["state packet outcome and safe_to_delegate"]
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
        "# Implementation Packet\n\nBind delegated Offdesk execution to design intent.\n",
    )?;
    Ok(packet_dir)
}

fn legacy_profile_dir_for(home: &std::path::Path, profile: &str) -> std::path::PathBuf {
    #[cfg(target_os = "linux")]
    {
        home.join(".config")
            .join("agent-of-empires")
            .join("profiles")
            .join(profile)
    }
    #[cfg(not(target_os = "linux"))]
    {
        home.join(".agent-of-empires")
            .join("profiles")
            .join(profile)
    }
}

fn wait_for_path(path: &Path) {
    for _ in 0..40 {
        if path.exists() {
            return;
        }
        thread::sleep(StdDuration::from_millis(50));
    }
}

fn durable_task(
    status: &str,
    now: chrono::DateTime<Utc>,
    command: &str,
    workdir: &Path,
) -> serde_json::Value {
    durable_task_with("task", "inspect.status", status, now, command, workdir)
}

fn durable_task_with(
    task_id: &str,
    capability_id: &str,
    status: &str,
    now: chrono::DateTime<Utc>,
    command: &str,
    workdir: &Path,
) -> serde_json::Value {
    json!({
        "task_id": task_id,
        "request_id": "request",
        "project_key": "project",
        "status": status,
        "capability_id": capability_id,
        "runner_kind": "local_background",
        "command": command,
        "workdir": workdir.to_str().expect("utf-8 path"),
        "background_ticket_id": "ticket",
        "attempt_count": 1,
        "last_gate_status": "denied",
        "last_error": "token=sk-secretsecretsecretsecret",
        "created_at": now,
        "updated_at": now,
        "preview": "preview token=sk-secretsecretsecretsecret",
        "reason": "reason token=sk-secretsecretsecretsecret"
    })
}

fn denied_approval(action: &str, now: chrono::DateTime<Utc>) -> serde_json::Value {
    json!({
        "approval_id": "approval_denied",
        "status": "denied",
        "scope": "once",
        "project_key": "project",
        "request_id": "request",
        "task_id": "task",
        "action": action,
        "risk_level": "runtime_mutation",
        "approval_mode": "operator_required",
        "preview": "safe preview",
        "reason": "operator denied",
        "created_at": now,
        "expires_at": now + Duration::minutes(10),
        "resolved_at": now,
        "resolved_by": "operator",
        "source_surface": "test"
    })
}

fn resume_state(now: chrono::DateTime<Utc>) -> serde_json::Value {
    json!({
        "task_id": "task",
        "request_id": "request",
        "project_key": "project",
        "status": "resume_pending",
        "phase": "background",
        "runner_target": "local_background",
        "background_ticket_id": "ticket",
        "last_evidence_artifacts": [],
        "next_safe_resume_step": "inspect result sidecar",
        "interrupted_at": now,
        "interruption_reason": "restart token=sk-secretsecretsecretsecret",
        "fresh_until": now + Duration::minutes(10)
    })
}

#[test]
#[serial]
fn offdesk_pending_and_ok_resolve_approval() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("pending_action_approvals.json"),
        serde_json::to_string_pretty(&json!([
            {
                "approval_id": "approval_one",
                "status": "pending",
                "scope": "once",
                "project_key": "project",
                "request_id": "request",
                "task_id": "task",
                "action": "dispatch.runtime",
                "risk_level": "runtime_mutation",
                "approval_mode": "operator_required",
                "preview": "safe preview",
                "reason": "outside envelope",
                "created_at": now,
                "expires_at": now + Duration::minutes(10),
                "source_surface": "test"
            }
        ]))?,
    )?;

    let pending_output = forager_command(temp.path())
        .args(["offdesk", "pending", "--json"])
        .output()?;
    assert!(pending_output.status.success());
    let pending: serde_json::Value = serde_json::from_slice(&pending_output.stdout)?;
    assert_eq!(pending.as_array().expect("array").len(), 1);
    assert_eq!(pending[0]["action_id"], serde_json::Value::Null);
    assert_eq!(pending[0]["next_safe_action"]["kind"], "approval_pending");
    assert!(pending[0]["next_safe_action"]["commands"]
        .as_array()
        .expect("next action commands")
        .iter()
        .any(|command| command
            .as_str()
            .expect("next action command")
            .contains("forager offdesk ok approval_one")));

    let ok_output = forager_command(temp.path())
        .args(["offdesk", "ok", "approval_one", "--json"])
        .output()?;
    assert!(ok_output.status.success());
    let approved: serde_json::Value = serde_json::from_slice(&ok_output.stdout)?;
    assert_eq!(approved["status"], "approved");
    assert_eq!(approved["approval_id"], "approval_one");

    let audit = fs::read_to_string(profile_dir.join("action_audit.jsonl"))?;
    assert!(audit.contains("\"transition\":\"approve\""));
    assert!(audit.contains("\"action_id\":\"approval_one\""));
    assert!(audit.contains("\"result\":\"approved\""));
    assert!(audit.contains("\"resolved_by\":\"cli\""));
    Ok(())
}

#[test]
#[serial]
fn offdesk_decisions_lists_and_shows_decision_records() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let pending = json!({
        "schema": "decision_record.v1",
        "decision_id": "decision-user",
        "project_key": "project",
        "request_id": "request",
        "task_id": "task",
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
            ]
        },
        "council_review": {
            "recommendation": "revise",
            "agreement": true,
            "reviewer_decisions": {
                "claude": "revise",
                "gpt": "revise"
            }
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
    let internal = json!({
        "schema": "decision_record.v1",
        "decision_id": "decision-auto",
        "project_key": "project",
        "request_id": "request",
        "task_id": "other-task",
        "raised_by": "council",
        "source_surface": "offdesk.council",
        "materiality": "low",
        "status": "auto_resolved",
        "created_at": now,
        "updated_at": now,
        "decision_request": {
            "kind": "verification_order",
            "summary": "Council selected the approved verification command.",
            "decision_needed": "Select verification order.",
            "current_scope": "Approved task contract.",
            "non_authorized_scope": []
        },
        "route": {
            "materiality": "low",
            "target": "agent",
            "reason": "Covered by existing task policy."
        }
    });
    fs::write(
        profile_dir.join("offdesk_decisions.jsonl"),
        format!(
            "{}\n{}\n",
            serde_json::to_string(&pending)?,
            serde_json::to_string(&internal)?
        ),
    )?;

    let list_output = forager_command(temp.path())
        .args(["offdesk", "decisions", "--status", "user_pending", "--json"])
        .output()?;
    assert!(
        list_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&list_output.stderr)
    );
    let list: serde_json::Value = serde_json::from_slice(&list_output.stdout)?;
    assert_eq!(list.as_array().expect("array").len(), 1);
    assert_eq!(list[0]["record"]["decision_id"], "decision-user");
    assert_eq!(
        list[0]["record"]["approval_brief"]["schema"],
        "approval_brief.v1"
    );
    assert_eq!(list[0]["validation_issues"], json!([]));

    let show_output = forager_command(temp.path())
        .args(["offdesk", "decision", "show", "decision-user", "--json"])
        .output()?;
    assert!(
        show_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&show_output.stderr)
    );
    let shown: serde_json::Value = serde_json::from_slice(&show_output.stdout)?;
    assert_eq!(shown["record"]["status"], "user_pending");
    assert_eq!(shown["record"]["route"]["target"], "user");
    Ok(())
}

#[test]
#[serial]
fn offdesk_decision_resolve_and_receipt_append_handoff_records() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let pending = json!({
        "schema": "decision_record.v1",
        "decision_id": "decision-handoff",
        "project_key": "project",
        "request_id": "request",
        "task_id": "task",
        "raised_by": "council",
        "source_surface": "offdesk.council",
        "materiality": "high",
        "status": "user_pending",
        "created_at": now,
        "updated_at": now,
        "decision_request": {
            "kind": "episode_council_continuation",
            "summary": "Council recommends revising before continuing.",
            "decision_needed": "Choose whether to continue, revise, block, or stop.",
            "why_now": ["Council did not return continue."],
            "current_scope": "Next episode only.",
            "non_authorized_scope": [
                "provider retargeting",
                "cleanup",
                "wiki promotion"
            ]
        },
        "council_review": {
            "recommendation": "revise",
            "agreement": true,
            "reviewer_decisions": {
                "claude": "revise",
                "gpt": "revise"
            }
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
        format!("{}\n", serde_json::to_string(&pending)?),
    )?;

    let resolve_output = forager_command(temp.path())
        .args([
            "offdesk",
            "decision",
            "resolve",
            "decision-handoff",
            "--decision",
            "revise",
            "--note",
            "Diagnose primary gate failure before continuing.",
            "--json",
        ])
        .output()?;
    assert!(
        resolve_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&resolve_output.stderr)
    );
    let resolved: serde_json::Value = serde_json::from_slice(&resolve_output.stdout)?;
    assert_eq!(resolved["record"]["status"], "handoff_ready");
    assert_eq!(
        resolved["record"]["execution_handoff"]["approved_direction"],
        "revise"
    );
    assert_eq!(resolved["record"]["execution_handoff"]["target"], "agent");
    assert!(resolved["record"]["execution_handoff"]["instructions"]
        .as_array()
        .expect("handoff instructions")
        .iter()
        .any(|line| line
            .as_str()
            .unwrap_or_default()
            .contains("Diagnose primary gate failure")));
    assert_eq!(resolved["validation_issues"], json!([]));

    let receipt_output = forager_command(temp.path())
        .args([
            "offdesk",
            "decision",
            "receipt",
            "decision-handoff",
            "--result-status",
            "applied",
            "--evidence",
            "Handoff was reviewed by the next harness.",
            "--remaining-review",
            "Closeout still needs final acceptance.",
            "--json",
        ])
        .output()?;
    assert!(
        receipt_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&receipt_output.stderr)
    );
    let receipted: serde_json::Value = serde_json::from_slice(&receipt_output.stdout)?;
    assert_eq!(receipted["record"]["status"], "receipted");
    assert_eq!(
        receipted["record"]["decision_receipt"]["final_decision"],
        "revise"
    );
    assert_eq!(
        receipted["record"]["decision_receipt"]["result_status"],
        "applied"
    );
    assert_eq!(receipted["validation_issues"], json!([]));

    let ledger = fs::read_to_string(profile_dir.join("offdesk_decisions.jsonl"))?;
    assert_eq!(ledger.lines().count(), 3);
    assert!(ledger.contains("\"status\":\"handoff_ready\""));
    assert!(ledger.contains("\"status\":\"receipted\""));
    Ok(())
}

#[test]
#[serial]
fn offdesk_decision_ingest_telegram_appends_profile_handoff_and_receipt() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let artifact_dir = temp.path().join("relay");
    fs::create_dir_all(&artifact_dir)?;
    let now = Utc::now();
    let decision_record = json!({
        "schema": "decision_record.v1",
        "decision_id": "decision-telegram",
        "project_key": "project",
        "request_id": "request",
        "task_id": "task",
        "raised_by": "council",
        "source_surface": "offdesk.council",
        "materiality": "high",
        "status": "user_pending",
        "created_at": now,
        "updated_at": now,
        "decision_request": {
            "kind": "episode_council_continuation",
            "summary": "Council needs a revised direction.",
            "decision_needed": "Choose the next direction.",
            "why_now": ["Council returned revise."],
            "current_scope": "Next episode only.",
            "non_authorized_scope": ["cleanup", "provider retargeting"]
        },
        "route": {
            "materiality": "high",
            "target": "user",
            "reason": "The next direction changes.",
            "default_if_no_reply": "defer"
        },
        "approval_brief": {
            "schema": "approval_brief.v1",
            "recommendation": "revise",
            "subject": "council continuation decision",
            "summary_lines": ["Council needs a revised direction."],
            "scope": "Only approves the next episode direction.",
            "question": "How should the run proceed?"
        }
    });
    let request_path = artifact_dir.join("request.json");
    fs::write(
        &request_path,
        serde_json::to_string_pretty(&json!({
            "decision_request_id": "request:episode-001:council",
            "decision_record": decision_record,
            "approval_brief": {
                "schema": "approval_brief.v1",
                "recommendation": "revise",
                "subject": "council continuation decision",
                "summary_lines": ["Council needs a revised direction."],
                "scope": "Only approves the next episode direction.",
                "question": "How should the run proceed?"
            }
        }))?,
    )?;
    let result_path = artifact_dir.join("telegram_decision.json");
    fs::write(
        &result_path,
        serde_json::to_string_pretty(&json!({
            "status": "accepted",
            "decision": "revise",
            "reason": "Focus on the failed primary gate before continuing.",
            "input_mode": "callback_then_message"
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "decision",
            "ingest-telegram",
            "--request",
            request_path.to_str().expect("utf8 request path"),
            "--result",
            result_path.to_str().expect("utf8 result path"),
            "--receipt-result-status",
            "applied",
            "--receipt-evidence",
            "Telegram reply was consumed by the workload control loop.",
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["decision_id"], "decision-telegram");
    assert_eq!(report["telegram_status"], "accepted");
    assert_eq!(report["telegram_decision"], "revise");
    assert_eq!(
        report["appended_records"],
        json!(["user_pending", "handoff_ready", "receipted"])
    );
    assert_eq!(report["record"]["status"], "receipted");
    assert_eq!(
        report["record"]["execution_handoff"]["approved_direction"],
        "revise"
    );
    assert_eq!(
        report["record"]["decision_receipt"]["result_status"],
        "applied"
    );
    assert_eq!(report["validation_issues"], json!([]));

    let ledger = fs::read_to_string(profile_dir.join("offdesk_decisions.jsonl"))?;
    assert_eq!(ledger.lines().count(), 3);
    assert!(ledger.contains("Focus on the failed primary gate"));
    assert!(ledger.contains("\"status\":\"receipted\""));
    Ok(())
}

#[test]
#[serial]
fn offdesk_decision_ingest_telegram_feedback_creates_reviewable_inbox_item() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let artifact_dir = temp.path().join("relay");
    fs::create_dir_all(&artifact_dir)?;
    let old_feedback = json!({
        "schema": "remote_operator_telegram_feedback.v1",
        "received_at": Utc::now(),
        "profile": "default",
        "chat_id_hash": "sha256:old-chat",
        "user_id_hash": "sha256:old-user",
        "message_id": 1,
        "feedback_text": "old feedback",
        "target_chat_id_hash": "sha256:old-chat",
        "feedback_context": serde_json::Value::Null
    });
    let feedback = json!({
        "schema": "remote_operator_telegram_feedback.v1",
        "received_at": Utc::now(),
        "profile": "default",
        "chat_id_hash": "sha256:chat",
        "user_id_hash": "sha256:user",
        "message_id": 777,
        "feedback_text": "실패 조건 보강 필요",
        "target_chat_id_hash": "sha256:chat",
        "feedback_context": {
            "schema": "telegram_interaction_context.v1",
            "command": "plans",
            "profile": "default",
            "context_kind": "plan_attention",
            "focus_kind": "plan",
            "focus_ref": "plan_harness_mobile",
            "focus_label": "수정 필요",
            "next_command": "/show plan_harness_mobile",
            "project_key": "project",
            "request_id": "request",
            "task_id": "task"
        }
    });
    let feedback_path = artifact_dir.join("feedback.jsonl");
    fs::write(
        &feedback_path,
        format!(
            "{}\n{}\n",
            serde_json::to_string(&old_feedback)?,
            serde_json::to_string(&feedback)?
        ),
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "decision",
            "ingest-telegram-feedback",
            "--feedback",
            feedback_path.to_str().expect("utf8 feedback path"),
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let decision_id = report["decision_id"].as_str().expect("decision id");
    assert!(decision_id.starts_with("telegram-feedback-"));
    assert_eq!(report["appended"], true);
    assert_eq!(report["record"]["status"], "user_pending");
    assert_eq!(report["record"]["materiality"], "medium");
    assert_eq!(
        report["record"]["source_surface"],
        "telegram.remote_operator.feedback"
    );
    assert_eq!(
        report["record"]["decision_request"]["kind"],
        "telegram_operator_feedback"
    );
    assert_eq!(report["record"]["route"]["target"], "user");
    assert_eq!(
        report["record"]["approval_brief"]["source"],
        "telegram.remote_operator.feedback"
    );
    assert!(report["record"]["decision_request"]["non_authorized_scope"]
        .as_array()
        .expect("non-authorized scope")
        .iter()
        .any(|scope| scope.as_str() == Some("approval resolution")));
    assert_eq!(report["validation_issues"], json!([]));
    let ledger_path = profile_dir.join("offdesk_decisions.jsonl");
    let ledger = fs::read_to_string(&ledger_path)?;
    assert_eq!(ledger.lines().count(), 1);

    let mut replay_feedback = feedback.clone();
    replay_feedback["received_at"] = json!(Utc::now() + Duration::seconds(5));
    fs::write(
        &feedback_path,
        format!("{}\n", serde_json::to_string(&replay_feedback)?),
    )?;

    let duplicate_output = forager_command(temp.path())
        .args([
            "offdesk",
            "decision",
            "ingest-telegram-feedback",
            "--feedback",
            feedback_path.to_str().expect("utf8 feedback path"),
            "--json",
        ])
        .output()?;
    assert!(
        duplicate_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&duplicate_output.stderr)
    );
    let duplicate_report: serde_json::Value = serde_json::from_slice(&duplicate_output.stdout)?;
    assert_eq!(duplicate_report["decision_id"], decision_id);
    assert_eq!(duplicate_report["appended"], false);
    let ledger = fs::read_to_string(&ledger_path)?;
    assert_eq!(ledger.lines().count(), 1);

    let resolve_output = forager_command(temp.path())
        .args([
            "offdesk",
            "decision",
            "resolve",
            decision_id,
            "--decision",
            "revise",
            "--note",
            "Tighten the mobile feedback summary before the next offdesk run.",
            "--json",
        ])
        .output()?;
    assert!(
        resolve_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&resolve_output.stderr)
    );
    let resolved: serde_json::Value = serde_json::from_slice(&resolve_output.stdout)?;
    assert_eq!(resolved["record"]["status"], "handoff_ready");
    assert_eq!(
        resolved["record"]["execution_handoff"]["approved_direction"],
        "revise"
    );

    let receipt_output = forager_command(temp.path())
        .args([
            "offdesk",
            "decision",
            "receipt",
            decision_id,
            "--result-status",
            "reviewed",
            "--evidence",
            "Telegram feedback was reviewed by the planning harness.",
            "--json",
        ])
        .output()?;
    assert!(
        receipt_output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&receipt_output.stderr)
    );
    let receipted: serde_json::Value = serde_json::from_slice(&receipt_output.stdout)?;
    assert_eq!(receipted["record"]["status"], "receipted");
    assert_eq!(
        receipted["record"]["decision_receipt"]["result_status"],
        "reviewed"
    );
    let ledger = fs::read_to_string(&ledger_path)?;
    assert_eq!(ledger.lines().count(), 3);
    Ok(())
}

#[test]
fn offdesk_decision_ingest_telegram_planning_request_is_not_generic_feedback() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let artifact_dir = temp.path().join("relay");
    fs::create_dir_all(&artifact_dir)?;
    let feedback = json!({
        "schema": "remote_operator_telegram_feedback.v1",
        "received_at": Utc::now(),
        "profile": "default",
        "chat_id_hash": "sha256:chat",
        "user_id_hash": "sha256:user",
        "message_id": 706,
        "feedback_text": "nanoclustering Fractal tree 개발쪽을 자율주행으로 처리할 수 있을지 검토해볼까",
        "feedback_kind": "planning_request",
        "target_chat_id_hash": "sha256:chat",
        "feedback_context": {
            "schema": "telegram_interaction_context.v1",
            "command": "status",
            "profile": "default",
            "context_kind": "status_clear",
            "focus_kind": "none",
            "focus_label": "처리할 항목 없음"
        }
    });
    let feedback_path = artifact_dir.join("feedback.json");
    fs::write(&feedback_path, serde_json::to_string_pretty(&feedback)?)?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "decision",
            "ingest-telegram-feedback",
            "--feedback",
            feedback_path.to_str().expect("utf8 feedback path"),
            "--json",
        ])
        .output()?;
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["appended"], true);
    assert_eq!(report["record"]["status"], "user_pending");
    assert_eq!(report["record"]["materiality"], "medium");
    assert_eq!(
        report["record"]["source_surface"],
        "telegram.remote_operator.plan_request"
    );
    assert_eq!(
        report["record"]["decision_request"]["kind"],
        "telegram_operator_plan_request"
    );
    assert_eq!(report["record"]["approval_brief"]["recommendation"], "plan");
    assert!(report["record"]["approval_brief"]["summary_lines"][1]
        .as_str()
        .expect("summary line")
        .contains("no work has started"));
    assert!(report["record"]["decision_request"]["non_authorized_scope"]
        .as_array()
        .expect("non-authorized scope")
        .iter()
        .any(|scope| scope.as_str() == Some("background dispatch")));
    assert_eq!(report["validation_issues"], json!([]));
    Ok(())
}

#[test]
#[serial]
fn offdesk_decisions_report_validation_issues() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let invalid = json!({
        "schema": "decision_record.v1",
        "decision_id": "decision-invalid",
        "project_key": "project",
        "request_id": "request",
        "task_id": "task",
        "raised_by": "agent",
        "source_surface": "offdesk.council",
        "materiality": "high",
        "status": "user_pending",
        "created_at": now,
        "updated_at": now,
        "decision_request": {
            "kind": "council_escalation",
            "summary": "Council needs operator input.",
            "decision_needed": "Choose next direction.",
            "current_scope": "Next episode only.",
            "non_authorized_scope": []
        }
    });
    fs::write(
        profile_dir.join("offdesk_decisions.jsonl"),
        format!("{}\n", serde_json::to_string(&invalid)?),
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "decision", "show", "decision-invalid", "--json"])
        .output()?;
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let shown: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert!(shown["validation_issues"]
        .as_array()
        .expect("validation issues")
        .iter()
        .any(|issue| issue["code"] == "user_pending_without_approval_brief"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_resume_json_reports_artifacts() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("task_resume_state.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task",
                "request_id": "request",
                "project_key": "project",
                "status": "resume_pending",
                "phase": "background",
                "runner_target": "local_tmux",
                "last_evidence_artifacts": [],
                "next_safe_resume_step": "inspect result sidecar",
                "interrupted_at": now,
                "interruption_reason": "restart",
                "fresh_until": now + Duration::minutes(10)
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "resume", "--json"])
        .output()?;
    assert!(output.status.success());
    let states: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(states[0]["status"], "resume_pending");
    assert_eq!(states[0]["resume_id"], serde_json::Value::Null);
    assert_eq!(states[0]["next_safe_resume_step"], "inspect result sidecar");

    let human_output = forager_command(temp.path())
        .args(["offdesk", "resume"])
        .output()?;
    assert!(human_output.status.success());
    let stdout = String::from_utf8_lossy(&human_output.stdout);
    assert!(stdout.contains("resume_id: project:task"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_gate_creates_pending_approval_for_runtime_mutation_without_brief() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "gate",
            "dispatch.runtime",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--preview",
            "token=sk-secretsecretsecretsecret",
            "--reason",
            "outside envelope",
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let outcome: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcome["status"], "pending_approval");
    assert_eq!(outcome["approval"]["status"], "pending");
    assert!(outcome["approval"]["action_id"]
        .as_str()
        .expect("action id")
        .starts_with("action_"));
    assert!(!outcome["approval"]["preview"]
        .as_str()
        .expect("preview")
        .contains("sk-secret"));

    let approvals: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir(temp.path()).join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(approvals.as_array().expect("approvals").len(), 1);
    assert!(approvals[0]["action_id"]
        .as_str()
        .expect("action id")
        .starts_with("action_"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_gate_proceeds_for_runtime_mutation_inside_execution_brief() -> Result<()> {
    let temp = tempdir()?;
    let brief_path = temp.path().join("brief.json");
    let now = Utc::now();
    fs::write(
        &brief_path,
        serde_json::to_string_pretty(&json!({
            "request_id": "request",
            "task_id": "task",
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["dispatch.runtime"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(10)
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "gate",
            "dispatch.runtime",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--brief",
            brief_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let outcome: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcome["status"], "proceed");
    assert_eq!(outcome["approval_mode"], "envelope_auto");

    let approvals: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir(temp.path()).join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(approvals.as_array().expect("approvals").len(), 0);
    Ok(())
}

#[test]
#[serial]
fn offdesk_gate_blocks_provider_capacity_before_approval() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let retry_at = now + Duration::minutes(2);
    fs::write(
        profile_dir.join("provider_capacity.json"),
        serde_json::to_string_pretty(&json!([
            {
                "provider_id": "openai",
                "model": "gpt-4.1",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": retry_at,
                "last_error_summary": "rate limit",
                "updated_at": now
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "gate",
            "dispatch.runtime",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--provider-id",
            "openai",
            "--model",
            "gpt-4.1",
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let outcome: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcome["status"], "blocked");
    assert_eq!(outcome["retry_at"], serde_json::to_value(retry_at)?);
    assert_eq!(outcome["provider_capacity"]["provider_id"], "openai");
    assert_eq!(outcome["provider_capacity"]["model"], "gpt-4.1");
    assert_eq!(
        outcome["provider_capacity"]["matched_scope"],
        "provider_model"
    );
    assert_eq!(
        outcome["provider_fallback"]["current_provider_id"],
        "openai"
    );
    assert_eq!(outcome["provider_fallback"]["current_model"], "gpt-4.1");
    assert!(outcome["provider_fallback"]["candidates"]
        .as_array()
        .expect("fallback candidates")
        .iter()
        .all(
            |candidate| !(candidate["provider_id"] == "openai" && candidate["model"] == "gpt-4.1")
        ));
    assert!(!profile_dir.join("pending_action_approvals.json").exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_provider_fallback_json_is_operator_safe() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("provider_capacity.json"),
        serde_json::to_string_pretty(&json!([
            {
                "provider_id": "anthropic",
                "model": "claude-3-5-sonnet-latest",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": now + Duration::minutes(1),
                "last_error_summary": "rate limit",
                "updated_at": now
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .env_remove("OPENAI_API_KEY")
        .env_remove("ANTHROPIC_API_KEY")
        .args([
            "offdesk",
            "provider-fallback",
            "--provider-id",
            "openai",
            "--model",
            "gpt-4.1",
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let recommendation: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(recommendation["current_provider_id"], "openai");
    let candidates = recommendation["candidates"]
        .as_array()
        .expect("fallback candidates");
    assert!(candidates.iter().all(
        |candidate| !(candidate["provider_id"] == "openai" && candidate["model"] == "gpt-4.1")
    ));
    let anthropic = candidates
        .iter()
        .find(|candidate| {
            candidate["provider_id"] == "anthropic"
                && candidate["model"] == "claude-3-5-sonnet-latest"
        })
        .expect("anthropic candidate");
    assert_eq!(anthropic["auth_status"], "missing_auth");
    assert_eq!(anthropic["capacity_status"], "cooling_down");
    assert_eq!(anthropic["recommended"], false);
    assert!(!String::from_utf8_lossy(&output.stdout).contains("ANTHROPIC_API_KEY"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_provider_capacity_json_is_operator_safe() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    fs::write(
        profile_dir.join("provider_capacity.json"),
        serde_json::to_string_pretty(&json!([
            {
                "provider_id": "openai",
                "model": "gpt-4.1",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": now + Duration::minutes(1),
                "last_error_summary": format!("rate limit token={secret}"),
                "updated_at": now
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "provider-capacity", "--json"])
        .output()?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains(secret));
    let states: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(states[0]["provider_id"], "openai");
    assert_eq!(states[0]["status"], "cooling_down");
    Ok(())
}

#[test]
#[serial]
fn offdesk_capabilities_json_exposes_artifact_contracts() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .args(["offdesk", "capabilities", "--json"])
        .output()?;

    assert!(output.status.success());
    let capabilities: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let syncback = capabilities
        .as_array()
        .expect("capability array")
        .iter()
        .find(|capability| capability["capability_id"] == "canonical.syncback")
        .expect("canonical syncback capability");
    assert_eq!(syncback["approval_scope"], "once");
    assert_eq!(syncback["retry_eligible"], false);
    assert_eq!(syncback["resume_eligible"], false);
    assert!(syncback["required_artifacts"]
        .as_array()
        .expect("required artifacts")
        .iter()
        .any(|artifact| artifact["artifact_id"] == "mutation_snapshot"));

    let launch = capabilities
        .as_array()
        .expect("capability array")
        .iter()
        .find(|capability| capability["capability_id"] == "background.launch")
        .expect("background launch capability");
    assert!(launch["produced_artifacts"]
        .as_array()
        .expect("produced artifacts")
        .iter()
        .any(|artifact| artifact["artifact_id"] == "background_run"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_gate_json_includes_adaptive_wiki_projection() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_report",
                    "kind": "failure_pattern",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Keep report evidence separate",
                    "ai_instruction": "Confirm before merging evidence and recommendations. token=sk-secretsecretsecretsecret",
                    "human_summary": "Human-only summary should not be in AI projection",
                    "evidence_refs": ["task:one"],
                    "confidence": "repeated"
                },
                {
                    "id": "wiki_other_artifact",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "spreadsheet",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Spreadsheet rule",
                    "ai_instruction": "Do not include this.",
                    "confidence": "explicit"
                },
                {
                    "id": "wiki_deprecated",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "deprecated",
                    "activation_mode": "confirm",
                    "claim": "Deprecated report rule",
                    "ai_instruction": "Do not include deprecated entries.",
                    "confidence": "explicit"
                }
            ]
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "gate",
            "inspect.status",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--artifact-kind",
            "report",
            "--json",
        ])
        .output()?;
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(value["status"], "proceed");
    let wiki = value["adaptive_wiki"].as_array().expect("wiki projection");
    assert_eq!(wiki.len(), 1);
    assert_eq!(wiki[0]["id"], "wiki_report");
    let runtime = value["adaptive_wiki_runtime"]
        .as_array()
        .expect("runtime wiki projection");
    assert_eq!(runtime.len(), 1);
    assert_eq!(runtime[0]["id"], "wiki_report");
    assert_eq!(
        value["adaptive_wiki_runtime_policy"]["review_expired"],
        "warn"
    );
    assert_eq!(wiki[0]["activation_mode"], "confirm");
    assert!(wiki[0]["instruction"]
        .as_str()
        .unwrap()
        .contains("REDACTED"));
    assert!(!wiki[0]["instruction"]
        .as_str()
        .unwrap()
        .contains("sk-secret"));
    assert!(
        !serde_json::to_string(&wiki[0])?.contains("Human-only summary"),
        "AI projection must not include human summary"
    );
    Ok(())
}

#[test]
fn offdesk_deck_writes_marp_markdown_from_closeout_json() -> Result<()> {
    let temp = tempdir()?;
    let source = temp.path().join("closeout_plan.json");
    let deck = temp.path().join("closeout.marp.md");
    fs::write(
        &source,
        serde_json::to_string_pretty(&json!({
            "generated_at": "2026-06-11T00:00:00Z",
            "closeout_id": "closeout_alpha",
            "profile": "default",
            "artifact_dir": temp.path().join("closeout_alpha"),
            "summary": {
                "completed_tasks": 3,
                "active_or_blocked_tasks": 1,
                "missing_artifacts": 0,
                "return_package_required": true
            },
            "open_decisions": [
                {
                    "kind": "archive_review",
                    "detail": "Review generated logs before archive."
                }
            ],
            "verification_commands": [
                "forager offdesk tasks --json"
            ],
            "required_first_reads": [
                {
                    "path": "REPORT.md",
                    "reason": "Review result summary first."
                }
            ],
            "artifacts": {
                "closeout_plan_json": "closeout_plan.json",
                "return_package_markdown": "RETURN_PACKAGE.md"
            }
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "deck", "--from"])
        .arg(&source)
        .args(["--out"])
        .arg(&deck)
        .arg("--json")
        .output()?;

    assert!(
        output.status.success(),
        "deck command failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["schema"], "offdesk_marp_deck.v1");
    assert_eq!(report["source_kind"], "closeout");
    assert_eq!(report["render_status"], "not_requested");
    assert_eq!(
        reported_path(&report["marp_markdown_path"]),
        expected_path(&deck)
    );

    let markdown = fs::read_to_string(&deck)?;
    assert!(markdown.contains("marp: true"));
    assert!(markdown.contains("source JSON remains authoritative"));
    assert!(markdown.contains("closeout_alpha"));
    assert!(markdown.contains("forager offdesk tasks --json"));
    assert!(markdown.contains("review surface only"));
    Ok(())
}

#[test]
fn offdesk_deck_without_render_does_not_require_marp_cli() -> Result<()> {
    let temp = tempdir()?;
    let source = temp.path().join("status.json");
    let deck = temp.path().join("status.marp.md");
    fs::write(
        &source,
        serde_json::to_string_pretty(&json!({
            "status": "healthy",
            "agent_runtime_status": "available",
            "listener_status": "polling",
            "model": "local-coder",
            "pending_approvals": [],
            "queued_offdesk_tasks": [],
            "active_offdesk_tasks": []
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "deck", "--from"])
        .arg(&source)
        .args(["--out"])
        .arg(&deck)
        .args(["--marp-bin", "definitely-missing-marp-bin"])
        .arg("--json")
        .output()?;

    assert!(
        output.status.success(),
        "deck command unexpectedly required Marp CLI\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["source_kind"], "status");
    assert_eq!(report["render_status"], "not_requested");
    assert!(deck.exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_gate_json_filters_adaptive_wiki_projection_by_agent_mode() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_shared",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Shared report rule",
                    "ai_instruction": "Use shared report guidance.",
                    "evidence_refs": ["task:shared"],
                    "confidence": "explicit"
                },
                {
                    "id": "wiki_code",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "agent_modes": ["code_development"],
                    "claim": "Code report rule",
                    "ai_instruction": "Use code-development report guidance.",
                    "evidence_refs": ["task:code"],
                    "confidence": "explicit"
                },
                {
                    "id": "wiki_research",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "agent_modes": ["research_writing"],
                    "claim": "Research report rule",
                    "ai_instruction": "Do not include for code mode.",
                    "evidence_refs": ["task:research"],
                    "confidence": "explicit"
                },
                {
                    "id": "wiki_critique",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "agent_modes": ["critique"],
                    "claim": "Critique report rule",
                    "ai_instruction": "Do not include for code mode.",
                    "evidence_refs": ["task:critique"],
                    "confidence": "explicit"
                }
            ]
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "gate",
            "inspect.status",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--artifact-kind",
            "report",
            "--agent-mode",
            "development",
            "--json",
        ])
        .output()?;
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let wiki = value["adaptive_wiki"].as_array().expect("wiki projection");
    let ids = wiki
        .iter()
        .map(|entry| entry["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"wiki_shared"));
    assert!(ids.contains(&"wiki_code"));
    assert!(!ids.contains(&"wiki_research"));
    assert!(!ids.contains(&"wiki_critique"));
    let code = wiki
        .iter()
        .find(|entry| entry["id"] == "wiki_code")
        .expect("code wiki");
    assert_eq!(code["agent_modes"], json!(["development"]));
    let runtime_ids = value["adaptive_wiki_runtime"]
        .as_array()
        .expect("runtime wiki projection")
        .iter()
        .map(|entry| entry["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(runtime_ids, ids);

    let shared_only_output = forager_command(temp.path())
        .args([
            "offdesk",
            "gate",
            "inspect.status",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--artifact-kind",
            "report",
            "--json",
        ])
        .output()?;
    assert!(shared_only_output.status.success());
    let shared_only: serde_json::Value = serde_json::from_slice(&shared_only_output.stdout)?;
    let shared_only_ids = shared_only["adaptive_wiki"]
        .as_array()
        .expect("shared-only wiki projection")
        .iter()
        .map(|entry| entry["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(shared_only_ids, vec!["wiki_shared"]);

    let review_output = forager_command(temp.path())
        .args([
            "offdesk",
            "gate",
            "inspect.status",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--artifact-kind",
            "report",
            "--agent-mode",
            "review",
            "--json",
        ])
        .output()?;
    assert!(review_output.status.success());
    let review: serde_json::Value = serde_json::from_slice(&review_output.stdout)?;
    let review_ids = review["adaptive_wiki"]
        .as_array()
        .expect("review wiki projection")
        .iter()
        .map(|entry| entry["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(review_ids, vec!["wiki_shared"]);
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_read_only_commands_expose_candidates_entries_projection_and_lint() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_project_entry",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Project entries are visible to operators",
                    "ai_instruction": format!("Confirm project-specific wiki rules before acting token={secret}"),
                    "human_summary": "Human project note",
                    "evidence_refs": ["task:project"],
                    "core_tags": ["project/project"],
                    "proposed_tags": ["method/baseline-first"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now
                },
                {
                    "id": "wiki_needs_review",
                    "kind": "policy_rule",
                    "scope": "project",
                    "scope_ref": "project",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Expired review entry",
                    "ai_instruction": "Review this entry.",
                    "human_summary": "Needs review",
                    "evidence_refs": [],
                    "confidence": "inferred",
                    "created_at": now,
                    "updated_at": now,
                    "review_after": now - Duration::minutes(1)
                },
                {
                    "id": "wiki_other_project",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "other-project",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Other project rule",
                    "ai_instruction": "Do not include this.",
                    "human_summary": "Other project",
                    "evidence_refs": ["task:other"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now
                }
            ]
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_candidates.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "candidates": [
                {
                    "id": "wiki_candidate_denial",
                    "kind": "policy_rule",
                    "scope": "project",
                    "scope_ref": "project",
                    "claim": "Operator denied dispatch for project task",
                    "suggested_ai_instruction": "Ask for confirmation before retrying dispatch.",
                    "human_summary": "Captured denial",
                    "evidence_refs": ["approval:approval_one"],
                    "signal_kind": "approval_denial",
                    "origin": "operator_explicit",
                    "source_refs": [format!("approval:approval_one?token={secret}")],
                    "source_hashes": ["sha256:abc"],
                    "core_tags": ["risk/operator-denial"],
                    "proposed_tags": ["project/project"],
                    "suggested_scope": {
                        "scope": "project",
                        "scope_ref": "project"
                    },
                    "review_reason": format!("Review before promotion token={secret}"),
                    "occurrence_count": 2,
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "last_seen_at": now
                }
            ]
        }))?,
    )?;

    let candidates_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "candidates",
            "--project-key",
            "project",
            "--json",
        ])
        .output()?;
    assert!(candidates_output.status.success());
    let candidates: serde_json::Value = serde_json::from_slice(&candidates_output.stdout)?;
    assert_eq!(candidates.as_array().expect("candidates").len(), 1);
    assert_eq!(candidates[0]["id"], "wiki_candidate_denial");
    assert_eq!(candidates[0]["signal_kind"], "approval_denial");
    assert_eq!(candidates[0]["origin"], "operator_explicit");
    assert_eq!(candidates[0]["suggested_scope"]["scope"], "project");
    assert!(!String::from_utf8_lossy(&candidates_output.stdout).contains(secret));
    assert!(candidates[0]["source_refs"][0]
        .as_str()
        .expect("source ref")
        .contains("[REDACTED]"));
    assert!(candidates[0]["review_reason"]
        .as_str()
        .expect("review reason")
        .contains("[REDACTED]"));

    let candidates_human_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "candidates", "--project-key", "project"])
        .output()?;
    assert!(candidates_human_output.status.success());
    let candidates_stdout = String::from_utf8_lossy(&candidates_human_output.stdout);
    assert!(candidates_stdout.contains("wiki_candidate_denial"));
    assert!(candidates_stdout.contains("project:project"));
    assert!(candidates_stdout.contains("review:"));
    assert!(candidates_stdout.contains("sources:"));
    assert!(!candidates_stdout.contains(secret));

    let entries_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "entries",
            "--project-key",
            "project",
            "--json",
        ])
        .output()?;
    assert!(entries_output.status.success());
    let entries: serde_json::Value = serde_json::from_slice(&entries_output.stdout)?;
    let entry_ids: Vec<_> = entries
        .as_array()
        .expect("entries")
        .iter()
        .map(|entry| entry["id"].as_str().expect("entry id"))
        .collect();
    assert!(entry_ids.contains(&"wiki_project_entry"));
    assert!(entry_ids.contains(&"wiki_needs_review"));
    assert!(!entry_ids.contains(&"wiki_other_project"));

    let projection_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "projection",
            "--project-key",
            "project",
            "--json",
        ])
        .output()?;
    assert!(projection_output.status.success());
    let projection: serde_json::Value = serde_json::from_slice(&projection_output.stdout)?;
    assert_eq!(projection.as_array().expect("projection").len(), 2);
    assert!(projection
        .as_array()
        .expect("projection")
        .iter()
        .any(|entry| entry["instruction"]
            .as_str()
            .expect("instruction")
            .contains("[REDACTED]")));
    assert!(!String::from_utf8_lossy(&projection_output.stdout).contains(secret));

    let projection_report_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "projection",
            "--project-key",
            "project",
            "--report",
            "--max-entries",
            "1",
            "--max-instruction-chars",
            "24",
            "--json",
        ])
        .output()?;
    assert!(projection_report_output.status.success());
    assert!(!String::from_utf8_lossy(&projection_report_output.stdout).contains(secret));
    let projection_report: serde_json::Value =
        serde_json::from_slice(&projection_report_output.stdout)?;
    assert_eq!(projection_report["budget"]["max_entries"], 1);
    assert_eq!(projection_report["budget"]["max_instruction_chars"], 24);
    assert_eq!(projection_report["summary"]["selected"], 1);
    assert_eq!(projection_report["summary"]["rejected"], 1);
    assert_eq!(projection_report["summary"]["instructions_truncated"], 1);
    assert_eq!(projection_report["selected"][0]["id"], "wiki_project_entry");
    assert!(projection_report["selected"][0]["instruction"]
        .as_str()
        .expect("truncated instruction")
        .ends_with("..."));
    assert!(projection_report["rejected"]
        .as_array()
        .expect("projection rejected entries")
        .iter()
        .any(|entry| entry["entry_id"] == "wiki_needs_review"
            && entry["reason"] == "budget_max_entries"));

    let projection_review_report_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "projection",
            "--project-key",
            "project",
            "--report",
            "--max-entries",
            "2",
            "--json",
        ])
        .output()?;
    assert!(projection_review_report_output.status.success());
    assert!(!String::from_utf8_lossy(&projection_review_report_output.stdout).contains(secret));
    let projection_review_report: serde_json::Value =
        serde_json::from_slice(&projection_review_report_output.stdout)?;
    assert_eq!(
        projection_review_report["summary"]["review_expired_projected"],
        1
    );
    assert!(projection_review_report["selected"]
        .as_array()
        .expect("projection selected entries")
        .iter()
        .any(|entry| entry["id"] == "wiki_needs_review"));
    assert!(projection_review_report["review_expired"]
        .as_array()
        .expect("review expired projection warnings")
        .iter()
        .any(|entry| entry["entry_id"] == "wiki_needs_review"
            && entry["detail"]
                .as_str()
                .expect("review expired detail")
                .contains("default warn policy")));

    let strict_projection_report_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "projection",
            "--project-key",
            "project",
            "--report",
            "--max-entries",
            "2",
            "--exclude-review-expired",
            "--json",
        ])
        .output()?;
    assert!(strict_projection_report_output.status.success());
    assert!(!String::from_utf8_lossy(&strict_projection_report_output.stdout).contains(secret));
    let strict_projection_report: serde_json::Value =
        serde_json::from_slice(&strict_projection_report_output.stdout)?;
    assert_eq!(
        strict_projection_report["policy"]["review_expired"],
        "exclude"
    );
    assert_eq!(
        strict_projection_report["summary"]["review_expired_projected"],
        0
    );
    assert!(!strict_projection_report["selected"]
        .as_array()
        .expect("strict selected entries")
        .iter()
        .any(|entry| entry["id"] == "wiki_needs_review"));
    assert!(strict_projection_report["rejected"]
        .as_array()
        .expect("strict rejected entries")
        .iter()
        .any(|entry| entry["entry_id"] == "wiki_needs_review"
            && entry["reason"] == "review_expired_excluded"));

    let comparison_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "projection",
            "--project-key",
            "project",
            "--compare-review-expired-policy",
            "--max-entries",
            "2",
            "--json",
        ])
        .output()?;
    assert!(comparison_output.status.success());
    assert!(!String::from_utf8_lossy(&comparison_output.stdout).contains(secret));
    let comparison: serde_json::Value = serde_json::from_slice(&comparison_output.stdout)?;
    assert_eq!(comparison["warn"]["policy"]["review_expired"], "warn");
    assert_eq!(comparison["strict"]["policy"]["review_expired"], "exclude");
    assert!(comparison["summary"]["selected_only_in_warn"]
        .as_array()
        .expect("selected only in warn")
        .iter()
        .any(|entry| entry == "wiki_needs_review"));
    assert!(comparison["summary"]["review_expired_excluded"]
        .as_array()
        .expect("review expired excluded")
        .iter()
        .any(|entry| entry == "wiki_needs_review"));
    assert!(!comparison["strict"]["selected"]
        .as_array()
        .expect("strict comparison selected")
        .iter()
        .any(|entry| entry["id"] == "wiki_needs_review"));

    let show_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "show", "wiki_candidate_denial", "--json"])
        .output()?;
    assert!(show_output.status.success());
    let show: serde_json::Value = serde_json::from_slice(&show_output.stdout)?;
    assert_eq!(show["kind"], "candidate");
    assert_eq!(show["candidate"]["id"], "wiki_candidate_denial");

    let lint_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "lint", "--json"])
        .output()?;
    assert!(lint_output.status.success());
    let lint: serde_json::Value = serde_json::from_slice(&lint_output.stdout)?;
    assert_eq!(lint["summary"]["entries_checked"], 3);
    assert_eq!(lint["summary"]["candidates_checked"], 1);
    let lint_codes: Vec<_> = lint["issues"]
        .as_array()
        .expect("lint issues")
        .iter()
        .map(|issue| issue["code"].as_str().expect("lint code"))
        .collect();
    assert!(lint_codes.contains(&"promoted_without_evidence"));
    assert!(lint_codes.contains(&"review_expired"));

    let graph_dir = temp.path().join("adaptive-wiki-graph");
    let graph_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "graph",
            "--output",
            graph_dir.to_str().expect("graph dir"),
            "--json",
        ])
        .output()?;
    assert!(graph_output.status.success());
    let graph: serde_json::Value = serde_json::from_slice(&graph_output.stdout)?;
    assert_eq!(graph["summary"]["entries"], 3);
    assert_eq!(graph["summary"]["candidates"], 1);
    assert!(graph["nodes"]
        .as_array()
        .expect("graph nodes")
        .iter()
        .any(|node| node["id"] == "tag:project/project"));
    assert!(graph["edges"]
        .as_array()
        .expect("graph edges")
        .iter()
        .any(|edge| edge["relationship"] == "has_proposed_tag"
            && edge["target"] == "tag:project/project"));
    assert!(graph["review_issues"]
        .as_array()
        .expect("graph review issues")
        .iter()
        .any(|issue| issue["code"] == "proposed_tag_matches_core_prefix"));
    assert!(graph_dir.join("graph.json").is_file());
    assert!(graph_dir.join("graph.md").is_file());
    assert!(!String::from_utf8_lossy(&graph_output.stdout).contains(secret));
    assert!(!fs::read_to_string(graph_dir.join("graph.md"))?.contains(secret));

    let entries_before_episode =
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?;
    let candidates_before_episode =
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?;
    let episode_dry_run_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "evaluate-episode",
            "wiki_project_entry",
            "--project-key",
            "project",
            "--out-project-key",
            "other-project",
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(episode_dry_run_output.status.success());
    let episode_dry_run: serde_json::Value =
        serde_json::from_slice(&episode_dry_run_output.stdout)?;
    assert_eq!(episode_dry_run["dry_run"], true);
    assert_eq!(episode_dry_run["passed"], false);
    assert_eq!(episode_dry_run["summary"]["target_entry_in_scope"], true);
    assert_eq!(
        episode_dry_run["summary"]["target_entry_out_of_scope"],
        false
    );
    assert_eq!(
        episode_dry_run["summary"]["review_expired_entry_projected"],
        true
    );
    assert_eq!(episode_dry_run["summary"]["files_written"], 0);
    assert!(episode_dry_run["review_expired_projected_entry_ids"]
        .as_array()
        .expect("review expired ids")
        .iter()
        .any(|id| id == "wiki_needs_review"));
    assert!(!String::from_utf8_lossy(&episode_dry_run_output.stdout).contains(secret));
    assert!(!profile_dir.join("adaptive_wiki_episode_reports").exists());
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        entries_before_episode
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?,
        candidates_before_episode
    );

    let episode_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "evaluate-episode",
            "wiki_project_entry",
            "--project-key",
            "project",
            "--out-project-key",
            "other-project",
            "--json",
        ])
        .output()?;
    assert!(episode_output.status.success());
    let episode: serde_json::Value = serde_json::from_slice(&episode_output.stdout)?;
    assert_eq!(episode["dry_run"], false);
    assert_eq!(episode["summary"]["files_written"], 2);
    assert_eq!(episode["summary"]["scope_leakage_count"], 0);
    let episode_dir = PathBuf::from(episode["report_dir"].as_str().expect("episode report dir"));
    assert!(episode_dir.join("episode.json").is_file());
    assert!(episode_dir.join("EPISODE.md").is_file());
    let episode_md = fs::read_to_string(episode_dir.join("EPISODE.md"))?;
    assert!(episode_md.contains("Adaptive Wiki Episode Evaluation"));
    assert!(episode_md.contains("wiki_project_entry"));
    assert!(episode_md.contains("review-expired entries were projected"));
    assert!(!episode_md.contains(secret));
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        entries_before_episode
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?,
        candidates_before_episode
    );

    let vault_dir = temp.path().join("adaptive-wiki-vault");
    let dry_run_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "export-markdown",
            "--output",
            vault_dir.to_str().expect("vault dir"),
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(dry_run_output.status.success());
    let dry_run: serde_json::Value = serde_json::from_slice(&dry_run_output.stdout)?;
    assert_eq!(dry_run["dry_run"], true);
    assert_eq!(dry_run["summary"]["entries_exported"], 3);
    assert_eq!(dry_run["summary"]["candidates_exported"], 1);
    assert_eq!(dry_run["summary"]["files_written"], 0);
    assert!(dry_run["files"]
        .as_array()
        .expect("export files")
        .iter()
        .any(|file| file["path"] == "entries/procedure/wiki-project-entry.md"));
    assert!(!vault_dir.exists());
    assert!(!String::from_utf8_lossy(&dry_run_output.stdout).contains(secret));

    let export_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "export-markdown",
            "--output",
            vault_dir.to_str().expect("vault dir"),
            "--json",
        ])
        .output()?;
    assert!(export_output.status.success());
    let export: serde_json::Value = serde_json::from_slice(&export_output.stdout)?;
    assert_eq!(export["dry_run"], false);
    assert_eq!(
        export["summary"]["files_written"],
        export["summary"]["files_planned"]
    );
    let index = fs::read_to_string(vault_dir.join("index.md"))?;
    assert!(index.contains("wiki_project_entry"));
    assert!(index.contains("wiki_candidate_denial"));
    assert!(!index.contains(secret));
    let entry_page = fs::read_to_string(vault_dir.join("entries/procedure/wiki-project-entry.md"))?;
    assert!(entry_page.contains("Human project note"));
    assert!(entry_page.contains("[REDACTED]"));
    assert!(!entry_page.contains(secret));
    assert!(vault_dir.join("raw/audits").is_dir());

    let entries_before_review = fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?;
    let candidates_before_review =
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?;
    let review_dry_run_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "review", "--dry-run", "--json"])
        .output()?;
    assert!(review_dry_run_output.status.success());
    let review_dry_run: serde_json::Value = serde_json::from_slice(&review_dry_run_output.stdout)?;
    assert_eq!(review_dry_run["dry_run"], true);
    assert_eq!(review_dry_run["summary"]["files_written"], 0);
    assert!(
        review_dry_run["summary"]["proposals"]
            .as_u64()
            .expect("proposal count")
            >= 2
    );
    assert!(review_dry_run["proposals"]
        .as_array()
        .expect("review proposals")
        .iter()
        .all(|proposal| proposal["subject_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
            && proposal["evidence_refs"]
                .as_array()
                .is_some_and(|refs| !refs.is_empty())));
    assert!(review_dry_run["proposals"]
        .as_array()
        .expect("review proposals")
        .iter()
        .any(|proposal| proposal["action"] == "promote"
            && proposal["subject_id"] == "wiki_candidate_denial"));
    assert!(!String::from_utf8_lossy(&review_dry_run_output.stdout).contains(secret));
    assert!(!profile_dir.join("adaptive_wiki_review_reports").exists());
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        entries_before_review
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?,
        candidates_before_review
    );

    let review_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "review", "--json"])
        .output()?;
    assert!(review_output.status.success());
    let review: serde_json::Value = serde_json::from_slice(&review_output.stdout)?;
    assert_eq!(review["dry_run"], false);
    assert_eq!(review["summary"]["files_written"], 2);
    let review_dir = PathBuf::from(review["report_dir"].as_str().expect("review report dir"));
    assert!(review_dir.join("report.json").is_file());
    assert!(review_dir.join("REPORT.md").is_file());
    let review_md = fs::read_to_string(review_dir.join("REPORT.md"))?;
    assert!(review_md.contains("Adaptive Wiki Review Report"));
    assert!(review_md.contains("wiki_candidate_denial"));
    assert!(!review_md.contains(secret));
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        entries_before_review
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?,
        candidates_before_review
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_export_markdown_defaults_to_profile_vault_and_reports_stale_state() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_export_default",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "status": "promoted",
                    "activation_mode": "context_only",
                    "claim": "Default vault export is inspectable",
                    "ai_instruction": "Use the default vault for human projection checks.",
                    "human_summary": "Default vault export note",
                    "evidence_refs": ["task:export"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now
                }
            ]
        }))?,
    )?;

    let dry_run_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "export-markdown", "--dry-run", "--json"])
        .output()?;
    assert!(
        dry_run_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&dry_run_output.stdout),
        String::from_utf8_lossy(&dry_run_output.stderr)
    );
    let dry_run: serde_json::Value = serde_json::from_slice(&dry_run_output.stdout)?;
    assert_eq!(
        reported_path(&dry_run["output_dir"]),
        expected_path(&profile_dir.join("wiki-vault"))
    );
    assert_eq!(dry_run["projection_status"]["state"], "missing");
    assert_eq!(dry_run["projection_status"]["reexport_recommended"], true);
    assert!(!profile_dir.join("wiki-vault/index.md").exists());

    let export_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "export-markdown", "--json"])
        .output()?;
    assert!(
        export_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&export_output.stdout),
        String::from_utf8_lossy(&export_output.stderr)
    );
    let export: serde_json::Value = serde_json::from_slice(&export_output.stdout)?;
    assert_eq!(export["projection_status"]["state"], "fresh");
    assert_eq!(export["projection_status"]["reexport_recommended"], false);
    assert!(profile_dir.join("wiki-vault/index.md").exists());

    thread::sleep(StdDuration::from_millis(1100));
    fs::write(
        profile_dir.join("adaptive_wiki_candidates.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "candidates": []
        }))?,
    )?;

    let stale_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "export-markdown", "--dry-run", "--json"])
        .output()?;
    assert!(
        stale_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&stale_output.stdout),
        String::from_utf8_lossy(&stale_output.stderr)
    );
    let stale: serde_json::Value = serde_json::from_slice(&stale_output.stdout)?;
    assert_eq!(stale["projection_status"]["state"], "stale");
    assert_eq!(stale["projection_status"]["reexport_recommended"], true);
    assert_eq!(stale["summary"]["files_written"], 0);
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_episode_trace_links_task_usage_candidate_and_audit_evidence() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_trace_entry",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Trace entry",
                    "ai_instruction": "Use trace entry.",
                    "human_summary": "Trace summary",
                    "evidence_refs": ["task:task_episode"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now
                }
            ]
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_candidates.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "candidates": [
                {
                    "id": "wiki_trace_candidate",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "claim": "Operator corrected trace behavior",
                    "suggested_ai_instruction": "Remember the correction.",
                    "human_summary": "Correction summary",
                    "evidence_refs": ["task:task_episode", "request:request_episode"],
                    "signal_kind": "operator_correction",
                    "origin": "operator_explicit",
                    "source_refs": [format!("approval:approval_episode?token={secret}")],
                    "source_hashes": ["sha256:abc"],
                    "review_reason": "Trace review",
                    "occurrence_count": 2,
                    "confidence": "repeated",
                    "created_at": now,
                    "updated_at": now,
                    "last_seen_at": now
                }
            ]
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_usage.jsonl"),
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "id": "wiki_usage_episode",
                "entry_id": "wiki_trace_entry",
                "task_id": "task_episode",
                "request_id": "request_episode",
                "project_key": "project",
                "artifact_kind": "report",
                "projection_kind": "runtime_probe",
                "activation_mode": "confirm",
                "created_at": now
            }))?
        ),
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_corrections.jsonl"),
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "id": "wiki_correction_episode",
                "correction_kind": "operator_correction",
                "candidate_id": "wiki_trace_candidate",
                "entry_id": "wiki_trace_entry",
                "task_id": "task_episode",
                "request_id": "request_episode",
                "project_key": "project",
                "artifact_kind": "report",
                "summary": "First-class correction summary",
                "evidence_refs": [],
                "source_refs": [],
                "created_at": now
            }))?
        ),
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_audit.jsonl"),
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "id": "wiki_audit_episode",
                "action": "promote",
                "subject_id": "wiki_trace_entry",
                "candidate_id": "wiki_trace_candidate",
                "entry_id": "wiki_trace_entry",
                "actor": "cli",
                "reason": "Trace promotion",
                "evidence_ref": "request:request_episode",
                "created_at": now
            }))?
        ),
    )?;
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task_episode",
                "request_id": "request_episode",
                "project_key": "project",
                "status": "completed",
                "capability_id": "capability.trace",
                "runner_kind": "local_background",
                "command": format!("echo token={secret}"),
                "workdir": "/tmp",
                "background_ticket_id": "ticket_episode",
                "attempt_count": 1,
                "created_at": now,
                "updated_at": now,
                "artifact_kind": "report",
                "last_adaptive_wiki_entry_ids": ["wiki_trace_entry"],
                "preview": "Trace preview",
                "reason": "Trace reason"
            }
        ]))?,
    )?;
    fs::write(
        profile_dir.join("background_runs.json"),
        serde_json::to_string_pretty(&json!([
            {
                "ticket_id": "ticket_episode",
                "capability_id": "capability.trace",
                "project_key": "project",
                "request_id": "request_episode",
                "task_id": "task_episode",
                "runner_kind": "local_background",
                "phase": "completed",
                "runtime_handle_alive": false,
                "last_observed_at": now,
                "last_recovery_evidence": "result artifact present",
                "adaptive_wiki_entry_ids": ["wiki_trace_entry"],
                "adaptive_wiki_context": "context token=sk-secretsecretsecretsecret"
            }
        ]))?,
    )?;
    fs::write(
        profile_dir.join("task_resume_state.json"),
        serde_json::to_string_pretty(&json!([
            {
                "resume_id": "resume_episode",
                "task_id": "task_episode",
                "request_id": "request_episode",
                "project_key": "project",
                "status": "resume_pending",
                "phase": "background_probe",
                "runner_target": "ticket_episode",
                "last_evidence_artifacts": ["log:episode"],
                "next_safe_resume_step": "inspect result",
                "interrupted_at": now,
                "interruption_reason": "test resume"
            }
        ]))?,
    )?;

    let entries_before = fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?;
    let candidates_before = fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?;
    let dry_run_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "episode-trace",
            "--request-id",
            "request_episode",
            "--project-key",
            "project",
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(dry_run_output.status.success());
    let dry_run: serde_json::Value = serde_json::from_slice(&dry_run_output.stdout)?;
    assert_eq!(dry_run["dry_run"], true);
    assert_eq!(dry_run["summary"]["files_written"], 0);
    assert!(dry_run["summary"]["events"].as_u64().expect("event count") >= 8);
    assert_eq!(dry_run["summary"]["runtime_usage_events"], 1);
    assert_eq!(dry_run["summary"]["candidate_events"], 1);
    assert_eq!(dry_run["summary"]["correction_events"], 1);
    assert_eq!(dry_run["summary"]["promotion_events"], 1);
    assert!(dry_run["events"]
        .as_array()
        .expect("trace events")
        .iter()
        .any(|event| event["kind"] == "runtime_usage_recorded"
            && event["entry_ids"][0] == "wiki_trace_entry"));
    assert!(dry_run["events"]
        .as_array()
        .expect("trace events")
        .iter()
        .any(|event| event["kind"] == "operator_correction_observed"
            && event["candidate_id"] == "wiki_trace_candidate"
            && event["summary"]
                .as_str()
                .expect("correction summary")
                .contains("correction kind=operator_correction")));
    assert!(!String::from_utf8_lossy(&dry_run_output.stdout).contains(secret));
    assert!(!profile_dir.join("adaptive_wiki_episode_traces").exists());
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        entries_before
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?,
        candidates_before
    );

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "episode-trace",
            "--request-id",
            "request_episode",
            "--project-key",
            "project",
            "--json",
        ])
        .output()?;
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["dry_run"], false);
    assert_eq!(report["summary"]["files_written"], 3);
    let report_dir = PathBuf::from(report["report_dir"].as_str().expect("trace report dir"));
    assert!(report_dir.join("report.json").is_file());
    assert!(report_dir.join("trace.jsonl").is_file());
    assert!(report_dir.join("REPORT.md").is_file());
    let trace_jsonl = fs::read_to_string(report_dir.join("trace.jsonl"))?;
    assert!(trace_jsonl.contains("runtime_usage_recorded"));
    assert!(trace_jsonl.contains("entry_promoted"));
    assert!(!trace_jsonl.contains(secret));
    let report_md = fs::read_to_string(report_dir.join("REPORT.md"))?;
    assert!(report_md.contains("Adaptive Wiki Live Episode Trace"));
    assert!(report_md.contains("wiki_trace_entry"));
    assert!(!report_md.contains(secret));
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        entries_before
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?,
        candidates_before
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_evaluate_recurrence_counts_pre_and_post_promotion_corrections() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let promotion_at = Utc::now();
    let before = promotion_at - Duration::hours(2);
    let after = promotion_at + Duration::hours(2);
    let secret = "sk-secretsecretsecretsecret";
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_recur_entry",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Avoid repeated correction",
                    "ai_instruction": "Use the promoted correction.",
                    "human_summary": "Recurrence target",
                    "evidence_refs": ["task:task_before"],
                    "confidence": "repeated",
                    "created_at": promotion_at,
                    "updated_at": promotion_at
                }
            ]
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_candidates.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "candidates": [
                {
                    "id": "wiki_recur_before",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "claim": "Before promotion correction",
                    "suggested_ai_instruction": "Before correction.",
                    "human_summary": "Before correction",
                    "evidence_refs": ["task:task_before"],
                    "signal_kind": "operator_correction",
                    "origin": "operator_explicit",
                    "source_refs": ["request:request_before"],
                    "source_hashes": ["sha256:before"],
                    "review_reason": "Before recurrence baseline",
                    "occurrence_count": 1,
                    "confidence": "explicit",
                    "created_at": before,
                    "updated_at": before,
                    "last_seen_at": before
                },
                {
                    "id": "wiki_recur_after",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "claim": "After promotion correction",
                    "suggested_ai_instruction": "After correction.",
                    "human_summary": "After correction",
                    "evidence_refs": ["task:task_after"],
                    "signal_kind": "operator_correction",
                    "origin": "operator_explicit",
                    "source_refs": [format!("request:request_after?token={secret}")],
                    "source_hashes": ["sha256:after"],
                    "review_reason": "After recurrence observed",
                    "occurrence_count": 1,
                    "confidence": "explicit",
                    "created_at": after,
                    "updated_at": after,
                    "last_seen_at": after
                }
            ]
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_usage.jsonl"),
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "id": "wiki_usage_after",
                "entry_id": "wiki_recur_entry",
                "task_id": "task_after",
                "request_id": "request_after",
                "project_key": "project",
                "artifact_kind": "report",
                "projection_kind": "runtime_probe",
                "activation_mode": "confirm",
                "created_at": after
            }))?
        ),
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_corrections.jsonl"),
        format!(
            "{}\n{}\n",
            serde_json::to_string(&json!({
                "id": "wiki_corr_before",
                "correction_kind": "operator_correction",
                "candidate_id": "wiki_recur_before",
                "entry_id": "wiki_recur_entry",
                "task_id": "task_before",
                "request_id": "request_before",
                "project_key": "project",
                "artifact_kind": "report",
                "summary": "Before correction record",
                "evidence_refs": ["task:task_before"],
                "source_refs": ["request:request_before"],
                "created_at": before
            }))?,
            serde_json::to_string(&json!({
                "id": "wiki_corr_after",
                "correction_kind": "operator_correction",
                "candidate_id": "wiki_recur_after",
                "entry_id": "wiki_recur_entry",
                "task_id": "task_after",
                "request_id": "request_after",
                "project_key": "project",
                "artifact_kind": "report",
                "summary": format!("After correction record token={secret}"),
                "evidence_refs": ["task:task_after"],
                "source_refs": [format!("request:request_after?token={secret}")],
                "created_at": after
            }))?
        ),
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_audit.jsonl"),
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "id": "wiki_audit_promote_recur",
                "action": "promote",
                "subject_id": "wiki_recur_entry",
                "candidate_id": "wiki_recur_before",
                "entry_id": "wiki_recur_entry",
                "actor": "cli",
                "reason": "Promotion boundary",
                "evidence_ref": "task:task_before",
                "created_at": promotion_at
            }))?
        ),
    )?;
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task_after",
                "request_id": "request_after",
                "project_key": "project",
                "status": "completed",
                "capability_id": "capability.recur",
                "runner_kind": "local_background",
                "command": format!("echo token={secret}"),
                "workdir": "/tmp",
                "attempt_count": 1,
                "created_at": after,
                "updated_at": after,
                "artifact_kind": "report",
                "last_adaptive_wiki_entry_ids": ["wiki_recur_entry"],
                "preview": "Recurrence preview",
                "reason": "Recurrence reason"
            }
        ]))?,
    )?;

    let entries_before = fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?;
    let candidates_before = fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?;
    let dry_run_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "evaluate-recurrence",
            "wiki_recur_entry",
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(dry_run_output.status.success());
    let dry_run: serde_json::Value = serde_json::from_slice(&dry_run_output.stdout)?;
    assert_eq!(dry_run["dry_run"], true);
    assert_eq!(dry_run["assessment"], "recurrence_observed");
    assert_eq!(dry_run["summary"]["correction_records_checked"], 2);
    assert_eq!(dry_run["summary"]["pre_promotion_correction_events"], 1);
    assert_eq!(dry_run["summary"]["post_promotion_correction_events"], 1);
    assert_eq!(dry_run["summary"]["post_promotion_usage_events"], 1);
    assert_eq!(
        dry_run["summary"]["post_promotion_recurrence_per_1000"],
        1000
    );
    assert_eq!(dry_run["summary"]["files_written"], 0);
    assert!(!String::from_utf8_lossy(&dry_run_output.stdout).contains(secret));
    assert!(!profile_dir
        .join("adaptive_wiki_recurrence_reports")
        .exists());
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        entries_before
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?,
        candidates_before
    );

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "evaluate-recurrence",
            "wiki_recur_entry",
            "--json",
        ])
        .output()?;
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["dry_run"], false);
    assert_eq!(report["summary"]["files_written"], 3);
    let report_dir = PathBuf::from(
        report["report_dir"]
            .as_str()
            .expect("recurrence report dir"),
    );
    assert!(report_dir.join("report.json").is_file());
    assert!(report_dir.join("recurrence.jsonl").is_file());
    assert!(report_dir.join("REPORT.md").is_file());
    let recurrence_jsonl = fs::read_to_string(report_dir.join("recurrence.jsonl"))?;
    assert!(recurrence_jsonl.contains("operator_correction_observed"));
    assert!(!recurrence_jsonl.contains(secret));
    let report_md = fs::read_to_string(report_dir.join("REPORT.md"))?;
    assert!(report_md.contains("Adaptive Wiki Correction Recurrence"));
    assert!(report_md.contains("wiki_recur_entry"));
    assert!(!report_md.contains(secret));
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        entries_before
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?,
        candidates_before
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_corrections_json_and_debug_bundle_redact_records() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    fs::write(
        profile_dir.join("adaptive_wiki_corrections.jsonl"),
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "id": "wiki_corr_cli",
                "correction_kind": "operator_correction",
                "candidate_id": "wiki_candidate_cli",
                "entry_id": "wiki_entry_cli",
                "task_id": "task_cli",
                "request_id": "request_cli",
                "project_key": "project",
                "artifact_kind": "report",
                "summary": format!("CLI correction token={secret}"),
                "evidence_refs": [format!("task:task_cli?token={secret}")],
                "source_refs": [format!("request:request_cli?token={secret}")],
                "created_at": now
            }))?
        ),
    )?;

    let corrections_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "corrections", "--json"])
        .output()?;
    assert!(corrections_output.status.success());
    let corrections_stdout = String::from_utf8_lossy(&corrections_output.stdout);
    assert!(!corrections_stdout.contains(secret));
    let corrections: serde_json::Value = serde_json::from_slice(&corrections_output.stdout)?;
    assert_eq!(corrections.as_array().expect("correction array").len(), 1);
    assert_eq!(corrections[0]["id"], "wiki_corr_cli");
    assert_eq!(corrections[0]["correction_kind"], "operator_correction");
    assert!(corrections[0]["summary"]
        .as_str()
        .expect("summary")
        .contains("[REDACTED]"));

    let bundle_output = forager_command(temp.path())
        .args(["offdesk", "debug-bundle", "--json"])
        .output()?;
    assert!(bundle_output.status.success());
    let bundle_stdout = String::from_utf8_lossy(&bundle_output.stdout);
    assert!(!bundle_stdout.contains(secret));
    let bundle: serde_json::Value = serde_json::from_slice(&bundle_output.stdout)?;
    assert_eq!(
        bundle["adaptive_wiki_corrections"]
            .as_array()
            .expect("bundle corrections")
            .len(),
        1
    );
    assert_eq!(
        bundle["adaptive_wiki_corrections"][0]["id"],
        "wiki_corr_cli"
    );

    let stored = fs::read_to_string(profile_dir.join("adaptive_wiki_corrections.jsonl"))?;
    assert!(stored.contains(secret));
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_proposal_events_record_list_and_debug_bundle_are_redacted() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    let proposal_id = "wiki_review_promote_candidate_wiki-candidate";
    fs::write(
        profile_dir.join("adaptive_wiki_candidates.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "candidates": [{
                "id": "wiki_candidate",
                "kind": "failure_pattern",
                "scope": "project",
                "scope_ref": "project",
                "claim": "Repeated operator correction needs a durable wiki entry",
                "suggested_ai_instruction": "Check the project wiki before applying repeated corrections.",
                "human_summary": "Repeated correction candidate.",
                "evidence_refs": ["task:wiki_candidate"],
                "signal_kind": "operator_correction",
                "origin": "runtime_observed",
                "occurrence_count": 2,
                "confidence": "repeated",
                "created_at": now,
                "updated_at": now,
                "last_seen_at": now
            }]
        }))?,
    )?;

    let record_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "record-proposal-event",
            proposal_id,
            "--decision",
            "accepted",
            "--proposal-action",
            "promote",
            "--subject-kind",
            "candidate",
            "--subject-id",
            "wiki_candidate",
            "--reason",
            &format!("operator accepted token={secret}"),
            "--evidence-ref",
            &format!("review:report?token={secret}"),
            "--supersedes",
            "old_proposal",
            "--json",
        ])
        .output()?;
    assert!(record_output.status.success());
    assert!(!String::from_utf8_lossy(&record_output.stdout).contains(secret));
    let record: serde_json::Value = serde_json::from_slice(&record_output.stdout)?;
    assert_eq!(record["proposal_id"], proposal_id);
    assert_eq!(record["decision"], "accepted");
    assert_eq!(record["proposal_action"], "promote");
    assert_eq!(record["subject_kind"], "candidate");
    assert_eq!(record["subject_id"], "wiki_candidate");
    assert!(record["reason"]
        .as_str()
        .expect("event reason")
        .contains("[REDACTED]"));

    let list_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-events",
            "--proposal-id",
            proposal_id,
            "--json",
        ])
        .output()?;
    assert!(list_output.status.success());
    assert!(!String::from_utf8_lossy(&list_output.stdout).contains(secret));
    let events: serde_json::Value = serde_json::from_slice(&list_output.stdout)?;
    assert_eq!(events.as_array().expect("proposal events").len(), 1);
    assert_eq!(events[0]["proposal_id"], proposal_id);

    let review_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "review", "--dry-run", "--json"])
        .output()?;
    assert!(review_output.status.success());
    let review: serde_json::Value = serde_json::from_slice(&review_output.stdout)?;
    assert_eq!(review["summary"]["review_events_checked"], 1);
    assert_eq!(review["summary"]["proposals_with_events"], 1);
    assert_eq!(review["summary"]["open_proposals"], 0);
    assert_eq!(review["summary"]["accepted_proposals"], 1);
    assert_eq!(review["summary"]["filtered_out_proposals"], 0);
    assert_eq!(review["summary"]["files_written"], 0);
    let proposals = review["proposals"].as_array().expect("review proposals");
    let proposal = proposals
        .iter()
        .find(|proposal| proposal["id"] == proposal_id)
        .expect("proposal with lifecycle");
    assert_eq!(proposal["lifecycle"]["decision"], "accepted");
    assert_eq!(proposal["lifecycle"]["actor"], "cli");
    assert!(proposal["lifecycle"]["reason"]
        .as_str()
        .expect("lifecycle reason")
        .contains("[REDACTED]"));

    let active_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "review",
            "--dry-run",
            "--active-only",
            "--json",
        ])
        .output()?;
    assert!(active_output.status.success());
    let active: serde_json::Value = serde_json::from_slice(&active_output.stdout)?;
    assert_eq!(
        active["proposals"]
            .as_array()
            .expect("active proposals")
            .len(),
        0
    );
    assert_eq!(active["summary"]["filtered_out_proposals"], 1);

    let decided_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "review",
            "--dry-run",
            "--decided-only",
            "--json",
        ])
        .output()?;
    assert!(decided_output.status.success());
    let decided: serde_json::Value = serde_json::from_slice(&decided_output.stdout)?;
    assert_eq!(
        decided["proposals"]
            .as_array()
            .expect("decided proposals")
            .len(),
        1
    );
    assert_eq!(decided["summary"]["accepted_proposals"], 1);
    assert_eq!(decided["summary"]["filtered_out_proposals"], 0);

    let stale_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "review",
            "--dry-run",
            "--stale-only",
            "--json",
        ])
        .output()?;
    assert!(stale_output.status.success());
    let stale: serde_json::Value = serde_json::from_slice(&stale_output.stdout)?;
    assert_eq!(
        stale["proposals"]
            .as_array()
            .expect("stale proposals")
            .len(),
        0
    );
    assert_eq!(stale["summary"]["filtered_out_proposals"], 1);

    let invalid_filter_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "review",
            "--dry-run",
            "--active-only",
            "--decided-only",
        ])
        .output()?;
    assert!(!invalid_filter_output.status.success());
    assert!(String::from_utf8_lossy(&invalid_filter_output.stderr)
        .contains("choose only one of --active-only, --decided-only, or --stale-only"));

    let bundle_output = forager_command(temp.path())
        .args(["offdesk", "debug-bundle", "--json"])
        .output()?;
    assert!(bundle_output.status.success());
    assert!(!String::from_utf8_lossy(&bundle_output.stdout).contains(secret));
    let bundle: serde_json::Value = serde_json::from_slice(&bundle_output.stdout)?;
    assert_eq!(
        bundle["adaptive_wiki_review_events"]
            .as_array()
            .expect("bundle review events")
            .len(),
        1
    );

    let stored = fs::read_to_string(profile_dir.join("adaptive_wiki_review_events.jsonl"))?;
    assert!(!stored.contains(secret));
    assert!(stored.contains("[REDACTED]"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_proposal_closure_helpers_copy_metadata_and_block_duplicates() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    let proposal_id = "wiki_review_promote_candidate_wiki-candidate";
    fs::write(
        profile_dir.join("adaptive_wiki_candidates.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "candidates": [{
                "id": "wiki_candidate",
                "kind": "failure_pattern",
                "scope": "project",
                "scope_ref": "project",
                "claim": "Repeated operator correction needs a durable wiki entry",
                "suggested_ai_instruction": "Check the project wiki before applying repeated corrections.",
                "human_summary": "Repeated correction candidate.",
                "evidence_refs": [format!("task:wiki_candidate?token={secret}")],
                "signal_kind": "operator_correction",
                "origin": "runtime_observed",
                "occurrence_count": 2,
                "confidence": "repeated",
                "created_at": now,
                "updated_at": now,
                "last_seen_at": now
            }]
        }))?,
    )?;

    let accept_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "accept-proposal",
            proposal_id,
            "--reason",
            &format!("accepted proposal token={secret}"),
            "--evidence-ref",
            &format!("review:manual?token={secret}"),
            "--json",
        ])
        .output()?;
    assert!(accept_output.status.success());
    let accept_stdout = String::from_utf8_lossy(&accept_output.stdout);
    assert!(!accept_stdout.contains(secret));
    let accepted: serde_json::Value = serde_json::from_slice(&accept_output.stdout)?;
    assert_eq!(accepted["proposal_id"], proposal_id);
    assert_eq!(accepted["decision"], "accepted");
    assert_eq!(accepted["proposal_action"], "promote");
    assert_eq!(accepted["subject_kind"], "candidate");
    assert_eq!(accepted["subject_id"], "wiki_candidate");
    let evidence_refs = accepted["evidence_refs"]
        .as_array()
        .expect("accepted evidence refs");
    assert!(evidence_refs
        .iter()
        .any(|value| value.as_str().expect("evidence ref") == "lint:promotion_candidate"));
    assert!(evidence_refs
        .iter()
        .any(|value| value.as_str().expect("evidence ref").contains("[REDACTED]")));

    let duplicate_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "reject-proposal",
            proposal_id,
            "--reason",
            "second decision should require explicit override",
        ])
        .output()?;
    assert!(!duplicate_output.status.success());
    assert!(String::from_utf8_lossy(&duplicate_output.stderr)
        .contains("already has a non-stale lifecycle decision"));

    let supersede_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "supersede-proposal",
            proposal_id,
            "--reason",
            "superseded by operator follow-up",
            "--supersedes",
            "wiki_review_old",
            "--allow-decided",
            "--json",
        ])
        .output()?;
    assert!(supersede_output.status.success());
    let superseded: serde_json::Value = serde_json::from_slice(&supersede_output.stdout)?;
    assert_eq!(superseded["decision"], "superseded");
    assert_eq!(superseded["proposal_action"], "promote");
    assert_eq!(superseded["supersedes"], "wiki_review_old");

    let stored = fs::read_to_string(profile_dir.join("adaptive_wiki_review_events.jsonl"))?;
    assert!(!stored.contains(secret));
    assert!(stored.contains("[REDACTED]"));
    assert!(stored.contains("\"accepted\""));
    assert!(stored.contains("\"superseded\""));
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_proposal_handoff_previews_ready_manual_and_blocked() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    let promote_proposal_id = "wiki_review_promote_candidate_wiki-candidate";
    let renew_proposal_id = "wiki_review_renew_review_entry_wiki-entry";
    let conflict_proposal_id = "wiki_review_split_entry_allow-tables";
    fs::write(
        profile_dir.join("adaptive_wiki_candidates.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "candidates": [{
                "id": "wiki_candidate",
                "kind": "failure_pattern",
                "scope": "project",
                "scope_ref": "project",
                "claim": "Repeated operator correction needs a durable wiki entry",
                "suggested_ai_instruction": "Check the project wiki before applying repeated corrections.",
                "human_summary": "Repeated correction candidate.",
                "evidence_refs": [format!("task:wiki_candidate?token={secret}")],
                "signal_kind": "operator_correction",
                "origin": "runtime_observed",
                "occurrence_count": 2,
                "confidence": "repeated",
                "created_at": now,
                "updated_at": now,
                "last_seen_at": now
            }]
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [{
                "id": "wiki_entry",
                "kind": "procedure",
                "scope": "project",
                "scope_ref": "project",
                "status": "promoted",
                "activation_mode": "confirm",
                "claim": "Review expired entries before projection.",
                "ai_instruction": "Check review windows before relying on this entry.",
                "human_summary": "Expired review entry.",
                "evidence_refs": ["audit:wiki_entry"],
                "confidence": "explicit",
                "created_at": now,
                "updated_at": now,
                "review_after": now - Duration::days(1)
            }, {
                "id": "allow_tables",
                "kind": "procedure",
                "scope": "project",
                "scope_ref": "project",
                "status": "promoted",
                "activation_mode": "confirm",
                "claim": "Use markdown tables for report evidence",
                "ai_instruction": "Use markdown tables for report evidence",
                "human_summary": "Tables are preferred for report evidence.",
                "evidence_refs": ["audit:allow_tables"],
                "confidence": "explicit",
                "created_at": now,
                "updated_at": now
            }, {
                "id": "block_tables",
                "kind": "procedure",
                "scope": "project",
                "scope_ref": "project",
                "status": "promoted",
                "activation_mode": "confirm",
                "claim": "Do not use markdown tables for report evidence",
                "ai_instruction": "Do not use markdown tables for report evidence",
                "human_summary": "Tables are blocked for report evidence.",
                "evidence_refs": ["audit:block_tables"],
                "confidence": "explicit",
                "created_at": now,
                "updated_at": now
            }]
        }))?,
    )?;

    let ready_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-handoff",
            promote_proposal_id,
            "--json",
        ])
        .output()?;
    assert!(ready_output.status.success());
    let ready_stdout = String::from_utf8_lossy(&ready_output.stdout);
    assert!(!ready_stdout.contains(secret));
    let ready: serde_json::Value = serde_json::from_slice(&ready_output.stdout)?;
    assert_eq!(ready["status"], "ready");
    assert_eq!(ready["action"], "promote");
    assert!(ready["command"]
        .as_str()
        .expect("ready command")
        .contains("forager offdesk wiki promote wiki_candidate"));
    assert!(ready["evidence_refs"]
        .as_array()
        .expect("ready evidence refs")
        .iter()
        .any(|value| value.as_str().expect("evidence ref").contains("[REDACTED]")));
    assert!(ready["required_inputs"]
        .as_array()
        .expect("ready required inputs")
        .is_empty());
    assert!(ready["mutation_options"]
        .as_array()
        .expect("ready mutation options")
        .is_empty());
    assert!(!profile_dir
        .join("adaptive_wiki_review_events.jsonl")
        .exists());

    let manual_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-handoff",
            renew_proposal_id,
            "--json",
        ])
        .output()?;
    assert!(manual_output.status.success());
    let manual: serde_json::Value = serde_json::from_slice(&manual_output.stdout)?;
    assert_eq!(manual["status"], "manual_required");
    assert!(manual["command"].is_null());
    assert!(manual["reason"]
        .as_str()
        .expect("manual reason")
        .contains("Renew-review proposals require"));
    let manual_inputs = manual["required_inputs"]
        .as_array()
        .expect("manual required inputs");
    assert!(manual_inputs.iter().any(|input| {
        input["name"] == "mutation" && input["required"].as_bool().expect("required bool")
    }));
    assert!(manual_inputs
        .iter()
        .any(|input| { input["name"] == "scope" && input["cli_flag"] == "--scope" }));
    assert!(manual_inputs
        .iter()
        .any(|input| { input["name"] == "evidence_ref" && input["cli_flag"] == "--evidence-ref" }));
    let manual_options = manual["mutation_options"]
        .as_array()
        .expect("manual mutation options");
    assert!(manual_options.iter().any(|option| {
        option["mutation"] == "rescope"
            && option["command_template"]
                .as_str()
                .expect("rescope template")
                .contains("forager offdesk wiki rescope")
    }));
    assert!(manual_options
        .iter()
        .any(|option| option["mutation"] == "deprecate"));
    assert!(manual_options
        .iter()
        .any(|option| option["mutation"] == "add_counterexample"));

    let conflict_manual_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-handoff",
            conflict_proposal_id,
            "--json",
        ])
        .output()?;
    assert!(conflict_manual_output.status.success());
    let conflict_manual: serde_json::Value =
        serde_json::from_slice(&conflict_manual_output.stdout)?;
    assert_eq!(conflict_manual["status"], "manual_required");
    assert!(conflict_manual["reason"]
        .as_str()
        .expect("conflict manual reason")
        .contains("Projection-conflict proposals require"));
    assert!(conflict_manual["evidence_refs"]
        .as_array()
        .expect("conflict evidence refs")
        .iter()
        .any(|value| value == "entry:block_tables"));
    assert!(conflict_manual["evidence_refs"]
        .as_array()
        .expect("conflict evidence refs")
        .iter()
        .any(|value| value == "projection:markdown tables for report evidence"));
    let conflict_options = conflict_manual["mutation_options"]
        .as_array()
        .expect("conflict mutation options");
    for mutation in ["rescope", "deprecate", "split", "add_counterexample"] {
        assert!(conflict_options
            .iter()
            .any(|option| option["mutation"] == mutation));
    }
    let conflict_inputs = conflict_manual["required_inputs"]
        .as_array()
        .expect("conflict required inputs");
    assert!(conflict_inputs.iter().any(|input| {
        input["name"] == "deprecated_entry_id" && input["cli_flag"] == "--deprecated-entry-id"
    }));

    let conflict_deprecate_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-handoff",
            conflict_proposal_id,
            "--mutation",
            "deprecate",
            "--deprecated-entry-id",
            "block_tables",
            "--reason",
            "resolve conflicting table policy",
            "--json",
        ])
        .output()?;
    assert!(conflict_deprecate_output.status.success());
    let conflict_deprecate: serde_json::Value =
        serde_json::from_slice(&conflict_deprecate_output.stdout)?;
    assert_eq!(conflict_deprecate["status"], "ready");
    assert!(conflict_deprecate["command"]
        .as_str()
        .expect("conflict deprecate command")
        .contains("forager offdesk wiki deprecate 'block_tables'"));

    let conflict_split_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-handoff",
            conflict_proposal_id,
            "--mutation",
            "split",
            "--json",
        ])
        .output()?;
    assert!(conflict_split_output.status.success());
    let conflict_split: serde_json::Value = serde_json::from_slice(&conflict_split_output.stdout)?;
    assert_eq!(conflict_split["status"], "manual_required");
    assert!(conflict_split["reason"]
        .as_str()
        .expect("conflict split reason")
        .contains("governed mutations"));

    let rescope_ready_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-handoff",
            renew_proposal_id,
            "--mutation",
            "rescope",
            "--scope",
            "artifact_kind",
            "--scope-ref",
            "runbook",
            "--reason",
            "narrow expired review entry",
            "--json",
        ])
        .output()?;
    assert!(rescope_ready_output.status.success());
    let rescope_ready: serde_json::Value = serde_json::from_slice(&rescope_ready_output.stdout)?;
    assert_eq!(rescope_ready["status"], "ready");
    assert_eq!(
        rescope_ready["required_inputs"].as_array().unwrap().len(),
        0
    );
    let rescope_command = rescope_ready["command"]
        .as_str()
        .expect("parameterized rescope command");
    assert!(rescope_command.contains("forager offdesk wiki rescope"));
    assert!(rescope_command.contains("--scope artifact_kind"));
    assert!(rescope_command.contains("--scope-ref 'runbook'"));
    assert!(rescope_command.contains("--reason 'narrow expired review entry'"));
    assert!(!profile_dir
        .join("adaptive_wiki_review_events.jsonl")
        .exists());

    let counterexample_ready_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-handoff",
            renew_proposal_id,
            "--mutation",
            "add-counterexample",
            "--evidence-ref",
            &format!("task:review?token={secret}"),
            "--reason",
            "attach limiting review evidence",
            "--json",
        ])
        .output()?;
    assert!(counterexample_ready_output.status.success());
    let counterexample_stdout = String::from_utf8_lossy(&counterexample_ready_output.stdout);
    assert!(!counterexample_stdout.contains(secret));
    let counterexample_ready: serde_json::Value =
        serde_json::from_slice(&counterexample_ready_output.stdout)?;
    assert_eq!(counterexample_ready["status"], "ready");
    assert!(counterexample_ready["command"]
        .as_str()
        .expect("counterexample command")
        .contains("forager offdesk wiki add-counterexample"));
    assert!(counterexample_ready["command"]
        .as_str()
        .expect("counterexample command")
        .contains("[REDACTED]"));

    let missing_rescope_input_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-handoff",
            renew_proposal_id,
            "--mutation",
            "rescope",
            "--scope",
            "project",
            "--json",
        ])
        .output()?;
    assert!(missing_rescope_input_output.status.success());
    let missing_rescope_input: serde_json::Value =
        serde_json::from_slice(&missing_rescope_input_output.stdout)?;
    assert_eq!(missing_rescope_input["status"], "manual_required");
    assert!(missing_rescope_input["reason"]
        .as_str()
        .expect("missing rescope reason")
        .contains("--scope-ref"));

    let accept_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "accept-proposal",
            promote_proposal_id,
            "--reason",
            "accepted before handoff preview",
        ])
        .output()?;
    assert!(accept_output.status.success());
    let blocked_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-handoff",
            promote_proposal_id,
            "--json",
        ])
        .output()?;
    assert!(blocked_output.status.success());
    let blocked: serde_json::Value = serde_json::from_slice(&blocked_output.stdout)?;
    assert_eq!(blocked["status"], "blocked_by_decision");
    assert!(blocked["command"].is_null());
    assert!(blocked["required_inputs"]
        .as_array()
        .expect("blocked required inputs")
        .is_empty());
    assert_eq!(blocked["lifecycle_decision"], "accepted");
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_proposal_receipt_links_preview_audit_and_event_without_mutation() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    let proposal_id = "wiki_review_promote_candidate_wiki-candidate";
    fs::write(
        profile_dir.join("adaptive_wiki_candidates.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "candidates": [{
                "id": "wiki_candidate",
                "kind": "failure_pattern",
                "scope": "project",
                "scope_ref": "project",
                "claim": "Repeated operator correction needs a durable wiki entry",
                "suggested_ai_instruction": "Check the project wiki before applying repeated corrections.",
                "human_summary": "Repeated correction candidate.",
                "evidence_refs": [format!("task:wiki_candidate?token={secret}")],
                "signal_kind": "operator_correction",
                "origin": "runtime_observed",
                "occurrence_count": 2,
                "confidence": "repeated",
                "created_at": now,
                "updated_at": now,
                "last_seen_at": now
            }]
        }))?,
    )?;

    let handoff_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "proposal-handoff", proposal_id, "--json"])
        .output()?;
    assert!(handoff_output.status.success());
    let handoff: serde_json::Value = serde_json::from_slice(&handoff_output.stdout)?;
    assert_eq!(handoff["status"], "ready");
    let preview_command = handoff["command"]
        .as_str()
        .expect("handoff preview command")
        .to_string();

    let accept_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "accept-proposal",
            proposal_id,
            "--reason",
            &format!("accepted proposal token={secret}"),
            "--json",
        ])
        .output()?;
    assert!(accept_output.status.success());
    let event: serde_json::Value = serde_json::from_slice(&accept_output.stdout)?;
    let event_id = event["id"].as_str().expect("event id");

    let promote_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "promote",
            "wiki_candidate",
            "--reason",
            &format!("executed preview token={secret}"),
            "--json",
        ])
        .output()?;
    assert!(promote_output.status.success());
    let mutation: serde_json::Value = serde_json::from_slice(&promote_output.stdout)?;
    let audit_id = mutation["audit"]["id"].as_str().expect("audit id");

    let audit_path = profile_dir.join("adaptive_wiki_audit.jsonl");
    let events_path = profile_dir.join("adaptive_wiki_review_events.jsonl");
    let audit_before = fs::read_to_string(&audit_path)?;
    let events_before = fs::read_to_string(&events_path)?;
    let executed_command = format!("{preview_command} --reason token={secret}");
    let receipt_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-receipt",
            proposal_id,
            "--audit-id",
            audit_id,
            "--event-id",
            event_id,
            "--command",
            &executed_command,
            "--json",
        ])
        .output()?;
    assert!(receipt_output.status.success());
    let receipt_stdout = String::from_utf8_lossy(&receipt_output.stdout);
    assert!(!receipt_stdout.contains(secret));
    let receipt: serde_json::Value = serde_json::from_slice(&receipt_output.stdout)?;
    assert_eq!(receipt["status"], "linked");
    assert_eq!(receipt["read_only"], true);
    assert_eq!(receipt["proposal"]["current"], false);
    assert_eq!(receipt["proposal"]["action"], "promote");
    assert_eq!(receipt["proposal"]["subject_kind"], "candidate");
    assert_eq!(receipt["proposal"]["subject_id"], "wiki_candidate");
    assert_eq!(receipt["audit"]["id"], audit_id);
    assert_eq!(receipt["event"]["id"], event_id);
    let receipt_command = receipt["preview_command"]
        .as_str()
        .expect("receipt command");
    assert!(receipt_command.contains("[REDACTED]"));
    assert_eq!(
        receipt["preview_command_sha256"],
        sha256_hex(receipt_command.as_bytes())
    );
    assert_eq!(
        receipt["preview_command_sha256"]
            .as_str()
            .expect("receipt command hash")
            .len(),
        64
    );
    assert!(receipt["checks"]
        .as_array()
        .expect("receipt checks")
        .iter()
        .all(|check| check["passed"].as_bool().expect("check passed bool")));
    assert!(receipt["blockers"]
        .as_array()
        .expect("receipt blockers")
        .is_empty());

    let output_path = temp.path().join("exports").join("proposal-receipt.json");
    let export_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-receipt",
            proposal_id,
            "--audit-id",
            audit_id,
            "--event-id",
            event_id,
            "--command",
            &executed_command,
            "--output",
            output_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(export_output.status.success());
    let export_stdout = String::from_utf8_lossy(&export_output.stdout);
    assert!(!export_stdout.contains(secret));
    let export_receipt: serde_json::Value = serde_json::from_slice(&export_output.stdout)?;
    assert_eq!(
        export_receipt["exported_to"],
        output_path.to_str().expect("utf-8 path")
    );
    assert_eq!(export_receipt["receipt"]["status"], "linked");
    assert_eq!(
        export_receipt["bytes_written"]
            .as_u64()
            .expect("bytes written"),
        fs::metadata(&output_path)?.len()
    );
    let exported_receipt: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&output_path)?)?;
    assert_eq!(exported_receipt, export_receipt["receipt"]);
    assert!(!fs::read_to_string(&output_path)?.contains(secret));

    let incomplete_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "proposal-receipt",
            proposal_id,
            "--audit-id",
            "missing_audit",
            "--event-id",
            event_id,
            "--command",
            &executed_command,
            "--json",
        ])
        .output()?;
    assert!(incomplete_output.status.success());
    let incomplete: serde_json::Value = serde_json::from_slice(&incomplete_output.stdout)?;
    assert_eq!(incomplete["status"], "incomplete");
    assert!(incomplete["blockers"]
        .as_array()
        .expect("incomplete blockers")
        .iter()
        .any(|blocker| blocker
            .as_str()
            .expect("blocker text")
            .contains("missing_audit was not found")));
    assert!(incomplete["checks"]
        .as_array()
        .expect("incomplete checks")
        .iter()
        .any(|check| check["name"] == "audit_matches_proposal"
            && !check["passed"].as_bool().expect("check passed")
            && check["detail"]
                .as_str()
                .expect("check detail")
                .contains("audit metadata unavailable")));
    assert_eq!(fs::read_to_string(&audit_path)?, audit_before);
    assert_eq!(fs::read_to_string(&events_path)?, events_before);
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_promotion_chain_reports_snapshots_and_usage_without_mutation() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let promotion_at = Utc::now();
    let after = promotion_at + Duration::hours(1);
    let secret = "sk-secretsecretsecretsecret";
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_chain_entry",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Keep promotion evidence replayable",
                    "ai_instruction": "Use the promotion evidence chain.",
                    "human_summary": "Promotion chain target",
                    "evidence_refs": ["task:task_chain"],
                    "confidence": "explicit",
                    "created_at": promotion_at,
                    "updated_at": after
                }
            ]
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_candidates.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "candidates": []
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_usage.jsonl"),
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "id": "wiki_usage_chain",
                "entry_id": "wiki_chain_entry",
                "task_id": "task_chain",
                "request_id": format!("request_chain?token={secret}"),
                "project_key": "project",
                "artifact_kind": "report",
                "projection_kind": "runtime_probe",
                "activation_mode": "confirm",
                "created_at": after
            }))?
        ),
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_audit.jsonl"),
        format!(
            "{}\n",
            serde_json::to_string(&json!({
                "id": "wiki_audit_chain_promote",
                "action": "promote",
                "subject_id": "wiki_chain_entry",
                "candidate_id": "wiki_chain_candidate",
                "entry_id": "wiki_chain_entry",
                "actor": "cli",
                "reason": format!("promotion chain token={secret}"),
                "evidence_ref": "task:task_chain",
                "candidate_snapshot": {
                    "id": "wiki_chain_candidate",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "claim": "Keep promotion evidence replayable",
                    "human_summary": "Promotion candidate",
                    "evidence_refs": ["task:task_chain"],
                    "signal_kind": "operator_correction",
                    "origin": "operator_explicit",
                    "source_refs": [format!("approval:chain?token={secret}")],
                    "source_hashes": ["sha256:chain"],
                    "review_reason": "Promotion chain review",
                    "occurrence_count": 2,
                    "confidence": "explicit",
                    "updated_at": promotion_at,
                    "last_seen_at": promotion_at
                },
                "entry_snapshot": {
                    "id": "wiki_chain_entry",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Keep promotion evidence replayable",
                    "human_summary": "Promotion entry",
                    "evidence_refs": ["task:task_chain"],
                    "counterexamples": [],
                    "confidence": "explicit",
                    "updated_at": promotion_at
                },
                "created_at": promotion_at
            }))?
        ),
    )?;

    let entries_before = fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?;
    let candidates_before = fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?;
    let audit_before = fs::read_to_string(profile_dir.join("adaptive_wiki_audit.jsonl"))?;
    let dry_run_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "promotion-chain",
            "wiki_chain_entry",
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(dry_run_output.status.success());
    let dry_run: serde_json::Value = serde_json::from_slice(&dry_run_output.stdout)?;
    assert_eq!(dry_run["dry_run"], true);
    assert_eq!(dry_run["summary"]["files_written"], 0);
    assert_eq!(dry_run["summary"]["promotion_audit_found"], true);
    assert_eq!(dry_run["summary"]["candidate_snapshot_present"], true);
    assert_eq!(dry_run["summary"]["entry_snapshot_present"], true);
    assert_eq!(dry_run["summary"]["usage_records"], 1);
    assert_eq!(dry_run["summary"]["related_audit_records"], 1);
    assert!(!String::from_utf8_lossy(&dry_run_output.stdout).contains(secret));
    assert!(!profile_dir.join("adaptive_wiki_promotion_chains").exists());
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        entries_before
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?,
        candidates_before
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_audit.jsonl"))?,
        audit_before
    );

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "promotion-chain",
            "wiki_chain_entry",
            "--json",
        ])
        .output()?;
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["dry_run"], false);
    assert_eq!(report["summary"]["files_written"], 3);
    assert_eq!(report["candidate_snapshot"]["id"], "wiki_chain_candidate");
    assert_eq!(report["entry_snapshot"]["id"], "wiki_chain_entry");
    assert_eq!(report["current_entry"]["id"], "wiki_chain_entry");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
    let report_dir = PathBuf::from(report["report_dir"].as_str().expect("chain report dir"));
    assert!(report_dir.join("report.json").is_file());
    assert!(report_dir.join("chain.jsonl").is_file());
    assert!(report_dir.join("REPORT.md").is_file());
    let chain_jsonl = fs::read_to_string(report_dir.join("chain.jsonl"))?;
    assert!(chain_jsonl.contains("promotion_audit"));
    assert!(chain_jsonl.contains("candidate_snapshot"));
    assert!(chain_jsonl.contains("usage"));
    assert!(!chain_jsonl.contains(secret));
    let report_md = fs::read_to_string(report_dir.join("REPORT.md"))?;
    assert!(report_md.contains("Adaptive Wiki Promotion Evidence Chain"));
    assert!(report_md.contains("wiki_chain_entry"));
    assert!(!report_md.contains(secret));
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        entries_before
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_candidates.json"))?,
        candidates_before
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_audit.jsonl"))?,
        audit_before
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_review_commands_mutate_entries_and_append_audit() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": []
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_candidates.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "candidates": [
                {
                    "id": "wiki_candidate_promote",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project-a",
                    "claim": "Ask before retrying dispatch",
                    "suggested_ai_instruction": "Ask the operator before retrying dispatch.",
                    "human_summary": "Captured denial",
                    "evidence_refs": ["approval:one"],
                    "signal_kind": "approval_denial",
                    "origin": "operator_explicit",
                    "source_refs": ["approval:one"],
                    "occurrence_count": 2,
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "last_seen_at": now
                },
                {
                    "id": "wiki_candidate_reject",
                    "kind": "fact",
                    "scope": "project",
                    "scope_ref": "project-a",
                    "claim": "Reject this",
                    "suggested_ai_instruction": "Do not promote this.",
                    "human_summary": "Low quality candidate",
                    "evidence_refs": ["task:reject"],
                    "signal_kind": "unknown",
                    "origin": "unknown",
                    "source_refs": ["task:reject"],
                    "occurrence_count": 1,
                    "confidence": "inferred",
                    "created_at": now,
                    "updated_at": now,
                    "last_seen_at": now
                }
            ]
        }))?,
    )?;

    let promote_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "promote",
            "wiki_candidate_promote",
            "--scope",
            "artifact_kind",
            "--scope-ref",
            "report",
            "--activation-mode",
            "context_only",
            "--reason",
            &format!("promote after review token={secret}"),
            "--json",
        ])
        .output()?;
    assert!(promote_output.status.success());
    assert!(!String::from_utf8_lossy(&promote_output.stdout).contains(secret));
    let promoted: serde_json::Value = serde_json::from_slice(&promote_output.stdout)?;
    assert_eq!(promoted["action"], "promote");
    assert_eq!(promoted["entry"]["scope"], "artifact_kind");
    assert_eq!(promoted["entry"]["scope_ref"], "report");
    assert_eq!(promoted["entry"]["activation_mode"], "context_only");
    assert!(promoted["audit"]["reason"]
        .as_str()
        .expect("audit reason")
        .contains("[REDACTED]"));
    assert_eq!(
        promoted["audit"]["candidate_snapshot"]["id"],
        "wiki_candidate_promote"
    );
    assert_eq!(
        promoted["audit"]["candidate_snapshot"]["scope_ref"],
        "project-a"
    );
    let entry_id = promoted["entry"]["id"]
        .as_str()
        .expect("entry id")
        .to_string();
    assert_eq!(promoted["audit"]["entry_snapshot"]["id"], entry_id.as_str());
    assert_eq!(promoted["audit"]["entry_snapshot"]["scope_ref"], "report");
    assert_eq!(
        promoted["promotion_receipt"]["schema"],
        "adaptive_wiki_promotion_receipt.v1"
    );
    assert_eq!(promoted["promotion_receipt"]["status"], "promoted");
    assert_eq!(
        promoted["promotion_receipt"]["candidate_id"],
        "wiki_candidate_promote"
    );
    assert_eq!(promoted["promotion_receipt"]["entry_id"], entry_id.as_str());
    assert_eq!(
        promoted["promotion_receipt"]["audit_id"],
        promoted["audit"]["id"]
    );
    assert_eq!(
        promoted["promotion_receipt"]["authority"]["canonical_mutation_recorded"],
        true
    );
    let promotion_receipt_path = PathBuf::from(
        promoted["promotion_receipt_path"]
            .as_str()
            .expect("promotion receipt path"),
    );
    assert!(promotion_receipt_path.is_file());
    let promotion_receipt_file = fs::read_to_string(&promotion_receipt_path)?;
    assert!(!promotion_receipt_file.contains(secret));

    let review_after_promotion_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "review", "--dry-run", "--json"])
        .output()?;
    assert!(review_after_promotion_output.status.success());
    let review_after_promotion: serde_json::Value =
        serde_json::from_slice(&review_after_promotion_output.stdout)?;
    assert_eq!(
        review_after_promotion["summary"]["promotion_receipts_checked"],
        1
    );
    assert_eq!(
        review_after_promotion["summary"]["promotion_receipt_files_invalid"],
        0
    );
    assert_eq!(
        review_after_promotion["summary"]["promoted_entries_with_promotion_receipt"],
        1
    );
    assert_eq!(
        review_after_promotion["summary"]["promoted_entries_missing_promotion_receipt"],
        0
    );
    let invalid_receipt_path = promotion_receipt_path
        .parent()
        .expect("promotion receipt parent")
        .join("broken_promotion_receipt.json");
    fs::write(&invalid_receipt_path, "{not valid json")?;
    let review_with_invalid_receipt_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "review", "--dry-run", "--json"])
        .output()?;
    assert!(review_with_invalid_receipt_output.status.success());
    let review_with_invalid_receipt: serde_json::Value =
        serde_json::from_slice(&review_with_invalid_receipt_output.stdout)?;
    assert_eq!(
        review_with_invalid_receipt["summary"]["promotion_receipts_checked"],
        1
    );
    assert_eq!(
        review_with_invalid_receipt["summary"]["promotion_receipt_files_invalid"],
        1
    );
    assert_eq!(
        review_with_invalid_receipt["summary"]["promoted_entries_with_promotion_receipt"],
        1
    );

    let candidates_after_promote: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("adaptive_wiki_candidates.json"),
    )?)?;
    assert_eq!(
        candidates_after_promote["candidates"]
            .as_array()
            .expect("candidates")
            .len(),
        1
    );
    assert_eq!(
        candidates_after_promote["candidates"][0]["id"],
        "wiki_candidate_reject"
    );

    let reject_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "reject",
            "wiki_candidate_reject",
            "--reason",
            "not durable enough",
            "--json",
        ])
        .output()?;
    assert!(reject_output.status.success());
    let rejected: serde_json::Value = serde_json::from_slice(&reject_output.stdout)?;
    assert_eq!(rejected["action"], "reject");
    assert_eq!(rejected["candidate"]["id"], "wiki_candidate_reject");

    let rescope_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "rescope",
            &entry_id,
            "--scope",
            "project",
            "--scope-ref",
            "project-b",
            "--reason",
            "narrow to project",
            "--json",
        ])
        .output()?;
    assert!(rescope_output.status.success());
    let rescoped: serde_json::Value = serde_json::from_slice(&rescope_output.stdout)?;
    assert_eq!(rescoped["action"], "rescope");
    assert_eq!(rescoped["entry"]["scope"], "project");
    assert_eq!(rescoped["entry"]["scope_ref"], "project-b");
    assert_eq!(rescoped["audit"]["before_scope"]["scope"], "artifact_kind");
    assert_eq!(rescoped["audit"]["after_scope"]["scope"], "project");

    let runbook_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "update-runbook",
            &entry_id,
            "--support-ref",
            &format!("references/report.md?token={secret}"),
            "--capability-id",
            "capability.syncback",
            "--required-artifact-kind",
            "report",
            "--reason",
            "attach runbook refs",
            "--json",
        ])
        .output()?;
    assert!(runbook_output.status.success());
    assert!(!String::from_utf8_lossy(&runbook_output.stdout).contains(secret));
    let runbook: serde_json::Value = serde_json::from_slice(&runbook_output.stdout)?;
    assert_eq!(runbook["action"], "update_runbook");
    assert!(runbook["entry"]["support_refs"][0]
        .as_str()
        .expect("support ref")
        .contains("[REDACTED]"));
    assert_eq!(
        runbook["entry"]["capability_ids"],
        json!(["capability.syncback"])
    );
    assert_eq!(
        runbook["entry"]["required_artifact_kinds"],
        json!(["report"])
    );

    let runbook_projection_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "projection",
            "--project-key",
            "project-b",
            "--artifact-kind",
            "report",
            "--json",
        ])
        .output()?;
    assert!(runbook_projection_output.status.success());
    let runbook_projection: serde_json::Value =
        serde_json::from_slice(&runbook_projection_output.stdout)?;
    let runbook_projection_text = serde_json::to_string(&runbook_projection)?;
    assert_eq!(runbook_projection.as_array().expect("projection").len(), 1);
    assert!(!runbook_projection_text.contains("support_refs"));
    assert!(!runbook_projection_text.contains("capability.syncback"));
    assert!(!runbook_projection_text.contains("references/report.md"));

    let edit_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "edit",
            &entry_id,
            "--kind",
            "policy_rule",
            "--agent-mode",
            "planning",
            "--agent-mode",
            "review",
            "--reason",
            "apply curator classification",
            "--json",
        ])
        .output()?;
    assert!(edit_output.status.success());
    let edited: serde_json::Value = serde_json::from_slice(&edit_output.stdout)?;
    assert_eq!(edited["action"], "edit");
    assert_eq!(edited["entry"]["kind"], "policy_rule");
    assert_eq!(
        edited["entry"]["agent_modes"],
        json!(["planning", "review"])
    );
    assert_eq!(edited["audit"]["entry_snapshot"]["kind"], "policy_rule");
    assert_eq!(
        edited["audit"]["entry_snapshot"]["agent_modes"],
        json!(["planning", "review"])
    );

    let counterexample_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "add-counterexample",
            &entry_id,
            "--evidence-ref",
            "audit:counterexample",
            "--reason",
            "limited case",
            "--json",
        ])
        .output()?;
    assert!(counterexample_output.status.success());
    let counterexample: serde_json::Value = serde_json::from_slice(&counterexample_output.stdout)?;
    assert_eq!(counterexample["action"], "add_counterexample");
    assert!(counterexample["entry"]["counterexamples"]
        .as_array()
        .expect("counterexamples")
        .iter()
        .any(|value| value == "audit:counterexample"));

    let deprecate_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "deprecate",
            &entry_id,
            "--reason",
            &format!("superseded token={secret}"),
            "--json",
        ])
        .output()?;
    assert!(deprecate_output.status.success());
    assert!(!String::from_utf8_lossy(&deprecate_output.stdout).contains(secret));
    let deprecated: serde_json::Value = serde_json::from_slice(&deprecate_output.stdout)?;
    assert_eq!(deprecated["action"], "deprecate");
    assert_eq!(deprecated["entry"]["status"], "deprecated");
    assert!(deprecated["audit"]["reason"]
        .as_str()
        .expect("audit reason")
        .contains("[REDACTED]"));

    let projection_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "projection",
            "--project-key",
            "project-b",
            "--json",
        ])
        .output()?;
    assert!(projection_output.status.success());
    let projection: serde_json::Value = serde_json::from_slice(&projection_output.stdout)?;
    assert!(projection.as_array().expect("projection").is_empty());

    let audit = fs::read_to_string(profile_dir.join("adaptive_wiki_audit.jsonl"))?;
    assert!(!audit.contains(secret));
    assert_eq!(audit.lines().count(), 7);
    assert!(audit.contains("\"action\":\"promote\""));
    assert!(audit.contains("\"candidate_snapshot\""));
    assert!(audit.contains("\"entry_snapshot\""));
    assert!(audit.contains("\"action\":\"reject\""));
    assert!(audit.contains("\"action\":\"rescope\""));
    assert!(audit.contains("\"action\":\"update_runbook\""));
    assert!(audit.contains("\"action\":\"edit\""));
    assert!(audit.contains("\"action\":\"add_counterexample\""));
    assert!(audit.contains("\"action\":\"deprecate\""));
    Ok(())
}

#[test]
#[serial]
fn offdesk_deny_records_adaptive_wiki_approval_denial_candidate() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("pending_action_approvals.json"),
        serde_json::to_string_pretty(&json!([
            {
                "approval_id": "approval_one",
                "status": "pending",
                "scope": "once",
                "project_key": "project",
                "request_id": "request",
                "task_id": "task",
                "action": "dispatch.runtime",
                "risk_level": "runtime_mutation",
                "approval_mode": "operator_required",
                "preview": "safe preview",
                "reason": "outside envelope",
                "created_at": now,
                "expires_at": now + Duration::minutes(10),
                "source_surface": "test"
            }
        ]))?,
    )?;

    let deny_output = forager_command(temp.path())
        .args(["offdesk", "deny", "approval_one", "--json"])
        .output()?;
    assert!(deny_output.status.success());
    let denied: serde_json::Value = serde_json::from_slice(&deny_output.stdout)?;
    assert_eq!(denied["status"], "denied");
    assert_eq!(denied["approval_id"], "approval_one");

    let candidates_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "candidates",
            "--project-key",
            "project",
            "--json",
        ])
        .output()?;
    assert!(candidates_output.status.success());
    let candidates: serde_json::Value = serde_json::from_slice(&candidates_output.stdout)?;
    assert_eq!(candidates.as_array().expect("candidates").len(), 1);
    let candidate = &candidates[0];
    assert_eq!(candidate["kind"], "policy_rule");
    assert_eq!(candidate["scope"], "project");
    assert_eq!(candidate["scope_ref"], "project");
    assert_eq!(candidate["signal_kind"], "approval_denial");
    assert_eq!(candidate["origin"], "operator_explicit");
    assert_eq!(candidate["confidence"], "explicit");
    assert_eq!(candidate["suggested_scope"]["scope"], "project");
    assert_eq!(candidate["suggested_scope"]["scope_ref"], "project");
    assert!(candidate["claim"]
        .as_str()
        .expect("claim")
        .contains("Operator denied `dispatch.runtime`"));
    assert!(candidate["evidence_refs"]
        .as_array()
        .expect("evidence refs")
        .iter()
        .any(|value| value == "approval:approval_one"));
    assert!(candidate["source_refs"]
        .as_array()
        .expect("source refs")
        .iter()
        .any(|value| value == "approval:approval_one"));
    assert!(candidate["source_refs"]
        .as_array()
        .expect("source refs")
        .iter()
        .any(|value| value == "task:task"));
    assert!(candidate["source_refs"]
        .as_array()
        .expect("source refs")
        .iter()
        .any(|value| value == "request:request"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_gate_blocks_missing_required_artifact_before_approval() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "gate",
            "canonical.syncback",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let outcome: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcome["status"], "blocked");
    assert!(outcome["reason"]
        .as_str()
        .expect("reason")
        .contains("mutation_snapshot"));
    assert!(!profile_dir(temp.path())
        .join("pending_action_approvals.json")
        .exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_gate_accepts_supplied_required_artifact() -> Result<()> {
    let temp = tempdir()?;
    let artifact_path = temp.path().join("mutation.json");
    fs::write(&artifact_path, "{}")?;
    let artifact_arg = format!("mutation_snapshot={}", artifact_path.display());

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "gate",
            "canonical.syncback",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--artifact",
            artifact_arg.as_str(),
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let outcome: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcome["status"], "pending_approval");
    assert_eq!(outcome["artifact_check"]["satisfied"], true);
    assert_eq!(
        outcome["artifact_check"]["missing_artifact_ids"]
            .as_array()
            .map(Vec::len)
            .unwrap_or(0),
        0
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_launch_without_brief_creates_pending_and_no_background_run() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "launch",
            "background.launch",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--ticket-id",
            "ticket",
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let outcome: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcome["gate"]["status"], "pending_approval");
    assert!(outcome.get("probe").is_none());

    let profile_dir = profile_dir(temp.path());
    let approvals: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(approvals.as_array().expect("approvals").len(), 1);
    assert!(!profile_dir.join("background_runs.json").exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_launch_with_brief_records_background_run() -> Result<()> {
    let temp = tempdir()?;
    write_implementation_packet_fixture(temp.path(), "project", "packet-launch-test")?;
    let brief_path = temp.path().join("brief.json");
    let now = Utc::now();
    fs::write(
        &brief_path,
        serde_json::to_string_pretty(&json!({
            "request_id": "request",
            "task_id": "task",
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["background.launch"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(10)
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "launch",
            "background.launch",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--ticket-id",
            "ticket",
            "--launch-spec",
            "token=sk-secretsecretsecretsecret",
            "--brief",
            brief_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let outcome: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcome["gate"]["status"], "proceed");
    assert_eq!(outcome["probe"]["ticket_id"], "ticket");
    assert_eq!(outcome["probe"]["phase"], "launched");
    assert_eq!(
        outcome["probe"]["implementation_packet"]["packet_id"],
        "packet-launch-test"
    );
    assert_eq!(
        outcome["probe"]["implementation_packet"]["safe_to_delegate"],
        true
    );
    assert!(!outcome["probe"]["launch_spec_summary"]
        .as_str()
        .expect("summary")
        .contains("sk-secret"));

    let runs: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir(temp.path()).join("background_runs.json"),
    )?)?;
    assert_eq!(runs[0]["ticket_id"], "ticket");
    assert_eq!(
        runs[0]["implementation_packet"]["packet_id"],
        "packet-launch-test"
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_poll_persists_background_phase_transition() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    fs::write(
        profile_dir.join("background_runs.json"),
        serde_json::to_string_pretty(&json!([
            {
                "ticket_id": "ticket",
                "runner_kind": "local_background",
                "phase": "launched",
                "runtime_handle_alive": false,
                "result_artifact_present": true
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "poll", "ticket", "--json"])
        .output()?;

    assert!(output.status.success());
    let outcomes: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcomes[0]["decision"]["phase"], "completed");
    assert_eq!(outcomes[0]["next_safe_action"]["kind"], "closeout_check");
    assert_eq!(
        outcomes[0]["next_safe_action"]["requires_operator_review"],
        true
    );

    let runs: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("background_runs.json"),
    )?)?;
    assert_eq!(runs[0]["phase"], "completed");
    assert_eq!(
        runs[0]["last_recovery_evidence"],
        "local background result artifact present"
    );
    assert_eq!(runs[0]["last_recovery_terminal"], true);
    assert!(runs[0]["last_observed_at"].as_str().is_some());
    Ok(())
}

#[test]
#[serial]
fn offdesk_poll_marks_stale_background_heartbeat() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("background_runs.json"),
        serde_json::to_string_pretty(&json!([
            {
                "ticket_id": "ticket",
                "runner_kind": "local_background",
                "phase": "launched",
                "runtime_handle_alive": true,
                "worker_heartbeat_at": now - Duration::minutes(20),
                "heartbeat_timeout_sec": 300
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "poll", "ticket", "--json"])
        .output()?;

    assert!(output.status.success());
    let outcomes: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcomes[0]["decision"]["phase"], "stale_lost_callback");
    assert_eq!(outcomes[0]["probe"]["worker_heartbeat_stale"], true);
    assert_eq!(
        outcomes[0]["next_safe_action"]["kind"],
        "resume_review_required"
    );
    assert!(outcomes[0]["next_safe_action"]["commands"]
        .as_array()
        .expect("next action commands")
        .iter()
        .any(|command| command
            .as_str()
            .expect("next action command")
            .contains("forager offdesk poll ticket")));

    let runs: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("background_runs.json"),
    )?)?;
    assert_eq!(runs[0]["phase"], "stale_lost_callback");
    assert_eq!(runs[0]["worker_heartbeat_stale"], true);
    assert_eq!(
        runs[0]["last_recovery_evidence"],
        "local background heartbeat is stale"
    );
    assert_eq!(runs[0]["last_recovery_terminal"], false);
    assert!(runs[0]["last_observed_at"].as_str().is_some());
    Ok(())
}

#[test]
#[serial]
fn offdesk_launch_executes_local_background_command_and_poll_completes() -> Result<()> {
    let temp = tempdir()?;
    let brief_path = temp.path().join("brief.json");
    let result_path = temp.path().join("result.txt");
    let log_path = temp.path().join("background.log");
    let now = Utc::now();
    fs::write(
        &brief_path,
        serde_json::to_string_pretty(&json!({
            "request_id": "request",
            "task_id": "task",
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["background.launch"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(10)
        }))?,
    )?;
    let command = format!(
        "printf 'token=sk-secretsecretsecretsecret\\n'; printf done > {}",
        result_path.display()
    );

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "launch",
            "background.launch",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--ticket-id",
            "ticket",
            "--brief",
            brief_path.to_str().expect("utf-8 path"),
            "--cmd",
            command.as_str(),
            "--workdir",
            temp.path().to_str().expect("utf-8 path"),
            "--log-artifact",
            log_path.to_str().expect("utf-8 path"),
            "--result-artifact",
            result_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let outcome: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcome["gate"]["status"], "proceed");
    assert_eq!(outcome["probe"]["ticket_id"], "ticket");
    assert_eq!(
        outcome["probe"]["log_artifact_path"].as_str(),
        Some(log_path.to_string_lossy().as_ref())
    );
    assert_eq!(
        outcome["probe"]["result_artifact_path"].as_str(),
        Some(result_path.to_string_lossy().as_ref())
    );

    wait_for_path(&result_path);
    assert!(result_path.exists());

    let poll_output = forager_command(temp.path())
        .args(["offdesk", "poll", "ticket", "--json"])
        .output()?;
    assert!(poll_output.status.success());
    let outcomes: serde_json::Value = serde_json::from_slice(&poll_output.stdout)?;
    assert_eq!(outcomes[0]["decision"]["phase"], "completed");
    assert!(!outcomes[0]["probe"]["last_log_tail"]
        .as_str()
        .expect("log tail")
        .contains("sk-secret"));

    let runs: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir(temp.path()).join("background_runs.json"),
    )?)?;
    assert_eq!(runs[0]["phase"], "completed");
    assert!(runs[0]["log_artifact_present"].as_bool().unwrap_or(false));
    assert!(runs[0]["result_artifact_present"]
        .as_bool()
        .unwrap_or(false));
    assert_eq!(
        runs[0]["last_recovery_evidence"],
        "local background result artifact present"
    );
    assert!(runs[0]["last_observed_at"].as_str().is_some());
    Ok(())
}

#[test]
#[serial]
fn offdesk_enqueue_tasks_json_redacts_command() -> Result<()> {
    let temp = tempdir()?;
    write_implementation_packet_fixture(temp.path(), "project", "packet-offdesk-test")?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--cmd",
            "printf token=sk-secretsecretsecretsecret",
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let task: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(task["task_id"], "task");
    assert_eq!(
        task["implementation_packet"]["packet_id"],
        "packet-offdesk-test"
    );
    assert_eq!(task["implementation_packet"]["outcome"], "pass");
    assert_eq!(task["implementation_packet"]["safe_to_delegate"], true);
    assert!(task["artifact_refs"]
        .as_array()
        .expect("artifact refs")
        .iter()
        .any(
            |artifact| artifact["artifact_id"] == "implementation_packet"
                && artifact["path"]
                    .as_str()
                    .expect("implementation packet path")
                    .contains("IMPLEMENTATION_PACKET.json")
        ));
    assert!(!task["command"]
        .as_str()
        .expect("command")
        .contains("sk-secret"));

    let tasks_output = forager_command(temp.path())
        .args(["offdesk", "tasks", "--json"])
        .output()?;
    assert!(tasks_output.status.success());
    let tasks: serde_json::Value = serde_json::from_slice(&tasks_output.stdout)?;
    assert_eq!(
        tasks[0]["implementation_packet"]["packet_id"],
        "packet-offdesk-test"
    );
    assert!(!tasks[0]["command"]
        .as_str()
        .expect("command")
        .contains("sk-secret"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_tasks_json_filters_by_project_status_task_and_latest() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let mut other_project = durable_task_with(
        "other-completed",
        "dispatch.runtime",
        "completed",
        now + Duration::seconds(3),
        "true",
        temp.path(),
    );
    other_project["project_key"] = json!("other");
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            durable_task_with(
                "queued-task",
                "dispatch.runtime",
                "queued",
                now,
                "true",
                temp.path(),
            ),
            durable_task_with(
                "completed-old",
                "dispatch.runtime",
                "completed",
                now + Duration::seconds(1),
                "true",
                temp.path(),
            ),
            durable_task_with(
                "completed-new",
                "dispatch.runtime",
                "completed",
                now + Duration::seconds(2),
                "true",
                temp.path(),
            ),
            other_project
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "tasks",
            "--json",
            "--project-key",
            "project",
            "--status",
            "completed",
        ])
        .output()?;
    assert!(output.status.success());
    let tasks: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let task_ids: Vec<_> = tasks
        .as_array()
        .expect("tasks array")
        .iter()
        .map(|task| task["task_id"].as_str().expect("task id"))
        .collect();
    assert_eq!(task_ids, vec!["completed-old", "completed-new"]);

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "tasks",
            "--json",
            "--project-key",
            "project",
            "--status",
            "completed",
            "--latest",
        ])
        .output()?;
    assert!(output.status.success());
    let tasks: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let task_ids: Vec<_> = tasks
        .as_array()
        .expect("tasks array")
        .iter()
        .map(|task| task["task_id"].as_str().expect("task id"))
        .collect();
    assert_eq!(task_ids, vec!["completed-new"]);

    let output = forager_command(temp.path())
        .args(["offdesk", "tasks", "--json", "--task-id", "queued-task"])
        .output()?;
    assert!(output.status.success());
    let tasks: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(tasks.as_array().expect("tasks array").len(), 1);
    assert_eq!(tasks[0]["task_id"], "queued-task");
    Ok(())
}

#[test]
#[serial]
fn offdesk_tasks_human_includes_recovery_commands_and_redacts_secrets() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            durable_task_with(
                "failed-task",
                "dispatch.runtime",
                "failed",
                now,
                "printf token=sk-secretsecretsecretsecret",
                temp.path(),
            ),
            durable_task_with(
                "resume-task",
                "dispatch.runtime",
                "resume_pending",
                now,
                "true",
                temp.path(),
            ),
            durable_task_with(
                "queued-task",
                "dispatch.runtime",
                "queued",
                now,
                "true",
                temp.path(),
            ),
            durable_task_with(
                "done-task",
                "dispatch.runtime",
                "completed",
                now,
                "true",
                temp.path(),
            )
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "tasks"])
        .output()?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("forager offdesk retry-task failed-task"));
    assert!(stdout.contains("forager offdesk retry-task failed-task --new-approval"));
    assert!(stdout.contains("forager offdesk resume-task resume-task"));
    assert!(stdout.contains("forager offdesk abandon-task resume-task"));
    assert!(stdout.contains("forager offdesk tick"));
    assert!(stdout.contains("forager offdesk cancel-task queued-task"));
    assert!(stdout.contains("verify closeout before treating Offdesk output as accepted"));
    assert!(stdout.contains("forager offdesk closeout --project-key project --task-id done-task"));
    assert!(!stdout.contains("sk-secret"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_debug_bundle_json_redacts_legacy_state_and_is_read_only() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    let runner_context =
        "<!-- FORAGER:RUNNER_CONTEXT_BEGIN -->runner-only hidden<!-- FORAGER:RUNNER_CONTEXT_END -->";

    fs::write(
        profile_dir.join("pending_action_approvals.json"),
        serde_json::to_string_pretty(&json!([
            {
                "approval_id": "approval_one",
                "status": "pending",
                "scope": "once",
                "project_key": "project",
                "request_id": "request",
                "task_id": "task",
                "action": "dispatch.runtime",
                "risk_level": "runtime_mutation",
                "approval_mode": "operator_required",
                "preview": format!("preview token={secret} {runner_context}"),
                "reason": "https://example.com?access_token=secret123",
                "created_at": now,
                "expires_at": now + Duration::minutes(10),
                "source_surface": "test",
                "metadata": {
                    "kind": "provider_fallback",
                    "current_provider_id": "openai",
                    "current_model": "gpt-4.1",
                    "runner_role": "worker",
                    "generated_at": now,
                    "candidate_limit": 3,
                    "candidates": [
                        {
                            "provider_id": "openai",
                            "model": "gpt-4.1-mini",
                            "source": "same_provider_model",
                            "auth_status": "available",
                            "capacity_status": "available",
                            "recommended": true,
                            "reason": format!("same provider token={secret}")
                        }
                    ],
                    "apply_scope": "request_matching_provider_model"
                }
            }
        ]))?,
    )?;
    let mut task = durable_task(
        "failed",
        now,
        &format!("printf token={secret}"),
        temp.path(),
    );
    task["last_provider_fallback"] = json!({
        "current_provider_id": "openai",
        "current_model": "gpt-4.1",
        "trigger_reason": format!("cooldown token={secret}"),
        "generated_at": now,
        "candidates": [
            {
                "provider_id": "anthropic",
                "model": "claude-3-5-sonnet-latest",
                "source": "cross_provider_fallback_model",
                "auth_status": "missing_auth",
                "capacity_status": "available",
                "recommended": false,
                "reason": format!("auth token={secret}")
            }
        ]
    });
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([task]))?,
    )?;
    fs::write(
        profile_dir.join("task_resume_state.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task",
                "request_id": "request",
                "project_key": "project",
                "status": "resume_pending",
                "phase": "background",
                "runner_target": "local_background",
                "last_evidence_artifacts": [],
                "evidence": [
                    {
                        "kind": "log_tail",
                        "summary": format!("tail token={secret} {runner_context}"),
                        "observed_at": now
                    }
                ],
                "last_log_tail": format!("tail token={secret}"),
                "next_safe_resume_step": "inspect result sidecar",
                "interrupted_at": now,
                "interruption_reason": format!("restart token={secret}"),
                "fresh_until": now + Duration::minutes(10)
            }
        ]))?,
    )?;
    fs::write(
        profile_dir.join("background_runs.json"),
        serde_json::to_string_pretty(&json!([
            {
                "ticket_id": "ticket",
                "runner_kind": "local_background",
                "phase": "launched",
                "launch_spec_summary": format!("cmd token={secret}"),
                "runtime_handle_alive": false,
                "last_log_tail": format!("log token={secret} {runner_context}")
            }
        ]))?,
    )?;
    fs::write(
        profile_dir.join("provider_capacity.json"),
        serde_json::to_string_pretty(&json!([
            {
                "provider_id": "openai",
                "model": "gpt-test",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": now + Duration::minutes(1),
                "last_error_summary": format!("provider token={secret}"),
                "updated_at": now
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "debug-bundle", "--json"])
        .output()?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains(secret));
    assert!(!stdout.contains("secret123"));
    assert!(!stdout.contains("runner-only hidden"));
    let bundle: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(bundle["read_only"], true);
    assert_eq!(bundle["redaction_applied"], true);
    assert!(
        bundle["redaction_summary"]["secrets_redacted"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    assert!(
        bundle["redaction_summary"]["runner_context_removed"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    assert_eq!(bundle["tasks"][0]["status"], "failed");
    assert_eq!(bundle["tasks"][0]["mode_verdict"], "unscoped");
    assert_eq!(bundle["tasks"][0]["mode_risk"], "missing_agent_mode");
    assert_eq!(
        bundle["tasks"][0]["last_provider_fallback"]["current_provider_id"],
        "openai"
    );
    assert!(
        !bundle["tasks"][0]["last_provider_fallback"]["trigger_reason"]
            .as_str()
            .expect("trigger reason")
            .contains(secret)
    );
    assert_eq!(
        bundle["background_runs"][0]["decision"]["phase"],
        "stale_lost_callback"
    );
    assert_eq!(bundle["background_runs"][0]["mode_verdict"], "unscoped");
    assert_eq!(
        bundle["background_runs"][0]["mode_risk"],
        "missing_agent_mode"
    );

    let stored_approvals = fs::read_to_string(profile_dir.join("pending_action_approvals.json"))?;
    assert!(stored_approvals.contains(secret));
    assert!(!profile_dir.join("action_audit.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_maintenance_report_json_is_read_only_and_summarizes_mode_risks() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let result_path = temp.path().join("writing-result.json");
    fs::write(&result_path, "{\"ok\":true}\n")?;

    fs::write(
        profile_dir.join("pending_action_approvals.json"),
        serde_json::to_string_pretty(&json!([
            {
                "approval_id": "approval_pending",
                "status": "pending",
                "scope": "once",
                "project_key": "project",
                "request_id": "request",
                "task_id": "writing-task",
                "action": "dispatch.runtime",
                "risk_level": "runtime_mutation",
                "approval_mode": "operator_required",
                "preview": "safe preview",
                "reason": "operator review",
                "created_at": now,
                "expires_at": now + Duration::minutes(10),
                "source_surface": "test"
            }
        ]))?,
    )?;

    let mut completed_task = durable_task_with(
        "writing-task",
        "dispatch.runtime",
        "completed",
        now,
        "printf secret",
        temp.path(),
    );
    completed_task["agent_mode"] = json!("writing");
    completed_task["result_artifact_path"] = json!(result_path.to_str().expect("utf-8 path"));
    let failed_task = durable_task_with(
        "unscoped-task",
        "dispatch.runtime",
        "failed",
        now,
        "printf secret",
        temp.path(),
    );
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([completed_task, failed_task]))?,
    )?;

    fs::write(
        profile_dir.join("task_resume_state.json"),
        serde_json::to_string_pretty(&json!([resume_state(now)]))?,
    )?;
    fs::write(
        profile_dir.join("provider_capacity.json"),
        serde_json::to_string_pretty(&json!([
            {
                "provider_id": "openai",
                "model": "gpt-test",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": now + Duration::minutes(5),
                "last_error_summary": "rate limited",
                "updated_at": now
            }
        ]))?,
    )?;
    let background_runs_json = serde_json::to_string_pretty(&json!([
        {
            "ticket_id": "ticket",
            "runner_kind": "local_background",
            "phase": "launched",
            "runtime_handle_alive": false
        }
    ]))?;
    fs::write(
        profile_dir.join("background_runs.json"),
        &background_runs_json,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "maintenance-report", "--json"])
        .output()?;

    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["read_only"], true);
    assert_eq!(report["tasks"]["total"], 2);
    assert_eq!(report["tasks"]["by_status"]["completed"], 1);
    assert_eq!(
        report["tasks"]["mode"]["by_risk"]["operator_review_required"],
        1
    );
    assert_eq!(report["tasks"]["missing_agent_mode"], 1);
    assert_eq!(report["approvals"]["pending"], 1);
    assert_eq!(report["resume_states"]["total"], 1);
    assert_eq!(report["provider_capacity"]["attention"], 1);
    assert_eq!(report["background_runs"]["total"], 1);
    assert_eq!(report["background_runs"]["missing_agent_mode"], 1);
    let action_kinds = report["recommended_actions"]
        .as_array()
        .expect("actions")
        .iter()
        .filter_map(|action| action["kind"].as_str())
        .collect::<Vec<_>>();
    assert!(action_kinds.contains(&"pending_approval"));
    assert!(action_kinds.contains(&"operator_review"));
    assert!(action_kinds.contains(&"missing_agent_mode"));
    assert!(action_kinds.contains(&"provider_capacity"));
    let next_action_kinds = report["next_safe_actions"]
        .as_array()
        .expect("next safe actions")
        .iter()
        .filter_map(|action| action["kind"].as_str())
        .collect::<Vec<_>>();
    assert!(next_action_kinds.contains(&"approval_pending"));
    assert!(next_action_kinds.contains(&"review_required"));
    assert!(next_action_kinds.contains(&"provider_attention"));
    assert!(report["next_safe_actions"]
        .as_array()
        .expect("next safe actions")
        .iter()
        .all(|action| action["requires_operator_review"] == true));
    assert_eq!(
        fs::read_to_string(profile_dir.join("background_runs.json"))?,
        background_runs_json
    );
    assert!(!profile_dir.join("action_audit.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_maintenance_request_creates_deduped_approval_without_execution() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let secret = "sk-secretsecretsecretsecret";

    let first = forager_command(temp.path())
        .args([
            "offdesk",
            "maintenance-request",
            "--kind",
            "artifact-cleanup",
            "--project-key",
            "project",
            "--request-id",
            "nightly-review",
            "--target-id",
            "debug/bundles",
            "--preview",
            &format!("Remove old debug bundles token={secret}"),
            "--reason",
            "operator wants to clean reviewed diagnostics",
            "--json",
        ])
        .output()?;
    assert!(first.status.success());
    let first_report: serde_json::Value = serde_json::from_slice(&first.stdout)?;
    assert_eq!(first_report["status"], "pending_approval");
    assert_eq!(first_report["action"], "maintenance.artifact_cleanup");
    assert_eq!(first_report["risk_level"], "destructive");
    assert_eq!(
        first_report["task_id"],
        "maintenance-artifact-cleanup-debug-bundles"
    );
    assert_eq!(first_report["approval"]["status"], "pending");
    assert!(!first_report["approval"]["preview"]
        .as_str()
        .expect("preview")
        .contains(secret));
    let approval_id = first_report["approval"]["approval_id"]
        .as_str()
        .expect("approval id")
        .to_string();

    let second = forager_command(temp.path())
        .args([
            "offdesk",
            "maintenance-request",
            "--kind",
            "artifact-cleanup",
            "--project-key",
            "project",
            "--request-id",
            "nightly-review",
            "--target-id",
            "debug/bundles",
            "--preview",
            "Remove old debug bundles",
            "--reason",
            "operator wants to clean reviewed diagnostics",
            "--json",
        ])
        .output()?;
    assert!(second.status.success());
    let second_report: serde_json::Value = serde_json::from_slice(&second.stdout)?;
    assert_eq!(
        second_report["approval"]["approval_id"]
            .as_str()
            .expect("approval id"),
        approval_id
    );

    let approvals: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(approvals.as_array().expect("approvals").len(), 1);
    assert_eq!(approvals[0]["action"], "maintenance.artifact_cleanup");
    assert_eq!(approvals[0]["risk_level"], "destructive");
    assert_eq!(
        approvals[0]["task_id"],
        "maintenance-artifact-cleanup-debug-bundles"
    );
    assert!(!profile_dir.join("offdesk_tasks.json").exists());
    assert!(!profile_dir.join("background_runs.json").exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_debug_bundle_includes_wiki_attention_summaries_read_only() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_bundle_expired",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Expired bundle review",
                    "ai_instruction": "Expired bundle guidance.",
                    "human_summary": "Expired review entry.",
                    "evidence_refs": ["audit:expired"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "review_after": now - Duration::days(1)
                },
                {
                    "id": "wiki_bundle_near",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Near bundle review",
                    "ai_instruction": "Near bundle guidance.",
                    "human_summary": "Near review entry.",
                    "evidence_refs": ["audit:near"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "review_after": now + Duration::hours(12)
                },
                {
                    "id": "wiki_bundle_missing",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Missing bundle review",
                    "ai_instruction": "Missing review_after guidance.",
                    "human_summary": "Missing review entry.",
                    "evidence_refs": ["audit:missing"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now
                }
            ]
        }))?,
    )?;

    let ack_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "ack-runtime-policy",
            "--artifact-kind",
            "report",
            "--ttl-hours",
            "1",
            "--reason",
            "bundle attention summary seed",
            "--json",
        ])
        .output()?;
    assert!(ack_output.status.success());
    let near_ack: serde_json::Value = serde_json::from_slice(&ack_output.stdout)?;
    let mut expired_ack = near_ack.clone();
    expired_ack["id"] = json!("wiki_runtime_policy_ack_expired_for_bundle");
    expired_ack["created_at"] = serde_json::to_value(now - Duration::hours(2))?;
    expired_ack["expires_at"] = serde_json::to_value(now - Duration::hours(1))?;
    fs::write(
        profile_dir.join("adaptive_wiki_runtime_policy_acknowledgements.jsonl"),
        format!(
            "{}\n{}\n",
            serde_json::to_string(&near_ack)?,
            serde_json::to_string(&expired_ack)?
        ),
    )?;

    let before_entries = fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?;
    let before_acks = fs::read_to_string(
        profile_dir.join("adaptive_wiki_runtime_policy_acknowledgements.jsonl"),
    )?;
    let bundle_output = forager_command(temp.path())
        .args(["offdesk", "debug-bundle", "--json"])
        .output()?;
    assert!(bundle_output.status.success());
    let bundle: serde_json::Value = serde_json::from_slice(&bundle_output.stdout)?;
    assert_eq!(
        bundle["adaptive_wiki_review_after_attention_summary"]["expired"],
        1
    );
    assert_eq!(
        bundle["adaptive_wiki_review_after_attention_summary"]["near_expiry"],
        1
    );
    assert_eq!(
        bundle["adaptive_wiki_review_after_attention_summary"]["missing_review_after"],
        1
    );
    assert_eq!(
        bundle["adaptive_wiki_review_after_attention_summary"]["attention"],
        2
    );
    assert_eq!(
        bundle["adaptive_wiki_runtime_policy_ack_attention_summary"]["total"],
        2
    );
    assert_eq!(
        bundle["adaptive_wiki_runtime_policy_ack_attention_summary"]["expired"],
        1
    );
    assert_eq!(
        bundle["adaptive_wiki_runtime_policy_ack_attention_summary"]["near_expiry"],
        1
    );
    assert_eq!(
        bundle["adaptive_wiki_runtime_policy_ack_attention_summary"]["suggested_actions"],
        2
    );
    assert_eq!(
        fs::read_to_string(profile_dir.join("adaptive_wiki_entries.json"))?,
        before_entries
    );
    assert_eq!(
        fs::read_to_string(
            profile_dir.join("adaptive_wiki_runtime_policy_acknowledgements.jsonl")
        )?,
        before_acks
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_debug_bundle_export_writes_sanitized_profile_artifact() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    let runner_context =
        "<!-- FORAGER:RUNNER_CONTEXT_BEGIN -->runner-only hidden<!-- FORAGER:RUNNER_CONTEXT_END -->";
    fs::write(
        profile_dir.join("pending_action_approvals.json"),
        serde_json::to_string_pretty(&json!([
            {
                "approval_id": "approval_export",
                "status": "pending",
                "scope": "once",
                "project_key": "project",
                "request_id": "request",
                "task_id": "task",
                "action": "dispatch.runtime",
                "risk_level": "runtime_mutation",
                "approval_mode": "operator_required",
                "preview": format!("preview token={secret} {runner_context}"),
                "reason": "export test",
                "created_at": now,
                "expires_at": now + Duration::minutes(10),
                "source_surface": "test"
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "debug-bundle", "--export"])
        .output()?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("exported_to:"));
    assert!(stdout.contains("bytes_written:"));
    assert!(!stdout.contains(secret));
    assert!(!stdout.contains("runner-only hidden"));

    let export_dir = profile_dir.join("debug_bundles");
    let mut exports = fs::read_dir(&export_dir)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    exports.sort();
    assert_eq!(exports.len(), 1);
    let export_name = exports[0]
        .file_name()
        .expect("export file name")
        .to_string_lossy();
    assert!(export_name.starts_with("offdesk_debug_bundle_"));
    assert!(export_name.ends_with(".json"));
    let exported = fs::read_to_string(&exports[0])?;
    assert!(!exported.contains(secret));
    assert!(!exported.contains("runner-only hidden"));
    let bundle: serde_json::Value = serde_json::from_str(&exported)?;
    assert_eq!(bundle["read_only"], true);
    assert_eq!(bundle["redaction_applied"], true);
    assert!(
        bundle["redaction_summary"]["secrets_redacted"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );

    let second_output = forager_command(temp.path())
        .args(["offdesk", "debug-bundle", "--export"])
        .output()?;
    assert!(second_output.status.success());
    let exports_after_second = fs::read_dir(&export_dir)?.count();
    assert_eq!(exports_after_second, 2);

    let stored_approvals = fs::read_to_string(profile_dir.join("pending_action_approvals.json"))?;
    assert!(stored_approvals.contains(secret));
    Ok(())
}

#[test]
#[serial]
fn offdesk_debug_bundle_output_json_receipt_writes_custom_file() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    fs::write(
        profile_dir.join("pending_action_approvals.json"),
        serde_json::to_string_pretty(&json!([
            {
                "approval_id": "approval_custom_export",
                "status": "pending",
                "scope": "once",
                "project_key": "project",
                "request_id": "request",
                "task_id": "task",
                "action": "dispatch.runtime",
                "risk_level": "runtime_mutation",
                "approval_mode": "operator_required",
                "preview": format!("preview token={secret}"),
                "reason": "custom export test",
                "created_at": now,
                "expires_at": now + Duration::minutes(10),
                "source_surface": "test"
            }
        ]))?,
    )?;
    let output_path = temp.path().join("exports").join("custom-bundle.json");

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "debug-bundle",
            "--output",
            output_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(!stdout.contains(secret));
    let receipt: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(
        receipt["exported_to"],
        output_path.to_str().expect("utf-8 path")
    );
    assert_eq!(receipt["bundle"]["read_only"], true);
    assert_eq!(
        receipt["bytes_written"].as_u64().expect("bytes written"),
        fs::metadata(&output_path)?.len()
    );
    let exported_bundle: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&output_path)?)?;
    assert_eq!(exported_bundle, receipt["bundle"]);
    assert!(!fs::read_to_string(&output_path)?.contains(secret));

    let overwrite_output = forager_command(temp.path())
        .args([
            "offdesk",
            "debug-bundle",
            "--output",
            output_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(!overwrite_output.status.success());
    let stderr = String::from_utf8_lossy(&overwrite_output.stderr);
    assert!(stderr.contains("write debug bundle export"));
    assert!(stderr.contains("exists") || stderr.contains("AlreadyExists"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_debug_bundle_empty_profile_does_not_create_storage() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .args(["offdesk", "debug-bundle", "--json"])
        .output()?;

    assert!(output.status.success());
    let bundle: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(bundle["read_only"], true);
    assert_eq!(bundle["approvals"].as_array().expect("approvals").len(), 0);
    assert_eq!(bundle["tasks"].as_array().expect("tasks").len(), 0);
    assert!(!app_dir(temp.path()).exists());
    Ok(())
}

#[test]
#[serial]
fn forager_binary_uses_forager_storage_layout() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;

    assert!(output.status.success());
    assert!(profile_dir(temp.path()).exists());
    Ok(())
}

#[test]
#[serial]
fn forager_binary_falls_back_to_existing_legacy_storage_layout() -> Result<()> {
    let temp = tempdir()?;
    fs::create_dir_all(legacy_profile_dir_for(temp.path(), "default"))?;

    let output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;

    assert!(output.status.success());
    assert!(legacy_profile_dir_for(temp.path(), "default").exists());
    assert!(!profile_dir(temp.path()).exists());
    Ok(())
}

#[test]
#[serial]
fn forager_profile_env_takes_precedence_over_legacy_env() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .env("FORAGER_PROFILE", "new-name")
        .env("AGENT_OF_EMPIRES_PROFILE", "old-name")
        .args(["status", "--json"])
        .output()?;

    assert!(output.status.success());
    assert!(profile_dir_for(temp.path(), "new-name").exists());
    assert!(!profile_dir_for(temp.path(), "old-name").exists());
    Ok(())
}

#[test]
#[serial]
fn legacy_profile_env_still_works() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .env("AGENT_OF_EMPIRES_PROFILE", "legacy-name")
        .args(["status", "--json"])
        .output()?;

    assert!(output.status.success());
    assert!(profile_dir_for(temp.path(), "legacy-name").exists());
    Ok(())
}

#[test]
#[serial]
fn configured_default_profile_is_used_when_no_profile_is_provided() -> Result<()> {
    let temp = tempdir()?;

    let create_output = forager_command(temp.path())
        .args(["profile", "create", "work"])
        .output()?;
    assert!(create_output.status.success());

    let default_output = forager_command(temp.path())
        .args(["profile", "default", "work"])
        .output()?;
    assert!(default_output.status.success());

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_output.status.success());

    assert!(profile_dir_for(temp.path(), "work").exists());
    assert!(!profile_dir(temp.path()).exists());
    Ok(())
}

#[test]
#[serial]
fn profile_argument_rejects_path_traversal() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .args(["-p", "../../outside", "list"])
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Profile name cannot contain path separators"));
    assert!(!temp.path().join(".config").join("outside").exists());
    Ok(())
}

#[test]
#[serial]
fn forager_init_creates_forager_repo_config() -> Result<()> {
    let home = tempdir()?;
    let repo = tempdir()?;

    let output = forager_command(home.path())
        .args(["init", repo.path().to_str().expect("utf-8 path")])
        .output()?;

    assert!(output.status.success());
    assert!(repo.path().join(".forager").join("config.toml").exists());
    assert!(!repo.path().join(".aoe").join("config.toml").exists());
    Ok(())
}

#[test]
#[serial]
fn forager_init_refuses_existing_legacy_repo_config() -> Result<()> {
    let home = tempdir()?;
    let repo = tempdir()?;
    let legacy_dir = repo.path().join(".aoe");
    fs::create_dir_all(&legacy_dir)?;
    fs::write(legacy_dir.join("config.toml"), "# legacy\n")?;

    let output = forager_command(home.path())
        .args(["init", repo.path().to_str().expect("utf-8 path")])
        .output()?;

    assert!(!output.status.success());
    assert!(!repo.path().join(".forager").join("config.toml").exists());
    Ok(())
}

#[test]
#[serial]
fn forager_add_rejects_deferred_sandbox_flags() -> Result<()> {
    let home = tempdir()?;
    let repo = tempdir()?;
    let deferred = "Docker sandbox support is deferred while Forager decides whether to benchmark";

    let sandbox_output = forager_command(home.path())
        .current_dir(repo.path())
        .args(["add", "--sandbox", "."])
        .output()?;
    assert!(!sandbox_output.status.success());
    assert!(
        String::from_utf8_lossy(&sandbox_output.stderr).contains(deferred),
        "stderr: {}",
        String::from_utf8_lossy(&sandbox_output.stderr)
    );

    let image_output = forager_command(home.path())
        .current_dir(repo.path())
        .args(["add", "--sandbox-image", "custom/image:latest", "."])
        .output()?;
    assert!(!image_output.status.success());
    assert!(
        String::from_utf8_lossy(&image_output.stderr).contains(deferred),
        "stderr: {}",
        String::from_utf8_lossy(&image_output.stderr)
    );

    Ok(())
}

#[test]
#[serial]
fn forager_doctor_reports_new_primary_paths_without_creating_storage() -> Result<()> {
    let home = tempdir()?;
    let repo = tempdir()?;

    let output = forager_command(home.path())
        .args([
            "doctor",
            "--json",
            "--project",
            repo.path().to_str().expect("utf-8 path"),
        ])
        .output()?;

    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["profile"]["active"], "default");
    assert_eq!(report["profile"]["dir_source"], "new_primary");
    assert_eq!(
        reported_path(&report["profile"]["dir"]),
        expected_path(&app_dir(home.path()).join("profiles").join("default"))
    );
    assert_eq!(
        reported_path(&report["profile"]["primary_dir"]),
        expected_path(&app_dir(home.path()).join("profiles").join("default"))
    );
    assert_eq!(report["profile"]["primary_exists"], false);
    assert_eq!(report["global_data"]["active_source"], "new_primary");
    assert_eq!(
        reported_path(&report["global_data"]["primary_path"]),
        expected_path(&app_dir(home.path()))
    );
    assert!(!app_dir(home.path()).exists());
    assert_eq!(report["repo_config"]["active_source"], "none");
    assert_eq!(
        reported_path(&report["repo_config"]["primary_path"]),
        expected_path(&repo.path().join(".forager").join("config.toml"))
    );
    Ok(())
}

#[test]
#[serial]
fn forager_doctor_reports_legacy_storage_and_repo_config() -> Result<()> {
    let home = tempdir()?;
    let repo = tempdir()?;
    fs::create_dir_all(legacy_app_dir(home.path()).join("profiles").join("default"))?;
    let legacy_repo_dir = repo.path().join(".aoe");
    fs::create_dir_all(&legacy_repo_dir)?;
    fs::write(legacy_repo_dir.join("config.toml"), "# legacy\n")?;

    let output = forager_command(home.path())
        .args([
            "doctor",
            "--json",
            "--project",
            repo.path().to_str().expect("utf-8 path"),
        ])
        .output()?;

    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["profile"]["active"], "default");
    assert_eq!(report["profile"]["dir_source"], "legacy");
    assert_eq!(
        reported_path(&report["profile"]["dir"]),
        expected_path(&legacy_app_dir(home.path()).join("profiles").join("default"))
    );
    assert_eq!(
        reported_path(&report["profile"]["primary_dir"]),
        expected_path(&app_dir(home.path()).join("profiles").join("default"))
    );
    assert_eq!(report["profile"]["primary_exists"], false);
    assert_eq!(report["global_data"]["active_source"], "legacy");
    assert_eq!(
        reported_path(&report["global_data"]["active_path"]),
        expected_path(&legacy_app_dir(home.path()))
    );
    assert_eq!(report["repo_config"]["active_source"], "legacy");
    assert_eq!(
        reported_path(&report["repo_config"]["active_path"]),
        expected_path(&repo.path().join(".aoe").join("config.toml"))
    );
    assert!(!app_dir(home.path()).exists());
    Ok(())
}

#[test]
#[serial]
fn forager_doctor_reports_profile_env_precedence() -> Result<()> {
    let home = tempdir()?;
    let repo = tempdir()?;

    let output = forager_command(home.path())
        .env("FORAGER_PROFILE", "new-name")
        .env("AGENT_OF_EMPIRES_PROFILE", "old-name")
        .args([
            "doctor",
            "--json",
            "--project",
            repo.path().to_str().expect("utf-8 path"),
        ])
        .output()?;

    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["profile"]["active"], "new-name");
    assert_eq!(report["profile"]["source"], "--profile/FORAGER_PROFILE");
    assert_eq!(report["profile"]["dir_source"], "new_primary");
    assert_eq!(report["env"]["forager_profile_set"], true);
    assert_eq!(report["env"]["legacy_profile_set"], true);
    Ok(())
}

#[test]
#[serial]
fn forager_migrate_aoe_copies_legacy_paths_and_preserves_sources() -> Result<()> {
    let home = tempdir()?;
    let repo = tempdir()?;
    let legacy_dir = legacy_app_dir(home.path());
    fs::create_dir_all(legacy_dir.join("profiles").join("default"))?;
    fs::write(legacy_dir.join("config.toml"), "legacy global")?;
    fs::write(
        legacy_dir
            .join("profiles")
            .join("default")
            .join("sessions.json"),
        "[]",
    )?;
    let legacy_repo_dir = repo.path().join(".aoe");
    fs::create_dir_all(&legacy_repo_dir)?;
    fs::write(legacy_repo_dir.join("config.toml"), "# legacy repo\n")?;

    let output = forager_command(home.path())
        .args([
            "migrate",
            "aoe",
            "--json",
            "--project",
            repo.path().to_str().expect("utf-8 path"),
        ])
        .output()?;

    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["migration"], "aoe");
    assert_eq!(report["mode"], "copy");
    assert_eq!(report["has_conflicts"], false);
    assert!(report["operations"]
        .as_array()
        .expect("operations")
        .iter()
        .all(|operation| operation["status"] == "copied"));
    assert_eq!(
        fs::read_to_string(app_dir(home.path()).join("config.toml"))?,
        "legacy global"
    );
    assert_eq!(
        fs::read_to_string(
            app_dir(home.path())
                .join("profiles")
                .join("default")
                .join("sessions.json")
        )?,
        "[]"
    );
    assert_eq!(
        fs::read_to_string(repo.path().join(".forager").join("config.toml"))?,
        "# legacy repo\n"
    );
    assert!(legacy_dir.join("config.toml").exists());
    assert!(legacy_repo_dir.join("config.toml").exists());
    Ok(())
}

#[test]
#[serial]
fn forager_migrate_aoe_dry_run_does_not_copy() -> Result<()> {
    let home = tempdir()?;
    let repo = tempdir()?;
    fs::create_dir_all(legacy_app_dir(home.path()).join("profiles").join("default"))?;
    let legacy_repo_dir = repo.path().join(".aoe");
    fs::create_dir_all(&legacy_repo_dir)?;
    fs::write(legacy_repo_dir.join("config.toml"), "# legacy repo\n")?;

    let output = forager_command(home.path())
        .args([
            "migrate",
            "aoe",
            "--dry-run",
            "--json",
            "--project",
            repo.path().to_str().expect("utf-8 path"),
        ])
        .output()?;

    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["dry_run"], true);
    assert_eq!(report["has_conflicts"], false);
    assert!(report["operations"]
        .as_array()
        .expect("operations")
        .iter()
        .all(|operation| operation["status"] == "would_copy"));
    assert!(!app_dir(home.path()).exists());
    assert!(!repo.path().join(".forager").join("config.toml").exists());
    Ok(())
}

#[test]
#[serial]
fn forager_migrate_aoe_refuses_conflicts_before_copying_anything() -> Result<()> {
    let home = tempdir()?;
    let repo = tempdir()?;
    fs::create_dir_all(legacy_app_dir(home.path()).join("profiles").join("default"))?;
    fs::create_dir_all(app_dir(home.path()))?;
    fs::write(app_dir(home.path()).join("config.toml"), "primary global")?;
    let legacy_repo_dir = repo.path().join(".aoe");
    fs::create_dir_all(&legacy_repo_dir)?;
    fs::write(legacy_repo_dir.join("config.toml"), "# legacy repo\n")?;

    let output = forager_command(home.path())
        .args([
            "migrate",
            "aoe",
            "--json",
            "--project",
            repo.path().to_str().expect("utf-8 path"),
        ])
        .output()?;

    assert!(!output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["has_conflicts"], true);
    let operations = report["operations"].as_array().expect("operations");
    assert!(operations.iter().any(|operation| {
        operation["scope"] == "global_data"
            && operation["status"] == "conflict"
            && operation["reason"] == "target_exists"
    }));
    assert!(operations.iter().any(|operation| {
        operation["scope"] == "repo_config"
            && operation["status"] == "blocked"
            && operation["reason"] == "conflict_in_plan"
    }));
    assert_eq!(
        fs::read_to_string(app_dir(home.path()).join("config.toml"))?,
        "primary global"
    );
    assert!(!repo.path().join(".forager").join("config.toml").exists());
    Ok(())
}

#[test]
#[serial]
fn aoe_human_commands_warn_that_alias_is_legacy() -> Result<()> {
    let temp = tempdir()?;

    let output = legacy_aoe_command(temp.path()).args(["status"]).output()?;

    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("`aoe` is a legacy alias; use `forager` instead."));
    Ok(())
}

#[test]
#[serial]
fn aoe_script_safe_commands_do_not_emit_deprecation_warning() -> Result<()> {
    let temp = tempdir()?;

    for args in [
        vec!["status", "--json"],
        vec!["status", "-q"],
        vec!["tmux", "status"],
        vec!["completion", "bash"],
    ] {
        let output = legacy_aoe_command(temp.path()).args(args).output()?;
        assert!(output.status.success());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stderr.contains("legacy alias"),
            "unexpected warning for stderr: {stderr}"
        );
    }

    Ok(())
}

#[test]
#[serial]
fn offdesk_tick_launches_briefed_task_and_completes_from_sidecar() -> Result<()> {
    let temp = tempdir()?;
    let brief_path = temp.path().join("brief.json");
    let result_path = temp.path().join("tick-result.txt");
    let now = Utc::now();
    fs::write(
        &brief_path,
        serde_json::to_string_pretty(&json!({
            "request_id": "request",
            "task_id": "task",
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["dispatch.runtime"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(10)
        }))?,
    )?;
    let command = format!("printf done > {}", result_path.display());

    let enqueue_output = forager_command(temp.path())
        .args([
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--brief",
            brief_path.to_str().expect("utf-8 path"),
            "--cmd",
            command.as_str(),
            "--workdir",
            temp.path().to_str().expect("utf-8 path"),
            "--result-artifact",
            result_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(enqueue_output.status.success());

    let tick_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(tick_output.status.success());
    let tick: serde_json::Value = serde_json::from_slice(&tick_output.stdout)?;
    assert_eq!(tick["launched"], 1);

    wait_for_path(&result_path);
    let complete_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(complete_output.status.success());
    let complete: serde_json::Value = serde_json::from_slice(&complete_output.stdout)?;
    assert_eq!(complete["completed"], 1);
    assert!(complete["next_safe_actions"]
        .as_array()
        .expect("next safe actions")
        .iter()
        .any(|action| action["kind"] == "review_required"));
    assert!(!complete["next_safe_actions"]
        .as_array()
        .expect("next safe actions")
        .iter()
        .any(|action| action["kind"] == "runtime_monitoring"));

    let tasks: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir(temp.path()).join("offdesk_tasks.json"),
    )?)?;
    assert_eq!(tasks[0]["status"], "completed");
    Ok(())
}

#[test]
#[serial]
fn offdesk_tick_project_task_filter_dispatches_only_matching_task() -> Result<()> {
    let temp = tempdir()?;
    let brief_path = temp.path().join("target-brief.json");
    let target_result_path = temp.path().join("target-result.txt");
    let other_result_path = temp.path().join("other-result.txt");
    let now = Utc::now();
    fs::write(
        &brief_path,
        serde_json::to_string_pretty(&json!({
            "request_id": "target-request",
            "task_id": "target-task",
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["dispatch.runtime"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(10)
        }))?,
    )?;
    let target_command = format!("printf target > {}", target_result_path.display());
    let other_command = format!("printf other > {}", other_result_path.display());

    let target_enqueue = forager_command(temp.path())
        .args([
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "target-request",
            "--task-id",
            "target-task",
            "--brief",
            brief_path.to_str().expect("utf-8 path"),
            "--cmd",
            target_command.as_str(),
            "--workdir",
            temp.path().to_str().expect("utf-8 path"),
            "--result-artifact",
            target_result_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(target_enqueue.status.success());

    let other_enqueue = forager_command(temp.path())
        .args([
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "other-request",
            "--task-id",
            "other-task",
            "--cmd",
            other_command.as_str(),
            "--workdir",
            temp.path().to_str().expect("utf-8 path"),
            "--result-artifact",
            other_result_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(other_enqueue.status.success());

    let tick_output = forager_command(temp.path())
        .args([
            "offdesk",
            "tick",
            "--project-key",
            "project",
            "--task-id",
            "target-task",
            "--limit",
            "1",
            "--json",
        ])
        .output()?;
    assert!(
        tick_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&tick_output.stdout),
        String::from_utf8_lossy(&tick_output.stderr)
    );
    let tick: serde_json::Value = serde_json::from_slice(&tick_output.stdout)?;
    assert_eq!(tick["launched"], 1);
    assert_eq!(tick["pending_approval"], 0);
    assert_eq!(tick["updated_task_ids"], json!(["target-task"]));

    let tasks: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir(temp.path()).join("offdesk_tasks.json"),
    )?)?;
    let target = tasks
        .as_array()
        .expect("tasks")
        .iter()
        .find(|task| task["task_id"] == "target-task")
        .expect("target task");
    let other = tasks
        .as_array()
        .expect("tasks")
        .iter()
        .find(|task| task["task_id"] == "other-task")
        .expect("other task");
    assert_eq!(target["status"], "launched");
    assert_eq!(other["status"], "queued");
    assert!(target["background_ticket_id"].is_string());
    assert!(other["background_ticket_id"].is_null());
    assert!(
        !profile_dir(temp.path())
            .join("pending_action_approvals.json")
            .exists(),
        "scoped tick must not create approval for the non-target task"
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_tick_emits_runner_work_slice_receipts_for_packet() -> Result<()> {
    let temp = tempdir()?;
    write_implementation_packet_fixture(temp.path(), "project", "packet-runner-receipts")?;
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

    let brief_path = temp.path().join("brief.json");
    let result_path = temp.path().join("tick-packet-result.txt");
    let now = Utc::now();
    fs::write(
        &brief_path,
        serde_json::to_string_pretty(&json!({
            "request_id": "request",
            "task_id": "task",
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["dispatch.runtime"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(10)
        }))?,
    )?;
    let command = format!("printf done > {}", result_path.display());

    let enqueue_output = forager_command(temp.path())
        .args([
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--brief",
            brief_path.to_str().expect("utf-8 path"),
            "--cmd",
            command.as_str(),
            "--workdir",
            temp.path().to_str().expect("utf-8 path"),
            "--result-artifact",
            result_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(enqueue_output.status.success());

    let launch_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(launch_output.status.success());
    wait_for_path(&result_path);

    let complete_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(complete_output.status.success());
    let complete: serde_json::Value = serde_json::from_slice(&complete_output.stdout)?;
    assert_eq!(complete["completed"], 1);

    let receipt_path = temp.path().join("work_slice_receipts.jsonl");
    let receipt_jsonl = fs::read_to_string(&receipt_path)?;
    let receipts = receipt_jsonl
        .lines()
        .map(serde_json::from_str::<serde_json::Value>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    assert_eq!(receipts.len(), 2);
    assert!(receipts.iter().all(|receipt| {
        receipt["schema"] == "work_slice_execution_receipt.v1"
            && receipt["packet_id"] == "packet-runner-receipts"
            && receipt["producer"] == "runner_poll"
            && receipt["status"] == "deferred"
            && receipt["next_safe_action"]
                .as_str()
                .unwrap_or_default()
                .contains("before accepting truth")
    }));

    let repeat_tick = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(repeat_tick.status.success());
    assert_eq!(fs::read_to_string(&receipt_path)?.lines().count(), 2);

    let closeout_output = forager_command(temp.path())
        .args([
            "offdesk",
            "closeout",
            "--project-key",
            "project",
            "--task-id",
            "task",
            "--dry-run",
            "--json",
        ])
        .output()?;
    assert!(
        closeout_output.status.success(),
        "{}",
        String::from_utf8_lossy(&closeout_output.stderr)
    );
    let closeout: serde_json::Value = serde_json::from_slice(&closeout_output.stdout)?;
    assert_eq!(closeout["summary"]["packet_goals_completed"], 1);
    assert_eq!(closeout["summary"]["packet_detail_items_deferred"], 2);
    assert_eq!(
        closeout["implementation_packet_coverage"]["items"][0]["detail_source"],
        "implementation_packet_and_work_slice_receipts"
    );
    assert!(
        closeout["implementation_packet_coverage"]["items"][0]["work_slices"]
            .as_array()
            .expect("work slices")
            .iter()
            .all(|item| item["status"] == "deferred"
                && item["receipt_source"]
                    .as_str()
                    .unwrap_or_default()
                    .ends_with("work_slice_receipts.jsonl"))
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_tick_injects_adaptive_wiki_runtime_context_and_records_usage() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let brief_path = temp.path().join("brief.json");
    let result_path = temp.path().join("wiki-runtime-result.txt");
    let now = Utc::now();
    let secret = "sk-secretsecretsecretsecret";
    fs::write(
        &brief_path,
        serde_json::to_string_pretty(&json!({
            "request_id": "request",
            "task_id": "task",
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["dispatch.runtime"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(10)
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_runtime_entry",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Runtime should see report guidance",
                    "ai_instruction": format!("Mention report evidence boundaries token={secret}"),
                    "human_summary": "Human-only wiki note",
                    "evidence_refs": ["task:one"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now
                }
            ]
        }))?,
    )?;
    let command = format!("printf done > {}", result_path.display());

    let enqueue_output = forager_command(temp.path())
        .args([
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--brief",
            brief_path.to_str().expect("utf-8 path"),
            "--artifact-kind",
            "report",
            "--cmd",
            command.as_str(),
            "--workdir",
            temp.path().to_str().expect("utf-8 path"),
            "--result-artifact",
            result_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(enqueue_output.status.success());

    let tick_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(tick_output.status.success());
    let tick: serde_json::Value = serde_json::from_slice(&tick_output.stdout)?;
    assert_eq!(tick["launched"], 1);

    let runs: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("background_runs.json"),
    )?)?;
    assert_eq!(
        runs[0]["adaptive_wiki_entry_ids"],
        json!(["wiki_runtime_entry"])
    );
    assert_eq!(
        runs[0]["adaptive_wiki_runtime_policy"]["review_expired"],
        "warn"
    );
    let context = runs[0]["adaptive_wiki_context"]
        .as_str()
        .expect("runtime wiki context");
    assert!(context.contains("<adaptive-wiki-context>"));
    assert!(context.contains("wiki_runtime_entry"));
    assert!(context.contains("[REDACTED]"));
    assert!(!context.contains(secret));
    assert_eq!(runs[0]["launch_spec_summary"], command);
    assert_eq!(
        runs[0]["working_dir"],
        temp.path().to_str().expect("utf-8 path")
    );

    let tasks: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(profile_dir.join("offdesk_tasks.json"))?)?;
    assert_eq!(tasks[0]["command"], command);
    assert_eq!(
        tasks[0]["workdir"],
        temp.path().to_str().expect("utf-8 path")
    );
    assert_eq!(
        tasks[0]["last_adaptive_wiki_entry_ids"],
        json!(["wiki_runtime_entry"])
    );

    let usage = fs::read_to_string(profile_dir.join("adaptive_wiki_usage.jsonl"))?;
    assert_eq!(usage.lines().count(), 1);
    let usage_record: serde_json::Value = serde_json::from_str(usage.lines().next().unwrap())?;
    assert_eq!(usage_record["entry_id"], "wiki_runtime_entry");
    assert_eq!(usage_record["task_id"], "task");
    assert_eq!(usage_record["request_id"], "request");
    assert_eq!(usage_record["project_key"], "project");
    assert_eq!(usage_record["artifact_kind"], "report");
    assert_eq!(usage_record["projection_kind"], "runtime_probe");
    assert_eq!(usage_record["projection_policy"]["review_expired"], "warn");
    Ok(())
}

#[test]
#[serial]
fn offdesk_launch_runtime_wiki_kill_switch_keeps_preflight_projection() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let brief_path = temp.path().join("brief.json");
    let now = Utc::now();
    fs::write(
        &brief_path,
        serde_json::to_string_pretty(&json!({
            "request_id": "request",
            "task_id": "task",
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["background.launch"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(10)
        }))?,
    )?;
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_disabled_runtime_entry",
                    "kind": "procedure",
                    "scope": "project",
                    "scope_ref": "project",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Visible in preflight only",
                    "ai_instruction": "Use only as preflight metadata.",
                    "human_summary": "Human-only note",
                    "evidence_refs": ["task:one"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now
                }
            ]
        }))?,
    )?;

    let output = forager_command(temp.path())
        .env("FORAGER_ADAPTIVE_WIKI_RUNTIME", "0")
        .args([
            "offdesk",
            "launch",
            "background.launch",
            "--runner",
            "remote-worker",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--ticket-id",
            "ticket",
            "--brief",
            brief_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(output.status.success());
    let outcome: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(outcome["gate"]["status"], "proceed");
    assert_eq!(
        outcome["gate"]["adaptive_wiki"][0]["id"],
        "wiki_disabled_runtime_entry"
    );
    assert_eq!(
        outcome["gate"]["adaptive_wiki_runtime"][0]["id"],
        "wiki_disabled_runtime_entry"
    );
    assert_eq!(
        outcome["gate"]["adaptive_wiki_runtime_policy"]["review_expired"],
        "warn"
    );
    assert!(outcome["probe"].get("adaptive_wiki_context").is_none());
    assert!(outcome["probe"].get("adaptive_wiki_entry_ids").is_none());
    assert!(outcome["probe"]
        .get("adaptive_wiki_runtime_policy")
        .is_none());
    assert!(!profile_dir.join("adaptive_wiki_usage.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_strict_runtime_wiki_requires_ack_and_excludes_review_expired() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_runtime_expired",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Expired runtime guidance",
                    "ai_instruction": "Use only after review renewal.",
                    "evidence_refs": ["task:expired"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "review_after": now - Duration::days(1)
                },
                {
                    "id": "wiki_runtime_fresh",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Fresh runtime guidance",
                    "ai_instruction": "Keep report evidence separate.",
                    "evidence_refs": ["task:fresh"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "review_after": now + Duration::days(1)
                }
            ]
        }))?,
    )?;

    let missing_ack_output = forager_command(temp.path())
        .env("FORAGER_ADAPTIVE_WIKI_RUNTIME_REVIEW_EXPIRED", "exclude")
        .args([
            "offdesk",
            "gate",
            "inspect.status",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--artifact-kind",
            "report",
            "--json",
        ])
        .output()?;
    assert!(missing_ack_output.status.success());
    let missing_ack: serde_json::Value = serde_json::from_slice(&missing_ack_output.stdout)?;
    assert_eq!(missing_ack["status"], "proceed");
    let preflight = missing_ack["adaptive_wiki"]
        .as_array()
        .expect("preflight wiki");
    assert!(preflight
        .iter()
        .any(|entry| entry["id"] == "wiki_runtime_expired"));
    assert!(preflight
        .iter()
        .any(|entry| entry["id"] == "wiki_runtime_fresh"));
    assert!(missing_ack["adaptive_wiki_runtime"]
        .as_array()
        .is_none_or(Vec::is_empty));
    assert_eq!(
        missing_ack["adaptive_wiki_runtime_decision"]["status"],
        "strict_requested_missing_acknowledgement"
    );
    assert_eq!(
        missing_ack["adaptive_wiki_runtime_decision"]["requested_policy"]["review_expired"],
        "exclude"
    );

    let ack_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "ack-runtime-policy",
            "--session-id",
            "request",
            "--project-key",
            "project",
            "--artifact-kind",
            "report",
            "--reason",
            "operator reviewed warn-vs-strict comparison",
            "--json",
        ])
        .output()?;
    assert!(ack_output.status.success());
    let ack: serde_json::Value = serde_json::from_slice(&ack_output.stdout)?;
    assert_eq!(ack["policy"]["review_expired"], "exclude");
    assert_eq!(
        ack["review_expired_excluded"],
        json!(["wiki_runtime_expired"])
    );
    assert!(ack["comparison_hash"]
        .as_str()
        .is_some_and(|hash| hash.len() == 64));

    let ack_list_output = forager_command(temp.path())
        .args(["offdesk", "wiki", "runtime-policy-acks", "--json"])
        .output()?;
    assert!(ack_list_output.status.success());
    let ack_list: serde_json::Value = serde_json::from_slice(&ack_list_output.stdout)?;
    assert_eq!(ack_list.as_array().expect("ack list").len(), 1);

    let acknowledged_output = forager_command(temp.path())
        .env("FORAGER_ADAPTIVE_WIKI_RUNTIME_REVIEW_EXPIRED", "exclude")
        .args([
            "offdesk",
            "gate",
            "inspect.status",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--artifact-kind",
            "report",
            "--json",
        ])
        .output()?;
    assert!(acknowledged_output.status.success());
    let acknowledged: serde_json::Value = serde_json::from_slice(&acknowledged_output.stdout)?;
    assert_eq!(
        acknowledged["adaptive_wiki_runtime_decision"]["status"],
        "applied_acknowledged"
    );
    assert_eq!(
        acknowledged["adaptive_wiki_runtime_decision"]["acknowledgement_id"],
        ack["id"]
    );
    let runtime = acknowledged["adaptive_wiki_runtime"]
        .as_array()
        .expect("runtime wiki");
    assert!(runtime
        .iter()
        .any(|entry| entry["id"] == "wiki_runtime_fresh"));
    assert!(!runtime
        .iter()
        .any(|entry| entry["id"] == "wiki_runtime_expired"));
    assert_eq!(
        acknowledged["adaptive_wiki_runtime_policy"]["review_expired"],
        "exclude"
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_renew_review_after_updates_only_review_metadata() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let expired_review_after = now - Duration::days(1);
    let renewed_review_after = now + Duration::days(7);
    let expired_review_after_json = serde_json::to_value(expired_review_after)?;
    let renewed_review_after_json = serde_json::to_value(renewed_review_after)?;
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [{
                "id": "wiki_review_renew",
                "kind": "procedure",
                "scope": "artifact_kind",
                "scope_ref": "report",
                "status": "promoted",
                "activation_mode": "confirm",
                "claim": "Renew review window",
                "ai_instruction": "Keep report review guidance unchanged.",
                "human_summary": "Review renewal target.",
                "evidence_refs": ["audit:renew"],
                "confidence": "explicit",
                "created_at": now,
                "updated_at": now,
                "review_after": expired_review_after
            }]
        }))?,
    )?;

    let renew_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "renew-review-after",
            "wiki_review_renew",
            "--review-after",
            &renewed_review_after.to_rfc3339(),
            "--reason",
            "operator revalidated entry",
            "--json",
        ])
        .output()?;
    assert!(renew_output.status.success());
    let renewed: serde_json::Value = serde_json::from_slice(&renew_output.stdout)?;
    assert_eq!(renewed["action"], "renew_review_after");
    assert_eq!(renewed["entry"]["id"], "wiki_review_renew");
    assert_eq!(
        renewed["entry"]["review_after"],
        renewed_review_after_json.clone()
    );
    assert_eq!(renewed["previous_review_after"], expired_review_after_json);
    assert_eq!(renewed["audit"]["action"], "renew_review_after");

    let stored: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("adaptive_wiki_entries.json"),
    )?)?;
    let entry = &stored["entries"][0];
    assert_eq!(entry["scope"], "artifact_kind");
    assert_eq!(entry["scope_ref"], "report");
    assert_eq!(
        entry["ai_instruction"],
        "Keep report review guidance unchanged."
    );
    assert_eq!(entry["review_after"], renewed_review_after_json);

    let projection_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "projection",
            "--artifact-kind",
            "report",
            "--report",
            "--exclude-review-expired",
            "--json",
        ])
        .output()?;
    assert!(projection_output.status.success());
    let projection: serde_json::Value = serde_json::from_slice(&projection_output.stdout)?;
    assert!(projection["review_expired"]
        .as_array()
        .expect("review_expired")
        .is_empty());
    assert_eq!(projection["selected"][0]["id"], "wiki_review_renew");
    Ok(())
}

#[test]
#[serial]
fn offdesk_wiki_review_after_report_flags_expired_and_near_expiry_entries() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": [
                {
                    "id": "wiki_review_expired",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Expired report review",
                    "ai_instruction": "Expired instruction should not be in attention report.",
                    "human_summary": "Expired review entry.",
                    "evidence_refs": ["audit:expired"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "review_after": now - Duration::days(1)
                },
                {
                    "id": "wiki_review_near",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Near expiry report review",
                    "ai_instruction": "Near expiry instruction should not be in attention report.",
                    "human_summary": "Near expiry review entry.",
                    "evidence_refs": ["audit:near"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "review_after": now + Duration::hours(12)
                },
                {
                    "id": "wiki_review_fresh",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Fresh report review",
                    "ai_instruction": "Fresh review is outside attention window.",
                    "human_summary": "Fresh review entry.",
                    "evidence_refs": ["audit:fresh"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "review_after": now + Duration::days(10)
                },
                {
                    "id": "wiki_review_missing",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Missing review_after",
                    "ai_instruction": "Missing review_after should be counted but not listed.",
                    "human_summary": "Missing review_after entry.",
                    "evidence_refs": ["audit:missing"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now
                },
                {
                    "id": "wiki_review_deprecated",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "report",
                    "status": "deprecated",
                    "activation_mode": "confirm",
                    "claim": "Deprecated expired review",
                    "ai_instruction": "Deprecated entries are ignored.",
                    "human_summary": "Deprecated review entry.",
                    "evidence_refs": ["audit:deprecated"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "review_after": now - Duration::days(1)
                },
                {
                    "id": "wiki_review_other_artifact",
                    "kind": "procedure",
                    "scope": "artifact_kind",
                    "scope_ref": "dataset",
                    "status": "promoted",
                    "activation_mode": "confirm",
                    "claim": "Other artifact review",
                    "ai_instruction": "Other artifact is out of scope.",
                    "human_summary": "Other artifact entry.",
                    "evidence_refs": ["audit:other"],
                    "confidence": "explicit",
                    "created_at": now,
                    "updated_at": now,
                    "review_after": now - Duration::days(1)
                }
            ]
        }))?,
    )?;

    let report_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "review-after-report",
            "--artifact-kind",
            "report",
            "--near-expiry-hours",
            "24",
            "--json",
        ])
        .output()?;
    assert!(report_output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&report_output.stdout)?;
    assert_eq!(report["summary"]["scoped_promoted"], 4);
    assert_eq!(report["summary"]["with_review_after"], 3);
    assert_eq!(report["summary"]["missing_review_after"], 1);
    assert_eq!(report["summary"]["expired"], 1);
    assert_eq!(report["summary"]["near_expiry"], 1);
    assert_eq!(report["summary"]["attention"], 2);
    assert_eq!(report["entries"][0]["id"], "wiki_review_expired");
    assert_eq!(report["entries"][0]["status"], "expired");
    assert_eq!(report["entries"][1]["id"], "wiki_review_near");
    assert_eq!(report["entries"][1]["status"], "near_expiry");
    assert!(report["entries"][0]["renew_command_template"]
        .as_str()
        .is_some_and(|command| command.contains("renew-review-after 'wiki_review_expired'")));
    let report_json = serde_json::to_string(&report)?;
    assert!(!report_json.contains("Expired instruction should not be in attention report"));
    assert!(!report_json.contains("wiki_review_fresh"));
    assert!(!report_json.contains("wiki_review_deprecated"));
    assert!(!report_json.contains("wiki_review_other_artifact"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_project_artifact_runtime_ack_reuses_only_without_session_specific_projection(
) -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let base_entries = json!([
        {
            "id": "wiki_project_expired",
            "kind": "procedure",
            "scope": "artifact_kind",
            "scope_ref": "report",
            "status": "promoted",
            "activation_mode": "confirm",
            "claim": "Expired project/artifact guidance",
            "ai_instruction": "Use only after project review renewal.",
            "evidence_refs": ["task:expired"],
            "confidence": "explicit",
            "created_at": now,
            "updated_at": now,
            "review_after": now - Duration::days(1)
        },
        {
            "id": "wiki_project_fresh",
            "kind": "procedure",
            "scope": "artifact_kind",
            "scope_ref": "report",
            "status": "promoted",
            "activation_mode": "confirm",
            "claim": "Fresh project/artifact guidance",
            "ai_instruction": "Keep report evidence separate.",
            "evidence_refs": ["task:fresh"],
            "confidence": "explicit",
            "created_at": now,
            "updated_at": now,
            "review_after": now + Duration::days(1)
        }
    ]);
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": base_entries
        }))?,
    )?;

    let ack_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "ack-runtime-policy",
            "--scope-mode",
            "project-artifact",
            "--project-key",
            "project",
            "--artifact-kind",
            "report",
            "--reason",
            "operator reviewed project/artifact strict projection",
            "--json",
        ])
        .output()?;
    assert!(ack_output.status.success());
    let ack: serde_json::Value = serde_json::from_slice(&ack_output.stdout)?;
    assert_eq!(ack["scope_mode"], "project_artifact");
    assert_eq!(ack["query"]["session_id"], serde_json::Value::Null);
    assert_eq!(ack["query"]["project_key"], "project");
    assert_eq!(ack["query"]["artifact_kind"], "report");

    let applied_output = forager_command(temp.path())
        .env("FORAGER_ADAPTIVE_WIKI_RUNTIME_REVIEW_EXPIRED", "exclude")
        .args([
            "offdesk",
            "gate",
            "inspect.status",
            "--project-key",
            "project",
            "--request-id",
            "request-one",
            "--task-id",
            "task",
            "--artifact-kind",
            "report",
            "--json",
        ])
        .output()?;
    assert!(applied_output.status.success());
    let applied: serde_json::Value = serde_json::from_slice(&applied_output.stdout)?;
    assert_eq!(
        applied["adaptive_wiki_runtime_decision"]["status"],
        "applied_project_artifact_acknowledged"
    );
    assert_eq!(
        applied["adaptive_wiki_runtime_decision"]["acknowledgement_scope_mode"],
        "project_artifact"
    );
    let runtime = applied["adaptive_wiki_runtime"]
        .as_array()
        .expect("runtime wiki");
    assert!(runtime
        .iter()
        .any(|entry| entry["id"] == "wiki_project_fresh"));
    assert!(!runtime
        .iter()
        .any(|entry| entry["id"] == "wiki_project_expired"));

    let mut entries = base_entries.as_array().expect("entries").clone();
    entries.push(json!({
        "id": "wiki_session_specific",
        "kind": "procedure",
        "scope": "session",
        "scope_ref": "request-two",
        "status": "promoted",
        "activation_mode": "confirm",
        "claim": "Session-specific guidance",
        "ai_instruction": "Request-specific context must be separately reviewed.",
        "evidence_refs": ["task:session"],
        "confidence": "explicit",
        "created_at": now,
        "updated_at": now,
        "review_after": now + Duration::days(1)
    }));
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": entries
        }))?,
    )?;

    let blocked_output = forager_command(temp.path())
        .env("FORAGER_ADAPTIVE_WIKI_RUNTIME_REVIEW_EXPIRED", "exclude")
        .args([
            "offdesk",
            "gate",
            "inspect.status",
            "--project-key",
            "project",
            "--request-id",
            "request-two",
            "--task-id",
            "task",
            "--artifact-kind",
            "report",
            "--json",
        ])
        .output()?;
    assert!(blocked_output.status.success());
    let blocked: serde_json::Value = serde_json::from_slice(&blocked_output.stdout)?;
    assert!(blocked["adaptive_wiki"]
        .as_array()
        .expect("preflight")
        .iter()
        .any(|entry| entry["id"] == "wiki_session_specific"));
    assert!(blocked["adaptive_wiki_runtime"]
        .as_array()
        .is_none_or(Vec::is_empty));
    assert_eq!(
        blocked["adaptive_wiki_runtime_decision"]["status"],
        "strict_requested_scope_mode_blocked"
    );
    assert_eq!(
        blocked["adaptive_wiki_runtime_decision"]["acknowledgement_scope_mode"],
        "project_artifact"
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_runtime_policy_ack_report_flags_near_expiry_and_session_block() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let mut entries = vec![
        json!({
            "id": "wiki_report_expired",
            "kind": "procedure",
            "scope": "artifact_kind",
            "scope_ref": "report",
            "status": "promoted",
            "activation_mode": "confirm",
            "claim": "Expired report guidance",
            "ai_instruction": "Use after explicit review.",
            "evidence_refs": ["task:expired"],
            "confidence": "explicit",
            "created_at": now,
            "updated_at": now,
            "review_after": now - Duration::days(1)
        }),
        json!({
            "id": "wiki_report_fresh",
            "kind": "procedure",
            "scope": "artifact_kind",
            "scope_ref": "report",
            "status": "promoted",
            "activation_mode": "confirm",
            "claim": "Fresh report guidance",
            "ai_instruction": "Keep reviewed report guidance.",
            "evidence_refs": ["task:fresh"],
            "confidence": "explicit",
            "created_at": now,
            "updated_at": now,
            "review_after": now + Duration::days(1)
        }),
    ];
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": entries
        }))?,
    )?;

    let ack_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "ack-runtime-policy",
            "--scope-mode",
            "project-artifact",
            "--project-key",
            "project",
            "--artifact-kind",
            "report",
            "--ttl-hours",
            "1",
            "--reason",
            "short lived strict projection review",
            "--json",
        ])
        .output()?;
    assert!(ack_output.status.success());
    let ack: serde_json::Value = serde_json::from_slice(&ack_output.stdout)?;

    entries.push(json!({
        "id": "wiki_report_session",
        "kind": "procedure",
        "scope": "session",
        "scope_ref": "request-two",
        "status": "promoted",
        "activation_mode": "confirm",
        "claim": "Session-specific report guidance",
        "ai_instruction": "This request has separate reviewed guidance.",
        "evidence_refs": ["task:session"],
        "confidence": "explicit",
        "created_at": now,
        "updated_at": now,
        "review_after": now + Duration::days(1)
    }));
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": entries
        }))?,
    )?;

    let report_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "runtime-policy-ack-report",
            "--session-id",
            "request-two",
            "--project-key",
            "project",
            "--artifact-kind",
            "report",
            "--near-expiry-hours",
            "2",
            "--json",
        ])
        .output()?;
    assert!(report_output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&report_output.stdout)?;
    assert_eq!(report["summary"]["total"], 1);
    assert_eq!(report["summary"]["active"], 1);
    assert_eq!(report["summary"]["near_expiry"], 1);
    assert_eq!(report["summary"]["suggested_actions"], 1);
    assert_eq!(report["summary"]["query_blocked"], 1);
    assert_eq!(
        report["decision"]["status"],
        "strict_requested_scope_mode_blocked"
    );
    assert_eq!(report["decision"]["acknowledgement_id"], ack["id"]);
    assert_eq!(report["acknowledgements"][0]["id"], ack["id"]);
    let statuses = report["acknowledgements"][0]["status"]
        .as_array()
        .expect("status");
    assert!(statuses.iter().any(|status| status == "near_expiry"));
    assert!(statuses
        .iter()
        .any(|status| status == "query_blocked_by_session_scope"));
    assert_eq!(
        report["acknowledgements"][0]["suggested_action"]["kind"],
        "record_exact_query_acknowledgement"
    );
    let suggested_ack = report["acknowledgements"][0]["suggested_action"]["ack_command_template"]
        .as_str()
        .expect("ack command");
    assert!(suggested_ack.contains("ack-runtime-policy"));
    assert!(suggested_ack.contains("--session-id 'request-two'"));
    assert!(!suggested_ack.contains("--scope-mode"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_runtime_policy_ack_report_suggests_recompare_for_expired_and_stale_ack() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let base_entries = json!([
        {
            "id": "wiki_ack_expired_review",
            "kind": "procedure",
            "scope": "artifact_kind",
            "scope_ref": "report",
            "status": "promoted",
            "activation_mode": "confirm",
            "claim": "Expired review guidance",
            "ai_instruction": "Use only after strict review.",
            "evidence_refs": ["task:expired"],
            "confidence": "explicit",
            "created_at": now,
            "updated_at": now,
            "review_after": now - Duration::days(1)
        },
        {
            "id": "wiki_ack_fresh_review",
            "kind": "procedure",
            "scope": "artifact_kind",
            "scope_ref": "report",
            "status": "promoted",
            "activation_mode": "confirm",
            "claim": "Fresh review guidance",
            "ai_instruction": "Fresh strict runtime guidance.",
            "evidence_refs": ["task:fresh"],
            "confidence": "explicit",
            "created_at": now,
            "updated_at": now,
            "review_after": now + Duration::days(1)
        }
    ]);
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": base_entries
        }))?,
    )?;

    let ack_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "ack-runtime-policy",
            "--session-id",
            "request",
            "--project-key",
            "project",
            "--artifact-kind",
            "report",
            "--reason",
            "operator reviewed strict projection",
            "--json",
        ])
        .output()?;
    assert!(ack_output.status.success());
    let ack: serde_json::Value = serde_json::from_slice(&ack_output.stdout)?;
    let mut expired_ack = ack.clone();
    expired_ack["created_at"] = serde_json::to_value(now - Duration::hours(2))?;
    expired_ack["expires_at"] = serde_json::to_value(now - Duration::hours(1))?;
    fs::write(
        profile_dir.join("adaptive_wiki_runtime_policy_acknowledgements.jsonl"),
        format!("{}\n", serde_json::to_string(&expired_ack)?),
    )?;

    let expired_report_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "runtime-policy-ack-report",
            "--session-id",
            "request",
            "--project-key",
            "project",
            "--artifact-kind",
            "report",
            "--json",
        ])
        .output()?;
    assert!(expired_report_output.status.success());
    let expired_report: serde_json::Value = serde_json::from_slice(&expired_report_output.stdout)?;
    assert_eq!(
        expired_report["decision"]["status"],
        "strict_requested_expired_acknowledgement"
    );
    assert_eq!(expired_report["summary"]["expired"], 1);
    assert_eq!(expired_report["summary"]["query_expired"], 1);
    assert_eq!(expired_report["summary"]["suggested_actions"], 1);
    assert_eq!(
        expired_report["acknowledgements"][0]["suggested_action"]["kind"],
        "recompare_and_append_acknowledgement"
    );
    let expired_ack_command = expired_report["acknowledgements"][0]["suggested_action"]
        ["ack_command_template"]
        .as_str()
        .expect("expired ack command");
    assert!(expired_ack_command.contains("ack-runtime-policy"));
    assert!(expired_ack_command.contains("--session-id 'request'"));
    assert!(!expired_ack_command.contains("expires-at"));

    fs::write(
        profile_dir.join("adaptive_wiki_runtime_policy_acknowledgements.jsonl"),
        format!("{}\n", serde_json::to_string(&ack)?),
    )?;
    let mut stale_entries = base_entries.as_array().expect("entries").clone();
    stale_entries.push(json!({
        "id": "wiki_ack_new_review",
        "kind": "procedure",
        "scope": "artifact_kind",
        "scope_ref": "report",
        "status": "promoted",
        "activation_mode": "confirm",
        "claim": "New strict review guidance",
        "ai_instruction": "New projection entry changes the comparison hash.",
        "evidence_refs": ["task:new"],
        "confidence": "explicit",
        "created_at": now,
        "updated_at": now,
        "review_after": now + Duration::days(1)
    }));
    fs::write(
        profile_dir.join("adaptive_wiki_entries.json"),
        serde_json::to_string_pretty(&json!({
            "version": "2026-05-14.v0",
            "entries": stale_entries
        }))?,
    )?;

    let stale_report_output = forager_command(temp.path())
        .args([
            "offdesk",
            "wiki",
            "runtime-policy-ack-report",
            "--session-id",
            "request",
            "--project-key",
            "project",
            "--artifact-kind",
            "report",
            "--json",
        ])
        .output()?;
    assert!(stale_report_output.status.success());
    let stale_report: serde_json::Value = serde_json::from_slice(&stale_report_output.stdout)?;
    assert_eq!(
        stale_report["decision"]["status"],
        "strict_requested_stale_acknowledgement"
    );
    assert_eq!(stale_report["summary"]["query_stale"], 1);
    assert_eq!(stale_report["summary"]["suggested_actions"], 1);
    assert_eq!(
        stale_report["acknowledgements"][0]["suggested_action"]["kind"],
        "recompare_and_append_acknowledgement"
    );
    let compare_command = stale_report["acknowledgements"][0]["suggested_action"]
        ["compare_command_template"]
        .as_str()
        .expect("compare command");
    assert!(compare_command.contains("projection"));
    assert!(compare_command.contains("--compare-review-expired-policy"));
    assert!(compare_command.contains("--runtime-agent-mode-default"));
    assert!(compare_command.contains("--session-id 'request'"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_tick_defers_provider_capacity_then_launches_after_cooldown() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let brief_path = temp.path().join("brief.json");
    let result_path = temp.path().join("provider-result.txt");
    let now = Utc::now();
    let retry_at = now + Duration::minutes(2);
    fs::write(
        &brief_path,
        serde_json::to_string_pretty(&json!({
            "request_id": "request",
            "task_id": "task",
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["dispatch.runtime"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(10)
        }))?,
    )?;
    fs::write(
        profile_dir.join("provider_capacity.json"),
        serde_json::to_string_pretty(&json!([
            {
                "provider_id": "openai",
                "model": "gpt-4.1",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": retry_at,
                "last_error_summary": "rate limit",
                "updated_at": now
            }
        ]))?,
    )?;
    let command = format!("printf done > {}", result_path.display());

    let enqueue_output = forager_command(temp.path())
        .args([
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--brief",
            brief_path.to_str().expect("utf-8 path"),
            "--provider-id",
            "openai",
            "--model",
            "gpt-4.1",
            "--cmd",
            command.as_str(),
            "--workdir",
            temp.path().to_str().expect("utf-8 path"),
            "--result-artifact",
            result_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(enqueue_output.status.success());

    let blocked_output = forager_command(temp.path())
        .env_remove("OPENAI_API_KEY")
        .env_remove("ANTHROPIC_API_KEY")
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(blocked_output.status.success());
    let blocked: serde_json::Value = serde_json::from_slice(&blocked_output.stdout)?;
    assert_eq!(blocked["provider_deferred"], 1);
    assert_eq!(blocked["failed"], 0);
    assert!(!profile_dir.join("pending_action_approvals.json").exists());
    let background_runs_path = profile_dir.join("background_runs.json");
    if background_runs_path.exists() {
        let runs: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&background_runs_path)?)?;
        assert_eq!(runs.as_array().map(Vec::len), Some(0));
    }

    let tasks_path = profile_dir.join("offdesk_tasks.json");
    let mut tasks: serde_json::Value = serde_json::from_str(&fs::read_to_string(&tasks_path)?)?;
    assert_eq!(tasks[0]["status"], "queued");
    assert_eq!(tasks[0]["not_before"], serde_json::to_value(retry_at)?);
    assert_eq!(tasks[0]["provider_id"], "openai");
    assert_eq!(tasks[0]["model"], "gpt-4.1");
    assert_eq!(
        tasks[0]["last_provider_fallback"]["current_provider_id"],
        "openai"
    );
    assert!(tasks[0]["last_provider_fallback"]["candidates"]
        .as_array()
        .expect("fallback candidates")
        .iter()
        .all(
            |candidate| !(candidate["provider_id"] == "openai" && candidate["model"] == "gpt-4.1")
        ));

    let tasks_output = forager_command(temp.path())
        .args(["offdesk", "tasks", "--json"])
        .output()?;
    assert!(tasks_output.status.success());
    let task_views: serde_json::Value = serde_json::from_slice(&tasks_output.stdout)?;
    assert_eq!(
        task_views[0]["last_provider_fallback"]["current_provider_id"],
        "openai"
    );

    tasks[0]["not_before"] = serde_json::to_value(now - Duration::seconds(1))?;
    fs::write(&tasks_path, serde_json::to_string_pretty(&tasks)?)?;
    fs::write(
        profile_dir.join("provider_capacity.json"),
        serde_json::to_string_pretty(&json!([
            {
                "provider_id": "openai",
                "model": "gpt-4.1",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": now - Duration::seconds(1),
                "last_error_summary": "rate limit",
                "updated_at": now
            }
        ]))?,
    )?;

    let launch_output = forager_command(temp.path())
        .env_remove("OPENAI_API_KEY")
        .env_remove("ANTHROPIC_API_KEY")
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(launch_output.status.success());
    let launched: serde_json::Value = serde_json::from_slice(&launch_output.stdout)?;
    assert_eq!(launched["launched"], 1);
    let tasks_after_launch: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&tasks_path)?)?;
    assert!(tasks_after_launch[0]
        .get("last_provider_fallback")
        .is_none());
    let runs: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("background_runs.json"),
    )?)?;
    assert_eq!(runs[0]["task_id"], "task");
    Ok(())
}

#[test]
#[serial]
fn offdesk_tick_creates_provider_fallback_approval_then_retargets_and_launches() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let brief_path = temp.path().join("brief.json");
    let result_path = temp.path().join("fallback-result.txt");
    let now = Utc::now();
    let retry_at = now + Duration::minutes(10);
    fs::write(
        &brief_path,
        serde_json::to_string_pretty(&json!({
            "request_id": "request",
            "task_id": "task",
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["dispatch.runtime"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(30)
        }))?,
    )?;
    fs::write(
        profile_dir.join("provider_capacity.json"),
        serde_json::to_string_pretty(&json!([
            {
                "provider_id": "openai",
                "model": "gpt-4.1",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": retry_at,
                "last_error_summary": "rate limit",
                "updated_at": now
            }
        ]))?,
    )?;
    let command = format!("printf done > {}", result_path.display());

    let enqueue_output = forager_command(temp.path())
        .args([
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--brief",
            brief_path.to_str().expect("utf-8 path"),
            "--provider-id",
            "openai",
            "--model",
            "gpt-4.1",
            "--cmd",
            command.as_str(),
            "--workdir",
            temp.path().to_str().expect("utf-8 path"),
            "--result-artifact",
            result_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(enqueue_output.status.success());

    let blocked_output = forager_command(temp.path())
        .env("OPENAI_API_KEY", "sk-test-provider-fallback")
        .env_remove("ANTHROPIC_API_KEY")
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(blocked_output.status.success());
    let blocked: serde_json::Value = serde_json::from_slice(&blocked_output.stdout)?;
    assert_eq!(blocked["provider_deferred"], 1);
    assert_eq!(blocked["pending_approval"], 0);
    assert_eq!(blocked["launched"], 0);

    let approvals_path = profile_dir.join("pending_action_approvals.json");
    let tasks_path = profile_dir.join("offdesk_tasks.json");
    let approvals: serde_json::Value = serde_json::from_str(&fs::read_to_string(&approvals_path)?)?;
    assert_eq!(approvals.as_array().expect("approvals").len(), 1);
    assert_eq!(approvals[0]["action"], "dispatch.provider_fallback");
    assert_eq!(approvals[0]["risk_level"], "runtime_mutation");
    assert_eq!(approvals[0]["approval_mode"], "operator_required");
    assert_eq!(approvals[0]["metadata"]["kind"], "provider_fallback");
    assert_eq!(approvals[0]["metadata"]["current_provider_id"], "openai");
    assert_eq!(approvals[0]["metadata"]["current_model"], "gpt-4.1");
    assert_eq!(approvals[0]["metadata"]["candidate_limit"], 3);
    assert_eq!(
        approvals[0]["metadata"]["apply_scope"],
        "request_matching_provider_model"
    );
    let approval_brief = &approvals[0]["metadata"]["approval_brief"];
    assert_eq!(approval_brief["schema"], "approval_brief.v1");
    assert_eq!(approval_brief["source"], "offdesk.provider_fallback");
    assert_eq!(approval_brief["recommendation"], "approve");
    assert_eq!(approval_brief["subject"], "provider fallback");
    assert!(approval_brief["summary_lines"]
        .as_array()
        .expect("summary lines")
        .iter()
        .any(|line| line
            .as_str()
            .unwrap_or_default()
            .contains("Provider/model retargeting")));
    assert!(approval_brief["scope"]
        .as_str()
        .unwrap_or_default()
        .contains("does not approve runtime dispatch"));
    assert!(approval_brief["decision_impacts"]["approve"]
        .as_str()
        .unwrap_or_default()
        .contains("runtime dispatch still needs its own approval"));
    assert!(approval_brief["judgment_route_summary"]
        .as_str()
        .unwrap_or_default()
        .contains("deterministic gate"));
    assert!(approval_brief["evidence_sufficiency"]
        .as_str()
        .unwrap_or_default()
        .contains("ranked fallback candidates"));
    assert_eq!(approval_brief["default_if_no_reply"], "defer");
    assert!(approval_brief["decision_impacts"]["deny"]
        .as_str()
        .unwrap_or_default()
        .contains("Keep openai model gpt-4.1 queued"));
    assert_eq!(approval_brief["options"][0]["id"], "approve");
    assert_eq!(approval_brief["options"][0]["label"], "Approve fallback");
    assert_eq!(approval_brief["options"][1]["id"], "deny");
    assert!(approval_brief["options"][1]["natural_input_prompt"]
        .as_str()
        .unwrap_or_default()
        .contains("should not be applied"));
    assert_eq!(approval_brief["options"][2]["id"], "defer");
    assert!(!serde_json::to_string(approval_brief)?.contains("sk-test-provider-fallback"));
    assert!(approvals[0]["metadata"]["candidates"]
        .as_array()
        .expect("candidates")
        .iter()
        .all(|candidate| candidate["recommended"] == true));

    let pending_json_output = forager_command(temp.path())
        .args(["offdesk", "pending", "--json"])
        .output()?;
    assert!(pending_json_output.status.success());
    let pending_json: serde_json::Value = serde_json::from_slice(&pending_json_output.stdout)?;
    assert_eq!(pending_json.as_array().expect("pending approvals").len(), 1);
    assert_eq!(pending_json[0]["metadata"], approvals[0]["metadata"]);
    assert_eq!(
        pending_json[0]["next_safe_action"]["kind"],
        "approval_pending"
    );

    let pending_output = forager_command(temp.path())
        .args(["offdesk", "pending"])
        .output()?;
    assert!(pending_output.status.success());
    let pending_stdout = String::from_utf8_lossy(&pending_output.stdout);
    assert!(pending_stdout.contains("prompt: approve recommendation for provider fallback"));
    assert!(pending_stdout.contains("Approve this provider fallback retargeting?"));
    assert!(pending_stdout.contains("does not approve runtime dispatch"));
    assert!(pending_stdout.contains("fallback target: openai model gpt-4.1"));
    assert!(pending_stdout.contains("gpt-4.1-mini"));
    assert!(pending_stdout.contains("forager offdesk ok"));
    assert!(pending_stdout.contains("forager offdesk cancel"));

    let mut tasks_before_ok: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&tasks_path)?)?;
    tasks_before_ok[0]["not_before"] = serde_json::to_value(now - Duration::seconds(1))?;
    fs::write(&tasks_path, serde_json::to_string_pretty(&tasks_before_ok)?)?;
    let repeated_output = forager_command(temp.path())
        .env("OPENAI_API_KEY", "sk-test-provider-fallback")
        .env_remove("ANTHROPIC_API_KEY")
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(repeated_output.status.success());
    let repeated: serde_json::Value = serde_json::from_slice(&repeated_output.stdout)?;
    assert_eq!(repeated["provider_deferred"], 1);
    let repeated_approvals: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&approvals_path)?)?;
    assert_eq!(repeated_approvals.as_array().expect("approvals").len(), 1);

    let ok_output = forager_command(temp.path())
        .args(["offdesk", "ok", "--json"])
        .output()?;
    assert!(ok_output.status.success());

    let launch_output = forager_command(temp.path())
        .env("OPENAI_API_KEY", "sk-test-provider-fallback")
        .env_remove("ANTHROPIC_API_KEY")
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(launch_output.status.success());
    let launched: serde_json::Value = serde_json::from_slice(&launch_output.stdout)?;
    assert_eq!(launched["provider_retargeted"], 1);
    assert_eq!(launched["launched"], 1);

    let tasks: serde_json::Value = serde_json::from_str(&fs::read_to_string(&tasks_path)?)?;
    assert_eq!(tasks[0]["provider_id"], "openai");
    assert_eq!(tasks[0]["model"], "gpt-4.1-mini");
    assert_eq!(tasks[0]["command"], command.as_str());
    assert_eq!(
        tasks[0]["workdir"],
        temp.path().to_str().expect("utf-8 path")
    );
    assert_eq!(
        tasks[0]["result_artifact_path"],
        result_path.to_str().expect("utf-8 path")
    );
    assert!(tasks[0].get("not_before").is_none());
    assert!(tasks[0].get("last_provider_fallback").is_none());
    let approvals_after: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&approvals_path)?)?;
    assert_eq!(approvals_after[0]["status"], "superseded");
    Ok(())
}

#[test]
#[serial]
fn offdesk_provider_fallback_approval_skips_invalid_candidate_and_keeps_scope() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let retry_at = now + Duration::minutes(10);
    let result_path = temp.path().join("fallback-scope-result.txt");
    let other_result_path = temp.path().join("fallback-scope-other.txt");
    let command = format!("printf done > {}", result_path.display());
    let other_command = format!("printf other > {}", other_result_path.display());
    let brief = |task_id: &str| {
        json!({
            "request_id": "request",
            "task_id": task_id,
            "project_key": "project",
            "approved": true,
            "allowed_runtime_mutations": ["dispatch.runtime"],
            "allowed_canonical_mutations": [],
            "fresh_until": now + Duration::minutes(30)
        })
    };
    fs::write(
        profile_dir.join("provider_capacity.json"),
        serde_json::to_string_pretty(&json!([
            {
                "provider_id": "openai",
                "model": "gpt-4.1",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": retry_at,
                "last_error_summary": "rate limit",
                "updated_at": now
            }
        ]))?,
    )?;
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task",
                "request_id": "request",
                "project_key": "project",
                "status": "queued",
                "capability_id": "dispatch.runtime",
                "runner_kind": "local_background",
                "command": command,
                "workdir": temp.path().to_str().expect("utf-8 path"),
                "execution_brief": brief("task"),
                "provider_id": "openai",
                "model": "gpt-4.1",
                "result_artifact_path": result_path.to_str().expect("utf-8 path"),
                "created_at": now,
                "updated_at": now
            },
            {
                "task_id": "other-model",
                "request_id": "request",
                "project_key": "project",
                "status": "queued",
                "capability_id": "dispatch.runtime",
                "runner_kind": "local_background",
                "command": other_command,
                "workdir": temp.path().to_str().expect("utf-8 path"),
                "execution_brief": brief("other-model"),
                "provider_id": "openai",
                "model": "gpt-4o",
                "not_before": retry_at,
                "result_artifact_path": other_result_path.to_str().expect("utf-8 path"),
                "created_at": now,
                "updated_at": now
            }
        ]))?,
    )?;

    let blocked_output = forager_command(temp.path())
        .env("OPENAI_API_KEY", "sk-test-provider-fallback")
        .env("ANTHROPIC_API_KEY", "sk-ant-test-provider-fallback")
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(blocked_output.status.success());
    let blocked: serde_json::Value = serde_json::from_slice(&blocked_output.stdout)?;
    assert_eq!(blocked["provider_deferred"], 1);

    let ok_output = forager_command(temp.path())
        .args(["offdesk", "ok", "--json"])
        .output()?;
    assert!(ok_output.status.success());
    fs::write(
        profile_dir.join("provider_capacity.json"),
        serde_json::to_string_pretty(&json!([
            {
                "provider_id": "openai",
                "model": "gpt-4.1",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": retry_at,
                "last_error_summary": "rate limit",
                "updated_at": now
            },
            {
                "provider_id": "openai",
                "model": "gpt-4.1-mini",
                "status": "cooling_down",
                "reason": "rate_limit",
                "cooldown_until": retry_at,
                "last_error_summary": "rate limit",
                "updated_at": now
            }
        ]))?,
    )?;

    let launch_output = forager_command(temp.path())
        .env("OPENAI_API_KEY", "sk-test-provider-fallback")
        .env("ANTHROPIC_API_KEY", "sk-ant-test-provider-fallback")
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(launch_output.status.success());
    let launched: serde_json::Value = serde_json::from_slice(&launch_output.stdout)?;
    assert_eq!(launched["provider_retargeted"], 1);
    assert_eq!(launched["launched"], 1);

    let tasks: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(profile_dir.join("offdesk_tasks.json"))?)?;
    let task = tasks
        .as_array()
        .expect("tasks")
        .iter()
        .find(|task| task["task_id"] == "task")
        .expect("task");
    let other = tasks
        .as_array()
        .expect("tasks")
        .iter()
        .find(|task| task["task_id"] == "other-model")
        .expect("other task");
    assert_eq!(task["provider_id"], "anthropic");
    assert_eq!(task["model"], "claude-3-5-sonnet-latest");
    assert_eq!(other["provider_id"], "openai");
    assert_eq!(other["model"], "gpt-4o");
    assert_eq!(other["not_before"], serde_json::to_value(retry_at)?);
    Ok(())
}

#[test]
#[serial]
fn offdesk_tick_pending_approval_then_ok_launches_next_tick() -> Result<()> {
    let temp = tempdir()?;
    let result_path = temp.path().join("approved-result.txt");
    let command = format!("printf done > {}", result_path.display());

    let enqueue_output = forager_command(temp.path())
        .args([
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--cmd",
            command.as_str(),
            "--workdir",
            temp.path().to_str().expect("utf-8 path"),
            "--result-artifact",
            result_path.to_str().expect("utf-8 path"),
            "--json",
        ])
        .output()?;
    assert!(enqueue_output.status.success());

    let pending_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(pending_output.status.success());
    let pending: serde_json::Value = serde_json::from_slice(&pending_output.stdout)?;
    assert_eq!(pending["pending_approval"], 1);
    assert_eq!(pending["next_safe_actions"][0]["kind"], "approval_pending");
    assert_eq!(
        pending["next_safe_actions"][0]["requires_operator_review"],
        true
    );

    let approvals: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir(temp.path()).join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(approvals.as_array().expect("approvals").len(), 1);

    let ok_output = forager_command(temp.path())
        .args(["offdesk", "ok", "--json"])
        .output()?;
    assert!(ok_output.status.success());

    let launch_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(launch_output.status.success());
    let launched: serde_json::Value = serde_json::from_slice(&launch_output.stdout)?;
    assert_eq!(launched["launched"], 1);
    assert!(launched["next_safe_actions"]
        .as_array()
        .expect("next safe actions")
        .iter()
        .any(|action| action["kind"] == "runtime_monitoring"));

    let approvals_after: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir(temp.path()).join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(approvals_after[0]["status"], "superseded");
    Ok(())
}

#[test]
#[serial]
fn offdesk_tick_stale_background_creates_resume_state() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let log_path = temp.path().join("background.log");
    let result_path = temp.path().join("background-result.txt");
    fs::write(
        &log_path,
        "first line\nlast line token=sk-secretsecretsecretsecret\n",
    )?;
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task",
                "request_id": "request",
                "project_key": "project",
                "status": "running",
                "capability_id": "dispatch.runtime",
                "runner_kind": "local_background",
                "command": "true",
                "workdir": temp.path().to_str().expect("utf-8 path"),
                "background_ticket_id": "ticket",
                "attempt_count": 1,
                "log_artifact_path": log_path.to_str().expect("utf-8 path"),
                "result_artifact_path": result_path.to_str().expect("utf-8 path"),
                "created_at": now,
                "updated_at": now
            }
        ]))?,
    )?;
    fs::write(
        profile_dir.join("background_runs.json"),
        serde_json::to_string_pretty(&json!([
            {
                "ticket_id": "ticket",
                "runner_kind": "local_background",
                "phase": "launched",
                "log_artifact_path": log_path.to_str().expect("utf-8 path"),
                "result_artifact_path": result_path.to_str().expect("utf-8 path"),
                "runtime_handle_alive": false
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["resume_pending"], 1);
    assert!(report["next_safe_actions"]
        .as_array()
        .expect("next safe actions")
        .iter()
        .any(|action| action["kind"] == "recovery_required"));

    let tasks: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(profile_dir.join("offdesk_tasks.json"))?)?;
    assert_eq!(tasks[0]["status"], "resume_pending");

    let resume: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("task_resume_state.json"),
    )?)?;
    assert_eq!(resume[0]["status"], "resume_pending");
    assert!(resume[0]["resume_id"]
        .as_str()
        .expect("resume id")
        .starts_with("resume_"));
    assert_eq!(resume[0]["background_ticket_id"], "ticket");
    assert_eq!(resume[0]["last_task_status"], "running");
    assert_eq!(resume[0]["attempt_count"], 1);
    assert!(!resume[0]["last_log_tail"]
        .as_str()
        .expect("last log tail")
        .contains("sk-secretsecretsecretsecret"));
    let evidence = resume[0]["evidence"].as_array().expect("evidence");
    assert!(evidence
        .iter()
        .any(|entry| entry["kind"].as_str() == Some("background_probe")));
    assert!(evidence.iter().any(|entry| {
        entry["kind"].as_str() == Some("log_artifact") && entry["present"].as_bool() == Some(true)
    }));
    assert!(evidence.iter().any(|entry| {
        entry["kind"].as_str() == Some("result_artifact")
            && entry["present"].as_bool() == Some(false)
    }));
    assert!(evidence
        .iter()
        .any(|entry| entry["kind"].as_str() == Some("log_tail")));

    let resume_output = forager_command(temp.path())
        .args(["offdesk", "resume"])
        .output()?;
    assert!(resume_output.status.success());
    let stdout = String::from_utf8_lossy(&resume_output.stdout);
    assert!(stdout.contains("resume_id: resume_"));
    assert!(stdout.contains("evidence: background_probe"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_background_ack_clears_stale_attention_for_cancelled_linked_task() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([durable_task_with(
            "task",
            "dispatch.runtime",
            "cancelled",
            now,
            "true",
            temp.path()
        )]))?,
    )?;
    fs::write(
        profile_dir.join("background_runs.json"),
        serde_json::to_string_pretty(&json!([
            {
                "ticket_id": "ticket",
                "task_id": "task",
                "request_id": "request",
                "project_key": "project",
                "runner_kind": "local_background",
                "phase": "stale_lost_callback",
                "runtime_handle_alive": false
            }
        ]))?,
    )?;

    let status_before_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_before_output.status.success());
    let status_before: serde_json::Value = serde_json::from_slice(&status_before_output.stdout)?;
    assert_eq!(status_before["stale_background_runs"], 1);
    assert!(status_before["offdesk_next_safe_actions"]
        .as_array()
        .expect("next safe actions")
        .iter()
        .any(|action| action["kind"] == "recovery_required"));

    let ack_output = forager_command(temp.path())
        .args([
            "offdesk",
            "background-ack",
            "ticket",
            "--reason",
            "cancelled stale local-background probe was superseded",
            "--by",
            "codex",
            "--json",
        ])
        .output()?;
    assert!(ack_output.status.success());
    let ack: serde_json::Value = serde_json::from_slice(&ack_output.stdout)?;
    assert_eq!(ack["ticket_id"], "ticket");
    assert_eq!(ack["linked_task_ids"], json!(["task"]));
    assert_eq!(
        ack["acknowledgement"]["previous_phase"],
        "stale_lost_callback"
    );
    assert_eq!(ack["acknowledgement"]["acknowledged_by"], "codex");
    assert_eq!(ack["status"]["decision"]["phase"], "recovery_acknowledged");

    let runs: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("background_runs.json"),
    )?)?;
    assert_eq!(runs[0]["phase"], "recovery_acknowledged");
    assert_eq!(
        runs[0]["operator_recovery_ack"]["reason"],
        "cancelled stale local-background probe was superseded"
    );
    assert!(runs[0]["operator_recovery_ack"]["does_not_authorize"]
        .as_array()
        .expect("does not authorize")
        .iter()
        .any(|value| value == "accepting any Offdesk output as truth"));

    let status_after_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_after_output.status.success());
    let status_after: serde_json::Value = serde_json::from_slice(&status_after_output.stdout)?;
    assert_eq!(status_after["stale_background_runs"], 0);
    assert!(!status_after["offdesk_next_safe_actions"]
        .as_array()
        .expect("next safe actions")
        .iter()
        .any(|action| action["kind"] == "recovery_required"));
    assert!(!profile_dir.join("task_resume_state.json").exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_background_ack_rejects_non_cancelled_linked_task() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([durable_task_with(
            "task",
            "dispatch.runtime",
            "running",
            now,
            "true",
            temp.path()
        )]))?,
    )?;
    fs::write(
        profile_dir.join("background_runs.json"),
        serde_json::to_string_pretty(&json!([
            {
                "ticket_id": "ticket",
                "task_id": "task",
                "request_id": "request",
                "project_key": "project",
                "runner_kind": "local_background",
                "phase": "stale_lost_callback",
                "runtime_handle_alive": false
            }
        ]))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "background-ack",
            "ticket",
            "--reason",
            "unsafe shortcut",
            "--json",
        ])
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("non-cancelled linked tasks"));
    assert!(stderr.contains("task:Running"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_poll_reconciles_completed_background_task() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    let log_path = temp.path().join("background.log");
    let result_path = temp.path().join("background-result.json");
    fs::write(&log_path, "completed\n")?;
    fs::write(&result_path, "{\"ok\":true}\n")?;
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            {
                "task_id": "task",
                "request_id": "request",
                "project_key": "project",
                "status": "launched",
                "capability_id": "dispatch.runtime",
                "runner_kind": "local_tmux",
                "command": "true",
                "workdir": temp.path().to_str().expect("utf-8 path"),
                "background_ticket_id": "ticket",
                "attempt_count": 1,
                "agent_mode": "writing",
                "log_artifact_path": log_path.to_str().expect("utf-8 path"),
                "result_artifact_path": result_path.to_str().expect("utf-8 path"),
                "created_at": now,
                "updated_at": now
            }
        ]))?,
    )?;
    fs::write(
        profile_dir.join("background_runs.json"),
        serde_json::to_string_pretty(&json!([
            {
                "ticket_id": "ticket",
                "task_id": "task",
                "request_id": "request",
                "project_key": "project",
                "agent_mode": "writing",
                "runner_kind": "local_tmux",
                "phase": "launched",
                "log_artifact_path": log_path.to_str().expect("utf-8 path"),
                "result_artifact_path": result_path.to_str().expect("utf-8 path"),
                "runtime_handle_alive": false
            }
        ]))?,
    )?;

    let poll_output = forager_command(temp.path())
        .args(["offdesk", "poll", "--json", "ticket"])
        .output()?;
    assert!(poll_output.status.success());
    let poll: serde_json::Value = serde_json::from_slice(&poll_output.stdout)?;
    assert_eq!(poll[0]["decision"]["phase"], "completed");
    assert_eq!(poll[0]["probe"]["result_artifact_present"], true);
    assert_eq!(poll[0]["mode_verdict"], "evidence_ready");
    assert_eq!(poll[0]["mode_risk"], "operator_review_required");
    assert_eq!(poll[0]["review_stage_required"], true);
    assert_eq!(poll[0]["next_safe_action"]["kind"], "review_required");
    assert!(poll[0]["next_safe_action"]["commands"]
        .as_array()
        .expect("poll next action commands")
        .iter()
        .any(|command| command
            .as_str()
            .expect("poll next action command")
            .contains("forager offdesk closeout --project-key project --task-id task")));

    let tasks: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(profile_dir.join("offdesk_tasks.json"))?)?;
    assert_eq!(tasks[0]["status"], "completed");
    assert!(tasks[0]["last_error"].is_null());

    let task_output = forager_command(temp.path())
        .args(["offdesk", "tasks", "--json"])
        .output()?;
    assert!(task_output.status.success());
    let task_views: serde_json::Value = serde_json::from_slice(&task_output.stdout)?;
    assert_eq!(task_views[0]["status"], "completed");
    assert_eq!(task_views[0]["agent_mode"], "writing");
    assert_eq!(task_views[0]["mode_verdict"], "evidence_ready");
    assert_eq!(task_views[0]["mode_risk"], "operator_review_required");
    assert_eq!(task_views[0]["review_stage_required"], true);
    assert_eq!(task_views[0]["next_safe_action"]["kind"], "review_required");
    assert_eq!(
        task_views[0]["next_safe_action"]["requires_operator_review"],
        true
    );
    assert!(task_views[0]["next_safe_action"]["commands"]
        .as_array()
        .expect("next action commands")
        .iter()
        .any(|command| command
            .as_str()
            .expect("next action command")
            .contains("forager offdesk closeout --project-key project --task-id task")));
    Ok(())
}

#[test]
#[serial]
fn offdesk_cancel_task_updates_task_json_and_redacts_output() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([durable_task(
            "running",
            now,
            "printf token=sk-secretsecretsecretsecret",
            temp.path(),
        )]))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "cancel-task",
            "task",
            "--reason",
            "operator stop token=sk-secretsecretsecretsecret",
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(report["changed"], true);
    assert_eq!(report["status"], "cancelled");
    assert!(!String::from_utf8_lossy(&output.stdout).contains("sk-secret"));

    let tasks: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(profile_dir.join("offdesk_tasks.json"))?)?;
    assert_eq!(tasks[0]["status"], "cancelled");
    assert_eq!(tasks[0]["background_ticket_id"], "ticket");
    Ok(())
}

#[test]
#[serial]
fn offdesk_retry_task_requeues_failed_task_and_tick_launches() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let result_path = temp.path().join("retry-result.txt");
    let command = format!("printf done > {}", result_path.display());
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([durable_task(
            "failed",
            now,
            command.as_str(),
            temp.path(),
        )]))?,
    )?;

    let retry_output = forager_command(temp.path())
        .args(["offdesk", "retry-task", "task", "--json"])
        .output()?;
    assert!(retry_output.status.success());
    let retry: serde_json::Value = serde_json::from_slice(&retry_output.stdout)?;
    assert_eq!(retry["changed"], true);
    assert_eq!(retry["status"], "queued");
    assert!(retry["task"].get("background_ticket_id").is_none());

    let tick_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(tick_output.status.success());
    let tick: serde_json::Value = serde_json::from_slice(&tick_output.stdout)?;
    assert_eq!(tick["launched"], 1);

    let tasks: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(profile_dir.join("offdesk_tasks.json"))?)?;
    assert_eq!(tasks[0]["status"], "launched");
    assert_ne!(tasks[0]["background_ticket_id"], "ticket");
    Ok(())
}

#[test]
#[serial]
fn offdesk_plain_retry_preserves_denied_approval_and_tick_denies() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([durable_task_with(
            "task",
            "dispatch.runtime",
            "failed",
            now,
            "true",
            temp.path(),
        )]))?,
    )?;
    fs::write(
        profile_dir.join("pending_action_approvals.json"),
        serde_json::to_string_pretty(&json!([denied_approval("dispatch.runtime", now)]))?,
    )?;

    let retry_output = forager_command(temp.path())
        .args(["offdesk", "retry-task", "task", "--json"])
        .output()?;
    assert!(retry_output.status.success());
    let retry: serde_json::Value = serde_json::from_slice(&retry_output.stdout)?;
    assert_eq!(retry["status"], "queued");
    assert_eq!(retry["superseded_denied_approvals"], 0);

    let tick_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(tick_output.status.success());
    let tick: serde_json::Value = serde_json::from_slice(&tick_output.stdout)?;
    assert_eq!(tick["failed"], 1);

    let approvals: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(approvals[0]["status"], "denied");
    let tasks: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(profile_dir.join("offdesk_tasks.json"))?)?;
    assert_eq!(tasks[0]["status"], "failed");
    Ok(())
}

#[test]
#[serial]
fn offdesk_retry_new_approval_supersedes_denied_and_tick_creates_pending() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([durable_task_with(
            "task",
            "dispatch.runtime",
            "failed",
            now,
            "true",
            temp.path(),
        )]))?,
    )?;
    fs::write(
        profile_dir.join("pending_action_approvals.json"),
        serde_json::to_string_pretty(&json!([denied_approval("dispatch.runtime", now)]))?,
    )?;

    let retry_output = forager_command(temp.path())
        .args(["offdesk", "retry-task", "task", "--new-approval", "--json"])
        .output()?;
    assert!(retry_output.status.success());
    let retry: serde_json::Value = serde_json::from_slice(&retry_output.stdout)?;
    assert_eq!(retry["status"], "queued");
    assert_eq!(retry["superseded_denied_approvals"], 1);

    let approvals_after_retry: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(approvals_after_retry[0]["status"], "superseded");

    let tick_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(tick_output.status.success());
    let tick: serde_json::Value = serde_json::from_slice(&tick_output.stdout)?;
    assert_eq!(tick["pending_approval"], 1);

    let approvals: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(approvals.as_array().expect("approvals").len(), 2);
    assert_eq!(approvals[0]["status"], "superseded");
    assert_eq!(approvals[1]["status"], "pending");
    let audit = fs::read_to_string(profile_dir.join("action_audit.jsonl"))?;
    assert!(audit.contains("\"transition\":\"supersede_denied\""));
    Ok(())
}

#[test]
#[serial]
fn offdesk_retry_new_approval_reports_supersede_even_when_retry_noops() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([durable_task_with(
            "task",
            "dispatch.runtime",
            "queued",
            now,
            "true",
            temp.path(),
        )]))?,
    )?;
    fs::write(
        profile_dir.join("pending_action_approvals.json"),
        serde_json::to_string_pretty(&json!([denied_approval("dispatch.runtime", now)]))?,
    )?;

    let retry_output = forager_command(temp.path())
        .args(["offdesk", "retry-task", "task", "--new-approval", "--json"])
        .output()?;
    assert!(retry_output.status.success());
    let retry: serde_json::Value = serde_json::from_slice(&retry_output.stdout)?;
    assert_eq!(retry["changed"], false);
    assert_eq!(retry["status"], "queued");
    assert_eq!(retry["superseded_denied_approvals"], 1);

    let approvals: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(approvals[0]["status"], "superseded");
    Ok(())
}

#[test]
#[serial]
fn offdesk_resume_task_marks_resume_artifact_resumed_and_tick_launches() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let result_path = temp.path().join("resume-result.txt");
    let command = format!("printf done > {}", result_path.display());
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([durable_task(
            "resume_pending",
            now,
            command.as_str(),
            temp.path(),
        )]))?,
    )?;
    fs::write(
        profile_dir.join("task_resume_state.json"),
        serde_json::to_string_pretty(&json!([resume_state(now)]))?,
    )?;

    let resume_output = forager_command(temp.path())
        .args(["offdesk", "resume-task", "task", "--json"])
        .output()?;
    assert!(resume_output.status.success());
    let report: serde_json::Value = serde_json::from_slice(&resume_output.stdout)?;
    assert_eq!(report["changed"], true);
    assert_eq!(report["status"], "queued");

    let resume: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("task_resume_state.json"),
    )?)?;
    assert_eq!(resume[0]["status"], "resumed");

    let tick_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(tick_output.status.success());
    let tick: serde_json::Value = serde_json::from_slice(&tick_output.stdout)?;
    assert_eq!(tick["launched"], 1);
    Ok(())
}

#[test]
#[serial]
fn offdesk_abandon_task_cancels_resume_pending_and_prevents_dispatch() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([durable_task(
            "resume_pending",
            now,
            "true",
            temp.path(),
        )]))?,
    )?;
    fs::write(
        profile_dir.join("task_resume_state.json"),
        serde_json::to_string_pretty(&json!([resume_state(now)]))?,
    )?;

    let abandon_output = forager_command(temp.path())
        .args(["offdesk", "abandon-task", "task", "--json"])
        .output()?;
    assert!(abandon_output.status.success());
    let abandon: serde_json::Value = serde_json::from_slice(&abandon_output.stdout)?;
    assert_eq!(abandon["status"], "cancelled");

    let tick_output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;
    assert!(tick_output.status.success());
    let tick: serde_json::Value = serde_json::from_slice(&tick_output.stdout)?;
    assert_eq!(tick["launched"], 0);

    let resume: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("task_resume_state.json"),
    )?)?;
    assert_eq!(resume[0]["status"], "abandoned");
    Ok(())
}

#[test]
#[serial]
fn offdesk_snapshot_commands_report_verify_and_restore_plan() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    let snapshot_dir = profile_dir.join("mutation_snapshots");
    fs::create_dir_all(&snapshot_dir)?;
    let now = Utc::now();
    let target = temp.path().join("target.txt");
    let before_path = snapshot_dir.join("mutation_one.before");
    fs::write(&target, "after")?;
    fs::write(&before_path, "before")?;
    fs::write(
        snapshot_dir.join("mutation_one.json"),
        serde_json::to_string_pretty(&json!({
            "snapshot_schema_version": 1,
            "mutation_id": "mutation_one",
            "project_key": "project",
            "request_id": "request",
            "task_id": "task",
            "target_path": target.to_str().expect("utf-8 path"),
            "mutation_kind": "canonical_syncback",
            "target_exists_before": true,
            "before_size_bytes": 6,
            "before_hash": sha256_hex(b"before"),
            "before_excerpt_or_snapshot_path": before_path.to_str().expect("utf-8 path"),
            "snapshot_truncated": false,
            "rollback_available": true,
            "rollback_blockers": [],
            "diff_preview": "diff token=sk-secretsecretsecretsecret",
            "created_at": now,
            "created_by": "worker token=sk-secretsecretsecretsecret"
        }))?,
    )?;

    let list_output = forager_command(temp.path())
        .args(["offdesk", "snapshots", "--json"])
        .output()?;
    assert!(list_output.status.success());
    let list: serde_json::Value = serde_json::from_slice(&list_output.stdout)?;
    assert_eq!(list[0]["mutation_id"], "mutation_one");
    assert_eq!(list[0]["rollback_available"], true);

    let snapshot_output = forager_command(temp.path())
        .args(["offdesk", "snapshot", "mutation_one", "--json"])
        .output()?;
    assert!(snapshot_output.status.success());
    let verification: serde_json::Value = serde_json::from_slice(&snapshot_output.stdout)?;
    assert_eq!(verification["snapshot_present"], true);
    assert_eq!(verification["rollback_available"], true);
    assert_eq!(verification["target_current_matches_before"], false);
    assert!(!verification["snapshot"]["diff_preview"]
        .as_str()
        .expect("diff preview")
        .contains("sk-secretsecretsecretsecret"));

    let plan_output = forager_command(temp.path())
        .args(["offdesk", "restore-plan", "mutation_one", "--json"])
        .output()?;
    assert!(plan_output.status.success());
    let plan: serde_json::Value = serde_json::from_slice(&plan_output.stdout)?;
    assert_eq!(plan["operation"], "restore_file");
    assert_eq!(plan["rollback_available"], true);
    assert_eq!(fs::read_to_string(&target)?, "after");
    Ok(())
}

#[test]
#[serial]
fn offdesk_snapshot_unknown_id_reports_error() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .args(["offdesk", "snapshot", "mutation_missing", "--json"])
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Mutation snapshot not found: mutation_missing"));
    Ok(())
}

#[test]
#[serial]
fn status_json_includes_offdesk_counts() -> Result<()> {
    let temp = tempdir()?;

    let enqueue_output = forager_command(temp.path())
        .args([
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--cmd",
            "true",
            "--json",
        ])
        .output()?;
    assert!(enqueue_output.status.success());

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_output.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status_output.stdout)?;
    assert_eq!(status["profile"], "default");
    assert_eq!(status["profile_dir_source"], "primary");
    assert_eq!(
        reported_path(&status["profile_dir"]),
        expected_path(&profile_dir(temp.path()))
    );
    assert_eq!(
        reported_path(&status["app_dir"]),
        expected_path(&app_dir(temp.path()))
    );
    assert_eq!(status["app_dir_source"], "primary");
    assert_eq!(
        reported_path(&status["primary_app_dir"]),
        expected_path(&app_dir(temp.path()))
    );
    assert_eq!(status["primary_app_dir_exists"], true);
    assert_eq!(status["queued_offdesk_tasks"], 1);
    assert_eq!(status["pending_approvals"], 0);
    assert_eq!(status["failed_offdesk_tasks"], 0);
    assert_eq!(status["resume_pending_offdesk_tasks"], 0);
    assert_eq!(status["cancelled_offdesk_tasks"], 0);
    assert_eq!(status["closeout_required_offdesk_tasks"], 0);
    Ok(())
}

#[test]
#[serial]
fn offdesk_harnesses_lists_supported_and_planned_profiles() -> Result<()> {
    let temp = tempdir()?;

    let output = forager_command(temp.path())
        .args(["offdesk", "harnesses", "--json"])
        .output()?;

    assert!(output.status.success());
    let profiles: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    let profiles = profiles.as_array().expect("profiles array");
    assert!(profiles
        .iter()
        .any(|profile| { profile["id"] == "codex" && profile["support_status"] == "supported" }));
    assert!(profiles
        .iter()
        .any(|profile| { profile["id"] == "claude" && profile["support_status"] == "supported" }));
    let claude = profiles
        .iter()
        .find(|profile| profile["id"] == "claude")
        .expect("claude profile");
    assert_eq!(
        claude["prompt_contract"]["strategy"],
        "compact_prompt_with_first_read_artifacts"
    );
    assert_eq!(claude["prompt_contract"]["first_read_required"], true);
    assert_eq!(
        claude["prompt_contract"]["first_read_total_budget_bytes"],
        262_144
    );
    assert!(claude["prompt_contract"]["discouraged_inline_context"]
        .as_array()
        .expect("discouraged inline context")
        .iter()
        .any(|item| item == "full git diff"));
    assert!(profiles
        .iter()
        .any(|profile| { profile["id"] == "gemini" && profile["support_status"] == "planned" }));
    Ok(())
}

#[test]
#[serial]
fn offdesk_harness_prompt_builds_first_read_packet() -> Result<()> {
    let temp = tempdir()?;
    let first_read = temp.path().join("RETURN_PACKAGE.md");
    fs::write(&first_read, "return package")?;
    let output_path = temp.path().join("claude_start.md");
    let result_path = temp.path().join("result.json");

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "harness-prompt",
            "claude",
            "--task",
            "Review the smoke result and report only missing evidence.",
            "--first-read",
            first_read.to_str().expect("utf-8 first read"),
            "--result-artifact",
            result_path.to_str().expect("utf-8 result path"),
            "--workdir",
            temp.path().to_str().expect("utf-8 workdir"),
            "--output",
            output_path.to_str().expect("utf-8 output path"),
            "--json",
        ])
        .output()?;

    assert!(output.status.success());
    let packet: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(packet["harness_id"], "claude");
    assert_eq!(
        packet["prompt_strategy"],
        "compact_prompt_with_first_read_artifacts"
    );
    assert_eq!(packet["first_reads"][0]["present"], true);
    assert_eq!(packet["first_reads"][0]["size_bytes"], 14);
    assert_eq!(packet["first_reads"][0]["over_file_budget"], false);
    assert_eq!(packet["first_read_budget_status"], "ok");
    assert_eq!(packet["first_read_total_bytes"], 14);
    assert_eq!(
        packet["result_artifact"],
        result_path.to_str().expect("utf-8 result path")
    );
    assert_eq!(
        packet["output_path"],
        output_path.to_str().expect("utf-8 output path")
    );
    assert!(packet["warnings"]
        .as_array()
        .expect("warnings array")
        .is_empty());
    let prompt = fs::read_to_string(output_path)?;
    assert!(prompt.contains("## First-Read Artifacts"));
    assert!(prompt.contains("Do not ask the operator to paste full git diffs"));
    assert!(prompt.contains(first_read.to_str().expect("utf-8 first read")));
    Ok(())
}

#[test]
#[serial]
fn offdesk_harness_prompt_warns_and_strict_fails_on_budget() -> Result<()> {
    let temp = tempdir()?;
    let first_read = temp.path().join("large.md");
    fs::write(&first_read, "0123456789")?;

    let warning_output = forager_command(temp.path())
        .args([
            "offdesk",
            "harness-prompt",
            "claude",
            "--task",
            "Review first-read budget.",
            "--first-read",
            first_read.to_str().expect("utf-8 first read"),
            "--max-first-read-total-bytes",
            "5",
            "--json",
        ])
        .output()?;

    assert!(warning_output.status.success());
    let packet: serde_json::Value = serde_json::from_slice(&warning_output.stdout)?;
    assert_eq!(packet["first_read_total_bytes"], 10);
    assert_eq!(packet["first_read_total_budget_bytes"], 5);
    assert_eq!(packet["first_read_budget_status"], "warning");
    assert!(packet["warnings"]
        .as_array()
        .expect("warnings array")
        .iter()
        .any(|warning| warning
            .as_str()
            .expect("warning text")
            .contains("first-read artifacts total 10 bytes")));

    let strict_output = forager_command(temp.path())
        .args([
            "offdesk",
            "harness-prompt",
            "claude",
            "--task",
            "Review first-read budget.",
            "--first-read",
            first_read.to_str().expect("utf-8 first read"),
            "--max-first-read-total-bytes",
            "5",
            "--strict-first-read-budget",
            "--json",
        ])
        .output()?;

    assert!(!strict_output.status.success());
    assert!(
        String::from_utf8_lossy(&strict_output.stderr).contains("first-read budget guard failed")
    );
    Ok(())
}

#[test]
#[serial]
fn offdesk_plan_registers_multiturn_plan_without_runtime_authority() -> Result<()> {
    let temp = tempdir()?;
    let input_path = temp.path().join("OVERNIGHT_PLAN.json");
    let plan = json!({
        "schema": "offdesk_multiturn_plan.v1",
        "profile_key": "generic",
        "profile_name": "Generic Offdesk Planning",
        "decision": {
            "ready_for_operator_review": true,
            "ready_for_launch_preparation": false,
            "ready_for_enqueue": false,
            "reason": "Operator review is required before any launch preparation."
        },
        "execution_sequence": [
            {
                "id": "phase_1",
                "objective": "Review evidence before any runtime work.",
                "stop_condition": "Stop at a human-readable plan artifact."
            }
        ],
        "authority": {
            "read_only_plan": true,
            "does_not_authorize": [
                "enqueue",
                "launch",
                "approval",
                "file movement",
                "archive",
                "delete",
                "wiki promotion",
                "accepted truth"
            ]
        }
    });
    let input_bytes = serde_json::to_vec_pretty(&plan)?;
    fs::write(&input_path, &input_bytes)?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan",
            input_path.to_str().expect("utf-8 plan path"),
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--json",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let registration: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(registration["schema"], "offdesk_plan_registration.v1");
    assert_eq!(registration["artifact_kind"], "offdesk_multiturn_plan");
    assert_eq!(registration["plan_schema"], "offdesk_multiturn_plan.v1");
    assert_eq!(registration["profile_key"], "generic");
    assert_eq!(registration["project_key"], "project");
    assert_eq!(registration["request_id"], "request");
    assert_eq!(registration["task_id"], "task");
    assert_eq!(registration["ready_for_operator_review"], true);
    assert_eq!(registration["ready_for_launch_preparation"], false);
    assert_eq!(registration["ready_for_enqueue"], false);
    assert_eq!(registration["source_sha256"], sha256_hex(&input_bytes));
    assert!(registration["does_not_authorize"]
        .as_array()
        .expect("denials array")
        .iter()
        .any(|item| item == "enqueue"));

    let registration_path = registration["artifacts"]["registration_json"]
        .as_str()
        .expect("registration path");
    let copied_source_path = registration["artifacts"]["copied_source_json"]
        .as_str()
        .expect("source copy path");
    let saved_registration: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(registration_path)?)?;
    assert_eq!(
        saved_registration["source_sha256"],
        registration["source_sha256"]
    );
    let copied_source: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(copied_source_path)?)?;
    assert_eq!(copied_source["schema"], "offdesk_multiturn_plan.v1");
    assert!(profile_dir(temp.path()).join("offdesk_plans").exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_plan_rejects_enqueue_ready_multiturn_plan() -> Result<()> {
    let temp = tempdir()?;
    let input_path = temp.path().join("OVERNIGHT_PLAN.json");
    fs::write(
        &input_path,
        serde_json::to_vec_pretty(&json!({
            "schema": "offdesk_multiturn_plan.v1",
            "profile_key": "generic",
            "decision": {
                "ready_for_operator_review": true,
                "ready_for_launch_preparation": false,
                "ready_for_enqueue": true
            },
            "execution_sequence": [
                {
                    "id": "phase_1",
                    "objective": "This should remain a plan only."
                }
            ],
            "authority": {
                "read_only_plan": true,
                "does_not_authorize": [
                    "enqueue",
                    "launch",
                    "approval",
                    "file movement",
                    "archive",
                    "delete",
                    "wiki promotion",
                    "accepted truth"
                ]
            }
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan",
            input_path.to_str().expect("utf-8 plan path"),
            "--json",
        ])
        .output()?;

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("decision.ready_for_enqueue_must_be_false"));
    assert!(!profile_dir(temp.path()).join("offdesk_plans").exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_plan_dry_run_accepts_planner_council_without_writing() -> Result<()> {
    let temp = tempdir()?;
    let input_path = temp.path().join("planner_council_result.json");
    fs::write(
        &input_path,
        serde_json::to_vec_pretty(&json!({
            "schema": "offdesk_planner_council.v1",
            "profile_key": "generic",
            "profile_name": "Generic Offdesk Planning",
            "consensus": {
                "decision": "ready_for_operator_review",
                "agreement": true,
                "ready_for_operator_review": true,
                "ready_for_launch_preparation": false,
                "ready_for_enqueue": false,
                "selected_planner": "planner_a"
            },
            "validation_failures": [],
            "synthesized_plan_path": temp.path().join("SYNTHESIZED_PLAN.json").display().to_string()
        }))?,
    )?;

    let output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan",
            input_path.to_str().expect("utf-8 council path"),
            "--dry-run",
            "--json",
        ])
        .output()?;

    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let registration: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(registration["artifact_kind"], "offdesk_planner_council");
    assert_eq!(registration["plan_schema"], "offdesk_planner_council.v1");
    assert_eq!(registration["dry_run"], true);
    assert_eq!(registration["ready_for_operator_review"], true);
    assert_eq!(registration["ready_for_launch_preparation"], false);
    assert_eq!(registration["ready_for_enqueue"], false);
    assert_eq!(registration["consensus"]["selected_planner"], "planner_a");
    assert!(registration["selected_plan_path"]
        .as_str()
        .expect("selected plan path")
        .ends_with("SYNTHESIZED_PLAN.json"));
    assert!(registration["artifacts"]["registration_json"].is_null());
    assert!(!profile_dir(temp.path()).join("offdesk_plans").exists());
    Ok(())
}

#[test]
#[serial]
fn offdesk_plans_list_and_plan_show_registered_artifact() -> Result<()> {
    let temp = tempdir()?;
    let input_path = temp.path().join("OVERNIGHT_PLAN.json");
    let plan = json!({
        "schema": "offdesk_multiturn_plan.v1",
        "profile_key": "generic",
        "decision": {
            "ready_for_operator_review": true,
            "ready_for_launch_preparation": false,
            "ready_for_enqueue": false
        },
        "execution_sequence": [
            {
                "id": "phase_1",
                "objective": "Register and inspect this plan."
            }
        ],
        "authority": {
            "read_only_plan": true,
            "does_not_authorize": [
                "enqueue",
                "launch",
                "approval",
                "file movement",
                "archive",
                "delete",
                "wiki promotion",
                "accepted truth"
            ]
        }
    });
    let input_bytes = serde_json::to_vec_pretty(&plan)?;
    fs::write(&input_path, &input_bytes)?;

    let register_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan",
            input_path.to_str().expect("utf-8 plan path"),
            "--project-key",
            "project",
            "--task-id",
            "task",
            "--json",
        ])
        .output()?;
    assert!(
        register_output.status.success(),
        "{}",
        String::from_utf8_lossy(&register_output.stderr)
    );

    let list_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plans",
            "--project-key",
            "project",
            "--latest",
            "--json",
        ])
        .output()?;
    assert!(
        list_output.status.success(),
        "{}",
        String::from_utf8_lossy(&list_output.stderr)
    );
    let plans: serde_json::Value = serde_json::from_slice(&list_output.stdout)?;
    let plans = plans.as_array().expect("plans array");
    assert_eq!(plans.len(), 1);
    let plan_id = plans[0]["plan_id"].as_str().expect("plan id");
    assert_eq!(plans[0]["registration"]["project_key"], "project");
    assert_eq!(plans[0]["registration"]["task_id"], "task");
    assert_eq!(
        plans[0]["registration"]["source_sha256"],
        sha256_hex(&input_bytes)
    );
    assert_eq!(plans[0]["registration"]["ready_for_enqueue"], false);

    let show_output = forager_command(temp.path())
        .args(["offdesk", "plan-show", plan_id, "--json"])
        .output()?;
    assert!(
        show_output.status.success(),
        "{}",
        String::from_utf8_lossy(&show_output.stderr)
    );
    let shown: serde_json::Value = serde_json::from_slice(&show_output.stdout)?;
    assert_eq!(shown["plan_id"], plan_id);
    assert_eq!(
        shown["registration"]["artifact_kind"],
        "offdesk_multiturn_plan"
    );
    assert_eq!(
        shown["registration"]["source_sha256"],
        sha256_hex(&input_bytes)
    );
    assert!(shown["registration"]["does_not_authorize"]
        .as_array()
        .expect("denials array")
        .iter()
        .any(|item| item == "launch"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_plan_review_records_decision_without_runtime_authority() -> Result<()> {
    let temp = tempdir()?;
    let input_path = temp.path().join("OVERNIGHT_PLAN.json");
    let plan = json!({
        "schema": "offdesk_multiturn_plan.v1",
        "profile_key": "generic",
        "decision": {
            "ready_for_operator_review": true,
            "ready_for_launch_preparation": false,
            "ready_for_enqueue": false
        },
        "execution_sequence": [
            {
                "id": "phase_1",
                "objective": "Record an operator review."
            }
        ],
        "authority": {
            "read_only_plan": true,
            "does_not_authorize": [
                "enqueue",
                "launch",
                "approval",
                "file movement",
                "archive",
                "delete",
                "wiki promotion",
                "accepted truth"
            ]
        }
    });
    fs::write(&input_path, serde_json::to_vec_pretty(&plan)?)?;

    let register_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan",
            input_path.to_str().expect("utf-8 plan path"),
            "--project-key",
            "project",
            "--json",
        ])
        .output()?;
    assert!(
        register_output.status.success(),
        "{}",
        String::from_utf8_lossy(&register_output.stderr)
    );
    let registration: serde_json::Value = serde_json::from_slice(&register_output.stdout)?;
    let registry_dir = registration["artifacts"]["registry_dir"]
        .as_str()
        .expect("registry dir");
    let plan_id = Path::new(registry_dir)
        .file_name()
        .expect("registry dir name")
        .to_string_lossy()
        .to_string();

    let review_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan-review",
            &plan_id,
            "--decision",
            "approved",
            "--reviewer",
            "operator",
            "--reason",
            "The plan is ready for a separate launch-preparation packet.",
            "--follow-up",
            "Prepare launch packet in a separate command.",
            "--json",
        ])
        .output()?;
    assert!(
        review_output.status.success(),
        "{}",
        String::from_utf8_lossy(&review_output.stderr)
    );
    let review: serde_json::Value = serde_json::from_slice(&review_output.stdout)?;
    assert_eq!(review["schema"], "offdesk_plan_review.v1");
    assert_eq!(review["plan_id"], plan_id);
    assert_eq!(review["decision"], "approved");
    assert_eq!(review["ready_for_launch_preparation_candidate"], true);
    assert_eq!(review["ready_for_enqueue"], false);
    assert_eq!(review["applies_file_operations"], false);
    assert!(review["does_not_authorize"]
        .as_array()
        .expect("review denials")
        .iter()
        .any(|item| item == "launch"));
    let review_record_path = review["artifacts"]["review_record_json"]
        .as_str()
        .expect("review record path");
    assert!(Path::new(review_record_path).exists());

    let list_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plans",
            "--project-key",
            "project",
            "--latest",
            "--json",
        ])
        .output()?;
    assert!(
        list_output.status.success(),
        "{}",
        String::from_utf8_lossy(&list_output.stderr)
    );
    let plans: serde_json::Value = serde_json::from_slice(&list_output.stdout)?;
    assert_eq!(plans[0]["review_state"]["status"], "approved");
    assert_eq!(
        plans[0]["review_state"]["ready_for_launch_preparation_candidate"],
        true
    );
    assert_eq!(
        plans[0]["review_state"]["next_safe_action"],
        "prepare_launch_packet"
    );
    assert_eq!(plans[0]["review_count"], 1);
    assert_eq!(plans[0]["latest_review"]["review_id"], review["review_id"]);

    let show_output = forager_command(temp.path())
        .args(["offdesk", "plan-show", &plan_id, "--json"])
        .output()?;
    assert!(
        show_output.status.success(),
        "{}",
        String::from_utf8_lossy(&show_output.stderr)
    );
    let shown: serde_json::Value = serde_json::from_slice(&show_output.stdout)?;
    assert_eq!(shown["reviews"].as_array().expect("reviews").len(), 1);
    assert_eq!(shown["reviews"][0]["review_id"], review["review_id"]);
    assert_eq!(shown["registration"]["ready_for_enqueue"], false);

    let blocked_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan-review",
            &plan_id,
            "--decision",
            "approved",
            "--reviewer",
            "operator",
            "--reason",
            "This should fail because approved reviews cannot carry blockers.",
            "--blocker",
            "missing launch packet",
            "--json",
        ])
        .output()?;
    assert!(!blocked_output.status.success());
    assert!(String::from_utf8_lossy(&blocked_output.stderr)
        .contains("approved Offdesk plan review cannot include blockers"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_plan_launch_prep_requires_approved_review_and_stays_read_only() -> Result<()> {
    let temp = tempdir()?;
    let input_path = temp.path().join("OVERNIGHT_PLAN.json");
    let plan = json!({
        "schema": "offdesk_multiturn_plan.v1",
        "profile_key": "generic",
        "decision": {
            "ready_for_operator_review": true,
            "ready_for_launch_preparation": false,
            "ready_for_enqueue": false
        },
        "execution_sequence": [
            {
                "id": "phase_1",
                "objective": "Prepare a launch-prep packet only."
            }
        ],
        "authority": {
            "read_only_plan": true,
            "does_not_authorize": [
                "enqueue",
                "launch",
                "approval",
                "file movement",
                "archive",
                "delete",
                "wiki promotion",
                "accepted truth"
            ]
        }
    });
    let input_bytes = serde_json::to_vec_pretty(&plan)?;
    fs::write(&input_path, &input_bytes)?;

    let register_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan",
            input_path.to_str().expect("utf-8 plan path"),
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--json",
        ])
        .output()?;
    assert!(
        register_output.status.success(),
        "{}",
        String::from_utf8_lossy(&register_output.stderr)
    );
    let registration: serde_json::Value = serde_json::from_slice(&register_output.stdout)?;
    let registry_dir = registration["artifacts"]["registry_dir"]
        .as_str()
        .expect("registry dir");
    let plan_id = Path::new(registry_dir)
        .file_name()
        .expect("registry dir name")
        .to_string_lossy()
        .to_string();

    let blocked_output = forager_command(temp.path())
        .args(["offdesk", "plan-launch-prep", &plan_id, "--json"])
        .output()?;
    assert!(!blocked_output.status.success());
    assert!(String::from_utf8_lossy(&blocked_output.stderr)
        .contains("launch-prep requires an approved review"));

    let review_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan-review",
            &plan_id,
            "--decision",
            "approved",
            "--reason",
            "Approved only for a separate launch-preparation packet.",
            "--json",
        ])
        .output()?;
    assert!(
        review_output.status.success(),
        "{}",
        String::from_utf8_lossy(&review_output.stderr)
    );
    let review: serde_json::Value = serde_json::from_slice(&review_output.stdout)?;

    let revision_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan-review",
            &plan_id,
            "--decision",
            "revision-required",
            "--reason",
            "The latest review requests a revision.",
            "--blocker",
            "Clarify the execution boundary.",
            "--json",
        ])
        .output()?;
    assert!(
        revision_output.status.success(),
        "{}",
        String::from_utf8_lossy(&revision_output.stderr)
    );

    let latest_blocked_output = forager_command(temp.path())
        .args(["offdesk", "plan-launch-prep", &plan_id, "--json"])
        .output()?;
    assert!(!latest_blocked_output.status.success());
    assert!(String::from_utf8_lossy(&latest_blocked_output.stderr)
        .contains("launch-prep requires an approved review"));

    let prep_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan-launch-prep",
            &plan_id,
            "--review-id",
            review["review_id"].as_str().expect("approved review id"),
            "--prepared-by",
            "operator",
            "--notes",
            "Prepare execution brief next; do not dispatch here.",
            "--json",
        ])
        .output()?;
    assert!(
        prep_output.status.success(),
        "{}",
        String::from_utf8_lossy(&prep_output.stderr)
    );
    let prep: serde_json::Value = serde_json::from_slice(&prep_output.stdout)?;
    assert_eq!(prep["schema"], "offdesk_plan_launch_prep.v1");
    assert_eq!(prep["plan_id"], plan_id);
    assert_eq!(prep["source_sha256"], sha256_hex(&input_bytes));
    assert_eq!(prep["review_id"], review["review_id"]);
    assert_eq!(prep["launch_preparation_candidate"], true);
    assert_eq!(prep["ready_for_launch"], false);
    assert_eq!(prep["ready_for_enqueue"], false);
    assert_eq!(prep["applies_file_operations"], false);
    assert_eq!(prep["project_key"], "project");
    assert_eq!(prep["request_id"], "request");
    assert_eq!(prep["task_id"], "task");
    assert!(prep["required_first_reads"]
        .as_array()
        .expect("first reads")
        .iter()
        .any(|item| item.as_str() == review["artifacts"]["review_record_json"].as_str()));
    assert!(prep["does_not_authorize"]
        .as_array()
        .expect("prep denials")
        .iter()
        .any(|item| item == "dispatch"));
    let prep_path = prep["artifacts"]["launch_prep_json"]
        .as_str()
        .expect("prep path");
    assert!(Path::new(prep_path).exists());

    let list_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plans",
            "--project-key",
            "project",
            "--latest",
            "--json",
        ])
        .output()?;
    assert!(
        list_output.status.success(),
        "{}",
        String::from_utf8_lossy(&list_output.stderr)
    );
    let plans: serde_json::Value = serde_json::from_slice(&list_output.stdout)?;
    assert_eq!(plans[0]["launch_prep_count"], 1);
    assert_eq!(plans[0]["latest_launch_prep"]["prep_id"], prep["prep_id"]);

    let show_output = forager_command(temp.path())
        .args(["offdesk", "plan-show", &plan_id, "--json"])
        .output()?;
    assert!(
        show_output.status.success(),
        "{}",
        String::from_utf8_lossy(&show_output.stderr)
    );
    let shown: serde_json::Value = serde_json::from_slice(&show_output.stdout)?;
    assert_eq!(
        shown["launch_preps"]
            .as_array()
            .expect("launch preps")
            .len(),
        1
    );
    assert_eq!(shown["latest_launch_prep"]["prep_id"], prep["prep_id"]);
    assert_eq!(shown["registration"]["ready_for_enqueue"], false);
    Ok(())
}

#[test]
#[serial]
fn offdesk_remote_operator_plans_and_show_are_read_only() -> Result<()> {
    let temp = tempdir()?;
    let input_path = temp.path().join("OVERNIGHT_PLAN.json");
    fs::write(
        &input_path,
        serde_json::to_vec_pretty(&json!({
            "schema": "offdesk_multiturn_plan.v1",
            "profile_key": "generic",
            "decision": {
                "ready_for_operator_review": true,
                "ready_for_launch_preparation": false,
                "ready_for_enqueue": false
            },
            "execution_sequence": [
                {
                    "id": "phase_1",
                    "objective": "Expose this plan to a read-only remote operator surface."
                }
            ],
            "authority": {
                "read_only_plan": true,
                "does_not_authorize": [
                    "enqueue",
                    "launch",
                    "approval",
                    "file movement",
                    "archive",
                    "delete",
                    "wiki promotion",
                    "accepted truth"
                ]
            }
        }))?,
    )?;

    let register_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan",
            input_path.to_str().expect("utf-8 plan path"),
            "--project-key",
            "project",
            "--request-id",
            "request",
            "--task-id",
            "task",
            "--json",
        ])
        .output()?;
    assert!(
        register_output.status.success(),
        "{}",
        String::from_utf8_lossy(&register_output.stderr)
    );
    let registration: serde_json::Value = serde_json::from_slice(&register_output.stdout)?;
    let registry_dir = registration["artifacts"]["registry_dir"]
        .as_str()
        .expect("registry dir");
    let plan_id = Path::new(registry_dir)
        .file_name()
        .expect("registry dir name")
        .to_string_lossy()
        .to_string();

    let review_output = forager_command(temp.path())
        .args([
            "offdesk",
            "plan-review",
            &plan_id,
            "--decision",
            "approved",
            "--reason",
            "Ready for read-only remote inspection.",
            "--json",
        ])
        .output()?;
    assert!(
        review_output.status.success(),
        "{}",
        String::from_utf8_lossy(&review_output.stderr)
    );

    let plans_output = forager_command(temp.path())
        .args([
            "offdesk",
            "remote-operator",
            "plans",
            "--project-key",
            "project",
            "--latest",
            "--json",
        ])
        .output()?;
    assert!(
        plans_output.status.success(),
        "{}",
        String::from_utf8_lossy(&plans_output.stderr)
    );
    let plans: serde_json::Value = serde_json::from_slice(&plans_output.stdout)?;
    assert_eq!(plans["schema"], "remote_operator_readonly_projection.v1");
    assert_eq!(plans["command"], "plans");
    assert_eq!(plans["transport"], "telegram");
    assert_eq!(plans["read_only"], true);
    assert_eq!(plans["mutation_authorized"], false);
    assert_eq!(plans["approval_authorized"], false);
    assert!(plans["forbidden_remote_intents"]
        .as_array()
        .expect("forbidden intents")
        .iter()
        .any(|item| item == "approve_plan"));
    assert_eq!(plans["payload"]["plan_count"], 1);
    assert_eq!(plans["payload"]["plans"][0]["plan_id"], plan_id);
    assert_eq!(plans["payload"]["plans"][0]["review_status"], "approved");
    assert_eq!(plans["payload"]["plans"][0]["ready_for_enqueue"], false);
    assert!(plans["payload"]["plans"][0]["observed_hash"]
        .as_str()
        .expect("plan observed hash")
        .starts_with("sha256:"));
    assert!(plans["payload"]["plans"][0]["remote_actions"]
        .as_array()
        .expect("remote actions")
        .iter()
        .any(|item| item == "inspect_plan"));

    let show_output = forager_command(temp.path())
        .args(["offdesk", "remote-operator", "show", &plan_id, "--json"])
        .output()?;
    assert!(
        show_output.status.success(),
        "{}",
        String::from_utf8_lossy(&show_output.stderr)
    );
    let shown: serde_json::Value = serde_json::from_slice(&show_output.stdout)?;
    assert_eq!(shown["schema"], "remote_operator_readonly_projection.v1");
    assert_eq!(shown["command"], "show");
    assert_eq!(shown["payload"]["plan"]["plan_id"], plan_id);
    assert_eq!(
        shown["payload"]["reviews"]
            .as_array()
            .expect("review summaries")
            .len(),
        1
    );
    assert_eq!(
        shown["payload"]["launch_preps"].as_array().unwrap().len(),
        0
    );
    assert!(shown["payload"]["does_not_authorize"]
        .as_array()
        .expect("denials")
        .iter()
        .any(|item| item == "launch"));
    assert!(shown["card"]["disabled_remote_actions"]
        .as_array()
        .expect("disabled actions")
        .iter()
        .any(|item| item == "dispatch"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_remote_operator_pending_is_read_only_and_does_not_expire() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("pending_action_approvals.json"),
        serde_json::to_string_pretty(&json!([
            {
                "approval_id": "approval_stale",
                "status": "pending",
                "scope": "once",
                "project_key": "project",
                "request_id": "request",
                "task_id": "task",
                "action": "dispatch.runtime",
                "risk_level": "runtime_mutation",
                "approval_mode": "operator_required",
                "preview": "safe preview",
                "reason": "outside envelope",
                "created_at": now - Duration::minutes(20),
                "expires_at": now - Duration::minutes(10),
                "source_surface": "test"
            }
        ]))?,
    )?;

    let pending_output = forager_command(temp.path())
        .args(["offdesk", "remote-operator", "pending", "--json"])
        .output()?;
    assert!(
        pending_output.status.success(),
        "{}",
        String::from_utf8_lossy(&pending_output.stderr)
    );
    let pending: serde_json::Value = serde_json::from_slice(&pending_output.stdout)?;
    assert_eq!(pending["schema"], "remote_operator_readonly_projection.v1");
    assert_eq!(pending["command"], "pending");
    assert_eq!(pending["read_only"], true);
    assert_eq!(pending["mutation_authorized"], false);
    assert_eq!(pending["approval_authorized"], false);
    assert_eq!(pending["payload"]["approval_count"], 1);
    assert_eq!(
        pending["payload"]["approvals"][0]["approval_id"],
        "approval_stale"
    );
    assert_eq!(pending["payload"]["approvals"][0]["status"], "pending");
    assert_eq!(pending["payload"]["approvals"][0]["expired"], true);
    assert_eq!(
        pending["payload"]["approvals"][0]["next_safe_action"]["kind"],
        "approval_expired"
    );
    assert!(pending["payload"]["approvals"][0]["remote_actions"]
        .as_array()
        .expect("remote actions")
        .iter()
        .any(|item| item == "inspect_approval"));
    assert!(pending["forbidden_remote_intents"]
        .as_array()
        .expect("forbidden intents")
        .iter()
        .any(|item| item == "approve_launch"));

    let stored: serde_json::Value = serde_json::from_str(&fs::read_to_string(
        profile_dir.join("pending_action_approvals.json"),
    )?)?;
    assert_eq!(stored[0]["status"], "pending");
    Ok(())
}

#[test]
#[serial]
fn status_json_reports_legacy_profile_dir_when_compat_storage_is_active() -> Result<()> {
    let temp = tempdir()?;
    fs::create_dir_all(legacy_app_dir(temp.path()).join("profiles").join("default"))?;

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_output.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status_output.stdout)?;
    assert_eq!(status["profile"], "default");
    assert_eq!(status["profile_dir_source"], "legacy");
    assert_eq!(
        reported_path(&status["profile_dir"]),
        expected_path(&legacy_app_dir(temp.path()).join("profiles").join("default"))
    );
    assert_eq!(
        reported_path(&status["app_dir"]),
        expected_path(&legacy_app_dir(temp.path()))
    );
    assert_eq!(status["app_dir_source"], "legacy");
    assert_eq!(
        reported_path(&status["primary_app_dir"]),
        expected_path(&app_dir(temp.path()))
    );
    assert_eq!(status["primary_app_dir_exists"], false);
    assert!(!app_dir(temp.path()).exists());
    Ok(())
}

#[test]
#[serial]
fn status_json_includes_offdesk_recovery_counts() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            durable_task_with(
                "failed-task",
                "dispatch.runtime",
                "failed",
                now,
                "true",
                temp.path(),
            ),
            durable_task_with(
                "resume-task",
                "dispatch.runtime",
                "resume_pending",
                now,
                "true",
                temp.path(),
            ),
            durable_task_with(
                "cancelled-task",
                "dispatch.runtime",
                "cancelled",
                now,
                "true",
                temp.path(),
            )
        ]))?,
    )?;

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_output.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status_output.stdout)?;
    assert_eq!(status["failed_offdesk_tasks"], 1);
    assert_eq!(status["resume_pending_offdesk_tasks"], 1);
    assert_eq!(status["cancelled_offdesk_tasks"], 1);
    assert_eq!(status["closeout_required_offdesk_tasks"], 0);
    Ok(())
}

#[test]
#[serial]
fn status_json_includes_resume_store_next_safe_action() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("task_resume_state.json"),
        serde_json::to_string_pretty(&json!([resume_state(now)]))?,
    )?;

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_output.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status_output.stdout)?;
    assert_eq!(status["resume_pending_fresh"], 1);
    assert_eq!(
        status["offdesk_next_safe_actions"][0]["kind"],
        "resume_review_required"
    );
    assert_eq!(
        status["offdesk_next_safe_actions"][0]["commands"][0],
        "forager offdesk resume"
    );
    assert_eq!(
        status["offdesk_next_safe_actions"][0]["requires_operator_review"],
        true
    );
    Ok(())
}

#[test]
#[serial]
fn status_json_orders_resume_store_next_safe_action_by_priority() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([
            durable_task_with(
                "approval-task",
                "dispatch.runtime",
                "pending_approval",
                now,
                "true",
                temp.path(),
            ),
            durable_task_with(
                "completed-task",
                "dispatch.runtime",
                "completed",
                now,
                "true",
                temp.path(),
            ),
            durable_task_with(
                "running-task",
                "dispatch.runtime",
                "running",
                now,
                "true",
                temp.path(),
            ),
            durable_task_with(
                "queued-task",
                "dispatch.runtime",
                "queued",
                now,
                "true",
                temp.path(),
            )
        ]))?,
    )?;
    fs::write(
        profile_dir.join("task_resume_state.json"),
        serde_json::to_string_pretty(&json!([resume_state(now)]))?,
    )?;

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_output.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status_output.stdout)?;
    let action_kinds: Vec<&str> = status["offdesk_next_safe_actions"]
        .as_array()
        .expect("offdesk next safe actions")
        .iter()
        .map(|action| action["kind"].as_str().expect("action kind"))
        .collect();

    assert_eq!(
        action_kinds,
        vec![
            "approval_pending",
            "resume_review_required",
            "review_required",
            "runtime_monitoring",
            "dispatch_pending",
        ]
    );
    Ok(())
}

#[test]
#[serial]
fn status_json_includes_closeout_required_count() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let now = Utc::now();
    fs::write(
        profile_dir.join("offdesk_tasks.json"),
        serde_json::to_string_pretty(&json!([durable_task_with(
            "completed-task",
            "dispatch.runtime",
            "completed",
            now,
            "true",
            temp.path(),
        )]))?,
    )?;

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_output.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status_output.stdout)?;
    assert_eq!(status["closeout_required_offdesk_tasks"], 1);
    assert_eq!(status["closeout_state"]["missing_closeout"], 1);
    assert_eq!(status["closeout_state"]["pending_review"], 0);
    assert_eq!(status["closeout_state"]["revision_required"], 0);
    assert!(status["offdesk_next_safe_actions"]
        .as_array()
        .expect("offdesk next safe actions")
        .iter()
        .any(|action| action["kind"] == "review_required"));

    let status_output = forager_command(temp.path()).args(["status"]).output()?;
    assert!(status_output.status.success());
    let stdout = String::from_utf8_lossy(&status_output.stdout);
    assert!(stdout.contains("1 closeout required"));
    assert!(stdout.contains("Closeout state: 1 missing, 0 pending review, 0 revise/blocked"));
    assert!(stdout.contains("Closeout: run `forager offdesk closeout`"));
    assert!(stdout.contains("Next safe actions:"));
    assert!(stdout.contains("forager ondesk prompt-package"));

    let closeout_dir = profile_dir.join("offdesk_closeouts").join("latest");
    fs::create_dir_all(&closeout_dir)?;
    fs::write(
        closeout_dir.join("closeout_review_20260521T000000Z.json"),
        serde_json::to_string_pretty(&json!({
            "reviewed_at": now + Duration::minutes(1),
            "verdict": "approved",
            "applies_to_tasks": [
                {
                    "project_key": "project",
                    "request_id": "request",
                    "task_id": "completed-task"
                }
            ]
        }))?,
    )?;

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_output.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status_output.stdout)?;
    assert_eq!(status["closeout_required_offdesk_tasks"], 0);
    assert_eq!(status["closeout_state"]["approved"], 1);

    fs::write(
        closeout_dir.join("closeout_review_20260521T000100Z.json"),
        serde_json::to_string_pretty(&json!({
            "reviewed_at": now + Duration::minutes(2),
            "verdict": "approved",
            "closeout_receipt": {
                "schema": "closeout_receipt.v1",
                "acceptance_status": "approved_with_followups"
            },
            "applies_to_tasks": [
                {
                    "project_key": "project",
                    "request_id": "request",
                    "task_id": "completed-task"
                }
            ]
        }))?,
    )?;

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_output.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status_output.stdout)?;
    assert_eq!(status["closeout_required_offdesk_tasks"], 1);
    assert_eq!(status["closeout_state"]["approved_with_followups"], 1);
    assert_eq!(status["closeout_state"]["accepted"], 0);
    assert!(status["offdesk_next_safe_actions"]
        .as_array()
        .expect("offdesk next safe actions")
        .iter()
        .any(|action| action["detail"]
            .as_str()
            .unwrap_or_default()
            .contains("receipt follow-ups")));

    let status_output = forager_command(temp.path()).args(["status"]).output()?;
    assert!(status_output.status.success());
    let stdout = String::from_utf8_lossy(&status_output.stdout);
    assert!(stdout.contains("1 approved with follow-ups"));
    assert!(stdout.contains("Closeout receipt: approved review still has follow-ups"));

    fs::write(
        closeout_dir.join("closeout_review_20260521T000200Z.json"),
        serde_json::to_string_pretty(&json!({
            "reviewed_at": now + Duration::minutes(3),
            "verdict": "approved",
            "closeout_receipt": {
                "schema": "closeout_receipt.v1",
                "acceptance_status": "accepted"
            },
            "applies_to_tasks": [
                {
                    "project_key": "project",
                    "request_id": "request",
                    "task_id": "completed-task"
                }
            ]
        }))?,
    )?;

    let status_output = forager_command(temp.path())
        .args(["status", "--json"])
        .output()?;
    assert!(status_output.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status_output.stdout)?;
    assert_eq!(status["closeout_required_offdesk_tasks"], 0);
    assert_eq!(status["closeout_state"]["accepted"], 1);
    assert_eq!(status["closeout_state"]["approved"], 1);
    Ok(())
}

#[test]
#[serial]
fn offdesk_tick_rejects_concurrent_lock() -> Result<()> {
    let temp = tempdir()?;
    let profile_dir = profile_dir(temp.path());
    fs::create_dir_all(&profile_dir)?;
    let lock_path = profile_dir.join("offdesk_tick.lock");
    let lock_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)?;
    FileExt::lock_exclusive(&lock_file)?;

    let output = forager_command(temp.path())
        .args(["offdesk", "tick", "--json"])
        .output()?;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("offdesk tick already running"));
    FileExt::unlock(&lock_file)?;
    Ok(())
}

#[test]
fn workload_review_allows_wiki_candidate_queue_exception() -> Result<()> {
    let temp = tempdir()?;
    let repo_dir = temp.path().join("repo");
    let out_dir = temp.path().join("out");
    fs::create_dir_all(&repo_dir)?;
    fs::create_dir_all(&out_dir)?;
    fs::write(out_dir.join("run_workload.sh"), "#!/usr/bin/env bash\n")?;
    fs::write(out_dir.join("evidence_bundle.json"), "{}\n")?;
    fs::write(
        out_dir.join("evidence_review.json"),
        serde_json::to_string_pretty(&json!({
            "kind": "evidence_bundle_review",
            "passed": true,
            "decision": "sufficient"
        }))?,
    )?;

    let manifest_path = out_dir.join("prepared_task.json");
    let review_path = out_dir.join("workload_review").join("results.json");
    let manifest = json!({
        "repo": repo_dir,
        "out_dir": out_dir,
        "duration_minutes": 10,
        "max_iterations": 1,
        "model": "qwen3-coder-next:latest",
        "schedule": {
            "mode": "run_until_local",
            "duration_minutes": 10,
            "target_at": "2026-05-22T09:00:00+09:00",
            "timezone": "Asia/Seoul"
        },
        "safety": {
            "repo_read_only": true,
            "writes_only_under_out_dir": false,
            "writes_only_under_out_dir_except_adaptive_wiki_candidate_queue": true,
            "adaptive_wiki_candidate_queue_write": true,
            "model_responses_not_executed": true,
            "no_file_deletion_or_cleanup": true,
            "no_reboot_shutdown_or_power_state_change": true,
            "no_service_restart_or_system_config_change": true,
            "no_storage_raid_nvme_or_mount_change": true,
            "no_package_install_or_permission_change": true,
            "no_process_termination_or_runner_interference": true,
            "no_network_firewall_or_remote_access_change": true,
            "no_kernel_driver_firmware_or_bios_change": true,
            "operator_approval_required_for_system_mutation": true,
            "capability": "dispatch.runtime",
            "runner": "local-tmux",
            "approval_required_before_dispatch": true,
            "clean_role_gate_required": true,
            "separate_review_artifact_required": true,
            "episode_council_between_episodes": true
        },
        "council": {
            "mode": "mock",
            "reviewers": ["gpt", "claude"],
            "every": 1
        },
        "preflight": {
            "role_gate": {
                "ready": true,
                "failed": 0,
                "failure_category_counts": {},
                "quality_gate": {
                    "ready_for_long_workload": true
                }
            }
        },
        "workload_command": [
            "python3",
            "scripts/offdesk_twinpaper_autonomy_workload.py",
            "--out-dir",
            out_dir,
            "--evidence-bundle",
            out_dir.join("evidence_bundle.json"),
            "--evidence-review",
            out_dir.join("evidence_review.json")
        ],
        "workload_wrapper": out_dir.join("run_workload.sh"),
        "artifacts": {
            "prepared_task": manifest_path,
            "preflight": out_dir.join("preflight.json"),
            "runner_log": out_dir.join("offdesk-runner.log"),
            "result": out_dir.join("result.json"),
            "report": out_dir.join("REPORT.md"),
            "evidence_bundle": out_dir.join("evidence_bundle.json"),
            "evidence_review": out_dir.join("evidence_review.json")
        },
        "enqueue_args": [
            "forager",
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--agent-mode",
            "critique",
            "--provider-id",
            "ollama"
        ]
    });
    fs::write(&manifest_path, serde_json::to_string_pretty(&manifest)?)?;

    let script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("offdesk_workload_review_harness.py");
    let output = Command::new("python3")
        .arg(script_path)
        .arg("--manifest")
        .arg(&manifest_path)
        .arg("--out")
        .arg(&review_path)
        .output()?;

    assert!(
        output.status.success(),
        "review harness failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let review: serde_json::Value = serde_json::from_slice(&fs::read(&review_path)?)?;
    assert_eq!(review["passed"], true);
    assert_eq!(review["blocking_reasons"], json!([]));
    assert_eq!(
        review["results"][0]["review_stage_decision"],
        "needs_approval"
    );
    Ok(())
}

#[test]
fn generic_prepare_offdesk_workload_writes_reviewed_launch_packet() -> Result<()> {
    let temp = tempdir()?;
    let repo_dir = temp.path().join("repo");
    let out_root = temp.path().join("prepared");
    fs::create_dir_all(&repo_dir)?;
    fs::write(repo_dir.join("README.md"), "# Generic Project\n")?;

    let script_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("prepare_offdesk_workload.py");
    let output = Command::new("python3")
        .env("HOME", temp.path())
        .env("XDG_CONFIG_HOME", temp.path().join(".config"))
        .arg(script_path)
        .arg("--profile")
        .arg("generic-profile")
        .arg("--project-key")
        .arg("generic-project")
        .arg("--repo")
        .arg(&repo_dir)
        .arg("--out-root")
        .arg(&out_root)
        .arg("--duration-minutes")
        .arg("0.1")
        .arg("--max-iterations")
        .arg("1")
        .arg("--workload-command")
        .arg("printf 'ok\\n'")
        .output()?;

    assert!(
        output.status.success(),
        "prepare generic workload failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let prepared: serde_json::Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(prepared["ready_for_enqueue"], true);
    assert_eq!(prepared["blocking_reasons"], json!([]));
    let manifest_path = PathBuf::from(prepared["manifest"].as_str().expect("manifest path"));
    let preflight_path = PathBuf::from(prepared["preflight"].as_str().expect("preflight path"));
    let launch_path = PathBuf::from(
        prepared["launch_dry_run_report"]
            .as_str()
            .expect("launch packet path"),
    );
    let validation_path = PathBuf::from(
        prepared["long_run_validation_packet"]
            .as_str()
            .expect("validation packet path"),
    );
    assert!(manifest_path.exists());
    assert!(preflight_path.exists());
    assert!(launch_path.exists());
    assert!(validation_path.exists());
    assert!(manifest_path.with_file_name("preflight_ready").exists());

    let manifest: serde_json::Value = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    assert_eq!(manifest["kind"], "forager_offdesk_prepared_workload");
    assert_eq!(manifest["project_key"], "generic-project");
    assert_eq!(manifest["preflight"]["ready_for_enqueue"], true);
    assert_eq!(
        manifest["review_contract"]["schema"],
        "forager_workload_review_contract.v1"
    );
    assert!(manifest["artifacts"]["review_artifact"]
        .as_str()
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .is_some());
    let launch = fs::read_to_string(&launch_path)?;
    let validation = fs::read_to_string(&validation_path)?;
    let manifest_text = fs::read_to_string(&manifest_path)?;
    assert!(launch.contains("Offdesk Launch Dry Run"));
    assert!(validation.contains("Offdesk Long-Run Validation Packet"));
    assert!(!launch.contains("TwinPaper"));
    assert!(!validation.contains("TwinPaper"));
    assert!(!manifest_text.contains("TwinPaper"));
    Ok(())
}

#[test]
#[serial]
fn offdesk_pause_unpause_round_trips_and_tick_holds_dispatch() -> Result<()> {
    let temp = tempdir()?;
    let profile = profile_dir(temp.path());
    fs::create_dir_all(&profile)?;

    // Not paused by default.
    let status = forager_command(temp.path())
        .args(["offdesk", "pause-status", "--json"])
        .output()?;
    assert!(status.status.success());
    let status: serde_json::Value = serde_json::from_slice(&status.stdout)?;
    assert_eq!(status["paused"], false);

    // Pause records the reason and flips the state.
    let pause = forager_command(temp.path())
        .args([
            "offdesk",
            "pause",
            "--reason",
            "emergency stop",
            "--by",
            "test",
            "--json",
        ])
        .output()?;
    assert!(
        pause.status.success(),
        "{}",
        String::from_utf8_lossy(&pause.stderr)
    );
    let pause: serde_json::Value = serde_json::from_slice(&pause.stdout)?;
    assert_eq!(pause["paused"], true);
    assert_eq!(pause["reason"], "emergency stop");
    assert!(profile.join("offdesk_operator_pause.json").exists());

    // A tick while paused holds new dispatch instead of launching.
    let tick = forager_command(temp.path())
        .args(["offdesk", "tick", "--limit", "5", "--json"])
        .output()?;
    assert!(tick.status.success());
    let tick: serde_json::Value = serde_json::from_slice(&tick.stdout)?;
    assert_eq!(tick["dispatch_paused"], true);
    assert_eq!(tick["launched"], 0);

    // Unpause clears it.
    let unpause = forager_command(temp.path())
        .args(["offdesk", "unpause", "--by", "test", "--json"])
        .output()?;
    assert!(unpause.status.success());
    let unpause: serde_json::Value = serde_json::from_slice(&unpause.stdout)?;
    assert_eq!(unpause["paused"], false);
    Ok(())
}

#[test]
#[serial]
fn offdesk_learning_scan_emits_candidates_once_and_tick_auto_scans() -> Result<()> {
    use forager::offdesk::{
        AdaptiveWikiKind, AdaptiveWikiStore, BackgroundRunnerKind, OffdeskTask, OffdeskTaskInput,
        OffdeskTaskStatus, OffdeskTaskStore,
    };

    let temp = tempdir()?;
    let profile = profile_dir(temp.path());
    fs::create_dir_all(&profile)?;
    let now = Utc::now();

    let failed_task = |task_id: &str, last_error: &str| -> OffdeskTask {
        let mut task = OffdeskTask::new(
            OffdeskTaskInput {
                task_id: Some(task_id.to_string()),
                request_id: "req-1".to_string(),
                project_key: "proj-a".to_string(),
                capability_id: "dispatch.runtime".to_string(),
                runner_kind: BackgroundRunnerKind::LocalBackground,
                command: "cargo build".to_string(),
                workdir: "/tmp".to_string(),
                execution_brief: None,
                not_before: None,
                mutation_class: None,
                artifact_refs: Vec::new(),
                implementation_packet: None,
                artifact_kind: None,
                agent_mode: None,
                provider_id: None,
                model: None,
                preview: String::new(),
                reason: String::new(),
                log_artifact_path: None,
                result_artifact_path: None,
            },
            now,
        );
        task.status = OffdeskTaskStatus::Failed;
        task.last_error = Some(last_error.to_string());
        task
    };

    // Seed one failed task, then run the CLI scan.
    let store = OffdeskTaskStore::new(&profile);
    store.save(&[failed_task("task-1", "compile error: token=sk-secret")])?;

    let scan = forager_command(temp.path())
        .args(["offdesk", "learning-scan", "--json"])
        .output()?;
    assert!(
        scan.status.success(),
        "{}",
        String::from_utf8_lossy(&scan.stderr)
    );
    let scan: serde_json::Value = serde_json::from_slice(&scan.stdout)?;
    assert_eq!(scan["emitted"].as_array().unwrap().len(), 1);
    assert_eq!(scan["emitted"][0]["source"], "task_failed");

    // Re-running is idempotent: the durable cursor skips the processed event.
    let rescan = forager_command(temp.path())
        .args(["offdesk", "learning-scan", "--json"])
        .output()?;
    assert!(rescan.status.success());
    let rescan: serde_json::Value = serde_json::from_slice(&rescan.stdout)?;
    assert_eq!(rescan["emitted"].as_array().unwrap().len(), 0);
    assert_eq!(rescan["skipped_already_processed"], 1);

    // A second failed task of the same shape is a new event; a tick auto-scans it.
    store.save(&[
        failed_task("task-1", "compile error: token=sk-secret"),
        failed_task("task-2", "another failure"),
    ])?;
    let tick = forager_command(temp.path())
        .args(["offdesk", "tick", "--limit", "5", "--json"])
        .output()?;
    assert!(
        tick.status.success(),
        "{}",
        String::from_utf8_lossy(&tick.stderr)
    );
    let tick: serde_json::Value = serde_json::from_slice(&tick.stdout)?;
    assert_eq!(tick["learning_signals_emitted"], 1);

    // Both failures share a runner/project, so they merge into one candidate whose
    // occurrence_count reflects the repeated pattern, and no secret leaks.
    let candidates = AdaptiveWikiStore::new(&profile)
        .load_candidates()?
        .candidates;
    assert_eq!(
        candidates.len(),
        1,
        "same-shape failures should merge into one candidate"
    );
    assert_eq!(candidates[0].kind, AdaptiveWikiKind::FailurePattern);
    assert_eq!(candidates[0].occurrence_count, 2);
    assert!(
        !candidates[0].human_summary.contains("sk-secret"),
        "secret leaked into candidate: {}",
        candidates[0].human_summary
    );
    Ok(())
}
