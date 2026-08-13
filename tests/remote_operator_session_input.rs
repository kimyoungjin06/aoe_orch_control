//! Integration tests for remote session-input delivery.
//!
//! `tmux send-keys` exits 0 as soon as the key is queued, so the Telegram
//! operator used to report every remote approval as delivered. On 2026-08-06 a
//! codex pane sat frozen for fourteen hours while six approvals were reported
//! successful and nothing moved. These tests pin the distinction: a pane that
//! never repaints is a failure, a pane that reacts is a success.

use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};
use tempfile::tempdir;

// The dispatcher resolves a session by the "_<8-char id>" suffix on the tmux
// name, so these must end with the ids used in the status fixtures below.
const FROZEN_SESSION: &str = "forager_test_deadbeef";
const LIVE_SESSION: &str = "forager_test_cafe1234";
const TEXT_SESSION: &str = "forager_test_feed5678";
const MULTILINE_SESSION: &str = "forager_test_face5678";
const QUIET_TEXT_SESSION: &str = "forager_test_f00d5678";
const HASH_SESSION: &str = "forager_test_bead9876";

fn tool_available(bin: &str, version_flag: &str) -> bool {
    Command::new(bin)
        .arg(version_flag)
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn scripts_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts")
}

fn kill_session(name: &str) {
    let _ = Command::new("tmux")
        .args(["kill-session", "-t", name])
        .output();
}

fn start_session(name: &str, command: &str) {
    kill_session(name);
    let created = Command::new("tmux")
        .args(["new-session", "-d", "-s", name, command])
        .output()
        .expect("failed to spawn tmux session");
    assert!(created.status.success(), "could not create {name}");
}

fn wait_for_quiet_sleep_pane(name: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut previous_capture = None;

    loop {
        let command = Command::new("tmux")
            .args([
                "display-message",
                "-p",
                "-t",
                name,
                "#{pane_current_command}",
            ])
            .output()
            .expect("failed to inspect tmux pane command");
        let is_sleeping =
            command.status.success() && String::from_utf8_lossy(&command.stdout).trim() == "sleep";
        if is_sleeping {
            let capture = Command::new("tmux")
                .args(["capture-pane", "-p", "-t", name])
                .output()
                .expect("failed to capture quiet tmux pane");
            assert!(capture.status.success(), "could not capture {name}");
            if previous_capture.as_ref() == Some(&capture.stdout) {
                return;
            }
            previous_capture = Some(capture.stdout);
        } else {
            previous_capture = None;
        }

        assert!(Instant::now() < deadline, "{name} did not become quiet");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Drive the Python dispatcher the way the Telegram poll loop does.
fn apply_session_input(status_file: &Path, session_id: &str) -> Value {
    let driver = r#"
import json, sys
sys.path.insert(0, sys.argv[1])
from telegram_operator.dispatch import apply_session_input
print(json.dumps(apply_session_input(
    "forager", "default", sys.argv[3], "approve", status_file=sys.argv[2])))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(driver)
        .arg(scripts_dir())
        .arg(status_file)
        .arg(session_id)
        .output()
        .expect("failed to run dispatcher");
    assert!(
        output.status.success(),
        "dispatcher failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("dispatcher returned non-JSON")
}

fn apply_session_text_input(
    status_file: &Path,
    session_id: &str,
    text: &str,
    expected_hash: &str,
) -> Value {
    let driver = r#"
import json, sys
sys.path.insert(0, sys.argv[1])
from telegram_operator.dispatch import apply_session_text_input
print(json.dumps(apply_session_text_input(
    "forager", "default", sys.argv[3], sys.argv[4], status_file=sys.argv[2],
    expected_hash=sys.argv[5])))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(driver)
        .arg(scripts_dir())
        .arg(status_file)
        .arg(session_id)
        .arg(text)
        .arg(expected_hash)
        .output()
        .expect("failed to run text dispatcher");
    assert!(
        output.status.success(),
        "text dispatcher failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("text dispatcher returned non-JSON")
}

fn apply_session_text_input_exact(status_file: &Path, session_id: &str, text: &str) -> Value {
    let driver = r#"
import json, sys
sys.path.insert(0, sys.argv[1])
from telegram_operator.dispatch import apply_session_text_input
print(json.dumps(apply_session_text_input(
    "forager", "default", sys.argv[3], sys.argv[4], status_file=sys.argv[2],
    require_exact_id=True)))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(driver)
        .arg(scripts_dir())
        .arg(status_file)
        .arg(session_id)
        .arg(text)
        .output()
        .expect("failed to run exact text dispatcher");
    assert!(
        output.status.success(),
        "exact text dispatcher failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("exact text dispatcher returned non-JSON")
}

fn render_session_input_error(error: &str) -> Value {
    let driver = r#"
import argparse, json, sys
sys.path.insert(0, sys.argv[1])
import offdesk_remote_operator_telegram as mod
error = sys.argv[2]
mod.apply_session_input = lambda *args, **kwargs: {
    "ok": False, "kind": "session_input", "error": error,
    "project": "demo", "session_title": "codex", "key": "Enter",
    "new_hash": "new12345", "prompt_line": "Allow the new command?",
}
args = argparse.Namespace(
    forager_bin="forager", profile="default", session_status_file=None,
)
parsed = {
    "command": "session_approve",
    "session_input_session_id": "deadbeef",
    "session_input_hash": "old12345",
    "session_input_option": "",
}
print(json.dumps(mod.render_dispatch_command(args, {}, parsed, {
    "parsed_command": parsed, "generated_at": "2026-08-11T00:00:00Z",
})))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(driver)
        .arg(scripts_dir())
        .arg(error)
        .output()
        .expect("failed to run render probe");
    assert!(
        output.status.success(),
        "render probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("render probe returned non-JSON")
}

fn normalize_send_agent_intent() -> Value {
    let driver = r#"
import json, sys
sys.path.insert(0, sys.argv[1])
from telegram_operator.agent import normalize_agent_chat
parsed = {
    "action": "send_agent",
    "session_id": "cafe123400000000",
    "message": "전체 테스트를 다시 실행하고 결과를 알려줘",
    "assistant_reply": "로컬 Codex에 전달합니다.",
    "confidence": 0.94,
    "reason": "operator directed the live agent",
}
runtime = {"provider": "ollama", "base_url": "local", "model": "test"}
print(json.dumps(normalize_agent_chat(parsed, runtime=runtime)))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(driver)
        .arg(scripts_dir())
        .output()
        .expect("failed to normalize agent intent");
    assert!(
        output.status.success(),
        "normalizer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("normalizer returned non-JSON")
}

fn validate_send_agent_intent(chat_text: &str, intent: Value, snapshot: Value) -> Value {
    let driver = r#"
import json, sys
sys.path.insert(0, sys.argv[1])
from telegram_operator.agent import validate_session_message_intent
print(json.dumps(validate_session_message_intent(
    json.loads(sys.argv[3]), chat_text=sys.argv[2],
    operator_snapshot=json.loads(sys.argv[4]))))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(driver)
        .arg(scripts_dir())
        .arg(chat_text)
        .arg(serde_json::to_string(&intent).expect("serialize intent"))
        .arg(serde_json::to_string(&snapshot).expect("serialize snapshot"))
        .output()
        .expect("failed to validate agent intent");
    assert!(
        output.status.success(),
        "validator failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("validator returned non-JSON")
}

fn render_session_message_dry_run() -> Value {
    let driver = r#"
import argparse, json, sys
sys.path.insert(0, sys.argv[1])
import offdesk_remote_operator_telegram as mod
def unexpected_apply(*args, **kwargs):
    raise RuntimeError("dry-run called apply_session_text_input")
mod.apply_session_text_input = unexpected_apply
args = argparse.Namespace(
    forager_bin="forager", profile="default", session_status_file=None,
)
result = {
    "mode": "dry_run", "generated_at": "2026-08-11T00:00:00Z",
    "read_only": True, "mutation_authorized": False,
}
print(json.dumps(mod.render_session_message_delivery(
    args, result, session_id="cafe123400000000", message="continue")))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(driver)
        .arg(scripts_dir())
        .output()
        .expect("failed to render dry-run message");
    assert!(
        output.status.success(),
        "dry-run render failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("dry-run render returned non-JSON")
}

fn resolve_replied_session_context() -> Value {
    let driver = r#"
import json, sys
sys.path.insert(0, sys.argv[1])
import offdesk_remote_operator_telegram as mod
state = {"waiting_notified_by_session": {
    "feed567800000000": {
        "message_id": 700,
        "resolved": False,
        "project": "forager",
        "title": "Codex",
        "tool": "codex",
        "prompt_hash": "abcd1234",
    }
}}
message = {"reply_to_message": {"message_id": 700}}
print(json.dumps(mod.replied_session_context(state, message)))
"#;
    let output = Command::new("python3")
        .arg("-c")
        .arg(driver)
        .arg(scripts_dir())
        .output()
        .expect("failed to resolve replied session context");
    assert!(
        output.status.success(),
        "reply context probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("reply context probe returned non-JSON")
}

#[test]
fn session_input_reports_no_effect_on_a_frozen_pane() {
    if !tool_available("tmux", "-V") || !tool_available("python3", "--version") {
        eprintln!("Skipping test: tmux or python3 not available");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let status_file = dir.path().join("status.json");
    std::fs::write(
        &status_file,
        r#"{"sessions":[{"id":"deadbeef00000000","title":"frozen","project":"t","status":"waiting"}]}"#,
    )
    .expect("write status");

    // The pane prints the exact output shape that made the detector call this
    // session "waiting", then sleeps. Enter cannot change anything here.
    start_session(
        FROZEN_SESSION,
        "printf 'reviewing_not_approved\\nALLOWED_POSTS\\n'; sleep 300",
    );
    std::thread::sleep(std::time::Duration::from_millis(500));

    let result = apply_session_input(&status_file, "deadbeef");
    kill_session(FROZEN_SESSION);

    assert_eq!(
        result["ok"], false,
        "a pane that never repaints must not report success: {result}"
    );
    assert_eq!(result["error"], "no_effect", "unexpected error: {result}");
}

#[test]
fn session_input_succeeds_when_the_pane_reacts() {
    if !tool_available("tmux", "-V") || !tool_available("python3", "--version") {
        eprintln!("Skipping test: tmux or python3 not available");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let status_file = dir.path().join("status.json");
    std::fs::write(
        &status_file,
        r#"{"sessions":[{"id":"cafe123400000000","title":"live","project":"t","status":"waiting"}]}"#,
    )
    .expect("write status");

    // An interactive shell repaints its prompt on Enter, standing in for an
    // agent that actually consumes the keystroke.
    start_session(LIVE_SESSION, "bash --norc -i");
    std::thread::sleep(std::time::Duration::from_millis(500));

    let result = apply_session_input(&status_file, "cafe1234");
    kill_session(LIVE_SESSION);

    assert_eq!(
        result["ok"], true,
        "a reacting pane must report success: {result}"
    );
}

#[test]
fn session_text_input_reaches_the_live_pane_and_submits() {
    if !tool_available("tmux", "-V") || !tool_available("python3", "--version") {
        eprintln!("Skipping test: tmux or python3 not available");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let status_file = dir.path().join("status.json");
    std::fs::write(
        &status_file,
        r#"{"sessions":[{"id":"feed567800000000","title":"live","tool":"codex","project":"t","status":"running"}]}"#,
    )
    .expect("write status");
    start_session(TEXT_SESSION, "bash --norc -i");
    std::thread::sleep(std::time::Duration::from_millis(500));

    let result = apply_session_text_input(
        &status_file,
        "feed5678",
        "printf telegram-text-delivered",
        "",
    );
    std::thread::sleep(std::time::Duration::from_millis(250));
    let capture = Command::new("tmux")
        .args(["capture-pane", "-p", "-t", TEXT_SESSION])
        .output()
        .expect("capture pane");
    kill_session(TEXT_SESSION);

    assert_eq!(result["ok"], true, "text delivery failed: {result}");
    assert_eq!(result["kind"], "session_message");
    assert_eq!(result["action"], "send_text");
    assert_eq!(result["delivery_status"], "pane_reacted");
    assert!(
        String::from_utf8_lossy(&capture.stdout).contains("telegram-text-delivered"),
        "pane did not receive message: {}",
        String::from_utf8_lossy(&capture.stdout)
    );
}

#[test]
fn session_text_input_preserves_multiline_payload() {
    if !tool_available("tmux", "-V") || !tool_available("python3", "--version") {
        eprintln!("Skipping test: tmux or python3 not available");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let status_file = dir.path().join("status.json");
    std::fs::write(
        &status_file,
        r#"{"sessions":[{"id":"face567800000000","title":"live","tool":"codex","project":"t","status":"running"}]}"#,
    )
    .expect("write status");
    start_session(MULTILINE_SESSION, "bash --norc -i");
    std::thread::sleep(std::time::Duration::from_millis(500));
    let message = "printf 'first-preserved\\n'\nprintf 'second-preserved\\n'";

    let result = apply_session_text_input(&status_file, "face5678", message, "");
    std::thread::sleep(std::time::Duration::from_millis(250));
    let capture = Command::new("tmux")
        .args(["capture-pane", "-p", "-t", MULTILINE_SESSION])
        .output()
        .expect("capture pane");
    kill_session(MULTILINE_SESSION);

    assert_eq!(result["ok"], true, "multiline delivery failed: {result}");
    assert_eq!(result["message_chars"], message.chars().count());
    let pane = String::from_utf8_lossy(&capture.stdout);
    assert!(
        pane.contains("first-preserved"),
        "first line missing: {pane}"
    );
    assert!(
        pane.contains("second-preserved"),
        "second line missing: {pane}"
    );
}

#[test]
fn session_text_input_reports_queued_without_claiming_agent_acknowledgement() {
    if !tool_available("tmux", "-V") || !tool_available("python3", "--version") {
        eprintln!("Skipping test: tmux or python3 not available");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let status_file = dir.path().join("status.json");
    std::fs::write(
        &status_file,
        r#"{"sessions":[{"id":"f00d567800000000","title":"quiet","tool":"codex","project":"t","status":"running"}]}"#,
    )
    .expect("write status");
    start_session(QUIET_TEXT_SESSION, "stty -echo; exec sleep 300");
    wait_for_quiet_sleep_pane(QUIET_TEXT_SESSION);

    let result = apply_session_text_input(&status_file, "f00d5678", "continue", "");
    kill_session(QUIET_TEXT_SESSION);

    assert_eq!(
        result["ok"], true,
        "tmux queue should be recorded: {result}"
    );
    assert_eq!(result["pane_reacted"], false);
    assert_eq!(result["delivery_status"], "input_queued");
}

#[test]
fn session_text_input_refuses_a_stopped_session() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let status_file = dir.path().join("status.json");
    std::fs::write(
        &status_file,
        r#"{"sessions":[{"id":"cafe123400000000","title":"old","tool":"codex","project":"t","status":"stopped"}]}"#,
    )
    .expect("write status");

    let result = apply_session_text_input(&status_file, "cafe1234", "continue", "");
    assert_eq!(result["ok"], false);
    assert_eq!(result["error"], "session_not_live:stopped");
}

#[test]
fn send_agent_model_action_becomes_a_session_message_intent() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }

    let result = normalize_send_agent_intent();
    assert_eq!(result["status"], "classified");
    assert_eq!(result["intent"], "session_message");
    assert_eq!(result["session_id"], "cafe123400000000");
    assert_eq!(
        result["session_message"],
        "전체 테스트를 다시 실행하고 결과를 알려줘"
    );
}

#[test]
fn ambiguous_codex_name_is_not_allowed_to_select_the_first_live_session() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }
    let intent = json!({
        "status": "classified",
        "intent": "session_message",
        "session_id": "lrnm000000000001",
        "session_message": "전체 테스트를 다시 실행해",
        "confidence": 0.95,
        "requires_clarification": false
    });
    let snapshot = json!({
        "live_agent_sessions": [
            {"id": "lrnm000000000001", "title": "Local-Anal", "tool": "codex", "project": "lrnm"},
            {"id": "fora000000000002", "title": "Codex", "tool": "codex", "project": "forager"},
            {"id": "scor000000000003", "title": "codex", "tool": "codex", "project": "scoreboard"}
        ]
    });

    let result = validate_send_agent_intent(
        "Codex에게 전체 테스트를 다시 실행하라고 해",
        intent,
        snapshot,
    );

    assert_eq!(result["intent"], "chat");
    assert_eq!(result["requires_clarification"], true);
    assert_eq!(result["session_target_status"], "session_target_ambiguous");
}

#[test]
fn named_project_with_multiple_agents_requires_the_agent_name() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }
    let intent = json!({
        "status": "classified",
        "intent": "session_message",
        "session_id": "clau000000000001",
        "session_message": "그 작업 계속 진행해",
        "confidence": 0.95,
        "requires_clarification": false
    });
    let snapshot = json!({
        "project_focus": {"key": "scoreboard", "focus_source": "mention"},
        "live_agent_sessions": [
            {"id": "clau000000000001", "title": "claude", "tool": "claude", "project": "scoreboard"},
            {"id": "code000000000002", "title": "codex", "tool": "codex", "project": "scoreboard"}
        ]
    });

    let result = validate_send_agent_intent("scoreboard에서 그 작업 계속 진행해", intent, snapshot);

    assert_eq!(result["intent"], "chat");
    assert_eq!(result["session_target_status"], "session_target_ambiguous");
}

#[test]
fn explicit_project_and_agent_resolve_one_exact_session() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }
    let intent = json!({
        "status": "classified",
        "intent": "session_message",
        "session_id": "code000000000002",
        "session_message": "그 작업 계속 진행해",
        "confidence": 0.95,
        "requires_clarification": false
    });
    let snapshot = json!({
        "project_focus": {"key": "scoreboard", "focus_source": "mention"},
        "live_agent_sessions": [
            {"id": "clau000000000001", "title": "claude", "tool": "claude", "project": "scoreboard"},
            {"id": "code000000000002", "title": "codex", "tool": "codex", "project": "scoreboard"}
        ]
    });

    let result =
        validate_send_agent_intent("scoreboard Codex에서 그 작업 계속 진행해", intent, snapshot);

    assert_eq!(result["intent"], "session_message");
    assert_eq!(result["session_id"], "code000000000002");
    assert_eq!(result["session_target_status"], "resolved");
}

#[test]
fn model_clarification_flag_always_blocks_session_delivery() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }
    let intent = json!({
        "status": "classified",
        "intent": "session_message",
        "session_id": "code000000000002",
        "session_message": "continue",
        "confidence": 0.99,
        "requires_clarification": true,
        "clarifying_question": "어느 세션인가요?"
    });
    let snapshot = json!({
        "live_agent_sessions": [
            {"id": "code000000000002", "title": "codex", "tool": "codex", "project": "scoreboard"}
        ]
    });

    let result = validate_send_agent_intent("계속해", intent, snapshot);

    assert_eq!(result["intent"], "chat");
    assert_eq!(
        result["session_target_status"],
        "model_requires_clarification"
    );
}

#[test]
fn telegram_reply_to_waiting_card_binds_the_agent_session() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }

    let result = resolve_replied_session_context();
    assert_eq!(result["session_id"], "feed567800000000");
    assert_eq!(result["project"], "forager");
    assert_eq!(result["prompt_hash"], "abcd1234");
    assert_eq!(result["source"], "reply_card");
}

#[test]
fn session_text_input_refuses_a_changed_card_bound_prompt() {
    if !tool_available("tmux", "-V") || !tool_available("python3", "--version") {
        eprintln!("Skipping test: tmux or python3 not available");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let status_file = dir.path().join("status.json");
    std::fs::write(
        &status_file,
        r#"{"sessions":[{"id":"bead987600000000","title":"changed","tool":"codex","project":"t","status":"waiting"}]}"#,
    )
    .expect("write status");
    start_session(HASH_SESSION, "bash --norc -i");
    std::thread::sleep(std::time::Duration::from_millis(500));

    let result = apply_session_text_input(
        &status_file,
        "bead9876",
        "this must not be delivered",
        "stale123",
    );
    kill_session(HASH_SESSION);

    assert_eq!(result["ok"], false);
    assert_eq!(result["error"], "prompt_changed");
}

#[test]
fn session_text_input_refuses_an_ambiguous_id_prefix() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let status_file = dir.path().join("status.json");
    std::fs::write(
        &status_file,
        r#"{"sessions":[
            {"id":"feed567800000001","status":"running"},
            {"id":"feed567800000002","status":"idle"}
        ]}"#,
    )
    .expect("write status");

    let result = apply_session_text_input(&status_file, "feed5678", "continue", "");
    assert_eq!(result["ok"], false);
    assert_eq!(result["error"], "session_id_ambiguous");
}

#[test]
fn natural_session_text_input_requires_the_full_exact_id() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let status_file = dir.path().join("status.json");
    std::fs::write(
        &status_file,
        r#"{"sessions":[{"id":"feed567800000001","status":"running"}]}"#,
    )
    .expect("write status");

    let result = apply_session_text_input_exact(&status_file, "feed5678", "continue");
    assert_eq!(result["ok"], false);
    assert_eq!(result["error"], "session_id_not_exact");
}

#[test]
fn session_text_input_never_defaults_an_empty_id_to_the_only_session() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }

    let dir = tempdir().expect("tempdir");
    let status_file = dir.path().join("status.json");
    std::fs::write(
        &status_file,
        r#"{"sessions":[{"id":"feed567800000001","status":"running"}]}"#,
    )
    .expect("write status");

    let result = apply_session_text_input(&status_file, "", "continue", "");
    assert_eq!(result["ok"], false);
    assert_eq!(result["error"], "session_id_missing");
}

#[test]
fn direct_session_message_dry_run_never_calls_the_tmux_dispatcher() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }

    let result = render_session_message_dry_run();
    assert_eq!(result["read_only"], true);
    assert!(result.get("dispatch_result").is_none());
    assert_eq!(
        result["session_message_preview"]["session_id"],
        "cafe123400000000"
    );
    assert!(result["message_preview"]
        .as_str()
        .expect("message preview")
        .contains("tmux 입력을 보내지 않았습니다"));
}

#[test]
fn no_effect_renders_the_dead_input_message() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }

    let result = render_session_input_error("no_effect");
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(
        preview.contains("입력이 먹히지 않았습니다"),
        "unexpected preview: {preview}"
    );
    assert!(
        !preview.contains("프롬프트가 바뀌어"),
        "wrong error branch: {preview}"
    );
}

#[test]
fn prompt_changed_keeps_its_refresh_message() {
    if !tool_available("python3", "--version") {
        eprintln!("Skipping test: python3 not available");
        return;
    }

    let result = render_session_input_error("prompt_changed");
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(
        preview.contains("프롬프트가 바뀌어"),
        "unexpected preview: {preview}"
    );
    assert!(
        preview.contains("Allow the new command?"),
        "missing current prompt: {preview}"
    );
}
