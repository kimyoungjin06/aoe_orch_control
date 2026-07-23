use anyhow::Result;
use serde_json::{json, Map, Value};
use serial_test::serial;
use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::tempdir;

fn script_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join(name)
}

fn write_env_file(path: &Path) -> Result<()> {
    fs::write(
        path,
        "TELEGRAM_BOT_TOKEN=999999:fake-token-for-test\nTELEGRAM_OWNER_CHAT_ID=123456789\n",
    )?;
    Ok(())
}

fn write_projection_file(path: &Path, command: &str, payload: Value) -> Result<()> {
    let projection = json!({
        "schema": "remote_operator_readonly_projection.v1",
        "generated_at": "2026-06-06T00:00:00Z",
        "forager_profile": "default",
        "transport": "telegram",
        "source_surface": "remote_operator.telegram",
        "command": command,
        "phase": "read_only_surface",
        "read_only": true,
        "mutation_authorized": false,
        "approval_authorized": false,
        "allowed_remote_intents": [
            "inspect_status",
            "inspect_pending",
            "inspect_plans",
            "inspect_plan"
        ],
        "forbidden_remote_intents": [
            "approve_plan",
            "approve_launch",
            "deny_launch",
            "enqueue",
            "launch",
            "dispatch",
            "shell"
        ],
        "card": {
            "title": "Forager Remote Status",
            "summary_lines": [],
            "detail_lines": [],
            "observed_hash": "sha256:0123456789abcdef0123456789abcdef",
            "remote_actions": ["inspect_status"],
            "disabled_remote_actions": ["approve_launch", "dispatch", "shell"]
        },
        "payload": payload
    });
    fs::write(path, serde_json::to_string_pretty(&projection)?)?;
    Ok(())
}

fn remote_operator_command(home: &Path) -> Command {
    let mut command = Command::new("python3");
    command.arg(script_path("offdesk_remote_operator_telegram.py"));
    command.env("HOME", home);
    command.env("XDG_CONFIG_HOME", home.join(".config"));
    command.env("OFFDESK_REMOTE_OPERATOR_AGENT_INTENT_MODE", "off");
    command.env_remove("FORAGER_PROFILE");
    command.env_remove("AGENT_OF_EMPIRES_PROFILE");
    command
}

fn watchdog_command(home: &Path) -> Command {
    let mut command = Command::new("python3");
    command.arg(script_path("offdesk_remote_operator_watchdog.py"));
    command.env("HOME", home);
    command.env("XDG_CONFIG_HOME", home.join(".config"));
    command.env_remove("FORAGER_PROFILE");
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

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|window| window == b"\r\n\r\n")
}

fn parse_content_length(headers: &str) -> usize {
    headers
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            if key.trim().eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0)
}

fn read_http_request(stream: &mut TcpStream) -> Result<(String, Vec<u8>)> {
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut data = Vec::new();
    let mut buffer = [0_u8; 4096];
    loop {
        let read = stream.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        data.extend_from_slice(&buffer[..read]);
        if let Some(header_end) = find_header_end(&data) {
            let headers = String::from_utf8_lossy(&data[..header_end]).to_string();
            let body_start = header_end + 4;
            let content_length = parse_content_length(&headers);
            if data.len() >= body_start + content_length {
                let line = headers.lines().next().unwrap_or_default().to_string();
                let body = data[body_start..body_start + content_length].to_vec();
                return Ok((line, body));
            }
        }
    }
    anyhow::bail!("incomplete HTTP request")
}

fn write_http_json(stream: &mut TcpStream, value: Value) -> Result<()> {
    let body = serde_json::to_vec(&value)?;
    write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    )?;
    stream.write_all(&body)?;
    Ok(())
}

fn spawn_fake_ollama_with_classification(
    body_path: PathBuf,
    classified: Value,
) -> Result<(String, thread::JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    listener.set_nonblocking(true)?;
    let handle = thread::spawn(move || -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut handled = 0;
        while handled < 2 {
            let (mut stream, _) = match listener.accept() {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        anyhow::bail!("fake Ollama timed out waiting for requests");
                    }
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let (request_line, body) = read_http_request(&mut stream)?;
            if request_line.starts_with("GET /api/tags ") {
                write_http_json(
                    &mut stream,
                    json!({"models": [{"name": "qwen3-coder-next:latest"}]}),
                )?;
            } else if request_line.starts_with("POST /api/generate ") {
                fs::write(&body_path, &body)?;
                write_http_json(
                    &mut stream,
                    json!({"response": serde_json::to_string(&classified)?}),
                )?;
            } else {
                write_http_json(&mut stream, json!({"error": request_line}))?;
            }
            handled += 1;
        }
        Ok(())
    });
    Ok((format!("http://127.0.0.1:{port}"), handle))
}

/// Serves /api/tags any number of times and answers successive
/// /api/generate calls from `responses` in order, writing each request body
/// to `<body_dir>/generate_<n>.json`. Returns after the last response.
fn spawn_fake_ollama_with_sequence(
    body_dir: PathBuf,
    responses: Vec<Value>,
) -> Result<(String, thread::JoinHandle<Result<()>>)> {
    let listener = TcpListener::bind(("127.0.0.1", 0))?;
    let port = listener.local_addr()?.port();
    listener.set_nonblocking(true)?;
    let handle = thread::spawn(move || -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut served = 0usize;
        while served < responses.len() {
            let (mut stream, _) = match listener.accept() {
                Ok(value) => value,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() > deadline {
                        anyhow::bail!("fake Ollama timed out waiting for requests");
                    }
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            let (request_line, body) = read_http_request(&mut stream)?;
            if request_line.starts_with("GET /api/tags ") {
                write_http_json(
                    &mut stream,
                    json!({"models": [{"name": "qwen3-coder-next:latest"}]}),
                )?;
            } else if request_line.starts_with("POST /api/generate ") {
                fs::write(body_dir.join(format!("generate_{served}.json")), &body)?;
                write_http_json(
                    &mut stream,
                    json!({"response": serde_json::to_string(&responses[served])?}),
                )?;
                served += 1;
            } else {
                write_http_json(&mut stream, json!({"error": request_line}))?;
            }
        }
        Ok(())
    });
    Ok((format!("http://127.0.0.1:{port}"), handle))
}

fn spawn_fake_ollama(body_path: PathBuf) -> Result<(String, thread::JoinHandle<Result<()>>)> {
    spawn_fake_ollama_with_classification(
        body_path,
        json!({
            "intent": "plan_request",
            "feedback_kind": "planning_request",
            "confidence": 0.91,
            "project_hint": "NanoClustering",
            "goal": "Assess Fractal tree work",
            "timebox": "tomorrow night",
            "requires_clarification": false,
            "clarifying_question": null,
            "reason": "The operator is asking whether this work should become a plan candidate.",
            "non_authorized": ["execution", "approval", "shell"]
        }),
    )
}

fn assert_mobile_contract(result: &Value) {
    let contract = &result["mobile_card_contract"];
    assert_eq!(contract["schema"], "telegram_mobile_card_contract.v1");
    assert!(contract["warnings"]
        .as_array()
        .expect("mobile card warnings")
        .is_empty());
    assert!(
        contract["line_count"].as_u64().expect("line count")
            <= contract["max_lines"].as_u64().expect("max lines")
    );
    assert!(
        contract["char_count"].as_u64().expect("char count")
            <= contract["max_chars"].as_u64().expect("max chars")
    );
    let choice_contract = &result["choice_surface_contract"];
    assert_eq!(
        choice_contract["schema"],
        "telegram_choice_surface_contract.v1"
    );
    assert!(choice_contract["warnings"]
        .as_array()
        .expect("choice surface warnings")
        .is_empty());
    let context_kind = choice_contract["context_kind"].as_str().unwrap_or("");
    let required_buttons: Vec<&str> = if matches!(
        context_kind,
        "remote_plan_project_selection" | "remote_plan_init_review"
    ) {
        vec!["상태", "계획"]
    } else {
        vec!["상태", "승인 대기", "계획", "도움말"]
    };
    for label in required_buttons {
        assert!(choice_contract["button_texts"]
            .as_array()
            .expect("button texts")
            .iter()
            .any(|item| item == label));
    }
    assert_eq!(choice_contract["has_freeform_placeholder"], true);

    let preview = result["message_preview"].as_str().expect("message preview");
    for forbidden in [
        "Forager Remote Status",
        "Read-only",
        "읽기 전용",
        "상태:",
        "다음:",
        "맥락:",
        "기준 ",
        "검증:",
        "sha256:",
        "receipt",
        "preview",
        "project init",
        "plan evidence",
        "shell",
        "launch-prep",
        "runtime_handle_alive",
    ] {
        assert!(
            !preview.contains(forbidden),
            "mobile preview leaked forbidden term {forbidden}:\n{preview}"
        );
    }
}

fn button_texts(result: &Value) -> Vec<String> {
    result["choice_surface_contract"]["button_texts"]
        .as_array()
        .expect("button texts")
        .iter()
        .map(|item| item.as_str().unwrap_or_default().to_string())
        .collect()
}

fn write_text_update(path: &Path, update_id: i64, message_id: i64, text: &str) -> Result<()> {
    fs::write(
        path,
        serde_json::to_string_pretty(&json!({
            "update_id": update_id,
            "message": {
                "message_id": message_id,
                "date": 1780000000,
                "chat": {"id": 123456789, "type": "private"},
                "from": {"id": 987654321, "is_bot": false, "first_name": "Operator"},
                "text": text
            }
        }))?,
    )?;
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_dry_run_status_renders_read_only_projection() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("remote_status.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--send-command-text")
        .arg("/status")
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(
        result["schema"],
        "remote_operator_telegram_adapter_result.v1"
    );
    assert_eq!(result["mode"], "dry_run");
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["read_only"], true);
    assert_eq!(result["mutation_authorized"], false);
    assert_eq!(result["approval_authorized"], false);
    assert_eq!(
        result["projection_schema"],
        "remote_operator_readonly_projection.v1"
    );
    assert_eq!(result["projection"]["command"], "status");
    assert_eq!(result["projection"]["read_only"], true);
    assert_eq!(result["projection"]["mutation_authorized"], false);
    assert_eq!(result["projection"]["approval_authorized"], false);
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>Forager 점검</b>"));
    assert!(preview.contains("처리할 항목이 없습니다."));
    assert!(preview.contains("새 알림이 오면 다시 확인하세요."));
    assert!(!preview.contains("Forager Remote Status"));
    assert!(!preview.contains("Read-only"));
    assert_mobile_contract(&result);
    assert!(result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash")
        .starts_with("sha256:"));

    let serialized = serde_json::to_string(&result)?;
    assert!(!serialized.contains("fake-token-for-test"));
    assert!(!serialized.contains("999999:"));
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_korean_status_question_routes_to_chat() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("korean_status_question.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("지금은 정상상태!?")
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "chat");
    assert_eq!(result["parsed_command"]["reason"], "plain_text_chat");
    assert_eq!(result["projection"], Value::Null);
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>Forager 응답</b>"));
    assert!(preview.contains("/status"));
    assert!(!preview.contains("<b>의견 접수</b>"));
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_status_includes_adapter_readiness_when_loop_status_exists() -> Result<()>
{
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let status_path = temp.path().join("loop_status.json");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&json!({
            "schema": "remote_operator_telegram_adapter_result.v1",
            "mode": "live_loop",
            "status": "polling",
            "poll_count": 3,
            "updates_seen": 0,
            "handled_result_count": 0,
            "last_result": {
                "generated_at": "2099-01-01T00:00:00+00:00",
                "status": "no_update"
            }
        }))?,
    )?;
    let out = temp.path().join("remote_status_with_adapter.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--send-command-text")
        .arg("/status")
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--loop-status-file")
        .arg(&status_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["adapter_health"]["health_status"], "healthy");
    assert_eq!(
        result["adapter_health"]["action_readiness"][2]["action"],
        "build_plan"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>Forager 점검</b>"));
    assert!(preview.contains("원격 정상 · 계획 준비 가능"));
    assert!(preview.contains("새 알림이 오면 다시 확인하세요."));
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_rejects_approval_command_without_projection() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("remote_approve.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("/approve approval_one")
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert_eq!(output.status.code(), Some(2));
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "unsupported");
    assert_eq!(result["reason"], "unsupported_remote_operator_command");
    assert_eq!(result["projection"], Value::Null);
    assert_eq!(result["read_only"], true);
    assert_eq!(result["mutation_authorized"], false);
    assert_eq!(result["approval_authorized"], false);
    assert_mobile_contract(&result);
    assert!(result["forbidden_remote_intents"]
        .as_array()
        .expect("forbidden intents")
        .iter()
        .any(|item| item == "approve_launch"));
    assert!(!profile_dir(temp.path())
        .join("pending_action_approvals.json")
        .exists());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_korean_buttons_map_to_safe_commands() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("remote_button_alias.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("계획")
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "plans");
    assert_eq!(
        result["parsed_command"]["argv"],
        json!(["plans", "--latest"])
    );
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_plain_text_defaults_to_chat() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("remote_chat.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("승인 전 실패 조건을 더 명확히 적어줘")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "chat");
    assert_eq!(result["projection"], Value::Null);
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>Forager 응답</b>"));
    assert!(preview.contains("/feedback"));
    assert!(!preview.contains("<b>의견 접수</b>"));
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_feedback_command_gets_mobile_receipt() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("remote_feedback.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("/feedback 승인 전 실패 조건을 더 명확히 적어줘")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "feedback");
    assert_eq!(
        result["parsed_command"]["feedback_kind"],
        "freeform_feedback"
    );
    assert_eq!(result["projection"], Value::Null);
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>의견 접수</b>"));
    assert!(preview.contains("의견을 저장했습니다."));
    assert!(!preview.contains("승인 전 실패 조건을 더 명확히 적어줘"));
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_remember_command_gets_wiki_candidate_preview() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("remote_remember.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("/remember 평문 텔레그램 메시지는 기본 채팅으로 답한다")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "remember");
    assert_eq!(result["projection"], Value::Null);
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>위키 후보</b>"));
    assert!(preview.contains("위키 후보 저장 미리보기입니다."));
    assert!(preview.contains("아직 런타임 지식은 아닙니다."));
    assert!(!profile_dir(temp.path())
        .join("adaptive_wiki_candidates.json")
        .exists());
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_plan_command_makes_non_execution_receipt_explicit() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("remote_plan_request.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("/plan nanoclustering Fractal tree 개발쪽을 자율주행으로 처리할 수 있을지 검토해볼까")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "plan_request");
    assert_eq!(
        result["parsed_command"]["feedback_kind"],
        "planning_request"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 요청 접수</b>"));
    assert!(preview.contains("아직 실행은 시작하지 않았습니다."));
    assert!(preview.contains("로컬에서 계획으로 바꾸세요."));
    assert!(!preview.contains("Fractal tree"));
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_plan_command_classifies_korean_night_run_as_planning_request(
) -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("night_run_request.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("/plan TwinPaper쪽에서 야간주행을 하고 싶어")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["parsed_command"]["command"], "plan_request");
    assert_eq!(
        result["parsed_command"]["feedback_kind"],
        "planning_request"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 요청 접수</b>"));
    assert!(preview.contains("아직 실행은 시작하지 않았습니다."));
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_agent_classifies_plan_command_request() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("remote_agent_plan_request.json");
    let agent_request_path = temp.path().join("ollama_generate_request.json");
    let (base_url, server) = spawn_fake_ollama(agent_request_path.clone())?;
    let telegram_text = "Please assess NanoClustering Fractal tree work for tomorrow night";
    let command_text = format!("/plan {telegram_text}");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg(&command_text)
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .arg("--agent-intent-mode")
        .arg("required")
        .arg("--agent-base-url")
        .arg(&base_url)
        .arg("--agent-model")
        .arg("qwen3-coder-next:latest")
        .output()?;

    server.join().expect("fake ollama server panicked")?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "plan_request");
    assert_eq!(
        result["parsed_command"]["feedback_kind"],
        "planning_request"
    );
    assert_eq!(result["parsed_command"]["agent_intent"]["source"], "ollama");
    assert_eq!(
        result["parsed_command"]["agent_intent"]["model"],
        "qwen3-coder-next:latest"
    );
    assert_eq!(
        result["parsed_command"]["agent_intent"]["intent"],
        "plan_request"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 요청 접수</b>"));
    assert!(preview.contains("아직 실행은 시작하지 않았습니다."));
    assert!(!preview.contains("NanoClustering"));
    assert_mobile_contract(&result);

    let agent_request: Value = serde_json::from_slice(&fs::read(&agent_request_path)?)?;
    assert_eq!(agent_request["model"], "qwen3-coder-next:latest");
    assert!(agent_request["prompt"]
        .as_str()
        .expect("agent prompt")
        .contains(telegram_text));
    assert_eq!(agent_request["stream"], false);
    assert_eq!(agent_request["think"], false);
    assert_eq!(agent_request["format"], "json");
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_agent_freeform_reply_is_conversational() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("remote_agent_conversation.json");
    let agent_request_path = temp.path().join("ollama_conversation_request.json");
    let (base_url, server) = spawn_fake_ollama_with_classification(
        agent_request_path.clone(),
        json!({
            "intent": "feedback",
            "feedback_kind": "freeform_feedback",
            "confidence": 0.93,
            "project_hint": null,
            "goal": null,
            "timebox": null,
            "requires_clarification": false,
            "clarifying_question": null,
            "assistant_reply": "지금 listener는 살아 있고, 최근 poll도 정상입니다. 다만 실행 권한은 로컬 검토 뒤에만 열립니다.",
            "reason": "The operator is asking for a conversational status explanation.",
            "non_authorized": ["execution", "approval", "shell"]
        }),
    )?;

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("방금 봇 상태를 사람이 읽기 좋게 설명해줘")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .arg("--agent-intent-mode")
        .arg("required")
        .arg("--agent-base-url")
        .arg(&base_url)
        .arg("--agent-model")
        .arg("qwen3-coder-next:latest")
        .output()?;

    server.join().expect("fake ollama server panicked")?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "chat");
    assert_eq!(result["parsed_command"]["feedback_kind"], Value::Null);
    assert_eq!(
        result["parsed_command"]["agent_intent"]["feedback_kind"],
        "chat"
    );
    assert_eq!(
        result["parsed_command"]["agent_intent"]["assistant_reply"],
        "지금 listener는 살아 있고, 최근 poll도 정상입니다. 다만 실행 권한은 로컬 검토 뒤에만 열립니다."
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>Forager 응답</b>"));
    assert!(preview.contains("최근 poll도 정상"));
    assert!(!preview.contains("<b>의견 접수</b>"));
    assert_mobile_contract(&result);

    let agent_request: Value = serde_json::from_slice(&fs::read(&agent_request_path)?)?;
    assert!(agent_request["prompt"]
        .as_str()
        .expect("agent prompt")
        .contains("assistant_reply"));
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_agent_clarification_is_visible_in_mobile_receipt() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("remote_agent_clarification.json");
    let agent_request_path = temp.path().join("ollama_clarification_request.json");
    let (base_url, server) = spawn_fake_ollama_with_classification(
        agent_request_path.clone(),
        json!({
            "intent": "clarification",
            "feedback_kind": "freeform_feedback",
            "confidence": 0.95,
            "project_hint": "nims",
            "goal": null,
            "timebox": null,
            "requires_clarification": true,
            "clarifying_question": "NIMS 안의 EPIMS 파일을 말하는지, 별도 EPIMS 프로젝트를 말하는지 확인해 주세요.",
            "reason": "The project scope is ambiguous.",
            "non_authorized": ["execution", "approval", "shell"]
        }),
    )?;

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("nims 프로젝트 내에 epims 프로젝트에서 지역별 클러스터링 파일 확인해줘")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .arg("--agent-intent-mode")
        .arg("required")
        .arg("--agent-base-url")
        .arg(&base_url)
        .arg("--agent-model")
        .arg("qwen3-coder-next:latest")
        .output()?;

    server.join().expect("fake ollama server panicked")?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["parsed_command"]["command"], "chat");
    assert_eq!(result["parsed_command"]["agent_intent"]["intent"], "chat");
    assert_eq!(
        result["parsed_command"]["agent_intent"]["requires_clarification"],
        true
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>확인 필요</b>"));
    assert!(preview.contains("NIMS 안의 EPIMS 파일"));
    assert!(preview.contains("/plan"));
    assert!(!preview.contains("<b>의견 접수</b>"));
    assert_mobile_contract(&result);

    let agent_request: Value = serde_json::from_slice(&fs::read(&agent_request_path)?)?;
    assert!(agent_request["prompt"]
        .as_str()
        .expect("agent prompt")
        .contains("same language as telegram_text"));
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_agent_uses_generic_provider_config() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp
        .path()
        .join("remote_generic_provider_plan_request.json");
    let agent_request_path = temp.path().join("ollama_generic_provider_request.json");
    let (base_url, server) = spawn_fake_ollama(agent_request_path.clone())?;
    let config_path = temp.path().join("forager_config.toml");
    fs::write(
        &config_path,
        format!(
            r#"
[llm.provider]
provider = "ollama"
base_urls = ["{base_url}"]
models = ["qwen3-coder-next:latest"]
timeout_sec = 20
num_ctx = 4096
num_predict = 512
"#
        ),
    )?;

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("/plan Please assess generic product telemetry cleanup for tonight")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .arg("--agent-intent-mode")
        .arg("required")
        .arg("--agent-config-file")
        .arg(&config_path)
        .output()?;

    server.join().expect("fake ollama server panicked")?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(
        result["parsed_command"]["agent_intent"]["base_url"],
        base_url
    );
    assert_eq!(result["parsed_command"]["command"], "plan_request");
    assert_eq!(
        result["parsed_command"]["agent_intent"]["config_sources"],
        json!(["llm.provider"])
    );
    assert_eq!(
        result["parsed_command"]["feedback_kind"],
        "planning_request"
    );
    assert_mobile_contract(&result);
    let agent_request: Value = serde_json::from_slice(&fs::read(&agent_request_path)?)?;
    assert_eq!(agent_request["options"]["num_ctx"], 4096);
    assert_eq!(agent_request["options"]["num_predict"], 512);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_feedback_uses_last_card_context() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let projection_path = temp.path().join("plans_attention.json");
    write_projection_file(
        &projection_path,
        "plans",
        json!({
            "plan_count": 1,
            "plans": [
                {"plan_id": "plan_harness_mobile", "review_status": "revision_required"}
            ]
        }),
    )?;
    let first_out = temp.path().join("plans_result.json");
    let state_path = temp.path().join("telegram_state.json");

    let first_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--send-command-text")
        .arg("/plans --latest")
        .arg("--projection-file")
        .arg(&projection_path)
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--out")
        .arg(&first_out)
        .output()?;

    assert!(
        first_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );
    let first: Value = serde_json::from_slice(&fs::read(&first_out)?)?;
    let chat_hash = first["target_chat_id_hash"]
        .as_str()
        .expect("target chat hash");
    let mut remembered_context = first["interaction_context"].clone();
    remembered_context["remembered_at"] = json!("2999-01-01T00:00:00+00:00");
    let mut contexts = Map::new();
    contexts.insert(chat_hash.to_string(), remembered_context);
    let state = json!({
        "schema": "remote_operator_telegram_state.v1",
        "offset": 0,
        "last_interaction_context_by_chat": Value::Object(contexts)
    });
    fs::write(&state_path, serde_json::to_string_pretty(&state)?)?;

    let feedback_out = temp.path().join("feedback_result.json");
    let feedback_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("/feedback 실패 조건 보강 필요")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--out")
        .arg(&feedback_out)
        .output()?;

    assert!(
        feedback_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&feedback_output.stdout),
        String::from_utf8_lossy(&feedback_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&feedback_out)?)?;
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("관련: 계획 plan_harness_mobile · 수정 필요"));
    assert!(preview.contains("로컬에서 검토합니다."));
    assert!(!preview.contains("남긴 말: 실패 조건 보강 필요"));
    assert_eq!(result["feedback_context"]["context_kind"], "plan_attention");
    assert_eq!(
        result["feedback_context"]["focus_ref"],
        "plan_harness_mobile"
    );
    assert!(button_texts(&result).contains(&"/show plan_harness_mobile".to_string()));
    assert_eq!(
        result["choice_surface_contract"]["has_contextual_choice"],
        true
    );
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_ignores_stale_last_card_context() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let state_path = temp.path().join("telegram_state.json");
    let out = temp.path().join("feedback_result.json");
    let chat_hash = "sha256:9dd7fefaf214ceca";
    let mut contexts = Map::new();
    contexts.insert(
        chat_hash.to_string(),
        json!({
            "schema": "telegram_interaction_context.v1",
            "context_kind": "plan_attention",
            "focus_ref": "stale_plan",
            "focus_label": "수정 필요",
            "next_command": "/show stale_plan",
            "remembered_at": "2000-01-01T00:00:00+00:00"
        }),
    );
    let state = json!({
        "schema": "remote_operator_telegram_state.v1",
        "offset": 0,
        "last_interaction_context_by_chat": Value::Object(contexts)
    });
    fs::write(&state_path, serde_json::to_string_pretty(&state)?)?;

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("/feedback 오래된 카드 맥락을 붙이지 마")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(!preview.contains("stale_plan"));
    assert_eq!(result["feedback_context"], Value::Null);
    assert_eq!(
        result["choice_surface_contract"]["has_contextual_choice"],
        false
    );
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_feedback_records_decision_inbox_item() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let update_path = temp.path().join("feedback_update.json");
    fs::write(
        &update_path,
        serde_json::to_string_pretty(&json!({
            "update_id": 500,
            "message": {
                "message_id": 777,
                "date": 1780000000,
                "chat": {"id": 123456789, "type": "private"},
                "from": {"id": 987654321, "is_bot": false, "first_name": "Operator"},
                "text": "/feedback 모바일 메시지에서 핵심만 남겨줘"
            }
        }))?,
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let out = temp.path().join("replay_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&update_path)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["mode"], "live_once");
    assert_eq!(result["feedback_recorded"], true);
    assert_eq!(result["decision_feedback_ingest_status"], "recorded");
    assert_eq!(result["decision_feedback_appended"], true);
    assert!(result["decision_feedback_decision_id"]
        .as_str()
        .expect("decision id")
        .starts_with("telegram-feedback-"));
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("의견을 검토 목록에 넣었습니다."));
    assert!(!preview.contains("모바일 메시지에서 핵심만 남겨줘"));
    assert_mobile_contract(&result);

    let feedback_rows = fs::read_to_string(&feedback_file)?;
    assert_eq!(feedback_rows.lines().count(), 1);
    assert!(feedback_rows.contains("remote_operator_telegram_feedback.v1"));

    let ledger_path = profile_dir(temp.path()).join("offdesk_decisions.jsonl");
    let ledger = fs::read_to_string(&ledger_path)?;
    assert_eq!(ledger.lines().count(), 1);
    let decision: Value = serde_json::from_str(ledger.lines().next().expect("ledger row"))?;
    assert_eq!(
        decision["decision_id"],
        result["decision_feedback_decision_id"]
    );
    assert_eq!(decision["status"], "user_pending");
    assert_eq!(
        decision["decision_request"]["kind"],
        "telegram_operator_feedback"
    );
    assert_eq!(
        decision["source_surface"],
        "telegram.remote_operator.feedback"
    );

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state["offset"], 501);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_plain_text_chat_does_not_record_feedback() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let update_path = temp.path().join("chat_update.json");
    write_text_update(&update_path, 600, 778, "챗봇이랑 대화하고 싶어")?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let out = temp.path().join("chat_replay_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&update_path)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "chat");
    assert_eq!(result["feedback_recorded"], Value::Null);
    assert_eq!(result["decision_feedback_ingest_status"], Value::Null);
    assert!(!feedback_file.exists());
    assert!(!ingest_dir.exists());
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>Forager 응답</b>"));
    assert!(!preview.contains("<b>의견 접수</b>"));
    assert_mobile_contract(&result);

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state["offset"], 601);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_chat_records_history_without_context_refresh() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let status_update = temp.path().join("status_update.json");
    write_text_update(&status_update, 630, 790, "/status")?;
    let status_out = temp.path().join("status_result.json");
    let status_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&status_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--out")
        .arg(&status_out)
        .output()?;
    assert!(
        status_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&status_output.stdout),
        String::from_utf8_lossy(&status_output.stderr)
    );
    let status_result: Value = serde_json::from_slice(&fs::read(&status_out)?)?;
    let chat_hash = status_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash")
        .to_string();
    let state_before: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let remembered_before = state_before["last_interaction_context_by_chat"][&chat_hash]
        ["remembered_at"]
        .as_str()
        .expect("remembered_at after status")
        .to_string();

    let chat_update = temp.path().join("chat_update.json");
    write_text_update(&chat_update, 631, 791, "지금 상태 요약해줘")?;
    let chat_out = temp.path().join("chat_result.json");
    let chat_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&chat_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--out")
        .arg(&chat_out)
        .output()?;
    assert!(
        chat_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&chat_output.stdout),
        String::from_utf8_lossy(&chat_output.stderr)
    );
    let chat_result: Value = serde_json::from_slice(&fs::read(&chat_out)?)?;
    assert_eq!(chat_result["parsed_command"]["command"], "chat");

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state["offset"], 632);
    let history = state["chat_history_by_chat"][&chat_hash]
        .as_array()
        .expect("chat history entries");
    assert!(history.iter().any(|entry| {
        entry["role"] == "operator"
            && entry["text"]
                .as_str()
                .is_some_and(|text| text.contains("지금 상태 요약해줘"))
    }));
    // Chat must not refresh the last card context timestamp; otherwise the
    // context-max-age expiry never fires for chatty operators.
    assert_eq!(
        state["last_interaction_context_by_chat"][&chat_hash]["remembered_at"]
            .as_str()
            .expect("remembered_at after chat"),
        remembered_before
    );
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_remember_records_adaptive_wiki_candidate() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let update_path = temp.path().join("remember_update.json");
    write_text_update(
        &update_path,
        610,
        779,
        "/remember 평문 텔레그램 메시지는 기본 채팅으로 답한다",
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let out = temp.path().join("remember_replay_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&update_path)
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "remember");
    assert_eq!(result["wiki_candidate_recorded"], true);
    assert_eq!(result["wiki_candidate_status"], "recorded");
    assert_eq!(result["feedback_recorded"], Value::Null);
    assert_eq!(result["decision_feedback_ingest_status"], Value::Null);
    assert!(!feedback_file.exists());
    assert!(!ingest_dir.exists());
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("위키 후보로 저장했습니다."));
    assert!(preview.contains("아직 런타임 지식은 아닙니다."));
    assert_mobile_contract(&result);

    let candidates_path = profile_dir(temp.path()).join("adaptive_wiki_candidates.json");
    let candidates_state: Value = serde_json::from_slice(&fs::read(&candidates_path)?)?;
    let candidates = candidates_state["candidates"]
        .as_array()
        .expect("candidate list");
    assert_eq!(candidates.len(), 1);
    let candidate = &candidates[0];
    assert_eq!(candidate["kind"], "preference");
    assert_eq!(candidate["scope"], "user_global");
    assert_eq!(
        candidate["claim"],
        "평문 텔레그램 메시지는 기본 채팅으로 답한다"
    );
    assert_eq!(candidate["signal_kind"], "explicit_preference");
    assert_eq!(candidate["origin"], "operator_explicit");
    assert_eq!(candidate["occurrence_count"], 1);
    assert!(candidate["evidence_refs"]
        .as_array()
        .expect("evidence refs")
        .iter()
        .any(|item| item == "telegram:message:779"));

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state["offset"], 611);
    assert!(state["last_interaction_context_by_chat"].is_null());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_attention_summarizes_waiting_work() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    // A task the surface flags for review counts toward the summary too.
    let tasks = json!([{
        "task_id": "task-stuck",
        "request_id": "req-1",
        "project_key": "project",
        "status": "resume_pending",
        "capability_id": "web.visual_review",
        "runner_kind": "local_background",
        "command": "echo hi",
        "workdir": "/tmp",
        "attempt_count": 1,
        "created_at": "2026-07-17T00:00:00Z",
        "updated_at": "2026-07-17T00:00:00Z"
    }]);
    fs::write(
        profile.join("offdesk_tasks.json"),
        serde_json::to_string(&tasks)?,
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let update = temp.path().join("attention_update.json");
    write_text_update(&update, 900, 1, "/attention")?;
    let out = temp.path().join("attention_out.json");
    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--out")
        .arg(&out)
        .output()?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["parsed_command"]["command"], "attention");
    let preview = result["message_preview"].as_str().expect("preview");
    assert!(preview.contains("조치 필요 요약"));
    // One decision and one review-flagged task are aggregated.
    assert!(preview.contains("결정 1"));
    assert!(preview.contains("작업 1"));
    // The most urgent action is surfaced first.
    assert!(preview.contains("/decision decision-user"));
    // ...and offered as a one-tap button.
    let keyboard = result["reply_markup_preview"]["keyboard"]
        .as_array()
        .expect("keyboard");
    let has_action_button = keyboard.iter().any(|row| {
        row.as_array().is_some_and(|labels| {
            labels.iter().any(|label| {
                label
                    .as_str()
                    .is_some_and(|text| text.contains("/decision decision-user"))
            })
        })
    });
    assert!(
        has_action_button,
        "attention card should offer the top action as a button"
    );
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_confirm_button_applies_pending_without_token() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    // The confirm card offers 확인/취소 as one-tap buttons.
    let decision_update = temp.path().join("decision_update.json");
    write_text_update(
        &decision_update,
        930,
        1,
        "/decision decision-user revise 수정",
    )?;
    let decision_out = temp.path().join("decision_out.json");
    let decision_result = replay(&decision_update, &decision_out)?;
    let keyboard = decision_result["reply_markup_preview"]["keyboard"]
        .as_array()
        .expect("keyboard");
    let has_confirm_button = keyboard.iter().any(|row| {
        row.as_array()
            .is_some_and(|labels| labels.iter().any(|label| label == "확인"))
    });
    assert!(
        has_confirm_button,
        "confirm card should offer a 확인 button"
    );

    // Tapping 확인 sends the plain label; it confirms the single pending action
    // without the operator typing the token.
    let confirm_update = temp.path().join("confirm_update.json");
    write_text_update(&confirm_update, 931, 2, "확인")?;
    let confirm_out = temp.path().join("confirm_out.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    assert_eq!(confirm_result["parsed_command"]["command"], "confirm");
    assert_eq!(confirm_result["parsed_command"]["confirm_token"], "");
    assert_eq!(confirm_result["dispatch_result"]["ok"], true);
    assert_eq!(confirm_result["dispatch_result"]["stage"], "applied");
    Ok(())
}

fn seed_pending_decision(profile: &Path) -> Result<()> {
    fs::create_dir_all(profile)?;
    let decision = json!({
        "schema": "decision_record.v1",
        "decision_id": "decision-user",
        "project_key": "project",
        "request_id": "request",
        "task_id": "approval-task",
        "raised_by": "agent",
        "source_surface": "offdesk.council",
        "materiality": "high",
        "status": "user_pending",
        "created_at": "2026-07-16T00:00:00Z",
        "updated_at": "2026-07-16T00:00:00Z",
        "decision_request": {
            "kind": "council_escalation",
            "summary": "Council recommends revising the next episode.",
            "decision_needed": "Choose whether to continue, revise, block, or stop.",
            "why_now": ["Council did not return continue."],
            "current_scope": "Next episode only.",
            "non_authorized_scope": ["provider retargeting"],
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
        profile.join("offdesk_decisions.jsonl"),
        format!("{}\n", serde_json::to_string(&decision)?),
    )?;
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_pause_is_immediate_and_resume_is_confirmed() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    fs::create_dir_all(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    // /pause is immediate: one message halts all new dispatch, no confirm step.
    let pause_update = temp.path().join("pause_update.json");
    write_text_update(&pause_update, 900, 1, "/pause 에이전트 오작동")?;
    let pause_out = temp.path().join("pause_out.json");
    let pause_result = replay(&pause_update, &pause_out)?;
    assert_eq!(pause_result["parsed_command"]["command"], "pause");
    assert_eq!(pause_result["dispatch_result"]["ok"], true);
    assert_eq!(pause_result["dispatch_result"]["paused"], true);
    assert!(pause_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("전면 정지"));
    assert_mobile_contract(&pause_result);
    // The pause state is persisted so the tick will hold new dispatch.
    let pause_state: Value =
        serde_json::from_slice(&fs::read(profile.join("offdesk_operator_pause.json"))?)?;
    assert_eq!(pause_state["paused"], true);

    // /resume is confirm-gated: it returns a confirmation card first.
    let resume_update = temp.path().join("resume_update.json");
    write_text_update(&resume_update, 901, 2, "/resume")?;
    let resume_out = temp.path().join("resume_out.json");
    let resume_result = replay(&resume_update, &resume_out)?;
    assert_eq!(resume_result["parsed_command"]["command"], "resume");
    assert!(resume_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("정지 해제 확인"));
    assert_mobile_contract(&resume_result);
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let chat_hash = resume_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash");
    let token = state["pending_dispatch_confirmations_by_chat"][chat_hash]["token"]
        .as_str()
        .expect("token")
        .to_string();

    // /confirm clears the pause.
    let confirm_update = temp.path().join("confirm_update.json");
    write_text_update(&confirm_update, 902, 3, &format!("/confirm {token}"))?;
    let confirm_out = temp.path().join("confirm_out.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    assert_eq!(confirm_result["dispatch_result"]["ok"], true);
    assert_eq!(confirm_result["dispatch_result"]["kind"], "resume");
    assert_eq!(confirm_result["dispatch_result"]["paused"], false);
    assert_mobile_contract(&confirm_result);
    let resumed_state: Value =
        serde_json::from_slice(&fs::read(profile.join("offdesk_operator_pause.json"))?)?;
    assert_eq!(resumed_state["paused"], false);
    Ok(())
}

fn seed_running_task(profile: &Path) -> Result<()> {
    fs::create_dir_all(profile)?;
    let tasks = json!([{
        "task_id": "task-run-1",
        "request_id": "req-1",
        "project_key": "Harness",
        "status": "running",
        "capability_id": "web.visual_review",
        "runner_kind": "local_background",
        "command": "forager offdesk tick",
        "workdir": "/tmp",
        "attempt_count": 1,
        "created_at": "2026-07-17T00:00:00Z",
        "updated_at": "2026-07-17T00:00:00Z"
    }]);
    fs::write(
        profile.join("offdesk_tasks.json"),
        serde_json::to_string(&tasks)?,
    )?;
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_cancel_task_after_confirmation() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_running_task(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    // /tasks lists the running, cancellable task.
    let tasks_update = temp.path().join("tasks_update.json");
    write_text_update(&tasks_update, 800, 1, "/tasks")?;
    let tasks_out = temp.path().join("tasks_out.json");
    let tasks_result = replay(&tasks_update, &tasks_out)?;
    assert_eq!(tasks_result["parsed_command"]["command"], "tasks");
    assert!(tasks_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("task-run-1"));
    assert_mobile_contract(&tasks_result);

    // /cancel-task returns a confirmation card.
    let cancel_update = temp.path().join("cancel_update.json");
    write_text_update(
        &cancel_update,
        801,
        2,
        "/cancel-task task-run-1 잘못된 실행",
    )?;
    let cancel_out = temp.path().join("cancel_out.json");
    let cancel_result = replay(&cancel_update, &cancel_out)?;
    assert_eq!(cancel_result["parsed_command"]["command"], "cancel_task");
    assert!(cancel_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("작업 취소 확인"));
    assert_mobile_contract(&cancel_result);
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let chat_hash = cancel_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash");
    let token = state["pending_dispatch_confirmations_by_chat"][chat_hash]["token"]
        .as_str()
        .expect("token")
        .to_string();

    // /confirm cancels the task.
    let confirm_update = temp.path().join("confirm_update.json");
    write_text_update(&confirm_update, 802, 3, &format!("/confirm {token}"))?;
    let confirm_out = temp.path().join("confirm_out.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    assert_eq!(confirm_result["dispatch_result"]["ok"], true);
    assert_eq!(confirm_result["dispatch_result"]["kind"], "cancel_task");
    assert_eq!(confirm_result["dispatch_result"]["changed"], true);
    assert_eq!(confirm_result["dispatch_result"]["status"], "cancelled");
    assert_mobile_contract(&confirm_result);

    // The task store reflects the cancellation.
    let stored: Value = serde_json::from_slice(&fs::read(profile.join("offdesk_tasks.json"))?)?;
    assert_eq!(stored[0]["status"], "cancelled");
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_cancel_task_reports_unknown_task() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    fs::create_dir_all(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(output.status.success());
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    let cancel_update = temp.path().join("cancel_update.json");
    write_text_update(&cancel_update, 810, 1, "/cancel-task task-missing")?;
    let cancel_out = temp.path().join("cancel_out.json");
    let cancel_result = replay(&cancel_update, &cancel_out)?;
    let chat_hash = cancel_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash");
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let token = state["pending_dispatch_confirmations_by_chat"][chat_hash]["token"]
        .as_str()
        .expect("token")
        .to_string();

    let confirm_update = temp.path().join("confirm_update.json");
    write_text_update(&confirm_update, 811, 2, &format!("/confirm {token}"))?;
    let confirm_out = temp.path().join("confirm_out.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    // An unknown task fails cleanly (no crash, error surfaced), not a wedge.
    assert_eq!(confirm_result["dispatch_result"]["ok"], false);
    assert!(confirm_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("취소 실패"));
    assert_mobile_contract(&confirm_result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_dispatch_applies_decision_after_confirmation() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    let list_update = temp.path().join("decisions_update.json");
    write_text_update(&list_update, 720, 900, "/decisions")?;
    let list_out = temp.path().join("decisions_result.json");
    let list_result = replay(&list_update, &list_out)?;
    assert_eq!(list_result["parsed_command"]["command"], "decisions");
    let list_preview = list_result["message_preview"].as_str().expect("preview");
    assert!(list_preview.contains("revise"));
    assert_mobile_contract(&list_result);

    // The decisions card must offer the open decision's action as a one-tap
    // button, so the operator dispatches it without typing the id and action.
    let list_keyboard = list_result["reply_markup_preview"]["keyboard"]
        .as_array()
        .expect("keyboard");
    let has_action_button = list_keyboard.iter().any(|row| {
        row.as_array()
            .map(|buttons| {
                buttons
                    .iter()
                    .any(|button| button.as_str() == Some("/decision decision-user revise"))
            })
            .unwrap_or(false)
    });
    assert!(
        has_action_button,
        "decisions card missing one-tap action button; keyboard: {list_keyboard:?}"
    );

    let decision_update = temp.path().join("decision_update.json");
    write_text_update(
        &decision_update,
        721,
        901,
        "/decision decision-user revise 다음 에피소드를 수정",
    )?;
    let decision_out = temp.path().join("decision_result.json");
    let decision_result = replay(&decision_update, &decision_out)?;
    assert_eq!(decision_result["parsed_command"]["command"], "decision");
    assert!(decision_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("실행 확인 필요"));
    assert_mobile_contract(&decision_result);

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let chat_hash = decision_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash");
    let token = state["pending_dispatch_confirmations_by_chat"][chat_hash]["token"]
        .as_str()
        .expect("confirmation token")
        .to_string();

    let confirm_update = temp.path().join("confirm_update.json");
    write_text_update(&confirm_update, 722, 902, &format!("/confirm {token}"))?;
    let confirm_out = temp.path().join("confirm_result.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    assert_eq!(confirm_result["parsed_command"]["command"], "confirm");
    assert_eq!(confirm_result["dispatch_result"]["ok"], true);
    assert_eq!(confirm_result["dispatch_result"]["stage"], "applied");
    assert_eq!(
        confirm_result["dispatch_result"]["execution_status"],
        "applied"
    );
    assert_eq!(confirm_result["dispatch_result"]["decision"], "revise");
    assert!(confirm_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("적용됨"));
    assert_mobile_contract(&confirm_result);

    // The canonical ledgers must reflect the applied decision action.
    assert_eq!(
        fs::read_to_string(profile.join("offdesk_decisions.jsonl"))?
            .lines()
            .count(),
        3
    );
    assert_eq!(
        fs::read_to_string(profile.join("decision_action_executions.jsonl"))?
            .lines()
            .count(),
        1
    );
    assert_eq!(
        fs::read_to_string(profile.join("decision_action_closeouts.jsonl"))?
            .lines()
            .count(),
        1
    );

    // The single-use token must be gone after a successful confirm.
    let state_after: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert!(state_after["pending_dispatch_confirmations_by_chat"][chat_hash].is_null());
    Ok(())
}

fn seed_recovery_closeout(profile: &Path) -> Result<()> {
    let closeout_dir = profile.join("offdesk_closeouts").join("completed-task");
    fs::create_dir_all(&closeout_dir)?;
    fs::write(
        closeout_dir.join("closeout_plan.json"),
        serde_json::to_string_pretty(&json!({
            "schema": "closeout_plan.v1",
            "closeout_id": "closeout-completed-task",
            "generated_at": "2026-07-16T00:00:00Z",
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
            "reviewed_at": "2026-07-16T00:01:00Z",
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
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_dispatch_validates_recovery_after_confirmation() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    fs::create_dir_all(&profile)?;
    seed_recovery_closeout(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    let list_update = temp.path().join("recovery_update.json");
    write_text_update(&list_update, 740, 940, "/recovery")?;
    let list_out = temp.path().join("recovery_result.json");
    let list_result = replay(&list_update, &list_out)?;
    assert_eq!(list_result["parsed_command"]["command"], "recovery");
    assert!(list_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("resolve_followup"));
    assert_mobile_contract(&list_result);

    let recover_update = temp.path().join("recover_update.json");
    write_text_update(
        &recover_update,
        741,
        941,
        "/recover closeout-completed-task resolve_followup 아카이브 검토",
    )?;
    let recover_out = temp.path().join("recover_out.json");
    let recover_result = replay(&recover_update, &recover_out)?;
    assert_eq!(recover_result["parsed_command"]["command"], "recover");
    assert_mobile_contract(&recover_result);

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let chat_hash = recover_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash");
    let confirmation = &state["pending_dispatch_confirmations_by_chat"][chat_hash];
    assert_eq!(confirmation["kind"], "recovery");
    let token = confirmation["token"].as_str().expect("token").to_string();

    let confirm_update = temp.path().join("recover_confirm_update.json");
    write_text_update(&confirm_update, 742, 942, &format!("/confirm {token}"))?;
    let confirm_out = temp.path().join("recover_confirm_out.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    assert_eq!(confirm_result["dispatch_result"]["ok"], true);
    assert_eq!(
        confirm_result["dispatch_result"]["stage"],
        "recovery_validated"
    );
    assert_eq!(confirm_result["dispatch_result"]["kind"], "recovery");
    assert!(confirm_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("검증됨"));
    assert_mobile_contract(&confirm_result);

    // Recovery validation records a receipt but must not record accepted truth.
    assert_eq!(
        fs::read_to_string(profile.join("accepted_truth_recovery_action_receipts.jsonl"))?
            .lines()
            .count(),
        1
    );
    assert!(!profile.join("accepted_truth.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_dispatch_queues_runtime_task_after_confirmation() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--enable-runtime-dispatch")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };
    let confirm_token = |chat_hash: &str| -> Result<String> {
        let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
        Ok(
            state["pending_dispatch_confirmations_by_chat"][chat_hash]["token"]
                .as_str()
                .expect("confirmation token")
                .to_string(),
        )
    };

    // Apply the decision so it reaches "receipted" and becomes a runtime handoff.
    let decision_update = temp.path().join("decision_update.json");
    write_text_update(
        &decision_update,
        750,
        950,
        "/decision decision-user revise 수정",
    )?;
    let decision_out = temp.path().join("decision_out.json");
    let decision_result = replay(&decision_update, &decision_out)?;
    let chat_hash = decision_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash")
        .to_string();
    let apply_update = temp.path().join("apply_update.json");
    write_text_update(
        &apply_update,
        751,
        951,
        &format!("/confirm {}", confirm_token(&chat_hash)?),
    )?;
    let apply_out = temp.path().join("apply_out.json");
    let apply_result = replay(&apply_update, &apply_out)?;
    assert_eq!(apply_result["dispatch_result"]["stage"], "applied");

    // The runtime dispatch surface should now expose the receipted closeout.
    let surface_output = Command::new(env!("CARGO_BIN_EXE_forager"))
        .arg("--profile")
        .arg("default")
        .args(["ondesk", "workstation-surface", "--json"])
        .env("HOME", temp.path())
        .env("XDG_CONFIG_HOME", temp.path().join(".config"))
        .env_remove("FORAGER_PROFILE")
        .output()?;
    assert!(surface_output.status.success());
    let surface: Value = serde_json::from_slice(&surface_output.stdout)?;
    let closeout_id = surface["runtime_dispatch"]["items"][0]["closeout_id"]
        .as_str()
        .expect("runtime dispatch closeout id")
        .to_string();

    let dispatch_update = temp.path().join("dispatch_update.json");
    write_text_update(
        &dispatch_update,
        752,
        952,
        &format!("/dispatch {closeout_id} local-background -- echo hello-from-telegram"),
    )?;
    let dispatch_out = temp.path().join("dispatch_out.json");
    let dispatch_result = replay(&dispatch_update, &dispatch_out)?;
    assert_eq!(dispatch_result["parsed_command"]["command"], "dispatch");
    assert!(dispatch_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("런타임 디스패치 확인"));
    assert_mobile_contract(&dispatch_result);

    let confirm_update = temp.path().join("dispatch_confirm_update.json");
    write_text_update(
        &confirm_update,
        753,
        953,
        &format!("/confirm {}", confirm_token(&chat_hash)?),
    )?;
    let confirm_out = temp.path().join("dispatch_confirm_out.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    assert_eq!(confirm_result["dispatch_result"]["ok"], true);
    assert_eq!(confirm_result["dispatch_result"]["stage"], "queued");
    assert_eq!(confirm_result["dispatch_result"]["kind"], "runtime");
    assert_eq!(confirm_result["dispatch_result"]["task_enqueued"], true);
    assert!(confirm_result["dispatch_result"]["task_id"]
        .as_str()
        .is_some());
    assert_mobile_contract(&confirm_result);

    assert_eq!(
        fs::read_to_string(profile.join("runtime_dispatch_receipts.jsonl"))?
            .lines()
            .count(),
        1
    );
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_run_dispatches_curated_template_without_runtime_flag() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    // Curated allowlist on the trusted local machine; the operator only names it.
    let allowlist_path = temp.path().join("dispatch_allowlist.json");
    fs::write(
        &allowlist_path,
        serde_json::to_vec(&serde_json::json!({
            "templates": [
                {
                    "name": "smoke",
                    "runner": "local-background",
                    "command": "echo hello-from-run",
                    "description": "smoke check"
                }
            ]
        }))?,
    )?;

    // Deliberately no --enable-runtime-dispatch: curated /run is the safe path.
    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--dispatch-allowlist-file")
            .arg(&allowlist_path)
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };
    let confirm_token = |chat_hash: &str| -> Result<String> {
        let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
        Ok(
            state["pending_dispatch_confirmations_by_chat"][chat_hash]["token"]
                .as_str()
                .expect("confirmation token")
                .to_string(),
        )
    };

    // Apply the decision so it reaches "receipted" and becomes a runtime handoff.
    let decision_update = temp.path().join("decision_update.json");
    write_text_update(
        &decision_update,
        770,
        970,
        "/decision decision-user revise 수정",
    )?;
    let decision_out = temp.path().join("decision_out.json");
    let decision_result = replay(&decision_update, &decision_out)?;
    let chat_hash = decision_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash")
        .to_string();
    let apply_update = temp.path().join("apply_update.json");
    write_text_update(
        &apply_update,
        771,
        971,
        &format!("/confirm {}", confirm_token(&chat_hash)?),
    )?;
    let apply_out = temp.path().join("apply_out.json");
    let apply_result = replay(&apply_update, &apply_out)?;
    assert_eq!(apply_result["dispatch_result"]["stage"], "applied");

    let surface_output = Command::new(env!("CARGO_BIN_EXE_forager"))
        .arg("--profile")
        .arg("default")
        .args(["ondesk", "workstation-surface", "--json"])
        .env("HOME", temp.path())
        .env("XDG_CONFIG_HOME", temp.path().join(".config"))
        .env_remove("FORAGER_PROFILE")
        .output()?;
    assert!(surface_output.status.success());
    let surface: Value = serde_json::from_slice(&surface_output.stdout)?;
    let closeout_id = surface["runtime_dispatch"]["items"][0]["closeout_id"]
        .as_str()
        .expect("runtime dispatch closeout id")
        .to_string();

    // /run (list) surfaces the curated template by name.
    let list_update = temp.path().join("run_list_update.json");
    write_text_update(&list_update, 772, 972, "/run")?;
    let list_out = temp.path().join("run_list_out.json");
    let list_result = replay(&list_update, &list_out)?;
    assert_eq!(list_result["parsed_command"]["command"], "run_list");
    assert!(list_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("smoke"));
    assert_mobile_contract(&list_result);

    // /run <closeout> <name> builds a confirm card and stashes a confirmation.
    let run_update = temp.path().join("run_update.json");
    write_text_update(&run_update, 773, 973, &format!("/run {closeout_id} smoke"))?;
    let run_out = temp.path().join("run_out.json");
    let run_result = replay(&run_update, &run_out)?;
    assert_eq!(run_result["parsed_command"]["command"], "run");
    assert!(run_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("실행 확인"));
    assert_mobile_contract(&run_result);

    // Confirm queues the runtime task even though --enable-runtime-dispatch is off.
    let confirm_update = temp.path().join("run_confirm_update.json");
    write_text_update(
        &confirm_update,
        774,
        974,
        &format!("/confirm {}", confirm_token(&chat_hash)?),
    )?;
    let confirm_out = temp.path().join("run_confirm_out.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    assert_eq!(confirm_result["dispatch_result"]["ok"], true);
    assert_eq!(confirm_result["dispatch_result"]["stage"], "queued");
    assert_eq!(confirm_result["dispatch_result"]["kind"], "runtime");
    assert_eq!(confirm_result["dispatch_result"]["task_enqueued"], true);
    assert_mobile_contract(&confirm_result);

    assert_eq!(
        fs::read_to_string(profile.join("runtime_dispatch_receipts.jsonl"))?
            .lines()
            .count(),
        1
    );
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_run_refuses_unknown_and_unconfigured() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    fs::create_dir_all(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let allowlist_path = temp.path().join("dispatch_allowlist.json");
    fs::write(
        &allowlist_path,
        serde_json::to_vec(&serde_json::json!({
            "templates": [
                {"name": "smoke", "runner": "local-background", "command": "echo hi"}
            ]
        }))?,
    )?;

    let replay = |update: &Path, out: &Path, allowlist: Option<&Path>| -> Result<Value> {
        let mut command = remote_operator_command(temp.path());
        command
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out);
        if let Some(path) = allowlist {
            command.arg("--dispatch-allowlist-file").arg(path);
        }
        let output = command.output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    // Unknown template name: no confirmation is stored.
    let unknown_update = temp.path().join("run_unknown_update.json");
    write_text_update(&unknown_update, 780, 980, "/run closeout-x nope")?;
    let unknown_out = temp.path().join("run_unknown_out.json");
    let unknown_result = replay(&unknown_update, &unknown_out, Some(&allowlist_path))?;
    assert_eq!(unknown_result["parsed_command"]["command"], "run");
    assert!(unknown_result["dispatch_result"].is_null());
    assert!(unknown_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("템플릿이 없습니다"));
    assert_mobile_contract(&unknown_result);
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert!(
        state["pending_dispatch_confirmations_by_chat"].is_null()
            || state["pending_dispatch_confirmations_by_chat"]
                .as_object()
                .expect("map")
                .is_empty()
    );

    // No allowlist configured: /run is refused as not configured.
    let disabled_update = temp.path().join("run_disabled_update.json");
    write_text_update(&disabled_update, 781, 981, "/run closeout-x smoke")?;
    let disabled_out = temp.path().join("run_disabled_out.json");
    let disabled_result = replay(&disabled_update, &disabled_out, None)?;
    assert_eq!(disabled_result["parsed_command"]["command"], "run");
    assert!(disabled_result["dispatch_result"].is_null());
    assert!(disabled_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("비활성"));
    assert_mobile_contract(&disabled_result);

    assert!(!profile.join("runtime_dispatch_receipts.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_dispatch_refuses_runtime_when_disabled() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    fs::create_dir_all(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    // No --enable-runtime-dispatch flag: /dispatch must be refused outright.
    let dispatch_update = temp.path().join("dispatch_update.json");
    write_text_update(
        &dispatch_update,
        760,
        960,
        "/dispatch closeout-x local-background -- rm -rf /",
    )?;
    let out = temp.path().join("dispatch_out.json");
    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&dispatch_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--out")
        .arg(&out)
        .output()?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["parsed_command"]["command"], "dispatch");
    assert!(result["dispatch_result"].is_null());
    assert!(result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("비활성"));
    assert_mobile_contract(&result);
    // No confirmation may be stored for a refused dispatch.
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert!(
        state["pending_dispatch_confirmations_by_chat"].is_null()
            || state["pending_dispatch_confirmations_by_chat"]
                .as_object()
                .expect("map")
                .is_empty()
    );
    assert!(!profile.join("runtime_dispatch_receipts.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_dispatch_rejects_stale_confirmation() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    let decision_update = temp.path().join("decision_update.json");
    write_text_update(
        &decision_update,
        730,
        910,
        "/decision decision-user revise 수정",
    )?;
    let decision_out = temp.path().join("decision_result.json");
    let decision_result = replay(&decision_update, &decision_out)?;
    let chat_hash = decision_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash")
        .to_string();
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let token = state["pending_dispatch_confirmations_by_chat"][&chat_hash]["token"]
        .as_str()
        .expect("confirmation token")
        .to_string();

    // Mutate the decision so its observed hash no longer matches the token.
    let mut decision: Value = serde_json::from_str(
        fs::read_to_string(profile.join("offdesk_decisions.jsonl"))?
            .lines()
            .next()
            .expect("decision line"),
    )?;
    decision["updated_at"] = json!("2026-07-16T09:00:00Z");
    decision["decision_request"]["summary"] = json!("Changed direction.");
    fs::write(
        profile.join("offdesk_decisions.jsonl"),
        format!("{}\n", serde_json::to_string(&decision)?),
    )?;

    let confirm_update = temp.path().join("confirm_update.json");
    write_text_update(&confirm_update, 731, 911, &format!("/confirm {token}"))?;
    let confirm_out = temp.path().join("confirm_result.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    assert!(confirm_result["dispatch_result"].is_null());
    assert!(confirm_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("변경"));
    assert_mobile_contract(&confirm_result);
    // A stale confirmation must never write an execution.
    assert!(!profile.join("decision_action_executions.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_attention_notify_pushes_then_dedupes() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--attention-notify")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    // First poll: the waiting decision is proactively pushed to the owner chat.
    let first_update = temp.path().join("poll1.json");
    write_text_update(&first_update, 900, 1, "/status")?;
    let first_out = temp.path().join("poll1_out.json");
    let first = replay(&first_update, &first_out)?;
    let notification = &first["attention_notification"];
    assert_eq!(notification["status"], "notified");
    assert_eq!(notification["notified_count"], 1);
    let preview = notification["message_preview"].as_str().expect("preview");
    assert!(preview.contains("조치 필요"));
    assert!(preview.contains("/decision decision-user"));
    // The attention card is a separate send to the owner chat, mobile-scannable.
    let contract = &notification["mobile_card_contract"];
    assert!(contract["warnings"]
        .as_array()
        .expect("warnings")
        .is_empty());

    // The notified item is recorded so it is not pushed again.
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert!(state["attention_notified_by_key"]["decision:decision-user"].is_object());

    // Second poll: the same decision is still open but must not re-notify.
    let second_update = temp.path().join("poll2.json");
    write_text_update(&second_update, 901, 2, "/status")?;
    let second_out = temp.path().join("poll2_out.json");
    let second = replay(&second_update, &second_out)?;
    assert_eq!(
        second["attention_notification"]["status"],
        "no_new_attention"
    );
    assert_eq!(second["attention_notification"]["pending_count"], 1);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_attention_notify_off_by_default() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let update = temp.path().join("poll.json");
    write_text_update(&update, 910, 1, "/status")?;
    let out = temp.path().join("poll_out.json");

    // Without --attention-notify no proactive scan runs.
    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--out")
        .arg(&out)
        .output()?;
    assert!(output.status.success());
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert!(result["attention_notification"].is_null());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_dispatch_confirm_with_control_char_note_does_not_wedge() -> Result<()> {
    // An operator note containing a NUL byte makes the CLI subprocess raise
    // ValueError; the confirm path must convert that into an error card and
    // still advance the poll offset instead of re-delivering the update.
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "adapter must exit cleanly (no crash)\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    let decision_update = temp.path().join("decision_update.json");
    let note_with_nul = format!("revise{}tail", '\u{0}');
    write_text_update(
        &decision_update,
        740,
        940,
        &format!("/decision decision-user revise {note_with_nul}"),
    )?;
    let decision_out = temp.path().join("decision_out.json");
    let decision_result = replay(&decision_update, &decision_out)?;
    let chat_hash = decision_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash")
        .to_string();
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let token = state["pending_dispatch_confirmations_by_chat"][&chat_hash]["token"]
        .as_str()
        .expect("token")
        .to_string();

    let confirm_update = temp.path().join("confirm_update.json");
    write_text_update(&confirm_update, 741, 941, &format!("/confirm {token}"))?;
    let confirm_out = temp.path().join("confirm_out.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    // No execution recorded, an error card rendered, and the offset advanced.
    assert!(confirm_result["dispatch_result"].is_null());
    assert!(confirm_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("실패"));
    let state_after: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state_after["offset"], 742);
    assert!(!profile.join("decision_action_executions.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_dispatch_rejects_expired_confirmation() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    let decision_update = temp.path().join("decision_update.json");
    write_text_update(
        &decision_update,
        770,
        970,
        "/decision decision-user revise 수정",
    )?;
    let decision_out = temp.path().join("decision_out.json");
    let decision_result = replay(&decision_update, &decision_out)?;
    let chat_hash = decision_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash")
        .to_string();

    // Age the confirmation past its TTL by rewriting created_at into the past.
    let mut state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let token = state["pending_dispatch_confirmations_by_chat"][&chat_hash]["token"]
        .as_str()
        .expect("token")
        .to_string();
    state["pending_dispatch_confirmations_by_chat"][&chat_hash]["created_at"] =
        json!("2020-01-01T00:00:00+00:00");
    fs::write(&state_path, serde_json::to_string_pretty(&state)?)?;

    let confirm_update = temp.path().join("confirm_update.json");
    write_text_update(&confirm_update, 771, 971, &format!("/confirm {token}"))?;
    let confirm_out = temp.path().join("confirm_out.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    assert!(confirm_result["dispatch_result"].is_null());
    assert!(confirm_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("만료"));
    assert_mobile_contract(&confirm_result);
    assert!(!profile.join("decision_action_executions.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_dispatch_cancel_clears_pending_confirmation() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    let decision_update = temp.path().join("decision_update.json");
    write_text_update(
        &decision_update,
        780,
        980,
        "/decision decision-user revise 수정",
    )?;
    let decision_out = temp.path().join("decision_out.json");
    let decision_result = replay(&decision_update, &decision_out)?;
    let chat_hash = decision_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash")
        .to_string();
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let token = state["pending_dispatch_confirmations_by_chat"][&chat_hash]["token"]
        .as_str()
        .expect("token")
        .to_string();

    let cancel_update = temp.path().join("cancel_update.json");
    write_text_update(&cancel_update, 781, 981, "/cancel")?;
    let cancel_out = temp.path().join("cancel_out.json");
    let cancel_result = replay(&cancel_update, &cancel_out)?;
    assert_eq!(cancel_result["parsed_command"]["command"], "cancel");
    assert!(cancel_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("취소"));
    assert_mobile_contract(&cancel_result);
    let cleared_state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert!(cleared_state["pending_dispatch_confirmations_by_chat"][&chat_hash].is_null());

    // The cancelled token must no longer confirm anything.
    let confirm_update = temp.path().join("confirm_update.json");
    write_text_update(&confirm_update, 782, 982, &format!("/confirm {token}"))?;
    let confirm_out = temp.path().join("confirm_out.json");
    let confirm_result = replay(&confirm_update, &confirm_out)?;
    assert!(confirm_result["dispatch_result"].is_null());
    assert!(confirm_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("찾을 수 없습니다"));
    assert!(!profile.join("decision_action_executions.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_dispatch_new_confirmation_supersedes_old() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let profile = profile_dir(temp.path());
    seed_pending_decision(&profile)?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update: &Path, out: &Path| -> Result<Value> {
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(serde_json::from_slice(&fs::read(out)?)?)
    };

    let first_update = temp.path().join("first_decision.json");
    write_text_update(
        &first_update,
        790,
        990,
        "/decision decision-user revise 수정",
    )?;
    let first_out = temp.path().join("first_out.json");
    let first_result = replay(&first_update, &first_out)?;
    let chat_hash = first_result["target_chat_id_hash"]
        .as_str()
        .expect("chat hash")
        .to_string();
    assert!(first_result["superseded_pending_confirmation"].is_null());
    let first_state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let first_token = first_state["pending_dispatch_confirmations_by_chat"][&chat_hash]["token"]
        .as_str()
        .expect("first token")
        .to_string();

    // A second /decision replaces the pending confirmation.
    let second_update = temp.path().join("second_decision.json");
    write_text_update(
        &second_update,
        791,
        991,
        "/decision decision-user block 보류",
    )?;
    let second_out = temp.path().join("second_out.json");
    let second_result = replay(&second_update, &second_out)?;
    assert_eq!(second_result["superseded_pending_confirmation"], true);

    // The first token is now dead.
    let confirm_first = temp.path().join("confirm_first.json");
    write_text_update(&confirm_first, 792, 992, &format!("/confirm {first_token}"))?;
    let confirm_first_out = temp.path().join("confirm_first_out.json");
    let confirm_first_result = replay(&confirm_first, &confirm_first_out)?;
    assert!(confirm_first_result["dispatch_result"].is_null());
    assert!(confirm_first_result["message_preview"]
        .as_str()
        .expect("preview")
        .contains("찾을 수 없습니다"));
    assert!(!profile.join("decision_action_executions.jsonl").exists());
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_plan_request_creates_project_selection_session() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let workspace_root = temp.path().join("workspace");
    fs::create_dir_all(workspace_root.join("Alpha"))?;
    fs::create_dir_all(workspace_root.join("Beta"))?;
    fs::write(
        workspace_root.join("Alpha").join("README.md"),
        "Alpha project\n",
    )?;
    fs::write(
        workspace_root.join("Alpha").join("Cargo.toml"),
        "[package]\nname = \"alpha\"\n",
    )?;
    fs::write(
        workspace_root.join("Beta").join("README.md"),
        "Beta project\n",
    )?;
    fs::write(
        workspace_root.join("Beta").join("pyproject.toml"),
        "[project]\nname = \"beta\"\n",
    )?;
    let update_path = temp.path().join("plan_update.json");
    write_text_update(
        &update_path,
        700,
        880,
        "/plan Alpha 프로젝트를 오늘 밤 자율주행 계획으로 잡아줘",
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let out = temp.path().join("plan_replay_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&update_path)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "plan_request");
    assert_eq!(
        result["parsed_command"]["feedback_kind"],
        "planning_request"
    );
    assert_eq!(
        result["remote_plan_session"]["schema"],
        "telegram_remote_plan_session.v1"
    );
    assert_eq!(result["remote_plan_session"]["stage"], "project_selection");
    assert_eq!(result["remote_plan_session"]["execution_authorized"], false);
    assert_eq!(
        result["remote_plan_session"]["candidates"][0]["display_name"],
        "Alpha"
    );
    assert_eq!(
        result["interaction_context"]["context_kind"],
        "remote_plan_project_selection"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 대상 선택</b>"));
    assert!(preview.contains("/select"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"1 Alpha".to_string()));
    assert!(button_texts(&result).contains(&"다시 스캔".to_string()));
    assert!(button_texts(&result).contains(&"보류".to_string()));
    assert_mobile_contract(&result);

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state["offset"], 701);
    assert_eq!(
        state["remote_plan_sessions_by_chat"][result["target_chat_id_hash"].as_str().unwrap()]
            ["stage"],
        "project_selection"
    );
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_delegation_capture_confirm_starts_plan_session() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let workspace_root = temp.path().join("workspace");
    fs::create_dir_all(workspace_root.join("Alpha"))?;
    fs::write(
        workspace_root.join("Alpha").join("README.md"),
        "Alpha project\n",
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update_path: &Path, out: &Path| -> Result<std::process::Output> {
        Ok(remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update_path)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--workspace-root")
            .arg(&workspace_root)
            .arg("--out")
            .arg(out)
            .output()?)
    };

    let delegation_update = temp.path().join("delegation_update.json");
    write_text_update(
        &delegation_update,
        710,
        890,
        "그럼 Alpha에서 마지막 기록을 확인하고 새로 방향을 검토해볼까",
    )?;
    let capture_out = temp.path().join("capture_result.json");
    let output = replay(&delegation_update, &capture_out)?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let capture: Value = serde_json::from_slice(&fs::read(&capture_out)?)?;
    assert_eq!(capture["status"], "rendered");
    assert_eq!(capture["parsed_command"]["command"], "chat");
    assert_eq!(capture["plan_capture_offered"], true);
    let capture_preview = capture["message_preview"]
        .as_str()
        .expect("capture preview");
    assert!(capture_preview.contains("<b>계획 후보로 등록할까요?</b>"));
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let chat_hash = capture["target_chat_id_hash"].as_str().expect("chat hash");
    assert_eq!(
        state["pending_dispatch_confirmations_by_chat"][chat_hash]["kind"],
        "plan_capture"
    );

    let confirm_update = temp.path().join("confirm_update.json");
    write_text_update(&confirm_update, 711, 891, "/confirm")?;
    let confirm_out = temp.path().join("confirm_result.json");
    let output = replay(&confirm_update, &confirm_out)?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let confirmed: Value = serde_json::from_slice(&fs::read(&confirm_out)?)?;
    assert_eq!(confirmed["status"], "rendered");
    assert_eq!(confirmed["feedback_recorded"], true);
    assert_eq!(confirmed["decision_feedback_ingest_status"], "recorded");
    assert_eq!(
        confirmed["remote_plan_session"]["schema"],
        "telegram_remote_plan_session.v1"
    );
    assert_eq!(
        confirmed["remote_plan_session"]["stage"],
        "project_selection"
    );
    assert_eq!(
        confirmed["remote_plan_session"]["execution_authorized"],
        false
    );
    let confirm_preview = confirmed["message_preview"]
        .as_str()
        .expect("confirm preview");
    assert!(confirm_preview.contains("<b>계획 대상 선택</b>"));
    assert_mobile_contract(&confirmed);

    let feedback_rows = fs::read_to_string(&feedback_file)?;
    assert_eq!(feedback_rows.lines().count(), 1);
    assert!(feedback_rows.contains("planning_request"));

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(
        state["pending_dispatch_confirmations_by_chat"][chat_hash],
        Value::Null
    );
    assert_eq!(
        state["remote_plan_sessions_by_chat"][chat_hash]["stage"],
        "project_selection"
    );
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_plan_session_plain_text_stays_chat() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let workspace_root = temp.path().join("workspace");
    fs::create_dir_all(workspace_root.join("Alpha"))?;
    fs::write(
        workspace_root.join("Alpha").join("README.md"),
        "Alpha project\n",
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let first_update = temp.path().join("plan_update.json");
    write_text_update(
        &first_update,
        705,
        885,
        "/plan Alpha 프로젝트를 오늘 밤 자율주행 계획으로 잡아줘",
    )?;
    let first_out = temp.path().join("plan_result.json");
    let first_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&first_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&first_out)
        .output()?;
    assert!(
        first_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );

    let second_update = temp.path().join("plain_chat_update.json");
    write_text_update(&second_update, 706, 886, "이 계획 상태를 설명해줘")?;
    let second_out = temp.path().join("plain_chat_result.json");
    let second_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&second_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&second_out)
        .output()?;
    assert!(
        second_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&second_output.stdout),
        String::from_utf8_lossy(&second_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&second_out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "chat");
    assert_eq!(result["feedback_recorded"], Value::Null);
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>Forager 응답</b>"));
    assert!(!preview.contains("<b>계획 대상 선택됨</b>"));
    assert_mobile_contract(&result);

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state["offset"], 707);
    assert_eq!(
        state["remote_plan_sessions_by_chat"][result["target_chat_id_hash"].as_str().unwrap()]
            ["stage"],
        "project_selection"
    );
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_plan_session_unmatched_short_text_stays_chat() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let workspace_root = temp.path().join("workspace");
    fs::create_dir_all(workspace_root.join("Alpha"))?;
    fs::write(
        workspace_root.join("Alpha").join("README.md"),
        "Alpha project\n",
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let first_update = temp.path().join("plan_update.json");
    write_text_update(
        &first_update,
        721,
        901,
        "/plan Alpha 프로젝트를 오늘 밤 자율주행 계획으로 잡아줘",
    )?;
    let first_out = temp.path().join("plan_result.json");
    let first_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&first_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&first_out)
        .output()?;
    assert!(
        first_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );

    // Short text with no question marker that matches no candidate, no existing
    // directory, and no workspace project name. This previously crashed session
    // routing with a NameError and poisoned the update offset.
    let second_update = temp.path().join("unmatched_short_text_update.json");
    write_text_update(&second_update, 722, 902, "Zeta")?;
    let second_out = temp.path().join("unmatched_short_text_result.json");
    let second_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&second_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&second_out)
        .output()?;
    assert!(
        second_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&second_output.stdout),
        String::from_utf8_lossy(&second_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&second_out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "chat");
    assert_eq!(result["feedback_recorded"], Value::Null);
    assert_mobile_contract(&result);

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state["offset"], 723);
    assert_eq!(
        state["remote_plan_sessions_by_chat"][result["target_chat_id_hash"].as_str().unwrap()]
            ["stage"],
        "project_selection"
    );
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_plan_session_accepts_direct_project_input() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let workspace_root = temp.path().join("workspace");
    fs::create_dir_all(workspace_root.join("Alpha"))?;
    fs::create_dir_all(workspace_root.join("Beta"))?;
    fs::write(
        workspace_root.join("Alpha").join("README.md"),
        "Alpha project\n",
    )?;
    fs::write(
        workspace_root.join("Alpha").join("Cargo.toml"),
        "[package]\nname = \"alpha\"\n",
    )?;
    fs::write(
        workspace_root.join("Beta").join("README.md"),
        "Beta project\n",
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let first_update = temp.path().join("plan_update.json");
    write_text_update(
        &first_update,
        710,
        890,
        "/plan Alpha 프로젝트를 오늘 밤 자율주행 계획으로 잡아줘",
    )?;
    let first_out = temp.path().join("plan_replay_result.json");

    let first_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&first_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&first_out)
        .output()?;
    assert!(
        first_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );

    let second_update = temp.path().join("selection_update.json");
    write_text_update(&second_update, 711, 891, "1번")?;
    let second_out = temp.path().join("selection_result.json");
    let second_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&second_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&second_out)
        .output()?;

    assert!(
        second_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&second_output.stdout),
        String::from_utf8_lossy(&second_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&second_out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(result["parsed_command"]["selection_status"], "selected");
    assert_eq!(result["remote_plan_session"]["stage"], "project_selected");
    assert_eq!(
        result["remote_plan_session"]["selected_candidate"]["display_name"],
        "Alpha"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 대상 선택됨</b>"));
    assert!(preview.contains("초기화 검토"));
    assert!(preview.contains("아직 실행은 시작하지 않았습니다."));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"초기화 검토".to_string()));
    assert!(button_texts(&result).contains(&"다시 선택".to_string()));
    assert_mobile_contract(&result);

    let feedback_rows = fs::read_to_string(&feedback_file)?;
    assert_eq!(feedback_rows.lines().count(), 1);
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state["offset"], 712);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_plan_session_searches_workspace_before_manual_fallback(
) -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let workspace_root = temp.path().join("workspace");
    for name in ["Alpha", "Beta", "Gamma", "TwinPaper"] {
        fs::create_dir_all(workspace_root.join(name))?;
        fs::write(
            workspace_root.join(name).join("README.md"),
            format!("{name} project\n"),
        )?;
    }
    fs::write(
        workspace_root.join("Alpha").join("Cargo.toml"),
        "[package]\nname = \"alpha\"\n",
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let first_update = temp.path().join("plan_update.json");
    write_text_update(
        &first_update,
        713,
        892,
        "/plan Alpha 프로젝트를 오늘 밤 자율주행 계획으로 잡아줘",
    )?;
    let first_out = temp.path().join("plan_replay_result.json");
    let first_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&first_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&first_out)
        .output()?;
    assert!(
        first_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );
    let first_result: Value = serde_json::from_slice(&fs::read(&first_out)?)?;
    let first_buttons = button_texts(&first_result);
    assert!(!first_buttons
        .iter()
        .any(|button| button.contains("TwinPaper")));

    let second_update = temp.path().join("workspace_search_selection_update.json");
    write_text_update(&second_update, 714, 893, "TwinPaper")?;
    let second_out = temp.path().join("workspace_search_selection_result.json");
    let second_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&second_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&second_out)
        .output()?;

    assert!(
        second_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&second_output.stdout),
        String::from_utf8_lossy(&second_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&second_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "selected_by_search"
    );
    assert_eq!(result["remote_plan_session"]["stage"], "project_selected");
    assert_eq!(
        result["remote_plan_session"]["selected_candidate"]["display_name"],
        "TwinPaper"
    );
    assert_eq!(
        result["remote_plan_session"]["selected_candidate"]["resolved_by"],
        "workspace_search"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 대상 선택됨</b>"));
    assert!(preview.contains("TwinPaper"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_plan_session_builds_init_preview_receipt() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let workspace_root = temp.path().join("workspace");
    fs::create_dir_all(workspace_root.join("Alpha"))?;
    fs::write(
        workspace_root.join("Alpha").join("README.md"),
        "Alpha project\n",
    )?;
    fs::write(
        workspace_root.join("Alpha").join("Cargo.toml"),
        "[package]\nname = \"alpha\"\n",
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let plan_artifact_dir = temp.path().join("plan_artifacts");
    let first_update = temp.path().join("plan_update.json");
    write_text_update(
        &first_update,
        730,
        910,
        "/plan Alpha 프로젝트를 오늘 밤 자율주행 계획으로 잡아줘",
    )?;
    let first_out = temp.path().join("plan_replay_result.json");
    let first_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&first_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&first_out)
        .output()?;
    assert!(
        first_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );

    let second_update = temp.path().join("selection_update.json");
    write_text_update(&second_update, 731, 911, "1번")?;
    let second_out = temp.path().join("selection_result.json");
    let second_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&second_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&second_out)
        .output()?;
    assert!(
        second_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&second_output.stdout),
        String::from_utf8_lossy(&second_output.stderr)
    );

    let third_update = temp.path().join("init_preview_update.json");
    write_text_update(&third_update, 732, 912, "초기화 검토")?;
    let third_out = temp.path().join("init_preview_result.json");
    let third_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&third_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&third_out)
        .output()?;
    assert!(
        third_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&third_output.stdout),
        String::from_utf8_lossy(&third_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&third_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "init_previewed"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "project_init_previewed"
    );
    assert_eq!(
        result["remote_plan_session"]["project_init_preview"]["schema"],
        "telegram_remote_project_init_preview.v1"
    );
    assert!(
        result["remote_plan_session"]["project_init_preview"]["recommended_next_command"]
            .as_array()
            .expect("command preview")
            .iter()
            .any(|item| item == "<workspace_path>")
    );
    let artifact_path = result["remote_plan_session"]["project_init_preview"]["artifact_path"]
        .as_str()
        .expect("artifact path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(
        artifact["schema"],
        "telegram_remote_project_init_preview.v1"
    );
    assert_eq!(artifact["display_name"], "Alpha");
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert!(artifact["root_markers"]
        .as_array()
        .expect("markers")
        .iter()
        .any(|item| item == "README.md"));
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>초기화 검토 준비</b>"));
    assert!(preview.contains("초기화 검토 기록을 저장했습니다."));
    assert!(preview.contains("아직 실행은 시작하지 않았습니다."));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"초기화 생성".to_string()));
    assert_mobile_contract(&result);

    let fourth_update = temp.path().join("init_create_update.json");
    write_text_update(&fourth_update, 733, 913, "초기화 생성")?;
    let fourth_out = temp.path().join("init_create_result.json");
    let fourth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&fourth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&fourth_out)
        .output()?;
    assert!(
        fourth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&fourth_output.stdout),
        String::from_utf8_lossy(&fourth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&fourth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(result["parsed_command"]["selection_status"], "init_created");
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "project_init_created"
    );
    assert_eq!(
        result["remote_plan_session"]["project_init_run"]["schema"],
        "telegram_remote_project_init_run.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["project_init_run"]["status"],
        "created"
    );
    let public_command = result["remote_plan_session"]["project_init_run"]["command"]
        .as_array()
        .expect("public command");
    assert!(public_command.iter().any(|item| item == "<workspace_path>"));
    assert!(!public_command.iter().any(|item| {
        item.as_str()
            .unwrap_or_default()
            .contains(workspace_root.to_str().expect("workspace path"))
    }));
    assert!(
        result["remote_plan_session"]["project_init_run"]["project_init_output"]["project_root"]
            .is_null()
    );
    assert!(
        result["remote_plan_session"]["project_init_run"]["project_init_output"]["artifact_dir"]
            .is_null()
    );
    assert!(
        result["remote_plan_session"]["project_init_run"]["project_init_output"]
            ["project_root_hash"]
            .is_string()
    );
    assert!(
        result["remote_plan_session"]["project_init_run"]["project_init_output"]
            ["artifact_dir_hash"]
            .is_string()
    );
    let artifact_path = result["remote_plan_session"]["project_init_run"]["artifact_path"]
        .as_str()
        .expect("run artifact path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(artifact["schema"], "telegram_remote_project_init_run.v1");
    assert_eq!(artifact["status"], "created");
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert_eq!(
        artifact["project_init_output"]["kind"],
        "forager_project_initialization"
    );
    assert_eq!(
        artifact["project_init_output"]["read_only_project_state"],
        true
    );
    assert_eq!(
        artifact["project_init_output"]["requires_operator_review"],
        true
    );
    let package_path = artifact["project_init_output"]["artifacts"]
        ["ondesk_start_package_markdown"]
        .as_str()
        .expect("ondesk package path");
    assert!(Path::new(package_path).exists());
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>초기화 패킷 생성됨</b>"));
    assert!(preview.contains("초기화 패킷을 저장했습니다."));
    assert!(preview.contains("아직 실행은 시작하지 않았습니다."));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"계획 초안 생성".to_string()));
    assert_mobile_contract(&result);

    let fifth_update = temp.path().join("plan_draft_update.json");
    write_text_update(&fifth_update, 734, 914, "계획 초안 생성")?;
    let fifth_out = temp.path().join("plan_draft_result.json");
    let fifth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&fifth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&fifth_out)
        .output()?;
    assert!(
        fifth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&fifth_output.stdout),
        String::from_utf8_lossy(&fifth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&fifth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_draft_validated"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_draft_validated"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_draft"]["schema"],
        "telegram_remote_plan_draft.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_draft"]["status"],
        "validated"
    );
    assert_eq!(result["remote_plan_session"]["plan_draft"]["dry_run"], true);
    assert_eq!(
        result["remote_plan_session"]["plan_draft"]["validation_output"]["schema"],
        "offdesk_plan_registration.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_draft"]["validation_output"]["dry_run"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_draft"]["validation_output"]
            ["ready_for_operator_review"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_draft"]["validation_output"]
            ["ready_for_launch_preparation"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_draft"]["validation_output"]["ready_for_enqueue"],
        false
    );
    assert!(
        result["remote_plan_session"]["plan_draft"]["validation_output"]["source_path"].is_null()
    );
    assert!(
        result["remote_plan_session"]["plan_draft"]["validation_output"]["source_path_hash"]
            .is_string()
    );
    assert!(result["remote_plan_session"]["plan_draft"]["plan_artifact_path"].is_null());
    assert!(result["remote_plan_session"]["plan_draft"]["plan_artifact_path_hash"].is_string());
    let public_command = result["remote_plan_session"]["plan_draft"]["validation_command"]
        .as_array()
        .expect("plan draft command");
    assert!(public_command
        .iter()
        .any(|item| item == "<plan_draft_path>"));
    assert!(!public_command.iter().any(|item| {
        item.as_str()
            .unwrap_or_default()
            .contains(workspace_root.to_str().expect("workspace path"))
    }));
    let artifact_path = result["remote_plan_session"]["plan_draft"]["artifact_path"]
        .as_str()
        .expect("plan draft receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(artifact["schema"], "telegram_remote_plan_draft.v1");
    assert_eq!(artifact["status"], "validated");
    assert_eq!(artifact["dry_run"], true);
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert_eq!(
        artifact["validation_output"]["schema"],
        "offdesk_plan_registration.v1"
    );
    assert_eq!(artifact["validation_output"]["dry_run"], true);
    assert!(artifact["validation_output"]["artifacts"]["registry_dir"].is_null());
    let plan_path = artifact["plan_artifact_path"]
        .as_str()
        .expect("plan draft path");
    let plan: Value = serde_json::from_slice(&fs::read(plan_path)?)?;
    assert_eq!(plan["schema"], "offdesk_multiturn_plan.v1");
    assert_eq!(plan["decision"]["ready_for_operator_review"], true);
    assert_eq!(plan["decision"]["ready_for_launch_preparation"], false);
    assert_eq!(plan["decision"]["ready_for_enqueue"], false);
    assert!(
        plan["execution_sequence"]
            .as_array()
            .expect("execution sequence")
            .len()
            >= 2
    );
    assert!(plan["authority"]["does_not_authorize"]
        .as_array()
        .expect("denials")
        .iter()
        .any(|item| item == "launch"));
    assert!(!profile_dir(temp.path()).join("offdesk_plans").exists());
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 초안 검증됨</b>"));
    assert!(preview.contains("계획 초안을 저장했습니다."));
    assert!(preview.contains("계획 등록/실행은 아직 하지 않았습니다."));
    assert!(preview.contains("아래 버튼으로 계획 등록"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"계획 등록".to_string()));
    assert_mobile_contract(&result);

    let sixth_update = temp.path().join("plan_register_update.json");
    write_text_update(&sixth_update, 735, 915, "계획 등록")?;
    let sixth_out = temp.path().join("plan_register_result.json");
    let sixth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&sixth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&sixth_out)
        .output()?;
    assert!(
        sixth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&sixth_output.stdout),
        String::from_utf8_lossy(&sixth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&sixth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_registered"
    );
    assert_eq!(result["remote_plan_session"]["stage"], "plan_registered");
    assert_eq!(
        result["remote_plan_session"]["plan_registration"]["schema"],
        "telegram_remote_plan_registration.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_registration"]["status"],
        "registered"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_registration"]["execution_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_registration"]["runtime_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_registration"]["approval_authorized"],
        false
    );
    assert!(result["remote_plan_session"]["plan_registration"]["plan_artifact_path"].is_null());
    assert!(
        result["remote_plan_session"]["plan_registration"]["plan_artifact_path_hash"].is_string()
    );
    let public_command = result["remote_plan_session"]["plan_registration"]["registration_command"]
        .as_array()
        .expect("registration command");
    assert!(public_command
        .iter()
        .any(|item| item == "<plan_draft_path>"));
    assert!(!public_command.iter().any(|item| {
        item.as_str()
            .unwrap_or_default()
            .contains(workspace_root.to_str().expect("workspace path"))
    }));
    assert_eq!(
        result["remote_plan_session"]["plan_registration"]["registration_output"]["schema"],
        "offdesk_plan_registration.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_registration"]["registration_output"]["dry_run"],
        false
    );
    assert!(
        result["remote_plan_session"]["plan_registration"]["registration_output"]["source_path"]
            .is_null()
    );
    assert!(
        result["remote_plan_session"]["plan_registration"]["registration_output"]
            ["source_path_hash"]
            .is_string()
    );
    assert!(
        result["remote_plan_session"]["plan_registration"]["registration_output"]["artifacts"]
            ["registration_json"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    assert!(
        result["remote_plan_session"]["plan_registration"]["registration_output"]["artifacts"]
            ["copied_source_json"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    let artifact_path = result["remote_plan_session"]["plan_registration"]["artifact_path"]
        .as_str()
        .expect("registration receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(artifact["schema"], "telegram_remote_plan_registration.v1");
    assert_eq!(artifact["status"], "registered");
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert_eq!(artifact["approval_authorized"], false);
    assert_eq!(
        artifact["registration_output"]["schema"],
        "offdesk_plan_registration.v1"
    );
    assert_eq!(artifact["registration_output"]["dry_run"], false);
    assert_eq!(
        artifact["registration_output"]["ready_for_operator_review"],
        true
    );
    assert_eq!(
        artifact["registration_output"]["ready_for_launch_preparation"],
        false
    );
    assert_eq!(artifact["registration_output"]["ready_for_enqueue"], false);
    let registration_path = artifact["registration_output"]["artifacts"]["registration_json"]
        .as_str()
        .expect("registration json path");
    let copied_source_path = artifact["registration_output"]["artifacts"]["copied_source_json"]
        .as_str()
        .expect("copied source path");
    assert!(Path::new(registration_path).exists());
    assert!(Path::new(copied_source_path).exists());
    assert!(profile_dir(temp.path()).join("offdesk_plans").exists());
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 등록됨</b>"));
    assert!(preview.contains("계획을 등록했습니다."));
    assert!(preview.contains("아직 실행은 시작하지 않았습니다."));
    assert!(preview.contains("로컬에서 계획 검토"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"계획 승인".to_string()));
    assert_mobile_contract(&result);

    let seventh_update = temp.path().join("plan_review_update.json");
    write_text_update(&seventh_update, 736, 916, "계획 승인")?;
    let seventh_out = temp.path().join("plan_review_result.json");
    let seventh_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&seventh_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&seventh_out)
        .output()?;
    assert!(
        seventh_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&seventh_output.stdout),
        String::from_utf8_lossy(&seventh_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&seventh_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_review_approved"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_review_approved"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["schema"],
        "telegram_remote_plan_review.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["status"],
        "approved"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["plan_review_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["execution_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["launch_preparation_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["runtime_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["review_output"]["schema"],
        "offdesk_plan_review.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["review_output"]["decision"],
        "approved"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["review_output"]
            ["ready_for_launch_preparation_candidate"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["review_output"]["ready_for_enqueue"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_review"]["review_output"]["applies_file_operations"],
        false
    );
    assert!(
        result["remote_plan_session"]["plan_review"]["review_output"]["registration_path"]
            .is_null()
    );
    assert!(
        result["remote_plan_session"]["plan_review"]["review_output"]["registration_path_hash"]
            .is_string()
    );
    assert!(
        result["remote_plan_session"]["plan_review"]["review_output"]["artifacts"]
            ["review_record_json"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    let artifact_path = result["remote_plan_session"]["plan_review"]["artifact_path"]
        .as_str()
        .expect("plan review receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(artifact["schema"], "telegram_remote_plan_review.v1");
    assert_eq!(artifact["status"], "approved");
    assert_eq!(artifact["plan_review_authorized"], true);
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["launch_preparation_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert_eq!(
        artifact["review_output"]["schema"],
        "offdesk_plan_review.v1"
    );
    assert_eq!(artifact["review_output"]["decision"], "approved");
    assert_eq!(
        artifact["review_output"]["ready_for_launch_preparation_candidate"],
        true
    );
    assert_eq!(artifact["review_output"]["ready_for_enqueue"], false);
    assert_eq!(artifact["review_output"]["applies_file_operations"], false);
    let review_record_path = artifact["review_output"]["artifacts"]["review_record_json"]
        .as_str()
        .expect("review record path");
    assert!(Path::new(review_record_path).exists());
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 승인됨</b>"));
    assert!(preview.contains("계획 검토를 기록했습니다."));
    assert!(preview.contains("실행 준비는 아직 하지 않았습니다."));
    assert!(preview.contains("아래 버튼으로 실행 준비 검토"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"실행 준비 검토".to_string()));
    assert_mobile_contract(&result);

    let eighth_update = temp.path().join("launch_prep_update.json");
    write_text_update(&eighth_update, 737, 916, "실행 준비 검토")?;
    let eighth_out = temp.path().join("launch_prep_result.json");
    let eighth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&eighth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&eighth_out)
        .output()?;
    assert!(
        eighth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&eighth_output.stdout),
        String::from_utf8_lossy(&eighth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&eighth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_launch_prep_prepared"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_launch_prep_prepared"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["schema"],
        "telegram_remote_plan_launch_prep.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["status"],
        "prepared"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["launch_preparation_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["approval_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["gate_approval_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["execution_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["enqueue_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["runtime_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["launch_prep_output"]["schema"],
        "offdesk_plan_launch_prep.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["launch_prep_output"]["ready_for_launch"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["launch_prep_output"]
            ["ready_for_enqueue"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_launch_prep"]["launch_prep_output"]
            ["applies_file_operations"],
        false
    );
    assert!(
        result["remote_plan_session"]["plan_launch_prep"]["launch_prep_output"]
            ["registration_path"]
            .is_null()
    );
    assert!(
        result["remote_plan_session"]["plan_launch_prep"]["launch_prep_output"]
            ["registration_path_hash"]
            .is_string()
    );
    assert!(
        result["remote_plan_session"]["plan_launch_prep"]["launch_prep_output"]["artifacts"]
            ["launch_prep_json"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    let artifact_path = result["remote_plan_session"]["plan_launch_prep"]["artifact_path"]
        .as_str()
        .expect("launch prep receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(artifact["schema"], "telegram_remote_plan_launch_prep.v1");
    assert_eq!(artifact["status"], "prepared");
    assert_eq!(artifact["launch_preparation_authorized"], true);
    assert_eq!(artifact["approval_authorized"], false);
    assert_eq!(artifact["gate_approval_authorized"], false);
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["launch_authorized"], false);
    assert_eq!(artifact["enqueue_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert_eq!(
        artifact["launch_prep_output"]["schema"],
        "offdesk_plan_launch_prep.v1"
    );
    assert_eq!(artifact["launch_prep_output"]["ready_for_launch"], false);
    assert_eq!(artifact["launch_prep_output"]["ready_for_enqueue"], false);
    assert_eq!(
        artifact["launch_prep_output"]["applies_file_operations"],
        false
    );
    assert!(artifact["launch_prep_output"]["does_not_authorize"]
        .as_array()
        .expect("does_not_authorize array")
        .iter()
        .any(|item| item.as_str() == Some("dispatch")));
    let launch_prep_path = artifact["launch_prep_output"]["artifacts"]["launch_prep_json"]
        .as_str()
        .expect("launch prep json path");
    assert!(Path::new(launch_prep_path).exists());
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>실행 준비 패킷 생성됨</b>"));
    assert!(preview.contains("패킷만 저장했습니다."));
    assert!(preview.contains("실행/승인은 아직 하지 않았습니다."));
    assert!(preview.contains("아래 버튼으로 게이트 요청"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"게이트 요청".to_string()));
    assert_mobile_contract(&result);

    let ninth_update = temp.path().join("gate_request_update.json");
    write_text_update(&ninth_update, 738, 916, "게이트 요청")?;
    let ninth_out = temp.path().join("gate_request_result.json");
    let ninth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&ninth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&ninth_out)
        .output()?;
    assert!(
        ninth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&ninth_output.stdout),
        String::from_utf8_lossy(&ninth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&ninth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_gate_request_created"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_gate_request_created"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["schema"],
        "telegram_remote_plan_gate_request.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["status"],
        "pending_approval"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["gate_request_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["approval_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["gate_approval_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["execution_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["launch_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["enqueue_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["runtime_authorized"],
        false
    );
    assert!(result["remote_plan_session"]["plan_gate_request"]["launch_prep_json"].is_null());
    assert!(
        result["remote_plan_session"]["plan_gate_request"]["launch_prep_json_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["gate_output"]["status"],
        "pending_approval"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_request"]["gate_output"]["approval"]["action"],
        "dispatch.runtime"
    );
    assert!(
        result["remote_plan_session"]["plan_gate_request"]["gate_output"]["approval"]
            ["approval_id"]
            .is_string()
    );
    let artifact_path = result["remote_plan_session"]["plan_gate_request"]["artifact_path"]
        .as_str()
        .expect("gate request receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(artifact["schema"], "telegram_remote_plan_gate_request.v1");
    assert_eq!(artifact["status"], "pending_approval");
    assert_eq!(artifact["gate_request_authorized"], true);
    assert_eq!(artifact["approval_authorized"], false);
    assert_eq!(artifact["gate_approval_authorized"], false);
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["launch_authorized"], false);
    assert_eq!(artifact["enqueue_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert_eq!(artifact["gate_output"]["status"], "pending_approval");
    assert_eq!(
        artifact["gate_output"]["approval"]["action"],
        "dispatch.runtime"
    );
    assert!(artifact["launch_prep_json"]
        .as_str()
        .is_some_and(|path| { Path::new(path).exists() }));
    let approvals_path = profile_dir(temp.path()).join("pending_action_approvals.json");
    let approvals: Value = serde_json::from_slice(&fs::read(&approvals_path)?)?;
    assert_eq!(approvals.as_array().expect("approvals").len(), 1);
    assert_eq!(approvals[0]["action"], "dispatch.runtime");
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>게이트 요청 생성됨</b>"));
    assert!(preview.contains("승인 대기열에 올렸습니다."));
    assert!(preview.contains("실행은 아직 시작하지 않았습니다."));
    assert!(preview.contains("로컬에서 approval 확인"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"게이트 승인".to_string()));
    assert!(button_texts(&result).contains(&"게이트 거절".to_string()));
    assert_mobile_contract(&result);

    let tenth_update = temp.path().join("gate_approval_update.json");
    write_text_update(&tenth_update, 739, 916, "게이트 승인")?;
    let tenth_out = temp.path().join("gate_approval_result.json");
    let tenth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&tenth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&tenth_out)
        .output()?;
    assert!(
        tenth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&tenth_output.stdout),
        String::from_utf8_lossy(&tenth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&tenth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_gate_approved"
    );
    assert_eq!(result["remote_plan_session"]["stage"], "plan_gate_approved");
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["schema"],
        "telegram_remote_plan_gate_resolution.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["status"],
        "approved"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["approval_resolution_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["approval_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["gate_approval_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["execution_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["launch_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["enqueue_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["runtime_authorized"],
        false
    );
    assert!(result["remote_plan_session"]["plan_gate_resolution"]["launch_prep_json"].is_null());
    assert!(
        result["remote_plan_session"]["plan_gate_resolution"]["launch_prep_json_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["pending_approval"]["status"],
        "pending"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["resolution_output"]["status"],
        "approved"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_gate_resolution"]["resolution_output"]["action"],
        "dispatch.runtime"
    );
    let artifact_path = result["remote_plan_session"]["plan_gate_resolution"]["artifact_path"]
        .as_str()
        .expect("gate resolution receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(
        artifact["schema"],
        "telegram_remote_plan_gate_resolution.v1"
    );
    assert_eq!(artifact["status"], "approved");
    assert_eq!(artifact["approval_resolution_authorized"], true);
    assert_eq!(artifact["approval_authorized"], true);
    assert_eq!(artifact["gate_approval_authorized"], true);
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["launch_authorized"], false);
    assert_eq!(artifact["enqueue_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert_eq!(artifact["pending_approval"]["status"], "pending");
    assert_eq!(artifact["resolution_output"]["status"], "approved");
    assert_eq!(artifact["resolution_output"]["action"], "dispatch.runtime");
    let approvals: Value = serde_json::from_slice(&fs::read(&approvals_path)?)?;
    assert_eq!(approvals.as_array().expect("approvals").len(), 1);
    assert_eq!(approvals[0]["action"], "dispatch.runtime");
    assert_eq!(approvals[0]["status"], "approved");
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>게이트 승인됨</b>"));
    assert!(preview.contains("approval만 해결했습니다."));
    assert!(preview.contains("실행은 아직 시작하지 않았습니다."));
    assert!(preview.contains("로컬에서 다음 단계 검토"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"실행 브리프 생성".to_string()));
    assert_mobile_contract(&result);

    let eleventh_update = temp.path().join("execution_brief_update.json");
    write_text_update(&eleventh_update, 740, 916, "실행 브리프 생성")?;
    let eleventh_out = temp.path().join("execution_brief_result.json");
    let eleventh_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&eleventh_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&eleventh_out)
        .output()?;
    assert!(
        eleventh_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&eleventh_output.stdout),
        String::from_utf8_lossy(&eleventh_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&eleventh_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_execution_brief_created"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_execution_brief_created"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["schema"],
        "telegram_remote_plan_execution_brief.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["status"],
        "created"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["execution_brief_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["approval_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["gate_approval_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["execution_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["launch_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["enqueue_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["runtime_authorized"],
        false
    );
    assert!(
        result["remote_plan_session"]["plan_execution_brief"]["execution_brief_json"].is_null()
    );
    assert!(
        result["remote_plan_session"]["plan_execution_brief"]["execution_brief_json_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["execution_brief"]["approved"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_execution_brief"]["execution_brief"]
            ["allowed_runtime_mutations"][0],
        "dispatch.runtime"
    );
    let artifact_path = result["remote_plan_session"]["plan_execution_brief"]["artifact_path"]
        .as_str()
        .expect("execution brief receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(
        artifact["schema"],
        "telegram_remote_plan_execution_brief.v1"
    );
    assert_eq!(artifact["status"], "created");
    assert_eq!(artifact["execution_brief_authorized"], true);
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["launch_authorized"], false);
    assert_eq!(artifact["enqueue_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert_eq!(artifact["execution_brief"]["approved"], true);
    assert_eq!(
        artifact["execution_brief"]["allowed_runtime_mutations"][0],
        "dispatch.runtime"
    );
    let execution_brief_path = artifact["execution_brief_json"]
        .as_str()
        .expect("execution brief json path");
    assert!(Path::new(execution_brief_path).exists());
    let execution_brief: Value = serde_json::from_slice(&fs::read(execution_brief_path)?)?;
    assert_eq!(execution_brief["approved"], true);
    assert_eq!(execution_brief["project_key"], artifact["project_key"]);
    assert_eq!(execution_brief["request_id"], artifact["request_id"]);
    assert_eq!(execution_brief["task_id"], artifact["task_id"]);
    assert_eq!(
        execution_brief["allowed_runtime_mutations"][0],
        "dispatch.runtime"
    );
    assert!(
        !profile_dir(temp.path()).join("offdesk_tasks.json").exists(),
        "execution brief creation must not enqueue a task"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>실행 브리프 생성됨</b>"));
    assert!(preview.contains("브리프 파일만 저장했습니다."));
    assert!(preview.contains("실행은 아직 시작하지 않았습니다."));
    assert!(preview.contains("로컬에서 enqueue 검토"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"큐 등록 검토".to_string()));
    assert_mobile_contract(&result);

    let twelfth_update = temp.path().join("enqueue_handoff_update.json");
    write_text_update(&twelfth_update, 741, 916, "큐 등록 검토")?;
    let twelfth_out = temp.path().join("enqueue_handoff_result.json");
    let twelfth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&twelfth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&twelfth_out)
        .output()?;
    assert!(
        twelfth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&twelfth_output.stdout),
        String::from_utf8_lossy(&twelfth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&twelfth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_enqueue_handoff_created"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_enqueue_handoff_created"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["schema"],
        "telegram_remote_plan_enqueue_handoff.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["status"],
        "created"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["prepared_workload_required"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["reviewed_workload_command_required"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["approval_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["gate_approval_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["execution_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["launch_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["enqueue_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["runtime_authorized"],
        false
    );
    assert!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["execution_brief_json"].is_null()
    );
    assert!(
        result["remote_plan_session"]["plan_enqueue_handoff"]["execution_brief_json_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    let public_command = result["remote_plan_session"]["plan_enqueue_handoff"]["command_template"]
        .as_array()
        .expect("public command template")
        .iter()
        .map(|item| item.as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert!(public_command.contains(&"enqueue"));
    assert!(public_command.contains(&"dispatch.runtime"));
    assert!(public_command.contains(&"<execution_brief_json>"));
    assert!(public_command.contains(&"<reviewed-workload-command-required>"));
    let artifact_path = result["remote_plan_session"]["plan_enqueue_handoff"]["artifact_path"]
        .as_str()
        .expect("enqueue handoff receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(
        artifact["schema"],
        "telegram_remote_plan_enqueue_handoff.v1"
    );
    assert_eq!(artifact["status"], "created");
    assert_eq!(artifact["prepared_workload_required"], true);
    assert_eq!(artifact["reviewed_workload_command_required"], true);
    assert_eq!(artifact["enqueue_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert_eq!(
        artifact["execution_brief_json"],
        Value::String(execution_brief_path.to_string())
    );
    let command = artifact["command_template"]
        .as_array()
        .expect("command template")
        .iter()
        .map(|item| item.as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert!(command.contains(&"enqueue"));
    assert!(command.contains(&"dispatch.runtime"));
    assert!(command.contains(&execution_brief_path));
    assert!(command.contains(&"<reviewed-workload-command-required>"));
    assert!(
        !profile_dir(temp.path()).join("offdesk_tasks.json").exists(),
        "enqueue handoff must not enqueue a task"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>큐 등록 검토 준비됨</b>"));
    assert!(preview.contains("명령 템플릿만 저장했습니다."));
    assert!(preview.contains("실행은 아직 시작하지 않았습니다."));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"워크로드 패킷 연결".to_string()));
    assert_mobile_contract(&result);

    let project_key = artifact["project_key"]
        .as_str()
        .expect("project key")
        .to_string();
    let request_id = artifact["request_id"]
        .as_str()
        .expect("request id")
        .to_string();
    let task_id = artifact["task_id"].as_str().expect("task id").to_string();
    let prepared_dir = temp.path().join("prepared_workload");
    fs::create_dir_all(&prepared_dir)?;
    let wrapper_path = prepared_dir.join("run_workload.sh");
    let workload_result_path = prepared_dir.join("result.json");
    fs::write(
        &wrapper_path,
        format!(
            "#!/usr/bin/env bash\nprintf 'ok\\n'\nprintf '{{\"status\":\"ok\"}}\\n' > {}\n",
            workload_result_path.display()
        ),
    )?;
    let prepared_task_path = prepared_dir.join("prepared_task.json");
    let prepared_manifest = json!({
        "created_at": "2026-06-17T00:00:00Z",
        "kind": "forager_offdesk_prepared_workload",
        "title": "Telegram prepared workload fixture",
        "profile": "default",
        "project_key": project_key,
        "request_id": request_id,
        "task_id": task_id,
        "repo": workspace_root,
        "out_dir": prepared_dir,
        "duration_minutes": 0.1,
        "max_iterations": 1,
        "provider": "ollama",
        "model": "qwen3-coder-next:latest",
        "workload_command": ["bash", "-lc", "printf 'ok\\n'"],
        "workload_command_text": "printf 'ok\\n'",
        "workload_wrapper": wrapper_path,
        "enqueue_args": [
            env!("CARGO_BIN_EXE_forager"),
            "--profile",
            "default",
            "offdesk",
            "enqueue",
            "dispatch.runtime",
            "--runner",
            "local-background",
            "--project-key",
            artifact["project_key"].as_str().expect("project key"),
            "--request-id",
            artifact["request_id"].as_str().expect("request id"),
            "--task-id",
            artifact["task_id"].as_str().expect("task id"),
            "--cmd",
            format!("bash {}", wrapper_path.display()),
            "--workdir",
            workspace_root,
            "--artifact-kind",
            "report",
            "--agent-mode",
            "critique",
            "--provider-id",
            "ollama",
            "--model",
            "qwen3-coder-next:latest",
            "--log-artifact",
            prepared_dir.join("offdesk-runner.log"),
            "--result-artifact",
            workload_result_path,
            "--json"
        ],
        "safety": {
            "repo_read_only": true,
            "writes_only_under_out_dir": true,
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
            "runner": "local-background",
            "approval_required_before_dispatch": true,
            "separate_review_artifact_required": true
        },
        "preflight": {
            "ready_for_enqueue": true,
            "blocking_reasons": [],
            "warnings": [],
            "role_gate": {
                "ready": false,
                "reason": "not_required_for_fixture"
            },
            "review_artifact": {
                "ready": true,
                "path": prepared_dir.join("workload_review").join("results.json"),
                "decision": "needs_approval"
            }
        },
        "artifacts": {
            "prepared_task": prepared_task_path,
            "preflight": prepared_dir.join("preflight.json"),
            "runner_log": prepared_dir.join("offdesk-runner.log"),
            "result": workload_result_path,
            "report": prepared_dir.join("REPORT.md"),
            "workload_wrapper": wrapper_path
        }
    });
    fs::write(
        &prepared_task_path,
        serde_json::to_string_pretty(&prepared_manifest)?,
    )?;

    let thirteenth_update = temp.path().join("workload_binding_update.json");
    write_text_update(
        &thirteenth_update,
        742,
        916,
        prepared_task_path.to_str().expect("prepared task path"),
    )?;
    let thirteenth_out = temp.path().join("workload_binding_result.json");
    let thirteenth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&thirteenth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&thirteenth_out)
        .output()?;
    assert!(
        thirteenth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&thirteenth_output.stdout),
        String::from_utf8_lossy(&thirteenth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&thirteenth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_workload_bound"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_workload_bound"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_workload_binding"]["schema"],
        "telegram_remote_plan_workload_binding.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_workload_binding"]["status"],
        "bound"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_workload_binding"]["ready_for_local_enqueue_review"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_workload_binding"]["execution_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_workload_binding"]["launch_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_workload_binding"]["enqueue_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_workload_binding"]["runtime_authorized"],
        false
    );
    assert!(result["remote_plan_session"]["plan_workload_binding"]["prepared_task_json"].is_null());
    assert!(
        result["remote_plan_session"]["plan_workload_binding"]["prepared_task_json_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    let public_bound_args = result["remote_plan_session"]["plan_workload_binding"]
        ["bound_enqueue_args"]
        .as_array()
        .expect("public bound enqueue args")
        .iter()
        .map(|item| item.as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert!(public_bound_args.contains(&"enqueue"));
    assert!(public_bound_args.contains(&"dispatch.runtime"));
    assert!(public_bound_args.contains(&"<execution_brief_json>"));
    assert!(public_bound_args
        .iter()
        .any(|item| item.contains("<workload_wrapper>")));
    let artifact_path = result["remote_plan_session"]["plan_workload_binding"]["artifact_path"]
        .as_str()
        .expect("workload binding receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(
        artifact["schema"],
        "telegram_remote_plan_workload_binding.v1"
    );
    assert_eq!(artifact["status"], "bound");
    assert_eq!(artifact["ready_for_local_enqueue_review"], true);
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["launch_authorized"], false);
    assert_eq!(artifact["enqueue_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    // The adapter canonicalizes the path; on macOS /var is a symlink to
    // /private/var, so the expectation must be canonicalized too.
    assert_eq!(
        artifact["prepared_task_json"],
        Value::String(
            prepared_task_path
                .canonicalize()?
                .to_string_lossy()
                .into_owned()
        )
    );
    let bound_args = artifact["bound_enqueue_args"]
        .as_array()
        .expect("bound enqueue args")
        .iter()
        .map(|item| item.as_str().unwrap_or_default())
        .collect::<Vec<_>>();
    assert!(bound_args.contains(&"--brief"));
    assert!(bound_args.contains(&execution_brief_path));
    assert!(bound_args.contains(&"--mutation-class"));
    assert!(bound_args.contains(&"dispatch.runtime"));
    assert!(
        !profile_dir(temp.path()).join("offdesk_tasks.json").exists(),
        "workload binding must not enqueue a task"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>워크로드 패킷 연결됨</b>"));
    assert!(preview.contains("검토된 패킷만 연결했습니다."));
    assert!(preview.contains("실행은 아직 시작하지 않았습니다."));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"큐 등록 실행".to_string()));
    assert_mobile_contract(&result);

    let fourteenth_update = temp.path().join("enqueue_run_update.json");
    write_text_update(&fourteenth_update, 743, 916, "큐 등록 실행")?;
    let fourteenth_out = temp.path().join("enqueue_run_result.json");
    let fourteenth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&fourteenth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&fourteenth_out)
        .output()?;
    assert!(
        fourteenth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&fourteenth_output.stdout),
        String::from_utf8_lossy(&fourteenth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&fourteenth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_enqueued"
    );
    assert_eq!(result["remote_plan_session"]["stage"], "plan_enqueued");
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_run"]["schema"],
        "telegram_remote_plan_enqueue_run.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_run"]["status"],
        "queued"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_run"]["queue_mutation_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_run"]["enqueue_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_run"]["execution_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_run"]["launch_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_run"]["runtime_authorized"],
        false
    );
    assert!(result["remote_plan_session"]["plan_enqueue_run"]["enqueue_command"].is_null());
    assert!(
        result["remote_plan_session"]["plan_enqueue_run"]["enqueue_command_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    assert_eq!(
        result["remote_plan_session"]["plan_enqueue_run"]["enqueue_output"]["status"],
        "queued"
    );
    let artifact_path = result["remote_plan_session"]["plan_enqueue_run"]["artifact_path"]
        .as_str()
        .expect("enqueue run receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(artifact["schema"], "telegram_remote_plan_enqueue_run.v1");
    assert_eq!(artifact["status"], "queued");
    assert_eq!(artifact["queue_mutation_authorized"], true);
    assert_eq!(artifact["enqueue_authorized"], true);
    assert_eq!(artifact["execution_authorized"], false);
    assert_eq!(artifact["launch_authorized"], false);
    assert_eq!(artifact["runtime_authorized"], false);
    assert_eq!(artifact["enqueue_output"]["status"], "queued");
    let tasks_path = profile_dir(temp.path()).join("offdesk_tasks.json");
    assert!(tasks_path.exists(), "enqueue run must create a queued task");
    let tasks: Value = serde_json::from_slice(&fs::read(&tasks_path)?)?;
    assert_eq!(tasks[0]["task_id"], task_id);
    assert_eq!(tasks[0]["status"], "queued");
    assert!(tasks[0]["background_ticket_id"].is_null());
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>큐 등록됨</b>"));
    assert!(preview.contains("Offdesk 큐에만 등록했습니다."));
    assert!(preview.contains("실행은 아직 시작하지 않았습니다."));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"실행 시작".to_string()));
    assert_mobile_contract(&result);

    let fifteenth_update = temp.path().join("runtime_start_update.json");
    write_text_update(&fifteenth_update, 744, 916, "실행 시작")?;
    let fifteenth_out = temp.path().join("runtime_start_result.json");
    let fifteenth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&fifteenth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&fifteenth_out)
        .output()?;
    assert!(
        fifteenth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&fifteenth_output.stdout),
        String::from_utf8_lossy(&fifteenth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&fifteenth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_runtime_started"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_runtime_started"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_start"]["schema"],
        "telegram_remote_plan_runtime_start.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_start"]["status"],
        "launched"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_start"]["runtime_start_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_start"]["tick_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_start"]["execution_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_start"]["closeout_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_start"]["accepted_truth_authorized"],
        false
    );
    assert!(result["remote_plan_session"]["plan_runtime_start"]["tick_command"].is_null());
    assert!(
        result["remote_plan_session"]["plan_runtime_start"]["tick_command_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_start"]["tick_output"]["launched"],
        1
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_start"]["tick_output"]["updated_task_ids"][0],
        task_id
    );
    let artifact_path = result["remote_plan_session"]["plan_runtime_start"]["artifact_path"]
        .as_str()
        .expect("runtime start receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(artifact["schema"], "telegram_remote_plan_runtime_start.v1");
    assert_eq!(artifact["status"], "launched");
    assert_eq!(artifact["tick_output"]["launched"], 1);
    assert_eq!(artifact["tick_output"]["updated_task_ids"][0], task_id);
    assert_eq!(artifact["closeout_authorized"], false);
    assert_eq!(artifact["accepted_truth_authorized"], false);
    let tasks: Value = serde_json::from_slice(&fs::read(&tasks_path)?)?;
    assert_eq!(tasks[0]["task_id"], task_id);
    assert_eq!(tasks[0]["status"], "launched");
    assert!(tasks[0]["background_ticket_id"].is_string());
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>실행 시작됨</b>"));
    assert!(preview.contains("대상 task만 시작했습니다."));
    assert!(preview.contains("완료 판정은 아직 없습니다."));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"실행 상태 확인".to_string()));
    assert_mobile_contract(&result);

    thread::sleep(Duration::from_millis(250));
    let sixteenth_update = temp.path().join("runtime_monitor_update.json");
    write_text_update(&sixteenth_update, 745, 916, "실행 상태 확인")?;
    let sixteenth_out = temp.path().join("runtime_monitor_result.json");
    let sixteenth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&sixteenth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&sixteenth_out)
        .output()?;
    assert!(
        sixteenth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&sixteenth_output.stdout),
        String::from_utf8_lossy(&sixteenth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&sixteenth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_runtime_monitored"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_runtime_monitored"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_monitor"]["schema"],
        "telegram_remote_plan_runtime_monitor.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_monitor"]["status"],
        "completed"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_monitor"]["task_status"],
        "completed"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_monitor"]["poll_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_monitor"]["dispatch_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_monitor"]["closeout_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_monitor"]["accepted_truth_authorized"],
        false
    );
    assert!(result["remote_plan_session"]["plan_runtime_monitor"]["tick_command"].is_null());
    assert!(result["remote_plan_session"]["plan_runtime_monitor"]["tasks_command"].is_null());
    assert!(
        result["remote_plan_session"]["plan_runtime_monitor"]["tick_command_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_monitor"]["tick_output"]["completed"],
        1
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_monitor"]["tick_output"]["updated_task_ids"][0],
        task_id
    );
    assert_eq!(
        result["remote_plan_session"]["plan_runtime_monitor"]["target_task"]["status"],
        "completed"
    );
    let artifact_path = result["remote_plan_session"]["plan_runtime_monitor"]["artifact_path"]
        .as_str()
        .expect("runtime monitor receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(
        artifact["schema"],
        "telegram_remote_plan_runtime_monitor.v1"
    );
    assert_eq!(artifact["status"], "completed");
    assert_eq!(artifact["task_status"], "completed");
    assert_eq!(artifact["tick_output"]["completed"], 1);
    assert_eq!(artifact["tick_output"]["updated_task_ids"][0], task_id);
    assert_eq!(artifact["dispatch_authorized"], false);
    assert_eq!(artifact["closeout_authorized"], false);
    assert_eq!(artifact["accepted_truth_authorized"], false);
    let tasks: Value = serde_json::from_slice(&fs::read(&tasks_path)?)?;
    assert_eq!(tasks[0]["task_id"], task_id);
    assert_eq!(tasks[0]["status"], "completed");
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>실행 완료 확인</b>"));
    assert!(preview.contains("로컬에서 closeout 검토"));
    assert!(preview.contains("결과 승인은 아직 없습니다."));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"실행 상태 확인".to_string()));
    assert!(button_texts(&result).contains(&"마무리 패킷 생성".to_string()));
    assert_mobile_contract(&result);

    let seventeenth_update = temp.path().join("closeout_packet_update.json");
    write_text_update(&seventeenth_update, 746, 916, "마무리 패킷 생성")?;
    let seventeenth_out = temp.path().join("closeout_packet_result.json");
    let seventeenth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&seventeenth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&seventeenth_out)
        .output()?;
    assert!(
        seventeenth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&seventeenth_output.stdout),
        String::from_utf8_lossy(&seventeenth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&seventeenth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_closeout_packet_created"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_closeout_packet_created"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_packet"]["schema"],
        "telegram_remote_plan_closeout_packet.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_packet"]["status"],
        "created"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_packet"]["closeout_packet_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_packet"]["closeout_review_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_packet"]["accepted_truth_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_packet"]["file_mutation_authorized"],
        false
    );
    assert!(result["remote_plan_session"]["plan_closeout_packet"]["closeout_command"].is_null());
    assert!(
        result["remote_plan_session"]["plan_closeout_packet"]["closeout_command_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_packet"]["closeout_output"]["dry_run"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_packet"]["closeout_output"]
            ["read_only_project_state"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_packet"]["closeout_output"]["review_contract"]
            ["required"],
        true
    );
    let artifact_path = result["remote_plan_session"]["plan_closeout_packet"]["artifact_path"]
        .as_str()
        .expect("closeout packet receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(
        artifact["schema"],
        "telegram_remote_plan_closeout_packet.v1"
    );
    assert_eq!(artifact["status"], "created");
    assert_eq!(artifact["closeout_packet_authorized"], true);
    assert_eq!(artifact["closeout_review_authorized"], false);
    assert_eq!(artifact["accepted_truth_authorized"], false);
    assert_eq!(artifact["file_mutation_authorized"], false);
    assert_eq!(artifact["closeout_output"]["dry_run"], true);
    assert_eq!(artifact["closeout_output"]["read_only_project_state"], true);
    assert_eq!(
        artifact["closeout_output"]["operator_requested_dry_run"],
        true
    );
    assert_eq!(artifact["closeout_output"]["tasks"][0]["task_id"], task_id);
    assert_eq!(
        artifact["closeout_output"]["tasks"][0]["status"],
        "completed"
    );
    assert!(PathBuf::from(
        artifact["closeout_output"]["artifacts"]["closeout_plan_json"]
            .as_str()
            .expect("closeout plan path")
    )
    .exists());
    assert!(PathBuf::from(
        artifact["closeout_output"]["artifacts"]["return_package_markdown"]
            .as_str()
            .expect("return package path")
    )
    .exists());
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>마무리 패킷 생성됨</b>"));
    assert!(preview.contains("closeout 자료만 만들었습니다."));
    assert!(preview.contains("결과 승인은 아직 없습니다."));
    assert!(preview.contains("로컬에서 closeout-review 검토"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"마무리 검토 준비".to_string()));
    assert_mobile_contract(&result);

    let eighteenth_update = temp.path().join("closeout_review_handoff_update.json");
    write_text_update(&eighteenth_update, 747, 917, "마무리 검토 준비")?;
    let eighteenth_out = temp.path().join("closeout_review_handoff_result.json");
    let eighteenth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&eighteenth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&eighteenth_out)
        .output()?;
    assert!(
        eighteenth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&eighteenth_output.stdout),
        String::from_utf8_lossy(&eighteenth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&eighteenth_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_closeout_review_handoff_created"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_closeout_review_handoff_created"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_review_handoff"]["schema"],
        "telegram_remote_plan_closeout_review_handoff.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_review_handoff"]["status"],
        "created"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_review_handoff"]
            ["remote_closeout_review_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_review_handoff"]["accepted_truth_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_review_handoff"]["file_mutation_authorized"],
        false
    );
    assert!(
        result["remote_plan_session"]["plan_closeout_review_handoff"]["local_review_commands"]
            .is_null()
    );
    assert!(
        result["remote_plan_session"]["plan_closeout_review_handoff"]
            ["local_review_command_hashes"]["approved"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    let artifact_path = result["remote_plan_session"]["plan_closeout_review_handoff"]
        ["artifact_path"]
        .as_str()
        .expect("closeout review handoff receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(
        artifact["schema"],
        "telegram_remote_plan_closeout_review_handoff.v1"
    );
    assert_eq!(artifact["status"], "created");
    assert_eq!(artifact["remote_closeout_review_authorized"], false);
    assert_eq!(artifact["closeout_review_authorized"], false);
    assert_eq!(artifact["accepted_truth_authorized"], false);
    assert_eq!(artifact["file_mutation_authorized"], false);
    assert_eq!(artifact["local_review_required"], true);
    assert!(artifact["approved_verdict_may_accept_truth"].is_boolean());
    for (verdict, expected) in [
        ("approved", "approved"),
        ("revise", "revise"),
        ("blocked", "blocked"),
    ] {
        let command = artifact["local_review_commands"][verdict]
            .as_array()
            .expect("local review command");
        assert!(command.iter().any(|item| item == "closeout-review"));
        assert!(command.iter().any(|item| item == expected));
    }
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>마무리 검토 준비됨</b>"));
    assert!(preview.contains("Telegram에서 verdict를 기록할 수 있습니다."));
    assert!(preview.contains("아래 버튼에서 verdict 선택"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert!(button_texts(&result).contains(&"승인 기록".to_string()));
    assert!(button_texts(&result).contains(&"수정 요청 기록".to_string()));
    assert!(button_texts(&result).contains(&"차단 기록".to_string()));
    assert_mobile_contract(&result);

    let nineteenth_update = temp.path().join("closeout_approved_update.json");
    write_text_update(&nineteenth_update, 748, 918, "승인 기록")?;
    let nineteenth_out = temp.path().join("closeout_approved_result.json");
    let nineteenth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&nineteenth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--remote-plan-artifact-dir")
        .arg(&plan_artifact_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&nineteenth_out)
        .output()?;
    assert!(
        nineteenth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&nineteenth_output.stdout),
        String::from_utf8_lossy(&nineteenth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&nineteenth_out)?)?;
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "plan_closeout_verdict_recorded"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "plan_closeout_verdict_recorded"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_verdict"]["schema"],
        "telegram_remote_plan_closeout_verdict.v1"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_verdict"]["status"],
        "recorded"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_verdict"]["verdict"],
        "approved"
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_verdict"]["accepted_truth_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_verdict"]["remote_closeout_review_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_verdict"]["closeout_review_authorized"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_verdict"]["project_file_mutation_authorized"],
        false
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_verdict"]["file_mutation_authorized"],
        false
    );
    assert!(
        result["remote_plan_session"]["plan_closeout_verdict"]["closeout_review_command"].is_null()
    );
    assert!(
        result["remote_plan_session"]["plan_closeout_verdict"]["closeout_review_command_hash"]
            .as_str()
            .unwrap_or_default()
            .starts_with("sha256:")
    );
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_verdict"]["closeout_review_output"]["verdict"],
        "approved"
    );
    let acceptance_status = result["remote_plan_session"]["plan_closeout_verdict"]
        ["closeout_review_output"]["closeout_receipt"]["acceptance_status"]
        .as_str()
        .expect("acceptance status");
    assert!(matches!(
        acceptance_status,
        "accepted" | "approved_with_followups"
    ));
    assert_eq!(
        result["remote_plan_session"]["plan_closeout_verdict"]["accepted_truth_recorded"],
        acceptance_status == "accepted"
    );
    let artifact_path = result["remote_plan_session"]["plan_closeout_verdict"]["artifact_path"]
        .as_str()
        .expect("closeout verdict receipt path");
    let artifact: Value = serde_json::from_slice(&fs::read(artifact_path)?)?;
    assert_eq!(
        artifact["schema"],
        "telegram_remote_plan_closeout_verdict.v1"
    );
    assert_eq!(artifact["status"], "recorded");
    assert_eq!(artifact["verdict"], "approved");
    assert_eq!(artifact["accepted_truth_authorized"], true);
    assert_eq!(
        artifact["accepted_truth_recorded"],
        artifact["acceptance_status"] == "accepted"
    );
    assert_eq!(artifact["project_file_mutation_authorized"], false);
    assert_eq!(artifact["file_mutation_authorized"], false);
    let artifact_acceptance = artifact["closeout_review_output"]["closeout_receipt"]
        ["acceptance_status"]
        .as_str()
        .expect("artifact acceptance status");
    assert!(matches!(
        artifact_acceptance,
        "accepted" | "approved_with_followups"
    ));
    assert_eq!(
        artifact["closeout_review_output"]["closeout_receipt"]["acceptance_status"],
        artifact["acceptance_status"]
    );
    assert_eq!(
        artifact["closeout_review_output"]["applies_file_operations"],
        false
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>마무리 verdict 기록됨</b>"));
    assert!(preview.contains("approved"));
    assert!(preview.contains(acceptance_status));
    if acceptance_status == "accepted" {
        assert!(preview.contains("accepted truth가 기록됐습니다."));
    } else {
        assert!(preview.contains("follow-up이 남아 아직 accepted는 아닙니다."));
    }
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert_mobile_contract(&result);

    let feedback_rows = fs::read_to_string(&feedback_file)?;
    assert_eq!(feedback_rows.lines().count(), 1);
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state["offset"], 749);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_plan_session_accepts_manual_project_input() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let workspace_root = temp.path().join("workspace");
    fs::create_dir_all(workspace_root.join("Alpha"))?;
    fs::write(
        workspace_root.join("Alpha").join("README.md"),
        "Alpha project\n",
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");
    let first_update = temp.path().join("plan_update.json");
    write_text_update(
        &first_update,
        720,
        900,
        "/plan Alpha 프로젝트를 오늘 밤 자율주행 계획으로 잡아줘",
    )?;
    let first_out = temp.path().join("plan_replay_result.json");
    let first_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&first_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&first_out)
        .output()?;
    assert!(
        first_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&first_output.stdout),
        String::from_utf8_lossy(&first_output.stderr)
    );

    let second_update = temp.path().join("manual_selection_update.json");
    write_text_update(&second_update, 721, 901, "/select Gamma 프로젝트")?;
    let second_out = temp.path().join("manual_selection_result.json");
    let second_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&second_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&second_out)
        .output()?;
    assert!(
        second_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&second_output.stdout),
        String::from_utf8_lossy(&second_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&second_out)?)?;
    assert_eq!(result["parsed_command"]["command"], "remote_plan_selection");
    assert_eq!(result["parsed_command"]["selection_status"], "manual_input");
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "project_manual_input"
    );
    assert_eq!(
        result["remote_plan_session"]["selected_candidate"]["manual_input"],
        true
    );
    assert_eq!(
        result["remote_plan_session"]["selected_candidate"]["display_name"],
        "Gamma 프로젝트"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 대상 확인 필요</b>"));
    assert!(preview.contains("Gamma 프로젝트"));
    assert!(preview.contains("경로 미확인"));
    assert!(preview.contains("실제 폴더명/경로 입력"));
    assert!(preview.contains("아직 실행은 시작하지 않았습니다."));
    assert_mobile_contract(&result);

    let third_update = temp.path().join("manual_init_update.json");
    write_text_update(&third_update, 722, 902, "초기화 검토")?;
    let third_out = temp.path().join("manual_init_result.json");
    let third_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&third_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&third_out)
        .output()?;
    assert!(
        third_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&third_output.stdout),
        String::from_utf8_lossy(&third_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&third_out)?)?;
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "path_required"
    );
    assert_eq!(
        result["remote_plan_session"]["stage"],
        "project_path_required"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>경로 확인 필요</b>"));
    assert!(preview.contains("프로젝트 경로를 직접 입력하세요."));
    assert!(preview.contains("아직 실행은 시작하지 않았습니다."));
    assert_mobile_contract(&result);

    let fourth_update = temp.path().join("manual_path_update.json");
    write_text_update(
        &fourth_update,
        723,
        903,
        workspace_root
            .join("Alpha")
            .to_str()
            .expect("workspace path"),
    )?;
    let fourth_out = temp.path().join("manual_path_result.json");
    let fourth_output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&fourth_update)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(&feedback_file)
        .arg("--feedback-ingest-dir")
        .arg(&ingest_dir)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&fourth_out)
        .output()?;
    assert!(
        fourth_output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&fourth_output.stdout),
        String::from_utf8_lossy(&fourth_output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&fourth_out)?)?;
    assert_eq!(
        result["parsed_command"]["selection_status"],
        "path_confirmed"
    );
    assert_eq!(result["remote_plan_session"]["stage"], "project_selected");
    assert_eq!(
        result["remote_plan_session"]["selected_candidate"]["display_name"],
        "Alpha"
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 대상 선택됨</b>"));
    assert!(!preview.contains(workspace_root.to_str().expect("workspace path")));
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_replay_poll_loop_handles_updates_without_once() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let update_path = temp.path().join("status_update.json");
    fs::write(
        &update_path,
        serde_json::to_string_pretty(&json!({
            "update_id": 600,
            "message": {
                "message_id": 778,
                "date": 1780000001,
                "chat": {"id": 123456789, "type": "private"},
                "from": {"id": 987654321, "is_bot": false, "first_name": "Operator"},
                "text": "/status"
            }
        }))?,
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let out = temp.path().join("poll_loop_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--replay-update-file")
        .arg(&update_path)
        .arg("--max-polls")
        .arg("2")
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["mode"], "live_loop");
    assert_eq!(result["status"], "max_polls_reached");
    assert_eq!(result["poll_count"], 2);
    assert_eq!(result["updates_seen"], 1);
    assert_eq!(result["handled_result_count"], 1);
    assert_eq!(result["last_handled_result"]["status"], "rendered");
    assert_eq!(
        result["last_handled_result"]["parsed_command"]["command"],
        "status"
    );
    assert_mobile_contract(&result["last_handled_result"]);
    assert_eq!(result["last_result"]["status"], "no_update");

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(state["offset"], 601);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_health_reports_fresh_listener_status() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let status_path = temp.path().join("loop_status.json");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&json!({
            "schema": "remote_operator_telegram_adapter_result.v1",
            "mode": "live_loop",
            "status": "polling",
            "poll_count": 7,
            "updates_seen": 2,
            "handled_result_count": 1,
            "last_result": {
                "generated_at": "2099-01-01T00:00:00+00:00",
                "status": "no_update"
            },
            "last_handled_result": {
                "status": "rendered"
            }
        }))?,
    )?;
    let out = temp.path().join("health.json");

    let output = remote_operator_command(temp.path())
        .arg("--health")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--loop-status-file")
        .arg(&status_path)
        .arg("--health-max-age-sec")
        .arg("999999999")
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["schema"], "remote_operator_telegram_health.v1");
    assert_eq!(result["health_status"], "healthy");
    assert_eq!(result["listener_status"], "polling");
    assert_eq!(result["poll_count"], 7);
    assert_eq!(result["handled_result_count"], 1);
    assert_eq!(result["agent_runtime_status"]["status"], "disabled");
    assert_eq!(result["transport_issues"], json!([]));
    assert_eq!(result["action_readiness"][0]["action"], "status");
    assert_eq!(result["action_readiness"][0]["status"], "healthy");
    assert_eq!(result["action_readiness"][2]["action"], "build_plan");
    assert_eq!(result["action_readiness"][2]["status"], "healthy");
    let serialized = serde_json::to_string(&result)?;
    assert!(!serialized.contains("fake-token-for-test"));
    assert!(!serialized.contains("999999:"));
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_health_degrades_when_agent_runtime_unavailable() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let status_path = temp.path().join("loop_status.json");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&json!({
            "schema": "remote_operator_telegram_adapter_result.v1",
            "mode": "live_loop",
            "status": "polling",
            "poll_count": 7,
            "updates_seen": 2,
            "handled_result_count": 1,
            "last_result": {
                "generated_at": "2099-01-01T00:00:00+00:00",
                "status": "no_update"
            },
            "last_handled_result": {
                "status": "rendered"
            }
        }))?,
    )?;
    let out = temp.path().join("health_degraded.json");

    let output = remote_operator_command(temp.path())
        .arg("--health")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--loop-status-file")
        .arg(&status_path)
        .arg("--health-max-age-sec")
        .arg("999999999")
        .arg("--agent-intent-mode")
        .arg("auto")
        .arg("--agent-base-url")
        .arg("http://127.0.0.1:9")
        .arg("--agent-model")
        .arg("qwen3-coder-next:latest")
        .arg("--agent-timeout-sec")
        .arg("1")
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        !output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["health_status"], "degraded");
    assert_eq!(result["transport_issues"], json!([]));
    assert_eq!(result["agent_runtime_status"]["status"], "unavailable");
    assert!(result["issues"]
        .as_array()
        .expect("issues")
        .contains(&json!("agent_runtime_unavailable")));
    assert_eq!(result["action_readiness"][0]["action"], "status");
    assert_eq!(result["action_readiness"][0]["status"], "healthy");
    assert_eq!(result["action_readiness"][2]["action"], "build_plan");
    assert_eq!(result["action_readiness"][2]["status"], "blocked");
    assert_eq!(
        result["action_readiness"][2]["blocked_actions"],
        json!(["new_plan", "start_offdesk"])
    );
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_health_reports_recent_poll_transport_error() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let status_path = temp.path().join("loop_status.json");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&json!({
            "schema": "remote_operator_telegram_adapter_result.v1",
            "mode": "live_loop",
            "status": "polling",
            "poll_count": 8,
            "updates_seen": 2,
            "handled_result_count": 1,
            "last_result": {
                "generated_at": "2099-01-01T00:00:00+00:00",
                "status": "poll_error",
                "reason": "telegram_transport_error",
                "error": "Telegram API transport error (getUpdates): TimeoutError"
            },
            "last_handled_result": {
                "status": "rendered"
            }
        }))?,
    )?;
    let out = temp.path().join("health_poll_error.json");

    let output = remote_operator_command(temp.path())
        .arg("--health")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--loop-status-file")
        .arg(&status_path)
        .arg("--health-max-age-sec")
        .arg("999999999")
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        !output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["health_status"], "unhealthy");
    assert_eq!(result["last_result_status"], "poll_error");
    assert!(result["transport_issues"]
        .as_array()
        .expect("transport issues")
        .contains(&json!("last_poll_transport_error")));
    assert_eq!(result["action_readiness"][0]["status"], "blocked");
    Ok(())
}

#[test]
#[serial]
fn remote_operator_watchdog_reports_stale_listener_without_listener_process() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let status_path = temp.path().join("loop_status.json");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&json!({
            "schema": "remote_operator_telegram_adapter_result.v1",
            "mode": "live_loop",
            "status": "polling",
            "poll_count": 11,
            "updates_seen": 3,
            "handled_result_count": 2,
            "last_result": {
                "generated_at": "2000-01-01T00:00:00+00:00",
                "status": "no_update"
            }
        }))?,
    )?;
    let out = temp.path().join("watchdog.json");

    let output = watchdog_command(temp.path())
        .arg("--dry-run")
        .arg("--systemd-mode")
        .arg("off")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--loop-status-file")
        .arg(&status_path)
        .arg("--health-max-age-sec")
        .arg("1")
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        !output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["schema"], "remote_operator_telegram_watchdog.v1");
    assert_eq!(result["health_status"], "unhealthy");
    assert!(result["issues"]
        .as_array()
        .expect("issues")
        .contains(&json!("last_poll_stale")));
    assert_eq!(result["listener"]["listener_status"], "polling");
    assert_eq!(result["systemd"]["active_state"], "not_checked");
    assert_eq!(result["alert"]["needed"], true);
    assert_eq!(result["alert"]["sent"], false);
    assert_eq!(result["alert"]["reason"], "dry_run");
    assert!(result["alert"]["line_count"].as_u64().unwrap() <= 5);
    assert!(result["alert"]["char_count"].as_u64().unwrap() <= 360);
    let message = result["alert"]["message_preview"]
        .as_str()
        .expect("message preview");
    assert!(message.contains("Remote Operator 고장"));
    assert!(message.contains("야간주행: 불가"));
    assert!(message.contains("systemctl --user restart forager-telegram-operator.service"));
    let serialized = serde_json::to_string(&result)?;
    assert!(!serialized.contains("fake-token-for-test"));
    assert!(!serialized.contains("999999:"));
    Ok(())
}

#[test]
#[serial]
fn remote_operator_watchdog_rate_limits_repeated_alerts() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let status_path = temp.path().join("loop_status.json");
    let state_path = temp.path().join("watchdog_state.json");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&json!({
            "schema": "remote_operator_telegram_adapter_result.v1",
            "mode": "live_loop",
            "status": "polling",
            "last_result": {
                "generated_at": "2000-01-01T00:00:00+00:00",
                "status": "no_update"
            }
        }))?,
    )?;
    fs::write(
        &state_path,
        serde_json::to_string_pretty(&json!({
            "schema": "remote_operator_telegram_watchdog_state.v1",
            "last_alert_key": "last_poll_stale",
            "last_alert_at": "2099-01-01T00:00:00+00:00"
        }))?,
    )?;
    let out = temp.path().join("watchdog_rate_limited.json");

    let output = watchdog_command(temp.path())
        .arg("--dry-run")
        .arg("--systemd-mode")
        .arg("off")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--loop-status-file")
        .arg(&status_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--health-max-age-sec")
        .arg("1")
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        !output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["health_status"], "unhealthy");
    assert_eq!(result["alert"]["needed"], true);
    assert_eq!(result["alert"]["suppressed"], true);
    assert_eq!(result["alert"]["reason"], "rate_limited");
    assert_eq!(result["alert"]["sent"], false);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_watchdog_accepts_fresh_listener() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let status_path = temp.path().join("loop_status.json");
    fs::write(
        &status_path,
        serde_json::to_string_pretty(&json!({
            "schema": "remote_operator_telegram_adapter_result.v1",
            "mode": "live_loop",
            "status": "polling",
            "poll_count": 11,
            "last_result": {
                "generated_at": "2099-01-01T00:00:00+00:00",
                "status": "no_update"
            }
        }))?,
    )?;
    let out = temp.path().join("watchdog_healthy.json");

    let output = watchdog_command(temp.path())
        .arg("--dry-run")
        .arg("--systemd-mode")
        .arg("off")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--loop-status-file")
        .arg(&status_path)
        .arg("--health-max-age-sec")
        .arg("999999999")
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["health_status"], "healthy");
    assert_eq!(result["issues"], json!([]));
    assert_eq!(result["alert"]["needed"], false);
    assert_eq!(result["alert"]["reason"], "healthy");
    Ok(())
}

#[test]
#[serial]
fn telegram_operator_systemd_installer_dry_run_renders_unit() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("service.json");

    let output = Command::new("python3")
        .arg(script_path("install_offdesk_telegram_operator_service.py"))
        .arg("--dry-run")
        .arg("--repo-root")
        .arg(temp.path())
        .arg("--forager-bin")
        .arg(temp.path().join("target/debug/forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--loop-status-file")
        .arg(temp.path().join("loop.json"))
        .env("HOME", temp.path())
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    fs::write(&out, &output.stdout)?;
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(
        result["schema"],
        "forager_telegram_operator_systemd_install.v1"
    );
    assert_eq!(result["installed"], false);
    let unit = result["unit_preview"].as_str().expect("unit preview");
    assert!(unit.contains("ExecStart="));
    assert!(unit.contains("offdesk_remote_operator_telegram.py"));
    assert!(unit.contains("--poll-timeout-sec 30"));
    assert!(unit.contains("--poll-error-backoff-sec 5"));
    // Proactive attention notification is enabled by default for deployments.
    assert!(unit.contains("--attention-notify"));
    assert!(unit.contains("StartLimitIntervalSec=0"));
    assert!(unit.contains("Restart=always"));
    assert!(!unit.contains("fake-token-for-test"));
    Ok(())
}

#[test]
#[serial]
fn telegram_operator_systemd_installer_dry_run_renders_watchdog_timer() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;

    let output = Command::new("python3")
        .arg(script_path("install_offdesk_telegram_operator_service.py"))
        .arg("--dry-run")
        .arg("--include-watchdog")
        .arg("--repo-root")
        .arg(temp.path())
        .arg("--forager-bin")
        .arg(temp.path().join("target/debug/forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--loop-status-file")
        .arg(temp.path().join("loop.json"))
        .env("HOME", temp.path())
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout)?;
    assert_eq!(result["watchdog_included"], true);
    assert_eq!(result["watchdog_installed"], false);
    let service_unit = result["watchdog_service_unit_preview"]
        .as_str()
        .expect("watchdog service unit");
    let timer_unit = result["watchdog_timer_unit_preview"]
        .as_str()
        .expect("watchdog timer unit");
    assert!(service_unit.contains("Type=oneshot"));
    assert!(service_unit.contains("offdesk_remote_operator_watchdog.py"));
    assert!(service_unit.contains("--systemd-mode required"));
    assert!(service_unit.contains("--alert-min-interval-sec 1800"));
    assert!(timer_unit.contains("OnBootSec=2min"));
    assert!(timer_unit.contains("OnUnitActiveSec=120s"));
    assert!(timer_unit.contains("WantedBy=timers.target"));
    let serialized = serde_json::to_string(&result)?;
    assert!(!serialized.contains("fake-token-for-test"));
    assert!(!serialized.contains("999999:"));
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_status_fixture_prioritizes_attention() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let projection_path = temp.path().join("status_attention.json");
    write_projection_file(
        &projection_path,
        "status",
        json!({
            "profile": "default",
            "waiting": 1,
            "running": 1,
            "total": 9,
            "pending_approvals": 2,
            "queued_offdesk_tasks": 3,
            "active_offdesk_tasks": 1,
            "failed_offdesk_tasks": 1,
            "closeout_required_offdesk_tasks": 1
        }),
    )?;
    let out = temp.path().join("status_attention_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--send-command-text")
        .arg("/status")
        .arg("--projection-file")
        .arg(&projection_path)
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(output.status.success());
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("승인 요청 2개가 먼저입니다."));
    assert!(preview.contains("아래 버튼으로 승인 내용 보기"));
    assert!(preview.contains("그 밖에 실패 1 · 마무리 1 · 진행 1 / 대기 3"));
    assert_eq!(
        result["interaction_context"]["context_kind"],
        "status_attention"
    );
    assert_eq!(
        result["interaction_context"]["focus_kind"],
        "approval_queue"
    );
    assert_eq!(result["interaction_context"]["next_command"], "/pending");
    assert_eq!(
        result["choice_surface_contract"]["has_contextual_choice"],
        true
    );
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_pending_fixture_is_mobile_scannable() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let projection_path = temp.path().join("pending_attention.json");
    write_projection_file(
        &projection_path,
        "pending",
        json!({
            "approval_count": 4,
            "approvals": [
                {"approval_id": "approval_one", "action": "approve_plan", "expired": false},
                {"approval_id": "approval_two", "action": "approve_launch", "expired": false},
                {"approval_id": "approval_three", "action": "provider_retarget", "expired": true},
                {"approval_id": "approval_four", "action": "approve_launch", "expired": false}
            ]
        }),
    )?;
    let out = temp.path().join("pending_attention_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--send-command-text")
        .arg("/pending --all")
        .arg("--projection-file")
        .arg(&projection_path)
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(output.status.success());
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>승인 대기</b>"));
    assert!(preview.contains("승인 요청 4개가 기다립니다. 만료 1개 포함."));
    assert!(preview.contains("계획 승인"));
    assert!(preview.contains("실행 승인"));
    assert!(preview.contains("외 2개 더 있음"));
    assert!(preview.contains("승인은 로컬에서 판단하세요."));
    assert_eq!(
        result["interaction_context"]["context_kind"],
        "approval_attention"
    );
    assert_eq!(result["interaction_context"]["focus_ref"], "approval_one");
    assert_eq!(
        result["interaction_context"]["next_command"],
        "/pending --all"
    );
    assert!(button_texts(&result).contains(&"전체 승인".to_string()));
    assert_eq!(
        result["choice_surface_contract"]["has_contextual_choice"],
        true
    );
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_plans_fixture_has_empty_and_nonempty_next_actions() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let projection_path = temp.path().join("plans_attention.json");
    write_projection_file(
        &projection_path,
        "plans",
        json!({
            "plan_count": 1,
            "plans": [
                {"plan_id": "plan_harness_mobile", "review_status": "revision_required"}
            ]
        }),
    )?;
    let out = temp.path().join("plans_attention_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--send-command-text")
        .arg("/plans --latest")
        .arg("--projection-file")
        .arg(&projection_path)
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(output.status.success());
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>자율주행 계획</b>"));
    assert!(preview.contains("plan_harness_mobile · 수정 필요"));
    assert!(preview.contains("아래 버튼으로 계획 상세 보기"));
    assert_eq!(
        result["interaction_context"]["context_kind"],
        "plan_attention"
    );
    assert_eq!(
        result["interaction_context"]["focus_ref"],
        "plan_harness_mobile"
    );
    assert_eq!(
        result["interaction_context"]["next_command"],
        "/show plan_harness_mobile"
    );
    assert!(button_texts(&result).contains(&"/show plan_harness_mobile".to_string()));
    assert_eq!(
        result["choice_surface_contract"]["has_contextual_choice"],
        true
    );
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_chat_prompt_includes_command_surface_and_snapshot() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("grounded_chat.json");
    let agent_request_path = temp.path().join("ollama_grounded_request.json");
    let (base_url, server) = spawn_fake_ollama_with_classification(
        agent_request_path.clone(),
        json!({
            "intent": "chat",
            "confidence": 0.9,
            "requires_clarification": false,
            "clarifying_question": null,
            "assistant_reply": "대기 중인 결정은 없습니다.",
            "reason": "State question answered from the operator snapshot.",
            "non_authorized": ["execution", "approval", "shell"]
        }),
    )?;

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("마지막 상태를 먼저 읽어볼까")
        .arg("--forager-bin")
        .arg(temp.path().join("missing-forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .arg("--agent-intent-mode")
        .arg("required")
        .arg("--agent-base-url")
        .arg(&base_url)
        .arg("--agent-model")
        .arg("qwen3-coder-next:latest")
        .output()?;

    server.join().expect("fake ollama server panicked")?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["parsed_command"]["command"], "chat");

    let agent_request: Value = serde_json::from_slice(&fs::read(&agent_request_path)?)?;
    let prompt = agent_request["prompt"].as_str().expect("agent prompt");
    // The chat agent must receive the live snapshot and the complete command
    // surface so it answers from real state and never invents slash commands.
    assert!(prompt.contains("operator_snapshot"));
    assert!(prompt.contains("supported_commands"));
    assert!(prompt.contains("autonomy_armed"));
    assert!(prompt.contains("workspace_projects"));
    assert!(prompt.contains("/remember"));
    assert!(prompt.contains("Never mention, suggest, or invent a slash command"));
    Ok(())
}

fn write_project_registry(home: &Path) -> Result<()> {
    let dir = home.join(".config").join("forager");
    fs::create_dir_all(&dir)?;
    fs::write(
        dir.join("projects.toml"),
        r#"schema = "forager_project_registry.v1"

[projects.twinpaper]
display_name = "TwinPaper"
workspace_patterns = ["1.2.8.TwinPaper"]
wiki_profile = "twinpaper-review"
"#,
    )?;
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_chat_grounds_mentioned_project_focus() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    write_project_registry(temp.path())?;
    let out = temp.path().join("focused_chat.json");
    let agent_request_path = temp.path().join("ollama_focus_request.json");
    let (base_url, server) = spawn_fake_ollama_with_classification(
        agent_request_path.clone(),
        json!({
            "intent": "chat",
            "confidence": 0.9,
            "requires_clarification": false,
            "clarifying_question": null,
            "assistant_reply": "TwinPaper 세션은 모두 중지 상태입니다.",
            "reason": "Project question answered from project_focus.",
            "non_authorized": ["execution", "approval", "shell"]
        }),
    )?;

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("좋아. Twinpaper 프로젝트 마지막 상태 리뷰해줘")
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .arg("--agent-intent-mode")
        .arg("required")
        .arg("--agent-base-url")
        .arg(&base_url)
        .arg("--agent-model")
        .arg("qwen3-coder-next:latest")
        .output()?;

    server.join().expect("fake ollama server panicked")?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["project_focus"]["key"], "twinpaper");
    assert_eq!(result["project_focus"]["source"], "mention");

    let agent_request: Value = serde_json::from_slice(&fs::read(&agent_request_path)?)?;
    let prompt = agent_request["prompt"].as_str().expect("agent prompt");
    // The agent must see the mentioned project's live state, not just the
    // registry row, so it answers with concrete session/wiki facts.
    assert!(prompt.contains("project_focus"));
    assert!(prompt.contains("\"focus_source\": \"mention\""));
    assert!(prompt.contains("TwinPaper"));
    assert!(prompt.contains("session_counts"));
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_chat_focus_sticks_across_messages() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    write_project_registry(temp.path())?;
    let state_path = temp.path().join("telegram_state.json");
    let feedback_file = temp.path().join("feedback.jsonl");
    let ingest_dir = temp.path().join("feedback_ingest");

    let replay = |update_path: &Path, out: &Path| -> Result<std::process::Output> {
        Ok(remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(update_path)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--feedback-file")
            .arg(&feedback_file)
            .arg("--feedback-ingest-dir")
            .arg(&ingest_dir)
            .arg("--out")
            .arg(out)
            .output()?)
    };

    let mention_update = temp.path().join("mention_update.json");
    write_text_update(&mention_update, 720, 900, "Twinpaper 마지막 기록 리뷰해줘")?;
    let mention_out = temp.path().join("mention_result.json");
    let output = replay(&mention_update, &mention_out)?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let mention: Value = serde_json::from_slice(&fs::read(&mention_out)?)?;
    assert_eq!(mention["parsed_command"]["command"], "chat");
    assert_eq!(mention["project_focus"]["key"], "twinpaper");
    assert_eq!(mention["project_focus"]["source"], "mention");
    let chat_hash = mention["target_chat_id_hash"].as_str().expect("chat hash");
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert_eq!(
        state["chat_focus_by_chat"][chat_hash]["project_key"],
        "twinpaper"
    );

    let followup_update = temp.path().join("followup_update.json");
    write_text_update(&followup_update, 721, 901, "지금 상태 어때")?;
    let followup_out = temp.path().join("followup_result.json");
    let output = replay(&followup_update, &followup_out)?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let followup: Value = serde_json::from_slice(&fs::read(&followup_out)?)?;
    assert_eq!(followup["parsed_command"]["command"], "chat");
    // The follow-up drops the project name; the chat's sticky focus keeps
    // the conversation about the same project.
    assert_eq!(followup["project_focus"]["key"], "twinpaper");
    assert_eq!(followup["project_focus"]["source"], "sticky");
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_agent_delegate_intent_offers_capture_without_keywords() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let agent_request_path = temp.path().join("ollama_delegate_request.json");
    let (base_url, server) = spawn_fake_ollama_with_classification(
        agent_request_path.clone(),
        json!({
            "intent": "delegate_work",
            "delegation_goal": "TwinPaper 논문 그림을 재정리한다",
            "confidence": 0.9,
            "requires_clarification": false,
            "clarifying_question": null,
            "assistant_reply": "계획 후보로 잡아둘게요.",
            "reason": "Operator handed over work in natural language.",
            "non_authorized": ["execution", "approval", "shell"]
        }),
    )?;
    let update_path = temp.path().join("delegate_update.json");
    // No deterministic planning keyword anywhere in this message: only the
    // agent's structured decision can route it to the capture card.
    write_text_update(
        &update_path,
        730,
        910,
        "트윈페이퍼 논문 그림 다시 정리해줘야겠다",
    )?;
    let state_path = temp.path().join("telegram_state.json");
    let out = temp.path().join("delegate_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&update_path)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(temp.path().join("feedback.jsonl"))
        .arg("--feedback-ingest-dir")
        .arg(temp.path().join("feedback_ingest"))
        .arg("--agent-intent-mode")
        .arg("required")
        .arg("--agent-base-url")
        .arg(&base_url)
        .arg("--agent-model")
        .arg("qwen3-coder-next:latest")
        .arg("--out")
        .arg(&out)
        .output()?;

    server.join().expect("fake ollama server panicked")?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["plan_capture_offered"], true);
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>계획 후보로 등록할까요?</b>"));
    // The card carries the agent's restated goal, not the raw message.
    assert!(preview.contains("TwinPaper 논문 그림을 재정리한다"));
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let chat_hash = result["target_chat_id_hash"].as_str().expect("chat hash");
    assert_eq!(
        state["pending_dispatch_confirmations_by_chat"][chat_hash]["note"],
        "TwinPaper 논문 그림을 재정리한다"
    );
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_agent_chat_intent_overrides_planning_keywords() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let agent_request_path = temp.path().join("ollama_question_request.json");
    let (base_url, server) = spawn_fake_ollama_with_classification(
        agent_request_path.clone(),
        json!({
            "intent": "chat",
            "delegation_goal": null,
            "confidence": 0.9,
            "requires_clarification": false,
            "clarifying_question": null,
            "assistant_reply": "지금 처리 중인 작업은 없습니다.",
            "reason": "Question about work in progress, not a delegation.",
            "non_authorized": ["execution", "approval", "shell"]
        }),
    )?;
    let update_path = temp.path().join("question_update.json");
    // Contains the deterministic marker '처리', but it is a question; the
    // agent's chat decision must win over the keyword fallback.
    write_text_update(&update_path, 731, 911, "지금 뭐 처리하고 있어?")?;
    let state_path = temp.path().join("telegram_state.json");
    let out = temp.path().join("question_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&update_path)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--feedback-file")
        .arg(temp.path().join("feedback.jsonl"))
        .arg("--feedback-ingest-dir")
        .arg(temp.path().join("feedback_ingest"))
        .arg("--agent-intent-mode")
        .arg("required")
        .arg("--agent-base-url")
        .arg(&base_url)
        .arg("--agent-model")
        .arg("qwen3-coder-next:latest")
        .arg("--out")
        .arg(&out)
        .output()?;

    server.join().expect("fake ollama server panicked")?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["plan_capture_offered"], Value::Null);
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("<b>Forager 응답</b>"));
    assert!(preview.contains("지금 처리 중인 작업은 없습니다."));
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let chat_hash = result["target_chat_id_hash"].as_str().expect("chat hash");
    assert_eq!(
        state["pending_dispatch_confirmations_by_chat"][chat_hash],
        Value::Null
    );
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_inspect_intent_probes_workspace_and_answers() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    write_project_registry(temp.path())?;
    let workspace_root = temp.path().join("workspace");
    let project_dir = workspace_root.join("1.2.8.TwinPaper");
    fs::create_dir_all(project_dir.join("modules"))?;
    fs::write(project_dir.join("PROJECT_STATE.md"), "current state\n")?;
    fs::write(project_dir.join("README.md"), "TwinPaper\n")?;
    fs::write(
        project_dir.join("modules").join("next.md"),
        "next actions\n",
    )?;

    let body_dir = temp.path().join("agent_requests");
    fs::create_dir_all(&body_dir)?;
    let (base_url, server) = spawn_fake_ollama_with_sequence(
        body_dir.clone(),
        vec![
            json!({
                "intent": "inspect_project",
                "delegation_goal": null,
                "confidence": 0.9,
                "requires_clarification": false,
                "clarifying_question": null,
                "assistant_reply": null,
                "reason": "Operator asked for the project's local file state.",
                "non_authorized": ["execution", "approval", "shell"]
            }),
            json!({
                "intent": "chat",
                "delegation_goal": null,
                "confidence": 0.9,
                "requires_clarification": false,
                "clarifying_question": null,
                "assistant_reply": "PROJECT_STATE.md와 README.md가 확인됩니다. git 저장소는 아닙니다.",
                "reason": "Answered from local_inspection.",
                "non_authorized": ["execution", "approval", "shell"]
            }),
        ],
    )?;
    let out = temp.path().join("inspect_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("Twinpaper 로컬 파일 상태 확인해봐")
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--workspace-root")
        .arg(&workspace_root)
        .arg("--out")
        .arg(&out)
        .arg("--agent-intent-mode")
        .arg("required")
        .arg("--agent-base-url")
        .arg(&base_url)
        .arg("--agent-model")
        .arg("qwen3-coder-next:latest")
        .output()?;

    server.join().expect("fake ollama server panicked")?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    assert_eq!(result["local_inspection"]["status"], "ok");
    assert_eq!(result["local_inspection"]["project_key"], "twinpaper");
    assert_eq!(result["project_focus"]["key"], "twinpaper");
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("PROJECT_STATE.md와 README.md가 확인됩니다."));

    // The second agent call must carry the probe results as ground truth.
    let second_request: Value =
        serde_json::from_slice(&fs::read(body_dir.join("generate_1.json"))?)?;
    let second_prompt = second_request["prompt"].as_str().expect("second prompt");
    assert!(second_prompt.contains("local_inspection"));
    assert!(second_prompt.contains("PROJECT_STATE.md"));
    assert!(second_prompt.contains("modules/next.md"));
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_chat_scrubs_hallucinated_commands() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("scrubbed_chat.json");
    let agent_request_path = temp.path().join("ollama_scrub_request.json");
    let (base_url, server) = spawn_fake_ollama_with_classification(
        agent_request_path.clone(),
        json!({
            "intent": "chat",
            "confidence": 0.95,
            "requires_clarification": false,
            "clarifying_question": null,
            "assistant_reply": "목록은 /list 명령어로 확인할 수 있습니다.",
            "reason": "Model hallucinated a nonexistent command.",
            "non_authorized": ["execution", "approval", "shell"]
        }),
    )?;

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("목록이 안보이잖아")
        .arg("--forager-bin")
        .arg(temp.path().join("missing-forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .arg("--agent-intent-mode")
        .arg("required")
        .arg("--agent-base-url")
        .arg(&base_url)
        .arg("--agent-model")
        .arg("qwen3-coder-next:latest")
        .output()?;

    server.join().expect("fake ollama server panicked")?;
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    // /list does not exist; the reply must not teach the operator a command
    // that will bounce with unsupported_remote_operator_command.
    assert_eq!(
        result["parsed_command"]["agent_intent"]["assistant_reply"],
        "목록은 /help 명령어로 확인할 수 있습니다."
    );
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(!preview.contains("/list"));
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_autonomy_stale_confirmation_does_not_block_proposal() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let update_path = temp.path().join("idle_update.json");
    write_text_update(&update_path, 700, 900, "/status")?;
    let state_path = temp.path().join("telegram_state.json");
    let out = temp.path().join("autonomy_result.json");

    // An expired proposal confirmation (TTL long past) left in state must be
    // cleared, not treated as pending forever.
    let chat_hash = "sha256:15e2b0d3c33891eb";
    let state = json!({
        "schema": "remote_operator_telegram_state.v1",
        "offset": 0,
        "pending_dispatch_confirmations_by_chat": {
            chat_hash: {
                "schema": "telegram_dispatch_confirmation.v1",
                "token": "deadbeef0000",
                "kind": "autonomy_start",
                "target_id": "overnight",
                "action_kind": "arm",
                "observed_hash": "",
                "note": "idle 45m",
                "chat_id_hash": chat_hash,
                "created_at": "2026-07-01T00:00:00+00:00",
                "ttl_sec": 1800
            }
        }
    });
    fs::write(&state_path, serde_json::to_string_pretty(&state)?)?;

    // Watch tree whose newest mtime is hours old -> idle.
    let watch_dir = temp.path().join("sessions");
    fs::create_dir_all(&watch_dir)?;
    let marker = watch_dir.join("session.jsonl");
    fs::write(&marker, "{}\n")?;
    let old = std::time::SystemTime::now() - Duration::from_secs(2 * 3600);
    fs::File::options()
        .write(true)
        .open(&marker)?
        .set_modified(old)?;
    fs::File::open(&watch_dir)?.set_modified(old)?;

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--once")
        .arg("--replay-update-file")
        .arg(&update_path)
        .arg("--forager-bin")
        .arg(env!("CARGO_BIN_EXE_forager"))
        .arg("--env-file")
        .arg(&env_path)
        .arg("--state-file")
        .arg(&state_path)
        .arg("--out")
        .arg(&out)
        .arg("--autonomy-propose")
        .arg("--autonomy-idle-min")
        .arg("30")
        .arg("--autonomy-propose-from-hour")
        .arg("0")
        .arg("--autonomy-watch-path")
        .arg(&watch_dir)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["autonomy_proposal"]["status"], "proposed");
    assert!(
        result["autonomy_proposal"]["idle_min"]
            .as_i64()
            .expect("idle minutes")
            >= 30
    );

    // The stale token was replaced by a fresh confirmation for the new card.
    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    let pending = &state["pending_dispatch_confirmations_by_chat"][chat_hash];
    assert_eq!(pending["kind"], "autonomy_start");
    assert_ne!(pending["token"], "deadbeef0000");
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_guide_renders_full_command_surface() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;

    for command in ["/guide", "/qna"] {
        let out = temp.path().join("guide_result.json");
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--command-text")
            .arg(command)
            .arg("--env-file")
            .arg(&env_path)
            .arg("--out")
            .arg(&out)
            .output()?;

        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
        assert_eq!(result["status"], "rendered");
        assert_eq!(result["parsed_command"]["command"], "guide");
        let preview = result["message_preview"].as_str().expect("message preview");
        // The reference sheet must teach the whole surface, grouped.
        assert!(preview.contains("<b>Forager 사용 안내</b>"));
        for group in ["[조회]", "[실행]", "[기록]", "[제어]"] {
            assert!(preview.contains(group), "missing group {group}");
        }
        for usage in [
            "/attention",
            "/decision",
            "/plan",
            "/remember",
            "/pause",
            "/guide",
        ] {
            assert!(preview.contains(usage), "missing command {usage}");
        }
        // Telegram HTML: every non-tag "<" must be escaped or sendMessage 400s.
        assert!(preview.contains("&lt;목표&gt;"));
        for line in preview.lines() {
            let stripped = line.replace("<b>", "").replace("</b>", "");
            assert!(
                !stripped.contains('<') && !stripped.contains('>'),
                "unescaped angle bracket in guide line: {line}"
            );
        }
        // Guide uses the relaxed reference-sheet budget, still warning-free.
        assert_mobile_contract(&result);
        assert!(
            result["mobile_card_contract"]["max_lines"]
                .as_u64()
                .expect("guide max lines")
                > 5
        );
    }
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_help_points_to_guide() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let out = temp.path().join("help_result.json");

    let output = remote_operator_command(temp.path())
        .arg("--dry-run")
        .arg("--command-text")
        .arg("/help")
        .arg("--env-file")
        .arg(&env_path)
        .arg("--out")
        .arg(&out)
        .output()?;

    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&fs::read(&out)?)?;
    assert_eq!(result["status"], "rendered");
    let preview = result["message_preview"].as_str().expect("message preview");
    assert!(preview.contains("/guide"));
    assert_mobile_contract(&result);
    Ok(())
}

#[test]
#[serial]
fn remote_operator_telegram_waiting_session_notifies_once_per_episode() -> Result<()> {
    let temp = tempdir()?;
    let env_path = temp.path().join("telegram.env");
    write_env_file(&env_path)?;
    let state_path = temp.path().join("telegram_state.json");
    let status_path = temp.path().join("status.json");
    fs::write(
        &status_path,
        serde_json::to_string(&json!({
            "sessions": [
                {"id": "abc123", "title": "ClaudePilot", "tool": "claude",
                 "path": "/ws/1.2.8.TwinPaper", "status": "waiting", "project": "twinpaper"},
                {"id": "def456", "title": "Tuner", "tool": "codex",
                 "path": "/ws/nano", "status": "running", "project": "nanoclustering"}
            ]
        }))?,
    )?;

    let mut results = Vec::new();
    for (index, update_id) in [(1, 810), (2, 811)] {
        let update_path = temp.path().join(format!("update_{index}.json"));
        write_text_update(&update_path, update_id, 960 + index, "/status")?;
        let out = temp.path().join(format!("waiting_result_{index}.json"));
        let output = remote_operator_command(temp.path())
            .arg("--dry-run")
            .arg("--once")
            .arg("--replay-update-file")
            .arg(&update_path)
            .arg("--forager-bin")
            .arg(env!("CARGO_BIN_EXE_forager"))
            .arg("--env-file")
            .arg(&env_path)
            .arg("--state-file")
            .arg(&state_path)
            .arg("--out")
            .arg(&out)
            .arg("--session-notify")
            .arg("--session-status-file")
            .arg(&status_path)
            .output()?;
        assert!(
            output.status.success(),
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        results.push(serde_json::from_slice::<Value>(&fs::read(&out)?)?);
    }

    // First poll notifies the waiting session (project-tagged card), second
    // poll is suppressed by the per-session backoff.
    assert_eq!(results[0]["session_notification"]["status"], "notified");
    let sent = results[0]["session_notification"]["sent"]
        .as_array()
        .expect("sent sessions");
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0]["session_id"], "abc123");
    assert_eq!(sent[0]["project"], "twinpaper");
    assert_eq!(
        results[1]["session_notification"]["status"],
        "no_new_waiting"
    );
    assert_eq!(results[1]["session_notification"]["waiting_count"], 1);

    let state: Value = serde_json::from_slice(&fs::read(&state_path)?)?;
    assert!(state["waiting_notified_by_session"]["abc123"].is_number());
    Ok(())
}
